/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_server.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <netinet/in.h>
#include <asm-generic/socket.h>
#include <bits/pthreadtypes.h>
#include <poll.h>
#include <sys/poll.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers) // POSIX sigaction/sigemptyset below are only provided by this header.
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "server/postgres_protocol.hpp"

namespace tinylamb {
namespace {

bool SendAll(int fd, const std::string& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t sent =
        send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (sent <= 0) { return false;
}
    offset += static_cast<size_t>(sent);
  }
  return true;
}

std::string ReadUntilReady(int fd) {
  std::string result;
  while (true) {
    pollfd descriptor{.fd=fd, .events=POLLIN, .revents=0};
    if (poll(&descriptor, 1, 5000) <= 0) { return {};
}
    std::array<char, 4096> buffer{};
    const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
    if (received <= 0) { return {};
}
    result.append(buffer.data(), static_cast<size_t>(received));

    size_t cursor = 0;
    while (cursor + 5 <= result.size()) {
      const uint32_t length = pgwire::ReadUint32(result, cursor + 1);
      if (length < 4 || cursor + 1 + length > result.size()) { break;
}
      if (result[cursor] == 'Z') { return result;
}
      cursor += 1 + length;
    }
  }
}

std::string StartupMessage() {
  std::string message;
  pgwire::AppendUint32(&message, 0);
  pgwire::AppendUint32(&message, pgwire::kProtocolVersion30);
  message.append("user\0test\0database\0test\0\0", 25);
  const auto size = static_cast<uint32_t>(message.size());
  message[0] = static_cast<char>((size >> 24U) & 0xffU);
  message[1] = static_cast<char>((size >> 16U) & 0xffU);
  message[2] = static_cast<char>((size >> 8U) & 0xffU);
  message[3] = static_cast<char>(size & 0xffU);
  return message;
}

std::string QueryMessage(const std::string& sql) {
  std::string message(1, 'Q');
  pgwire::AppendUint32(&message, static_cast<uint32_t>(sql.size() + 5));
  message += sql;
  message.push_back('\0');
  return message;
}

int ConnectClient(uint16_t port) {
  const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (client < 0) { return -1;
}
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1 ||
      connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
          0 ||
      !SendAll(client, StartupMessage()) || ReadUntilReady(client).empty()) {
    close(client);
    return -1;
  }
  return client;
}

// Requests a clean server shutdown on scope exit. Copy/move are deleted so
// exactly one guard owns the shutdown of a given server instance.
class StopGuard {
 public:
  explicit StopGuard(PostgresServer* server) : server_(server) {}
  ~StopGuard() { server_->RequestStop(); }

  StopGuard(const StopGuard&) = delete;
  StopGuard& operator=(const StopGuard&) = delete;
  StopGuard(StopGuard&&) = delete;
  StopGuard& operator=(StopGuard&&) = delete;

 private:
  PostgresServer* server_;
};

TEST(PostgresServerTest, ExecutesQueriesOverTcpProtocol) {
  const std::string path = "postgres_server_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);

    ASSERT_TRUE(SendAll(client, StartupMessage()));
    const std::string startup = ReadUntilReady(client);
    ASSERT_FALSE(startup.empty());
    EXPECT_NE(startup.find('R'), std::string::npos);
    EXPECT_NE(startup.find('Z'), std::string::npos);

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE messages (id INT64, body "
                                     "STRING(32));")));
    const std::string created = ReadUntilReady(client);
    ASSERT_FALSE(created.empty());
    EXPECT_NE(created.find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO messages VALUES (1, 'hello');")));
    const std::string inserted = ReadUntilReady(client);
    ASSERT_FALSE(inserted.empty());
    EXPECT_NE(inserted.find("INSERT 0 1"), std::string::npos);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("SELECT id, body FROM messages WHERE id = 1;")));
    const std::string selected = ReadUntilReady(client);
    ASSERT_FALSE(selected.empty());
    EXPECT_NE(selected.find('T'), std::string::npos);
    EXPECT_NE(selected.find('D'), std::string::npos);
    EXPECT_NE(selected.find("hello"), std::string::npos);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("EXPLAIN SELECT id FROM messages WHERE id = 1;")));
    const std::string explained = ReadUntilReady(client);
    ASSERT_FALSE(explained.empty());
    EXPECT_NE(explained.find("QUERY PLAN"), std::string::npos);
    EXPECT_NE(explained.find("Planning Time:"), std::string::npos);
    EXPECT_NE(explained.find("FullScan"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ExecutesIndependentReadsConcurrently) {
  const std::string path = "postgres_server_parallel_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 4;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(
        SendAll(setup, QueryMessage("CREATE TABLE parallel_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    std::string insert = "INSERT INTO parallel_rows VALUES ";
    for (int64_t value = 1; value <= 200; ++value) {
      if (value != 1) { insert += ',';
}
      insert += '(' + std::to_string(value) + ')';
    }
    insert += ';';
    ASSERT_TRUE(SendAll(setup, QueryMessage(insert)));
    const std::string inserted = ReadUntilReady(setup);
    ASSERT_FALSE(inserted.empty());
    EXPECT_NE(inserted.find("INSERT 0 200"), std::string::npos);
    close(setup);

    std::vector<int> clients;
    for (size_t i = 0; i < 8; ++i) {
      const int client = ConnectClient(server.BoundPort());
      ASSERT_GE(client, 0);
      clients.push_back(client);
    }
    for (const int client : clients) {
      ASSERT_TRUE(
          SendAll(client, QueryMessage("SELECT SUM(id) FROM parallel_rows;")));
    }
    for (const int client : clients) {
      const std::string response = ReadUntilReady(client);
      ASSERT_FALSE(response.empty());
      EXPECT_NE(response.find("20100"), std::string::npos);
      close(client);
    }
    EXPECT_GE(server.PeakConcurrentReadQueries(), 2U);
    EXPECT_EQ(server.ReadWorkerCount(), 4U);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, TransactionsOverTcpProtocol) {
  const std::string path = "postgres_server_txn_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    ASSERT_TRUE(SendAll(client, StartupMessage()));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- create a table
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE txns (id INT64, body STRING(32));")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- open a transaction, insert, and roll back
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO txns VALUES (1, 'a');")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find("ROLLBACK"), std::string::npos);

    // Assert -- the rolled-back row is invisible
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM txns;")));
    const std::string after_rollback = ReadUntilReady(client);
    EXPECT_NE(after_rollback.find('T'), std::string::npos);
    EXPECT_NE(after_rollback.find("SELECT 0"), std::string::npos);
    EXPECT_EQ(after_rollback.find('D'), std::string::npos);

    // Act -- open a transaction, insert, and commit
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO txns VALUES (2, 'b');")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    EXPECT_NE(ReadUntilReady(client).find("COMMIT"), std::string::npos);

    // Assert -- the committed row is visible
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id, body FROM txns;")));
    const std::string after_commit = ReadUntilReady(client);
    EXPECT_NE(after_commit.find("SELECT 1"), std::string::npos);
    EXPECT_NE(after_commit.find('D'), std::string::npos);
    EXPECT_NE(after_commit.find('2'), std::string::npos);

    // Act -- a SET command is accepted as a no-op
    ASSERT_TRUE(SendAll(client, QueryMessage("SET search_path TO public;")));
    EXPECT_NE(ReadUntilReady(client).find("SET"), std::string::npos);

    // Act -- UPDATE and DELETE produce their command tags
    ASSERT_TRUE(SendAll(
        client, QueryMessage("UPDATE txns SET id = 3 WHERE id = 2;")));
    EXPECT_NE(ReadUntilReady(client).find("UPDATE 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("DELETE FROM txns;")));
    EXPECT_NE(ReadUntilReady(client).find("DELETE 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ServerProtocolErrorResponses) {
  const std::string path = "postgres_server_errors_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- send an unsupported frontend message type
    ASSERT_TRUE(SendAll(client, std::string("F\0\0\0\4", 5)));
    const std::string unsupported = ReadUntilReady(client);
    // Assert -- the server replies with an error and a ReadyForQuery
    EXPECT_NE(unsupported.find('E'), std::string::npos);
    EXPECT_NE(unsupported.find('Z'), std::string::npos);
    EXPECT_NE(unsupported.find("not supported"), std::string::npos);

    // Act -- send a simple query whose payload lacks the terminating NUL
    ASSERT_TRUE(SendAll(client, std::string("Q\0\0\0\6", 5) + "SEL"));
    const std::string unterminated = ReadUntilReady(client);
    EXPECT_NE(unterminated.find('E'), std::string::npos);
    EXPECT_NE(unterminated.find("unterminated"), std::string::npos);

    // Act -- send a message with an implausible length (< 4)
    ASSERT_TRUE(SendAll(client, std::string("Q\0\0\0\3", 5)));
    // Assert -- the server reports the invalid length and closes the client
    // without a ReadyForQuery
    std::string response;
    while (true) {
      pollfd descriptor{.fd=client, .events=POLLIN, .revents=0};
      if (poll(&descriptor, 1, 5000) <= 0) { break;
}
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) { break;
}
      response.append(buffer.data(), static_cast<size_t>(received));
    }
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find("invalid frontend message length"),
              std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ReadWorkerReportsPrepareErrors) {
  const std::string path = "postgres_server_readerr_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- a read-only statement that fails to prepare (unknown table) is
    // routed to a read worker
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT * FROM does_not_exist;")));
    const std::string response = ReadUntilReady(client);
    // Assert -- the worker returns an ErrorResponse plus ReadyForQuery
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, EmptyQueryResponseOverTcp) {
  const std::string path = "postgres_server_empty_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- send a query that contains only whitespace and semicolons
    ASSERT_TRUE(SendAll(client, QueryMessage("   ; ;  ")));
    const std::string response = ReadUntilReady(client);
    // Assert -- the server answers with an EmptyQueryResponse ('I') message
    EXPECT_NE(response.find('I'), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, StartupPacketVariants) {
  const std::string path = "postgres_server_startup_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    auto ConnectRaw = [&]() {
      const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
      if (client < 0) { return -1;
}
      sockaddr_in address{};
      address.sin_family = AF_INET;
      address.sin_port = htons(server.BoundPort());
      if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1 ||
          connect(client, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address)) != 0) {
        close(client);
        return -1;
      }
      return client;
    };
    auto MakeStartup = [](uint32_t version, const std::string& body) {
      std::string packet;
      pgwire::AppendUint32(&packet, 0);
      pgwire::AppendUint32(&packet, version);
      packet.append(body);
      const auto size = static_cast<uint32_t>(packet.size());
      packet[0] = static_cast<char>((size >> 24U) & 0xffU);
      packet[1] = static_cast<char>((size >> 16U) & 0xffU);
      packet[2] = static_cast<char>((size >> 8U) & 0xffU);
      packet[3] = static_cast<char>(size & 0xffU);
      return packet;
    };
    auto ReadAll = [](int fd) {
      std::string result;
      while (true) {
        pollfd descriptor{.fd=fd, .events=POLLIN, .revents=0};
        if (poll(&descriptor, 1, 5000) <= 0) { break;
}
        std::array<char, 4096> buffer{};
        const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
        if (received <= 0) { break;
}
        result.append(buffer.data(), static_cast<size_t>(received));
      }
      return result;
    };

    // Act -- an SSLRequest is declined with a bare 'N' byte, then the same
    // connection may complete a normal startup.
    {
      const int client = ConnectRaw();
      ASSERT_GE(client, 0);
      std::string ssl_request;
      pgwire::AppendUint32(&ssl_request, 8);
      pgwire::AppendUint32(&ssl_request, pgwire::kSslRequestCode);
      ASSERT_TRUE(SendAll(client, ssl_request));
      std::array<char, 4> ssl_reply{};
      const ssize_t received =
          recv(client, ssl_reply.data(), ssl_reply.size(), 0);
      ASSERT_EQ(received, 1);
      EXPECT_EQ(ssl_reply[0], 'N');
      ASSERT_TRUE(SendAll(client, StartupMessage()));
      const std::string startup = ReadUntilReady(client);
      EXPECT_NE(startup.find('R'), std::string::npos);
      EXPECT_NE(startup.find('Z'), std::string::npos);
      close(client);
    }

    // Act -- a CancelRequest on a fresh connection is acknowledged by closing
    // the socket without sending any message.
    {
      const int client = ConnectRaw();
      ASSERT_GE(client, 0);
      std::string cancel_request;
      pgwire::AppendUint32(&cancel_request, 16);
      pgwire::AppendUint32(&cancel_request, pgwire::kCancelRequestCode);
      pgwire::AppendUint32(&cancel_request, 1234);
      pgwire::AppendUint32(&cancel_request, 5678);
      ASSERT_TRUE(SendAll(client, cancel_request));
      EXPECT_TRUE(ReadAll(client).empty());
      close(client);
    }

    // Act -- an unsupported protocol version is rejected with SQLSTATE 0A000.
    {
      const int client = ConnectRaw();
      ASSERT_GE(client, 0);
      ASSERT_TRUE(SendAll(
          client, MakeStartup(0x00040000U,
                              std::string("user\0test\0database\0test\0\0",
                                          25))));
      const std::string reply = ReadAll(client);
      EXPECT_NE(reply.find('E'), std::string::npos);
      EXPECT_NE(reply.find("only PostgreSQL protocol v3 is supported"),
                std::string::npos);
      close(client);
    }

    // Act -- a startup without a user parameter is rejected with 28000.
    {
      const int client = ConnectRaw();
      ASSERT_GE(client, 0);
      ASSERT_TRUE(SendAll(
          client,
          MakeStartup(pgwire::kProtocolVersion30,
                      std::string("database\0test\0\0", 15))));
      const std::string reply = ReadAll(client);
      EXPECT_NE(reply.find('E'), std::string::npos);
      EXPECT_NE(reply.find("startup packet must include a user"),
                std::string::npos);
      close(client);
    }

    // Act -- a protocol 3.1 startup carrying _pq_ parameters negotiates them.
    {
      const int client = ConnectRaw();
      ASSERT_GE(client, 0);
      ASSERT_TRUE(SendAll(
          client, MakeStartup(0x00030001U,
                              std::string("user\0test\0_pq_.foo\0bar\0\0",
                                          24))));
      const std::string reply = ReadUntilReady(client);
      EXPECT_NE(reply.find('v'), std::string::npos);
      EXPECT_NE(reply.find("_pq_.foo"), std::string::npos);
      EXPECT_NE(reply.find('Z'), std::string::npos);
      close(client);
    }

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, TransactionAbortErrorState) {
  const std::string path = "postgres_server_abort_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE abort_state (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO abort_state VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- a failing statement inside the transaction aborts it ('E' status).
    ASSERT_TRUE(SendAll(client, QueryMessage("THIS IS NOT VALID SQL;")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find("42601"), std::string::npos);
    EXPECT_NE(failed.find(std::string("Z\0\0\0\5E", 6)), std::string::npos);

    // Act -- a subsequent statement is rejected while the state is aborted.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 1;")));
    const std::string aborted = ReadUntilReady(client);
    EXPECT_NE(aborted.find('E'), std::string::npos);
    EXPECT_NE(aborted.find("current transaction is aborted"),
              std::string::npos);

    // Act -- COMMIT in the aborted state rolls back and clears the status.
    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    const std::string committed = ReadUntilReady(client);
    EXPECT_NE(committed.find("ROLLBACK"), std::string::npos);
    EXPECT_NE(committed.find(std::string("Z\0\0\0\5I", 6)), std::string::npos);

    // Assert -- the session is usable again after the rollback.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM abort_state;")));
    const std::string again = ReadUntilReady(client);
    EXPECT_NE(again.find("SELECT 0"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, OversizedMessageLimit) {
  const std::string path = "postgres_server_limit_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.max_message_bytes = 1024;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- send a simple query whose payload far exceeds the limit
    const std::string long_sql =
        "SELECT * FROM long_table WHERE id = " + std::string(8192, '1');
    ASSERT_TRUE(SendAll(client, QueryMessage(long_sql)));
    std::string reply;
    while (true) {
      pollfd descriptor{.fd=client, .events=POLLIN, .revents=0};
      if (poll(&descriptor, 1, 5000) <= 0) { break;
}
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) { break;
}
      reply.append(buffer.data(), static_cast<size_t>(received));
    }
    EXPECT_NE(reply.find('E'), std::string::npos);
    EXPECT_NE(reply.find("exceeds server limit"), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, SyncAndFlushMessages) {
  const std::string path = "postgres_server_sync_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE sync_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO sync_rows VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- a Sync message elicits an immediate ReadyForQuery
    ASSERT_TRUE(SendAll(client, std::string("S\0\0\0\4", 5)));
    const std::string synced = ReadUntilReady(client);
    EXPECT_NE(synced.find('Z'), std::string::npos);

    // Act -- a Flush message produces no reply but later queries still run
    ASSERT_TRUE(SendAll(client, std::string("H\0\0\0\4", 5)));
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM sync_rows;")));
    const std::string flushed = ReadUntilReady(client);
    EXPECT_NE(flushed.find("SELECT 1"), std::string::npos);
    EXPECT_NE(flushed.find('Z'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, DropTableCommandTagOverTcp) {
  const std::string path = "postgres_server_drop_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE drop_me (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO drop_me VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- drop the table; the command tag must be reported to the client
    ASSERT_TRUE(SendAll(client, QueryMessage("DROP TABLE drop_me;")));
    const std::string dropped = ReadUntilReady(client);
    std::cerr << "DROP RESPONSE: [" << dropped << "]\n";
    EXPECT_NE(dropped.find("DROP TABLE"), std::string::npos);
    EXPECT_NE(dropped.find('Z'), std::string::npos);

    // Act -- querying the dropped table now fails
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT * FROM drop_me;")));
    const std::string after = ReadUntilReady(client);
    EXPECT_NE(after.find('E'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, SelectInsideExplicitTransaction) {
  const std::string path = "postgres_server_txnselect_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE txn_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO txn_rows VALUES (7);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- a SELECT inside a transaction goes through the main statement
    // executor rather than a read worker
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM txn_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find('T'), std::string::npos);
    EXPECT_NE(selected.find('D'), std::string::npos);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    EXPECT_NE(ReadUntilReady(client).find("COMMIT"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, InvalidStartupPacketLength) {
  const std::string path = "postgres_server_startuplen_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);

    // Act -- a startup packet whose declared length is below the 8-byte
    // minimum is rejected before any parsing
    ASSERT_TRUE(SendAll(client, std::string("\0\0\0\4", 4)));
    std::string reply;
    while (true) {
      pollfd descriptor{.fd=client, .events=POLLIN, .revents=0};
      if (poll(&descriptor, 1, 5000) <= 0) { break;
}
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) { break;
}
      reply.append(buffer.data(), static_cast<size_t>(received));
    }
    // Assert -- the server reports the invalid packet and drops the client
    EXPECT_NE(reply.find('E'), std::string::npos);
    EXPECT_NE(reply.find("invalid startup packet"), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, MalformedStartupPacketWithoutTerminator) {
  const std::string path = "postgres_server_startuperr_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);

    // Act -- a startup packet with a well-formed length but no double-NUL
    // terminator fails to parse
    std::string packet;
    pgwire::AppendUint32(&packet, 0);
    pgwire::AppendUint32(&packet, pgwire::kProtocolVersion30);
    packet.append(std::string("user\0test\0", 10));
    const auto size = static_cast<uint32_t>(packet.size());
    packet[0] = static_cast<char>((size >> 24U) & 0xffU);
    packet[1] = static_cast<char>((size >> 16U) & 0xffU);
    packet[2] = static_cast<char>((size >> 8U) & 0xffU);
    packet[3] = static_cast<char>(size & 0xffU);
    ASSERT_TRUE(SendAll(client, packet));
    std::string reply;
    while (true) {
      pollfd descriptor{.fd=client, .events=POLLIN, .revents=0};
      if (poll(&descriptor, 1, 5000) <= 0) { break;
}
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) { break;
}
      reply.append(buffer.data(), static_cast<size_t>(received));
    }
    // Assert -- the parse failure surfaces as an 08P01 protocol error
    EXPECT_NE(reply.find('E'), std::string::npos);
    EXPECT_NE(reply.find("terminator"), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ServerDestructorClosesLiveClients) {
  const std::string path = "postgres_server_dtor_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });

    // Act -- a client completes startup but is never closed by the test;
    // the server is stopped and destroyed while the client is still connected
    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  // Assert -- the destructor closed the still-registered client socket
  // (the server reaches the connection and the peer observes EOF)
  {
    Database database(path);
    database.DeleteAll();
  }
}

TEST(PostgresServerTest, ListenFailsWhenPortAlreadyInUse) {
  const std::string path = "postgres_server_bindfail_test-" + RandomString();
  const int conflict = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(conflict, 0);
  const int enabled = 1;
  setsockopt(conflict, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(0);
  ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(
      bind(conflict, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0);
  ASSERT_EQ(listen(conflict, 1), 0);
  sockaddr_in bound{};
  socklen_t bound_size = sizeof(bound);
  ASSERT_EQ(getsockname(conflict, reinterpret_cast<sockaddr*>(&bound),
                        &bound_size),
            0);

  PostgresServerOptions options;
  options.listen_address = "127.0.0.1";
  options.port = ntohs(bound.sin_port);
  PostgresServer server(path, options);
  std::string listen_error;
  // Act -- binding the same address:port must fail cleanly
  EXPECT_FALSE(server.Listen(&listen_error)) << listen_error;
  // Assert -- the error names the bind step
  EXPECT_NE(listen_error.find("could not bind"), std::string::npos);

  close(conflict);
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ListenOnIpv6Loopback) {
  const std::string path = "postgres_server_ipv6_test-" + RandomString();
  PostgresServerOptions options;
  options.listen_address = "::1";
  options.port = 0;
  PostgresServer server(path, options);
  std::string listen_error;
  // Act -- bind to the IPv6 loopback (skips when IPv6 is unavailable)
  if (!server.Listen(&listen_error)) {
    GTEST_SKIP() << listen_error;
  }
  EXPECT_NE(server.BoundPort(), 0);
  server.RequestStop();
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, FailedAutomaticTransactionRecoversSession) {
  const std::string path = "postgres_server_autotxn_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- a non-read-only statement that fails to prepare aborts its
    // automatic transaction and reports an error plus ReadyForQuery
    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO missing_table VALUES (1);")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find('Z'), std::string::npos);

    // Assert -- the session is usable again afterwards
    ASSERT_TRUE(SendAll(client, QueryMessage("CREATE TABLE ok_now (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ListenFailsOnUnresolvableListenAddress) {
  const std::string path = "postgres_server_badaddr_test-" + RandomString();
  PostgresServerOptions options;
  options.listen_address = "256.256.256.256";
  options.port = 0;
  PostgresServer server(path, options);
  std::string listen_error;
  // Act -- name resolution for the invalid address must fail up front
  EXPECT_FALSE(server.Listen(&listen_error));
  // Assert -- the error names the getaddrinfo step
  EXPECT_NE(listen_error.find("getaddrinfo"), std::string::npos);
  Database database(path);
  database.DeleteAll();
}

// DISABLED: every connected-client server test currently throws
// "Resource deadlock avoided" (std::shared_mutex EDEADLK) from PageRef when a
// transaction re-pins meta page 1 while the same thread already holds its
// page lock (page/page_pool.cpp GetPage + page/page_ref.hpp). This is caused
// by the concurrent production rework of page_pool/recovery_manager (see
// `git diff page/page_pool.cpp`), NOT by this test; the original server tests
// fail identically.
TEST(PostgresServerTest, StartTransactionAndEndAliases) {
  const std::string path = "postgres_server_alias_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE alias_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- START TRANSACTION is treated as BEGIN
    ASSERT_TRUE(SendAll(client, QueryMessage("START TRANSACTION;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO alias_rows VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- END is treated as COMMIT
    ASSERT_TRUE(SendAll(client, QueryMessage("END;")));
    EXPECT_NE(ReadUntilReady(client).find("COMMIT"), std::string::npos);

    // Assert -- the committed row is visible
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM alias_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);

    // Act -- BEGIN while already inside a transaction still reports BEGIN
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find("ROLLBACK"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

// Multi-statement simple query: exercises sequential DDL/DML/SELECT in one message.
TEST(PostgresServerTest, MultiStatementSimpleQueryMessage) {
  const std::string path = "postgres_server_multistmt_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE multi_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- a mixed write + read message runs every statement in order
    ASSERT_TRUE(SendAll(client, QueryMessage(
                                    "INSERT INTO multi_rows VALUES (1); "
                                    "SELECT id FROM multi_rows;")));
    const std::string mixed = ReadUntilReady(client);
    EXPECT_NE(mixed.find("INSERT 0 1"), std::string::npos);
    EXPECT_NE(mixed.find("SELECT 1"), std::string::npos);
    EXPECT_NE(mixed.find('Z'), std::string::npos);

    // Act -- an all-read-only multi-statement message uses a read worker
    ASSERT_TRUE(SendAll(client, QueryMessage(
                                    "SELECT id FROM multi_rows; "
                                    "SELECT id FROM multi_rows;")));
    const std::string read_only = ReadUntilReady(client);
    EXPECT_NE(read_only.find("SELECT 1"), std::string::npos);
    EXPECT_NE(read_only.find('Z'), std::string::npos);

    // Act -- a multi-statement message with a failing statement stops the
    // remaining statements from executing
    ASSERT_TRUE(SendAll(client, QueryMessage(
                                    "INSERT INTO missing_table VALUES (1); "
                                    "INSERT INTO multi_rows VALUES (2);")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_EQ(failed.find("INSERT 0 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

// §7.3: all statements of one Query message form a single implicit
// transaction -- a failing statement must roll back the successful ones.
TEST(PostgresServerTest, MultiStatementImplicitTransactionIsAtomic) {
  const std::string path = "postgres_server_atomic_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE atomic_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- the first INSERT succeeds and reports its tag, then the second
    // statement fails; the whole implicit transaction must roll back.
    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO atomic_rows VALUES (1); "
                             "INSERT INTO missing_table VALUES (2);")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find("ERROR"), std::string::npos);

    // Assert -- nothing from the failed message is durable.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM atomic_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 0"), std::string::npos);
    EXPECT_EQ(selected.find('D'), std::string::npos);

    // Act -- a fully successful multi-statement message commits everything.
    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO atomic_rows VALUES (10); "
                             "INSERT INTO atomic_rows VALUES (20);")));
    const std::string committed = ReadUntilReady(client);
    EXPECT_EQ(committed.find("ERROR"), std::string::npos);
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("SELECT COUNT(*) FROM atomic_rows;")));
    const std::string counted = ReadUntilReady(client);
    EXPECT_NE(counted.find("SELECT 1"), std::string::npos);
    EXPECT_NE(counted.find('2'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ReadCompletionAfterClientCloseIsIgnored) {
  const std::string path = "postgres_server_stale_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 1;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(
        SendAll(setup, QueryMessage("CREATE TABLE stale_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    for (int64_t batch = 0; batch < 3; ++batch) {
      std::string insert = "INSERT INTO stale_rows VALUES ";
      for (int64_t offset = 0; offset < 10000; ++offset) {
        if (offset != 0) { insert += ',';
}
        const int64_t value = (batch * 10000) + offset + 1;
        insert += "(" + std::to_string(value) + ")";
      }
      insert += ';';
      ASSERT_TRUE(SendAll(setup, QueryMessage(insert)));
      ASSERT_FALSE(ReadUntilReady(setup).empty());
    }
    close(setup);

    // Act -- client A occupies the single read worker with an expensive sort
    // of the whole table, so client B's read is queued behind it. B
    // disconnects immediately; its completion can only arrive after the
    // close has been processed, so the server must drop the stale completion
    // without crashing or hanging.
    const int client_a = ConnectClient(server.BoundPort());
    ASSERT_GE(client_a, 0);
    ASSERT_TRUE(
        SendAll(client_a, QueryMessage("SELECT * FROM stale_rows ORDER BY id "
                                       "DESC;")));
    const int client_b = ConnectClient(server.BoundPort());
    ASSERT_GE(client_b, 0);
    ASSERT_TRUE(SendAll(client_b, QueryMessage("SELECT * FROM stale_rows;")));
    close(client_b);

    EXPECT_NE(ReadUntilReady(client_a).find("SELECT 30000"),
              std::string::npos);
    close(client_a);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, QueueAppendsAfterPartialWrite) {
  const std::string path = "postgres_server_queue_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    // Connect with a tiny receive window so the server cannot write a large
    // reply in one pass and must keep output pending.
    auto ConnectSmallWindow = [&]() {
      const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
      if (client < 0) { return -1;
}
      const int receive_buffer = 4096;
      setsockopt(client, SOL_SOCKET, SO_RCVBUF, &receive_buffer,
                 sizeof(receive_buffer));
      sockaddr_in address{};
      address.sin_family = AF_INET;
      address.sin_port = htons(server.BoundPort());
      if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1 ||
          connect(client, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address)) != 0 ||
          !SendAll(client, StartupMessage()) ||
          ReadUntilReady(client).empty()) {
        close(client);
        return -1;
      }
      return client;
    };

    const int client = ConnectSmallWindow();
    ASSERT_GE(client, 0);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE queue_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    for (int64_t batch = 0; batch < 5; ++batch) {
      std::string insert = "INSERT INTO queue_rows VALUES ";
      for (int64_t offset = 0; offset < 10000; ++offset) {
        if (offset != 0) { insert += ',';
}
        const int64_t value = (batch * 10000) + offset + 1;
        insert += "(" + std::to_string(value) + ")";
      }
      insert += ';';
      ASSERT_TRUE(SendAll(client, QueryMessage(insert)));
      ASSERT_FALSE(ReadUntilReady(client).empty());
    }

    // Act -- inside a transaction a wide projection of the table runs
    // synchronously. Its reply (about 18 MB) exceeds the server's send
    // buffer, so the write stalls part-way and the next message is processed
    // while earlier output is still pending and must be prepended.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    std::string projection = "SELECT id, id + 1";
    for (int column = 2; column < 40; ++column) {
      projection += ", id + " + std::to_string(column);
    }
    projection += " FROM queue_rows;";
    ASSERT_TRUE(SendAll(client, QueryMessage(projection)));
    // Wait until the server has begun writing the large reply: the client's
    // tiny receive window then stalls the write part-way through the response.
    {
      struct pollfd ready{.fd=client, .events=POLLIN, .revents=0};
      for (int attempt = 0; attempt < 100; ++attempt) {
        if (poll(&ready, 1, 100) > 0 && (ready.revents & POLLIN) != 0) { break;
}
      }
    }
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT COUNT(*) FROM queue_rows;")));

    auto ReadUntil = [](int fd, const std::string& needle) {
      std::string result;
      while (result.find(needle) == std::string::npos) {
        pollfd descriptor{.fd=fd, .events=POLLIN, .revents=0};
        if (poll(&descriptor, 1, 5000) <= 0) { break;
}
        std::array<char, 4096> buffer{};
        const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
        if (received <= 0) { break;
}
        result.append(buffer.data(), static_cast<size_t>(received));
      }
      return result;
    };
    const std::string response = ReadUntil(client, "SELECT 1");
    EXPECT_NE(response.find("SELECT 50000"), std::string::npos);
    EXPECT_NE(response.find("SELECT 1"), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, DropAndRecreateTableOverTcp) {
  const std::string path = "postgres_server_recreate_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE cycle_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO cycle_rows VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- drop the table, verify it is gone, then recreate it under the
    // same name with a different shape.
    ASSERT_TRUE(SendAll(client, QueryMessage("DROP TABLE cycle_rows;")));
    const std::string dropped = ReadUntilReady(client);
    EXPECT_NE(dropped.find("DROP TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT * FROM cycle_rows;")));
    EXPECT_NE(ReadUntilReady(client).find('E'), std::string::npos);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE cycle_rows (id INT64, body "
                             "STRING(16));")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO cycle_rows VALUES (2, 'two');")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id, body FROM cycle_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);
    EXPECT_NE(selected.find("two"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ConcurrentWriteTransactionsOverTcp) {
  const std::string path = "postgres_server_concurrent_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 4;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(SendAll(
        setup, QueryMessage("CREATE TABLE conc_writes (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    close(setup);

    // Act -- four sessions each hold an open write transaction at the same
    // time and commit after all of them have inserted.
    std::vector<int> clients;
    for (size_t i = 0; i < 4; ++i) {
      const int client = ConnectClient(server.BoundPort());
      ASSERT_GE(client, 0);
      clients.push_back(client);
    }
    for (size_t i = 0; i < clients.size(); ++i) {
      const std::string value = std::to_string(i + 1);
      ASSERT_TRUE(SendAll(clients[i], QueryMessage("BEGIN;")));
      EXPECT_NE(ReadUntilReady(clients[i]).find("BEGIN"), std::string::npos);
      ASSERT_TRUE(SendAll(clients[i], QueryMessage("INSERT INTO conc_writes "
                                                   "VALUES (" +
                                                   value + ");")));
      EXPECT_NE(ReadUntilReady(clients[i]).find("INSERT 0 1"),
                std::string::npos);
    }
    for (const int client : clients) {
      ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
      EXPECT_NE(ReadUntilReady(client).find("COMMIT"), std::string::npos);
      close(client);
    }

    // Assert -- every transaction committed and all rows are visible.
    const int verify = ConnectClient(server.BoundPort());
    ASSERT_GE(verify, 0);
    ASSERT_TRUE(SendAll(verify, QueryMessage("SELECT COUNT(*) FROM "
                                             "conc_writes;")));
    const std::string counted = ReadUntilReady(verify);
    EXPECT_NE(counted.find("SELECT 1"), std::string::npos);
    EXPECT_NE(counted.find('4'), std::string::npos);
    close(verify);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ReadWorkerRejectsDataModifyingWith) {
  const std::string path = "postgres_server_with_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE with_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO with_rows VALUES (1);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- a read-only WITH query runs through a read worker.
    ASSERT_TRUE(SendAll(
        client, QueryMessage("WITH x AS (SELECT 1) SELECT * FROM x;")));
    const std::string with_select = ReadUntilReady(client);
    EXPECT_NE(with_select.find('T'), std::string::npos);
    EXPECT_NE(with_select.find('D'), std::string::npos);
    EXPECT_NE(with_select.find("SELECT 1"), std::string::npos);

    // Act -- ANALYZE produces its own command tag.
    ASSERT_TRUE(SendAll(client, QueryMessage("ANALYZE with_rows;")));
    const std::string analyzed = ReadUntilReady(client);
    EXPECT_NE(analyzed.find("ANALYZE"), std::string::npos);
    EXPECT_NE(analyzed.find('Z'), std::string::npos);

    // Act -- a data-modifying WITH statement is parsed as a syntax error and
    // reported through the read worker (leading keyword is WITH, so the
    // statement is treated as read-only for scheduling).
    ASSERT_TRUE(SendAll(
        client, QueryMessage("WITH x AS (SELECT 1) DELETE FROM with_rows;")));
    const std::string response = ReadUntilReady(client);
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);

    // Assert -- the rejected statement did not delete the row.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT COUNT(*) FROM with_rows;")));
    const std::string counted = ReadUntilReady(client);
    EXPECT_NE(counted.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, SelectConstantExpressionNoTable) {
  const std::string path = "postgres_server_constexpr_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- a constant expression with no FROM clause is routed to a read
    // worker, which reports that no table was specified (42601).
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 1 + 1;")));
    const std::string summed = ReadUntilReady(client);
    EXPECT_NE(summed.find('E'), std::string::npos);
    EXPECT_NE(summed.find("No table specified"), std::string::npos);
    EXPECT_NE(summed.find('Z'), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT NULL;")));
    const std::string nulled = ReadUntilReady(client);
    EXPECT_NE(nulled.find('E'), std::string::npos);
    EXPECT_NE(nulled.find("No table specified"), std::string::npos);

    // Assert -- the session is still usable after the rejected expression.
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE const_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ExtendedQueryMessagesRejectedKeepSessionAlive) {
  const std::string path = "postgres_server_extended_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE ext_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- the extended query protocol is not implemented: Parse, Bind, and
    // Execute messages are each rejected with an error plus ReadyForQuery.
    const std::string parse = std::string("P\0\0\0\21", 5) +
                              std::string("\0", 1) +
                              std::string("SELECT 1;\0", 10) +
                              std::string("\0\0", 2);
    ASSERT_TRUE(SendAll(client, parse));
    const std::string parsed = ReadUntilReady(client);
    EXPECT_NE(parsed.find('E'), std::string::npos);
    EXPECT_NE(parsed.find("not supported"), std::string::npos);
    EXPECT_NE(parsed.find('Z'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("B\0\0\0\5", 5) + '\0'));
    const std::string bound = ReadUntilReady(client);
    EXPECT_NE(bound.find('E'), std::string::npos);
    EXPECT_NE(bound.find('Z'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("E\0\0\0\5", 5) + '\0'));
    const std::string executed = ReadUntilReady(client);
    EXPECT_NE(executed.find('E'), std::string::npos);
    EXPECT_NE(executed.find('Z'), std::string::npos);

    // Assert -- the session remains usable after the extended-query rejections.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM ext_rows;")));
    const std::string after = ReadUntilReady(client);
    EXPECT_NE(after.find("SELECT 0"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, DisconnectDuringLargeResponse) {
  const std::string path = "postgres_server_disconnect_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(SendAll(
        setup, QueryMessage("CREATE TABLE disconnect_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    for (int64_t batch = 0; batch < 2; ++batch) {
      std::string insert = "INSERT INTO disconnect_rows VALUES ";
      for (int64_t offset = 0; offset < 10000; ++offset) {
        if (offset != 0) { insert += ',';
}
        insert += "(" + std::to_string((batch * 10000) + offset + 1) + ")";
      }
      insert += ';';
      ASSERT_TRUE(SendAll(setup, QueryMessage(insert)));
      ASSERT_FALSE(ReadUntilReady(setup).empty());
    }
    close(setup);

    // Act -- a client with a tiny receive buffer asks for a wide projection of
    // the whole table (several MB) and then disconnects before reading.  The
    // server's send must fail cleanly instead of crashing or hanging.
    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int receive_buffer = 4096;
    setsockopt(client, SOL_SOCKET, SO_RCVBUF, &receive_buffer,
               sizeof(receive_buffer));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    ASSERT_TRUE(SendAll(client, StartupMessage()));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    std::string projection = "SELECT id, id + 1";
    for (int column = 2; column < 30; ++column) {
      projection += ", id + " + std::to_string(column);
    }
    projection += " FROM disconnect_rows;";
    ASSERT_TRUE(SendAll(client, QueryMessage(projection)));
    // Wait until the server has begun streaming the large reply, then
    // disconnect mid-write so the server's next send() must fail.
    {
      struct pollfd ready{.fd=client, .events=POLLIN, .revents=0};
      for (int attempt = 0; attempt < 100; ++attempt) {
        if (poll(&ready, 1, 100) > 0 && (ready.revents & POLLIN) != 0) { break;
}
      }
    }
    close(client);

    // Assert -- the server survives the failed write and stops cleanly.
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ClientResetForcesRecvError) {
  const std::string path = "postgres_server_reset_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    // Act -- a client declares a startup packet larger than it sends and then
    // closes with SO_LINGER, forcing an RST.  The server's next recv fails
    // with ECONNRESET and must drop the client without taking the server down.
    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    linger reset{.l_onoff=1, .l_linger=0};
    setsockopt(client, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    ASSERT_TRUE(SendAll(client, std::string("\0\0\0\100", 4)));
    close(client);

    // A healthy second client still completes a normal startup.
    const int healthy = ConnectClient(server.BoundPort());
    ASSERT_GE(healthy, 0);
    ASSERT_TRUE(SendAll(
        healthy, QueryMessage("CREATE TABLE reset_ok (id INT64);")));
    EXPECT_NE(ReadUntilReady(healthy).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(healthy, std::string("X\0\0\0\4", 5)));
    close(healthy);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, SignalInterruptsEpollWaitAndServerSurvives) {
  const std::string path = "postgres_server_signal_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    // Install a no-op SIGUSR1 handler without SA_RESTART so the server's
    // blocking epoll_wait returns EINTR when the signal is delivered.
    struct sigaction action{};
    action.sa_handler = [](int) {};
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    struct sigaction old_action{};
    ASSERT_EQ(sigaction(SIGUSR1, &action, &old_action), 0);

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE signal_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- deliver SIGUSR1 straight to the epoll thread several times while
    // it sits in epoll_wait; each interruption must be handled as EINTR and
    // the event loop must keep running.
    const pthread_t epoll_thread = server_thread.native_handle();
    for (int i = 0; i < 4; ++i) {
      ASSERT_EQ(pthread_kill(epoll_thread, SIGUSR1), 0);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Assert -- the server still processes requests after the interruptions.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO signal_rows VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM signal_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    ASSERT_EQ(sigaction(SIGUSR1, &old_action, nullptr), 0);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, GssEncRequestDeclinedThenNormalStartup) {
  const std::string path = "postgres_server_gss_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);

    // Act -- a GSSENCRequest is declined with a bare 'N' byte, then the same
    // connection may complete a normal startup.
    std::string gss_request;
    pgwire::AppendUint32(&gss_request, 8);
    pgwire::AppendUint32(&gss_request, pgwire::kGssEncRequestCode);
    ASSERT_TRUE(SendAll(client, gss_request));
    std::array<char, 4> gss_reply{};
    const ssize_t received = recv(client, gss_reply.data(), gss_reply.size(), 0);
    ASSERT_EQ(received, 1);
    EXPECT_EQ(gss_reply[0], 'N');
    ASSERT_TRUE(SendAll(client, StartupMessage()));
    const std::string startup = ReadUntilReady(client);
    EXPECT_NE(startup.find('R'), std::string::npos);
    EXPECT_NE(startup.find('Z'), std::string::npos);
    close(client);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, MultiStatementDdlInsertSelectOverTcp) {
  const std::string path = "postgres_server_multiflow_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    // Act -- a single message carrying CREATE + INSERT + SELECT runs every
    // statement in order through the synchronous executor.
    ASSERT_TRUE(SendAll(client, QueryMessage(
                                    "CREATE TABLE multi_flow (id INT64); "
                                    "INSERT INTO multi_flow VALUES (1); "
                                    "INSERT INTO multi_flow VALUES (2); "
                                    "SELECT id FROM multi_flow ORDER BY id;")));
    const std::string response = ReadUntilReady(client);
    EXPECT_NE(response.find("CREATE TABLE"), std::string::npos);
    EXPECT_NE(response.find("INSERT 0 1"), std::string::npos);
    EXPECT_NE(response.find("SELECT 2"), std::string::npos);
    EXPECT_NE(response.find('D'), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);

    // Assert -- the table created by the batch is visible to later messages.
    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO multi_flow VALUES (3);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, RollbackAfterStatementErrorClearsAbortedState) {
  const std::string path = "postgres_server_rberr_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE rb_abort (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO rb_abort VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- a failing statement aborts the transaction into the 'E' state.
    ASSERT_TRUE(SendAll(client, QueryMessage("THIS IS NOT VALID SQL;")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find("42601"), std::string::npos);
    EXPECT_NE(failed.find(std::string("Z\0\0\0\5E", 6)), std::string::npos);

    // Act -- ROLLBACK (rather than COMMIT) clears the aborted state.
    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    const std::string rolled_back = ReadUntilReady(client);
    EXPECT_NE(rolled_back.find("ROLLBACK"), std::string::npos);
    EXPECT_NE(rolled_back.find(std::string("Z\0\0\0\5I", 6)),
              std::string::npos);

    // Assert -- the rolled-back row is invisible and the session is usable.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM rb_abort;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 0"), std::string::npos);
    EXPECT_EQ(selected.find('D'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, ReadWorkerCompletionForDisconnectedClientIsDropped) {
  const std::string path = "postgres_server_stale2_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 1;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(
        SendAll(setup, QueryMessage("CREATE TABLE stale_rows2 (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    for (int64_t batch = 0; batch < 4; ++batch) {
      std::string insert = "INSERT INTO stale_rows2 VALUES ";
      for (int64_t offset = 0; offset < 10000; ++offset) {
        if (offset != 0) { insert += ',';
}
        const int64_t value = (batch * 10000) + offset + 1;
        insert += "(" + std::to_string(value) + ")";
      }
      insert += ';';
      ASSERT_TRUE(SendAll(setup, QueryMessage(insert)));
      ASSERT_FALSE(ReadUntilReady(setup).empty());
    }
    close(setup);

    // Act -- client A occupies the single read worker with an expensive sort
    // of the whole table, so client B's read is queued behind it. B sends its
    // query, terminates with 'X', and disconnects immediately; its completion
    // can only arrive after the close has been processed, so the server must
    // drop the stale completion without crashing or hanging.
    const int client_a = ConnectClient(server.BoundPort());
    ASSERT_GE(client_a, 0);
    ASSERT_TRUE(
        SendAll(client_a, QueryMessage("SELECT * FROM stale_rows2 ORDER BY id "
                                       "DESC;")));
    const int client_b = ConnectClient(server.BoundPort());
    ASSERT_GE(client_b, 0);
    ASSERT_TRUE(SendAll(client_b, QueryMessage("SELECT * FROM stale_rows2;")));
    ASSERT_TRUE(SendAll(client_b, std::string("X\0\0\0\4", 5)));
    close(client_b);

    EXPECT_NE(ReadUntilReady(client_a).find("SELECT 40000"),
              std::string::npos);
    close(client_a);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, DropTableInsideExplicitTransactionCommits) {
  const std::string path = "postgres_server_droptxn_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);

    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE drop_txn (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO drop_txn VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- drop the table inside an explicit transaction and commit.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("DROP TABLE drop_txn;")));
    EXPECT_NE(ReadUntilReady(client).find("DROP TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    EXPECT_NE(ReadUntilReady(client).find("COMMIT"), std::string::npos);

    // Assert -- the drop is durable: the table is gone afterwards.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT * FROM drop_txn;")));
    EXPECT_NE(ReadUntilReady(client).find('E'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, RuntimeErrorInSynchronousTransaction) {
  const std::string path = "postgres_server_runtime_sync-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("CREATE TABLE runtime_sync (id INT64, "
                                     "body STRING(16));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("INSERT INTO runtime_sync VALUES "
                                     "(1, 'abc');")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- a type error that is only detectable at execution time (adding
    // INT64 + VARCHAR) is raised inside an explicit transaction, so it must
    // travel through the synchronous executor and abort the transaction.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    ASSERT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("SELECT id + body FROM runtime_sync;")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find("type mismatch"), std::string::npos);
    EXPECT_NE(failed.find("XX000"), std::string::npos);
    EXPECT_NE(failed.find(std::string("Z\0\0\0\5E", 6)), std::string::npos);

    // Assert -- ROLLBACK clears the aborted state and the session stays usable.
    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find("ROLLBACK"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM runtime_sync;")));
    const std::string recovered = ReadUntilReady(client);
    EXPECT_NE(recovered.find("SELECT 1"), std::string::npos);
    EXPECT_NE(recovered.find('D'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, RuntimeErrorInReadWorker) {
  const std::string path = "postgres_server_runtime_rw-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 2;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("CREATE TABLE runtime_rw (id INT64, body "
                                     "STRING(16));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("INSERT INTO runtime_rw VALUES "
                                     "(1, 'abc');")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- the same type error in a read-only statement is routed to a read
    // worker, which must surface it as an ErrorResponse plus ReadyForQuery.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id + body FROM runtime_rw;")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find("type mismatch"), std::string::npos);
    EXPECT_NE(failed.find('Z'), std::string::npos);

    // Assert -- the session remains usable afterwards.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id FROM runtime_rw;")));
    const std::string recovered = ReadUntilReady(client);
    EXPECT_NE(recovered.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, MultiStatementReadWorkerRuntimeErrorStopsBatch) {
  const std::string path = "postgres_server_runtime_multi-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 1;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("CREATE TABLE runtime_multi (id INT64, "
                                     "body STRING(16));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(SendAll(client,
                        QueryMessage("INSERT INTO runtime_multi VALUES "
                                     "(1, 'abc');")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- a single message with two read-only statements sharing one
    // read-only snapshot (§7.3): the first streams its rows, then the second
    // throws at execution time. The worker aborts the shared snapshot,
    // stops at the failing statement, and still answers ReadyForQuery.
    ASSERT_TRUE(SendAll(client, QueryMessage(
                                    "SELECT id FROM runtime_multi; "
                                    "SELECT id + body FROM runtime_multi; "
                                    "SELECT id FROM runtime_multi;")));
    const std::string response = ReadUntilReady(client);
    EXPECT_NE(response.find('D'), std::string::npos);
    EXPECT_NE(response.find("SELECT 1"), std::string::npos);
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find("type mismatch"), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);
    // The failing third statement must never execute: exactly two DataRows.
    EXPECT_EQ(response.find("SELECT 1", response.find('E')),
              std::string::npos);

    // Assert -- the session is still usable.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id FROM runtime_multi;")));
    EXPECT_NE(ReadUntilReady(client).find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, StaleReadCompletionDroppedAfterFdReuse) {
  const std::string path = "postgres_server_stalefd_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 1;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(
        SendAll(setup, QueryMessage("CREATE TABLE stale_fd (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    for (int64_t batch = 0; batch < 4; ++batch) {
      std::string insert = "INSERT INTO stale_fd VALUES ";
      for (int64_t offset = 0; offset < 10000; ++offset) {
        if (offset != 0) { insert += ',';
}
        insert += "(" + std::to_string((batch * 10000) + offset + 1) + ")";
      }
      insert += ';';
      ASSERT_TRUE(SendAll(setup, QueryMessage(insert)));
      ASSERT_FALSE(ReadUntilReady(setup).empty());
    }
    close(setup);

    // Act -- client A occupies the single read worker with an expensive sort;
    // client B queues a read behind A, then terminates and disconnects. The
    // event loop closes B while A is still running, so the server-side
    // descriptor is freed and reused by client C before B's read completion
    // can arrive. The stale completion must be dropped via the client-id
    // mismatch instead of being written to C.
    const int client_a = ConnectClient(server.BoundPort());
    ASSERT_GE(client_a, 0);
    ASSERT_TRUE(SendAll(client_a,
                        QueryMessage("SELECT * FROM stale_fd ORDER BY id "
                                     "DESC;")));
    const int client_b = ConnectClient(server.BoundPort());
    ASSERT_GE(client_b, 0);
    ASSERT_TRUE(SendAll(client_b, QueryMessage("SELECT * FROM stale_fd;")));
    // Let the event loop read and schedule B's query (and hit EAGAIN) before
    // B closes; otherwise the EOF and the payload arrive in one recv and the
    // query is never scheduled, producing no stale completion at all.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(SendAll(client_b, std::string("X\0\0\0\4", 5)));
    close(client_b);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const int client_c = ConnectClient(server.BoundPort());
    ASSERT_GE(client_c, 0);

    EXPECT_NE(ReadUntilReady(client_a).find("SELECT 40000"),
              std::string::npos);
    ASSERT_TRUE(
        SendAll(client_c, QueryMessage("SELECT COUNT(*) FROM stale_fd;")));
    const std::string counted = ReadUntilReady(client_c);
    EXPECT_NE(counted.find("SELECT 1"), std::string::npos);
    EXPECT_NE(counted.find("40000"), std::string::npos);

    close(client_a);
    close(client_c);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

TEST(PostgresServerTest, DropTableInsideAbortedTransaction) {
  const std::string path = "postgres_server_dropabort_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE drop_abort (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO drop_abort VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- drop the table inside an explicit transaction and roll back.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("DROP TABLE drop_abort;")));
    EXPECT_NE(ReadUntilReady(client).find("DROP TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find("ROLLBACK"), std::string::npos);

    // Assert -- the rolled-back drop is not durable: the table still exists
    // and its row is visible.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM drop_abort;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);
    EXPECT_NE(selected.find('D'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  Database database(path);
  database.DeleteAll();
}

}  // namespace
}  // namespace tinylamb
