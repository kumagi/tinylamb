/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX sigaction/sigemptyset
#include <signal.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/socket.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "server/postgres_protocol.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool SendAll(int fd, const std::string& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t sent = platform::SendNoSignal(fd, bytes.data() + offset,
                                                bytes.size() - offset);
    if (sent <= 0) {
      return false;
    }
    offset += static_cast<size_t>(sent);
  }
  return true;
}

std::string ReadUntilReady(int fd) {
  std::string result;
  while (true) {
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    if (poll(&descriptor, 1, 5000) <= 0) {
      return {};
    }
    std::array<char, 4096> buffer{};
    const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
    if (received <= 0) {
      return {};
    }
    result.append(buffer.data(), static_cast<size_t>(received));

    size_t cursor = 0;
    while (cursor + 5 <= result.size()) {
      const uint32_t length = pgwire::ReadUint32(result, cursor + 1).Value();
      if (length < 4 || cursor + 1 + length > result.size()) {
        break;
      }
      if (result[cursor] == 'Z') {
        return result;
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

// Concatenate the payloads of every ErrorResponse in a captured reply.
// Substrings like "SELECT" contain the letter 'E', so negative assertions
// must walk the frame headers instead of searching raw bytes.
std::string ErrorPayloads(const std::string& bytes) {
  std::string errors;
  size_t cursor = 0;
  while (cursor + 5 <= bytes.size()) {
    const StatusOr<uint32_t> length = pgwire::ReadUint32(bytes, cursor + 1);
    if (!length.HasValue() || length.Value() < 4 ||
        cursor + 1 + length.Value() > bytes.size()) {
      break;
    }
    if (bytes[cursor] == 'E') {
      errors.append(bytes, cursor + 5, length.Value() - 4);
    }
    cursor += 1 + length.Value();
  }
  return errors;
}

// Count ReadyForQuery ('Z') frames by walking wire frames: 'Z' appears in
// data payloads too, so raw substring counts are unreliable.
size_t CountReadyFrames(const std::string& bytes) {
  size_t count = 0;
  size_t cursor = 0;
  while (cursor + 5 <= bytes.size()) {
    const StatusOr<uint32_t> length = pgwire::ReadUint32(bytes, cursor + 1);
    if (!length.HasValue() || length.Value() < 4 ||
        cursor + 1 + length.Value() > bytes.size()) {
      break;
    }
    if (bytes[cursor] == 'Z') {
      ++count;
    }
    cursor += 1 + length.Value();
  }
  return count;
}

int ConnectClient(uint16_t port) {
  const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  if (client < 0) {
    return -1;
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

class LateFailureExecutor final : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* dst, size_t /*max_rows*/) override {
    dst->Reset();
    ++next_calls;
    if (next_calls == 1) {
      dst->Append(Row({Value("Insert Rows"), Value(int64_t{7})}));
      return 1;
    }
    FailWith(StatusError(StatusCode::kRuntimeError, "late executor failure"));
    return 0;
  }

  void Dump(std::ostream& output, int /*indent*/) const override {
    output << "LateFailureExecutor";
  }

  int next_calls{0};
};

TEST(PostgresServerTest, NonSelectCommandDrainRejectsLateFailure) {
  auto executor = std::make_shared<LateFailureExecutor>();
  QueryResult result(executor, StatementType::kInsert, {});
  int64_t affected_rows = 0;
  EXPECT_EQ(DrainCommandResult(result, &affected_rows), Status::kRuntimeError);
  EXPECT_EQ(affected_rows, 7);
  EXPECT_EQ(executor->next_calls, 2);
}

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

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
      if (value != 1) {
        insert += ',';
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
    clients.reserve(8);
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
        client,
        QueryMessage("CREATE TABLE txns (id INT64, body STRING(32));")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- open a transaction, insert, and roll back
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO txns VALUES (1, 'a');")));
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("UPDATE txns SET id = 3 WHERE id = 2;")));
    EXPECT_NE(ReadUntilReady(client).find("UPDATE 1"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("DELETE FROM txns;")));
    EXPECT_NE(ReadUntilReady(client).find("DELETE 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, SingleMessageImplicitBlockRollsBackWithControl) {
  const std::string path =
      "postgres_server_implicit_txn_test-" + RandomString();
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

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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

    ASSERT_TRUE(SendAll(
        client, QueryMessage(
                    "CREATE TABLE implicit_txn (id INT64, body STRING(32));")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- UPDATE followed by ROLLBACK in ONE Query message: PostgreSQL
    // runs the statement inside an implicit block that the ROLLBACK undoes
    // (the legacy per-statement commit made this a silent no-op).
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO implicit_txn VALUES "
                                             "(1, 'keep');"
                                             "UPDATE implicit_txn SET body = "
                                             "'lost' WHERE id = 1;"
                                             "ROLLBACK;")));
    const std::string rollback_reply = ReadUntilReady(client);
    EXPECT_NE(rollback_reply.find("INSERT 0 1"), std::string::npos);
    EXPECT_NE(rollback_reply.find("UPDATE 1"), std::string::npos);
    EXPECT_NE(rollback_reply.find("ROLLBACK"), std::string::npos);

    // Assert -- the WHOLE implicit block is undone (PostgreSQL treats the
    // single Query message as one implicit transaction), so neither the
    // INSERT nor the UPDATE survived.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id, body FROM implicit_txn;")));
    const std::string after_rollback = ReadUntilReady(client);
    EXPECT_NE(after_rollback.find("SELECT 0"), std::string::npos);
    EXPECT_EQ(after_rollback.find("keep"), std::string::npos);
    EXPECT_EQ(after_rollback.find("lost"), std::string::npos);

    // Act -- INSERT followed by COMMIT in one message persists the row
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO implicit_txn VALUES "
                                             "(2, 'kept');"
                                             "COMMIT;")));
    const std::string commit_reply = ReadUntilReady(client);
    EXPECT_NE(commit_reply.find("INSERT 0 1"), std::string::npos);
    EXPECT_NE(commit_reply.find("COMMIT"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id, body FROM implicit_txn;")));
    const std::string after_commit = ReadUntilReady(client);
    EXPECT_NE(after_commit.find("kept"), std::string::npos);

    // Act -- BEGIN adopts the implicit block: statements before BEGIN and
    // after it share one transaction that ROLLBACK undoes.
    ASSERT_TRUE(SendAll(client, QueryMessage("UPDATE implicit_txn SET body = "
                                             "'gone' WHERE id = 1;"
                                             "BEGIN;"
                                             "UPDATE implicit_txn SET body = "
                                             "'also-gone' WHERE id = 2;"
                                             "ROLLBACK;")));
    const std::string begin_rollback = ReadUntilReady(client);
    EXPECT_NE(begin_rollback.find("BEGIN"), std::string::npos);
    EXPECT_NE(begin_rollback.find("ROLLBACK"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id, body FROM implicit_txn;")));
    const std::string after_begin_rollback = ReadUntilReady(client);
    EXPECT_NE(after_begin_rollback.find("kept"), std::string::npos);
    EXPECT_EQ(after_begin_rollback.find("gone"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    // Act -- send an unsupported frontend message type followed by Sync;
    // the error is answered immediately but ReadyForQuery is owed by Sync
    // (one Sync must close with exactly one RFQ).
    ASSERT_TRUE(SendAll(
        client, std::string("F\0\0\0\4", 5) + std::string("S\0\0\0\4", 5)));
    const std::string unsupported = ReadUntilReady(client);
    // Assert -- the server replies with an error and a ReadyForQuery
    EXPECT_NE(unsupported.find('E'), std::string::npos);
    EXPECT_NE(unsupported.find('Z'), std::string::npos);
    EXPECT_NE(unsupported.find("not supported"), std::string::npos);

    // Positions refer to the full Query message, including earlier SQL,
    // UTF-8 characters, CRLFs and leading whitespace removed by the splitter.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT '雪';\r\n \tSELECT\n\t)")));
    const std::string multiline = ErrorPayloads(ReadUntilReady(client));
    EXPECT_NE(multiline.find(std::string("P24\0", 4)), std::string::npos)
        << multiline;

    // EXPLAIN sends only its wrapped query to the parser.
    ASSERT_TRUE(SendAll(client, QueryMessage(" \tEXPLAIN SELECT '雪', )")));
    const std::string explain = ErrorPayloads(ReadUntilReady(client));
    EXPECT_NE(explain.find(std::string("P23\0", 4)), std::string::npos)
        << explain;

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
      pollfd descriptor{.fd = client, .events = POLLIN, .revents = 0};
      if (poll(&descriptor, 1, 5000) <= 0) {
        break;
      }
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT * FROM does_not_exist;")));
    const std::string response = ReadUntilReady(client);
    // Assert -- the worker returns an ErrorResponse plus ReadyForQuery
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
      const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
      if (client < 0) {
        return -1;
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
        pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
        if (poll(&descriptor, 1, 5000) <= 0) {
          break;
        }
        std::array<char, 4096> buffer{};
        const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
        if (received <= 0) {
          break;
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
      pgwire::AppendUint32(&ssl_request, pgwire::kSSLRequestCode);
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
          client,
          MakeStartup(0x00040000U,
                      std::string("user\0test\0database\0test\0\0", 25))));
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
      ASSERT_TRUE(
          SendAll(client, MakeStartup(pgwire::kProtocolVersion30,
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
          client,
          MakeStartup(0x00030001U,
                      std::string("user\0test\0_pq_.foo\0bar\0\0", 24))));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE abort_state (id INT64);")));
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
    EXPECT_NE(aborted.find("current transaction is aborted, commands ignored "
                           "until end of transaction block"),
              std::string::npos);

    // T1129/T1697: a BEGIN in the aborted state is NOT the 25P02 rejection
    // path any more -- PostgreSQL answers a NOTICE (25001) plus the BEGIN
    // tag and keeps the aborted state. The generic rejection wording stays
    // pinned through the SELECT above.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    const std::string begin_notice = ReadUntilReady(client);
    EXPECT_NE(begin_notice.find("there is already a transaction in progress"),
              std::string::npos);
    EXPECT_NE(begin_notice.find("25001"), std::string::npos);
    EXPECT_EQ(begin_notice.find("25P02"), std::string::npos);
    EXPECT_EQ(begin_notice.find("issue ROLLBACK"), std::string::npos);
    // The block stays aborted: the next real command still gets 25P02.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 2;")));
    EXPECT_NE(ReadUntilReady(client).find("25P02"), std::string::npos);

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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
      pollfd descriptor{.fd = client, .events = POLLIN, .revents = 0};
      if (poll(&descriptor, 1, 5000) <= 0) {
        break;
      }
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE sync_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO sync_rows VALUES (1);")));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, ViewCommandTagsOverTcp) {
  const std::string path = "postgres_server_view_tag_test-" + RandomString();
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
    for (const auto& [sql, tag] :
         std::vector<std::pair<std::string, std::string>>{
             {"CREATE VIEW protocol_view AS SELECT 1 AS id", "CREATE VIEW"},
             {"DROP VIEW protocol_view", "DROP VIEW"}}) {
      ASSERT_TRUE(SendAll(client, QueryMessage(sql)));
      const std::string response = ReadUntilReady(client);
      EXPECT_NE(response.find(tag), std::string::npos) << sql;
    }
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database = Database::Create(path).MoveValue();
  database->DeleteAll();
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE drop_me (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO drop_me VALUES (1);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    // Act -- drop the table; the command tag must be reported to the client
    ASSERT_TRUE(SendAll(client, QueryMessage("DROP TABLE drop_me;")));
    const std::string dropped = ReadUntilReady(client);
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE txn_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO txn_rows VALUES (7);")));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
      pollfd descriptor{.fd = client, .events = POLLIN, .revents = 0};
      if (poll(&descriptor, 1, 5000) <= 0) {
        break;
      }
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
      pollfd descriptor{.fd = client, .events = POLLIN, .revents = 0};
      if (poll(&descriptor, 1, 5000) <= 0) {
        break;
      }
      std::array<char, 4096> buffer{};
      const ssize_t received = recv(client, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, ServerDestructorClosesLiveClients) {
  const std::string path = "postgres_server_dtor_test-" + RandomString();
  int client = -1;
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
    client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  // Assert -- the destructor closed the still-registered client socket
  // (the server reaches the connection and the peer observes EOF).
  ASSERT_GE(client, 0);
  // POLLIN only proves the socket is readable; the peer may have queued reply
  // bytes before closing, so drain until recv reports the EOF itself (0)
  // instead of treating the first chunk as the whole story.
  ssize_t received = -1;
  for (int attempt = 0; attempt < 8; ++attempt) {
    pollfd descriptor{.fd = client, .events = POLLIN, .revents = 0};
    if (poll(&descriptor, 1, 5000) <= 0) {
      break;
    }
    std::array<char, 4096> buffer{};
    received = recv(client, buffer.data(), buffer.size(), 0);
    if (received <= 0) {
      break;
    }
  }
  close(client);
  ASSERT_NE(received, -1);
  EXPECT_EQ(received, 0);
  {
    auto database_holder = Database::Create(path).MoveValue();
    CHECK(database_holder != nullptr);
    Database& database = *database_holder;
    database.DeleteAll();
  }
}

TEST(PostgresServerTest, ListenFailsWhenPortAlreadyInUse) {
  const std::string path = "postgres_server_bindfail_test-" + RandomString();
  const int conflict = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
  ASSERT_EQ(
      getsockname(conflict, reinterpret_cast<sockaddr*>(&bound), &bound_size),
      0);

  {
    PostgresServerOptions options;
    options.listen_address = "127.0.0.1";
    options.port = ntohs(bound.sin_port);
    PostgresServer server(path, options);
    std::string listen_error;
    // Act -- binding the same address:port must fail cleanly
    EXPECT_FALSE(server.Listen(&listen_error)) << listen_error;
    // Assert -- the error names the bind step
    EXPECT_NE(listen_error.find("could not bind"), std::string::npos);
  }

  close(conflict);
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, ListenOnIpv6Loopback) {
  const std::string path = "postgres_server_ipv6_test-" + RandomString();
  {
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
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO missing_table VALUES (1);")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find('Z'), std::string::npos);

    // Assert -- the session is usable again afterwards
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE ok_now (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, ListenFailsOnUnresolvableListenAddress) {
  const std::string path = "postgres_server_badaddr_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.listen_address = "256.256.256.256";
    options.port = 0;
    PostgresServer server(path, options);
    std::string listen_error;
    // Act -- name resolution for the invalid address must fail up front
    EXPECT_FALSE(server.Listen(&listen_error));
    // Assert -- the error names the getaddrinfo step
    EXPECT_NE(listen_error.find("getaddrinfo"), std::string::npos);
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE alias_rows (id INT64);")));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// Multi-statement simple query: exercises sequential DDL/DML/SELECT in one
// message.
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE multi_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- a mixed write + read message runs every statement in order
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO multi_rows VALUES (1); "
                                     "SELECT id FROM multi_rows;")));
    const std::string mixed = ReadUntilReady(client);
    EXPECT_NE(mixed.find("INSERT 0 1"), std::string::npos);
    EXPECT_NE(mixed.find("SELECT 1"), std::string::npos);
    EXPECT_NE(mixed.find('Z'), std::string::npos);

    // Act -- an all-read-only multi-statement message uses a read worker
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM multi_rows; "
                                             "SELECT id FROM multi_rows;")));
    const std::string read_only = ReadUntilReady(client);
    EXPECT_NE(read_only.find("SELECT 1"), std::string::npos);
    EXPECT_NE(read_only.find('Z'), std::string::npos);

    // Act -- a multi-statement message with a failing statement stops the
    // remaining statements from executing
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO missing_table VALUES (1); "
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE atomic_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    // Act -- the first INSERT succeeds and reports its tag, then the second
    // statement fails; the whole implicit transaction must roll back.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO atomic_rows VALUES (1); "
                                     "INSERT INTO missing_table VALUES (2);")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find("ERROR"), std::string::npos);

    // Assert -- nothing from the failed message is durable.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM atomic_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 0"), std::string::npos);
    EXPECT_EQ(selected.find('D'), std::string::npos);

    // Act -- a fully successful multi-statement message commits everything.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO atomic_rows VALUES (10); "
                                     "INSERT INTO atomic_rows VALUES (20);")));
    const std::string committed = ReadUntilReady(client);
    EXPECT_EQ(committed.find("ERROR"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT COUNT(*) FROM atomic_rows;")));
    const std::string counted = ReadUntilReady(client);
    EXPECT_NE(counted.find("SELECT 1"), std::string::npos);
    EXPECT_NE(counted.find('2'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
        if (offset != 0) {
          insert += ',';
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

    EXPECT_NE(ReadUntilReady(client_a).find("SELECT 30000"), std::string::npos);
    close(client_a);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
      const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
      if (client < 0) {
        return -1;
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
        if (offset != 0) {
          insert += ',';
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
      // Sanitizer builds on loaded CI runners can spend over a minute
      // computing the projection before the first reply byte moves; a fixed
      // window here gave up (and pipelined the COUNT below) far too early.
      const auto first_byte_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(60);
      while (std::chrono::steady_clock::now() < first_byte_deadline) {
        struct pollfd ready{.fd = client, .events = POLLIN, .revents = 0};
        if (poll(&ready, 1, 100) > 0 && (ready.revents & POLLIN) != 0) {
          break;
        }
      }
    }
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT COUNT(*) FROM queue_rows;")));

    auto ReadUntil = [](int fd, const std::string& needle) {
      std::string result;
      // Sanitizer builds serialize the 18 MB projection slowly enough to
      // open minute-long no-data windows on loaded CI runners; a fixed 30 s
      // gap there aborted the read before the queued replies arrived even
      // though the server was still flushing. A generous overall deadline
      // keeps the queueing assertions, not the machine speed, under test.
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(180);
      while (result.find(needle) == std::string::npos &&
             std::chrono::steady_clock::now() < deadline) {
        pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
        if (poll(&descriptor, 1, 30000) <= 0) {
          continue;
        }
        std::array<char, 4096> buffer{};
        const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
        if (received <= 0) {
          break;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE cycle_rows (id INT64);")));
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE cycle_rows (id INT64, body "
                                     "STRING(16));")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(
        client, QueryMessage("INSERT INTO cycle_rows VALUES (2, 'two');")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id, body FROM cycle_rows;")));
    const std::string selected = ReadUntilReady(client);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);
    EXPECT_NE(selected.find("two"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(setup, QueryMessage("CREATE TABLE conc_writes (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    close(setup);

    // Act -- four sessions each hold an open write transaction at the same
    // time and commit after all of them have inserted.
    std::vector<int> clients;
    clients.reserve(4);
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// Every write statement of every session runs on the single event-loop
// thread, so a writer that WAITED for another session's write intent blocked
// the only thread able to commit the holder: each row conflict stalled the
// whole server for the 5 s write-intent escape hatch.  The conflicting
// statement must fail at once and the holder must commit normally.
TEST(PostgresServerTest, RowConflictFailsFastInsteadOfStallingTheLoop) {
  const std::string path = "postgres_server_conflict_test-" + RandomString();
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

    const int holder = ConnectClient(server.BoundPort());
    const int waiter = ConnectClient(server.BoundPort());
    ASSERT_GE(holder, 0);
    ASSERT_GE(waiter, 0);
    ASSERT_TRUE(SendAll(
        holder, QueryMessage("CREATE TABLE hot (id INT64 PRIMARY KEY, v "
                             "INT64);")));
    ASSERT_FALSE(ReadUntilReady(holder).empty());
    ASSERT_TRUE(
        SendAll(holder, QueryMessage("INSERT INTO hot VALUES (1, 0);")));
    ASSERT_FALSE(ReadUntilReady(holder).empty());

    ASSERT_TRUE(SendAll(holder, QueryMessage("BEGIN;")));
    ASSERT_FALSE(ReadUntilReady(holder).empty());
    ASSERT_TRUE(SendAll(
        holder, QueryMessage("UPDATE hot SET v = v + 1 WHERE id = 1;")));
    EXPECT_NE(ReadUntilReady(holder).find("UPDATE 1"), std::string::npos);

    // Act -- a second session updates the same row while the intent is held.
    const auto begin = std::chrono::steady_clock::now();
    ASSERT_TRUE(SendAll(
        waiter, QueryMessage("UPDATE hot SET v = v + 10 WHERE id = 1;")));
    const std::string conflicted = ReadUntilReady(waiter);
    const auto elapsed = std::chrono::steady_clock::now() - begin;

    // Assert -- the conflict is reported promptly (no multi-second stall)...
    EXPECT_NE(conflicted.find('E'), std::string::npos);
    EXPECT_LT(elapsed, std::chrono::seconds(10));
    // ...and the holder still commits its own update.
    ASSERT_TRUE(SendAll(holder, QueryMessage("COMMIT;")));
    EXPECT_NE(ReadUntilReady(holder).find("COMMIT"), std::string::npos);
    ASSERT_TRUE(
        SendAll(waiter, QueryMessage("SELECT v FROM hot WHERE id = 1;")));
    const std::string selected = ReadUntilReady(waiter);
    EXPECT_NE(selected.find("SELECT 1"), std::string::npos);
    // DataRow cell: 4-byte length 1 followed by the text "1" (v = 0 + 1).
    EXPECT_NE(selected.find(std::string("\0\0\0\x01"
                                        "1",
                                        5)),
              std::string::npos);
    close(holder);
    close(waiter);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  database_holder->DeleteAll();
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE with_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO with_rows VALUES (1);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- the frontend accepts a read-only WITH query.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("WITH x AS (SELECT 1) SELECT * FROM x;")));
    const std::string with_select = ReadUntilReady(client);
    EXPECT_NE(with_select.find('T'), std::string::npos);
    EXPECT_NE(with_select.find('D'), std::string::npos);
    EXPECT_NE(with_select.find("SELECT 1"), std::string::npos);

    // Act -- ANALYZE produces its own command tag.
    ASSERT_TRUE(SendAll(client, QueryMessage("ANALYZE with_rows;")));
    const std::string analyzed = ReadUntilReady(client);
    EXPECT_NE(analyzed.find("ANALYZE"), std::string::npos);
    EXPECT_NE(analyzed.find('Z'), std::string::npos);

    // Act -- a different WITH spelling has no SELECT classification proof.
    // The normal frontend rejects this data-modifying form.
    ASSERT_TRUE(SendAll(
        client, QueryMessage("WITH x AS (SELECT 1) DELETE FROM with_rows;")));
    const std::string response = ReadUntilReady(client);
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);

    // Assert -- the rejected statement did not delete the row.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT COUNT(*) FROM with_rows;")));
    const std::string counted = ReadUntilReady(client);
    EXPECT_NE(counted.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    // Act -- a constant expression with no FROM clause executes through the
    // one-row DummyScan path and returns its value without touching a table.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 1 + 1;")));
    const std::string summed = ReadUntilReady(client);
    ASSERT_FALSE(summed.empty());
    EXPECT_EQ(summed.find("SERROR"), std::string::npos) << summed;
    EXPECT_NE(summed.find('D'), std::string::npos) << summed;
    EXPECT_NE(summed.find("SELECT 1"), std::string::npos) << summed;

    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT NULL;")));
    const std::string nulled = ReadUntilReady(client);
    ASSERT_FALSE(nulled.empty());
    EXPECT_EQ(nulled.find("SERROR"), std::string::npos) << nulled;
    EXPECT_NE(nulled.find('D'), std::string::npos) << nulled;

    // Assert -- the session stays usable after the constant queries.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE const_rows (id INT64);")));
    EXPECT_NE(ReadUntilReady(client).find("CREATE TABLE"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE ext_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- Extended Query Protocol: Parse + Bind (binary result format) +
    // Describe + Execute + Sync executes cleanly and returns binary-encoded
    // DataRow frames; malformed Bind/Execute return protocol errors and keep
    // the session alive across Sync.
    const std::string parse =
        std::string("P\0\0\0\21", 5) + std::string("\0", 1) +
        std::string("SELECT 1;\0", 10) + std::string("\0\0", 2) +
        std::string("S\0\0\0\4", 5);
    ASSERT_TRUE(SendAll(client, parse));
    const std::string parsed = ReadUntilReady(client);
    EXPECT_NE(parsed.find('1'), std::string::npos);  // ParseComplete
    EXPECT_NE(parsed.find('Z'), std::string::npos);

    // Malformed Bind / Execute frames produce 08P01 and keep session alive.
    ASSERT_TRUE(SendAll(client, std::string("B\0\0\0\5", 5) + '\0' +
                                    std::string("S\0\0\0\4", 5)));
    const std::string bound = ReadUntilReady(client);
    EXPECT_NE(bound.find('E'), std::string::npos);
    EXPECT_NE(bound.find('Z'), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("E\0\0\0\5", 5) + '\0' +
                                    std::string("S\0\0\0\4", 5)));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
        if (offset != 0) {
          insert += ',';
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
    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
      struct pollfd ready{.fd = client, .events = POLLIN, .revents = 0};
      for (int attempt = 0; attempt < 100; ++attempt) {
        if (poll(&ready, 1, 100) > 0 && (ready.revents & POLLIN) != 0) {
          break;
        }
      }
    }
    close(client);

    // Assert -- the server survives the failed write and stops cleanly.
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
    ASSERT_GE(client, 0);
    linger reset{.l_onoff = 1, .l_linger = 0};
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
    ASSERT_TRUE(
        SendAll(healthy, QueryMessage("CREATE TABLE reset_ok (id INT64);")));
    EXPECT_NE(ReadUntilReady(healthy).find("CREATE TABLE"), std::string::npos);
    ASSERT_TRUE(SendAll(healthy, std::string("X\0\0\0\4", 5)));
    close(healthy);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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
    pgwire::AppendUint32(&gss_request, pgwire::kGSSEncRequestCode);
    ASSERT_TRUE(SendAll(client, gss_request));
    std::array<char, 4> gss_reply{};
    const ssize_t received =
        recv(client, gss_reply.data(), gss_reply.size(), 0);
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, OnlyDdlOwnerCanWriteWhileItsBlockIsOpen) {
  const std::string path = "postgres_ddl_owner-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string error;
    ASSERT_TRUE(server.Listen(&error)) << error;
    std::jthread worker([&] { server.Run(&error); });
    StopGuard stop_guard{&server};
    const int owner = ConnectClient(server.BoundPort());
    const int other = ConnectClient(server.BoundPort());
    ASSERT_GE(owner, 0);
    ASSERT_GE(other, 0);
    const auto query = [&](int fd, const std::string& sql) {
      EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
      return ReadUntilReady(fd);
    };
    ASSERT_NE(
        query(owner, "CREATE TABLE ddl_target (k INT64)").find("CREATE TABLE"),
        std::string::npos);
    ASSERT_NE(query(owner, "BEGIN; CREATE INDEX ddl_idx ON ddl_target(k)")
                  .find("CREATE INDEX"),
              std::string::npos);
    EXPECT_NE(
        query(owner, "INSERT INTO ddl_target VALUES (1)").find("INSERT 0 1"),
        std::string::npos);
    ASSERT_NE(query(other, "BEGIN").find("BEGIN"), std::string::npos);
    EXPECT_NE(query(other, "INSERT INTO ddl_target VALUES (2)").find("55006"),
              std::string::npos);
    EXPECT_NE(query(other, "ROLLBACK").find("ROLLBACK"), std::string::npos);
    EXPECT_NE(query(owner, "COMMIT").find("COMMIT"), std::string::npos);
    EXPECT_NE(
        query(other, "INSERT INTO ddl_target VALUES (2)").find("INSERT 0 1"),
        std::string::npos);
    close(owner);
    close(other);
  }
  Database::Create(path).MoveValue()->DeleteAll();
}

TEST(PostgresServerTest, DisconnectReleasesOpenDdlBlock) {
  const std::string path = "postgres_server_ddl_disconnect-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    PostgresServer server(path, options);
    std::string error;
    ASSERT_TRUE(server.Listen(&error)) << error;
    std::jthread worker([&] { server.Run(&error); });
    StopGuard stop_guard{&server};
    const int first = ConnectClient(server.BoundPort());
    ASSERT_GE(first, 0);
    ASSERT_TRUE(SendAll(first, QueryMessage("BEGIN")));
    ASSERT_NE(ReadUntilReady(first).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(first, QueryMessage("CREATE TABLE abandoned (k INT64)")));
    ASSERT_NE(ReadUntilReady(first).find("CREATE TABLE"), std::string::npos);
    close(first);
    const int next = ConnectClient(server.BoundPort());
    ASSERT_GE(next, 0);
    std::string reply;
    for (int attempt = 0; attempt < 50; ++attempt) {
      ASSERT_TRUE(
          SendAll(next, QueryMessage("CREATE TABLE after_close (k INT64)")));
      reply = ReadUntilReady(next);
      if (reply.find("55006") == std::string::npos) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_NE(reply.find("CREATE TABLE"), std::string::npos);
    close(next);
  }
  Database::Create(path).MoveValue()->DeleteAll();
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
    ASSERT_TRUE(SendAll(
        client, QueryMessage("CREATE TABLE multi_flow (id INT64); "
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO multi_flow VALUES (3);")));
    EXPECT_NE(ReadUntilReady(client).find("INSERT 0 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE rb_abort (id INT64);")));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
        if (offset != 0) {
          insert += ',';
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

    EXPECT_NE(ReadUntilReady(client_a).find("SELECT 40000"), std::string::npos);
    close(client_a);

    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE drop_txn (id INT64);")));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE runtime_sync (id INT64, "
                                     "body STRING(16));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO runtime_sync VALUES "
                                             "(1, 'abc');")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- a type error that is only detectable at execution time (adding
    // INT64 + VARCHAR) is raised inside an explicit transaction, so it must
    // travel through the synchronous executor and abort the transaction.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    ASSERT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id + body FROM runtime_sync;")));
    const std::string failed = ReadUntilReady(client);
    EXPECT_NE(failed.find('E'), std::string::npos);
    EXPECT_NE(failed.find("type mismatch"), std::string::npos);
    // P2838: the runtime type error carries kInvalidArgument, which now maps
    // to 42601 identically at Prepare-phase and execution-phase sites.
    EXPECT_NE(failed.find("42601"), std::string::npos);
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE runtime_rw (id INT64, body "
                                     "STRING(16));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO runtime_rw VALUES "
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
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM runtime_rw;")));
    const std::string recovered = ReadUntilReady(client);
    EXPECT_NE(recovered.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE runtime_multi (id INT64, "
                                     "body STRING(16));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(SendAll(client, QueryMessage("INSERT INTO runtime_multi VALUES "
                                             "(1, 'abc');")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Act -- a single message with three read-only statements sharing one
    // read-only snapshot (§7.3): the first streams its rows, the second
    // throws at execution time. The worker aborts the shared snapshot,
    // stops at the failing statement, and still answers ReadyForQuery.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id FROM runtime_multi; "
                                     "SELECT id + body FROM runtime_multi; "
                                     "SELECT id FROM runtime_multi;")));
    const std::string response = ReadUntilReady(client);
    EXPECT_NE(response.find('D'), std::string::npos);
    EXPECT_NE(response.find("SELECT 1"), std::string::npos);
    EXPECT_NE(response.find('E'), std::string::npos);
    EXPECT_NE(response.find("type mismatch"), std::string::npos);
    EXPECT_NE(response.find('Z'), std::string::npos);
    // The second statement fails, so the third must never execute: no
    // "SELECT 1" CommandComplete may follow the ErrorResponse.
    EXPECT_EQ(response.find("SELECT 1", response.find('E')), std::string::npos);

    // Assert -- the session is still usable.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM runtime_multi;")));
    EXPECT_NE(ReadUntilReady(client).find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
        if (offset != 0) {
          insert += ',';
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
    ASSERT_TRUE(
        SendAll(client_a, QueryMessage("SELECT * FROM stale_fd ORDER BY id "
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

    EXPECT_NE(ReadUntilReady(client_a).find("SELECT 40000"), std::string::npos);
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE drop_abort (id INT64);")));
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
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest,
     StreamsLargeResultBeyondOldRowAndBufferCapsOverSlowClient) {
  const std::string path = "postgres_server_stream_large-" + RandomString();
  const size_t floor_before =
      ProcessMemoryBudget::Global().ServerBufferFloorBytes();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.max_connections = 64;
    PostgresServer server(path, options);
    EXPECT_GT(ProcessMemoryBudget::Global().ServerBufferFloorBytes(),
              floor_before);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
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

    ASSERT_TRUE(SendAll(
        client,
        QueryMessage("CREATE TABLE stream_a (id INT64, pad STRING(64)); "
                     "CREATE TABLE stream_b (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // Insert 1,050 rows into stream_a (50B pad) and 1,000 rows into stream_b
    // so SELECT produces 1,050,000 rows (> old kMaxServerResultRows=1,000,000)
    // and > 75 MiB wire output (> old kMaxQueuedOutputBytes=64 MiB).
    const std::string pad(50, 'S');
    {
      std::string ins_a = "INSERT INTO stream_a VALUES ";
      for (int i = 0; i < 1050; ++i) {
        if (i > 0) {
          ins_a += ',';
        }
        ins_a += "(" + std::to_string(i) + ",'" + pad + "')";
      }
      ASSERT_TRUE(SendAll(client, QueryMessage(ins_a + ";")));
      ASSERT_FALSE(ReadUntilReady(client).empty());

      std::string ins_b = "INSERT INTO stream_b VALUES ";
      for (int i = 0; i < 1000; ++i) {
        if (i > 0) {
          ins_b += ',';
        }
        ins_b += "(" + std::to_string(i) + ")";
      }
      ASSERT_TRUE(SendAll(client, QueryMessage(ins_b + ";")));
      ASSERT_FALSE(ReadUntilReady(client).empty());
    }

    ASSERT_TRUE(SendAll(
        client, QueryMessage("SELECT a.id, a.pad FROM stream_a a CROSS JOIN "
                             "stream_b b;")));

    // Incrementally frame PG wire messages without materializing 75 MiB in RAM;
    // sleep briefly on the first few reads to force socket backpressure.
    std::string tail;
    tail.reserve(65536);
    size_t data_rows = 0;
    size_t total_wire_bytes = 0;
    std::string command_tag;
    std::string error_msg;
    bool got_ready = false;
    int slow_polls = 0;
    std::array<char, 16384> buf{};
    size_t cursor = 0;

    while (!got_ready) {
      pollfd pfd{.fd = client, .events = POLLIN, .revents = 0};
      ASSERT_GT(poll(&pfd, 1, 30000), 0);
      if (slow_polls < 5) {
        ++slow_polls;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      const ssize_t n = recv(client, buf.data(), buf.size(), 0);
      ASSERT_GT(n, 0);
      total_wire_bytes += static_cast<size_t>(n);
      tail.append(buf.data(), static_cast<size_t>(n));

      while (cursor + 5 <= tail.size()) {
        const uint32_t len = pgwire::ReadUint32(tail, cursor + 1).Value();
        ASSERT_GE(len, 4U);
        const size_t total = static_cast<size_t>(len) + 1;
        if (cursor + total > tail.size()) {
          break;
        }
        const char type = tail[cursor];
        if (type == 'D') {
          ++data_rows;
        } else if (type == 'C') {
          command_tag.assign(tail.data() + cursor + 5, total - 6);
        } else if (type == 'E') {
          error_msg.assign(tail.data() + cursor + 5, total - 6);
        } else if (type == 'Z') {
          got_ready = true;
        }
        cursor += total;
      }
      if (cursor > 0) {
        tail.erase(0, cursor);
        cursor = 0;
      }
    }

    EXPECT_TRUE(error_msg.empty()) << error_msg;
    EXPECT_EQ(data_rows, 1'050'000U);
    EXPECT_GT(total_wire_bytes, size_t{64} << 20);
    EXPECT_EQ(command_tag, "SELECT 1050000");

    // Verify a subsequent pipelined query runs cleanly after streaming EOF.
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT COUNT(*) FROM stream_a;")));
    const std::string follow = ReadUntilReady(client);
    EXPECT_NE(follow.find("1050"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  EXPECT_EQ(ProcessMemoryBudget::Global().ServerBufferFloorBytes(),
            floor_before);
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

TEST(PostgresServerTest, PropagatesResourceLimitSqlStates53200And53100) {
  const std::string path = "postgres_server_sqlstate-" + RandomString();
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
        client, QueryMessage("CREATE TABLE rlim (k INT64, pad STRING(256));")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    const std::string pad(200, 'R');
    std::string ins = "INSERT INTO rlim VALUES ";
    for (int i = 0; i < 600; ++i) {
      if (i > 0) {
        ins += ',';
      }
      ins += "(" + std::to_string(600 - i) + ",'" + pad + "')";
    }
    ASSERT_TRUE(SendAll(client, QueryMessage(ins + ";")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    // 1. Force kNoSpace during spill -> PG SQLSTATE 53100.
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(32ULL << 10);
    ProcessMemoryBudget::Global().SetTestTempLimitBytes(1);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT k, pad FROM rlim ORDER BY k;")));
    const std::string err_nospace = ReadUntilReady(client);
    ProcessMemoryBudget::Global().ResetTestKnobs();
    EXPECT_NE(err_nospace.find('E'), std::string::npos);
    EXPECT_NE(err_nospace.find("53100"), std::string::npos) << err_nospace;

    // 2. Trigger kMemoryLimitExceeded (non-streaming unbounded ARRAY_AGG window
    //    over an overflowed skew partition) -> PG SQLSTATE 53200.
    // Reference `pad` so column pruning keeps the wide column inside Window.
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(32ULL << 10);
    ASSERT_TRUE(SendAll(
        client,
        QueryMessage("SELECT ARRAY_AGG(pad) OVER (ORDER BY k ROWS BETWEEN "
                     "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
                     "FROM rlim;")));
    const std::string err_mem = ReadUntilReady(client);
    ProcessMemoryBudget::Global().ResetTestKnobs();
    EXPECT_NE(err_mem.find('E'), std::string::npos);
    EXPECT_NE(err_mem.find("53200"), std::string::npos) << err_mem;

    // Connection stays usable afterwards.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT COUNT(*) FROM rlim;")));
    const std::string ok_after = ReadUntilReady(client);
    EXPECT_NE(ok_after.find("600"), std::string::npos);

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  ProcessMemoryBudget::Global().ResetTestKnobs();
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// SSLRequest / GSSENCRequest are fixed 8-byte frames. A broken driver that
// lies in the length field must still only consume those 8 bytes: the old
// code consumed the claimed length, ate the start of the following
// StartupMessage, and answered a perfectly legal startup with
// "invalid startup packet".
TEST(PostgresServerTest, SslRequestWithBadLengthKeepsFrameAlignment) {
  const std::string path = "postgres_server_ssl_len_test-" + RandomString();
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

    // Pre-auth probes must precede the startup handshake, so connect raw.
    const int client = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
    ASSERT_GE(client, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.BoundPort());
    ASSERT_EQ(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    ASSERT_EQ(
        connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    std::string probe;
    pgwire::AppendUint32(&probe, 12);  // lies about the frame size
    pgwire::AppendUint32(&probe, pgwire::kSSLRequestCode);
    // The frame is only 8 bytes on the wire; the lie must not make the
    // server nibble into the StartupMessage that follows immediately.
    probe += StartupMessage();
    ASSERT_TRUE(SendAll(client, probe));
    std::array<char, 1> first{};
    const ssize_t declined = recv(client, first.data(), first.size(), 0);
    ASSERT_EQ(declined, 1);
    // The SSL negotiation is declined with a bare 'N'…
    EXPECT_EQ(first[0], 'N');
    // …and the startup that follows completes instead of erroring.
    const std::string reply = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(reply).empty()) << ErrorPayloads(reply);
    EXPECT_NE(reply.find('R'), std::string::npos);
    EXPECT_NE(reply.find('Z'), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 1;")));
    const std::string query = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(query).empty()) << ErrorPayloads(query);
    EXPECT_NE(query.find("SELECT 1"), std::string::npos);
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// An explicit zero-worker pool is the same degraded state that all worker
// spawns failing produces. Every scheduler branch must then run the work
// inline on the event loop: reads, DML, and explicit transactions all stay
// correct, and EnqueueSqlTask's modulo cannot SIGFPE.
TEST(PostgresServerTest, ZeroReadWorkerPoolServesInline) {
  const std::string path = "postgres_server_zero_pool_test-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 0;
    PostgresServer server(path, options);
    EXPECT_EQ(server.ReadWorkerCount(), 0U);
    std::string listen_error;
    ASSERT_TRUE(server.Listen(&listen_error)) << listen_error;
    std::string run_error;
    int run_result = -1;
    std::jthread server_thread([&] { run_result = server.Run(&run_error); });
    StopGuard stop_guard{&server};

    // ConnectClient already performs the startup handshake.
    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE zero_pool (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());
    ASSERT_TRUE(
        SendAll(client, QueryMessage("INSERT INTO zero_pool VALUES (1);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM zero_pool;")));
    const std::string read = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(read).empty()) << ErrorPayloads(read);
    EXPECT_NE(read.find("SELECT 1"), std::string::npos);

    ASSERT_TRUE(
        SendAll(client, QueryMessage("BEGIN; INSERT INTO zero_pool VALUES (2);"
                                     " COMMIT;")));
    const std::string txn = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(txn).empty()) << ErrorPayloads(txn);

    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT id FROM zero_pool ORDER BY id;")));
    const std::string after = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(after).empty()) << ErrorPayloads(after);
    EXPECT_NE(after.find("SELECT 2"), std::string::npos);
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// An idle COMMIT / ROLLBACK must stay a tag-carrying no-op. Before the
// gate in ExecuteStatements, each such message opened an empty implicit
// transaction (allocating a txn id, snapshot, and commit round-trip); the
// observable contract is PostgreSQL's: warning-class no-op with the plain
// command tag and an 'I' status.
TEST(PostgresServerTest, TransactionControlNoopWithoutOpenBlock) {
  const std::string path = "postgres_server_noop_txn_test-" + RandomString();
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
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE noop_txn (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(client).empty());

    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    const std::string commit = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(commit).empty()) << ErrorPayloads(commit);
    EXPECT_NE(commit.find("COMMIT"), std::string::npos);
    EXPECT_NE(commit.find(std::string("Z\0\0\0\5I", 6)), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    const std::string rollback = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(rollback).empty()) << ErrorPayloads(rollback);
    EXPECT_NE(rollback.find("ROLLBACK"), std::string::npos);

    // A stray COMMIT as the first statement of a multi-statement message
    // must not swallow the statements that follow it.
    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT; INSERT INTO noop_txn "
                                             "VALUES (7);")));
    const std::string mixed = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(mixed).empty()) << ErrorPayloads(mixed);
    EXPECT_NE(mixed.find("COMMIT"), std::string::npos);
    EXPECT_NE(mixed.find("INSERT 0 1"), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT id FROM noop_txn;")));
    const std::string read = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(read).empty()) << ErrorPayloads(read);
    EXPECT_NE(read.find("SELECT 1"), std::string::npos);
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// ProcessInput consumes pipelined frames by offset and compacts the buffer
// once per burst (T1659): every frame must still be answered exactly once,
// with one ReadyForQuery per Query message.
TEST(PostgresServerTest, PipelinedQueriesInOneWriteGetOneReplyEach) {
  const std::string path = "postgres_server_pipeline_test-" + RandomString();
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

    std::string burst = QueryMessage("SELECT 101;");
    burst += QueryMessage("SELECT 202;");
    ASSERT_TRUE(SendAll(client, burst));
    std::string total;
    for (int i = 0; i < 4 && CountReadyFrames(total) < 2; ++i) {
      total += ReadUntilReady(client);
    }
    ASSERT_EQ(CountReadyFrames(total), 2U);
    EXPECT_TRUE(ErrorPayloads(total).empty()) << ErrorPayloads(total);
    EXPECT_NE(total.find("101"), std::string::npos);
    EXPECT_NE(total.find("202"), std::string::npos);

    // A frame split across writes must be processed after the earlier
    // frames are compacted out of the buffer.
    const std::string a = QueryMessage("SELECT 100;");
    const std::string b = QueryMessage("SELECT 200;");
    ASSERT_TRUE(SendAll(client, a + b.substr(0, b.size() - 4)));
    const std::string first = ReadUntilReady(client);
    EXPECT_NE(first.find("100"), std::string::npos);
    ASSERT_EQ(CountReadyFrames(first), 1U);
    ASSERT_TRUE(SendAll(client, b.substr(b.size() - 4)));
    const std::string second = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(second).empty()) << ErrorPayloads(second);
    EXPECT_NE(second.find("200"), std::string::npos);
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// T1713: an explicit LIMIT 0 lowers the query normally and publishes the
// declared column types, so RowDescription must carry the int8 OID (20),
// not the text (25) fallback that an untyped empty result would produce.
// T1664 additionally pins that a statement burst over the splitter cap is
// rejected with 54000 while the connection stays usable.
TEST(PostgresServerTest, LimitZeroKeepsColumnTypesAndBurstCapRejects) {
  const std::string path = "postgres_server_limit0_types-" + RandomString();
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

    ASSERT_TRUE(
        SendAll(client, QueryMessage("SELECT CAST(1 AS INT64) AS k LIMIT 0;")));
    const std::string zero = ReadUntilReady(client);
    ASSERT_TRUE(ErrorPayloads(zero).empty()) << ErrorPayloads(zero);

    bool found_column = false;
    uint32_t column_oid = 0;
    size_t cursor = 0;
    while (cursor + 5 <= zero.size() && !found_column) {
      const StatusOr<uint32_t> length = pgwire::ReadUint32(zero, cursor + 1);
      if (!length.HasValue() || length.Value() < 4 ||
          cursor + 1 + length.Value() > zero.size()) {
        break;
      }
      if (zero[cursor] == 'T') {
        // RowDescription: header, int16 field count, then per field
        // name(cstr) tableOID(int32) attnum(int16) typeOID(int32) ...
        size_t name = cursor + 7;
        size_t end = name;
        while (end < zero.size() && zero[end] != '\0') {
          ++end;
        }
        ASSERT_EQ(std::string(zero.data() + name, end - name), "k");
        const StatusOr<uint32_t> oid = pgwire::ReadUint32(zero, end + 1 + 6);
        ASSERT_TRUE(oid.HasValue());
        column_oid = oid.Value();
        found_column = true;
      }
      cursor += 1 + length.Value();
    }
    ASSERT_TRUE(found_column);
    EXPECT_EQ(column_oid, 20U);  // INT8OID, not TEXTOID (25)

    std::string many = "a;";
    for (size_t i = 0; i < pgwire::kMaxSqlStatements; ++i) {
      many += "a;";
    }
    ASSERT_TRUE(SendAll(client, QueryMessage(many)));
    const std::string capped = ReadUntilReady(client);
    EXPECT_NE(capped.find("54000"), std::string::npos);
    EXPECT_NE(capped.find("too many SQL statements"), std::string::npos);

    // The rejection is per-message: the session stays usable afterwards.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 7;")));
    const std::string after = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(after).empty()) << ErrorPayloads(after);
    EXPECT_NE(after.find('7'), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// T4601: a single row larger than the 4 MiB queued-output cap used to make
// Queue() drop the stream bytes and tear the connection down without any
// ErrorResponse or ReadyForQuery. The server now answers a clean limit
// error and keeps the session.
TEST(PostgresServerTest, OversizedResultRowAnswersLimitError) {
  const std::string path = "postgres_server_oversize_row-" + RandomString();
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

    // A single varchar cell is capped at 64 KiB by the value codec
    // (bin_size_t = uint16), so an over-cap result row is built from 66
    // REPEAT columns (per-call output cap is 1 MiB). The Q message stays
    // tiny -- the ROW exceeds the 4 MiB queued-output limit, not the
    // input -- keeping the test fast under parallel ctest load.
    std::string projection;
    for (int col = 0; col < 66; ++col) {
      if (col != 0) {
        projection += ", ";
      }
      projection += "REPEAT('x', 65535) AS c" + std::to_string(col);
    }
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT " + projection + ";")));
    const std::string reply = ReadUntilReady(client);
    const bool saw_error_frame = reply.find('E') != std::string::npos;
    EXPECT_NE(reply.find("53200"), std::string::npos) << saw_error_frame;
    EXPECT_NE(reply.find("queued-output limit"), std::string::npos);
    EXPECT_NE(reply.find(std::string("Z\0\0\0\5I", 6)), std::string::npos);

    // The rejection is per-statement: the session stays usable.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 9;")));
    const std::string after = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(after).empty()) << ErrorPayloads(after);
    EXPECT_NE(after.find('9'), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// T1657/T1658: SET is a fixed no-op reply. It must work standalone (no
// implicit transaction round-trip behind it) and inside a mixed message
// that now gets worker-offloaded instead of pinning the event loop.
TEST(PostgresServerTest, SetNoopAnswersStandaloneAndMixed) {
  const std::string path = "postgres_server_set_noop-" + RandomString();
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

    ASSERT_TRUE(SendAll(client, QueryMessage("SET application_name = 'x';")));
    const std::string solo = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(solo).empty()) << ErrorPayloads(solo);
    EXPECT_NE(solo.find("SET"), std::string::npos);
    EXPECT_NE(solo.find(std::string("Z\0\0\0\5I", 6)), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("SET x = 1; SELECT 42;")));
    const std::string mixed = ReadUntilReady(client);
    EXPECT_TRUE(ErrorPayloads(mixed).empty()) << ErrorPayloads(mixed);
    EXPECT_NE(mixed.find("SELECT 1"), std::string::npos);
    EXPECT_NE(mixed.find("42"), std::string::npos);

    // SET inside an aborted block still gets rejected like any command.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN; THIS IS NOT SQL;")));
    EXPECT_NE(ReadUntilReady(client).find("42601"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SET x = 2;")));
    EXPECT_NE(ReadUntilReady(client).find("25P02"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find("ROLLBACK"), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// T1693: transaction-state PG notices. A nested BEGIN and a lone
// COMMIT/ROLLBACK must reach the client as NoticeResponse ('N') frames, the
// way psql VERBOSITY and JDBC NoticeListeners expect them.
TEST(PostgresServerTest, TransactionStateNoticesMatchPostgres) {
  const std::string path = "postgres_server_notices-" + RandomString();
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

    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find("BEGIN"), std::string::npos);

    // Nested BEGIN: NOTICE 25001 + BEGIN tag, block stays open ('T').
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    const std::string nested = ReadUntilReady(client);
    EXPECT_NE(nested.find(std::string("N\0\0", 3)), std::string::npos);
    EXPECT_NE(nested.find("there is already a transaction in progress"),
              std::string::npos);
    EXPECT_NE(nested.find("25001"), std::string::npos);
    EXPECT_NE(nested.find("BEGIN"), std::string::npos);
    EXPECT_NE(nested.find(std::string("Z\0\0\0\5T", 6)), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find(std::string("Z\0\0\0\5I", 6)),
              std::string::npos);

    // Lone COMMIT / lone ROLLBACK on an idle session: 25P01 notices.
    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    const std::string lone_commit = ReadUntilReady(client);
    EXPECT_NE(lone_commit.find("there is no transaction in progress"),
              std::string::npos);
    EXPECT_NE(lone_commit.find("25P01"), std::string::npos);
    EXPECT_NE(lone_commit.find("COMMIT"), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    const std::string lone_rollback = ReadUntilReady(client);
    EXPECT_NE(lone_rollback.find("there is no transaction in progress"),
              std::string::npos);
    EXPECT_NE(lone_rollback.find("25P01"), std::string::npos);
    EXPECT_NE(lone_rollback.find("ROLLBACK"), std::string::npos);

    // A COMMIT closing a real in-message block must NOT warn (PG treats the
    // implicit block as a live transaction).
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 1; COMMIT;")));
    const std::string block_commit = ReadUntilReady(client);
    EXPECT_EQ(block_commit.find("25P01"), std::string::npos);

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

// T1660: a protocol violation (unterminated Q, extended-protocol message)
// must abort an open explicit block instead of letting it ride to COMMIT at
// status 'T'.
TEST(PostgresServerTest, ProtocolViolationAbortsExplicitBlock) {
  const std::string path = "postgres_server_proto_abort-" + RandomString();
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

    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find(std::string("Z\0\0\0\5T", 6)),
              std::string::npos);

    // A Q frame whose payload lacks the terminating NUL.
    const std::string sql = "SELECT 1";
    const uint32_t len = 4U + static_cast<uint32_t>(sql.size());
    std::string unterminated;
    unterminated.push_back('Q');
    unterminated.push_back(static_cast<char>((len >> 24U) & 0xffU));
    unterminated.push_back(static_cast<char>((len >> 16U) & 0xffU));
    unterminated.push_back(static_cast<char>((len >> 8U) & 0xffU));
    unterminated.push_back(static_cast<char>(len & 0xffU));
    unterminated += sql;
    ASSERT_TRUE(SendAll(client, unterminated));
    const std::string violation = ReadUntilReady(client);
    EXPECT_NE(violation.find("08P01"), std::string::npos);
    EXPECT_NE(violation.find(std::string("Z\0\0\0\5E", 6)), std::string::npos)
        << violation;

    // The block is aborted: normal commands are gated until recovery.
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 1;")));
    EXPECT_NE(ReadUntilReady(client).find("25P02"), std::string::npos);

    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find(std::string("Z\0\0\0\5I", 6)),
              std::string::npos);

    // And again with a malformed extended-protocol message: truncated Parse
    // gets 08P01 and Sync's RFQ must report the aborted state.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN;")));
    EXPECT_NE(ReadUntilReady(client).find(std::string("Z\0\0\0\5T", 6)),
              std::string::npos);
    ASSERT_TRUE(SendAll(client, std::string("P\0\0\0\4", 5)));
    ASSERT_TRUE(SendAll(client, std::string("S\0\0\0\4", 5)));
    const std::string extended = ReadUntilReady(client);
    EXPECT_NE(extended.find("08P01"), std::string::npos);
    EXPECT_NE(extended.find(std::string("Z\0\0\0\5E", 6)), std::string::npos)
        << extended;

    ASSERT_TRUE(SendAll(client, QueryMessage("ROLLBACK;")));
    EXPECT_NE(ReadUntilReady(client).find("ROLLBACK"), std::string::npos);
    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 7;")));
    EXPECT_NE(ReadUntilReady(client).find('7'),
              std::string::npos);  // NOLINT(performance-faster-string-find)

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  Database& database = *database_holder;
  database.DeleteAll();
}

}  // namespace
}  // namespace tinylamb
