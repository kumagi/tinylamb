/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_server.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
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
    if (sent <= 0) return false;
    offset += static_cast<size_t>(sent);
  }
  return true;
}

std::string ReadUntilReady(int fd) {
  std::string result;
  while (true) {
    pollfd descriptor{fd, POLLIN, 0};
    if (poll(&descriptor, 1, 5000) <= 0) return {};
    std::array<char, 4096> buffer{};
    const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
    if (received <= 0) return {};
    result.append(buffer.data(), static_cast<size_t>(received));

    size_t cursor = 0;
    while (cursor + 5 <= result.size()) {
      const uint32_t length = pgwire::ReadUint32(result, cursor + 1);
      if (length < 4 || cursor + 1 + length > result.size()) break;
      if (result[cursor] == 'Z') return result;
      cursor += 1 + length;
    }
  }
}

std::string StartupMessage() {
  std::string message;
  pgwire::AppendUint32(&message, 0);
  pgwire::AppendUint32(&message, pgwire::kProtocolVersion30);
  message.append("user\0test\0database\0test\0\0", 25);
  const uint32_t size = static_cast<uint32_t>(message.size());
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
  if (client < 0) return -1;
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
    struct StopGuard {
      PostgresServer* server;
      ~StopGuard() { server->RequestStop(); }
    } stop_guard{&server};

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
    struct StopGuard {
      PostgresServer* server;
      ~StopGuard() { server->RequestStop(); }
    } stop_guard{&server};

    const int setup = ConnectClient(server.BoundPort());
    ASSERT_GE(setup, 0);
    ASSERT_TRUE(
        SendAll(setup, QueryMessage("CREATE TABLE parallel_rows (id INT64);")));
    ASSERT_FALSE(ReadUntilReady(setup).empty());
    std::string insert = "INSERT INTO parallel_rows VALUES ";
    for (int64_t value = 1; value <= 200; ++value) {
      if (value != 1) insert += ',';
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

}  // namespace
}  // namespace tinylamb
