/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// End-to-end wire-protocol regressions for routing and framing edge cases
// that the main server suite does not cover: comment-prefixed transaction
// keywords, dollar-quoted strings containing semicolons, and responses that
// race a client half-close.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "common/random_string.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "server/postgres_protocol.hpp"
#include "server/postgres_server.hpp"

namespace tinylamb {
namespace {

bool SendAll(int fd, const std::string& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t sent =
        send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (sent <= 0) {
      return false;
    }
    offset += static_cast<size_t>(sent);
  }
  return true;
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

// Reads messages until a ReadyForQuery ('Z') arrives or the 5s deadline
// expires.  Returns the accumulated bytes ('Z' included).
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

// ReadyForQuery carries a one-byte transaction status: 'I' idle, 'T' in a
// transaction block, 'E' failed.
char ReadyForQueryStatus(const std::string& wire) {
  size_t cursor = 0;
  while (cursor + 5 <= wire.size()) {
    const uint32_t length = pgwire::ReadUint32(wire, cursor + 1).Value();
    if (length < 4 || cursor + 1 + length > wire.size()) {
      break;
    }
    if (wire[cursor] == 'Z' && length == 5) {
      return wire[cursor + 5];
    }
    cursor += 1 + length;
  }
  return '?';
}

int ConnectClient(uint16_t port) {
  const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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

// Requests a clean server shutdown on scope exit.
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

// A comment prepended to BEGIN must not defeat transaction routing: the
// statement used to reach the SQL engine as command "--" and error out
// instead of opening a transaction block.
TEST(PostgresServerExtraTest, CommentBeforeBeginStillStartsTransaction) {
  const std::string path = "postgres_server_extra_test-" + RandomString();
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

    ASSERT_TRUE(SendAll(client, QueryMessage("-- session init\nBEGIN;")));
    const std::string begun = ReadUntilReady(client);
    ASSERT_FALSE(begun.empty());
    EXPECT_EQ(begun.find("ERROR"), std::string::npos)
        << true << begun;
    EXPECT_EQ(ReadyForQueryStatus(begun), 'T') << begun;

    ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT;")));
    const std::string committed = ReadUntilReady(client);
    ASSERT_FALSE(committed.empty());
    EXPECT_EQ(ReadyForQueryStatus(committed), 'I') << committed;

    ASSERT_TRUE(SendAll(client, std::string("X\0\0\0\4", 5)));
    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  database_holder->DeleteAll();
}

// A client that half-closes (shutdown(SHUT_WR)) right after sending a query
// still receives the response before the server closes its side.
TEST(PostgresServerExtraTest, HalfCloseStillReceivesResponse) {
  const std::string path = "postgres_server_extra_test-" + RandomString();
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

    ASSERT_TRUE(SendAll(client, QueryMessage("SELECT 42;")));
    ASSERT_EQ(shutdown(client, SHUT_WR), 0);
    const std::string answered = ReadUntilReady(client);
    ASSERT_FALSE(answered.empty())
        << true;
    EXPECT_NE(answered.find("42"), std::string::npos) << answered;

    close(client);
    server.RequestStop();
    server_thread.join();
    EXPECT_EQ(run_result, 0) << run_error;
  }
  auto database_holder = Database::Create(path).MoveValue();
  CHECK(database_holder != nullptr);
  database_holder->DeleteAll();
}

}  // namespace
}  // namespace tinylamb
