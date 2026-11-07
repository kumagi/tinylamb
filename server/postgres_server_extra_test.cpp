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
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/platform/cpu_affinity.hpp"
#include "common/platform/socket.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "recovery/logger.hpp"
#include "server/postgres_protocol.hpp"
#include "server/postgres_server.hpp"
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
std::string ReadUntilReady(int fd, size_t ready_count = 1) {
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
    size_t ready = 0;
    while (cursor + 5 <= result.size()) {
      const uint32_t length = pgwire::ReadUint32(result, cursor + 1).Value();
      if (length < 4 || cursor + 1 + length > result.size()) {
        break;
      }
      if (result[cursor] == 'Z' && ++ready == ready_count) {
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

// Hold real fdatasync at the boundary; release before server thread teardown,
// including ASSERT failures. Shared state keeps an already-copied hook safe.
class SyncGate {
 public:
  struct State {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered{false};
    bool released{false};
  };
  explicit SyncGate(int failure = 0) : state_(std::make_shared<State>()) {
    Logger::SetBeforeSyncHookForTest([state = state_, failure](int) {
      std::unique_lock lock(state->mutex);
      state->entered = true;
      state->cv.notify_all();
      state->cv.wait(lock, [&] { return state->released; });
      return failure;
    });
  }
  ~SyncGate() { Release(); }
  bool Wait() {
    std::unique_lock lock(state_->mutex);
    return state_->cv.wait_for(lock, std::chrono::seconds(5),
                               [&] { return state_->entered; });
  }
  void Release() {
    Logger::SetBeforeSyncHookForTest({});
    std::scoped_lock lock(state_->mutex);
    state_->released = true;
    state_->cv.notify_all();
  }

 private:
  std::shared_ptr<State> state_;
};

TEST(PostgresServerExtraTest, PendingDurableRepliesDoNotOccupySqlWorkers) {
  const std::string path = "postgres_durable_replies-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 2;
    PostgresServer server(path, options);
    std::string error;
    ASSERT_TRUE(server.Listen(&error)) << error;
    std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
    StopGuard stop{&server};
    const int control = ConnectClient(server.BoundPort());
    ASSERT_GE(control, 0);
    ASSERT_TRUE(SendAll(
        control,
        QueryMessage(
            "CREATE TABLE durable_rows (id BIGINT PRIMARY KEY, v BIGINT)")));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(control)), 'I');
    std::vector<int> writers;
    for (int i = 0; i < 8; ++i) {
      writers.push_back(ConnectClient(server.BoundPort()));
      ASSERT_GE(writers.back(), 0);
    }
    SyncGate gate;
    for (int i = 0; i < 8; ++i) {
      ASSERT_TRUE(
          SendAll(writers[i], QueryMessage("INSERT INTO durable_rows VALUES (" +
                                           std::to_string(i) + ", 10)")));
    }
    ASSERT_TRUE(gate.Wait());
    // All workers can execute later SQL while the durability coordinator is
    // parked. Repeated probes ensure the first INSERT was actually published.
    ASSERT_TRUE(SendAll(control, QueryMessage("SELECT 42")));
    EXPECT_NE(ReadUntilReady(control).find(pgwire::DataRow(Row({Value(42)}))),
              std::string::npos);
    for (int fd : writers) {
      pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
      EXPECT_EQ(poll(&descriptor, 1, 20), 0);
    }
    // A reader of published rows, including inside an explicit block, must
    // wait for the observed writer's WAL rather than disclose its result.
    const int reader = ConnectClient(server.BoundPort());
    ASSERT_GE(reader, 0);
    ASSERT_TRUE(SendAll(reader, QueryMessage("BEGIN")));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(reader)), 'T');
    ASSERT_TRUE(SendAll(
        reader, QueryMessage("SELECT v FROM durable_rows WHERE id = 0")));
    pollfd descriptor{.fd = reader, .events = POLLIN, .revents = 0};
    EXPECT_EQ(poll(&descriptor, 1, 100), 0);
    // Disconnect a prepared writer: publication cannot be rolled back.
    close(writers.back());
    writers.pop_back();
    gate.Release();
    for (int fd : writers) {
      const auto response = ReadUntilReady(fd);
      EXPECT_NE(response.find("INSERT 0 1"), std::string::npos);
      EXPECT_EQ(ReadyForQueryStatus(response), 'I');
      close(fd);
    }
    const auto observed = ReadUntilReady(reader);
    EXPECT_EQ(ReadyForQueryStatus(observed), 'T');
    EXPECT_NE(observed.find(pgwire::DataRow(Row({Value(10)}))),
              std::string::npos);
    close(reader);
    close(control);
  }
  auto database = Database::Create(path).MoveValue();
  ASSERT_NE(database, nullptr);
  {
    // The result's executors hold index read gates owned by the storage that
    // DeleteAll() tears down, so they must be gone first.
    auto context = database->BeginReadOnlyContext();
    SqlEngine engine(*database);
    auto result = engine.Execute(context, "SELECT COUNT(*) FROM durable_rows");
    ASSERT_TRUE(result.HasValue()) << engine.LastError();
    EXPECT_EQ(result.Value().Collect(), std::vector<Row>({Row({Value(8)})}));
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  }
  database->DeleteAll();
}

TEST(PostgresServerExtraTest, FailedDurabilityNeverAcknowledgesCommit) {
  const std::string path = "postgres_failed_durability-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int client = ConnectClient(server.BoundPort());
  ASSERT_GE(client, 0);
  ASSERT_TRUE(SendAll(
      client,
      QueryMessage("CREATE TABLE failed_rows (id BIGINT PRIMARY KEY)")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(client)), 'I');
  ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(client)), 'T');
  ASSERT_TRUE(
      SendAll(client, QueryMessage("INSERT INTO failed_rows VALUES (1)")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(client)), 'T');
  SyncGate gate(EIO);
  ASSERT_TRUE(SendAll(client, QueryMessage("COMMIT")));
  ASSERT_TRUE(gate.Wait());
  pollfd descriptor{.fd = client, .events = POLLIN, .revents = 0};
  EXPECT_EQ(poll(&descriptor, 1, 50), 0);
  gate.Release();
  const auto response = ReadUntilReady(client);
  EXPECT_EQ(response.find("COMMIT"), std::string::npos);
  EXPECT_NE(response.find("40001"), std::string::npos);
  EXPECT_EQ(ReadyForQueryStatus(response), 'I');
  close(client);
}

TEST(PostgresServerExtraTest, DurableReadDoesNotWaitForUnrelatedFutureCommit) {
  const std::string path = "postgres_durable_frontier-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int writer = ConnectClient(server.BoundPort());
  const int pin = ConnectClient(server.BoundPort());
  const int reader = ConnectClient(server.BoundPort());
  ASSERT_GE(writer, 0);
  ASSERT_GE(pin, 0);
  ASSERT_GE(reader, 0);
  const auto execute = [](int fd, const std::string& sql) {
    EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
    auto reply = ReadUntilReady(fd);
    EXPECT_FALSE(reply.empty());
    EXPECT_EQ(reply.find("ERROR"), std::string::npos);
    return reply;
  };
  execute(writer,
          "CREATE TABLE frontier_rows (id BIGINT PRIMARY KEY, v BIGINT)");
  execute(writer, "INSERT INTO frontier_rows VALUES (0, 10), (1, 20)");
  execute(pin, "BEGIN");
  execute(pin, "SELECT v FROM frontier_rows WHERE id = 0");
  // The old snapshot keeps row 0's committed version (and dependency LSN)
  // resident even if background GC runs before the new reader arrives.
  execute(writer, "UPDATE frontier_rows SET v = 11 WHERE id = 0");
  SyncGate gate;
  ASSERT_TRUE(SendAll(
      writer, QueryMessage("UPDATE frontier_rows SET v = 21 WHERE id = 1")));
  ASSERT_TRUE(gate.Wait());
  const auto reply =
      execute(reader, "SELECT v FROM frontier_rows WHERE id = 0");
  EXPECT_NE(reply.find(pgwire::DataRow(Row({Value(11)}))), std::string::npos);
  pollfd descriptor{.fd = writer, .events = POLLIN, .revents = 0};
  EXPECT_EQ(poll(&descriptor, 1, 20), 0);
  gate.Release();
  EXPECT_EQ(ReadyForQueryStatus(ReadUntilReady(writer)), 'I');
  close(writer);
  close(reader);
  close(pin);
}

TEST(PostgresServerExtraTest,
     PartialRowsBeforeQueryErrorStillWaitForDurability) {
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 4;
  PostgresServer server("postgres_partial_durable-" + RandomString(), options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int writer = ConnectClient(server.BoundPort());
  const std::array<int, 2> readers{ConnectClient(server.BoundPort()),
                                   ConnectClient(server.BoundPort())};
  ASSERT_GE(writer, 0);
  for (int fd : readers) ASSERT_GE(fd, 0);
  const auto execute = [](int fd, const std::string& sql) {
    EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
    const auto reply = ReadUntilReady(fd);
    EXPECT_FALSE(reply.empty());
    EXPECT_EQ(reply.find("ERROR"), std::string::npos);
  };
  execute(writer,
          "CREATE TABLE partial_rows (id BIGINT PRIMARY KEY, v BIGINT)");
  execute(writer, "INSERT INTO partial_rows VALUES (0, 10)");
  execute(writer, "BEGIN");
  execute(writer, "UPDATE partial_rows SET v = 11 WHERE id = 0");
  SyncGate gate;
  ASSERT_TRUE(SendAll(writer, QueryMessage("COMMIT")));
  ASSERT_TRUE(gate.Wait());
  // Publication does not wait for the held fdatasync; allow its tiny worker
  // continuation to finish before taking the readers' new snapshots.
  std::this_thread::sleep_for(std::chrono::milliseconds(25));
  for (size_t i = 0; i < readers.size(); ++i) {
    const std::string prefix = i == 0 ? "" : "BEGIN; ";
    ASSERT_TRUE(SendAll(
        readers[i],
        QueryMessage(prefix + "SELECT v FROM partial_rows WHERE id = 0; "
                              "SELECT absent_column FROM partial_rows")));
  }
  for (int fd : readers) {
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    EXPECT_EQ(poll(&descriptor, 1, 50), 0)
        << "partial SELECT result escaped an undurable dependency";
  }
  gate.Release();
  EXPECT_NE(ReadUntilReady(writer).find("COMMIT"), std::string::npos);
  for (size_t i = 0; i < readers.size(); ++i) {
    const auto reply = ReadUntilReady(readers[i]);
    EXPECT_NE(reply.find(pgwire::DataRow(Row({Value(11)}))), std::string::npos);
    EXPECT_NE(reply.find("ERROR"), std::string::npos);
    EXPECT_EQ(ReadyForQueryStatus(reply), i == 0 ? 'I' : 'E');
    close(readers[i]);
  }
  close(writer);
}

TEST(PostgresServerExtraTest, SessionWorkersPreserveOrderAndDrainOpenBlocks) {
  const std::string path = "postgres_session_workers-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 4;
    PostgresServer server(path, options);
    std::string error;
    ASSERT_TRUE(server.Listen(&error)) << error;
    std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
    StopGuard stop{&server};
    const int client = ConnectClient(server.BoundPort());
    ASSERT_GE(client, 0);
    ASSERT_TRUE(
        SendAll(client, QueryMessage("CREATE TABLE worker_rows "
                                     "(id BIGINT PRIMARY KEY, v BIGINT)")));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(client)), 'I');
    // Several separate Query messages arrive in one TCP read. Completion
    // must restore the transaction before dispatching the next buffered one.
    ASSERT_TRUE(SendAll(client, QueryMessage("BEGIN") +
                                    QueryMessage("INSERT INTO worker_rows "
                                                 "VALUES (1, 10)") +
                                    QueryMessage("COMMIT") +
                                    QueryMessage("SELECT v FROM worker_rows "
                                                 "WHERE id = 1")));
    const std::string pipelined = ReadUntilReady(client, 4);
    ASSERT_FALSE(pipelined.empty());
    EXPECT_EQ(pipelined.find("ERROR"), std::string::npos) << pipelined;
    EXPECT_LT(pipelined.find("BEGIN"), pipelined.find("INSERT 0 1"));
    EXPECT_LT(pipelined.find("INSERT 0 1"), pipelined.find("COMMIT"));
    EXPECT_LT(pipelined.find("COMMIT"), pipelined.find("SELECT 1"));
    EXPECT_NE(pipelined.find(pgwire::DataRow(Row({Value(10)}))),
              std::string::npos);
    close(client);

    // Leave one completed, open block on the server and disconnect another
    // just after enqueueing its INSERT. Both must abort before shutdown ends.
    const int open = ConnectClient(server.BoundPort());
    const int gone = ConnectClient(server.BoundPort());
    ASSERT_GE(open, 0);
    ASSERT_GE(gone, 0);
    ASSERT_TRUE(SendAll(open, QueryMessage("BEGIN; INSERT INTO worker_rows "
                                           "VALUES (2, 20)")));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(open)), 'T');
    const int ddl = ConnectClient(server.BoundPort());
    ASSERT_GE(ddl, 0);
    ASSERT_TRUE(SendAll(ddl, QueryMessage("CREATE INDEX unsafe_online "
                                          "ON worker_rows(v)")));
    const std::string refused = ReadUntilReady(ddl);
    EXPECT_NE(refused.find("55006"), std::string::npos) << refused;
    EXPECT_EQ(ReadyForQueryStatus(refused), 'I');
    close(ddl);
    ASSERT_TRUE(SendAll(gone, QueryMessage("BEGIN; INSERT INTO worker_rows "
                                           "VALUES (3, 30)")));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(gone)), 'T');
    ASSERT_TRUE(SendAll(gone, QueryMessage("INSERT INTO worker_rows "
                                           "VALUES (4, 40)")));
    close(gone);
    server.RequestStop();
    thread.join();
    close(open);
  }
  auto database = Database::Create(path).MoveValue();
  ASSERT_NE(database, nullptr);
  {
    auto context = database->BeginReadOnlyContext();
    SqlEngine engine(*database);
    auto result = engine.Execute(context, "SELECT id, v FROM worker_rows");
    ASSERT_TRUE(result.HasValue()) << engine.LastError();
    EXPECT_EQ(result.Value().Collect(),
              std::vector<Row>({Row({Value(1), Value(10)})}));
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  }
  database->DeleteAll();
}

TEST(PostgresServerExtraTest, DistinctKeyChurnKeepsOtherSessionsIndependent) {
  const std::string path = "postgres_churn-" + RandomString();
  {
    PostgresServerOptions options;
    options.port = 0;
    options.read_worker_threads = 4;
    PostgresServer server(path, options);
    std::string error;
    ASSERT_TRUE(server.Listen(&error)) << error;
    std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
    StopGuard stop{&server};
    std::vector<int> clients;
    for (int i = 0; i < 16; ++i) {
      clients.push_back(ConnectClient(server.BoundPort()));
      ASSERT_GE(clients.back(), 0);
    }
    const auto execute = [&](int fd, const std::string& sql, char status) {
      EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
      const std::string response = ReadUntilReady(fd);
      EXPECT_EQ(response.find("ERROR"), std::string::npos) << sql << response;
      EXPECT_EQ(ReadyForQueryStatus(response), status) << sql << response;
      return response;
    };
    execute(clients[0], "CREATE TABLE churn (id BIGINT PRIMARY KEY, v BIGINT)",
            'I');
    for (int round = 0; round < 2; ++round) {
      for (size_t i = 0; i < clients.size(); ++i) {
        execute(clients[i],
                "BEGIN; INSERT INTO churn VALUES (" +
                    std::to_string(i + 100 * round) + ", 1)",
                'T');
      }
      // Every owner is still open when the next session deletes. The first
      // deletion creates an undo reservation, but other keys still fit.
      for (size_t i = 0; i < clients.size(); ++i) {
        EXPECT_NE(execute(clients[i],
                          "DELETE FROM churn WHERE id = " +
                              std::to_string(i + 100 * round),
                          'T')
                      .find("DELETE 1"),
                  std::string::npos);
      }
      for (size_t i = 0; i < clients.size(); ++i) {
        execute(clients[i], i % 2 == 0 ? "COMMIT" : "ROLLBACK", 'I');
      }
      EXPECT_NE(execute(clients[0], "SELECT COUNT(*) FROM churn", 'I')
                    .find(pgwire::DataRow(Row({Value(0)}))),
                std::string::npos);
    }
    // The narrow owned-row exception must preserve the snapshot-reader
    // fallback while a physical index entry is absent, and abort restores it.
    execute(clients[0], "INSERT INTO churn VALUES (42, 7)", 'I');
    execute(clients[0], "BEGIN; DELETE FROM churn WHERE id = 42", 'T');
    EXPECT_NE(execute(clients[1], "SELECT v FROM churn WHERE id = 42", 'I')
                  .find(pgwire::DataRow(Row({Value(7)}))),
              std::string::npos);
    execute(clients[0], "ROLLBACK", 'I');
    EXPECT_NE(execute(clients[1], "SELECT v FROM churn WHERE id = 42", 'I')
                  .find(pgwire::DataRow(Row({Value(7)}))),
              std::string::npos);
    for (int fd : clients) {
      close(fd);
    }
    server.RequestStop();
    thread.join();
  }
  auto database = Database::Create(path).MoveValue();
  ASSERT_NE(database, nullptr);
  database->DeleteAll();
}

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
    EXPECT_EQ(begun.find("ERROR"), std::string::npos) << begun;
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
    ASSERT_FALSE(answered.empty());
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
TEST(PostgresServerExtraTest, CachedPacketsKeepLargeSplitAndPipelinedMessages) {
  const std::string path = "postgres_cached_packets-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  const std::string prefix = "SELECT '", suffix = "' AS label";
  const std::string text(8192 - 6 - prefix.size() - suffix.size(), 'x');
  const std::string sql = prefix + text + suffix;
  const auto large = QueryMessage(sql), small = QueryMessage("SELECT 4242");
  ASSERT_EQ(large.size(), 8192U);
  ASSERT_TRUE(SendAll(fd, large));
  const auto cold = ReadUntilReady(fd);
  const auto row = pgwire::DataRow(Row({Value(std::string(text))}));
  ASSERT_NE(cold.find(row), std::string::npos);
  ASSERT_TRUE(SendAll(fd, small));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(fd)), 'I');
  const auto before = server.CachedLiteralReplyCount();
  const auto check_pair = [&](const std::string& wire) {
    const auto first = wire.find(row);
    EXPECT_NE(first, std::string::npos);
    if (first != std::string::npos) {
      EXPECT_NE(
          wire.find(pgwire::DataRow(Row({Value(4242)})), first + row.size()),
          std::string::npos);
    }
    EXPECT_EQ(ReadyForQueryStatus(wire), 'I');
  };
  ASSERT_TRUE(SendAll(fd, large + small));
  check_pair(ReadUntilReady(fd, 2));
  EXPECT_EQ(server.CachedLiteralReplyCount(), before + 2);
  ASSERT_TRUE(SendAll(fd, large.substr(0, 4093)));
  pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
  EXPECT_EQ(poll(&descriptor, 1, 20), 0);
  ASSERT_TRUE(SendAll(fd, large.substr(4093) + small));
  check_pair(ReadUntilReady(fd, 2));
  EXPECT_EQ(server.CachedLiteralReplyCount(), before + 4);
  ASSERT_TRUE(SendAll(fd, large));
  EXPECT_EQ(ReadUntilReady(fd), cold);
  const auto valid_count = server.CachedLiteralReplyCount();
  auto malformed = small;
  malformed.back() = 'x';
  ASSERT_TRUE(SendAll(fd, malformed));
  EXPECT_NE(ReadUntilReady(fd).find("unterminated"), std::string::npos);
  EXPECT_EQ(server.CachedLiteralReplyCount(), valid_count);
  std::string burst;
  for (int i = 0; i < 50; ++i) burst += large;
  ASSERT_TRUE(SendAll(fd, burst));
  ASSERT_EQ(shutdown(fd, SHUT_WR), 0);
  const auto drained = ReadUntilReady(fd, 50);
  size_t rows = 0;
  for (size_t offset = 0;
       (offset = drained.find(row, offset)) != std::string::npos;
       offset += row.size())
    ++rows;
  EXPECT_EQ(rows, 50U);
  EXPECT_EQ(server.CachedLiteralReplyCount(), valid_count + 50);
  close(fd);
}

TEST(PostgresServerExtraTest, LiteralRepliesKeepExactSqlAndTransactionState) {
  const std::string path = "postgres_literal_replies-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  const auto query = [&](const std::string& sql) {
    EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
    return ReadUntilReady(fd);
  };
  const std::string sql = "SELECT 7 + 8 AS answer, 'text' AS label";
  const auto cold = query(sql);
  ASSERT_EQ(ReadyForQueryStatus(cold), 'I');
  const auto before = server.CachedLiteralReplyCount();
  EXPECT_EQ(query(sql), cold);
  EXPECT_EQ(server.CachedLiteralReplyCount(), before + 1);
  const auto different = query("SELECT 7 + 9 AS answer, 'text' AS label");
  EXPECT_NE(different, cold);
  const auto count = server.CachedLiteralReplyCount();
  EXPECT_EQ(ReadyForQueryStatus(query("BEGIN")), 'T');
  EXPECT_EQ(ReadyForQueryStatus(query(sql)), 'T');
  EXPECT_EQ(server.CachedLiteralReplyCount(), count);
  EXPECT_EQ(ReadyForQueryStatus(query("SELECT missing_column")), 'E');
  EXPECT_EQ(ReadyForQueryStatus(query(sql)), 'E');
  EXPECT_EQ(server.CachedLiteralReplyCount(), count);
  EXPECT_EQ(ReadyForQueryStatus(query("ROLLBACK")), 'I');
  EXPECT_EQ(query(sql), cold);
  EXPECT_EQ(server.CachedLiteralReplyCount(), count + 1);
  const auto before_volatile = server.CachedLiteralReplyCount();
  const auto random1 = query("SELECT RAND()");
  const auto random2 = query("SELECT RAND()");
  EXPECT_NE(random1, random2);
  EXPECT_EQ(server.CachedLiteralReplyCount(), before_volatile);
  const auto error1 = query("SELECT 1 / 0");
  EXPECT_EQ(query("SELECT 1 / 0"), error1);
  EXPECT_EQ(server.CachedLiteralReplyCount(), before_volatile);
  EXPECT_EQ(ReadyForQueryStatus(query("SELECT 1; SELECT 2")), 'I');
  EXPECT_EQ(server.CachedLiteralReplyCount(), before_volatile);
  close(fd);
}

TEST(PostgresServerExtraTest, DirectReplyRestartsIdleTimeout) {
  const std::string path = "postgres_direct_idle-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  options.idle_timeout = std::chrono::seconds(2);
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  const int control = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  ASSERT_GE(control, 0);
  for (const auto& sql : {"CREATE TABLE idle_rows (id INT64)",
                          "INSERT INTO idle_rows VALUES (1)"}) {
    ASSERT_TRUE(SendAll(fd, QueryMessage(sql)));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(fd)), 'I');
  }
  // The control query is certified before the WAL sync hook is installed.
  ASSERT_TRUE(SendAll(control, QueryMessage("SELECT 1")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(control)), 'I');
  const auto before = server.DirectReplyCount();
  SyncGate gate;
  ASSERT_TRUE(SendAll(fd, QueryMessage("UPDATE idle_rows SET id = 2")));
  ASSERT_TRUE(gate.Wait());
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  ASSERT_TRUE(SendAll(control, QueryMessage("SELECT 1")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(control)), 'I');
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  gate.Release();
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(fd)), 'I');
  // More than the idle limit since request arrival, less than the limit
  // since its reply. Another live client wakes epoll's idle sweep first.
  std::this_thread::sleep_for(std::chrono::milliseconds(1150));
  ASSERT_TRUE(SendAll(control, QueryMessage("SELECT 1")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(control)), 'I');
  ASSERT_TRUE(SendAll(fd, QueryMessage("SELECT id FROM idle_rows")));
  EXPECT_NE(ReadUntilReady(fd).find(pgwire::DataRow(Row({Value(2)}))),
            std::string::npos);
  EXPECT_GT(server.DirectReplyCount(), before);
  close(fd);
  close(control);
}

TEST(PostgresServerExtraTest,
     DirectRepliesKeepPipelineTransactionsAndHalfClose) {
  const std::string path = "postgres_direct_replies-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  for (const auto& sql : {"CREATE TABLE direct_rows (id INT64)",
                          "INSERT INTO direct_rows VALUES (101)"}) {
    ASSERT_TRUE(SendAll(fd, QueryMessage(sql)));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(fd)), 'I');
  }
  const auto before = server.DirectReplyCount();
  std::string messages;
  for (const auto& sql :
       {"SELECT id FROM direct_rows", "BEGIN",
        "UPDATE direct_rows SET id = 202", "SELECT id FROM direct_rows",
        "SELECT no_such_column", "ROLLBACK", "SELECT id FROM direct_rows",
        "SELECT 303"})
    messages += QueryMessage(sql);
  ASSERT_TRUE(SendAll(fd, messages));
  ASSERT_EQ(shutdown(fd, SHUT_WR), 0);
  const auto wire = ReadUntilReady(fd, 8);
  const auto first = wire.find(pgwire::DataRow(Row({Value(101)})));
  ASSERT_NE(first, std::string::npos) << "wire=" << wire;
  const auto own = wire.find(pgwire::DataRow(Row({Value(202)})), first + 1);
  ASSERT_NE(own, std::string::npos);
  const auto failed = wire.find(pgwire::ReadyForQuery('E'), own + 1);
  ASSERT_NE(failed, std::string::npos);
  const auto rolled_back =
      wire.find(pgwire::DataRow(Row({Value(101)})), failed + 1);
  ASSERT_NE(rolled_back, std::string::npos);
  EXPECT_NE(wire.find(pgwire::DataRow(Row({Value(303)})), rolled_back + 1),
            std::string::npos);
  EXPECT_EQ(ReadyForQueryStatus(wire), 'I');
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (server.DirectReplyCount() <= before &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  EXPECT_GT(server.DirectReplyCount(), before);
  close(fd);
}

TEST(PostgresServerExtraTest,
     CertifiedWithSelectUsesFreshSnapshotAndKeepsBlocks) {
  const std::string path = "postgres_with_snapshot-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  const auto query = [&](const std::string& sql) {
    EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
    return ReadUntilReady(fd);
  };
  EXPECT_EQ(ReadyForQueryStatus(query("CREATE TABLE w (id INT64)")), 'I');
  EXPECT_EQ(ReadyForQueryStatus(query("INSERT INTO w VALUES (7)")), 'I');
  const std::string sql = "WITH x AS (SELECT id FROM w) SELECT id FROM x;";
  const auto cold = query(sql);
  EXPECT_NE(cold.find(pgwire::DataRow(Row({Value(7)}))), std::string::npos);
  EXPECT_EQ(server.PeakConcurrentReadQueries(), 0U);
  EXPECT_EQ(query(sql), cold);
  EXPECT_GE(server.PeakConcurrentReadQueries(), 1U);
  EXPECT_EQ(ReadyForQueryStatus(query("UPDATE w SET id = 8")), 'I');
  EXPECT_NE(query(sql).find(pgwire::DataRow(Row({Value(8)}))),
            std::string::npos);
  EXPECT_EQ(ReadyForQueryStatus(query("BEGIN")), 'T');
  EXPECT_EQ(ReadyForQueryStatus(query("UPDATE w SET id = 9")), 'T');
  const auto own_write = query(sql);
  EXPECT_EQ(ReadyForQueryStatus(own_write), 'T');
  EXPECT_NE(own_write.find(pgwire::DataRow(Row({Value(9)}))),
            std::string::npos);
  EXPECT_EQ(ReadyForQueryStatus(query("SELECT no_such_column")), 'E');
  EXPECT_EQ(ReadyForQueryStatus(query(sql)), 'E');
  EXPECT_EQ(ReadyForQueryStatus(query("ROLLBACK")), 'I');
  EXPECT_NE(query(sql).find(pgwire::DataRow(Row({Value(8)}))),
            std::string::npos);
  const auto different = query("WITH x AS (SELECT id FROM w) DELETE FROM w");
  EXPECT_NE(different.find('E'), std::string::npos);
  EXPECT_EQ(ReadyForQueryStatus(query("DROP TABLE w")), 'I');
  EXPECT_EQ(ReadyForQueryStatus(query("CREATE TABLE w (id INT64)")), 'I');
  EXPECT_EQ(ReadyForQueryStatus(query("INSERT INTO w VALUES (10)")), 'I');
  EXPECT_NE(query(sql).find(pgwire::DataRow(Row({Value(10)}))),
            std::string::npos);
  ASSERT_TRUE(SendAll(fd, QueryMessage(sql) + QueryMessage(sql)));
  ASSERT_EQ(shutdown(fd, SHUT_WR), 0);
  const auto pipelined = ReadUntilReady(fd, 2);
  const auto row = pgwire::DataRow(Row({Value(10)}));
  const auto first = pipelined.find(row);
  ASSERT_NE(first, std::string::npos);
  EXPECT_NE(pipelined.find(row, first + row.size()), std::string::npos);
  close(fd);
}

TEST(PostgresServerExtraTest,
     ShortReadsKeepSplitQueriesAndPipelineOnHalfClose) {
  const std::string path = "postgres_split_pipeline-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  for (const std::string sql : {"SELECT 12345", "SELECT 67890"}) {
    ASSERT_TRUE(SendAll(fd, QueryMessage(sql)));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(fd)), 'I');
  }
  const auto first = QueryMessage("SELECT 12345");
  ASSERT_TRUE(SendAll(fd, first.substr(0, 7)));
  pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
  EXPECT_EQ(poll(&descriptor, 1, 20), 0);
  ASSERT_TRUE(SendAll(fd, first.substr(7) + QueryMessage("SELECT 67890")));
  ASSERT_EQ(shutdown(fd, SHUT_WR), 0);
  const auto response = ReadUntilReady(fd, 2);
  const auto one = response.find(pgwire::DataRow(Row({Value(12345)})));
  const auto two = response.find(pgwire::DataRow(Row({Value(67890)})));
  EXPECT_NE(one, std::string::npos);
  EXPECT_NE(two, std::string::npos);
  EXPECT_LT(one, two);
  close(fd);
}

TEST(PostgresServerExtraTest,
     PooledEngineRefreshesViewsAfterCatalogEpochChange) {
  const std::string path = "postgres_pooled_views-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int admin = ConnectClient(server.BoundPort());
  const int reader = ConnectClient(server.BoundPort());
  ASSERT_GE(admin, 0);
  ASSERT_GE(reader, 0);
  const auto query = [&](int fd, const std::string& sql) {
    EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
    return ReadUntilReady(fd);
  };
  EXPECT_NE(
      query(admin, "CREATE TABLE pooled_base (v INT64)").find("CREATE TABLE"),
      std::string::npos);
  EXPECT_NE(query(admin, "INSERT INTO pooled_base VALUES (7)").find("INSERT"),
            std::string::npos);
  EXPECT_NE(query(admin, "CREATE VIEW pooled_view AS SELECT v FROM pooled_base")
                .find("CREATE VIEW"),
            std::string::npos);
  for (int i = 0; i < 4; ++i)
    EXPECT_NE(query(reader, "SELECT v FROM pooled_view")
                  .find(pgwire::DataRow(Row({Value(7)}))),
              std::string::npos);
  EXPECT_NE(query(admin, "DROP VIEW pooled_view").find("DROP VIEW"),
            std::string::npos);
  EXPECT_NE(query(admin,
                  "CREATE VIEW pooled_view AS SELECT v+1 AS v FROM pooled_base")
                .find("CREATE VIEW"),
            std::string::npos);
  for (int i = 0; i < 4; ++i)
    EXPECT_NE(query(reader, "SELECT v FROM pooled_view")
                  .find(pgwire::DataRow(Row({Value(8)}))),
              std::string::npos);
  close(reader);
  close(admin);
}

TEST(PostgresServerExtraTest, ReusedSessionShellKeepsCommitAndAbortState) {
  const std::string path = "postgres_session_shell-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  const auto query = [&](const std::string& sql) {
    EXPECT_TRUE(SendAll(fd, QueryMessage(sql)));
    return ReadUntilReady(fd);
  };
  EXPECT_NE(query("CREATE TABLE shell_state (k INT64 PRIMARY KEY, v INT64)")
                .find("CREATE TABLE"),
            std::string::npos);
  EXPECT_NE(query("INSERT INTO shell_state VALUES (1,0)").find("INSERT"),
            std::string::npos);
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(ReadyForQueryStatus(query("BEGIN")), 'T');
    EXPECT_EQ(
        ReadyForQueryStatus(query("UPDATE shell_state SET v=v+1 WHERE k=1")),
        'T');
    EXPECT_EQ(ReadyForQueryStatus(query(i % 2 == 0 ? "COMMIT" : "ROLLBACK")),
              'I');
  }
  EXPECT_NE(query("SELECT v FROM shell_state WHERE k=1")
                .find(pgwire::DataRow(Row({Value(10)}))),
            std::string::npos);
  close(fd);
}

TEST(PostgresServerExtraTest, CompletionBurstsKeepEveryReplyAndCommitBarrier) {
  const std::string path = "postgres_completion_bursts-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 4;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] { EXPECT_EQ(server.Run(&error), 0) << error; });
  StopGuard stop{&server};
  const int admin = ConnectClient(server.BoundPort());
  ASSERT_GE(admin, 0);
  ASSERT_TRUE(SendAll(
      admin,
      QueryMessage("CREATE TABLE bursts (k INT64 PRIMARY KEY, v INT64)")));
  ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(admin)), 'I');
  std::vector<int> clients;
  for (int i = 0; i < 24; ++i) {
    ASSERT_TRUE(SendAll(admin, QueryMessage("INSERT INTO bursts VALUES (" +
                                            std::to_string(i) + ",0)")));
    ASSERT_EQ(ReadyForQueryStatus(ReadUntilReady(admin)), 'I');
    clients.push_back(ConnectClient(server.BoundPort()));
    ASSERT_GE(clients.back(), 0);
  }
  const auto burst = [&](const auto& sql, char status, char first = 'C') {
    for (size_t i = 0; i < clients.size(); ++i)
      EXPECT_TRUE(SendAll(clients[i], QueryMessage(sql(i))));
    for (int fd : clients) {
      const auto response = ReadUntilReady(fd);
      EXPECT_EQ(ReadyForQueryStatus(response), status);
      EXPECT_FALSE(response.empty());
      if (!response.empty()) {
        EXPECT_EQ(response.front(), first) << response;
      }
    }
  };
  for (int round = 0; round < 8; ++round) {
    burst([](size_t) { return std::string("BEGIN"); }, 'T');
    burst(
        [](size_t i) {
          return "UPDATE bursts SET v=v+1 WHERE k=" + std::to_string(i);
        },
        'T');
    burst(
        [&](size_t) { return std::string(round % 2 ? "ROLLBACK" : "COMMIT"); },
        'I');
  }
  // These are admitted commit candidates but publish no new commit: a
  // zero-row implicit UPDATE and a failed implicit UPDATE. Retire both
  // cohorts before the next real request and before DDL quiescence checks.
  burst(
      [](size_t i) {
        return "UPDATE bursts SET v=v+1 WHERE k=" + std::to_string(1000000 + i);
      },
      'I');
  burst([](size_t) { return "UPDATE missing_bursts SET v=1"; }, 'I', 'E');
  for (int fd : clients) {
    EXPECT_TRUE(SendAll(fd, QueryMessage("SELECT SUM(v) FROM bursts")));
  }
  for (int fd : clients) {
    const auto response = ReadUntilReady(fd);
    EXPECT_EQ(ReadyForQueryStatus(response), 'I');
    EXPECT_NE(response.find(pgwire::DataRow(Row({Value(96)}))),
              std::string::npos);
    close(fd);
  }
  ASSERT_TRUE(SendAll(admin, QueryMessage("DROP TABLE bursts")));
  EXPECT_NE(ReadUntilReady(admin).find("DROP TABLE"), std::string::npos);
  close(admin);
}

TEST(PostgresServerExtraTest, RunRestoresCallingThreadCpuAffinity) {
  const std::string path = "postgres_io_affinity-" + RandomString();
  PostgresServerOptions options;
  options.port = 0;
  options.read_worker_threads = 2;
  PostgresServer server(path, options);
  std::string error;
  ASSERT_TRUE(server.Listen(&error)) << error;
  std::jthread thread([&] {
    const std::optional<platform::CpuSet> before =
        platform::CpuSet::CurrentThread();
    EXPECT_EQ(before.has_value(), platform::CpuSet::Supported());
    EXPECT_EQ(server.Run(&error), 0) << error;
    const std::optional<platform::CpuSet> after =
        platform::CpuSet::CurrentThread();
    EXPECT_EQ(after.has_value(), platform::CpuSet::Supported());
    EXPECT_TRUE(before == after);
  });
  StopGuard stop{&server};
  const int fd = ConnectClient(server.BoundPort());
  ASSERT_GE(fd, 0);
  ASSERT_TRUE(SendAll(fd, QueryMessage("SELECT 7")));
  EXPECT_EQ(ReadyForQueryStatus(ReadUntilReady(fd)), 'I');
  close(fd);
}

}  // namespace tinylamb
