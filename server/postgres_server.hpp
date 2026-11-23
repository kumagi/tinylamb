/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_POSTGRES_SERVER_HPP
#define TINYLAMB_POSTGRES_SERVER_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class QueryResult;

Status DrainCommandResult(QueryResult& result, int64_t* affected_rows);

struct PostgresServerOptions {
  std::string listen_address{"127.0.0.1"};
  uint16_t port{54321};
  int backlog{128};
  size_t max_message_bytes{static_cast<size_t>(16U * 1024U * 1024U)};
  // Shared pool for snapshot reads and ordinary DML/transaction messages.
  // One message per session is in flight. TEMP/UDF sessions and DDL retain
  // the frontend's thread-local affinity on the event loop.
  // Unset selects std::thread::hardware_concurrency() (capped at 16); an
  // explicit 0 starts a degraded pool where every task runs inline.
  std::optional<size_t> read_worker_threads{};
  // Connections beyond this limit are shed immediately (client sees EOF).
  size_t max_connections{1024};
  // Truncate a torn/unparsable WAL tail to its intact prefix at boot instead
  // of treating the corruption as fatal (see RecoveryManager).
  bool force_recovery{false};
  // Clients sending nothing for this long are disconnected. Zero disables
  // the sweep.
  std::chrono::seconds idle_timeout{3600};
};

// Upper bound on the shared read/DML worker pool regardless of the requested
// read_worker_threads value.
inline constexpr size_t kMaxReadWorkers = 16;

class PostgresServer {
 public:
  PostgresServer(PostgresServer&&) = delete;
  PostgresServer& operator=(PostgresServer&&) = delete;
  PostgresServer(const std::string& database_path,
                 PostgresServerOptions options);
  ~PostgresServer();

  PostgresServer(const PostgresServer&) = delete;
  PostgresServer& operator=(const PostgresServer&) = delete;

  // Classifies the statement spellings that populate the thread-local TEMP
  // object registries. Exposed so the word-boundary handling (including the
  // seven-word "CREATE OR REPLACE TEMP AGGREGATE TABLE FUNCTION" spelling) has
  // a direct regression test instead of relying on an end-to-end worker test.
  static bool RegistersTempObject(std::string_view sql);

  bool Listen(std::string* error);
  int Run(std::string* error);
  void RequestStop();
  [[nodiscard]] uint16_t BoundPort() const;
  [[nodiscard]] size_t ReadWorkerCount() const;
  [[nodiscard]] size_t PeakConcurrentReadQueries() const;
  [[nodiscard]] uint64_t CachedLiteralReplyCount() const;
  [[nodiscard]] uint64_t DirectReplyCount() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_POSTGRES_SERVER_HPP
