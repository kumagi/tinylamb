/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_server.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exception_status.hpp"
#include "common/log_message.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/cpu_affinity.hpp"
#include "common/platform/event_poller.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/os.hpp"
#include "common/platform/process.hpp"
#include "common/platform/socket.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "recovery/recovery_manager.hpp"
#include "server/postgres_protocol.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// Chunked outbound buffer size per streaming query result (ooca-v2.md §5.6).
constexpr size_t kClientStreamChunkBytes = 256U << 10;    // 256 KiB
constexpr size_t kClientTxBufferFloorBytes = 512U << 10;  // 512 KiB D1 floor
constexpr size_t kMaxStreamBytesPerWriteTurn = 1U << 20;  // 1 MiB yield cap
// How often the event loop wakes to sweep idle connections (milliseconds).
constexpr int kIdleSweepIntervalMs = 1000;
// Input cap per connection before authentication completes; unauthenticated
// clients must not be able to pin max_message_bytes of memory each.
// The startup packet cap counts the four-byte length word like PostgreSQL's.
constexpr size_t kMaxPreAuthInputBytes = 10000 + 4;
// Per-connection bound on non-streaming queued outbound bytes (e.g. pipelined
// DML spam by a peer that never reads). Streaming SELECT results pull into a
// ~`kClientStreamChunkBytes` chunk buffer on `EventPoller` writability; one
// indivisible row may overshoot that budget, and PumpActiveStream then
// defers the chunk until the queue drains instead of letting this cap drop
// the bytes and tear the connection down silently (P4601).
constexpr size_t kMaxQueuedOutputBytes = 4U << 20;  // 4 MiB

// Keep the socket working set on one CPU while SQL workers and background
// storage threads retain the caller's entire allowed CPU set. Changing the
// calling thread's affinity never enlarges that set. SQL executed on the I/O
// thread temporarily restores it so newly spawned query threads can use it.
// Thread pinning is Linux-only; elsewhere platform::CpuSet::CurrentThread()
// is nullopt and both scopes below are no-ops.
struct IoCpuMasks {
  platform::CpuSet available;
  platform::CpuSet io;
};
thread_local const IoCpuMasks* current_io_masks = nullptr;

class ScopedIoCpuAffinity {
 public:
  ScopedIoCpuAffinity() {
    if (std::getenv("TINYLAMB_DISABLE_IO_AFFINITY") != nullptr) {
      return;
    }
    const std::optional<platform::CpuSet> available =
        platform::CpuSet::CurrentThread();
    if (!available.has_value() || available->Count() < 2) {
      return;
    }
    masks_.available = *available;
    const int cpu = masks_.available.First();
    if (cpu < 0) {
      return;
    }
    masks_.io = platform::CpuSet::Single(cpu);
    if (masks_.io.ApplyToCurrentThread()) {
      previous_ = current_io_masks;
      current_io_masks = &masks_;
      cpu_ = cpu;
    }
  }
  ~ScopedIoCpuAffinity() {
    if (cpu_ < 0) {
      return;
    }
    current_io_masks = previous_;
    if (!masks_.available.ApplyToCurrentThread()) {
      const int restore_errno = errno;
      LOG(WARN) << "could not restore I/O thread CPU affinity: "
                << std::strerror(restore_errno);
    }
  }
  ScopedIoCpuAffinity(const ScopedIoCpuAffinity&) = delete;
  ScopedIoCpuAffinity& operator=(const ScopedIoCpuAffinity&) = delete;
  ScopedIoCpuAffinity(ScopedIoCpuAffinity&&) = delete;
  ScopedIoCpuAffinity& operator=(ScopedIoCpuAffinity&&) = delete;
  [[nodiscard]] int Cpu() const { return cpu_; }

 private:
  IoCpuMasks masks_{};
  const IoCpuMasks* previous_{nullptr};
  int cpu_{-1};
};

class ScopedSqlCpuAffinity {
 public:
  ScopedSqlCpuAffinity() : masks_(current_io_masks) {
    if ((masks_ != nullptr) && masks_->available.ApplyToCurrentThread()) {
      current_io_masks = nullptr;
    } else {
      masks_ = nullptr;
    }
  }
  ~ScopedSqlCpuAffinity() {
    if (masks_ == nullptr) {
      return;
    }
    if (!masks_->io.ApplyToCurrentThread()) {
      const int restore_errno = errno;
      LOG(WARN) << "could not restore SQL thread CPU affinity: "
                << std::strerror(restore_errno);
    }
    current_io_masks = masks_;
  }
  ScopedSqlCpuAffinity(const ScopedSqlCpuAffinity&) = delete;
  ScopedSqlCpuAffinity& operator=(const ScopedSqlCpuAffinity&) = delete;
  ScopedSqlCpuAffinity(ScopedSqlCpuAffinity&&) = delete;
  ScopedSqlCpuAffinity& operator=(ScopedSqlCpuAffinity&&) = delete;

 private:
  const IoCpuMasks* masks_;
};

std::string ErrnoMessage(std::string_view operation) {
  return std::string(operation) + ": " + std::strerror(errno);
}

// Listen addresses that keep the unauthenticated server private: all of
// 127.0.0.0/8 (not just 127.0.0.1, including numeric inet_aton shorthand),
// ::1 with or without IPv6 literal brackets, and "localhost". A DNS name is
// never trusted here: it may resolve to a public address. An empty address
// resolves to the wildcard via AI_PASSIVE and must still warn.
bool IsLoopbackListenAddress(std::string_view address) {
  if (address == "localhost") {
    return true;
  }
  // Only a numeric 127.0.0.0/8 literal is loopback. A DNS name such as
  // "127.0.0.1.evil.example" also starts with "127." but getaddrinfo (called
  // without AI_NUMERICHOST) can resolve it to a public address; accepting the
  // prefix would suppress the exposure warning for a publicly reachable
  // listener.
  if (address.starts_with("127.")) {
    size_t pos = 4;  // skip the leading "127."
    for (int octet = 0; octet < 3; ++octet) {
      const size_t begin = pos;
      int value = 0;
      while (pos < address.size() && address[pos] >= '0' &&
             address[pos] <= '9') {
        value = (value * 10) + (address[pos] - '0');
        if (value > 255) {
          break;
        }
        ++pos;
      }
      if (begin == pos || value > 255) {
        return false;
      }
      if (octet == 2) {
        return pos == address.size();
      }
      if (pos == address.size()) {
        // Only a complete dotted-quad literal is trusted. Resolver behavior
        // for abbreviated numeric names can vary and may involve DNS.
        return false;
      }
      if (address[pos] != '.') {
        return false;
      }
      ++pos;
    }
  }
  if (address.size() >= 2 && address.front() == '[' && address.back() == ']') {
    address = address.substr(1, address.size() - 2);
  }
  return address == "::1" || address == "0:0:0:0:0:0:0:1";
}

std::string StatusMessage(const Status& status) {
  // P1268: build the string directly. The ostringstream round-trip through
  // operator<< produced the exact same text (Status's operator<< delegates
  // to ToString) but allocated a second full copy plus stream bookkeeping on
  // every error path.
  return ToString(status);
}

// Skips whitespace and leading comments (`--`, `#`, `/* */`) so keyword
// extraction sees the first real statement token even when a client prepends
// a comment to a routing keyword (`-- init\nBEGIN`).
std::string_view StripLeadingComments(std::string_view sql) {
  size_t pos = 0;
  for (;;) {
    while (pos < sql.size() &&
           std::isspace(static_cast<unsigned char>(sql[pos])) != 0) {
      ++pos;
    }
    if (pos + 1 < sql.size() && sql[pos] == '-' && sql[pos + 1] == '-') {
      while (pos < sql.size() && sql[pos] != '\n') {
        ++pos;
      }
      continue;
    }
    if (pos < sql.size() && sql[pos] == '#') {
      while (pos < sql.size() && sql[pos] != '\n') {
        ++pos;
      }
      continue;
    }
    if (pos + 1 < sql.size() && sql[pos] == '/' && sql[pos + 1] == '*') {
      pos += 2;
      size_t depth = 1;
      while (pos < sql.size() && depth != 0) {
        // Compare the two bytes directly instead of forming a temporary
        // string_view per byte on long license-header comments.
        if (pos + 1 < sql.size() && sql[pos] == '/' && sql[pos + 1] == '*') {
          ++depth;
          pos += 2;
        } else if (pos + 1 < sql.size() && sql[pos] == '*' &&
                   sql[pos + 1] == '/') {
          --depth;
          pos += 2;
        } else {
          ++pos;
        }
      }
      continue;
    }
    return sql.substr(std::min(pos, sql.size()));
  }
}

std::string UppercaseCommand(std::string_view sql) {
  sql = StripLeadingComments(sql);
  const size_t begin = sql.find_first_not_of(" \t\r\n");
  if (begin == std::string_view::npos) {
    return {};
  }
  const size_t end = sql.find_first_of(" \t\r\n", begin);
  std::string command(sql.substr(begin, end - begin));
  for (char& c : command) {
    c = AsciiToUpper(c);
  }
  return command;
}

// COPY FROM mutates a user table and must be mirrored by the open-DDL gate;
// COPY TO is a read and does not participate in the table-image race. Find
// the direction keyword at top-level so FROM inside a query, literal, comment,
// or quoted identifier does not make COPY TO look like a write.
bool IsCatalogCommand(std::string_view command) {
  return command == "CREATE" || command == "DROP" || command == "ALTER" ||
         command == "ANALYZE";
}

bool IsCopyFrom(std::string_view sql) {
  sql = StripLeadingComments(sql);
  size_t pos = 0;
  while (pos < sql.size() &&
         std::isspace(static_cast<unsigned char>(sql[pos])) != 0) {
    ++pos;
  }
  while (pos < sql.size() &&
         std::isspace(static_cast<unsigned char>(sql[pos])) == 0) {
    ++pos;
  }
  size_t depth = 0;
  while (pos < sql.size()) {
    const char c = sql[pos];
    if (c == '\'' || c == '"') {
      const char quote = c;
      ++pos;
      while (pos < sql.size()) {
        if (sql[pos++] == quote) {
          if (pos < sql.size() && sql[pos] == quote) {
            ++pos;
          } else {
            break;
          }
        }
      }
      continue;
    }
    if (pos + 1 < sql.size() && c == '-' && sql[pos + 1] == '-') {
      pos = sql.find('\n', pos + 2);
      if (pos == std::string_view::npos) {
        return false;
      }
      continue;
    }
    if (pos + 1 < sql.size() && c == '/' && sql[pos + 1] == '*') {
      const size_t end = sql.find("*/", pos + 2);
      if (end == std::string_view::npos) {
        return false;
      }
      pos = end + 2;
      continue;
    }
    if (c == '(') {
      ++depth;
      ++pos;
      continue;
    }
    if (c == ')') {
      if (depth > 0) {
        --depth;
      }
      ++pos;
      continue;
    }
    if (depth == 0 &&
        (std::isalpha(static_cast<unsigned char>(c)) != 0 || c == '_')) {
      const size_t begin = pos++;
      while (pos < sql.size() &&
             (std::isalnum(static_cast<unsigned char>(sql[pos])) != 0 ||
              sql[pos] == '_')) {
        ++pos;
      }
      std::string word(sql.substr(begin, pos - begin));
      for (char& ch : word) {
        ch = AsciiToUpper(ch);
      }
      if (word == "FROM") {
        return true;
      }
      if (word == "TO") {
        return false;
      }
      continue;
    }
    ++pos;
  }
  return false;
}

// PostgreSQL starts a transaction with "BEGIN" or "START TRANSACTION" only:
// a bare "START" (or "START foo") is a syntax error. UppercaseCommand looks
// at the first word alone, so inspect the next word for the START spelling.
bool IsBeginStatement(std::string_view sql) {
  const std::string command = UppercaseCommand(sql);
  if (command == "BEGIN") {
    return true;
  }
  if (command != "START") {
    return false;
  }
  const std::string_view rest = StripLeadingComments(sql);
  const size_t begin = rest.find_first_not_of(" \t\r\n");
  const size_t end = rest.find_first_of(" \t\r\n", begin);
  if (end == std::string_view::npos) {
    return false;
  }
  return UppercaseCommand(rest.substr(end)) == "TRANSACTION";
}

std::string CommandTag(StatementType type, int64_t affected_rows) {
  switch (type) {
    case StatementType::kCreateTable:
      return "CREATE TABLE";
    case StatementType::kCreateIndex:
      return "CREATE INDEX";
    case StatementType::kDropTable:
      return "DROP TABLE";
    case StatementType::kAlterTable:
      return "ALTER TABLE";
    case StatementType::kSelect:
      return "SELECT " + std::to_string(affected_rows);
    case StatementType::kInsert:
      return "INSERT 0 " + std::to_string(affected_rows);
    case StatementType::kUpdate:
      return "UPDATE " + std::to_string(affected_rows);
    case StatementType::kDelete:
      return "DELETE " + std::to_string(affected_rows);
    case StatementType::kMerge:
      return "MERGE " + std::to_string(affected_rows);
    case StatementType::kAnalyze:
      return "ANALYZE";
    case StatementType::kCreateMaterializedView:
      return "CREATE MATERIALIZED VIEW";
    case StatementType::kDropMaterializedView:
      return "DROP MATERIALIZED VIEW";
    case StatementType::kCreateView:
      return "CREATE VIEW";
    case StatementType::kDropView:
      return "DROP VIEW";
    case StatementType::kCreateFunction:
      return "CREATE FUNCTION";
    case StatementType::kCreateConstant:
      return "CREATE CONSTANT";
  }
  return "OK";
}

}  // namespace

// Pins the thread-local UDF definition registry to one connection for the
// duration of a statement batch: CREATE [TEMP] FUNCTION registers under this
// id and call sites only resolve definitions of the same scope. Without this,
// two sessions sharing the event-loop thread would see each other's TEMP
// functions (and a new session's handshake would wipe a live one's).
class ScopedUdfSessionScope {
 public:
  explicit ScopedUdfSessionScope(uint64_t session_id)
      : previous_(UdfSessionScope()) {
    SetUdfSessionScope(session_id);
  }
  ~ScopedUdfSessionScope() { SetUdfSessionScope(previous_); }
  ScopedUdfSessionScope(const ScopedUdfSessionScope&) = delete;
  ScopedUdfSessionScope& operator=(const ScopedUdfSessionScope&) = delete;
  ScopedUdfSessionScope(ScopedUdfSessionScope&&) = delete;
  ScopedUdfSessionScope& operator=(ScopedUdfSessionScope&&) = delete;

 private:
  uint64_t previous_;
};

Status DrainCommandResult(QueryResult& result, int64_t* affected_rows) {
  const int64_t count = result.AffectedRows();
  result.Drain();
  if (affected_rows != nullptr) {
    *affected_rows = count;
  }
  return result.GetStatus();
}

// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding) field order is
// logical, not size-packed
class PostgresServer::
    Impl {  // NOLINT(clang-analyzer-optin.performance.Padding)
 public:
  Impl(const std::string& database_path, PostgresServerOptions options)
      : options_(std::move(options)) {
    try {
      secret_rng_.seed(std::random_device{}());
    } catch (...) {
      // Entropy source unavailable: degrade to a time/address mix rather
      // than letting the exception escape the constructor.
      secret_rng_.seed(
          static_cast<uint64_t>(
              std::chrono::steady_clock::now().time_since_epoch().count()) ^
          static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this)));
    }
    if (options_.max_connections >
        std::numeric_limits<size_t>::max() / kClientTxBufferFloorBytes) {
      startup_error_ = "max_connections exceeds the server buffer limit";
      read_worker_count_ = 0;  // no workers will ever start on this failure
      return;
    }
    // Honor force_recovery here as well as in postgres_server_main: an
    // embedder constructing PostgresServer directly with the flag set gets
    // the same torn-tail truncation behavior without touching the global
    // beforehand. Set it unconditionally — the flag is process-global, so a
    // previous server instance with force_recovery=true must not leak the
    // permission into a later instance that explicitly disabled it.
    RecoveryManager::SetTornTailTruncationAllowed(options_.force_recovery);
    // Opening (and recovering) the database is fallible; keep the failure
    // for Listen() instead of throwing out of the constructor.
    StatusOr<std::unique_ptr<Database>> created =
        Database::Create(database_path, 1, true);
    if (created.HasValue()) {
      database_ = created.MoveValue();
      // A bounded query pool can be occupied by waiters while their holders'
      // next COMMIT/ROLLBACK messages are queued. Keep admission nonblocking
      // even when writes run in workers, so the pool cannot starve its owners.
      database_->SetDeadlockPolicy(Database::DeadlockPolicy::kNoWait);
      metrics_enabled_ = std::getenv("TINYLAMB_SERVER_METRICS") != nullptr;
      database_->SetTransactionMetricsEnabled(metrics_enabled_);
    } else {
      startup_error_ = "failed to open database " + database_path + ": " +
                       ToString(created.GetStatus());
    }
    if (options_.read_worker_threads.has_value()) {
      read_worker_count_ =
          std::min(*options_.read_worker_threads, kMaxReadWorkers);
      if (*options_.read_worker_threads > kMaxReadWorkers) {
        // The CLI accepts 1..1024 (postgres_server_main.cpp); say so out
        // loud when the value is rounded down, instead of silently
        // serving fewer workers than requested. P2153: via the LOG
        // framework so every warning in this process shares one vocabulary.
        LOG(WARN) << "read_worker_threads " << *options_.read_worker_threads
                  << " exceeds the server cap of " << kMaxReadWorkers
                  << "; using " << kMaxReadWorkers;
      }
    } else {
      read_worker_count_ =
          std::min(kMaxReadWorkers,
                   std::max<size_t>(1, std::thread::hardware_concurrency()));
    }
    if (database_ != nullptr) {
      const size_t floor = options_.max_connections * kClientTxBufferFloorBytes;
      if (!ProcessMemoryBudget::Global().TryAddServerBufferFloorBytes(floor)) {
        startup_error_ = "combined server buffer floor exceeds the byte limit";
        read_worker_count_ = 0;
        return;
      }
      server_tx_floor_bytes_ = floor;
    }
  }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  ~Impl() {
    StopReadWorkers();
    for (auto& [fd, client] : clients_) {
      CleanupActiveStream(client);
      AbortOpenTransaction(client);
      platform::CloseSocket(fd);
    }
    if (platform::IsValidSocketFd(listener_fd_)) {
      platform::CloseSocket(listener_fd_);
    }
    waker_.Close();
    poller_.Close();
    if (reserve_fd_ >= 0) {
      platform::CloseFile(reserve_fd_);
    }
    ProcessMemoryBudget::Global().SubServerBufferFloorBytes(
        server_tx_floor_bytes_);
  }

  bool Listen(std::string* error) {
    if (platform::IsValidSocketFd(listener_fd_)) {
      return true;
    }
    // A (re)start clears a previous stop request HERE — before the slow
    // worker-startup window — so a RequestStop() arriving while
    // StartReadWorkers spins up threads is not erased by its flag reset.
    stopping_.store(false, std::memory_order_release);
    if (database_ == nullptr || !startup_error_.empty()) {
      *error = startup_error_;
      return false;
    }

    platform::EnsureSocketSubsystemInitialized();
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    addrinfo* addresses = nullptr;
    const std::string port = std::to_string(options_.port);
    const int lookup = getaddrinfo(options_.listen_address.c_str(),
                                   port.c_str(), &hints, &addresses);
    if (lookup != 0) {
      *error = std::string("getaddrinfo: ") + gai_strerror(lookup);
      return false;
    }

    for (addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
      const platform::SocketFd candidate = platform::OpenSocket(
          address->ai_family, address->ai_socktype, address->ai_protocol,
          {.nonblocking = true, .close_on_exec = true});
      if (!platform::IsValidSocketFd(candidate)) {
        continue;
      }
      // A silent failure here turns the next restart's bind into an opaque
      // TIME_WAIT rejection, so keep the diagnosis local.
      if (platform::SetSocketOptInt(candidate, SOL_SOCKET, SO_REUSEADDR, 1) !=
          0) {
        LOG(WARN) << ErrnoMessage("setsockopt(SO_REUSEADDR)");
      }
      if (bind(candidate, address->ai_addr, address->ai_addrlen) == 0 &&
          listen(candidate, options_.backlog) == 0) {
        listener_fd_ = candidate;
        break;
      }
      platform::CloseSocket(candidate);
    }
    freeaddrinfo(addresses);
    if (!platform::IsValidSocketFd(listener_fd_)) {
      *error = ErrnoMessage("could not bind PostgreSQL listener");
      return false;
    }

    sockaddr_storage bound{};
    socklen_t bound_size = sizeof(bound);
    if (getsockname(listener_fd_, reinterpret_cast<sockaddr*>(&bound),
                    &bound_size) != 0) {
      *error = ErrnoMessage("getsockname");
      platform::CloseSocket(listener_fd_);
      listener_fd_ = platform::kInvalidSocketFd;
      return false;
    }
    if (bound.ss_family == AF_INET) {
      bound_port_ = ntohs(reinterpret_cast<sockaddr_in*>(&bound)->sin_port);
    } else {
      bound_port_ = ntohs(reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port);
    }

    if (!poller_.Open()) {
      *error = ErrnoMessage("event poller");
      platform::CloseSocket(listener_fd_);
      listener_fd_ = platform::kInvalidSocketFd;
      return false;
    }
    if (!waker_.Open()) {
      *error = ErrnoMessage("event loop waker");
      platform::CloseSocket(listener_fd_);
      listener_fd_ = platform::kInvalidSocketFd;
      return false;
    }
    if (!AddPollFd(listener_fd_, platform::kPollReadable, error) ||
        !AddPollFd(waker_.ReadFd(), platform::kPollReadable, error)) {
      // listener_fd_ must not stay valid here: a Listen() retry would
      // early-return at the top and never register the waker/read workers.
      platform::CloseSocket(listener_fd_);
      listener_fd_ = platform::kInvalidSocketFd;
      return false;
    }
    // Reserve one descriptor so AcceptClients() can still drain the
    // listener when the process hits EMFILE/ENFILE (level-triggered epoll
    // would otherwise spin forever on the readable listener).
#ifdef TINYLAMB_OS_POSIX
    reserve_fd_ = platform::OpenFile("/dev/null", {});
#endif
    // Authentication is a stub: every connecting user is trusted. Warn when
    // the listen address reaches beyond loopback.
    if (!IsLoopbackListenAddress(options_.listen_address)) {
      LOG(WARN) << "tinylamb implements no authentication; every "
                   "connecting user is trusted. Do not expose it beyond "
                   "loopback.";
    }
    StartReadWorkers();
    return true;
  }

  int Run(std::string* error) {
    if (!platform::IsValidSocketFd(listener_fd_) && !Listen(error)) {
      return 1;
    }
    ScopedIoCpuAffinity affinity;
    io_cpu_ = affinity.Cpu();
    std::array<platform::PollResult, 64> events{};
    auto listener_retry_at = std::chrono::steady_clock::now();
    while (!stopping_.load()) {
      // Bounded wait so the idle-connection sweep below keeps running even
      // when no events arrive.
      const int count = poller_.Wait(events, kIdleSweepIntervalMs);
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        *error = ErrnoMessage("event poller wait");
        // P2082: without this the started read/durability workers sit in
        // their loop until ~Impl runs, and any completion they produce
        // after this point has no event loop to deliver it. Announce the
        // stop and join them on the error path, like the normal exit does.
        RequestStop();
        StopReadWorkers();
        return 1;
      }
      for (int i = 0; i < count; ++i) {
        const platform::SocketFd fd = events[static_cast<size_t>(i)].fd;
        const uint32_t flags = events[static_cast<size_t>(i)].events;
        if (fd == listener_fd_) {
          AcceptClients();
          continue;
        }
        if (fd == waker_.ReadFd()) {
          waker_.Drain();
          ProcessReadCompletions();
          continue;
        }
        if (!clients_.contains(fd)) {
          continue;
        }
        bool alive = true;
        // A completion can re-enable reads after epoll returned an older
        // RDHUP-only event. Drain the socket before deciding it is closed:
        // that event may still have complete pipelined queries behind it.
        if ((flags & platform::kPollReadable) != 0U ||
            ((flags & platform::kPollPeerClosed) != 0U &&
             !clients_.at(fd).read_query_in_flight)) {
          try {
            alive = ReadClient(fd);
          } catch (const std::bad_alloc&) {
            alive = false;
          }
        }
        if (alive && clients_.contains(fd) &&
            (flags & platform::kPollWritable) != 0U) {
          alive = WriteClient(fd);
        }
        if ((flags & platform::kPollError) != 0U) {
          alive = false;
        }
        if ((flags & platform::kPollHangup) != 0U) {
          // P2969: kqueue reports EV_EOF per filter, so on macOS a peer that
          // closed its read half raises hangup while our write half can
          // still deliver (half-close); EPOLLHUP means the socket is gone
          // anyway. Either way one best-effort flush turn gives already
          // queued T/D/C/Z bytes a chance to reach the peer before
          // CloseClient discards them; the connection still dies here.
          if (alive && clients_.contains(fd)) {
            WriteClient(fd);
          }
          alive = false;
        }
        if (!alive) {
          CloseClient(fd);
        }
      }
      SweepIdleClients();
      if (listener_shed_due_to_fd_pressure_) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= listener_retry_at) {
          listener_retry_at =
              now + std::chrono::milliseconds(kIdleSweepIntervalMs);
          AcceptClients();
        }
      }
      EmitMetrics();
    }
    StopReadWorkers();
    return 0;
  }

  void EmitMetrics() {
    if (!metrics_enabled_) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now - last_metrics_ < std::chrono::seconds(1)) {
      return;
    }
    last_metrics_ = now;
    const auto stats = database_->TransactionStats();
    // std::cerr is unit-buffered: each `<<` may flush independently, so a
    // 50-segment line would be dozens of write syscalls interleaving with
    // other threads' logs. Build the record first and emit one write.
    std::ostringstream line;
    line << "TINYLAMB_METRICS {\"time_ns\":"
         << std::chrono::duration_cast<std::chrono::nanoseconds>(
                now.time_since_epoch())
                .count()
         << ",\"fixed_size_wal\":"
         << (database_->FixedSizeWal() ? "true" : "false")
         << ",\"io_cpu\":" << io_cpu_ << ",\"tasks\":" << executed_tasks_.load()
         << ",\"queue_ns\":" << queue_ns_.load()
         << ",\"execution_ns\":" << execution_ns_.load()
         << ",\"completion_ns\":" << completion_ns_.load()
         << ",\"direct_replies\":" << direct_reply_count_.load()
         << ",\"durability_commands\":" << durability_commands_.load()
         << ",\"durability_wait_ns\":" << durability_wait_ns_.load()
         << ",\"durability_batches\":" << durability_batches_.load()
         << ",\"commit_driven_wal_sync\":"
         << (deferred_commits_enabled_.load(std::memory_order_relaxed)
                 ? "true"
                 : "false")
         << ",\"pending_tasks\":" << pending_worker_tasks_.load()
         << ",\"wal_wait_count\":" << stats.wal_wait_count
         << ",\"wal_wait_ns\":" << stats.wal_wait_ns
         << ",\"wal_sync_count\":" << stats.wal_sync_count
         << ",\"wal_sync_ns\":" << stats.wal_sync_ns
         << ",\"wal_buffered_bytes\":" << stats.wal_buffered_bytes
         << ",\"wal_durable_bytes\":" << stats.wal_durable_bytes
         << ",\"active_transactions\":" << stats.active_transactions
         << ",\"version_chains\":" << stats.version_chains
         << ",\"oldest_snapshot_ts\":" << stats.oldest_snapshot_ts
         << ",\"current_commit_ts\":" << stats.current_commit_ts << "}";
    std::cerr << line.str() << '\n';
  }

  void RequestStop() {
    {
      std::scoped_lock lock(read_completion_mutex_);
      stopping_.store(true);
    }
    waker_.Wake();
  }

  uint16_t BoundPort() const { return bound_port_; }
  size_t ReadWorkerCount() const { return read_worker_count_; }
  uint64_t CachedLiteralReplyCount() const {
    return cached_literal_reply_count_.load(std::memory_order_relaxed);
  }
  uint64_t DirectReplyCount() const {
    return direct_reply_count_.load(std::memory_order_relaxed);
  }
  size_t PeakConcurrentReadQueries() const {
    return peak_concurrent_reads_.load();
  }

 private:
  // Unpredictable BackendKeyData secret (CancelRequest is recognized on startup
  // but only closes the connection; the key must not be guessable from the fd
  // alone). Called only by AcceptClients on the Run event-loop thread;
  // RequestStop and worker threads must not access secret_rng_.
  uint32_t NextSecret() { return static_cast<uint32_t>(secret_rng_()); }

  // Epoll owns socket lifetime. A completed worker may borrow it for one
  // nonblocking send; close and send share this lock so an old completion
  // can never write to a recycled descriptor belonging to another client.
  struct ReplySocket {
    explicit ReplySocket(platform::SocketFd socket) : fd(socket) {}
    ReplySocket(const ReplySocket&) = delete;
    ReplySocket& operator=(const ReplySocket&) = delete;
    ReplySocket(ReplySocket&&) = delete;
    ReplySocket& operator=(ReplySocket&&) = delete;
    ~ReplySocket() { Close(); }
    void Close() {
      std::scoped_lock lock(mutex);
      if (platform::IsValidSocketFd(fd)) {
        platform::CloseSocket(fd);
      }
      fd = platform::kInvalidSocketFd;
    }
    ssize_t Send(std::string_view response, int* failure) {
      std::scoped_lock lock(mutex);
      if (!platform::IsValidSocketFd(fd)) {
        *failure = EBADF;
        return -1;
      }
      ssize_t sent = 0;
      do {
        sent = platform::SendNoSignal(fd, response.data(), response.size(),
                                      MSG_DONTWAIT);
      } while (sent < 0 && errno == EINTR);
      *failure = sent < 0 ? errno : 0;
      return sent;
    }
    std::mutex mutex;
    platform::SocketFd fd;
  };

  struct ActiveSelectStream {
    // Declared BEFORE `result` so C++ reverse member destruction destroys
    // `result` (and its executor) before any owned `TransactionContext`.
    std::unique_ptr<TransactionContext> owned_txn;
    std::unique_ptr<TransactionContext> implicit_txn;
    std::unique_ptr<QueryResult> result;
    DataChunk chunk;
    size_t chunk_row_offset{0};
    size_t chunk_row_count{0};
    std::vector<std::string> column_names;
    std::vector<ValueType> column_types;
    size_t row_count{0};
    bool header_sent{false};
    // Set when the wire chunk for the final (exhausted) result has been
    // queued: PumpActiveStream skips the pull and runs the continuation, so
    // the stream object can also serve as the per-turn re-arm marker for
    // multi-statement messages.
    bool eof_sent{false};
    bool in_client_txn{false};
    bool in_statement_block{false};
    // P693: statement vector shared immutably between the installing
    // message and every stream continuation. Copying it per (re)install
    // made a multi-statement message with streamed results re-allocate
    // the whole text block for every continuation hop.
    std::shared_ptr<const std::vector<pgwire::SqlStatement>> statements;
    size_t next_statement_idx{0};
    lsn_t reply_durability_target{0};
    bool permit_deferred_commit{false};
    // P4601: a single indivisible row can overshoot the chunk budget; when
    // such a chunk (or its EOF tail) no longer fits the queued-output cap,
    // PumpActiveStream parks it here and retries on the next writability
    // turn instead of letting Queue() drop it and close silently.
    std::string pending_wire;
    bool pending_eof{false};
  };

  // Hash for the exact-SQL reply caches. Transparent so a lookup by
  // string_view (the SQL text of an incoming packet) never allocates.
  struct SqlTextHash {
    using is_transparent = void;
    size_t operator()(std::string_view text) const noexcept {
      return std::hash<std::string_view>{}(text);
    }
  };

  struct Client {
    Client(platform::SocketFd socket, uint64_t identifier, uint32_t secret_key,
           bool create_plan_cache = true)
        : fd(socket),
          id(identifier),
          secret(secret_key),
          plan_cache(create_plan_cache
                         ? std::make_unique<PreparedPlanSessionCache>()
                         : nullptr),
          reply_socket(create_plan_cache ? std::make_shared<ReplySocket>(socket)
                                         : nullptr) {}
    platform::SocketFd fd;
    uint64_t id;
    uint32_t secret;
    bool startup_complete{false};
    bool close_after_write{false};
    // True once this session's open block executed a catalog DDL statement.
    bool open_block_has_ddl{false};
    // A FIN does not discard complete messages buffered behind a worker.
    bool peer_eof_seen{false};
    std::string input;
    std::string output;
    size_t output_offset{0};
    uint32_t registered_events{platform::kPollReadable |
                               platform::kPollPeerClosed};
    std::string user;
    std::string database;
    std::unique_ptr<TransactionContext> transaction;
    std::unique_ptr<PreparedPlanSessionCache> plan_cache;
    std::shared_ptr<ReplySocket> reply_socket{};
    // The socket owner retains one empty worker shell between requests.
    // Its transaction/cache have already moved back before it is recycled.
    std::unique_ptr<Client> worker_shell;
    char transaction_status{'I'};
    bool read_query_in_flight{false};
    // CREATE VIEW / CREATE FUNCTION register into thread-local frontend
    // registries on the event-loop thread; a read query offloaded to a
    // worker would not see those session objects. Once a session created
    // one, every later query stays on the loop thread.
    bool session_has_temp_objects{false};
    // When a read is offloaded to a worker, last_activity stops advancing
    // until the completion returns. Track the offload time separately so a
    // hung worker cannot pin the fd (and its buffers) past idle_timeout:
    // in-flight connections are swept on the same timeout measured from
    // here. ProcessReadCompletions drops completions for an unknown client
    // id, so closing is safe.
    std::chrono::steady_clock::time_point read_started_at{};
    std::chrono::steady_clock::time_point last_activity{
        std::chrono::steady_clock::now()};
    // Set when a reply starts queueing. The idle sweep grants a full
    // idle_timeout measured from here before discarding a still-draining
    // reply; WriteClient clears it once the queue is empty.
    std::chrono::steady_clock::time_point output_pending_at{};
    bool output_overflow = false;
    bool permit_deferred_commit{false};
    lsn_t reply_durability_target{0};
    std::unique_ptr<ActiveSelectStream> active_stream;
    // Cached wire frames for exact-SQL literal queries. The cache belongs to
    // the connection that produced it: a literal reply can embed session
    // state (a session constant's value), so it must never be handed to
    // another connection. The process-wide byte budget in Impl keeps the
    // per-connection caches from multiplying the resident memory.
    std::unordered_map<std::string, std::string, SqlTextHash, std::equal_to<>>
        literal_constant_replies;
    size_t literal_constant_reply_bytes{0};
  };

  struct ReadTask {
    platform::SocketFd client_fd{platform::kInvalidSocketFd};
    uint64_t client_id{};
    std::vector<pgwire::SqlStatement> statements;
    std::unique_ptr<Client> session;
    bool cleanup_only{false};
    std::unique_ptr<PreparedPlanSessionCache> plan_cache{};
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
    bool potential_commit{false};
    std::shared_ptr<ReplySocket> reply_socket{};
  };

  struct ReadCompletion {
    platform::SocketFd client_fd;
    uint64_t client_id;
    std::string response;
    std::unique_ptr<Client> session;
    std::string literal_constant_sql;
    std::unique_ptr<PreparedPlanSessionCache> plan_cache{};
    std::chrono::steady_clock::time_point ready_at{
        std::chrono::steady_clock::now()};
    std::shared_ptr<ReplySocket> reply_socket{};
    bool reply_send_failed{false};
    std::unique_ptr<ActiveSelectStream> active_stream;
  };

  bool AddPollFd(platform::SocketFd fd, uint32_t interest,
                 std::string* error) const {
    if (!poller_.Add(fd, interest)) {
      *error = ErrnoMessage("event poller add");
      return false;
    }
    return true;
  }

  void AcceptClients() {
#ifdef TINYLAMB_OS_POSIX
    // A previous EMFILE shed removed the listener from epoll; try to restore
    // the reserve descriptor (and the epoll registration) before accepting.
    if (listener_shed_due_to_fd_pressure_) {
      reserve_fd_ = platform::OpenFile("/dev/null", {});
      if (reserve_fd_ >= 0) {
        std::string error;
        if (AddPollFd(listener_fd_, platform::kPollReadable, &error)) {
          listener_shed_due_to_fd_pressure_ = false;
        } else {
          LOG(WARN) << error;
          platform::CloseFile(reserve_fd_);
          reserve_fd_ = -1;
          return;
        }
      } else {
        return;
      }
    }
#endif
    while (true) {
      const platform::SocketFd fd = platform::AcceptSocket(
          listener_fd_, {.nonblocking = true, .close_on_exec = true});
      if (!platform::IsValidSocketFd(fd)) {
#ifdef TINYLAMB_OS_POSIX
        if ((errno == EMFILE || errno == ENFILE) && reserve_fd_ >= 0) {
          // Descriptor exhaustion: the level-triggered listener stays
          // readable and would spin forever. Consume (and shed) one pending
          // connection via the reserved descriptor, then restore it.
          const std::string reason = ErrnoMessage("accept");
          platform::CloseFile(reserve_fd_);
          reserve_fd_ = -1;
          const platform::SocketFd shed = platform::AcceptSocket(
              listener_fd_, {.nonblocking = true, .close_on_exec = true});
          if (platform::IsValidSocketFd(shed)) {
            // P691: same fire-and-forget diagnosis on the descriptor-
            // pressure shed path; send() may simply fail under EMFILE, the
            // close follows either way.
            const std::string refuse =
                pgwire::ErrorResponse("too many connections", "53300", "FATAL");
            platform::SendNoSignal(shed, refuse.data(), refuse.size());
            platform::CloseSocket(shed);
          }
          ++fd_pressure_sheds_since_warn_;
          // During a sustained descriptor outage this fires on every
          // readable listener event; throttle it like the other failure
          // warnings and report the shed count so the outage duration is
          // still reconstructable from the log.
          const auto warn_now = std::chrono::steady_clock::now();
          if (warn_now - last_fd_pressure_warn_ >= std::chrono::seconds(60)) {
            last_fd_pressure_warn_ = warn_now;
            LOG(WARN) << "descriptor limit hit (" << reason
                      << "); shed one pending connection ("
                      << fd_pressure_sheds_since_warn_
                      << " sheds since previous notice)";
            fd_pressure_sheds_since_warn_ = 0;
          }
          reserve_fd_ = platform::OpenFile("/dev/null", {});
          if (reserve_fd_ < 0) {
            // Without a reserve descriptor the next EMFILE accept would spin
            // the level-triggered loop. Shed the listener from epoll until a
            // timed event-loop retry restores the reserve. The retry must
            // not depend on readability of the now-unregistered listener.
            if (poller_.Remove(listener_fd_)) {
              LOG(WARN) << ErrnoMessage(
                  "poller.Remove(listener) on descriptor pressure");
            }
            listener_shed_due_to_fd_pressure_ = true;
          }
        } else
#endif
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
          LOG(WARN) << ErrnoMessage("accept");
        }
        return;
      }
      // Disable Nagle so small responses are not delayed waiting for ACKs.
      if (platform::SetSocketOptInt(fd, IPPROTO_TCP, TCP_NODELAY, 1) != 0) {
        LOG(WARN) << ErrnoMessage("setsockopt(TCP_NODELAY)");
      }
      if (clients_.size() >= options_.max_connections) {
        // Over capacity. P691: instead of a bare RST that clients can only
        // read as "connection reset", fire-and-forget the PG-canonical
        // too_many_connections FATAL first (the socket is non-blocking, so
        // this is best-effort: a client that never reads still just sees
        // the close, but every normal connect() then getpeername-read path
        // gets the diagnosis).
        const std::string refuse =
            pgwire::ErrorResponse("too many connections", "53300", "FATAL");
        platform::SendNoSignal(fd, refuse.data(), refuse.size());
        platform::CloseSocket(fd);
        continue;
      }
      // Register the client before adding the fd to epoll: if the map insert
      // throws, the fd is still ours to close; registered the other way
      // around, an epoll-enrolled fd with no client entry would be ignored
      // by the event loop until shutdown.
      try {
        clients_.try_emplace(fd, fd, next_client_id_++, NextSecret());
      } catch (const std::exception& error) {
        // bad_alloc from the node allocation or the Client ctor's
        // plan-cache/ReplySocket members: the promise above only holds
        // because the fd is closed right here; letting the exception
        // escape would terminate the process through Run().
        LOG(WARN) << "failed to register connection: " << error.what();
        platform::CloseSocket(fd);
        continue;
      }
      std::string error;
      if (!AddPollFd(fd, platform::kPollReadable | platform::kPollPeerClosed,
                     &error)) {
        LOG(WARN) << error;
        CloseClient(fd);
        continue;
      }
    }
  }

  void SweepIdleClients() {
    if (options_.idle_timeout <= std::chrono::seconds::zero()) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now - last_idle_sweep_ < std::chrono::milliseconds(100)) {
      return;
    }
    last_idle_sweep_ = now;
    for (auto it = clients_.begin(); it != clients_.end();) {
      const Client& client = it->second;
      // A hung worker must not pin the connection forever, so an in-flight
      // read is swept on the same timeout measured from the offload time
      // (read_started_at is stamped once at enqueue; P2462: there is no
      // worker progress heartbeat to honor, so a non-streaming statement
      // that legitimately runs longer than idle_timeout is cut as well --
      // the earlier "exempt while the worker is making progress" wording
      // overstated the guarantee). Streams do make progress on the event
      // loop: PumpActiveStream refreshes last_activity per chunk. The
      // orphaned completion is dropped by the client-id check when it
      // eventually arrives.
      // last_activity is refreshed on every successful write (WriteClient)
      // as well as on receive/Queue, so a slow-but-progressing reader keeps
      // getting a full idle_timeout and its CommandComplete/ReadyForQuery
      // is not cut off mid-send. Anchoring on output_pending_at instead
      // measured from the moment the reply was queued and disconnected slow
      // readers whose query had already succeeded. A peer that never reads
      // generates no writes and is still bounded by idle_timeout.
      const auto idle_since = client.last_activity;
      const bool idle =
          client.read_query_in_flight
              ? now - client.read_started_at > options_.idle_timeout
              : now - idle_since > options_.idle_timeout;
      if (idle) {
        const platform::SocketFd fd = it->first;
        ++it;
        CloseClient(fd);
      } else {
        ++it;  // NOLINT(cppcoreguidelines-pro-type-member-init)
      }
    }
  }

  bool ReadClient(platform::SocketFd fd) {
    auto found = clients_.find(fd);
    if (found == clients_.end()) {
      return false;
    }
    Client& client = found->second;
    // Uninitialized on purpose: RecvSocket returns the number of valid
    // bytes and nothing reads past it, so the 8 KiB zero-fill per {}call
    // would just be hot-path memset.
    std::array<char, 8192>
        buffer;  // NOLINT(cppcoreguidelines-pro-type-member-init)
                 // only bytes read are used
    bool eof_seen = false;
    bool read_blocked = false;
    size_t cached_packets = 0;
    while (true) {
      const size_t receive_limit =
          client.startup_complete
              ? buffer.size()
              : std::min(buffer.size(),
                         kMaxPreAuthInputBytes - client.input.size());
      if (receive_limit == 0) {
        // Parse startup at its cap before receiving any pipelined query.
        break;
      }
      const ssize_t size =
          platform::RecvSocket(fd, buffer.data(), receive_limit, 0);
      if (size > 0) {
        client.last_activity = std::chrono::steady_clock::now();
        if (client.input.empty() &&
            TryCachedQueryPacket(
                client,
                std::string_view(buffer.data(), static_cast<size_t>(size)))) {
          if (client.output_overflow ||
              static_cast<size_t>(size) < buffer.size() ||
              ++cached_packets == 32) {
            break;
          }
          // Consumed frames no longer contribute to the queued-input cap.
          // Yield after a bounded burst; level-triggered epoll retains any
          // remaining bytes so another client gets a turn too.
          continue;
        }
        client.input.append(buffer.data(), static_cast<size_t>(size));
        const size_t input_limit = client.startup_complete
                                       ? options_.max_message_bytes + 5
                                       : kMaxPreAuthInputBytes;
        if (client.input.size() > input_limit) {
          // Queue() latches output_overflow once the 4 MiB queued-output
          // cap is exceeded -- a pipelining client that never reads can fill
          // the buffer to within a message of the limit.  This call sits in
          // the recv loop, OUTSIDE the ProcessInput try below.  The refused
          // error response trips the overflow check after ProcessInput and
          // the socket closes right after.
          Queue(client,
                pgwire::ErrorResponse("message exceeds server limit", "54000"));
          client.close_after_write = true;
          break;
        }
        // Level-triggered epoll will report any later bytes again. A short
        // read need not pay for a second recv just to discover EAGAIN.
        if (std::cmp_less(size, receive_limit)) {
          break;
        }
        continue;
      }
      if (size == 0) {
        // Peer half-closed (shutdown(SHUT_WR)) or closed.  Do NOT latch
        // close_after_write here: ProcessInput's `while (!close_after_write)`
        // guard would then skip the message that arrived just before the
        // EOF.  Process it first, answer it, and close afterwards.
        eof_seen = true;
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        read_blocked = true;
        break;
      }
      LOG(WARN) << "client recv failed: " << ErrnoMessage("recv");
      return false;
    }
    if (!client.input.empty()) {
      ProcessInput(client);
    }
    if (read_blocked && client.input.empty() &&
        client.input.capacity() > 4096) {
      // The input cap is max_message_bytes + 5 (16 MiB); give the capacity
      // back only once recv() has actually blocked and the backlog drained,
      // so an idle connection does not pin it — shrinking per message
      // instead would force the next pipeline frame to regrow the buffer.
      client.input.shrink_to_fit();
    }
    if (eof_seen) {
      client.peer_eof_seen = true;
    }
    if (client.peer_eof_seen && !client.read_query_in_flight &&
        client.active_stream == nullptr) {
      client.close_after_write = true;
    }
    if (client.output_overflow) {
      // Queue() refuses to buffer past the per-connection output cap: drop
      // the connection instead of propagating into the event loop.
      CloseClient(fd);
      return false;
    }
    if (client.output_offset < client.output.size() ||
        client.active_stream != nullptr) {
      return WriteClient(fd);
    }
    UpdateClientInterest(client);
    // A half-close (close_after_write) must not close the connection while a
    // read query is still running on a worker or streaming out on epoll: its
    // completion queues the response, which WriteClient() then flushes before
    // closing.
    return client.read_query_in_flight || client.active_stream != nullptr ||
           !client.close_after_write || !client.output.empty();
  }

  bool WriteClient(platform::SocketFd fd) {
    auto found = clients_.find(fd);
    if (found == clients_.end()) {
      return false;
    }
    Client& client = found->second;
    size_t bytes_sent_this_turn = 0;
    while (true) {
      while (client.output_offset < client.output.size()) {
        const char* data = client.output.data() + client.output_offset;
        const size_t remaining = client.output.size() - client.output_offset;
        const ssize_t size = platform::SendNoSignal(fd, data, remaining);
        if (size > 0) {
          const auto sent_bytes = static_cast<size_t>(size);
          client.output_offset += sent_bytes;
          bytes_sent_this_turn += sent_bytes;
          client.last_activity = std::chrono::steady_clock::now();
          continue;
        }
        if (size < 0 && errno == EINTR) {
          continue;
        }
        if (size < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          // Keep kPollWritable armed for the unsent suffix. A peer-close flag
          // raised in the same event is deliberately honored only after the
          // queue drains: a FIN must not discard already-queued replies.
          UpdateClientInterest(client);
          return true;
        }
        return false;
      }
      if (client.output_offset == client.output.size()) {
        client.output.clear();
        client.output_offset = 0;
        client.output_pending_at = {};
        const size_t retain_cap =
            client.active_stream != nullptr ? kClientStreamChunkBytes : 4096;
        // Only release buffers several times larger than the retain
        // target: shrinking a just-over-target buffer frees the allocation
        // the next streamed chunk immediately re-grows, costing a realloc
        // plus copy per chunk.
        if (client.output.capacity() > 4 * retain_cap) {
          client.output.shrink_to_fit();
        }
      }
      if (client.active_stream != nullptr) {
        if (bytes_sent_this_turn >= kMaxStreamBytesPerWriteTurn) {
          // Yield after a 1 MiB burst so one large stream cannot starve other
          // connections on the event loop; `kPollWritable` remains armed.
          break;
        }
        const bool eof_marker = client.active_stream->eof_sent;
        PumpActiveStream(client);
        if (client.output_overflow) {
          return false;
        }
        if (eof_marker) {
          // The pump ran a multi-statement continuation that re-armed the
          // marker (or finalized): yield so the next statement executes on a
          // later event-loop turn instead of chaining inline.
          break;
        }
        if (client.output_offset < client.output.size() ||
            client.active_stream != nullptr) {
          continue;
        }
      }
      if (!client.read_query_in_flight && client.active_stream == nullptr &&
          !client.input.empty() && !client.close_after_write) {
        ProcessInput(client);
        if (client.output_overflow) {
          return false;
        }
        if (client.peer_eof_seen && !client.read_query_in_flight &&
            client.active_stream == nullptr) {
          client.close_after_write = true;
        }
        if (client.output_offset < client.output.size() ||
            client.active_stream != nullptr) {
          continue;
        }
      }
      if (client.close_after_write && !client.read_query_in_flight &&
          client.active_stream == nullptr &&
          client.output_offset == client.output.size()) {
        return false;
      }
      break;
    }
    UpdateClientInterest(client);
    return true;
  }

  void UpdateClientInterest(Client& client) const {
    uint32_t interest = 0;
    if (!client.read_query_in_flight && client.active_stream == nullptr &&
        !client.peer_eof_seen && !client.close_after_write) {
      interest |= platform::kPollReadable | platform::kPollPeerClosed;
    }
    if (client.output_offset < client.output.size() ||
        client.active_stream != nullptr) {
      interest |= platform::kPollWritable;
    }
    if (interest == client.registered_events) {
      return;
    }
    if (poller_.Modify(client.fd, interest)) {
      client.registered_events = interest;
    }
  }

  static void Queue(Client& client, const std::string& message) {
    client.last_activity = std::chrono::steady_clock::now();
    if (client.output_offset != 0) {
      client.output.erase(0, client.output_offset);
      client.output_offset = 0;
    }
    if (client.output.empty() && !message.empty()) {
      client.output_pending_at = client.last_activity;
    }
    if (client.output.size() + message.size() > kMaxQueuedOutputBytes) {
      // Backpressure: refuse further buffering for a client that does not
      // drain its results.  The connection is torn down by the caller seeing
      // the flag instead of letting output grow without bound.
      client.output_overflow = true;
      return;
    }
    client.output += message;
  }

  void ProcessInput(Client& client) {
    // Consume pipelined frames by advancing `consumed` rather than
    // erase(0, total) per message: every front erase memmoves the whole
    // remaining backlog (up to 16 MiB), so a burst of N frames cost
    // O(N^2) bytes of copying. The buffer is compacted once at loop exit.
    size_t consumed = 0;
    while (!client.close_after_write) {
      if (client.read_query_in_flight || client.active_stream != nullptr) {
        break;
      }
      if (!client.startup_complete) {
        if (!ProcessStartup(client)) {
          break;
        }
        continue;
      }
      const size_t remaining = client.input.size() - consumed;
      if (remaining < 5) {
        break;
      }
      const StatusOr<uint32_t> length_field =
          pgwire::ReadUint32(client.input, consumed + 1);
      if (!length_field.HasValue()) {
        break;
      }
      const uint32_t length = length_field.Value();
      if (length < 4 || length > options_.max_message_bytes) {
        Queue(client, pgwire::ErrorResponse("invalid frontend message length",
                                            "08P01", "FATAL"));
        client.close_after_write = true;
        break;
      }
      const size_t total = static_cast<size_t>(length) + 1;
      if (remaining < total) {
        break;
      }
      const char type = client.input[consumed];
      // The handler either executes synchronously or copies statements into
      // its task. Keep this frame in place until it returns, avoiding an
      // owned payload copy (including for uncached, nonliteral SQL).
      const std::string_view payload(client.input.data() + consumed + 5,
                                     length - 4);
      if (type == 'Q') {
        if (payload.empty() || payload.back() != '\0') {
          Queue(client, pgwire::ErrorResponse(
                            "unterminated simple query message", "08P01"));
          // P1660: a protocol violation must not leave an explicit block
          // alive and committable. PostgreSQL answers such violations with
          // FATAL + disconnect; this server keeps the session (established
          // contract), so the closest equivalent is to abort the open
          // block: the 'E' status then gates every following command with
          // 25P02 until COMMIT/ROLLBACK, exactly like a SQL error.
          AbortTransactionForProtocolViolation(client);
          Queue(client, pgwire::ReadyForQuery(client.transaction_status));
        } else {
          ExecuteSimpleQuery(client, payload.substr(0, payload.size() - 1));
        }
      }
      consumed += total;
      if (type == 'X') {
        client.close_after_write = true;
        break;
      }
      if (type == 'Q') {
        continue;
      }
      if (type == 'H') {
        continue;  // Flush: queued output is already writable.
      }
      if (type == 'S') {
        Queue(client, pgwire::ReadyForQuery(client.transaction_status));
        continue;
      }
      // Unsupported extended-protocol messages get an ErrorResponse only;
      // ReadyForQuery is owed by Sync so that one Sync always closes with
      // exactly one RFQ (Parse+Sync must not yield two).
      Queue(client, pgwire::ErrorResponse(
                        "frontend message type is not supported", "0A000"));
      // P1660: a Parse/Bind sprinkled into an explicit block is likewise a
      // protocol violation; abort the block rather than letting it ride to
      // a later COMMIT at status 'T'.
      AbortTransactionForProtocolViolation(client);
    }
    if (consumed > 0) {
      client.input.erase(0, consumed);
    }
  }

  static bool ProcessStartup(Client& client) {
    if (client.input.size() < 4) {
      return false;
    }
    const StatusOr<uint32_t> length_field = pgwire::ReadUint32(client.input, 0);
    if (!length_field.HasValue()) {
      return false;
    }
    const uint32_t length = length_field.Value();
    if (length < 8 || length > kMaxPreAuthInputBytes) {
      // Every other fatal startup rejection (bad code field, parse failure,
      // missing user) uses FATAL; this one disconnects too, so keep the
      // severity consistent instead of falling back to the default ERROR.
      Queue(client,
            pgwire::ErrorResponse("invalid startup packet", "08P01", "FATAL"));
      client.close_after_write = true;
      return false;
    }
    if (client.input.size() >= 8) {
      const StatusOr<uint32_t> early_code = pgwire::ReadUint32(client.input, 4);
      if (early_code.HasValue() &&
          (early_code.Value() == pgwire::kSSLRequestCode ||
           early_code.Value() == pgwire::kGSSEncRequestCode)) {
        // SSLRequest / GSSENCRequest are fixed 8-byte frames: answer from
        // those 8 bytes alone, and consume only those 8. Waiting on the
        // claimed length would stall on a driver that lies about the size,
        // and erasing the claimed length would swallow the following
        // StartupMessage and desync the stream.
        client.input.erase(0, 8);
        Queue(client, "N");
        return true;
      }
    }
    if (client.input.size() < length) {
      return false;
    }
    const std::string_view packet(client.input.data(), length);
    const StatusOr<uint32_t> code_field = pgwire::ReadUint32(packet, 4);
    if (!code_field.HasValue()) {
      Queue(client,
            pgwire::ErrorResponse("invalid startup packet", "08P01", "FATAL"));
      client.close_after_write = true;
      return false;
    }
    const uint32_t code = code_field.Value();
    if (code == pgwire::kCancelRequestCode) {
      client.input.erase(0, length);
      client.close_after_write = true;
      return false;
    }

    std::string parse_error;
    // The optional result is checked immediately below: a corrupt startup
    // packet becomes a FATAL ErrorResponse, it is never used as-if parsed.
    std::optional<pgwire::StartupPacket> startup =
        pgwire::ParseStartupPacket(packet, &parse_error);
    client.input.erase(0, length);
    if (!startup) {
      Queue(client, pgwire::ErrorResponse(parse_error, "08P01", "FATAL"));
      client.close_after_write = true;
      return false;
    }
    // kProtocolVersion30 is the single source of truth for the handshake
    // contract: the major must match it, and any higher minor is answered
    // with NegotiateProtocolVersion offering that version back.
    const uint32_t major = startup->protocol_version >> 16U;
    const uint32_t minor = startup->protocol_version & 0xffffU;
    if (major != pgwire::kProtocolVersion30 >> 16U) {
      Queue(client, pgwire::ErrorResponse("only PostgreSQL protocol v3 is "
                                          "supported",
                                          "0A000", "FATAL"));
      client.close_after_write = true;
      return false;
    }
    const auto user = startup->parameters.find("user");
    if (user == startup->parameters.end() || user->second.empty()) {
      Queue(client, pgwire::ErrorResponse("startup packet must include a user",
                                          "28000", "FATAL"));
      client.close_after_write = true;
      return false;
    }
    if (minor > (pgwire::kProtocolVersion30 & 0xffffU)) {
      std::vector<std::string> unsupported;
      for (const auto& [name, value] : startup->parameters) {
        (void)value;
        if (name.starts_with("_pq_.")) {
          unsupported.push_back(name);
        }
      }
      Queue(client, pgwire::NegotiateProtocolVersion(
                        pgwire::kProtocolVersion30 & 0xffffU, unsupported));
    }
    client.user = user->second;
    const auto database = startup->parameters.find("database");
    client.database =
        database == startup->parameters.end() ? client.user : database->second;
    Queue(client, pgwire::AuthenticationOk());
    // All seven startup ParameterStatus frames go into one buffer and one
    // Queue(): one allocation instead of seven strings plus seven appends.
    std::string params;
    params.reserve(256);
    pgwire::AppendParameterStatus(&params, "server_version", "16.0-tinylamb");
    pgwire::AppendParameterStatus(&params, "server_version_num", "160000");
    pgwire::AppendParameterStatus(&params, "client_encoding", "UTF8");
    pgwire::AppendParameterStatus(&params, "DateStyle", "ISO, MDY");
    pgwire::AppendParameterStatus(&params, "integer_datetimes", "on");
    pgwire::AppendParameterStatus(&params, "standard_conforming_strings",
                                  "off");
    pgwire::AppendParameterStatus(&params, "TimeZone", "UTC");
    Queue(client, params);
    Queue(client,
          pgwire::BackendKeyData(static_cast<uint32_t>(platform::GetPid()),
                                 client.secret));
    Queue(client, pgwire::ReadyForQuery('I'));
    // Frontend registries are thread-local and this event-loop thread is
    // shared by every session. Start each new session from an empty set so
    // the previous session's CREATE CONSTANT / TEMP VIEW / UDF definitions
    // cannot resolve as identifiers in this one.
    ClearSessionRegistries();
    client.startup_complete = true;
    return true;
  }

  static void SendCachedLiteralReply(Client& client,
                                     const std::string& response) {
    if (client.output_overflow) {
      return;
    }
    if (!client.output.empty()) {
      Queue(client, response);
      return;
    }
    ssize_t sent = 0;
    do {
      sent =
          platform::SendNoSignal(client.fd, response.data(), response.size());
    } while (sent < 0 && errno == EINTR);
    if (std::cmp_equal(sent, response.size())) {
      return;
    }
    if (sent > 0) {
      // Queue the whole response and advance the send cursor past the
      // prefix this call already flushed: copying the tail into a fresh
      // string re-allocated and memcpy'd the unsent remainder on every
      // partial flush. Cached replies are capped at 64 KiB when filed
      // (ProcessReadCompletions), well below the 4 MiB queue cap; if the
      // queue is already full, Queue() latches output_overflow and the
      // caller closes, which is the shared backpressure contract.
      Queue(client, response);
      if (!client.output_overflow) {
        client.output_offset = static_cast<size_t>(sent);
      }
    } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      Queue(client, response);
    } else {
      client.output_overflow = true;
      client.close_after_write = true;
    }
  }

  bool TryCachedQueryPacket(Client& client, std::string_view packet) {
    if (!client.startup_complete || client.close_after_write ||
        client.output_overflow || client.read_query_in_flight ||
        client.active_stream != nullptr || client.transaction_status != 'I' ||
        client.session_has_temp_objects || packet.size() < 6 ||
        packet[0] != 'Q' || packet.back() != '\0') {
      return false;
    }
    const auto length = pgwire::ReadUint32(packet, 1);
    if (!length.HasValue() || length.Value() < 5 ||
        length.Value() > options_.max_message_bytes ||
        static_cast<size_t>(length.Value()) + 1 != packet.size()) {
      return false;
    }
    // The reply cache belongs to the connection that produced it: a literal
    // reply can embed session state, so another connection never reuses it.
    const auto found = client.literal_constant_replies.find(
        packet.substr(5, packet.size() - 6));
    if (found == client.literal_constant_replies.end()) {
      return false;
    }
    // The same exact-SQL, idle-state certificate as ExecuteSimpleQuery.
    // Partial frames and multiple frames in one recv keep the general path.
    cached_literal_reply_count_.fetch_add(1, std::memory_order_relaxed);
    SendCachedLiteralReply(client, found->second);
    return true;
  }

  void ExecuteSimpleQuery(Client& client, std::string_view sql) {
    // No pre-split reply-cache probe here: TryCachedQueryPacket already
    // checked this exact byte sequence on the packet path (or declined for
    // conditions that hold here too), so re-hashing it could only repeat a
    // miss. The post-split lookup below is the one whose key matches the
    // stored (normalized) single statement.
    auto split = pgwire::SplitSqlStatementsWithPositions(sql);
    if (!split.HasValue()) {
      // The splitter capped the statement count. Reject the message but
      // keep the connection: the frame was fully consumed, so wire
      // framing stays in sync.
      Queue(client,
            pgwire::ErrorResponse(StatusMessage(split.GetStatus()), "54000"));
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
      return;
    }
    const auto statements = std::move(split).MoveValue();
    if (statements.empty()) {
      Queue(client, pgwire::EmptyQueryResponse());
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
      return;
    }
    // Mirror gate: while another session has an open block with an uncommitted
    // catalog DDL, refuse this session's catalog statements (CREATE/DROP/
    // ALTER/ANALYZE) AND its writes to user tables (INSERT/UPDATE/DELETE/
    // MERGE/COPY/TRUNCATE/REPLACE). The latter are not catalog changes, but a
    // concurrent writer would race the stable table image the DDL build
    // relies on; rejecting them keeps that image -- and acknowledged writes
    // -- consistent.
    if (open_block_ddl_sessions_.load(std::memory_order_acquire) != 0 &&
        !client.open_block_has_ddl) {
      const bool touches_catalog = std::ranges::any_of(
          statements, [](const pgwire::SqlStatement& statement) {
            const std::string command = UppercaseCommand(statement.text);
            return IsCatalogCommand(command) ||
                   command == "INSERT" || command == "UPDATE" ||
                   command == "DELETE" || command == "MERGE" ||
                   // COPY ... FROM and TRUNCATE rewrite user tables just as
                   // surely as DML; leaving them out let a foreign DDL
                   // session publish an index over a moving table image.
                   (command == "COPY" && IsCopyFrom(statement.text)) ||
                   command == "TRUNCATE" ||
                   command == "REPLACE";
          });
      if (touches_catalog) {
        if (client.transaction) {
          AbortOpenTransaction(client);
          client.transaction_status = 'E';
        }
        Queue(client,
              pgwire::ErrorResponse(
                  "catalog change in progress in another session; retry",
                  "55006"));
        Queue(client, pgwire::ReadyForQuery(client.transaction_status));
        return;
      }
    }
    if (statements.size() == 1) {
      const std::string command = UppercaseCommand(statements[0].text);
      // BEGIN/START only registers a snapshot: no SQL planning, page reads,
      // undo, or durability wait. ROLLBACK/ABORT are NOT equally cheap --
      // they run AbortOpenTransaction (undo replay plus an abort WAL
      // record) inline -- but they stay on this epoll path because they
      // must resolve against the session's block state before any later
      // statement in the message is planned, and they never plan or read
      // user pages. Preserve the existing state machine while avoiding a
      // worker transfer for these bounded state transitions.
      if (client.transaction == nullptr &&
          (IsBeginStatement(statements[0].text) || command == "ROLLBACK" ||
           command == "ABORT")) {
        ExecuteStatements(client, statements);
        return;
      }
    }
    if (client.transaction_status == 'I' && !client.session_has_temp_objects &&
        statements.size() == 1) {
      if (auto found = client.literal_constant_replies.find(statements[0].text);
          found != client.literal_constant_replies.end()) {
        cached_literal_reply_count_.fetch_add(1, std::memory_order_relaxed);
        SendCachedLiteralReply(client, found->second);
        return;
      }
    }
    if (client.transaction_status == 'I' && IsReadOnly(statements) &&
        !client.session_has_temp_objects) {
      ScheduleReadQuery(client, statements);
      return;
    }
    if (!client.session_has_temp_objects && !read_workers_.empty() &&
        std::ranges::all_of(
            statements, [](const pgwire::SqlStatement& statement) {
              const std::string command = UppercaseCommand(statement.text);
              // P1658: SET is a pure no-op here (fixed CommandComplete, no
              // storage), so a message mixing SET with real work must not be
              // pinned to the epoll thread just because of the SET.
              return command == "SELECT" || command == "INSERT" ||
                     command == "UPDATE" || command == "DELETE" ||
                     command == "MERGE" || command == "BEGIN" ||
                     command == "START" || command == "COMMIT" ||
                     command == "END" || command == "ROLLBACK" ||
                     command == "ABORT" || command == "SET";
            })) {
      ScheduleSessionQuery(client, statements);
      return;
    }
    const bool catalog_change = std::ranges::any_of(
        statements, [](const pgwire::SqlStatement& statement) {
          return IsCatalogCommand(UppercaseCommand(statement.text));
        });
    if (catalog_change &&
        (pending_worker_tasks_.load() != 0 ||
         std::ranges::any_of(clients_, [&](const auto& entry) {
           return entry.second.id != client.id &&
                  entry.second.transaction_status != 'I';
         }))) {
      // Catalog builds use a stable table image; they do not implement online
      // index catch-up. The event loop admits no new worker while executing
      // this message. Refuse while a foreign block or worker owns an image,
      // rather than publish an index missing a concurrent writer's changes.
      if (client.transaction) {
        AbortOpenTransaction(client);
        client.transaction_status = 'E';
      }
      Queue(client,
            pgwire::ErrorResponse(
                "catalog change requires quiescent sessions; retry", "55006"));
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
      return;
    }
    ExecuteStatements(client, statements);
  }

  void EndOpenBlockDdl(Client& client) {
    if (client.open_block_has_ddl) {
      client.open_block_has_ddl = false;
      open_block_ddl_sessions_.fetch_sub(1, std::memory_order_acq_rel);
    }
  }

  static void RecordReplyDependency(Client& client,
                                    const TransactionContext& context) {
    client.reply_durability_target = std::max(
        client.reply_durability_target, context.txn_.DurabilityDependence());
  }

  static Status CommitForReply(Client& client, TransactionContext& context) {
    if (!client.permit_deferred_commit) {
      return context.PreCommit();
    }
    ASSIGN_OR_RETURN(lsn_t, target, context.PrepareCommitForDurableReply());
    client.reply_durability_target =
        std::max(client.reply_durability_target, target);
    return Status::kSuccess;
  }

  void ExecuteStatements(
      Client& client, const std::vector<pgwire::SqlStatement>& statements,
      bool permit_deferred_commit = false, SqlEngine* pooled_engine = nullptr,
      size_t start_idx = 0,
      std::unique_ptr<TransactionContext> resumed_implicit = nullptr,
      lsn_t initial_durability_target = 0) {
    // CREATE [TEMP] FUNCTION definitions are thread-local frontend state.
    // Scope them to this connection so a session cannot resolve another
    // session's function on the shared event-loop thread.
    ScopedUdfSessionScope udf_scope(client.id);
    client.permit_deferred_commit =
        permit_deferred_commit && statements.size() == 1 && start_idx == 0;
    client.reply_durability_target = initial_durability_target;
    // Count pending bytes so Queue may compact the sent prefix. On a stream
    // continuation, all chunks already buffered belong to the earlier reply
    // and must be preserved to finish messages that started on the wire.
    const size_t earlier_pending = client.output.size() - client.output_offset;
    // PostgreSQL semantics: all statements of one Query message run in a
    // single implicit transaction unless the message itself manages
    // transactions. The implicit transaction commits only after every
    // statement succeeded.  The block opens even when the message carries
    // transaction control: BEGIN adopts it as the explicit transaction and
    // COMMIT/ROLLBACK finalize it, so `UPDATE ...; ROLLBACK` in one message
    // really undoes the UPDATE.
    std::unique_ptr<TransactionContext> implicit = std::move(resumed_implicit);
    bool ok = true;
    for (size_t idx = start_idx; idx < statements.size(); ++idx) {
      const pgwire::SqlStatement& statement = statements[idx];
      // COMMIT/ROLLBACK ends the previous block, but the remaining statements
      // still share a new implicit block until the message ends or another
      // transaction-control statement consumes it.
      // A COMMIT/END/ROLLBACK/ABORT arriving with no open block must not
      // create an empty one: PostgreSQL answers it as a no-op with a
      // warning, and an empty implicit context here would allocate a
      // transaction id, snapshot, and commit/durability round-trip for
      // nothing (and would let a stray COMMIT commit a snapshot that
      // started after the previous block closed).
      const std::string opening_command = UppercaseCommand(statement.text);
      const bool closes_only =
          opening_command == "COMMIT" || opening_command == "END" ||
          opening_command == "ROLLBACK" || opening_command == "ABORT";
      // P1657: SET is answered with a fixed CommandComplete and touches no
      // storage (see ExecuteStatement), so it must not pay for an implicit
      // context either: transaction id, snapshot, commit record and a
      // durability wait per SET would hit every psql/JDBC startup handshake
      // (they send a dozen SETs). A later real statement in the same
      // message opens the context on its own turn.
      const bool needs_no_context = closes_only || opening_command == "SET";
      if (!implicit && client.transaction == nullptr &&
          client.transaction_status != 'E' && !needs_no_context) {
        implicit =
            std::make_unique<TransactionContext>(database_->BeginContext());
      }
      if (PostgresServer::RegistersTempObject(statement.text)) {
        client.session_has_temp_objects = true;
      }
      if (!ExecuteStatement(client, statement, implicit, pooled_engine,
                            statements, idx + 1, earlier_pending)) {
        ok = false;
        break;
      }
      if (client.active_stream != nullptr) {
        return;
      }
      // A later statement can abort/reset the context after earlier SELECT
      // rows have been buffered. Keep their dependency outside that context.
      if (statements.size() > 1) {
        if (client.transaction) {
          RecordReplyDependency(client, *client.transaction);
        }
        if (implicit) {
          RecordReplyDependency(client, *implicit);
        }
      }
    }
    if (implicit != nullptr) {
      if (ok) {
        RecordReplyDependency(client, *implicit);
        const Status commit = CommitForReply(client, *implicit);
        if (commit != Status::kSuccess) {
          AbortContext(client, *implicit);
          Queue(client,
                pgwire::ErrorResponse(
                    "transaction commit failed: " + StatusMessage(commit),
                    SqlStateForStatus(commit, "40001")));
        }
      } else {
        // A failing statement already aborted the context via FailStatement;
        // this covers failure paths that bypass it.
        AbortContext(client, *implicit);
      }
    }
    // Explicit blocks and partial results preceding a later error both
    // disclose observed versions before their final COMMIT (D4).
    if (client.transaction) {
      RecordReplyDependency(client, *client.transaction);
    }
    if (!client.permit_deferred_commit && client.reply_durability_target != 0) {
      const Status durable =
          database_->WaitForCommitDurable(client.reply_durability_target);
      if (durable != Status::kSuccess) {
        const bool block = client.transaction_status != 'I';
        AbortOpenTransaction(client);
        client.transaction_status = block ? 'E' : 'I';
        // Preserve queued responses from earlier Query messages on epoll;
        // only this message's still-withheld results lose their proof.
        pgwire::DiscardQueuedReply(client.output, client.output_offset,
                                   earlier_pending);
        Queue(client,
              pgwire::ErrorResponse(
                  "transaction durability failed: " + StatusMessage(durable),
                  SqlStateForStatus(durable, "XX000")));
      }
      client.reply_durability_target = 0;
    }
    Queue(client, pgwire::ReadyForQuery(client.transaction_status));
  }

  bool ExecuteStatement(
      Client& client, const pgwire::SqlStatement& statement,
      std::unique_ptr<TransactionContext>& implicit,
      SqlEngine* pooled_engine = nullptr,
      const std::vector<pgwire::SqlStatement>& statements = {},
      size_t next_statement_idx = 0, size_t earlier_pending = 0) {
    const std::string& sql = statement.text;
    const std::string command = UppercaseCommand(sql);
    if (IsBeginStatement(sql)) {
      // PostgreSQL: an explicit BEGIN inside an aborted transaction must not
      // clear the aborted state (25P02); only ROLLBACK/COMMIT recover it.
      if (client.transaction_status == 'E') {
        // P1697: PG answers BEGIN inside an aborted block with a NOTICE
        // (25001, "there is already a transaction in progress" -- PG reuses
        // the nested-BEGIN notice for the aborted block) and the BEGIN
        // command tag, keeping the aborted state. An ErrorResponse here
        // would inflate psql/JDBC error counters and drive their recovery
        // logic down a non-PG branch.
        Queue(client,
              pgwire::NoticeResponse(
                  "there is already a transaction in progress", "25001",
                  "WARNING"));
        Queue(client, pgwire::CommandComplete("BEGIN"));
        return true;
      }
      if (!client.transaction) {
        if (implicit) {
          // Adopt this message's implicit block as the explicit
          // transaction: statements that already ran in it (and everything
          // after BEGIN) commit or roll back together.
          client.transaction = std::move(implicit);
        } else {
          client.transaction =
              std::make_unique<TransactionContext>(database_->BeginContext());
        }
      } else {
        // P1693: a nested BEGIN inside an open block is a no-op that PG
        // warns about (25001); psql VERBOSITY and JDBC NoticeListeners
        // expect the NoticeResponse frame.
        Queue(client,
              pgwire::NoticeResponse(
                  "there is already a transaction in progress", "25001",
                  "WARNING"));
      }
      client.transaction_status = 'T';
      Queue(client, pgwire::CommandComplete("BEGIN"));
      return true;
    }
    if (command == "ROLLBACK" || command == "ABORT") {
      // P1693: ROLLBACK with no open block is a no-op that PG warns about
      // (25P01) before answering the tag. A statement block still open in
      // this message (implicit) or an aborted block ('E', whose transaction
      // is live for PG purposes) are both real blocks: no warning there.
      const bool no_open_block = client.transaction == nullptr &&
                                 implicit == nullptr &&
                                 client.transaction_status != 'E';
      AbortOpenTransaction(client);
      // Statements earlier in this Query message ran in the implicit
      // block; PostgreSQL rolls those back with the ROLLBACK.
      AbortAndRelease(client, implicit);
      EndOpenBlockDdl(client);
      client.transaction_status = 'I';
      if (no_open_block) {
        Queue(client, pgwire::NoticeResponse(
                          "there is no transaction in progress", "25P01",
                          "WARNING"));
      }
      Queue(client, pgwire::CommandComplete("ROLLBACK"));
      return true;
    }
    if (command == "COMMIT" || command == "END") {
      if (client.transaction_status == 'E') {
        // P1912: mirror the ROLLBACK arm and drop this message's implicit
        // block. Flipping the status alone left a live (e.g. stream-
        // resumed) implicit context behind, which ExecuteStatements would
        // happily CommitForReply -- committing writes of a block that 25P02
        // had just rejected.
        AbortAndRelease(client, implicit);
        EndOpenBlockDdl(client);
        client.transaction_status = 'I';
        Queue(client, pgwire::CommandComplete("ROLLBACK"));
        return true;
      }
      // P1693: a lone COMMIT with neither an explicit nor an in-message
      // implicit block: PG answers the tag with a 25P01 NOTICE.
      const bool no_open_block =
          client.transaction == nullptr && implicit == nullptr;
      if (client.transaction) {
        const Status status = CommitForReply(client, *client.transaction);
        client.transaction.reset();
        if (status != Status::kSuccess) {
          EndOpenBlockDdl(client);
          client.transaction_status = 'I';
          Queue(client,
                pgwire::ErrorResponse(
                    "transaction commit failed: " + StatusMessage(status),
                    SqlStateForStatus(status, "40001")));
          return false;
        }
      }
      if (implicit) {
        // Finalize the implicit block opened by earlier statements of this
        // message (no BEGIN ran, or it would have adopted the block).
        const bool finished = implicit->IsFinished();
        const Status status =
            finished ? Status::kSuccess : CommitForReply(client, *implicit);
        implicit.reset();
        if (!finished && status != Status::kSuccess) {
          EndOpenBlockDdl(client);
          client.transaction_status = 'I';
          Queue(client,
                pgwire::ErrorResponse(
                    "transaction commit failed: " + StatusMessage(status),
                    SqlStateForStatus(status, "40001")));
          return false;
        }
      }
      EndOpenBlockDdl(client);
      client.transaction_status = 'I';
      if (no_open_block) {
        Queue(client, pgwire::NoticeResponse(
                          "there is no transaction in progress", "25P01",
                          "WARNING"));
      }
      Queue(client, pgwire::CommandComplete("COMMIT"));
      return true;
    }
    if (client.transaction_status == 'E') {
      Queue(client,
            pgwire::ErrorResponse(
                "current transaction is aborted, commands ignored until "
                "end of transaction block",
                "25P02"));
      return false;
    }
    // psql startup files commonly contain SET commands. Tinylamb has no
    // session GUCs yet, so accepting them as no-ops improves client usability.
    if (command == "SET") {
      Queue(client, pgwire::CommandComplete("SET"));
      return true;
    }

    // BEGIN and DDL can arrive together. Track the block that actually
    // executes this statement, after prior transaction commands have run.
    if (client.transaction && !client.open_block_has_ddl &&
        IsCatalogCommand(command)) {
      client.open_block_has_ddl = true;
      open_block_ddl_sessions_.fetch_add(1, std::memory_order_acq_rel);
    }
    std::unique_ptr<TransactionContext> automatic;
    TransactionContext* context = client.transaction.get();
    if (context == nullptr) {
      if (implicit != nullptr) {
        // Shared implicit transaction for this Query message; committed by
        // ExecuteSimpleQuery after the whole message succeeded.
        context = implicit.get();
      } else {
        automatic =
            std::make_unique<TransactionContext>(database_->BeginContext());
        context = automatic.get();
      }
    }

    try {
      ScopedSqlCpuAffinity sql_affinity;
      std::optional<SqlEngine> local_engine;
      SqlEngine& engine = (pooled_engine != nullptr)
                              ? *pooled_engine
                              : local_engine.emplace(*database_);
      StatusOr<QueryResult> executed = engine.Execute(*context, sql);
      if (!executed.HasValue()) {
        const std::string error = engine.LastError().empty()
                                      ? StatusMessage(executed.GetStatus())
                                      : engine.LastError();
        FailStatement(client, automatic, implicit, error,
                      SqlStateForStatus(executed.GetStatus(), "42601"),
                      StatementErrorPosition(engine, statement, error));
        return false;
      }
      // `result` keeps the statement executor alive, and that executor
      // points back at `context`.  FailStatement() frees the context (it
      // resets client.transaction), so the result must die BEFORE any
      // failure path runs or the executor destructor reads freed memory
      // (ASan: use-after-free in RelationalExecutor::~RelationalExecutor).
      std::optional<QueryResult> result;
      result.emplace(std::move(executed.Value()));
      if (!result->Statement()) {
        result.reset();
        FailStatement(client, automatic, implicit,
                      "SQL statement type is unavailable", "XX000");
        return false;
      }
      const StatementType type = *result->Statement();
      if (type == StatementType::kSelect) {
        auto stream = std::make_unique<ActiveSelectStream>();
        stream->column_names = result->ColumnNames();
        stream->column_types = result->ColumnTypes();
        stream->result = std::make_unique<QueryResult>(std::move(*result));
        result.reset();
        std::string wire;
        bool eof = false;
        const Status chunk_st = PullSelectWireChunk(
            stream.get(), kClientStreamChunkBytes, &wire, &eof);
        if (chunk_st != Status::kSuccess) {
          stream->result.reset();
          stream.reset();
          FailStatement(client, automatic, implicit, chunk_st.GetMessage(),
                        SqlStateForStatus(chunk_st, "XX000"));
          return false;
        }
        // A WITH statement is not certified as a snapshot read: its leading
        // keyword proves nothing about its CTE bodies, and a data-modifying
        // CTE (WITH d AS (DELETE ... RETURNING *) SELECT ...) would succeed
        // once on the normal path, get certified here, then fail forever on
        // a read-only worker whose transaction rejects write intents. Keep
        // every WITH on the normal path, which handles both cases.
        if (!eof) {
          if (automatic) {
            RecordReplyDependency(client, *automatic);
          }
          if (implicit) {
            RecordReplyDependency(client, *implicit);
          }
          if (client.transaction) {
            RecordReplyDependency(client, *client.transaction);
          }
          if (!client.permit_deferred_commit &&
              client.reply_durability_target != 0) {
            const Status durable =
                database_->WaitForCommitDurable(client.reply_durability_target);
            if (durable != Status::kSuccess) {
              stream->result.reset();
              stream.reset();
              const bool block = client.transaction_status != 'I';
              if (automatic) {
                AbortContext(client, *automatic);
              }
              if (implicit) {
                AbortContext(client, *implicit);
              }
              AbortOpenTransaction(client);
              client.transaction_status = block ? 'E' : 'I';
              pgwire::DiscardQueuedReply(client.output, client.output_offset,
                                         earlier_pending);
              Queue(client,
                    pgwire::ErrorResponse("transaction durability failed: " +
                                              StatusMessage(durable),
                                          SqlStateForStatus(durable, "XX000")));
              client.reply_durability_target = 0;
              return false;
            }
            client.reply_durability_target = 0;
          }
          if (client.output.size() - client.output_offset + wire.size() >
              kMaxQueuedOutputBytes) {
            // P4601: the fresh chunk (e.g. one indivisible oversized row)
            // would trip Queue()'s silent overflow teardown the moment it
            // is buffered. Nothing of this statement has reached the
            // client yet (streaming starts with this queue), so answer a
            // clean limit error and discard the pulled rows loss-free.
            stream->result.reset();
            stream.reset();
            FailStatement(client, automatic, implicit,
                          "result exceeds the server's queued-output limit",
                          "53200");
            return false;
          }
          stream->owned_txn = std::move(automatic);
          stream->implicit_txn = std::move(implicit);
          stream->in_client_txn = (client.transaction != nullptr);
          stream->in_statement_block = true;
          stream->statements =
              std::make_shared<const std::vector<pgwire::SqlStatement>>(
                  statements);
          stream->next_statement_idx = next_statement_idx;
          stream->permit_deferred_commit = client.permit_deferred_commit;
          stream->reply_durability_target = client.reply_durability_target;
          client.last_activity = std::chrono::steady_clock::now();
          if (client.output_offset != 0) {
            client.output.erase(0, client.output_offset);
            client.output_offset = 0;
          }
          Queue(client, wire);
          client.active_stream = std::move(stream);
          return true;
        }
        stream->result.reset();
        stream.reset();
        Queue(client, wire);
      } else {
        int64_t affected_rows = 0;
        const Status drain_status = DrainCommandResult(*result, &affected_rows);
        if (drain_status != Status::kSuccess) {
          result.reset();
          FailStatement(
              client, automatic, implicit,
              "statement execution failed: " + StatusMessage(drain_status),
              SqlStateForStatus(drain_status, "XX000"));
          return false;
        }
        Queue(client, pgwire::CommandComplete(CommandTag(type, affected_rows)));
      }
      // Tear the executor (and its pins/read-set) down before committing so
      // FailStatement/PreCommit below can never outlive their context.
      result.reset();
      if (automatic) {
        RecordReplyDependency(client, *automatic);
        // Standalone statement in a transaction-control message: legacy
        // commit-per-statement behavior.
        const Status commit = CommitForReply(client, *automatic);
        if (commit != Status::kSuccess) {
          FailStatement(client, automatic, implicit,
                        "transaction commit failed: " + StatusMessage(commit),
                        SqlStateForStatus(commit, "40001"));
          return false;
        }
      }
      return true;
    } catch (const std::exception&) {
      const Status failure =
          StatusFromCurrentException("statement execution failed");
      FailStatement(client, automatic, implicit, failure.GetMessage(),
                    SqlStateForStatus(failure, "XX000"));
      return false;
    } catch (...) {
      // Same two-tier guard as the worker loops: a non-standard exception
      // must not escape into the event loop with the contexts still live.
      const Status failure =
          StatusFromCurrentException("statement execution failed");
      FailStatement(client, automatic, implicit, failure.GetMessage(),
                    SqlStateForStatus(failure, "XX000"));
      return false;
    }
  }

  static std::vector<pgwire::ColumnDescription> BuildColumnDescriptions(
      const std::vector<std::string>& names, const Row& sample,
      size_t fallback_count, const std::vector<ValueType>& declared_types) {
    size_t column_count = names.empty() ? fallback_count : names.size();
    if (!sample.values_.empty()) {
      column_count = sample.values_.size();
    }
    const bool have_declared =
        declared_types.size() == column_count &&
        std::ranges::any_of(declared_types,
                            [](ValueType t) { return t != ValueType::kNull; });
    std::vector<pgwire::ColumnDescription> columns(column_count);
    for (size_t i = 0; i < column_count; ++i) {
      columns[i].name =
          i < names.size() && !names[i].empty() ? names[i] : "?column?";
      columns[i].type = ValueType::kVarChar;
      // The plan's declared type is authoritative (it survives empty results
      // and leading NULLs); a kNull/absent entry falls back to the row
      // sample, then to text.
      if (have_declared && declared_types[i] != ValueType::kNull) {
        columns[i].type = declared_types[i];
      } else if (i < sample.values_.size() && !sample[i].IsNull()) {
        columns[i].type = sample[i].type;
      }
    }
    return columns;
  }

  static Status PullSelectWireChunk(ActiveSelectStream* stream,
                                    size_t max_bytes, std::string* wire_out,
                                    bool* eof_out) {
    wire_out->clear();
    *eof_out = false;
    // The PostgreSQL wire count is uint16. Reject oversized projections here
    // so the protocol encoder's CHECK_MSG never aborts the whole server
    // process on behalf of one connection.
    if (stream->column_names.size() > std::numeric_limits<uint16_t>::max()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "too many columns for PostgreSQL protocol");
    }
    static const bool disable_chunked_wire =
        std::getenv("TINYLAMB_DISABLE_CHUNKED_PGWIRE") != nullptr;
    if (!disable_chunked_wire) {
      // Lean proof: lean/TinylambOpt/ResearchProofs/ColumnarToPgWireChunkedFramingEquiv.lean (sound)
      if (max_bytes > 0) {
        wire_out->reserve(std::min<size_t>(max_bytes + 4096, 262144));
      }
      while (wire_out->size() < max_bytes) {
        if (stream->chunk_row_offset >= stream->chunk_row_count) {
          stream->chunk_row_offset = 0;
          stream->chunk_row_count = stream->result->NextBatch(&stream->chunk);
          if (stream->chunk_row_count == 0) {
            Status st = stream->result->GetStatus();
            if (st != Status::kSuccess) {
              return st;
            }
            if (!stream->header_sent) {
              *wire_out += pgwire::RowDescription(BuildColumnDescriptions(
                  stream->column_names, Row(), stream->column_names.size(),
                  stream->column_types));
              stream->header_sent = true;
            }
            *wire_out += pgwire::CommandComplete(
                "SELECT " + std::to_string(stream->row_count));
            *eof_out = true;
            return Status::kSuccess;
          }
          if (stream->chunk.ColumnCount() >
              std::numeric_limits<uint16_t>::max()) {
            return StatusError(StatusCode::kInvalidArgument,
                               "too many columns for PostgreSQL protocol");
          }
          if (!stream->header_sent) {
            *wire_out += pgwire::RowDescription(BuildColumnDescriptions(
                stream->column_names, stream->chunk.RowAt(0),
                stream->column_names.size(), stream->column_types));
            stream->header_sent = true;
          }
          if (stream->chunk.ColumnCount() != stream->column_names.size()) {
            return StatusError(
                StatusCode::kCorrupt,
                "SELECT row width disagrees with RowDescription");
          }
        }
        while (stream->chunk_row_offset < stream->chunk_row_count &&
               wire_out->size() < max_bytes) {
          RETURN_IF_FAIL(pgwire::AppendDataRowFromChunk(
              wire_out, stream->chunk, stream->chunk_row_offset));
          ++stream->chunk_row_offset;
          ++stream->row_count;
        }
      }
      return Status::kSuccess;
    }
    Row row;
    while (wire_out->size() < max_bytes) {
      if (!stream->result->Next(&row)) {
        Status st = stream->result->GetStatus();
        if (st != Status::kSuccess) {
          return st;
        }
        if (!stream->header_sent) {
          *wire_out += pgwire::RowDescription(BuildColumnDescriptions(
              stream->column_names, Row(), stream->column_names.size(),
              stream->column_types));
          stream->header_sent = true;
        }
        *wire_out += pgwire::CommandComplete("SELECT " +
                                             std::to_string(stream->row_count));
        *eof_out = true;
        return Status::kSuccess;
      }
      ++stream->row_count;
      // Width must be validated before RowDescription: the description's
      // column count comes from the sample row's values, so an oversized row
      // would hit the encoder's CHECK_MSG before this guard ran.
      if (row.values_.size() > std::numeric_limits<uint16_t>::max()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "too many columns for PostgreSQL protocol");
      }
      if (!stream->header_sent) {
        *wire_out += pgwire::RowDescription(BuildColumnDescriptions(
            stream->column_names, row, stream->column_names.size(),
            stream->column_types));
        stream->header_sent = true;
      }
      if (row.values_.size() != stream->column_names.size()) {
        return StatusError(StatusCode::kCorrupt,
                           "SELECT row width disagrees with RowDescription");
      }
      // A VARCHAR cell's wire text is exactly varchar_value (NUL->space
      // preserves length), so its size is knowable without materializing it.
      // Other types format to bounded text; the encoder CHECK_MSG remains
      // the last resort for exotic oversized payloads (e.g. huge arrays).
      for (const Value& cell : row.values_) {
        if (cell.type == ValueType::kVarChar &&
            cell.value.varchar_value.size() >
                static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
          return StatusError(StatusCode::kTooBigData,
                             "PostgreSQL DataRow cell exceeds uint32 length");
        }
      }
      *wire_out += pgwire::DataRow(row);
    }
    return Status::kSuccess;
  }

  static void CleanupActiveStream(Client& client) {
    if (!client.active_stream) {
      return;
    }
    client.active_stream->result.reset();
    if (client.active_stream->owned_txn) {
      AbortContext(client, *client.active_stream->owned_txn);
    }
    if (client.active_stream->implicit_txn) {
      AbortContext(client, *client.active_stream->implicit_txn);
    }
    client.active_stream.reset();
  }

  void PumpActiveStream(Client& client) {
    if (!client.active_stream) {
      return;
    }
    client.last_activity = std::chrono::steady_clock::now();
    std::string chunk;
    bool eof = false;
    if (!client.active_stream->eof_sent) {
      // P611: the chunk pull runs engine code (QueryResult::Next -> the
      // three-tier expression evaluators) inline on the epoll thread. The
      // worker paths wrap every task in a two-tier guard; without one here
      // a leaked exception escaped Run() and std::terminate took down all
      // connections over one bad row.
      Status chunk_st = Status::kSuccess;
      if (!client.active_stream->pending_wire.empty()) {
        // P4601: retry a chunk deferred by an earlier turn -- without
        // pulling more rows -- as soon as the socket has drained.
        chunk = std::move(client.active_stream->pending_wire);
        eof = client.active_stream->pending_eof;
        client.active_stream->pending_eof = false;
      } else {
        try {
          chunk_st = PullSelectWireChunk(client.active_stream.get(),
                                         kClientStreamChunkBytes, &chunk, &eof);
        } catch (const std::exception&) {
          chunk_st = StatusFromCurrentException("stream chunk failed");
        } catch (...) {
          chunk_st = StatusError(StatusCode::kRuntimeError,
                                 "stream chunk failed (unknown exception)");
        }
      }
      // Later pulls can observe a commit that the first chunk did not read.
      // Once queued, a partial protocol frame cannot be retracted.
      if (chunk_st == Status::kSuccess) {
        const auto& stream = *client.active_stream;
        const auto* context = stream.owned_txn      ? stream.owned_txn.get()
                              : stream.implicit_txn ? stream.implicit_txn.get()
                                                    : client.transaction.get();
        if (context != nullptr) {
          const lsn_t target = context->txn_.DurabilityDependence();
          if (target > database_->DurableWalLSN()) {
            chunk_st = database_->WaitForCommitDurable(target);
          }
        }
      }
      if (chunk_st == Status::kSuccess && !chunk.empty() &&
          client.output.size() - client.output_offset + chunk.size() >
              kMaxQueuedOutputBytes) {
        if (client.output.size() == client.output_offset) {
          // Even an empty queue cannot hold this chunk (a single row above
          // the cap): answer a clean limit error instead of Queue()'s
          // silent drop + close (P4601).
          chunk_st =
              StatusError(StatusCode::kMemoryLimitExceeded,
                          "result exceeds the server's queued-output limit");
        } else {
          client.active_stream->pending_wire = std::move(chunk);
          client.active_stream->pending_eof = eof;
          return;
        }
      }
      if (chunk_st != Status::kSuccess) {
        auto failed = std::move(client.active_stream);
        failed->result.reset();
        if (failed->owned_txn) {
          AbortContext(client, *failed->owned_txn);
        }
        if (failed->implicit_txn) {
          AbortContext(client, *failed->implicit_txn);
        }
        if (failed->in_client_txn) {
          AbortOpenTransaction(client);
          client.transaction_status = 'E';
        }
        Queue(client,
              pgwire::ErrorResponse(chunk_st.GetMessage(),
                                    SqlStateForStatus(chunk_st, "XX000")));
        Queue(client, pgwire::ReadyForQuery(client.transaction_status));
        return;
      }

      if (client.output_offset != 0) {
        client.output.erase(0, client.output_offset);
        client.output_offset = 0;
      }
      Queue(client, chunk);
      if (!eof) {
        return;
      }
      client.active_stream->eof_sent = true;
    }

    auto finished = std::move(client.active_stream);
    finished->result.reset();

    if (finished->in_statement_block) {
      bool ok = true;
      lsn_t dep = finished->reply_durability_target;
      if (finished->owned_txn) {
        dep = std::max(dep, finished->owned_txn->txn_.DurabilityDependence());
        if (!finished->owned_txn->IsFinished()) {
          const Status commit = finished->owned_txn->PreCommit();
          if (commit != Status::kSuccess) {
            ok = false;
            // PreCommit failure does not finish the context; abort it
            // explicitly instead of letting the unique_ptr destructor drop
            // a transaction that still holds write intents.
            AbortContext(client, *finished->owned_txn);
            Queue(client,
                  pgwire::ErrorResponse(
                      "transaction commit failed: " + StatusMessage(commit),
                      SqlStateForStatus(commit, "40001")));
          }
        }
      }
      if (ok && finished->next_statement_idx < finished->statements->size()) {
        ExecuteStatements(client, *finished->statements,
                          /*permit_deferred_commit=*/false, nullptr,
                          finished->next_statement_idx,
                          std::move(finished->implicit_txn), dep);
        return;
      }
      if (finished->implicit_txn) {
        dep =
            std::max(dep, finished->implicit_txn->txn_.DurabilityDependence());
        if (ok && !finished->implicit_txn->IsFinished()) {
          const Status commit = finished->implicit_txn->PreCommit();
          if (commit != Status::kSuccess) {
            AbortContext(client, *finished->implicit_txn);
            Queue(client,
                  pgwire::ErrorResponse(
                      "transaction commit failed: " + StatusMessage(commit),
                      SqlStateForStatus(commit, "40001")));
          }
        } else if (!ok) {
          AbortContext(client, *finished->implicit_txn);
        }
      }
      if (client.transaction) {
        dep = std::max(dep, client.transaction->txn_.DurabilityDependence());
      }
      if (dep != 0) {
        const Status durable = database_->WaitForCommitDurable(dep);
        if (durable != Status::kSuccess) {
          const bool block = client.transaction_status != 'I';
          AbortOpenTransaction(client);
          client.transaction_status = block ? 'E' : 'I';
          Queue(client,
                pgwire::ErrorResponse(
                    "transaction durability failed: " + StatusMessage(durable),
                    SqlStateForStatus(durable, "XX000")));
        }
      }
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
      return;
    }

    // Read-only snapshot stream continuation.
    // P4591: TEMP views/UDFs resolve through a thread-local registry pinned
    // to the connection id. ExecuteStatements installs that scope but
    // returns early when a stream starts, so this continuation loop -- which
    // also runs statements on the event-loop thread -- must re-pin it
    // itself, or the second statement of `SELECT ...; SELECT temp_udf(...)`
    // resolves against scope 0 and dies with "unknown function".
    ScopedUdfSessionScope udf_scope(client.id);
    std::unique_ptr<TransactionContext> context =
        std::move(finished->owned_txn);
    bool ok = true;
    lsn_t observed_dependency = finished->reply_durability_target;
    for (size_t idx = finished->next_statement_idx;
         idx < finished->statements->size(); ++idx) {
      const pgwire::SqlStatement& statement = (*finished->statements)[idx];
      try {
        SqlEngine engine(*database_);
        StatusOr<QueryResult> executed =
            engine.Execute(*context, statement.text);
        if (!executed.HasValue()) {
          const std::string error = engine.LastError().empty()
                                        ? StatusMessage(executed.GetStatus())
                                        : engine.LastError();
          Queue(client,
                pgwire::ErrorResponse(
                    error, SqlStateForStatus(executed.GetStatus(), "42601"),
                    "ERROR", StatementErrorPosition(engine, statement, error)));
          ok = false;
          break;
        }
        QueryResult next_res = std::move(executed.Value());
        if (!next_res.Statement() ||
            *next_res.Statement() != StatementType::kSelect) {
          Queue(client,
                pgwire::ErrorResponse(
                    "only SELECT statements may use a read worker", "XX000"));
          ok = false;
          break;
        }
        auto next_stream = std::make_unique<ActiveSelectStream>();
        next_stream->column_names = next_res.ColumnNames();
        next_stream->column_types = next_res.ColumnTypes();
        next_stream->result =
            std::make_unique<QueryResult>(std::move(next_res));
        std::string next_wire;
        bool next_eof = false;
        const Status next_st = PullSelectWireChunk(
            next_stream.get(), kClientStreamChunkBytes, &next_wire, &next_eof);
        if (next_st != Status::kSuccess) {
          next_stream->result.reset();
          Queue(client,
                pgwire::ErrorResponse(next_st.GetMessage(),
                                      SqlStateForStatus(next_st, "XX000")));
          ok = false;
          break;
        }
        if (client.output.size() - client.output_offset + next_wire.size() >
            kMaxQueuedOutputBytes) {
          // P4601: oversized first chunk on the continuation path: same
          // clean limit error as the worker and install paths.
          next_stream->result.reset();
          Queue(client, pgwire::ErrorResponse(
                            "result exceeds the server's queued-output limit",
                            "53200"));
          ok = false;
          break;
        }
        observed_dependency =
            std::max(observed_dependency, context->txn_.DurabilityDependence());
        if (observed_dependency > database_->DurableWalLSN()) {
          const Status durable =
              database_->WaitForCommitDurable(observed_dependency);
          if (durable != Status::kSuccess) {
            next_stream->result.reset();
            Queue(client,
                  pgwire::ErrorResponse("read transaction durability failed: " +
                                            StatusMessage(durable),
                                        SqlStateForStatus(durable, "XX000")));
            ok = false;
            observed_dependency = 0;
            break;
          }
        }
        if (!next_eof) {
          next_stream->owned_txn = std::move(context);
          next_stream->in_statement_block = false;
          next_stream->statements = finished->statements;
          next_stream->next_statement_idx = idx + 1;
          next_stream->reply_durability_target = observed_dependency;
          Queue(client, next_wire);
          client.active_stream = std::move(next_stream);
          return;
        }
        next_stream->result.reset();
        // The result fit in one chunk: queue it and re-arm the continuation
        // so the next statement runs on a later event-loop turn. Running the
        // remaining statements inline here monopolized the loop (accept and
        // every other connection stalled) and kept executing after the
        // client died.
        next_stream->eof_sent = true;
        next_stream->owned_txn = std::move(context);
        next_stream->in_statement_block = false;
        next_stream->statements = finished->statements;
        next_stream->next_statement_idx = idx + 1;
        next_stream->reply_durability_target = observed_dependency;
        Queue(client, next_wire);
        client.active_stream = std::move(next_stream);
        return;
      } catch (const std::exception&) {
        const Status failure =
            StatusFromCurrentException("stream statement failed");
        Queue(client,
              pgwire::ErrorResponse(failure.GetMessage(),
                                    SqlStateForStatus(failure, "XX000")));
        ok = false;
        break;
      } catch (...) {
        // Same two-tier guard as ReadWorkerLoop: a non-standard exception
        // must not escape the event loop, and the partial result is torn
        // down during unwinding so the post-loop commit/abort below never
        // runs against a live executor.
        const Status failure =
            StatusFromCurrentException("stream statement failed");
        Queue(client,
              pgwire::ErrorResponse(failure.GetMessage(),
                                    SqlStateForStatus(failure, "XX000")));
        ok = false;
        break;
      }
    }
    if (context && !context->IsFinished()) {
      if (ok) {
        const Status commit = context->PreCommit();
        if (commit != Status::kSuccess) {
          ok = false;
          AbortContext(client, *context);
          Queue(client,
                pgwire::ErrorResponse(
                    "read transaction commit failed: " + StatusMessage(commit),
                    SqlStateForStatus(commit, "40001")));
        }
      } else {
        AbortContext(client, *context);
      }
    }
    if (!ok && observed_dependency != 0) {
      const Status durable =
          database_->WaitForCommitDurable(observed_dependency);
      if (durable != Status::kSuccess) {
        Queue(client,
              pgwire::ErrorResponse("read transaction durability failed: " +
                                        StatusMessage(durable),
                                    SqlStateForStatus(durable, "XX000")));
      }
    }
    Queue(client, pgwire::ReadyForQuery('I'));
  }

  // Only bare SELECT statements may use the snapshot read worker. WITH and
  // EXPLAIN are classified by their first word but may wrap writes
  // (WITH ... DELETE/UPDATE/INSERT, EXPLAIN ANALYZE <DML>), so they take the
  // normal execution path which handles them correctly.
  static bool IsReadOnly(const std::vector<pgwire::SqlStatement>& statements) {
    return std::ranges::all_of(
        statements, [](const pgwire::SqlStatement& statement) {
          return UppercaseCommand(statement.text) == "SELECT";
        });
  }

  void EnqueueSqlTask(ReadTask task) {
    // Only these tasks can publish a new commit record in this message.
    // Reads and DML inside an explicit block may have dependency barriers,
    // but waiting for their future COMMIT would add a useless batch delay.
    if (!task.cleanup_only && task.session && task.statements.size() == 1) {
      const auto command = UppercaseCommand(task.statements.front().text);
      task.potential_commit = command == "COMMIT" || command == "END" ||
                              (task.session->transaction_status == 'I' &&
                               (command == "INSERT" || command == "UPDATE" ||
                                command == "DELETE" || command == "MERGE"));
    }
    size_t chosen = 0;
    {
      std::scoped_lock lock(read_task_mutex_);
      const size_t count = read_workers_.size();
      if (count == 0) {
        // Degraded pool (all spawns failed or an explicit zero-worker
        // start): no queue consumer exists and `client_id % count` below
        // would be a deterministic SIGFPE. The schedulers already route
        // regular queries through their inline paths; finish any session
        // state the task carries and answer stray tasks explicitly
        // instead of letting them sit unconsumed.
        if (task.session) {
          // A session query can carry an open-block DDL claim out of the
          // event-loop thread. No worker will process this task, so release
          // the global admission counter here or every later DDL is stuck
          // behind a session that no longer exists. Idempotent for
          // cleanup_only tasks (ScheduleSessionCleanup already ended it).
          EndOpenBlockDdl(*task.session);
          AbortOpenTransaction(*task.session);
        }
        if (!task.cleanup_only) {
          const auto stray = clients_.find(task.client_fd);
          if (stray != clients_.end() && stray->second.id == task.client_id) {
            // ScheduleSessionQuery latched the in-flight gate before handing
            // us this task. With no worker there is no completion to clear
            // it, so release it here or ProcessInput stays blocked forever.
            stray->second.read_query_in_flight = false;
            if (stray->second.transaction_status != 'I') {
              stray->second.transaction_status = 'E';
            }
            std::string response =
                pgwire::ErrorResponse("no read workers available", "53000");
            response +=
                pgwire::ReadyForQuery(stray->second.transaction_status);
            Queue(stray->second, response);
          }
        }
        return;
      }
      const auto preferred = static_cast<size_t>(task.client_id % count);
      chosen = preferred;
      if (read_worker_running_[preferred] || !read_tasks_[preferred].empty()) {
        size_t best = read_tasks_[preferred].size() +
                      (read_worker_running_[preferred] ? 1 : 0);
        for (size_t worker = 0; worker < count; ++worker) {
          const size_t load = read_tasks_[worker].size() +
                              (read_worker_running_[worker] ? 1 : 0);
          if (load < best) {
            best = load;
            chosen = worker;
          }
        }
      }
      const bool potential_commit = task.potential_commit;
      read_tasks_[chosen].push_back(std::move(task));
      pending_worker_tasks_.fetch_add(1);
      if (potential_commit) {
        potential_commit_tasks_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    // Quiet connections reuse a recently active worker. Busy workers steal
    // pending work after finishing, so a long query cannot pin an idle pool.
    read_task_ready_[chosen].notify_one();
  }

  void ScheduleReadQuery(Client& client,
                         const std::vector<pgwire::SqlStatement>& statements) {
    client.read_query_in_flight = true;
    client.read_started_at = std::chrono::steady_clock::now();
    if (read_workers_.empty()) {
      std::string response;
      std::unique_ptr<ActiveSelectStream> stream;
      try {
        auto res = ExecuteReadTask(statements);
        response = std::move(res.first);
        stream = std::move(res.second);
      } catch (const std::exception&) {
        const Status failure = StatusFromCurrentException("read worker failed");
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + failure.GetMessage(),
            SqlStateForStatus(failure, "XX000"));
        response += pgwire::ReadyForQuery('I');
      } catch (...) {
        const Status failure = StatusFromCurrentException("read worker failed");
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + failure.GetMessage(),
            SqlStateForStatus(failure, "XX000"));
        response += pgwire::ReadyForQuery('I');
      }
      client.read_query_in_flight = false;
      if (stream != nullptr) {
        if (client.output_offset != 0) {
          client.output.erase(0, client.output_offset);
          client.output_offset = 0;
        }
        Queue(client, response);
        client.active_stream = std::move(stream);
      } else {
        Queue(client, response);
      }
      return;
    }
    ReadTask task{.client_fd = client.fd,
                  .client_id = client.id,
                  .statements = statements,
                  .session = nullptr};
    task.plan_cache = std::move(client.plan_cache);
    if (client.output.empty() && !client.output_overflow) {
      task.reply_socket = client.reply_socket;
    }
    EnqueueSqlTask(std::move(task));
  }

  void ScheduleSessionQuery(
      Client& client, const std::vector<pgwire::SqlStatement>& statements) {
    // Only SQL state crosses threads. Socket/input/output buffers stay owned
    // by epoll, and at most one task per connection is ever in flight.
    auto session = std::move(client.worker_shell);
    if (!session) {
      session =
          std::make_unique<Client>(client.fd, client.id, client.secret, false);
    }
    session->output.clear();
    session->output_offset = 0;
    session->output_overflow = false;
    session->reply_durability_target = 0;
    session->transaction = std::move(client.transaction);
    session->transaction_status = client.transaction_status;
    session->open_block_has_ddl =
        std::exchange(client.open_block_has_ddl, false);
    session->plan_cache = std::move(client.plan_cache);
    client.read_query_in_flight = true;
    client.read_started_at = std::chrono::steady_clock::now();
    ReadTask task{.client_fd = client.fd,
                  .client_id = client.id,
                  .statements = statements,
                  .session = std::move(session)};
    if (client.output.empty() && !client.output_overflow) {
      task.reply_socket = client.reply_socket;
    }
    EnqueueSqlTask(std::move(task));
  }

  void ScheduleSessionCleanup(std::unique_ptr<Client> session) {
    if (!session) {
      return;
    }
    EndOpenBlockDdl(*session);
    if (!session->transaction) {
      return;
    }
    EnqueueSqlTask({.client_fd = platform::kInvalidSocketFd,
                    .client_id = 0,
                    .statements = {},
                    .session = std::move(session),
                    .cleanup_only = true});
  }

  void StartReadWorkers() {
    if (!read_workers_.empty()) {
      return;
    }
    // A restart (e.g. a second Listen() after a degraded/zero-worker start)
    // can still own a joinable durability worker. Assigning over a joinable
    // std::thread calls std::terminate, so park and join it first.
    if (durability_worker_.joinable()) {
      StopDurabilityWorker();
    }
    if (read_worker_count_ == 0) {
      deferred_commits_enabled_.store(false, std::memory_order_release);
      database_->SetCommitDrivenWalSync(false);
      return;
    }
    try {
      read_workers_.reserve(read_worker_count_);
    } catch (...) {
      read_worker_count_ = 0;
      deferred_commits_enabled_.store(false, std::memory_order_release);
      database_->SetCommitDrivenWalSync(false);
      LOG(WARN) << "read worker pool could not reserve thread storage; "
                   "queries run on the event loop and commits stay synchronous";
      return;
    }
    // StopDurabilityWorker/StopReadWorkers latch these stop flags. Clear them
    // for a fresh start: otherwise the new durability worker exits at the
    // top of its loop and the read workers retire immediately, stranding
    // commit replies in a queue nobody drains.
    {
      std::scoped_lock lock(durability_mutex_);
      durability_stopping_ = false;
    }
    {
      std::scoped_lock lock(read_task_mutex_);
      read_workers_stopping_ = false;
    }
    // stopping_ itself was already cleared at Listen() entry; clearing it
    // again here would erase a RequestStop() that arrived during startup.
    try {
      durability_worker_ = std::thread([this] { DurabilityWorkerLoop(); });
      database_->SetCommitDrivenWalSync(true);
      deferred_commits_enabled_.store(true, std::memory_order_release);
    } catch (...) {
      // Retain the blocking commit path if the coordinator cannot start,
      // but say so: a silently degraded durability queue reads as an
      // unexplained throughput drop in the field.
      // P1483: the throw may land AFTER the thread started (e.g. from the
      // DB call); undo the whole stack -- flag, WAL-sync mode, and the
      // joinable thread -- so a failed startup leaves no orphan worker
      // idling behind a queue that no longer feeds it, symmetric with the
      // read-worker loop below which shrinks read_worker_count_ on failure.
      deferred_commits_enabled_.store(false, std::memory_order_release);
      database_->SetCommitDrivenWalSync(false);
      StopDurabilityWorker();
      LOG(WARN) << "durability worker could not start; commits stay "
                   "synchronous";
    }
    const size_t requested_workers = read_worker_count_;
    for (size_t i = 0; i < read_worker_count_; ++i) {
      try {
        read_workers_.emplace_back([this, i] { ReadWorkerLoop(i); });
      } catch (...) {
        read_worker_count_ = read_workers_.size();
        LOG(WARN) << "read worker pool degraded: started " << read_worker_count_
                  << " of " << requested_workers
                  << " workers; remaining work runs on the event loop";
        break;
      }
    }
    if (read_workers_.empty() && durability_worker_.joinable()) {
      // P2084: zero read workers means the thread budget is exhausted.
      // Keeping the durability coordinator alive leaves the server in a
      // half-degraded state -- every query on the epoll thread AND every
      // commit routed through an orphan deferred-durability queue. Tear
      // that stack down too: with no pool the synchronous commit path is
      // the only coherent choice.
      LOG(WARN) << "durability coordinator stopped: no read workers "
                   "survived; commits stay synchronous";
      database_->SetCommitDrivenWalSync(false);
      StopDurabilityWorker();
    }
  }

  // P1483/P2084: park and join the durability worker (idempotent; tolerates
  // a never-started thread). deferred_commits_enabled_ gates the producer,
  // so flip it BEFORE signaling to strand nothing in the queue.
  void StopDurabilityWorker() {
    deferred_commits_enabled_.store(false, std::memory_order_release);
    {
      std::scoped_lock lock(durability_mutex_);
      durability_stopping_ = true;
    }
    durability_ready_.notify_all();
    if (durability_worker_.joinable()) {
      durability_worker_.join();
    }
  }

  void StopReadWorkers() {
    RequestStop();
    deferred_commits_enabled_.store(false, std::memory_order_release);
    // Drain accepted messages and abort unfinished blocks on workers before
    // the database is destroyed. No worker references a map-owned Client.
    while (!clients_.empty()) {
      CloseClient(clients_.begin()->first);
    }
    {
      std::scoped_lock lock(read_completion_mutex_);
      for (ReadCompletion& completion : read_completions_) {
        pending_worker_tasks_.fetch_sub(1);
        completion.active_stream.reset();
        if (completion.session) {
          CleanupActiveStream(*completion.session);
        }
        ScheduleSessionCleanup(std::move(completion.session));
      }
      read_completions_.clear();
    }
    {
      std::scoped_lock lock(read_task_mutex_);
      read_workers_stopping_ = true;
    }
    for (auto& ready : read_task_ready_) {
      ready.notify_all();
    }
    for (std::thread& worker : read_workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    read_workers_.clear();
    // Use the shared helper rather than duplicating the durability stop
    // sequence here, so both shutdown paths stay in lockstep.
    StopDurabilityWorker();
  }

  void ReadWorkerLoop(size_t worker) {
    // The frontend's per-engine catalog-epoch guard avoids rescanning the
    // persisted view catalog for every statement on a quiet connection.
    // This engine stays on its worker; mutable plans stay session-owned.
    SqlEngine pooled_engine(*database_);
    while (true) {
      ReadTask task;
      {
        std::unique_lock lock(read_task_mutex_);
        const auto pending = [&] {
          return std::ranges::any_of(
              read_tasks_, [](const auto& queue) { return !queue.empty(); });
        };
        read_task_ready_[worker].wait(
            lock, [&] { return read_workers_stopping_ || pending(); });
        if (read_workers_stopping_ && !pending()) {
          return;
        }
        size_t owner = worker;
        if (read_tasks_[owner].empty()) {
          for (size_t other = 0; other < read_tasks_.size(); ++other) {
            if (!read_tasks_[other].empty()) {
              owner = other;
              break;
            }
          }
        }
        task = std::move(read_tasks_[owner].front());
        read_tasks_[owner].pop_front();
        read_worker_running_[worker] = true;
      }
      if (task.cleanup_only) {
        AbortOpenTransaction(*task.session);
        pending_worker_tasks_.fetch_sub(1);
        {
          std::scoped_lock lock(read_task_mutex_);
          read_worker_running_[worker] = false;
        }
        continue;
      }
      // The admission count includes pending durability replies: a writer
      // waiting for its barrier must retain CPU headroom for its next step.
      ScopedQueryParallelismBudget parallelism(&pending_worker_tasks_, 8,
                                               &active_read_queries_);
      const auto execution_begin =
          metrics_enabled_ ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
      if (metrics_enabled_) {
        queue_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    execution_begin - task.enqueued_at)
                    .count()),
            std::memory_order_relaxed);
      }
      const bool snapshot_read = !task.session;
      if (snapshot_read) {
        const size_t active = active_read_queries_.fetch_add(1) + 1;
        size_t peak = peak_concurrent_reads_.load();
        while (peak < active &&
               !peak_concurrent_reads_.compare_exchange_weak(peak, active)) {
        }
      }
      // A throwing worker would escape ReadWorkerLoop and take down the
      // whole server via std::terminate. ExecuteReadTask only catches
      // std::exception per statement; allocation failures, unknown
      // exceptions, and transaction-context setup must also surface as a
      // per-query error response, never as a dead thread.
      std::string response;
      std::unique_ptr<ActiveSelectStream> active_stream;
      lsn_t read_target = 0;
      bool literal_constant = false;
      try {
        ScopedPreparedPlanSession plans(task.session
                                            ? task.session->plan_cache.get()
                                            : task.plan_cache.get());
        if (task.session) {
          ExecuteStatements(
              *task.session, task.statements,
              deferred_commits_enabled_.load(std::memory_order_acquire),
              &pooled_engine);
          response = std::move(task.session->output);
          active_stream = std::move(task.session->active_stream);
        } else {
          auto read_res = ExecuteReadTask(
              task.statements,
              deferred_commits_enabled_.load(std::memory_order_acquire)
                  ? &read_target
                  : nullptr,
              &literal_constant, &pooled_engine);
          response = std::move(read_res.first);
          active_stream = std::move(read_res.second);
        }
      } catch (const std::exception&) {
        const Status failure = StatusFromCurrentException("read worker failed");
        active_stream.reset();
        if (task.session) {
          CleanupActiveStream(*task.session);
          AbortOpenTransaction(*task.session);
          if (task.session->transaction_status == 'T') {
            task.session->transaction_status = 'E';
          }
        }
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + failure.GetMessage(),
            SqlStateForStatus(failure, "XX000"));
        response += pgwire::ReadyForQuery(
            task.session ? task.session->transaction_status : 'I');
      } catch (...) {
        const Status failure = StatusFromCurrentException("read worker failed");
        active_stream.reset();
        if (task.session) {
          CleanupActiveStream(*task.session);
          AbortOpenTransaction(*task.session);
          if (task.session->transaction_status == 'T') {
            task.session->transaction_status = 'E';
          }
        }
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + failure.GetMessage(),
            SqlStateForStatus(failure, "XX000"));
        response += pgwire::ReadyForQuery(
            task.session ? task.session->transaction_status : 'I');
      }
      if (metrics_enabled_) {
        execution_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - execution_begin)
                    .count()),
            std::memory_order_relaxed);
        executed_tasks_.fetch_add(1, std::memory_order_relaxed);
      }
      if (snapshot_read) {
        active_read_queries_.fetch_sub(1);
      }

      {
        std::scoped_lock lock(read_task_mutex_);
        read_worker_running_[worker] = false;
      }
      if (read_target != 0) {
        task.session =
            std::make_unique<Client>(task.client_fd, task.client_id, 0, false);
        task.session->reply_durability_target = read_target;
        task.session->plan_cache = std::move(task.plan_cache);
      }
      ReadCompletion completion{.client_fd = task.client_fd,
                                .client_id = task.client_id,
                                .response = std::move(response),
                                .session = std::move(task.session),
                                .literal_constant_sql = {},
                                .active_stream = std::move(active_stream)};
      completion.plan_cache = std::move(task.plan_cache);
      completion.reply_socket = std::move(task.reply_socket);
      if (literal_constant && !completion.active_stream &&
          task.statements.size() == 1) {
        completion.literal_constant_sql = task.statements[0].text;
      }
      const bool needs_durability =
          completion.session && completion.session->reply_durability_target >
                                    database_->DurableWalLSN();
      if (needs_durability) {
        {
          std::scoped_lock lock(durability_mutex_);
          durability_tasks_.push_back(std::move(completion));
          // Publish the queue entry before retiring its candidate, under the
          // coordinator's lock. Its cohort never observes a missing commit.
          if (task.potential_commit) {
            potential_commit_tasks_.fetch_sub(1, std::memory_order_relaxed);
          }
        }
        durability_ready_.notify_one();
      } else {
        if (task.potential_commit) {
          {
            std::scoped_lock lock(durability_mutex_);
            potential_commit_tasks_.fetch_sub(1, std::memory_order_relaxed);
          }
          // Also wake a batch when a candidate finishes without a new commit
          // (zero-row DML, error, or an already-durable dependency).
          durability_ready_.notify_one();
        }
        PublishReadCompletion(std::move(completion));
      }
    }
  }

  void PublishReadCompletion(ReadCompletion completion) {
    // One SQL task remains in flight until epoll processes this completion.
    // There was no earlier queued output at admission, so a short ready
    // reply may go directly to the socket. Durability callers reach here
    // only after their own/dependency LSN proof. Partial sends and EAGAIN
    // return the unsent suffix to epoll before it admits another message.
    std::unique_ptr<Client> aborted_session;
    bool completed_on_loop = false;
    bool wake_loop = false;
    {
      std::scoped_lock lock(read_completion_mutex_);
      if (!stopping_.load()) {
        if (completion.reply_socket && !completion.active_stream &&
            !completion.response.empty() && completion.response.size() <= 4096 &&
            completion.literal_constant_sql.empty() &&
            (!completion.session || !completion.session->output_overflow)) {
          int failure = 0;
          const ssize_t sent =
              completion.reply_socket->Send(completion.response, &failure);
          if (sent > 0) {
            completion.response.erase(0, static_cast<size_t>(sent));
            if (completion.response.empty()) {
              direct_reply_count_.fetch_add(1, std::memory_order_relaxed);
            }
          } else if (sent < 0 && failure != EAGAIN &&
                     failure != EWOULDBLOCK) {
            completion.reply_send_failed = true;
          }
        }
        // A nonempty queue already has a notification in flight, or is
        // about to be drained by epoll. Swapping it to empty and deciding
        // whether to notify share this mutex, so no completion loses a wake.
        wake_loop = read_completions_.empty();
        read_completions_.push_back(std::move(completion));
        completed_on_loop = true;
      } else {
        aborted_session = std::move(completion.session);
      }
    }
    if (!completed_on_loop) {
      if (aborted_session) {
        AbortOpenTransaction(*aborted_session);
      }
      pending_worker_tasks_.fetch_sub(1);
    }
    if (wake_loop) {
      waker_.Wake();
    }
  }

  void DurabilityWorkerLoop() {
    std::chrono::microseconds batch_window{100};
    for (;;) {
      std::deque<ReadCompletion> batch;
      {
        std::unique_lock lock(durability_mutex_);
        durability_ready_.wait(lock, [&] {
          return durability_stopping_ || !durability_tasks_.empty();
        });
        if (durability_tasks_.empty() && durability_stopping_) {
          return;
        }
        // Wait only for peer tasks that can create a commit now. Explicit
        // block SELECT/UPDATE steps cannot join this cohort with a future
        // COMMIT; their dependency targets retain the same durability gate.
        if (potential_commit_tasks_.load(std::memory_order_relaxed) != 0 &&
            !durability_stopping_) {
          durability_ready_.wait_for(lock, batch_window, [&] {
            return durability_stopping_ ||
                   potential_commit_tasks_.load(std::memory_order_relaxed) == 0;
          });
        }
        batch.swap(durability_tasks_);
      }
      lsn_t target = 0;
      for (const auto& completion : batch) {
        // The processing loop below explicitly handles a null session; do
        // not dereference one here before that guard gets a chance to run.
        if (completion.session != nullptr) {
          target =
              std::max(target, completion.session->reply_durability_target);
        }
      }
      Status durable = Status::kSuccess;
      const auto sync_begin = std::chrono::steady_clock::now();
      // A wake can lose its batch to a stopping/shutdown race. LSN 0 is not
      // a meaningful wait target -- skip it instead of taking the lock/log
      // path for an empty cohort.
      if (!batch.empty() && target != 0) {
        try {
          durable = database_->WaitForCommitDurable(target);
        } catch (const std::exception& error) {
          durable = StatusError(StatusCode::kIOError, error.what());
        } catch (...) {
          durable =
              StatusError(StatusCode::kIOError, "durability worker failed");
        }
      }
      const auto sync_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - sync_begin)
              .count();
      // A slow disk makes premature half-cohort syncs expensive; a fast disk
      // keeps the window short. This affects grouping, never the LSN proof.
      batch_window = std::chrono::microseconds(std::clamp<int64_t>(
          (batch_window.count() * 3 + sync_us) / 4, 100, 1000));
      if (metrics_enabled_) {
        durability_batches_.fetch_add(1, std::memory_order_relaxed);
      }
      for (auto& completion : batch) {
        if (!completion.session) {
          // A completion without its session cannot safely restore the
          // connection's transaction state, regardless of the sync result.
          completion.active_stream.reset();
          completion.reply_send_failed = true;
          completion.ready_at = std::chrono::steady_clock::now();
          PublishReadCompletion(std::move(completion));
          continue;
        }
        if (durable != Status::kSuccess) {
          completion.active_stream.reset();
          CleanupActiveStream(*completion.session);
          const bool block = completion.session->transaction_status == 'T';
          AbortOpenTransaction(*completion.session);
          completion.session->transaction_status = block ? 'E' : 'I';
          completion.response =
              pgwire::ErrorResponse(
                  "transaction durability failed: " + StatusMessage(durable),
                  SqlStateForStatus(durable, "XX000")) +
              pgwire::ReadyForQuery(completion.session->transaction_status);
        }
        const auto ready = std::chrono::steady_clock::now();
        if (metrics_enabled_) {
          durability_wait_ns_.fetch_add(
              static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::nanoseconds>(
                      ready - completion.ready_at)
                      .count()),
              std::memory_order_relaxed);
          durability_commands_.fetch_add(1, std::memory_order_relaxed);
        }
        completion.ready_at = ready;
        completion.session->reply_durability_target = 0;
        PublishReadCompletion(std::move(completion));
      }
    }
  }

  std::pair<std::string, std::unique_ptr<ActiveSelectStream>> ExecuteReadTask(
      const std::vector<pgwire::SqlStatement>& statements,
      lsn_t* reply_target = nullptr, bool* literal_constant = nullptr,
      SqlEngine* pooled_engine = nullptr) {
    ScopedSqlCpuAffinity sql_affinity;
    if (literal_constant != nullptr) {
      *literal_constant = false;
    }
    bool literal_only = statements.size() == 1;
    std::string response;
    // One snapshot for the whole message so multi-statement reads observe a
    // consistent database image.
    std::unique_ptr<TransactionContext> context =
        std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
    bool ok = true;
    lsn_t observed_dependency = 0;
    for (size_t idx = 0; idx < statements.size(); ++idx) {
      const pgwire::SqlStatement& statement = statements[idx];
      try {
        std::optional<SqlEngine> local_engine;
        SqlEngine& engine = (pooled_engine != nullptr)
                                ? *pooled_engine
                                : local_engine.emplace(*database_);
        StatusOr<QueryResult> executed =
            engine.Execute(*context, statement.text);
        if (!executed.HasValue()) {
          const std::string error = engine.LastError().empty()
                                        ? StatusMessage(executed.GetStatus())
                                        : engine.LastError();
          response += pgwire::ErrorResponse(
              error, SqlStateForStatus(executed.GetStatus(), "42601"), "ERROR",
              StatementErrorPosition(engine, statement, error));
          ok = false;
          break;
        }
        literal_only = literal_only && engine.LastQueryIsLiteralConstant();
        QueryResult result = std::move(executed.Value());
        if (!result.Statement() ||
            *result.Statement() != StatementType::kSelect) {
          response += pgwire::ErrorResponse(
              "only SELECT statements may use a read worker", "XX000");
          ok = false;
          break;
        }
        auto stream = std::make_unique<ActiveSelectStream>();
        stream->column_names = result.ColumnNames();
        stream->column_types = result.ColumnTypes();
        stream->result = std::make_unique<QueryResult>(std::move(result));
        std::string chunk_wire;
        bool eof = false;
        const Status chunk_st = PullSelectWireChunk(
            stream.get(), kClientStreamChunkBytes, &chunk_wire, &eof);
        if (chunk_st != Status::kSuccess) {
          stream->result.reset();
          response += pgwire::ErrorResponse(
              chunk_st.GetMessage(), SqlStateForStatus(chunk_st, "XX000"));
          ok = false;
          break;
        }
        if (response.size() + chunk_wire.size() > kMaxQueuedOutputBytes) {
          // P4601: the whole response ships on completion, so an oversized
          // chunk here would be dropped by Queue() in
          // ProcessReadCompletions without any diagnostics. The client has
          // seen nothing of this statement yet: answer a clean limit error.
          stream->result.reset();
          response += pgwire::ErrorResponse(
              "result exceeds the server's queued-output limit", "53200");
          ok = false;
          break;
        }
        observed_dependency =
            std::max(observed_dependency, context->txn_.DurabilityDependence());
        if (!eof) {
          if (reply_target != nullptr) {
            *reply_target = std::max(*reply_target, observed_dependency);
          } else if (observed_dependency != 0) {
            const Status durable =
                database_->WaitForCommitDurable(observed_dependency);
            if (durable != Status::kSuccess) {
              stream->result.reset();
              response +=
                  pgwire::ErrorResponse("read transaction durability failed: " +
                                            StatusMessage(durable),
                                        SqlStateForStatus(durable, "XX000"));
              ok = false;
              break;
            }
          }
          response += chunk_wire;
          stream->owned_txn = std::move(context);
          stream->in_statement_block = false;
          stream->statements =
              std::make_shared<const std::vector<pgwire::SqlStatement>>(
                  statements);
          stream->next_statement_idx = idx + 1;
          stream->reply_durability_target = observed_dependency;
          if (literal_constant != nullptr) {
            *literal_constant = false;
          }
          return {std::move(response), std::move(stream)};
        }
        stream->result.reset();
        response += chunk_wire;
      } catch (const std::exception&) {
        const Status failure =
            StatusFromCurrentException("read task chunk failed");
        response += pgwire::ErrorResponse(failure.GetMessage(),
                                          SqlStateForStatus(failure, "XX000"));
        ok = false;
        break;
      }
    }
    if (!context->IsFinished()) {
      if (ok) {
        Status commit = Status::kSuccess;
        if (reply_target != nullptr) {
          auto prepared = context->PrepareCommitForDurableReply();
          if (prepared.HasValue()) {
            *reply_target = prepared.Value();
          } else {
            commit = prepared.GetStatus();
          }
        } else {
          commit = context->PreCommit();
        }
        if (commit != Status::kSuccess) {
          ok = false;
          response += pgwire::ErrorResponse(
              "read transaction commit failed: " + StatusMessage(commit),
              SqlStateForStatus(commit, "40001"));
        }
      } else {
        AbortContextLoggingOnly(*context);
      }
    }
    if (!ok && observed_dependency != 0) {
      if (reply_target != nullptr) {
        *reply_target = std::max(*reply_target, observed_dependency);
      } else {
        const Status durable =
            database_->WaitForCommitDurable(observed_dependency);
        if (durable != Status::kSuccess) {
          response = pgwire::ErrorResponse(
              "read transaction durability failed: " + StatusMessage(durable),
              SqlStateForStatus(durable, "XX000"));
        }
      }
    }
    if (literal_constant != nullptr) {
      *literal_constant = ok && literal_only && observed_dependency == 0 &&
                          ((reply_target == nullptr) || *reply_target == 0);
    }
    response += pgwire::ReadyForQuery('I');
    return {std::move(response), nullptr};
  }

  // Drops every cached literal reply (all connections) and releases the
  // process-wide byte budget they held.
  void ClearLiteralConstantReplies() {
    for (auto& entry : clients_) {
      entry.second.literal_constant_reply_bytes = 0;
      entry.second.literal_constant_replies.clear();
    }
    literal_constant_reply_bytes_ = 0;
  }

  void ProcessReadCompletions() {
    std::deque<ReadCompletion> completions;
    {
      std::scoped_lock lock(read_completion_mutex_);
      completions.swap(read_completions_);
    }
    for (ReadCompletion& completion : completions) {
      if (metrics_enabled_) {
        completion_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - completion.ready_at)
                    .count()),
            std::memory_order_relaxed);
      }
      pending_worker_tasks_.fetch_sub(1);
      const auto found = clients_.find(completion.client_fd);
      if (found == clients_.end() || found->second.id != completion.client_id) {
        completion.active_stream.reset();
        if (completion.session) {
          CleanupActiveStream(*completion.session);
        }
        ScheduleSessionCleanup(std::move(completion.session));
        continue;
      }
      if (completion.reply_send_failed) {
        completion.active_stream.reset();
        if (completion.session) {
          CleanupActiveStream(*completion.session);
        }
        ScheduleSessionCleanup(std::move(completion.session));
        CloseClient(completion.client_fd);
        continue;
      }
      // Only a successful, dependency-free literal query is certified by
      // SqlEngine. Cached wire frames are exact-SQL and idle-state only.
      // The reply is filed under the connection that produced it: a literal
      // answer can embed session state (a session constant's value), so it
      // must never be replayed to another connection.
      Client& client = found->second;
      if (!completion.literal_constant_sql.empty() &&
          !completion.active_stream && !client.session_has_temp_objects &&
          completion.response.size() + completion.literal_constant_sql.size() <=
              65536) {
        // The per-connection caches share one process-wide byte budget, so a
        // burst of connections cannot multiply the resident reply memory.
        if (literal_constant_reply_bytes_ + completion.response.size() +
                completion.literal_constant_sql.size() >
            size_t{4} * 1024 * 1024) {
          ClearLiteralConstantReplies();
        }
        if (client.literal_constant_replies.size() >= 512) {
          literal_constant_reply_bytes_ -= client.literal_constant_reply_bytes;
          client.literal_constant_replies.clear();
          client.literal_constant_reply_bytes = 0;
        }
        const auto [entry, inserted] = client.literal_constant_replies.emplace(
            completion.literal_constant_sql, completion.response);
        if (inserted) {
          const size_t bytes = entry->first.size() + entry->second.size();
          client.literal_constant_reply_bytes += bytes;
          literal_constant_reply_bytes_ += bytes;
        } else {
          // A re-executed literal refreshes the cached wire bytes: the
          // replayed answer must match the newest execution, and the
          // per-connection / process-wide byte accounting with it.
          const size_t old_size = entry->second.size();
          entry->second = completion.response;
          const size_t new_size = entry->second.size();
          if (new_size >= old_size) {
            client.literal_constant_reply_bytes += new_size - old_size;
            literal_constant_reply_bytes_ += new_size - old_size;
          } else {
            client.literal_constant_reply_bytes -= old_size - new_size;
            literal_constant_reply_bytes_ -= old_size - new_size;
          }
        }
      }
      if (completion.plan_cache) {
        client.plan_cache = std::move(completion.plan_cache);
      }
      if (completion.session) {
        if (completion.session->plan_cache) {
          client.plan_cache = std::move(completion.session->plan_cache);
        }
        client.transaction = std::move(completion.session->transaction);
        client.transaction_status = completion.session->transaction_status;
        client.open_block_has_ddl =
            std::exchange(completion.session->open_block_has_ddl, false);
        client.output_overflow |= completion.session->output_overflow;
        if (!completion.active_stream && completion.session->active_stream) {
          completion.active_stream =
              std::move(completion.session->active_stream);
        }
        client.worker_shell = std::move(completion.session);
      }
      client.read_query_in_flight = false;
      if (completion.active_stream) {
        client.last_activity = std::chrono::steady_clock::now();
        if (client.output_offset != 0) {
          client.output.erase(0, client.output_offset);
          client.output_offset = 0;
        }
        Queue(client, completion.response);
        client.active_stream = std::move(completion.active_stream);
      } else {
        Queue(client, completion.response);
      }
      if (client.output_overflow) {
        // Queue() latched output_overflow: the response exceeds the queued
        // output cap and was dropped.  Close the connection so the client
        // does not idle until the timeout waiting for an answer that will
        // never arrive (the epoll-path ReadClient does the same).
        CloseClient(completion.client_fd);
        continue;
      }
      ProcessInput(client);
      if (client.peer_eof_seen && !client.read_query_in_flight &&
          !client.active_stream) {
        client.close_after_write = true;
      }
      if (!WriteClient(completion.client_fd)) {
        CloseClient(completion.client_fd);
      }
    }
  }

  // P1908: TransactionContext::Abort reports the storage Status of the
  // kAbort WAL append / undo replay, and every server rollback site used to
  // drop it: a client received its success reply while crash recovery could
  // still resurrect writes that should have been undone. A rollback that
  // cannot be honoured makes the session untrustworthy, so -- PostgreSQL's
  // convention for such storage faults -- the connection is torn down: log
  // the cause and schedule the close after the queued replies flush.
  static void AbortContext(Client& client, TransactionContext& txn) {
    if (txn.IsFinished()) {
      return;
    }
    const Status status = txn.Abort();
    if (status != Status::kSuccess) {
      LOG(WARN) << "transaction rollback failed: " << StatusMessage(status)
                << "; terminating session";
      client.close_after_write = true;
    }
  }

  // Convenience for owned contexts: abort (if still live) then release.
  static void AbortAndRelease(Client& client,
                              std::unique_ptr<TransactionContext>& txn) {
    if (txn) {
      AbortContext(client, *txn);
    }
    txn.reset();
  }

  // Worker-thread teardown has no Client to retire; thestatic  completion queue
  // owns the reply, so a logged cause is the only channel.
  static void AbortContextLoggingOnly(TransactionContext& txn) {
    if (txn.IsFinished()) {
      return;
    }
    const Status status = txn.Abort();
    if (status != Status::kSuccess) {
      LOG(WARN) << "transaction rollback failed: " << StatusMessage(status)
                << " during worker teardown";
    }
  }

  static std::optional<size_t> StatementErrorPosition(
      const SqlEngine& engine, const pgwire::SqlStatement& statement,
      std::string_view message) {
    const auto offset = engine.LastParseInputOffset();
    if (!offset) {
      return std::nullopt;
    }
    // GoogleSQL diagnostics can span multiple lines and the [at L:C] tag may
    // sit on a later one; SqlErrorPosition only reads the first line it is
    // given, so probe each line of the message.
    std::string_view rest = message;
    while (true) {
      const size_t newline = rest.find('\n');
      if (auto position = pgwire::SqlErrorPosition(rest.substr(0, newline),
                                                   statement, *offset)) {
        return position;
      }
      if (newline == std::string_view::npos) {
        return std::nullopt;
      }
      rest.remove_prefix(newline + 1);
    }
  }

  static void FailStatement(Client& client,
                            std::unique_ptr<TransactionContext>& automatic,
                            std::unique_ptr<TransactionContext>& implicit,
                            const std::string& message,
                            std::string_view sqlstate,
                            std::optional<size_t> position = std::nullopt) {
    AbortAndRelease(client, automatic);
    AbortAndRelease(client, implicit);
    if (client.transaction) {
      AbortContext(client, *client.transaction);
      client.transaction.reset();
      client.transaction_status = 'E';
    }
    Queue(client, pgwire::ErrorResponse(message, sqlstate, "ERROR", position));
  }

  static void AbortOpenTransaction(Client& client) {
    AbortAndRelease(client, client.transaction);
  }

  // P1660: state transition for protocol-level rejections that keep the
  // connection alive (08P01 unterminated query, 0A000 extended message).
  // Without it an explicit block survived a refused message at status 'T'
  // and could still COMMIT; PostgreSQL tears the whole connection down
  // (FATAL), so an aborted block is the closest in-session equivalent.
  void AbortTransactionForProtocolViolation(
      Client&
          client) {  // NOLINT(readability-convert-member-functions-to-static)
    if (client.transaction_status == 'T') {
      AbortOpenTransaction(client);
      client.transaction_status = 'E';
    }
  }

  void CloseClient(platform::SocketFd fd) {
    const auto found = clients_.find(fd);
    if (found == clients_.end()) {
      return;
    }
    EndOpenBlockDdl(found->second);
    CleanupActiveStream(found->second);
    if (found->second.session_has_temp_objects) {
      // These registries are thread-local on the event-loop thread; a
      // disconnect must not leave this session's constants or views visible
      // to the next session that runs here. UDF definitions are tagged with
      // the owning connection id, so drop exactly this session's entries
      // (other live sessions on this thread keep theirs).
      PurgeUdfSessionDefinitions(found->second.id);
      ClearSessionRegistries();
    }
    if (found->second.transaction && !read_workers_.empty()) {
      auto session = std::make_unique<Client>(fd, found->second.id, 0, false);
      session->transaction = std::move(found->second.transaction);
      ScheduleSessionCleanup(std::move(session));
    } else {
      AbortOpenTransaction(found->second);
    }
    if (poller_.Remove(fd)) {
      LOG(WARN) << ErrnoMessage("poller.Remove on client close");
    }
    found->second.reply_socket->Close();
    // The connection's cached literal replies die with it; the shared byte
    // budget must follow.
    const size_t cached_bytes = found->second.literal_constant_reply_bytes;
    literal_constant_reply_bytes_ -=
        std::min(literal_constant_reply_bytes_, cached_bytes);
    found->second.literal_constant_reply_bytes = 0;
    found->second.literal_constant_replies.clear();
    clients_.erase(found);
  }

  // Members are grouped by lifetime/ownership role; exact padding depends on
  // the platform allocator and is not part of this API's contract.
  std::unique_ptr<Database> database_;
  uint64_t next_client_id_{1};
  std::thread durability_worker_;
  // Request-path counters are updated by every read worker on every task;
  // they sit on dedicated cache lines so one worker's fetch_add does not
  // invalidate the line another worker is hammering.
  alignas(64) std::atomic<uint64_t> durability_commands_{0};
  alignas(64) std::atomic<uint64_t> durability_wait_ns_{0};
  alignas(64) std::atomic<uint64_t> durability_batches_{0};
  std::chrono::steady_clock::time_point last_metrics_{};
  std::chrono::steady_clock::time_point last_idle_sweep_{};
  std::chrono::steady_clock::time_point last_fd_pressure_warn_{};
  uint64_t fd_pressure_sheds_since_warn_{0};
  alignas(64) std::atomic<uint64_t> queue_ns_{0};
  alignas(64) std::atomic<uint64_t> execution_ns_{0};
  alignas(64) std::atomic<uint64_t> executed_tasks_{0};
  alignas(64) std::atomic<uint64_t> completion_ns_{0};
  size_t read_worker_count_{1};
  alignas(64) std::atomic<size_t> active_read_queries_{0};
  alignas(64) std::atomic<size_t> pending_worker_tasks_{0};
  // Number of sessions with an open block that has executed a catalog DDL
  // statement. While nonzero, other sessions' catalog reads/writes must be
  // refused (55006): an uncommitted DDL change is visible in the physical
  // catalog image, and its rollback would erase other sessions' commits.
  alignas(64) std::atomic<size_t> open_block_ddl_sessions_{0};
  // Includes completions already swapped into the event loop's local batch,
  // until their pending count is retired. These cannot join a future sync.
  alignas(64) std::atomic<size_t> potential_commit_tasks_{0};
  alignas(64) std::atomic<size_t> peak_concurrent_reads_{0};
  size_t literal_constant_reply_bytes_{0};
  alignas(64) std::atomic<uint64_t> cached_literal_reply_count_{0};
  alignas(64) std::atomic<uint64_t> direct_reply_count_{0};
  size_t server_tx_floor_bytes_{0};
  std::vector<std::thread> read_workers_;
  std::string startup_error_;
  // Server-side latch discipline (all leaf locks relative to
  // docs/lock_order.md -- no storage/txn latch is ever held under them):
  // the only nested acquisition is read_completion_mutex_ ->
  // read_task_mutex_ in StopReadWorkers (via ScheduleSessionCleanup ->
  // EnqueueSqlTask). Every other acquisition in ReadWorkerLoop,
  // DurabilityWorkerLoop, PublishReadCompletion and the start/stop helpers
  // is in its own scope, so no reverse edge exists and the repeated
  // scoped_locks in a single function are sequential, not nested.
  std::mutex durability_mutex_;
  std::mutex read_task_mutex_;
  std::mutex read_completion_mutex_;
  std::condition_variable durability_ready_;
  std::unordered_map<platform::SocketFd, Client> clients_;
  std::deque<ReadCompletion> durability_tasks_;
  PostgresServerOptions options_;
  std::deque<ReadCompletion> read_completions_;
  std::array<std::condition_variable, kMaxReadWorkers> read_task_ready_;
  std::array<std::deque<ReadTask>, kMaxReadWorkers> read_tasks_;
  // Seeded in the Impl ctor body: std::random_device can throw (entropy
  // exhaustion), and a member initializer would let that escape the ctor —
  // bypassing the startup_error_ reporting contract every other failure uses.
  std::mt19937_64 secret_rng_;  // Event-loop owned.
  int io_cpu_{-1};
  platform::SocketFd listener_fd_{platform::kInvalidSocketFd};
  // Registration is logically const for the event loop's bookkeeping.
  mutable platform::EventPoller poller_;
  int reserve_fd_{-1};
  platform::Waker waker_;
  uint16_t bound_port_{0};
  std::atomic<bool> deferred_commits_enabled_{false};
  bool durability_stopping_{false};
  bool metrics_enabled_{false};
  // Set when AcceptClients shed the listener from epoll after losing the
  // EMFILE reserve descriptor; cleared once the reserve is restored.
  [[maybe_unused]] bool listener_shed_due_to_fd_pressure_{false};
  std::atomic<bool> stopping_{false};
  bool read_workers_stopping_{false};
  std::array<bool, 16> read_worker_running_{};
};

PostgresServer::PostgresServer(const std::string& database_path,
                               PostgresServerOptions options)
    : impl_(std::make_unique<Impl>(database_path, std::move(options))) {}

PostgresServer::~PostgresServer() = default;

// True for the statement spellings whose frontend visit mutates the
// thread-local TEMP-view/UDF/constant registries (CREATE [OR REPLACE]
// [TEMP] VIEW, CREATE [OR REPLACE] [TEMP] [AGGREGATE] FUNCTION,
// CREATE [OR REPLACE] [TEMP] TABLE FUNCTION, CREATE [OR REPLACE]
// [TEMP] CONSTANT).  Missing a spelling here offloads a later all-SELECT
// message to a worker whose thread-local registries never saw the object.
bool PostgresServer::RegistersTempObject(std::string_view sql) {
  // Longest spelling checked below: CREATE OR REPLACE TEMP AGGREGATE
  // TABLE FUNCTION — 7 words; 8 leaves headroom for a future modifier.
  std::array<std::string_view, 8> words;
  size_t count = 0;
  sql = StripLeadingComments(sql);
  size_t pos = sql.find_first_not_of(" \t\r\n");
  while (pos != std::string_view::npos && count < words.size()) {
    const size_t end = sql.find_first_of(" \t\r\n", pos);
    words[count++] = sql.substr(pos, end - pos);
    pos = sql.find_first_not_of(" \t\r\n", end);
  }
  if (count == 0 || !IdentifierEquals(words[0], "CREATE")) {
    return false;
  }
  size_t index = 1;
  if (index < count && IdentifierEquals(words[index], "OR")) {
    if (++index >= count || !IdentifierEquals(words[index], "REPLACE")) {
      return false;
    }
    ++index;
  }
  // "CREATE TEMP VIEW" / "CREATE TEMPORARY AGGREGATE FUNCTION ..." land in
  // the same thread-local tables; skip the location modifiers.
  while (index < count && (IdentifierEquals(words[index], "TEMP") ||
                           IdentifierEquals(words[index], "TEMPORARY"))) {
    ++index;
  }
  if (index < count && IdentifierEquals(words[index], "AGGREGATE")) {
    ++index;
  }
  if (index >= count) {
    return false;
  }
  if (IdentifierEquals(words[index], "VIEW") ||
      IdentifierEquals(words[index], "FUNCTION") ||
      IdentifierEquals(words[index], "CONSTANT")) {
    return true;
  }
  return IdentifierEquals(words[index], "TABLE") && index + 1 < count &&
         IdentifierEquals(words[index + 1], "FUNCTION");
}

bool PostgresServer::Listen(std::string* error) { return impl_->Listen(error); }

int PostgresServer::Run(std::string* error) { return impl_->Run(error); }

void PostgresServer::RequestStop() { impl_->RequestStop(); }

uint16_t PostgresServer::BoundPort() const { return impl_->BoundPort(); }

size_t PostgresServer::ReadWorkerCount() const {
  return impl_->ReadWorkerCount();
}

uint64_t PostgresServer::CachedLiteralReplyCount() const {
  return impl_->CachedLiteralReplyCount();
}

uint64_t PostgresServer::DirectReplyCount() const {
  return impl_->DirectReplyCount();
}

size_t PostgresServer::PeakConcurrentReadQueries() const {
  return impl_->PeakConcurrentReadQueries();
}

}  // namespace tinylamb
