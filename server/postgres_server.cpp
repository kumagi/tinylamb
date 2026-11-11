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
#include "common/platform/cpu_affinity.hpp"
#include "common/platform/event_poller.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/os.hpp"
#include "common/platform/process.hpp"
#include "common/platform/socket.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
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

constexpr size_t kMaxServerResultRows = 1'000'000;
// How often the event loop wakes to sweep idle connections (milliseconds).
constexpr int kIdleSweepIntervalMs = 1000;
// Input cap per connection before authentication completes; unauthenticated
// clients must not be able to pin max_message_bytes of memory each.
constexpr size_t kMaxPreAuthInputBytes = 1024;
// Per-connection bound on queued outbound bytes.  The input side is capped,
// but a client that pipelines queries and never reads would otherwise let
// `Client::output` grow without bound (each 'Q' can emit up to
// kMaxServerResultRows rows) and OOM the process.
constexpr size_t kMaxQueuedOutputBytes = 64U << 20;  // 64 MiB

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
    (void)masks_.available.ApplyToCurrentThread();
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
    (void)masks_->io.ApplyToCurrentThread();
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

std::string StatusMessage(const Status& status) {
  std::ostringstream output;
  output << status;
  return output.str();
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
        if (sql.substr(pos, 2) == "/*") {
          ++depth;
          pos += 2;
        } else if (sql.substr(pos, 2) == "*/") {
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
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return command;
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
  }
  return "OK";
}

}  // namespace

Status DrainCommandResult(QueryResult& result, int64_t* affected_rows) {
  const int64_t count = result.AffectedRows();
  result.Drain();
  if (affected_rows != nullptr) {
    *affected_rows = count;
  }
  return result.GetStatus();
}

class PostgresServer::Impl {
 public:
  Impl(const std::string& database_path, PostgresServerOptions options)
      : options_(std::move(options)) {
    // Honor force_recovery here as well as in postgres_server_main: an
    // embedder constructing PostgresServer directly with the flag set gets
    // the same torn-tail truncation behavior without touching the global
    // beforehand.
    if (options_.force_recovery) {
      RecoveryManager::SetTornTailTruncationAllowed(true);
    }
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
    constexpr size_t kMaxReadWorkers = 16;
    read_worker_count_ =
        std::min(options_.read_worker_threads, kMaxReadWorkers);
    if (read_worker_count_ == 0) {
      read_worker_count_ =
          std::min(kMaxReadWorkers,
                   std::max<size_t>(1, std::thread::hardware_concurrency()));
    }
  }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  ~Impl() {
    StopReadWorkers();
    for (auto& [fd, client] : clients_) {
      AbortOpenTransaction(client);
      platform::CloseSocket(fd);
    }
    if (listener_fd_ >= 0) {
      platform::CloseSocket(listener_fd_);
    }
    waker_.Close();
    poller_.Close();
    if (reserve_fd_ >= 0) {
      platform::CloseFile(reserve_fd_);
    }
  }

  bool Listen(std::string* error) {
    if (listener_fd_ >= 0) {
      return true;
    }
    if (database_ == nullptr) {
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
      const int candidate = platform::OpenSocket(
          address->ai_family, address->ai_socktype, address->ai_protocol,
          {.nonblocking = true, .close_on_exec = true});
      if (candidate < 0) {
        continue;
      }
      (void)platform::SetSocketOptInt(candidate, SOL_SOCKET, SO_REUSEADDR, 1);
      if (bind(candidate, address->ai_addr, address->ai_addrlen) == 0 &&
          listen(candidate, options_.backlog) == 0) {
        listener_fd_ = candidate;
        break;
      }
      platform::CloseSocket(candidate);
    }
    freeaddrinfo(addresses);
    if (listener_fd_ < 0) {
      *error = ErrnoMessage("could not bind PostgreSQL listener");
      return false;
    }

    sockaddr_storage bound{};
    socklen_t bound_size = sizeof(bound);
    if (getsockname(listener_fd_, reinterpret_cast<sockaddr*>(&bound),
                    &bound_size) != 0) {
      *error = ErrnoMessage("getsockname");
      return false;
    }
    if (bound.ss_family == AF_INET) {
      bound_port_ = ntohs(reinterpret_cast<sockaddr_in*>(&bound)->sin_port);
    } else {
      bound_port_ = ntohs(reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port);
    }

    if (!poller_.Open()) {
      *error = ErrnoMessage("event poller");
      return false;
    }
    if (!waker_.Open()) {
      *error = ErrnoMessage("event loop waker");
      return false;
    }
    if (!AddPollFd(listener_fd_, platform::kPollReadable, error) ||
        !AddPollFd(waker_.ReadFd(), platform::kPollReadable, error)) {
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
    if (options_.listen_address != "127.0.0.1" &&
        options_.listen_address != "localhost" &&
        options_.listen_address != "::1") {
      std::cerr << "WARNING: tinylamb implements no authentication; every "
                   "connecting user is trusted. Do not expose it beyond "
                   "loopback.\n";
    }
    StartReadWorkers();
    return true;
  }

  int Run(std::string* error) {
    if (listener_fd_ < 0 && !Listen(error)) {
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
        return 1;
      }
      for (int i = 0; i < count; ++i) {
        const int fd = events[static_cast<size_t>(i)].fd;
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
          alive = ReadClient(fd);
        }
        if (alive && clients_.contains(fd) &&
            (flags & platform::kPollWritable) != 0U) {
          alive = WriteClient(fd);
        }
        if ((flags & (platform::kPollError | platform::kPollHangup)) != 0U) {
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
    std::cerr << "TINYLAMB_METRICS {\"time_ns\":"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(
                     now.time_since_epoch())
                     .count()
              << ",\"fixed_size_wal\":"
              << (database_->FixedSizeWal() ? "true" : "false")
              << ",\"io_cpu\":" << io_cpu_
              << ",\"tasks\":" << executed_tasks_.load()
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
              << ",\"current_commit_ts\":" << stats.current_commit_ts << "}\n";
  }

  void RequestStop() {
    stopping_.store(true);
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
  // Unpredictable BackendKeyData secret (CancelRequest is not implemented
  // yet, but the key must not be guessable from the fd alone).
  uint32_t NextSecret() { return static_cast<uint32_t>(secret_rng_()); }

  // Epoll owns socket lifetime. A completed worker may borrow it for one
  // nonblocking send; close and send share this lock so an old completion
  // can never write to a recycled descriptor belonging to another client.
  struct ReplySocket {
    explicit ReplySocket(int socket) : fd(socket) {}
    ReplySocket(const ReplySocket&) = delete;
    ReplySocket& operator=(const ReplySocket&) = delete;
    ReplySocket(ReplySocket&&) = delete;
    ReplySocket& operator=(ReplySocket&&) = delete;
    ~ReplySocket() { Close(); }
    void Close() {
      std::scoped_lock lock(mutex);
      if (fd >= 0) {
        platform::CloseSocket(fd);
      }
      fd = -1;
    }
    ssize_t Send(std::string_view response, int* failure) {
      std::scoped_lock lock(mutex);
      if (fd < 0) {
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
    int fd;
  };

  struct Client {
    Client(int socket, uint64_t identifier, uint32_t secret_key,
           bool create_plan_cache = true)
        : fd(socket),
          id(identifier),
          secret(secret_key),
          plan_cache(create_plan_cache
                         ? std::make_unique<PreparedPlanSessionCache>()
                         : nullptr),
          reply_socket(create_plan_cache ? std::make_shared<ReplySocket>(socket)
                                         : nullptr) {}
    int fd;
    uint64_t id;
    uint32_t secret;
    bool startup_complete{false};
    bool close_after_write{false};
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
    // One bounded, exact-SQL classification learned from a successful
    // WITH ... SELECT. This contains no result or bound plan. Only idle
    // sessions without thread-local objects may reuse it for scheduling.
    std::string certified_snapshot_sql;
    // When a read is offloaded to a worker, last_activity stops advancing
    // until the completion returns. Track the offload time separately so a
    // hung worker cannot pin the fd (and its buffers) past idle_timeout:
    // in-flight connections are swept on the same timeout measured from
    // here. ProcessReadCompletions drops completions for an unknown client
    // id, so closing is safe.
    std::chrono::steady_clock::time_point read_started_at{};
    std::chrono::steady_clock::time_point last_activity{
        std::chrono::steady_clock::now()};
    bool output_overflow = false;
    bool permit_deferred_commit{false};
    lsn_t reply_durability_target{0};
  };

  struct ReadTask {
    int client_fd{};
    uint64_t client_id{};
    std::vector<std::string> statements;
    std::unique_ptr<Client> session;
    bool cleanup_only{false};
    std::unique_ptr<PreparedPlanSessionCache> plan_cache{};
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
    bool potential_commit{false};
    std::shared_ptr<ReplySocket> reply_socket{};
  };

  struct ReadCompletion {
    int client_fd;
    uint64_t client_id;
    std::string response;
    std::unique_ptr<Client> session;
    std::string literal_constant_sql;
    std::unique_ptr<PreparedPlanSessionCache> plan_cache{};
    std::chrono::steady_clock::time_point ready_at{
        std::chrono::steady_clock::now()};
    std::shared_ptr<ReplySocket> reply_socket{};
    bool reply_send_failed{false};
  };

  bool AddPollFd(int fd, uint32_t interest, std::string* error) const {
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
          std::cerr << error << '\n';
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
      const int fd = platform::AcceptSocket(
          listener_fd_, {.nonblocking = true, .close_on_exec = true});
      if (fd < 0) {
#ifdef TINYLAMB_OS_POSIX
        if ((errno == EMFILE || errno == ENFILE) && reserve_fd_ >= 0) {
          // Descriptor exhaustion: the level-triggered listener stays
          // readable and would spin forever. Consume (and shed) one pending
          // connection via the reserved descriptor, then restore it.
          const std::string reason = ErrnoMessage("accept");
          platform::CloseFile(reserve_fd_);
          reserve_fd_ = -1;
          const int shed = platform::AcceptSocket(
              listener_fd_, {.nonblocking = true, .close_on_exec = true});
          if (shed >= 0) {
            platform::CloseSocket(shed);
          }
          std::cerr << "descriptor limit hit (" << reason
                    << "); shed one pending connection\n";
          reserve_fd_ = platform::OpenFile("/dev/null", {});
          if (reserve_fd_ < 0) {
            // Without a reserve descriptor the next EMFILE accept would spin
            // the level-triggered loop. Shed the listener from epoll until a
            // timed event-loop retry restores the reserve. The retry must
            // not depend on readability of the now-unregistered listener.
            (void)poller_.Remove(listener_fd_);
            listener_shed_due_to_fd_pressure_ = true;
          }
        } else
#endif
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
          std::cerr << ErrnoMessage("accept") << '\n';
        }
        return;
      }
      // Disable Nagle so small responses are not delayed waiting for ACKs.
      (void)platform::SetSocketOptInt(fd, IPPROTO_TCP, TCP_NODELAY, 1);
      if (clients_.size() >= options_.max_connections) {
        // Over capacity: shed immediately; the client observes EOF.
        platform::CloseSocket(fd);
        continue;
      }
      // Register the client before adding the fd to epoll: if the map insert
      // throws, the fd is still ours to close; registered the other way
      // around, an epoll-enrolled fd with no client entry would be ignored
      // by the event loop until shutdown.
      clients_.try_emplace(fd, fd, next_client_id_++, NextSecret());
      std::string error;
      if (!AddPollFd(fd, platform::kPollReadable | platform::kPollPeerClosed,
                     &error)) {
        std::cerr << error << '\n';
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
      // In-flight reads are exempt while the worker is making progress
      // (last_activity cannot advance off the event loop), but a hung worker
      // must not pin the connection forever: sweep it on the same timeout
      // measured from the offload time. The orphaned completion is dropped
      // by client-id check when it eventually arrives.
      const bool idle =
          client.read_query_in_flight
              ? now - client.read_started_at > options_.idle_timeout
              : now - client.last_activity > options_.idle_timeout;
      if (idle) {
        const int fd = it->first;
        ++it;
        CloseClient(fd);
      } else {
        ++it;
      }
    }
  }

  bool ReadClient(int fd) {
    auto found = clients_.find(fd);
    if (found == clients_.end()) {
      return false;
    }
    Client& client = found->second;
    std::array<char, 8192> buffer{};
    bool eof_seen = false;
    size_t cached_packets = 0;
    while (true) {
      const ssize_t size =
          platform::RecvSocket(fd, buffer.data(), buffer.size(), 0);
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
          // Queue() latches output_overflow once the 64 MiB queued-output
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
        if (static_cast<size_t>(size) < buffer.size()) {
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
        break;
      }
      return false;
    }
    if (!client.input.empty()) {
      ProcessInput(client);
    }
    if (eof_seen) {
      client.peer_eof_seen = true;
    }
    if (client.peer_eof_seen && !client.read_query_in_flight) {
      client.close_after_write = true;
    }
    if (client.output_overflow) {
      // Queue() refuses to buffer past the per-connection output cap: drop
      // the connection instead of propagating into the event loop.
      CloseClient(fd);
      return false;
    }
    if (client.output_offset < client.output.size()) {
      return WriteClient(fd);
    }
    UpdateClientInterest(client);
    // A half-close (close_after_write) must not close the connection while a
    // read query is still running on a worker: its completion queues the
    // response, which WriteClient() then flushes before closing.
    return client.read_query_in_flight || !client.close_after_write ||
           !client.output.empty();
  }

  bool WriteClient(int fd) {
    auto found = clients_.find(fd);
    if (found == clients_.end()) {
      return false;
    }
    Client& client = found->second;
    while (client.output_offset < client.output.size()) {
      const char* data = client.output.data() + client.output_offset;
      const size_t remaining = client.output.size() - client.output_offset;
      const ssize_t size = platform::SendNoSignal(fd, data, remaining);
      if (size > 0) {
        client.output_offset += static_cast<size_t>(size);
        client.last_activity = std::chrono::steady_clock::now();
        continue;
      }
      if (size < 0 && errno == EINTR) {
        continue;
      }
      if (size < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        break;
      }
      return false;
    }
    if (client.output_offset == client.output.size()) {
      client.output.clear();
      // Retain only a small reusable wire buffer. Large responses still
      // release their high-water allocation when the socket drains.
      if (client.output.capacity() > 4096) {
        client.output.shrink_to_fit();
      }
      client.output_offset = 0;
      if (client.close_after_write && !client.read_query_in_flight) {
        return false;
      }
    }
    UpdateClientInterest(client);
    return true;
  }

  void UpdateClientInterest(Client& client) const {
    uint32_t interest = 0;
    if (!client.read_query_in_flight && !client.peer_eof_seen &&
        !client.close_after_write) {
      interest |= platform::kPollReadable | platform::kPollPeerClosed;
    }
    if (client.output_offset < client.output.size()) {
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
    while (!client.close_after_write) {
      if (client.read_query_in_flight) {
        return;
      }
      if (!client.startup_complete) {
        if (!ProcessStartup(client)) {
          return;
        }
        continue;
      }
      if (client.input.size() < 5) {
        return;
      }
      const StatusOr<uint32_t> length_field =
          pgwire::ReadUint32(client.input, 1);
      if (!length_field.HasValue()) {
        return;
      }
      const uint32_t length = length_field.Value();
      if (length < 4 || length > options_.max_message_bytes) {
        Queue(client, pgwire::ErrorResponse("invalid frontend message length",
                                            "08P01"));
        client.close_after_write = true;
        return;
      }
      const size_t total = static_cast<size_t>(length) + 1;
      if (client.input.size() < total) {
        return;
      }
      const char type = client.input[0];
      // The handler either executes synchronously or copies statements into
      // its task. Keep this frame in place until it returns, avoiding an
      // owned payload copy (including for uncached, nonliteral SQL).
      const std::string_view payload(client.input.data() + 5, length - 4);
      if (type == 'Q') {
        if (payload.empty() || payload.back() != '\0') {
          Queue(client, pgwire::ErrorResponse(
                            "unterminated simple query message", "08P01"));
          Queue(client, pgwire::ReadyForQuery(client.transaction_status));
        } else {
          ExecuteSimpleQuery(client, payload.substr(0, payload.size() - 1));
        }
      }
      client.input.erase(0, total);
      // The input cap is max_message_bytes + 5 (16 MiB); give the capacity
      // back once the backlog drains so an idle connection does not pin it.
      if (client.input.empty() && client.input.capacity() > 4096) {
        client.input.shrink_to_fit();
      }
      if (type == 'X') {
        client.close_after_write = true;
        return;
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
      Queue(client, pgwire::ErrorResponse(
                        "frontend message type is not supported", "0A000"));
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
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
      Queue(client, pgwire::ErrorResponse("invalid startup packet", "08P01"));
      client.close_after_write = true;
      return false;
    }
    if (client.input.size() < length) {
      return false;
    }
    const std::string packet = client.input.substr(0, length);
    client.input.erase(0, length);
    const StatusOr<uint32_t> code_field = pgwire::ReadUint32(packet, 4);
    if (!code_field.HasValue()) {
      Queue(client, pgwire::ErrorResponse("invalid startup packet", "08P01"));
      client.close_after_write = true;
      return false;
    }
    const uint32_t code = code_field.Value();
    if (code == pgwire::kSslRequestCode || code == pgwire::kGssEncRequestCode) {
      Queue(client, "N");
      return true;
    }
    if (code == pgwire::kCancelRequestCode) {
      client.close_after_write = true;
      return false;
    }

    std::string parse_error;
    std::optional<pgwire::StartupPacket> startup =
        pgwire::ParseStartupPacket(packet, &parse_error);
    if (!startup) {
      Queue(client, pgwire::ErrorResponse(parse_error, "08P01"));
      client.close_after_write = true;
      return false;
    }
    const uint32_t major = startup->protocol_version >> 16U;
    const uint32_t minor = startup->protocol_version & 0xffffU;
    if (major != 3) {
      Queue(client, pgwire::ErrorResponse(
                        "only PostgreSQL protocol v3 is supported", "0A000"));
      client.close_after_write = true;
      return false;
    }
    const auto user = startup->parameters.find("user");
    if (user == startup->parameters.end() || user->second.empty()) {
      Queue(client, pgwire::ErrorResponse("startup packet must include a user",
                                          "28000"));
      client.close_after_write = true;
      return false;
    }
    if (minor > 0) {
      std::vector<std::string> unsupported;
      for (const auto& [name, value] : startup->parameters) {
        (void)value;
        if (name.starts_with("_pq_.")) {
          unsupported.push_back(name);
        }
      }
      Queue(client, pgwire::NegotiateProtocolVersion(0, unsupported));
    }
    client.user = user->second;
    const auto database = startup->parameters.find("database");
    client.database =
        database == startup->parameters.end() ? client.user : database->second;
    Queue(client, pgwire::AuthenticationOk());
    Queue(client, pgwire::ParameterStatus("server_version", "16.0-tinylamb"));
    Queue(client, pgwire::ParameterStatus("server_version_num", "160000"));
    Queue(client, pgwire::ParameterStatus("client_encoding", "UTF8"));
    Queue(client, pgwire::ParameterStatus("DateStyle", "ISO, MDY"));
    Queue(client, pgwire::ParameterStatus("integer_datetimes", "on"));
    Queue(client,
          pgwire::ParameterStatus("standard_conforming_strings", "off"));
    Queue(client, pgwire::ParameterStatus("TimeZone", "UTC"));
    Queue(client,
          pgwire::BackendKeyData(static_cast<uint32_t>(platform::GetPid()),
                                 client.secret));
    Queue(client, pgwire::ReadyForQuery('I'));
    client.startup_complete = true;
    return true;
  }

  static void SendCachedLiteralReply(Client& client,
                                     const std::string& response) {
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
      Queue(client, response.substr(static_cast<size_t>(sent)));
    } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      Queue(client, response);
    } else {
      client.output_overflow = true;
      client.close_after_write = true;
    }
  }

  bool TryCachedQueryPacket(Client& client, std::string_view packet) {
    if (!client.startup_complete || client.close_after_write ||
        client.read_query_in_flight || client.transaction_status != 'I' ||
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
    const auto found =
        literal_constant_replies_.find(packet.substr(5, packet.size() - 6));
    if (found == literal_constant_replies_.end()) {
      return false;
    }
    // The same exact-SQL, idle-state certificate as ExecuteSimpleQuery.
    // Partial frames and multiple frames in one recv keep the general path.
    cached_literal_reply_count_.fetch_add(1, std::memory_order_relaxed);
    SendCachedLiteralReply(client, found->second);
    return true;
  }

  void ExecuteSimpleQuery(Client& client, std::string_view sql) {
    // Already-certified exact SQL can bypass statement splitting as well as
    // planning. A different spelling simply takes the normal parser path.
    if (client.transaction_status == 'I' && !client.session_has_temp_objects) {
      if (auto found = literal_constant_replies_.find(sql);
          found != literal_constant_replies_.end()) {
        cached_literal_reply_count_.fetch_add(1, std::memory_order_relaxed);
        SendCachedLiteralReply(client, found->second);
        return;
      }
    }
    const std::vector<std::string> statements = pgwire::SplitSqlStatements(sql);
    if (statements.empty()) {
      Queue(client, pgwire::EmptyQueryResponse());
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
      return;
    }
    if (statements.size() == 1) {
      const std::string command = UppercaseCommand(statements[0]);
      // BEGIN only registers a snapshot: it performs no SQL planning, page
      // reads, undo or durability wait. Preserve the existing state machine
      // while avoiding a worker transfer for this bounded operation.
      if (client.transaction == nullptr &&
          (command == "BEGIN" || command == "START" || command == "ROLLBACK" ||
           command == "ABORT")) {
        ExecuteStatements(client, statements);
        return;
      }
    }
    if (client.transaction_status == 'I' && !client.session_has_temp_objects &&
        statements.size() == 1) {
      if (auto found = literal_constant_replies_.find(statements[0]);
          found != literal_constant_replies_.end()) {
        cached_literal_reply_count_.fetch_add(1, std::memory_order_relaxed);
        SendCachedLiteralReply(client, found->second);
        return;
      }
    }
    const bool certified_snapshot =
        statements.size() == 1 && !client.certified_snapshot_sql.empty() &&
        statements[0] == client.certified_snapshot_sql;
    if (client.transaction_status == 'I' &&
        (IsReadOnly(statements) || certified_snapshot) &&
        !client.session_has_temp_objects) {
      ScheduleReadQuery(client, statements);
      return;
    }
    if (!client.session_has_temp_objects && !read_workers_.empty() &&
        std::ranges::all_of(statements, [](const std::string& statement) {
          const std::string command = UppercaseCommand(statement);
          return command == "SELECT" || command == "INSERT" ||
                 command == "UPDATE" || command == "DELETE" ||
                 command == "BEGIN" || command == "START" ||
                 command == "COMMIT" || command == "END" ||
                 command == "ROLLBACK" || command == "ABORT";
        })) {
      ScheduleSessionQuery(client, statements);
      return;
    }
    const bool catalog_change =
        std::ranges::any_of(statements, [](const std::string& statement) {
          const std::string command = UppercaseCommand(statement);
          return command == "CREATE" || command == "DROP" ||
                 command == "ALTER" || command == "ANALYZE";
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

  void ExecuteStatements(Client& client,
                         const std::vector<std::string>& statements,
                         bool permit_deferred_commit = false,
                         SqlEngine* pooled_engine = nullptr) {
    client.permit_deferred_commit =
        permit_deferred_commit && statements.size() == 1;
    client.reply_durability_target = 0;
    // Queue compacts this previously-sent prefix before appending anything.
    const size_t reply_begin = client.output.size() - client.output_offset;
    // PostgreSQL semantics: all statements of one Query message run in a
    // single implicit transaction unless the message itself manages
    // transactions. The implicit transaction commits only after every
    // statement succeeded.  The block opens even when the message carries
    // transaction control: BEGIN adopts it as the explicit transaction and
    // COMMIT/ROLLBACK finalize it, so `UPDATE ...; ROLLBACK` in one message
    // really undoes the UPDATE.
    std::unique_ptr<TransactionContext> implicit;
    bool ok = true;
    for (const std::string& statement : statements) {
      // COMMIT/ROLLBACK ends the previous block, but the remaining statements
      // still share a new implicit block until the message ends or another
      // transaction-control statement consumes it.
      if (!implicit && client.transaction == nullptr &&
          client.transaction_status != 'E') {
        implicit =
            std::make_unique<TransactionContext>(database_->BeginContext());
      }
      if (RegistersTempObject(statement)) {
        client.session_has_temp_objects = true;
      }
      if (!ExecuteStatement(client, statement, implicit, pooled_engine)) {
        ok = false;
        break;
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
          Queue(client, pgwire::ErrorResponse("transaction commit failed: " +
                                                  StatusMessage(commit),
                                              "40001"));
        }
      } else if (!implicit->IsFinished()) {
        // A failing statement already aborted the context via FailStatement;
        // this covers failure paths that bypass it.
        implicit->Abort();
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
        client.output.resize(reply_begin);
        Queue(client, pgwire::ErrorResponse("transaction durability failed: " +
                                                StatusMessage(durable),
                                            "40001"));
      }
      client.reply_durability_target = 0;
    }
    Queue(client, pgwire::ReadyForQuery(client.transaction_status));
  }

  bool ExecuteStatement(Client& client, const std::string& sql,
                        std::unique_ptr<TransactionContext>& implicit,
                        SqlEngine* pooled_engine = nullptr) {
    const std::string command = UppercaseCommand(sql);
    if (command == "BEGIN" || command == "START") {
      // PostgreSQL: an explicit BEGIN inside an aborted transaction must not
      // clear the aborted state (25P02); only ROLLBACK/COMMIT recover it.
      if (client.transaction_status == 'E') {
        Queue(client,
              pgwire::ErrorResponse(
                  "current transaction is aborted, commands ignored until "
                  "end of transaction block",
                  "25P02"));
        return false;
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
      }
      client.transaction_status = 'T';
      Queue(client, pgwire::CommandComplete("BEGIN"));
      return true;
    }
    if (command == "ROLLBACK" || command == "ABORT") {
      AbortOpenTransaction(client);
      if (implicit) {
        // Statements earlier in this Query message ran in the implicit
        // block; PostgreSQL rolls those back with the ROLLBACK.
        if (!implicit->IsFinished()) {
          implicit->Abort();
        }
        implicit.reset();
      }
      client.transaction_status = 'I';
      Queue(client, pgwire::CommandComplete("ROLLBACK"));
      return true;
    }
    if (command == "COMMIT" || command == "END") {
      if (client.transaction_status == 'E') {
        client.transaction_status = 'I';
        Queue(client, pgwire::CommandComplete("ROLLBACK"));
        return true;
      }
      if (client.transaction) {
        const Status status = CommitForReply(client, *client.transaction);
        client.transaction.reset();
        if (status != Status::kSuccess) {
          client.transaction_status = 'I';
          Queue(client, pgwire::ErrorResponse("transaction commit failed: " +
                                                  StatusMessage(status),
                                              "40001"));
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
          client.transaction_status = 'I';
          Queue(client, pgwire::ErrorResponse("transaction commit failed: " +
                                                  StatusMessage(status),
                                              "40001"));
          return false;
        }
      }
      client.transaction_status = 'I';
      Queue(client, pgwire::CommandComplete("COMMIT"));
      return true;
    }
    if (client.transaction_status == 'E') {
      Queue(client,
            pgwire::ErrorResponse(
                "current transaction is aborted; issue ROLLBACK", "25P02"));
      return false;
    }
    // psql startup files commonly contain SET commands. Tinylamb has no
    // session GUCs yet, so accepting them as no-ops improves client usability.
    if (command == "SET") {
      Queue(client, pgwire::CommandComplete("SET"));
      return true;
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
        FailStatement(client, automatic, implicit, error, "42601");
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
        StatusOr<std::string> wire = StreamSelectResult(*result);
        if (!wire.HasValue()) {
          result.reset();
          FailStatement(client, automatic, implicit,
                        wire.GetStatus().GetMessage(), "XX000");
          return false;
        }
        Queue(client, wire.MoveValue());
        if (command == "WITH" && sql.size() <= 65536) {
          // The frontend, rather than the leading keyword, proved SELECT.
          // A later invocation still executes against a fresh snapshot;
          // DML spellings, explicit blocks and TEMP sessions keep routing
          // through their existing transaction/session paths.
          client.certified_snapshot_sql = sql;
        }
      } else {
        int64_t affected_rows = 0;
        const Status drain_status = DrainCommandResult(*result, &affected_rows);
        if (drain_status != Status::kSuccess) {
          result.reset();
          FailStatement(
              client, automatic, implicit,
              "statement execution failed: " + StatusMessage(drain_status),
              "XX000");
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
                        "40001");
          return false;
        }
      }
      return true;
    } catch (const std::exception& exception) {
      FailStatement(client, automatic, implicit, exception.what(), "XX000");
      return false;
    }
  }

  static std::vector<pgwire::ColumnDescription> BuildColumnDescriptions(
      const std::vector<std::string>& names, const Row& sample,
      size_t fallback_count) {
    size_t column_count = names.empty() ? fallback_count : names.size();
    if (!sample.values_.empty()) {
      column_count = sample.values_.size();
    }
    std::vector<pgwire::ColumnDescription> columns(column_count);
    for (size_t i = 0; i < column_count; ++i) {
      columns[i].name =
          i < names.size() && !names[i].empty() ? names[i] : "?column?";
      columns[i].type = ValueType::kVarChar;
      if (i < sample.values_.size() && !sample[i].IsNull()) {
        columns[i].type = sample[i].type;
      }
    }
    return columns;
  }

  static StatusOr<std::string> StreamSelectResult(QueryResult& query_result) {
    const std::vector<std::string>& names = query_result.ColumnNames();
    std::string result;
    size_t row_count = 0;
    bool header_sent = false;
    bool row_limit_exceeded = false;
    query_result.ForEach([&](const Row& row) {
      if (row_limit_exceeded) {
        return;
      }
      if (++row_count > kMaxServerResultRows) {
        row_limit_exceeded = true;
        return;
      }
      if (!header_sent) {
        result += pgwire::RowDescription(
            BuildColumnDescriptions(names, row, names.size()));
        header_sent = true;
      }
      result += pgwire::DataRow(row);
    });
    if (row_limit_exceeded) {
      return Status(Status::kTooBigData, "result row limit exceeded");
    }
    if (const Status st = query_result.GetStatus(); st != Status::kSuccess) {
      return st;
    }
    if (!header_sent) {
      result += pgwire::RowDescription(
          BuildColumnDescriptions(names, Row(), names.size()));
    }
    result += pgwire::CommandComplete("SELECT " + std::to_string(row_count));
    return result;
  }

  // Only bare SELECT statements may use the snapshot read worker. WITH and
  // EXPLAIN are classified by their first word but may wrap writes
  // (WITH ... DELETE/UPDATE/INSERT, EXPLAIN ANALYZE <DML>), so they take the
  // normal execution path which handles them correctly.
  static bool IsReadOnly(const std::vector<std::string>& statements) {
    return std::ranges::all_of(statements, [](const std::string& statement) {
      return UppercaseCommand(statement) == "SELECT";
    });
  }

  // True for the statement spellings whose frontend visit mutates the
  // thread-local TEMP-view/UDF/constant registries (CREATE [OR REPLACE]
  // [TEMP] VIEW, CREATE [OR REPLACE] [TEMP] [AGGREGATE] FUNCTION,
  // CREATE [OR REPLACE] [TEMP] TABLE FUNCTION, CREATE [OR REPLACE]
  // [TEMP] CONSTANT).  Missing a spelling here offloads a later all-SELECT
  // message to a worker whose thread-local registries never saw the object.
  static bool RegistersTempObject(std::string_view sql) {
    std::vector<std::string> words;
    sql = StripLeadingComments(sql);
    size_t pos = sql.find_first_not_of(" \t\r\n");
    while (pos != std::string_view::npos && words.size() < 6) {
      const size_t end = sql.find_first_of(" \t\r\n", pos);
      std::string word(sql.substr(
          pos, end == std::string_view::npos ? sql.size() - pos : end - pos));
      for (char& c : word) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      }
      words.push_back(std::move(word));
      pos = sql.find_first_not_of(" \t\r\n", end);
    }
    if (words.empty() || words[0] != "CREATE") {
      return false;
    }
    size_t index = 1;
    if (index < words.size() && words[index] == "OR") {
      if (++index >= words.size() || words[index] != "REPLACE") {
        return false;
      }
      ++index;
    }
    // "CREATE TEMP VIEW" / "CREATE TEMPORARY AGGREGATE FUNCTION ..." land in
    // the same thread-local tables; skip the location modifiers.
    while (index < words.size() &&
           (words[index] == "TEMP" || words[index] == "TEMPORARY")) {
      ++index;
    }
    if (index < words.size() && words[index] == "AGGREGATE") {
      ++index;
    }
    if (index >= words.size()) {
      return false;
    }
    if (words[index] == "VIEW" || words[index] == "FUNCTION" ||
        words[index] == "CONSTANT") {
      return true;
    }
    return words[index] == "TABLE" && index + 1 < words.size() &&
           words[index + 1] == "FUNCTION";
  }

  void EnqueueSqlTask(ReadTask task) {
    // Only these tasks can publish a new commit record in this message.
    // Reads and DML inside an explicit block may have dependency barriers,
    // but waiting for their future COMMIT would add a useless batch delay.
    if (!task.cleanup_only && task.session && task.statements.size() == 1) {
      const auto command = UppercaseCommand(task.statements.front());
      task.potential_commit =
          command == "COMMIT" || command == "END" ||
          (task.session->transaction_status == 'I' &&
           (command == "INSERT" || command == "UPDATE" || command == "DELETE"));
    }
    size_t chosen = 0;
    {
      std::scoped_lock lock(read_task_mutex_);
      const size_t count = read_workers_.size();
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
                         const std::vector<std::string>& statements) {
    client.read_query_in_flight = true;
    client.read_started_at = std::chrono::steady_clock::now();
    if (read_workers_.empty()) {
      std::string response;
      try {
        response = ExecuteReadTask(statements);
      } catch (const std::exception& exception) {
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + exception.what(), "XX000");
        response += pgwire::ReadyForQuery('I');
      } catch (...) {
        response =
            pgwire::ErrorResponse("read worker failed: unknown error", "XX000");
        response += pgwire::ReadyForQuery('I');
      }
      client.read_query_in_flight = false;
      Queue(client, response);
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

  void ScheduleSessionQuery(Client& client,
                            const std::vector<std::string>& statements) {
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
    if (!session || !session->transaction) {
      return;
    }
    EnqueueSqlTask({.client_fd = -1,
                    .client_id = 0,
                    .statements = {},
                    .session = std::move(session),
                    .cleanup_only = true});
  }

  void StartReadWorkers() {
    if (!read_workers_.empty()) {
      return;
    }
    try {
      durability_worker_ = std::thread([this] { DurabilityWorkerLoop(); });
      database_->SetCommitDrivenWalSync(true);
      deferred_commits_enabled_.store(true, std::memory_order_release);
    } catch (...) {
      // Retain the blocking commit path if the coordinator cannot start.
      deferred_commits_enabled_.store(false, std::memory_order_release);
    }
    read_workers_.reserve(read_worker_count_);
    for (size_t i = 0; i < read_worker_count_; ++i) {
      try {
        read_workers_.emplace_back([this, i] { ReadWorkerLoop(i); });
      } catch (...) {
        read_worker_count_ = read_workers_.size();
        break;
      }
    }
  }

  void StopReadWorkers() {
    stopping_.store(true);
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
    {
      std::scoped_lock lock(durability_mutex_);
      durability_stopping_ = true;
    }
    durability_ready_.notify_all();
    if (durability_worker_.joinable()) {
      durability_worker_.join();
    }
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
        } else {
          response = ExecuteReadTask(
              task.statements,
              deferred_commits_enabled_.load(std::memory_order_acquire)
                  ? &read_target
                  : nullptr,
              &literal_constant, &pooled_engine);
        }
      } catch (const std::exception& exception) {
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + exception.what(), "XX000");
        if (task.session) {
          AbortOpenTransaction(*task.session);
          if (task.session->transaction_status == 'T') {
            task.session->transaction_status = 'E';
          }
        }
        response += pgwire::ReadyForQuery(
            task.session ? task.session->transaction_status : 'I');
      } catch (...) {
        response =
            pgwire::ErrorResponse("read worker failed: unknown error", "XX000");
        if (task.session) {
          AbortOpenTransaction(*task.session);
          if (task.session->transaction_status == 'T') {
            task.session->transaction_status = 'E';
          }
        }
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
                                .literal_constant_sql = {}};
      completion.plan_cache = std::move(task.plan_cache);
      completion.reply_socket = std::move(task.reply_socket);
      if (literal_constant && task.statements.size() == 1) {
        completion.literal_constant_sql = task.statements[0];
      }
      const bool needs_durability =
          completion.session && completion.session->reply_durability_target >
                                    database_->DurableWALLSN();
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
    if (!stopping_.load() && completion.reply_socket &&
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
      } else if (sent == 0 || (failure != EAGAIN && failure != EWOULDBLOCK)) {
        completion.reply_send_failed = true;
      }
    }
    std::unique_ptr<Client> aborted_session;
    bool completed_on_loop = false;
    bool wake_loop = false;
    {
      std::scoped_lock lock(read_completion_mutex_);
      if (!stopping_.load()) {
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
        target = std::max(target, completion.session->reply_durability_target);
      }
      Status durable = Status::kSuccess;
      const auto sync_begin = std::chrono::steady_clock::now();
      try {
        durable = database_->WaitForCommitDurable(target);
      } catch (const std::exception& error) {
        durable = StatusError(StatusCode::kIOError, error.what());
      } catch (...) {
        durable = StatusError(StatusCode::kIOError, "durability worker failed");
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
        if (durable != Status::kSuccess) {
          const bool block = completion.session->transaction_status == 'T';
          AbortOpenTransaction(*completion.session);
          completion.session->transaction_status = block ? 'E' : 'I';
          completion.response =
              pgwire::ErrorResponse(
                  "transaction durability failed: " + StatusMessage(durable),
                  "40001") +
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

  std::string ExecuteReadTask(const std::vector<std::string>& statements,
                              lsn_t* reply_target = nullptr,
                              bool* literal_constant = nullptr,
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
    for (const std::string& statement : statements) {
      try {
        std::optional<SqlEngine> local_engine;
        SqlEngine& engine = (pooled_engine != nullptr)
                                ? *pooled_engine
                                : local_engine.emplace(*database_);
        StatusOr<QueryResult> executed = engine.Execute(*context, statement);
        if (!executed.HasValue()) {
          const std::string error = engine.LastError().empty()
                                        ? StatusMessage(executed.GetStatus())
                                        : engine.LastError();
          response += pgwire::ErrorResponse(error, "42601");
          ok = false;
          break;
        }
        literal_only = literal_only && engine.LastQueryIsLiteralConstant();
        QueryResult result = std::move(executed.Value());
        if (!result.Statement() ||
            *result.Statement() != StatementType::kSelect) {
          response += pgwire::ErrorResponse(
              "only SELECT statements may use a read worker", "0A000");
          ok = false;
          break;
        }
        StatusOr<std::string> wire = StreamSelectResult(result);
        if (!wire.HasValue()) {
          response +=
              pgwire::ErrorResponse(wire.GetStatus().GetMessage(), "XX000");
          ok = false;
          break;
        }
        response += wire.MoveValue();
        observed_dependency =
            std::max(observed_dependency, context->txn_.DurabilityDependence());
      } catch (const std::exception& exception) {
        response += pgwire::ErrorResponse(exception.what(), "XX000");
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
              "40001");
        }
      } else {
        context->Abort();
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
              "40001");
        }
      }
    }
    if (literal_constant != nullptr) {
      *literal_constant = ok && literal_only && observed_dependency == 0 &&
                          ((reply_target == nullptr) || *reply_target == 0);
    }
    response += pgwire::ReadyForQuery('I');
    return response;
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
        ScheduleSessionCleanup(std::move(completion.session));
        continue;
      }
      if (completion.reply_send_failed) {
        ScheduleSessionCleanup(std::move(completion.session));
        CloseClient(completion.client_fd);
        continue;
      }
      // Only a successful, dependency-free literal query is certified by
      // SqlEngine. Cached wire frames are exact-SQL and idle-state only.
      if (!completion.literal_constant_sql.empty() &&
          completion.response.size() + completion.literal_constant_sql.size() <=
              65536) {
        if (literal_constant_replies_.size() >= 512 ||
            literal_constant_reply_bytes_ + completion.response.size() +
                    completion.literal_constant_sql.size() >
                size_t{4} * 1024 * 1024) {
          literal_constant_replies_.clear();
          literal_constant_reply_bytes_ = 0;
        }
        const auto [entry, inserted] = literal_constant_replies_.emplace(
            completion.literal_constant_sql, completion.response);
        if (inserted) {
          literal_constant_reply_bytes_ +=
              entry->first.size() + entry->second.size();
        }
      }
      Client& client = found->second;
      if (completion.plan_cache) {
        client.plan_cache = std::move(completion.plan_cache);
      }
      if (completion.session) {
        if (completion.session->plan_cache) {
          client.plan_cache = std::move(completion.session->plan_cache);
        }
        client.transaction = std::move(completion.session->transaction);
        client.transaction_status = completion.session->transaction_status;
        client.output_overflow |= completion.session->output_overflow;
        client.worker_shell = std::move(completion.session);
      }
      client.read_query_in_flight = false;
      Queue(client, completion.response);
      if (client.output_overflow) {
        // Queue() latched output_overflow: the response exceeds the queued
        // output cap and was dropped.  Close the connection so the client
        // does not idle until the timeout waiting for an answer that will
        // never arrive (the epoll-path ReadClient does the same).
        CloseClient(completion.client_fd);
        continue;
      }
      ProcessInput(client);
      if (client.peer_eof_seen && !client.read_query_in_flight) {
        client.close_after_write = true;
      }
      if (!WriteClient(completion.client_fd)) {
        CloseClient(completion.client_fd);
      }
    }
  }

  static void FailStatement(Client& client,
                            std::unique_ptr<TransactionContext>& automatic,
                            std::unique_ptr<TransactionContext>& implicit,
                            const std::string& message,
                            std::string_view sqlstate) {
    if (automatic && !automatic->IsFinished()) {
      automatic->Abort();
    }
    if (implicit && !implicit->IsFinished()) {
      implicit->Abort();
    }
    if (client.transaction) {
      if (!client.transaction->IsFinished()) {
        client.transaction->Abort();
      }
      client.transaction.reset();
      client.transaction_status = 'E';
    }
    Queue(client, pgwire::ErrorResponse(message, sqlstate));
  }

  static void AbortOpenTransaction(Client& client) {
    if (client.transaction && !client.transaction->IsFinished()) {
      client.transaction->Abort();
    }
    client.transaction.reset();
  }

  void CloseClient(int fd) {
    const auto found = clients_.find(fd);
    if (found == clients_.end()) {
      return;
    }
    if (found->second.transaction && !read_workers_.empty()) {
      auto session = std::make_unique<Client>(fd, found->second.id, 0, false);
      session->transaction = std::move(found->second.transaction);
      ScheduleSessionCleanup(std::move(session));
    } else {
      AbortOpenTransaction(found->second);
    }
    (void)poller_.Remove(fd);
    found->second.reply_socket->Close();
    clients_.erase(found);
  }

  // Member order minimizes padding (see clang-analyzer-optin.performance.
  // Padding): small scalars are grouped ahead of the larger containers.
  std::unique_ptr<Database> database_;
  std::string startup_error_;
  uint64_t next_client_id_{1};
  std::atomic<bool> deferred_commits_enabled_{false};
  std::thread durability_worker_;
  std::mutex durability_mutex_;
  std::condition_variable durability_ready_;
  std::deque<ReadCompletion> durability_tasks_;
  bool durability_stopping_{false};
  std::atomic<uint64_t> durability_commands_{0};
  std::atomic<uint64_t> durability_wait_ns_{0};
  std::atomic<uint64_t> durability_batches_{0};
  bool metrics_enabled_{false};
  int io_cpu_{-1};
  std::chrono::steady_clock::time_point last_metrics_{};
  std::chrono::steady_clock::time_point last_idle_sweep_{};
  std::atomic<uint64_t> queue_ns_{0};
  std::atomic<uint64_t> execution_ns_{0};
  std::atomic<uint64_t> executed_tasks_{0};
  std::atomic<uint64_t> completion_ns_{0};
  size_t read_worker_count_{1};
  std::atomic<size_t> active_read_queries_{0};
  std::atomic<size_t> pending_worker_tasks_{0};
  // Includes completions already swapped into the event loop's local batch,
  // until their pending count is retired. These cannot join a future sync.
  std::atomic<size_t> potential_commit_tasks_{0};
  std::atomic<size_t> peak_concurrent_reads_{0};
  std::vector<std::thread> read_workers_;
  std::mutex read_task_mutex_;
  std::mutex read_completion_mutex_;
  std::array<std::condition_variable, 16> read_task_ready_;
  std::unordered_map<int, Client> clients_;
  PostgresServerOptions options_;
  // Owned by epoll; workers only return a certification in their completion.
  struct SqlTextHash {
    using is_transparent = void;
    size_t operator()(std::string_view text) const noexcept {
      return std::hash<std::string_view>{}(text);
    }
  };
  std::unordered_map<std::string, std::string, SqlTextHash, std::equal_to<>>
      literal_constant_replies_;
  size_t literal_constant_reply_bytes_{0};
  std::atomic<uint64_t> cached_literal_reply_count_{0};
  std::atomic<uint64_t> direct_reply_count_{0};
  std::array<std::deque<ReadTask>, 16> read_tasks_;
  std::array<bool, 16> read_worker_running_{};
  std::deque<ReadCompletion> read_completions_;
  std::mt19937_64 secret_rng_{std::random_device{}()};
  int listener_fd_{-1};
  // Registration is logically const for the event loop's bookkeeping.
  mutable platform::EventPoller poller_;
  platform::Waker waker_;
  int reserve_fd_{-1};
  // Set when AcceptClients shed the listener from epoll after losing the
  // EMFILE reserve descriptor; cleared once the reserve is restored.
  [[maybe_unused]] bool listener_shed_due_to_fd_pressure_{false};
  uint16_t bound_port_{0};
  std::atomic<bool> stopping_{false};
  bool read_workers_stopping_{false};
};

PostgresServer::PostgresServer(const std::string& database_path,
                               PostgresServerOptions options)
    : impl_(std::make_unique<Impl>(database_path, std::move(options))) {}

PostgresServer::~PostgresServer() = default;

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
