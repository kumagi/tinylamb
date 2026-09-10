/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_server.hpp"

#include <asm-generic/socket.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
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
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
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
      while (pos + 1 < sql.size() && (sql[pos] != '*' || sql[pos + 1] != '/')) {
        ++pos;
      }
      pos = std::min(sql.size(), pos + 2);
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
    case StatementType::kDropTable:
      return "DROP TABLE";
    case StatementType::kSelect:
      return "SELECT " + std::to_string(affected_rows);
    case StatementType::kInsert:
      return "INSERT 0 " + std::to_string(affected_rows);
    case StatementType::kUpdate:
      return "UPDATE " + std::to_string(affected_rows);
    case StatementType::kDelete:
      return "DELETE " + std::to_string(affected_rows);
    case StatementType::kAnalyze:
      return "ANALYZE";
  }
  return "OK";
}

}  // namespace

class PostgresServer::Impl {
 public:
  Impl(const std::string& database_path, PostgresServerOptions options)
      : options_(std::move(options)) {
    // Opening (and recovering) the database is fallible; keep the failure
    // for Listen() instead of throwing out of the constructor.
    StatusOr<std::unique_ptr<Database>> created =
        Database::Create(database_path);
    if (created.HasValue()) {
      database_ = created.MoveValue();
    } else {
      startup_error_ = "failed to open database " + database_path + ": " +
                       ToString(created.GetStatus());
    }
    read_worker_count_ = options_.read_worker_threads;
    if (read_worker_count_ == 0) {
      read_worker_count_ =
          std::max<size_t>(1, std::thread::hardware_concurrency());
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
      close(fd);
    }
    if (listener_fd_ >= 0) {
      close(listener_fd_);
    }
    if (wake_fd_ >= 0) {
      close(wake_fd_);
    }
    if (epoll_fd_ >= 0) {
      close(epoll_fd_);
    }
    if (reserve_fd_ >= 0) {
      close(reserve_fd_);
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
      const int candidate =
          socket(address->ai_family,
                 address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                 address->ai_protocol);
      if (candidate < 0) {
        continue;
      }
      const int enabled = 1;
      setsockopt(candidate, SOL_SOCKET, SO_REUSEADDR, &enabled,
                 sizeof(enabled));
      if (bind(candidate, address->ai_addr, address->ai_addrlen) == 0 &&
          listen(candidate, options_.backlog) == 0) {
        listener_fd_ = candidate;
        break;
      }
      close(candidate);
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

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      *error = ErrnoMessage("epoll_create1");
      return false;
    }
    wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
      *error = ErrnoMessage("eventfd");
      return false;
    }
    if (!AddEpollFd(listener_fd_, EPOLLIN, error) ||
        !AddEpollFd(wake_fd_, EPOLLIN, error)) {
      return false;
    }
    // Reserve one descriptor so AcceptClients() can still drain the
    // listener when the process hits EMFILE/ENFILE (level-triggered epoll
    // would otherwise spin forever on the readable listener).
    reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
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
    std::array<epoll_event, 64> events{};
    while (!stopping_.load()) {
      // Bounded wait so the idle-connection sweep below keeps running even
      // when no events arrive.
      const int count = epoll_wait(epoll_fd_, events.data(), events.size(),
                                   kIdleSweepIntervalMs);
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        *error = ErrnoMessage("epoll_wait");
        return 1;
      }
      for (int i = 0; i < count; ++i) {
        const int fd = events[static_cast<size_t>(i)].data.fd;
        const uint32_t flags = events[static_cast<size_t>(i)].events;
        if (fd == listener_fd_) {
          AcceptClients();
          continue;
        }
        if (fd == wake_fd_) {
          uint64_t ignored = 0;
          const ssize_t read_size = read(wake_fd_, &ignored, sizeof(ignored));
          (void)read_size;
          ProcessReadCompletions();
          continue;
        }
        if (!clients_.contains(fd)) {
          continue;
        }
        bool alive = true;
        if ((flags & EPOLLIN) != 0U) {
          alive = ReadClient(fd);
        }
        if (alive && clients_.contains(fd) && (flags & EPOLLOUT) != 0U) {
          alive = WriteClient(fd);
        }
        if ((flags & (EPOLLERR | EPOLLHUP)) != 0U) {
          alive = false;
        } else if ((flags & EPOLLRDHUP) != 0U && alive &&
                   clients_.contains(fd)) {
          // Half-close: stop reading, but let an already-produced response
          // (or a worker read still in flight) drain before the socket
          // closes instead of discarding the answer.
          const Client& client = clients_.at(fd);
          if (client.output.empty() && !client.read_query_in_flight) {
            alive = false;
          }
        }
        if (!alive) {
          CloseClient(fd);
        }
      }
      SweepIdleClients();
    }
    return 0;
  }

  void RequestStop() {
    stopping_.store(true);
    if (wake_fd_ >= 0) {
      const uint64_t wake = 1;
      const ssize_t write_size = write(wake_fd_, &wake, sizeof(wake));
      (void)write_size;
    }
  }

  uint16_t BoundPort() const { return bound_port_; }
  size_t ReadWorkerCount() const { return read_worker_count_; }
  size_t PeakConcurrentReadQueries() const {
    return peak_concurrent_reads_.load();
  }

 private:
  // Unpredictable BackendKeyData secret (CancelRequest is not implemented
  // yet, but the key must not be guessable from the fd alone).
  uint32_t NextSecret() { return static_cast<uint32_t>(secret_rng_()); }

  struct Client {
    Client(int socket, uint64_t identifier, uint32_t secret_key)
        : fd(socket), id(identifier), secret(secret_key) {}
    int fd;
    uint64_t id;
    uint32_t secret;
    bool startup_complete{false};
    bool close_after_write{false};
    std::string input;
    std::string output;
    size_t output_offset{0};
    std::string user;
    std::string database;
    std::unique_ptr<TransactionContext> transaction;
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
    bool output_overflow = false;
  };

  struct ReadTask {
    int client_fd{};
    uint64_t client_id{};
    std::vector<std::string> statements;
  };

  struct ReadCompletion {
    int client_fd;
    uint64_t client_id;
    std::string response;
  };

  bool AddEpollFd(int fd, uint32_t events, std::string* error) const {
    epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
      *error = ErrnoMessage("epoll_ctl add");
      return false;
    }
    return true;
  }

  void AcceptClients() {
    // A previous EMFILE shed removed the listener from epoll; try to restore
    // the reserve descriptor (and the epoll registration) before accepting.
    if (listener_shed_due_to_fd_pressure_) {
      reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
      if (reserve_fd_ >= 0) {
        std::string error;
        if (AddEpollFd(listener_fd_, EPOLLIN, &error)) {
          listener_shed_due_to_fd_pressure_ = false;
        } else {
          std::cerr << error << '\n';
          ::close(reserve_fd_);
          reserve_fd_ = -1;
          return;
        }
      } else {
        return;
      }
    }
    while (true) {
      const int fd =
          accept4(listener_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        if ((errno == EMFILE || errno == ENFILE) && reserve_fd_ >= 0) {
          // Descriptor exhaustion: the level-triggered listener stays
          // readable and would spin forever. Consume (and shed) one pending
          // connection via the reserved descriptor, then restore it.
          const std::string reason = ErrnoMessage("accept4");
          ::close(reserve_fd_);
          reserve_fd_ = -1;
          const int shed = accept4(listener_fd_, nullptr, nullptr,
                                   SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (shed >= 0) {
            ::close(shed);
          }
          std::cerr << "descriptor limit hit (" << reason
                    << "); shed one pending connection\n";
          reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
          if (reserve_fd_ < 0) {
            // Without a reserve descriptor the next EMFILE accept would spin
            // the level-triggered loop. Shed the listener from epoll until a
            // future AcceptClients call restores the reserve: the cost is a
            // delayed accept, not a busy loop.
            epoll_event dummy{};
            (void)epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listener_fd_, &dummy);
            listener_shed_due_to_fd_pressure_ = true;
          }
        } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
          std::cerr << ErrnoMessage("accept4") << '\n';
        }
        return;
      }
      // Disable Nagle so small responses are not delayed waiting for ACKs.
      const int enabled = 1;
      setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
      if (clients_.size() >= options_.max_connections) {
        // Over capacity: shed immediately; the client observes EOF.
        close(fd);
        continue;
      }
      // Register the client before adding the fd to epoll: if the map insert
      // throws, the fd is still ours to close; registered the other way
      // around, an epoll-enrolled fd with no client entry would be ignored
      // by the event loop until shutdown.
      clients_.try_emplace(fd, fd, next_client_id_++, NextSecret());
      std::string error;
      if (!AddEpollFd(fd, EPOLLIN | EPOLLRDHUP, &error)) {
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
    while (true) {
      const ssize_t size = recv(fd, buffer.data(), buffer.size(), 0);
      if (size > 0) {
        client.input.append(buffer.data(), static_cast<size_t>(size));
        client.last_activity = std::chrono::steady_clock::now();
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
    ProcessInput(client);
    if (eof_seen) {
      client.close_after_write = true;
    }
    if (client.output_overflow) {
      // Queue() refuses to buffer past the per-connection output cap: drop
      // the connection instead of propagating into the event loop.
      CloseClient(fd);
      return false;
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
      const ssize_t size = send(fd, data, remaining, MSG_NOSIGNAL);
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
      // release the (up to 64 MiB) capacity: an idle connection would
      // otherwise pin its high-water output buffer until close.
      client.output.shrink_to_fit();
      client.output_offset = 0;
      if (client.close_after_write) {
        return false;
      }
    }
    UpdateClientInterest(client);
    return true;
  }

  void UpdateClientInterest(const Client& client) const {
    epoll_event event{};
    event.events = EPOLLRDHUP;
    if (!client.read_query_in_flight) {
      event.events |= EPOLLIN;
    }
    if (client.output_offset < client.output.size()) {
      event.events |= EPOLLOUT;
    }
    event.data.fd = client.fd;
    (void)epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client.fd, &event);
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
      const std::string payload = client.input.substr(5, length - 4);
      client.input.erase(0, total);
      // The input cap is max_message_bytes + 5 (16 MiB); give the capacity
      // back once the backlog drains so an idle connection does not pin it.
      if (client.input.empty()) {
        client.input.shrink_to_fit();
      }
      if (type == 'X') {
        client.close_after_write = true;
        return;
      }
      if (type == 'Q') {
        if (payload.empty() || payload.back() != '\0') {
          Queue(client, pgwire::ErrorResponse(
                            "unterminated simple query message", "08P01"));
          Queue(client, pgwire::ReadyForQuery(client.transaction_status));
          continue;
        }
        ExecuteSimpleQuery(
            client, std::string_view(payload.data(), payload.size() - 1));
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
    Queue(client, pgwire::ParameterStatus("standard_conforming_strings", "on"));
    Queue(client, pgwire::ParameterStatus("TimeZone", "UTC"));
    Queue(client, pgwire::BackendKeyData(static_cast<uint32_t>(getpid()),
                                         client.secret));
    Queue(client, pgwire::ReadyForQuery('I'));
    client.startup_complete = true;
    return true;
  }

  void ExecuteSimpleQuery(Client& client, std::string_view sql) {
    const std::vector<std::string> statements = pgwire::SplitSqlStatements(sql);
    if (statements.empty()) {
      Queue(client, pgwire::EmptyQueryResponse());
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
      return;
    }
    if (client.transaction_status == 'I' && IsReadOnly(statements) &&
        !client.session_has_temp_objects) {
      ScheduleReadQuery(client, statements);
      return;
    }
    // PostgreSQL semantics: all statements of one Query message run in a
    // single implicit transaction unless the message itself manages
    // transactions. The implicit transaction commits only after every
    // statement succeeded.
    std::unique_ptr<TransactionContext> implicit;
    if (!ContainsTransactionControl(statements) &&
        client.transaction == nullptr && client.transaction_status != 'E') {
      implicit =
          std::make_unique<TransactionContext>(database_->BeginContext());
    }
    bool ok = true;
    for (const std::string& statement : statements) {
      if (RegistersTempObject(statement)) {
        client.session_has_temp_objects = true;
      }
      if (!ExecuteStatement(client, statement, implicit)) {
        ok = false;
        break;
      }
    }
    if (implicit != nullptr) {
      if (ok) {
        const Status commit = implicit->PreCommit();
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
    Queue(client, pgwire::ReadyForQuery(client.transaction_status));
  }

  static bool ContainsTransactionControl(
      const std::vector<std::string>& statements) {
    return std::ranges::any_of(statements, [](const std::string& statement) {
      const std::string command = UppercaseCommand(statement);
      return command == "BEGIN" || command == "START" || command == "COMMIT" ||
             command == "END" || command == "ROLLBACK" ||
             // PostgreSQL alias for ROLLBACK.
             command == "ABORT";
    });
  }

  bool ExecuteStatement(Client& client, const std::string& sql,
                        std::unique_ptr<TransactionContext>& implicit) {
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
        client.transaction =
            std::make_unique<TransactionContext>(database_->BeginContext());
      }
      client.transaction_status = 'T';
      Queue(client, pgwire::CommandComplete("BEGIN"));
      return true;
    }
    if (command == "ROLLBACK" || command == "ABORT") {
      AbortOpenTransaction(client);
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
        const Status status = client.transaction->PreCommit();
        client.transaction.reset();
        if (status != Status::kSuccess) {
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
      SqlEngine engine(*database_);
      StatusOr<QueryResult> executed = engine.Execute(*context, sql);
      if (!executed.HasValue()) {
        const std::string error = engine.LastError().empty()
                                      ? StatusMessage(executed.GetStatus())
                                      : engine.LastError();
        FailStatement(client, automatic, implicit, error, "42601");
        return false;
      }
      QueryResult result = std::move(executed.Value());
      if (!result.Statement()) {
        FailStatement(client, automatic, implicit,
                      "SQL statement type is unavailable", "XX000");
        return false;
      }
      const StatementType type = *result.Statement();
      if (type == StatementType::kSelect) {
        StatusOr<std::string> wire = StreamSelectResult(result);
        if (!wire.HasValue()) {
          FailStatement(client, automatic, implicit,
                        wire.GetStatus().GetMessage(), "XX000");
          return false;
        }
        Queue(client, wire.MoveValue());
      } else {
        Queue(client,
              pgwire::CommandComplete(CommandTag(type, result.AffectedRows())));
      }
      if (automatic) {
        // Standalone statement in a transaction-control message: legacy
        // commit-per-statement behavior.
        const Status commit = automatic->PreCommit();
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

  void ScheduleReadQuery(Client& client,
                         const std::vector<std::string>& statements) {
    client.read_query_in_flight = true;
    client.read_started_at = std::chrono::steady_clock::now();
    {
      std::scoped_lock lock(read_task_mutex_);
      read_tasks_.push_back({client.fd, client.id, statements});
    }
    read_task_ready_.notify_one();
  }

  void StartReadWorkers() {
    if (!read_workers_.empty()) {
      return;
    }
    read_workers_.reserve(read_worker_count_);
    for (size_t i = 0; i < read_worker_count_; ++i) {
      read_workers_.emplace_back([this] { ReadWorkerLoop(); });
    }
  }

  void StopReadWorkers() {
    {
      std::scoped_lock lock(read_task_mutex_);
      read_workers_stopping_ = true;
    }
    read_task_ready_.notify_all();
    for (std::thread& worker : read_workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    read_workers_.clear();
  }

  void ReadWorkerLoop() {
    while (true) {
      ReadTask task;
      {
        std::unique_lock lock(read_task_mutex_);
        read_task_ready_.wait(lock, [&] {
          return read_workers_stopping_ || !read_tasks_.empty();
        });
        if (read_workers_stopping_ && read_tasks_.empty()) {
          return;
        }
        task = std::move(read_tasks_.front());
        read_tasks_.pop_front();
      }

      const size_t active = active_read_queries_.fetch_add(1) + 1;
      size_t peak = peak_concurrent_reads_.load();
      while (peak < active &&
             !peak_concurrent_reads_.compare_exchange_weak(peak, active)) {
      }
      // A throwing worker would escape ReadWorkerLoop and take down the
      // whole server via std::terminate. ExecuteReadTask only catches
      // std::exception per statement; allocation failures, unknown
      // exceptions, and transaction-context setup must also surface as a
      // per-query error response, never as a dead thread.
      std::string response;
      try {
        response = ExecuteReadTask(task.statements);
      } catch (const std::exception& exception) {
        response = pgwire::ErrorResponse(
            std::string("read worker failed: ") + exception.what(), "XX000");
        response += pgwire::ReadyForQuery('I');
      } catch (...) {
        response =
            pgwire::ErrorResponse("read worker failed: unknown error", "XX000");
        response += pgwire::ReadyForQuery('I');
      }
      active_read_queries_.fetch_sub(1);

      {
        std::scoped_lock lock(read_completion_mutex_);
        read_completions_.push_back(
            {task.client_fd, task.client_id, std::move(response)});
      }
      if (wake_fd_ >= 0) {
        const uint64_t wake = 1;
        const ssize_t write_size = write(wake_fd_, &wake, sizeof(wake));
        (void)write_size;
      }
    }
  }

  std::string ExecuteReadTask(const std::vector<std::string>& statements) {
    std::string response;
    // One snapshot for the whole message so multi-statement reads observe a
    // consistent database image.
    std::unique_ptr<TransactionContext> context =
        std::make_unique<TransactionContext>(database_->BeginReadOnlyContext());
    bool ok = true;
    for (const std::string& statement : statements) {
      try {
        SqlEngine engine(*database_);
        StatusOr<QueryResult> executed = engine.Execute(*context, statement);
        if (!executed.HasValue()) {
          const std::string error = engine.LastError().empty()
                                        ? StatusMessage(executed.GetStatus())
                                        : engine.LastError();
          response += pgwire::ErrorResponse(error, "42601");
          ok = false;
          break;
        }
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
      } catch (const std::exception& exception) {
        response += pgwire::ErrorResponse(exception.what(), "XX000");
        ok = false;
        break;
      }
    }
    if (!context->IsFinished()) {
      if (ok) {
        const Status commit = context->PreCommit();
        if (commit != Status::kSuccess) {
          response += pgwire::ErrorResponse(
              "read transaction commit failed: " + StatusMessage(commit),
              "40001");
        }
      } else {
        context->Abort();
      }
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
      const auto found = clients_.find(completion.client_fd);
      if (found == clients_.end() || found->second.id != completion.client_id) {
        continue;
      }
      Client& client = found->second;
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
      UpdateClientInterest(client);
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
    AbortOpenTransaction(found->second);
    (void)epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    clients_.erase(found);
  }

  // Member order minimizes padding (see clang-analyzer-optin.performance.
  // Padding): small scalars are grouped ahead of the larger containers.
  std::unique_ptr<Database> database_;
  std::string startup_error_;
  uint64_t next_client_id_{1};
  size_t read_worker_count_{1};
  std::atomic<size_t> active_read_queries_{0};
  std::atomic<size_t> peak_concurrent_reads_{0};
  std::vector<std::thread> read_workers_;
  std::mutex read_task_mutex_;
  std::mutex read_completion_mutex_;
  std::condition_variable read_task_ready_;
  std::unordered_map<int, Client> clients_;
  PostgresServerOptions options_;
  std::deque<ReadTask> read_tasks_;
  std::deque<ReadCompletion> read_completions_;
  std::mt19937_64 secret_rng_{std::random_device{}()};
  int listener_fd_{-1};
  int epoll_fd_{-1};
  int wake_fd_{-1};
  int reserve_fd_{-1};
  // Set when AcceptClients shed the listener from epoll after losing the
  // EMFILE reserve descriptor; cleared once the reserve is restored.
  bool listener_shed_due_to_fd_pressure_{false};
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

size_t PostgresServer::PeakConcurrentReadQueries() const {
  return impl_->PeakConcurrentReadQueries();
}

}  // namespace tinylamb
