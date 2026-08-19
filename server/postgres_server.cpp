/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "server/postgres_server.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "parser/ast.hpp"
#include "query/sql_engine.hpp"
#include "server/postgres_protocol.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::string ErrnoMessage(std::string_view operation) {
  return std::string(operation) + ": " + std::strerror(errno);
}

std::string StatusMessage(Status status) {
  std::ostringstream output;
  output << status;
  return output.str();
}

std::string UppercaseCommand(std::string_view sql) {
  const size_t begin = sql.find_first_not_of(" \t\r\n");
  if (begin == std::string_view::npos) return {};
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
  }
  return "OK";
}

}  // namespace

class PostgresServer::Impl {
 public:
  Impl(std::string database_path, PostgresServerOptions options)
      : database_(std::move(database_path)), options_(std::move(options)) {
    read_worker_count_ = options_.read_worker_threads;
    if (read_worker_count_ == 0) {
      read_worker_count_ =
          std::max<size_t>(1, std::thread::hardware_concurrency());
    }
  }

  ~Impl() {
    StopReadWorkers();
    for (auto& [fd, client] : clients_) {
      AbortOpenTransaction(client);
      close(fd);
    }
    if (listener_fd_ >= 0) close(listener_fd_);
    if (wake_fd_ >= 0) close(wake_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
  }

  bool Listen(std::string* error) {
    if (listener_fd_ >= 0) return true;

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
      if (candidate < 0) continue;
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
    StartReadWorkers();
    return true;
  }

  int Run(std::string* error) {
    if (listener_fd_ < 0 && !Listen(error)) return 1;
    std::array<epoll_event, 64> events{};
    while (!stopping_.load()) {
      const int count = epoll_wait(epoll_fd_, events.data(), events.size(), -1);
      if (count < 0) {
        if (errno == EINTR) continue;
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
        if (!clients_.contains(fd)) continue;
        bool alive = true;
        if ((flags & EPOLLIN) != 0U) alive = ReadClient(fd);
        if (alive && clients_.contains(fd) && (flags & EPOLLOUT) != 0U) {
          alive = WriteClient(fd);
        }
        if ((flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U) alive = false;
        if (!alive) CloseClient(fd);
      }
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
  struct Client {
    Client(int socket, uint64_t identifier) : fd(socket), id(identifier) {}
    int fd;
    uint64_t id;
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
  };

  struct ReadTask {
    int client_fd;
    uint64_t client_id;
    std::vector<std::string> statements;
  };

  struct ReadCompletion {
    int client_fd;
    uint64_t client_id;
    std::string response;
  };

  bool AddEpollFd(int fd, uint32_t events, std::string* error) {
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
    while (true) {
      const int fd =
          accept4(listener_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
          std::cerr << ErrnoMessage("accept4") << '\n';
        }
        return;
      }
      std::string error;
      if (!AddEpollFd(fd, EPOLLIN | EPOLLRDHUP, &error)) {
        std::cerr << error << '\n';
        close(fd);
        continue;
      }
      clients_.try_emplace(fd, fd, next_client_id_++);
    }
  }

  bool ReadClient(int fd) {
    auto found = clients_.find(fd);
    if (found == clients_.end()) return false;
    Client& client = found->second;
    std::array<char, 8192> buffer{};
    while (true) {
      const ssize_t size = recv(fd, buffer.data(), buffer.size(), 0);
      if (size > 0) {
        client.input.append(buffer.data(), static_cast<size_t>(size));
        if (client.input.size() > options_.max_message_bytes + 5) {
          Queue(client,
                pgwire::ErrorResponse("message exceeds server limit", "54000"));
          client.close_after_write = true;
          break;
        }
        continue;
      }
      if (size == 0) return false;
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      return false;
    }
    ProcessInput(client);
    UpdateClientInterest(client);
    return !(client.close_after_write && client.output.empty());
  }

  bool WriteClient(int fd) {
    auto found = clients_.find(fd);
    if (found == clients_.end()) return false;
    Client& client = found->second;
    while (client.output_offset < client.output.size()) {
      const char* data = client.output.data() + client.output_offset;
      const size_t remaining = client.output.size() - client.output_offset;
      const ssize_t size = send(fd, data, remaining, MSG_NOSIGNAL);
      if (size > 0) {
        client.output_offset += static_cast<size_t>(size);
        continue;
      }
      if (size < 0 && errno == EINTR) continue;
      if (size < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
      return false;
    }
    if (client.output_offset == client.output.size()) {
      client.output.clear();
      client.output_offset = 0;
      if (client.close_after_write) return false;
    }
    UpdateClientInterest(client);
    return true;
  }

  void UpdateClientInterest(const Client& client) {
    epoll_event event{};
    event.events = EPOLLRDHUP;
    if (!client.read_query_in_flight) event.events |= EPOLLIN;
    if (client.output_offset < client.output.size()) event.events |= EPOLLOUT;
    event.data.fd = client.fd;
    (void)epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client.fd, &event);
  }

  void Queue(Client& client, std::string message) {
    if (client.output_offset != 0) {
      client.output.erase(0, client.output_offset);
      client.output_offset = 0;
    }
    client.output += std::move(message);
  }

  void ProcessInput(Client& client) {
    while (!client.close_after_write) {
      if (client.read_query_in_flight) return;
      if (!client.startup_complete) {
        if (!ProcessStartup(client)) return;
        continue;
      }
      if (client.input.size() < 5) return;
      uint32_t length = 0;
      try {
        length = pgwire::ReadUint32(client.input, 1);
      } catch (const std::exception&) {
        return;
      }
      if (length < 4 || length > options_.max_message_bytes) {
        Queue(client, pgwire::ErrorResponse("invalid frontend message length",
                                            "08P01"));
        client.close_after_write = true;
        return;
      }
      const size_t total = static_cast<size_t>(length) + 1;
      if (client.input.size() < total) return;
      const char type = client.input[0];
      const std::string payload = client.input.substr(5, length - 4);
      client.input.erase(0, total);
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
      if (type == 'H') continue;  // Flush: queued output is already writable.
      if (type == 'S') {
        Queue(client, pgwire::ReadyForQuery(client.transaction_status));
        continue;
      }
      Queue(client, pgwire::ErrorResponse(
                        "frontend message type is not supported", "0A000"));
      Queue(client, pgwire::ReadyForQuery(client.transaction_status));
    }
  }

  bool ProcessStartup(Client& client) {
    if (client.input.size() < 4) return false;
    uint32_t length = 0;
    try {
      length = pgwire::ReadUint32(client.input, 0);
    } catch (const std::exception&) {
      return false;
    }
    if (length < 8 || length > options_.max_message_bytes) {
      Queue(client, pgwire::ErrorResponse("invalid startup packet", "08P01"));
      client.close_after_write = true;
      return false;
    }
    if (client.input.size() < length) return false;
    const std::string packet = client.input.substr(0, length);
    client.input.erase(0, length);
    const uint32_t code = pgwire::ReadUint32(packet, 4);
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
        if (name.starts_with("_pq_.")) unsupported.push_back(name);
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
    Queue(client, pgwire::BackendKeyData(
                      static_cast<uint32_t>(getpid()),
                      static_cast<uint32_t>(client.fd) ^ 0x544c414dU));
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
    if (client.transaction_status == 'I' && IsReadOnly(statements)) {
      ScheduleReadQuery(client, statements);
      return;
    }
    for (const std::string& statement : statements) {
      if (!ExecuteStatement(client, statement)) break;
    }
    Queue(client, pgwire::ReadyForQuery(client.transaction_status));
  }

  bool ExecuteStatement(Client& client, const std::string& sql) {
    const std::string command = UppercaseCommand(sql);
    if (command == "BEGIN" || command == "START") {
      if (!client.transaction) {
        client.transaction =
            std::make_unique<TransactionContext>(database_.BeginContext());
      }
      client.transaction_status = 'T';
      Queue(client, pgwire::CommandComplete("BEGIN"));
      return true;
    }
    if (command == "ROLLBACK") {
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
      automatic =
          std::make_unique<TransactionContext>(database_.BeginContext());
      context = automatic.get();
    }

    try {
      SqlEngine engine(database_);
      StatusOr<Executor> prepared = engine.Prepare(*context, sql);
      if (!prepared.HasValue()) {
        const std::string error = engine.LastError().empty()
                                      ? StatusMessage(prepared.GetStatus())
                                      : engine.LastError();
        FailStatement(client, automatic, error, "42601");
        return false;
      }
      Executor executor = std::move(prepared.Value());
      std::vector<Row> rows;
      Row row;
      while (executor->Next(&row, nullptr)) {
        rows.push_back(std::move(row));
        row = Row();
      }
      if (automatic) {
        const Status commit = automatic->PreCommit();
        if (commit != Status::kSuccess) {
          FailStatement(client, automatic,
                        "transaction commit failed: " + StatusMessage(commit),
                        "40001");
          return false;
        }
      }

      if (!engine.LastStatementType()) {
        FailStatement(client, automatic, "SQL statement type is unavailable",
                      "XX000");
        return false;
      }
      const StatementType type = *engine.LastStatementType();
      if (type == StatementType::kSelect) {
        Queue(client, EncodeSelectResult(engine.ResultColumnNames(), rows));
      } else {
        int64_t affected = 0;
        if (!rows.empty() && rows.back().values_.size() >= 2 &&
            rows.back()[1].type == ValueType::kInt64) {
          affected = rows.back()[1].value.int_value;
        }
        Queue(client, pgwire::CommandComplete(CommandTag(type, affected)));
      }
      return true;
    } catch (const std::exception& exception) {
      FailStatement(client, automatic, exception.what(), "XX000");
      return false;
    }
  }

  static std::string EncodeSelectResult(const std::vector<std::string>& names,
                                        const std::vector<Row>& rows) {
    std::string result;
    size_t column_count = names.size();
    if (!rows.empty()) column_count = rows.front().values_.size();
    std::vector<pgwire::ColumnDescription> columns(column_count);
    for (size_t i = 0; i < column_count; ++i) {
      columns[i].name =
          i < names.size() && !names[i].empty() ? names[i] : "?column?";
      columns[i].type = ValueType::kVarChar;
      for (const Row& row : rows) {
        if (i < row.values_.size() && !row[i].IsNull()) {
          columns[i].type = row[i].type;
          break;
        }
      }
    }
    result += pgwire::RowDescription(columns);
    for (const Row& row : rows) result += pgwire::DataRow(row);
    result += pgwire::CommandComplete("SELECT " + std::to_string(rows.size()));
    return result;
  }

  static bool IsReadOnly(const std::vector<std::string>& statements) {
    return std::all_of(
        statements.begin(), statements.end(), [](const std::string& statement) {
          const std::string command = UppercaseCommand(statement);
          return command == "SELECT" || command == "WITH" ||
                 command == "EXPLAIN";
        });
  }

  void ScheduleReadQuery(Client& client,
                         const std::vector<std::string>& statements) {
    client.read_query_in_flight = true;
    {
      std::scoped_lock lock(read_task_mutex_);
      read_tasks_.push_back({client.fd, client.id, statements});
    }
    read_task_ready_.notify_one();
  }

  void StartReadWorkers() {
    if (!read_workers_.empty()) return;
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
      if (worker.joinable()) worker.join();
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
        if (read_workers_stopping_ && read_tasks_.empty()) return;
        task = std::move(read_tasks_.front());
        read_tasks_.pop_front();
      }

      const size_t active = active_read_queries_.fetch_add(1) + 1;
      size_t peak = peak_concurrent_reads_.load();
      while (peak < active &&
             !peak_concurrent_reads_.compare_exchange_weak(peak, active)) {
      }
      std::string response = ExecuteReadTask(task.statements);
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
    for (const std::string& statement : statements) {
      std::unique_ptr<TransactionContext> context =
          std::make_unique<TransactionContext>(
              database_.BeginReadOnlyContext());
      try {
        SqlEngine engine(database_);
        StatusOr<Executor> prepared = engine.Prepare(*context, statement);
        if (!prepared.HasValue()) {
          const std::string error = engine.LastError().empty()
                                        ? StatusMessage(prepared.GetStatus())
                                        : engine.LastError();
          if (!context->txn_.IsFinished()) context->Abort();
          response += pgwire::ErrorResponse(error, "42601");
          break;
        }
        if (!engine.LastStatementType() ||
            *engine.LastStatementType() != StatementType::kSelect) {
          if (!context->txn_.IsFinished()) context->Abort();
          response += pgwire::ErrorResponse(
              "only SELECT statements may use a read worker", "0A000");
          break;
        }
        Executor executor = std::move(prepared.Value());
        std::vector<Row> rows;
        Row row;
        while (executor->Next(&row, nullptr)) {
          rows.push_back(std::move(row));
          row = Row();
        }
        const Status commit = context->PreCommit();
        if (commit != Status::kSuccess) {
          response += pgwire::ErrorResponse(
              "read transaction commit failed: " + StatusMessage(commit),
              "40001");
          break;
        }
        response += EncodeSelectResult(engine.ResultColumnNames(), rows);
      } catch (const std::exception& exception) {
        if (!context->txn_.IsFinished()) context->Abort();
        response += pgwire::ErrorResponse(exception.what(), "XX000");
        break;
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
      Queue(client, std::move(completion.response));
      ProcessInput(client);
      UpdateClientInterest(client);
    }
  }

  void FailStatement(Client& client,
                     std::unique_ptr<TransactionContext>& automatic,
                     const std::string& message, std::string_view sqlstate) {
    if (automatic && !automatic->txn_.IsFinished()) automatic->Abort();
    if (client.transaction) {
      if (!client.transaction->txn_.IsFinished()) client.transaction->Abort();
      client.transaction.reset();
      client.transaction_status = 'E';
    }
    Queue(client, pgwire::ErrorResponse(message, sqlstate));
  }

  static void AbortOpenTransaction(Client& client) {
    if (client.transaction && !client.transaction->txn_.IsFinished()) {
      client.transaction->Abort();
    }
    client.transaction.reset();
  }

  void CloseClient(int fd) {
    const auto found = clients_.find(fd);
    if (found == clients_.end()) return;
    AbortOpenTransaction(found->second);
    (void)epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    clients_.erase(found);
  }

  Database database_;
  PostgresServerOptions options_;
  int listener_fd_{-1};
  int epoll_fd_{-1};
  int wake_fd_{-1};
  uint16_t bound_port_{0};
  std::atomic<bool> stopping_{false};
  std::unordered_map<int, Client> clients_;
  uint64_t next_client_id_{1};

  size_t read_worker_count_{1};
  std::mutex read_task_mutex_;
  std::condition_variable read_task_ready_;
  std::deque<ReadTask> read_tasks_;
  bool read_workers_stopping_{false};
  std::vector<std::thread> read_workers_;
  std::mutex read_completion_mutex_;
  std::deque<ReadCompletion> read_completions_;
  std::atomic<size_t> active_read_queries_{0};
  std::atomic<size_t> peak_concurrent_reads_{0};
};

PostgresServer::PostgresServer(std::string database_path,
                               PostgresServerOptions options)
    : impl_(std::make_unique<Impl>(std::move(database_path),
                                   std::move(options))) {}

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
