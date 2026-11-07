/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Tiny/PG use Simple Query; the optional MySQL adapter uses COM_QUERY.
// All transports share one TPC-C generator, without transaction rewrites.
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <latch>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "benchmark/tpcc_workload.hpp"
#include "common/platform/socket.hpp"
#include "common/platform/synced_stream.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#ifdef TINYLAMB_TPCC_MYSQL
#include "benchmark/tpcc_mysql_session.hpp"
#endif

namespace {
using namespace tinylamb;
using Clock = std::chrono::steady_clock;

uint32_t U32(std::string_view s, size_t n) {
  uint32_t v = 0;
  for (size_t i = 0; i < 4; ++i)
    v = (v << 8U) | static_cast<unsigned char>(s[n + i]);
  return v;
}
uint16_t U16(std::string_view s, size_t n) {
  return static_cast<uint16_t>((static_cast<unsigned char>(s[n]) << 8U) |
                               static_cast<unsigned char>(s[n + 1]));
}
void Put32(std::string& s, uint32_t v) {
  for (int shift = 24; shift >= 0; shift -= 8)
    s.push_back(static_cast<char>((v >> shift) & 255U));
}

class Session final : public TpccSqlSession {
 public:
  ~Session() override {
    if (fd_ >= 0) close(fd_);
  }
  Status Connect(int port) {
    fd_ = platform::OpenSocket(AF_INET, SOCK_STREAM, 0, {});
    if (fd_ < 0) return Status::kIOError;
    timeval timeout{.tv_sec = 15, .tv_usec = 0};
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    int yes = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0)
      return Status::kIOError;
    std::string startup;
    Put32(startup, 8 + 11 + 15 + 1);
    Put32(startup, 196608);
    startup.append("user\0bench\0database\0bench\0\0", 27);
    std::vector<Row> ignored;
    if (!Send(startup)) return Status::kIOError;
    return Receive(&ignored);
  }
  Status Begin(bool /*read_only*/) override {
    std::vector<Row> ignored;
    return Execute("BEGIN", &ignored);
  }
  Status Commit() override {
    std::vector<Row> ignored;
    const Status status = Execute("COMMIT", &ignored);
    if (status == Status::kSuccess && tag_ != "COMMIT") {
      error_ = "commit did not commit: " + tag_;
      return Status::kConflicts;
    }
    return status;
  }
  void Abort() override {
    std::vector<Row> ignored;
    (void)Execute("ROLLBACK", &ignored);
  }
  std::string_view LastError() const override { return error_; }
  void TraceTransaction(uint64_t sequence, TpccTransactionType type) {
    trace_ = true;
    trace_sequence_ = sequence;
    trace_type_ = static_cast<int>(type);
  }
  Status Execute(std::string_view sql, std::vector<Row>* rows) override {
    const auto before = trace_ ? Clock::now() : Clock::time_point{};
    rows->clear();
    error_.clear();
    tag_.clear();
    std::string packet(1, 'Q');
    Put32(packet, static_cast<uint32_t>(sql.size() + 5));
    packet.append(sql);
    packet.push_back('\0');
    if (!Send(packet)) {
      error_ = "socket send failed";
      return Status::kIOError;
    }
    const Status status = Receive(rows);
    if (trace_) {
      const auto us =
          std::chrono::duration<double, std::micro>(Clock::now() - before)
              .count();
      platform::SyncedOStream(std::cerr)
          << "TPCC_SQL {\"txn\":" << trace_sequence_
          << ",\"type\":" << trace_type_ << ",\"us\":" << us
          << ",\"status\":" << static_cast<int>(status.GetCode())
          << ",\"rows\":" << rows->size() << ",\"tag\":" << std::quoted(tag_)
          << ",\"sql\":" << std::quoted(std::string(sql)) << "}\n";
    }
    return status;
  }

 private:
  bool Send(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t n = platform::SendNoSignal(fd_, bytes.data(), bytes.size());
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return false;
      bytes.remove_prefix(static_cast<size_t>(n));
    }
    return true;
  }
  bool Read(char* data, size_t size) {
    while (size) {
      const ssize_t n = recv(fd_, data, size, 0);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return false;
      data += n;
      size -= static_cast<size_t>(n);
    }
    return true;
  }
  Status Receive(std::vector<Row>* rows) {
    std::vector<uint32_t> types;
    bool failed = false;
    while (true) {
      std::array<char, 5> header{};
      if (!Read(header.data(), header.size())) {
        error_ = "socket read failed";
        return Status::kIOError;
      }
      const uint32_t length = U32(std::string_view(header.data(), 5), 1);
      if (length < 4 || length > 16 * 1024 * 1024) return Status::kCorrupt;
      std::string body(length - 4, '\0');
      if (!Read(body.data(), body.size())) return Status::kIOError;
      if (header[0] == 'T') {
        if (body.size() < 2) return Status::kCorrupt;
        size_t offset = 2;
        types.clear();
        for (size_t i = 0; i < U16(body, 0); ++i) {
          const size_t end = body.find('\0', offset);
          if (end == std::string::npos || end + 19 > body.size())
            return Status::kCorrupt;
          types.push_back(U32(body, end + 7));
          offset = end + 19;
        }
        if (offset != body.size()) return Status::kCorrupt;
      } else if (header[0] == 'D') {
        if (body.size() < 2 || U16(body, 0) != types.size())
          return Status::kCorrupt;
        size_t offset = 2;
        std::vector<Value> values;
        for (uint32_t type : types) {
          if (offset + 4 > body.size()) return Status::kCorrupt;
          const uint32_t count = U32(body, offset);
          offset += 4;
          if (count == UINT32_MAX) {
            values.emplace_back();
            continue;
          }
          if (count > body.size() - offset) return Status::kCorrupt;
          std::string_view text(body.data() + offset, count);
          offset += count;
          if (type == 20 || type == 21 || type == 23) {
            int64_t v = 0;
            const auto [end, ec] =
                std::from_chars(text.data(), text.data() + text.size(), v);
            if (ec != std::errc() || end != text.data() + text.size())
              return Status::kCorrupt;
            values.emplace_back(v);
          } else if (type == 700 || type == 701 || type == 1700) {
            double v = 0;
            const auto [end, ec] =
                std::from_chars(text.data(), text.data() + text.size(), v);
            if (ec != std::errc() || end != text.data() + text.size())
              return Status::kCorrupt;
            values.emplace_back(v);
          } else {
            values.emplace_back(std::string(text));
          }
        }
        if (offset != body.size()) return Status::kCorrupt;
        rows->emplace_back(std::move(values));
      } else if (header[0] == 'C') {
        if (body.empty() || body.back() != '\0') return Status::kCorrupt;
        tag_ = body.substr(0, body.size() - 1);
        if (tag_.starts_with("INSERT ") || tag_.starts_with("UPDATE ") ||
            tag_.starts_with("DELETE ")) {
          const std::string count = tag_.substr(tag_.find_last_of(' ') + 1);
          int64_t n = 0;
          const auto [end, ec] =
              std::from_chars(count.data(), count.data() + count.size(), n);
          if (ec != std::errc() || end != count.data() + count.size())
            return Status::kCorrupt;
          rows->emplace_back(
              std::vector<Value>{Value(std::string("affected")), Value(n)});
        }
      } else if (header[0] == 'E') {
        failed = true;
        error_ = body;
        std::replace(error_.begin(), error_.end(), '\0', ' ');
      } else if (header[0] == 'R') {
        if (body.size() != 4 || U32(body, 0) != 0)
          return Status::kInvalidArgument;
      } else if (header[0] == 'Z') {
        return failed ? Status::kConflicts : Status::kSuccess;
      }
    }
  }
  int fd_{-1};
  std::string error_, tag_;
  bool trace_{false};
  uint64_t trace_sequence_{0};
  int trace_type_{0};
};

struct Metrics {
  uint64_t attempted{0}, committed{0}, user_rollback{0}, sql{0};
  std::vector<double> success_ms;
  std::string error;
};

}  // namespace

int main(int argc, char** argv) {
  int port = 0, clients = 10, seconds = 30, warmup = 5;
  uint64_t seed = 20260819;
  bool trace_sql = false, use_mysql = false, transport_test = false;
  int transaction_only = -1;
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string_view key(argv[i]), value(argv[i + 1]);
    uint64_t n = 0;
    const auto [end, ec] =
        std::from_chars(value.data(), value.data() + value.size(), n);
    if (ec != std::errc() || end != value.data() + value.size()) return 2;
    if (key == "--port" && n <= 65535)
      port = static_cast<int>(n);
    else if (key == "--clients" && n <= 128)
      clients = static_cast<int>(n);
    else if (key == "--seconds" && n <= 3600)
      seconds = static_cast<int>(n);
    else if (key == "--warmup" && n <= 3600)
      warmup = static_cast<int>(n);
    else if (key == "--seed")
      seed = n;
    else if (key == "--trace-sql" && n <= 1)
      trace_sql = n != 0;
    else if (key == "--mysql" && n <= 1)
      use_mysql = n != 0;
    else if (key == "--transport-test" && n <= 1)
      transport_test = n != 0;
    else if (key == "--transaction" && n < 5)
      transaction_only = static_cast<int>(n);
    else
      return 2;
  }
  if (!port || clients < 1 || seconds < 1 || argc % 2 == 0) return 2;
  if (use_mysql && trace_sql) return 2;
#ifdef TINYLAMB_TPCC_MYSQL
  if (use_mysql && mysql_library_init(0, nullptr, nullptr) != 0) return 1;
#else
  if (use_mysql) {
    std::cerr << "build with TINYLAMB_TPCC_MYSQL=ON for MySQL\n";
    return 2;
  }
#endif
  const auto connect_session = [&]() -> std::unique_ptr<TpccSqlSession> {
#ifdef TINYLAMB_TPCC_MYSQL
    if (use_mysql) {
      auto session = std::make_unique<TpccMySqlSession>();
      if (session->Connect(port) != Status::kSuccess) {
        std::cerr << session->LastError() << '\n';
        return nullptr;
      }
      return session;
    }
#endif
    auto session = std::make_unique<Session>();
    if (session->Connect(port) != Status::kSuccess) return nullptr;
    return session;
  };
  if (transport_test) {
    auto session = connect_session();
    if (!session) return 1;
    std::vector<Row> rows;
    const auto execute = [&](std::string_view sql) {
      const auto status = session->Execute(sql, &rows);
      if (status != Status::kSuccess) std::cerr << session->LastError() << '\n';
      return status == Status::kSuccess;
    };
    if (!execute("SELECT 9223372036854775807, 1.25, NULL, 'a,b'")) return 1;
    const Row expected{Value(int64_t{9223372036854775807}), Value(1.25),
                       Value(), Value(std::string("a,b"))};
    if (rows.size() != 1 || rows[0] != expected) return 1;
    if (!execute("CREATE TABLE tpcc_transport_probe (id BIGINT, v BIGINT)") ||
        !execute(
            "CREATE UNIQUE INDEX tpcc_probe_pk ON tpcc_transport_probe(id)") ||
        !execute("INSERT INTO tpcc_transport_probe VALUES (1,10)"))
      return 1;
    if (rows.size() != 1 ||
        rows[0] != Row{Value(std::string("affected")), Value(int64_t{1})})
      return 1;
    if (session->Begin(false) != Status::kSuccess ||
        !execute("UPDATE tpcc_transport_probe SET v=20 WHERE id=1"))
      return 1;
    session->Abort();
    if (!execute("SELECT v FROM tpcc_transport_probe") ||
        rows != std::vector<Row>{Row{Value(int64_t{10})}})
      return 1;
    if (session->Begin(false) != Status::kSuccess ||
        !execute("UPDATE tpcc_transport_probe SET v=30 WHERE id=1") ||
        session->Commit() != Status::kSuccess ||
        !execute("SELECT v FROM tpcc_transport_probe") ||
        rows != std::vector<Row>{Row{Value(int64_t{30})}})
      return 1;
    if (session->Begin(false) != Status::kSuccess) return 1;
    if (session->Execute("INSERT INTO tpcc_transport_probe VALUES (1,99)",
                         &rows) == Status::kSuccess)
      return 1;
    session->Abort();
    if (!execute("SELECT v FROM tpcc_transport_probe") ||
        rows != std::vector<Row>{Row{Value(int64_t{30})}} ||
        !execute("UPDATE tpcc_transport_probe SET v=0 WHERE id=2") ||
        rows != std::vector<Row>{Row{Value(std::string("affected")),
                                     Value(int64_t{0})}} ||
        !execute("DROP TABLE tpcc_transport_probe"))
      return 1;
    std::cout << "transport_test=passed\n";
    return 0;
  }
  const TpccScale scale = TpccScale::Official(1);
  const TpccNurand nurand = TpccNurand::FromSeed(seed);
  {
    auto session = connect_session();
    if (!session) return 1;
    TpccWorkload check(*session, scale, seed, 0, nurand);
    for (size_t i = 0; i < 5; ++i) {
      const auto result = check.Execute(static_cast<TpccTransactionType>(i));
      if (!result.committed && !result.user_rollback) {
        std::cerr << result.error << '\n';
        return 1;
      }
    }
  }
  std::latch ready(clients), start(1);
  Clock::time_point begin, end;
  std::atomic<bool> failed{false};
  std::vector<std::array<Metrics, 5>> metrics(static_cast<size_t>(clients));
  std::vector<std::thread> workers;
  for (int i = 0; i < clients; ++i) {
    workers.emplace_back([&, i] {
      auto session = connect_session();
      if (!session) failed = true;
      ready.count_down();
      start.wait();
      if (failed) return;
      TpccWorkload workload(*session, scale,
                            seed + static_cast<uint64_t>(i) + 1,
                            (i * scale.Terminals()) / clients, nurand);
      uint64_t trace_sequence = 0;
      while (Clock::now() < end) {
        const bool measured = Clock::now() >= begin;
        const auto t = transaction_only < 0
                           ? workload.NextTransactionType()
                           : static_cast<TpccTransactionType>(transaction_only);
        // One worker only, diagnostics only. Never use traced latency/TPS in
        // comparisons: trace formatting and output alter the generator's pace.
        if (trace_sql && i == 0)
          static_cast<Session*>(session.get())
              ->TraceTransaction(++trace_sequence, t);
        const auto before = Clock::now();
        const auto r = workload.Execute(t);
        const auto after = Clock::now();
        if (!measured) continue;
        auto& m = metrics[static_cast<size_t>(i)][static_cast<size_t>(t)];
        ++m.attempted;
        m.sql += r.sql_statements;
        if (r.committed) {
          ++m.committed;
          m.success_ms.push_back(
              std::chrono::duration<double, std::milli>(after - before)
                  .count());
        }
        m.user_rollback += r.user_rollback ? 1 : 0;
        if (!r.committed && !r.user_rollback && m.error.empty())
          m.error = r.error;
      }
    });
  }
  ready.wait();
  begin = Clock::now() + std::chrono::seconds(warmup);
  end = begin + std::chrono::seconds(seconds);
  start.count_down();
  for (auto& thread : workers) thread.join();
  if (failed) return 1;
  uint64_t total = 0;
  for (size_t t = 0; t < 5; ++t) {
    Metrics merged;
    for (auto& worker : metrics) {
      auto& m = worker[t];
      merged.attempted += m.attempted;
      merged.committed += m.committed;
      merged.user_rollback += m.user_rollback;
      merged.sql += m.sql;
      merged.success_ms.insert(merged.success_ms.end(), m.success_ms.begin(),
                               m.success_ms.end());
      if (merged.error.empty()) merged.error = m.error;
    }
    std::sort(merged.success_ms.begin(), merged.success_ms.end());
    const auto quantile = [&](double p) {
      return merged.success_ms.empty()
                 ? 0.0
                 : merged.success_ms[std::min(
                       merged.success_ms.size() - 1,
                       static_cast<size_t>(std::ceil(
                           p * static_cast<double>(merged.success_ms.size()))) -
                           1)];
    };
    const std::string prefix =
        std::string(ToString(static_cast<TpccTransactionType>(t))) + '.';
    std::cout << prefix << "attempted=" << merged.attempted << '\n'
              << prefix << "committed=" << merged.committed << '\n'
              << prefix << "user_rollback=" << merged.user_rollback << '\n'
              << prefix << "engine_aborts="
              << merged.attempted - merged.committed - merged.user_rollback
              << '\n'
              << prefix << "sql=" << merged.sql << '\n'
              << prefix
              << "tps=" << static_cast<double>(merged.committed) / seconds
              << '\n'
              << prefix << "p50_ms=" << quantile(.5) << '\n'
              << prefix << "p95_ms=" << quantile(.95) << '\n'
              << prefix << "p99_ms=" << quantile(.99) << '\n';
    if (!merged.error.empty())
      std::cout << prefix << "first_error=" << merged.error << '\n';
    total += merged.committed;
  }
  std::cout << "tps=" << static_cast<double>(total) / seconds << '\n';
}
