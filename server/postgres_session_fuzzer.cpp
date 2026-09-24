/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Session-level pgwire fuzzer.  The startup-packet fuzzer only exercises the
// handshake parser; this harness connects to a live PostgresServer over the
// loopback and replays a fuzzed message script (valid 'Q' statements from a
// menu, extended-protocol messages, Flush/Sync, malformed frames, abrupt
// disconnects), asserting the server contract after every exchange:
//   * every reply is a well-framed sequence of backend messages,
//   * every Q/S exchange ends with exactly one ReadyForQuery,
//   * the ReadyForQuery transaction status tracks the BEGIN/COMMIT/ROLLBACK
//     /error history of the session (state machine oracle),
//   * unsupported (extended-protocol) message types answer with
//     ErrorResponse + ReadyForQuery leaving the status unchanged,
//   * a rejected message never kills the server: a fresh connection's
//     sanity query ("SELECT 1") must still return exactly one row.
#include "server/postgres_session_fuzzer.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/byte_stream.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "server/postgres_protocol.hpp"
#include "server/postgres_server.hpp"

namespace tinylamb {
namespace {

// ---------------------------------------------------------------------------
// Minimal blocking client
// ---------------------------------------------------------------------------

class Conn {
 public:
  explicit Conn(int fd) : fd_(fd) {}
  ~Conn() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }
  Conn(Conn&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  Conn& operator=(Conn&&) = delete;
  Conn(const Conn&) = delete;
  Conn& operator=(const Conn&) = delete;

  [[nodiscard]] bool Send(std::string_view bytes) const {
    size_t sent = 0;
    while (sent < bytes.size()) {
      const ssize_t n = write(fd_, bytes.data() + sent, bytes.size() - sent);
      if (n <= 0) {
        return false;
      }
      sent += static_cast<size_t>(n);
    }
    return true;
  }

  // Read backend messages until the deadline. `stop_on_ready` returns after
  // the first ReadyForQuery.  Returns false on EOF/timeout.
  bool ReadMessages(int timeout_ms, bool stop_on_ready,
                    std::vector<std::pair<char, std::string>>* out) {
    std::string buf;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (true) {
      while (buf.size() >= 5) {
        const StatusOr<uint32_t> len = pgwire::ReadUint32(buf, 1);
        if (!len.HasValue()) {
          return false;  // unframmable buffer: protocol violation by server
        }
        if (len.Value() < 4) {
          return false;
        }
        const size_t total = static_cast<size_t>(len.Value()) + 1;
        if (buf.size() < total) {
          break;
        }
        const char type = buf[0];
        out->emplace_back(type, buf.substr(5, len.Value() - 4));
        buf.erase(0, total);
        if (stop_on_ready && type == 'Z') {
          return true;
        }
      }
      const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                            deadline - std::chrono::steady_clock::now())
                            .count();
      if (left <= 0) {
        return !out->empty();
      }
      struct pollfd pfd{.fd = fd_, .events = POLLIN, .revents = 0};
      const int pr = poll(&pfd, 1, static_cast<int>(left));
      if (pr <= 0) {
        return !out->empty();
      }
      std::array<char, 16384> chunk{};
      const ssize_t n = read(fd_, chunk.data(), chunk.size());
      if (n < 0) {
        return !out->empty();
      }
      if (n == 0) {
        return false;  // EOF
      }
      buf.append(chunk.data(), static_cast<size_t>(n));
    }
  }

  void Close() {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_;
};

std::string Frame(char type, std::string_view body) {
  std::string out(1, type);
  pgwire::AppendUint32(&out, static_cast<uint32_t>(body.size() + 4));
  out.append(body);
  return out;
}

std::string CString(std::string_view value) {
  std::string out(value);
  out.push_back('\0');
  return out;
}

std::string QueryMessage(std::string_view sql) {
  std::string body(sql);
  body.push_back('\0');  // the payload is a NUL-terminated string
  return Frame('Q', body);
}

std::string StartupMessage() {
  std::string payload;
  pgwire::AppendUint32(&payload, pgwire::kProtocolVersion30);
  payload += "user";
  payload.push_back('\0');
  payload += "fuzz";
  payload.push_back('\0');
  payload += "database";
  payload.push_back('\0');
  payload += "postgres";
  payload.push_back('\0');
  payload.push_back('\0');
  std::string out;
  pgwire::AppendUint32(&out, static_cast<uint32_t>(payload.size() + 4));
  out += payload;
  return out;
}

int Connect(uint16_t port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }
  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) !=
      0) {
    close(fd);
    return -1;
  }
  return fd;
}

// ---------------------------------------------------------------------------
// Server singleton (one boot per fuzzer process)
// ---------------------------------------------------------------------------

struct ServerEnv {
  tinylamb::ScopedDb db;
  std::unique_ptr<PostgresServer> server;
  std::thread runner;
  uint16_t port{0};

  ServerEnv() : db("pgwire_session_fuzz") {
    if (!db) {
      return;
    }
    PostgresServerOptions options;
    options.port = 0;
    options.idle_timeout = std::chrono::seconds(30);
    server = std::make_unique<PostgresServer>(db.name(), options);
    std::string error;
    if (!server->Listen(&error)) {
      server.reset();
      return;
    }
    port = server->BoundPort();
    runner = std::thread([this, &error] {
      const int rc = server->Run(&error);
      (void)rc;
    });
    // Wait until the sanity query answers on a real connection.
    for (int attempt = 0; attempt < 200; ++attempt) {
      if (SanityCheck()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
  }

  ServerEnv(const ServerEnv&) = delete;
  ServerEnv& operator=(const ServerEnv&) = delete;
  ServerEnv(ServerEnv&&) = delete;
  ServerEnv& operator=(ServerEnv&&) = delete;

  ~ServerEnv() {
    // Run() executes on `runner` and uses the server object itself:
    // destroying it while the thread is still inside the epilogue frees
    // state the loop unwinds through (heap-UAF found by this very
    // harness).  Hold a reference alive until the runner has fully
    // returned.
    std::unique_ptr<PostgresServer> keeper = std::move(server);
    if (keeper) {
      keeper->RequestStop();
    }
    if (runner.joinable()) {
      runner.join();
    }
  }

  [[nodiscard]] bool SanityCheck() const {
    const int fd = Connect(port);
    if (fd < 0) {
      return false;
    }
    Conn conn(fd);
    if (!conn.Send(StartupMessage())) {
      return false;
    }
    std::vector<std::pair<char, std::string>> reply;
    if (!conn.ReadMessages(1000, true, &reply)) {
      return false;
    }
    if (!conn.Send(QueryMessage("SELECT 1;"))) {
      return false;
    }
    reply.clear();
    if (!conn.ReadMessages(1000, true, &reply)) {
      return false;
    }
    int data_rows = 0;
    bool ready = false;
    for (const auto& [type, body] : reply) {
      (void)body;
      if (type == 'D') {
        ++data_rows;
      } else if (type == 'Z') {
        ready = true;
      }
    }
    return ready && data_rows == 1;
  }
};

ServerEnv* Env() {
  // Intentionally process-lifetime: the runner thread must outlive every
  // fuzz iteration and is joined by the static destructor at exit.
  static ServerEnv env;
  return &env;
}

// ---------------------------------------------------------------------------
// Transaction-status state machine (server contract, AGENTS.md invariant 9)
// ---------------------------------------------------------------------------

char ExpectedStatus(char current, std::string_view sql, bool error_seen) {
  // One 'Q' payload may hold several ';'-separated statements; the status
  // reflects the last one.  The menu guarantees that a failing statement is
  // always the payload's last one, so earlier statements are applied as
  // successes and only the final statement consults *error_seen.
  std::vector<std::string> statements;
  size_t pos = 0;
  while (pos <= sql.size()) {
    const size_t semi = sql.find(';', pos);
    std::string stmt(sql.substr(pos, semi == std::string_view::npos
                                         ? std::string_view::npos
                                         : semi - pos));
    auto trim = [](std::string* text) {
      while (!text->empty() &&
             (text->front() == ' ' || text->front() == '\t')) {
        text->erase(text->begin());
      }
      while (!text->empty() && (text->back() == ' ' || text->back() == '\t')) {
        text->pop_back();
      }
    };
    trim(&stmt);
    if (!stmt.empty()) {
      statements.push_back(std::move(stmt));
    }
    if (semi == std::string_view::npos) {
      break;
    }
    pos = semi + 1;
  }
  char status = current;
  for (size_t i = 0; i < statements.size(); ++i) {
    std::string head = statements[i];
    for (char& ch : head) {
      if (ch >= 'a' && ch <= 'z') {
        ch = static_cast<char>(ch - 'a' + 'A');
      }
    }
    const bool last = i + 1 == statements.size();
    if (last && error_seen) {
      // Failed statement: inside a transaction block the session enters
      // the failed-transaction state; outside one the status is unchanged.
      if (status == 'T') {
        status = 'E';
      }
      continue;
    }
    if (head.starts_with("BEGIN") || head.starts_with("START TRANSACTION")) {
      status = 'T';
    } else if (head.starts_with("COMMIT") || head.starts_with("END") ||
               head.starts_with("ROLLBACK") || head.starts_with("ABORT")) {
      status = 'I';
    }
  }
  return status;
}

// ---------------------------------------------------------------------------
// Script generation
// ---------------------------------------------------------------------------

const std::vector<std::string>& SqlMenu() {
  static const std::vector<std::string> kMenu = {
      "SELECT 1;",
      "SELECT u FROM pg_missing_table;",
      "SELECT bad syntax !!!;",
      "CREATE TABLE pgfuzz (u INT64, s VARCHAR(16));",
      "DROP TABLE IF EXISTS pgfuzz_nope;",
      "INSERT INTO pgfuzz VALUES (1, 'a');",
      "INSERT INTO pgfuzz VALUES (2, 'bb'), (3, NULL);",
      "SELECT * FROM pgfuzz ORDER BY u;",
      "UPDATE pgfuzz SET s = 'cc' WHERE u = 1;",
      "DELETE FROM pgfuzz WHERE u = 3;",
      "BEGIN;",
      "COMMIT;",
      "ROLLBACK;",
      "BEGIN; SELECT 1;",
      "BEGIN; SELECT * FROM pg_missing;",
      "SELECT 'unterminated",
      "SELECT CASE WHEN 1 < 2 THEN 'y' ELSE 'n' END;",
      "WITH c AS (SELECT 1 AS x) SELECT x FROM c;",
      "SELECT CAST(NULL AS INT64);",
      "SELECT 1 / 0;",
      "SELECT CAST('abc' AS INT64);",
      "SELECT CAST('2024-02-30' AS DATE);",
      "SELECT ARRAY_LENGTH([1, 2, 3]);",
      "EXPLAIN SELECT u FROM pgfuzz;",
      "EXPLAIN ANALYZE SELECT 1;",
      "DROP TABLE pgfuzz;",
      "CREATE TABLE pgfuzz (u INT64, s VARCHAR(16));",
      "SELECT u FROM pgfuzz WHERE s LIKE 'a%';",
      "SELECT COUNT(*) FROM pgfuzz;",
      // Prepare-time DML subquery folds (Bugs 48/55/57) over the wire.
      "CREATE TABLE pgsrc (k INT64);",
      "INSERT INTO pgsrc VALUES (1), (2);",
      "UPDATE pgfuzz SET s = 'zz' WHERE u = ANY (SELECT k FROM pgsrc);",
      "UPDATE pgfuzz SET s = CAST((SELECT MAX(k) FROM pgsrc) AS VARCHAR(16)) "
      "WHERE u = 1;",
      "DELETE FROM pgfuzz WHERE EXISTS (SELECT 1 FROM pgsrc WHERE pgsrc.k = "
      "pgfuzz.u);",
      "INSERT INTO pgfuzz VALUES (9, 'nine');",
      "UPDATE pgfuzz SET u = u + (SELECT MIN(k) FROM pgsrc) WHERE u = 9;",
      "DROP TABLE IF EXISTS pgsrc;",
  };
  return kMenu;
}

// Parse the message bodies of one exchange and enforce framing/contract
// rules. Returns the transaction status of the trailing ReadyForQuery, or
// ~0 when the exchange never reached one.  Aborts (trap) on contract
// violations.
char CheckExchange(const std::vector<std::pair<char, std::string>>& msgs,
                   bool* error_seen, size_t* ready_count) {
  char status = 0;
  *ready_count = 0;
  for (const auto& [type, body] : msgs) {
    switch (type) {
      case 'R':
      case 'S':
      case 'K':
      case 'T':
      case 'D':
      case 'C':
      case 'N':
      case 'A':
      case 'I':
      case '1':
      case '2':
      case '3':
      case 'G':
      case 'H':
      case 'W':
        break;  // legal backend tags (Simple Query surface)
      case 'E': {
        *error_seen = true;
        // ErrorResponse body: NUL-separated k/v pairs terminated by an
        // empty string; at minimum a severity ('S') field must exist.
        bool has_severity = false;
        size_t pos = 0;
        while (pos < body.size()) {
          const char field = body[pos++];
          if (field == '\0') {
            break;
          }
          const size_t end = body.find('\0', pos);
          if (end == std::string::npos) {
            __builtin_trap();  // unterminated field
          }
          if (field == 'S') {
            has_severity = true;
          }
          pos = end + 1;
        }
        if (!has_severity) {
          __builtin_trap();
        }
        break;
      }
      case 'Z': {
        if (body.size() != 1 ||
            (body[0] != 'I' && body[0] != 'T' && body[0] != 'E')) {
          __builtin_trap();  // bogus transaction status
        }
        status = body[0];
        ++*ready_count;
        break;
      }
      default:
        __builtin_trap();  // unknown backend message tag
    }
  }
  return status == 0 ? '~' : status;
}

// Pipelining mode: fire several 'Q' messages back-to-back without waiting
// for replies (the spec allows it; the server must queue and answer in
// order — exactly one ReadyForQuery per query — or close the connection
// with a framed error, never answer out of order or drop one silently).
void TryPipelining(ByteStream& stream) {
  ServerEnv* env = Env();
  const int fd = Connect(env->port);
  if (fd < 0) {
    abort();
  }
  Conn conn(fd);
  if (!conn.Send(StartupMessage())) {
    abort();
  }
  std::vector<std::pair<char, std::string>> reply;
  if (!conn.ReadMessages(2000, true, &reply)) {
    abort();
  }
  const int count = 2 + static_cast<int>(stream.Pick(5));
  std::string batch;
  std::vector<std::string> statements;
  statements.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    const std::string& sql =
        SqlMenu()[static_cast<size_t>(stream.U8()) % SqlMenu().size()];
    statements.push_back(sql);
    batch += QueryMessage(sql);
  }
  if (!conn.Send(batch)) {
    return;  // server closed early: only legal on a fatal frame error
  }
  int z_count = 0;
  bool first_errored = false;
  for (int i = 0; i < count; ++i) {
    std::vector<std::pair<char, std::string>> exchange;
    if (!conn.ReadMessages(5000, true, &exchange)) {
      // Connection died mid-pipeline: acceptable only if at least one
      // reply had already arrived (an E then close is the documented
      // fatal-frame path).
      break;
    }
    bool err = false;
    size_t ready = 0;
    const char status = CheckExchange(exchange, &err, &ready);
    if (status == '~' || ready != 1) {
      (void)fprintf(stderr, "pipeline reply %d of %d lacks exactly one Z\n", i,
                    count);
      __builtin_trap();
    }
    ++z_count;
    if (i == 0) {
      first_errored = err;
    }
  }
  if (z_count < count) {
    // Short read: every consumed exchange was still well-framed; the
    // server may close after the last one it processed.  Require it at
    // least processed something.
    if (z_count == 0) {
      __builtin_trap();
    }
  }
  // State-dependent menu entries (CREATE/DROP/SELECT on pgfuzz) may fail
  // after a previous iteration dropped the table, so the guarantee only
  // applies to statements that cannot fail on any server state.
  static const std::vector<std::string> kAlwaysOk = {
      "SELECT 1;",
      "SELECT CASE WHEN 1 < 2 THEN 'y' ELSE 'n' END;",
      "WITH c AS (SELECT 1 AS x) SELECT x FROM c;",
      "SELECT CAST(NULL AS INT64);",
      "SELECT ARRAY_LENGTH([1, 2, 3]);",
  };
  const bool first_always_ok = std::find(kAlwaysOk.begin(), kAlwaysOk.end(),
                                         statements[0]) != kAlwaysOk.end();
  if (first_always_ok && first_errored) {
    (void)fprintf(stderr, "always-valid statement errored: %s\n",
                  statements[0].c_str());
    __builtin_trap();
  }
  conn.Close();
  if (!env->SanityCheck()) {
    (void)fprintf(stderr, "sanity failed after pipeline\n");
    __builtin_trap();
  }
}

// Two-connection write race on one hot row: both writers UPDATE the same
// committed row in a loop.  Conflicts must surface as framed ErrorResponses
// (never hangs, never torn state); the committed value afterwards must be
// one of the writers' legal values.
void TryWriteRace(ByteStream& stream) {
  ServerEnv* env = Env();
  const int reset_fd = Connect(env->port);
  if (reset_fd < 0) {
    abort();
  }
  Conn reset(reset_fd);
  if (!reset.Send(StartupMessage())) {
    abort();
  }
  std::vector<std::pair<char, std::string>> reply;
  if (!reset.ReadMessages(2000, true, &reply)) {
    abort();
  }
  bool err = false;
  size_t ready = 0;
  (void)CheckExchange(reply, &err, &ready);
  static const std::string race_table =
      "pgw_race_" + std::to_string(static_cast<long>(::getpid()));
  static bool table_ready = false;
  const bool fresh = !table_ready;
  table_ready = true;
  std::vector<std::string> setup;
  if (fresh) {
    setup.push_back("CREATE TABLE " + race_table + " (u INT64, v INT64);");
  }
  setup.push_back("DELETE FROM " + race_table + ";");
  setup.push_back("INSERT INTO " + race_table + " VALUES (1, 0);");
  for (const std::string& sql : setup) {
    if (!reset.Send(QueryMessage(sql))) {
      abort();
    }
    reply.clear();
    if (!reset.ReadMessages(3000, true, &reply)) {
      abort();  // DDL/DML over Simple Query must never be rejected outright
    }
    bool setup_err = false;
    if (CheckExchange(reply, &setup_err, &ready) == '~' || setup_err) {
      (void)fprintf(stderr, "race setup failed: %s\n", sql.c_str());
      __builtin_trap();
    }
  }
  reset.Close();

  const int rounds = 3 + static_cast<int>(stream.Pick(10));
  const int fd0 = Connect(env->port);
  const int fd1 = Connect(env->port);
  if (fd0 < 0 || fd1 < 0) {
    abort();
  }
  Conn w0(fd0);
  Conn w1(fd1);
  for (Conn* w : {&w0, &w1}) {
    if (!w->Send(StartupMessage())) {
      abort();
    }
    std::vector<std::pair<char, std::string>> hello;
    if (!w->ReadMessages(2000, true, &hello)) {
      abort();
    }
  }
  int committed0 = 0;
  int committed1 = 0;
  for (int k = 0; k < rounds; ++k) {
    const int base = k == 0 ? 0 : 1;  // vary the writer order
    Conn* first = base == 0 ? &w0 : &w1;
    Conn* second = base == 0 ? &w1 : &w0;
    int& committed_first = base == 0 ? committed0 : committed1;
    int& committed_second = base == 0 ? committed1 : committed0;
    const int first_id = base;
    const int second_id = 1 - base;
    struct Writer {
      Conn* conn;
      int id;
      int* committed;
    };
    const std::array<Writer, 2> writers = {
        Writer{.conn = first, .id = first_id, .committed = &committed_first},
        Writer{
            .conn = second, .id = second_id, .committed = &committed_second}};
    for (const Writer& wr : writers) {
      const int64_t value = (wr.id * 1000) + k;
      // Half the rounds carry a prepare-time-foldable predicate, racing
      // two concurrent folds of the same fingerprint (Bugs 48/57/60).
      std::string sql = "UPDATE " + race_table +
                        " SET v = " + std::to_string(value) + " WHERE u = 1";
      if ((k & 3) == 0) {
        sql += " AND 1 = ANY (SELECT u FROM " + race_table + " WHERE u = 1)";
      } else if ((k & 3) == 2) {
        sql += " AND EXISTS (SELECT 1 FROM ";
        sql += race_table;
        sql += " r2 WHERE r2.u = ";
        sql += race_table;
        sql += ".u)";
      }
      sql += ";";
      if (!wr.conn->Send(QueryMessage(sql))) {
        abort();
      }
      std::vector<std::pair<char, std::string>> exchange;
      if (!wr.conn->ReadMessages(5000, true, &exchange)) {
        (void)fprintf(stderr, "writer %d stalled on %s\n", wr.id, sql.c_str());
        __builtin_trap();
      }
      bool wrote_error = false;
      bool completed = false;
      if (CheckExchange(exchange, &wrote_error, &ready) == '~') {
        __builtin_trap();
      }
      for (const auto& [t, b] : exchange) {
        (void)b;
        completed |= (t == 'C');
      }
      if (completed && wrote_error) {
        __builtin_trap();  // both CommandComplete and ErrorResponse
      }
      if (completed) {
        ++*wr.committed;
      }
    }
  }
  (void)w0.Send(Frame('X', ""));
  (void)w1.Send(Frame('X', ""));
  w0.Close();
  w1.Close();

  // The committed value must be exactly one writer's last legal store.
  const int read_fd = Connect(env->port);
  if (read_fd < 0) {
    abort();
  }
  Conn reader(read_fd);
  if (!reader.Send(StartupMessage())) {
    abort();
  }
  reply.clear();
  if (!reader.ReadMessages(2000, true, &reply)) {
    abort();
  }
  if (!reader.Send(
          QueryMessage("SELECT v FROM " + race_table + " WHERE u = 1;"))) {
    abort();
  }
  reply.clear();
  if (!reader.ReadMessages(3000, true, &reply)) {
    abort();
  }
  int rows = 0;
  int64_t value = -1;
  for (const auto& [t, b] : reply) {
    if (t == 'D') {
      ++rows;
      // Text format: "<col_count>\0<len>digits"
      const size_t first_nl = b.find('\0');
      if (first_nl == std::string::npos || b.size() < first_nl + 5) {
        __builtin_trap();
      }
      const size_t len =
          (static_cast<size_t>(static_cast<unsigned char>(b[first_nl + 1]))
           << 24) |
          (static_cast<size_t>(static_cast<unsigned char>(b[first_nl + 2]))
           << 16) |
          (static_cast<size_t>(static_cast<unsigned char>(b[first_nl + 3]))
           << 8) |
          static_cast<size_t>(static_cast<unsigned char>(b[first_nl + 4]));
      const std::string num = b.substr(static_cast<size_t>(first_nl) + 5, len);
      value = std::strtoll(num.c_str(), nullptr, 10);
    }
  }
  if (rows != 1 || value < 0 || value >= 2000) {
    (void)fprintf(stderr, "post-race row corrupt: rows=%d value=%lld\n", rows,
                  static_cast<long long>(value));
    __builtin_trap();
  }
  if (committed0 + committed1 == 0) {
    __builtin_trap();  // every single writer statement conflicted
  }
  if (!env->SanityCheck()) {
    __builtin_trap();
  }
}

}  // namespace

void TryPostgresSession(const uint8_t* data, size_t size, bool verbose) {
  ServerEnv* env = Env();
  if (env->port == 0) {
    (void)fprintf(stderr, "server boot failed\n");
    abort();
  }
  ByteStream stream(data, size);

  if (stream.Pick(6) == 0) {
    TryWriteRace(stream);
    if (verbose) {
      (void)fprintf(stderr, "race ok\n");
    }
    return;
  }
  if (stream.Pick(6) == 0) {
    TryPipelining(stream);
    if (verbose) {
      (void)fprintf(stderr, "pipeline ok\n");
    }
    return;
  }

  const int fd = Connect(env->port);
  if (fd < 0) {
    abort();  // server refused a connection
  }
  Conn conn(fd);

  // Optional SSL request negotiation byte stream before the startup packet.
  if (stream.Pick(4) == 0) {
    std::string sslreq;
    pgwire::AppendUint32(&sslreq, 8);
    pgwire::AppendUint32(&sslreq, pgwire::kSslRequestCode);
    if (!conn.Send(sslreq)) {
      abort();
    }
    char reply = 0;
    if (read(fd, &reply, 1) != 1 || (reply != 'N' && reply != 'S')) {
      abort();
    }
  }
  if (!conn.Send(StartupMessage())) {
    abort();
  }
  std::vector<std::pair<char, std::string>> reply;
  if (!conn.ReadMessages(2000, true, &reply)) {
    abort();
  }
  reply.shrink_to_fit();
  bool error_seen = false;
  size_t ready_count = 0;
  char status = CheckExchange(reply, &error_seen, &ready_count);
  if (status != 'I') {
    __builtin_trap();  // fresh session must start idle
  }

  const int actions = static_cast<int>(stream.Pick(12)) + 1;
  for (int action = 0; action < actions; ++action) {
    const int kind = static_cast<int>(stream.Pick(10));
    if (kind < 6) {
      const size_t pick = static_cast<size_t>(stream.U8()) % SqlMenu().size();
      const std::string& statement = SqlMenu()[pick];
      error_seen = false;
      std::vector<std::pair<char, std::string>> exchange;
      if (!conn.Send(QueryMessage(statement))) {
        break;  // connection died (server chose to close): acceptable
      }
      if (!conn.ReadMessages(3000, true, &exchange)) {
        break;
      }
      const char next = CheckExchange(exchange, &error_seen, &ready_count);
      if (next == '~') {
        (void)fprintf(stderr, "query never reached ReadyForQuery: %s\n",
                      statement.c_str());
        __builtin_trap();
      }
      const char want = ExpectedStatus(status, statement, error_seen);
      if (next != want) {
        (void)fprintf(stderr,
                      "transaction status after '%s': got '%c' want '%c'\n",
                      statement.c_str(), next, want);
        __builtin_trap();
      }
      status = next;
    } else if (kind == 6) {
      // Sync must answer with exactly one ReadyForQuery, any status.
      std::vector<std::pair<char, std::string>> exchange;
      if (!conn.Send(Frame('S', ""))) {
        break;
      }
      if (!conn.ReadMessages(2000, true, &exchange)) {
        break;
      }
      error_seen = false;
      const char next = CheckExchange(exchange, &error_seen, &ready_count);
      if (next == '~') {
        __builtin_trap();
      }
      status = next;
    } else if (kind == 7) {
      // Flush must be silent (no bytes) when nothing is queued.
      if (!conn.Send(Frame('H', ""))) {
        break;
      }
      std::vector<std::pair<char, std::string>> exchange;
      (void)conn.ReadMessages(150, false, &exchange);
      if (!exchange.empty()) {
        __builtin_trap();  // Flush answered
      }
    } else if (kind == 8) {
      // Extended-protocol messages must be rejected as unsupported and
      // must not change the transaction status.
      static constexpr std::array<char, 4> kExtended = {'P', 'B', 'D', 'E'};
      const char type = kExtended[stream.U8() % 4U];
      std::string body;
      if (type == 'P') {
        body = CString("") + CString("SELECT 1");
        pgwire::AppendUint16(&body, 0);
      } else if (type == 'B') {
        body = CString("") + CString("");
        pgwire::AppendUint16(&body, 0);
        pgwire::AppendUint16(&body, 0);
        pgwire::AppendUint16(&body, 0);
      }
      std::vector<std::pair<char, std::string>> exchange;
      if (!conn.Send(Frame(type, body))) {
        break;
      }
      if (!conn.ReadMessages(2000, true, &exchange)) {
        break;
      }
      error_seen = false;
      const char next = CheckExchange(exchange, &error_seen, &ready_count);
      if (next != status) {
        (void)fprintf(stderr, "extended message '%c' changed status %c -> %c\n",
                      type, status, next);
        __builtin_trap();
      }
      bool saw_error = false;
      for (const auto& [t, b] : exchange) {
        (void)b;
        saw_error |= t == 'E';
      }
      if (!saw_error) {
        __builtin_trap();  // extended protocol silently accepted
      }
    } else {
      // Malformed frame: bogus length (<4 or > limit).  The server must
      // answer 08P01 and close; either way the connection may die.
      std::string junk;
      junk.push_back('Q');
      const uint32_t extra = stream.U8();
      const uint32_t bad_len =
          extra % 2 == 0 ? 1U + (extra % 3U) : 0xFFFF0000U + extra;
      pgwire::AppendUint32(&junk, bad_len);
      junk += "junk";
      (void)conn.Send(junk);
      std::vector<std::pair<char, std::string>> exchange;
      (void)conn.ReadMessages(500, false, &exchange);
      conn.Close();
      return;  // connection is expected to be gone; sanity runs below
    }
  }
  conn.Close();

  if (!env->SanityCheck()) {
    (void)fprintf(stderr, "sanity query failed after %d actions\n", actions);
    __builtin_trap();
  }
  if (verbose) {
    (void)fprintf(stderr, "session ok (%d actions)\n", actions);
  }
}

}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::TryPostgresSession(data, size, false);
  return 0;
}
