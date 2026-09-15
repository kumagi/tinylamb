/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_plancache_fuzzer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

constexpr const char* kTestHeader = "-- tinylamb-plancache-test v1";
constexpr int64_t kNullRepr = INT64_MIN;

class Gen {
 public:
  explicit Gen(std::mt19937& rng) : rng_(rng) {}
  int Pick(int lo, int hi) {
    return std::uniform_int_distribution<int>(lo, hi)(rng_);
  }
  bool Chance(int percent) { return Pick(1, 100) <= percent; }

 private:
  std::mt19937& rng_;
};

// Mirror row of t(u, k, a).
struct MRow {
  int64_t u{0};
  int64_t k{0};
  int64_t a{kNullRepr};
};

std::string Fmt(const Value& v) {
  if (v.IsNull()) {
    return "NULL";
  }
  return "|" + v.AsString() + "|";
}
std::string FmtInt(int64_t v) {
  return v == kNullRepr ? "NULL" : "|" + std::to_string(v) + "|";
}

std::optional<std::vector<std::vector<Value>>> RunQuery(Database& db,
                                                        TransactionContext& ctx,
                                                        const std::string& sql,
                                                        std::string* error) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    *error = engine.LastError();
    return std::nullopt;
  }
  std::vector<std::vector<Value>> rows;
  Row row;
  // Row::Size() is the serialized byte size and label counts can diverge
  // from row width on malformed inputs; iterate the row's own cells.
  while (result.Value().Next(&row)) {
    std::vector<Value> cells;
    cells.reserve(row.values_.size());
    for (size_t i = 0; i < row.values_.size(); ++i) {
      cells.push_back(row[i]);
    }
    rows.push_back(std::move(cells));
  }
  return rows;
}

bool RunSql(Database& db, TransactionContext& ctx, const std::string& sql) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    return false;
  }
  // QueryResults are lazy: drain or the DML never lands.
  result.Value().Drain();
  return true;
}

std::vector<std::string> FormatRows(
    const std::vector<std::vector<Value>>& rows) {
  std::vector<std::string> out;
  for (const auto& cells : rows) {
    std::string row;
    for (const Value& v : cells) {
      row += Fmt(v) + ",";
    }
    out.push_back(row);
  }
  std::sort(out.begin(), out.end());
  return out;
}

// The mirror's answer for each query over the given rows.
std::vector<std::string> MirrorAnswers(
    const std::vector<MRow>& rows, const std::vector<std::string>& queries) {
  std::vector<std::string> out;
  for (const std::string& q : queries) {
    std::vector<MRow> selected;
    for (const MRow& r : rows) {
      bool keep = true;
      // The generated queries only use these predicate shapes.
      if (q.find("u < ") != std::string::npos) {
        const int64_t bound = std::stoll(q.substr(q.find("u < ") + 4));
        keep = keep && r.u < bound;
      }
      if (q.find("k = ") != std::string::npos) {
        const size_t pos = q.find("k = ") + 4;
        const int64_t val = std::stoll(
            q.substr(pos, q.find_first_not_of("-0123456789", pos) - pos));
        keep = keep && r.k == val;
      }
      if (q.find("a IS NULL") != std::string::npos) {
        keep = keep && r.a == kNullRepr;
      }
      if (q.find("a > ") != std::string::npos) {
        const size_t pos = q.find("a > ") + 4;
        const int64_t val = std::stoll(
            q.substr(pos, q.find_first_not_of("-0123456789", pos) - pos));
        keep = keep && r.a != kNullRepr && r.a > val;
      }
      if (keep) {
        selected.push_back(r);
      }
    }
    if (q.find("COUNT(*)") != std::string::npos) {
      out.push_back(FmtInt(static_cast<int64_t>(selected.size())) + ",");
      continue;
    }
    for (const MRow& r : selected) {
      std::string row;
      if (q.find("SELECT u, a") != std::string::npos) {
        row = FmtInt(r.u) + "," + FmtInt(r.a) + ",";
      } else {
        row = FmtInt(r.u) + ",";
      }
      out.push_back(row);
    }
  }
  std::sort(out.begin(), out.end());
  return out;
}

constexpr const char* kDdl = "CREATE TABLE t (u INT64, k INT64, a INT64);";

// Executes the setup script on a throwaway ScopedDb database (the scope
// deletes the .db/.log files when the iteration ends).  Returns false if the
// engine rejected anything (caller skips the iteration).
bool BuildDatabase(const std::vector<std::string>& setup, ScopedDb* out) {
  if (!*out) {
    return false;
  }
  TransactionContext ctx = (*out)->BeginContext();
  for (const std::string& sql : setup) {
    if (!RunSql(**out, ctx, sql)) {
      return false;
    }
  }
  ctx.txn_.PreCommit();
  return true;
}

std::vector<std::vector<Value>> ExecuteAll(
    Database& db, const std::vector<std::string>& queries, bool* ok) {
  TransactionContext ctx = db.BeginContext();
  std::vector<std::vector<Value>> all;
  for (const std::string& q : queries) {
    std::string error;
    auto rows = RunQuery(db, ctx, q, &error);
    if (!rows.has_value()) {
      *ok = false;
      return all;
    }
    all.insert(all.end(), rows->begin(), rows->end());
  }
  *ok = true;
  return all;
}

}  // namespace

std::string RunPlanCacheIteration(std::mt19937& rng, bool verbose,
                                  PlanCacheStats* stats,
                                  PlanCacheTrace* trace) {
  Gen g(rng);
  PlanCacheTrace local;
  PlanCacheTrace& t = (trace != nullptr) ? *trace : local;

  t.setup.emplace_back(kDdl);
  std::vector<MRow> mirror;
  const int row_count = g.Pick(6, 14);
  for (int i = 0; i < row_count; ++i) {
    MRow r;
    r.u = i;
    r.k = g.Pick(0, 3);
    r.a = g.Chance(25) ? kNullRepr : g.Pick(-3, 3);
    mirror.push_back(r);
    t.setup.push_back("INSERT INTO t VALUES (" + std::to_string(r.u) + ", " +
                      std::to_string(r.k) + ", " +
                      (r.a == kNullRepr ? "NULL" : std::to_string(r.a)) + ");");
  }

  // Query set: identical text in all phases so the cache hits.
  const int u_bound = g.Pick(3, 10);
  const int k_val = g.Pick(0, 3);
  const int a_bound = g.Pick(-2, 2);
  t.queries = {
      "SELECT u FROM t WHERE u < " + std::to_string(u_bound) + ";",
      "SELECT u, a FROM t WHERE k = " + std::to_string(k_val) + ";",
      "SELECT COUNT(*) FROM t WHERE a IS NULL;",
      "SELECT u FROM t WHERE k = " + std::to_string(k_val) + " AND a > " +
          std::to_string(a_bound) + ";",
  };

  // Mutations drift the data after the cache is warm.
  const int mut_count = g.Pick(1, 4);
  for (int i = 0; i < mut_count; ++i) {
    switch (g.Pick(0, 2)) {
      case 0: {
        MRow r;
        r.u = 100 + i;
        r.k = g.Pick(0, 3);
        r.a = g.Chance(30) ? kNullRepr : g.Pick(-3, 3);
        mirror.push_back(r);
        t.mutations.push_back(
            "INSERT INTO t VALUES (" + std::to_string(r.u) + ", " +
            std::to_string(r.k) + ", " +
            (r.a == kNullRepr ? "NULL" : std::to_string(r.a)) + ");");
        break;
      }
      case 1: {
        const int64_t u_target = g.Pick(0, row_count - 1);
        for (MRow& r : mirror) {
          if (r.u == u_target) {
            r.a = g.Chance(30) ? kNullRepr : g.Pick(-3, 3);
            t.mutations.push_back(
                "UPDATE t SET a = " +
                std::string(r.a == kNullRepr ? "NULL" : std::to_string(r.a)) +
                " WHERE u = " + std::to_string(u_target) + ";");
          }
        }
        break;
      }
      default: {
        const int64_t u_target = 100 + g.Pick(0, mut_count - 1);
        auto it =
            std::remove_if(mirror.begin(), mirror.end(),
                           [&](const MRow& r) { return r.u == u_target; });
        if (it != mirror.end()) {
          mirror.erase(it, mirror.end());
          t.mutations.push_back(
              "DELETE FROM t WHERE u = " + std::to_string(u_target) + ";");
        }
        break;
      }
    }
  }

  // ---- Phase 1+2: fresh DB, miss then hit. ----
  ScopedDb db_a("sql_plancache_fuzz");
  if (!BuildDatabase(t.setup, &db_a)) {
    if (verbose) {
      std::cerr << "[plancache_fuzz][skip-setup]\n";
    }
    return "";
  }
  bool ok = false;
  const std::vector<std::vector<Value>> r1 = ExecuteAll(*db_a, t.queries, &ok);
  if (!ok) {
    if (verbose) {
      std::cerr << "[plancache_fuzz][skip-query]\n";
    }
    return "";
  }
  const std::vector<std::vector<Value>> r2 = ExecuteAll(*db_a, t.queries, &ok);
  if (!ok) {
    return "";
  }

  // ---- Mutate (cache stays warm). ----
  {
    TransactionContext ctx = db_a->BeginContext();
    for (const std::string& m : t.mutations) {
      if (!RunSql(*db_a, ctx, m)) {
        return "mutation rejected on db_a: " + m;
      }
    }
    ctx.txn_.PreCommit();
  }
  const std::vector<std::vector<Value>> r3 = ExecuteAll(*db_a, t.queries, &ok);
  if (!ok) {
    return "";
  }

  // ---- Phase 4: fresh database, replayed script (all misses). ----
  std::vector<std::string> setup_b = t.setup;
  setup_b.insert(setup_b.end(), t.mutations.begin(), t.mutations.end());
  ScopedDb db_b("sql_plancache_fuzz");
  if (!BuildDatabase(setup_b, &db_b)) {
    return "";
  }
  const std::vector<std::vector<Value>> r4 = ExecuteAll(*db_b, t.queries, &ok);
  if (!ok) {
    return "";
  }

  const std::vector<std::string> m1 = FormatRows(r1);
  const std::vector<std::string> m2 = FormatRows(r2);
  const std::vector<std::string> m3 = FormatRows(r3);
  const std::vector<std::string> m4 = FormatRows(r4);
  const std::vector<std::string> expected = MirrorAnswers(mirror, t.queries);

  if (stats != nullptr) {
    stats->queries_compared = static_cast<int>(t.queries.size()) * 4;
  }

  auto diff = [&](const std::vector<std::string>& a,
                  const std::vector<std::string>& b) {
    for (size_t i = 0; i < std::max(a.size(), b.size()); ++i) {
      const std::string ea = i < a.size() ? a[i] : "<missing>";
      const std::string eb = i < b.size() ? b[i] : "<missing>";
      if (ea != eb) {
        std::string first_diff;
        first_diff += "\n    first diff: [";
        first_diff += ea;
        first_diff += "] vs [";
        first_diff += eb;
        first_diff += "]";
        return first_diff;
      }
    }
    return std::string();
  };

  std::string report;
  if (m1 != m2) {
    report += "[PLANCACHE MISMATCH] miss-vs-hit diverged" + diff(m1, m2) + "\n";
  }
  if (m3 != m4) {
    report += "[PLANCACHE MISMATCH] drifted-hit vs fresh-miss diverged" +
              diff(m3, m4) + "\n";
  }
  if (m3 != expected) {
    report += "[PLANCACHE MISMATCH] drifted results differ from mirror" +
              diff(m3, expected) + "\n";
  }
  if (!report.empty()) {
    for (const auto& querie : t.queries) {
      report += "  query: " + querie + "\n";
    }
    t.expected = expected;
    return report;
  }
  t.expected = expected;
  return "";
}

std::string ReplayPlanCacheTrace(const PlanCacheTrace& trace, bool verbose) {
  ScopedDb db_a("sql_plancache_replay");
  if (!BuildDatabase(trace.setup, &db_a)) {
    return "replay setup failed";
  }
  bool ok = false;
  const std::vector<std::vector<Value>> r1 =
      ExecuteAll(*db_a, trace.queries, &ok);
  if (!ok) {
    return "replay query failed";
  }
  const std::vector<std::vector<Value>> r2 =
      ExecuteAll(*db_a, trace.queries, &ok);
  {
    TransactionContext ctx = db_a->BeginContext();
    for (const std::string& m : trace.mutations) {
      if (!RunSql(*db_a, ctx, m)) {
        return "replay mutation failed: " + m;
      }
    }
    ctx.txn_.PreCommit();
  }
  const std::vector<std::vector<Value>> r3 =
      ExecuteAll(*db_a, trace.queries, &ok);
  std::vector<std::string> setup_b = trace.setup;
  setup_b.insert(setup_b.end(), trace.mutations.begin(), trace.mutations.end());
  ScopedDb db_b("sql_plancache_replay");
  if (!BuildDatabase(setup_b, &db_b)) {
    return "replay db_b setup failed";
  }
  const std::vector<std::vector<Value>> r4 =
      ExecuteAll(*db_b, trace.queries, &ok);

  const std::vector<std::string> m1 = FormatRows(r1);
  const std::vector<std::string> m2 = FormatRows(r2);
  const std::vector<std::string> m3 = FormatRows(r3);
  const std::vector<std::string> m4 = FormatRows(r4);
  std::vector<std::string> expected = trace.expected;
  std::sort(expected.begin(), expected.end());
  if (m1 != m2 || m3 != m4 || m3 != expected) {
    std::string report = "[PLANCACHE REPLAY MISMATCH]\n";
    report +=
        "  miss-vs-hit equal: " + std::string(m1 == m2 ? "yes" : "NO") + "\n";
    report +=
        "  drift-hit-vs-fresh equal: " + std::string(m3 == m4 ? "yes" : "NO") +
        "\n";
    report +=
        "  mirror match: " + std::string(m3 == expected ? "yes" : "NO") + "\n";
    return report;
  }
  if (verbose) {
    std::cerr << "[plancache_fuzz][replay-ok]\n";
  }
  return "";
}

std::string SerializePlanCacheTest(uint64_t seed, const PlanCacheTrace& trace,
                                   const std::string& failure_summary) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  for (const std::string& sql : trace.setup) {
    out += "-- setup: " + sql + "\n";
  }
  for (const std::string& sql : trace.queries) {
    out += "-- query: " + sql + "\n";
  }
  for (const std::string& sql : trace.mutations) {
    out += "-- mutation: " + sql + "\n";
  }
  for (const std::string& row : trace.expected) {
    out += "-- expect: " + row + "\n";
  }
  if (!failure_summary.empty()) {
    out += "-- failure: " + failure_summary + "\n";
  }
  return out;
}

bool ParsePlanCacheTest(std::string_view text, uint64_t* seed,
                        PlanCacheTrace* trace, std::string* failure_summary) {
  *seed = 0;
  *trace = PlanCacheTrace{};
  failure_summary->clear();
  bool saw_header = false;
  size_t pos = 0;
  while (pos <= text.size()) {
    const size_t eol = text.find('\n', pos);
    std::string line(text.substr(pos, eol == std::string_view::npos
                                          ? std::string_view::npos
                                          : eol - pos));
    pos = (eol == std::string_view::npos) ? text.size() + 1 : eol + 1;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    if (line.starts_with("-- tinylamb-plancache-test")) {
      saw_header = true;
      continue;
    }
    auto consume = [&](const std::string& tag, std::string* value) {
      if (!line.starts_with(tag)) {
        return false;
      }
      *value = line.substr(tag.size());
      return true;
    };
    std::string value;
    if (consume("-- seed: ", &value)) {
      *seed = std::stoull(value);
    } else if (consume("-- setup: ", &value)) {
      trace->setup.push_back(value);
    } else if (consume("-- query: ", &value)) {
      trace->queries.push_back(value);
    } else if (consume("-- mutation: ", &value)) {
      trace->mutations.push_back(value);
    } else if (consume("-- expect: ", &value)) {
      trace->expected.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;
    }
  }
  return saw_header && !trace->setup.empty();
}

void PlanCacheFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x5deece66d1101ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
  PlanCacheTrace trace;
  std::string report = RunPlanCacheIteration(rng, verbose, nullptr, &trace);
  if (report.empty()) {
    return;
  }
  const std::string text = SerializePlanCacheTest(
      seed, trace, "auto-generated by sql_plancache_fuzzer");
  uint64_t parsed_seed = 0;
  PlanCacheTrace parsed;
  std::string summary;
  std::string verify;
  if (!ParsePlanCacheTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized plancache test does not parse";
  } else {
    verify = ReplayPlanCacheTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized plancache replay holds (flaky)";
    }
  }
  const std::string path =
      "sql_plancache_fuzz-repro-" + std::to_string(seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();
  std::cerr << "[sql_plancache_fuzz] seed=" << seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
