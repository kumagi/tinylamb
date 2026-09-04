/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_oracle_fuzzer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

// Tiny deterministic RNG wrapper over the caller's engine.
class Gen {
 public:
  explicit Gen(std::mt19937& rng) : rng_(rng) {}
  int Pick(int lo, int hi) {
    return std::uniform_int_distribution<int>(lo, hi)(rng_);
  }
  bool Chance(int percent) { return Pick(1, 100) <= percent; }
  const std::string& PickFrom(const std::vector<std::string>& v) {
    return v[static_cast<size_t>(Pick(0, static_cast<int>(v.size()) - 1))];
  }

 private:
  std::mt19937& rng_;
};

const std::vector<std::string>& IntColumns() {
  static const std::vector<std::string> kCols = {"a", "b"};
  return kCols;
}

std::string IntLeafPredicate(Gen& g) {
  static const std::vector<std::string> kOps = {"=", "!=", "<", "<=", ">",
                                                ">="};
  const std::string& lhs = g.PickFrom(IntColumns());
  switch (g.Pick(0, 3)) {
    case 0:
      return "(" + lhs + " " + g.PickFrom(kOps) + " " +
             std::to_string(g.Pick(-3, 3)) + ")";
    case 1:
      return "(" + lhs + " " + g.PickFrom(kOps) + " " +
             g.PickFrom(IntColumns()) + ")";
    case 2:
      return "(" + lhs + " IS NULL)";
    default:
      return "(" + lhs + " IS NOT NULL)";
  }
}

std::string BoolLeafPredicate(Gen& g) {
  switch (g.Pick(0, 4)) {
    case 0:
      return "flag";
    case 1:
      return "(NOT flag)";
    case 2:
      return "(flag = TRUE)";
    case 3:
      return "(flag IS NULL)";
    default:
      return "(flag IS NOT NULL)";
  }
}

std::string StringLeafPredicate(Gen& g) {
  switch (g.Pick(0, 3)) {
    case 0:
      return "(s = 'a')";
    case 1:
      return "(s != 'bb')";
    case 2:
      return "(s IS NULL)";
    default:
      return "(s IS NOT NULL)";
  }
}

std::string GenPredicate(Gen& g, int depth) {
  if (depth <= 0 || g.Chance(40)) {
    switch (g.Pick(0, 2)) {
      case 0:
        return IntLeafPredicate(g);
      case 1:
        return BoolLeafPredicate(g);
      default:
        return StringLeafPredicate(g);
    }
  }
  const std::string left = GenPredicate(g, depth - 1);
  switch (g.Pick(0, 2)) {
    case 0:
      return "(" + left + " AND " + GenPredicate(g, depth - 1) + ")";
    case 1:
      return "(" + left + " OR " + GenPredicate(g, depth - 1) + ")";
    default:
      return "(NOT " + left + ")";
  }
}

std::string GenProjection(Gen& g) {
  switch (g.Pick(0, 3)) {
    case 0:
      return "*";
    case 1:
      return "a";
    case 2:
      return "a, b";
    default:
      return "flag, s";
  }
}

std::string GenInsertValues(Gen& g) {
  const int a = g.Pick(-3, 3);
  const int b = g.Pick(-2, 2);
  std::string flag;
  switch (g.Pick(0, 2)) {
    case 0:
      flag = "TRUE";
      break;
    case 1:
      flag = "FALSE";
      break;
    default:
      flag = "NULL";
      break;
  }
  std::string s;
  switch (g.Pick(0, 2)) {
    case 0:
      s = "'a'";
      break;
    case 1:
      s = "'bb'";
      break;
    default:
      s = "NULL";
      break;
  }
  return "(" + std::to_string(a) + ", " + std::to_string(b) + ", " + flag +
         ", " + s + ")";
}

// Executes a SELECT and returns its rows.  nullopt means the engine rejected
// the query (skip the oracle).
std::optional<std::vector<Row>> RunRows(Database& db, TransactionContext& ctx,
                                        const std::string& sql,
                                        std::string* error) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    *error = engine.LastError();
    return std::nullopt;
  }
  std::vector<Row> rows;
  Row row;
  while (result.Value().Next(&row)) {
    rows.push_back(row);
  }
  return rows;
}

std::vector<std::string> SerializeSorted(const std::vector<Row>& rows) {
  std::vector<std::string> serialized;
  serialized.reserve(rows.size());
  for (const Row& r : rows) {
    serialized.push_back(r.ToString());
  }
  std::sort(serialized.begin(), serialized.end());
  return serialized;
}

// Runs `sql` expected to yield a single scalar row; NULL counts as 0.
std::optional<std::string> RunScalar(Database& db, TransactionContext& ctx,
                                     const std::string& sql,
                                     std::string* error) {
  auto rows = RunRows(db, ctx, sql, error);
  if (!rows.has_value() || rows->size() != 1) {
    return std::nullopt;
  }
  const Value& value = (*rows)[0][0];
  if (value.IsNull()) {
    return std::string("0");
  }
  return value.AsString();
}

std::string DumpRows(const std::vector<std::string>& rows) {
  std::string out;
  for (const std::string& r : rows) {
    out += "    " + r + "\n";
  }
  return out;
}

constexpr const char* kTestHeader = "-- tinylamb-oracle-test v1";

}  // namespace

std::string ReplayOracleTrace(const OracleTrace& trace, bool verbose) {
  if (trace.setup.empty() || trace.setup[0].rfind("CREATE TABLE", 0) != 0) {
    return "malformed trace: no CREATE TABLE in setup";
  }
  Database db("sql_oracle_replay-" + RandomString(8));
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  std::string error;

  for (const std::string& sql : trace.setup) {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    // QueryResults are lazy: drain or the INSERT never happens.
    if (!result.HasValue() || result.Value().Drain() == 0) {
      return "setup statement failed: " + sql + " :: " + engine.LastError();
    }
  }

  std::string report;

  // TLP: trace.tlp = [original, part1, part2, part3].
  if (trace.tlp.size() == 4) {
    auto r0 = RunRows(db, ctx, trace.tlp[0], &error);
    auto r1 = RunRows(db, ctx, trace.tlp[1], &error);
    auto r2 = RunRows(db, ctx, trace.tlp[2], &error);
    auto r3 = RunRows(db, ctx, trace.tlp[3], &error);
    if (r0.has_value() && r1.has_value() && r2.has_value() && r3.has_value()) {
      const std::vector<std::string> s0 = SerializeSorted(*r0);
      std::vector<std::string> s1 = SerializeSorted(*r1);
      std::vector<std::string> s2 = SerializeSorted(*r2);
      std::vector<std::string> s3 = SerializeSorted(*r3);
      std::vector<std::string> merged = s1;
      merged.insert(merged.end(), s2.begin(), s2.end());
      merged.insert(merged.end(), s3.begin(), s3.end());
      std::sort(merged.begin(), merged.end());
      if (s0 != merged) {
        report += "[TLP MISMATCH]\n";
        report += "  original: " + trace.tlp[0] + " (" +
                  std::to_string(s0.size()) + " rows)\n" + DumpRows(s0);
        report += "  part1:    " + trace.tlp[1] + " (" +
                  std::to_string(s1.size()) + " rows)\n" + DumpRows(s1);
        report += "  part2:    " + trace.tlp[2] + " (" +
                  std::to_string(s2.size()) + " rows)\n" + DumpRows(s2);
        report += "  part3:    " + trace.tlp[3] + " (" +
                  std::to_string(s3.size()) + " rows)\n" + DumpRows(s3);
        report += "  merged partition rows: " + std::to_string(merged.size()) +
                  "\n";
      } else if (verbose) {
        std::cerr << "[sql_oracle_replay][tlp-ok] orig=" << s0.size()
                  << " p1=" << s1.size() << " p2=" << s2.size()
                  << " p3=" << s3.size() << " merged=" << merged.size()
                  << "\n";
      }
    } else if (verbose) {
      std::cerr << "[sql_oracle_replay][tlp-error] " << error << "\n";
    }
  }

  // NoREC: trace.norec = [optimized COUNT, reference SUM].
  if (trace.norec.size() == 2) {
    auto c = RunScalar(db, ctx, trace.norec[0], &error);
    auto s = RunScalar(db, ctx, trace.norec[1], &error);
    if (c.has_value() && s.has_value()) {
      if (*c != *s) {
        report += "[NoREC MISMATCH]\n";
        report += "  optimized:   " + trace.norec[0] + " => " + *c + "\n";
        report += "  reference:   " + trace.norec[1] + " => " + *s + "\n";
      }
    } else if (verbose) {
      std::cerr << "[sql_oracle_replay][norec-error] " << error << "\n";
    }
  }

  if (!report.empty() && !trace.predicate.empty()) {
    report = "predicate: " + trace.predicate + "\n" + report;
  }
  return report;
}

std::string RunOracleIteration(std::mt19937& rng, bool verbose,
                               OracleIterationStats* stats,
                               OracleTrace* trace) {
  Gen g(rng);
  OracleTrace local;
  OracleTrace& t = (trace != nullptr) ? *trace : local;

  static const std::string kCreate =
      "CREATE TABLE t0 (a INT64, b INT64, flag BOOL, s VARCHAR(8));";
  t.setup.push_back(kCreate);
  Database db("sql_oracle_fuzz-" + RandomString(8));
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  const int row_count = g.Pick(4, 10);
  for (int i = 0; i < row_count; ++i) {
    t.setup.push_back("INSERT INTO t0 VALUES " + GenInsertValues(g) + ";");
  }
  for (const std::string& sql : t.setup) {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    // QueryResults are lazy: drain or the INSERT never happens.
    if (!result.HasValue() ||
        (result.Value().Drain() == 0 && sql.rfind("INSERT", 0) != 0)) {
      if (verbose) {
        std::cerr << "[sql_oracle_fuzz][skip-setup] "
                  << (result.HasValue() ? "no rows" : engine.LastError())
                  << " :: " << sql << "\n";
      }
      return "";
    }
  }

  t.predicate = GenPredicate(g, g.Pick(1, 3));

  // ---- TLP: Q == Q[p AND c] U Q[p AND NOT c] U Q[p AND c IS NULL] ----
  {
    const std::string proj = GenProjection(g);
    const std::string aux = GenPredicate(g, g.Pick(1, 2));
    t.tlp = {
        "SELECT " + proj + " FROM t0 WHERE " + t.predicate + ";",
        "SELECT " + proj + " FROM t0 WHERE (" + t.predicate + ") AND (" + aux +
            ");",
        "SELECT " + proj + " FROM t0 WHERE (" + t.predicate + ") AND NOT (" +
            aux + ");",
        "SELECT " + proj + " FROM t0 WHERE (" + t.predicate + ") AND (" + aux +
            ") IS NULL;",
    };
    std::string error;
    bool all_ok = true;
    for (const std::string& sql : t.tlp) {
      if (!RunRows(db, ctx, sql, &error).has_value()) {
        all_ok = false;
        break;
      }
    }
    if (all_ok) {
      if (stats != nullptr) {
        stats->tlp_ran = true;
      }
    } else {
      if (verbose) {
        std::cerr << "[sql_oracle_fuzz][skip-tlp] " << error << "\n";
      }
      t.tlp.clear();
    }
  }

  // ---- NoREC: COUNT(*) WHERE p == SUM(CASE WHEN p THEN 1 ELSE 0 END) ----
  {
    t.norec = {"SELECT COUNT(*) FROM t0 WHERE " + t.predicate + ";",
               "SELECT SUM(CASE WHEN " + t.predicate +
                   " THEN 1 ELSE 0 END) FROM t0;"};
    std::string error;
    bool all_ok = true;
    for (const std::string& sql : t.norec) {
      if (!RunScalar(db, ctx, sql, &error).has_value()) {
        all_ok = false;
        break;
      }
    }
    if (all_ok) {
      if (stats != nullptr) {
        stats->norec_ran = true;
      }
    } else {
      if (verbose) {
        std::cerr << "[sql_oracle_fuzz][skip-norec] " << error << "\n";
      }
      t.norec.clear();
    }
  }

  return ReplayOracleTrace(t, verbose);
}

std::string SerializeOracleTest(uint64_t seed, const OracleTrace& trace,
                                const std::string& failure_summary) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  if (!trace.predicate.empty()) {
    out += "-- predicate: " + trace.predicate + "\n";
  }
  for (const std::string& sql : trace.setup) {
    out += "-- setup: " + sql + "\n";
  }
  for (const std::string& sql : trace.tlp) {
    out += "-- tlp: " + sql + "\n";
  }
  for (const std::string& sql : trace.norec) {
    out += "-- norec: " + sql + "\n";
  }
  if (!failure_summary.empty()) {
    out += "-- failure: " + failure_summary + "\n";
  }
  return out;
}

bool ParseOracleTest(std::string_view text, uint64_t* seed, OracleTrace* trace,
                     std::string* failure_summary) {
  *seed = 0;
  *trace = OracleTrace{};
  failure_summary->clear();
  bool saw_header = false;
  size_t pos = 0;
  while (pos <= text.size()) {
    const size_t eol = text.find('\n', pos);
    std::string line(
        text.substr(pos, eol == std::string_view::npos ? std::string_view::npos
                                                       : eol - pos));
    pos = (eol == std::string_view::npos) ? text.size() + 1 : eol + 1;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    if (line.rfind("-- tinylamb-oracle-test", 0) == 0) {
      saw_header = true;
      continue;
    }
    auto consume = [&](const std::string& tag, std::string* value) {
      if (line.rfind(tag, 0) != 0) {
        return false;
      }
      *value = line.substr(tag.size());
      return true;
    };
    std::string value;
    if (consume("-- seed: ", &value)) {
      *seed = std::stoull(value);
    } else if (consume("-- predicate: ", &value)) {
      trace->predicate = value;
    } else if (consume("-- setup: ", &value)) {
      trace->setup.push_back(value);
    } else if (consume("-- tlp: ", &value)) {
      trace->tlp.push_back(value);
    } else if (consume("-- norec: ", &value)) {
      trace->norec.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;  // unknown line: refuse to replay a half-understood file
    }
  }
  return saw_header && !trace->setup.empty();
}

void OracleFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = seed * 257 + data[i] + 1;
  }
  std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
  OracleTrace trace;
  std::string report = RunOracleIteration(rng, verbose, nullptr, &trace);
  if (report.empty()) {
    return;
  }

  // Prove the emitted file reproduces the mismatch before saving it.
  const std::string text =
      SerializeOracleTest(seed, trace, "auto-generated by sql_oracle_fuzzer");
  uint64_t parsed_seed = 0;
  OracleTrace parsed;
  std::string summary;
  std::string verify;
  if (!ParseOracleTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized test does not parse";
  } else {
    verify = ReplayOracleTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized test replay holds (flaky mismatch)";
    }
  }

  const std::string path = "sql_oracle_fuzz-repro-" + std::to_string(seed) +
                           ".test";
  std::ofstream file(path);
  file << text;
  file.close();

  std::cerr << "[sql_oracle_fuzz] seed=" << seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
