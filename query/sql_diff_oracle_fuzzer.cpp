/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_diff_oracle_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/platform/random.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

constexpr const char* kHeader = "-- tinylamb-diff-oracle v1";
constexpr std::string_view kSep = " || ";

class Gen {
 public:
  explicit Gen(std::mt19937& rng) : rng_(rng) {}
  int Pick(int lo, int hi) { return platform::UniformInt<int>(rng_, lo, hi); }
  bool Chance(int percent) { return Pick(1, 100) <= percent; }

 private:
  std::mt19937& rng_;
};

struct ScopedEnv {
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* prev = std::getenv(name)) {
      had_ = true;
      prev_ = prev;
    }
    if (value != nullptr) {
      platform::SetEnv(name, value);
    } else {
      platform::UnsetEnv(name);
    }
  }
  ~ScopedEnv() {
    if (had_) {
      platform::SetEnv(name_, prev_.c_str());
    } else {
      platform::UnsetEnv(name_);
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;
  ScopedEnv(ScopedEnv&&) = delete;
  ScopedEnv& operator=(ScopedEnv&&) = delete;

  const char* name_;
  bool had_{false};
  std::string prev_;
};

std::optional<std::vector<Row>> RunRows(Database& db, TransactionContext& ctx,
                                        const std::string& sql, bool relational,
                                        std::string* error) {
  SqlEngine engine(db);
  engine.SetForceRelationalFallback(relational);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    if (error != nullptr) {
      *error = engine.LastError();
    }
    return std::nullopt;
  }
  std::vector<Row> rows;
  Row row;
  while (result.Value().Next(&row)) {
    rows.push_back(row);
  }
  if (result.Value().GetStatus() != Status::kSuccess) {
    if (error != nullptr) {
      *error = result.Value().GetStatus().GetMessage();
    }
    return std::nullopt;
  }
  return rows;
}

bool RunStmt(Database& db, TransactionContext& ctx, const std::string& sql,
             std::string* error) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    if (error != nullptr) {
      *error = engine.LastError();
    }
    return false;
  }
  result.Value().Drain();
  if (result.Value().GetStatus() != Status::kSuccess) {
    if (error != nullptr) {
      *error = result.Value().GetStatus().GetMessage();
    }
    return false;
  }
  return true;
}

std::string JoinSorted(std::vector<std::string> cells) {
  std::sort(cells.begin(), cells.end());
  std::string out;
  for (size_t i = 0; i < cells.size(); ++i) {
    if (i != 0) {
      out += " ;; ";
    }
    out += cells[i];
  }
  return out;
}

std::string SortedDump(const std::vector<Row>& rows) {
  std::vector<std::string> cells;
  cells.reserve(rows.size());
  for (const Row& row : rows) {
    cells.push_back(row.ToString());
  }
  return JoinSorted(std::move(cells));
}

std::string Cell(int64_t id) { return Row({Value(id)}).ToString(); }

std::string Pair(int64_t left, bool right_null, int64_t right) {
  std::vector<Value> cells;
  cells.emplace_back(left);
  if (right_null) {
    cells.emplace_back();
  } else {
    cells.emplace_back(right);
  }
  return Row(std::move(cells)).ToString();
}

std::set<int64_t> IdSet(const std::vector<Row>& rows) {
  std::set<int64_t> ids;
  for (const Row& row : rows) {
    if (row.Count() < 1 || row[0].IsNull() ||
        row[0].type != ValueType::kInt64) {
      continue;
    }
    ids.insert(row[0].value.int_value);
  }
  return ids;
}

bool IsSubset(const std::set<int64_t>& small, const std::set<int64_t>& big) {
  return std::includes(big.begin(), big.end(), small.begin(), small.end());
}

std::optional<int64_t> ScalarInt(Database& db, TransactionContext& ctx,
                                 const std::string& sql, std::string* error) {
  auto rows = RunRows(db, ctx, sql, false, error);
  if (!rows.has_value() || rows->size() != 1 || (*rows)[0].Count() < 1) {
    return std::nullopt;
  }
  const Value& value = (*rows)[0][0];
  if (value.IsNull()) {
    return 0;
  }
  if (value.type != ValueType::kInt64) {
    return std::nullopt;
  }
  return value.value.int_value;
}

bool SplitPair(const std::string& packed, std::string* left,
               std::string* right) {
  const size_t pos = packed.find(kSep);
  if (pos == std::string::npos) {
    return false;
  }
  *left = packed.substr(0, pos);
  *right = packed.substr(pos + kSep.size());
  return true;
}

std::string TableName(std::mt19937& rng) {
  return "g" + std::to_string(platform::UniformInt<int64_t>(rng, 0, INT32_MAX));
}

bool Setup(Database& db, TransactionContext* ctx,
           const std::vector<std::string>& setup, std::string* error) {
  for (const std::string& sql : setup) {
    if (!RunStmt(db, *ctx, sql, error)) {
      return false;
    }
  }
  (void)ctx->txn_.PreCommit();
  *ctx = db.BeginContext();
  return true;
}

std::string CheckTqs(Database& db, TransactionContext& ctx,
                     const DiffOracleTrace& trace) {
  if (trace.queries.size() != trace.expect.size()) {
    return "tqs trace has a different number of queries and expectations";
  }
  std::string error;
  for (size_t i = 0; i < trace.queries.size(); ++i) {
    auto rows = RunRows(db, ctx, trace.queries[i], false, &error);
    if (!rows.has_value()) {
      return "tqs query failed: " + error + "\n  " + trace.queries[i];
    }
    const std::string got = SortedDump(*rows);
    if (got != trace.expect[i]) {
      return "[TQS MISMATCH]\n  query: " + trace.queries[i] +
             "\n  expected:\n" + trace.expect[i] + "  got:\n" + got;
    }
  }
  return {};
}

std::string CheckSubset(Database& db, TransactionContext& ctx,
                        const DiffOracleTrace& trace, DiffOracleStats* stats) {
  std::string error;
  auto load = [&](const std::string& sql) -> std::optional<std::set<int64_t>> {
    auto rows = RunRows(db, ctx, sql, false, &error);
    if (!rows.has_value()) {
      return std::nullopt;
    }
    return IdSet(*rows);
  };
  auto note = [&](const char* tag, const std::string& left_sql,
                  const std::string& right_sql) {
    return std::string("[SUBSET MISMATCH]\n  ") + tag +
           "\n  left:  " + left_sql + "\n  right: " + right_sql + "\n";
  };
  for (const std::string& packed : trace.subset_and) {
    std::string weak;
    std::string strong;
    if (!SplitPair(packed, &weak, &strong)) {
      return "malformed subset-and pair";
    }
    auto w = load(weak);
    auto s = load(strong);
    if (!w.has_value() || !s.has_value()) {
      return "subset query failed: " + error;
    }
    if (stats != nullptr) {
      ++stats->subset_checks;
    }
    if (!IsSubset(*s, *w)) {
      return note("conjunct not subset of factor", weak, strong);
    }
  }
  for (const std::string& packed : trace.subset_or) {
    std::string narrow;
    std::string wide;
    if (!SplitPair(packed, &narrow, &wide)) {
      return "malformed subset-or pair";
    }
    auto n = load(narrow);
    auto w = load(wide);
    if (!n.has_value() || !w.has_value()) {
      return "subset query failed: " + error;
    }
    if (stats != nullptr) {
      ++stats->subset_checks;
    }
    if (!IsSubset(*n, *w)) {
      return note("disjunct not superset", narrow, wide);
    }
  }
  for (const std::string& packed : trace.subset_dis) {
    std::string pos;
    std::string neg;
    if (!SplitPair(packed, &pos, &neg)) {
      return "malformed subset-dis pair";
    }
    auto p = load(pos);
    auto n = load(neg);
    if (!p.has_value() || !n.has_value()) {
      return "subset query failed: " + error;
    }
    if (stats != nullptr) {
      ++stats->subset_checks;
    }
    std::vector<int64_t> both;
    std::set_intersection(p->begin(), p->end(), n->begin(), n->end(),
                          std::back_inserter(both));
    if (!both.empty()) {
      return note("predicate overlaps its negation", pos, neg);
    }
  }
  for (const std::string& packed : trace.subset_eq) {
    std::string left;
    std::string right;
    if (!SplitPair(packed, &left, &right)) {
      return "malformed subset-eq pair";
    }
    auto l = load(left);
    auto r = load(right);
    if (!l.has_value() || !r.has_value()) {
      return "rewrite query failed: " + error;
    }
    if (stats != nullptr) {
      ++stats->subset_checks;
    }
    if (*l != *r) {
      return note("expression rewrite", left, right);
    }
  }
  for (const std::string& packed : trace.subset_card) {
    std::string left;
    std::string right;
    if (!SplitPair(packed, &left, &right)) {
      return "malformed subset-card pair";
    }
    auto l = ScalarInt(db, ctx, left, &error);
    auto r = ScalarInt(db, ctx, right, &error);
    if (!l.has_value() || !r.has_value()) {
      return "cardinality query failed: " + error;
    }
    if (stats != nullptr) {
      ++stats->subset_checks;
    }
    if (*l > *r) {
      std::string message = "[CARD MISMATCH]\n  ";
      message.append(left).append(" => ").append(std::to_string(*l));
      message.append("\n  ").append(right).append(" => ").append(
          std::to_string(*r));
      message += '\n';
      return message;
    }
  }
  return {};
}

std::string RunMode(Database& db, TransactionContext& ctx,
                    const std::string& sql, int mode, std::string* dump,
                    std::string* error) {
  std::optional<std::vector<Row>> rows;
  if (mode == 0) {
    rows = RunRows(db, ctx, sql, false, error);
  } else if (mode == 1) {
    ScopedEnv bytecode("TINYLAMB_DISABLE_BYTECODE", "1");
    ScopedEnv fuse("TINYLAMB_FUSE_SIBLING_AGG", "0");
    rows = RunRows(db, ctx, sql, true, error);
  } else {
    ScopedEnv route("TINYLAMB_GROUPED_ROUTE", "cascades");
    rows = RunRows(db, ctx, sql, false, error);
  }
  if (!rows.has_value()) {
    return {};
  }
  *dump = SortedDump(*rows);
  static constexpr std::array<const char*, 3> kNames = {"default", "relational",
                                                        "grouped"};
  return kNames[mode];
}

std::string CheckDqp(Database& db, TransactionContext& ctx,
                     const DiffOracleTrace& trace, DiffOracleStats* stats) {
  for (const std::string& sql : trace.queries) {
    std::vector<std::pair<std::string, std::string>> ran;
    for (int mode = 0; mode < 3; ++mode) {
      std::string error;
      std::string dump;
      const std::string name = RunMode(db, ctx, sql, mode, &dump, &error);
      if (!name.empty()) {
        ran.emplace_back(name, dump);
      }
    }
    if (ran.size() < 2) {
      continue;
    }
    if (stats != nullptr) {
      ++stats->dqp_queries;
    }
    for (size_t i = 1; i < ran.size(); ++i) {
      if (ran[i].second != ran[0].second) {
        return "[DQP MISMATCH]\n  query: " + sql + "\n  " + ran[0].first +
               ":\n" + ran[0].second + "  " + ran[i].first + ":\n" +
               ran[i].second;
      }
    }
  }
  return {};
}

std::string CheckRadar(const DiffOracleTrace& trace, DiffOracleStats* stats) {
  ScopedDb plain("diff_oracle_radar_plain");
  ScopedDb indexed("diff_oracle_radar_indexed");
  if (!plain || !indexed) {
    return {};
  }
  TransactionContext plain_ctx = plain->BeginContext();
  TransactionContext indexed_ctx = indexed->BeginContext();
  std::string error;
  if (!Setup(*plain, &plain_ctx, trace.setup, &error) ||
      !Setup(*indexed, &indexed_ctx, trace.setup, &error)) {
    return "radar setup failed: " + error;
  }
  for (const std::string& ddl : trace.indexes) {
    if (!ApplyIndexSpec(*indexed, indexed_ctx, ddl)) {
      return "radar index failed: " + ddl;
    }
  }
  if (!trace.setup.empty()) {
    const std::string& table = trace.setup[0];
    const size_t begin = std::string("CREATE TABLE ").size();
    const size_t end = table.find(' ', begin);
    if (end != std::string::npos && end > begin) {
      RunStmt(*indexed, indexed_ctx,
              "ANALYZE " + table.substr(begin, end - begin) + ";", &error);
    }
  }
  for (const std::string& sql : trace.queries) {
    auto left = RunRows(*plain, plain_ctx, sql, false, &error);
    auto right = RunRows(*indexed, indexed_ctx, sql, false, &error);
    if (!left.has_value() || !right.has_value()) {
      std::string message = "radar query failed: ";
      message.append(error).append("\n  ").append(sql);
      return message;
    }
    if (stats != nullptr) {
      ++stats->radar_queries;
    }
    const std::string a = SortedDump(*left);
    const std::string b = SortedDump(*right);
    if (a != b) {
      std::string message = "[RADAR MISMATCH]\n  query: ";
      message.append(sql)
          .append("\n  plain:\n")
          .append(a)
          .append("  indexed:\n")
          .append(b);
      return message;
    }
  }
  return {};
}

}  // namespace

std::string ReplayDiffOracleTrace(const DiffOracleTrace& trace, bool verbose) {
  if (trace.kind == "radar") {
    return CheckRadar(trace, nullptr);
  }
  if (trace.setup.empty() || !trace.setup[0].starts_with("CREATE TABLE")) {
    return "malformed diff-oracle trace";
  }
  ScopedDb db("diff_oracle");
  if (!db) {
    return {};
  }
  TransactionContext ctx = db->BeginContext();
  std::string error;
  if (!Setup(*db, &ctx, trace.setup, &error)) {
    if (verbose) {
      std::cerr << "[sql_diff_oracle][setup] " << error << "\n";
    }
    return "setup failed: " + error;
  }
  if (trace.kind == "tqs") {
    return CheckTqs(*db, ctx, trace);
  }
  if (trace.kind == "subset") {
    return CheckSubset(*db, ctx, trace, nullptr);
  }
  if (trace.kind == "dqp") {
    return CheckDqp(*db, ctx, trace, nullptr);
  }
  return "unknown diff-oracle kind " + trace.kind;
}

std::string RunTqsIteration(std::mt19937& rng, bool /*verbose*/,
                            DiffOracleStats* stats, DiffOracleTrace* trace) {
  Gen g(rng);
  DiffOracleTrace local;
  DiffOracleTrace& t = trace != nullptr ? *trace : local;
  t = DiffOracleTrace{};
  t.kind = "tqs";
  const std::string base = TableName(rng);
  const std::string t1 = base + "_a";
  const std::string t2 = base + "_b";
  t.setup.push_back("CREATE TABLE " + t1 + " (id INT64, a INT64, b INT64);");
  t.setup.push_back("CREATE TABLE " + t2 + " (id INT64, a INT64, b INT64);");
  struct Membership {
    int64_t id;
    bool in1;
    bool in2;
  };
  std::vector<Membership> rows;
  const int n = g.Pick(5, 10);
  rows.reserve(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    rows.push_back(
        Membership{.id = i, .in1 = g.Chance(65), .in2 = g.Chance(55)});
  }
  rows.front().in1 = true;
  rows.front().in2 = true;
  std::vector<std::string> inner;
  std::vector<std::string> left;
  std::vector<std::string> semi;
  std::vector<std::string> anti;
  for (const Membership& row : rows) {
    const std::string values = "(" + std::to_string(row.id) + ", " +
                               std::to_string(g.Pick(-2, 3)) + ", " +
                               std::to_string(g.Pick(-2, 2)) + ");";
    if (row.in1) {
      std::string insert = "INSERT INTO ";
      insert.append(t1).append(" VALUES ").append(values);
      t.setup.push_back(std::move(insert));
    }
    if (row.in2) {
      std::string insert = "INSERT INTO ";
      insert.append(t2).append(" VALUES ").append(values);
      t.setup.push_back(std::move(insert));
    }
    if (row.in1 && row.in2) {
      inner.push_back(Pair(row.id, false, row.id));
      semi.push_back(Cell(row.id));
    } else if (row.in1) {
      anti.push_back(Cell(row.id));
    }
    if (row.in1) {
      left.push_back(Pair(row.id, !row.in2, row.id));
    }
  }
  t.queries = {
      "SELECT " + t1 + ".id, " + t2 + ".id FROM " + t1 + " JOIN " + t2 +
          " ON " + t1 + ".id = " + t2 + ".id;",
      "SELECT " + t1 + ".id, " + t2 + ".id FROM " + t1 + " LEFT JOIN " + t2 +
          " ON " + t1 + ".id = " + t2 + ".id;",
      "SELECT id FROM " + t1 + " WHERE id IN (SELECT id FROM " + t2 + ");",
      "SELECT id FROM " + t1 + " WHERE id NOT IN (SELECT id FROM " + t2 + ");",
  };
  t.expect = {JoinSorted(std::move(inner)), JoinSorted(std::move(left)),
              JoinSorted(std::move(semi)), JoinSorted(std::move(anti))};
  DiffOracleStats local_stats;
  DiffOracleStats* out = stats != nullptr ? stats : &local_stats;
  const std::string report = ReplayDiffOracleTrace(t, false);
  if (report.empty()) {
    out->tqs_queries += static_cast<int>(t.queries.size());
  }
  return report;
}

std::string RunSubsetIteration(std::mt19937& rng, bool /*verbose*/,
                               DiffOracleStats* stats, DiffOracleTrace* trace) {
  Gen g(rng);
  DiffOracleTrace local;
  DiffOracleTrace& t = trace != nullptr ? *trace : local;
  t = DiffOracleTrace{};
  t.kind = "subset";
  const std::string tab = TableName(rng);
  t.setup.push_back("CREATE TABLE " + tab +
                    " (u INT64, a INT64, b INT64, flag BOOL, s VARCHAR(8));");
  static constexpr std::array<const char*, 4> kStr = {"''", "'a'", "'bb'",
                                                      "NULL"};
  const int n = g.Pick(6, 12);
  for (int i = 0; i < n; ++i) {
    const std::string a = g.Chance(20) ? "NULL" : std::to_string(g.Pick(-2, 3));
    const std::string b = g.Chance(15) ? "NULL" : std::to_string(g.Pick(-2, 2));
    const char* flag =
        g.Chance(40) ? "TRUE" : (g.Chance(50) ? "FALSE" : "NULL");
    const char* s = kStr[g.Pick(0, 3)];
    std::string insert = "INSERT INTO ";
    insert.append(tab).append(" VALUES (").append(std::to_string(i));
    insert.append(", ").append(a).append(", ").append(b);
    insert.append(", ").append(flag).append(", ").append(s).append(");");
    t.setup.push_back(std::move(insert));
  }
  const int k = g.Pick(-1, 2);
  const int m = g.Pick(-1, 2);
  const std::string all = "SELECT u FROM " + tab;
  const std::string pred = "(a > " + std::to_string(k) + ")";
  const std::string other = "(b < " + std::to_string(m) + ")";
  const std::string weak = all + " WHERE " + pred + ";";
  const std::string strong = all + " WHERE " + pred + " AND " + other + ";";
  const std::string wide = all + " WHERE " + pred + " OR " + other + ";";
  const std::string neg = all + " WHERE NOT " + pred + ";";
  t.subset_and.push_back(weak + std::string(kSep) + strong);
  t.subset_or.push_back(weak + std::string(kSep) + wide);
  t.subset_dis.push_back(weak + std::string(kSep) + neg);
  t.subset_eq.push_back(all + " WHERE s = 'a';" + std::string(kSep) + all +
                        " WHERE s LIKE 'a';");
  t.subset_eq.push_back(all + " WHERE a != " + std::to_string(k) + ";" +
                        std::string(kSep) + all +
                        " WHERE NOT (a = " + std::to_string(k) + ");");
  t.subset_card.push_back("SELECT COUNT(DISTINCT a) FROM " + tab + ";" +
                          std::string(kSep) + "SELECT COUNT(*) FROM " + tab +
                          ";");
  t.subset_card.push_back("SELECT COUNT(*) FROM " + tab + " WHERE " + pred +
                          " AND " + other + ";" + std::string(kSep) +
                          "SELECT COUNT(*) FROM " + tab + " WHERE " + pred +
                          ";");
  const std::string report = ReplayDiffOracleTrace(t, false);
  if (stats != nullptr && report.empty()) {
    stats->subset_checks += static_cast<int>(
        t.subset_and.size() + t.subset_or.size() + t.subset_dis.size() +
        t.subset_eq.size() + t.subset_card.size());
  }
  return report;
}

std::string RunDqpIteration(std::mt19937& rng, bool /*verbose*/,
                            DiffOracleStats* stats, DiffOracleTrace* trace) {
  Gen g(rng);
  DiffOracleTrace local;
  DiffOracleTrace& t = trace != nullptr ? *trace : local;
  t = DiffOracleTrace{};
  t.kind = "dqp";
  const std::string tab = TableName(rng);
  t.setup.push_back("CREATE TABLE " + tab +
                    " (u INT64, a INT64, b INT64, flag BOOL, s VARCHAR(8));");
  const int n = g.Pick(5, 9);
  for (int i = 0; i < n; ++i) {
    const std::string a = g.Chance(20) ? "NULL" : std::to_string(g.Pick(-2, 3));
    const std::string b = g.Chance(15) ? "NULL" : std::to_string(g.Pick(-1, 2));
    const char* flag = g.Chance(50) ? "TRUE" : "FALSE";
    std::string insert = "INSERT INTO ";
    insert.append(tab).append(" VALUES (").append(std::to_string(i));
    insert.append(", ").append(a).append(", ").append(b);
    insert.append(", ").append(flag).append(", 'a');");
    t.setup.push_back(std::move(insert));
  }
  const int k = g.Pick(-1, 2);
  t.queries = {
      "SELECT u, a, b FROM " + tab + " WHERE a > " + std::to_string(k) +
          " OR b IS NULL;",
      "SELECT a, COUNT(*) FROM " + tab + " GROUP BY a;",
      "SELECT l.u, r.u FROM " + tab + " AS l JOIN " + tab +
          " AS r ON l.a = r.a WHERE l.u < r.u;",
      "SELECT u FROM " + tab + " WHERE a IN (" + std::to_string(k) + ", " +
          std::to_string(k + 1) + ") OR flag IS TRUE;",
  };
  DiffOracleStats local_stats;
  DiffOracleStats* out = stats != nullptr ? stats : &local_stats;
  const std::string report = ReplayDiffOracleTrace(t, false);
  if (report.empty()) {
    // Replay does not count. Re-count by a cheap second pass only when the
    // caller asked, using the same checker against the trace's own database
    // inside CheckDqp via Replay's path: call Check through a live db.
    ScopedDb db("diff_oracle_dqp_stats");
    if (db) {
      TransactionContext ctx = db->BeginContext();
      std::string error;
      if (Setup(*db, &ctx, t.setup, &error)) {
        CheckDqp(*db, ctx, t, out);
      }
    }
  }
  return report;
}

std::string RunRadarIteration(std::mt19937& rng, bool /*verbose*/,
                              DiffOracleStats* stats, DiffOracleTrace* trace) {
  Gen g(rng);
  DiffOracleTrace local;
  DiffOracleTrace& t = trace != nullptr ? *trace : local;
  t = DiffOracleTrace{};
  t.kind = "radar";
  const std::string tab = TableName(rng);
  t.setup.push_back("CREATE TABLE " + tab +
                    " (u INT64, a INT64, b INT64, flag BOOL, s VARCHAR(8));");
  const int n = g.Pick(5, 10);
  for (int i = 0; i < n; ++i) {
    const std::string a = g.Chance(25) ? "NULL" : std::to_string(g.Pick(-2, 3));
    const std::string b = g.Chance(15) ? "NULL" : std::to_string(g.Pick(-2, 2));
    std::string insert = "INSERT INTO ";
    insert.append(tab).append(" VALUES (").append(std::to_string(i));
    insert.append(", ").append(a).append(", ").append(b).append(
        ", TRUE, 'a');");
    t.setup.push_back(std::move(insert));
  }
  t.indexes = {
      "CREATE INDEX idx_gap_a ON " + tab + "(a);",
      "CREATE INDEX idx_gap_b ON " + tab + "(b);",
      "CREATE UNIQUE INDEX idx_gap_u ON " + tab + "(u);",
  };
  const int k = g.Pick(-1, 2);
  t.queries = {
      "SELECT u, a, b FROM " + tab + " WHERE a > " + std::to_string(k) + ";",
      "SELECT u FROM " + tab + " WHERE a = " + std::to_string(k) +
          " OR b IS NULL;",
      "SELECT a, COUNT(*) FROM " + tab + " GROUP BY a;",
      "SELECT u FROM " + tab +
          " WHERE u = " + std::to_string(g.Pick(0, n - 1)) + ";",
  };
  DiffOracleStats local_stats;
  DiffOracleStats* out = stats != nullptr ? stats : &local_stats;
  const std::string report = ReplayDiffOracleTrace(t, false);
  if (report.empty()) {
    out->radar_queries += static_cast<int>(t.queries.size());
  }
  return report;
}

std::string SerializeDiffOracleTest(uint64_t seed, const DiffOracleTrace& trace,
                                    const std::string& failure_summary) {
  std::string out = std::string(kHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  out += "-- kind: " + trace.kind + "\n";
  for (const std::string& sql : trace.setup) {
    out += "-- setup: " + sql + "\n";
  }
  for (const std::string& sql : trace.indexes) {
    out += "-- index: " + sql + "\n";
  }
  for (const std::string& sql : trace.queries) {
    out += "-- query: " + sql + "\n";
  }
  for (const std::string& exp : trace.expect) {
    out += "-- expect: " + exp + "\n";
  }
  auto dump_pairs = [&](const char* tag,
                        const std::vector<std::string>& pairs) {
    for (const std::string& pair : pairs) {
      out += "-- ";
      out += tag;
      out += ": ";
      out += pair;
      out.push_back('\n');
    }
  };
  dump_pairs("and", trace.subset_and);
  dump_pairs("or", trace.subset_or);
  dump_pairs("dis", trace.subset_dis);
  dump_pairs("eq", trace.subset_eq);
  dump_pairs("card", trace.subset_card);
  if (!failure_summary.empty()) {
    out += "-- failure: " + failure_summary + "\n";
  }
  return out;
}

bool ParseDiffOracleTest(std::string_view text, uint64_t* seed,
                         DiffOracleTrace* trace, std::string* failure_summary) {
  *seed = 0;
  *trace = DiffOracleTrace{};
  failure_summary->clear();
  bool saw = false;
  size_t pos = 0;
  while (pos <= text.size()) {
    const size_t eol = text.find('\n', pos);
    std::string line(text.substr(pos, eol == std::string_view::npos
                                          ? std::string_view::npos
                                          : eol - pos));
    pos = eol == std::string_view::npos ? text.size() + 1 : eol + 1;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    if (line == kHeader) {
      saw = true;
      continue;
    }
    auto take = [&](std::string_view tag, std::string* value) {
      if (!line.starts_with(tag)) {
        return false;
      }
      *value = line.substr(tag.size());
      return true;
    };
    std::string value;
    if (take("-- seed: ", &value)) {
      *seed = std::stoull(value);
    } else if (take("-- kind: ", &value)) {
      trace->kind = value;
    } else if (take("-- setup: ", &value)) {
      trace->setup.push_back(value);
    } else if (take("-- index: ", &value)) {
      trace->indexes.push_back(value);
    } else if (take("-- query: ", &value)) {
      trace->queries.push_back(value);
    } else if (take("-- expect: ", &value)) {
      trace->expect.push_back(value);
    } else if (take("-- and: ", &value)) {
      trace->subset_and.push_back(value);
    } else if (take("-- or: ", &value)) {
      trace->subset_or.push_back(value);
    } else if (take("-- dis: ", &value)) {
      trace->subset_dis.push_back(value);
    } else if (take("-- eq: ", &value)) {
      trace->subset_eq.push_back(value);
    } else if (take("-- card: ", &value)) {
      trace->subset_card.push_back(value);
    } else if (take("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;
    }
  }
  return saw && !trace->setup.empty();
}

void DiffOracleFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
  const int kind = size == 0 ? 0 : static_cast<int>(data[0] % 4);
  DiffOracleTrace trace;
  std::string report;
  if (kind == 0) {
    report = RunTqsIteration(rng, verbose, nullptr, &trace);
  } else if (kind == 1) {
    report = RunSubsetIteration(rng, verbose, nullptr, &trace);
  } else if (kind == 2) {
    report = RunDqpIteration(rng, verbose, nullptr, &trace);
  } else {
    report = RunRadarIteration(rng, verbose, nullptr, &trace);
  }
  if (report.empty()) {
    return;
  }
  const std::string text = SerializeDiffOracleTest(
      seed, trace,
      FormatFuzzFailureSummary("auto-generated by sql_diff_oracle_fuzzer",
                               report));
  uint64_t parsed_seed = 0;
  DiffOracleTrace parsed;
  std::string summary;
  std::string verify;
  if (!ParseDiffOracleTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized diff-oracle trace does not parse";
  } else {
    verify = ReplayDiffOracleTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized diff-oracle replay holds (flaky)";
    }
  }
  const std::string path =
      "sql_diff_oracle_fuzz-repro-" + std::to_string(seed) + ".test";
  EmitFuzzFinding("sql_diff_oracle_fuzz", seed, path, text, report, verify);
}

}  // namespace tinylamb
