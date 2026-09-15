/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_aggregate_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
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

constexpr const char* kTestHeader = "-- tinylamb-aggregate-test v1";
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

// Mirror row of t(u, a, b, s).
struct MRow {
  int64_t u{0};
  int64_t a{kNullRepr};
  int64_t b{kNullRepr};
  int64_t s{kNullRepr};  // 0 = 'x', 1 = 'yy'
};

std::string FmtInt(int64_t v) {
  return v == kNullRepr ? "NULL" : "|" + std::to_string(v) + "|";
}
std::string FmtStr(int64_t v) {
  if (v == kNullRepr) {
    return "NULL";
  }
  // Value::AsString keeps the quotes for VARCHAR cells.
  return v == 0 ? "|\"x\"|" : "|\"yy\"|";
}
std::string FmtEngine(const Value& v) {
  if (v.IsNull()) {
    return "NULL";
  }
  return "|" + v.AsString() + "|";
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

constexpr const char* kDdl =
    "CREATE TABLE t (u INT64, a INT64, b INT64, s VARCHAR(8));";

std::vector<std::string> FormatRows(
    const std::vector<std::vector<Value>>& rows) {
  std::vector<std::string> out;
  for (const auto& cells : rows) {
    std::string row;
    for (const Value& v : cells) {
      row += FmtEngine(v) + ",";
    }
    out.push_back(row);
  }
  std::sort(out.begin(), out.end());
  return out;
}

// ---------------------------------------------------------------------------
// Mirror computation.
// ---------------------------------------------------------------------------

// Column selectors: 0 = a, 1 = b, 2 = s.
int64_t CellOf(const MRow& r, int col) {
  return col == 0 ? r.a : (col == 1 ? r.b : r.s);
}
bool IsStrCol(int col) { return col == 2; }
std::string ColName(int col) { return col == 0 ? "a" : (col == 1 ? "b" : "s"); }
std::string CellSql(int col, int64_t v) {
  if (v == kNullRepr) {
    return "NULL";
  }
  return IsStrCol(col) ? (v == 0 ? "'x'" : "'yy'") : std::to_string(v);
}
std::string GroupKey(int col, const MRow& r) { return FmtInt(CellOf(r, col)); }

// Simple WHERE: comparisons / IS NULL over a, b, s, AND/OR/NOT depth 2.
struct WPred {
  enum class Kind { kCmp, kIsNull, kNot, kBin };
  Kind kind{Kind::kCmp};
  bool negated{false};
  std::string op{"="};
  int col{0};
  int64_t const_val{0};
  std::unique_ptr<WPred> left, right;

  [[nodiscard]] char Eval(const MRow& r) const {
    auto cmp = [](int64_t l, const std::string& o, int64_t rr, bool str) {
      if (l == kNullRepr || rr == kNullRepr) {
        return 'N';
      }
      const bool eq = l == rr;
      const bool lt = l < rr;
      char res = 0;
      if (o == "=") {
        res = eq ? 'T' : 'F';
      } else if (o == "!=") {
        res = !eq ? 'T' : 'F';
      } else if (o == "<") {
        res = lt ? 'T' : 'F';
      } else if (o == "<=") {
        res = (lt || eq) ? 'T' : 'F';
      } else if (o == ">") {
        res = (!lt && !eq) ? 'T' : 'F';
      } else {
        res = (!lt || eq) ? 'T' : 'F';
      }
      (void)str;
      return res;
    };
    switch (kind) {
      case Kind::kCmp:
        return cmp(CellOf(r, col), op, const_val, IsStrCol(col));
      case Kind::kIsNull: {
        const bool is_null = CellOf(r, col) == kNullRepr;
        return (negated ? !is_null : is_null) ? 'T' : 'F';
      }
      case Kind::kNot:
        return left->Eval(r) == 'T' ? 'F' : (left->Eval(r) == 'F' ? 'T' : 'N');
      case Kind::kBin: {
        const char l = left->Eval(r);
        const char rr = right->Eval(r);
        if (op == "AND") {
          if (l == 'F' || rr == 'F') {
            return 'F';
          }
          if (l == 'N' || rr == 'N') {
            return 'N';
          }
          return 'T';
        }
        if (l == 'T' || rr == 'T') {
          return 'T';
        }
        if (l == 'N' || rr == 'N') {
          return 'N';
        }
        return 'F';
      }
    }
    return 'N';
  }

  [[nodiscard]] std::string Render() const {
    switch (kind) {
      case Kind::kCmp: {
        return "(" + ColName(col) + " " + op + " " + CellSql(col, const_val) +
               ")";
      }
      case Kind::kIsNull:
        return "(" + ColName(col) + " IS " + (negated ? "NOT " : "") + "NULL)";
      case Kind::kNot:
        return "(NOT " + left->Render() + ")";
      case Kind::kBin:
        return "(" + left->Render() + " " + op + " " + right->Render() + ")";
    }
    return "TRUE";
  }
};
using WPredPtr = std::unique_ptr<WPred>;

WPredPtr GenWhere(Gen& g, int depth) {
  if (depth <= 0 || g.Chance(45)) {
    auto p = std::make_unique<WPred>();
    p->col = g.Pick(0, 2);
    if (g.Chance(35)) {
      p->kind = WPred::Kind::kIsNull;
      p->negated = g.Chance(50);
      return p;
    }
    p->kind = WPred::Kind::kCmp;
    static constexpr std::array<std::string_view, 6> kOps = {"=",  "!=", "<",
                                                             "<=", ">",  ">="};
    p->op = std::string(kOps[static_cast<size_t>(g.Pick(0, 5))]);
    p->const_val = IsStrCol(p->col) ? g.Pick(0, 1) : g.Pick(-2, 2);
    return p;
  }
  auto p = std::make_unique<WPred>();
  if (g.Pick(0, 3) == 3) {
    p->kind = WPred::Kind::kNot;
    p->left = GenWhere(g, depth - 1);
    return p;
  }
  p->kind = WPred::Kind::kBin;
  p->op = g.Chance(60) ? "AND" : "OR";
  p->left = GenWhere(g, depth - 1);
  p->right = GenWhere(g, depth - 1);
  return p;
}

struct AggSpec {
  std::string sql;  // "COUNT(*)", "SUM(a)", ...
  int col{0};       // argument column (-1 for COUNT(*))
  enum Kind { kCountStar, kCount, kSum, kMin, kMax } kind{kCountStar};
};

std::string ComputeAgg(const AggSpec& spec, const std::vector<MRow>& rows) {
  switch (spec.kind) {
    case AggSpec::Kind::kCountStar:
      return FmtInt(static_cast<int64_t>(rows.size()));
    case AggSpec::Kind::kCount: {
      int64_t n = 0;
      for (const MRow& r : rows) {
        if (CellOf(r, spec.col) != kNullRepr) {
          ++n;
        }
      }
      return FmtInt(n);
    }
    case AggSpec::Kind::kSum: {
      bool any = false;
      int64_t sum = 0;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (v != kNullRepr) {
          any = true;
          sum += v;
        }
      }
      return any ? FmtInt(sum) : std::string("NULL");
    }
    case AggSpec::Kind::kMin:
    case AggSpec::Kind::kMax: {
      bool any = false;
      int64_t best = 0;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (v == kNullRepr) {
          continue;
        }
        if (!any) {
          best = v;
          any = true;
        } else if (spec.kind == AggSpec::Kind::kMin ? v < best : v > best) {
          best = v;
        }
      }
      return any ? (IsStrCol(spec.col) ? FmtStr(best) : FmtInt(best))
                 : std::string("NULL");
    }
  }
  return "NULL";
}

}  // namespace

std::string RunAggregateIteration(std::mt19937& rng, bool verbose,
                                  AggStats* stats, AggTrace* trace) {
  Gen g(rng);
  AggTrace local;
  AggTrace& t = (trace != nullptr) ? *trace : local;

  t.setup.emplace_back(kDdl);
  std::vector<MRow> mirror;
  const int row_count = g.Pick(5, 12);
  for (int i = 0; i < row_count; ++i) {
    MRow r;
    r.u = i;
    r.a = g.Chance(25) ? kNullRepr : g.Pick(-3, 3);
    r.b = g.Chance(25) ? kNullRepr : g.Pick(-2, 2);
    r.s = g.Chance(15) ? kNullRepr : g.Pick(0, 1);
    mirror.push_back(r);
    t.setup.push_back("INSERT INTO t VALUES (" + std::to_string(r.u) + ", " +
                      CellSql(0, r.a) + ", " + CellSql(1, r.b) + ", " +
                      CellSql(2, r.s) + ");");
  }

  // ScopedDb deletes the throwaway .db/.log pair when the iteration ends.
  ScopedDb db_owner("sql_aggregate_fuzz");
  Database& db = *db_owner;
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  for (const std::string& sql : t.setup) {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    if (!result.HasValue()) {
      if (verbose) {
        std::cerr << "[agg_fuzz][skip-setup] " << sql << "\n";
      }
      return "";
    }
    // QueryResults are lazy: drain or the INSERT never happens.
    result.Value().Drain();
  }

  std::string report;
  const int query_count = g.Pick(2, 5);
  for (int q = 0; q < query_count; ++q) {
    const bool window = g.Chance(45);
    std::string sql;
    std::vector<std::string> expected;

    if (!window) {
      // GROUP BY query.
      const int gcol = g.Pick(0, 2);
      std::vector<AggSpec> aggs;
      aggs.push_back({"COUNT(*)", -1, AggSpec::Kind::kCountStar});
      if (g.Chance(70)) {
        aggs.push_back({IsStrCol(gcol) ? "COUNT(s)" : "COUNT(a)",
                        IsStrCol(gcol) ? 2 : 0, AggSpec::Kind::kCount});
      }
      if (g.Chance(60)) {
        aggs.push_back({"SUM(a)", 0, AggSpec::Kind::kSum});
      }
      if (g.Chance(40)) {
        aggs.push_back(
            {"MIN(" + ColName(gcol) + ")", gcol, AggSpec::Kind::kMin});
      }
      if (g.Chance(40)) {
        aggs.push_back(
            {"MAX(" + ColName(gcol) + ")", gcol, AggSpec::Kind::kMax});
      }
      std::string agg_sql;
      for (size_t i = 0; i < aggs.size(); ++i) {
        agg_sql += (i == 0 ? "" : ", ") + aggs[i].sql;
      }
      sql = "SELECT " + ColName(gcol) + ", " + agg_sql + " FROM t";
      WPredPtr where;
      if (g.Chance(50)) {
        where = GenWhere(g, g.Pick(1, 2));
        sql += " WHERE " + where->Render();
      }
      sql += " GROUP BY " + ColName(gcol) + ";";

      // Mirror: group the surviving rows.  The map key is internal; the
      // rendered group label follows the column's value formatting.
      std::map<std::string, std::vector<MRow>> groups;
      for (const MRow& r : mirror) {
        if (where != nullptr && where->Eval(r) != 'T') {
          continue;
        }
        groups[GroupKey(gcol, r)].push_back(r);
      }
      for (auto& [key, rows] : groups) {
        const std::string label =
            IsStrCol(gcol) ? FmtStr(rows[0].s) : FmtInt(CellOf(rows[0], gcol));
        std::string row = label + ",";
        for (const AggSpec& spec : aggs) {
          row += ComputeAgg(spec, rows) + ",";
        }
        expected.push_back(row);
      }
      std::sort(expected.begin(), expected.end());
      if (stats != nullptr) {
        ++stats->group_queries;
      }
    } else {
      // Window query; ORDER BY u is unique so ties are impossible except for
      // RANK over a, which the mirror handles with peers.
      const int mode = g.Pick(0, 3);
      const int pcol = g.Pick(0, 1);  // partition by a or b (nullable)
      WPredPtr where;
      std::string where_sql;
      if (g.Chance(40)) {
        where = GenWhere(g, 1);
        where_sql = " WHERE " + where->Render();
      }
      switch (mode) {
        case 0:
          sql = "SELECT u, ROW_NUMBER() OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u) FROM t" + where_sql + ";";
          break;
        case 1:
          sql = "SELECT u, RANK() OVER (ORDER BY a) FROM t" + where_sql + ";";
          break;
        case 2:
          sql = "SELECT u, COUNT(*) OVER (PARTITION BY " + ColName(pcol) +
                ") FROM t" + where_sql + ";";
          break;
        default:
          sql = "SELECT u, SUM(a) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u) FROM t" + where_sql + ";";
          break;
      }

      std::vector<const MRow*> selected;
      for (const MRow& r : mirror) {
        if (where == nullptr || where->Eval(r) == 'T') {
          selected.push_back(&r);
        }
      }
      for (const MRow* r : selected) {
        std::string row = FmtInt(r->u) + ",";
        if (mode == 0) {
          int64_t rn = 0;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) == CellOf(*r, pcol) && o->u <= r->u) {
              ++rn;
            }
          }
          row += FmtInt(rn) + ",";
        } else if (mode == 1) {
          int64_t rank = 1;
          for (const MRow* o : selected) {
            const int64_t va = o->a == kNullRepr ? kNullRepr : o->a;
            const int64_t vb = r->a == kNullRepr ? kNullRepr : r->a;
            const bool o_less = va == kNullRepr
                                    ? vb != kNullRepr
                                    : (vb == kNullRepr ? false : va < vb);
            if (o_less) {
              ++rank;
            }
          }
          row += FmtInt(rank) + ",";
        } else if (mode == 2) {
          int64_t n = 0;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) == CellOf(*r, pcol)) {
              ++n;
            }
          }
          row += FmtInt(n) + ",";
        } else {
          int64_t sum = 0;
          bool any = false;
          for (const MRow* o : selected) {
            if (o->u <= r->u && CellOf(*o, pcol) == CellOf(*r, pcol) &&
                o->a != kNullRepr) {
              sum += o->a;
              any = true;
            }
          }
          row += (any ? FmtInt(sum) : std::string("NULL")) + ",";
        }
        expected.push_back(row);
      }
      std::sort(expected.begin(), expected.end());
      if (stats != nullptr) {
        ++stats->window_queries;
      }
    }

    std::string error;
    auto got = RunQuery(db, ctx, sql, &error);
    if (!got.has_value()) {
      if (verbose) {
        std::cerr << "[agg_fuzz][skip-query] " << sql << " :: " << error
                  << "\n";
      }
      continue;
    }
    const std::vector<std::string> actual = FormatRows(*got);
    t.queries.push_back({sql, expected});
    if (actual == expected) {
      continue;
    }
    report += "[AGGREGATE MISMATCH] " + sql + "\n";
    report += "  expected (" + std::to_string(expected.size()) + "):\n";
    for (const std::string& r : expected) {
      report += "    " + r + "\n";
    }
    report += "  actual (" + std::to_string(actual.size()) + "):\n";
    for (const std::string& r : actual) {
      report += "    " + r + "\n";
    }
    return report;
  }
  return report;
}

std::string ReplayAggregateTrace(const AggTrace& trace, bool verbose) {
  if (trace.setup.empty()) {
    return "malformed trace: no setup";
  }
  ScopedDb db_owner("sql_aggregate_replay");
  Database& db = *db_owner;
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
  for (const std::string& sql : trace.setup) {
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    // QueryResults are lazy: drain or the INSERT never happens.
    if (!result.HasValue()) {
      return "setup failed: " + sql;
    }
    result.Value().Drain();
  }
  for (const AggQueryExpect& q : trace.queries) {
    std::string error;
    auto got = RunQuery(db, ctx, q.sql, &error);
    if (!got.has_value()) {
      return "query failed: " + q.sql + " :: " + error;
    }
    std::vector<std::string> actual = FormatRows(*got);
    std::vector<std::string> expected = q.expected;
    std::sort(expected.begin(), expected.end());
    if (actual != expected) {
      std::string report = "[AGGREGATE REPLAY MISMATCH] " + q.sql + "\n";
      report += "  expected:\n";
      for (const std::string& r : expected) {
        report += "    " + r + "\n";
      }
      report += "  actual:\n";
      for (const std::string& r : actual) {
        report += "    " + r + "\n";
      }
      return report;
    }
    if (verbose) {
      std::cerr << "[agg_fuzz][replay-ok] " << q.sql << "\n";
    }
  }
  return "";
}

std::string SerializeAggregateTest(uint64_t seed, const AggTrace& trace,
                                   const std::string& failure_summary) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  for (const std::string& sql : trace.setup) {
    out += "-- setup: " + sql + "\n";
  }
  for (const AggQueryExpect& q : trace.queries) {
    out += "-- query: " + q.sql + "\n";
    for (const std::string& row : q.expected) {
      out += "-- expect: " + row + "\n";
    }
  }
  if (!failure_summary.empty()) {
    out += "-- failure: " + failure_summary + "\n";
  }
  return out;
}

bool ParseAggregateTest(std::string_view text, uint64_t* seed, AggTrace* trace,
                        std::string* failure_summary) {
  *seed = 0;
  *trace = AggTrace{};
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
    if (line.starts_with("-- tinylamb-aggregate-test")) {
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
      trace->queries.push_back({value, {}});
    } else if (consume("-- expect: ", &value)) {
      if (trace->queries.empty()) {
        return false;
      }
      trace->queries.back().expected.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;
    }
  }
  return saw_header && !trace->setup.empty();
}

void AggregateFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x77cfae3b91d2f60aULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
  AggTrace trace;
  std::string report = RunAggregateIteration(rng, verbose, nullptr, &trace);
  if (report.empty()) {
    return;
  }
  const std::string text = SerializeAggregateTest(
      seed, trace, "auto-generated by sql_aggregate_fuzzer");
  uint64_t parsed_seed = 0;
  AggTrace parsed;
  std::string summary;
  std::string verify;
  if (!ParseAggregateTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized aggregate test does not parse";
  } else {
    verify = ReplayAggregateTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized aggregate replay holds (flaky)";
    }
  }
  const std::string path =
      "sql_aggregate_fuzz-repro-" + std::to_string(seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();
  std::cerr << "[sql_aggregate_fuzz] seed=" << seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
