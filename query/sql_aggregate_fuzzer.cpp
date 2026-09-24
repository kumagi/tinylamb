/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_aggregate_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <cmath>
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
#include "type/value.hpp"

namespace tinylamb {
namespace {

constexpr const char* kTestHeader = "-- tinylamb-aggregate-test v1";
constexpr int64_t kNullRepr = INT64_MIN;
constexpr std::array<std::string_view, 6> kCmpOps = {"=",  "!=", "<",
                                                     "<=", ">",  ">="};
// __int128 in a typedef position would trip -Wpedantic under the strict
// warning tier; the extension marker silences it at the single definition.
__extension__ typedef __int128 int128_t;

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

// Mirror row of t(u, a, b, s, f).
struct MRow {
  int64_t u{0};
  int64_t a{kNullRepr};
  int64_t b{kNullRepr};
  int64_t s{kNullRepr};  // 0 = 'x', 1 = 'yy'
  double f{0.0};
  bool f_null{true};
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
  // QueryResult is lazy: a mid-stream error is not a rejected query but a
  // truncated result.  Generated SELECTs cannot legitimately fail, so mark
  // it distinctly — the caller reports instead of skipping.
  if (Status st = result.Value().GetStatus(); st != Status::kSuccess) {
    *error = "stream-error: " + (st.GetMessage().empty()
                                     ? std::string(ToString(st.GetCode()))
                                     : std::string(st.GetMessage()));
    return std::nullopt;
  }
  return rows;
}

constexpr const char* kDdl =
    "CREATE TABLE t (u INT64, a INT64, b INT64, s VARCHAR(8), f DOUBLE);";

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

// DOUBLE sums differ in their last ulps depending on accumulation order
// (mirror sums rows sequentially; the engine folds partial-aggregate
// trees). A string-equal comparison would flag those as wrong results,
// so mismatch candidates get a numeric second look: cells are split on
// the "|...|," format, finite doubles compare with a relative epsilon,
// and rows are matched as a multiset.
bool CellAlmostEqual(const std::string& lhs, const std::string& rhs) {
  if (lhs == rhs) {
    return true;
  }
  const auto parse = [](const std::string& cell) -> std::optional<double> {
    if (cell.size() < 3 || cell.front() != '|' || cell.back() != '|') {
      return std::nullopt;
    }
    const std::string body = cell.substr(1, cell.size() - 2);
    if (body.find_first_of("eE") == std::string::npos &&
        body.find('.') == std::string::npos) {
      return std::nullopt;
    }
    size_t pos = 0;
    try {
      const double v = std::stod(body, &pos);
      if (pos != body.size() || !std::isfinite(v)) {
        return std::nullopt;
      }
      return v;
    } catch (...) {
      return std::nullopt;
    }
  };
  const std::optional<double> a = parse(lhs);
  const std::optional<double> b = parse(rhs);
  if (!a.has_value() || !b.has_value()) {
    return false;
  }
  const double scale = std::max({1.0, std::abs(*a), std::abs(*b)});
  return std::abs(*a - *b) <= 1e-9 * scale;
}

bool RowAlmostEqual(const std::string& lhs, const std::string& rhs) {
  // A formatted row is a run of "NULL," or "|body|," cells.
  const auto cells = [](const std::string& row) {
    std::vector<std::string> out;
    size_t i = 0;
    while (i < row.size()) {
      if (row[i] == '|') {
        const size_t j = row.find('|', i + 1);
        if (j == std::string::npos) {
          out.push_back(row.substr(i));
          break;
        }
        out.push_back(row.substr(i, j - i + 1));
        i = j + 1;
      } else if (row.compare(i, 4, "NULL") == 0) {
        out.emplace_back("NULL");
        i += 4;
      } else {
        out.push_back(row.substr(i));
        break;
      }
      if (i < row.size() && row[i] == ',') {
        ++i;
      }
    }
    return out;
  };
  const std::vector<std::string> a = cells(lhs);
  const std::vector<std::string> b = cells(rhs);
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (!CellAlmostEqual(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

bool RowsAlmostEqual(const std::vector<std::string>& actual,
                     const std::vector<std::string>& expected) {
  if (actual.size() != expected.size()) {
    return false;
  }
  std::vector<bool> used(expected.size(), false);
  for (const std::string& row : actual) {
    bool matched = false;
    for (size_t j = 0; j < expected.size(); ++j) {
      if (!used[j] && RowAlmostEqual(row, expected[j])) {
        used[j] = true;
        matched = true;
        break;
      }
    }
    if (!matched) {
      return false;
    }
  }
  return true;
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

// Column 3 is the DOUBLE column `f` (aggregates only; never a group key or
// a WPred target).
bool IsDblCol(int col) { return col == 3; }
std::string FmtDbl(double v) { return "|" + FormatDoubleShortest(v) + "|"; }

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
    p->op = std::string(kCmpOps[static_cast<size_t>(g.Pick(0, 5))]);
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
  enum Kind {
    kCountStar,
    kCount,
    kSum,
    kMin,
    kMax,
    kCountDistinct,
    kSumDistinct,
    kAvg,
    kAvgDistinct,
    kCountIf,
    kBitAnd,
    kBitOr,
    kBitXor,
    kLogicalAnd,
    kLogicalOr,
    kStringAgg,
    kArrayAgg,
    kVarPop,
    kVarSamp,
    kStddevPop,
    kStddevSamp
  } kind{kCountStar};
  // Optional inline-WHERE predicate (GoogleSQL `AGG(arg WHERE cond)`);
  // only rows evaluating TRUE feed the aggregate.
  std::shared_ptr<const WPred> filter{nullptr};
  WPredPtr cond{nullptr};  // kCountIf / kLogicalAnd / kLogicalOr only
  size_t where_pos{0};     // sql index where " WHERE cond" is inserted
  bool distinct{false};    // kArrayAgg only
  bool order_desc{false};  // inner ORDER BY u direction
  int inner_limit{0};      // inner LIMIT (0 = none)
  std::string delim{","};  // kStringAgg only
  // Optional HAVING MIN/MAX modifier: gates the group to rows whose
  // condition column equals the group's extremum (NULL conditions drop out).
  int having_col{-1};  // -1 = no modifier; otherwise int column index
  bool having_min{false};
};

// Rows of `input` satisfying the spec's inline-WHERE clause (all of them
// when unset), then gated by the HAVING MIN/MAX extremum when present.
std::vector<MRow> FilteredRows(const AggSpec& spec,
                               const std::vector<MRow>& input) {
  std::vector<MRow> out;
  for (const MRow& r : input) {
    if (!spec.filter || spec.filter->Eval(r) == 'T') {
      out.push_back(r);
    }
  }
  if (spec.having_col >= 0) {
    bool any = false;
    int64_t extremum = 0;
    for (const MRow& r : out) {
      const int64_t c = CellOf(r, spec.having_col);
      if (c == kNullRepr) {
        continue;
      }
      if (!any || (spec.having_min ? c < extremum : extremum < c)) {
        extremum = c;
        any = true;
      }
    }
    std::vector<MRow> gated;
    for (const MRow& r : out) {
      if (any && CellOf(r, spec.having_col) == extremum) {
        gated.push_back(r);
      }
    }
    out = std::move(gated);
  }
  return out;
}

// FilteredRows plus the inner ORDER BY u [DESC] / LIMIT n modifiers.  Rows
// arrive in ascending u order, so DESC is a reversal; LIMIT then counts
// ordered rows (NULLs included) before aggregate-specific null handling.
std::vector<MRow> PreparedRows(const AggSpec& spec,
                               const std::vector<MRow>& input) {
  std::vector<MRow> rows = FilteredRows(spec, input);
  if (spec.order_desc) {
    std::reverse(rows.begin(), rows.end());
  }
  if (spec.inner_limit > 0 &&
      rows.size() > static_cast<size_t>(spec.inner_limit)) {
    rows.resize(static_cast<size_t>(spec.inner_limit));
  }
  return rows;
}

// Numeric value of an aggregate over a group, when it is one (COUNT/SUM);
// used by the HAVING mirror.
std::optional<int64_t> AggNum(const AggSpec& spec,
                              const std::vector<MRow>& input) {
  const std::vector<MRow> rows = PreparedRows(spec, input);
  switch (spec.kind) {
    case AggSpec::Kind::kCountStar:
      return static_cast<int64_t>(rows.size());
    case AggSpec::Kind::kCount: {
      int64_t n = 0;
      for (const MRow& r : rows) {
        if (IsDblCol(spec.col) ? !r.f_null : CellOf(r, spec.col) != kNullRepr) {
          ++n;
        }
      }
      return n;
    }
    case AggSpec::Kind::kSum:
    case AggSpec::Kind::kSumDistinct: {
      if (IsDblCol(spec.col)) {
        return std::nullopt;  // double sums never feed HAVING comparisons
      }
      bool any = false;
      int128_t sum = 0;  // extremes must not UB the mirror
      std::vector<int64_t> seen;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (v == kNullRepr ||
            (spec.kind == AggSpec::Kind::kSumDistinct &&
             std::find(seen.begin(), seen.end(), v) != seen.end())) {
          continue;
        }
        seen.push_back(v);
        any = true;
        sum += v;
      }
      if (!any) {
        return std::nullopt;
      }
      // Out-of-int64 results are modeled as NULL: an engine that errors
      // skips the query anyway, and a wrapped result mismatches loudly.
      if (sum > INT64_MAX || sum < INT64_MIN) {
        return std::nullopt;
      }
      return static_cast<int64_t>(sum);
    }
    case AggSpec::Kind::kCountIf: {
      int64_t n = 0;
      for (const MRow& r : rows) {
        if (spec.cond->Eval(r) == 'T') {
          ++n;
        }
      }
      return n;
    }
    default:
      return std::nullopt;
  }
}

std::string ComputeAgg(const AggSpec& spec, const std::vector<MRow>& input) {
  const std::vector<MRow> rows = PreparedRows(spec, input);
  switch (spec.kind) {
    case AggSpec::Kind::kCountStar:
      return FmtInt(static_cast<int64_t>(rows.size()));
    case AggSpec::Kind::kCount: {
      int64_t n = 0;
      for (const MRow& r : rows) {
        if (IsDblCol(spec.col) ? !r.f_null : CellOf(r, spec.col) != kNullRepr) {
          ++n;
        }
      }
      return FmtInt(n);
    }
    case AggSpec::Kind::kCountIf: {
      int64_t n = 0;
      for (const MRow& r : rows) {
        if (spec.cond->Eval(r) == 'T') {
          ++n;
        }
      }
      return FmtInt(n);
    }
    case AggSpec::Kind::kSum:
    case AggSpec::Kind::kSumDistinct: {
      if (IsDblCol(spec.col)) {
        bool any = false;
        double sum = 0.0;
        std::vector<double> seen;
        for (const MRow& r : rows) {
          const double v = r.f;
          if (r.f_null ||
              (spec.kind == AggSpec::Kind::kSumDistinct &&
               std::find(seen.begin(), seen.end(), v) != seen.end())) {
            continue;
          }
          seen.push_back(v);
          any = true;
          sum += v;
        }
        return any ? FmtDbl(sum) : std::string("NULL");
      }
      const std::optional<int64_t> sum = AggNum(spec, rows);
      // INT64_MIN is a legitimate sum but collides with the kNullRepr
      // sentinel inside FmtInt; format past the sentinel here.
      return sum.has_value() ? "|" + std::to_string(*sum) + "|"
                             : std::string("NULL");
    }
    case AggSpec::Kind::kCountDistinct: {
      std::vector<int64_t> seen;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (v != kNullRepr &&
            std::find(seen.begin(), seen.end(), v) == seen.end()) {
          seen.push_back(v);
        }
      }
      return FmtInt(static_cast<int64_t>(seen.size()));
    }
    case AggSpec::Kind::kMin:
    case AggSpec::Kind::kMax: {
      if (IsDblCol(spec.col)) {
        bool any = false;
        double best = 0.0;
        for (const MRow& r : rows) {
          if (r.f_null) {
            continue;
          }
          const double v = r.f;
          if (!any) {
            best = v;
            any = true;
          } else if (spec.kind == AggSpec::Kind::kMin ? v < best : v > best) {
            best = v;
          }
        }
        return any ? FmtDbl(best) : std::string("NULL");
      }
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
    case AggSpec::Kind::kAvg:
    case AggSpec::Kind::kAvgDistinct: {
      // AVG yields DOUBLE: the engine accumulates in double, so huge sums
      // round rather than overflow-error like SUM(int64) does.
      double sum = 0;
      int64_t cnt = 0;
      std::vector<int64_t> seen;
      std::vector<double> fseen;
      for (const MRow& r : rows) {
        if (IsDblCol(spec.col)) {
          const double v = r.f;
          if (r.f_null ||
              (spec.kind == AggSpec::Kind::kAvgDistinct &&
               std::find(fseen.begin(), fseen.end(), v) != fseen.end())) {
            continue;
          }
          fseen.push_back(v);
          sum += v;
          ++cnt;
          continue;
        }
        const int64_t v = CellOf(r, spec.col);
        if (v == kNullRepr ||
            (spec.kind == AggSpec::Kind::kAvgDistinct &&
             std::find(seen.begin(), seen.end(), v) != seen.end())) {
          continue;
        }
        seen.push_back(v);
        sum += static_cast<double>(v);
        ++cnt;
      }
      if (cnt == 0) {
        return {"NULL"};
      }
      return {"|" + FormatDoubleShortest(sum / static_cast<double>(cnt)) +
              "|"};
    }
    case AggSpec::Kind::kBitAnd:
    case AggSpec::Kind::kBitOr:
    case AggSpec::Kind::kBitXor: {
      // NULLs are skipped; an all-NULL or empty group yields NULL.
      bool any = false;
      int64_t acc = 0;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (v == kNullRepr) {
          continue;
        }
        if (!any) {
          acc = v;
          any = true;
        } else if (spec.kind == AggSpec::Kind::kBitAnd) {
          acc &= v;
        } else if (spec.kind == AggSpec::Kind::kBitOr) {
          acc |= v;
        } else {
          acc ^= v;
        }
      }
      // XOR can legitimately yield INT64_MIN (== kNullRepr); format past
      // the sentinel like SUM does.
      return any ? "|" + std::to_string(acc) + "|" : std::string("NULL");
    }
    case AggSpec::Kind::kLogicalAnd:
    case AggSpec::Kind::kLogicalOr: {
      // NULL operands are ignored; no non-NULL input yields NULL.
      bool any = false;
      bool acc = spec.kind == AggSpec::Kind::kLogicalAnd;
      for (const MRow& r : rows) {
        const char v = spec.cond->Eval(r);
        if (v == 'N') {
          continue;
        }
        const bool b = v == 'T';
        if (!any) {
          acc = b;
          any = true;
        } else {
          acc =
              spec.kind == AggSpec::Kind::kLogicalAnd ? (acc && b) : (acc || b);
        }
      }
      return any ? FmtInt(acc ? 1 : 0) : std::string("NULL");
    }
    case AggSpec::Kind::kStringAgg: {
      // PreparedRows applied ORDER BY u [DESC] then LIMIT (counting rows,
      // NULLs included); NULL elements are skipped at emit time.  Empty
      // input yields NULL.
      int emitted = 0;
      std::string out;
      for (const MRow& r : rows) {
        if (r.s == kNullRepr) {
          continue;
        }
        if (emitted++ != 0) {
          out += spec.delim;
        }
        out += (r.s == 0 ? "x" : "yy");
      }
      if (emitted == 0) {
        return {"NULL"};
      }
      return {"|\"" + out + "\"|"};
    }
    case AggSpec::Kind::kArrayAgg: {
      // NULL elements are kept; DISTINCT collapses them to one.
      // PreparedRows applied ORDER BY u [DESC] then LIMIT (counting ordered
      // rows before NULL/dedup handling).
      int emitted = 0;
      std::string out;
      std::vector<int64_t> seen;
      bool saw_null = false;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (spec.distinct) {
          if (v == kNullRepr) {
            if (saw_null) {
              continue;
            }
            saw_null = true;
          } else if (std::find(seen.begin(), seen.end(), v) != seen.end()) {
            continue;
          } else {
            seen.push_back(v);
          }
        }
        out += (emitted++ == 0 ? "" : ", ");
        out += (v == kNullRepr ? "NULL" : std::to_string(v));
      }
      if (emitted == 0) {
        return {"NULL"};
      }
      return {"|ARRAY<INT64>[" + out + "]|"};
    }
    case AggSpec::Kind::kVarPop:
    case AggSpec::Kind::kVarSamp:
    case AggSpec::Kind::kStddevPop:
    case AggSpec::Kind::kStddevSamp: {
      // Bit-exact mirror of the engine accumulator: long double sx/sxx.
      // Generation restricts the column to the small int domain so the
      // sums are exact regardless of morsel ordering.
      const bool samp = spec.kind == AggSpec::Kind::kVarSamp ||
                        spec.kind == AggSpec::Kind::kStddevSamp;
      long double sx = 0.0L, sxx = 0.0L;
      int64_t n = 0;
      for (const MRow& r : rows) {
        const int64_t v = CellOf(r, spec.col);
        if (v == kNullRepr) {
          continue;
        }
        const auto x = static_cast<long double>(v);
        sx += x;
        sxx += x * x;
        ++n;
      }
      if (n < (samp ? 2 : 1)) {
        return {"NULL"};
      }
      const long double mean = sx / static_cast<long double>(n);
      const long double ssd = sxx - (static_cast<long double>(n) * mean * mean);
      long double res = ssd / static_cast<long double>(samp ? n - 1 : n);
      if (spec.kind == AggSpec::Kind::kStddevPop ||
          spec.kind == AggSpec::Kind::kStddevSamp) {
        res = std::sqrt(res);
      }
      return FmtDbl(static_cast<double>(res));
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
  // ~12% of rounds cross the morsel/parallel-aggregation threshold; the
  // small-table domain (-3..3) is kept so group counts stay meaningful.
  const bool large = g.Chance(12);
  const int row_count = large ? g.Pick(8300, 8450) : g.Pick(5, 12);
  std::string batch;
  int pending = 0;
  // Per-column flag for INT64 extremes: VAR_*/STDDEV_* accumulation order
  // is only bit-exact when every term stays inside the long-double
  // mantissa, so those aggregates are generated only for extreme-free
  // columns.
  std::array<bool, 2> col_extreme{false, false};
  for (int i = 0; i < row_count; ++i) {
    MRow r;
    r.u = i;
    r.a = g.Chance(25) ? kNullRepr : g.Pick(-3, 3);
    r.b = g.Chance(25) ? kNullRepr : g.Pick(-2, 2);
    // INT64 extremes stress SUM/running aggregates (never kNullRepr).
    if (g.Chance(8)) {
      r.a = g.Chance(50) ? INT64_MAX : INT64_MIN + 1;
      col_extreme[0] = true;
    }
    if (g.Chance(8)) {
      r.b = g.Chance(50) ? INT64_MAX : INT64_MIN + 1;
      col_extreme[1] = true;
    }
    r.s = g.Chance(15) ? kNullRepr : g.Pick(0, 1);
    r.f_null = g.Chance(25);
    if (!r.f_null) {
      static constexpr std::array<double, 10> kFPool = {
          0.5, -1.5, 2.25, -3.0, 100.5, 1e-300, 1e17, 3.7, -0.25, 7.0};
      r.f = g.Chance(8) ? (g.Chance(50) ? 1e300 : -1e300)
                        : kFPool[static_cast<size_t>(g.Pick(0, 9))];
    }
    mirror.push_back(r);
    const std::string tuple =
        "(" + std::to_string(r.u) + ", " + CellSql(0, r.a) + ", " +
        CellSql(1, r.b) + ", " + CellSql(2, r.s) + ", " +
        (r.f_null ? std::string("NULL") : FormatDoubleShortest(r.f)) + ")";
    if (!large) {
      t.setup.push_back("INSERT INTO t VALUES " + tuple + ";");
      continue;
    }
    batch += (pending == 0 ? "" : ", ") + tuple;
    if (++pending == 200) {
      t.setup.push_back("INSERT INTO t VALUES " + batch + ";");
      batch.clear();
      pending = 0;
    }
  }
  if (pending != 0) {
    t.setup.push_back("INSERT INTO t VALUES " + batch + ";");
  }

  // ScopedDb deletes the throwaway .db/.log pair when the iteration ends.
  ScopedDb db_owner("sql_aggregate_fuzz");
  if (!db_owner) {
    return "";  // resource pressure: iteration skipped
  }
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
    if (result.Value().GetStatus() != Status::kSuccess) {
      if (verbose) {
        std::cerr << "[agg_fuzz][skip-setup-status] " << sql << "\n";
      }
      return "";
    }
  }

  std::string report;
  const int query_count = g.Pick(2, 5);
  for (int q = 0; q < query_count; ++q) {
    const bool window = !large && g.Chance(45);
    std::string sql;
    std::vector<std::string> expected;

    if (!window) {
      // GROUP BY query over 1-2 key columns (nullable), optionally with
      // DISTINCT aggregates and a HAVING over numeric aggregates.
      // Sometimes expressed as GROUPING SETS over 2-3 distinct subsets.
      std::vector<int> gcols{g.Pick(0, 2)};
      if (g.Chance(35)) {
        const int second = g.Pick(0, 2);
        if (second != gcols[0]) {
          gcols.push_back(second);
        }
      }
      // Explicit grouping sets (each a distinct subset of the output cols);
      // the first entry is the empty grouping set when enabled and picked.
      std::vector<std::vector<int>> gsets;
      const bool grouping_sets = g.Chance(30);
      std::vector<AggSpec> aggs;
      aggs.push_back({"COUNT(*)", -1, AggSpec::Kind::kCountStar});
      const int gcol0 = gcols[0];
      if (g.Chance(70)) {
        aggs.push_back({IsStrCol(gcol0) ? "COUNT(s)" : "COUNT(a)",
                        IsStrCol(gcol0) ? 2 : 0, AggSpec::Kind::kCount});
      }
      if (g.Chance(60)) {
        aggs.push_back({"SUM(a)", 0, AggSpec::Kind::kSum});
      }
      if (g.Chance(40)) {
        aggs.push_back(
            {"MIN(" + ColName(gcol0) + ")", gcol0, AggSpec::Kind::kMin});
      }
      if (g.Chance(40)) {
        aggs.push_back(
            {"MAX(" + ColName(gcol0) + ")", gcol0, AggSpec::Kind::kMax});
      }
      if (g.Chance(35)) {
        const int dcol = g.Pick(0, 2);
        aggs.push_back({"COUNT(DISTINCT " + ColName(dcol) + ")", dcol,
                        AggSpec::Kind::kCountDistinct});
      }
      if (g.Chance(25)) {
        aggs.push_back({"SUM(DISTINCT a)", 0, AggSpec::Kind::kSumDistinct});
      }
      if (g.Chance(40)) {
        aggs.push_back({"AVG(a)", 0, AggSpec::Kind::kAvg});
      }
      if (g.Chance(20)) {
        aggs.push_back({"AVG(DISTINCT a)", 0, AggSpec::Kind::kAvgDistinct});
      }
      // DOUBLE-column aggregates: float SUM/AVG/MIN/MAX exercise the double
      // accumulator path incl. extreme magnitudes (1e300) and NULLs.
      if (g.Chance(35)) {
        aggs.push_back({"SUM(f)", 3, AggSpec::Kind::kSum});
      }
      if (g.Chance(35)) {
        aggs.push_back({"AVG(f)", 3, AggSpec::Kind::kAvg});
      }
      if (g.Chance(25)) {
        const bool is_min = g.Chance(50);
        aggs.push_back({is_min ? "MIN(f)" : "MAX(f)", 3,
                        is_min ? AggSpec::Kind::kMin : AggSpec::Kind::kMax});
      }
      if (g.Chance(30)) {
        aggs.push_back({"COUNT(f)", 3, AggSpec::Kind::kCount});
      }
      if (g.Chance(30)) {
        // ANY_VALUE normalizes to MIN in the frontend, so the mirror can
        // reuse the kMin computation verbatim.  With a HAVING MIN/MAX
        // modifier this is the argmin/argmax idiom.
        const int acol = g.Pick(0, 2);
        aggs.push_back(
            {"ANY_VALUE(" + ColName(acol) + ")", acol, AggSpec::Kind::kMin});
      }
      if (g.Chance(15)) {
        aggs.push_back({"AVG(DISTINCT f)", 3, AggSpec::Kind::kAvgDistinct});
      }
      if (g.Chance(15)) {
        aggs.push_back({"SUM(DISTINCT f)", 3, AggSpec::Kind::kSumDistinct});
      }
      if (g.Chance(30)) {
        AggSpec ci;
        ci.cond = GenWhere(g, 1);
        ci.sql = "COUNTIF(" + ci.cond->Render() + ")";
        ci.kind = AggSpec::Kind::kCountIf;
        aggs.push_back(std::move(ci));
      }
      if (g.Chance(30)) {
        // Bitwise aggregates over a nullable int column; NULLs skipped.
        const int bcol = g.Pick(0, 1);
        static constexpr std::array<const char*, 3> kBitFn = {
            "BIT_AND", "BIT_OR", "BIT_XOR"};
        static constexpr std::array<AggSpec::Kind, 3> kBitKind = {
            AggSpec::Kind::kBitAnd, AggSpec::Kind::kBitOr,
            AggSpec::Kind::kBitXor};
        const int pick = g.Pick(0, 2);
        aggs.push_back({std::string(kBitFn[static_cast<size_t>(pick)]) + "(" +
                            ColName(bcol) + ")",
                        bcol, kBitKind[static_cast<size_t>(pick)]});
      }
      if (g.Chance(30)) {
        // LOGICAL_AND/OR take a boolean expression; NULL operands are
        // ignored and all-NULL groups yield NULL.
        AggSpec la;
        la.cond = GenWhere(g, 1);
        la.kind = g.Chance(50) ? AggSpec::Kind::kLogicalAnd
                               : AggSpec::Kind::kLogicalOr;
        la.sql = (la.kind == AggSpec::Kind::kLogicalAnd ? "LOGICAL_AND("
                                                        : "LOGICAL_OR(") +
                 la.cond->Render() + ")";
        la.where_pos = la.sql.size() - 1;
        aggs.push_back(std::move(la));
      }
      if (g.Chance(30)) {
        // STRING_AGG ordered by u for determinism, with optional explicit
        // delimiter and inner LIMIT (which counts rows, NULLs included).
        AggSpec sa;
        sa.kind = AggSpec::Kind::kStringAgg;
        sa.col = 2;
        sa.order_desc = g.Chance(25);
        sa.inner_limit = g.Chance(25) ? g.Pick(1, 3) : 0;
        const bool with_delim = g.Chance(70);
        sa.delim = with_delim && g.Chance(40) ? "-" : ",";
        std::string base = "STRING_AGG(s";
        if (with_delim) {
          base += ", '" + sa.delim + "'";
        }
        sa.where_pos = base.size();
        sa.sql =
            base + (sa.order_desc ? " ORDER BY u DESC" : " ORDER BY u") +
            (sa.inner_limit != 0 ? " LIMIT " + std::to_string(sa.inner_limit)
                                 : "") +
            ")";
        aggs.push_back(std::move(sa));
      }
      if (g.Chance(30)) {
        // ARRAY_AGG keeps NULL elements; DISTINCT collapses them to one.
        AggSpec aa;
        aa.kind = AggSpec::Kind::kArrayAgg;
        aa.col = g.Pick(0, 1);
        aa.distinct = g.Chance(35);
        aa.order_desc = g.Chance(25);
        aa.inner_limit = g.Chance(25) ? g.Pick(1, 3) : 0;
        std::string base =
            "ARRAY_AGG(" +
            (aa.distinct ? std::string("DISTINCT ") : std::string()) +
            ColName(aa.col);
        aa.where_pos = base.size();
        aa.sql =
            base + (aa.order_desc ? " ORDER BY u DESC" : " ORDER BY u") +
            (aa.inner_limit != 0 ? " LIMIT " + std::to_string(aa.inner_limit)
                                 : "") +
            ")";
        aggs.push_back(std::move(aa));
      }
      if (g.Chance(30)) {
        // VAR_*/STDDEV_* only over extreme-free int columns: the mirror
        // reproduces the long-double sx/sxx accumulator bit-exactly only
        // when every term stays inside its mantissa.
        std::vector<int> ok_cols;
        for (int c = 0; c < 2; ++c) {
          if (!col_extreme[static_cast<size_t>(c)]) {
            ok_cols.push_back(c);
          }
        }
        if (!ok_cols.empty()) {
          const int vcol = ok_cols[static_cast<size_t>(
              g.Pick(0, static_cast<int>(ok_cols.size()) - 1))];
          static constexpr std::array<const char*, 4> kVarFn = {
              "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP"};
          static constexpr std::array<AggSpec::Kind, 4> kVarKind = {
              AggSpec::Kind::kVarPop, AggSpec::Kind::kVarSamp,
              AggSpec::Kind::kStddevPop, AggSpec::Kind::kStddevSamp};
          const int pick = g.Pick(0, 3);
          aggs.push_back({std::string(kVarFn[static_cast<size_t>(pick)]) + "(" +
                              ColName(vcol) + ")",
                          vcol, kVarKind[static_cast<size_t>(pick)]});
        }
      }
      // Inline WHERE (GoogleSQL `AGG(arg WHERE cond)`), HAVING MIN/MAX,
      // and inner ORDER BY u/LIMIT modifiers.  Grammar order is
      // `AGG(arg [WHERE c] [HAVING {MIN|MAX} e] [ORDER BY ...] [LIMIT n])`,
      // so all insert at the same anchor: before ORDER BY/LIMIT when
      // `where_pos` marks one, else before the closing paren.
      for (AggSpec& spec : aggs) {
        size_t pos = spec.where_pos != 0 ? spec.where_pos : spec.sql.size() - 1;
        if (spec.kind != AggSpec::Kind::kCountIf && g.Chance(25)) {
          // COUNTIF already carries its own condition.
          WPredPtr f = GenWhere(g, 1);
          const std::string text = " WHERE " + f->Render();
          spec.sql.insert(pos, text);
          pos += text.size();
          spec.filter = std::shared_ptr<const WPred>(std::move(f));
        }
        // COUNT(*) has no argument to gate on; COUNTIF and LOGICAL_AND/OR
        // carry a predicate expression rather than a plain column argument.
        if (spec.kind != AggSpec::Kind::kCountStar &&
            spec.kind != AggSpec::Kind::kCountIf &&
            spec.kind != AggSpec::Kind::kLogicalAnd &&
            spec.kind != AggSpec::Kind::kLogicalOr && g.Chance(20)) {
          spec.having_col = g.Pick(0, 2);
          spec.having_min = g.Chance(50);
          const std::string text = std::string(" HAVING ") +
                                   (spec.having_min ? "MIN " : "MAX ") +
                                   ColName(spec.having_col);
          spec.sql.insert(pos, text);
          pos += text.size();
        }
        // STRING_AGG/ARRAY_AGG already carry ORDER BY/LIMIT in `sql`.
        if (spec.kind != AggSpec::Kind::kStringAgg &&
            spec.kind != AggSpec::Kind::kArrayAgg && g.Chance(15)) {
          spec.order_desc = g.Chance(30);
          std::string text =
              spec.order_desc ? " ORDER BY u DESC" : " ORDER BY u";
          if (g.Chance(60)) {
            spec.inner_limit = g.Pick(1, 3);
            text += " LIMIT " + std::to_string(spec.inner_limit);
          }
          spec.sql.insert(pos, text);
        }
      }
      // HAVING: 1-2 conditions over numeric aggregates (COUNT(*)/SUM(a),
      // incl. IS [NOT] NULL for SUM over all-NULL groups), AND/OR joined.
      struct HavingCond {
        AggSpec spec;
        std::string op;  // "=", "<", ... or "IS NULL"/"IS NOT NULL"
        int64_t rhs{0};
      };
      std::vector<HavingCond> having;
      std::string having_join;
      if (g.Chance(40)) {
        const int ncond = g.Chance(40) ? 2 : 1;
        for (int i = 0; i < ncond; ++i) {
          HavingCond c;
          if (g.Chance(30)) {
            c.spec = {.sql = "SUM(a)", .col = 0, .kind = AggSpec::Kind::kSum};
            c.op = g.Chance(50) ? "IS NULL" : "IS NOT NULL";
          } else if (g.Chance(50)) {
            c.spec = {.sql = "COUNT(*)",
                      .col = -1,
                      .kind = AggSpec::Kind::kCountStar};
            c.op = std::string(kCmpOps[static_cast<size_t>(g.Pick(0, 5))]);
            c.rhs = g.Pick(0, 4);
          } else {
            c.spec = {.sql = "SUM(a)", .col = 0, .kind = AggSpec::Kind::kSum};
            c.op = std::string(kCmpOps[static_cast<size_t>(g.Pick(0, 5))]);
            c.rhs = g.Pick(-4, 4);
          }
          if (!c.op.starts_with("IS") && g.Chance(20)) {
            WPredPtr f = GenWhere(g, 1);
            c.spec.sql.insert(c.spec.sql.size() - 1, " WHERE " + f->Render());
            c.spec.filter = std::shared_ptr<const WPred>(std::move(f));
          }
          having.push_back(std::move(c));
        }
        having_join = g.Chance(65) ? "AND" : "OR";
      }
      std::string agg_sql;
      for (size_t i = 0; i < aggs.size(); ++i) {
        agg_sql += (i == 0 ? "" : ", ") + aggs[i].sql;
      }
      sql = "SELECT ";
      std::string group_sql;
      for (size_t i = 0; i < gcols.size(); ++i) {
        sql += ColName(gcols[i]) + ", ";
        group_sql += (i == 0 ? "" : ", ") + ColName(gcols[i]);
      }
      sql += agg_sql + " FROM t";
      WPredPtr where;
      if (g.Chance(50)) {
        where = GenWhere(g, g.Pick(1, 2));
        sql += " WHERE " + where->Render();
      }
      if (grouping_sets) {
        // Choose 2-3 distinct subsets of the output key columns (incl.
        // the grand-total set).
        std::vector<std::vector<int>> all_subsets;
        all_subsets.reserve(gcols.size());
        for (int c : gcols) {
          all_subsets.push_back({c});
        }
        all_subsets.emplace_back();  // grand total
        if (gcols.size() == 2) {
          all_subsets.push_back(gcols);
        }
        std::vector<int> picked;
        const int nsets = g.Pick(2, static_cast<int>(all_subsets.size()));
        for (int i = 0; i < nsets; ++i) {
          int idx = g.Pick(0, static_cast<int>(all_subsets.size()) - 1);
          while (std::find(picked.begin(), picked.end(), idx) != picked.end()) {
            idx = g.Pick(0, static_cast<int>(all_subsets.size()) - 1);
          }
          picked.push_back(idx);
          gsets.push_back(all_subsets[static_cast<size_t>(idx)]);
        }
        // Every selected key column must appear in at least one grouping
        // set: a column outside the union of sets has no defined value
        // (the engine leniently reports an arbitrary group member, which
        // is not deterministic across plans). Append its singleton set so
        // the oracle stays decidable.
        for (const int c : gcols) {
          const bool covered =
              std::ranges::any_of(gsets, [c](const std::vector<int>& gset) {
                return std::find(gset.begin(), gset.end(), c) != gset.end();
              });
          if (!covered) {
            gsets.push_back({c});
          }
        }
        sql += " GROUP BY GROUPING SETS (";
        for (size_t i = 0; i < gsets.size(); ++i) {
          if (i != 0) {
            sql += ", ";
          }
          sql += "(";
          for (size_t j = 0; j < gsets[i].size(); ++j) {
            if (j != 0) {
              sql += ", ";
            }
            sql += ColName(gsets[i][j]);
          }
          sql += ")";
        }
        sql += ")";
      } else {
        gsets.push_back(gcols);
        sql += " GROUP BY " + group_sql;
      }
      if (!having.empty()) {
        sql += " HAVING ";
        for (size_t i = 0; i < having.size(); ++i) {
          if (i != 0) {
            sql += " " + having_join + " ";
          }
          const HavingCond& c = having[i];
          if (c.op.starts_with("IS")) {
            sql += "(" + c.spec.sql + " " + c.op + ")";
          } else {
            sql += "(" + c.spec.sql + " " + c.op + " " + std::to_string(c.rhs) +
                   ")";
          }
        }
      }
      sql += ";";

      // Mirror: for each grouping set, group the surviving rows on that
      // set's columns and apply HAVING; columns outside the set emit NULL.
      // The `()` grouping set yields one row even when nothing survives.
      std::vector<MRow> survivors;
      for (const MRow& r : mirror) {
        if (where != nullptr && where->Eval(r) != 'T') {
          continue;
        }
        survivors.push_back(r);
      }
      std::vector<std::pair<std::vector<int>, std::vector<MRow>>> keyed;
      for (const std::vector<int>& gset : gsets) {
        std::map<std::string, std::vector<MRow>> groups;
        if (gset.empty()) {
          groups[""] = survivors;
        } else {
          for (const MRow& r : survivors) {
            std::string key;
            for (const int c : gset) {
              key += GroupKey(c, r) + "#";
            }
            groups[key].push_back(r);
          }
        }
        for (auto& [key, rows] : groups) {
          keyed.emplace_back(gset, std::move(rows));
        }
      }
      auto having_holds = [&](const std::vector<MRow>& rows) {
        std::vector<bool> flags;
        for (const HavingCond& c : having) {
          const std::optional<int64_t> v = AggNum(c.spec, rows);
          if (c.op == "IS NULL") {
            flags.push_back(!v.has_value());
          } else if (c.op == "IS NOT NULL") {
            flags.push_back(v.has_value());
          } else if (!v.has_value()) {
            flags.push_back(false);  // HAVING comparison with NULL is false
          } else {
            const int64_t l = *v;
            if (c.op == "=") {
              flags.push_back(l == c.rhs);
            } else if (c.op == "!=") {
              flags.push_back(l != c.rhs);
            } else if (c.op == "<") {
              flags.push_back(l < c.rhs);
            } else if (c.op == "<=") {
              flags.push_back(l <= c.rhs);
            } else if (c.op == ">") {
              flags.push_back(l > c.rhs);
            } else {
              flags.push_back(l >= c.rhs);
            }
          }
        }
        bool acc = flags.empty() ? true : flags[0];
        for (size_t i = 1; i < flags.size(); ++i) {
          acc = (having_join == "AND") ? (acc && flags[i]) : (acc || flags[i]);
        }
        return acc;
      };
      for (auto& [gset, rows] : keyed) {
        // Only the `()` grouping set emits a row over empty input; key
        // columns are then all NULL and aggregates are identity values.
        if (rows.empty() && !gset.empty()) {
          continue;
        }
        if (!having_holds(rows)) {
          continue;
        }
        std::string row;
        for (const int c : gcols) {
          const bool in_set =
              std::find(gset.begin(), gset.end(), c) != gset.end();
          row += (in_set ? (IsStrCol(c) ? FmtStr(CellOf(rows[0], c))
                                        : FmtInt(CellOf(rows[0], c)))
                         : "NULL") +
                 ",";
        }
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
      // rank functions over a, which the mirror handles with peers.
      const int mode = g.Pick(0, 12);
      const int pcol = g.Pick(0, 1);  // partition by a or b (nullable)
      WPredPtr where;
      std::string where_sql;
      if (g.Chance(40)) {
        where = GenWhere(g, 1);
        where_sql = " WHERE " + where->Render();
      }
      // ROWS BETWEEN frame params for mode 8; NTILE bucket count for mode 5;
      // direction flag for mode 6 (1 = LAG, -1 = LEAD).
      int frame_lo = 0, frame_hi = 0, ntile_n = 0, lag_lead = 0;
      // Frame-mode aggregate: 0=SUM, 1=COUNT, 2=MIN, 3=MAX over b.
      int wagg = 0;
      static constexpr std::array<const char*, 4> kWagg = {"SUM", "COUNT",
                                                           "MIN", "MAX"};
      // EXCLUDE clause for GROUPS/ROWS-frame modes:
      // 0 = NO OTHERS, 1 = CURRENT ROW, 2 = GROUP, 3 = TIES.
      int exclude = 0;
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
        case 3:
          sql = "SELECT u, SUM(a) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u) FROM t" + where_sql + ";";
          break;
        case 4:
          sql = "SELECT u, DENSE_RANK() OVER (ORDER BY a) FROM t" + where_sql +
                ";";
          break;
        case 5: {
          const int buckets = g.Pick(2, 3);
          ntile_n = buckets;
          sql = "SELECT u, NTILE(" + std::to_string(buckets) +
                ") OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u) FROM t" + where_sql + ";";
          break;
        }
        case 6:
          lag_lead = g.Chance(50) ? 1 : -1;  // 1 = LAG, -1 = LEAD
          sql = "SELECT u, " + std::string(lag_lead == 1 ? "LAG" : "LEAD") +
                "(a, 1) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u) FROM t" + where_sql + ";";
          break;
        case 7:
          sql = "SELECT u, FIRST_VALUE(a) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u) FROM t" + where_sql + ";";
          break;
        case 8: {
          frame_lo = g.Pick(0, 2);
          frame_hi = g.Pick(0, 2);
          wagg = g.Pick(0, 3);
          sql = "SELECT u, " + std::string(kWagg[static_cast<size_t>(wagg)]) +
                "(b) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY u ROWS BETWEEN " + std::to_string(frame_lo) +
                " PRECEDING AND " + std::to_string(frame_hi) +
                " FOLLOWING) FROM t" + where_sql + ";";
          break;
        }
        case 9:
          sql = "SELECT u, NTH_VALUE(b, 2) OVER (PARTITION BY " +
                ColName(pcol) +
                " ORDER BY u ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED "
                "FOLLOWING) FROM t" +
                where_sql + ";";
          break;
        case 10: {
          // GROUPS frame over ORDER BY a (which has peers); optionally
          // EXCLUDE CURRENT ROW / GROUP / TIES.
          frame_lo = g.Pick(0, 2);
          frame_hi = g.Pick(0, 2);
          exclude = g.Pick(0, 3);
          wagg = g.Pick(0, 3);
          static constexpr std::array<const char*, 4> kExclude = {
              "", " EXCLUDE CURRENT ROW", " EXCLUDE GROUP", " EXCLUDE TIES"};
          sql = "SELECT u, " + std::string(kWagg[static_cast<size_t>(wagg)]) +
                "(b) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY a GROUPS BETWEEN " + std::to_string(frame_lo) +
                " PRECEDING AND " + std::to_string(frame_hi) + " FOLLOWING" +
                kExclude[static_cast<size_t>(exclude)] + ") FROM t" +
                where_sql + ";";
          break;
        }
        case 11: {
          // RANGE frame over ORDER BY a with a numeric offset.
          frame_lo = g.Pick(0, 2);
          frame_hi = g.Pick(0, 2);
          wagg = g.Pick(0, 3);
          sql = "SELECT u, " + std::string(kWagg[static_cast<size_t>(wagg)]) +
                "(b) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY a RANGE BETWEEN " + std::to_string(frame_lo) +
                " PRECEDING AND " + std::to_string(frame_hi) +
                " FOLLOWING) FROM t" + where_sql + ";";
          break;
        }
        default: {
          // ROWS frame over ORDER BY a (peers) with an EXCLUDE clause.
          frame_lo = g.Pick(0, 2);
          frame_hi = g.Pick(0, 2);
          exclude = g.Pick(1, 3);
          wagg = g.Pick(0, 3);
          static constexpr std::array<const char*, 4> kExclude = {
              "", " EXCLUDE CURRENT ROW", " EXCLUDE GROUP", " EXCLUDE TIES"};
          sql = "SELECT u, " + std::string(kWagg[static_cast<size_t>(wagg)]) +
                "(b) OVER (PARTITION BY " + ColName(pcol) +
                " ORDER BY a ROWS BETWEEN " + std::to_string(frame_lo) +
                " PRECEDING AND " + std::to_string(frame_hi) + " FOLLOWING" +
                kExclude[static_cast<size_t>(exclude)] + ") FROM t" +
                where_sql + ";";
          break;
        }
      }

      std::vector<const MRow*> selected;
      for (const MRow& r : mirror) {
        if (where == nullptr || where->Eval(r) == 'T') {
          selected.push_back(&r);
        }
      }
      // Frame-aggregate mirror for the SUM(b)-family modes (8, 10-12):
      // render the aggregate over a frame's non-NULL b values.
      auto frame_agg = [&](const std::vector<int64_t>& vals) -> std::string {
        if (vals.empty()) {
          // COUNT over an empty/NULL-only frame is 0, not NULL.
          return wagg == 1 ? "|0|" : "NULL";
        }
        switch (wagg) {
          case 1:
            return "|" + std::to_string(static_cast<int64_t>(vals.size())) +
                   "|";
          case 2:
            return "|" +
                   std::to_string(*std::min_element(vals.begin(), vals.end())) +
                   "|";
          case 3:
            return "|" +
                   std::to_string(*std::max_element(vals.begin(), vals.end())) +
                   "|";
          default: {
            int128_t sum = 0;
            for (const int64_t v : vals) {
              sum += v;
            }
            // An overflowing SUM errors the whole query in the engine; the
            // run skips it, so NULL here is only a "don't care" marker.
            if (sum > INT64_MAX || sum < INT64_MIN) {
              return "NULL";
            }
            return "|" + std::to_string(static_cast<int64_t>(sum)) + "|";
          }
        }
      };
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
        } else if (mode == 3) {
          int128_t sum = 0;
          bool any = false;
          bool overflow = false;
          for (const MRow* o : selected) {
            if (o->u <= r->u && CellOf(*o, pcol) == CellOf(*r, pcol) &&
                o->a != kNullRepr) {
              sum += o->a;
              overflow = overflow || sum > INT64_MAX || sum < INT64_MIN;
              any = true;
            }
          }
          row += (any && !overflow
                      ? "|" + std::to_string(static_cast<int64_t>(sum)) + "|"
                      : std::string("NULL")) +
                 ",";
        } else if (mode == 4) {
          // DENSE_RANK over a: 1 + distinct non-peer values below r->a;
          // NULL sorts first, so every non-NULL outranks it.
          std::vector<int64_t> below;
          for (const MRow* o : selected) {
            const int64_t va = o->a, vb = r->a;
            const bool o_less = va == kNullRepr
                                    ? vb != kNullRepr
                                    : (vb == kNullRepr ? false : va < vb);
            if (o_less &&
                std::find(below.begin(), below.end(), va) == below.end()) {
              below.push_back(va);
            }
          }
          row += FmtInt(static_cast<int64_t>(below.size()) + 1) + ",";
        } else if (mode == 5) {
          // NTILE(n) within partition ordered by u: first (m % n) buckets
          // hold one extra row.
          int64_t pos = 0, seen = 0;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) != CellOf(*r, pcol)) {
              continue;
            }
            if (o->u < r->u) {
              ++pos;
            }
            ++seen;
          }
          const int64_t base = seen / ntile_n, rem = seen % ntile_n;
          const int64_t bucket =
              pos < rem * (base + 1)
                  ? (pos / (base + 1)) + 1
                  : rem + ((pos - rem * (base + 1)) / base) + 1;
          row += FmtInt(bucket) + ",";
        } else if (mode == 6) {
          // LAG(a,1)/LEAD(a,1) within partition ordered by u.
          const MRow* prev = nullptr;
          const MRow* next = nullptr;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) != CellOf(*r, pcol)) {
              continue;
            }
            if (o->u < r->u && (prev == nullptr || o->u > prev->u)) {
              prev = o;
            }
            if (o->u > r->u && (next == nullptr || o->u < next->u)) {
              next = o;
            }
          }
          const MRow* src = lag_lead == 1 ? prev : next;
          row += (src == nullptr ? std::string("NULL") : FmtInt(src->a)) + ",";
        } else if (mode == 7) {
          // FIRST_VALUE(a): a of the partition's first row by u.
          const MRow* first = nullptr;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) == CellOf(*r, pcol) &&
                (first == nullptr || o->u < first->u)) {
              first = o;
            }
          }
          row +=
              (first == nullptr ? std::string("NULL") : FmtInt(first->a)) + ",";
        } else if (mode == 8) {
          // SUM(b) over ROWS BETWEEN frame: positional within partition
          // ordered by u.
          int64_t pos = 0;
          std::vector<const MRow*> part;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) == CellOf(*r, pcol)) {
              part.push_back(o);
            }
          }
          for (const MRow* o : part) {
            if (o->u < r->u) {
              ++pos;
            }
          }
          std::vector<int64_t> vals;
          for (size_t i = 0; i < part.size(); ++i) {
            const auto idx = static_cast<int64_t>(i);
            if (idx < pos - frame_lo || idx > pos + frame_hi ||
                part[i]->b == kNullRepr) {
              continue;
            }
            vals.push_back(part[i]->b);
          }
          row += frame_agg(vals) + ",";
        } else if (mode == 9) {
          // NTH_VALUE(b, 2) with UNBOUNDED frame: b of the partition's
          // second row by u.
          const MRow* first = nullptr;
          const MRow* second = nullptr;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) != CellOf(*r, pcol)) {
              continue;
            }
            if (first == nullptr || o->u < first->u) {
              second = first;
              first = o;
            } else if (second == nullptr || o->u < second->u) {
              second = o;
            }
          }
          row += (second == nullptr ? std::string("NULL") : FmtInt(second->b)) +
                 ",";
        } else {
          // Modes 10-12: SUM(b) over GROUPS/RANGE/ROWS frames ordered by a.
          // Partition sorted by a with NULLs first (ASC default), u as the
          // deterministic tiebreak.
          std::vector<const MRow*> part;
          for (const MRow* o : selected) {
            if (CellOf(*o, pcol) == CellOf(*r, pcol)) {
              part.push_back(o);
            }
          }
          std::sort(part.begin(), part.end(), [](const MRow* x, const MRow* y) {
            const bool xn = x->a == kNullRepr;
            const bool yn = y->a == kNullRepr;
            if (xn != yn) {
              return xn;  // NULLs first
            }
            if (!xn && x->a != y->a) {
              return x->a < y->a;
            }
            return x->u < y->u;
          });
          size_t pos = 0;
          for (size_t i = 0; i < part.size(); ++i) {
            if (part[i]->u == r->u) {
              pos = i;
            }
          }
          // Peer-group id per position: groups of equal a (NULL == NULL).
          std::vector<int> gid(part.size(), 0);
          for (size_t i = 1; i < part.size(); ++i) {
            gid[i] = part[i]->a != part[i - 1]->a ? gid[i - 1] + 1 : gid[i - 1];
          }
          auto excluded_at = [&](size_t i) {
            if (exclude == 1) {
              return i == pos;
            }
            if (exclude == 2) {
              return gid[i] == gid[pos];
            }
            if (exclude == 3) {
              return i != pos && gid[i] == gid[pos];
            }
            return false;
          };
          std::vector<int64_t> vals;
          for (size_t i = 0; i < part.size(); ++i) {
            bool in_frame = false;
            if (mode == 10) {
              // GROUPS BETWEEN lo PRECEDING AND hi FOLLOWING (group units).
              in_frame = gid[i] >= gid[pos] - frame_lo &&
                         gid[i] <= gid[pos] + frame_hi;
            } else if (mode == 11) {
              // RANGE n PRECEDING/FOLLOWING: value distance from a_r.
              if (r->a == kNullRepr) {
                in_frame = part[i]->a == kNullRepr;
              } else {
                in_frame = part[i]->a != kNullRepr &&
                           static_cast<int128_t>(part[i]->a) >=
                               static_cast<int128_t>(r->a) - frame_lo &&
                           static_cast<int128_t>(part[i]->a) <=
                               static_cast<int128_t>(r->a) + frame_hi;
              }
            } else {
              in_frame =
                  i >= pos - std::min<size_t>(pos,
                                              static_cast<size_t>(frame_lo)) &&
                  i <= pos + std::min<size_t>(part.size() - 1 - pos,
                                              static_cast<size_t>(frame_hi));
            }
            if (!in_frame || excluded_at(i) || part[i]->b == kNullRepr) {
              continue;
            }
            vals.push_back(part[i]->b);
          }
          row += frame_agg(vals) + ",";
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
      if (error.starts_with("stream-error")) {
        if (error.find("overflow") != std::string::npos) {
          // INT64 extremes legitimately abort SUM mid-stream; the mirror
          // models the overflow as NULL so a wrapped result still trips.
          continue;
        }
        report += "[AGGREGATE MISMATCH] query aborted mid-stream: ";
        report += sql;
        report += "\n  ";
        report += error;
        report += "\n";
        return report;
      }
      if (verbose) {
        std::cerr << "[agg_fuzz][skip-query] " << sql << " :: " << error
                  << "\n";
      }
      continue;
    }
    const std::vector<std::string> actual = FormatRows(*got);
    t.queries.push_back({sql, expected});
    if (actual == expected || RowsAlmostEqual(actual, expected)) {
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
  if (!db_owner) {
    return "";  // resource pressure: replay skipped, not a mismatch
  }
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
    if (result.Value().GetStatus() != Status::kSuccess) {
      return "setup stream-error: " + sql;
    }
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
    if (actual != expected && !RowsAlmostEqual(actual, expected)) {
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
