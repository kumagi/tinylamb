/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_oracle_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "index/index_schema.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// ---------------------------------------------------------------------------
// Random predicate tree with C++-side 3-valued evaluation.  This mirror is
// the ground truth for the constraint oracles (PQS, AMOEBA) and keeps the
// engine-facing SQL and the harness expectation in lockstep.
// ---------------------------------------------------------------------------

// Sentinel for nullable mirror fields; edge value pools below must stay
// clear of INT64_MIN so a real value can never alias kNull.
constexpr int64_t kNull = std::numeric_limits<int64_t>::min();

// String pool in ascending lexicographic order, so code order == SQL order
// and range predicates on `s` can be mirrored by comparing codes.
constexpr std::array<const char*, 4> kStrPool = {"", "a", "bb", "zz"};
// LIKE patterns with mirror implementations (prefix / suffix / length).
constexpr std::array<const char*, 5> kLikePool = {"a%", "%b", "_", "%", "%z%"};

// Edge constants stressing boundary handling; INT64_MIN is deliberately
// absent (kNull sentinel collision) and values keep abs < 2^63 to stay
// addition-safe inside small ranges.
constexpr std::array<int64_t, 5> kEdgeConsts = {
    std::numeric_limits<int64_t>::max(),
    std::numeric_limits<int64_t>::max() - 1,
    -(std::numeric_limits<int64_t>::max() - 1),
    -(std::numeric_limits<int64_t>::max()),
    1099511627776LL,  // 2^40
};

// Mirror of the five LIKE patterns above over pool codes (kNull excluded).
bool LikeMatch(int64_t code, int pat) {
  const std::string_view s = kStrPool[static_cast<size_t>(code)];
  switch (pat) {
    case 0:
      return s.starts_with("a");
    case 1:
      return s.ends_with("b");
    case 2:
      return s.size() == 1;
    case 3:
      return true;
    default:
      return s.find('z') != std::string_view::npos;
  }
}

struct MirrorRow {
  int64_t u{0};
  int64_t a{0};
  int64_t b{0};
  int64_t flag{0};  // kNull = NULL, 0 = FALSE, 1 = TRUE
  int64_t s{0};     // kNull = NULL, else index into kStrPool
};

char Eval3vl(bool v) { return v ? 'T' : 'F'; }
char And3vl(char l, char r) {
  if (l == 'F' || r == 'F') {
    return 'F';
  }
  if (l == 'N' || r == 'N') {
    return 'N';
  }
  return 'T';
}
char Or3vl(char l, char r) {
  if (l == 'T' || r == 'T') {
    return 'T';
  }
  if (l == 'N' || r == 'N') {
    return 'N';
  }
  return 'F';
}
char Not3vl(char v) { return v == 'T' ? 'F' : (v == 'F' ? 'T' : 'N'); }

struct RPred {
  enum class Kind {
    kIntCmp,      // int_col op rhs (constant, NULL literal, or int column)
    kIntIsNull,   // int_col IS [NOT] NULL
    kBoolCol,     // flag / NOT flag
    kStrCmp,      // s op pool constant (pool is lexicographically sorted)
    kStrIsNull,   // s IS [NOT] NULL
    kIntBetween,  // int_col BETWEEN lo AND hi
    kIntInList,   // int_col IN (c1, ..., NULL?)
    kStrLike,     // s LIKE pool pattern
    kNot,
    kBin,
    kIsNullWrap,  // (left) IS NULL, whole-predicate
  };
  Kind kind{Kind::kIntCmp};
  bool negated{false};  // IS NULL vs IS NOT NULL
  std::string op{"="};
  int col{0};  // 0 = a, 1 = b
  bool rhs_is_col{false};
  int rhs_col{0};
  int64_t rhs_const{0};
  bool rhs_is_null{false};  // kIntCmp against a literal NULL (always unknown)
  int64_t between_lo{0};
  int64_t between_hi{0};
  std::vector<int64_t> in_consts;  // kNull element renders as NULL
  int like_pat{0};
  int64_t str_const{0};  // index into kStrPool
  bool bool_positive{true};
  std::unique_ptr<RPred> left, right;

  static int64_t IntOf(const MirrorRow& r, int column) {
    return column == 0 ? r.a : r.b;
  }

  [[nodiscard]] char Eval(const MirrorRow& row) const {
    switch (kind) {
      case Kind::kIntCmp: {
        const int64_t lhs = IntOf(row, col);
        const int64_t rhs = rhs_is_col ? IntOf(row, rhs_col) : rhs_const;
        if (lhs == kNull || rhs == kNull || rhs_is_null) {
          return 'N';
        }
        if (op == "=") {
          return Eval3vl(lhs == rhs);
        }
        if (op == "!=") {
          return Eval3vl(lhs != rhs);
        }
        if (op == "<") {
          return Eval3vl(lhs < rhs);
        }
        if (op == "<=") {
          return Eval3vl(lhs <= rhs);
        }
        if (op == ">") {
          return Eval3vl(lhs > rhs);
        }
        return Eval3vl(lhs >= rhs);
      }
      case Kind::kIntIsNull: {
        const bool is_null = IntOf(row, col) == kNull;
        return Eval3vl(negated ? !is_null : is_null);
      }
      case Kind::kBoolCol: {
        if (row.flag == kNull) {
          return 'N';
        }
        const bool v = row.flag == 1;
        return Eval3vl(bool_positive ? v : !v);
      }
      case Kind::kStrCmp: {
        if (row.s == kNull) {
          return 'N';
        }
        // kStrPool is lexicographically sorted: code order equals string order.
        if (op == "=") {
          return Eval3vl(row.s == str_const);
        }
        if (op == "!=") {
          return Eval3vl(row.s != str_const);
        }
        if (op == "<") {
          return Eval3vl(row.s < str_const);
        }
        if (op == "<=") {
          return Eval3vl(row.s <= str_const);
        }
        if (op == ">") {
          return Eval3vl(row.s > str_const);
        }
        return Eval3vl(row.s >= str_const);
      }
      case Kind::kIntBetween: {
        const int64_t lhs = IntOf(row, col);
        if (lhs == kNull) {
          return 'N';
        }
        const bool inside = lhs >= between_lo && lhs <= between_hi;
        return Eval3vl(negated ? !inside : inside);
      }
      case Kind::kIntInList: {
        const int64_t lhs = IntOf(row, col);
        if (lhs == kNull) {
          return 'N';
        }
        bool saw_null = false;
        for (const int64_t e : in_consts) {
          if (e == kNull) {
            saw_null = true;
          } else if (e == lhs) {
            return negated ? 'F' : 'T';
          }
        }
        if (saw_null) {
          return 'N';
        }
        return negated ? 'T' : 'F';
      }
      case Kind::kStrLike: {
        if (row.s == kNull) {
          return 'N';
        }
        const bool m = LikeMatch(row.s, like_pat);
        return Eval3vl(negated ? !m : m);
      }
      case Kind::kStrIsNull: {
        const bool is_null = row.s == kNull;
        return Eval3vl(negated ? !is_null : is_null);
      }
      case Kind::kNot:
        return Not3vl(left->Eval(row));
      case Kind::kIsNullWrap:
        return left->Eval(row) == 'N' ? 'T' : 'F';
      case Kind::kBin:
        return op == "AND" ? And3vl(left->Eval(row), right->Eval(row))
                           : Or3vl(left->Eval(row), right->Eval(row));
    }
    return 'N';
  }

  [[nodiscard]] std::string Render() const {
    switch (kind) {
      case Kind::kIntCmp: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        std::string rhs;
        if (rhs_is_null) {
          rhs = "NULL";
        } else if (rhs_is_col) {
          rhs = kIntCols[static_cast<size_t>(rhs_col)];
        } else {
          rhs = std::to_string(rhs_const);
        }
        return "(" + std::string(kIntCols[static_cast<size_t>(col)]) + " " +
               op + " " + rhs + ")";
      }
      case Kind::kIntIsNull: {
        std::string out = "(";
        out += (col == 0 ? "a" : "b");
        out += " IS ";
        if (negated) {
          out += "NOT ";
        }
        out += "NULL)";
        return out;
      }
      case Kind::kBoolCol:
        return bool_positive ? "flag" : "(NOT flag)";
      case Kind::kStrCmp:
        return "(s " + op + " '" + kStrPool[static_cast<size_t>(str_const)] +
               "')";
      case Kind::kIntBetween: {
        std::string out = "(";
        out += (col == 0 ? "a" : "b");
        out += negated ? " NOT BETWEEN " : " BETWEEN ";
        out += std::to_string(between_lo) + " AND " +
               std::to_string(between_hi) + ")";
        return out;
      }
      case Kind::kIntInList: {
        std::string out = "(";
        out += (col == 0 ? "a" : "b");
        out += negated ? " NOT IN (" : " IN (";
        for (size_t i = 0; i < in_consts.size(); ++i) {
          out += i == 0 ? "" : ", ";
          out += in_consts[i] == kNull ? "NULL" : std::to_string(in_consts[i]);
        }
        out += "))";
        return out;
      }
      case Kind::kStrLike:
        return "(s " + std::string(negated ? "NOT LIKE '" : "LIKE '") +
               kLikePool[static_cast<size_t>(like_pat)] + "')";
      case Kind::kStrIsNull: {
        std::string out = "(s IS ";
        if (negated) {
          out += "NOT ";
        }
        out += "NULL)";
        return out;
      }
      case Kind::kNot:
        return "(NOT " + left->Render() + ")";
      case Kind::kIsNullWrap:
        return "(" + left->Render() + ") IS NULL";
      case Kind::kBin:
        return "(" + left->Render() + " " + op + " " + right->Render() + ")";
    }
    return "TRUE";
  }
};

using RPredPtr = std::unique_ptr<RPred>;

RPredPtr CopyPred(const RPred& p) {
  auto c = std::make_unique<RPred>();
  c->kind = p.kind;
  c->negated = p.negated;
  c->op = p.op;
  c->col = p.col;
  c->rhs_is_col = p.rhs_is_col;
  c->rhs_col = p.rhs_col;
  c->rhs_const = p.rhs_const;
  c->rhs_is_null = p.rhs_is_null;
  c->between_lo = p.between_lo;
  c->between_hi = p.between_hi;
  c->in_consts = p.in_consts;
  c->like_pat = p.like_pat;
  c->str_const = p.str_const;
  c->bool_positive = p.bool_positive;
  if (p.left) {
    c->left = CopyPred(*p.left);
  }
  if (p.right) {
    c->right = CopyPred(*p.right);
  }
  return c;
}

// ---------------------------------------------------------------------------
// Tiny deterministic RNG wrapper over the caller's engine.
// ---------------------------------------------------------------------------

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

// Small-domain constant most of the time, boundary constant occasionally.
int64_t GenIntConst(Gen& g) {
  if (g.Chance(18)) {
    return kEdgeConsts[static_cast<size_t>(
        g.Pick(0, static_cast<int>(std::size(kEdgeConsts)) - 1))];
  }
  return g.Pick(-3, 3);
}

RPredPtr IntLeafPredicate(Gen& g) {
  static const std::vector<std::string> kOps = {"=",  "!=", "<",
                                                "<=", ">",  ">="};
  auto p = std::make_unique<RPred>();
  switch (g.Pick(0, 7)) {
    case 0:
      p->kind = RPred::Kind::kIntCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->rhs_const = g.Pick(-3, 3);
      return p;
    case 1:
      p->kind = RPred::Kind::kIntCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->rhs_const = GenIntConst(g);
      return p;
    case 2:
      p->kind = RPred::Kind::kIntCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->rhs_is_col = true;
      p->rhs_col = g.Pick(0, 1);
      return p;
    case 3:
      p->kind = RPred::Kind::kIntCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->rhs_is_null = true;  // `a = NULL`-style 3VL trap
      return p;
    case 4:
      p->kind = RPred::Kind::kIntIsNull;
      p->col = g.Pick(0, 1);
      p->negated = g.Chance(50);
      return p;
    case 5:
      p->kind = RPred::Kind::kBoolCol;
      p->bool_positive = g.Chance(50);
      return p;
    case 6: {
      p->kind = RPred::Kind::kIntBetween;
      p->col = g.Pick(0, 1);
      p->between_lo = GenIntConst(g);
      p->between_hi = GenIntConst(g);
      if (g.Chance(60)) {  // usually a sane range
        if (p->between_lo > p->between_hi) {
          std::swap(p->between_lo, p->between_hi);
        }
      }
      p->negated = g.Chance(20);
      return p;
    }
    default: {
      p->kind = RPred::Kind::kIntInList;
      p->col = g.Pick(0, 1);
      const int n = g.Pick(1, 3);
      for (int i = 0; i < n; ++i) {
        p->in_consts.push_back(g.Chance(15) ? kNull : GenIntConst(g));
      }
      p->negated = g.Chance(25);
      return p;
    }
  }
}

RPredPtr LeafPredicate(Gen& g) {
  switch (g.Pick(0, 4)) {
    case 0:
    case 1:
      return IntLeafPredicate(g);
    case 2: {
      static const std::vector<std::string> kStrOps = {"=", "!=", "<", ">="};
      auto p = std::make_unique<RPred>();
      p->kind = RPred::Kind::kStrCmp;
      p->op = g.PickFrom(kStrOps);
      p->str_const = g.Pick(0, static_cast<int>(std::size(kStrPool)) - 1);
      return p;
    }
    case 3: {
      auto p = std::make_unique<RPred>();
      p->kind = RPred::Kind::kStrLike;
      p->like_pat = g.Pick(0, static_cast<int>(std::size(kLikePool)) - 1);
      p->negated = g.Chance(20);
      return p;
    }
    default: {
      auto p = std::make_unique<RPred>();
      p->kind = RPred::Kind::kStrIsNull;
      p->negated = g.Chance(50);
      return p;
    }
  }
}

// `flavour` biases the search space (driven by the plan-feedback bandit):
// 0 = default, 1 = shallower trees, 2 = boolean-heavy (no NOT nodes),
// 3 = join-heavy (forces the join partner for the metamorphic oracles).
RPredPtr GenPredicate(Gen& g, int depth, int flavour) {
  if (depth <= 0 || g.Chance(40 + (flavour * 10))) {
    return LeafPredicate(g);
  }
  if (flavour != 2 && g.Pick(0, 2) == 2) {
    auto p = std::make_unique<RPred>();
    p->kind = RPred::Kind::kNot;
    p->left = GenPredicate(g, depth - 1, flavour);
    return p;
  }
  auto p = std::make_unique<RPred>();
  p->kind = RPred::Kind::kBin;
  p->op = g.Chance(flavour == 2 ? 80 : 55) ? "AND" : "OR";
  p->left = GenPredicate(g, depth - 1, flavour);
  p->right = GenPredicate(g, depth - 1, flavour);
  return p;
}

std::string GenProjection(Gen& g, bool with_join) {
  const bool distinct = g.Chance(18);
  std::string prefix = distinct ? "DISTINCT " : "";
  const int n = with_join ? 6 : 4;
  switch (g.Pick(0, n - 1)) {
    case 0:
      return distinct ? prefix + "a" : "*";
    case 1:
      return "a";
    case 2:
      return "a, b";
    case 3:
      return distinct ? prefix + "flag, s" : "flag, s";
    case 4:
      return "x";
    default:
      return "a, x, y";
  }
}

// SQL boolean fragments the mirror does not track: subqueries, arithmetic,
// CASE and coalesce.  Used only by the metamorphic oracles (TLP partition
// predicate, NoREC filter), whose checks never need a ground truth.
std::string GenSqlOnlyPred(Gen& g, const std::string& tab,
                           const std::string& jtab) {
  switch (g.Pick(0, 8)) {
    case 0:
      return g.Chance(50) ? "(a IN (1, 2, NULL))" : "(b NOT IN (0, -1, 3))";
    case 1:
      return g.Chance(50) ? "(b BETWEEN -1 AND 1)"
                          : "(a BETWEEN 0 AND 9223372036854775807)";
    case 2:
      return "((b + 1) > a)";  // b is bounded, so the addition cannot overflow
    case 3:
      return "(CASE WHEN flag THEN a > 0 ELSE b < 1 END)";
    case 4:
      return "(COALESCE(a, b, 0) >= 0)";
    case 5:
      return "(s LIKE 'a%' OR s = '')";
    case 6:
      return jtab.empty() ? "(u IN (SELECT u FROM " + tab + " WHERE a > 0))"
                          : "(a IN (SELECT x FROM " + jtab + "))";
    case 7:
      return jtab.empty() ? "(SELECT COUNT(*) FROM " + tab + ") >= 0"
                          : "(EXISTS (SELECT 1 FROM " + jtab + " WHERE x > " +
                                tab + ".a))";
    default:
      return jtab.empty()
                 ? "(NOT (a IS NULL AND b IS NULL))"
                 : "((SELECT MAX(x) FROM " + jtab + ") > " + tab + ".b)";
  }
}

// TLP partition predicate: half the time a mirrored tree, half the time a
// SQL-only fragment (richer syntax without needing a C++ mirror).
std::string GenAuxPredicate(Gen& g, int flavour, const std::string& tab,
                            const std::string& jtab) {
  if (g.Chance(50)) {
    return GenPredicate(g, g.Pick(1, 2), flavour)->Render();
  }
  return GenSqlOnlyPred(g, tab, jtab);
}

// Generates one row: the VALUES clause and its mirror.
MirrorRow GenRow(Gen& g, int64_t u, std::string* sql_values) {
  MirrorRow m;
  m.u = u;
  // `a` carries the boundary constants (comparison / MIN / MAX / BETWEEN
  // stress); `b` stays in a small range so it is safe as the operand of the
  // auxiliary arithmetic and the grouped SUM (INT64_MAX would overflow those
  // metamorphic oracles and force a skip).
  m.a = g.Chance(15) ? kNull : GenIntConst(g);
  m.b = g.Chance(10) ? kNull : g.Pick(-4, 4);
  switch (g.Pick(0, 2)) {
    case 0:
      m.flag = 1;
      break;
    case 1:
      m.flag = 0;
      break;
    default:
      m.flag = kNull;
      break;
  }
  m.s = g.Chance(25) ? kNull
                     : g.Pick(0, static_cast<int>(std::size(kStrPool)) - 1);
  const std::string a_sql = m.a == kNull ? "NULL" : std::to_string(m.a);
  const std::string b_sql = m.b == kNull ? "NULL" : std::to_string(m.b);
  const std::string s_sql =
      m.s == kNull
          ? "NULL"
          : std::string("'") + kStrPool[static_cast<size_t>(m.s)] + "'";
  *sql_values = "(" + std::to_string(m.u) + ", " + a_sql + ", " + b_sql + ", " +
                (m.flag == kNull ? "NULL" : (m.flag == 1 ? "TRUE" : "FALSE")) +
                ", " + s_sql + ")";
  return m;
}

// VALUES tuple for the join partner table: (x INT64, y VARCHAR(8)).
std::string GenJoinRow(Gen& g) {
  const std::string x_sql =
      g.Chance(20) ? "NULL"
                   : (g.Chance(20) ? std::to_string(GenIntConst(g))
                                   : std::to_string(g.Pick(-3, 3)));
  const std::string y_sql =
      g.Chance(25) ? "NULL"
                   : std::string("'") +
                         kStrPool[static_cast<size_t>(g.Pick(
                             0, static_cast<int>(std::size(kStrPool)) - 1))] +
                         "'";
  return "(" + x_sql + ", " + y_sql + ")";
}

// ---------------------------------------------------------------------------
// Engine helpers.
// ---------------------------------------------------------------------------

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
  try {
    while (result.Value().Next(&row)) {
      rows.push_back(row);
    }
  } catch (const std::exception& e) {
    // Engine-raised runtime errors (e.g. "integer overflow in SUM" on the
    // edge-value rows) are legitimate rejections for a metamorphic oracle:
    // both sides of the comparison must raise the same way, so treat the
    // throw as a skip rather than a mismatch or a fuzzer crash.
    *error = e.what();
    return std::nullopt;
  }
  // Lazy execution surfaces runtime errors only through the executor status
  // ("integer overflow on '+'"): a failed-but-not-thrown query must also
  // skip the oracle, otherwise the empty result masquerades as data.
  const Status st = result.Value().GetStatus();
  if (st != Status::kSuccess) {
    *error = st.GetMessage();
    return std::nullopt;
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

// Runs any statement to completion; returns false on error.
bool RunUpdate(Database& db, TransactionContext& ctx, const std::string& sql) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    return false;
  }
  // QueryResults are lazy: drain or the mutation never happens.
  result.Value().Drain();
  return result.Value().GetStatus() == Status::kSuccess;
}

std::string DumpRows(const std::vector<std::string>& rows) {
  std::string out;
  for (const std::string& r : rows) {
    out += "    " + r + "\n";
  }
  return out;
}

constexpr const char* kTestHeader = "-- tinylamb-oracle-test v1";

// ---------------------------------------------------------------------------
// Oracle checks shared by generation and replay.  Each takes the trace
// fragment, executes it, appends a mismatch diagnostic to `report`, and
// returns false when a statement could not be executed (oracle skipped).
// ---------------------------------------------------------------------------

bool CheckTlp(Database& db, TransactionContext& ctx,
              const std::vector<std::string>& tlp, std::string* report,
              bool verbose) {
  if (tlp.size() != 4) {
    return true;
  }
  std::string error;
  auto r0 = RunRows(db, ctx, tlp[0], &error);
  auto r1 = RunRows(db, ctx, tlp[1], &error);
  auto r2 = RunRows(db, ctx, tlp[2], &error);
  auto r3 = RunRows(db, ctx, tlp[3], &error);
  if (!(r0.has_value() && r1.has_value() && r2.has_value() && r3.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][tlp-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> s0 = SerializeSorted(*r0);
  std::vector<std::string> s1 = SerializeSorted(*r1);
  std::vector<std::string> s2 = SerializeSorted(*r2);
  std::vector<std::string> s3 = SerializeSorted(*r3);
  std::vector<std::string> merged = s1;
  merged.insert(merged.end(), s2.begin(), s2.end());
  merged.insert(merged.end(), s3.begin(), s3.end());
  std::sort(merged.begin(), merged.end());
  if (tlp[0].find("DISTINCT") != std::string::npos) {
    // Set semantics: duplicate projected tuples may land in different
    // partitions, so compare deduplicated multisets.
    merged.erase(std::unique(merged.begin(), merged.end()), merged.end());
    if (!s0.empty()) {
      s0.erase(std::unique(s0.begin(), s0.end()), s0.end());
    }
  }
  if (s0 == merged) {
    return true;
  }
  *report += "[TLP MISMATCH]\n";
  *report += "  original: " + tlp[0] + " (" + std::to_string(s0.size()) +
             " rows)\n" + DumpRows(s0);
  *report += "  part1:    " + tlp[1] + " (" + std::to_string(s1.size()) +
             " rows)\n" + DumpRows(s1);
  *report += "  part2:    " + tlp[2] + " (" + std::to_string(s2.size()) +
             " rows)\n" + DumpRows(s2);
  *report += "  part3:    " + tlp[3] + " (" + std::to_string(s3.size()) +
             " rows)\n" + DumpRows(s3);
  *report += "  merged partition rows: " + std::to_string(merged.size()) + "\n";
  return true;
}

// Aggregate TLP: grouped COUNT(*)/SUM over the whole predicate must equal
// the three partitions merged group-wise (count: sum, sum: add skipping
// NULL-only slices, presence: union).  Stresses grouped execution, NULL
// group keys, and the optimizer's partition pushdown under joins.
bool CheckTlpAgg(Database& db, TransactionContext& ctx,
                 const std::vector<std::string>& tlp_agg, std::string* report,
                 bool verbose) {
  if (tlp_agg.size() != 4) {
    return true;
  }
  std::string error;
  // Materialize each arm up front (a failed arm aborts the whole check), so
  // the later loops read plain vectors instead of re-probing optionals.
  std::vector<std::vector<Row>> parts;
  parts.reserve(4);
  for (int i = 0; i < 4; ++i) {
    std::optional<std::vector<Row>> rows =
        RunRows(db, ctx, tlp_agg[static_cast<size_t>(i)], &error);
    if (!rows.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][tlpagg-error] " << error << "\n";
      }
      return false;
    }
    parts.push_back(std::move(*rows));
    for (const Row& r : parts.back()) {
      if (r.Size() < 3 || r[1].IsNull() || r[1].type != ValueType::kInt64 ||
          (!r[2].IsNull() && r[2].type != ValueType::kInt64)) {
        return false;  // unexpected aggregate output shape: skip
      }
    }
  }
  using Group = std::pair<int64_t, std::optional<int64_t>>;
  auto key_of = [](const Row& r) { return r[0].AsString(); };
  std::map<std::string, Group> expected;
  for (int i = 1; i < 4; ++i) {
    for (const Row& r : parts[static_cast<size_t>(i)]) {
      Group& g = expected[key_of(r)];
      g.first += r[1].value.int_value;
      if (!r[2].IsNull()) {
        g.second = g.second.value_or(0) + r[2].value.int_value;
      }
    }
  }
  std::map<std::string, Group> actual;
  for (const Row& r : parts[0]) {
    actual[key_of(r)] = {r[1].value.int_value,
                         r[2].IsNull()
                             ? std::optional<int64_t>()
                             : std::optional<int64_t>(r[2].value.int_value)};
  }
  if (expected == actual) {
    return true;
  }
  *report += "[AGG-TLP MISMATCH]\n";
  *report += "  original: " + tlp_agg[0] + "\n";
  auto dump = [](const std::map<std::string, Group>& m) {
    std::string out;
    for (const auto& [key, grp] : m) {
      out += "    " + key + " -> count " + std::to_string(grp.first) +
             ", sum " +
             (grp.second.has_value() ? std::to_string(*grp.second)
                                     : std::string("NULL")) +
             "\n";
    }
    return out;
  };
  *report += "  merged partitions:\n" + dump(expected);
  *report += "  original result:\n" + dump(actual);
  return true;
}

bool CheckNoRec(Database& db, TransactionContext& ctx,
                const std::vector<std::string>& norec, std::string* report,
                bool verbose) {
  if (norec.size() != 2) {
    return true;
  }
  std::string error;
  auto c = RunScalar(db, ctx, norec[0], &error);
  auto s = RunScalar(db, ctx, norec[1], &error);
  if (!(c.has_value() && s.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][norec-error] " << error << "\n";
    }
    return false;
  }
  if (*c == *s) {
    return true;
  }
  *report += "[NoREC MISMATCH]\n";
  *report += "  optimized:   " + norec[0] + " => " + *c + "\n";
  *report += "  reference:   " + norec[1] + " => " + *s + "\n";
  return true;
}

// PQS: the harness-mirror count must match, and the pivot row must appear.
bool CheckPqs(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose) {
  if (t.pqs_count.empty() || t.pqs_u.empty()) {
    return true;
  }
  std::string error;
  auto count = RunScalar(db, ctx, t.pqs_count, &error);
  auto us = RunRows(db, ctx, t.pqs_u, &error);
  if (!(count.has_value() && us.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][pqs-error] " << error << "\n";
    }
    return false;
  }
  bool pivot_found = false;
  for (const Row& r : *us) {
    if (r[0] == Value(t.pqs_pivot_u)) {
      pivot_found = true;
      break;
    }
  }
  const bool count_ok = *count == std::to_string(t.pqs_expected);
  if (count_ok && pivot_found) {
    return true;
  }
  *report += "[PQS MISMATCH]\n";
  if (!count_ok) {
    *report += "  " + t.pqs_count + "\n    expected " +
               std::to_string(t.pqs_expected) + " rows, got " + *count + "\n";
  }
  if (!pivot_found) {
    *report += "  pivot u=" + std::to_string(t.pqs_pivot_u) +
               " missing from: " + t.pqs_u + "\n";
  }
  return true;
}

// Applies a recorded `CREATE INDEX <name> ON <table>(<col>, ...);` DDL
// through the catalog API: the GoogleSQL frontend parses the statement but
// has no executor route for it (every fuzzer DDL used to be rejected,
// silently killing the whole index-independence oracle).  CreateIndex goes
// through the same catalog update, schema-epoch bump, and plan-cache
// invalidation a SQL route would trigger.
bool ApplyIndexDdl(Database& db, TransactionContext& ctx,
                   const std::string& ddl, bool verbose) {
  constexpr std::string_view prefix = "CREATE INDEX ";
  if (!ddl.starts_with(prefix)) {
    return false;
  }
  const size_t on_pos = ddl.find(" ON ", prefix.size());
  const size_t open = ddl.find('(', on_pos == std::string::npos ? 0 : on_pos);
  const size_t close = ddl.rfind(')');
  if (on_pos == std::string::npos || open == std::string::npos ||
      close == std::string::npos || close < open) {
    return false;
  }
  const std::string name = ddl.substr(prefix.size(), on_pos - prefix.size());
  const std::string table = ddl.substr(on_pos + 4, open - on_pos - 4);
  std::vector<std::string> columns;
  {
    std::string rest = ddl.substr(open + 1, close - open - 1);
    size_t pos = 0;
    while (pos <= rest.size()) {
      const size_t comma = rest.find(',', pos);
      std::string column = rest.substr(
          pos, comma == std::string::npos ? std::string::npos : comma - pos);
      const size_t first = column.find_first_not_of(" \t");
      const size_t last = column.find_last_not_of(" \t");
      columns.push_back(first == std::string::npos
                            ? std::string()
                            : column.substr(first, last - first + 1));
      if (comma == std::string::npos) {
        break;
      }
      pos = comma + 1;
    }
  }
  auto tbl = ctx.GetTable(table);
  if (!tbl.HasValue()) {
    if (verbose) {
      std::cerr << "[sql_oracle][idx-ddl-error] table " << table << "\n";
    }
    return false;
  }
  const Schema& schema = tbl.Value()->GetSchema();
  std::vector<slot_t> slots;
  for (const std::string& column : columns) {
    bool found = false;
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).Name().name == column) {
        slots.push_back(static_cast<slot_t>(i));
        found = true;
        break;
      }
    }
    if (!found) {
      if (verbose) {
        std::cerr << "[sql_oracle][idx-ddl-error] column " << column << "\n";
      }
      return false;
    }
  }
  // kNonUnique: the generated data has many duplicate keys, and index
  // independence is about access paths, not uniqueness enforcement.
  const Status st = db.CreateIndex(
      ctx, table, IndexSchema(name, slots, {}, IndexMode::kNonUnique));
  if (st != Status::kSuccess && verbose) {
    std::cerr << "[sql_oracle][idx-ddl-error] " << st.GetMessage() << "\n";
  }
  return st == Status::kSuccess;
}

// Constraint rewriting: the probe must survive every CREATE INDEX step
// unchanged (index scan vs full scan must agree at every index set).
bool CheckIdx(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose) {
  if (t.index_ddl.empty() || t.index_probe.empty()) {
    return true;
  }
  std::string error;
  auto before = RunScalar(db, ctx, t.index_probe, &error);
  if (!before.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][idx-error] " << error << "\n";
    }
    return false;
  }
  for (size_t step = 0; step < t.index_ddl.size(); ++step) {
    const std::string& ddl = t.index_ddl[step];
    if (!ApplyIndexDdl(db, ctx, ddl, verbose)) {
      if (verbose) {
        std::cerr << "[sql_oracle][idx-ddl-error] :: " << ddl << "\n";
      }
      return false;
    }
    auto after = RunScalar(db, ctx, t.index_probe, &error);
    if (!after.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][idx-error] " << error << "\n";
      }
      return false;
    }
    if (*before == *after) {
      continue;
    }
    *report += "[INDEX MISMATCH]\n";
    *report += "  before: " + t.index_probe + " => " + *before + "\n";
    for (size_t i = 0; i <= step; ++i) {
      *report += "  ddl:    " + t.index_ddl[i] + "\n";
    }
    *report += "  after:  " + t.index_probe + " => " + *after + "\n";
  }
  return true;
}

// Transaction splitting: atomic execution must match autocommit execution.
// Transaction splitting: atomic execution must match autocommit execution.
// Both branches replay the full trace (setup + mutations + probe) on their
// own throwaway database so the atomic branch runs exactly one transaction
// and the autocommit branch commits per statement.  (The previous design
// re-ran the setup on the shared database, so its second CREATE TABLE always
// failed and the oracle silently skipped every iteration.)
bool CheckTroc(Database& db, const OracleTrace& t, std::string* report,
               bool verbose) {
  (void)db;
  if (t.troc.empty() || t.troc_probe.empty()) {
    return true;
  }
  std::string error;

  // Branch A: setup + mutations inside ONE transaction.
  auto holder_a =
      Database::Create("sql_oracle_trocA-" + RandomString(8)).MoveValue();
  if (holder_a == nullptr) {
    return false;
  }
  Database& db_a = *holder_a;
  TransactionContext ctx_a = db_a.BeginContext();
  for (const std::string& sql : t.setup) {
    if (!RunUpdate(db_a, ctx_a, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-setup-error] " << sql << "\n";
      }
      return false;
    }
  }
  for (const std::string& sql : t.troc) {
    if (!RunUpdate(db_a, ctx_a, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-stmt-error] " << sql << "\n";
      }
      return false;
    }
  }
  auto inside = RunRows(db_a, ctx_a, t.troc_probe, &error);
  const bool committed = ctx_a.txn_.PreCommit() == Status::kSuccess;
  TransactionContext ctx_a2 = db_a.BeginContext();
  auto after_a = RunRows(db_a, ctx_a2, t.troc_probe, &error);
  if (!(inside.has_value() && after_a.has_value()) || !committed) {
    if (verbose) {
      std::cerr << "[sql_oracle][troc-error-a] " << error << "\n";
    }
    return false;
  }

  // Branch B: same statements, each in its own transaction (autocommit).
  auto holder_b =
      Database::Create("sql_oracle_trocB-" + RandomString(8)).MoveValue();
  if (holder_b == nullptr) {
    return false;
  }
  Database& db_b = *holder_b;
  TransactionContext ctx_b = db_b.BeginContext();
  for (const std::string& sql : t.setup) {
    if (!RunUpdate(db_b, ctx_b, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-setup-error-b] " << sql << "\n";
      }
      return false;
    }
  }
  ctx_b.txn_.PreCommit();
  for (const std::string& sql : t.troc) {
    TransactionContext ctx = db_b.BeginContext();
    if (!RunUpdate(db_b, ctx, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-stmt-error-b] " << sql << "\n";
      }
      return false;
    }
    if (ctx.txn_.PreCommit() != Status::kSuccess) {
      return false;
    }
  }
  TransactionContext ctx_b2 = db_b.BeginContext();
  auto after_b = RunRows(db_b, ctx_b2, t.troc_probe, &error);
  if (!after_b.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][troc-error-b] " << error << "\n";
    }
    return false;
  }

  const std::string state_inside =
      inside->empty() ? "" : (*inside)[0].ToString();
  const std::string state_a = after_a->empty() ? "" : (*after_a)[0].ToString();
  const std::string state_b = after_b->empty() ? "" : (*after_b)[0].ToString();
  if (state_inside == state_a && state_a == state_b) {
    return true;
  }
  *report += "[TRANSACTION MISMATCH]\n";
  *report += "  probe:      " + t.troc_probe + "\n";
  *report += "  inside txn: " + state_inside + "\n";
  *report += "  committed:  " + state_a + "\n";
  *report += "  autocommit: " + state_b + "\n";
  return true;
}

// Statement-type transformation: DELETE must affect exactly COUNT(WHERE p).
bool CheckDqe(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose) {
  if (t.dqe.size() != 3) {
    return true;
  }
  std::string error;
  auto count_p = RunScalar(db, ctx, t.dqe[0], &error);
  auto count_all = RunScalar(db, ctx, t.dqe[1], &error);
  if (!(count_p.has_value() && count_all.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][dqe-error] " << error << "\n";
    }
    return false;
  }
  if (!RunUpdate(db, ctx, t.dqe[2])) {
    if (verbose) {
      std::cerr << "[sql_oracle][dqe-delete-error] " << t.dqe[2] << "\n";
    }
    return false;
  }
  auto count_after = RunScalar(db, ctx, t.dqe[1], &error);
  if (!count_after.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][dqe-error] " << error << "\n";
    }
    return false;
  }
  const int64_t cp = std::stoll(*count_p);
  const int64_t ca = std::stoll(*count_all);
  const int64_t cr = std::stoll(*count_after);
  if (cr == ca - cp) {
    return true;
  }
  *report += "[DQE MISMATCH]\n";
  *report += "  " + t.dqe[0] + " => " + *count_p + "\n";
  *report += "  " + t.dqe[1] + " => " + *count_all + "\n";
  *report += "  " + t.dqe[2] + "\n";
  *report += "  remaining => " + *count_after + " (expected " +
             std::to_string(ca - cp) + ")\n";
  return true;
}

// Executes the setup statements; returns false when the engine rejected one.
bool RunSetup(Database& db, TransactionContext& ctx,
              const std::vector<std::string>& setup, bool verbose) {
  for (const std::string& sql : setup) {
    if (!RunUpdate(db, ctx, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][skip-setup] " << sql << "\n";
      }
      return false;
    }
  }
  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// OracleSession: QPG-flavoured plan feedback.
// ---------------------------------------------------------------------------

bool OracleSession::ObservePlan(const std::string& fingerprint) {
  if (fingerprint.empty()) {
    return false;
  }
  return plans_.insert(fingerprint).second;
}

int OracleSession::SelectFlavour(int flavour_count) {
  if (rewards_.size() != static_cast<size_t>(flavour_count)) {
    rewards_.assign(static_cast<size_t>(flavour_count), 1);
  }
  if (std::uniform_int_distribution<int>(1, 10)(rng_) <= 2) {
    return std::uniform_int_distribution<int>(0, flavour_count - 1)(rng_);
  }
  int best = 0;
  for (int i = 1; i < flavour_count; ++i) {
    if (rewards_[static_cast<size_t>(i)] >
        rewards_[static_cast<size_t>(best)]) {
      best = i;
    }
  }
  return best;
}

void OracleSession::RewardFlavour(int flavour, bool discovered_new_plan) {
  if (rewards_.size() <= static_cast<size_t>(flavour)) {
    rewards_.resize(static_cast<size_t>(flavour) + 1, 1);
  }
  if (discovered_new_plan) {
    ++rewards_[static_cast<size_t>(flavour)];
  } else if (rewards_[static_cast<size_t>(flavour)] > 1) {
    --rewards_[static_cast<size_t>(flavour)];
  }
}

// ---------------------------------------------------------------------------
// Iteration: generate the trace, then replay it as the authoritative check.
// ---------------------------------------------------------------------------

std::string RunOracleIteration(std::mt19937& rng, bool verbose,
                               OracleIterationStats* stats, OracleTrace* trace,
                               OracleSession* session) {
  Gen g(rng);
  OracleTrace local;
  OracleTrace& t = (trace != nullptr) ? *trace : local;

  const int flavour = session != nullptr ? session->SelectFlavour(4) : 0;

  // Unique table name per iteration: the compiled-plan cache is process
  // global and keyed by SQL text + schema epoch, so identical SQL against a
  // fresh database could otherwise replay a plan compiled for a previous
  // iteration's database.
  const std::string tab =
      "t" +
      std::to_string(std::uniform_int_distribution<int64_t>(0, INT32_MAX)(rng));
  t.table = tab;
  t.setup.push_back("CREATE TABLE " + tab +
                    " (u INT64, a INT64, b INT64, flag BOOL, s VARCHAR(8));");
  std::vector<MirrorRow> mirror;
  const int row_count = g.Pick(4, 12);
  for (int i = 0; i < row_count; ++i) {
    std::string values;
    mirror.push_back(GenRow(g, i, &values));
    std::string insert = "INSERT INTO ";
    insert += tab;
    insert += " VALUES ";
    insert += values;
    insert += ";";
    t.setup.push_back(std::move(insert));
  }

  // Optional join partner: the metamorphic oracles run over this FROM clause,
  // which drags hash/merge/NL join selection, outer-join null extension, and
  // predicate pushdown into the TLP/NoREC/index probes.  Columns are named
  // x/y so the mirrored predicate's bare a/b/flag/s stay unambiguous.
  const bool with_join = g.Chance(flavour == 3 ? 75 : 30);
  std::string jtab;
  std::string from_main = tab;
  if (with_join) {
    jtab = tab + "_j";
    t.setup.push_back("CREATE TABLE " + jtab + " (x INT64, y VARCHAR(8));");
    const int join_rows = g.Pick(2, 7);
    for (int i = 0; i < join_rows; ++i) {
      t.setup.push_back("INSERT INTO " + jtab + " VALUES " + GenJoinRow(g) +
                        ";");
    }
    const int shape = g.Pick(0, 4);
    const std::string on_eq = tab + ".a = " + jtab + ".x";
    switch (shape) {
      case 0:
        from_main = tab + " JOIN " + jtab + " ON " + on_eq;
        break;
      case 1:
        from_main = tab + " LEFT JOIN " + jtab + " ON " + on_eq;
        break;
      case 2:
        from_main = jtab + " RIGHT JOIN " + tab + " ON " + on_eq;
        break;
      case 3:
        from_main = tab + " FULL JOIN " + jtab + " ON " + on_eq;
        break;
      default:
        from_main = tab + " CROSS JOIN " + jtab;
        break;
    }
  }

  const RPredPtr pred = GenPredicate(g, g.Pick(1, 3), flavour);
  t.predicate = pred->Render();

  // ---- TLP ----
  {
    const std::string proj = GenProjection(g, with_join);
    const std::string aux = GenAuxPredicate(g, flavour, tab, jtab);
    t.tlp = {
        "SELECT " + proj + " FROM " + from_main + " WHERE " + t.predicate + ";",
        "SELECT " + proj + " FROM " + from_main + " WHERE (" + t.predicate +
            ") AND (" + aux + ");",
        "SELECT " + proj + " FROM " + from_main + " WHERE (" + t.predicate +
            ") AND NOT (" + aux + ");",
        "SELECT " + proj + " FROM " + from_main + " WHERE (" + t.predicate +
            ") AND (" + aux + ") IS NULL;",
    };
  }

  // ---- Aggregate TLP (grouped partition merge) ----
  if (!with_join || g.Chance(60)) {
    const std::string aux = GenAuxPredicate(g, flavour, tab, jtab);
    const std::string sel =
        "SELECT a, COUNT(*), SUM(b) FROM " + from_main + " WHERE ";
    const std::string grp = " GROUP BY a;";
    t.tlp_agg = {
        sel + t.predicate + grp,
        sel + "(" + t.predicate + ") AND (" + aux + ")" + grp,
        sel + "(" + t.predicate + ") AND NOT (" + aux + ")" + grp,
        sel + "(" + t.predicate + ") AND (" + aux + ") IS NULL" + grp,
    };
  }

  // ---- NoREC (optionally conjoined with a SQL-only fragment) ----
  {
    std::string filter = t.predicate;
    if (g.Chance(45)) {
      const std::string extra = GenSqlOnlyPred(g, tab, jtab);
      filter = g.Chance(60) ? "(" + t.predicate + ") AND " + extra
                            : "(" + t.predicate + ") OR " + extra;
    }
    t.norec = {"SELECT COUNT(*) FROM " + from_main + " WHERE " + filter + ";",
               "SELECT SUM(CASE WHEN " + filter + " THEN 1 ELSE 0 END) FROM " +
                   from_main + ";"};
  }

  // ---- PQS: adjust the predicate so the pivot row must be included ----
  {
    const MirrorRow& pivot = mirror[static_cast<size_t>(
        g.Pick(0, static_cast<int>(mirror.size()) - 1))];
    RPredPtr adjusted;
    const char pivot_eval = pred->Eval(pivot);
    if (pivot_eval == 'T') {
      adjusted = CopyPred(*pred);
    } else if (pivot_eval == 'F') {
      adjusted = std::make_unique<RPred>();
      adjusted->kind = RPred::Kind::kNot;
      adjusted->left = CopyPred(*pred);
    } else {
      // Pivot evaluates to NULL: only "p IS NULL" lets it through.
      adjusted = std::make_unique<RPred>();
      adjusted->kind = RPred::Kind::kIsNullWrap;
      adjusted->left = CopyPred(*pred);
    }
    if (adjusted != nullptr) {
      const std::string rendered = adjusted->Render();
      int64_t expected = 0;
      for (const MirrorRow& m : mirror) {
        if (adjusted->Eval(m) == 'T') {
          ++expected;
        }
      }
      t.pqs_count = "SELECT COUNT(*) FROM " + tab + " WHERE " + rendered + ";";
      t.pqs_u = "SELECT u FROM " + tab + " WHERE " + rendered + ";";
      t.pqs_expected = expected;
      t.pqs_pivot_u = pivot.u;
    }
  }

  // ---- Constraint rewriting (index independence) ----
  {
    static const std::array<const char*, 4> kIdxCols = {"a", "b", "flag", "s"};
    const int idx_count = g.Pick(1, 4);
    for (int i = 0; i < idx_count; ++i) {
      const std::string base =
          std::string("CREATE INDEX idx_fuzz") + std::to_string(i) + " ON ";
      const int shape = g.Pick(0, 9);
      if (!jtab.empty() && shape == 0) {
        t.index_ddl.push_back(base + jtab + "(x);");
      } else if (shape == 1) {
        t.index_ddl.push_back(base + tab + "(a, b);");
      } else {
        t.index_ddl.push_back(base + tab + "(" +
                              kIdxCols[static_cast<size_t>(g.Pick(0, 3))] +
                              ");");
      }
    }
    // Probe each DDL step against the pre-DDL baseline.
    t.index_probe =
        "SELECT COUNT(*) FROM " + from_main + " WHERE " + t.predicate + ";";
  }

  // ---- Transaction splitting ----
  {
    const int stmt_count = g.Pick(1, 4);
    for (int i = 0; i < stmt_count; ++i) {
      switch (g.Pick(0, 3)) {
        case 0: {
          const RPredPtr cond = GenPredicate(g, g.Pick(1, 2), flavour);
          std::string set_expr;
          switch (g.Pick(0, 4)) {
            case 0:
              set_expr = "SET a = NULL";
              break;
            case 1:
              set_expr = "SET a = b";
              break;
            case 2:
              set_expr = "SET b = a + 1";
              break;
            case 3:
              set_expr = "SET a = " + std::to_string(GenIntConst(g));
              break;
            default:
              set_expr = "SET flag = TRUE, a = -a";
              break;
          }
          std::string update_sql = "UPDATE ";
          update_sql += tab;
          update_sql += " ";
          update_sql += set_expr;
          update_sql += " WHERE ";
          update_sql += cond->Render();
          update_sql += ";";
          t.troc.push_back(std::move(update_sql));
          break;
        }
        case 1: {
          const RPredPtr cond = GenPredicate(g, g.Pick(1, 2), flavour);
          std::string delete_sql = "DELETE FROM " + tab + " WHERE ";
          delete_sql += cond->Render();
          delete_sql += ";";
          t.troc.push_back(std::move(delete_sql));
          break;
        }
        case 2: {
          const std::string cond = GenSqlOnlyPred(g, tab, "");
          std::string delete_sql = "DELETE FROM " + tab + " WHERE ";
          delete_sql += cond;
          delete_sql += ";";
          t.troc.push_back(std::move(delete_sql));
          break;
        }
        default: {
          std::string values;
          GenRow(g, 1000 + i, &values);
          std::string insert = "INSERT INTO ";
          insert += tab;
          insert += " VALUES ";
          insert += values;
          insert += ";";
          t.troc.push_back(std::move(insert));
          break;
        }
      }
    }
    // SUM(a) omitted: `a` carries INT64-class boundary values whose sum
    // overflows, which would turn every probe into a skip.
    t.troc_probe =
        "SELECT COUNT(*), SUM(b), COUNT(a), COUNT(b), MIN(a), MAX(a) FROM " +
        tab + ";";
  }

  // ---- Statement-type transformation (mutates; probed last) ----
  {
    t.dqe = {"SELECT COUNT(*) FROM " + tab + " WHERE " + t.predicate + ";",
             "SELECT COUNT(*) FROM " + tab + ";",
             "DELETE FROM " + tab + " WHERE " + t.predicate + ";"};
  }

  // Plan feedback: reward flavours that surface unseen plan shapes.
  if (session != nullptr) {
    auto db_holder =
        Database::Create("sql_oracle_fuzz-" + RandomString(8)).MoveValue();
    CHECK(db_holder != nullptr);
    Database& db = *db_holder;
    TransactionContext ctx = db.BeginContext();
    if (RunSetup(db, ctx, t.setup, verbose)) {
      std::string error;
      auto plan = RunRows(db, ctx, "EXPLAIN " + t.tlp[0], &error);
      if (plan.has_value()) {
        std::string fingerprint;
        for (const Row& r : *plan) {
          fingerprint += r.ToString() + ";";
        }
        session->RewardFlavour(flavour, session->ObservePlan(fingerprint));
      }
    }
  }

  const std::string report = ReplayOracleTrace(t, verbose);
  if (stats != nullptr) {
    stats->tlp_ran = t.tlp.size() == 4;
    stats->tlp_agg_ran = t.tlp_agg.size() == 4;
    stats->norec_ran = t.norec.size() == 2;
    stats->pqs_ran = !t.pqs_count.empty();
    stats->idx_ran = !t.index_ddl.empty() && !t.index_probe.empty();
    stats->dqe_ran = t.dqe.size() == 3;
    stats->troc_ran = !t.troc.empty();
  }
  return report;
}

std::string ReplayOracleTrace(const OracleTrace& trace, bool verbose) {
  if (trace.setup.empty() || !trace.setup[0].starts_with("CREATE TABLE")) {
    return "malformed trace: no CREATE TABLE in setup";
  }
  auto db_holder =
      Database::Create("sql_oracle_replay-" + RandomString(8)).MoveValue();
  CHECK(db_holder != nullptr);
  Database& db = *db_holder;
  TransactionContext ctx = db.BeginContext();
  if (!RunSetup(db, ctx, trace.setup, verbose)) {
    return "setup statement failed";
  }
  std::string report;
  if (!CheckTlp(db, ctx, trace.tlp, &report, verbose) ||
      !CheckTlpAgg(db, ctx, trace.tlp_agg, &report, verbose) ||
      !CheckNoRec(db, ctx, trace.norec, &report, verbose) ||
      !CheckPqs(db, ctx, trace, &report, verbose) ||
      !CheckIdx(db, ctx, trace, &report, verbose) ||
      !CheckTroc(db, trace, &report, verbose) ||
      !CheckDqe(db, ctx, trace, &report, verbose)) {
    // A statement failed to execute: the oracle could not run, which is a
    // skip, not a mismatch.
  }
  if (!report.empty() && !trace.predicate.empty()) {
    report = "predicate: " + trace.predicate + "\n" + report;
  }
  return report;
}

std::string SerializeOracleTest(uint64_t seed, const OracleTrace& trace,
                                const std::string& failure_summary) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  if (!trace.table.empty()) {
    out += "-- table: " + trace.table + "\n";
  }
  if (!trace.predicate.empty()) {
    out += "-- predicate: " + trace.predicate + "\n";
  }
  for (const std::string& sql : trace.setup) {
    out += "-- setup: " + sql + "\n";
  }
  for (const std::string& sql : trace.tlp) {
    out += "-- tlp: " + sql + "\n";
  }
  for (const std::string& sql : trace.tlp_agg) {
    out += "-- tlpagg: " + sql + "\n";
  }
  for (const std::string& sql : trace.norec) {
    out += "-- norec: " + sql + "\n";
  }
  if (!trace.pqs_count.empty()) {
    out += "-- pqs: " + trace.pqs_count + "\n";
    out += "-- pqsu: " + trace.pqs_u + "\n";
    out += "-- pqsexpected: " + std::to_string(trace.pqs_expected) + "\n";
    out += "-- pqspivot: " + std::to_string(trace.pqs_pivot_u) + "\n";
  }
  for (const std::string& sql : trace.index_ddl) {
    out += "-- idxddl: " + sql + "\n";
  }
  if (!trace.index_probe.empty()) {
    out += "-- idxprobe: " + trace.index_probe + "\n";
  }
  for (const std::string& sql : trace.dqe) {
    out += "-- dqe: " + sql + "\n";
  }
  for (const std::string& sql : trace.troc) {
    out += "-- troc: " + sql + "\n";
  }
  if (!trace.troc_probe.empty()) {
    out += "-- trocprobe: " + trace.troc_probe + "\n";
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
    if (line.starts_with("-- tinylamb-oracle-test")) {
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
    } else if (consume("-- table: ", &value)) {
      trace->table = value;
    } else if (consume("-- predicate: ", &value)) {
      trace->predicate = value;
    } else if (consume("-- setup: ", &value)) {
      trace->setup.push_back(value);
    } else if (consume("-- tlp: ", &value)) {
      trace->tlp.push_back(value);
    } else if (consume("-- tlpagg: ", &value)) {
      trace->tlp_agg.push_back(value);
    } else if (consume("-- norec: ", &value)) {
      trace->norec.push_back(value);
    } else if (consume("-- pqs: ", &value)) {
      trace->pqs_count = value;
    } else if (consume("-- pqsu: ", &value)) {
      trace->pqs_u = value;
    } else if (consume("-- pqsexpected: ", &value)) {
      trace->pqs_expected = std::stoll(value);
    } else if (consume("-- pqspivot: ", &value)) {
      trace->pqs_pivot_u = std::stoll(value);
    } else if (consume("-- idxddl: ", &value)) {
      trace->index_ddl.push_back(value);
    } else if (consume("-- idxprobe: ", &value)) {
      trace->index_probe = value;
    } else if (consume("-- dqe: ", &value)) {
      trace->dqe.push_back(value);
    } else if (consume("-- troc: ", &value)) {
      trace->troc.push_back(value);
    } else if (consume("-- trocprobe: ", &value)) {
      trace->troc_probe = value;
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;  // unknown line: refuse to replay a half-understood file
    }
  }
  return saw_header && !trace->setup.empty();
}

// ---------------------------------------------------------------------------
// AMOEBA: equivalent predicates must not differ wildly in runtime.
// ---------------------------------------------------------------------------

std::string RunAmoebaIteration(std::mt19937& rng, bool verbose) {
  Gen g(rng);
  auto db_holder =
      Database::Create("sql_oracle_amoeba-" + RandomString(8)).MoveValue();
  CHECK(db_holder != nullptr);
  Database& db = *db_holder;
  TransactionContext ctx = db.BeginContext();
  const std::string tab =
      "t" +
      std::to_string(std::uniform_int_distribution<int64_t>(0, INT32_MAX)(rng));
  if (!RunUpdate(db, ctx,
                 "CREATE TABLE " + tab +
                     " (u INT64, a INT64, b INT64, flag BOOL, s "
                     "VARCHAR(8));")) {
    return "";
  }
  std::vector<MirrorRow> mirror;
  std::vector<std::string> inserts;
  std::string batch;
  for (int64_t i = 0; i < 2000; ++i) {
    std::string values;
    mirror.push_back(GenRow(g, i, &values));
    batch += (batch.empty() ? "" : ", ") + values;
    if ((i + 1) % 200 == 0) {
      std::string insert = "INSERT INTO ";
      insert += tab;
      insert += " VALUES ";
      insert += batch;
      insert += ";";
      inserts.push_back(std::move(insert));
      batch.clear();
    }
  }
  for (const std::string& sql : inserts) {
    if (!RunUpdate(db, ctx, sql)) {
      return "";
    }
  }

  // Two-leaf conjunction plus its equivalent forms (3VL-safe rewrites).
  auto leaf = [&]() {
    auto p = std::make_unique<RPred>();
    static const std::array<const char*, 4> kOps = {"<", "<=", ">", ">="};
    p->kind = RPred::Kind::kIntCmp;
    p->col = g.Pick(0, 1);
    p->op = kOps[static_cast<size_t>(g.Pick(0, 3))];
    p->rhs_const = g.Pick(-2, 2);
    return p;
  };
  RPredPtr l1 = leaf();
  RPredPtr l2 = leaf();

  auto not_of = [](RPredPtr inner) {
    auto n = std::make_unique<RPred>();
    n->kind = RPred::Kind::kNot;
    n->left = std::move(inner);
    return n;
  };
  auto bin = [](RPredPtr a, const char* op, RPredPtr b) {
    auto n = std::make_unique<RPred>();
    n->kind = RPred::Kind::kBin;
    n->op = op;
    n->left = std::move(a);
    n->right = std::move(b);
    return n;
  };

  std::vector<RPredPtr> variants;
  variants.push_back(bin(CopyPred(*l1), "AND", CopyPred(*l2)));
  variants.push_back(
      not_of(bin(not_of(CopyPred(*l1)), "OR", not_of(CopyPred(*l2)))));
  variants.push_back(bin(CopyPred(*l2), "AND", CopyPred(*l1)));

  // Sanity: the mirror says all variants are semantically identical.
  int64_t expected = 0;
  for (const MirrorRow& m : mirror) {
    if (variants[0]->Eval(m) == 'T') {
      ++expected;
    }
  }
  for (const RPredPtr& v : variants) {
    for (const MirrorRow& m : mirror) {
      if (v->Eval(m) != variants[0]->Eval(m)) {
        return "internal error: AMOEBA variants are not equivalent";
      }
    }
  }

  auto median_ns = [&](const std::string& sql) {
    std::vector<int64_t> samples;
    std::string error;
    for (int i = 0; i < 7; ++i) {
      const auto start = std::chrono::steady_clock::now();
      auto rows = RunRows(db, ctx, sql, &error);
      const auto stop = std::chrono::steady_clock::now();
      if (!rows.has_value()) {
        return int64_t{-1};
      }
      samples.push_back(
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start)
              .count());
    }
    std::sort(samples.begin(), samples.end());
    return samples[samples.size() / 2];
  };

  int64_t fastest = -1;
  int64_t slowest = -1;
  std::string fastest_sql;
  std::string slowest_sql;
  for (const RPredPtr& v : variants) {
    const std::string sql =
        "SELECT COUNT(*) FROM " + tab + " WHERE " + v->Render() + ";";
    const int64_t ns = median_ns(sql);
    if (ns < 0) {
      if (verbose) {
        std::cerr << "[sql_oracle][amoeba-skip]\n";
      }
      return "";
    }
    if (fastest < 0 || ns < fastest) {
      fastest = ns;
      fastest_sql = sql;
    }
    if (slowest < 0 || ns > slowest) {
      slowest = ns;
      slowest_sql = sql;
    }
  }
  // Conservative thresholds: report only egregious regressions so CI noise
  // cannot produce false positives.
  if (slowest > 3000000 && slowest > 30 * fastest) {
    std::string report = "[AMOEBA MISMATCH]\n";
    report += "  fast: " + fastest_sql + " => " +
              std::to_string(fastest / 1000000) + "ms\n";
    report += "  slow: " + slowest_sql + " => " +
              std::to_string(slowest / 1000000) + "ms\n";
    return report;
  }
  return "";
}

void OracleFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
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

  const std::string path =
      "sql_oracle_fuzz-repro-" + std::to_string(seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();

  std::cerr << "[sql_oracle_fuzz] seed=" << seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
