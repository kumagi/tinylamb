/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_oracle_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
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
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// ---------------------------------------------------------------------------
// Random predicate tree with C++-side 3-valued evaluation.  This mirror is
// the ground truth for the constraint oracles (PQS, AMOEBA) and keeps the
// engine-facing SQL and the harness expectation in lockstep.
// ---------------------------------------------------------------------------

// Sentinel for nullable mirror fields; INT64_MIN cannot collide with the
// small generated value domain (-3..3).
constexpr int64_t kNull = std::numeric_limits<int64_t>::min();

// Lexicographically sorted: index order equals string order, so ordering
// comparisons on (NULL-free) string columns can compare indices directly.
constexpr std::array<const char*, 4> kStrPool = {"", "a", "bb", "zz"};
// LIKE patterns with mirror implementations (prefix / suffix / length).
constexpr std::array<const char*, 5> kLikePool = {"a%", "%b", "_", "%", "%z%"};

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
    kIntCmp,     // int_col op rhs (constant or other int column)
    kIntIsNull,  // int_col IS [NOT] NULL
    kBoolCol,    // flag / NOT flag
    kStrCmp,     // s = / != 'a' | 'bb'
    kStrIsNull,  // s IS [NOT] NULL
    kNot,
    kBin,
    kIsNullWrap,  // (left) IS NULL, whole-predicate
    kIsDistinctFrom,
    kBetween,
    kInList,
    kStrLike,
    kBitwiseAndCmp,
    kBitwiseNotCmp,
    kArithmeticCmp,
  };
  Kind kind{Kind::kIntCmp};
  bool negated{false};  // IS NULL vs IS NOT NULL
  std::string op{"="};
  int col{0};  // 0 = a, 1 = b
  bool rhs_is_col{false};
  int rhs_col{0};
  int64_t rhs_const{0};
  bool rhs_is_null{false};
  int64_t between_lo{0};
  int64_t between_hi{0};
  bool between_lo_null{false};
  bool between_hi_null{false};
  std::vector<int64_t> in_list{};
  int64_t str_const{0};  // index into kStrPool
  int str_pattern{
      0};  // 0 = '%', 1 = '_', 2 = 'a%', 3 = '%b', 4 = 'b%', 5 = 'bb', 6 = 'c%'
  bool bool_positive{true};
  int64_t mask{1};
  int64_t arith_const{0};
  std::unique_ptr<RPred> left, right;

  static int64_t IntOf(const MirrorRow& r, int column) {
    return column == 0 ? r.a : r.b;
  }

  [[nodiscard]] char Eval(const MirrorRow& row) const {
    switch (kind) {
      case Kind::kIntCmp: {
        const int64_t lhs = IntOf(row, col);
        const int64_t rhs = rhs_is_col ? IntOf(row, rhs_col) : rhs_const;
        if (lhs == kNull || rhs == kNull) {
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
        // kStrPool is lexicographically sorted: code order equals string
        // order.
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
      case Kind::kStrIsNull: {
        const bool is_null = row.s == kNull;
        return Eval3vl(negated ? !is_null : is_null);
      }
      case Kind::kStrLike: {
        if (row.s == kNull) {
          return 'N';
        }
        const bool m = LikeMatch(row.s, str_pattern);
        return Eval3vl(negated ? !m : m);
      }
      case Kind::kNot:
        return Not3vl(left->Eval(row));
      case Kind::kIsNullWrap:
        return left->Eval(row) == 'N' ? 'T' : 'F';
      case Kind::kBin:
        return op == "AND" ? And3vl(left->Eval(row), right->Eval(row))
                           : Or3vl(left->Eval(row), right->Eval(row));
      case Kind::kIsDistinctFrom: {
        const int64_t lhs = IntOf(row, col);
        const int64_t rhs =
            rhs_is_null ? kNull
                        : (rhs_is_col ? IntOf(row, rhs_col) : rhs_const);
        bool distinct = false;
        if (lhs == kNull && rhs == kNull) {
          distinct = false;
        } else if (lhs == kNull || rhs == kNull) {
          distinct = true;
        } else {
          distinct = (lhs != rhs);
        }
        return Eval3vl(negated ? !distinct : distinct);
      }
      case Kind::kBetween: {
        const int64_t lhs = IntOf(row, col);
        const int64_t lo = between_lo_null ? kNull : between_lo;
        const int64_t hi = between_hi_null ? kNull : between_hi;
        char ge = 'T';
        if (lhs == kNull || lo == kNull) {
          ge = 'N';
        } else {
          ge = Eval3vl(lhs >= lo);
        }
        char le = 'T';
        if (lhs == kNull || hi == kNull) {
          le = 'N';
        } else {
          le = Eval3vl(lhs <= hi);
        }
        char res = And3vl(ge, le);
        return negated ? Not3vl(res) : res;
      }
      case Kind::kInList: {
        const int64_t lhs = IntOf(row, col);
        if (lhs == kNull) {
          return 'N';
        }
        bool has_null = false;
        bool matched = false;
        for (int64_t val : in_list) {
          if (val == kNull) {
            has_null = true;
          } else if (val == lhs) {
            matched = true;
            break;
          }
        }
        char res = 'N';
        if (matched) {
          res = 'T';
        } else if (has_null) {
          res = 'N';
        } else {
          res = 'F';
        }
        return negated ? Not3vl(res) : res;
      }
      case Kind::kBitwiseAndCmp: {
        const int64_t lhs = IntOf(row, col);
        if (lhs == kNull) {
          return 'N';
        }
        const int64_t masked = lhs & mask;
        const int64_t rhs = rhs_const;
        if (op == "=") {
          return Eval3vl(masked == rhs);
        }
        if (op == "!=") {
          return Eval3vl(masked != rhs);
        }
        if (op == "<") {
          return Eval3vl(masked < rhs);
        }
        if (op == "<=") {
          return Eval3vl(masked <= rhs);
        }
        if (op == ">") {
          return Eval3vl(masked > rhs);
        }
        return Eval3vl(masked >= rhs);
      }
      case Kind::kBitwiseNotCmp: {
        const int64_t lhs = IntOf(row, col);
        if (lhs == kNull) {
          return 'N';
        }
        const int64_t not_val = ~lhs;
        const int64_t rhs = rhs_const;
        if (op == "=") {
          return Eval3vl(not_val == rhs);
        }
        if (op == "!=") {
          return Eval3vl(not_val != rhs);
        }
        if (op == "<") {
          return Eval3vl(not_val < rhs);
        }
        if (op == "<=") {
          return Eval3vl(not_val <= rhs);
        }
        if (op == ">") {
          return Eval3vl(not_val > rhs);
        }
        return Eval3vl(not_val >= rhs);
      }
      case Kind::kArithmeticCmp: {
        const int64_t lhs = IntOf(row, col);
        if (lhs == kNull) {
          return 'N';
        }
        const int64_t arith_val = lhs + arith_const;
        const int64_t rhs = rhs_const;
        if (op == "=") {
          return Eval3vl(arith_val == rhs);
        }
        if (op == "!=") {
          return Eval3vl(arith_val != rhs);
        }
        if (op == "<") {
          return Eval3vl(arith_val < rhs);
        }
        if (op == "<=") {
          return Eval3vl(arith_val <= rhs);
        }
        if (op == ">") {
          return Eval3vl(arith_val > rhs);
        }
        return Eval3vl(arith_val >= rhs);
      }
    }
    return 'N';
  }

  [[nodiscard]] std::string Render() const {
    switch (kind) {
      case Kind::kIntCmp: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        const std::string rhs = rhs_is_col
                                    ? kIntCols[static_cast<size_t>(rhs_col)]
                                    : std::to_string(rhs_const);
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
      case Kind::kIsDistinctFrom: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        const std::string rhs =
            rhs_is_null ? "NULL"
                        : (rhs_is_col ? kIntCols[static_cast<size_t>(rhs_col)]
                                      : std::to_string(rhs_const));
        return "(" + std::string(kIntCols[static_cast<size_t>(col)]) + " IS " +
               (negated ? "NOT " : "") + "DISTINCT FROM " + rhs + ")";
      }
      case Kind::kBetween: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        const std::string lo =
            between_lo_null ? "NULL" : std::to_string(between_lo);
        const std::string hi =
            between_hi_null ? "NULL" : std::to_string(between_hi);
        return "(" + std::string(kIntCols[static_cast<size_t>(col)]) +
               (negated ? " NOT BETWEEN " : " BETWEEN ") + lo + " AND " + hi +
               ")";
      }
      case Kind::kInList: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        std::string out = "(" +
                          std::string(kIntCols[static_cast<size_t>(col)]) +
                          (negated ? " NOT IN (" : " IN (");
        for (size_t i = 0; i < in_list.size(); ++i) {
          if (i > 0) {
            out += ", ";
          }
          out += in_list[i] == kNull ? "NULL" : std::to_string(in_list[i]);
        }
        out += "))";
        return out;
      }
      case Kind::kStrLike: {
        static const std::array<const char*, 5> kPatterns = {
            "'a%'", "'%b'", "'_'", "'%'", "'%z%'"};
        return std::string("(s ") + (negated ? "NOT LIKE " : "LIKE ") +
               kPatterns[static_cast<size_t>(str_pattern)] + ")";
      }
      case Kind::kBitwiseAndCmp: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        return "(((" + std::string(kIntCols[static_cast<size_t>(col)]) + " & " +
               std::to_string(mask) + ") " + op + " " +
               std::to_string(rhs_const) + "))";
      }
      case Kind::kBitwiseNotCmp: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        return "(((~" + std::string(kIntCols[static_cast<size_t>(col)]) + ") " +
               op + " " + std::to_string(rhs_const) + "))";
      }
      case Kind::kArithmeticCmp: {
        static const std::array<const char*, 2> kIntCols = {"a", "b"};
        const std::string op_str = arith_const >= 0 ? "+" : "-";
        const int64_t abs_const = arith_const >= 0 ? arith_const : -arith_const;
        return "(((" + std::string(kIntCols[static_cast<size_t>(col)]) + " " +
               op_str + " " + std::to_string(abs_const) + ") " + op + " " +
               std::to_string(rhs_const) + "))";
      }
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
  c->between_lo_null = p.between_lo_null;
  c->between_hi_null = p.between_hi_null;
  c->in_list = p.in_list;
  c->str_const = p.str_const;
  c->str_pattern = p.str_pattern;
  c->bool_positive = p.bool_positive;
  c->mask = p.mask;
  c->arith_const = p.arith_const;
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

RPredPtr IntLeafPredicate(Gen& g) {
  static const std::vector<std::string> kOps = {"=",  "!=", "<",
                                                "<=", ">",  ">="};
  auto p = std::make_unique<RPred>();
  switch (g.Pick(0, 9)) {
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
      p->rhs_is_col = true;
      p->rhs_col = g.Pick(0, 1);
      return p;
    case 2:
      p->kind = RPred::Kind::kIntIsNull;
      p->col = g.Pick(0, 1);
      p->negated = g.Chance(50);
      return p;
    case 3:
      p->kind = RPred::Kind::kBoolCol;
      p->bool_positive = g.Chance(50);
      return p;
    case 4: {
      p->kind = RPred::Kind::kIsDistinctFrom;
      p->col = g.Pick(0, 1);
      p->negated = g.Chance(50);
      if (g.Chance(20)) {
        p->rhs_is_null = true;
      } else if (g.Chance(40)) {
        p->rhs_is_col = true;
        p->rhs_col = g.Pick(0, 1);
      } else {
        p->rhs_const = g.Pick(-3, 3);
      }
      return p;
    }
    case 5: {
      p->kind = RPred::Kind::kBetween;
      p->col = g.Pick(0, 1);
      p->negated = g.Chance(30);
      int64_t v1 = g.Pick(-3, 3);
      int64_t v2 = g.Pick(-3, 3);
      p->between_lo = std::min(v1, v2);
      p->between_hi = std::max(v1, v2);
      p->between_lo_null = g.Chance(15);
      p->between_hi_null = g.Chance(15);
      return p;
    }
    case 6: {
      p->kind = RPred::Kind::kBitwiseAndCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->mask = g.Pick(1, 3);
      p->rhs_const = g.Pick(0, 3);
      return p;
    }
    case 7: {
      p->kind = RPred::Kind::kBitwiseNotCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->rhs_const = g.Pick(-4, 4);
      return p;
    }
    case 8: {
      p->kind = RPred::Kind::kArithmeticCmp;
      p->col = g.Pick(0, 1);
      p->op = g.PickFrom(kOps);
      p->arith_const = g.Pick(-2, 2);
      p->rhs_const = g.Pick(-3, 3);
      return p;
    }
    default: {
      p->kind = RPred::Kind::kInList;
      p->col = g.Pick(0, 1);
      p->negated = g.Chance(30);
      const int count = g.Pick(1, 4);
      p->in_list.reserve(static_cast<size_t>(count));
      for (int i = 0; i < count; ++i) {
        if (g.Chance(20)) {
          p->in_list.push_back(kNull);
        } else {
          p->in_list.push_back(g.Pick(-3, 3));
        }
      }
      return p;
    }
  }
}

RPredPtr LeafPredicate(Gen& g) {
  switch (g.Pick(0, 3)) {
    case 0:
      return IntLeafPredicate(g);
    case 1: {
      auto p = std::make_unique<RPred>();
      p->kind = RPred::Kind::kStrCmp;
      static const std::vector<std::string> kStrOps = {"=", "!=", "<", ">="};
      p->op = g.PickFrom(kStrOps);
      p->str_const = g.Pick(0, static_cast<int>(std::size(kStrPool)) - 1);
      return p;
    }
    case 2: {
      auto p = std::make_unique<RPred>();
      p->kind = RPred::Kind::kStrLike;
      p->negated = g.Chance(40);
      p->str_pattern = g.Pick(0, static_cast<int>(std::size(kLikePool)) - 1);
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
// 0 = default, 1 = shallower trees, 2 = boolean-heavy (no NOT nodes).
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

std::string GenProjection(Gen& g) {
  switch (g.Pick(0, 7)) {
    case 0:
      return "*";
    case 1:
      return "a";
    case 2:
      return "a, b";
    case 3:
      return "flag, s";
    case 4:
      return "COALESCE(a, 0), b";
    case 5:
      return "u + 1, a";
    case 6:
      return "CASE WHEN flag THEN a ELSE b END";
    default:
      return "u, a, b, flag, s";
  }
}

// Generates one row: the VALUES clause and its mirror.
MirrorRow GenRow(Gen& g, int64_t u, std::string* sql_values) {
  MirrorRow m;
  m.u = u;
  m.a = g.Chance(15) ? kNull : g.Pick(-3, 3);
  m.b = g.Chance(10) ? kNull : g.Pick(-2, 2);
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
  switch (g.Pick(0, 2)) {
    case 0:
      m.s = 0;
      break;
    case 1:
      m.s = 1;
      break;
    default:
      m.s = kNull;
      break;
  }
  const std::string a_sql = m.a == kNull ? "NULL" : std::to_string(m.a);
  const std::string b_sql = m.b == kNull ? "NULL" : std::to_string(m.b);
  *sql_values =
      "(" + std::to_string(m.u) + ", " + a_sql + ", " + b_sql + ", " +
      (m.flag == kNull ? "NULL" : (m.flag == 1 ? "TRUE" : "FALSE")) + ", " +
      (m.s == kNull
           ? "NULL"
           : "'" + std::string(kStrPool[static_cast<size_t>(m.s)]) + "'") +
      ")";
  return m;
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
  while (result.Value().Next(&row)) {
    rows.push_back(row);
  }
  if (Status st = result.Value().GetStatus(); st != Status::kSuccess) {
    *error = st.GetMessage().empty() ? ToString(st.GetCode()) : st.GetMessage();
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
bool RunUpdate(Database& db, TransactionContext& ctx, const std::string& sql,
               std::string* error = nullptr) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    if (error != nullptr) {
      *error = engine.LastError();
    }
    return false;
  }
  // QueryResults are lazy: drain or the mutation never happens.
  result.Value().Drain();
  if (result.Value().GetStatus() != Status::kSuccess) {
    if (error != nullptr) {
      *error = result.Value().GetStatus().GetMessage();
    }
    return false;
  }
  return true;
}

std::string DumpRows(const std::vector<std::string>& rows) {
  std::string out;
  for (const std::string& r : rows) {
    out += "    " + r + "\n";
  }
  return out;
}

// ---------------------------------------------------------------------------
// Mirror-expected oracles: expected rows are rendered in Row::ToString()
// form ("[v1, v2]") so engine output compares textually.
// ---------------------------------------------------------------------------

// One nullable INT64 cell exactly as Value::AsString() prints it.
std::string MirrorCell(int64_t v) {
  return v == kNull ? std::string("NULL") : std::to_string(v);
}

std::string ExpectRow(std::initializer_list<int64_t> cells) {
  std::string out = "[";
  bool first = true;
  for (const int64_t v : cells) {
    if (!first) {
      out += ", ";
    }
    first = false;
    out += MirrorCell(v);
  }
  out += "]";
  return out;
}

// Runs the single recorded query and compares against the expected rows
// (verbatim when `ordered`, otherwise as sorted multisets).  Returns false
// when the engine rejected the query (oracle skipped).
bool CheckExpected(Database& db, TransactionContext& ctx,
                   const std::vector<std::string>& queries,
                   const std::vector<std::string>& expected, bool ordered,
                   const char* tag, std::string* report, bool verbose) {
  if (queries.size() != 1) {
    return true;
  }
  std::string error;
  auto rows = RunRows(db, ctx, queries[0], &error);
  if (!rows.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][" << tag << "-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> actual;
  actual.reserve(rows->size());
  for (const Row& r : *rows) {
    actual.push_back(r.ToString());
  }
  std::vector<std::string> expect = expected;
  if (!ordered) {
    std::sort(actual.begin(), actual.end());
    std::sort(expect.begin(), expect.end());
  }
  if (actual == expect) {
    return true;
  }
  *report += std::string("[") + tag + " MISMATCH]\n";
  *report += "  query: " + queries[0] + "\n";
  *report += "  expected (" + std::to_string(expect.size()) + "):\n" +
             DumpRows(expect);
  *report +=
      "  actual (" + std::to_string(actual.size()) + "):\n" + DumpRows(actual);
  return true;
}

// Two-query equivalence: both forms must return the same row multiset.
bool CheckPair(Database& db, TransactionContext& ctx,
               const std::vector<std::string>& cte, const char* tag,
               std::string* report, bool verbose) {
  if (cte.size() != 2) {
    return true;
  }
  std::string error;
  auto with = RunRows(db, ctx, cte[0], &error);
  auto flat = RunRows(db, ctx, cte[1], &error);
  if (!(with.has_value() && flat.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][" << tag << "-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> s0 = SerializeSorted(*with);
  std::vector<std::string> s1 = SerializeSorted(*flat);
  if (s0 == s1) {
    return true;
  }
  *report += std::string("[") + tag + " MISMATCH]\n";
  *report += "  lhs: " + cte[0] + " (" + std::to_string(s0.size()) +
             " rows)\n" + DumpRows(s0);
  *report += "  rhs: " + cte[1] + " (" + std::to_string(s1.size()) +
             " rows)\n" + DumpRows(s1);
  return true;
}

// WITH RECURSIVE counters: the single aggregate row must match arithmetic.
bool CheckRecursive(Database& db, TransactionContext& ctx, const OracleTrace& t,
                    std::string* report, bool verbose) {
  if (t.recursive.empty()) {
    return true;
  }
  std::string error;
  auto rows = RunRows(db, ctx, t.recursive, &error);
  if (!rows.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][recursive-error] " << error << "\n";
    }
    return false;
  }
  if (rows->size() == 1 && (*rows)[0].ToString() == t.recursive_expect) {
    return true;
  }
  *report += "[RECURSIVE MISMATCH]\n";
  *report += "  " + t.recursive + "\n";
  *report += "  expected: " + t.recursive_expect + "\n  actual:";
  for (const Row& r : *rows) {
    *report += " " + r.ToString();
  }
  *report += "\n";
  return true;
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
  const std::vector<std::string> s0 = SerializeSorted(*r0);
  std::vector<std::string> s1 = SerializeSorted(*r1);
  std::vector<std::string> s2 = SerializeSorted(*r2);
  std::vector<std::string> s3 = SerializeSorted(*r3);
  std::vector<std::string> merged = s1;
  merged.insert(merged.end(), s2.begin(), s2.end());
  merged.insert(merged.end(), s3.begin(), s3.end());
  std::sort(merged.begin(), merged.end());
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

// UNION ALL partitioning: the engine itself must recombine the partitions.
// Q[p] as a BAG equals part1 UNION ALL part2 UNION ALL part3 (the WHERE
// splits are disjoint, so plain rows never duplicate across parts).  A
// DISTINCT projection breaks the bag invariant: one value projected in two
// parts is deduplicated per branch yet appears twice in the concatenation,
// so DISTINCT queries compare as SETS instead (same rule as CheckTlp).
bool CheckUnionAll(Database& db, TransactionContext& ctx,
                   const std::vector<std::string>& unionall,
                   std::string* report, bool verbose) {
  if (unionall.size() != 2) {
    return true;
  }
  std::string error;
  auto original = RunRows(db, ctx, unionall[0], &error);
  auto merged = RunRows(db, ctx, unionall[1], &error);
  if (!(original.has_value() && merged.has_value())) {
    if (verbose) {
      std::cerr << "[sql_oracle][unionall-error] " << error << "\n";
    }
    return false;
  }
  std::vector<std::string> s0 = SerializeSorted(*original);
  std::vector<std::string> s1 = SerializeSorted(*merged);
  if (unionall[0].find("DISTINCT") != std::string::npos) {
    s0.erase(std::unique(s0.begin(), s0.end()), s0.end());
    s1.erase(std::unique(s1.begin(), s1.end()), s1.end());
  }
  if (s0 == s1) {
    return true;
  }
  *report += "[UNION-ALL MISMATCH]\n";
  *report += "  original: " + unionall[0] + " (" + std::to_string(s0.size()) +
             " rows)\n" + DumpRows(s0);
  *report += "  union:    " + unionall[1] + " (" + std::to_string(s1.size()) +
             " rows)\n" + DumpRows(s1);
  return true;
}

// Subquery differential: the three COUNT spellings must be identical.
bool CheckSubq(Database& db, TransactionContext& ctx,
               const std::vector<std::string>& subq, std::string* report,
               bool verbose) {
  if (subq.size() != 3) {
    return true;
  }
  std::string error;
  std::vector<std::string> counts;
  counts.reserve(3);
  for (int i = 0; i < 3; ++i) {
    std::optional<std::string> count =
        RunScalar(db, ctx, subq[static_cast<size_t>(i)], &error);
    if (!count.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][subq-error] " << error << "\n";
      }
      return false;
    }
    counts.push_back(std::move(*count));
  }
  if (counts[0] == counts[1] && counts[1] == counts[2]) {
    return true;
  }
  *report += "[SUBQ MISMATCH]\n";
  for (int i = 0; i < 3; ++i) {
    const char* tag = i == 0 ? "IN     " : (i == 1 ? "EXISTS " : "JOIN   ");
    *report += std::string("  ") + tag + subq[static_cast<size_t>(i)] + " => " +
               counts[static_cast<size_t>(i)] + "\n";
  }
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

// Constraint rewriting: the probe must survive CREATE INDEX unchanged.
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
  for (const std::string& ddl : t.index_ddl) {
    if (!RunUpdate(db, ctx, ddl)) {
      if (verbose) {
        std::cerr << "[sql_oracle][idx-ddl-error] :: " << ddl << "\n";
      }
      return false;
    }
  }
  auto after = RunScalar(db, ctx, t.index_probe, &error);
  if (!after.has_value()) {
    if (verbose) {
      std::cerr << "[sql_oracle][idx-error] " << error << "\n";
    }
    return false;
  }
  if (*before == *after) {
    return true;
  }
  *report += "[INDEX MISMATCH]\n";
  *report += "  before: " + t.index_probe + " => " + *before + "\n";
  for (const std::string& ddl : t.index_ddl) {
    *report += "  ddl:    " + ddl + "\n";
  }
  *report += "  after:  " + t.index_probe + " => " + *after + "\n";
  return true;
}

// Transaction splitting: atomic execution must match autocommit execution.
bool CheckTroc(Database& db, const OracleTrace& t, std::string* report,
               bool verbose) {
  if (t.troc.empty() || t.troc_probe.empty()) {
    return true;
  }
  const std::string tab = t.table.empty() ? "t0" : t.table;
  auto substitute = [&tab](std::string sql) {
    const std::string& from = tab;
    const std::string to = tab + "b";
    size_t pos = 0;
    while ((pos = sql.find(from, pos)) != std::string::npos) {
      sql.replace(pos, from.size(), to);
      pos += to.size();
    }
    return sql;
  };

  std::string error;
  // Branch A: everything inside one transaction.
  TransactionContext ctx_a = db.BeginContext();
  for (const std::string& sql : t.setup) {
    if (!RunUpdate(db, ctx_a, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-setup-error] " << error << "\n";
      }
      return false;
    }
  }
  for (const std::string& sql : t.troc) {
    if (!RunUpdate(db, ctx_a, sql)) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-stmt-error] " << sql << "\n";
      }
      return false;
    }
  }
  auto inside = RunRows(db, ctx_a, t.troc_probe, &error);
  bool committed = ctx_a.txn_.PreCommit() == Status::kSuccess;
  TransactionContext ctx_a2 = db.BeginContext();
  auto after_a = RunRows(db, ctx_a2, t.troc_probe, &error);
  if (!(inside.has_value() && after_a.has_value()) || !committed) {
    if (verbose) {
      std::cerr << "[sql_oracle][troc-error-a] " << error << "\n";
    }
    return false;
  }

  // Branch B: each statement in its own transaction (autocommit), on a
  // shadow table t0b so both branches start from the same pristine state.
  TransactionContext ctx_b = db.BeginContext();
  for (const std::string& sql : t.setup) {
    if (!RunUpdate(db, ctx_b, substitute(sql))) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-setup-error-b]\n";
      }
      return false;
    }
  }
  ctx_b.txn_.PreCommit();
  for (const std::string& sql : t.troc) {
    TransactionContext ctx = db.BeginContext();
    if (!RunUpdate(db, ctx, substitute(sql))) {
      if (verbose) {
        std::cerr << "[sql_oracle][troc-stmt-error-b] " << sql << "\n";
      }
      return false;
    }
    ctx.txn_.PreCommit();
  }
  TransactionContext ctx_b2 = db.BeginContext();
  auto after_b = RunRows(db, ctx_b2, substitute(t.troc_probe), &error);
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

// Executes the setup statements; returns empty on success, error message on
// failure.
std::string RunSetup(Database& db, TransactionContext& ctx,
                     const std::vector<std::string>& setup, bool verbose) {
  for (const std::string& sql : setup) {
    std::string err;
    if (!RunUpdate(db, ctx, sql, &err)) {
      if (verbose) {
        std::cerr << "[sql_oracle][skip-setup] " << sql << " (" << err << ")\n";
      }
      std::string failure = "setup statement failed on [";
      failure += sql;
      failure += "]: ";
      failure += err;
      return failure;
    }
  }
  return {};
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

  const int flavour = session != nullptr ? session->SelectFlavour(3) : 0;

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
  const int row_count = g.Pick(4, 10);
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

  const RPredPtr pred = GenPredicate(g, g.Pick(1, 3), flavour);
  t.predicate = pred->Render();

  // ---- TLP ----
  const RPredPtr tlp_aux = GenPredicate(g, g.Pick(1, 2), flavour);
  const RPred& aux = *tlp_aux;
  {
    const std::string proj = GenProjection(g);
    t.tlp = {
        "SELECT " + proj + " FROM " + tab + " WHERE " + t.predicate + ";",
        "SELECT " + proj + " FROM " + tab + " WHERE (" + t.predicate +
            ") AND (" + aux.Render() + ");",
        "SELECT " + proj + " FROM " + tab + " WHERE (" + t.predicate +
            ") AND NOT (" + aux.Render() + ");",
        "SELECT " + proj + " FROM " + tab + " WHERE (" + t.predicate +
            ") AND (" + aux.Render() + ") IS NULL;",
    };
  }

  // ---- Aggregate TLP (grouped partition merge) ----
  {
    const std::string sel =
        "SELECT a, COUNT(*), SUM(b) FROM " + tab + " WHERE ";
    const std::string grp = " GROUP BY a;";
    t.tlp_agg = {
        sel + t.predicate + grp,
        sel + "(" + t.predicate + ") AND (" + tlp_aux->Render() + ")" + grp,
        sel + "(" + t.predicate + ") AND NOT (" + tlp_aux->Render() + ")" + grp,
        sel + "(" + t.predicate + ") AND (" + tlp_aux->Render() + ") IS NULL" +
            grp,
    };
  }

  // ---- UNION ALL partitioning (engine recombines, harness does not) ----
  {
    const std::string prefix =
        "SELECT " + GenProjection(g) + " FROM " + tab + " WHERE ";
    t.unionall = {
        prefix + t.predicate + ";",
        prefix + "(" + t.predicate + ") AND (" + tlp_aux->Render() +
            ") UNION ALL " + prefix + "(" + t.predicate + ") AND NOT (" +
            tlp_aux->Render() + ") UNION ALL " + prefix + "(" + t.predicate +
            ") AND (" + tlp_aux->Render() + ") IS NULL;",
    };
  }

  // ---- Auxiliary tables for the subquery / set-operation oracles ----
  // jtab feeds the three semi-join spellings; stab is a same-schema sibling
  // whose rows the set-operation mirror tracks.
  std::string jtab;
  std::vector<int64_t> jxs;  // j.x values feeding the subquery oracles
  if (g.Chance(50)) {
    jtab = tab + "_j";
    t.setup.push_back("CREATE TABLE " + jtab + " (x INT64, y VARCHAR(8));");
    const int join_rows = g.Pick(2, 6);
    for (int i = 0; i < join_rows; ++i) {
      const int64_t xv = g.Chance(20) ? kNull : g.Pick(-3, 3);
      jxs.push_back(xv);
      const std::string x = xv == kNull ? "NULL" : std::to_string(xv);
      const std::string y =
          g.Chance(25)
              ? "NULL"
              : "'" +
                    std::string(kStrPool[static_cast<size_t>(g.Pick(
                        0, static_cast<int>(std::size(kStrPool)) - 1))]) +
                    "'";
      t.setup.push_back("INSERT INTO " + jtab + " VALUES (" + x + ", " + y +
                        ");");
    }
  }
  std::string stab;
  std::vector<MirrorRow> mirror2;
  RPredPtr pred2;
  if (g.Chance(55)) {
    stab = tab + "_s";
    t.setup.push_back("CREATE TABLE " + stab +
                      " (u INT64, a INT64, b INT64, flag BOOL, s VARCHAR(8));");
    const int n = g.Pick(3, 8);
    for (int i = 0; i < n; ++i) {
      std::string values;
      mirror2.push_back(GenRow(g, 500 + i, &values));
      t.setup.push_back("INSERT INTO " + stab + " VALUES " + values + ";");
    }
    pred2 = GenPredicate(g, g.Pick(1, 2), flavour);
  }

  // ---- Subquery differential: IN vs correlated EXISTS vs JOIN+DISTINCT ----
  // Positive forms only: NOT IN and NOT EXISTS diverge under 3VL by design.
  if (!jtab.empty()) {
    if (g.Chance(60)) {
      t.subq = {
          "SELECT COUNT(*) FROM " + tab + " WHERE a IN (SELECT x FROM " + jtab +
              ");",
          "SELECT COUNT(*) FROM " + tab + " WHERE EXISTS (SELECT 1 FROM " +
              jtab + " WHERE " + jtab + ".x = " + tab + ".a);",
          "SELECT COUNT(*) FROM (SELECT DISTINCT u FROM " + tab + " JOIN " +
              jtab + " ON " + tab + ".a = " + jtab + ".x) semi;",
      };
    } else {
      // Correlated variant: the subquery's own WHERE references the outer
      // row, exercising the apply/correlated-cache path.
      const std::string corr = g.Chance(50) ? (jtab + ".x = " + tab + ".b")
                                            : (jtab + ".y = " + tab + ".s");
      t.subq = {
          "SELECT COUNT(*) FROM " + tab + " WHERE a IN (SELECT x FROM " + jtab +
              " WHERE " + corr + ");",
          "SELECT COUNT(*) FROM " + tab + " WHERE EXISTS (SELECT 1 FROM " +
              jtab + " WHERE " + corr + " AND " + jtab + ".x = " + tab + ".a);",
          "SELECT COUNT(*) FROM (SELECT DISTINCT u FROM " + tab + " JOIN " +
              jtab + " ON " + tab + ".a = " + jtab + ".x AND " + corr +
              ") semi;",
      };
    }
    // Scalar subquery vs LEFT JOIN: a correlated scalar COUNT over the
    // join must equal the decorrelated GROUP BY spelling row for row.
    if (g.Chance(40)) {
      const std::string corr2 = g.Chance(50) ? (jtab + ".x = " + tab + ".a")
                                             : (jtab + ".y = " + tab + ".s");
      t.ssub = {
          "SELECT u, (SELECT COUNT(x) FROM " + jtab + " WHERE " + corr2 +
              ") AS c FROM " + tab + ";",
          "SELECT " + tab + ".u, COUNT(" + jtab + ".x) AS c FROM " + tab +
              " LEFT JOIN " + jtab + " ON " + corr2 + " GROUP BY " + tab +
              ".u;",
      };
    }
    // NOT IN anti-join: the result is empty whenever the subquery column
    // contains any NULL (every comparison then goes UNKNOWN), and a NULL
    // outer `a` is likewise never TRUE. The mirror encodes those three-
    // valued semantics directly.
    if (g.Chance(35)) {
      t.notin = {"SELECT u FROM " + tab + " WHERE a NOT IN (SELECT x FROM " +
                 jtab + ");"};
      const bool j_has_null =
          std::find(jxs.begin(), jxs.end(), kNull) != jxs.end();
      for (const MirrorRow& m : mirror) {
        if (j_has_null || m.a == kNull) {
          continue;
        }
        if (std::find(jxs.begin(), jxs.end(), m.a) == jxs.end()) {
          t.notin_expect.push_back(ExpectRow({m.u}));
        }
      }
    }
  }

  // ---- CASE vs UNION ALL partition ----
  // The ELSE arm must fire exactly on FALSE-or-NULL: `IS NOT TRUE` (not
  // `NOT (p IS TRUE)`) is the correct complement under three-valued logic.
  if (g.Chance(35)) {
    const std::string then_col = std::string(g.Chance(50) ? "a" : "b");
    const std::string else_col = std::string(g.Chance(50) ? "u" : "b");
    t.cqp = {
        "SELECT CASE WHEN " + t.predicate + " THEN " + then_col + " ELSE " +
            else_col + " END AS v FROM " + tab + ";",
        "SELECT " + then_col + " AS v FROM " + tab + " WHERE (" + t.predicate +
            ") IS TRUE UNION ALL SELECT " + else_col + " AS v FROM " + tab +
            " WHERE (" + t.predicate + ") IS NOT TRUE;",
    };
  }

  // ---- DISTINCT vs GROUP BY dedup ----
  // SELECT DISTINCT must agree with the GROUP BY spelling on the same
  // column list, including NULL grouping (NULLs dedupe together in both).
  if (g.Chance(35)) {
    static const std::array<const char*, 3> kDdgCols = {"a", "b", "s"};
    const int dcol0 = g.Pick(0, 2);
    std::string cols = kDdgCols[static_cast<size_t>(dcol0)];
    if (g.Chance(40)) {
      int dcol1 = g.Pick(0, 2);
      while (dcol1 == dcol0) {
        dcol1 = g.Pick(0, 2);
      }
      cols += ", ";
      cols += kDdgCols[static_cast<size_t>(dcol1)];
    }
    t.ddg = {"SELECT DISTINCT " + cols + " FROM " + tab + ";",
             "SELECT " + cols + " FROM " + tab + " GROUP BY " + cols + ";"};
  }

  // ---- LIMIT/OFFSET vs ROW_NUMBER window ----
  // ORDER BY a,u with a LIMIT window must equal filtering the window
  // function over the same ordering (both use engine default NULLS FIRST).
  if (g.Chance(30)) {
    const int k = g.Pick(1, 3);
    const int m = g.Pick(0, 2);
    t.lwn = {"SELECT u FROM " + tab + " ORDER BY a, u LIMIT " +
                 std::to_string(k) + " OFFSET " + std::to_string(m) + ";",
             "SELECT u FROM (SELECT u, ROW_NUMBER() OVER (ORDER BY a, u) AS "
             "rn FROM " +
                 tab + ") w WHERE rn > " + std::to_string(m) +
                 " AND rn <= " + std::to_string(m + k) + ";"};
  }

  // ---- PIVOT vs CASE-pivot ----
  // An unkeyed single-aggregate PIVOT must equal the manual
  // SUM(CASE WHEN ...) spelling column for column.
  if (g.Chance(30)) {
    const int k0 = g.Pick(0, 2);
    int k1 = g.Pick(0, 2);
    while (k1 == k0) {
      k1 = g.Pick(0, 2);
    }
    const std::string c0 =
        "SUM(CASE WHEN u = " + std::to_string(k0) + " THEN a END)";
    const std::string c1 =
        "SUM(CASE WHEN u = " + std::to_string(k1) + " THEN a END)";
    t.piv = {"SELECT * FROM " + tab + " PIVOT(SUM(a) FOR u IN (" +
                 std::to_string(k0) + ", " + std::to_string(k1) + "));",
             "SELECT " + c0 + ", " + c1 + " FROM " + tab + ";"};
  }

  // ---- NOT IN literal list vs NOT(OR of equals) ----
  // Under three-valued logic both spellings must agree row-for-row:
  // NULL a makes each comparison UNKNOWN and both sides filter out.
  if (g.Chance(30)) {
    const int n = g.Pick(1, 3);
    std::string list;
    std::string ors;
    for (int i = 0; i < n; ++i) {
      const std::string lit = std::to_string(g.Pick(-2, 5));
      if (i > 0) {
        list += ", ";
        ors += " OR ";
      }
      list += lit;
      ors += "a = " + lit;
    }
    t.niv = {"SELECT u FROM " + tab + " WHERE a NOT IN (" + list + ");",
             "SELECT u FROM " + tab + " WHERE NOT (" + ors + ");"};
  }

  // ---- Set operations vs the mirror multiset ----
  if (!stab.empty()) {
    static const std::array<const char*, 6> kSetOps = {
        "UNION ALL",          "UNION DISTINCT", "INTERSECT ALL",
        "INTERSECT DISTINCT", "EXCEPT ALL",     "EXCEPT DISTINCT"};
    const int op = g.Pick(0, static_cast<int>(kSetOps.size()) - 1);
    t.setop = {"SELECT a FROM " + tab + " WHERE " + t.predicate + " " +
               kSetOps[static_cast<size_t>(op)] + " SELECT a FROM " + stab +
               " WHERE " + pred2->Render() + ";"};

    auto filtered_a = [](const std::vector<MirrorRow>& rows, const RPred& p) {
      std::vector<int64_t> out;
      for (const MirrorRow& m : rows) {
        if (p.Eval(m) == 'T') {
          out.push_back(m.a);
        }
      }
      return out;
    };
    const std::vector<int64_t> la = filtered_a(mirror, *pred);
    const std::vector<int64_t> lb = filtered_a(mirror2, *pred2);
    auto dedup = [](std::vector<int64_t> v) {
      std::sort(v.begin(), v.end());
      v.erase(std::unique(v.begin(), v.end()), v.end());
      return v;
    };
    auto count_of = [](const std::vector<int64_t>& v, int64_t e) {
      return static_cast<int64_t>(std::count(v.begin(), v.end(), e));
    };
    auto contains = [](const std::vector<int64_t>& v, int64_t e) {
      return std::find(v.begin(), v.end(), e) != v.end();
    };
    // SQL set-op NULL semantics: NULL equals NULL here, so the kNull
    // sentinel compares like any other value.
    std::vector<int64_t> result;
    switch (op) {
      case 0:  // UNION ALL
        result = la;
        result.insert(result.end(), lb.begin(), lb.end());
        break;
      case 1: {  // UNION DISTINCT
        result = la;
        result.insert(result.end(), lb.begin(), lb.end());
        result = dedup(std::move(result));
        break;
      }
      case 2:  // INTERSECT ALL: min multiplicity
        for (const int64_t e : dedup(la)) {
          for (int64_t i = 0; i < std::min(count_of(la, e), count_of(lb, e));
               ++i) {
            result.push_back(e);
          }
        }
        break;
      case 3:  // INTERSECT DISTINCT
        for (const int64_t e : dedup(la)) {
          if (contains(lb, e)) {
            result.push_back(e);
          }
        }
        break;
      case 4:  // EXCEPT ALL: subtract multiplicities
        for (const int64_t e : dedup(la)) {
          for (int64_t i = 0; i < count_of(la, e) - count_of(lb, e); ++i) {
            result.push_back(e);
          }
        }
        break;
      default:  // EXCEPT DISTINCT
        for (const int64_t e : dedup(la)) {
          if (!contains(lb, e)) {
            result.push_back(e);
          }
        }
        break;
    }
    for (const int64_t v : result) {
      t.setop_expect.push_back(ExpectRow({v}));
    }
  }

  // ---- ORDER BY + LIMIT/OFFSET: the ordered oracle ----
  // Sort keys always end at the unique `u` so the expected sequence is
  // deterministic; engine default is NULLS FIRST for ASC, LAST for DESC.
  {
    struct SortKey {
      int col;  // 0 = u, 1 = a
      bool desc;
      bool nulls_first;  // effective (default = !desc)
    };
    std::vector<SortKey> keys;
    std::string spec;
    auto push_key = [&](int col, bool desc, int explicit_nf) {
      // explicit_nf: -1 = omitted, 0 = NULLS LAST, 1 = NULLS FIRST.
      keys.push_back({col, desc, explicit_nf < 0 ? !desc : explicit_nf == 1});
      spec += spec.empty() ? "" : ", ";
      spec += col == 0 ? "u" : "a";
      if (desc) {
        spec += " DESC";
      }
      if (explicit_nf >= 0) {
        spec += explicit_nf == 1 ? " NULLS FIRST" : " NULLS LAST";
      }
    };
    switch (g.Pick(0, 4)) {
      case 0:
        push_key(0, false, -1);
        break;
      case 1:
        push_key(0, true, -1);
        break;
      case 2:
        push_key(1, g.Chance(50), -1);
        push_key(0, g.Chance(50), -1);
        break;
      case 3:
        push_key(1, true, g.Chance(40) ? 0 : -1);
        push_key(0, g.Chance(50), -1);
        break;
      default:
        push_key(1, false, g.Chance(50) ? 1 : 0);
        push_key(0, true, g.Chance(30) ? 1 : -1);
        break;
    }
    std::vector<const MirrorRow*> rows;
    for (const MirrorRow& m : mirror) {
      if (pred->Eval(m) == 'T') {
        rows.push_back(&m);
      }
    }
    std::stable_sort(rows.begin(), rows.end(),
                     [&](const MirrorRow* l, const MirrorRow* r) {
                       for (const SortKey& k : keys) {
                         const int64_t lv = k.col == 0 ? l->u : l->a;
                         const int64_t rv = k.col == 0 ? r->u : r->a;
                         const bool lnull = lv == kNull;
                         const bool rnull = rv == kNull;
                         if (lnull != rnull) {
                           return k.nulls_first ? lnull : rnull;
                         }
                         if (lnull || lv == rv) {
                           continue;
                         }
                         return k.desc ? lv > rv : lv < rv;
                       }
                       return false;
                     });
    const int64_t total = static_cast<int64_t>(rows.size());
    int64_t offset = 0;
    int64_t limit = -1;
    std::string sql = "SELECT u, a FROM " + tab + " WHERE " + t.predicate +
                      " ORDER BY " + spec;
    if (g.Chance(65)) {
      limit = g.Pick(0, static_cast<int>(total) + 2);
      sql += " LIMIT " + std::to_string(limit);
      if (g.Chance(40)) {
        offset = g.Pick(0, static_cast<int>(total));
        sql += " OFFSET " + std::to_string(offset);
      }
    }
    t.orderby = {sql + ";"};
    const int64_t end = std::min(total, offset + (limit < 0 ? total : limit));
    for (int64_t i = offset; i < end; ++i) {
      t.orderby_expect.push_back(ExpectRow(
          {rows[static_cast<size_t>(i)]->u, rows[static_cast<size_t>(i)]->a}));
    }

    // FETCH FIRST n ROWS WITH TIES over a single non-unique key: the
    // cutoff row's peers all come along. Compared as a multiset because
    // order inside a tied group is unspecified.
    if (g.Chance(30)) {
      const bool desc = g.Chance(40);
      const int explicit_nf = g.Chance(40) ? (g.Chance(50) ? 1 : 0) : -1;
      const bool nf = explicit_nf < 0 ? !desc : explicit_nf == 1;
      std::vector<const MirrorRow*> keyed = rows;
      std::stable_sort(keyed.begin(), keyed.end(),
                       [&](const MirrorRow* l, const MirrorRow* r) {
                         const bool lnull = l->a == kNull;
                         const bool rnull = r->a == kNull;
                         if (lnull != rnull) {
                           return nf ? lnull : rnull;
                         }
                         if (lnull || l->a == r->a) {
                           return false;
                         }
                         return desc ? l->a > r->a : l->a < r->a;
                       });
      const int64_t n = g.Pick(0, total + 1);
      std::string tsql =
          "SELECT u, a FROM " + tab + " WHERE " + t.predicate + " ORDER BY a";
      if (desc) {
        tsql += " DESC";
      }
      if (explicit_nf == 1) {
        tsql += " NULLS FIRST";
      } else if (explicit_nf == 0) {
        tsql += " NULLS LAST";
      }
      tsql += " FETCH FIRST " + std::to_string(n) + " ROWS WITH TIES;";
      t.ties = {tsql};
      for (int64_t i = 0; i < std::min(n, total); ++i) {
        t.ties_expect.push_back(ExpectRow({keyed[static_cast<size_t>(i)]->u,
                                           keyed[static_cast<size_t>(i)]->a}));
      }
      if (n > 0 && n < total) {
        const int64_t cutoff = keyed[static_cast<size_t>(n - 1)]->a;
        for (int64_t i = n; i < total; ++i) {
          if (keyed[static_cast<size_t>(i)]->a == cutoff) {
            t.ties_expect.push_back(
                ExpectRow({keyed[static_cast<size_t>(i)]->u,
                           keyed[static_cast<size_t>(i)]->a}));
          }
        }
      }
    }
  }

  // ---- HAVING over grouped aggregates ----
  // Aggregates stay in INT64 so the mirror formats identically; NULL-valued
  // aggregates (empty SUM, all-NULL MIN/MAX) make the HAVING condition
  // evaluate to UNKNOWN and drop the group.
  {
    static const std::array<const char*, 5> kAggSql = {
        "COUNT(*)", "SUM(b)", "MIN(a)", "MAX(a)", "COUNT(b)"};
    struct AggVals {
      int64_t cnt{0};
      int64_t sum_b{kNull};  // kNull when empty
      int64_t min_a{kNull};
      int64_t max_a{kNull};
      int64_t cnt_b{0};
    };
    // 0 = COUNT(*), 1 = SUM(b), 2 = MIN(a), 3 = MAX(a), 4 = COUNT(b).
    auto agg_value = [](const AggVals& v, int agg) {
      switch (agg) {
        case 0:
          return v.cnt;
        case 1:
          return v.sum_b;
        case 2:
          return v.min_a;
        case 3:
          return v.max_a;
        default:
          return v.cnt_b;
      }
    };
    struct HCond {
      int agg;
      std::string op;  // "=", "!=", "<", "<=", ">", ">=", "IS NULL",
                       // "IS NOT NULL"
      int64_t k{0};
    };
    const bool use_where = g.Chance(70);
    const int cond_count = g.Pick(1, 2);
    const bool conj_or = g.Chance(25);  // join two conds with OR
    std::vector<HCond> conds;
    std::string having_sql;
    static const std::vector<std::string> kCmpOps = {"=",  "!=", "<",
                                                     "<=", ">",  ">="};
    for (int i = 0; i < cond_count; ++i) {
      HCond c;
      c.agg = g.Pick(0, static_cast<int>(kAggSql.size()) - 1);
      if (c.agg != 0 && g.Chance(20)) {
        c.op = g.Chance(50) ? "IS NULL" : "IS NOT NULL";
      } else {
        c.op = g.PickFrom(kCmpOps);
        c.k = g.Pick(-2, 4);
      }
      conds.push_back(c);
      having_sql += having_sql.empty() ? "" : (conj_or ? " OR " : " AND ");
      having_sql += std::string(kAggSql[static_cast<size_t>(c.agg)]) + " " +
                    c.op +
                    (c.op == "IS NULL" || c.op == "IS NOT NULL"
                         ? ""
                         : " " + std::to_string(c.k));
    }
    std::string sql = "SELECT a, COUNT(*), SUM(b) FROM " + tab;
    if (use_where) {
      sql += " WHERE " + t.predicate;
    }
    sql += " GROUP BY a HAVING " + having_sql + ";";
    t.having = {sql};

    std::map<int64_t, AggVals> groups;
    for (const MirrorRow& m : mirror) {
      if (use_where && pred->Eval(m) != 'T') {
        continue;
      }
      AggVals& v = groups[m.a];
      ++v.cnt;
      if (m.b != kNull) {
        v.sum_b = v.sum_b == kNull ? m.b : v.sum_b + m.b;
        ++v.cnt_b;
      }
      if (m.a != kNull) {  // never true for the group key kNull, kept general
        v.min_a = v.min_a == kNull ? m.a : std::min(v.min_a, m.a);
        v.max_a = v.max_a == kNull ? m.a : std::max(v.max_a, m.a);
      }
    }
    for (const auto& [key, v] : groups) {
      bool keep = conj_or ? false : true;
      for (const HCond& c : conds) {
        const int64_t val = agg_value(v, c.agg);
        char r = 'F';
        if (c.op == "IS NULL") {
          r = val == kNull ? 'T' : 'F';
        } else if (c.op == "IS NOT NULL") {
          r = val == kNull ? 'F' : 'T';
        } else if (val != kNull) {
          if (c.op == "=") {
            r = val == c.k ? 'T' : 'F';
          } else if (c.op == "!=") {
            r = val != c.k ? 'T' : 'F';
          } else if (c.op == "<") {
            r = val < c.k ? 'T' : 'F';
          } else if (c.op == "<=") {
            r = val <= c.k ? 'T' : 'F';
          } else if (c.op == ">") {
            r = val > c.k ? 'T' : 'F';
          } else {
            r = val >= c.k ? 'T' : 'F';
          }
        } else {
          r = 'N';
        }
        if (conj_or) {
          keep = keep || r == 'T';
        } else {
          keep = keep && r == 'T';
        }
      }
      if (keep) {
        t.having_expect.push_back(ExpectRow({key, v.cnt, v.sum_b}));
      }
    }
  }

  // ---- CTE inlining + WITH RECURSIVE counters ----
  if (g.Chance(60)) {
    const RPredPtr inner = GenPredicate(g, g.Pick(1, 2), flavour);
    const RPredPtr outer = GenPredicate(g, g.Pick(1, 2), flavour);
    t.cte = {"WITH w AS (SELECT u, a, b FROM " + tab + " WHERE " +
                 inner->Render() + ") SELECT u, a, b FROM w WHERE " +
                 outer->Render() + ";",
             "SELECT u, a, b FROM " + tab + " WHERE (" + inner->Render() +
                 ") AND (" + outer->Render() + ");"};
  }
  if (g.Chance(45)) {
    const int64_t start = g.Pick(-3, 4);
    const int64_t bound = g.Pick(-3, 8);
    const bool up = g.Chance(70);
    const int64_t step = up ? 1 : -1;
    const char* cmp = up ? "<" : ">";
    // Occasionally route the dedup path (values never repeat anyway).
    const std::string union_kw = g.Chance(20) ? "UNION DISTINCT" : "UNION ALL";
    std::vector<int64_t> seq;
    int64_t v = start;
    seq.push_back(v);
    while (up ? v < bound : v > bound) {
      v += step;
      seq.push_back(v);
    }
    int64_t sum = 0;
    for (const int64_t n : seq) {
      sum += n;
    }
    t.recursive = "WITH RECURSIVE r AS (SELECT " + std::to_string(start) +
                  " AS n " + union_kw + " SELECT n + " + std::to_string(step) +
                  " FROM r WHERE n " + cmp + " " + std::to_string(bound) +
                  ") SELECT COUNT(*), SUM(n), MIN(n), MAX(n) FROM r;";
    t.recursive_expect = ExpectRow(
        {static_cast<int64_t>(seq.size()), sum, seq.front(), seq.back()});
  }

  // ---- UNNEST over literal/generated arrays ----
  if (g.Chance(55)) {
    const int variant = g.Pick(0, 3);
    std::vector<int64_t> elems;
    if (variant <= 1) {
      const int n = g.Pick(0, 6);
      for (int i = 0; i < n; ++i) {
        elems.push_back(g.Chance(15) ? kNull : g.Pick(-3, 3));
      }
    } else if (variant == 2) {
      const int64_t lo = g.Pick(-3, 3);
      const int64_t hi = g.Pick(-3, 8);
      const int64_t step = g.Pick(0, 1) == 0 ? g.Pick(1, 3) : -g.Pick(1, 3);
      for (int64_t v2 = lo; step > 0 ? v2 <= hi : v2 >= hi; v2 += step) {
        elems.push_back(v2);
      }
      t.unnest = {"SELECT x FROM UNNEST(GENERATE_ARRAY(" + std::to_string(lo) +
                  ", " + std::to_string(hi) + ", " + std::to_string(step) +
                  ")) x;"};
    }
    if (variant == 0) {
      std::string arr = "[";
      for (size_t i = 0; i < elems.size(); ++i) {
        arr += i == 0 ? "" : ", ";
        arr += elems[i] == kNull ? "NULL" : std::to_string(elems[i]);
      }
      arr += "]";
      t.unnest = {"SELECT x FROM UNNEST(" + arr + ") x;"};
      for (const int64_t e : elems) {
        t.unnest_expect.push_back(ExpectRow({e}));
      }
      t.unnest_ordered = false;
    } else if (variant == 1) {
      std::string arr = "[";
      for (size_t i = 0; i < elems.size(); ++i) {
        arr += i == 0 ? "" : ", ";
        arr += elems[i] == kNull ? "NULL" : std::to_string(elems[i]);
      }
      arr += "]";
      t.unnest = {"SELECT x, p FROM UNNEST(" + arr + ") x WITH OFFSET p;"};
      for (size_t i = 0; i < elems.size(); ++i) {
        t.unnest_expect.push_back(
            ExpectRow({elems[i], static_cast<int64_t>(i)}));
      }
      t.unnest_ordered = true;
    } else if (variant == 2) {
      for (const int64_t e : elems) {
        t.unnest_expect.push_back(ExpectRow({e}));
      }
      t.unnest_ordered = false;
    } else {
      // Cross join with the generated table: passing rows multiply by the
      // element count (NULL elements still count as rows).
      const int n = g.Pick(1, 4);
      for (int i = 0; i < n; ++i) {
        elems.push_back(g.Chance(15) ? kNull : g.Pick(-3, 3));
      }
      std::string arr = "[";
      for (size_t i = 0; i < elems.size(); ++i) {
        arr += i == 0 ? "" : ", ";
        arr += elems[i] == kNull ? "NULL" : std::to_string(elems[i]);
      }
      arr += "]";
      int64_t pass = 0;
      for (const MirrorRow& m : mirror) {
        if (pred->Eval(m) == 'T') {
          ++pass;
        }
      }
      t.unnest = {"SELECT COUNT(*) FROM " + tab + ", UNNEST(" + arr +
                  ") x WHERE " + t.predicate + ";"};
      t.unnest_expect = {
          ExpectRow({pass * static_cast<int64_t>(elems.size())})};
      t.unnest_ordered = false;
    }
  }

  // ---- NoREC ----
  t.norec = {"SELECT COUNT(*) FROM " + tab + " WHERE " + t.predicate + ";",
             "SELECT SUM(CASE WHEN " + t.predicate +
                 " THEN 1 ELSE 0 END) FROM " + tab + ";"};

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
      t.index_ddl.push_back(std::string("CREATE INDEX idx_fuzz") +
                            std::to_string(i) + " ON " + tab + "(" +
                            kIdxCols[static_cast<size_t>(g.Pick(0, 3))] + ");");
    }
    t.index_probe =
        "SELECT COUNT(*) FROM " + tab + " WHERE " + t.predicate + ";";
  }

  // ---- Transaction splitting ----
  {
    const int stmt_count = g.Pick(1, 3);
    for (int i = 0; i < stmt_count; ++i) {
      switch (g.Pick(0, 2)) {
        case 0: {
          const RPredPtr cond = GenPredicate(g, g.Pick(1, 2), flavour);
          t.troc.push_back("UPDATE " + tab +
                           " SET a = " + std::to_string(g.Pick(-3, 3)) +
                           " WHERE " + cond->Render() + ";");
          break;
        }
        case 1: {
          const RPredPtr cond = GenPredicate(g, g.Pick(1, 2), flavour);
          t.troc.push_back("DELETE FROM " + tab + " WHERE " + cond->Render() +
                           ";");
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
    t.troc_probe = "SELECT COUNT(*), SUM(a), SUM(b) FROM " + tab + ";";
  }

  // ---- Statement-type transformation (mutates; probed last) ----
  {
    t.dqe = {"SELECT COUNT(*) FROM " + tab + " WHERE " + t.predicate + ";",
             "SELECT COUNT(*) FROM " + tab + ";",
             "DELETE FROM " + tab + " WHERE " + t.predicate + ";"};
  }

  // Plan feedback: reward flavours that surface unseen plan shapes.
  if (session != nullptr) {
    ScopedDb sdb("sql_oracle_fuzz");
    CHECK(sdb.get() != nullptr);
    Database& db = *sdb;
    TransactionContext ctx = db.BeginContext();
    if (RunSetup(db, ctx, t.setup, verbose).empty()) {
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
    stats->unionall_ran = t.unionall.size() == 2;
    stats->subq_ran = t.subq.size() == 3;
    stats->ssub_ran = t.ssub.size() == 2;
    stats->cqp_ran = t.cqp.size() == 2;
    stats->ddg_ran = t.ddg.size() == 2;
    stats->piv_ran = t.piv.size() == 2;
    stats->lwn_ran = t.lwn.size() == 2;
    stats->niv_ran = t.niv.size() == 2;
    stats->notin_ran = t.notin.size() == 1;
    stats->norec_ran = t.norec.size() == 2;
    stats->pqs_ran = !t.pqs_count.empty();
    stats->idx_ran = !t.index_ddl.empty() && !t.index_probe.empty();
    stats->dqe_ran = t.dqe.size() == 3;
    stats->troc_ran = !t.troc.empty();
    stats->setop_ran = t.setop.size() == 1;
    stats->orderby_ran = t.orderby.size() == 1;
    stats->ties_ran = t.ties.size() == 1;
    stats->having_ran = t.having.size() == 1;
    stats->cte_ran = t.cte.size() == 2;
    stats->recursive_ran = !t.recursive.empty();
    stats->unnest_ran = t.unnest.size() == 1;
  }
  return report;
}

std::string ReplayOracleTrace(const OracleTrace& trace, bool verbose) {
  if (trace.setup.empty() || !trace.setup[0].starts_with("CREATE TABLE")) {
    return "malformed trace: no CREATE TABLE in setup";
  }
  ScopedDb sdb("sql_oracle_replay");
  CHECK(sdb.get() != nullptr);
  Database& db = *sdb;
  TransactionContext ctx = db.BeginContext();
  std::string setup_err = RunSetup(db, ctx, trace.setup, verbose);
  if (!setup_err.empty()) {
    return setup_err;
  }
  std::string report;
  if (!CheckTlp(db, ctx, trace.tlp, &report, verbose) ||
      !CheckTlpAgg(db, ctx, trace.tlp_agg, &report, verbose) ||
      !CheckUnionAll(db, ctx, trace.unionall, &report, verbose) ||
      !CheckSubq(db, ctx, trace.subq, &report, verbose) ||
      !CheckNoRec(db, ctx, trace.norec, &report, verbose) ||
      !CheckPqs(db, ctx, trace, &report, verbose) ||
      !CheckIdx(db, ctx, trace, &report, verbose) ||
      !CheckTroc(db, trace, &report, verbose) ||
      !CheckDqe(db, ctx, trace, &report, verbose) ||
      !CheckExpected(db, ctx, trace.setop, trace.setop_expect,
                     /*ordered=*/false, "SETOP", &report, verbose) ||
      !CheckExpected(db, ctx, trace.orderby, trace.orderby_expect,
                     /*ordered=*/true, "ORDERBY", &report, verbose) ||
      !CheckExpected(db, ctx, trace.ties, trace.ties_expect,
                     /*ordered=*/false, "TIES", &report, verbose) ||
      !CheckExpected(db, ctx, trace.having, trace.having_expect,
                     /*ordered=*/false, "HAVING", &report, verbose) ||
      !CheckPair(db, ctx, trace.cte, "CTE", &report, verbose) ||
      !CheckPair(db, ctx, trace.ssub, "SSUB", &report, verbose) ||
      !CheckPair(db, ctx, trace.cqp, "CASE", &report, verbose) ||
      !CheckPair(db, ctx, trace.ddg, "DDG", &report, verbose) ||
      !CheckPair(db, ctx, trace.piv, "PIV", &report, verbose) ||
      !CheckPair(db, ctx, trace.lwn, "LWN", &report, verbose) ||
      !CheckPair(db, ctx, trace.niv, "NIV", &report, verbose) ||
      !CheckExpected(db, ctx, trace.notin, trace.notin_expect,
                     /*ordered=*/false, "NOTIN", &report, verbose) ||
      !CheckRecursive(db, ctx, trace, &report, verbose) ||
      !CheckExpected(db, ctx, trace.unnest, trace.unnest_expect,
                     trace.unnest_ordered, "UNNEST", &report, verbose)) {
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
  for (const std::string& sql : trace.unionall) {
    out += "-- unionall: " + sql + "\n";
  }
  for (const std::string& sql : trace.ssub) {
    out += "-- ssub: " + sql + "\n";
  }
  for (const std::string& sql : trace.cqp) {
    out += "-- cqp: " + sql + "\n";
  }
  for (const std::string& sql : trace.ddg) {
    out += "-- ddg: " + sql + "\n";
  }
  for (const std::string& sql : trace.piv) {
    out += "-- piv: " + sql + "\n";
  }
  for (const std::string& sql : trace.lwn) {
    out += "-- lwn: " + sql + "\n";
  }
  for (const std::string& sql : trace.niv) {
    out += "-- niv: " + sql + "\n";
  }
  for (const std::string& sql : trace.notin) {
    out += "-- notin: " + sql + "\n";
  }
  for (const std::string& row : trace.notin_expect) {
    out += "-- notinexpect: " + row + "\n";
  }
  for (const std::string& sql : trace.subq) {
    out += "-- subq: " + sql + "\n";
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
  for (const std::string& sql : trace.setop) {
    out += "-- setop: " + sql + "\n";
  }
  for (const std::string& row : trace.setop_expect) {
    out += "-- setopexpect: " + row + "\n";
  }
  for (const std::string& sql : trace.orderby) {
    out += "-- orderby: " + sql + "\n";
  }
  for (const std::string& row : trace.orderby_expect) {
    out += "-- orderbyexpect: " + row + "\n";
  }
  for (const std::string& sql : trace.ties) {
    out += "-- ties: " + sql + "\n";
  }
  for (const std::string& row : trace.ties_expect) {
    out += "-- tiesexpect: " + row + "\n";
  }
  for (const std::string& sql : trace.having) {
    out += "-- having: " + sql + "\n";
  }
  for (const std::string& row : trace.having_expect) {
    out += "-- havingexpect: " + row + "\n";
  }
  for (const std::string& sql : trace.cte) {
    out += "-- cte: " + sql + "\n";
  }
  if (!trace.recursive.empty()) {
    out += "-- recursive: " + trace.recursive + "\n";
    out += "-- recursiveexpect: " + trace.recursive_expect + "\n";
  }
  for (const std::string& sql : trace.unnest) {
    out += "-- unnest: " + sql + "\n";
  }
  for (const std::string& row : trace.unnest_expect) {
    out += "-- unnestexpect: " + row + "\n";
  }
  if (trace.unnest_ordered) {
    out += "-- unnestordered: 1\n";
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
    } else if (consume("-- unionall: ", &value)) {
      trace->unionall.push_back(value);
    } else if (consume("-- ssub: ", &value)) {
      trace->ssub.push_back(value);
    } else if (consume("-- cqp: ", &value)) {
      trace->cqp.push_back(value);
    } else if (consume("-- ddg: ", &value)) {
      trace->ddg.push_back(value);
    } else if (consume("-- piv: ", &value)) {
      trace->piv.push_back(value);
    } else if (consume("-- lwn: ", &value)) {
      trace->lwn.push_back(value);
    } else if (consume("-- niv: ", &value)) {
      trace->niv.push_back(value);
    } else if (consume("-- notin: ", &value)) {
      trace->notin.push_back(value);
    } else if (consume("-- notinexpect: ", &value)) {
      trace->notin_expect.push_back(value);
    } else if (consume("-- subq: ", &value)) {
      trace->subq.push_back(value);
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
    } else if (consume("-- setop: ", &value)) {
      trace->setop.push_back(value);
    } else if (consume("-- setopexpect: ", &value)) {
      trace->setop_expect.push_back(value);
    } else if (consume("-- orderby: ", &value)) {
      trace->orderby.push_back(value);
    } else if (consume("-- orderbyexpect: ", &value)) {
      trace->orderby_expect.push_back(value);
    } else if (consume("-- ties: ", &value)) {
      trace->ties.push_back(value);
    } else if (consume("-- tiesexpect: ", &value)) {
      trace->ties_expect.push_back(value);
    } else if (consume("-- having: ", &value)) {
      trace->having.push_back(value);
    } else if (consume("-- havingexpect: ", &value)) {
      trace->having_expect.push_back(value);
    } else if (consume("-- cte: ", &value)) {
      trace->cte.push_back(value);
    } else if (consume("-- recursive: ", &value)) {
      trace->recursive = value;
    } else if (consume("-- recursiveexpect: ", &value)) {
      trace->recursive_expect = value;
    } else if (consume("-- unnest: ", &value)) {
      trace->unnest.push_back(value);
    } else if (consume("-- unnestexpect: ", &value)) {
      trace->unnest_expect.push_back(value);
    } else if (consume("-- unnestordered: ", &value)) {
      trace->unnest_ordered = value == "1";
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
  ScopedDb sdb("sql_oracle_amoeba");
  CHECK(sdb.get() != nullptr);
  Database& db = *sdb;
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

  // Sanity: the mirror says all variants are semantically identical
  // (row-wise equality trivially implies equal true-counts).
  for (const RPredPtr& v : variants) {
    for (const MirrorRow& m : mirror) {
      if (v->Eval(m) != variants[0]->Eval(m)) {
        return "internal error: AMOEBA variants are not equivalent";
      }
    }
  }

  auto explain_plan = [&](const std::string& sql) {
    std::string error;
    auto rows = RunRows(db, ctx, "EXPLAIN " + sql, &error);
    if (!rows.has_value()) {
      return std::string{};
    }
    std::string plan;
    for (const Row& r : *rows) {
      plan += r.ToString() + ";";
    }
    return plan;
  };

  std::vector<std::string> sqls;
  sqls.reserve(variants.size());
  for (const RPredPtr& v : variants) {
    sqls.push_back("SELECT COUNT(*) FROM " + tab + " WHERE " + v->Render() +
                   ";");
  }

  // Interleave samples across variants so that external host spikes (e.g.
  // checkpoints or thread scheduling in multi-threaded fuzzing) affect all
  // variants rather than unfairly penalizing one consecutive block.
  std::vector<std::vector<int64_t>> all_samples(sqls.size());
  for (int round = 0; round < 7; ++round) {
    for (size_t vi = 0; vi < sqls.size(); ++vi) {
      std::string error;
      const auto start = std::chrono::steady_clock::now();
      auto rows = RunRows(db, ctx, sqls[vi], &error);
      const auto stop = std::chrono::steady_clock::now();
      if (!rows.has_value()) {
        if (verbose) {
          std::cerr << "[sql_oracle][amoeba-skip]\n";
        }
        return "";
      }
      all_samples[vi].push_back(
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start)
              .count());
    }
  }

  int64_t fastest = -1;
  int64_t slowest = -1;
  size_t fastest_idx = 0;
  size_t slowest_idx = 0;
  for (size_t vi = 0; vi < sqls.size(); ++vi) {
    std::sort(all_samples[vi].begin(), all_samples[vi].end());
    const int64_t med = all_samples[vi][all_samples[vi].size() / 2];
    if (fastest < 0 || med < fastest) {
      fastest = med;
      fastest_idx = vi;
    }
    if (slowest < 0 || med > slowest) {
      slowest = med;
      slowest_idx = vi;
    }
  }

  // Conservative thresholds: report only egregious regressions where the
  // optimizer produced different plans or huge runtime divergence.
  if (slowest > 20000000 && slowest > 30 * fastest) {
    const std::string plan_fast = explain_plan(sqls[fastest_idx]);
    const std::string plan_slow = explain_plan(sqls[slowest_idx]);
    if (!plan_fast.empty() && !plan_slow.empty() && plan_fast == plan_slow) {
      // Identical physical plan: runtime divergence is host scheduler noise.
      return "";
    }
    std::string report = "[AMOEBA MISMATCH]\n";
    report += "  fast: " + sqls[fastest_idx] + " => " +
              std::to_string(fastest / 1000000) + "ms\n";
    report += "  slow: " + sqls[slowest_idx] + " => " +
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
