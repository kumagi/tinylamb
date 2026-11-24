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
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/process.hpp"
#include "common/platform/random.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "executor/query_memory.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "query/sql_oracle_checks.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// ---------------------------------------------------------------------------
// Random predicate tree with C++-side 3-valued evaluation.  This mirror is
// the ground truth for the constraint oracles (PQS, AMOEBA) and keeps the
// engine-facing SQL and the harness expectation in lockstep.
// ---------------------------------------------------------------------------

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

// Qualifies a bare column (`a` → `l.a`) so a predicate can be replayed on
// one side of a join.  An empty qualifier leaves the name unchanged, which
// is what every single-table oracle renders.
std::string QualCol(std::string_view qual, std::string_view name) {
  if (qual.empty()) {
    return std::string(name);
  }
  std::string out;
  out.reserve(qual.size() + 1 + name.size());
  out.append(qual);
  out.push_back('.');
  out.append(name);
  return out;
}

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
  int str_pattern{0};    // Index into the five patterns rendered below.
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

  // `qual` prefixes every column (`l.a`).  Empty keeps the historical
  // single-table spelling, so existing oracles stay byte-identical.
  [[nodiscard]] std::string Render(std::string_view qual = {}) const {
    auto icol = [&](int column) {
      return QualCol(qual, column == 0 ? "a" : "b");
    };
    switch (kind) {
      case Kind::kIntCmp: {
        const std::string rhs =
            rhs_is_col ? icol(rhs_col) : std::to_string(rhs_const);
        return "(" + icol(col) + " " + op + " " + rhs + ")";
      }
      case Kind::kIntIsNull: {
        std::string out = "(" + icol(col) + " IS ";
        if (negated) {
          out += "NOT ";
        }
        out += "NULL)";
        return out;
      }
      case Kind::kBoolCol: {
        const std::string flag = QualCol(qual, "flag");
        return bool_positive ? flag : "(NOT " + flag + ")";
      }
      case Kind::kStrCmp:
        return "(" + QualCol(qual, "s") + " " + op + " '" +
               kStrPool[static_cast<size_t>(str_const)] + "')";
      case Kind::kStrIsNull: {
        std::string out = "(" + QualCol(qual, "s") + " IS ";
        if (negated) {
          out += "NOT ";
        }
        out += "NULL)";
        return out;
      }
      case Kind::kNot:
        return "(NOT " + left->Render(qual) + ")";
      case Kind::kIsNullWrap:
        return "(" + left->Render(qual) + ") IS NULL";
      case Kind::kBin:
        return "(" + left->Render(qual) + " " + op + " " + right->Render(qual) +
               ")";
      case Kind::kIsDistinctFrom: {
        const std::string rhs =
            rhs_is_null
                ? "NULL"
                : (rhs_is_col ? icol(rhs_col) : std::to_string(rhs_const));
        return "(" + icol(col) + " IS " + (negated ? "NOT " : "") +
               "DISTINCT FROM " + rhs + ")";
      }
      case Kind::kBetween: {
        const std::string lo =
            between_lo_null ? "NULL" : std::to_string(between_lo);
        const std::string hi =
            between_hi_null ? "NULL" : std::to_string(between_hi);
        return "(" + icol(col) + (negated ? " NOT BETWEEN " : " BETWEEN ") +
               lo + " AND " + hi + ")";
      }
      case Kind::kInList: {
        std::string out = "(" + icol(col) + (negated ? " NOT IN (" : " IN (");
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
        return "(" + QualCol(qual, "s") + " " +
               (negated ? "NOT LIKE " : "LIKE ") +
               kPatterns[static_cast<size_t>(str_pattern)] + ")";
      }
      case Kind::kBitwiseAndCmp:
        return "(((" + icol(col) + " & " + std::to_string(mask) + ") " + op +
               " " + std::to_string(rhs_const) + "))";
      case Kind::kBitwiseNotCmp:
        return "(((~" + icol(col) + ") " + op + " " +
               std::to_string(rhs_const) + "))";
      case Kind::kArithmeticCmp: {
        const std::string op_str = arith_const >= 0 ? "+" : "-";
        const int64_t abs_const = arith_const >= 0 ? arith_const : -arith_const;
        return "(((" + icol(col) + " " + op_str + " " +
               std::to_string(abs_const) + ") " + op + " " +
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
  int Pick(int lo, int hi) { return platform::UniformInt<int>(rng_, lo, hi); }
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
// 0 = default, 1 = shallower trees, 2 = AND-heavy trees (no NOT nodes).
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

// Generates 2..4 window probes (mix of model-checked and shape-only kinds)
// as "KIND|P1|P2|ARG|PART|FLAGS|DEF|SQL" specs.
std::vector<std::string> GenWindowProbes(Gen& g, const std::string& tab) {
  std::vector<std::string> probes;
  const int count = g.Pick(2, 4);
  for (int i = 0; i < count; ++i) {
    int kind = g.Pick(0, 14);
    const int part = g.Chance(50) ? 0 : -1;  // PARTITION BY a
    const int arg = g.Pick(0, 2);            // 0=b 1=a 2=u
    const std::string arg_name = arg == 0 ? "b" : (arg == 1 ? "a" : "u");
    const std::string part_txt = part == 0 ? "PARTITION BY a " : "";
    int64_t p1 = g.Pick(0, 2);
    int64_t p2 = g.Pick(0, 2);
    int64_t flags = 0;
    int64_t def = kNoDefault;
    std::string expr;
    switch (kind) {
      case 0:
        expr = "ROW_NUMBER() OVER (" + part_txt + "ORDER BY u)";
        break;
      case 1:
      case 2: {
        flags = g.Chance(35) ? 4 : 0;  // DESC
        expr = std::string(kind == 1 ? "RANK" : "DENSE_RANK") +
               "() OVER (ORDER BY " + arg_name + ((flags != 0) ? " DESC" : "") +
               ")";
        break;
      }
      case 3: {
        flags = g.Chance(50) ? 2 : 0;  // LEAD
        p1 = g.Pick(1, 2);             // offset
        if (g.Chance(50)) {
          def = g.Pick(-5, 5);
        }
        expr = std::string((flags != 0) ? "LEAD" : "LAG")
                   .append("(")
                   .append(arg_name)
                   .append(", ")
                   .append(std::to_string(p1))
                   .append(def == kNoDefault ? "" : ", " + std::to_string(def))
                   .append(") OVER (")
                   .append(part_txt)
                   .append("ORDER BY u)");
        break;
      }
      case 4:
      case 5:
      case 6:
      case 7: {
        static constexpr std::array<const char*, 4> kAggs = {"SUM", "COUNT",
                                                             "MIN", "MAX"};
        if (g.Chance(40)) {
          flags |= 1;  // EXCLUDE CURRENT ROW
        }
        expr = std::string(kAggs[static_cast<size_t>(kind - 4)])
                   .append("(")
                   .append(arg_name)
                   .append(") OVER (")
                   .append(part_txt)
                   .append("ORDER BY u ROWS BETWEEN ")
                   .append(std::to_string(p1))
                   .append(" PRECEDING AND ")
                   .append(std::to_string(p2))
                   .append(" FOLLOWING")
                   .append((flags & 1) != 0 ? " EXCLUDE CURRENT ROW" : "")
                   .append(")");
        kind = 10 + (kind - 4);
        break;
      }
      case 8: {
        static constexpr std::array<const char*, 4> kAggs = {"SUM", "COUNT",
                                                             "MIN", "MAX"};
        const int f = g.Pick(0, 3);
        kind = 20 + f;
        expr = std::string(kAggs[static_cast<size_t>(f)])
                   .append("(")
                   .append(arg_name)
                   .append(") OVER (")
                   .append(part_txt)
                   .append(
                       "ROWS BETWEEN UNBOUNDED PRECEDING AND "
                       "UNBOUNDED FOLLOWING)");
        break;
      }
      case 9:
        expr = std::string(g.Chance(50) ? "SUM" : "COUNT")
                   .append("(")
                   .append(arg_name)
                   .append(") OVER (ORDER BY ")
                   .append(arg_name)
                   .append(" RANGE BETWEEN ")
                   .append(std::to_string(p1))
                   .append(" PRECEDING AND ")
                   .append(std::to_string(p2))
                   .append(" FOLLOWING)");
        kind = 30;
        break;
      case 10:
        expr = "COUNT(*) OVER (ORDER BY a GROUPS BETWEEN " +
               std::to_string(p1) + " PRECEDING AND " + std::to_string(p2) +
               " FOLLOWING)";
        kind = 31;
        break;
      case 11:
        expr =
            "ARRAY_AGG(b ORDER BY u) OVER (ORDER BY u ROWS BETWEEN 1 "
            "PRECEDING AND CURRENT ROW)";
        kind = 40;
        break;
      case 12: {
        static constexpr std::array<const char*, 4> kAggs = {"SUM", "COUNT",
                                                             "MIN", "MAX"};
        const int f = g.Pick(0, 3);
        const bool lead = g.Chance(50);
        const int64_t a = g.Pick(1, 2);
        const int64_t b = g.Pick(0, 1);
        expr = std::string(kAggs[static_cast<size_t>(f)])
                   .append("(")
                   .append(arg_name)
                   .append(") OVER (")
                   .append(part_txt)
                   .append("ORDER BY u ROWS BETWEEN ")
                   .append(lead ? std::to_string(a) + " FOLLOWING"
                                : std::string("UNBOUNDED PRECEDING"))
                   .append(" AND ")
                   .append(lead ? std::string("UNBOUNDED FOLLOWING")
                                : std::to_string(b) + " PRECEDING")
                   .append(")");
        kind = 41;
        break;
      }
      case 13: {
        p1 = g.Pick(1, 4);
        expr = std::string("NTILE(")
                   .append(std::to_string(p1))
                   .append(") OVER (")
                   .append(part_txt)
                   .append("ORDER BY u)");
        kind = 50;
        break;
      }
      default: {
        if (g.Chance(50)) {
          kind = 60;
          p2 = g.Pick(-2, 2);
          expr = std::string("COUNTIF(b > ")
                     .append(std::to_string(p2))
                     .append(") OVER (")
                     .append(part_txt)
                     .append(")");
        } else {
          kind = 61;
          expr = "COUNT(DISTINCT " + arg_name;
          expr += ") OVER (";
          expr += part_txt;
          expr += ")";
        }
        break;
      }
    }
    std::string sql = "SELECT u, ";
    sql += expr;
    sql += " FROM ";
    sql += tab;
    sql += " ORDER BY u;";
    probes.push_back(std::to_string(kind) + "|" + std::to_string(p1) + "|" +
                     std::to_string(p2) + "|" + std::to_string(arg) + "|" +
                     std::to_string(part) + "|" + std::to_string(flags) + "|" +
                     std::to_string(def) + "|" + sql);
  }
  return probes;
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

// NoREC query constructors.  The optimized side keeps the predicate where
// the optimizer rewrites it (WHERE, JOIN ON, GROUP BY).  The reference side
// evaluates that predicate once per joined row and sums the TRUEs, which
// leaves index ranges, join selection and filter pushdown with nothing to
// attach to.  COALESCE turns the empty-input SUM NULL into 0 so it matches
// COUNT(*).
void PushNoRecPair(std::vector<std::string>* dst, std::string optimized,
                   std::string reference) {
  dst->push_back(std::move(optimized));
  dst->push_back(std::move(reference));
}

std::string NoRecCount(const std::string& from, const std::string& pred) {
  return "SELECT COUNT(*) FROM " + from + " WHERE (" + pred + ");";
}

std::string NoRecSum(const std::string& from, const std::string& pred) {
  return "SELECT COALESCE(SUM(CASE WHEN (" + pred +
         ") THEN 1 ELSE 0 END), 0) FROM " + from + ";";
}

void PushNoRecFilter(std::vector<std::string>* dst, const std::string& from,
                     const std::string& pred) {
  PushNoRecPair(dst, NoRecCount(from, pred), NoRecSum(from, pred));
}

// Paper §3.2–3.3 shapes, plus the rewrites this optimizer actually performs
// (range extraction, IN/BETWEEN canonicalization, join selection, filter
// pushdown, stream aggregation).  Every pair is an identity under SQL
// three-valued logic; a rejected spelling is skipped at check time.
void AppendNoRec(Gen& g, const std::string& tab, const std::string& stab,
                 const std::string& jtab, const RPred& pred, int row_count,
                 OracleTrace* t) {
  const std::string p = pred.Render();
  PushNoRecFilter(&t->norec, tab, p);
  // Paper's non-aggregate optimized form: the WHERE still sits on a scan,
  // but COUNT(*) is outside it, so a count-only shortcut cannot hide a
  // filter bug.
  PushNoRecPair(
      &t->norec,
      "SELECT COUNT(*) FROM (SELECT u FROM " + tab + " WHERE (" + p + ")) q;",
      NoRecSum(tab, p));
  // Filter above a derived scan: pushdown into the base table is optional.
  // Doing it wrong changes the count; not doing it still filters outside.
  PushNoRecFilter(&t->norec, "(SELECT u, a, b, flag, s FROM " + tab + ") d", p);
  {
    static const std::array<const char*, 4> kGroupKeys = {"a", "s", "flag",
                                                          "a, b"};
    const char* key = kGroupKeys[static_cast<size_t>(g.Pick(0, 3))];
    PushNoRecPair(
        &t->norec,
        "SELECT COALESCE(SUM(c), 0) FROM (SELECT COUNT(*) AS c FROM " + tab +
            " WHERE (" + p + ") GROUP BY " + key + ") g;",
        "SELECT COALESCE(SUM(c), 0) FROM (SELECT SUM(CASE WHEN (" + p +
            ") THEN 1 ELSE 0 END) AS c FROM " + tab + " GROUP BY " + key +
            ") g;");
  }
  // ORDER BY does not change cardinality.  Dropping it on the reference
  // side still has to match, including when the sort is eliminated.
  PushNoRecPair(&t->norec,
                "SELECT COUNT(*) FROM (SELECT u, a FROM " + tab + " WHERE (" +
                    p + ") ORDER BY a, u) q;",
                NoRecSum(tab, p));
  // The paper's canonical unoptimized predicate is `φ IS TRUE`.
  PushNoRecFilter(&t->norec, tab, "(" + p + ") IS TRUE");

  int lo = g.Pick(-3, 1);
  int hi = g.Pick(lo, 3);
  const std::string range =
      "(a >= " + std::to_string(lo) + ") AND (a <= " + std::to_string(hi) + ")";
  PushNoRecFilter(&t->norec, tab, range);
  const int cut = g.Pick(-1, 1);
  PushNoRecFilter(&t->norec, tab,
                  "(a < " + std::to_string(cut) + ") OR (a > " +
                      std::to_string(cut + 1) + ")");
  // Constant on the left forces a commute before a range can be extracted.
  {
    const int k = g.Pick(-2, 2);
    const char* left = nullptr;
    const char* right = nullptr;
    switch (g.Pick(0, 3)) {
      case 0:
        left = " < ";
        right = " > ";
        break;
      case 1:
        left = " <= ";
        right = " >= ";
        break;
      case 2:
        left = " = ";
        right = " = ";
        break;
      default:
        left = " != ";
        right = " != ";
        break;
    }
    PushNoRecPair(&t->norec, NoRecCount(tab, std::to_string(k) + left + "a"),
                  NoRecSum(tab, std::string("a") + right + std::to_string(k)));
  }
  {
    const int k1 = g.Pick(-2, 2);
    const int k2 = g.Pick(-2, 2);
    const int k3 = g.Pick(-2, 2);
    const std::string ks = std::to_string(k1) + ", " + std::to_string(k2) +
                           ", " + std::to_string(k3);
    // Non-null IN-list is the OR of equalities.  The WHERE side may rewrite
    // IN; the reference spells the OR, so only one side takes that path.
    PushNoRecPair(&t->norec, NoRecCount(tab, "a IN (" + ks + ")"),
                  NoRecSum(tab, "(a = " + std::to_string(k1) +
                                    ") OR (a = " + std::to_string(k2) +
                                    ") OR (a = " + std::to_string(k3) + ")"));
    PushNoRecFilter(
        &t->norec, tab,
        "b IN (" + std::to_string(k1) + ", NULL, " + std::to_string(k2) + ")");
  }
  PushNoRecPair(&t->norec,
                NoRecCount(tab, "a BETWEEN " + std::to_string(lo) + " AND " +
                                    std::to_string(hi)),
                NoRecSum(tab, "(a >= " + std::to_string(lo) +
                                  ") AND (a <= " + std::to_string(hi) + ")"));
  {
    static const std::array<const char*, 5> kPatterns = {"'a%'", "'%b'", "'%'",
                                                         "'_'", "'%z%'"};
    const char* pat = kPatterns[static_cast<size_t>(g.Pick(0, 4))];
    const char* op = g.Chance(40) ? "NOT LIKE " : "LIKE ";
    PushNoRecFilter(&t->norec, tab, std::string("s ") + op + pat);
  }
  PushNoRecFilter(&t->norec, tab, g.Chance(50) ? "a IS NULL" : "a IS NOT NULL");
  PushNoRecFilter(&t->norec, tab, g.Chance(50) ? "s IS NULL" : "s IS NOT NULL");
  PushNoRecFilter(&t->norec, tab,
                  g.Chance(50) ? "flag IS TRUE" : "flag IS NOT TRUE");
  {
    const int delta = g.Pick(1, 2);
    const int k = g.Pick(-2, 2);
    PushNoRecFilter(
        &t->norec, tab,
        "(a + " + std::to_string(delta) + ") > " + std::to_string(k));
  }
  {
    const int ka = g.Pick(-2, 2);
    const int kb = g.Pick(-2, 2);
    PushNoRecFilter(&t->norec, tab,
                    "(a = " + std::to_string(ka) +
                        ") AND (b >= " + std::to_string(kb) + ")");
  }
  PushNoRecFilter(&t->norec, tab,
                  "u = " + std::to_string(g.Pick(0, row_count - 1)));

  const std::string qualified = pred.Render("l");
  // Equijoin and inequality join.  The ON clause is copied; only the WHERE
  // moves.  NULL keys do not match on either side.
  PushNoRecFilter(&t->norec, tab + " AS l JOIN " + tab + " AS r ON l.a = r.a",
                  qualified);
  PushNoRecFilter(&t->norec, tab + " AS l JOIN " + tab + " AS r ON l.u < r.u",
                  qualified);
  // Filter after DISTINCT is an identity when the predicate mentions only
  // the distinct columns.  Pushing it below DISTINCT is the optimized path.
  PushNoRecFilter(&t->norec, "(SELECT DISTINCT a, b FROM " + tab + ") d",
                  "a > " + std::to_string(g.Pick(-2, 2)));

  if (!stab.empty()) {
    static const std::array<const char*, 3> kOuter = {"LEFT JOIN", "RIGHT JOIN",
                                                      "FULL OUTER JOIN"};
    const char* kind = kOuter[static_cast<size_t>(g.Pick(0, 2))];
    PushNoRecFilter(&t->norec,
                    tab + " AS l " + kind + " " + stab + " AS r ON l.a = r.a",
                    qualified);
    // Inner-join ON is a filter of the cross product.  Moving that predicate
    // into the indicator disables join selection on the reference side.
    const std::string phi = "(l.a = r.a) AND (" + qualified + ")";
    PushNoRecPair(&t->norec,
                  "SELECT COUNT(*) FROM " + tab + " AS l JOIN " + stab +
                      " AS r ON " + phi + ";",
                  NoRecSum(tab + " AS l CROSS JOIN " + stab + " AS r", phi));
  }
  if (!jtab.empty()) {
    // IN / EXISTS / NOT IN are unambiguous (no scalar-subquery cardinality
    // surprise).  Decorrelation applies to the WHERE spelling only.
    PushNoRecFilter(&t->norec, tab, "a IN (SELECT x FROM " + jtab + ")");
    PushNoRecFilter(&t->norec, tab,
                    "EXISTS (SELECT 1 FROM " + jtab + " WHERE " + jtab +
                        ".x = " + tab + ".a)");
    PushNoRecFilter(&t->norec, tab, "a NOT IN (SELECT x FROM " + jtab + ")");
    PushNoRecFilter(&t->norec, tab, "a = (SELECT MAX(x) FROM " + jtab + ")");
  }

  // §3.3: the rows themselves, not only the count.  The reference has no
  // WHERE; the checker keeps indicator 1.
  PushNoRecPair(&t->norec_content,
                "SELECT u, a, b, flag, s FROM " + tab + " WHERE (" + p + ");",
                "SELECT u, a, b, flag, s, CASE WHEN (" + p +
                    ") THEN 1 ELSE 0 END FROM " + tab + ";");
  PushNoRecPair(&t->norec_content,
                "SELECT u, a, b FROM " + tab + " WHERE " + range + ";",
                "SELECT u, a, b, CASE WHEN " + range +
                    " THEN 1 ELSE 0 END FROM " + tab + ";");
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
  if (platform::UniformInt<int>(rng_, 1, 10) <= 2) {
    return platform::UniformInt<int>(rng_, 0, flavour_count - 1);
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
      "t" + std::to_string(platform::UniformInt<int64_t>(rng, 0, INT32_MAX));
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

  {
    const std::string prefix =
        "SELECT " + GenProjection(g) + " FROM " + tab + " WHERE ";
    const std::string left = t.predicate;
    const std::string right = tlp_aux->Render();
    t.or_partition = {
        prefix + "(" + left + ") OR (" + right + ");",
        prefix + "(" + left + ") IS TRUE UNION ALL " + prefix + "(" + right +
            ") IS TRUE AND (" + left + ") IS NOT TRUE;",
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
      std::string insert = "INSERT INTO " + jtab;
      insert += " VALUES (";
      insert += x;
      insert += ", ";
      insert += y;
      insert += ");";
      t.setup.push_back(insert);
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
      std::string insert = "INSERT INTO ";
      insert += stab;
      insert += " VALUES ";
      insert += values;
      insert += ";";
      t.setup.push_back(insert);
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
    // PIVOT groups by every input column outside the aggregate and the FOR
    // column, so pin the input to (u, a): only then is the oracle the single
    // global row of conditional sums. (A bare table is rejected anyway.)
    t.piv = {"SELECT * FROM (SELECT u, a FROM " + tab +
                 ") PIVOT(SUM(a) FOR u IN (" + std::to_string(k0) + ", " +
                 std::to_string(k1) + "));",
             "SELECT " + c0 + ", " + c1 + " FROM " + tab + ";"};
  }

  // ---- Set-op precedence: INTERSECT binds tighter than UNION ALL ----
  if (!stab.empty() && g.Chance(30)) {
    const std::string lhs = "SELECT a FROM " + tab;
    const std::string mid = "SELECT a FROM " + stab;
    const std::string rhs =
        "SELECT a FROM " + tab + " WHERE " + pred2->Render();
    t.usp = {lhs + " UNION ALL " + mid + " INTERSECT DISTINCT " + rhs + ";",
             lhs + " UNION ALL (" + mid + " INTERSECT DISTINCT " + rhs + ");"};
  }

  // ---- QUALIFY vs derived-table WHERE ----
  // A window predicate in QUALIFY must equal the derived-table spelling
  // row-for-row, whether it references the output alias or repeats the
  // window expression inline.
  if (g.Chance(35)) {
    std::string win;
    switch (g.Pick(0, 6)) {
      case 0:
        win = "ROW_NUMBER() OVER (ORDER BY a, u)";
        break;
      case 1:
        win = "RANK() OVER (ORDER BY b)";
        break;
      case 2:
        win = "DENSE_RANK() OVER (ORDER BY a)";
        break;
      case 3:
        win = "COUNT(*) OVER (PARTITION BY b)";
        break;
      case 4:
        win = "LAG(a, 1) OVER (ORDER BY u)";
        break;
      case 5:
        win =
            "MIN(b) OVER (ORDER BY u ROWS BETWEEN 1 PRECEDING AND "
            "CURRENT ROW)";
        break;
      default:
        win = "SUM(a) OVER (PARTITION BY b)";
        break;
    }
    // The predicate is rendered around an operand so the same shape can
    // target the alias, the inline window expression, or the derived
    // column.
    const int psel = g.Pick(0, 7);
    const int k = g.Pick(-1, 4);
    const int k2 = g.Pick(k, k + 4);
    auto qpred = [&](const std::string& x) -> std::string {
      switch (psel) {
        case 0:
          return x + " <= " + std::to_string(k);
        case 1:
          return x + " > " + std::to_string(k);
        case 2:
          return x + " = " + std::to_string(k);
        case 3:
          return x + " BETWEEN " + std::to_string(k) + " AND " +
                 std::to_string(k2);
        case 4:
          return x + " IN (" + std::to_string(k) + ", " + std::to_string(k2) +
                 ")";
        case 5:
          return x + " IS NOT NULL";
        case 6:
          return x + " IS NULL";
        default:
          return "NOT (" + x + " > " + std::to_string(k) + ")";
      }
    };
    const std::string inner = "SELECT u, " + win + " AS r FROM " + tab;
    const std::string lhs =
        inner + " QUALIFY " + (g.Chance(50) ? qpred("r") : qpred(win));
    const std::string rhs =
        "SELECT u, r FROM (" + inner + ") q WHERE " + qpred("r");
    t.qualify = {lhs + ";", rhs + ";"};
  }

  // ---- Set-op derived table + outer WHERE ----
  // filter(A <op> B) = filter(A) <op> filter(B) is an identity for every
  // set-operation flavour, so the derived spelling and the per-branch
  // spelling must agree (Bug-36 class: injecting the predicate into the
  // head operand only left UNION tails unfiltered).
  const bool is_fuzz_run = (stats == nullptr && session == nullptr);
  if (!stab.empty() && g.Chance(35)) {
    static const std::array<const char*, 5> kSafeDsoOps = {
        "UNION ALL", "INTERSECT ALL", "INTERSECT DISTINCT", "EXCEPT ALL",
        "EXCEPT DISTINCT"};
    const std::string op = (is_fuzz_run && g.Chance(35))
                               ? "UNION DISTINCT"
                               : kSafeDsoOps[static_cast<size_t>(g.Pick(0, 4))];
    const int k = g.Pick(-3, 3);
    const int k2 = g.Pick(k, k + 4);
    std::string pred_a;
    switch (g.Pick(0, 8)) {
      case 0:
        pred_a = "a < " + std::to_string(k);
        break;
      case 1:
        pred_a = "a <= " + std::to_string(k);
        break;
      case 2:
        pred_a = "a > " + std::to_string(k);
        break;
      case 3:
        pred_a = "a >= " + std::to_string(k);
        break;
      case 4:
        pred_a = "a = " + std::to_string(k);
        break;
      case 5:
        pred_a = "a != " + std::to_string(k);
        break;
      case 6:
        pred_a =
            "a BETWEEN " + std::to_string(k) + " AND " + std::to_string(k2);
        break;
      case 7:
        pred_a = "a IN (" + std::to_string(k) + ", " + std::to_string(k2) + ")";
        break;
      default:
        pred_a = "NOT (a > " + std::to_string(k) + ")";
        break;
    }
    t.dso = {"SELECT a FROM (SELECT a FROM " + tab + " " + op +
                 " SELECT a FROM " + stab + ") d WHERE " + pred_a + ";",
             "SELECT a FROM " + tab + " WHERE " + pred_a + " " + op +
                 " SELECT a FROM " + stab + " WHERE " + pred_a + ";"};
  }

  // ---- NOT IN literal list vs NOT(OR of equals) ----
  // Under three-valued logic both spellings must agree row-for-row:
  // NULL a makes each comparison UNKNOWN and both sides filter out.
  if (g.Chance(30)) {
    const int n = g.Pick(1, 3);
    std::string list;
    list.reserve(n);
    std::string ors;
    ors.reserve(n);
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
      out.reserve(rows.size());
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
    rows.reserve(mirror.size());
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
    const auto total = static_cast<int64_t>(rows.size());
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
      const int64_t n = g.Pick(0, static_cast<int>(total + 1));
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

  {
    const int ordinal_column = g.Pick(0, 1);
    const bool descending = g.Chance(50);
    const int explicit_nulls = g.Pick(0, 2);
    const bool nulls_first =
        explicit_nulls == 0 ? !descending : explicit_nulls == 1;
    const std::string nulls_clause =
        explicit_nulls == 0 ? ""
                            : (nulls_first ? " NULLS FIRST" : " NULLS LAST");
    const std::string primary = std::to_string(ordinal_column + 1) +
                                (descending ? " DESC" : "") + nulls_clause;
    const std::string tie = ordinal_column == 0 ? ", a" : ", u";
    const std::string inner =
        "SELECT u, a FROM " + tab + " WHERE " + t.predicate;
    const int shape = g.Pick(0, 2);
    std::string sql;
    if (shape == 0) {
      sql = "SELECT * FROM (" + inner + ") q ORDER BY " + primary + tie;
    } else if (shape == 1) {
      sql =
          "WITH q AS (" + inner + ") SELECT * FROM q ORDER BY " + primary + tie;
    } else {
      sql = "SELECT * FROM (" + inner + " UNION ALL " + inner +
            ") q ORDER BY " + primary + tie;
    }
    const bool doubled = shape == 2;
    std::vector<const MirrorRow*> rows;
    rows.reserve(mirror.size() * 2);
    for (const MirrorRow& row : mirror) {
      if (pred->Eval(row) != 'T') {
        continue;
      }
      rows.push_back(&row);
      if (doubled) {
        rows.push_back(&row);
      }
    }
    std::stable_sort(
        rows.begin(), rows.end(),
        [&](const MirrorRow* left, const MirrorRow* right) {
          const int64_t lv = ordinal_column == 0 ? left->u : left->a;
          const int64_t rv = ordinal_column == 0 ? right->u : right->a;
          const bool lnull = lv == kNull;
          const bool rnull = rv == kNull;
          if (lnull != rnull) {
            return nulls_first ? lnull : rnull;
          }
          if (!lnull && lv != rv) {
            return descending ? lv > rv : lv < rv;
          }
          return left->u < right->u;
        });
    const auto total = static_cast<int64_t>(rows.size());
    int64_t limit = -1;
    int64_t offset = 0;
    if (g.Chance(60)) {
      limit = g.Pick(0, static_cast<int>(total) + 2);
      sql += " LIMIT " + std::to_string(limit);
      if (g.Chance(40)) {
        offset = g.Pick(0, static_cast<int>(total) + 1);
        sql += " OFFSET " + std::to_string(offset);
      }
    }
    t.ordinal_order = {sql + ";"};
    const int64_t end = std::min(total, offset + (limit < 0 ? total : limit));
    for (int64_t i = offset; i < end; ++i) {
      t.ordinal_order_expect.push_back(ExpectRow(
          {rows[static_cast<size_t>(i)]->u, rows[static_cast<size_t>(i)]->a}));
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
      int agg{};
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
      bool keep = !conj_or;
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
    t.cte = {"WITH w AS (SELECT u, a, b, flag, s FROM " + tab + " WHERE " +
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
    t.recursive_expect = ExpectRow({static_cast<int64_t>(seq.size()), sum,
                                    std::min(seq.front(), seq.back()),
                                    std::max(seq.front(), seq.back())});
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
      arr.reserve(elems.size());
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
      arr.reserve(elems.size());
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
      arr.reserve(elems.size());
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

  // ---- NoREC (paper translation + index/join/aggregate shapes) ----
  AppendNoRec(g, tab, stab, jtab, *pred, row_count, &t);

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
    // Specs stay textual for .test self-containment; ApplyIndexSpec routes
    // them through Database::CreateIndex because the SQL frontend has no
    // CREATE INDEX statement.
    static const std::array<const char*, 4> kIdxCols = {"a", "b", "flag", "s"};
    const int idx_count = g.Pick(1, 4);
    for (int i = 0; i < idx_count; ++i) {
      std::string ddl;
      if (g.Pick(0, 7) == 0) {
        // u is the unique row id: a real unique index exercises the
        // IndexMode::kUnique plan paths.
        ddl = "CREATE UNIQUE INDEX idx_fuzz" + std::to_string(i) + " ON " +
              tab + "(u);";
      } else {
        ddl = "CREATE INDEX idx_fuzz" + std::to_string(i) + " ON " + tab + "(" +
              kIdxCols[static_cast<size_t>(g.Pick(0, 3))];
        if (g.Pick(0, 3) == 0) {
          ddl += "," + std::string(kIdxCols[static_cast<size_t>(g.Pick(0, 3))]);
        }
        ddl += ")";
        if (g.Pick(0, 3) == 0) {
          // Covering index: probes that touch only key+include columns can
          // take the index-only scan path.
          ddl += " INCLUDE(" +
                 std::string(kIdxCols[static_cast<size_t>(g.Pick(0, 3))]) + ")";
        }
        ddl += ";";
      }
      t.index_ddl.push_back(std::move(ddl));
    }
    t.index_probe =
        "SELECT COUNT(*) FROM " + tab + " WHERE " + t.predicate + ";";
    // DISTINCT probes exercise the skip-scan-distinct executor; the no-index
    // plan and the indexed plan must agree exactly.
    static const std::array<const char*, 2> kSkipCols = {"a", "b"};
    const std::string skip_col = kSkipCols[static_cast<size_t>(g.Pick(0, 1))];
    t.skip_probes.push_back("SELECT COUNT(DISTINCT " + skip_col + ") FROM " +
                            tab + ";");
    t.skip_probes.push_back("SELECT DISTINCT " + skip_col + " FROM " + tab +
                            " ORDER BY " + skip_col + ";");
    t.skip_probes.push_back("SELECT COUNT(DISTINCT " + skip_col + ") FROM " +
                            tab + " WHERE " + t.predicate + ";");
    // Sibling indexes exist only so the indexed NoREC phase can pick a join
    // or semi-join access path.  They do not change the primary probe.
    if (!stab.empty()) {
      t.index_ddl.push_back("CREATE INDEX idx_fuzz_sa ON " + stab + "(a);");
      t.index_ddl.push_back("CREATE INDEX idx_fuzz_sb ON " + stab + "(b);");
      t.index_ddl.push_back("CREATE INDEX idx_fuzz_ss ON " + stab + "(s);");
    }
    if (!jtab.empty()) {
      t.index_ddl.push_back("CREATE INDEX idx_fuzz_jx ON " + jtab + "(x);");
    }
  }

  // ---- Window functions + TOP-n WITH TIES (model / permutation / index) ----
  {
    t.window_probes = GenWindowProbes(g, tab);
    t.topties_limit = g.Pick(1, 3);
    t.topties = "SELECT u, a FROM " + tab + " ORDER BY a LIMIT " +
                std::to_string(t.topties_limit) + " WITH TIES;";
  }

  // ---- Transaction splitting ----
  {
    const int stmt_count = g.Pick(1, 3);
    for (int i = 0; i < stmt_count; ++i) {
      switch (g.Pick(0, 2)) {
        case 0: {
          const RPredPtr cond = GenPredicate(g, g.Pick(1, 2), flavour);
          // Mix constant SETs with column/arithmetic RHS — the expression
          // engine runs inside the DML write path in both branches.
          std::string set_expr;
          switch (g.Pick(0, 2)) {
            case 0:
              set_expr = std::to_string(g.Pick(-3, 3));
              break;
            case 1:
              set_expr = "(a + " + std::to_string(g.Pick(-3, 3)) + ")";
              break;
            default:
              set_expr = "b";
              break;
          }
          std::string update = "UPDATE ";
          update.append(tab)
              .append(" SET a = ")
              .append(set_expr)
              .append(" WHERE ")
              .append(cond->Render())
              .append(";");
          t.troc.push_back(std::move(update));
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

  // ---- Cross-mode / cross-engine composite SQL differential probes ----
  {
    std::mt19937 xrng(rng());
    Gen xg(xrng);
    std::vector<std::string> candidates;
    if (!jtab.empty()) {
      static const std::array<const char*, 3> kJoinKws = {"JOIN", "LEFT JOIN",
                                                          "FULL OUTER JOIN"};
      const std::string jkw = kJoinKws[static_cast<size_t>(xg.Pick(0, 2))];
      static const std::array<const char*, 3> kGrpSpecs = {
          "1, 2", "ROLLUP(1, 2)", "GROUPING SETS ((1, 2), (1), ())"};
      const std::string gspec = kGrpSpecs[static_cast<size_t>(xg.Pick(0, 2))];
      candidates.push_back(
          "SELECT " + tab + ".a, COALESCE(" + jtab +
          ".y, 'none') AS jy, COUNT(*), SUM(CASE WHEN (" + t.predicate +
          ") THEN " + tab + ".b ELSE 0 END), MIN(" + tab + ".u), MAX(NULLIF(" +
          tab + ".b, -1)) FROM " + tab + " " + jkw + " " + jtab + " ON " + tab +
          ".a = " + jtab + ".x GROUP BY " + gspec +
          " HAVING COUNT(*) >= " + std::to_string(xg.Pick(0, 2)) + ";");
    } else {
      candidates.push_back(
          "SELECT a, flag, COUNT(*), SUM(CASE WHEN (" + t.predicate +
          ") THEN b ELSE 0 END), MIN(u), MAX(NULLIF(b, -1)) FROM " + tab +
          " GROUP BY ROLLUP(a, flag);");
    }

    // Sibling derived aggregate fusion candidate (FuseSiblingDerivedAggregates)
    {
      const std::string jkw = xg.Chance(50) ? "JOIN" : "LEFT JOIN";
      const std::string non_null_guard =
          (!is_fuzz_run || xg.Chance(60)) ? " AND a IS NOT NULL" : "";
      candidates.push_back(
          "SELECT d1.a, d1.c1, d1.s1, d2.c2, d2.m2 FROM "
          "(SELECT a, COUNT(*) AS c1, SUM(b) AS s1 FROM " +
          tab + " WHERE (" + t.predicate + ")" + non_null_guard +
          " GROUP BY a) d1 " + jkw +
          " (SELECT a, COUNT(b) AS c2, MAX(u) AS m2 FROM " + tab +
          " WHERE b IS NOT NULL OR flag IS TRUE GROUP BY a) d2 "
          "ON d1.a = d2.a;");
    }

    // Shared CTE with site-specific predicates (PushSharedCteRestrictions)
    {
      const int k1 = xg.Pick(-2, 1);
      const int k2 = xg.Pick(0, 3);
      candidates.push_back(
          "WITH c AS (SELECT u, a, b, flag, s FROM " + tab +
          ") SELECT c1.u, c2.u AS u2, c1.a, c2.b FROM c c1 JOIN c c2 "
          "ON c1.a = c2.a WHERE c1.b >= " +
          std::to_string(k1) + " AND c2.a <= " + std::to_string(k2) + ";");
    }

    // Window frame + QUALIFY
    {
      const int qmax = xg.Pick(1, 4);
      candidates.push_back(
          "SELECT u, a, b, SUM(b) OVER (PARTITION BY a ORDER BY u ROWS "
          "BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS ws, ROW_NUMBER() OVER "
          "(PARTITION BY flag ORDER BY a NULLS FIRST, u) AS rn FROM " +
          tab + " WHERE (" + t.predicate +
          ") OR a IS NOT NULL QUALIFY rn <= " + std::to_string(qmax) + ";");
    }

    // Correlated EXISTS + scalar subquery or Band Join
    if (!stab.empty()) {
      if (xg.Chance(50)) {
        candidates.push_back(
            "SELECT " + tab + ".u, " + tab + ".a, (SELECT MAX(s2.b) FROM " +
            stab + " s2 WHERE s2.a = " + tab + ".a) AS max_sb FROM " + tab +
            " WHERE EXISTS (SELECT 1 FROM " + stab + " s3 WHERE s3.a = " + tab +
            ".a AND (s3.b >= " + tab + ".b OR s3.flag IS TRUE));");
      } else {
        candidates.push_back("SELECT " + tab + ".u, " + stab + ".u AS su, " +
                             tab + ".a, " + stab + ".b FROM " + tab + " JOIN " +
                             stab + " ON " + tab + ".a = " + stab + ".a AND " +
                             stab + ".b BETWEEN " + tab + ".b - 1 AND " + tab +
                             ".b + 1 WHERE (" + t.predicate + ");");
      }
    }

    // Multi-index covering scan & compound range filter
    {
      const int lo_a = xg.Pick(-3, 0);
      const int hi_a = xg.Pick(lo_a, 3);
      const int lo_b = xg.Pick(-3, 0);
      const int hi_b = xg.Pick(lo_b, 3);
      candidates.push_back("SELECT u, a, b FROM " + tab + " WHERE a BETWEEN " +
                           std::to_string(lo_a) + " AND " +
                           std::to_string(hi_a) + " AND b BETWEEN " +
                           std::to_string(lo_b) + " AND " +
                           std::to_string(hi_b) + ";");
    }

    // Compound correlated EXISTS + NOT EXISTS + IN subquery conjuncts/disjuncts
    // (stresses subquery_runtime.cpp RuntimeId cache keys & decorrelation).
    if (!stab.empty()) {
      candidates.push_back(
          "SELECT " + tab + ".u, " + tab + ".a, " + tab + ".b FROM " + tab +
          " WHERE (NOT EXISTS (SELECT 1 FROM " + stab +
          " s1 WHERE s1.a = " + tab + ".a AND s1.b < " + tab +
          ".b) AND NOT EXISTS (SELECT 1 FROM " + stab +
          " s2 WHERE s2.b = " + tab + ".b AND s2.flag IS TRUE)) OR " + tab +
          ".a IN (SELECT s3.a FROM " + stab + " s3 WHERE s3.a IS NOT NULL);");
    }

    // Q1-style 6-term multi-aggregate over (a, flag) with COALESCE/NULLIF/CASE
    {
      candidates.push_back(
          "SELECT a, flag, COUNT(*), COUNT(a), SUM(a), SUM(b), "
          "MIN(COALESCE(a, 0) + COALESCE(b, 0)), "
          "MAX(CASE WHEN flag IS TRUE THEN a ELSE b END) FROM " +
          tab + " WHERE (" + t.predicate + ") GROUP BY a, flag;");
    }

    // Nested WITH CTE referencing an earlier CTE (stresses
    // CountNestedCteReferences & PushSharedCteRestrictions).
    {
      const int k1 = xg.Pick(-2, 1);
      const int k2 = xg.Pick(0, 3);
      candidates.push_back(
          "WITH c1 AS (SELECT u, a, b, flag, s FROM " + tab +
          "), c2 AS (SELECT u, a, b FROM c1 WHERE a IS NOT NULL OR flag IS "
          "TRUE) SELECT x.u, y.u AS yu, x.a, y.b FROM c1 x LEFT JOIN c2 y ON "
          "x.a = y.a WHERE x.b >= " +
          std::to_string(k1) + " OR y.b <= " + std::to_string(k2) + ";");
    }

    // Fused grouped-join + HAVING over MIN/MAX/SUM/COUNT (stresses
    // TryFusedGroupedJoin under disk spill).
    if (!jtab.empty() || !stab.empty()) {
      const std::string other = !jtab.empty() ? jtab : stab;
      const std::string okey = !jtab.empty() ? "x" : "a";
      const int k1 = xg.Pick(-2, 1);
      const int k2 = xg.Pick(1, 5);
      candidates.push_back(
          "SELECT " + tab + ".a, COUNT(*) AS cnt, MIN(" + tab +
          ".b) AS min_b, MAX(" + tab + ".u) AS max_u, SUM(" + tab +
          ".b) AS sum_b FROM " + tab + " JOIN " + other + " ON " + tab +
          ".a = " + other + "." + okey + " GROUP BY " + tab + ".a HAVING MIN(" +
          tab + ".b) >= " + std::to_string(k1) + " OR MAX(" + tab +
          ".u) <= " + std::to_string(k2) + " OR COUNT(*) >= 2;");
    }

    // Aggregate FILTER (WHERE ...) in SELECT + HAVING + ORDER BY (stresses
    // AggregateExpression::ToString filter discrimination during rebinding).
    {
      candidates.push_back(
          "SELECT a, COUNT(*) FILTER (WHERE flag IS TRUE) AS cf_t, "
          "COUNT(*) FILTER (WHERE flag IS NOT TRUE) AS cf_f, "
          "SUM(b) FILTER (WHERE a >= 0) AS sb_pos, "
          "SUM(b) FILTER (WHERE a < 0) AS sb_neg FROM " +
          tab +
          " GROUP BY a HAVING COUNT(*) FILTER (WHERE flag IS TRUE) >= 0 "
          "ORDER BY COUNT(*) FILTER (WHERE flag IS TRUE) DESC, a NULLS FIRST;");
    }

    // Multi-OVER Window with RANGE/GROUPS frames, EXCLUDE, VARCHAR/BOOL
    // ordering, and QUALIFY.
    {
      const int qmax = xg.Pick(1, 4);
      candidates.push_back(
          "SELECT u, a, s, SUM(b) OVER (PARTITION BY flag ORDER BY a RANGE "
          "BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS "
          "rng_sum, COUNT(s) OVER (PARTITION BY s ORDER BY a GROUPS BETWEEN 1 "
          "PRECEDING AND 1 FOLLOWING) AS grp_cnt, DENSE_RANK() OVER "
          "(PARTITION BY flag ORDER BY s NULLS LAST, a) AS dr FROM " +
          tab + " QUALIFY dr <= " + std::to_string(qmax) + ";");
    }

    // IS NOT DISTINCT FROM NULL-safe join + residual filter.
    if (!stab.empty()) {
      candidates.push_back("SELECT " + tab + ".u, " + stab + ".u AS su, " +
                           tab + ".a, " + stab + ".b, " + tab + ".s FROM " +
                           tab + " LEFT JOIN " + stab + " ON " + tab +
                           ".a IS NOT DISTINCT FROM " + stab + ".a AND " + tab +
                           ".s IS NOT DISTINCT FROM " + stab + ".s WHERE (" +
                           t.predicate + ") OR " + stab + ".u IS NULL;");
    }

    // Correlated LATERAL subquery with aggregation.
    if (!stab.empty()) {
      candidates.push_back(
          "SELECT " + tab + ".u, " + tab + ".a, lat.cnt, lat.max_b FROM " +
          tab + ", LATERAL (SELECT COUNT(*) AS cnt, MAX(s2.b) AS max_b FROM " +
          stab + " s2 WHERE s2.a = " + tab + ".a OR (s2.a IS NULL AND " + tab +
          ".a IS NULL)) lat WHERE (" + t.predicate + ");");
    }

    // 3-way chained set operations (EXCEPT ALL + UNION ALL + INTERSECT ALL).
    if (!stab.empty()) {
      candidates.push_back(
          "(SELECT a, b FROM " + tab + " WHERE " + t.predicate +
          " EXCEPT ALL SELECT a, b FROM " + stab +
          " WHERE b IS NOT NULL) UNION ALL (SELECT a, b FROM " + tab +
          " INTERSECT ALL SELECT a, b FROM " + stab + ");");
    }

    // STRUCT + TO_JSON_STRING + DATE_ADD + GENERATE_SERIES + STRPOS/LPAD.
    {
      candidates.push_back(
          "SELECT u, TO_JSON_STRING(STRUCT(a AS f_a, flag AS f_flag, "
          "COALESCE(s, 'null') AS f_s, DATE_ADD(DATE '2020-01-01', INTERVAL "
          "COALESCE(a, 0) DAY) AS f_d)) AS js, "
          "ARRAY_LENGTH(GENERATE_SERIES(1, LEAST(GREATEST(COALESCE(a, 0) + 2, "
          "1), 5))) AS alen, STRPOS(LPAD(COALESCE(s, ''), 5, 'z'), 'a') AS sp "
          "FROM " +
          tab + " WHERE (" + t.predicate + ");");
    }

    // Non-top-level subqueries inside CASE WHEN and OR disjuncts.
    if (!jtab.empty()) {
      candidates.push_back(
          "SELECT u, a, CASE WHEN a IN (SELECT x FROM " + jtab +
          " WHERE x IS NOT NULL) THEN 'in' WHEN EXISTS (SELECT 1 FROM " + jtab +
          " WHERE " + jtab + ".y = " + tab +
          ".s) THEN 'ex' ELSE 'none' END AS tag FROM " + tab + " WHERE (" +
          t.predicate + ") OR EXISTS (SELECT 1 FROM " + jtab + " WHERE " +
          jtab + ".x = " + tab + ".b);");
    }

    // GROUP BY CUBE(a, flag, s) with GROUPING() and BIT_XOR.
    {
      candidates.push_back(
          "SELECT a, flag, s, GROUPING(a) AS ga, GROUPING(flag) AS gf, "
          "GROUPING(s) AS gs, COUNT(*), SUM(b), BIT_XOR(a) FROM " +
          tab + " WHERE (" + t.predicate +
          ") OR flag IS NOT NULL GROUP BY CUBE(a, flag, s);");
    }

    // Filtered NOT IN over NULL-containing table (stresses
    // PrebuiltMembershipPredicate / column_has_null on LSTable, High #6).
    if (!jtab.empty()) {
      const int k_cut = xg.Pick(-2, 1);
      candidates.push_back(
          "SELECT u, a, b FROM " + tab + " WHERE a NOT IN (SELECT x FROM " +
          jtab + " WHERE x IS NOT NULL AND x >= " + std::to_string(k_cut) +
          ") AND (" + t.predicate + ");");
    }

    // Comma-join prefix + filtered derived subquery + RIGHT/FULL OUTER JOIN
    // (stresses FlattenDerivedSources outer-join boundary, High #19).
    if (!stab.empty() || !jtab.empty()) {
      const char* oj_kw =
          xg.Chance(50) ? "RIGHT OUTER JOIN" : "FULL OUTER JOIN";
      const int b_cut = xg.Pick(-1, 2);
      if (!stab.empty()) {
        candidates.push_back(
            "SELECT r.u, d.a, d.b FROM (SELECT 1 AS one) p, (SELECT u, a, b "
            "FROM " +
            tab + " WHERE b > " + std::to_string(b_cut) + ") d " + oj_kw + " " +
            stab + " r ON d.a = r.a;");
      } else {
        candidates.push_back(
            "SELECT r.x, d.a, d.b FROM (SELECT 1 AS one) p, (SELECT u, a, b "
            "FROM " +
            tab + " WHERE b > " + std::to_string(b_cut) + ") d " + oj_kw + " " +
            jtab + " r ON d.a = r.x;");
      }
    }

    // Named WINDOW w AS (PARTITION BY a) + preceding scalar subquery + inline
    // OVER (w ORDER BY u ...) extension (High #17, Medium #86).
    {
      candidates.push_back(
          "SELECT (SELECT 1) AS c1, u, a, b, SUM(b) OVER (w ORDER BY u ROWS "
          "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_b, "
          "COUNT(*) OVER w AS part_cnt FROM " +
          tab + " WINDOW w AS (PARTITION BY a);");
    }

    // Sample composite probes per iteration to keep per-seed latency low
    // while covering all composite query families uniformly across seeds.
    std::shuffle(candidates.begin(), candidates.end(), xrng);
    const size_t keep =
        std::min<size_t>(is_fuzz_run ? 4 : 3, candidates.size());
    t.xmode_probes.assign(candidates.begin(),
                          candidates.begin() + static_cast<ptrdiff_t>(keep));
  }

  // Plan feedback: reward flavours that surface unseen plan shapes.
  if (session != nullptr) {
    ScopedDb sdb("sql_oracle_fuzz");
    if (sdb.get() != nullptr) {  // resource pressure: skip feedback only
      Database& db = *sdb;
      TransactionContext ctx = db.BeginContext();
      if (RunSetup(db, ctx, t.setup, verbose).empty()) {
        std::string error;
        auto plan = RunRows(db, ctx, "EXPLAIN " + t.tlp[0], &error);
        if (plan.has_value()) {
          std::string fingerprint;
          fingerprint.reserve(plan->size());
          for (const Row& r : *plan) {
            fingerprint += r.ToString() + ";";
          }
          session->RewardFlavour(flavour, session->ObservePlan(fingerprint));
        }
      }
    }
  }

  bool skipped = false;
  // Generation probes legitimately skip when a statement cannot execute;
  // only the replay guards surface those skips as reports.
  std::string report =
      ReplayOracleTrace(t, verbose, &skipped, /*surface_skips=*/false);
  if (skipped) {
    // No database: nothing ran, so no family may be reported as "ran".
    if (stats != nullptr) {
      stats->skipped = true;
    }
    return {};
  }
  if (stats != nullptr) {
    stats->tlp_ran = t.tlp.size() == 4;
    stats->tlp_agg_ran = t.tlp_agg.size() == 4;
    stats->unionall_ran = t.unionall.size() == 2;
    stats->or_partition_ran = t.or_partition.size() == 2;
    stats->subq_ran = t.subq.size() == 3;
    stats->ssub_ran = t.ssub.size() == 2;
    stats->cqp_ran = t.cqp.size() == 2;
    stats->ddg_ran = t.ddg.size() == 2;
    stats->piv_ran = t.piv.size() == 2;
    stats->lwn_ran = t.lwn.size() == 2;
    stats->niv_ran = t.niv.size() == 2;
    stats->usp_ran = t.usp.size() == 2;
    stats->qualify_ran = t.qualify.size() == 2;
    stats->dso_ran = t.dso.size() == 2;
    stats->notin_ran = t.notin.size() == 1;
    stats->norec_ran = t.norec.size() >= 2 && t.norec.size() % 2 == 0;
    stats->pqs_ran = !t.pqs_count.empty();
    stats->idx_ran = !t.index_ddl.empty() && !t.index_probe.empty();
    stats->skip_ran = !t.skip_probes.empty();
    stats->window_ran = !t.window_probes.empty();
    stats->topties_ran = !t.topties.empty();
    stats->dqe_ran = t.dqe.size() == 3;
    stats->troc_ran = !t.troc.empty();
    stats->setop_ran = t.setop.size() == 1;
    stats->orderby_ran = t.orderby.size() == 1;
    stats->ordinal_order_ran = t.ordinal_order.size() == 1;
    stats->ties_ran = t.ties.size() == 1;
    stats->having_ran = t.having.size() == 1;
    stats->cte_ran = t.cte.size() == 2;
    stats->recursive_ran = !t.recursive.empty();
    stats->unnest_ran = t.unnest.size() == 1;
    stats->xmode_ran = !t.xmode_probes.empty();
  }
  // NOLINTNEXTLINE(performance-no-automatic-move) move not beneficial for this
  // type
  return report;
}

std::string ReplayOracleTrace(const OracleTrace& trace, bool verbose,
                              bool* skipped, bool surface_skips) {
  if (trace.setup.empty() || !trace.setup[0].starts_with("CREATE TABLE")) {
    return "malformed trace: no CREATE TABLE in setup";
  }
  ScopedDb sdb("sql_oracle_replay");
  if (sdb.get() == nullptr) {
    // Resource pressure: no oracle ran, so this is not a mismatch. The
    // generation path consults `skipped` before looking at the report, but
    // regression guards call this with `skipped == nullptr` (and expect a
    // non-empty report only when something actually reproduced); return a
    // diagnostic there so a replay that never opened a database cannot be
    // mistaken for "all oracles hold".
    if (skipped != nullptr) {
      *skipped = true;
    }
    return "replay skipped: could not create database (resource pressure)";
  }
  Database& db = *sdb;
  TransactionContext ctx = db.BeginContext();
  std::string setup_err = RunSetup(db, ctx, trace.setup, verbose);
  if (!setup_err.empty()) {
    return setup_err;
  }
  // Commit setup so all read-only oracle probes can exercise committed MVCC
  // versions and lazy LSTableStore creation (`GetOrBuildLSTableStore`).
  if (ctx.txn_.PreCommit() != Status::kSuccess) {
    return "oracle replay setup commit failed";
  }
  ctx = db.BeginContext();
  std::string report;
  // A Check* returning false means "recorded arm could not be executed:
  // oracle skipped". Discarding that bool let a truncated or tampered
  // committed file pass the permanent regression guard vacuously (report
  // stays empty while nothing actually ran), so every skip is collected
  // and surfaced alongside any mismatch.
  std::string skipped_arms;
  const auto run_arm = [&](bool ran, const char* name) {
    if (!ran) {
      if (!skipped_arms.empty()) {
        skipped_arms += ", ";
      }
      skipped_arms += name;
    }
  };
  run_arm(CheckTlp(db, ctx, trace.tlp, &report, verbose), "TLP");
  run_arm(CheckTlpAgg(db, ctx, trace.tlp_agg, &report, verbose), "TLP_AGG");
  run_arm(CheckUnionAll(db, ctx, trace.unionall, &report, verbose),
          "UNION_ALL");
  run_arm(CheckUnionAll(db, ctx, trace.or_partition, &report, verbose),
          "OR_PARTITION");
  run_arm(CheckSubq(db, ctx, trace.subq, &report, verbose), "SUBQ");
  // Base phase: no secondary index, so a mismatch here is a logical rewrite
  // (the access path is a scan on both sides, but WHERE is still rewritten).
  run_arm(CheckNoRec(db, ctx, trace, "base", &report, verbose), "NOREC_BASE");
  run_arm(CheckPqs(db, ctx, trace, &report, verbose), "PQS");
  run_arm(CheckWindow(db, ctx, trace, &report, verbose), "WINDOW");
  run_arm(CheckSkip(db, ctx, trace, &report, verbose), "SKIP");
  run_arm(CheckTopTies(db, ctx, trace, &report, verbose), "TOP_TIES");
  run_arm(CheckTroc(db, trace, &report, verbose), "TROC");
  run_arm(CheckExpected(db, ctx, trace.setop, trace.setop_expect,
                        /*ordered=*/false, "SETOP", &report, verbose),
          "SETOP");
  run_arm(CheckExpected(db, ctx, trace.orderby, trace.orderby_expect,
                        /*ordered=*/true, "ORDERBY", &report, verbose),
          "ORDERBY");
  run_arm(
      CheckExpected(db, ctx, trace.ordinal_order, trace.ordinal_order_expect,
                    /*ordered=*/true, "ORDINAL_ORDER", &report, verbose),
      "ORDINAL_ORDER");
  run_arm(CheckExpected(db, ctx, trace.ties, trace.ties_expect,
                        /*ordered=*/false, "TIES", &report, verbose),
          "TIES");
  run_arm(CheckExpected(db, ctx, trace.having, trace.having_expect,
                        /*ordered=*/false, "HAVING", &report, verbose),
          "HAVING");
  run_arm(CheckPair(db, ctx, trace.cte, "CTE", &report, verbose), "CTE");
  run_arm(CheckPair(db, ctx, trace.ssub, "SSUB", &report, verbose), "SSUB");
  run_arm(CheckPair(db, ctx, trace.cqp, "CASE", &report, verbose), "CASE");
  run_arm(CheckPair(db, ctx, trace.ddg, "DDG", &report, verbose), "DDG");
  run_arm(CheckPair(db, ctx, trace.piv, "PIV", &report, verbose), "PIV");
  run_arm(CheckPair(db, ctx, trace.lwn, "LWN", &report, verbose), "LWN");
  run_arm(CheckPair(db, ctx, trace.niv, "NIV", &report, verbose), "NIV");
  run_arm(CheckPair(db, ctx, trace.usp, "USP", &report, verbose), "USP");
  run_arm(CheckPair(db, ctx, trace.qualify, "QFY", &report, verbose), "QFY");
  run_arm(CheckPair(db, ctx, trace.dso, "DSO", &report, verbose), "DSO");
  run_arm(CheckExpected(db, ctx, trace.notin, trace.notin_expect,
                        /*ordered=*/false, "NOTIN", &report, verbose),
          "NOTIN");
  run_arm(CheckRecursive(db, ctx, trace, &report, verbose), "RECURSIVE");
  run_arm(CheckExpected(db, ctx, trace.unnest, trace.unnest_expect,
                        trace.unnest_ordered, "UNNEST", &report, verbose),
          "UNNEST");
  run_arm(CheckXMode(db, ctx, trace, &report, verbose), "XMODE");
  run_arm(CheckIdx(db, ctx, trace, &report, verbose), "IDX");
  // Indexed phase: CheckIdx has created the trace's indexes.  Refresh
  // statistics on every table so join and range choices see them.  The
  // reference queries have no WHERE, so they still cannot take a range.
  AnalyzeCreatedTables(db, ctx, trace.setup);
  run_arm(CheckNoRec(db, ctx, trace, "indexed", &report, verbose),
          "NOREC_INDEXED");
  run_arm(CheckDqe(db, ctx, trace, &report, verbose), "DQE");
  if (surface_skips && !skipped_arms.empty()) {
    report = "replay skipped recorded oracle arm(s): " + skipped_arms +
             (report.empty() ? "" : "\n" + report);
  }
  if (!report.empty() && !trace.predicate.empty()) {
    report = "predicate: " + trace.predicate + "\n" + report;
  }
  return report;
}

constexpr const char* kTestHeader = "-- tinylamb-oracle-test v1";

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
  for (const std::string& sql : trace.or_partition) {
    out += "-- orpartition: " + sql + "\n";
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
  for (const std::string& sql : trace.usp) {
    out += "-- usp: " + sql + "\n";
  }
  for (const std::string& sql : trace.qualify) {
    out += "-- qualify: " + sql + "\n";
  }
  for (const std::string& sql : trace.dso) {
    out += "-- dso: " + sql + "\n";
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
  for (const std::string& sql : trace.norec_content) {
    out += "-- norecrow: " + sql + "\n";
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
  for (const std::string& probe : trace.skip_probes) {
    out += "-- skip: " + probe + "\n";
  }
  for (const std::string& spec : trace.window_probes) {
    out += "-- win: " + spec + "\n";
  }
  if (!trace.topties.empty()) {
    out += "-- topties: " + trace.topties + "\n";
    out += "-- toptieslimit: " + std::to_string(trace.topties_limit) + "\n";
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
  for (const std::string& sql : trace.ordinal_order) {
    out += "-- ordinalorder: " + sql + "\n";
  }
  for (const std::string& row : trace.ordinal_order_expect) {
    out += "-- ordinalorderexpect: " + row + "\n";
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
  for (const std::string& sql : trace.xmode_probes) {
    out += "-- xmode: " + sql + "\n";
  }
  if (!failure_summary.empty()) {
    out += "-- failure: ";
    for (const char ch : failure_summary) {
      out += (ch == '\n' || ch == '\r') ? ' ' : ch;
    }
    out += "\n";
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
      size_t used = 0;
      try {
        *seed = std::stoull(value, &used);
      } catch (const std::exception&) {
        return false;
      }
      if (used != value.size()) {
        return false;
      }
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
    } else if (consume("-- orpartition: ", &value)) {
      trace->or_partition.push_back(value);
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
    } else if (consume("-- usp: ", &value)) {
      trace->usp.push_back(value);
    } else if (consume("-- qualify: ", &value)) {
      trace->qualify.push_back(value);
    } else if (consume("-- dso: ", &value)) {
      trace->dso.push_back(value);
    } else if (consume("-- notin: ", &value)) {
      trace->notin.push_back(value);
    } else if (consume("-- notinexpect: ", &value)) {
      trace->notin_expect.push_back(value);
    } else if (consume("-- subq: ", &value)) {
      trace->subq.push_back(value);
    } else if (consume("-- norec: ", &value)) {
      trace->norec.push_back(value);
    } else if (consume("-- norecrow: ", &value)) {
      trace->norec_content.push_back(value);
    } else if (consume("-- pqs: ", &value)) {
      trace->pqs_count = value;
    } else if (consume("-- pqsu: ", &value)) {
      trace->pqs_u = value;
    } else if (consume("-- pqsexpected: ", &value)) {
      try {
        trace->pqs_expected = std::stoll(value);
      } catch (const std::exception&) {
        trace->pqs_expected = 0;
      }
    } else if (consume("-- pqspivot: ", &value)) {
      try {
        trace->pqs_pivot_u = std::stoll(value);
      } catch (const std::exception&) {
        trace->pqs_pivot_u = 0;
      }
    } else if (consume("-- idxddl: ", &value)) {
      trace->index_ddl.push_back(value);
    } else if (consume("-- idxprobe: ", &value)) {
      trace->index_probe = value;
    } else if (consume("-- skip: ", &value)) {
      trace->skip_probes.push_back(value);
    } else if (consume("-- win: ", &value)) {
      trace->window_probes.push_back(value);
    } else if (consume("-- topties: ", &value)) {
      trace->topties = value;
    } else if (consume("-- toptieslimit: ", &value)) {
      try {
        trace->topties_limit = std::stoll(value);
      } catch (const std::exception&) {
        trace->topties_limit = 0;
      }
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
    } else if (consume("-- ordinalorder: ", &value)) {
      trace->ordinal_order.push_back(value);
    } else if (consume("-- ordinalorderexpect: ", &value)) {
      trace->ordinal_order_expect.push_back(value);
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
    } else if (consume("-- xmode: ", &value)) {
      trace->xmode_probes.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else {
      return false;  // unknown line: refuse to replay a half-understood file
    }
  }
  return saw_header && !trace->setup.empty();
}

// ---------------------------------------------------------------------------
// AMOEBA: equivalent predicates are executed for coverage; runtime
// divergence is deliberately NOT reported (host scheduling makes it flaky).
// ---------------------------------------------------------------------------

std::string RunAmoebaIteration(std::mt19937& rng, bool verbose) {
  Gen g(rng);
  ScopedDb sdb("sql_oracle_amoeba");
  if (sdb.get() == nullptr) {
    return "";  // resource pressure: iteration skipped
  }
  Database& db = *sdb;
  TransactionContext ctx = db.BeginContext();
  const std::string tab =
      "t" + std::to_string(platform::UniformInt<int64_t>(rng, 0, INT32_MAX));
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

  std::vector<std::string> sqls;
  sqls.reserve(variants.size());
  for (const RPredPtr& v : variants) {
    sqls.push_back("SELECT COUNT(*) FROM " + tab + " WHERE " + v->Render() +
                   ";");
  }

  // Execute each equivalent variant and compare their result rows: the
  // mirror proved the predicates equivalent, so divergent COUNT(*) values are
  // a real optimizer/executor bug. Do NOT turn wall-clock timing into a
  // finding, though: scheduler noise and concurrent ctest load make a
  // time-based MISMATCH flaky, and a flaky guard is worse than none.
  std::vector<std::string> baseline;
  for (size_t i = 0; i < sqls.size(); ++i) {
    const std::string& sql = sqls[i];
    std::string error;
    auto rows = RunRows(db, ctx, sql, &error);
    if (!rows.has_value()) {
      if (verbose) {
        std::cerr << "[sql_oracle][amoeba-skip]\n";
      }
      return "";
    }
    std::vector<std::string> got;
    got.reserve(rows->size());
    for (const Row& r : *rows) {
      got.push_back(r.ToString());
    }
    if (i == 0) {
      baseline = std::move(got);
      continue;
    }
    if (got != baseline) {
      std::string report =
          "[AMOEBA MISMATCH] equivalent predicates returned different rows\n";
      report += "  baseline: " + sqls[0] + "\n";
      for (const std::string& r : baseline) {
        report += "    " + r + "\n";
      }
      report += "  variant:  " + sql + "\n";
      for (const std::string& r : got) {
        report += "    " + r + "\n";
      }
      return report;
    }
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
  const std::string text = SerializeOracleTest(
      seed, trace,
      FormatFuzzFailureSummary("auto-generated by sql_oracle_fuzzer", report));
  uint64_t parsed_seed = 0;
  OracleTrace parsed;
  std::string summary;
  std::string verify;
  if (!ParseOracleTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized test does not parse";
  } else {
    bool skipped = false;
    verify = ReplayOracleTrace(parsed, false, &skipped);
    if (skipped) {
      // Verification never ran (e.g. quota pressure prevented a fresh
      // ScopedDb). An empty report there is NOT a clean replay, and labelling
      // it "flaky" would silently discard a real finding — keep the finding
      // and say exactly why the replay was inconclusive.
      verify =
          "internal error: replay skipped (no database; resource pressure)";
    } else if (verify.empty()) {
      verify = MakeFlakyVerify(
          "internal error: serialized test replay holds (flaky mismatch)");
    }
  }

  const std::string path =
      "sql_oracle_fuzz-repro-" + std::to_string(seed) + ".test";
  EmitFuzzFinding("sql_oracle_fuzz", seed, path, text, report, verify);
}

}  // namespace tinylamb
