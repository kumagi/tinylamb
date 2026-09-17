/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_session_fuzzer.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "database/database.hpp"
#include "index/index_schema.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

constexpr const char* kTestHeader = "-- tinylamb-session-test v1";
constexpr int64_t kStrA = 0;  // 'a'
constexpr int64_t kStrB = 1;  // 'bb'

// ---------------------------------------------------------------------------
// Schema-driven predicate with mirror evaluation.
// ---------------------------------------------------------------------------

enum class ColType { kInt, kStr };

struct MirrorColumn {
  std::string name;
  ColType type{ColType::kInt};
};

struct MirrorTable {
  std::string name;
  std::vector<MirrorColumn> columns;  // slot 0 is always "u" (INT64)
  std::vector<Row> rows;              // ordered by ascending u
  int64_t next_u{0};

  [[nodiscard]] static size_t Slot(size_t col_index) {
    return col_index;
  }  // slot == index
};

class Gen {
 public:
  explicit Gen(std::mt19937& rng) : rng_(rng) {}
  int Pick(int lo, int hi) {
    return std::uniform_int_distribution<int>(lo, hi)(rng_);
  }
  bool Chance(int percent) { return Pick(1, 100) <= percent; }
  int PickCol(const MirrorTable& t) {
    return Pick(0, static_cast<int>(t.columns.size()) - 1);
  }

 private:
  std::mt19937& rng_;
};

// Column 0 is the unique "u" column; the rest are c1, c2, ...
std::string ColName(int slot) {
  return slot == 0 ? "u" : "c" + std::to_string(slot);
}

// 3-valued result: 'T', 'F', 'N'.
struct SPred {
  enum class Kind {
    kIntCmp,      // int col op (const | other int col)
    kIntIsNull,   // int col IS [NOT] NULL
    kStrCmp,      // str col = / != const
    kStrIsNull,   // str col IS [NOT] NULL
    kInSubquery,  // int col [NOT] IN (SELECT int_col FROM tbl)
    kNot,
    kBin,
  };
  Kind kind{Kind::kIntCmp};
  bool negated{false};
  std::string op{"="};
  int col{0};
  bool rhs_is_col{false};
  int rhs_col{0};
  int64_t const_val{0};  // INT value, or kStrA/kStrB for strings
  std::unique_ptr<SPred> left, right;
  // kInSubquery: snapshot of the subquery column's values taken at generation
  // time (pre-statement state, which is what the engine's decorrelated
  // semi/anti join sees too).
  std::vector<Value> sub_vals;
  std::string sub_render;  // e.g. "tbl1.c2" parts pre-rendered below
  // kInSubquery: qualified LHS ("tbl0.u").  An unqualified name that also
  // exists in the subquery's table makes the engine treat the predicate as
  // correlated and reject it (next-actions D-8); qualifying keeps it
  // decorrelatable.
  std::string lhs_qual;

  static char And(char l, char r) {
    if (l == 'F' || r == 'F') {
      return 'F';
    }
    if (l == 'N' || r == 'N') {
      return 'N';
    }
    return 'T';
  }
  static char Or(char l, char r) {
    if (l == 'T' || r == 'T') {
      return 'T';
    }
    if (l == 'N' || r == 'N') {
      return 'N';
    }
    return 'F';
  }
  static char Not(char v) { return v == 'T' ? 'F' : (v == 'F' ? 'T' : 'N'); }

  [[nodiscard]] char Eval(const Row& row) const {
    auto cmp = [](const Value& l, const std::string& o, const Value& r) {
      if (l.IsNull() || r.IsNull()) {
        return 'N';
      }
      if (o == "=") {
        return (l == r) ? 'T' : 'F';
      }
      if (o == "!=") {
        return (l != r) ? 'T' : 'F';
      }
      if (o == "<") {
        return (l < r) ? 'T' : 'F';
      }
      if (o == "<=") {
        return (l <= r) ? 'T' : 'F';
      }
      if (o == ">") {
        return (l > r) ? 'T' : 'F';
      }
      return (l >= r) ? 'T' : 'F';
    };
    switch (kind) {
      case Kind::kIntCmp:
        return cmp(
            row[static_cast<size_t>(col)], op,
            rhs_is_col ? row[static_cast<size_t>(rhs_col)] : Value(const_val));
      case Kind::kIntIsNull: {
        const bool is_null = row[static_cast<size_t>(col)].IsNull();
        return (negated ? !is_null : is_null) ? 'T' : 'F';
      }
      case Kind::kStrCmp: {
        const Value rhs = const_val == kStrA ? Value(std::string("a"))
                                             : Value(std::string("bb"));
        return cmp(row[static_cast<size_t>(col)], op, rhs);
      }
      case Kind::kStrIsNull: {
        const bool is_null = row[static_cast<size_t>(col)].IsNull();
        return (negated ? !is_null : is_null) ? 'T' : 'F';
      }
      case Kind::kInSubquery: {
        // x IN (S): NULL x or a NULL member with no match yields NULL;
        // negated form is the same under NOT (N stays N).  An empty S is
        // unconditional: x IN ∅ is FALSE and x NOT IN ∅ is TRUE, even for
        // NULL x.
        if (sub_vals.empty()) {
          return negated ? 'T' : 'F';
        }
        const Value& v = row[static_cast<size_t>(col)];
        bool saw_null = v.IsNull();
        bool matched = false;
        for (const Value& s : sub_vals) {
          if (s.IsNull()) {
            saw_null = true;
            continue;
          }
          if (!v.IsNull() && v == s) {
            matched = true;
          }
        }
        const char res = matched ? 'T' : (saw_null ? 'N' : 'F');
        return negated ? Not(res) : res;
      }
      case Kind::kNot:
        return Not(left->Eval(row));
      case Kind::kBin:
        return op == "AND" ? And(left->Eval(row), right->Eval(row))
                           : Or(left->Eval(row), right->Eval(row));
    }
    return 'N';
  }

  [[nodiscard]] std::string Render() const {
    auto sql_const = [&]() { return const_val == kStrA ? "'a'" : "'bb'"; };
    switch (kind) {
      case Kind::kIntCmp: {
        const std::string rhs =
            rhs_is_col ? ColName(rhs_col) : std::to_string(const_val);
        return "(" + ColName(col) + " " + op + " " + rhs + ")";
      }
      case Kind::kIntIsNull:
      case Kind::kStrIsNull:
        return "(" + ColName(col) + " IS " + (negated ? "NOT " : "") + "NULL)";
      case Kind::kStrCmp:
        return "(" + ColName(col) + " " + op + " " + sql_const() + ")";
      case Kind::kInSubquery:
        return "(" + lhs_qual + (negated ? " NOT" : "") + " IN " + sub_render +
               ")";
      case Kind::kNot:
        return "(NOT " + left->Render() + ")";
      case Kind::kBin:
        return "(" + left->Render() + " " + op + " " + right->Render() + ")";
    }
    return "TRUE";
  }
};

using SPredPtr = std::unique_ptr<SPred>;

// Empty table list for predicate contexts that may never reference other
// relations (SET-expression CASE conditions).
const std::vector<MirrorTable> kNoTables;

// Where an IN-subquery may appear: kNone disables it; kConjunct allows it
// only at top-level WHERE conjuncts (under AND/NOT) where the QueryData path
// decorrelates it into a semi/anti join — under OR or inside a SET
// expression the engine keeps a per-row QueryExp which fails by design.
// kAnywhere additionally permits OR-nested positions, which only the
// relational SELECT path (e.g. INSERT ... SELECT) evaluates correctly.
enum class SubqMode { kNone, kConjunct, kAnywhere };

SPredPtr GenPred(Gen& g, const MirrorTable& t,
                 const std::vector<MirrorTable>& all, int depth,
                 SubqMode subq) {
  if (depth <= 0 || g.Chance(40)) {
    const int col = g.PickCol(t);
    auto p = std::make_unique<SPred>();
    if (subq != SubqMode::kNone &&
        t.columns[static_cast<size_t>(col)].type == ColType::kInt &&
        g.Chance(18)) {
      // col [NOT] IN (SELECT int_col FROM tblN) — exercises the semi/anti
      // decorrelation path inside UPDATE/DELETE WHERE clauses.  The
      // subquery's relation must differ from the target: a self-referencing
      // subquery (t IN (SELECT ... FROM t)) cannot be decorrelated because
      // inner/outer scopes collide, so the engine rejects it by design.
      std::vector<const MirrorTable*> candidates;
      for (const MirrorTable& other : all) {
        if (other.name != t.name) {
          candidates.push_back(&other);
        }
      }
      if (subq == SubqMode::kAnywhere) {
        candidates.push_back(&t);  // relational path handles self-reference
      }
      if (!candidates.empty()) {
        const MirrorTable& sub_t = *candidates[static_cast<size_t>(
            g.Pick(0, static_cast<int>(candidates.size()) - 1))];
        std::vector<int> int_cols;
        for (size_t i = 0; i < sub_t.columns.size(); ++i) {
          if (sub_t.columns[i].type == ColType::kInt) {
            int_cols.push_back(static_cast<int>(i));
          }
        }
        if (!int_cols.empty()) {
          const int sub_col = int_cols[static_cast<size_t>(
              g.Pick(0, static_cast<int>(int_cols.size()) - 1))];
          p->kind = SPred::Kind::kInSubquery;
          p->col = col;
          p->lhs_qual = t.name + "." + ColName(col);
          p->negated = g.Chance(35);
          p->sub_vals.reserve(sub_t.rows.size());
          for (const Row& r : sub_t.rows) {
            p->sub_vals.push_back(r[static_cast<size_t>(sub_col)]);
          }
          p->sub_render =
              "(SELECT " + ColName(sub_col) + " FROM " + sub_t.name + ")";
          return p;
        }
      }
    }
    if (t.columns[static_cast<size_t>(col)].type == ColType::kInt) {
      switch (g.Pick(0, 2)) {
        case 0: {
          static constexpr std::array<const char*, 6> kOps = {"=",  "!=", "<",
                                                              "<=", ">",  ">="};
          p->kind = SPred::Kind::kIntCmp;
          p->col = col;
          p->op = kOps[static_cast<size_t>(g.Pick(0, 5))];
          p->const_val = g.Pick(-3, 3);
          return p;
        }
        case 1: {
          p->kind = SPred::Kind::kIntCmp;
          p->col = col;
          static constexpr std::array<const char*, 4> kOps = {"=", "!=", "<",
                                                              ">="};
          p->op = kOps[static_cast<size_t>(g.Pick(0, 3))];
          int other = g.PickCol(t);
          while (static_cast<size_t>(other) >= t.columns.size() ||
                 t.columns[static_cast<size_t>(other)].type != ColType::kInt) {
            other = g.PickCol(t);
          }
          p->rhs_is_col = true;
          p->rhs_col = other;
          return p;
        }
        default:
          p->kind = SPred::Kind::kIntIsNull;
          p->col = col;
          p->negated = g.Chance(50);
          return p;
      }
    }
    if (g.Chance(60)) {
      p->kind = SPred::Kind::kStrCmp;
      p->col = col;
      p->op = g.Chance(60) ? "=" : "!=";
      p->const_val = g.Pick(0, 1);
    } else {
      p->kind = SPred::Kind::kStrIsNull;
      p->col = col;
      p->negated = g.Chance(50);
    }
    return p;
  }
  auto p = std::make_unique<SPred>();
  if (g.Pick(0, 2) == 2) {
    p->kind = SPred::Kind::kNot;
    // NOT(...) is not a top-level conjunct: the decorrelator cannot see
    // through it, so only the relational path (kAnywhere) may carry a
    // subquery below a NOT.
    const SubqMode not_mode =
        subq == SubqMode::kAnywhere ? subq : SubqMode::kNone;
    p->left = GenPred(g, t, all, depth - 1, not_mode);
    return p;
  }
  p->kind = SPred::Kind::kBin;
  const bool is_and = g.Chance(55);
  p->op = is_and ? "AND" : "OR";
  // AND children stay top-level conjuncts (SplitConjuncts flattens them);
  // OR children do not, so kConjunct mode must disable subqueries there.
  const SubqMode child_mode =
      (subq == SubqMode::kConjunct && !is_and) ? SubqMode::kNone : subq;
  p->left = GenPred(g, t, all, depth - 1, child_mode);
  p->right = GenPred(g, t, all, depth - 1, child_mode);
  return p;
}

// ---------------------------------------------------------------------------
// SET-expression generator for UPDATE.  Renders SQL and mirrors the value
// against the PRE-update row (standard SQL: every SET expression sees the
// old row).  Exercises the expression engine in the DML write path.
// ---------------------------------------------------------------------------

struct SExpr {
  enum class Kind { kConst, kCol, kArith, kCoalesce, kConcat, kCase };
  Kind kind{Kind::kConst};
  bool is_str{false};
  bool const_null{false};
  int64_t const_val{0};  // INT literal, or kStrA/kStrB for strings
  int col{-1};
  std::string op;
  std::unique_ptr<SExpr> lhs, rhs;
  SPredPtr cond;                          // kCase only
  std::unique_ptr<SExpr> then_e, else_e;  // kCase only

  [[nodiscard]] Value Eval(const Row& row) const {
    switch (kind) {
      case Kind::kConst:
        if (const_null) {
          return Value();
        }
        return is_str ? Value(std::string(const_val == kStrA ? "a" : "bb"))
                      : Value(const_val);
      case Kind::kCol:
        return row[static_cast<size_t>(col)];
      case Kind::kArith: {
        const Value l = lhs->Eval(row);
        const Value r = rhs->Eval(row);
        if (l.IsNull() || r.IsNull()) {
          return Value();
        }
        const int64_t lv = l.value.int_value;
        const int64_t rv = r.value.int_value;
        if (op == "+") {
          return Value(lv + rv);
        }
        if (op == "-") {
          return Value(lv - rv);
        }
        return Value(lv * rv);
      }
      case Kind::kCoalesce: {
        const Value l = lhs->Eval(row);
        return l.IsNull() ? rhs->Eval(row) : l;
      }
      case Kind::kConcat: {
        const Value l = lhs->Eval(row);
        if (l.IsNull()) {
          return Value();
        }
        return Value(std::string(l.value.varchar_value) + "!");
      }
      case Kind::kCase:
        return (cond->Eval(row) == 'T' ? then_e : else_e)->Eval(row);
    }
    return Value();
  }

  [[nodiscard]] std::string Render() const {
    switch (kind) {
      case Kind::kConst:
        if (const_null) {
          return "NULL";
        }
        return is_str ? (const_val == kStrA ? "'a'" : "'bb'")
                      : std::to_string(const_val);
      case Kind::kCol:
        return ColName(col);
      case Kind::kArith:
        return "(" + lhs->Render() + " " + op + " " + rhs->Render() + ")";
      case Kind::kCoalesce:
        return "COALESCE(" + lhs->Render() + ", " + rhs->Render() + ")";
      case Kind::kConcat:
        return "(" + lhs->Render() + " || '!')";
      case Kind::kCase:
        return "(CASE WHEN " + cond->Render() + " THEN " + then_e->Render() +
               " ELSE " + else_e->Render() + " END)";
    }
    return "NULL";
  }
};
using SExprPtr = std::unique_ptr<SExpr>;

int PickTypedCol(Gen& g, const MirrorTable& t, ColType type) {
  for (int tries = 0; tries < 8; ++tries) {
    const int c = g.PickCol(t);
    if (t.columns[static_cast<size_t>(c)].type == type) {
      return c;
    }
  }
  for (size_t i = 1; i < t.columns.size(); ++i) {
    if (t.columns[i].type == type) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

SExprPtr GenLeafExpr(Gen& g, const MirrorTable& t, ColType type,
                     bool allow_null = true) {
  auto e = std::make_unique<SExpr>();
  e->is_str = type == ColType::kStr;
  if (g.Chance(40)) {
    e->kind = SExpr::Kind::kConst;
    if (allow_null && g.Chance(15)) {
      e->const_null = true;
    } else {
      e->const_val = type == ColType::kInt ? g.Pick(-3, 3) : g.Pick(0, 1);
    }
    return e;
  }
  e->kind = SExpr::Kind::kCol;
  e->col = PickTypedCol(g, t, type);
  if (e->col < 0) {  // no column of this type: fall back to a NULL constant
    e->kind = SExpr::Kind::kConst;
    e->const_null = true;
  }
  return e;
}

SExprPtr GenSetExpr(Gen& g, const MirrorTable& t, ColType type, int depth) {
  const int roll = g.Pick(0, 99);
  if (depth <= 0 || roll < 60) {
    return GenLeafExpr(g, t, type);
  }
  auto e = std::make_unique<SExpr>();
  e->is_str = type == ColType::kStr;
  if (type == ColType::kInt) {
    if (roll < 80) {
      // The frontend rejects arithmetic on a literal NULL operand, so
      // arith children never render a bare NULL.
      e->kind = SExpr::Kind::kArith;
      e->op = g.Pick(0, 2) == 0 ? "+" : (g.Pick(0, 1) == 0 ? "-" : "*");
      e->lhs = GenLeafExpr(g, t, ColType::kInt, /*allow_null=*/false);
      e->rhs = GenLeafExpr(g, t, ColType::kInt, /*allow_null=*/false);
      return e;
    }
    if (roll < 90) {
      e->kind = SExpr::Kind::kCoalesce;
      e->lhs = GenLeafExpr(g, t, ColType::kInt);
      e->rhs = GenLeafExpr(g, t, ColType::kInt);
      return e;
    }
    e->kind = SExpr::Kind::kCase;
    e->cond = GenPred(g, t, kNoTables, 1, SubqMode::kNone);
    e->then_e = GenSetExpr(g, t, type, depth - 1);
    e->else_e = GenLeafExpr(g, t, type);
    return e;
  }
  if (roll < 80) {
    e->kind = SExpr::Kind::kConcat;
    e->lhs = GenLeafExpr(g, t, ColType::kStr);
    return e;
  }
  if (roll < 90) {
    e->kind = SExpr::Kind::kCoalesce;
    e->lhs = GenLeafExpr(g, t, ColType::kStr);
    e->rhs = GenLeafExpr(g, t, ColType::kStr);
    return e;
  }
  e->kind = SExpr::Kind::kCase;
  e->cond = GenPred(g, t, kNoTables, 1, SubqMode::kNone);
  e->then_e = GenLeafExpr(g, t, type);
  e->else_e = GenLeafExpr(g, t, type);
  return e;
}

// ---------------------------------------------------------------------------
// Value generation and mirror helpers.
// ---------------------------------------------------------------------------

constexpr int64_t kNullRepr = INT64_MIN;

// Returns {mirror int value (kNullRepr = NULL), SQL literal}.
std::pair<int64_t, std::string> GenIntValue(Gen& g) {
  if (g.Chance(20)) {
    return {kNullRepr, "NULL"};
  }
  const int64_t v = g.Pick(-3, 3);
  return {v, std::to_string(v)};
}

std::pair<int64_t, std::string> GenStrValue(Gen& g) {
  if (g.Chance(20)) {
    return {kNullRepr, "NULL"};
  }
  if (g.Chance(55)) {
    return {kStrA, "'a'"};
  }
  return {kStrB, "'bb'"};
}

// Builds the mirror Row slot vector: u plus per-column values.
Row BuildRow(int64_t u,
             const std::vector<std::pair<int64_t, ColType>>& values) {
  std::vector<Value> cells;
  cells.emplace_back(u);
  for (const auto& [repr, type] : values) {
    if (repr == kNullRepr) {
      cells.emplace_back();
    } else if (type == ColType::kInt) {
      cells.emplace_back(repr);
    } else {
      cells.emplace_back(repr == kStrA ? std::string("a") : std::string("bb"));
    }
  }
  return Row(cells);
}

std::vector<std::string> SortedDump(const std::vector<Row>& rows) {
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const Row& r : rows) {
    out.push_back(r.ToString());
  }
  std::sort(out.begin(), out.end());
  return out;
}

// ---------------------------------------------------------------------------
// Engine helpers.
// ---------------------------------------------------------------------------

std::optional<std::vector<Row>> RunSelect(Database& db, TransactionContext& ctx,
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
  // Lazy result: a mid-stream error would silently truncate the dump used
  // for state comparison — surface it as an error instead.
  if (result.Value().GetStatus() != Status::kSuccess) {
    *error = "stream-error: " + result.Value().GetStatus().GetMessage();
    return std::nullopt;
  }
  return rows;
}

bool RunSql(Database& db, TransactionContext& ctx, const std::string& sql) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  // QueryResults are lazy: drain or the mutation never happens.
  if (!result.HasValue()) {
    return false;
  }
  static_cast<void>(result.Value().Drain());
  if (result.Value().GetStatus() != Status::kSuccess) {
    return false;
  }
  return true;
}

std::string DumpLines(const std::vector<std::string>& rows) {
  std::string out;
  out.reserve(64);
  for (const std::string& r : rows) {
    out += "    " + r + "\n";
  }
  return out;
}

// ---------------------------------------------------------------------------
// Session driver shared by generation and replay.
// ---------------------------------------------------------------------------

// Executes one DDL pseudo-statement through the C++ index API.
bool ApplyDdl(Database& db, TransactionContext& ctx, const std::string& ddl,
              std::string* error) {
  // "CREATE INDEX <name> ON <table> KEY(i,j) INCLUDE(k,l)"
  auto parse_slots = [](const std::string& text) {
    std::vector<slot_t> slots;
    size_t pos = 0;
    while (pos < text.size()) {
      const size_t comma = text.find(',', pos);
      const std::string part = text.substr(
          pos, comma == std::string::npos ? std::string::npos : comma - pos);
      slots.push_back(static_cast<slot_t>(std::stoi(part)));
      if (comma == std::string::npos) {
        break;
      }
      pos = comma + 1;
    }
    return slots;
  };
  const auto name_pos = ddl.find("CREATE INDEX ");
  if (name_pos == std::string::npos) {
    *error = "unknown ddl: " + ddl;
    return false;
  }
  const size_t name_end = ddl.find(" ON ", name_pos);
  const std::string name =
      ddl.substr(name_pos + strlen("CREATE INDEX "),
                 name_end - name_pos - strlen("CREATE INDEX "));
  const size_t key_pos = ddl.find(" KEY(", name_end);
  const size_t key_end = ddl.find(')', key_pos);
  const size_t inc_pos = ddl.find(" INCLUDE(", key_end);
  size_t inc_end = std::string::npos;
  if (inc_pos != std::string::npos) {
    inc_end = ddl.find(')', inc_pos);
  }
  const std::string table = ddl.substr(name_end + strlen(" ON "),
                                       key_pos - name_end - strlen(" ON "));
  std::vector<slot_t> key = parse_slots(ddl.substr(
      key_pos + strlen(" KEY("), key_end - key_pos - strlen(" KEY(")));
  std::vector<slot_t> include;
  if (inc_pos != std::string::npos) {
    include = parse_slots(ddl.substr(inc_pos + strlen(" INCLUDE("),
                                     inc_end - inc_pos - strlen(" INCLUDE(")));
  }
  const IndexSchema schema(name, key, include, IndexMode::kNonUnique);
  const Status status = db.CreateIndex(ctx, table, schema);
  if (status != Status::kSuccess) {
    *error = "CreateIndex failed: " + ddl;
    return false;
  }
  return true;
}

// Debug aid (TINYLAMB_SESSION_DEBUG_PAGES=1): dump a table's row-page
// chain so a mismatch report shows which page ids the scan walked and
// what type each page actually is.
std::string DebugDumpTableChain(TransactionContext& ctx,
                                const std::string& name) {
  if (std::getenv("TINYLAMB_SESSION_DEBUG_PAGES") == nullptr) {
    return "";
  }
  std::string report;
  StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(name);
  if (!tbl.HasValue() || tbl.Value() == nullptr) {
    return "  page chain: <table " + name + " not found>\n";
  }
  PageManager* pm = ctx.txn_.GetPageManager();
  report +=
      "  page chain: first=" + std::to_string(tbl.Value()->FirstPageId()) +
      " last=" + std::to_string(tbl.Value()->LastPageId()) + "\n";
  page_id_t pid = tbl.Value()->FirstPageId();
  for (int hops = 0; pid != 0 && hops < 32; ++hops) {
    StatusOr<PageRef> ref = pm->GetPage(pid);
    if (!ref.HasValue()) {
      report += "    pid=" + std::to_string(pid) + " GetPage failed\n";
      break;
    }
    PageRef page = ref.MoveValue();
    report += "    pid=" + std::to_string(pid) +
              " type=" + std::to_string(static_cast<int>(page->Type()));
    if (page->Type() == PageType::kRowPage) {
      const RowPage& rp = page->body.row_page;
      report += " rows=" + std::to_string(rp.RowCount()) +
                " max=" + std::to_string(rp.RowMax()) +
                " page_lsn=" + std::to_string(page->PageLSN()) + " slots=[";
      for (slot_t s = 0; s < rp.RowMax(); ++s) {
        report += "{" + std::to_string(rp.rows_[s].offset) + "," +
                  std::to_string(rp.rows_[s].size) + "}";
      }
      report += "]";
      pid = rp.next_page_id_;
    } else {
      report += " <not a row page>\n";
      break;
    }
    report += "\n";
    report += ctx.txn_.DebugDumpVersionChains(page->PageID());
  }
  // Cross-check: a raw heap scan bypassing the SQL/plan layer.  If it
  // sees 0 rows while the engine returned phantom rows, the phantoms
  // came through a non-heap path (e.g. a stale secondary index).
  report +=
      "  indexes on table: " + std::to_string(tbl.Value()->IndexCount()) + "\n";
  Iterator scan = tbl.Value()->BeginFullScan(ctx.txn_);
  size_t raw_rows = 0;
  for (; scan.IsValid(); ++scan) {
    ++raw_rows;
  }
  report += "  raw BeginFullScan rows: " + std::to_string(raw_rows) + "\n";
  return report;
}

std::string CheckTable(Database& db, TransactionContext& ctx,
                       const MirrorTable& t, std::string* error) {
  const std::string sql = "SELECT * FROM " + t.name + " ORDER BY u;";
  auto got = RunSelect(db, ctx, sql, error);
  if (!got.has_value()) {
    return "engine error on check: " + sql + " :: " + *error + "\n";
  }
  const std::vector<std::string> expected = SortedDump(t.rows);
  std::vector<std::string> actual;
  for (const Row& r : *got) {
    actual.push_back(r.ToString());
  }
  std::sort(actual.begin(), actual.end());
  if (expected == actual) {
    return "";
  }
  std::string report = "table " + t.name + ": mirror has " +
                       std::to_string(expected.size()) + " rows, engine has " +
                       std::to_string(actual.size()) + "\n";
  report += "  expected:\n" + DumpLines(expected);
  report += "  actual:\n" + DumpLines(actual);
  report += DebugDumpTableChain(ctx, t.name);
  // Show first differing row.
  for (size_t i = 0; i < std::max(expected.size(), actual.size()); ++i) {
    const std::string e = i < expected.size() ? expected[i] : "<missing>";
    const std::string a = i < actual.size() ? actual[i] : "<missing>";
    if (e != a) {
      report += "  first diff at " + std::to_string(i) + ": expected ";
      report += e;
      report += " vs actual ";
      report += a;
      report += "\n";
      break;
    }
  }
  return report;
}

// ---------------------------------------------------------------------------
// Dual-transaction sessions: two contexts interleave point DML on one thread.
// Snapshot isolation means each context sees (committed at its own Begin) +
// its own writes, so the mirror keeps three states — `committed`, and a
// base+view pair per txn.  A commit merges the txn's delta (diff of its base
// and its view) into committed; an abort/crash drops it.  Write-write
// conflicts are deterministic under the default kLegacy deadlock policy: the
// would-be waiter's bounded intent wait can never be satisfied on a single
// thread, so the statement fails and nothing is applied.
// ---------------------------------------------------------------------------

int64_t RowU(const Row& r) { return r[0].value.int_value; }

MirrorTable* FindMirror(std::vector<MirrorTable>& v, const std::string& name) {
  for (MirrorTable& m : v) {
    if (m.name == name) {
      return &m;
    }
  }
  return nullptr;
}

const MirrorTable* FindMirror(const std::vector<MirrorTable>& v,
                              const std::string& name) {
  for (const MirrorTable& m : v) {
    if (m.name == name) {
      return &m;
    }
  }
  return nullptr;
}

void SortRowsByU(std::vector<Row>& rows) {
  std::sort(rows.begin(), rows.end(),
            [](const Row& a, const Row& b) { return RowU(a) < RowU(b); });
}

// Merges txn's writes — the exact key set it owns in |owner| — into
// committed.  Driving the merge by written keys (not by a value diff
// against the begin-time snapshot) matters: a txn can commit a write whose
// value equals its own snapshot image while the committed image differs —
// e.g. its base says 'a', another txn committed 'bb', and this txn writes
// 'a' back.  A value-diff merge would skip that key and lose the commit.
void ApplyDelta(std::vector<MirrorTable>& committed,
                const std::vector<MirrorTable>& view,
                const std::vector<std::pair<std::string, int64_t>>& written) {
  for (const auto& [name, u] : written) {
    MirrorTable* ct = FindMirror(committed, name);
    const MirrorTable* vt = FindMirror(view, name);
    if (ct == nullptr || vt == nullptr) {
      continue;  // dual sessions never DDL; defensive only
    }
    const Row* view_row = nullptr;
    for (const Row& r : vt->rows) {
      if (RowU(r) == u) {
        view_row = &r;
        break;
      }
    }
    auto it = std::find_if(ct->rows.begin(), ct->rows.end(),
                           [u](const Row& c) { return RowU(c) == u; });
    if (view_row == nullptr) {
      if (it != ct->rows.end()) {
        ct->rows.erase(it);
      }
    } else if (it != ct->rows.end()) {
      *it = *view_row;
    } else {
      ct->rows.push_back(*view_row);
    }
    SortRowsByU(ct->rows);
  }
}

// Uncommitted write/intent holder for one row in the dual-session mirror.
// vacant marks a pending delete: the slot is physically empty, so another
// transaction's DML scan skips it instead of conflicting on the intent.
// written marks an actual row modification — bare scan intents (a DML
// source scan write-intent-locks every occupied slot before resolving its
// head version) are NOT written and must never feed ApplyDelta at commit.
struct OwnerInfo {
  int txn;
  bool vacant;
  bool written;
};

// Keys txn |x| actually wrote (scan-held bare intents excluded), in commit
// order.
std::vector<std::pair<std::string, int64_t>> WrittenKeys(
    const std::map<std::pair<std::string, int64_t>, OwnerInfo>& owner, int x) {
  std::vector<std::pair<std::string, int64_t>> keys;
  for (const auto& [key, holder] : owner) {
    if (holder.txn == x && holder.written) {
      keys.push_back(key);
    }
  }
  return keys;
}

// Runs one dual-transaction session.  Shares the trace format with the
// single-txn driver; txn-1 steps serialize with a "2" suffix.
std::string RunDualTxnSession(Gen& g, bool verbose, SessionStats* stats,
                              SessionTrace* trace) {
  SessionTrace& t = *trace;
  ScopedDb sdb("sql_session_fuzz");
  if (!sdb) {
    return "failed to create throwaway database\n";
  }
  Database* db = sdb.get();
  std::optional<SqlEngine> engine;
  engine.emplace(*db);

  // ---- setup phase: schema + seed rows under one committed txn ----
  TransactionContext setup_ctx = db->BeginContext();
  std::vector<MirrorTable> committed;
  std::map<std::string, int64_t> next_u;
  const int table_count = g.Pick(1, 2);
  for (int i = 0; i < table_count; ++i) {
    MirrorTable m;
    m.name = "tbl" + std::to_string(i);
    m.columns.push_back({"u", ColType::kInt});
    std::string cols = "u INT64";
    const int col_count = g.Pick(2, 3);
    for (int c = 0; c < col_count; ++c) {
      const std::string cname = "c" + std::to_string(m.columns.size());
      if (g.Chance(60)) {
        cols += ", " + cname + " INT64";
        m.columns.push_back({cname, ColType::kInt});
      } else {
        cols += ", " + cname + " VARCHAR(8)";
        m.columns.push_back({cname, ColType::kStr});
      }
    }
    const std::string sql = "CREATE TABLE " + m.name + " (" + cols + ");";
    if (!RunSql(*db, setup_ctx, sql)) {
      return "dual setup CREATE TABLE rejected: " + sql +
             " :: " + engine->LastError() + "\n";
    }
    t.steps.push_back({.sql = sql});
    next_u[m.name] = 0;
    const int seed_rows = g.Pick(2, 5);
    for (int r = 0; r < seed_rows; ++r) {
      std::vector<std::pair<int64_t, ColType>> values;
      std::string tuple = "(" + std::to_string(next_u[m.name]++);
      for (size_t c = 1; c < m.columns.size(); ++c) {
        const auto [repr, lit] = m.columns[c].type == ColType::kInt
                                     ? GenIntValue(g)
                                     : GenStrValue(g);
        values.emplace_back(repr, m.columns[c].type);
        tuple += ", " + lit;
      }
      tuple += ")";
      const std::string ins =
          "INSERT INTO " + m.name + " VALUES " + tuple + ";";
      if (!RunSql(*db, setup_ctx, ins)) {
        return "dual setup INSERT rejected: " + ins +
               " :: " + engine->LastError() + "\n";
      }
      t.steps.push_back({.sql = ins});
      m.rows.push_back(BuildRow(next_u[m.name] - 1, values));
    }
    committed.push_back(std::move(m));
  }
  if (setup_ctx.txn_.PreCommit() != Status::kSuccess) {
    return "dual setup commit failed\n";
  }
  t.steps.push_back({.is_commit = true});
  if (stats != nullptr) {
    ++stats->commits;
  }

  // ---- dual phase ----
  // ctxs[0] re-begins via the commit above's semantics; begin both contexts.
  TransactionContext ctxs[2] = {db->BeginContext(), db->BeginContext()};
  t.steps.push_back({.is_begin = true, .txn = 1});
  std::vector<MirrorTable> base[2] = {committed, committed};
  std::vector<MirrorTable> view[2] = {committed, committed};
  // owner[(table,u)] = holder of an uncommitted write/intent on that row.
  std::map<std::pair<std::string, int64_t>, OwnerInfo> owner;

  auto check_txn = [&](int x) -> std::string {
    for (const MirrorTable& tab : view[x]) {
      std::string error;
      const std::string mismatch = CheckTable(*db, ctxs[x], tab, &error);
      if (verbose) {
        std::cerr << "[session][dual-check] txn" << x << " " << tab.name
                  << " rows=" << tab.rows.size()
                  << (mismatch.empty() ? " ok" : " MISMATCH") << "\n";
      }
      SessionCheck check;
      check.table = tab.name;
      check.after_step = t.steps.size() - 1;
      check.expected = SortedDump(tab.rows);
      check.txn = x;
      t.checks.push_back(std::move(check));
      if (stats != nullptr) {
        ++stats->checks;
      }
      if (!mismatch.empty()) {
        return "state mismatch on " + tab.name + " (txn " + std::to_string(x) +
               ") after step " + std::to_string(t.steps.size() - 1) + "\n" +
               mismatch;
      }
    }
    return "";
  };

  const int total_steps = g.Pick(15, 35);
  for (int step = 0; step < total_steps; ++step) {
    const int roll = g.Pick(1, 100);
    if (roll <= 9) {
      // Commit txn x: merge its delta into committed, re-begin fresh.
      const int x = g.Pick(0, 1);
      if (ctxs[x].txn_.PreCommit() != Status::kSuccess) {
        return "dual commit failed for txn " + std::to_string(x) + " at step " +
               std::to_string(step) + "\n";
      }
      const auto written = WrittenKeys(owner, x);
      ApplyDelta(committed, view[x], written);
      for (auto it = owner.begin(); it != owner.end();) {
        it = it->second.txn == x ? owner.erase(it) : std::next(it);
      }
      ctxs[x] = db->BeginContext();
      base[x] = committed;
      view[x] = committed;
      t.steps.push_back({.is_commit = true, .txn = x});
      if (stats != nullptr) {
        ++stats->commits;
      }
      if (std::string mismatch = check_txn(x); !mismatch.empty()) {
        return "post-commit " + mismatch;
      }
      continue;
    }
    if (roll <= 15) {
      const int x = g.Pick(0, 1);
      if (ctxs[x].txn_.Abort() != Status::kSuccess) {
        return "dual abort failed for txn " + std::to_string(x) + " at step " +
               std::to_string(step) + "\n";
      }
      for (auto it = owner.begin(); it != owner.end();) {
        it = it->second.txn == x ? owner.erase(it) : std::next(it);
      }
      ctxs[x] = db->BeginContext();
      base[x] = committed;
      view[x] = committed;
      t.steps.push_back({.is_abort = true, .txn = x});
      if (stats != nullptr) {
        ++stats->aborts;
      }
      if (std::string mismatch = check_txn(x); !mismatch.empty()) {
        return "post-abort " + mismatch;
      }
      continue;
    }
    if (roll <= 19) {
      // Crash: BOTH in-flight transactions are loser-undone.
      sdb.CrashAndReopen();
      if (!sdb) {
        return "crash recovery failed to reopen database at step " +
               std::to_string(step) + "\n";
      }
      db = sdb.get();
      engine.emplace(*db);
      ctxs[0] = db->BeginContext();
      ctxs[1] = db->BeginContext();
      owner.clear();
      base[0] = view[0] = committed;
      base[1] = view[1] = committed;
      t.steps.push_back({.is_crash = true});
      if (stats != nullptr) {
        ++stats->crashes;
      }
      if (std::string mismatch = check_txn(0); !mismatch.empty()) {
        return "post-crash " + mismatch;
      }
      if (std::string mismatch = check_txn(1); !mismatch.empty()) {
        return "post-crash " + mismatch;
      }
      continue;
    }
    if (roll <= 23) {
      if (db->WriteCheckpoint().GetStatus() != Status::kSuccess) {
        return "dual checkpoint failed at step " + std::to_string(step) + "\n";
      }
      t.steps.push_back({.is_checkpoint = true});
      if (stats != nullptr) {
        ++stats->checkpoints;
      }
      continue;
    }

    // Statement on a randomly chosen context.
    const int x = g.Pick(0, 1);
    MirrorTable* target = &view[x][static_cast<size_t>(
        g.Pick(0, static_cast<int>(view[x].size()) - 1))];
    const int oproll = g.Pick(1, 100);
    if (oproll <= 25) {
      // Check this txn's view of one table.
      std::string error;
      const std::string mismatch = CheckTable(*db, ctxs[x], *target, &error);
      SessionCheck check;
      check.table = target->name;
      check.after_step = t.steps.size() - 1;
      check.expected = SortedDump(target->rows);
      check.txn = x;
      t.checks.push_back(std::move(check));
      if (stats != nullptr) {
        ++stats->checks;
      }
      if (!mismatch.empty()) {
        return "state mismatch on " + target->name + " (txn " +
               std::to_string(x) + ") after step " +
               std::to_string(t.steps.size() - 1) + "\n" + mismatch;
      }
      continue;
    }
    if (oproll <= 55 || target->rows.empty()) {
      // Single-row INSERT: fresh u, no predicted intent conflict (a failure
      // here means an insert slot race — record it and move on).
      const int64_t u = next_u[target->name]++;
      std::vector<std::pair<int64_t, ColType>> values;
      std::string tuple = "(" + std::to_string(u);
      for (size_t c = 1; c < target->columns.size(); ++c) {
        const auto [repr, lit] = target->columns[c].type == ColType::kInt
                                     ? GenIntValue(g)
                                     : GenStrValue(g);
        values.emplace_back(repr, target->columns[c].type);
        tuple += ", " + lit;
      }
      tuple += ")";
      const std::string sql =
          "INSERT INTO " + target->name + " VALUES " + tuple + ";";
      if (RunSql(*db, ctxs[x], sql)) {
        t.steps.push_back({.txn = x, .sql = sql});
        target->rows.push_back(BuildRow(u, values));
        owner[{target->name, u}] = OwnerInfo{x, false, true};
      } else {
        t.steps.push_back({.txn = x, .must_fail = true, .sql = sql});
        if (stats != nullptr) {
          ++stats->conflicts;
        }
      }
      continue;
    }

    // Single-row UPDATE or DELETE: WHERE t.u = <const>.  The DML source
    // scan takes a write intent on every physically occupied slot of the
    // table and resolves each row at its head version, so:
    //  - any uncommitted non-vacant write by the OTHER txn on this table
    //    makes the scan fail (write-intent conflict);
    //  - the predicate is evaluated on the committed head image, not this
    //    txn's snapshot — a row whose head moved is written at its NEW
    //    image, and a row whose head is gone is a vacuous no-op.
    const size_t row_idx = static_cast<size_t>(
        g.Pick(0, static_cast<int>(target->rows.size()) - 1));
    const Row old_row = target->rows[row_idx];
    const int64_t u = RowU(old_row);
    const auto owner_it = owner.find({target->name, u});
    bool predicted_conflict = false;
    for (const auto& [key, info] : owner) {
      if (key.first == target->name && info.txn != x && !info.vacant) {
        predicted_conflict = true;
        break;
      }
    }
    // The image the predicate sees for the target row: this txn's staged
    // image when it owns the row, else the committed head — and when the
    // committed table no longer has u (deleted or its slot reused), or the
    // other txn holds a pending delete on it, the position is dead/vacant
    // at head and the statement is a vacuous success.
    const MirrorTable* committed_tbl = FindMirror(committed, target->name);
    const Row* committed_row = nullptr;
    if (committed_tbl != nullptr) {
      for (const Row& r : committed_tbl->rows) {
        if (RowU(r) == u) {
          committed_row = &r;
          break;
        }
      }
    }
    const bool foreign_vacant = owner_it != owner.end() &&
                                owner_it->second.txn != x &&
                                owner_it->second.vacant;
    const Row* head_row = nullptr;
    if (!foreign_vacant) {
      head_row = (owner_it != owner.end() && owner_it->second.txn == x)
                     ? &old_row
                     : committed_row;
    }
    std::string sql;
    std::vector<std::pair<int, SExprPtr>> sets;
    const bool is_update = g.Chance(55);
    if (is_update) {
      const int set_col =
          g.Pick(1, static_cast<int>(target->columns.size()) - 1);
      sets.emplace_back(
          set_col,
          GenSetExpr(g, *target,
                     target->columns[static_cast<size_t>(set_col)].type, 1));
      sql = "UPDATE " + target->name + " SET " + ColName(set_col) + " = " +
            sets[0].second->Render() + " WHERE " + target->name +
            ".u = " + std::to_string(u) + ";";
    } else {
      sql = "DELETE FROM " + target->name + " WHERE " + target->name +
            ".u = " + std::to_string(u) + ";";
    }
    const bool ok = RunSql(*db, ctxs[x], sql);
    if (predicted_conflict && ok) {
      return "concurrent write-intent violated: " + sql + " succeeded while" +
             " txn " +
             std::to_string(owner_it != owner.end() ? owner_it->second.txn
                                                    : 1 - x) +
             " holds uncommitted writes on " + target->name + "\n";
    }
    if (!ok) {
      t.steps.push_back({.txn = x, .must_fail = true, .sql = sql});
      if (stats != nullptr) {
        ++stats->conflicts;
      }
      continue;
    }
    t.steps.push_back({.txn = x, .sql = sql});
    // The successful scan intent-locked every occupied slot of the table;
    // record that so a later DML by the other txn predicts the conflict.
    if (committed_tbl != nullptr) {
      for (const Row& r : committed_tbl->rows) {
        owner.try_emplace({target->name, RowU(r)}, OwnerInfo{x, false, false});
      }
    }
    if (head_row == nullptr) {
      // Vacuous success: the predicate sees a dead/vacant position at head,
      // so nothing is written and our snapshot keeps showing the row.
      continue;
    }
    if (is_update) {
      // The written image is the head row with SET applied — columns the
      // statement does not touch take their head values, not the snapshot's.
      std::vector<std::pair<int, Value>> results;
      for (const auto& [col, expr] : sets) {
        results.emplace_back(col, expr->Eval(*head_row));
      }
      target->rows[row_idx] = *head_row;
      for (const auto& [col, v] : results) {
        target->rows[row_idx][static_cast<size_t>(col)] = v;
      }
      owner[{target->name, u}] = OwnerInfo{x, false, true};
    } else {
      target->rows.erase(target->rows.begin() + static_cast<long>(row_idx));
      owner[{target->name, u}] = OwnerInfo{x, true, true};
    }
  }

  // ---- teardown: commit both, then check both views ----
  for (int x = 0; x < 2; ++x) {
    if (ctxs[x].txn_.PreCommit() != Status::kSuccess) {
      return "dual teardown commit failed for txn " + std::to_string(x) + "\n";
    }
    const auto written = WrittenKeys(owner, x);
    ApplyDelta(committed, view[x], written);
    ctxs[x] = db->BeginContext();
    base[x] = committed;
    view[x] = committed;
    t.steps.push_back({.is_commit = true, .txn = x});
    if (stats != nullptr) {
      ++stats->commits;
    }
    if (std::string mismatch = check_txn(x); !mismatch.empty()) {
      return "final " + mismatch;
    }
  }
  return "";
}

}  // namespace

std::string RunSessionIteration(std::mt19937& rng, bool verbose,
                                SessionStats* stats, SessionTrace* trace) {
  Gen g(rng);
  SessionTrace local;
  SessionTrace& t = (trace != nullptr) ? *trace : local;

  // ~30% of sessions run the dual-transaction interleaving driver: snapshot
  // isolation + write-intent conflicts are exercised that the single-txn
  // path cannot reach.
  if (g.Chance(30)) {
    return RunDualTxnSession(g, verbose, stats, &t);
  }

  ScopedDb sdb("sql_session_fuzz");
  if (!sdb) {
    return "failed to create throwaway database\n";
  }
  // Held as a pointer so a crash step can rebind to the reopened Database.
  Database* db = sdb.get();
  TransactionContext ctx = db->BeginContext();
  std::optional<SqlEngine> engine;
  engine.emplace(*db);

  std::vector<MirrorTable> tables;
  // Committed mirror snapshot: an abort rolls the engine AND the mirror back
  // to this state (DDL is transactional — verified by catalog abort tests).
  std::vector<MirrorTable> committed_tables;
  int table_counter = 0;
  int index_counter = 0;
  std::string report;
  report.reserve(256);

  auto create_table = [&]() -> bool {
    const std::string name = "tbl" + std::to_string(table_counter++);
    std::string cols = "u INT64";
    MirrorTable m;
    m.name = name;
    m.columns.push_back({"u", ColType::kInt});
    const int col_count = g.Pick(2, 3);
    for (int i = 0; i < col_count; ++i) {
      const std::string cname = "c" + std::to_string(m.columns.size());
      if (g.Chance(60)) {
        cols += ", " + cname + " INT64";
        m.columns.push_back({cname, ColType::kInt});
      } else {
        cols += ", " + cname + " VARCHAR(8)";
        m.columns.push_back({cname, ColType::kStr});
      }
    }
    const std::string sql = "CREATE TABLE " + name + " (" + cols + ");";
    if (!RunSql(*db, ctx, sql)) {
      report =
          "CREATE TABLE rejected: " + sql + " :: " + engine->LastError() + "\n";
      return false;
    }
    t.steps.push_back({.sql = sql});
    tables.push_back(std::move(m));
    return true;
  };

  // Two independent create_table() calls: the repetition is the point, not
  // a copy-paste error.
  if (!create_table() ||  // NOLINT(misc-redundant-expression)
      !create_table()) {
    return report;
  }

  // Checks every mirrored table against the engine right now; used after
  // commit/abort boundaries to pin the post-boundary state sharply.
  auto check_all_tables = [&]() -> std::string {
    for (const MirrorTable& tab : tables) {
      std::string error;
      const std::string mismatch = CheckTable(*db, ctx, tab, &error);
      SessionCheck check;
      check.table = tab.name;
      check.after_step = t.steps.size() - 1;
      check.expected = SortedDump(tab.rows);
      t.checks.push_back(std::move(check));
      if (stats != nullptr) {
        ++stats->checks;
      }
      if (!mismatch.empty()) {
        return "state mismatch on " + tab.name + " after step " +
               std::to_string(t.steps.size() - 1) + "\n" + mismatch;
      }
    }
    return "";
  };

  const int total_steps = g.Pick(20, 45);
  for (int step = 0; step < total_steps; ++step) {
    // Commit boundary: periodic PreCommit exercises WAL + cross-txn reads.
    if (step > 0 && step % 8 == 0) {
      if (ctx.txn_.PreCommit() != Status::kSuccess) {
        return "PreCommit failed at step " + std::to_string(step) + "\n";
      }
      ctx = db->BeginContext();
      committed_tables = tables;
      t.steps.push_back({.is_ddl = false, .is_commit = true, .sql = {}});
      if (stats != nullptr) {
        ++stats->commits;
      }
    } else if (step > 0 && g.Chance(9)) {
      // Abort boundary: Transaction::Abort walks the undo chain (ARIES CLR),
      // rolling back DML, DDL, and index builds since the last commit.
      if (ctx.txn_.Abort() != Status::kSuccess) {
        return "Abort failed at step " + std::to_string(step) + "\n";
      }
      ctx = db->BeginContext();
      tables = committed_tables;
      t.steps.push_back(
          {.is_ddl = false, .is_commit = false, .is_abort = true, .sql = {}});
      if (stats != nullptr) {
        ++stats->aborts;
      }
      if (std::string mismatch = check_all_tables(); !mismatch.empty()) {
        return "post-abort " + mismatch;
      }
    } else if (step > 0 && g.Chance(4)) {
      // Crash boundary: discard the buffer pool and reopen the same files.
      // ARIES recovery must restore exactly the committed mirror — REDO for
      // committed work, loser-UNDO for the in-flight transaction (which the
      // mirror treats as an abort), free-list rebuild, index/catalog redo.
      sdb.CrashAndReopen();
      if (!sdb) {
        return "crash recovery failed to reopen database at step " +
               std::to_string(step) + "\n";
      }
      db = sdb.get();
      engine.emplace(*db);
      ctx = db->BeginContext();
      tables = committed_tables;
      t.steps.push_back({.is_ddl = false,
                         .is_commit = false,
                         .is_abort = false,
                         .is_crash = true,
                         .sql = {}});
      if (stats != nullptr) {
        ++stats->crashes;
      }
      if (std::string mismatch = check_all_tables(); !mismatch.empty()) {
        return "post-crash " + mismatch;
      }
    } else if (step > 0 && g.Chance(6)) {
      // Checkpoint boundary: force a fuzzy checkpoint mid-transaction.  The
      // visible state must not change (the ATT snapshot may even carry this
      // still-open transaction), but a later crash now replays from the
      // DPT rather than LSN 0 — a materially different recovery path.
      if (db->WriteCheckpoint().GetStatus() != Status::kSuccess) {
        return "checkpoint failed at step " + std::to_string(step) + "\n";
      }
      t.steps.push_back({.is_checkpoint = true});
      if (stats != nullptr) {
        ++stats->checkpoints;
      }
      if (std::string mismatch = check_all_tables(); !mismatch.empty()) {
        return "post-checkpoint " + mismatch;
      }
    }

    MirrorTable* target = tables.empty()
                              ? nullptr
                              : &tables[static_cast<size_t>(g.Pick(
                                    0, static_cast<int>(tables.size()) - 1))];

    enum class Op {
      kInsert,
      kUpdate,
      kDelete,
      kCheck,
      kIndex,
      kDropTable,
      kCreateTable,
      kAnalyze
    };
    Op op = Op::kCheck;
    const int roll = g.Pick(1, 100);
    // The nullptr branch and the small-table branch share a body by design:
    // both refill the table list.
    if (target == nullptr) {  // NOLINT(bugprone-branch-clone)
      op = Op::kCreateTable;
    } else if (static_cast<int>(tables.size()) >= 3 && roll <= 8) {
      op = Op::kDropTable;
    } else if (roll <= 35 && target->rows.size() < 20) {
      op = Op::kInsert;
    } else if (roll <= 55) {
      op = Op::kUpdate;
    } else if (roll <= 65) {
      op = Op::kDelete;
    } else if (roll <= 75 && target->columns.size() >= 2) {
      op = Op::kIndex;
    } else if (roll <= 80 && static_cast<int>(tables.size()) < 3) {
      op = Op::kCreateTable;
    } else if (roll <= 86) {
      // ANALYZE refreshes persisted statistics + bumps the schema epoch —
      // invisible to the row mirror but it walks the stats B+Tree inside
      // the open transaction, so abort/crash must roll it back cleanly.
      op = Op::kAnalyze;
    } else {
      op = Op::kCheck;
    }

    switch (op) {
      case Op::kInsert: {
        if (g.Chance(30) && !target->rows.empty()) {
          // INSERT ... SELECT: projection inside the DML write path.  The
          // new u key is `u + next_u` so keys stay unique and ascending.
          const SPredPtr pred =
              GenPred(g, *target, tables, 1, SubqMode::kAnywhere);
          std::vector<SExprPtr> exprs;
          auto bump_u = std::make_unique<SExpr>();
          bump_u->kind = SExpr::Kind::kArith;
          bump_u->op = "+";
          auto u_col = std::make_unique<SExpr>();
          u_col->kind = SExpr::Kind::kCol;
          u_col->col = 0;
          auto u_off = std::make_unique<SExpr>();
          u_off->kind = SExpr::Kind::kConst;
          u_off->const_val = target->next_u;
          bump_u->lhs = std::move(u_col);
          bump_u->rhs = std::move(u_off);
          exprs.push_back(std::move(bump_u));
          for (size_t i = 1; i < target->columns.size(); ++i) {
            exprs.push_back(GenSetExpr(g, *target, target->columns[i].type,
                                       /*depth=*/1));
          }
          std::string sql = "INSERT INTO " + target->name + " SELECT ";
          for (size_t i = 0; i < exprs.size(); ++i) {
            sql += (i == 0 ? "" : ", ") + exprs[i]->Render();
          }
          sql += " FROM " + target->name + " WHERE " + pred->Render() + ";";
          if (!RunSql(*db, ctx, sql)) {
            return "INSERT SELECT rejected: " + sql +
                   " :: " + engine->LastError() + "\n";
          }
          t.steps.push_back({.sql = sql});
          const int64_t next_u = target->next_u;
          const size_t existing = target->rows.size();
          std::vector<Row> added;
          for (size_t i = 0; i < existing; ++i) {
            const Row& row = target->rows[i];
            if (pred->Eval(row) != 'T') {
              continue;
            }
            std::vector<Value> cells;
            cells.reserve(exprs.size());
            for (const SExprPtr& e : exprs) {
              cells.push_back(e->Eval(row));
            }
            added.push_back(Row(std::move(cells)));
          }
          for (Row& r : added) {
            target->rows.push_back(std::move(r));
          }
          // New u values are old_u + next_u; keep next_u above every key.
          target->next_u = next_u * 2 + 1;
          break;
        }
        std::vector<std::pair<int64_t, ColType>> values;
        std::string tuple = "(" + std::to_string(target->next_u++);
        for (size_t i = 1; i < target->columns.size(); ++i) {
          const auto [repr, lit] = target->columns[i].type == ColType::kInt
                                       ? GenIntValue(g)
                                       : GenStrValue(g);
          values.emplace_back(repr, target->columns[i].type);
          tuple += ", " + lit;
        }
        tuple += ")";
        std::string sql;
        if (g.Chance(25)) {
          // Column-list insert with a shuffled order: the engine must map
          // values positionally onto the listed columns, not ordinal ones.
          std::vector<int> order;
          for (size_t i = 0; i < target->columns.size(); ++i) {
            order.push_back(static_cast<int>(i));
          }
          for (size_t i = order.size() - 1; i > 0; --i) {
            std::swap(order[i], order[static_cast<size_t>(g.Pick(0, i))]);
          }
          std::string col_list = "(u";
          std::string lit_list = "(" + std::to_string(target->next_u - 1);
          for (const int i : order) {
            if (i == 0) {
              continue;
            }
            col_list += ", c" + std::to_string(i);
            // values[j] holds the literal for column j + 1 (column 0 is u).
            const int64_t repr = values[static_cast<size_t>(i - 1)].first;
            lit_list +=
                ", " + (repr == kNullRepr
                            ? "NULL"
                            : (values[static_cast<size_t>(i - 1)].second ==
                                       ColType::kInt
                                   ? std::to_string(repr)
                                   : (repr == kStrA ? "'a'" : "'bb'")));
          }
          col_list += ")";
          lit_list += ")";
          sql = "INSERT INTO " + target->name + " " + col_list + " VALUES " +
                lit_list + ";";
        } else {
          sql = "INSERT INTO " + target->name + " VALUES " + tuple + ";";
        }
        if (!RunSql(*db, ctx, sql)) {
          return "INSERT rejected: " + sql + " :: " + engine->LastError() +
                 "\n";
        }
        t.steps.push_back({.sql = sql});
        target->rows.push_back(BuildRow(target->next_u - 1, values));
        break;
      }
      case Op::kUpdate: {
        const SPredPtr pred =
            GenPred(g, *target, tables, g.Pick(1, 2), SubqMode::kConjunct);
        const int set_col =
            g.Pick(1, static_cast<int>(target->columns.size()) - 1);
        std::vector<std::pair<int, SExprPtr>> sets;
        if (g.Chance(45)) {
          // Expression SET: column refs, arithmetic, COALESCE, CASE, string
          // concat — every SET expression sees the pre-update row.
          sets.emplace_back(
              set_col,
              GenSetExpr(g, *target,
                         target->columns[static_cast<size_t>(set_col)].type,
                         1));
          if (g.Chance(20) && target->columns.size() >= 3) {
            int c2 = g.Pick(1, static_cast<int>(target->columns.size()) - 1);
            if (c2 != set_col) {
              if (g.Chance(40) &&
                  target->columns[static_cast<size_t>(c2)].type ==
                      target->columns[static_cast<size_t>(set_col)].type) {
                // Swap-style: SET cX = cY, cY = cX — both RHS must see the
                // pre-update row (a sequential-assign engine loses this).
                auto lhs_ref = std::make_unique<SExpr>();
                lhs_ref->kind = SExpr::Kind::kCol;
                lhs_ref->col = c2;
                auto rhs_ref = std::make_unique<SExpr>();
                rhs_ref->kind = SExpr::Kind::kCol;
                rhs_ref->col = set_col;
                sets.emplace_back(c2, std::move(rhs_ref));
                sets[0].second = std::move(lhs_ref);
              } else {
                sets.emplace_back(
                    c2, GenSetExpr(
                            g, *target,
                            target->columns[static_cast<size_t>(c2)].type, 0));
              }
            }
          }
        } else {
          auto lit = std::make_unique<SExpr>();
          lit->is_str = target->columns[static_cast<size_t>(set_col)].type ==
                        ColType::kStr;
          if (g.Chance(25)) {
            lit->const_null = true;
          } else {
            lit->const_val = lit->is_str ? g.Pick(0, 1) : g.Pick(-3, 3);
          }
          sets.emplace_back(set_col, std::move(lit));
        }
        std::string sql = "UPDATE " + target->name + " SET ";
        for (size_t i = 0; i < sets.size(); ++i) {
          sql += (i == 0 ? "" : ", ") + ColName(sets[i].first) + " = " +
                 sets[i].second->Render();
        }
        sql += " WHERE " + pred->Render() + ";";
        if (!RunSql(*db, ctx, sql)) {
          return "UPDATE rejected: " + sql + " :: " + engine->LastError() +
                 "\n";
        }
        t.steps.push_back({.sql = sql});
        for (Row& row : target->rows) {
          if (pred->Eval(row) != 'T') {
            continue;
          }
          // Evaluate all SET expressions before assigning: every RHS reads
          // the pre-update row.
          std::vector<std::pair<int, Value>> results;
          results.reserve(sets.size());
          for (const auto& [col, expr] : sets) {
            results.emplace_back(col, expr->Eval(row));
          }
          for (const auto& [col, v] : results) {
            row[static_cast<size_t>(col)] = v;
          }
        }
        break;
      }
      case Op::kDelete: {
        const SPredPtr pred = g.Chance(15)
                                  ? nullptr
                                  : GenPred(g, *target, tables, g.Pick(1, 2),
                                            SubqMode::kConjunct);
        const std::string sql = pred ? "DELETE FROM " + target->name +
                                           " WHERE " + pred->Render() + ";"
                                     : "DELETE FROM " + target->name + ";";
        if (!RunSql(*db, ctx, sql)) {
          return "DELETE rejected: " + sql + " :: " + engine->LastError() +
                 "\n";
        }
        t.steps.push_back({.sql = sql});
        std::vector<Row> kept;
        for (Row& row : target->rows) {
          if (pred != nullptr && pred->Eval(row) != 'T') {
            kept.push_back(std::move(row));
          }
        }
        target->rows = std::move(kept);
        break;
      }
      case Op::kIndex: {
        const std::string idx_name = "idx" + std::to_string(index_counter++);
        std::string key_list = std::to_string(g.PickCol(*target));
        if (g.Chance(40)) {
          key_list += ",0";  // fold in the unique u column
        }
        std::string ddl = "CREATE INDEX ";
        ddl += idx_name;
        ddl += " ON ";
        ddl += target->name;
        ddl += " KEY(";
        ddl += key_list;
        ddl += ")";
        if (g.Chance(40)) {
          ddl += " INCLUDE(" + std::to_string(g.PickCol(*target));
          ddl += ")";
        }
        std::string error;
        error.reserve(64);
        if (!ApplyDdl(*db, ctx, ddl, &error)) {
          std::string msg = "index DDL failed at step ";
          msg += std::to_string(step);
          msg += ": ";
          msg += ddl;
          msg += " :: ";
          msg += error;
          msg += "\n";
          return msg;
        }
        t.steps.push_back({.is_ddl = true, .sql = ddl});
        if (stats != nullptr) {
          ++stats->indexes_created;
        }
        break;
      }
      case Op::kDropTable: {
        const std::string sql = "DROP TABLE " + target->name + ";";
        if (!RunSql(*db, ctx, sql)) {
          return "DROP TABLE rejected: " + sql + " :: " + engine->LastError() +
                 "\n";
        }
        t.steps.push_back({.sql = sql});
        tables.erase(std::remove_if(tables.begin(), tables.end(),
                                    [&](const MirrorTable& tab) {
                                      return tab.name == target->name;
                                    }),
                     tables.end());
        if (stats != nullptr) {
          ++stats->tables_dropped;
        }
        break;
      }
      case Op::kCreateTable: {
        if (!create_table()) {
          return report;
        }
        break;
      }
      case Op::kAnalyze: {
        const std::string sql = "ANALYZE " + target->name + ";";
        if (!RunSql(*db, ctx, sql)) {
          return "ANALYZE rejected: " + sql + " :: " + engine->LastError() +
                 "\n";
        }
        t.steps.push_back({.sql = sql});
        break;
      }
      case Op::kCheck: {
        std::string error;
        error.reserve(64);
        const std::string mismatch = CheckTable(*db, ctx, *target, &error);
        if (verbose) {
          std::cerr << "[session][check] " << target->name
                    << " rows=" << target->rows.size()
                    << (mismatch.empty() ? " ok" : " MISMATCH") << "\n";
        }
        SessionCheck check;
        check.table = target->name;
        check.after_step = t.steps.size() - 1;
        check.expected = SortedDump(target->rows);
        t.checks.push_back(std::move(check));
        if (stats != nullptr) {
          ++stats->checks;
        }
        if (!mismatch.empty()) {
          return "state mismatch after step " +
                 std::to_string(t.steps.size() - 1) + " (" +
                 t.steps.back().sql + ")\n" + mismatch;
        }
        break;
      }
    }
  }

  // Final full check of every surviving table.
  if (std::string mismatch = check_all_tables(); !mismatch.empty()) {
    return "final " + mismatch;
  }

  if (stats != nullptr) {
    stats->statements = static_cast<int>(t.steps.size());
  }
  return "";
}

std::string ReplaySessionTrace(const SessionTrace& trace, bool verbose) {
  if (trace.steps.empty()) {
    return "malformed trace: no steps";
  }
  ScopedDb sdb("sql_session_replay");
  if (!sdb) {
    return "failed to create replay database\n";
  }
  Database* db = sdb.get();
  // Context 0 begins implicitly; context 1 begins at "-- begin2" so its
  // snapshot timestamp matches generation exactly.
  std::optional<TransactionContext> ctxs[2];
  ctxs[0].emplace(db->BeginContext());
  std::optional<SqlEngine> engine;
  engine.emplace(*db);
  // Rebuild the mirror from the executed statements (CREATE TABLE shapes the
  // columns; INSERT/UPDATE/DELETE effects are re-derived by re-execution).
  // The expected dumps in the trace carry the ground truth, so the replay
  // only needs to compare against them.
  std::vector<std::string> report;
  size_t check_index = 0;
  for (size_t i = 0; i < trace.steps.size(); ++i) {
    const SessionStep& step = trace.steps[i];
    std::string error;
    error.reserve(64);
    if (!step.is_crash && !step.is_checkpoint && !step.is_begin &&
        !ctxs[step.txn].has_value()) {
      return "malformed trace: step " + std::to_string(i) +
             " uses a context that was never begun\n";
    }
    if (step.is_begin) {
      ctxs[step.txn].emplace(db->BeginContext());
    } else if (step.is_commit) {
      // Reproduce the original transaction boundary: everything committed so
      // far must become visible to a fresh context, exactly as during
      // generation.
      if (ctxs[step.txn]->txn_.PreCommit() != Status::kSuccess) {
        return "replay commit failed at step " + std::to_string(i) + "\n";
      }
      ctxs[step.txn].emplace(db->BeginContext());
    } else if (step.is_abort) {
      if (ctxs[step.txn]->txn_.Abort() != Status::kSuccess) {
        return "replay abort failed at step " + std::to_string(i) + "\n";
      }
      ctxs[step.txn].emplace(db->BeginContext());
    } else if (step.is_crash) {
      sdb.CrashAndReopen();
      if (!sdb) {
        return "replay crash reopen failed at step " + std::to_string(i) + "\n";
      }
      db = sdb.get();
      engine.emplace(*db);
      ctxs[0].emplace(db->BeginContext());
      if (ctxs[1].has_value()) {
        ctxs[1].emplace(db->BeginContext());
      }
    } else if (step.is_checkpoint) {
      // Same fuzzy checkpoint as generation: no transaction boundary, no
      // state change — it only moves where a later crash replays from.
      if (db->WriteCheckpoint().GetStatus() != Status::kSuccess) {
        return "replay checkpoint failed at step " + std::to_string(i) + "\n";
      }
    } else if (step.is_ddl) {
      if (!ApplyDdl(*db, *ctxs[step.txn], step.sql, &error)) {
        return "replay ddl failed at step " + std::to_string(i) + ": " +
               step.sql + " :: " + error + "\n";
      }
    } else {
      const bool ok = RunSql(*db, *ctxs[step.txn], step.sql);
      if (step.must_fail) {
        if (ok) {
          return "replay statement unexpectedly succeeded at step " +
                 std::to_string(i) + ": " + step.sql +
                 " (generation observed a conflict)\n";
        }
      } else if (!ok) {
        return "replay statement failed at step " + std::to_string(i) + ": " +
               step.sql + " :: " + engine->LastError() + "\n";
      }
    }
    while (check_index < trace.checks.size() &&
           trace.checks[check_index].after_step == i) {
      const SessionCheck& check = trace.checks[check_index];
      if (!ctxs[check.txn].has_value()) {
        return "malformed trace: check on unbegun context\n";
      }
      auto got =
          RunSelect(*db, *ctxs[check.txn],
                    "SELECT * FROM " + check.table + " ORDER BY u;", &error);
      if (!got.has_value()) {
        return "replay check failed on " + check.table + " :: " + error + "\n";
      }
      std::vector<std::string> actual;
      for (const Row& r : *got) {
        actual.push_back(r.ToString());
      }
      std::sort(actual.begin(), actual.end());
      std::vector<std::string> expected = check.expected;
      std::sort(expected.begin(), expected.end());
      if (expected != actual) {
        std::string mismatch = "replay mismatch on " + check.table +
                               " after step " + std::to_string(i) + "\n";
        mismatch += "  expected:\n" + DumpLines(expected);
        mismatch += "  actual:\n" + DumpLines(actual);
        mismatch += DebugDumpTableChain(*ctxs[check.txn], check.table);
        return mismatch;
      }
      if (verbose) {
        std::cerr << "[session][replay-check] " << check.table << " ok\n";
      }
      ++check_index;
    }
  }
  return "";
}

std::string SerializeSessionTest(uint64_t seed, const SessionTrace& trace,
                                 const std::string& failure_summary) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  size_t check_index = 0;
  for (size_t i = 0; i < trace.steps.size(); ++i) {
    const SessionStep& step = trace.steps[i];
    // Dual-transaction sessions tag context-1 directives with a "2" suffix;
    // context-0 lines stay identical to single-txn traces.
    const std::string sfx = step.txn == 1 ? "2" : "";
    if (step.is_begin) {
      out += "-- begin" + sfx + "\n";
    } else if (step.is_commit) {
      out += "-- commit" + sfx + "\n";
    } else if (step.is_abort) {
      out += "-- abort" + sfx + "\n";
    } else if (step.is_crash) {
      out += "-- crash\n";
    } else if (step.is_checkpoint) {
      out += "-- checkpoint\n";
    } else if (step.must_fail) {
      out += "-- fail" + sfx + ": " + step.sql + "\n";
    } else {
      out += std::string(step.is_ddl ? "-- ddl" : "-- stmt") + sfx + ": " +
             step.sql + "\n";
    }
    while (check_index < trace.checks.size() &&
           trace.checks[check_index].after_step == i) {
      out +=
          (trace.checks[check_index].txn == 1 ? "-- check2: " : "-- check: ") +
          trace.checks[check_index].table + "\n";
      for (const std::string& row : trace.checks[check_index].expected) {
        out += "-- expect: " + row + "\n";
      }
      ++check_index;
    }
  }
  while (check_index < trace.checks.size()) {
    out += (trace.checks[check_index].txn == 1 ? "-- check2: " : "-- check: ") +
           trace.checks[check_index].table + "\n";
    for (const std::string& row : trace.checks[check_index].expected) {
      out += "-- expect: " + row + "\n";
    }
    ++check_index;
  }
  if (!failure_summary.empty()) {
    // The line-based format cannot carry the multi-line report body; the
    // first line names the failure and the rest stays in the fuzzer log.
    out +=
        "-- failure: " + failure_summary.substr(0, failure_summary.find('\n')) +
        "\n";
  }
  return out;
}

bool ParseSessionTest(std::string_view text, uint64_t* seed,
                      SessionTrace* trace, std::string* failure_summary) {
  *seed = 0;
  *trace = SessionTrace{};
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
    if (line.starts_with("-- tinylamb-session-test")) {
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
    value.reserve(64);
    if (line == "-- commit" || line == "-- commit2") {
      trace->steps.push_back(
          {.is_commit = true, .txn = line.back() == '2' ? 1 : 0});
    } else if (line == "-- abort" || line == "-- abort2") {
      trace->steps.push_back(
          {.is_abort = true, .txn = line.back() == '2' ? 1 : 0});
    } else if (line == "-- crash") {
      trace->steps.push_back({.is_crash = true});
    } else if (line == "-- checkpoint") {
      trace->steps.push_back({.is_checkpoint = true});
    } else if (line == "-- begin" || line == "-- begin2") {
      trace->steps.push_back(
          {.is_begin = true, .txn = line.back() == '2' ? 1 : 0});
    } else if (consume("-- seed: ", &value)) {
      *seed = std::stoull(value);
    } else if (consume("-- stmt: ", &value)) {
      trace->steps.push_back({.sql = value});
    } else if (consume("-- stmt2: ", &value)) {
      trace->steps.push_back({.txn = 1, .sql = value});
    } else if (consume("-- ddl: ", &value)) {
      trace->steps.push_back({.is_ddl = true, .sql = value});
    } else if (consume("-- ddl2: ", &value)) {
      trace->steps.push_back({.is_ddl = true, .txn = 1, .sql = value});
    } else if (consume("-- fail: ", &value)) {
      trace->steps.push_back({.must_fail = true, .sql = value});
    } else if (consume("-- fail2: ", &value)) {
      trace->steps.push_back({.txn = 1, .must_fail = true, .sql = value});
    } else if (consume("-- check: ", &value) ||
               consume("-- check2: ", &value)) {
      SessionCheck check;
      check.table = value;
      check.txn = line.starts_with("-- check2:") ? 1 : 0;
      check.after_step = trace->steps.empty() ? 0 : trace->steps.size() - 1;
      trace->checks.push_back(std::move(check));
    } else if (consume("-- expect: ", &value)) {
      if (trace->checks.empty()) {
        return false;
      }
      trace->checks.back().expected.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
    } else if (!failure_summary->empty() && !line.starts_with("--")) {
      // Older repro files embedded the raw multi-line report after
      // "-- failure:"; those continuation lines carry no replay data, so
      // fold them into the summary instead of failing the parse.
      *failure_summary += "\n" + std::string(line);
    } else {
      return false;
    }
  }
  return saw_header && !trace->steps.empty();
}

void SessionFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x51ed270b2f1f8d53ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
  SessionTrace trace;
  std::string report = RunSessionIteration(rng, verbose, nullptr, &trace);
  if (report.empty()) {
    return;
  }

  // Prove the emitted file reproduces before saving it.
  const std::string text =
      SerializeSessionTest(seed, trace, "auto-generated by sql_session_fuzzer");
  uint64_t parsed_seed = 0;
  SessionTrace parsed;
  std::string summary;
  std::string verify;
  summary.reserve(128);
  verify.reserve(128);
  if (!ParseSessionTest(text, &parsed_seed, &parsed, &summary)) {
    verify = "internal error: serialized session does not parse";
  } else {
    verify = ReplaySessionTrace(parsed, false);
    if (verify.empty()) {
      verify = "internal error: serialized session replay holds (flaky)";
    }
  }

  const std::string path =
      "sql_session_fuzz-repro-" + std::to_string(seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();

  std::cerr << "[sql_session_fuzz] seed=" << seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
