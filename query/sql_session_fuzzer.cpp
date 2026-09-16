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
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "database/database.hpp"
#include "index/index_schema.hpp"
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
    kIntCmp,     // int col op (const | other int col)
    kIntIsNull,  // int col IS [NOT] NULL
    kStrCmp,     // str col = / != const
    kStrIsNull,  // str col IS [NOT] NULL
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
      case Kind::kNot:
        return "(NOT " + left->Render() + ")";
      case Kind::kBin:
        return "(" + left->Render() + " " + op + " " + right->Render() + ")";
    }
    return "TRUE";
  }
};

using SPredPtr = std::unique_ptr<SPred>;

SPredPtr GenPred(Gen& g, const MirrorTable& t, int depth) {
  if (depth <= 0 || g.Chance(40)) {
    const int col = g.PickCol(t);
    auto p = std::make_unique<SPred>();
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
    p->left = GenPred(g, t, depth - 1);
    return p;
  }
  p->kind = SPred::Kind::kBin;
  p->op = g.Chance(55) ? "AND" : "OR";
  p->left = GenPred(g, t, depth - 1);
  p->right = GenPred(g, t, depth - 1);
  return p;
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
  if (Status st = result.Value().GetStatus(); st != Status::kSuccess) {
    *error = st.GetMessage().empty() ? ToString(st.GetCode()) : st.GetMessage();
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
  return result.Value().GetStatus() == Status::kSuccess;
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

}  // namespace

std::string RunSessionIteration(std::mt19937& rng, bool verbose,
                                SessionStats* stats, SessionTrace* trace) {
  Gen g(rng);
  SessionTrace local;
  SessionTrace& t = (trace != nullptr) ? *trace : local;

  ScopedDb sdb("sql_session_fuzz");
  if (!sdb) {
    return "failed to create throwaway database\n";
  }
  Database& db = *sdb;
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);

  std::vector<MirrorTable> tables;
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
    if (!RunSql(db, ctx, sql)) {
      report =
          "CREATE TABLE rejected: " + sql + " :: " + engine.LastError() + "\n";
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

  const int total_steps = g.Pick(20, 45);
  for (int step = 0; step < total_steps; ++step) {
    // Commit boundary: periodic PreCommit exercises WAL + cross-txn reads.
    if (step > 0 && step % 8 == 0) {
      if (ctx.txn_.PreCommit() != Status::kSuccess) {
        return "PreCommit failed at step " + std::to_string(step) + "\n";
      }
      ctx = db.BeginContext();
      t.steps.push_back({.is_ddl = false, .is_commit = true, .sql = {}});
      if (stats != nullptr) {
        ++stats->commits;
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
      kCreateTable
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
    } else if (roll <= 82 && static_cast<int>(tables.size()) < 3) {
      op = Op::kCreateTable;
    } else {
      op = Op::kCheck;
    }

    switch (op) {
      case Op::kInsert: {
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
        const std::string sql =
            "INSERT INTO " + target->name + " VALUES " + tuple + ";";
        if (!RunSql(db, ctx, sql)) {
          return "INSERT rejected: " + sql + " :: " + engine.LastError() + "\n";
        }
        t.steps.push_back({.sql = sql});
        target->rows.push_back(BuildRow(target->next_u - 1, values));
        break;
      }
      case Op::kUpdate: {
        const SPredPtr pred = GenPred(g, *target, g.Pick(1, 2));
        const int set_col =
            g.Pick(1, static_cast<int>(target->columns.size()) - 1);
        const bool set_null = g.Chance(25);
        std::string set_lit;
        set_lit.reserve(24);
        int64_t repr = kNullRepr;
        if (!set_null) {
          if (target->columns[static_cast<size_t>(set_col)].type ==
              ColType::kInt) {
            repr = g.Pick(-3, 3);
            set_lit = std::to_string(repr);
          } else {
            repr = g.Pick(0, 1);
            set_lit = repr == kStrA ? "'a'" : "'bb'";
          }
        }
        const std::string sql = "UPDATE " + target->name + " SET c" +
                                std::to_string(set_col) + " = " +
                                (set_null ? "NULL" : set_lit) + " WHERE " +
                                pred->Render() + ";";
        if (!RunSql(db, ctx, sql)) {
          return "UPDATE rejected: " + sql + " :: " + engine.LastError() + "\n";
        }
        t.steps.push_back({.sql = sql});
        for (Row& row : target->rows) {
          if (pred->Eval(row) == 'T') {
            row[static_cast<size_t>(set_col)] =
                set_null
                    ? Value()
                    : (target->columns[static_cast<size_t>(set_col)].type ==
                               ColType::kInt
                           ? Value(repr)
                           : (repr == kStrA ? Value(std::string("a"))
                                            : Value(std::string("bb"))));
          }
        }
        break;
      }
      case Op::kDelete: {
        const SPredPtr pred = GenPred(g, *target, g.Pick(1, 2));
        const std::string sql =
            "DELETE FROM " + target->name + " WHERE " + pred->Render() + ";";
        if (!RunSql(db, ctx, sql)) {
          return "DELETE rejected: " + sql + " :: " + engine.LastError() + "\n";
        }
        t.steps.push_back({.sql = sql});
        std::vector<Row> kept;
        for (Row& row : target->rows) {
          if (pred->Eval(row) != 'T') {
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
        if (!ApplyDdl(db, ctx, ddl, &error)) {
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
        if (!RunSql(db, ctx, sql)) {
          return "DROP TABLE rejected: " + sql + " :: " + engine.LastError() +
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
      case Op::kCheck: {
        std::string error;
        error.reserve(64);
        const std::string mismatch = CheckTable(db, ctx, *target, &error);
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
  for (const MirrorTable& tab : tables) {
    std::string error;
    error.reserve(64);
    const std::string mismatch = CheckTable(db, ctx, tab, &error);
    SessionCheck check;
    check.table = tab.name;
    check.after_step = t.steps.size() - 1;
    check.expected = SortedDump(tab.rows);
    t.checks.push_back(std::move(check));
    if (stats != nullptr) {
      ++stats->checks;
    }
    if (!mismatch.empty()) {
      return "final state mismatch on " + tab.name + "\n" + mismatch;
    }
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
  Database& db = *sdb;
  TransactionContext ctx = db.BeginContext();
  SqlEngine engine(db);
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
    if (step.is_commit) {
      // Reproduce the original transaction boundary: everything committed so
      // far must become visible to a fresh context, exactly as during
      // generation.
      if (ctx.txn_.PreCommit() != Status::kSuccess) {
        return "replay commit failed at step " + std::to_string(i) + "\n";
      }
      ctx = db.BeginContext();
    } else if (step.is_ddl) {
      if (!ApplyDdl(db, ctx, step.sql, &error)) {
        return "replay ddl failed at step " + std::to_string(i) + ": " +
               step.sql + " :: " + error + "\n";
      }
    } else if (!RunSql(db, ctx, step.sql)) {
      return "replay statement failed at step " + std::to_string(i) + ": " +
             step.sql + " :: " + engine.LastError() + "\n";
    }
    while (check_index < trace.checks.size() &&
           trace.checks[check_index].after_step == i) {
      const SessionCheck& check = trace.checks[check_index];
      auto got = RunSelect(
          db, ctx, "SELECT * FROM " + check.table + " ORDER BY u;", &error);
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
    if (step.is_commit) {
      out += "-- commit\n";
    } else {
      out +=
          std::string(step.is_ddl ? "-- ddl: " : "-- stmt: ") + step.sql + "\n";
    }
    while (check_index < trace.checks.size() &&
           trace.checks[check_index].after_step == i) {
      out += "-- check: " + trace.checks[check_index].table + "\n";
      for (const std::string& row : trace.checks[check_index].expected) {
        out += "-- expect: " + row + "\n";
      }
      ++check_index;
    }
  }
  while (check_index < trace.checks.size()) {
    out += "-- check: " + trace.checks[check_index].table + "\n";
    for (const std::string& row : trace.checks[check_index].expected) {
      out += "-- expect: " + row + "\n";
    }
    ++check_index;
  }
  if (!failure_summary.empty()) {
    out += "-- failure: " + failure_summary + "\n";
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
    if (line == "-- commit") {
      trace->steps.push_back({.is_ddl = false, .is_commit = true, .sql = {}});
    } else if (consume("-- seed: ", &value)) {
      *seed = std::stoull(value);
    } else if (consume("-- stmt: ", &value)) {
      trace->steps.push_back({.sql = value});
    } else if (consume("-- ddl: ", &value)) {
      trace->steps.push_back({.is_ddl = true, .sql = value});
    } else if (consume("-- check: ", &value)) {
      SessionCheck check;
      check.table = value;
      check.after_step = trace->steps.empty() ? 0 : trace->steps.size() - 1;
      trace->checks.push_back(std::move(check));
    } else if (consume("-- expect: ", &value)) {
      if (trace->checks.empty()) {
        return false;
      }
      trace->checks.back().expected.push_back(value);
    } else if (consume("-- failure: ", &value)) {
      *failure_summary = value;
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
