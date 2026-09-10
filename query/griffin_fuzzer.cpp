/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/griffin_fuzzer.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "query/sql_engine.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr const char* kTestHeader = "-- tinylamb-griffin-test v1";

// Built-in seed cases. The pool is static so a 32-bit seed fully determines
// an iteration: statement selection, shuffle order, and substitutions are all
// draws from one std::mt19937 stream.  Coverage budget: every iteration merges
// two cases, so the pool is organised as a feature matrix -- outer joins,
// set operations, windows, CTEs, subqueries (IN/EXISTS/scalar), grouping
// variants, and INT64/DOUBLE/string boundary literals -- each row exercising a
// different corner so merged sessions mix unrelated operators.
const std::vector<std::vector<std::string>>& SeedPool() {
  static const std::vector<std::vector<std::string>>*
      pool =
          new std::
              vector<std::vector<std::string>>{
                  // Boundary literals: INT64 extremes, signed zeros, empty
                  // string.
                  {
                      "CREATE TABLE edge (k INT64, d FLOAT64, s STRING);",
                      "INSERT INTO edge VALUES (-9223372036854775808, 1.5, "
                      "''), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(9223372036854775807, -0.0, 'x'), (0, 3.25, 'a%b');",
                      "SELECT k, d FROM edge ORDER BY k;",
                      "SELECT k FROM edge WHERE k > 9223372036854775806;",
                      "SELECT k FROM edge WHERE k < -9223372036854775807;",
                      "SELECT MIN(k), MAX(k), COUNT(*) FROM edge;",
                      "UPDATE edge SET k = 0 WHERE s = '';",
                      "SELECT k FROM edge;",
                  },
                  // Outer joins with NULL keys on both sides.
                  {
                      "CREATE TABLE l (lk INT64, lv STRING);",
                      "INSERT INTO l VALUES (1, 'a'), (2, NULL), (NULL, 'c'), "
                      "(2, 'd');",
                      "CREATE TABLE r (rk INT64, rv INT64);",
                      "INSERT INTO r VALUES (2, 20), (2, 21), (NULL, 0), (3, "
                      "30);",
                      "SELECT l.lk, r.rv FROM l LEFT JOIN r ON l.lk = r.rk "
                      "ORDER BY "  // NOLINT(bugprone-suspicious-missing-comma)
                      "l.lk;",
                      "SELECT l.lk, r.rv FROM l RIGHT JOIN r ON l.lk = r.rk "
                      "ORDER BY "
                      "r.rv;",
                      "SELECT l.lk, r.rv FROM l FULL JOIN r ON l.lk = r.rk;",
                      "SELECT COUNT(*) FROM l LEFT JOIN r ON l.lk = r.rk WHERE "
                      "r.rv "
                      "IS NULL;",
                      "SELECT COALESCE(l.lv, '?') v, r.rv FROM l LEFT JOIN r "
                      "ON "
                      "l.lk = r.rk;",
                  },
                  // Set operations.
                  {
                      "CREATE TABLE s1 (x INT64);",
                      "INSERT INTO s1 VALUES (1), (2), (2), (NULL);",
                      "CREATE TABLE s2 (x INT64);",
                      "INSERT INTO s2 VALUES (2), (3), (NULL);",
                      "SELECT x FROM s1 UNION ALL SELECT x FROM s2 ORDER BY x;",
                      "SELECT x FROM s1 UNION SELECT x FROM s2;",
                      "SELECT x FROM s1 INTERSECT ALL SELECT x FROM s2;",
                      "SELECT x FROM s1 EXCEPT ALL SELECT x FROM s2;",
                  },
                  // Window functions.
                  {
                      "CREATE TABLE w (g INT64, v INT64, t STRING);",
                      "INSERT INTO w VALUES (1, 10, 'a'), (1, NULL, 'b'), (2, "
                      "30, 'a'), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(2, 30, 'c'), (NULL, 5, NULL);",
                      "SELECT g, v, ROW_NUMBER() OVER (ORDER BY v) rn FROM w;",
                      "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) rk "
                      "FROM w;",
                      "SELECT g, SUM(v) OVER (PARTITION BY g) FROM w;",
                      "SELECT g, COUNT(*) OVER () c FROM w ORDER BY c;",
                  },
                  // CTE + correlated and uncorrelated subqueries.
                  {
                      "CREATE TABLE c (id INT64, grp INT64, val INT64);",
                      "INSERT INTO c VALUES (1, 1, 10), (2, 1, NULL), (3, 2, "
                      "30), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(4, NULL, 5);",
                      "WITH big AS (SELECT grp, SUM(val) sv FROM c GROUP BY "
                      "grp) "
                      "SELECT c.id FROM c JOIN big ON c.grp = big.grp WHERE "
                      "big.sv > "
                      "20;",
                      "SELECT id FROM c WHERE grp IN (SELECT grp FROM c WHERE "
                      "val > "
                      "20);",
                      "SELECT id FROM c WHERE EXISTS (SELECT 1 FROM c d WHERE "
                      "d.grp = "
                      "c.grp AND d.val > c.val);",
                      "SELECT id, (SELECT MAX(val) FROM c d WHERE d.grp = "
                      "c.grp) mx "
                      "FROM c ORDER BY id;",
                      "SELECT COUNT(*) FROM c WHERE val > (SELECT AVG(val) "
                      "FROM c);",
                  },
                  // GROUPING SETS / FILTER / DISTINCT aggregates.
                  {
                      "CREATE TABLE gs (region STRING, product STRING, qty "
                      "INT64);",
                      "INSERT INTO gs VALUES ('e','p1',3), ('w','p2',NULL), "
                      "('e','p2',"  // NOLINT(bugprone-suspicious-missing-comma)
                      "7), ('e','p1',-2);",
                      "SELECT region, SUM(qty) FROM gs GROUP BY region;",
                      "SELECT region, COUNT(DISTINCT product) FROM gs GROUP BY "
                      "region;",
                      "SELECT SUM(qty) FILTER (WHERE qty > 0) FROM gs;",
                      "SELECT region, product, SUM(qty) FROM gs GROUP BY "
                      "GROUPING "
                      "SETS ((region), (region, product));",
                  },
                  // CASE / COALESCE / NULLIF / boolean 3VL stress.
                  {
                      "CREATE TABLE cs (i INT64, s STRING);",
                      "INSERT INTO cs VALUES (1, 'x'), (NULL, 'y'), (-2, "
                      "NULL), (0, "  // NOLINT(bugprone-suspicious-missing-comma)
                      "'');",
                      "SELECT i, CASE WHEN i > 0 THEN 'pos' WHEN i < 0 THEN "
                      "'neg' "
                      "WHEN i IS NULL THEN 'null' ELSE 'zero' END c FROM cs;",
                      "SELECT COALESCE(s, 'dflt'), NULLIF(i, 0) FROM cs;",
                      "SELECT i FROM cs WHERE (i > 0) OR (s IS NULL);",
                      "SELECT i FROM cs WHERE NOT (i > 0 AND s IS NOT NULL);",
                      "SELECT i IS NULL IS NULL nn FROM cs;",
                  },
                  // LIKE / BETWEEN / IN-lists with NULLs.
                  {
                      "CREATE TABLE pat (id INT64, txt STRING);",
                      "INSERT INTO pat VALUES (1, 'abc'), (2, 'a%c'), (3, "
                      "'_x'), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(4, ''), (NULL, NULL), (6, 'ABC');",
                      "SELECT id FROM pat WHERE txt LIKE 'a%';",
                      "SELECT id FROM pat WHERE txt LIKE '%c';",
                      "SELECT id FROM pat WHERE txt LIKE '_x';",
                      "SELECT id FROM pat WHERE id BETWEEN 2 AND 4;",
                      "SELECT id FROM pat WHERE id IN (1, NULL, 6);",
                      "SELECT id FROM pat WHERE id NOT IN (1, 2);",
                  },
                  // UPDATE/DELETE with expressions and subquery predicates.
                  {
                      "CREATE TABLE dm (id INT64, v INT64, tag STRING);",
                      "INSERT INTO dm VALUES (1, 100, 'a'), (2, -5, 'b'), (3, "
                      "NULL, "  // NOLINT(bugprone-suspicious-missing-comma)
                      "'a'), (4, 7, NULL);",
                      "UPDATE dm SET v = v * 2 WHERE tag = 'a';",
                      "UPDATE dm SET v = NULL WHERE v < 0;",
                      "DELETE FROM dm WHERE id IN (SELECT id FROM dm WHERE v "
                      "IS NULL);",
                      "SELECT COUNT(*), MIN(v), MAX(v) FROM dm;",
                      "UPDATE dm SET tag = 'z' WHERE v > (SELECT MIN(v) FROM "
                      "dm);",
                      "SELECT id, tag FROM dm ORDER BY id;",
                  },
                  // Self-join / CROSS JOIN with duplicate and NULL keys.
                  {
                      "CREATE TABLE tree (id INT64, parent INT64, w DOUBLE);",
                      "INSERT INTO tree VALUES (1, NULL, 1.0), (2, 1, 2.5), "
                      "(3, 1, "  // NOLINT(bugprone-suspicious-missing-comma)
                      "2.5), (4, 2, NULL), (5, 5, 0.0);",
                      "SELECT c.id, p.id FROM tree c JOIN tree p ON c.parent = "
                      "p.id "
                      "ORDER BY c.id;",
                      "SELECT a.id, b.id FROM tree a CROSS JOIN tree b WHERE "
                      "a.id = "
                      "b.id;",
                      "SELECT t.id FROM tree t LEFT JOIN tree k ON t.parent = "
                      "k.id "
                      "WHERE k.id IS NULL;",
                      "SELECT parent, COUNT(*) FROM tree GROUP BY parent "
                      "HAVING "
                      "COUNT(*) > 1;",
                  },
                  {
                      "CREATE TABLE t1 (a INT64, b STRING, c DOUBLE);",
                      // NOLINTNEXTLINE(bugprone-suspicious-missing-comma)
                      "INSERT INTO t1 (a, b, c) VALUES (1, 'x', 1.5), (2, 'y', "
                      "NULL), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(NULL, 'z', 3.25);",
                      "SELECT a * 3 + 1 FROM t1 WHERE a > 0;",
                      "SELECT b, COUNT(*) FROM t1 GROUP BY b;",
                      "SELECT t1.a, t2.d FROM t1 JOIN t2 ON t1.a = t2.d ORDER "
                      "BY "
                      "t1.a;",
                  },
                  {
                      "CREATE TABLE t2 (d INT64, e STRING);",
                      "INSERT INTO t2 VALUES (1, 'p'), (2, NULL), (3, 'q');",
                      "SELECT d, e FROM t2 WHERE d >= 2 OR e IS NULL;",
                      "SELECT SUM(d) FROM t2;",
                      "SELECT d * -1 AS neg FROM t2;",
                  },
                  {
                      // NOLINTNEXTLINE(bugprone-suspicious-missing-comma)
                      "CREATE TABLE items (id INT64, price DOUBLE, name "
                      "STRING);",
                      // NOLINTNEXTLINE(bugprone-suspicious-missing-comma)
                      "INSERT INTO items VALUES (1, 10.0, 'aa'), (2, 20.5, "
                      "'bb'), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(3, NULL, 'cc'), (4, -5.0, NULL);",
                      "SELECT name, price * 2 AS double_price FROM items WHERE "
                      "price "
                      "< 100.0 ORDER BY price DESC;",
                      "UPDATE items SET price = price + 1 WHERE id = 1;",
                      "SELECT COUNT(*), MIN(price), MAX(price) FROM items;",
                      "DELETE FROM items WHERE id = 3;",
                      "SELECT id FROM items;",
                  },
                  {
                      "CREATE TABLE emp (id INT64, mgr INT64, sal INT64);",
                      // NOLINTNEXTLINE(bugprone-suspicious-missing-comma)
                      "INSERT INTO emp VALUES (1, NULL, 100), (2, 1, 50), (3, "
                      "1, 70), "  // NOLINT(bugprone-suspicious-missing-comma)
                      "(4, 2, NULL);",
                      "SELECT e.id FROM emp e JOIN emp m ON e.mgr = m.id WHERE "
                      "m.sal "
                      "> 60;",
                      "SELECT mgr, SUM(sal) FROM emp GROUP BY mgr HAVING "
                      "SUM(sal) > "
                      "0;",
                      "SELECT id, sal + 1000 * 2 FROM emp WHERE sal IS NOT "
                      "NULL;",
                  },
              };
  return *pool;
}

bool IsSelect(const std::string& sql) { return sql.starts_with("SELECT"); }

bool IsDdl(const std::string& sql) {
  return sql.starts_with("CREATE") || sql.starts_with("DROP");
}

// ---------------------------------------------------------------------------
// Metadata snapshot: the Griffin metadata graph's nodes, harvested from the
// live catalog instead of information_schema queries.
// ---------------------------------------------------------------------------

struct ColumnMeta {
  std::string table;
  std::string name;
  ValueType type;
};

struct MetadataSnapshot {
  std::vector<std::string> tables;
  std::vector<ColumnMeta> columns;

  [[nodiscard]] std::vector<const ColumnMeta*> ColumnsOfType(
      ValueType type, std::mt19937& rng, bool same_table,
      const std::string& table) const {
    std::vector<const ColumnMeta*> matches;
    for (const ColumnMeta& col : columns) {
      if (col.type != type) {
        continue;
      }
      if (same_table && col.table != table) {
        continue;
      }
      matches.push_back(&col);
    }
    if (matches.empty() && same_table) {
      return ColumnsOfType(type, rng, false, table);
    }
    return matches;
  }
};

MetadataSnapshot SnapshotMetadata(Database& db, TransactionContext& ctx) {
  MetadataSnapshot snapshot;
  for (const std::string& table : db.ListTables(ctx)) {
    snapshot.tables.push_back(table);
    auto maybe_table = ctx.GetTable(table);
    if (!maybe_table.HasValue()) {
      continue;
    }
    const Schema& schema = maybe_table.Value()->GetSchema();
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const Column& col = schema.GetColumn(i);
      snapshot.columns.push_back(
          ColumnMeta{.table = table,
                     .name = std::string(col.Name().name),
                     .type = col.Type()});
    }
  }
  return snapshot;
}

// ---------------------------------------------------------------------------
// Metadata-guided substitution (Griffin's Algorithm 2, simplified). Tokenizes
// the statement skipping string literals, and swaps known identifiers for
// catalog entries of the same kind: table -> table, column -> same-typed
// column. Type and containment are the predicates Griffin uses to keep
// mutations semantically plausible.
// ---------------------------------------------------------------------------

bool IsIdentStart(char c) {
  return (std::isalpha(static_cast<unsigned char>(c)) != 0) || c == '_';
}

bool IsIdentChar(char c) {
  return (std::isalnum(static_cast<unsigned char>(c)) != 0) || c == '_';
}

std::string SubstituteIdentifiers(const std::string& sql,
                                  const MetadataSnapshot& meta,
                                  std::mt19937& rng) {
  if (meta.tables.empty()) {
    return sql;
  }
  std::string out;
  out.reserve(sql.size() + 16);
  size_t i = 0;
  while (i < sql.size()) {
    const char c = sql[i];
    if (c == '\'') {  // string literal: copy verbatim
      out.push_back(c);
      ++i;
      while (i < sql.size()) {
        out.push_back(sql[i]);
        const bool quote = sql[i] == '\'';
        ++i;
        if (quote) {
          break;
        }
      }
      continue;
    }
    if (IsIdentStart(c)) {
      size_t end = i + 1;
      while (end < sql.size() && IsIdentChar(sql[end])) {
        ++end;
      }
      const std::string token = sql.substr(i, end - i);
      bool substituted = false;
      for (const std::string& table : meta.tables) {
        if (token == table && !meta.tables.empty()) {
          if (std::uniform_int_distribution<size_t>(0, 2)(rng) == 0) {
            std::uniform_int_distribution<size_t> pick(0,
                                                       meta.tables.size() - 1);
            out += meta.tables[pick(rng)];
            substituted = true;
          }
          break;
        }
      }
      if (!substituted) {
        for (const ColumnMeta& col : meta.columns) {
          if (token != col.name) {
            continue;
          }
          if (std::uniform_int_distribution<size_t>(0, 2)(rng) != 0) {
            break;
          }
          const bool same_table =
              std::uniform_int_distribution<int>(0, 1)(rng) == 0;
          const std::vector<const ColumnMeta*> candidates =
              meta.ColumnsOfType(col.type, rng, same_table, col.table);
          if (!candidates.empty()) {
            std::uniform_int_distribution<size_t> pick(0,
                                                       candidates.size() - 1);
            out += candidates[pick(rng)]->name;
            substituted = true;
          }
          break;
        }
      }
      if (!substituted) {
        out += token;
      }
      i = end;
      continue;
    }
    out.push_back(c);
    ++i;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Engine helpers (same conventions as sql_oracle_fuzzer.cpp).
// ---------------------------------------------------------------------------

struct RunOutcome {
  bool ran = false;    // the engine executed the statement at all
  bool threw = false;  // an exception escaped during execution/drain
  std::string error;
  std::vector<std::string> rows;  // sorted serialization of the result
};

RunOutcome RunStatement(Database& db, TransactionContext& ctx,
                        const std::string& sql) {
  RunOutcome outcome;
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    return outcome;  // rejected: skip, not a finding
  }
  try {
    std::vector<Row> rows;
    Row row;
    while (result.Value().Next(&row)) {
      rows.push_back(row);
    }
    for (Row& r : rows) {
      outcome.rows.push_back(r.ToString());
    }
    std::sort(outcome.rows.begin(), outcome.rows.end());
    outcome.ran = true;
  } catch (const std::exception& error) {
    // "column/table X not found"-class messages are the engine's ordinary
    // rejection mechanism on some paths (cf. executor_test expectations), so
    // they count as rejections, not findings.  The transitional EXC-SHIM
    // boundary (no-exception-rule-migration.md Phase 7) still re-raises data-
    // dependent runtime errors (SUM/+ overflow, struct-field access on a
    // non-struct) as C++ exceptions; those carry the same messages the
    // StatusOr channel would report, so they too are rejections.  Anything
    // else escaping is oracle (1) material.
    const std::string what = error.what();
    for (const std::string_view known :
         {"not found", "no such", "ambiguous column", "numeric value",
          "overflow", "cannot access field"}) {
      if (what.find(known) != std::string::npos) {
        return outcome;  // rejected
      }
    }
    outcome.ran = true;
    outcome.threw = true;
    outcome.error = what;
  } catch (...) {
    outcome.ran = true;
    outcome.threw = true;
    outcome.error = "unknown exception";
  }
  return outcome;
}

// Environment switch for oracle (2). Returns the previous state so nested
// callers (replay inside fuzz try) restore correctly.
bool SetBytecodeDisabled(bool disabled) {
  const bool was = std::getenv("TINYLAMB_DISABLE_BYTECODE") != nullptr;
  if (disabled) {
    setenv("TINYLAMB_DISABLE_BYTECODE", "1", 1);
  } else {
    unsetenv("TINYLAMB_DISABLE_BYTECODE");
  }
  return was;
}

struct ScopedBytecodeDisabled {
  explicit ScopedBytecodeDisabled(bool disabled) {
    previous_ = SetBytecodeDisabled(disabled);
  }
  ~ScopedBytecodeDisabled() { SetBytecodeDisabled(previous_); }
  ScopedBytecodeDisabled(const ScopedBytecodeDisabled&) = delete;
  ScopedBytecodeDisabled& operator=(const ScopedBytecodeDisabled&) = delete;
  ScopedBytecodeDisabled(ScopedBytecodeDisabled&&) = delete;
  ScopedBytecodeDisabled& operator=(ScopedBytecodeDisabled&&) = delete;

 private:
  bool previous_;
};

// Oracle (2): re-runs `sql` with bytecode disabled and compares the AST-path
// result against `first`, produced on the bytecode/JIT fast path. Returns ""
// on agreement, otherwise a diagnostic naming the diverging path.
std::string CheckEvaluationPaths(Database& db, TransactionContext& ctx,
                                 const std::string& sql,
                                 const RunOutcome& first) {
  ScopedBytecodeDisabled guard(true);
  const RunOutcome again = RunStatement(db, ctx, sql);
  if (again.threw) {
    return "[GRIFFIN-THROW-AST] " + sql + "\n  what: " + again.error + "\n" +
           "  bytecode path: " + std::to_string(first.rows.size()) + " rows\n";
  }
  if (again.ran != first.ran) {
    return "[GRIFFIN-PATH MISMATCH] " + sql +
           "\n  bytecode ran=" + (first.ran ? "true" : "false") +
           " ast ran=" + (again.ran ? "true" : "false") + "\n";
  }
  if (again.ran && again.rows != first.rows) {
    std::string report = "[GRIFFIN-RESULT MISMATCH] " + sql + "\n";
    for (const std::string& row : first.rows) {
      report += "  bytecode| " + row + "\n";
    }
    for (const std::string& row : again.rows) {
      report += "  ast     | " + row + "\n";
    }
    return report;
  }
  return "";
}

// Session driver shared by iteration and replay: executes the statements in
// order under oracle (1), and every accepted SELECT immediately under
// oracle (2), so both evaluations observe identical database state. When
// `mutate` is set it rewrites each non-DDL statement before execution
// (metadata-guided substitution during iteration; identity for replay).
std::string RunSession(
    Database& db, TransactionContext& ctx,
    const std::vector<std::string>& statements, GriffinTrace* t,
    const std::function<std::string(const std::string&)>& mutate) {
  for (const std::string& original : statements) {
    const std::string sql =
        (mutate != nullptr && !IsDdl(original)) ? mutate(original) : original;
    t->statements.push_back(sql);
    const RunOutcome first = RunStatement(db, ctx, sql);
    if (first.threw) {
      // Oracle (1): an escaped exception is a crash-adjacent finding; under
      // ASAN/-UNDEBUG builds genuine memory/assert bugs abort before this.
      t->failure = "exception escaped execution";
      return "[GRIFFIN-THROW] " + sql + "\n  what: " + first.error + "\n";
    }
    if (IsSelect(sql) && first.ran) {
      t->ran_select = true;
      const std::string mismatch = CheckEvaluationPaths(db, ctx, sql, first);
      if (!mismatch.empty()) {
        t->failure = "evaluation-path mismatch";
        return mismatch;
      }
    }
  }
  return "";
}

}  // namespace

std::string RunGriffinIteration(std::mt19937& rng, bool verbose,
                                GriffinTrace* trace) {
  GriffinTrace local;
  GriffinTrace& t = (trace != nullptr) ? *trace : local;

  // 1. Reshuffle: merge two random seed cases and shuffle their statements.
  const auto& pool = SeedPool();
  std::uniform_int_distribution<size_t> pick_case(0, pool.size() - 1);
  std::vector<std::string> statements;
  for (size_t side = 0; side < 2; ++side) {
    const std::vector<std::string>& src = pool[pick_case(rng)];
    statements.insert(statements.end(), src.begin(), src.end());
  }
  std::shuffle(statements.begin(), statements.end(), rng);

  auto db_holder =
      Database::Create("griffin_fuzz-" + RandomString(8)).MoveValue();
  CHECK(db_holder != nullptr);
  Database& db = *db_holder;
  TransactionContext ctx = db.BeginContext();

  // 2. Metadata-guided substitution: the snapshot refreshes per statement, so
  // substitutions target the tables that exist at that point in the session.
  const auto substitute = [&db, &ctx,
                           &rng](const std::string& sql) -> std::string {
    const MetadataSnapshot meta = SnapshotMetadata(db, ctx);
    return SubstituteIdentifiers(sql, meta, rng);
  };
  const std::string report = RunSession(db, ctx, statements, &t, substitute);

  if (verbose && report.empty()) {
    std::cerr << "[griffin_fuzz][ok] " << t.statements.size()
              << " statements checked\n";
  }
  return report;
}

std::string ReplayGriffinTrace(const GriffinTrace& trace, bool verbose) {
  // Regenerate from the seed: the built-in pool makes the iteration fully
  // deterministic, so a tampered statement list must not match the seed.
  const auto seed32 = static_cast<uint32_t>(trace.seed);
  GriffinTrace regenerated;
  std::mt19937 rng(seed32);
  RunGriffinIteration(rng, false, &regenerated);
  if (regenerated.statements != trace.statements) {
    return "tampered trace: statements do not match seed " +
           std::to_string(trace.seed);
  }

  // Re-run the recorded session verbatim (identity mutation) and report
  // whether the failure still reproduces. "" means fixed or flaky-clean.
  auto db_holder =
      Database::Create("griffin_replay-" + RandomString(8)).MoveValue();
  CHECK(db_holder != nullptr);
  Database& db = *db_holder;
  TransactionContext ctx = db.BeginContext();
  GriffinTrace fresh;
  fresh.seed = trace.seed;
  const std::string report =
      RunSession(db, ctx, trace.statements, &fresh, nullptr);
  if (verbose && report.empty()) {
    std::cerr << "[griffin_fuzz][replay-clean] seed=" << trace.seed << "\n";
  }
  return report;
}

std::string SerializeGriffinTest(const GriffinTrace& trace) {
  std::string out = std::string(kTestHeader) + "\n";
  out += "-- seed: " + std::to_string(trace.seed) + "\n";
  for (const std::string& sql : trace.statements) {
    out += "-- stmt: " + sql + "\n";
  }
  out += std::string("-- ran_select: ") +
         (trace.ran_select ? "true" : "false") + "\n";
  if (!trace.failure.empty()) {
    out += "-- failure: " + trace.failure + "\n";
  }
  return out;
}

bool ParseGriffinTest(std::string_view text, GriffinTrace* trace) {
  *trace = GriffinTrace{};
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
    if (line.starts_with("-- tinylamb-griffin-test")) {
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
      trace->seed = std::stoull(value);
    } else if (consume("-- stmt: ", &value)) {
      trace->statements.push_back(value);
    } else if (consume("-- ran_select: ", &value)) {
      if (value != "true" && value != "false") {
        return false;
      }
      trace->ran_select = (value == "true");
    } else if (consume("-- failure: ", &value)) {
      trace->failure = value;
    } else {
      return false;  // unknown line: refuse a half-understood file
    }
  }
  return saw_header && !trace->statements.empty();
}

void GriffinFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  const auto seed32 = static_cast<uint32_t>(seed ^ (seed >> 32));
  std::mt19937 rng(seed32);
  GriffinTrace trace;
  trace.seed = (static_cast<uint64_t>(seed32) << 32) | seed32;
  std::mt19937 run_rng(seed32);
  std::string report = RunGriffinIteration(run_rng, verbose, &trace);
  if (report.empty()) {
    return;
  }

  // Prove the emitted file reproduces before saving it.
  const std::string text = SerializeGriffinTest(trace);
  GriffinTrace parsed;
  std::string verify;
  if (!ParseGriffinTest(text, &parsed)) {
    verify = "internal error: serialized test does not parse";
  } else if (!(parsed == trace)) {
    verify = "internal error: serialized test does not round-trip";
  } else if (ReplayGriffinTrace(parsed, false).empty()) {
    verify = "internal error: serialized test replay holds (flaky mismatch)";
  }

  const std::string path =
      "griffin_fuzz-repro-" + std::to_string(trace.seed) + ".test";
  std::ofstream file(path);
  file << text;
  file.close();

  std::cerr << "[griffin_fuzz] seed=" << trace.seed << "\n"
            << report << "regression test written to " << path
            << (verify.empty() ? "" : "\n" + verify) << "\n";
  abort();
}

}  // namespace tinylamb
