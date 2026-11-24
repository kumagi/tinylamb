/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/test_util.hpp"
#include "query/query_test.hpp"

namespace tinylamb {

// Column DEFAULT clauses: the constant rides the column's reserved
// Constraint::kDefault slot, the INSERT executor substitutes it for columns
// omitted from the target list, and the DEFAULT keyword in VALUES resolves
// through the same catalog lookup.
TEST_F(QueryTest, ColumnDefaultSubstitutesForOmittedColumns) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE td (k INT64, x INT64 DEFAULT 5, "
         "s STRING DEFAULT 'hello', d DATE DEFAULT DATE '2024-01-01');");
  RunSql(ctx, *db_, "INSERT INTO td (k) VALUES (1), (2);");
  const auto rows = RunSql(ctx, *db_, "SELECT k, x, s, d FROM td ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{5}));
  EXPECT_EQ(rows[0][2], Value("hello"));
  EXPECT_EQ(rows[0][3], Value::Date("2024-01-01"));
  // Explicit values still win, in every position.
  RunSql(ctx, *db_,
         "INSERT INTO td VALUES (3, 99, 'override', DATE '2024-12-31');");
  const auto overridden = RunSql(ctx, *db_, "SELECT x, s FROM td WHERE k = 3;");
  ASSERT_EQ(overridden.size(), 1U);
  EXPECT_EQ(overridden[0][0], Value(int64_t{99}));
  EXPECT_EQ(overridden[0][1], Value("override"));
}

// NOT NULL DDL contract: the frontend attaches the parsed
// NotNullColumnAttribute to the catalog column (it used to be silently
// dropped), the table writer enforces it for INSERT and UPDATE alike
// (Table::CheckRowConstraints)--and because every writer funnels through
// that check, the planner may treat the guarantee as grounded and rewrite
// COUNT over a NOT NULL column into the argument-free COUNT(*): the per-row
// null test can never reject a row, so the scan stops decoding the
// argument. COUNT(DISTINCT col) keeps its argument--the dedup set is part
// of the result definition, not a null filter. A PRIMARY KEY column needs
// no extra spelling: key constraints are already enforced non-NULL.
TEST_F(QueryTest, NotNullColumnEnforcesInsertAndUpdateAndRewritesCount) {
  TransactionContext seed = db_->BeginContext();
  RunSql(seed, *db_, "CREATE TABLE nn (a INT64 NOT NULL, b INT64, g INT64);");
  RunSql(seed, *db_, "CREATE TABLE pk (k INT64 PRIMARY KEY, v INT64);");
  RunSql(seed, *db_,
         "INSERT INTO nn VALUES (1, NULL, 1), (2, 4, 1), (3, 9, 2);");
  RunSql(seed, *db_, "INSERT INTO pk VALUES (1, 10), (2, 20), (3, 30);");
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  // Writer enforcement, INSERT and UPDATE alike.  A violating statement
  // turns out to a failed QueryResult: the DML applies rows off the
  // statement drain, so the rejection surfaces in the drain status.
  const auto expect_rejection = [&](std::string_view sql) {
    TransactionContext probe = db_->BeginContext();
    SqlEngine probe_engine(*db_);
    StatusOr<QueryResult> result = probe_engine.Execute(probe, sql);
    ASSERT_TRUE(result.HasValue()) << sql;
    if (result.HasValue()) {
      QueryResult drained = result.MoveValue();
      Row row;
      while (drained.Next(&row)) {
      }
      EXPECT_NE(drained.GetStatus(), Status::kSuccess) << sql;
      // Table::CheckRowConstraints' own message surfaces for INSERT;
      // the UPDATE wrapper reports a generic DML failure with the same
      // underlying rejection.
      const std::string message =
          drained.GetStatus().GetMessage() + " " + probe_engine.LastError();
      const bool non_nullable_message =
          message.find("non-nullable") != std::string::npos ||
          message.find("update failed on table") != std::string::npos;
      EXPECT_TRUE(non_nullable_message) << message;
    }
    ASSERT_SUCCESS(probe.txn_.Abort());
  };
  expect_rejection("INSERT INTO nn VALUES (NULL, 1, 0);");
  expect_rejection("UPDATE nn SET a = NULL WHERE a = 1;");
  expect_rejection("INSERT INTO pk VALUES (NULL, 1);");
  // The writes were rejected as a whole: the surviving state is intact.
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*) FROM nn;"),
            (std::vector<Row>{Row({Value(int64_t{3})})}));

  // Planner rewrite: argument-free plan for NOT NULL arguments.
  const auto explain = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    std::string text;
    text.reserve(8192);
    Row row;
    while (prepared.HasValue() && prepared.Value()->Next(&row, nullptr)) {
      text += row[0].AsString();
    }
    return text;
  };
  const std::string not_null_plan = explain("EXPLAIN SELECT COUNT(a) FROM nn;");
  EXPECT_NE(not_null_plan.find(": COUNT(*)"), std::string::npos)
      << not_null_plan;
  EXPECT_EQ(not_null_plan.find("COUNT(nn."), std::string::npos)
      << not_null_plan;
  // The decode win: the scan projection under the aggregate lost its only
  // column.
  EXPECT_NE(not_null_plan.find("Projection: []"), std::string::npos)
      << not_null_plan;
  const std::string primary_key_plan =
      explain("EXPLAIN SELECT COUNT(k) FROM pk;");
  EXPECT_NE(primary_key_plan.find(": COUNT(*)"), std::string::npos)
      << primary_key_plan;
  // Nullable and DISTINCT arguments keep their spelling.
  const std::string nullable_plan = explain("EXPLAIN SELECT COUNT(b) FROM nn;");
  EXPECT_NE(nullable_plan.find(": COUNT(nn.b)"), std::string::npos)
      << nullable_plan;
  const std::string distinct_plan =
      explain("EXPLAIN SELECT COUNT(DISTINCT a) FROM nn;");
  EXPECT_NE(distinct_plan.find(": COUNT("), std::string::npos) << distinct_plan;

  // Values are identical under the rewrite: full table, filtered, empty,
  // and grouped shapes.  The grouped shape is a value-only check: a plain
  // GROUP BY key next to aggregates routes through the relational bridge,
  // which does not apply the rewrite (the memo path covers the
  // all-aggregate select lists the rewrite fires on).
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*), COUNT(a), COUNT(b) FROM nn;"),
            (std::vector<Row>{Row(
                {Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{2})})}));
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(a) FROM nn WHERE a > 2;"),
            (std::vector<Row>{Row({Value(int64_t{1})})}));
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(a) FROM nn WHERE a > 100;"),
            (std::vector<Row>{Row({Value(int64_t{0})})}));
  // SUM keeps its argument and its empty-input semantics: over an empty
  // prefix SUM stays NULL -- a zero identity only applies inside a formed
  // group, so the argument-free COUNT rewrite never extends to SUM.
  const auto empty_sum =
      RunSql(ctx, *db_, "SELECT SUM(b) FROM nn WHERE a > 100;");
  ASSERT_EQ(empty_sum.size(), 1U);
  EXPECT_TRUE(empty_sum[0][0].IsNull());
  EXPECT_EQ(
      RunSql(ctx, *db_, "SELECT k, COUNT(k) FROM pk GROUP BY k ORDER BY k;"),
      (std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{1})}),
                        Row({Value(int64_t{2}), Value(int64_t{1})}),
                        Row({Value(int64_t{3}), Value(int64_t{1})})}));
  EXPECT_EQ(
      RunSql(ctx, *db_, "SELECT g, COUNT(a) FROM nn GROUP BY g ORDER BY g;"),
      (std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{2})}),
                        Row({Value(int64_t{2}), Value(int64_t{1})})}));
  ASSERT_SUCCESS(ctx.txn_.Abort());
}

// Error-absorbing calls (ISERROR / NULLIFERROR) in a grouped HAVING must be
// evaluated per group, per row: `A + 1/A` raises only for the A == 0 group,
// ISERROR keeps it and NULLIFERROR's absorbed-NULL comparison rejects it.
// The query used to lose whole groups when the scan-filter compile boundary
// "decided" the column-bearing conjunct against the result of an
// empty-schema evaluation, so these statements are pinned at the SQL level.
// The compile-level counterpart is ScanFilterTest's
// ConstantResidualBoundaryNeverDecidesColumnConjuncts test.
TEST_F(QueryTest, GroupedHavingErrorAbsorptionStaysPerRow) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE T AS SELECT 0 AS A, 0 AS B, 0.0 AS C UNION ALL "
         "SELECT 1, 10, 100.0 UNION ALL SELECT 2, 20, 200.0;");
  ASSERT_SUCCESS(ctx.PreCommit());

  TransactionContext probe = db_->BeginContext();
  // Group order is unspecified: compare as sorted multisets.
  const auto sorted = [](std::vector<Row> rows) {
    std::sort(rows.begin(), rows.end(), [](const Row& l, const Row& r) {
      return l[0].AsString() < r[0].AsString();
    });
    return rows;
  };

  // ISERROR(A + 1/A): true only for the A == 0 group; the other two groups
  // must be filtered by a per-group FALSE, not silently decided away.
  EXPECT_EQ(sorted(RunSql(probe, *db_,
                          "SELECT MAX(B) FROM T GROUP BY A "
                          "HAVING ISERROR(A + 1/A);")),
            (std::vector<Row>{Row({Value(int64_t{0})})}));
  // NULLIFERROR(A + 1/A) >= 0: the raising group absorbs to NULL and is
  // rejected; the clean groups (2 >= 0, 2.5 >= 0) survive.
  EXPECT_EQ(
      sorted(RunSql(probe, *db_,
                    "SELECT MAX(B) FROM T GROUP BY A "
                    "HAVING NULLIFERROR(A + 1/A) >= 0;")),
      (std::vector<Row>{Row({Value(int64_t{10})}), Row({Value(int64_t{20})})}));
  // The identical expressions behave the same in grouping-key position.
  EXPECT_EQ(
      sorted(
          RunSql(probe, *db_, "SELECT COUNT(B) FROM T GROUP BY ISERROR(1/A);")),
      (std::vector<Row>{Row({Value(int64_t{1})}), Row({Value(int64_t{2})})}));
  EXPECT_EQ(
      sorted(RunSql(probe, *db_,
                    "SELECT MAX(B) FROM T GROUP BY NULLIFERROR(1/A);")),
      (std::vector<Row>{Row({Value(int64_t{0})}), Row({Value(int64_t{10})}),
                        Row({Value(int64_t{20})})}));
  // The same shapes with the transpose available (key-only conjunct) and
  // through the wrapped-operand spelling stay correct as well: lifting the
  // conjunct into the scan filter must not change its verdict per group.
  EXPECT_EQ(
      sorted(RunSql(probe, *db_,
                    "SELECT MAX(B) FROM T GROUP BY A HAVING "
                    "IFERROR(1/A, -999) > -100;")),
      (std::vector<Row>{Row({Value(int64_t{10})}), Row({Value(int64_t{20})})}));
  ASSERT_SUCCESS(probe.txn_.Abort());
}

TEST_F(QueryTest, ColumnDefaultKeywordInValuesResolvesTargetColumn) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE tk (a INT64, b INT64 DEFAULT 42);");
  // Column-list form: DEFAULT maps through the target column list.
  RunSql(ctx, *db_, "INSERT INTO tk (b, a) VALUES (DEFAULT, 7);");
  const auto listed = RunSql(ctx, *db_, "SELECT a, b FROM tk;");
  ASSERT_EQ(listed.size(), 1U);
  EXPECT_EQ(listed[0][0], Value(int64_t{7}));
  EXPECT_EQ(listed[0][1], Value(int64_t{42}));
  // Positional form: DEFAULT resolves against the column at its own index.
  RunSql(ctx, *db_, "INSERT INTO tk VALUES (8, DEFAULT), (9, 1);");
  const auto positional = RunSql(ctx, *db_, "SELECT a, b FROM tk ORDER BY a;");
  ASSERT_EQ(positional.size(), 3U);
  EXPECT_EQ(positional[1][1], Value(int64_t{42}));
  // A column without a declared default has the implicit default NULL
  // (BigQuery semantics, pinned by the dml_* compliance cases).
  RunSql(ctx, *db_, "INSERT INTO tk VALUES (DEFAULT, 2);");
  const auto undeclared =
      RunSql(ctx, *db_, "SELECT a, b FROM tk WHERE a IS NULL;");
  ASSERT_EQ(undeclared.size(), 1U);
  EXPECT_EQ(undeclared[0][1], Value(int64_t{2}));
}

TEST_F(QueryTest, InsertValidatesWidthBeforeEvaluatingValues) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE WidthCheck(a INT64 DEFAULT 7)");
  SqlEngine engine(*db_);
  // Use well-formed values first: the only possible failure is the width
  // check, so the diagnostic cannot be produced by expression evaluation.
  for (const auto& [sql, message] :
       {std::pair{"INSERT INTO WidthCheck VALUES (1, 2)",
                  "value count does not match"},
        std::pair{"INSERT INTO WidthCheck(a) VALUES (1, 2)",
                  "column/value count mismatch"}}) {
    const auto result = engine.Prepare(ctx, sql);
    EXPECT_EQ(result.GetStatus(), Status::kInvalidArgument);
    EXPECT_NE(engine.LastError().find(message), std::string::npos);
  }
  // The poisoned case pins the ordering: the width error wins over 1/0.
  const auto poisoned =
      engine.Prepare(ctx, "INSERT INTO WidthCheck VALUES (1, 1/0)");
  EXPECT_EQ(poisoned.GetStatus(), Status::kInvalidArgument);
  EXPECT_NE(engine.LastError().find("value count does not match"),
            std::string::npos);
  // A single scalar is not a whole-row STRUCT: it must not be accepted by
  // the allow_struct=true early check for a multi-column table.
  RunSql(ctx, *db_, "CREATE TABLE WidthCheckPair(a INT64, b INT64);");
  const auto scalar =
      engine.Prepare(ctx, "INSERT INTO WidthCheckPair VALUES (1);");
  EXPECT_EQ(scalar.GetStatus(), Status::kInvalidArgument);
  EXPECT_NE(engine.LastError().find("value count does not match"),
            std::string::npos);
  RunSql(ctx, *db_, "INSERT INTO WidthCheck VALUES (DEFAULT)");
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT a FROM WidthCheck"),
            (std::vector<Row>{Row({Value(7)})}));
}

TEST_F(QueryTest, ColumnDefaultAppliesToInsertSelectAndPersists) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ts (a INT64, b INT64 DEFAULT 42);");
  RunSql(ctx, *db_, "INSERT INTO ts (a) SELECT 1;");
  const auto rows = RunSql(ctx, *db_, "SELECT a, b FROM ts;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][1], Value(int64_t{42}));
  // The constraint (with its payload) round-trips through the catalog, so
  // defaults keep applying after a full close/reopen.
  const std::string prefix = prefix_;
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  db_.reset();
  std::unique_ptr<Database> reopened = Database::Create(prefix).MoveValue();
  TransactionContext reopened_ctx = reopened->BeginContext();
  SqlEngine reopened_engine(*reopened);
  StatusOr<Executor> prepared =
      reopened_engine.Prepare(reopened_ctx, "INSERT INTO ts (a) VALUES (2);");
  ASSERT_TRUE(prepared.HasValue()) << reopened_engine.LastError();
  Row inserted;
  while (prepared.Value()->Next(&inserted, nullptr)) {
  }
  const auto after =
      RunSql(reopened_ctx, *reopened, "SELECT a, b FROM ts ORDER BY a;");
  ASSERT_EQ(after.size(), 2U);
  EXPECT_EQ(after[1][1], Value(int64_t{42}));
  ASSERT_EQ(reopened_ctx.txn_.PreCommit(), Status::kSuccess);
}

TEST_F(QueryTest, ColumnDefaultRejectsVolatileExpressions) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  StatusOr<Executor> rejected =
      engine.Prepare(ctx, "CREATE TABLE tv (a INT64 DEFAULT (RAND() * 10));");
  EXPECT_FALSE(rejected.HasValue());
  EXPECT_EQ(engine.LastError(), "column DEFAULT must be a constant expression");
  ASSERT_SUCCESS(ctx.txn_.Abort());
}

// Persisted views: CREATE VIEW stores the defining query in the catalog
// (re-seeded into the session registry on demand), DROP VIEW removes it, and
// OR REPLACE overwrites.  Expansion happens at FROM-reference time, so the
// view tracks its base table without a stored row image.
TEST_F(QueryTest, ViewPersistsAcrossRestartAndDropsCleanly) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE base (k INT64, v STRING);");
  RunSql(ctx, *db_, "INSERT INTO base VALUES (1, 'a'), (2, 'b');");
  RunSql(ctx, *db_, "CREATE VIEW vv AS SELECT k, v FROM base WHERE k > 1;");
  const auto immediate = RunSql(ctx, *db_, "SELECT * FROM vv;");
  ASSERT_EQ(immediate.size(), 1U);
  EXPECT_EQ(immediate[0][0], Value(int64_t{2}));

  const std::string prefix = prefix_;
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  db_.reset();
  std::unique_ptr<Database> reopened = Database::Create(prefix).MoveValue();
  TransactionContext reopened_ctx = reopened->BeginContext();
  const auto after =
      RunSql(reopened_ctx, *reopened, "SELECT * FROM vv ORDER BY k;");
  ASSERT_EQ(after.size(), 1U);
  EXPECT_EQ(after[0][0], Value(int64_t{2}));
  EXPECT_EQ(after[0][1], Value("b"));

  // DROP VIEW removes the definition durably; the same-transaction SELECT
  // fails and the abort rolls the drop back, so the view survives.
  SqlEngine dropping(*reopened);
  StatusOr<Executor> dropped = dropping.Prepare(reopened_ctx, "DROP VIEW vv; ");
  ASSERT_TRUE(dropped.HasValue()) << dropping.LastError();
  Row drop_row;
  ASSERT_TRUE(dropped.Value()->Next(&drop_row, nullptr))
      << dropped.Value()->GetStatus();
  ASSERT_GT(drop_row.Count(), 0U);
  EXPECT_EQ(drop_row[0], Value("DROP VIEW"));
  EXPECT_FALSE(dropped.Value()->Next(&drop_row, nullptr));
  EXPECT_EQ(dropped.Value()->GetStatus(), Status::kSuccess);
  StatusOr<Executor> gone = dropping.Prepare(reopened_ctx, "SELECT * FROM vv;");
  EXPECT_FALSE(gone.HasValue());
  ASSERT_SUCCESS(reopened_ctx.txn_.Abort());
  db_ = std::move(reopened);
  TransactionContext verify_ctx = db_->BeginContext();
  const auto rolled_back = RunSql(verify_ctx, *db_, "SELECT COUNT(*) FROM vv;");
  ASSERT_EQ(rolled_back.size(), 1U);
  EXPECT_EQ(rolled_back[0][0], Value(int64_t{1}));

  // A durable drop removes the view for later sessions too.
  RunSql(verify_ctx, *db_, "DROP VIEW vv;");
  ASSERT_EQ(verify_ctx.txn_.PreCommit(), Status::kSuccess);
  db_.reset();
  std::unique_ptr<Database> reopened2 = Database::Create(prefix).MoveValue();
  TransactionContext verify2 = reopened2->BeginContext();
  SqlEngine engine2(*reopened2);
  StatusOr<Executor> missing = engine2.Prepare(verify2, "SELECT * FROM vv;");
  EXPECT_FALSE(missing.HasValue());
  StatusOr<Executor> missing_drop = engine2.Prepare(verify2, "DROP VIEW vv;");
  EXPECT_FALSE(missing_drop.HasValue());
  EXPECT_EQ(engine2.LastError(), "view vv does not exist");
  ASSERT_SUCCESS(verify2.txn_.Abort());
  db_ = std::move(reopened2);
}

TEST_F(QueryTest, ViewOrReplaceAndNameConflicts) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE plain (x INT64);");
  RunSql(ctx, *db_, "CREATE VIEW v2 AS SELECT 1 AS one;");
  SqlEngine engine(*db_);
  StatusOr<Executor> duplicate =
      engine.Prepare(ctx, "CREATE VIEW v2 AS SELECT 2 AS two;");
  EXPECT_FALSE(duplicate.HasValue());
  EXPECT_EQ(engine.LastError(), "view v2 already exists");
  // Tables and materialized views block the name too.
  StatusOr<Executor> over_table =
      engine.Prepare(ctx, "CREATE VIEW plain AS SELECT 1;");
  EXPECT_FALSE(over_table.HasValue());
  EXPECT_EQ(engine.LastError(), "view plain already exists");
  // OR REPLACE overwrites the definition; a view over a view chains.
  RunSql(ctx, *db_, "CREATE OR REPLACE VIEW v2 AS SELECT 3 AS three;");
  RunSql(ctx, *db_, "CREATE VIEW v4 AS SELECT * FROM v2;");
  const auto chained = RunSql(ctx, *db_, "SELECT * FROM v4;");
  ASSERT_EQ(chained.size(), 1U);
  EXPECT_EQ(chained[0][0], Value(int64_t{3}));
  // Dropping a non-view name is a loud error.
  StatusOr<Executor> not_a_view = engine.Prepare(ctx, "DROP VIEW plain;");
  EXPECT_FALSE(not_a_view.HasValue());
  EXPECT_EQ(engine.LastError(), "view plain does not exist");
  ASSERT_SUCCESS(ctx.txn_.Abort());
}

TEST_F(QueryTest, DropIfExistsOnMissingObjects) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  const auto drop_ok = [&](const std::string& sql, const char* tag) {
    StatusOr<Executor> exec = engine.Prepare(ctx, sql);
    ASSERT_TRUE(exec.HasValue()) << sql << ": " << engine.LastError();
    Row row;
    ASSERT_TRUE(exec.Value()->Next(&row, nullptr))
        << sql << ": " << exec.Value()->GetStatus();
    ASSERT_GT(row.Count(), 0U) << sql;
    EXPECT_EQ(row[0], Value(tag));
    EXPECT_FALSE(exec.Value()->Next(&row, nullptr)) << sql;
    EXPECT_EQ(exec.Value()->GetStatus(), Status::kSuccess) << sql;
  };

  // Missing objects: IF EXISTS turns the not-exists error into a no-op.
  drop_ok("DROP TABLE IF EXISTS missing_t;", "DROP TABLE");
  drop_ok("DROP VIEW IF EXISTS missing_v;", "DROP VIEW");
  drop_ok("DROP MATERIALIZED VIEW IF EXISTS missing_mv;",
          "DROP MATERIALIZED VIEW");

  RunSql(ctx, *db_, "CREATE TABLE t_ifexist (x INT64);");
  RunSql(ctx, *db_, "CREATE VIEW v_ifexist AS SELECT 1 AS one;");
  // IF NOT EXISTS is a successful no-op for an already existing view.
  RunSql(ctx, *db_, "CREATE VIEW IF NOT EXISTS v_ifexist AS SELECT 2 AS one;");
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT one FROM v_ifexist")[0][0],
            Value(int64_t{1}));
  RunSql(ctx, *db_, "CREATE TABLE mv_base (k INT64);");
  RunSql(ctx, *db_,
         "CREATE MATERIALIZED VIEW mv_ifexist AS SELECT k FROM mv_base;");
  drop_ok("DROP TABLE IF EXISTS t_ifexist;", "DROP TABLE");
  drop_ok("DROP VIEW IF EXISTS v_ifexist;", "DROP VIEW");
  drop_ok("DROP MATERIALIZED VIEW IF EXISTS mv_ifexist;",
          "DROP MATERIALIZED VIEW");
  // Repeating the drops is still a successful no-op.
  drop_ok("DROP TABLE IF EXISTS t_ifexist;", "DROP TABLE");
  drop_ok("DROP VIEW IF EXISTS v_ifexist;", "DROP VIEW");
  drop_ok("DROP MATERIALIZED VIEW IF EXISTS mv_ifexist;",
          "DROP MATERIALIZED VIEW");

  // Without IF EXISTS the same drops stay loud errors.
  EXPECT_FALSE(engine.Prepare(ctx, "DROP TABLE t_ifexist;").HasValue());
  EXPECT_FALSE(engine.Prepare(ctx, "DROP VIEW v_ifexist;").HasValue());
  EXPECT_FALSE(
      engine.Prepare(ctx, "DROP MATERIALIZED VIEW mv_ifexist;").HasValue());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

// Sibling derived-aggregate fusion (TPC-H Q21 shape): two derived tables
// over one base table sharing the GROUP BY key fold into a single
// aggregation.  Pins the row multisets, the fusion counter, the group-
// membership witness (a key whose only rows fail the sibling WHERE must not
// surface through the join), the HAVING lift, and the ORDER BY output-name
// mapping; bare member references keep the statement unfused and correct.
TEST_F(QueryTest, SiblingDerivedAggregatesFuseIntoSingleScan) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE fs (k INT64, s INT64, v FLOAT64);");
  RunSql(ctx, *db_,
         "INSERT INTO fs VALUES (1, 10, 1.0), (1, 20, 2.0), (1, 10, 3.0), "
         "(2, 30, 0.5), (3, 40, 5.0), (3, 41, 0.8), (4, 50, 0.2);");

  SqlEngine engine(*db_);
  const auto run = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
    std::vector<Row> rows;
    rows.reserve(1024);
    if (!prepared.HasValue()) {
      return rows;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return rows;
  };

  const std::string q21_shape =
      "SELECT m.k, m.ds, o.k2, o.late_ds FROM "
      "(SELECT k, COUNT(DISTINCT s) AS ds FROM fs GROUP BY k) m, "
      "(SELECT k AS k2, COUNT(DISTINCT s) AS late_ds FROM fs "
      "WHERE v > 1.0 GROUP BY k) o "
      "WHERE m.k = o.k2 ORDER BY o.k2;";
  const std::vector<Row> fused = run(q21_shape);
  EXPECT_EQ(engine.LastFusedSiblingAggregates(), 1U);
  // k=2 and k=4 have no row with v > 1.0, so the `o` side has no group for
  // them and the inner join drops them entirely (the fused witness must not
  // resurrect them as late_ds=0 rows).
  ASSERT_EQ(fused.size(), 2U);
  EXPECT_EQ(fused[0][0].value.int_value, 1);
  EXPECT_EQ(fused[0][1].value.int_value, 2);
  EXPECT_EQ(fused[0][2].value.int_value, 1);
  EXPECT_EQ(fused[0][3].value.int_value, 2);
  EXPECT_EQ(fused[1][0].value.int_value, 3);
  EXPECT_EQ(fused[1][1].value.int_value, 2);
  EXPECT_EQ(fused[1][2].value.int_value, 3);
  EXPECT_EQ(fused[1][3].value.int_value, 1);

  // HAVING-only aggregates lift to outer conjuncts over the fused columns.
  const std::vector<Row> having_lift =
      run("SELECT m.k FROM "
          "(SELECT k FROM fs GROUP BY k HAVING COUNT(DISTINCT s) > 1) m, "
          "(SELECT k AS k2 FROM fs WHERE v > 1.0 GROUP BY k "
          "HAVING COUNT(DISTINCT s) = 1) o "
          "WHERE m.k = o.k2 ORDER BY m.k;");
  EXPECT_EQ(engine.LastFusedSiblingAggregates(), 1U);
  ASSERT_EQ(having_lift.size(), 1U);
  EXPECT_EQ(having_lift[0][0].value.int_value, 3);

  // Conditional SUM/AVG keep value semantics (NULL-skipping makes the CASE
  // wrap exact), and the witness conjunct guards membership.
  const std::vector<Row> conditional =
      run("SELECT m.k, m.tot, o.avg2 FROM "
          "(SELECT k, SUM(v) AS tot FROM fs GROUP BY k) m, "
          "(SELECT k AS k2, AVG(v) AS avg2 FROM fs WHERE v > 1.0 GROUP BY k) o "
          "WHERE m.k = o.k2 ORDER BY m.k;");
  EXPECT_EQ(engine.LastFusedSiblingAggregates(), 1U);
  ASSERT_EQ(conditional.size(), 2U);
  EXPECT_EQ(conditional[0][0].value.int_value, 1);
  EXPECT_DOUBLE_EQ(conditional[0][1].value.double_value, 6.0);
  EXPECT_DOUBLE_EQ(conditional[0][2].value.double_value, 2.5);
  EXPECT_EQ(conditional[1][0].value.int_value, 3);
  EXPECT_DOUBLE_EQ(conditional[1][1].value.double_value, 5.8);
  EXPECT_DOUBLE_EQ(conditional[1][2].value.double_value, 5.0);

  // A bare reference to a member output must not fuse (ambiguous binding
  // changes with the renamed fused columns) but must stay correct.
  const std::vector<Row> bare =
      run("SELECT m.k, m.ds, o.k2, o.late_ds FROM "
          "(SELECT k, COUNT(DISTINCT s) AS ds FROM fs GROUP BY k) m, "
          "(SELECT k AS k2, COUNT(DISTINCT s) AS late_ds FROM fs "
          "WHERE v > 1.0 GROUP BY k) o "
          "WHERE k = o.k2 ORDER BY o.k2;");
  EXPECT_EQ(engine.LastFusedSiblingAggregates(), 0U);
  ASSERT_EQ(bare.size(), fused.size());
  for (size_t i = 0; i < fused.size(); ++i) {
    EXPECT_EQ(bare[i], fused[i]);
  }
}

TEST_F(QueryTest, StoredQueryExpansionsPreserveSetOperationSource) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE stored_left(n INT64)");
  RunSql(ctx, *db_, "INSERT INTO stored_left VALUES (1),(2)");
  RunSql(ctx, *db_, "CREATE TABLE stored_right(n INT64)");
  RunSql(ctx, *db_, "INSERT INTO stored_right VALUES (2),(3)");
  const std::string body =
      "SELECT n FROM stored_left |> EXCEPT DISTINCT (SELECT n FROM "
      "stored_right)";
  RunSql(ctx, *db_, "CREATE VIEW stored_view AS " + body);
  RunSql(ctx, *db_, "CREATE TABLE FUNCTION stored_tvf() AS (" + body + ")");
  const std::vector<Row> expected{Row({Value(1)})};
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT * FROM stored_view"), expected);
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT * FROM stored_tvf()"), expected);
}

// CREATE TABLE FUNCTION bodies expand at FROM-reference time with the
// call-site arguments: constant arguments substitute into the stored query
// (multi-argument, WHERE, and alias forms), and unknown functions or
// non-constant arguments are explicit errors.
TEST_F(QueryTest, TableValuedFunctionInvokesStoredBody) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE td (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO td VALUES (1), (2), (3);");

  SqlEngine engine(*db_);
  const auto run = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
    std::vector<Row> rows;
    rows.reserve(1024);
    if (!prepared.HasValue()) {
      return rows;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return rows;
  };

  run("CREATE TABLE FUNCTION dbl(m INT64) AS SELECT m * k AS mk FROM td;");
  const std::vector<Row> doubled = run("SELECT mk FROM dbl(2) ORDER BY mk;");
  ASSERT_EQ(doubled.size(), 3U);
  EXPECT_EQ(doubled[0][0].value.int_value, 2);
  EXPECT_EQ(doubled[1][0].value.int_value, 4);
  EXPECT_EQ(doubled[2][0].value.int_value, 6);

  run("CREATE TABLE FUNCTION pick(lo INT64, hi INT64) AS "
      "SELECT k FROM td WHERE k >= lo AND k < hi;");
  const std::vector<Row> picked = run("SELECT k FROM pick(1, 3) ORDER BY k;");
  ASSERT_EQ(picked.size(), 2U);
  EXPECT_EQ(picked[0][0].value.int_value, 1);
  EXPECT_EQ(picked[1][0].value.int_value, 2);

  const std::vector<Row> aliased =
      run("SELECT d.mk FROM dbl(3) AS d ORDER BY d.mk;");
  ASSERT_EQ(aliased.size(), 3U);
  EXPECT_EQ(aliased[2][0].value.int_value, 9);

  EXPECT_FALSE(engine.Prepare(ctx, "SELECT * FROM no_such_tvf(1);").HasValue());
  EXPECT_FALSE(engine.Prepare(ctx, "SELECT * FROM dbl(k);").HasValue());
}

TEST_F(QueryTest, TableSampleExecutorSemantics) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE sample_t (id INT64, part INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO sample_t VALUES (1, 10), (2, 10), (3, 20), (4, 20), "
         "(5, 30);");

  SqlEngine engine(*db_);
  const auto run = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
    std::vector<Row> rows;
    rows.reserve(1024);
    if (!prepared.HasValue()) {
      return rows;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return rows;
  };

  const std::vector<Row> sampled =
      run("SELECT * FROM sample_t TABLESAMPLE RESERVOIR(2 ROWS) "
          "WITH WEIGHT ORDER BY id;");
  ASSERT_EQ(sampled.size(), 2U);
  EXPECT_EQ(sampled[0].values_.size(), 3U);
  EXPECT_DOUBLE_EQ(sampled[0][2].value.double_value, 2.5);
  EXPECT_DOUBLE_EQ(sampled[1][2].value.double_value, 2.5);

  const std::vector<Row> derived_count =
      run("SELECT COUNT(*) FROM (SELECT id, part FROM sample_t) d "
          "TABLESAMPLE RESERVOIR(0 ROWS);");
  ASSERT_EQ(derived_count.size(), 1U);
  EXPECT_EQ(derived_count[0][0].value.int_value, 0);

  const std::vector<Row> partition_count =
      run("SELECT part, COUNT(*) FROM (SELECT part FROM sample_t) d "
          "TABLESAMPLE RESERVOIR(1 ROWS PARTITION BY part) "
          "GROUP BY part ORDER BY part;");
  ASSERT_EQ(partition_count.size(), 3U);
  EXPECT_EQ(partition_count[0][0].value.int_value, 10);
  EXPECT_EQ(partition_count[1][0].value.int_value, 20);
  EXPECT_EQ(partition_count[2][0].value.int_value, 30);

  const std::string repeatable =
      "SELECT id FROM sample_t TABLESAMPLE RESERVOIR(2 ROWS) "
      "REPEATABLE(7) ORDER BY id;";
  const std::vector<Row> first_repeat = run(repeatable);
  const std::vector<Row> second_repeat = run(repeatable);
  ASSERT_EQ(first_repeat.size(), 2U);
  EXPECT_EQ(second_repeat.size(), first_repeat.size());
  for (size_t i = 0; i < first_repeat.size(); ++i) {
    EXPECT_EQ(second_repeat[i][0].value.int_value,
              first_repeat[i][0].value.int_value);
  }
}

// SELECT ... FOR UPDATE reuses the DML source contract (`require_row_position`
// -> scan `lock_rows`): every row the access path visits is write-intent locked
// before the WHERE is applied, exactly as UPDATE/DELETE lock their sources, so
// a full scan reserves the whole scanned table.  The intent lives until
// commit/abort and blocks another transaction's reservation of the same row.
// A plain read takes no intent, and the flag survives a template-cache re-bind.
TEST_F(QueryTest, SqlEngineForUpdateLocksScannedRows) {
  // The seeding transaction must finish first: an inserting transaction keeps
  // its write intents, and a locking scan would wait on them.
  TransactionContext seed = db_->BeginContext();
  RunSql(seed, *db_, "CREATE TABLE fu (k INT64, v INT64);");
  RunSql(seed, *db_, "INSERT INTO fu VALUES (1, 10), (2, 20), (3, 30);");
  ASSERT_SUCCESS(seed.PreCommit());

  TransactionContext ctx = db_->BeginContext();
  TransactionContext locker = db_->BeginContext();
  SqlEngine engine(*db_);
  const auto drain_for_update = [&](std::string_view sql) {
    std::vector<Row> rows;
    rows.reserve(1024);
    StatusOr<Executor> prepared = engine.Prepare(locker, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    if (!prepared.HasValue()) {
      return rows;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return rows;
  };
  std::vector<Row> rows =
      drain_for_update("SELECT k FROM fu WHERE k = 1 FOR UPDATE;");
  ASSERT_EQ(rows.size(), 1U);

  // Physical positions come from a plain read.
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(ctx, "fu"));
  std::unordered_map<int64_t, RowPosition> positions;
  for (Iterator it = table.BeginFullScan(ctx.txn_); it.IsValid(); ++it) {
    for (int64_t k = 1; k <= 3; ++k) {
      if ((*it).values_[0] == Value(k)) {
        positions[k] = it.Position();
      }
    }
  }
  ASSERT_EQ(positions.size(), 3U);
  // The reader above ran the same table without FOR UPDATE: no intents at all.
  EXPECT_FALSE(ctx.txn_.HasWriteSet(positions[1]));
  EXPECT_FALSE(ctx.txn_.HasWriteSet(positions[2]));
  EXPECT_FALSE(ctx.txn_.HasWriteSet(positions[3]));
  // The locking scan reserved the rows it visited, before the WHERE dropped
  // the non-matching ones (the DML source contract).
  EXPECT_TRUE(locker.txn_.HasWriteSet(positions[1]));
  EXPECT_TRUE(locker.txn_.HasWriteSet(positions[2]));
  EXPECT_TRUE(locker.txn_.HasWriteSet(positions[3]));

  // A second transaction cannot reserve a locked row.
  TransactionContext writer = db_->BeginContext();
  EXPECT_FALSE(writer.txn_.TryAddWriteSet(positions[1]));
  // Aborting the locker releases the row to the writer.
  ASSERT_SUCCESS(locker.txn_.Abort());
  EXPECT_TRUE(writer.txn_.TryAddWriteSet(positions[1]));
  EXPECT_EQ(tinylamb::Status::kSuccess, writer.txn_.Abort());

  // Re-running the same text locks again: a cached plan must not turn the
  // statement into an unlocked read.
  TransactionContext again = db_->BeginContext();
  for (int run = 0; run < 2; ++run) {
    StatusOr<Executor> prepared =
        engine.Prepare(again, "SELECT k FROM fu WHERE k = 1 FOR UPDATE;");
    ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_TRUE(again.txn_.HasWriteSet(positions[1]));
  }
  ASSERT_SUCCESS(again.txn_.Abort());
  ASSERT_SUCCESS(ctx.txn_.Abort());
}

// GoogleSQL has no NOWAIT, so the locking select's wait policy is an embedder
// knob.  kNoWait must fail the statement as soon as the scan meets a held row
// (try-lock, the policy DELETE's source scan uses) instead of parking for the
// engine's 5s wait cap.  The policy is part of the plan-cache key: filling the
// cache under kWait must not make a kNoWait engine replay the blocking plan.
TEST_F(QueryTest, SqlEngineForUpdateNoWaitFailsFast) {
  TransactionContext seed = db_->BeginContext();
  RunSql(seed, *db_, "CREATE TABLE fu_nowait (k INT64, v INT64);");
  RunSql(seed, *db_, "INSERT INTO fu_nowait VALUES (1, 10), (2, 20);");
  ASSERT_SUCCESS(seed.PreCommit());

  // kWait engine: this fills the compiled-plan cache for the text below while
  // it locks every scanned row.  The literal keeps the statement templatable,
  // so the compiled-plan cache is live for the repeat below -- that is the path
  // the policy key must guard.
  TransactionContext holder = db_->BeginContext();
  SqlEngine holder_engine(*db_);
  {
    StatusOr<Executor> held = holder_engine.Prepare(
        holder, "SELECT k FROM fu_nowait WHERE k >= 1 FOR UPDATE;");
    ASSERT_TRUE(held.HasValue()) << holder_engine.LastError();
    Row row;
    size_t rows = 0;
    while (held.Value()->Next(&row, nullptr)) {
      ++rows;
    }
    EXPECT_TRUE(held.Value()->ok()) << held.Value()->GetStatus().GetMessage();
    EXPECT_EQ(rows, 2U);
  }
  TransactionContext reader = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(reader, "fu_nowait"));
  RowPosition first;
  for (Iterator it = table.BeginFullScan(reader.txn_); it.IsValid(); ++it) {
    if ((*it).values_[0] == Value(1)) {
      first = it.Position();
    }
  }
  ASSERT_TRUE(first.IsValid());
  EXPECT_TRUE(holder.txn_.HasWriteSet(first));

  SqlEngine nowait_engine(*db_);
  nowait_engine.SetForUpdateWaitMode(SqlEngine::ForUpdateWaitMode::kNoWait);
  TransactionContext other = db_->BeginContext();
  const auto started = std::chrono::steady_clock::now();
  StatusOr<Executor> prepared = nowait_engine.Prepare(
      other, "SELECT k FROM fu_nowait WHERE k >= 1 FOR UPDATE;");
  ASSERT_TRUE(prepared.HasValue()) << nowait_engine.LastError();
  Row row;
  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  const double elapsed_ms = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - started)
                                .count();
  EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
  // A try-lock rejection is recorded as kConflict at once; a plan filled
  // under kWait would instead park until the write-intent wait cap (and
  // report kTimeout).  Assert the recorded reason rather than a tight wall
  // clock budget, which is flaky under a loaded parallel ctest run.
  EXPECT_EQ(other.txn_.LastIntentWaitFailure(),
            Transaction::IntentWaitFailure::kConflict);
  // Loose sanity bound only; the reason above is the real wait/no-wait check.
  EXPECT_LT(elapsed_ms, 10000.0) << elapsed_ms << "ms";
  ASSERT_SUCCESS(other.txn_.Abort());
  ASSERT_SUCCESS(holder.txn_.Abort());
  EXPECT_EQ(tinylamb::Status::kSuccess, reader.txn_.Abort());
}

// The wait mode must also be part of the cache key when FOR and UPDATE are
// separated by more than one space: a raw find("FOR UPDATE") misses that
// spelling, so a kNoWait engine could replay the kWait plan filled below and
// block on the conflicting row instead of failing fast.
TEST_F(QueryTest, SqlEngineForUpdateNoWaitWhitespaceVariantFailsFast) {
  TransactionContext seed = db_->BeginContext();
  RunSql(seed, *db_, "CREATE TABLE fu_nowait_ws (k INT64, v INT64);");
  RunSql(seed, *db_, "INSERT INTO fu_nowait_ws VALUES (1, 10), (2, 20);");
  ASSERT_SUCCESS(seed.PreCommit());

  const std::string sql =
      "SELECT k FROM fu_nowait_ws WHERE k >= 1 FOR  UPDATE;";
  TransactionContext holder = db_->BeginContext();
  SqlEngine holder_engine(*db_);
  {
    StatusOr<Executor> held = holder_engine.Prepare(holder, sql);
    ASSERT_TRUE(held.HasValue()) << holder_engine.LastError();
    Row row;
    size_t rows = 0;
    while (held.Value()->Next(&row, nullptr)) {
      ++rows;
    }
    EXPECT_TRUE(held.Value()->ok()) << held.Value()->GetStatus().GetMessage();
    EXPECT_EQ(rows, 2U);
  }
  TransactionContext reader = db_->BeginContext();
  Table table = db_->GetTable(reader, "fu_nowait_ws").Value();
  RowPosition first;
  for (Iterator it = table.BeginFullScan(reader.txn_); it.IsValid(); ++it) {
    if ((*it).values_[0] == Value(1)) {
      first = it.Position();
    }
  }
  ASSERT_TRUE(first.IsValid());
  EXPECT_TRUE(holder.txn_.HasWriteSet(first));

  SqlEngine nowait_engine(*db_);
  nowait_engine.SetForUpdateWaitMode(SqlEngine::ForUpdateWaitMode::kNoWait);
  TransactionContext other = db_->BeginContext();
  const auto started = std::chrono::steady_clock::now();
  StatusOr<Executor> prepared = nowait_engine.Prepare(other, sql);
  ASSERT_TRUE(prepared.HasValue()) << nowait_engine.LastError();
  Row row;
  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  const double elapsed_ms = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - started)
                                .count();
  EXPECT_NE(prepared.Value()->GetStatus(), Status::kSuccess);
  // Same reason-based check as the single-space spelling: the whitespace
  // variant must still take the try-lock path, not park for the wait cap.
  EXPECT_EQ(other.txn_.LastIntentWaitFailure(),
            Transaction::IntentWaitFailure::kConflict);
  EXPECT_LT(elapsed_ms, 10000.0) << elapsed_ms << "ms";
  ASSERT_SUCCESS(other.txn_.Abort());
  EXPECT_EQ(tinylamb::Status::kSuccess, holder.txn_.Abort());
  EXPECT_EQ(tinylamb::Status::kSuccess, reader.txn_.Abort());
}

// Calc merge policy: a filter that the scan already applies (the scan rule
// wraps the pushed group filter in one SelectionPlan) must not be evaluated a
// second time by the selection node that produced it.  The merge compares
// predicates up to relation qualifiers, so the aliased spelling merges too, and
// it never changes which rows survive.
TEST_F(QueryTest, SqlEngineCalcMergesDuplicateScanFilters) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE calc_t (a INT64, b INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO calc_t VALUES (1, 10), (2, 20), (3, 30), (4, NULL);");
  const auto explain = [&](std::string_view sql) {
    std::string text;
    text.reserve(8192);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    if (!prepared.HasValue()) {
      return text;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      text += row[0].AsString();
    }
    return text;
  };
  const auto filters = [](std::string_view text) {
    size_t count = 0;
    for (size_t at = text.find("Selection: ("); at != std::string_view::npos;
         at = text.find("Selection: (", at + 1)) {
      ++count;
    }
    return count;
  };

  const std::string plain =
      explain("EXPLAIN SELECT a + b AS c FROM calc_t WHERE a > 1;");
  EXPECT_EQ(filters(plain), 1U) << plain;
  const std::string aliased =
      explain("EXPLAIN SELECT x.a + x.b AS c FROM calc_t AS x WHERE x.a > 1;");
  EXPECT_EQ(filters(aliased), 1U) << aliased;
  // A conjunct that is not a simple scan filter still reaches the executor.
  const std::string residual =
      explain("EXPLAIN SELECT a + b AS c FROM calc_t WHERE a > 1 AND b > 15;");
  EXPECT_EQ(filters(residual), 1U) << residual;

  // Results are unchanged by the merge: a>1 keeps 2, 3 and 4, and the NULL
  // sum stays NULL.
  const std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT a + b AS c FROM calc_t WHERE a > 1;");
  ASSERT_EQ(rows.size(), 3U);
  size_t nulls = 0;
  for (const Row& row : rows) {
    if (row[0].type == ValueType::kNull) {
      ++nulls;
      continue;
    }
    EXPECT_TRUE(row[0] == Value(22) || row[0] == Value(33));
  }
  EXPECT_EQ(nulls, 1U);
  const std::vector<Row> filtered = RunSql(
      ctx, *db_, "SELECT a + b AS c FROM calc_t WHERE a > 1 AND b > 15;");
  ASSERT_EQ(filtered.size(), 2U);
  EXPECT_EQ(filtered[0][0], Value(20 + 2));
  EXPECT_EQ(filtered[1][0], Value(30 + 3));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

// The shapes FOR UPDATE cannot lock are refused with a message naming the
// supported shape, instead of returning rows that are not actually locked.
TEST_F(QueryTest, SqlEngineForUpdateRejectsUnlockableShapes) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE fu_bad (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO fu_bad VALUES (1, 10), (2, 20);");
  SqlEngine engine(*db_);
  for (const char* sql :
       {"SELECT COUNT(*) FROM fu_bad FOR UPDATE;",
        "SELECT k FROM fu_bad GROUP BY k FOR UPDATE;",
        "SELECT DISTINCT k FROM fu_bad FOR UPDATE;",
        "SELECT a.k FROM fu_bad a JOIN fu_bad b ON a.k = b.k FOR UPDATE;",
        "WITH c AS (SELECT k FROM fu_bad) SELECT k FROM c FOR UPDATE;"}) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_FALSE(prepared.HasValue()) << sql;
    EXPECT_NE(engine.LastError().find("FOR UPDATE"), std::string::npos)
        << sql << ": " << engine.LastError();
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

// GoogleSQL has no DELETE ... USING, so the join spelling is a correlated
// subquery (or an IN list).  Both must delete exactly the joined rows, and a
// target row matched by several source rows is deleted once -- unlike
// UPDATE ... FROM, whose shared MERGE machinery rejects more than one match.
TEST_F(QueryTest, SqlEngineDeleteJoinsThroughCorrelatedSubqueries) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE dt (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE du (k INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO dt VALUES (1, 10), (2, 20), (3, 30), (4, 40);");
  RunSql(ctx, *db_, "INSERT INTO du VALUES (1), (3), (3);");
  RunSql(ctx, *db_,
         "DELETE FROM dt WHERE EXISTS (SELECT 1 FROM du WHERE du.k = dt.k);");
  std::vector<Row> rows = RunSql(ctx, *db_, "SELECT k FROM dt ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[1][0], Value(4));

  // The IN spelling deletes the same set; du holds no NULL, so the
  // NULL-propagating IN rule cannot change it.
  RunSql(ctx, *db_, "INSERT INTO du VALUES (2);");
  RunSql(ctx, *db_, "DELETE FROM dt WHERE k IN (SELECT k FROM du);");
  rows = RunSql(ctx, *db_, "SELECT k FROM dt ORDER BY k;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(4));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

// Bug 48 regression: UPDATE/DELETE with a non-correlated `x IN (SELECT ...)`
// must actually apply.  Before DecorateDmlWhere the predicate fell into the
// scalar evaluator ("requires relational evaluation", masked as a sticky
// executor error) or lost row positions to a semi-join wrap.
// Bug 55 regression: ANY/ALL, non-correlated EXISTS and scalar subqueries
// in a DML WHERE must fold (and apply) instead of falling into the scalar
// evaluator's "requires relational evaluation" error; a WITH-bearing inner
// keeps the engine's documented unimplemented rejection.
// Bug 55 companion: the prepare-time subquery fold must reach the UPDATE
// SET values and INSERT VALUES, not only the WHERE clause.
TEST_F(QueryTest, DmlSetAndValuesFoldSubqueries) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE s (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO s VALUES (7);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  RunSql(ctx, *db_, "UPDATE t SET v = (SELECT MIN(k) FROM s);");
  const std::vector<Row> after_set =
      RunSql(ctx, *db_, "SELECT k, v FROM t ORDER BY k;");
  ASSERT_EQ(after_set.size(), 2U);
  EXPECT_EQ(after_set[0][1].value.int_value, 7);
  EXPECT_EQ(after_set[1][1].value.int_value, 7);

  RunSql(ctx, *db_, "UPDATE t SET v = v + (SELECT MAX(k) FROM s) WHERE k = 1;");
  const std::vector<Row> after_add =
      RunSql(ctx, *db_, "SELECT k, v FROM t ORDER BY k;");
  EXPECT_EQ(after_add[0][1].value.int_value, 14);

  RunSql(ctx, *db_,
         "UPDATE t SET v = CASE WHEN EXISTS (SELECT 1 FROM s) THEN 99 "
         "ELSE 1 END;");
  const std::vector<Row> after_case =
      RunSql(ctx, *db_, "SELECT k, v FROM t ORDER BY k;");
  EXPECT_EQ(after_case[0][1].value.int_value, 99);

  RunSql(ctx, *db_, "INSERT INTO t VALUES (3, (SELECT k FROM s));");
  const std::vector<Row> after_insert =
      RunSql(ctx, *db_, "SELECT k, v FROM t ORDER BY k;");
  ASSERT_EQ(after_insert.size(), 3U);
  EXPECT_EQ(after_insert[2][1].value.int_value, 7);
}

TEST_F(QueryTest, AggregateRewritesPreserveInnerLimitAndDistinctModifiers) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE agg_modifiers (v INT64, u INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO agg_modifiers VALUES (100, 1), (1, 2), (2, 3);");
  const auto array_rows =
      RunSql(ctx, *db_,
             "SELECT ARRAY_AGG(v ORDER BY u LIMIT 1) FROM agg_modifiers "
             "WHERE u > 0;");
  ASSERT_EQ(array_rows.size(), 1U);
  ASSERT_TRUE(array_rows[0][0].IsArray());
  ASSERT_EQ(array_rows[0][0].ArrayElements().size(), 1U);
  EXPECT_EQ(array_rows[0][0].ArrayElements()[0], Value(int64_t{100}));

  // ORDER BY / LIMIT is only meaningful for order-keeping aggregates;
  // COUNT with those modifiers is rejected (project decision, 2026-11-20).
  SqlEngine engine(*db_);
  EXPECT_FALSE(engine
                   .Prepare(ctx,
                            "SELECT COUNT(DISTINCT v ORDER BY u LIMIT 1) FROM "
                            "agg_modifiers WHERE u > 0;")
                   .HasValue());
  EXPECT_NE(engine.LastError().find("ORDER BY / LIMIT"), std::string::npos)
      << engine.LastError();
}

TEST_F(QueryTest, WindowFilterDoesNotConfuseQualifiedPartitionColumns) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE window_a (x INT64, v INT64, id INT64);");
  RunSql(ctx, *db_, "CREATE TABLE window_b (aid INT64, x INT64);");
  RunSql(ctx, *db_, "INSERT INTO window_a VALUES (1, 10, 1), (1, 20, 2);");
  RunSql(ctx, *db_, "INSERT INTO window_b VALUES (1, 1), (2, 2);");
  const auto rows = RunSql(ctx, *db_,
                           "SELECT a.x, SUM(a.v) OVER (PARTITION BY a.x) AS s "
                           "FROM window_a a JOIN window_b b ON a.id = b.aid "
                           "QUALIFY b.x = 1 ORDER BY a.x;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][1], Value(int64_t{30}));
}

// Bug 56 regression: non-equality quantified subqueries must keep their
// operator/mode through planning (no equality-semijoin lowering), in SELECT
// and DML alike, including the NOT-wrapped empty-subquery edge.
// Differential for the prepare-time quantifier fold (Bugs 48/55/57):
// the SAME DML must modify the same rows whether it goes through the
// Cascades/vectorized route (folds apply) or the forced-relational route
// (row-level quantified evaluator).  Crosses operators, ANY/ALL, NOT,
// NULL and empty inner tables.
// Bug 60 regression: the template cache must round-trip quantifier
// op/mode. Running the same DML text twice through one SqlEngine hits
// the template cache on the second Prepare; before the fix the
// `NOT ... < ALL` predicate degraded to IN membership and updated 0
// rows instead of 2.
TEST_F(QueryTest, TemplateCacheKeepsQuantifierMode) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t0 (u INT64, c1 INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t1 (u INT64);");
  RunSql(ctx, *db_, "INSERT INTO t1 VALUES (1), (2), (1);");
  RunSql(ctx, *db_, "INSERT INTO t0 VALUES (1, 2), (2, 9);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();
  const std::string sql =
      "UPDATE t0 SET c1 = -1 WHERE NOT u < ALL (SELECT u FROM t1);";
  const std::vector<Row> first = RunSql(ctx, *db_, sql);
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(first[0][1].value.int_value, 2);
  RunSql(ctx, *db_, "UPDATE t0 SET c1 = 5 WHERE u = 2;");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();
  const std::vector<Row> second = RunSql(ctx, *db_, sql);
  ASSERT_EQ(second.size(), 1U);
  // t1 is unchanged ({1,2,1}): neither row satisfies u < ALL(t1), so the
  // correct predicate is TRUE for both rows. The pre-fix template cache
  // degraded it to IN membership and matched 0 rows here.
  EXPECT_EQ(second[0][1].value.int_value, 2)
      << "second pass lost the quantifier via the template cache";
  const std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT u, c1 FROM t0 ORDER BY u;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][1].value.int_value, -1);
  EXPECT_EQ(rows[1][1].value.int_value, -1);
}

// Bug 69 regression: template rebinding must round-trip the ARRAY(SELECT ...)
// element-type annotation on QueryExpression.  Before the fix the second
// execution of the same SQL text rebound the cached plan with
// `array_element_sql_type_` dropped, so the runtime element-type inference
// emitted ARRAY<STRING> where the first (cached-tree) run emitted
// ARRAY<INT64>.  The lambda desugars to ARRAY(SELECT ... FROM UNNEST ...) and
// the `1` literal makes the statement templatable.
TEST_F(QueryTest, TemplateCacheKeepsArraySubqueryElementType) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, arr ARRAY<INT64>);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, [3, 1, 2]);");
  const std::string sql = "SELECT ARRAY_TRANSFORM(arr, e -> e || 'a') FROM t;";
  const std::vector<Row> first = RunSql(ctx, *db_, sql);
  ASSERT_EQ(first.size(), 1U);
  const std::vector<Row> second = RunSql(ctx, *db_, sql);
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0].ToString(), first[0].ToString())
      << "template-cache hit changed the declared array element type";
  // `e || 'a'` is string concatenation, so the declared tag is STRING.
  EXPECT_EQ(first[0][0].ArrayElementSqlType(), "STRING");
}

// The declared element type of a lambda-desugared array must match the
// values the lambda actually produces: InferArrayElementSqlType reports ""
// for bodies it cannot type statically, and both evaluators then infer the
// tag from the produced elements instead of defaulting to INT64.
TEST_F(QueryTest, LambdaArrayElementTypeFollowsProducedValues) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (arr ARRAY<STRING>);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (['a', 'bb']);");

  std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT ARRAY_TRANSFORM(arr, e -> e || '!') FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0].ArrayElementSqlType(), "STRING");

  // A predicate-producing body is tagged BOOL statically (runtime values
  // cannot distinguish BOOL from INT64).
  rows = RunSql(ctx, *db_, "SELECT ARRAY_TRANSFORM(arr, e -> e > 'a') FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0].ArrayElementSqlType(), "BOOL");

  // ARRAY_FILTER keeps the input's element type, not the predicate's BOOL.
  rows = RunSql(ctx, *db_, "SELECT ARRAY_FILTER(arr, e -> e > 'a') FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0].ArrayElementSqlType(), "STRING");
}

TEST_F(QueryTest, DmlQuantifiedFoldMatchesRelationalRoute) {
  static constexpr std::array<const char*, 18> kPreds = {
      "k = ANY (SELECT s FROM src)",
      "k != ANY (SELECT s FROM src)",
      "k > ANY (SELECT s FROM src)",
      "k <= ANY (SELECT s FROM src)",
      "k = ALL (SELECT s FROM src)",
      "k != ALL (SELECT s FROM src)",
      "k < ALL (SELECT s FROM src)",
      "k >= ALL (SELECT s FROM src)",
      "NOT (k = ANY (SELECT s FROM src))",
      "NOT (k != ALL (SELECT s FROM src))",
      "NOT (k > ALL (SELECT s FROM src))",
      "(k IS NOT NULL) AND (k <> ALL (SELECT s FROM src))",
      "(k IN (0, 2)) OR (k = ANY (SELECT s FROM src))",
      "EXISTS (SELECT s FROM src)",
      "NOT EXISTS (SELECT s FROM src)",
      "EXISTS (SELECT 1 FROM src WHERE src.s = tgt.k)",
      "k > (SELECT MIN(s) FROM src)",
      "(k = ANY (SELECT s FROM src)) AND (k != ALL (SELECT s FROM src))",
  };
  static constexpr std::array<std::array<const char*, 3>, 3> kSrcSets = {{
      {"CREATE TABLE src (s INT64);", "INSERT INTO src VALUES (0), (2);", ""},
      {"CREATE TABLE src (s INT64);",
       "INSERT INTO src VALUES (0), (NULL), (-2);", ""},
      {"CREATE TABLE src (s INT64);", "", ""},
  }};
  // One engine per database, kept alive across the whole case so the
  // force-relational flag actually applies to every Prepare.
  auto drain = [](SqlEngine& engine, TransactionContext& ctx,
                  const std::string& sql, std::string* out) -> std::string {
    auto r = engine.Execute(ctx, sql);
    if (!r.HasValue()) {
      return "PERR:" + ToString(r.GetStatus());
    }
    std::string rows;
    r.Value().ForEach([&](const Row& y) { rows += y.ToString() + ";"; });
    const std::string st = ToString(r.Value().GetStatus());
    *out = rows;
    return st;
  };
  for (const char* kPred : kPreds) {
    for (size_t variant = 0; variant < kSrcSets.size(); ++variant) {
      SCOPED_TRACE(std::string(kPred) + " variant " + std::to_string(variant));
      auto db1 = MakeDatabase();
      SqlEngine engine1(*db1);
      TransactionContext ctx = db1->BeginContext();
      for (const char* ddl :
           {"CREATE TABLE tgt (k INT64, v INT64);",
            "INSERT INTO tgt VALUES (0, 0), (1, 10), (2, 20), (NULL, 30);",
            kSrcSets[variant][0], kSrcSets[variant][1]}) {
        if (ddl[0] != '\0') {
          auto r = engine1.Execute(ctx, ddl);
          EXPECT_TRUE(r.HasValue()) << ddl;
        }
      }
      ASSERT_SUCCESS(ctx.txn_.PreCommit());
      ctx = db1->BeginContext();
      const std::string sql =
          "UPDATE tgt SET v = v + 1 WHERE (" + std::string(kPred) + ");";
      std::string rows1;
      const std::string st1 = drain(engine1, ctx, sql, &rows1);
      const std::string sel1a =
          drain(engine1, ctx, "SELECT k, v FROM tgt ORDER BY k;", &rows1);
      ASSERT_SUCCESS(ctx.txn_.PreCommit());
      ctx = db1->BeginContext();
      const std::string sel1b =
          drain(engine1, ctx, "SELECT k, v FROM tgt ORDER BY k;", &rows1);

      auto db2 = MakeDatabase();
      SqlEngine engine2(*db2);
      engine2.SetForceRelationalFallback(true);
      TransactionContext ctx2 = db2->BeginContext();
      for (const char* ddl :
           {"CREATE TABLE tgt (k INT64, v INT64);",
            "INSERT INTO tgt VALUES (0, 0), (1, 10), (2, 20), (NULL, 30);",
            kSrcSets[variant][0], kSrcSets[variant][1]}) {
        if (ddl[0] != '\0') {
          auto r = engine2.Execute(ctx2, ddl);
          EXPECT_TRUE(r.HasValue()) << ddl;
        }
      }
      ASSERT_SUCCESS(ctx2.txn_.PreCommit());
      ctx2 = db2->BeginContext();
      std::string rows2;
      const std::string st2 = drain(engine2, ctx2, sql, &rows2);
      const std::string sel2a =
          drain(engine2, ctx2, "SELECT k, v FROM tgt ORDER BY k;", &rows2);
      ASSERT_SUCCESS(ctx2.txn_.PreCommit());
      ctx2 = db2->BeginContext();
      const std::string sel2b =
          drain(engine2, ctx2, "SELECT k, v FROM tgt ORDER BY k;", &rows2);

      // Every statement in this differential must actually succeed: without
      // a success check, a predicate broken on BOTH routes would compare two
      // identical error strings and pass all 54 cases unchecked.
      EXPECT_EQ(st1, "Success") << "cascades route failed";
      EXPECT_EQ(sel1a, "Success") << "post-update select failed";
      EXPECT_EQ(sel1b, "Success") << "post-commit select failed";
      EXPECT_EQ(st1, st2) << "prepare/drain status differs";
      EXPECT_EQ(sel1a, sel2a) << "post-update select status differs";
      EXPECT_EQ(sel1b, sel2b) << "post-commit select status differs";
      if (st1 == "Success" && sel1a == "Success" && sel1b == "Success") {
        EXPECT_EQ(rows1, rows2) << "final table contents differ";
      }
    }
  }
}

TEST_F(QueryTest, QuantifiedSubqueryKeepsOperatorAndNegation) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE s (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE e (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, j INT64);");
  RunSql(ctx, *db_, "INSERT INTO s VALUES (5), (6);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (0, 5), (1, 9), (2, 7);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  const std::vector<Row> gt_any = RunSql(
      ctx, *db_, "SELECT k FROM t WHERE j > ANY (SELECT k FROM s) ORDER BY k;");
  ASSERT_EQ(gt_any.size(), 2U);
  EXPECT_EQ(gt_any[0][0].value.int_value, 1);
  EXPECT_EQ(gt_any[1][0].value.int_value, 2);

  EXPECT_TRUE(
      RunSql(ctx, *db_, "SELECT k FROM t WHERE j = ALL (SELECT k FROM s);")
          .empty());

  const std::vector<Row> gt_ne_all =
      RunSql(ctx, *db_,
             "SELECT k FROM t WHERE j <> ALL (SELECT k FROM s) ORDER BY k;");
  ASSERT_EQ(gt_ne_all.size(), 2U);

  // NOT over an empty ALL-subquery: NOT(TRUE) keeps nothing.
  EXPECT_TRUE(RunSql(ctx, *db_,
                     "SELECT k FROM t WHERE NOT (j = ALL (SELECT k FROM e));")
                  .empty());

  // The DML fold agrees with the SELECT reference.
  const std::vector<Row> upd =
      RunSql(ctx, *db_, "UPDATE t SET j = 0 WHERE j > ANY (SELECT k FROM s);");
  ASSERT_EQ(upd.size(), 1U);
  EXPECT_EQ(upd[0][1].value.int_value, 2);

  const std::vector<Row> del =
      RunSql(ctx, *db_, "DELETE FROM t WHERE NOT (j = ALL (SELECT k FROM e));");
  ASSERT_EQ(del.size(), 1U);
  EXPECT_EQ(del[0][1].value.int_value, 0);
}

// A failed ANY/ALL fold must leave the whole quantified comparison node in
// place so the checked evaluator reports the subquery error. Returning only
// Test() used to reduce the WHERE to the bare probe column and silently
// delete every row whose key was truthy.
TEST_F(QueryTest, DmlWhereAnyFoldFailureSurfacesDivisionByZero) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE any_div_d (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO any_div_d VALUES (0), (3);");
  RunSql(ctx, *db_, "CREATE TABLE any_div_s (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO any_div_s VALUES (0);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  SqlEngine engine(*db_);
  StatusOr<Executor> bad = engine.Prepare(
      ctx, "DELETE FROM any_div_d WHERE k = ANY (SELECT 1/x FROM any_div_s);");
  ASSERT_TRUE(bad.HasValue()) << engine.LastError();
  Row sink;
  while (bad.Value()->Next(&sink, nullptr)) {
  }
  EXPECT_NE(bad.Value()->GetStatus(), Status::kSuccess)
      << "division by zero in the folded subquery must fail the statement";
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*) FROM any_div_d")[0][0],
            Value(int64_t{2}));
}

TEST_F(QueryTest, DmlWhereAnyFailedFoldDoesNotDeleteRows) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE any_t (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO any_t VALUES (1), (2), (3);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  SqlEngine engine(*db_);
  StatusOr<Executor> bad = engine.Prepare(
      ctx, "DELETE FROM any_t WHERE k > ANY (SELECT c FROM missing_table);");
  ASSERT_TRUE(bad.HasValue()) << engine.LastError();
  Row sink;
  while (bad.Value()->Next(&sink, nullptr)) {
  }
  EXPECT_NE(bad.Value()->GetStatus(), Status::kSuccess);
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*) FROM any_t")[0][0],
            Value(int64_t{3}));
}

TEST_F(QueryTest, DmlWhereQuantifiedAndExistsFold) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE s (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (0, 0), (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO s VALUES (1), (NULL), (5);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  const std::vector<Row> updated =
      RunSql(ctx, *db_, "UPDATE t SET v = -1 WHERE k = ANY (SELECT k FROM s);");
  ASSERT_EQ(updated.size(), 1U);
  EXPECT_EQ(updated[0][1].value.int_value, 1);

  // `k <> ALL (s)` sees a NULL candidate: UNKNOWN for every row, so the
  // three-valued fold must delete nothing (SELECT reference agrees).
  const std::vector<Row> deleted =
      RunSql(ctx, *db_, "DELETE FROM t WHERE k <> ALL (SELECT k FROM s);");
  ASSERT_EQ(deleted.size(), 1U);
  EXPECT_EQ(deleted[0][1].value.int_value, 0);

  const std::vector<Row> scalar_upd =
      RunSql(ctx, *db_, "UPDATE t SET v = 7 WHERE k > (SELECT MIN(k) FROM s);");
  ASSERT_EQ(scalar_upd.size(), 1U);
  EXPECT_EQ(scalar_upd[0][1].value.int_value, 1);

  const std::vector<Row> exists_upd =
      RunSql(ctx, *db_, "UPDATE t SET v = 8 WHERE EXISTS (SELECT 1 FROM s);");
  ASSERT_EQ(exists_upd.size(), 1U);
  EXPECT_EQ(exists_upd[0][1].value.int_value, 3);

  // WITH + set operation under a DML subquery: documented unimplemented
  // (dml_delete.test / b/362158469), not a crash.
  SqlEngine engine(*db_);
  const StatusOr<Executor> with_dml = engine.Prepare(
      ctx,
      "DELETE FROM t WHERE EXISTS (WITH c AS (SELECT 1 AS a) SELECT * FROM c "
      "UNION ALL SELECT * FROM c);");
  // The error may surface at Prepare or while the DML result is drained.
  if (with_dml.HasValue()) {
    Row sink;
    while (with_dml.Value()->Next(&sink, nullptr)) {
    }
    EXPECT_NE(with_dml.Value()->GetStatus(), Status::kSuccess);
  }
}

TEST_F(QueryTest, DmlDoesNotFoldCorrelatedExists) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE dml_outer (u INT64, c1 STRING(MAX), c2 INT64);");
  RunSql(ctx, *db_, "CREATE TABLE dml_inner (u INT64);");
  RunSql(ctx, *db_, "INSERT INTO dml_outer VALUES (1, 'a', 0);");
  RunSql(ctx, *db_, "INSERT INTO dml_inner VALUES (0);");
  const auto updated =
      RunSql(ctx, *db_,
             "UPDATE dml_outer SET c1 = 'bb' WHERE u > -3 AND NOT EXISTS "
             "(SELECT 1 FROM dml_inner WHERE dml_inner.u = dml_outer.c2);");
  EXPECT_EQ(updated.size(), 1U);
  const auto rows = RunSql(ctx, *db_, "SELECT c1 FROM dml_outer;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value("a"));
}

TEST_F(QueryTest, DmlWhereInSubqueryAppliesRows) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE src (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE tgt (k INT64, v VARCHAR(4));");
  ASSERT_EQ(
      RunSql(ctx, *db_, "INSERT INTO tgt VALUES (0, 'a'), (1, 'b'), (2, 'c');")
          .size(),
      1U);
  RunSql(ctx, *db_, "INSERT INTO src VALUES (0), (2);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  std::vector<Row> updated = RunSql(
      ctx, *db_, "UPDATE tgt SET v = 'z' WHERE k IN (SELECT k FROM src);");
  ASSERT_EQ(updated.size(), 1U);
  EXPECT_EQ(updated[0][1].value.int_value, 2);

  std::vector<Row> deleted =
      RunSql(ctx, *db_, "DELETE FROM tgt WHERE k NOT IN (SELECT k FROM src);");
  ASSERT_EQ(deleted.size(), 1U);
  EXPECT_EQ(deleted[0][1].value.int_value, 1);

  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();
  const std::vector<Row> left = RunSql(ctx, *db_, "SELECT k FROM tgt;");
  ASSERT_EQ(left.size(), 2U);

  // Empty subquery: IN () is FALSE, NOT IN () is TRUE (three-valued fold).
  RunSql(ctx, *db_, "CREATE TABLE empty_src (k INT64);");
  const std::vector<Row> upd0 =
      RunSql(ctx, *db_,
             "UPDATE tgt SET v = 'q' WHERE k IN (SELECT k FROM empty_src);");
  ASSERT_EQ(upd0.size(), 1U);
  EXPECT_EQ(upd0[0][1].value.int_value, 0);
  const std::vector<Row> del_all = RunSql(
      ctx, *db_, "DELETE FROM tgt WHERE k NOT IN (SELECT k FROM empty_src);");
  ASSERT_EQ(del_all.size(), 1U);
  EXPECT_EQ(del_all[0][1].value.int_value, 2);
}

TEST_F(QueryTest, SimpleSelect) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();

  // Act + Assert: CREATE TABLE t1
  RunSql(ctx, *db_, "CREATE TABLE t1 (c1 INT64, c2 INT64, c3 VARCHAR(10));");

  // Act + Assert: INSERT (1, 10, 'hello') and (2, 20, 'world'), one row each
  std::vector<Row> insert1 =
      RunSql(ctx, *db_, "INSERT INTO t1 VALUES (1, 10, 'hello');");
  ASSERT_EQ(insert1.size(), 1U);
  EXPECT_EQ(insert1[0][1], Value(1));
  std::vector<Row> insert2 =
      RunSql(ctx, *db_, "INSERT INTO t1 VALUES (2, 20, 'world');");
  ASSERT_EQ(insert2.size(), 1U);
  EXPECT_EQ(insert2[0][1], Value(1));

  // Act + Assert: SELECT * FROM t1 WHERE c1 = 1
  std::vector<Row> rows = RunSql(ctx, *db_, "SELECT * FROM t1 WHERE c1 = 1;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[0][1], Value(10));
  EXPECT_EQ(rows[0][2], Value("hello"));

  // Act + Assert: PreCommit
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

// A qualified star with an unknown qualifier must be rejected; silently
// erasing it used to produce an empty select list.
TEST_F(QueryTest, QualifiedStarWithUnknownRelationFails) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t1 (c1 INT64, c2 INT64, c3 VARCHAR(10));");
  SqlEngine engine(*db_);
  // The engine may reject at prepare time or at first execution; both count.
  bool rejected = false;
  StatusOr<QueryResult> executed =
      engine.Execute(ctx, "SELECT no_such_rel.* FROM t1;");
  if (!executed.HasValue()) {
    rejected = true;
  } else {
    ASSERT_NO_THROW({
      Row row;
      while (executed.Value().Next(&row)) {
      }
    });
    rejected = executed.Value().GetStatus() != Status::kSuccess;
  }
  EXPECT_TRUE(rejected);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, NaturalJoinIsRejectedNotCrossProduct) {
  // A NATURAL join's key is the shared column set, which the AST visitor
  // cannot resolve; previously the join silently ran as a cross product.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (u INT64, a INT64);");
  RunSql(ctx, *db_, "CREATE TABLE j (u INT64, x INT64);");
  SqlEngine engine(*db_);
  bool rejected = false;
  StatusOr<QueryResult> executed =
      engine.Execute(ctx, "SELECT * FROM t NATURAL JOIN j;");
  if (!executed.HasValue()) {
    rejected = true;
  } else {
    Row row;
    while (executed.Value().Next(&row)) {
    }
    rejected = executed.Value().GetStatus() != Status::kSuccess;
  }
  EXPECT_TRUE(rejected);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, GroupByEmptyParensFormsSingleGroup) {
  // `GROUP BY ()` keys on a synthesized constant so every row lands in one
  // group; previously the empty key list degenerated into "no GROUP BY".
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (u INT64, a INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1,10),(2,20),(3,NULL);");
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT 1 FROM t GROUP BY ();").size(), 1U);
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*) FROM t GROUP BY ();"),
            std::vector<Row>{Row({Value(3)})});
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, SelectWithProjection) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();

  // Act + Assert: CREATE TABLE t1 and two INSERTs
  RunSql(ctx, *db_, "CREATE TABLE t1 (c1 INT64, c2 INT64, c3 VARCHAR(10));");
  RunSql(ctx, *db_, "INSERT INTO t1 VALUES (1, 10, 'hello');");
  RunSql(ctx, *db_, "INSERT INTO t1 VALUES (2, 20, 'world');");

  // Act + Assert: SELECT c1, c3 FROM t1 WHERE c1 = 2
  std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT c1, c3 FROM t1 WHERE c1 = 2;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[0][1], Value("world"));

  // Act + Assert: PreCommit
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, SqlEngineExplainRequiresQuery) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  // Act + Assert -- bare EXPLAIN is rejected with a precise message.
  StatusOr<Executor> prepared = engine.Prepare(ctx, "EXPLAIN");
  EXPECT_FALSE(prepared.HasValue());
  EXPECT_EQ(prepared.GetStatus(), Status::kUnknown);
  EXPECT_EQ(engine.LastError(), "EXPLAIN requires a query");
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineExplainRejectsNonSelect) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  // Act + Assert -- EXPLAIN of DDL is rejected as not-implemented.
  StatusOr<Executor> prepared =
      engine.Prepare(ctx, "EXPLAIN CREATE TABLE t (a INT64);");
  EXPECT_FALSE(prepared.HasValue());
  EXPECT_EQ(prepared.GetStatus(), Status::kNotImplemented);
  EXPECT_EQ(engine.LastError(),
            "EXPLAIN currently supports SELECT and WITH queries");
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineExplainAnalyzeSelect) {
  // Arrange -- a table with a few rows.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  ASSERT_TRUE(engine.Prepare(ctx, "CREATE TABLE t (a INT64);").HasValue());
  ASSERT_TRUE(
      engine.Prepare(ctx, "INSERT INTO t VALUES (1), (2), (3);").HasValue());

  // Act -- EXPLAIN ANALYZE a simple SELECT.
  StatusOr<Executor> prepared =
      engine.Prepare(ctx, "EXPLAIN ANALYZE SELECT a FROM t;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
  const std::optional<StatementType> explain_type = engine.LastStatementType();
  ASSERT_TRUE(explain_type.has_value());
  EXPECT_EQ(explain_type, std::optional(StatementType::kSelect));
  Row row;
  std::string text;
  text.reserve(8192);
  while (prepared.Value()->Next(&row, nullptr)) {
    text += row[0].AsString();
  }

  // Assert -- the plan embeds the runtime statistics.
  EXPECT_NE(text.find("Planning Time"), std::string::npos) << text;
  EXPECT_NE(text.find("Actual Rows"), std::string::npos) << text;
  EXPECT_NE(text.find("Execution Time"), std::string::npos) << text;
  EXPECT_NE(text.find("Buffer Pool"), std::string::npos) << text;
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineAnalyzeRefreshesStatistics) {
  // Arrange -- insert rows without refreshing catalog statistics.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  auto run = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    ASSERT_TRUE(prepared.HasValue()) << sql << "\n" << engine.LastError();
    Row consumed;
    while (prepared.Value()->Next(&consumed, nullptr)) {
    }
  };
  run("CREATE TABLE stats_t (a INT64);");
  run("INSERT INTO stats_t VALUES (1), (2), (3);");

  const auto stats_before = ctx.GetStats("stats_t");
  ASSERT_EQ(stats_before.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& before = stats_before.Value();
  EXPECT_EQ(before->Rows(), 0);
  // The schema is known even before ANALYZE; check a second, independent
  // statistic instead of repeating the row count.
  EXPECT_EQ(before->Columns(), 1U);

  // Act -- force a statistics refresh.
  StatusOr<Executor> analyzed = engine.Prepare(ctx, "ANALYZE stats_t;");
  ASSERT_TRUE(analyzed.HasValue()) << engine.LastError();
  const std::optional<StatementType> analyzed_type = engine.LastStatementType();
  ASSERT_TRUE(analyzed_type.has_value());
  EXPECT_EQ(analyzed_type, std::optional(StatementType::kAnalyze));
  Row row;
  ASSERT_TRUE(analyzed.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(std::string("ANALYZE")));
  EXPECT_EQ(row[1], Value(std::string("stats_t")));
  EXPECT_EQ(row[2], Value(int64_t{3}));
  ASSERT_FALSE(analyzed.Value()->Next(&row, nullptr));

  const auto stats_after = ctx.GetStats("stats_t");
  ASSERT_EQ(stats_after.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& after = stats_after.Value();
  EXPECT_EQ(after->Rows(), 3);

  // Act -- ANALYZE without a table list refreshes every catalog table.
  StatusOr<Executor> all = engine.Prepare(ctx, "ANALYZE;");
  ASSERT_TRUE(all.HasValue()) << engine.LastError();
  size_t analyze_rows = 0;
  while (all.Value()->Next(&row, nullptr)) {
    ++analyze_rows;
  }
  EXPECT_GE(analyze_rows, 1U);

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineExplainBoundaryRejectsMalformedKeyword) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  // Act + Assert -- "EXPLAINX" is not EXPLAIN; it must fail as bad SQL.
  StatusOr<Executor> prepared = engine.Prepare(ctx, "EXPLAINX SELECT 1;");
  EXPECT_FALSE(prepared.HasValue());
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineInsertValidationErrors) {
  // Arrange -- a two-column table.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  ASSERT_TRUE(
      engine.Prepare(ctx, "CREATE TABLE t (a INT64, b INT64);").HasValue());

  // Act + Assert -- column/value count mismatch.
  StatusOr<Executor> mismatch =
      engine.Prepare(ctx, "INSERT INTO t (a, b) VALUES (1);");
  EXPECT_FALSE(mismatch.HasValue());
  // A column/value count mismatch is a caller error, reported as such.
  EXPECT_EQ(mismatch.GetStatus(), Status::kInvalidArgument);
  EXPECT_NE(engine.LastError().find("mismatch"), std::string::npos)
      << engine.LastError();

  // Act + Assert -- unknown column name.
  StatusOr<Executor> unknown =
      engine.Prepare(ctx, "INSERT INTO t (nope) VALUES (1);");
  EXPECT_FALSE(unknown.HasValue());
  EXPECT_EQ(unknown.GetStatus(), Status::kNotExists);
  EXPECT_NE(engine.LastError().find("unknown INSERT column"), std::string::npos)
      << engine.LastError();

  // Act + Assert -- value count does not match the schema.
  StatusOr<Executor> too_few = engine.Prepare(ctx, "INSERT INTO t VALUES (1);");
  EXPECT_FALSE(too_few.HasValue());
  EXPECT_NE(engine.LastError().find("does not match"), std::string::npos)
      << engine.LastError();

  // Act + Assert -- type mismatch on a typed column.
  StatusOr<Executor> type_mismatch =
      engine.Prepare(ctx, "INSERT INTO t VALUES ('x', 2);");
  EXPECT_FALSE(type_mismatch.HasValue());
  EXPECT_NE(engine.LastError().find("type mismatch"), std::string::npos)
      << engine.LastError();

  // Act + Assert -- a well-formed insert still works afterwards.
  EXPECT_TRUE(engine.Prepare(ctx, "INSERT INTO t VALUES (1, 2);").HasValue());
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineDistinctAndDropTable) {
  // Arrange -- a table with duplicate values.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1), (1), (2), (3);");

  // Act + Assert -- SELECT DISTINCT removes duplicates.
  std::vector<Row> distinct = RunSql(ctx, *db_, "SELECT DISTINCT a FROM t;");
  ASSERT_EQ(distinct.size(), 3);
  EXPECT_EQ(distinct[0][0], Value(1));
  EXPECT_EQ(distinct[1][0], Value(2));
  EXPECT_EQ(distinct[2][0], Value(3));

  // Act + Assert -- DROP TABLE removes the table from the catalog.
  ASSERT_TRUE(engine.Prepare(ctx, "DROP TABLE t;").HasValue());
  EXPECT_FALSE(ctx.GetTable("t").HasValue());
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, SqlEngineDistinctDoesNotStackDuplicateDistinctExecutor) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t_nodup (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO t_nodup VALUES (1, 10), (1, 20), (2, 30);");

  auto count_distinct_in_explain = [&](std::string_view sql) -> size_t {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    if (!prepared.HasValue()) {
      return 0;
    }
    Row row;
    std::string explain_text;
    while (prepared.Value()->Next(&row, nullptr)) {
      explain_text += row[0].AsString();
    }
    // The memo's explanation also names the selected physical operator.
    // Count nodes in the tree, excluding the repeated metadata name.
    if (const size_t metadata = explain_text.find("Rule:");
        metadata != std::string::npos) {
      explain_text.resize(metadata);
    }
    size_t count = 0;
    size_t pos = 0;
    while ((pos = explain_text.find("Distinct", pos)) != std::string::npos) {
      ++count;
      pos += 8;
    }
    return count;
  };

  EXPECT_EQ(
      count_distinct_in_explain("EXPLAIN SELECT DISTINCT a FROM t_nodup;"), 1);
  EXPECT_EQ(count_distinct_in_explain(
                "EXPLAIN SELECT DISTINCT a FROM t_nodup ORDER BY a;"),
            1);
  EXPECT_EQ(count_distinct_in_explain(
                "EXPLAIN SELECT DISTINCT a FROM t_nodup LIMIT 2;"),
            1);

  std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT DISTINCT a FROM t_nodup ORDER BY a;");
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, SqlEngineQueryDataRewriteErrors) {
  // Arrange -- a table with a known schema.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");

  // Act + Assert -- a SELECT of a missing column fails during rewrite.
  StatusOr<Executor> bad_select = engine.Prepare(ctx, "SELECT nope FROM t;");
  EXPECT_FALSE(bad_select.HasValue());

  // Act + Assert -- a WHERE on a missing column fails during rewrite.
  StatusOr<Executor> bad_where =
      engine.Prepare(ctx, "SELECT a FROM t WHERE nope = 1;");
  EXPECT_FALSE(bad_where.HasValue());

  // Act + Assert -- unary NOT and IN resolve correctly in WHERE.
  std::vector<Row> not_rows =
      RunSql(ctx, *db_, "SELECT a FROM t WHERE NOT a = 2;");
  ASSERT_EQ(not_rows.size(), 1);
  EXPECT_EQ(not_rows[0][0], Value(1));

  std::vector<Row> in_rows =
      RunSql(ctx, *db_, "SELECT a FROM t WHERE a IN (1, 3);");
  ASSERT_EQ(in_rows.size(), 1);
  EXPECT_EQ(in_rows[0][0], Value(1));

  // Act + Assert -- a binary expression in WHERE is planned and executed.
  StatusOr<Executor> fn =
      engine.Prepare(ctx, "SELECT a FROM t WHERE a + 1 = 3;");
  ASSERT_TRUE(fn.HasValue()) << engine.LastError();
  Row row;
  ASSERT_TRUE(fn.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(2));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineAnalyzeEdgeCases) {
  // Arrange -- two tables so the comma-separated table list is meaningful.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t1 (a INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t2 (a INT64);");
  RunSql(ctx, *db_, "INSERT INTO t1 VALUES (1), (2);");

  // Act + Assert -- a trailing semicolon + whitespace is trimmed by the
  // ParseAnalyze parser before the table name is read.
  ASSERT_TRUE(engine.Prepare(ctx, "ANALYZE t1;  ").HasValue());

  // Act + Assert -- P3555: ANALYZE publishes the declared column types
  // together with its names. With the types empty, consumers of the
  // QueryResult (BuildColumnDescriptions on a streamed result, print
  // paths) had to fall back to first-row sampling, which is absent when
  // ANALYZE covers zero tables.
  StatusOr<QueryResult> typed = engine.Execute(ctx, "ANALYZE t1, t2");
  ASSERT_TRUE(typed.HasValue()) << engine.LastError();
  EXPECT_EQ(typed.Value().ColumnNames(),
            (std::vector<std::string>{"command", "table", "rows"}));
  EXPECT_EQ(typed.Value().ColumnTypes(),
            (std::vector<ValueType>{ValueType::kVarChar, ValueType::kVarChar,
                                    ValueType::kInt64}));

  // Act + Assert -- a comma-separated table list refreshes both tables.
  StatusOr<Executor> list = engine.Prepare(ctx, "ANALYZE t1, t2");
  ASSERT_TRUE(list.HasValue()) << engine.LastError();
  size_t analyzed = 0;
  Row row;
  while (list.Value()->Next(&row, nullptr)) {
    ++analyzed;
  }
  EXPECT_EQ(analyzed, 2U);

  // Act + Assert -- ANALYZE of a missing table fails with the engine error.
  StatusOr<Executor> missing = engine.Prepare(ctx, "ANALYZE no_such_table");
  EXPECT_FALSE(missing.HasValue());
  EXPECT_EQ(missing.GetStatus(), Status::kNotExists);
  EXPECT_EQ(engine.LastError(),
            "ANALYZE failed: " + missing.GetStatus().GetMessage());

  // Act + Assert -- "ANALYZEt" is not the ANALYZE keyword; the boundary check
  // rejects it and the query falls through to the SQL parser.
  EXPECT_FALSE(engine.Prepare(ctx, "ANALYZEt;").HasValue());

  // Act + Assert -- a table list that starts with a non-identifier char.
  EXPECT_FALSE(engine.Prepare(ctx, "ANALYZE 1abc").HasValue());

  // Act + Assert -- table names separated without a comma are rejected.
  EXPECT_FALSE(engine.Prepare(ctx, "ANALYZE t1 t2").HasValue());

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineTemplateCacheEvictsFullShard) {
  // Arrange -- prepare enough distinct templatable statements to overflow
  // template-cache shards and force eviction of the oldest entries.
  {
    TransactionContext ctx = db_->BeginContext();
    RunSql(ctx, *db_, "CREATE TABLE tpl_t (a INT64);");
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  SqlEngine engine(*db_);
  const uint64_t evictions_before =
      PlanCacheStats().template_evictions.load(std::memory_order_relaxed);
  // Every fingerprint must resolve against the real table, otherwise the
  // template fill never happens.  Keep the SQL shape identical and vary only
  // a comment so each statement has its own literal-free fingerprint.  The
  // loop stops as soon as a shard actually overflows (16 shards x 64 entries
  // => at most 1025 fingerprints by pigeonhole; the bound is just a guard).
  for (size_t i = 0;
       i < 2048 &&
       PlanCacheStats().template_evictions.load(std::memory_order_relaxed) ==
           evictions_before;
       ++i) {
    const std::string sql =
        "INSERT INTO tpl_t /* " + std::to_string(i) + " */ VALUES (1);";
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(engine.Prepare(ctx, sql).HasValue())
        << sql << "\n" << engine.LastError();
    ASSERT_SUCCESS(ctx.txn_.Abort());
  }
  // Assert -- the shard really overflowed and dropped its oldest template.
  EXPECT_GT(PlanCacheStats().template_evictions.load(std::memory_order_relaxed),
            evictions_before);
  // Assert -- preparing a templatable statement afterwards still succeeds
  // and can hit the (now-trimmed) cache without crashing.
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(
        engine.Prepare(ctx, "INSERT INTO tpl_t VALUES (1);").HasValue());
    ASSERT_SUCCESS(ctx.txn_.Abort());
  }
}

TEST_F(QueryTest, SqlEngineUpdateAndDelete) {
  // Arrange -- a three-row table.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  // Act + Assert -- UPDATE rewrites the matched row (draining the executor
  // applies the changes).
  StatusOr<Executor> update_stmt =
      engine.Prepare(ctx, "UPDATE t SET b = 99 WHERE a = 2;");
  ASSERT_TRUE(update_stmt.HasValue()) << engine.LastError();
  Row update_result;
  ASSERT_TRUE(update_stmt.Value()->Next(&update_result, nullptr));
  std::vector<Row> updated = RunSql(ctx, *db_, "SELECT b FROM t WHERE a = 2;");
  ASSERT_EQ(updated.size(), 1U);
  EXPECT_EQ(updated[0][0], Value(99));

  // Act + Assert -- DELETE removes the matched row.
  StatusOr<Executor> delete_stmt =
      engine.Prepare(ctx, "DELETE FROM t WHERE a = 1;");
  ASSERT_TRUE(delete_stmt.HasValue()) << engine.LastError();
  Row delete_result;
  ASSERT_TRUE(delete_stmt.Value()->Next(&delete_result, nullptr));
  std::vector<Row> remaining = RunSql(ctx, *db_, "SELECT a FROM t;");
  ASSERT_EQ(remaining.size(), 2U);

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, UpdateWithAliasAndTableQualifiedSetTargets) {
  // "UPDATE t AS x SET x.a = ..." must update rows: the alias qualifier is a
  // relation name, not a STRUCT column. A raw != against the table name used
  // to misroute the statement into the dotted-STRUCT path, which reported a
  // modified-row count and updated nothing.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  StatusOr<Executor> aliased =
      engine.Prepare(ctx, "UPDATE t AS x SET x.a = 9 WHERE x.b = 20;");
  ASSERT_TRUE(aliased.HasValue()) << engine.LastError();
  Row update_result;
  ASSERT_TRUE(aliased.Value()->Next(&update_result, nullptr));
  std::vector<Row> updated = RunSql(ctx, *db_, "SELECT a FROM t WHERE b = 20;");
  ASSERT_EQ(updated.size(), 1U);
  EXPECT_EQ(updated[0][0], Value(9));

  // The table-name qualifier matches case-insensitively like any identifier.
  StatusOr<Executor> qualified =
      engine.Prepare(ctx, "UPDATE t SET T.a = 5 WHERE t.b = 30;");
  ASSERT_TRUE(qualified.HasValue()) << engine.LastError();
  ASSERT_TRUE(qualified.Value()->Next(&update_result, nullptr));
  std::vector<Row> qualified_updated =
      RunSql(ctx, *db_, "SELECT a FROM t WHERE b = 30;");
  ASSERT_EQ(qualified_updated.size(), 1U);
  EXPECT_EQ(qualified_updated[0][0], Value(5));

  // A qualifier naming neither the table, the alias, nor a column is an
  // unresolvable target, not a silent no-op.
  StatusOr<Executor> unknown =
      engine.Prepare(ctx, "UPDATE t SET nosuch.a = 1 WHERE b = 10;");
  EXPECT_FALSE(unknown.HasValue());
  EXPECT_NE(unknown.GetStatus().GetMessage().find("not found"),
            std::string::npos)
      << unknown.GetStatus().GetMessage();

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineSelectOrderByLimitOffset) {
  // Arrange -- rows inserted out of order.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (3, 30), (1, 10), (2, 20);");

  // Act + Assert -- ORDER BY over a hidden sort column is projected away and
  // rows come back sorted.
  std::vector<Row> sorted = RunSql(ctx, *db_, "SELECT b FROM t ORDER BY a;");
  ASSERT_EQ(sorted.size(), 3U);
  EXPECT_EQ(sorted[0][0], Value(10));
  EXPECT_EQ(sorted[1][0], Value(20));
  EXPECT_EQ(sorted[2][0], Value(30));

  // Act + Assert -- ORDER BY over a selected column with LIMIT + OFFSET.
  std::vector<Row> page =
      RunSql(ctx, *db_, "SELECT a FROM t ORDER BY a DESC LIMIT 2 OFFSET 1;");
  ASSERT_EQ(page.size(), 2U);
  EXPECT_EQ(page[0][0], Value(2));
  EXPECT_EQ(page[1][0], Value(1));

  // A literal ORDER BY key cannot distinguish rows and must not introduce a
  // physical sort or disturb LIMIT enforcement.
  std::vector<Row> literal_order =
      RunSql(ctx, *db_, "SELECT a FROM t ORDER BY 1 LIMIT 2;");
  ASSERT_EQ(literal_order.size(), 2U);

  const std::vector<Row> repeated_projection = RunSql(
      ctx, *db_, "SELECT a + 1 AS first, a + 1 AS second FROM t ORDER BY a;");
  ASSERT_EQ(repeated_projection[0], Row({Value(2), Value(2)}));

  // Act + Assert -- LIMIT + OFFSET without ORDER BY is enforced exactly once,
  // whether or not the optimizer folded it into the plan (D6).
  std::vector<Row> window =
      RunSql(ctx, *db_, "SELECT a FROM t LIMIT 2 OFFSET 1;");
  ASSERT_EQ(window.size(), 2U);

  // Act + Assert -- DISTINCT sees every row before truncation even when LIMIT
  // is present; duplicates collapse first, then the limit applies.
  RunSql(ctx, *db_, "INSERT INTO t VALUES (3, 300);");
  std::vector<Row> distinct_page =
      RunSql(ctx, *db_, "SELECT DISTINCT a FROM t LIMIT 3;");
  ASSERT_EQ(distinct_page.size(), 3U);

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, DistinctWithInSubqueryOnNonProjectedColumn) {
  // REGRESSION: the hidden $semiN correlation key of a decorrelated IN
  // subquery used to sit below DISTINCT, so two rows equal on the selected
  // column but differing in the probe column both survived dedup and the
  // final trim re-emitted the duplicate.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64, x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (y INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (1, 20), (2, 10), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (10), (20);");

  // (a=1,x=10), (a=1,x=20), (a=2,x=10) satisfy `x IN (SELECT y FROM u)`;
  // DISTINCT over the caller-visible column must collapse a=1's two rows
  // while a=3 (no matching x) stays out.
  std::vector<Row> rows = RunSql(
      ctx, *db_, "SELECT DISTINCT a FROM t WHERE x IN (SELECT y FROM u);");
  ASSERT_EQ(rows.size(), 2U);
  std::vector<int64_t> as;
  as.reserve(rows.size());
  for (const Row& row : rows) {
    as.push_back(row[0].value.int_value);
  }
  std::sort(as.begin(), as.end());
  EXPECT_EQ(as, (std::vector<int64_t>{1, 2}));

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, OrderByAliasKeyResolvesAcrossSelfJoinScopes) {
  // REGRESSION: an aliased self-join evaluates ORDER BY keys against the
  // projected output schema first. A qualified key over the second input
  // (p2.id, spelled through its output alias) must bind to the join input
  // tuple it names, not be captured by the projection's same-named bare
  // column of the FIRST input -- which silently degraded every tie of
  // `ORDER BY p1.id, other_id` to hash-build order.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE probe (id INT64, k INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO probe VALUES (1, 10), (2, 10), (3, 20), (4, 20);");

  std::vector<Row> pairs =
      RunSql(ctx, *db_,
             "SELECT p1.id, p2.id AS other_id FROM probe p1 "
             "JOIN probe p2 ON p1.k = p2.k ORDER BY p1.id, other_id;");
  ASSERT_EQ(pairs.size(), 8U);
  EXPECT_EQ(pairs[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(pairs[1], Row({Value(1), Value(2)}));
  EXPECT_EQ(pairs[2], Row({Value(2), Value(1)}));
  EXPECT_EQ(pairs[3], Row({Value(2), Value(2)}));
  EXPECT_EQ(pairs[4], Row({Value(3), Value(3)}));
  EXPECT_EQ(pairs[5], Row({Value(3), Value(4)}));
  EXPECT_EQ(pairs[6], Row({Value(4), Value(3)}));
  EXPECT_EQ(pairs[7], Row({Value(4), Value(4)}));

  // The alias as the sole ordering key orders by the second input column.
  std::vector<Row> alias_only =
      RunSql(ctx, *db_,
             "SELECT p1.id, p2.id AS other_id FROM probe p1 "
             "JOIN probe p2 ON p1.k = p2.k ORDER BY other_id, p1.id;");
  ASSERT_EQ(alias_only.size(), 8U);
  EXPECT_EQ(alias_only[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(alias_only[1], Row({Value(2), Value(1)}));
  EXPECT_EQ(alias_only[2], Row({Value(1), Value(2)}));
  EXPECT_EQ(alias_only[3], Row({Value(2), Value(2)}));
  EXPECT_EQ(alias_only[4], Row({Value(3), Value(3)}));
  EXPECT_EQ(alias_only[5], Row({Value(4), Value(3)}));

  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineWindowFunctionsPartitionRankAndCumulativeSum) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE w (g INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO w VALUES (1, 20), (1, 10), (2, 5), (2, 15);");

  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v), "
             "SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN "
             "UNBOUNDED PRECEDING AND CURRENT ROW) FROM w ORDER BY g, v;");

  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(10), Value(1), Value(10)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(20), Value(2), Value(30)}));
  EXPECT_EQ(rows[2], Row({Value(2), Value(5), Value(1), Value(5)}));
  EXPECT_EQ(rows[3], Row({Value(2), Value(15), Value(2), Value(20)}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, WindowRangeOffsetFollowingStartBoundsFrameCorrectly) {
  // PRODUCTION BUG (fixed): `RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED
  // FOLLOWING` collapsed every frame start onto the last row, so the SUM
  // reported only the final partition row's value.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE wf (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO wf VALUES (1, 10), (2, 20), (3, 30), (5, 50), "
         "(8, 80);");

  // Frame start = first row with k >= k + 1:
  //   k=1 -> rows k=2,3,5,8 -> 20+30+50+80 = 180
  //   k=2 -> rows k=3,5,8 -> 160
  //   k=3 -> rows k=5,8 -> 130
  //   k=5 -> row k=8 -> 80
  //   k=8 -> no row with k >= 9 -> empty frame (NULL per SQL)
  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 FOLLOWING "
             "AND UNBOUNDED FOLLOWING) FROM wf ORDER BY k;");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(180)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(160)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(130)}));
  EXPECT_EQ(rows[3], Row({Value(5), Value(80)}));
  EXPECT_TRUE(rows[4][1].IsNull());
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, WindowGroupsFrameUsesPeerGroups) {
  TransactionContext ctx = db_->BeginContext();
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("v"))};
  window->order_by = {
      WindowOrderTerm{.expression = ColumnValueExp(ColumnName("v")),
                      .ascending = true,
                      .nulls_first = std::nullopt}};
  window->frame_unit = WindowFrameUnit::kGroups;
  window->has_frame = true;
  window->frame_start = {.type = WindowFrameBoundType::kOffsetPreceding,
                         .offset = ConstantValueExp(Value(1))};
  window->frame_end = {.type = WindowFrameBoundType::kCurrentRow,
                       .offset = nullptr};
  SelectStatement statement({NamedExpression("sum", Expression(window))}, {},
                            nullptr);
  relational_detail::Relation input;
  input.schema = Schema("", {Column("v", ValueType::kInt64)});
  input.rows = {Row({Value(10)}), Row({Value(10)}), Row({Value(20)}),
                Row({Value(30)})};
  auto windowed = relational_detail::ApplyWindows(ctx, statement,
                                                  std::move(input), nullptr, {})
                      .MoveValue();
  std::vector<Row> rows;
  EXPECT_EQ(tinylamb::Status::kSuccess,
            windowed.input.ForEachRow(
                [&](const Row& row) { rows.push_back(row); }));
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][1], Value(20));
  EXPECT_EQ(rows[1][1], Value(20));
  EXPECT_EQ(rows[2][1], Value(40));
  EXPECT_EQ(rows[3][1], Value(50));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, WindowFrameExclusionRemovesCurrentRow) {
  TransactionContext ctx = db_->BeginContext();
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("v"))};
  window->order_by = {
      WindowOrderTerm{.expression = ColumnValueExp(ColumnName("v")),
                      .ascending = true,
                      .nulls_first = std::nullopt}};
  window->frame_unit = WindowFrameUnit::kRows;
  window->has_frame = true;
  window->frame_start = {.type = WindowFrameBoundType::kUnboundedPreceding,
                         .offset = nullptr};
  window->frame_end = {.type = WindowFrameBoundType::kCurrentRow,
                       .offset = nullptr};
  window->exclusion = WindowFrameExclusion::kCurrentRow;
  SelectStatement statement({NamedExpression("sum", Expression(window))}, {},
                            nullptr);
  relational_detail::Relation input;
  input.schema = Schema("", {Column("v", ValueType::kInt64)});
  input.rows = {Row({Value(10)}), Row({Value(20)}), Row({Value(30)})};
  auto windowed = relational_detail::ApplyWindows(ctx, statement,
                                                  std::move(input), nullptr, {})
                      .MoveValue();
  std::vector<Row> rows;
  EXPECT_EQ(tinylamb::Status::kSuccess,
            windowed.input.ForEachRow(
                [&](const Row& row) { rows.push_back(row); }));
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][1], Value(10));
  EXPECT_EQ(rows[2][1], Value(30));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, WindowFrameExclusionHandlesPeerGroupsAndTies) {
  for (const auto& [exclusion, expected] :
       std::vector<std::pair<WindowFrameExclusion, std::vector<Value>>>{
           {WindowFrameExclusion::kGroup, {Value(20), Value(20), Value(20)}},
           {WindowFrameExclusion::kTies, {Value(30), Value(30), Value(40)}}}) {
    TransactionContext ctx = db_->BeginContext();
    auto window = std::make_shared<WindowFunctionCallExpression>();
    window->function = "SUM";
    window->args = {ColumnValueExp(ColumnName("v"))};
    window->order_by = {
        WindowOrderTerm{.expression = ColumnValueExp(ColumnName("v")),
                        .ascending = true,
                        .nulls_first = std::nullopt}};
    window->frame_unit = WindowFrameUnit::kRows;
    window->has_frame = true;
    window->frame_start = {.type = WindowFrameBoundType::kUnboundedPreceding,
                           .offset = nullptr};
    window->frame_end = {.type = WindowFrameBoundType::kUnboundedFollowing,
                         .offset = nullptr};
    window->exclusion = exclusion;
    SelectStatement statement({NamedExpression("sum", Expression(window))}, {},
                              nullptr);
    relational_detail::Relation input;
    input.schema = Schema("", {Column("v", ValueType::kInt64)});
    input.rows = {Row({Value(10)}), Row({Value(10)}), Row({Value(20)})};
    auto windowed = relational_detail::ApplyWindows(
                        ctx, statement, std::move(input), nullptr, {})
                        .MoveValue();
    std::vector<Row> rows;
    rows.reserve(1024);
    EXPECT_EQ(tinylamb::Status::kSuccess,
              windowed.input.ForEachRow(
                  [&](const Row& row) { rows.push_back(row); }));
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(rows[i][1], expected[i]) << "row " << i;
    }
    EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
  }
}

TEST_F(QueryTest, SqlEngineUnionAllConcatenatesMultipleBranches) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE u (v INT64);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (1), (2);");

  const std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT v FROM u WHERE v = 1 UNION ALL "
      "SELECT v FROM u WHERE v >= 1 UNION ALL SELECT v FROM u WHERE v = 2;");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(1)}));
  EXPECT_EQ(rows[2], Row({Value(2)}));
  EXPECT_EQ(rows[3], Row({Value(2)}));
  const std::vector<Row> distinct =
      RunSql(ctx, *db_, "SELECT v FROM u UNION DISTINCT SELECT v FROM u;");
  EXPECT_EQ(distinct, (std::vector<Row>{Row({Value(1)}), Row({Value(2)})}));
  const std::vector<Row> except =
      RunSql(ctx, *db_,
             "SELECT v FROM u EXCEPT DISTINCT SELECT v FROM u WHERE v = 2;");
  EXPECT_EQ(except, (std::vector<Row>{Row({Value(1)})}));
  const std::vector<Row> intersect =
      RunSql(ctx, *db_,
             "SELECT v FROM u INTERSECT DISTINCT SELECT v FROM u WHERE v = 2;");
  EXPECT_EQ(intersect, (std::vector<Row>{Row({Value(2)})}));
  const std::vector<Row> promoted =
      RunSql(ctx, *db_, "SELECT 1 AS v UNION ALL SELECT 2.5 AS v;");
  EXPECT_EQ(promoted, (std::vector<Row>{Row({Value(1.0)}), Row({Value(2.5)})}));
  const std::vector<Row> limited_union =
      RunSql(ctx, *db_, "SELECT v FROM u UNION ALL SELECT v FROM u LIMIT 3;");
  EXPECT_EQ(limited_union, (std::vector<Row>{Row({Value(1)}), Row({Value(2)}),
                                             Row({Value(1)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, RightJoinRoutesThroughCascadesOuterLowering) {
  // M6+1: a single plain RIGHT JOIN lowers through the Cascades outer
  // slice (EXPLAIN shows the memo Product node); rows prove the
  // null-supplying (left) side pads while unmatched left rows drop.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE rj_l (a INT64);");
  RunSql(ctx, *db_, "CREATE TABLE rj_r (b INT64);");
  RunSql(ctx, *db_, "INSERT INTO rj_l VALUES (1), (2), (3);");
  RunSql(ctx, *db_, "INSERT INTO rj_r VALUES (2), (3), (4);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explained =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT a, b FROM rj_l RIGHT JOIN rj_r "
                     "ON a = b;");
  ASSERT_TRUE(explained.HasValue()) << engine.LastError();
  std::string plan_text;
  Row plan_row;
  while (explained.Value()->Next(&plan_row, nullptr)) {
    plan_text += plan_row[0].AsString();
  }
  // Cascades-lowered hash join, not the relational interpreter.
  EXPECT_NE(plan_text.find("RightHashJoin"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("RelationalExecutor"), std::string::npos)
      << plan_text;

  const std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT a, b FROM rj_l RIGHT JOIN rj_r ON a = b;");
  ASSERT_EQ(rows.size(), 3U);
  size_t matched = 0;
  size_t padded = 0;
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 2U);
    if (row[0].IsNull()) {
      ++padded;
      EXPECT_EQ(row[1], Value(4));
    } else {
      ++matched;
      EXPECT_EQ(row[0], row[1]);
    }
  }
  EXPECT_EQ(matched, 2U);
  EXPECT_EQ(padded, 1U);
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, FullJoinRoutesThroughCascadesOuterLowering) {
  // M6+1: single FULL JOIN lowers the same way; both sides pad (a = 1 has
  // no partner on the right, b = 4 has none on the left).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE fj_l (a INT64);");
  RunSql(ctx, *db_, "CREATE TABLE fj_r (b INT64);");
  RunSql(ctx, *db_, "INSERT INTO fj_l VALUES (1), (2), (3);");
  RunSql(ctx, *db_, "INSERT INTO fj_r VALUES (2), (3), (4);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explained =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT a, b FROM fj_l FULL JOIN fj_r "
                     "ON a = b;");
  ASSERT_TRUE(explained.HasValue()) << engine.LastError();
  std::string plan_text;
  Row plan_row;
  while (explained.Value()->Next(&plan_row, nullptr)) {
    plan_text += plan_row[0].AsString();
  }
  // Cascades-lowered hash join, not the relational interpreter.
  EXPECT_NE(plan_text.find("FullHashJoin"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("RelationalExecutor"), std::string::npos)
      << plan_text;

  const std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT a, b FROM fj_l FULL JOIN fj_r ON a = b;");
  ASSERT_EQ(rows.size(), 4U);
  size_t matched = 0;
  size_t left_padded = 0;
  size_t right_padded = 0;
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 2U);
    if (row[0].IsNull()) {
      ++left_padded;
      EXPECT_EQ(row[1], Value(4));
    } else if (row[1].IsNull()) {
      ++right_padded;
      EXPECT_EQ(row[0], Value(1));
    } else {
      ++matched;
      EXPECT_EQ(row[0], row[1]);
    }
  }
  EXPECT_EQ(matched, 2U);
  EXPECT_EQ(left_padded, 1U);
  EXPECT_EQ(right_padded, 1U);
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, OuterJoinChainPlansThroughCascadesAndStaysCorrect) {
  // A left-deep LEFT chain lowers through Cascades so the existing outer
  // hash-join executor runs. WHERE on a null-supplying side stays above the
  // join and still observes padded NULLs.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE oc_a (a INT64);");
  RunSql(ctx, *db_, "CREATE TABLE oc_b (b INT64);");
  RunSql(ctx, *db_, "CREATE TABLE oc_c (c INT64);");
  RunSql(ctx, *db_, "INSERT INTO oc_a VALUES (1), (2);");
  RunSql(ctx, *db_, "INSERT INTO oc_b VALUES (2), (3);");
  RunSql(ctx, *db_, "INSERT INTO oc_c VALUES (3), (4);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explained =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT a, b, c FROM oc_a LEFT JOIN oc_b "
                     "ON a = b LEFT JOIN oc_c ON b = c;");
  ASSERT_TRUE(explained.HasValue()) << engine.LastError();
  std::string plan_text;
  Row plan_row;
  while (explained.Value()->Next(&plan_row, nullptr)) {
    plan_text += plan_row[0].AsString();
  }
  EXPECT_NE(plan_text.find("LeftHashJoin"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("RelationalExecutor"), std::string::npos)
      << plan_text;

  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a, b, c FROM oc_a LEFT JOIN oc_b ON a = b "
             "LEFT JOIN oc_c ON b = c;");
  // (1,NULL): a=1 matches nothing, pads through both joins.
  // (2,2,NULL): a=2 matches b=2, which matches no c.
  ASSERT_EQ(rows.size(), 2U);
  size_t both_padded = 0;
  size_t right_padded = 0;
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 3U);
    if (row[1].IsNull()) {
      ++both_padded;
      EXPECT_EQ(row[0], Value(1));
      EXPECT_TRUE(row[2].IsNull());
    } else {
      ++right_padded;
      EXPECT_EQ(row[0], Value(2));
      EXPECT_EQ(row[1], Value(2));
      EXPECT_TRUE(row[2].IsNull());
    }
  }
  EXPECT_EQ(both_padded, 1U);
  EXPECT_EQ(right_padded, 1U);

  // b IS NULL sees the padded row; b = 2 rejects it. Pushing that predicate
  // into the null-supplying scan would drop the pad before WHERE runs.
  const std::vector<Row> only_pad =
      RunSql(ctx, *db_,
             "SELECT a, b, c FROM oc_a LEFT JOIN oc_b ON a = b "
             "LEFT JOIN oc_c ON b = c WHERE b IS NULL;");
  ASSERT_EQ(only_pad.size(), 1U);
  EXPECT_EQ(only_pad[0][0], Value(1));
  EXPECT_TRUE(only_pad[0][1].IsNull());
  const std::vector<Row> matched_b =
      RunSql(ctx, *db_,
             "SELECT a, b, c FROM oc_a LEFT JOIN oc_b ON a = b "
             "LEFT JOIN oc_c ON b = c WHERE b = 2;");
  ASSERT_EQ(matched_b.size(), 1U);
  EXPECT_EQ(matched_b[0][0], Value(2));
  EXPECT_EQ(matched_b[0][1], Value(2));

  // Inner prefix plus one LEFT: the inner match survives when the outer
  // side misses.
  const std::vector<Row> inner_left = RunSql(
      ctx, *db_,
      "SELECT a, b, c FROM oc_a JOIN oc_b ON a = b LEFT JOIN oc_c ON b = c;");
  ASSERT_EQ(inner_left.size(), 1U);
  EXPECT_EQ(inner_left[0][0], Value(2));
  EXPECT_EQ(inner_left[0][1], Value(2));
  EXPECT_TRUE(inner_left[0][2].IsNull());
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, StarSelectReportsExpandedColumnNames) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE star_names (a INT64, b VARCHAR);");
  SqlEngine engine(*db_);
  StatusOr<QueryResult> result =
      engine.Execute(ctx, "SELECT * FROM star_names");
  ASSERT_EQ(result.GetStatus(), Status::kSuccess) << engine.LastError();
  EXPECT_EQ(result.Value().ColumnNames(), (std::vector<std::string>{"a", "b"}));
}

TEST_F(QueryTest, SetOperationKeepsFirstBranchColumnNames) {
  // PRODUCTION BUG (fixed): the recursive operand prepares overwrote
  // result_column_names_, so a set operation reported the LAST branch's
  // labels; PostgreSQL derives set-operation output names from the FIRST
  // branch and pgwire RowDescription follows this member.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  StatusOr<Executor> prepared =
      engine.Prepare(ctx,
                     "SELECT 1 AS first_col UNION ALL "
                     "SELECT 2 AS second_col UNION ALL "
                     "SELECT 3 AS third_col;");
  ASSERT_EQ(prepared.GetStatus(), Status::kSuccess) << engine.LastError();
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  EXPECT_EQ(engine.ResultColumnNames(),
            (std::vector<std::string>{"first_col"}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, MixedSetOperationChainsHonorPerPairOperators) {
  // PRODUCTION BUG (fixed): the visitor derived ONE kind from the
  // SetOperation detail and applied it to every pair, so
  // `A EXCEPT B UNION ALL C` executed as `A EXCEPT B EXCEPT C` and
  // `UNION ALL ... INTERSECT ...` lost the INTERSECT entirely.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE m (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO m VALUES (1), (2);");

  // {1,2} EXCEPT {3} UNION ALL {4} = {1,2,4}; the old code returned {1,2}.
  const std::vector<Row> mixed =
      RunSql(ctx, *db_,
             "SELECT x FROM m EXCEPT DISTINCT SELECT 3 UNION ALL SELECT 4;");
  ASSERT_EQ(mixed.size(), 3U);
  EXPECT_NE(std::ranges::find(mixed, Row({Value(1)})), mixed.end());
  EXPECT_NE(std::ranges::find(mixed, Row({Value(2)})), mixed.end());
  EXPECT_NE(std::ranges::find(mixed, Row({Value(4)})), mixed.end());

  // `1 UNION ALL (2 INTERSECT DISTINCT 2)` = {1, 2}; the old code returned
  // three rows.
  const std::vector<Row> intersect_chain = RunSql(
      ctx, *db_, "SELECT 1 UNION ALL SELECT 2 INTERSECT DISTINCT SELECT 2;");
  ASSERT_EQ(intersect_chain.size(), 2U);

  // A trailing UNION DISTINCT pair must de-duplicate its own result.
  const std::vector<Row> distinct_tail = RunSql(
      ctx, *db_, "SELECT x FROM m UNION ALL SELECT 9 UNION DISTINCT SELECT 9;");
  ASSERT_EQ(distinct_tail.size(), 3U);
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, CteIsMaterializedOnceAndCanBeReadByMultipleConsumers) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE cte_source (v INT64);");
  RunSql(ctx, *db_, "INSERT INTO cte_source VALUES (1), (2), (3);");

  const std::vector<Row> rows = RunSql(
      ctx, *db_,
      "WITH filtered AS (SELECT v FROM cte_source WHERE v > 1) "
      "SELECT v FROM filtered UNION ALL SELECT v FROM filtered ORDER BY v;");
  EXPECT_EQ(rows, (std::vector<Row>{Row({Value(2)}), Row({Value(2)}),
                                    Row({Value(3)}), Row({Value(3)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineUnnestExpandsArraysAndEmitsOffsets) {
  TransactionContext ctx = db_->BeginContext();
  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT x, p FROM UNNEST([3, NULL]) x WITH OFFSET p ORDER BY p;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(3), Value(0)}));
  EXPECT_EQ(rows[1][0], Value());
  EXPECT_EQ(rows[1][1], Value(1));
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT x FROM UNNEST([]) x;").empty());

  const std::vector<Row> product =
      RunSql(ctx, *db_,
             "SELECT a, b FROM UNNEST([1, 2]) a, UNNEST([10, 20]) b "
             "ORDER BY a, b;");
  EXPECT_EQ(product,
            (std::vector<Row>{
                Row({Value(1), Value(10)}), Row({Value(1), Value(20)}),
                Row({Value(2), Value(10)}), Row({Value(2), Value(20)})}));

  const std::vector<Row> lateral = RunSql(
      ctx, *db_,
      "SELECT x FROM UNNEST([[1, 2], [3]]) arr, UNNEST(arr) x ORDER BY x;");
  EXPECT_EQ(lateral, (std::vector<Row>{Row({Value(1)}), Row({Value(2)}),
                                       Row({Value(3)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineUnnestStructColumnKeepsWholeElementWidth) {
  // UNNEST over a STRUCT array column: the plan declares a single element
  // column, so per-row member flattening (a NULL element decodes as a scalar
  // cell, a sibling row flattens two members) must be re-projected back to a
  // stable width or vectorized consumers reject the batch with "data chunk row
  // width mismatch".
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE StructArr AS "
         "SELECT ARRAY<STRUCT<int64, int64>>[(11, 12), NULL, "
         "(31, 32)] arr "
         "UNION ALL "
         "SELECT ARRAY<STRUCT<int64, int64>>[NULL, (21, 22)];");
  const std::vector<Row> rows = RunSql(
      ctx, *db_, "SELECT elem FROM StructArr t, t.arr elem ORDER BY elem;");
  ASSERT_EQ(rows.size(), 5U);
  for (const Row& row : rows) {
    ASSERT_EQ(row.values_.size(), 1U);
  }
  EXPECT_EQ(rows[0][0], Value());
  EXPECT_EQ(rows[1][0], Value());
  EXPECT_EQ(rows[2], Row({Value(std::string("{\"f1\":11,\"f2\":12}"))}));
  EXPECT_EQ(rows[3], Row({Value(std::string("{\"f1\":21,\"f2\":22}"))}));
  EXPECT_EQ(rows[4], Row({Value(std::string("{\"f1\":31,\"f2\":32}"))}));

  // A positional ORDER BY over an alias-qualified field reference must sort
  // the rewritten accessor, not the pre-rewrite column (which the unnest
  // output does not carry).
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT elem.f1 FROM StructArr t, t.arr elem ORDER BY 1;"),
            (std::vector<Row>{Row({Value()}), Row({Value()}), Row({Value(11)}),
                              Row({Value(21)}), Row({Value(31)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineUnnestFlattenedStructFeedsProjectionAndAggregate) {
  // UNNEST([STRUCT(1 AS y, 2 AS x)]) flattens y and x to top-level columns;
  // both the projection and an aggregate over them used to fail with "numeric
  // value required" because the executor emitted the raw struct JSON cell.
  TransactionContext ctx = db_->BeginContext();
  EXPECT_EQ(
      RunSql(ctx, *db_, "SELECT y, x FROM UNNEST([STRUCT(1 AS y, 2 AS x)]);"),
      (std::vector<Row>{Row({Value(1), Value(2)})}));
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT SUM(y) FROM UNNEST([STRUCT(1 AS y)]) x;"),
            (std::vector<Row>{Row({Value(1)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineUnnestStructFieldNameContainingColon) {
  // A struct field name containing ':' must survive the JSON round-trip:
  // the field splitter used to cut the key at the first ':' even inside the
  // quoted name, so the anonymous sibling's f1 was renamed to a broken key
  // and e.`a:b` resolved to NULL for it.
  TransactionContext ctx = db_->BeginContext();
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT e.`a:b` AS v FROM UNNEST([STRUCT(1 AS `a:b`), "
                   "STRUCT(2)]) e ORDER BY v;"),
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineCastWrapsAggregateOverUnnest) {
  TransactionContext ctx = db_->BeginContext();
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT CAST(SUM(x) AS STRING) FROM UNNEST([1,2]) AS x;"),
            (std::vector<Row>{Row({Value(std::string("3"))})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineNullSafeJoinMatchesNulls) {
  // `IS NOT DISTINCT FROM` becomes a null-safe hash-join key: NULL must join
  // to NULL (plain `=` would drop the NULL=NULL pair).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ns_l (id INT64, k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE ns_r (id INT64, k INT64);");
  RunSql(ctx, *db_, "INSERT INTO ns_l VALUES (1, 5), (2, NULL);");
  RunSql(ctx, *db_, "INSERT INTO ns_r VALUES (7, 5), (8, NULL);");
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT l.id, r.id FROM ns_l l JOIN ns_r r "
             "ON l.k IS NOT DISTINCT FROM r.k ORDER BY l.id;"),
      (std::vector<Row>{Row({Value(1), Value(7)}), Row({Value(2), Value(8)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, UsingColumnSignednessMismatchDoesNotJoin) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "SELECT * FROM (SELECT -1 AS x) a "
         "JOIN (SELECT CAST(18446744073709551615 AS UINT64) AS x) b "
         "USING (x);");
  const auto rows =
      RunSql(ctx, *db_,
             "SELECT * FROM (SELECT -1 AS x) a "
             "JOIN (SELECT CAST(18446744073709551615 AS UINT64) AS x) b "
             "USING (x);");
  EXPECT_TRUE(rows.empty()) << "-1 INT64 must not equal UINT64_MAX in USING";
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, OocHashJoinDoesNotMatchSignedUnsignedBitTwins) {
  TransactionContext ctx = db_->BeginContext();
  const auto rows =
      RunSql(ctx, *db_,
             "SELECT * FROM (SELECT -1 AS k) a "
             "JOIN (SELECT CAST(18446744073709551615 AS UINT64) AS k) b "
             "ON a.k = b.k;");
  EXPECT_TRUE(rows.empty())
      << "signed -1 must not equal unsigned 2^64-1 in the OOC hash join";
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineChainedUsingEnforcesEveryKey) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE using_a (x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE using_b (x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE using_c_bad (x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE using_c_good (x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE using_c_plain (y INT64);");
  RunSql(ctx, *db_, "INSERT INTO using_a VALUES (1);");
  RunSql(ctx, *db_, "INSERT INTO using_b VALUES (1);");
  RunSql(ctx, *db_, "INSERT INTO using_c_bad VALUES (2);");
  RunSql(ctx, *db_, "INSERT INTO using_c_good VALUES (1);");
  RunSql(ctx, *db_, "INSERT INTO using_c_plain VALUES (1);");
  EXPECT_TRUE(RunSql(ctx, *db_,
                     "SELECT * FROM using_a a JOIN using_b b USING (x) "
                     "JOIN using_c_bad c USING (x);")
                  .empty());
  const auto matched =
      RunSql(ctx, *db_,
             "SELECT * FROM using_a a JOIN using_b b USING (x) "
             "JOIN using_c_good c USING (x);");
  ASSERT_EQ(matched.size(), 1U);
  ASSERT_EQ(matched[0].values_.size(), 1U);
  EXPECT_EQ(matched[0][0], Value(int64_t{1}));
  const auto bare_after_using =
      RunSql(ctx, *db_,
             "SELECT x FROM using_a a JOIN using_b b USING (x) "
             "JOIN using_c_plain c ON TRUE;");
  ASSERT_EQ(bare_after_using.size(), 1U);
  EXPECT_EQ(bare_after_using[0][0], Value(int64_t{1}));
  SqlEngine engine(*db_);
  StatusOr<Executor> ambiguous =
      engine.Prepare(ctx,
                     "SELECT x FROM using_a a JOIN using_b b USING (x) "
                     "JOIN using_c_good c ON TRUE;");
  if (!ambiguous.HasValue()) {
    EXPECT_NE(engine.LastError().find("ambiguous"), std::string::npos)
        << engine.LastError();
  } else {
    Row row;
    while (ambiguous.Value()->Next(&row, nullptr)) {
    }
    const Status status = ambiguous.Value()->GetStatus();
    EXPECT_NE(status, Status::kSuccess);
    EXPECT_NE(status.GetMessage().find("ambiguous"), std::string::npos)
        << status.GetMessage();
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineContradictoryOrPredicateEliminatesScan) {
  // `(x > 100 AND x < 10) OR (b IS TRUE AND b IS FALSE)` is false for every
  // row; the disjunction must collapse so the scan is eliminated (empty).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE contra (x INT64, b BOOL);");
  RunSql(ctx, *db_, "INSERT INTO contra VALUES (1, true), (2, false);");
  EXPECT_TRUE(
      RunSql(ctx, *db_,
             "SELECT x FROM contra "
             "WHERE (x > 100 AND x < 10) OR (b IS TRUE AND b IS FALSE);")
          .empty());
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineCompositeAndResidualEquiJoin) {
  // Composite equality keys hash on both columns; a non-key residual conjunct
  // rides above the hash join without demoting it to a nested loop.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE cj_l (id INT64, a INT64, amt INT64);");
  RunSql(ctx, *db_, "CREATE TABLE cj_r (id INT64, a INT64, qty INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO cj_l VALUES (1, 1, 50), (1, 2, 10), (2, 2, 5);");
  RunSql(ctx, *db_,
         "INSERT INTO cj_r VALUES (1, 1, 20), (1, 2, 40), (2, 2, 3);");
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT l.id, r.id FROM cj_l l JOIN cj_r r "
             "ON l.id = r.id AND l.a = r.a AND l.amt > r.qty "
             "ORDER BY l.id, r.id;"),
      (std::vector<Row>{Row({Value(1), Value(1)}), Row({Value(2), Value(2)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineUnusedParentJoinEliminatedKeepsMultiplicity) {
  // Eliminating an inner join to a unique parent whose columns are unused must
  // preserve the child's rows exactly (no duplication, no filtering).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE up_parent (pid INT64, label STRING);");
  RunSql(ctx, *db_, "CREATE TABLE up_child (cid INT64, pid INT64);");
  RunSql(ctx, *db_, "INSERT INTO up_parent VALUES (1, 'a'), (2, 'b');");
  RunSql(ctx, *db_, "INSERT INTO up_child VALUES (10, 1), (11, 1), (12, 2);");
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT c.cid FROM up_child c JOIN up_parent p "
             "ON c.pid = p.pid ORDER BY c.cid;"),
      (std::vector<Row>{Row({Value(10)}), Row({Value(11)}), Row({Value(12)})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineOrderBySelectAliasOnJoin) {
  // ORDER BY a SELECT-list alias over a self-join (`other_id`) must not push a
  // Sort below the projection that produces the alias ("column not found").
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ob (id INT64, k INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO ob VALUES (1, 10), (2, 10), (3, 20), (4, 20);");
  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.id, b.id AS other_id FROM ob a JOIN ob b "
             "ON a.k = b.k ORDER BY a.id, other_id;");
  ASSERT_EQ(rows.size(), 8U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(2)}));
  EXPECT_EQ(rows.back(), Row({Value(4), Value(4)}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, SqlEngineUnresolvedColumnInSubqueryPredicateFailsCleanly) {
  // An unresolvable qualified reference inside a correlated subquery
  // predicate (`tree.grp`, `d.parent` with `tree` out of scope) used to
  // escape as a std::runtime_error from the per-row WHERE evaluation (found
  // by the Griffin fuzzer).  It must fail through the Status channel like
  // the top-level form of the same mistake does.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE rs_c (id INT64, grp INT64, val INT64);");
  RunSql(ctx, *db_, "CREATE TABLE rs_tree (id INT64, parent INT64);");
  RunSql(ctx, *db_, "INSERT INTO rs_c VALUES (1, 1, 10), (2, NULL, 30);");
  SqlEngine engine(*db_);
  StatusOr<QueryResult> executed = engine.Execute(
      ctx,
      "SELECT val FROM rs_c WHERE EXISTS (SELECT 1 FROM rs_c d WHERE "
      "d.parent = rs_c.val AND d.val > rs_tree.grp);");
  if (executed.HasValue()) {
    // Rejection may surface at Execute or lazily during drain; either way it
    // must be a Status, and iterating must not throw.
    Row row;
    EXPECT_NO_THROW({
      while (executed.Value().Next(&row)) {
      }
    });
    EXPECT_NE(executed.Value().GetStatus(), Status::kSuccess);
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, DmlWhereBareBoolColumnOnlyTouchesTrueRows) {
  // `DELETE FROM t WHERE flag` / `UPDATE ... WHERE flag`: a bare column in
  // the DELETE/UPDATE WHERE arrives as a PathExpression in the GoogleSQL
  // dump -- the same node kind as the target table.  The visitor skipped
  // every PathExpression while hunting for the predicate, so the WHERE was
  // silently dropped and the statement hit the WHOLE table (found by
  // sql_oracle_fuzzer's DQE oracle: DELETE WHERE flag removed FALSE and
  // NULL rows too).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE bb (u INT64, flag BOOL);");
  RunSql(ctx, *db_,
         "INSERT INTO bb VALUES (1, TRUE), (2, FALSE), (3, NULL), (4, TRUE);");
  const std::vector<Row> deleted =
      RunSql(ctx, *db_, "DELETE FROM bb WHERE flag;");
  ASSERT_EQ(deleted.size(), 1U);
  EXPECT_EQ(deleted[0][1], Value(int64_t{2}));
  const std::vector<Row> rest =
      RunSql(ctx, *db_, "SELECT u FROM bb ORDER BY u;");
  ASSERT_EQ(rest.size(), 2U);
  EXPECT_EQ(rest[0][0], Value(int64_t{2}));
  EXPECT_EQ(rest[1][0], Value(int64_t{3}));

  RunSql(ctx, *db_, "UPDATE bb SET u = 9 WHERE flag;");
  const std::vector<Row> untouched =
      RunSql(ctx, *db_, "SELECT COUNT(*) FROM bb WHERE u = 9;");
  ASSERT_EQ(untouched.size(), 1U);
  EXPECT_EQ(untouched[0][0], Value(int64_t{0}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

TEST_F(QueryTest, ComparisonAgainstNullLiteralIsUnknownNotError) {
  // `WHERE a > NULL` must evaluate to UNKNOWN for every row (zero rows).
  // The Cascades range-extraction pass narrowed the scan envelope with
  // Value::operator< (ExcShim) using the NULL literal as a bound, which
  // made PLANNING abort with "Different type cannot be compared."
  // (found by sql_oracle_fuzzer's TLP oracle partitioning on
  // `(a > NULL) AND (a IN (-3))`).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE nl (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO nl VALUES (1, 2), (NULL, 5), (7, 2);");
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM nl WHERE a > NULL;").empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM nl WHERE NULL < a;").empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM nl WHERE a = NULL;").empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM nl WHERE a >= NULL;")
                  .empty());  // LCOV_EXCL_LINE
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*) FROM nl WHERE a > 0;"),
            (std::vector<Row>{Row({Value(int64_t{2})})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT COUNT(*) FROM nl WHERE (a > NULL) AND (a > 0);"),
            (std::vector<Row>{Row({Value(int64_t{0})})}));
  EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.Abort());
}

}  // namespace tinylamb
