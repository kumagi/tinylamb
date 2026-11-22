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

#include "query/query_test.hpp"

namespace tinylamb {

TEST_F(QueryTest, ConstantConjunctsDecidedAtScanFilterCompile) {
  // Constant-residual boundary: a deterministic conjunct whose value is a
  // compile-time fact is resolved once by CompileScanFilter (TRUE discharges
  // it, FALSE / NULL rejects the whole conjunction), including on the
  // relational route where expressions stay on the AST.  Results must equal
  // the per-row evaluation, and a rejecting conjunct still suppresses
  // sibling errors (WHERE tolerant semantics).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE cc (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO cc VALUES (1), (5);");
  // Constant TRUE keeps only the runtime compare.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM cc WHERE UPPER('A') = 'A' AND x > 1 "
                   "ORDER BY x;"),
            (std::vector<Row>{Row({Value(5)})}));
  // Constant FALSE decides once: no rows, and the sibling ABS never raises.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM cc WHERE UPPER('A') = 'B' AND x > 1 "
                   "ORDER BY x;"),
            (std::vector<Row>{}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM cc WHERE 1 = 0 AND ABS(x) > 100 ORDER BY x;"),
            (std::vector<Row>{}));
  // Constant NULL (three-valued): rejects the conjunction.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM cc WHERE NULL AND ABS(x) > 100 ORDER BY x;"),
            (std::vector<Row>{}));
  // A stable call is NOT folded either: the conservative gate (kImmutable
  // only) keeps the conjunct a runtime predicate, so its within-statement
  // timing meaning is untouched.
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT x FROM cc WHERE CURRENT_TIMESTAMP IS NOT NULL AND x > 1 "
             "ORDER BY x;"),
      (std::vector<Row>{Row({Value(5)})}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, AndOfOrPredicatesKeepConjunctionInScanFilter) {
  // CompileScanFilter decomposes each top-level OR conjunct into
  // disjunctive branches; appending the branch lists of SEPARATE OR
  // predicates evaluated the whole WHERE as one flat OR, accepting rows
  // that satisfied only one of the two conjuncts (sql_oracle_fuzzer
  // AGG-TLP oracle: `(s >= 'a' OR s != 'zz') AND (s = 'zz' OR s < 'a')`
  // returned every p-true row on the relational fallback path).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ao (a INT64, b INT64, s VARCHAR(8));");
  RunSql(ctx, *db_,
         "INSERT INTO ao VALUES (1, 3, 'zz'), (1, 4, 'a'), (2, 5, 'zz'), "
         "(7, 7, 'a'), (3, 3, NULL);");
  // (a IN {1,2}) AND (b IN {3,4}) as OR-of-equals: intersection, not union.
  const std::vector<Row> joined =
      RunSql(ctx, *db_,
             "SELECT a, b FROM ao WHERE (a = 1 OR a = 2) AND (b = 3 OR b = 4) "
             "ORDER BY a, b;");
  ASSERT_EQ(joined.size(), 2U);
  EXPECT_EQ(joined[0][0], Value(int64_t{1}));
  EXPECT_EQ(joined[0][1], Value(int64_t{3}));
  EXPECT_EQ(joined[1][0], Value(int64_t{1}));
  EXPECT_EQ(joined[1][1], Value(int64_t{4}));
  // The fuzzer shape: q is true only for s = 'zz' rows here, so the
  // conjunction keeps exactly those; the flat-OR bug kept s = 'a' too.
  const std::vector<Row> strings =
      RunSql(ctx, *db_,
             "SELECT a FROM ao WHERE (s >= 'a' OR s != 'zz') AND (s = 'zz' OR "
             "s < 'a') ORDER BY a;");
  ASSERT_EQ(strings.size(), 2U);
  EXPECT_EQ(strings[0][0], Value(int64_t{1}));
  EXPECT_EQ(strings[1][0], Value(int64_t{2}));
  // Same filter under GROUP BY (the plan that surfaced the bug).
  const std::vector<Row> grouped =
      RunSql(ctx, *db_,
             "SELECT a, COUNT(*) FROM ao WHERE (s >= 'a' OR s != 'zz') AND "
             "(s = 'zz' OR s < 'a') GROUP BY a ORDER BY a;");
  ASSERT_EQ(grouped.size(), 2U);
  EXPECT_EQ(grouped[0][0], Value(int64_t{1}));
  EXPECT_EQ(grouped[0][1], Value(int64_t{1}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineUnnestDistinctOrderByOffsetAndUnionAll) {
  TransactionContext ctx = db_->BeginContext();

  // DISTINCT above UNNEST used to throw std::bad_cast: the optimizer's
  // distinct-as-aggregation alternative built an aggregate executor whose
  // target list held plain columns.
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT DISTINCT x FROM UNNEST([2, 1, 2, 3, 1]) x "
             "ORDER BY x;"),
      (std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})}));
  EXPECT_TRUE(
      RunSql(ctx, *db_, "SELECT DISTINCT x FROM UNNEST(ARRAY<INT64>[]) x;")
          .empty());

  // ORDER BY the WITH OFFSET pseudo-column: the sort node sits above the
  // projection, so the engine must project a hidden key column it can sort
  // on (and strip it from the output).
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST([30, 10, 20]) AS x WITH OFFSET "
             "ORDER BY OFFSET;"),
      (std::vector<Row>{Row({Value(30)}), Row({Value(10)}), Row({Value(20)})}));
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST([30, 10, 20]) AS x WITH OFFSET AS o "
             "ORDER BY o DESC;"),
      (std::vector<Row>{Row({Value(20)}), Row({Value(10)}), Row({Value(30)})}));

  // Every UNION ALL branch must execute; the unnest route used to drop the
  // non-head branches.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM UNNEST([1]) x UNION ALL "
                   "SELECT x FROM UNNEST([2]) x;"),
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)})}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineArrayGeneratorsFeedUnnest) {
  TransactionContext ctx = db_->BeginContext();
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5, 2)) x ORDER BY x;"),
      (std::vector<Row>{Row({Value(1)}), Row({Value(3)}), Row({Value(5)})}));
  EXPECT_EQ(
      RunSql(
          ctx, *db_,
          "SELECT x FROM UNNEST(GENERATE_SERIES(3, 1, -1)) x ORDER BY x DESC;"),
      (std::vector<Row>{Row({Value(3)}), Row({Value(2)}), Row({Value(1)})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', "
                   "'2020-01-03')) x ORDER BY x;"),
            (std::vector<Row>{Row({Value::Date("2020-01-01")}),
                              Row({Value::Date("2020-01-02")}),
                              Row({Value::Date("2020-01-03")})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT x FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', "
                   "'2020-01-05', INTERVAL 2 DAY)) x ORDER BY x;"),
            (std::vector<Row>{Row({Value::Date("2020-01-01")}),
                              Row({Value::Date("2020-01-03")}),
                              Row({Value::Date("2020-01-05")})}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, BuiltinTableFunctionBecomesRowSource) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  // GENERATE_SERIES is a scalar generator returning an ARRAY, so a FROM-level
  // call is an implicit UNNEST of that array: no UNNEST wrapper needed, and
  // the unaliased value column keeps BigQuery's TVF output name.
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT * FROM GENERATE_SERIES(1, 4);"),
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)}),
                              Row({Value(4)})}));
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT value FROM GENERATE_SERIES(START => 1, STOP => 7, "
             "STEP => 3) ORDER BY value DESC;"),
      (std::vector<Row>{Row({Value(7)}), Row({Value(4)}), Row({Value(1)})}));
  // An explicit alias renames the value column exactly like UNNEST's alias.
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT g FROM GENERATE_SERIES(5, 1, -2) AS g ORDER BY g;"),
      (std::vector<Row>{Row({Value(1)}), Row({Value(3)}), Row({Value(5)})}));
  // The call is an implicit UNNEST over the preceding sources, so its
  // arguments may reference them; the join form reaches the same path.
  RunSql(ctx, *db_, "CREATE TABLE tfv_t (x INT);");
  RunSql(ctx, *db_, "INSERT INTO tfv_t VALUES (1), (5);");
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT tfv_t.x, value FROM tfv_t, "
             "GENERATE_SERIES(tfv_t.x, tfv_t.x + 1) "
             "ORDER BY tfv_t.x, value;"),
      (std::vector<Row>{Row({Value(1), Value(1)}), Row({Value(1), Value(2)}),
                        Row({Value(5), Value(5)}), Row({Value(5), Value(6)})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT value FROM GENERATE_SERIES(1, 2) JOIN tfv_t "
                   "ON tfv_t.x = value ORDER BY value;"),
            (std::vector<Row>{Row({Value(1)})}));
  // INSERT ... SELECT carries the TVF's column name into the target list.
  RunSql(ctx, *db_, "CREATE TABLE tfv_target (v INT);");
  RunSql(ctx, *db_,
         "INSERT INTO tfv_target SELECT * FROM GENERATE_SERIES(1, 3);");
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*), SUM(v) FROM tfv_target;"),
            (std::vector<Row>{Row({Value(int64_t{3}), Value(6)})}));

  // Plan contract: the frontend lowers the call to kUnnest over the group's
  // current input (here the empty one-row input), not to a second scan node.
  // Constant args fold: constant_unnest_to_values expands the series into a
  // kValues node, so no runtime Unnest executor remains for this shape.
  StatusOr<Executor> explain =
      engine.Prepare(ctx, "EXPLAIN SELECT * FROM GENERATE_SERIES(1, 2);");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  std::string plan_text;
  Row row;
  while (explain.Value()->Next(&row, nullptr)) {
    plan_text += row[0].value.varchar_value;
    plan_text += "\n";
  }
  EXPECT_NE(plan_text.find("Values"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("Unnest"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  // A correlated series keeps the runtime unnest: the fold is for constant
  // args only.
  StatusOr<Executor> explain_correlated = engine.Prepare(
      ctx, "EXPLAIN SELECT * FROM tfv_t, GENERATE_SERIES(tfv_t.x, 3);");
  ASSERT_TRUE(explain_correlated.HasValue()) << engine.LastError();
  std::string correlated_plan;
  while (explain_correlated.Value()->Next(&row, nullptr)) {
    correlated_plan += row[0].value.varchar_value;
    correlated_plan += "\n";
  }
  EXPECT_NE(correlated_plan.find("Unnest"), std::string::npos)
      << correlated_plan;

  // Argument binding failures are statement errors, not silent empty reads.
  const auto expect_prepare_error = [&](std::string_view sql,
                                        std::string_view needle) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_FALSE(prepared.HasValue()) << sql;
    EXPECT_NE(engine.LastError().find(needle), std::string::npos)
        << sql << "\n"
        << engine.LastError();
  };
  expect_prepare_error("SELECT * FROM GENERATE_SERIES(1);",
                       "requires START and STOP");
  expect_prepare_error("SELECT * FROM GENERATE_SERIES(1, 2, 3, 4);",
                       "too many arguments");
  expect_prepare_error(
      "SELECT * FROM GENERATE_SERIES(START => 1, START => 2, STOP => 3);",
      "duplicate argument start");
  expect_prepare_error("SELECT * FROM GENERATE_SERIES(FOO => 1, STOP => 2);",
                       "no argument named foo");
  expect_prepare_error(
      "SELECT * FROM GENERATE_SERIES(1, 3) TABLESAMPLE SYSTEM (10 PERCENT);",
      "TABLESAMPLE");
  ctx.txn_.Abort();
}

TEST_F(QueryTest, UnnestAnonymousOrderByKeyRidesAHiddenColumn) {
  TransactionContext ctx = db_->BeginContext();
  // An anonymous select item has no output name for the sort to re-read, and
  // re-evaluating its expression above the projection cannot see the UNNEST
  // value column ("column x not found", seen with both UNNEST and TVF
  // sources). The key must ride a hidden projection column instead.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT MOD(x, 2) FROM UNNEST([1, 2, 3, 4, 5]) x "
                   "ORDER BY 1;"),
            (std::vector<Row>{Row({Value(0)}), Row({Value(0)}), Row({Value(1)}),
                              Row({Value(1)}), Row({Value(1)})}));
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT DISTINCT x + 1 FROM UNNEST([1, 2, 3]) x "
             "ORDER BY 1 DESC;"),
      (std::vector<Row>{Row({Value(4)}), Row({Value(3)}), Row({Value(2)})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT value * 10 FROM GENERATE_SERIES(3, 1, -1) "
                   "ORDER BY 1 LIMIT 2;"),
            (std::vector<Row>{Row({Value(10)}), Row({Value(20)})}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, CascadesOptimizesUnnestPlans) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  // A constant array argument folds to a kValues node (no runtime unnest)
  // via constant_unnest_to_values.
  StatusOr<Executor> explain =
      engine.Prepare(ctx, "EXPLAIN SELECT x FROM UNNEST([1, 2, 3]) x;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  std::string plan_text;
  Row row;
  while (explain.Value()->Next(&row, nullptr)) {
    plan_text += row[0].value.varchar_value;
    plan_text += "\n";
  }
  EXPECT_NE(plan_text.find("Values"), std::string::npos);
  EXPECT_EQ(plan_text.find("Unnest"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos);

  const std::vector<Row> filtered =
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) x WHERE x > 2 ORDER BY x;");
  EXPECT_EQ(filtered, (std::vector<Row>{Row({Value(3)}), Row({Value(4)}),
                                        Row({Value(5)})}));

  const std::vector<Row> grouped =
      RunSql(ctx, *db_,
             "SELECT count(*), max(x), min(x) FROM UNNEST([10, 20, 30]) x;");
  ASSERT_EQ(grouped.size(), 1U);
  EXPECT_EQ(grouped[0], Row({Value(int64_t{3}), Value(30), Value(10)}));

  ASSERT_TRUE(engine.Execute(ctx, "CREATE TABLE tbl_u (id INT);").HasValue());
  RunSql(ctx, *db_, "INSERT INTO tbl_u VALUES (1), (2);");
  StatusOr<Executor> explain_joined =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT tbl_u.id, x FROM tbl_u, UNNEST([10, 20]) "
                     "x ORDER BY tbl_u.id, x;");
  ASSERT_TRUE(explain_joined.HasValue()) << engine.LastError();
  std::string explain_joined_text;
  while (explain_joined.Value()->Next(&row, nullptr)) {
    explain_joined_text += row[0].value.varchar_value;
    explain_joined_text += "\n";
  }
  EXPECT_NE(explain_joined_text.find("Unnest"), std::string::npos);
  EXPECT_NE(explain_joined_text.find("FullScan"), std::string::npos);
  EXPECT_EQ(explain_joined_text.find("RelationalPlan"), std::string::npos);

  const std::vector<Row> joined =
      RunSql(ctx, *db_,
             "SELECT tbl_u.id, x FROM tbl_u, UNNEST([10, 20]) x ORDER BY "
             "tbl_u.id, x;");
  EXPECT_EQ(joined,
            (std::vector<Row>{
                Row({Value(1), Value(10)}), Row({Value(1), Value(20)}),
                Row({Value(2), Value(10)}), Row({Value(2), Value(20)})}));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineInsertSelectCopiesAndMapsRows) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  ASSERT_TRUE(
      engine.Execute(ctx, "CREATE TABLE src (id INT, name VARCHAR(10));")
          .HasValue());
  ASSERT_TRUE(
      engine.Execute(ctx, "CREATE TABLE dst (name VARCHAR(10), id INT);")
          .HasValue());
  auto source_insert =
      engine.Execute(ctx, "INSERT INTO src VALUES (1, 'one'), (2, 'two');");
  ASSERT_TRUE(source_insert.HasValue()) << engine.LastError();
  EXPECT_EQ(source_insert.Value().AffectedRows(), 2);

  auto inserted = engine.Execute(
      ctx, "INSERT INTO dst (id, name) SELECT id, name FROM src WHERE id > 1;");
  ASSERT_TRUE(inserted.HasValue()) << engine.LastError();
  EXPECT_EQ(inserted.Value().AffectedRows(), 1);

  auto selected = engine.Execute(ctx, "SELECT name, id FROM dst;");
  ASSERT_TRUE(selected.HasValue()) << engine.LastError();
  const std::vector<Row> rows = selected.Value().Collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0][0], Value("two"));
  EXPECT_EQ(rows[0][1], Value(2));
}

TEST_F(QueryTest, SqlEngineUnionAllAppliesLimitAfterConcatenation) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ul (v INT64);");
  RunSql(ctx, *db_, "INSERT INTO ul VALUES (1), (2);");

  const std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT v FROM ul UNION ALL SELECT v FROM ul LIMIT 3;");
  EXPECT_EQ(rows.size(), 3U);
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineSingleTableAlias) {
  // Phase 8: an aliased table resolves through the optimizer's relation-key
  // schemas.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT a.v FROM t AS a WHERE a.k > 1 ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(20));
  EXPECT_EQ(rows[1][0], Value(30));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineSelfJoinEquiJoin) {
  // Phase 8: two aliases of one physical table join through the optimizer;
  // each side renames its schema so `a.k` and `b.k` stay distinguishable.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v FROM t AS a JOIN t AS b ON a.k = b.k WHERE b.k >= 2 "
             "ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(20));
  EXPECT_EQ(rows[1][0], Value(30));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineSelfJoinCommaWithWhere) {
  // Phase 8: comma-separated self-join with the condition in WHERE.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT x.v FROM t AS x, t AS y WHERE x.k = y.k AND "
             "y.k < 3 ORDER BY x.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(10));
  EXPECT_EQ(rows[1][0], Value(20));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineSelfJoinResidualCondition) {
  // Phase 8: a non-equality ON condition cannot drive hash/index joins; the
  // residual predicate is applied above the cross product.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE s (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO s VALUES (1), (2), (3);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.k FROM s AS a JOIN s AS b ON a.k > b.k ORDER BY a.k;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[1][0], Value(3));
  EXPECT_EQ(rows[2][0], Value(3));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineLeftJoinStaysCorrect) {
  // Outer joins keep the syntactic relational path and preserve NULL padding
  // semantics end-to-end.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (9, 90);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(10));
  EXPECT_EQ(rows[1][0], Value(20));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineRightAndFullJoinPreserveUnmatchedRows) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200), (3, 300);");

  std::vector<Row> right_rows = RunSql(
      ctx, *db_, "SELECT a.v, b.w FROM l AS a RIGHT JOIN r AS b ON a.k = b.k;");
  ASSERT_EQ(right_rows.size(), 2U);
  EXPECT_TRUE(std::ranges::any_of(right_rows, [](const Row& row) {
    return row == Row({Value(20), Value(200)});
  }));
  EXPECT_TRUE(std::ranges::any_of(right_rows, [](const Row& row) {
    return row == Row({Value(), Value(300)});
  }));

  std::vector<Row> full_rows = RunSql(
      ctx, *db_, "SELECT a.v, b.w FROM l AS a FULL JOIN r AS b ON a.k = b.k;");
  ASSERT_EQ(full_rows.size(), 3U);
  EXPECT_TRUE(std::ranges::any_of(full_rows, [](const Row& row) {
    return row == Row({Value(10), Value()});
  }));
  EXPECT_TRUE(std::ranges::any_of(full_rows, [](const Row& row) {
    return row == Row({Value(20), Value(200)});
  }));
  EXPECT_TRUE(std::ranges::any_of(full_rows, [](const Row& row) {
    return row == Row({Value(), Value(300)});
  }));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineLeftJoinReducesToInnerOnNullRejectingWhere) {
  // A WHERE conjunct that cannot survive a NULL-padded right side proves the
  // padded rows never reach the result, so the LEFT JOIN collapses to an
  // inner join. Results must equal the plain outer join filtered afterwards.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200);");

  std::vector<Row> reduced =
      RunSql(ctx, *db_,
             "SELECT a.v FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "WHERE b.w > 500 ORDER BY a.v;");
  EXPECT_TRUE(reduced.empty());

  std::vector<Row> matched =
      RunSql(ctx, *db_,
             "SELECT a.v FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "WHERE b.k = 2 ORDER BY a.v;");
  ASSERT_EQ(matched.size(), 1U);
  EXPECT_EQ(matched[0][0], Value(20));

  ctx.txn_.Abort();
}

// A WHERE conjunct over the preserved (left) side filters rows before the
// join while unmatched left rows still NULL-pad.
TEST_F(QueryTest, SqlEngineLeftJoinPushesLeftSideFilter) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20), (4, 40);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "WHERE a.v >= 10 AND a.v < 40 ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(10));
  EXPECT_EQ(rows[1][0], Value(20));

  ctx.txn_.Abort();
}

// M6: a plain LEFT JOIN plans through the Cascades memo (outer hash/merge
// alternatives) instead of the relational fallback, preserving NULL padding.
TEST_F(QueryTest, SqlEngineLeftJoinCascadesPlansOuterJoin) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b "
                     "ON a.k = b.k ORDER BY a.v;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  // Fully-relational plans render as "Relational Physical Plan"; a
  // Cascades-native plan never contains that marker (nor the opaque
  // kRelational fallback node).
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(10), Value()}));
  EXPECT_EQ(rows[1], Row({Value(20), Value(200)}));

  ctx.txn_.Abort();
}

// M6 ON-vs-WHERE contract: ON decides the match (before padding), WHERE
// filters the joined rows (after padding). A left-only ON keeps every left
// row; the same predicate in WHERE drops rows. A right-only ON filters the
// build side before matching; WHERE over the null-supplying side observes
// padded NULLs.
TEST_F(QueryTest, SqlEngineLeftJoinOnVsWhereSemantics) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200), (3, 5);");

  // Left-only ON: the v = 30 row fails its ON (30 < 25 is false) yet
  // survives padded; the same predicate in WHERE would drop the row.
  std::vector<Row> on_left = RunSql(
      ctx, *db_,
      "SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b ON a.k = b.k AND a.v < 25 "
      "ORDER BY a.v;");
  ASSERT_EQ(on_left.size(), 3U);
  EXPECT_EQ(on_left[0], Row({Value(10), Value()}));
  EXPECT_EQ(on_left[1], Row({Value(20), Value(200)}));
  EXPECT_EQ(on_left[2], Row({Value(30), Value()}));

  // Same predicate in WHERE: only rows with v > 15 survive.
  std::vector<Row> where_left =
      RunSql(ctx, *db_,
             "SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "WHERE a.v > 15 ORDER BY a.v;");
  ASSERT_EQ(where_left.size(), 2U);
  EXPECT_EQ(where_left[0], Row({Value(20), Value(200)}));
  EXPECT_EQ(where_left[1], Row({Value(30), Value(5)}));

  // Right-only ON: build rows with w <= 100 never match; their left rows pad.
  std::vector<Row> on_right = RunSql(
      ctx, *db_,
      "SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b ON a.k = b.k AND b.w > 100 "
      "ORDER BY a.v;");
  ASSERT_EQ(on_right.size(), 3U);
  EXPECT_EQ(on_right[0], Row({Value(10), Value()}));
  EXPECT_EQ(on_right[1], Row({Value(20), Value(200)}));
  EXPECT_EQ(on_right[2], Row({Value(30), Value()}));

  // WHERE over the null-supplying side observes padded NULLs.
  std::vector<Row> where_null =
      RunSql(ctx, *db_,
             "SELECT a.v FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "WHERE b.w IS NULL ORDER BY a.v;");
  ASSERT_EQ(where_null.size(), 1U);
  EXPECT_EQ(where_null[0][0], Value(10));

  ctx.txn_.Abort();
}

// M6: an empty build side pads every preserved row (no match is still a row).
TEST_F(QueryTest, SqlEngineLeftJoinEmptyInnerSidePadsAll) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(10), Value()}));
  EXPECT_EQ(rows[1], Row({Value(20), Value()}));

  ctx.txn_.Abort();
}

// M6: ORDER BY + LIMIT compose above the outer join through the engine's
// Sort/Limit safety net.
TEST_F(QueryTest, SqlEngineLeftJoinWithOrderAndLimit) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v, b.w FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
             "ORDER BY a.v DESC LIMIT 2;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(30), Value()}));
  EXPECT_EQ(rows[1], Row({Value(20), Value(200)}));

  ctx.txn_.Abort();
}

// M-union: UNION ALL branches over one table with disjoint equality
// predicates merge into a single OR-filtered scan (bag-identical).
TEST_F(QueryTest, SqlEngineUnionAllDisjointMergesToSingleScan) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN SELECT k FROM t WHERE k = 1 UNION ALL SELECT k FROM t "
      "WHERE k = 2 ORDER BY k;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_NE(plan_text.find("IN (1, 2)"), std::string::npos) << plan_text;

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT k FROM t WHERE k = 1 UNION ALL SELECT k FROM t WHERE k = 2 "
      "ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));

  ctx.txn_.Abort();
}

// M-union: UNION DISTINCT merges regardless of overlap (dedup absorbs it).
TEST_F(QueryTest, SqlEngineUnionDistinctMergesToSingleScan) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT k FROM t WHERE k >= 2 UNION DISTINCT SELECT k FROM t WHERE "
      "k <= 2 ORDER BY k;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[2][0], Value(3));

  ctx.txn_.Abort();
}

// M-union negative shapes keep the branch-wise path: overlapping UNION ALL
// branches must preserve duplicates, and grouped branches keep grouping.
TEST_F(QueryTest, SqlEngineUnionOverlappingAndGroupedStayCorrect) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  // Overlapping predicates: k = 2 appears twice (bag semantics).
  std::vector<Row> overlap = RunSql(
      ctx, *db_,
      "SELECT k FROM t WHERE k >= 2 UNION ALL SELECT k FROM t WHERE k <= 2 "
      "ORDER BY k;");
  ASSERT_EQ(overlap.size(), 4U);
  EXPECT_EQ(overlap[0][0], Value(1));
  EXPECT_EQ(overlap[1][0], Value(2));
  EXPECT_EQ(overlap[2][0], Value(2));
  EXPECT_EQ(overlap[3][0], Value(3));

  // Grouped branch: aggregation boundary preserved.
  std::vector<Row> grouped = RunSql(
      ctx, *db_,
      "SELECT k, SUM(v) AS s FROM t GROUP BY k UNION ALL SELECT k, v FROM t "
      "ORDER BY k;");
  ASSERT_EQ(grouped.size(), 6U);

  ctx.txn_.Abort();
}

// M5: a single-table uncorrelated FROM-subquery flattens into the outer
// query and plans through Cascades (join ordering, access paths apply).
TEST_F(QueryTest, SqlEngineDerivedTableFlattensToCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (2, 200), (3, 300);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN SELECT s.v, u.w FROM (SELECT k, v FROM t WHERE v >= 20) AS s "
      "JOIN u ON s.k = u.k ORDER BY s.v;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  // Fully-relational plans render as "Relational Physical Plan"; a
  // Cascades-native plan never contains that marker (nor the opaque
  // kRelational fallback node).
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT s.v, u.w FROM (SELECT k, v FROM t WHERE v >= 20) AS s "
             "JOIN u ON s.k = u.k ORDER BY s.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(20), Value(200)}));
  EXPECT_EQ(rows[1], Row({Value(30), Value(300)}));

  ctx.txn_.Abort();
}

// M5: computed (immutable) derived outputs rebind wherever the outer query
// names them, including the WHERE clause and ORDER BY.
TEST_F(QueryTest, SqlEngineDerivedTableComputedOutputs) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT s.y FROM (SELECT k, v + 1 AS y FROM t WHERE v >= 20) AS s "
             "WHERE s.y >= 21 ORDER BY s.y;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(21));
  EXPECT_EQ(rows[1][0], Value(31));

  ctx.txn_.Abort();
}

// ORDER BY a SELECT-list alias routes the ordering to the post-plan
// SortExecutor (the optimizer never learns it), so the LIMIT must stay
// hidden from the optimizer as DISTINCT already does: a visible LIMIT folds
// a bare kLimit straight onto the scan and truncates the input before the
// sort reads it, answering from the table's first rows instead of its
// smallest ones.
TEST_F(QueryTest, SqlEngineOrderByAliasKeepsLimitAboveTheSort) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (x INT64, z INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1,2),(3,4),(5,6),(0,7),(2,8);");

  std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT x + 1 AS y, z FROM t ORDER BY y LIMIT 3;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
  EXPECT_EQ(rows[2][0], Value(3));
  // OFFSET rides the same hidden-LIMIT gate.
  rows = RunSql(ctx, *db_,
                "SELECT x + 1 AS y FROM t ORDER BY y LIMIT 2 OFFSET 1;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[1][0], Value(3));
  // Without the LIMIT every row is ordered by the same alias.
  rows = RunSql(ctx, *db_, "SELECT x + 1 AS y FROM t ORDER BY y;");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[4][0], Value(6));
  // Control: the computed (non-alias) spelling keeps its plan-side ordering,
  // so the optimizer pushes the TopN down and still answers correctly.
  rows = RunSql(ctx, *db_, "SELECT x + 1 AS y FROM t ORDER BY x + 1 LIMIT 3;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[2][0], Value(3));

  ctx.txn_.Abort();
}

// M5: an inner `*` expands over the base schema before flattening. (A star
// in the OUTER select list keeps the dedicated expansion paths, including
// the lone-star value-table wrapper, so it is out of scope in this slice.)
TEST_F(QueryTest, SqlEngineDerivedTableInnerStarExpansion) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT s.k, s.kk FROM (SELECT *, k AS kk FROM t WHERE v >= 20) AS s "
      "ORDER BY s.k;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(2), Value(2)}));

  ctx.txn_.Abort();
}

// M5: an inner WHERE under a LEFT JOIN rides the ON condition, so rows the
// derived table filters out still NULL-pad instead of vanishing.
TEST_F(QueryTest, SqlEngineDerivedTableUnderLeftJoinKeepsPadding) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (1, 100), (2, 5);");

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT a.v, s.w FROM l AS a LEFT JOIN (SELECT k, w FROM r WHERE w > 50) "
      "AS s ON a.k = s.k ORDER BY a.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(20), Value()}));

  ctx.txn_.Abort();
}

// M5 negative shapes stay correct on the relational path: aggregation
// inside the derived table, and a correlated reference to the outer query.
TEST_F(QueryTest, SqlEngineDerivedTableFallbackShapesStayCorrect) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (1, 20), (2, 30);");

  std::vector<Row> grouped = RunSql(
      ctx, *db_,
      "SELECT s.k, s.total FROM (SELECT k, SUM(v) AS total FROM t GROUP BY k) "
      "AS s ORDER BY s.k;");
  ASSERT_EQ(grouped.size(), 2U);
  EXPECT_EQ(grouped[0], Row({Value(1), Value(30)}));
  EXPECT_EQ(grouped[1], Row({Value(2), Value(30)}));

  std::vector<Row> correlated =
      RunSql(ctx, *db_,
             "SELECT a.v FROM t AS a WHERE a.v IN (SELECT v FROM t AS b "
             "WHERE b.k = a.k) ORDER BY a.v;");
  ASSERT_EQ(correlated.size(), 3U);
  EXPECT_EQ(correlated[0][0], Value(10));

  ctx.txn_.Abort();
}

// M4 follow-up: a CTE that stays shared cannot take one consumer's filter --
// the cell is built once and every reference reads it -- but it can take the
// *disjunction* of every site's restriction: that keeps each row any site can
// emit and drops the rest before the cell is built.  The estimate gates it, so
// a restriction that keeps every row is not pushed.
TEST_F(QueryTest, SqlEnginePushesSharedCteRestrictionsDisjunctively) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE big (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO big SELECT k, k * 10 FROM UNNEST(GENERATE_SERIES(0, 63)) "
         "AS k;");
  RunSql(ctx, *db_, "ANALYZE big;");
  SqlEngine engine(*db_);
  const auto run = [&](std::string_view sql) {
    std::vector<Row> rows;
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
    if (!prepared.HasValue()) {
      return rows;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return rows;
  };

  // Both sites restrict the same column to the same value.
  std::vector<Row> rows =
      run("WITH c AS (SELECT k, v FROM big) "
          "SELECT x.v + y.v FROM c x JOIN c y ON x.k = y.k "
          "WHERE x.k = 1 AND y.k = 1;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(20));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 1U);

  // Different restrictions: the cell keeps the union of both sides (k = 1 or
  // k = 2), so x still sees k = 1 and y still sees k = 2.  A conjunction
  // would leave the cell empty and the join with no row at all.
  rows =
      run("WITH c AS (SELECT k, v FROM big) "
          "SELECT x.v, y.v FROM c x JOIN c y ON x.k + 1 = y.k "
          "WHERE x.k = 1 AND y.k = 2;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(10));
  EXPECT_EQ(rows[0][1], Value(20));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 1U);

  // Restriction that keeps every row: the estimate says no reduction, so the
  // cell stays unfiltered and the counter stays still.
  rows =
      run("WITH c AS (SELECT k, v FROM big) "
          "SELECT COUNT(*) FROM c x JOIN c y ON x.k = y.k "
          "WHERE x.k >= 0 AND y.k >= 0;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(64));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 0U);

  // One site unrestricted: the disjunction is TRUE, so nothing is pushed.
  rows =
      run("WITH c AS (SELECT k, v FROM big) "
          "SELECT x.v + y.v FROM c x JOIN c y ON x.k = y.k WHERE x.k = 1;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(20));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 0U);

  // Sites inside subqueries are not read: reading a WHERE across a scope
  // boundary is what the pushdown deliberately does not do.
  rows =
      run("WITH c AS (SELECT k, v FROM big) "
          "SELECT (SELECT COUNT(*) FROM c WHERE c.k = 1) + "
          "(SELECT COUNT(*) FROM c WHERE c.k = 2);");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 0U);

  // A LIMIT pins the CTE's content, so a filter may not move above it.
  rows =
      run("WITH c AS (SELECT k, v FROM big LIMIT 10) "
          "SELECT COUNT(*) FROM c x JOIN c y ON x.k = y.k "
          "WHERE x.k = 1 AND y.k = 1;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 0U);

  // A body may read a base table that shares the entry's name: a self-named
  // source is not visible inside its own definition, so it is somebody else's
  // relation and must not count as a reference site (it would look
  // unrestricted and block the pushdown).
  RunSql(ctx, *db_, "CREATE TABLE c (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO c VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "ANALYZE c;");
  rows =
      run("WITH c AS (SELECT k, v FROM c) "
          "SELECT x.v + y.v FROM c x JOIN c y ON x.k = y.k "
          "WHERE x.k = 1 AND y.k = 1;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(20));
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 1U);

  // The rewrite has to land before the cell is built: the shared cell then
  // feeds both references at 1 row (k = 1) instead of 64, which the per-leaf
  // scan statistics show.
  const auto plan_text = [&](std::string_view sql) {
    std::string text;
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
    if (!prepared.HasValue()) {
      return text;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      text += row[0].AsString() + "\n";
    }
    return text;
  };
  const std::string pushed = plan_text(
      "EXPLAIN ANALYZE WITH c AS (SELECT k, v FROM big) "
      "SELECT x.v + y.v FROM c x JOIN c y ON x.k = y.k "
      "WHERE x.k = 1 AND y.k = 1;");
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 1U) << pushed;
  EXPECT_EQ(pushed.find("rows_evaluated=64"), std::string::npos) << pushed;
  const std::string unfiltered = plan_text(
      "EXPLAIN ANALYZE WITH c AS (SELECT k, v FROM big) "
      "SELECT x.v + y.v FROM c x JOIN c y ON x.k = y.k "
      "WHERE x.k >= 0 AND y.k >= 0;");
  EXPECT_EQ(engine.LastCteRestrictionPushdowns(), 0U) << unfiltered;
  EXPECT_NE(unfiltered.find("rows_evaluated=64"), std::string::npos)
      << unfiltered;
  ctx.txn_.Abort();
}

// M4: a singly-referenced non-recursive CTE inlines into a derived source
// (and flattens further through M5), reaching Cascades with the same rows.
TEST_F(QueryTest, SqlEngineSingleUseCteInlinesToCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (2, 200), (3, 300);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN WITH c AS (SELECT k, v FROM t WHERE v >= 20) "
      "SELECT c.v, u.w FROM c JOIN u ON c.k = u.k ORDER BY c.v;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  // Fully-relational plans render as "Relational Physical Plan"; a
  // Cascades-native plan never contains that marker (nor the opaque
  // kRelational fallback node).
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM t WHERE v >= 20) "
             "SELECT c.v, u.w FROM c JOIN u ON c.k = u.k ORDER BY c.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(20), Value(200)}));
  EXPECT_EQ(rows[1], Row({Value(30), Value(300)}));

  ctx.txn_.Abort();
}

// M4: chained CTEs inline inside-out (c1 into c2, then c2 into the outer
// query) when each has a single reference.
TEST_F(QueryTest, SqlEngineChainedCteInlinesInsideOut) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH a AS (SELECT k, v FROM t WHERE v >= 20), "
             "b AS (SELECT k, v FROM a WHERE k >= 3) "
             "SELECT b.v FROM b ORDER BY b.v;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(30));

  ctx.txn_.Abort();
}

// M4/M5: a derived output used as a nested-field scope (`st.y` where `st`
// is an output, not a relation) must not flatten: substituting the alias
// away would orphan the qualifier. The query keeps the existing
// (relational) path with identical rows.
TEST_F(QueryTest, SqlEngineDerivedOutputAsScopeStaysCorrect) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN WITH c AS (SELECT STRUCT(v AS y) AS st FROM t WHERE k = 2) "
      "SELECT st.y FROM c;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_NE(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT STRUCT(v AS y) AS st FROM t WHERE k = 2) "
             "SELECT st.y FROM c;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(20));

  ctx.txn_.Abort();
}

// M4 negative shapes keep the materialized path with identical rows: a
// multiply-referenced CTE (shared rescan, not re-evaluation per inline)
// and a recursive CTE.
TEST_F(QueryTest, SqlEngineMultiUseAndRecursiveCteStayCorrect) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");

  std::vector<Row> shared = RunSql(
      ctx, *db_,
      "WITH c AS (SELECT k, v FROM t) "
      "SELECT x.v, y.v FROM c AS x JOIN c AS y ON x.k = y.k ORDER BY x.v;");
  ASSERT_EQ(shared.size(), 2U);
  EXPECT_EQ(shared[0], Row({Value(10), Value(10)}));
  EXPECT_EQ(shared[1], Row({Value(20), Value(20)}));

  std::vector<Row> rec =
      RunSql(ctx, *db_,
             "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r "
             "WHERE n < 3) SELECT n FROM r ORDER BY n;");
  ASSERT_EQ(rec.size(), 3U);
  EXPECT_EQ(rec[0][0], Value(1));
  EXPECT_EQ(rec[2][0], Value(3));

  ctx.txn_.Abort();
}

// M4 materialization: a multiply-referenced small CTE lifts into one shared
// eager cell scanned from every site (kValues leaves, no CTE rescan nodes).
TEST_F(QueryTest, SqlEngineMultiUseCteMaterializesOnceToCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN WITH c AS (SELECT k, v FROM t WHERE v >= 10) "
      "SELECT x.v, y.v FROM c AS x JOIN c AS y ON x.k = y.k ORDER BY x.v;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("CteOrMissingScan"), std::string::npos) << plan_text;

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "WITH c AS (SELECT k, v FROM t WHERE v >= 10) "
      "SELECT x.v, y.v FROM c AS x JOIN c AS y ON x.k = y.k ORDER BY x.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(10)}));
  EXPECT_EQ(rows[1], Row({Value(20), Value(20)}));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, LargeCteAvoidsDiscardedSpeculativeExecution) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE cte_cost(k INT64, v INT64)");
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable("cte_cost"));
  for (int64_t i = 0; i < 2048; ++i) {
    ASSERT_SUCCESS(
        table->Insert(ctx.txn_, Row({Value(i), Value(i * 2)})).GetStatus());
  }
  ASSERT_SUCCESS(db_->RefreshStatistics(ctx, "cte_cost"));
  SqlEngine engine(*db_);
  for (const bool bounded : {false, true}) {
    const std::string sql =
        "WITH c AS (SELECT k, SUM(v) AS v FROM cte_cost GROUP BY k" +
        std::string(bounded ? " ORDER BY k LIMIT 3" : "") +
        ") SELECT x.k,x.v,y.v FROM c x JOIN c y ON x.k=y.k ORDER BY x.k";
    ASSIGN_OR_ASSERT_FAIL(Executor, prepared, engine.Prepare(ctx, sql));
    EXPECT_EQ(engine.LastCteMaterializationAttempts(), bounded ? 1U : 0U);
    DataChunk chunk;
    size_t count = 0;
    while (prepared->NextBatch(&chunk) != 0) {
      for (size_t r = 0; r < chunk.Size(); ++r) {
        const auto row = chunk.RowAt(r);
        const auto key = static_cast<int64_t>(count++);
        EXPECT_EQ(row, Row({Value(key), Value(key * 2), Value(key * 2)}));
      }
    }
    EXPECT_EQ(count, bounded ? 3U : 2048U);
    ASSERT_SUCCESS(prepared->GetStatus());
  }
  ctx.Abort();
}

// M4 CTE predicate pushdown : the shared cell is narrowed
// by the weakening over every reference site (here v >= min(20, 10) = 10);
// each site keeps its own filter as the residual.
TEST_F(QueryTest, SqlEngineMultiSiteCtePushesWeakenedFilter) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN WITH c AS (SELECT k, v FROM t) "
                     "SELECT x.v FROM c AS x JOIN c AS y ON x.k = y.k "
                     "WHERE x.v >= 20 AND y.v >= 10 ORDER BY x.v;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  // The lifted path reads the eager cell through a Values leaf, not the
  // map-scoped rescan path.
  EXPECT_NE(plan_text.find("Values"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("CteOrMissingScan"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM t) "
             "SELECT x.v FROM c AS x JOIN c AS y ON x.k = y.k "
             "WHERE x.v >= 20 AND y.v >= 10 ORDER BY x.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(20));
  EXPECT_EQ(rows[1][0], Value(30));

  // Unanimous equalities narrow to the equality itself.
  std::vector<Row> eq_rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM t) "
             "SELECT x.v FROM c AS x JOIN c AS y ON x.k = y.k "
             "WHERE x.v = 20 AND y.v = 20 ORDER BY x.v;");
  ASSERT_EQ(eq_rows.size(), 1U);
  EXPECT_EQ(eq_rows[0][0], Value(20));

  ctx.txn_.Abort();
}

// Multi-site CTE narrowing must weaken to the WEAKEST bound per direction
// (min lower, MAX upper): the shared cell feeds sites whose filters admit
// different ranges, and a row any site needs must survive the injected
// body predicate.  A narrower site's upper bound must not drop rows that
// the wider site still joins against surviving partners.
TEST_F(QueryTest, SqlEngineMultiSiteCteUpperBoundStaysWeakest) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 28), (1, 20), (2, 5);");

  SqlEngine engine(*db_);
  // Site x admits v <= 28, site y admits v <= 20.  The narrow (y) bound
  // must not shrink the shared body: x's row (1, 28) still joins y's
  // surviving (1, 20) partner.
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM t) "
             "SELECT x.v, y.v FROM c AS x JOIN c AS y ON x.k = y.k "
             "WHERE x.v <= 28 AND y.v <= 20 ORDER BY x.v, y.v;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(5));
  EXPECT_EQ(rows[0][1], Value(5));
  EXPECT_EQ(rows[1][0], Value(20));
  EXPECT_EQ(rows[1][1], Value(20));
  EXPECT_EQ(rows[2][0], Value(28));
  EXPECT_EQ(rows[2][1], Value(20));

  // Same shape with the bounds swapped between the aliases.
  std::vector<Row> swapped =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM t) "
             "SELECT x.v, y.v FROM c AS x JOIN c AS y ON x.k = y.k "
             "WHERE x.v <= 20 AND y.v <= 28 ORDER BY x.v, y.v;");
  ASSERT_EQ(swapped.size(), 3U);
  EXPECT_EQ(swapped[0][0], Value(5));
  EXPECT_EQ(swapped[0][1], Value(5));
  EXPECT_EQ(swapped[1][0], Value(20));
  EXPECT_EQ(swapped[1][1], Value(20));
  EXPECT_EQ(swapped[2][0], Value(20));
  EXPECT_EQ(swapped[2][1], Value(28));

  ctx.txn_.Abort();
}

// M4 shared-cell safety: a multiply-referenced CTE never takes a
// site-specific filter into its cell; every site observes complete rows.
TEST_F(QueryTest, SqlEngineMultiUseCteCellIgnoresSiteSpecificFilter) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM t) "
             "SELECT x.v, y.v FROM c AS x JOIN c AS y ON x.k = y.k "
             "WHERE x.v >= 20 ORDER BY x.v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(20), Value(20)}));
  EXPECT_EQ(rows[1], Row({Value(30), Value(30)}));

  ctx.txn_.Abort();
}

// M4 pushdown boundary: grouped bodies keep their groups (a filter on an
// output column must not move below the aggregation), with identical rows.
TEST_F(QueryTest, SqlEngineGroupedCteBodyKeepsGroupsUnderFilter) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 20), (4, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT v, COUNT(*) AS n FROM t GROUP BY v) "
             "SELECT v, n FROM c WHERE v >= 20 ORDER BY v;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(20), Value(2)}));
  EXPECT_EQ(rows[1], Row({Value(30), Value(1)}));

  ctx.txn_.Abort();
}

// M4 pushdown firing proof: without narrowing, the 2000-row body would
// overflow the 1024-row cell budget and fall back to the rescan path; the
// weakened filter (v >= 1990) keeps the shared cell at 10 rows so the plan
// reads the eager cell through a Values leaf.
TEST_F(QueryTest, SqlEngineWeakenedCteFilterFitsCellBudget) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE big (k INT64, v INT64);");
  std::string insert = "INSERT INTO big VALUES ";
  insert.reserve(2000 * 8);
  for (int i = 0; i < 2000; ++i) {
    insert += (i == 0 ? "(" : ",(") + std::to_string(i) + "," +
              std::to_string(i) + ")";
  }
  insert += ";";
  RunSql(ctx, *db_, insert);

  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN WITH c AS (SELECT k, v FROM big) "
                     "SELECT x.v FROM c AS x JOIN c AS y ON x.k = y.k "
                     "WHERE x.v >= 1990 AND y.v >= 1990 ORDER BY x.v;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_NE(plan_text.find("Values"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("CteOrMissingScan"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k, v FROM big) "
             "SELECT x.v FROM c AS x JOIN c AS y ON x.k = y.k "
             "WHERE x.v >= 1990 AND y.v >= 1990 ORDER BY x.v;");
  ASSERT_EQ(rows.size(), 10U);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(rows[static_cast<size_t>(i)][0], Value(1990 + i));
  }

  ctx.txn_.Abort();
}

// M5+ LATERAL decorrelation : a single-table equality
// LATERAL becomes a decorrelated hash join; per-row lateral evaluation and
// the join agree row-for-row (t=3 matches nothing and drops out).
TEST_F(QueryTest, SqlEngineSingleTableLateralDecorrelatesToJoin) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (id INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (uid INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_,
         "INSERT INTO u VALUES (1, 100), (1, 101), (2, 200), (4, 400);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT t.v, d.w FROM t, "
                     "(SELECT u.uid, u.w FROM u WHERE u.uid = t.id) AS d "
                     "ORDER BY t.v, d.w;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  // Decorrelated: a hash join, with no per-row Apply left.
  EXPECT_NE(plan_text.find("HashJoin"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("ApplyPlan"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT t.v, d.w FROM t, "
             "(SELECT u.uid, u.w FROM u WHERE u.uid = t.id) AS d "
             "ORDER BY t.v, d.w;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(10), Value(101)}));
  EXPECT_EQ(rows[2], Row({Value(20), Value(200)}));

  ctx.txn_.Abort();
}

// LEFT LATERAL keeps unmatched outer rows null-padded through decorrelation.
TEST_F(QueryTest, SqlEngineLeftLateralDecorrelatesWithNullPadding) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (id INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (uid INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_,
         "INSERT INTO u VALUES (1, 100), (1, 101), (2, 200), (4, 400);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT t.v, d.w FROM t LEFT JOIN "
             "(SELECT u.uid, u.w FROM u WHERE u.uid = t.id) AS d ON t.v > 0 "
             "ORDER BY t.v, d.w;");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(10), Value(101)}));
  EXPECT_EQ(rows[2], Row({Value(20), Value(200)}));
  EXPECT_EQ(rows[3], Row({Value(30), Value()}));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, LateralSelectStarAndCrossTypeJoinPreserveExactSemantics) {
  // M7 (`cpp_lateral_decorrelate_star_phantom_col_bug`):
  // `SELECT *` over a correlated LATERAL subquery that does NOT project its
  // correlation key must not leak a phantom correlation column.
  // M5b (`cpp_cross_type_int64_double_nontransitive_bug`):
  // `INT64` `2^53 + 1` (`9007199254740993`) equals `DOUBLE` `2^53`
  // (`9007199254740992.0`); inequality/constant inference must not drop the
  // matching row across mismatched catalog types.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (id INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (uid INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (1, 100), (2, 200);");

  std::vector<Row> star_rows =
      RunSql(ctx, *db_,
             "SELECT * FROM t, LATERAL (SELECT u.w FROM u WHERE u.uid = t.id) "
             "AS d ORDER BY t.id;");
  ASSERT_EQ(star_rows.size(), 2U);
  EXPECT_EQ(star_rows[0], Row({Value(1), Value(10), Value(100)}));
  EXPECT_EQ(star_rows[1], Row({Value(2), Value(20), Value(200)}));

  RunSql(ctx, *db_, "CREATE TABLE ti (i INT64);");
  RunSql(ctx, *db_, "CREATE TABLE td (d DOUBLE);");
  RunSql(ctx, *db_, "INSERT INTO ti VALUES (9007199254740993);");
  RunSql(ctx, *db_, "INSERT INTO td VALUES (9007199254740992.0);");

  std::vector<Row> cross_ineq =
      RunSql(ctx, *db_,
             "SELECT ti.i FROM ti JOIN td ON ti.i = td.d "
             "WHERE ti.i > 9007199254740992;");
  ASSERT_EQ(cross_ineq.size(), 1U);
  EXPECT_EQ(cross_ineq[0], Row({Value(int64_t{9007199254740993LL})}));

  std::vector<Row> cross_eq =
      RunSql(ctx, *db_,
             "SELECT ti.i FROM td JOIN ti ON td.d = ti.i "
             "WHERE td.d = 9007199254740992;");
  ASSERT_EQ(cross_eq.size(), 1U);
  EXPECT_EQ(cross_eq[0], Row({Value(int64_t{9007199254740993LL})}));

  ctx.txn_.Abort();
}

// Non-equality correlations and aggregate bodies stay on the lateral path
// with identical rows.
TEST_F(QueryTest, SqlEngineNonEquiAndAggregateLateralStayCorrect) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (id INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (uid INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (1, 100), (2, 200);");

  std::vector<Row> range_rows =
      RunSql(ctx, *db_,
             "SELECT t.v, d.w FROM t, "
             "(SELECT u.uid, u.w FROM u WHERE u.uid > t.id) AS d "
             "ORDER BY t.v, d.w;");
  ASSERT_EQ(range_rows.size(), 1U);
  EXPECT_EQ(range_rows[0], Row({Value(10), Value(200)}));

  std::vector<Row> agg_rows =
      RunSql(ctx, *db_,
             "SELECT t.v, d.n FROM t, "
             "(SELECT COUNT(*) AS n FROM u WHERE u.uid = t.id) AS d "
             "ORDER BY t.v;");
  ASSERT_EQ(agg_rows.size(), 2U);
  EXPECT_EQ(agg_rows[0], Row({Value(10), Value(1)}));
  EXPECT_EQ(agg_rows[1], Row({Value(20), Value(1)}));

  ctx.txn_.Abort();
}

// An UNCORRELATED LATERAL query yields the same element set for every
// prefix row, so RIGHT/FULL unmatched element rows must be emitted once
// globally — not once per prefix row (and not at all for an empty prefix).
TEST_F(QueryTest, SqlEngineUncorrelatedLateralRightFullJoinEmitsOnce) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (id INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1), (2);");

  std::vector<Row> right_rows =
      RunSql(ctx, *db_,
             "SELECT * FROM t RIGHT JOIN LATERAL "
             "(SELECT 10 AS x UNION ALL SELECT 20 AS x) d ON t.id = d.x - 9 "
             "ORDER BY 1 NULLS FIRST, 2 NULLS FIRST;");
  ASSERT_EQ(right_rows.size(), 2U);
  EXPECT_EQ(right_rows[0], Row({Value(), Value(20)}));
  EXPECT_EQ(right_rows[1], Row({Value(1), Value(10)}));

  std::vector<Row> full_rows =
      RunSql(ctx, *db_,
             "SELECT * FROM t FULL JOIN LATERAL "
             "(SELECT 10 AS x UNION ALL SELECT 99 AS x) d ON t.id = d.x - 9 "
             "ORDER BY 1 NULLS FIRST, 2 NULLS FIRST;");
  ASSERT_EQ(full_rows.size(), 3U);
  EXPECT_EQ(full_rows[0], Row({Value(), Value(99)}));
  EXPECT_EQ(full_rows[1], Row({Value(1), Value(10)}));
  EXPECT_EQ(full_rows[2], Row({Value(2), Value()}));

  // Empty prefix still preserves the right side.
  std::vector<Row> empty_rows =
      RunSql(ctx, *db_,
             "SELECT * FROM (SELECT 1 AS id WHERE false) e RIGHT JOIN "
             "LATERAL (SELECT 10 AS x) d ON e.id = d.x;");
  ASSERT_EQ(empty_rows.size(), 1U);
  EXPECT_EQ(empty_rows[0], Row({Value(), Value(10)}));

  // A CORRELATED lateral keeps per-expansion unmatched emission.
  std::vector<Row> corr_rows =
      RunSql(ctx, *db_,
             "SELECT * FROM t RIGHT JOIN LATERAL (SELECT t.id * 10 AS x) d "
             "ON t.id = d.x ORDER BY 2;");
  ASSERT_EQ(corr_rows.size(), 2U);
  EXPECT_EQ(corr_rows[0], Row({Value(), Value(10)}));
  EXPECT_EQ(corr_rows[1], Row({Value(), Value(20)}));

  ctx.txn_.Abort();
}

// M4 materialization boundary: an over-budget CTE keeps the map-scoped
// rescan path (CteScan) with identical rows.
TEST_F(QueryTest, SqlEngineOverBudgetCteKeepsRescanPath) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t1 (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t2 (k INT64);");
  std::string insert1 = "INSERT INTO t1 VALUES ";
  insert1.reserve(40 * 6);
  std::string insert2 = "INSERT INTO t2 VALUES ";
  insert2.reserve(30 * 6);
  for (int i = 0; i < 40; ++i) {
    insert1 += (i == 0 ? "(" : ",(") + std::to_string(i) + ")";
    if (i < 30) {
      insert2 += (i == 0 ? "(" : ",(") + std::to_string(i) + ")";
    }
  }
  insert1 += ";";
  insert2 += ";";
  RunSql(ctx, *db_, insert1);
  RunSql(ctx, *db_, insert2);

  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN WITH c AS (SELECT t1.k FROM t1 CROSS JOIN t2) "
                     "SELECT COUNT(*) FROM c;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  // 40 x 30 = 1200 rows exceeds the eager cell budget: the CTE stays mapped.
  EXPECT_NE(plan_text.find("CteOrMissingScan"), std::string::npos) << plan_text;

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "WITH c AS (SELECT t1.k FROM t1 CROSS JOIN t2) SELECT COUNT(*) FROM c;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1200));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineSetOperationCteKeepsMapScopedPath) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t1 (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t2 (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO t1 VALUES (1);");
  RunSql(ctx, *db_, "INSERT INTO t2 VALUES (2);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN WITH c AS (SELECT k FROM t1 UNION ALL SELECT k FROM t2) "
      "SELECT a.k + b.k FROM c a, c b WHERE a.k = 1 AND b.k = 2;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row row;
  std::string plan;
  while (explain.Value()->Next(&row, nullptr)) {
    plan += row[0].AsString() + "\n";
  }
  EXPECT_NE(plan.find("CteOrMissingScan"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("CteInline"), std::string::npos) << plan;

  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k FROM t1 UNION ALL SELECT k FROM t2) "
             "SELECT a.k + b.k FROM c a, c b WHERE a.k = 1 AND b.k = 2;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(3));
  ctx.txn_.Abort();
}

// M4 recursive leaf: a singly-referenced recursive CTE plans its outer
// query through Cascades around an opaque worktable leaf (same fixpoint,
// same rows), instead of running the whole statement relationally.
TEST_F(QueryTest, SqlEngineRecursiveCtePlansOuterThroughCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
      "FROM r WHERE n < 3) SELECT n, n + 100 AS m FROM r WHERE n >= 2 "
      "ORDER BY n;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;
  EXPECT_NE(plan_text.find("RecursiveCte"), std::string::npos) << plan_text;

  // Same fixpoint rows as the relational path: 2, 3 filtered and projected.
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
             "FROM r WHERE n < 3) SELECT n, n + 100 AS m FROM r WHERE n >= 2 "
             "ORDER BY n;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(2), Value(102)}));
  EXPECT_EQ(rows[1], Row({Value(3), Value(103)}));

  ctx.txn_.Abort();
}

// ORDER BY over an unnamed computed projection sorts by the projected
// output column (previously the source form evaluated against output rows
// that no longer expose the source columns, yielding no rows).
TEST_F(QueryTest, SqlEngineOrderByUnnamedComputedSortsOutput) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (1, 100), (2, 200), (3, 300);");

  std::vector<Row> single =
      RunSql(ctx, *db_, "SELECT v + 1 FROM t ORDER BY v + 1;");
  ASSERT_EQ(single.size(), 3U);
  EXPECT_EQ(single[0][0], Value(11));
  EXPECT_EQ(single[2][0], Value(31));

  std::vector<Row> dup =
      RunSql(ctx, *db_, "SELECT v + 1, v + 1 FROM t ORDER BY v + 1, v + 1;");
  ASSERT_EQ(dup.size(), 3U);
  EXPECT_EQ(dup[0], Row({Value(11), Value(11)}));

  std::vector<Row> joined =
      RunSql(ctx, *db_,
             "SELECT a.v + 1, b.w FROM t AS a JOIN u AS b ON a.k = b.k "
             "ORDER BY a.v + 1;");
  ASSERT_EQ(joined.size(), 3U);
  EXPECT_EQ(joined[0], Row({Value(11), Value(200)}));
  EXPECT_EQ(joined[2], Row({Value(31), Value(100)}));

  ctx.txn_.Abort();
}

// M-window: ranking functions plan through the Cascades memo (kWindow +
// WindowExecutor delegating to the canonical evaluator) instead of the
// relational pre-computation, with identical row multisets.
TEST_F(QueryTest, SqlEngineWindowRankingPlansThroughCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (2, 25), (3, 30);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN SELECT k, RANK() OVER (PARTITION BY k ORDER BY v) AS rnk "
      "FROM t ORDER BY k, rnk;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_EQ(plan_text.find("Relational Physical Plan"), std::string::npos)
      << plan_text;
  EXPECT_NE(plan_text.find("Window"), std::string::npos) << plan_text;

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT k, RANK() OVER (PARTITION BY k ORDER BY v) AS rnk "
             "FROM t ORDER BY k, rnk;");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(1)}));
  EXPECT_EQ(rows[2], Row({Value(2), Value(2)}));
  EXPECT_EQ(rows[3], Row({Value(3), Value(1)}));

  ctx.txn_.Abort();
}

// M-window: frame aggregation (cumulative SUM) and multi-spec chaining
// (two partition specs lower two kWindow nodes).
TEST_F(QueryTest, SqlEngineWindowFramesAndChainedSpecs) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (1, 20), (2, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT k, SUM(v) OVER (PARTITION BY k ORDER BY v ROWS BETWEEN "
             "UNBOUNDED PRECEDING AND CURRENT ROW) AS running, "
             "ROW_NUMBER() OVER () AS rn FROM t ORDER BY k, v;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(10), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value(30), Value(2)}));
  EXPECT_EQ(rows[2], Row({Value(2), Value(30), Value(3)}));

  ctx.txn_.Abort();
}

// M-window: QUALIFY filters windowed rows above the kWindow nodes.
TEST_F(QueryTest, SqlEngineWindowQualifyFiltersWindowedRows) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (1, 20), (2, 30);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT k, RANK() OVER (PARTITION BY k ORDER BY v) AS rnk "
             "FROM t QUALIFY rnk = 1 ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(1)}));

  ctx.txn_.Abort();
}

// RANGE `1 FOLLOWING .. 2 FOLLOWING` under DESC ordering must open the frame
// on the lower values (the regression made the start bound `>= key - off`,
// which matched the largest keys and collapsed the frame to the partition
// head).
TEST_F(QueryTest, SqlEngineRangeDescendingFollowingStartBound) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, id INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (10,1),(9,2),(8,3),(7,4),(6,5);");

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT id, MIN(id) OVER (ORDER BY k DESC "
      "RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS e FROM t ORDER BY id;");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(2)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(3)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(4)}));
  EXPECT_EQ(rows[3], Row({Value(4), Value(5)}));
  EXPECT_EQ(rows[4], Row({Value(5), Value()}));

  ctx.txn_.Abort();
}

// RANGE `0 PRECEDING` is defined as CURRENT ROW, so the end bound is the last
// row of the peer group, not the current row.
TEST_F(QueryTest, SqlEngineRangeZeroPrecedingEndsAtPeer) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (5),(5);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT SUM(k) OVER (ORDER BY k "
             "RANGE BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING), "
             "SUM(k) OVER (ORDER BY k "
             "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(10)}));
  EXPECT_EQ(rows[1], Row({Value(10), Value(10)}));

  ctx.txn_.Abort();
}

// MERGE of UNION ALL branches must not fold window calls into one shared scan:
// each branch's window is evaluated over its own rows.
TEST_F(QueryTest, SqlEngineUnionMergeKeepsWindowPartitions) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1,30),(2,10),(3,20);");

  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT k, RANK() OVER (ORDER BY v) AS r FROM t WHERE k=1 "
      "UNION ALL "
      "SELECT k, RANK() OVER (ORDER BY v) AS r FROM t WHERE k=2 ORDER BY k;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(1)}));

  ctx.txn_.Abort();
}

// A merged UNION ALL still honors an integer ORDER BY ordinal.
TEST_F(QueryTest, SqlEngineUnionMergeKeepsOrderByOrdinal) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1),(2);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT k FROM t WHERE k=1 UNION ALL SELECT k FROM t WHERE k=2 "
             "ORDER BY 1 DESC LIMIT 1;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(2)}));

  ctx.txn_.Abort();
}

// A derived table on the right of an inner join must have its own ON
// condition rebound through the derived outputs before the alias disappears.
TEST_F(QueryTest, SqlEngineDerivedJoinOnRebindsOutputs) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1,11),(2,22),(3,33);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (1,100),(2,200),(3,300);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT d.x, u.w FROM u JOIN (SELECT k AS x FROM t) d "
             "ON d.x = u.k ORDER BY u.w;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(200)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(300)}));

  ctx.txn_.Abort();
}

// An inner WHERE of a derived table placed on the NULL-supplying left of a
// RIGHT join must ride the join's ON, so padded right rows survive.
TEST_F(QueryTest, SqlEngineDerivedLeftOfRightJoinKeepsPaddedRows) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE u (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1,5),(2,22),(3,33);");
  RunSql(ctx, *db_, "INSERT INTO u VALUES (1,100),(2,200),(3,300);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT d.x, u.w FROM (SELECT k AS x, v FROM t WHERE v > 10) d "
             "RIGHT JOIN u ON d.x = u.k ORDER BY u.w;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(), Value(100)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(200)}));
  EXPECT_EQ(rows[2], Row({Value(3), Value(300)}));

  ctx.txn_.Abort();
}

// Two window calls differing only in the aggregate WHERE filter are distinct
// outputs and must not be deduplicated onto one hidden column.
TEST_F(QueryTest, SqlEngineWindowWhereFilterNotDeduplicated) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t2 (a INT64, x INT64);");
  RunSql(ctx, *db_, "INSERT INTO t2 VALUES (1,10),(-1,20),(2,30),(-2,40);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT SUM(x WHERE a > 0) OVER () AS s_pos, "
             "SUM(x WHERE a < 0) OVER () AS s_neg FROM t2 ORDER BY s_pos;");
  ASSERT_EQ(rows.size(), 4U);
  for (const Row& row : rows) {
    EXPECT_EQ(row, Row({Value(40), Value(60)}));
  }

  ctx.txn_.Abort();
}

// A filter on the NULL-supplying side of a RIGHT/FULL join must not leak into
// the preserved side's scan (it would drop the padded rows the join keeps).
TEST_F(QueryTest, SqlEngineNullSupplyingFilterNotPushed) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE a (x INT64);");
  RunSql(ctx, *db_, "CREATE TABLE b (y INT64);");
  RunSql(ctx, *db_, "INSERT INTO a VALUES (1),(2),(3);");
  RunSql(ctx, *db_, "INSERT INTO b VALUES (2),(4);");

  std::vector<Row> right =
      RunSql(ctx, *db_,
             "SELECT a.x, b.y FROM a RIGHT JOIN b ON a.x = b.y "
             "WHERE a.x IS NULL ORDER BY b.y;");
  ASSERT_EQ(right.size(), 1U);
  EXPECT_EQ(right[0], Row({Value(), Value(4)}));

  std::vector<Row> full =
      RunSql(ctx, *db_,
             "SELECT a.x, b.y FROM a FULL JOIN b ON a.x > b.y "
             "WHERE a.x IS NULL ORDER BY b.y;");
  ASSERT_EQ(full.size(), 1U);
  EXPECT_EQ(full[0], Row({Value(), Value(4)}));

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineOuterNonRejectingFilterContract) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE of_l (id INT64, grp INT64);");
  RunSql(ctx, *db_, "CREATE TABLE of_r (id INT64, val INT64, label STRING);");
  RunSql(ctx, *db_,
         "INSERT INTO of_l VALUES (1,10),(1,11),(2,20),(3,30),(NULL,40);");
  RunSql(ctx, *db_,
         "INSERT INTO of_r VALUES (1,0,'zero'),(1,100,'high'),"
         "(2,NULL,'null'),(3,0,'zero3'),(4,400,'four'),"
         "(NULL,500,'nullkey');");
  RunSql(ctx, *db_, "CREATE TABLE of_empty (id INT64, val INT64);");

  const std::vector<Row> left =
      RunSql(ctx, *db_,
             "SELECT l.id, r.id, r.val FROM of_l l LEFT JOIN of_r r "
             "ON l.id = r.id AND COALESCE(r.val, 0) = 0 ORDER BY l.id, r.val;");
  EXPECT_EQ(
      left,
      (std::vector<Row>{
          Row({Value(), Value(), Value()}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{2}), Value(int64_t{2}), Value()}),
          Row({Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{0})})}));

  const std::vector<Row> right =
      RunSql(ctx, *db_,
             "SELECT l.id, r.id, r.val FROM of_l l RIGHT JOIN of_r r "
             "ON l.id = r.id AND COALESCE(r.val, 0) = 0 ORDER BY r.id, l.id;");
  EXPECT_EQ(right,
            (std::vector<Row>{
                Row({Value(), Value(), Value(int64_t{500})}),
                Row({Value(), Value(int64_t{1}), Value(int64_t{100})}),
                Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
                Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
                Row({Value(int64_t{2}), Value(int64_t{2}), Value()}),
                Row({Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{0})}),
                Row({Value(), Value(int64_t{4}), Value(int64_t{400})})}));

  const std::vector<Row> full =
      RunSql(ctx, *db_,
             "SELECT l.id, r.id, r.val FROM of_l l FULL JOIN of_r r "
             "ON l.id = r.id AND COALESCE(r.val, 0) = 0 ORDER BY l.id, r.id;");
  EXPECT_EQ(
      full,
      (std::vector<Row>{
          Row({Value(), Value(), Value()}),
          Row({Value(), Value(), Value(int64_t{500})}),
          Row({Value(), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(), Value(int64_t{4}), Value(int64_t{400})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{2}), Value(int64_t{2}), Value()}),
          Row({Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{0})})}));

  const std::vector<Row> where =
      RunSql(ctx, *db_,
             "SELECT l.id, r.id, r.val FROM of_l l LEFT JOIN of_r r "
             "ON l.id = r.id WHERE r.val IS NULL OR r.val >= 100 "
             "ORDER BY l.id, r.val;");
  EXPECT_EQ(
      where,
      (std::vector<Row>{
          Row({Value(), Value(), Value()}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(int64_t{2}), Value(int64_t{2}), Value()})}));

  const std::vector<Row> right_strict =
      RunSql(ctx, *db_,
             "SELECT l.id, r.id, r.val FROM of_l l RIGHT JOIN of_r r "
             "ON l.id = r.id WHERE l.id >= 0 ORDER BY r.id, l.id;");
  EXPECT_EQ(
      right_strict,
      (std::vector<Row>{
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(int64_t{2}), Value(int64_t{2}), Value()}),
          Row({Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{0})})}));

  const std::vector<Row> full_strict =
      RunSql(ctx, *db_,
             "SELECT l.id, r.id, r.val FROM of_l l FULL JOIN of_r r "
             "ON l.id = r.id WHERE l.id >= 0 AND r.val >= 0 "
             "ORDER BY l.id, r.id;");
  EXPECT_EQ(
      full_strict,
      (std::vector<Row>{
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{0})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{100})}),
          Row({Value(int64_t{3}), Value(int64_t{3}), Value(int64_t{0})})}));

  const std::vector<Row> boundary =
      RunSql(ctx, *db_,
             "SELECT l.id, r.val FROM of_l l LEFT JOIN of_r r ON l.id = r.id "
             "WHERE r.val >= 0 AND r.val <= 0 ORDER BY l.id;");
  EXPECT_EQ(boundary,
            (std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{0})}),
                              Row({Value(int64_t{1}), Value(int64_t{0})}),
                              Row({Value(int64_t{3}), Value(int64_t{0})})}));

  const std::vector<Row> empty = RunSql(
      ctx, *db_,
      "SELECT l.id, e.val FROM of_l l LEFT JOIN of_empty e ON l.id = e.id "
      "WHERE e.val IS NULL ORDER BY l.id;");
  EXPECT_EQ(empty, (std::vector<Row>{Row({Value(), Value()}),
                                     Row({Value(int64_t{1}), Value()}),
                                     Row({Value(int64_t{1}), Value()}),
                                     Row({Value(int64_t{2}), Value()}),
                                     Row({Value(int64_t{3}), Value()})}));

  const std::vector<Row> aggregate = RunSql(
      ctx, *db_,
      "SELECT l.id, COUNT(*), COUNT(r.val), SUM(r.val) "
      "FROM of_l l LEFT JOIN of_r r ON l.id = r.id "
      "WHERE r.val IS NULL OR r.val >= 100 GROUP BY l.id ORDER BY l.id;");
  EXPECT_EQ(aggregate,
            (std::vector<Row>{
                Row({Value(), Value(int64_t{1}), Value(int64_t{0}), Value()}),
                Row({Value(int64_t{1}), Value(int64_t{2}), Value(int64_t{2}),
                     Value(int64_t{200})}),
                Row({Value(int64_t{2}), Value(int64_t{1}), Value(int64_t{0}),
                     Value()})}));

  const std::vector<Row> limited =
      RunSql(ctx, *db_,
             "SELECT l.id, r.val FROM of_l l LEFT JOIN of_r r ON l.id = r.id "
             "WHERE r.val IS NULL OR r.val >= 100 "
             "ORDER BY l.id DESC, r.val DESC LIMIT 2;");
  EXPECT_EQ(limited,
            (std::vector<Row>{Row({Value(int64_t{2}), Value()}),
                              Row({Value(int64_t{1}), Value(int64_t{100})})}));

  SqlEngine error_engine(*db_);
  StatusOr<Executor> bad =
      error_engine.Prepare(ctx, "SELECT CAST('bad' AS DATE) FROM of_r;");
  if (bad.HasValue()) {
    Row bad_row;
    EXPECT_FALSE(bad.Value()->Next(&bad_row, nullptr));
    EXPECT_NE(bad.Value()->GetStatus(), Status::kSuccess);
  } else {
    EXPECT_NE(bad.GetStatus(), Status::kSuccess);
  }

  const auto run_fallback = [&](bool fallback) {
    SqlEngine engine(*db_);
    engine.SetForceRelationalFallback(fallback);
    StatusOr<Executor> prepared = engine.Prepare(
        ctx,
        "SELECT l.id, r.id, r.val FROM of_l l LEFT JOIN of_r r "
        "ON l.id = r.id AND COALESCE(r.val, 0) = 0 ORDER BY l.id, r.val;");
    std::vector<Row> rows;
    if (!prepared.HasValue()) {
      return rows;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return rows;
  };
  EXPECT_EQ(run_fallback(false), run_fallback(true));
  ctx.txn_.Abort();
}

// A unique index on a nullable column stores NULL-bearing keys in the
// multi-value encoding, so it may hold many NULL rows; DISTINCT must not be
// dropped on the strength of that index.
TEST_F(QueryTest, DistinctKeepsNullableUniqueKey) {
  TransactionContext ctx = db_->BeginContext();
  Schema schema("nullable_unique", {Column("id", ValueType::kInt64,
                                           Constraint(Constraint::kUnique)),
                                    Column("name", ValueType::kVarChar)});
  ASSERT_TRUE(db_->CreateTable(ctx, schema).HasValue());
  RunSql(ctx, *db_,
         "INSERT INTO nullable_unique VALUES (NULL, 'a'), (NULL, 'b');");

  std::vector<Row> rows =
      RunSql(ctx, *db_, "SELECT DISTINCT id FROM nullable_unique;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_TRUE(rows[0][0].IsNull());

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineWindowOverLeftJoinWithAliasOrder) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE l (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE r (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO r VALUES (2, 200);");

  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v, ROW_NUMBER() OVER (ORDER BY a.v) AS rn "
             "FROM l AS a LEFT JOIN r AS b ON a.k = b.k ORDER BY rn;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(10), Value(1)}));
  EXPECT_EQ(rows[2], Row({Value(30), Value(3)}));

  ctx.txn_.Abort();
}

// WITH RECURSIVE: the relational engine evaluates recursive CTEs with a
// work table (anchor pass seeds it; each round binds the CTE name to the
// previous round's rows until nothing new is produced).
TEST_F(QueryTest, SqlEngineRecursiveCteCountsToLimit) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> total =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
             "FROM t WHERE n < 5) SELECT SUM(n) FROM t;");
  ASSERT_EQ(total.size(), 1U);
  EXPECT_EQ(total[0][0], Value(15));

  std::vector<Row> ordered =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 3 AS n UNION ALL SELECT n - 1 FROM t "
             "WHERE n > 0) SELECT n FROM t ORDER BY n;");
  ASSERT_EQ(ordered.size(), 4U);
  EXPECT_EQ(ordered[0][0], Value(0));
  EXPECT_EQ(ordered[3][0], Value(3));
}

TEST_F(QueryTest, SqlEngineRecursiveCteTransitiveClosureWithUnionDistinct) {
  // UNION DISTINCT keeps a seen-row set, so a cycle (2 -> 2) terminates at
  // the transitive closure instead of walking forever.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE edges (src INT64, dst INT64);");
  RunSql(ctx, *db_, "INSERT INTO edges VALUES (1, 2), (2, 3), (3, 4), (2, 2);");
  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "WITH RECURSIVE reach AS (SELECT src, dst FROM edges WHERE src = 1 "
      "UNION DISTINCT SELECT e.src, e.dst FROM edges e JOIN reach r ON "
      "e.src = r.dst) SELECT COUNT(*) FROM reach;");
  ASSERT_EQ(rows.size(), 1U);
  // Closure edges reachable from node 1: (1,2), (2,2), (2,3), (3,4).
  EXPECT_EQ(rows[0][0], Value(4));
  ctx.txn_.Abort();
}

// UNION (DISTINCT) recursion drops a row any earlier round already produced.
// The loop keeps that working-table set in a hash index (WorkTableIndex)
// instead of scanning every row seen so far -- the scan was quadratic (a
// 320k-row recursion spent minutes in it).  The counters prove the loop really
// consults the index; the peer cases prove it keeps the DISTINCT rule while
// doing so, so a recursive UNION DISTINCT and a top-level DISTINCT agree on
// which rows are duplicates.
TEST_F(QueryTest, SqlEngineRecursiveCteDedupesThroughWorkTableIndex) {
  TransactionContext ctx = db_->BeginContext();
  relational_detail::WorkTableIndexCounters& counters =
      relational_detail::WorkTableIndexStats();
  const uint64_t inserts_before = counters.inserts.load();
  const uint64_t duplicates_before = counters.duplicates.load();
  // Three distinct rounds: 1 (anchor), 2, 3.
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS n UNION DISTINCT SELECT n + 1 "
             "FROM t WHERE n < 3) SELECT n FROM t ORDER BY n;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[2][0], Value(3));
  EXPECT_EQ(counters.inserts.load() - inserts_before, 3U);
  EXPECT_EQ(counters.duplicates.load() - duplicates_before, 0U);

  // A term that reproduces its input is a duplicate, not a new round: the
  // index has to stop the recursion after one round instead of walking to the
  // iteration cap.  NaN, -0.0/+0.0 and NULL all have to collapse to one peer.
  const uint64_t peer_inserts_before = counters.inserts.load();
  const uint64_t peer_duplicates_before = counters.duplicates.load();
  rows = RunSql(ctx, *db_,
                "WITH RECURSIVE t AS (SELECT CAST('NaN' AS FLOAT64) AS x "
                "UNION DISTINCT SELECT x FROM t) SELECT COUNT(*) FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  rows = RunSql(ctx, *db_,
                "WITH RECURSIVE t AS (SELECT -0.0 AS x UNION DISTINCT "
                "SELECT x + 0.0 FROM t) SELECT COUNT(*) FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  rows = RunSql(ctx, *db_,
                "WITH RECURSIVE t AS (SELECT CAST(NULL AS INT64) AS x "
                "UNION DISTINCT SELECT x FROM t) SELECT COUNT(*) FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1));
  // Each statement indexed its anchor and its one duplicate round.
  EXPECT_EQ(counters.inserts.load() - peer_inserts_before, 6U);
  EXPECT_EQ(counters.duplicates.load() - peer_duplicates_before, 3U);

  // Wide recursion: every round adds thousands of rows, so the seen set grows
  // to the whole result.  This is the shape that made the linear scan
  // quadratic; keep it as a scale guard.
  rows = RunSql(
      ctx, *db_,
      "WITH RECURSIVE t AS (SELECT 0 AS v UNION DISTINCT SELECT v * 16 + k "
      "FROM t CROSS JOIN UNNEST(GENERATE_SERIES(1, 16)) AS k WHERE v < 20000) "
      "SELECT COUNT(*) FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(320001));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineRecursiveCteMixedWithPlainCte) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 5 AS n UNION ALL SELECT n - 1 "
             "FROM t WHERE n > 1), plain AS (SELECT 100 AS x) "
             "SELECT x + (SELECT MAX(n) FROM t) FROM plain;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(105));
  ctx.txn_.Abort();
}

// UNION DISTINCT BY NAME inside a recursive body aligns each round's output
// on column names, so a term selecting columns in a different order still
// dedupes against the accumulated rows.
TEST_F(QueryTest, SqlEngineRecursiveCteByNameUnionDedupesAlignedRows) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b "
             "UNION DISTINCT BY NAME SELECT b, a FROM t) SELECT * FROM t;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(2)}));
  ctx.txn_.Abort();
}

// Every name-matched union mode binds a recursive term's values by column
// name.  UNION ALL used to ignore BY NAME entirely (the flag was only
// recorded for UNION DISTINCT), so the term stayed positional and the second
// row came out as (100, 200) instead of the aligned (200, 100).
TEST_F(QueryTest, SqlEngineRecursiveCteNameMatchingAlignsEveryUnionMode) {
  TransactionContext ctx = db_->BeginContext();
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b "
                   "UNION ALL BY NAME "
                   "SELECT a * 100 AS b, b * 100 AS a FROM t WHERE a = 1) "
                   "SELECT * FROM t ORDER BY a, b;"),
            (std::vector<Row>{Row({Value(1), Value(2)}),
                              Row({Value(200), Value(100)})}));
  // STRICT CORRESPONDING is BY NAME under its standard-SQL spelling.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH RECURSIVE t AS (SELECT 1 AS x, 2 AS y "
                   "UNION ALL STRICT CORRESPONDING "
                   "SELECT x * 10 AS y, y * 10 AS x FROM t WHERE x = 1) "
                   "SELECT * FROM t ORDER BY x, y;"),
            (std::vector<Row>{Row({Value(1), Value(2)}),
                              Row({Value(20), Value(10)})}));
  // Plain CORRESPONDING matches by name and drops the term's extra columns;
  // the anchor still fixes the CTE's shape.
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b "
             "UNION ALL CORRESPONDING "
             "SELECT 3 AS b, 4 AS a, 5 AS c FROM t WHERE a = 1) "
             "SELECT * FROM t ORDER BY a, b;"),
      (std::vector<Row>{Row({Value(1), Value(2)}), Row({Value(4), Value(3)})}));
  // An ON/BY list keeps only the listed columns as name-match targets; it has
  // to cover the working shape, so here it is BY NAME.
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b "
             "UNION ALL BY NAME ON (a, b) "
             "SELECT 5 AS b, 6 AS a FROM t WHERE a = 1) "
             "SELECT * FROM t ORDER BY a, b;"),
      (std::vector<Row>{Row({Value(1), Value(2)}), Row({Value(6), Value(5)})}));
  ctx.txn_.Abort();
}

// The working shape stays the anchor's even when a term is name matched, so
// WITH DEPTH still appends its counter after the aligned columns.
TEST_F(QueryTest, SqlEngineRecursiveCteNameMatchingKeepsDepthLast) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b UNION ALL BY NAME "
             "SELECT b + 10 AS b, a + 100 AS a FROM t WHERE a < 50) "
             "WITH DEPTH BETWEEN 0 AND 2 SELECT a, b, depth FROM t "
             "ORDER BY depth;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(2), Value(int64_t{0})}));
  EXPECT_EQ(rows[1], Row({Value(101), Value(12), Value(int64_t{1})}));
  ctx.txn_.Abort();
}

// JOIN USING merges its key into one column, and that merged name is what a
// name-matched recursive term aligns on.
TEST_F(QueryTest, SqlEngineRecursiveCteJoinUsingFeedsNameMatching) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE rc_left (k INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE rc_right (k INT64, w INT64);");
  RunSql(ctx, *db_, "INSERT INTO rc_left VALUES (1, 1), (2, 2);");
  RunSql(ctx, *db_, "INSERT INTO rc_right VALUES (1, 10), (2, 20);");
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT l.k, l.v, r.w FROM rc_left l "
             "JOIN rc_right r USING (k) WHERE l.k = 1 "
             "UNION ALL BY NAME SELECT w AS w, k * 10 AS k, v + 1 AS v FROM t "
             "WHERE v < 3) SELECT k, v, w FROM t ORDER BY v;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0],
            Row({Value(int64_t{1}), Value(int64_t{1}), Value(int64_t{10})}));
  EXPECT_EQ(rows[1],
            Row({Value(int64_t{10}), Value(int64_t{2}), Value(int64_t{10})}));
  EXPECT_EQ(rows[2],
            Row({Value(int64_t{100}), Value(int64_t{3}), Value(int64_t{10})}));
  ctx.txn_.Abort();
}

// A recursive CTE fixes its shape from the anchor, so name matching never
// drops a column silently: a missing name, an extra strict name, a duplicate
// name, or an ON list that leaves a column out are all errors, and a
// positional term must keep the anchor width.
TEST_F(QueryTest, SqlEngineRecursiveCteNameMatchingRejectsMismatchedColumns) {
  TransactionContext ctx = db_->BeginContext();
  const auto expect_error = [&](std::string_view sql, std::string_view needle) {
    SqlEngine engine(*db_);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    std::string message;
    if (!prepared.HasValue()) {
      message = engine.LastError();
    } else {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      message = prepared.Value()->GetStatus().GetMessage();
    }
    EXPECT_NE(message.find(needle), std::string::npos) << sql << "\n"
                                                       << message;
  };
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b UNION ALL BY NAME "
      "SELECT 3 AS a FROM t) SELECT * FROM t;",
      "term has no column b");
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a UNION ALL BY NAME "
      "SELECT 2 AS a, 3 AS c FROM t) SELECT * FROM t;",
      "produces a column the anchor does not name: c");
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b UNION ALL BY NAME ON (a) "
      "SELECT 3 AS a, 4 AS b FROM t) SELECT * FROM t;",
      "cannot drop column b");
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a UNION ALL BY NAME ON (zzz) "
      "SELECT 2 AS a FROM t) SELECT * FROM t;",
      "has no column zzz");
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a UNION ALL "
      "SELECT 2 AS a, 3 AS b FROM t) SELECT * FROM t;",
      "columns but the working table");
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS a UNION ALL BY NAME "
      "SELECT a FROM t) SELECT * FROM t;",
      "cannot match duplicate column a");
  // A star term's names are only known once it runs, so the same strict
  // check runs against the executed schema.
  expect_error(
      "WITH RECURSIVE t AS (SELECT 1 AS a UNION ALL BY NAME "
      "SELECT * FROM (SELECT 2 AS a, 3 AS c)) SELECT * FROM t;",
      "produces column c that the working table does not name");
  ctx.txn_.Abort();
}

// CTE entries live in an unordered map, so the WITH list is resolved in
// dependency order rather than declaration order -- and that order has to
// follow the references wherever they sit.  A sibling named only inside a
// derived table, a scalar subquery or a set-operation branch used to be
// executed before its dependency existed ("table a not found").
TEST_F(QueryTest, SqlEngineCteDependencyOrderFollowsNestedReferences) {
  TransactionContext ctx = db_->BeginContext();
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH b AS (SELECT * FROM (SELECT * FROM a)), "
                   "a AS (SELECT 1 AS x) SELECT * FROM b;"),
            (std::vector<Row>{Row({Value(1)})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH b AS (SELECT (SELECT MAX(x) FROM a) AS m), "
                   "a AS (SELECT 1 AS x) SELECT * FROM b;"),
            (std::vector<Row>{Row({Value(1)})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH b AS (SELECT * FROM a UNION ALL SELECT * FROM a), "
                   "a AS (SELECT 1 AS x) SELECT * FROM b ORDER BY x;"),
            (std::vector<Row>{Row({Value(1)}), Row({Value(1)})}));
  ctx.txn_.Abort();
}

// A recursive body that reads a sibling CTE keeps the dependency-ordered
// route: the work-table leaf runs with no sibling in scope, so the fixpoint
// has to be driven by the route that resolved the whole WITH list first.
TEST_F(QueryTest, SqlEngineRecursiveCteReadsSiblingCteInDependencyOrder) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> rows =
      RunSql(ctx, *db_,
             "WITH RECURSIVE a AS (SELECT n FROM b "
             "UNION ALL SELECT n + 1 FROM a WHERE n < 3), b AS (SELECT 1 AS n) "
             "SELECT * FROM a ORDER BY n;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value(1)}));
  EXPECT_EQ(rows[2], Row({Value(3)}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH RECURSIVE a AS (SELECT n FROM b "
                   "UNION ALL SELECT n + 1 FROM a WHERE n < 3), "
                   "b AS (SELECT 1 AS n) SELECT SUM(n) FROM a;"),
            (std::vector<Row>{Row({Value(6)})}));
  ctx.txn_.Abort();
}

// A mutual reference among WITH entries has no evaluation order at all, so
// it is rejected by name instead of surfacing as "table not found" (a plain
// self reference is not a cycle -- it resolves outward; see below).
TEST_F(QueryTest, SqlEngineCyclicCteReferenceRejected) {
  TransactionContext ctx = db_->BeginContext();
  const auto expect_cycle = [&](std::string_view sql, std::string_view listed) {
    SqlEngine engine(*db_);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_FALSE(prepared.HasValue()) << sql;
    EXPECT_NE(engine.LastError().find("cyclic CTE reference among " +
                                      std::string(listed)),
              std::string::npos)
        << sql << "\n"
        << engine.LastError();
  };
  expect_cycle(
      "WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM a;",
      "a, b");
  expect_cycle(
      "WITH RECURSIVE a AS (SELECT * FROM b), b AS (SELECT * FROM a) "
      "SELECT * FROM a;",
      "a, b");
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH RECURSIVE a AS (SELECT 1 AS n "
                   "UNION ALL SELECT n + 1 FROM a WHERE n < 2) "
                   "SELECT * FROM a ORDER BY n;"),
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)})}));
  // A non-recursive entry is invisible inside its own definition, so a self
  // reference resolves outward: the enclosing WITH, or a catalog table
  // (`with_on_multiple_levels` in the compliance corpus pins the same rule
  // for nested WITHs).  It is therefore not a cycle -- and with nothing to
  // resolve to it is still a missing relation, not a false cycle report.
  RunSql(ctx, *db_, "CREATE TABLE cte_outward (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO cte_outward VALUES (1);");
  EXPECT_EQ(RunSql(ctx, *db_,
                   "WITH cte_outward AS (SELECT x + 1 AS x FROM cte_outward) "
                   "SELECT * FROM cte_outward;"),
            (std::vector<Row>{Row({Value(2)})}));
  {
    SqlEngine engine(*db_);
    StatusOr<Executor> unresolved =
        engine.Prepare(ctx, "WITH a AS (SELECT * FROM a) SELECT * FROM a;");
    std::string message;
    if (!unresolved.HasValue()) {
      message = engine.LastError();
    } else {
      Row row;
      while (unresolved.Value()->Next(&row, nullptr)) {
      }
      message = unresolved.Value()->GetStatus().GetMessage();
    }
    EXPECT_NE(message.find("not found"), std::string::npos) << message;
  }
  ctx.txn_.Abort();
}

}  // namespace tinylamb
