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

#include <cstdlib>
#include <string_view>

#include "common/platform/process.hpp"
#include "common/unwind_guard.hpp"
#include "expression/aggregate_expression.hpp"
#include "query/query_test.hpp"
#include "query/select_rewrites.hpp"

namespace tinylamb {

TEST_F(QueryTest, GroupedUnionRewriteKeepsCteUsedInClauseSubqueries) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE union_left (v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE union_right (v INT64);");

  enum class Clause { kNone, kGroupBy, kOrderBy, kDistinctOn };
  for (const Clause clause : {Clause::kNone, Clause::kGroupBy, Clause::kOrderBy,
                              Clause::kDistinctOn}) {
    SCOPED_TRACE(static_cast<int>(clause));
    auto make_branch = [](const std::string& table) {
      auto branch = std::make_shared<SelectStatement>();
      branch->SetSources({SelectSource(table, "")});
      branch->SetSelectList({NamedExpression("v", ColumnValueExp("v"))});
      return branch;
    };
    auto union_cte = make_branch("union_left");
    union_cte->AddUnionAll(make_branch("union_right"));

    auto grouped = std::make_shared<SelectStatement>();
    grouped->SetSources(
        {SelectSource("union_cte", "u"), SelectSource("union_right", "r")});
    const Expression group_key = ColumnValueExp(ColumnName("u", "v"));
    grouped->SetGroupBy({group_key});
    grouped->SetSelectList(
        {NamedExpression("v", group_key),
         NamedExpression("total", AggregateExpressionExp(AggregationType::kSum,
                                                         group_key))});

    SelectStatement root;
    root.AddWithQuery("union_cte", std::move(union_cte));
    root.SetSources({SelectSource("", "grouped", grouped)});
    const Expression ref = QueryExpressionExp(make_branch("union_cte"));
    switch (clause) {
      case Clause::kNone:
        break;
      case Clause::kGroupBy:
        root.SetGroupBy({ref});
        break;
      case Clause::kOrderBy:
        root.SetOrderBy(
            {SelectStatement::OrderByTerm{.expression = ref,
                                          .ascending = true,
                                          .nulls_first = std::nullopt}});
        break;
      case Clause::kDistinctOn:
        root.SetDistinctOn({ref});
        break;
    }

    ASSERT_TRUE(RewriteDistributeGroupedSumOverUnionAll(&root, ctx));
    ASSERT_EQ(grouped->Sources().size(), 1U);
    EXPECT_EQ(root.WithQueries().contains("union_cte"),
              clause != Clause::kNone);
  }
  ASSERT_SUCCESS(ctx.txn_.Abort());
}

TEST_F(QueryTest, LeftJoinKeyDomainRewriteValidatesNamesAndNullRejection) {
  TransactionContext ctx = db_->BeginContext();
  const Schema fact("domain_fact", {Column("k1", ValueType::kInt64),
                                    Column("k2", ValueType::kInt64)});
  ASSERT_SUCCESS(db_->CreateTable(ctx, fact).GetStatus());
  TableStatistics stats(fact);
  stats.Assign(100000, std::vector<ColumnStats>(fact.ColumnCount()));
  ASSERT_SUCCESS(db_->UpdateStatistics(ctx, fact.Name(), stats));

  auto make_grouped = [](bool duplicate_output) {
    auto grouped = std::make_shared<SelectStatement>();
    grouped->SetSources(
        {SelectSource("domain_fact", "f"), SelectSource("domain_fact", "d")});
    const Expression k1 = ColumnValueExp(ColumnName("f", "k1"));
    const Expression k2 = ColumnValueExp(ColumnName("f", "k2"));
    grouped->SetGroupBy({k1, k2});
    std::vector<NamedExpression> output{{"k1", k1}, {"k2", k2}};
    if (duplicate_output) {
      output.emplace_back("K1", k2);
    }
    grouped->SetSelectList(std::move(output));
    return grouped;
  };
  auto join_condition = [](std::string_view right) {
    return BinaryExpressionExp(
        BinaryExpressionExp(ColumnValueExp(ColumnName("l", "k1")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName(right, "k1"))),
        BinaryOperation::kAnd,
        BinaryExpressionExp(ColumnValueExp(ColumnName("l", "k2")),
                            BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName(right, "k2"))));
  };

  enum class Case {
    kAllowed,
    kOutputCollision,
    kCteCollision,
    kUnsignedDefault
  };
  for (const Case test : {Case::kAllowed, Case::kOutputCollision,
                          Case::kCteCollision, Case::kUnsignedDefault}) {
    SCOPED_TRACE(static_cast<int>(test));
    const std::string left =
        test == Case::kCteCollision ? "__LJ_ACTIVE_KEYS" : "left_group";
    SelectStatement root;
    root.AddWithQuery(left, make_grouped(test == Case::kOutputCollision));
    root.AddWithQuery("right_group1", make_grouped(false));
    root.AddWithQuery("right_group2", make_grouped(false));
    root.SetSources({SelectSource(left, "l"),
                     SelectSource("right_group1", "r1", nullptr,
                                  JoinType::kLeft, join_condition("r1")),
                     SelectSource("right_group2", "r2", nullptr,
                                  JoinType::kLeft, join_condition("r2"))});
    const Value default_value =
        test == Case::kUnsignedDefault
            ? Value(std::numeric_limits<int64_t>::min()).WithUnsigned()
            : Value(int64_t{0});
    root.SetWhereClause(BinaryExpressionExp(
        BinaryExpressionExp(
            FunctionCallExp("coalesce", {ColumnValueExp(ColumnName("r1", "k1")),
                                         ConstantValueExp(default_value)}),
            BinaryOperation::kGreaterThan, ConstantValueExp(Value(1))),
        BinaryOperation::kOr,
        UnaryExpressionExp(ColumnValueExp(ColumnName("r2", "k1")),
                           UnaryOperation::kIsNotNull)));
    EXPECT_EQ(RewriteDisjunctiveLeftJoinCteKeyDomain(&root, ctx),
              test == Case::kAllowed);
    EXPECT_EQ(root.WithQueries().contains("__lj_active_keys"),
              test == Case::kAllowed);
    EXPECT_TRUE(root.WithQueries().contains(left));
  }
  ASSERT_SUCCESS(ctx.txn_.Abort());
}

// WITH DEPTH modifier: iteration count is bounded explicitly, every row
// carries its depth, and the depth-0 anchor stays hidden below a non-zero
// lower bound.
TEST_F(QueryTest, SqlEngineRecursiveCteDepthModifierBoundsIterations) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "WITH RECURSIVE t AS (SELECT 0 AS n UNION ALL SELECT MOD(n + 1, 3) "
      "AS n FROM t) WITH DEPTH BETWEEN 1 AND 4 "
      "SELECT n, depth FROM t ORDER BY depth, n;");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(1)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(2)}));
  EXPECT_EQ(rows[2], Row({Value(0), Value(3)}));
  EXPECT_EQ(rows[3], Row({Value(1), Value(4)}));

  std::vector<Row> anchored = RunSql(
      ctx, *db_,
      "WITH RECURSIVE t AS (SELECT 0 AS n UNION ALL SELECT MOD(n + 1, 3) "
      "AS n FROM t) WITH DEPTH BETWEEN 0 AND 2 "
      "SELECT n, depth FROM t ORDER BY depth, n;");
  ASSERT_EQ(anchored.size(), 3U);
  EXPECT_EQ(anchored[0], Row({Value(0), Value(0)}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// WITH DEPTH metadata must survive the set-operation wrapper root: without
// it the recursion runs unbounded (100 rows) and no DEPTH column is produced.
TEST_F(QueryTest, SqlEngineRecursiveCteDepthModifierSurvivesSetOpRoot) {
  TransactionContext ctx = db_->BeginContext();
  std::vector<Row> rows = RunSql(
      ctx, *db_,
      "WITH RECURSIVE t AS (SELECT 0 AS n UNION ALL SELECT n + 1 AS n FROM t "
      "WHERE n < 100) WITH DEPTH BETWEEN 0 AND 2 "
      "SELECT n, depth FROM t UNION ALL SELECT -1, -1");
  ASSERT_EQ(rows.size(), 4U);  // depths 0,1,2 plus the UNION ALL's extra row
  for (const Row& row : rows) {
    EXPECT_EQ(row.Count(), 2U);
  }
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// EXTRACT(YEAR FROM date_col) predicates rewrite into column ranges so scan
// filters can skip pages; results must match the original function form.
TEST_F(QueryTest, SqlEngineExtractYearPredicateUsesDateRange) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE events (d DATE, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO events VALUES (DATE '2023-12-31', 1), "
         "(DATE '2024-01-01', 2), (DATE '2024-06-15', 3), "
         "(DATE '2024-12-31', 4), (DATE '2025-01-01', 5);");

  std::vector<Row> in_2024 =
      RunSql(ctx, *db_,
             "SELECT v FROM events WHERE EXTRACT(YEAR FROM d) = 2024 "
             "ORDER BY v;");
  ASSERT_EQ(in_2024.size(), 3U);
  EXPECT_EQ(in_2024[0][0], Value(2));
  EXPECT_EQ(in_2024[2][0], Value(4));

  std::vector<Row> from_2025 =
      RunSql(ctx, *db_,
             "SELECT v FROM events WHERE EXTRACT(YEAR FROM d) >= "
             "2025 ORDER BY v;");
  ASSERT_EQ(from_2025.size(), 1U);
  EXPECT_EQ(from_2025[0][0], Value(5));

  std::vector<Row> before_2024 = RunSql(
      ctx, *db_, "SELECT v FROM events WHERE EXTRACT(YEAR FROM d) < 2024;");
  ASSERT_EQ(before_2024.size(), 1U);
  EXPECT_EQ(before_2024[0][0], Value(1));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineSelfJoinAmbiguousColumnRejected) {
  // Phase 8: unqualified references across aliased relations are ambiguous.
  // The rejection surfaces either at prepare time or when the executor first
  // resolves the column, depending on which engine plans the statement.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1);");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared =
      engine.Prepare(ctx, "SELECT k FROM t AS a JOIN t AS b ON a.k = b.k;");
  if (!prepared.HasValue()) {
    EXPECT_NE(engine.LastError().find("ambiguous"), std::string::npos)
        << engine.LastError();
  } else {
    Row row;
    ASSERT_NO_THROW({
      while (prepared.Value()->Next(&row, nullptr)) {
      }
    });
    const Status status = prepared.Value()->GetStatus();
    EXPECT_NE(status, Status::kSuccess);
    EXPECT_NE(status.GetMessage().find("ambiguous"), std::string::npos)
        << status;
  }

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineMultiRelationGroupByUnqualifiedColumns) {
  // Verifies that multi-relation GROUP BY with unambiguous unqualified columns
  // executes through the Cascades-optimized core (ExecuteGroupedSelect).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE dept (d_id INT64, d_name VARCHAR(32));");
  RunSql(ctx, *db_,
         "CREATE TABLE emp (e_id INT64, e_dept INT64, e_salary INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Sales');");
  RunSql(ctx, *db_,
         "INSERT INTO emp VALUES (10, 1, 100), (20, 1, 150), (30, 2, 200);");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared = engine.Prepare(
      ctx,
      "SELECT d_name, COUNT(*) AS cnt, SUM(e_salary) AS total "
      "FROM emp, dept WHERE e_dept = d_id GROUP BY d_name ORDER BY d_name;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Engineering"));
  EXPECT_EQ(row[1], Value(2));
  EXPECT_EQ(row[2], Value(250));

  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Sales"));
  EXPECT_EQ(row[1], Value(1));
  EXPECT_EQ(row[2], Value(200));

  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SyntheticOrderByNameCannotCollideWithUserAlias) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ord_a (k INT64, v VARCHAR);");
  RunSql(ctx, *db_, "CREATE TABLE ord_b (k INT64, pad VARCHAR);");
  RunSql(ctx, *db_, "INSERT INTO ord_a VALUES (1, 'aaa'), (2, 'zzz');");
  RunSql(ctx, *db_, "INSERT INTO ord_b VALUES (1, 'x'), (1, 'y'), (2, 'z');");
  const auto rows =
      RunSql(ctx, *db_,
             "SELECT v AS __tinylamb_ord_0, COUNT(*) AS c FROM ord_a, ord_b "
             "WHERE ord_a.k = ord_b.k GROUP BY v ORDER BY c DESC, v;");
  ASSERT_EQ(rows.size(), 2U);
  // Ordered by the computed COUNT(*), not by the user column named like the
  // internal sort key.
  EXPECT_EQ(rows[0][0], Value("aaa"));
  EXPECT_EQ(rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(rows[1][0], Value("zzz"));
  EXPECT_EQ(rows[1][1], Value(int64_t{1}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineGroupedLeftJoinRoutesThroughCascadesBridge) {
  // M6-bridge: GROUP BY over a LEFT JOIN rides the Cascades core with
  // QueryData::OuterJoinEdge payloads. The null-padded row of a department
  // without employees must survive grouping (COUNT(*)=1, SUM=NULL): folding
  // the ON condition into WHERE would filter that row away and drop the
  // group entirely.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE dept (d_id INT64, d_name VARCHAR(32));");
  RunSql(ctx, *db_,
         "CREATE TABLE emp (e_id INT64, e_dept INT64, e_salary INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Sales');");
  RunSql(ctx, *db_, "INSERT INTO emp VALUES (10, 1, 100), (20, 1, 150);");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared =
      engine.Prepare(ctx,
                     "SELECT d_name, COUNT(*) AS cnt, SUM(e_salary) AS total "
                     "FROM dept LEFT JOIN emp ON e_dept = d_id "
                     "GROUP BY d_name ORDER BY d_name;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Engineering"));
  EXPECT_EQ(row[1], Value(2));
  EXPECT_EQ(row[2], Value(250));

  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Sales"));
  EXPECT_EQ(row[1], Value(1));
  EXPECT_TRUE(row[2].IsNull());

  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineScalarAggregateOverLeftJoin) {
  // Scalar aggregation over an outer join also rides the M6-bridge: the
  // preserved (null-padded) rows must be counted, not filtered by the ON
  // condition.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE dept (d_id INT64, d_name VARCHAR(32));");
  RunSql(ctx, *db_,
         "CREATE TABLE emp (e_id INT64, e_dept INT64, e_salary INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Sales');");
  RunSql(ctx, *db_, "INSERT INTO emp VALUES (10, 1, 100);");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared = engine.Prepare(
      ctx, "SELECT COUNT(*) FROM dept LEFT JOIN emp ON e_dept = d_id;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(2));  // one matched row + one null-padded row
  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineGroupedLeftJoinChainRoutesThroughCascadesBridge) {
  // A LEFT-join chain (multiple LEFT edges) with GROUP BY stays on the
  // M6-bridge slice: every edge keeps its own ON predicate.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE region (r_id INT64, r_name VARCHAR(32));");
  RunSql(ctx, *db_,
         "CREATE TABLE dept (d_id INT64, d_name VARCHAR(32), r_id INT64);");
  RunSql(ctx, *db_,
         "CREATE TABLE emp (e_id INT64, e_dept INT64, e_salary INT64);");
  RunSql(ctx, *db_, "INSERT INTO region VALUES (1, 'West'), (2, 'East');");
  RunSql(ctx, *db_, "INSERT INTO dept VALUES (1, 'Eng', 1);");
  RunSql(ctx, *db_, "INSERT INTO emp VALUES (10, 1, 100);");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared =
      engine.Prepare(ctx,
                     "SELECT r_name, COUNT(*) AS cnt "
                     "FROM region LEFT JOIN dept ON dept.r_id = region.r_id "
                     "LEFT JOIN emp ON e_dept = d_id "
                     "GROUP BY r_name ORDER BY r_name;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("East"));
  EXPECT_EQ(row[1], Value(1));  // null-padded twice: dept and emp both absent

  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("West"));
  EXPECT_EQ(row[1], Value(1));

  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineSelfJoinWithAliasesGroupBy) {
  // Verifies that self-joins with distinct aliases and grouping execute
  // successfully through Cascades and GroupByPlan.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE dept (d_id INT64, d_name VARCHAR(32));");
  RunSql(ctx, *db_, "INSERT INTO dept VALUES (1, 'Eng'), (2, 'Sales');");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared =
      engine.Prepare(ctx,
                     "SELECT a.d_name, COUNT(*) AS cnt "
                     "FROM dept AS a JOIN dept AS b ON a.d_id = b.d_id "
                     "GROUP BY a.d_name ORDER BY a.d_name;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Eng"));
  EXPECT_EQ(row[1], Value(1));

  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Sales"));
  EXPECT_EQ(row[1], Value(1));

  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineMultiRelationUnqualifiedSelectWithoutGrouping) {
  // Verifies that multi-relation joins with unqualified non-ambiguous columns
  // execute directly through the Cascades optimizer.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE dept (d_id INT64, d_name VARCHAR(32));");
  RunSql(ctx, *db_,
         "CREATE TABLE emp (e_id INT64, e_dept INT64, e_salary INT64);");
  RunSql(ctx, *db_, "INSERT INTO dept VALUES (1, 'Engineering');");
  RunSql(ctx, *db_, "INSERT INTO emp VALUES (10, 1, 100);");

  SqlEngine engine(*db_);
  StatusOr<Executor> prepared = engine.Prepare(
      ctx, "SELECT d_name, e_salary FROM emp, dept WHERE e_dept = d_id;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("Engineering"));
  EXPECT_EQ(row[1], Value(100));

  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineSelectLimitZeroReturnsNoRows) {
  // Arrange -- a non-empty table; LIMIT 0 must yield an empty result (§6.3).
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64, b INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);");

  // Act + Assert -- explicit LIMIT 0 returns zero rows on every route.
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM t LIMIT 0;").empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM t ORDER BY a LIMIT 0;").empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT DISTINCT a FROM t LIMIT 0;").empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT a FROM t WHERE a > 1 LIMIT 0 OFFSET 2;")
                  .empty());
  EXPECT_TRUE(RunSql(ctx, *db_, "SELECT x.a FROM t AS x WHERE x.a = 2 LIMIT 0;")
                  .empty());

  auto empty = engine.Execute(ctx, "SELECT a FROM t WHERE b / 0 = 1 LIMIT 0;");
  ASSERT_TRUE(empty.HasValue()) << empty.GetStatus();
  EXPECT_TRUE(empty.Value().Collect().empty());
  EXPECT_EQ(empty.Value().GetStatus(), Status::kSuccess);
  EXPECT_EQ(empty.Value().ColumnTypes(), (std::vector{ValueType::kInt64}));

  // Act + Assert -- neighbouring queries keep working after a LIMIT 0 ran.
  std::vector<Row> limited = RunSql(ctx, *db_, "SELECT a FROM t LIMIT 2;");
  ASSERT_EQ(limited.size(), 2U);
  std::vector<Row> all = RunSql(ctx, *db_, "SELECT a FROM t;");
  ASSERT_EQ(all.size(), 3U);
  EXPECT_EQ(engine.Prepare(ctx, "SELECT missing FROM t LIMIT 0;").GetStatus(),
            Status::kNotExists);

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineExplainDoesNotExecuteDdlOrDml) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE t (a INT64);");
  RunSql(ctx, *db_, "INSERT INTO t VALUES (1);");

  // Act + Assert -- EXPLAIN rejects DDL without executing it: the table must
  // still exist and EXPLAIN CREATE must not have created anything.
  StatusOr<Executor> explain_drop =
      engine.Prepare(ctx, "EXPLAIN DROP TABLE t;");
  EXPECT_FALSE(explain_drop.HasValue());
  ASSERT_EQ(RunSql(ctx, *db_, "SELECT a FROM t;").size(), 1U);

  StatusOr<Executor> explain_create =
      engine.Prepare(ctx, "EXPLAIN CREATE TABLE ghost_t (a INT64);");
  EXPECT_FALSE(explain_create.HasValue());
  ASSERT_EQ(RunSql(ctx, *db_, "SELECT a FROM t;").size(), 1U);

  // Act + Assert -- EXPLAIN INSERT neither inserts nor drains the executor.
  StatusOr<Executor> explain_insert =
      engine.Prepare(ctx, "EXPLAIN INSERT INTO t VALUES (2);");
  EXPECT_FALSE(explain_insert.HasValue());
  ASSERT_EQ(RunSql(ctx, *db_, "SELECT a FROM t;").size(), 1U);

  // Act + Assert -- EXPLAIN over SELECT still works.
  StatusOr<Executor> explain_select =
      engine.Prepare(ctx, "EXPLAIN SELECT a FROM t;");
  ASSERT_TRUE(explain_select.HasValue()) << engine.LastError();

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, OrToUnionPreservesBagMultiplicity) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE or_bag (a INT64, b INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO or_bag VALUES (1, 2), (1, 2), (1, NULL), "
         "(NULL, 2), (NULL, 2), (3, NULL), (4, 0);");
  const std::vector<Row> rows = RunSql(
      ctx, *db_, "SELECT a, b FROM or_bag WHERE a = 1 OR b = 2 ORDER BY a, b;");
  EXPECT_EQ(rows,
            (std::vector<Row>{Row({Value(), Value(int64_t{2})}),
                              Row({Value(), Value(int64_t{2})}),
                              Row({Value(int64_t{1}), Value()}),
                              Row({Value(int64_t{1}), Value(int64_t{2})}),
                              Row({Value(int64_t{1}), Value(int64_t{2})})}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEnginePlanCacheHitsOnRepeatedStatement) {
  // Arrange -- a unique table so no other test shares this fingerprint.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_hit (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_hit VALUES (7, 70), (8, 80);");
  // Setup only: publish the DDL/DML before measuring anything.  This commit
  // advances the schema epoch, so the fresh context below is a different
  // snapshot from the setup transaction -- but BOTH measured executions use
  // that same post-commit snapshot.  The hit assertion therefore measures
  // plan/template reuse, not an epoch invalidation between the two runs.
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();

  // Act -- run the identical templatable SELECT twice.
  const std::string sql = "SELECT v FROM pcache_hit WHERE k = 7;";
  std::vector<Row> first = RunPrepared(&engine, &ctx, sql);
  const uint64_t hits_before = PlanCacheStats().hits.load();
  std::vector<Row> second = RunPrepared(&engine, &ctx, sql);

  // Assert -- the second execution is a compiled-plan cache hit and both
  // executions agree with the table contents.
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(first[0][0], Value(70));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0][0], Value(70));
  EXPECT_EQ(PlanCacheStats().hits.load(), hits_before + 1);
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// The template cache replays a WITH body through `SqlTemplate::BindSelect`, so
// the rebuilt tree has to carry the recursive flags and the depth spec:
// dropping the flags turns the self reference into an unknown table, and
// keeping a stale depth spec changes how many rounds the work table runs.
// Literals are rebound on top, so a same-shape replay also proves the anchor,
// the term and the depth bounds take the new values.
TEST_F(QueryTest, TemplateCacheKeepsRecursiveCteMetadata) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  const auto template_hits = [] {
    return PlanCacheStats().template_hits.load();
  };
  const std::string sql =
      "WITH RECURSIVE t AS (SELECT 1 AS n "
      "UNION ALL SELECT n + 1 FROM t WHERE n < 3) "
      "SELECT n FROM t ORDER BY n;";
  const std::vector<Row> expected = {Row({Value(1)}), Row({Value(2)}),
                                     Row({Value(3)})};
  EXPECT_EQ(RunPrepared(&engine, &ctx, sql), expected);
  const uint64_t hits_before = template_hits();
  EXPECT_EQ(RunPrepared(&engine, &ctx, sql), expected);
  EXPECT_EQ(template_hits(), hits_before + 1);
  const uint64_t changed_before = template_hits();
  EXPECT_EQ(
      RunPrepared(&engine, &ctx,
                  "WITH RECURSIVE t AS (SELECT 10 AS n "
                  "UNION ALL SELECT n + 1 FROM t WHERE n < 12) "
                  "SELECT n FROM t ORDER BY n;"),
      (std::vector<Row>{Row({Value(10)}), Row({Value(11)}), Row({Value(12)})}));
  EXPECT_EQ(template_hits(), changed_before + 1);

  // WITH DEPTH: the bounds live in CTE metadata, and the second statement has
  // the same fingerprint with a smaller upper bound, so a replay that kept the
  // cached spec would emit one round too many.
  EXPECT_EQ(RunPrepared(&engine, &ctx,
                        "WITH RECURSIVE t AS (SELECT 0 AS n "
                        "UNION ALL BY NAME SELECT n + 1 AS n FROM t "
                        "WHERE n < 5) WITH DEPTH BETWEEN 0 AND 2 "
                        "SELECT n, depth FROM t ORDER BY n;"),
            (std::vector<Row>{Row({Value(0), Value(int64_t{0})}),
                              Row({Value(1), Value(int64_t{1})}),
                              Row({Value(2), Value(int64_t{2})})}));
  const uint64_t depth_before = template_hits();
  EXPECT_EQ(RunPrepared(&engine, &ctx,
                        "WITH RECURSIVE t AS (SELECT 0 AS n "
                        "UNION ALL BY NAME SELECT n + 1 AS n FROM t "
                        "WHERE n < 5) WITH DEPTH BETWEEN 0 AND 1 "
                        "SELECT n, depth FROM t ORDER BY n;"),
            (std::vector<Row>{Row({Value(0), Value(int64_t{0})}),
                              Row({Value(1), Value(int64_t{1})})}));
  EXPECT_EQ(template_hits(), depth_before + 1);
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest,
       SqlEnginePlanCacheRevalidatesSnapshotDependentJoinElimination) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(
      ctx, *db_,
      "CREATE TABLE pcache_snap_parent (id INT64 PRIMARY KEY, label INT64);");
  RunSql(ctx, *db_,
         "CREATE TABLE pcache_snap_child (id INT64, pid INT64, value INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_snap_parent VALUES (1, 10);");
  RunSql(ctx, *db_, "INSERT INTO pcache_snap_child VALUES (1, 1, 100);");
  const std::string sql =
      "SELECT c.value, 0 AS marker FROM pcache_snap_child c "
      "JOIN pcache_snap_parent p ON c.pid = p.id WHERE c.value = 100;";
  ASSERT_EQ(RunPrepared(&engine, &ctx, sql).size(), 1U);
  const uint64_t hits_before = PlanCacheStats().hits.load();
  ASSERT_EQ(RunPrepared(&engine, &ctx, sql).size(), 1U);
  EXPECT_EQ(PlanCacheStats().hits.load(), hits_before);
  RunSql(ctx, *db_, "UPDATE pcache_snap_child SET pid = 999 WHERE id = 1;");
  EXPECT_TRUE(RunPrepared(&engine, &ctx, sql).empty());
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEnginePlanCacheRevalidatesSnapshotDependentTopNProof) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_topn_parent (id INT64 PRIMARY KEY);");
  RunSql(ctx, *db_,
         "CREATE TABLE pcache_topn_child (id INT64 PRIMARY KEY, pid INT64, "
         "payload INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO pcache_topn_parent VALUES (1), (2), (3), (4), (5), "
         "(6), (7), (8);");
  RunSql(ctx, *db_,
         "INSERT INTO pcache_topn_child VALUES (1, 1, 10), (2, 2, 20), "
         "(3, 3, 30), (4, 4, 40), (5, 5, 50), (6, 6, 60), (7, 7, 70), "
         "(8, 8, 80);");
  const std::string sql =
      "SELECT c.id, c.payload, p.id AS parent_id, 0 AS marker "
      "FROM pcache_topn_child c JOIN pcache_topn_parent p ON c.pid = p.id "
      "WHERE c.payload >= 0 ORDER BY c.id LIMIT 1;";
  const std::vector<Row> warm = RunPrepared(&engine, &ctx, sql);
  ASSERT_EQ(warm.size(), 1U);
  const uint64_t hits_before = PlanCacheStats().hits.load();
  ASSERT_EQ(RunPrepared(&engine, &ctx, sql).size(), 1U);
  EXPECT_EQ(PlanCacheStats().hits.load(), hits_before);
  RunSql(ctx, *db_, "INSERT INTO pcache_topn_child VALUES (-1, 999, 20);");
  const std::vector<Row> after = RunPrepared(&engine, &ctx, sql);
  ASSERT_EQ(after.size(), 1U);
  EXPECT_EQ(after[0][0], Value(int64_t{1}));
  EXPECT_EQ(after[0][1], Value(int64_t{10}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, CompliancePrimaryKeyModeNeverReplaysUncheckedDml) {
  struct ModeReset {
    ModeReset() = default;
    ModeReset(const ModeReset&) = delete;
    ModeReset& operator=(const ModeReset&) = delete;
    ModeReset(ModeReset&&) = delete;
    ModeReset& operator=(ModeReset&&) = delete;
    ~ModeReset() { SqlEngine::SetCompliancePrimaryKeyMode(false); }
  } mode_reset;
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE compliance_cache (id INT64, marker INT64);");
  RunSql(ctx, *db_, "INSERT INTO compliance_cache VALUES (1, 1), (2, 2);");
  SqlEngine::SetCompliancePrimaryKeyMode(false);
  SqlEngine engine(*db_);
  const std::string sql =
      "UPDATE compliance_cache SET id = 3 WHERE marker = 1;";
  const auto execute_update = [&](TransactionContext& transaction) {
    StatusOr<Executor> prepared = engine.Prepare(transaction, sql);
    if (!prepared.HasValue()) {
      return prepared.GetStatus();
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    return prepared.Value()->GetStatus();
  };
  EXPECT_EQ(execute_update(ctx), Status::kSuccess);
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();
  RunSql(ctx, *db_, "INSERT INTO compliance_cache VALUES (3, 3);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();
  SqlEngine::SetCompliancePrimaryKeyMode(true);
  EXPECT_EQ(execute_update(ctx), Status::kDuplicates);
}

TEST_F(QueryTest, SqlEnginePlanCacheInvalidatedByDdlAndAnalyze) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_ddl (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (5, 50);");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  ASSERT_EQ(
      RunPrepared(&engine, &ctx, "SELECT v FROM pcache_ddl WHERE k = 5;")[0][0],
      Value(50));

  // Act (a) -- DROP + re-CREATE the same-named table with different data.
  // The fingerprint of the SELECT is unchanged, but the stale entry must be
  // discarded via the epoch check instead of replaying old page contents.
  ASSERT_TRUE(engine.Prepare(ctx, "DROP TABLE pcache_ddl;").HasValue());
  RunSql(ctx, *db_, "CREATE TABLE pcache_ddl (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (5, 555);");
  // Unlike the pure cache-hit tests, here the commit is deliberate: its epoch
  // bump is what must invalidate the entry filled before the DROP.  Measure
  // invalidations only in the new snapshot, after that bump.
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  const uint64_t invalidations_before =
      PlanCacheStats().epoch_invalidations.load();
  std::vector<Row> recreated =
      RunPrepared(&engine, &ctx, "SELECT v FROM pcache_ddl WHERE k = 5;");

  // Assert (a) -- fresh compile observed the new table contents.
  ASSERT_EQ(recreated.size(), 1U);
  EXPECT_EQ(recreated[0][0], Value(555));
  EXPECT_GT(PlanCacheStats().epoch_invalidations.load(), invalidations_before);

  // Act (b) -- ANALYZE refreshes statistics and must invalidate too.
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (6, 60);");
  ASSERT_TRUE(engine.Prepare(ctx, "ANALYZE pcache_ddl;").HasValue());
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  const uint64_t invalidations_mid =
      PlanCacheStats().epoch_invalidations.load();
  std::vector<Row> analyzed =
      RunPrepared(&engine, &ctx, "SELECT v FROM pcache_ddl WHERE k = 6;");
  ASSERT_EQ(analyzed.size(), 1U);
  EXPECT_EQ(analyzed[0][0], Value(60));
  EXPECT_GT(PlanCacheStats().epoch_invalidations.load(), invalidations_mid);

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEnginePlanCacheRejectsPreDdlSnapshotAtPostDdlEpoch) {
  const std::string table_name =
      "pcache_incarnation_" + std::string(RandomString());
  TransactionContext setup = db_->BeginContext();
  RunSql(setup, *db_, "CREATE TABLE " + table_name + " (k INT64, v INT64);");
  RunSql(setup, *db_, "INSERT INTO " + table_name + " VALUES (5, 500);");
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  TransactionContext reader = db_->BeginContext();
  SqlEngine engine(*db_);
  ASSERT_TRUE(reader.GetTable(table_name).HasValue());
  const std::string sql = "SELECT v FROM " + table_name + " WHERE k = 5;";

  TransactionContext ddl = db_->BeginContext();
  RunSql(ddl, *db_, "DROP TABLE " + table_name + ";");
  RunSql(ddl, *db_, "CREATE TABLE " + table_name + " (k INT64, v INT64);");
  RunSql(ddl, *db_, "INSERT INTO " + table_name + " VALUES (5, 900);");

  {
    // The pre-DDL snapshot must not serve the recreated table: the read
    // fails (Conflicts) or yields nothing rather than the stale catalog
    // image.  RunPrepared's success assertion does not apply here.
    StatusOr<Executor> stale = engine.Prepare(reader, sql);
    if (stale.HasValue()) {
      Row stale_row;
      while (stale.Value()->Next(&stale_row, nullptr)) {
      }
    }
  }
  ASSERT_SUCCESS(reader.txn_.PreCommit());
  ASSERT_SUCCESS(ddl.txn_.PreCommit());

  const uint64_t invalidations_before =
      PlanCacheStats().epoch_invalidations.load();
  TransactionContext fresh = db_->BeginContext();
  const std::vector<Row> recreated = RunPrepared(&engine, &fresh, sql);
  ASSERT_EQ(recreated.size(), 1U);
  EXPECT_EQ(recreated[0][0], Value(900));
  EXPECT_GT(PlanCacheStats().epoch_invalidations.load(), invalidations_before);
  ASSERT_SUCCESS(fresh.txn_.PreCommit());
}

TEST_F(QueryTest, SqlEnginePlanCacheParameterCorrectness) {
  // (1) SELECT with differing literals: a specialized entry is only served
  // for identical parameters, and differing parameters re-specialize it.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_param (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO pcache_param VALUES (1, 10), (2, 20), (3, 30);");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  ASSERT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_param WHERE k = 1;")[0][0],
            Value(10));  // fills the entry with k=1's plan
  const uint64_t rebound_hits = PlanCacheStats().hits.load();
  const uint64_t rebound_replays = PlanCacheStats().replays.load();
  ASSERT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_param WHERE k = 2;")[0][0],
            Value(20));  // different literal rebinds the cached plan
  EXPECT_EQ(PlanCacheStats().hits.load(), rebound_hits + 1);
  EXPECT_EQ(PlanCacheStats().replays.load(), rebound_replays + 1);
  uint64_t hits_before = PlanCacheStats().hits.load();
  ASSERT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_param WHERE k = 2;")[0][0],
            Value(20));  // repeat must HIT the refreshed k=2 plan
  EXPECT_EQ(PlanCacheStats().hits.load(), hits_before + 1);
  EXPECT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_param WHERE k = 3;")[0][0],
            Value(30));

  // (2) INSERT artifacts are parametric: every repeated shape hits regardless
  // of its literal values because slots receive per-execution values.
  RunSql(ctx, *db_, "CREATE TABLE pcache_ins (a INT64, b STRING(8));");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  RunSql(ctx, *db_, "INSERT INTO pcache_ins VALUES (11, 'x');");
  hits_before = PlanCacheStats().hits.load();
  RunSql(ctx, *db_, "INSERT INTO pcache_ins VALUES (12, 'y');");
  EXPECT_GT(PlanCacheStats().hits.load(), hits_before);
  // Explicit column lists exercise the baked reorder map under the cache.
  RunSql(ctx, *db_, "INSERT INTO pcache_ins (b, a) VALUES ('z', 13);");
  hits_before = PlanCacheStats().hits.load();
  RunSql(ctx, *db_, "INSERT INTO pcache_ins (b, a) VALUES ('w', 14);");
  EXPECT_GT(PlanCacheStats().hits.load(), hits_before);

  // Assert -- exactly the inserted rows are visible.
  std::vector<Row> contents =
      RunPrepared(&engine, &ctx, "SELECT a, b FROM pcache_ins ORDER BY a;");
  ASSERT_EQ(contents.size(), 4U);
  EXPECT_EQ(contents[0][0], Value(11));
  EXPECT_EQ(contents[0][1], Value("x"));
  EXPECT_EQ(contents[1][0], Value(12));
  EXPECT_EQ(contents[1][1], Value("y"));
  EXPECT_EQ(contents[2][0], Value(13));
  EXPECT_EQ(contents[2][1], Value("z"));
  EXPECT_EQ(contents[3][0], Value(14));
  EXPECT_EQ(contents[3][1], Value("w"));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEnginePlanCacheRebindsUpdateAndDelete) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_upd (k INT64 PRIMARY KEY, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_upd VALUES (1, 10), (2, 20);");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();

  RunSql(ctx, *db_, "UPDATE pcache_upd SET v = v + 1 WHERE k = 1;");
  const uint64_t hits = PlanCacheStats().hits.load();
  const uint64_t replays = PlanCacheStats().replays.load();
  RunSql(ctx, *db_, "UPDATE pcache_upd SET v = v + 5 WHERE k = 2;");
  EXPECT_EQ(PlanCacheStats().hits.load(), hits + 1);
  EXPECT_EQ(PlanCacheStats().replays.load(), replays + 1);

  std::vector<Row> updated =
      RunPrepared(&engine, &ctx, "SELECT k, v FROM pcache_upd ORDER BY k;");
  ASSERT_EQ(updated.size(), 2U);
  EXPECT_EQ(updated[0][0], Value(1));
  EXPECT_EQ(updated[0][1], Value(11));
  EXPECT_EQ(updated[1][0], Value(2));
  EXPECT_EQ(updated[1][1], Value(25));

  RunSql(ctx, *db_, "DELETE FROM pcache_upd WHERE k = 1;");
  const uint64_t delete_hits = PlanCacheStats().hits.load();
  const uint64_t delete_replays = PlanCacheStats().replays.load();
  RunSql(ctx, *db_, "DELETE FROM pcache_upd WHERE k = 2;");
  EXPECT_EQ(PlanCacheStats().hits.load(), delete_hits + 1);
  EXPECT_EQ(PlanCacheStats().replays.load(), delete_replays + 1);
  EXPECT_TRUE(RunPrepared(&engine, &ctx, "SELECT k FROM pcache_upd;").empty());

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// The optimizer sorts conjuncts by text, so `d_id` precedes `d_w_id`. The
// first execution's literals are all 1. A later district must still bind in
// SQL order (warehouse, then district), not in that sorted order.
TEST_F(QueryTest, SqlEnginePlanCacheRebindsDuplicateKeyLiterals) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_,
         "CREATE TABLE pcache_district (d_w_id INT64 NOT NULL, d_id INT64 NOT "
         "NULL, d_next_o_id INT64, PRIMARY KEY (d_w_id, d_id));");
  RunSql(ctx, *db_,
         "INSERT INTO pcache_district VALUES (1, 1, 10), (1, 2, 20);");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();

  std::vector<Row> first = RunPrepared(
      &engine, &ctx,
      "SELECT d_next_o_id FROM pcache_district WHERE d_w_id = 1 AND d_id = 1;");
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(first[0][0], Value(10));
  const uint64_t select_hits = PlanCacheStats().hits.load();
  const uint64_t select_replays = PlanCacheStats().replays.load();
  std::vector<Row> second = RunPrepared(
      &engine, &ctx,
      "SELECT d_next_o_id FROM pcache_district WHERE d_w_id = 1 AND d_id = 2;");
  EXPECT_EQ(PlanCacheStats().hits.load(), select_hits + 1);
  EXPECT_EQ(PlanCacheStats().replays.load(), select_replays + 1);
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0][0], Value(20));

  RunSql(ctx, *db_,
         "UPDATE pcache_district SET d_next_o_id = d_next_o_id + 1 WHERE "
         "d_w_id = 1 AND d_id = 1;");
  const uint64_t update_hits = PlanCacheStats().hits.load();
  const uint64_t update_replays = PlanCacheStats().replays.load();
  RunSql(ctx, *db_,
         "UPDATE pcache_district SET d_next_o_id = d_next_o_id + 1 WHERE "
         "d_w_id = 1 AND d_id = 2;");
  EXPECT_EQ(PlanCacheStats().hits.load(), update_hits + 1);
  EXPECT_EQ(PlanCacheStats().replays.load(), update_replays + 1);

  std::vector<Row> updated = RunPrepared(
      &engine, &ctx,
      "SELECT d_id, d_next_o_id FROM pcache_district ORDER BY d_id;");
  ASSERT_EQ(updated.size(), 2U);
  EXPECT_EQ(updated[0][0], Value(1));
  EXPECT_EQ(updated[0][1], Value(11));
  EXPECT_EQ(updated[1][0], Value(2));
  EXPECT_EQ(updated[1][1], Value(21));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEnginePlanCacheRetainsSubqueryTablesAcrossDmlReplays) {
  {
    TransactionContext setup = db_->BeginContext();
    RunSql(setup, *db_,
           "CREATE TABLE pcache_del_target (u INT64, c1 INT64, c3 INT64);");
    RunSql(setup, *db_, "CREATE TABLE pcache_del_sub (c1 INT64);");
    RunSql(setup, *db_,
           "INSERT INTO pcache_del_target VALUES (1, 10, 100), (2, 20, 200), "
           "(3, 30, 300);");
    RunSql(setup, *db_, "INSERT INTO pcache_del_sub VALUES (100), (300);");
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }

  SqlEngine engine(*db_);
  const std::string del_sql =
      "DELETE FROM pcache_del_target WHERE EXISTS ("
      "SELECT 1 FROM pcache_del_sub WHERE pcache_del_sub.c1 = "
      "pcache_del_target.c3);";
  const std::string upd_sql =
      "UPDATE pcache_del_target SET c1 = c1 + 1 WHERE EXISTS ("
      "SELECT 1 FROM pcache_del_sub WHERE pcache_del_sub.c1 = "
      "pcache_del_target.c3);";

  // Fill the DML plan cache inside txn1 and roll back its mutations before
  // destroying its ctx.tables_ map.
  {
    TransactionContext txn1 = db_->BeginContext();
    RunSql(txn1, *db_, del_sql);
    RunSql(txn1, *db_, upd_sql);
    EXPECT_EQ(txn1.txn_.Abort(), Status::kSuccess);
  }

  // Replaying both statements across a fresh transaction must keep
  // FullScanPlan/IndexScanPlan Table references alive for subquery tables and
  // preserve correctness across checkpointing.
  {
    // Auto-compaction advances the schema epoch and invalidates the plans.
    // Keep this checkpoint focused on replaying the existing cached plans.
    constexpr const char* kAutoTlcs = "TINYLAMB_AUTO_TLCS";
    const char* configured = ::tinylamb::platform::GetEnv(kAutoTlcs);
    const auto previous = configured != nullptr
                              ? std::optional<std::string>(configured)
                              : std::nullopt;
    ASSERT_EQ(platform::SetEnv(kAutoTlcs, "0"), 0);
    const UnwindGuard restore_auto_tlcs([&] {
      if (previous) {
        (void)platform::SetEnv(kAutoTlcs, previous->c_str());
      } else {
        (void)platform::UnsetEnv(kAutoTlcs);
      }
    });
    const uint64_t epoch = db_->SchemaEpoch();
    ASSERT_SUCCESS(db_->WriteCheckpoint().GetStatus());
    ASSERT_EQ(db_->SchemaEpoch(), epoch);
  }
  {
    TransactionContext txn2 = db_->BeginContext();
    const uint64_t hits_before = PlanCacheStats().hits.load();
    const uint64_t replays_before = PlanCacheStats().replays.load();
    RunSql(txn2, *db_, upd_sql);
    RunSql(txn2, *db_, del_sql);
    EXPECT_GE(PlanCacheStats().hits.load(), hits_before + 2);
    EXPECT_GE(PlanCacheStats().replays.load(), replays_before + 2);
    const std::vector<Row> left = RunPrepared(
        &engine, &txn2, "SELECT u, c1 FROM pcache_del_target ORDER BY u;");
    ASSERT_EQ(left.size(), 1U);
    EXPECT_EQ(left[0][0], Value(2));
    EXPECT_EQ(left[0][1], Value(20));
    ASSERT_SUCCESS(txn2.txn_.PreCommit());
  }

  // Recreating only the subquery table must invalidate the cached DELETE/UPDATE
  // plan (via retained_tables incarnation check), not replay against the dead
  // table object.
  {
    TransactionContext ddl = db_->BeginContext();
    RunSql(ddl, *db_, "DROP TABLE pcache_del_sub;");
    RunSql(ddl, *db_, "CREATE TABLE pcache_del_sub (c1 INT64);");
    RunSql(ddl, *db_, "INSERT INTO pcache_del_sub VALUES (200);");
    ASSERT_SUCCESS(ddl.txn_.PreCommit());
  }
  {
    TransactionContext txn3 = db_->BeginContext();
    const uint64_t invalidations_before =
        PlanCacheStats().epoch_invalidations.load();
    RunSql(txn3, *db_, del_sql);
    EXPECT_GT(PlanCacheStats().epoch_invalidations.load(),
              invalidations_before);
    const std::vector<Row> left = RunPrepared(
        &engine, &txn3, "SELECT u FROM pcache_del_target ORDER BY u;");
    EXPECT_TRUE(left.empty());
    ASSERT_SUCCESS(txn3.txn_.PreCommit());
  }
}

TEST_F(QueryTest, LateralJoinExpansion) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE lat_outer (id INT64, name STRING(16));");
  RunSql(ctx, *db_, "CREATE TABLE lat_inner (outer_id INT64, score INT64);");
  RunSql(
      ctx, *db_,
      "INSERT INTO lat_outer VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');");
  RunSql(ctx, *db_, "INSERT INTO lat_inner VALUES (1, 100), (1, 95), (2, 80);");

  // Correlated LATERAL subquery: inner query references outer's id
  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN SELECT o.name, i.score FROM lat_outer o, LATERAL (SELECT score "
      "FROM lat_inner WHERE outer_id = o.id) i ORDER BY o.name, i.score;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  std::string plan_text;
  Row r;
  while (explain.Value()->Next(&r, nullptr)) {
    plan_text += r[0].value.varchar_value;
    plan_text += "\n";
  }
  // M5+ decorrelation : the single-table equality LATERAL
  // plans as a decorrelated hash join instead of per-row Apply execution.
  EXPECT_NE(plan_text.find("HashJoin"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("ApplyPlan"), std::string::npos) << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos);

  std::vector<Row> result = RunSql(
      ctx, *db_,
      "SELECT o.name, i.score FROM lat_outer o, LATERAL (SELECT score FROM "
      "lat_inner WHERE outer_id = o.id) i ORDER BY o.name, i.score;");
  ASSERT_EQ(result.size(), 3U);
  EXPECT_EQ(result[0][0], Value("Alice"));
  EXPECT_EQ(result[0][1], Value(95));
  EXPECT_EQ(result[1][0], Value("Alice"));
  EXPECT_EQ(result[1][1], Value(100));
  EXPECT_EQ(result[2][0], Value("Bob"));
  EXPECT_EQ(result[2][1], Value(80));

  // LEFT LATERAL JOIN: includes Charlie with NULL score
  std::vector<Row> left_result =
      RunSql(ctx, *db_,
             "SELECT o.name, i.score FROM lat_outer o LEFT JOIN LATERAL "
             "(SELECT score FROM lat_inner WHERE outer_id = o.id) i ON TRUE "
             "ORDER BY o.name, i.score;");
  ASSERT_EQ(left_result.size(), 4U);
  EXPECT_EQ(left_result[0][0], Value("Alice"));
  EXPECT_EQ(left_result[0][1], Value(95));
  EXPECT_EQ(left_result[1][0], Value("Alice"));
  EXPECT_EQ(left_result[1][1], Value(100));
  EXPECT_EQ(left_result[2][0], Value("Bob"));
  EXPECT_EQ(left_result[2][1], Value(80));
  EXPECT_EQ(left_result[3][0], Value("Charlie"));
  EXPECT_TRUE(left_result[3][1].IsNull());

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, LeftUsingBeforeLateralPreservesOuterRow) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE lat_empty (id INT64);");
  RunSql(ctx, *db_, "CREATE TABLE lat_seed (id INT64);");
  RunSql(ctx, *db_, "INSERT INTO lat_seed VALUES (1);");

  std::vector<Row> result = RunSql(
      ctx, *db_,
      "SELECT l.id, d.x FROM lat_seed l LEFT JOIN lat_empty r USING (id), "
      "LATERAL (SELECT 42 AS x) d ORDER BY l.id");
  ASSERT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0][0], Value(1));
  EXPECT_EQ(result[0][1], Value(42));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, PreparedStatementGenericCustomPlan) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_thresh (id INT64, val INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO pcache_thresh VALUES (1, 10), (2, 20), (3, 30);");

  // Repeated executions of parameterized SELECT
  for (int i = 0; i < 8; ++i) {
    std::vector<Row> res = RunPrepared(
        &engine, &ctx, "SELECT val FROM pcache_thresh WHERE id = 1;");
    ASSERT_EQ(res.size(), 1U);
    EXPECT_EQ(res[0][0], Value(10));
  }
  // Parameter variation
  std::vector<Row> res2 =
      RunPrepared(&engine, &ctx, "SELECT val FROM pcache_thresh WHERE id = 2;");
  ASSERT_EQ(res2.size(), 1U);
  EXPECT_EQ(res2[0][0], Value(20));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, BatchInsertChunking) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE batch_tbl (id INT64, val INT64);");

  // Construct a large batch INSERT statement with 150 rows (> 2 chunks of 64)
  std::string sql = "INSERT INTO batch_tbl VALUES ";
  sql.reserve(150);
  for (int i = 0; i < 150; ++i) {
    if (i > 0) {
      sql += ", ";
    }
    sql += "(" + std::to_string(i) + ", " + std::to_string(i * 10) + ")";
  }
  sql += ";";

  RunSql(ctx, *db_, sql);

  std::vector<Row> count_res =
      RunSql(ctx, *db_, "SELECT COUNT(*) FROM batch_tbl;");
  ASSERT_EQ(count_res.size(), 1U);
  EXPECT_EQ(count_res[0][0], Value(150));

  std::vector<Row> sample_res =
      RunSql(ctx, *db_, "SELECT val FROM batch_tbl WHERE id = 70;");
  ASSERT_EQ(sample_res.size(), 1U);
  EXPECT_EQ(sample_res[0][0], Value(700));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SelectDistinctOnExecution) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE emp_dist (dept VARCHAR, salary INT64, name VARCHAR);");
  RunSql(
      ctx, *db_,
      "INSERT INTO emp_dist VALUES ('HR', 100, 'Alice'), ('HR', 120, 'Bob'), "
      "('IT', 200, 'Charlie'), ('IT', 180, 'David');");

  std::vector<Row> res = RunSql(ctx, *db_,
                                "SELECT DISTINCT ON (dept) dept, salary, name "
                                "FROM emp_dist ORDER BY dept, salary DESC;");
  ASSERT_EQ(res.size(), 2U);
  EXPECT_EQ(res[0][0], Value("HR"));
  EXPECT_EQ(res[0][1], Value(120));
  EXPECT_EQ(res[0][2], Value("Bob"));
  EXPECT_EQ(res[1][0], Value("IT"));
  EXPECT_EQ(res[1][1], Value(200));
  EXPECT_EQ(res[1][2], Value("Charlie"));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, IntersectWithEmptyBranchKeepsExpandedHeadColumnNames) {
  auto setup = db_->BeginContext();
  RunSql(setup, *db_, "CREATE TABLE intersect_head (a INT64, b INT64);");
  RunSql(setup, *db_, "CREATE TABLE intersect_empty (x INT64, y INT64);");
  RunSql(setup, *db_, "INSERT INTO intersect_head VALUES (1, 2), (3, 4);");
  ASSERT_EQ(setup.PreCommit(), Status::kSuccess);
  auto context = db_->BeginContext();
  RunSql(context, *db_, "ANALYZE;");

  EXPECT_TRUE(RunSql(context, *db_,
                     "SELECT a, b FROM (SELECT * FROM intersect_head "
                     "INTERSECT DISTINCT SELECT x, y FROM intersect_empty);")
                  .empty());
}

TEST_F(QueryTest, FetchFirstWithTiesExecution) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE score_ties (name VARCHAR, score INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO score_ties VALUES ('Alice', 100), ('Bob', 90), "
         "('Charlie', 90), ('Dave', 80);");

  std::vector<Row> res = RunSql(ctx, *db_,
                                "SELECT name, score FROM score_ties ORDER BY "
                                "score DESC FETCH FIRST 2 ROWS WITH TIES;");
  ASSERT_EQ(res.size(), 3U);
  EXPECT_EQ(res[0][0], Value("Alice"));
  EXPECT_EQ(res[0][1], Value(100));
  EXPECT_EQ(res[1][1], Value(90));
  EXPECT_EQ(res[2][1], Value(90));

  // Replaying the identical statement must keep WITH TIES. The compiled
  // SELECT cache used to force with_ties=false on the second execution,
  // silently dropping the tied peer at the fetch boundary.
  std::vector<Row> replay =
      RunSql(ctx, *db_,
             "SELECT name, score FROM score_ties ORDER BY "
             "score DESC FETCH FIRST 2 ROWS WITH TIES;");
  ASSERT_EQ(replay.size(), 3U);
  EXPECT_EQ(replay[1][1], Value(90));
  EXPECT_EQ(replay[2][1], Value(90));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// WITH TIES tie detection must resolve a collated SELECT-list ordinal
// (`ORDER BY 2 COLLATE 'und:ci'`) to its bound output column, exactly like
// the sort binding does. Evaluating the COLLATE wrapper per row compared
// the ordinal literal with itself, tied every remaining row, and emitted
// the whole tail past the fetch boundary.
TEST_F(QueryTest, FetchFirstWithTiesCollatedOrdinal) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ordinal_ties (name VARCHAR, score INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO ordinal_ties VALUES ('a', 100), ('B', 90), "
         "('c', 90), ('D', 80);");

  std::vector<Row> res =
      RunSql(ctx, *db_,
             "SELECT name, score FROM ordinal_ties ORDER BY 2 COLLATE 'und:ci' "
             "FETCH FIRST 2 ROWS WITH TIES;");
  // Ascending score: 80 then the tied pair of 90s; 100 is past the boundary.
  ASSERT_EQ(res.size(), 3U);
  EXPECT_EQ(res[0][0], Value("D"));
  EXPECT_EQ(res[0][1], Value(80));
  EXPECT_EQ(res[1][1], Value(90));
  EXPECT_EQ(res[2][1], Value(90));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// A COLLATE-wrapped ORDER BY ordinal is still an ordinal: out-of-range
// values must fail like the bare-integer form instead of degrading into a
// constant sort key that orders nothing.
TEST_F(QueryTest, CollatedOrderByOrdinalRangeChecked) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE coll_ord (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO coll_ord VALUES (3), (1), (2);");

  SqlEngine engine(*db_);
  for (const char* sql :
       {"SELECT x FROM coll_ord ORDER BY 99 COLLATE 'und:ci'",
        "SELECT x FROM coll_ord ORDER BY 0 COLLATE 'und:ci'",
        "SELECT x FROM coll_ord ORDER BY 2 COLLATE 'und:ci'"}) {
    // The ordinal check runs when the sort executor is driven, so the
    // failure surfaces through GetStatus after Next() drains to a stop.
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    ASSERT_TRUE(prepared.HasValue()) << sql << ": " << engine.LastError();
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    // Pin the *reason* for the failure, not merely that something failed:
    // a syntax error or an unrelated runtime fault must not turn this
    // range-check regression test green.
    const Status status = prepared.Value()->GetStatus();
    ASSERT_EQ(status.GetCode(), StatusCode::kInvalidArgument) << sql;
    EXPECT_NE(status.GetMessage().find("ordinal out of range"),
              std::string::npos)
        << sql << ": " << status.GetMessage();
  }

  // The in-range collated ordinal still binds and sorts.
  std::vector<Row> res =
      RunSql(ctx, *db_, "SELECT x FROM coll_ord ORDER BY 1 COLLATE 'und:ci'");
  ASSERT_EQ(res.size(), 3U);
  EXPECT_EQ(res[0][0], Value(1));
  EXPECT_EQ(res[2][0], Value(3));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, GroupByAllAndDistinctExecution) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(
      ctx, *db_,
      "CREATE TABLE sales_grp (dept VARCHAR, region VARCHAR, amount INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO sales_grp VALUES ('A', 'North', 10), ('A', 'North', 20), "
         "('B', 'South', 30);");

  std::vector<Row> res_all =
      RunSql(ctx, *db_,
             "SELECT dept, region, sum(amount) AS total FROM sales_grp GROUP "
             "BY ALL ORDER BY dept;");
  ASSERT_EQ(res_all.size(), 2U);
  EXPECT_EQ(res_all[0][0], Value("A"));
  EXPECT_EQ(res_all[0][1], Value("North"));
  EXPECT_EQ(res_all[0][2], Value(30));
  EXPECT_EQ(res_all[1][0], Value("B"));
  EXPECT_EQ(res_all[1][1], Value("South"));
  EXPECT_EQ(res_all[1][2], Value(30));

  std::vector<Row> res_dist =
      RunSql(ctx, *db_,
             "SELECT dept, sum(amount) AS total FROM sales_grp GROUP BY "
             "DISTINCT dept ORDER BY dept;");
  ASSERT_EQ(res_dist.size(), 2U);
  EXPECT_EQ(res_dist[0][0], Value("A"));
  EXPECT_EQ(res_dist[0][1], Value(30));
  EXPECT_EQ(res_dist[1][0], Value("B"));
  EXPECT_EQ(res_dist[1][1], Value(30));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, QualifyExecution) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE emp_qual (dept VARCHAR, salary INT64, name VARCHAR);");
  RunSql(
      ctx, *db_,
      "INSERT INTO emp_qual VALUES ('HR', 100, 'Alice'), ('HR', 120, 'Bob'), "
      "('HR', 90, 'Charlie'), ('IT', 200, 'Dave');");

  std::vector<Row> res =
      RunSql(ctx, *db_,
             "SELECT dept, name, salary, row_number() OVER (PARTITION BY dept "
             "ORDER BY salary DESC) as rn "
             "FROM emp_qual QUALIFY rn <= 2 ORDER BY dept, rn;");
  ASSERT_EQ(res.size(), 3U);
  EXPECT_EQ(res[0][0], Value("HR"));
  EXPECT_EQ(res[0][1], Value("Bob"));
  EXPECT_EQ(res[1][0], Value("HR"));
  EXPECT_EQ(res[1][1], Value("Alice"));
  EXPECT_EQ(res[2][0], Value("IT"));
  EXPECT_EQ(res[2][1], Value("Dave"));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, PivotAndUnpivotExecution) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(
      ctx, *db_,
      "CREATE TABLE piv_sales (dept VARCHAR, quarter VARCHAR, amount INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO piv_sales VALUES ('A', 'Q1', 10), ('A', 'Q2', 20), ('B', "
         "'Q1', 30), ('B', 'Q2', 40);");

  std::vector<Row> res_p = RunSql(
      ctx, *db_,
      "SELECT dept, Q1, Q2 FROM (SELECT dept, quarter, amount FROM piv_sales) "
      "PIVOT(sum(amount) FOR quarter IN ('Q1', 'Q2')) ORDER BY dept;");
  ASSERT_EQ(res_p.size(), 2U);
  EXPECT_EQ(res_p[0][0], Value("A"));
  EXPECT_EQ(res_p[0][1], Value(10));
  EXPECT_EQ(res_p[0][2], Value(20));
  EXPECT_EQ(res_p[1][0], Value("B"));
  EXPECT_EQ(res_p[1][1], Value(30));
  EXPECT_EQ(res_p[1][2], Value(40));

  RunSql(ctx, *db_, "CREATE TABLE widetab (dept VARCHAR, q1 INT64, q2 INT64);");
  RunSql(ctx, *db_, "INSERT INTO widetab VALUES ('A', 10, 20), ('B', 30, 40);");

  std::vector<Row> res_u =
      RunSql(ctx, *db_,
             "SELECT dept, val, col FROM widetab UNPIVOT(val FOR col IN (q1, "
             "q2)) ORDER BY dept, col;");
  ASSERT_EQ(res_u.size(), 4U);
  EXPECT_EQ(res_u[0][0], Value("A"));
  EXPECT_EQ(res_u[0][1], Value(10));
  EXPECT_EQ(res_u[0][2], Value("q1"));
  EXPECT_EQ(res_u[1][0], Value("A"));
  EXPECT_EQ(res_u[1][1], Value(20));
  EXPECT_EQ(res_u[1][2], Value("q2"));
  EXPECT_EQ(res_u[2][0], Value("B"));
  EXPECT_EQ(res_u[2][1], Value(30));
  EXPECT_EQ(res_u[2][2], Value("q1"));
  EXPECT_EQ(res_u[3][0], Value("B"));
  EXPECT_EQ(res_u[3][1], Value(40));
  EXPECT_EQ(res_u[3][2], Value("q2"));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, OrderByHonorsExplicitNullsFirstLast) {
  // PRODUCTION BUG (fixed): SortExecutor dropped the explicit NULLS FIRST /
  // NULLS LAST request (TopN honored it), so the position of NULLs changed
  // merely by adding LIMIT to the same ORDER BY.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE nls (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO nls VALUES (2), (NULL), (1);");

  const std::vector<Row> asc_last =
      RunSql(ctx, *db_, "SELECT x FROM nls ORDER BY x ASC NULLS LAST;");
  ASSERT_EQ(asc_last.size(), 3U);
  EXPECT_EQ(asc_last[0][0], Value(1));
  EXPECT_EQ(asc_last[1][0], Value(2));
  EXPECT_TRUE(asc_last[2][0].IsNull());

  const std::vector<Row> desc_first =
      RunSql(ctx, *db_, "SELECT x FROM nls ORDER BY x DESC NULLS FIRST;");
  ASSERT_EQ(desc_first.size(), 3U);
  EXPECT_TRUE(desc_first[0][0].IsNull());
  EXPECT_EQ(desc_first[1][0], Value(2));
  EXPECT_EQ(desc_first[2][0], Value(1));

  // TopN (LIMIT) and Sort must agree on the same ORDER BY.
  const std::vector<Row> topn_last =
      RunSql(ctx, *db_, "SELECT x FROM nls ORDER BY x ASC NULLS LAST LIMIT 3;");
  ASSERT_EQ(topn_last.size(), 3U);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(topn_last[i], asc_last[i]);
  }
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, DistinctIndexScanHonorsNullsLastBeforeLimit) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE nls_idx (x INT64);");
  ASSERT_EQ(db_->CreateIndex(
                ctx, "nls_idx",
                IndexSchema("nls_idx_x", {0}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  RunSql(ctx, *db_, "INSERT INTO nls_idx VALUES (2), (NULL), (1), (2), (3);");

  const std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT DISTINCT x FROM nls_idx ORDER BY x ASC NULLS LAST LIMIT 2;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, DescendingIndexScanReturnsReversedRange) {
  // The optimizer must offer reverse index scans for a descending ORDER BY
  // over a range: the LsmIndex iterator positions below `end` and walks left,
  // so the sort (or TopN) can be elided.  Data correctness is the point:
  // begin/end keys are direction-symmetric and the residual predicate
  // re-checks boundary inclusivity.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE r (c1 INT64);");
  {
    Status index = db_->CreateIndex(
        ctx, "r", IndexSchema("rpk", {0}, {}, IndexMode::kUnique));
    ASSERT_EQ(index, Status::kSuccess);
  }
  RunSql(ctx, *db_,
         "INSERT INTO r VALUES (1), (2), (94), (95), (96), (97), (98), (99),"
         " (100);");

  const std::vector<Row> desc =
      RunSql(ctx, *db_,
             "SELECT c1 FROM r WHERE c1 >= 95 AND c1 <= 99 ORDER BY c1 DESC;");
  ASSERT_EQ(desc.size(), 5U);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(desc[i][0], Value(static_cast<int64_t>(99 - i)));
  }

  // DESC with LIMIT goes through the same reverse candidate (TopN must not
  // reorder what the index already delivers in reverse).
  const std::vector<Row> desc_limit = RunSql(
      ctx, *db_, "SELECT c1 FROM r WHERE c1 >= 95 ORDER BY c1 DESC LIMIT 2;");
  ASSERT_EQ(desc_limit.size(), 2U);
  EXPECT_EQ(desc_limit[0][0], Value(int64_t{100}));
  EXPECT_EQ(desc_limit[1][0], Value(int64_t{99}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, UnknownColumnOnSingleColumnTableIsAnError) {
  // A bare unknown name must not silently become a proto __get_field_safe
  // probe that returns NULL; only proto value tables absorb unknown fields.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE sc (only_col INT64);");
  RunSql(ctx, *db_, "INSERT INTO sc VALUES (7);");

  SqlEngine engine(*db_);
  const StatusOr<Executor> prepared =
      engine.Prepare(ctx, "SELECT typo_name FROM sc;");
  EXPECT_FALSE(prepared.HasValue());
  EXPECT_EQ(prepared.GetStatus(), Status::kNotExists);
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, StructConstantFoldingEscapesQuotesAndBackslashes) {
  // Folded STRUCT JSON must stay parseable when a field value contains
  // quotes, backslashes or control characters.
  TransactionContext ctx = db_->BeginContext();
  const std::vector<Row> rows =
      RunSql(ctx, *db_, R"(SELECT STRUCT('a"b\\c' AS s, 3 AS n);)");
  ASSERT_EQ(rows.size(), 1U);
  ASSERT_TRUE(rows[0][0].type == ValueType::kVarChar);
  const std::string json(rows[0][0].value.varchar_value);
  // The quote and backslash must be escaped inside the JSON string value.
  EXPECT_NE(json.find(R"(a\"b\\c)"), std::string::npos) << json;
  EXPECT_NE(json.find(R"("n":3)"), std::string::npos) << json;
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, MixedDottedAndPlainSetTargetsAreRejected) {
  // The dotted STRUCT-field update path cannot apply plain column targets;
  // previously the plain assignments were silently dropped.  A table whose
  // dotted target is an ordinary column still trips the guard (the check is
  // syntactic, before any STRUCT machinery runs).
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE mix (id INT64, s INT64);");
  RunSql(ctx, *db_, "INSERT INTO mix VALUES (1, 10);");

  SqlEngine engine(*db_);
  const StatusOr<Executor> prepared =
      engine.Prepare(ctx, "UPDATE mix SET s.a = 20, id = 2 WHERE id = 1;");
  EXPECT_FALSE(prepared.HasValue());
  EXPECT_EQ(prepared.GetStatus(), Status::kInvalidArgument);
  EXPECT_NE(prepared.GetStatus().GetMessage().find("cannot mix dotted"),
            std::string::npos);
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, EighthScanNumericEdgeCases) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE one (x INT64);");
  RunSql(ctx, *db_, "INSERT INTO one VALUES (7);");

  // A1: IN must promote INT64/DOUBLE like `=` (was folded to FALSE).
  EXPECT_EQ(RunScalar(ctx, *db_, "SELECT x FROM one WHERE 1 IN (1.0);").size(),
            1U);  // 1 == 1.0 promotes to TRUE, so the row survives
  EXPECT_EQ(RunScalar(ctx, *db_, "SELECT x FROM one WHERE 1 IN (2.0);").size(),
            0U);  // 1 != 2.0
  EXPECT_EQ(RunScalar(ctx, *db_, "SELECT x FROM one WHERE 7 IN (7.0);").size(),
            1U);

  // A11: ROUND/TRUNC of INT64_MAX must not wrap through double.
  const auto round_max =
      RunScalar(ctx, *db_, "SELECT ROUND(9223372036854775807);");
  ASSERT_EQ(round_max.size(), 1U);
  EXPECT_EQ(round_max[0][0], Value(std::numeric_limits<int64_t>::max()));

  // C26: ABS(INT64_MIN) throws (UB / wrong sign before).
  EXPECT_THROW(RunScalar(ctx, *db_, "SELECT ABS(-9223372036854775808);"),
               std::exception);

  // A9: MOD(INT64_MIN, -1) raises like the AST reference (was a raw SIGFPE;
  // the fast path briefly returned the mathematical 0, diverging from
  // Value::operator% and the sibling ABS/DIV edges pinned below).
  EXPECT_THROW(RunScalar(ctx, *db_, "SELECT MOD(-9223372036854775808, -1);"),
               std::exception);

  // B14: GREATEST/LEAST promote mixed INT64/DOUBLE (was a type error).
  const auto greatest = RunScalar(ctx, *db_, "SELECT GREATEST(1, 2.5);");
  ASSERT_EQ(greatest.size(), 1U);
  EXPECT_EQ(greatest[0][0], Value(2.5));

  // B16: SAFE_ADD overflow to +inf returns NULL.
  const auto safe = RunScalar(ctx, *db_, "SELECT SAFE_ADD(1e308, 1e308);");
  ASSERT_EQ(safe.size(), 1U);
  EXPECT_TRUE(safe[0][0].IsNull());

  // B17: DIV out of INT64 range throws (was UB wrap).
  EXPECT_THROW(RunScalar(ctx, *db_, "SELECT DIV(1e300, 2.0);"), std::exception);

  // A10: DATE_BUCKET with a zero interval throws (was SIGFPE).
  EXPECT_THROW(
      RunScalar(ctx, *db_,
                "SELECT DATE_BUCKET(DATE '2024-03-15', INTERVAL 0 MONTH);"),
      std::exception);
  EXPECT_THROW(
      RunScalar(ctx, *db_,
                "SELECT DATE_BUCKET(DATE '0001-01-01', INTERVAL 3000 YEAR);"),
      std::exception);

  // A4: AVG OVER a negative int64 sum (was uint64 misinterpretation).
  RunSql(ctx, *db_, "CREATE TABLE av (v INT64);");
  RunSql(ctx, *db_, "INSERT INTO av VALUES (-5), (3);");
  const auto avg_over =
      RunScalar(ctx, *db_, "SELECT AVG(v) OVER () FROM av ORDER BY v;");
  ASSERT_GE(avg_over.size(), 1U);
  EXPECT_EQ(avg_over[0][0], Value(-1.0));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, EighthScanSingleColumnTypoRejected) {
  // D2-query: an unknown column on a plain single-column table must error,
  // not silently resolve through __get_field_safe to NULL.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE solo (only INT64);");
  RunSql(ctx, *db_, "INSERT INTO solo VALUES (5);");
  SqlEngine engine(*db_);
  const StatusOr<Executor> prepared =
      engine.Prepare(ctx, "SELECT only FROM solo WHERE typo_col = 5;");
  EXPECT_FALSE(prepared.HasValue());
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineNonEqualityJoinCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE a (k INT64, v VARCHAR(10));");
  RunSql(ctx, *db_, "CREATE TABLE b (k INT64, w VARCHAR(10));");
  RunSql(ctx, *db_, "INSERT INTO a VALUES (1, 'a1'), (3, 'a3');");
  RunSql(ctx, *db_, "INSERT INTO b VALUES (2, 'b2'), (4, 'b4');");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx,
      "EXPLAIN SELECT a.v, b.w FROM a JOIN b ON a.k < b.k ORDER BY a.v, b.w;");
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

  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v, b.w FROM a JOIN b ON a.k < b.k ORDER BY a.v, b.w;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value("a1"), Value("b2")}));
  EXPECT_EQ(rows[1], Row({Value("a1"), Value("b4")}));
  EXPECT_EQ(rows[2], Row({Value("a3"), Value("b4")}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineJoinOnlySourceCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE x (id INT64, val INT64);");
  RunSql(ctx, *db_, "CREATE TABLE y (id INT64, active INT64);");
  RunSql(ctx, *db_, "INSERT INTO x VALUES (1, 100), (2, 200), (3, 300);");
  RunSql(ctx, *db_, "INSERT INTO y VALUES (1, 1), (3, 1);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain = engine.Prepare(
      ctx, "EXPLAIN SELECT x.val FROM x JOIN y ON x.id = y.id ORDER BY x.val;");
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

  const std::vector<Row> rows = RunSql(
      ctx, *db_, "SELECT x.val FROM x JOIN y ON x.id = y.id ORDER BY x.val;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(100));
  EXPECT_EQ(rows[1][0], Value(300));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineCompoundJoinPredicateCascades) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE p (k INT64, num INT64);");
  RunSql(ctx, *db_, "CREATE TABLE q (k INT64, threshold INT64);");
  RunSql(ctx, *db_, "INSERT INTO p VALUES (1, 10), (1, 50), (2, 30);");
  RunSql(ctx, *db_, "INSERT INTO q VALUES (1, 20), (2, 40);");

  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT p.num FROM p JOIN q ON p.k = q.k AND "
                     "p.num > q.threshold;");
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

  const std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT p.num FROM p JOIN q ON p.k = q.k AND p.num > q.threshold;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(50));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineRecursiveCteCascadesPlan) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  StatusOr<Executor> explain =
      engine.Prepare(ctx,
                     "EXPLAIN WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL "
                     "SELECT n + 1 FROM t "
                     "WHERE n < 5) SELECT SUM(n) FROM t;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  Row exp_row;
  std::string plan_text;
  while (explain.Value()->Next(&exp_row, nullptr)) {
    plan_text += exp_row[0].AsString() + "\n";
  }
  EXPECT_NE(plan_text.find("RecursiveCtePlan"), std::string::npos) << plan_text;
  EXPECT_NE(plan_text.find("Selected Cascades Core"), std::string::npos)
      << plan_text;
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  std::vector<Row> total =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
             "FROM t WHERE n < 5) SELECT SUM(n) FROM t;");
  ASSERT_EQ(total.size(), 1U);
  EXPECT_EQ(total[0][0], Value(15));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, SqlEngineWholeRowRefNestedInExpressionResolves) {
  // A bare FROM-relation name denotes that relation's whole row (rewritten
  // into __struct_json__).  When the reference sits INSIDE a composite
  // expression, the rewrite used to be resolved into a temporary and
  // dropped, leaving an unresolvable column behind.
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE wr (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO wr VALUES (1);");
  // Row compared with itself: the struct JSON must round-trip through the
  // rewrite on both sides of the equality.
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT s = s FROM wr s;"),
            (std::vector<Row>{Row({Value(true)})}));
  // A CASE branch and a NOT wrapper carry nested rewrites the same way.
  EXPECT_EQ(
      RunSql(ctx, *db_, "SELECT CASE WHEN s = s THEN 7 ELSE 0 END FROM wr s;"),
      (std::vector<Row>{Row({Value(7)})}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// Index nested-loop (lookup join) end to end through the SQL engine. The
// composite-index shapes here are the ones the probe is designed for: an
// equality prefix plus a range bound, an equality on a non-key column, and a
// second range the single bound slot cannot take. Whatever the planner picks,
// the answers must match the nested-loop semantics the mirror below spells out.
TEST_F(QueryTest, CompositeIndexLookupJoinRangeAndResidualSemantics) {
  TransactionContext setup = db_->BeginContext();
  RunSql(setup, *db_, "CREATE TABLE lk (x INT64, y INT64, z FLOAT64);");
  RunSql(setup, *db_,
         "INSERT INTO lk VALUES (1, 5, 2.5), (1, 6, 3.5), (2, 1, 0.5), "
         "(3, 9, 1.5);");
  RunSql(setup, *db_,
         "CREATE TABLE rk (a INT64, b INT64, c INT64, d FLOAT64);");
  {
    std::string values =
        "(1, 10, 1, 9.5), (1, 20, 7, 1.5), (1, 30, 1, 3.5), (1, 4, 5, 0.5), "
        "(2, 5, 2, 0.25), (2, 40, 9, 7.5), (2, 0, 3, 2.5), (3, 2, 3, 0.5), "
        "(4, 100, 4, 1.0)";
    RunSql(setup, *db_, "INSERT INTO rk VALUES " + values + ";");
  }
  ASSERT_EQ(
      db_->CreateIndex(setup, "rk",
                       IndexSchema("rk_ab", {0, 1}, {}, IndexMode::kNonUnique)),
      Status::kSuccess);
  ASSERT_SUCCESS(setup.PreCommit());
  {
    TransactionContext stat_ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->RefreshStatistics(stat_ctx, "lk"));
    ASSERT_SUCCESS(db_->RefreshStatistics(stat_ctx, "rk"));
    ASSERT_EQ(stat_ctx.PreCommit(), Status::kSuccess);
  }

  TransactionContext ctx = db_->BeginContext();
  // Equality prefix on (a) plus a range bound on (b). The mirror below lists
  // the pairs the predicate accepts, computed in plain C++.
  const std::vector<Row> probe_left{{Value(1), Value(5), Value(2.5)},
                                    {Value(1), Value(6), Value(3.5)},
                                    {Value(2), Value(1), Value(0.5)},
                                    {Value(3), Value(9), Value(1.5)}};
  const std::vector<Row> probe_right{
      {Value(1), Value(10), Value(1), Value(9.5)},
      {Value(1), Value(20), Value(7), Value(1.5)},
      {Value(1), Value(30), Value(1), Value(3.5)},
      {Value(1), Value(4), Value(5), Value(0.5)},
      {Value(2), Value(5), Value(2), Value(0.25)},
      {Value(2), Value(40), Value(9), Value(7.5)},
      {Value(2), Value(0), Value(3), Value(2.5)},
      {Value(3), Value(2), Value(3), Value(0.5)},
      {Value(4), Value(100), Value(4), Value(1.0)}};
  const auto mirror = [&](const std::string& sql_extra, int comparison) {
    std::vector<std::pair<int64_t, int64_t>> expected;
    expected.reserve(probe_left.size() * probe_right.size());
    for (const Row& l : probe_left) {
      for (const Row& r : probe_right) {
        if (l[0].value.int_value != r[0].value.int_value) {
          continue;
        }
        if (!sql_extra.empty()) {
          continue;
        }
        const int64_t l_y = l[1].value.int_value;
        const int64_t r_b = r[1].value.int_value;
        const bool keep = comparison == 0   ? r_b > l_y
                          : comparison == 1 ? r_b >= l_y
                                            : r_b < l_y;
        if (keep) {
          expected.emplace_back(r[0].value.int_value, r[1].value.int_value);
        }
      }
    }
    std::ranges::sort(expected);
    return expected;
  };
  const auto collect = [&](const std::string& sql) {
    std::vector<std::pair<int64_t, int64_t>> got;
    for (const Row& row : RunSql(ctx, *db_, sql)) {
      got.emplace_back(row[0].value.int_value, row[1].value.int_value);
    }
    std::ranges::sort(got);
    return got;
  };

  // `b > y` -- the mirror's comparison 0.
  EXPECT_EQ(collect("SELECT rk.a, rk.b FROM lk JOIN rk ON lk.x = rk.a AND "
                    "rk.b > lk.y;"),
            mirror("", 0));
  // `b >= y` includes the boundary rows the seek's exclusive end drops.
  EXPECT_EQ(collect("SELECT rk.a, rk.b FROM lk JOIN rk ON lk.x = rk.a AND "
                    "rk.b >= lk.y;"),
            mirror("", 1));
  // `b < y` -- the mirrored probe, seeking from the prefix start to the bound.
  EXPECT_EQ(collect("SELECT rk.a, rk.b FROM lk JOIN rk ON lk.x = rk.a AND "
                    "rk.b < lk.y;"),
            mirror("", 2));

  // An equality on a NON-key column: the probe pins (a) only, so `x = c` has
  // to filter the over-fetch. (The old seek built its key from the join's own
  // key order and returned nothing here.)
  {
    std::vector<std::pair<int64_t, int64_t>> expected;
    expected.reserve(probe_left.size() * probe_right.size());
    for (const Row& l : probe_left) {
      for (const Row& r : probe_right) {
        if (l[0].value.int_value == r[0].value.int_value &&
            l[0].value.int_value == r[2].value.int_value) {
          expected.emplace_back(r[0].value.int_value, r[1].value.int_value);
        }
      }
    }
    std::ranges::sort(expected);
    EXPECT_EQ(collect("SELECT rk.a, rk.b FROM lk JOIN rk ON lk.x = rk.a AND "
                      "lk.x = rk.c;"),
              expected);
  }

  // A range on a FLOAT64 column is outside the probe's range whitelist: it is
  // an explicit fallback to a residual predicate, never a dropped one. The
  // right `d` values are the third kind of conjunct the single bound slot and
  // the key prefix both decline, so the whole predicate decides the rows.
  {
    std::vector<std::pair<int64_t, int64_t>> expected;
    expected.reserve(probe_left.size() * probe_right.size());
    for (const Row& l : probe_left) {
      for (const Row& r : probe_right) {
        if (l[0].value.int_value == r[0].value.int_value &&
            r[1].value.int_value > l[1].value.int_value &&
            r[3].value.double_value < l[2].value.double_value) {
          expected.emplace_back(r[0].value.int_value, r[1].value.int_value);
        }
      }
    }
    std::ranges::sort(expected);
    EXPECT_EQ(collect("SELECT rk.a, rk.b FROM lk JOIN rk ON lk.x = rk.a AND "
                      "rk.b > lk.y AND rk.d < lk.z;"),
              expected);
  }

  // A NULL equality key never matches, and neither does a NULL range bound.
  RunSql(ctx, *db_, "INSERT INTO lk VALUES (NULL, 5, 1.0), (1, NULL, 1.0);");
  EXPECT_EQ(collect("SELECT rk.a, rk.b FROM lk JOIN rk ON lk.x = rk.a AND "
                    "rk.b > lk.y;"),
            mirror("", 0));
  // An empty right side makes every probe miss, and the query returns nothing
  // rather than failing.
  RunSql(ctx, *db_, "CREATE TABLE rk_empty (a INT64, b INT64);");
  ASSERT_EQ(db_->CreateIndex(
                ctx, "rk_empty",
                IndexSchema("rk_empty_ab", {0, 1}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  EXPECT_TRUE(RunSql(ctx, *db_,
                     "SELECT lk.x FROM lk JOIN rk_empty ON lk.x = "
                     "rk_empty.a AND rk_empty.b > lk.y;")
                  .empty());
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// The two-sided interval probe reached through SQL: `lo < b < hi` adopts BOTH
// bounds, so the index join narrows its seek to the interval instead of
// over-fetching from one side.  The plan text pins the interval and the mirror
// below lists the accepted pairs in plain C++.
TEST_F(QueryTest, CompositeIndexLookupJoinIntervalProbe) {
  TransactionContext setup = db_->BeginContext();
  RunSql(setup, *db_, "CREATE TABLE il (x INT64, lo INT64, hi INT64);");
  RunSql(setup, *db_,
         "INSERT INTO il VALUES (1, 5, 15), (2, 9, 26), (3, 20, 31);");
  RunSql(setup, *db_, "CREATE TABLE ir (a INT64, b INT64);");
  RunSql(setup, *db_,
         "INSERT INTO ir VALUES (1, 4), (1, 10), (1, 15), (1, 20), (2, 10), "
         "(2, 25), (3, 21), (3, 30);");
  ASSERT_EQ(
      db_->CreateIndex(setup, "ir",
                       IndexSchema("ir_ab", {0, 1}, {}, IndexMode::kNonUnique)),
      Status::kSuccess);
  ASSERT_SUCCESS(setup.PreCommit());
  {
    TransactionContext stat_ctx = db_->BeginContext();
    ASSERT_SUCCESS(db_->RefreshStatistics(stat_ctx, "il"));
    ASSERT_SUCCESS(db_->RefreshStatistics(stat_ctx, "ir"));
    ASSERT_EQ(stat_ctx.PreCommit(), Status::kSuccess);
  }

  TransactionContext ctx = db_->BeginContext();
  const std::vector<Row> left{{Value(1), Value(5), Value(15)},
                              {Value(2), Value(9), Value(26)},
                              {Value(3), Value(20), Value(31)}};
  const std::vector<Row> right{{Value(1), Value(4)},  {Value(1), Value(10)},
                               {Value(1), Value(15)}, {Value(1), Value(20)},
                               {Value(2), Value(10)}, {Value(2), Value(25)},
                               {Value(3), Value(21)}, {Value(3), Value(30)}};
  const auto mirror = [&](bool lower_inclusive, bool upper_inclusive) {
    std::vector<std::pair<int64_t, int64_t>> expected;
    expected.reserve(left.size() * right.size());
    for (const Row& l : left) {
      for (const Row& r : right) {
        if (l[0].value.int_value != r[0].value.int_value) {
          continue;
        }
        const int64_t lo = l[1].value.int_value;
        const int64_t hi = l[2].value.int_value;
        const int64_t b = r[1].value.int_value;
        const bool keep = (lower_inclusive ? b >= lo : b > lo) &&
                          (upper_inclusive ? b <= hi : b < hi);
        if (keep) {
          expected.emplace_back(r[0].value.int_value, r[1].value.int_value);
        }
      }
    }
    std::ranges::sort(expected);
    return expected;
  };
  const auto collect = [&](const std::string& sql) {
    std::vector<std::pair<int64_t, int64_t>> got;
    for (const Row& row : RunSql(ctx, *db_, sql)) {
      got.emplace_back(row[0].value.int_value, row[1].value.int_value);
    }
    std::ranges::sort(got);
    return got;
  };

  // The SQL planner is free to pick the hash join here (it does at this size);
  // the interval probe itself is pinned by the rule-level and executor-level
  // regressions, and this test holds the SQL shape to the same answer either
  // way.
  EXPECT_EQ(collect("SELECT ir.a, ir.b FROM il JOIN ir ON il.x = ir.a AND "
                    "ir.b > il.lo AND ir.b < il.hi;"),
            mirror(false, false));
  EXPECT_EQ(collect("SELECT ir.a, ir.b FROM il JOIN ir ON il.x = ir.a AND "
                    "ir.b >= il.lo AND ir.b <= il.hi;"),
            mirror(true, true));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, ZigZagJoinSqlContracts) {
  TransactionContext setup = db_->BeginContext();
  RunSql(setup, *db_, "CREATE TABLE zz_left (k INT64);");
  RunSql(setup, *db_, "CREATE TABLE zz_right (k INT64);");
  ASSERT_EQ(db_->CreateIndex(
                setup, "zz_left",
                IndexSchema("zz_left_k", {0}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  ASSERT_EQ(db_->CreateIndex(
                setup, "zz_right",
                IndexSchema("zz_right_k", {0}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  RunSql(setup, *db_, "INSERT INTO zz_left VALUES (NULL), (1), (1), (3), (5);");
  RunSql(setup, *db_,
         "INSERT INTO zz_right VALUES (NULL), (1), (1), (2), (3), (3), "
         "(6);");
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  TransactionContext stats_context = db_->BeginContext();
  ASSERT_SUCCESS(db_->RefreshStatistics(stats_context, "zz_left"));
  ASSERT_SUCCESS(db_->RefreshStatistics(stats_context, "zz_right"));
  ASSERT_SUCCESS(stats_context.txn_.PreCommit());

  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  const std::string equality_sql =
      "SELECT zz_left.k, zz_right.k FROM zz_left JOIN zz_right ON "
      "zz_left.k = zz_right.k ORDER BY zz_left.k, zz_right.k;";
  const std::vector<Row> equality = RunSql(ctx, *db_, equality_sql);
  ASSERT_EQ(equality.size(), 6U);
  EXPECT_EQ(equality[0], (Row({Value(1), Value(1)})));
  EXPECT_EQ(equality[1], (Row({Value(1), Value(1)})));
  EXPECT_EQ(equality[2], (Row({Value(1), Value(1)})));
  EXPECT_EQ(equality[3], (Row({Value(1), Value(1)})));
  EXPECT_EQ(equality[4], (Row({Value(3), Value(3)})));
  EXPECT_EQ(equality[5], (Row({Value(3), Value(3)})));

  const std::vector<Row> range =
      RunSql(ctx, *db_,
             "SELECT zz_left.k, zz_right.k FROM zz_left JOIN zz_right ON "
             "zz_left.k < zz_right.k ORDER BY zz_left.k, zz_right.k;");
  ASSERT_EQ(range.size(), 10U);
  EXPECT_EQ(range.front(), (Row({Value(1), Value(2)})));
  EXPECT_EQ(range.back(), (Row({Value(5), Value(6)})));

  const std::vector<Row> residual =
      RunSql(ctx, *db_,
             "SELECT zz_left.k, zz_right.k FROM zz_left JOIN zz_right ON "
             "zz_left.k = zz_right.k AND zz_left.k <> 1 ORDER BY zz_left.k, "
             "zz_right.k;");
  ASSERT_EQ(residual.size(), 2U);
  EXPECT_EQ(residual[0], (Row({Value(3), Value(3)})));
  EXPECT_EQ(residual[1], (Row({Value(3), Value(3)})));

  // Costing now charges distinct-key navigation. Use duplicate-heavy
  // indexes to pin the ordered-index join; the tiny semantic fixture above
  // may legitimately choose a hash join.
  for (const std::string name : {"zz_order_left", "zz_order_right"}) {
    RunSql(ctx, *db_, "CREATE TABLE " + name + " (k INT64)");
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable(name));
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(table->Insert(ctx.txn_, Row({Value(i % 10)})).GetStatus());
    }
    ASSERT_SUCCESS(db_->CreateIndex(
        ctx, name, IndexSchema(name + "_k", {0}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(db_->RefreshStatistics(ctx, name));
  }
  StatusOr<Executor> explained =
      engine.Prepare(ctx,
                     "EXPLAIN SELECT zz_order_left.k, zz_order_right.k FROM "
                     "zz_order_left JOIN zz_order_right "
                     "ON zz_order_left.k = zz_order_right.k ORDER BY "
                     "zz_order_left.k, zz_order_right.k;");
  ASSERT_TRUE(explained.HasValue()) << engine.LastError();
  std::string plan_text;
  Row plan_row;
  while (explained.Value()->Next(&plan_row, nullptr)) {
    plan_text += plan_row[0].AsString() + "\n";
  }
  EXPECT_NE(plan_text.find("ZigZagJoin"), std::string::npos) << plan_text;

  RunSql(ctx, *db_, "CREATE TABLE zz_empty (k INT64);");
  ASSERT_EQ(db_->CreateIndex(
                ctx, "zz_empty",
                IndexSchema("zz_empty_k", {0}, {}, IndexMode::kNonUnique)),
            Status::kSuccess);
  EXPECT_TRUE(RunSql(ctx, *db_,
                     "SELECT zz_left.k FROM zz_left JOIN zz_empty ON "
                     "zz_left.k = zz_empty.k;")
                  .empty());

  RunSql(ctx, *db_, "CREATE TABLE zz_unique_left (k INT64);");
  RunSql(ctx, *db_, "CREATE TABLE zz_unique_right (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO zz_unique_left VALUES (1), (2);");
  RunSql(ctx, *db_, "INSERT INTO zz_unique_right VALUES (2), (3);");
  ASSERT_EQ(db_->CreateIndex(
                ctx, "zz_unique_left",
                IndexSchema("zz_unique_left_k", {0}, {}, IndexMode::kUnique)),
            Status::kSuccess);
  ASSERT_EQ(db_->CreateIndex(
                ctx, "zz_unique_right",
                IndexSchema("zz_unique_right_k", {0}, {}, IndexMode::kUnique)),
            Status::kSuccess);
  const std::vector<Row> unique = RunSql(
      ctx, *db_,
      "SELECT zz_unique_left.k, zz_unique_right.k FROM zz_unique_left JOIN "
      "zz_unique_right ON zz_unique_left.k = zz_unique_right.k ORDER BY "
      "zz_unique_left.k;");
  ASSERT_EQ(unique.size(), 1U);
  EXPECT_EQ(unique[0], (Row({Value(2), Value(2)})));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// A unique inner side means a probe can name at most one row: the join must
// then emit exactly one row per left row -- never a second copy for a repeated
// outer key, and nothing at all for a key the inner side lacks.  The planner is
// free to pick the hash join here; the executor short-circuit itself is pinned
// by `ExecutorTest.LookupJoinUniquePointProbe*`, and this test holds the SQL
// shape to the same answer either way.
TEST_F(QueryTest, UniqueInnerJoinEmitsOneRowPerOuterRow) {
  TransactionContext setup = db_->BeginContext();
  RunSql(setup, *db_, "CREATE TABLE ui_left (k INT64, tag STRING);");
  RunSql(setup, *db_, "CREATE TABLE ui_right (k INT64, v INT64);");
  ASSERT_EQ(
      db_->CreateIndex(setup, "ui_right",
                       IndexSchema("ui_right_k", {0}, {}, IndexMode::kUnique)),
      Status::kSuccess);
  // The outer side repeats key 2 (once per tag) and carries a key the inner
  // side does not have.
  RunSql(setup, *db_,
         "INSERT INTO ui_left VALUES (2, 'a'), (2, 'b'), (1, 'c'), (5, 'd');");
  RunSql(setup, *db_, "INSERT INTO ui_right VALUES (2, 20), (1, 10);");
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  TransactionContext stats_context = db_->BeginContext();
  ASSERT_SUCCESS(db_->RefreshStatistics(stats_context, "ui_left"));
  ASSERT_SUCCESS(db_->RefreshStatistics(stats_context, "ui_right"));
  ASSERT_SUCCESS(stats_context.txn_.PreCommit());

  TransactionContext ctx = db_->BeginContext();
  const std::vector<Row> joined = RunSql(
      ctx, *db_,
      "SELECT ui_left.k, ui_left.tag, ui_right.v FROM ui_left JOIN ui_right "
      "ON ui_left.k = ui_right.k ORDER BY ui_left.k, ui_left.tag;");
  ASSERT_EQ(joined.size(), 3U);
  EXPECT_EQ(joined[0], (Row({Value(1), Value("c"), Value(10)})));
  EXPECT_EQ(joined[1], (Row({Value(2), Value("a"), Value(20)})));
  EXPECT_EQ(joined[2], (Row({Value(2), Value("b"), Value(20)})));

  // The same shape counted from the inner side: a unique key must not add a
  // second row to the aggregate either.
  const std::vector<Row> counted = RunSql(
      ctx, *db_,
      "SELECT ui_right.k, COUNT(*) FROM ui_left JOIN ui_right "
      "ON ui_left.k = ui_right.k GROUP BY ui_right.k ORDER BY ui_right.k;");
  ASSERT_EQ(counted.size(), 2U);
  EXPECT_EQ(counted[0], (Row({Value(1), Value(1)})));
  EXPECT_EQ(counted[1], (Row({Value(2), Value(2)})));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, UnsignedInt64_CreateInsertOrderAndIndex) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE u_tbl (u UINT64);");
  Status idx_st = db_->CreateIndex(
      ctx, "u_tbl", IndexSchema("upk", {0}, {}, IndexMode::kUnique));
  ASSERT_EQ(idx_st, Status::kSuccess);

  // Rejects negative numbers
  {
    SqlEngine engine(*db_);
    auto prep = engine.Prepare(ctx, "INSERT INTO u_tbl VALUES (-1);");
    EXPECT_FALSE(prep.HasValue());
    EXPECT_NE(engine.LastError().find("assignment out of range for column u"),
              std::string::npos);
  }

  // Accepts 0, 10, and 2^64 - 1
  RunSql(ctx, *db_,
         "INSERT INTO u_tbl VALUES (10), (18446744073709551615), (0);");

  // ORDER BY u ASC
  const std::vector<Row> asc =
      RunSql(ctx, *db_, "SELECT u FROM u_tbl ORDER BY u ASC;");
  ASSERT_EQ(asc.size(), 3U);
  EXPECT_EQ(asc[0][0].AsString(), "0");
  EXPECT_EQ(asc[1][0].AsString(), "10");
  EXPECT_EQ(asc[2][0].AsString(), "18446744073709551615");
  EXPECT_TRUE(asc[0][0].IsUnsigned());
  EXPECT_TRUE(asc[1][0].IsUnsigned());
  EXPECT_TRUE(asc[2][0].IsUnsigned());

  // ORDER BY u DESC
  const std::vector<Row> desc =
      RunSql(ctx, *db_, "SELECT u FROM u_tbl ORDER BY u DESC;");
  ASSERT_EQ(desc.size(), 3U);
  EXPECT_EQ(desc[0][0].AsString(), "18446744073709551615");
  EXPECT_EQ(desc[1][0].AsString(), "10");
  EXPECT_EQ(desc[2][0].AsString(), "0");

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, UnsignedInt64_CastQuerySemantics) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE u_cast_tbl (u UINT64);");
  RunSql(ctx, *db_,
         "INSERT INTO u_cast_tbl VALUES (10), (18446744073709551615);");

  // String to UINT64 cast
  std::vector<Row> parsed =
      RunSql(ctx, *db_, "SELECT CAST('18446744073709551615' AS UINT64);");
  ASSERT_EQ(parsed.size(), 1U);
  EXPECT_TRUE(parsed[0][0].IsUnsigned());
  EXPECT_EQ(parsed[0][0].AsString(), "18446744073709551615");

  // SAFE_CAST to INT64 returns NULL for UINT64_MAX
  std::vector<Row> safe_rows = RunSql(
      ctx, *db_,
      "SELECT SAFE_CAST(u AS INT64), CAST(u AS STRING), CAST(u AS DOUBLE) "
      "FROM u_cast_tbl WHERE u = 18446744073709551615;");
  ASSERT_EQ(safe_rows.size(), 1U);
  EXPECT_TRUE(safe_rows[0][0].IsNull());
  EXPECT_EQ(std::string(safe_rows[0][1].value.varchar_value),
            "18446744073709551615");
  EXPECT_GT(safe_rows[0][2].value.double_value, 1.8e19);

  // In-range UINT64 converts to signed INT64
  std::vector<Row> in_range = RunSql(
      ctx, *db_, "SELECT CAST(u AS INT64) FROM u_cast_tbl WHERE u = 10;");
  ASSERT_EQ(in_range.size(), 1U);
  EXPECT_FALSE(in_range[0][0].IsUnsigned());
  EXPECT_EQ(in_range[0][0].value.int_value, 10LL);

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, UnsignedInt64SumOverJoinGroupByDoesNotWrap) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE uj_fact (k INT64, u UINT64);");
  RunSql(ctx, *db_, "CREATE TABLE uj_dim (k INT64, label VARCHAR);");
  RunSql(ctx, *db_,
         "INSERT INTO uj_fact VALUES "
         "(1, 18446744073709551600), (1, 5), (2, 100);");
  RunSql(ctx, *db_, "INSERT INTO uj_dim VALUES (1, 'a'), (2, 'b');");
  const auto rows =
      RunSql(ctx, *db_,
             "SELECT f.k, SUM(f.u) AS s FROM uj_fact f "
             "JOIN uj_dim d ON f.k = d.k GROUP BY f.k ORDER BY f.k");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_TRUE(rows[0][1].IsUnsigned());
  EXPECT_EQ(rows[0][1].AsString(), "18446744073709551605");
  EXPECT_EQ(rows[1][1].AsString(), "100");
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, UnsignedInt64_AggregationSemantics) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE u_agg_tbl (g INT64, u UINT64);");
  RunSql(ctx, *db_,
         "INSERT INTO u_agg_tbl VALUES "
         "(1, 10), (1, 18446744073709551600), (1, 5), "
         "(2, 0), (2, 18446744073709551615);");

  // MIN and MAX on unsigned int64 columns (scalar aggregate)
  std::vector<Row> min_max =
      RunSql(ctx, *db_, "SELECT MIN(u), MAX(u) FROM u_agg_tbl WHERE g = 1;");
  ASSERT_EQ(min_max.size(), 1U);
  EXPECT_TRUE(min_max[0][0].IsUnsigned());
  EXPECT_EQ(min_max[0][0].AsString(), "5");
  EXPECT_TRUE(min_max[0][1].IsUnsigned());
  EXPECT_EQ(min_max[0][1].AsString(), "18446744073709551600");

  // SUM on unsigned int64 columns
  std::vector<Row> sum_rows =
      RunSql(ctx, *db_, "SELECT SUM(u) FROM u_agg_tbl WHERE g = 1;");
  ASSERT_EQ(sum_rows.size(), 1U);
  EXPECT_TRUE(sum_rows[0][0].IsUnsigned());
  EXPECT_EQ(sum_rows[0][0].AsString(), "18446744073709551615");

  // AVG on unsigned int64 columns (must be positive double)
  std::vector<Row> avg_rows =
      RunSql(ctx, *db_, "SELECT AVG(u) FROM u_agg_tbl WHERE g = 2;");
  ASSERT_EQ(avg_rows.size(), 1U);
  EXPECT_GT(avg_rows[0][0].value.double_value, 9.0e18);

  // BIT_AND and BIT_OR on unsigned int64 columns
  std::vector<Row> bit_rows = RunSql(
      ctx, *db_, "SELECT BIT_AND(u), BIT_OR(u) FROM u_agg_tbl WHERE g = 2;");
  ASSERT_EQ(bit_rows.size(), 1U);
  EXPECT_TRUE(bit_rows[0][0].IsUnsigned());
  EXPECT_EQ(bit_rows[0][0].AsString(), "0");
  EXPECT_TRUE(bit_rows[0][1].IsUnsigned());
  EXPECT_EQ(bit_rows[0][1].AsString(), "18446744073709551615");

  // GROUP BY aggregations on unsigned int64
  std::vector<Row> group_rows =
      RunSql(ctx, *db_,
             "SELECT g, MIN(u), MAX(u) FROM u_agg_tbl GROUP BY g ORDER BY g;");
  ASSERT_EQ(group_rows.size(), 2U);
  EXPECT_EQ(group_rows[0][0].value.int_value, 1LL);
  EXPECT_EQ(group_rows[0][1].AsString(), "5");
  EXPECT_EQ(group_rows[0][2].AsString(), "18446744073709551600");
  EXPECT_EQ(group_rows[1][0].value.int_value, 2LL);
  EXPECT_EQ(group_rows[1][1].AsString(), "0");
  EXPECT_EQ(group_rows[1][2].AsString(), "18446744073709551615");

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, OuterJoinNullPadGolden) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE oj_a (id INT64, name STRING, val INT64);");
  RunSql(ctx, *db_,
         "CREATE TABLE oj_b (id INT64, b_name STRING, score INT64);");
  RunSql(ctx, *db_, "CREATE TABLE oj_c (id INT64, extra STRING);");

  RunSql(ctx, *db_,
         "INSERT INTO oj_a VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, "
         "'Charlie', 30);");
  RunSql(ctx, *db_,
         "INSERT INTO oj_b VALUES (1, 'B-Alice', 100), (2, 'B-Bob', 5), (4, "
         "'B-David', 400);");
  RunSql(ctx, *db_, "INSERT INTO oj_c VALUES (1, 'C-One'), (4, 'C-Four');");

  // 1. Basic LEFT JOIN with null-padding:
  // Charlie (id=3) has no match in oj_b.
  // Projections test expressions on NULL-padded columns:
  // - COALESCE(b.score, -1) -> -1
  // - IFNULL(b.score, -2) -> -2
  // - b.score + 10 -> NULL
  // - b.score = 100 -> NULL
  // - b.score IS NULL -> TRUE
  // - b.score IS NOT NULL -> FALSE
  // - CASE WHEN b.score IS NULL THEN 'absent' ELSE 'present' END -> 'absent'
  // - CONCAT('name:', b.b_name) -> NULL
  std::vector<Row> left_res = RunSql(
      ctx, *db_,
      "SELECT a.id, b.score, COALESCE(b.score, -1), IFNULL(b.score, -2), "
      "b.score + 10, b.score IS NULL, b.score IS NOT NULL, "
      "CASE WHEN b.score IS NULL THEN 'absent' ELSE 'present' END, "
      "CONCAT('name:', b.b_name) "
      "FROM oj_a a LEFT JOIN oj_b b ON a.id = b.id ORDER BY a.id;");
  ASSERT_EQ(left_res.size(), 3U);
  // Row 1 (Alice)
  EXPECT_EQ(left_res[0][0], Value(1));
  EXPECT_EQ(left_res[0][1], Value(100));
  EXPECT_EQ(left_res[0][2], Value(100));
  EXPECT_EQ(left_res[0][3], Value(100));
  EXPECT_EQ(left_res[0][4], Value(110));
  EXPECT_EQ(left_res[0][5], Value(false));
  EXPECT_EQ(left_res[0][6], Value(true));
  EXPECT_EQ(left_res[0][7], Value("present"));
  EXPECT_EQ(left_res[0][8], Value("name:B-Alice"));
  // Row 3 (Charlie - NULL padded)
  EXPECT_EQ(left_res[2][0], Value(3));
  EXPECT_TRUE(left_res[2][1].IsNull());
  EXPECT_EQ(left_res[2][2], Value(-1));
  EXPECT_EQ(left_res[2][3], Value(-2));
  EXPECT_TRUE(left_res[2][4].IsNull());
  EXPECT_EQ(left_res[2][5], Value(true));
  EXPECT_EQ(left_res[2][6], Value(false));
  EXPECT_EQ(left_res[2][7], Value("absent"));
  EXPECT_TRUE(left_res[2][8].IsNull());

  // 2. LEFT JOIN with residual ON predicate:
  // ON a.id = b.id AND a.val < b.score
  // For Bob (id=2), a.val=20, b.score=5 -> 20 < 5 is FALSE!
  // So Bob must NOT match and should be NULL-padded!
  std::vector<Row> res_pred =
      RunSql(ctx, *db_,
             "SELECT a.id, a.name, b.score FROM oj_a a LEFT JOIN oj_b b "
             "ON a.id = b.id AND a.val < b.score ORDER BY a.id;");
  ASSERT_EQ(res_pred.size(), 3U);
  EXPECT_EQ(res_pred[0][0], Value(1));
  EXPECT_EQ(res_pred[0][2], Value(100));
  EXPECT_EQ(res_pred[1][0], Value(2));
  EXPECT_TRUE(res_pred[1][2].IsNull());
  EXPECT_EQ(res_pred[2][0], Value(3));
  EXPECT_TRUE(res_pred[2][2].IsNull());

  // 3. RIGHT JOIN with null-padding:
  // oj_b has David (id=4) which is not in oj_a.
  std::vector<Row> right_res = RunSql(
      ctx, *db_,
      "SELECT a.id, a.name, b.id, b.b_name FROM oj_a a RIGHT JOIN oj_b b "
      "ON a.id = b.id ORDER BY b.id;");
  ASSERT_EQ(right_res.size(), 3U);
  EXPECT_EQ(right_res[2][2], Value(4));
  EXPECT_TRUE(right_res[2][0].IsNull());
  EXPECT_TRUE(right_res[2][1].IsNull());

  // 4. FULL OUTER JOIN:
  // Both Charlie (id=3 in a) and David (id=4 in b) must be present.
  std::vector<Row> full_res =
      RunSql(ctx, *db_,
             "SELECT a.id, b.id FROM oj_a a FULL JOIN oj_b b ON a.id = b.id "
             "ORDER BY COALESCE(a.id, b.id);");
  ASSERT_EQ(full_res.size(), 4U);
  EXPECT_EQ(full_res[0][0], Value(1));
  EXPECT_EQ(full_res[0][1], Value(1));
  EXPECT_EQ(full_res[1][0], Value(2));
  EXPECT_EQ(full_res[1][1], Value(2));
  EXPECT_EQ(full_res[2][0], Value(3));
  EXPECT_TRUE(full_res[2][1].IsNull());
  EXPECT_TRUE(full_res[3][0].IsNull());
  EXPECT_EQ(full_res[3][1], Value(4));

  // 5. Aggregations on NULL-padded rows:
  // Charlie has NULL b.score.
  // COUNT(*) = 1, COUNT(b.score) = 0, SUM(b.score) = NULL, AVG = NULL.
  std::vector<Row> agg_res = RunSql(
      ctx, *db_,
      "SELECT a.id, COUNT(*), COUNT(b.score), SUM(b.score), AVG(b.score), "
      "MIN(b.score), MAX(b.score) FROM oj_a a LEFT JOIN oj_b b ON a.id = b.id "
      "GROUP BY a.id ORDER BY a.id;");
  ASSERT_EQ(agg_res.size(), 3U);
  EXPECT_EQ(agg_res[2][0], Value(3));
  EXPECT_EQ(agg_res[2][1], Value(1));
  EXPECT_EQ(agg_res[2][2], Value(0));
  EXPECT_TRUE(agg_res[2][3].IsNull());
  EXPECT_TRUE(agg_res[2][4].IsNull());
  EXPECT_TRUE(agg_res[2][5].IsNull());
  EXPECT_TRUE(agg_res[2][6].IsNull());

  // 6. Multi-table chained LEFT JOIN:
  // a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id
  // Charlie (id=3): b is null, so b.id = c.id fails -> c is also null!
  std::vector<Row> chain_res =
      RunSql(ctx, *db_,
             "SELECT a.id, b.b_name, c.extra FROM oj_a a "
             "LEFT JOIN oj_b b ON a.id = b.id "
             "LEFT JOIN oj_c c ON b.id = c.id "
             "ORDER BY a.id;");
  ASSERT_EQ(chain_res.size(), 3U);
  EXPECT_EQ(chain_res[0][0], Value(1));
  EXPECT_EQ(chain_res[0][1], Value("B-Alice"));
  EXPECT_EQ(chain_res[0][2], Value("C-One"));
  EXPECT_EQ(chain_res[1][0], Value(2));
  EXPECT_EQ(chain_res[1][1], Value("B-Bob"));
  EXPECT_TRUE(chain_res[1][2].IsNull());
  EXPECT_EQ(chain_res[2][0], Value(3));
  EXPECT_TRUE(chain_res[2][1].IsNull());
  EXPECT_TRUE(chain_res[2][2].IsNull());

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, CancellationResourceLeakVerification) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE cancel_t (id INT64, val INT64, str STRING);");

  for (int i = 0; i < 300; ++i) {
    std::string sql = "INSERT INTO cancel_t VALUES (" + std::to_string(i) +
                      ", " + std::to_string(300 - i) + ", '" +
                      std::string(80, static_cast<char>('A' + (i % 26))) +
                      "');";
    RunSql(ctx, *db_, sql);
  }

  StatusOr<Table> table = db_->GetTable(ctx, "cancel_t");
  ASSERT_TRUE(table.HasValue())
      << "CREATE/INSERT setup failed: " << table.GetStatus();
  Table tbl = table.Value();

  // Part 1: ParallelScan early destruction and worker join.
  // 1a. ParallelScan destroyed after 0 rows read.
  {
    auto scan = std::make_unique<ParallelScan>(ctx.txn_, tbl, 4);
    scan.reset();
    EXPECT_EQ(db_->PinnedPageCount(), 0U);
  }

  // 1b. ParallelScan destroyed after reading 1 row.
  {
    auto scan = std::make_unique<ParallelScan>(ctx.txn_, tbl, 4);
    Row r;
    RowPosition p;
    EXPECT_TRUE(scan->Next(&r, &p));
    scan.reset();
    EXPECT_EQ(db_->PinnedPageCount(), 0U);
  }

  // 1c. ParallelScan destroyed after reading 1 batch.
  {
    auto scan = std::make_unique<ParallelScan>(ctx.txn_, tbl, 4);
    DataChunk chunk;
    EXPECT_GT(scan->NextBatch(&chunk, 16), 0U);
    scan.reset();
    EXPECT_EQ(db_->PinnedPageCount(), 0U);
  }

  // Part 2: QueryMemoryBudget and SpillFile cleanup on Sort mid-execution
  // destruction. The leak check counts spill files in SpillFile's temp
  // directory, which defaults to a system-wide folder shared by every
  // concurrently running test binary; point TINYLAMB_TEMP at a fresh
  // per-process directory so external spills cannot land between the
  // baseline and post-cancellation counts.
  const std::filesystem::path spill_isolation_dir =
      std::filesystem::temp_directory_path() /
      ("tinylamb-spill-test-" + RandomString());
  std::filesystem::create_directories(spill_isolation_dir);
  ::tinylamb::platform::SetEnv("TINYLAMB_TEMP", spill_isolation_dir.c_str(), 1);
  // Restore the process-global budget cap and TINYLAMB_TEMP on every exit
  // path: the ASSERT_* checks below return early on failure, and without
  // this guard a failed assertion would leak both into later tests.
  struct SpillIsolationGuard {
    explicit SpillIsolationGuard(std::filesystem::path dir)
        : dir_(std::move(dir)) {}
    SpillIsolationGuard(const SpillIsolationGuard&) = delete;
    SpillIsolationGuard& operator=(const SpillIsolationGuard&) = delete;
    SpillIsolationGuard(SpillIsolationGuard&&) = delete;
    SpillIsolationGuard& operator=(SpillIsolationGuard&&) = delete;
    ~SpillIsolationGuard() {
      QueryMemoryBudget::Global().ResetForTest(0);
      ::tinylamb::platform::UnsetEnv("TINYLAMB_TEMP");
      std::error_code ec;
      std::filesystem::remove_all(dir_, ec);
    }
    std::filesystem::path dir_;
  } spill_guard{spill_isolation_dir};
  auto count_spill_files = []() -> size_t {
    size_t count = 0;
    std::error_code ec;
    for (const auto& entry :
         std::filesystem::directory_iterator(SpillFile::TempDirectory(), ec)) {
      if (entry.path().filename().string().starts_with("tinylamb-spill-") &&
          entry.path().extension() == ".bin") {
        ++count;
      }
    }
    return count;
  };

  const size_t baseline_memory = QueryMemoryBudget::Global().Used();
  const size_t baseline_spills = count_spill_files();

  {
    // Restrict memory to force spilling.
    QueryMemoryBudget::Global().ResetForTest(2048);
    SqlEngine engine(*db_);
    StatusOr<Executor> exec =
        engine.Prepare(ctx, "SELECT * FROM cancel_t ORDER BY val;");
    ASSERT_EQ(exec.GetStatus(), Status::kSuccess);

    // Read 1 row: triggers Materialize() -> external sort / spilling.
    Row r;
    EXPECT_TRUE(exec.Value()->Next(&r, nullptr));

    // Destroy executor midway without exhausting result rows.
    exec = StatusError(StatusCode::kUnknown, "cancelled");

    EXPECT_EQ(QueryMemoryBudget::Global().Used(), baseline_memory);
    EXPECT_EQ(count_spill_files(), baseline_spills);
    EXPECT_EQ(db_->PinnedPageCount(), 0U);
  }

  // Part 3: QueryMemoryBudget and SpillFile cleanup on HashJoin mid-execution
  // destruction.
  RunSql(ctx, *db_,
         "CREATE TABLE cancel_t2 (id INT64, val INT64, info STRING);");
  for (int i = 0; i < 200; ++i) {
    std::string sql = "INSERT INTO cancel_t2 VALUES (" + std::to_string(i) +
                      ", " + std::to_string(i) + ", '" + std::string(80, 'x') +
                      "');";
    RunSql(ctx, *db_, sql);
  }

  {
    QueryMemoryBudget::Global().ResetForTest(2048);
    SqlEngine engine(*db_);
    StatusOr<Executor> exec = engine.Prepare(
        ctx,
        "SELECT a.id, b.id FROM cancel_t a JOIN cancel_t2 b ON a.id = b.id;");
    ASSERT_EQ(exec.GetStatus(), Status::kSuccess);

    // Read 1 row: triggers join build and probe.
    Row r;
    EXPECT_TRUE(exec.Value()->Next(&r, nullptr));

    // Destroy executor midway.
    exec = StatusError(StatusCode::kUnknown, "cancelled");

    EXPECT_EQ(QueryMemoryBudget::Global().Used(), baseline_memory);
    EXPECT_EQ(count_spill_files(), baseline_spills);
    EXPECT_EQ(db_->PinnedPageCount(), 0U);
  }

  // Part 4: Reset budget to unlimited.
  QueryMemoryBudget::Global().ResetForTest(0);
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), baseline_memory);
  EXPECT_EQ(count_spill_files(), baseline_spills);
  EXPECT_EQ(db_->PinnedPageCount(), 0U);

  ::tinylamb::platform::UnsetEnv("TINYLAMB_TEMP");
  std::error_code cleanup_ec;
  std::filesystem::remove_all(spill_isolation_dir, cleanup_ec);

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

TEST_F(QueryTest, DifferentialTestCascadesVsRelationalFallback) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE diff_t1 (id INT64, val INT64, name STRING, score "
         "NUMERIC);");
  RunSql(ctx, *db_,
         "CREATE TABLE diff_t2 (id INT64, t1_id INT64, note STRING, bonus "
         "NUMERIC);");

  for (int i = 1; i <= 60; ++i) {
    std::string sql1 = "INSERT INTO diff_t1 VALUES (" + std::to_string(i) +
                       ", " + std::to_string(i % 10) + ", 'user_" +
                       std::to_string(i % 7) + "', " + std::to_string(i * 1.5) +
                       ");";
    RunSql(ctx, *db_, sql1);
  }
  // Add a row with NULLs in diff_t1:
  RunSql(ctx, *db_, "INSERT INTO diff_t1 VALUES (61, NULL, NULL, 0.0);");

  for (int i = 1; i <= 50; ++i) {
    std::string sql2 = "INSERT INTO diff_t2 VALUES (" + std::to_string(i) +
                       ", " + std::to_string(((i * 2) % 60) + 1) + ", 'note_" +
                       std::to_string(i % 5) + "', " + std::to_string(i * 2.0) +
                       ");";
    RunSql(ctx, *db_, sql2);
  }

  auto run_differential = [&](std::string_view sql, bool ordered = true) {
    SCOPED_TRACE(sql);
    SqlEngine engine_cascades(*db_);
    engine_cascades.SetForceRelationalFallback(false);

    SqlEngine engine_relational(*db_);
    engine_relational.SetForceRelationalFallback(true);

    StatusOr<Executor> exec_cascades = engine_cascades.Prepare(ctx, sql);
    StatusOr<Executor> exec_relational = engine_relational.Prepare(ctx, sql);

    ASSERT_EQ(exec_cascades.GetStatus(), Status::kSuccess)
        << "Cascades error: " << engine_cascades.LastError()
        << " for SQL: " << sql;
    ASSERT_EQ(exec_relational.GetStatus(), Status::kSuccess)
        << "Relational error: " << engine_relational.LastError()
        << " for SQL: " << sql;

    std::vector<Row> rows_cascades;
    Row r;
    while (exec_cascades.Value()->Next(&r, nullptr)) {
      rows_cascades.push_back(r);
    }

    std::vector<Row> rows_relational;
    while (exec_relational.Value()->Next(&r, nullptr)) {
      rows_relational.push_back(r);
    }

    ASSERT_EQ(rows_cascades.size(), rows_relational.size())
        << "Row count mismatch for SQL: " << sql;

    if (ordered) {
      for (size_t i = 0; i < rows_cascades.size(); ++i) {
        EXPECT_EQ(rows_cascades[i], rows_relational[i])
            << "Row " << i << " mismatch for SQL: " << sql;
      }
    } else {
      auto row_less = [](const Row& a, const Row& b) {
        return a.ToString() < b.ToString();
      };
      std::sort(rows_cascades.begin(), rows_cascades.end(), row_less);
      std::sort(rows_relational.begin(), rows_relational.end(), row_less);
      for (size_t i = 0; i < rows_cascades.size(); ++i) {
        EXPECT_EQ(rows_cascades[i], rows_relational[i])
            << "Row " << i << " mismatch (sorted) for SQL: " << sql;
      }
    }
  };

  // 1. Basic scans, filters, and projections:
  run_differential("SELECT id, val FROM diff_t1 WHERE val > 3 ORDER BY id;");
  run_differential(
      "SELECT id, val * 2 + 10, CONCAT('Hello ', name) FROM diff_t1 WHERE val "
      "> 5 ORDER BY id;");
  run_differential(
      "SELECT id, name FROM diff_t1 WHERE val BETWEEN 2 AND 7 AND name LIKE "
      "'%user%' ORDER BY id;");

  // 2. Arithmetic, NULL functions, and conditionals:
  run_differential(
      "SELECT id, CASE WHEN val < 3 THEN 'low' WHEN val < 7 THEN 'mid' ELSE "
      "'high' END FROM diff_t1 ORDER BY id;");
  run_differential(
      "SELECT id, COALESCE(val, -1), IFNULL(name, 'missing') FROM diff_t1 "
      "ORDER BY id;");

  // 3. Filtering with IN and OR:
  run_differential(
      "SELECT id, val FROM diff_t1 WHERE val IN (1, 3, 5, 7) ORDER BY id;");
  run_differential(
      "SELECT id, val FROM diff_t1 WHERE (val = 2 OR val = 8) AND id > 20 "
      "ORDER BY id;");

  // 4. Distinct, Limit, and Offset:
  run_differential("SELECT DISTINCT val FROM diff_t1 ORDER BY val;");
  run_differential(
      "SELECT id, val FROM diff_t1 ORDER BY val DESC, id ASC LIMIT 10 OFFSET "
      "5;");
  run_differential(
      "SELECT DISTINCT val, name FROM diff_t1 ORDER BY val, name;");

  // 5. Aggregations and GROUP BY:
  run_differential(
      "SELECT val, COUNT(*), SUM(id), MIN(id), MAX(id) FROM diff_t1 GROUP BY "
      "val ORDER BY val;");
  run_differential(
      "SELECT val, COUNT(*) FROM diff_t1 GROUP BY val HAVING COUNT(*) > 5 "
      "ORDER BY val;");
  run_differential(
      "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM diff_t1;");

  // 6. Joins:
  run_differential(
      "SELECT a.id, b.id, a.val, b.note FROM diff_t1 a JOIN diff_t2 b ON a.id "
      "= "
      "b.t1_id ORDER BY a.id, b.id;");
  run_differential(
      "SELECT a.id, b.id, a.name, b.note FROM diff_t1 a JOIN diff_t2 b ON a.id "
      "= "
      "b.t1_id WHERE a.val > 5 AND b.bonus > 10 ORDER BY a.id, b.id;");
  run_differential(
      "SELECT a.id, b.bonus + a.val FROM diff_t1 a JOIN diff_t2 b ON a.id = "
      "b.t1_id ORDER BY a.id, b.id;");

  // 7. Subqueries:
  run_differential(
      "SELECT id, val FROM diff_t1 WHERE id IN (SELECT t1_id FROM diff_t2 "
      "WHERE bonus > 50) ORDER BY id;");
  run_differential(
      "SELECT a.id, a.val FROM diff_t1 a WHERE EXISTS (SELECT * FROM diff_t2 b "
      "WHERE b.t1_id = a.id AND b.bonus > 20) ORDER BY a.id;");

  // 8. Queries without explicit ORDER BY (compared as multisets):
  run_differential("SELECT val, COUNT(*) FROM diff_t1 GROUP BY val;", false);
  run_differential("SELECT id, val FROM diff_t1 WHERE val = 5;", false);

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// The optimizer canonicalizes conjunct order (lexicographic sort in
// plan/cascades.cpp CanonicalizeConjuncts), so a WHERE conjunction is not
// evaluated strictly left-to-right. Under commutative WHERE semantics a
// conjunct that cleanly rejects a row suppresses errors raised by sibling
// conjuncts: `a <> 0` discards the a=0 rows and `b / a` must never raise
// division-by-zero on them. Every executor that applies a canonicalized
// conjunctive predicate (Selection, index scan filters, inner-join
// residuals) must follow that rule; the Cascades Selection used to evaluate
// the reordered AND tree strictly and surfaced a division-by-zero the
// relational scan filter suppressed.
TEST_F(QueryTest, WhereConjunctReorderSuppressesSiblingError) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE div_t (u INT64, a INT64, b INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO div_t VALUES (0, 0, 5), (1, 2, -3), (2, NULL, 0), "
         "(3, 1, NULL);");

  const auto run_checked =
      [&](std::string_view sql,
          bool fallback) -> std::pair<std::vector<Row>, Status> {
    SCOPED_TRACE(sql);
    SqlEngine engine(*db_);
    engine.SetForceRelationalFallback(fallback);
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << engine.LastError();
    std::vector<Row> rows;
    if (!prepared.HasValue()) {
      return std::pair{rows, Status::kSuccess};
    }
    Executor executor = prepared.MoveValue();
    Row row;
    while (executor->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    return std::pair{rows, executor->GetStatus()};
  };

  for (const bool fallback : {false, true}) {
    // Guard-first and guard-second orderings agree under commutative WHERE
    // semantics: the a=0 row is rejected by `a <> 0` before division runs.
    for (std::string_view sql :
         {"SELECT u FROM div_t WHERE a <> 0 AND b/a > -10 ORDER BY u;",
          "SELECT u FROM div_t WHERE b/a > -10 AND a <> 0 ORDER BY u;",
          "SELECT COUNT(*) FROM div_t WHERE a <> 0 AND b/a > -10;"}) {
      auto [rows, status] = run_checked(sql, fallback);
      ASSERT_EQ(status, Status::kSuccess) << sql;
      ASSERT_EQ(rows.size(), 1U) << sql;
      EXPECT_EQ(rows[0][0], Value(int64_t{1})) << sql;
    }
    // Without a rejecting sibling the division error must still surface.
    auto [rows, status] =
        run_checked("SELECT u FROM div_t WHERE b/a > -10;", fallback);
    EXPECT_NE(status, Status::kSuccess) << "expected division-by-zero";
  }

  // Inner-join predicates go through the same canonicalization; a spanning
  // conjunct that rejects the pair suppresses errors from its siblings.
  // Pairs formed on u=k: (0,0): 5/1>-10 passes; (1,1): j.d=0 rejects before
  // -3/0 raises; (2,2): 0/4>-10 passes; u=3 joins nothing.
  RunSql(ctx, *db_, "CREATE TABLE div_j (k INT64, d INT64);");
  RunSql(ctx, *db_, "INSERT INTO div_j VALUES (0, 1), (1, 0), (2, 4);");
  for (const bool fallback : {false, true}) {
    auto [rows, status] = run_checked(
        "SELECT a.u, j.k FROM div_t a JOIN div_j j ON a.u = j.k "
        "WHERE j.d <> 0 AND a.b / j.d > -10 ORDER BY a.u;",
        fallback);
    ASSERT_EQ(status, Status::kSuccess);
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0][0], Value(int64_t{0}));
    EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  }

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

// UPDATE/DELETE build a QueryData whose WHERE may hold an IN/EXISTS
// subquery.  TryDecorrelate wraps the core plan in a semi/anti ProductPlan
// keyed on qualified names (t.u) — but the UPDATE select list projects the
// SET expressions under unqualified output names, so the key was not
// resolvable (FATAL in BuildKeyOffsets) and, once resolvable, the trim
// projection must not re-evaluate the SET expression (a+1 applied twice).
TEST_F(QueryTest, UpdateWithInSubqueryDecorrelatesOnce) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE upd_t (u INT64, a INT64);");
  RunSql(ctx, *db_, "CREATE TABLE upd_s (k INT64);");
  RunSql(ctx, *db_, "INSERT INTO upd_t VALUES (1, 10), (2, 20), (3, 30);");
  RunSql(ctx, *db_, "INSERT INTO upd_s VALUES (1), (3);");

  SqlEngine engine(*db_);
  StatusOr<Executor> updated =
      engine.Prepare(ctx,
                     "UPDATE upd_t SET a = a + 1 WHERE u IN "
                     "(SELECT k FROM upd_s);");
  ASSERT_TRUE(updated.HasValue()) << engine.LastError();
  Executor executor = updated.MoveValue();
  Row row;
  while (executor->Next(&row, nullptr)) {
  }
  ASSERT_EQ(executor->GetStatus(), Status::kSuccess);

  auto dump = [&]() -> std::vector<Row> {
    SqlEngine reader(*db_);
    StatusOr<Executor> sel =
        reader.Prepare(ctx, "SELECT u, a FROM upd_t ORDER BY u;");
    EXPECT_TRUE(sel.HasValue()) << reader.LastError();
    if (!sel.HasValue()) {
      return {};
    }
    std::vector<Row> rows;
    Executor ex = sel.MoveValue();
    Row r;
    while (ex->Next(&r, nullptr)) {
      rows.push_back(r);
    }
    EXPECT_EQ(ex->GetStatus(), Status::kSuccess);
    return rows;
  };
  std::vector<Row> rows = dump();
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][1], Value(int64_t{11})) << "a+1 must apply exactly once";
  EXPECT_EQ(rows[1][1], Value(int64_t{20}));
  EXPECT_EQ(rows[2][1], Value(int64_t{31}));

  // Correlated EXISTS and NOT IN take the same decorrelation wrap.
  updated = engine.Prepare(ctx,
                           "UPDATE upd_t SET a = a * 2 WHERE NOT EXISTS "
                           "(SELECT 1 FROM upd_s WHERE upd_s.k = upd_t.u);");
  ASSERT_TRUE(updated.HasValue()) << engine.LastError();
  executor = updated.MoveValue();
  while (executor->Next(&row, nullptr)) {
  }
  ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
  rows = dump();
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][1], Value(int64_t{11}));
  EXPECT_EQ(rows[1][1], Value(int64_t{40}));
  EXPECT_EQ(rows[2][1], Value(int64_t{31}));

  // Null-aware anti join: NOT IN over a list containing NULL keeps nothing.
  RunSql(ctx, *db_, "INSERT INTO upd_s VALUES (NULL);");
  updated = engine.Prepare(ctx,
                           "UPDATE upd_t SET a = 0 WHERE u NOT IN "
                           "(SELECT k FROM upd_s);");
  ASSERT_TRUE(updated.HasValue()) << engine.LastError();
  executor = updated.MoveValue();
  while (executor->Next(&row, nullptr)) {
  }
  ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
  rows = dump();
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][1], Value(int64_t{11})) << "NOT IN with NULL: no updates";

  // DELETE through the same wrap still positions rows correctly.
  updated = engine.Prepare(ctx,
                           "DELETE FROM upd_t WHERE u IN "
                           "(SELECT k FROM upd_s);");
  ASSERT_TRUE(updated.HasValue()) << engine.LastError();
  executor = updated.MoveValue();
  while (executor->Next(&row, nullptr)) {
  }
  ASSERT_EQ(executor->GetStatus(), Status::kSuccess);
  rows = dump();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
  EXPECT_EQ(rows[0][1], Value(int64_t{40}));

  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}
TEST_F(QueryTest, DmlDuplicateExistsConjunctDecorrelateOnce) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE t0 (u INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE t1 (u INT64);");
  RunSql(ctx, *db_, "INSERT INTO t0 VALUES (9, 1);");
  RunSql(ctx, *db_, "INSERT INTO t1 VALUES (9);");
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
  ctx = db_->BeginContext();

  // The same correlated EXISTS conjunct twice: semi-joins are
  // idempotent, so `E AND E` must match exactly like `E` alone.
  // Pre-fix the duplicate wrapped a second semi-join over the key the
  // first one reshaped, silently dropping every row (0 updates).
  const std::vector<Row> upd = RunSql(
      ctx, *db_,
      "UPDATE t0 SET v = 2 WHERE (EXISTS (SELECT 1 FROM t1 WHERE "
      "t1.u = t0.u)) AND (EXISTS (SELECT 1 FROM t1 WHERE t1.u = t0.u));");
  ASSERT_EQ(upd.size(), 1U);
  EXPECT_EQ(upd[0][1].value.int_value, 1);

  const std::vector<Row> rows = RunSql(ctx, *db_, "SELECT u, v FROM t0;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][1].value.int_value, 2);
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}
TEST_F(QueryTest, GroupedOrderBySpelledAggregate) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ord_t (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO ord_t VALUES (1, 30), (1, 10), (2, 5), (2, 20), "
         "(3, 7);");

  SqlEngine engine(*db_);
  const auto run = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
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

  // The single-table stream-aggregation path must prepare aggregates that
  // appear only in the ORDER BY: the AggregateResultMap is keyed by node
  // pointer, and the spelled-out sort key ("ORDER BY SUM(v)") is a different
  // instance from the select-list aggregate.  Pre-fix these failed with
  // "aggregate was not prepared".
  const std::vector<Row> spelled =
      run("SELECT k, SUM(v) FROM ord_t GROUP BY k ORDER BY SUM(v) DESC;");
  ASSERT_EQ(spelled.size(), 3U);
  EXPECT_EQ(spelled[0][0].value.int_value, 1);
  EXPECT_EQ(spelled[0][1].value.int_value, 40);
  EXPECT_EQ(spelled[1][0].value.int_value, 2);
  EXPECT_EQ(spelled[1][1].value.int_value, 25);
  EXPECT_EQ(spelled[2][0].value.int_value, 3);
  EXPECT_EQ(spelled[2][1].value.int_value, 7);

  const std::vector<Row> count_star =
      run("SELECT k, COUNT(*) FROM ord_t GROUP BY k ORDER BY COUNT(*) ASC, k;");
  ASSERT_EQ(count_star.size(), 3U);
  EXPECT_EQ(count_star[0][0].value.int_value, 3);
  EXPECT_EQ(count_star[0][1].value.int_value, 1);
  EXPECT_EQ(count_star[1][0].value.int_value, 1);
  EXPECT_EQ(count_star[1][1].value.int_value, 2);
  EXPECT_EQ(count_star[2][0].value.int_value, 2);
  EXPECT_EQ(count_star[2][1].value.int_value, 2);

  // An ORDER BY aggregate that is not projected gets prepared on its own.
  const std::vector<Row> unprojected =
      run("SELECT k FROM ord_t GROUP BY k ORDER BY SUM(v) DESC;");
  ASSERT_EQ(unprojected.size(), 3U);
  EXPECT_EQ(unprojected[0][0].value.int_value, 1);
  EXPECT_EQ(unprojected[1][0].value.int_value, 2);
  EXPECT_EQ(unprojected[2][0].value.int_value, 3);

  // Alias and bounded (Top-N) forms stay correct alongside the fix.
  const std::vector<Row> aliased =
      run("SELECT k, SUM(v) AS s FROM ord_t GROUP BY k ORDER BY s DESC "
          "LIMIT 2;");
  ASSERT_EQ(aliased.size(), 2U);
  EXPECT_EQ(aliased[0][0].value.int_value, 1);
  EXPECT_EQ(aliased[1][0].value.int_value, 2);

  // DISTINCT over a grouped projection: the grouped path already consumed
  // ORDER BY with the non-projected aggregate as a hidden sorter key, and
  // the first-occurrence DISTINCT fold preserves that order. Re-applying
  // the raw ORDER BY expression against the visible-only relation used to
  // re-evaluate the hidden aggregate and fail.
  const std::vector<Row> distinct_unprojected =
      run("SELECT DISTINCT k FROM ord_t GROUP BY k ORDER BY SUM(v) DESC;");
  ASSERT_EQ(distinct_unprojected.size(), 3U);
  EXPECT_EQ(distinct_unprojected[0][0].value.int_value, 1);
  EXPECT_EQ(distinct_unprojected[1][0].value.int_value, 2);
  EXPECT_EQ(distinct_unprojected[2][0].value.int_value, 3);

  // A non-projected GROUP BY key as the order term takes the same path.
  const std::vector<Row> distinct_group_key =
      run("SELECT DISTINCT k FROM ord_t GROUP BY k, v ORDER BY v DESC;");
  ASSERT_EQ(distinct_group_key.size(), 3U);
  EXPECT_EQ(distinct_group_key[0][0].value.int_value, 1);
  EXPECT_EQ(distinct_group_key[1][0].value.int_value, 2);
  EXPECT_EQ(distinct_group_key[2][0].value.int_value, 3);
}

// A typo'd ORDER BY column over a grouped statement must be an error.  The
// grouped-emission scopes used to let the whole-row fallback resolve ANY bare
// name to a STRUCT of the output row, so `ORDER BY nosuch` silently "sorted"
// by that struct (or by its JSON text) instead of failing.  Both the bare and
// the arithmetic spelling must report the missing column.
TEST_F(QueryTest, GroupedOrderByUnknownColumnErrors) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE ord_u (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO ord_u VALUES (1, 30), (1, 10), (2, 5), (2, 20), (3, 7);");
  SqlEngine engine(*db_);
  for (std::string_view sql :
       {"SELECT k, COUNT(*) FROM ord_u GROUP BY k ORDER BY nosuch",
        "SELECT k, COUNT(*) FROM ord_u GROUP BY k ORDER BY nosuch + 1",
        "SELECT k, COUNT(*) AS c FROM ord_u GROUP BY k ORDER BY nosuch "
        "LIMIT 2"}) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    ASSERT_TRUE(prepared.HasValue()) << sql << "\n" << engine.LastError();
    // The relational finish defers to the first drain; the failure must
    // surface there and name the missing column (pre-fix this silently
    // "sorted" by a fabricated STRUCT of the output row).
    Row row;
    std::vector<Row> leaked;
    while (prepared.Value()->Next(&row, nullptr)) {
      leaked.push_back(row);
    }
    const Status status = prepared.Value()->GetStatus();
    ASSERT_NE(status, Status::kSuccess)
        << sql << " silently returned " << leaked.size() << " rows";
    EXPECT_NE(status.GetMessage().find("nosuch"), std::string::npos)
        << sql << "\nerror was: " << status.GetMessage();
  }
  // Referencing the whole source row in a grouped ORDER BY is not a group
  // key either: the query is grouped by parts of that row.
  StatusOr<Executor> whole_row = engine.Prepare(
      ctx, "SELECT k, COUNT(*) FROM ord_u GROUP BY k ORDER BY v");
  // `v` IS a real base column but not a group key; GoogleSQL allows ordering
  // by input columns of the group only when they are functionally derived
  // from the key, which the evaluator decides -- it must not silently
  // fabricate a STRUCT either way.
  if (whole_row.HasValue()) {
    Row row;
    while (whole_row.Value()->Next(&row, nullptr)) {
      for (const Value& value : row.values_) {
        EXPECT_NE(value.type, ValueType::kVarChar)
            << "order key fabricated a STRUCT value: " << value;
      }
    }
  }
}

// `col = col +/- constant` join predicates become biased hash keys (the
// constant folds into the key on the expression side); every matched pair is
// still re-verified against the original predicate, so results must equal
// the unbiased spelling exactly.  Week-over-week self joins exercise the
// string+int composite key path the TPC-DS shapes hit.
TEST_F(QueryTest, BiasedJoinKeysMatchPlainEquality) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE biased_l (k STRING, week INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE biased_r (k STRING, week INT64, w INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO biased_l VALUES ('a', 10, 1), ('a', 20, 2), "
         "('b', 10, 3), ('c', 30, 4);");
  RunSql(ctx, *db_,
         "INSERT INTO biased_r VALUES ('a', 20, 100), ('a', 30, 200), "
         "('b', 20, 300), ('d', 40, 400);");
  SqlEngine engine(*db_);
  const auto run = [&](std::string_view sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
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
  // Inner join, constant on the right side of the expression.
  const std::vector<Row> shifted =
      run("SELECT l.v, r.w FROM biased_l l JOIN biased_r r "
          "ON l.k = r.k AND l.week = r.week - 10 ORDER BY l.v;");
  ASSERT_EQ(shifted.size(), 3U);
  EXPECT_EQ(shifted[0][0].value.int_value, 1);
  EXPECT_EQ(shifted[0][1].value.int_value, 100);
  EXPECT_EQ(shifted[1][0].value.int_value, 2);
  EXPECT_EQ(shifted[1][1].value.int_value, 200);
  EXPECT_EQ(shifted[2][0].value.int_value, 3);
  EXPECT_EQ(shifted[2][1].value.int_value, 300);
  // The unbiased spelling of the same predicate agrees row for row.
  const std::vector<Row> plain =
      run("SELECT l.v, r.w FROM biased_l l JOIN biased_r r "
          "ON l.k = r.k AND l.week + 10 = r.week ORDER BY l.v;");
  ASSERT_EQ(plain.size(), shifted.size());
  for (size_t i = 0; i < plain.size(); ++i) {
    EXPECT_EQ(plain[i][0], shifted[i][0]);
    EXPECT_EQ(plain[i][1], shifted[i][1]);
  }
  // Constant folded on the left side too.
  const std::vector<Row> mirrored =
      run("SELECT l.v, r.w FROM biased_l l JOIN biased_r r "
          "ON l.k = r.k AND l.week - 10 = r.week - 20 ORDER BY l.v;");
  ASSERT_EQ(mirrored.size(), 3U);
  EXPECT_EQ(mirrored[0][1].value.int_value, 100);
  // LEFT JOIN keeps unmatched left rows with NULL padding.
  const std::vector<Row> left_join =
      run("SELECT l.v, r.w FROM biased_l l LEFT JOIN biased_r r "
          "ON l.k = r.k AND l.week = r.week - 10 ORDER BY l.v;");
  ASSERT_EQ(left_join.size(), 4U);
  EXPECT_EQ(left_join[0][0].value.int_value, 1);
  EXPECT_EQ(left_join[0][1].value.int_value, 100);
  EXPECT_EQ(left_join[3][0].value.int_value, 4);
  EXPECT_TRUE(left_join[3][1].IsNull());
  // DATE keys bias by whole days: (a=(03-01, 03-29)) joins b=(03-29, 03-29)
  // on d2/d1, and a.d1 = b.d2 - 28 holds (03-29 minus 28 days = 03-01).
  RunSql(ctx, *db_, "CREATE TABLE biased_d (d1 DATE, d2 DATE);");
  RunSql(ctx, *db_,
         "INSERT INTO biased_d VALUES (DATE '2024-03-01', DATE "
         "'2024-03-29'), (DATE '2024-03-29', DATE '2024-03-29');");
  const std::vector<Row> dates =
      run("SELECT 'x' FROM biased_d a JOIN biased_d b ON a.d2 = b.d1 "
          "WHERE a.d1 = b.d2 - 28;");
  EXPECT_EQ(dates.size(), 1U);
  // Large-magnitude join keys: the bias folding into the hash key must stay
  // exact near the INT64 bounds, and the residual must still decide every
  // k-matched pair. Values stay inside the range where `column +/- 10`
  // cannot overflow: an overflow inside the optimizer-derived
  // `IS NOT NULL (week +/- 10)` scan conjunct currently drops rows silently
  // instead of surfacing the evaluation error, and the probe-biased spelling
  // (`l.week + 10 = r.week`) loses matches near the bounds -- both are
  // pre-existing issues in the wider join/predicate machinery, tracked
  // separately from the biased-key hashing this case guards.
  RunSql(ctx, *db_, "CREATE TABLE biased_xl (k STRING, week INT64, v INT64);");
  RunSql(ctx, *db_, "CREATE TABLE biased_xr (k STRING, week INT64, w INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO biased_xl VALUES "
         "('p', 9223372036854775798, 1), "   // INT64_MAX - 9
         "('q', -9223372036854775798, 2), "  // INT64_MIN + 10
         "('z', 0, 4);");
  RunSql(ctx, *db_,
         "INSERT INTO biased_xr VALUES "
         "('p', 9223372036854775807, 100), "   // INT64_MAX
         "('q', -9223372036854775788, 200), "  // INT64_MIN + 20
         "('z', 10, 500);");
  const std::vector<Row> extreme_biased =
      run("SELECT xl.v, xr.w FROM biased_xl xl JOIN biased_xr xr "
          "ON xl.k = xr.k AND xl.week = xr.week - 10 ORDER BY xl.v;");
  // p straddles the top of the range without matching (MAX - 9 does not
  // equal MAX - 10); the q pair matches across the minimum; z matches at 0.
  ASSERT_EQ(extreme_biased.size(), 2U);
  EXPECT_EQ(extreme_biased[0][0].value.int_value, 2);
  EXPECT_EQ(extreme_biased[0][1].value.int_value, 200);
  EXPECT_EQ(extreme_biased[1][0].value.int_value, 4);
  EXPECT_EQ(extreme_biased[1][1].value.int_value, 500);
}

// Disjunctions whose every branch implies the same equi-join conjunct gain a
// synthetic join edge in the relational planner (TPC-DS q13 shape): the
// pair keys a hash join while the original disjunction re-verifies matched
// pairs, so results match the explicit conjunct spelling row for row and
// the join stops being a nested loop over the cross product.
TEST_F(QueryTest, DisjunctCommonEqualityBecomesJoinEdge) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE edge_l (k INT64, m INT64);");
  RunSql(ctx, *db_, "CREATE TABLE edge_r (k INT64, n INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO edge_l VALUES (1, 1), (2, 1), (3, 2), (4, 9);");
  RunSql(ctx, *db_,
         "INSERT INTO edge_r VALUES (1, 5), (2, 9), (3, 2), (9, 9);");
  SqlEngine engine(*db_);
  const auto run_count = [&](const std::string& sql) {
    StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
    EXPECT_TRUE(prepared.HasValue()) << sql << "\n" << engine.LastError();
    if (!prepared.HasValue()) {
      return std::make_pair(int64_t{-1}, std::string());
    }
    Row row;
    EXPECT_TRUE(prepared.Value()->Next(&row, nullptr));
    const int64_t count = row[0].value.int_value;
    std::ostringstream plan;
    prepared.Value()->Dump(plan, 0);
    EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));
    return std::make_pair(count, plan.str());
  };
  // The trailing uncorrelated EXISTS forces the relational interpreter
  // (PostRewriteNeedsRelational), where the greedy planner would otherwise
  // see `(a AND p) OR (a AND q)` as an opaque three-source blob.
  const auto [or_count, or_plan] = run_count(
      "SELECT COUNT(*) FROM edge_l, edge_r "
      "WHERE ((edge_l.k = edge_r.k AND edge_l.m = 1) "
      "OR (edge_l.k = edge_r.k AND edge_r.n = 2)) "
      "AND EXISTS (SELECT 1 FROM edge_r);");
  EXPECT_EQ(or_count, 3);
  EXPECT_NE(or_plan.find("nested_loop_joins=0"), std::string::npos) << or_plan;
  EXPECT_NE(or_plan.find("hash_joins=1"), std::string::npos) << or_plan;
  // Same multiset as the explicit conjunct spelling.
  const auto [explicit_count, explicit_plan] = run_count(
      "SELECT COUNT(*) FROM edge_l, edge_r "
      "WHERE edge_l.k = edge_r.k "
      "AND (edge_l.m = 1 OR edge_r.n = 2) "
      "AND EXISTS (SELECT 1 FROM edge_r);");
  EXPECT_EQ(explicit_count, or_count);
  // A branch that drops the shared equality must NOT gain an edge: rows
  // where the equality fails can still satisfy the disjunction, so the only
  // sound plan evaluates the full predicate (result pin; the plan shape is
  // unchanged and may stay a nested loop).
  const auto [partial_count, partial_plan] = run_count(
      "SELECT COUNT(*) FROM edge_l, edge_r "
      "WHERE ((edge_l.k = edge_r.k AND edge_l.m = 1) "
      "OR (edge_r.n = 9)) "
      "AND EXISTS (SELECT 1 FROM edge_r);");
  // Equality pairs: (1,1)r(1,5), (2,1)r(2,9); plus every left row against
  // the two n=9 right rows (one pair already counted).
  EXPECT_EQ(partial_count, 9);
  EXPECT_NE(partial_plan.find("join_comparisons="), std::string::npos);
}

// Three-way disjunction with two shared equi-join pairs (TPC-DS q13): both
// pairs become hash join keys, so every join step in the chain stays a hash
// join instead of collapsing into nested loops over the accumulated cross.
TEST_F(QueryTest, ThreeWayDisjunctCommonPairsKeyBothJoins) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE fact (h INT64, c INT64, tag INT64);");
  RunSql(ctx, *db_, "CREATE TABLE dimh (h INT64, y INT64);");
  RunSql(ctx, *db_, "CREATE TABLE dimc (c INT64, z INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO fact VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3), "
         "(4, 40, 4);");
  RunSql(ctx, *db_, "INSERT INTO dimh VALUES (1, 100), (3, 300);");
  RunSql(ctx, *db_, "INSERT INTO dimc VALUES (10, 7), (30, 9), (40, 1);");
  SqlEngine engine(*db_);
  StatusOr<Executor> prepared = engine.Prepare(
      ctx,
      "SELECT fact.tag, dimh.y, dimc.z FROM fact, dimh, dimc "
      "WHERE ((fact.h = dimh.h AND fact.c = dimc.c AND fact.tag = 1) "
      "OR (fact.h = dimh.h AND fact.c = dimc.c AND dimh.y = 300) "
      "OR (fact.h = dimh.h AND fact.c = dimc.c AND dimc.z = 1)) "
      "AND EXISTS (SELECT 1 FROM dimc) "
      "ORDER BY fact.tag;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  // Stats are only populated after execution (Dump does not execute).
  std::ostringstream plan;
  prepared.Value()->Dump(plan, 0);
  EXPECT_NE(plan.str().find("nested_loop_joins=0"), std::string::npos)
      << plan.str();
  EXPECT_NE(plan.str().find("hash_joins=2"), std::string::npos) << plan.str();
  // fact(1,10): branch 1 (tag=1) TRUE. fact(3,30): branch 2 (y=300) TRUE.
  // fact(2,20) has no dimh row and fact(4,40) fails `fact.h = dimh.h` in
  // every branch, so both drop out.
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0].value.int_value, 1);
  EXPECT_EQ(rows[0][1].value.int_value, 100);
  EXPECT_EQ(rows[0][2].value.int_value, 7);
  EXPECT_EQ(rows[1][0].value.int_value, 3);
  EXPECT_EQ(rows[1][1].value.int_value, 300);
  EXPECT_EQ(rows[1][2].value.int_value, 9);
}

// An arithmetic overflow inside a WHERE conjunct must surface through the
// drain-status channel instead of reading as EOF (a silent short result).
TEST_F(QueryTest, FilterOverflowSurfacesThroughDrainStatus) {
  SqlEngine& engine = *engines_.front();
  TransactionContext drain_ctx = db_->BeginContext();
  RunSql(drain_ctx, *db_, "CREATE TABLE t (k VARCHAR(4), week INT64);");
  RunSql(drain_ctx, *db_,
         "INSERT INTO t VALUES ('z', 10), "
         "('lo', -9223372036854775803);");  // INT64_MIN + 5
  ASSERT_SUCCESS(drain_ctx.txn_.PreCommit());
  drain_ctx = db_->BeginContext();

  auto drained = [&](const std::string& sql) {
    auto r = engine.Execute(drain_ctx, sql);
    EXPECT_TRUE(r.HasValue()) << sql << "\n" << engine.LastError();
    if (!r.HasValue()) {
      return std::make_pair(std::string(), false);
    }
    std::vector<Row> rows;
    r.Value().ForEach([&](const Row& row) { rows.push_back(row); });
    const Status status = r.Value().GetStatus();
    return std::make_pair(ToString(status), status == Status::kSuccess);
  };

  // The 'lo' row raises on `week - 10`: the query fails loudly even though
  // the 'z' row matched (drain may have collected it before the error).
  const auto [status, ok] = drained("SELECT k FROM t WHERE week - 10 = 0;");
  EXPECT_FALSE(ok) << "overflow swallowed as success (status: " << status
                   << ")";
  EXPECT_NE(status.find("overflow"), std::string::npos)
      << "expected an overflow diagnostic, got: " << status;

  // Tolerant-WHERE semantics: a sibling conjunct that cleanly rejects the
  // row suppresses the sibling error, so the same scan succeeds with an
  // empty result instead of raising.
  drain_ctx = db_->BeginContext();
  const auto [tolerant_status, tolerant_ok] =
      drained("SELECT k FROM t WHERE week - 10 = 0 AND k = 'nomatch';");
  EXPECT_TRUE(tolerant_ok) << tolerant_status;
}
TEST_F(QueryTest, ProjectedIndexReadsRebindPredicatesAndPreserveNulls) {
  {
    auto seed = db_->BeginContext();
    RunSql(seed, *db_,
           "CREATE TABLE read_projection (id BIGINT PRIMARY KEY, bucket "
           "BIGINT, balance BIGINT, padding STRING)");
    RunSql(seed, *db_,
           "INSERT INTO read_projection VALUES "
           "(0,0,NULL,'zero'),(1,1,10,'one'),(2,NULL,20,'two'),(3,1,30,'three')"
           ",(4,0,40,'four'),(5,1,NULL,'five')");
    RunSql(seed, *db_, "ANALYZE");
    RunSql(seed, *db_, "UPDATE read_projection SET balance = balance");
    ASSERT_SUCCESS(seed.PreCommit());
  }
  SqlEngine engine(*db_);
  const uint64_t before = PlanCacheStats().replays.load();
  for (int repetition = 0; repetition < 3; ++repetition) {
    for (int key = 0; key < 2; ++key) {
      auto reader = db_->BeginReadOnlyContext();
      const auto rows = RunPrepared(
          &engine, &reader,
          "SELECT id, balance FROM read_projection WHERE bucket = " +
              std::to_string(key) + " ORDER BY id LIMIT 2");
      const std::vector<Row> expected =
          key == 0 ? std::vector<Row>{Row({Value(0), Value()}),
                                      Row({Value(4), Value(40)})}
                   : std::vector<Row>{Row({Value(1), Value(10)}),
                                      Row({Value(3), Value(30)})};
      EXPECT_EQ(rows, expected);
      const auto sum = RunPrepared(
          &engine, &reader,
          "SELECT SUM(balance) FROM read_projection WHERE id BETWEEN " +
              std::to_string(key) + " AND " + std::to_string(key + 2));
      EXPECT_EQ(sum, std::vector<Row>{Row({Value(key == 0 ? 30 : 60)})});
      // Reordered/duplicated outputs keep the general projection path.
      EXPECT_EQ(RunPrepared(&engine, &reader,
                            "SELECT padding, balance, id, id FROM "
                            "read_projection WHERE id = 1"),
                (std::vector<Row>{
                    Row({Value("one"), Value(10), Value(1), Value(1)})}));
      ASSERT_SUCCESS(reader.PreCommit());
    }
  }
  EXPECT_GT(PlanCacheStats().replays.load(), before);
}

TEST_F(QueryTest,
       ProjectedIndexReadsKeepNonIntegerResidualAndUnsignedConstant) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE read_residual (id BIGINT PRIMARY KEY, balance BIGINT, "
         "text STRING)");
  RunSql(ctx, *db_,
         "INSERT INTO read_residual VALUES "
         "(0,-1,'skip'),(1,20,'keep'),(2,NULL,'keep'),(3,30,'skip')");
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT id, balance FROM read_residual WHERE id BETWEEN 0 AND 3 "
             "AND text = 'keep' ORDER BY id"),
      (std::vector<Row>{Row({Value(1), Value(20)}), Row({Value(2), Value()})}));
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT id FROM read_residual WHERE id BETWEEN 0 AND 3 AND "
             "balance < CAST(18446744073709551615 AS UINT64) ORDER BY id"),
      (std::vector<Row>{Row({Value(0)}), Row({Value(1)}), Row({Value(3)})}));
}

TEST_F(QueryTest, ArrayColumnTableSupportsAggregationAndGroupBy) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE arr_agg (grp BIGINT, tags ARRAY<INT64>, val BIGINT)");
  RunSql(ctx, *db_,
         "INSERT INTO arr_agg VALUES "
         "(1, [10, 20], 100), (1, [30], 200), (2, [], 50)");
  EXPECT_EQ(RunSql(ctx, *db_, "SELECT COUNT(*), SUM(val) FROM arr_agg"),
            (std::vector<Row>{Row({Value(3), Value(350)})}));
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT grp, COUNT(*), SUM(val) FROM arr_agg "
                   "GROUP BY grp ORDER BY grp"),
            (std::vector<Row>{Row({Value(1), Value(2), Value(300)}),
                              Row({Value(2), Value(1), Value(50)})}));
}

TEST_F(QueryTest,
       ParallelLstAggregationHandlesVarcharCountMinMaxAboveThreshold) {
  {
    auto seed = db_->BeginContext();
    RunSql(seed, *db_,
           "CREATE TABLE lst_varchar_agg (id BIGINT, label STRING)");
    ASSIGN_OR_ASSERT_FAIL_CONST(std::shared_ptr<Table>, tbl,
                                seed.GetTable("lst_varchar_agg"));
    for (int64_t i = 0; i < 33000; ++i) {
      ASSERT_SUCCESS(
          tbl->Insert(seed.txn_,
                      Row({Value(i), Value(i % 2 == 0 ? "aa" : "zz")}))
              .GetStatus());
    }
    ASSERT_SUCCESS(seed.PreCommit());
  }
  {
    auto analyze_ctx = db_->BeginContext();
    RunSql(analyze_ctx, *db_, "ANALYZE");
    ASSERT_SUCCESS(analyze_ctx.PreCommit());
  }
  auto ctx = db_->BeginContext();
  EXPECT_EQ(
      RunSql(
          ctx, *db_,
          "SELECT COUNT(label), MIN(label), MAX(label) FROM lst_varchar_agg"),
      (std::vector<Row>{Row({Value(33000), Value("aa"), Value("zz")})}));
}

TEST_F(QueryTest,
       TimestampsOutsideInt64NanosEpochAndIsoTSeparatorEvaluateCorrectly) {
  auto ctx = db_->BeginContext();
  // Bug 5: CivilTimeToNanos before 1677 and after 2262 must not wrap int64_t.
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT UNIX_SECONDS(TIMESTAMP('0001-01-01 00:00:00+00')), "
             "UNIX_SECONDS(TIMESTAMP('9999-12-31 23:59:59+00'))"),
      (std::vector<Row>{Row({Value(-62135596800LL), Value(253402300799LL)})}));

  // Bug 15: ISO-8601 'T' separator with explicit offset or Z must preserve
  // time-of-day and apply timezone offset accurately.
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT CAST(TIMESTAMP '2020-01-01T12:34:56Z' AS STRING), "
                   "CAST(TIMESTAMP '2020-01-01T12:34:56+09:00' AS STRING)"),
            (std::vector<Row>{Row({Value("2020-01-01 12:34:56+00"),
                                   Value("2020-01-01 03:34:56+00")})}));
}

TEST_F(QueryTest, OrderByDistinguishesFilteredAggregatesWithDifferentFilters) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE filt_ord (grp BIGINT, v BIGINT)");
  RunSql(ctx, *db_,
         "INSERT INTO filt_ord VALUES "
         "(1, 10), (1, 10), (1, -5), "
         "(2, 10), (2, -5), (2, -5)");
  // grp=1 has COUNT(v>0)=2, COUNT(v<0)=1; grp=2 has COUNT(v>0)=1, COUNT(v<0)=2.
  // Ordering by COUNT(v WHERE v < 0) ASC must place grp=1 first.
  const auto rows = RunSql(ctx, *db_,
                           "SELECT grp, COUNT(v WHERE v > 0) "
                           "FROM filt_ord GROUP BY grp "
                           "ORDER BY COUNT(v WHERE v < 0) ASC");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(2)}));
  EXPECT_EQ(rows[1], Row({Value(2), Value(1)}));
}

TEST_F(QueryTest, StructWithDateEncodesValidQuotedJsonForJsonType) {
  auto ctx = db_->BeginContext();
  EXPECT_EQ(RunSql(ctx, *db_,
                   "SELECT JSON_TYPE(TO_JSON(STRUCT(DATE '2026-01-02' AS d)))"),
            (std::vector<Row>{Row({Value("object")})}));
  RunSql(ctx, *db_, "CREATE TABLE struct_date_tbl (d DATE)");
  RunSql(ctx, *db_, "INSERT INTO struct_date_tbl VALUES (DATE '2026-01-02')");
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT JSON_TYPE(TO_JSON(STRUCT(d AS d))) FROM struct_date_tbl"),
      (std::vector<Row>{Row({Value("object")})}));
}

TEST_F(QueryTest, NestedLeftNaturalJoinRejectsInsteadOfDroppingSources) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE nj1 (id BIGINT)");
  RunSql(ctx, *db_, "CREATE TABLE nj2 (id BIGINT)");
  RunSql(ctx, *db_, "CREATE TABLE nj3 (id BIGINT)");
  SqlEngine engine(*db_);
  StatusOr<Executor> prepared = engine.Prepare(
      ctx, "SELECT * FROM (nj1 NATURAL JOIN nj2) JOIN nj3 ON nj1.id = nj3.id");
  EXPECT_FALSE(prepared.HasValue());
}

TEST_F(QueryTest, TvfWithUnionAllBodyPreservesAllSetOperationBranches) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_,
         "CREATE TABLE FUNCTION tvf_union(x INT64) AS "
         "SELECT x AS v UNION ALL SELECT x + 10 AS v");
  const auto rows = RunSql(ctx, *db_, "SELECT v FROM tvf_union(5) ORDER BY v");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(5)}));
  EXPECT_EQ(rows[1], Row({Value(15)}));
}

TEST_F(QueryTest,
       SharedCteReferencedInSubqueryDoesNotPushDownRestrictiveFilter) {
  auto ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE cte_src (k BIGINT)");
  RunSql(ctx, *db_, "INSERT INTO cte_src VALUES (1), (2), (3)");
  // The outer FROM filters `c WHERE c.k = 1`, while the scalar subquery counts
  // all rows in `c`. Pushing `k = 1` into `c` would starve the scalar subquery
  // and return 1 instead of 3.
  const auto rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k FROM cte_src) "
             "SELECT c.k, (SELECT COUNT(*) FROM c) FROM c WHERE c.k = 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(1), Value(3)}));

  // Also test when `c` is referenced inside a nested WITH inside a sibling CTE.
  const auto nested_with_rows =
      RunSql(ctx, *db_,
             "WITH c AS (SELECT k FROM cte_src), "
             "d AS (WITH inner_c AS (SELECT k FROM c) "
             "SELECT COUNT(*) AS cnt FROM inner_c) "
             "SELECT c.k, d.cnt FROM c CROSS JOIN d WHERE c.k = 1");
  ASSERT_EQ(nested_with_rows.size(), 1U);
  EXPECT_EQ(nested_with_rows[0], Row({Value(1), Value(3)}));
}

// Views are expanded inline while parsing, so the cached template holds the
// expanded body.  A view redefinition must not replay the old body for a
// statement with the same literalized fingerprint.
TEST_F(QueryTest, TemplateCacheDoesNotReplayStaleViewBody) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  const auto template_fills = [] {
    return PlanCacheStats().template_fills.load();
  };
  RunPrepared(&engine, &ctx, "CREATE TABLE tpl_view_t (a INT64, b INT64)");
  RunPrepared(&engine, &ctx,
              "INSERT INTO tpl_view_t VALUES (10, 1), (20, 2), (30, 3)");
  RunPrepared(&engine, &ctx,
              "CREATE VIEW tpl_view_v AS SELECT a, b FROM tpl_view_t");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  const uint64_t fills_before = template_fills();
  EXPECT_EQ(RunPrepared(&engine, &ctx, "SELECT a FROM tpl_view_v WHERE b = 1"),
            (std::vector<Row>{Row({Value(int64_t{10})})}));
  // Precondition: the expanded-view tree really went into the template cache
  // (a view body containing literals would fail to rebind and skip the fill).
  EXPECT_EQ(template_fills(), fills_before + 1);
  EXPECT_EQ(RunPrepared(&engine, &ctx, "SELECT a FROM tpl_view_v WHERE b = 2"),
            (std::vector<Row>{Row({Value(int64_t{20})})}));

  RunPrepared(&engine, &ctx, "DROP VIEW tpl_view_v");
  RunPrepared(&engine, &ctx,
              "CREATE VIEW tpl_view_v AS SELECT b AS a, b FROM tpl_view_t");
  ASSERT_SUCCESS(ctx.PreCommit());
  ctx = db_->BeginContext();
  EXPECT_EQ(RunPrepared(&engine, &ctx, "SELECT a FROM tpl_view_v WHERE b = 3"),
            (std::vector<Row>{Row({Value(int64_t{3})})}));
  EXPECT_EQ(ctx.txn_.Abort(), Status::kSuccess);
}

}  // namespace tinylamb
