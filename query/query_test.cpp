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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/window_function_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class QueryTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "query_test-" + RandomString();
    db_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

namespace {
std::vector<Row> RunSql(TransactionContext& ctx, Database& db,
                        std::string_view sql) {
  SqlEngine engine(db);
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
}
}  // namespace

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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  while (prepared.Value()->Next(&row, nullptr)) {
    text += row[0].AsString();
  }

  // Assert -- the plan embeds the runtime statistics.
  EXPECT_NE(text.find("Planning Time"), std::string::npos) << text;
  EXPECT_NE(text.find("Actual Rows"), std::string::npos) << text;
  EXPECT_NE(text.find("Execution Time"), std::string::npos) << text;
  ctx.txn_.Abort();
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
  EXPECT_EQ(before->Rows(), 0);

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

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineExplainBoundaryRejectsMalformedKeyword) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  // Act + Assert -- "EXPLAINX" is not EXPLAIN; it must fail as bad SQL.
  StatusOr<Executor> prepared = engine.Prepare(ctx, "EXPLAINX SELECT 1;");
  EXPECT_FALSE(prepared.HasValue());
  ctx.txn_.Abort();
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
  EXPECT_EQ(mismatch.GetStatus(), Status::kUnknown);
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  EXPECT_EQ(engine.LastError(), "ANALYZE failed");

  // Act + Assert -- "ANALYZEt" is not the ANALYZE keyword; the boundary check
  // rejects it and the query falls through to the SQL parser.
  EXPECT_FALSE(engine.Prepare(ctx, "ANALYZEt;").HasValue());

  // Act + Assert -- a table list that starts with a non-identifier char.
  EXPECT_FALSE(engine.Prepare(ctx, "ANALYZE 1abc").HasValue());

  // Act + Assert -- table names separated without a comma are rejected.
  EXPECT_FALSE(engine.Prepare(ctx, "ANALYZE t1 t2").HasValue());

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineTemplateCacheEvictsFullShard) {
  // Arrange -- the process-global template cache has 16 shards of 64 entries.
  // Prepare thousands of distinct templatable statements so every shard
  // overflows and the oldest cached template in a shard is evicted.
  {
    TransactionContext ctx = db_->BeginContext();
    RunSql(ctx, *db_, "CREATE TABLE tpl_t (a INT64);");
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  SqlEngine engine(*db_);
  for (size_t i = 0; i < 50; ++i) {
    const std::string sql =
        "INSERT INTO tpl_tbl" + std::to_string(i) + " VALUES (1);";
    TransactionContext ctx = db_->BeginContext();
    (void)engine.Prepare(ctx, sql);
    ctx.txn_.Abort();
  }
  // Assert -- implicit; preparing a templatable statement afterwards still
  // succeeds and can hit the (now-trimmed) cache without crashing.
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(
        engine.Prepare(ctx, "INSERT INTO tpl_t VALUES (1);").HasValue());
    ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
}

TEST_F(QueryTest, WindowGroupsFrameUsesPeerGroups) {
  TransactionContext ctx = db_->BeginContext();
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("v"))};
  window->order_by = {
      WindowOrderTerm{ColumnValueExp(ColumnName("v")), true, std::nullopt}};
  window->frame_unit = WindowFrameUnit::kGroups;
  window->has_frame = true;
  window->frame_start = {WindowFrameBoundType::kOffsetPreceding,
                         ConstantValueExp(Value(1))};
  window->frame_end = {WindowFrameBoundType::kCurrentRow, nullptr};
  SelectStatement statement({NamedExpression("sum", Expression(window))}, {},
                            nullptr);
  relational_detail::Relation input;
  input.schema = Schema("", {Column("v", ValueType::kInt64)});
  input.rows = {Row({Value(10)}), Row({Value(10)}), Row({Value(20)}),
                Row({Value(30)})};
  auto windowed = relational_detail::ApplyWindows(
      ctx, statement, std::move(input), nullptr, {});
  std::vector<Row> rows;
  windowed.input.ForEachRow([&](const Row& row) { rows.push_back(row); });
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][1], Value(20));
  EXPECT_EQ(rows[1][1], Value(20));
  EXPECT_EQ(rows[2][1], Value(40));
  EXPECT_EQ(rows[3][1], Value(50));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, WindowFrameExclusionRemovesCurrentRow) {
  TransactionContext ctx = db_->BeginContext();
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp(ColumnName("v"))};
  window->order_by = {
      WindowOrderTerm{ColumnValueExp(ColumnName("v")), true, std::nullopt}};
  window->frame_unit = WindowFrameUnit::kRows;
  window->has_frame = true;
  window->frame_start = {WindowFrameBoundType::kUnboundedPreceding, nullptr};
  window->frame_end = {WindowFrameBoundType::kCurrentRow, nullptr};
  window->exclusion = WindowFrameExclusion::kCurrentRow;
  SelectStatement statement({NamedExpression("sum", Expression(window))}, {},
                            nullptr);
  relational_detail::Relation input;
  input.schema = Schema("", {Column("v", ValueType::kInt64)});
  input.rows = {Row({Value(10)}), Row({Value(20)}), Row({Value(30)})};
  auto windowed = relational_detail::ApplyWindows(
      ctx, statement, std::move(input), nullptr, {});
  std::vector<Row> rows;
  windowed.input.ForEachRow([&](const Row& row) { rows.push_back(row); });
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][1], Value(10));
  EXPECT_EQ(rows[2][1], Value(30));
  ctx.txn_.Abort();
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
        WindowOrderTerm{ColumnValueExp(ColumnName("v")), true, std::nullopt}};
    window->frame_unit = WindowFrameUnit::kRows;
    window->has_frame = true;
    window->frame_start = {WindowFrameBoundType::kUnboundedPreceding, nullptr};
    window->frame_end = {WindowFrameBoundType::kUnboundedFollowing, nullptr};
    window->exclusion = exclusion;
    SelectStatement statement({NamedExpression("sum", Expression(window))}, {},
                              nullptr);
    relational_detail::Relation input;
    input.schema = Schema("", {Column("v", ValueType::kInt64)});
    input.rows = {Row({Value(10)}), Row({Value(10)}), Row({Value(20)})};
    auto windowed = relational_detail::ApplyWindows(
        ctx, statement, std::move(input), nullptr, {});
    std::vector<Row> rows;
    windowed.input.ForEachRow([&](const Row& row) { rows.push_back(row); });
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(rows[i][1], expected[i]) << "row " << i;
    }
    ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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

TEST_F(QueryTest, CascadesOptimizesUnnestPlans) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  StatusOr<Executor> explain =
      engine.Prepare(ctx, "EXPLAIN SELECT x FROM UNNEST([1, 2, 3]) x;");
  ASSERT_TRUE(explain.HasValue()) << engine.LastError();
  std::string plan_text;
  Row row;
  while (explain.Value()->Next(&row, nullptr)) {
    plan_text += row[0].value.varchar_value;
    plan_text += "\n";
  }
  EXPECT_NE(plan_text.find("Unnest"), std::string::npos);
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
    bool rejected = false;
    try {
      while (prepared.Value()->Next(&row, nullptr)) {
      }
    } catch (const std::exception& error) {
      rejected = true;
      EXPECT_NE(std::string(error.what()).find("ambiguous"), std::string::npos)
          << error.what();
    }
    EXPECT_TRUE(rejected) << "ambiguous column was silently resolved";
  }

  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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

  // Act + Assert -- neighbouring queries keep working after a LIMIT 0 ran.
  std::vector<Row> limited = RunSql(ctx, *db_, "SELECT a FROM t LIMIT 2;");
  ASSERT_EQ(limited.size(), 2U);
  std::vector<Row> all = RunSql(ctx, *db_, "SELECT a FROM t;");
  ASSERT_EQ(all.size(), 3U);

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
}

namespace {
// Drains one statement through `engine` and returns the materialized rows.
std::vector<Row> RunPrepared(SqlEngine* engine, TransactionContext* ctx,
                             const std::string& sql) {
  std::vector<Row> rows;
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine->LastError();
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  return rows;
}
}  // namespace

TEST_F(QueryTest, SqlEnginePlanCacheHitsOnRepeatedStatement) {
  // Arrange -- a unique table so no other test shares this fingerprint.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_hit (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_hit VALUES (7, 70), (8, 80);");

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
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEnginePlanCacheInvalidatedByDdlAndAnalyze) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_ddl (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (5, 50);");
  ASSERT_EQ(
      RunPrepared(&engine, &ctx, "SELECT v FROM pcache_ddl WHERE k = 5;")[0][0],
      Value(50));

  // Act (a) -- DROP + re-CREATE the same-named table with different data.
  // The fingerprint of the SELECT is unchanged, but the stale entry must be
  // discarded via the epoch check instead of replaying old page contents.
  ASSERT_TRUE(engine.Prepare(ctx, "DROP TABLE pcache_ddl;").HasValue());
  RunSql(ctx, *db_, "CREATE TABLE pcache_ddl (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (5, 555);");
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
  const uint64_t invalidations_mid =
      PlanCacheStats().epoch_invalidations.load();
  std::vector<Row> analyzed =
      RunPrepared(&engine, &ctx, "SELECT v FROM pcache_ddl WHERE k = 6;");
  ASSERT_EQ(analyzed.size(), 1U);
  EXPECT_EQ(analyzed[0][0], Value(60));
  EXPECT_GT(PlanCacheStats().epoch_invalidations.load(), invalidations_mid);

  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEnginePlanCacheParameterCorrectness) {
  // (1) SELECT with differing literals: a specialized entry is only served
  // for identical parameters, and differing parameters re-specialize it.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE pcache_param (k INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO pcache_param VALUES (1, 10), (2, 20), (3, 30);");
  ASSERT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_param WHERE k = 1;")[0][0],
            Value(10));  // fills the entry with k=1's plan
  ASSERT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_param WHERE k = 2;")[0][0],
            Value(20));  // parameter mismatch: legacy compile, refresh entry
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

  ctx.txn_.Abort();
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
  EXPECT_NE(plan_text.find("Apply"), std::string::npos);
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
}

TEST_F(QueryTest, BatchInsertChunking) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE batch_tbl (id INT64, val INT64);");

  // Construct a large batch INSERT statement with 150 rows (> 2 chunks of 64)
  std::string sql = "INSERT INTO batch_tbl VALUES ";
  for (int i = 0; i < 150; ++i) {
    if (i > 0) sql += ", ";
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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

  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
}

TEST_F(QueryTest, DescendingIndexScanReturnsReversedRange) {
  // The optimizer must offer reverse index scans for a descending ORDER BY
  // over a range: the B+Tree iterator positions below `end` and walks left,
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
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  EXPECT_NE(json.find(R"(a\"b\\c)"), std::string::npos)
      << "unparseable struct JSON: " << json;
  EXPECT_NE(json.find(R"("n":3)"), std::string::npos) << json;
  ctx.txn_.Abort();
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
  EXPECT_EQ(prepared.GetStatus(), Status::kUnknown);
  ctx.txn_.Abort();
}

namespace {
// Runs a scalar SELECT to completion; execution-time errors (overflow, divide
// by zero) surface as exceptions from Next(), matching how the engine reports
// them to callers.
std::vector<Row> RunScalar(TransactionContext& ctx, Database& db,
                           std::string_view sql) {
  SqlEngine engine(db);
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  return rows;
}
}  // namespace

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

  // A9: MOD(INT64_MIN, -1) returns 0 (was SIGFPE).
  const auto mod_min =
      RunScalar(ctx, *db_, "SELECT MOD(-9223372036854775808, -1);");
  ASSERT_EQ(mod_min.size(), 1U);
  EXPECT_EQ(mod_min[0][0], Value(int64_t{0}));

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

  // A4: AVG OVER a negative int64 sum (was uint64 misinterpretation).
  RunSql(ctx, *db_, "CREATE TABLE av (v INT64);");
  RunSql(ctx, *db_, "INSERT INTO av VALUES (-5), (3);");
  const auto avg_over =
      RunScalar(ctx, *db_, "SELECT AVG(v) OVER () FROM av ORDER BY v;");
  ASSERT_GE(avg_over.size(), 1U);
  EXPECT_EQ(avg_over[0][0], Value(-1.0));
  ctx.txn_.Abort();
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
  ctx.txn_.Abort();
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
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  const std::vector<Row> rows =
      RunSql(ctx, *db_,
             "SELECT a.v, b.w FROM a JOIN b ON a.k < b.k ORDER BY a.v, b.w;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Row({Value("a1"), Value("b2")}));
  EXPECT_EQ(rows[1], Row({Value("a1"), Value("b4")}));
  EXPECT_EQ(rows[2], Row({Value("a3"), Value("b4")}));
  ctx.txn_.Abort();
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
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  const std::vector<Row> rows = RunSql(
      ctx, *db_, "SELECT x.val FROM x JOIN y ON x.id = y.id ORDER BY x.val;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0][0], Value(100));
  EXPECT_EQ(rows[1][0], Value(300));
  ctx.txn_.Abort();
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
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  const std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT p.num FROM p JOIN q ON p.k = q.k AND p.num > q.threshold;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(50));
  ctx.txn_.Abort();
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
  EXPECT_EQ(plan_text.find("RelationalPlan"), std::string::npos) << plan_text;

  std::vector<Row> total =
      RunSql(ctx, *db_,
             "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
             "FROM t WHERE n < 5) SELECT SUM(n) FROM t;");
  ASSERT_EQ(total.size(), 1U);
  EXPECT_EQ(total[0][0], Value(15));
  ctx.txn_.Abort();
}

}  // namespace tinylamb
