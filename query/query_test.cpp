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
#include <cstdint>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <vector>
#include <utility>
#include <string_view>
#include <string>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "executor/constant_executor.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/insert.hpp"
#include "expression/window_function_expression.hpp"
#include "gtest/gtest.h"
#include "parser/parser.hpp"
#include "parser/token.hpp"
#include "parser/tokenizer.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "query/plan_cache.hpp"
#include "query/query_data.hpp"
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

  StatusOr<Executor> Execute(TransactionContext& ctx,
                             std::unique_ptr<Statement> stmt) const {
    switch (stmt->Type()) {
      case StatementType::kCreateTable: {
        auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
        ASSIGN_OR_RETURN(Table, table,
                         db_->CreateTable(ctx, Schema(create_table.TableName(),
                                                      create_table.Columns())));
        return {std::make_shared<ConstantExecutor>(
            Row({Value(0), Value("CREATE TABLE")}))};
      }
      case StatementType::kInsert: {
        auto& insert = dynamic_cast<InsertStatement&>(*stmt);
        ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, ctx.GetTable(insert.TableName()));
        if (insert.Values().size() == 1) {
          std::vector<Value> vals;
          for (const auto& expr : insert.Values()[0]) {
            vals.emplace_back(expr->Evaluate(Row(), Schema()));
          }
          Row row(std::move(vals));
          return {std::make_shared<Insert>(ctx.txn_, table.get(),
                                           std::make_shared<ConstantExecutor>(row))};
        }
        return Status::kNotImplemented;
      }
      case StatementType::kSelect: {
        auto& select = dynamic_cast<SelectStatement&>(*stmt);
        QueryData query;
        query.from_ = select.FromClause();
        query.where_ = select.WhereClause();
        query.select_ = select.SelectList();
        ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
        return plan->EmitExecutor(ctx);
      }
      default:
        return Status::kNotImplemented;
    }
  }

  StatusOr<Executor> ExecuteQuery(TransactionContext& ctx,
                                  const std::string& sql) const {
    Tokenizer tokenizer(sql);
    std::vector<Token> tokens = tokenizer.Tokenize();
    Parser parser(tokens);
    return Execute(ctx, parser.Parse());
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(QueryTest, SimpleSelect) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  Row result;

  // Act + Assert: CREATE TABLE t1
  auto st_create =
      ExecuteQuery(ctx, "CREATE TABLE t1 (c1 INT, c2 INT, c3 VARCHAR(10));");
  ASSERT_EQ(st_create.GetStatus(), Status::kSuccess);
  auto exec_create = std::move(st_create.Value());
  ASSERT_TRUE(exec_create->Next(&result, nullptr));

  // Act + Assert: INSERT (1, 10, 'hello')
  auto st_insert1 = ExecuteQuery(ctx, "INSERT INTO t1 VALUES (1, 10, 'hello');");
  ASSERT_EQ(st_insert1.GetStatus(), Status::kSuccess);
  auto exec_insert1 = std::move(st_insert1.Value());
  ASSERT_TRUE(exec_insert1->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(1));
  ASSERT_FALSE(exec_insert1->Next(&result, nullptr));

  // Act + Assert: INSERT (2, 20, 'world')
  auto st_insert2 = ExecuteQuery(ctx, "INSERT INTO t1 VALUES (2, 20, 'world');");
  ASSERT_EQ(st_insert2.GetStatus(), Status::kSuccess);
  auto exec_insert2 = std::move(st_insert2.Value());
  ASSERT_TRUE(exec_insert2->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(1));
  ASSERT_FALSE(exec_insert2->Next(&result, nullptr));

  // Act + Assert: SELECT * FROM t1 WHERE c1 = 1
  auto st_select = ExecuteQuery(ctx, "SELECT * FROM t1 WHERE c1 = 1;");
  ASSERT_EQ(st_select.GetStatus(), Status::kSuccess);
  auto exec_select = std::move(st_select.Value());
  ASSERT_TRUE(exec_select->Next(&result, nullptr));
  ASSERT_EQ(result[0], Value(1));
  ASSERT_EQ(result[1], Value(10));
  ASSERT_EQ(result[2], Value("hello"));
  ASSERT_FALSE(exec_select->Next(&result, nullptr));

  // Act + Assert: PreCommit
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, SelectWithProjection) {
  // Arrange
  TransactionContext ctx = db_->BeginContext();
  Row result;

  // Act + Assert: CREATE TABLE t1
  auto st_create =
      ExecuteQuery(ctx, "CREATE TABLE t1 (c1 INT, c2 INT, c3 VARCHAR(10));");
  ASSERT_EQ(st_create.GetStatus(), Status::kSuccess);

  // Act + Assert: INSERT (1, 10, 'hello')
  auto st_insert1 = ExecuteQuery(ctx, "INSERT INTO t1 VALUES (1, 10, 'hello');");
  ASSERT_EQ(st_insert1.GetStatus(), Status::kSuccess);
  auto exec_insert1 = std::move(st_insert1.Value());
  ASSERT_TRUE(exec_insert1->Next(&result, nullptr));
  ASSERT_FALSE(exec_insert1->Next(&result, nullptr));

  // Act + Assert: INSERT (2, 20, 'world')
  auto st_insert2 = ExecuteQuery(ctx, "INSERT INTO t1 VALUES (2, 20, 'world');");
  ASSERT_EQ(st_insert2.GetStatus(), Status::kSuccess);
  auto exec_insert2 = std::move(st_insert2.Value());
  ASSERT_TRUE(exec_insert2->Next(&result, nullptr));
  ASSERT_FALSE(exec_insert2->Next(&result, nullptr));

  // Act + Assert: SELECT c1, c3 FROM t1 WHERE c1 = 2
  auto st_select = ExecuteQuery(ctx, "SELECT c1, c3 FROM t1 WHERE c1 = 2;");
  ASSERT_EQ(st_select.GetStatus(), Status::kSuccess);
  auto exec_select = std::move(st_select.Value());
  ASSERT_TRUE(exec_select->Next(&result, nullptr));
  ASSERT_EQ(result[0], Value(2));
  ASSERT_EQ(result[1], Value("world"));
  ASSERT_FALSE(exec_select->Next(&result, nullptr));

  // Act + Assert: PreCommit
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

namespace {
std::vector<Row> RunSql(TransactionContext& ctx, Database& db,
                        std::string_view sql) {
  SqlEngine engine(db);
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) { return rows;
}
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) { rows.push_back(row);
}
  return rows;
}
}  // namespace

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
  EXPECT_EQ(engine.LastError(), "EXPLAIN currently supports SELECT and WITH queries");
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineExplainAnalyzeSelect) {
  // Arrange -- a table with a few rows.
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  ASSERT_TRUE(engine.Prepare(ctx, "CREATE TABLE t (a INT64);").HasValue());
  ASSERT_TRUE(engine.Prepare(ctx, "INSERT INTO t VALUES (1), (2), (3);")
                  .HasValue());

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
  while (all.Value()->Next(&row, nullptr)) { ++analyze_rows;
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
  ASSERT_TRUE(engine.Prepare(ctx, "CREATE TABLE t (a INT64, b INT64);")
                  .HasValue());

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
  EXPECT_NE(engine.LastError().find("unknown INSERT column"),
            std::string::npos)
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
  while (list.Value()->Next(&row, nullptr)) { ++analyzed;
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
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);
  RunSql(ctx, *db_, "CREATE TABLE tpl_t (a INT64);");
  for (size_t i = 0; i < 1500; ++i) {
    const std::string sql =
        "INSERT INTO tpl_tbl" + std::to_string(i) + " VALUES (1);";
    // The statement is cached before the (inevitably failing) table lookup,
    // so the return status is irrelevant here.
    (void)engine.Prepare(ctx, sql);
  }
  // Assert -- implicit; preparing a templatable statement afterwards still
  // succeeds and can hit the (now-trimmed) cache without crashing.
  ASSERT_TRUE(engine.Prepare(ctx, "INSERT INTO tpl_t VALUES (1);").HasValue());
  ctx.txn_.Abort();
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

TEST_F(QueryTest, SqlEngineWindowFunctionsPartitionRankAndCumulativeSum) {
  TransactionContext ctx = db_->BeginContext();
  RunSql(ctx, *db_, "CREATE TABLE w (g INT64, v INT64);");
  RunSql(ctx, *db_,
         "INSERT INTO w VALUES (1, 20), (1, 10), (2, 5), (2, 15);");

  const std::vector<Row> rows = RunSql(
      ctx, *db_,
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
  SelectStatement statement({NamedExpression("sum", Expression(window))},
                            {}, nullptr);
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
  SelectStatement statement({NamedExpression("sum", Expression(window))},
                            {}, nullptr);
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
  for (const auto [exclusion, expected] :
       std::vector<std::pair<WindowFrameExclusion, std::vector<Value>>>{
           {WindowFrameExclusion::kGroup,
            {Value(20), Value(20), Value(20)}},
           {WindowFrameExclusion::kTies,
            {Value(30), Value(30), Value(40)}}}) {
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
    SelectStatement statement({NamedExpression("sum", Expression(window))},
                              {}, nullptr);
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
  const std::vector<Row> distinct = RunSql(
      ctx, *db_, "SELECT v FROM u UNION DISTINCT SELECT v FROM u;");
  EXPECT_EQ(distinct,
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)})}));
  const std::vector<Row> except = RunSql(
      ctx, *db_,
      "SELECT v FROM u EXCEPT DISTINCT SELECT v FROM u WHERE v = 2;");
  EXPECT_EQ(except, (std::vector<Row>{Row({Value(1)})}));
  const std::vector<Row> intersect = RunSql(
      ctx, *db_,
      "SELECT v FROM u INTERSECT DISTINCT SELECT v FROM u WHERE v = 2;");
  EXPECT_EQ(intersect, (std::vector<Row>{Row({Value(2)})}));
  const std::vector<Row> promoted = RunSql(
      ctx, *db_, "SELECT 1 AS v UNION ALL SELECT 2.5 AS v;");
  EXPECT_EQ(promoted,
            (std::vector<Row>{Row({Value(1.0)}), Row({Value(2.5)})}));
  const std::vector<Row> limited_union = RunSql(
      ctx, *db_, "SELECT v FROM u UNION ALL SELECT v FROM u LIMIT 3;");
  EXPECT_EQ(limited_union,
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)}),
                              Row({Value(1)})}));
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
  EXPECT_EQ(rows,
            (std::vector<Row>{Row({Value(2)}), Row({Value(2)}),
                              Row({Value(3)}), Row({Value(3)})}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineUnnestExpandsArraysAndEmitsOffsets) {
  TransactionContext ctx = db_->BeginContext();
  const std::vector<Row> rows = RunSql(
      ctx, *db_,
      "SELECT x, p FROM UNNEST([3, NULL]) x WITH OFFSET p ORDER BY p;");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(3), Value(0)}));
  EXPECT_EQ(rows[1][0], Value());
  EXPECT_EQ(rows[1][1], Value(1));
  EXPECT_TRUE(
      RunSql(ctx, *db_, "SELECT x FROM UNNEST([]) x;").empty());

  const std::vector<Row> product = RunSql(
      ctx, *db_,
      "SELECT a, b FROM UNNEST([1, 2]) a, UNNEST([10, 20]) b "
      "ORDER BY a, b;");
  EXPECT_EQ(product,
            (std::vector<Row>{Row({Value(1), Value(10)}),
                              Row({Value(1), Value(20)}),
                              Row({Value(2), Value(10)}),
                              Row({Value(2), Value(20)})}));

  const std::vector<Row> lateral = RunSql(
      ctx, *db_,
      "SELECT x FROM UNNEST([[1, 2], [3]]) arr, UNNEST(arr) x ORDER BY x;");
  EXPECT_EQ(lateral,
            (std::vector<Row>{Row({Value(1)}), Row({Value(2)}),
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
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST(GENERATE_SERIES(3, 1, -1)) x ORDER BY x DESC;"),
      (std::vector<Row>{Row({Value(3)}), Row({Value(2)}), Row({Value(1)})}));
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2020-01-03')) x ORDER BY x;"),
      (std::vector<Row>{Row({Value::Date("2020-01-01")}),
                        Row({Value::Date("2020-01-02")}),
                        Row({Value::Date("2020-01-03")})}));
  EXPECT_EQ(
      RunSql(ctx, *db_,
             "SELECT x FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2020-01-05', INTERVAL 2 DAY)) x ORDER BY x;"),
      (std::vector<Row>{Row({Value::Date("2020-01-01")}),
                        Row({Value::Date("2020-01-03")}),
                        Row({Value::Date("2020-01-05")})}));
  ctx.txn_.Abort();
}

TEST_F(QueryTest, SqlEngineInsertSelectCopiesAndMapsRows) {
  TransactionContext ctx = db_->BeginContext();
  SqlEngine engine(*db_);

  ASSERT_TRUE(engine.Execute(ctx, "CREATE TABLE src (id INT, name VARCHAR(10));")
                  .HasValue());
  ASSERT_TRUE(engine.Execute(ctx, "CREATE TABLE dst (name VARCHAR(10), id INT);")
                  .HasValue());
  auto source_insert = engine.Execute(
      ctx, "INSERT INTO src VALUES (1, 'one'), (2, 'two');");
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

  const std::vector<Row> rows = RunSql(
      ctx, *db_, "SELECT v FROM ul UNION ALL SELECT v FROM ul LIMIT 3;");
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

  std::vector<Row> rows = RunSql(
      ctx, *db_,
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
      RunSql(ctx, *db_, "SELECT x.v FROM t AS x, t AS y WHERE x.k = y.k AND "
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

  std::vector<Row> rows = RunSql(
      ctx, *db_,
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
      RunSql(ctx, *db_, "SELECT a.v FROM l AS a LEFT JOIN r AS b ON a.k = b.k "
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
      ctx, *db_,
      "SELECT a.v, b.w FROM l AS a RIGHT JOIN r AS b ON a.k = b.k;");
  ASSERT_EQ(right_rows.size(), 2U);
  EXPECT_TRUE(std::ranges::any_of(right_rows, [](const Row& row) {
    return row == Row({Value(20), Value(200)});
  }));
  EXPECT_TRUE(std::ranges::any_of(right_rows, [](const Row& row) {
    return row == Row({Value(), Value(300)});
  }));

  std::vector<Row> full_rows = RunSql(
      ctx, *db_,
      "SELECT a.v, b.w FROM l AS a FULL JOIN r AS b ON a.k = b.k;");
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
  EXPECT_TRUE(
      RunSql(ctx, *db_, "SELECT a FROM t WHERE a > 1 LIMIT 0 OFFSET 2;")
          .empty());
  EXPECT_TRUE(
      RunSql(ctx, *db_, "SELECT x.a FROM t AS x WHERE x.a = 2 LIMIT 0;")
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
  StatusOr<Executor> explain_select = engine.Prepare(ctx, "EXPLAIN SELECT a FROM t;");
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
  if (!prepared.HasValue()) { return rows;
}
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) { rows.push_back(row);
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
  ASSERT_EQ(RunPrepared(&engine, &ctx,
                        "SELECT v FROM pcache_ddl WHERE k = 5;")[0][0],
            Value(50));

  // Act (a) -- DROP + re-CREATE the same-named table with different data.
  // The fingerprint of the SELECT is unchanged, but the stale entry must be
  // discarded via the epoch check instead of replaying old page contents.
  ASSERT_TRUE(engine.Prepare(ctx, "DROP TABLE pcache_ddl;").HasValue());
  RunSql(ctx, *db_, "CREATE TABLE pcache_ddl (k INT64, v INT64);");
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (5, 555);");
  const uint64_t invalidations_before = PlanCacheStats().epoch_invalidations.load();
  std::vector<Row> recreated =
      RunPrepared(&engine, &ctx, "SELECT v FROM pcache_ddl WHERE k = 5;");

  // Assert (a) -- fresh compile observed the new table contents.
  ASSERT_EQ(recreated.size(), 1U);
  EXPECT_EQ(recreated[0][0], Value(555));
  EXPECT_GT(PlanCacheStats().epoch_invalidations.load(), invalidations_before);

  // Act (b) -- ANALYZE refreshes statistics and must invalidate too.
  RunSql(ctx, *db_, "INSERT INTO pcache_ddl VALUES (6, 60);");
  ASSERT_TRUE(engine.Prepare(ctx, "ANALYZE pcache_ddl;").HasValue());
  const uint64_t invalidations_mid = PlanCacheStats().epoch_invalidations.load();
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
  std::vector<Row> contents = RunPrepared(
      &engine, &ctx, "SELECT a, b FROM pcache_ins ORDER BY a;");
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

}  // namespace tinylamb
