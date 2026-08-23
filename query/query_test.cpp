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
#include "executor/executor_base.hpp"
#include "executor/insert.hpp"
#include "gtest/gtest.h"
#include "parser/parser.hpp"
#include "parser/token.hpp"
#include "parser/tokenizer.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
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
  // Phase 8: LEFT JOIN keeps the relational path (null-padding is not yet
  // modeled in the Cascades memo) and stays correct end-to-end.
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

}  // namespace tinylamb
