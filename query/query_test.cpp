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

#include <memory>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/insert.hpp"
#include "gtest/gtest.h"
#include "parser/parser.hpp"
#include "parser/tokenizer.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/selection_plan.hpp"
#include "query/query_data.hpp"
#include "query/sql_engine.hpp"
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
                             std::unique_ptr<Statement> stmt) {
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
                                  const std::string& sql) {
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
  if (!prepared.HasValue()) return rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) rows.push_back(row);
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
  ASSERT_TRUE(engine.LastStatementType().has_value());
  EXPECT_EQ(engine.LastStatementType().value(), StatementType::kSelect);
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

}  // namespace tinylamb
