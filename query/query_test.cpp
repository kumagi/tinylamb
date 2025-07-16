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
        ASSIGN_OR_RETURN(
            Table, table,
            db_->CreateTable(ctx, Schema(create_table.TableName(),
                                         create_table.Columns())));
        return {std::make_shared<ConstantExecutor>(
            Row({Value(0), Value("CREATE TABLE")}))};
      }
      case StatementType::kInsert: {
        auto& insert = dynamic_cast<InsertStatement&>(*stmt);
        QueryData query;
        query.from_.push_back(insert.TableName());
        for (const auto& row : insert.Values()) {
          for (const auto& val : row) {
            query.select_.emplace_back("", val);
          }
        }
        ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
        return plan->EmitExecutor(ctx);
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

TEST_F(QueryTest, DISABLED_SimpleSelect) {
  TransactionContext ctx = db_->BeginContext();
  {
    auto st = ExecuteQuery(ctx,
                           "CREATE TABLE t1 (c1 INT, c2 INT, c3 VARCHAR(10));");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
    auto exec = std::move(st.Value());
    Row result;
    ASSERT_TRUE(exec->Next(&result, nullptr));
  }
  {
    auto st =
        ExecuteQuery(ctx, "INSERT INTO t1 VALUES (1, 10, 'hello');");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
    auto exec = std::move(st.Value());
    Row result;
    ASSERT_TRUE(exec->Next(&result, nullptr));
    ASSERT_EQ(result[1], Value(1));
    ASSERT_FALSE(exec->Next(&result, nullptr));
  }
  {
    auto st =
        ExecuteQuery(ctx, "INSERT INTO t1 VALUES (2, 20, 'world');");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
    auto exec = std::move(st.Value());
    Row result;
    ASSERT_TRUE(exec->Next(&result, nullptr));
    ASSERT_EQ(result[1], Value(1));
    ASSERT_FALSE(exec->Next(&result, nullptr));
  }
  {
    auto st = ExecuteQuery(ctx, "SELECT * FROM t1 WHERE c1 = 1;");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
    auto exec = std::move(st.Value());
    Row result;
    ASSERT_TRUE(exec->Next(&result, nullptr));
    ASSERT_EQ(result[0], Value(1));
    ASSERT_EQ(result[1], Value(10));
    ASSERT_EQ(result[2], Value("hello"));
    ASSERT_FALSE(exec->Next(&result, nullptr));
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(QueryTest, DISABLED_SelectWithProjection) {
  TransactionContext ctx = db_->BeginContext();
  {
    auto st = ExecuteQuery(ctx,
                           "CREATE TABLE t1 (c1 INT, c2 INT, c3 VARCHAR(10));");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
  }
  {
    auto st =
        ExecuteQuery(ctx, "INSERT INTO t1 VALUES (1, 10, 'hello');");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
  }
  {
    auto st =
        ExecuteQuery(ctx, "INSERT INTO t1 VALUES (2, 20, 'world');");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
  }
  {
    auto st = ExecuteQuery(ctx, "SELECT c1, c3 FROM t1 WHERE c1 = 2;");
    ASSERT_EQ(st.GetStatus(), Status::kSuccess);
    auto exec = std::move(st.Value());
    Row result;
    ASSERT_TRUE(exec->Next(&result, nullptr));
    ASSERT_EQ(result[0], Value(2));
    ASSERT_EQ(result[1], Value("world"));
    ASSERT_FALSE(exec->Next(&result, nullptr));
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

}  // namespace tinylamb
