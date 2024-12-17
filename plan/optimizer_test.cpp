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

#include "plan/optimizer.hpp"

#include <iostream>
#include <memory>
#include <string>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "plan/plan.hpp"
#include "query/query_data.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
static const char* const kIndexName = "SampleIndex";

class OptimizerTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "optimizer_test-" + RandomString();
    Recover();
    TransactionContext ctx = rs_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc1", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar),
                                          Column("c3", ValueType::kDouble)})));
      for (int i = 0; i < 100; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value("c2-" + std::to_string(i)),
                            Value(i + 9.9)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc2", {Column("d1", ValueType::kInt64),
                                          Column("d2", ValueType::kDouble),
                                          Column("d3", ValueType::kVarChar),
                                          Column("d4", ValueType::kInt64)})));
      for (int i = 0; i < 200; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value(i + 0.2),
                            Value("d3-" + std::to_string(i % 10)), Value(16)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc3", {Column("e1", ValueType::kInt64),
                                          Column("e2", ValueType::kDouble)})));
      for (int i = 20; 0 < i; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 53.4)})).GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc4", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar)})));
      for (int i = 100; 0 < i; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(std::to_string(i % 4))}))
                .GetStatus());
      }
    }
    IndexSchema idx_sc(kIndexName, {1, 2});
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, "Sc1", IndexSchema("KeyIdx", {1, 2})));
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, "Sc1", IndexSchema("Sc1PK", {0})));
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, "Sc2", IndexSchema("Sc2PK", {0})));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, "Sc2",
        IndexSchema("NameIdx", {2, 3}, {0, 1}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, "Sc4", IndexSchema("Sc4_IDX", {1}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());

    auto stat_tx = rs_->BeginContext();
    rs_->RefreshStatistics(stat_tx, "Sc1");
    rs_->RefreshStatistics(stat_tx, "Sc2");
    rs_->RefreshStatistics(stat_tx, "Sc3");
    rs_->RefreshStatistics(stat_tx, "Sc4");
    ASSERT_SUCCESS(stat_tx.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  [[nodiscard]] Status DumpAll(const QueryData& qd) const {
    TransactionContext ctx = rs_->BeginContext();
    QueryData qd_resolved = qd;
    qd_resolved.Rewrite(ctx);
    std::cout << qd << "\n\n";
    ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(qd_resolved, ctx));
    Executor exec = plan->EmitExecutor(ctx);
    std::cout << " --- Logical Plan ---\n" << *plan << "\n";
    std::cout << "\n --- Physical Plan ---\n" << *exec << "\n";
    std::cout << "\n --- Output ---\n" << plan->GetSchema() << "\n";
    Row result;
    while (exec->Next(&result, nullptr)) {
      std::cout << result << "\n";
    }
    return Status::kSuccess;
  }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

TEST_F(OptimizerTest, Construct) {}

TEST_F(OptimizerTest, Simple) {
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"),
       NamedExpression("Column2Varchar", ColumnName("c2"))}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, IndexScan) {
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("c2-32"))),
      {NamedExpression("c1"), NamedExpression("score", ColumnName("c3"))}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, IndexOnlyScan) {
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("c2-32"))),
      {NamedExpression("name", ColumnName("c2")),
       NamedExpression("score", ColumnName("c3"))}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, IndexOnlyScanInclude) {
  QueryData qd{{"Sc2"},
               BinaryExpressionExp(
                   BinaryExpressionExp(ColumnValueExp("d3"),
                                       BinaryOperation::kGreaterThanEquals,
                                       ConstantValueExp(Value("d3-3"))),
                   BinaryOperation::kAnd,
                   BinaryExpressionExp(ColumnValueExp("d3"),
                                       BinaryOperation::kLessThanEquals,
                                       ConstantValueExp(Value("d3-5")))),
               {NamedExpression("key", ColumnName("d1")),
                NamedExpression("score", ColumnName("d2")),
                NamedExpression("name", ColumnName("d3")),
                NamedExpression("const", ColumnName("d4"))}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, Join) {
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("d1")),
      {NamedExpression("c2"), NamedExpression("d1"), NamedExpression("d3")}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, IndexScanJoin) {
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                              ConstantValueExp(Value("c2-4")))),
      {NamedExpression("c2"), NamedExpression("d1"), NamedExpression("d3")}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, ThreeJoin) {
  QueryData qd{
      {"Sc1", "Sc2", "Sc3"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("d1"), BinaryOperation::kEquals,
                              ColumnValueExp("e1"))),

      {NamedExpression("Sc1-c2", ColumnName("c2")),
       NamedExpression("Sc2-d1", ColumnName("d1")),
       NamedExpression("Sc3-e2", ColumnName("e2")),
       NamedExpression(
           "e1+100",
           BinaryExpressionExp(ConstantValueExp(Value(100)),
                               BinaryOperation::kAdd, ColumnValueExp("e1")))}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, JoinWhere) {
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(2)))),
      {NamedExpression("c1"), NamedExpression("c2"), NamedExpression("d1"),
       NamedExpression("d2"), NamedExpression("d3")}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, SameNameColumn) {
  QueryData qd{
      {"Sc1", "Sc4"},
      BinaryExpressionExp(BinaryExpressionExp(ColumnValueExp("Sc1.c1"),
                                              BinaryOperation::kEquals,
                                              ColumnValueExp("Sc4.c1")),
                          BinaryOperation::kAnd,
                          BinaryExpressionExp(ColumnValueExp("Sc4.c1"),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value(2)))),
      {NamedExpression("Sc1.c1"), NamedExpression("Sc1.c2"),
       NamedExpression("c3"), NamedExpression("SC4.c1"),
       NamedExpression("Sc4.c2")}};
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, Asterisk) {
  QueryData qd{
      {"Sc1", "Sc4"},
      BinaryExpressionExp(BinaryExpressionExp(ColumnValueExp("Sc1.c1"),
                                              BinaryOperation::kEquals,
                                              ColumnValueExp("Sc4.c1")),
                          BinaryOperation::kAnd,
                          BinaryExpressionExp(ColumnValueExp("Sc4.c1"),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value(2)))),
      {NamedExpression("*")}};
  ASSERT_SUCCESS(DumpAll(qd));
}
}  // namespace tinylamb
