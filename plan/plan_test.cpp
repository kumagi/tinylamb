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

#include "plan/plan.hpp"

#include <iostream>
#include <memory>

#include "aggregation_plan.hpp"
#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "full_scan_plan.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "product_plan.hpp"
#include "projection_plan.hpp"
#include "selection_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class PlanTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "plan_test-" + RandomString();
    Recover();
    TransactionContext ctx = rs_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc1", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar),
                                          Column("c3", ValueType::kDouble)})));
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(12), Value("hello"), Value(2.3)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(10), Value("world"), Value(4.5)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(52), Value("ought"), Value(5.3)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(242), Value("arise"), Value(6.0)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(431), Value("vivid"), Value(2.03)}))
              .GetStatus());
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(100), Value("aster"), Value(1.2)}))
              .GetStatus());
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc2", {Column("d1", ValueType::kInt64),
                                          Column("d2", ValueType::kDouble),
                                          Column("d3", ValueType::kVarChar),
                                          Column("d4", ValueType::kInt64)})));
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(52), Value(53.4),
                                               Value("ou"), Value(16)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(242), Value(6.1),
                                               Value("ai"), Value(32)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(12), Value(5.3),
                                               Value("heo"), Value(4)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(10), Value(6.5),
                                               Value("wld"), Value(8)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(33), Value(2.5),
                                               Value("vid"), Value(64)}))
                         .GetStatus());
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(1), Value(7.2),
                                               Value("aer"), Value(128)}))
                         .GetStatus());
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc3", {Column("e1", ValueType::kInt64),
                                          Column("e2", ValueType::kDouble)})));
      ASSERT_SUCCESS(tbl.Insert(ctx.txn_, Row({Value(52), Value(53.4),
                                               Value("ou"), Value(16)}))
                         .GetStatus());
    }
    rs_->CreateIndex(ctx, "Sc2", IndexSchema("Sc2PK", {0}));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  void DumpAll(const Plan& plan) const {
    DumpLog(*plan, 0);
    TransactionContext ctx = rs_->BeginContext();
    Executor scan = plan->EmitExecutor(ctx);
    Row result;
    LOG(INFO) << plan->GetSchema();
    while (scan->Next(&result, nullptr)) {
      LOG(INFO) << result;
    }
  }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

TEST_F(PlanTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(PlanTest, ScanPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));

  // Act -- construct FullScanPlan and dump via LOG(INFO)
  Plan fs(new FullScanPlan(*tbl, ts));
  DumpAll(fs);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, ProjectPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));

  // Act -- construct ProjectionPlan projecting c1, dump via LOG(INFO)
  Plan pp(new ProjectionPlan(std::make_shared<FullScanPlan>(*tbl, ts),
                              {NamedExpression("c1")}));
  DumpAll(pp);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, SelectionPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));

  // Act -- construct SelectionPlan with filter c1 >= 100, dump via LOG(INFO)
  Expression exp = BinaryExpressionExp(ColumnValueExp("c1"),
                                       BinaryOperation::kGreaterThanEquals,
                                       ConstantValueExp(Value(100)));
  Plan sp(new SelectionPlan(std::make_shared<FullScanPlan>(*tbl, ts), exp, ts));
  DumpAll(sp);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, HashJoinPlan) {
  // Arrange -- begin context, get Sc1 and Sc2 tables, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));

  // Act -- construct ProductPlan (hash join on Sc1.c1 = Sc2.d1), dump via LOG(INFO)
  Plan prop(new ProductPlan(
      std::make_shared<FullScanPlan>(*tbl1, ts), {ColumnName("Sc1.c1")},
      std::make_shared<FullScanPlan>(*tbl2, ts), {ColumnName("Sc2.d1")}));
  DumpAll(prop);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, IndexJoinPlan) {
  // Arrange -- begin context, get Sc1 and Sc2 tables, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));

  // Act -- construct ProductPlan (index join on Sc2PK index), dump via LOG(INFO)
  Plan prop(new ProductPlan(std::make_shared<FullScanPlan>(*tbl1, ts),
                             {ColumnName("Sc1.c1")}, *tbl2, tbl2->GetIndex(0),
                             {ColumnName("Sc2.d1")}, ts));
  DumpAll(prop);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, ProductPlanCrossJoin) {
  // Arrange -- begin context, get Sc1 and Sc2 tables, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));

  // Act -- construct ProductPlan (cross join), dump via LOG(INFO)
  Plan prop(new ProductPlan(std::make_shared<FullScanPlan>(*tbl1, ts),
                             std::make_shared<FullScanPlan>(*tbl2, ts)));
  DumpAll(prop);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, UnaryPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));

  // Act -- construct SelectionPlan with IS NULL filter on c1, dump via LOG(INFO)
  Expression exp =
      UnaryExpressionExp(ColumnValueExp("c1"), UnaryOperation::kIsNull);
  Plan sp(new SelectionPlan(std::make_shared<FullScanPlan>(*tbl, ts), exp, ts));
  DumpAll(sp);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, AggregationPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));

  // Act -- construct AggregationPlan with count/sum/avg/min/max on c1/c3, dump via LOG(INFO)
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("c1"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("c3"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("c3"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("c3"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("c3")))};
  Plan ap(new AggregationPlan(std::make_shared<FullScanPlan>(*tbl, ts),
                              std::move(aggregates)));
  DumpAll(ap);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

}  // namespace tinylamb
