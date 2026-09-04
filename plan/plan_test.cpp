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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "aggregation_plan.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "distinct_plan.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "full_scan_plan.hpp"
#include "gtest/gtest.h"
#include "index/index.hpp"
#include "index_only_scan_plan.hpp"
#include "merge_join_plan.hpp"
#include "page/page_manager.hpp"
#include "product_plan.hpp"
#include "projection_plan.hpp"
#include "selection_plan.hpp"
#include "set_operation_plan.hpp"
#include "sort_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "topn_plan.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"
#include "values_plan.hpp"

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

TEST_F(PlanTest, ValuesPlanEmitsTypedMultiColumnRowsAndValidatesWidth) {
  const Schema schema("values", {Column("id", ValueType::kInt64),
                                 Column("name", ValueType::kVarChar)});
  ValuesPlan values(
      schema, {Row({Value(1), Value("one")}), Row({Value(2), Value("two")})});
  EXPECT_EQ(values.AccessRowCount(), 2U);
  EXPECT_EQ(values.GetSchema().ColumnCount(), 2U);
  EXPECT_EQ(values.ToString(), "Values (rows=2)");

  TransactionContext context = rs_->BeginContext();
  Executor executor = values.EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1), Value("one")}));
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2), Value("two")}));
  EXPECT_FALSE(executor->Next(&row, nullptr));

  EXPECT_THROW(ValuesPlan(schema, {Row({Value(1)})}), std::invalid_argument);
}

TEST_F(PlanTest, SetOperationPlanPublishesNumericCommonSchema) {
  Plan left = std::make_shared<ValuesPlan>(
      Schema("left", {Column("v", ValueType::kInt64)}),
      std::vector<Row>{Row({Value(1)})});
  Plan right = std::make_shared<ValuesPlan>(
      Schema("right", {Column("v", ValueType::kDouble)}),
      std::vector<Row>{Row({Value(2.5)})});
  SetOperationPlan plan({std::move(left), std::move(right)},
                        SetOperationKind::kUnionAll);
  ASSERT_EQ(plan.GetSchema().ColumnCount(), 1U);
  EXPECT_EQ(plan.GetSchema().GetColumn(0).Type(), ValueType::kDouble);

  TransactionContext context = rs_->BeginContext();
  Executor executor = plan.EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1.0)}));
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2.5)}));
  EXPECT_FALSE(executor->Next(&row, nullptr));
}

TEST_F(PlanTest, ScanPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();

  // Act -- construct FullScanPlan and dump via LOG(INFO)
  Plan fs(new FullScanPlan(*tbl, ts));
  DumpAll(fs);

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(PlanTest, ProjectPlan) {
  // Arrange -- begin context, get Sc1 table, empty statistics
  TableStatistics ts((Schema()));
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();

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
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();

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
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();

  // Act -- construct ProductPlan (hash join on Sc1.c1 = Sc2.d1), dump via
  // LOG(INFO)
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
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();

  // Act -- construct ProductPlan (index join on Sc2PK index), dump via
  // LOG(INFO)
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
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();

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
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();

  // Act -- construct SelectionPlan with IS NULL filter on c1, dump via
  // LOG(INFO)
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
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();

  // Act -- construct AggregationPlan with count/sum/avg/min/max on c1/c3, dump
  // via LOG(INFO)
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

TEST_F(PlanTest, AggregatePhysicalStrategiesHaveDistinctPlanContracts) {
  auto ctx = rs_->BeginContext();
  const auto table_or = ctx.GetTable("Sc1");
  ASSERT_TRUE(table_or.HasValue());
  const auto stats_or = ctx.GetStats("Sc1");
  ASSERT_TRUE(stats_or.HasValue());
  const Plan child =
      std::make_shared<FullScanPlan>(*table_or.Value(), *stats_or.Value());
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "n",
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("c1")))};

  const HashAggregatePlan hash(child, aggregates);
  const SortAggregatePlan sort(child, aggregates);
  const StreamAggregatePlan stream(child, aggregates);
  EXPECT_EQ(hash.Strategy(), AggregationStrategy::kHash);
  EXPECT_EQ(sort.Strategy(), AggregationStrategy::kSort);
  EXPECT_EQ(stream.Strategy(), AggregationStrategy::kStream);
  EXPECT_NE(hash.ToString(), sort.ToString());
  EXPECT_NE(sort.ToString(), stream.ToString());
}

TEST_F(PlanTest, FullScanAccessors) {
  // Arrange -- begin context, get Sc1 table and its real statistics
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc1"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();

  // Act -- construct FullScanPlan and query its accessors
  Plan fs(new FullScanPlan(*tbl, *ts));

  // Assert -- schema, row counts, stats, and dump output are consistent
  EXPECT_EQ(fs->GetSchema().Name(), "Sc1");
  EXPECT_EQ(fs->AccessRowCount(), ts->Rows());
  EXPECT_EQ(fs->EmitRowCount(), ts->Rows());
  EXPECT_EQ(fs->GetStats().Rows(), ts->Rows());
  EXPECT_NE(fs->ScanSource(), nullptr);
  EXPECT_NE(fs->ToString().find("FullScan: Sc1"), std::string::npos);
  std::ostringstream oss;
  fs->Dump(oss, 0);
  EXPECT_NE(oss.str().find("FullScan: Sc1"), std::string::npos);
}

TEST_F(PlanTest, FullScanWithRowLimit_ReportsCappedCostAndCardinality) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Sc1"));

  FullScanPlan scan(*table, *stats, 2);
  EXPECT_EQ(scan.MaxRows(), 2U);
  const size_t expected = stats->Rows() < 2 ? stats->Rows() : 2;
  EXPECT_EQ(scan.AccessRowCount(), expected);
  EXPECT_EQ(scan.EmitRowCount(), expected);
  EXPECT_NE(scan.ToString().find("max rows: 2"), std::string::npos);
}

TEST_F(PlanTest, ProjectionExpressionColumn) {
  // Arrange -- begin context, get Sc1 table
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  TableStatistics ts((Schema()));

  // Act -- project a synthetic expression (non-column) and a named column
  std::vector<NamedExpression> columns;
  columns.emplace_back("", ConstantValueExp(Value(1)));
  columns.emplace_back("c1");
  Plan pp(new ProjectionPlan(std::make_shared<FullScanPlan>(*tbl, ts),
                             std::move(columns)));

  // Assert -- the synthetic column is auto-named "$col<index>"
  const Schema& sc = pp->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 2);
  EXPECT_EQ(sc.GetColumn(0).Name().name, "$col0");
  EXPECT_EQ(sc.GetColumn(1).Name().name, "c1");
}

TEST_F(PlanTest, ProjectionToString) {
  // Arrange -- begin context, get Sc1 table
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  TableStatistics ts((Schema()));

  // Act -- construct ProjectionPlan and render it
  Plan pp(new ProjectionPlan(std::make_shared<FullScanPlan>(*tbl, ts),
                             {NamedExpression("c1", ColumnValueExp("c1"))}));
  std::ostringstream oss;
  pp->Dump(oss, 0);

  // Assert -- both the tree dump and the string form carry the projection
  EXPECT_NE(pp->ToString().find("Project: {c1}"), std::string::npos);
  EXPECT_NE(oss.str().find("Project: {c1}"), std::string::npos);
}

TEST_F(PlanTest, SelectionToString) {
  // Arrange -- begin context, get Sc1 table
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  TableStatistics ts((Schema()));

  // Act -- construct SelectionPlan with a >= filter and render it
  Expression exp = BinaryExpressionExp(ColumnValueExp("c1"),
                                       BinaryOperation::kGreaterThanEquals,
                                       ConstantValueExp(Value(100)));
  Plan sp(new SelectionPlan(std::make_shared<FullScanPlan>(*tbl, ts), exp, ts));
  std::ostringstream oss;
  sp->Dump(oss, 0);

  // Assert -- the predicate and cost are rendered
  EXPECT_NE(sp->ToString().find("Select: ["), std::string::npos);
  EXPECT_NE(sp->ToString().find(">="), std::string::npos);
  EXPECT_EQ(sp->AccessRowCount(), ts.Rows());
  EXPECT_NE(oss.str().find("Select: ["), std::string::npos);
}

TEST_F(PlanTest, AggregationAccessors) {
  // Arrange -- begin context, get Sc1 table
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  TableStatistics ts((Schema()));
  auto child = std::make_shared<FullScanPlan>(*tbl, ts);

  // Act -- construct AggregationPlan with typed aggregates over c1 (int) and
  // c3 (double)
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("c1"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("c3"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("c3"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("c1"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("c1")))};
  Plan ap(new AggregationPlan(child, std::move(aggregates)));

  // Assert -- per-aggregate result types and delegation of accessors
  const Schema& sc = ap->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 5);
  EXPECT_EQ(sc.GetColumn(0).Type(), ValueType::kInt64);
  EXPECT_EQ(sc.GetColumn(1).Type(), ValueType::kDouble);
  EXPECT_EQ(sc.GetColumn(2).Type(), ValueType::kDouble);
  EXPECT_EQ(sc.GetColumn(3).Type(), ValueType::kInt64);
  EXPECT_EQ(sc.GetColumn(4).Type(), ValueType::kInt64);
  EXPECT_EQ(ap->AccessRowCount(), child->AccessRowCount());
  EXPECT_EQ(ap->EmitRowCount(), 1);
  EXPECT_EQ(ap->ScanSource(), child->ScanSource());
  EXPECT_EQ(ap->GetStats().Rows(), child->GetStats().Rows());
  EXPECT_NE(ap->ToString().find("HashAggregate {"), std::string::npos);
  std::ostringstream oss;
  ap->Dump(oss, 0);
  EXPECT_NE(oss.str().find("HashAggregate {"), std::string::npos);
}

TEST_F(PlanTest, SortPlanOrdersRowsAndReportsOrdering) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Sc1"));
  Plan child(new FullScanPlan(*table, *stats));
  Plan sorted(new SortPlan(child, {SortKey{ColumnValueExp(ColumnName("Sc1.c1")),
                                           true, std::nullopt}}));

  EXPECT_TRUE(
      sorted->IsOrderedBy({ColumnValueExp(ColumnName("Sc1.c1"))}, {true}));
  EXPECT_FALSE(
      sorted->IsOrderedBy({ColumnValueExp(ColumnName("Sc1.c1"))}, {false}));
  EXPECT_NE(sorted->ToString().find("Sort"), std::string::npos);

  Executor executor = sorted->EmitExecutor(ctx);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(10));
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(12));
}

TEST_F(PlanTest, SortAndTopNPlansReportSortedPrefixes) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Sc1"));
  Plan scan = std::make_shared<FullScanPlan>(*table, *stats);
  const Expression first = ColumnValueExp(ColumnName("Sc1.c1"));
  const Expression second = ColumnValueExp(ColumnName("Sc1.c2"));
  Plan sorted = std::make_shared<SortPlan>(
      scan, std::vector<SortKey>{{first, true, std::nullopt},
                                 {second, true, std::nullopt}});
  Plan topn = std::make_shared<TopNPlan>(
      scan,
      std::vector<TopNKey>{{first, true, std::nullopt},
                           {second, true, std::nullopt}},
      3, 0);

  for (const Plan& plan : {sorted, topn}) {
    EXPECT_TRUE(plan->IsOrderedBy({first}, {true}));
    EXPECT_TRUE(plan->IsOrderedBy({first, second}, {true, true}));
    EXPECT_FALSE(plan->IsOrderedBy({first}, {false}));
  }
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(PlanTest, IsOrderedByComparesNullPlacement) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Sc1"));
  const Expression key = ColumnValueExp(ColumnName("Sc1.c1"));
  // Explicit NULLS LAST on an ascending key: only a matching request (or an
  // explicitly matching one) reports ordered.
  Plan sorted_last =
      std::make_shared<SortPlan>(std::make_shared<FullScanPlan>(*table, *stats),
                                 std::vector<SortKey>{{key, true, false}});
  EXPECT_TRUE(
      sorted_last->IsOrderedBy({key}, {true}, {std::optional<bool>(false)}));
  EXPECT_FALSE(
      sorted_last->IsOrderedBy({key}, {true}, {std::optional<bool>(true)}));
  // A bare request resolves to the engine default (NULLS FIRST for ASC),
  // which this plan does not provide.
  EXPECT_FALSE(sorted_last->IsOrderedBy({key}, {true}, {}));
  // Defaulted keys keep the legacy behavior: bare and default requests match.
  Plan sorted_default = std::make_shared<SortPlan>(
      std::make_shared<FullScanPlan>(*table, *stats),
      std::vector<SortKey>{{key, true, std::nullopt}});
  EXPECT_TRUE(sorted_default->IsOrderedBy({key}, {true}, {}));
  EXPECT_TRUE(
      sorted_default->IsOrderedBy({key}, {true}, {std::optional<bool>(true)}));
  EXPECT_FALSE(
      sorted_default->IsOrderedBy({key}, {true}, {std::optional<bool>(false)}));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(PlanTest, ProjectionTranslatesAliasedOrderKeysToChildExpressions) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Sc1"));
  const Expression source_key = ColumnValueExp(ColumnName("Sc1.c1"));
  Plan sorted = std::make_shared<SortPlan>(
      std::make_shared<FullScanPlan>(*table, *stats),
      std::vector<SortKey>{{source_key, true, std::nullopt}});
  Plan projected = std::make_shared<ProjectionPlan>(
      sorted, std::vector<NamedExpression>{NamedExpression(
                  "$order0", ColumnValueExp(ColumnName("Sc1.c1")))});

  EXPECT_TRUE(
      projected->IsOrderedBy({ColumnValueExp(ColumnName("$order0"))}, {true}));
  EXPECT_FALSE(
      projected->IsOrderedBy({ColumnValueExp(ColumnName("$order0"))}, {false}));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(PlanTest, DistinctPlanUsesHashExecutorAndPreservesOrderingMetadata) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        ctx.GetStats("Sc1"));
  Plan child(new FullScanPlan(*table, *stats));
  Plan distinct(new DistinctPlan(child));

  EXPECT_EQ(distinct->EmitRowCount(), child->EmitRowCount());
  EXPECT_FALSE(
      distinct->IsOrderedBy({ColumnValueExp(ColumnName("Sc1.c1"))}, {true}));
  EXPECT_EQ(distinct->ToString(), "HashDistinct");

  Executor executor = distinct->EmitExecutor(ctx);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    ++count;
  }
  EXPECT_EQ(count, 6U);
}

TEST_F(PlanTest, ProductCrossJoinAccessors) {
  // Arrange -- begin context, get Sc1 and Sc2 tables
  auto ctx = rs_->BeginContext();
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();
  TableStatistics ts((Schema()));
  auto left = std::make_shared<FullScanPlan>(*tbl1, ts);
  auto right = std::make_shared<FullScanPlan>(*tbl2, ts);

  // Act -- construct a cross-join ProductPlan and query its accessors
  Plan prop(new ProductPlan(left, right));

  // Assert -- schema is the concatenation and costs reflect the cross join
  EXPECT_EQ(prop->GetSchema().ColumnCount(), 7);
  EXPECT_NE(prop->ToString().find("Cross Join"), std::string::npos);
  EXPECT_EQ(prop->EmitRowCount(), left->EmitRowCount() * right->EmitRowCount());
  EXPECT_EQ(prop->AccessRowCount(),
            left->AccessRowCount() +
                (1 + left->EmitRowCount() * right->AccessRowCount()));
  EXPECT_EQ(prop->ScanSource(), nullptr);
  std::ostringstream oss;
  prop->Dump(oss, 0);
  EXPECT_NE(oss.str().find("Cross Join"), std::string::npos);
}

TEST_F(PlanTest, ProductHashJoinAccessors) {
  // Arrange -- begin context, get Sc1 and Sc2 tables
  auto ctx = rs_->BeginContext();
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();
  TableStatistics ts((Schema()));
  auto left = std::make_shared<FullScanPlan>(*tbl1, ts);
  auto right = std::make_shared<FullScanPlan>(*tbl2, ts);

  // Act -- construct a hash-join ProductPlan and query its accessors
  Plan prop(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                            {ColumnName("Sc2.d1")}));

  // Assert -- costs and rendered join keys are correct
  EXPECT_EQ(prop->GetSchema().ColumnCount(), 7);
  EXPECT_NE(prop->ToString().find("Hash Join"), std::string::npos);
  EXPECT_NE(prop->ToString().find("left:{Sc1.c1}"), std::string::npos);
  EXPECT_NE(prop->ToString().find("right:{Sc2.d1}"), std::string::npos);
  EXPECT_EQ(prop->EmitRowCount(),
            std::min(left->EmitRowCount(), right->EmitRowCount()));
  EXPECT_EQ(prop->AccessRowCount(),
            left->AccessRowCount() + right->AccessRowCount());
  std::ostringstream oss;
  prop->Dump(oss, 0);
  EXPECT_NE(oss.str().find("right:{Sc2.d1}"), std::string::npos);
}

TEST_F(PlanTest, MergeJoinPlanCarriesSortedKeyContractAndOutputSchema) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));
  TableStatistics ts((Schema()));
  auto left_scan = std::make_shared<FullScanPlan>(*tbl1, ts);
  auto left = std::make_shared<SortPlan>(
      left_scan, std::vector<SortKey>{{ColumnValueExp(ColumnName("Sc1.c1")),
                                       true, std::nullopt}});
  auto right = std::make_shared<FullScanPlan>(*tbl2, ts);

  Plan plan(new MergeJoinPlan(left, {ColumnName("Sc1.c1")}, right,
                              {ColumnName("Sc2.d1")}));
  EXPECT_EQ(plan->GetSchema().ColumnCount(), 7U);
  EXPECT_EQ(plan->EmitRowCount(),
            std::min(left->EmitRowCount(), right->EmitRowCount()));
  EXPECT_TRUE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc1.c1"))}, {true}));
  EXPECT_FALSE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {true}));
  EXPECT_NE(plan->ToString().find("MergeJoin"), std::string::npos);
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("keys=1"), std::string::npos);
}

TEST_F(PlanTest, MergeSemiAndAntiJoinPlansExposeProbeSchema) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        ctx.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        ctx.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        ctx.GetStats("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);

  MergeJoinPlan semi(left, {ColumnName("Sc1.c1")}, right,
                     {ColumnName("Sc2.d1")}, SemiJoinKind());
  MergeJoinPlan anti(left, {ColumnName("Sc1.c1")}, right,
                     {ColumnName("Sc2.d1")}, AntiJoinKind());
  EXPECT_EQ(semi.GetSchema().ColumnCount(), left->GetSchema().ColumnCount());
  EXPECT_EQ(anti.GetSchema().ColumnCount(), left->GetSchema().ColumnCount());
  EXPECT_EQ(semi.Kind(), SemiJoinKind());
  EXPECT_EQ(anti.Kind(), AntiJoinKind());
  EXPECT_NE(semi.ToString().find("MergeSemiJoin"), std::string::npos);
  EXPECT_NE(anti.ToString().find("MergeAntiJoin"), std::string::npos);
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(PlanTest, ProductSemiAndAntiJoinPreserveProbeSchemaAndCardinalityBound) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, ts1,
                        ctx.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, ts2,
                        ctx.GetStats("Sc2"));
  auto left = std::make_shared<FullScanPlan>(*tbl1, *ts1);
  auto right = std::make_shared<FullScanPlan>(*tbl2, *ts2);

  Plan semi(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                            {ColumnName("Sc2.d1")}, SemiJoinKind()));
  Plan anti(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                            {ColumnName("Sc2.d1")}, AntiJoinKind()));

  EXPECT_EQ(semi->GetSchema().ColumnCount(), left->GetSchema().ColumnCount());
  EXPECT_EQ(anti->GetSchema().ColumnCount(), left->GetSchema().ColumnCount());
  EXPECT_EQ(semi->EmitRowCount(),
            std::min(left->EmitRowCount(), right->EmitRowCount()));
  EXPECT_EQ(anti->EmitRowCount(), left->EmitRowCount());
  EXPECT_NE(semi->ToString().find("Semi Join"), std::string::npos);
  EXPECT_NE(anti->ToString().find("Anti Join"), std::string::npos);
}

TEST_F(PlanTest, ProductOuterJoinPreservesBothSchemasAndOuterCardinalityBound) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, ts1,
                        ctx.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, ts2,
                        ctx.GetStats("Sc2"));
  auto left = std::make_shared<FullScanPlan>(*tbl1, *ts1);
  auto right = std::make_shared<FullScanPlan>(*tbl2, *ts2);

  Plan left_outer(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                                  {ColumnName("Sc2.d1")}, LeftOuterJoinKind()));
  Plan right_outer(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                                   {ColumnName("Sc2.d1")},
                                   RightOuterJoinKind()));
  Plan full_outer(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                                  {ColumnName("Sc2.d1")}, FullOuterJoinKind()));

  for (const Plan& plan : {left_outer, right_outer, full_outer}) {
    EXPECT_EQ(
        plan->GetSchema().ColumnCount(),
        left->GetSchema().ColumnCount() + right->GetSchema().ColumnCount());
    EXPECT_EQ(plan->EmitRowCount(),
              std::max(left->EmitRowCount(), right->EmitRowCount()));
  }
  EXPECT_NE(left_outer->ToString().find("Left Outer Join"), std::string::npos);
  EXPECT_NE(right_outer->ToString().find("Right Outer Join"),
            std::string::npos);
  EXPECT_NE(full_outer->ToString().find("Full Outer Join"), std::string::npos);
}

TEST_F(PlanTest, ProductHybridHashJoinPreferredUnderTinyBudget) {
  auto ctx = rs_->BeginContext();
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();
  // Non-zero row estimates so PreferHybridHashJoin sees a real build footprint.
  TableStatistics left_ts((Schema()));
  TableStatistics right_ts((Schema()));
  left_ts = left_ts.ScaleToRows(10'000);
  right_ts = right_ts.ScaleToRows(10'000);
  auto left = std::make_shared<FullScanPlan>(*tbl1, left_ts);
  auto right = std::make_shared<FullScanPlan>(*tbl2, right_ts);

  QueryMemoryBudget::Global().ResetForTest(static_cast<size_t>(64) *
                                           static_cast<size_t>(1024));
  Plan in_memory(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                                 {ColumnName("Sc2.d1")},
                                 HashJoinMode::kInMemory));
  Plan hybrid(new ProductPlan(left, {ColumnName("Sc1.c1")}, right,
                              {ColumnName("Sc2.d1")}, HashJoinMode::kHybrid));
  EXPECT_GT(in_memory->AccessRowCount(), hybrid->AccessRowCount());
  EXPECT_NE(hybrid->ToString().find("Hybrid Hash Join"), std::string::npos);
  QueryMemoryBudget::Global().ResetForTest(0);
}

TEST_F(PlanTest, ProductIndexJoinAccessors) {
  // Arrange -- begin context, get Sc1 and Sc2 tables and the Sc2PK index
  auto ctx = rs_->BeginContext();
  const auto tbl1_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl1_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl1 = tbl1_or.Value();
  const auto tbl2_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl2 = tbl2_or.Value();
  const auto ts2_or = (ctx.GetStats("Sc2"));
  ASSERT_EQ(ts2_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts2 = ts2_or.Value();
  TableStatistics ts((Schema()));
  auto left = std::make_shared<FullScanPlan>(*tbl1, ts);

  // Act -- construct an index-join ProductPlan and query its accessors
  Plan prop(new ProductPlan(left, {ColumnName("Sc1.c1")}, *tbl2,
                            tbl2->GetIndex(0), {ColumnName("Sc2.d1")}, *ts2));

  // Assert -- index join cost model and rendered keys are correct
  EXPECT_EQ(prop->EmitRowCount(), std::min(left->EmitRowCount(), ts2->Rows()));
  EXPECT_EQ(prop->AccessRowCount(), left->AccessRowCount() * 3);
  EXPECT_NE(prop->ToString().find("left:{Sc1.c1}"), std::string::npos);
  EXPECT_NE(prop->ToString().find("right:{Sc2.d1}"), std::string::npos);
  std::ostringstream oss;
  prop->Dump(oss, 0);
  EXPECT_NE(oss.str().find("left:{Sc1.c1}"), std::string::npos);
}

TEST_F(PlanTest, IndexOnlyScanPlanBasic) {
  // Arrange -- begin context, get Sc2 table and its Sc2PK index
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();

  // Act -- construct an unbounded index-only scan
  Plan plan(new IndexOnlyScanPlan(*tbl, tbl->GetIndex(0), *ts, {}, {}, true,
                                  nullptr));

  // Assert -- the output schema covers the indexed key and costs follow stats
  EXPECT_EQ(plan->GetSchema().ColumnCount(), 1);
  EXPECT_EQ(plan->GetSchema().GetColumn(0).Name().name, "d1");
  EXPECT_NE(plan->ToString().find("IndexOnlyScan: Sc2"), std::string::npos);
  EXPECT_NE(plan->ToString().find("Sc2PK"), std::string::npos);
  EXPECT_EQ(plan->EmitRowCount(), ts->Rows());
  EXPECT_EQ(plan->AccessRowCount(), ts->Rows());
  std::ostringstream oss;
  plan->Dump(oss, 0);
  EXPECT_NE(oss.str().find("IndexOnlyScan: Sc2"), std::string::npos);
}

TEST_F(PlanTest, IndexOnlyScanPlanUniqueLookup) {
  // Arrange -- begin context, get Sc2 table and its unique Sc2PK index
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();

  // Act -- construct a point lookup on the unique key
  Plan plan(new IndexOnlyScanPlan(*tbl, tbl->GetIndex(0), *ts, {Value(52)},
                                  {Value(52)}, true, nullptr));

  // Assert -- a unique full-key lookup emits exactly one row
  EXPECT_EQ(plan->EmitRowCount(), 1);
  EXPECT_EQ(plan->AccessRowCount(), 1);
}

TEST_F(PlanTest, IndexOnlyScanPlanOrderedBy) {
  // Arrange -- begin context, get Sc2 table and its Sc2PK index
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();

  // Act -- construct an ascending index-only scan advertising its order
  std::vector<ColumnName> provided{ColumnName("Sc2.d1")};
  Plan plan(new IndexOnlyScanPlan(*tbl, tbl->GetIndex(0), *ts, {}, {}, true,
                                  nullptr, provided));

  // Assert -- order matches only for the same column and direction
  EXPECT_TRUE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {true}));
  EXPECT_FALSE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {false}));
  EXPECT_FALSE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d2"))}, {true}));
  EXPECT_FALSE(plan->IsOrderedBy({}, {true}));
  EXPECT_FALSE(plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {}));
}

TEST_F(PlanTest, IndexOnlyScanPlanHistoricalRead) {
  // Arrange -- begin a writer that inserts an uncommitted row into Sc2
  TransactionContext writer = rs_->BeginContext();
  const auto tbl_or = (writer.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  ASSERT_SUCCESS(tbl->Insert(writer.txn_, Row({Value(999), Value(1.0),
                                               Value("new"), Value(8)}))
                     .GetStatus());

  // Act -- a later transaction must not see the uncommitted index entry.
  // Pending writers do not force a FullScan plan gate.
  TransactionContext reader = rs_->BeginContext();
  ASSERT_FALSE(reader.txn_.RequiresHistoricalRead());
  const auto ts_or = (reader.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  Expression where = BinaryExpressionExp(ColumnValueExp("d1"),
                                         BinaryOperation::kGreaterThanEquals,
                                         ConstantValueExp(Value(0)));
  Plan plan(
      new IndexOnlyScanPlan(*tbl, tbl->GetIndex(0), *ts, {}, {}, true, where));
  Executor scan = plan->EmitExecutor(reader);
  Row result;
  size_t count = 0;
  while (scan->Next(&result, nullptr)) {
    ++count;
  }

  // Assert -- only the 6 committed rows are visible to the reader
  ASSERT_EQ(count, 6);
  reader.txn_.Abort();
  writer.txn_.Abort();
}

TEST_F(PlanTest, ProjectionExpressionCtorAccessors) {
  // Arrange -- begin context, get Sc1 table and its real statistics
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc1"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  auto child = std::make_shared<FullScanPlan>(*tbl, *ts);

  // Act -- project a column value, a renamed constant, and an anonymous one
  std::vector<NamedExpression> columns;
  columns.emplace_back("first", ColumnValueExp("c1"));
  columns.emplace_back("second", ConstantValueExp(Value(7)));
  columns.emplace_back("", ConstantValueExp(Value(9)));
  Plan pp(new ProjectionPlan(child, std::move(columns)));

  // Assert -- named columns keep their names, anonymous ones become $col<i>
  const Schema& sc = pp->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 3);
  EXPECT_EQ(sc.GetColumn(0).Name().name, "first");
  EXPECT_EQ(sc.GetColumn(1).Name().name, "second");
  EXPECT_EQ(sc.GetColumn(2).Name().name, "$col2");
  // Assert -- accessors delegate to the child plan
  EXPECT_EQ(pp->GetStats().Rows(), child->GetStats().Rows());
  EXPECT_EQ(pp->GetStats().Rows(), ts->Rows());
  EXPECT_EQ(pp->AccessRowCount(), child->AccessRowCount());
  EXPECT_EQ(pp->EmitRowCount(), child->EmitRowCount());
  EXPECT_EQ(pp->ScanSource(), child->ScanSource());
  // Assert -- ordering is delegated (FullScanPlan advertises no order)
  EXPECT_FALSE(pp->IsOrderedBy({ColumnValueExp("c1")}, {true}));
  EXPECT_FALSE(pp->IsOrderedBy({ColumnValueExp("c1")}, {false}));
  // Assert -- ToString and Dump render every projected column
  EXPECT_NE(pp->ToString().find("Project: {first, second, }"),
            std::string::npos);
  std::ostringstream oss;
  pp->Dump(oss, 0);
  EXPECT_NE(oss.str().find("c1 AS first"), std::string::npos);
  EXPECT_NE(oss.str().find("7 AS second"), std::string::npos);
  EXPECT_NE(oss.str().find("FullScan"), std::string::npos);
}

TEST_F(PlanTest, ProjectionQualifiedColumnValueNames) {
  // Arrange -- begin context, get Sc1 table and its real statistics
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc1"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  auto child = std::make_shared<FullScanPlan>(*tbl, *ts);

  // Act -- project a qualified ColumnValue (auto-named by its column) and an
  // explicitly renamed column
  std::vector<NamedExpression> columns;
  columns.emplace_back("", ColumnValueExp(ColumnName("Sc1.c1")));
  columns.emplace_back("renamed", ColumnValueExp(ColumnName("Sc1.c2")));
  Plan pp(new ProjectionPlan(child, std::move(columns)));

  // Assert -- the schema carries the qualified name / the alias
  const Schema& sc = pp->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 2);
  EXPECT_EQ(sc.GetColumn(0).Name().ToString(), "Sc1.c1");
  EXPECT_EQ(sc.GetColumn(0).Name().schema, "Sc1");
  EXPECT_EQ(sc.GetColumn(1).Name().name, "renamed");
  EXPECT_EQ(pp->GetStats().Rows(), ts->Rows());
}

TEST_F(PlanTest, ProjectionEmitExecutorProjectsValues) {
  // Arrange -- begin context, get Sc1 table and its real statistics
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc1"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  auto child = std::make_shared<FullScanPlan>(*tbl, *ts);

  // Act -- project the int column and a constant 5
  std::vector<NamedExpression> columns;
  columns.emplace_back("c1");
  columns.emplace_back("", ConstantValueExp(Value(5)));
  Plan pp(new ProjectionPlan(child, std::move(columns)));

  // Assert -- each source row becomes a two-column projected row
  Executor executor = pp->EmitExecutor(ctx);
  Row result;
  size_t count = 0;
  int64_t sum = 0;
  while (executor->Next(&result, nullptr)) {
    ASSERT_EQ(result.values_.size(), 2);
    EXPECT_EQ(result[0].type, ValueType::kInt64);
    EXPECT_EQ(result[1], Value(5));
    sum += result[0].value.int_value;
    ++count;
  }
  EXPECT_EQ(count, 6);
  EXPECT_EQ(sum, 847);  // 12 + 10 + 52 + 242 + 431 + 100
}

TEST_F(PlanTest, ProjectionPlanIsOrderedByDelegatesToChild) {
  // Arrange -- begin context, get Sc2 table and its Sc2PK index; the index
  // scan advertises an ascending order on d1
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  std::vector<ColumnName> provided{ColumnName("Sc2.d1")};
  auto scan = std::make_shared<IndexOnlyScanPlan>(
      *tbl, tbl->GetIndex(0), *ts, std::vector<Value>{}, std::vector<Value>{},
      true, nullptr, provided);

  // Act -- project the index key column over the ordered scan
  std::vector<NamedExpression> columns;
  columns.emplace_back("d1");
  Plan pp(new ProjectionPlan(scan, std::move(columns)));

  // Assert -- ordering is delegated to the child plan
  EXPECT_TRUE(pp->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {true}));
  EXPECT_FALSE(
      pp->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {false}));
  EXPECT_FALSE(pp->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d2"))}, {true}));
  // Assert -- Dump renders the whole tree, including the ordered child scan
  std::ostringstream oss;
  pp->Dump(oss, 0);
  EXPECT_NE(oss.str().find("IndexOnlyScan: Sc2"), std::string::npos);
}

TEST_F(PlanTest, IndexOnlyScanPlanDescendingOrder) {
  // Arrange -- begin context, get Sc2 table and its Sc2PK index
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();

  // Act -- construct a descending index-only scan advertising d1 order
  std::vector<ColumnName> provided{ColumnName("Sc2.d1")};
  Plan plan(new IndexOnlyScanPlan(*tbl, tbl->GetIndex(0), *ts, {}, {}, false,
                                  nullptr, provided));

  // Assert -- the order matches only for the descending direction
  EXPECT_TRUE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {false}));
  EXPECT_FALSE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d1"))}, {true}));
  EXPECT_FALSE(
      plan->IsOrderedBy({ColumnValueExp(ColumnName("Sc2.d2"))}, {false}));
  EXPECT_NE(plan->ToString().find("IndexOnlyScan: Sc2"), std::string::npos);
}

TEST_F(PlanTest, IndexOnlyScanPlanStaleIndexKeysFallback) {
  // Arrange -- begin a reader BEFORE a concurrent writer commits so the
  // reader's snapshot predates the committed index mutation
  TransactionContext reader = rs_->BeginContext();
  const auto tbl_or = (reader.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (reader.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  {
    TransactionContext writer = rs_->BeginContext();
    ASSERT_SUCCESS(tbl->Insert(writer.txn_, Row({Value(999), Value(1.0),
                                                 Value("new"), Value(8)}))
                       .GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  ASSERT_TRUE(reader.txn_.IndexKeysMayBeStale());

  // Act -- emit an index-only scan; stale index keys must degrade to the
  // FullScan + Selection + Projection fallback path
  Expression where = BinaryExpressionExp(ColumnValueExp("d1"),
                                         BinaryOperation::kGreaterThanEquals,
                                         ConstantValueExp(Value(0)));
  Plan plan(
      new IndexOnlyScanPlan(*tbl, tbl->GetIndex(0), *ts, {}, {}, true, where));
  Executor scan = plan->EmitExecutor(reader);
  Row result;
  size_t count = 0;
  while (scan->Next(&result, nullptr)) {
    ASSERT_EQ(result.values_.size(), 1);
    ++count;
  }

  // Assert -- the reader's snapshot hides the committed row: 6 rows visible
  ASSERT_EQ(count, 6);
  reader.txn_.Abort();
}

TEST_F(PlanTest, IndexOnlyScanPlanStaleFallbackProjectsIncludedColumns) {
  // Arrange -- add a covering index whose key is d1 and includes d2 and d3
  {
    TransactionContext setup = rs_->BeginContext();
    ASSERT_SUCCESS(
        rs_->CreateIndex(setup, "Sc2", IndexSchema("Sc2Inc", {0}, {1, 2})));
    ASSERT_SUCCESS(setup.txn_.PreCommit());
  }

  // Act -- a reader begun before a later committed writer must use the
  // FullScan fallback, which projects both key and included columns
  TransactionContext reader = rs_->BeginContext();
  const auto tbl_or = (reader.GetTable("Sc2"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (reader.GetStats("Sc2"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  ASSERT_GE(tbl->IndexCount(), 2);
  {
    TransactionContext writer = rs_->BeginContext();
    ASSERT_SUCCESS(tbl->Insert(writer.txn_, Row({Value(777), Value(2.5),
                                                 Value("inc"), Value(9)}))
                       .GetStatus());
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  ASSERT_TRUE(reader.txn_.IndexKeysMayBeStale());

  // NOTE: the stale fallback passes the where expression straight to a
  // Selection executor; a null where crashes (see Selection/BytecodeCompiler
  // handling of an empty expression), so supply a real predicate.
  Expression where = BinaryExpressionExp(ColumnValueExp("d1"),
                                         BinaryOperation::kGreaterThanEquals,
                                         ConstantValueExp(Value(0)));
  Plan plan(
      new IndexOnlyScanPlan(*tbl, tbl->GetIndex(1), *ts, {}, {}, true, where));
  EXPECT_EQ(plan->GetSchema().ColumnCount(), 3);

  Executor scan = plan->EmitExecutor(reader);
  Row result;
  size_t count = 0;
  while (scan->Next(&result, nullptr)) {
    ASSERT_EQ(result.values_.size(), 3);
    ++count;
  }

  // Assert -- only the 6 snapshot-visible rows are returned
  ASSERT_EQ(count, 6);
  reader.txn_.Abort();
}

// ProjectionPlan(Plan, vector<ColumnName>) used to dereference a moved-from
// src; fixed in projection_plan.cpp (stats_/CalcSchema ordering).
TEST_F(PlanTest, ProjectionColumnNameCtorBuildsSchema) {
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  TableStatistics ts((Schema()));
  auto child = std::make_shared<FullScanPlan>(*tbl, ts);

  // Constructing through the ColumnName overload must not crash and must
  // preserve child statistics and schema.
  Plan pp(
      new ProjectionPlan(child, {ColumnName("Sc1.c1"), ColumnName("Sc1.c2")}));

  const Schema& sc = pp->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 2);
  EXPECT_EQ(sc.GetColumn(0).Name().name, "c1");
  EXPECT_EQ(sc.GetColumn(1).Name().name, "c2");
  EXPECT_EQ(pp->AccessRowCount(), child->AccessRowCount());
  EXPECT_EQ(pp->EmitRowCount(), child->EmitRowCount());
  std::ostringstream oss;
  pp->Dump(oss, 0);
  EXPECT_NE(oss.str().find("Project: {Sc1.c1, Sc1.c2}"), std::string::npos);
}

TEST_F(PlanTest, ProjectionQualifiedColumnValueAccessorsAndRender) {
  // Arrange -- begin context, get Sc1 table and its real statistics
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  const auto ts_or = (ctx.GetStats("Sc1"));
  ASSERT_EQ(ts_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<TableStatistics>& ts = ts_or.Value();
  auto child = std::make_shared<FullScanPlan>(*tbl, *ts);

  // Act -- project a fully qualified ColumnValue (auto-named by its column)
  std::vector<NamedExpression> columns;
  columns.emplace_back("", ColumnValueExp(ColumnName("Sc1.c3")));
  Plan pp(new ProjectionPlan(child, std::move(columns)));

  // Assert -- the schema carries the qualified name and all accessors delegate
  // to the child plan
  const Schema& sc = pp->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 1);
  EXPECT_EQ(sc.GetColumn(0).Name().ToString(), "Sc1.c3");
  EXPECT_EQ(sc.GetColumn(0).Name().schema, "Sc1");
  EXPECT_EQ(pp->AccessRowCount(), child->AccessRowCount());
  EXPECT_EQ(pp->EmitRowCount(), child->EmitRowCount());
  EXPECT_EQ(pp->GetStats().Rows(), child->GetStats().Rows());
  EXPECT_EQ(pp->GetStats().Rows(), ts->Rows());
  EXPECT_EQ(pp->ScanSource(), child->ScanSource());
  // Assert -- ordering is delegated (FullScanPlan advertises no order)
  EXPECT_FALSE(pp->IsOrderedBy({ColumnValueExp("c3")}, {true}));
  // Assert -- both renderings name the projected column.  ToString uses the
  // NamedExpression name (empty for an anonymous ColumnValue) while Dump
  // renders the underlying expression's qualified column name.
  EXPECT_NE(pp->ToString().find("Project: {}"), std::string::npos);
  std::ostringstream oss;
  pp->Dump(oss, 0);
  EXPECT_NE(oss.str().find("Sc1.c3"), std::string::npos);
  EXPECT_NE(oss.str().find("FullScan"), std::string::npos);
}

TEST_F(PlanTest, ProjectionAllAnonymousColumnsRenderAndEmit) {
  // Arrange -- begin context, get Sc1 table
  auto ctx = rs_->BeginContext();
  const auto tbl_or = (ctx.GetTable("Sc1"));
  ASSERT_EQ(tbl_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& tbl = tbl_or.Value();
  TableStatistics ts((Schema()));
  auto child = std::make_shared<FullScanPlan>(*tbl, ts);

  // Act -- project three anonymous constants so every column is auto-named
  std::vector<NamedExpression> columns;
  columns.emplace_back("", ConstantValueExp(Value(1)));
  columns.emplace_back("", ConstantValueExp(Value(2)));
  columns.emplace_back("", ConstantValueExp(Value(3)));
  Plan pp(new ProjectionPlan(child, std::move(columns)));

  // Assert -- the schema auto-names each anonymous column
  const Schema& sc = pp->GetSchema();
  EXPECT_EQ(sc.ColumnCount(), 3);
  EXPECT_EQ(sc.GetColumn(0).Name().name, "$col0");
  EXPECT_EQ(sc.GetColumn(1).Name().name, "$col1");
  EXPECT_EQ(sc.GetColumn(2).Name().name, "$col2");
  // Assert -- ToString joins empty names with commas
  EXPECT_NE(pp->ToString().find("Project: {, , }"), std::string::npos);
  EXPECT_EQ(pp->EmitRowCount(), child->EmitRowCount());
  // Assert -- executing the plan emits one row per source row
  Executor executor = pp->EmitExecutor(ctx);
  Row result;
  size_t count = 0;
  while (executor->Next(&result, nullptr)) {
    ASSERT_EQ(result.values_.size(), 3);
    EXPECT_EQ(result[0], Value(1));
    EXPECT_EQ(result[1], Value(2));
    EXPECT_EQ(result[2], Value(3));
    ++count;
  }
  EXPECT_EQ(count, 6);
  std::ostringstream oss;
  pp->Dump(oss, 0);
  EXPECT_NE(oss.str().find("Project: {"), std::string::npos);
  EXPECT_NE(oss.str().find("FullScan"), std::string::npos);
}

TEST_F(PlanTest, LateralJoinExpansion) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl1, ctx.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl2, ctx.GetTable("Sc2"));
  TableStatistics ts((Schema()));
  Plan left = std::make_shared<FullScanPlan>(*tbl1, ts);
  Plan right = std::make_shared<FullScanPlan>(*tbl2, ts);
  Plan join_plan(new MergeJoinPlan(left, {ColumnName("Sc1.c1")}, right,
                                   {ColumnName("Sc2.d1")}));
  EXPECT_EQ(join_plan->GetSchema().ColumnCount(), 7U);
  Executor exec = join_plan->EmitExecutor(ctx);
  Row r;
  size_t cnt = 0;
  while (exec->Next(&r, nullptr)) {
    ++cnt;
  }
  EXPECT_GT(cnt, 0U);
  ctx.txn_.Abort();
}

TEST_F(PlanTest, BatchInsertChunking) {
  auto ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable("Sc1"));
  // Insert multi-row values in chunks
  std::vector<Row> batch_rows;
  batch_rows.reserve(70);
  for (int i = 0; i < 70; ++i) {
    batch_rows.emplace_back(
        Row({Value(1000 + i), Value("batch_" + std::to_string(i)),
             Value(static_cast<double>(i) * 1.5)}));
  }
  for (const auto& r : batch_rows) {
    ASSERT_SUCCESS(tbl->Insert(ctx.txn_, r).GetStatus());
  }
  TableStatistics ts((Schema()));
  Plan scan(new FullScanPlan(*tbl, ts));
  Executor exec = scan->EmitExecutor(ctx);
  Row r;
  size_t cnt = 0;
  while (exec->Next(&r, nullptr)) {
    ++cnt;
  }
  EXPECT_EQ(cnt, 6 + 70);
  ctx.txn_.Abort();
}

}  // namespace tinylamb
