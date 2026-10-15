/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/late_materialization_join_plan.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "index/index_schema.hpp"
#include "plan/cascades.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/optimizer.hpp"
#include "plan/values_plan.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

Schema LeftSchema() {
  return Schema("LMPLeft", {Column("k", ValueType::kInt64),
                            Column("v", ValueType::kInt64)});
}

Schema RightSchema() {
  return Schema("LMPRight",
                {Column("k", ValueType::kInt64), Column("a", ValueType::kInt64),
                 Column("b", ValueType::kInt64), Column("c", ValueType::kInt64),
                 Column("d", ValueType::kInt64), Column("e", ValueType::kInt64),
                 Column("f", ValueType::kInt64)});
}

std::vector<Row> Drain(
    Executor executor) {  // NOLINT(performance-unnecessary-value-param)
  std::vector<Row> rows;
  Row row;
  while (executor->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

class LateMaterializationPlanTest : public ::testing::Test {
 public:
  void SetUp() override {
    database_ =
        Database::Create("late_materialization_plan_test-" + RandomString())
            .MoveValue();
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, left,
                          database_->CreateTable(setup, LeftSchema()));
    ASSIGN_OR_ASSERT_FAIL(Table, right,
                          database_->CreateTable(setup, RightSchema()));
    for (int64_t i = 0; i < 40; ++i) {
      ASSERT_SUCCESS(
          left.Insert(setup.txn_, Row({Value(i % 10), Value(1000 + i)}))
              .GetStatus());
    }
    for (int64_t i = 0; i < 4; ++i) {
      ASSERT_SUCCESS(right
                         .Insert(setup.txn_, Row({Value(i + 1), Value(10 + i),
                                                  Value(20 + i), Value(30 + i),
                                                  Value(40 + i), Value(50 + i),
                                                  Value(60 + i)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "LMPRight",
        IndexSchema("lmp_right_k", {0}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(setup.txn_.PreCommit());
    TransactionContext statistics = database_->BeginContext();
    database_->RefreshStatistics(statistics, "LMPLeft");
    database_->RefreshStatistics(statistics, "LMPRight");
    ASSERT_SUCCESS(statistics.txn_.PreCommit());
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    left_table_ = context_->GetTable("LMPLeft").MoveValue();
    right_table_ = context_->GetTable("LMPRight").MoveValue();
    left_stats_ = context_->GetStats("LMPLeft").MoveValue();
    right_stats_ = context_->GetStats("LMPRight").MoveValue();
  }

  void TearDown() override {
    if (context_ != nullptr && !context_->IsFinished()) {
      context_->txn_.PreCommit();
    }
    database_->DeleteAll();
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::shared_ptr<Table> left_table_;
  std::shared_ptr<Table> right_table_;
  std::shared_ptr<TableStatistics> left_stats_;
  std::shared_ptr<TableStatistics> right_stats_;
};

TEST_F(LateMaterializationPlanTest, RulePublishesLogicalPlanForWideSmallBuild) {
  QueryData query(
      {"LMPLeft", "LMPRight"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("LMPLeft", "k")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("LMPRight", "k"))),
      {NamedExpression("left_k", ColumnValueExp(ColumnName("LMPLeft", "k"))),
       NamedExpression("right_a",
                       ColumnValueExp(ColumnName("LMPRight", "a")))});
  ASSERT_SUCCESS(query.Rewrite(*context_));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, *context_));
  std::ostringstream dump;
  dump << *plan;
  Executor executor = plan->EmitExecutor(*context_);
  const std::vector<Row> rows = Drain(executor);
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  std::vector<std::pair<int64_t, int64_t>> actual;
  actual.reserve(rows.size());
  for (const Row& row : rows) {
    actual.emplace_back(row[0].value.int_value, row[1].value.int_value);
  }
  std::vector<std::pair<int64_t, int64_t>> expected;
  for (int64_t i = 0; i < 40; ++i) {
    for (int64_t key = 1; key <= 4; ++key) {
      if (i % 10 == key) {
        expected.emplace_back(i % 10, 9 + key);
      }
    }
  }
  std::ranges::sort(actual);
  std::ranges::sort(expected);
  EXPECT_EQ(actual, expected);

  OptimizerOptions fallback_options = OptimizerOptions::Default();
  fallback_options.disabled_implementation_rules = {
      "late_materialization_join"};
  ASSIGN_OR_ASSERT_FAIL(
      Plan, fallback_plan,
      Optimizer::Optimize(query, *context_, fallback_options));
  std::ostringstream fallback_dump;
  fallback_dump << *fallback_plan;
  EXPECT_EQ(fallback_dump.str().find("LateMaterializationJoin"),
            std::string::npos)
      << fallback_dump.str();
  EXPECT_EQ(Drain(fallback_plan->EmitExecutor(*context_)).size(), 16U);
}

TEST_F(LateMaterializationPlanTest,
       PlanCarriesIdentityPayloadAndOrderContract) {
  Plan left = std::make_shared<FullScanPlan>(*left_table_, *left_stats_);
  const Schema output = LeftSchema() + RightSchema();
  Plan plan = std::make_shared<LateMaterializationJoinPlan>(
      left, std::vector<ColumnName>{ColumnName("LMPLeft", "k")}, *right_table_,
      right_table_->GetIndex(0),
      std::vector<ColumnName>{ColumnName("LMPRight", "k")}, *right_stats_,
      nullptr, output, std::vector<slot_t>{0, 1, 2, 3, 4, 5, 6}, false, 4U);
  const auto late =
      std::dynamic_pointer_cast<LateMaterializationJoinPlan>(plan);
  ASSERT_NE(late, nullptr);
  EXPECT_EQ(plan->GetSchema().ColumnCount(), 9U);
  EXPECT_EQ(late->Identity(), LateMaterializationIdentity::kEquiKey);
  EXPECT_EQ(late->RightProjection().size(), 7U);
  EXPECT_NE(plan->ToString().find("payload=late"), std::string::npos);
  EXPECT_FALSE(plan->ContainsParameterSlots());
}

TEST_F(LateMaterializationPlanTest, RuleRejectsRowPositionOuterAndArrayShapes) {
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "late_materialization_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  EXPECT_FALSE(rule->MayApply(cascades::LogicalOperator::kOuterJoin));
  EXPECT_FALSE(rule->MayApply(cascades::LogicalOperator::kSemiJoin));
  EXPECT_FALSE(rule->MayApply(cascades::LogicalOperator::kAntiJoin));

  Plan left = std::make_shared<FullScanPlan>(*left_table_, *left_stats_);
  Plan right = std::make_shared<FullScanPlan>(*right_table_, *right_stats_);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"LMPLeft", "LMPRight"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("LMPLeft", "k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("LMPRight", "k")));
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left, .estimated_rows = 40.0},
      cascades::BestPlan{.plan = right, .estimated_rows = 4.0}};
  cascades::RuleContext context;
  context.transaction = context_.get();
  context.tables = {{"LMPLeft", left_table_}, {"LMPRight", right_table_}};
  context.statistics = {{"LMPLeft", left_stats_}, {"LMPRight", right_stats_}};
  cascades::PhysicalProperties row_position;
  row_position.require_row_position = true;
  EXPECT_TRUE(rule->Apply(memo, logical.children[1], logical, children,
                          row_position, context)
                  .empty());

  TransactionContext array_setup = database_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, array_table,
      database_->CreateTable(
          array_setup,
          Schema("ArrayRight", {Column("k", ValueType::kInt64),
                                Column("payload", ValueType::kArray)})));
  ASSERT_SUCCESS(array_table
                     .Insert(array_setup.txn_,
                             Row({Value(1), Value::Array({Value(1)}, "INT64")}))
                     .GetStatus());
  ASSERT_SUCCESS(database_->CreateIndex(
      array_setup, "ArrayRight",
      IndexSchema("array_right_k", {0}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(array_setup.txn_.PreCommit());
  TransactionContext array_statistics = database_->BeginContext();
  database_->RefreshStatistics(array_statistics, "ArrayRight");
  ASSERT_SUCCESS(array_statistics.txn_.PreCommit());
  auto array_shared = array_statistics.GetTable("ArrayRight").MoveValue();
  auto array_stats = array_statistics.GetStats("ArrayRight").MoveValue();
  Plan array_left = std::make_shared<ValuesPlan>(
      Schema("ArrayLeft", {Column("k", ValueType::kInt64)}),
      std::vector<Row>{Row({Value(1)})});
  Plan array_right =
      std::make_shared<FullScanPlan>(*array_shared, *array_stats);
  cascades::Memo array_memo;
  const cascades::GroupId array_root =
      array_memo.Build({"ArrayLeft", "ArrayRight"});
  cascades::LogicalExpression array_logical =
      array_memo.Get(array_root).expressions.front();
  array_logical.predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("ArrayLeft", "k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("ArrayRight", "k")));
  const std::vector<cascades::BestPlan> array_children{
      cascades::BestPlan{.plan = array_left, .estimated_rows = 1.0},
      cascades::BestPlan{.plan = array_right, .estimated_rows = 1.0}};
  cascades::RuleContext array_context;
  array_context.transaction = &array_statistics;
  array_context.tables = {{"ArrayRight", array_shared}};
  array_context.statistics = {{"ArrayRight", array_stats}};
  EXPECT_TRUE(rule->Apply(array_memo, array_logical.children[1], array_logical,
                          array_children, cascades::PhysicalProperties{},
                          array_context)
                  .empty());
  array_statistics.txn_.Abort();
}

}  // namespace
}  // namespace tinylamb
