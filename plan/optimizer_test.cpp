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

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/platform/random.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join.hpp"
#include "executor/update.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "plan/band_join_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/zigzag_join_plan.hpp"
#include "query/query_data.hpp"
#include "query/statement.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
static const char* const kIndexName = "SampleIndex";

namespace {

// Builds `EXISTS (SELECT <inner_select> FROM <table> WHERE <where>)` shaped
// subquery expressions for the decorrelation tests.
Expression ExistsSubquery(const std::vector<NamedExpression>& select,
                          const std::string& table, Expression where,
                          bool negated = false) {
  auto statement = std::make_shared<SelectStatement>(
      select, std::vector<std::string>{table}, std::move(where));
  Expression expression =
      QueryExpressionExp(std::move(statement), nullptr, true, false);
  return negated
             ? UnaryExpressionExp(std::move(expression), UnaryOperation::kNot)
             : expression;
}
}  // namespace

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
      rs_.reset();
    }
    rs_ = Database::Create(prefix_).MoveValue();
  }

  void TearDown() override { rs_->DeleteAll(); }

  [[nodiscard]] Status DumpAll(const QueryData& qd)
      const {  // Arrange: open context + rewrite query for debugging
    TransactionContext ctx = rs_->BeginContext();
    QueryData qd_resolved = qd;
    qd_resolved.Rewrite(ctx);
    LOG(INFO) << qd << "\n";

    // Act: optimize plan + emit executor + execute query
    ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(qd_resolved, ctx));
    Executor exec = plan->EmitExecutor(ctx);
    LOG(INFO) << " --- Logical Plan ---\n" << *plan;
    LOG(INFO) << "\n --- Physical Plan ---\n" << *exec;
    LOG(INFO) << "\n --- Output ---\n" << plan->GetSchema();
    Row result;
    while (exec->Next(&result, nullptr)) {
      LOG(INFO) << result;
    }
    return Status::kSuccess;
  }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

namespace {

// gtest ASSERT_TRUE is invisible to clang-tidy's dataflow, so the optional
// probe is unwrapped through this helper: the assert keeps
// bugprone-unchecked-optional-access quiet while the failure mode (abort in
// debug, a benign static_cast in release) matches the tests' expectations.
const LookupSeek& SeekOf(const std::optional<LookupSeek>& probe) {
  assert(probe.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return *probe;
}
}  // namespace
TEST_F(OptimizerTest, Construct) {
  // Arrange (SetUp created 4 tables + indexes + statistics)

  // Act + Assert: database fixture is usable
  TransactionContext ctx = rs_->BeginContext();
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

TEST_F(OptimizerTest, Simple) {
  // Arrange
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"),
       NamedExpression("Column2Varchar", ColumnName("c2"))}};

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, FilteredScanEstimateAppliesSelectivityOnce) {
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c3"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(50.0))),
      {NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const StatusOr<Plan> plan = Optimizer::Optimize(query, context);
  ASSERT_TRUE(plan.HasValue());
  EXPECT_GE(plan.Value()->EmitRowCount(), 55U);
  EXPECT_LE(plan.Value()->EmitRowCount(), 65U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, FullScanPeeksCheckColumnTypeBeforeReadingIntegerBytes) {
  TransactionContext context = rs_->BeginContext();
  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, statistics,
                        context.GetStats("Sc1"));
  rule_context.tables.emplace("Sc1", table);
  rule_context.statistics.emplace("Sc1", statistics);
  const cascades::ImplementationRule* scan_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "full_scan") {
      scan_rule = &rule;
      break;
    }
  }
  ASSERT_NE(scan_rule, nullptr);
  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                          BinaryOperation::kLessThan,
                          ConstantValueExp(Value(25))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c3")),
                          BinaryOperation::kLessThan,
                          ConstantValueExp(Value(25))));
  cascades::Memo memo;
  const auto group = memo.Build({"Sc1"}, {{predicate, {"Sc1"}}});
  const auto alternatives =
      scan_rule->Apply(memo, group, memo.Get(group).expressions.front(), {},
                       cascades::PhysicalProperties{}, rule_context);
  ASSERT_EQ(alternatives.size(), 1);
  const auto selection =
      std::dynamic_pointer_cast<SelectionPlan>(alternatives.front().plan);
  ASSERT_NE(selection, nullptr);
  const auto scan = std::dynamic_pointer_cast<FullScanPlan>(selection->Src());
  ASSERT_NE(scan, nullptr);
  ASSERT_EQ(scan->PeekCompares().size(), 1);
  EXPECT_EQ(scan->PeekCompares()[0].column, 0);
  EXPECT_EQ(scan->PeekCompares()[0].constant, 25);
  EXPECT_EQ(scan->PeekCompares()[0].op, BinaryOperation::kLessThan);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, NonUniqueIndexEstimatesMatchPointFrequencies) {
  TransactionContext context = rs_->BeginContext();
  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, context.GetTable("Sc4"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, statistics,
                        context.GetStats("Sc4"));
  rule_context.tables.emplace("Sc4", table);
  rule_context.statistics.emplace("Sc4", std::move(statistics));

  const cascades::ImplementationRuleSet& implementation_rules =
      DefaultImplementationRules();
  const cascades::ImplementationRule* index_rule = nullptr;
  for (const cascades::ImplementationRule& rule :
       implementation_rules.Rules()) {
    if (rule.Name() == "index_scan") {
      index_rule = &rule;
      break;
    }
  }
  ASSERT_NE(index_rule, nullptr);

  const Expression equality = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc4", "c2")), BinaryOperation::kEquals,
      ConstantValueExp(Value(std::string("0"))));
  cascades::Memo equality_memo;
  const cascades::GroupId equality_group =
      equality_memo.Build({"Sc4"}, {{equality, {"Sc4"}}});
  const auto equality_alternatives =
      index_rule->Apply(equality_memo, equality_group,
                        equality_memo.Get(equality_group).expressions.front(),
                        {}, cascades::PhysicalProperties{}, rule_context);
  bool equality_found = false;
  for (const cascades::PlanAlternative& alternative : equality_alternatives) {
    if (alternative.estimated_rows >= 24.0 &&
        alternative.estimated_rows <= 26.0) {
      equality_found = true;
      EXPECT_NEAR(static_cast<double>(alternative.plan->EmitRowCount()), 25.0,
                  1.0);
    }
  }
  EXPECT_TRUE(equality_found);

  const Expression in =
      InExpressionExp(ColumnValueExp(ColumnName("Sc4", "c2")),
                      {ConstantValueExp(Value(std::string("0"))),
                       ConstantValueExp(Value(std::string("2")))});
  cascades::Memo in_memo;
  const cascades::GroupId in_group = in_memo.Build({"Sc4"}, {{in, {"Sc4"}}});
  const auto in_alternatives = index_rule->Apply(
      in_memo, in_group, in_memo.Get(in_group).expressions.front(), {},
      cascades::PhysicalProperties{}, rule_context);
  Plan point_plan;
  double point_estimate = 0.0;
  for (const cascades::PlanAlternative& alternative : in_alternatives) {
    std::ostringstream dump;
    alternative.plan->Dump(dump, 0);
    if (dump.str().find("x2 points") != std::string::npos) {
      point_plan = alternative.plan;
      point_estimate = alternative.estimated_rows;
      break;
    }
  }
  ASSERT_NE(point_plan, nullptr);
  EXPECT_NEAR(point_estimate, 50.0, 1.0);
  EXPECT_NEAR(static_cast<double>(point_plan->EmitRowCount()), 50.0, 1.0);
  Executor executor = point_plan->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    row = Row();
  }
  EXPECT_EQ(rows, 50U);
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, ConstantFalseSelectionBecomesEmptyPlan) {
  QueryData query{{"Sc1"}, ConstantValueExp(Value(0)), {NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("Empty"), std::string::npos) << dump.str();
  EXPECT_EQ(plan->EmitRowCount(), 0U);

  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("EmptyResult"), std::string::npos)
      << physical.str();
  EXPECT_EQ(physical.str().find("FullScan"), std::string::npos)
      << physical.str();
  Row row;
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, RewritesArithmeticInsideProjection) {
  QueryData query{
      {"Sc1"},
      nullptr,
      {NamedExpression(
           "identity",
           BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kAdd,
                               ConstantValueExp(Value(0)))),
       NamedExpression("folded",
                       BinaryExpressionExp(
                           ColumnValueExp("c1"), BinaryOperation::kAdd,
                           BinaryExpressionExp(ConstantValueExp(Value(1)),
                                               BinaryOperation::kAdd,
                                               ConstantValueExp(Value(2)))))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("+ 0"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("1 + 2"), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find("+ 3"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  ASSERT_EQ(row.values_.size(), 2U);
  EXPECT_EQ(row[0], Value(0));
  EXPECT_EQ(row[1], Value(3));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, RewritesTypedIntegerMultiplyByZeroInsideProjection) {
  QueryData query{
      {"Sc1"},
      nullptr,
      {NamedExpression("lhs", BinaryExpressionExp(ColumnValueExp("c1"),
                                                  BinaryOperation::kMultiply,
                                                  ConstantValueExp(Value(0)))),
       NamedExpression("rhs", BinaryExpressionExp(ConstantValueExp(Value(0)),
                                                  BinaryOperation::kMultiply,
                                                  ColumnValueExp("c1")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("* 0"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("0 *"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row.values_.size(), 2U);
    EXPECT_EQ(row[0], Value(0));
    EXPECT_EQ(row[1], Value(0));
    ++rows;
  }
  EXPECT_EQ(rows, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, SimplifiesSelfComparisonsInFilterContext) {
  TransactionContext context = rs_->BeginContext();

  QueryData equality{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("c1")),
      {NamedExpression("c1")}};
  ASSERT_SUCCESS(equality.Rewrite(context));
  const auto equality_plan = Optimizer::Optimize(equality, context);
  ASSERT_EQ(equality_plan.GetStatus(), Status::kSuccess);
  std::ostringstream equality_dump;
  equality_dump << *equality_plan.Value();
  EXPECT_NE(equality_dump.str().find("IS NOT NULL"), std::string::npos)
      << equality_dump.str();
  EXPECT_EQ(equality_dump.str().find("c1 = c1"), std::string::npos)
      << equality_dump.str();

  QueryData inequality{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kNotEquals,
                          ColumnValueExp("c1")),
      {NamedExpression("c1")}};
  ASSERT_SUCCESS(inequality.Rewrite(context));
  const auto inequality_plan = Optimizer::Optimize(inequality, context);
  ASSERT_EQ(inequality_plan.GetStatus(), Status::kSuccess);
  std::ostringstream inequality_dump;
  inequality_dump << *inequality_plan.Value();
  EXPECT_NE(inequality_dump.str().find("EmptyResult"), std::string::npos)
      << inequality_dump.str();
  EXPECT_EQ(inequality_dump.str().find("FullScan"), std::string::npos)
      << inequality_dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, DoesNotFoldThrowingSelfComparison) {
  const Expression arithmetic = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kAdd,
      ConstantValueExp(Value(std::numeric_limits<int64_t>::max())));
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(arithmetic, BinaryOperation::kNotEquals, arithmetic),
      {NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  plan_or.Value()->Dump(dump, 0);
  EXPECT_NE(dump.str().find(" + "), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  while (executor->Next(&row, nullptr)) {
  }
  EXPECT_NE(executor->GetStatus(), Status::kSuccess);
  context.Abort();
}

TEST_F(OptimizerTest, ContradictoryConjunctsBecomeEmptyResult) {
  TransactionContext context = rs_->BeginContext();
  const auto check_empty = [&](const Expression& predicate) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    ASSERT_SUCCESS(query.Rewrite(context));
    const auto plan_or = Optimizer::Optimize(query, context);
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    std::ostringstream dump;
    dump << *plan_or.Value();
    EXPECT_NE(dump.str().find("EmptyResult"), std::string::npos) << dump.str();
    EXPECT_EQ(dump.str().find("FullScan"), std::string::npos) << dump.str();
  };

  check_empty(BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(10))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(11)))));
  check_empty(BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(10))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("c1"),
                          BinaryOperation::kLessThanEquals,
                          ConstantValueExp(Value(10)))));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, StrongerBoundsSubsumeWeakerBounds) {
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(BinaryExpressionExp(ColumnValueExp("c1"),
                                              BinaryOperation::kGreaterThan,
                                              ConstantValueExp(Value(0))),
                          BinaryOperation::kAnd,
                          BinaryExpressionExp(ColumnValueExp("c1"),
                                              BinaryOperation::kGreaterThan,
                                              ConstantValueExp(Value(20)))),
      {NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("c1 > 0"), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find("c1 > 20"), std::string::npos) << dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, EqualityClassesPropagateAndRejectConstants) {
  TransactionContext context = rs_->BeginContext();
  const auto optimize = [&](int64_t d1, int64_t d4) {
    QueryData query{
        {"Sc2"},
        BinaryExpressionExp(
            BinaryExpressionExp(ColumnValueExp("d1"), BinaryOperation::kEquals,
                                ColumnValueExp("d4")),
            BinaryOperation::kAnd,
            BinaryExpressionExp(
                BinaryExpressionExp(ColumnValueExp("d1"),
                                    BinaryOperation::kEquals,
                                    ConstantValueExp(Value(d1))),
                BinaryOperation::kAnd,
                BinaryExpressionExp(ColumnValueExp("d4"),
                                    BinaryOperation::kEquals,
                                    ConstantValueExp(Value(d4))))),
        {NamedExpression("d1")}};
    EXPECT_EQ(query.Rewrite(context), Status::kSuccess);
    return Optimizer::Optimize(query, context);
  };

  const auto propagated = optimize(16, 16);
  ASSERT_EQ(propagated.GetStatus(), Status::kSuccess);
  std::ostringstream propagated_dump;
  propagated_dump << *propagated.Value();
  EXPECT_EQ(propagated_dump.str().find("d1 = d4"), std::string::npos)
      << propagated_dump.str();
  EXPECT_NE(propagated_dump.str().find("d1 = 16"), std::string::npos)
      << propagated_dump.str();

  const auto conflicting = optimize(10, 16);
  ASSERT_EQ(conflicting.GetStatus(), Status::kSuccess);
  std::ostringstream conflicting_dump;
  conflicting_dump << *conflicting.Value();
  EXPECT_NE(conflicting_dump.str().find("EmptyResult"), std::string::npos)
      << conflicting_dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, NullAndInComplementsBecomeEmptyResult) {
  TransactionContext context = rs_->BeginContext();
  const auto assert_empty = [&](const Expression& predicate) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    EXPECT_EQ(query.Rewrite(context), Status::kSuccess);
    const auto plan_or = Optimizer::Optimize(query, context);
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    std::ostringstream dump;
    dump << *plan_or.Value();
    EXPECT_NE(dump.str().find("EmptyResult"), std::string::npos) << dump.str();
  };

  assert_empty(BinaryExpressionExp(
      UnaryExpressionExp(ColumnValueExp("c1"), UnaryOperation::kIsNull),
      BinaryOperation::kAnd,
      UnaryExpressionExp(ColumnValueExp("c1"), UnaryOperation::kIsNotNull)));

  const auto in = [] {
    return InExpressionExp(
        ColumnValueExp("c2"),
        {ConstantValueExp(Value("c2-1")), ConstantValueExp(Value("c2-2"))});
  };
  assert_empty(
      BinaryExpressionExp(in(), BinaryOperation::kAnd,
                          UnaryExpressionExp(in(), UnaryOperation::kNot)));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, SimplePrefixLikeBecomesExactBinaryRange) {
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kLike,
                          ConstantValueExp(Value("c2-3%"))),
      {NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("LIKE"), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find(">= \"c2-3\""), std::string::npos) << dump.str();
  EXPECT_NE(dump.str().find("< \"c2-4\""), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 11U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, LimitEstimateAccountsForOffset) {
  QueryData query{{"Sc1"}, nullptr, {NamedExpression("c1")}};
  query.limit_count_ = 2;
  query.limit_offset_ = 1000;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  EXPECT_EQ(plan_or.Value()->EmitRowCount(), 0U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, UnorderedLimitPushesRowCapIntoFullScan) {
  QueryData query{{"Sc3"}, nullptr, {NamedExpression("e1")}};
  query.limit_count_ = 1;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << plan_or.Value();
  EXPECT_NE(dump.str().find("max rows: 1"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 1U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, AggregateLimitDoesNotCapInputScan) {
  QueryData query{
      {"Sc1"},
      nullptr,
      {NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                       ColumnValueExp("c1")))}};
  query.limit_count_ = 1;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << plan_or.Value();
  EXPECT_EQ(dump.str().find("max rows:"), std::string::npos) << dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, CountStarPrefersFullScanForDistinctIndexKeys) {
  QueryData query{
      {"Sc1"},
      nullptr,
      {NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                       ColumnValueExp("*")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream logical;
  plan->Dump(logical, 0);
  EXPECT_EQ(logical.str().find("IndexOnlyScan"), std::string::npos)
      << logical.str();
  EXPECT_NE(logical.str().find("FullScan"), std::string::npos) << logical.str();

  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("FullScan"), std::string::npos)
      << physical.str();
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(100));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, LikePrefixUsesIndexRangeAndRetainsResidualPredicate) {
  QueryData query{
      {"Sc2"},
      BinaryExpressionExp(ColumnValueExp("d3"), BinaryOperation::kLike,
                          ConstantValueExp(Value("d3-1%"))),
      {NamedExpression("d3")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << plan_or.Value();
  EXPECT_NE(dump.str().find("Index"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row[0].type, ValueType::kVarChar);
    EXPECT_EQ(row[0].value.varchar_value.rfind("d3-1", 0), 0U);
    ++rows;
  }
  EXPECT_EQ(rows, 20U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, LikeSuffixDoesNotInventAnIndexRange) {
  QueryData query{
      {"Sc2"},
      BinaryExpressionExp(ColumnValueExp("d3"), BinaryOperation::kLike,
                          ConstantValueExp(Value("%1"))),
      {NamedExpression("d3")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << plan_or.Value();
  EXPECT_EQ(dump.str().find("IndexScan"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 20U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OrderByLiteralIsRemovedBeforeSortAndTopNPlanning) {
  QueryData query{{"Sc1"}, nullptr, {NamedExpression("c1")}};
  query.order_expressions_ = {ConstantValueExp(Value(1))};
  query.order_ascending_ = {true};
  query.limit_count_ = 2;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("Sort"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("TopN"), std::string::npos) << dump.str();
  EXPECT_EQ(plan_or.Value()->EmitRowCount(), 2U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, FoldedOrderByConstantIsRemovedBeforePlanning) {
  QueryData query{{"Sc1"}, nullptr, {NamedExpression("c1")}};
  query.order_expressions_ = {BinaryExpressionExp(ConstantValueExp(Value(1)),
                                                  BinaryOperation::kAdd,
                                                  ConstantValueExp(Value(2)))};
  query.order_ascending_ = {true};
  query.limit_count_ = 2;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("Sort"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("TopN"), std::string::npos) << dump.str();
  EXPECT_EQ(plan_or.Value()->EmitRowCount(), 2U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, CascadesSemiAndAntiJoinImplementationsUseHashJoinKind) {
  TransactionContext context = rs_->BeginContext();
  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  for (const char* relation : {"Sc1", "Sc2"}) {
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable(relation));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, statistics,
                          context.GetStats(relation));
    rule_context.tables.emplace(relation, std::move(table));
    rule_context.statistics.emplace(relation, std::move(statistics));
  }

  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  for (const auto [operation, expected_kind] :
       {std::pair{cascades::LogicalOperator::kSemiJoin, SemiJoinKind()},
        std::pair{cascades::LogicalOperator::kAntiJoin, AntiJoinKind()}}) {
    cascades::Memo memo;
    const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
    const cascades::GroupId left = memo.EnsureGroup({"Sc1"});
    const cascades::GroupId right = memo.EnsureGroup({"Sc2"});
    const cascades::GroupId derived =
        memo.EnsureDerivedGroup({"Sc1", "Sc2"}, "kinded-join");
    ASSERT_TRUE(memo.AddExpression(
        derived, cascades::LogicalExpression{.operation = operation,
                                             .children = {left, right},
                                             .predicate = predicate}));
    (void)root;

    const cascades::RuleSet no_rules;
    cascades::SearchEngine search(std::move(memo), no_rules);
    const auto best =
        search.Optimize(derived, cascades::PhysicalProperties{},
                        DefaultImplementationRules(), rule_context);
    ASSERT_TRUE(best.has_value());
    if (!best.has_value()) {
      continue;
    }
    const auto product = std::dynamic_pointer_cast<ProductPlan>(best->plan);
    ASSERT_NE(product, nullptr);
    EXPECT_EQ(product->Kind(), expected_kind);
    EXPECT_EQ(product->GetSchema().ColumnCount(),
              rule_context.tables.at("Sc1")->GetSchema().ColumnCount());
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, CompositeIndexUsesEqualityPrefix) {
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                              ConstantValueExp(Value("c2-32"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("c3"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(32 + 9.9)))),
      {NamedExpression("c1"), NamedExpression("c2")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream dump;
  dump << plan;
  EXPECT_NE(dump.str().find("Index"), std::string::npos) << dump.str();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(32));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, IndexScan) {
  // Arrange
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("c2-32"))),
      {NamedExpression("c1"), NamedExpression("score", ColumnName("c3"))}};

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, HistoricalSnapshotFallsBackFromMutableIndex) {
  TransactionContext old_reader = rs_->BeginContext();
  TransactionContext writer = rs_->BeginContext();
  const auto table_or = (writer.GetTable("Sc1"));
  ASSERT_EQ(table_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_or.Value();

  RowPosition target;
  Row replacement;
  for (Iterator iter = table->BeginFullScan(writer.txn_); iter.IsValid();
       ++iter) {
    if ((*iter)[0] == Value(2)) {
      target = iter.Position();
      replacement = Row({Value(1002), (*iter)[1], (*iter)[2]});
      break;
    }
  }
  ASSERT_TRUE(target.IsValid());
  ASSERT_SUCCESS(table->Update(writer.txn_, target, replacement).GetStatus());
  ASSERT_SUCCESS(writer.PreCommit());
  ASSERT_TRUE(old_reader.txn_.RequiresHistoricalRead());

  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"), NamedExpression("c2")}};
  ASSERT_SUCCESS(query.Rewrite(old_reader));
  const auto plan_or = (Optimizer::Optimize(query, old_reader));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(old_reader);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("FullScan"), std::string::npos)
      << physical.str();

  Row result;
  ASSERT_TRUE(executor->Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(2));
  EXPECT_FALSE(executor->Next(&result, nullptr));
  ASSERT_SUCCESS(old_reader.PreCommit());
}

TEST_F(OptimizerTest, PhysicalRulesCanBeRemovedIndependently) {
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"), NamedExpression("c2")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  OptimizerOptions full_scan_only = OptimizerOptions::Default();
  full_scan_only.disabled_implementation_rules.insert("index_scan");
  const auto full_plan_or =
      (Optimizer::Optimize(query, context, full_scan_only));
  ASSERT_EQ(full_plan_or.GetStatus(), Status::kSuccess);
  const Plan& full_plan = full_plan_or.Value();
  std::ostringstream full_dump;
  full_dump << full_plan;
  EXPECT_NE(full_dump.str().find("FullScan"), std::string::npos);

  OptimizerOptions index_scan_only = OptimizerOptions::Default();
  index_scan_only.disabled_implementation_rules.insert("full_scan");
  const auto index_plan_or =
      (Optimizer::Optimize(query, context, index_scan_only));
  ASSERT_EQ(index_plan_or.GetStatus(), Status::kSuccess);
  const Plan& index_plan = index_plan_or.Value();
  std::ostringstream index_dump;
  index_dump << index_plan;
  EXPECT_NE(index_dump.str().find("IndexScan"), std::string::npos);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, PhysicalRuleSubsetsPreserveOrderedLimitResults) {
  // Phase 9 hardening: exercise every useful subset of the alternative scan
  // and join implementations.  A subset may legitimately be unable to plan,
  // but every plan it does produce must retain the same ORDER BY/LIMIT
  // semantics as the complete rule set.
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("Sc2", "d1"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                              BinaryOperation::kLessThan,
                              ConstantValueExp(Value(6)))),
      {NamedExpression("key", ColumnName("Sc1", "c1"))}};
  query.order_expressions_ = {ColumnValueExp(ColumnName("Sc1", "c1"))};
  query.order_ascending_ = {true};
  query.limit_count_ = 2;
  query.limit_offset_ = 1;

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const std::array<std::string, 5> optional_rules = {
      "index_scan", "full_scan", "hash_join", "index_join", "nested_loop_join"};
  std::vector<uint32_t> masks;
  masks.reserve(1U << optional_rules.size());
  for (uint32_t mask = 0; mask < (1U << optional_rules.size()); ++mask) {
    masks.push_back(mask);
  }
  // A fixed seed makes failures reproducible while avoiding reliance on rule
  // registration order during the subset sweep.
  std::mt19937 rng(  // NOLINT(cert-msc51-cpp, cert-msc32-c)
      0x54494E59U);  // "TINY": deterministic seed is intentional in a test.
  platform::Shuffle(masks.begin(), masks.end(), rng);

  size_t planned_subsets = 0;
  for (const uint32_t mask : masks) {
    const bool has_scan = (mask & 0b00011U) != 0;
    const bool has_join = (mask & 0b11100U) != 0;
    if (!has_scan || !has_join) {
      continue;
    }

    OptimizerOptions options = OptimizerOptions::Default();
    for (size_t bit = 0; bit < optional_rules.size(); ++bit) {
      if ((mask & (1U << bit)) == 0) {
        options.disabled_implementation_rules.insert(optional_rules[bit]);
      }
    }

    const auto plan_or = Optimizer::Optimize(query, context, options);
    if (plan_or.GetStatus() == Status::kNotImplemented) {
      continue;
    }
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess) << "rule mask=" << mask;
    const Plan& plan = plan_or.Value();
    Executor executor = plan->EmitExecutor(context);
    Row row;
    std::vector<int64_t> keys;
    while (executor->Next(&row, nullptr)) {
      keys.push_back(row[0].value.int_value);
    }

    const bool ordered =
        plan->IsOrderedBy(query.order_expressions_, query.order_ascending_);
    const bool limited =
        plan->EnforcesLimit(query.limit_count_, query.limit_offset_);
    if (!ordered) {
      std::sort(keys.begin(), keys.end());
    }
    if (!limited) {
      const size_t begin = std::min(query.limit_offset_, keys.size());
      const size_t end = std::min(begin + query.limit_count_, keys.size());
      keys = std::vector<int64_t>(
          keys.begin() + static_cast<std::ptrdiff_t>(begin),
          keys.begin() + static_cast<std::ptrdiff_t>(end));
    }

    EXPECT_EQ(keys, (std::vector<int64_t>{1, 2})) << "rule mask=" << mask;
    ++planned_subsets;
  }

  EXPECT_GE(planned_subsets, 10U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, JoinChainMatchesGoldenResultUnderRuleSubsets) {
  // Phase 9 hardening: a four-relation chain whose result depends on getting
  // every join predicate applied exactly once. The default rule set captures
  // the golden output; every logical join-order subset crossed with every
  // physical-rule subset must reproduce it row for row.
  QueryData query{
      {"Sc4", "Sc1", "Sc2", "Sc3"},
      BinaryExpressionExp(
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc4", "c1")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("Sc1", "c1"))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("Sc2", "d1")))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc2", "d1")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("Sc3", "e1"))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc4", "c2")),
                                  BinaryOperation::kEquals,
                                  ConstantValueExp(Value("1"))))),
      {NamedExpression("sc4", ColumnName("Sc4", "c1")),
       NamedExpression("sc1", ColumnName("Sc1", "c1")),
       NamedExpression("sc3", ColumnName("Sc3", "e1"))}};
  query.order_expressions_ = {ColumnValueExp(ColumnName("Sc4", "c1"))};
  query.order_ascending_ = {true};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  auto drain = [&](const OptimizerOptions& options) {
    const auto plan_or = Optimizer::Optimize(query, context, options);
    if (plan_or.GetStatus() == Status::kNotImplemented) {
      return std::optional<std::vector<std::array<int64_t, 3>>>{};
    }
    EXPECT_EQ(plan_or.GetStatus(), Status::kSuccess);
    Executor executor = plan_or.Value()->EmitExecutor(context);
    std::vector<std::array<int64_t, 3>> rows;
    Row row;
    while (executor->Next(&row, nullptr)) {
      rows.push_back({row[0].value.int_value, row[1].value.int_value,
                      row[2].value.int_value});
    }
    return std::optional<std::vector<std::array<int64_t, 3>>>{std::move(rows)};
  };

  const auto golden = drain(OptimizerOptions::Default());
  ASSERT_TRUE(golden.has_value());
  if (!golden.has_value()) {
    return;
  }
  const std::vector<std::array<int64_t, 3>>& golden_rows = *golden;
  // Sc4.c1 values in the filtered chain are distinct and ascending, so the
  // golden output is a deterministic sequence.
  ASSERT_GE(golden_rows.size(), 5U);
  for (size_t i = 1; i < golden_rows.size(); ++i) {
    EXPECT_LT(golden_rows[i - 1][0], golden_rows[i][0]);
  }

  const std::array<std::string, 4> order_rules = {
      "join_commutativity", "join_enumeration", "join_associativity_left",
      "join_associativity_right"};
  const std::array<std::string, 5> physical_rules = {
      "index_scan", "full_scan", "hash_join", "index_join", "nested_loop_join"};

  std::vector<uint32_t> physical_masks;
  for (uint32_t mask = 0; mask < (1U << physical_rules.size()); ++mask) {
    const bool has_scan = (mask & 0b00011U) != 0;
    const bool has_join = (mask & 0b11100U) != 0;
    if (has_scan && has_join) {
      physical_masks.push_back(mask);
    }
  }
  // Fixed seed keeps failures reproducible.
  std::mt19937 rng(  // NOLINT(cert-msc51-cpp, cert-msc32-c)
      0x4A4F494EU);  // "JOIN": deterministic seed is intentional in a test.
  platform::Shuffle(physical_masks.begin(), physical_masks.end(), rng);

  size_t planned = 0;
  auto check = [&](const OptimizerOptions& options, uint32_t order_mask,
                   uint32_t physical_mask) {
    SCOPED_TRACE(::testing::Message() << "order_mask=" << order_mask
                                      << " physical_mask=" << physical_mask);
    const auto rows = drain(options);
    if (!rows.has_value()) {
      return;
    }
    EXPECT_EQ(*rows, *golden);
    ++planned;
  };

  // Dimension 1: every logical join-order subset with the full physical set.
  for (uint32_t order_mask = 0; order_mask < (1U << order_rules.size());
       ++order_mask) {
    OptimizerOptions options = OptimizerOptions::Default();
    for (size_t bit = 0; bit < order_rules.size(); ++bit) {
      if ((order_mask & (1U << bit)) != 0) {
        options.relational_rules.Remove(order_rules[bit]);
      }
    }
    check(options, order_mask, (1U << physical_rules.size()) - 1);
  }

  // Dimension 2: every physical-rule subset with the full logical set.
  for (const uint32_t physical_mask : physical_masks) {
    OptimizerOptions options = OptimizerOptions::Default();
    for (size_t bit = 0; bit < physical_rules.size(); ++bit) {
      if ((physical_mask & (1U << bit)) == 0) {
        options.disabled_implementation_rules.insert(physical_rules[bit]);
      }
    }
    check(options, 0, physical_mask);
  }

  EXPECT_GE(planned, 20U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, IndexOnlyScan) {
  // Arrange
  QueryData qd{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("c2-32"))),
      {NamedExpression("name", ColumnName("c2")),
       NamedExpression("score", ColumnName("c3"))}};

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, IndexOnlyScanInclude) {
  // Arrange
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

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, Join) {
  // Arrange
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("d1")),
      {NamedExpression("c2"), NamedExpression("d1"), NamedExpression("d3")}};

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, IndexScanJoin) {
  // Arrange
  QueryData qd{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                              ConstantValueExp(Value("c2-4")))),
      {NamedExpression("c2"), NamedExpression("d1"), NamedExpression("d3")}};

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, ThreeJoin) {
  // Arrange
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

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, JoinWhere) {
  // Arrange
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

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, SameNameColumn) {
  // Arrange
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

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, Asterisk) {
  // Arrange
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

  // Act + Assert: optimize + execute + dump
  ASSERT_SUCCESS(DumpAll(qd));
}

TEST_F(OptimizerTest, StrictRangePredicatesDriveIndexBounds) {
  // Arrange: Sc1 has an index on c1 (Sc1PK) plus a composite index on (c2, c3).
  // Strict inequalities, reversed operands and NOT-EQUALS exercise every branch
  // of Range::Update and the index prefix construction in ScanCandidates.
  TransactionContext context = rs_->BeginContext();
  auto run = [&](const Expression& predicate,
                 const std::vector<int64_t>& expected) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    ASSERT_SUCCESS(query.Rewrite(context));
    const auto plan_or = (Optimizer::Optimize(query, context));
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    const Plan& plan = plan_or.Value();
    Executor executor = plan->EmitExecutor(context);
    std::vector<int64_t> keys;
    Row row;
    while (executor->Next(&row, nullptr)) {
      keys.push_back(row[0].value.int_value);
    }
    EXPECT_EQ(keys, expected);
  };
  auto range = [](int64_t begin, int64_t end) {
    std::vector<int64_t> values;
    for (int64_t i = begin; i < end; ++i) {
      values.push_back(i);
    }
    return values;
  };

  // Act + Assert: NOT-EQUALS never narrows a range; upper/lower bounds and
  // constant-on-left comparisons all produce the matching key sets.
  std::vector<int64_t> all_but_five;
  for (int64_t i = 0; i < 100; ++i) {
    if (i != 5) {
      all_but_five.push_back(i);
    }
  }
  run(BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kNotEquals,
                          ConstantValueExp(Value(5))),
      all_but_five);
  run(BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(8))),
      range(0, 8));
  run(BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(2))),
      range(3, 100));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kLessThan, ColumnValueExp("c1")),
      range(3, 100));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kGreaterThan, ColumnValueExp("c1")),
      range(0, 2));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kGreaterThanEquals,
                          ColumnValueExp("c1")),
      range(0, 3));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kLessThanEquals,
                          ColumnValueExp("c1")),
      range(2, 100));
  ASSERT_SUCCESS(context.PreCommit());
}

// An equality prefix on the first key column of a composite index combined
// with a one-sided range on the trailing column produces begin/end vectors of
// DIFFERENT lengths (the short end acts as a prefix ceiling). The optimizer
// must still land on the index range and re-check the range conjunct with a
// residual Selection so boundary rows stay exact.
TEST_F(OptimizerTest, CompositeIndexEqualityPrefixWithOneSidedRange) {
  // Boundary cases for composite index key construction: an equality prefix
  // on the first key column plus a one-sided range on the second produces
  // begin/end vectors of DIFFERENT lengths (the short end acts as a prefix
  // ceiling). The scan predicate must keep boundary rows exact.
  // Sc1 carries KeyIdx(c2, c3); c2 = "c2-<i>", c3 = i + 9.9.
  TransactionContext context = rs_->BeginContext();
  auto run = [&](const Expression& predicate,
                 const std::vector<int64_t>& expected) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    ASSERT_SUCCESS(query.Rewrite(context));
    const auto plan_or = (Optimizer::Optimize(query, context));
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    const Plan& plan = plan_or.Value();
    Executor executor = plan->EmitExecutor(context);
    std::vector<int64_t> keys;
    Row row;
    while (executor->Next(&row, nullptr)) {
      keys.push_back(row[0].value.int_value);
    }
    EXPECT_EQ(keys, expected);
  };
  const auto eq_c2 = [](std::string_view value, BinaryOperation op,
                        double bound) {
    return BinaryExpressionExp(
        BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(std::string(value)))),
        BinaryOperation::kAnd,
        BinaryExpressionExp(ColumnValueExp("c3"), op,
                            ConstantValueExp(Value(bound))));
  };
  // Row i=5: c2="c2-5", c3=14.9. Equality on c2 pins the prefix; the range
  // column exercises [prefix+min] / [prefix] and [prefix] / [prefix+max]
  // mismatched key pairs. Bounds stay far from the stored value so the
  // assertions never depend on double rounding.
  run(eq_c2("c2-5", BinaryOperation::kLessThan, 20.0), {5});
  run(eq_c2("c2-5", BinaryOperation::kLessThanEquals, 15.0), {5});
  run(eq_c2("c2-5", BinaryOperation::kLessThan, 10.0), {});
  run(eq_c2("c2-5", BinaryOperation::kGreaterThan, 10.0), {5});
  run(eq_c2("c2-5", BinaryOperation::kGreaterThanEquals, 20.0), {});
  run(eq_c2("c2-6", BinaryOperation::kGreaterThan, 10.0), {6});
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, ResidualPredicateWrapsIndexScanInSelection) {
  // Arrange: the predicate touches c1 (indexed) and c2 (only reachable through
  // the composite index prefix), so the pushed predicate is not fully covered
  // by any single index key and must be wrapped in a SelectionPlan.
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(2))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                              ConstantValueExp(Value("c2-2")))),
      {NamedExpression("c1"), NamedExpression("c2"), NamedExpression("c3")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act + Assert: optimize + execute, the residual c1/c2 predicate still
  // produces exactly the matching row.
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream dump;
  dump << plan;
  // The optimizer may choose IndexScan+Selection or BitmapAnd; both are valid.
  EXPECT_TRUE(dump.str().find("Select:") != std::string::npos ||
              dump.str().find("BitmapAnd") != std::string::npos)
      << dump.str();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(2));
  EXPECT_EQ(row[1], Value("c2-2"));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, SelectStarSingleTableExpandsAllColumns) {
  // Arrange: an un-rewritten QueryData keeps the literal "*" so ExpandSelect
  // must turn it into every column of the single FROM table.
  QueryData query{{"Sc1"}, nullptr, {NamedExpression("*")}};
  TransactionContext context = rs_->BeginContext();

  // Act: optimize the raw query without QueryData::Rewrite (which would expand
  // the star itself).
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Assert: the projected schema has all three Sc1 columns and every row flows
  // through the executor.
  EXPECT_EQ(plan->GetSchema().ColumnCount(), 3U);
  Executor executor = plan->EmitExecutor(context);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    ++count;
  }
  EXPECT_EQ(count, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, SelectStarMultipleTablesExpandsAllColumns) {
  // Arrange: SELECT * over a join expands to every column of every FROM
  // table using their schema-qualified names (Phase 8).
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("Sc1.c1"), BinaryOperation::kEquals,
                          ColumnValueExp("Sc2.d1")),
      {NamedExpression("*")}};
  TransactionContext context = rs_->BeginContext();

  // Act: optimize the raw query without QueryData::Rewrite so the optimizer's
  // own star expansion runs.
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Assert: the projected schema holds all seven columns of Sc1 + Sc2 and
  // every joined row flows through the executor.
  EXPECT_EQ(plan->GetSchema().ColumnCount(), 7U);
  Executor executor = plan->EmitExecutor(context);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    ++count;
  }
  EXPECT_EQ(count, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OrderByCapsCostForIndexProvidedOrdering) {
  // Arrange: an equality on c2 leaves c3 as the ordering suffix of the
  // composite index (c2, c3), so the index-scan alternative reports that it
  // satisfies ORDER BY c3 and its cost is capped at 1.0.
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("c2-32"))),
      {NamedExpression("c1"), NamedExpression("c2")}};
  query.order_expressions_ = {ColumnValueExp("c3")};
  query.order_ascending_ = {true};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(32));
  EXPECT_EQ(row[1], Value("c2-32"));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, AggregateSelectBuildsAggregationPlan) {
  // Arrange: a SELECT list consisting only of aggregate expressions must be
  // capped with an AggregationPlan.
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("sum_score",
                       AggregateExpressionExp(AggregationType::kSum,
                                              ColumnValueExp("c3")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act + Assert: optimize + execute, the single matching row c1=2 has
  // c3 = 2 + 9.9 = 11.9.
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(11.9));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, MixedAggregateAndScalarSelectIsNotImplemented) {
  // Arrange: a SELECT list mixing aggregates with scalar columns is rejected.
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression(
           "sum_score",
           AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("c3"))),
       NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act: optimize the mixed aggregate + scalar query.
  StatusOr<Plan> result = Optimizer::Optimize(query, context);

  // Assert: the optimizer reports the feature as not implemented.
  EXPECT_FALSE(result.HasValue());
  EXPECT_EQ(result.GetStatus(), Status::kNotImplemented);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, ConstantLeftPredicatesWithoutCanonicalization) {
  // Arrange: the default "canonicalize_comparison" rule flips `2 < c1` into
  // `c1 > 2`. With an empty expression rule set the constant-left shape stays
  // intact and drives the reverse-direction Range::Update paths.
  TransactionContext context = rs_->BeginContext();
  OptimizerOptions options;
  options.relational_rules = cascades::RuleSet::Default();
  auto run = [&](const Expression& predicate,
                 const std::vector<int64_t>& expected) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    ASSERT_SUCCESS(query.Rewrite(context));
    const auto plan_or = (Optimizer::Optimize(query, context, options));
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    const Plan& plan = plan_or.Value();
    Executor executor = plan->EmitExecutor(context);
    std::vector<int64_t> keys;
    Row row;
    while (executor->Next(&row, nullptr)) {
      keys.push_back(row[0].value.int_value);
    }
    EXPECT_EQ(keys, expected);
  };
  auto range = [](int64_t begin, int64_t end) {
    std::vector<int64_t> values;
    for (int64_t i = begin; i < end; ++i) {
      values.push_back(i);
    }
    return values;
  };

  // Act + Assert: each constant-on-the-left comparison selects the same keys
  // as its flipped counterpart.
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kLessThan, ColumnValueExp("c1")),
      range(3, 100));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kLessThanEquals,
                          ColumnValueExp("c1")),
      range(2, 100));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kGreaterThan, ColumnValueExp("c1")),
      range(0, 2));
  run(BinaryExpressionExp(ConstantValueExp(Value(2)),
                          BinaryOperation::kGreaterThanEquals,
                          ColumnValueExp("c1")),
      range(0, 3));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, ExtraImplementationRuleIsRegistered) {
  // Arrange: an extra implementation rule is injected through
  // OptimizerOptions.extra_implementation_rules. Its implement returns no
  // alternatives, so the optimization result is unchanged.
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"), NamedExpression("c2")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  OptimizerOptions options = OptimizerOptions::Default();
  options.extra_implementation_rules.emplace_back(
      "extra_scan_probe", cascades::dsl::Scan(),
      [](cascades::GroupId, const cascades::Memo&, const cascades::Bindings&,
         const cascades::LogicalExpression&,
         const std::vector<cascades::BestPlan>&,
         const cascades::PhysicalProperties&, const cascades::RuleContext&) {
        return std::vector<cascades::PlanAlternative>{};
      });

  // Act + Assert: the customized rule set still optimizes and the query returns
  // the expected row.
  const auto plan_or = (Optimizer::Optimize(query, context, options));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(2));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, NotComparisonPredicateExecutesAfterPushdown) {
  // Arrange: NOT(c1 = 2) is normalized into c1 != 2 by not_comparison before
  // planning; execution must return every row except c1 == 2.
  QueryData query{
      {"Sc1"},
      UnaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(2))),
          UnaryOperation::kNot),
      {NamedExpression("c1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act + Assert: optimize + execute, expecting the 99 surviving keys.
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);
  std::vector<int64_t> keys;
  Row row;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  ASSERT_EQ(keys.size(), 99U);
  for (int64_t key : keys) {
    EXPECT_NE(key, 2);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, HashJoinPreferredOverCrossProductForEquiJoin) {
  // Arrange: a 100x200 equality join. The Phase 6 cost model (|L|*|R| local
  // cost for cross products versus |L|+|R| for hash joins) must prefer a
  // real join operator.
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("d1")),
      {NamedExpression("c1"), NamedExpression("d1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules.insert("zigzag_join");

  // Act
  const auto plan_or = (Optimizer::Optimize(query, context, options));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Assert: the chosen plan is a hash or index join, never a cross product.
  std::ostringstream dump;
  dump << plan;
  const std::string text = dump.str();
  EXPECT_TRUE(text.find("Hash Join") != std::string::npos ||
              text.find("Index Join") != std::string::npos ||
              text.find("MergeJoin") != std::string::npos)
      << text;
  EXPECT_EQ(text.find("Cross Join"), std::string::npos) << text;
  ASSERT_SUCCESS(context.PreCommit());
}

// The physical half of the dynamic filter contract: the implementation rule
// turns the logical probe-key annotation into a marker on the hash-join
// ProductPlan, but only for a shape the filter can actually pay for -- an
// inner, non null-safe, single INT64/DATE equi-join whose build side is
// materially smaller than the probe side. Everything else keeps the plain hash
// join.
TEST_F(OptimizerTest, DynamicFilterMarkerFollowsTheAnnotatedProfitableShape) {
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("d1")),
      {NamedExpression("c1"), NamedExpression("d1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules.insert("merge_join");
  options.disabled_implementation_rules.insert("zigzag_join");
  options.disabled_implementation_rules.insert("index_join");
  options.disabled_implementation_rules.insert("late_materialization_join");
  const auto plan_or = Optimizer::Optimize(query, context, options);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Sc1 has 100 rows and Sc2 200: the build side is not materially smaller, so
  // the filter is declined and the marker never appears in EXPLAIN.
  std::ostringstream dump;
  dump << plan;
  const std::string text = dump.str();
  // The marker is rendered by ProductPlan::Dump / ToString, so its absence
  // is exactly "no dynamic filter on this plan".
  EXPECT_NE(text.find("Hash Join"), std::string::npos) << text;
  EXPECT_EQ(text.find("dynamic_filter:build="), std::string::npos) << text;
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, DynamicFilterMarkerEngagesOnAProbeHeavyShape) {
  // Sc1 (100 rows) is the build side, and a subquery keeps Sc2 small enough to
  // be scanned twice: the join has a real filter opportunity, and the marker
  // must appear with the build side on the left.
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, sc1_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, sc2_stats,
                        context.GetStats("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, sc1_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, sc2_table,
                        context.GetTable("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*sc1_table, *sc1_stats);
  Plan right = std::make_shared<FullScanPlan>(*sc2_table, *sc2_stats);
  cascades::Memo memo;
  (void)memo.Build({"Sc1", "Sc2"});
  const cascades::GroupId left_group = memo.EnsureGroup({"Sc1"});
  const cascades::GroupId right_group = memo.EnsureGroup({"Sc2"});
  const cascades::GroupId join_group =
      memo.EnsureDerivedGroup({"Sc1", "Sc2"}, "root_join");
  memo.AddExpression(
      left_group,
      cascades::LogicalExpression{.operation = cascades::LogicalOperator::kScan,
                                  .table = "Sc1"});
  memo.AddExpression(
      right_group,
      cascades::LogicalExpression{.operation = cascades::LogicalOperator::kScan,
                                  .table = "Sc2"});
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  memo.AddExpression(
      join_group,
      cascades::LogicalExpression{.operation = cascades::LogicalOperator::kJoin,
                                  .children = {left_group, right_group},
                                  .predicate = predicate});

  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  search.Explore(join_group);
  const cascades::ImplementationRule* hash_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "hash_join") {
      hash_rule = &rule;
    }
  }
  ASSERT_NE(hash_rule, nullptr);

  // Both orientations are enumerated; the one whose build side is the small
  // relation is the only one a filter pays for. Each shape is handed to the
  // implementation rule bound to the memo group the rule reads the annotation
  // from, which is what a real search does.
  const auto marker_for = [&](double left_rows,
                              double right_rows) -> std::optional<bool> {
    cascades::Memo rebuilt;
    (void)rebuilt.Build({"Sc1", "Sc2"});
    const cascades::GroupId l = rebuilt.EnsureGroup({"Sc1"});
    const cascades::GroupId r = rebuilt.EnsureGroup({"Sc2"});
    const cascades::GroupId j =
        rebuilt.EnsureDerivedGroup({"Sc1", "Sc2"}, "root_join");
    rebuilt.AddExpression(j, cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kJoin,
                                 .children = {l, r},
                                 .predicate = predicate});
    cascades::SearchEngine engine(std::move(rebuilt),
                                  cascades::RuleSet::Default());
    engine.Explore(j);
    const cascades::Memo& annotated_memo = engine.GetMemo();
    // Implement the expression whose children[1] is the annotated probe group.
    for (const auto& expression : annotated_memo.Get(j).expressions) {
      if (expression.operation != cascades::LogicalOperator::kJoin ||
          expression.children.size() != 2) {
        continue;
      }
      if (annotated_memo.Get(expression.children[1])
              .dynamic_filter_keys.empty()) {
        continue;
      }
      const std::vector<cascades::BestPlan> children{
          cascades::BestPlan{.plan = left,
                             .estimated_rows = left_rows,
                             .group = expression.children[0]},
          cascades::BestPlan{.plan = right,
                             .estimated_rows = right_rows,
                             .group = expression.children[1]}};
      const auto alternatives = hash_rule->Apply(
          annotated_memo, j, expression, children,
          cascades::PhysicalProperties{}, cascades::RuleContext::Empty());
      for (const auto& alternative : alternatives) {
        const auto* product =
            dynamic_cast<const ProductPlan*>(alternative.plan.get());
        if (product != nullptr && product->PrefersDynamicFilter()) {
          return product->DynamicFilterBuildsLeft();
        }
      }
    }
    return std::nullopt;
  };

  // children[1] is the annotated probe, so the build is children[0]: the small
  // relation when the first shape is used, the large one otherwise.
  EXPECT_EQ(marker_for(/*left_rows=*/5.0, /*right_rows=*/40000.0),
            std::optional<bool>(true));
  EXPECT_EQ(marker_for(/*left_rows=*/40000.0, /*right_rows=*/5.0),
            std::nullopt);
  // Same orientation, both sides the same size: no shape is profitable enough.
  EXPECT_EQ(marker_for(/*left_rows=*/100.0, /*right_rows=*/200.0),
            std::nullopt);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, HashJoinUsesSmallerInputAsBuildSide) {
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ColumnValueExp("d1")),
      {NamedExpression("c1"), NamedExpression("d1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules.insert("merge_join");
  options.disabled_implementation_rules.insert("zigzag_join");
  options.disabled_implementation_rules.insert("index_join");
  options.disabled_implementation_rules.insert("late_materialization_join");
  const auto plan_or = Optimizer::Optimize(query, context, options);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  plan_or.Value()->Dump(dump, 0);
  const std::string text = dump.str();
  const size_t hash = text.find("Hash Join left:{");
  ASSERT_NE(hash, std::string::npos) << text;
  const size_t right = text.find("right:{", hash);
  ASSERT_NE(right, std::string::npos) << text;
  const size_t end = text.find('}', right);
  ASSERT_NE(end, std::string::npos) << text;
  EXPECT_NE(text.substr(right + 7, end - right - 7).find("c1"),
            std::string::npos)
      << text;

  Executor executor = plan_or.Value()->EmitExecutor(context);
  size_t rows = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    EXPECT_EQ(row[0], row[1]);
  }
  EXPECT_EQ(rows, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, UnqualifiedJoinBecomesExplicitCrossJoin) {
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(1)),
                  {NamedExpression(ColumnName("Sc1", "c1")),
                   NamedExpression(ColumnName("Sc2", "d1"))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_NE(dump.str().find("Cross Join"), std::string::npos) << dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, MergeJoinRuleUsesChildrenThatAlreadyProvideKeyOrder) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));

  Plan left_scan = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right_scan = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  Plan left_sorted = std::make_shared<SortPlan>(
      left_scan, std::vector<SortKey>{{ColumnValueExp(ColumnName("Sc1", "c1")),
                                       true, std::nullopt}});
  Plan right_sorted = std::make_shared<SortPlan>(
      right_scan, std::vector<SortKey>{{ColumnValueExp(ColumnName("Sc2", "d1")),
                                        true, std::nullopt}});

  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  const auto& initial = memo.Get(root).expressions.front();
  const Expression equality = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  cascades::LogicalExpression logical = initial;
  logical.predicate = equality;
  std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left_sorted,
          .estimated_rows = static_cast<double>(left_sorted->EmitRowCount())},
      cascades::BestPlan{
          .plan = right_sorted,
          .estimated_rows = static_cast<double>(right_sorted->EmitRowCount())}};

  const cascades::ImplementationRule* merge_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "merge_join") {
      merge_rule = &rule;
      break;
    }
  }
  ASSERT_NE(merge_rule, nullptr);
  const std::vector<cascades::PlanAlternative> alternatives = merge_rule->Apply(
      memo, root, logical, children, cascades::PhysicalProperties{},
      cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  EXPECT_NE(std::dynamic_pointer_cast<MergeJoinPlan>(alternatives.front().plan),
            nullptr);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, TopNCostUsesHeapCapacityUnlessTies) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        context.GetStats("Sc2"));
  Plan scan = std::make_shared<FullScanPlan>(*table, *stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc2"});
  cascades::LogicalExpression logical;
  logical.operation = cascades::LogicalOperator::kTopN;
  logical.children = {root};
  logical.target_list = {
      NamedExpression("", ColumnValueExp(ColumnName("Sc2", "d1")))};
  logical.sort_ascending = {true};
  logical.sort_nulls_first = {std::nullopt};
  logical.limit_count = 1;
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = scan, .estimated_rows = 200.0}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "topn") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const auto heap =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(heap.size(), 1U);
  logical.with_ties = true;
  const auto ties =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(ties.size(), 1U);
  EXPECT_LT(heap.front().local_cost, ties.front().local_cost);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OrderedInputsPreferMergeJoinCost) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));
  Plan left = std::make_shared<SortPlan>(
      std::make_shared<FullScanPlan>(*left_table, *left_stats),
      std::vector<SortKey>{
          {ColumnValueExp(ColumnName("Sc1", "c1")), true, std::nullopt}});
  Plan right = std::make_shared<SortPlan>(
      std::make_shared<FullScanPlan>(*right_table, *right_stats),
      std::vector<SortKey>{
          {ColumnValueExp(ColumnName("Sc2", "d1")), true, std::nullopt}});
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left, .estimated_rows = 10000.0},
      cascades::BestPlan{.plan = right, .estimated_rows = 20000.0}};
  const cascades::ImplementationRule* hash_rule = nullptr;
  const cascades::ImplementationRule* merge_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "hash_join") {
      hash_rule = &rule;
    } else if (rule.Name() == "merge_join") {
      merge_rule = &rule;
    }
  }
  ASSERT_NE(hash_rule, nullptr);
  ASSERT_NE(merge_rule, nullptr);
  const auto hash = hash_rule->Apply(memo, root, logical, children,
                                     cascades::PhysicalProperties{},
                                     cascades::RuleContext::Empty());
  const auto merge = merge_rule->Apply(memo, root, logical, children,
                                       cascades::PhysicalProperties{},
                                       cascades::RuleContext::Empty());
  ASSERT_FALSE(hash.empty());
  ASSERT_FALSE(merge.empty());
  double hash_cost = std::numeric_limits<double>::max();
  for (const auto& alternative : hash) {
    hash_cost = std::min(hash_cost, alternative.local_cost);
  }
  double merge_cost = std::numeric_limits<double>::max();
  for (const auto& alternative : merge) {
    merge_cost = std::min(merge_cost, alternative.local_cost);
  }
  EXPECT_LT(merge_cost, hash_cost);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, MergeJoinRuleSortsUnorderedChildren) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));

  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left,
          .estimated_rows = static_cast<double>(left->EmitRowCount())},
      cascades::BestPlan{
          .plan = right,
          .estimated_rows = static_cast<double>(right->EmitRowCount())}};

  const cascades::ImplementationRule* merge_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "merge_join") {
      merge_rule = &rule;
      break;
    }
  }
  ASSERT_NE(merge_rule, nullptr);
  const std::vector<cascades::PlanAlternative> alternatives = merge_rule->Apply(
      memo, root, logical, children, cascades::PhysicalProperties{},
      cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  const auto merge =
      std::dynamic_pointer_cast<MergeJoinPlan>(alternatives.front().plan);
  ASSERT_NE(merge, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(merge->Left()), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(merge->Right()), nullptr);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinRuleAddsMergeJoinAlternative) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.operation = cascades::LogicalOperator::kOuterJoin;
  logical.join_type = 0;  // LEFT
  logical.predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left,
          .estimated_rows = static_cast<double>(left->EmitRowCount())},
      cascades::BestPlan{
          .plan = right,
          .estimated_rows = static_cast<double>(right->EmitRowCount())}};

  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "outer_hash_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const auto alternatives =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  bool found_merge = false;
  for (const auto& alternative : alternatives) {
    const auto merge =
        std::dynamic_pointer_cast<MergeJoinPlan>(alternative.plan);
    if (merge != nullptr && merge->Kind() == LeftOuterJoinKind()) {
      found_merge = true;
      EXPECT_EQ(
          merge->GetSchema().ColumnCount(),
          left->GetSchema().ColumnCount() + right->GetSchema().ColumnCount());
    }
  }
  EXPECT_TRUE(found_merge);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, NullSafeOuterJoinUsesOnlySupportedHashLowering) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc2", "d1"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c2")),
                          BinaryOperation::kIsNotDistinctFrom,
                          ColumnValueExp(ColumnName("Sc2", "d3"))));
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left,
          .estimated_rows = static_cast<double>(left->EmitRowCount())},
      cascades::BestPlan{
          .plan = right,
          .estimated_rows = static_cast<double>(right->EmitRowCount())}};

  const cascades::ImplementationRule* outer_hash = nullptr;
  const cascades::ImplementationRule* merge = nullptr;
  const cascades::ImplementationRule* index = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "outer_hash_join") {
      outer_hash = &candidate;
    } else if (candidate.Name() == "merge_join") {
      merge = &candidate;
    } else if (candidate.Name() == "index_join") {
      index = &candidate;
    }
  }
  ASSERT_NE(outer_hash, nullptr);
  ASSERT_NE(merge, nullptr);
  ASSERT_NE(index, nullptr);

  logical.operation = cascades::LogicalOperator::kOuterJoin;
  logical.join_type = 0;
  const auto outer_alternatives = outer_hash->Apply(
      memo, root, logical, children, cascades::PhysicalProperties{},
      cascades::RuleContext::Empty());
  bool found_hash = false;
  for (const auto& alternative : outer_alternatives) {
    const auto product =
        std::dynamic_pointer_cast<ProductPlan>(alternative.plan);
    if (product == nullptr) {
      continue;
    }
    found_hash = true;
    EXPECT_EQ(product->Kind(), LeftOuterJoinKind());
    EXPECT_EQ(product->KeyNullSafe(), (std::vector<bool>{false, true}));
    EXPECT_EQ(std::dynamic_pointer_cast<MergeJoinPlan>(alternative.plan),
              nullptr);
  }
  EXPECT_TRUE(found_hash);
  EXPECT_TRUE(merge
                  ->Apply(memo, root, logical, children,
                          cascades::PhysicalProperties{},
                          cascades::RuleContext::Empty())
                  .empty());

  logical.operation = cascades::LogicalOperator::kJoin;
  cascades::RuleContext rule_context;
  rule_context.tables.emplace("Sc1", left_table);
  rule_context.tables.emplace("Sc2", right_table);
  rule_context.statistics.emplace("Sc1", left_stats);
  rule_context.statistics.emplace("Sc2", right_stats);
  EXPECT_TRUE(index
                  ->Apply(memo, root, logical, children,
                          cascades::PhysicalProperties{}, rule_context)
                  .empty());
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterHashCarriesResidualBeforeNullPadding) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.operation = cascades::LogicalOperator::kOuterJoin;
  logical.join_type = 0;
  logical.predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc2", "d1"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc2", "d4")),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{0}))));
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left,
          .estimated_rows = static_cast<double>(left->EmitRowCount())},
      cascades::BestPlan{
          .plan = right,
          .estimated_rows = static_cast<double>(right->EmitRowCount())}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "outer_hash_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const auto alternatives =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  bool found = false;
  for (const auto& alternative : alternatives) {
    const auto product =
        std::dynamic_pointer_cast<ProductPlan>(alternative.plan);
    if (product != nullptr && product->Kind() == LeftOuterJoinKind() &&
        product->ResidualNote()) {
      found = true;
    }
  }
  EXPECT_TRUE(found);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterHashResidualPadsWhenAllKeyMatchesFail) {
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 0;
  edge.on_condition = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc2", "d1"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(
          ColumnValueExp(ColumnName("Sc1", "c3")), BinaryOperation::kLessThan,
          BinaryExpressionExp(ColumnValueExp(ColumnName("Sc2", "d2")),
                              BinaryOperation::kSubtract,
                              ConstantValueExp(Value(1000000.0)))));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules = {"merge_join", "outer_nested_loop",
                                           "batch_nested_loop_outer",
                                           "index_join", "band_join"};
  const StatusOr<Plan> plan = Optimizer::Optimize(query, context, options);
  ASSERT_TRUE(plan.HasValue()) << plan.GetStatus().GetMessage();
  std::ostringstream dump;
  plan.Value()->Dump(dump, 0);
  EXPECT_NE(dump.str().find("Left Outer Join"), std::string::npos)
      << dump.str();
  Executor executor = plan.Value()->EmitExecutor(context);
  size_t rows = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    ASSERT_EQ(row.values_.size(), 2U);
    EXPECT_TRUE(row[1].IsNull());
  }
  EXPECT_EQ(rows, 100U);
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceLowersLeftJoinThroughMemo) {
  // M6: QueryData with a single LEFT edge optimizes to a LeftOuter physical
  // join. Sc2.d4 is constant 16, so left row c1 = 16 matches all 200 right
  // rows; the other 99 left rows must survive as NULL-padded rows.
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 0;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d4"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_NE(dump.str().find("Left Outer Join"), std::string::npos)
      << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  size_t rows = 0;
  size_t padded = 0;
  size_t matched = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    ASSERT_EQ(row.values_.size(), 2U);
    if (row[1].IsNull()) {
      ++padded;
      EXPECT_NE(row[0], Value(16));
    } else {
      ++matched;
      EXPECT_EQ(row[0], Value(16));
    }
  }
  EXPECT_EQ(rows, 299U);
  EXPECT_EQ(matched, 200U);
  EXPECT_EQ(padded, 99U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceKeepsRightSideFilterAboveJoin) {
  // M6 ON-vs-WHERE contract: a WHERE conjunct over the null-supplying side
  // filters AFTER padding. Sc1.c1 = Sc2.d1 matches c1 in 0..99 fully, so
  // WHERE d1 IS NULL must yield zero rows (no padded rows exist). Pushing
  // the predicate into the right scan would empty the build side and pad
  // every left row instead.
  QueryData query{
      {"Sc1", "Sc2"},
      UnaryExpressionExp(ColumnValueExp("d1"), UnaryOperation::kIsNull),
      {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 0;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceLowersRightJoinThroughMemo) {
  // M6+1: QueryData with a single RIGHT edge optimizes to a RightOuter
  // physical join (possibly normalized to LEFT inside the memo; rows
  // decide). Sc1.c1 = Sc2.d1 matches c1 in 0..99; the 100 unmatched right
  // rows (d1 = 100..199) survive as left-padded rows.
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 1;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  Executor executor = plan_or.Value()->EmitExecutor(context);
  size_t rows = 0;
  size_t padded = 0;
  size_t matched = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    ASSERT_EQ(row.values_.size(), 2U);
    if (row[0].IsNull()) {
      ++padded;
      EXPECT_EQ(row[1].value.int_value >= 100, true);
    } else {
      ++matched;
      EXPECT_EQ(row[0], row[1]);
    }
  }
  EXPECT_EQ(rows, 200U);
  EXPECT_EQ(matched, 100U);
  EXPECT_EQ(padded, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceKeepsLeftSideFilterAboveJoin) {
  // M6+1 ON-vs-WHERE mirror: a WHERE conjunct over the null-supplying
  // (left) side of a RIGHT join filters AFTER padding. ON c1 = d1 matches
  // c1 in 0..99, so WHERE c1 IS NULL must yield exactly the 100 padded
  // rows. Pushing it into the left scan would match nothing pre-join.
  QueryData query{
      {"Sc1", "Sc2"},
      UnaryExpressionExp(ColumnValueExp("c1"), UnaryOperation::kIsNull),
      {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 1;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    EXPECT_TRUE(row[0].IsNull());
    ++count;
  }
  EXPECT_EQ(count, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceLowersFullJoinThroughMemo) {
  // M6+1: single FULL edge with an equi ON lowers to a FullOuter hash join.
  // c1 = d1 matches 100 pairs; every left row matches, so only the 100
  // unmatched right rows (d1 = 100..199) pad. The equi pair keeps the
  // batch nested loop out (its gate refuses equi shapes).
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 2;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_NE(dump.str().find("Full Outer Join"), std::string::npos)
      << dump.str();
  EXPECT_EQ(dump.str().find("Batch"), std::string::npos) << dump.str();
  Executor executor = plan_or.Value()->EmitExecutor(context);
  size_t rows = 0;
  size_t left_padded = 0;
  size_t right_padded = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    ASSERT_EQ(row.values_.size(), 2U);
    if (row[0].IsNull()) {
      ++left_padded;
    } else if (row[1].IsNull()) {
      ++right_padded;
    } else {
      EXPECT_EQ(row[0], row[1]);
    }
  }
  EXPECT_EQ(rows, 200U);
  EXPECT_EQ(left_padded, 100U);
  EXPECT_EQ(right_padded, 0U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceFullNonEquiJoinUsesBatchNestedLoop) {
  // M6+1 meets batch_nested_loop: a non-equi FULL ON has no equi pair, so
  // hash/merge offer nothing and the blocked executor wins through the
  // real lowering. c1 > d1 matches 4950 pairs; c1 = 0 pads right (1 row)
  // and d1 = 99..199 pad left (101 rows).
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 2;
  edge.on_condition =
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kGreaterThan,
                          ColumnValueExp("d1"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream debug_plan;
  debug_plan << plan_or.Value();
  SCOPED_TRACE(debug_plan.str());
  Executor executor = plan_or.Value()->EmitExecutor(context);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("BatchNestedLoopJoin"), std::string::npos)
      << dump.str();
  size_t rows = 0;
  size_t left_padded = 0;
  size_t right_padded = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    ASSERT_EQ(row.values_.size(), 2U);
    if (row[0].IsNull()) {
      ++left_padded;
    } else if (row[1].IsNull()) {
      ++right_padded;
    }
  }
  EXPECT_EQ(rows, 5052U);
  EXPECT_EQ(left_padded, 101U);
  EXPECT_EQ(right_padded, 1U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceKeepsFullWhereAboveJoin) {
  // M6+1: FULL preserves neither side, so every WHERE stays above the
  // join. ON c1 = d1 leaves d1 = 100..199 padded; WHERE c1 IS NULL must
  // yield exactly those 100 left-padded rows. Pushing it into the left
  // scan would empty that input and pad every right row instead (200 rows
  // reaching down to d1 = 0).
  QueryData query{
      {"Sc1", "Sc2"},
      UnaryExpressionExp(ColumnValueExp("c1"), UnaryOperation::kIsNull),
      {NamedExpression("c1"), NamedExpression("d1")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 2;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
  query.outer_joins_.push_back(std::move(edge));
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream debug_plan;
  debug_plan << plan_or.Value();
  SCOPED_TRACE(debug_plan.str());
  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    EXPECT_TRUE(row[0].IsNull());
    ASSERT_FALSE(row[1].IsNull());
    EXPECT_LE(100, row[1].value.int_value);
    ++count;
  }
  EXPECT_EQ(count, 100U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceRejectsChainsAndAggregates) {
  // A left-deep LEFT chain plans through Cascades. RIGHT/FULL chains stay
  // on the relational path, as do single outer joins under aggregation:
  // the optimizer reports kNotImplemented instead of planning them unsoundly.
  for (uint8_t kind : {uint8_t{0}, uint8_t{1}, uint8_t{2}}) {
    QueryData query{{"Sc1", "Sc2", "Sc3"},
                    ConstantValueExp(Value(true)),
                    {NamedExpression("c1")}};
    for (size_t right : {size_t{1}, size_t{2}}) {
      QueryData::OuterJoinEdge edge;
      edge.right_index = right;
      edge.join_kind = kind;
      edge.on_condition = BinaryExpressionExp(
          ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
      query.outer_joins_.push_back(std::move(edge));
    }
    TransactionContext context = rs_->BeginContext();
    ASSERT_SUCCESS(query.Rewrite(context));
    const Status status = Optimizer::Optimize(query, context).GetStatus();
    if (kind == 0) {
      EXPECT_EQ(status, Status::kSuccess);
    } else {
      EXPECT_EQ(status, Status::kNotImplemented);
    }
  }
  for (uint8_t kind : {uint8_t{0}, uint8_t{1}, uint8_t{2}}) {
    QueryData query{
        {"Sc1", "Sc2"},
        ConstantValueExp(Value(true)),
        {NamedExpression("n", AggregateExpressionExp(AggregationType::kCount,
                                                     ColumnValueExp("c1")))}};
    QueryData::OuterJoinEdge edge;
    edge.right_index = 1;
    edge.join_kind = kind;
    edge.on_condition = BinaryExpressionExp(
        ColumnValueExp("c1"), BinaryOperation::kEquals, ColumnValueExp("d1"));
    query.outer_joins_.push_back(std::move(edge));
    TransactionContext context = rs_->BeginContext();
    ASSERT_SUCCESS(query.Rewrite(context));
    EXPECT_EQ(Optimizer::Optimize(query, context).GetStatus(),
              Status::kNotImplemented);
  }
  ASSERT_SUCCESS(rs_->BeginContext().PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceEliminatesUnusedUniqueRightSide) {
  // M6: nothing above the join names the right side and the equi-key is
  // UNIQUE there, so each preserved row matches at most once and the LEFT
  // join is the identity over the preserved side.
  TransactionContext context = rs_->BeginContext();
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, parent,
        rs_->CreateTable(
            context,
            Schema("OjParent", {Column("id", ValueType::kInt64,
                                       Constraint(Constraint::kPrimaryKey)),
                                Column("v", ValueType::kInt64)})));
    for (int i = 1; i <= 3; ++i) {
      ASSERT_SUCCESS(parent.Insert(context.txn_, Row({Value(i), Value(i * 10)}))
                         .GetStatus());
    }
  }
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, child,
        rs_->CreateTable(
            context,
            Schema("OjChild", {Column("id", ValueType::kInt64,
                                      Constraint(Constraint::kPrimaryKey)),
                               Column("w", ValueType::kInt64)})));
    ASSERT_SUCCESS(
        child.Insert(context.txn_, Row({Value(2), Value(200)})).GetStatus());
  }
  QueryData query{{"OjParent", "OjChild"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("v")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 0;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp(ColumnName("OjParent", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("OjChild", "id")));
  query.outer_joins_.push_back(std::move(edge));
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("Join"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  std::vector<int64_t> values;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row.values_.size(), 1U);
    values.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(values, (std::vector<int64_t>{10, 20, 30}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceKeepsJoinWithoutUniquenessProof) {
  // Same shape but the equi-key carries no uniqueness proof: dropping the
  // join could collapse duplicate matches, so the outer join stays.
  TransactionContext context = rs_->BeginContext();
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, parent,
        rs_->CreateTable(
            context,
            Schema("OjParentNu", {Column("id", ValueType::kInt64,
                                         Constraint(Constraint::kPrimaryKey)),
                                  Column("v", ValueType::kInt64)})));
    for (int i = 1; i <= 2; ++i) {
      ASSERT_SUCCESS(parent.Insert(context.txn_, Row({Value(i), Value(i * 10)}))
                         .GetStatus());
    }
  }
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, child,
        rs_->CreateTable(
            context, Schema("OjChildNu", {Column("id", ValueType::kInt64),
                                          Column("w", ValueType::kInt64)})));
    ASSERT_SUCCESS(
        child.Insert(context.txn_, Row({Value(1), Value(100)})).GetStatus());
    ASSERT_SUCCESS(
        child.Insert(context.txn_, Row({Value(1), Value(101)})).GetStatus());
  }
  QueryData query{{"OjParentNu", "OjChildNu"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("v")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 0;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp(ColumnName("OjParentNu", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("OjChildNu", "id")));
  query.outer_joins_.push_back(std::move(edge));
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_NE(dump.str().find("Left Outer Join"), std::string::npos)
      << dump.str();

  // v = 10 matches twice (multiplied), v = 20 pads: elimination would have
  // reported 2 rows instead of 3.
  Executor executor = plan_or.Value()->EmitExecutor(context);
  size_t rows = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 3U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceEliminatesUnusedUniqueLeftSideForRight) {
  // M6+1 mirror: nothing above the join names the left side of a RIGHT join
  // and the equi-key is UNIQUE there, so the RIGHT join is the identity
  // over the preserved (right) side.
  TransactionContext context = rs_->BeginContext();
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, parent,
        rs_->CreateTable(
            context,
            Schema("RjParent", {Column("id", ValueType::kInt64,
                                       Constraint(Constraint::kPrimaryKey)),
                                Column("v", ValueType::kInt64)})));
    for (int i = 1; i <= 3; ++i) {
      ASSERT_SUCCESS(parent.Insert(context.txn_, Row({Value(i), Value(i * 10)}))
                         .GetStatus());
    }
  }
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, child,
        rs_->CreateTable(
            context,
            Schema("RjChild", {Column("id", ValueType::kInt64,
                                      Constraint(Constraint::kPrimaryKey)),
                               Column("w", ValueType::kInt64)})));
    ASSERT_SUCCESS(
        child.Insert(context.txn_, Row({Value(2), Value(200)})).GetStatus());
  }
  QueryData query{{"RjChild", "RjParent"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("v")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 1;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp(ColumnName("RjChild", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("RjParent", "id")));
  query.outer_joins_.push_back(std::move(edge));
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_EQ(dump.str().find("Join"), std::string::npos) << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  std::vector<int64_t> values;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row.values_.size(), 1U);
    values.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(values, (std::vector<int64_t>{10, 20, 30}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OuterJoinSliceKeepsFullJoinWithoutElimination) {
  // M6+1: FULL preserves both sides, so even a unique unreferenced side
  // cannot be eliminated. RjChild {2} FULL JOIN RjParent {1,2,3} yields the
  // id = 2 match plus the two unmatched parent rows.
  TransactionContext context = rs_->BeginContext();
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, parent,
        rs_->CreateTable(
            context,
            Schema("FjParent", {Column("id", ValueType::kInt64,
                                       Constraint(Constraint::kPrimaryKey)),
                                Column("v", ValueType::kInt64)})));
    for (int i = 1; i <= 3; ++i) {
      ASSERT_SUCCESS(parent.Insert(context.txn_, Row({Value(i), Value(i * 10)}))
                         .GetStatus());
    }
  }
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, child,
        rs_->CreateTable(
            context,
            Schema("FjChild", {Column("id", ValueType::kInt64,
                                      Constraint(Constraint::kPrimaryKey)),
                               Column("w", ValueType::kInt64)})));
    ASSERT_SUCCESS(
        child.Insert(context.txn_, Row({Value(2), Value(200)})).GetStatus());
  }
  QueryData query{{"FjChild", "FjParent"},
                  ConstantValueExp(Value(true)),
                  {NamedExpression("v")}};
  QueryData::OuterJoinEdge edge;
  edge.right_index = 1;
  edge.join_kind = 2;
  edge.on_condition = BinaryExpressionExp(
      ColumnValueExp(ColumnName("FjChild", "id")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("FjParent", "id")));
  query.outer_joins_.push_back(std::move(edge));
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_NE(dump.str().find("Full Outer Join"), std::string::npos)
      << dump.str();

  Executor executor = plan_or.Value()->EmitExecutor(context);
  std::vector<int64_t> values;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row.values_.size(), 1U);
    ASSERT_FALSE(row[0].IsNull());
    values.push_back(row[0].value.int_value);
  }
  std::sort(values.begin(), values.end());
  EXPECT_EQ(values, (std::vector<int64_t>{10, 20, 30}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, MergeSemiJoinRuleBuildsSortedProbeOnlyPlan) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical;
  logical.operation = cascades::LogicalOperator::kSemiJoin;
  logical.children = memo.Get(root).expressions.front().children;
  logical.predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left,
          .estimated_rows = static_cast<double>(left->EmitRowCount())},
      cascades::BestPlan{
          .plan = right,
          .estimated_rows = static_cast<double>(right->EmitRowCount())}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "semi_merge_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const auto alternatives =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  const auto merge =
      std::dynamic_pointer_cast<MergeJoinPlan>(alternatives.front().plan);
  ASSERT_NE(merge, nullptr);
  EXPECT_EQ(merge->Kind(), SemiJoinKind());
  EXPECT_EQ(merge->GetSchema().ColumnCount(), left->GetSchema().ColumnCount());
  ASSERT_SUCCESS(context.PreCommit());
}

// A semi join whose predicate mixes the equality with an inequality keeps a
// merge alternative: the inequality rides along as the plan's residual
// instead of disqualifying merge join entirely.
TEST_F(OptimizerTest, MergeSemiJoinRuleCarriesInequalityResidual) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, left_table,
                        context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_table,
                        context.GetTable("Sc2"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, left_stats,
                        context.GetStats("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, right_stats,
                        context.GetStats("Sc2"));
  Plan left = std::make_shared<FullScanPlan>(*left_table, *left_stats);
  Plan right = std::make_shared<FullScanPlan>(*right_table, *right_stats);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"Sc1", "Sc2"});
  cascades::LogicalExpression logical;
  logical.operation = cascades::LogicalOperator::kSemiJoin;
  logical.children = memo.Get(root).expressions.front().children;
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  predicate = BinaryExpressionExp(
      predicate, BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc2", "d4")),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(16))));
  logical.predicate = predicate;
  std::vector<cascades::BestPlan> children{
      cascades::BestPlan{
          .plan = left,
          .estimated_rows = static_cast<double>(left->EmitRowCount())},
      cascades::BestPlan{
          .plan = right,
          .estimated_rows = static_cast<double>(right->EmitRowCount())}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "semi_merge_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);
  const auto alternatives =
      rule->Apply(memo, root, logical, children, cascades::PhysicalProperties{},
                  cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  const auto merge =
      std::dynamic_pointer_cast<MergeJoinPlan>(alternatives.front().plan);
  ASSERT_NE(merge, nullptr);
  EXPECT_EQ(merge->Kind(), SemiJoinKind());
  ASSERT_TRUE(merge->Residual());
  EXPECT_NE(merge->Residual()->ToString().find(">="), std::string::npos)
      << merge->Residual()->ToString();
  EXPECT_NE(merge->ToString().find("residual"), std::string::npos)
      << merge->ToString();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, SortDistinctRuleAddsSortForUnorderedInput) {
  TransactionContext context = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                        context.GetStats("Sc1"));
  Plan scan = std::make_shared<FullScanPlan>(*table, *stats);

  cascades::Memo memo;
  const cascades::GroupId child_group = memo.Build({"Sc1"});
  cascades::LogicalExpression logical;
  logical.operation = cascades::LogicalOperator::kDistinct;
  logical.children = {child_group};
  std::vector<cascades::BestPlan> children{cascades::BestPlan{
      .plan = scan,
      .estimated_rows = static_cast<double>(scan->EmitRowCount())}};

  const cascades::ImplementationRule* sort_distinct_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "sort_distinct") {
      sort_distinct_rule = &rule;
      break;
    }
  }
  ASSERT_NE(sort_distinct_rule, nullptr);
  const auto alternatives = sort_distinct_rule->Apply(
      memo, child_group, logical, children, cascades::PhysicalProperties{},
      cascades::RuleContext::Empty());
  ASSERT_EQ(alternatives.size(), 1U);
  const auto distinct =
      std::dynamic_pointer_cast<SortDistinctPlan>(alternatives.front().plan);
  ASSERT_NE(distinct, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SortPlan>(distinct->Child()), nullptr);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OptimizeWithoutFromUsesDummyScanForConstantProjection) {
  QueryData query;
  query.select_ = {NamedExpression("answer", ConstantValueExp(Value(42)))};
  query.where_ = ConstantValueExp(Value(1));
  TransactionContext context = rs_->BeginContext();

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  ASSERT_NE(plan_or.Value(), nullptr);
  EXPECT_NE(plan_or.Value()->ToString().find("Project"), std::string::npos);
  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(42)}));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OptimizeWithoutFromFalsePredicateProducesNoRows) {
  QueryData query;
  query.select_ = {NamedExpression("answer", ConstantValueExp(Value(42)))};
  query.where_ = ConstantValueExp(Value(0));
  TransactionContext context = rs_->BeginContext();

  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  Executor executor = plan_or.Value()->EmitExecutor(context);
  Row row;
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, CrossTableResidualPredicateIsAppliedByTheJoin) {
  // Arrange: an equi-join conjunct plus a non-equi cross-table conjunct. The
  // residual must be applied by the join's Selection wrap now that the root
  // SelectionPlan fallback is gone (Phase 4).
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                              ColumnValueExp("d1")),
          BinaryOperation::kAnd,
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("c3"), BinaryOperation::kAdd,
                                  ColumnValueExp("d2")),
              BinaryOperation::kGreaterThan, ConstantValueExp(Value(100.0)))),
      {NamedExpression("c1"), NamedExpression("d1")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);

  // Assert: joined rows have c1 == d1 and c3 + d2 > 100. With c3 = i + 9.9
  // and d2 = i + 0.2 the surviving keys are i >= 45, i.e. 55 rows.
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row[0], row[1]);
    ASSERT_TRUE(row[0].value.int_value >= 45);
    ++count;
  }
  EXPECT_EQ(count, 55U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, BatchNestedLoopExecutesNonEquiInnerJoin) {
  // Arrange: a pure non-equi join predicate (no equi pair, so hash/merge
  // offer nothing) with selective single-table filters. c1 = 99 can only
  // pair with d1 = 100, so exactly one row survives.
  auto query = [&] {
    return QueryData{
        {"Sc1", "Sc2"},
        BinaryExpressionExp(
            BinaryExpressionExp(ColumnValueExp("c1"),
                                BinaryOperation::kLessThan,
                                ColumnValueExp("d1")),
            BinaryOperation::kAnd,
            BinaryExpressionExp(
                BinaryExpressionExp(ColumnValueExp("c1"),
                                    BinaryOperation::kGreaterThanEquals,
                                    ConstantValueExp(Value(99))),
                BinaryOperation::kAnd,
                BinaryExpressionExp(ColumnValueExp("d1"),
                                    BinaryOperation::kLessThanEquals,
                                    ConstantValueExp(Value(100))))),
        {NamedExpression("c1"), NamedExpression("d1")}};
  };
  {
    // Act: disable the tuple nested loop so the blocked executor must win.
    QueryData q = query();
    TransactionContext context = rs_->BeginContext();
    ASSERT_SUCCESS(q.Rewrite(context));
    OptimizerOptions options = OptimizerOptions::Default();
    options.disabled_implementation_rules.insert("nested_loop_join");
    options.disabled_implementation_rules.insert("band_join");
    const auto plan_or = Optimizer::Optimize(q, context, options);
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    Executor executor = plan_or.Value()->EmitExecutor(context);
    std::ostringstream dump;
    executor->Dump(dump, 0);
    EXPECT_NE(dump.str().find("BatchNestedLoopJoin"), std::string::npos)
        << dump.str();

    // Assert: exactly (99, 100).
    Row row;
    ASSERT_TRUE(executor->Next(&row, nullptr));
    EXPECT_EQ(row[0].value.int_value, 99);
    EXPECT_EQ(row[1].value.int_value, 100);
    EXPECT_FALSE(executor->Next(&row, nullptr));
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    // Act: selective index bounds make BandJoin cheaper by default.
    QueryData q = query();
    TransactionContext context = rs_->BeginContext();
    ASSERT_SUCCESS(q.Rewrite(context));
    const auto plan_or = Optimizer::Optimize(q, context);
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    std::ostringstream plan_dump;
    plan_dump << plan_or.Value();
    EXPECT_NE(plan_dump.str().find("BandJoin"), std::string::npos)
        << plan_dump.str();
    Executor executor = plan_or.Value()->EmitExecutor(context);
    std::ostringstream dump;
    executor->Dump(dump, 0);
    EXPECT_NE(dump.str().find("BandJoin"), std::string::npos) << dump.str();

    // Assert: same single row.
    Row row;
    ASSERT_TRUE(executor->Next(&row, nullptr));
    EXPECT_EQ(row[0].value.int_value, 99);
    EXPECT_EQ(row[1].value.int_value, 100);
    EXPECT_FALSE(executor->Next(&row, nullptr));
    ASSERT_SUCCESS(context.PreCommit());
  }
}

TEST_F(OptimizerTest, BatchNestedLoopGateKeepsEquiJoins) {
  // Arrange: with hash/merge/index/tuple-loop all disabled, an equi join
  // must fail to plan (the batch gate refuses equi pairs) while a non-equi
  // join still plans through the blocked executor.
  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules.insert("hash_join");
  options.disabled_implementation_rules.insert("merge_join");
  options.disabled_implementation_rules.insert("index_join");
  options.disabled_implementation_rules.insert("nested_loop_join");
  options.disabled_implementation_rules.insert("band_join");
  options.disabled_implementation_rules.insert("zigzag_join");
  {
    QueryData query{
        {"Sc1", "Sc2"},
        BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                            ColumnValueExp("d1")),
        {NamedExpression("c1"), NamedExpression("d1")}};
    TransactionContext context = rs_->BeginContext();
    ASSERT_SUCCESS(query.Rewrite(context));
    EXPECT_EQ(Optimizer::Optimize(query, context, options).GetStatus(),
              Status::kNotImplemented);
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    QueryData query{
        {"Sc1", "Sc2"},
        BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kLessThan,
                            ColumnValueExp("d1")),
        {NamedExpression("c1"), NamedExpression("d1")}};
    TransactionContext context = rs_->BeginContext();
    ASSERT_SUCCESS(query.Rewrite(context));
    const auto plan_or = Optimizer::Optimize(query, context, options);
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    std::ostringstream dump;
    plan_or.Value()->EmitExecutor(context)->Dump(dump, 0);
    EXPECT_NE(dump.str().find("BatchNestedLoopJoin"), std::string::npos)
        << dump.str();
    ASSERT_SUCCESS(context.PreCommit());
  }
}

TEST_F(OptimizerTest, BatchNestedLoopCoversNonEquiSemiAntiAndFullJoin) {
  // Arrange: memo-level non-equi semi/anti/full joins (c1 > d1). Semi keeps
  // c1 = 1..99 (99 rows), anti keeps only c1 = 0, and the full join emits
  // 4950 matches plus every unmatched row padded (c1 = 0 on the left,
  // d1 = 99..199 on the right).
  TransactionContext context = rs_->BeginContext();
  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  for (const char* relation : {"Sc1", "Sc2"}) {
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          context.GetTable(relation));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, statistics,
                          context.GetStats(relation));
    rule_context.tables.emplace(relation, std::move(table));
    rule_context.statistics.emplace(relation, std::move(statistics));
  }
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("Sc1", "c1")), BinaryOperation::kGreaterThan,
      ColumnValueExp(ColumnName("Sc2", "d1")));
  struct Case {
    cascades::LogicalOperator operation;
    uint8_t join_type;
    JoinKind kind;
    size_t expected_rows;
    size_t expected_left_nulls;
  };
  const std::vector<Case> cases = {
      {cascades::LogicalOperator::kSemiJoin, 0, SemiJoinKind(), 99U, 0U},
      {cascades::LogicalOperator::kAntiJoin, 0, AntiJoinKind(), 1U, 0U},
      {cascades::LogicalOperator::kOuterJoin, 2, FullOuterJoinKind(), 5052U,
       101U},
  };
  for (const Case& test_case : cases) {
    cascades::Memo memo;
    // Register the relations in the memo join graph (outer-join planning
    // probes it; the derived group below carries the join itself).
    (void)memo.Build({"Sc1", "Sc2"});
    const cascades::GroupId left = memo.EnsureGroup({"Sc1"});
    const cascades::GroupId right = memo.EnsureGroup({"Sc2"});
    const cascades::GroupId derived =
        memo.EnsureDerivedGroup({"Sc1", "Sc2"}, "kinded-join");
    ASSERT_TRUE(memo.AddExpression(
        derived,
        cascades::LogicalExpression{.operation = test_case.operation,
                                    .children = {left, right},
                                    .predicate = predicate,
                                    .join_type = test_case.join_type}));
    const cascades::RuleSet no_rules;
    cascades::SearchEngine search(std::move(memo), no_rules);
    const auto best =
        search.Optimize(derived, cascades::PhysicalProperties{},
                        DefaultImplementationRules(), rule_context);
    ASSERT_TRUE(best.has_value());
    if (!best.has_value()) {
      continue;
    }
    const auto product = std::dynamic_pointer_cast<ProductPlan>(best->plan);
    ASSERT_NE(product, nullptr);
    EXPECT_EQ(product->Kind(), test_case.kind);
    EXPECT_TRUE(product->PrefersBatchNestedLoop());
    Executor executor = best->plan->EmitExecutor(context);
    std::ostringstream dump;
    executor->Dump(dump, 0);
    EXPECT_NE(dump.str().find("BatchNestedLoopJoin"), std::string::npos)
        << dump.str();
    Row row;
    size_t count = 0;
    size_t left_nulls = 0;
    while (executor->Next(&row, nullptr)) {
      if (row[0].IsNull()) {
        ++left_nulls;
      }
      ++count;
    }
    EXPECT_EQ(count, test_case.expected_rows);
    EXPECT_EQ(left_nulls, test_case.expected_left_nulls);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, LimitWithOrderedIndexStreamsOnlyTopKRows) {
  // Arrange: a range on the indexed column c1 with ORDER BY c1 and LIMIT 5.
  // The Sc1PK index delivers the ordering, so the optimizer folds LIMIT into
  // the plan and the lazy pipeline stops after five rows (Top-K, Phase 5).
  QueryData query{{"Sc1"},
                  BinaryExpressionExp(ColumnValueExp("c1"),
                                      BinaryOperation::kGreaterThanEquals,
                                      ConstantValueExp(Value(80))),
                  {NamedExpression("c1")}};
  query.order_expressions_ = {ColumnValueExp("c1")};
  query.order_ascending_ = {true};
  query.limit_count_ = 5;
  query.limit_offset_ = 0;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream dump;
  dump << plan;
  const std::string text = dump.str();
  EXPECT_NE(text.find("Limit"), std::string::npos) << text;
  EXPECT_NE(text.find("Index"), std::string::npos) << text;
  Executor executor = plan->EmitExecutor(context);

  // Assert: exactly the first five keys of the ordered range arrive.
  Row row;
  std::vector<int64_t> keys;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(keys, (std::vector<int64_t>{80, 81, 82, 83, 84}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, UnboundedIndexProvidesAscendingAndDescendingOrder) {
  TransactionContext context = rs_->BeginContext();
  const auto optimize = [&](bool ascending) {
    QueryData query{
        {"Sc1"}, nullptr, {NamedExpression("key", ColumnName("Sc1", "c1"))}};
    query.order_expressions_ = {ColumnValueExp(ColumnName("Sc1", "c1"))};
    query.order_ascending_ = {ascending};
    query.limit_count_ = 1;
    EXPECT_EQ(query.Rewrite(context), Status::kSuccess);
    return Optimizer::Optimize(query, context);
  };

  for (const bool ascending : {true, false}) {
    const auto plan_or = optimize(ascending);
    ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
    std::ostringstream dump;
    dump << *plan_or.Value();
    EXPECT_NE(dump.str().find("IndexOnlyScan"), std::string::npos)
        << dump.str();
    EXPECT_EQ(dump.str().find("TopN"), std::string::npos) << dump.str();
    EXPECT_EQ(plan_or.Value()->IsOrderedBy(
                  {ColumnValueExp(ColumnName("Sc1", "c1"))}, {ascending}),
              true)
        << dump.str();
    Executor executor = plan_or.Value()->EmitExecutor(context);
    Row row;
    ASSERT_TRUE(executor->Next(&row, nullptr));
    EXPECT_EQ(row[0], Value(ascending ? 0 : 99));
    EXPECT_FALSE(executor->Next(&row, nullptr));
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, SelfJoinWithAliasesUsesRelationSchemas) {
  // Phase 8: two aliases of one physical table join through the memo. Scan
  // implementations rename their output schemas to the alias identity so
  // `a.c1` / `b.c1` stay distinguishable end-to-end, while the physical PK
  // index still drives range selection on the filtered side.
  QueryData query{
      {"a", "b"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("b", "c1"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("b", "c1")),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(95)))),
      {NamedExpression("ac3", ColumnValueExp(ColumnName("a", "c3")))}};
  query.aliases_ = {{"a", "Sc1"}, {"b", "Sc1"}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<double> sums;
  while (executor->Next(&row, nullptr)) {
    sums.push_back(row[0].value.double_value);
  }
  // Pairs with c1 in [95, 99]: a.c3 = c1 + 9.9.
  std::ranges::sort(sums);
  const std::vector<double> expected{104.9, 105.9, 106.9, 107.9, 108.9};
  EXPECT_EQ(sums.size(), expected.size());
  for (size_t i = 0; i < std::min(sums.size(), expected.size()); ++i) {
    EXPECT_DOUBLE_EQ(sums[i], expected[i]);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, FilteredSelfJoinKeepsRightSidePredicate) {
  // Phase 8 follow-up: the IndexJoin executor bypasses its right child plan
  // entirely, so an index join must never be offered when the right scan
  // group carries a pushed filter -- otherwise that filter silently
  // disappears. Whichever join strategy wins, the predicate holds.
  QueryData query{
      {"a", "b"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("b", "c1"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("b", "c3")),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(107.0)))),
      {NamedExpression("ac3", ColumnValueExp(ColumnName("a", "c3")))}};
  query.aliases_ = {{"a", "Sc1"}, {"b", "Sc1"}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<double> sums;
  while (executor->Next(&row, nullptr)) {
    sums.push_back(row[0].value.double_value);
  }
  std::sort(sums.begin(), sums.end());
  // b.c3 > 107 pins b.c1 to {98, 99}; the equality join copies those to a.
  EXPECT_EQ(sums, (std::vector<double>{107.9, 108.9}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, FilteredSelfJoinStillPlansThroughTheIndexNestedLoop) {
  // The index nested-loop bypasses its right child plan, so a filter merged
  // into the right scan group is not enforced by that child. The rule used to
  // refuse the shape outright; it now carries the inner filter into the
  // residual instead. With hash/NL joins disabled this is the only join left,
  // so it pins both halves of that contract at once: the plan really is the
  // probe, and the filter still decides the rows.
  QueryData query{
      {"a", "b"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("b", "c1"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("b", "c3")),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(107.0)))),
      {NamedExpression("ac3", ColumnValueExp(ColumnName("a", "c3")))}};
  query.aliases_ = {{"a", "Sc1"}, {"b", "Sc1"}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules = {"hash_join", "nested_loop_join"};
  ASSIGN_OR_ASSERT_FAIL(Plan, plan,
                        Optimizer::Optimize(query, context, options));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("Index Join"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<double> sums;
  while (executor->Next(&row, nullptr)) {
    sums.push_back(row[0].value.double_value);
  }
  std::sort(sums.begin(), sums.end());
  // b.c3 > 107 pins b.c1 to {98, 99}; the equality join copies those to a.
  // Without the inner filter carried into the residual the probe would also
  // return every lower key, so this vector IS the filter's evidence.
  EXPECT_EQ(sums, (std::vector<double>{107.9, 108.9}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, AliasedSelfJoinCanUseIndexNestedLoop) {
  // Phase 8 follow-up: with hash/NL joins disabled the aliased self-join
  // must still plan through the index nested-loop path, which resolves keys
  // physically while declaring renamed output columns.
  QueryData query{
      {"a", "b"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("b", "c1"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                              BinaryOperation::kLessThan,
                              ConstantValueExp(Value(5)))),
      {NamedExpression("ac3", ColumnValueExp(ColumnName("a", "c3")))}};
  query.aliases_ = {{"a", "Sc1"}, {"b", "Sc1"}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  OptimizerOptions options = OptimizerOptions::Default();
  options.disabled_implementation_rules = {"hash_join", "nested_loop_join"};
  ASSIGN_OR_ASSERT_FAIL(Plan, plan,
                        Optimizer::Optimize(query, context, options));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("Rename"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<double> sums;
  while (executor->Next(&row, nullptr)) {
    sums.push_back(row[0].value.double_value);
  }
  std::sort(sums.begin(), sums.end());
  const std::vector<double> expected{9.9, 10.9, 11.9, 12.9, 13.9};
  ASSERT_EQ(sums.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_DOUBLE_EQ(sums[i], expected[i]);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

// The index nested-loop rule can push ONE range bound into the probe, on the
// first index key column the equality prefix leaves open. The plan must carry
// that bound -- so the seek is a range, not a point -- and the residual above
// it must carry everything the probe does not decide.
//
// The rule is applied directly (not through the full memo exploration) so the
// test pins the rule's own contract. The memo path reaches the same rule
// through FilteredSelfJoinStillPlansThroughTheIndexNestedLoop, which shows the
// inferred null-safety conjuncts no longer disqualify an index whose right
// scan group carries a filter.
TEST_F(OptimizerTest, IndexJoinRulePushesOneRangeBoundIntoTheProbe) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_tbl,
      rs_->CreateTable(setup,
                       Schema("ProbeLeft", {Column("x", ValueType::kInt64),
                                            Column("y", ValueType::kInt64)})));
  for (const auto& row : {Row({Value(1), Value(5)}), Row({Value(1), Value(6)}),
                          Row({Value(2), Value(5)})}) {
    ASSERT_SUCCESS(left_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(setup,
                       Schema("ProbeRight", {Column("a", ValueType::kInt64),
                                             Column("b", ValueType::kInt64),
                                             Column("c", ValueType::kInt64)})));
  for (const auto& row : {Row({Value(1), Value(4), Value(50)}),
                          Row({Value(1), Value(10), Value(100)}),
                          Row({Value(1), Value(20), Value(200)}),
                          Row({Value(1), Value(30), Value(300)}),
                          Row({Value(2), Value(6), Value(60)}),
                          Row({Value(2), Value(40), Value(400)})}) {
    ASSERT_SUCCESS(right_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSERT_SUCCESS(rs_->CreateIndex(
      setup, "ProbeRight",
      IndexSchema("ProbeRightAB", {0, 1}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  TransactionContext stat_ctx = rs_->BeginContext();
  rs_->RefreshStatistics(stat_ctx, "ProbeLeft");
  rs_->RefreshStatistics(stat_ctx, "ProbeRight");
  ASSERT_SUCCESS(stat_ctx.PreCommit());

  TransactionContext context = rs_->BeginContext();
  const auto left = context.GetTable("ProbeLeft");
  const auto right = context.GetTable("ProbeRight");
  const auto left_stats = context.GetStats("ProbeLeft");
  const auto right_stats = context.GetStats("ProbeRight");
  ASSERT_TRUE(left.HasValue());
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(left_stats.HasValue());
  ASSERT_TRUE(right_stats.HasValue());

  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("ProbeLeft", "x")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("ProbeRight", "a"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("ProbeRight", "b")),
                          BinaryOperation::kGreaterThan,
                          ColumnValueExp(ColumnName("ProbeLeft", "y"))));

  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  rule_context.tables = {{"ProbeLeft", left.Value()},
                         {"ProbeRight", right.Value()}};
  rule_context.statistics = {{"ProbeLeft", left_stats.Value()},
                             {"ProbeRight", right_stats.Value()}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "index_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);

  const TableStatistics left_ts = *left_stats.Value();
  Plan left_plan = std::make_shared<FullScanPlan>(*left.Value(), left_ts);
  Plan right_plan = std::make_shared<FullScanPlan>(*right.Value(), left_ts);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"ProbeLeft", "ProbeRight"});
  const cascades::GroupId right_group =
      memo.Get(root).expressions.front().children[1];
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = predicate;
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left_plan, .estimated_rows = 3.0},
      cascades::BestPlan{.plan = right_plan, .estimated_rows = 6.0}};
  const std::vector<cascades::PlanAlternative> alternatives =
      rule->Apply(memo, right_group, logical, children,
                  cascades::PhysicalProperties{}, rule_context);
  ASSERT_EQ(alternatives.size(), 1U);
  const auto product =
      std::dynamic_pointer_cast<ProductPlan>(alternatives.front().plan);
  ASSERT_NE(product, nullptr);
  ASSERT_TRUE(product->Lookup().has_value());
  // The equality prefix pins (a); the range bound rides on (b), the first
  // index key column the prefix leaves open. Both are shown in index key
  // order, not in the order the predicate lists them.
  ASSERT_EQ(SeekOf(product->Lookup()).right_columns.size(), 2U);
  EXPECT_EQ(SeekOf(product->Lookup()).right_columns[0],
            ColumnName("ProbeRight", "a"));
  ASSERT_EQ(SeekOf(product->Lookup()).range_position, 1U);
  EXPECT_EQ(SeekOf(product->Lookup()).left_columns[1],
            ColumnName("ProbeLeft", "y"));
  EXPECT_EQ(SeekOf(product->Lookup()).right_columns[1],
            ColumnName("ProbeRight", "b"));
  EXPECT_EQ(SeekOf(product->Lookup()).range_operation,
            BinaryOperation::kGreaterThan);
  // The range bound is enforced by the probe, so no residual is left.
  EXPECT_EQ(product->ResidualNote(), nullptr);
  EXPECT_NE(product->ToString().find("probe:{ProbeRight.a, ProbeLeft.y >"
                                     " ProbeRight.b}"),
            std::string::npos)
      << product->ToString();

  Executor executor = product->EmitExecutor(context);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("LookupJoin"), std::string::npos) << dump.str();
  // The join emits `left(2) + right(3)`, so the payload `c` sits at index 4.
  constexpr size_t kPayloadColumn = 4;
  std::vector<int64_t> matched;
  Row row;
  while (executor->Next(&row, nullptr)) {
    matched.push_back(row[kPayloadColumn].value.int_value);
  }
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  // (x=1, y=5): b > 5 -> 10, 20, 30.  (x=1, y=6): b > 6 -> 10, 20, 30.
  // (x=2, y=5): b > 5 -> 6, 40.
  std::vector<int64_t> expected{60, 100, 100, 200, 200, 300, 300, 400};
  std::ranges::sort(matched);
  std::ranges::sort(expected);
  EXPECT_EQ(matched, expected);
  ASSERT_SUCCESS(context.PreCommit());
}

// Both sides of an inequality on the same open key column are adopted
// together: the probe becomes an interval seek instead of a one-sided bound.
// The rule removes BOTH conjuncts from the residual, so the interval has to be
// exact -- and it is, because the executor re-checks each bound per candidate.
TEST_F(OptimizerTest, IndexJoinRuleAdoptsBothIntervalBoundsIntoTheProbe) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_tbl,
      rs_->CreateTable(
          setup, Schema("IntervalLeft", {Column("x", ValueType::kInt64),
                                         Column("lo", ValueType::kInt64),
                                         Column("hi", ValueType::kInt64)})));
  for (const auto& row : {Row({Value(1), Value(5), Value(15)}),
                          Row({Value(2), Value(9), Value(26)}),
                          Row({Value(3), Value(20), Value(31)})}) {
    ASSERT_SUCCESS(left_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(setup, Schema("IntervalRight",
                                     {Column("a", ValueType::kInt64),
                                      Column("b", ValueType::kInt64),
                                      Column("payload", ValueType::kInt64)})));
  for (const auto& row : {Row({Value(1), Value(4), Value(40)}),
                          Row({Value(1), Value(10), Value(100)}),
                          Row({Value(1), Value(15), Value(150)}),
                          Row({Value(1), Value(20), Value(200)}),
                          Row({Value(2), Value(10), Value(210)}),
                          Row({Value(2), Value(25), Value(250)}),
                          Row({Value(3), Value(21), Value(321)}),
                          Row({Value(3), Value(30), Value(330)})}) {
    ASSERT_SUCCESS(right_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSERT_SUCCESS(rs_->CreateIndex(
      setup, "IntervalRight",
      IndexSchema("IntervalRightAB", {0, 1}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  TransactionContext stat_ctx = rs_->BeginContext();
  rs_->RefreshStatistics(stat_ctx, "IntervalLeft");
  rs_->RefreshStatistics(stat_ctx, "IntervalRight");
  ASSERT_SUCCESS(stat_ctx.PreCommit());

  TransactionContext context = rs_->BeginContext();
  const auto left = context.GetTable("IntervalLeft");
  const auto right = context.GetTable("IntervalRight");
  const auto left_stats = context.GetStats("IntervalLeft");
  const auto right_stats = context.GetStats("IntervalRight");
  ASSERT_TRUE(left.HasValue());
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(left_stats.HasValue());
  ASSERT_TRUE(right_stats.HasValue());

  // x = a AND lo < b AND b < hi: one equality pinning the leading key column
  // and an interval on the next one.
  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("IntervalLeft", "x")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("IntervalRight", "a"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(
              ColumnValueExp(ColumnName("IntervalLeft", "lo")),
              BinaryOperation::kLessThan,
              ColumnValueExp(ColumnName("IntervalRight", "b")))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("IntervalRight", "b")),
                          BinaryOperation::kLessThan,
                          ColumnValueExp(ColumnName("IntervalLeft", "hi"))));

  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  rule_context.tables = {{"IntervalLeft", left.Value()},
                         {"IntervalRight", right.Value()}};
  rule_context.statistics = {{"IntervalLeft", left_stats.Value()},
                             {"IntervalRight", right_stats.Value()}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "index_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);

  const TableStatistics left_ts = *left_stats.Value();
  Plan left_plan = std::make_shared<FullScanPlan>(*left.Value(), left_ts);
  Plan right_plan = std::make_shared<FullScanPlan>(*right.Value(), left_ts);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"IntervalLeft", "IntervalRight"});
  const cascades::GroupId right_group =
      memo.Get(root).expressions.front().children[1];
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = predicate;
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left_plan, .estimated_rows = 3.0},
      cascades::BestPlan{.plan = right_plan, .estimated_rows = 8.0}};
  const std::vector<cascades::PlanAlternative> alternatives =
      rule->Apply(memo, right_group, logical, children,
                  cascades::PhysicalProperties{}, rule_context);
  ASSERT_EQ(alternatives.size(), 1U);
  const auto product =
      std::dynamic_pointer_cast<ProductPlan>(alternatives.front().plan);
  ASSERT_NE(product, nullptr);
  ASSERT_TRUE(product->Lookup().has_value());
  // The equality prefix pins (a); the interval rides on (b).
  ASSERT_EQ(SeekOf(product->Lookup()).right_columns.size(), 2U);
  ASSERT_EQ(SeekOf(product->Lookup()).range_position, 1U);
  // The lower conjunct arrived first, so it is the probe's first bound; the
  // upper one lands on the same position as the interval's other end.
  EXPECT_EQ(SeekOf(product->Lookup()).left_columns[1],
            ColumnName("IntervalLeft", "lo"));
  EXPECT_EQ(SeekOf(product->Lookup()).range_operation,
            BinaryOperation::kGreaterThan);
  EXPECT_EQ(SeekOf(product->Lookup()).second_range_left_column,
            ColumnName("IntervalLeft", "hi"));
  EXPECT_EQ(SeekOf(product->Lookup()).second_range_operation,
            BinaryOperation::kLessThan);
  // Both bounds are enforced by the probe, so no residual is left.
  EXPECT_EQ(product->ResidualNote(), nullptr);
  EXPECT_NE(product->ToString().find("probe:{IntervalRight.a, IntervalLeft.lo <"
                                     " IntervalRight.b < IntervalLeft.hi}"),
            std::string::npos)
      << product->ToString();

  Executor executor = product->EmitExecutor(context);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("LookupJoin"), std::string::npos) << dump.str();
  // The join emits `left(3) + right(3)`, so the payload sits at index 5.
  constexpr size_t kPayloadColumn = 5;
  std::vector<int64_t> matched;
  Row row;
  while (executor->Next(&row, nullptr)) {
    matched.push_back(row[kPayloadColumn].value.int_value);
  }
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  // (x=1, 5 < b < 15): b = 10.  (x=2, 9 < b < 26): b = 10, 25.
  // (x=3, 20 < b < 31): b = 21, 30.  The exclusive edges at b = 15 and
  // b = 20/25 are excluded by the per-candidate re-check.
  std::vector<int64_t> expected{100, 210, 250, 321, 330};
  std::ranges::sort(matched);
  std::ranges::sort(expected);
  EXPECT_EQ(matched, expected);
  ASSERT_SUCCESS(context.PreCommit());
}

// A second range column, a range the type whitelist rejects and an equality on
// a non-key column all have to stay residual predicates. The probe
// over-fetches and the predicate above it decides the rows -- never the other
// way round.
TEST_F(OptimizerTest, IndexJoinRuleKeepsUnprobedPredicatesResidual) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_tbl,
      rs_->CreateTable(setup,
                       Schema("ProbeLeft", {Column("x", ValueType::kInt64),
                                            Column("y", ValueType::kInt64),
                                            Column("z", ValueType::kDouble)})));
  for (const auto& row : {Row({Value(1), Value(5), Value(2.5)}),
                          Row({Value(2), Value(1), Value(0.5)})}) {
    ASSERT_SUCCESS(left_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(
          setup, Schema("ProbeRight", {Column("a", ValueType::kInt64),
                                       Column("b", ValueType::kInt64),
                                       Column("c", ValueType::kInt64),
                                       Column("d", ValueType::kDouble)})));
  for (const auto& row : {Row({Value(1), Value(10), Value(1), Value(9.5)}),
                          Row({Value(1), Value(20), Value(7), Value(1.5)}),
                          Row({Value(1), Value(30), Value(1), Value(3.5)}),
                          Row({Value(2), Value(5), Value(2), Value(0.25)}),
                          Row({Value(2), Value(40), Value(9), Value(7.5)})}) {
    ASSERT_SUCCESS(right_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSERT_SUCCESS(rs_->CreateIndex(
      setup, "ProbeRight",
      IndexSchema("ProbeRightAB", {0, 1}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  TransactionContext stat_ctx = rs_->BeginContext();
  rs_->RefreshStatistics(stat_ctx, "ProbeLeft");
  rs_->RefreshStatistics(stat_ctx, "ProbeRight");
  ASSERT_SUCCESS(stat_ctx.PreCommit());

  TransactionContext context = rs_->BeginContext();
  const auto left = context.GetTable("ProbeLeft");
  const auto right = context.GetTable("ProbeRight");
  const auto left_stats = context.GetStats("ProbeLeft");
  const auto right_stats = context.GetStats("ProbeRight");
  ASSERT_TRUE(left.HasValue());
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(left_stats.HasValue());
  ASSERT_TRUE(right_stats.HasValue());

  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(
              BinaryExpressionExp(
                  ColumnValueExp(ColumnName("ProbeLeft", "x")),
                  BinaryOperation::kEquals,
                  ColumnValueExp(ColumnName("ProbeRight", "a"))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(
                  ColumnValueExp(ColumnName("ProbeRight", "b")),
                  BinaryOperation::kGreaterThan,
                  ColumnValueExp(ColumnName("ProbeLeft", "y")))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("ProbeRight", "d")),
                              BinaryOperation::kLessThan,
                              ColumnValueExp(ColumnName("ProbeLeft", "z")))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("ProbeLeft", "x")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("ProbeRight", "c"))));

  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  rule_context.tables = {{"ProbeLeft", left.Value()},
                         {"ProbeRight", right.Value()}};
  rule_context.statistics = {{"ProbeLeft", left_stats.Value()},
                             {"ProbeRight", right_stats.Value()}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "index_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);

  const TableStatistics left_ts = *left_stats.Value();
  Plan left_plan = std::make_shared<FullScanPlan>(*left.Value(), left_ts);
  Plan right_plan = std::make_shared<FullScanPlan>(*right.Value(), left_ts);
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build({"ProbeLeft", "ProbeRight"});
  const cascades::GroupId right_group =
      memo.Get(root).expressions.front().children[1];
  cascades::LogicalExpression logical = memo.Get(root).expressions.front();
  logical.predicate = predicate;
  const std::vector<cascades::BestPlan> children{
      cascades::BestPlan{.plan = left_plan, .estimated_rows = 2.0},
      cascades::BestPlan{.plan = right_plan, .estimated_rows = 5.0}};
  const std::vector<cascades::PlanAlternative> alternatives =
      rule->Apply(memo, right_group, logical, children,
                  cascades::PhysicalProperties{}, rule_context);
  ASSERT_EQ(alternatives.size(), 1U);
  // The unprobed conjuncts become a residual Selection above the join, and the
  // probe still carries the one admissible bound.
  std::ostringstream rendered_stream;
  alternatives.front().plan->Dump(rendered_stream, 0);
  const std::string rendered = rendered_stream.str();
  EXPECT_EQ(rendered.rfind("Select:", 0), 0U) << rendered;
  EXPECT_NE(rendered.find("ProbeRight.d < ProbeLeft.z"), std::string::npos)
      << rendered;
  EXPECT_NE(rendered.find("ProbeLeft.x = ProbeRight.c"), std::string::npos)
      << rendered;
  EXPECT_NE(rendered.find("probe:{ProbeRight.a, ProbeLeft.y >"
                          " ProbeRight.b}"),
            std::string::npos)
      << rendered;
  // The range bound the probe does enforce must NOT be re-checked above it.
  EXPECT_EQ(rendered.find("ProbeRight.b > ProbeLeft.y"), std::string::npos)
      << rendered;

  Executor executor = alternatives.front().plan->EmitExecutor(context);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("LookupJoin"), std::string::npos) << dump.str();
  // The join emits `left(3) + right(4)`, so the right `b` sits at index 4.
  constexpr size_t kPayloadColumn = 4;
  std::vector<int64_t> matched;
  Row row;
  while (executor->Next(&row, nullptr)) {
    matched.push_back(row[kPayloadColumn].value.int_value);
  }
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  // (x=1, y=5, z=2.5): a=1, b>5, d<2.5, c=1 -> only (1, 30, 1, 3.5) has
  // c=1, but its d=3.5 is not below 2.5.
  // (x=2, y=1, z=0.5): a=2, b>1, d<0.5, c=2 -> (2, 5, 2, 0.25) only.
  EXPECT_EQ(matched, std::vector<int64_t>{5});
  ASSERT_SUCCESS(context.PreCommit());
}

// A band join planned through the full memo: join_commutativity registers both
// child orders, so the band_join rule can be handed children whose order is the
// reverse of the one the predicate is written in. The seed-17 SqlJoinFuzzer
// regression came from here -- the candidate range followed the *written*
// operator while the children's order was swapped, so the range and the
// residual re-check disagreed and the join lost rows. Whichever orientation
// wins, the pairs must equal the nested-loop mirror below.
TEST_F(OptimizerTest, ZigZagJoinChoosesTwoOrderedIndexesAndCanFallback) {
  // Duplicate-heavy indexes amortize distinct-key cursor navigation. With
  // unique keys the newly calibrated full scan + hash/index join is cheaper.
  {
    auto setup = rs_->BeginContext();
    for (const std::string name : {"ZigLeft", "ZigRight"}) {
      ASSIGN_OR_ASSERT_FAIL(
          Table, table,
          rs_->CreateTable(setup,
                           Schema(name, {Column("k", ValueType::kInt64)})));
      for (int64_t i = 0; i < 100; ++i) {
        ASSERT_SUCCESS(
            table.Insert(setup.txn_, Row({Value(i % 10)})).GetStatus());
      }
      ASSERT_SUCCESS(rs_->CreateIndex(
          setup, name,
          IndexSchema(name + "Idx", {0}, {}, IndexMode::kNonUnique)));
      ASSERT_SUCCESS(rs_->RefreshStatistics(setup, name));
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }
  QueryData query{
      {"ZigLeft", "ZigRight"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("ZigLeft", "k")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("ZigRight", "k"))),
      {NamedExpression("left_key", ColumnValueExp(ColumnName("ZigLeft", "k"))),
       NamedExpression("right_key",
                       ColumnValueExp(ColumnName("ZigRight", "k")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  dump << *plan;
  EXPECT_NE(dump.str().find("ZigZagJoin"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    ++rows;
    row = Row();
  }
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
  EXPECT_EQ(rows, 1000U);

  OptimizerOptions fallback = OptimizerOptions::Default();
  fallback.disabled_implementation_rules = {"zigzag_join"};
  ASSIGN_OR_ASSERT_FAIL(Plan, fallback_plan,
                        Optimizer::Optimize(query, context, fallback));
  std::ostringstream fallback_dump;
  fallback_dump << *fallback_plan;
  EXPECT_EQ(fallback_dump.str().find("ZigZagJoin"), std::string::npos)
      << fallback_dump.str();
  Executor fallback_executor = fallback_plan->EmitExecutor(context);
  rows = 0;
  while (fallback_executor->Next(&row, nullptr)) {
    ++rows;
    row = Row();
  }
  EXPECT_EQ(fallback_executor->GetStatus(), Status::kSuccess);
  EXPECT_EQ(rows, 1000U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, BandJoinSurvivesJoinCommutativity) {
  TransactionContext setup = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_tbl,
      rs_->CreateTable(setup,
                       Schema("BandLeft", {Column("a", ValueType::kInt64)})));
  for (const auto& row : {Row({Value(10)}), Row({Value(2)}), Row({Value(4)})}) {
    ASSERT_SUCCESS(left_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(setup,
                       Schema("BandRight", {Column("k", ValueType::kInt64)})));
  for (const auto& row : {Row({Value(1)}), Row({Value(3)}), Row({Value(5)})}) {
    ASSERT_SUCCESS(right_tbl.Insert(setup.txn_, row).GetStatus());
  }
  ASSERT_SUCCESS(setup.txn_.PreCommit());
  TransactionContext stat_ctx = rs_->BeginContext();
  rs_->RefreshStatistics(stat_ctx, "BandLeft");
  rs_->RefreshStatistics(stat_ctx, "BandRight");
  ASSERT_SUCCESS(stat_ctx.PreCommit());

  TransactionContext context = rs_->BeginContext();
  const auto left = context.GetTable("BandLeft");
  const auto right = context.GetTable("BandRight");
  const auto left_stats = context.GetStats("BandLeft");
  const auto right_stats = context.GetStats("BandRight");
  ASSERT_TRUE(left.HasValue());
  ASSERT_TRUE(right.HasValue());
  ASSERT_TRUE(left_stats.HasValue());
  ASSERT_TRUE(right_stats.HasValue());

  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  rule_context.tables = {{"BandLeft", left.Value()},
                         {"BandRight", right.Value()}};
  rule_context.statistics = {{"BandLeft", left_stats.Value()},
                             {"BandRight", right_stats.Value()}};
  const cascades::ImplementationRule* rule = nullptr;
  for (const auto& candidate : DefaultImplementationRules().Rules()) {
    if (candidate.Name() == "band_join") {
      rule = &candidate;
      break;
    }
  }
  ASSERT_NE(rule, nullptr);

  // The nested-loop mirror: every (a, k) pair with a <= k.
  std::vector<std::pair<int64_t, int64_t>> expected;
  for (const int64_t a : {10, 2, 4}) {
    for (const int64_t k : {1, 3, 5}) {
      if (a <= k) {
        expected.emplace_back(a, k);
      }
    }
  }
  std::ranges::sort(expected);

  // NOTE: FullScanPlan keeps its statistics by reference, so the scans' stats
  // must outlive the plans.
  const TableStatistics left_ts = *left_stats.Value();
  const TableStatistics right_ts = *right_stats.Value();
  Plan left_plan = std::make_shared<FullScanPlan>(*left.Value(), left_ts);
  Plan right_plan = std::make_shared<FullScanPlan>(*right.Value(), right_ts);
  const Expression written =
      BinaryExpressionExp(ColumnValueExp(ColumnName("BandLeft", "a")),
                          BinaryOperation::kLessThanEquals,
                          ColumnValueExp(ColumnName("BandRight", "k")));

  // Both orientations are exercised, each with a fresh memo so the child order
  // in the bindings is exactly the one under test.
  for (const bool swapped : {false, true}) {
    SCOPED_TRACE(swapped ? "children (right, left)" : "children (left, right)");
    cascades::Memo memo;
    const cascades::GroupId root = memo.Build({"BandLeft", "BandRight"});
    cascades::LogicalExpression logical = memo.Get(root).expressions.front();
    logical.predicate = written;
    std::vector<cascades::BestPlan> children;
    children.reserve(2);
    if (swapped) {
      children.push_back(
          cascades::BestPlan{.plan = right_plan, .estimated_rows = 3.0});
      children.push_back(
          cascades::BestPlan{.plan = left_plan, .estimated_rows = 3.0});
    } else {
      children.push_back(
          cascades::BestPlan{.plan = left_plan, .estimated_rows = 3.0});
      children.push_back(
          cascades::BestPlan{.plan = right_plan, .estimated_rows = 3.0});
    }
    const std::vector<cascades::PlanAlternative> alternatives =
        rule->Apply(memo, logical.children[1], logical, children,
                    cascades::PhysicalProperties{}, rule_context);
    ASSERT_EQ(alternatives.size(), 1U);
    const auto band =
        std::dynamic_pointer_cast<BandJoinPlan>(alternatives.front().plan);
    ASSERT_NE(band, nullptr);
    // Each side is ordered by its OWN key, and the comparison reads in the
    // child order the plan actually has.
    if (swapped) {
      EXPECT_EQ(band->LeftKeys().front(), ColumnName("BandRight", "k"));
      EXPECT_EQ(band->RightKeys().front(), ColumnName("BandLeft", "a"));
      EXPECT_EQ(band->Comparison(), BinaryOperation::kGreaterThanEquals);
    } else {
      EXPECT_EQ(band->LeftKeys().front(), ColumnName("BandLeft", "a"));
      EXPECT_EQ(band->RightKeys().front(), ColumnName("BandRight", "k"));
      EXPECT_EQ(band->Comparison(), BinaryOperation::kLessThanEquals);
    }
    // The plan is ordered by each child's own key.
    EXPECT_TRUE(band->Left()->IsOrderedBy(
        {ColumnValueExp(band->LeftKeys().front())}, {true}));
    EXPECT_TRUE(band->Right()->IsOrderedBy(
        {ColumnValueExp(band->RightKeys().front())}, {true}));

    Executor executor = alternatives.front().plan->EmitExecutor(context);
    std::vector<std::pair<int64_t, int64_t>> got;
    Row row;
    while (executor->Next(&row, nullptr)) {
      const int64_t first = row[0].value.int_value;
      const int64_t second = row[1].value.int_value;
      // The join emits (left, right); normalize to (a, k) either way.
      got.emplace_back(swapped ? second : first, swapped ? first : second);
    }
    EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
    std::ranges::sort(got);
    EXPECT_EQ(got, expected);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, AliasedOrderByLimitFoldsTopK) {
  // Phase 8 follow-up: the relation rename translates ordering requests back
  // to physical names. An ordered IndexOnlyScan already delivers rows in the
  // requested order, so no Sort (and no Top-N heap) is needed above the scan;
  // LIMIT applies straight to the ordered stream.
  QueryData query{
      {"a"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(80))),
      {NamedExpression("ac1", ColumnValueExp(ColumnName("a", "c1")))}};
  query.aliases_ = {{"a", "Sc1"}};
  query.order_expressions_ = {ColumnValueExp(ColumnName("a", "c1"))};
  query.order_ascending_ = {true};
  query.limit_count_ = 5;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("Rename"), std::string::npos) << dump.str();
  EXPECT_EQ(plan->IsOrderedBy(query.order_expressions_, query.order_ascending_),
            true)
      << dump.str();
  EXPECT_EQ(plan->EnforcesLimit(query.limit_count_, query.limit_offset_), true)
      << dump.str();

  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<int64_t> keys;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(keys, (std::vector<int64_t>{80, 81, 82, 83, 84}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, GeneralCascadesPathUsesLogicalTopN) {
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc2", "d1"))),
      {NamedExpression("key", ColumnValueExp(ColumnName("Sc1", "c1"))),
       NamedExpression("name", ColumnValueExp(ColumnName("Sc2", "d3")))}};
  query.order_expressions_ = {ColumnValueExp(ColumnName("Sc1", "c1"))};
  query.order_ascending_ = {false};
  query.limit_count_ = 2;
  query.limit_offset_ = 1;
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("TopN"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<int64_t> keys;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(keys, (std::vector<int64_t>{98, 97}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, CommonEqualityInEveryOrBranchBecomesJoinKey) {
  const Expression left_key = ColumnValueExp(ColumnName("Sc1", "c1"));
  const Expression right_key = ColumnValueExp(ColumnName("Sc2", "d1"));
  const auto conjunction = [](Expression left, Expression right) {
    return BinaryExpressionExp(std::move(left), BinaryOperation::kAnd,
                               std::move(right));
  };
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          conjunction(
              BinaryExpressionExp(
                  left_key, BinaryOperation::kEquals,
                  right_key),  // NOLINT(readability-suspicious-call-argument)
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                                  BinaryOperation::kLessThan,
                                  ConstantValueExp(Value(10)))),
          BinaryOperation::kOr,
          conjunction(
              // NOLINTNEXTLINE(readability-suspicious-call-argument)
              BinaryExpressionExp(right_key, BinaryOperation::kEquals,
                                  left_key),
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                                  BinaryOperation::kGreaterThanEquals,
                                  ConstantValueExp(Value(90))))),
      {NamedExpression("key", ColumnValueExp(ColumnName("Sc1", "c1")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  OptimizerOptions options = OptimizerOptions::Default();
  options.expression_rules.Remove("factor_or_common_and");
  options.expression_rules.Remove("boolean_filter_pullup");
  options.expression_rules.Remove("distribute_or_over_and_budgeted");
  options.disabled_implementation_rules = {"merge_join", "index_join",
                                           "nested_loop_join", "cross_join"};
  ASSIGN_OR_ASSERT_FAIL(Plan, plan,
                        Optimizer::Optimize(query, context, options));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("Hash Join"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("Cross Join"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  size_t rows = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 20U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, EqualityMissingFromOneOrBranchDoesNotBecomeJoinKey) {
  const Expression key = ColumnValueExp(ColumnName("Sc1", "c1"));
  QueryData query{
      {"Sc1", "Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(90))),
          BinaryOperation::kOr,
          BinaryExpressionExp(
              BinaryExpressionExp(key, BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("Sc2", "d1"))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                                  BinaryOperation::kLessThan,
                                  ConstantValueExp(Value(10))))),
      {NamedExpression("key", ColumnValueExp(ColumnName("Sc1", "c1")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  OptimizerOptions options = OptimizerOptions::Default();
  options.expression_rules.Remove("factor_or_common_and");
  options.expression_rules.Remove("boolean_filter_pullup");
  options.expression_rules.Remove("distribute_or_over_and_budgeted");
  options.disabled_implementation_rules = {"merge_join", "index_join"};
  ASSIGN_OR_ASSERT_FAIL(Plan, plan,
                        Optimizer::Optimize(query, context, options));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_EQ(dump.str().find("Hash Join"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("NestedLoopJoin"), std::string::npos)
      << physical.str();
  size_t rows = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 2010U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, InListDrivesPointUnionIndexAccess) {
  // Phase 8 follow-up: a constant IN list on the leading index key becomes
  // one point range per distinct value instead of a full scan.
  QueryData query{
      {"a"},
      InExpressionExp(ColumnValueExp(ColumnName("a", "c1")),
                      {ConstantValueExp(Value(2)), ConstantValueExp(Value(97)),
                       ConstantValueExp(Value(99))}),
      {NamedExpression("ac3", ColumnValueExp(ColumnName("a", "c3")))}};
  query.aliases_ = {{"a", "Sc1"}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("x3 points"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical_dump;
  executor->Dump(physical_dump, 0);
  EXPECT_NE(physical_dump.str().find("x3 points"), std::string::npos)
      << physical_dump.str();
  Row row;
  std::vector<double> sums;
  while (executor->Next(&row, nullptr)) {
    sums.push_back(row[0].value.double_value);
  }
  std::sort(sums.begin(), sums.end());
  const std::vector<double> expected{11.9, 106.9, 108.9};
  ASSERT_EQ(sums.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_DOUBLE_EQ(sums[i], expected[i]);
  }
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, DisjointRangesUseBitmapOrWithResidualRecheck) {
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("c1"),
                                  BinaryOperation::kGreaterThanEquals,
                                  ConstantValueExp(Value(1))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp("c1"),
                                  BinaryOperation::kLessThanEquals,
                                  ConstantValueExp(Value(2)))),
          BinaryOperation::kOr,
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("c1"),
                                  BinaryOperation::kGreaterThanEquals,
                                  ConstantValueExp(Value(95))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp("c1"),
                                  BinaryOperation::kLessThanEquals,
                                  ConstantValueExp(Value(99))))),
      {NamedExpression("key", ColumnValueExp("c1"))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream logical;
  plan->Dump(logical, 0);
  EXPECT_NE(logical.str().find("BitmapOr"), std::string::npos) << logical.str();
  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("BitmapHeapScan"), std::string::npos)
      << physical.str();
  Row row;
  std::vector<int64_t> keys;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  std::ranges::sort(keys);
  EXPECT_EQ(keys, (std::vector<int64_t>{1, 2, 95, 96, 97, 98, 99}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, IndependentIndexPredicatesUseBitmapAnd) {
  QueryData query{
      {"Sc2"},
      BinaryExpressionExp(
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("d3"),
                                  BinaryOperation::kGreaterThanEquals,
                                  ConstantValueExp(Value("d3-2"))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp("d3"),
                                  BinaryOperation::kLessThanEquals,
                                  ConstantValueExp(Value("d3-7")))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("d1"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(100)))),
      {NamedExpression("key", ColumnValueExp("d1")),
       NamedExpression("name", ColumnValueExp("d3"))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream logical;
  plan->Dump(logical, 0);
  EXPECT_NE(logical.str().find("BitmapAnd"), std::string::npos)
      << logical.str();
  EXPECT_GT(plan->AccessRowCount(), 0U);
  EXPECT_GT(plan->EmitRowCount(), 1U);
  Executor executor = plan->EmitExecutor(context);
  Row row;
  size_t rows = 0;
  while (executor->Next(&row, nullptr)) {
    EXPECT_GE(row[0].value.int_value, 100);
    EXPECT_GE(row[1].value.varchar_value, "d3-2");
    EXPECT_LE(row[1].value.varchar_value, "d3-7");
    ++rows;
  }
  EXPECT_EQ(rows, 60U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, EqualityPrefixAndInListDriveCompositePointUnion) {
  // A predicate such as (warehouse = ?) AND (item IN (...)) must use one
  // composite point range per item instead of scanning every row under the
  // equality prefix.  The dedicated table makes the IN list selective: the
  // prefix a matches 20 rows while each (a, b) point matches exactly one, so
  // the point union is genuinely cheaper than the single equality-prefix
  // range (which a covering index would otherwise win on cost).
  const std::string kTable = "PointUnionChoice";
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(writer,
                         Schema(kTable, {Column("a", ValueType::kInt64),
                                         Column("b", ValueType::kInt64),
                                         Column("c", ValueType::kInt64)})));
    for (int64_t i = 0; i < 400; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(writer.txn_, Row({Value(i % 20), Value(i / 20), Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(rs_->CreateIndex(
        writer, kTable,
        IndexSchema("CompositeIdx", {0, 1}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  {
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, kTable));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  QueryData query{
      {kTable},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{5}))),
          BinaryOperation::kAnd,
          InExpressionExp(ColumnValueExp("b"),
                          {ConstantValueExp(Value(int64_t{1})),
                           ConstantValueExp(Value(int64_t{2}))})),
      {NamedExpression("a"), NamedExpression("b")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("x2 points"), std::string::npos) << dump.str();

  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<int64_t> keys;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(keys.size(), 2U);
  EXPECT_TRUE(std::all_of(keys.begin(), keys.end(),
                          [](int64_t key) { return key == 5; }));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(
    OptimizerTest,
    AccessMethodHintAndMemoDumpDiagnosticsSmoke) {  // Arrange: a plain equality
                                                    // query planned with the
                                                    // Phase 5 access-method
  // hint and Phase 9 memo dumping enabled.
  QueryData query{
      {"Sc1"},
      BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))),
      {NamedExpression("c1"), NamedExpression("c2")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  OptimizerOptions options = OptimizerOptions::Default();
  options.access_method = cascades::AccessMethod::kPreferIndex;
  options.dump_memo = true;

  // Act + Assert: planning succeeds and returns the matching row.
  const auto plan_or = (Optimizer::Optimize(query, context, options));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(2));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

namespace {
// Row count safely above kParallelScanMinRows / kParallelAggregationMinRows so
// the emitted executor is the parallel one.
constexpr int64_t kParallelTestRows = 9000;

void InsertAnalyzedBigTable(Database* db, std::string_view table) {
  TransactionContext writer = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, tbl,
      db->CreateTable(writer,
                      Schema(table, {Column("key", ValueType::kInt64),
                                     Column("payload", ValueType::kInt64)})));
  for (int64_t key = 0; key < kParallelTestRows; ++key) {
    ASSERT_SUCCESS(
        tbl.Insert(writer.txn_, Row({Value(key), Value(key * 3)})).GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
  TransactionContext stats_ctx = db->BeginContext();
  ASSERT_SUCCESS(db->RefreshStatistics(stats_ctx, table));
  ASSERT_SUCCESS(stats_ctx.PreCommit());
}
}  // namespace

TEST_F(OptimizerTest, ParallelScanEmittedForLargeAnalyzedTable) {
  // Arrange: an analyzed table whose row count exceeds the parallel threshold.
  const std::string kTable = "BigParallelScan";
  InsertAnalyzedBigTable(rs_.get(), kTable);
  QueryData query{{kTable},
                  BinaryExpressionExp(ColumnValueExp("key"),
                                      BinaryOperation::kGreaterThanEquals,
                                      ConstantValueExp(Value(0))),
                  {NamedExpression("key"), NamedExpression("payload")}};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Act + Assert (batch path): the emitted executor is a morsel-driven
  // ParallelScan that streams every row exactly once.
  Executor batch_executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  batch_executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("ParallelScan"), std::string::npos)
      << physical.str();
  DataChunk chunk;
  size_t rows = 0;
  size_t batches = 0;
  while (batch_executor->NextBatch(&chunk) != 0) {
    rows += chunk.Size();
    ++batches;
  }
  EXPECT_EQ(rows, static_cast<size_t>(kParallelTestRows));
  EXPECT_GT(batches, 1U);

  // Act + Assert (scalar path): Next() observes the same data.
  Executor scalar_executor = plan->EmitExecutor(context);
  int64_t count = 0;
  int64_t key_sum = 0;
  Row row;
  while (scalar_executor->Next(&row, nullptr)) {
    ++count;
    key_sum += row[0].value.int_value;
  }
  EXPECT_EQ(count, kParallelTestRows);
  EXPECT_EQ(key_sum, kParallelTestRows * (kParallelTestRows - 1) / 2);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, IntegerAggregationFoldsLargeParallelScan) {
  // Arrange: aggregation over a child estimated above the parallel threshold.
  const std::string kTable = "BigParallelAgg";
  InsertAnalyzedBigTable(rs_.get(), kTable);
  QueryData query{
      {kTable},
      BinaryExpressionExp(ColumnValueExp("key"),
                          BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(0))),
      {NamedExpression("cnt", AggregateExpressionExp(AggregationType::kCount,
                                                     ColumnValueExp("*"))),
       NamedExpression("total",
                       AggregateExpressionExp(AggregationType::kSum,
                                              ColumnValueExp("key")))}};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Integer payloads fold in the parallel scan without a second worker
  // pool. Keep the parallel scan and validate both aggregate results.
  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("ParallelScan"), std::string::npos)
      << physical.str();
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(kParallelTestRows));
  EXPECT_EQ(row[1], Value(kParallelTestRows * (kParallelTestRows - 1) / 2));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}

namespace {
// Analyzed table whose primary key arrives in ascending order, so a full scan
// over it may report the clustered index order (plan/full_scan_plan.hpp).
void InsertAnalyzedClusteredTable(Database* db, std::string_view table,
                                  int64_t rows) {
  TransactionContext writer = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, tbl,
      db->CreateTable(
          writer, Schema(table, {Column("key", ValueType::kInt64,
                                        Constraint(Constraint::kPrimaryKey)),
                                 Column("payload", ValueType::kInt64)})));
  for (int64_t key = 0; key < rows; ++key) {
    ASSERT_SUCCESS(
        tbl.Insert(writer.txn_, Row({Value(key), Value(key * 3)})).GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
  TransactionContext stats_ctx = db->BeginContext();
  ASSERT_SUCCESS(db->RefreshStatistics(stats_ctx, table));
  ASSERT_SUCCESS(stats_ctx.PreCommit());
}
}  // namespace

TEST_F(OptimizerTest, ClusteredFullScanDoesNotOutrankASelectiveIndexPath) {
  // The clustered order is a measured property of the heap, not a free access
  // path: a point/range index lookup must still win the cost comparison, and
  // the plan must not borrow the ordering preference an index scan earns.
  const std::string kTable = "ClusteredCost";
  InsertAnalyzedClusteredTable(rs_.get(), kTable, 40);
  QueryData query{
      {kTable},
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))),
      {NamedExpression("key"), NamedExpression("payload")}};
  query.order_expressions_ = {ColumnValueExp(ColumnName("ClusteredCost.key"))};
  query.order_ascending_ = {true};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << plan_or.Value();
  EXPECT_NE(dump.str().find("Index"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("FullScan"), std::string::npos) << dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, ClusteredFullScanIsNotPricedAsALimitEarlyStop) {
  // ORDER BY <clustered key> LIMIT n must not price a whole-chain read as an
  // n-row prefix: the LIMIT stays above the scan, so the covering index (which
  // can stop after the prefix) keeps winning.
  const std::string kTable = "ClusteredLimit";
  InsertAnalyzedClusteredTable(rs_.get(), kTable, 40);
  QueryData query{
      {kTable}, nullptr, {NamedExpression("key"), NamedExpression("payload")}};
  query.order_expressions_ = {ColumnValueExp(ColumnName("ClusteredLimit.key"))};
  query.order_ascending_ = {true};
  query.limit_count_ = 1;

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream dump;
  dump << plan;
  EXPECT_NE(dump.str().find("IndexScan"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("FullScan"), std::string::npos) << dump.str();
  // The result is the same either way, and it must be the smallest key.
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(0));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, UpdateOverParallelScanStaysCorrect) {
  // Arrange: an UPDATE-shaped plan (row positions required) over an analyzed
  // big table, mirroring how SqlEngine builds UPDATE statements.
  const std::string kTable = "BigParallelUpdate";
  InsertAnalyzedBigTable(rs_.get(), kTable);
  QueryData query;
  query.from_ = {kTable};
  query.where_ =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(100)));
  query.select_ = {NamedExpression("key", ColumnValueExp("key")),
                   NamedExpression("payload", BinaryExpressionExp(
                                                  ColumnValueExp("payload"),
                                                  BinaryOperation::kAdd,
                                                  ConstantValueExp(Value(1))))};
  query.require_row_position_ = true;

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Act: emit the mutation source. A DML source must acquire a write
  // intent and re-resolve each row's committed head; the morsel-based
  // ParallelScan has no such hook, so the plan stays on the single-threaded
  // FullScan even for an analyzed big table.
  Executor source = plan->EmitExecutor(context);
  std::ostringstream physical;
  source->Dump(physical, 0);
  EXPECT_NE(physical.str().find("FullScan"), std::string::npos)
      << physical.str();
  EXPECT_EQ(physical.str().find("ParallelScan"), std::string::npos)
      << physical.str();
  const auto table_or = (context.GetTable(kTable));
  ASSERT_EQ(table_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& table = table_or.Value();
  Update update(context.txn_, table.get(), std::move(source));
  Row row;
  ASSERT_TRUE(update.Next(&row, nullptr));
  EXPECT_EQ(row[1], Value(100));
  EXPECT_FALSE(update.Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());

  // Assert: exactly the targeted payloads incremented, everything else intact.
  TransactionContext reader = rs_->BeginContext();
  const auto verify_table_or = (reader.GetTable(kTable));
  ASSERT_EQ(verify_table_or.GetStatus(), Status::kSuccess);
  const std::shared_ptr<Table>& verify_table = verify_table_or.Value();
  int64_t total = 0;
  int64_t mismatched = 0;
  for (Iterator it = verify_table->BeginFullScan(reader.txn_); it.IsValid();
       ++it) {
    ++total;
    const int64_t key = (*it)[0].value.int_value;
    const int64_t expected = (key * 3) + (key < 100 ? 1 : 0);
    if ((*it)[1].value.int_value != expected) {
      ++mismatched;
    }
  }
  EXPECT_EQ(total, kParallelTestRows);
  EXPECT_EQ(mismatched, 0);
  ASSERT_SUCCESS(reader.PreCommit());
}
// ---------------------------------------------------------------------------
// Semi/anti join decorrelation (tpch Phase2-4 / P1-5).
// ---------------------------------------------------------------------------

namespace {

int CountRows(const Executor& executor) {
  Row row;
  int count = 0;
  while (executor->Next(&row, nullptr)) {
    ++count;
  }
  return count;
}

}  // namespace

TEST_F(OptimizerTest, InSubqueryBecomesSemiJoin) {
  // Arrange: Sc1.c1 = 0..99, Sc3.e1 = 1..20. `c1 IN (SELECT e1 FROM Sc3)`
  // must keep exactly the 20 intersecting rows.
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(ColumnName("Sc3", "e1"))},
      std::vector<std::string>{"Sc3"}, Expression());
  QueryData query;
  query.from_ = {"Sc1"};
  query.where_ =
      QueryExpressionExp(std::move(statement),
                         ColumnValueExp(ColumnName("Sc1", "c1")), false, false);
  query.select_ = {NamedExpression("c1")};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream logical;
  logical << *plan;
  EXPECT_NE(logical.str().find("Semi Join"), std::string::npos)
      << logical.str();

  // Act + Assert: emit and count.
  Executor executor = plan->EmitExecutor(context);
  EXPECT_EQ(CountRows(executor), 20);
}

TEST_F(OptimizerTest, CorrelatedExistsBecomesSemiJoin) {
  // Arrange: EXISTS(SELECT ... FROM Sc3 WHERE Sc3.e1 = Sc1.c1) decorrelates
  // to a semi join on that equality; inner-only conjuncts stay as filters.
  Expression sub_where = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc3", "e1")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc1", "c1"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc3", "e1")),
                          BinaryOperation::kLessThan,
                          ConstantValueExp(Value(10))));
  QueryData query;
  query.from_ = {"Sc1"};
  query.where_ =
      ExistsSubquery({NamedExpression("e1")}, "Sc3", std::move(sub_where));
  query.select_ = {NamedExpression("c1")};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream logical;
  logical << *plan;
  EXPECT_NE(logical.str().find("Semi Join"), std::string::npos)
      << logical.str();

  Executor executor = plan->EmitExecutor(context);
  EXPECT_EQ(CountRows(executor), 9);  // c1 in 1..9
}

TEST_F(OptimizerTest, CorrelatedExistsKeepsCompositeCorrelationKey) {
  // A single-column decorrelation would incorrectly retain Sc1.c1 == 0.
  // The second equality must participate in the semi join as well.
  TransactionContext writer = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, tbl,
      rs_->CreateTable(writer,
                       Schema("Sc5", {Column("x", ValueType::kInt64),
                                      Column("y", ValueType::kDouble)})));
  ASSERT_SUCCESS(
      tbl.Insert(writer.txn_, Row({Value(0), Value(999.0)})).GetStatus());
  for (int64_t key = 1; key < 50; ++key) {
    ASSERT_SUCCESS(
        tbl.Insert(writer.txn_,
                   Row({Value(key), Value(static_cast<double>(key) + 9.9)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
  auto stats_context = rs_->BeginContext();
  ASSERT_SUCCESS(rs_->RefreshStatistics(stats_context, "Sc5"));
  ASSERT_SUCCESS(stats_context.PreCommit());

  Expression correlation = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc5", "x")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc1", "c1"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc5", "y")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc1", "c3"))));
  QueryData query;
  query.from_ = {"Sc1"};
  query.where_ =
      ExistsSubquery({NamedExpression("x")}, "Sc5", std::move(correlation));
  query.select_ = {NamedExpression("c1")};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = Optimizer::Optimize(query, context);
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  std::ostringstream dump;
  dump << *plan_or.Value();
  EXPECT_NE(dump.str().find("Semi Join"), std::string::npos) << dump.str();
  Executor executor = plan_or.Value()->EmitExecutor(context);
  EXPECT_EQ(CountRows(executor), 49);
  ASSERT_SUCCESS(context.PreCommit());

  // The same composite key machinery also applies when the projected inner
  // column supplies the positive IN probe key in addition to a correlation
  // predicate.
  auto in_statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(ColumnName("Sc5", "x"))},
      std::vector<std::string>{"Sc5"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc5", "y")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc1", "c3"))));
  QueryData in_query;
  in_query.from_ = {"Sc1"};
  in_query.where_ =
      QueryExpressionExp(std::move(in_statement),
                         ColumnValueExp(ColumnName("Sc1", "c1")), false, false);
  in_query.select_ = {NamedExpression("c1")};
  TransactionContext in_context = rs_->BeginContext();
  ASSERT_SUCCESS(in_query.Rewrite(in_context));
  const auto in_plan_or = Optimizer::Optimize(in_query, in_context);
  ASSERT_EQ(in_plan_or.GetStatus(), Status::kSuccess);
  Executor in_executor = in_plan_or.Value()->EmitExecutor(in_context);
  EXPECT_EQ(CountRows(in_executor), 49);
  ASSERT_SUCCESS(in_context.PreCommit());
}

TEST_F(OptimizerTest, NotExistsBecomesAntiJoin) {
  // Arrange: NOT EXISTS keeps rows with no match: c1 outside 1..20 -> 80.
  QueryData query;
  query.from_ = {"Sc1"};
  query.where_ = ExistsSubquery(
      {NamedExpression("e1")}, "Sc3",
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc3", "e1")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("Sc1", "c1"))),
      true);
  query.select_ = {NamedExpression("c1")};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream logical;
  logical << *plan;
  EXPECT_NE(logical.str().find("Anti Join"), std::string::npos)
      << logical.str();

  Executor executor = plan->EmitExecutor(context);
  EXPECT_EQ(CountRows(executor), 80);
}

TEST_F(OptimizerTest, NotInWithoutNotNullConstraintUsesNullAwareAntiJoin) {
  // Arrange: fixture columns carry no NOT NULL constraint, so `NOT IN` uses
  // the null-aware anti implementation rather than silently treating NULL as
  // an ordinary unmatched key.
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(ColumnName("Sc3", "e1"))},
      std::vector<std::string>{"Sc3"}, Expression());
  QueryData query;
  query.from_ = {"Sc1"};
  query.where_ =
      QueryExpressionExp(std::move(statement),
                         ColumnValueExp(ColumnName("Sc1", "c1")), false, true);
  query.select_ = {NamedExpression("c1")};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream logical;
  logical << *plan;
  EXPECT_NE(logical.str().find("Null-aware Anti Join"), std::string::npos)
      << logical.str();

  // Act + Assert: Sc3 has no NULL values, so the null-aware path has the same
  // row result as ordinary NOT IN while remaining correct if NULL is added.
  Executor executor = plan->EmitExecutor(context);
  EXPECT_EQ(CountRows(executor), 80);
}

TEST_F(OptimizerTest, NotInWithNotNullKeysBecomesAntiJoin) {
  // Arrange: both key columns are declared NOT NULL (primary keys), which is
  // exactly the gate that makes `NOT IN` -> anti join sound.
  TransactionContext writer = rs_->BeginContext();
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(
            writer,
            Schema("Po", {Column("c", ValueType::kInt64,
                                 Constraint(Constraint::kPrimaryKey))})));
    for (int i = 0; i < 20; ++i) {
      ASSERT_SUCCESS(tbl.Insert(writer.txn_, Row({Value(i)})).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl2,
        rs_->CreateTable(
            writer,
            Schema("Nn", {Column("k", ValueType::kInt64,
                                 Constraint(Constraint::kPrimaryKey))})));
    for (int i = 5; i <= 7; ++i) {
      ASSERT_SUCCESS(tbl2.Insert(writer.txn_, Row({Value(i)})).GetStatus());
    }
  }
  ASSERT_SUCCESS(rs_->RefreshStatistics(writer, "Po"));
  ASSERT_SUCCESS(rs_->RefreshStatistics(writer, "Nn"));
  ASSERT_SUCCESS(writer.txn_.PreCommit());

  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(ColumnName("Nn", "k"))},
      std::vector<std::string>{"Nn"}, Expression());
  QueryData query;
  query.from_ = {"Po"};
  query.where_ = QueryExpressionExp(
      std::move(statement), ColumnValueExp(ColumnName("Po", "c")), false, true);
  query.select_ = {NamedExpression("c")};

  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();
  std::ostringstream logical;
  logical << *plan;
  EXPECT_NE(logical.str().find("Anti Join"), std::string::npos)
      << logical.str();

  Executor executor = plan->EmitExecutor(context);
  EXPECT_EQ(CountRows(executor), 17);  // 0..19 minus 5..7
}

TEST(HashJoinKindTest, SemiJoinEmitsProbeRowOnceAndNullNeverMatches) {
  using tinylamb::Value;
  // Build side holds duplicates of 5 plus a NULL: semi emits each matching
  // probe row once regardless of duplicate build matches.
  std::vector<Row> right_rows{Row({Value(int64_t{5})}),
                              Row({Value(int64_t{5})}), Row({Value()})};
  std::vector<Row> left_rows{Row({Value(int64_t{5})}), Row({Value(int64_t{7})}),
                             Row({Value()})};
  HashJoin join(
      std::make_shared<ConstantExecutor>(left_rows), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(right_rows), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kSemi);
  Row row;
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(5));
  EXPECT_FALSE(join.Next(&row, nullptr));  // 7 unmatched, NULL never matches
}

TEST(HashJoinKindTest, AntiJoinKeepsUnmatchedRowsOnly) {
  std::vector<Row> right_rows{Row({Value(int64_t{5})})};
  std::vector<Row> left_rows{Row({Value(int64_t{5})}), Row({Value(int64_t{7})}),
                             Row({Value(int64_t{9})})};
  HashJoin join(
      std::make_shared<ConstantExecutor>(left_rows), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(right_rows), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kAnti);
  Row row;
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(7));
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(9));
  EXPECT_FALSE(join.Next(&row, nullptr));
}

TEST(HashJoinKindTest, AntiJoinEmptyBuildSideEmitsEverythingIncludingNull) {
  // `x NOT IN ()` is TRUE even for x IS NULL: every probe row survives,
  // NULL keys included.
  std::vector<Row> left_rows{Row({Value()}), Row({Value(int64_t{1})})};
  HashJoin join(
      std::make_shared<ConstantExecutor>(left_rows), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(std::vector<Row>{}),
      std::vector<slot_t>{0}, HashJoinMode::kInMemory, JoinKind::kAnti);
  Row row;
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_TRUE(row[0].IsNull());
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(1));
  EXPECT_FALSE(join.Next(&row, nullptr));
}

TEST(HashJoinKindTest, NullAwareAntiJoinRejectsAllRowsWhenBuildContainsNull) {
  std::vector<Row> left_rows{Row({Value(int64_t{1})}), Row({Value()})};
  std::vector<Row> right_rows{Row({Value(int64_t{9})}), Row({Value()})};
  HashJoin join(
      std::make_shared<ConstantExecutor>(left_rows), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(right_rows), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kNullAwareAnti);
  Row row;
  EXPECT_FALSE(join.Next(&row, nullptr));
}

TEST(HashJoinKindTest, NullAwareAntiJoinDropsNullProbeButKeepsUnmatchedValue) {
  std::vector<Row> left_rows{Row({Value()}), Row({Value(int64_t{1})}),
                             Row({Value(int64_t{9})})};
  std::vector<Row> right_rows{Row({Value(int64_t{9})})};
  HashJoin join(
      std::make_shared<ConstantExecutor>(left_rows), std::vector<slot_t>{0},
      std::make_shared<ConstantExecutor>(right_rows), std::vector<slot_t>{0},
      HashJoinMode::kInMemory, JoinKind::kNullAwareAnti);
  Row row;
  ASSERT_TRUE(join.Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(1));
  EXPECT_FALSE(join.Next(&row, nullptr));
}

namespace {

// TODO.md item 1d end-to-end: TopnDim(id PRIMARY KEY, val) and
// TopnFact(id, dim_id, payload) with every fact.dim_id present in dim.id.
// Returns the fact ids of ORDER BY TopnFact.id LIMIT 5 over the inner join.
std::vector<int64_t> RunTopNOneToOneJoin(Database* db, bool disable_rule,
                                         std::string* plan_dump) {
  QueryData query{
      {"TopnFact", "TopnDim"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("TopnFact", "dim_id")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("TopnDim", "id"))),
      {NamedExpression("id", ColumnValueExp(ColumnName("TopnFact", "id")))}};
  query.order_expressions_.push_back(
      ColumnValueExp(ColumnName("TopnFact", "id")));
  query.order_ascending_.push_back(true);
  query.limit_count_ = 5;
  TransactionContext context = db->BeginContext();
  const Status rewrite_status = query.Rewrite(context);
  EXPECT_EQ(rewrite_status, Status::kSuccess);
  if (rewrite_status != Status::kSuccess) {
    return {};
  }
  OptimizerOptions options = OptimizerOptions::Default();
  if (disable_rule) {
    EXPECT_TRUE(options.relational_rules.Remove(
        "topn_push_through_proven_one_to_one_join"));
  }
  auto plan_or = Optimizer::Optimize(query, context, options);
  EXPECT_EQ(plan_or.GetStatus(), Status::kSuccess);
  if (plan_or.GetStatus() != Status::kSuccess) {
    return {};
  }
  if (plan_dump != nullptr) {
    std::ostringstream dump;
    plan_or.Value()->Dump(dump, 0);
    *plan_dump = dump.str();
  }
  Executor executor = plan_or.Value()->EmitExecutor(context);
  std::vector<int64_t> ids;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ids.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return ids;
}

void BuildTopNOneToOneTables(Database* db) {
  TransactionContext writer = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, dim,
      db->CreateTable(
          writer,
          Schema("TopnDim", {Column("id", ValueType::kInt64,
                                    Constraint(Constraint::kPrimaryKey)),
                             Column("val", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      db->CreateTable(
          writer, Schema("TopnFact", {Column("id", ValueType::kInt64),
                                      Column("dim_id", ValueType::kInt64),
                                      Column("payload", ValueType::kInt64)})));
  for (int i = 0; i < 10; ++i) {
    ASSERT_SUCCESS(
        dim.Insert(writer.txn_, Row({Value(i), Value(i * 100)})).GetStatus());
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS(
        fact.Insert(writer.txn_, Row({Value(i), Value(i % 10), Value(i * 3)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
}

}  // namespace

TEST_F(OptimizerTest, TopNPushThroughSnapshotProvenOneToOneJoin) {
  BuildTopNOneToOneTables(rs_.get());
  const std::vector<int64_t> expected{0, 1, 2, 3, 4};
  std::string plan_dump;
  EXPECT_EQ(RunTopNOneToOneJoin(rs_.get(), /*disable_rule=*/false, &plan_dump),
            expected);
  EXPECT_NE(plan_dump.find("TopN"), std::string::npos) << plan_dump;
  // The same rows come back with the rule disabled: the push is a pure
  // performance alternative, never a semantic change.
  EXPECT_EQ(RunTopNOneToOneJoin(rs_.get(), /*disable_rule=*/true, nullptr),
            expected);
}

TEST_F(OptimizerTest, TopNPushSkippedWhenReferentialIntegrityFails) {
  BuildTopNOneToOneTables(rs_.get());
  // An unmatched key and a NULL key sort first, so an unsound push of
  // TopN(5) below the join would surface them (or truncate the output to
  // 3-4 rows). The snapshot proof must fail and the join must drop them.
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, fact,
                          writer.GetTable("TopnFact"));
    ASSERT_SUCCESS(
        fact->Insert(writer.txn_, Row({Value(-1), Value(999), Value(7)}))
            .GetStatus());
    ASSERT_SUCCESS(
        fact->Insert(writer.txn_, Row({Value(-2), Value(), Value(8)}))
            .GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  const std::vector<int64_t> expected{0, 1, 2, 3, 4};
  EXPECT_EQ(RunTopNOneToOneJoin(rs_.get(), /*disable_rule=*/false, nullptr),
            expected);
  EXPECT_EQ(RunTopNOneToOneJoin(rs_.get(), /*disable_rule=*/true, nullptr),
            expected);
}

TEST_F(OptimizerTest, ClusteringPenaltySteersSelectiveRangeScanChoice) {
  // Two 2000-row tables with an index on key: one in ascending order,
  // one permuted with adjacent comparisons balanced in both directions.
  // A selective range still wins on clustered data after charging heap
  // fetches, but its random-access penalty makes the unclustered scan lose.
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, clust,
        rs_->CreateTable(
            writer,
            Schema("ClustKeys", {Column("key", ValueType::kInt64),
                                 Column("payload", ValueType::kInt64)})));
    ASSIGN_OR_ASSERT_FAIL(
        Table, unclust,
        rs_->CreateTable(
            writer,
            Schema("UnclustKeys", {Column("key", ValueType::kInt64),
                                   Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          clust.Insert(writer.txn_, Row({Value(i), Value(i)})).GetStatus());
      ASSERT_SUCCESS(
          unclust.Insert(writer.txn_, Row({Value((i * 997) % kRows), Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(writer, "ClustKeys", IndexSchema("ClustIdx", {0})));
    ASSERT_SUCCESS(rs_->CreateIndex(writer, "UnclustKeys",
                                    IndexSchema("UnclustIdx", {0})));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  {
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, "ClustKeys"));
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, "UnclustKeys"));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  auto plan_for = [&](std::string_view table) {
    QueryData query{
        {std::string(table)},
        BinaryExpressionExp(
            BinaryExpressionExp(ColumnValueExp("key"),
                                BinaryOperation::kGreaterThanEquals,
                                ConstantValueExp(Value(int64_t{200}))),
            BinaryOperation::kAnd,
            BinaryExpressionExp(ColumnValueExp("key"),
                                BinaryOperation::kLessThan,
                                ConstantValueExp(Value(int64_t{400})))),
        {NamedExpression("key"), NamedExpression("payload")}};
    TransactionContext context = rs_->BeginContext();
    EXPECT_EQ(query.Rewrite(context), Status::kSuccess);
    auto plan_or = Optimizer::Optimize(query, context);
    EXPECT_EQ(plan_or.GetStatus(), Status::kSuccess);
    std::ostringstream dump;
    dump << plan_or.Value();
    size_t rows = 0;
    Executor executor = plan_or.Value()->EmitExecutor(context);
    Row row;
    while (executor->Next(&row, nullptr)) {
      ++rows;
    }
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
    return std::make_pair(dump.str(), rows);
  };
  const auto [clust_dump, clust_rows] = plan_for("ClustKeys");
  EXPECT_NE(clust_dump.find("IndexScan"), std::string::npos) << clust_dump;
  EXPECT_EQ(clust_rows, 200U);
  const auto [unclust_dump, unclust_rows] = plan_for("UnclustKeys");
  EXPECT_NE(unclust_dump.find("FullScan"), std::string::npos) << unclust_dump;
  EXPECT_EQ(unclust_rows, 200U);
}

TEST_F(OptimizerTest, CoveringIndexWinsOverNonCoveringIndexForTheSameRange) {
  // Two indexes over the same leading key with identical range selectivity:
  // the covering one answers the projection without a heap fetch, so it must
  // win the cost comparison even though the non-covering one was created
  // first (a cost tie keeps the first candidate found).
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(
            writer,
            Schema("CoveringChoice", {Column("key", ValueType::kInt64),
                                      Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(writer.txn_, Row({Value(i), Value(i * 2)})).GetStatus());
    }
    // Non-covering first: `SELECT key, payload` needs the heap through it.
    ASSERT_SUCCESS(rs_->CreateIndex(writer, "CoveringChoice",
                                    IndexSchema("WideIdx", {0})));
    // Covering second: the same projection is answered by the index alone.
    ASSERT_SUCCESS(rs_->CreateIndex(
        writer, "CoveringChoice",
        IndexSchema("CoverIdx", {0}, {1}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  {
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, "CoveringChoice"));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  QueryData query{
      {"CoveringChoice"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("key"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(int64_t{200}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kLessThan,
                              ConstantValueExp(Value(int64_t{1400})))),
      {NamedExpression("key"), NamedExpression("payload")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("IndexOnlyScan"), std::string::npos) << dump.str();
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, CoveringIndexRangeSkipsTheClusteringPenalty) {
  // A covering range scan reads only the index (already key-ordered): the
  // heap-seek clustering penalty must not apply, so an unclustered column's
  // covering index still beats the full scan on a wide range.
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(writer,
                         Schema("CoveringUnclustered",
                                {Column("key", ValueType::kInt64),
                                 Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(writer.txn_, Row({Value((i * 997) % kRows), Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(rs_->CreateIndex(
        writer, "CoveringUnclustered",
        IndexSchema("CoverIdx", {0}, {1}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  {
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, "CoveringUnclustered"));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  QueryData query{
      {"CoveringUnclustered"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("key"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(int64_t{200}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kLessThan,
                              ConstantValueExp(Value(int64_t{1400})))),
      {NamedExpression("key"), NamedExpression("payload")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("IndexOnlyScan"), std::string::npos) << dump.str();
  EXPECT_EQ(dump.str().find("FullScan"), std::string::npos) << dump.str();
  // The rows are still exactly the range's, and none of them are duplicated.
  Executor executor = plan->EmitExecutor(context);
  size_t rows = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ++rows;
  }
  EXPECT_EQ(rows, 1200U);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, ParallelIndexScanEmittedForLargeIndexRange) {
  // A large ascending range scan over a non-covering index partitions at
  // leaf boundaries and scans the partitions in parallel (RangePartitionedScan
  // + ParallelIndexScan); a small range stays on the serial executor.
  const std::string kTable = "BigIdxRange";
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(
            writer, Schema(kTable, {Column("key", ValueType::kInt64),
                                    Column("payload", ValueType::kInt64)})));
    for (int64_t key = 0; key < 6 * kParallelTestRows; ++key) {
      ASSERT_SUCCESS(tbl.Insert(writer.txn_, Row({Value(key), Value(key * 3)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(writer, kTable, IndexSchema("BigIdx", {0})));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  {
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, kTable));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  QueryData query{
      {kTable},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("key"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(int64_t{0}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kLessThan,
                              ConstantValueExp(Value(kParallelTestRows)))),
      {NamedExpression("key"), NamedExpression("payload")}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("ParallelIndexScan"), std::string::npos)
      << physical.str();
  // The morsel-ordered handoff delivers every row exactly once, in key
  // order.
  int64_t expected = 0;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ASSERT_EQ(row[0], Value(expected));
    ++expected;
  }
  EXPECT_EQ(expected, kParallelTestRows);
  ASSERT_SUCCESS(context.PreCommit());

  // A small range (10 rows) keeps the serial IndexScan.
  QueryData small{
      {kTable},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("key"),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(Value(kParallelTestRows - 10))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kLessThan,
                              ConstantValueExp(Value(kParallelTestRows)))),
      {NamedExpression("key"), NamedExpression("payload")}};
  TransactionContext small_ctx = rs_->BeginContext();
  ASSERT_SUCCESS(small.Rewrite(small_ctx));
  ASSIGN_OR_ASSERT_FAIL(Plan, small_plan,
                        Optimizer::Optimize(small, small_ctx));
  Executor small_executor = small_plan->EmitExecutor(small_ctx);
  std::ostringstream small_physical;
  small_executor->Dump(small_physical, 0);
  EXPECT_EQ(small_physical.str().find("ParallelIndexScan"), std::string::npos)
      << small_physical.str();
  int64_t small_rows = 0;
  while (small_executor->Next(&row, nullptr)) {
    ++small_rows;
  }
  EXPECT_EQ(small_rows, 10);
  ASSERT_SUCCESS(small_ctx.PreCommit());
}

TEST_F(OptimizerTest, InListPointUnionKeepsKeyOrderWithoutSort) {
  // Multi-range point unions scan sorted, disjoint ranges in sequence, so the
  // output is globally key-ordered: the plan must claim the order (and the
  // SQL layer therefore needs no Sort above it).
  const std::string kTable = "PointOrder";
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(
            writer, Schema(kTable, {Column("key", ValueType::kInt64),
                                    Column("payload", ValueType::kInt64)})));
    for (int64_t key = 0; key < 200; ++key) {
      ASSERT_SUCCESS(tbl.Insert(writer.txn_, Row({Value(key), Value(key * 2)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(writer, kTable, IndexSchema("PointIdx", {0})));
    ASSERT_SUCCESS(writer.PreCommit());
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, kTable));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  QueryData query{{kTable},
                  InExpressionExp(ColumnValueExp("key"),
                                  {ConstantValueExp(Value(int64_t{9})),
                                   ConstantValueExp(Value(int64_t{3})),
                                   ConstantValueExp(Value(int64_t{7}))}),
                  {NamedExpression("key"), NamedExpression("payload")}};
  query.order_expressions_ = {ColumnValueExp(ColumnName(kTable + ".key"))};
  query.order_ascending_ = {true};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("x3 points"), std::string::npos) << dump.str();
  EXPECT_TRUE(
      plan->IsOrderedBy(query.order_expressions_, query.order_ascending_))
      << dump.str();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<int64_t> keys;
  while (executor->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(keys, (std::vector<int64_t>{3, 7, 9}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, DisjunctivePrefixRangesKeepKeyOrderWithoutSort) {
  // OR-of-conjunctions over distinct key prefixes is a union of sorted,
  // disjoint ranges: the plan must claim the full key order.  Three
  // single-comparison disjuncts keep the shape intact through the rewrites
  // (OR-to-union needs exactly two branches; OR-distribution needs one side
  // to be an AND), so the disjunctive index scan is the index alternative.
  const std::string kTable = "PrefixOrder";
  {
    TransactionContext writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        rs_->CreateTable(
            writer, Schema(kTable, {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kInt64),
                                    Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(writer.txn_, Row({Value(i % 10), Value(i / 10), Value(i)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(rs_->CreateIndex(
        writer, kTable,
        IndexSchema("PrefixIdx", {0, 1}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(writer.PreCommit());
    TransactionContext stats_ctx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stats_ctx, kTable));
    ASSERT_SUCCESS(stats_ctx.PreCommit());
  }
  const auto equality = [](std::string_view column, int64_t value) {
    return BinaryExpressionExp(ColumnValueExp(column), BinaryOperation::kEquals,
                               ConstantValueExp(Value(value)));
  };
  QueryData query{
      {kTable},
      BinaryExpressionExp(
          BinaryExpressionExp(equality("a", 2), BinaryOperation::kOr,
                              equality("a", 5)),
          BinaryOperation::kOr, equality("a", 8)),
      {NamedExpression("a"), NamedExpression("b"), NamedExpression("payload")}};
  query.order_expressions_ = {ColumnValueExp(ColumnName(kTable + ".a")),
                              ColumnValueExp(ColumnName(kTable + ".b"))};
  query.order_ascending_ = {true, true};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("x3 points"), std::string::npos) << dump.str();
  EXPECT_TRUE(
      plan->IsOrderedBy(query.order_expressions_, query.order_ascending_))
      << dump.str();
  Executor executor = plan->EmitExecutor(context);
  Row row;
  std::vector<std::pair<int64_t, int64_t>> rows;
  while (executor->Next(&row, nullptr)) {
    rows.emplace_back(row[0].value.int_value, row[1].value.int_value);
  }
  ASSERT_EQ(rows.size(), 60U);
  for (size_t i = 1; i < rows.size(); ++i) {
    EXPECT_LT(rows[i - 1], rows[i]) << "row " << i;
  }
  EXPECT_EQ(rows.front(), (std::pair<int64_t, int64_t>{2, 0}));
  EXPECT_EQ(rows.back(), (std::pair<int64_t, int64_t>{8, 19}));
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, OrderedOuterDiscountsTheIndexProbeIntoAWin) {
  // Lookup locality (docs/todo.md Phase 2 "ソート済み外側からのlookup
  // locality"). Two outer tables hold the same 100 rows; only one of them has
  // an index on the join key, so only that one can hand the probe its keys in
  // probe order. The filter keeps 20 outer rows and the inner side has 37 rows
  // with a unique key: the ordered probe (0.75x -> 55) beats the best hash
  // orientation (37 + 20 = 57) while the unordered probe (60) loses to it, so
  // the chosen shape IS the evidence that the discount exists.
  TransactionContext setup = rs_->BeginContext();
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, ordered_outer,
        rs_->CreateTable(
            setup,
            Schema("LocOrdered", {Column("k", ValueType::kInt64),
                                  Column("payload", ValueType::kInt64)})));
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(ordered_outer.Insert(setup.txn_, Row({Value(i), Value(i)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(rs_->CreateIndex(setup, "LocOrdered",
                                    IndexSchema("LocOrderedPK", {0})));
  }
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, plain_outer,
        rs_->CreateTable(
            setup, Schema("LocPlain", {Column("k", ValueType::kInt64),
                                       Column("payload", ValueType::kInt64)})));
    for (int i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(plain_outer.Insert(setup.txn_, Row({Value(i), Value(i)}))
                         .GetStatus());
    }
  }
  {
    ASSIGN_OR_ASSERT_FAIL(
        Table, inner,
        rs_->CreateTable(setup,
                         Schema("LocInner", {Column("k", ValueType::kInt64),
                                             Column("v", ValueType::kInt64)})));
    for (int i = 80; i < 117; ++i) {
      ASSERT_SUCCESS(
          inner.Insert(setup.txn_, Row({Value(i), Value(i * 10)})).GetStatus());
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(setup, "LocInner", IndexSchema("LocInnerPK", {0})));
  }
  rs_->RefreshStatistics(setup, "LocOrdered");
  rs_->RefreshStatistics(setup, "LocPlain");
  rs_->RefreshStatistics(setup, "LocInner");
  ASSERT_SUCCESS(setup.txn_.PreCommit());

  // Returns the plan text and the emitted rows for one outer table.
  const auto plan_and_rows = [&](const std::string& outer) {
    QueryData query{
        {outer, "LocInner"},
        BinaryExpressionExp(
            BinaryExpressionExp(ColumnValueExp(ColumnName(outer, "k")),
                                BinaryOperation::kEquals,
                                ColumnValueExp(ColumnName("LocInner", "k"))),
            BinaryOperation::kAnd,
            BinaryExpressionExp(ColumnValueExp(ColumnName(outer, "k")),
                                BinaryOperation::kGreaterThanEquals,
                                ConstantValueExp(Value(80)))),
        {NamedExpression("k", ColumnValueExp(ColumnName(outer, "k"))),
         NamedExpression("v", ColumnValueExp(ColumnName("LocInner", "v")))}};
    TransactionContext context = rs_->BeginContext();
    EXPECT_SUCCESS(query.Rewrite(context));
    StatusOr<Plan> optimized = Optimizer::Optimize(query, context);
    EXPECT_TRUE(optimized.HasValue());
    const Plan& plan = optimized.Value();
    std::ostringstream dump;
    plan->Dump(dump, 0);
    std::vector<std::pair<int64_t, int64_t>> rows;
    Executor executor = plan->EmitExecutor(context);
    Row row;
    while (executor->Next(&row, nullptr)) {
      rows.emplace_back(row[0].value.int_value, row[1].value.int_value);
    }
    EXPECT_SUCCESS(context.PreCommit());
    return std::pair<std::string, std::vector<std::pair<int64_t, int64_t>>>{
        dump.str(), std::move(rows)};
  };

  const auto [ordered_text, ordered_rows] = plan_and_rows("LocOrdered");
  const auto [plain_text, plain_rows] = plan_and_rows("LocPlain");
  // Same answer either way -- the discount may only change the shape.
  EXPECT_EQ(ordered_rows, plain_rows);
  EXPECT_EQ(ordered_rows.size(), 20U);
  // The ordered outer side keeps the index nested-loop; without an order to
  // offer, the same query loses to the hash join.
  EXPECT_NE(ordered_text.find("Index Join"), std::string::npos) << ordered_text;
  EXPECT_EQ(plain_text.find("Index Join"), std::string::npos) << plain_text;
}

TEST_F(OptimizerTest, UnboundedNonCoveringIndexPaysForHeapAccess) {
  QueryData query{{"Sc1"},
                  nullptr,
                  {NamedExpression("key", ColumnValueExp("c1")),
                   NamedExpression("payload", ColumnValueExp("c2")),
                   NamedExpression("score", ColumnValueExp("c3"))}};
  auto context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("FullScan"), std::string::npos) << dump.str();
  auto executor = plan->EmitExecutor(context);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    EXPECT_EQ(row.values_.size(), 3);
    ++count;
  }
  EXPECT_EQ(count, 100);
  ASSERT_SUCCESS(executor->GetStatus());
}

TEST_F(OptimizerTest, MixedDirectionTopNUsesStreamingOrderedPrefix) {
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer, Schema("PrefixTopN", {Column("x", ValueType::kInt64),
                                          Column("y", ValueType::kInt64)})));
    for (int64_t i = 0; i < 1000; ++i) {
      ASSERT_SUCCESS(
          table.Insert(writer.txn_, Row{Value(i / 10), Value(i % 10)})
              .GetStatus());
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(writer, "PrefixTopN",
                         IndexSchema("xy", {0, 1}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(rs_->RefreshStatistics(writer, "PrefixTopN"));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  QueryData query{
      {"PrefixTopN"}, nullptr, {NamedExpression("x"), NamedExpression("y")}};
  query.order_expressions_ = {ColumnValueExp("x"), ColumnValueExp("y")};
  query.order_ascending_ = {false, true};
  query.limit_count_ = 5;
  auto context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("IncrementalSort"), std::string::npos)
      << dump.str();
  auto executor = plan->EmitExecutor(context);
  Row row;
  for (int64_t y = 0; y < 5; ++y) {
    ASSERT_TRUE(executor->Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(99), Value(y)}));
  }
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(executor->GetStatus());
}

TEST_F(OptimizerTest, FilteredOrderedLimitUsesIndexAfterLayoutRewrite) {
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer,
            Schema("ResidualLimit", {Column("id", ValueType::kInt64),
                                     Column("bucket", ValueType::kInt64),
                                     Column("payload", ValueType::kInt64)})));
    for (int64_t i = 0; i < 10000; ++i) {
      ASSIGN_OR_ASSERT_FAIL(
          RowPosition, position,
          table.Insert(writer.txn_,
                       Row({Value(i), Value(i % 100), Value(10)})));
      if (i == 0) {
        ASSERT_SUCCESS(table
                           .Update(writer.txn_, position,
                                   Row({Value(i), Value(i % 100), Value(11)}))
                           .GetStatus());
      }
    }
    ASSERT_SUCCESS(
        rs_->CreateIndex(writer, "ResidualLimit",
                         IndexSchema("pk", {0}, {}, IndexMode::kUnique)));
    ASSERT_SUCCESS(rs_->RefreshStatistics(writer, "ResidualLimit"));
    ASSERT_SUCCESS(writer.PreCommit());
  }
  QueryData query{
      {"ResidualLimit"},
      BinaryExpressionExp(ColumnValueExp("bucket"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(7))),
      {NamedExpression("id"), NamedExpression("payload")}};
  query.order_expressions_ = {ColumnValueExp("id")};
  query.order_ascending_ = {true};
  query.limit_count_ = 3;
  auto context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_NE(dump.str().find("IndexScan"), std::string::npos) << dump.str();
  const uint64_t hits_before = rs_->CacheHits();
  auto executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_EQ(physical.str().find("ParallelIndexScan"), std::string::npos)
      << physical.str();
  Row row;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(executor->Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(7 + (i * 100)), Value(10)}));
  }
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(executor->GetStatus());
  EXPECT_LT(rs_->CacheHits() - hits_before, 5000U);
}

TEST_F(OptimizerTest, ReboundFirstKeyUpperRangeKeepsCurrentEndpoint) {
  auto context = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, context.GetTable("Sc1"));
  const Index* index = nullptr;
  for (size_t i = 0; i < table->IndexCount(); ++i) {
    if (table->GetIndex(i).sc_.key_ == std::vector<slot_t>{0}) {
      index = &table->GetIndex(i);
    }
  }
  ASSERT_NE(index, nullptr);
  auto bound = std::make_shared<ParameterSlot>(0, Value(2));
  const Expression predicate = BinaryExpressionExp(
      ColumnValueExp("c1"), BinaryOperation::kLessThan, bound);
  for (int64_t upper : {2, 7, -1}) {
    bound->Bind(Value(upper));
    const auto keys = ReboundIndexKeysFromPredicate(
        predicate, table->GetSchema(), *index, false);
    EXPECT_TRUE(keys.begin_key.empty());
    EXPECT_EQ(keys.end_key, std::vector<Value>{Value(upper)});
  }
}

}  // namespace tinylamb
