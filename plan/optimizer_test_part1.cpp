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

#include "plan/optimizer_test.hpp"

namespace tinylamb {

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
  physical_masks.reserve(1U << physical_rules.size());
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
    values.reserve(end > begin ? static_cast<size_t>(end - begin) : 0);
    for (int64_t i = begin; i < end; ++i) {
      values.push_back(i);
    }
    return values;
  };

  // Act + Assert: NOT-EQUALS never narrows a range; upper/lower bounds and
  // constant-on-left comparisons all produce the matching key sets.
  std::vector<int64_t> all_but_five;
  all_but_five.reserve(100);
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

TEST_F(OptimizerTest, AuditRangeIndexAppliesResidualExactlyOnce) {
  auto context = rs_->BeginContext();
  cascades::RuleContext rule_context;
  rule_context.transaction = &context;
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, context.GetTable("Sc1"));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, statistics,
                        context.GetStats("Sc1"));
  rule_context.tables.emplace("Sc1", table);
  rule_context.statistics.emplace("Sc1", statistics);
  const cascades::ImplementationRule* scan_rule = nullptr;
  for (const auto& rule : DefaultImplementationRules().Rules()) {
    if (rule.Name() == "index_scan") {
      scan_rule = &rule;
    }
  }
  ASSERT_NE(scan_rule, nullptr);
  auto bound = std::make_shared<ParameterSlot>(0, Value(3));
  auto excluded = std::make_shared<ParameterSlot>(1, Value("c2-1"));
  auto predicate = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c1")),
                          BinaryOperation::kLessThan, bound),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp(ColumnName("Sc1", "c2")),
                          BinaryOperation::kNotEquals, excluded));
  cascades::Memo memo;
  const auto group = memo.Build({"Sc1"}, {{predicate, {"Sc1"}}});
  const auto alternatives =
      scan_rule->Apply(memo, group, memo.Get(group).expressions.front(), {},
                       cascades::PhysicalProperties{}, rule_context);
  ASSERT_FALSE(alternatives.empty());
  for (const auto& alternative : alternatives) {
    bound->Bind(Value(3));
    excluded->Bind(Value("c2-1"));
    std::ostringstream dump;
    auto executor = alternative.plan->EmitExecutor(context);
    executor->Dump(dump, 0);
    // The non-covering IndexScan emits one Selection for the residual.
    // A second Selection would evaluate it again on every surviving row.
    const size_t first = dump.str().find("Selection:");
    if (first != std::string::npos) {
      EXPECT_EQ(dump.str().find("Selection:", first + 1), std::string::npos)
          << dump.str();
    }
    Row row;
    std::vector<int64_t> keys;
    while (executor->Next(&row, nullptr)) {
      keys.push_back(row[0].value.int_value);
    }
    ASSERT_SUCCESS(executor->GetStatus());
    std::ranges::sort(keys);
    EXPECT_EQ(keys, (std::vector<int64_t>{0, 2}));
    size_t cursor = 0;
    std::vector<Expression> slots;
    ASSERT_TRUE(alternative.plan->InstallParameterSlots(
        {Value(3), Value("c2-1")}, &cursor, &slots));
    ASSERT_EQ(slots.size(), 2U);
    const std::vector<Value> rebound{Value(2), Value("c2-0")};
    for (const Expression& slot : slots) {
      auto parameter = std::dynamic_pointer_cast<ParameterSlot>(slot);
      ASSERT_NE(parameter, nullptr);
      parameter->Bind(rebound.at(parameter->Index()));
    }
    auto replay = alternative.plan->EmitExecutor(context);
    std::ostringstream replay_dump;
    replay->Dump(replay_dump, 0);
    const size_t replay_first = replay_dump.str().find("Selection:");
    if (replay_first != std::string::npos) {
      EXPECT_EQ(replay_dump.str().find("Selection:", replay_first + 1),
                std::string::npos)
          << replay_dump.str();
    }
    keys.clear();
    while (replay->Next(&row, nullptr)) {
      keys.push_back(row[0].value.int_value);
    }
    ASSERT_SUCCESS(replay->GetStatus());
    EXPECT_EQ(keys, (std::vector<int64_t>{1}));
  }
}

TEST_F(OptimizerTest, ResidualPredicateSurvivesIndexAccessChoice) {
  // The predicate touches columns across index keys. Whichever access path
  // wins must still evaluate the complete residual.
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

}  // namespace tinylamb
