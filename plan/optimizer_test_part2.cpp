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

TEST_F(OptimizerTest, LookupCostIncludesFilteredOutInnerCandidates) {
  auto setup = rs_->BeginContext();
  for (const std::string name : {"LookupOuter", "LookupInner"}) {
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(setup,
                         Schema(name, {Column("k", ValueType::kInt64),
                                       Column("g", ValueType::kInt64)})));
    const int count = name == "LookupOuter" ? 30 : 300;
    for (int k = 0; k < count; ++k) {
      ASSERT_TRUE(
          table.Insert(setup.txn_, Row({Value(k), Value(k % 3)})).HasValue());
    }
  }
  ASSERT_SUCCESS(rs_->CreateIndex(
      setup, "LookupInner",
      IndexSchema("LookupGroup", {1}, {}, IndexMode::kNonUnique)));
  ASSERT_SUCCESS(rs_->RefreshStatistics(setup, "LookupInner"));
  // The outer table intentionally retains an unknown (zero) row estimate.
  ASSERT_SUCCESS(setup.PreCommit());
  QueryData query{
      {"LookupOuter", "LookupInner"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("LookupOuter", "g")),
                              BinaryOperation::kEquals,
                              ColumnValueExp(ColumnName("LookupInner", "g"))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(ColumnName("LookupInner", "k")),
                              BinaryOperation::kLessThan,
                              ConstantValueExp(Value(2)))),
      {NamedExpression("outer_k",
                       ColumnValueExp(ColumnName("LookupOuter", "k"))),
       NamedExpression("inner_k",
                       ColumnValueExp(ColumnName("LookupInner", "k")))}};
  auto reader = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(reader));
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, reader));
  std::ostringstream dump;
  plan->Dump(dump, 0);
  EXPECT_EQ(dump.str().find("Index Join"), std::string::npos) << dump.str();
  auto executor = plan->EmitExecutor(reader);
  Row row;
  size_t count = 0;
  while (executor->Next(&row, nullptr)) {
    EXPECT_LT(row[1].value.int_value, 2);
    ++count;
  }
  EXPECT_EQ(count, 20U);
  EXPECT_SUCCESS(executor->GetStatus());
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
  // SelectionPlan fallback is gone.
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
  // the plan and the lazy pipeline stops after five rows.
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
  keys.reserve(5);
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
  ASSERT_SUCCESS(rs_->RefreshStatistics(stat_ctx, "ProbeLeft"));
  ASSERT_SUCCESS(rs_->RefreshStatistics(stat_ctx, "ProbeRight"));
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
  ASSERT_SUCCESS(rs_->RefreshStatistics(stat_ctx, "IntervalLeft"));
  ASSERT_SUCCESS(rs_->RefreshStatistics(stat_ctx, "IntervalRight"));
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

// A second range column, a range the type allowlist rejects and an equality on
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
  ASSERT_SUCCESS(rs_->RefreshStatistics(stat_ctx, "ProbeLeft"));
  ASSERT_SUCCESS(rs_->RefreshStatistics(stat_ctx, "ProbeRight"));
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

}  // namespace tinylamb
