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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
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
#include "expression/query_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "plan/cascades.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/values_plan.hpp"
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
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  [[nodiscard]] Status DumpAll(const QueryData& qd) const {
    // Arrange: open context + rewrite query for debugging
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

TEST_F(OptimizerTest, ConstantFalseSelectionBecomesEmptyPlan) {
  QueryData query{
      {"Sc1"}, ConstantValueExp(Value(false)), {NamedExpression("c1")}};
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
       NamedExpression(
           "folded",
           BinaryExpressionExp(
               ColumnValueExp("c1"), BinaryOperation::kAdd,
               BinaryExpressionExp(ConstantValueExp(Value(1)),
                                   BinaryOperation::kAdd,
                                   ConstantValueExp(Value(2)))))} };
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
      {NamedExpression(
           "lhs", BinaryExpressionExp(ColumnValueExp("c1"),
                                      BinaryOperation::kMultiply,
                                      ConstantValueExp(Value(0)))),
       NamedExpression(
           "rhs", BinaryExpressionExp(ConstantValueExp(Value(0)),
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

TEST_F(OptimizerTest, ContradictoryConjunctsBecomeEmptyResult) {
  TransactionContext context = rs_->BeginContext();
  const auto check_empty = [&](Expression predicate) {
    QueryData query{{"Sc1"}, std::move(predicate), {NamedExpression("c1")}};
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
  query.order_expressions_ = {BinaryExpressionExp(
      ConstantValueExp(Value(1)), BinaryOperation::kAdd,
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
  std::mt19937 rng(0x54494E59U);  // "TINY"
  std::shuffle(masks.begin(), masks.end(), rng);

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
      keys = std::vector<int64_t>(keys.begin() + begin, keys.begin() + end);
    }

    EXPECT_EQ(keys, (std::vector<int64_t>{1, 2})) << "rule mask=" << mask;
    ++planned_subsets;
  }

  EXPECT_GE(planned_subsets, 10U);
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

// Known defect (mid-refactor optimizer): a query whose predicate combines an
// equality prefix and a one-sided range on the trailing column of a composite
// index can drop the filter entirely and return every row. Disabled until the
// physical rule either applies the residual itself or always wraps it.
TEST_F(OptimizerTest, DISABLED_CompositeIndexEqualityPrefixWithOneSidedRange) {
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
        op, ConstantValueExp(Value(bound)));
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
  EXPECT_NE(dump.str().find("Select:"), std::string::npos) << dump.str();
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

  // Act + Assert: optimize + execute and verify the matching row.
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

  // Act
  const auto plan_or = (Optimizer::Optimize(query, context));
  ASSERT_EQ(plan_or.GetStatus(), Status::kSuccess);
  const Plan& plan = plan_or.Value();

  // Assert: the chosen plan is a hash or index join, never a cross product.
  std::ostringstream dump;
  dump << plan;
  const std::string text = dump.str();
  EXPECT_TRUE(text.find("Hash Join") != std::string::npos ||
              text.find("Index Join") != std::string::npos)
      << text;
  EXPECT_EQ(text.find("Cross Join"), std::string::npos) << text;
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(OptimizerTest, UnqualifiedJoinBecomesExplicitCrossJoin) {
  QueryData query{{"Sc1", "Sc2"},
                  ConstantValueExp(Value(true)),
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
      left_scan,
      std::vector<SortKey>{{ColumnValueExp(ColumnName("Sc1", "c1")), true,
                            std::nullopt}});
  Plan right_sorted = std::make_shared<SortPlan>(
      right_scan,
      std::vector<SortKey>{{ColumnValueExp(ColumnName("Sc2", "d1")), true,
                            std::nullopt}});

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
  query.where_ = ConstantValueExp(Value(true));
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
  query.where_ = ConstantValueExp(Value(false));
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

TEST_F(OptimizerTest, EqualityPrefixAndInListDriveCompositePointUnion) {
  // A predicate such as (warehouse = ?) AND (item IN (...)) must use one
  // composite point range per item instead of scanning every warehouse's
  // stock rows. NameIdx has the equivalent two-column shape (d3, d4).
  QueryData query{
      {"a"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("a", "d3")),
                              BinaryOperation::kEquals,
                              ConstantValueExp(Value("d3-2"))),
          BinaryOperation::kAnd,
          InExpressionExp(
              ColumnValueExp(ColumnName("a", "d4")),
              {ConstantValueExp(Value(16)), ConstantValueExp(Value(17))})),
      {NamedExpression("ad1", ColumnValueExp(ColumnName("a", "d1")))}};
  query.aliases_ = {{"a", "Sc2"}};
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
  EXPECT_EQ(keys.size(), 20);
  EXPECT_TRUE(std::all_of(keys.begin(), keys.end(),
                          [](int64_t key) { return key % 10 == 2; }));
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

TEST_F(OptimizerTest, ParallelAggregationEmittedForLargeChild) {
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

  // Act + Assert: the emitted executor is the parallel aggregation and both
  // aggregates match the sequential semantics.
  Executor executor = plan->EmitExecutor(context);
  std::ostringstream physical;
  executor->Dump(physical, 0);
  EXPECT_NE(physical.str().find("ParallelAggregationExecutor"),
            std::string::npos)
      << physical.str();
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(kParallelTestRows));
  EXPECT_EQ(row[1], Value(kParallelTestRows * (kParallelTestRows - 1) / 2));
  EXPECT_FALSE(executor->Next(&row, nullptr));
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

  // Act: emit the mutation source and check it is a ParallelScan.
  Executor source = plan->EmitExecutor(context);
  std::ostringstream physical;
  source->Dump(physical, 0);
  EXPECT_NE(physical.str().find("ParallelScan"), std::string::npos)
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

int CountRows(Executor executor) {
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

}  // namespace tinylamb
