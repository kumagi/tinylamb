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
#include <sstream>
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
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
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
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, writer.GetTable("Sc1"));

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
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, old_reader));
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
  ASSIGN_OR_ASSERT_FAIL(Plan, full_plan,
                        Optimizer::Optimize(query, context, full_scan_only));
  std::ostringstream full_dump;
  full_dump << full_plan;
  EXPECT_NE(full_dump.str().find("FullScan"), std::string::npos);

  OptimizerOptions index_scan_only = OptimizerOptions::Default();
  index_scan_only.disabled_implementation_rules.insert("full_scan");
  ASSIGN_OR_ASSERT_FAIL(Plan, index_plan,
                        Optimizer::Optimize(query, context, index_scan_only));
  std::ostringstream index_dump;
  index_dump << index_plan;
  EXPECT_NE(index_dump.str().find("IndexScan"), std::string::npos);
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
  auto run = [&](const Expression& predicate, std::vector<int64_t> expected) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    ASSERT_SUCCESS(query.Rewrite(context));
    ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
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
    for (int64_t i = begin; i < end; ++i) values.push_back(i);
    return values;
  };

  // Act + Assert: NOT-EQUALS never narrows a range; upper/lower bounds and
  // constant-on-left comparisons all produce the matching key sets.
  std::vector<int64_t> all_but_five;
  for (int64_t i = 0; i < 100; ++i) {
    if (i != 5) all_but_five.push_back(i);
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
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
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
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));

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

TEST_F(OptimizerTest, SelectStarMultipleTablesThrows) {
  // Arrange: SELECT * over more than one table is rejected by ExpandSelect.
  QueryData query{{"Sc1", "Sc2"}, nullptr, {NamedExpression("*")}};
  TransactionContext context = rs_->BeginContext();

  // Act + Assert: the raw query (no Rewrite) throws at optimization time.
  EXPECT_THROW(Optimizer::Optimize(query, context), std::runtime_error);
  context.Abort();
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
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
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
      {NamedExpression(
          "sum_score",
          AggregateExpressionExp(AggregationType::kSum,
                                 ColumnValueExp("c3")))}};
  TransactionContext context = rs_->BeginContext();
  ASSERT_SUCCESS(query.Rewrite(context));

  // Act + Assert: optimize + execute, the single matching row c1=2 has
  // c3 = 2 + 9.9 = 11.9.
  ASSIGN_OR_ASSERT_FAIL(Plan, plan, Optimizer::Optimize(query, context));
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
           AggregateExpressionExp(AggregationType::kSum,
                                  ColumnValueExp("c3"))),
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
  auto run = [&](const Expression& predicate, std::vector<int64_t> expected) {
    QueryData query{{"Sc1"}, predicate, {NamedExpression("c1")}};
    ASSERT_SUCCESS(query.Rewrite(context));
    ASSIGN_OR_ASSERT_FAIL(Plan, plan,
                          Optimizer::Optimize(query, context, options));
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
    for (int64_t i = begin; i < end; ++i) values.push_back(i);
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
  options.extra_implementation_rules.push_back(cascades::ImplementationRule(
      "extra_scan_probe", cascades::dsl::Scan(),
      [](const cascades::Bindings&, const cascades::LogicalExpression&,
         const std::vector<cascades::BestPlan>&,
         const cascades::PhysicalProperties&) {
        return std::vector<cascades::PlanAlternative>{};
      }));

  // Act + Assert: the customized rule set still optimizes and the query returns
  // the expected row.
  ASSIGN_OR_ASSERT_FAIL(Plan, plan,
                        Optimizer::Optimize(query, context, options));
  Executor executor = plan->EmitExecutor(context);
  Row row;
  ASSERT_TRUE(executor->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value(2));
  EXPECT_FALSE(executor->Next(&row, nullptr));
  ASSERT_SUCCESS(context.PreCommit());
}
}  // namespace tinylamb
