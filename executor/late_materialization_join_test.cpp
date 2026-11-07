/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/late_materialization_join.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/full_scan.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "index/index_schema.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/late_materialization_join_plan.hpp"
#include "plan/values_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

class FailingSource final : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* dst, size_t /*max_rows*/) override {
    dst->Reset();
    return 0;
  }
  void Dump(std::ostream& output, int /*indent*/) const override {
    output << "FailingSource";
  }
  [[nodiscard]] Status GetStatus() const override {
    return StatusError(StatusCode::kRuntimeError, "late source failure");
  }
};

class ErrorExpression final : public ExpressionBase {
 public:
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kInvalid; }
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override {
    return {};
  }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& /*row*/, const Schema& /*schema*/) const override {
    return StatusError(StatusCode::kInvalidArgument, "late residual failure");
  }
  [[nodiscard]] std::string ToString() const override { return "ERROR()"; }
  void Dump(std::ostream& /*output*/) const override {}
};

std::vector<Row> Drain(ExecutorBase* executor,
                       std::vector<RowPosition>* positions = nullptr) {
  std::vector<Row> rows;
  Row row;
  RowPosition position;
  while (executor->Next(&row, &position)) {
    rows.push_back(row);
    if (positions != nullptr) {
      positions->push_back(position);
    }
    row = Row();
  }
  return rows;
}

Schema LeftSchema() {
  return Schema("LMLeft", {Column("k", ValueType::kInt64),
                           Column("tag", ValueType::kInt64)});
}

Schema RightSchema(const std::string& name = "LMRight") {
  return Schema(
      name, {Column("k", ValueType::kInt64), Column("p1", ValueType::kInt64),
             Column("p2", ValueType::kInt64), Column("p3", ValueType::kInt64),
             Column("p4", ValueType::kInt64), Column("p5", ValueType::kInt64)});
}

class LateMaterializationJoinTest : public ::testing::Test {
 public:
  void SetUp() override {
    database_ =
        Database::Create("late_materialization_join_test-" + RandomString())
            .MoveValue();
    TransactionContext setup = database_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, left,
                          database_->CreateTable(setup, LeftSchema()));
    ASSIGN_OR_ASSERT_FAIL(Table, right,
                          database_->CreateTable(setup, RightSchema()));
    for (const Row& row :
         {Row({Value(1), Value(10)}), Row({Value(1), Value(11)}),
          Row({Value(), Value(12)}), Row({Value(2), Value(20)})}) {
      ASSERT_SUCCESS(left.Insert(setup.txn_, row).GetStatus());
    }
    for (const Row& row : {Row({Value(1), Value(100), Value(101), Value(102),
                                Value(103), Value(104)}),
                           Row({Value(1), Value(110), Value(111), Value(112),
                                Value(113), Value(114)}),
                           Row({Value(2), Value(200), Value(201), Value(202),
                                Value(203), Value(204)}),
                           Row({Value(), Value(900), Value(901), Value(902),
                                Value(903), Value(904)})}) {
      ASSERT_SUCCESS(right.Insert(setup.txn_, row).GetStatus());
    }
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "LMRight",
        IndexSchema("lm_right_k", {0}, {}, IndexMode::kNonUnique)));
    ASSIGN_OR_ASSERT_FAIL(
        Table, unique_table,
        database_->CreateTable(setup, RightSchema("LMRightUnique")));
    ASSERT_SUCCESS(
        unique_table
            .Insert(setup.txn_, Row({Value(1), Value(301), Value(302),
                                     Value(303), Value(304), Value(305)}))
            .GetStatus());
    ASSERT_SUCCESS(
        unique_table
            .Insert(setup.txn_, Row({Value(2), Value(401), Value(402),
                                     Value(403), Value(404), Value(405)}))
            .GetStatus());
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "LMRightUnique",
        IndexSchema("lm_right_unique_k", {0}, {}, IndexMode::kUnique)));
    ASSIGN_OR_ASSERT_FAIL(
        Table, empty_table,
        database_->CreateTable(setup, RightSchema("LMEmpty")));
    ASSERT_SUCCESS(database_->CreateIndex(
        setup, "LMEmpty",
        IndexSchema("lm_empty_k", {0}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(setup.txn_.PreCommit());

    TransactionContext statistics_context = database_->BeginContext();
    database_->RefreshStatistics(statistics_context, "LMLeft");
    database_->RefreshStatistics(statistics_context, "LMRight");
    database_->RefreshStatistics(statistics_context, "LMRightUnique");
    database_->RefreshStatistics(statistics_context, "LMEmpty");
    ASSERT_SUCCESS(statistics_context.txn_.PreCommit());

    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    left_table_ = context_->GetTable("LMLeft").MoveValue();
    right_table_ = context_->GetTable("LMRight").MoveValue();
    unique_table_ = context_->GetTable("LMRightUnique").MoveValue();
    empty_table_ = context_->GetTable("LMEmpty").MoveValue();
    left_statistics_ = context_->GetStats("LMLeft").MoveValue();
    right_statistics_ = context_->GetStats("LMRight").MoveValue();
  }

  void TearDown() override {
    if (context_ != nullptr && !context_->IsFinished()) {
      context_->txn_.PreCommit();
    }
    database_->DeleteAll();
  }

  Executor LeftScan() {
    return std::make_shared<FullScan>(context_->txn_, *left_table_);
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::shared_ptr<Table> left_table_;
  std::shared_ptr<Table> right_table_;
  std::shared_ptr<Table> unique_table_;
  std::shared_ptr<Table> empty_table_;
  std::shared_ptr<TableStatistics> left_statistics_;
  std::shared_ptr<TableStatistics> right_statistics_;
};

TEST(LateMaterializationJoinTypeTest, OnlySignedIntegerLikeKeysAreSupported) {
  EXPECT_TRUE(LateMaterializationJoin::SupportsKeyType(ValueType::kInt64));
  EXPECT_TRUE(LateMaterializationJoin::SupportsKeyType(ValueType::kDate));
  EXPECT_FALSE(LateMaterializationJoin::SupportsKeyType(ValueType::kDouble));
  EXPECT_FALSE(LateMaterializationJoin::SupportsKeyType(ValueType::kVarChar));
  EXPECT_FALSE(LateMaterializationJoin::SupportsKeyType(ValueType::kArray));
}

TEST_F(LateMaterializationJoinTest, EquiIdentityDefersPayloadAndPreservesRows) {
  const Schema output = LeftSchema() + RightSchema();
  LateMaterializationJoin join(context_->txn_, LeftScan(), {0}, *right_table_,
                               right_table_->GetIndex(0), {0}, output, nullptr,
                               false, {0, 1, 2, 3, 4, 5});
  std::vector<RowPosition> positions;
  const std::vector<Row> rows = Drain(&join, &positions);

  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0], (Row({Value(1), Value(10), Value(1), Value(100),
                           Value(101), Value(102), Value(103), Value(104)})));
  EXPECT_EQ(rows[1], (Row({Value(1), Value(10), Value(1), Value(110),
                           Value(111), Value(112), Value(113), Value(114)})));
  EXPECT_EQ(rows[2], (Row({Value(1), Value(11), Value(1), Value(100),
                           Value(101), Value(102), Value(103), Value(104)})));
  EXPECT_EQ(rows[3], (Row({Value(1), Value(11), Value(1), Value(110),
                           Value(111), Value(112), Value(113), Value(114)})));
  EXPECT_EQ(rows[4], (Row({Value(2), Value(20), Value(2), Value(200),
                           Value(201), Value(202), Value(203), Value(204)})));
  ASSERT_EQ(positions.size(), rows.size());
  std::vector<RowPosition> left_positions;
  Iterator left_iterator = left_table_->BeginFullScan(context_->txn_);
  while (left_iterator.IsValid()) {
    left_positions.push_back(left_iterator.Position());
    ++left_iterator;
  }
  ASSERT_EQ(left_positions.size(), 4U);
  const std::vector<RowPosition> expected_positions{
      left_positions[0], left_positions[0], left_positions[1],
      left_positions[1], left_positions[3]};
  EXPECT_EQ(positions, expected_positions);
  EXPECT_EQ(join.CandidateCount(), 5U);
  EXPECT_EQ(join.PayloadFetchCount(), 5U);
  std::stringstream dump;
  join.Dump(dump, 0);
  EXPECT_NE(dump.str().find("LateMaterializationJoin"), std::string::npos);
  EXPECT_NE(dump.str().find("identity=equi-key"), std::string::npos);
  EXPECT_NE(dump.str().find("LateMaterializePayload"), std::string::npos);
}

TEST_F(LateMaterializationJoinTest, UnsupportedRuntimeKeyTypeIsRejected) {
  const Schema output = LeftSchema() + RightSchema();
  LateMaterializationJoin unsupported(
      context_->txn_, std::make_shared<ConstantExecutor>(Row({Value(1.0)})),
      {0}, *right_table_, right_table_->GetIndex(0), {0}, output, nullptr,
      false, {0, 1, 2, 3, 4, 5});
  Row row;
  RowPosition position;
  EXPECT_FALSE(unsupported.Next(&row, &position));
  EXPECT_EQ(unsupported.GetStatus(), Status::kInvalidArgument);
}

TEST_F(LateMaterializationJoinTest, UniqueWinnerAndEmptyRightKeepContracts) {
  const Schema output = LeftSchema() + RightSchema();
  LateMaterializationJoin unique_join(context_->txn_, LeftScan(), {0},
                                      *unique_table_,
                                      unique_table_->GetIndex(0), {0}, output,
                                      nullptr, true, {0, 1, 2, 3, 4, 5});
  const std::vector<Row> unique_rows = Drain(&unique_join);
  EXPECT_EQ(unique_join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(unique_rows.size(), 3U);
  EXPECT_EQ(unique_rows[0][3], Value(301));
  EXPECT_EQ(unique_rows[1][3], Value(301));
  EXPECT_EQ(unique_rows[2][3], Value(401));

  LateMaterializationJoin empty_join(
      context_->txn_, LeftScan(), {0}, *empty_table_, empty_table_->GetIndex(0),
      {0}, output, nullptr, false, {0, 1, 2, 3, 4, 5});
  EXPECT_TRUE(Drain(&empty_join).empty());
  EXPECT_EQ(empty_join.GetStatus(), Status::kSuccess);
}

TEST_F(LateMaterializationJoinTest,
       BatchPathMatchesRowPathAndChildErrorIsSticky) {
  const Schema output = LeftSchema() + RightSchema();
  const auto make = [&] {
    return std::make_shared<LateMaterializationJoin>(
        context_->txn_, LeftScan(), std::vector<slot_t>{0}, *right_table_,
        right_table_->GetIndex(0), std::vector<slot_t>{0}, output, nullptr,
        false, std::vector<slot_t>{0, 1, 2, 3, 4, 5});
  };
  auto row_join = make();
  const std::vector<Row> row_rows = Drain(row_join.get());
  auto batch_join = make();
  DataChunk chunk(output);
  std::vector<Row> batch_rows;
  while (true) {
    const size_t count = batch_join->NextBatch(&chunk, 2);
    for (size_t i = 0; i < count; ++i) {
      batch_rows.push_back(chunk.RowAt(i));
    }
    if (count < 2) {
      break;
    }
  }
  EXPECT_EQ(batch_join->GetStatus(), Status::kSuccess);
  EXPECT_EQ(batch_rows, row_rows);

  LateMaterializationJoin child_error(
      context_->txn_, std::make_shared<FailingSource>(), {0}, *right_table_,
      right_table_->GetIndex(0), {0}, output);
  Row row;
  RowPosition position;
  EXPECT_FALSE(child_error.Next(&row, &position));
  EXPECT_EQ(child_error.GetStatus(), Status::kRuntimeError);
}

TEST_F(LateMaterializationJoinTest,
       ResidualRunsAfterPayloadAndErrorsAreSticky) {
  const Schema output = LeftSchema() + RightSchema();
  const Expression residual = BinaryExpressionExp(
      ConstantValueExp(Value(105)), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("LMRight", "p1")));
  LateMaterializationJoin filtered(
      context_->txn_, LeftScan(), {0}, *right_table_, right_table_->GetIndex(0),
      {0}, output, residual, false, {0, 1, 2, 3, 4, 5});
  const std::vector<Row> rows = Drain(&filtered);
  EXPECT_EQ(filtered.GetStatus(), Status::kSuccess);
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][3], Value(110));
  EXPECT_EQ(rows[1][3], Value(110));
  EXPECT_EQ(rows[2][3], Value(200));

  const Expression failing = BinaryExpressionExp(
      residual, BinaryOperation::kAnd, std::make_shared<ErrorExpression>());
  LateMaterializationJoin predicate_error(
      context_->txn_, LeftScan(), {0}, *right_table_, right_table_->GetIndex(0),
      {0}, output, failing, false, {0, 1, 2, 3, 4, 5});
  Row row;
  RowPosition position;
  EXPECT_FALSE(predicate_error.Next(&row, &position));
  EXPECT_EQ(predicate_error.GetStatus(), Status::kInvalidArgument);
}

TEST_F(LateMaterializationJoinTest, RowIdIdentityFetchesThePayloadLater) {
  std::vector<Row> identity_rows;
  Iterator iterator = right_table_->BeginFullScan(context_->txn_);
  while (iterator.IsValid()) {
    const RowPosition position = iterator.Position();
    identity_rows.push_back(
        Row({Value(static_cast<int64_t>(position.page_id)),
             Value(static_cast<int64_t>(position.slot)), Value(0)}));
    ++iterator;
  }
  ASSERT_FALSE(identity_rows.empty());
  const Schema left_identity("ids", {Column("page", ValueType::kInt64),
                                     Column("slot", ValueType::kInt64),
                                     Column("unused", ValueType::kInt64)});
  const Schema output = left_identity + RightSchema();
  LateMaterializationJoin join(
      context_->txn_, std::make_shared<ConstantExecutor>(identity_rows),
      *right_table_, output,
      LateMaterializationRowIdBinding{.page_column = 0, .slot_column = 1},
      {0, 1, 2, 3, 4, 5});
  std::vector<RowPosition> positions;
  const std::vector<Row> rows = Drain(&join, &positions);
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  ASSERT_EQ(positions.size(), rows.size());
  for (const RowPosition& position : positions) {
    EXPECT_FALSE(position.IsValid());
  }
  ASSERT_EQ(rows.size(), identity_rows.size());
  EXPECT_EQ(join.Identity(), LateMaterializationIdentity::kRowId);
  EXPECT_EQ(join.PayloadFetchCount(), identity_rows.size());
  EXPECT_EQ(rows.front()[4], Value(100));

  Plan identity_plan =
      std::make_shared<ValuesPlan>(left_identity, identity_rows);
  Plan late_plan = std::make_shared<LateMaterializationJoinPlan>(
      identity_plan, *right_table_, output,
      std::vector<slot_t>{0, 1, 2, 3, 4, 5},
      LateMaterializationRowIdBinding{.page_column = 0, .slot_column = 1});
  Executor factory_executor = late_plan->EmitExecutor(*context_);
  EXPECT_NE(
      std::dynamic_pointer_cast<LateMaterializationJoin>(factory_executor),
      nullptr);
  EXPECT_EQ(Drain(factory_executor.get()).size(), identity_rows.size());
  EXPECT_EQ(factory_executor->GetStatus(), Status::kSuccess);
}

TEST_F(LateMaterializationJoinTest, StaleIndexFallsBackToPredicateJoin) {
  RowPosition deleted_position;
  Iterator iterator = right_table_->BeginFullScan(context_->txn_);
  while (iterator.IsValid()) {
    if (!(*iterator)[0].IsNull() && (*iterator)[0].value.int_value == 2) {
      deleted_position = iterator.Position();
      break;
    }
    ++iterator;
  }
  ASSERT_TRUE(deleted_position.IsValid());
  ASSERT_SUCCESS(right_table_->Delete(context_->txn_, deleted_position));

  Plan left = std::make_shared<FullScanPlan>(*left_table_, *left_statistics_);
  const Schema right_subset("", {right_table_->GetSchema().GetColumn(0),
                                 right_table_->GetSchema().GetColumn(1)});
  const Schema output = left->GetSchema() + right_subset;
  Plan plan = std::make_shared<LateMaterializationJoinPlan>(
      left, std::vector<ColumnName>{ColumnName("LMLeft", "k")}, *right_table_,
      right_table_->GetIndex(0),
      std::vector<ColumnName>{ColumnName("LMRight", "k")}, *right_statistics_,
      BinaryExpressionExp(ColumnValueExp(ColumnName("LMLeft", "k")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("LMRight", "k"))),
      output, std::vector<slot_t>{0, 1}, false, 4U);
  Executor executor = plan->EmitExecutor(*context_);
  EXPECT_EQ(std::dynamic_pointer_cast<LateMaterializationJoin>(executor),
            nullptr);
  std::ostringstream dump;
  executor->Dump(dump, 0);
  EXPECT_NE(dump.str().find("NestedLoopJoin"), std::string::npos) << dump.str();
  const std::vector<Row> rows = Drain(executor.get());
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows.front(), (Row({Value(1), Value(10), Value(1), Value(100)})));
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

TEST_F(LateMaterializationJoinTest, FactoryEmitsLateMaterializationJoin) {
  Plan left = std::make_shared<FullScanPlan>(*left_table_, *left_statistics_);
  const Schema output = left->GetSchema() + RightSchema();
  Plan plan = std::make_shared<LateMaterializationJoinPlan>(
      left, std::vector<ColumnName>{ColumnName("LMLeft", "k")}, *right_table_,
      right_table_->GetIndex(0),
      std::vector<ColumnName>{ColumnName("LMRight", "k")}, *right_statistics_,
      nullptr, output, std::vector<slot_t>{0, 1, 2, 3, 4, 5}, false, 5U);
  Executor executor = plan->EmitExecutor(*context_);
  EXPECT_NE(std::dynamic_pointer_cast<LateMaterializationJoin>(executor),
            nullptr);
  EXPECT_EQ(Drain(executor.get()).size(), 5U);
  EXPECT_EQ(executor->GetStatus(), Status::kSuccess);
}

}  // namespace
}  // namespace tinylamb
