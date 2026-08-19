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

#include <initializer_list>
#include <iostream>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/constant_executor.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/index_join.hpp"
#include "executor/index_scan.hpp"
#include "executor/insert.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "executor/sort.hpp"
#include "executor/update.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "index_only_scan.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
static const char* const kTableName = "SampleTable";

class SyntheticBatchExecutor final : public ExecutorBase {
 public:
  explicit SyntheticBatchExecutor(size_t row_count) : row_count_(row_count) {}

  bool Next(Row* destination, RowPosition* position) override {
    if (next_row_ == row_count_) return false;
    *destination = Row({Value(static_cast<int64_t>(next_row_ % 100))});
    if (position) *position = RowPosition(1, next_row_);
    ++next_row_;
    return true;
  }

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
    destination->Reset(schema, max_rows);
    const size_t batch_rows = std::min<size_t>(max_rows, 64);
    while (next_row_ < row_count_ && destination->Size() < batch_rows) {
      destination->Append(
          Row({Value(static_cast<int64_t>(next_row_ % 100))}),
          RowPosition(1, next_row_));
      ++next_row_;
    }
    return destination->Size();
  }

  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "SyntheticBatchExecutor";
  }

 private:
  size_t row_count_;
  size_t next_row_{0};
};

class ExecutorTest : public ::testing::Test {
 public:
  static void BulkInsert(Transaction& txn, Table& tbl,
                         std::initializer_list<Row> rows) {
    for (const auto& row : rows) {
      ASSERT_SUCCESS(tbl.Insert(txn, row).GetStatus());
    }
  }

  void SetUp() override {
    prefix_ = "executor_test-" + RandomString();
    Recover();
    Schema schema{
        kTableName,
        {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar),
         Column("score", ValueType::kDouble)}};
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(ctx, schema));
    BulkInsert(ctx.txn_, tbl,
               {{Row({Value(0), Value("hello"), Value(1.2)})},
                {Row({Value(3), Value("piyo"), Value(12.2)})},
                {Row({Value(1), Value("world"), Value(4.9)})},
                {Row({Value(2), Value("arise"), Value(4.14)})}});
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, kTableName, IndexSchema("Idx1", {1, 2})));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, kTableName,
        IndexSchema("Idx2", {1}, {1, 2}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
    }
    rs_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

TEST_F(ExecutorTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest death on crash, gtest green on pass
}

TEST_F(ExecutorTest, FullScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  FullScan fs(ctx.txn_, *tbl);
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                            Row({Value(3), Value("piyo"), Value(12.2)}),
                            Row({Value(1), Value("world"), Value(4.9)}),
                            Row({Value(2), Value("arise"), Value(4.14)})});
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- iterate FullScan cursor through all rows
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_NE(rows.find(got), rows.end());
  rows.erase(got);

  // Assert -- cursor exhausted and all rows consumed
  ASSERT_TRUE(rows.empty());
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value("he"), Value("q"), true,
                BinaryExpressionExp(ColumnValueExp("score"),
                                    BinaryOperation::kGreaterThan,
                                    ConstantValueExp(Value(10.0))),
                sc);
  Row target({Value(3), Value("piyo"), Value(12.2)});
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- advance IndexScan cursor to first match
  ASSERT_TRUE(fs.Next(&got, &pos));
  LOG(INFO) << got;

  // Assert -- matched row equals target and cursor exhausts after first match
  ASSERT_EQ(got, target);
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexOnlyScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexOnlyScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value("he"), Value("q"),
                    true,
                    BinaryExpressionExp(ColumnValueExp("score"),
                                        BinaryOperation::kGreaterThan,
                                        ConstantValueExp(Value(10.0))),
                    sc);
  Row expected({Value("piyo"), Value(12.2)});
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- advance IndexOnlyScan cursor to first match
  ASSERT_TRUE(fs.Next(&got, &pos));
  LOG(INFO) << got;

  // Assert -- matched projected row equals expected and cursor exhausts
  ASSERT_EQ(got, expected);
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexOnlyFullScan) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexOnlyScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value(), Value(), true,
                    BinaryExpressionExp(ColumnValueExp("score"),
                                        BinaryOperation::kGreaterThan,
                                        ConstantValueExp(Value(1.0))),
                    sc);
  DumpLog(fs);
  Row got;
  RowPosition pos;

  // Act -- iterate IndexOnlyScan cursor through all projected rows
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("arise"), Value(4.14)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("hello"), Value(1.20)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("piyo"), Value(12.2)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("world"), Value(4.9)}));

  // Assert -- cursor exhausted after all projected rows consumed
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, Projection) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  Projection proj({NamedExpression("key"), NamedExpression("score")},
                   tbl->GetSchema(), std::move(fs));
  std::unordered_set rows(
      {Row({Value(0), Value(1.2)}), Row({Value(3), Value(12.2)}),
        Row({Value(1), Value(4.9)}), Row({Value(2), Value(4.14)})});
  DumpLog(proj);
  Row got;

  // Act -- iterate Projection cursor through all projected rows
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));
  ASSERT_TRUE(proj.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_TRUE(rows.erase(got));

  // Assert -- cursor exhausted and all projected rows consumed
  ASSERT_FALSE(proj.Next(&got, nullptr));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Selection) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Expression key_is_1 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                           ConstantValueExp(Value(1)));
  Selection sel(key_is_1, tbl->GetSchema(),
                std::make_shared<FullScan>(ctx.txn_, *tbl));
  std::unordered_set rows({Row({Value(1), Value("world"), Value(4.9)})});
  DumpLog(sel);
  Row got;

  // Act -- advance Selection cursor to first matching row
  ASSERT_TRUE(sel.Next(&got, nullptr));

  // Assert -- matched row is in expected set and cursor exhausts after one match
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(sel.Next(&got, nullptr));
}

TEST_F(ExecutorTest, SelectionSkipsImpossibleZoneMapBatch) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  Selection selection(
      BinaryExpressionExp(ColumnValueExp("key"),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1000))),
      table->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *table));
  DataChunk output;
  EXPECT_EQ(selection.NextBatch(&output), 0U);
  EXPECT_EQ(selection.SkippedBatches(), 1U);
}

TEST_F(ExecutorTest, SelectionUsesJitForLargeIntegerBatches) {
  std::vector<Row> rows;
  rows.reserve(2048);
  for (int64_t value = 0; value < 2048; ++value) {
    rows.emplace_back(std::vector<Value>{Value(value)});
  }
  const Schema schema("jit", {Column("value", ValueType::kInt64)});
  Selection selection(
      BinaryExpressionExp(ColumnValueExp("value"),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1000))),
      schema, std::make_shared<ConstantExecutor>(std::move(rows)), 1024);
  DataChunk output;
  size_t selected = 0;
  while (selection.NextBatch(&output) != 0) selected += output.Size();
  EXPECT_EQ(selected, 1047U);
  EXPECT_GE(selection.JitBatches(), 1U);
}

TEST_F(ExecutorTest, ProjectionUsesJitForLargeAffineIntegerBatches) {
  std::vector<Row> rows;
  rows.reserve(2048);
  for (int64_t value = 0; value < 2048; ++value) {
    rows.emplace_back(std::vector<Value>{Value(value)});
  }
  const Schema schema("jit", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> expressions = {NamedExpression(
      "affine",
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("value"),
                              BinaryOperation::kMultiply,
                              ConstantValueExp(Value(3))),
          BinaryOperation::kAdd, ConstantValueExp(Value(7))))};
  Projection projection(std::move(expressions), schema,
                        std::make_shared<ConstantExecutor>(std::move(rows)),
                        1024);
  DataChunk output;
  size_t offset = 0;
  while (projection.NextBatch(&output) != 0) {
    for (size_t row = 0; row < output.Size(); ++row) {
      EXPECT_EQ(output.ColumnAt(0).ValueAt(row),
                Value(static_cast<int64_t>((offset + row) * 3 + 7)));
    }
    offset += output.Size();
  }
  EXPECT_EQ(offset, 2048U);
  EXPECT_GE(projection.JitBatches(), 1U);
}

TEST_F(ExecutorTest, AggregationUsesJitForLargeIntegerSumBatches) {
  std::vector<Row> rows;
  rows.reserve(2048);
  int64_t expected = 0;
  for (int64_t value = 0; value < 2048; ++value) {
    rows.emplace_back(std::vector<Value>{Value(value)});
    expected += value;
  }
  const Schema schema("jit", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {NamedExpression(
      "sum", AggregateExpressionExp(AggregationType::kSum,
                                     ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates), 1024);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(expected));
  EXPECT_GE(aggregate.JitBatches(), 1U);
}

TEST_F(ExecutorTest, BasicJoin) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key2", ValueType::kInt64),
                                    Column("score2", ValueType::kDouble),
                                    Column("name2", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(9), Value(1.2), Value("troop")})},
              {Row({Value(7), Value(3.9), Value("arise")})},
              {Row({Value(1), Value(4.9), Value("probe")})},
              {Row({Value(3), Value(12.4), Value("ought")})},
              {Row({Value(3), Value(99.9), Value("extra")})},
              {Row({Value(232), Value(40.9), Value("out")})},
              {Row({Value(0), Value(9.2), Value("arise")})}});
  HashJoin hj(std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
              std::make_shared<FullScan>(ctx.txn_, right_tbl), {0});
  DumpLog(hj);
  std::unordered_set expected({Row({Value(0), Value("hello"), Value(1.2),
                                    Value(0), Value(9.2), Value("arise")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(12.4), Value("ought")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(99.9), Value("extra")}),
                               Row({Value(1), Value("world"), Value(4.9),
                                    Value(1), Value(4.9), Value("probe")})});
  Row got;

  // Act -- iterate HashJoin cursor through all matched pairs
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(hj.Next(&got, nullptr));
  ASSERT_NE(expected.find(got), expected.end());
  ASSERT_TRUE(expected.erase(got));

  // Assert -- cursor exhausted and all matched pairs consumed
  ASSERT_FALSE(hj.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, IndexJoin) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key", ValueType::kInt64),
                                    Column("score", ValueType::kDouble),
                                    Column("name", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(1), Value(4.9), Value("right one")})},
              {Row({Value(3), Value(12.4), Value("right three")})},
              {Row({Value(3), Value(99.9), Value("right duplicated three")})},
              {Row({Value(2), Value(99.9), Value("right two")})},
              {Row({Value(232), Value(40.9), Value("right ignored")})},
              {Row({Value(0), Value(9.2), Value("right zero")})}});
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, "RightTable",
      IndexSchema("RightIdx", {0}, {}, IndexMode::kNonUnique)));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, reload_right,
                        ctx.GetTable("RightTable"));
  ASSERT_EQ(reload_right->IndexCount(), 1);
  IndexJoin ij(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0},
                *reload_right, reload_right->GetIndex(0), {0});
  LOG(INFO) << ij;
  std::unordered_set expected(
      {Row({Value(0), Value("hello"), Value(1.2), Value(0), Value(9.2),
            Value("right zero")}),
       Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value(12.4),
            Value("right three")}),
       Row({Value(3), Value("piyo"), Value(12.2), Value(3), Value(99.9),
            Value("right duplicated three")}),
       Row({Value(1), Value("world"), Value(4.9), Value(1), Value(4.9),
            Value("right one")}),
       Row({Value(2), Value("arise"), Value(4.14), Value(2), Value(99.9),
            Value("right two")})});
  Row got;

  // Act -- iterate IndexJoin cursor through all matched pairs
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));

  // Assert -- cursor exhausted and all matched pairs consumed
  ASSERT_FALSE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, IndexJoinWithCompositeKey) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key", ValueType::kInt64),
                                    Column("score", ValueType::kDouble),
                                    Column("name", ValueType::kVarChar)}}));
  BulkInsert(ctx.txn_, right_tbl,
             {{Row({Value(1), Value(4.9), Value("right one")})},
              {Row({Value(3), Value(12.4), Value("right three")})},
              {Row({Value(3), Value(99.9), Value("piyo")})},
              {Row({Value(2), Value(12.3), Value("arise")})},
              {Row({Value(232), Value(40.9), Value("right ignored")})},
              {Row({Value(0), Value(9.2), Value("hello")})},
              {Row({Value(0), Value(0.1), Value("build")})}});
  ASSERT_SUCCESS(rs_->CreateIndex(
      ctx, "RightTable",
      IndexSchema("RightIdx", {0}, {}, IndexMode::kNonUnique)));
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, reload_right,
                        ctx.GetTable("RightTable"));
  ASSERT_EQ(reload_right->IndexCount(), 1);
  IndexJoin ij(ctx.txn_, std::make_shared<FullScan>(ctx.txn_, *tbl), {0, 1},
               *reload_right, reload_right->GetIndex(0), {0, 2});
  LOG(INFO) << ij;
  std::unordered_set expected({Row({Value(0), Value("hello"), Value(1.2),
                                    Value(0), Value(9.2), Value("hello")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(99.9), Value("piyo")}),
                               Row({Value(2), Value("arise"), Value(4.14),
                                    Value(2), Value(12.3), Value("arise")})});
  Row got;

  // Act -- iterate IndexJoin cursor through all composite-key matched pairs
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));

  // Assert -- cursor exhausted and all matched pairs consumed
  ASSERT_FALSE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, Insert) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Schema src_schema{
      "SrcTable",
      {Column("key2", ValueType::kInt64), Column("name2", ValueType::kVarChar),
       Column("score2", ValueType::kDouble)}};
  rs_->CreateTable(ctx, src_schema);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_tbl,
                        ctx.GetTable("SrcTable"));
  BulkInsert(ctx.txn_, *right_tbl,
             {{Row({Value(9), Value("troop"), Value(1.2)})},
              {Row({Value(7), Value("arise"), Value(3.9)})},
              {Row({Value(1), Value("probe"), Value(4.9)})},
              {Row({Value(3), Value("ought"), Value(12.4)})},
              {Row({Value(3), Value("extra"), Value(99.9)})},
              {Row({Value(232), Value("out"), Value(40.9)})},
              {Row({Value(0), Value("arise"), Value(9.2)})}});
  auto insert = std::make_shared<Insert>(
      ctx.txn_, &*tbl, std::make_shared<FullScan>(ctx.txn_, *right_tbl));
  LOG(INFO) << *insert;
  Row result;

  // Act 1 -- iterate Insert executor to produce rows from SrcTable into SampleTable
  ASSERT_TRUE(insert->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(7));
  ASSERT_FALSE(insert->Next(&result, nullptr));

  // Assert 1 -- Insert executor produced the expected row and then exhausted
  // (implicit above)

  // Arrange 2 -- prepare verification FullScan over the populated SampleTable
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)}),
                           Row({Value(9), Value("troop"), Value(1.2)}),
                           Row({Value(7), Value("arise"), Value(3.9)}),
                           Row({Value(1), Value("probe"), Value(4.9)}),
                           Row({Value(3), Value("ought"), Value(12.4)}),
                           Row({Value(3), Value("extra"), Value(99.9)}),
                           Row({Value(232), Value("out"), Value(40.9)}),
                           Row({Value(0), Value("arise"), Value(9.2)})});

  // Act 2 -- iterate FullScan to verify all 12 rows are present in SampleTable
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }

  // Assert 2 -- FullScan consumed all expected rows and cursor exhausted
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Update) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Schema src_schema{
      "SrcTable",
      {Column("key2", ValueType::kInt64), Column("name2", ValueType::kVarChar),
       Column("score2", ValueType::kDouble)}};
  rs_->CreateTable(ctx, src_schema);
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, right_tbl,
                        ctx.GetTable("SrcTable"));

  std::vector<NamedExpression> update_rule = {
      NamedExpression("key", ColumnValueExp("key")),
      NamedExpression("name", ConstantValueExp(Value("****"))),
      NamedExpression("score",
                      BinaryExpressionExp(ColumnValueExp("score"),
                                          BinaryOperation::kMultiply,
                                          ConstantValueExp(Value(2.0))))};
  auto update = std::make_shared<Update>(
      ctx.txn_, &*tbl,
      std::make_shared<Projection>(update_rule, tbl->GetSchema(),
                                   std::make_shared<FullScan>(ctx.txn_, *tbl)));
  LOG(INFO) << *update;
  Row result;

  // Act 1 -- iterate Update executor to apply updates to SampleTable rows
  ASSERT_TRUE(update->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(4));
  ASSERT_FALSE(update->Next(&result, nullptr));

  // Assert 1 -- Update executor produced the expected updated row and exhausted
  // (implicit above)

  // Arrange 2 -- prepare verification FullScan over the updated SampleTable
  std::unordered_set<Row> rows({{Row({Value(0), Value("****"), Value(2.4)})},
                                {Row({Value(3), Value("****"), Value(24.4)})},
                                {Row({Value(1), Value("****"), Value(9.8)})},
                                {Row({Value(2), Value("****"), Value(8.28)})}});

  // Act 2 -- iterate FullScan to verify all 4 updated rows are present
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }

  // Assert 2 -- FullScan consumed all expected updated rows and cursor exhausted
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Aggregation) {
  // Arrange
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("key"))),
      NamedExpression("sum", AggregateExpressionExp(AggregationType::kSum,
                                                    ColumnValueExp("score"))),
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("score"))),
      NamedExpression("min", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("score"))),
      NamedExpression("max", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("score")))};
  AggregationExecutor agg(std::move(fs), tbl->GetSchema(),
                          std::move(aggregates));
  Row result;

  // Act -- advance Aggregation cursor to compute aggregates over all rows
  ASSERT_TRUE(agg.Next(&result, nullptr));

  // Assert -- aggregated values match expected statistics
  ASSERT_EQ(result[0], Value(4));
  ASSERT_EQ(result[1], Value(22.44));
  ASSERT_EQ(result[2], Value(5.61));
  ASSERT_EQ(result[3], Value(1.2));
  ASSERT_EQ(result[4], Value(12.2));
  ASSERT_FALSE(agg.Next(&result, nullptr));
}

TEST_F(ExecutorTest, VectorizedScanFilterProjectAggregatePipeline) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        ctx.GetTable(kTableName));
  const Schema input_schema = table->GetSchema();
  auto scan = std::make_shared<FullScan>(ctx.txn_, *table);
  auto filter = std::make_shared<Selection>(
      BinaryExpressionExp(ColumnValueExp("score"),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(4.0))),
      input_schema, scan);
  std::vector<NamedExpression> projections = {
      NamedExpression(
          "doubled",
          BinaryExpressionExp(ColumnValueExp("score"),
                              BinaryOperation::kMultiply,
                              ConstantValueExp(Value(2.0))))};
  auto project = std::make_shared<Projection>(projections, input_schema,
                                              std::move(filter));
  const Schema projected_schema(
      "projected", {Column("doubled", ValueType::kDouble)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("doubled"))),
      NamedExpression("sum",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("doubled")))};
  AggregationExecutor aggregate(std::move(project), projected_schema,
                                std::move(aggregates));

  DataChunk output;
  ASSERT_EQ(aggregate.NextBatch(&output, 8), 1);
  ASSERT_EQ(output.Size(), 1);
  const Row result = output.RowAt(0);
  EXPECT_EQ(result[0], Value(3));
  EXPECT_EQ(result[1], Value(42.48));
  EXPECT_EQ(aggregate.NextBatch(&output, 8), 0);
}

TEST_F(ExecutorTest, MorselDrivenParallelScanReadsEveryPageOnce) {
  constexpr char kParallelTable[] = "ParallelScanTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kParallelTable,
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      std::string payload = "row-" + std::to_string(key);
      payload.resize(100, 'x');
      ASSERT_SUCCESS(table.Insert(
          writer.txn_, Row({Value(key), Value(std::move(payload))})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }

  TransactionContext reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kParallelTable));
  ParallelScan scan(reader.txn_, *table, 4, 1);
  ASSERT_GT(scan.MorselCount(), 4U);
  EXPECT_EQ(scan.WorkerCount(), 4U);

  std::unordered_set<int64_t> keys;
  DataChunk chunk;
  while (scan.NextBatch(&chunk, 37) != 0) {
    ASSERT_LE(chunk.Size(), 37U);
    for (size_t row = 0; row < chunk.Size(); ++row) {
      EXPECT_TRUE(keys.insert(chunk.ColumnAt(0).ValueAt(row).value.int_value)
                      .second);
    }
  }
  EXPECT_EQ(keys.size(), static_cast<size_t>(kRows));
  EXPECT_TRUE(keys.contains(0));
  EXPECT_TRUE(keys.contains(kRows - 1));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ThreadLocalAggregationMergesPartialAndDistinctStates) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(10000);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("value"))),
      NamedExpression("sum",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("value"))),
      NamedExpression("avg",
                      AggregateExpressionExp(AggregationType::kAvg,
                                             ColumnValueExp("value"))),
      NamedExpression("min",
                      AggregateExpressionExp(AggregationType::kMin,
                                             ColumnValueExp("value"))),
      NamedExpression("max",
                      AggregateExpressionExp(AggregationType::kMax,
                                             ColumnValueExp("value"))),
      NamedExpression("distinct_count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("value"), true))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        4);

  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(10000));
  EXPECT_EQ(result[1], Value(495000));
  EXPECT_EQ(result[2], Value(49.5));
  EXPECT_EQ(result[3], Value(0));
  EXPECT_EQ(result[4], Value(99));
  EXPECT_EQ(result[5], Value(100));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelSortPreservesOrderAndStableTies) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(10000);
  SortExecutor sort(input, schema, {{ColumnValueExp("value"), true}}, 4);

  int64_t previous_value = -1;
  slot_t previous_position = 0;
  size_t count = 0;
  Row row;
  RowPosition position;
  while (sort.Next(&row, &position)) {
    const int64_t value = row[0].value.int_value;
    EXPECT_GE(value, previous_value);
    if (value == previous_value) {
      EXPECT_GT(position.slot, previous_position);
    }
    previous_value = value;
    previous_position = position.slot;
    ++count;
  }
  EXPECT_EQ(count, 10000U);
  EXPECT_EQ(previous_value, 99);
}
}  // namespace tinylamb
