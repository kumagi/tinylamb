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

#include <algorithm>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
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
#include "executor/cross_join.hpp"
#include "executor/delete.hpp"
#include "executor/distinct.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/index_join.hpp"
#include "executor/index_scan.hpp"
#include "executor/insert.hpp"
#include "executor/limit.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "executor/sort.hpp"
#include "executor/update.hpp"
#include "executor/zone_map.hpp"
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

// ===== DistinctExecutor =====
TEST_F(ExecutorTest, DistinctEmptyInput) {
  DistinctExecutor distinct(
      std::make_shared<ConstantExecutor>(std::vector<Row>{}));
  Row row;
  RowPosition pos;
  ASSERT_FALSE(distinct.Next(&row, &pos));
  ASSERT_FALSE(distinct.Next(&row, nullptr));
}

TEST_F(ExecutorTest, DistinctAllDuplicates) {
  std::vector<Row> rows;
  for (int i = 0; i < 6; ++i) {
    rows.emplace_back(std::vector<Value>{Value(7)});
  }
  DistinctExecutor distinct(std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  ASSERT_TRUE(distinct.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(7)}));
  ASSERT_FALSE(distinct.Next(&row, nullptr));
}

TEST_F(ExecutorTest, DistinctAlreadySortedInput) {
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(2)}),
                        Row({Value(3)}), Row({Value(4)}), Row({Value(4)})};
  DistinctExecutor distinct(std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  int64_t expected = 1;
  while (distinct.Next(&row, &pos)) {
    EXPECT_EQ(row, Row({Value(expected)}));
    ++expected;
  }
  EXPECT_EQ(expected, 5);
}

TEST_F(ExecutorTest, DistinctUnsortedInterleavedDuplicates) {
  std::vector<Row> rows{Row({Value(3)}), Row({Value(1)}), Row({Value(3)}),
                        Row({Value(2)}), Row({Value(1)}), Row({Value(4)})};
  DistinctExecutor distinct(std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  std::unordered_set<Value> seen;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_TRUE(seen.insert(row[0]).second);
    ++count;
  }
  EXPECT_EQ(count, 4U);
  EXPECT_EQ(seen,
            std::unordered_set<Value>({Value(1), Value(2), Value(3), Value(4)}));
}

TEST_F(ExecutorTest, DistinctMultipleColumns) {
  std::vector<Row> rows{
      Row({Value(1), Value("a")}), Row({Value(1), Value("a")}),
      Row({Value(1), Value("b")}), Row({Value(2), Value("a")}),
      Row({Value(2), Value("a")}), Row({Value(2), Value("c")})};
  DistinctExecutor distinct(std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  std::unordered_set<Row> seen;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_TRUE(seen.insert(row).second);
    ++count;
  }
  EXPECT_EQ(count, 4U);
  EXPECT_TRUE(seen.contains(Row({Value(1), Value("b")})));
  EXPECT_TRUE(seen.contains(Row({Value(2), Value("c")})));
}

TEST_F(ExecutorTest, DistinctNullValueThrowsFromHash) {
  // std::hash<Value> throws for kNull, so DistinctExecutor (which deduplicates
  // via std::unordered_set<Row>) cannot handle NULL-containing rows.
  std::vector<Row> rows{Row({Value()}), Row({Value(5)})};
  DistinctExecutor distinct(std::make_shared<ConstantExecutor>(std::move(rows)));
  Row row;
  RowPosition pos;
  EXPECT_THROW(distinct.Next(&row, &pos), std::runtime_error);
}

TEST_F(ExecutorTest, DistinctOverFullScanPreservesRowsAndPositions) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  DistinctExecutor distinct(std::make_shared<FullScan>(ctx.txn_, *tbl));
  LOG(INFO) << distinct;
  std::unordered_set<Row> expected({
      Row({Value(0), Value("hello"), Value(1.2)}),
      Row({Value(3), Value("piyo"), Value(12.2)}),
      Row({Value(1), Value("world"), Value(4.9)}),
      Row({Value(2), Value("arise"), Value(4.14)})});
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (distinct.Next(&row, &pos)) {
    ASSERT_NE(expected.find(row), expected.end());
    ASSERT_TRUE(pos.IsValid());
    ++count;
  }
  EXPECT_EQ(count, 4U);
  std::stringstream ss;
  distinct.Dump(ss, 0);
  EXPECT_NE(ss.str().find("Distinct"), std::string::npos);
}

// ===== CrossJoin =====
TEST_F(ExecutorTest, CrossJoinProducesCartesianProduct) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value("a")}), Row({Value("b")})});
  CrossJoin join(left, right);
  std::unordered_set<Row> expected({
      Row({Value(1), Value("a")}), Row({Value(1), Value("b")}),
      Row({Value(2), Value("a")}), Row({Value(2), Value("b")})});
  Row got;
  RowPosition pos;
  size_t count = 0;
  while (join.Next(&got, &pos)) {
    ASSERT_NE(expected.find(got), expected.end());
    ++count;
  }
  EXPECT_EQ(count, 4U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("CrossJoin"), std::string::npos);
}

// ===== DeleteExecutor =====
TEST_F(ExecutorTest, DeleteRemovesAllRows) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto deleter = std::make_shared<DeleteExecutor>(
      ctx.txn_, *tbl, std::make_shared<FullScan>(ctx.txn_, *tbl));
  LOG(INFO) << *deleter;
  Row result;
  RowPosition rp;
  ASSERT_TRUE(deleter->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Delete Rows"), Value(4)}));
  ASSERT_FALSE(deleter->Next(&result, &rp));
  Row got;
  FullScan fs(ctx.txn_, *tbl);
  ASSERT_FALSE(fs.Next(&got, nullptr));
  std::stringstream ss;
  deleter->Dump(ss, 0);
  EXPECT_NE(ss.str().find("Delete"), std::string::npos);
}

TEST_F(ExecutorTest, DeleteEmptySourceCountsZero) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto deleter = std::make_shared<DeleteExecutor>(
      ctx.txn_, *tbl, std::make_shared<ConstantExecutor>(std::vector<Row>{}));
  Row result;
  ASSERT_TRUE(deleter->Next(&result, nullptr));
  EXPECT_EQ(result, Row({Value("Delete Rows"), Value(0)}));
  ASSERT_FALSE(deleter->Next(&result, nullptr));
}

// ===== ConstantExecutor =====
TEST_F(ExecutorTest, ConstantExecutorNextBatchSplitsRows) {
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  ConstantExecutor ce(std::move(rows));
  DataChunk chunk;
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 2U);
  EXPECT_EQ(chunk.RowAt(0), Row({Value(1)}));
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 1U);
  EXPECT_EQ(ce.NextBatch(&chunk, 2), 0U);
  ConstantExecutor single(Row({Value(9)}));
  Row got;
  ASSERT_TRUE(single.Next(&got, nullptr));
  EXPECT_EQ(got, Row({Value(9)}));
  ASSERT_FALSE(single.Next(&got, nullptr));
  std::stringstream ss;
  ce.Dump(ss, 0);
  EXPECT_NE(ss.str().find("ConstantExecutor"), std::string::npos);
}

// ===== ZoneMap =====
TEST_F(ExecutorTest, ZoneMapTracksStats) {
  ZoneMap map;
  EXPECT_EQ(map.NullCount(), 0U);
  EXPECT_EQ(map.ValueCount(), 0U);
  EXPECT_FALSE(map.Minimum().has_value());
  EXPECT_FALSE(map.Maximum().has_value());
  map.Add(Value(10));
  map.Add(Value(3));
  map.Add(Value(7));
  map.Add(Value());
  EXPECT_EQ(map.NullCount(), 1U);
  EXPECT_EQ(map.ValueCount(), 3U);
  ASSERT_TRUE(map.Minimum().has_value());
  ASSERT_TRUE(map.Maximum().has_value());
  EXPECT_EQ(*map.Minimum(), Value(3));
  EXPECT_EQ(*map.Maximum(), Value(10));
  map.Reset();
  EXPECT_EQ(map.NullCount(), 0U);
  EXPECT_EQ(map.ValueCount(), 0U);
  EXPECT_FALSE(map.Minimum().has_value());
  EXPECT_FALSE(map.Maximum().has_value());
}

TEST_F(ExecutorTest, ZoneMapMayMatchComparisons) {
  ZoneMap map;
  map.Add(Value(5));
  map.Add(Value(10));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kEquals, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kEquals, Value(7)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kEquals, Value(4)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kEquals, Value(11)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kNotEquals, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kNotEquals, Value(4)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kLessThan, Value(11)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kLessThan, Value(5)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kLessThanEquals, Value(5)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kLessThanEquals, Value(4)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kGreaterThan, Value(4)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kGreaterThan, Value(10)));
  EXPECT_TRUE(map.MayMatch(BinaryOperation::kGreaterThanEquals, Value(10)));
  EXPECT_FALSE(map.MayMatch(BinaryOperation::kGreaterThanEquals, Value(11)));
}

TEST_F(ExecutorTest, ZoneMapMayMatchDegenerateCases) {
  ZoneMap single;
  single.Add(Value(7));
  EXPECT_FALSE(single.MayMatch(BinaryOperation::kNotEquals, Value(7)));
  EXPECT_TRUE(single.MayMatch(BinaryOperation::kNotEquals, Value(6)));
  EXPECT_TRUE(single.MayMatch(BinaryOperation::kAdd, Value(1)));
  EXPECT_FALSE(single.MayMatch(BinaryOperation::kEquals, Value()));
  ZoneMap empty;
  EXPECT_FALSE(empty.MayMatch(BinaryOperation::kEquals, Value(1)));
  ZoneMap doubles;
  doubles.Add(Value(1.0));
  EXPECT_FALSE(doubles.MayMatch(BinaryOperation::kEquals, Value(1)));
  EXPECT_FALSE(doubles.MayMatch(BinaryOperation::kLessThan, Value(2)));
}

// ===== Selection edge cases =====
TEST_F(ExecutorTest, SelectionConstantOnLeftIsFlipped) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto collect_keys = [&](const Expression& exp) {
    Selection sel(exp, tbl->GetSchema(),
                  std::make_shared<FullScan>(ctx.txn_, *tbl));
    std::vector<int64_t> keys;
    Row got;
    while (sel.Next(&got, nullptr)) {
      keys.push_back(got[0].value.int_value);
    }
    std::sort(keys.begin(), keys.end());
    return keys;
  };
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(2)), BinaryOperation::kLessThan,
                ColumnValueExp("key"))),
            std::vector<int64_t>({3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(2)), BinaryOperation::kLessThanEquals,
                ColumnValueExp("key"))),
            std::vector<int64_t>({2, 3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(3)), BinaryOperation::kGreaterThan,
                ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(3)), BinaryOperation::kGreaterThanEquals,
                ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2, 3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(3)), BinaryOperation::kEquals,
                ColumnValueExp("key"))),
            std::vector<int64_t>({3}));
  EXPECT_EQ(collect_keys(BinaryExpressionExp(
                ConstantValueExp(Value(3)), BinaryOperation::kNotEquals,
                ColumnValueExp("key"))),
            std::vector<int64_t>({0, 1, 2}));
}

TEST_F(ExecutorTest, SelectionSupportsOrPredicate) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Expression key_is_0 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(0)));
  Expression key_is_3 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(3)));
  Selection sel(BinaryExpressionExp(key_is_0, BinaryOperation::kOr, key_is_3),
                tbl->GetSchema(), std::make_shared<FullScan>(ctx.txn_, *tbl));
  Row got;
  std::unordered_set<Value> keys;
  while (sel.Next(&got, nullptr)) {
    keys.insert(got[0]);
  }
  EXPECT_EQ(keys, std::unordered_set<Value>({Value(0), Value(3)}));
}

// ===== ParallelScan =====
TEST_F(ExecutorTest, ParallelScanEmptyTableReturnsNoRows) {
  constexpr char kTable[] = "EmptyScanTable";
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTable));
  ParallelScan scan(reader.txn_, *table, 2, 1);
  DataChunk chunk;
  EXPECT_EQ(scan.NextBatch(&chunk), 0U);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanNextScalarPath) {
  constexpr char kTable[] = "ParallelScalarTable";
  constexpr int64_t kRows = 500;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(key)})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTable));
  ParallelScan scan(reader.txn_, *table, 2, 1);
  LOG(INFO) << scan;
  std::unordered_set<int64_t> keys;
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (scan.Next(&row, &pos)) {
    ASSERT_TRUE(keys.insert(row[0].value.int_value).second);
    ASSERT_TRUE(pos.IsValid());
    ++count;
  }
  EXPECT_EQ(count, static_cast<size_t>(kRows));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanProjectedColumns) {
  constexpr char kTable[] = "ParallelProjectionTable";
  constexpr int64_t kRows = 800;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64),
                           Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(
          writer.txn_, Row({Value(key), Value("n" + std::to_string(key))}))
          .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTable));
  ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{0, 1});
  DataChunk chunk;
  size_t total = 0;
  std::unordered_set<std::string> names;
  while (scan.NextBatch(&chunk, 64) != 0) {
    ASSERT_EQ(chunk.ColumnCount(), 2U);
    for (size_t row = 0; row < chunk.Size(); ++row) {
      names.insert(chunk.ColumnAt(0).ValueAt(row).AsString());
    }
    total += chunk.Size();
  }
  EXPECT_EQ(total, static_cast<size_t>(kRows));
  EXPECT_EQ(names.size(), static_cast<size_t>(kRows));
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanReorderedProjectionThrows) {
  // Row::DeserializeProjected assumes an ascending projection column list, so
  // a reordered projection like {1, 0} yields a projected row narrower than the
  // chunk layout ParallelScan initializes, and the worker throws.
  constexpr char kTable[] = "ParallelProjectionTable";
  constexpr int64_t kRows = 800;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64),
                           Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(
          writer.txn_, Row({Value(key), Value("n" + std::to_string(key))}))
          .GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTable));
  ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{1, 0});
  DataChunk chunk;
  EXPECT_THROW(scan.NextBatch(&chunk, 64), std::invalid_argument);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

TEST_F(ExecutorTest, ParallelScanSplitsLargeChunksWhenAskedSmallBatches) {
  constexpr char kTable[] = "ParallelSplitTable";
  constexpr int64_t kRows = 2000;
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{kTable, {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t key = 0; key < kRows; ++key) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({Value(key)})).GetStatus());
    }
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  TransactionContext reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTable));
  ParallelScan scan(reader.txn_, *table, 4, 1);
  DataChunk chunk;
  ASSERT_GT(scan.NextBatch(&chunk, 1000), 0U);
  size_t total = chunk.Size();
  size_t batches = 0;
  while (true) {
    const size_t got = scan.NextBatch(&chunk, 1);
    if (got == 0) break;
    EXPECT_EQ(got, 1U);
    total += got;
    ++batches;
  }
  EXPECT_EQ(total, static_cast<size_t>(kRows));
  EXPECT_GT(batches, 10U);
  ASSERT_SUCCESS(reader.txn_.PreCommit());
}

// ===== ParallelAggregationExecutor =====
TEST_F(ExecutorTest, ParallelAggregationCountStarAndExpressionChild) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(100);
  std::vector<NamedExpression> aggregates = {
      NamedExpression(
          "count_star",
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"))),
      NamedExpression(
          "sum_plus_one",
          AggregateExpressionExp(
              AggregationType::kSum,
              BinaryExpressionExp(ColumnValueExp("value"),
                                  BinaryOperation::kAdd,
                                  ConstantValueExp(Value(1)))))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(100));
  EXPECT_EQ(result[1], Value(5050));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, ParallelAggregationAverageOverEmptyInput) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_TRUE(result[0].IsNull());
}

TEST_F(ExecutorTest, ParallelAggregationNextBatchAndDump) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(50);
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("value")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        3);
  LOG(INFO) << aggregate;
  DataChunk chunk;
  EXPECT_EQ(aggregate.NextBatch(&chunk, 8), 1U);
  EXPECT_EQ(chunk.RowAt(0)[0], Value(50));
  EXPECT_EQ(aggregate.NextBatch(&chunk, 8), 0U);
}

TEST_F(ExecutorTest, ParallelAggregationMissingColumnThrows) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(2)})});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("sum_missing",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("missing")))};
  ParallelAggregationExecutor aggregate(input, schema, std::move(aggregates),
                                        2);
  Row result;
  EXPECT_THROW(aggregate.Next(&result, nullptr), std::runtime_error);
}

// ===== Insert / Update =====
TEST_F(ExecutorTest, InsertWithRowPosition) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto insert = std::make_shared<Insert>(
      ctx.txn_, &*tbl,
      std::make_shared<ConstantExecutor>(
          std::vector<Row>{Row({Value(100), Value("new"), Value(1.0)})}));
  Row result;
  RowPosition rp;
  ASSERT_TRUE(insert->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Insert Rows"), Value(1)}));
  ASSERT_FALSE(insert->Next(&result, &rp));
}

TEST_F(ExecutorTest, UpdateWithRowPosition) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  std::vector<NamedExpression> update_rule = {
      NamedExpression("key", ColumnValueExp("key")),
      NamedExpression("name", ConstantValueExp(Value("changed"))),
      NamedExpression("score", ColumnValueExp("score"))};
  auto update = std::make_shared<Update>(
      ctx.txn_, &*tbl,
      std::make_shared<Projection>(update_rule, tbl->GetSchema(),
                                   std::make_shared<FullScan>(ctx.txn_, *tbl)));
  Row result;
  RowPosition rp;
  ASSERT_TRUE(update->Next(&result, &rp));
  EXPECT_EQ(result, Row({Value("Update Rows"), Value(4)}));
  ASSERT_FALSE(update->Next(&result, &rp));
}

// ===== Sort / HashJoin =====
TEST_F(ExecutorTest, SortDumpAndSmallInput) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(2)}), Row({Value(1)}), Row({Value(2)})});
  SortExecutor sort(input, schema, {{ColumnValueExp("value"), true}});
  Row row;
  RowPosition pos;
  ASSERT_TRUE(sort.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(sort.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_TRUE(sort.Next(&row, &pos));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(sort.Next(&row, nullptr));
  std::stringstream ss;
  sort.Dump(ss, 0);
  EXPECT_NE(ss.str().find("ParallelSort"), std::string::npos);
}

TEST_F(ExecutorTest, HashJoinNextBatchAndDump) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1), Value("l")}),
                       Row({Value(2), Value("l2")})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1), Value("r1")}),
                       Row({Value(1), Value("r2")})});
  HashJoin join(left, {0}, right, {0});
  LOG(INFO) << join;
  DataChunk chunk;
  EXPECT_EQ(join.NextBatch(&chunk, 8), 2U);
  EXPECT_EQ(chunk.Size(), 2U);
  EXPECT_EQ(join.NextBatch(&chunk, 8), 0U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("PartitionedHashJoin"), std::string::npos);
}

TEST_F(ExecutorTest, HashJoinWithMultipleJoinColumnsDumpsAllKeys) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1), Value(10), Value("l")})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1), Value(10), Value("r")})});
  HashJoin join(left, {0, 1}, right, {0, 1});
  Row got;
  ASSERT_TRUE(join.Next(&got, nullptr));
  EXPECT_EQ(got, Row({Value(1), Value(10), Value("l"), Value(1), Value(10),
                      Value("r")}));
  ASSERT_FALSE(join.Next(&got, nullptr));
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("left: {0, 1}"), std::string::npos);
  EXPECT_NE(ss.str().find("right: {0, 1}"), std::string::npos);
}

// ===== LimitExecutor =====
TEST_F(ExecutorTest, LimitLessThanInputRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  LimitExecutor limit(input, 2, 0);
  Row row;
  RowPosition rp;
  ASSERT_TRUE(limit.Next(&row, &rp));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(limit.Next(&row, &rp));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitEqualToInputRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  LimitExecutor limit(input, 2, 0);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitGreaterThanInputRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)})});
  LimitExecutor limit(input, 5, 0);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitZeroMeansNoLimit) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  LimitExecutor limit(input, 0, 0);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1)}));
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitOffsetSkipsLeadingRows) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  LimitExecutor limit(input, 1, 2);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(3)}));
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitOffsetBeyondInputEmitsNothing) {
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  LimitExecutor limit(input, 5, 10);
  Row row;
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitOffsetWithEmptyInputEmitsNothing) {
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  LimitExecutor limit(input, 3, 1);
  Row row;
  ASSERT_FALSE(limit.Next(&row, nullptr));
}

TEST_F(ExecutorTest, LimitDump) {
  auto input = std::make_shared<SyntheticBatchExecutor>(5);
  LimitExecutor limit(input, 3, 1);
  Row row;
  ASSERT_TRUE(limit.Next(&row, nullptr));
  std::stringstream ss;
  limit.Dump(ss, 0);
  EXPECT_NE(ss.str().find("Limit: 3 offset 1"), std::string::npos);
  EXPECT_NE(ss.str().find("SyntheticBatchExecutor"), std::string::npos);
}
}  // namespace tinylamb
