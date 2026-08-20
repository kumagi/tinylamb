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

// ===== RelationalExecutor (complex SELECT plans) coverage =====
#include "query/sql_engine.hpp"

namespace tinylamb {
namespace {

// Executes SQL through the SqlEngine (the same driver the server uses) in a
// fresh committed context and returns every produced row. Queries that throw
// must be tested separately (see the *Throws tests below).
std::vector<Row> RelationalRun(Database& database, std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    context.Abort();
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) rows.push_back(row);
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return rows;
}

// Runs `sql` and captures the first std::exception message ("" if none).
std::string RelationalThrow(Database& database, std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    context.Abort();
    return "<prepare: " + engine.LastError() + ">";
  }
  try {
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
  } catch (const std::exception& error) {
    context.Abort();
    return error.what();
  }
  context.Abort();
  return "";
}

// Fails unless `message` contains `substring`.
void ExpectMessageContains(const std::string& message,
                           const std::string& substring) {
  EXPECT_NE(message.find(substring), std::string::npos) << message;
}

}  // namespace

TEST_F(ExecutorTest, RelationalTruthinessOfBareColumns) {
  // name (varchar), score (double) and key (int64) are tested for truthiness
  // directly inside the relational predicate evaluator (relational.cpp Truthy).
  const auto rows = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE (name AND score AND key) OR "
            "key = 5 ORDER BY key;");
  ASSERT_EQ(rows.size(), 3u);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
  EXPECT_EQ(rows[2][0], Value(3));
}

TEST_F(ExecutorTest, RelationalNotEqualsAndLikePredicates) {
  // kNotEquals on numeric operands and LIKE with a trailing '%'.
  const auto not_equals = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key != 2 OR key = 5 ORDER BY "
            "key;");
  ASSERT_EQ(not_equals.size(), 3u);
  EXPECT_EQ(not_equals[0][0], Value(0));
  EXPECT_EQ(not_equals[1][0], Value(1));
  EXPECT_EQ(not_equals[2][0], Value(3));

  const auto like = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE name LIKE 'hello%' OR key = 3 "
            "ORDER BY key;");
  ASSERT_EQ(like.size(), 2u);
  EXPECT_EQ(like[0][0], Value(0));
  EXPECT_EQ(like[1][0], Value(3));
}

TEST_F(ExecutorTest, RelationalStringConcatenationWithPlus) {
  // '+' on two VARCHAR operands concatenates in the relational evaluator.
  const auto rows = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key = 1 OR name + name = "
            "'hellohello' ORDER BY key;");
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0][0], Value(0));
  EXPECT_EQ(rows[1][0], Value(1));
}

TEST_F(ExecutorTest, RelationalUnaryOperators) {
  const auto minus = RelationalRun(
      *rs_, "SELECT -key, -score FROM SampleTable WHERE key = 1 OR key = 2;");
  ASSERT_EQ(minus.size(), 2u);
  EXPECT_EQ(minus[0][0], Value(-1));
  EXPECT_EQ(minus[0][1], Value(-4.9));
  EXPECT_EQ(minus[1][0], Value(-2));
  EXPECT_EQ(minus[1][1], Value(-4.14));

  const auto is_null = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key IS NULL OR key IS NOT "
            "NULL;");
  EXPECT_EQ(is_null.size(), 4u);

  const auto not_expr = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE NOT (key = 1) OR key = 2 "
            "ORDER BY key;");
  ASSERT_EQ(not_expr.size(), 3u);
  EXPECT_EQ(not_expr[0][0], Value(0));
  EXPECT_EQ(not_expr[1][0], Value(2));
  EXPECT_EQ(not_expr[2][0], Value(3));
}

TEST_F(ExecutorTest, RelationalAggregateInUnaryAndCase) {
  const auto negated = RelationalRun(*rs_, "SELECT -COUNT(*) FROM SampleTable;");
  ASSERT_EQ(negated.size(), 1u);
  EXPECT_EQ(negated[0][0], Value(-4));

  const auto conditional = RelationalRun(
      *rs_,
      "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM SampleTable;");
  ASSERT_EQ(conditional.size(), 1u);
  EXPECT_EQ(conditional[0][0], Value(1));

  // Aggregate in the CASE else-clause (when-clauses carry no aggregate).
  const auto aggregate_else = RelationalRun(
      *rs_, "SELECT CASE WHEN key > 0 THEN 1 ELSE COUNT(*) END FROM "
            "SampleTable;");
  ASSERT_EQ(aggregate_else.size(), 1u);
  EXPECT_EQ(aggregate_else[0][0], Value(4));
}

TEST_F(ExecutorTest, RelationalScalarFunctionsInGroupedQuery) {
  const auto coalesce = RelationalRun(
      *rs_, "SELECT COALESCE(NULL, key) AS c FROM SampleTable GROUP BY key;");
  ASSERT_EQ(coalesce.size(), 4u);

  const auto coalesce_all_null = RelationalRun(
      *rs_, "SELECT COALESCE(NULL, NULL) AS c FROM SampleTable GROUP BY key;");
  ASSERT_EQ(coalesce_all_null.size(), 4u);
  EXPECT_TRUE(coalesce_all_null[0][0].IsNull());

  const auto concat = RelationalRun(
      *rs_, "SELECT CONCAT('a', NULL, 'b') FROM SampleTable GROUP BY key;");
  ASSERT_EQ(concat.size(), 4u);
  EXPECT_EQ(concat[0][0], Value("ab"));

  const auto now = RelationalRun(
      *rs_, "SELECT CURRENT_TIMESTAMP() FROM SampleTable GROUP BY key;");
  ASSERT_EQ(now.size(), 4u);
  EXPECT_EQ(now[0][0].type, ValueType::kVarChar);
  EXPECT_FALSE(now[0][0].IsNull());
}

TEST_F(ExecutorTest, RelationalDateFunctions) {
  const auto add = RelationalRun(*rs_, "SELECT DATE_ADD('2026-01-01', INTERVAL 3 DAY);");
  ASSERT_EQ(add.size(), 1u);
  EXPECT_EQ(add[0][0], Value("2026-01-04"));

  const auto extract = RelationalRun(
      *rs_, "SELECT EXTRACT(YEAR FROM '2026-08-21'), EXTRACT(MONTH FROM "
            "'2026-08-21'), EXTRACT(DAY FROM '2026-08-21');");
  ASSERT_EQ(extract.size(), 1u);
  EXPECT_EQ(extract[0][0], Value(2026));
  EXPECT_EQ(extract[0][1], Value(8));
  EXPECT_EQ(extract[0][2], Value(21));
}

TEST_F(ExecutorTest, RelationalCorrelatedExistsBuildsIndex) {
  // The correlated equality `o.key = key` drives the per-subquery correlated
  // index (local key on the right-hand side of the equality).
  const auto rows = RelationalRun(
      *rs_, "SELECT o.key FROM SampleTable AS o WHERE EXISTS (SELECT 1 FROM "
            "SampleTable WHERE o.key = key AND key > 1) ORDER BY o.key;");
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0][0], Value(2));
  EXPECT_EQ(rows[1][0], Value(3));
}

TEST_F(ExecutorTest, RelationalCorrelatedExistsCacheHits) {
  // A cross join repeats every outer key 4 times; only the first evaluation
  // per key builds the correlated index, the rest must hit the result cache.
  const auto rows = RelationalRun(
      *rs_, "SELECT o.key FROM SampleTable AS o CROSS JOIN SampleTable AS b "
            "WHERE EXISTS (SELECT COUNT(*) FROM SampleTable WHERE o.key = key "
            "HAVING COUNT(*) > 0) ORDER BY o.key;");
  ASSERT_EQ(rows.size(), 16u);
  for (const Row& row : rows) {
    EXPECT_TRUE(row[0].value.int_value >= 0 && row[0].value.int_value <= 3);
  }
}

TEST_F(ExecutorTest, RelationalInnerAndLeftJoins) {
  // Inner join without an equality key uses the nested-loop fallback.
  const auto inner = RelationalRun(
      *rs_, "SELECT a.key FROM SampleTable AS a JOIN SampleTable AS b ON "
            "a.key > b.key ORDER BY a.key;");
  ASSERT_EQ(inner.size(), 6u);
  // Left join without an equality key emits unmatched rows with NULLs.
  const auto left = RelationalRun(
      *rs_, "SELECT a.key FROM SampleTable AS a LEFT JOIN SampleTable AS b ON "
            "a.key > b.key ORDER BY a.key;");
  ASSERT_EQ(left.size(), 7u);
  int64_t previous = -1;
  for (const Row& row : left) {
    EXPECT_GE(row[0].value.int_value, previous);
    previous = row[0].value.int_value;
  }
}

TEST_F(ExecutorTest, RelationalDistinctDeduplicatesJoinedRows) {
  const auto rows = RelationalRun(
      *rs_, "SELECT DISTINCT a.key FROM SampleTable AS a CROSS JOIN SampleTable "
            "AS b WHERE a.key = 1 OR a.key = 2;");
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
}

TEST_F(ExecutorTest, RelationalSortAndLimitOffset) {
  const auto sorted = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key = 1 OR key = 2 ORDER BY "
            "key = 99;");
  ASSERT_EQ(sorted.size(), 2u);

  const auto limited = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key = 1 OR key = 2 LIMIT 1 "
            "OFFSET 1;");
  ASSERT_EQ(limited.size(), 1u);
  EXPECT_EQ(limited[0][0], Value(2));
}

TEST_F(ExecutorTest, RelationalInSubqueriesAndScalarFallback) {
  const auto uncorrelated = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key IN (SELECT key FROM "
            "SampleTable ORDER BY key);");
  EXPECT_EQ(uncorrelated.size(), 4u);

  const auto derived_star = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key IN (SELECT * FROM (SELECT * "
            "FROM SampleTable));");
  EXPECT_EQ(derived_star.size(), 4u);

  // Scalar subquery that is neither correlated-indexable nor cacheable must
  // fall back to a full ExecuteQuery.
  const auto scalar = RelationalRun(
      *rs_, "SELECT (SELECT * FROM (SELECT * FROM SampleTable)) FROM "
            "SampleTable;");
  ASSERT_EQ(scalar.size(), 4u);
  for (const Row& row : scalar) {
    EXPECT_EQ(row[0], Value(0));
  }

  // A subquery whose JOIN condition references an outer-column qualifier is
  // rejected as uncorrelated by StatementUsesOnlyScopes; the unresolvable join
  // predicate is then dropped, leaving a cross join, so IN tests only match
  // the returned constant.
  const auto cross = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE key IN (SELECT 1 FROM "
            "SampleTable AS s JOIN SampleTable AS s2 ON s2.key = "
            "SampleTable.key);");
  ASSERT_EQ(cross.size(), 1u);
  EXPECT_EQ(cross[0][0], Value(1));
}

TEST_F(ExecutorTest, RelationalScalarSubqueryReturnsNullWhenEmpty) {
  const auto rows = RelationalRun(
      *rs_, "SELECT (SELECT key FROM SampleTable WHERE key = 99) FROM "
            "SampleTable;");
  ASSERT_EQ(rows.size(), 4u);
  EXPECT_TRUE(rows[0][0].IsNull());
}

TEST_F(ExecutorTest, RelationalIntervalAndNoFromQueries) {
  const auto interval = RelationalRun(*rs_, "SELECT INTERVAL 3 DAY;");
  ASSERT_EQ(interval.size(), 1u);
  EXPECT_EQ(interval[0][0], Value("3 day"));

  const auto boolean = RelationalRun(*rs_, "SELECT 1 OR 1;");
  ASSERT_EQ(boolean.size(), 1u);
  EXPECT_EQ(boolean[0][0], Value(1));
}

TEST_F(ExecutorTest, RelationalDdlAndDmlRoundTrip) {
  RelationalRun(*rs_, "CREATE TABLE RoundTrip (id INT64, label STRING);");
  RelationalRun(*rs_, "INSERT INTO RoundTrip VALUES (1, 'one'), (2, 'two');");
  RelationalRun(*rs_, "UPDATE RoundTrip SET label = 'uno' WHERE id = 1;");
  RelationalRun(*rs_, "DELETE FROM RoundTrip WHERE id = 2;");
  const auto rows = RelationalRun(*rs_, "SELECT id, label FROM RoundTrip;");
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[0][1], Value("uno"));
}

TEST_F(ExecutorTest, RelationalExplainPlans) {
  auto explain = [&](std::string_view sql) {
    std::string text;
    for (const Row& row :
         RelationalRun(*rs_, std::string("EXPLAIN ") + std::string(sql))) {
      text += row[0].AsString();
      text += '\n';
    }
    return text;
  };

  const std::string derived = explain("SELECT * FROM (SELECT * FROM SampleTable);");
  EXPECT_NE(derived.find("RelationalExecutor"), std::string::npos);
  EXPECT_NE(derived.find("derived_query"), std::string::npos);

  const std::string aliased = explain("SELECT * FROM SampleTable AS t;");
  EXPECT_NE(aliased.find("AS t"), std::string::npos);

  const std::string inner = explain(
      "SELECT * FROM SampleTable AS a JOIN SampleTable AS b ON a.key = b.key;");
  EXPECT_NE(inner.find("join=inner"), std::string::npos);
  EXPECT_NE(inner.find("on="), std::string::npos);

  const std::string left = explain(
      "SELECT a.key FROM SampleTable AS a LEFT JOIN SampleTable AS b ON "
      "a.key = b.key;");
  EXPECT_NE(left.find("join=left"), std::string::npos);

  const std::string limited = explain(
      "SELECT key FROM SampleTable WHERE key = 1 OR key = 2 LIMIT 3 OFFSET 1;");
  EXPECT_NE(limited.find("Limit(count=3"), std::string::npos);
  EXPECT_NE(limited.find("offset=1"), std::string::npos);
}

TEST_F(ExecutorTest, RelationalAggregateOnVarcharThrows) {
  ExpectMessageContains(
      RelationalThrow(*rs_, "SELECT name, SUM(name) AS s FROM SampleTable "
                            "GROUP BY name;"),
      "numeric value required");
}

TEST_F(ExecutorTest, RelationalAmbiguousColumnThrows) {
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT key FROM SampleTable AS a "
                                              "JOIN SampleTable AS b ON "
                                              "a.key = b.key;"),
                        "ambiguous column key");
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT a.key FROM SampleTable AS "
                                              "a JOIN SampleTable AS b ON "
                                              "a.key = b.key WHERE key > 1;"),
                        "ambiguous column key");
}

TEST_F(ExecutorTest, RelationalUnknownColumnThrows) {
  ExpectMessageContains(
      RelationalThrow(*rs_, "SELECT (SELECT nope) FROM SampleTable;"),
      "column nope not found");
}

TEST_F(ExecutorTest, RelationalLikeNonStringOperandThrows) {
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT key FROM SampleTable WHERE "
                                              "key LIKE '5' OR key = 1;"),
                        "LIKE requires string operands");
}

TEST_F(ExecutorTest, RelationalAggregateInWhereThrows) {
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT key FROM SampleTable WHERE "
                                              "COUNT(*) > 0;"),
                        "aggregate outside grouping");
}

TEST_F(ExecutorTest, RelationalMissingTableThrows) {
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT key FROM nonexistent JOIN "
                                              "SampleTable ON TRUE;"),
                        "table nonexistent not found");
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT (SELECT x FROM missing) "
                                              "FROM SampleTable;"),
                        "table missing not found");
}

TEST_F(ExecutorTest, RelationalUnsupportedFunctionThrows) {
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT FOOBAR(1) FROM SampleTable "
                                              "GROUP BY key;"),
                        "unsupported function foobar");
}

TEST_F(ExecutorTest, RelationalDateAddArityThrows) {
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT DATE_ADD('2026-01-01', "
                                              "5);"),
                        "DATE_ADD/DATE_SUB arity");
}

TEST_F(ExecutorTest, RelationalDatePlusDateThrows) {
  RelationalRun(*rs_, "CREATE TABLE RelDate (d DATE, v INT64);");
  RelationalRun(*rs_, "INSERT INTO RelDate VALUES (date '2026-01-01', 1);");
  RelationalRun(*rs_, "INSERT INTO RelDate VALUES (date '2026-06-01', 2);");
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT v FROM RelDate WHERE v = 1 "
                                              "OR d + d = 'x';"),
                        "unsupported binary operation");
}

TEST_F(ExecutorTest, RelationalCorrelatedSubqueryOverNullOuterDocumentsBug) {
  // PRODUCTION BUG: a correlated EXISTS whose outer column is NULL crashes.
  // ExecuteCorrelatedSingleSource encodes the outer values (including NULL)
  // into the cache key *before* the IsNull() guard at relational.cpp:1752, so
  // Row::EncodeMemcomparableFormat() throws "Cannot encode unknown type"
  // instead of returning an empty relation. The null-extension branch of the
  // code is therefore dead and this test documents the observed behavior.
  ExpectMessageContains(RelationalThrow(*rs_, "SELECT o.key FROM SampleTable AS "
                                              "o LEFT JOIN SampleTable AS b ON "
                                              "o.key = b.key AND b.key = 99 "
                                              "WHERE EXISTS (SELECT 1 FROM "
                                              "SampleTable WHERE b.key = key);"),
                        "Cannot encode unknown type");
}

TEST_F(ExecutorTest, AggregationExpressionChildIsEvaluatedPerRow) {
  // Arrange: the aggregate child is an expression (not a plain column), so the
  // executor materializes each row and evaluates it via the expression.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("sum_plus_one",
                      AggregateExpressionExp(
                          AggregationType::kSum,
                          BinaryExpressionExp(ColumnValueExp("value"),
                                              BinaryOperation::kAdd,
                                              ConstantValueExp(Value(1)))))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: (1+1)+(2+1)+(3+1) = 9
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(9));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationMissingColumnThrows) {
  // Arrange: the aggregate references a column that is not in the input schema.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)})};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("sum_missing",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("nope")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: evaluating the missing column throws.
  Row result;
  EXPECT_THROW(aggregate.Next(&result, nullptr), std::runtime_error);
}

TEST_F(ExecutorTest, AggregationAverageOverIntColumn) {
  // Arrange: AVG over an INT64 column must convert each value to double.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: (1+2+3)/3 = 2.0
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(2.0));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationDumpListsAggregates) {
  // Arrange: an aggregation over a two-row constant input.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)})};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count", AggregateExpressionExp(AggregationType::kCount,
                                                      ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act: dump the executor and then consume its single result row.
  std::stringstream ss;
  aggregate.Dump(ss, 0);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));

  // Assert: the dump names the executor and the aggregate; the count is 2.
  EXPECT_NE(ss.str().find("AggregationExecutor"), std::string::npos);
  EXPECT_NE(ss.str().find("count"), std::string::npos);
  EXPECT_EQ(result[0], Value(2));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, SortWithThrowingKeyExpressionRethrows) {
  // Arrange: the sort key references a column missing from the schema, so the
  // comparator throws inside the sort worker and must be rethrown.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(2)}), Row({Value(1)})});
  SortExecutor sort(input, schema, {{ColumnValueExp("nope"), true}});

  // Act + Assert: materializing the sort rethrows the worker exception.
  Row row;
  RowPosition pos;
  EXPECT_THROW(sort.Next(&row, &pos), std::runtime_error);
}

TEST_F(ExecutorTest, AggregationJitEligibleSumWithoutThresholdUsesFallback) {
  // Arrange: a single INT64 SUM aggregate is JIT-eligible, but with a row
  // count below the threshold the JIT is never compiled and every batch (one
  // of which carries a NULL) must fall back to the row-wise accumulator.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value(2)}), Row({Value()}),
                        Row({Value(4)})};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("sum",
                      AggregateExpressionExp(AggregationType::kSum,
                                             ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: the NULL is skipped and the remaining values sum to 7.
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(7));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationCountStarCountsNullRows) {
  // Arrange: COUNT(*) must count every row, including rows whose value column
  // is NULL.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<Row> rows{Row({Value(1)}), Row({Value()}), Row({Value(3)})};
  std::vector<NamedExpression> aggregates = {
      NamedExpression("count_star",
                      AggregateExpressionExp(AggregationType::kCount,
                                             ColumnValueExp("*")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates));

  // Act + Assert: all three rows are counted.
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(3));
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

TEST_F(ExecutorTest, AggregationAverageOverEmptyInputIsNull) {
  // Arrange: AVG over an empty input has no values to average.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("avg", AggregateExpressionExp(AggregationType::kAvg,
                                                    ColumnValueExp("value")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::vector<Row>{}), schema,
      std::move(aggregates));

  // Act + Assert: the result is NULL rather than zero.
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_TRUE(result[0].IsNull());
  EXPECT_FALSE(aggregate.Next(&result, nullptr));
}

}  // namespace tinylamb
