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
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/index_join.hpp"
#include "executor/index_scan.hpp"
#include "executor/insert.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "executor/update.hpp"
#include "executor/aggregation.hpp"
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

TEST_F(ExecutorTest, Construct) {}

TEST_F(ExecutorTest, FullScan) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  FullScan fs(ctx.txn_, *tbl);
  std::unordered_set rows({Row({Value(0), Value("hello"), Value(1.2)}),
                           Row({Value(3), Value("piyo"), Value(12.2)}),
                           Row({Value(1), Value("world"), Value(4.9)}),
                           Row({Value(2), Value("arise"), Value(4.14)})});
  fs.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  RowPosition pos;
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
  ASSERT_TRUE(rows.empty());
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexScan) {
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

  fs.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  RowPosition pos;
  ASSERT_TRUE(fs.Next(&got, &pos));
  std::cout << got << "\n";
  ASSERT_EQ(got, target);
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexOnlyScan) {
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

  fs.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  RowPosition pos;
  ASSERT_TRUE(fs.Next(&got, &pos));
  std::cout << got << "\n";
  ASSERT_EQ(got, expected);
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, IndexOnlyFullScan) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSERT_EQ(tbl->IndexCount(), 2);
  const Schema sc = tbl->GetSchema();
  IndexOnlyScan fs(ctx.txn_, *tbl, tbl->GetIndex(0), Value(), Value(), true,
                   BinaryExpressionExp(ColumnValueExp("score"),
                                       BinaryOperation::kGreaterThan,
                                       ConstantValueExp(Value(1.0))),
                   sc);

  fs.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  RowPosition pos;
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("arise"), Value(4.14)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("hello"), Value(1.20)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("piyo"), Value(12.2)}));
  ASSERT_TRUE(fs.Next(&got, &pos));
  ASSERT_EQ(got, Row({Value("world"), Value(4.9)}));
  ASSERT_FALSE(fs.Next(&got, &pos));
}

TEST_F(ExecutorTest, Projection) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  Projection proj({NamedExpression("key"), NamedExpression("score")},
                  tbl->GetSchema(), std::move(fs));
  std::unordered_set rows(
      {Row({Value(0), Value(1.2)}), Row({Value(3), Value(12.2)}),
       Row({Value(1), Value(4.9)}), Row({Value(2), Value(4.14)})});
  proj.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
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
  ASSERT_FALSE(proj.Next(&got, nullptr));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Selection) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  Expression key_is_1 =
      BinaryExpressionExp(ColumnValueExp("key"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)));
  Selection sel(key_is_1, tbl->GetSchema(),
                std::make_shared<FullScan>(ctx.txn_, *tbl));
  std::unordered_set rows({Row({Value(1), Value("world"), Value(4.9)})});
  sel.Dump(std::cout, 0);
  std::cout << "\n";
  Row got;
  ASSERT_TRUE(sel.Next(&got, nullptr));
  ASSERT_NE(rows.find(got), rows.end());
  ASSERT_FALSE(sel.Next(&got, nullptr));
}

TEST_F(ExecutorTest, BasicJoin) {
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
  hj.Dump(std::cout, 0);
  std::cout << "\n";
  std::unordered_set expected({Row({Value(0), Value("hello"), Value(1.2),
                                    Value(0), Value(9.2), Value("arise")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(12.4), Value("ought")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(99.9), Value("extra")}),
                               Row({Value(1), Value("world"), Value(4.9),
                                    Value(1), Value(4.9), Value("probe")})});

  Row got;
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
  ASSERT_FALSE(hj.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, IndexJoin) {
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
  std::cout << ij << "\n";

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
  ASSERT_FALSE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, IndexJoinWithCompositeKey) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTable",
                                   {Column("key", ValueType::kInt64),
                                    Column("score", ValueType::kDouble),
                                    Column("name", ValueType::kVarChar)}}));
  /*
               {{Row({Value(0), Value("hello"), Value(1.2)})},
                {Row({Value(3), Value("piyo"), Value(12.2)})},
                {Row({Value(1), Value("world"), Value(4.9)})},
                {Row({Value(2), Value("arise"), Value(4.14)})}});
                */
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
  std::cout << ij << "\n";

  std::unordered_set expected({Row({Value(0), Value("hello"), Value(1.2),
                                    Value(0), Value(9.2), Value("hello")}),
                               Row({Value(3), Value("piyo"), Value(12.2),
                                    Value(3), Value(99.9), Value("piyo")}),
                               Row({Value(2), Value("arise"), Value(4.14),
                                    Value(2), Value(12.3), Value("arise")})});
  Row got;
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_TRUE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.contains(got));
  ASSERT_TRUE(expected.erase(got));
  ASSERT_FALSE(ij.Next(&got, nullptr));
  ASSERT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, Insert) {
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
  std::cout << *insert << "\n";
  Row result;
  ASSERT_TRUE(insert->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(7));
  ASSERT_FALSE(insert->Next(&result, nullptr));

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
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Update) {
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
  std::cout << *update << "\n";
  Row result;
  ASSERT_TRUE(update->Next(&result, nullptr));
  ASSERT_EQ(result[1], Value(4));
  ASSERT_FALSE(update->Next(&result, nullptr));

  std::unordered_set<Row> rows({{Row({Value(0), Value("****"), Value(2.4)})},
                                {Row({Value(3), Value("****"), Value(24.4)})},
                                {Row({Value(1), Value("****"), Value(9.8)})},
                                {Row({Value(2), Value("****"), Value(8.28)})}});
  FullScan fs(ctx.txn_, *tbl);
  while (!rows.empty()) {
    Row got;
    RowPosition pos;
    ASSERT_TRUE(fs.Next(&got, &pos));
    ASSERT_NE(rows.find(got), rows.end());
    rows.erase(got);
  }
  ASSERT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, Aggregation) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, tbl, ctx.GetTable(kTableName));
  auto fs = std::make_shared<FullScan>(ctx.txn_, *tbl);
  std::vector<NamedExpression> aggregates = {
      NamedExpression(
          "count",
          AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("key"))),
      NamedExpression(
          "sum",
          AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("score"))),
      NamedExpression(
          "avg",
          AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("score"))),
      NamedExpression(
          "min",
          AggregateExpressionExp(AggregationType::kMin, ColumnValueExp("score"))),
      NamedExpression(
          "max",
          AggregateExpressionExp(AggregationType::kMax, ColumnValueExp("score")))};
  AggregationExecutor agg(std::move(fs), std::move(aggregates));
  Row result;
  ASSERT_TRUE(agg.Next(&result, nullptr));
  ASSERT_EQ(result[0], Value(4));
  ASSERT_EQ(result[1], Value(22.44));
  ASSERT_EQ(result[2], Value(5.61));
  ASSERT_EQ(result[3], Value(1.2));
  ASSERT_EQ(result[4], Value(12.2));
  ASSERT_FALSE(agg.Next(&result, nullptr));
}
}  // namespace tinylamb
