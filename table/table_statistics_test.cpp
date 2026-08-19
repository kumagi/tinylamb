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

#include "table/table_statistics.hpp"

#include <sstream>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
class TableStatisticsTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "table_statistics_test-" + RandomString();
    Recover();
    TransactionContext ctx = db_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          db_->CreateTable(ctx,
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
          db_->CreateTable(ctx,
                           Schema("Sc2", {Column("d1", ValueType::kInt64),
                                          Column("d2", ValueType::kDouble),
                                          Column("d3", ValueType::kVarChar),
                                          Column("d4", ValueType::kInt64)})));
      for (int i = 0; i < 20; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value(i + 0.2),
                            Value("d3-" + std::to_string(i)), Value(16)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          db_->CreateTable(ctx,
                           Schema("Sc3", {Column("e1", ValueType::kInt64),
                                          Column("e2", ValueType::kDouble)})));
      for (int i = 10; 0 < i; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 53.4)})).GetStatus());
      }
    }
    ctx.txn_.PreCommit();
    {
      TransactionContext stat_tx = db_->BeginContext();
      db_->RefreshStatistics(stat_tx, "Sc1");
      db_->RefreshStatistics(stat_tx, "Sc2");
      db_->RefreshStatistics(stat_tx, "Sc3");
      ASSERT_SUCCESS(stat_tx.PreCommit());
    }
  }

  void Recover() {
    if (db_) {
      db_->EmulateCrash();
    }
    db_ = std::make_unique<Database>(prefix_);
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(TableStatisticsTest, Construct) {
  // Arrange -- nothing to set up; default TableStatistics created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(TableStatisticsTest, Update) {
  // Arrange -- begin context, get table "Sc1" and its statistics
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(ctx, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, ts, db_->GetStatistics(ctx, "Sc1"));

  // Act -- update the statistics from the table
  ts.Update(ctx.txn_, tbl);
  LOG(TRACE) << ts;
}

TEST_F(TableStatisticsTest, Store) {
  // Arrange -- begin context, get table "Sc1" and its statistics
  TransactionContext ctx = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(ctx, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, ts, db_->GetStatistics(ctx, "Sc1"));

  // Act -- update the statistics, then store them for "Sc2"
  ts.Update(ctx.txn_, tbl);
  LOG(TRACE) << ts;
  db_->UpdateStatistics(ctx, "Sc2", ts);
}

TEST_F(TableStatisticsTest, CollectsHistogramBoundariesAndCommonValues) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));

  ASSERT_SUCCESS(statistics.Update(context.txn_, table));

  EXPECT_EQ(statistics.Rows(), 100);
  const ColumnStats& integers = statistics.Column(0);
  EXPECT_EQ(integers.NonNullCount(), 100);
  EXPECT_EQ(integers.NullCount(), 0);
  EXPECT_EQ(integers.Distinct(), 100);
  EXPECT_FALSE(integers.Histogram().empty());
  EXPECT_LE(integers.Histogram().size(), kHistogramBucketCount);
  ASSERT_EQ(integers.LowestValues().size(), kBoundaryValueCount);
  ASSERT_EQ(integers.HighestValues().size(), kBoundaryValueCount);
  EXPECT_EQ(integers.LowestValues().front().value, Value(0));
  EXPECT_EQ(integers.LowestValues().back().value, Value(4));
  EXPECT_EQ(integers.HighestValues().front().value, Value(95));
  EXPECT_EQ(integers.HighestValues().back().value, Value(99));
  EXPECT_NEAR(statistics.EstimateCount(0, Value(10), Value(19)), 10, 1.5);
}

TEST_F(TableStatisticsTest, UsesSkewAndNullsForSelectivity) {
  {
    TransactionContext context = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        db_->CreateTable(
            context,
            Schema("Distribution", {Column("skew", ValueType::kInt64),
                                    Column("nullable", ValueType::kVarChar)})));
    for (int i = 0; i < 100; ++i) {
      const int skew = i < 60 ? 1 : (i < 80 ? 2 : i - 77);
      const Value nullable =
          i % 10 == 0 ? Value() : Value("value-" + std::to_string(i));
      ASSERT_SUCCESS(
          table.Insert(context.txn_, Row({Value(skew), nullable})).GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    TransactionContext context = db_->BeginContext();
    ASSERT_SUCCESS(db_->RefreshStatistics(context, "Distribution"));
    ASSERT_SUCCESS(context.PreCommit());
  }

  Recover();
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Distribution"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Distribution"));

  ASSERT_EQ(statistics.Rows(), 100);
  const ColumnStats& skew = statistics.Column(0);
  EXPECT_EQ(skew.Distinct(), 22);
  ASSERT_FALSE(skew.MostCommonValues().empty());
  EXPECT_EQ(skew.MostCommonValues().front().value, Value(1));
  EXPECT_EQ(skew.MostCommonValues().front().count, 60);
  EXPECT_DOUBLE_EQ(skew.EstimateEqual(Value(1)), 60);
  EXPECT_NEAR(statistics.EstimateCount(0, Value(3), Value(22)), 20, 1.0);

  EXPECT_EQ(statistics.Column(1).NullCount(), 10);
  Expression equals_one =
      BinaryExpressionExp(ColumnValueExp("Distribution.skew"),
                          BinaryOperation::kEquals, ConstantValueExp(Value(1)));
  EXPECT_NEAR(statistics.EstimateSelectivity(table.GetSchema(), equals_one),
              0.60, 0.001);
  EXPECT_EQ(statistics.Filter(table.GetSchema(), equals_one).Rows(), 60);
  Expression equals_two =
      BinaryExpressionExp(ColumnValueExp("Distribution.skew"),
                          BinaryOperation::kEquals, ConstantValueExp(Value(2)));
  Expression one_or_two =
      BinaryExpressionExp(equals_one, BinaryOperation::kOr, equals_two);
  EXPECT_NEAR(statistics.EstimateSelectivity(table.GetSchema(), one_or_two),
              0.80, 0.001);
  Expression one_and_two =
      BinaryExpressionExp(equals_one, BinaryOperation::kAnd, equals_two);
  EXPECT_DOUBLE_EQ(
      statistics.EstimateSelectivity(table.GetSchema(), one_and_two), 0);
  Expression is_null = UnaryExpressionExp(
      ColumnValueExp("Distribution.nullable"), UnaryOperation::kIsNull);
  EXPECT_NEAR(statistics.EstimateSelectivity(table.GetSchema(), is_null), 0.10,
              0.001);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST(TableStatisticsSerializationTest, ReadsLegacyStatistics) {
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << uint64_t{1} << ValueType::kInt64 << int64_t{99} << int64_t{0}
          << uint64_t{100} << uint64_t{100};

  TableStatistics statistics(
      Schema("Legacy", {Column("value", ValueType::kInt64)}));
  Decoder decoder(stream);
  decoder >> statistics;

  EXPECT_EQ(statistics.Rows(), 100);
  EXPECT_EQ(statistics.Column(0).Distinct(), 100);
  EXPECT_NEAR(statistics.EstimateCount(0, Value(10), Value(19)), 10, 2.0);
}
}  // namespace tinylamb
