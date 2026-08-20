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
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

#include <limits>
#include <stdexcept>

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

TEST_F(TableStatisticsTest, PersistsWideTableAsSeparateColumnEntries) {
  {
    TransactionContext context = db_->BeginContext();
    std::vector<Column> columns;
    columns.reserve(20);
    for (int i = 0; i < 20; ++i) {
      columns.emplace_back("c" + std::to_string(i), ValueType::kVarChar);
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        db_->CreateTable(context, Schema("WideStats", std::move(columns))));
    for (int row = 0; row < 40; ++row) {
      std::vector<Value> values;
      values.reserve(20);
      for (int column = 0; column < 20; ++column) {
        values.emplace_back(std::string(80, static_cast<char>('a' + (row + column) % 26)));
      }
      ASSERT_SUCCESS(
          table.Insert(context.txn_, Row(std::move(values))).GetStatus());
    }
    ASSERT_SUCCESS(context.PreCommit());
  }
  {
    TransactionContext context = db_->BeginContext();
    ASSERT_SUCCESS(db_->RefreshStatistics(context, "WideStats"));
    ASSERT_SUCCESS(context.PreCommit());
  }

  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "WideStats"));
  EXPECT_EQ(statistics.Rows(), 40);
  EXPECT_EQ(statistics.Columns(), 20);
  EXPECT_EQ(statistics.Column(0).NonNullCount(), 40);
  EXPECT_EQ(statistics.Column(19).NonNullCount(), 40);
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

TEST_F(TableStatisticsTest, ColumnStatsFitInOneLeafEntry) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << statistics.Column(1);
  constexpr size_t kMaxLeafEntry = kPageBodySize / 6;
  EXPECT_LT(stream.str().size() + 8, kMaxLeafEntry);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST_F(TableStatisticsTest, ReductionFactorAndTransformBy) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  // Act -- ask for the reduction factor of a selective predicate
  Expression equals = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kEquals,
      ConstantValueExp(Value(int64_t{5})));
  // Assert -- one of ~100 distinct values implies a factor near 100
  EXPECT_NEAR(statistics.ReductionFactor(schema, equals), 100.0, 20.0);

  // Act -- ask for a predicate that matches nothing
  Expression impossible = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kEquals,
      ConstantValueExp(Value(int64_t{-999})));
  // Assert -- zero selectivity maps to the maximum double factor
  EXPECT_DOUBLE_EQ(statistics.ReductionFactor(schema, impossible),
                   std::numeric_limits<double>::max());

  // Act -- transform by a ~10-row range
  TableStatistics transformed =
      statistics.TransformBy(0, Value(int64_t{10}), Value(int64_t{19}));
  // Assert -- the result keeps a proportional row count
  EXPECT_NEAR(transformed.Rows(), 10, 2.0);
}

TEST_F(TableStatisticsTest, ReverseComparisonWithConstantOnLeft) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  // Act -- constant-on-left equality (exercises ReverseComparison through
  // ExtractAtomicPredicate)
  Expression left_equals = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{5})), BinaryOperation::kEquals,
      ColumnValueExp("Sc1.c1"));
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, left_equals), 0.01, 0.01);

  // Act -- constant-on-left "greater than" becomes column < constant
  Expression left_greater = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{50})), BinaryOperation::kGreaterThan,
      ColumnValueExp("Sc1.c1"));
  // Assert -- about half of the 0..99 rows are below 50
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, left_greater), 0.5, 0.05);
}

TEST_F(TableStatisticsTest, NotEqualsAndLikeSelectivity) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  // Act -- c1 != 5
  Expression not_equals = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kNotEquals,
      ConstantValueExp(Value(int64_t{5})));
  // Assert -- nearly every row survives
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, not_equals), 0.99, 0.01);

  // Act -- c2 LIKE and NOT LIKE a wildcard
  Expression like = BinaryExpressionExp(
      ColumnValueExp("Sc1.c2"), BinaryOperation::kLike,
      ConstantValueExp(Value(std::string("%"))));
  Expression not_like = BinaryExpressionExp(
      ColumnValueExp("Sc1.c2"), BinaryOperation::kNotLike,
      ConstantValueExp(Value(std::string("%"))));
  // Assert -- the LIKE guess is a small fraction of the rows
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, like), 0.1, 0.05);
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, not_like), 0.9, 0.05);
}

TEST_F(TableStatisticsTest, CorrelatedRangeOnSameColumn) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  Expression equals_one = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kEquals,
      ConstantValueExp(Value(int64_t{1})));
  Expression greater_double = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(2.5)));
  // Act -- (c1 = 1) AND (c1 > 2.5): the cross-type constant check must rule it
  // out (1 > 2.5 is false)
  Expression impossible_and = BinaryExpressionExp(
      equals_one, BinaryOperation::kAnd, greater_double);
  // Assert -- ValueSatisfies drives the AND to zero
  EXPECT_DOUBLE_EQ(statistics.EstimateSelectivity(schema, impossible_and), 0);

  // Act -- (c1 > 1) OR (c1 < 3): correlated bounds on one ordered column
  Expression greater_one = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{1})));
  Expression less_three = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kLessThan,
      ConstantValueExp(Value(int64_t{3})));
  Expression correlated_or = BinaryExpressionExp(
      greater_one, BinaryOperation::kOr, less_three);
  // Assert -- the wider bound wins
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, correlated_or), 0.98,
              0.05);

  // Act -- (c1 > 1) AND (c1 < 3)
  Expression correlated_and = BinaryExpressionExp(
      greater_one, BinaryOperation::kAnd, less_three);
  // Assert -- the tighter bound wins for a single ordered column
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, correlated_and), 0.03,
              0.03);
}

TEST_F(TableStatisticsTest, InExpressionEstimate) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  // Act -- c1 IN (1, 2)
  Expression in = InExpressionExp(
      ColumnValueExp("Sc1.c1"),
      {ConstantValueExp(Value(int64_t{1})), ConstantValueExp(Value(int64_t{2}))});
  // Assert -- the two members contribute their estimated counts
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, in), 0.02, 0.01);

  // Act -- IN over a compound (non-column) child that still touches columns
  Expression in_compound = InExpressionExp(
      BinaryExpressionExp(ColumnValueExp("Sc1.c1"),
                          BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1}))),
      {ConstantValueExp(Value(int64_t{2}))});
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, in_compound), 0.25,
              0.001);

  // Act -- IN whose member list is not all constants
  Expression in_column = InExpressionExp(
      ColumnValueExp("Sc1.c1"), {ColumnValueExp("Sc1.c2")});
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, in_column), 0.25, 0.001);
}

TEST_F(TableStatisticsTest, ColumnToColumnEquality) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  // Act -- c1 = c1 (same column)
  Expression self = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kEquals,
      ColumnValueExp("Sc1.c1"));
  // Assert -- every non-null row matches itself
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, self), 1.0, 0.001);

  // Act -- c1 = c2 (two independent distinct columns)
  Expression cross = BinaryExpressionExp(
      ColumnValueExp("Sc1.c1"), BinaryOperation::kEquals,
      ColumnValueExp("Sc1.c2"));
  // Assert -- the two-column equality is bounded by the max distinct count
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, cross), 0.01, 0.01);
}

TEST_F(TableStatisticsTest, ConstantAndUnknownPredicateShapes) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, table, db_->GetTable(context, "Sc1"));
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));
  const Schema& schema = table.GetSchema();

  // Act -- a predicate over no columns that evaluates to true / false
  Expression constant_true = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{3})), BinaryOperation::kEquals,
      ConstantValueExp(Value(int64_t{3})));
  Expression constant_false = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{3})), BinaryOperation::kEquals,
      ConstantValueExp(Value(int64_t{4})));
  // Assert -- the evaluated boolean drives the estimate to 1.0 / 0.0
  EXPECT_DOUBLE_EQ(statistics.EstimateSelectivity(schema, constant_true), 1.0);
  EXPECT_DOUBLE_EQ(statistics.EstimateSelectivity(schema, constant_false), 0.0);

  // Act -- a bare column value is not a supported predicate shape
  // Assert -- the fallback guess of 0.25 is returned
  EXPECT_NEAR(statistics.EstimateSelectivity(schema, ColumnValueExp("Sc1.c1")),
              0.25, 0.001);
}

TEST_F(TableStatisticsTest, ScaleMultiplyConcatAssign) {
  TransactionContext context = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(TableStatistics, statistics,
                        db_->GetStatistics(context, "Sc1"));

  // Act -- scale up to 200 rows (multiplier > 1)
  TableStatistics grown = statistics.ScaleToRows(200);
  // Assert -- the row count and per-column non-null counts scale up
  EXPECT_EQ(grown.Rows(), 200);
  EXPECT_EQ(grown.Column(0).NonNullCount(), 200);

  // Act -- duplicate via operator*
  TableStatistics doubled = statistics * 2;
  EXPECT_EQ(doubled.Rows(), 200);

  // Act -- concatenate two statistics objects
  TableStatistics merged = statistics;
  merged.Concat(statistics);
  // Assert -- stats_ appends while the row count stays untouched
  EXPECT_EQ(merged.Rows(), 100);
  EXPECT_EQ(merged.Columns(), 6);

  // Act -- rebuild via Assign
  TableStatistics assigned(Schema("T", {Column("x", ValueType::kInt64)}));
  std::vector<ColumnStats> columns;
  columns.emplace_back(ValueType::kInt64);
  assigned.Assign(10, std::move(columns));
  EXPECT_EQ(assigned.Rows(), 10);
  EXPECT_EQ(assigned.Columns(), 1);
  ASSERT_SUCCESS(context.PreCommit());
}

TEST(TableStatisticsSerializationTest, NewFormatRoundTrip) {
  // Arrange -- statistics over a small schema built from raw column stats
  TableStatistics original(
      Schema("T", {Column("a", ValueType::kInt64)}));
  std::vector<ColumnStats> columns;
  columns.emplace_back(ValueType::kInt64);
  original.Assign(5, std::move(columns));

  // Act -- serialize and deserialize with the v2 (magic-prefixed) format
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << original;
  TableStatistics decoded(Schema("T", {Column("a", ValueType::kInt64)}));
  Decoder decoder(stream);
  decoder >> decoded;

  // Assert -- the round trip preserves every field
  EXPECT_EQ(decoded, original);
  EXPECT_EQ(decoded.Rows(), 5);
  EXPECT_EQ(decoded.Column(0).Type(), ValueType::kInt64);
}

TEST(TableStatisticsSerializationTest, LegacyDoubleDateVarcharDecode) {
  {
    // Act -- decode a legacy (no magic) statistics blob for a double column
    std::stringstream stream;
    Encoder encoder(stream);
    encoder << uint64_t{1} << ValueType::kDouble << double{5.5} << double{1.5}
            << uint64_t{3} << uint64_t{2};
    TableStatistics statistics(
        Schema("T", {Column("d", ValueType::kDouble)}));
    Decoder decoder(stream);
    decoder >> statistics;
    // Assert -- the legacy double histogram bucket is reconstructed
    EXPECT_EQ(statistics.Rows(), 3);
    EXPECT_EQ(statistics.Column(0).NonNullCount(), 3);
    EXPECT_EQ(statistics.Column(0).Distinct(), 2);
    ASSERT_EQ(statistics.Column(0).Histogram().size(), 1);
    EXPECT_EQ(statistics.Column(0).Histogram().front().count, 3);
  }
  {
    // Act -- decode a legacy blob for a date column
    std::stringstream stream;
    Encoder encoder(stream);
    encoder << uint64_t{1} << ValueType::kDate << int64_t{20000}
            << int64_t{10000} << uint64_t{4} << uint64_t{3};
    TableStatistics statistics(
        Schema("T", {Column("t", ValueType::kDate)}));
    Decoder decoder(stream);
    decoder >> statistics;
    // Assert -- the date bucket is rebuilt from day counts
    EXPECT_EQ(statistics.Rows(), 4);
    ASSERT_EQ(statistics.Column(0).Histogram().size(), 1);
    EXPECT_EQ(statistics.Column(0).Histogram().front().count, 4);
  }
  {
    // Act -- decode a legacy blob for a varchar column (no bucket payload)
    std::stringstream stream;
    Encoder encoder(stream);
    encoder << uint64_t{1} << ValueType::kVarChar << uint64_t{7} << uint64_t{6};
    TableStatistics statistics(
        Schema("T", {Column("v", ValueType::kVarChar)}));
    Decoder decoder(stream);
    decoder >> statistics;
    // Assert -- counts are recovered with an empty histogram
    EXPECT_EQ(statistics.Rows(), 7);
    EXPECT_EQ(statistics.Column(0).NonNullCount(), 7);
    EXPECT_EQ(statistics.Column(0).Distinct(), 6);
    EXPECT_TRUE(statistics.Column(0).Histogram().empty());
  }
}

TEST(TableStatisticsSerializationTest, LegacyNullColumnThrows) {
  // Act -- decode a legacy blob whose column type is kNull
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << uint64_t{1} << ValueType::kNull;
  TableStatistics statistics(Schema("T", {Column("n", ValueType::kNull)}));
  Decoder decoder(stream);
  // Assert -- the invalid legacy type is rejected loudly
  EXPECT_THROW(decoder >> statistics, std::runtime_error);
}

}  // namespace tinylamb
