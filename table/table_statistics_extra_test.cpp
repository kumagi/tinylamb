/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra TableStatistics coverage beyond table_statistics_test.cpp: the shape
// of the public estimators on a known uniform column (monotonicity, domain
// bounds, inversion clamps) and the distinct-rows group estimate.
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class TableStatisticsExtraTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "table_statistics_extra-" + RandomString();
    db_ = Database::Create(prefix_).MoveValue();
    TransactionContext ctx = db_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, tbl,
        db_->CreateTable(ctx, Schema("U", {Column("n", ValueType::kInt64),
                                           Column("s", ValueType::kVarChar)})));
    // 200 uniform rows: n = 0..199 with every key distinct.
    for (int i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          tbl.Insert(ctx.txn_, Row({Value(int64_t{i}),
                                    Value("key-" + std::to_string(i))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.PreCommit());
    TransactionContext stat_tx = db_->BeginContext();
    ASSERT_SUCCESS(db_->RefreshStatistics(stat_tx, "U"));
    ASSERT_SUCCESS(stat_tx.PreCommit());
    ASSIGN_OR_ASSERT_FAIL(TableStatistics, stats,
                          db_->GetStatistics(stat_tx, "U"));
    stats_ = std::move(stats);
  }

  void TearDown() override { db_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> db_;
  TableStatistics stats_{Schema("U", {})};
};

TEST_F(TableStatisticsExtraTest, EqualEstimateRespectsTheObservedDomain) {
  const ColumnStats& n = stats_.Column(0);
  // Every observed key is unique: an equality estimate may not exceed a
  // small row count, and a key outside the measured domain matches nothing.
  EXPECT_GE(n.EstimateEqual(Value(int64_t{100})), 0.0);
  EXPECT_LE(n.EstimateEqual(Value(int64_t{100})), 4.0);
  EXPECT_DOUBLE_EQ(n.EstimateEqual(Value(int64_t{100000})), 0.0);
}

TEST_F(TableStatisticsExtraTest, RangeEstimateNeverInvertsOrExceedsRows) {
  const ColumnStats& n = stats_.Column(0);
  // An inverted range (lower above upper) clamps to zero rows.
  EXPECT_DOUBLE_EQ(
      n.EstimateRange(Value(int64_t{150}), true, Value(int64_t{50}), true),
      0.0);
  // The full domain stays within the measured row budget and covers
  // essentially every row.
  const double full =
      n.EstimateRange(Value(int64_t{0}), true, Value(int64_t{199}), true);
  EXPECT_GE(full, 0.0);
  EXPECT_LE(full, static_cast<double>(stats_.Rows()) + 1.0);
  // A range outside the domain matches nothing.
  EXPECT_DOUBLE_EQ(
      n.EstimateRange(Value(int64_t{1000}), true, Value(int64_t{2000}), true),
      0.0);
}

TEST_F(TableStatisticsExtraTest, JoinEstimateStaysBoundedByRowCounts) {
  const ColumnStats& n = stats_.Column(0);
  // Self-join over 200 unique keys: at most one match per side, so the
  // estimate may not blow past the row budget.
  const double self_join = n.EstimateJoinMatches(n);
  EXPECT_GE(self_join, 0.0);
  EXPECT_LE(self_join, static_cast<double>(stats_.Rows()) + 1.0);
}

TEST_F(TableStatisticsExtraTest, DistinctRowsEstimateTracksUniqueKeys) {
  // n has 200 distinct values; the group estimate should be near 200 (NULL
  // included), never orders of magnitude off.
  const double groups = stats_.EstimateDistinctRows({0});
  EXPECT_GE(groups, 100.0);
  EXPECT_LE(groups, 400.0);
  // The varchar column is also unique per row (key-<n>).
  const double strings = stats_.EstimateDistinctRows({1});
  EXPECT_GE(strings, 100.0);
  EXPECT_LE(strings, 400.0);
}

}  // namespace tinylamb
