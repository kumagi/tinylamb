/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_window_agg_coverage_test.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
using namespace window_coverage_test;

class SqlWindowAggCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sql_window_agg_coverage_test-" + RandomString())
            .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlWindowAggCoverageTest, Smoke) {
  const auto rows = RunSql(engine_.get(), context_.get(), "SELECT 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

// The OOC invertible sliding SUM must remove the leaving row before adding
// the entering one. Add-first briefly holds both rows and raises a spurious
// INT64 overflow even though every real frame is in range.
TEST_F(SqlWindowAggCoverageTest, SlidingSumIgnoresTransientFrameOverflow) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE win_ov (k INT64, v INT64);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO win_ov VALUES "
         "(1, 9223372036854775807), (2, -1), (3, 9223372036854775807);");

  auto& budget = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  struct ResetGuard {
    ResetGuard() = default;
    ResetGuard(const ResetGuard&) = delete;
    ResetGuard& operator=(const ResetGuard&) = delete;
    ResetGuard(ResetGuard&&) = delete;
    ResetGuard& operator=(ResetGuard&&) = delete;
    ~ResetGuard() {
      ProcessMemoryBudget::Global().ResetTestKnobs();
      ProcessMemoryBudget::Global().SetTestOpMemoryBytes(0);
    }
  } guard;
  budget.SetTestOpMemoryBytes(64ULL << 10);
  budget.SetTestAlwaysSpill(true);

  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND "
             "CURRENT ROW) FROM win_ov");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{9223372036854775807LL}));
  EXPECT_EQ(rows[1][0], Value(int64_t{9223372036854775806LL}));
  EXPECT_EQ(rows[2][0], Value(int64_t{9223372036854775806LL}));
}

// ---------------------------------------------------------------------------
// Window functions through the SQL surface with ties, NULLs, NaNs, and every
// frame spec the frontend can spell.
// ---------------------------------------------------------------------------

class WindowSqlCov : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("window_cov-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    // g=1 rows carry ties on k, a NULL k, a NaN f, and a NULL f/date so the
    // frame-resolution code sees every degenerate order key.
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(),
                  "CREATE TABLE wx (g INT64, k INT64, v INT64, f FLOAT64, "
                  "d DATE, s VARCHAR(20));")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO wx VALUES "
                          "(1, 1, 10, 1.5, DATE '2020-01-01', 'Apple'), "
                          "(1, 1, 20, 1.5, DATE '2020-01-02', 'banana'), "
                          "(1, 2, 30, 2.5, DATE '2020-01-03', 'Cherry'), "
                          "(1, NULL, 40, CAST('NaN' AS FLOAT64), "
                          "DATE '2020-01-04', 'date'), "
                          "(1, 4, 50, NULL, NULL, 'Egg'), "
                          "(2, 5, 60, 3.5, DATE '2020-01-05', 'Fig');")
                    .HasValue());
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(WindowSqlCov, RankingFamilyWithNullAndDefaultOrderKeys) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT ROW_NUMBER() OVER (ORDER BY k), "
      "RANK() OVER (ORDER BY k), DENSE_RANK() OVER (ORDER BY k), "
      "PERCENT_RANK() OVER (ORDER BY k), CUME_DIST() OVER (ORDER BY k), "
      "RANK() OVER (ORDER BY k DESC) "
      "FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  // k ASC default sorts NULLS FIRST: 40(NULL), 10, 20, 30, 50.
  // k DESC default sorts NULLS LAST: 50, 30, 10, 20, 40.
  ASSERT_EQ(rows.Value().size(), 5U);
  const std::vector<Row>& out = rows.Value();
  EXPECT_EQ(
      out[0],
      (Row({Value(1), Value(1), Value(1), Value(0.0), Value(0.2), Value(5)})));
  EXPECT_EQ(
      out[1],
      (Row({Value(2), Value(2), Value(2), Value(0.25), Value(0.6), Value(3)})));
  EXPECT_EQ(
      out[2],
      (Row({Value(3), Value(2), Value(2), Value(0.25), Value(0.6), Value(3)})));
  EXPECT_EQ(
      out[3],
      (Row({Value(4), Value(4), Value(3), Value(0.75), Value(0.8), Value(2)})));
  EXPECT_EQ(
      out[4],
      (Row({Value(5), Value(5), Value(4), Value(1.0), Value(1.0), Value(1)})));
}

TEST_F(WindowSqlCov, NullsFirstLastExplicitInWindowOrder) {
  const auto rows = TryRunSql(engine_.get(), context_.get(),
                              "SELECT RANK() OVER (ORDER BY k NULLS FIRST), "
                              "RANK() OVER (ORDER BY k NULLS LAST), "
                              "RANK() OVER (ORDER BY k DESC NULLS FIRST), "
                              "SUM(v) OVER (ORDER BY k ASC NULLS LAST) "
                              "FROM wx WHERE g = 1 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  const std::vector<Row>& out = rows.Value();
  // v=10 row (k=1): NULLS FIRST rank 2, NULLS LAST rank 1, DESC rank 4.
  EXPECT_EQ(out[0], (Row({Value(2), Value(1), Value(4), Value(30)})));
  EXPECT_EQ(out[1], (Row({Value(2), Value(1), Value(4), Value(30)})));
  EXPECT_EQ(out[2], (Row({Value(4), Value(3), Value(3), Value(60)})));
  // NULL-key row: first under NULLS FIRST/DESC-first, last under NULLS LAST;
  // the trailing RANGE frame spans the whole partition (sum 150).
  EXPECT_EQ(out[3], (Row({Value(1), Value(5), Value(1), Value(150)})));
  EXPECT_EQ(out[4], (Row({Value(5), Value(4), Value(2), Value(110)})));
}

TEST_F(WindowSqlCov, NanOrderKeysAreOnePeerGroupBeforeFiniteValues) {
  // A second NaN row makes the peer-group fold observable: if NaN keys ever
  // split into separate peer groups the two NaN rows would get different
  // ranks and frame sums instead of sharing one group.
  ASSERT_TRUE(TryRunSql(engine_.get(), context_.get(),
                        "INSERT INTO wx VALUES "
                        "(1, 3, 45, CAST('NaN' AS FLOAT64), "
                        "DATE '2020-01-05', 'date2');")
                  .HasValue());
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT RANK() OVER (ORDER BY f), SUM(v) OVER (ORDER BY f), "
                "CUME_DIST() OVER (ORDER BY f DESC) "
                "FROM wx WHERE g = 1 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 6U);
  const std::vector<Row>& out = rows.Value();
  // f order: NULL(50) < NaN(40,45) < 1.5(10,20) < 2.5(30).  Peer groups:
  // {NULL}:rank 1, {NaN,NaN}:rank 2, {1.5,1.5}:rank 4, {2.5}:rank 6.
  EXPECT_EQ(out[0][0], Value(4));
  EXPECT_EQ(out[1][0], Value(4));
  EXPECT_EQ(out[2][0], Value(6));
  EXPECT_EQ(out[3][0], Value(2));
  EXPECT_EQ(out[4][0], Value(2));
  EXPECT_EQ(out[5][0], Value(1));
  // RANGE .. CURRENT ROW frames: NaN peers share {NULL,NaN,NaN}=135; the two
  // 1.5 rows share {NULL,NaN,NaN,1.5,1.5}=165; 2.5 sees all six = 195.
  EXPECT_EQ(out[0][1], Value(165));
  EXPECT_EQ(out[1][1], Value(165));
  EXPECT_EQ(out[2][1], Value(195));
  EXPECT_EQ(out[3][1], Value(135));
  EXPECT_EQ(out[4][1], Value(135));
  EXPECT_EQ(out[5][1], Value(50));
  // CUME_DIST over f DESC (2.5, 1.5, NaN, NULL): 1/6, 3/6, 5/6, 6/6.
  EXPECT_DOUBLE_EQ(out[0][2].value.double_value, 0.5);
  EXPECT_DOUBLE_EQ(out[1][2].value.double_value, 0.5);
  EXPECT_DOUBLE_EQ(out[2][2].value.double_value, 1.0 / 6.0);
  EXPECT_DOUBLE_EQ(out[3][2].value.double_value, 5.0 / 6.0);
  EXPECT_DOUBLE_EQ(out[4][2].value.double_value, 5.0 / 6.0);
  EXPECT_DOUBLE_EQ(out[5][2].value.double_value, 1.0);
}

// P349: PERCENTILE_CONT/DISC ignore the frame, and frame-less MODE over an
// unordered partition sees the whole partition from every row; such
// whole-partition frames are evaluated once per partition and broadcast.
// No counter exposes the broadcast itself, so this pin locks the broadcast
// values to the per-row values the old m-times loop produced, covering a
// multi-row partition, a single-row partition (m=1), a unique-mode
// partition, and a no-unique-mode partition.
TEST_F(WindowSqlCov, WholePartitionFrameValuesArePartitionConstant) {
  ASSERT_TRUE(TryRunSql(engine_.get(), context_.get(),
                        "INSERT INTO wx VALUES "
                        "(3, 7, 70, 9.5, DATE '2020-02-01', 'plum'), "
                        "(3, 8, 70, 9.5, DATE '2020-02-02', 'quince'), "
                        "(3, 9, 80, 9.5, DATE '2020-02-03', 'raisin');")
                  .HasValue());
  const auto rows = TryRunSql(engine_.get(), context_.get(),
                              "SELECT g, v, MODE(v) OVER (PARTITION BY g), "
                              "PERCENTILE_CONT(v, 0.5) OVER (PARTITION BY g), "
                              "PERCENTILE_DISC(v, 0.5) OVER (PARTITION BY g) "
                              "FROM wx ORDER BY g, v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 9U);
  const std::vector<Row>& out = rows.Value();
  // g=1: five rows, every value frequency 1 -> the engine's documented
  // first-to-maximum tie-break keeps 10 on every row of the partition.
  for (size_t i = 0; i < 5U; ++i) {
    EXPECT_EQ(out[i][0], Value(1)) << i;
    EXPECT_EQ(out[i][2], Value(10)) << i;
    EXPECT_DOUBLE_EQ(out[i][3].value.double_value, 30.0) << i;
    EXPECT_EQ(out[i][4], Value(30)) << i;
  }
  // g=2: single-row partition broadcast.
  EXPECT_EQ(out[5][2], Value(60));
  EXPECT_DOUBLE_EQ(out[5][3].value.double_value, 60.0);
  EXPECT_EQ(out[5][4], Value(60));
  // g=3: 70 appears twice -> unique mode; median 70.
  for (size_t i = 6; i < 9U; ++i) {
    EXPECT_EQ(out[i][0], Value(3)) << i;
    EXPECT_EQ(out[i][2], Value(70)) << i;
    EXPECT_DOUBLE_EQ(out[i][3].value.double_value, 70.0) << i;
    EXPECT_EQ(out[i][4], Value(70)) << i;
  }
}

TEST_F(WindowSqlCov, WindowOrderPlacesNanBeforeFiniteValues) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT v, ROW_NUMBER() OVER (ORDER BY f) "
                "FROM wx WHERE g = 1 AND f IS NOT NULL ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);
  EXPECT_EQ(rows.Value()[0], Row({Value(10), Value(2)}));
  EXPECT_EQ(rows.Value()[1], Row({Value(20), Value(3)}));
  EXPECT_EQ(rows.Value()[2], Row({Value(30), Value(4)}));
  EXPECT_EQ(rows.Value()[3], Row({Value(40), Value(1)}));
}

TEST_F(WindowSqlCov, RangeOffsetFramesAroundInt64Keys) {
  // ASC n PRECEDING..n FOLLOWING.
  const auto asc = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 "
      "FOLLOWING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(asc.HasValue()) << asc.GetStatus().GetMessage();
  EXPECT_EQ(asc.Value(), (std::vector<Row>({Row({Value(40)}), Row({Value(60)}),
                                            Row({Value(60)}), Row({Value(60)}),
                                            Row({Value(50)})})));

  // DESC UNBOUNDED PRECEDING..n PRECEDING: DESC mirrors the band; the NULL
  // key row sits below every band and keeps the whole partition.
  const auto desc = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN UNBOUNDED PRECEDING "
      "AND 1 PRECEDING) FROM wx WHERE g = 1 ORDER BY k DESC NULLS LAST, v");
  ASSERT_TRUE(desc.HasValue()) << desc.GetStatus().GetMessage();
  EXPECT_EQ(desc.Value(), (std::vector<Row>({Row({Value()}), Row({Value(50)}),
                                             Row({Value(80)}), Row({Value(80)}),
                                             Row({Value(150)})})));

  // DESC n PRECEDING..CURRENT ROW: the band is [k, k+1] plus the peer
  // group; the NULL key's offset band is just its own peer group.
  const auto desc_start = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 1 PRECEDING AND "
      "CURRENT ROW) FROM wx WHERE g = 1 ORDER BY k DESC NULLS LAST, v");
  ASSERT_TRUE(desc_start.HasValue()) << desc_start.GetStatus().GetMessage();
  EXPECT_EQ(
      desc_start.Value(),
      (std::vector<Row>({Row({Value(50)}), Row({Value(30)}), Row({Value(60)}),
                         Row({Value(60)}), Row({Value(40)})})));

  // CURRENT ROW..n PRECEDING is empty except for the NULL-key singleton.
  const auto inverted = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND 1 "
      "PRECEDING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(inverted.HasValue()) << inverted.GetStatus().GetMessage();
  EXPECT_EQ(inverted.Value(),
            (std::vector<Row>({Row({Value(40)}), Row({Value()}), Row({Value()}),
                               Row({Value()}), Row({Value()})})));

  // n FOLLOWING..m FOLLOWING with n > m never matches: numeric keys get an
  // empty frame; the NULL key's offset band degenerates to its own peer
  // group.
  const auto past_end = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 2 FOLLOWING AND 1 "
      "FOLLOWING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(past_end.HasValue()) << past_end.GetStatus().GetMessage();
  ASSERT_EQ(past_end.Value().size(), 5U);
  EXPECT_EQ(past_end.Value()[0], Row({Value(40)}));
  for (size_t i = 1; i < past_end.Value().size(); ++i) {
    EXPECT_TRUE(past_end.Value()[i][0].IsNull());
  }

  // UNBOUNDED FOLLOWING start bound pins the frame to the last row.
  const auto unbounded_following = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED FOLLOWING AND "
      "UNBOUNDED FOLLOWING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(unbounded_following.HasValue())
      << unbounded_following.GetStatus().GetMessage();
  for (const Row& row : unbounded_following.Value()) {
    EXPECT_EQ(row[0], Value(50));
  }

  // CURRENT ROW..UNBOUNDED PRECEDING keeps only the leading NULL peer group.
  const auto unbounded_preceding = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND "
      "UNBOUNDED PRECEDING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(unbounded_preceding.HasValue())
      << unbounded_preceding.GetStatus().GetMessage();
  ASSERT_EQ(unbounded_preceding.Value().size(), 5U);
  EXPECT_EQ(unbounded_preceding.Value()[0], Row({Value(40)}));
  for (size_t i = 1; i < unbounded_preceding.Value().size(); ++i) {
    EXPECT_TRUE(unbounded_preceding.Value()[i][0].IsNull());
  }
}

TEST_F(WindowSqlCov, RangeOffsetFramesOnFloatKeys) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY f RANGE BETWEEN 1.0 PRECEDING AND 1.0 "
      "FOLLOWING) FROM wx WHERE g = 1 ORDER BY f NULLS FIRST, v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  // Order: NULL(50), NaN(40), 1.5(10), 1.5(20), 2.5(30).
  EXPECT_EQ(rows.Value(), (std::vector<Row>({Row({Value(50)}), Row({Value(40)}),
                                             Row({Value(60)}), Row({Value(60)}),
                                             Row({Value(60)})})));
}

TEST_F(WindowSqlCov, FloatRangeIncludesExactBoundaryOnly) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1.0 PRECEDING "
      "AND CURRENT ROW), "
      "SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 1.0 PRECEDING "
      "AND CURRENT ROW) "
      "FROM UNNEST([STRUCT(1.5 AS k, 10 AS v), STRUCT(2.5 AS k, 20 AS v), "
      "STRUCT(2.5000000004 AS k, 40 AS v), STRUCT(3.5 AS k, 80 AS v)]) "
      "ORDER BY k");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus();
  EXPECT_EQ(rows.Value(),
            (std::vector<Row>{
                Row({Value(10), Value(30)}), Row({Value(30), Value(140)}),
                Row({Value(60), Value(120)}), Row({Value(140), Value(80)})}));
}

// INTERVAL boundaries turn RANGE frames into calendar-aware rolling windows
// over DATE keys: day arithmetic for DAY/WEEK, month-end clamping for MONTH.
TEST_F(WindowSqlCov, RangeIntervalFramesOnDateKeys) {
  // g=1 date rows: 01-01(v=10), 01-02(20), 01-03(30), 01-04(40), NULL(50).
  const auto rolling = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT d, SUM(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL 1 DAY "
      "PRECEDING AND CURRENT ROW) FROM wx WHERE d IS NOT NULL AND g = 1 "
      "ORDER BY d");
  ASSERT_TRUE(rolling.HasValue()) << rolling.GetStatus().GetMessage();
  EXPECT_EQ(rolling.Value(),
            (std::vector<Row>({Row({Value::Date("2020-01-01"), Value(10)}),
                               Row({Value::Date("2020-01-02"), Value(30)}),
                               Row({Value::Date("2020-01-03"), Value(50)}),
                               Row({Value::Date("2020-01-04"), Value(70)})})));

  // MONTH applies the same calendar arithmetic as DATE - INTERVAL: each
  // frame opens one month before its key, so only the last row's window
  // (opened at 2019-12-04) reaches back over the whole group.
  const auto monthly = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT d, SUM(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL 1 MONTH "
      "PRECEDING AND CURRENT ROW) FROM wx WHERE d IS NOT NULL AND g = 1 "
      "ORDER BY d");
  ASSERT_TRUE(monthly.HasValue()) << monthly.GetStatus().GetMessage();
  // Each frame opens at key - 1 month: only 2020-01-04's window (back to
  // 2019-12-04) reaches the whole group.
  EXPECT_EQ(monthly.Value(),
            (std::vector<Row>({Row({Value::Date("2020-01-01"), Value(10)}),
                               Row({Value::Date("2020-01-02"), Value(30)}),
                               Row({Value::Date("2020-01-03"), Value(60)}),
                               Row({Value::Date("2020-01-04"), Value(100)})})));

  // FOLLOWING end bound: the frame closes one calendar day after the key.
  const auto following = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT d, SUM(v) OVER (ORDER BY d RANGE BETWEEN CURRENT ROW AND "
      "INTERVAL 1 DAY FOLLOWING) FROM wx WHERE d IS NOT NULL AND g = 1 "
      "ORDER BY d");
  ASSERT_TRUE(following.HasValue()) << following.GetStatus().GetMessage();
  EXPECT_EQ(following.Value(),
            (std::vector<Row>({Row({Value::Date("2020-01-01"), Value(30)}),
                               Row({Value::Date("2020-01-02"), Value(50)}),
                               Row({Value::Date("2020-01-03"), Value(70)}),
                               Row({Value::Date("2020-01-04"), Value(40)})})));

  // DESC order mirrors the band around the current key.
  const auto descending = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT d, SUM(v) OVER (ORDER BY d DESC RANGE BETWEEN INTERVAL 1 DAY "
      "PRECEDING AND CURRENT ROW) FROM wx WHERE d IS NOT NULL AND g = 1 "
      "ORDER BY d");
  ASSERT_TRUE(descending.HasValue()) << descending.GetStatus().GetMessage();
  EXPECT_EQ(descending.Value(),
            (std::vector<Row>({Row({Value::Date("2020-01-01"), Value(30)}),
                               Row({Value::Date("2020-01-02"), Value(50)}),
                               Row({Value::Date("2020-01-03"), Value(70)}),
                               Row({Value::Date("2020-01-04"), Value(40)})})));

  // A NULL key has no interval arithmetic: the bound collapses to the peer
  // group, so the NULL-d row aggregates alone.
  const auto null_key = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL 1 DAY PRECEDING "
      "AND CURRENT ROW) FROM wx WHERE d IS NULL AND g = 1");
  ASSERT_TRUE(null_key.HasValue()) << null_key.GetStatus().GetMessage();
  EXPECT_EQ(null_key.Value(), (std::vector<Row>({Row({Value(50)})})));

  // INTERVAL bounds are DATE-only; other key types keep the numeric rules.
  const auto int_key = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN INTERVAL 1 DAY PRECEDING "
      "AND CURRENT ROW) FROM wx WHERE g = 1");
  ASSERT_FALSE(int_key.HasValue());
  EXPECT_NE(int_key.GetStatus().GetMessage().find(
                "RANGE INTERVAL offset requires a DATE order key"),
            std::string::npos)
      << int_key.GetStatus().GetMessage();

  // A negative interval offset must be rejected like a negative numeric one;
  // otherwise PRECEDING's key-minus-negative lands after the current row and
  // the bounded scan silently returns the wrong frame.
  const auto negative = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL -1 DAY PRECEDING "
      "AND CURRENT ROW) FROM wx WHERE g = 1");
  ASSERT_FALSE(negative.HasValue());
  EXPECT_NE(negative.GetStatus().GetMessage().find("non-negative"),
            std::string::npos)
      << negative.GetStatus().GetMessage();
}

TEST_F(WindowSqlCov, RowsFrameCornerBounds) {
  // CURRENT ROW..CURRENT ROW isolates the row itself.
  const auto current = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN CURRENT ROW AND CURRENT "
      "ROW) FROM wx WHERE g = 1 ORDER BY v");
  ASSERT_TRUE(current.HasValue()) << current.GetStatus().GetMessage();
  EXPECT_EQ(
      current.Value(),
      (std::vector<Row>({Row({Value(10)}), Row({Value(20)}), Row({Value(30)}),
                         Row({Value(40)}), Row({Value(50)})})));

  // n FOLLOWING..m FOLLOWING slides past the partition end; rows whose start
  // bound falls off the end get an empty frame.
  const auto following =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN 1 FOLLOWING AND 2 "
                "FOLLOWING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(following.HasValue()) << following.GetStatus().GetMessage();
  // Positions (k NULLS FIRST): 40, 10, 20, 30, 50 -> windows {10,20},
  // {20,30}, {30,50}, {50}, empty.
  EXPECT_EQ(
      following.Value(),
      (std::vector<Row>({Row({Value(30)}), Row({Value(50)}), Row({Value(80)}),
                         Row({Value(50)}), Row({Value()})})));

  // UNBOUNDED PRECEDING..n PRECEDING inverts to an empty frame once n
  // PRECEDING passes the partition start.
  const auto preceding_end = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND 1 "
      "PRECEDING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(preceding_end.HasValue())
      << preceding_end.GetStatus().GetMessage();
  EXPECT_EQ(preceding_end.Value()[0], Row({Value()}));
  EXPECT_EQ(preceding_end.Value()[1], Row({Value(40)}));

  // The engine accepts this extension for RANGE and GROUPS too. An
  // UNBOUNDED PRECEDING endpoint pins the frame to the first ordered row.
  const auto first_only =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED "
                "PRECEDING AND UNBOUNDED PRECEDING) FROM wx WHERE g = 1 "
                "ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(first_only.HasValue()) << first_only.GetStatus().GetMessage();
  EXPECT_EQ(first_only.Value(), std::vector<Row>(5, Row({Value(40)})));

  // UNBOUNDED FOLLOWING start bound selects only the last row.
  const auto last_only = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED FOLLOWING AND "
      "UNBOUNDED FOLLOWING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(last_only.HasValue()) << last_only.GetStatus().GetMessage();
  for (const Row& row : last_only.Value()) {
    EXPECT_EQ(row[0], Value(50));
  }

  // A start bound past the partition end yields an empty frame for ROWS too.
  const auto rows_past = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN 10 FOLLOWING AND "
      "UNBOUNDED FOLLOWING) FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(rows_past.HasValue()) << rows_past.GetStatus().GetMessage();
  for (const Row& row : rows_past.Value()) {
    EXPECT_TRUE(row[0].IsNull());
  }
}

TEST_F(WindowSqlCov, FilterClauseAppliesPerWindowAggregate) {
  const auto rows = TryRunSql(engine_.get(), context_.get(),
                              "SELECT SUM(v WHERE v > 20) OVER (), "
                              "COUNT(* WHERE v > 20) OVER (), "
                              "COUNT(v WHERE v > 20) OVER () "
                              "FROM wx WHERE g = 1 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  for (const Row& row : rows.Value()) {
    EXPECT_EQ(row[0], Value(120));
    EXPECT_EQ(row[1], Value(3));
    EXPECT_EQ(row[2], Value(3));
  }
}

TEST_F(WindowSqlCov, FirstLastNthValueAcrossFrames) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT FIRST_VALUE(v) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 "
      "FOLLOWING), "
      "LAST_VALUE(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND "
      "UNBOUNDED FOLLOWING), "
      "NTH_VALUE(v, 2) OVER (ORDER BY k ROWS BETWEEN CURRENT ROW AND "
      "UNBOUNDED FOLLOWING), "
      "FIRST_VALUE(v) OVER (ORDER BY k ROWS BETWEEN 2 FOLLOWING AND 3 "
      "FOLLOWING) "
      "FROM wx WHERE g = 1 ORDER BY k NULLS FIRST, v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  const std::vector<Row>& out = rows.Value();
  EXPECT_EQ(out[0], (Row({Value(40), Value(50), Value(10), Value(20)})));
  EXPECT_EQ(out[1], (Row({Value(40), Value(50), Value(20), Value(30)})));
  EXPECT_EQ(out[2], (Row({Value(10), Value(50), Value(30), Value(50)})));
  EXPECT_EQ(out[3], (Row({Value(20), Value(50), Value(50), Value()})));
  EXPECT_EQ(out[4], (Row({Value(30), Value(50), Value(), Value()})));
}

TEST_F(WindowSqlCov, WindowFunctionsNestedInExpressions) {
  // Window calls hidden inside unary/CASE/CAST/function/array/IN trees are
  // collected and rewritten back into hidden-column references.
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT -SUM(v) OVER (), "
                "CASE WHEN SUM(v) OVER () > 100 THEN 'big' ELSE 'small' END, "
                "CAST(SUM(v) OVER () AS STRING), ABS(SUM(v) OVER () - 160), "
                "[SUM(v) OVER ()], v IN (10, SUM(v) OVER () - 140) "
                "FROM wx WHERE g = 1 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  const Row& first = rows.Value()[0];
  EXPECT_EQ(first[0], Value(int64_t{-150}));
  EXPECT_EQ(first[1], Value("big"));
  EXPECT_EQ(first[2], Value("150"));
  EXPECT_EQ(first[3], Value(int64_t{10}));
  EXPECT_TRUE(first[4].IsArray());
  EXPECT_TRUE(first[5].Truthy());  // v=10 is in (10, 10)
  EXPECT_FALSE(rows.Value()[2][5].Truthy());
}

TEST_F(WindowSqlCov, CollateWindowOrderSortsCaseInsensitively) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT RANK() OVER (ORDER BY COLLATE(s, 'und:ci')) "
                "FROM wx WHERE g = 1 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  // apple < banana < Cherry < date < Egg case-insensitively.
  EXPECT_EQ(rows.Value()[0][0], Value(1));
  EXPECT_EQ(rows.Value()[1][0], Value(2));
  EXPECT_EQ(rows.Value()[2][0], Value(3));
  EXPECT_EQ(rows.Value()[3][0], Value(4));
  EXPECT_EQ(rows.Value()[4][0], Value(5));
}

TEST_F(WindowSqlCov, AggregateWindowSweepOverUnnest) {
  // One wide SELECT exercises most aggregate-over-frame implementations at
  // once on values {10, NULL, 20, 20, 30}.
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT COUNT(*) OVER (), COUNT(v) OVER (), COUNT(DISTINCT v) OVER (), "
      "COUNTIF(v > 15) OVER (), APPROX_COUNT_DISTINCT(v) OVER (), "
      "SUM(v) OVER (), AVG(v) OVER (), MIN(v) OVER (), MAX(v) OVER (), "
      "LOGICAL_AND(v > 0) OVER (), LOGICAL_OR(v > 25) OVER (), "
      "BIT_AND(v) OVER (), BIT_OR(v) OVER (), BIT_XOR(v) OVER (), "
      "ANY_VALUE(v) OVER (), VAR_POP(v) OVER (), STDDEV_SAMP(v) OVER () "
      "FROM UNNEST([10, NULL, 20, 20, 30]) AS v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(5));
  EXPECT_EQ(r[1], Value(4));
  EXPECT_EQ(r[2], Value(3));
  EXPECT_EQ(r[3], Value(3));
  EXPECT_EQ(r[4], Value(3));
  EXPECT_EQ(r[5], Value(80));
  EXPECT_DOUBLE_EQ(r[6].value.double_value, 20.0);
  EXPECT_EQ(r[7], Value(10));
  EXPECT_EQ(r[8], Value(30));
  EXPECT_TRUE(r[9].Truthy());
  EXPECT_TRUE(r[10].Truthy());
  EXPECT_EQ(r[11], Value(0));
  EXPECT_EQ(r[12], Value(30));
  EXPECT_EQ(r[13], Value(20));
  EXPECT_EQ(r[14], Value(10));
  EXPECT_DOUBLE_EQ(r[15].value.double_value, 50.0);
  EXPECT_DOUBLE_EQ(r[16].value.double_value, std::sqrt(200.0 / 3.0));
  for (size_t i = 1; i < rows.Value().size(); ++i) {
    EXPECT_EQ(rows.Value()[i], r);
  }
}

TEST_F(WindowSqlCov, WindowMinMaxPropagateNan) {
  const auto rows = TryRunSql(engine_.get(), context_.get(),
                              "SELECT MIN(f) OVER (), MAX(f) OVER () "
                              "FROM wx WHERE g = 1");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  for (const Row& row : rows.Value()) {
    ASSERT_EQ(row[0].type, ValueType::kDouble);
    ASSERT_EQ(row[1].type, ValueType::kDouble);
    EXPECT_TRUE(std::isnan(row[0].value.double_value));
    EXPECT_TRUE(std::isnan(row[1].value.double_value));
  }
}

TEST_F(WindowSqlCov, WindowArrayAggPreservesNullElements) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT ARRAY_AGG(v) OVER () FROM UNNEST([10, NULL, 20]) AS v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 3U);
  const Value& result = rows.Value()[0][0];
  ASSERT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArrayElements(),
            (std::vector<Value>({Value(10), Value(), Value(20)})));

  const auto all_null =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT ARRAY_AGG(v) OVER () FROM UNNEST([CAST(NULL AS INT64), "
                "CAST(NULL AS INT64)]) AS v");
  ASSERT_TRUE(all_null.HasValue()) << all_null.GetStatus().GetMessage();
  ASSERT_EQ(all_null.Value().size(), 2U);
  ASSERT_TRUE(all_null.Value()[0][0].IsArray());
  EXPECT_EQ(all_null.Value()[0][0].ArrayElements(),
            (std::vector<Value>({Value(), Value()})));
}

TEST_F(WindowSqlCov, WindowIgnoreNullsFiltersAnalyticValues) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT ARRAY_AGG(v IGNORE NULLS) OVER (), "
      "FIRST_VALUE(v IGNORE NULLS) OVER (ORDER BY ord ROWS BETWEEN "
      "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), "
      "LAST_VALUE(v IGNORE NULLS) OVER (ORDER BY ord ROWS BETWEEN "
      "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), "
      "NTH_VALUE(v, 2 IGNORE NULLS) OVER (ORDER BY ord ROWS BETWEEN "
      "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
      "FROM UNNEST([NULL, 10, 20]) AS v WITH OFFSET AS ord ORDER BY ord");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 3U);
  for (const Row& row : rows.Value()) {
    ASSERT_TRUE(row[0].IsArray());
    EXPECT_EQ(row[0].ArrayElements(),
              (std::vector<Value>({Value(10), Value(20)})));
    EXPECT_EQ(row[1], Value(10));
    EXPECT_EQ(row[2], Value(20));
    EXPECT_EQ(row[3], Value(20));
  }
}

TEST_F(WindowSqlCov, ApproxQuantilesKeepsDeclaredTextDomains) {
  constexpr const char* projections =
      "SELECT APPROX_QUANTILES(s, 2), APPROX_QUANTILES(t, 2) FROM ";
  constexpr const char* literal_source =
      "SELECT '2026-01-01 10:00:00+00' AS s, "
      "TIMESTAMP '2026-01-01 10:00:00+00' AS t";
  ASSERT_TRUE(TryRunSql(engine_.get(), context_.get(),
                        "CREATE TABLE quantile_types (s STRING, t TIMESTAMP)")
                  .HasValue());
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(),
                "INSERT INTO quantile_types " + std::string(literal_source))
          .HasValue());
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(),
                "CREATE TABLE quantile_ctas AS " + std::string(literal_source))
          .HasValue());
  for (const std::string& source :
       {std::string("(") + literal_source + ")", std::string("quantile_types"),
        std::string("quantile_ctas")}) {
    SCOPED_TRACE(source);
    const auto rows =
        TryRunSql(engine_.get(), context_.get(), projections + source);
    ASSERT_TRUE(rows.HasValue()) << rows.GetStatus();
    ASSERT_EQ(rows.Value().size(), 1U);
    const Row& row = rows.Value()[0];
    ASSERT_TRUE(row[0].IsArray());
    ASSERT_TRUE(row[1].IsArray());
    EXPECT_EQ(row[0].ArrayElementSqlType(), "STRING");
    EXPECT_EQ(row[1].ArrayElementSqlType(), "TIMESTAMP");
    EXPECT_EQ(row[0].ArrayElements(), row[1].ArrayElements());
  }
}

TEST_F(WindowSqlCov, ApproxTopWindowPayloadsMatchGroupedAggregates) {
  const std::string source = R"sql(
    FROM UNNEST([
      STRUCT('a"b\\c\n' AS x, 0.123456789 AS w),
      STRUCT('other' AS x, 0.0 AS w)
    ])
  )sql";
  auto grouped = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT APPROX_TOP_COUNT(w, 2), APPROX_TOP_SUM(x, w, 2) " + source);
  ASSERT_TRUE(grouped.HasValue()) << grouped.GetStatus();
  ASSERT_EQ(grouped.Value().size(), 1U);
  auto analytic = TryRunSql(engine_.get(), context_.get(),
                            "SELECT APPROX_TOP_COUNT(w, 2) OVER (), "
                            "APPROX_TOP_SUM(x, w, 2) OVER () " +
                                source);
  ASSERT_TRUE(analytic.HasValue()) << analytic.GetStatus();
  ASSERT_EQ(analytic.Value().size(), 2U);
  for (const Row& row : analytic.Value()) {
    ASSERT_TRUE(row[0].IsArray());
    ASSERT_TRUE(row[1].IsArray());
    EXPECT_EQ(row[0].ArrayElements(), grouped.Value()[0][0].ArrayElements());
    EXPECT_EQ(row[1].ArrayElements(), grouped.Value()[0][1].ArrayElements());
    EXPECT_EQ(row[1].ArrayElements()[0].AsStringView(),
              R"json({"value":"a\"b\\c\n","sum":0.123456789})json");
    EXPECT_EQ(row[1].ArrayElementSqlType(),
              "STRUCT<value STRING, sum FLOAT64>");
  }
}

TEST_F(WindowSqlCov, StringAggWindowUsesFirstOrderedDelimiter) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT STRING_AGG(x, d) OVER (), "
                "STRING_AGG(x, d ORDER BY x DESC) OVER () "
                "FROM UNNEST([STRUCT('a' AS x, '|' AS d), "
                "STRUCT('b' AS x, ',' AS d), STRUCT('c' AS x, '/' AS d)])");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus();
  ASSERT_EQ(rows.Value().size(), 3U);
  for (const Row& row : rows.Value()) {
    EXPECT_EQ(row[0], Value("a|b|c"));
    EXPECT_EQ(row[1], Value("c/b/a"));
  }
}

TEST_F(WindowSqlCov, ApproxTopGroupedAggregatesRejectWrongArityOnEmptyInput) {
  for (const char* call :
       {"APPROX_TOP_COUNT(v)", "APPROX_TOP_COUNT(v, 1, 2)",
        "APPROX_TOP_SUM(v, 1)", "APPROX_TOP_SUM(v, 1, 2, 3)"}) {
    const auto rows = TryRunSql(
        engine_.get(), context_.get(),
        std::string("SELECT ") + call + " FROM UNNEST([1]) AS v WHERE FALSE");
    EXPECT_FALSE(rows.HasValue()) << call;
    EXPECT_NE(rows.GetStatus().GetMessage().find("aggregate arity"),
              std::string::npos)
        << call;
  }
}

TEST_F(WindowSqlCov, ArrayAndSketchWindowAggregates) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT ARRAY_AGG(v) OVER (), ARRAY_AGG(v LIMIT 3) OVER (), "
      "ARRAY_CONCAT_AGG([v]) OVER (), "
      "STRING_AGG(CAST(v AS STRING), '|') OVER (), "
      "APPROX_QUANTILES(v, 2) OVER (), APPROX_TOP_COUNT(v, 2) OVER (), "
      "APPROX_TOP_SUM(v, 1, 1) OVER () "
      "FROM UNNEST([10, 20, 20, 30]) AS v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);
  const Row& r = rows.Value()[0];
  ASSERT_TRUE(r[0].IsArray());
  EXPECT_EQ(r[0].ArrayElements(),
            (std::vector<Value>({Value(10), Value(20), Value(20), Value(30)})));
  ASSERT_TRUE(r[1].IsArray());
  EXPECT_EQ(r[1].ArrayElements(),
            (std::vector<Value>({Value(10), Value(20), Value(20)})));
  ASSERT_TRUE(r[2].IsArray());
  EXPECT_EQ(r[2].ArrayElements(),
            (std::vector<Value>({Value(10), Value(20), Value(20), Value(30)})));
  EXPECT_EQ(r[3], Value("10|20|20|30"));
  ASSERT_TRUE(r[4].IsArray());
  EXPECT_EQ(r[4].ArrayElements(),
            (std::vector<Value>({Value(10), Value(20), Value(30)})));
  // Top-2 by count: 20 (count 2) leads; the tie for second is unspecified.
  ASSERT_TRUE(r[5].IsArray());
  EXPECT_NE(r[5].ArrayElements()[0].AsStringView().find("\"value\":20"),
            std::string::npos);
  EXPECT_NE(r[5].ArrayElements()[0].AsStringView().find("\"count\":2"),
            std::string::npos);
  ASSERT_TRUE(r[6].IsArray());
  EXPECT_EQ(r[6].ArrayElements().size(), 1U);
  EXPECT_NE(r[6].ArrayElements()[0].AsStringView().find("\"value\":20"),
            std::string::npos);
  EXPECT_NE(r[6].ArrayElements()[0].AsStringView().find("\"sum\":2"),
            std::string::npos);
}

TEST_F(WindowSqlCov, InnerLimitAppliesAfterDistinctAndNullFilter) {
  // LIMIT counts emitted elements, not raw inputs: DISTINCT / IGNORE NULLS
  // removal happens before the cut (GoogleSQL aggregation input pipeline).
  const auto distinct =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT ARRAY_AGG(DISTINCT v ORDER BY v LIMIT 2) "
                "FROM UNNEST([1, 1, 2, 3]) AS v");
  ASSERT_TRUE(distinct.HasValue()) << distinct.GetStatus().GetMessage();
  ASSERT_EQ(distinct.Value().size(), 1U);
  EXPECT_EQ(distinct.Value()[0][0].ArrayElements(),
            (std::vector<Value>({Value(1), Value(2)})));

  const auto ignore_nulls =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT ARRAY_AGG(v IGNORE NULLS ORDER BY v LIMIT 2) "
                "FROM UNNEST([1, CAST(NULL AS INT64), 2, 3]) AS v");
  ASSERT_TRUE(ignore_nulls.HasValue()) << ignore_nulls.GetStatus().GetMessage();
  ASSERT_EQ(ignore_nulls.Value().size(), 1U);
  EXPECT_EQ(ignore_nulls.Value()[0][0].ArrayElements(),
            (std::vector<Value>({Value(1), Value(2)})));
}

TEST_F(WindowSqlCov, WindowAggregatesApplyInnerOrderByBeforeLimit) {
  // BUG-029/P148: inner ORDER BY must sort the aggregated inputs before the
  // LIMIT cut; ties keep frame order and NULL keys use the default placement.
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT ARRAY_AGG(v ORDER BY v DESC LIMIT 2) OVER (PARTITION BY g), "
      "ARRAY_AGG(v ORDER BY k) OVER (PARTITION BY g), "
      "STRING_AGG(s ORDER BY s DESC) OVER (PARTITION BY g) "
      "FROM wx ORDER BY g, v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 6U);
  for (size_t i = 0; i < 5; ++i) {
    const Row& row = rows.Value()[i];
    ASSERT_TRUE(row[0].IsArray());
    // LIMIT 2 picks the two largest v, not the first two frame rows.
    EXPECT_EQ(row[0].ArrayElements(),
              (std::vector<Value>({Value(50), Value(40)})));
    ASSERT_TRUE(row[1].IsArray());
    // NULL k sorts first (ASC default); the k=1 tie keeps frame order 10, 20.
    EXPECT_EQ(row[1].ArrayElements(),
              (std::vector<Value>(
                  {Value(40), Value(10), Value(20), Value(30), Value(50)})));
    EXPECT_EQ(row[2], Value("date,banana,Egg,Cherry,Apple"));
  }
  const Row& last = rows.Value()[5];
  ASSERT_TRUE(last[0].IsArray());
  EXPECT_EQ(last[0].ArrayElements(), (std::vector<Value>({Value(60)})));
  ASSERT_TRUE(last[1].IsArray());
  EXPECT_EQ(last[1].ArrayElements(), (std::vector<Value>({Value(60)})));
  EXPECT_EQ(last[2], Value("Fig"));
}

// P669: GROUP BY + window used to run the statement's LIMIT/OFFSET twice —
// FinishQuery applies LimitedRows at its tail and ExecuteWindowOverAggregate
// sliced again with the same statement — so OFFSET rows silently vanished
// (LIMIT 10 OFFSET 5 returned 5 rows).  The window must additionally see the
// full grouped input even though ORDER BY + LIMIT lets the sort pre-truncate.
TEST_F(WindowSqlCov, GroupByWindowLimitOffsetAppliesExactlyOnce) {
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(), "CREATE TABLE wg (g INT64);")
          .HasValue());
  std::string insert = "INSERT INTO wg VALUES ";
  for (int64_t g = 1; g <= 22; ++g) {
    if (g > 1) {
      insert += ", ";
    }
    insert += "(" + std::to_string(g) + ")";
  }
  ASSERT_TRUE(TryRunSql(engine_.get(), context_.get(), insert).HasValue());

  // Restore the caller's TINYLAMB_EXEC_ENGINE on scope exit instead of
  // unconditionally unsetting it.
  EnvGuard engine_env("TINYLAMB_EXEC_ENGINE");
  for (const char* mode : {static_cast<const char*>("legacy"), "pipeline"}) {
    ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", mode, 1), 0);
    const auto rows = TryRunSql(
        engine_.get(), context_.get(),
        "SELECT g, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY g) AS rn, "
        "SUM(COUNT(*)) OVER () AS total "
        "FROM wg GROUP BY g ORDER BY g LIMIT 10 OFFSET 5;");
    engine_env.Restore();
    ASSERT_TRUE(rows.HasValue())
        << mode << ": " << rows.GetStatus().GetMessage();
    ASSERT_EQ(rows.Value().size(), 10U) << "engine " << mode;
    for (size_t i = 0; i < 10; ++i) {
      const Row& row = rows.Value()[i];
      const int64_t g = 6 + static_cast<int64_t>(i);
      EXPECT_EQ(row[0], Value(g)) << "engine " << mode << " row " << i;
      EXPECT_EQ(row[1], Value(1)) << "engine " << mode << " row " << i;
      EXPECT_EQ(row[2], Value(g)) << "engine " << mode << " row " << i;
      EXPECT_EQ(row[3], Value(22)) << "engine " << mode << " row " << i;
    }
  }
}

TEST_F(WindowSqlCov, SpilledOocRangeFramesWithNullAndNaNKeysFollowAst) {
  // Bug 726: the OOC RangeFrameScanner resolves offset bounds with
  // sequential readers and the deque consumer retires rows at a monotone
  // floor.  NULL / NaN order keys collapse their offset frames to their own
  // peer group, which sorts at the far end of the partition, so those frames
  // restart below positions that earlier finite or empty frames had already
  // retired -- the spill lane used to answer NULL for them.  The AST
  // (legacy engine) is the ground truth.
  ASSERT_TRUE(TryRunSql(engine_.get(), context_.get(),
                        "CREATE TABLE wn (g INT64, k FLOAT64, v INT64);")
                  .HasValue());
  ASSERT_TRUE(
      TryRunSql(
          engine_.get(), context_.get(),
          "INSERT INTO wn VALUES "
          "(1, CAST('NaN' AS FLOAT64), 1), (1, CAST('NaN' AS FLOAT64), 2), "
          "(1, 10.0, 10), (1, 20.0, 20), (1, 30.0, 30), "
          "(2, NULL, 1), (2, NULL, 2), (2, 10.0, 10), (2, 20.0, 20), "
          "(2, 30.0, 30);")
          .HasValue());

  struct Case {
    const char* sql;
    std::vector<Row> expected;
  };
  const std::array<Case, 7> cases = {{
      {.sql =
           "SELECT v, SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 15 FOLLOWING "
           "AND 25 FOLLOWING) FROM wn WHERE g = 1 ORDER BY k DESC;",
       .expected = {{Value(30), Value(10)},
                    {Value(20), Value()},
                    {Value(10), Value()},
                    {Value(1), Value(3)},
                    {Value(2), Value(3)}}},
      {.sql = "SELECT v, SUM(v) OVER (ORDER BY k RANGE BETWEEN 15 FOLLOWING "
              "AND 25 FOLLOWING) FROM wn WHERE g = 1 ORDER BY k;",
       .expected = {{Value(1), Value(3)},
                    {Value(2), Value(3)},
                    {Value(10), Value(30)},
                    {Value(20), Value()},
                    {Value(30), Value()}}},
      {.sql =
           "SELECT v, SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 100 FOLLOWING "
           "AND 200 FOLLOWING) FROM wn WHERE g = 2 ORDER BY k DESC;",
       .expected = {{Value(30), Value()},
                    {Value(20), Value()},
                    {Value(10), Value()},
                    {Value(1), Value(3)},
                    {Value(2), Value(3)}}},
      {.sql =
           "SELECT v, MIN(v) OVER (ORDER BY k DESC RANGE BETWEEN 100 FOLLOWING "
           "AND 200 FOLLOWING) FROM wn WHERE g = 2 ORDER BY k DESC;",
       .expected = {{Value(30), Value()},
                    {Value(20), Value()},
                    {Value(10), Value()},
                    {Value(1), Value(1)},
                    {Value(2), Value(1)}}},
      {.sql = "SELECT v, COUNT(*) OVER (ORDER BY k DESC RANGE BETWEEN 100 "
              "FOLLOWING "
              "AND 200 FOLLOWING) FROM wn WHERE g = 2 ORDER BY k DESC;",
       .expected = {{Value(30), Value(0)},
                    {Value(20), Value(0)},
                    {Value(10), Value(0)},
                    {Value(1), Value(2)},
                    {Value(2), Value(2)}}},
      {.sql =
           "SELECT v, SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 15 PRECEDING "
           "AND 15 PRECEDING) FROM wn WHERE g = 1 ORDER BY k DESC;",
       .expected = {{Value(30), Value()},
                    {Value(20), Value()},
                    {Value(10), Value()},
                    {Value(1), Value()},
                    {Value(2), Value()}}},
      {.sql = "SELECT v, SUM(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED "
              "PRECEDING "
              "AND 15 FOLLOWING) FROM wn WHERE g = 1 ORDER BY k;",
       .expected = {{Value(1), Value(3)},
                    {Value(2), Value(3)},
                    {Value(10), Value(33)},
                    {Value(20), Value(63)},
                    {Value(30), Value(63)}}},
  }};

  // Restore the caller's TINYLAMB_EXEC_ENGINE and memory-budget knobs on
  // scope exit instead of unconditionally resetting them.  The knob guard
  // captures the pre-test values (not ResetTestKnobs defaults) and restores
  // them both per spill lane and on early ASSERT return.
  EnvGuard engine_env("TINYLAMB_EXEC_ENGINE");
  auto& budget = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) RAII knob guard
  struct SpillKnobGuard {
    SpillKnobGuard()
        : always_spill(ProcessMemoryBudget::Global().AlwaysSpillEnabled()),
          op_bytes(ProcessMemoryBudget::Global().OpMemoryOverrideBytes()) {}
    SpillKnobGuard(const SpillKnobGuard&) = delete;
    SpillKnobGuard& operator=(const SpillKnobGuard&) = delete;
    ~SpillKnobGuard() { Restore(); }
    void Restore() const {
      auto& b = ProcessMemoryBudget::Global();
      b.SetTestAlwaysSpill(always_spill);
      b.SetTestOpMemoryBytes(op_bytes);
    }
    bool always_spill;
    size_t op_bytes;
  } spill_knobs;

  for (const auto& c : cases) {
    // The AST ground truth itself.
    ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "legacy", 1),
              0);
    const auto legacy = TryRunSql(engine_.get(), context_.get(), c.sql);
    engine_env.Restore();
    ASSERT_TRUE(legacy.HasValue()) << legacy.GetStatus().GetMessage();
    EXPECT_EQ(legacy.Value(), c.expected) << c.sql;

    // In-memory pipeline lane and the forced-spill (OOC RangeFrameScanner)
    // lane, both pinned to the pipeline engine regardless of the caller's
    // TINYLAMB_EXEC_ENGINE.
    ASSERT_EQ(
        ::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "pipeline", 1), 0);
    const auto normal = TryRunSql(engine_.get(), context_.get(), c.sql);
    ASSERT_TRUE(normal.HasValue()) << normal.GetStatus().GetMessage();
    EXPECT_EQ(normal.Value(), c.expected) << c.sql;

    budget.SetTestAlwaysSpill(true);
    budget.SetTestOpMemoryBytes(4096);
    const auto spilled = TryRunSql(engine_.get(), context_.get(), c.sql);
    spill_knobs.Restore();
    engine_env.Restore();
    ASSERT_TRUE(spilled.HasValue()) << spilled.GetStatus().GetMessage();
    EXPECT_EQ(spilled.Value(), c.expected) << "spill lane: " << c.sql;
  }
}

TEST_F(WindowSqlCov, SpilledSlidingSumsForgetDepartedUnsignedValues) {
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(), "CREATE TABLE wu (k INT64)")
          .HasValue());
  ASSERT_TRUE(TryRunSql(engine_.get(), context_.get(),
                        "INSERT INTO wu SELECT k "
                        "FROM UNNEST(GENERATE_ARRAY(1, 200)) AS k")
                  .HasValue());
  const std::string input =
      "CASE WHEN k = 2 THEN CAST('9223372036854775808' AS UINT64) "
      "ELSE CAST(k AS FLOAT64) + 0.5 END";
  const auto types = TryRunSql(engine_.get(), context_.get(),
                               "SELECT " + input + " FROM wu ORDER BY k");
  ASSERT_TRUE(types.HasValue()) << types.GetStatus();
  ASSERT_EQ(types.Value().size(), 200U);
  ASSERT_EQ(types.Value()[1][0].type, ValueType::kDouble);
  ASSERT_EQ(types.Value()[0][0].type, ValueType::kDouble);

  EnvGuard engine_env("TINYLAMB_EXEC_ENGINE");
  ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "pipeline", 1),
            0);
  auto& budget = ProcessMemoryBudget::Global();
  // RAII, not a manual restore: an early ASSERT before the end of this test
  // must not leave the process-global budget in always-spill/4096 mode and
  // silently push later tests in the same binary onto the spill path.
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) RAII knob guard
  struct SpillKnobGuard {
    SpillKnobGuard()
        : always_spill(ProcessMemoryBudget::Global().AlwaysSpillEnabled()),
          op_bytes(ProcessMemoryBudget::Global().OpMemoryOverrideBytes()) {}
    SpillKnobGuard(const SpillKnobGuard&) = delete;
    SpillKnobGuard& operator=(const SpillKnobGuard&) = delete;
    ~SpillKnobGuard() {
      auto& b = ProcessMemoryBudget::Global();
      b.SetTestAlwaysSpill(always_spill);
      b.SetTestOpMemoryBytes(op_bytes);
    }
    bool always_spill;
    size_t op_bytes;
  } spill_knobs;
  budget.SetTestAlwaysSpill(true);
  budget.SetTestOpMemoryBytes(4096);
  const std::string frame =
      ") OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)";
  const auto rows = TryRunSql(engine_.get(), context_.get(),
                              "SELECT SUM(" + input + frame + ", AVG(" + input +
                                  frame + " FROM wu ORDER BY k");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus();
  constexpr double big = 9223372036854775808.0;
  ASSERT_EQ(rows.Value().size(), 200U);
  EXPECT_EQ(rows.Value()[0], (Row{Value(1.5), Value(1.5)}));
  EXPECT_EQ(rows.Value()[1], (Row{Value(big), Value(big / 2)}));
  EXPECT_EQ(rows.Value()[2], (Row{Value(big), Value(big / 2)}));
  for (size_t i = 3; i < rows.Value().size(); ++i) {
    const auto key = static_cast<double>(i + 1);
    EXPECT_EQ(rows.Value()[i], (Row{Value(2 * key), Value(key)}));
  }
}

TEST_F(WindowSqlCov, ElementwiseWindowAggregatesOverArrays) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT ELEMENTWISE_SUM([v, NULL]) OVER (), ELEMENTWISE_AVG([v]) OVER "
      "(), ELEMENTWISE_SUM([CAST(v AS FLOAT64), v]) OVER () "
      "FROM UNNEST([10, 20, 30]) AS v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 3U);
  const Row& r = rows.Value()[0];
  ASSERT_TRUE(r[0].IsArray());
  EXPECT_EQ(r[0].ArrayElements().size(), 2U);
  EXPECT_EQ(r[0].ArrayElements()[0], Value(60));
  EXPECT_TRUE(r[0].ArrayElements()[1].IsNull());
  ASSERT_TRUE(r[1].IsArray());
  EXPECT_EQ(r[1].ArrayElements().size(), 1U);
  EXPECT_DOUBLE_EQ(r[1].ArrayElements()[0].value.double_value, 20.0);
  ASSERT_TRUE(r[2].IsArray());
  EXPECT_EQ(r[2].ArrayElements().size(), 2U);
  EXPECT_DOUBLE_EQ(r[2].ArrayElements()[0].value.double_value, 60.0);
  EXPECT_EQ(r[2].ArrayElements()[1], Value(60));
}

TEST_F(WindowSqlCov, WindowAggregatesOverDatesKeepElementTypes) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT ARRAY_AGG(d) OVER (), ARRAY_CONCAT_AGG([d]) OVER () "
                "FROM wx WHERE g = 2");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 1U);
  EXPECT_TRUE(rows.Value()[0][0].IsArray());
  EXPECT_EQ(rows.Value()[0][0].ArrayElementSqlType(), "DATE");
  EXPECT_TRUE(rows.Value()[0][1].IsArray());
  EXPECT_EQ(rows.Value()[0][1].ArrayElementSqlType(), "DATE");
}

TEST_F(WindowSqlCov, WindowAggregateDegenerateInputs) {
  // All-NULL frames aggregate to NULL.
  const auto nulls = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (), AVG(v) OVER (), COUNT(v) OVER (), "
      "COUNT(*) OVER () FROM UNNEST([CAST(NULL AS INT64), CAST(NULL AS "
      "INT64)]) AS v");
  ASSERT_TRUE(nulls.HasValue()) << nulls.GetStatus().GetMessage();
  ASSERT_EQ(nulls.Value().size(), 2U);
  EXPECT_TRUE(nulls.Value()[0][0].IsNull());
  EXPECT_TRUE(nulls.Value()[0][1].IsNull());
  EXPECT_EQ(nulls.Value()[0][2], Value(0));
  EXPECT_EQ(nulls.Value()[0][3], Value(2));

  // DISTINCT dedupes the frame.
  const auto distinct =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(DISTINCT v) OVER (), COUNT(DISTINCT v) OVER () "
                "FROM UNNEST([10, 10, 20]) AS v");
  ASSERT_TRUE(distinct.HasValue()) << distinct.GetStatus().GetMessage();
  ASSERT_EQ(distinct.Value().size(), 3U);
  EXPECT_EQ(distinct.Value()[0][0], Value(30));
  EXPECT_EQ(distinct.Value()[0][1], Value(2));

  // int64 overflow raises instead of wrapping.
  const auto overflow = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER () FROM UNNEST([9223372036854775807, 1]) AS v");
  ASSERT_FALSE(overflow.HasValue());
  EXPECT_NE(overflow.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << overflow.GetStatus().GetMessage();
}

TEST_F(WindowSqlCov, PercentileWindowVariants) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT PERCENTILE_CONT(v, 0) OVER (), PERCENTILE_CONT(v, 1) OVER (), "
      "PERCENTILE_DISC(v, 0) OVER (), PERCENTILE_CONT(v, NULL) OVER () "
      "FROM UNNEST([10, 20, 30]) AS v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 3U);
  const Row& r = rows.Value()[0];
  EXPECT_DOUBLE_EQ(r[0].value.double_value, 10.0);
  EXPECT_DOUBLE_EQ(r[1].value.double_value, 30.0);
  EXPECT_EQ(r[2], Value(10));
  EXPECT_TRUE(r[3].IsNull());

  // Out-of-range percentile raises, including NaN (an IEEE comparison with
  // the [0,1] bounds would let it through to an out-of-range index).
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT PERCENTILE_CONT(v, 1.5) OVER () "
                         "FROM UNNEST([10, 20]) AS v")
                   .HasValue());
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT PERCENTILE_CONT(v, CAST('nan' AS FLOAT64)) "
                         "OVER () FROM UNNEST([10, 20]) AS v")
                   .HasValue());
}

TEST_F(WindowSqlCov, WindowErrorDiagnostics) {
  const std::string from = " FROM UNNEST([1, 2, 3]) AS v";
  // Each case pairs the bad SQL with the diagnostic category fragment its
  // error must carry, so a silently-wrong error path cannot pass.
  struct Case {
    std::string sql;
    std::string fragment;
  };
  const std::vector<Case> bad = {
      {"SELECT NTILE(NULL) OVER (ORDER BY v)" + from,
       "NTILE function must not be NULL"},
      {"SELECT NTILE(0) OVER (ORDER BY v)" + from,
       "NTILE function must be positive"},
      {"SELECT NTILE(1.5) OVER (ORDER BY v)" + from,
       "NTILE requires an integer bucket count"},
      {"SELECT LAG(v, NULL) OVER (ORDER BY v)" + from, "LAG must not be NULL"},
      {"SELECT LAG(v, 1.5) OVER (ORDER BY v)" + from, "LAG must be an integer"},
      {"SELECT LAG(v, -1) OVER (ORDER BY v)" + from,
       "LAG must not be negative"},
      {"SELECT LEAD(v, NULL) OVER (ORDER BY v)" + from,
       "LEAD must not be NULL"},
      {"SELECT NTH_VALUE(v, NULL) OVER (ORDER BY v)" + from,
       "NthValue function must not be NULL"},
      {"SELECT NTH_VALUE(v, 1.5) OVER (ORDER BY v)" + from,
       "NthValue function must be an integer"},
      {"SELECT NTH_VALUE(v, 0) OVER (ORDER BY v)" + from,
       "NthValue function must be positive"},
      // RANGE offsets need exactly one ORDER BY key.
      {"SELECT SUM(v) OVER (ORDER BY g, v RANGE BETWEEN 1 PRECEDING AND "
       "CURRENT ROW)" +
           from,
       "RANGE offset requires one constant key"},
      {"SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN NULL PRECEDING AND "
       "CURRENT ROW)" +
           from,
       "cannot be NULL"},
      {"SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN 'x' PRECEDING AND "
       "CURRENT ROW)" +
           from,
       "RANGE offset must be numeric"},
      {"SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN NULL PRECEDING AND "
       "CURRENT ROW)" +
           from,
       "cannot be NULL"},
      {"SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN 1.5 PRECEDING AND "
       "CURRENT ROW)" +
           from,
       "ROWS offset must be a constant integer"},
  };
  for (const Case& c : bad) {
    const auto result = TryRunSql(engine_.get(), context_.get(), c.sql);
    ASSERT_FALSE(result.HasValue()) << c.sql;
    EXPECT_NE(result.GetStatus().GetMessage().find(c.fragment),
              std::string::npos)
        << c.sql << " -> " << result.GetStatus().GetMessage();
  }
}

TEST_F(WindowSqlCov, LeadLagWithOffsetsAndDefaults) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT LAG(v) OVER (ORDER BY v), LEAD(v, 2, 0) OVER (ORDER BY v), "
      "LAG(v, 2, -1) OVER (ORDER BY v) "
      "FROM wx WHERE g = 2 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 1U);
  EXPECT_EQ(rows.Value()[0], (Row({Value(), Value(0), Value(-1)})));
}

TEST_F(WindowSqlCov, QualifyWithoutWindowFunctionsFiltersRows) {
  const auto rows = TryRunSql(engine_.get(), context_.get(),
                              "SELECT g, v FROM wx QUALIFY v >= 30 ORDER BY v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);
  EXPECT_EQ(rows.Value()[0][1], Value(30));
  EXPECT_EQ(rows.Value()[3][1], Value(60));
}

TEST_F(WindowSqlCov, QualifyInlinesSelectAliases) {
  const auto binary =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT v * 2 AS vv FROM wx QUALIFY vv >= 60 ORDER BY v");
  ASSERT_TRUE(binary.HasValue()) << binary.GetStatus().GetMessage();
  ASSERT_EQ(binary.Value().size(), 4U);

  const auto case_expr = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT v AS c FROM wx QUALIFY CASE WHEN c >= 30 THEN true ELSE false "
      "END ORDER BY c");
  ASSERT_TRUE(case_expr.HasValue()) << case_expr.GetStatus().GetMessage();
  ASSERT_EQ(case_expr.Value().size(), 4U);

  const auto in_expr =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT v AS c FROM wx QUALIFY c IN (10, 40) ORDER BY c");
  ASSERT_TRUE(in_expr.HasValue()) << in_expr.GetStatus().GetMessage();
  ASSERT_EQ(in_expr.Value().size(), 2U);

  const auto unary =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT v AS c FROM wx QUALIFY NOT c > 10 ORDER BY c");
  ASSERT_TRUE(unary.HasValue()) << unary.GetStatus().GetMessage();
  ASSERT_EQ(unary.Value().size(), 1U);
  EXPECT_EQ(unary.Value()[0][0], Value(10));

  const auto func =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT v AS c FROM wx QUALIFY ABS(c - 40) = 0 ORDER BY c");
  ASSERT_TRUE(func.HasValue()) << func.GetStatus().GetMessage();
  ASSERT_EQ(func.Value().size(), 1U);
  EXPECT_EQ(func.Value()[0][0], Value(40));
}

TEST_F(WindowSqlCov, QualifyReferencingWindowResult) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g FROM wx QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY "
      "v) = 1 ORDER BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 2U);
  EXPECT_EQ(rows.Value()[0][0], Value(1));
  EXPECT_EQ(rows.Value()[1][0], Value(2));
}

// ---------------------------------------------------------------------------
// window_eval internals the SQL frontend cannot spell: GROUPS frames, every
// EXCLUDE mode, FILTER clauses, and the frame diagnostics (asserted through
// the Status channel so the coverage build records the error paths).
// ---------------------------------------------------------------------------

class DirectWinCov : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("window_direct_cov-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Applies one hand-built window function over a single INT64 column "v".
  static StatusOr<std::vector<Value>> Apply(
      TransactionContext& context,
      const std::shared_ptr<WindowFunctionCallExpression>& window,
      const std::vector<Value>& values = {Value(10), Value(10), Value(20),
                                          Value(30)}) {
    SelectStatement statement({NamedExpression("w", Expression(window))}, {},
                              nullptr);
    relational_detail::Relation input;
    input.schema = Schema("", {Column("v", ValueType::kInt64)});
    std::vector<Row> rows;
    rows.reserve(values.size());
    for (const Value& value : values) {
      rows.emplace_back(Row({value}));
    }
    input.rows = std::move(rows);
    auto windowed = relational_detail::ApplyWindows(
        context, statement, std::move(input), nullptr, {});
    if (!windowed.HasValue()) {
      return windowed.GetStatus();
    }
    std::vector<Value> out;
    EXPECT_EQ(tinylamb::Status::kSuccess,
              windowed.Value().input.ForEachRow(
                  [&](const Row& row) { out.push_back(row[1]); }));
    return out;
  }

  static StatusOr<std::vector<Value>> ApplyOverStrings(
      TransactionContext& context,
      const std::shared_ptr<WindowFunctionCallExpression>& window,
      const std::vector<Value>& values, bool columnar = false) {
    SelectStatement statement({NamedExpression("w", Expression(window))}, {},
                              nullptr);
    relational_detail::Relation input;
    input.schema = Schema("", {Column("v", ValueType::kVarChar)});
    std::vector<Row> rows;
    rows.reserve(values.size());
    for (const Value& value : values) {
      rows.emplace_back(Row({value}));
    }
    if (columnar) {
      auto table =
          relational_detail::BuildColumnarTableFromRows(input.schema, rows);
      if (!table.HasValue()) {
        return table.GetStatus();
      }
      input.SetColumnarTable(table.MoveValue(), true);
    } else {
      input.rows = std::move(rows);
    }
    auto windowed = relational_detail::ApplyWindows(
        context, statement, std::move(input), nullptr, {});
    if (!windowed.HasValue()) {
      return windowed.GetStatus();
    }
    std::vector<Value> out;
    EXPECT_EQ(tinylamb::Status::kSuccess,
              windowed.Value().input.ForEachRow(
                  [&](const Row& row) { out.push_back(row[1]); }));
    return out;
  }

  // Expect an error whose message contains `fragment`.
  static void ExpectError(
      TransactionContext& context,
      const std::shared_ptr<WindowFunctionCallExpression>& window,
      const std::string& fragment,
      const std::vector<Value>& values = {Value(10), Value(20)}) {
    auto result = Apply(context, window, values);
    ASSERT_FALSE(result.HasValue());
    EXPECT_NE(result.GetStatus().GetMessage().find(fragment), std::string::npos)
        << result.GetStatus().GetMessage();
  }

  // SUM(v) OVER (ORDER BY v <frame>) with caller-chosen bounds.
  static std::shared_ptr<WindowFunctionCallExpression> SumWithFrame(
      WindowFrameUnit unit, WindowFrameBound start, WindowFrameBound end) {
    auto window = MakeWindow("SUM", {ColumnValueExp("v")});
    window->order_by = {TermOnV(true)};
    window->frame_unit = unit;
    window->has_frame = true;
    window->frame_start = std::move(start);
    window->frame_end = std::move(end);
    return window;
  }

  static std::shared_ptr<WindowFunctionCallExpression> FullFrameSum(
      WindowFrameExclusion exclusion, bool with_order_by = true) {
    auto window = MakeWindow("SUM", {ColumnValueExp("v")});
    if (with_order_by) {
      window->order_by = {TermOnV(true)};
    }
    window->frame_unit = WindowFrameUnit::kRows;
    window->has_frame = true;
    window->frame_start = Bound(WindowFrameBoundType::kUnboundedPreceding);
    window->frame_end = Bound(WindowFrameBoundType::kUnboundedFollowing);
    window->exclusion = exclusion;
    return window;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(DirectWinCov, WindowStringOrderUsesEnumRanks) {
  auto rank = MakeWindow("RANK", {});
  const std::vector<Value> values{Value("TESTENUM!"), Value("TESTENUM0"),
                                  Value("TESTENUM!"), Value("TESTENUMNEGATIVE"),
                                  Value()};
  for (bool columnar : {false, true}) {
    SCOPED_TRACE(columnar);
    rank->order_by = {TermOnV(true)};
    auto asc = ApplyOverStrings(*context_, rank, values, columnar);
    ASSERT_TRUE(asc.HasValue()) << asc.GetStatus();
    EXPECT_EQ(asc.Value(), (std::vector<Value>{Value(4), Value(3), Value(4),
                                               Value(2), Value(1)}));
    rank->order_by = {TermOnV(false)};
    auto desc = ApplyOverStrings(*context_, rank, values, columnar);
    ASSERT_TRUE(desc.HasValue()) << desc.GetStatus();
    EXPECT_EQ(desc.Value(), (std::vector<Value>{Value(1), Value(3), Value(1),
                                                Value(4), Value(5)}));
  }
}

TEST_F(DirectWinCov, GroupsFramesWithEveryBoundKind) {
  // Peer groups of {10, 10}, {20}, {30}.
  struct Case {
    std::string name;
    WindowFrameBound start;
    WindowFrameBound end;
    std::vector<Value> expected;
  };
  const std::array<Case, 6> cases = {{
      {.name = "current..following",
       .start = Bound(WindowFrameBoundType::kCurrentRow),
       .end = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 1),
       .expected = {Value(40), Value(40), Value(50), Value(30)}},
      {.name = "following..unbounded",
       .start = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 1),
       .end = Bound(WindowFrameBoundType::kUnboundedFollowing),
       // The last peer group's 1-FOLLOWING start is past the partition
       // end: empty frame (NULL), not a clamp onto the last group.
       .expected = {Value(50), Value(50), Value(30), Value()}},
      {.name = "unbounded..unbounded",
       .start = Bound(WindowFrameBoundType::kUnboundedFollowing),
       .end = Bound(WindowFrameBoundType::kUnboundedFollowing),
       .expected = {Value(30), Value(30), Value(30), Value(30)}},
      {.name = "unbounded_prec..unbounded_prec",
       .start = Bound(WindowFrameBoundType::kUnboundedPreceding),
       .end = Bound(WindowFrameBoundType::kUnboundedPreceding),
       .expected = {Value(10), Value(10), Value(10), Value(10)}},
      // A following-start past the last group empties the frame for every
      // row (PostgreSQL/DuckDB return NULL), matching ROWS/RANGE.
      {.name = "following-past-end..unbounded",
       .start = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 5),
       .end = Bound(WindowFrameBoundType::kUnboundedFollowing),
       .expected = {Value(), Value(), Value(), Value()}},
      // Preceding-end below a following-start empties the frame.
      {.name = "following..preceding",
       .start = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 1),
       .end = OffsetBound(WindowFrameBoundType::kOffsetPreceding, 1),
       .expected = {Value(), Value(), Value(), Value()}},
  }};
  for (const Case& c : cases) {
    auto result = Apply(*context_,
                        SumWithFrame(WindowFrameUnit::kGroups, c.start, c.end));
    ASSERT_TRUE(result.HasValue())
        << c.name << ": " << result.GetStatus().GetMessage();
    EXPECT_EQ(result.Value(), c.expected) << c.name;
  }
}

TEST_F(DirectWinCov, GroupsFrameDiagnosticsSurfaceAsStatus) {
  auto no_order = SumWithFrame(WindowFrameUnit::kGroups,
                               Bound(WindowFrameBoundType::kUnboundedPreceding),
                               Bound(WindowFrameBoundType::kCurrentRow));
  no_order->order_by.clear();
  ExpectError(*context_, no_order, "GROUPS frame requires an ORDER BY key");

  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kGroups,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ColumnValueExp("v")),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "GROUPS offset requires a constant value");

  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kGroups,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ConstantValueExp(Value())),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "cannot be NULL");

  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kGroups,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ConstantValueExp(Value("x"))),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "GROUPS offset must be numeric");

  ExpectError(
      *context_,
      SumWithFrame(WindowFrameUnit::kGroups,
                   Bound(WindowFrameBoundType::kOffsetPreceding,
                         ConstantValueExp(
                             Value(std::numeric_limits<double>::quiet_NaN()))),
                   Bound(WindowFrameBoundType::kCurrentRow)),
      "cannot be NaN");

  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kGroups,
                           OffsetBoundDouble(
                               WindowFrameBoundType::kOffsetPreceding, -1.5),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "must be non-negative");

  for (double offset : {1.9, std::numeric_limits<double>::infinity()}) {
    ExpectError(
        *context_,
        SumWithFrame(
            WindowFrameUnit::kGroups,
            OffsetBoundDouble(WindowFrameBoundType::kOffsetPreceding, offset),
            Bound(WindowFrameBoundType::kCurrentRow)),
        "GROUPS offset must be an integer");
  }
  auto wide =
      Apply(*context_,
            SumWithFrame(
                WindowFrameUnit::kGroups,
                OffsetBoundDouble(WindowFrameBoundType::kOffsetPreceding, 1e30),
                Bound(WindowFrameBoundType::kCurrentRow)),
            {Value(10), Value(20), Value(20), Value(30)});
  ASSERT_TRUE(wide.HasValue()) << wide.GetStatus();
  EXPECT_EQ(wide.Value(),
            (std::vector<Value>{Value(10), Value(50), Value(50), Value(80)}));
}

TEST_F(DirectWinCov, RowsAndRangeOffsetDiagnosticsSurfaceAsStatus) {
  // ROWS offsets: non-constant, NULL, non-integer, negative.
  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kRows,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ColumnValueExp("v")),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "ROWS offset must be a constant integer");
  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kRows,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ConstantValueExp(Value())),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "cannot be NULL");
  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kRows,
                           OffsetBoundDouble(
                               WindowFrameBoundType::kOffsetPreceding, 1.5),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "ROWS offset must be a constant integer");
  ExpectError(
      *context_,
      SumWithFrame(WindowFrameUnit::kRows,
                   OffsetBound(WindowFrameBoundType::kOffsetPreceding, -1),
                   Bound(WindowFrameBoundType::kCurrentRow)),
      "must be non-negative");

  // RANGE offsets: multiple order keys, NULL, string, NaN, negative.
  auto two_keys = SumWithFrame(WindowFrameUnit::kRange,
                               Bound(WindowFrameBoundType::kOffsetPreceding,
                                     ConstantValueExp(Value(int64_t{1}))),
                               Bound(WindowFrameBoundType::kCurrentRow));
  two_keys->order_by.push_back(TermOnV(true));
  ExpectError(*context_, two_keys, "RANGE offset requires one constant key");

  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kRange,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ConstantValueExp(Value())),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "cannot be NULL");
  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kRange,
                           Bound(WindowFrameBoundType::kOffsetPreceding,
                                 ConstantValueExp(Value("x"))),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "RANGE offset must be numeric");
  ExpectError(
      *context_,
      SumWithFrame(WindowFrameUnit::kRange,
                   Bound(WindowFrameBoundType::kOffsetPreceding,
                         ConstantValueExp(
                             Value(std::numeric_limits<double>::quiet_NaN()))),
                   Bound(WindowFrameBoundType::kCurrentRow)),
      "cannot be NaN");
  ExpectError(*context_,
              SumWithFrame(WindowFrameUnit::kRange,
                           OffsetBoundDouble(
                               WindowFrameBoundType::kOffsetFollowing, -2.0),
                           Bound(WindowFrameBoundType::kCurrentRow)),
              "must be non-negative");
}

TEST_F(DirectWinCov, ExclusionModesWithoutOrderBy) {
  // No ORDER BY: the frame is the whole partition {10, 20}.
  auto group = FullFrameSum(WindowFrameExclusion::kGroup, false);
  auto rows = Apply(*context_, group, {Value(10), Value(20)});
  ASSERT_TRUE(rows.HasValue());
  // EXCLUDE GROUP removes every row: the frame empties to NULL.
  EXPECT_EQ(rows.Value(), (std::vector<Value>({Value(), Value()})));

  auto ties = FullFrameSum(WindowFrameExclusion::kTies, false);
  rows = Apply(*context_, ties, {Value(10), Value(20)});
  ASSERT_TRUE(rows.HasValue());
  // EXCLUDE TIES keeps only the current row.
  EXPECT_EQ(rows.Value(), (std::vector<Value>({Value(10), Value(20)})));

  // COUNT(*) over an exclusion-emptied frame reports 0.
  auto count = MakeWindow("COUNT", {ColumnValueExp("*")});
  count->exclusion = WindowFrameExclusion::kCurrentRow;
  rows = Apply(*context_, count, {Value(10)});
  ASSERT_TRUE(rows.HasValue());
  EXPECT_EQ(rows.Value(), (std::vector<Value>({Value(0)})));
}

TEST_F(DirectWinCov, FilterClauseDirectOnFrameSurvivors) {
  auto window = FullFrameSum(WindowFrameExclusion::kNone);
  window->where_filter = Vgt(15);
  auto rows = Apply(*context_, window);
  ASSERT_TRUE(rows.HasValue());
  // Survivors {20, 30} for every row.
  EXPECT_EQ(rows.Value(),
            (std::vector<Value>({Value(50), Value(50), Value(50), Value(50)})));

  // FILTER + EXCLUDE CURRENT ROW combine.
  auto both = FullFrameSum(WindowFrameExclusion::kCurrentRow);
  both->where_filter = Vgt(15);
  rows = Apply(*context_, both);
  ASSERT_TRUE(rows.HasValue());
  EXPECT_EQ(rows.Value(),
            (std::vector<Value>({Value(50), Value(50), Value(30), Value(20)})));
}

TEST_F(DirectWinCov, UnsupportedWindowAggregateRaises) {
  auto bogus = MakeWindow("DEFINITELY_NOT_AN_AGG", {ColumnValueExp("v")});
  auto rows = Apply(*context_, bogus);
  ASSERT_FALSE(rows.HasValue());
  EXPECT_NE(rows.GetStatus().GetMessage().find("unsupported window aggregate"),
            std::string::npos);
}

TEST_F(DirectWinCov, PercentileRespectNullsAndInterpolation) {
  auto percentile = MakeWindow(
      "PERCENTILE_CONT", {ColumnValueExp("v"), ConstantValueExp(Value(0.5))});
  percentile->order_by = {TermOnV(true)};
  percentile->respect_nulls = true;
  // With RESPECT NULLS the frame is {NULL, 10}; the interpolated median
  // crosses the NULL endpoint and yields NULL.
  auto rows = Apply(*context_, percentile, {Value(), Value(10)});
  ASSERT_TRUE(rows.HasValue());
  EXPECT_TRUE(rows.Value()[0].IsNull());
  EXPECT_TRUE(rows.Value()[1].IsNull());

  // Without RESPECT NULLS, NULLs drop out and the median is 10.
  percentile->respect_nulls = false;
  rows = Apply(*context_, percentile, {Value(), Value(10)});
  ASSERT_TRUE(rows.HasValue());
  EXPECT_DOUBLE_EQ(rows.Value()[0].value.double_value, 10.0);
  rows = Apply(*context_, percentile, {Value(), Value()});
  ASSERT_TRUE(rows.HasValue());
  ASSERT_EQ(rows.Value().size(), 2U);
  EXPECT_TRUE(rows.Value()[0].IsNull());
  EXPECT_TRUE(rows.Value()[1].IsNull());

  // PERCENTILE_DISC at p=1 with RESPECT NULLS must still see the non-NULL
  // value (the frame is {NULL, 10}); a moved-from `values` would return NULL.
  auto disc = MakeWindow("PERCENTILE_DISC",
                         {ColumnValueExp("v"), ConstantValueExp(Value(1.0))});
  disc->order_by = {TermOnV(true)};
  disc->respect_nulls = true;
  rows = Apply(*context_, disc, {Value(), Value(10)});
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 2U);
  EXPECT_EQ(rows.Value()[0], Value(10));
  EXPECT_EQ(rows.Value()[1], Value(10));
}

TEST_F(DirectWinCov, PercentileOverNumericStringsSortsNumerically) {
  auto percentile = MakeWindow(
      "PERCENTILE_CONT", {ColumnValueExp("v"), ConstantValueExp(Value(0.5))});
  auto rows = ApplyOverStrings(*context_, percentile,
                               {Value("10"), Value("2"), Value("33")});
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  // Numeric ordering 2 < 10 < 33 puts the median at "10"; lexicographic
  // ordering would answer "2".
  EXPECT_EQ(rows.Value()[0], Value("10"));
}

TEST_F(DirectWinCov, PercentileInterpolatesNumericStrings) {
  auto percentile = MakeWindow(
      "PERCENTILE_CONT", {ColumnValueExp("v"), ConstantValueExp(Value(0.5))});
  auto rows =
      ApplyOverStrings(*context_, percentile, {Value("10"), Value("2")});
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value(), (std::vector<Value>{Value(6.0), Value(6.0)}));
}

TEST_F(DirectWinCov, FirstLastNthValueWithExclusions) {
  // Frame = whole partition {10, 10, 20, 30}; NTH(2) skips excluded rows.
  auto nth = MakeWindow("NTH_VALUE",
                        {ColumnValueExp("v"), ConstantValueExp(Value(2))});
  nth->order_by = {TermOnV(true)};
  nth->frame_unit = WindowFrameUnit::kRows;
  nth->has_frame = true;
  nth->frame_start = Bound(WindowFrameBoundType::kUnboundedPreceding);
  nth->frame_end = Bound(WindowFrameBoundType::kUnboundedFollowing);

  nth->exclusion = WindowFrameExclusion::kCurrentRow;
  auto rows = Apply(*context_, nth);
  ASSERT_TRUE(rows.HasValue());
  // Skipping the current row, the 2nd survivor is 20 on the two 10-rows and
  // 10 on the 20/30-rows.
  EXPECT_EQ(rows.Value(),
            (std::vector<Value>({Value(20), Value(20), Value(10), Value(10)})));

  // NTH beyond the surviving frame yields NULL.
  nth->exclusion = WindowFrameExclusion::kNone;
  rows = Apply(*context_, nth, {Value(5)});
  ASSERT_TRUE(rows.HasValue());
  EXPECT_TRUE(rows.Value()[0].IsNull());

  // FIRST_VALUE with EXCLUDE CURRENT ROW falls to the second frame row.
  auto first = MakeWindow("FIRST_VALUE", {ColumnValueExp("v")});
  first->order_by = {TermOnV(true)};
  first->frame_unit = WindowFrameUnit::kRows;
  first->has_frame = true;
  first->frame_start = Bound(WindowFrameBoundType::kUnboundedPreceding);
  first->frame_end = Bound(WindowFrameBoundType::kUnboundedFollowing);
  first->exclusion = WindowFrameExclusion::kCurrentRow;
  rows = Apply(*context_, first);
  ASSERT_TRUE(rows.HasValue());
  EXPECT_EQ(rows.Value(),
            (std::vector<Value>({Value(10), Value(10), Value(10), Value(10)})));

  // EXCLUDE TIES without ORDER BY keeps only the current row.
  auto last = MakeWindow("LAST_VALUE", {ColumnValueExp("v")});
  last->frame_unit = WindowFrameUnit::kRows;
  last->has_frame = true;
  last->frame_start = Bound(WindowFrameBoundType::kUnboundedPreceding);
  last->frame_end = Bound(WindowFrameBoundType::kUnboundedFollowing);
  last->exclusion = WindowFrameExclusion::kTies;
  rows = Apply(*context_, last, {Value(10), Value(20)});
  ASSERT_TRUE(rows.HasValue());
  EXPECT_EQ(rows.Value(), (std::vector<Value>({Value(10), Value(20)})));

  // A frame emptied by bounds leaves FIRST_VALUE at NULL.
  auto empty = MakeWindow("FIRST_VALUE", {ColumnValueExp("v")});
  empty->order_by = {TermOnV(true)};
  empty->frame_unit = WindowFrameUnit::kRows;
  empty->has_frame = true;
  empty->frame_start = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 5);
  empty->frame_end = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 6);
  rows = Apply(*context_, empty);
  ASSERT_TRUE(rows.HasValue());
  EXPECT_TRUE(rows.Value()[0].IsNull());
}

TEST_F(DirectWinCov, CountDistinctExcludeTiesFastPathParity) {
  // EngineDecisions.Executor.DistinctExcludeTies: verify sliding
  // COUNT(DISTINCT) with EXCLUDE TIES matches the general evaluator (v + 0
  // expression disables TryComputeSlidingDistinctRows) across peer groups,
  // NULLs, and offset frames.
  const std::vector<Value> sample = {
      Value(10), Value(10), Value(20), Value(),   Value(10), Value(20),
      Value(20), Value(30), Value(30), Value(10), Value(),   Value(30)};
  auto check_parity = [&](bool with_order_by, const WindowFrameBound& start,
                          const WindowFrameBound& end) {
    auto fast = MakeWindow("COUNT", {ColumnValueExp("v")});
    fast->distinct = true;
    fast->has_frame = true;
    fast->frame_unit = WindowFrameUnit::kRows;
    fast->frame_start = start;
    fast->frame_end = end;
    fast->exclusion = WindowFrameExclusion::kTies;
    if (with_order_by) {
      fast->order_by = {TermOnV(true)};
    }
    auto ref =
        MakeWindow("COUNT", {std::make_shared<BinaryExpression>(
                                ColumnValueExp("v"), BinaryOperation::kAdd,
                                ConstantValueExp(Value(0)))});
    ref->distinct = true;
    ref->has_frame = true;
    ref->frame_unit = WindowFrameUnit::kRows;
    ref->frame_start = start;
    ref->frame_end = end;
    ref->exclusion = WindowFrameExclusion::kTies;
    if (with_order_by) {
      ref->order_by = {TermOnV(true)};
    }
    auto fast_res = Apply(*context_, fast, sample);
    auto ref_res = Apply(*context_, ref, sample);
    ASSERT_TRUE(fast_res.HasValue());
    ASSERT_TRUE(ref_res.HasValue());
    EXPECT_EQ(fast_res.Value(), ref_res.Value());
  };
  check_parity(true, OffsetBound(WindowFrameBoundType::kOffsetPreceding, 3),
               OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2));
  check_parity(true, OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2),
               OffsetBound(WindowFrameBoundType::kOffsetFollowing, 5));
  check_parity(false, OffsetBound(WindowFrameBoundType::kOffsetPreceding, 2),
               OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2));
}

}  // namespace tinylamb
