/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/grouping_sets.hpp"
#include "executor/values.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

// Non-throwing variant: errors come back as a failed StatusOr.
StatusOr<std::vector<Row>> TryRunSql(SqlEngine* engine, TransactionContext* ctx,
                                     const std::string& sql) {
  try {
    return RunSql(engine, ctx, sql);
  } catch (const std::exception& error) {
    return StatusError(StatusCode::kRuntimeError, error.what());
  }
}

// First row whose column `col` equals `key`, or nullptr.
const Row* FindRow(const std::vector<Row>& rows, size_t col, const Value& key) {
  for (const Row& row : rows) {
    if (row[col] == key) {
      return &row;
    }
  }
  return nullptr;
}

WindowOrderTerm TermOnV(bool ascending = true) {
  return WindowOrderTerm{.expression = ColumnValueExp("v"),
                         .ascending = ascending,
                         .nulls_first = std::nullopt};
}

std::shared_ptr<WindowFunctionCallExpression> MakeWindow(
    const std::string& function, std::vector<Expression> args = {}) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = function;
  window->args = std::move(args);
  return window;
}

WindowFrameBound Bound(WindowFrameBoundType type, Expression offset = nullptr) {
  return WindowFrameBound{.type = type, .offset = std::move(offset)};
}

WindowFrameBound OffsetBound(WindowFrameBoundType type, int64_t offset) {
  return Bound(type, ConstantValueExp(Value(offset)));
}

WindowFrameBound OffsetBoundDouble(WindowFrameBoundType type, double offset) {
  return Bound(type, ConstantValueExp(Value(offset)));
}

// Aggregate argument filters used by FILTER (WHERE ...) style tests.
Expression Vgt(int64_t threshold) {
  return BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kGreaterThan,
                             ConstantValueExp(Value(threshold)));
}

}  // namespace

class SqlWindowAggCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("sql_window_agg_coverage_test").MoveValue();
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

// ---------------------------------------------------------------------------
// Window functions through the SQL surface with ties, NULLs, NaNs, and every
// frame spec the frontend can spell.
// ---------------------------------------------------------------------------

class WindowSqlCov : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
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

TEST_F(WindowSqlCov, NanOrderKeysAreOnePeerGroupAboveEverything) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT RANK() OVER (ORDER BY f), SUM(v) OVER (ORDER BY f), "
                "CUME_DIST() OVER (ORDER BY f DESC) "
                "FROM wx WHERE g = 1 ORDER BY f NULLS FIRST, v");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 5U);
  const std::vector<Row>& out = rows.Value();
  // Order: NULL, 1.5, 1.5, 2.5, NaN.  NaN peers with nothing and sorts above
  // every number; the 1.5 ties share their cumulative frame.
  EXPECT_EQ(out[0][0], Value(1));
  EXPECT_EQ(out[1][0], Value(2));
  EXPECT_EQ(out[2][0], Value(2));
  EXPECT_EQ(out[3][0], Value(4));
  EXPECT_EQ(out[4][0], Value(5));
  // The engine sorts NULL lowest for ASC, so the NULL-key row's cumulative
  // sum includes it in every later frame: 50, 80, 80, 110, 150.
  EXPECT_EQ(out[0][1], Value(50));
  EXPECT_EQ(out[1][1], Value(80));
  EXPECT_EQ(out[2][1], Value(80));
  EXPECT_EQ(out[3][1], Value(110));
  EXPECT_EQ(out[4][1], Value(150));
  // CUME_DIST over f DESC (NULLS LAST by default): NaN, 2.5, 1.5, 1.5, NULL.
  EXPECT_DOUBLE_EQ(out[0][2].value.double_value, 1.0);
  EXPECT_DOUBLE_EQ(out[3][2].value.double_value, 0.4);
  EXPECT_DOUBLE_EQ(out[4][2].value.double_value, 0.2);
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
  // Order: NULL(50), 1.5(10), 1.5(20), 2.5(30), NaN(40).
  EXPECT_EQ(rows.Value(), (std::vector<Row>({Row({Value(50)}), Row({Value(60)}),
                                             Row({Value(60)}), Row({Value(60)}),
                                             Row({Value(40)})})));
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
  EXPECT_NE(r[5].ArrayElements()[0].AsString().find("\"value\":20"),
            std::string::npos);
  EXPECT_NE(r[5].ArrayElements()[0].AsString().find("\"count\":2"),
            std::string::npos);
  ASSERT_TRUE(r[6].IsArray());
  EXPECT_EQ(r[6].ArrayElements().size(), 1U);
  EXPECT_NE(r[6].ArrayElements()[0].AsString().find("\"value\":20"),
            std::string::npos);
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
  EXPECT_FALSE(overflow.HasValue());
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

  // Out-of-range percentile raises.
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT PERCENTILE_CONT(v, 1.5) OVER () "
                         "FROM UNNEST([10, 20]) AS v")
                   .HasValue());
}

TEST_F(WindowSqlCov, WindowErrorDiagnostics) {
  const std::string from = " FROM UNNEST([1, 2, 3]) AS v";
  const std::vector<std::string> bad = {
      "SELECT NTILE(NULL) OVER (ORDER BY v)" + from,
      "SELECT NTILE(0) OVER (ORDER BY v)" + from,
      "SELECT NTILE(1.5) OVER (ORDER BY v)" + from,
      "SELECT LAG(v, NULL) OVER (ORDER BY v)" + from,
      "SELECT LAG(v, 1.5) OVER (ORDER BY v)" + from,
      "SELECT LAG(v, -1) OVER (ORDER BY v)" + from,
      "SELECT LEAD(v, NULL) OVER (ORDER BY v)" + from,
      "SELECT NTH_VALUE(v, NULL) OVER (ORDER BY v)" + from,
      "SELECT NTH_VALUE(v, 1.5) OVER (ORDER BY v)" + from,
      "SELECT NTH_VALUE(v, 0) OVER (ORDER BY v)" + from,
      // RANGE offsets need exactly one ORDER BY key.
      "SELECT SUM(v) OVER (ORDER BY g, v RANGE BETWEEN 1 PRECEDING AND "
      "CURRENT ROW)" +
          from,
      "SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN NULL PRECEDING AND "
      "CURRENT ROW)" +
          from,
      "SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN 'x' PRECEDING AND "
      "CURRENT ROW)" +
          from,
      "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN NULL PRECEDING AND "
      "CURRENT ROW)" +
          from,
      "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN 1.5 PRECEDING AND "
      "CURRENT ROW)" +
          from,
  };
  for (const std::string& sql : bad) {
    EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(), sql).HasValue())
        << sql;
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
    windowed.Value().input.ForEachRow(
        [&](const Row& row) { out.push_back(row[1]); });
    return out;
  }

  static StatusOr<std::vector<Value>> ApplyOverStrings(
      TransactionContext& context,
      const std::shared_ptr<WindowFunctionCallExpression>& window,
      const std::vector<Value>& values) {
    SelectStatement statement({NamedExpression("w", Expression(window))}, {},
                              nullptr);
    relational_detail::Relation input;
    input.schema = Schema("", {Column("v", ValueType::kVarChar)});
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
    windowed.Value().input.ForEachRow(
        [&](const Row& row) { out.push_back(row[1]); });
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

TEST_F(DirectWinCov, GroupsFramesWithEveryBoundKind) {
  // Peer groups of {10, 10}, {20}, {30}.
  struct Case {
    std::string name;
    WindowFrameBound start;
    WindowFrameBound end;
    std::vector<Value> expected;
  } cases[] = {
      {"current..following",
       Bound(WindowFrameBoundType::kCurrentRow),
       OffsetBound(WindowFrameBoundType::kOffsetFollowing, 1),
       {Value(40), Value(40), Value(50), Value(30)}},
      {"following..unbounded",
       OffsetBound(WindowFrameBoundType::kOffsetFollowing, 1),
       Bound(WindowFrameBoundType::kUnboundedFollowing),
       {Value(50), Value(50), Value(30), Value(30)}},
      {"unbounded..unbounded",
       Bound(WindowFrameBoundType::kUnboundedFollowing),
       Bound(WindowFrameBoundType::kUnboundedFollowing),
       {Value(30), Value(30), Value(30), Value(30)}},
      {"unbounded_prec..unbounded_prec",
       Bound(WindowFrameBoundType::kUnboundedPreceding),
       Bound(WindowFrameBoundType::kUnboundedPreceding),
       {Value(10), Value(10), Value(10), Value(10)}},
      // A following-start clamped past the last group pins to it.
      {"following-clamped..unbounded",
       OffsetBound(WindowFrameBoundType::kOffsetFollowing, 5),
       Bound(WindowFrameBoundType::kUnboundedFollowing),
       {Value(30), Value(30), Value(30), Value(30)}},
      // Preceding-end below a following-start empties the frame.
      {"following..preceding",
       OffsetBound(WindowFrameBoundType::kOffsetFollowing, 1),
       OffsetBound(WindowFrameBoundType::kOffsetPreceding, 1),
       {Value(), Value(), Value(), Value()}},
  };
  for (const Case& c : cases) {
    auto result =
        Apply(*context_, SumWithFrame(WindowFrameUnit::kGroups,
                                      std::move(c.start), std::move(c.end)));
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

// ---------------------------------------------------------------------------
// GroupingSetsExecutor: DISTINCT aggregates, empty inputs, DATE/ARRAY keys,
// error surfaces, and the batched reader.
// ---------------------------------------------------------------------------

namespace {

Schema GsInputSchema() {
  return Schema(
      "gs", {Column("a", ValueType::kVarChar), Column("b", ValueType::kInt64)});
}

std::shared_ptr<ValuesExecutor> GsSource() {
  return std::make_shared<ValuesExecutor>(std::vector<Row>{
      Row({Value("x"), Value(int64_t{1})}),
      Row({Value("x"), Value(int64_t{2})}),
      Row({Value("y"), Value(int64_t{1})}),
  });
}

std::vector<NamedExpression> GsKeys() {
  return {NamedExpression("a", ColumnValueExp("a")),
          NamedExpression("b", ColumnValueExp("b"))};
}

std::vector<Row> Drain(ExecutorBase* executor) {
  std::vector<Row> out;
  Row row;
  while (executor->Next(&row, nullptr)) {
    out.push_back(row);
    row = Row();
  }
  return out;
}

NamedExpression CountStarAgg(const std::string& name) {
  return NamedExpression(
      name, std::make_shared<AggregateExpression>(AggregationType::kCount,
                                                  ColumnValueExp("*")));
}

}  // namespace

TEST(GroupingSetsCoverageTest, DistinctAggregatesOverExplicitSets) {
  std::vector<NamedExpression> aggs;
  auto distinct_count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("b"), true);
  auto distinct_sum = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("b"), true);
  auto distinct_avg = std::make_shared<AggregateExpression>(
      AggregationType::kAvg, ColumnValueExp("b"), true);
  auto distinct_min = std::make_shared<AggregateExpression>(
      AggregationType::kMin, ColumnValueExp("b"), true);
  auto distinct_max = std::make_shared<AggregateExpression>(
      AggregationType::kMax, ColumnValueExp("b"), true);
  auto distinct_any = std::make_shared<AggregateExpression>(
      AggregationType::kAnyValue, ColumnValueExp("b"), true);
  aggs.emplace_back("dc", std::move(distinct_count));
  aggs.emplace_back("ds", std::move(distinct_sum));
  aggs.emplace_back("da", std::move(distinct_avg));
  aggs.emplace_back("dmin", std::move(distinct_min));
  aggs.emplace_back("dmax", std::move(distinct_max));
  aggs.emplace_back("dany", std::move(distinct_any));

  GroupingSetsExecutor gs(GsSource(), GsInputSchema(), GsKeys(), {{0}, {}},
                          std::move(aggs));
  const std::vector<Row> rows = Drain(&gs);
  ASSERT_EQ(rows.size(), 3U);
  // (x): distinct b in {1, 2}; (y): {1}; grand total: {1, 2}.
  EXPECT_EQ(rows[0][2], Value(int64_t{2}));
  EXPECT_EQ(rows[0][3], Value(3));
  EXPECT_DOUBLE_EQ(rows[0][4].value.double_value, 1.5);
  EXPECT_EQ(rows[0][5], Value(1));
  EXPECT_EQ(rows[0][6], Value(2));
  // ANY_VALUE(DISTINCT b) over {1, 2} may surface either element.
  EXPECT_TRUE(rows[0][7] == Value(1) || rows[0][7] == Value(2));
  EXPECT_EQ(rows[2][2], Value(int64_t{2}));
  EXPECT_EQ(rows[2][3], Value(3));
}

TEST(GroupingSetsCoverageTest, NonDistinctAnyValueAndBitAggregates) {
  std::vector<NamedExpression> aggs;
  aggs.emplace_back("av", std::make_shared<AggregateExpression>(
                              AggregationType::kAnyValue, ColumnValueExp("b")));
  aggs.emplace_back("ba", std::make_shared<AggregateExpression>(
                              AggregationType::kBitAnd, ColumnValueExp("b")));
  aggs.emplace_back("bo", std::make_shared<AggregateExpression>(
                              AggregationType::kBitOr, ColumnValueExp("b")));
  aggs.emplace_back("bx", std::make_shared<AggregateExpression>(
                              AggregationType::kBitXor, ColumnValueExp("b")));
  // DISTINCT forms of unhandled kinds fall back to the distinct defaults.
  aggs.emplace_back(
      "dbx", std::make_shared<AggregateExpression>(AggregationType::kBitXor,
                                                   ColumnValueExp("b"), true));
  GroupingSetsExecutor gs(GsSource(), GsInputSchema(), GsKeys(), {{0, 1}},
                          std::move(aggs));
  const std::vector<Row> rows = Drain(&gs);
  ASSERT_EQ(rows.size(), 3U);
  // Group (x, 1): any=1, and=1, or=1, xor=1.
  EXPECT_EQ(rows[0][2], Value(1));
  EXPECT_EQ(rows[0][3], Value(1));
  EXPECT_EQ(rows[0][4], Value(1));
  EXPECT_EQ(rows[0][5], Value(1));
  EXPECT_EQ(rows[0][6], Value(int64_t{1}));
  // Group (x, 2): any=2, and=2, or=2, xor=2.
  EXPECT_EQ(rows[1][2], Value(2));
  EXPECT_EQ(rows[1][3], Value(2));
  EXPECT_EQ(rows[1][5], Value(2));
}

TEST(GroupingSetsCoverageTest, AvgOverAllNullGroupIsNull) {
  std::vector<Row> source_rows = {
      Row({Value("x"), Value()}),
      Row({Value("x"), Value()}),
  };
  std::vector<NamedExpression> aggs;
  aggs.emplace_back("cnt", std::make_shared<AggregateExpression>(
                               AggregationType::kCount, ColumnValueExp("b")));
  aggs.emplace_back("sum", std::make_shared<AggregateExpression>(
                               AggregationType::kSum, ColumnValueExp("b")));
  aggs.emplace_back("avg", std::make_shared<AggregateExpression>(
                               AggregationType::kAvg, ColumnValueExp("b")));
  aggs.emplace_back("min", std::make_shared<AggregateExpression>(
                               AggregationType::kMin, ColumnValueExp("b")));
  GroupingSetsExecutor gs(std::make_shared<ValuesExecutor>(source_rows),
                          GsInputSchema(), GsKeys(), {{0}}, std::move(aggs));
  const std::vector<Row> rows = Drain(&gs);
  ASSERT_EQ(rows.size(), 1U);
  // Row layout: [a key, b key, cnt, sum, avg, min]; the {0} set leaves b NULL.
  EXPECT_EQ(rows[0][0], Value("x"));
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[0][2], Value(int64_t{0}));
  EXPECT_TRUE(rows[0][3].IsNull());
  EXPECT_TRUE(rows[0][4].IsNull());
  EXPECT_TRUE(rows[0][5].IsNull());
}

TEST(GroupingSetsCoverageTest, EmptyInputEmitsGrandTotalOnly) {
  GroupingSetsExecutor gs(std::make_shared<ValuesExecutor>(std::vector<Row>{}),
                          GsInputSchema(), GsKeys(), {{0}, {}},
                          {CountStarAgg("cnt")});
  const std::vector<Row> rows = Drain(&gs);
  // Only the empty grouping set survives on an empty input.  Row layout is
  // [a key, b key, cnt]: the grand total reports COUNT(*) = 0.
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_TRUE(rows[0][0].IsNull());
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[0][2], Value(int64_t{0}));
}

TEST(GroupingSetsCoverageTest, DateAndArrayGroupKeysTypeTheOutputSchema) {
  Schema input(
      "gsd", {Column("d", ValueType::kDate), Column("arr", ValueType::kArray)});
  std::vector<Row> source_rows = {
      Row({Value::Date("2020-01-01"),
           Value::Array({Value(int64_t{1})}, "INT64")}),
      Row({Value::Date("2020-01-01"),
           Value::Array({Value(int64_t{2})}, "INT64")}),
  };
  std::vector<NamedExpression> keys = {
      NamedExpression("d", ColumnValueExp("d")),
      NamedExpression("arr", ColumnValueExp("arr"))};
  GroupingSetsExecutor gs(std::make_shared<ValuesExecutor>(source_rows), input,
                          keys, {{0, 1}, {0}}, {CountStarAgg("cnt")});
  const std::vector<Row> rows = Drain(&gs);
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(gs.OutputSchema().GetColumn(0).Type(), ValueType::kDate);
  EXPECT_EQ(gs.OutputSchema().GetColumn(1).Type(), ValueType::kArray);
}

TEST(GroupingSetsCoverageTest, FailingGroupKeyAndAggregateAbortTheExecutor) {
  // A group key that fails to evaluate (1/0) aborts materialization.
  std::vector<NamedExpression> bad_keys = {NamedExpression(
      "bad",
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kDivide,
                          ConstantValueExp(Value(int64_t{0}))))};
  GroupingSetsExecutor bad_key(GsSource(), GsInputSchema(), bad_keys, {{0}},
                               {CountStarAgg("cnt")});
  Row row;
  EXPECT_FALSE(bad_key.Next(&row, nullptr));

  // An aggregate whose child fails to evaluate aborts too.
  std::vector<NamedExpression> bad_aggs = {NamedExpression(
      "bad", std::make_shared<AggregateExpression>(
                 AggregationType::kSum,
                 BinaryExpressionExp(ConstantValueExp(Value(1)),
                                     BinaryOperation::kDivide,
                                     ConstantValueExp(Value(int64_t{0})))))};
  GroupingSetsExecutor bad_agg(GsSource(), GsInputSchema(), GsKeys(), {{0}},
                               std::move(bad_aggs));
  EXPECT_FALSE(bad_agg.Next(&row, nullptr));
}

TEST(GroupingSetsCoverageTest, WhereFilterInsideGroupingSets) {
  auto filtered = std::make_shared<AggregateExpression>(AggregationType::kSum,
                                                        ColumnValueExp("b"));
  filtered->SetWhereFilter(
      BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{1}))));
  GroupingSetsExecutor gs(GsSource(), GsInputSchema(), GsKeys(), {{0}},
                          {NamedExpression("s", std::move(filtered))});
  const std::vector<Row> rows = Drain(&gs);
  ASSERT_EQ(rows.size(), 2U);
  // Row layout: [a key, b key, filtered sum].  (x): only b=2 survives the
  // filter; (y): nothing survives.
  EXPECT_EQ(rows[0][2], Value(2));
  EXPECT_TRUE(rows[1][2].IsNull());
}

TEST(GroupingSetsCoverageTest, NextBatchStreamsRowsInChunks) {
  GroupingSetsExecutor gs = GroupingSetsExecutor::Rollup(
      GsSource(), GsInputSchema(), GsKeys(), {CountStarAgg("cnt")});
  DataChunk chunk;
  EXPECT_EQ(gs.NextBatch(&chunk, 0), 0U);
  size_t total = 0;
  while (true) {
    const size_t got = gs.NextBatch(&chunk, 2);
    if (got == 0) {
      break;
    }
    total += got;
    EXPECT_LE(got, 2U);
  }
  // 3 (a,b) + 2 (a) + 1 (grand total).
  EXPECT_EQ(total, 6U);
}

// ---------------------------------------------------------------------------
// Scalar aggregation (AggregationExecutor): typed batch fast paths, constant
// and all-NULL inputs, generic mixed lists, DISTINCT and FILTER clauses.
// ---------------------------------------------------------------------------

class AggSmallCov : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("agg_small-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(),
                  "CREATE TABLE med (a INT64, cnt_uint64 INT64, f FLOAT64, "
                  "nul INT64, logi BOOL, s VARCHAR(20));")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med SELECT n, n * 2, n * 0.5, NULL, "
                          "MOD(n, 2) = 0, CAST(MOD(n, 7) AS STRING) "
                          "FROM UNNEST(GENERATE_ARRAY(1, 64)) AS n;")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med VALUES (0, 0, CAST('NaN' AS "
                          "FLOAT64), NULL, FALSE, 'x');")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med VALUES (0, 0, NULL, NULL, TRUE, "
                          "'y');")
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

TEST_F(AggSmallCov, TypedBatchSweepOverColumns) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT COUNT(*), COUNT(a), COUNT(nul), SUM(a), AVG(a), MIN(a), "
      "MAX(a), SUM(f), AVG(f), MIN(f), MAX(f), SUM(cnt_uint64), "
      "BIT_AND(a), BIT_OR(a), BIT_XOR(a), LOGICAL_AND(a), LOGICAL_OR(a) "
      "FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 1U);
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{66}));
  EXPECT_EQ(r[1], Value(int64_t{66}));
  EXPECT_EQ(r[2], Value(int64_t{0}));
  EXPECT_EQ(r[3], Value(int64_t{2080}));  // 1..64 plus two extra zeros.
  EXPECT_DOUBLE_EQ(r[4].value.double_value, 2080.0 / 66.0);
  EXPECT_EQ(r[5], Value(int64_t{0}));
  EXPECT_EQ(r[6], Value(int64_t{64}));
  // The NaN row poisons every float aggregate.
  EXPECT_TRUE(std::isnan(r[7].value.double_value));
  EXPECT_TRUE(std::isnan(r[8].value.double_value));
  EXPECT_TRUE(std::isnan(r[9].value.double_value));
  EXPECT_TRUE(std::isnan(r[10].value.double_value));
  EXPECT_EQ(r[11].value.int_value, int64_t{4160});  // uint64-named column.
  EXPECT_EQ(r[12], Value(int64_t{0}));
  EXPECT_EQ(r[13], Value(int64_t{127}));
  EXPECT_EQ(r[14], Value(int64_t{64}));
  EXPECT_FALSE(r[15].Truthy());
  EXPECT_TRUE(r[16].Truthy());
}

TEST_F(AggSmallCov, ConstantAggregatesUseTypedShortcuts) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(5), SUM(2.5), AVG(4), MIN(3), MAX(7), COUNT(2), "
                "BIT_AND(9), BIT_OR(8), BIT_XOR(7), LOGICAL_AND(TRUE), "
                "LOGICAL_OR(FALSE) FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{330}));
  EXPECT_DOUBLE_EQ(r[1].value.double_value, 165.0);
  EXPECT_DOUBLE_EQ(r[2].value.double_value, 4.0);
  EXPECT_EQ(r[3], Value(int64_t{3}));
  EXPECT_EQ(r[4], Value(int64_t{7}));
  EXPECT_EQ(r[5], Value(int64_t{66}));
  EXPECT_EQ(r[6], Value(int64_t{9}));
  EXPECT_EQ(r[7], Value(int64_t{8}));
  // 66 rows: 7 XORed an even number of times cancels out.
  EXPECT_EQ(r[8], Value(int64_t{0}));
  EXPECT_TRUE(r[9].Truthy());
  EXPECT_FALSE(r[10].Truthy());
}

TEST_F(AggSmallCov, AllNullColumnBatchShortCircuits) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(nul), AVG(nul), MIN(nul), MAX(nul), COUNT(nul), "
                "BIT_AND(nul), BIT_OR(nul), BIT_XOR(nul), LOGICAL_AND(nul), "
                "LOGICAL_OR(nul) FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_TRUE(r[0].IsNull());
  EXPECT_TRUE(r[1].IsNull());
  EXPECT_TRUE(r[2].IsNull());
  EXPECT_TRUE(r[3].IsNull());
  EXPECT_EQ(r[4], Value(int64_t{0}));
  EXPECT_TRUE(r[5].IsNull());
  EXPECT_TRUE(r[6].IsNull());
  EXPECT_TRUE(r[7].IsNull());
  EXPECT_TRUE(r[8].IsNull());
  EXPECT_TRUE(r[9].IsNull());
}

TEST_F(AggSmallCov, GenericMixedListKeepsTypedAggregatesCorrect) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(a), BIT_AND(a), BIT_OR(a), BIT_XOR(a), "
      "LOGICAL_AND(logi), LOGICAL_OR(logi), STRING_AGG(s, ',') FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{2080}));
  EXPECT_EQ(r[1], Value(int64_t{0}));
  EXPECT_EQ(r[2], Value(int64_t{127}));
  EXPECT_EQ(r[3], Value(int64_t{64}));
  EXPECT_FALSE(r[4].Truthy());
  EXPECT_TRUE(r[5].Truthy());
  EXPECT_FALSE(r[6].IsNull());
  // All 66 rows are joined: 65 separators.
  const std::string joined = r[6].AsString();
  EXPECT_EQ(std::count(joined.begin(), joined.end(), ','), 65);
}

TEST_F(AggSmallCov, DistinctAggregatesRouteThroughAccumulators) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT COUNT(DISTINCT s), SUM(DISTINCT a), MIN(DISTINCT s), "
                "MAX(DISTINCT s), COUNT(DISTINCT a) FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{9}));  // '0'..'6', 'x', 'y'.
  EXPECT_EQ(r[1], Value(int64_t{2080}));
  EXPECT_EQ(r[2], Value("0"));
  EXPECT_EQ(r[3], Value("y"));
  EXPECT_EQ(r[4], Value(int64_t{65}));
}

TEST_F(AggSmallCov, FilteredAggregatesSkipNonMatchingRows) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT COUNT(* WHERE a > 32), SUM(a WHERE a > 32), "
                "MIN(a WHERE a > 32), SUM(a WHERE a > 1000) "
                "FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{32}));
  EXPECT_EQ(r[1], Value(int64_t{1552}));
  EXPECT_EQ(r[2], Value(int64_t{33}));
  EXPECT_TRUE(r[3].IsNull());
}

TEST_F(AggSmallCov, SumOverVarcharRaises) {
  EXPECT_FALSE(
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(s) FROM med")
          .HasValue());
}

TEST_F(AggSmallCov, IntegerAndDoubleSumOverflowRaise) {
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT SUM(v), COUNT(v) FROM "
                         "UNNEST([9223372036854775807, 1]) AS v")
                   .HasValue());
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT SUM(f), COUNT(f) FROM "
                         "UNNEST([1e308, 1e308]) AS f")
                   .HasValue());
}

TEST_F(AggSmallCov, EmptyInputScalarAggregates) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(a), AVG(a), COUNT(*), MIN(a), STRING_AGG(s, ',') FROM med "
      "WHERE 1 = 0");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_TRUE(r[0].IsNull());
  EXPECT_TRUE(r[1].IsNull());
  EXPECT_EQ(r[2], Value(int64_t{0}));
  EXPECT_TRUE(r[3].IsNull());
  EXPECT_TRUE(r[4].IsNull());
}

TEST_F(AggSmallCov, SingleSumUsesCheckedJitFallbackPath) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(a) FROM med");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{2080}));
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT SUM(v) FROM "
                         "UNNEST([9223372036854775807, 1]) AS v")
                   .HasValue());
}

// ---------------------------------------------------------------------------
// Parallel aggregation: a 10k-row table crosses kParallelAggregationMinRows
// so GROUP BY plans route through ParallelAggregationExecutor.
// ---------------------------------------------------------------------------

class AggBigCov : public ::testing::Test {
 protected:
  static constexpr int64_t kRows = 10000;

  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("agg_big-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "CREATE TABLE big (g INT64, a INT64, f FLOAT64, "
                          "nul INT64, u_uint64 INT64, huge INT64, "
                          "s VARCHAR(20));")
                    .HasValue());
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(),
                  "INSERT INTO big SELECT MOD(n, 3), n, n * 1.5, NULL, n * 3, "
                  "CASE WHEN n <= 2 THEN 9223372036854775807 ELSE 0 END, "
                  "CAST(MOD(n, 7) AS STRING) "
                  "FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS n;")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO big VALUES (0, 0, CAST('NaN' AS "
                          "FLOAT64), NULL, 0, 0, 'zz');")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO big VALUES (0, 0, NULL, NULL, 0, 0, "
                          "'zz');")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO big VALUES (NULL, 1, 2.5, NULL, 3, 0, "
                          "'zz');")
                    .HasValue());
    // A small dimension table for the join-driven runtime key filter test.
    EXPECT_TRUE(
        TryRunSql(engine_.get(), context_.get(), "CREATE TABLE med (a INT64);")
            .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med SELECT n FROM "
                          "UNNEST(GENERATE_ARRAY(1, 64)) AS n;")
                    .HasValue());
    EXPECT_TRUE(TryRunSql(engine_.get(), context_.get(),
                          "INSERT INTO med VALUES (0), (0);")
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

  // Reference sums for group g: a = {g, g+3, ...} <= 10000.
  static int64_t SumFor(int64_t g) {
    int64_t total = 0;
    for (int64_t n = g; n <= kRows; n += 3) {
      total += n;
    }
    return total;
  }
  static int64_t CountFor(int64_t g) {
    int64_t count = 0;
    for (int64_t n = g; n <= kRows; n += 3) {
      ++count;
    }
    return count;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(AggBigCov, GroupedWideAggregateSweep) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, COUNT(*), COUNT(a), COUNT(f), COUNT(nul), SUM(a), AVG(a), "
      "MIN(a), MAX(a), SUM(f), SUM(u_uint64), LOGICAL_AND(a), LOGICAL_OR(a) "
      "FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);  // g = NULL, 0, 1, 2.
  const Row* null_group = FindRow(rows.Value(), 0, Value());
  ASSERT_NE(null_group, nullptr);
  EXPECT_EQ((*null_group)[1], Value(int64_t{1}));
  EXPECT_EQ((*null_group)[4], Value(int64_t{0}));
  EXPECT_EQ((*null_group)[5], Value(int64_t{1}));

  const Row* g0 = FindRow(rows.Value(), 0, Value(int64_t{0}));
  ASSERT_NE(g0, nullptr);
  // g=0: 3333 generated rows + the NaN and NULL-f rows = 3335.
  EXPECT_EQ((*g0)[1], Value(int64_t{3335}));
  EXPECT_EQ((*g0)[2], Value(int64_t{3335}));
  EXPECT_EQ((*g0)[3], Value(int64_t{3334}));  // one NULL f.
  EXPECT_EQ((*g0)[4], Value(int64_t{0}));
  EXPECT_EQ((*g0)[5], Value(int64_t{SumFor(0)}));
  EXPECT_TRUE(std::isnan((*g0)[9].value.double_value));  // NaN poisons.
  EXPECT_EQ((*g0)[10].value.int_value, 3 * SumFor(0));
  // a=0 rows make LOGICAL_AND false; a has non-zero values for OR.
  EXPECT_EQ((*g0)[11], Value(int64_t{0}));
  EXPECT_EQ((*g0)[12], Value(int64_t{1}));

  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  EXPECT_EQ((*g1)[1], Value(int64_t{CountFor(1)}));
  EXPECT_EQ((*g1)[5], Value(int64_t{SumFor(1)}));
  EXPECT_DOUBLE_EQ(
      (*g1)[6].value.double_value,
      static_cast<double>(SumFor(1)) / static_cast<double>(CountFor(1)));
  EXPECT_EQ((*g1)[7], Value(int64_t{1}));
  EXPECT_EQ((*g1)[8], Value(int64_t{10000}));
  EXPECT_DOUBLE_EQ((*g1)[9].value.double_value,
                   1.5 * static_cast<double>(SumFor(1)));
  EXPECT_EQ((*g1)[10].value.int_value, 3 * SumFor(1));
}

TEST_F(AggBigCov, GroupedStatisticsMatchClosedForms) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, VAR_POP(a), VAR_SAMP(a), STDDEV_POP(a), STDDEV_SAMP(a), "
      "COVAR_POP(a, f), COVAR_SAMP(a, f), CORR(a, f) FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  // Reference moments for a = 1, 4, ..., 10000 and f = 1.5 * a.
  const double n = static_cast<double>(CountFor(1));
  double sx = 0, sxx = 0, sy = 0, sxy = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    sx += v;
    sxx += static_cast<double>(v) * v;
    sy += 1.5 * v;
    sxy += 1.5 * static_cast<double>(v) * v;
  }
  const double mean = sx / n;
  const double var_pop = sxx / n - mean * mean;
  EXPECT_NEAR((*g1)[1].value.double_value, var_pop, var_pop * 1e-9 + 1e-9);
  EXPECT_NEAR((*g1)[2].value.double_value, var_pop * n / (n - 1.0),
              var_pop * 1e-9 + 1e-9);
  EXPECT_NEAR((*g1)[3].value.double_value, std::sqrt(var_pop),
              std::sqrt(var_pop) * 1e-9 + 1e-9);
  const double covar_pop = sxy / n - mean * (sy / n);
  EXPECT_NEAR((*g1)[5].value.double_value, covar_pop,
              std::abs(covar_pop) * 1e-9 + 1e-9);
  EXPECT_NEAR((*g1)[6].value.double_value, covar_pop * n / (n - 1.0),
              std::abs(covar_pop) * 1e-9 + 1e-9);
  EXPECT_NEAR((*g1)[7].value.double_value, 1.0, 1e-9);

  // VAR_SAMP with a single sample row is NULL (the NULL-key group).
  const Row* null_group = FindRow(rows.Value(), 0, Value());
  ASSERT_NE(null_group, nullptr);
  EXPECT_TRUE((*null_group)[2].IsNull());
  EXPECT_TRUE((*null_group)[6].IsNull());
  EXPECT_TRUE((*null_group)[7].IsNull());
}

TEST_F(AggBigCov, GroupedDistinctAggregatesReplayThroughMerge) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, COUNT(DISTINCT s), SUM(DISTINCT a), MIN(DISTINCT a), "
      "MAX(DISTINCT a), AVG(DISTINCT a) FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  ASSERT_EQ(rows.Value().size(), 4U);
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  EXPECT_EQ((*g1)[1], Value(int64_t{7}));  // n % 7 covers '0'..'6'.
  EXPECT_EQ((*g1)[2], Value(int64_t{SumFor(1)}));
  EXPECT_EQ((*g1)[3], Value(int64_t{1}));
  EXPECT_EQ((*g1)[4], Value(int64_t{10000}));
  EXPECT_DOUBLE_EQ(
      (*g1)[5].value.double_value,
      static_cast<double>(SumFor(1)) / static_cast<double>(CountFor(1)));
}

TEST_F(AggBigCov, GroupedDistinctStatisticsForceSingleWorker) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, VAR_POP(DISTINCT a), CORR(DISTINCT a, f) FROM big GROUP BY "
      "g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  const double n = static_cast<double>(CountFor(1));
  double sx = 0, sxx = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    sx += v;
    sxx += static_cast<double>(v) * v;
  }
  const double mean = sx / n;
  const double var_pop = sxx / n - mean * mean;
  EXPECT_NEAR((*g1)[1].value.double_value, var_pop, var_pop * 1e-9 + 1e-9);
  // a and f are perfectly correlated within the group.
  EXPECT_NEAR((*g1)[2].value.double_value, 1.0, 1e-9);
}

TEST_F(AggBigCov, GroupedFilteredAggregatesAccumulateGenerically) {
  int64_t big_count = 0;
  int64_t big_sum = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    if (v > 5000) {
      ++big_count;
      big_sum += v;
    }
  }
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT g, COUNT(* WHERE a > 5000), SUM(a WHERE a > "
                "5000), AVG(a WHERE a > 0), VAR_POP(a WHERE a > 0) "
                "FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  EXPECT_EQ((*g1)[1], Value(big_count));
  EXPECT_EQ((*g1)[2], Value(big_sum));
  EXPECT_DOUBLE_EQ(
      (*g1)[3].value.double_value,
      static_cast<double>(SumFor(1)) / static_cast<double>(CountFor(1)));
  // FILTER (WHERE a > 0) keeps every row of the group.
  double sx = 0, sxx = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    sx += v;
    sxx += static_cast<double>(v) * v;
  }
  const double n = static_cast<double>(CountFor(1));
  const double mean = sx / n;
  const double var_pop = sxx / n - mean * mean;
  EXPECT_NEAR((*g1)[4].value.double_value, var_pop, var_pop * 1e-9 + 1e-9);
}

TEST_F(AggBigCov, GenericStatisticalAggregatesOnExpressions) {
  // Non-column children keep the per-row generic statistical path; adding a
  // per-group constant never changes variance or correlation.
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, VAR_POP(a - g), CORR(a - g, f - g) FROM big GROUP BY g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g2 = FindRow(rows.Value(), 0, Value(int64_t{2}));
  ASSERT_NE(g2, nullptr);
  const double n = static_cast<double>(CountFor(2));
  double sx = 0, sxx = 0;
  for (int64_t v = 2; v <= kRows; v += 3) {
    sx += v;
    sxx += static_cast<double>(v) * v;
  }
  const double mean = sx / n;
  const double var_pop = sxx / n - mean * mean;
  EXPECT_NEAR((*g2)[1].value.double_value, var_pop, var_pop * 1e-9 + 1e-9);
  EXPECT_NEAR((*g2)[2].value.double_value, 1.0, 1e-9);
}

TEST_F(AggBigCov, ParallelSumOverflowRaises) {
  // Multi-aggregate: the parallel typed int64 path must raise instead of
  // wrapping.
  EXPECT_FALSE(TryRunSql(engine_.get(), context_.get(),
                         "SELECT SUM(huge), COUNT(*) FROM big")
                   .HasValue());
  // Single SUM: the checked JIT fallback path raises too.
  EXPECT_FALSE(
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(huge) FROM big")
          .HasValue());
}

TEST_F(AggBigCov, ParallelScanAppliesSimpleAndResidualFilters) {
  // 10002 rows have a non-NULL g (10000 generated + two g=0 extras);
  // two of them carry s = 'zz'.
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT COUNT(*) FROM big WHERE g >= 0 AND a / 2 >= 0 AND s != 'zz'");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{10000}));
}

TEST_F(AggBigCov, JoinInstallsIntegerKeyFilterOnProbeSide) {
  // med.a (1..64 plus two zeros) joins big.a; the runtime key filter must not
  // change the answer (69 matching pairs).
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT COUNT(*) FROM med JOIN big ON med.a = big.a WHERE big.a >= 0");
  EXPECT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  if (rows.HasValue()) {
    EXPECT_EQ(rows.Value()[0][0], Value(int64_t{69}));
  }
}

// ---------------------------------------------------------------------------
// Direct CompiledScanFilter coverage: compile/evaluate branches that typed
// SQL cannot spell (constant-left comparisons, DATE-vs-string literals,
// mixed numeric operands, disjunctive branch errors).
// ---------------------------------------------------------------------------

class ScanFilterDirectCov : public ::testing::Test {
 protected:
  using CompiledScanFilter = relational_detail::CompiledScanFilter;

  void SetUp() override {
    database_ =
        Database::Create("scan_filter_cov-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  bool Match(const CompiledScanFilter& filter, const Row& row,
             const Schema& schema, Status* error = nullptr) {
    Status sink{Status::kSuccess};
    Status* err = error != nullptr ? error : &sink;
    return relational_detail::MatchScanFilter(row, schema, filter, nullptr,
                                              *context_, {}, err);
  }

  static CompiledScanFilter Compile(const std::vector<Expression>& preds,
                                    const Schema& schema) {
    return relational_detail::CompileScanFilter(preds, schema);
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(ScanFilterDirectCov, SimpleCompareEveryOpBothOperandOrders) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  const auto cmp = [](BinaryOperation op, int64_t l, int64_t r) {
    switch (op) {
      case BinaryOperation::kEquals:
        return l == r;
      case BinaryOperation::kNotEquals:
        return l != r;
      case BinaryOperation::kLessThan:
        return l < r;
      case BinaryOperation::kLessThanEquals:
        return l <= r;
      case BinaryOperation::kGreaterThan:
        return l > r;
      case BinaryOperation::kGreaterThanEquals:
        return l >= r;
      default:
        return false;
    }
  };
  const std::vector<BinaryOperation> ops = {
      BinaryOperation::kEquals,      BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,    BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals};
  for (const BinaryOperation op : ops) {
    // Column on the left.
    CompiledScanFilter f =
        Compile({BinaryExpressionExp(ColumnValueExp("v"), op,
                                     ConstantValueExp(Value(int64_t{3})))},
                schema);
    EXPECT_EQ(Match(f, Row({Value(int64_t{3})}), schema), cmp(op, 3, 3))
        << static_cast<int>(op);
    EXPECT_EQ(Match(f, Row({Value(int64_t{1})}), schema), cmp(op, 1, 3))
        << static_cast<int>(op);
    // Constant on the left exercises FlipCompare.
    CompiledScanFilter flipped =
        Compile({BinaryExpressionExp(ConstantValueExp(Value(int64_t{3})), op,
                                     ColumnValueExp("v"))},
                schema);
    EXPECT_EQ(flipped.simple.size(), 1U) << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{2})}), schema), cmp(op, 3, 2))
        << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{4})}), schema), cmp(op, 3, 4))
        << static_cast<int>(op);
  }
}

TEST_F(ScanFilterDirectCov, DateColumnVersusStringConstants) {
  const Schema schema("t", {Column("d", ValueType::kDate)});
  const Value date = Value::Date("2020-01-15");
  struct OpCase {
    BinaryOperation op;
    std::string literal;
    bool expected;
  } cases[] = {
      {BinaryOperation::kEquals, "2020-01-15", true},
      {BinaryOperation::kNotEquals, "2020-01-15", false},
      {BinaryOperation::kLessThan, "2020-02-01", true},
      {BinaryOperation::kLessThanEquals, "2020-01-15", true},
      {BinaryOperation::kGreaterThan, "2020-01-01", true},
      {BinaryOperation::kGreaterThanEquals, "2020-01-16", false},
  };
  for (const OpCase& c : cases) {
    CompiledScanFilter f = Compile(
        {BinaryExpressionExp(ColumnValueExp("d"), c.op,
                             ConstantValueExp(Value(c.literal.c_str())))},
        schema);
    EXPECT_EQ(Match(f, Row({date}), schema), c.expected)
        << c.literal << " op " << static_cast<int>(c.op);
  }
  // An unparsable date literal rejects the row without raising.
  CompiledScanFilter bad = Compile(
      {BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                           ConstantValueExp(Value("not-a-date")))},
      schema);
  EXPECT_FALSE(Match(bad, Row({date}), schema));
  // NULL on either side never matches.
  CompiledScanFilter null_row = Compile(
      {BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                           ConstantValueExp(Value("2020-01-15")))},
      schema);
  EXPECT_FALSE(Match(null_row, Row({Value()}), schema));
}

TEST_F(ScanFilterDirectCov, MixedNumericOperandTypes) {
  const Schema schema(
      "t", {Column("i", ValueType::kInt64), Column("f", ValueType::kDouble)});
  struct Case {
    Expression pred;
    Row row;
    bool expected;
  } cases[] = {
      // INT64 value vs DOUBLE constant: falls through to double coercion.
      {BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                           ConstantValueExp(Value(1.5))),
       Row({Value(int64_t{2}), Value(0.0)}), true},
      {BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                           ConstantValueExp(Value(1.5))),
       Row({Value(int64_t{1}), Value(0.0)}), false},
      // DOUBLE value vs INT64 constant.
      {BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kEquals,
                           ConstantValueExp(Value(int64_t{2}))),
       Row({Value(int64_t{0}), Value(2.0)}), true},
      {BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kNotEquals,
                           ConstantValueExp(Value(int64_t{2}))),
       Row({Value(int64_t{0}), Value(2.5)}), true},
      {BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kLessThan,
                           ConstantValueExp(Value(int64_t{3}))),
       Row({Value(int64_t{0}), Value(2.5)}), true},
      {BinaryExpressionExp(ColumnValueExp("f"),
                           BinaryOperation::kLessThanEquals,
                           ConstantValueExp(Value(int64_t{2}))),
       Row({Value(int64_t{0}), Value(2.0)}), true},
      {BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kGreaterThan,
                           ConstantValueExp(Value(int64_t{2}))),
       Row({Value(int64_t{0}), Value(2.5)}), true},
      {BinaryExpressionExp(ColumnValueExp("f"),
                           BinaryOperation::kGreaterThanEquals,
                           ConstantValueExp(Value(int64_t{3}))),
       Row({Value(int64_t{0}), Value(2.5)}), false},
      // INT64 value vs VARCHAR constant: no numeric coercion, never matches.
      {BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kEquals,
                           ConstantValueExp(Value("3"))),
       Row({Value(int64_t{3}), Value(0.0)}), false},
      {BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kNotEquals,
                           ConstantValueExp(Value("3"))),
       Row({Value(int64_t{3}), Value(0.0)}), false},
      {BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kLessThan,
                           ConstantValueExp(Value("9"))),
       Row({Value(int64_t{3}), Value(0.0)}), false},
      {BinaryExpressionExp(ColumnValueExp("i"),
                           BinaryOperation::kLessThanEquals,
                           ConstantValueExp(Value("9"))),
       Row({Value(int64_t{3}), Value(0.0)}), false},
      {BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                           ConstantValueExp(Value("1"))),
       Row({Value(int64_t{3}), Value(0.0)}), false},
      {BinaryExpressionExp(ColumnValueExp("i"),
                           BinaryOperation::kGreaterThanEquals,
                           ConstantValueExp(Value("1"))),
       Row({Value(int64_t{3}), Value(0.0)}), false},
  };
  for (const Case& c : cases) {
    CompiledScanFilter f = Compile({c.pred}, schema);
    EXPECT_EQ(Match(f, c.row, schema), c.expected);
  }
}

TEST_F(ScanFilterDirectCov, DisjunctiveBranchesOrOfAnds) {
  const Schema schema(
      "t", {Column("v", ValueType::kInt64), Column("w", ValueType::kInt64)});
  Expression pred = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{1}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kAdd,
                                  ColumnValueExp("w")),
              BinaryOperation::kGreaterThan,
              ConstantValueExp(Value(int64_t{3})))),
      BinaryOperation::kOr,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(int64_t{0}))));
  CompiledScanFilter f = Compile({pred}, schema);
  ASSERT_EQ(f.disjunctive_branches.size(), 2U);
  EXPECT_FALSE(f.disjunctive_branches[0].residual.empty());
  EXPECT_TRUE(Match(f, Row({Value(int64_t{2}), Value(int64_t{3})}), schema));
  EXPECT_TRUE(Match(f, Row({Value(int64_t{-1}), Value(int64_t{0})}), schema));
  EXPECT_FALSE(Match(f, Row({Value(int64_t{1}), Value(int64_t{9})}), schema));
}

TEST_F(ScanFilterDirectCov, DisjunctiveCrossProductAndTooWideGuard) {
  const Schema schema(
      "t", {Column("v", ValueType::kInt64), Column("w", ValueType::kInt64)});
  auto eq = [](const std::string& col, int64_t value) {
    return BinaryExpressionExp(ColumnValueExp(col), BinaryOperation::kEquals,
                               ConstantValueExp(Value(value)));
  };
  Expression six_way_v = eq("v", 1);
  Expression six_way_w = eq("w", 1);
  for (int i = 2; i <= 6; ++i) {
    six_way_v = BinaryExpressionExp(six_way_v, BinaryOperation::kOr,
                                    eq("v", static_cast<int64_t>(i)));
    six_way_w = BinaryExpressionExp(six_way_w, BinaryOperation::kOr,
                                    eq("w", static_cast<int64_t>(i)));
  }
  // Two 6-way OR predicates would cross into 36 branches, exceeding
  // kMaxDisjunctiveBranches: the second predicate falls back to residual.
  CompiledScanFilter wide = Compile({six_way_v, six_way_w}, schema);
  ASSERT_EQ(wide.disjunctive_branches.size(), 6U);
  EXPECT_EQ(wide.residual.size(), 1U);
  EXPECT_TRUE(Match(wide, Row({Value(int64_t{6}), Value(int64_t{1})}), schema));
  EXPECT_FALSE(
      Match(wide, Row({Value(int64_t{7}), Value(int64_t{1})}), schema));

  // 3x2 = 6 branches stay within the guard and build a cross product.
  Expression three_way = BinaryExpressionExp(
      eq("v", 1), BinaryOperation::kOr,
      BinaryExpressionExp(eq("v", 2), BinaryOperation::kOr, eq("v", 3)));
  Expression two_way =
      BinaryExpressionExp(eq("w", 4), BinaryOperation::kOr, eq("w", 5));
  CompiledScanFilter crossed = Compile({three_way, two_way}, schema);
  ASSERT_EQ(crossed.disjunctive_branches.size(), 6U);
  EXPECT_TRUE(
      Match(crossed, Row({Value(int64_t{2}), Value(int64_t{5})}), schema));
  EXPECT_FALSE(
      Match(crossed, Row({Value(int64_t{2}), Value(int64_t{6})}), schema));
}

TEST_F(ScanFilterDirectCov, BranchResidualErrorIsReportedToCaller) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  // (v = 1 AND 10/(v - v) > 0) OR v = 2: evaluating row v=1 raises inside the
  // first branch's residual; no branch passes and the error surfaces.
  Expression division = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{10})), BinaryOperation::kDivide,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kSubtract,
                          ColumnValueExp("v")));
  Expression pred = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{1}))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(division, BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{0})))),
      BinaryOperation::kOr,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{2}))));
  CompiledScanFilter f = Compile({pred}, schema);
  ASSERT_EQ(f.disjunctive_branches.size(), 2U);
  Status error{Status::kSuccess};
  EXPECT_FALSE(Match(f, Row({Value(int64_t{1})}), schema, &error));
  EXPECT_FALSE(error.ok());

  // The second branch passes cleanly, so no error is reported.
  Status ok_error{Status::kSuccess};
  EXPECT_TRUE(Match(f, Row({Value(int64_t{2})}), schema, &ok_error));
  EXPECT_TRUE(ok_error.ok());
}

TEST_F(ScanFilterDirectCov, ResidualBytecodeErrorFallsBackPerConjunct) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  // 10/(v-v) > 0 raises on every row: bytecode eval fails, the per-conjunct
  // fallback records the error, and the row is rejected.
  Expression division = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{10})), BinaryOperation::kDivide,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kSubtract,
                          ColumnValueExp("v")));
  CompiledScanFilter f =
      Compile({BinaryExpressionExp(division, BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(int64_t{0})))},
              schema);
  EXPECT_TRUE(f.residual_bytecode.has_value() || !f.residual.empty());
  Status error{Status::kSuccess};
  EXPECT_FALSE(Match(f, Row({Value(int64_t{1})}), schema, &error));
  EXPECT_FALSE(error.ok());
}

TEST_F(ScanFilterDirectCov, ResidualOnlyFiltersUseBytecode) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  CompiledScanFilter f = Compile(
      {BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kModulo,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})))},
      schema);
  EXPECT_TRUE(f.simple.empty());
  EXPECT_TRUE(Match(f, Row({Value(int64_t{4})}), schema));
  EXPECT_FALSE(Match(f, Row({Value(int64_t{5})}), schema));
  EXPECT_FALSE(Match(f, Row({Value()}), schema));
}

TEST_F(ScanFilterDirectCov, UnsignedColumnsAreRetaggedBeforeMatching) {
  Column u("u", ValueType::kInt64);
  u.SetUnsigned(true);
  Schema schema("t", {u});
  CompiledScanFilter f = Compile(
      {BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kLessThan,
                           ConstantValueExp(Value(int64_t{1})))},
      schema);
  EXPECT_TRUE(f.needs_unsigned_tagging);
  // Stored as INT64_MIN bit pattern; as UINT64 that is huge, so NOT < 1.
  EXPECT_FALSE(
      Match(f, Row({Value(std::numeric_limits<int64_t>::min())}), schema));
  EXPECT_TRUE(Match(f, Row({Value(int64_t{0})}), schema));
}

TEST_F(ScanFilterDirectCov, BuildIntegerPeeksSkipsUnusablePredicates) {
  Column unsigned_col("u", ValueType::kInt64);
  unsigned_col.SetUnsigned(true);
  const Schema schema(
      "t", {Column("i", ValueType::kInt64), Column("s", ValueType::kVarChar)});
  const Schema full("t", {Column("i", ValueType::kInt64),
                          Column("s", ValueType::kVarChar), unsigned_col});

  std::vector<Expression> preds = {
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{3}))),
      BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("x"))),
      BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(int64_t{9}))),
      BinaryExpressionExp(ColumnValueExp("nope"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1})))};
  CompiledScanFilter filter = Compile(preds, schema);

  // No projection: column indexes pass through; the string and unsigned
  // predicates are skipped.
  std::vector<IntegerPeekCompare> peeks =
      relational_detail::BuildIntegerPeeks(filter, nullptr, full);
  ASSERT_EQ(peeks.size(), 1U);
  EXPECT_EQ(peeks[0].column, 0U);
  EXPECT_EQ(peeks[0].constant, int64_t{3});

  // Projection remaps slot 0 -> full-schema column 1 (VARCHAR) which is not
  // peekable, and drops out-of-range slots.
  const std::vector<slot_t> projection = {1};
  peeks = relational_detail::BuildIntegerPeeks(filter, &projection, full);
  EXPECT_TRUE(peeks.empty());
}

TEST_F(ScanFilterDirectCov, SplitAndCombineDisjuncts) {
  Expression or3 = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{1}))),
      BinaryOperation::kOr,
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kOr,
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(int64_t{3})))));
  std::vector<Expression> parts = relational_detail::SplitDisjuncts(or3);
  ASSERT_EQ(parts.size(), 3U);
  Expression combined = relational_detail::CombineDisjuncts(parts);
  EXPECT_TRUE(relational_detail::SplitDisjuncts(combined).size() == 3U);
  EXPECT_FALSE(relational_detail::CombineDisjuncts({}));
}

}  // namespace tinylamb
