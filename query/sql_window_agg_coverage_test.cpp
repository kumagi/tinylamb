/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

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

#include "common/test_util.hpp"
#include "common/platform/process.hpp"
#include "common/memory_budget.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
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

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  // One retry for empty diagnostics: an empty LastError can indicate that an
  // external parse child is transiently unavailable under load. Other paths
  // may also return an empty diagnostic, so this retry is deliberately bounded.
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    if (engine->LastError().empty()) {
      prepared = engine->Prepare(*ctx, sql);
    }
    if (!prepared.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
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

// Prepares and drains `sql`, returning the executor's Dump() text so tests can
// assert route/counter evidence (ParallelScan, HashAggregate, fused typed
// folds, runtime filters, ...).  Returns nullopt on prepare or run failure.
std::optional<std::string> DumpSql(SqlEngine* engine, TransactionContext* ctx,
                                   const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    return std::nullopt;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    row = Row();
  }
  if (prepared.Value()->GetStatus() != Status::kSuccess) {
    return std::nullopt;
  }
  std::ostringstream dump;
  prepared.Value()->Dump(dump, 0);
  return dump.str();
}

// Restores an environment variable to the value it held on construction (or
// unsets it when it was absent) on destruction, so tests that toggle
// TINYLAMB_EXEC_ENGINE do not leak state into later tests.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions): scoped test environment guard.
class EnvGuard {
 public:
  explicit EnvGuard(const char* name) : name_(name) {
    if (const char* value = ::tinylamb::platform::GetEnv(name)) {
      had_value_ = true;
      value_ = value;
    }
  }
  EnvGuard(const EnvGuard&) = delete;
  EnvGuard& operator=(const EnvGuard&) = delete;
  ~EnvGuard() { Restore(); }
  void Restore() {
    if (had_value_) {
      ::tinylamb::platform::SetEnv(name_.c_str(), value_.c_str(), 1);
    } else {
      ::tinylamb::platform::UnsetEnv(name_.c_str());
    }
  }

 private:
  std::string name_;
  std::string value_;
  bool had_value_{false};
};

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
  // non-copyable by design
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions): scoped test state guard.
  struct ResetGuard {
    ResetGuard() = default;
    ResetGuard(const ResetGuard&) = delete;
    ResetGuard& operator=(const ResetGuard&) = delete;
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
    ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "legacy", 1), 0);
    const auto legacy = TryRunSql(engine_.get(), context_.get(), c.sql);
    engine_env.Restore();
    ASSERT_TRUE(legacy.HasValue()) << legacy.GetStatus().GetMessage();
    EXPECT_EQ(legacy.Value(), c.expected) << c.sql;

    // In-memory pipeline lane and the forced-spill (OOC RangeFrameScanner)
    // lane, both pinned to the pipeline engine regardless of the caller's
    // TINYLAMB_EXEC_ENGINE.
    ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "pipeline", 1), 0);
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
  ASSERT_TRUE(types.Value()[1][0].IsUnsigned());
  ASSERT_EQ(types.Value()[0][0].type, ValueType::kDouble);

  EnvGuard engine_env("TINYLAMB_EXEC_ENGINE");
  ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "pipeline", 1), 0);
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

// ---------------------------------------------------------------------------
// Scalar aggregation (AggregationExecutor): typed batch fast paths, constant
// and all-NULL inputs, generic mixed lists, DISTINCT and FILTER clauses.
// ---------------------------------------------------------------------------

class AggSmallCov : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
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

TEST_F(AggSmallCov, TypedColumnAggregateSweep) {
  const std::string sql =
      "SELECT COUNT(*), COUNT(a), COUNT(nul), SUM(a), AVG(a), MIN(a), "
      "MAX(a), SUM(f), AVG(f), MIN(f), MAX(f), SUM(cnt_uint64), "
      "BIT_AND(a), BIT_OR(a), BIT_XOR(a), LOGICAL_AND(a), LOGICAL_OR(a) "
      "FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
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
  // 66 rows fed all 17 typed column aggregates (66 * 17 = 1122 updates), so
  // the sweep touched every row/aggregate pair rather than short-circuiting.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=66"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("aggregate_updates=1122"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, ConstantAggregatesSkipColumnDecoding) {
  const std::string sql =
      "SELECT SUM(5), SUM(2.5), AVG(4), MIN(3), MAX(7), COUNT(2), "
      "BIT_AND(9), BIT_OR(8), BIT_XOR(7), LOGICAL_AND(TRUE), "
      "LOGICAL_OR(FALSE) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
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
  // Every aggregate argument is a literal, so the scan decodes no column
  // values (scan_values_decoded=0) even though it still counts 66 rows.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("scan_values_decoded=0"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, AllNullColumnAggregatesEvaluateToNull) {
  const std::string sql =
      "SELECT SUM(nul), AVG(nul), MIN(nul), MAX(nul), COUNT(nul), "
      "BIT_AND(nul), BIT_OR(nul), BIT_XOR(nul), LOGICAL_AND(nul), "
      "LOGICAL_OR(nul) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
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
  // There is no all-NULL short circuit: all 66 rows still feed all 10
  // aggregates (66 * 10 = 660 updates), each folding NULLs to the shown
  // NULL/0 results.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=66"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("aggregate_updates=660"), std::string::npos) << *dump;
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
  // med.s is CAST(MOD(n,7) AS STRING) for n = 1..64 followed by 'x','y', so
  // the join is nine "1,2,3,4,5,6,0" cycles, a trailing '1' (n = 64), then
  // 'x','y' — 66 elements in insertion order.
  std::string expected_join;
  for (int i = 0; i < 9; ++i) {
    if (i > 0) {
      expected_join += ',';
    }
    expected_join += "1,2,3,4,5,6,0";
  }
  expected_join += ",1,x,y";
  EXPECT_EQ(std::string(r[6].AsStringView()), expected_join);
}

TEST_F(AggSmallCov, DistinctAggregatesRouteThroughAccumulators) {
  const std::string sql =
      "SELECT COUNT(DISTINCT s), SUM(DISTINCT a), MIN(DISTINCT s), "
      "MAX(DISTINCT s), COUNT(DISTINCT a) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{9}));  // '0'..'6', 'x', 'y'.
  EXPECT_EQ(r[1], Value(int64_t{2080}));
  EXPECT_EQ(r[2], Value("0"));
  EXPECT_EQ(r[3], Value("y"));
  EXPECT_EQ(r[4], Value(int64_t{65}));
  // DISTINCT members are folded through the hash-table accumulator path, not
  // the typed batch sweep: the plan shows a HashAggregate consuming all 66
  // input rows into one group.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("HashAggregate"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("input_rows=66"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, FilteredAggregatesSkipNonMatchingRows) {
  const std::string sql =
      "SELECT COUNT(* WHERE a > 32), SUM(a WHERE a > 32), "
      "MIN(a WHERE a > 32), SUM(a WHERE a > 1000) "
      "FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_EQ(r[0], Value(int64_t{32}));
  EXPECT_EQ(r[1], Value(int64_t{1552}));
  EXPECT_EQ(r[2], Value(int64_t{33}));
  EXPECT_TRUE(r[3].IsNull());
  // The FILTER (WHERE ...) clauses survive into the aggregate plan rather
  // than being silently dropped — each member keeps its own predicate text.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("a > 32"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("a > 1000"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, SumOverVarcharRaises) {
  const auto result =
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(s) FROM med");
  ASSERT_FALSE(result.HasValue());
  EXPECT_NE(result.GetStatus().GetMessage().find("numeric value required"),
            std::string::npos)
      << result.GetStatus().GetMessage();
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
  const std::string sql =
      "SELECT SUM(a), AVG(a), COUNT(*), MIN(a), STRING_AGG(s, ',') FROM med "
      "WHERE 1 = 0";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row& r = rows.Value()[0];
  EXPECT_TRUE(r[0].IsNull());
  EXPECT_TRUE(r[1].IsNull());
  EXPECT_EQ(r[2], Value(int64_t{0}));
  EXPECT_TRUE(r[3].IsNull());
  EXPECT_TRUE(r[4].IsNull());
  // The empty input is observable: the scalar aggregate consumed zero rows
  // (input_rows=0) yet still emitted the single NULL/0 group row.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("input_rows=0"), std::string::npos) << *dump;
}

TEST_F(AggSmallCov, PercentileAndQuantilesReturnNullOnEmptyInput) {
  const auto rows =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT PERCENTILE_CONT(a, 0.5), PERCENTILE_DISC(a, 0.5), "
                "APPROX_QUANTILES(a, 2) FROM med WHERE FALSE");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus();
  ASSERT_EQ(rows.Value().size(), 1U);
  for (const auto& value : rows.Value()[0].values_) {
    EXPECT_TRUE(value.IsNull());
  }
}

TEST_F(AggSmallCov, SingleSumUsesCheckedTypedFoldPath) {
  const std::string sql = "SELECT SUM(a) FROM med";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{2080}));
  // A lone SUM over a scanned column folds through the checked typed path
  // (fused-typed-fold), which is what makes the overflow below safe.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("fused-typed-fold"), std::string::npos) << *dump;
  const auto overflow =
      TryRunSql(engine_.get(), context_.get(),
                "SELECT SUM(v) FROM UNNEST([9223372036854775807, 1]) AS v");
  ASSERT_FALSE(overflow.HasValue());
  EXPECT_NE(overflow.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << overflow.GetStatus().GetMessage();
}

// ---------------------------------------------------------------------------
// Grouped aggregation coverage over 10k rows. This is below the current
// parallel threshold; aggregate_shape_test verifies parallel operator
// selection with inputs sized from kParallelAggregationMinRows.
// ---------------------------------------------------------------------------

class AggBigCov : public ::testing::Test {
 protected:
  static constexpr int64_t kRows = 10000;

  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
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
  const auto n = static_cast<double>(CountFor(1));
  double sx = 0, sxx = 0, sy = 0, sxy = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
    sy += (1.5 * dv);
    sxy += ((1.5 * dv) * dv);
  }
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g1)[1].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[2].value.double_value, var_pop * n / (n - 1.0),
              (var_pop * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[3].value.double_value, std::sqrt(var_pop),
              (std::sqrt(var_pop) * 1e-9) + 1e-9);
  const double covar_pop = (sxy / n) - (mean * (sy / n));
  EXPECT_NEAR((*g1)[5].value.double_value, covar_pop,
              (std::abs(covar_pop) * 1e-9) + 1e-9);
  EXPECT_NEAR((*g1)[6].value.double_value, covar_pop * n / (n - 1.0),
              (std::abs(covar_pop) * 1e-9) + 1e-9);
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

// DISTINCT statistical aggregates over a 10k-row GROUP BY must still merge
// correctly per group.  No runtime counter exposes the worker count and the
// table is below kParallelAggregationMinRows anyway, so this pins the merged
// per-group values rather than claiming a single-worker route.
TEST_F(AggBigCov, GroupedDistinctStatisticsEvaluatePerGroup) {
  const auto rows = TryRunSql(
      engine_.get(), context_.get(),
      "SELECT g, VAR_POP(DISTINCT a), CORR(DISTINCT a, f) FROM big GROUP BY "
      "g");
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g1 = FindRow(rows.Value(), 0, Value(int64_t{1}));
  ASSERT_NE(g1, nullptr);
  const auto n = static_cast<double>(CountFor(1));
  double sx = 0, sxx = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
  }
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g1)[1].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  // a and f are perfectly correlated within the group.
  EXPECT_NEAR((*g1)[2].value.double_value, 1.0, 1e-9);
}

TEST_F(AggBigCov, GroupedFilteredAggregatesApplyRowFilters) {
  int64_t big_count = 0;
  int64_t big_sum = 0;
  for (int64_t v = 1; v <= kRows; v += 3) {
    if (v > 5000) {
      ++big_count;
      big_sum += v;
    }
  }
  const std::string sql =
      "SELECT g, COUNT(* WHERE a > 5000), SUM(a WHERE a > "
      "5000), AVG(a WHERE a > 0), VAR_POP(a WHERE a > 0) "
      "FROM big GROUP BY g";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
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
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
  }
  const auto n = static_cast<double>(CountFor(1));
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g1)[4].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  // The grouped aggregation consumed all 10003 input rows into 4 groups and
  // applied each member's row filter during accumulation: 30002 updates is
  // exactly the per-(row,aggregate) pairs whose predicate held.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=10003"), std::string::npos)
      << *dump;
  EXPECT_NE(dump->find("aggregate_groups=4"), std::string::npos) << *dump;
  EXPECT_NE(dump->find("aggregate_updates=30002"), std::string::npos) << *dump;
}

// Statistical aggregates over expression children ((a - g), (f - g)) keep the
// per-row generic accumulation path; adding a per-group constant never
// changes variance or correlation.  The counters show every one of the 10003
// input rows fed both aggregates (20006 updates).
TEST_F(AggBigCov, StatisticalAggregatesOnExpressionChildren) {
  const std::string sql =
      "SELECT g, VAR_POP(a - g), CORR(a - g, f - g) FROM big GROUP BY g";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  const Row* g2 = FindRow(rows.Value(), 0, Value(int64_t{2}));
  ASSERT_NE(g2, nullptr);
  const auto n = static_cast<double>(CountFor(2));
  double sx = 0, sxx = 0;
  for (int64_t v = 2; v <= kRows; v += 3) {
    const auto dv = static_cast<double>(v);
    sx += dv;
    sxx += (dv * dv);
  }
  const double mean = sx / n;
  const double var_pop = (sxx / n) - (mean * mean);
  EXPECT_NEAR((*g2)[1].value.double_value, var_pop, (var_pop * 1e-9) + 1e-9);
  EXPECT_NEAR((*g2)[2].value.double_value, 1.0, 1e-9);
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("aggregate_input_rows=10003"), std::string::npos)
      << *dump;
  EXPECT_NE(dump->find("aggregate_updates=20006"), std::string::npos) << *dump;
}

TEST_F(AggBigCov, ParallelSumOverflowRaises) {
  // Multi-aggregate: the parallel typed int64 path must raise instead of
  // wrapping.
  const auto multi = TryRunSql(engine_.get(), context_.get(),
                               "SELECT SUM(huge), COUNT(*) FROM big");
  ASSERT_FALSE(multi.HasValue());
  EXPECT_NE(multi.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << multi.GetStatus().GetMessage();
  // Single SUM: the checked typed fold raises too.
  const auto single =
      TryRunSql(engine_.get(), context_.get(), "SELECT SUM(huge) FROM big");
  ASSERT_FALSE(single.HasValue());
  EXPECT_NE(single.GetStatus().GetMessage().find("integer overflow in SUM"),
            std::string::npos)
      << single.GetStatus().GetMessage();
}

TEST_F(AggBigCov, ParallelScanAppliesSimpleAndResidualFilters) {
  // Statistics let the planner pick the parallel scan; the mixed predicate
  // (g >= 0 simple, a / 2 >= 0 residual, s != 'zz' simple) must be applied by
  // the worker-filter on the scan side.
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(), "ANALYZE big;").HasValue());
  const std::string sql =
      "SELECT COUNT(*) FROM big WHERE g >= 0 AND a / 2 >= 0 AND s != 'zz'";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{10000}));
  // 10002 rows have a non-NULL g (10000 generated + two g=0 extras);
  // two of them carry s = 'zz'.  The dump proves the parallel worker-filter
  // scan ran rather than a serial full scan.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("ParallelScan: big ("), std::string::npos) << *dump;
  EXPECT_NE(dump->find("worker-filter"), std::string::npos) << *dump;
}

TEST_F(AggBigCov, JoinInstallsIntegerKeyFilterOnProbeSide) {
  // Statistics drive the join to build a runtime exact-set filter over big.a
  // and probe med with it; the filter must not change the answer (69
  // matching pairs).
  ASSERT_TRUE(
      TryRunSql(engine_.get(), context_.get(), "ANALYZE big;").HasValue());
  const std::string sql =
      "SELECT COUNT(*) FROM med JOIN big ON med.a = big.a WHERE big.a >= 0";
  const auto rows = TryRunSql(engine_.get(), context_.get(), sql);
  ASSERT_TRUE(rows.HasValue()) << rows.GetStatus().GetMessage();
  EXPECT_EQ(rows.Value()[0][0], Value(int64_t{69}));
  // The dump proves a runtime join filter was installed and consulted at
  // least one scan.
  const auto dump = DumpSql(engine_.get(), context_.get(), sql);
  if (!dump.has_value()) {
    FAIL() << "executor dump unavailable";
    return;
  }
  EXPECT_NE(dump->find("RuntimeFilter type="), std::string::npos) << *dump;
  EXPECT_NE(dump->find("scans="), std::string::npos) << *dump;
}

// ---------------------------------------------------------------------------
// Direct CompiledScanFilter coverage: compile/evaluate branches that typed
// SQL cannot spell (constant-left comparisons, DATE-vs-string literals,
// mixed numeric operands, disjunctive branch errors).
// ---------------------------------------------------------------------------

class ScanFilterDirectTest : public ::testing::Test {
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

TEST_F(ScanFilterDirectTest, SimpleCompareEveryOpBothOperandOrders) {
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
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{3})}), schema), cmp(op, 3, 3))
        << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{2})}), schema), cmp(op, 3, 2))
        << static_cast<int>(op);
    EXPECT_EQ(Match(flipped, Row({Value(int64_t{4})}), schema), cmp(op, 3, 4))
        << static_cast<int>(op);
  }
}

TEST_F(ScanFilterDirectTest, DateColumnVersusStringConstants) {
  const Schema schema("t", {Column("d", ValueType::kDate)});
  const Value date = Value::Date("2020-01-15");
  struct OpCase {
    std::string literal;
    BinaryOperation op;
    bool expected;
  };
  const std::array<OpCase, 6> cases = {{
      {.literal = "2020-01-15",
       .op = BinaryOperation::kEquals,
       .expected = true},
      {.literal = "2020-01-15",
       .op = BinaryOperation::kNotEquals,
       .expected = false},
      {.literal = "2020-02-01",
       .op = BinaryOperation::kLessThan,
       .expected = true},
      {.literal = "2020-01-15",
       .op = BinaryOperation::kLessThanEquals,
       .expected = true},
      {.literal = "2020-01-01",
       .op = BinaryOperation::kGreaterThan,
       .expected = true},
      {.literal = "2020-01-16",
       .op = BinaryOperation::kGreaterThanEquals,
       .expected = false},
  }};
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

TEST_F(ScanFilterDirectTest, MixedNumericOperandTypes) {
  const Schema schema(
      "t", {Column("i", ValueType::kInt64), Column("f", ValueType::kDouble)});
  struct Case {
    Expression pred;
    Row row;
    bool expected;
  };
  const std::array<Case, 14> cases = {{
      // INT64 value vs DOUBLE constant: falls through to double coercion.
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(1.5))),
       .row = Row({Value(int64_t{2}), Value(0.0)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(1.5))),
       .row = Row({Value(int64_t{1}), Value(0.0)}),
       .expected = false},
      // DOUBLE value vs INT64 constant.
      {.pred =
           BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kEquals,
                               ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.0)}),
       .expected = true},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kNotEquals,
                               ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = true},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("f"), BinaryOperation::kLessThan,
                               ConstantValueExp(Value(int64_t{3}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("f"),
                                   BinaryOperation::kLessThanEquals,
                                   ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.0)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("f"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(int64_t{2}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = true},
      {.pred = BinaryExpressionExp(ColumnValueExp("f"),
                                   BinaryOperation::kGreaterThanEquals,
                                   ConstantValueExp(Value(int64_t{3}))),
       .row = Row({Value(int64_t{0}), Value(2.5)}),
       .expected = false},
      // INT64 value vs VARCHAR constant: no numeric coercion, never matches.
      {.pred =
           BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kEquals,
                               ConstantValueExp(Value("3"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kNotEquals,
                               ConstantValueExp(Value("3"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred =
           BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kLessThan,
                               ConstantValueExp(Value("9"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kLessThanEquals,
                                   ConstantValueExp(Value("9"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value("1"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
      {.pred = BinaryExpressionExp(ColumnValueExp("i"),
                                   BinaryOperation::kGreaterThanEquals,
                                   ConstantValueExp(Value("1"))),
       .row = Row({Value(int64_t{3}), Value(0.0)}),
       .expected = false},
  }};
  for (const Case& c : cases) {
    CompiledScanFilter f = Compile({c.pred}, schema);
    EXPECT_EQ(Match(f, c.row, schema), c.expected);
  }
}

TEST_F(ScanFilterDirectTest, DisjunctiveBranchesOrOfAnds) {
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

TEST_F(ScanFilterDirectTest, DisjunctiveCrossProductAndTooWideGuard) {
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

TEST_F(ScanFilterDirectTest, BranchResidualErrorIsReportedToCaller) {
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

TEST_F(ScanFilterDirectTest, ResidualBytecodeErrorFallsBackPerConjunct) {
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

TEST_F(ScanFilterDirectTest, ResidualOnlyFiltersUseBytecode) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  CompiledScanFilter f = Compile(
      {BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kModulo,
                              ConstantValueExp(Value(int64_t{2}))),
          BinaryOperation::kEquals, ConstantValueExp(Value(int64_t{0})))},
      schema);
  // v % 2 is not a simple compare, so the whole predicate is residual and is
  // carried as compiled bytecode rather than a per-row AST residual.
  EXPECT_TRUE(f.simple.empty());
  EXPECT_TRUE(f.residual_bytecode.has_value());
  EXPECT_TRUE(Match(f, Row({Value(int64_t{4})}), schema));
  EXPECT_FALSE(Match(f, Row({Value(int64_t{5})}), schema));
  EXPECT_FALSE(Match(f, Row({Value()}), schema));
}

TEST_F(ScanFilterDirectTest, UnsignedColumnsAreRetaggedBeforeMatching) {
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

TEST_F(ScanFilterDirectTest, BuildIntegerPeeksSkipsUnusablePredicates) {
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

TEST_F(ScanFilterDirectTest, SplitAndCombineDisjuncts) {
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
  EXPECT_EQ(relational_detail::SplitDisjuncts(combined).size(), 3U);
  EXPECT_FALSE(relational_detail::CombineDisjuncts({}));
}

TEST(LeanPhase3ParityTest,
     WindowDistinctInverseMatchesAcrossNullsDuplicatesAndExclusions) {
  // The body toggles this knob repeatedly and unsets it at the end; the guard
  // restores any externally supplied value even on an early ASSERT exit.
  EnvGuard wdi_env("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
  auto db_or = Database::Create("lean_wdi_" + RandomString());
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    SqlEngine engine(*db);
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE t (id INT64, v INT64);").HasValue());
    ASSERT_TRUE(engine
                    .Execute(ctx,
                             "INSERT INTO t VALUES "
                             "(1, 10), (2, 10), (3, NULL), (4, -5), (5, 20), "
                             "(6, 10), (7, 20), (8, NULL), (9, 30), (10, -5);")
                    .HasValue());
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  const std::vector<std::string> queries = {
      "SELECT id, "
      "COUNT(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 "
      "FOLLOWING), "
      "SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 "
      "FOLLOWING) "
      "FROM t ORDER BY id;",
      "SELECT id, "
      "COUNT(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 "
      "PRECEDING), "
      "SUM(DISTINCT v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 "
      "FOLLOWING) "
      "FROM t ORDER BY id;",
      "SELECT id, "
      "COUNT(DISTINCT s) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND "
      "CURRENT ROW) "
      "FROM (SELECT id, CASE WHEN v IS NULL THEN NULL WHEN v < 0 THEN 'neg' "
      "WHEN v = 10 THEN 'ten' ELSE 'pos' END AS s FROM t) sub "
      "ORDER BY id;"};

  for (const std::string& q : queries) {
    auto eval = [&](bool disable) -> std::vector<Row> {
      if (disable) {
        ::tinylamb::platform::SetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE", "1", 1);
      } else {
        ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
      }
      PreparedPlanCache::Instance().Clear();
      CurrentCompiledPlans().clear();
      TransactionContext ctx = db->BeginReadOnlyContext();
      SqlEngine engine(*db);
      auto rows = RunSql(&engine, &ctx, q);
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
      return rows;
    };
    const auto on_rows = eval(false);
    const auto off_rows = eval(true);
    EXPECT_EQ(on_rows, off_rows) << q;
  }

  // Test EXCLUDE CURRENT ROW via AST + ApplyWindows (parser rejects EXCLUDE).
  {
    auto cnt_w = MakeWindow("COUNT", {ColumnValueExp("v")});
    cnt_w->distinct = true;
    cnt_w->order_by = {WindowOrderTerm{.expression = ColumnValueExp("id"),
                                       .ascending = true,
                                       .nulls_first = std::nullopt}};
    cnt_w->has_frame = true;
    cnt_w->frame_unit = WindowFrameUnit::kRows;
    cnt_w->frame_start = OffsetBound(WindowFrameBoundType::kOffsetPreceding, 2);
    cnt_w->frame_end = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2);
    cnt_w->exclusion = WindowFrameExclusion::kCurrentRow;

    auto sum_w = MakeWindow("SUM", {ColumnValueExp("v")});
    sum_w->distinct = true;
    sum_w->order_by = cnt_w->order_by;
    sum_w->has_frame = true;
    sum_w->frame_unit = WindowFrameUnit::kRows;
    sum_w->frame_start = cnt_w->frame_start;
    sum_w->frame_end = cnt_w->frame_end;
    sum_w->exclusion = WindowFrameExclusion::kCurrentRow;

    SelectStatement stmt({NamedExpression("id", ColumnValueExp("id")),
                          NamedExpression("c", Expression(cnt_w)),
                          NamedExpression("s", Expression(sum_w))},
                         {}, nullptr);
    const Schema schema(
        "t", {Column("id", ValueType::kInt64), Column("v", ValueType::kInt64)});
    const std::vector<Row> base_rows{
        Row({Value(1), Value(10)}), Row({Value(2), Value(10)}),
        Row({Value(3), Value()}),   Row({Value(4), Value(-5)}),
        Row({Value(5), Value(20)}), Row({Value(6), Value(10)}),
        Row({Value(7), Value(20)}), Row({Value(8), Value()})};
    auto run_excl = [&](bool disable) -> std::vector<Row> {
      if (disable) {
        ::tinylamb::platform::SetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE", "1", 1);
      } else {
        ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
      }
      TransactionContext ctx = db->BeginReadOnlyContext();
      relational_detail::Relation rel;
      rel.schema = schema;
      rel.rows = base_rows;
      auto res_or = relational_detail::ApplyWindows(ctx, stmt, std::move(rel),
                                                    nullptr, {});
      if (!res_or.HasValue()) {
        ADD_FAILURE() << res_or.GetStatus();
        return {};
      }
      std::vector<Row> out;
      EXPECT_EQ(tinylamb::Status::kSuccess,
                res_or.Value().input.ForEachRow(
                    [&](const Row& r) { out.push_back(r); }));
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
      return out;
    };
    const auto on_excl = run_excl(false);
    const auto off_excl = run_excl(true);
    EXPECT_EQ(on_excl.size(), base_rows.size());
    EXPECT_EQ(on_excl, off_excl);
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
  db->DeleteAll();
}

TEST(LeanPhase3ParityTest, WindowHaloAndDistinctOnAndOuterOrBranchesParity) {
  // The bodies below toggle these knobs repeatedly; the guards restore any
  // externally supplied values even on an early ASSERT exit.
  EnvGuard halo_env("TINYLAMB_DISABLE_WINDOW_HALO");
  EnvGuard distinct_on_env("TINYLAMB_DISABLE_DISTINCT_ON_TOP1");
  EnvGuard or_branches_env("TINYLAMB_DISABLE_OUTER_OR_BRANCHES");
  auto db_or = Database::Create("lean_p3sql_" + RandomString());
  ASSERT_EQ(db_or.GetStatus(), Status::kSuccess);
  auto db = db_or.MoveValue();
  {
    TransactionContext ctx = db->BeginContext();
    SqlEngine engine(*db);
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE w (id INT64, grp INT64, val INT64);")
            .HasValue());
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE lo (a INT64, b INT64, c INT64);")
            .HasValue());
    ASSERT_TRUE(
        engine.Execute(ctx, "CREATE TABLE ro (a INT64, b INT64, d INT64);")
            .HasValue());
    std::string inserts = "INSERT INTO w VALUES ";
    for (int i = 1; i <= 60; ++i) {
      if (i > 1) { inserts += ", ";
}
      const std::string val_str =
          (i % 9 == 0) ? "NULL" : std::to_string(((i * 17) % 50) - 15);
      inserts += "(" + std::to_string(i) + ", " + std::to_string(i % 7) + ", " +
                 val_str + ")";
    }
    inserts += ";";
    ASSERT_TRUE(engine.Execute(ctx, inserts).HasValue());
    ASSERT_TRUE(
        engine
            .Execute(ctx,
                     "INSERT INTO lo VALUES "
                     "(1, 10, 1), (NULL, 20, 1), (99, 99, 1), (2, 30, -5);")
            .HasValue());
    ASSERT_TRUE(
        engine
            .Execute(ctx,
                     "INSERT INTO ro VALUES "
                     "(1, 10, 1), (50, 20, 1), (50, 20, 2), (2, 30, 3);")
            .HasValue());
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  auto eval_sorted = [&](const std::string& q, const char* disable_var,
                         bool disable) -> std::vector<Row> {
    if (disable) {
      ::tinylamb::platform::SetEnv(disable_var, "1", 1);
    } else {
      ::tinylamb::platform::UnsetEnv(disable_var);
    }
    PreparedPlanCache::Instance().Clear();
    CurrentCompiledPlans().clear();
    TransactionContext ctx = db->BeginReadOnlyContext();
    SqlEngine engine(*db);
    auto rows = RunSql(&engine, &ctx, q);
    EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
    return rows;
  };

  // WindowHalo on direct relational ApplyWindows path (both Row-backed and
  // Columnar-backed inputs) and SQL path.
  {
    auto win = MakeWindow("AVG", {ColumnValueExp("val")});
    win->order_by = {WindowOrderTerm{.expression = ColumnValueExp("id"),
                                     .ascending = true,
                                     .nulls_first = std::nullopt}};
    win->has_frame = true;
    win->frame_unit = WindowFrameUnit::kRows;
    win->frame_start = OffsetBound(WindowFrameBoundType::kOffsetPreceding, 2);
    win->frame_end = OffsetBound(WindowFrameBoundType::kOffsetFollowing, 2);

    SelectStatement stmt({NamedExpression("id", ColumnValueExp("id")),
                          NamedExpression("a", Expression(win))},
                         {}, nullptr);
    stmt.SetOrderBy({{ColumnValueExp("id"), true, std::nullopt}});
    stmt.SetLimit(7);
    stmt.SetOffset(12);

    const Schema schema("w", {Column("id", ValueType::kInt64),
                              Column("val", ValueType::kInt64)});
    std::vector<Row> base_rows;
    base_rows.reserve(60);
    for (int i = 60; i >= 1; --i) {  // intentionally unsorted input
      base_rows.push_back(Row(
          {Value(int64_t{i}), (i % 7 == 0) ? Value() : Value(static_cast<int64_t>(i * 3))}));
    }

    auto run_apply = [&](bool disable) -> std::vector<Row> {
      if (disable) {
        ::tinylamb::platform::SetEnv("TINYLAMB_DISABLE_WINDOW_HALO", "1", 1);
      } else {
        ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_HALO");
      }
      TransactionContext ctx = db->BeginReadOnlyContext();
      relational_detail::Relation rel;
      rel.schema = schema;
      rel.rows = base_rows;
      auto win_or = relational_detail::ApplyWindows(
          ctx, stmt, std::move(rel), nullptr, {}, /*allow_window_halo=*/true);
      EXPECT_TRUE(win_or.HasValue()) << win_or.GetStatus();
      auto win_res = win_or.MoveValue();
      EXPECT_EQ(relational_detail::ApplyOrderBy(ctx, *win_res.statement,
                                                &win_res.input, nullptr, {}),
                Status::kSuccess);
      auto fin_or = relational_detail::LimitedRows(
          *win_res.statement, std::move(win_res.input), &ctx, nullptr, {});
      EXPECT_TRUE(fin_or.HasValue()) << fin_or.GetStatus();
      std::vector<Row> out;
      EXPECT_EQ(tinylamb::Status::kSuccess,
                fin_or.Value().ForEachRow(
                    [&](const Row& r) { out.push_back(r); }));
      EXPECT_EQ(tinylamb::Status::kSuccess, ctx.txn_.PreCommit());
      return out;
    };

    const auto on_halo = run_apply(false);
    const auto off_halo = run_apply(true);
    EXPECT_EQ(on_halo.size(), 7U);
    EXPECT_EQ(on_halo, off_halo);
  }
  for (const std::string& wq :
       {"SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 "
        "FOLLOWING) AS a "
        "FROM w ORDER BY id LIMIT 10 OFFSET 15;",
        "SELECT id, SUM(val) OVER (ORDER BY id DESC ROWS BETWEEN 2 PRECEDING "
        "AND 4 FOLLOWING) AS s, "
        "COUNT(val) OVER (ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND "
        "CURRENT ROW) AS c "
        "FROM w ORDER BY id DESC LIMIT 12 OFFSET 5;"}) {
    const auto on_rows = eval_sorted(wq, "TINYLAMB_DISABLE_WINDOW_HALO", false);
    const auto off_rows = eval_sorted(wq, "TINYLAMB_DISABLE_WINDOW_HALO", true);
    EXPECT_EQ(on_rows, off_rows) << wq;
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_WINDOW_HALO");

  // DistinctOnTop1 parity (ties, DESC + NULLS FIRST/LAST, ordinals, expression
  // keys).
  for (const std::string& dq :
       {"SELECT DISTINCT ON (grp) grp, id, val FROM w ORDER BY grp, val DESC "
        "NULLS LAST, id ASC;",
        "SELECT DISTINCT ON (grp) grp, id, val FROM w ORDER BY grp DESC, val "
        "ASC NULLS FIRST, id DESC;",
        "SELECT DISTINCT ON (MOD(grp, 3)) grp, id, COALESCE(val, -1) FROM w "
        "ORDER BY 1, id DESC;"}) {
    const auto on_rows =
        eval_sorted(dq, "TINYLAMB_DISABLE_DISTINCT_ON_TOP1", false);
    const auto off_rows =
        eval_sorted(dq, "TINYLAMB_DISABLE_DISTINCT_ON_TOP1", true);
    EXPECT_EQ(on_rows, off_rows) << dq;
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_DISTINCT_ON_TOP1");

  // OuterOrBranches end-to-end through SqlEngine (PipelineLowering
  // OocCrossJoinProbeSource).
  for (const std::string& jq :
       {"SELECT lo.a, lo.b, ro.a, ro.b, ro.d FROM lo LEFT JOIN ro ON lo.a = "
        "ro.a OR lo.b = ro.b;",
        "SELECT lo.a, lo.b, ro.d FROM lo INNER JOIN ro ON (lo.a = ro.a AND "
        "lo.c > 0) OR (lo.b = ro.b AND ro.d < 5);"}) {
    auto on_rows = eval_sorted(jq, "TINYLAMB_DISABLE_OUTER_OR_BRANCHES", false);
    auto off_rows = eval_sorted(jq, "TINYLAMB_DISABLE_OUTER_OR_BRANCHES", true);
    auto cmp = [](const Row& x, const Row& y) {
      return x.ToString() < y.ToString();
    };
    std::ranges::sort(on_rows, cmp);
    std::ranges::sort(off_rows, cmp);
    EXPECT_EQ(on_rows, off_rows) << jq;
  }
  ::tinylamb::platform::UnsetEnv("TINYLAMB_DISABLE_OUTER_OR_BRANCHES");

  db->DeleteAll();
}

}  // namespace tinylamb
