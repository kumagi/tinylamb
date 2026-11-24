/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Engine-level coverage for calendar-stepped GENERATE_DATE_ARRAY and
// lexicographic ARRAY comparisons. Every query runs through BOTH evaluator
// routes -- the compiled (AST) path and the forced relational fallback --
// so the two evaluators cannot drift apart silently.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// Executes the query on both routes and asserts identical single-column
// results, returned as display strings.
std::vector<std::string> RunOnBothRoutes(SqlEngine* engine,
                                         TransactionContext* context,
                                         std::string_view sql) {
  std::vector<std::string> result;
  // RAII for the route knob: every early return below must leave the engine
  // as it was found so a failing case cannot leak into later tests sharing
  // this engine.
  struct ForceRelationalGuard {  // NOLINT(cppcoreguidelines-special-member-functions):
                                 // scope-owned cleanup.
    explicit ForceRelationalGuard(SqlEngine* e)
        : engine(e), saved(e->ForceRelationalFallback()) {}
    ~ForceRelationalGuard() { engine->SetForceRelationalFallback(saved); }
    SqlEngine* engine;
    bool saved;
  } guard(engine);
  // T4446: the loop used to clear() and overwrite `result` per route and
  // return only the last (relational) run, so the documented "identical
  // results" contract was never checked -- a wrong answer on the compiled
  // (AST) route was invisible. Keep each route's rows and compare.
  for (const bool relational : {false, true}) {
    engine->SetForceRelationalFallback(relational);
    StatusOr<Executor> prepared = engine->Prepare(*context, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine->LastError();
    if (!prepared.HasValue()) {
      result.clear();
      return result;
    }
    std::vector<std::string> route_result;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      EXPECT_EQ(row.values_.size(), 1U) << "route divergence for: " << sql;
      if (row.values_.size() != 1) {
        return {};
      }
      route_result.push_back(row[0].AsString());
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
    if (!relational) {
      result = std::move(route_result);
    } else {
      EXPECT_EQ(route_result, result) << "route divergence for: " << sql;
      result = std::move(route_result);
    }
  }
  return result;
}

std::string Join(const std::vector<std::string>& elements) {
  std::string joined;
  for (const std::string& element : elements) {
    joined += element + ",";
  }
  return joined;
}

}  // namespace

class DateArrayStepTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP();
    }
    database_ =
        Database::Create("date_array_test-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    context_.reset();
    database_->DeleteAll();
    database_.reset();
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(DateArrayStepTest, MonthStepsUseClampedCalendarArithmetic) {
  // Month-end clamping: 2026-01-31 + 1 MONTH lands on 2026-02-28.
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "DATE '2026-01-31', DATE '2026-04-30', "
                                 "INTERVAL 1 MONTH)")),
            "ARRAY<DATE>[2026-01-31, 2026-02-28, 2026-03-28, 2026-04-28],");
}

TEST_F(DateArrayStepTest, QuarterAndYearSteps) {
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "DATE '2026-01-01', DATE '2026-07-01', "
                                 "INTERVAL 1 QUARTER)")),
            "ARRAY<DATE>[2026-01-01, 2026-04-01, 2026-07-01],");
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "DATE '2025-01-01', DATE '2027-01-01', "
                                 "INTERVAL 1 YEAR)")),
            "ARRAY<DATE>[2025-01-01, 2026-01-01, 2027-01-01],");
}

TEST_F(DateArrayStepTest, OvershootingMonthStepKeepsStartOnly) {
  // The 4-day window never reaches the next 2-month step.
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "DATE '2026-01-01', DATE '2026-01-05', "
                                 "INTERVAL 2 MONTH)")),
            "ARRAY<DATE>[2026-01-01],");
}

TEST_F(DateArrayStepTest, DescendingMonthSteps) {
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "DATE '2026-03-31', DATE '2026-01-31', "
                                 "INTERVAL -1 MONTH)")),
            "ARRAY<DATE>[2026-03-31, 2026-02-28],");
}

TEST_F(DateArrayStepTest, EmptyArrayWhenDirectionsDiverge) {
  // Start after the end with a positive step: empty, not an error.
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "DATE '2026-06-01', DATE '2026-01-01', "
                                 "INTERVAL 1 MONTH)")),
            "ARRAY<DATE>[],");
}

TEST_F(DateArrayStepTest, CompositeIntervalStepsFollowTheIntervalText) {
  // The batch evaluator used to decode a literal INTERVAL step from the
  // expression's Amount()/Unit() pair, which truncates compound intervals
  // to the leading field and skips the sub-field validation the AST applies.
  // Read against a table so the call executes per-row rather than being
  // constant-folded away.
  const auto run = [&](const std::string& sql) {
    StatusOr<Executor> prepared = engine_->Prepare(*context_, sql);
    ASSERT_TRUE(prepared.HasValue()) << engine_->LastError();
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    ASSERT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
  };
  run("CREATE TABLE gda_t (d1 DATE, d2 DATE)");
  run("INSERT INTO gda_t VALUES "
      "(DATE '2026-01-01', DATE '2027-06-30')");
  // "1-2" YEAR TO MONTH is a 14-month step: the leading amount alone
  // would walk 12 months and land on 2027-01-01 instead.
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "d1, d2, INTERVAL '1-2' YEAR TO MONTH) "
                                 "FROM gda_t")),
            "ARRAY<DATE>[2026-01-01, 2027-03-01],");

  // A step that mixes days with a time component has no whole-day/month
  // reading: both routes must reject it, not silently drop the time part.
  for (const bool relational : {false, true}) {
    engine_->SetForceRelationalFallback(relational);
    StatusOr<Executor> prepared = engine_->Prepare(
        *context_,
        "SELECT GENERATE_DATE_ARRAY(d1, d2, INTERVAL '2 3' DAY TO HOUR) "
        "FROM gda_t");
    bool ok = prepared.HasValue();
    if (ok) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      ok = prepared.Value()->GetStatus() == Status::kSuccess;
    }
    EXPECT_FALSE(ok) << (relational ? "relational" : "compiled")
                     << " route accepted a day+hour step";
  }
  engine_->SetForceRelationalFallback(false);

  // A pre-evaluated interval text step (make_interval call) must decode its
  // days from the parse result instead of a shadowed zero value.
  run("DELETE FROM gda_t");
  run("INSERT INTO gda_t VALUES "
      "(DATE '2026-01-01', DATE '2026-01-07')");
  EXPECT_EQ(Join(RunOnBothRoutes(engine_.get(), context_.get(),
                                 "SELECT GENERATE_DATE_ARRAY("
                                 "d1, d2, MAKE_INTERVAL(3, 'DAY')) "
                                 "FROM gda_t")),
            "ARRAY<DATE>[2026-01-01, 2026-01-04, 2026-01-07],");
}

class ArrayCompareTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP();
    }
    database_ =
        Database::Create("array_compare_test-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    context_.reset();
    database_->DeleteAll();
    database_.reset();
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ArrayCompareTest, LexicographicElementwiseComparison) {
  // First differing element decides.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2] < ARRAY[1, 3]"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2] > ARRAY[1, 1]"),
            (std::vector<std::string>{"1"}));
  // A shared prefix makes the longer array greater.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2] < ARRAY[1, 2, 0]"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2, 0] > ARRAY[1, 2]"),
            (std::vector<std::string>{"1"}));
  // Equal arrays compare false on both operators.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[4, 5] < ARRAY[4, 5]"),
            (std::vector<std::string>{"0"}));
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[4, 5] > ARRAY[4, 5]"),
            (std::vector<std::string>{"0"}));
}

// A column-vs-array-literal predicate folds into a constant comparison. The
// scan filter must decline to compile type pairs with no comparison payload
// (arrays here) instead of rejecting every row.
TEST_F(ArrayCompareTest, ArrayEqualityPredicateSurvivesScanFilter) {
  const auto run = [&](const std::string& sql) {
    StatusOr<Executor> prepared = engine_->Prepare(*context_, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine_->LastError();
    if (!prepared.HasValue()) {
      return;
    }
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
  };
  run("CREATE TABLE arr_scan_t (id INT64, a ARRAY<INT64>)");
  run("INSERT INTO arr_scan_t VALUES (1, [1, 2]), (2, [3, 4])");
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT id FROM arr_scan_t WHERE a = [1, 2]"),
            (std::vector<std::string>{"1"}));
}

TEST_F(ArrayCompareTest, NullElementPropagationMatchesNullSemantics) {
  // Ordered array comparison is element-wise lexicographic through
  // Value::TryLess: two NULL elements at the same position compare equal
  // (NULL == NULL) and the scan moves on, so a NULL prefix does not decide
  // the result by itself.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[NULL] < ARRAY[NULL]"),
            (std::vector<std::string>{"0"}));
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[NULL, 2] < ARRAY[NULL, 3]"),
            (std::vector<std::string>{"1"}));
  // A shared prefix that includes NULLs still lets the longer array win.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, NULL] < ARRAY[1, NULL, 0]"),
            (std::vector<std::string>{"1"}));
  // A NULL element compared against a non-NULL element is incomparable:
  // TryLess reports a type mismatch.  The error may surface at Prepare
  // (constant folding) or during execution; both routes must agree it is
  // not a successful result.
  for (const bool relational : {false, true}) {
    engine_->SetForceRelationalFallback(relational);
    StatusOr<Executor> prepared =
        engine_->Prepare(*context_, "SELECT ARRAY[NULL] < ARRAY[1]");
    bool ok = prepared.HasValue();
    if (ok) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
      }
      ok = prepared.Value()->GetStatus() == Status::kSuccess;
    }
    EXPECT_FALSE(ok);
  }
  engine_->SetForceRelationalFallback(false);
}

}  // namespace tinylamb
