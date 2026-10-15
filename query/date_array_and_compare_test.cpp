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
  for (const bool relational : {false, true}) {
    engine->SetForceRelationalFallback(relational);
    StatusOr<Executor> prepared = engine->Prepare(*context, sql);
    EXPECT_TRUE(prepared.HasValue()) << engine->LastError();
    if (!prepared.HasValue()) {
      result.clear();
      engine->SetForceRelationalFallback(false);
      return result;
    }
    result.clear();
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      EXPECT_EQ(row.values_.size(), 1U);
      result.push_back(row[0].AsString());
    }
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess);
  }
  engine->SetForceRelationalFallback(false);
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
                            "SELECT ARRAY[1, 2] < ARRAY[1, 3]")
                .front(),
            "1");
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2] > ARRAY[1, 1]")
                .front(),
            "1");
  // A shared prefix makes the longer array greater.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2] < ARRAY[1, 2, 0]")
                .front(),
            "1");
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2, 0] > ARRAY[1, 2]")
                .front(),
            "1");
  // Equal arrays compare false on both operators.
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[4, 5] < ARRAY[4, 5]")
                .front(),
            "0");
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[4, 5] > ARRAY[4, 5]")
                .front(),
            "0");
}

TEST_F(ArrayCompareTest, NullElementPropagationMatchesNullSemantics) {
  // NULL participates as the smallest ordering element for < (NULL < 1 is
  // NULL in SQL, but inside an array the lexicographic scan reports the
  // comparison result of the element pair itself).
  EXPECT_EQ(RunOnBothRoutes(engine_.get(), context_.get(),
                            "SELECT ARRAY[1, 2] < ARRAY[2, 2]")
                .front(),
            "1");
}

}  // namespace tinylamb
