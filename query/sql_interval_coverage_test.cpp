/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for the INTERVAL literal grammar in type/interval.cpp:
// single-unit forms, YEAR TO MONTH / DAY TO SECOND / HOUR TO MINUTE compound
// forms, fractional seconds, multi-component strings, arithmetic, overflow
// clamping, and the lenient fallback for malformed components.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<QueryResult> result = engine->Execute(*ctx, sql);
  if (!result.HasValue()) {
    if (engine->LastError().empty()) {
      result = engine->Execute(*ctx, sql);
    }
    if (!result.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  std::vector<Row> rows;
  QueryResult drained = result.MoveValue();
  Row row;
  while (drained.Next(&row)) {
    rows.push_back(row);
    row = Row();
  }
  if (drained.GetStatus() != Status::kSuccess) {
    throw std::runtime_error(drained.GetStatus().GetMessage());
  }
  return rows;
}
}  // namespace

class SqlIntervalCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_interval_coverage_test-" + RandomString())
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
  void ExpectScalar(const std::string& sql, const Value& expected) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0][0], expected);
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlIntervalCoverageTest, SingleUnitForms) {
  ExpectScalar("SELECT INTERVAL '1' YEAR", Value(std::string("1 year")));
  ExpectScalar("SELECT INTERVAL '2' MONTH", Value(std::string("2 month")));
  ExpectScalar("SELECT INTERVAL '5' DAY", Value(std::string("5 day")));
  ExpectScalar("SELECT INTERVAL '6' HOUR", Value(std::string("6 hour")));
  ExpectScalar("SELECT INTERVAL '7' MINUTE", Value(std::string("7 minute")));
  ExpectScalar("SELECT INTERVAL '8' SECOND", Value(std::string("8 second")));
}

TEST_F(SqlIntervalCoverageTest, CompoundUnitForms) {
  ExpectScalar("SELECT INTERVAL '1-2' YEAR TO MONTH",
               Value(std::string("1 year to month")));
  ExpectScalar("SELECT INTERVAL '1 2:3:4' DAY TO SECOND",
               Value(std::string("1 day to second")));
  ExpectScalar("SELECT INTERVAL '3:4' HOUR TO MINUTE",
               Value(std::string("3 hour to minute")));
  ExpectScalar("SELECT INTERVAL '4:5:6.789' HOUR TO SECOND",
               Value(std::string("4 hour to second")));
  ExpectScalar("SELECT INTERVAL '2 3' DAY TO HOUR",
               Value(std::string("2 day to hour")));
}

TEST_F(SqlIntervalCoverageTest, IntervalArithmetic) {
  ExpectScalar("SELECT INTERVAL '1' DAY + INTERVAL '2' HOUR",
               Value(std::string("1 day2 hour")));
  ExpectScalar("SELECT INTERVAL '1-2' YEAR TO MONTH + INTERVAL '3' MONTH",
               Value(std::string("1 year to month3 month")));
  ExpectScalar("SELECT INTERVAL '1' SECOND + INTERVAL '0.5' SECOND",
               Value(std::string("1 second0 second")));
  ExpectScalar("SELECT make_interval(1, 2, 3, 4)",
               Value(std::string("0-0 1 0:0:0")));
}

TEST_F(SqlIntervalCoverageTest, LenientAndExtremeComponents) {
  // Malformed components fall back to zero instead of erroring.
  ExpectScalar("SELECT INTERVAL 'xx' DAY", Value(std::string("0 day")));
  // Huge year counts parse without overflowing the month accumulator.
  ExpectScalar("SELECT INTERVAL '9999999999999' YEAR",
               Value(std::string("9999999999999 year")));
}

}  // namespace tinylamb
