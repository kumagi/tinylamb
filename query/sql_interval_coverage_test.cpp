/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for the INTERVAL literal grammar in type/interval.cpp:
// single-unit forms, YEAR TO MONTH / DAY TO SECOND / HOUR TO MINUTE compound
// forms, fractional seconds, multi-component strings, arithmetic,
// preservation of very large values without clamping, and the lenient
// fallback for malformed components.

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
  if (!result.HasValue() && engine->LastErrorIsTransientParseFailure()) {
    result = engine->Execute(*ctx, sql);
  }
  if (!result.HasValue()) {
    throw std::runtime_error(engine->LastError().empty()
                                 ? result.GetStatus().GetMessage()
                                 : engine->LastError());
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
  void ExpectThrow(const std::string& sql) {
    SCOPED_TRACE(sql);
    EXPECT_THROW(RunSql(engine_.get(), context_.get(), sql), std::exception);
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlIntervalCoverageTest, SingleUnitForms) {
  ExpectScalar("SELECT INTERVAL '1' YEAR", Value("1-0 0 0:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '2' MONTH",
               Value("0-2 0 0:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '5' DAY", Value("0-0 5 0:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '6' HOUR", Value("0-0 0 6:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '7' MINUTE",
               Value("0-0 0 0:7:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '8' SECOND",
               Value("0-0 0 0:0:8").WithInterval());
}

TEST_F(SqlIntervalCoverageTest, CompoundUnitForms) {
  ExpectScalar("SELECT INTERVAL '1-2' YEAR TO MONTH",
               Value("1-2 0 0:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '1 2:3:4' DAY TO SECOND",
               Value("0-0 1 2:3:4").WithInterval());
  ExpectScalar("SELECT INTERVAL '3:4' HOUR TO MINUTE",
               Value("0-0 0 3:4:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '4:5:6.789' HOUR TO SECOND",
               Value("0-0 0 4:5:6.789").WithInterval());
  ExpectScalar("SELECT INTERVAL '2 3' DAY TO HOUR",
               Value("0-0 2 3:0:0").WithInterval());
}

TEST_F(SqlIntervalCoverageTest, IntervalArithmetic) {
  ExpectScalar("SELECT INTERVAL '1' DAY + INTERVAL '2' HOUR",
               Value("0-0 1 2:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '1-2' YEAR TO MONTH + INTERVAL '3' MONTH",
               Value("1-5 0 0:0:0").WithInterval());
  ExpectScalar("SELECT INTERVAL '1' SECOND + INTERVAL '0.5' SECOND",
               Value("0-0 0 0:0:1.5").WithInterval());
  // make_interval is exactly (value, unit); extra arguments are rejected.
  ExpectScalar("SELECT make_interval(1, 'DAY')",
               Value("0-0 1 0:0:0").WithInterval());
  // T2233: the GoogleSQL named-argument spelling desugars into the same
  // (value, unit) calls joined by interval addition.
  ExpectScalar("SELECT MAKE_INTERVAL(day => 4)",
               Value("0-0 4 0:0:0").WithInterval());
  ExpectScalar("SELECT MAKE_INTERVAL(second => -6)",
               Value("0-0 0 -0:0:6").WithInterval());
  ExpectScalar("SELECT MAKE_INTERVAL(hour => -4, day => 3)",
               Value("0-0 3 -4:0:0").WithInterval());
  ExpectScalar("SELECT MAKE_INTERVAL(year => 1, month => 2)",
               Value("1-2 0 0:0:0").WithInterval());
  // Positional arguments fill the year..second slots in order; mixing them
  // ahead of named ones is allowed, after a named one is not.
  ExpectScalar("SELECT MAKE_INTERVAL(1, month => 2)",
               Value("1-2 0 0:0:0").WithInterval());
  ExpectThrow("SELECT MAKE_INTERVAL(day => 4, 2)");
  ExpectThrow("SELECT MAKE_INTERVAL(bogus => 1)");
}

TEST_F(SqlIntervalCoverageTest, LenientAndExtremeComponents) {
  // A malformed component is a GoogleSQL error, not a zero interval.
  ExpectThrow("SELECT INTERVAL 'xx' DAY");
  // Huge year counts parse without overflowing the month accumulator.
  ExpectScalar("SELECT INTERVAL '9999999999999' YEAR",
               Value("9999999999999-0 0 0:0:0").WithInterval());
}

}  // namespace tinylamb
