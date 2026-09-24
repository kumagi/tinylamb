/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for scalar functions implemented only in the relational AST
// evaluator (executor/detail/expression_eval.cpp TryEvaluateFunction): the
// EXTRACT part family, date_trunc, date_bucket, DATE(), array_first, and the
// safe_* overflow family.  Expectations were pinned against the engine.

#include <gtest/gtest.h>

#include <cstdint>
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

class SqlRelationalEvalCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sql_relational_eval_coverage_test-" + RandomString())
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

TEST_F(SqlRelationalEvalCoverageTest, ExtractPartFamily) {
  ExpectScalar("SELECT extract_week(DATE '2026-01-01')", Value(int64_t{0}));
  ExpectScalar("SELECT extract_dayofweek(DATE '2026-09-18')",
               Value(int64_t{6}));
  ExpectScalar("SELECT extract_dayofyear(DATE '2026-09-18')",
               Value(int64_t{261}));
  ExpectScalar("SELECT extract_isoyear(DATE '2026-01-01')",
               Value(int64_t{2026}));
  ExpectScalar("SELECT extract_hour(TIMESTAMP '2026-09-18 05:06:07+00')",
               Value(int64_t{5}));
  ExpectScalar("SELECT extract_minute(TIMESTAMP '2026-09-18 05:06:07+00')",
               Value(int64_t{6}));
  ExpectScalar("SELECT extract_second(TIMESTAMP '2026-09-18 05:06:07+00')",
               Value(int64_t{7}));
  ExpectScalar(
      "SELECT extract_millisecond(TIMESTAMP '2026-09-18 05:06:07.123456+00')",
      Value(int64_t{123}));
  ExpectScalar(
      "SELECT extract_microsecond(TIMESTAMP '2026-09-18 05:06:07.123456+00')",
      Value(int64_t{123456}));
  ExpectScalar(
      "SELECT extract_nanosecond(TIMESTAMP '2026-09-18 05:06:07.123456789+00')",
      Value(int64_t{123456789}));
}

TEST_F(SqlRelationalEvalCoverageTest, DateTruncAndDateBucket) {
  ExpectScalar("SELECT date_trunc(DATE '2026-09-18', 'MONTH')",
               Value::Date("2026-09-01"));
  ExpectScalar("SELECT date_trunc(DATE '2026-09-18', 'YEAR')",
               Value::Date("2026-01-01"));
  ExpectScalar("SELECT date_bucket(DATE '2026-09-18', INTERVAL '1' MONTH)",
               Value::Date("2026-09-01"));
  ExpectScalar("SELECT date_bucket(DATE '2026-01-15', INTERVAL '1' MONTH)",
               Value::Date("2026-01-01"));
}

TEST_F(SqlRelationalEvalCoverageTest, DateAndArrayFirstHelpers) {
  ExpectScalar("SELECT DATE('2026-01-02')", Value::Date("2026-01-02"));
  ExpectScalar("SELECT array_first([4, 5, 6])", Value(int64_t{4}));
  ExpectScalar("SELECT array_first([DATE '2026-03-04'])",
               Value::Date("2026-03-04"));
  EXPECT_THROW(RunSql(engine_.get(), context_.get(), "SELECT array_first([])"),
               std::runtime_error);
}

TEST_F(SqlRelationalEvalCoverageTest, SafeMultiplyOverflow) {
  ExpectScalar("SELECT safe_multiply(3, 4)", Value(int64_t{12}));
  ExpectScalar("SELECT safe_multiply(9223372036854775807, 2) IS NULL",
               Value(true));
  ExpectScalar("SELECT safe_multiply(-3, 4)", Value(int64_t{-12}));
}

TEST_F(SqlRelationalEvalCoverageTest, LikeThroughRelationalContext) {
  RunSql(engine_.get(), context_.get(), "CREATE TABLE lk (s STRING)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO lk (s) VALUES ('abc'), ('xbc')");
  const char* kSetOp =
      "SELECT s FROM (SELECT s FROM lk UNION ALL SELECT s FROM lk) z WHERE ";
  // LIKE patterns evaluated over a set operation route through the
  // relational fallback evaluator.
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), std::string(kSetOp) + "s LIKE 'a%'")
          .size(),
      2U);
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), std::string(kSetOp) + "s LIKE 'x%'")
          .size(),
      2U);
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   std::string(kSetOp) + "s LIKE '_bc'")
                .size(),
            4U);
  EXPECT_EQ(RunSql(engine_.get(), context_.get(),
                   std::string(kSetOp) + "s LIKE 'a__'")
                .size(),
            2U);
  EXPECT_EQ(
      RunSql(engine_.get(), context_.get(), std::string(kSetOp) + "s LIKE '%'")
          .size(),
      4U);
}

}  // namespace tinylamb
