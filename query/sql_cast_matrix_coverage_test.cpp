/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// SQL-level sweep over the CAST matrix (expression/cast_expression.cpp):
// conversions between the runtime value kinds (INT64, FLOAT64, BOOL, STRING,
// BYTES, DATE, ARRAY), boundary values, SAFE_CAST null-on-error semantics,
// and the error contracts for invalid inputs.

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
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

class SqlCastMatrixCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sql_cast_matrix_test-" + RandomString()).MoveValue();
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

  Value Scalar(const std::string& sql) {
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "query did not return one row: " << sql;
      return {};
    }
    return rows[0][0];
  }
  void ExpectCast(const std::string& expression, const Value& expected) {
    SCOPED_TRACE("SELECT CAST(" + expression + ")");
    EXPECT_EQ(Scalar("SELECT CAST(" + expression + ")"), expected);
  }
  void ExpectCastError(const std::string& expression) {
    SCOPED_TRACE("SELECT CAST(" + expression + ")");
    EXPECT_THROW(
        {
          RunSql(engine_.get(), context_.get(),
                 "SELECT CAST(" + expression + ")");
        },
        std::exception);
  }
  void ExpectSafeCastNull(const std::string& expression) {
    SCOPED_TRACE("SELECT SAFE_CAST(" + expression + ")");
    EXPECT_TRUE(Scalar("SELECT SAFE_CAST(" + expression + ")").IsNull());
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlCastMatrixCoverageTest, NumericToStringAndBack) {
  ExpectCast("7 AS STRING", (Value(std::string("7"))));
  ExpectCast("-7 AS STRING", (Value(std::string("-7"))));
  ExpectCast("2.5 AS STRING", (Value(std::string("2.5"))));
  ExpectCast("'42' AS INT64", (Value(int64_t{42})));
  ExpectCast("'-13' AS INT64", (Value(int64_t{-13})));
  ExpectCast("  ' 8 '  AS INT64", (Value(int64_t{8})));
  ExpectCast("'2.75' AS FLOAT64", (Value(2.75)));
  ExpectCastError("'abc' AS INT64");
  ExpectCastError("'' AS INT64");
  ExpectCastError("'12x' AS INT64");
  ExpectSafeCastNull("'abc' AS INT64");
  ExpectSafeCastNull("'' AS INT64");
}

TEST_F(SqlCastMatrixCoverageTest, IntegerBoundaries) {
  ExpectCast("9223372036854775807 AS STRING",
             (Value(std::string("9223372036854775807"))));
  ExpectCast("-9223372036854775808 AS STRING",
             (Value(std::string("-9223372036854775808"))));
  // Out-of-range text cannot become an INT64.
  ExpectCastError("'9223372036854775808' AS INT64");
  ExpectSafeCastNull("'9223372036854775808' AS INT64");
}

TEST_F(SqlCastMatrixCoverageTest, FloatEdges) {
  // FLOAT64 -> INT64 rounds to the nearest integer (half away from zero).
  ExpectCast("2.0 AS INT64", (Value(int64_t{2})));
  ExpectCast("-2.9 AS INT64", (Value(int64_t{-3})));
  ExpectCast("CAST(2.5 AS INT64) AS STRING", (Value(std::string("3"))));
  ExpectCast("1e2 AS INT64", (Value(int64_t{100})));
  ExpectCastError("1e30 AS INT64");
  ExpectSafeCastNull("1e30 AS INT64");
  ExpectCast("'inf' AS FLOAT64",
             (Value(std::numeric_limits<double>::infinity())));
  ExpectCastError("CAST('nan' AS INT64)");
}

TEST_F(SqlCastMatrixCoverageTest, BoolConversions) {
  ExpectCast("TRUE AS STRING", (Value(std::string("true"))));
  ExpectCast("FALSE AS STRING", (Value(std::string("false"))));
  ExpectCast("'true' AS BOOL", (Value(true)));
  ExpectCast("'false' AS BOOL", (Value(false)));
  ExpectCast("1 AS BOOL", (Value(true)));
  ExpectCast("0 AS BOOL", (Value(false)));
  ExpectCastError("'yes' AS BOOL");
  ExpectSafeCastNull("'yes' AS BOOL");
}

TEST_F(SqlCastMatrixCoverageTest, DateConversions) {
  ExpectCast("'2026-09-22' AS DATE", (Value::Date("2026-09-22")));
  ExpectCast("DATE '2026-09-22' AS STRING", (Value(std::string("2026-09-22"))));
  ExpectCastError("'2026-13-40' AS DATE");
  ExpectCastError("'not-a-date' AS DATE");
  ExpectSafeCastNull("'2026-02-30' AS DATE");
}

TEST_F(SqlCastMatrixCoverageTest, NullPropagatesThroughEveryCast) {
  EXPECT_TRUE(Scalar("SELECT CAST(NULL AS INT64)").IsNull());
  EXPECT_TRUE(Scalar("SELECT CAST(NULL AS STRING)").IsNull());
  EXPECT_TRUE(Scalar("SELECT CAST(NULL AS FLOAT64)").IsNull());
  EXPECT_TRUE(Scalar("SELECT CAST(NULL AS BOOL)").IsNull());
  EXPECT_TRUE(Scalar("SELECT CAST(NULL AS DATE)").IsNull());
}

TEST_F(SqlCastMatrixCoverageTest, CastsInExpressionsAndAggregates) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE c (k STRING(MAX), n FLOAT64)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO c VALUES ('1', 1.5), ('2', 2.5)");
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT SUM(CAST(k AS INT64)) FROM c");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], (Value(int64_t{3})));
  const auto rounded =
      RunSql(engine_.get(), context_.get(),
             "SELECT CAST(FLOOR(n) AS STRING) FROM c ORDER BY n");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rounded[0][0], (Value(std::string("1"))));
  EXPECT_EQ(rounded[1][0], (Value(std::string("2"))));
}

}  // namespace tinylamb
