/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

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
  return rows;
}
}  // namespace

class SqlUdfTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
    }
    database_ = std::make_unique<Database>("sql_udf_test");
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

TEST_F(SqlUdfTest, NullaryScalarFunction) {
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP FUNCTION UdfThousand() AS ( 1000 )"));
  const auto rows =
      RunSql(engine_.get(), context_.get(), "SELECT UdfThousand() + 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1001}));
}

TEST_F(SqlUdfTest, TypedParameterAndArgumentsEvaluatedOncePerRow) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP FUNCTION UdfDoubleX(v INT64) AS ( v * 2 )"));
  // Column arguments rebind per row.
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT UdfDoubleX(a) FROM UNNEST([1, 2, 3]) AS a");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
  EXPECT_EQ(rows[2][0], Value(int64_t{6}));
}

TEST_F(SqlUdfTest, DefaultParameterValueUsedWhenOmitted) {
  EXPECT_NO_THROW(RunSql(
      engine_.get(), context_.get(),
      "CREATE TEMP FUNCTION UdfWithDefault(x INT64 DEFAULT 7) AS ( x + 1 )"));
  const auto omitted =
      RunSql(engine_.get(), context_.get(), "SELECT UdfWithDefault()");
  ASSERT_EQ(omitted.size(), 1U);
  EXPECT_EQ(omitted[0][0], Value(int64_t{8}));
  const auto supplied =
      RunSql(engine_.get(), context_.get(), "SELECT UdfWithDefault(10)");
  ASSERT_EQ(supplied.size(), 1U);
  EXPECT_EQ(supplied[0][0], Value(int64_t{11}));
}

TEST_F(SqlUdfTest, WrongArityIsRejected) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP FUNCTION UdfUnaryOnly(a INT64) AS ( a )"));
  bool threw = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT UdfUnaryOnly()");
  } catch (const std::exception&) {
    threw = true;
  }
  EXPECT_TRUE(threw);
}

TEST_F(SqlUdfTest, ScalarCallingScalarFunction) {
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP FUNCTION UdfBase() AS ( 5 )"));
  EXPECT_NO_THROW(RunSql(
      engine_.get(), context_.get(),
      "CREATE TEMP FUNCTION UdfTimesBase(v INT64) AS ( v * UdfBase() )"));
  const auto rows =
      RunSql(engine_.get(), context_.get(), "SELECT UdfTimesBase(4)");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{20}));
}

TEST_F(SqlUdfTest, CorrelatedSubqueryBodySeesParameter) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP FUNCTION UdfIdentityCorrelated(v STRING) AS "
             "( (SELECT v) )"));
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT UdfIdentityCorrelated('z') UNION ALL SELECT "
                           "UdfIdentityCorrelated('y')");
  ASSERT_EQ(rows.size(), 2U);
}

TEST_F(SqlUdfTest, TemplatedParameterAcceptsAnyType) {
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP FUNCTION UdfIsNullT(v ANY TYPE) AS "
                         "( v IS NULL )"));
  const auto null_case =
      RunSql(engine_.get(), context_.get(), "SELECT UdfIsNullT(NULL)");
  ASSERT_EQ(null_case.size(), 1U);
  EXPECT_EQ(null_case[0][0], Value(int64_t{1}));
  const auto non_null =
      RunSql(engine_.get(), context_.get(), "SELECT UdfIsNullT(3)");
  ASSERT_EQ(non_null.size(), 1U);
  EXPECT_EQ(non_null[0][0], Value(int64_t{0}));
}

TEST_F(SqlUdfTest, AggregateFunctionSplicesIntoQuery) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP AGGREGATE FUNCTION UdaSumPlusOne(v INT64) AS "
             "( SUM(v) + 1 )"));
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT UdaSumPlusOne(a) FROM UNNEST([1, 2, 3]) AS a");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{7}));
}

TEST_F(SqlUdfTest, AggregateFunctionRespectsGrouping) {
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP AGGREGATE FUNCTION UdaCountAll() AS "
                         "( COUNT(*) )"));
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT k, UdaCountAll() FROM "
      "(SELECT 1 AS k UNION ALL SELECT 1 UNION ALL SELECT 2) GROUP BY k");
  ASSERT_EQ(rows.size(), 2U);
}

TEST_F(SqlUdfTest, RecursiveDefinitionFailsInsteadOfHanging) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP FUNCTION UdfLoop(x INT64) AS ( UdfLoop(x) )"));
  bool threw = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT UdfLoop(1)");
  } catch (const std::exception&) {
    threw = true;
  }
  EXPECT_TRUE(threw);
}

TEST_F(SqlUdfTest, CreateTableFunctionStatementPrepares) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TABLE FUNCTION UdfTvf(x INT64) RETURNS TABLE<a "
             "INT64> AS ( SELECT x AS a )"));
}

TEST_F(SqlUdfTest, TempViewResolvesThroughFromClause) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP VIEW UdfTestView AS SELECT 41 AS answer"));
  const auto rows =
      RunSql(engine_.get(), context_.get(), "SELECT * FROM UdfTestView");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{41}));
}

TEST_F(SqlUdfTest, TempViewBodyExpandsUdfCalls) {
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP FUNCTION UdfTwo() AS ( 2 )"));
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP VIEW UdfTwoView AS SELECT UdfTwo() + 40 AS n"));
  const auto rows =
      RunSql(engine_.get(), context_.get(), "SELECT * FROM UdfTwoView");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{42}));
}

TEST_F(SqlUdfTest, RedefinitionReplacesPreviousBody) {
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP FUNCTION UdfVersioned() AS ( 1 )"));
  const auto first =
      RunSql(engine_.get(), context_.get(), "SELECT UdfVersioned()");
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(first[0][0], Value(int64_t{1}));
  EXPECT_NO_THROW(RunSql(engine_.get(), context_.get(),
                         "CREATE TEMP FUNCTION UdfVersioned() AS ( 2 )"));
  const auto second =
      RunSql(engine_.get(), context_.get(), "SELECT UdfVersioned()");
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0][0], Value(int64_t{2}));
}

TEST_F(SqlUdfTest, StructConstructorInsideFunctionBindsParameter) {
  EXPECT_NO_THROW(
      RunSql(engine_.get(), context_.get(),
             "CREATE TEMP FUNCTION UdfPair(a ANY STRING) AS ( (a, a) )"));
  const auto rows =
      RunSql(engine_.get(), context_.get(), "SELECT UdfPair(\"A\")");
  ASSERT_EQ(rows.size(), 1U);
  // Struct values are encoded as JSON objects with positional field names.
  const std::string text = rows[0][0].AsString();
  EXPECT_NE(text.find("\"f1\":\"A\""), std::string::npos);
  EXPECT_NE(text.find("\"f2\":\"A\""), std::string::npos);
}

TEST_F(SqlUdfTest, UndefinedFunctionStillFails) {
  bool threw = false;
  try {
    RunSql(engine_.get(), context_.get(), "SELECT UdfNeverDefined()");
  } catch (const std::exception&) {
    threw = true;
  }
  EXPECT_TRUE(threw);
}

}  // namespace tinylamb
