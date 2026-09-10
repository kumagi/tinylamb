/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
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
}  // namespace

class SqlUdfTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("sql_udf_test").MoveValue();
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

// ---------------------------------------------------------------------------
// Window functions through the SQL surface: ranking, offsets, frames, and
// the aggregate-over-frame family.
// ---------------------------------------------------------------------------

class WindowSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("window_sql-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE w (g INT64, k INT64, v INT64, s VARCHAR(10));");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO w VALUES (1, 1, 10, 'a'), (1, 1, 20, 'b'), "
           "(1, 2, 30, 'c'), (1, 4, 40, 'd'), (2, 5, 50, 'e');");
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

TEST_F(WindowSqlTest, RankingFamilyMatchesPeerGroups) {
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY k), "
             "RANK() OVER (PARTITION BY g ORDER BY k), "
             "DENSE_RANK() OVER (PARTITION BY g ORDER BY k), "
             "PERCENT_RANK() OVER (PARTITION BY g ORDER BY k), "
             "CUME_DIST() OVER (PARTITION BY g ORDER BY k) "
             "FROM w ORDER BY g, k, v;");
  ASSERT_EQ(rows.size(), 5U);
  // g=1 has a peer group on k=1: ranks 1,1,3,4; dense 1,1,2,3.
  EXPECT_EQ(rows[0],
            (Row({Value(1), Value(1), Value(1), Value(0.0), Value(0.5)})));
  EXPECT_EQ(rows[1],
            (Row({Value(2), Value(1), Value(1), Value(0.0), Value(0.5)})));
  EXPECT_EQ(rows[2][2], Value(2));
  EXPECT_DOUBLE_EQ(rows[2][3].value.double_value, 2.0 / 3.0);
  EXPECT_DOUBLE_EQ(rows[2][4].value.double_value, 0.75);
  EXPECT_EQ(rows[3],
            (Row({Value(4), Value(4), Value(3), Value(1.0), Value(1.0)})));
  // Single-row partition: percent_rank denominator collapses to 1.
  EXPECT_EQ(rows[4],
            (Row({Value(1), Value(1), Value(1), Value(0.0), Value(1.0)})));
}

TEST_F(WindowSqlTest, NtileLagLeadAcrossPartitions) {
  const auto rows = RunSql(engine_.get(), context_.get(),
                           "SELECT NTILE(3) OVER (PARTITION BY g ORDER BY k), "
                           "LAG(v) OVER (PARTITION BY g ORDER BY k), "
                           "LAG(v, 2, 0) OVER (PARTITION BY g ORDER BY k), "
                           "LEAD(v) OVER (PARTITION BY g ORDER BY k) "
                           "FROM w ORDER BY g, k, v;");
  ASSERT_EQ(rows.size(), 5U);
  // 4 rows into 3 buckets -> sizes 2, 1, 1.
  EXPECT_EQ(rows[0], (Row({Value(1), Value(), Value(0), Value(20)})));
  EXPECT_EQ(rows[1], (Row({Value(1), Value(10), Value(0), Value(30)})));
  EXPECT_EQ(rows[2], (Row({Value(2), Value(20), Value(10), Value(40)})));
  EXPECT_EQ(rows[3], (Row({Value(3), Value(30), Value(20), Value()})));
  EXPECT_EQ(rows[4], (Row({Value(1), Value(), Value(0), Value()})));
}

TEST_F(WindowSqlTest, RowOffsetFrameSlidesAcrossPeers) {
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING "
      "AND 1 FOLLOWING), "
      "AVG(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING "
      "AND 1 FOLLOWING), "
      "MIN(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING "
      "AND 1 FOLLOWING), "
      "MAX(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING "
      "AND 1 FOLLOWING), "
      "COUNT(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING "
      "AND 1 FOLLOWING) "
      "FROM w ORDER BY g, k, v;");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0],
            (Row({Value(30), Value(15.0), Value(10), Value(20), Value(2)})));
  EXPECT_EQ(rows[1],
            (Row({Value(60), Value(20.0), Value(10), Value(30), Value(3)})));
  EXPECT_EQ(rows[2],
            (Row({Value(90), Value(30.0), Value(20), Value(40), Value(3)})));
  EXPECT_EQ(rows[3],
            (Row({Value(70), Value(35.0), Value(30), Value(40), Value(2)})));
  EXPECT_EQ(rows[4],
            (Row({Value(50), Value(50.0), Value(50), Value(50), Value(1)})));
}

TEST_F(WindowSqlTest, RangePeersAndOffsetFollowing) {
  // Default RANGE frame includes the current row's peers.
  const auto cum = RunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (PARTITION BY g ORDER BY k) FROM w ORDER BY g, k, "
      "v;");
  ASSERT_EQ(cum.size(), 5U);
  EXPECT_EQ(cum[0], Row({Value(30)}));  // peers k=1 share the frame
  EXPECT_EQ(cum[1], Row({Value(30)}));
  EXPECT_EQ(cum[2], Row({Value(60)}));
  EXPECT_EQ(cum[3], Row({Value(100)}));

  const auto following = RunSql(
      engine_.get(), context_.get(),
      "SELECT SUM(v) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN CURRENT "
      "ROW AND 1 FOLLOWING) FROM w WHERE g = 1 ORDER BY k, v;");
  ASSERT_EQ(following.size(), 4U);
  // RANGE bounds are peer-aware on both sides: the k=1 peers share the
  // frame k in [1, 2], and the end bound reaches key + 1 (k=4 is out).
  EXPECT_EQ(following[0], Row({Value(60)}));
  EXPECT_EQ(following[1], Row({Value(60)}));
  EXPECT_EQ(following[2], Row({Value(30)}));
  EXPECT_EQ(following[3], Row({Value(40)}));
}

TEST_F(WindowSqlTest, CountFamilyOverPartition) {
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT COUNT(*) OVER (PARTITION BY g), COUNT(v) OVER (PARTITION BY g), "
      "COUNT(DISTINCT v) OVER (PARTITION BY g), "
      "COUNTIF(v > 15) OVER (PARTITION BY g), "
      "APPROX_COUNT_DISTINCT(v) OVER (PARTITION BY g) "
      "FROM w ORDER BY g, k, v;");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0], (Row({Value(4), Value(4), Value(4), Value(3), Value(4)})));
  EXPECT_EQ(rows[4], (Row({Value(1), Value(1), Value(1), Value(1), Value(1)})));
}

TEST_F(WindowSqlTest, BitwiseLogicalAndStringAggOverFrame) {
  const auto bits = RunSql(
      engine_.get(), context_.get(),
      "SELECT BIT_AND(v) OVER (PARTITION BY g), BIT_OR(v) OVER (PARTITION "
      "BY g), BIT_XOR(v) OVER (PARTITION BY g), "
      "LOGICAL_AND(v > 0) OVER (PARTITION BY g), "
      "LOGICAL_OR(v > 35) OVER (PARTITION BY g) "
      "FROM w ORDER BY g, k, v;");
  ASSERT_EQ(bits.size(), 5U);
  EXPECT_EQ(bits[0],
            (Row({Value(0), Value(62), Value(40), Value(true), Value(true)})));
  EXPECT_EQ(bits[4],
            (Row({Value(50), Value(50), Value(50), Value(true), Value(true)})));

  const auto strings = RunSql(
      engine_.get(), context_.get(),
      "SELECT STRING_AGG(s, ',') OVER (PARTITION BY g ORDER BY k, v RANGE "
      "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w WHERE g = 1 "
      "ORDER BY k, v;");
  ASSERT_EQ(strings.size(), 4U);
  EXPECT_EQ(strings[0], Row({Value("a")}));
  EXPECT_EQ(strings[1], Row({Value("a,b")}));
  EXPECT_EQ(strings[2], Row({Value("a,b,c")}));
  EXPECT_EQ(strings[3], Row({Value("a,b,c,d")}));
}

TEST_F(WindowSqlTest, VarianceCovarianceAndPercentilesOverFrame) {
  const auto rows = RunSql(
      engine_.get(), context_.get(),
      "SELECT VAR_POP(v) OVER (ORDER BY k, v), "
      "VAR_SAMP(v) OVER (ORDER BY k, v RANGE BETWEEN UNBOUNDED PRECEDING "
      "AND CURRENT ROW), "
      "COVAR_POP(v, v) OVER (ORDER BY k, v RANGE BETWEEN UNBOUNDED PRECEDING "
      "AND CURRENT ROW) "
      "FROM w ORDER BY k, v;");
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_DOUBLE_EQ(rows[0][0].value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(rows[1][0].value.double_value, 25.0);  // {10, 20}
  EXPECT_DOUBLE_EQ(rows[2][0].value.double_value, 200.0 / 3.0);
  EXPECT_DOUBLE_EQ(rows[3][0].value.double_value, 125.0);
  EXPECT_DOUBLE_EQ(rows[4][0].value.double_value, 200.0);  // {10..50}
  // COVAR_POP(x, x) equals VAR_POP(x).
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_DOUBLE_EQ(rows[i][0].value.double_value,
                     rows[i][2].value.double_value)
        << true << (i != 0U);
  }
  // VAR_SAMP is undefined for a single-row frame.
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_DOUBLE_EQ(rows[1][1].value.double_value, 50.0);

  const auto percentiles =
      RunSql(engine_.get(), context_.get(),
             "SELECT PERCENTILE_CONT(v, 0.5) OVER (PARTITION BY g), "
             "PERCENTILE_DISC(v, 0.5) OVER (PARTITION BY g) "
             "FROM w WHERE g = 1 ORDER BY k, v;");
  ASSERT_EQ(percentiles.size(), 4U);
  // Interpolated median of {10, 20, 30, 40} is 25; the discrete median
  // rounds the p*(n-1) position up (compliance-corpus convention) -> 30.
  EXPECT_DOUBLE_EQ(percentiles[0][0].value.double_value, 25.0);
  EXPECT_EQ(percentiles[0][1], Value(30));
}

TEST_F(WindowSqlTest, AnyValueNthValueEmptyWindowAndQualify) {
  const auto rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT ANY_VALUE(v) OVER (PARTITION BY g), "
             "NTH_VALUE(v, 2) OVER (PARTITION BY g ORDER BY k, v) "
             "FROM w ORDER BY g, k, v;");
  ASSERT_EQ(rows.size(), 5U);
  // NTH_VALUE(2) needs a two-row frame; the default frame grows per row.
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][1], Value(20));
  EXPECT_EQ(rows[2][1], Value(20));
  EXPECT_EQ(rows[3][1], Value(20));
  EXPECT_TRUE(rows[4][1].IsNull());
  const bool any_ok = (rows[0][0] == Value(10) || rows[0][0] == Value(20) ||
                       rows[0][0] == Value(30) || rows[0][0] == Value(40)) &&
                      rows[4][0] == Value(50);
  EXPECT_TRUE(any_ok);

  // Windows over a filtered-empty relation produce no rows.
  EXPECT_TRUE(RunSql(engine_.get(), context_.get(),
                     "SELECT SUM(v) OVER (ORDER BY k) FROM w WHERE k > 100")
                  .empty());

  // QUALIFY filters on a window aggregate after evaluation.
  const auto qualified =
      RunSql(engine_.get(), context_.get(),
             "SELECT g FROM w QUALIFY COUNT(*) OVER (PARTITION BY g) >= 4 "
             "ORDER BY g, k, v;");
  ASSERT_EQ(qualified.size(), 4U);
  for (const Row& row : qualified) {
    EXPECT_EQ(row[0], Value(1));
  }
}

// ---------------------------------------------------------------------------
// window_eval internals the GoogleSQL surface cannot spell: GROUPS frames,
// every EXCLUDE mode, and the frame-error diagnostics.  These drive
// relational_detail::ApplyWindows with hand-built statements.
// ---------------------------------------------------------------------------

namespace {

class WindowEvalDirectTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ = Database::Create("window_direct-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Runs one hand-built window function over `v` in {10, 10, 20, 30}.
  std::vector<Row> Apply(
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
                        *context_, statement, std::move(input), nullptr, {})
                        .MoveValue();
    std::vector<Row> out;
    windowed.input.ForEachRow([&](const Row& row) { out.push_back(row); });
    return out;
  }

  static std::shared_ptr<WindowFunctionCallExpression> MakeSumFrame(
      WindowFrameUnit unit, size_t preceding) {
    auto window = std::make_shared<WindowFunctionCallExpression>();
    window->function = "SUM";
    window->args = {ColumnValueExp("v")};
    window->order_by = {WindowOrderTerm{.expression = ColumnValueExp("v"),
                                        .ascending = true,
                                        .nulls_first = std::nullopt}};
    window->frame_unit = unit;
    window->has_frame = true;
    window->frame_start = {
        .type = WindowFrameBoundType::kOffsetPreceding,
        .offset = ConstantValueExp(Value(static_cast<int64_t>(preceding)))};
    window->frame_end = {.type = WindowFrameBoundType::kCurrentRow,
                         .offset = nullptr};
    return window;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

}  // namespace

TEST_F(WindowEvalDirectTest, GroupsFrameExpandsPeerGroups) {
  const auto rows = Apply(MakeSumFrame(WindowFrameUnit::kGroups, 1));
  ASSERT_EQ(rows.size(), 4U);
  // Peer group {10, 10} is indivisible: rows 0-1 see only themselves.
  EXPECT_EQ(rows[0][1], Value(20));
  EXPECT_EQ(rows[1][1], Value(20));
  EXPECT_EQ(rows[2][1], Value(40));
  EXPECT_EQ(rows[3][1], Value(50));
}

TEST_F(WindowEvalDirectTest, ExclusionModesOnFullFrame) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "SUM";
  window->args = {ColumnValueExp("v")};
  window->order_by = {WindowOrderTerm{.expression = ColumnValueExp("v"),
                                      .ascending = true,
                                      .nulls_first = std::nullopt}};
  window->frame_unit = WindowFrameUnit::kRows;
  window->has_frame = true;
  window->frame_start = {.type = WindowFrameBoundType::kUnboundedPreceding,
                         .offset = nullptr};
  window->frame_end = {.type = WindowFrameBoundType::kUnboundedFollowing,
                       .offset = nullptr};

  // Frame = whole partition {10, 10, 20, 30}, total 70.
  window->exclusion = WindowFrameExclusion::kCurrentRow;
  auto rows = Apply(window);
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][1], Value(60));  // 70 - 10 (own row)
  EXPECT_EQ(rows[2][1], Value(50));  // 70 - 20

  window->exclusion = WindowFrameExclusion::kGroup;
  rows = Apply(window);
  EXPECT_EQ(rows[0][1], Value(50));  // 70 - 20 (whole peer group)
  EXPECT_EQ(rows[2][1], Value(50));  // 70 - 20 (own singleton group)

  window->exclusion = WindowFrameExclusion::kTies;
  rows = Apply(window);
  EXPECT_EQ(rows[0][1], Value(60));  // 70 - 10 (peer, keeping self)
  EXPECT_EQ(rows[2][1], Value(70));  // singleton group keeps everything

  // A frame emptied by EXCLUDE yields NULL, not a crash.
  auto single = MakeSumFrame(WindowFrameUnit::kRows, 0);
  single->frame_start = {.type = WindowFrameBoundType::kCurrentRow,
                         .offset = nullptr};
  single->exclusion = WindowFrameExclusion::kCurrentRow;
  rows = Apply(single, {Value(10), Value(20)});
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_TRUE(rows[1][1].IsNull());
}

TEST_F(WindowEvalDirectTest, FrameErrorsSurfaceAsExceptions) {
  // ROWS offset must be a constant integer.
  auto nonconst = MakeSumFrame(WindowFrameUnit::kRows, 1);
  nonconst->frame_start.offset = ColumnValueExp("v");
  EXPECT_DEATH(Apply(nonconst), "");

  // GROUPS frames require an ORDER BY key.
  auto nogroups = MakeSumFrame(WindowFrameUnit::kGroups, 1);
  nogroups->order_by.clear();
  EXPECT_DEATH(Apply(nogroups), "");

  // RANGE offsets require exactly one ORDER BY key.
  auto tworanges = MakeSumFrame(WindowFrameUnit::kRange, 1);
  tworanges->order_by.push_back(
      WindowOrderTerm{.expression = ColumnValueExp("v"),
                      .ascending = true,
                      .nulls_first = std::nullopt});
  EXPECT_DEATH(Apply(tworanges), "");

  // NTILE needs a positive constant bucket count and NTH_VALUE needs 2 args.
  auto ntile = std::make_shared<WindowFunctionCallExpression>();
  ntile->function = "NTILE";
  ntile->args = {ConstantValueExp(Value(0))};
  EXPECT_DEATH(Apply(ntile), "");

  auto nth = std::make_shared<WindowFunctionCallExpression>();
  nth->function = "NTH_VALUE";
  nth->args = {ColumnValueExp("v")};
  nth->order_by = {WindowOrderTerm{.expression = ColumnValueExp("v"),
                                   .ascending = true,
                                   .nulls_first = std::nullopt}};
  EXPECT_DEATH(Apply(nth), "");
}

TEST_F(WindowEvalDirectTest, NonWindowStatementPassesThroughUntouched) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = "COUNT";
  window->args = {ColumnValueExp("v")};
  // COUNT over the whole partition (no ORDER BY, default frame).
  const auto rows = Apply(window);
  ASSERT_EQ(rows.size(), 4U);
  for (const Row& row : rows) {
    EXPECT_EQ(row[1], Value(4));
  }
}

}  // namespace tinylamb
