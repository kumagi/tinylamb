/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

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

class SqlFunctionCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("sql_function_coverage_test").MoveValue();
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
  // Asserts the statement yields exactly one row whose first column equals
  // `expected`.
  void ExpectScalar(const std::string& sql, Value expected) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0][0], expected);
  }
  // Asserts the statement yields exactly one row of scalar values.
  void ExpectRow(const std::string& sql, const Row& expected) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0], expected);
  }
  void ExpectNullScalar(const std::string& sql) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_TRUE(rows[0][0].IsNull());
  }
  void ExpectError(const std::string& sql) {
    SCOPED_TRACE(sql);
    EXPECT_THROW({ RunSql(engine_.get(), context_.get(), sql); },
                 std::exception);
  }
  // Materializes the array produced by a single SELECT column.
  std::vector<Value> ExpectArray(const std::string& sql, size_t expected_size) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    EXPECT_EQ(rows.size(), expected_size);
    std::vector<Value> values;
    for (const Row& row : rows) {
      values.push_back(row[0]);
    }
    return values;
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlFunctionCoverageTest, Smoke) {
  const auto rows = RunSql(engine_.get(), context_.get(), "SELECT 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

// ---------------------------------------------------------------------------
// Arithmetic / transcendental family (ExecuteFunction numeric branches).
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, AbsSignBranches) {
  ExpectRow("SELECT ABS(-5), ABS(5), ABS(-5.5), ABS(5.5)",
            Row({Value(5), Value(5), Value(5.5), Value(5.5)}));
  ExpectRow("SELECT SIGN(-3), SIGN(0), SIGN(3), SIGN(-2.5), SIGN(2.5)",
            Row({Value(-1), Value(0), Value(1), Value(-1.0), Value(1.0)}));
  // ABS(INT64_MIN) raises instead of wrapping.
  ExpectError("SELECT ABS(-9223372036854775808)");
  ExpectError("SELECT ABS('x')");
  ExpectError("SELECT ABS()");
}

TEST_F(SqlFunctionCoverageTest, RoundFamilyBranches) {
  ExpectRow("SELECT ROUND(2.5), ROUND(-2.5), ROUND(2.4), ROUND(2.6)",
            Row({Value(3.0), Value(-3.0), Value(2.0), Value(3.0)}));
  ExpectScalar("SELECT ROUND(2.567, 2)",
               Value(std::round(2.567 * 100.0) / 100.0));
  ExpectScalar("SELECT ROUND(123, -1)", Value(120));
  ExpectScalar("SELECT ROUND(125, -1)", Value(130));
  ExpectScalar("SELECT ROUND(5)", Value(5));
  ExpectNullScalar("SELECT ROUND(NULL)");
  ExpectNullScalar("SELECT ROUND(NULL, 2)");
  ExpectError("SELECT ROUND(2.5, 1, 3)");
  ExpectError("SELECT ROUND(2.5, 1.5)");
  ExpectError("SELECT ROUND('x')");
}

TEST_F(SqlFunctionCoverageTest, TruncTruncateCeilFloorBranches) {
  ExpectRow("SELECT TRUNC(2.7), TRUNC(-2.7), TRUNCATE(123, -1)",
            Row({Value(2.0), Value(-2.0), Value(120)}));
  ExpectRow("SELECT CEIL(1.2), CEILING(-1.2), FLOOR(1.8), FLOOR(-1.8)",
            Row({Value(2.0), Value(-1.0), Value(1.0), Value(-2.0)}));
  // Integer inputs pass through unchanged.
  ExpectRow("SELECT CEIL(2), FLOOR(-2), TRUNC(7, -1)",
            Row({Value(2), Value(-2), Value(0)}));
  ExpectNullScalar("SELECT CEIL(NULL)");
  ExpectNullScalar("SELECT FLOOR(NULL)");
  ExpectError("SELECT CEIL('x')");
  ExpectError("SELECT FLOOR(1, 2)");
  ExpectError("SELECT TRUNC('x')");
}

TEST_F(SqlFunctionCoverageTest, ModPowDivBranches) {
  ExpectRow("SELECT MOD(10, 3), MOD(-10, 3), MOD(10, -3), MOD(10.5, 3)",
            Row({Value(1), Value(-1), Value(1), Value(1.5)}));
  ExpectRow("SELECT DIV(7, 2), DIV(-7, 2), DIV(7.5, 2)",
            Row({Value(3), Value(-3), Value(3)}));
  ExpectScalar("SELECT POW(2, 10)", Value(1024.0));
  ExpectScalar("SELECT POWER(2, 0.5)", Value(std::sqrt(2.0)));
  ExpectNullScalar("SELECT MOD(NULL, 3)");
  ExpectNullScalar("SELECT POW(NULL, 2)");
  ExpectError("SELECT DIV(7, 0)");
  ExpectError("SELECT MOD(1, 0)");
  ExpectError("SELECT POW(-1, 0.5)");
  ExpectError("SELECT POW(0, -1)");
  ExpectError("SELECT DIV(7)");
  ExpectError("SELECT MOD('a', 2)");
}

TEST_F(SqlFunctionCoverageTest, SqrtCbrtLogExpBranches) {
  ExpectRow("SELECT SQRT(9.0), CBRT(27.0), CBRT(-8.0), EXP(0.0), LN(1.0)",
            Row({Value(3.0), Value(std::cbrt(27.0)), Value(-2.0), Value(1.0),
                 Value(0.0)}));
  ExpectScalar("SELECT LOG(8, 2)", Value(std::log(8.0) / std::log(2.0)));
  ExpectScalar("SELECT LOG(100)", Value(std::log(100.0)));
  ExpectScalar("SELECT LOG10(1000)", Value(3.0));
  ExpectError("SELECT SQRT(-1)");
  ExpectError("SELECT EXP(1000)");
  ExpectError("SELECT COSH(10000)");
  ExpectError("SELECT SINH(10000)");
  ExpectError("SELECT SQRT()");
  ExpectError("SELECT LOG10('x')");
  ExpectError("SELECT LOG(1, 2, 3)");
}

TEST_F(SqlFunctionCoverageTest, TrigonometryBranches) {
  ExpectRow("SELECT SIN(0.0), COS(0.0), TAN(0.0), ATAN(0.0), ATAN2(0.0, 1.0)",
            Row({Value(0.0), Value(1.0), Value(0.0), Value(0.0), Value(0.0)}));
  ExpectScalar("SELECT ACOS(1.0)", Value(0.0));
  ExpectScalar("SELECT ASIN(0.0)", Value(0.0));
  ExpectScalar("SELECT PI()", Value(M_PI));
  ExpectScalar("SELECT RADIANS(180.0)", Value(M_PI));
  ExpectScalar("SELECT DEGREES(PI())", Value(180.0));
  ExpectRow("SELECT COSH(0.0), SINH(0.0), TANH(0.0)",
            Row({Value(1.0), Value(0.0), Value(0.0)}));
  ExpectError("SELECT ACOS(2.0)");
  ExpectError("SELECT ASIN(-2.0)");
  ExpectError("SELECT SIN('x')");
  ExpectError("SELECT ATAN2(1)");
}

TEST_F(SqlFunctionCoverageTest, IsInfIsNanAndIeeeDivide) {
  const double inf = std::numeric_limits<double>::infinity();
  const double nan = std::numeric_limits<double>::quiet_NaN();
  ExpectRow("SELECT IS_INF(CAST('inf' AS FLOAT64)), IS_INF(1.5), IS_NAN(1.5)",
            Row({Value(true), Value(false), Value(false)}));
  ExpectScalar("SELECT IS_NAN(CAST('nan' AS FLOAT64))", Value(true));
  // Integer operands report false without requiring a float conversion.
  ExpectRow("SELECT IS_INF(1), IS_NAN(1)", Row({Value(false), Value(false)}));
  ExpectScalar("SELECT IEEE_DIVIDE(6.0, 3.0)", Value(2.0));
  ExpectScalar("SELECT IEEE_DIVIDE(1.0, 0.0)", Value(inf));
  ExpectNullScalar("SELECT SAFE_DIVIDE(1.0, 0.0)");
  ExpectNullScalar("SELECT IEEE_DIVIDE(NULL, 3.0)");
  ExpectNullScalar("SELECT IS_NAN(NULL)");
  const auto nan_rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT IEEE_DIVIDE(0.0, 0.0), SAFE_DIVIDE(0.0, 0.0)");
  ASSERT_EQ(nan_rows.size(), 1U);
  EXPECT_TRUE(std::isnan(nan_rows[0][0].value.double_value));
  EXPECT_TRUE(nan_rows[0][1].IsNull());
  (void)nan;
  ExpectError("SELECT IEEE_DIVIDE(1, 2, 3)");
}

TEST_F(SqlFunctionCoverageTest, RandBranches) {
  const auto rows = RunSql(engine_.get(), context_.get(), "SELECT RAND()");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0].type, ValueType::kDouble);
  EXPECT_GE(rows[0][0].value.double_value, 0.0);
  EXPECT_LT(rows[0][0].value.double_value, 1.0);
  ExpectError("SELECT RAND(1)");
}

// ---------------------------------------------------------------------------
// SAFE_* arithmetic: overflow collapses to NULL instead of raising.
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, SafeArithmeticBranches) {
  ExpectRow("SELECT SAFE_ADD(2, 3), SAFE_SUBTRACT(2, 3), SAFE_MULTIPLY(2, 3)",
            Row({Value(5), Value(-1), Value(6)}));
  ExpectRow("SELECT SAFE_NEGATE(-5), SAFE_NEGATE(5.5), SAFE_ADD(1.5, 2.5)",
            Row({Value(5), Value(-5.5), Value(4.0)}));
  ExpectNullScalar("SELECT SAFE_ADD(9223372036854775807, 1)");
  ExpectNullScalar("SELECT SAFE_SUBTRACT(-9223372036854775807 - 1, 1)");
  ExpectNullScalar("SELECT SAFE_MULTIPLY(9223372036854775807, 2)");
  ExpectNullScalar("SELECT SAFE_NEGATE(-9223372036854775807 - 1)");
  ExpectNullScalar("SELECT SAFE_ADD(1.0e308, 1.0e308)");
  ExpectNullScalar("SELECT SAFE_ADD(NULL, 1)");
  ExpectError("SELECT SAFE_ADD('a', 1)");
  ExpectError("SELECT SAFE_NEGATE('a')");
  ExpectError("SELECT SAFE_ADD(1)");
}

TEST_F(SqlFunctionCoverageTest, UnsignedSafeArithmetic) {
  ExpectScalar("SELECT SAFE_ADD(CAST(5 AS UINT64), CAST(3 AS UINT64))",
               Value(int64_t{8}).WithUnsigned());
  // Unsigned wrap-around counts as overflow: SAFE_SUBTRACT yields NULL
  // rather than the wrapped 2^64-2.
  ExpectNullScalar("SELECT SAFE_SUBTRACT(CAST(3 AS UINT64), CAST(5 AS UINT64))");
  ExpectNullScalar(
      "SELECT SAFE_ADD(CAST(18446744073709551615 AS UINT64), CAST(1 AS "
      "UINT64))");
}

// ---------------------------------------------------------------------------
// Bitwise operations through the __bit_* / shift spellings.
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, BitwiseOperatorBranches) {
  ExpectRow("SELECT 5 & 3, 5 | 3, 5 ^ 3, 1 << 4, 256 >> 4",
            Row({Value(1), Value(7), Value(6), Value(16), Value(16)}));
  ExpectScalar("SELECT 1 << 64", Value(0));
  ExpectScalar("SELECT 255 >> 64", Value(0));
  ExpectRow("SELECT NULL & 3, 5 | NULL", Row({Value(), Value()}));
  ExpectError("SELECT 1 << -1");
  ExpectError("SELECT 'a' & 3");
  ExpectError("SELECT 1 << 1.5");
}

// ---------------------------------------------------------------------------
// String function family.
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, CaseAndLengthBranches) {
  ExpectRow("SELECT UPPER('aBc'), LOWER('aBc'), UPPER('')",
            Row({Value("ABC"), Value("abc"), Value("")}));
  ExpectRow("SELECT LENGTH('hello'), OCTET_LENGTH('hello'), BYTE_LENGTH('hi')",
            Row({Value(5), Value(5), Value(2)}));
  ExpectScalar("SELECT CHAR_LENGTH('héllo')", Value(5));
  ExpectScalar("SELECT CHARACTER_LENGTH('€a')", Value(2));
  ExpectRow("SELECT LENGTH(NULL), CHAR_LENGTH(NULL)",
            Row({Value(), Value()}));
  ExpectError("SELECT UPPER('a', 'b')");
  ExpectError("SELECT LENGTH()");
}

TEST_F(SqlFunctionCoverageTest, ConcatBranches) {
  ExpectScalar("SELECT CONCAT('a', 'b', 'c')", Value("abc"));
  ExpectScalar("SELECT CONCAT('a')", Value("a"));
  ExpectNullScalar("SELECT CONCAT('a', NULL)");
  ExpectNullScalar("SELECT CONCAT(NULL)");
  // Non-string arguments are coerced to their text form.
  ExpectScalar("SELECT CONCAT('a', 1)", Value("a1"));
}

TEST_F(SqlFunctionCoverageTest, SubstrBranches) {
  ExpectRow("SELECT SUBSTR('hello', 2), SUBSTRING('hello', 2)",
            Row({Value("ello"), Value("ello")}));
  ExpectRow("SELECT SUBSTR('hello', 2, 2), SUBSTR('hello', 1, 0), "
            "SUBSTR('hello', 1, 10)",
            Row({Value("el"), Value(""), Value("hello")}));
  ExpectScalar("SELECT SUBSTR('hello', -2)", Value("lo"));
  ExpectScalar("SELECT SUBSTR('hello', 10)", Value(""));
  ExpectScalar("SELECT SUBSTR('héllo', 2)", Value("éllo"));
  ExpectNullScalar("SELECT SUBSTR(NULL, 2)");
  ExpectNullScalar("SELECT SUBSTR('hello', NULL)");
  ExpectError("SELECT SUBSTR('hello', 2, -1)");
  // Non-string starts are coerced; SUBSTR(1, 2) reads the text "1".
  ExpectScalar("SELECT SUBSTR(1, 2)", Value(""));
  ExpectError("SELECT SUBSTR('hello')");
}

TEST_F(SqlFunctionCoverageTest, ByteSubstrBranches) {
  ExpectRow("SELECT BYTE_SUBSTR('hello', 2), BYTE_SUBSTR('hello', -2)",
            Row({Value("ello"), Value("lo")}));
  ExpectScalar("SELECT BYTE_SUBSTR('hello', 2, 2)", Value("el"));
  ExpectScalar("SELECT BYTE_SUBSTR('hello', 1, 0)", Value(""));
  ExpectNullScalar("SELECT BYTE_SUBSTR(NULL, 1)");
  ExpectError("SELECT BYTE_SUBSTR('hello', 2, -1)");
}

TEST_F(SqlFunctionCoverageTest, InstrStartsEndsWithBranches) {
  ExpectRow("SELECT INSTR('abcabc', 'b'), STRPOS('abc', 'z'), INSTR('abc', '')",
            Row({Value(2), Value(0), Value(1)}));
  ExpectRow("SELECT STARTS_WITH('abc', 'ab'), ENDS_WITH('abc', 'bc'), "
            "STARTS_WITH('abc', 'b')",
            Row({Value(true), Value(true), Value(false)}));
  ExpectRow("SELECT INSTR(NULL, 'a'), STARTS_WITH(NULL, 'a')",
            Row({Value(), Value()}));
  ExpectError("SELECT INSTR('a')");
}

TEST_F(SqlFunctionCoverageTest, LpadRpadBranches) {
  ExpectRow("SELECT LPAD('hi', 5), LPAD('hi', 5, '*'), RPAD('hi', 5, '-')",
            Row({Value("   hi"), Value("***hi"), Value("hi---")}));
  ExpectScalar("SELECT LPAD('hi', 5, 'xy')", Value("xyxhi"));
  ExpectScalar("SELECT RPAD('hi', 5, 'xy')", Value("hixyx"));
  ExpectScalar("SELECT LPAD('hello', 3)", Value("hel"));
  ExpectScalar("SELECT LPAD('hi', 0)", Value(""));
  ExpectScalar("SELECT LPAD('héllo', 3)", Value("hél"));
  ExpectNullScalar("SELECT LPAD(NULL, 2)");
  ExpectNullScalar("SELECT RPAD('hi', NULL)");
  ExpectError("SELECT LPAD('hi', -1)");
  ExpectError("SELECT RPAD('hi', 2000000)");
  ExpectError("SELECT LPAD('hi', 3, '')");
}

TEST_F(SqlFunctionCoverageTest, TrimFamilyBranches) {
  ExpectRow("SELECT TRIM('  hi  '), LTRIM('  hi  '), RTRIM('  hi  ')",
            Row({Value("hi"), Value("hi  "), Value("  hi")}));
  ExpectRow("SELECT TRIM('xxhixx', 'x'), LTRIM('xxhi', 'x'), RTRIM('hixx', 'x')",
            Row({Value("hi"), Value("hi"), Value("hi")}));
  ExpectRow("SELECT TRIM('   '), LTRIM('xxx', 'x'), RTRIM('xxx', 'x')",
            Row({Value(""), Value(""), Value("")}));
  ExpectNullScalar("SELECT TRIM(NULL)");
  ExpectNullScalar("SELECT RTRIM('hi', NULL)");
  ExpectError("SELECT TRIM()");
}

TEST_F(SqlFunctionCoverageTest, ReplaceRepeatReverseBranches) {
  ExpectRow("SELECT REPLACE('aaa', 'a', 'b'), REPLACE('abc', '', 'x'), "
            "REPLACE('abc', 'z', 'q')",
            Row({Value("bbb"), Value("abc"), Value("abc")}));
  ExpectRow("SELECT REPEAT('ab', 3), REPEAT('ab', 0), REPEAT('', 5)",
            Row({Value("ababab"), Value(""), Value("")}));
  ExpectRow("SELECT REVERSE('abc'), REVERSE('€x'), BYTE_REVERSE('abc')",
            Row({Value("cba"), Value("x€"), Value("cba")}));
  ExpectNullScalar("SELECT REPLACE(NULL, 'a', 'b')");
  ExpectNullScalar("SELECT REVERSE(NULL)");
  ExpectError("SELECT REPEAT('a', -1)");
  ExpectError("SELECT REPEAT('abcdefgh', 200000)");
  ExpectError("SELECT REPEAT('a')");
}

TEST_F(SqlFunctionCoverageTest, LeftRightBranches) {
  ExpectRow("SELECT LEFT('hello', 2), RIGHT('hello', 2), LEFT('hello', 0)",
            Row({Value("he"), Value("lo"), Value("")}));
  ExpectScalar("SELECT RIGHT('hello', 10)", Value("hello"));
  ExpectScalar("SELECT LEFT('héllo', 2)", Value("hé"));
  ExpectScalar("SELECT RIGHT('héllo', 2)", Value("lo"));
  ExpectScalar("SELECT BYTE_LEFT('hello', 2)", Value("he"));
  ExpectScalar("SELECT BYTE_RIGHT('hello', 3)", Value("llo"));
  ExpectNullScalar("SELECT LEFT(NULL, 2)");
  ExpectError("SELECT LEFT('hello', -1)");
  ExpectError("SELECT RIGHT('hello', -1)");
  ExpectError("SELECT LEFT('hello')");
}

TEST_F(SqlFunctionCoverageTest, AsciiUnicodeChrBranches) {
  ExpectRow("SELECT ASCII('A'), ASCII(''), UNICODE('A'), UNICODE('')",
            Row({Value(65), Value(0), Value(65), Value(0)}));
  ExpectScalar("SELECT UNICODE('€')", Value(8364));
  ExpectScalar("SELECT CHR(65)", Value("A"));
  ExpectScalar("SELECT CHR(233)", Value("é"));
  ExpectScalar("SELECT UNICODE(CHR(128512))", Value(128512));
  ExpectNullScalar("SELECT ASCII(NULL)");
  ExpectNullScalar("SELECT CHR(NULL)");
  ExpectError("SELECT CHR(-1)");
  ExpectError("SELECT CHR(1114112)");
  ExpectError("SELECT ASCII()");
}

TEST_F(SqlFunctionCoverageTest, CodePointsBranches) {
  ExpectScalar("SELECT CODE_POINTS_TO_STRING([72, 105])", Value("Hi"));
  ExpectScalar("SELECT CODE_POINTS_TO_STRING([128512])", Value("\xF0\x9F\x98\x80"));
  ExpectScalar("SELECT CODE_POINTS_TO_BYTES([65, 66])", Value("AB"));
  ExpectScalar("SELECT CODE_POINTS_TO_STRING([])", Value(""));
  ExpectNullScalar("SELECT CODE_POINTS_TO_STRING(NULL)");
  ExpectNullScalar("SELECT CODE_POINTS_TO_STRING(5)");
  ExpectNullScalar("SELECT CODE_POINTS_TO_BYTES(NULL)");
  ExpectError("SELECT CODE_POINTS_TO_STRING([55296])");
  ExpectError("SELECT CODE_POINTS_TO_STRING([-1])");
  ExpectError("SELECT CODE_POINTS_TO_BYTES([256])");
  ExpectError("SELECT CODE_POINTS_TO_STRING()");
}

TEST_F(SqlFunctionCoverageTest, InitcapTranslateSoundexBranches) {
  ExpectRow("SELECT INITCAP('hello world'), INITCAP('a1b2'), "
            "INITCAP('jean-luc', '-')",
            Row({Value("Hello World"), Value("A1b2"), Value("Jean-Luc")}));
  ExpectRow("SELECT TRANSLATE('hello', 'el', 'ip'), TRANSLATE('hello', 'el', 'i')",
            Row({Value("hippo"), Value("hio")}));
  ExpectScalar("SELECT TRANSLATE('€x', '€', 'E')", Value("Ex"));
  ExpectRow("SELECT SOUNDEX('Robert'), SOUNDEX('Rupert'), SOUNDEX('123')",
            Row({Value("R163"), Value("R163"), Value("")}));
  ExpectNullScalar("SELECT INITCAP(NULL)");
  ExpectNullScalar("SELECT TRANSLATE(NULL, 'a', 'b')");
  ExpectNullScalar("SELECT SOUNDEX(NULL)");
  ExpectError("SELECT TRANSLATE('a', 'b')");
  ExpectError("SELECT SOUNDEX()");
}

TEST_F(SqlFunctionCoverageTest, SplitBranches) {
  auto parts = ExpectArray("SELECT x FROM UNNEST(SPLIT('a,b,c')) AS x", 3);
  EXPECT_EQ(parts[0], Value("a"));
  EXPECT_EQ(parts[1], Value("b"));
  EXPECT_EQ(parts[2], Value("c"));
  parts = ExpectArray("SELECT x FROM UNNEST(SPLIT('a::b', '::')) AS x", 2);
  EXPECT_EQ(parts[0], Value("a"));
  EXPECT_EQ(parts[1], Value("b"));
  parts = ExpectArray("SELECT x FROM UNNEST(SPLIT('ab', '')) AS x", 2);
  EXPECT_EQ(parts[0], Value("a"));
  EXPECT_EQ(parts[1], Value("b"));
  parts = ExpectArray("SELECT x FROM UNNEST(SPLIT('')) AS x", 1);
  EXPECT_EQ(parts[0], Value(""));
  ExpectScalar("SELECT SPLIT_SUBSTR('a-b-c', '-', 2)", Value("b"));
  ExpectScalar("SELECT SPLIT_SUBSTR('a-b-c', '-', 1, 2)", Value("a-b"));
  ExpectScalar("SELECT SPLIT_SUBSTR('a-b-c', '-', -1)", Value("c"));
  ExpectScalar("SELECT SPLIT_SUBSTR('a-b-c', '-', 4)", Value(""));
  ExpectScalar("SELECT SPLIT_SUBSTR('a-b-c', '-')", Value("a-b-c"));
  ExpectNullScalar("SELECT SPLIT(NULL)");
  ExpectNullScalar("SELECT SPLIT_SUBSTR(NULL, '-')");
  ExpectError("SELECT SPLIT()");
  ExpectError("SELECT SPLIT_SUBSTR('a', '-', 1, 2, 3)");
}

TEST_F(SqlFunctionCoverageTest, RegexpFamilyBranches) {
  ExpectRow("SELECT REGEXP_CONTAINS('abc', 'b'), REGEXP_CONTAINS('abc', '^b')",
            Row({Value(true), Value(false)}));
  ExpectRow("SELECT REGEXP_MATCH('abc', 'a.c'), REGEXP_MATCH('abc', 'a')",
            Row({Value(true), Value(false)}));
  ExpectRow("SELECT REGEXP_EXTRACT('abc123', '[0-9]+'), "
            "REGEXP_EXTRACT('abc', '([a-z])')",
            Row({Value("123"), Value("a")}));
  ExpectNullScalar("SELECT REGEXP_EXTRACT('abc', '[0-9]+')");
  ExpectScalar("SELECT REGEXP_REPLACE('abcabc', 'b', 'X')", Value("aXcaXc"));
  ExpectScalar("SELECT REGEXP_INSTR('abc123def', '[0-9]+')", Value(4));
  ExpectScalar("SELECT REGEXP_INSTR('a1b2c3', '[0-9]', 1, 2)", Value(4));
  ExpectScalar("SELECT REGEXP_INSTR('abc123', '[0-9]+', 1, 1, 1)", Value(6));
  ExpectScalar("SELECT REGEXP_INSTR('abc', '[0-9]+')", Value(0));
  auto hits =
      ExpectArray("SELECT x FROM UNNEST(REGEXP_EXTRACT_ALL('a1b2', '[a-z]')) AS x",
                  2);
  EXPECT_EQ(hits[0], Value("a"));
  EXPECT_EQ(hits[1], Value("b"));
  ExpectNullScalar("SELECT REGEXP_CONTAINS(NULL, 'a')");
  ExpectNullScalar("SELECT REGEXP_EXTRACT_ALL(NULL, 'a')");
  ExpectError("SELECT REGEXP_CONTAINS('abc', '(')");
  ExpectError("SELECT REGEXP_MATCH('abc', '[')");
  ExpectError("SELECT REGEXP_REPLACE('abc', '(', 'x')");
  ExpectError("SELECT REGEXP_EXTRACT('abc', '*')");
  ExpectError("SELECT REGEXP_EXTRACT('abc')");
  ExpectError("SELECT REGEXP_INSTR('abc', '[0-9]', 0)");
  ExpectError("SELECT REGEXP_INSTR('abc', '[0-9]', 1, 0)");
  ExpectError("SELECT REGEXP_EXTRACT_ALL('abc')");
}

// ---------------------------------------------------------------------------
// Hashing family (raw digest bytes).
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, HashFamilyBranches) {
  // LENGTH counts UTF-8 code points over the raw digest bytes, so raw-digest
  // lengths land below the byte counts (16/20/32/64).  These constants pin
  // the digests are present and NULL-propagating.
  ExpectRow("SELECT LENGTH(MD5('abc')), LENGTH(SHA1('abc')), "
            "LENGTH(SHA256('abc')), LENGTH(SHA512('abc'))",
            Row({Value(12), Value(18), Value(27), Value(51)}));
  ExpectNullScalar("SELECT MD5(NULL)");
  ExpectNullScalar("SELECT SHA1(NULL)");
  ExpectNullScalar("SELECT SHA256(NULL)");
  ExpectNullScalar("SELECT SHA512(NULL)");
  ExpectError("SELECT MD5('a', 'b')");
  ExpectError("SELECT SHA1()");
}

// ---------------------------------------------------------------------------
// Conditional expressions with lazy branch evaluation.
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, IfBranches) {
  ExpectRow("SELECT IF(TRUE, 1, 2), IF(FALSE, 1, 2)",
            Row({Value(1), Value(2)}));
  ExpectScalar("SELECT IF(TRUE, 1.5, 2)", Value(1.5));
  // Untaken branch is not evaluated: an error there never surfaces.
  ExpectScalar("SELECT IF(TRUE, 1, 1/0)", Value(1));
  ExpectScalar("SELECT IF(FALSE, 1/0, 42)", Value(42));
  // A NULL condition is not truthy, so the else branch is taken.
  ExpectScalar("SELECT IF(NULL, 1, 2)", Value(2));
  ExpectError("SELECT IF(TRUE, 1)");
  ExpectError("SELECT IF(TRUE, 1, 2, 3)");
}

TEST_F(SqlFunctionCoverageTest, IfErrorIsErrorBranches) {
  // The divide contract is DOUBLE, so the fallback branch promotes too.
  ExpectRow("SELECT IFERROR(1/0, -1), IFERROR(5, -1), IFERROR(1.5, -2.5)",
            Row({Value(-1.0), Value(5), Value(1.5)}));
  ExpectRow("SELECT ISERROR(1/0), ISERROR(5)",
            Row({Value(1), Value(0)}));
  ExpectNullScalar("SELECT NULLIFERROR(1/0)");
  ExpectScalar("SELECT NULLIFERROR(7)", Value(7));
  ExpectError("SELECT IFERROR(1)");
  ExpectError("SELECT ISERROR(1, 2)");
  ExpectError("SELECT NULLIFERROR()");
}

TEST_F(SqlFunctionCoverageTest, CoalesceIfNullNullIfBranches) {
  ExpectRow("SELECT COALESCE(NULL, NULL, 3), COALESCE(1, 2), IFNULL(NULL, 5), "
            "IFNULL(4, 5)",
            Row({Value(3), Value(1), Value(5), Value(4)}));
  ExpectRow("SELECT NULLIF(3, 3), NULLIF(3, 4)",
            Row({Value(), Value(3)}));
  ExpectScalar("SELECT COALESCE(1.5, 2)", Value(1.5));
  ExpectNullScalar("SELECT COALESCE(NULL)");
  ExpectNullScalar("SELECT NULLIF(NULL, 3)");
  // Short-circuit over row values: an error in an unevaluated branch does
  // not surface.
  ExpectScalar("SELECT COALESCE(a, 1/0) FROM UNNEST([1]) AS a", Value(1));
  ExpectScalar("SELECT IFNULL(a, 1/0) FROM UNNEST([2]) AS a", Value(2));
  ExpectError("SELECT NULLIF(1)");
  ExpectError("SELECT IFNULL(1, 2, 3)");
}

TEST_F(SqlFunctionCoverageTest, GreatestLeastBranches) {
  ExpectRow("SELECT GREATEST(1, 5, 3), LEAST(1, 5, 3)",
            Row({Value(5), Value(1)}));
  ExpectRow("SELECT GREATEST(1, 2.5), LEAST(2, 1.5)",
            Row({Value(2.5), Value(1.5)}));
  ExpectRow("SELECT GREATEST('a', 'b'), LEAST('a', 'b')",
            Row({Value("b"), Value("a")}));
  ExpectScalar("SELECT GREATEST(1)", Value(1));
  ExpectNullScalar("SELECT GREATEST(NULL, 1)");
  ExpectNullScalar("SELECT LEAST(1, NULL)");
  const auto nan_rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT GREATEST(CAST('nan' AS FLOAT64), 1), "
             "LEAST(CAST('nan' AS FLOAT64), 1)");
  ASSERT_EQ(nan_rows.size(), 1U);
  EXPECT_TRUE(std::isnan(nan_rows[0][0].value.double_value));
  EXPECT_TRUE(std::isnan(nan_rows[0][1].value.double_value));
  ExpectError("SELECT GREATEST()");
}

// ---------------------------------------------------------------------------
// CAST / SAFE_CAST matrix.
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, CastStringToIntBranches) {
  ExpectRow("SELECT CAST('42' AS INT64), CAST(' 42 ' AS INT64), "
            "CAST('0x1F' AS INT64), CAST('-0x10' AS INT64)",
            Row({Value(42), Value(42), Value(31), Value(-16)}));
  ExpectError("SELECT CAST('3.7' AS INT64)");
  ExpectError("SELECT CAST('abc' AS INT64)");
  ExpectError("SELECT CAST('' AS INT64)");
  ExpectError("SELECT CAST('99999999999999999999' AS INT64)");
  ExpectError("SELECT CAST('0xzz' AS INT64)");
  ExpectNullScalar("SELECT CAST(NULL AS INT64)");
}

TEST_F(SqlFunctionCoverageTest, CastFloatToIntBranches) {
  ExpectRow("SELECT CAST(2.4 AS INT64), CAST(2.5 AS INT64), CAST(-2.5 AS INT64)",
            Row({Value(2), Value(3), Value(-3)}));
  ExpectError("SELECT CAST(9.3e18 AS INT64)");
  ExpectError("SELECT CAST(-9.3e18 AS INT64)");
  ExpectError("SELECT CAST(CAST('nan' AS FLOAT64) AS INT64)");
  ExpectError("SELECT CAST(CAST('inf' AS FLOAT64) AS INT64)");
  ExpectScalar("SELECT CAST(DATE '2020-03-14' AS INT64)", Value(18335));
}

TEST_F(SqlFunctionCoverageTest, CastIntWidthBranches) {
  ExpectRow("SELECT CAST(100 AS INT8), CAST(300 AS INT16)",
            Row({Value(100), Value(300)}));
  ExpectError("SELECT CAST(300 AS INT8)");
  ExpectError("SELECT CAST(-300 AS INT8)");
  ExpectError("SELECT CAST(40000 AS INT16)");
  ExpectNullScalar("SELECT SAFE_CAST(300 AS INT8)");
  ExpectNullScalar("SELECT SAFE_CAST(40000 AS INT16)");
  ExpectError("SELECT CAST(-1 AS UINT64)");
  ExpectError("SELECT CAST(-1 AS UINT32)");
  ExpectError("SELECT CAST(300 AS UINT8)");
  ExpectScalar("SELECT CAST(300 AS INT32)", Value(300));
  ExpectScalar("SELECT CAST('18446744073709551615' AS UINT64)",
               Value(int64_t{-1}).WithUnsigned());
  ExpectError("SELECT CAST('18446744073709551616' AS UINT64)");
  ExpectScalar("SELECT CAST(255 AS UINT8)", Value(255).WithUnsigned());
}

TEST_F(SqlFunctionCoverageTest, CastHexBranches) {
  // An explicit '+' sign keeps the magnitude positive and unsigned-valid.
  ExpectScalar("SELECT CAST('+0x10' AS UINT64)", Value(16).WithUnsigned());
  ExpectError("SELECT CAST('0x8000000000000000' AS INT64)");
  ExpectScalar("SELECT CAST('0xFFFFFFFFFFFFFFFF' AS UINT64)",
               Value(int64_t{-1}).WithUnsigned());
  ExpectScalar("SELECT CAST('-0x80' AS INT8)", Value(-128));
  ExpectError("SELECT CAST('-0x81' AS INT8)");
}

TEST_F(SqlFunctionCoverageTest, CastDoubleBranches) {
  ExpectRow("SELECT CAST('2.5' AS FLOAT64), CAST(3 AS FLOAT64)",
            Row({Value(2.5), Value(3.0)}));
  const double inf = std::numeric_limits<double>::infinity();
  ExpectRow("SELECT CAST('inf' AS FLOAT64), CAST('-inf' AS FLOAT64), "
            "CAST('infinity' AS FLOAT64), CAST('-Infinity' AS FLOAT64)",
            Row({Value(inf), Value(-inf), Value(inf), Value(-inf)}));
  ExpectScalar("SELECT CAST(1.005 AS FLOAT32)",
               Value(static_cast<double>(static_cast<float>(1.005))));
  ExpectError("SELECT CAST('abc' AS FLOAT64)");
  ExpectError("SELECT CAST('1e400' AS FLOAT64)");
  ExpectError("SELECT CAST(1.0e40 AS FLOAT32)");
  ExpectNullScalar("SELECT SAFE_CAST('abc' AS FLOAT64)");
}

TEST_F(SqlFunctionCoverageTest, CastToStringBranches) {
  ExpectRow("SELECT CAST(123 AS STRING), CAST(0.5 AS STRING), "
            "CAST(TRUE AS STRING), CAST(FALSE AS STRING)",
            Row({Value("123"), Value("0.5"), Value("true"), Value("false")}));
  ExpectRow("SELECT CAST(CAST('nan' AS FLOAT64) AS STRING), "
            "CAST(CAST('inf' AS FLOAT64) AS STRING), "
            "CAST(CAST('-inf' AS FLOAT64) AS STRING)",
            Row({Value("nan"), Value("inf"), Value("-inf")}));
  ExpectScalar("SELECT CAST(DATE '2020-03-14' AS STRING)", Value("2020-03-14"));
  ExpectNullScalar("SELECT CAST(NULL AS STRING)");
}

TEST_F(SqlFunctionCoverageTest, CastBoolBranches) {
  ExpectRow("SELECT CAST(TRUE AS BOOL), CAST(FALSE AS BOOL)",
            Row({Value(true), Value(false)}));
  ExpectRow("SELECT CAST('true' AS BOOL), CAST('T' AS BOOL), "
            "CAST('1' AS BOOL), CAST('false' AS BOOL), CAST('0' AS BOOL)",
            Row({Value(true), Value(true), Value(true), Value(false),
                 Value(false)}));
  ExpectRow("SELECT CAST(2 AS BOOL), CAST(0 AS BOOL), CAST(0.0 AS BOOL)",
            Row({Value(true), Value(false), Value(false)}));
  ExpectError("SELECT CAST('yes' AS BOOL)");
  ExpectNullScalar("SELECT SAFE_CAST('yes' AS BOOL)");
  ExpectNullScalar("SELECT CAST(NULL AS BOOL)");
}

TEST_F(SqlFunctionCoverageTest, CastDateBranches) {
  ExpectScalar("SELECT CAST('2020-03-14' AS DATE)", Value::Date("2020-03-14"));
  ExpectScalar("SELECT CAST('2020-03-14 10:11:12' AS DATE)",
               Value::Date("2020-03-14"));
  ExpectScalar("SELECT CAST('2020-03-14T10:11:12' AS DATE)",
               Value::Date("2020-03-14"));
  ExpectScalar("SELECT CAST(18335 AS DATE)", Value::Date("2020-03-14"));
  ExpectScalar("SELECT CAST(DATE '2020-03-14' AS DATE)",
               Value::Date("2020-03-14"));
  ExpectNullScalar("SELECT CAST(NULL AS DATE)");
  ExpectError("SELECT CAST('garbage' AS DATE)");
  ExpectError("SELECT CAST('2020-99-99' AS DATE)");
}

TEST_F(SqlFunctionCoverageTest, CastDatetimeBranches) {
  ExpectRow("SELECT CAST('2020-03-14 10:11:12' AS DATETIME), "
            "CAST('2020-03-14T10:11:12' AS DATETIME), "
            "CAST('2020-03-14' AS DATETIME)",
            Row({Value("2020-03-14 10:11:12"), Value("2020-03-14 10:11:12"),
                 Value("2020-03-14 00:00:00")}));
  ExpectError("SELECT CAST('garbage' AS DATETIME)");
  ExpectNullScalar("SELECT SAFE_CAST('garbage' AS DATETIME)");
}

TEST_F(SqlFunctionCoverageTest, CastTimeBranches) {
  ExpectRow("SELECT CAST('10:11:12' AS TIME), "
            "CAST('2020-03-14 10:11:12' AS TIME), "
            "CAST('2020-03-14' AS TIME)",
            Row({Value("10:11:12"), Value("10:11:12"), Value("00:00:00")}));
  ExpectRow("SELECT CAST('10:11:12.5' AS TIME), CAST('10:11:12.000000001' AS TIME)",
            Row({Value("10:11:12.500"), Value("10:11:12.000000001")}));
  ExpectError("SELECT CAST('garbage' AS TIME)");
}

TEST_F(SqlFunctionCoverageTest, CastTimestampBranches) {
  // Explicit zone suffixes convert deterministically regardless of the
  // session default time zone.
  ExpectScalar("SELECT CAST('2020-03-14 10:11:12+02' AS TIMESTAMP)",
               Value("2020-03-14 08:11:12+00"));
  ExpectScalar("SELECT CAST('2020-03-14 10:11:12Z' AS TIMESTAMP)",
               Value("2020-03-14 10:11:12+00"));
  ExpectScalar("SELECT CAST('2020-03-14 10:11:12 UTC' AS TIMESTAMP)",
               Value("2020-03-14 10:11:12+00"));
  ExpectError("SELECT CAST('garbage' AS TIMESTAMP)");
}

TEST_F(SqlFunctionCoverageTest, SafeCastNullsOnError) {
  ExpectNullScalar("SELECT SAFE_CAST('abc' AS INT64)");
  ExpectNullScalar("SELECT SAFE_CAST(1.0e300 AS INT64)");
  ExpectNullScalar("SELECT SAFE_CAST('garbage' AS DATE)");
  ExpectNullScalar("SELECT SAFE_CAST('garbage' AS TIMESTAMP)");
  ExpectScalar("SELECT SAFE_CAST('42' AS INT64)", Value(42));
}

TEST_F(SqlFunctionCoverageTest, CastIntervalBranches) {
  ExpectScalar("SELECT CAST('1-2 3 4:5:6' AS INTERVAL)",
               Value("1-2 3 4:5:6"));
  ExpectNullScalar("SELECT CAST(NULL AS INTERVAL)");
}

TEST_F(SqlFunctionCoverageTest, CastBytesBranches) {
  ExpectScalar("SELECT CAST('abc' AS BYTES)", Value("abc"));
  ExpectScalar("SELECT CAST(CAST('abc' AS BYTES) AS STRING)", Value("abc"));
}

// ---------------------------------------------------------------------------
// Binary operators across the type matrix.
// ---------------------------------------------------------------------------

TEST_F(SqlFunctionCoverageTest, BinaryArithmeticMatrix) {
  ExpectRow("SELECT 7 + 3, 7 - 3, 7 * 3, MOD(7, 3), MOD(-7, 3)",
            Row({Value(10), Value(4), Value(21), Value(1), Value(-1)}));
  ExpectRow("SELECT 7.5 + 2.5, 7.5 - 2.5, 7.5 * 2.0",
            Row({Value(10.0), Value(5.0), Value(15.0)}));
  ExpectRow("SELECT 7 + 0.5, 7 - 0.5, 7 * 0.5, MOD(7.5, 0.5)",
            Row({Value(7.5), Value(6.5), Value(3.5), Value(0.0)}));
  ExpectScalar("SELECT 7 / 2", Value(3.5));
  ExpectScalar("SELECT 7.5 / 2.5", Value(3.0));
  ExpectNullScalar("SELECT NULL + 1");
  ExpectNullScalar("SELECT 1 * NULL");
  ExpectError("SELECT 1 / 0");
  ExpectError("SELECT 1.0 / 0");
  ExpectError("SELECT MOD(1, 0)");
  ExpectError("SELECT 9223372036854775807 + 1");
  ExpectError("SELECT 9223372036854775807 * 2");
  ExpectError("SELECT 'a' + 1");
}

TEST_F(SqlFunctionCoverageTest, BinaryComparisonMatrix) {
  ExpectRow("SELECT 1 < 2, 2 <= 2, 3 > 2, 3 >= 4, 5 = 5, 5 != 5",
            Row({Value(true), Value(true), Value(true), Value(false),
                 Value(true), Value(false)}));
  ExpectRow("SELECT 1 < 1.5, 2.5 <= 2.5, 'a' < 'b', 'a' = 'a'",
            Row({Value(true), Value(true), Value(true), Value(true)}));
  ExpectRow("SELECT NULL = NULL, NULL != NULL",
            Row({Value(), Value()}));
  ExpectRow("SELECT 1 < NULL, NULL >= NULL", Row({Value(), Value()}));
  ExpectError("SELECT 1 = '1'");
  ExpectError("SELECT 'a' < 1");
}

TEST_F(SqlFunctionCoverageTest, BinaryBooleanLogicThreeValued) {
  ExpectRow("SELECT TRUE AND TRUE, TRUE AND FALSE, TRUE AND NULL, "
            "FALSE AND NULL",
            Row({Value(true), Value(false), Value(), Value(false)}));
  ExpectRow("SELECT FALSE OR FALSE, TRUE OR FALSE, TRUE OR NULL, NULL OR NULL",
            Row({Value(false), Value(true), Value(true), Value()}));
  ExpectScalar("SELECT TRUE ^ FALSE", Value(true));
  ExpectScalar("SELECT NOT NULL", Value());
}

TEST_F(SqlFunctionCoverageTest, BinaryLikeBranches) {
  ExpectRow("SELECT 'hello' LIKE 'h%', 'hello' LIKE 'h_llo', "
            "'hello' NOT LIKE 'x%', 'abc' LIKE 'ABC'",
            Row({Value(true), Value(true), Value(true), Value(false)}));
  ExpectNullScalar("SELECT NULL LIKE 'a'");
  ExpectError("SELECT 1 LIKE 'a'");
  ExpectError("SELECT 'a' LIKE 1");
  // Coercion path: ENDS_WITH over integers reads their text form.
  ExpectScalar("SELECT ENDS_WITH(1, 2)", Value(false));
}

TEST_F(SqlFunctionCoverageTest, BinaryUnsignedMixedBranches) {
  ExpectScalar("SELECT CAST(5 AS UINT64) + 3", Value(8));
  ExpectScalar("SELECT CAST(5 AS UINT64) * CAST(4 AS UINT64)", Value(20));
  ExpectScalar("SELECT MOD(CAST(5 AS UINT64), 3)", Value(2));
  ExpectScalar("SELECT CAST('18446744073709551615' AS UINT64) > 0",
               Value(true));
  ExpectError("SELECT CAST(5 AS UINT64) / CAST(0 AS UINT64)");
  ExpectError("SELECT MOD(CAST(5 AS UINT64), CAST(0 AS UINT64))");
}

TEST_F(SqlFunctionCoverageTest, BinaryIntervalArithmetic) {
  ExpectScalar("SELECT '0-0 2 0:0:0' * 3", Value("0-0 6 0:0:0"));
  ExpectScalar("SELECT 3 * '0-0 2 0:0:0'", Value("0-0 6 0:0:0"));
  ExpectScalar("SELECT '0-0 2 0:0:0' + '0-0 1 0:0:0'", Value("0-0 3 0:0:0"));
  ExpectScalar("SELECT '0-0 2 0:0:0' - '0-0 1 0:0:0'", Value("0-0 1 0:0:0"));
  ExpectScalar("SELECT '0-0 2 0:0:0' = '0-0 2 0:0:0'", Value(true));
  ExpectScalar("SELECT '0-0 1 0:0:0' < '0-0 2 0:0:0'", Value(true));
}

}  // namespace tinylamb
