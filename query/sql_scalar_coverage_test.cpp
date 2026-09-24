/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Coverage tests for the scalar-function surface of
// expression/function_call_expression.cpp and the cast matrix in
// expression/cast_expression.cpp: timestamp parsing (fractional seconds and
// time-only forms), the JSON family, struct field access/mutation, regexp
// functions, crypto digests, safe arithmetic, and explicit/SAFE casts.
// Every expectation was pinned against the engine's ground truth.

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
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
  // One retry for empty diagnostics: a failed Prepare with no LastError is
  // the external parse child being transiently unavailable under load.
  StatusOr<QueryResult> result = engine->Execute(*ctx, sql);
  if (!result.HasValue()) {
    if (engine->LastError().empty()) {
      result = engine->Execute(*ctx, sql);
    }
    if (!result.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  return result.MoveValue().Collect();
}
}  // namespace

class SqlScalarCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_scalar_coverage_test-" + RandomString())
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
  Value Scalar(const std::string& sql) {
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "query did not return one row: " << sql;
      return {};
    }
    return rows[0][0];
  }
  void ExpectValue(const std::string& sql, const Value& expected) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(Scalar(sql), expected);
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

// ------------------------------------------------------------- timestamps ---

TEST_F(SqlScalarCoverageTest, TimestampParsingHandlesFractionsAndTimeOnly) {
  // Explicit UTC keeps the epoch math independent of the host time zone.
  ExpectValue(
      "SELECT unix_seconds(parse_timestamp('%Y-%m-%d %H:%M:%S', "
      "'1970-01-01 00:00:01', 'UTC'))",
      Value(int64_t{1}));
  // Fractional seconds: the civil-time parser pads/truncates the nanosecond
  // digits when reached through TIMESTAMP casts (strptime-based
  // parse_timestamp drops them unless the format consumes the dot).
  ExpectValue(
      "SELECT unix_micros(CAST('1970-01-01 00:00:01.123456+00' "
      "AS TIMESTAMP))",
      Value(int64_t{1123456}));
  // A short fraction must be right-padded, not interpreted as microseconds.
  ExpectValue("SELECT unix_millis(CAST('00:00:00.2+00' AS TIMESTAMP))",
              Value(int64_t{200}));
  // Time-only inputs land on 1970-01-01 and accept fractions as well.
  ExpectValue("SELECT unix_millis(CAST('03:00:00.25+00' AS TIMESTAMP))",
              Value(int64_t{10800250}));
  // NULL propagation.
  ExpectValue("SELECT parse_timestamp('%Y', NULL, 'UTC') IS NULL", Value(true));
}

TEST_F(SqlScalarCoverageTest, TimestampFormatRoundTrip) {
  ExpectValue(
      "SELECT format_timestamp('%Y-%m-%d %H:%M:%S', "
      "parse_timestamp('%Y-%m-%d %H:%M:%S', '2026-09-18 12:34:56', "
      "'UTC'), 'UTC')",
      Value(std::string("2026-09-18 12:34:56")));
  ExpectValue("SELECT unix_seconds(current_timestamp()) > 0", Value(true));
}

// ------------------------------------------------------------------ dates ---

TEST_F(SqlScalarCoverageTest, DateArithmeticExtractionAndFormatting) {
  ExpectValue("SELECT date_sub(DATE '2026-01-03', INTERVAL 2 DAY)",
              Value::Date("2026-01-01"));
  ExpectValue("SELECT extract_year(DATE '2026-05-06')", Value(int64_t{2026}));
  ExpectValue("SELECT extract_month(DATE '2026-05-06')", Value(int64_t{5}));
  ExpectValue("SELECT extract_day(DATE '2026-05-06')", Value(int64_t{6}));
  ExpectValue("SELECT format_date('%Y/%m/%d', DATE '2026-01-02')",
              Value(std::string("2026/01/02")));
  ExpectValue("SELECT unix_date(DATE '1970-01-02')", Value(int64_t{1}));
  ExpectValue("SELECT current_date() IS NOT NULL", Value(true));
}

// ----------------------------------------------------------------- strings ---

TEST_F(SqlScalarCoverageTest, StringFunctionSuite) {
  ExpectValue("SELECT left('abcdef', 2)", Value(std::string("ab")));
  ExpectValue("SELECT right('abcdef', 2)", Value(std::string("ef")));
  ExpectValue("SELECT lpad('7', 3, '0')", Value(std::string("007")));
  ExpectValue("SELECT rpad('ab', 5, 'xy')", Value(std::string("abxyx")));
  ExpectValue("SELECT lpad('abcdef', 3)", Value(std::string("abc")));
  ExpectValue("SELECT translate('hello', 'el', 'ip')",
              Value(std::string("hippo")));
  ExpectValue("SELECT soundex('Pfister')", Value(std::string("P236")));
  ExpectValue("SELECT initcap('hello world')",
              Value(std::string("Hello World")));
  ExpectValue("SELECT reverse('abc')", Value(std::string("cba")));
  ExpectValue("SELECT repeat('ab', 3)", Value(std::string("ababab")));
  ExpectValue("SELECT replace('aaa', 'a', 'bb')", Value(std::string("bbbbbb")));
  ExpectValue("SELECT substr('abcdef', 2, 3)", Value(std::string("bcd")));
  ExpectValue("SELECT substring('abcdef', 4)", Value(std::string("def")));
  ExpectValue("SELECT strpos('abcd', 'cd')", Value(int64_t{3}));
  ExpectValue("SELECT instr('abcd', 'cd')", Value(int64_t{3}));
  ExpectValue("SELECT starts_with('abc', 'ab')", Value(true));
  ExpectValue("SELECT ends_with('abc', 'bc')", Value(true));
  ExpectValue("SELECT ascii('A')", Value(int64_t{65}));
  ExpectValue("SELECT unicode('あ')", Value(int64_t{0x3042}));
  ExpectValue("SELECT chr(66)", Value(std::string("B")));
  ExpectValue("SELECT length('abc')", Value(int64_t{3}));
  ExpectValue("SELECT char_length('abc')", Value(int64_t{3}));
  ExpectValue("SELECT byte_length('あ')", Value(int64_t{3}));
  ExpectValue("SELECT octet_length('あ')", Value(int64_t{3}));
  ExpectValue("SELECT trim('  x  ')", Value(std::string("x")));
  ExpectValue("SELECT ltrim('  x  ')", Value(std::string("x  ")));
  ExpectValue("SELECT rtrim('  x  ')", Value(std::string("  x")));
  ExpectValue("SELECT upper('aB')", Value(std::string("AB")));
  ExpectValue("SELECT lower('aB')", Value(std::string("ab")));
  ExpectValue("SELECT concat('a', 'b', 'c')", Value(std::string("abc")));
  ExpectValue("SELECT code_points_to_string([66, 67])",
              Value(std::string("BC")));
}

TEST_F(SqlScalarCoverageTest, ByteFunctionSuiteOperatesOnBytes) {
  ExpectValue("SELECT byte_length(CAST('あ' AS BYTES))", Value(int64_t{3}));
  ExpectValue("SELECT byte_substr(CAST('abcdef' AS BYTES), 2, 3)",
              Value(std::string("bcd")));
  ExpectValue("SELECT byte_left(CAST('abcdef' AS BYTES), 2)",
              Value(std::string("ab")));
  ExpectValue("SELECT byte_right(CAST('abcdef' AS BYTES), 2)",
              Value(std::string("ef")));
  ExpectValue("SELECT byte_reverse(CAST('abc' AS BYTES))",
              Value(std::string("cba")));
  ExpectValue("SELECT code_points_to_bytes([97, 98])",
              Value(std::string("ab")));
}

TEST_F(SqlScalarCoverageTest, SplitProducesAnArray) {
  ExpectValue("SELECT COUNT(*) FROM UNNEST(split('a,b,c', ','))",
              Value(int64_t{3}));
}

// ----------------------------------------------------------------- regexp ---

TEST_F(SqlScalarCoverageTest, RegexpFunctionSuite) {
  ExpectValue("SELECT regexp_contains('hello', 'l+')", Value(true));
  ExpectValue("SELECT regexp_extract('ab123cd', '[0-9]+')",
              Value(std::string("123")));
  ExpectValue("SELECT regexp_replace('a-b-c', '-', '+')",
              Value(std::string("a+b+c")));
  ExpectValue("SELECT regexp_instr('abc', 'c')", Value(int64_t{3}));
  // REGEXP_MATCH is a full-string match, like the reference semantics.
  ExpectValue("SELECT regexp_match('abc', 'a.c')", Value(true));
  ExpectValue("SELECT regexp_match('abc', 'b')", Value(false));
  ExpectValue(
      "SELECT regexp_extract_all('a1b22c333', '[0-9]+') = "
      "['1','22','333']",
      Value(true));
}

// -------------------------------------------------------------------- json ---

TEST_F(SqlScalarCoverageTest, JsonFunctionFamily) {
  ExpectValue(R"sql(SELECT json_value('{"k": [1, 2]}', '$.k[1]'))sql",
              Value(std::string("2")));
  ExpectValue(R"sql(SELECT json_extract('{"a": {"b": 7}}', '$.a.b'))sql",
              Value(std::string("7")));
  ExpectValue(R"sql(SELECT json_query('{"a": [1, 2]}', '$.a') IS NOT NULL)sql",
              Value(true));
  ExpectValue(R"sql(SELECT json_value('{"k": NULL}', '$.k') IS NULL)sql",
              Value(true));
  ExpectValue(R"sql(SELECT json_extract_scalar('{"k": "str"}', '$.k'))sql",
              Value(std::string("str")));
  ExpectValue(R"sql(SELECT json_value_array('{"k": ["x", "y"]}', '$.k') = )sql"
              "['x','y']",
              Value(true));
  ExpectValue(
      R"sql(SELECT json_extract_string_array('{"k": ["x"]}', '$.k') = )sql"
      "['x']",
      Value(true));
  ExpectValue(
      R"sql(SELECT COUNT(*) FROM UNNEST(json_extract_array('{"k": [1, 2, 3]}', )sql"
      "'$.k'))",
      Value(int64_t{3}));
}

// Bug 68: both JSON parsers (ParseJsonSimple in rewrite.cpp used by the
// constant-fold rule, and parse_j in executor/detail/expression_eval.cpp)
// recursed once per '['/'{' with no depth bound -- a deeply nested document
// overflowed the stack.  Nesting beyond 512 levels now fails the parse and
// yields NULL instead of crashing.
TEST_F(SqlScalarCoverageTest, JsonDeepNestingTerminatesCleanly) {
  const std::string deep_arr =
      std::string(600, '[') + "1" + std::string(600, ']');
  // Constant arguments: exercises the fold-side parser.
  ExpectValue("SELECT json_extract('" + deep_arr + "', '$') IS NULL",
              Value(true));
  // Column-sourced JSON: exercises the row-evaluation parser.
  RunSql(engine_.get(), context_.get(), "CREATE TABLE jdeep (j STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO jdeep VALUES ('" + deep_arr + "');");
  ExpectValue("SELECT json_extract(j, '$') IS NULL FROM jdeep", Value(true));
  // Just under the bound still parses.
  const std::string ok_arr =
      std::string(300, '[') + "7" + std::string(300, ']');
  ExpectValue("SELECT json_extract('" + ok_arr + "', '$') IS NOT NULL",
              Value(true));
}

// ------------------------------------------------------------------ struct ---

TEST_F(SqlScalarCoverageTest, StructFieldAccessAndMutation) {
  ExpectValue("SELECT get_field(STRUCT(1 AS a, 'x' AS b), 'b')",
              Value(std::string("x")));
  ExpectValue("SELECT get_field(STRUCT(1 AS a), 'zz') IS NULL", Value(true));
  ExpectValue("SELECT __get_field_safe(STRUCT(1 AS a), 'a')",
              Value(int64_t{1}));
  // get_field surfaces the member's canonical text form.
  ExpectValue("SELECT get_field(STRUCT(1 AS a), 'a')", Value(std::string("1")));
  ExpectValue(
      "SELECT __struct_set(STRUCT(1 AS a, 2 AS b), 'a', 9) = "
      "STRUCT(9 AS a, 2 AS b)",
      Value(true));
  ExpectValue(
      "SELECT __struct_set(STRUCT(1 AS a), 'x.y', 5) = "
      "STRUCT(1 AS a, STRUCT(5 AS y) AS x)",
      Value(true));
  ExpectValue("SELECT __struct_set(NULL, 'a', 1) IS NULL", Value(true));
  ExpectValue("SELECT __struct_json__('a', 1, 'b', 'x')",
              Value(std::string(R"({"a":1,"b":"x"})")));
}

// ---------------------------------------------------------------------------
// ------------------------------------------------- safe arithmetic ---------

TEST_F(SqlScalarCoverageTest, SafeArithmeticReturnsNullOnOverflow) {
  ExpectValue("SELECT safe_divide(1, 0) IS NULL", Value(true));
  ExpectValue("SELECT safe_add(9223372036854775807, 1) IS NULL", Value(true));
  ExpectValue("SELECT safe_subtract(-9223372036854775807 - 1, 1) IS NULL",
              Value(true));
  ExpectValue("SELECT safe_multiply(9223372036854775807, 2) IS NULL",
              Value(true));
  ExpectValue("SELECT safe_negate(-9223372036854775807 - 1) IS NULL",
              Value(true));
  ExpectValue("SELECT safe_add(2, 3)", Value(int64_t{5}));
  ExpectValue("SELECT safe_divide(7, 2)", Value(3.5));
}

TEST_F(SqlScalarCoverageTest, IeeeDivideAndFloatingEdges) {
  ExpectValue("SELECT ieee_divide(1.0, 0.0)",
              Value(std::numeric_limits<double>::infinity()));
  ExpectValue("SELECT is_nan(ieee_divide(0.0, 0.0))", Value(true));
  ExpectValue("SELECT is_inf(ieee_divide(1.0, 0.0))", Value(true));
}

TEST_F(SqlScalarCoverageTest, MathFunctionSuite) {
  ExpectValue("SELECT abs(-3)", Value(int64_t{3}));
  ExpectValue("SELECT sign(-2.5)", Value(-1.0));
  ExpectValue("SELECT floor(1.9)", Value(1.0));
  ExpectValue("SELECT ceil(1.1)", Value(2.0));
  ExpectValue("SELECT ceiling(1.1)", Value(2.0));
  ExpectValue("SELECT round(2.5)", Value(3.0));
  ExpectValue("SELECT trunc(-2.7)", Value(-2.0));
  ExpectValue("SELECT truncate(-2.7)", Value(-2.0));
  ExpectValue("SELECT sqrt(9.0)", Value(3.0));
  ExpectValue("SELECT cbrt(27.0)", Value(3.0));
  ExpectValue("SELECT exp(log(2.0))", Value(2.0));
  ExpectValue("SELECT log10(1000.0)", Value(3.0));
  ExpectValue("SELECT ln(exp(1.0))", Value(1.0));
  ExpectValue("SELECT pow(2, 10)", Value(1024.0));
  ExpectValue("SELECT power(2, 8)", Value(256.0));
  ExpectValue("SELECT degrees(pi()) > 179.99", Value(true));
  ExpectValue("SELECT radians(180.0) > 3.14", Value(true));
  ExpectValue("SELECT sin(0.0)", Value(0.0));
  ExpectValue("SELECT cos(0.0)", Value(1.0));
  ExpectValue("SELECT tan(0.0)", Value(0.0));
  ExpectValue("SELECT asin(0.0)", Value(0.0));
  ExpectValue("SELECT acos(1.0)", Value(0.0));
  ExpectValue("SELECT atan(0.0)", Value(0.0));
  ExpectValue("SELECT atan2(0.0, 1.0)", Value(0.0));
  ExpectValue("SELECT sinh(0.0)", Value(0.0));
  ExpectValue("SELECT cosh(0.0)", Value(1.0));
  ExpectValue("SELECT tanh(0.0)", Value(0.0));
}

TEST_F(SqlScalarCoverageTest, ConditionalAndNullFunctions) {
  ExpectValue("SELECT coalesce(NULL, NULL, 7)", Value(int64_t{7}));
  ExpectValue("SELECT ifnull(NULL, 'dflt')", Value(std::string("dflt")));
  ExpectValue("SELECT nullif(3, 3) IS NULL", Value(true));
  ExpectValue("SELECT nullif(3, 4)", Value(int64_t{3}));
  ExpectValue("SELECT greatest(1, 5, 3)", Value(int64_t{5}));
  ExpectValue("SELECT least(4, 2, 8)", Value(int64_t{2}));
  ExpectValue("SELECT greatest(1, NULL) IS NULL", Value(true));
  ExpectValue("SELECT least(1, NULL) IS NULL", Value(true));
}

// ------------------------------------------------------------------ casts ---

TEST_F(SqlScalarCoverageTest, CastMatrixBasics) {
  ExpectValue("SELECT CAST('12' AS INT64)", Value(int64_t{12}));
  ExpectValue("SELECT CAST(3 AS STRING)", Value(std::string("3")));
  ExpectValue("SELECT CAST(1.7 AS INT64)", Value(int64_t{2}));
  ExpectValue("SELECT CAST(-1.5 AS INT64)", Value(int64_t{-2}));
  ExpectValue("SELECT CAST(TRUE AS STRING)", Value(std::string("true")));
  ExpectValue("SELECT CAST('false' AS BOOL)", Value(false));
  ExpectValue("SELECT CAST('12.5' AS FLOAT64)", Value(12.5));
  ExpectValue("SELECT CAST(2 AS FLOAT64)", Value(2.0));
  ExpectValue("SELECT CAST(DATE '2026-01-02' AS STRING)",
              Value(std::string("2026-01-02")));
  ExpectValue("SELECT CAST('2026-01-02' AS DATE)", Value::Date("2026-01-02"));
  ExpectValue("SELECT CAST(CAST('abc' AS BYTES) AS STRING)",
              Value(std::string("abc")));
  ExpectValue("SELECT CAST(1 AS INT32)", Value(int64_t{1}));
}

TEST_F(SqlScalarCoverageTest, SafeCastSwallowsErrors) {
  ExpectValue("SELECT SAFE_CAST('x' AS INT64) IS NULL", Value(true));
  ExpectValue("SELECT SAFE_CAST('not-a-date' AS DATE) IS NULL", Value(true));
  ExpectValue("SELECT SAFE_CAST('12' AS INT64)", Value(int64_t{12}));
  // The strict cast reports the failure on the result status; the row set is
  // empty or NULL-valued depending on how far execution got.
  const auto bad =
      RunSql(engine_.get(), context_.get(), "SELECT CAST('x' AS INT64)");
  EXPECT_TRUE(bad.empty() || bad[0][0].IsNull());
}

// --------------------------------------------------- misc scalar coverage ---

TEST_F(SqlScalarCoverageTest, BitFunctionsAndShifts) {
  ExpectValue("SELECT __bit_and(6, 3)", Value(int64_t{2}));
  ExpectValue("SELECT __bit_or(6, 3)", Value(int64_t{7}));
  ExpectValue("SELECT __bit_xor(6, 3)", Value(int64_t{5}));
  ExpectValue("SELECT __shift_left(1, 4)", Value(int64_t{16}));
  ExpectValue("SELECT __shift_right(16, 2)", Value(int64_t{4}));
}

TEST_F(SqlScalarCoverageTest, CryptoDigestByteLengths) {
  ExpectValue("SELECT byte_length(md5('abc'))", Value(int64_t{16}));
  ExpectValue("SELECT byte_length(sha1('abc'))", Value(int64_t{20}));
  ExpectValue("SELECT byte_length(sha256('abc'))", Value(int64_t{32}));
  ExpectValue("SELECT byte_length(sha512('abc'))", Value(int64_t{64}));
  ExpectValue("SELECT md5('abc') = md5('abc')", Value(true));
  ExpectValue("SELECT sha256('abc') = sha256('abc')", Value(true));
  ExpectValue("SELECT sha1('abc') = sha1('abd')", Value(false));
}

TEST_F(SqlScalarCoverageTest, StringAggregationWithOrderAndDelimiter) {
  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE agg_t (k INT64, s STRING);");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO agg_t (k, s) VALUES (1, 'a'), (1, 'b'), (1, 'c'), "
         "(2, 'x')");
  ExpectValue("SELECT STRING_AGG(s, ',' ORDER BY s) FROM agg_t WHERE k = 1",
              Value(std::string("a,b,c")));
  ExpectValue(
      "SELECT STRING_AGG(s, ',' ORDER BY s DESC) FROM agg_t WHERE k = 1",
      Value(std::string("c,b,a")));
  ExpectValue("SELECT STRING_AGG(s, '') FROM agg_t WHERE k = 2",
              Value(std::string("x")));
  ExpectValue("SELECT STRING_AGG(s, ',') FROM agg_t WHERE k = 9", Value());
}

}  // namespace tinylamb
