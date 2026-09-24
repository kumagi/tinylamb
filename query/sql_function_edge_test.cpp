/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Characterization sweep for scalar functions (expression/
// function_call_expression.cpp ExecuteFunction) and civil-time parsing.
// Each pinned case asserts the recorded engine result or error; the
// unpinned sweep executes behavior currently under review for
// GoogleSQL compliance without endorsing a specific value.

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
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
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    if (engine->LastError().empty()) {
      prepared = engine->Prepare(*ctx, sql);
    }
    if (!prepared.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
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

std::string Repr(const Value& value) {
  std::ostringstream os;
  os << value;
  return os.str();
}

}  // namespace

class SqlFunctionEdgeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_function_edge_test-" + RandomString())
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
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

// Pinned results: one row, first column equals the recorded repr.
TEST_F(SqlFunctionEdgeTest, PinnedScalarTable) {
  struct Case {
    const char* sql;
    const char* expected;
  };
  const std::vector<Case> cases = {
      {"SELECT FORMAT('%d', 42)", R"sql("42")sql"},
      {"SELECT FORMAT('%i', -7)", R"sql("-7")sql"},
      {"SELECT FORMAT('%x', 255)", R"sql("ff")sql"},
      {"SELECT FORMAT('%#x', 255)", R"sql("0xff")sql"},
      {"SELECT FORMAT('%X', 48879)", R"sql("BEEF")sql"},
      {"SELECT FORMAT('%#X', 48879)", R"sql("0XBEEF")sql"},
      {"SELECT FORMAT('%o', 8)", R"sql("10")sql"},
      {"SELECT FORMAT('%#o', 8)", R"sql("010")sql"},
      {"SELECT FORMAT('%e', 1234.5678)", R"sql("1234.567800")sql"},
      {"SELECT FORMAT('%f', 3.14159)", R"sql("3.141590")sql"},
      {"SELECT FORMAT('%.2f', 3.14159)", R"sql("3.14")sql"},
      {"SELECT FORMAT('%g', 0.00001234)", R"sql("0.000012")sql"},
      {"SELECT FORMAT('%s', 'abc')", R"sql("abc")sql"},
      {R"sql(SELECT FORMAT('%t', 'a"b\c'))sql", R"(""a\"bc"")"},
      {"SELECT FORMAT('%t', DATE '2026-01-02')", R"sql("2026-01-02")sql"},
      {"SELECT FORMAT('%t', 3.5)", R"sql("3.500000")sql"},
      {"SELECT FORMAT('%%')", R"sql("%")sql"},
      {"SELECT FORMAT('100%%')", R"sql("100%")sql"},
      {"SELECT FORMAT('%10s|', 'ab')", R"sql("        ab|")sql"},
      {"SELECT FORMAT('%05d', 42)", R"sql("00042")sql"},
      {"SELECT FORMAT('%*d', 6, 42)", R"sql("    42")sql"},
      {"SELECT FORMAT('%.*f', 2, 3.14159)", R"sql("3.14")sql"},
      {"SELECT FORMAT('%d', NULL)", "NULL"},
      {"SELECT FORMAT('%s', NULL)", "NULL"},
      {"SELECT FORMAT(NULL, 1)", "NULL"},
      {"SELECT FORMAT('%*d', NULL, 1)", "NULL"},
      {"SELECT FORMAT('%.*f', NULL, 1.5)", "NULL"},
      {"SELECT FORMAT('%q', 1)", R"sql("%q")sql"},
      {"SELECT FORMAT('%t', true)", R"sql("1")sql"},
      {"SELECT FORMAT('%t', false)", R"sql("0")sql"},
      {"SELECT FORMAT('%t', NULL)", "NULL"},
      {"SELECT FORMAT('%t', TIMESTAMP '2026-01-02 03:04:05+00')",
       R"(""2026-01-02 03:04:05+00"")"},
      {"SELECT FORMAT('%t', DATETIME '2026-01-02T03:04:05')",
       R"(""2026-01-02T03:04:05"")"},
      {"SELECT ABS(NULL)", "NULL"},
      {"SELECT SIGN(NULL)", "NULL"},
      {"SELECT CEIL(NULL)", "NULL"},
      {"SELECT FLOOR(NULL)", "NULL"},
      {"SELECT ROUND(NULL)", "NULL"},
      {"SELECT CEIL(2.5)", "3"},
      {"SELECT CEIL(-2.5)", "-2"},
      {"SELECT CEILING(2.5)", "3"},
      {"SELECT FLOOR(2.5)", "2"},
      {"SELECT FLOOR(-2.5)", "-3"},
      {"SELECT ROUND(2.567, 1)", "2.6"},
      {"SELECT ROUND(2.567, -1)", "0"},
      {"SELECT ROUND(-2.5)", "-3"},
      {"SELECT ROUND(2.5)", "3"},
      {"SELECT TRUNC(2.7)", "2"},
      {"SELECT TRUNC(-2.7)", "-2"},
      {"SELECT TRUNCATE(2.7)", "2"},
      {"SELECT TRUNC(123.456, -2)", "100"},
      {"SELECT TRUNC(123.456, 2)", "123.45"},
      {"SELECT TRUNCATE(123.456, 2)", "123.45"},
      {"SELECT MOD(10, 3)", "1"},
      {"SELECT MOD(-10, 3)", "-1"},
      {"SELECT MOD(10.5, 3)", "1.5"},
      {"SELECT DIV(10, 3)", "3"},
      {"SELECT DIV(-10, 3)", "-3"},
      {"SELECT DIV(10.0, 3.0)", "3"},
      {"SELECT SAFE_DIVIDE(10, 0)", "NULL"},
      {"SELECT SAFE_MULTIPLY(9223372036854775807, 2)", "NULL"},
      {"SELECT SAFE_ADD(9223372036854775807, 1)", "NULL"},
      {"SELECT SAFE_SUBTRACT(-9223372036854775807 - 1, 1)", "NULL"},
      {"SELECT SAFE_NEGATE(-9223372036854775807 - 1)", "NULL"},
      {"SELECT SAFE_NEGATE(5)", "-5"},
      {"SELECT IEEE_DIVIDE(1, 0)", "inf"},
      {"SELECT IEEE_DIVIDE(0, 0)", "nan"},
      {"SELECT IEEE_DIVIDE(7, 2)", "3.5"},
      {"SELECT IS_INF(1.5)", "0"},
      {"SELECT IS_NAN(IEEE_DIVIDE(0, 0))", "1"},
      {"SELECT IS_NAN(1.5)", "0"},
      {"SELECT SQRT(2)", "1.4142135623730951"},
      {"SELECT CBRT(27)", "3"},
      {"SELECT CBRT(-27)", "-3"},
      {"SELECT EXP(1)", "2.718281828459045"},
      {"SELECT EXP(0)", "1"},
      {"SELECT LN(1)", "0"},
      {"SELECT LN(0)", "-inf"},
      {"SELECT LN(-1)", "nan"},
      {"SELECT LOG(100)", "4.605170185988092"},
      {"SELECT LOG(8, 2)", "3"},
      {"SELECT LOG(0, 2)", "-inf"},
      {"SELECT LOG(-1, 2)", "nan"},
      {"SELECT LOG10(1000)", "3"},
      {"SELECT LOG10(0)", "-inf"},
      {"SELECT POW(2, 10)", "1024"},
      {"SELECT POW(2, 0.5)", "1.4142135623730951"},
      {"SELECT POWER(2, -2)", "0.25"},
      {"SELECT GREATEST(1, 5, 3)", "5"},
      {"SELECT GREATEST(1, NULL, 3)", "NULL"},
      {"SELECT GREATEST('a', 'b')", R"sql("b")sql"},
      {"SELECT LEAST(1, 5, 3)", "1"},
      {"SELECT LEAST(1, NULL, 3)", "NULL"},
      {"SELECT PI()", "3.141592653589793"},
      {"SELECT DEGREES(PI())", "180"},
      {"SELECT RADIANS(180)", "3.141592653589793"},
      {"SELECT RAND() IS NOT NULL", "1"},
      {"SELECT SIGN(-2.5)", "-1"},
      {"SELECT SIGN(0.0)", "0"},
      {"SELECT ABS(9223372036854775807)", "9223372036854775807"},
      {"SELECT CONCAT('a', 'b', 'c')", R"sql("abc")sql"},
      {"SELECT CONCAT('a', NULL, 'c')", "NULL"},
      {"SELECT CONCAT(1, 2.5, true)", R"sql("12.5true")sql"},
      {"SELECT CONCAT()", R"sql("")sql"},
      {"SELECT LENGTH('h\xc3\xa9llo')",
       "5"},  // NOLINT(modernize-raw-string-literal)
      {"SELECT BYTE_LENGTH('h\xc3\xa9llo')",
       "6"},  // NOLINT(modernize-raw-string-literal)
      {"SELECT CHAR_LENGTH('h\xc3\xa9llo')",
       "5"},  // NOLINT(modernize-raw-string-literal)
      {"SELECT CHARACTER_LENGTH('h\xc3\xa9llo')",
       "5"},  // NOLINT(modernize-raw-string-literal)
      {"SELECT OCTET_LENGTH('h\xc3\xa9llo')",
       "6"},  // NOLINT(modernize-raw-string-literal)
      {"SELECT LENGTH('')", "0"},
      {"SELECT ASCII('A')", "65"},
      {"SELECT ASCII('')", "0"},
      {"SELECT UNICODE('\xe2\x82\xac')",
       "8364"},  // NOLINT(modernize-raw-string-literal)
      {"SELECT UNICODE('A')", "65"},
      {"SELECT CHR(65)", R"sql("A")sql"},
      {"SELECT CHR(8364)",
       "\"\xe2\x82\xac\""},  // NOLINT(modernize-raw-string-literal)
      {"SELECT UPPER('abc')", R"sql("ABC")sql"},
      {"SELECT LOWER('ABC')", R"sql("abc")sql"},
      {"SELECT INITCAP('hello world')", R"sql("Hello World")sql"},
      {"SELECT INITCAP('hELLO wORLD-foo_bar')",
       R"sql("Hello World-Foo_Bar")sql"},
      {"SELECT LEFT('abcdef', 3)", R"sql("abc")sql"},
      {"SELECT LEFT('abc', 10)", R"sql("abc")sql"},
      {"SELECT LEFT('abc', 0)", R"sql("")sql"},
      {"SELECT RIGHT('abcdef', 3)", R"sql("def")sql"},
      {"SELECT RIGHT('abc', 10)", R"sql("abc")sql"},
      {"SELECT SUBSTR('abcdef', 2)", R"sql("bcdef")sql"},
      {"SELECT SUBSTR('abcdef', 2, 3)", R"sql("bcd")sql"},
      {"SELECT SUBSTR('abcdef', -3, 2)", R"sql("de")sql"},
      {"SELECT SUBSTR('abcdef', 10)", R"sql("")sql"},
      {"SELECT SUBSTR('abcdef', 2, 0)", R"sql("")sql"},
      {"SELECT SUBSTRING('abcdef', 2, 2)", R"sql("bc")sql"},
      {"SELECT LPAD('ab', 5)", R"sql("   ab")sql"},
      {"SELECT LPAD('ab', 5, '*')", R"sql("***ab")sql"},
      {"SELECT LPAD('abcde', 3, '*')", R"sql("abc")sql"},
      {"SELECT LPAD('ab', 0)", R"sql("")sql"},
      {"SELECT LPAD('ab', 5, NULL)", "NULL"},
      {"SELECT RPAD('ab', 5)", R"sql("ab   ")sql"},
      {"SELECT RPAD('ab', 5, '*')", R"sql("ab***")sql"},
      {"SELECT RPAD('abcde', 3, '*')", R"sql("abc")sql"},
      {"SELECT LTRIM('  ab  ')", R"sql("ab  ")sql"},
      {"SELECT RTRIM('  ab  ')", R"sql("  ab")sql"},
      {"SELECT TRIM('  ab  ')", R"sql("ab")sql"},
      {"SELECT LTRIM('xxabxx', 'x')", R"sql("abxx")sql"},
      {"SELECT RTRIM('xxabxx', 'x')", R"sql("xxab")sql"},
      {"SELECT TRIM('xxabxx', 'x')", R"sql("ab")sql"},
      {"SELECT TRIM('xyabxy', 'xy')", R"sql("ab")sql"},
      {"SELECT REVERSE('abc')", R"sql("cba")sql"},
      {"SELECT REVERSE('')", R"sql("")sql"},
      {"SELECT REPEAT('ab', 3)", R"sql("ababab")sql"},
      {"SELECT REPEAT('ab', 0)", R"sql("")sql"},
      {"SELECT REPLACE('aaa', 'a', 'bb')", R"sql("bbbbbb")sql"},
      {"SELECT REPLACE('abc', '', 'x')", R"sql("abc")sql"},
      {"SELECT REPLACE('abc', 'z', 'x')", R"sql("abc")sql"},
      {"SELECT INSTR('abcabc', 'b')", "2"},
      {"SELECT INSTR('abcabc', 'b', 3)", "5"},
      {"SELECT INSTR('abcabc', 'b', 3, 2)", "0"},
      {"SELECT INSTR('abcabc', 'z')", "0"},
      {"SELECT INSTR('abcabc', 'b', -1)", "5"},
      {"SELECT TRANSLATE('hello', 'el', 'ip')", R"sql("hippo")sql"},
      {"SELECT TRANSLATE('hello', '', 'x')", R"sql("hello")sql"},
      {"SELECT SOUNDEX('Robert')", R"sql("R163")sql"},
      {"SELECT SOUNDEX('Rupert')", R"sql("R163")sql"},
      {"SELECT SOUNDEX('')", R"sql("")sql"},
      {"SELECT STARTS_WITH('hello', 'he')", "1"},
      {"SELECT STARTS_WITH('hello', 'x')", "0"},
      {"SELECT ENDS_WITH('hello', 'lo')", "1"},
      {"SELECT ENDS_WITH('hello', 'x')", "0"},
      {"SELECT STRPOS('hello', 'll')", "3"},
      {"SELECT STRPOS('hello', 'z')", "0"},
      {"SELECT SPLIT('a,b,c', ',')", R"(ARRAY<STRING>["a", "b", "c"])"},
      {"SELECT SPLIT('a,b,c')", R"(ARRAY<STRING>["a", "b", "c"])"},
      {"SELECT SPLIT('a1b1c', '1')", R"(ARRAY<STRING>["a", "b", "c"])"},
      {"SELECT TO_HEX(MD5('abc'))",
       R"sql("900150983cd24fb0d6963f7d28e17f72")sql"},
      {"SELECT TO_HEX(SHA1('abc'))",
       R"sql("a9993e364706816aba3e25717850c26c9cd0d89d")sql"},
      {"SELECT TO_HEX(SHA256('abc'))",
       R"sql("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")sql"},
      {"SELECT TO_HEX(SHA512('abc'))",
       R"sql("ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a21929)sql"
       R"sql(92a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f")sql"},
      {"SELECT md5('abc')",
       "\"\x90\x01P\x98<\xd2O\xb0\xd6\x96?}(\xe1r\""},  // NOLINT(modernize-raw-string-literal)
      {"SELECT SHA1('abc')",
       "\"\xa9\x99>6G\x06\x81j\xba>%qxP\xc2l\x9c\xd0\xd8\x9d\""},  // NOLINT(modernize-raw-string-literal)
      {"SELECT __bit_and(6, 3)", "2"},
      {"SELECT __bit_or(6, 3)", "7"},
      {"SELECT __bit_xor(6, 3)", "5"},
      {"SELECT __bit_and(-1, 3)", "3"},
      {"SELECT __shift_left(1, 4)", "16"},
      {"SELECT __shift_right(16, 2)", "4"},
      {"SELECT __shift_left(1, 64)", "0"},
      {"SELECT CODE_POINTS_TO_BYTES([104, 105])", R"sql("hi")sql"},
      {"SELECT CODE_POINTS_TO_STRING([104, 8364])",
       "\"h\xe2\x82\xac\""},  // NOLINT(modernize-raw-string-literal)
      {"SELECT BYTE_LEFT('abc', 2)", R"sql("ab")sql"},
      {"SELECT BYTE_RIGHT('abc', 2)", R"sql("bc")sql"},
      {"SELECT BYTE_REVERSE('abc')", R"sql("cba")sql"},
      {"SELECT BYTE_SUBSTR('abcde', 2)", R"sql("bcde")sql"},
      {"SELECT BYTE_SUBSTR('abcde', 2, 2)", R"sql("bc")sql"},
      {"SELECT BYTE_LENGTH('abc')", "3"},
      {"SELECT REGEXP_CONTAINS('hello', 'l+')", "1"},
      {"SELECT REGEXP_EXTRACT('hello 42 world', '[0-9]+')", R"sql("42")sql"},
      {"SELECT REGEXP_EXTRACT('hello', 'xyz')", "NULL"},
      {"SELECT REGEXP_EXTRACT_ALL('a1b2', '[0-9]')",
       R"(ARRAY<STRING>["1", "2"])"},
      {"SELECT REGEXP_EXTRACT_ALL('abc', '[0-9]')", "ARRAY<STRING>[]"},
      {"SELECT REGEXP_INSTR('abcabc', 'b')", "2"},
      {"SELECT REGEXP_INSTR('abcabc', 'z')", "0"},
      {"SELECT REGEXP_MATCH('hello', 'h.*o')", "1"},
      {"SELECT REGEXP_REPLACE('a1b2', '[0-9]', '_')", R"sql("a_b_")sql"},
      {"SELECT BYTE_REGEXP_EXTRACT_ALL('a1b2', '[0-9]')",
       R"(ARRAY<BYTES>["1", "2"])"},
      {"SELECT BYTE_REGEXP_INSTR('abcabc', 'b')", "2"},
      {"SELECT IFNULL(NULL, 'd')", R"sql("d")sql"},
      {"SELECT IFNULL('x', 'd')", R"sql("x")sql"},
      {"SELECT NULLIF('a', 'a')", "NULL"},
      {"SELECT NULLIF('a', 'b')", R"sql("a")sql"},
      {"SELECT COALESCE(NULL, NULL, 3)", "3"},
      {"SELECT COALESCE()", "NULL"},
      {"SELECT ABS(-2.5)", "2.5"},
      {"SELECT EXTRACT(DAY FROM DATE '2026-01-02')", "2"},
      {"SELECT EXTRACT(MONTH FROM DATE '2026-01-02')", "1"},
      {"SELECT EXTRACT(YEAR FROM DATE '2026-01-02')", "2026"},
      {"SELECT EXTRACT(QUARTER FROM DATE '2026-05-02')", "2"},
      {"SELECT CURRENT_DATE() IS NOT NULL", "1"},
      {"SELECT CURRENT_DATETIME() IS NOT NULL", "1"},
      {"SELECT CURRENT_TIMESTAMP() IS NOT NULL", "1"},
      {"SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:01+00')", "1"},
      {"SELECT UNIX_MILLIS(TIMESTAMP '1970-01-01 00:00:01+00')", "1000"},
      {"SELECT UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:01+00')", "1000000"},
      {"SELECT UNIX_DATE(DATE '1970-01-02')", "1"},
      {"SELECT GENERATE_ARRAY(1, 5)", "ARRAY<INT64>[1, 2, 3, 4, 5]"},
      {"SELECT GENERATE_ARRAY(1, 10, 3)", "ARRAY<INT64>[1, 4, 7, 10]"},
      {"SELECT GENERATE_ARRAY(5, 1, -2)", "ARRAY<INT64>[5, 3, 1]"},
      {"SELECT GENERATE_DATE_ARRAY(DATE '2026-01-01', DATE '2026-01-05')",
       "ARRAY<DATE>[2026-01-01, 2026-01-02, 2026-01-03, 2026-01-04, "
       "2026-01-05]"},
      {"SELECT GENERATE_DATE_ARRAY(DATE '2026-01-01', DATE '2026-01-05', "
       "INTERVAL 1 DAY)",
       "ARRAY<DATE>[2026-01-01, 2026-01-02, 2026-01-03, 2026-01-04, "
       "2026-01-05]"},
      {R"sql(SELECT JSON_EXTRACT('{"a": 1}', '$.a'))sql", R"sql("1")sql"},
      {R"sql(SELECT JSON_EXTRACT('{"a": {"b": 2}}', '$.a.b'))sql",
       R"sql("2")sql"},
      {"SELECT JSON_EXTRACT('[1,2,3]', '$[1]')", R"sql("2")sql"},
      {R"sql(SELECT JSON_EXTRACT('{"a": 1}', '$.z'))sql", "NULL"},
      {"SELECT JSON_EXTRACT('not json', '$.a')", "NULL"},
      {R"sql(SELECT JSON_EXTRACT_SCALAR('{"a": "x"}', '$.a'))sql",
       R"sql("x")sql"},
      {R"sql(SELECT JSON_EXTRACT_SCALAR('{"a": 1}', '$.a'))sql",
       R"sql("1")sql"},
      {"SELECT JSON_EXTRACT_ARRAY('[1,2]', '$')", R"(ARRAY<STRING>["1", "2"])"},
      {R"sql(SELECT JSON_EXTRACT_STRING_ARRAY('["a","b"]', '$'))sql",
       R"(ARRAY<STRING>["a", "b"])"},
      {R"sql(SELECT JSON_QUERY('{"a": 1}', '$.a'))sql", R"sql("1")sql"},
      {R"sql(SELECT JSON_VALUE('{"a": 1}', '$.a'))sql", R"sql("1")sql"},
      {R"sql(SELECT JSON_VALUE_ARRAY('["a","b"]', '$'))sql",
       R"(ARRAY<STRING>["a", "b"])"},
      {"SELECT JSON_EXTRACT_SCALAR(STRUCT(1 AS a, 'x' AS b), '$.a')",
       R"sql("1")sql"},
      {"SELECT GET_FIELD(STRUCT(1 AS a), 'z')", "NULL"},
      {"SELECT __struct_json__(1, 'x')", R"("{"1":"x"}")"},
      {"SELECT ARRAY_LENGTH([1,2,3])", "3"},
      {"SELECT ARRAY_REVERSE([1,2,3])", "ARRAY<INT64>[3, 2, 1]"},
      {"SELECT FORMAT_DATE('%Y-%m-%d', DATE '2026-01-02')",
       R"sql("2026-01-02")sql"},
      {"SELECT FORMAT_DATE('%b %e', DATE '2026-03-05')", R"sql("Mar  5")sql"},
      {"SELECT PARSE_DATE('%Y-%m-%d', '2026-01-02')", "2026-01-02"},
      {"SELECT PARSE_DATE('%d/%m/%Y', '02/01/2026')", "2026-01-02"},
      {"SELECT FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', DATETIME "
       "'2026-01-02T03:04:05')",
       R"sql("2026-01-01T19:04:05")sql"},
      {"SELECT FORMAT_DATETIME('%F %T', DATETIME '2026-01-02T03:04:05')",
       R"sql("2026-01-01 19:04:05")sql"},
      {"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', TIMESTAMP '2026-01-02 "
       "03:04:05+00')",
       R"sql("2026-01-01 19:04:05+0000")sql"},
      {"SELECT FORMAT_TIMESTAMP('%Ez', TIMESTAMP '2026-01-02 03:04:05+00')",
       R"sql("+0000")sql"},
      {"SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2026-01-02 03:04:05')",
       R"sql("2026-01-02 11:04:05+00")sql"},
      {"SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %z', '2026-01-02 03:04:05 "
       "+0530')",
       R"sql("2026-01-02 11:04:05+00")sql"},
      {"SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', '2026-01-02 03:04:05 "
       "UTC')",
       R"sql("2026-01-02 11:04:05+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 03:04:05.123456789+00'",
       R"sql("2026-01-02 03:04:05.123456789+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 03:04:05+05:30'",
       R"sql("2026-01-01 21:34:05+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 03:04:05-08:00'",
       R"sql("2026-01-02 11:04:05+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 03:04:05 UTC'",
       R"sql("2026-01-02 03:04:05+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 03:04:05 JST'",
       R"sql("2026-01-02 11:04:05+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 03:04'",
       R"sql("2026-01-02 11:04:00+00")sql"},
      {"SELECT DATETIME '2026-01-02T03:04:05'",
       R"sql("2026-01-02T03:04:05")sql"},
      {"SELECT DATETIME '2026-01-02 03:04:05.5'",
       R"sql("2026-01-02 03:04:05.5")sql"},
      {"SELECT DATETIME '03:04:05'", R"sql("03:04:05")sql"},
      {"SELECT TIMESTAMP '2026-02-29 00:00:00+00'",
       R"sql("2026-03-01 00:00:00+00")sql"},
      {"SELECT TIMESTAMP '2026-01-02 23:59:60+00'",
       R"sql("2026-01-03 00:00:00+00")sql"},
      {"SELECT EXTRACT(WEEK FROM DATE '2026-01-01')", "0"},
      {"SELECT EXTRACT(DAYOFYEAR FROM DATE '2026-05-01')", "121"},
      {"SELECT EXTRACT(ISOYEAR FROM DATE '2026-01-01')", "2026"},
      {"SELECT EXTRACT(ISOWEEK FROM DATE '2026-01-01')", "1"},
      {"SELECT EXTRACT(SECOND FROM DATETIME '2026-01-02T03:04:05.123')", "5"},
      {"SELECT EXTRACT(MILLISECOND FROM DATETIME '2026-01-02T03:04:05.123')",
       "123"},
      {"SELECT EXTRACT(MICROSECOND FROM DATETIME '2026-01-02T03:04:05.123456')",
       "123456"},
      {"SELECT EXTRACT(MINUTE FROM DATETIME '2026-01-02T03:04:05')", "4"},
      {"SELECT EXTRACT(HOUR FROM DATETIME '2026-01-02T03:04:05')", "3"},
      {"SELECT EXTRACT(DATE FROM TIMESTAMP '2026-01-02 03:04:05+00')",
       "2026-01-02"},
      {"SELECT EXTRACT(DATETIME FROM TIMESTAMP '2026-01-02 03:04:05+00')",
       R"sql("2026-01-02 03:04:05")sql"},
      {"SELECT EXTRACT(TIME FROM TIMESTAMP '2026-01-02 03:04:05+00')",
       R"sql("03:04:05")sql"},
      {"SELECT EXTRACT(YEAR FROM TIMESTAMP '2026-01-02 03:04:05+00')", "2026"},
      {"SELECT EXTRACT(NANOSECOND FROM DATETIME "
       "'2026-01-02T03:04:05.123456789')",
       "123456789"},
      {"SELECT DATE_TRUNC(DATE '2026-05-04', MONTH)", "2026-05-01"},
      {"SELECT DATE_TRUNC(DATE '2026-05-04', YEAR)", "2026-01-01"},
      {"SELECT DATE_TRUNC(DATE '2026-05-04', WEEK)", "2026-05-03"},
      {"SELECT DATE_TRUNC(DATE '2026-05-04', QUARTER)", "2026-04-01"},
      {"SELECT DATETIME_TRUNC(DATETIME '2026-05-04T03:04:05', HOUR)",
       R"sql("2026-05-04 03:00:00")sql"},
      {"SELECT DATETIME_TRUNC(DATETIME '2026-05-04T03:04:05', DAY)",
       R"sql("2026-05-04 00:00:00")sql"},
      {"SELECT DATETIME_TRUNC(DATETIME '2026-05-04T03:04:05', MINUTE)",
       R"sql("2026-05-04 03:04:00")sql"},
      {"SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-05-04 03:04:05+00', HOUR)",
       R"sql("2026-05-04 03:00:00+00")sql"},
      {"SELECT LAST_DAY(DATE '2026-02-05')", "2026-02-28"},
      {"SELECT LAST_DAY(DATE '2026-02-05', MONTH)", "2026-02-28"},
      {"SELECT LAST_DAY(DATE '2024-02-05', YEAR)", "2024-12-31"},
      {"SELECT LAST_DAY(DATE '2026-05-05', QUARTER)", "2026-06-30"},
      {"SELECT DATE_ADD(DATE '2026-01-02', INTERVAL 5 DAY)", "2026-01-07"},
      {"SELECT DATE_SUB(DATE '2026-01-02', INTERVAL 5 DAY)", "2025-12-28"},
      {"SELECT DATE(DATETIME '2026-01-02T03:04:05')", "2026-01-02"},
      {"SELECT DATE(TIMESTAMP '2026-01-02 03:04:05+00')", "2026-01-01"},
      {"SELECT DATE('2026-01-02')", "2026-01-02"},
      {"SELECT DATETIME(2026, 1, 2, 3, 4, 5)",
       R"sql("2026-01-02 03:04:05")sql"},
      {"SELECT TIME(3, 4, 5)", R"sql("03:04:05")sql"},
      {"SELECT TIME(DATETIME '2026-01-02T03:04:05')", R"sql("03:04:05")sql"},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.sql);
    const auto rows = RunSql(engine_.get(), context_.get(), c.sql);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(Repr(rows[0][0]), c.expected);
  }
}

TEST_F(SqlFunctionEdgeTest, UnicodeCaseAndFloatingPrecision) {
  const auto scalar_rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT UPPER('äöü'), LOWER('ÉÀÇ'), "
             "INITCAP('éCOLE ψΥΧΗ'), FORMAT('%.2f', 3.14159)");
  ASSERT_EQ(scalar_rows.size(), 1U);
  EXPECT_EQ(scalar_rows[0][0], Value("ÄÖÜ"));
  EXPECT_EQ(scalar_rows[0][1], Value("éàç"));
  EXPECT_EQ(scalar_rows[0][2], Value("École Ψυχη"));
  EXPECT_EQ(scalar_rows[0][3], Value("3.14"));

  RunSql(engine_.get(), context_.get(),
         "CREATE TABLE unicode_case_values (s STRING, v FLOAT64)");
  RunSql(engine_.get(), context_.get(),
         "INSERT INTO unicode_case_values VALUES ('éCOLE', 3.14159)");
  const auto row_rows =
      RunSql(engine_.get(), context_.get(),
             "SELECT UPPER(s), LOWER(s), INITCAP(s), FORMAT('%.2f', v) "
             "FROM unicode_case_values");
  ASSERT_EQ(row_rows.size(), 1U);
  EXPECT_EQ(row_rows[0][0], Value("ÉCOLE"));
  EXPECT_EQ(row_rows[0][1], Value("école"));
  EXPECT_EQ(row_rows[0][2], Value("École"));
  EXPECT_EQ(row_rows[0][3], Value("3.14"));
}

// Pinned failures: the statement must raise through the SQL boundary.
TEST_F(SqlFunctionEdgeTest, ErrorTable) {
  const std::vector<std::string> cases = {
      "SELECT FORMAT('%d')",
      "SELECT FORMAT('%d', 'x')",
      "SELECT FORMAT('%*d', 'x', 1)",
      "SELECT FORMAT('%.*f', 'x', 1.5)",
      "SELECT FORMAT('%d %d', 1)",
      "SELECT IS_INF(9.9e308 * 10)",
      "SELECT SQRT(-1)",
      "SELECT CONCAT_WS('-', 'a', 'b', NULL, 'c')",
      "SELECT CHR(-1)",
      "SELECT CHR(55296)",
      "SELECT CHR(57343)",
      "SELECT CHR(1114112)",
      "SELECT LEFT('abc', -1)",
      "SELECT RIGHT('abc', -1)",
      "SELECT SUBSTR('abcdef', 2, -1)",
      "SELECT LPAD('ab', -1)",
      "SELECT REPEAT('ab', -1)",
      "SELECT STRING('a', 'b')",
      "SELECT STR_VALUE(1)",
      "SELECT __shift_right(1, -1)",
      "SELECT CODE_POINTS_TO_STRING([-1])",
      "SELECT CODE_POINTS_TO_BYTES([256])",
      "SELECT TRANSLATE('aba', 'aa', 'xy')",
      "SELECT TO_CODE_POINTS('h\xe2\x82\xac')",  // NOLINT(modernize-raw-string-literal)
      "SELECT REGEXP_CONTAINS('hello', '(')",
      "SELECT REGEXP_REPLACE('abc', '(', 'x')",
      "SELECT BOOL_VAL(true)",
      "SELECT BOOL_VAL(1)",
      "SELECT STR_VALUE('x')",
      "SELECT MAKE_INTERVAL(day => 4)",
      "SELECT GENERATE_ARRAY(1, 5, 0)",
      "SELECT GENERATE_DATE_ARRAY(DATE '2026-01-01', DATE '2026-01-05', "
      "INTERVAL 2 MONTH)",
      "SELECT PARSE_DATE('%Y-%m-%d', 'bad')",
      "SELECT PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', '2026-01-02T03:04:05')",
      "SELECT PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', "
      "'2026-01-02T03:04:05.123456+05:30')",
      "SELECT PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', 'bad')",
      "SELECT DATE '2026-01-02' + INTERVAL 24 HOUR",
      "SELECT EXTRACT(WEEKDAY FROM DATE '2026-01-01')",
      "SELECT DATE(2026, 1, 2)",
      "SELECT DATE('bad')",
      "SELECT DATETIME(DATE '2026-01-02', TIME 03:04:05)",
      "SELECT NEW_UUID()",
  };
  for (const auto& sql : cases) {
    SCOPED_TRACE(sql);
    EXPECT_THROW(
        { RunSql(engine_.get(), context_.get(), sql); }, std::exception);
  }
}

// Executed without value pinning: behavior currently deviates from
// GoogleSQL (review backlog); coverage keeps the branches exercised.
TEST_F(SqlFunctionEdgeTest, DeviationSweep) {
  const std::vector<std::string> cases = {
      "SELECT FORMAT('%-10s|', 'ab')",
      "SELECT CHR(0)",
      "SELECT SUBSTR('abcdef', 0, 3)",
      "SELECT MAKE_INTERVAL(1, 2, 3)",
      "SELECT '2026-01-02' + INTERVAL 5 DAY",
      "SELECT 'x' + INTERVAL 5 DAY",
      // A non-numeric interval amount parses to a zero interval today
      // (IntervalValue::TryParse saturates to 0); deviation kept for coverage.
      "SELECT DATE '2026-01-02' + INTERVAL 'foo' DAY",
      "SELECT DATETIME '2026-13-01'",
      "SELECT DATETIME '2026-01-02T24:00:00'",
      "SELECT DATETIME '2026-02-29'",
      "SELECT TIMESTAMP '2026-12-31 23:59:59+00' + INTERVAL 1 SECOND",
  };
  for (const auto& sql : cases) {
    SCOPED_TRACE(sql);
    EXPECT_NO_THROW({ RunSql(engine_.get(), context_.get(), sql); });
  }
}

}  // namespace tinylamb
