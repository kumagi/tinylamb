/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache License 2.0. */
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Exercises built-in scalar functions through the SQL engine (which routes
// them through the bytecode VM and expression evaluation), covering the
// runtime branches in function_call_expression.cpp / expression_eval.cpp that
// the AST-only Evaluate path leaves unimplemented.
class ScalarFunctionSqlTest : public ::testing::Test {
 protected:
  void SetUp() override { db_ = std::make_unique<Database>("fn_" + RandomString()); }

  std::vector<Row> RunSql(std::string_view sql) {
    TransactionContext ctx = db_->BeginContext();
    SqlEngine engine(*db_);
    auto prepared = engine.Prepare(ctx, sql);
    std::vector<Row> rows;
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) { rows.push_back(row); }
    } else {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
    }
    ctx.txn_.Abort();
    return rows;
  }

  Value Scalar(std::string_view sql) {
    auto rows = RunSql(sql);
    EXPECT_EQ(rows.size(), 1u) << sql;
    if (rows.empty()) { return Value(); }
    EXPECT_EQ(rows[0].values_.size(), 1u) << sql;
    return rows[0].values_[0];
  }

  std::unique_ptr<Database> db_;
};

TEST_F(ScalarFunctionSqlTest, MathUnaryFunctions) {
  EXPECT_EQ(Scalar("SELECT ABS(-5)"), Value(int64_t{5}));
  EXPECT_EQ(Scalar("SELECT SIGN(-3)"), Value(int64_t{-1}));
  EXPECT_EQ(Scalar("SELECT SIGN(0)"), Value(int64_t{0}));
  EXPECT_EQ(Scalar("SELECT SIGN(3)"), Value(int64_t{1}));
  EXPECT_DOUBLE_EQ(Scalar("SELECT SQRT(16.0)").value.double_value, 4.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT CBRT(27.0)").value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT FLOOR(2.7)").value.double_value, 2.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT CEIL(2.1)").value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT CEILING(2.1)").value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT ROUND(2.5)").value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT TRUNC(2.7)").value.double_value, 2.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT TRUNCATE(2.7)").value.double_value, 2.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT EXP(0.0)").value.double_value, 1.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT LN(1.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT SIN(0.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT COS(0.0)").value.double_value, 1.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT TAN(0.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT ASIN(0.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT ACOS(1.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT ATAN(0.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT DEGREES(0.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT RADIANS(0.0)").value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT PI()").value.double_value, 3.141592653589793);
}

TEST_F(ScalarFunctionSqlTest, MathBinaryFunctions) {
  EXPECT_DOUBLE_EQ(Scalar("SELECT POW(2.0, 10.0)").value.double_value, 1024.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT POWER(2.0, 3.0)").value.double_value, 8.0);
  EXPECT_DOUBLE_EQ(Scalar("SELECT LOG(100.0, 10.0)").value.double_value, 2.0);
  EXPECT_EQ(Scalar("SELECT MOD(10, 3)"), Value(int64_t{1}));
  EXPECT_EQ(Scalar("SELECT DIV(10, 3)"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT IEEE_DIVIDE(1.0, 2.0)"), Value(0.5));
  EXPECT_EQ(Scalar("SELECT SAFE_ADD(1, 2)"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT SAFE_SUBTRACT(5, 2)"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT SAFE_MULTIPLY(3, 4)"), Value(int64_t{12}));
  EXPECT_DOUBLE_EQ(Scalar("SELECT SAFE_DIVIDE(8, 2)").value.double_value, 4.0);
  EXPECT_EQ(Scalar("SELECT SAFE_NEGATE(8)"), Value(int64_t{-8}));
}

TEST_F(ScalarFunctionSqlTest, StringFunctions) {
  EXPECT_EQ(Scalar("SELECT CONCAT('a', 'b')"), Value("ab"));
  EXPECT_EQ(Scalar("SELECT LENGTH('hello')"), Value(int64_t{5}));
  EXPECT_EQ(Scalar("SELECT CHAR_LENGTH('hi')"), Value(int64_t{2}));
  EXPECT_EQ(Scalar("SELECT CHARACTER_LENGTH('hi')"), Value(int64_t{2}));
  EXPECT_EQ(Scalar("SELECT LOWER('ABC')"), Value("abc"));
  EXPECT_EQ(Scalar("SELECT UPPER('abc')"), Value("ABC"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('hello', 2)"), Value("ello"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('hello', 2, 3)"), Value("ell"));
  EXPECT_EQ(Scalar("SELECT SUBSTRING('hello', 2, 3)"), Value("ell"));
  // Negative start counts from the end (GoogleSQL semantics; the old
  // implementation clamped every start <= 1 to the first byte).
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', -2)"), Value("de"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', -2, 2)"), Value("de"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', -10)"), Value(""));
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', 0)"), Value("abcde"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', 10)"), Value(""));
  EXPECT_EQ(Scalar("SELECT LEFT('hello', 2)"), Value("he"));
  EXPECT_EQ(Scalar("SELECT RIGHT('hello', 2)"), Value("lo"));
  EXPECT_EQ(Scalar("SELECT TRIM('  x  ')"), Value("x"));
  EXPECT_EQ(Scalar("SELECT LTRIM('  x  ')"), Value("x  "));
  EXPECT_EQ(Scalar("SELECT RTRIM('  x  ')"), Value("  x"));
  EXPECT_EQ(Scalar("SELECT REVERSE('abc')"), Value("cba"));
  EXPECT_EQ(Scalar("SELECT REPEAT('ab', 3)"), Value("ababab"));
  EXPECT_EQ(Scalar("SELECT REPLACE('aaa', 'a', 'b')"), Value("bbb"));
  EXPECT_EQ(Scalar("SELECT TRANSLATE('abc', 'ab', 'xy')"), Value("xyc"));
  EXPECT_EQ(Scalar("SELECT STARTS_WITH('hello', 'he')"), Value(true));
  EXPECT_EQ(Scalar("SELECT ENDS_WITH('hello', 'lo')"), Value(true));
  EXPECT_EQ(Scalar("SELECT INSTR('hello', 'l')"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT STRPOS('hello', 'l')"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT ASCII('A')"), Value(int64_t{65}));
  EXPECT_EQ(Scalar("SELECT CHR(65)"), Value("A"));
  EXPECT_EQ(Scalar("SELECT UNICODE('A')"), Value(int64_t{65}));
  EXPECT_EQ(Scalar("SELECT INITCAP('hello world')"), Value("Hello World"));
  EXPECT_EQ(Scalar("SELECT LPAD('ab', 5, '*')"), Value("***ab"));
  EXPECT_EQ(Scalar("SELECT RPAD('ab', 5, '*')"), Value("ab***"));
}

TEST_F(ScalarFunctionSqlTest, ConditionalFunctions) {
  EXPECT_EQ(Scalar("SELECT COALESCE(NULL, 42)"), Value(int64_t{42}));
  EXPECT_EQ(Scalar("SELECT IFNULL(NULL, 7)"), Value(int64_t{7}));
  EXPECT_TRUE(Scalar("SELECT NULLIF(1, 1)").IsNull());
  EXPECT_EQ(Scalar("SELECT NULLIF(1, 2)"), Value(int64_t{1}));
  EXPECT_EQ(Scalar("SELECT GREATEST(1, 2, 3)"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT LEAST(1, 2, 3)"), Value(int64_t{1}));
  EXPECT_EQ(Scalar("SELECT IF(true, 1, 2)"), Value(int64_t{1}));
  EXPECT_EQ(Scalar("SELECT IF(false, 1, 2)"), Value(int64_t{2}));
}

TEST_F(ScalarFunctionSqlTest, RegexpFunctions) {
  EXPECT_EQ(Scalar("SELECT REGEXP_CONTAINS('hello', 'ell')"), Value(true));
  EXPECT_EQ(Scalar("SELECT REGEXP_CONTAINS('hello', 'xyz')"), Value(false));
  EXPECT_EQ(Scalar("SELECT REGEXP_REPLACE('hello', 'l', 'L')"), Value("heLLo"));
}

}  // namespace
}  // namespace tinylamb
