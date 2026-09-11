#include <cmath>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "query/sql_engine.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/rewrite.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Exercises built-in scalar functions through the SQL engine (which routes
// them through the bytecode VM and expression evaluation), covering the
// runtime branches in function_call_expression.cpp / expression_eval.cpp that
// the AST-only Evaluate path leaves unimplemented.
class ScalarFunctionSqlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_name_ = "fn_" + RandomString();
    db_ = Database::Create(db_name_).MoveValue();
  }

  void TearDown() override {
    db_.reset();
    std::filesystem::remove(db_name_ + ".db");
    std::filesystem::remove(db_name_ + ".log");
    std::filesystem::remove(db_name_ + ".last_checkpoint");
  }

  std::vector<Row> RunSql(std::string_view sql) {
    TransactionContext ctx = db_->BeginContext();
    SqlEngine engine(*db_);
    auto prepared = engine.Prepare(ctx, sql);
    std::vector<Row> rows;
    if (prepared.HasValue()) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        rows.push_back(row);
      }
    } else {
      ADD_FAILURE() << sql << " -> " << engine.LastError();
    }
    ctx.txn_.Abort();
    return rows;
  }

  Value Scalar(std::string_view sql) {
    auto rows = RunSql(sql);
    EXPECT_EQ(rows.size(), 1U) << sql;
    if (rows.empty()) {
      return {};
    }
    EXPECT_EQ(rows[0].values_.size(), 1U) << sql;
    return rows[0].values_[0];
  }

  bool SqlFails(std::string_view sql) {
    TransactionContext ctx = db_->BeginContext();
    SqlEngine engine(*db_);
    auto prepared = engine.Prepare(ctx, sql);
    if (!prepared.HasValue()) {
      ctx.txn_.Abort();
      return true;
    }
    Row row;
    bool has_row = prepared.Value()->Next(&row, nullptr);
    ctx.txn_.Abort();
    return !has_row;
  }

  std::string db_name_;
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
  // implementation clamped every start <= 1 to the first byte). A start
  // before the first byte behaves like start 0/1: from the first byte.
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', -2)"), Value("de"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', -2, 2)"), Value("de"));
  EXPECT_EQ(Scalar("SELECT SUBSTR('abcde', -10)"), Value("abcde"));
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
  EXPECT_TRUE(Scalar("SELECT NULLIF(1, 1.0)").IsNull());
  EXPECT_TRUE(Scalar("SELECT NULLIF(1.0, 1)").IsNull());
  Value nan_nullif =
      Scalar("SELECT NULLIF(CAST('nan' AS DOUBLE), CAST('nan' AS DOUBLE))");
  EXPECT_EQ(nan_nullif.type, ValueType::kDouble);
  EXPECT_TRUE(std::isnan(nan_nullif.value.double_value));

  EXPECT_EQ(Scalar("SELECT GREATEST(1, 2, 3)"), Value(int64_t{3}));
  EXPECT_EQ(Scalar("SELECT LEAST(1, 2, 3)"), Value(int64_t{1}));
  EXPECT_EQ(Scalar("SELECT GREATEST(1, 2.0, 3)"), Value(3.0));
  EXPECT_EQ(Scalar("SELECT LEAST(1.0, 2, 3)"), Value(1.0));

  Value g1 = Scalar("SELECT GREATEST(1.0, CAST('nan' AS DOUBLE))");
  Value g2 = Scalar("SELECT GREATEST(CAST('nan' AS DOUBLE), 1.0)");
  EXPECT_EQ(g1.type, ValueType::kDouble);
  EXPECT_EQ(g2.type, ValueType::kDouble);
  EXPECT_TRUE(std::isnan(g1.value.double_value));
  EXPECT_TRUE(std::isnan(g2.value.double_value));

  Value l1 = Scalar("SELECT LEAST(1.0, CAST('nan' AS DOUBLE))");
  Value l2 = Scalar("SELECT LEAST(CAST('nan' AS DOUBLE), 1.0)");
  EXPECT_EQ(l1.type, ValueType::kDouble);
  EXPECT_EQ(l2.type, ValueType::kDouble);
  EXPECT_TRUE(std::isnan(l1.value.double_value));
  EXPECT_TRUE(std::isnan(l2.value.double_value));

  EXPECT_EQ(Scalar("SELECT IF(true, 1, 2)"), Value(int64_t{1}));
  EXPECT_EQ(Scalar("SELECT IF(false, 1, 2)"), Value(int64_t{2}));
}

TEST_F(ScalarFunctionSqlTest, RegexpFunctions) {
  EXPECT_EQ(Scalar("SELECT REGEXP_CONTAINS('hello', 'ell')"), Value(true));
  EXPECT_EQ(Scalar("SELECT REGEXP_CONTAINS('hello', 'xyz')"), Value(false));
  EXPECT_EQ(Scalar("SELECT REGEXP_REPLACE('hello', 'l', 'L')"), Value("heLLo"));
}

TEST(FunctionCallAstTest, MathFunctionsAstParity) {
  Row dummy_row;
  Schema dummy_schema;

  auto eval = [&](const std::string& name, std::vector<Value> args) {
    std::vector<Expression> exprs;
    for (auto& a : args) {
      exprs.push_back(ConstantValueExp(std::move(a)));
    }
    return FunctionCallExp(name, std::move(exprs))->TryEvaluate(dummy_row, dummy_schema);
  };

  EXPECT_EQ(eval("abs", {Value(int64_t{-5})}).Value(), Value(int64_t{5}));
  EXPECT_EQ(eval("sign", {Value(int64_t{-3})}).Value(), Value(int64_t{-1}));
  EXPECT_EQ(eval("sign", {Value(int64_t{0})}).Value(), Value(int64_t{0}));
  EXPECT_EQ(eval("sign", {Value(int64_t{3})}).Value(), Value(int64_t{1}));
  EXPECT_DOUBLE_EQ(eval("sqrt", {Value(16.0)}).Value().value.double_value, 4.0);
  EXPECT_DOUBLE_EQ(eval("cbrt", {Value(27.0)}).Value().value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(eval("floor", {Value(2.7)}).Value().value.double_value, 2.0);
  EXPECT_DOUBLE_EQ(eval("ceil", {Value(2.1)}).Value().value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(eval("round", {Value(2.5)}).Value().value.double_value, 3.0);
  EXPECT_DOUBLE_EQ(eval("trunc", {Value(2.7)}).Value().value.double_value, 2.0);
  EXPECT_DOUBLE_EQ(eval("pow", {Value(2.0), Value(3.0)}).Value().value.double_value, 8.0);
  EXPECT_DOUBLE_EQ(eval("ln", {Value(1.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("exp", {Value(0.0)}).Value().value.double_value, 1.0);
  EXPECT_DOUBLE_EQ(eval("sin", {Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("cos", {Value(0.0)}).Value().value.double_value, 1.0);
  EXPECT_DOUBLE_EQ(eval("tan", {Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("asin", {Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("acos", {Value(1.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("atan", {Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("atan2", {Value(0.0), Value(1.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("cosh", {Value(0.0)}).Value().value.double_value, 1.0);
  EXPECT_DOUBLE_EQ(eval("sinh", {Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("tanh", {Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_DOUBLE_EQ(eval("radians", {Value(180.0)}).Value().value.double_value, M_PI);
  EXPECT_DOUBLE_EQ(eval("degrees", {Value(M_PI)}).Value().value.double_value, 180.0);
  EXPECT_DOUBLE_EQ(eval("pi", {}).Value().value.double_value, M_PI);
  EXPECT_EQ(eval("mod", {Value(int64_t{10}), Value(int64_t{3})}).Value(), Value(int64_t{1}));
  EXPECT_EQ(eval("div", {Value(int64_t{10}), Value(int64_t{3})}).Value(), Value(int64_t{3}));
  EXPECT_EQ(eval("ieee_divide", {Value(1.0), Value(2.0)}).Value(), Value(0.5));
  EXPECT_DOUBLE_EQ(eval("safe_divide", {Value(8.0), Value(2.0)}).Value().value.double_value, 4.0);
}

TEST(FunctionCallAstTest, TypeSafetyAndDomainErrors) {
  Row dummy_row;
  Schema dummy_schema;

  auto eval = [&](const std::string& name, std::vector<Value> args) {
    std::vector<Expression> exprs;
    for (auto& a : args) {
      exprs.push_back(ConstantValueExp(std::move(a)));
    }
    return FunctionCallExp(name, std::move(exprs))->TryEvaluate(dummy_row, dummy_schema);
  };

  // Rejection of non-numeric arguments (prevent union corruption)
  EXPECT_EQ(eval("sqrt", {Value("not_a_number")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("cbrt", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("ln", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("log", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("cos", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("sin", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("tan", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("acos", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("asin", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("atan", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("atan2", {Value("text"), Value(1.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("atan2", {Value(1.0), Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("pow", {Value("text"), Value(2.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("pow", {Value(2.0), Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("ceil", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("floor", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("round", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("trunc", {Value("text")}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("div", {Value("text"), Value(2)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("ieee_divide", {Value("text"), Value(2.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("safe_divide", {Value("text"), Value(2.0)}).GetStatus(), StatusCode::kInvalidArgument);

  // Domain error validation
  EXPECT_EQ(eval("sqrt", {Value(-1.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(std::isinf(eval("ln", {Value(0.0)}).Value().value.double_value));
  EXPECT_TRUE(std::isnan(eval("ln", {Value(-5.0)}).Value().value.double_value));
  EXPECT_TRUE(std::isinf(eval("log", {Value(0.0)}).Value().value.double_value));
  EXPECT_DOUBLE_EQ(eval("log", {Value(10.0), Value(0.0)}).Value().value.double_value, 0.0);
  EXPECT_TRUE(std::isinf(eval("log", {Value(10.0), Value(1.0)}).Value().value.double_value) ||
              std::isnan(eval("log", {Value(10.0), Value(1.0)}).Value().value.double_value));
  EXPECT_EQ(eval("acos", {Value(2.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("acos", {Value(-2.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("asin", {Value(2.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("asin", {Value(-2.0)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("div", {Value(int64_t{10}), Value(int64_t{0})}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("div", {Value(std::numeric_limits<int64_t>::min()), Value(int64_t{-1})}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("div", {Value(std::numeric_limits<double>::quiet_NaN()), Value(2.0)}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("div", {Value(1.0), Value(std::numeric_limits<double>::quiet_NaN())}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("div", {Value(std::numeric_limits<double>::infinity()), Value(2.0)}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("pow", {Value(-1.0), Value(0.5)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("pow", {Value(0.0), Value(-1.0)}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("pow", {Value(2.0), Value(1024.0)}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("power", {Value(-1.0), Value(0.5)}).GetStatus(), StatusCode::kInvalidArgument);
  EXPECT_EQ(eval("exp", {Value(1000.0)}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("cosh", {Value(1000.0)}).GetStatus(), StatusCode::kIsInfinity);
  EXPECT_EQ(eval("sinh", {Value(1000.0)}).GetStatus(), StatusCode::kIsInfinity);
}

TEST(FunctionCallAstTest, FoldMathFunctions) {
  Expression pow_expr = FunctionCallExp("pow", {ConstantValueExp(Value(2.0)), ConstantValueExp(Value(3.0))});
  Expression rewritten_pow = ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(pow_expr);
  ASSERT_EQ(rewritten_pow->Type(), TypeTag::kConstantValue);
  EXPECT_DOUBLE_EQ(rewritten_pow->AsConstantValue().GetValue().value.double_value, 8.0);

  Expression sqrt_expr = FunctionCallExp("sqrt", {ConstantValueExp(Value(16.0))});
  Expression rewritten_sqrt = ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(sqrt_expr);
  ASSERT_EQ(rewritten_sqrt->Type(), TypeTag::kConstantValue);
  EXPECT_DOUBLE_EQ(rewritten_sqrt->AsConstantValue().GetValue().value.double_value, 4.0);

  Expression ceil_expr = FunctionCallExp("ceil", {ConstantValueExp(Value(2.1))});
  Expression rewritten_ceil = ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(ceil_expr);
  ASSERT_EQ(rewritten_ceil->Type(), TypeTag::kConstantValue);
  EXPECT_DOUBLE_EQ(rewritten_ceil->AsConstantValue().GetValue().value.double_value, 3.0);
}

TEST(FunctionCallAstTest, ResultTypeMixedNumeric) {
  Schema dummy_schema;
  Expression safe_add_int = FunctionCallExp("safe_add", {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  EXPECT_EQ(safe_add_int->ResultType(dummy_schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(safe_add_int->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kBigInt);

  Expression safe_add_mixed = FunctionCallExp("safe_add", {ConstantValueExp(Value(1)), ConstantValueExp(Value(2.5))});
  EXPECT_EQ(safe_add_mixed->ResultType(dummy_schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(safe_add_mixed->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kDouble);

  Expression safe_sub_mixed = FunctionCallExp("safe_subtract", {ConstantValueExp(Value(1)), ConstantValueExp(Value(2.5))});
  EXPECT_EQ(safe_sub_mixed->ResultType(dummy_schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(safe_sub_mixed->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kDouble);

  Expression safe_mul_mixed = FunctionCallExp("safe_multiply", {ConstantValueExp(Value(1)), ConstantValueExp(Value(2.5))});
  EXPECT_EQ(safe_mul_mixed->ResultType(dummy_schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(safe_mul_mixed->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kDouble);

  Expression mod_int = FunctionCallExp("mod", {ConstantValueExp(Value(5)), ConstantValueExp(Value(2))});
  EXPECT_EQ(mod_int->ResultType(dummy_schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(mod_int->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kBigInt);

  Expression mod_mixed1 = FunctionCallExp("mod", {ConstantValueExp(Value(5)), ConstantValueExp(Value(2.5))});
  EXPECT_EQ(mod_mixed1->ResultType(dummy_schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(mod_mixed1->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kDouble);

  Expression mod_mixed2 = FunctionCallExp("mod", {ConstantValueExp(Value(5.5)), ConstantValueExp(Value(2))});
  EXPECT_EQ(mod_mixed2->ResultType(dummy_schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(mod_mixed2->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kDouble);
}

TEST(FunctionCallAstTest, StringScalarParity) {
  Row dummy_row;
  Schema dummy_schema;

  // ascii / unicode
  auto ascii_val = FunctionCallExp("ascii", {ConstantValueExp(Value("ABC"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(ascii_val, Value(int64_t{65}));

  auto unicode_val = FunctionCallExp("unicode", {ConstantValueExp(Value("€"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(unicode_val, Value(int64_t{8364}));

  // chr
  auto chr_val = FunctionCallExp("chr", {ConstantValueExp(Value(int64_t{65}))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(chr_val, Value("A"));

  // reverse / byte_reverse
  auto rev_val = FunctionCallExp("reverse", {ConstantValueExp(Value("hello"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(rev_val, Value("olleh"));

  // repeat
  auto rep_val = FunctionCallExp("repeat", {ConstantValueExp(Value("ab")), ConstantValueExp(Value(int64_t{3}))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(rep_val, Value("ababab"));

  // initcap
  auto init_val = FunctionCallExp("initcap", {ConstantValueExp(Value("hello world"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(init_val, Value("Hello World"));

  // translate
  auto trans_val = FunctionCallExp("translate", {ConstantValueExp(Value("12345")), ConstantValueExp(Value("135")), ConstantValueExp(Value("abc"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(trans_val, Value("a2b4c"));

  // lpad / rpad
  auto lpad_val = FunctionCallExp("lpad", {ConstantValueExp(Value("hi")), ConstantValueExp(Value(int64_t{5})), ConstantValueExp(Value("*"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(lpad_val, Value("***hi"));

  auto rpad_val = FunctionCallExp("rpad", {ConstantValueExp(Value("hi")), ConstantValueExp(Value(int64_t{5})), ConstantValueExp(Value("*"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(rpad_val, Value("hi***"));

  // left / right
  auto left_val = FunctionCallExp("left", {ConstantValueExp(Value("hello")), ConstantValueExp(Value(int64_t{2}))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(left_val, Value("he"));

  auto right_val = FunctionCallExp("right", {ConstantValueExp(Value("hello")), ConstantValueExp(Value(int64_t{2}))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(right_val, Value("lo"));

  // regexp_contains / regexp_replace
  auto re_val = FunctionCallExp("regexp_contains", {ConstantValueExp(Value("hello world")), ConstantValueExp(Value("world"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(re_val, Value(true));

  auto rep_re_val = FunctionCallExp("regexp_replace", {ConstantValueExp(Value("hello 123")), ConstantValueExp(Value("[0-9]+")), ConstantValueExp(Value("world"))})->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(rep_re_val, Value("hello world"));
}

TEST(FunctionCallTest, UnsignedInt64_MathematicalFunctions) {
  Row dummy_row;
  Schema dummy_schema;
  const Value umax =
      Value(static_cast<int64_t>(std::numeric_limits<uint64_t>::max()))
          .WithUnsigned();
  const Value u0 = Value(int64_t{0}).WithUnsigned();
  const Value u10 = Value(int64_t{10}).WithUnsigned();

  // ABS
  auto abs_res = FunctionCallExp("abs", {ConstantValueExp(umax)})
                     ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(abs_res.IsUnsigned());
  EXPECT_EQ(abs_res, umax);

  // SIGN
  auto sign_umax = FunctionCallExp("sign", {ConstantValueExp(umax)})
                       ->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(sign_umax, Value(int64_t{1}));

  auto sign_u0 = FunctionCallExp("sign", {ConstantValueExp(u0)})
                     ->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(sign_u0, Value(int64_t{0}));

  // MOD
  auto mod_res =
      FunctionCallExp("mod", {ConstantValueExp(umax), ConstantValueExp(u10)})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(mod_res.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(mod_res.value.int_value), 5ULL);

  // DIV
  auto div_res =
      FunctionCallExp("div", {ConstantValueExp(umax),
                              ConstantValueExp(Value(int64_t{2}).WithUnsigned())})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(div_res.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(div_res.value.int_value),
            9223372036854775807ULL);

  // GREATEST / LEAST
  auto greatest_res =
      FunctionCallExp("greatest", {ConstantValueExp(Value(int64_t{100})),
                                   ConstantValueExp(umax)})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(greatest_res.IsUnsigned());
  EXPECT_EQ(greatest_res, umax);

  auto least_res =
      FunctionCallExp("least", {ConstantValueExp(Value(int64_t{100})),
                                ConstantValueExp(umax)})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(least_res, Value(int64_t{100}));

  // SQRT on large unsigned value (converted to positive double, not negative)
  auto sqrt_res = FunctionCallExp("sqrt", {ConstantValueExp(umax)})
                      ->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(sqrt_res.type, ValueType::kDouble);
  EXPECT_GT(sqrt_res.value.double_value, 4000000000.0);

  // SAFE_ADD / SAFE_SUBTRACT / SAFE_MULTIPLY / SAFE_NEGATE
  auto safe_add_overflow =
      FunctionCallExp("safe_add",
                      {ConstantValueExp(umax),
                       ConstantValueExp(Value(int64_t{1}).WithUnsigned())})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(safe_add_overflow.IsNull());

  auto safe_add_ok =
      FunctionCallExp("safe_add", {ConstantValueExp(umax - u10),
                                   ConstantValueExp(u10)})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(safe_add_ok.IsUnsigned());
  EXPECT_EQ(safe_add_ok, umax);

  auto safe_sub_overflow =
      FunctionCallExp("safe_subtract",
                      {ConstantValueExp(u0),
                       ConstantValueExp(Value(int64_t{1}).WithUnsigned())})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(safe_sub_overflow.IsNull());

  auto safe_mul_overflow =
      FunctionCallExp("safe_multiply",
                      {ConstantValueExp(umax),
                       ConstantValueExp(Value(int64_t{2}).WithUnsigned())})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(safe_mul_overflow.IsNull());

  auto safe_neg_umax = FunctionCallExp("safe_negate", {ConstantValueExp(umax)})
                           ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(safe_neg_umax.IsNull());

  auto safe_neg_u0 = FunctionCallExp("safe_negate", {ConstantValueExp(u0)})
                         ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(safe_neg_u0.IsUnsigned());
  EXPECT_EQ(safe_neg_u0, u0);

  // FORMAT
  auto fmt_res =
      FunctionCallExp("format",
                      {ConstantValueExp(Value("%u")), ConstantValueExp(umax)})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(fmt_res, Value("18446744073709551615"));

  auto fmt_t_res =
      FunctionCallExp("format",
                      {ConstantValueExp(Value("%T")), ConstantValueExp(umax)})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_EQ(fmt_t_res, Value("18446744073709551615"));

  // ROUND / TRUNC
  auto round_res =
      FunctionCallExp("round", {ConstantValueExp(umax),
                                ConstantValueExp(Value(int64_t{0}))})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(round_res.IsUnsigned());
  EXPECT_EQ(round_res, umax);

  auto trunc_res =
      FunctionCallExp("trunc", {ConstantValueExp(umax),
                                ConstantValueExp(Value(int64_t{0}))})
          ->Evaluate(dummy_row, dummy_schema);
  EXPECT_TRUE(trunc_res.IsUnsigned());
  EXPECT_EQ(trunc_res, umax);
}

TEST(FunctionCallTest, GenerateArray_AstEvaluation) {
  Row dummy_row;
  Schema dummy_schema;

  auto res1 = FunctionCallExp("generate_array",
                              {ConstantValueExp(Value(int64_t{1})),
                               ConstantValueExp(Value(int64_t{5}))})
                  ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res1.HasValue());
  EXPECT_TRUE(res1.Value().IsArray());
  EXPECT_EQ(res1.Value().ArrayElementSqlType(), "INT64");
  EXPECT_EQ(res1.Value().ArrayElements().size(), 5U);
  EXPECT_EQ(res1.Value().ArrayElements()[0], Value(int64_t{1}));
  EXPECT_EQ(res1.Value().ArrayElements()[4], Value(int64_t{5}));

  auto res2 = FunctionCallExp("generate_array",
                              {ConstantValueExp(Value(1.5)),
                               ConstantValueExp(Value(3.5)),
                               ConstantValueExp(Value(1.0))})
                  ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res2.HasValue());
  EXPECT_TRUE(res2.Value().IsArray());
  EXPECT_EQ(res2.Value().ArrayElementSqlType(), "DOUBLE");
  EXPECT_EQ(res2.Value().ArrayElements().size(), 3U);
  EXPECT_DOUBLE_EQ(res2.Value().ArrayElements()[0].value.double_value, 1.5);
  EXPECT_DOUBLE_EQ(res2.Value().ArrayElements()[2].value.double_value, 3.5);

  auto res3 = FunctionCallExp("generate_array",
                              {ConstantValueExp(Value(int64_t{5})),
                               ConstantValueExp(Value(int64_t{1})),
                               ConstantValueExp(Value(int64_t{-1}))})
                  ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res3.HasValue());
  EXPECT_EQ(res3.Value().ArrayElements().size(), 5U);
  EXPECT_EQ(res3.Value().ArrayElements()[0], Value(int64_t{5}));
  EXPECT_EQ(res3.Value().ArrayElements()[4], Value(int64_t{1}));

  auto res4 = FunctionCallExp("generate_array",
                              {ConstantValueExp(Value(int64_t{5})),
                               ConstantValueExp(Value(int64_t{1})),
                               ConstantValueExp(Value(int64_t{1}))})
                  ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res4.HasValue());
  EXPECT_TRUE(res4.Value().IsArray());
  EXPECT_EQ(res4.Value().ArrayElements().size(), 0U);
  EXPECT_EQ(res4.Value().ArrayElementSqlType(), "INT64");

  auto res_zero = FunctionCallExp("generate_array",
                                  {ConstantValueExp(Value(int64_t{1})),
                                   ConstantValueExp(Value(int64_t{5})),
                                   ConstantValueExp(Value(int64_t{0}))})
                      ->TryEvaluate(dummy_row, dummy_schema);
  EXPECT_FALSE(res_zero.HasValue());
  EXPECT_EQ(res_zero.GetStatus(), Status::kIsInfinity);

  auto res_non_num = FunctionCallExp("generate_array",
                                     {ConstantValueExp(Value("a")),
                                      ConstantValueExp(Value("b"))})
                         ->TryEvaluate(dummy_row, dummy_schema);
  EXPECT_FALSE(res_non_num.HasValue());
  EXPECT_EQ(res_non_num.GetStatus(), Status::kInvalidArgument);

  auto res_null = FunctionCallExp("generate_array",
                                  {ConstantValueExp(Value()),
                                   ConstantValueExp(Value(int64_t{5}))})
                      ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res_null.HasValue());
  EXPECT_TRUE(res_null.Value().IsNull());

  auto res_uint =
      FunctionCallExp("generate_array",
                      {ConstantValueExp(Value(int64_t{1}).WithUnsigned()),
                       ConstantValueExp(Value(int64_t{3}).WithUnsigned())})
          ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res_uint.HasValue());
  EXPECT_TRUE(res_uint.Value().IsArray());
  EXPECT_EQ(res_uint.Value().ArrayElementSqlType(), "UINT64");
  EXPECT_EQ(res_uint.Value().ArrayElements().size(), 3U);
  EXPECT_TRUE(res_uint.Value().ArrayElements()[0].IsUnsigned());
}

TEST(FunctionCallTest, GenerateDateArray_SafetyAndLimits) {
  Row dummy_row;
  Schema dummy_schema;

  auto res1 = FunctionCallExp("generate_date_array",
                              {ConstantValueExp(Value("2024-01-01")),
                               ConstantValueExp(Value("2024-01-03"))})
                  ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res1.HasValue());
  EXPECT_TRUE(res1.Value().IsArray());
  EXPECT_EQ(res1.Value().ArrayElementSqlType(), "DATE");
  EXPECT_EQ(res1.Value().ArrayElements().size(), 3U);

  auto res_inv = FunctionCallExp("generate_date_array",
                                 {ConstantValueExp(Value("not-a-date")),
                                  ConstantValueExp(Value("2024-01-03"))})
                     ->TryEvaluate(dummy_row, dummy_schema);
  EXPECT_FALSE(res_inv.HasValue());
  EXPECT_EQ(res_inv.GetStatus(), Status::kInvalidArgument);

  auto res_zero = FunctionCallExp("generate_date_array",
                                  {ConstantValueExp(Value("2024-01-01")),
                                   ConstantValueExp(Value("2024-01-03")),
                                   ConstantValueExp(Value(int64_t{0}))})
                      ->TryEvaluate(dummy_row, dummy_schema);
  EXPECT_FALSE(res_zero.HasValue());
  EXPECT_EQ(res_zero.GetStatus(), Status::kIsInfinity);

  auto res_empty = FunctionCallExp("generate_date_array",
                                   {ConstantValueExp(Value("2024-01-03")),
                                    ConstantValueExp(Value("2024-01-01")),
                                    ConstantValueExp(Value(int64_t{1}))})
                       ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(res_empty.HasValue());
  EXPECT_TRUE(res_empty.Value().IsArray());
  EXPECT_EQ(res_empty.Value().ArrayElements().size(), 0U);
}

TEST_F(ScalarFunctionSqlTest, GenerateArrayAndDateArraySql) {
  Value arr = Scalar("SELECT GENERATE_ARRAY(1, 3)");
  EXPECT_TRUE(arr.IsArray());
  EXPECT_EQ(arr.ArrayElementSqlType(), "INT64");
  EXPECT_EQ(arr.ArrayElements().size(), 3U);
  EXPECT_EQ(arr.ArrayElements()[0], Value(int64_t{1}));
  EXPECT_EQ(arr.ArrayElements()[2], Value(int64_t{3}));

  Value darr = Scalar("SELECT GENERATE_DATE_ARRAY('2024-01-01', '2024-01-03')");
  EXPECT_TRUE(darr.IsArray());
  EXPECT_EQ(darr.ArrayElementSqlType(), "DATE");
  EXPECT_EQ(darr.ArrayElements().size(), 3U);
}

TEST(FunctionCallTest, UnsignedMathAndScalarFunctions) {
  Row dummy_row;
  Schema dummy_schema;

  const uint64_t u_max = std::numeric_limits<uint64_t>::max();
  const Value u_max_val = Value(static_cast<int64_t>(u_max)).WithUnsigned();

  // ABS
  auto abs_res = FunctionCallExp("abs", {ConstantValueExp(u_max_val)})
                     ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(abs_res.HasValue());
  EXPECT_TRUE(abs_res.Value().IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(abs_res.Value().value.int_value), u_max);

  // SIGN
  auto sign_res = FunctionCallExp("sign", {ConstantValueExp(u_max_val)})
                      ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(sign_res.HasValue());
  EXPECT_EQ(sign_res.Value(), Value(int64_t{1}));

  // MOD
  auto mod_res = FunctionCallExp("mod", {ConstantValueExp(u_max_val),
                                         ConstantValueExp(Value(int64_t{10}))})
                     ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(mod_res.HasValue());
  EXPECT_TRUE(mod_res.Value().IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(mod_res.Value().value.int_value), u_max % 10);

  // GREATEST / LEAST
  auto gr_res = FunctionCallExp("greatest", {ConstantValueExp(u_max_val),
                                             ConstantValueExp(Value(int64_t{0}))})
                    ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(gr_res.HasValue());
  EXPECT_TRUE(gr_res.Value().IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(gr_res.Value().value.int_value), u_max);

  auto lt_res = FunctionCallExp("least", {ConstantValueExp(u_max_val),
                                          ConstantValueExp(Value(int64_t{10}))})
                    ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(lt_res.HasValue());
  EXPECT_EQ(lt_res.Value(), Value(int64_t{10}));

  // Precision preservation for 64-bit integers > 2^53
  const int64_t large_a = 9007199254740993LL;
  const int64_t large_b = 9007199254740992LL;
  auto gr_large =
      FunctionCallExp("greatest", {ConstantValueExp(Value(large_a)),
                                   ConstantValueExp(Value(large_b))})
          ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(gr_large.HasValue());
  EXPECT_EQ(gr_large.Value().value.int_value, large_a);

  // SAFE_*
  auto safe_add_overflow =
      FunctionCallExp("safe_add", {ConstantValueExp(u_max_val),
                                   ConstantValueExp(Value(int64_t{1}))})
          ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(safe_add_overflow.HasValue());
  EXPECT_TRUE(safe_add_overflow.Value().IsNull());

  auto safe_neg = FunctionCallExp("safe_negate", {ConstantValueExp(u_max_val)})
                      ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(safe_neg.HasValue());
  EXPECT_TRUE(safe_neg.Value().IsNull());

  auto safe_neg_zero =
      FunctionCallExp("safe_negate",
                      {ConstantValueExp(Value(int64_t{0}).WithUnsigned())})
          ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(safe_neg_zero.HasValue());
  EXPECT_TRUE(safe_neg_zero.Value().IsUnsigned());
  EXPECT_EQ(safe_neg_zero.Value().value.int_value, 0);

  // SQRT / LN
  auto sqrt_res = FunctionCallExp("sqrt", {ConstantValueExp(u_max_val)})
                      ->TryEvaluate(dummy_row, dummy_schema);
  ASSERT_TRUE(sqrt_res.HasValue());
  EXPECT_GT(sqrt_res.Value().value.double_value, 0.0);
}

TEST_F(ScalarFunctionSqlTest, UnsignedMathAndScalarFunctionsSql) {
  Value abs_v = Scalar("SELECT ABS(CAST(9223372036854775808 AS UINT64))");
  EXPECT_TRUE(abs_v.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(abs_v.value.int_value), 9223372036854775808ULL);

  Value sign_v = Scalar("SELECT SIGN(CAST(18446744073709551615 AS UINT64))");
  EXPECT_EQ(sign_v, Value(int64_t{1}));

  Value mod_v = Scalar("SELECT MOD(CAST(18446744073709551615 AS UINT64), 10)");
  EXPECT_TRUE(mod_v.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(mod_v.value.int_value), 5ULL);

  Value gr_v = Scalar("SELECT GREATEST(CAST(18446744073709551615 AS UINT64), 0)");
  EXPECT_TRUE(gr_v.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(gr_v.value.int_value), 18446744073709551615ULL);

  Value lt_v = Scalar("SELECT LEAST(CAST(18446744073709551615 AS UINT64), 10)");
  EXPECT_EQ(lt_v, Value(int64_t{10}));

  Value gr_prec = Scalar("SELECT GREATEST(9007199254740993, 9007199254740992)");
  EXPECT_EQ(gr_prec.value.int_value, 9007199254740993LL);

  Value safe_add_null =
      Scalar("SELECT SAFE_ADD(CAST(18446744073709551615 AS UINT64), 1)");
  EXPECT_TRUE(safe_add_null.IsNull());

  Value safe_add_ok =
      Scalar("SELECT SAFE_ADD(CAST(18446744073709551614 AS UINT64), 1)");
  EXPECT_TRUE(safe_add_ok.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(safe_add_ok.value.int_value),
            18446744073709551615ULL);

  Value safe_sub_null =
      Scalar("SELECT SAFE_SUBTRACT(CAST(0 AS UINT64), 1)");
  EXPECT_TRUE(safe_sub_null.IsNull());

  Value safe_neg_null =
      Scalar("SELECT SAFE_NEGATE(CAST(5 AS UINT64))");
  EXPECT_TRUE(safe_neg_null.IsNull());

  Value safe_neg_zero =
      Scalar("SELECT SAFE_NEGATE(CAST(0 AS UINT64))");
  EXPECT_TRUE(safe_neg_zero.IsUnsigned());
  EXPECT_EQ(safe_neg_zero.value.int_value, 0);

  Value sqrt_v = Scalar("SELECT SQRT(CAST(18446744073709551615 AS UINT64))");
  EXPECT_GT(sqrt_v.value.double_value, 4000000000.0);
}

TEST_F(ScalarFunctionSqlTest, BitwiseAndShiftOperatorsConformance) {
  EXPECT_EQ(Scalar("SELECT ~0").value.int_value, -1);
  EXPECT_EQ(Scalar("SELECT ~1").value.int_value, -2);
  EXPECT_EQ(Scalar("SELECT ~(-1)").value.int_value, 0);
  EXPECT_TRUE(Scalar("SELECT ~CAST(NULL AS INT64)").IsNull());

  Value u_not = Scalar("SELECT ~CAST(0 AS UINT64)");
  EXPECT_TRUE(u_not.IsUnsigned());
  EXPECT_EQ(static_cast<uint64_t>(u_not.value.int_value),
            std::numeric_limits<uint64_t>::max());

  EXPECT_EQ(Scalar("SELECT 1 << 64").value.int_value, 0);
  EXPECT_EQ(Scalar("SELECT 1 >> 64").value.int_value, 0);
  EXPECT_EQ(Scalar("SELECT 100 << 100").value.int_value, 0);
  EXPECT_EQ(Scalar("SELECT 100 >> 100").value.int_value, 0);

  Value u_shl = Scalar("SELECT CAST(1 AS UINT64) << 64");
  EXPECT_TRUE(u_shl.IsUnsigned());
  EXPECT_EQ(u_shl.value.int_value, 0);

  Value u_shr = Scalar("SELECT CAST(1 AS UINT64) >> 64");
  EXPECT_TRUE(u_shr.IsUnsigned());
  EXPECT_EQ(u_shr.value.int_value, 0);

  EXPECT_TRUE(SqlFails("SELECT 1 << -1"));
  EXPECT_TRUE(SqlFails("SELECT 1 >> -1"));

  Value u_and = Scalar("SELECT CAST(1 AS UINT64) & 3");
  EXPECT_TRUE(u_and.IsUnsigned());
  EXPECT_EQ(u_and.value.int_value, 1);

  Value u_or = Scalar("SELECT CAST(1 AS UINT64) | 2");
  EXPECT_TRUE(u_or.IsUnsigned());
  EXPECT_EQ(u_or.value.int_value, 3);

  Value u_xor = Scalar("SELECT CAST(1 AS UINT64) ^ 3");
  EXPECT_TRUE(u_xor.IsUnsigned());
  EXPECT_EQ(u_xor.value.int_value, 2);
}

}  // namespace
}  // namespace tinylamb

