//
// Created by kumagi on 2022/02/22.
//

#include "expression.hpp"

#include "common/log_message.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

TEST(ExpressionTest, Constant) {
  Expression cv_int = Expression::ConstantValue(Value(1));
  Expression cv_varchar = Expression::ConstantValue(Value("hello"));
  Expression cv_double = Expression::ConstantValue(Value(1.1));
  LOG(INFO) << "cv_int: " << cv_int << "\ncv_varchar: " << cv_varchar
            << "\ncv_double: " << cv_double;
}

TEST(ExpressionTest, ConstantEval) {
  Expression cv_int = Expression::ConstantValue(Value(1));
  Expression cv_varchar = Expression::ConstantValue(Value("hello"));
  Expression cv_double = Expression::ConstantValue(Value(1.1));
  Row dummy({});
  ASSERT_EQ(cv_int.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(cv_varchar.Evaluate(dummy, nullptr), Value("hello"));
  ASSERT_EQ(cv_double.Evaluate(dummy, nullptr), Value(1.1));
}

TEST(ExpressionTest, BinaryPlus) {
  Expression int_plus = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1)), BinaryOperation::kAdd,
      Expression::ConstantValue(Value(2)));
  Expression varchar_plus = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kAdd,
      Expression::ConstantValue(Value(" world")));
  Expression double_plus = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1.1)), BinaryOperation::kAdd,
      Expression::ConstantValue(Value(2.2)));
  Row dummy({});
  ASSERT_EQ(int_plus.Evaluate(dummy, nullptr), Value(3));
  ASSERT_EQ(varchar_plus.Evaluate(dummy, nullptr), Value("hello world"));
  ASSERT_DOUBLE_EQ(double_plus.Evaluate(dummy, nullptr).value.double_value,
                   Value(3.3).value.double_value);
}

TEST(ExpressionTest, BinaryMinus) {
  Expression int_minus = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1)), BinaryOperation::kSubtract,
      Expression::ConstantValue(Value(2)));
  Expression double_minus = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1.1)), BinaryOperation::kSubtract,
      Expression::ConstantValue(Value(2.2)));
  Row dummy({});
  ASSERT_EQ(int_minus.Evaluate(dummy, nullptr), Value(-1));
  ASSERT_DOUBLE_EQ(double_minus.Evaluate(dummy, nullptr).value.double_value,
                   Value(-1.1).value.double_value);
}

TEST(ExpressionTest, BinaryMultiple) {
  Expression int_multiple = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1)), BinaryOperation::kMultiply,
      Expression::ConstantValue(Value(2)));
  Expression double_multiple = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1.1)), BinaryOperation::kMultiply,
      Expression::ConstantValue(Value(2.2)));
  Row dummy({});
  ASSERT_EQ(int_multiple.Evaluate(dummy, nullptr), Value(2));
  ASSERT_DOUBLE_EQ(double_multiple.Evaluate(dummy, nullptr).value.double_value,
                   Value(2.42).value.double_value);
}

TEST(ExpressionTest, BinaryDiv) {
  Expression int_div = Expression::BinaryExpression(
      Expression::ConstantValue(Value(10)), BinaryOperation::kDivide,
      Expression::ConstantValue(Value(2)));
  Expression double_div = Expression::BinaryExpression(
      Expression::ConstantValue(Value(8.8)), BinaryOperation::kDivide,
      Expression::ConstantValue(Value(2.2)));
  Row dummy({});
  ASSERT_EQ(int_div.Evaluate(dummy, nullptr), Value(5));
  ASSERT_DOUBLE_EQ(double_div.Evaluate(dummy, nullptr).value.double_value,
                   Value(4.0).value.double_value);
}

TEST(ExpressionTest, BinaryMod) {
  Expression int_mod = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13)), BinaryOperation::kModulo,
      Expression::ConstantValue(Value(5)));
  Row dummy({});
  ASSERT_EQ(int_mod.Evaluate(dummy, nullptr), Value(3));
}

TEST(ExpressionTest, Equal) {
  Expression int_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kEquals,
      Expression::ConstantValue(Value(120)));
  Expression int_ne = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13)), BinaryOperation::kEquals,
      Expression::ConstantValue(Value(5)));
  Expression double_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120.0)), BinaryOperation::kEquals,
      Expression::ConstantValue(Value(120.0)));
  Expression double_ne = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13.0)), BinaryOperation::kEquals,
      Expression::ConstantValue(Value(5.0)));
  Expression varchar_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kEquals,
      Expression::ConstantValue(Value("hello")));
  Expression varchar_ne = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kEquals,
      Expression::ConstantValue(Value("world")));
  Row dummy({});
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(int_ne.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_ne.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_ne.Evaluate(dummy, nullptr), Value(0));
}

TEST(ExpressionTest, NotEqual) {
  Expression int_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kNotEquals,
      Expression::ConstantValue(Value(120)));
  Expression int_ne = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13)), BinaryOperation::kNotEquals,
      Expression::ConstantValue(Value(5)));
  Expression double_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120.0)), BinaryOperation::kNotEquals,
      Expression::ConstantValue(Value(120.0)));
  Expression double_ne = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13.0)), BinaryOperation::kNotEquals,
      Expression::ConstantValue(Value(5.0)));
  Expression varchar_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kNotEquals,
      Expression::ConstantValue(Value("hello")));
  Expression varchar_ne = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kNotEquals,
      Expression::ConstantValue(Value("world")));
  Row dummy({});
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(int_ne.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_ne.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_ne.Evaluate(dummy, nullptr), Value(1));
}

TEST(ExpressionTest, LessThan) {
  Expression int_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(100)), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value(12312)));
  Expression int_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value(120)));
  Expression int_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value(-1)));
  Expression double_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1.2)), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value(2.2)));
  Expression double_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120.0)), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value(120.0)));
  Expression double_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13.3)), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value(5.0)));
  Expression varchar_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value("aaa")), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value("aaab")));
  Expression varchar_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value("hello")));
  Expression varchar_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value("b")), BinaryOperation::kLessThan,
      Expression::ConstantValue(Value("a")));
  Row dummy({});
  ASSERT_EQ(int_lt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(int_gt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_lt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_gt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_lt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_gt.Evaluate(dummy, nullptr), Value(0));
}

TEST(ExpressionTest, LessThanEquals) {
  Expression int_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(100)), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value(12312)));
  Expression int_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value(120)));
  Expression int_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value(-1)));
  Expression double_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1.2)), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value(2.2)));
  Expression double_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120.0)), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value(120.0)));
  Expression double_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13.3)), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value(5.0)));
  Expression varchar_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value("aaa")), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value("aaab")));
  Expression varchar_eq =
      Expression::BinaryExpression(Expression::ConstantValue(Value("hello")),
                                   BinaryOperation::kLessThanEquals,
                                   Expression::ConstantValue(Value("hello")));
  Expression varchar_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value("b")), BinaryOperation::kLessThanEquals,
      Expression::ConstantValue(Value("a")));
  Row dummy({});
  ASSERT_EQ(int_lt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(int_gt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_lt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_gt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_lt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_gt.Evaluate(dummy, nullptr), Value(0));
}

TEST(ExpressionTest, GreaterThan) {
  Expression int_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(100)), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value(12312)));
  Expression int_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value(120)));
  Expression int_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120)), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value(-1)));
  Expression double_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(1.2)), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value(2.2)));
  Expression double_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value(120.0)), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value(120.0)));
  Expression double_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value(13.3)), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value(5.0)));
  Expression varchar_lt = Expression::BinaryExpression(
      Expression::ConstantValue(Value("aaa")), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value("aaab")));
  Expression varchar_eq = Expression::BinaryExpression(
      Expression::ConstantValue(Value("hello")), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value("hello")));
  Expression varchar_gt = Expression::BinaryExpression(
      Expression::ConstantValue(Value("b")), BinaryOperation::kGreaterThan,
      Expression::ConstantValue(Value("a")));
  Row dummy({});
  ASSERT_EQ(int_lt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(int_gt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_lt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_gt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_lt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_gt.Evaluate(dummy, nullptr), Value(1));
}

TEST(ExpressionTest, GreaterThanEquals) {
  Expression int_lt =
      Expression::BinaryExpression(Expression::ConstantValue(Value(100)),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value(12312)));
  Expression int_eq =
      Expression::BinaryExpression(Expression::ConstantValue(Value(120)),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value(120)));
  Expression int_gt =
      Expression::BinaryExpression(Expression::ConstantValue(Value(120)),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value(-1)));
  Expression double_lt =
      Expression::BinaryExpression(Expression::ConstantValue(Value(1.2)),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value(2.2)));
  Expression double_eq =
      Expression::BinaryExpression(Expression::ConstantValue(Value(120.0)),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value(120.0)));
  Expression double_gt =
      Expression::BinaryExpression(Expression::ConstantValue(Value(13.3)),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value(5.0)));
  Expression varchar_lt =
      Expression::BinaryExpression(Expression::ConstantValue(Value("aaa")),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value("aaab")));
  Expression varchar_eq =
      Expression::BinaryExpression(Expression::ConstantValue(Value("hello")),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value("hello")));
  Expression varchar_gt =
      Expression::BinaryExpression(Expression::ConstantValue(Value("b")),
                                   BinaryOperation::kGreaterThanEquals,
                                   Expression::ConstantValue(Value("a")));
  Row dummy({});
  ASSERT_EQ(int_lt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(int_gt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_lt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_gt.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_lt.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_gt.Evaluate(dummy, nullptr), Value(1));
}

TEST(ExpressionTest, ColumnValue) {
  std::vector<Column> cols{
      Column("name", ValueType::kVarChar), Column("score", ValueType::kInt64),
      Column("flv", ValueType::kDouble), Column("date", ValueType::kInt64)};
  Schema sc("test_schema", cols);
  Row row({Value("foo"), Value(12), Value(132.3), Value(9)});

  ASSERT_EQ(Expression::ColumnValue("name").Evaluate(row, &sc), Value("foo"));
  ASSERT_EQ(Expression::ColumnValue("score").Evaluate(row, &sc), Value(12));
  ASSERT_EQ(Expression::ColumnValue("flv").Evaluate(row, &sc), Value(132.3));
  ASSERT_EQ(Expression::ColumnValue("date").Evaluate(row, &sc), Value(9));
}

}  // namespace tinylamb