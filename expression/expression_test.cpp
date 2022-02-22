//
// Created by kumagi on 2022/02/22.
//

#include "common/log_message.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

TEST(ExpressionTest, Constant) {
  ConstantValue cv_int(Value(1));
  ConstantValue cv_varchar(Value("hello"));
  ConstantValue cv_double(Value(1.1));
  LOG(INFO) << "cv_int: " << cv_int << "\ncv_varchar: " << cv_varchar
            << "\ncv_double: " << cv_double;
}

TEST(ExpressionTest, ConstantEval) {
  ConstantValue cv_int(Value(1));
  ConstantValue cv_varchar(Value("hello"));
  ConstantValue cv_double(Value(1.1));
  Row dummy({});
  ASSERT_EQ(cv_int.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(cv_varchar.Evaluate(dummy, nullptr), Value("hello"));
  ASSERT_EQ(cv_double.Evaluate(dummy, nullptr), Value(1.1));
}

TEST(ExpressionTest, BinaryPlus) {
  BinaryExpression int_plus(std::make_unique<ConstantValue>(Value(1)),
                            std::make_unique<ConstantValue>(Value(2)),
                            OpType::kAdd);
  BinaryExpression varchar_plus(
      std::make_unique<ConstantValue>(Value("hello")),
      std::make_unique<ConstantValue>(Value(" world")), OpType::kAdd);
  BinaryExpression double_plus(std::make_unique<ConstantValue>(Value(1.1)),
                               std::make_unique<ConstantValue>(Value(2.2)),
                               OpType::kAdd);
  Row dummy({});
  ASSERT_EQ(int_plus.Evaluate(dummy, nullptr), Value(3));
  ASSERT_EQ(varchar_plus.Evaluate(dummy, nullptr), Value("hello world"));
  ASSERT_DOUBLE_EQ(double_plus.Evaluate(dummy, nullptr).value.double_value,
                   Value(3.3).value.double_value);
}

TEST(ExpressionTest, BinaryMinus) {
  BinaryExpression int_minus(std::make_unique<ConstantValue>(Value(1)),
                             std::make_unique<ConstantValue>(Value(2)),
                             OpType::kSubtract);
  BinaryExpression double_minus(std::make_unique<ConstantValue>(Value(1.1)),
                                std::make_unique<ConstantValue>(Value(2.2)),
                                OpType::kSubtract);
  Row dummy({});
  ASSERT_EQ(int_minus.Evaluate(dummy, nullptr), Value(-1));
  ASSERT_DOUBLE_EQ(double_minus.Evaluate(dummy, nullptr).value.double_value,
                   Value(-1.1).value.double_value);
}

TEST(ExpressionTest, BinaryMultiple) {
  BinaryExpression int_multiple(std::make_unique<ConstantValue>(Value(1)),
                                std::make_unique<ConstantValue>(Value(2)),
                                OpType::kMultiply);
  BinaryExpression double_multiple(std::make_unique<ConstantValue>(Value(1.1)),
                                   std::make_unique<ConstantValue>(Value(2.2)),
                                   OpType::kMultiply);
  Row dummy({});
  ASSERT_EQ(int_multiple.Evaluate(dummy, nullptr), Value(2));
  ASSERT_DOUBLE_EQ(double_multiple.Evaluate(dummy, nullptr).value.double_value,
                   Value(2.42).value.double_value);
}

TEST(ExpressionTest, BinaryDiv) {
  BinaryExpression int_div(std::make_unique<ConstantValue>(Value(10)),
                           std::make_unique<ConstantValue>(Value(2)),
                           OpType::kDivide);
  BinaryExpression double_div(std::make_unique<ConstantValue>(Value(8.8)),
                              std::make_unique<ConstantValue>(Value(2.2)),
                              OpType::kDivide);
  Row dummy({});
  ASSERT_EQ(int_div.Evaluate(dummy, nullptr), Value(5));
  ASSERT_DOUBLE_EQ(double_div.Evaluate(dummy, nullptr).value.double_value,
                   Value(4.0).value.double_value);
}

TEST(ExpressionTest, BinaryMod) {
  BinaryExpression int_mod(std::make_unique<ConstantValue>(Value(13)),
                           std::make_unique<ConstantValue>(Value(5)),
                           OpType::kModulo);
  Row dummy({});
  ASSERT_EQ(int_mod.Evaluate(dummy, nullptr), Value(3));
}

TEST(ExpressionTest, Equal) {
  BinaryExpression int_eq(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(120)),
                          OpType::kEquals);
  BinaryExpression int_ne(std::make_unique<ConstantValue>(Value(13)),
                          std::make_unique<ConstantValue>(Value(5)),
                          OpType::kEquals);
  BinaryExpression double_eq(std::make_unique<ConstantValue>(Value(120.0)),
                             std::make_unique<ConstantValue>(Value(120.0)),
                             OpType::kEquals);
  BinaryExpression double_ne(std::make_unique<ConstantValue>(Value(13.0)),
                             std::make_unique<ConstantValue>(Value(5.0)),
                             OpType::kEquals);
  BinaryExpression varchar_eq(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("hello")),
                              OpType::kEquals);
  BinaryExpression varchar_ne(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("world")),
                              OpType::kEquals);
  Row dummy({});
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(int_ne.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_ne.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_ne.Evaluate(dummy, nullptr), Value(0));
}

TEST(ExpressionTest, NotEqual) {
  BinaryExpression int_eq(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(120)),
                          OpType::kNotEquals);
  BinaryExpression int_ne(std::make_unique<ConstantValue>(Value(13)),
                          std::make_unique<ConstantValue>(Value(5)),
                          OpType::kNotEquals);
  BinaryExpression double_eq(std::make_unique<ConstantValue>(Value(120.0)),
                             std::make_unique<ConstantValue>(Value(120.0)),
                             OpType::kNotEquals);
  BinaryExpression double_ne(std::make_unique<ConstantValue>(Value(13.0)),
                             std::make_unique<ConstantValue>(Value(5.0)),
                             OpType::kNotEquals);
  BinaryExpression varchar_eq(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("hello")),
                              OpType::kNotEquals);
  BinaryExpression varchar_ne(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("world")),
                              OpType::kNotEquals);
  Row dummy({});
  ASSERT_EQ(int_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(int_ne.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(double_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(double_ne.Evaluate(dummy, nullptr), Value(1));
  ASSERT_EQ(varchar_eq.Evaluate(dummy, nullptr), Value(0));
  ASSERT_EQ(varchar_ne.Evaluate(dummy, nullptr), Value(1));
}

TEST(ExpressionTest, LessThan) {
  BinaryExpression int_lt(std::make_unique<ConstantValue>(Value(100)),
                          std::make_unique<ConstantValue>(Value(12312)),
                          OpType::kLessThan);
  BinaryExpression int_eq(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(120)),
                          OpType::kLessThan);
  BinaryExpression int_gt(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(-1)),
                          OpType::kLessThan);
  BinaryExpression double_lt(std::make_unique<ConstantValue>(Value(1.2)),
                             std::make_unique<ConstantValue>(Value(2.2)),
                             OpType::kLessThan);
  BinaryExpression double_eq(std::make_unique<ConstantValue>(Value(120.0)),
                             std::make_unique<ConstantValue>(Value(120.0)),
                             OpType::kLessThan);
  BinaryExpression double_gt(std::make_unique<ConstantValue>(Value(13.3)),
                             std::make_unique<ConstantValue>(Value(5.0)),
                             OpType::kLessThan);
  BinaryExpression varchar_lt(std::make_unique<ConstantValue>(Value("aaa")),
                              std::make_unique<ConstantValue>(Value("aaab")),
                              OpType::kLessThan);
  BinaryExpression varchar_eq(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("hello")),
                              OpType::kLessThan);
  BinaryExpression varchar_gt(std::make_unique<ConstantValue>(Value("b")),
                              std::make_unique<ConstantValue>(Value("a")),
                              OpType::kLessThan);
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
  BinaryExpression int_lt(std::make_unique<ConstantValue>(Value(100)),
                          std::make_unique<ConstantValue>(Value(12312)),
                          OpType::kLessThanEquals);
  BinaryExpression int_eq(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(120)),
                          OpType::kLessThanEquals);
  BinaryExpression int_gt(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(-1)),
                          OpType::kLessThanEquals);
  BinaryExpression double_lt(std::make_unique<ConstantValue>(Value(1.2)),
                             std::make_unique<ConstantValue>(Value(2.2)),
                             OpType::kLessThanEquals);
  BinaryExpression double_eq(std::make_unique<ConstantValue>(Value(120.0)),
                             std::make_unique<ConstantValue>(Value(120.0)),
                             OpType::kLessThanEquals);
  BinaryExpression double_gt(std::make_unique<ConstantValue>(Value(13.3)),
                             std::make_unique<ConstantValue>(Value(5.0)),
                             OpType::kLessThanEquals);
  BinaryExpression varchar_lt(std::make_unique<ConstantValue>(Value("aaa")),
                              std::make_unique<ConstantValue>(Value("aaab")),
                              OpType::kLessThanEquals);
  BinaryExpression varchar_eq(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("hello")),
                              OpType::kLessThanEquals);
  BinaryExpression varchar_gt(std::make_unique<ConstantValue>(Value("b")),
                              std::make_unique<ConstantValue>(Value("a")),
                              OpType::kLessThanEquals);
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
  BinaryExpression int_lt(std::make_unique<ConstantValue>(Value(100)),
                          std::make_unique<ConstantValue>(Value(12312)),
                          OpType::kGreaterThan);
  BinaryExpression int_eq(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(120)),
                          OpType::kGreaterThan);
  BinaryExpression int_gt(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(-1)),
                          OpType::kGreaterThan);
  BinaryExpression double_lt(std::make_unique<ConstantValue>(Value(1.2)),
                             std::make_unique<ConstantValue>(Value(2.2)),
                             OpType::kGreaterThan);
  BinaryExpression double_eq(std::make_unique<ConstantValue>(Value(120.0)),
                             std::make_unique<ConstantValue>(Value(120.0)),
                             OpType::kGreaterThan);
  BinaryExpression double_gt(std::make_unique<ConstantValue>(Value(13.3)),
                             std::make_unique<ConstantValue>(Value(5.0)),
                             OpType::kGreaterThan);
  BinaryExpression varchar_lt(std::make_unique<ConstantValue>(Value("aaa")),
                              std::make_unique<ConstantValue>(Value("aaab")),
                              OpType::kGreaterThan);
  BinaryExpression varchar_eq(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("hello")),
                              OpType::kGreaterThan);
  BinaryExpression varchar_gt(std::make_unique<ConstantValue>(Value("b")),
                              std::make_unique<ConstantValue>(Value("a")),
                              OpType::kGreaterThan);
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
  BinaryExpression int_lt(std::make_unique<ConstantValue>(Value(100)),
                          std::make_unique<ConstantValue>(Value(12312)),
                          OpType::kGreaterThanEquals);
  BinaryExpression int_eq(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(120)),
                          OpType::kGreaterThanEquals);
  BinaryExpression int_gt(std::make_unique<ConstantValue>(Value(120)),
                          std::make_unique<ConstantValue>(Value(-1)),
                          OpType::kGreaterThanEquals);
  BinaryExpression double_lt(std::make_unique<ConstantValue>(Value(1.2)),
                             std::make_unique<ConstantValue>(Value(2.2)),
                             OpType::kGreaterThanEquals);
  BinaryExpression double_eq(std::make_unique<ConstantValue>(Value(120.0)),
                             std::make_unique<ConstantValue>(Value(120.0)),
                             OpType::kGreaterThanEquals);
  BinaryExpression double_gt(std::make_unique<ConstantValue>(Value(13.3)),
                             std::make_unique<ConstantValue>(Value(5.0)),
                             OpType::kGreaterThanEquals);
  BinaryExpression varchar_lt(std::make_unique<ConstantValue>(Value("aaa")),
                              std::make_unique<ConstantValue>(Value("aaab")),
                              OpType::kGreaterThanEquals);
  BinaryExpression varchar_eq(std::make_unique<ConstantValue>(Value("hello")),
                              std::make_unique<ConstantValue>(Value("hello")),
                              OpType::kGreaterThanEquals);
  BinaryExpression varchar_gt(std::make_unique<ConstantValue>(Value("b")),
                              std::make_unique<ConstantValue>(Value("a")),
                              OpType::kGreaterThanEquals);
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

  ASSERT_EQ(ColumnValue("name").Evaluate(row, &sc), Value("foo"));
  ASSERT_EQ(ColumnValue("score").Evaluate(row, &sc), Value(12));
  ASSERT_EQ(ColumnValue("flv").Evaluate(row, &sc), Value(132.3));
  ASSERT_EQ(ColumnValue("date").Evaluate(row, &sc), Value(9));
}

}  // namespace tinylamb