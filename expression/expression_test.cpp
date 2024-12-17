/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "expression/expression.hpp"

#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(ExpressionTest, Constant) {
  Expression cv_int = ConstantValueExp(Value(1));
  Expression cv_varchar = ConstantValueExp(Value("hello"));
  Expression cv_double = ConstantValueExp(Value(1.1));
  LOG(INFO) << "cv_int: " << cv_int << "\ncv_varchar: " << cv_varchar
            << "\ncv_double: " << cv_double;
}

TEST(ExpressionTest, ConstantEval) {
  Expression cv_int = ConstantValueExp(Value(1));
  Expression cv_varchar = ConstantValueExp(Value("hello"));
  Expression cv_double = ConstantValueExp(Value(1.1));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(cv_int->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(cv_varchar->Evaluate(dummy, dummy_schema), Value("hello"));
  ASSERT_EQ(cv_double->Evaluate(dummy, dummy_schema), Value(1.1));
}

TEST(ExpressionTest, BinaryPlus) {
  Expression int_plus =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2)));
  Expression varchar_plus = BinaryExpressionExp(
      ConstantValueExp(Value("hello")), BinaryOperation::kAdd,
      ConstantValueExp(Value(" world")));
  Expression double_plus =
      BinaryExpressionExp(ConstantValueExp(Value(1.1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_plus->Evaluate(dummy, dummy_schema), Value(3));
  ASSERT_EQ(varchar_plus->Evaluate(dummy, dummy_schema), Value("hello world"));
  ASSERT_DOUBLE_EQ(
      double_plus->Evaluate(dummy, dummy_schema).value.double_value,
      Value(3.3).value.double_value);
}

TEST(ExpressionTest, BinaryMinus) {
  Expression int_minus = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                             BinaryOperation::kSubtract,
                                             ConstantValueExp(Value(2)));
  Expression double_minus = BinaryExpressionExp(ConstantValueExp(Value(1.1)),
                                                BinaryOperation::kSubtract,
                                                ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_minus->Evaluate(dummy, dummy_schema), Value(-1));
  ASSERT_DOUBLE_EQ(
      double_minus->Evaluate(dummy, dummy_schema).value.double_value,
      Value(-1.1).value.double_value);
}

TEST(ExpressionTest, BinaryMultiple) {
  Expression int_multiple = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                                BinaryOperation::kMultiply,
                                                ConstantValueExp(Value(2)));
  Expression double_multiple = BinaryExpressionExp(
      ConstantValueExp(Value(1.1)), BinaryOperation::kMultiply,
      ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_multiple->Evaluate(dummy, dummy_schema), Value(2));
  ASSERT_DOUBLE_EQ(
      double_multiple->Evaluate(dummy, dummy_schema).value.double_value,
      Value(2.42).value.double_value);
}

TEST(ExpressionTest, BinaryDiv) {
  Expression int_div =
      BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kDivide,
                          ConstantValueExp(Value(2)));
  Expression double_div = BinaryExpressionExp(ConstantValueExp(Value(8.8)),
                                              BinaryOperation::kDivide,
                                              ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_div->Evaluate(dummy, dummy_schema), Value(5));
  ASSERT_DOUBLE_EQ(double_div->Evaluate(dummy, dummy_schema).value.double_value,
                   Value(4.0).value.double_value);
}

TEST(ExpressionTest, BinaryMod) {
  Expression int_mod =
      BinaryExpressionExp(ConstantValueExp(Value(13)), BinaryOperation::kModulo,
                          ConstantValueExp(Value(5)));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_mod->Evaluate(dummy, dummy_schema), Value(3));
}

TEST(ExpressionTest, Equal) {
  Expression int_eq = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kEquals,
                                          ConstantValueExp(Value(120)));
  Expression int_ne =
      BinaryExpressionExp(ConstantValueExp(Value(13)), BinaryOperation::kEquals,
                          ConstantValueExp(Value(5)));
  Expression double_eq = BinaryExpressionExp(ConstantValueExp(Value(120.0)),
                                             BinaryOperation::kEquals,
                                             ConstantValueExp(Value(120.0)));
  Expression double_ne = BinaryExpressionExp(ConstantValueExp(Value(13.0)),
                                             BinaryOperation::kEquals,
                                             ConstantValueExp(Value(5.0)));
  Expression varchar_eq = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value("hello")));
  Expression varchar_ne = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value("world")));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(int_ne->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_ne->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_ne->Evaluate(dummy, dummy_schema), Value(0));
}

TEST(ExpressionTest, NotEqual) {
  Expression int_eq = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kNotEquals,
                                          ConstantValueExp(Value(120)));
  Expression int_ne = BinaryExpressionExp(ConstantValueExp(Value(13)),
                                          BinaryOperation::kNotEquals,
                                          ConstantValueExp(Value(5)));
  Expression double_eq = BinaryExpressionExp(ConstantValueExp(Value(120.0)),
                                             BinaryOperation::kNotEquals,
                                             ConstantValueExp(Value(120.0)));
  Expression double_ne = BinaryExpressionExp(ConstantValueExp(Value(13.0)),
                                             BinaryOperation::kNotEquals,
                                             ConstantValueExp(Value(5.0)));
  Expression varchar_eq = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kNotEquals,
                                              ConstantValueExp(Value("hello")));
  Expression varchar_ne = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kNotEquals,
                                              ConstantValueExp(Value("world")));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(int_ne->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_ne->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_ne->Evaluate(dummy, dummy_schema), Value(1));
}

TEST(ExpressionTest, LessThan) {
  Expression int_lt = BinaryExpressionExp(ConstantValueExp(Value(100)),
                                          BinaryOperation::kLessThan,
                                          ConstantValueExp(Value(12312)));
  Expression int_eq = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kLessThan,
                                          ConstantValueExp(Value(120)));
  Expression int_gt = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kLessThan,
                                          ConstantValueExp(Value(-1)));
  Expression double_lt = BinaryExpressionExp(ConstantValueExp(Value(1.2)),
                                             BinaryOperation::kLessThan,
                                             ConstantValueExp(Value(2.2)));
  Expression double_eq = BinaryExpressionExp(ConstantValueExp(Value(120.0)),
                                             BinaryOperation::kLessThan,
                                             ConstantValueExp(Value(120.0)));
  Expression double_gt = BinaryExpressionExp(ConstantValueExp(Value(13.3)),
                                             BinaryOperation::kLessThan,
                                             ConstantValueExp(Value(5.0)));
  Expression varchar_lt = BinaryExpressionExp(ConstantValueExp(Value("aaa")),
                                              BinaryOperation::kLessThan,
                                              ConstantValueExp(Value("aaab")));
  Expression varchar_eq = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kLessThan,
                                              ConstantValueExp(Value("hello")));
  Expression varchar_gt = BinaryExpressionExp(ConstantValueExp(Value("b")),
                                              BinaryOperation::kLessThan,
                                              ConstantValueExp(Value("a")));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_lt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(int_gt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_lt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_gt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_lt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_gt->Evaluate(dummy, dummy_schema), Value(0));
}

TEST(ExpressionTest, LessThanEquals) {
  Expression int_lt = BinaryExpressionExp(ConstantValueExp(Value(100)),
                                          BinaryOperation::kLessThanEquals,
                                          ConstantValueExp(Value(12312)));
  Expression int_eq = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kLessThanEquals,
                                          ConstantValueExp(Value(120)));
  Expression int_gt = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kLessThanEquals,
                                          ConstantValueExp(Value(-1)));
  Expression double_lt = BinaryExpressionExp(ConstantValueExp(Value(1.2)),
                                             BinaryOperation::kLessThanEquals,
                                             ConstantValueExp(Value(2.2)));
  Expression double_eq = BinaryExpressionExp(ConstantValueExp(Value(120.0)),
                                             BinaryOperation::kLessThanEquals,
                                             ConstantValueExp(Value(120.0)));
  Expression double_gt = BinaryExpressionExp(ConstantValueExp(Value(13.3)),
                                             BinaryOperation::kLessThanEquals,
                                             ConstantValueExp(Value(5.0)));
  Expression varchar_lt = BinaryExpressionExp(ConstantValueExp(Value("aaa")),
                                              BinaryOperation::kLessThanEquals,
                                              ConstantValueExp(Value("aaab")));
  Expression varchar_eq = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kLessThanEquals,
                                              ConstantValueExp(Value("hello")));
  Expression varchar_gt = BinaryExpressionExp(ConstantValueExp(Value("b")),
                                              BinaryOperation::kLessThanEquals,
                                              ConstantValueExp(Value("a")));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_lt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(int_gt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_lt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_gt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_lt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_gt->Evaluate(dummy, dummy_schema), Value(0));
}

TEST(ExpressionTest, GreaterThan) {
  Expression int_lt = BinaryExpressionExp(ConstantValueExp(Value(100)),
                                          BinaryOperation::kGreaterThan,
                                          ConstantValueExp(Value(12312)));
  Expression int_eq = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kGreaterThan,
                                          ConstantValueExp(Value(120)));
  Expression int_gt = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kGreaterThan,
                                          ConstantValueExp(Value(-1)));
  Expression double_lt = BinaryExpressionExp(ConstantValueExp(Value(1.2)),
                                             BinaryOperation::kGreaterThan,
                                             ConstantValueExp(Value(2.2)));
  Expression double_eq = BinaryExpressionExp(ConstantValueExp(Value(120.0)),
                                             BinaryOperation::kGreaterThan,
                                             ConstantValueExp(Value(120.0)));
  Expression double_gt = BinaryExpressionExp(ConstantValueExp(Value(13.3)),
                                             BinaryOperation::kGreaterThan,
                                             ConstantValueExp(Value(5.0)));
  Expression varchar_lt = BinaryExpressionExp(ConstantValueExp(Value("aaa")),
                                              BinaryOperation::kGreaterThan,
                                              ConstantValueExp(Value("aaab")));
  Expression varchar_eq = BinaryExpressionExp(ConstantValueExp(Value("hello")),
                                              BinaryOperation::kGreaterThan,
                                              ConstantValueExp(Value("hello")));
  Expression varchar_gt = BinaryExpressionExp(ConstantValueExp(Value("b")),
                                              BinaryOperation::kGreaterThan,
                                              ConstantValueExp(Value("a")));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_lt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(int_gt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_lt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_gt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_lt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_gt->Evaluate(dummy, dummy_schema), Value(1));
}

TEST(ExpressionTest, GreaterThanEquals) {
  Expression int_lt = BinaryExpressionExp(ConstantValueExp(Value(100)),
                                          BinaryOperation::kGreaterThanEquals,
                                          ConstantValueExp(Value(12312)));
  Expression int_eq = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kGreaterThanEquals,
                                          ConstantValueExp(Value(120)));
  Expression int_gt = BinaryExpressionExp(ConstantValueExp(Value(120)),
                                          BinaryOperation::kGreaterThanEquals,
                                          ConstantValueExp(Value(-1)));
  Expression double_lt = BinaryExpressionExp(
      ConstantValueExp(Value(1.2)), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value(2.2)));
  Expression double_eq = BinaryExpressionExp(
      ConstantValueExp(Value(120.0)), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value(120.0)));
  Expression double_gt = BinaryExpressionExp(
      ConstantValueExp(Value(13.3)), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value(5.0)));
  Expression varchar_lt = BinaryExpressionExp(
      ConstantValueExp(Value("aaa")), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value("aaab")));
  Expression varchar_eq = BinaryExpressionExp(
      ConstantValueExp(Value("hello")), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value("hello")));
  Expression varchar_gt = BinaryExpressionExp(
      ConstantValueExp(Value("b")), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value("a")));
  Row dummy({});
  Schema dummy_schema;
  ASSERT_EQ(int_lt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(int_gt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_lt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_gt->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_lt->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_gt->Evaluate(dummy, dummy_schema), Value(1));
}

TEST(ExpressionTest, ColumnValue) {
  std::vector<Column> cols{
      Column("name", ValueType::kVarChar), Column("score", ValueType::kInt64),
      Column("flv", ValueType::kDouble), Column("date", ValueType::kInt64)};
  Schema sc("sc", cols);
  Row row({Value("foo"), Value(12), Value(132.3), Value(9)});

  ASSERT_EQ(ColumnValueExp("sc.name")->Evaluate(row, sc), Value("foo"));
  ASSERT_EQ(ColumnValueExp("score")->Evaluate(row, sc), Value(12));
  ASSERT_EQ(ColumnValueExp("flv")->Evaluate(row, sc), Value(132.3));
  ASSERT_EQ(ColumnValueExp("date")->Evaluate(row, sc), Value(9));
}

}  // namespace tinylamb