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

TEST(ExpressionTest, UnaryExpression) {
  Row dummy({});
  Schema dummy_schema;

  // IS NULL
  Expression is_null_true =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kIsNull);
  Expression is_null_false =
      UnaryExpressionExp(ConstantValueExp(Value(1)), UnaryOperation::kIsNull);
  ASSERT_EQ(is_null_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(is_null_false->Evaluate(dummy, dummy_schema), Value(false));

  // IS NOT NULL
  Expression is_not_null_true = UnaryExpressionExp(ConstantValueExp(Value(1)),
                                                   UnaryOperation::kIsNotNull);
  Expression is_not_null_false =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kIsNotNull);
  ASSERT_EQ(is_not_null_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(is_not_null_false->Evaluate(dummy, dummy_schema), Value(false));

  // NOT
  Expression not_true =
      UnaryExpressionExp(ConstantValueExp(Value(true)), UnaryOperation::kNot);
  Expression not_false =
      UnaryExpressionExp(ConstantValueExp(Value(false)), UnaryOperation::kNot);
  Expression not_null =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kNot);
  ASSERT_EQ(not_true->Evaluate(dummy, dummy_schema), Value(false));
  ASSERT_EQ(not_false->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_TRUE(not_null->Evaluate(dummy, dummy_schema).IsNull());

  // Unary Minus
  Expression int_minus =
      UnaryExpressionExp(ConstantValueExp(Value(1)), UnaryOperation::kMinus);
  Expression double_minus =
      UnaryExpressionExp(ConstantValueExp(Value(1.1)), UnaryOperation::kMinus);
  ASSERT_EQ(int_minus->Evaluate(dummy, dummy_schema), Value(-1));
  ASSERT_DOUBLE_EQ(
      double_minus->Evaluate(dummy, dummy_schema).value.double_value,
      Value(-1.1).value.double_value);
}

TEST(ExpressionTest, AggregateExpression) {
  Row dummy({});
  Schema dummy_schema;

  Expression count_all =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
  Expression count_col =
      AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("col"));
  Expression sum_col =
      AggregateExpressionExp(AggregationType::kSum, ColumnValueExp("col"));
  Expression avg_col =
      AggregateExpressionExp(AggregationType::kAvg, ColumnValueExp("col"));
  Expression min_col =
      AggregateExpressionExp(AggregationType::kMin, ColumnValueExp("col"));
  Expression max_col =
      AggregateExpressionExp(AggregationType::kMax, ColumnValueExp("col"));

  // Note: Aggregate expressions are not evaluated directly.
  // The executor is responsible for computing the result.
  // Here, we just check the ToString() method.
  ASSERT_EQ(count_all->ToString(), "COUNT(*)");
  ASSERT_EQ(count_col->ToString(), "COUNT(col)");
  ASSERT_EQ(sum_col->ToString(), "SUM(col)");
  ASSERT_EQ(avg_col->ToString(), "AVG(col)");
  ASSERT_EQ(min_col->ToString(), "MIN(col)");
  ASSERT_EQ(max_col->ToString(), "MAX(col)");
}

TEST(ExpressionTest, CaseExpression) {
  Row dummy({});
  Schema dummy_schema;

  Expression case_exp_true =
      CaseExpressionExp({{BinaryExpressionExp(ConstantValueExp(Value(1)),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value(1))),
                          ConstantValueExp(Value("one"))},
                         {BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value(1))),
                          ConstantValueExp(Value("two"))}},
                        ConstantValueExp(Value("other")));
  ASSERT_EQ(case_exp_true->Evaluate(dummy, dummy_schema), Value("one"));

  Expression case_exp_false =
      CaseExpressionExp({{BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value(1))),
                          ConstantValueExp(Value("one"))},
                         {BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kEquals,
                                              ConstantValueExp(Value(1))),
                          ConstantValueExp(Value("two"))}},
                        ConstantValueExp(Value("other")));
  ASSERT_EQ(case_exp_false->Evaluate(dummy, dummy_schema), Value("other"));
}

TEST(ExpressionTest, InExpression) {
  Row dummy({});
  Schema dummy_schema;

  Expression in_exp_true =
      InExpressionExp(ConstantValueExp(Value(1)),
                      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2)),
                       ConstantValueExp(Value(3))});
  Expression in_exp_false =
      InExpressionExp(ConstantValueExp(Value(4)),
                      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2)),
                       ConstantValueExp(Value(3))});
  ASSERT_EQ(in_exp_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(in_exp_false->Evaluate(dummy, dummy_schema), Value(false));
}

TEST(ExpressionTest, PathologicalCases) {
  Row dummy({});
  Schema dummy_schema;

  // (1 + 2) * 3 = 9
  Expression exp1 = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2))),
      BinaryOperation::kMultiply, ConstantValueExp(Value(3)));
  ASSERT_EQ(exp1->Evaluate(dummy, dummy_schema), Value(9));

  // 1 + (2 * 3) = 7
  Expression exp2 =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kMultiply,
                                              ConstantValueExp(Value(3))));
  ASSERT_EQ(exp2->Evaluate(dummy, dummy_schema), Value(7));

  // (true AND false) OR true = true
  Expression exp3 = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(true)), BinaryOperation::kAnd,
                          ConstantValueExp(Value(false))),
      BinaryOperation::kOr, ConstantValueExp(Value(true)));
  ASSERT_EQ(exp3->Evaluate(dummy, dummy_schema), Value(true));

  // true AND (false OR true) = true
  Expression exp4 = BinaryExpressionExp(
      ConstantValueExp(Value(true)), BinaryOperation::kAnd,
      BinaryExpressionExp(ConstantValueExp(Value(false)), BinaryOperation::kOr,
                          ConstantValueExp(Value(true))));
  ASSERT_EQ(exp4->Evaluate(dummy, dummy_schema), Value(true));

  // ((1 + 2) * 3 - (4 / 2)) > 5 AND (true OR false) = (9 - 2) > 5 AND true = 7
  // > 5 AND true = true
  Expression exp5 = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(BinaryExpressionExp(ConstantValueExp(Value(1)),
                                                  BinaryOperation::kAdd,
                                                  ConstantValueExp(Value(2))),
                              BinaryOperation::kMultiply,
                              ConstantValueExp(Value(3))),
          BinaryOperation::kSubtract,
          BinaryExpressionExp(ConstantValueExp(Value(4)),
                              BinaryOperation::kDivide,
                              ConstantValueExp(Value(2)))),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(5)));
  ASSERT_EQ(exp5->Evaluate(dummy, dummy_schema), Value(true));
}

}  // namespace tinylamb
