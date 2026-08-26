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

#include <cmath>
#include <memory>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "gtest/gtest.h"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(ExpressionTest, ConstantValue_WhenConstructed_OutputsToString) {
  Expression cv_int = ConstantValueExp(Value(1));
  Expression cv_varchar = ConstantValueExp(Value("hello"));
  Expression cv_double = ConstantValueExp(Value(1.1));

  LOG(INFO) << "cv_int: " << cv_int << "\ncv_varchar: " << cv_varchar
            << "\ncv_double: " << cv_double;
}

TEST(ExpressionTest, ConstantValue_Evaluate_ReturnsConstantValue) {
  Expression cv_int = ConstantValueExp(Value(1));
  Expression cv_varchar = ConstantValueExp(Value("hello"));
  Expression cv_double = ConstantValueExp(Value(1.1));
  Row dummy({});
  Schema dummy_schema;

  ASSERT_EQ(cv_int->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(cv_varchar->Evaluate(dummy, dummy_schema), Value("hello"));
  ASSERT_EQ(cv_double->Evaluate(dummy, dummy_schema), Value(1.1));
}

TEST(ExpressionTest, BinaryAdd_WithIntVarcharAndDouble_ComputesSumOrConcatenates) {
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

TEST(ExpressionTest, BinarySubtract_WithIntAndDouble_ComputesDifference) {
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

TEST(ExpressionTest, BinaryMultiply_WithIntAndDouble_ComputesProduct) {
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

TEST(ExpressionTest, BinaryDivide_WithIntAndDouble_ComputesQuotient) {
  Expression int_div =
      BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kDivide,
                          ConstantValueExp(Value(2)));
  Expression double_div = BinaryExpressionExp(ConstantValueExp(Value(8.8)),
                                              BinaryOperation::kDivide,
                                              ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;

  ASSERT_EQ(int_div->Evaluate(dummy, dummy_schema), Value(5.0));
  EXPECT_NEAR(double_div->Evaluate(dummy, dummy_schema).value.double_value, 4.0, 1e-6);
}

TEST(ExpressionTest, BinaryModulo_WithInt_ComputesRemainder) {
  Expression int_mod =
      BinaryExpressionExp(ConstantValueExp(Value(13)), BinaryOperation::kModulo,
                          ConstantValueExp(Value(5)));
  Row dummy({});
  Schema dummy_schema;

  ASSERT_EQ(int_mod->Evaluate(dummy, dummy_schema), Value(3));
}

TEST(ExpressionTest, BinaryEquals_WithVariousTypes_ReturnsCorrectBoolean) {
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

TEST(ExpressionTest, BinaryNotEquals_WithVariousTypes_ReturnsCorrectBoolean) {
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

TEST(ExpressionTest, BinaryLessThan_WithVariousTypes_ReturnsCorrectBoolean) {
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

TEST(ExpressionTest, BinaryLessThanEquals_WithVariousTypes_ReturnsCorrectBoolean) {
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

TEST(ExpressionTest, BinaryGreaterThan_WithVariousTypes_ReturnsCorrectBoolean) {
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

TEST(ExpressionTest, BinaryGreaterThanEquals_WithVariousTypes_ReturnsCorrectBoolean) {
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

TEST(ExpressionTest, ColumnValue_Evaluate_ResolvesColumnFromRow) {
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

TEST(ExpressionTest, UnaryExpression_Evaluate_ComputesExpectedResults) {
  Row dummy({});
  Schema dummy_schema;

  Expression is_null_true =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kIsNull);
  Expression is_null_false =
      UnaryExpressionExp(ConstantValueExp(Value(1)), UnaryOperation::kIsNull);
  ASSERT_EQ(is_null_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(is_null_false->Evaluate(dummy, dummy_schema), Value(false));

  Expression is_not_null_true = UnaryExpressionExp(ConstantValueExp(Value(1)),
                                                   UnaryOperation::kIsNotNull);
  Expression is_not_null_false =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kIsNotNull);
  ASSERT_EQ(is_not_null_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(is_not_null_false->Evaluate(dummy, dummy_schema), Value(false));

  Expression not_true =
      UnaryExpressionExp(ConstantValueExp(Value(true)), UnaryOperation::kNot);
  Expression not_false =
      UnaryExpressionExp(ConstantValueExp(Value(false)), UnaryOperation::kNot);
  Expression not_null =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kNot);
  ASSERT_EQ(not_true->Evaluate(dummy, dummy_schema), Value(false));
  ASSERT_EQ(not_false->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_TRUE(not_null->Evaluate(dummy, dummy_schema).IsNull());

  Expression int_minus =
      UnaryExpressionExp(ConstantValueExp(Value(1)), UnaryOperation::kMinus);
  Expression double_minus =
      UnaryExpressionExp(ConstantValueExp(Value(1.1)), UnaryOperation::kMinus);
  ASSERT_EQ(int_minus->Evaluate(dummy, dummy_schema), Value(-1));
  ASSERT_DOUBLE_EQ(
      double_minus->Evaluate(dummy, dummy_schema).value.double_value,
      Value(-1.1).value.double_value);
}

TEST(ExpressionTest, AggregateExpression_ToString_RendersSqlSyntax) {
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

  ASSERT_EQ(count_all->ToString(), "COUNT(*)");
  ASSERT_EQ(count_col->ToString(), "COUNT(col)");
  ASSERT_EQ(sum_col->ToString(), "SUM(col)");
  ASSERT_EQ(avg_col->ToString(), "AVG(col)");
  ASSERT_EQ(min_col->ToString(), "MIN(col)");
  ASSERT_EQ(max_col->ToString(), "MAX(col)");
}

TEST(ExpressionTest, CaseExpression_Evaluate_EvaluatesMatchingBranchOrElse) {
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

  ASSERT_EQ(case_exp_true->Evaluate(dummy, dummy_schema), Value("one"));
  ASSERT_EQ(case_exp_false->Evaluate(dummy, dummy_schema), Value("other"));
}

TEST(ExpressionTest, InExpression_Evaluate_ReturnsTrueIfContained) {
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

TEST(ExpressionTest, BinaryExpression_WithNestedExpressions_ComputesCorrectly) {
  Row dummy({});
  Schema dummy_schema;

  Expression nested_product = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2))),
      BinaryOperation::kMultiply, ConstantValueExp(Value(3)));
  ASSERT_EQ(nested_product->Evaluate(dummy, dummy_schema), Value(9));

  Expression exp2 =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kMultiply,
                                              ConstantValueExp(Value(3))));
  ASSERT_EQ(exp2->Evaluate(dummy, dummy_schema), Value(7));

  Expression exp3 = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(true)), BinaryOperation::kAnd,
                          ConstantValueExp(Value(false))),
      BinaryOperation::kOr, ConstantValueExp(Value(true)));
  ASSERT_EQ(exp3->Evaluate(dummy, dummy_schema), Value(true));

  Expression exp4 = BinaryExpressionExp(
      ConstantValueExp(Value(true)), BinaryOperation::kAnd,
      BinaryExpressionExp(ConstantValueExp(Value(false)), BinaryOperation::kOr,
                          ConstantValueExp(Value(true))));
  ASSERT_EQ(exp4->Evaluate(dummy, dummy_schema), Value(true));

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

TEST(ExpressionTest, BinaryExpression_WithNullOperands_FollowsThreeValuedLogic) {
  const Row row;
  const Schema schema;
  const Expression null_value = ConstantValueExp(Value());
  const Expression true_value = ConstantValueExp(Value(true));
  const Expression false_value = ConstantValueExp(Value(false));

  EXPECT_EQ(BinaryExpressionExp(null_value, BinaryOperation::kAnd, false_value)
                ->Evaluate(row, schema),
            Value(false));
  EXPECT_TRUE(BinaryExpressionExp(null_value, BinaryOperation::kAnd, true_value)
                  ->Evaluate(row, schema)
                  .IsNull());
  EXPECT_EQ(BinaryExpressionExp(null_value, BinaryOperation::kOr, true_value)
                ->Evaluate(row, schema),
            Value(true));
  EXPECT_TRUE(BinaryExpressionExp(null_value, BinaryOperation::kOr, false_value)
                  ->Evaluate(row, schema)
                  .IsNull());
  EXPECT_TRUE(BinaryExpressionExp(null_value, BinaryOperation::kEquals,
                                  ConstantValueExp(Value(1)))
                  ->Evaluate(row, schema)
                  .IsNull());
}

TEST(ExpressionTest, BinaryExpression_ArithmeticWithNull_PropagatesNull) {
  const Row row;
  const Schema schema;
  const Expression null_value = ConstantValueExp(Value());
  const Expression one = ConstantValueExp(Value(1));

  for (const BinaryOperation op : {BinaryOperation::kAdd,
                                   BinaryOperation::kSubtract,
                                   BinaryOperation::kMultiply,
                                   BinaryOperation::kDivide,
                                   BinaryOperation::kModulo}) {
    const Value result =
        BinaryExpressionExp(null_value, op, one)->Evaluate(row, schema);
    EXPECT_TRUE(result.IsNull()) << "op " << static_cast<int>(op);
  }
  for (const BinaryOperation op : {BinaryOperation::kLessThan,
                                   BinaryOperation::kLessThanEquals,
                                   BinaryOperation::kGreaterThan,
                                   BinaryOperation::kGreaterThanEquals}) {
    const Value result =
        BinaryExpressionExp(null_value, op, one)->Evaluate(row, schema);
    EXPECT_TRUE(result.IsNull()) << "op " << static_cast<int>(op);
  }
  const Value neg =
      UnaryExpressionExp(null_value, UnaryOperation::kMinus)
          ->Evaluate(row, schema);
  EXPECT_TRUE(neg.IsNull());
}

TEST(ExpressionTest, BinaryExpression_WithMixedNumericTypes_InfersTypeAndComputes) {
  const Schema schema("numbers", {Column("integer", ValueType::kInt64),
                                  Column("floating", ValueType::kDouble)});
  const Row row({Value(2), Value(0.5)});
  const Expression expression =
      BinaryExpressionExp(ColumnValueExp("integer"), BinaryOperation::kAdd,
                          ColumnValueExp("floating"));

  const Value result = expression->Evaluate(row, schema);

  ASSERT_EQ(result.type, ValueType::kDouble);
  EXPECT_DOUBLE_EQ(result.value.double_value, 2.5);
  EXPECT_EQ(expression->ResultType(schema).GetType(), TypeTag::kDouble);
}

TEST(ExpressionTest, FunctionCall_WithTpchScalarFunctions_EvaluatesCorrectly) {
  const Row row;
  const Schema schema;
  const Expression date_add =
      FunctionCallExp("date_add", {ConstantValueExp(Value("1994-01-01")),
                                   IntervalExpressionExp(1, "year")});
  const Expression date_sub =
      FunctionCallExp("date_sub", {ConstantValueExp(Value("1998-12-01")),
                                   IntervalExpressionExp(74, "day")});
  const Expression substring = FunctionCallExp(
      "substr", {ConstantValueExp(Value("19-555-0100")),
                 ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  const Expression year =
      FunctionCallExp("extract_year", {ConstantValueExp(Value("1996-07-15"))});

  EXPECT_EQ(date_add->Evaluate(row, schema), Value("1995-01-01"));
  EXPECT_EQ(date_sub->Evaluate(row, schema), Value("1998-09-18"));
  EXPECT_EQ(substring->Evaluate(row, schema), Value("19"));
  EXPECT_EQ(year->Evaluate(row, schema), Value(1996));
  EXPECT_EQ(date_add->ResultType(schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(year->ResultType(schema).GetType(), TypeTag::kBigInt);
}

namespace {

class BareExpression : public ExpressionBase {
 public:
  using ExpressionBase::Evaluate;
  using ExpressionBase::ResultType;
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kConstantValue;
  }
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override {
    return Value(1);
  }
  [[nodiscard]] std::string ToString() const override { return "bare"; }
  void Dump(std::ostream& o) const override { o << "bare"; }
};

}  // namespace

TEST(ExpressionTest, QueryExpression_Inspect_ReturnsTypeStringAndTouchedColumns) {
  std::shared_ptr<SelectStatement> no_query;
  Expression test = ColumnValueExp("x");
  Expression exists = QueryExpressionExp(no_query, test, true, false);
  Expression not_exists = QueryExpressionExp(no_query, test, true, true);
  Expression in = QueryExpressionExp(no_query, test, false, false);
  Expression not_in = QueryExpressionExp(no_query, test, false, true);
  Expression scalar = QueryExpressionExp(no_query, nullptr, false, false);

  EXPECT_EQ(exists->Type(), TypeTag::kQueryExp);
  EXPECT_EQ(exists->ToString(), "EXISTS(...)");
  EXPECT_EQ(not_exists->ToString(), "NOT EXISTS(...)");
  EXPECT_EQ(in->ToString(), "x IN(...)");
  EXPECT_EQ(not_in->ToString(), "x NOT IN(...)");
  EXPECT_EQ(scalar->ToString(), "SCALAR_SUBQUERY(...)");

  ASSERT_EQ(exists->TouchedColumns().size(), 1);
  EXPECT_EQ(scalar->TouchedColumns().size(), 0);

  const auto& query = exists->AsQueryExpression();
  EXPECT_EQ(query.Test()->Type(), TypeTag::kColumnValue);
  EXPECT_TRUE(query.Exists());
  EXPECT_FALSE(query.Negated());
  EXPECT_EQ(not_exists->AsQueryExpression().Negated(), true);
}

TEST(ExpressionTest, QueryExpression_EvaluateWithoutContext_ThrowsRuntimeError) {
  Row row;
  Schema schema;
  Expression scalar = QueryExpressionExp(
      std::shared_ptr<SelectStatement>(), ColumnValueExp("x"));

  EXPECT_THROW(scalar->Evaluate(row, schema), std::runtime_error);

  std::ostringstream oss;
  scalar->Dump(oss);
  EXPECT_EQ(oss.str(), "x IN(...)");
}

TEST(ExpressionTest, AggregateExpression_EvaluateDirectly_ThrowsLogicError) {
  Schema schema("s", {Column("amount", ValueType::kInt64),
                      Column("rate", ValueType::kDouble)});
  Row row({Value(10), Value(0.5)});
  Expression count = AggregateExpressionExp(AggregationType::kCount,
                                            ColumnValueExp("amount"));
  Expression avg = AggregateExpressionExp(AggregationType::kAvg,
                                          ColumnValueExp("rate"));
  Expression sum = AggregateExpressionExp(AggregationType::kSum,
                                          ColumnValueExp("amount"));
  Expression min = AggregateExpressionExp(AggregationType::kMin,
                                          ColumnValueExp("rate"));
  Expression max = AggregateExpressionExp(AggregationType::kMax,
                                          ColumnValueExp("amount"));

  EXPECT_EQ(count->ResultType(schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(avg->ResultType(schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(sum->ResultType(schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(min->ResultType(schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(max->ResultType(schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(count->ResultType(schema, schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(avg->ResultType(schema, schema).GetType(), TypeTag::kDouble);

  EXPECT_THROW(count->Evaluate(row, schema), std::logic_error);
  EXPECT_EQ(count->ToString(), "COUNT(amount)");
  EXPECT_EQ(AggregateExpressionExp(AggregationType::kCount,
                                   ColumnValueExp("amount"), true)
                ->ToString(),
            "COUNT(DISTINCT amount)");
  EXPECT_EQ(count->TouchedColumns().size(), 1);

  const auto& aggregate = count->AsAggregateExpression();
  EXPECT_EQ(aggregate.GetType(), AggregationType::kCount);
  EXPECT_EQ(aggregate.Child()->Type(), TypeTag::kColumnValue);
  EXPECT_FALSE(aggregate.Distinct());

  std::ostringstream oss;
  count->Dump(oss);
  EXPECT_EQ(oss.str(), "COUNT(amount)");
}

TEST(ExpressionTest, CaseExpression_WithNullConditionOrMissingElse_ReturnsNullOrElse) {
  Schema schema("s", {Column("v", ValueType::kInt64)});
  Row row({Value(5)});
  Expression no_else = CaseExpressionExp(
      {{BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(1))),
        ConstantValueExp(Value("one"))}},
      nullptr);
  Expression null_condition = CaseExpressionExp(
      {{ConstantValueExp(Value()), ConstantValueExp(Value("matched"))}},
      ConstantValueExp(Value("else")));

  EXPECT_TRUE(no_else->Evaluate(row, schema).IsNull());
  EXPECT_EQ(null_condition->Evaluate(&row, schema, nullptr, schema),
            Value("else"));
  EXPECT_EQ(null_condition->Evaluate(row, schema), Value("else"));
}

TEST(ExpressionTest, CaseExpression_ResultType_InfersCorrectType) {
  Schema schema;
  Expression with_when =
      CaseExpressionExp({{ConstantValueExp(Value(true)),
                          ConstantValueExp(Value(1))}},
                        ConstantValueExp(Value(2)));
  Expression no_when_no_else = CaseExpressionExp({}, nullptr);

  EXPECT_EQ(with_when->ResultType(schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(with_when->ResultType(schema, schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(no_when_no_else->ResultType(schema).GetType(), TypeTag::kInvalid);
  EXPECT_EQ(no_when_no_else->ResultType(schema, schema).GetType(),
            TypeTag::kInvalid);
}

TEST(ExpressionTest, CaseExpression_Inspect_ReturnsTouchedColumnsAndString) {
  Row row;
  Schema schema;
  Expression case_exp = CaseExpressionExp(
      {{ColumnValueExp("a"), ColumnValueExp("b")}}, ColumnValueExp("c"));

  EXPECT_EQ(case_exp->Type(), TypeTag::kCaseExp);
  EXPECT_EQ(case_exp->TouchedColumns().size(), 3);
  EXPECT_EQ(case_exp->ToString(), "CASE WHEN a THEN b ELSE c END");

  std::ostringstream oss;
  case_exp->Dump(oss);
  EXPECT_EQ(oss.str(), "CASE WHEN a THEN b ELSE c END");
}

TEST(ExpressionTest, IntervalExpression_EvaluateAndToString_ReturnsFormattedInterval) {
  Row row;
  Schema schema;
  Expression interval = IntervalExpressionExp(3, "day");

  EXPECT_EQ(interval->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(interval->Evaluate(row, schema), Value("0-0 3 0:0:0"));
  EXPECT_EQ(interval->Evaluate(&row, schema, &row, schema), Value("0-0 3 0:0:0"));
  EXPECT_EQ(interval->ResultType(schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(interval->ResultType(schema, schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(interval->ToString(), "INTERVAL 0-0 3 0:0:0");

  std::ostringstream oss;
  interval->Dump(oss);
  EXPECT_EQ(oss.str(), "INTERVAL 0-0 3 0:0:0");

  const auto& interval_expression = interval->AsIntervalExpression();
  EXPECT_EQ(interval_expression.Amount(), 3);
  EXPECT_EQ(interval_expression.Unit(), "day");
}

TEST(ExpressionTest, InExpression_WithNullValues_FollowsThreeValuedLogic) {
  Row row;
  Schema schema;
  Expression child_null = InExpressionExp(
      ConstantValueExp(Value()),
      {ConstantValueExp(Value(1)), ConstantValueExp(Value(2))});
  Expression candidate_null = InExpressionExp(
      ConstantValueExp(Value(3)),
      {ConstantValueExp(Value()), ConstantValueExp(Value(2))});
  Expression candidate_null_match = InExpressionExp(
      ConstantValueExp(Value(2)),
      {ConstantValueExp(Value()), ConstantValueExp(Value(2))});

  EXPECT_TRUE(child_null->Evaluate(row, schema).IsNull());
  EXPECT_TRUE(candidate_null->Evaluate(row, schema).IsNull());
  EXPECT_EQ(candidate_null_match->Evaluate(row, schema), Value(true));
  EXPECT_TRUE(InExpressionExp(ConstantValueExp(Value(3)),
                              {ConstantValueExp(Value())})
                  ->Evaluate(&row, schema, &row, schema)
                  .IsNull());
  EXPECT_EQ(InExpressionExp(ConstantValueExp(Value(1)),
                            {ConstantValueExp(Value(1))})
                ->Evaluate(&row, schema, &row, schema),
            Value(true));
}

TEST(ExpressionTest, InExpression_Inspect_ReturnsStringAndResultType) {
  Row row;
  Schema schema;
  Expression in = InExpressionExp(
      ColumnValueExp("x"), {ConstantValueExp(Value(1)),
                            ConstantValueExp(Value(2))});

  EXPECT_EQ(in->Type(), TypeTag::kInExp);
  EXPECT_EQ(in->ToString(), "x IN (1, 2)");
  EXPECT_EQ(in->ResultType(schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(in->ResultType(schema, schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(in->TouchedColumns().size(), 1);

  std::ostringstream oss;
  in->Dump(oss);
  EXPECT_EQ(oss.str(), "x IN (1, 2)");
}

TEST(ExpressionTest, EvaluateBinary_WithBooleanLogic_FollowsThreeValuedLogic) {
  const Value vtrue(true);
  const Value vfalse(false);
  const Value vnull;

  EXPECT_EQ(EvaluateBinary(BinaryOperation::kAnd, vtrue, vtrue), Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kAnd, vfalse, vnull),
            Value(false));
  EXPECT_TRUE(EvaluateBinary(BinaryOperation::kAnd, vnull, vtrue).IsNull());
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kOr, vnull, vtrue), Value(true));
  EXPECT_TRUE(EvaluateBinary(BinaryOperation::kOr, vnull, vfalse).IsNull());
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kOr, vfalse, vfalse),
            Value(false));
  EXPECT_TRUE(EvaluateBinary(BinaryOperation::kXor, vnull, vtrue).IsNull());
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kXor, vtrue, vfalse), Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kXor, vtrue, vtrue), Value(false));
}

TEST(ExpressionTest, EvaluateBinary_WithLikePattern_MatchesCorrectly) {
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kLike, Value("hello"), Value("h%")),
            Value(true));
  EXPECT_EQ(
      EvaluateBinary(BinaryOperation::kLike, Value("hello"), Value("h_llo")),
      Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kLike, Value("hello"), Value("h%o")),
            Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kLike, Value("hello"), Value("H%")),
            Value(false));
  EXPECT_EQ(
      EvaluateBinary(BinaryOperation::kNotLike, Value("hello"), Value("z%")),
      Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kLike, Value("abc"), Value("abc")),
            Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kLike, Value("abc"), Value("%")),
            Value(true));
  EXPECT_TRUE(EvaluateBinary(BinaryOperation::kLike, Value("a"), Value("%b"))
                  .Truthy() == false);
}

TEST(ExpressionTest, EvaluateBinary_WithInvalidOperands_ThrowsOrReturnsNull) {
  EXPECT_THROW(std::ignore = EvaluateBinary(BinaryOperation::kLike, Value(1),
                                            Value("a%")),
               std::runtime_error);
  EXPECT_THROW(std::ignore = EvaluateBinary(BinaryOperation::kLike, Value("a"),
                                            Value(1)),
               std::runtime_error);
  EXPECT_THROW(std::ignore = EvaluateBinary(BinaryOperation::kAdd, Value(1),
                                            Value("a")),
               std::runtime_error);
  EXPECT_TRUE(EvaluateBinary(BinaryOperation::kAdd, Value(1), Value())
                  .IsNull());
}

TEST(ExpressionTest, EvaluateBinary_WithMixedNumeric_ComputesCorrectly) {
  const Value int_val(10);
  const Value double_val(2.5);

  EXPECT_DOUBLE_EQ(
      EvaluateBinary(BinaryOperation::kAdd, int_val, double_val)
          .value.double_value,
      12.5);
  EXPECT_DOUBLE_EQ(
      EvaluateBinary(BinaryOperation::kSubtract, int_val, double_val)
          .value.double_value,
      7.5);
  EXPECT_DOUBLE_EQ(
      EvaluateBinary(BinaryOperation::kMultiply, double_val, int_val)
          .value.double_value,
      25.0);
  EXPECT_DOUBLE_EQ(
      EvaluateBinary(BinaryOperation::kDivide, int_val, double_val)
          .value.double_value,
      4.0);
  EXPECT_DOUBLE_EQ(
      EvaluateBinary(BinaryOperation::kModulo, int_val, double_val)
          .value.double_value,
      0.0);
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kEquals, int_val, double_val),
            Value(false));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kNotEquals, int_val, double_val),
            Value(true));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kLessThan, int_val, double_val),
            Value(false));
  EXPECT_EQ(
      EvaluateBinary(BinaryOperation::kLessThanEquals, int_val, double_val),
      Value(false));
  EXPECT_EQ(EvaluateBinary(BinaryOperation::kGreaterThan, int_val, double_val),
            Value(true));
  EXPECT_EQ(
      EvaluateBinary(BinaryOperation::kGreaterThanEquals, int_val, double_val),
      Value(true));
}

TEST(ExpressionTest, BinaryExpression_ResultType_InfersCorrectType) {
  Schema int_schema("s", {Column("a", ValueType::kInt64),
                          Column("b", ValueType::kInt64)});
  Schema double_schema("s", {Column("a", ValueType::kDouble),
                             Column("b", ValueType::kInt64)});
  Schema varchar_schema("s", {Column("a", ValueType::kVarChar),
                              Column("b", ValueType::kVarChar)});
  Expression add = BinaryExpressionExp(ColumnValueExp("a"),
                                       BinaryOperation::kAdd,
                                       ColumnValueExp("b"));

  EXPECT_EQ(add->ResultType(int_schema).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(add->ResultType(double_schema).GetType(), TypeTag::kDouble);
  EXPECT_EQ(BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd,
                                ColumnValueExp("b"))
                ->ResultType(varchar_schema)
                .GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
                                ColumnValueExp("b"))
                ->ResultType(int_schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kLike,
                                ColumnValueExp("b"))
                ->ResultType(varchar_schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAnd,
                                ColumnValueExp("b"))
                ->ResultType(int_schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(add->ResultType(int_schema, int_schema).GetType(),
            TypeTag::kBigInt);
}

TEST(ExpressionTest, BinaryExpression_EvaluateWithTwoSchemas_ResolvesBothSchemas) {
  Schema left_schema("l", {Column("a", ValueType::kInt64)});
  Schema right_schema("r", {Column("b", ValueType::kInt64)});
  Row left_row({Value(1)});
  Row right_row({Value(2)});
  Expression expr = BinaryExpressionExp(ColumnValueExp("a"),
                                        BinaryOperation::kAdd,
                                        ColumnValueExp("b"));

  EXPECT_EQ(expr->Evaluate(&left_row, left_schema, &right_row, right_schema),
            Value(3));
  EXPECT_EQ(expr->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(expr->TouchedColumns().size(), 2);
  EXPECT_EQ(expr->ToString(), "(a + b)");

  std::ostringstream oss;
  expr->Dump(oss);
  EXPECT_EQ(oss.str(), "(a + b)");
}

TEST(ExpressionTest, FunctionCall_CoalesceAndConcat_EvaluatesCorrectly) {
  Row row;
  Schema schema;
  Expression coalesce = FunctionCallExp(
      "coalesce", {ConstantValueExp(Value()), ConstantValueExp(Value("b")),
                   ConstantValueExp(Value(3))});
  Expression all_null = FunctionCallExp(
      "coalesce", {ConstantValueExp(Value()), ConstantValueExp(Value())});
  Expression concat = FunctionCallExp(
      "concat", {ConstantValueExp(Value("a")), ConstantValueExp(Value("b")),
                 ConstantValueExp(Value("c"))});
  Expression concat_null = FunctionCallExp(
      "concat", {ConstantValueExp(Value("a")), ConstantValueExp(Value())});

  EXPECT_EQ(coalesce->Evaluate(row, schema), Value("b"));
  EXPECT_TRUE(all_null->Evaluate(row, schema).IsNull());
  EXPECT_EQ(concat->Evaluate(row, schema), Value("abc"));
  EXPECT_TRUE(concat_null->Evaluate(row, schema).IsNull());
  EXPECT_THROW(FunctionCallExp("concat", {ConstantValueExp(Value(1)),
                                          ConstantValueExp(Value("a"))})
                   ->Evaluate(row, schema),
               std::runtime_error);
}

TEST(ExpressionTest, FunctionCall_Substr_EvaluatesCorrectly) {
  Row row;
  Schema schema;
  Expression substr2 = FunctionCallExp(
      "substr", {ConstantValueExp(Value("hello")), ConstantValueExp(Value(2))});
  Expression substring3 = FunctionCallExp(
      "substring",
      {ConstantValueExp(Value("hello")), ConstantValueExp(Value(2)),
       ConstantValueExp(Value(3))});
  Expression substr_start_one = FunctionCallExp(
      "substr", {ConstantValueExp(Value("hello")), ConstantValueExp(Value(1)),
                 ConstantValueExp(Value(2))});
  Expression substr_out_of_range = FunctionCallExp(
      "substr",
      {ConstantValueExp(Value("hello")), ConstantValueExp(Value(99))});
  Expression substr_null = FunctionCallExp(
      "substr",
      {ConstantValueExp(Value("hello")), ConstantValueExp(Value())});

  EXPECT_EQ(substr2->Evaluate(row, schema), Value("ello"));
  EXPECT_EQ(substring3->Evaluate(row, schema), Value("ell"));
  EXPECT_EQ(substr_start_one->Evaluate(row, schema), Value("he"));
  EXPECT_EQ(substr_out_of_range->Evaluate(row, schema), Value(std::string()));
  EXPECT_TRUE(substr_null->Evaluate(row, schema).IsNull());
  EXPECT_THROW(FunctionCallExp("substr", {ConstantValueExp(Value("hello"))})
                   ->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_THROW(
      FunctionCallExp("substr", {ConstantValueExp(Value("hello")),
                                 ConstantValueExp(Value("x"))})
          ->Evaluate(row, schema),
      std::runtime_error);

  Expression substr_negative_length = FunctionCallExp(
      "substr", {ConstantValueExp(Value("abc")), ConstantValueExp(Value(1)),
                 ConstantValueExp(Value(-1))});
  EXPECT_THROW(substr_negative_length->Evaluate(row, schema),
               std::runtime_error);
  Expression substr_zero_length = FunctionCallExp(
      "substr", {ConstantValueExp(Value("abc")), ConstantValueExp(Value(2)),
                 ConstantValueExp(Value(0))});
  EXPECT_EQ(substr_zero_length->Evaluate(row, schema), Value(std::string()));

  Expression substr_start_zero = FunctionCallExp(
      "substr", {ConstantValueExp(Value("abc")), ConstantValueExp(Value(0))});
  EXPECT_EQ(substr_start_zero->Evaluate(row, schema), Value("abc"));
  Expression substr_start_negative =
      FunctionCallExp("substr", {ConstantValueExp(Value("abc")),
                                 ConstantValueExp(Value(-5))});
  EXPECT_EQ(substr_start_negative->Evaluate(row, schema), Value("abc"));
  EXPECT_EQ(substr2->ToString(), "substr(\"hello\", 2)");

  std::ostringstream oss;
  substr2->Dump(oss);
  EXPECT_EQ(oss.str(), "substr(\"hello\", 2)");
  EXPECT_EQ(substr2->AsFunctionCallExpression().Args().size(), 2);
}

TEST(ExpressionTest, FunctionCall_Extract_EvaluatesCorrectly) {
  Row row;
  Schema schema;
  Expression year = FunctionCallExp(
      "extract_year", {ConstantValueExp(Value::Date("1996-07-15"))});
  Expression month = FunctionCallExp(
      "extract_month", {ConstantValueExp(Value::Date("1996-07-15"))});
  Expression day = FunctionCallExp(
      "extract_day", {ConstantValueExp(Value("1996-07-15"))});
  Expression null_extract = FunctionCallExp(
      "extract_year", {ConstantValueExp(Value())});

  EXPECT_EQ(year->Evaluate(row, schema), Value(1996));
  EXPECT_EQ(month->Evaluate(row, schema), Value(7));
  EXPECT_EQ(day->Evaluate(row, schema), Value(15));
  EXPECT_TRUE(null_extract->Evaluate(row, schema).IsNull());
  EXPECT_THROW(FunctionCallExp("extract_year", {})->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_THROW(FunctionCallExp("extract_year",
                               {ConstantValueExp(Value("bad"))})
                   ->Evaluate(row, schema),
               std::runtime_error);
}

TEST(ExpressionTest, FunctionCall_CurrentTimestampAndResultType_EvaluatesAndInfersType) {
  Row row;
  Schema schema;

  EXPECT_EQ(FunctionCallExp("current_timestamp", {})
                ->Evaluate(row, schema)
                .type,
            ValueType::kVarChar);
  EXPECT_THROW(FunctionCallExp("current_timestamp",
                               {ConstantValueExp(Value(1))})
                   ->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_THROW(FunctionCallExp("unknown_func", {})->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_EQ(FunctionCallExp("unknown_func", {})->ToString(), "unknown_func()");
  EXPECT_EQ(FunctionCallExp("COALESCE",
                            {ConstantValueExp(Value(1)),
                             ConstantValueExp(Value(2))})
                ->AsFunctionCallExpression()
                .FuncName(),
            "coalesce");
  EXPECT_EQ(FunctionCallExp("coalesce", {ConstantValueExp(Value(1))})
                ->ResultType(schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(FunctionCallExp("concat", {})->ResultType(schema).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("substr", {})->ResultType(schema).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("current_timestamp", {})
                ->ResultType(schema)
                .GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("extract_year", {})
                ->ResultType(schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(FunctionCallExp("unknown_func", {})->ResultType(schema).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("coalesce", {ColumnValueExp("x"),
                                         ColumnValueExp("y")})
                ->TouchedColumns()
                .size(),
            2);
}

TEST(ExpressionTest, FunctionCall_DateAddSub_EvaluatesCorrectly) {
  Row row;
  Schema schema;
  Expression date_add = FunctionCallExp(
      "date_add", {ConstantValueExp(Value::Date("1994-01-01")),
                   IntervalExpressionExp(1, "year")});
  Expression date_sub = FunctionCallExp(
      "date_sub", {ConstantValueExp(Value("1998-12-01")),
                   IntervalExpressionExp(74, "day")});
  Expression date_add_4 = FunctionCallExp(
      "date_add", {ConstantValueExp(Value("1994-01-01")),
                   IntervalExpressionExp(1, "year")});
  Expression date_add_null = FunctionCallExp(
      "date_add", {ConstantValueExp(Value()), IntervalExpressionExp(1, "day")});

  EXPECT_EQ(date_add->Evaluate(row, schema), Value::Date("1995-01-01"));
  EXPECT_EQ(date_sub->Evaluate(row, schema), Value("1998-09-18"));
  EXPECT_EQ(date_add_4->Evaluate(&row, schema, &row, schema),
            Value("1995-01-01"));
  EXPECT_TRUE(date_add_null->Evaluate(row, schema).IsNull());
  EXPECT_THROW(
      FunctionCallExp("date_add", {ConstantValueExp(Value("1994-01-01"))})
          ->Evaluate(row, schema),
      std::runtime_error);
  EXPECT_THROW(FunctionCallExp("date_add",
                               {ConstantValueExp(Value("1994-01-01")),
                                ConstantValueExp(Value(1))})
                   ->Evaluate(row, schema),
               std::runtime_error);
}

TEST(ExpressionTest, ColumnValue_CaseInsensitiveLookup_ResolvesColumn) {
  std::vector<Column> cols{Column("Name", ValueType::kVarChar),
                           Column("Score", ValueType::kInt64)};
  Schema sc("sc", cols);
  Row row({Value("foo"), Value(12)});

  EXPECT_EQ(ColumnValueExp("name")->Evaluate(row, sc), Value("foo"));
  EXPECT_EQ(ColumnValueExp("SCORE")->Evaluate(row, sc), Value(12));
  EXPECT_EQ(ColumnValueExp("sc.NAME")->Evaluate(row, sc), Value("foo"));
  EXPECT_THROW(ColumnValueExp("missing")->Evaluate(row, sc),
               std::runtime_error);
}

TEST(ExpressionTest, ColumnValue_ResultType_InfersCorrectType) {
  std::vector<Column> cols{
      Column("i", ValueType::kInt64), Column("d", ValueType::kDouble),
      Column("s", ValueType::kVarChar), Column("dt", ValueType::kDate),
      Column("n", ValueType::kNull)};
  Schema sc("sc", cols);

  EXPECT_EQ(ColumnValueExp("i")->ResultType(sc).GetType(), TypeTag::kBigInt);
  EXPECT_EQ(ColumnValueExp("d")->ResultType(sc).GetType(), TypeTag::kDouble);
  EXPECT_EQ(ColumnValueExp("s")->ResultType(sc).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(ColumnValueExp("dt")->ResultType(sc).GetType(), TypeTag::kDate);
  EXPECT_EQ(ColumnValueExp("n")->ResultType(sc).GetType(), TypeTag::kInvalid);
  EXPECT_THROW(ColumnValueExp("zzz")->ResultType(sc), std::runtime_error);
}

TEST(ExpressionTest, ColumnValue_EvaluateWithTwoSchemas_ResolvesCorrectSchema) {
  Schema left_schema("l", {Column("a", ValueType::kInt64)});
  Schema right_schema("r", {Column("b", ValueType::kVarChar)});
  Row left_row({Value(1)});
  Row right_row({Value("two")});

  EXPECT_EQ(ColumnValueExp("a")
                ->Evaluate(&left_row, left_schema, &right_row, right_schema),
            Value(1));
  EXPECT_EQ(ColumnValueExp("b")
                ->Evaluate(&left_row, left_schema, &right_row, right_schema),
            Value("two"));
  EXPECT_THROW(ColumnValueExp("zzz")
                   ->Evaluate(&left_row, left_schema, &right_row, right_schema),
               std::runtime_error);
  EXPECT_EQ(ColumnValueExp("b")->ResultType(left_schema, right_schema).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(ColumnValueExp("a")->ResultType(left_schema, right_schema).GetType(),
            TypeTag::kBigInt);
  EXPECT_THROW(ColumnValueExp("zzz")->ResultType(left_schema, right_schema),
               std::runtime_error);
}

TEST(ExpressionTest, ColumnValue_Accessors_ReturnExpectedProperties) {
  Expression column = ColumnValueExp("col");

  EXPECT_EQ(column->Type(), TypeTag::kColumnValue);
  EXPECT_EQ(column->TouchedColumns().size(), 1);

  auto& cv = column->AsColumnValue();
  cv.SetSchemaName("schema");
  EXPECT_EQ(cv.GetName(), "col");
  EXPECT_EQ(cv.ToString(), "schema.col");
  EXPECT_EQ(cv.GetColumnName().name, "col");

  std::ostringstream oss;
  column->Dump(oss);
  EXPECT_EQ(oss.str(), "schema.col");
}

TEST(ExpressionTest, UnaryExpression_WithInvalidOrNullOperands_HandlesCorrectly) {
  Row row;
  Schema schema;

  EXPECT_TRUE(UnaryExpressionExp(ConstantValueExp(Value()),
                                 UnaryOperation::kMinus)
                  ->Evaluate(row, schema)
                  .IsNull());
  EXPECT_THROW(UnaryExpressionExp(ConstantValueExp(Value("x")),
                                  UnaryOperation::kMinus)
                   ->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_EQ(EvaluateUnary(UnaryOperation::kMinus, Value(5)), Value(-5));
  EXPECT_DOUBLE_EQ(
      EvaluateUnary(UnaryOperation::kMinus, Value(2.5)).value.double_value,
      -2.5);
  EXPECT_TRUE(EvaluateUnary(UnaryOperation::kMinus, Value()).IsNull());
  EXPECT_TRUE(EvaluateUnary(UnaryOperation::kNot, Value()).IsNull());
  EXPECT_EQ(EvaluateUnary(UnaryOperation::kIsNull, Value(1)), Value(false));
  EXPECT_EQ(EvaluateUnary(UnaryOperation::kIsNotNull, Value()),
            Value(false));
  EXPECT_EQ(EvaluateUnary(UnaryOperation::kNot, Value(true)), Value(false));
}

TEST(ExpressionTest, UnaryExpression_ResultType_InfersCorrectType) {
  Schema schema("s", {Column("v", ValueType::kInt64),
                      Column("d", ValueType::kDouble)});

  EXPECT_EQ(UnaryExpressionExp(ColumnValueExp("v"), UnaryOperation::kIsNull)
                ->ResultType(schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(UnaryExpressionExp(ColumnValueExp("v"),
                               UnaryOperation::kIsNotNull)
                ->ResultType(schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(UnaryExpressionExp(ColumnValueExp("v"), UnaryOperation::kNot)
                ->ResultType(schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(UnaryExpressionExp(ColumnValueExp("v"), UnaryOperation::kMinus)
                ->ResultType(schema)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(UnaryExpressionExp(ColumnValueExp("d"), UnaryOperation::kMinus)
                ->ResultType(schema, schema)
                .GetType(),
            TypeTag::kDouble);
}

TEST(ExpressionTest, UnaryExpression_Inspect_ReturnsStringAndDump) {
  Row row;
  Schema schema;
  Expression minus = UnaryExpressionExp(ColumnValueExp("x"),
                                        UnaryOperation::kMinus);
  Expression not_exp = UnaryExpressionExp(ColumnValueExp("x"),
                                          UnaryOperation::kNot);

  EXPECT_EQ(minus->ToString(), "(-x)");
  EXPECT_EQ(not_exp->ToString(), "(NOT x)");

  std::ostringstream oss;
  minus->Dump(oss);
  EXPECT_EQ(oss.str(), "(-x)");

  std::ostringstream oss2;
  not_exp->Dump(oss2);
  EXPECT_EQ(oss2.str(), "(NOT x)");

  EXPECT_EQ(minus->Type(), TypeTag::kUnaryExp);
  EXPECT_EQ(minus->TouchedColumns().size(), 1);
  EXPECT_EQ(minus->AsUnaryExpression().Op(), UnaryOperation::kMinus);
  EXPECT_EQ(minus->AsUnaryExpression().Child()->Type(),
            TypeTag::kColumnValue);

  Schema schema2("s", {Column("x", ValueType::kInt64)});
  Row row2({Value(7)});
  EXPECT_EQ(UnaryExpressionExp(ColumnValueExp("x"), UnaryOperation::kMinus)
                ->Evaluate(&row2, schema2, nullptr, schema2),
            Value(-7));
}

TEST(ExpressionTest, ExpressionBase_DynamicCasts_CastSuccessfully) {
  Expression column = ColumnValueExp("c");
  EXPECT_NO_THROW((void)column->AsColumnValue());

  Expression binary = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                          BinaryOperation::kAdd,
                                          ConstantValueExp(Value(2)));
  EXPECT_NO_THROW((void)binary->AsBinaryExpression());

  Expression constant = ConstantValueExp(Value(1));
  EXPECT_NO_THROW((void)constant->AsConstantValue());

  Expression unary = UnaryExpressionExp(constant, UnaryOperation::kNot);
  EXPECT_NO_THROW((void)unary->AsUnaryExpression());

  Expression aggregate =
      AggregateExpressionExp(AggregationType::kCount, constant);
  EXPECT_NO_THROW((void)aggregate->AsAggregateExpression());

  Expression case_exp = CaseExpressionExp(
      {{ConstantValueExp(Value(true)), constant}}, constant);
  EXPECT_NO_THROW((void)case_exp->AsCaseExpression());

  Expression in = InExpressionExp(constant, {constant});
  EXPECT_NO_THROW((void)in->AsInExpression());

  Expression func = FunctionCallExp("coalesce", {constant});
  EXPECT_NO_THROW((void)func->AsFunctionCallExpression());

  Expression interval = IntervalExpressionExp(1, "day");
  EXPECT_NO_THROW((void)interval->AsIntervalExpression());

  Expression query = QueryExpressionExp(
      std::shared_ptr<SelectStatement>(), constant);
  EXPECT_NO_THROW((void)query->AsQueryExpression());
}

TEST(ExpressionTest, ExpressionBase_DefaultImplementations_ThrowRuntimeError) {
  BareExpression bare;
  Row row;
  Schema schema;

  EXPECT_THROW((void)bare.Evaluate(&row, schema, &row, schema),
               std::runtime_error);
  EXPECT_THROW((void)bare.ResultType(schema), std::runtime_error);
  EXPECT_THROW((void)bare.ResultType(schema, schema), std::runtime_error);
  EXPECT_EQ(bare.TouchedColumns().size(), 0);
}

TEST(ExpressionTest, NamedExpression_OutputOperator_StreamsExpectedFormat) {
  std::ostringstream with_alias;
  with_alias << NamedExpression("alias", ConstantValueExp(Value(1)));
  EXPECT_EQ(with_alias.str(), "1 AS alias");

  std::ostringstream no_alias;
  no_alias << NamedExpression(std::string_view(""), ConstantValueExp(Value(1)));
  EXPECT_EQ(no_alias.str(), "1");

  std::ostringstream same_as_expr;
  same_as_expr << NamedExpression("1", ConstantValueExp(Value(1)));
  EXPECT_EQ(same_as_expr.str(), "1");
}

TEST(ExpressionTest, FunctionCall_EvaluateWithTwoRows_ResolvesBothRows) {
  Schema left("", {Column("x", ValueType::kVarChar),
                   Column("b", ValueType::kInt64)});
  Row left_row({Value("hello"), Value(2)});
  Schema right("", {Column("d", ValueType::kVarChar)});
  Row right_row({Value("1996-07-15")});

  EXPECT_EQ(FunctionCallExp("coalesce",
                            {ColumnValueExp("d"), ColumnValueExp("x")})
                ->Evaluate(&left_row, left, &right_row, right),
            Value("1996-07-15"));
  EXPECT_EQ(FunctionCallExp("extract_day", {ColumnValueExp("d")})
                ->Evaluate(&left_row, left, &right_row, right),
            Value(15));
  EXPECT_EQ(FunctionCallExp("substr",
                            {ColumnValueExp("x"), ColumnValueExp("b")})
                ->Evaluate(&left_row, left, &right_row, right),
            Value("ello"));
  EXPECT_EQ(FunctionCallExp("concat",
                            {ColumnValueExp("x"), ColumnValueExp("x")})
                ->Evaluate(&left_row, left, &right_row, right),
            Value("hellohello"));
  EXPECT_EQ(FunctionCallExp("current_timestamp", {})
                ->Evaluate(&left_row, left, &right_row, right)
                .type,
            ValueType::kVarChar);
}

TEST(ExpressionTest, FunctionCall_ResultTypeWithTwoSchemas_InfersCorrectType) {
  Schema left("", {Column("i", ValueType::kInt64),
                   Column("v", ValueType::kVarChar)});
  Schema right("", {Column("v", ValueType::kVarChar)});

  EXPECT_EQ(FunctionCallExp("coalesce",
                            {ColumnValueExp("i"), ColumnValueExp("v")})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(FunctionCallExp("concat", {})->ResultType(left, right).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("substr", {})->ResultType(left, right).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("substring", {})->ResultType(left, right).GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("current_timestamp", {})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("date_add", {ColumnValueExp("v"),
                                         IntervalExpressionExp(1, "day")})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kVarChar);
  EXPECT_EQ(FunctionCallExp("date_sub", {ColumnValueExp("i"),
                                         IntervalExpressionExp(1, "day")})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(FunctionCallExp("extract_month", {ColumnValueExp("v")})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kBigInt);
  EXPECT_EQ(FunctionCallExp("unknown_func", {})->ResultType(left, right).GetType(),
            TypeTag::kVarChar);
}

TEST(ExpressionTest, FunctionCall_WithNestedFunctions_TouchesAllColumnsAndRendersString) {
  Row row;
  Schema schema;

  Expression nested = FunctionCallExp(
      "concat",
      {FunctionCallExp("substr", {ConstantValueExp(Value("hello")),
                                  ConstantValueExp(Value(2))}),
       ConstantValueExp(Value("!"))});

  EXPECT_EQ(nested->Evaluate(row, schema), Value("ello!"));
  EXPECT_EQ(nested->ToString(), "concat(substr(\"hello\", 2), \"!\")");

  std::ostringstream oss;
  nested->Dump(oss);
  EXPECT_EQ(oss.str(), "concat(substr(\"hello\", 2), \"!\")");

  Expression substring3 = FunctionCallExp(
      "substring", {ConstantValueExp(Value("hello")),
                    ConstantValueExp(Value(2)), ConstantValueExp(Value(3))});
  EXPECT_EQ(substring3->ToString(), "substring(\"hello\", 2, 3)");

  Expression columns = FunctionCallExp(
      "coalesce",
      {ColumnValueExp("a"),
       FunctionCallExp("substr", {ColumnValueExp("b"),
                                  ConstantValueExp(Value(1))})});

  EXPECT_EQ(columns->TouchedColumns().size(), 2);
  EXPECT_EQ(columns->Type(), TypeTag::kFunctionCallExp);
}

TEST(ExpressionTest, FunctionCall_DateAddSubWithInvalidArgs_ThrowsRuntimeError) {
  Row row;
  Schema schema;

  EXPECT_THROW(FunctionCallExp("date_add", {ConstantValueExp(Value("1994-01-01")),
                                            ConstantValueExp(Value(1))})
                   ->Evaluate(&row, schema, &row, schema),
               std::runtime_error);
  EXPECT_THROW(FunctionCallExp("date_add", {ConstantValueExp(Value("1994-01-01"))})
                   ->Evaluate(&row, schema, &row, schema),
               std::runtime_error);
  EXPECT_THROW(FunctionCallExp("date_sub", {ConstantValueExp(Value("1998-12-01")),
                                            ConstantValueExp(Value(1))})
                   ->Evaluate(&row, schema, &row, schema),
               std::runtime_error);
}

class FakeEvaluationContext : public EvaluationContext {
 public:
  FakeEvaluationContext() = default;
  explicit FakeEvaluationContext(std::vector<std::vector<Value>> results)
      : results_(std::move(results)) {}

  StatusOr<std::vector<Value>> RunSubquery(const SelectStatement&,
                                           const Row*) override {
    ++subquery_calls_;
    if (next_result_ < results_.size()) {
      return StatusOr<std::vector<Value>>(results_[next_result_++]);
    }
    return StatusOr<std::vector<Value>>(std::vector<Value>{});
  }
  const AggregateResultMap* CurrentAggregates() const override {
    return nullptr;
  }
  Status GetOrAddFunction(std::string_view name, int argument_count) override {
    registered.emplace_back(std::string(name), argument_count);
    return registration_status;
  }

  std::vector<std::pair<std::string, int>> registered;
  int subquery_calls_{0};
  Status registration_status = Status::kSuccess;

 private:
  std::vector<std::vector<Value>> results_;
  size_t next_result_{0};
};

TEST(ExpressionTest, FunctionCall_Validate_RegistersThroughEvaluationContext) {
  Schema schema("s", {Column("a", ValueType::kVarChar)});
  Expression concat =
      FunctionCallExp("concat", {ColumnValueExp("a"), ConstantValueExp(Value("x"))});

  FakeEvaluationContext ctx;
  EXPECT_EQ(concat->Validate(ctx, schema), Status::kSuccess);
  EXPECT_EQ(concat->Validate(ctx, schema), Status::kSuccess);
  ASSERT_EQ(ctx.registered.size(), 2U);
  EXPECT_EQ(ctx.registered[0], std::make_pair(std::string("concat"), 2));
  EXPECT_EQ(ctx.registered[1], std::make_pair(std::string("concat"), 2));

  FakeEvaluationContext failing_ctx;
  failing_ctx.registration_status = Status::kNotExists;
  Expression custom = FunctionCallExp("my_udf", {ConstantValueExp(Value(1))});
  EXPECT_EQ(custom->Validate(failing_ctx, schema), Status::kNotExists);
  EXPECT_EQ(failing_ctx.registered.size(), 1U);

  FakeEvaluationContext arg_ctx;
  arg_ctx.registration_status = Status::kSuccess;
  Expression nested = FunctionCallExp(
      "concat", {FunctionCallExp("inner", {}), ConstantValueExp(Value("y"))});
  EXPECT_EQ(nested->Validate(arg_ctx, schema), Status::kSuccess);
  ASSERT_EQ(arg_ctx.registered.size(), 2U);
  EXPECT_EQ(arg_ctx.registered[0], std::make_pair(std::string("inner"), 0));
  EXPECT_EQ(arg_ctx.registered[1], std::make_pair(std::string("concat"), 2));
}

TEST(ExpressionTest, QueryExpression_EvaluateWithContext_ExecutesSubquery) {
  Schema schema("s", {Column("i", ValueType::kInt64)});
  Row row({Value(int64_t{7})});
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("v")}, std::vector<std::string>{"t"},
      nullptr);

  FakeEvaluationContext empty_ctx;
  const Expression exists_empty =
      QueryExpressionExp(statement, nullptr, true, false);
  EXPECT_EQ(exists_empty->Evaluate(row, schema, empty_ctx), Value(false));
  const Expression not_exists_empty =
      QueryExpressionExp(statement, nullptr, true, true);
  EXPECT_EQ(not_exists_empty->Evaluate(row, schema, empty_ctx), Value(true));
  EXPECT_EQ(empty_ctx.subquery_calls_, 2);

  FakeEvaluationContext filled_ctx({{Value(int64_t{7}), Value(int64_t{9})},
                                    {Value(int64_t{7}), Value(int64_t{9})}});
  const Expression exists =
      QueryExpressionExp(statement, nullptr, true, false);
  EXPECT_EQ(exists->Evaluate(row, schema, filled_ctx), Value(true));
  const Expression not_exists =
      QueryExpressionExp(statement, nullptr, true, true);
  EXPECT_EQ(not_exists->Evaluate(row, schema, filled_ctx), Value(false));

  FakeEvaluationContext scalar_ctx({{Value(int64_t{42})}});
  const Expression scalar =
      QueryExpressionExp(statement, nullptr, false, false);
  EXPECT_EQ(scalar->Evaluate(row, schema, scalar_ctx), Value(int64_t{42}));
  EXPECT_EQ(scalar->Evaluate(row, schema, empty_ctx), Value());

  const Expression in_subquery = QueryExpressionExp(
      statement, ConstantValueExp(Value(int64_t{7})), false, false);
  FakeEvaluationContext hit_ctx({{Value(int64_t{7}), Value()}});
  EXPECT_EQ(in_subquery->Evaluate(row, schema, hit_ctx), Value(true));
  const Expression miss_subquery = QueryExpressionExp(
      statement, ConstantValueExp(Value(int64_t{8})), false, false);
  FakeEvaluationContext null_list_ctx({{Value(int64_t{1}), Value()},
                                       {Value(int64_t{1}), Value()}});
  EXPECT_EQ(miss_subquery->Evaluate(row, schema, null_list_ctx), Value());
  FakeEvaluationContext plain_miss_ctx({{Value(int64_t{1})},
                                        {Value(int64_t{1})}});
  EXPECT_EQ(miss_subquery->Evaluate(row, schema, plain_miss_ctx),
            Value(false));
  const Expression null_test_subquery = QueryExpressionExp(
      statement, ConstantValueExp(Value()), false, false);
  FakeEvaluationContext fresh_ctx({{Value(int64_t{1})}});
  EXPECT_EQ(null_test_subquery->Evaluate(row, schema, fresh_ctx), Value());

  const Expression not_in_subquery = QueryExpressionExp(
      statement, ConstantValueExp(Value(int64_t{8})), false, true);
  EXPECT_EQ(not_in_subquery->Evaluate(row, schema, null_list_ctx), Value());
  EXPECT_EQ(not_in_subquery->Evaluate(row, schema, plain_miss_ctx),
            Value(true));
}

TEST(ExpressionTest, Expression_EvaluateWithContext_MatchesPlainEvaluator) {
  Schema schema("s", {Column("i", ValueType::kInt64)});
  Row row({Value(int64_t{5})});
  FakeEvaluationContext ctx;
  const Expression binary =
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(int64_t{3})));
  const Expression negated = UnaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{3}))),
      UnaryOperation::kNot);
  const Expression case_expr = CaseExpressionExp(
      {{BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                            ConstantValueExp(Value(int64_t{3}))),
        ConstantValueExp(Value(int64_t{10}))}},
      ConstantValueExp(Value(int64_t{20})));
  const Expression in_expr =
      InExpressionExp(ColumnValueExp("i"), {ConstantValueExp(Value(int64_t{4})),
                                            ConstantValueExp(Value(int64_t{5}))});
  const Expression call =
      FunctionCallExp("substr", {ConstantValueExp(Value("hello")),
                                 ConstantValueExp(Value(int64_t{2}))});

  EXPECT_EQ(binary->Evaluate(row, schema, ctx), binary->Evaluate(row, schema));
  EXPECT_EQ(negated->Evaluate(row, schema, ctx), negated->Evaluate(row, schema));
  EXPECT_EQ(case_expr->Evaluate(row, schema, ctx),
            case_expr->Evaluate(row, schema));
  EXPECT_EQ(in_expr->Evaluate(row, schema, ctx), in_expr->Evaluate(row, schema));
  EXPECT_EQ(call->Evaluate(row, schema, ctx), call->Evaluate(row, schema));
}

TEST(ExpressionTest, CastExpression_EvaluateAndResultType_HandlesTimestamps) {
  Row dummy({});
  Schema dummy_schema;

  Expression ts_cast = std::make_shared<CastExpression>(
      ConstantValueExp(Value("2023-06-15 10:00:00")), "TIMESTAMP");
  Value ts_val = ts_cast->Evaluate(dummy, dummy_schema);
  EXPECT_EQ(ts_val.type, ValueType::kVarChar);

  Expression ts_cast_utc = std::make_shared<CastExpression>(
      ConstantValueExp(Value("2023-06-15 10:00:00 UTC+0530")), "TIMESTAMP");
  Value ts_val_utc = ts_cast_utc->Evaluate(dummy, dummy_schema);
  EXPECT_EQ(ts_val_utc.type, ValueType::kVarChar);

  Expression ts_cast_gmt = std::make_shared<CastExpression>(
      ConstantValueExp(Value("2023-06-15 10:00:00 GMT-08")), "TIMESTAMP");
  Value ts_val_gmt = ts_cast_gmt->Evaluate(dummy, dummy_schema);
  EXPECT_EQ(ts_val_gmt.type, ValueType::kVarChar);

  FakeEvaluationContext ctx;
  EXPECT_EQ(ts_cast->Evaluate(&dummy, dummy_schema, &dummy, dummy_schema),
            ts_cast->Evaluate(dummy, dummy_schema));
  EXPECT_EQ(ts_cast->Evaluate(dummy, dummy_schema, ctx),
            ts_cast->Evaluate(dummy, dummy_schema));
  EXPECT_EQ(ts_cast->ResultType(dummy_schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(ts_cast->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kVarChar);
  EXPECT_FALSE(ts_cast->ToString().empty());
}

TEST(ExpressionTest, BinaryExpression_SpecialOperations_EvaluatesCorrectly) {
  Row dummy({});
  Schema dummy_schema;

  Expression bin_cols = BinaryExpressionExp(
      ColumnValueExp("col1"), BinaryOperation::kAdd, ColumnValueExp("col2"));
  EXPECT_EQ(bin_cols->TouchedColumns().size(), 2U);

  Expression like_nomatch = BinaryExpressionExp(
      ConstantValueExp(Value("hello")), BinaryOperation::kLike,
      ConstantValueExp(Value("world")));
  EXPECT_EQ(like_nomatch->Evaluate(dummy, dummy_schema), Value(false));

  Expression div_nan = BinaryExpressionExp(
      ConstantValueExp(Value(0.0)), BinaryOperation::kDivide,
      ConstantValueExp(Value(0)));
  EXPECT_THROW((void)div_nan->Evaluate(dummy, dummy_schema), std::runtime_error);

  Expression div_mixed = BinaryExpressionExp(
      ConstantValueExp(Value(10)), BinaryOperation::kDivide,
      ConstantValueExp(Value(2.0)));
  EXPECT_DOUBLE_EQ(div_mixed->Evaluate(dummy, dummy_schema).value.double_value, 5.0);

  Expression mult_iv = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{3})), BinaryOperation::kMultiply,
      ConstantValueExp(Value("1-0 0:0:0")));
  EXPECT_EQ(mult_iv->Evaluate(dummy, dummy_schema).type, ValueType::kVarChar);

  Expression date_cmp = BinaryExpressionExp(
      ConstantValueExp(Value("2023-01-01")), BinaryOperation::kEquals,
      ConstantValueExp(Value(Value::DateFromDays(ParseDateDays("2023-01-01")))));
  EXPECT_EQ(date_cmp->Evaluate(dummy, dummy_schema), Value(true));

  Expression div_zero_double = BinaryExpressionExp(
      ConstantValueExp(Value(1.0)), BinaryOperation::kDivide,
      ConstantValueExp(Value(0.0)));
  EXPECT_THROW((void)div_zero_double->Evaluate(dummy, dummy_schema), std::runtime_error);

  Expression div_overflow_double = BinaryExpressionExp(
      ConstantValueExp(Value(std::numeric_limits<double>::max())),
      BinaryOperation::kDivide,
      ConstantValueExp(Value(1e-300)));
  EXPECT_THROW((void)div_overflow_double->Evaluate(dummy, dummy_schema), std::runtime_error);

  Expression and_short = BinaryExpressionExp(
      ConstantValueExp(Value(false)), BinaryOperation::kAnd,
      ColumnValueExp("unresolved"));
  EXPECT_EQ(and_short->Evaluate(&dummy, dummy_schema, &dummy, dummy_schema), Value(false));

  Expression or_short = BinaryExpressionExp(
      ConstantValueExp(Value(true)), BinaryOperation::kOr,
      ColumnValueExp("unresolved"));
  EXPECT_EQ(or_short->Evaluate(&dummy, dummy_schema, &dummy, dummy_schema), Value(true));
}

TEST(ExpressionTest, ArrayExpression_EvaluateAndResultType_HandlesVariousTypes) {
  Row dummy({});
  Schema dummy_schema;

  auto arr_str = std::make_shared<ArrayExpression>(
      std::vector<Expression>{ConstantValueExp(Value("hello")),
                              ConstantValueExp(Value("world"))},
      "");
  Value v_str = arr_str->Evaluate(dummy, dummy_schema);
  EXPECT_TRUE(v_str.IsArray());

  auto arr_dbl = std::make_shared<ArrayExpression>(
      std::vector<Expression>{ConstantValueExp(Value(1.5)),
                              ConstantValueExp(Value(2.5))},
      "");
  Value v_dbl = arr_dbl->Evaluate(dummy, dummy_schema);
  EXPECT_TRUE(v_dbl.IsArray());

  auto arr_date = std::make_shared<ArrayExpression>(
      std::vector<Expression>{ConstantValueExp(Value(Value::DateFromDays(ParseDateDays("2023-01-01"))))},
      "");
  Value v_date = arr_date->Evaluate(dummy, dummy_schema);
  EXPECT_TRUE(v_date.IsArray());

  auto arr_bool = std::make_shared<ArrayExpression>(
      std::vector<Expression>{ConstantValueExp(Value(int64_t{1}))},
      "BOOL");
  Value v_bool = arr_bool->Evaluate(dummy, dummy_schema);
  EXPECT_TRUE(v_bool.IsArray());

  auto arr_flt = std::make_shared<ArrayExpression>(
      std::vector<Expression>{ConstantValueExp(Value(int64_t{2}))},
      "FLOAT64");
  Value v_flt = arr_flt->Evaluate(dummy, dummy_schema);
  EXPECT_TRUE(v_flt.IsArray());

  auto arr_date_coerce = std::make_shared<ArrayExpression>(
      std::vector<Expression>{ConstantValueExp(Value("2023-06-15"))},
      "DATE");
  Value v_date_coerce = arr_date_coerce->Evaluate(dummy, dummy_schema);
  EXPECT_TRUE(v_date_coerce.IsArray());

  FakeEvaluationContext ctx;
  EXPECT_TRUE(arr_str->Evaluate(&dummy, dummy_schema, &dummy, dummy_schema).IsArray());
  EXPECT_TRUE(arr_str->Evaluate(dummy, dummy_schema, ctx).IsArray());
  EXPECT_EQ(arr_str->ResultType(dummy_schema).GetType(), TypeTag::kArray);
  EXPECT_EQ(arr_str->ResultType(dummy_schema, dummy_schema).GetType(), TypeTag::kArray);
  EXPECT_FALSE(arr_str->ToString().empty());
}

TEST(ExpressionTest, WindowFunctionExpression_EvaluateAndInspect_ThrowsOnEvaluateAndRenders) {
  auto node = std::make_shared<WindowFunctionCallExpression>();
  node->function = "SUM";
  node->args = {ColumnValueExp("x")};
  node->inner_order_by = {{ColumnValueExp("y"), true, std::nullopt}};
  node->inner_limit = 10;
  node->partition_by = {ColumnValueExp("p")};
  node->order_by = {{ColumnValueExp("o"), false, std::optional<bool>(true)}};
  node->has_frame = true;
  node->frame_unit = WindowFrameUnit::kRows;
  node->frame_start = {WindowFrameBoundType::kUnboundedPreceding, nullptr};
  node->frame_end = {WindowFrameBoundType::kUnboundedFollowing, nullptr};

  Row dummy({});
  Schema dummy_schema;
  EXPECT_THROW((void)node->Evaluate(dummy, dummy_schema), std::runtime_error);
  EXPECT_EQ(node->TouchedColumns().size(), 4U);
  std::string s = node->ToString();
  EXPECT_NE(s.find("ORDER BY"), std::string::npos);
  EXPECT_NE(s.find("LIMIT"), std::string::npos);
  EXPECT_NE(s.find("UNBOUNDED FOLLOWING"), std::string::npos);
}

TEST(ExpressionTest, FunctionCall_VariousBuiltinFunctions_EvaluatesCorrectly) {
  Row dummy({});
  Schema dummy_schema;

  auto eval = [&](const std::string& name, const std::vector<Value>& args) {
    std::vector<Expression> exprs;
    for (const auto& a : args) {
      exprs.push_back(ConstantValueExp(a));
    }
    return FunctionCallExp(name, exprs)->Evaluate(dummy, dummy_schema);
  };

  EXPECT_EQ(eval("coalesce", {Value(), Value(42)}), Value(42));
  EXPECT_EQ(eval("coalesce", {Value(), Value()}), Value());
  EXPECT_EQ(eval("concat", {Value("foo"), Value("bar")}), Value("foobar"));
  EXPECT_EQ(eval("concat", {Value(), Value("bar")}), Value());

  EXPECT_EQ(eval("substr", {Value("hello"), Value(int64_t{2})}), Value("ello"));
  EXPECT_EQ(eval("substr", {Value("hello"), Value(int64_t{2}), Value(int64_t{3})}), Value("ell"));
  EXPECT_EQ(eval("substr", {Value("hello"), Value(int64_t{2}), Value(int64_t{0})}), Value(""));
  EXPECT_EQ(eval("substr", {Value("hello"), Value(int64_t{10})}), Value(""));

  EXPECT_EQ(eval("extract_year", {Value("2023-08-25")}), Value(int64_t{2023}));
  EXPECT_EQ(eval("extract_month", {Value("2023-08-25")}), Value(int64_t{8}));
  EXPECT_EQ(eval("extract_day", {Value("2023-08-25")}), Value(int64_t{25}));

  EXPECT_FALSE(eval("current_timestamp", {}).IsNull());
  EXPECT_FALSE(eval("current_datetime", {}).IsNull());
  EXPECT_FALSE(eval("current_datetime", {Value("UTC")}).IsNull());
  EXPECT_FALSE(eval("current_date", {}).IsNull());
  EXPECT_FALSE(eval("current_date", {Value("UTC")}).IsNull());

  EXPECT_EQ(eval("string", {Value("2023-01-01"), Value("UTC+0530")}).type, ValueType::kVarChar);
  EXPECT_EQ(eval("string", {Value(Value::DateFromDays(100))}), Value("1970-04-11"));
  EXPECT_EQ(eval("string", {Value(int64_t{123})}), Value("123"));

  EXPECT_EQ(eval("format_timestamp", {Value("%Y-%m-%d"), Value("2023-06-15 10:00:00"), Value("UTC")}),
            Value("2023-06-15"));
  EXPECT_EQ(eval("format_datetime", {Value("%Y/%m/%d"), Value("2023-06-15 10:00:00")}),
            Value("2023/06/15"));
  EXPECT_EQ(eval("format_date", {Value("%Y"), Value("2023-06-15")}),
            Value("2023"));

  EXPECT_EQ(eval("parse_timestamp", {Value("%Y-%m-%d %H:%M:%S"), Value("2023-06-15 10:00:00"), Value("UTC")}),
            Value("2023-06-15 10:00:00+00"));
}

TEST(ExpressionTest, CaseExpression_WithTwoRowsAndUnifiedType_EvaluatesAndInfersType) {
  Schema left_schema("L", {Column("a", ValueType::kInt64)});
  Schema right_schema("R", {Column("b", ValueType::kInt64)});
  Row left_row({Value(int64_t{10})});
  Row right_row({Value(int64_t{20})});

  CaseExpression case_exp(
      {std::make_pair(BinaryExpressionExp(ColumnValueExp("a"),
                                          BinaryOperation::kEquals,
                                          ConstantValueExp(Value(int64_t{10}))),
                      ColumnValueExp("b"))},
      ConstantValueExp(Value(int64_t{99})));
  EXPECT_EQ(case_exp.Evaluate(&left_row, left_schema, &right_row, right_schema),
            Value(int64_t{20}));

  CaseExpression case_mismatch(
      {std::make_pair(ConstantValueExp(Value(true)),
                      ConstantValueExp(Value(int64_t{1})))},
      ConstantValueExp(Value(std::string("str"))));
  EXPECT_EQ(case_mismatch.ResultType(left_schema).GetType(), TypeTag::kInvalid);
  EXPECT_EQ(case_mismatch.ResultType(left_schema, right_schema).GetType(), TypeTag::kInvalid);

  CaseExpression case_empty({}, nullptr);
  EXPECT_EQ(case_empty.ResultType(left_schema).GetType(), TypeTag::kInvalid);
}

TEST(ExpressionTest, InExpression_EvaluateWithTwoRows_EvaluatesCorrectly) {
  Schema left_schema("L", {Column("a", ValueType::kInt64)});
  Schema right_schema("R", {Column("b", ValueType::kInt64)});
  Row left_row({Value(int64_t{10})});
  Row right_row({Value(int64_t{20})});

  InExpression in_exp(ColumnValueExp("a"),
                      {ColumnValueExp("b"), ConstantValueExp(Value(int64_t{10}))});
  EXPECT_EQ(in_exp.Evaluate(&left_row, left_schema, &right_row, right_schema),
            Value(true));

  InExpression in_nomatch(ColumnValueExp("a"), {ColumnValueExp("b")});
  EXPECT_EQ(in_nomatch.Evaluate(&left_row, left_schema, &right_row, right_schema),
            Value(false));
}

TEST(ExpressionTest, Expression_PathologicalPatterns_EvaluatesCorrectly) {
  Row dummy({});
  Schema dummy_schema;

  Expression current = ConstantValueExp(Value(int64_t{1}));
  for (int i = 0; i < 30; ++i) {
    current = BinaryExpressionExp(std::move(current), BinaryOperation::kAdd,
                                  ConstantValueExp(Value(int64_t{1})));
  }
  EXPECT_EQ(current->Evaluate(dummy, dummy_schema), Value(int64_t{31}));

  double nan_val = std::numeric_limits<double>::quiet_NaN();
  double inf_val = std::numeric_limits<double>::infinity();
  double min_subnormal = std::numeric_limits<double>::denorm_min();

  Expression exp_nan_cmp = BinaryExpressionExp(
      ConstantValueExp(Value(nan_val)), BinaryOperation::kEquals,
      ConstantValueExp(Value(nan_val)));
  EXPECT_EQ(exp_nan_cmp->Evaluate(dummy, dummy_schema), Value(int64_t{0}));

  Expression exp_inf_arith = BinaryExpressionExp(
      ConstantValueExp(Value(inf_val)), BinaryOperation::kSubtract,
      ConstantValueExp(Value(1e300)));
  Value inf_res = exp_inf_arith->Evaluate(dummy, dummy_schema);
  EXPECT_EQ(inf_res.type, ValueType::kDouble);
  EXPECT_TRUE(std::isinf(inf_res.value.double_value));
  EXPECT_GT(inf_res.value.double_value, 0.0);

  Expression exp_inf_minus_inf = BinaryExpressionExp(
      ConstantValueExp(Value(inf_val)), BinaryOperation::kSubtract,
      ConstantValueExp(Value(inf_val)));
  Value nan_res = exp_inf_minus_inf->Evaluate(dummy, dummy_schema);
  EXPECT_EQ(nan_res.type, ValueType::kDouble);
  EXPECT_TRUE(std::isnan(nan_res.value.double_value));

  Expression exp_subnorm = BinaryExpressionExp(
      ConstantValueExp(Value(min_subnormal)), BinaryOperation::kMultiply,
      ConstantValueExp(Value(2.0)));
  EXPECT_EQ(exp_subnorm->Evaluate(dummy, dummy_schema), Value(min_subnormal * 2.0));

  int64_t int_min = std::numeric_limits<int64_t>::min();
  int64_t int_max = std::numeric_limits<int64_t>::max();

  Expression exp_int_min_plus_zero = BinaryExpressionExp(
      ConstantValueExp(Value(int_min)), BinaryOperation::kAdd,
      ConstantValueExp(Value(int64_t{0})));
  EXPECT_EQ(exp_int_min_plus_zero->Evaluate(dummy, dummy_schema), Value(int_min));

  Expression exp_int_max_minus_zero = BinaryExpressionExp(
      ConstantValueExp(Value(int_max)), BinaryOperation::kSubtract,
      ConstantValueExp(Value(int64_t{0})));
  EXPECT_EQ(exp_int_max_minus_zero->Evaluate(dummy, dummy_schema), Value(int_max));

  Expression case_safe = CaseExpressionExp(
      {{ConstantValueExp(Value(true)), ConstantValueExp(Value(int64_t{42}))}},
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{1})),
                          BinaryOperation::kDivide,
                          ConstantValueExp(Value(int64_t{0}))));
  EXPECT_EQ(case_safe->Evaluate(dummy, dummy_schema), Value(int64_t{42}));

  Expression and_safe = BinaryExpressionExp(
      ConstantValueExp(Value(false)), BinaryOperation::kAnd,
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{10})),
                          BinaryOperation::kDivide,
                          ConstantValueExp(Value(int64_t{0}))));
  EXPECT_EQ(and_safe->Evaluate(dummy, dummy_schema), Value(false));

  Expression or_safe = BinaryExpressionExp(
      ConstantValueExp(Value(true)), BinaryOperation::kOr,
      BinaryExpressionExp(ConstantValueExp(Value(int64_t{10})),
                          BinaryOperation::kDivide,
                          ConstantValueExp(Value(int64_t{0}))));
  EXPECT_EQ(or_safe->Evaluate(dummy, dummy_schema), Value(true));

  Expression mixed_eq = BinaryExpressionExp(
      ConstantValueExp(Value("123")), BinaryOperation::kEquals,
      ConstantValueExp(Value("123")));
  EXPECT_EQ(mixed_eq->Evaluate(dummy, dummy_schema), Value(int64_t{1}));

  Expression unary_chain = ConstantValueExp(Value(int64_t{7}));
  for (int i = 0; i < 10; ++i) {
    unary_chain = UnaryExpressionExp(std::move(unary_chain), UnaryOperation::kMinus);
  }
  EXPECT_EQ(unary_chain->Evaluate(dummy, dummy_schema), Value(int64_t{7}));
}

}  // namespace tinylamb
