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

#include <sstream>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "common/log_message.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(ExpressionTest, Constant) {
  // Arrange -- construct three constant value expressions of different types
  Expression cv_int = ConstantValueExp(Value(1));
  Expression cv_varchar = ConstantValueExp(Value("hello"));
  Expression cv_double = ConstantValueExp(Value(1.1));

  // Act -- stream all three to LOG (no assertion; output-only)
  LOG(INFO) << "cv_int: " << cv_int << "\ncv_varchar: " << cv_varchar
            << "\ncv_double: " << cv_double;

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(ExpressionTest, ConstantEval) {
  // Arrange -- construct three constant value expressions of different types
  Expression cv_int = ConstantValueExp(Value(1));
  Expression cv_varchar = ConstantValueExp(Value("hello"));
  Expression cv_double = ConstantValueExp(Value(1.1));
  Row dummy({});
  Schema dummy_schema;

  // Act + Assert -- evaluate each expression against empty row/schema and
  // verify
  ASSERT_EQ(cv_int->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(cv_varchar->Evaluate(dummy, dummy_schema), Value("hello"));
  ASSERT_EQ(cv_double->Evaluate(dummy, dummy_schema), Value(1.1));
}

TEST(ExpressionTest, BinaryPlus) {
  // Arrange -- construct three binary plus expressions (int, varchar, double)
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

  // Act + Assert -- evaluate each and verify arithmetic/concatenation semantics
  ASSERT_EQ(int_plus->Evaluate(dummy, dummy_schema), Value(3));
  ASSERT_EQ(varchar_plus->Evaluate(dummy, dummy_schema), Value("hello world"));
  ASSERT_DOUBLE_EQ(
      double_plus->Evaluate(dummy, dummy_schema).value.double_value,
      Value(3.3).value.double_value);
}

TEST(ExpressionTest, BinaryMinus) {
  // Arrange -- construct binary minus expressions (int, double)
  Expression int_minus = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                             BinaryOperation::kSubtract,
                                             ConstantValueExp(Value(2)));
  Expression double_minus = BinaryExpressionExp(ConstantValueExp(Value(1.1)),
                                                BinaryOperation::kSubtract,
                                                ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;

  // Act + Assert -- evaluate each and verify subtraction semantics
  ASSERT_EQ(int_minus->Evaluate(dummy, dummy_schema), Value(-1));
  ASSERT_DOUBLE_EQ(
      double_minus->Evaluate(dummy, dummy_schema).value.double_value,
      Value(-1.1).value.double_value);
}

TEST(ExpressionTest, BinaryMultiple) {
  // Arrange -- construct binary multiply expressions (int, double)
  Expression int_multiple = BinaryExpressionExp(ConstantValueExp(Value(1)),
                                                BinaryOperation::kMultiply,
                                                ConstantValueExp(Value(2)));
  Expression double_multiple = BinaryExpressionExp(
      ConstantValueExp(Value(1.1)), BinaryOperation::kMultiply,
      ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;

  // Act + Assert -- evaluate each and verify multiplication semantics
  ASSERT_EQ(int_multiple->Evaluate(dummy, dummy_schema), Value(2));
  ASSERT_DOUBLE_EQ(
      double_multiple->Evaluate(dummy, dummy_schema).value.double_value,
      Value(2.42).value.double_value);
}

TEST(ExpressionTest, BinaryDiv) {
  // Arrange -- construct binary divide expressions (int, double)
  Expression int_div =
      BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kDivide,
                          ConstantValueExp(Value(2)));
  Expression double_div = BinaryExpressionExp(ConstantValueExp(Value(8.8)),
                                              BinaryOperation::kDivide,
                                              ConstantValueExp(Value(2.2)));
  Row dummy({});
  Schema dummy_schema;

  // Act + Assert -- evaluate each and verify division semantics
  ASSERT_EQ(int_div->Evaluate(dummy, dummy_schema), Value(5));
  ASSERT_DOUBLE_EQ(double_div->Evaluate(dummy, dummy_schema).value.double_value,
                   Value(4.0).value.double_value);
}

TEST(ExpressionTest, BinaryMod) {
  // Arrange -- construct binary modulo expression (int only)
  Expression int_mod =
      BinaryExpressionExp(ConstantValueExp(Value(13)), BinaryOperation::kModulo,
                          ConstantValueExp(Value(5)));
  Row dummy({});
  Schema dummy_schema;

  // Act + Assert -- evaluate and verify modulo semantics
  ASSERT_EQ(int_mod->Evaluate(dummy, dummy_schema), Value(3));
}

TEST(ExpressionTest, Equal) {
  // Arrange -- construct six binary equals expressions across types
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

  // Act + Assert -- evaluate each and verify equals semantics (1=true, 0=false)
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(int_ne->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_ne->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_ne->Evaluate(dummy, dummy_schema), Value(0));
}

TEST(ExpressionTest, NotEqual) {
  // Arrange -- construct six binary not-equals expressions across types
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

  // Act + Assert -- evaluate each and verify not-equals semantics (0=false,
  // 1=true)
  ASSERT_EQ(int_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(int_ne->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(double_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(double_ne->Evaluate(dummy, dummy_schema), Value(1));
  ASSERT_EQ(varchar_eq->Evaluate(dummy, dummy_schema), Value(0));
  ASSERT_EQ(varchar_ne->Evaluate(dummy, dummy_schema), Value(1));
}

TEST(ExpressionTest, LessThan) {
  // Arrange -- construct nine binary less-than expressions across types/signs
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

  // Act + Assert -- evaluate each and verify less-than semantics (1=true,
  // 0=false)
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
  // Arrange -- construct nine binary less-than-or-equals expressions across
  // types/signs
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

  // Act + Assert -- evaluate each and verify <= semantics (1=true for
  // less-or-equal, 0=false for greater)
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
  // Arrange -- construct nine binary greater-than expressions across
  // types/signs
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

  // Act + Assert -- evaluate each and verify > semantics (1=true for greater,
  // 0=false for less-or-equal)
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
  // Arrange -- construct nine binary greater-than-or-equals expressions across
  // types/signs
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

  // Act + Assert -- evaluate each and verify >= semantics (1=true for
  // greater-or-equal, 0=false for less)
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
  // Arrange -- construct a schema with four columns and a matching row
  std::vector<Column> cols{
      Column("name", ValueType::kVarChar), Column("score", ValueType::kInt64),
      Column("flv", ValueType::kDouble), Column("date", ValueType::kInt64)};
  Schema sc("sc", cols);
  Row row({Value("foo"), Value(12), Value(132.3), Value(9)});

  // Act + Assert -- evaluate ColumnValueExp for each column and verify it reads
  // the row's field
  ASSERT_EQ(ColumnValueExp("sc.name")->Evaluate(row, sc), Value("foo"));
  ASSERT_EQ(ColumnValueExp("score")->Evaluate(row, sc), Value(12));
  ASSERT_EQ(ColumnValueExp("flv")->Evaluate(row, sc), Value(132.3));
  ASSERT_EQ(ColumnValueExp("date")->Evaluate(row, sc), Value(9));
}

TEST(ExpressionTest, UnaryExpression) {
  // Arrange -- empty row/schema for unary evaluation
  Row dummy({});
  Schema dummy_schema;

  // Act 1 + Assert 1 -- IS NULL on null and non-null
  Expression is_null_true =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kIsNull);
  Expression is_null_false =
      UnaryExpressionExp(ConstantValueExp(Value(1)), UnaryOperation::kIsNull);
  ASSERT_EQ(is_null_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(is_null_false->Evaluate(dummy, dummy_schema), Value(false));

  // Act 2 + Assert 2 -- IS NOT NULL on null and non-null
  Expression is_not_null_true = UnaryExpressionExp(ConstantValueExp(Value(1)),
                                                   UnaryOperation::kIsNotNull);
  Expression is_not_null_false =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kIsNotNull);
  ASSERT_EQ(is_not_null_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(is_not_null_false->Evaluate(dummy, dummy_schema), Value(false));

  // Act 3 + Assert 3 -- NOT on true, false, null
  Expression not_true =
      UnaryExpressionExp(ConstantValueExp(Value(true)), UnaryOperation::kNot);
  Expression not_false =
      UnaryExpressionExp(ConstantValueExp(Value(false)), UnaryOperation::kNot);
  Expression not_null =
      UnaryExpressionExp(ConstantValueExp(Value()), UnaryOperation::kNot);
  ASSERT_EQ(not_true->Evaluate(dummy, dummy_schema), Value(false));
  ASSERT_EQ(not_false->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_TRUE(not_null->Evaluate(dummy, dummy_schema).IsNull());

  // Act 4 + Assert 4 -- Unary Minus on int and double
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
  // Arrange -- empty row/schema; construct six aggregate expressions
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

  // Act + Assert -- aggregate expressions are not evaluated directly; executor
  // computes them. Here we only verify ToString() renders the expected
  // aggregate syntax.
  ASSERT_EQ(count_all->ToString(), "COUNT(*)");
  ASSERT_EQ(count_col->ToString(), "COUNT(col)");
  ASSERT_EQ(sum_col->ToString(), "SUM(col)");
  ASSERT_EQ(avg_col->ToString(), "AVG(col)");
  ASSERT_EQ(min_col->ToString(), "MIN(col)");
  ASSERT_EQ(max_col->ToString(), "MAX(col)");
}

TEST(ExpressionTest, CaseExpression) {
  // Arrange -- empty row/schema; construct two CASE expressions
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

  // Act + Assert -- CASE returns "one" when first WHEN matches, "other"
  // otherwise
  ASSERT_EQ(case_exp_true->Evaluate(dummy, dummy_schema), Value("one"));
  ASSERT_EQ(case_exp_false->Evaluate(dummy, dummy_schema), Value("other"));
}

TEST(ExpressionTest, InExpression) {
  // Arrange -- empty row/schema; construct two IN expressions
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

  // Act + Assert -- IN returns true when left value is in the list, false
  // otherwise
  ASSERT_EQ(in_exp_true->Evaluate(dummy, dummy_schema), Value(true));
  ASSERT_EQ(in_exp_false->Evaluate(dummy, dummy_schema), Value(false));
}

TEST(ExpressionTest, PathologicalCases) {
  // Arrange -- empty row/schema for all sub-cases; sub-cases build their own
  // expressions
  Row dummy({});
  Schema dummy_schema;

  // Subcase 1 -- (1 + 2) * 3 = 9
  // Arrange 1 -- nested binary expression: (1+2)*3
  Expression exp1 = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2))),
      BinaryOperation::kMultiply, ConstantValueExp(Value(3)));
  // Act 1 + Assert 1 -- evaluate and verify result equals 9
  ASSERT_EQ(exp1->Evaluate(dummy, dummy_schema), Value(9));

  // Subcase 2 -- 1 + (2 * 3) = 7
  // Arrange 2 -- nested binary expression: 1+(2*3)
  Expression exp2 =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kAdd,
                          BinaryExpressionExp(ConstantValueExp(Value(2)),
                                              BinaryOperation::kMultiply,
                                              ConstantValueExp(Value(3))));
  // Act 2 + Assert 2 -- evaluate and verify result equals 7
  ASSERT_EQ(exp2->Evaluate(dummy, dummy_schema), Value(7));

  // Subcase 3 -- (true AND false) OR true = true
  // Arrange 3 -- nested logical: (true AND false) OR true
  Expression exp3 = BinaryExpressionExp(
      BinaryExpressionExp(ConstantValueExp(Value(true)), BinaryOperation::kAnd,
                          ConstantValueExp(Value(false))),
      BinaryOperation::kOr, ConstantValueExp(Value(true)));
  // Act 3 + Assert 3 -- evaluate and verify result equals true
  ASSERT_EQ(exp3->Evaluate(dummy, dummy_schema), Value(true));

  // Subcase 4 -- true AND (false OR true) = true
  // Arrange 4 -- nested logical: true AND (false OR true)
  Expression exp4 = BinaryExpressionExp(
      ConstantValueExp(Value(true)), BinaryOperation::kAnd,
      BinaryExpressionExp(ConstantValueExp(Value(false)), BinaryOperation::kOr,
                          ConstantValueExp(Value(true))));
  // Act 4 + Assert 4 -- evaluate and verify result equals true
  ASSERT_EQ(exp4->Evaluate(dummy, dummy_schema), Value(true));

  // Subcase 5 -- ((1 + 2) * 3 - (4 / 2)) > 5 AND (true OR false) = (9 - 2) > 5
  // AND true = 7 > 5 AND true = true Arrange 5 -- deeply nested
  // arithmetic+logical: ((1+2)*3 - 4/2) > 5 AND (true OR false)
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
  // Act 5 + Assert 5 -- evaluate and verify result equals true
  ASSERT_EQ(exp5->Evaluate(dummy, dummy_schema), Value(true));
}

TEST(ExpressionTest, SqlNullThreeValuedLogic) {
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

TEST(ExpressionTest, MixedNumericEvaluationAndTypeInference) {
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

TEST(ExpressionTest, TpchScalarFunctionsUseTheCommonExpressionEvaluator) {
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
  TypeTag Type() const override { return TypeTag::kConstantValue; }
  Value Evaluate(const Row&, const Schema&) const override { return Value(1); }
  std::string ToString() const override { return "bare"; }
  void Dump(std::ostream& o) const override { o << "bare"; }
};

}  // namespace

TEST(ExpressionTest, QueryExpressionToStringAndTouchedColumns) {
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

TEST(ExpressionTest, QueryExpressionEvaluateThrows) {
  Row row;
  Schema schema;
  Expression scalar = QueryExpressionExp(
      std::shared_ptr<SelectStatement>(), ColumnValueExp("x"));
  EXPECT_THROW(scalar->Evaluate(row, schema), std::runtime_error);
  std::ostringstream oss;
  scalar->Dump(oss);
  EXPECT_EQ(oss.str(), "x IN(...)");
}

TEST(ExpressionTest, AggregateResultTypeAndDistinct) {
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

  EXPECT_TRUE(count->Evaluate(row, schema).IsNull());
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

TEST(ExpressionTest, CaseExpressionWithoutElseAndNullCondition) {
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
  // A NULL WHEN condition must not match in SQL three-valued logic. The
  // 4-argument overload skips NULL conditions explicitly.
  EXPECT_EQ(null_condition->Evaluate(&row, schema, nullptr, schema),
            Value("else"));
  EXPECT_EQ(null_condition->Evaluate(row, schema), Value("else"));
}

TEST(ExpressionTest, CaseExpressionResultType) {
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

TEST(ExpressionTest, CaseExpressionTouchedColumnsAndString) {
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

TEST(ExpressionTest, IntervalExpressionEvaluateAndToString) {
  Row row;
  Schema schema;
  Expression interval = IntervalExpressionExp(3, "day");
  EXPECT_EQ(interval->Type(), TypeTag::kIntervalExp);
  EXPECT_EQ(interval->Evaluate(row, schema), Value("3 day"));
  EXPECT_EQ(interval->Evaluate(&row, schema, &row, schema), Value("3 day"));
  EXPECT_EQ(interval->ResultType(schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(interval->ResultType(schema, schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(interval->ToString(), "INTERVAL 3 day");
  std::ostringstream oss;
  interval->Dump(oss);
  EXPECT_EQ(oss.str(), "INTERVAL 3 day");
  const auto& interval_expression = interval->AsIntervalExpression();
  EXPECT_EQ(interval_expression.Amount(), 3);
  EXPECT_EQ(interval_expression.Unit(), "day");
}

TEST(ExpressionTest, InExpressionNullSemantics) {
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

TEST(ExpressionTest, InExpressionToStringAndResultType) {
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

TEST(ExpressionTest, EvaluateBinaryBooleanLogic) {
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

TEST(ExpressionTest, EvaluateBinaryLike) {
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

TEST(ExpressionTest, EvaluateBinaryErrors) {
  EXPECT_THROW(
      EvaluateBinary(BinaryOperation::kLike, Value(1), Value("a%")),
      std::runtime_error);
  EXPECT_THROW(
      EvaluateBinary(BinaryOperation::kLike, Value("a"), Value(1)),
      std::runtime_error);
  EXPECT_THROW(EvaluateBinary(BinaryOperation::kAdd, Value(1), Value("a")),
               std::runtime_error);
  EXPECT_TRUE(EvaluateBinary(BinaryOperation::kAdd, Value(1), Value())
                  .IsNull());
}

TEST(ExpressionTest, EvaluateBinaryMixedNumeric) {
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

TEST(ExpressionTest, BinaryExpressionResultType) {
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

TEST(ExpressionTest, BinaryExpressionTwoSchemaEvaluate) {
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

TEST(ExpressionTest, FunctionCallCoalesceAndConcat) {
  Row row;
  Schema schema;
  Expression coalesce = FunctionCallExp(
      "coalesce", {ConstantValueExp(Value()), ConstantValueExp(Value("b")),
                   ConstantValueExp(Value(3))});
  EXPECT_EQ(coalesce->Evaluate(row, schema), Value("b"));
  Expression all_null = FunctionCallExp(
      "coalesce", {ConstantValueExp(Value()), ConstantValueExp(Value())});
  EXPECT_TRUE(all_null->Evaluate(row, schema).IsNull());
  Expression concat = FunctionCallExp(
      "concat", {ConstantValueExp(Value("a")), ConstantValueExp(Value("b")),
                 ConstantValueExp(Value("c"))});
  EXPECT_EQ(concat->Evaluate(row, schema), Value("abc"));
  Expression concat_null = FunctionCallExp(
      "concat", {ConstantValueExp(Value("a")), ConstantValueExp(Value())});
  EXPECT_TRUE(concat_null->Evaluate(row, schema).IsNull());
  EXPECT_THROW(FunctionCallExp("concat", {ConstantValueExp(Value(1)),
                                          ConstantValueExp(Value("a"))})
                   ->Evaluate(row, schema),
               std::runtime_error);
}

TEST(ExpressionTest, FunctionCallSubstr) {
  Row row;
  Schema schema;
  Expression substr2 = FunctionCallExp(
      "substr", {ConstantValueExp(Value("hello")), ConstantValueExp(Value(2))});
  EXPECT_EQ(substr2->Evaluate(row, schema), Value("ello"));
  Expression substring3 = FunctionCallExp(
      "substring",
      {ConstantValueExp(Value("hello")), ConstantValueExp(Value(2)),
       ConstantValueExp(Value(3))});
  EXPECT_EQ(substring3->Evaluate(row, schema), Value("ell"));
  Expression substr_start_one = FunctionCallExp(
      "substr", {ConstantValueExp(Value("hello")), ConstantValueExp(Value(1)),
                 ConstantValueExp(Value(2))});
  EXPECT_EQ(substr_start_one->Evaluate(row, schema), Value("he"));
  Expression substr_out_of_range = FunctionCallExp(
      "substr",
      {ConstantValueExp(Value("hello")), ConstantValueExp(Value(99))});
  EXPECT_EQ(substr_out_of_range->Evaluate(row, schema), Value(std::string()));
  Expression substr_null = FunctionCallExp(
      "substr",
      {ConstantValueExp(Value("hello")), ConstantValueExp(Value())});
  EXPECT_TRUE(substr_null->Evaluate(row, schema).IsNull());
  EXPECT_THROW(FunctionCallExp("substr", {ConstantValueExp(Value("hello"))})
                   ->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_THROW(
      FunctionCallExp("substr", {ConstantValueExp(Value("hello")),
                                 ConstantValueExp(Value("x"))})
          ->Evaluate(row, schema),
      std::runtime_error);
  EXPECT_EQ(substr2->ToString(), "substr(\"hello\", 2)");
  std::ostringstream oss;
  substr2->Dump(oss);
  EXPECT_EQ(oss.str(), "substr(\"hello\", 2)");
  EXPECT_EQ(substr2->AsFunctionCallExpression().Args().size(), 2);
}

TEST(ExpressionTest, FunctionCallExtract) {
  Row row;
  Schema schema;
  Expression year = FunctionCallExp(
      "extract_year", {ConstantValueExp(Value::Date("1996-07-15"))});
  EXPECT_EQ(year->Evaluate(row, schema), Value(1996));
  Expression month = FunctionCallExp(
      "extract_month", {ConstantValueExp(Value::Date("1996-07-15"))});
  EXPECT_EQ(month->Evaluate(row, schema), Value(7));
  Expression day = FunctionCallExp(
      "extract_day", {ConstantValueExp(Value("1996-07-15"))});
  EXPECT_EQ(day->Evaluate(row, schema), Value(15));
  Expression null_extract = FunctionCallExp(
      "extract_year", {ConstantValueExp(Value())});
  EXPECT_TRUE(null_extract->Evaluate(row, schema).IsNull());
  EXPECT_THROW(FunctionCallExp("extract_year", {})->Evaluate(row, schema),
               std::runtime_error);
  EXPECT_THROW(FunctionCallExp("extract_year",
                               {ConstantValueExp(Value("bad"))})
                   ->Evaluate(row, schema),
               std::runtime_error);
}

TEST(ExpressionTest, FunctionCallCurrentTimestampAndResultType) {
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
  EXPECT_THROW(FunctionCallExp("unknown_func", {})->ResultType(schema),
               std::runtime_error);
  EXPECT_EQ(FunctionCallExp("coalesce", {ColumnValueExp("x"),
                                         ColumnValueExp("y")})
                ->TouchedColumns()
                .size(),
            2);
}

TEST(ExpressionTest, FunctionCallDateAddSub) {
  Row row;
  Schema schema;
  Expression date_add = FunctionCallExp(
      "date_add", {ConstantValueExp(Value::Date("1994-01-01")),
                   IntervalExpressionExp(1, "year")});
  EXPECT_EQ(date_add->Evaluate(row, schema), Value::Date("1995-01-01"));
  Expression date_sub = FunctionCallExp(
      "date_sub", {ConstantValueExp(Value("1998-12-01")),
                   IntervalExpressionExp(74, "day")});
  EXPECT_EQ(date_sub->Evaluate(row, schema), Value("1998-09-18"));
  Expression date_add_4 = FunctionCallExp(
      "date_add", {ConstantValueExp(Value("1994-01-01")),
                   IntervalExpressionExp(1, "year")});
  EXPECT_EQ(date_add_4->Evaluate(&row, schema, &row, schema),
            Value("1995-01-01"));
  Expression date_add_null = FunctionCallExp(
      "date_add", {ConstantValueExp(Value()), IntervalExpressionExp(1, "day")});
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

TEST(ExpressionTest, ColumnValueCaseInsensitiveLookup) {
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

TEST(ExpressionTest, ColumnValueResultType) {
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

TEST(ExpressionTest, ColumnValueTwoSchemaEvaluation) {
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

TEST(ExpressionTest, ColumnValueMisc) {
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

TEST(ExpressionTest, UnaryMinusErrorsAndNull) {
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

TEST(ExpressionTest, UnaryResultType) {
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

TEST(ExpressionTest, UnaryToStringAndDump) {
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

TEST(ExpressionTest, ExpressionBaseCasts) {
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

TEST(ExpressionTest, BaseClassDefaultsThrow) {
  BareExpression bare;
  Row row;
  Schema schema;
  EXPECT_THROW((void)bare.Evaluate(&row, schema, &row, schema),
               std::runtime_error);
  EXPECT_THROW((void)bare.ResultType(schema), std::runtime_error);
  EXPECT_THROW((void)bare.ResultType(schema, schema), std::runtime_error);
  EXPECT_EQ(bare.TouchedColumns().size(), 0);
}

TEST(ExpressionTest, NamedExpressionStreaming) {
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

TEST(ExpressionTest, FunctionCallTwoRowEvaluate) {
  Schema left("", {Column("x", ValueType::kVarChar),
                   Column("b", ValueType::kInt64)});
  Row left_row({Value("hello"), Value(2)});
  Schema right("", {Column("d", ValueType::kVarChar)});
  Row right_row({Value("1996-07-15")});

  // Act -- evaluate each registered function through the left/right overload
  // Assert -- the right row supplies columns missing from the left row
  EXPECT_EQ(FunctionCallExp("coalesce",
                            {ColumnValueExp("d"), ColumnValueExp("x")})
                ->Evaluate(&left_row, left, &right_row, right),
            Value("1996-07-15"));
  EXPECT_EQ(FunctionCallExp("extract_day", {ColumnValueExp("d")})
                ->Evaluate(&left_row, left, &right_row, right),
            Value(15));
  // Assert -- columns present on the left row are read from there
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

TEST(ExpressionTest, FunctionCallResultTypeTwoSchemas) {
  Schema left("", {Column("i", ValueType::kInt64),
                   Column("v", ValueType::kVarChar)});
  Schema right("", {Column("v", ValueType::kVarChar)});

  // Act -- resolve result types through the left/right schema overload
  // Assert -- coalesce follows the first argument's type
  EXPECT_EQ(FunctionCallExp("coalesce",
                            {ColumnValueExp("i"), ColumnValueExp("v")})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kBigInt);
  // Assert -- string-producing functions are always varchar
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
  // Assert -- date_add/date_sub follow their date argument
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
  // Assert -- extract_* functions resolve to bigint
  EXPECT_EQ(FunctionCallExp("extract_month", {ColumnValueExp("v")})
                ->ResultType(left, right)
                .GetType(),
            TypeTag::kBigInt);
  // Assert -- an unknown function still fails in the two-schema variant
  EXPECT_THROW(FunctionCallExp("unknown_func", {})->ResultType(left, right),
               std::runtime_error);
}

TEST(ExpressionTest, FunctionCallNestedTouchedAndToString) {
  Row row;
  Schema schema;

  // Act -- nest a function call inside another and render it
  Expression nested = FunctionCallExp(
      "concat",
      {FunctionCallExp("substr", {ConstantValueExp(Value("hello")),
                                  ConstantValueExp(Value(2))}),
       ConstantValueExp(Value("!"))});
  // Assert -- nested evaluation and a multi-argument ToString/Dump
  EXPECT_EQ(nested->Evaluate(row, schema), Value("ello!"));
  EXPECT_EQ(nested->ToString(), "concat(substr(\"hello\", 2), \"!\")");
  std::ostringstream oss;
  nested->Dump(oss);
  EXPECT_EQ(oss.str(), "concat(substr(\"hello\", 2), \"!\")");

  // Assert -- three-argument separators in ToString
  Expression substring3 = FunctionCallExp(
      "substring", {ConstantValueExp(Value("hello")),
                    ConstantValueExp(Value(2)), ConstantValueExp(Value(3))});
  EXPECT_EQ(substring3->ToString(), "substring(\"hello\", 2, 3)");

  // Act -- a function whose arguments are themselves functions and columns
  Expression columns = FunctionCallExp(
      "coalesce",
      {ColumnValueExp("a"),
       FunctionCallExp("substr", {ColumnValueExp("b"),
                                  ConstantValueExp(Value(1))})});
  // Assert -- touched columns merge from every nested argument
  EXPECT_EQ(columns->TouchedColumns().size(), 2);
  EXPECT_EQ(columns->Type(), TypeTag::kFunctionCallExp);
}

}  // namespace tinylamb
