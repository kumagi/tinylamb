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

#include "parser/pratt_parser.hpp"

#include <gtest/gtest.h>

#include <string>
#include <stdexcept>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "parser/token.hpp"
#include "parser/tokenizer.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
Expression ParseExpressionString(const std::string& sql) {
  Tokenizer tokenizer(sql);
  std::vector<Token> tokens = tokenizer.Tokenize();
  PrattParser parser(tokens.begin(), tokens.end());
  return parser.ParseExpression();
}
}  // namespace

TEST(ExpressionParserTest, Simple) {
  // Arrange
  Tokenizer tokenizer("1 + 2");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  const Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  const auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAdd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kConstantValue);
}

TEST(ExpressionParserTest, Precedence) {
  // Arrange
  Tokenizer tokenizer("1 + 2 * 3");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  const auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAdd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kBinaryExp);
  const auto& be2 = be.Right()->AsBinaryExpression();
  ASSERT_EQ(be2.Op(), BinaryOperation::kMultiply);
}

TEST(ExpressionParserTest, Parentheses) {
  // Arrange
  Tokenizer tokenizer("(1 + 2) * 3");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  const auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kMultiply);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kConstantValue);
  const auto& be2 = be.Left()->AsBinaryExpression();
  ASSERT_EQ(be2.Op(), BinaryOperation::kAdd);
}

TEST(ExpressionParserTest, DifficultCaseUnaryMultiply) {
  // Arrange
  Tokenizer tokenizer("-1 * (2 + 3)");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->ToString(), "((-1) * (2 + 3))");
}

TEST(ExpressionParserTest, DifficultCaseMultiplyAddMultiply) {
  // Arrange
  Tokenizer tokenizer("a * b + c * d");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->ToString(), "((a * b) + (c * d))");
}

TEST(ExpressionParserTest, DifficultCaseAddMultiplyAdd) {
  // Arrange
  Tokenizer tokenizer("a + b * c + d");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->ToString(), "((a + (b * c)) + d)");
}

TEST(ExpressionParserTest, DifficultCaseParenthesesMultiplyParentheses) {
  // Arrange
  Tokenizer tokenizer("(a + b) * (c + d)");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->ToString(), "((a + b) * (c + d))");
}

TEST(ExpressionParserTest, DifficultCaseNegativeParenthesesMultiply) {
  // Arrange
  Tokenizer tokenizer("-(a + b) * c");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->ToString(), "((-(a + b)) * c)");
}

TEST(ExpressionParserTest, FunctionCall) {
  // Arrange
  Tokenizer tokenizer("f(1, a + 2)");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->Type(), TypeTag::kFunctionCallExp);
  const auto& fce = expr->AsFunctionCallExpression();
  ASSERT_EQ(fce.FuncName(), "f");
  ASSERT_EQ(fce.Args().size(), 2);
  ASSERT_EQ(fce.Args()[0]->ToString(), "1");
  ASSERT_EQ(fce.Args()[1]->ToString(), "(a + 2)");
}

TEST(ExpressionParserTest, ComparisonOperators) {
  // Arrange + Act -- parse each comparison operator
  // Assert -- each operator maps to the expected BinaryOperation
  {
    Expression expr = ParseExpressionString("a <> b");
    ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
    ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kNotEquals);
    ASSERT_EQ(expr->ToString(), "(a != b)");
  }
  {
    Expression expr = ParseExpressionString("a < b");
    ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kLessThan);
  }
  {
    Expression expr = ParseExpressionString("a <= b");
    ASSERT_EQ(expr->AsBinaryExpression().Op(),
              BinaryOperation::kLessThanEquals);
  }
  {
    Expression expr = ParseExpressionString("a > b");
    ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kGreaterThan);
  }
  {
    Expression expr = ParseExpressionString("a >= b");
    ASSERT_EQ(expr->AsBinaryExpression().Op(),
              BinaryOperation::kGreaterThanEquals);
  }
  {
    Expression expr = ParseExpressionString("a = b");
    ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kEquals);
  }
  {
    Expression expr = ParseExpressionString("a != b");
    ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kNotEquals);
  }
}

TEST(ExpressionParserTest, DivideModuloAndOr) {
  // Arrange + Act -- parse chained /, %, AND, OR with different precedences
  Expression expr = ParseExpressionString("a / b % c AND d OR e");
  // Assert -- OR has the lowest precedence, then AND, then the arithmetic ops
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kOr);
  const auto& and_expr = expr->AsBinaryExpression().Left()->AsBinaryExpression();
  ASSERT_EQ(and_expr.Op(), BinaryOperation::kAnd);
  const auto& modulo_expr =
      and_expr.Left()->AsBinaryExpression();
  ASSERT_EQ(modulo_expr.Op(), BinaryOperation::kModulo);
  const auto& divide_expr = modulo_expr.Left()->AsBinaryExpression();
  ASSERT_EQ(divide_expr.Op(), BinaryOperation::kDivide);
  ASSERT_EQ(expr->ToString(), "((((a / b) % c) AND d) OR e)");
}

TEST(ExpressionParserTest, InExpression) {
  // Arrange + Act -- parse IN with three values
  Expression expr = ParseExpressionString("a IN (1, 2, 3)");
  // Assert -- InExpression with 3 list entries and child "a"
  ASSERT_EQ(expr->Type(), TypeTag::kInExp);
  const auto& in = expr->AsInExpression();
  ASSERT_EQ(in.child_->ToString(), "a");
  ASSERT_EQ(in.list_.size(), 3);
  ASSERT_EQ(in.list_[0]->ToString(), "1");
  ASSERT_EQ(in.list_[1]->ToString(), "2");
  ASSERT_EQ(in.list_[2]->ToString(), "3");
  ASSERT_EQ(expr->ToString(), "a IN (1, 2, 3)");
}

TEST(ExpressionParserTest, InExpressionEmpty) {
  // Arrange + Act + Assert -- an empty IN list is a parse error
  EXPECT_THROW(ParseExpressionString("a IN ()"), std::runtime_error);
}

TEST(ExpressionParserTest, IsNull) {
  // Arrange + Act -- parse IS NULL
  Expression expr = ParseExpressionString("a IS NULL");
  // Assert -- UnaryExpression with kIsNull over column a
  ASSERT_EQ(expr->Type(), TypeTag::kUnaryExp);
  ASSERT_EQ(expr->AsUnaryExpression().Op(), UnaryOperation::kIsNull);
  ASSERT_EQ(expr->AsUnaryExpression().Child()->ToString(), "a");
}

TEST(ExpressionParserTest, IsNotNull) {
  // Arrange + Act -- parse IS NOT NULL
  Expression expr = ParseExpressionString("a IS NOT NULL");
  // Assert -- UnaryExpression with kIsNotNull
  ASSERT_EQ(expr->Type(), TypeTag::kUnaryExp);
  ASSERT_EQ(expr->AsUnaryExpression().Op(), UnaryOperation::kIsNotNull);
}

TEST(ExpressionParserTest, UnaryNot) {
  // Arrange + Act -- parse NOT
  Expression expr = ParseExpressionString("NOT a");
  // Assert -- UnaryExpression with kNot
  ASSERT_EQ(expr->Type(), TypeTag::kUnaryExp);
  ASSERT_EQ(expr->AsUnaryExpression().Op(), UnaryOperation::kNot);
  ASSERT_EQ(expr->AsUnaryExpression().Child()->ToString(), "a");
}

TEST(ExpressionParserTest, CaseExpression) {
  // Arrange + Act -- parse CASE with one WHEN/THEN and an ELSE
  Expression expr =
      ParseExpressionString("CASE WHEN a = 1 THEN 10 ELSE 20 END");
  // Assert -- CaseExpression with one clause and an else clause
  ASSERT_EQ(expr->Type(), TypeTag::kCaseExp);
  const auto& case_expr = expr->AsCaseExpression();
  ASSERT_EQ(case_expr.when_clauses_.size(), 1);
  ASSERT_EQ(case_expr.when_clauses_[0].first->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(case_expr.when_clauses_[0].second->Type(), TypeTag::kConstantValue);
  ASSERT_NE(case_expr.else_clause_, nullptr);
  ASSERT_EQ(case_expr.else_clause_->ToString(), "20");
}

TEST(ExpressionParserTest, CaseWithoutElse) {
  // Arrange + Act -- parse CASE without an ELSE clause
  Expression expr = ParseExpressionString("CASE WHEN a THEN 1 END");
  // Assert -- else_clause_ is null; single WHEN clause
  ASSERT_EQ(expr->Type(), TypeTag::kCaseExp);
  const auto& case_expr = expr->AsCaseExpression();
  ASSERT_EQ(case_expr.when_clauses_.size(), 1);
  ASSERT_EQ(case_expr.else_clause_, nullptr);
}

TEST(ExpressionParserTest, AggregateFunctions) {
  // Arrange + Act -- parse each aggregate function
  // Assert -- AggregateExpression type is preserved
  ASSERT_EQ(ParseExpressionString("SUM(a)")->Type(), TypeTag::kAggregateExp);
  ASSERT_EQ(ParseExpressionString("AVG(a)")->Type(), TypeTag::kAggregateExp);
  ASSERT_EQ(ParseExpressionString("MIN(a)")->Type(), TypeTag::kAggregateExp);
  ASSERT_EQ(ParseExpressionString("MAX(a)")->Type(), TypeTag::kAggregateExp);
  Expression sum = ParseExpressionString("SUM(a)");
  ASSERT_EQ(sum->AsAggregateExpression().GetType(), AggregationType::kSum);
  ASSERT_EQ(sum->AsAggregateExpression().Child()->ToString(), "a");
  ASSERT_FALSE(sum->AsAggregateExpression().Distinct());
}

TEST(ExpressionParserTest, AggregateCountStar) {
  // Arrange + Act -- parse COUNT(*) and COUNT(DISTINCT a)
  Expression count_star = ParseExpressionString("COUNT(*)");
  // Assert -- COUNT(*) becomes an aggregate over the "*" column
  ASSERT_EQ(count_star->Type(), TypeTag::kAggregateExp);
  ASSERT_EQ(count_star->AsAggregateExpression().GetType(),
            AggregationType::kCount);
  ASSERT_EQ(count_star->AsAggregateExpression().Child()->Type(),
            TypeTag::kColumnValue);
  ASSERT_EQ(count_star->AsAggregateExpression().Child()->ToString(), "*");
  // Act -- COUNT(DISTINCT a)
  Expression count_distinct = ParseExpressionString("COUNT(DISTINCT a)");
  // Assert -- distinct flag is set
  ASSERT_TRUE(count_distinct->AsAggregateExpression().Distinct());
  ASSERT_EQ(count_distinct->ToString(), "COUNT(DISTINCT a)");
}

TEST(ExpressionParserTest, QualifiedColumn) {
  // Arrange + Act -- parse a schema-qualified column
  Expression expr = ParseExpressionString("t.col");
  // Assert -- ColumnValue carries schema "t" and name "col"
  ASSERT_EQ(expr->Type(), TypeTag::kColumnValue);
  const auto& col = expr->AsColumnValue().GetColumnName();
  ASSERT_EQ(col.schema, "t");
  ASSERT_EQ(col.name, "col");
}

TEST(ExpressionParserTest, DecimalConstant) {
  // Arrange + Act -- parse a decimal literal in an expression
  Expression expr = ParseExpressionString("1.5 + 2");
  // Assert -- 1.5 is parsed as a kDouble constant
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  const auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAdd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(be.Left()->AsConstantValue().GetValue(), Value(1.5));
  ASSERT_EQ(be.Right()->AsConstantValue().GetValue(), Value(2));
}

TEST(ExpressionParserTest, BooleanAndNullConstants) {
  // Arrange + Act -- parse TRUE / FALSE / NULL literals
  // Assert -- each yields the expected constant Value
  ASSERT_EQ(ParseExpressionString("TRUE")->AsConstantValue().GetValue(),
            Value(true));
  ASSERT_EQ(ParseExpressionString("FALSE")->AsConstantValue().GetValue(),
            Value(false));
  ASSERT_TRUE(
      ParseExpressionString("NULL")->AsConstantValue().GetValue().IsNull());
}

TEST(ExpressionParserTest, ParseErrors) {
  // Arrange + Act + Assert -- malformed inputs throw
  EXPECT_THROW(ParseExpressionString("a IS 5"), std::runtime_error);
  EXPECT_THROW(ParseExpressionString("CASE WHEN a THEN 1 b"), std::runtime_error);
  EXPECT_THROW(ParseExpressionString("CASE WHEN a THEN 1"), std::runtime_error);
  EXPECT_THROW(ParseExpressionString("COUNT(1, 2)"), std::runtime_error);
  EXPECT_THROW(ParseExpressionString("t.1"), std::runtime_error);
  EXPECT_THROW(ParseExpressionString("+"), std::runtime_error);
  // Malformed / out-of-range numeric literals must not leak stod/stoll
  // exception types.
  EXPECT_THROW(ParseExpressionString("1.2.3 + 4"), std::runtime_error);
  EXPECT_THROW(ParseExpressionString("99999999999999999999999"),
               std::runtime_error);
}

TEST(ExpressionParserTest, DeepNestingIsRejected) {
  // Arrange -- deeply nested parentheses exceed the parser depth limit.
  const std::string deep(10000, '(');
  const std::string sql = deep + "1" + std::string(10000, ')');

  // Act + Assert -- throws runtime_error instead of overflowing the stack.
  EXPECT_THROW(ParseExpressionString(sql), std::runtime_error);

  // Moderate nesting below the limit still parses.
  const std::string moderate(100, '(');
  const Expression ok =
      ParseExpressionString(moderate + "1" + std::string(100, ')'));
  ASSERT_EQ(ok->Type(), TypeTag::kConstantValue);
}

TEST(ExpressionParserTest, UnaryMinusWithArithmetic) {
  // Arrange + Act -- parse "-a * b + c"
  Expression expr = ParseExpressionString("-a * b + c");
  // Assert -- the minus binds to 'a', and the sum is at the top
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kAdd);
  const auto& left = expr->AsBinaryExpression().Left()->AsBinaryExpression();
  ASSERT_EQ(left.Op(), BinaryOperation::kMultiply);
  ASSERT_EQ(left.Left()->Type(), TypeTag::kUnaryExp);
  ASSERT_EQ(left.Left()->AsUnaryExpression().Op(), UnaryOperation::kMinus);
  ASSERT_EQ(expr->ToString(), "(((-a) * b) + c)");
}

}  // namespace tinylamb
