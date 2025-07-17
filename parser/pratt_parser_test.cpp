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

#include <memory>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/function_call_expression.hpp"
#include "parser/token.hpp"
#include "parser/tokenizer.hpp"

namespace tinylamb {

TEST(ExpressionParserTest, Simple) {
  // Arrange
  Tokenizer tokenizer("1 + 2");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act
  PrattParser parser(tokens.begin(), tokens.end());
  Expression expr = parser.ParseExpression();

  // Assert
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  auto& be = expr->AsBinaryExpression();
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
  auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAdd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kBinaryExp);
  auto& be2 = be.Right()->AsBinaryExpression();
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
  auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kMultiply);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kConstantValue);
  auto& be2 = be.Left()->AsBinaryExpression();
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
  auto& fce = expr->AsFunctionCallExpression();
  ASSERT_EQ(fce.FuncName(), "f");
  ASSERT_EQ(fce.Args().size(), 2);
  ASSERT_EQ(fce.Args()[0]->ToString(), "1");
  ASSERT_EQ(fce.Args()[1]->ToString(), "(a + 2)");
}

}  // namespace tinylamb
