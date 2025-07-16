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
#include "parser/token.hpp"
#include "parser/tokenizer.hpp"

namespace tinylamb {

TEST(ExpressionParserTest, Simple) {
  Tokenizer tokenizer("1 + 2");
  std::vector<Token> tokens = tokenizer.Tokenize();
  PrattParser parser(tokens);
  Expression expr = parser.ParseExpression();
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAdd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kConstantValue);
}

TEST(ExpressionParserTest, Precedence) {
  Tokenizer tokenizer("1 + 2 * 3");
  std::vector<Token> tokens = tokenizer.Tokenize();
  PrattParser parser(tokens);
  Expression expr = parser.ParseExpression();
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAdd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kConstantValue);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kBinaryExp);
  auto& be2 = be.Right()->AsBinaryExpression();
  ASSERT_EQ(be2.Op(), BinaryOperation::kMultiply);
}

TEST(ExpressionParserTest, Parentheses) {
  Tokenizer tokenizer("(1 + 2) * 3");
  std::vector<Token> tokens = tokenizer.Tokenize();
  PrattParser parser(tokens);
  Expression expr = parser.ParseExpression();
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  auto& be = expr->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kMultiply);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kConstantValue);
  auto& be2 = be.Left()->AsBinaryExpression();
  ASSERT_EQ(be2.Op(), BinaryOperation::kAdd);
}

TEST(ExpressionParserTest, DifficultCases) {
  // -1 * (2 + 3)
  Tokenizer tokenizer1("-1 * (2 + 3)");
  std::vector<Token> tokens1 = tokenizer1.Tokenize();
  PrattParser parser1(tokens1);
  Expression expr1 = parser1.ParseExpression();
  ASSERT_EQ(expr1->ToString(), "(-1 * (2 + 3))");

  // a * b + c * d
  Tokenizer tokenizer2("a * b + c * d");
  std::vector<Token> tokens2 = tokenizer2.Tokenize();
  PrattParser parser2(tokens2);
  Expression expr2 = parser2.ParseExpression();
  ASSERT_EQ(expr2->ToString(), "((a * b) + (c * d))");

  // a + b * c + d
  Tokenizer tokenizer3("a + b * c + d");
  std::vector<Token> tokens3 = tokenizer3.Tokenize();
  PrattParser parser3(tokens3);
  Expression expr3 = parser3.ParseExpression();
  ASSERT_EQ(expr3->ToString(), "((a + (b * c)) + d)");

  // (a + b) * (c + d)
  Tokenizer tokenizer4("(a + b) * (c + d)");
  std::vector<Token> tokens4 = tokenizer4.Tokenize();
  PrattParser parser4(tokens4);
  Expression expr4 = parser4.ParseExpression();
  ASSERT_EQ(expr4->ToString(), "((a + b) * (c + d))");
}

}  // namespace tinylamb
