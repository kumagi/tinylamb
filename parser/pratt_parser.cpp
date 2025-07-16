/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law of agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "parser/pratt_parser.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/unary_expression.hpp"
#include "parser/ast.hpp"
#include "parser/token.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

PrattParser::PrattParser(const std::vector<Token>& tokens) : tokens_(tokens) {}

int PrattParser::GetPrecedence() {
  const Token& token = Peek();
  if (token.type == TokenType::kOperator) {
    if (token.value == "=" || token.value == "!=" || token.value == "<" ||
        token.value == "<=" || token.value == ">" || token.value == ">=") {
      return 1;
    }
    if (token.value == "+" || token.value == "-") {
      return 2;
    }
    if (token.value == "*" || token.value == "/") {
      return 3;
    }
  }
  return 0;
}

Expression PrattParser::ParseExpression(int precedence) {
  Expression left = ParseUnary();
  while (precedence < GetPrecedence()) {
    Token op = Advance();
    if (op.value == "=") {
      left = BinaryExpressionExp(left, BinaryOperation::kEquals,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "!=") {
      left = BinaryExpressionExp(left, BinaryOperation::kNotEquals,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "<") {
      left = BinaryExpressionExp(left, BinaryOperation::kLessThan,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "<=") {
      left = BinaryExpressionExp(left, BinaryOperation::kLessThanEquals,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == ">") {
      left = BinaryExpressionExp(left, BinaryOperation::kGreaterThan,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == ">=") {
      left = BinaryExpressionExp(left, BinaryOperation::kGreaterThanEquals,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "+") {
      left = BinaryExpressionExp(left, BinaryOperation::kAdd,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "-") {
      left = BinaryExpressionExp(left, BinaryOperation::kSubtract,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "*") {
      left = BinaryExpressionExp(left, BinaryOperation::kMultiply,
                                 ParseExpression(GetPrecedence()));
    } else if (op.value == "/") {
      left = BinaryExpressionExp(left, BinaryOperation::kDivide,
                                 ParseExpression(GetPrecedence()));
    } else {
      throw std::runtime_error("Unsupported expression: " + op.ToString());
    }
  }
  return left;
}

Expression PrattParser::ParseUnary() {
  if (Peek().type == TokenType::kOperator && Peek().value == "-") {
    Advance();
    return UnaryExpressionExp(ParseExpression(3), UnaryOperation::kMinus);
  }
  return ParsePrimary();
}

Expression PrattParser::ParsePrimary() {
  if (Peek().type == TokenType::kLParen) {
    Advance();
    Expression expr = ParseExpression(0);
    Expect(TokenType::kRParen);
    return expr;
  }
  Token token = Advance();
  if (token.type == TokenType::kIdentifier) {
    return ColumnValueExp(token.value);
  }
  if (token.type == TokenType::kNumeric) {
    return ConstantValueExp(
        Value(static_cast<int64_t>(std::stoll(token.value))));
  }
  if (token.type == TokenType::kString) {
    return ConstantValueExp(Value(std::move(token.value)));
  }
  throw std::runtime_error("Unsupported expression");
}

Token PrattParser::Peek() {
  if (pos_ >= tokens_.size()) {
    return {TokenType::kEof, ""};
  }
  return tokens_[pos_];
}

Token PrattParser::Advance() {
  if (pos_ >= tokens_.size()) {
    return {TokenType::kEof, ""};
  }
  return tokens_[pos_++];
}

void PrattParser::Expect(TokenType type) {
  Token token = Advance();
  if (token.type != type) {
    throw std::runtime_error("Unexpected token");
  }
}

}  // namespace tinylamb
