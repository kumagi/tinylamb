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
#include "expression/function_call_expression.hpp"
#include "expression/unary_expression.hpp"
#include "parser/ast.hpp"
#include "parser/token.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

PrattParser::PrattParser(std::vector<Token>::const_iterator begin,
                         std::vector<Token>::const_iterator end)
    : begin_pos_(begin), current_pos_(begin), end_pos_(end) {}

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

namespace {
BinaryOperation GetBinaryOperation(const std::string& op_str) {
  if (op_str == "=") return BinaryOperation::kEquals;
  if (op_str == "!=") return BinaryOperation::kNotEquals;
  if (op_str == "<") return BinaryOperation::kLessThan;
  if (op_str == "<=") return BinaryOperation::kLessThanEquals;
  if (op_str == ">") return BinaryOperation::kGreaterThan;
  if (op_str == ">=") return BinaryOperation::kGreaterThanEquals;
  if (op_str == "+") return BinaryOperation::kAdd;
  if (op_str == "-") return BinaryOperation::kSubtract;
  if (op_str == "*") return BinaryOperation::kMultiply;
  if (op_str == "/") return BinaryOperation::kDivide;
  throw std::runtime_error("Unsupported binary operation: " + op_str);
}
}  // namespace

Expression PrattParser::ParseExpression(int precedence) {
  Expression left = ParseUnary();
  while (precedence < GetPrecedence()) {
    int current_op_precedence = GetPrecedence();
    Token op = Advance();
    left = BinaryExpressionExp(left, GetBinaryOperation(op.value),
                               ParseExpression(current_op_precedence));
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
    std::string func_name = token.value;
    if (Peek().type == TokenType::kLParen) {
      Advance();  // Consume '('
      std::vector<Expression> args;
      if (Peek().type != TokenType::kRParen) {
        args.push_back(ParseExpression(0));
        while (Peek().type == TokenType::kComma) {
          Advance();  // Consume ','
          args.push_back(ParseExpression(0));
        }
      }
      Expect(TokenType::kRParen);  // Consume ')'
      return FunctionCallExp(func_name, std::move(args));
    }
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
  if (current_pos_ >= end_pos_) {
    return {TokenType::kEof, ""};
  }
  return *current_pos_;
}

Token PrattParser::Advance() {
  if (current_pos_ >= end_pos_) {
    return {TokenType::kEof, ""};
  }
  return *current_pos_++;
}

void PrattParser::Expect(TokenType type) {
  Token token = Advance();
  if (token.type != type) {
    throw std::runtime_error("Unexpected token");
  }
}

}  // namespace tinylamb
