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
#include "expression/case_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
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
  if (token.type == TokenType::kKeyword) {
    if (token.value == "OR") return 1;
    if (token.value == "AND") return 2;
    if (token.value == "IN" || token.value == "IS") return 3;
  }
  if (token.type == TokenType::kOperator) {
    if (token.value == "=" || token.value == "!=" || token.value == "<" ||
        token.value == "<>" || token.value == "<=" || token.value == ">" ||
        token.value == ">=") {
      return 3;
    }
    if (token.value == "+" || token.value == "-") {
      return 4;
    }
    if (token.value == "*" || token.value == "/" || token.value == "%") {
      return 5;
    }
  }
  return 0;
}

namespace {
BinaryOperation GetBinaryOperation(const std::string& op_str) {
  if (op_str == "=") return BinaryOperation::kEquals;
  if (op_str == "!=" || op_str == "<>") return BinaryOperation::kNotEquals;
  if (op_str == "<") return BinaryOperation::kLessThan;
  if (op_str == "<=") return BinaryOperation::kLessThanEquals;
  if (op_str == ">") return BinaryOperation::kGreaterThan;
  if (op_str == ">=") return BinaryOperation::kGreaterThanEquals;
  if (op_str == "+") return BinaryOperation::kAdd;
  if (op_str == "-") return BinaryOperation::kSubtract;
  if (op_str == "*") return BinaryOperation::kMultiply;
  if (op_str == "/") return BinaryOperation::kDivide;
  if (op_str == "%") return BinaryOperation::kModulo;
  if (op_str == "AND") return BinaryOperation::kAnd;
  if (op_str == "OR") return BinaryOperation::kOr;
  throw std::runtime_error("Unsupported binary operation: " + op_str);
}
}  // namespace

Expression PrattParser::ParseExpression(int precedence) {
  Expression left = ParseUnary();
  while (precedence < GetPrecedence()) {
    int current_op_precedence = GetPrecedence();
    Token op = Advance();
    if (op.type == TokenType::kKeyword && op.value == "IN") {
      Expect(TokenType::kLParen);
      std::vector<Expression> values;
      if (Peek().type != TokenType::kRParen) {
        values.push_back(ParseExpression(0));
        while (Peek().type == TokenType::kComma) {
          Advance();
          values.push_back(ParseExpression(0));
        }
      }
      Expect(TokenType::kRParen);
      left = InExpressionExp(left, std::move(values));
    } else if (op.type == TokenType::kKeyword && op.value == "IS") {
      bool negate = false;
      if (Peek().type == TokenType::kKeyword && Peek().value == "NOT") {
        Advance();
        negate = true;
      }
      if (Peek().type != TokenType::kKeyword || Peek().value != "NULL") {
        throw std::runtime_error("IS currently supports only NULL");
      }
      Advance();
      left = UnaryExpressionExp(
          left, negate ? UnaryOperation::kIsNotNull : UnaryOperation::kIsNull);
    } else {
      left = BinaryExpressionExp(left, GetBinaryOperation(op.value),
                                 ParseExpression(current_op_precedence));
    }
  }
  return left;
}

Expression PrattParser::ParseUnary() {
  if (Peek().type == TokenType::kOperator && Peek().value == "-") {
    Advance();
    return UnaryExpressionExp(ParseExpression(5), UnaryOperation::kMinus);
  }
  if (Peek().type == TokenType::kKeyword && Peek().value == "NOT") {
    Advance();
    return UnaryExpressionExp(ParseExpression(2), UnaryOperation::kNot);
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
  if (Peek().type == TokenType::kKeyword && Peek().value == "CASE") {
    Advance();
    std::vector<std::pair<Expression, Expression>> clauses;
    while (Peek().type == TokenType::kKeyword && Peek().value == "WHEN") {
      Advance();
      Expression condition = ParseExpression(0);
      if (Advance().value != "THEN") {
        throw std::runtime_error("expected THEN in CASE expression");
      }
      Expression value = ParseExpression(0);
      clauses.emplace_back(std::move(condition), std::move(value));
    }
    Expression otherwise;
    if (Peek().type == TokenType::kKeyword && Peek().value == "ELSE") {
      Advance();
      otherwise = ParseExpression(0);
    }
    if (Advance().value != "END") {
      throw std::runtime_error("expected END in CASE expression");
    }
    return CaseExpressionExp(std::move(clauses), std::move(otherwise));
  }

  Token token = Advance();
  if (token.type == TokenType::kIdentifier) {
    std::string func_name = token.value;
    if (Peek().type == TokenType::kLParen) {
      Advance();  // Consume '('
      std::vector<Expression> args;
      bool distinct = false;
      if (Peek().type == TokenType::kKeyword && Peek().value == "DISTINCT") {
        Advance();
        distinct = true;
      }
      if (Peek().type != TokenType::kRParen) {
        if (Peek().type == TokenType::kOperator && Peek().value == "*") {
          Advance();
          args.push_back(ColumnValueExp("*"));
        } else {
          args.push_back(ParseExpression(0));
        }
        while (Peek().type == TokenType::kComma) {
          Advance();  // Consume ','
          args.push_back(ParseExpression(0));
        }
      }
      Expect(TokenType::kRParen);  // Consume ')'
      std::string upper_name = func_name;
      std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                     ::toupper);
      if (upper_name == "COUNT" || upper_name == "SUM" || upper_name == "AVG" ||
          upper_name == "MIN" || upper_name == "MAX") {
        if (args.size() != 1) {
          throw std::runtime_error("aggregate function requires one argument");
        }
        AggregationType type = AggregationType::kCount;
        if (upper_name == "SUM") type = AggregationType::kSum;
        if (upper_name == "AVG") type = AggregationType::kAvg;
        if (upper_name == "MIN") type = AggregationType::kMin;
        if (upper_name == "MAX") type = AggregationType::kMax;
        return AggregateExpressionExp(type, args[0], distinct);
      }
      return FunctionCallExp(func_name, std::move(args));
    }
    if (Peek().type == TokenType::kDot) {
      Advance();
      Token name = Advance();
      if (name.type != TokenType::kIdentifier) {
        throw std::runtime_error("expected column name after '.'");
      }
      return ColumnValueExp(ColumnName(token.value, name.value));
    }
    return ColumnValueExp(token.value);
  }
  if (token.type == TokenType::kNumeric) {
    if (token.value.find('.') != std::string::npos) {
      return ConstantValueExp(Value(std::stod(token.value)));
    }
    return ConstantValueExp(
        Value(static_cast<int64_t>(std::stoll(token.value))));
  }
  if (token.type == TokenType::kString) {
    return ConstantValueExp(Value(std::move(token.value)));
  }
  if (token.type == TokenType::kKeyword && token.value == "NULL") {
    return ConstantValueExp(Value());
  }
  if (token.type == TokenType::kKeyword && token.value == "TRUE") {
    return ConstantValueExp(Value(true));
  }
  if (token.type == TokenType::kKeyword && token.value == "FALSE") {
    return ConstantValueExp(Value(false));
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
