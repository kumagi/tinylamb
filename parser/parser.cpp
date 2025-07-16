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

#include "parser/parser.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <vector>

#include "parser/ast.hpp"
#include "parser/token.hpp"
#include "type/value_type.hpp"

#include "expression/constant_value.hpp"

#include "common/log_message.hpp"

namespace tinylamb {

Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}

std::unique_ptr<Statement> Parser::Parse() {
  Token token = Peek();
  if (token.type == TokenType::kKeyword) {
    if (token.value == "CREATE") {
      return ParseCreateTable();
    } else if (token.value == "DROP") {
      return ParseDropTable();
    } else if (token.value == "INSERT") {
      return ParseInsert();
    } else if (token.value == "SELECT") {
      return ParseSelect();
    }
  }
  throw std::runtime_error("Unsupported statement");
}

std::unique_ptr<Statement> Parser::ParseInsert() {
  Advance();  // INSERT
  Advance();  // INTO
  std::string table_name = Advance().value;
  Advance();  // VALUES
  Expect(TokenType::kLParen);
  std::vector<std::vector<Expression>> values;
  while (Peek().type != TokenType::kSemicolon) {
    std::vector<Expression> row;
    while (Peek().type != TokenType::kRParen) {
      row.push_back(ParseExpression(0));
      if (Peek().type == TokenType::kComma) {
        Advance();
      }
    }
    values.push_back(row);
    Expect(TokenType::kRParen);
    if (Peek().type == TokenType::kComma) {
      Advance();
      Expect(TokenType::kLParen);
    }
  }
  Expect(TokenType::kSemicolon);
  return std::make_unique<InsertStatement>(table_name, values);
}

std::unique_ptr<Statement> Parser::ParseSelect() {
  Advance();  // SELECT
  std::vector<NamedExpression> select_list;
  if (Peek().type == TokenType::kOperator && Peek().value == "*") {
    Advance();
    select_list.emplace_back("", ColumnValueExp("*"));
  } else {
    while (Peek().type != TokenType::kKeyword || Peek().value != "FROM") {
      select_list.emplace_back("", ParseExpression(0));
      if (Peek().type == TokenType::kComma) {
        Advance();
      }
    }
  }
  Expect(TokenType::kKeyword);
  Advance();  // FROM
  std::vector<std::string> from_clause;
  while (Peek().type != TokenType::kKeyword || Peek().value != "WHERE") {
    from_clause.push_back(Advance().value);
    if (Peek().type == TokenType::kComma) {
      Advance();
    }
  }
  Expect(TokenType::kKeyword);
  Advance();  // WHERE
  Expression where_clause = ParseExpression(0);
  Expect(TokenType::kSemicolon);
  return std::make_unique<SelectStatement>(select_list, from_clause,
                                           where_clause);
}


std::unique_ptr<Statement> Parser::ParseDropTable() {
  Advance();  // DROP
  Advance();  // TABLE
  std::string table_name = Advance().value;
  Expect(TokenType::kSemicolon);
  return std::make_unique<DropTableStatement>(table_name);
}

std::unique_ptr<Statement> Parser::ParseCreateTable() {
  Advance();  // CREATE
  Advance();  // TABLE
  std::string table_name = Advance().value;
  Expect(TokenType::kLParen);
  std::vector<Column> columns;
  while (Peek().type != TokenType::kRParen) {
    std::string column_name = Advance().value;
    std::string type_name = Advance().value;
    ValueType type;
    std::string upper_type_name;
    std::transform(type_name.begin(), type_name.end(),
                   std::back_inserter(upper_type_name), ::toupper);
    if (upper_type_name == "INT") {
      type = ValueType::kInt64;
    } else if (upper_type_name == "VARCHAR") {
      type = ValueType::kVarChar;
      Expect(TokenType::kLParen);
      Advance();
      Expect(TokenType::kRParen);
    } else if (upper_type_name == "DOUBLE") {
      type = ValueType::kDouble;
    } else {
      throw std::runtime_error("Unsupported type");
    }
    columns.emplace_back(column_name, type);
    if (Peek().type == TokenType::kComma) {
      Advance();
    }
  }
  Expect(TokenType::kRParen);
  Expect(TokenType::kSemicolon);
  return std::make_unique<CreateTableStatement>(table_name, columns);
}

int Parser::GetPrecedence() {
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

Expression Parser::ParseExpression(int precedence) {
  Expression left = ParseUnary();
  while (precedence < GetPrecedence()) {
    Token op = Advance();
    Expression right = ParseExpression(GetPrecedence());
    if (op.value == "=") {
      left = BinaryExpressionExp(left, BinaryOperation::kEquals, right);
    } else if (op.value == "!=") {
      left = BinaryExpressionExp(left, BinaryOperation::kNotEquals, right);
    } else if (op.value == "<") {
      left = BinaryExpressionExp(left, BinaryOperation::kLessThan, right);
    } else if (op.value == "<=") {
      left = BinaryExpressionExp(left, BinaryOperation::kLessThanEquals, right);
    } else if (op.value == ">") {
      left = BinaryExpressionExp(left, BinaryOperation::kGreaterThan, right);
    } else if (op.value == ">=") {
      left =
          BinaryExpressionExp(left, BinaryOperation::kGreaterThanEquals, right);
    } else if (op.value == "+") {
      left = BinaryExpressionExp(left, BinaryOperation::kAdd, right);
    } else if (op.value == "-") {
      left = BinaryExpressionExp(left, BinaryOperation::kSubtract, right);
    } else if (op.value == "*") {
      left = BinaryExpressionExp(left, BinaryOperation::kMultiply, right);
    } else if (op.value == "/") {
      left = BinaryExpressionExp(left, BinaryOperation::kDivide, right);
    } else {
      throw std::runtime_error("Unsupported expression: " + op.ToString());
    }
  }
  return left;
}

Expression Parser::ParseUnary() {
  if (Peek().type == TokenType::kOperator && Peek().value == "-") {
    Advance();
    return UnaryExpressionExp(ParseExpression(4), UnaryOperation::kMinus);
  }
  return ParsePrimary();
}

Expression Parser::ParsePrimary() {
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

Token Parser::Peek() {
  if (pos_ >= tokens_.size()) {
    return {TokenType::kEof, ""};
  }
  return tokens_[pos_];
}


Token Parser::Advance() {
  if (pos_ >= tokens_.size()) {
    return {TokenType::kEof, ""};
  }
  return tokens_[pos_++];
}

void Parser::Expect(TokenType type) {
  Token token = Advance();
  if (token.type != type) {
    throw std::runtime_error("Unexpected token");
  }
}

}  // namespace tinylamb
