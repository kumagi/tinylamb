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

#include <memory>
#include <stdexcept>
#include <vector>

#include "parser/ast.hpp"
#include "parser/token.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}

std::unique_ptr<Statement> Parser::Parse() {
  Token token = Peek();
  if (token.type == TokenType::kKeyword) {
    if (token.value == "CREATE") {
      return ParseCreateTable();
    } else if (token.value == "DROP") {
      return ParseDropTable();
    }
  }
  throw std::runtime_error("Unsupported statement");
}

std::unique_ptr<Statement> Parser::ParseDropTable() {
  Advance();  // DROP
  Expect(TokenType::kKeyword);
  Advance();  // TABLE
  std::string table_name = Advance().value;
  Expect(TokenType::kSemicolon);
  return std::make_unique<DropTableStatement>(table_name);
}

std::unique_ptr<Statement> Parser::ParseCreateTable() {
  Advance();  // CREATE
  Expect(TokenType::kKeyword);
  Advance();  // TABLE
  std::string table_name = Advance().value;
  Expect(TokenType::kLParen);
  std::vector<Column> columns;
  while (Peek().type != TokenType::kRParen) {
    std::string column_name = Advance().value;
    std::string type_name = Advance().value;
    ValueType type;
    if (type_name == "INT") {
      type = ValueType::kInt64;
    } else if (type_name == "VARCHAR") {
      type = ValueType::kVarChar;
    } else if (type_name == "DOUBLE") {
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
