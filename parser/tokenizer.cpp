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

#include "parser/tokenizer.hpp"

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>
#include <vector>

#include "parser/token.hpp"

namespace tinylamb {

Tokenizer::Tokenizer(const std::string& sql) : sql_(sql) {}

std::vector<Token> Tokenizer::Tokenize() {
  std::vector<Token> tokens;
  while (pos_ < sql_.size()) {
    SkipWhitespace();
    if (pos_ >= sql_.size()) {
      break;
    }
    tokens.push_back(NextToken());
  }
  tokens.push_back({TokenType::kEof, ""});
  return tokens;
}

Token Tokenizer::NextToken() {
  char c = Peek();
  if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
    return Keyword();
  }
  if (std::isdigit(c)) {
    return Numeric();
  }
  if (c == '\'') {
    return String();
  }
  if (c == '`') {
    return QuotedIdentifier();
  }
  if (c == ',') {
    Advance();
    return {TokenType::kComma, ","};
  }
  if (c == '.') {
    Advance();
    return {TokenType::kDot, "."};
  }
  if (c == '(') {
    Advance();
    return {TokenType::kLParen, "("};
  }
  if (c == ')') {
    Advance();
    return {TokenType::kRParen, ")"};
  }
  if (c == ';') {
    Advance();
    return {TokenType::kSemicolon, ";"};
  }
  if (std::string("+-*/%<>=!").find(c) != std::string::npos) {
    return Operator();
  }
  Advance();
  return {TokenType::kUnknown, std::string(1, c)};
}

char Tokenizer::Peek() {
  if (pos_ >= sql_.size()) {
    return '\0';
  }
  return sql_[pos_];
}

char Tokenizer::Advance() {
  if (pos_ >= sql_.size()) {
    return '\0';
  }
  return sql_[pos_++];
}

void Tokenizer::SkipWhitespace() {
  while (pos_ < sql_.size() && std::isspace(sql_[pos_])) {
    pos_++;
  }
}

Token Tokenizer::Identifier() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         (std::isalnum(sql_[pos_]) || sql_[pos_] == '_')) {
    pos_++;
  }
  return {TokenType::kIdentifier, sql_.substr(start, pos_ - start)};
}

Token Tokenizer::QuotedIdentifier() {
  Advance();  // Skip the opening backtick.
  std::string value;
  while (pos_ < sql_.size()) {
    if (sql_[pos_] != '`') {
      value.push_back(sql_[pos_++]);
      continue;
    }
    if (pos_ + 1 < sql_.size() && sql_[pos_ + 1] == '`') {
      value.push_back('`');
      pos_ += 2;
      continue;
    }
    Advance();
    break;
  }
  return {TokenType::kIdentifier, value};
}

Token Tokenizer::Numeric() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         (std::isdigit(sql_[pos_]) || sql_[pos_] == '.')) {
    pos_++;
  }
  return {TokenType::kNumeric, sql_.substr(start, pos_ - start)};
}

Token Tokenizer::String() {
  Advance();  // Skip the opening quote
  std::string value;
  while (pos_ < sql_.size()) {
    if (sql_[pos_] != '\'') {
      value.push_back(sql_[pos_++]);
      continue;
    }
    if (pos_ + 1 < sql_.size() && sql_[pos_ + 1] == '\'') {
      value.push_back('\'');
      pos_ += 2;
      continue;
    }
    break;
  }
  Advance();  // Skip the closing quote
  return {TokenType::kString, value};
}

Token Tokenizer::Operator() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         std::string("+-*/%<>=!").find(sql_[pos_]) != std::string::npos) {
    pos_++;
  }
  return {TokenType::kOperator, sql_.substr(start, pos_ - start)};
}

Token Tokenizer::Keyword() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         (std::isalnum(sql_[pos_]) || sql_[pos_] == '_')) {
    pos_++;
  }
  std::string value = sql_.substr(start, pos_ - start);
  std::string upper_value;
  std::transform(value.begin(), value.end(), std::back_inserter(upper_value),
                 ::toupper);
  static const std::vector<std::string> keywords = {
      "SELECT",   "FROM",   "WHERE",   "CREATE", "DROP",   "TABLE",
      "INSERT",   "INTO",   "VALUES",  "UPDATE", "SET",    "DELETE",
      "AND",      "OR",     "NOT",     "IS",     "NULL",   "AS",
      "DISTINCT", "ORDER",  "BY",      "ASC",    "DESC",   "LIMIT",
      "OFFSET",   "JOIN",   "INNER",   "LEFT",   "RIGHT",  "ON",
      "CASE",     "WHEN",   "THEN",    "ELSE",   "END",    "IN",
      "GROUP",    "HAVING", "PRIMARY", "KEY",    "UNIQUE", "REFERENCES",
      "DEFAULT",  "TRUE",   "FALSE"};
  if (std::find(keywords.begin(), keywords.end(), upper_value) !=
      keywords.end()) {
    return {TokenType::kKeyword, upper_value};
  }
  return {TokenType::kIdentifier, value};
}

}  // namespace tinylamb
