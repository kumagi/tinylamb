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
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "parser/token.hpp"

namespace tinylamb {
namespace {

constexpr std::string_view kOperatorChars = "+-*/%<>=!";

}  // namespace

Tokenizer::Tokenizer(std::string sql) : sql_(std::move(sql)) {}

std::vector<Token> Tokenizer::Tokenize() {
  std::vector<Token> tokens;
  while (pos_ < sql_.size()) {
    SkipWhitespace();
    if (pos_ >= sql_.size()) {
      break;
    }
    tokens.push_back(NextToken());
  }
  if (!error_.empty()) {
    throw std::runtime_error(error_);
  }
  tokens.push_back({TokenType::kEof, ""});
  return tokens;
}

Token Tokenizer::NextToken() {
  char c = Peek();
  if (std::isalpha(static_cast<unsigned char>(c)) != 0 || c == '_') {
    return Keyword();
  }
  if (std::isdigit(static_cast<unsigned char>(c)) != 0) {
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
    return {.type=TokenType::kComma, .value=","};
  }
  if (c == '.') {
    Advance();
    return {.type=TokenType::kDot, .value="."};
  }
  if (c == '(') {
    Advance();
    return {.type=TokenType::kLParen, .value="("};
  }
  if (c == ')') {
    Advance();
    return {.type=TokenType::kRParen, .value=")"};
  }
  if (c == ';') {
    Advance();
    return {.type=TokenType::kSemicolon, .value=";"};
  }
  if (kOperatorChars.find(c) != std::string_view::npos) {
    return Operator();
  }
  Advance();
  return {.type=TokenType::kUnknown, .value=std::string(1, c)};
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
  while (pos_ < sql_.size() &&
         std::isspace(static_cast<unsigned char>(sql_[pos_])) != 0) {
    pos_++;
  }
}

Token Tokenizer::Identifier() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         (std::isalnum(static_cast<unsigned char>(sql_[pos_])) != 0 ||
          sql_[pos_] == '_')) {
    pos_++;
  }
  return {.type=TokenType::kIdentifier, .value=sql_.substr(start, pos_ - start)};
}

Token Tokenizer::QuotedIdentifier() {
  Advance();  // Skip the opening backtick.
  std::string value;
  bool terminated = false;
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
    terminated = true;
    break;
  }
  if (!terminated) {
    error_ = "unterminated quoted identifier";
  }
  return {.type=TokenType::kIdentifier, .value=value};
}

Token Tokenizer::Numeric() {
  size_t start = pos_;
  size_t dots = 0;
  while (pos_ < sql_.size() &&
         (std::isdigit(static_cast<unsigned char>(sql_[pos_])) != 0 ||
          sql_[pos_] == '.')) {
    if (sql_[pos_] == '.') {
      ++dots;
      if (dots > 1) {
        throw std::runtime_error("malformed numeric literal: " +
                                 sql_.substr(start));
      }
    }
    pos_++;
  }
  return {.type=TokenType::kNumeric, .value=sql_.substr(start, pos_ - start)};
}

Token Tokenizer::String() {
  Advance();  // Skip the opening quote
  std::string value;
  bool terminated = false;
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
    Advance();
    terminated = true;
    break;
  }
  if (!terminated) {
    error_ = "unterminated string literal";
  }
  return {.type=TokenType::kString, .value=value};
}

Token Tokenizer::Operator() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         kOperatorChars.find(sql_[pos_]) != std::string_view::npos) {
    pos_++;
  }
  return {.type=TokenType::kOperator, .value=sql_.substr(start, pos_ - start)};
}

Token Tokenizer::Keyword() {
  size_t start = pos_;
  while (pos_ < sql_.size() &&
         (std::isalnum(static_cast<unsigned char>(sql_[pos_])) != 0 ||
          sql_[pos_] == '_')) {
    pos_++;
  }
  std::string value = sql_.substr(start, pos_ - start);
  std::string upper_value;
  std::ranges::transform(value, std::back_inserter(upper_value),
                         [](unsigned char c) {
                           return static_cast<char>(std::toupper(c));
                         });
  static const std::unordered_set<std::string> keywords = {
      "SELECT",   "FROM",   "WHERE",   "CREATE", "DROP",   "TABLE",
      "INSERT",   "INTO",   "VALUES",  "UPDATE", "SET",    "DELETE",
      "AND",      "OR",     "NOT",     "IS",     "NULL",   "AS",
      "DISTINCT", "ORDER",  "BY",      "ASC",    "DESC",   "LIMIT",
      "OFFSET",   "NULLS",  "FIRST",   "LAST",    "JOIN",   "INNER",
      "LEFT",     "RIGHT",  "ON",
      "CASE",     "WHEN",   "THEN",    "ELSE",   "END",    "IN",
      "GROUP",    "HAVING", "PRIMARY", "KEY",    "UNIQUE", "REFERENCES",
      "DEFAULT",  "TRUE",   "FALSE"};
  if (keywords.contains(upper_value)) {
    return {.type=TokenType::kKeyword, .value=upper_value};
  }
  return {.type=TokenType::kIdentifier, .value=value};
}

}  // namespace tinylamb
