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

#ifndef TINYLAMB_TOKEN_HPP
#define TINYLAMB_TOKEN_HPP

#include <string>
#include <string_view>

namespace tinylamb {

enum class TokenType {
  kUnknown,
  kIdentifier,
  kNumeric,
  kString,
  kComma,
  kLParen,
  kRParen,
  kSemicolon,
  kOperator,
  kKeyword,
  kEof,
};

struct Token {
  TokenType type;
  std::string value;

  [[nodiscard]] std::string ToString() const {
    switch (type) {
      case TokenType::kIdentifier:
        return "Identifier<" + value + ">";
      case TokenType::kNumeric:
        return "Numeric<" + value + ">";
      case TokenType::kString:
        return "String<" + value + ">";
      case TokenType::kComma:
        return "Comma";
      case TokenType::kLParen:
        return "LParen";
      case TokenType::kRParen:
        return "RParen";
      case TokenType::kSemicolon:
        return "Semicolon";
      case TokenType::kOperator:
        return "Operator<" + value + ">";
      case TokenType::kKeyword:
        return "Keyword<" + value + ">";
      case TokenType::kEof:
        return "Eof";
      case TokenType::kUnknown:
        return "Unknown<" + value + ">";
    }
    return "INVALID_TOKEN_TYPE";
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_TOKEN_HPP
