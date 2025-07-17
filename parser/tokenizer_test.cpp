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

#include <gtest/gtest.h>

#include <vector>

#include "parser/token.hpp"

namespace tinylamb {

TEST(TokenizerTest, Empty) {
  Tokenizer tokenizer("");
  std::vector<Token> tokens = tokenizer.Tokenize();
  ASSERT_EQ(tokens.size(), 1);
  ASSERT_EQ(tokens[0].type, TokenType::kEof);
}

TEST(TokenizerTest, Select) {
  Tokenizer tokenizer("SELECT * FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();
  ASSERT_EQ(tokens.size(), 10);
  ASSERT_EQ(tokens[0].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[0].value, "SELECT");
  ASSERT_EQ(tokens[1].type, TokenType::kOperator);
  ASSERT_EQ(tokens[1].value, "*");
  ASSERT_EQ(tokens[2].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[2].value, "FROM");
  ASSERT_EQ(tokens[3].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[3].value, "users");
  ASSERT_EQ(tokens[4].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[4].value, "WHERE");
  ASSERT_EQ(tokens[5].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[5].value, "id");
  ASSERT_EQ(tokens[6].type, TokenType::kOperator);
  ASSERT_EQ(tokens[6].value, "=");
  ASSERT_EQ(tokens[7].type, TokenType::kNumeric);
  ASSERT_EQ(tokens[7].value, "1");
  ASSERT_EQ(tokens[8].type, TokenType::kSemicolon);
  ASSERT_EQ(tokens[9].type, TokenType::kEof);
}

TEST(TokenizerTest, Create) {
  Tokenizer tokenizer(
      "CREATE TABLE users (id INT, name VARCHAR(20), score DOUBLE);");
  std::vector<Token> tokens = tokenizer.Tokenize();
  ASSERT_EQ(tokens.size(), 18);
  ASSERT_EQ(tokens[0].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[0].value, "CREATE");
  ASSERT_EQ(tokens[1].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[1].value, "TABLE");
  ASSERT_EQ(tokens[2].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[2].value, "users");
  ASSERT_EQ(tokens[3].type, TokenType::kLParen);
  ASSERT_EQ(tokens[4].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[4].value, "id");
  ASSERT_EQ(tokens[5].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[5].value, "INT");
  ASSERT_EQ(tokens[6].type, TokenType::kComma);
  ASSERT_EQ(tokens[7].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[7].value, "name");
  ASSERT_EQ(tokens[8].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[8].value, "VARCHAR");
  ASSERT_EQ(tokens[9].type, TokenType::kLParen);
  ASSERT_EQ(tokens[10].type, TokenType::kNumeric);
  ASSERT_EQ(tokens[10].value, "20");
  ASSERT_EQ(tokens[11].type, TokenType::kRParen);
  ASSERT_EQ(tokens[12].type, TokenType::kComma);
  ASSERT_EQ(tokens[13].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[13].value, "score");
  ASSERT_EQ(tokens[14].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[14].value, "DOUBLE");
  ASSERT_EQ(tokens[15].type, TokenType::kRParen);
  ASSERT_EQ(tokens[16].type, TokenType::kSemicolon);
  ASSERT_EQ(tokens[16].value, ";");
  ASSERT_EQ(tokens[17].type, TokenType::kEof);
}

TEST(TokenizerTest, Insert) {
  Tokenizer tokenizer("INSERT INTO users VALUES (1, 'foo', 1.2);");
  std::vector<Token> tokens = tokenizer.Tokenize();
  ASSERT_EQ(tokens.size(), 13);
  ASSERT_EQ(tokens[0].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[0].value, "INSERT");
  ASSERT_EQ(tokens[1].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[1].value, "INTO");
  ASSERT_EQ(tokens[2].type, TokenType::kIdentifier);
  ASSERT_EQ(tokens[2].value, "users");
  ASSERT_EQ(tokens[3].type, TokenType::kKeyword);
  ASSERT_EQ(tokens[3].value, "VALUES");
  ASSERT_EQ(tokens[4].type, TokenType::kLParen);
  ASSERT_EQ(tokens[5].type, TokenType::kNumeric);
  ASSERT_EQ(tokens[5].value, "1");
  ASSERT_EQ(tokens[6].type, TokenType::kComma);
  ASSERT_EQ(tokens[7].type, TokenType::kString);
  ASSERT_EQ(tokens[7].value, "foo");
  ASSERT_EQ(tokens[8].type, TokenType::kComma);
  ASSERT_EQ(tokens[9].type, TokenType::kNumeric);
  ASSERT_EQ(tokens[9].value, "1.2");
  ASSERT_EQ(tokens[10].type, TokenType::kRParen);
  ASSERT_EQ(tokens[11].type, TokenType::kSemicolon);
  ASSERT_EQ(tokens[11].value, ";");
  ASSERT_EQ(tokens[12].type, TokenType::kEof);
}

}  // namespace tinylamb