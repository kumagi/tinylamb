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
  // Arrange -- tokenize empty string
  Tokenizer tokenizer("");

  // Act -- tokenize
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- empty input yields single EOF token
  ASSERT_EQ(tokens.size(), 1);
  ASSERT_EQ(tokens[0].type, TokenType::kEof);
}

TEST(TokenizerTest, Select) {
  // Arrange -- tokenize SELECT * FROM with WHERE clause
  Tokenizer tokenizer("SELECT * FROM users WHERE id = 1;");

  // Act -- tokenize
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- 10 tokens: SELECT, *, FROM, users, WHERE, id, =, 1, ;, EOF
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
  // Arrange -- tokenize CREATE TABLE statement with 3 columns
  Tokenizer tokenizer(
      "CREATE TABLE users (id INT, name VARCHAR(20), score DOUBLE);");

  // Act -- tokenize
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- 18 tokens: CREATE, TABLE, users, (, id, INT, ,, name, VARCHAR, (,
  // 20, ), ,, score, DOUBLE, ), ;, EOF
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
  // Arrange -- tokenize INSERT INTO statement with 3 values
  Tokenizer tokenizer("INSERT INTO users VALUES (1, 'foo', 1.2);");

  // Act -- tokenize
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- 13 tokens: INSERT, INTO, users, VALUES, (, 1, ,, foo, ,, 1.2, ),
  // ;, EOF
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

TEST(TokenizerTest, GoogleSqlQuotedIdentifier) {
  Tokenizer tokenizer("SELECT `CURRENT_TIMESTAMP`(), `customer`.`c_id`;");

  std::vector<Token> tokens = tokenizer.Tokenize();

  ASSERT_EQ(tokens[1].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[1].value, "CURRENT_TIMESTAMP");
  ASSERT_EQ(tokens[5].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[5].value, "customer");
  ASSERT_EQ(tokens[7].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[7].value, "c_id");
}

TEST(TokenizerTest, UnknownCharacterStillAdvances) {
  Tokenizer tokenizer("?");

  std::vector<Token> tokens = tokenizer.Tokenize();

  ASSERT_EQ(tokens.size(), 2);
  EXPECT_EQ(tokens[0].type, TokenType::kUnknown);
  EXPECT_EQ(tokens[1].type, TokenType::kEof);
}

TEST(TokenizerTest, StringEscapesAndKeywordCase) {
  // Act -- tokenize lowercase keywords, a doubled-quote escape, and an escaped
  // backtick identifier
  Tokenizer tokenizer("SELECT 'it''s';\nselect * from `a``b`");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- the escaped quote collapses into one string token
  ASSERT_EQ(tokens.size(), 8);
  ASSERT_EQ(tokens[0].type, TokenType::kKeyword);
  EXPECT_EQ(tokens[0].value, "SELECT");
  ASSERT_EQ(tokens[1].type, TokenType::kString);
  EXPECT_EQ(tokens[1].value, "it's");
  ASSERT_EQ(tokens[2].type, TokenType::kSemicolon);
  // Assert -- lowercase keywords are normalized to upper case
  ASSERT_EQ(tokens[3].type, TokenType::kKeyword);
  EXPECT_EQ(tokens[3].value, "SELECT");
  ASSERT_EQ(tokens[4].type, TokenType::kOperator);
  EXPECT_EQ(tokens[4].value, "*");
  ASSERT_EQ(tokens[5].type, TokenType::kKeyword);
  EXPECT_EQ(tokens[5].value, "FROM");
  // Assert -- doubled backticks escape a single backtick in an identifier
  ASSERT_EQ(tokens[6].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[6].value, "a`b");
  ASSERT_EQ(tokens[7].type, TokenType::kEof);
}

TEST(TokenizerTest, NumbersAndOperators) {
  // Act -- tokenize decimals, dotted numerics, exponent-like numbers, and runs
  // of operator characters
  Tokenizer tokenizer("1.5 1.2.3 .5 1e5 <= >= <> != ++ --");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- decimal and multi-dot numerics stay in one numeric token
  ASSERT_EQ(tokens.size(), 13);
  EXPECT_EQ(tokens[0].type, TokenType::kNumeric);
  EXPECT_EQ(tokens[0].value, "1.5");
  EXPECT_EQ(tokens[1].type, TokenType::kNumeric);
  EXPECT_EQ(tokens[1].value, "1.2.3");
  // Assert -- a leading dot is a Dot token followed by the fractional part
  EXPECT_EQ(tokens[2].type, TokenType::kDot);
  EXPECT_EQ(tokens[3].type, TokenType::kNumeric);
  EXPECT_EQ(tokens[3].value, "5");
  // Assert -- exponent notation splits into numeric + identifier
  EXPECT_EQ(tokens[4].type, TokenType::kNumeric);
  EXPECT_EQ(tokens[4].value, "1");
  EXPECT_EQ(tokens[5].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[5].value, "e5");
  // Assert -- each operator run is grouped into a single token
  EXPECT_EQ(tokens[6].type, TokenType::kOperator);
  EXPECT_EQ(tokens[6].value, "<=");
  EXPECT_EQ(tokens[7].type, TokenType::kOperator);
  EXPECT_EQ(tokens[7].value, ">=");
  EXPECT_EQ(tokens[8].type, TokenType::kOperator);
  EXPECT_EQ(tokens[8].value, "<>");
  EXPECT_EQ(tokens[9].type, TokenType::kOperator);
  EXPECT_EQ(tokens[9].value, "!=");
  EXPECT_EQ(tokens[10].type, TokenType::kOperator);
  EXPECT_EQ(tokens[10].value, "++");
  EXPECT_EQ(tokens[11].type, TokenType::kOperator);
  EXPECT_EQ(tokens[11].value, "--");
  EXPECT_EQ(tokens[12].type, TokenType::kEof);
}

TEST(TokenizerTest, WhitespaceCommentsAndUnknowns) {
  // Act -- tokenize a statement broken up by tabs/newlines/CRLF plus comment-
  // looking input and unknown characters
  Tokenizer tokenizer("\t\n  SELECT  id,\tname \r\n;  -- foo /* x */ ? @ #");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Assert -- whitespace between tokens is ignored entirely
  ASSERT_EQ(tokens.size(), 14);
  EXPECT_EQ(tokens[0].type, TokenType::kKeyword);
  EXPECT_EQ(tokens[0].value, "SELECT");
  EXPECT_EQ(tokens[1].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[1].value, "id");
  EXPECT_EQ(tokens[2].type, TokenType::kComma);
  EXPECT_EQ(tokens[3].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[3].value, "name");
  EXPECT_EQ(tokens[4].type, TokenType::kSemicolon);
  // Assert -- the tokenizer has no comment lexing: -- and /* are operators
  EXPECT_EQ(tokens[5].type, TokenType::kOperator);
  EXPECT_EQ(tokens[5].value, "--");
  EXPECT_EQ(tokens[6].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[6].value, "foo");
  EXPECT_EQ(tokens[7].type, TokenType::kOperator);
  EXPECT_EQ(tokens[7].value, "/*");
  EXPECT_EQ(tokens[8].type, TokenType::kIdentifier);
  EXPECT_EQ(tokens[8].value, "x");
  EXPECT_EQ(tokens[9].type, TokenType::kOperator);
  EXPECT_EQ(tokens[9].value, "*/");
  // Assert -- unknown characters yield Unknown tokens without stalling
  EXPECT_EQ(tokens[10].type, TokenType::kUnknown);
  EXPECT_EQ(tokens[10].value, "?");
  EXPECT_EQ(tokens[11].type, TokenType::kUnknown);
  EXPECT_EQ(tokens[11].value, "@");
  EXPECT_EQ(tokens[12].type, TokenType::kUnknown);
  EXPECT_EQ(tokens[12].value, "#");
  EXPECT_EQ(tokens[13].type, TokenType::kEof);
}

}  // namespace tinylamb
