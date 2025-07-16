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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "parser/token.hpp"
#include "parser/tokenizer.hpp"

namespace tinylamb {

TEST(ParserTest, CreateTable) {
  Tokenizer tokenizer(
      "CREATE TABLE users (id INT, name VARCHAR(20), score DOUBLE);");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kCreateTable);
  auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create_table.TableName(), "users");
  ASSERT_EQ(create_table.Columns().size(), 3);
  ASSERT_EQ(create_table.Columns()[0].Name().name, "id");
  ASSERT_EQ(create_table.Columns()[0].Type(), ValueType::kInt64);
  ASSERT_EQ(create_table.Columns()[1].Name().name, "name");
  ASSERT_EQ(create_table.Columns()[1].Type(), ValueType::kVarChar);
  ASSERT_EQ(create_table.Columns()[2].Name().name, "score");
  ASSERT_EQ(create_table.Columns()[2].Type(), ValueType::kDouble);
}

TEST(ParserTest, CreateTableWithExtraWhitespace) {
  Tokenizer tokenizer(
      "   CREATE   TABLE   users   (  id   INT  ,   name   VARCHAR  (  20  )  "
      ",   score   DOUBLE  )  ;   ");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kCreateTable);
  auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create_table.TableName(), "users");
  ASSERT_EQ(create_table.Columns().size(), 3);
}

TEST(ParserTest, CreateTableWithMixedCase) {
  Tokenizer tokenizer(
      "cReAtE tAbLe uSeRs (iD iNt, nAmE vArChAr(20), sCoRe dOuBlE);");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kCreateTable);
  auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create_table.TableName(), "uSeRs");
  ASSERT_EQ(create_table.Columns().size(), 3);
}

TEST(ParserTest, DropTable) {
  Tokenizer tokenizer("DROP TABLE users;");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kDropTable);
  auto& drop_table = dynamic_cast<DropTableStatement&>(*stmt);
  ASSERT_EQ(drop_table.TableName(), "users");
}

TEST(ParserTest, Insert) {
  Tokenizer tokenizer("INSERT INTO users VALUES (1, 'foo', 1.2);");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kInsert);
  auto& insert = dynamic_cast<InsertStatement&>(*stmt);
  ASSERT_EQ(insert.TableName(), "users");
  ASSERT_EQ(insert.Values().size(), 1);
  ASSERT_EQ(insert.Values()[0].size(), 3);
}

TEST(ParserTest, Select) {
  Tokenizer tokenizer("SELECT id, name FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 1);
  ASSERT_EQ(select.FromClause()[0], "users");
  ASSERT_EQ(select.SelectList().size(), 2);
}

TEST(ParserTest, SelectStar) {
  Tokenizer tokenizer("SELECT * FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 1);
  ASSERT_EQ(select.FromClause()[0], "users");
  ASSERT_EQ(select.SelectList().size(), 1);
  ASSERT_EQ(select.SelectList()[0].expression->AsColumnValue().GetColumnName().name,
            "*");
}

TEST(ParserTest, SelectWithExpression) {
  Tokenizer tokenizer("SELECT id + 1 FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 1);
  ASSERT_EQ(select.FromClause()[0], "users");
  ASSERT_EQ(select.SelectList().size(), 1);
}

}  // namespace tinylamb
