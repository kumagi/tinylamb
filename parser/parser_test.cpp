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
  // Arrange -- tokenize CREATE TABLE statement with 3 columns
  Tokenizer tokenizer(
      "CREATE TABLE users (id INT, name VARCHAR(20), score DOUBLE);");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into CreateTableStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, table name, columns count and types match
  ASSERT_EQ(stmt->Type(), StatementType::kCreateTable);
  const auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
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
  // Arrange -- tokenize CREATE TABLE with extra whitespace between tokens
  Tokenizer tokenizer(
      "   CREATE   TABLE   users   (  id   INT  ,   name   VARCHAR  (  20  )  "
      ",   score   DOUBLE  )  ;   ");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into CreateTableStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type and table name match; whitespace ignored
  ASSERT_EQ(stmt->Type(), StatementType::kCreateTable);
  const auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create_table.TableName(), "users");
  ASSERT_EQ(create_table.Columns().size(), 3);
}

TEST(ParserTest, CreateTableWithMixedCase) {
  // Arrange -- tokenize CREATE TABLE with mixed-case keywords/identifiers
  Tokenizer tokenizer(
      "cReAtE tAbLe uSeRs (iD iNt, nAmE vArChAr(20), sCoRe dOuBlE);");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into CreateTableStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type and table name match; mixed-case accepted
  ASSERT_EQ(stmt->Type(), StatementType::kCreateTable);
  auto& create_table = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create_table.TableName(), "uSeRs");
  ASSERT_EQ(create_table.Columns().size(), 3);
}

TEST(ParserTest, DropTable) {
  // Arrange -- tokenize DROP TABLE statement
  Tokenizer tokenizer("DROP TABLE users;");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into DropTableStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type and table name match
  ASSERT_EQ(stmt->Type(), StatementType::kDropTable);
  auto& drop_table = dynamic_cast<DropTableStatement&>(*stmt);
  ASSERT_EQ(drop_table.TableName(), "users");
}

TEST(ParserTest, Insert) {
  // Arrange -- tokenize INSERT INTO statement with 3 values
  Tokenizer tokenizer("INSERT INTO users VALUES (1, 'foo', 1.2);");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into InsertStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, table name, values count and tuple size match
  ASSERT_EQ(stmt->Type(), StatementType::kInsert);
  auto& insert = dynamic_cast<InsertStatement&>(*stmt);
  ASSERT_EQ(insert.TableName(), "users");
  ASSERT_EQ(insert.Values().size(), 1);
  ASSERT_EQ(insert.Values()[0].size(), 3);
}

TEST(ParserTest, Select) {
  // Arrange -- tokenize SELECT statement with 2 columns and WHERE clause
  Tokenizer tokenizer("SELECT id, name FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into SelectStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, from-clause table, select-list size match
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 1);
  ASSERT_EQ(select.FromClause()[0], "users");
  ASSERT_EQ(select.SelectList().size(), 2);
}

TEST(ParserTest, SelectStar) {
  // Arrange -- tokenize SELECT * FROM with WHERE clause
  Tokenizer tokenizer("SELECT * FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into SelectStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, from-clause table, select-list has 1 entry with
  // "*"
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 1);
  ASSERT_EQ(select.FromClause()[0], "users");
  ASSERT_EQ(select.SelectList().size(), 1);
  ASSERT_EQ(
      select.SelectList()[0].expression->AsColumnValue().GetColumnName().name,
      "*");
}

TEST(ParserTest, SelectWithExpression) {
  // Arrange -- tokenize SELECT with expression "id + 1" and WHERE clause
  Tokenizer tokenizer("SELECT id + 1 FROM users WHERE id = 1;");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse tokens into SelectStatement
  Parser parser(tokens);
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, from-clause table, select-list has 1 entry
  // (expression)
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 1);
  ASSERT_EQ(select.FromClause()[0], "users");
  ASSERT_EQ(select.SelectList().size(), 1);
}

TEST(ParserTest, TpccDdlTypesAndConstraints) {
  Tokenizer tokenizer(
      "CREATE TABLE warehouse (w_id INT64 NOT NULL, "
      "w_tax NUMERIC(4, 4), w_name STRING(10), PRIMARY KEY(w_id));");
  Parser parser(tokenizer.Tokenize());

  std::unique_ptr<Statement> stmt = parser.Parse();

  const auto& create = dynamic_cast<const CreateTableStatement&>(*stmt);
  ASSERT_EQ(create.Columns().size(), 3);
  EXPECT_EQ(create.Columns()[0].Type(), ValueType::kInt64);
  EXPECT_EQ(create.Columns()[1].Type(), ValueType::kDouble);
  EXPECT_EQ(create.Columns()[2].Type(), ValueType::kVarChar);
}

TEST(ParserTest, TpccJoinOrderingAndLimit) {
  Tokenizer tokenizer(
      "SELECT c.c_id, o.o_id FROM customer AS c JOIN orders AS o "
      "ON c.c_id = o.o_c_id WHERE c.c_w_id = 1 ORDER BY o.o_id DESC "
      "LIMIT 1 OFFSET 2;");
  Parser parser(tokenizer.Tokenize());

  std::unique_ptr<Statement> stmt = parser.Parse();

  const auto& select = dynamic_cast<const SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 2);
  ASSERT_EQ(select.OrderBy().size(), 1);
  EXPECT_FALSE(select.OrderBy()[0].ascending);
  EXPECT_EQ(select.Limit(), 1);
  EXPECT_EQ(select.Offset(), 2);
  EXPECT_EQ(select.Aliases().at("c"), "customer");
  EXPECT_EQ(select.Aliases().at("o"), "orders");
}

}  // namespace tinylamb
