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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "expression/binary_expression.hpp"
#include "parser/token.hpp"
#include "parser/tokenizer.hpp"

namespace tinylamb {

namespace {

// Runs `action` with a watchdog thread that aborts the process if the action
// does not finish within 2 seconds. Used to turn parser infinite loops into
// test failures instead of hanging the whole test binary forever.
template <typename F>
void RunWithHangWatchdog(F&& action) {
  std::atomic<bool> done{false};
  std::thread watchdog([&done] {
    for (int i = 0; i < 200 && !done.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!done.load()) {
      std::abort();
    }
  });
  try {
    action();
  } catch (...) {
    done.store(true);
    watchdog.join();
    throw;
  }
  done.store(true);
  watchdog.join();
}

}  // namespace

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

TEST(ParserTest, Update) {
  // Arrange -- tokenize UPDATE with two SET assignments and a WHERE clause
  Tokenizer tokenizer(
      "UPDATE users SET name = 'foo', score = 1.5 WHERE id = 1;");
  // Act -- parse into UpdateStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, table name, assignments and predicate survive
  ASSERT_EQ(stmt->Type(), StatementType::kUpdate);
  const auto& update = dynamic_cast<UpdateStatement&>(*stmt);
  ASSERT_EQ(update.TableName(), "users");
  ASSERT_EQ(update.SetClause().size(), 2);
  ASSERT_EQ(update.SetClause()[0].first.name, "name");
  ASSERT_EQ(update.SetClause()[1].first.name, "score");
  ASSERT_NE(update.WhereClause(), nullptr);
  ASSERT_EQ(update.WhereClause()->Type(), TypeTag::kBinaryExp);
}

TEST(ParserTest, UpdateWithoutWhere) {
  // Arrange -- tokenize UPDATE without a WHERE clause
  Tokenizer tokenizer("UPDATE users SET name = 'foo';");
  // Act -- parse into UpdateStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the where clause is left null
  ASSERT_EQ(stmt->Type(), StatementType::kUpdate);
  const auto& update = dynamic_cast<UpdateStatement&>(*stmt);
  ASSERT_EQ(update.WhereClause(), nullptr);
}

TEST(ParserTest, DeleteWithFrom) {
  // Arrange -- tokenize DELETE FROM with a WHERE clause
  Tokenizer tokenizer("DELETE FROM users WHERE id = 3;");
  // Act -- parse into DeleteStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- statement type, table name, predicate
  ASSERT_EQ(stmt->Type(), StatementType::kDelete);
  const auto& del = dynamic_cast<DeleteStatement&>(*stmt);
  ASSERT_EQ(del.TableName(), "users");
  ASSERT_NE(del.WhereClause(), nullptr);
}

TEST(ParserTest, DeleteWithoutFromAndWhere) {
  // Arrange -- tokenize DELETE with neither FROM nor WHERE
  Tokenizer tokenizer("DELETE users;");
  // Act -- parse into DeleteStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the bare table name is accepted and no predicate is parsed
  ASSERT_EQ(stmt->Type(), StatementType::kDelete);
  const auto& del = dynamic_cast<DeleteStatement&>(*stmt);
  ASSERT_EQ(del.TableName(), "users");
  ASSERT_EQ(del.WhereClause(), nullptr);
}

TEST(ParserTest, InsertWithColumnsAndMultipleRows) {
  // Arrange -- tokenize INSERT with a column list and two value tuples
  Tokenizer tokenizer(
      "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b');");
  // Act -- parse into InsertStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the explicit column list and both tuples are preserved
  ASSERT_EQ(stmt->Type(), StatementType::kInsert);
  const auto& insert = dynamic_cast<InsertStatement&>(*stmt);
  ASSERT_EQ(insert.TableName(), "users");
  ASSERT_EQ(insert.Columns().size(), 2);
  ASSERT_EQ(insert.Columns()[0], "id");
  ASSERT_EQ(insert.Columns()[1], "name");
  ASSERT_EQ(insert.Values().size(), 2);
  ASSERT_EQ(insert.Values()[0].size(), 2);
  ASSERT_EQ(insert.Values()[1].size(), 2);
}

TEST(ParserTest, SelectDistinct) {
  // Arrange -- tokenize SELECT DISTINCT
  Tokenizer tokenizer("SELECT DISTINCT id FROM users;");
  // Act -- parse into SelectStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the distinct flag is set
  ASSERT_EQ(stmt->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_TRUE(select.Distinct());
  ASSERT_EQ(select.SelectList().size(), 1);
}

TEST(ParserTest, SelectWithAliases) {
  // Arrange -- tokenize SELECT with an output alias and a table alias
  Tokenizer tokenizer("SELECT id AS uid FROM users AS u;");
  // Act -- parse into SelectStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- both aliases are captured
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.SelectList()[0].name, "uid");
  ASSERT_EQ(select.Aliases().at("u"), "users");
}

TEST(ParserTest, SelectInnerJoin) {
  // Arrange -- tokenize SELECT with an INNER JOIN and WHERE
  Tokenizer tokenizer(
      "SELECT t1.x FROM t1 INNER JOIN t2 ON t1.x = t2.x WHERE t1.y = 1;");
  // Act -- parse into SelectStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- both tables appear in the FROM clause and WHERE is combined
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 2);
  ASSERT_EQ(select.FromClause()[0], "t1");
  ASSERT_EQ(select.FromClause()[1], "t2");
  ASSERT_NE(select.WhereClause(), nullptr);
}

TEST(ParserTest, SelectWhereAnd) {
  // Arrange -- tokenize a WHERE with two AND'd terms
  Tokenizer tokenizer("SELECT * FROM users WHERE a = 1 AND b = 2;");
  // Act -- parse into SelectStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the predicate is a binary AND of both comparisons
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_NE(select.WhereClause(), nullptr);
  ASSERT_EQ(select.WhereClause()->Type(), TypeTag::kBinaryExp);
  const auto& be = select.WhereClause()->AsBinaryExpression();
  ASSERT_EQ(be.Op(), BinaryOperation::kAnd);
  ASSERT_EQ(be.Left()->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(be.Right()->Type(), TypeTag::kBinaryExp);
}

TEST(ParserTest, SelectWhereInSubquery) {
  // Arrange -- tokenize a WHERE with an IN (SELECT ...) subquery
  Tokenizer tokenizer(
      "SELECT * FROM users WHERE id IN (SELECT id FROM other);");
  // Act -- parse into SelectStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the subquery's table is appended to the FROM clause
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.FromClause().size(), 2);
  ASSERT_EQ(select.FromClause()[1], "other");
  ASSERT_NE(select.WhereClause(), nullptr);
}

TEST(ParserTest, SelectOrderByMultipleTerms) {
  // Arrange -- tokenize ORDER BY with two terms, one descending
  Tokenizer tokenizer(
      "SELECT id FROM users ORDER BY id ASC, name DESC LIMIT 5;");
  // Act -- parse into SelectStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- both terms and the limit survive
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  ASSERT_EQ(select.OrderBy().size(), 2);
  ASSERT_TRUE(select.OrderBy()[0].ascending);
  ASSERT_FALSE(select.OrderBy()[1].ascending);
  ASSERT_EQ(select.Limit(), 5);
}

TEST(ParserTest, CreateTableTypeAliases) {
  // Arrange -- tokenize CREATE TABLE exercising several type aliases
  Tokenizer tokenizer(
      "CREATE TABLE t (a INTEGER, b BIGINT, c BOOL, d CHAR(10), "
      "e TIMESTAMP, f DATETIME, g STRING, h FLOAT, i FLOAT64, "
      "j DECIMAL, k NUMERIC(4, 4), l DATE);");
  // Act -- parse into CreateTableStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- every alias maps to the canonical ValueType
  const auto& create = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create.Columns().size(), 12);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(create.Columns()[i].Type(), ValueType::kInt64);
  }
  for (size_t i = 3; i < 7; ++i) {
    ASSERT_EQ(create.Columns()[i].Type(), ValueType::kVarChar);
  }
  for (size_t i = 7; i < 11; ++i) {
    ASSERT_EQ(create.Columns()[i].Type(), ValueType::kDouble);
  }
  ASSERT_EQ(create.Columns()[11].Type(), ValueType::kDate);
}

TEST(ParserTest, CreateTableWithUniqueTableConstraint) {
  // Arrange -- tokenize CREATE TABLE with a table-level UNIQUE constraint
  Tokenizer tokenizer(
      "CREATE TABLE t (a INT, b INT, UNIQUE (a, b));");
  // Act -- parse into CreateTableStatement
  Parser parser(tokenizer.Tokenize());
  std::unique_ptr<Statement> stmt = parser.Parse();

  // Assert -- the constraint is skipped and both columns are kept
  const auto& create = dynamic_cast<CreateTableStatement&>(*stmt);
  ASSERT_EQ(create.Columns().size(), 2);
}

TEST(ParserTest, ParseErrors) {
  // Arrange + Act + Assert -- malformed statements throw runtime_error
  EXPECT_THROW(Parser(Tokenizer("GRANT ALL ON t;").Tokenize()).Parse(),
               std::runtime_error);
  EXPECT_THROW(Parser(Tokenizer("CREATE TABLE t (a BLOB);").Tokenize()).Parse(),
               std::runtime_error);
  EXPECT_THROW(Parser(Tokenizer("INSERT INTO users 42;").Tokenize()).Parse(),
               std::runtime_error);
  EXPECT_THROW(Parser(Tokenizer("UPDATE users name = 'x';").Tokenize()).Parse(),
               std::runtime_error);
}

// Bug: an unterminated INSERT column list runs forever.
// ParseInsert()'s column-list loop (parser/parser.cpp:63-68) only exits on
// TokenType::kRParen.  Once the trailing EOF sentinel is reached,
// Parser::Advance() (parser/parser.cpp:352-357) returns {kEof, ""} without
// advancing pos_, so `while (Peek() != kRParen)` spins forever, appending
// empty strings to `columns` until it exhausts memory.  The fuzzer observed
// this as malloc(2147483648) / OOM followed by a timeout.
TEST(ParserTest, InsertColumnListMustTerminateAtEof) {
  // Arrange -- tokenize INSERT with an unterminated column list
  const std::string sql = "INSERT INTO users (id";
  // Act + Assert -- parsing must throw, not loop forever at end of input
  RunWithHangWatchdog([&sql] {
    EXPECT_THROW(Parser(Tokenizer(sql).Tokenize()).Parse(),
                 std::runtime_error);
  });
}

// Bug: CREATE TABLE with a column but no closing ')' loops forever.
// The per-column constraint skip loop (parser/parser.cpp:329-335) repeats
// until a Comma/RParen at depth 0; at end of input Peek() is forever Eof and
// Advance() never moves pos_, so the loop never exits.
TEST(ParserTest, CreateTableColumnConstraintMustTerminateAtEof) {
  // Arrange -- tokenize CREATE TABLE with a column type but no closing ')'
  const std::string sql = "CREATE TABLE users (id INT";
  // Act + Assert -- parsing must throw, not loop forever at end of input
  RunWithHangWatchdog([&sql] {
    EXPECT_THROW(Parser(Tokenizer(sql).Tokenize()).Parse(),
                 std::runtime_error);
  });
}

// Bug: CREATE TABLE with a dangling PRIMARY/UNIQUE constraint loops forever.
// The table-level constraint skip loop (parser/parser.cpp:287-292) has the
// same EOF defect: at end of input Advance() returns Eof without advancing,
// so `while (!(depth == 0 && (Comma || RParen)))` never terminates.
TEST(ParserTest, CreateTableTableConstraintMustTerminateAtEof) {
  // Arrange -- tokenize CREATE TABLE with a dangling PRIMARY constraint
  const std::string sql = "CREATE TABLE users (PRIMARY";
  // Act + Assert -- parsing must throw, not loop forever at end of input
  RunWithHangWatchdog([&sql] {
    EXPECT_THROW(Parser(Tokenizer(sql).Tokenize()).Parse(),
                 std::runtime_error);
  });
}

// Bug: a NUMERIC/DECIMAL/FLOAT type with '(' but no closing ')' loops forever.
// The type-suffix skip loop (parser/parser.cpp:321-324) repeats
// `while (Peek() != kRParen) Advance();`; at end of input Advance() returns
// Eof without moving pos_, so the loop never exits.
TEST(ParserTest, CreateTableNumericTypeMustTerminateAtEof) {
  // Arrange -- tokenize CREATE TABLE with a NUMERIC type and no closing ')'
  const std::string sql = "CREATE TABLE users (score NUMERIC(";
  // Act + Assert -- parsing must throw, not loop forever at end of input
  RunWithHangWatchdog([&sql] {
    EXPECT_THROW(Parser(Tokenizer(sql).Tokenize()).Parse(),
                 std::runtime_error);
  });
}

}  // namespace tinylamb
