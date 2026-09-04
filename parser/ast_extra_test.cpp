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

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "expression/binary_expression.hpp"
#include "query/statement.hpp"
#include "parser/parser.hpp"
#include "parser/pratt_parser.hpp"
#include "parser/tokenizer.hpp"

namespace tinylamb {

namespace {

bool Contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

std::unique_ptr<Statement> ParseSql(const std::string& sql) {
  return Parser(Tokenizer(sql).Tokenize()).Parse();
}

std::string DumpStatement(const Statement& statement) {
  std::ostringstream out;
  out << statement;
  return out.str();
}

}  // namespace

// Targets query/statement.hpp:96-105 (CreateTableStatement::Dump)
TEST(AstDumpTest, CreateTableDump) {
  // Arrange -- tokenize CREATE TABLE with three columns
  std::unique_ptr<Statement> stmt = ParseSql(
      "CREATE TABLE users (id INT, name VARCHAR(20), score DOUBLE);");

  // Act -- dump the statement through Statement::operator<<
  std::string dump = DumpStatement(*stmt);

  // Assert -- table name, column list and ", " separators appear
  EXPECT_EQ(stmt->Type(), StatementType::kCreateTable);
  EXPECT_TRUE(Contains(dump, "CreateTable table=users columns=["));
  EXPECT_TRUE(Contains(dump, "id: Integer"));
  EXPECT_TRUE(Contains(dump, ", "));
  EXPECT_TRUE(Contains(dump, "]"));
}

// Targets query/statement.hpp:119 (DropTableStatement::Dump)
TEST(AstDumpTest, DropTableDump) {
  // Arrange -- tokenize DROP TABLE
  std::unique_ptr<Statement> stmt = ParseSql("DROP TABLE users;");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- the full dump matches
  EXPECT_EQ(stmt->Type(), StatementType::kDropTable);
  EXPECT_EQ(dump, "DropTable table=users");
}

// Targets query/statement.hpp:189-206 (SelectStatement::Dump with WHERE)
TEST(AstDumpTest, SelectDumpWithWhere) {
  // Arrange -- tokenize SELECT with two items, two tables and a WHERE clause
  std::unique_ptr<Statement> stmt =
      ParseSql("SELECT id, name FROM users, orders WHERE id = 1;");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- select list, from list and where clause are dumped
  EXPECT_EQ(stmt->Type(), StatementType::kSelect);
  EXPECT_TRUE(Contains(dump, "Select select=[id, name]"));
  EXPECT_TRUE(Contains(dump, "from=[users, orders]"));
  EXPECT_TRUE(Contains(dump, "where=(id = 1)"));
}

// Targets query/statement.hpp:207-208 (SelectStatement::Dump null WHERE branch)
TEST(AstDumpTest, SelectDumpWithoutWhere) {
  // Arrange -- tokenize SELECT without WHERE
  std::unique_ptr<Statement> stmt = ParseSql("SELECT id FROM users;");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- the where clause prints as "(null)"
  EXPECT_EQ(stmt->Type(), StatementType::kSelect);
  EXPECT_TRUE(Contains(dump, "select=[id]"));
  EXPECT_TRUE(Contains(dump, "from=[users]"));
  EXPECT_TRUE(Contains(dump, "where=(null)"));
}

// Targets query/statement.hpp:242-258 (InsertStatement::Dump)
TEST(AstDumpTest, InsertDump) {
  // Arrange -- tokenize INSERT with a column list and two value tuples
  std::unique_ptr<Statement> stmt = ParseSql(
      "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b');");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- table name, both tuples and "; " row separator appear
  EXPECT_EQ(stmt->Type(), StatementType::kInsert);
  EXPECT_TRUE(Contains(dump, "Insert table=users values=["));
  EXPECT_TRUE(Contains(dump, "(1, "));
  EXPECT_TRUE(Contains(dump, ", "));
  EXPECT_TRUE(Contains(dump, "; ("));
  EXPECT_TRUE(Contains(dump, "]"));
}

// Targets query/statement.hpp:281-291 (UpdateStatement::Dump with WHERE)
TEST(AstDumpTest, UpdateDump) {
  // Arrange -- tokenize UPDATE with two assignments and a WHERE clause
  std::unique_ptr<Statement> stmt =
      ParseSql("UPDATE users SET name = 'a', score = 1.5 WHERE id = 1;");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- table name, both assignments and where clause appear
  EXPECT_EQ(stmt->Type(), StatementType::kUpdate);
  EXPECT_TRUE(Contains(dump, "Update table=users set=["));
  EXPECT_TRUE(Contains(dump, "name = "));
  EXPECT_TRUE(Contains(dump, "score = "));
  EXPECT_TRUE(Contains(dump, ", "));
  EXPECT_TRUE(Contains(dump, "where=(id = 1)"));
}

// Targets query/statement.hpp:292-293 (UpdateStatement::Dump null WHERE branch)
TEST(AstDumpTest, UpdateDumpWithoutWhere) {
  // Arrange -- tokenize UPDATE without WHERE
  std::unique_ptr<Statement> stmt = ParseSql("UPDATE users SET name = 'a';");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- the where clause prints as "(null)"
  EXPECT_EQ(stmt->Type(), StatementType::kUpdate);
  EXPECT_TRUE(Contains(dump, "set=[name = "));
  EXPECT_TRUE(Contains(dump, "where=(null)"));
}

// Targets query/statement.hpp:312-315 (DeleteStatement::Dump with WHERE)
TEST(AstDumpTest, DeleteDumpWithWhere) {
  // Arrange -- tokenize DELETE with a WHERE clause
  std::unique_ptr<Statement> stmt = ParseSql("DELETE FROM users WHERE id = 3;");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- table name and where clause appear
  EXPECT_EQ(stmt->Type(), StatementType::kDelete);
  EXPECT_TRUE(Contains(dump, "Delete table=users where="));
  EXPECT_TRUE(Contains(dump, "(id = 3)"));
}

// Targets query/statement.hpp:316-318 (DeleteStatement::Dump null WHERE branch)
TEST(AstDumpTest, DeleteDumpWithoutWhere) {
  // Arrange -- tokenize DELETE without FROM/WHERE
  std::unique_ptr<Statement> stmt = ParseSql("DELETE users;");

  // Act -- dump the statement
  std::string dump = DumpStatement(*stmt);

  // Assert -- the where clause prints as "(null)"
  EXPECT_EQ(stmt->Type(), StatementType::kDelete);
  EXPECT_TRUE(Contains(dump, "Delete table=users where=(null)"));
}

// Targets parser/parser.cpp:118 (SELECT list must end with ',' or FROM)
TEST(ParserBranchTest, SelectListRejectsTrailingIdentifier) {
  // Arrange -- tokenize SELECT with a stray identifier after the expression
  // Act + Assert -- parsing must throw
  EXPECT_THROW(ParseSql("SELECT a b FROM users;"), std::runtime_error);
}

// Targets parser/parser.cpp:130-131 (bare alias on the first FROM table)
TEST(ParserBranchTest, SelectFirstTableBareAlias) {
  // Arrange -- tokenize SELECT with a bare (non-AS) table alias
  std::unique_ptr<Statement> stmt = ParseSql("SELECT * FROM users u;");

  // Act -- inspect the alias map
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);

  // Assert -- the bare alias maps to the table name
  EXPECT_EQ(stmt->Type(), StatementType::kSelect);
  EXPECT_EQ(select.FromClause().size(), 1);
  EXPECT_EQ(select.Aliases().at("u"), "users");
}

// Targets parser/parser.cpp:134-144 (comma-separated FROM tables with
// AS/bare/no aliases)
TEST(ParserBranchTest, SelectCommaSeparatedTables) {
  // Arrange + Act + Assert -- plain comma-separated tables
  {
    std::unique_ptr<Statement> stmt = ParseSql("SELECT * FROM users, orders;");
    const auto& select = dynamic_cast<SelectStatement&>(*stmt);
    ASSERT_EQ(select.FromClause().size(), 2);
    EXPECT_EQ(select.FromClause()[0], "users");
    EXPECT_EQ(select.FromClause()[1], "orders");
  }
  // Arrange + Act + Assert -- second table with an AS alias
  {
    std::unique_ptr<Statement> stmt =
        ParseSql("SELECT * FROM users, orders AS o;");
    const auto& select = dynamic_cast<SelectStatement&>(*stmt);
    ASSERT_EQ(select.FromClause().size(), 2);
    EXPECT_EQ(select.Aliases().at("o"), "orders");
  }
  // Arrange + Act + Assert -- second table with a bare alias
  {
    std::unique_ptr<Statement> stmt = ParseSql("SELECT * FROM users, orders o;");
    const auto& select = dynamic_cast<SelectStatement&>(*stmt);
    ASSERT_EQ(select.FromClause().size(), 2);
    EXPECT_EQ(select.Aliases().at("o"), "orders");
  }
}

// Targets parser/parser.cpp:152 (INNER must be followed by JOIN)
TEST(ParserBranchTest, InnerWithoutJoinThrows) {
  // Arrange + Act + Assert -- INNER followed by a non-JOIN token throws
  EXPECT_THROW(ParseSql("SELECT * FROM users INNER x;"), std::runtime_error);
}

// Targets parser/parser.cpp:159-163 (JOIN target with a bare alias)
TEST(ParserBranchTest, JoinWithBareAlias) {
  // Arrange -- tokenize SELECT with a JOIN whose target has a bare alias
  std::unique_ptr<Statement> stmt =
      ParseSql("SELECT * FROM users JOIN orders o ON users.a = o.b;");

  // Act -- inspect aliases and the join condition
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);

  // Assert -- alias is captured and the ON condition is an equality
  EXPECT_EQ(select.Aliases().at("o"), "orders");
  ASSERT_NE(select.WhereClause(), nullptr);
  ASSERT_EQ(select.WhereClause()->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(select.WhereClause()->AsBinaryExpression().Op(),
            BinaryOperation::kEquals);
}

// Targets parser/parser.cpp:166 (JOIN target must be followed by ON)
TEST(ParserBranchTest, JoinWithoutOnThrows) {
  // Arrange + Act + Assert -- JOIN without an ON clause throws
  EXPECT_THROW(ParseSql("SELECT * FROM users JOIN orders;"),
               std::runtime_error);
}

// Targets parser/parser.cpp:188 (ORDER must be followed by BY)
TEST(ParserBranchTest, OrderWithoutByThrows) {
  // Arrange + Act + Assert -- ORDER followed by a non-BY token throws
  EXPECT_THROW(ParseSql("SELECT * FROM users ORDER x;"), std::runtime_error);
}

// Targets parser/parser.cpp:236 (UPDATE assignment requires '=')
TEST(ParserBranchTest, UpdateAssignmentRequiresEquals) {
  // Arrange + Act + Assert -- UPDATE SET without '=' throws
  EXPECT_THROW(ParseSql("UPDATE users SET a b;"), std::runtime_error);
}

// Targets parser/parser.cpp:367 (Parser::Expect mismatch throws)
TEST(ParserBranchTest, MissingSemicolonThrows) {
  // Arrange + Act + Assert -- a statement without ';' fails Expect()
  EXPECT_THROW(ParseSql("DROP TABLE users"), std::runtime_error);
}

// Targets parser/parser.cpp:431 (WHERE term that PrattParser cannot consume)
TEST(ParserBranchTest, WhereTermWithTrailingTokenThrows) {
  // Arrange + Act + Assert -- "WHERE a b" leaves a trailing token behind
  EXPECT_THROW(ParseSql("SELECT * FROM users WHERE a b;"), std::runtime_error);
}

// Targets parser/parser.cpp:439 (unterminated IN subquery)
TEST(ParserBranchTest, InSubqueryWithoutClosingParenThrows) {
  // Arrange + Act + Assert -- IN (SELECT ... without ')' throws
  EXPECT_THROW(
      ParseSql("SELECT * FROM users WHERE x IN (SELECT a FROM other;"),
      std::runtime_error);
}

// Targets parser/parser.cpp:454 (IN subquery must select exactly one column)
TEST(ParserBranchTest, InSubqueryMultipleColumnsThrows) {
  // Arrange + Act + Assert -- a two-column IN subquery is rejected
  EXPECT_THROW(
      ParseSql("SELECT * FROM users WHERE x IN (SELECT a, b FROM other);"),
      std::runtime_error);
}

// Targets parser/parser.cpp:461-463 (IN subquery with its own WHERE clause)
TEST(ParserBranchTest, InSubqueryWithWhereClause) {
  // Arrange -- tokenize a WHERE whose IN subquery carries a WHERE clause
  std::unique_ptr<Statement> stmt = ParseSql(
      "SELECT * FROM users WHERE x IN (SELECT a FROM other WHERE a > 5);");

  // Act -- inspect the combined predicate
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);

  // Assert -- the term is (x = a) AND (a > 5) and 'other' joins the FROM list
  ASSERT_EQ(select.FromClause().size(), 2);
  EXPECT_EQ(select.FromClause()[1], "other");
  ASSERT_NE(select.WhereClause(), nullptr);
  ASSERT_EQ(select.WhereClause()->Type(), TypeTag::kBinaryExp);
  const auto& and_expr = select.WhereClause()->AsBinaryExpression();
  ASSERT_EQ(and_expr.Op(), BinaryOperation::kAnd);
  ASSERT_EQ(and_expr.Left()->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(and_expr.Left()->AsBinaryExpression().Op(),
            BinaryOperation::kEquals);
  ASSERT_EQ(and_expr.Right()->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(and_expr.Right()->AsBinaryExpression().Op(),
            BinaryOperation::kGreaterThan);
}

// Targets parser/pratt_parser.cpp:132-137 (parenthesized primary expression)
TEST(PrattBranchTest, ParenthesizedExpressionInSelectList) {
  // Arrange -- tokenize SELECT (1 + 2) * 3
  std::unique_ptr<Statement> stmt = ParseSql("SELECT (1 + 2) * 3 FROM users;");

  // Act -- inspect the select-list expression
  const auto& select = dynamic_cast<SelectStatement&>(*stmt);
  const Expression& expr = select.SelectList()[0].expression;

  // Assert -- top level is multiply over the parenthesized add
  ASSERT_EQ(expr->Type(), TypeTag::kBinaryExp);
  ASSERT_EQ(expr->AsBinaryExpression().Op(), BinaryOperation::kMultiply);
  ASSERT_EQ(expr->AsBinaryExpression().Left()->Type(), TypeTag::kBinaryExp);
  EXPECT_EQ(expr->AsBinaryExpression().Left()->AsBinaryExpression().Op(),
            BinaryOperation::kAdd);
}

// Targets parser/pratt_parser.cpp:145 (CASE WHEN condition missing THEN)
TEST(PrattBranchTest, CaseMissingThenThrows) {
  // Arrange -- tokenize a CASE whose WHEN condition is not followed by THEN
  Tokenizer tokenizer("CASE WHEN a b THEN 1 END");
  std::vector<Token> tokens = tokenizer.Tokenize();

  // Act -- parse the expression
  PrattParser parser(tokens.begin(), tokens.end());

  // Assert -- the parser must reject the missing THEN
  EXPECT_THROW(parser.ParseExpression(), std::runtime_error);
}

// Targets parser/pratt_parser.cpp:243 (Advance past end of token range)
TEST(PrattBranchTest, CaseWithoutEndAdvancesPastEndOfRange) {
  // Arrange -- tokenize a WHERE whose CASE lacks END, so the WHERE-clause
  // token slice ends right after the last WHEN value
  // Act + Assert -- the final Advance() runs past the range and the parser
  // must throw instead of looping
  EXPECT_THROW(
      ParseSql("SELECT * FROM users WHERE CASE WHEN a THEN b;"),
      std::runtime_error);
}

// Targets parser/pratt_parser.cpp:251 (Expect() mismatch throws)
TEST(PrattBranchTest, ExpectFailureOnMismatchedTokenThrows) {
  // Arrange -- tokenize an unterminated parenthesized expression in WHERE
  // Act + Assert -- Expect(kRParen) hits ';' and throws
  EXPECT_THROW(ParseSql("SELECT * FROM users WHERE (1 + 2;"),
               std::runtime_error);
}

}  // namespace tinylamb
