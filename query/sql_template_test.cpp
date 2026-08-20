/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_template.hpp"

#include <gtest/gtest.h>

#include "parser/ast.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::unique_ptr<Statement> ParseSql(std::string_view sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  EXPECT_TRUE(parsed.ok) << parsed.error;
  if (!parsed.ok) return nullptr;
  auto ast = GoogleSqlAstParser::Parse(parsed.ast);
  EXPECT_EQ(ast.GetStatus(), Status::kSuccess);
  if (!ast.HasValue()) return nullptr;
  return GoogleSqlAstVisitor::Visit(*ast.Value());
}

TEST(SqlTemplateTest, FingerprintsIgnoreLiterals) {
  const SqlTemplate left = ExtractSqlTemplate(
      "SELECT c_id FROM customer WHERE c_w_id = 1 AND c_id = 4;");
  const SqlTemplate right = ExtractSqlTemplate(
      "SELECT c_id FROM customer WHERE c_w_id = 9 AND c_id = 12;");
  EXPECT_TRUE(left.templatable);
  EXPECT_EQ(left.fingerprint, right.fingerprint);
  ASSERT_EQ(left.parameters.size(), 2);
  EXPECT_EQ(left.parameters[0], Value(1));
  EXPECT_EQ(left.parameters[1], Value(4));
  EXPECT_EQ(right.parameters[0], Value(9));
  EXPECT_EQ(right.parameters[1], Value(12));
}

TEST(SqlTemplateTest, LeavesLimitAndStringsTyped) {
  const SqlTemplate extracted = ExtractSqlTemplate(
      "SELECT c_id FROM customer WHERE c_last = 'Last#4' ORDER BY c_first "
      "LIMIT 1;");
  EXPECT_NE(extracted.fingerprint.find("LIMIT 1"), std::string::npos);
  ASSERT_EQ(extracted.parameters.size(), 1);
  EXPECT_EQ(extracted.parameters[0], Value(std::string("Last#4")));
}

TEST(SqlTemplateTest, BindsCachedSelectTree) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto original = ParseSql(
      "SELECT c_discount FROM customer WHERE c_w_id = 1 AND c_id = 4;");
  ASSERT_TRUE(original);
  const SqlTemplate templated = ExtractSqlTemplate(
      "SELECT c_discount FROM customer WHERE c_w_id = 8 AND c_id = 9;");
  auto bound = BindStatementLiterals(*original, templated.parameters);
  ASSERT_EQ(bound->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_TRUE(select.WhereClause());
  EXPECT_NE(select.WhereClause()->ToString().find("8"), std::string::npos);
  EXPECT_NE(select.WhereClause()->ToString().find("9"), std::string::npos);
  EXPECT_EQ(select.WhereClause()->ToString().find("c_w_id = 1"),
            std::string::npos);
}

TEST(SqlTemplateTest, BindsCompositeStockLookup) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto original = ParseSql(
      "SELECT s_quantity, s_data FROM stock WHERE s_w_id = 1 AND s_i_id = 2;");
  ASSERT_TRUE(original);
  const SqlTemplate templated = ExtractSqlTemplate(
      "SELECT s_quantity, s_data FROM stock WHERE s_w_id = 1 AND s_i_id = 7;");
  auto bound = BindStatementLiterals(*original, templated.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  EXPECT_EQ(select.WhereClause()->ToString(),
            "((s_w_id = 1) AND (s_i_id = 7))");
}

TEST(SqlTemplateTest, BindsDateLiteralsPreservingTheDateType) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto original =
      ParseSql("SELECT d FROM t WHERE d <= date '1998-09-18';");
  ASSERT_TRUE(original);
  const SqlTemplate templated =
      ExtractSqlTemplate("SELECT d FROM t WHERE d <= date '1994-01-01';");
  ASSERT_TRUE(templated.templatable);
  auto bound = BindStatementLiterals(*original, templated.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_TRUE(select.WhereClause());
  const auto& binary = select.WhereClause()->AsBinaryExpression();
  const Value date = binary.Right()->AsConstantValue().GetValue();
  EXPECT_EQ(date.type, ValueType::kDate);
  EXPECT_EQ(date.AsString(), "1994-01-01");
}

TEST(SqlTemplateTest, ExtractsNegativeLiteralsWithoutTheSign) {
  const SqlTemplate negative = ExtractSqlTemplate("SELECT -5, -5.5;");
  ASSERT_EQ(negative.parameters.size(), 2);
  EXPECT_EQ(negative.parameters[0], Value(5));
  EXPECT_EQ(negative.parameters[1], Value(5.5));
  const SqlTemplate same_shape = ExtractSqlTemplate("SELECT -7, -1.25;");
  EXPECT_EQ(negative.fingerprint, same_shape.fingerprint);
}

TEST(SqlTemplateTest, BindsNegativeLiteralWithoutDoubleNegation) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto original = ParseSql("SELECT -5;");
  ASSERT_TRUE(original);
  const SqlTemplate templated = ExtractSqlTemplate("SELECT -7;");
  auto bound = BindStatementLiterals(*original, templated.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_EQ(select.SelectList().size(), 1);
  EXPECT_EQ(select.SelectList()[0].expression->ToString(), "(-7)");
}

}  // namespace
}  // namespace tinylamb

namespace tinylamb {
namespace {

TEST(SqlTemplateTest, ExtractsEscapedQuotesInsideStringLiterals) {
  // A doubled '' inside a quoted literal is one quote character, not the end
  // of the literal (sql_template.cpp escaped-quote handling).
  const SqlTemplate extracted = ExtractSqlTemplate("SELECT 'it''s';");
  ASSERT_TRUE(extracted.templatable);
  ASSERT_EQ(extracted.parameters.size(), 1u);
  EXPECT_EQ(extracted.parameters[0], Value("it's"));
  EXPECT_EQ(extracted.fingerprint, "SELECT '?';");
}

TEST(SqlTemplateTest, PreservesLineCommentsInFingerprint) {
  // "--" comments are copied verbatim into the fingerprint so that two
  // queries differing only by a trailing comment stay distinct.
  const SqlTemplate extracted =
      ExtractSqlTemplate("SELECT 1 -- trailing note\n;");
  EXPECT_EQ(extracted.fingerprint, "SELECT 0 -- trailing note\n;");
  ASSERT_EQ(extracted.parameters.size(), 1u);
  EXPECT_EQ(extracted.parameters[0], Value(1));
}

TEST(SqlTemplateTest, PreservesBlockCommentsInFingerprint) {
  const SqlTemplate extracted = ExtractSqlTemplate("SELECT 1 /* block note */;");
  EXPECT_EQ(extracted.fingerprint, "SELECT 0 /* block note */;");
  ASSERT_EQ(extracted.parameters.size(), 1u);
  EXPECT_EQ(extracted.parameters[0], Value(1));
}

TEST(SqlTemplateTest, ExtractsFloatExponentLiterals) {
  const SqlTemplate extracted = ExtractSqlTemplate("SELECT 1e3, 2.5e-2, 3E+4;");
  ASSERT_TRUE(extracted.templatable);
  ASSERT_EQ(extracted.parameters.size(), 3u);
  EXPECT_EQ(extracted.parameters[0], Value(1000.0));
  EXPECT_EQ(extracted.parameters[1], Value(0.025));
  EXPECT_EQ(extracted.parameters[2], Value(30000.0));
  EXPECT_EQ(extracted.fingerprint, "SELECT 0.0, 0.0, 0.0;");
}

TEST(SqlTemplateTest, BindStatementLiteralsParameterUnderflowThrows) {
  // Two constants in the cached tree but only one parameter supplied.
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k"))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(1))),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(2)))));
  EXPECT_THROW(BindStatementLiterals(*statement, {Value(9)}),
               std::runtime_error);
}

TEST(SqlTemplateTest, BindStatementLiteralsPreservesAliases) {
  // BindSelect copies the alias table into the bound statement.
  auto statement = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k"))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1))));
  statement->AddAlias("alias_t", "t");
  auto bound = BindStatementLiterals(*statement, {Value(7)});
  ASSERT_EQ(bound->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  EXPECT_EQ(select.WhereClause()->ToString(), "(k = 7)");
  const auto& aliases = select.Aliases();
  const auto found = aliases.find("alias_t");
  ASSERT_NE(found, aliases.end());
  EXPECT_EQ(found->second, "t");
}

TEST(SqlTemplateTest, BindStatementLiteralsRejectsDdl) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto create = ParseSql("CREATE TABLE t (a INT64);");
  ASSERT_TRUE(create);
  EXPECT_THROW(BindStatementLiterals(*create, {}), std::runtime_error);
}

TEST(SqlTemplateTest, BindStatementLiteralsParameterCountMismatchThrows) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto statement = ParseSql("SELECT 1;");
  ASSERT_TRUE(statement);
  EXPECT_THROW(BindStatementLiterals(*statement, {Value(5), Value(6)}),
               std::runtime_error);
}

}  // namespace
}  // namespace tinylamb
