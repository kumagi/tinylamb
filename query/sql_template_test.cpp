/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_template.hpp"

#include <gtest/gtest.h>

#include "parser/ast.hpp"
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

}  // namespace
}  // namespace tinylamb
