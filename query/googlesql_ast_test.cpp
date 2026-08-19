/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/googlesql_ast.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "parser/ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"

namespace tinylamb {

TEST(GoogleSqlAstTest, VisitsRichQueryWithoutReparsingSql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(
      "SELECT n_name, SUM(l_quantity) AS quantity FROM nation, lineitem "
      "WHERE l_quantity BETWEEN 1 AND 10 GROUP BY n_name "
      "HAVING SUM(l_quantity) > 0 ORDER BY quantity DESC LIMIT 3;");
  ASSERT_TRUE(parsed.ok) << parsed.error;

  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  EXPECT_EQ(ast.Value()->kind, "QueryStatement");

  std::unique_ptr<Statement> statement =
      GoogleSqlAstVisitor::Visit(*ast.Value());
  ASSERT_EQ(statement->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  EXPECT_TRUE(select.RequiresRelationalEvaluation());
  EXPECT_EQ(select.Sources().size(), 2);
  EXPECT_EQ(select.GroupBy().size(), 1);
  EXPECT_TRUE(select.Having());
  EXPECT_EQ(select.OrderBy().size(), 1);
  EXPECT_EQ(select.Limit(), 3);
}

}  // namespace tinylamb
