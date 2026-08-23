/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_template.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <string_view>
#include <vector>
#include <stdexcept>
#include <utility>
#include <cstdint>

#include "common/constants.hpp"
#include "expression/named_expression.hpp"
#include "expression/expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/query_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/statement.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::unique_ptr<Statement> ParseSql(std::string_view sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  EXPECT_TRUE(parsed.ok) << parsed.error;
  if (!parsed.ok) { return nullptr;
}
  auto ast = GoogleSqlAstParser::Parse(parsed.ast);
  EXPECT_EQ(ast.GetStatus(), Status::kSuccess);
  if (!ast.HasValue()) { return nullptr;
}
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
  EXPECT_NE(select.WhereClause()->ToString().find('8'), std::string::npos);
  EXPECT_NE(select.WhereClause()->ToString().find('9'), std::string::npos);
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
  ASSERT_EQ(extracted.parameters.size(), 1U);
  EXPECT_EQ(extracted.parameters[0], Value("it's"));
  EXPECT_EQ(extracted.fingerprint, "SELECT '?';");
}

TEST(SqlTemplateTest, PreservesLineCommentsInFingerprint) {
  // "--" comments are copied verbatim into the fingerprint so that two
  // queries differing only by a trailing comment stay distinct.
  const SqlTemplate extracted =
      ExtractSqlTemplate("SELECT 1 -- trailing note\n;");
  EXPECT_EQ(extracted.fingerprint, "SELECT 0 -- trailing note\n;");
  ASSERT_EQ(extracted.parameters.size(), 1U);
  EXPECT_EQ(extracted.parameters[0], Value(1));
}

TEST(SqlTemplateTest, PreservesBlockCommentsInFingerprint) {
  const SqlTemplate extracted = ExtractSqlTemplate("SELECT 1 /* block note */;");
  EXPECT_EQ(extracted.fingerprint, "SELECT 0 /* block note */;");
  ASSERT_EQ(extracted.parameters.size(), 1U);
  EXPECT_EQ(extracted.parameters[0], Value(1));
}

TEST(SqlTemplateTest, ExtractsFloatExponentLiterals) {
  const SqlTemplate extracted = ExtractSqlTemplate("SELECT 1e3, 2.5e-2, 3E+4;");
  ASSERT_TRUE(extracted.templatable);
  ASSERT_EQ(extracted.parameters.size(), 3U);
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

// Cached tree shaped like the parsed miss statement
// "SELECT a FROM t WHERE a = 1 ORDER BY b + 2".
std::shared_ptr<SelectStatement> WhereOrderByTree() {
  return std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("a", ColumnValueExp("a"))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1))),
      std::vector<SelectStatement::OrderByTerm>{
          {BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kAdd,
                               ConstantValueExp(Value(2))),
           true}});
}

TEST(SqlTemplateTest, BindsWhereBeforeOrderByInTextOrder) {
  // Regression (improvements2.md §7.1): BindSelect used to bind ORDER BY
  // before WHERE, so re-binding the cached "...WHERE a=1 ORDER BY b+2" tree
  // with parameters [7, 9] extracted from "...WHERE a=7 ORDER BY b+9"
  // executed "WHERE a=9 ORDER BY b+7".  Parameters are extracted in text
  // order, so binding must consume WHERE's parameter first.
  const SqlTemplate hit =
      ExtractSqlTemplate("SELECT a FROM t WHERE a = 7 ORDER BY b + 9;");
  ASSERT_EQ(hit.parameters.size(), 2U);
  EXPECT_EQ(hit.parameters[0], Value(7));
  EXPECT_EQ(hit.parameters[1], Value(9));
  auto bound = BindStatementLiterals(*WhereOrderByTree(), hit.parameters);
  ASSERT_EQ(bound->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_TRUE(select.WhereClause());
  EXPECT_EQ(select.WhereClause()->ToString(), "(a = 7)");
  ASSERT_EQ(select.OrderBy().size(), 1U);
  EXPECT_TRUE(select.OrderBy()[0].ascending);
  EXPECT_EQ(select.OrderBy()[0].expression->ToString(), "(b + 9)");
}

TEST(SqlTemplateTest, RepeatedBindsOfSameParametersStayStable) {
  // Same-fingerprint re-execution with identical literals must produce the
  // identical tree every time (no double swap).
  const SqlTemplate hit =
      ExtractSqlTemplate("SELECT a FROM t WHERE a = 7 ORDER BY b + 9;");
  auto first = BindStatementLiterals(*WhereOrderByTree(), hit.parameters);
  auto second = BindStatementLiterals(*WhereOrderByTree(), hit.parameters);
  const auto& one = dynamic_cast<const SelectStatement&>(*first);
  const auto& two = dynamic_cast<const SelectStatement&>(*second);
  ASSERT_TRUE(one.WhereClause());
  ASSERT_TRUE(two.WhereClause());
  EXPECT_EQ(one.WhereClause()->ToString(), "(a = 7)");
  EXPECT_EQ(one.WhereClause()->ToString(), two.WhereClause()->ToString());
  ASSERT_EQ(one.OrderBy().size(), 1U);
  ASSERT_EQ(two.OrderBy().size(), 1U);
  EXPECT_EQ(one.OrderBy()[0].expression->ToString(), "(b + 9)");
  EXPECT_EQ(one.OrderBy()[0].expression->ToString(),
            two.OrderBy()[0].expression->ToString());
}

TEST(SqlTemplateTest, BindsJoinConditionBeforeWhereClause) {
  // Regression (§7.1): JOIN ON conditions appear before WHERE in the text;
  // binding WHERE first swapped their parameters, executing
  // "ON ja=1 ... WHERE wb=2" as "ON ja=2 ... WHERE wb=1".
  auto cached = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("v", ColumnValueExp("v"))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(ColumnValueExp("wb"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))));
  std::vector<SelectSource> sources;
  sources.push_back(SelectSource{.table="t", .alias="t", .query=nullptr, .join_type=JoinType::kCross, .join_condition=nullptr});
  sources.push_back(SelectSource{
      .table="u", .alias="u", .query=nullptr, .join_type=JoinType::kInner,
      .join_condition=BinaryExpressionExp(ColumnValueExp("ja"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1)))});
  cached->SetSources(std::move(sources));

  const SqlTemplate hit =
      ExtractSqlTemplate("SELECT v FROM t JOIN u ON ja = 11 WHERE wb = 22;");
  ASSERT_EQ(hit.parameters.size(), 2U);
  EXPECT_EQ(hit.parameters[0], Value(11));
  EXPECT_EQ(hit.parameters[1], Value(22));
  auto bound = BindStatementLiterals(*cached, hit.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_EQ(select.Sources().size(), 2U);
  ASSERT_TRUE(select.Sources()[1].join_condition);
  EXPECT_EQ(select.Sources()[1].join_condition->ToString(), "(ja = 11)");
  ASSERT_TRUE(select.WhereClause());
  EXPECT_EQ(select.WhereClause()->ToString(), "(wb = 22)");
}

TEST(SqlTemplateTest, BindsGroupingClausesBeforeOrderBy) {
  // Text order is GROUP BY -> HAVING -> ORDER BY; the old bind order
  // (ORDER BY before GROUP BY/HAVING) consumed ORDER BY's parameter first
  // and threw an underflow on "...HAVING k > 30 ORDER BY k + 40".
  auto cached = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k"))},
      std::vector<std::string>{"t"}, Expression{},
      std::vector<SelectStatement::OrderByTerm>{
          {BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kAdd,
                               ConstantValueExp(Value(4))),
           true}});
  cached->SetGroupBy(std::vector<Expression>{ColumnValueExp("k")});
  cached->SetHaving(BinaryExpressionExp(ColumnValueExp("k"),
                                        BinaryOperation::kGreaterThan,
                                        ConstantValueExp(Value(3))));

  const SqlTemplate hit = ExtractSqlTemplate(
      "SELECT k FROM t GROUP BY k HAVING k > 30 ORDER BY k + 40;");
  ASSERT_EQ(hit.parameters.size(), 2U);
  EXPECT_EQ(hit.parameters[0], Value(30));
  EXPECT_EQ(hit.parameters[1], Value(40));
  auto bound = BindStatementLiterals(*cached, hit.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_EQ(select.GroupBy().size(), 1U);
  EXPECT_EQ(select.GroupBy()[0]->ToString(), "k");
  ASSERT_TRUE(select.Having());
  EXPECT_EQ(select.Having()->ToString(), "(k > 30)");
  ASSERT_EQ(select.OrderBy().size(), 1U);
  EXPECT_EQ(select.OrderBy()[0].expression->ToString(), "(k + 40)");
}

TEST(SqlTemplateTest, RebindsParsedTreeWithoutSwappingClauses) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto miss = ParseSql("SELECT a FROM t WHERE a = 1 ORDER BY b + 2;");
  ASSERT_TRUE(miss);
  const SqlTemplate hit =
      ExtractSqlTemplate("SELECT a FROM t WHERE a = 7 ORDER BY b + 9;");
  auto bound = BindStatementLiterals(*miss, hit.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_TRUE(select.WhereClause());
  EXPECT_EQ(select.WhereClause()->ToString(), "(a = 7)");
  ASSERT_EQ(select.OrderBy().size(), 1U);
  EXPECT_EQ(select.OrderBy()[0].expression->ToString(), "(b + 9)");
}

TEST(SqlTemplateTest, BindsWithQueriesBeforeMainSelect) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  auto miss = ParseSql("WITH w AS (SELECT 1 AS x) SELECT x + 2 FROM w;");
  ASSERT_TRUE(miss);
  const SqlTemplate hit =
      ExtractSqlTemplate("WITH w AS (SELECT 9 AS x) SELECT x + 8 FROM w;");
  ASSERT_EQ(hit.parameters.size(), 2U);
  EXPECT_EQ(hit.parameters[0], Value(9));
  EXPECT_EQ(hit.parameters[1], Value(8));
  auto bound = BindStatementLiterals(*miss, hit.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_EQ(select.WithQueries().count("w"), 1U);
  const auto& w = *select.WithQueries().at("w");
  ASSERT_EQ(w.SelectList().size(), 1U);
  EXPECT_EQ(w.SelectList()[0].expression->ToString(), "9");
  ASSERT_EQ(select.SelectList().size(), 1U);
  EXPECT_EQ(select.SelectList()[0].expression->ToString(), "(x + 8)");
}

TEST(SqlTemplateTest, FingerprintIgnoresQuoteInsideQuotedIdentifier) {
  // Regression (improvements2.md §7.5): the apostrophe inside the quoted
  // identifier used to start a bogus string literal, polluting both the
  // fingerprint and the parameter list.
  const SqlTemplate extracted =
      ExtractSqlTemplate("SELECT \"it's\" FROM t WHERE a = 1;");
  ASSERT_TRUE(extracted.templatable);
  ASSERT_EQ(extracted.parameters.size(), 1U);
  EXPECT_EQ(extracted.parameters[0], Value(1));
  EXPECT_EQ(extracted.fingerprint, "SELECT \"it's\" FROM t WHERE a = 0;");
  const SqlTemplate other =
      ExtractSqlTemplate("SELECT \"it's\" FROM t WHERE a = 2;");
  EXPECT_EQ(extracted.fingerprint, other.fingerprint);
  const SqlTemplate doubled = ExtractSqlTemplate(R"(SELECT "a""b";)");
  EXPECT_EQ(doubled.fingerprint, "SELECT \"a\"\"b\";");
  EXPECT_TRUE(doubled.parameters.empty());
}

TEST(SqlTemplateTest, FingerprintIgnoresQuoteInsideBacktickIdentifier) {
  const SqlTemplate extracted =
      ExtractSqlTemplate("SELECT `it's` FROM t WHERE b = 5;");
  ASSERT_TRUE(extracted.templatable);
  ASSERT_EQ(extracted.parameters.size(), 1U);
  EXPECT_EQ(extracted.parameters[0], Value(5));
  EXPECT_EQ(extracted.fingerprint, "SELECT `it's` FROM t WHERE b = 0;");
}

TEST(SqlTemplateTest, FingerprintHandlesDollarQuotedStrings) {
  const SqlTemplate plain = ExtractSqlTemplate("SELECT $$it's$$;");
  ASSERT_EQ(plain.parameters.size(), 1U);
  EXPECT_EQ(plain.parameters[0], Value("it's"));
  EXPECT_EQ(plain.fingerprint, "SELECT '?';");
  const SqlTemplate tagged = ExtractSqlTemplate("SELECT $n$it's$n$;");
  ASSERT_EQ(tagged.parameters.size(), 1U);
  EXPECT_EQ(tagged.parameters[0], Value("it's"));
  EXPECT_EQ(tagged.fingerprint, "SELECT '?';");
  const SqlTemplate other = ExtractSqlTemplate("SELECT $n$other$n$;");
  EXPECT_EQ(other.fingerprint, plain.fingerprint);
  EXPECT_NE(other.parameters[0], plain.parameters[0]);
}

TEST(SqlTemplateTest, FingerprintIgnoresQuotesInsideComments) {
  // Quote characters inside -- or /* */ comments must not open string
  // literals; comments stay verbatim in the fingerprint.
  const SqlTemplate extracted =
      ExtractSqlTemplate("SELECT 1 /* don't */ FROM t WHERE a = 2 -- won't\n;");
  ASSERT_EQ(extracted.parameters.size(), 2U);
  EXPECT_EQ(extracted.parameters[0], Value(1));
  EXPECT_EQ(extracted.parameters[1], Value(2));
  EXPECT_EQ(extracted.fingerprint,
            "SELECT 0 /* don't */ FROM t WHERE a = 0 -- won't\n;");
}

TEST(SqlTemplateTest, FingerprintSeparatesIdentifierFromLiteralShapes) {
  // Same parameter count but different identifiers must not collide.
  const SqlTemplate quoted =
      ExtractSqlTemplate("SELECT \"it's\" FROM t WHERE a = 1;");
  const SqlTemplate bare = ExtractSqlTemplate("SELECT its FROM t WHERE a = 1;");
  ASSERT_EQ(quoted.parameters.size(), bare.parameters.size());
  EXPECT_NE(quoted.fingerprint, bare.fingerprint);
}

TEST(SqlTemplateTest, BindsInSubqueryTestBeforeSubqueryBody) {
  // Regression (§7.1): inside "11 IN (SELECT ... u.k = 22)" the tested
  // literal appears in the text before the subquery body; binding the
  // subquery first executed "22 IN (... u.k = 11)".
  auto inner = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("j", ColumnValueExp("j"))},
      std::vector<std::string>{"u"},
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(2))));
  auto cached = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("x", ColumnValueExp("x"))},
      std::vector<std::string>{"t"},
      QueryExpressionExp(inner, ConstantValueExp(Value(1)), false, false));

  const SqlTemplate hit = ExtractSqlTemplate(
      "SELECT x FROM t WHERE 11 IN (SELECT j FROM u WHERE u.k = 22);");
  ASSERT_EQ(hit.parameters.size(), 2U);
  EXPECT_EQ(hit.parameters[0], Value(11));
  EXPECT_EQ(hit.parameters[1], Value(22));
  auto bound = BindStatementLiterals(*cached, hit.parameters);
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  ASSERT_TRUE(select.WhereClause());
  ASSERT_EQ(select.WhereClause()->Type(), TypeTag::kQueryExp);
  const auto& query_expression =
      select.WhereClause()->AsQueryExpression();
  ASSERT_TRUE(query_expression.Test());
  EXPECT_EQ(query_expression.Test()->ToString(), "11");
  const auto& subquery = *query_expression.Query();
  ASSERT_TRUE(subquery.WhereClause());
  EXPECT_EQ(subquery.WhereClause()->ToString(), "(k = 22)");
}

std::shared_ptr<SelectStatement> LiteralCteBody(const char* column,
                                                int64_t literal) {
  return std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(column,
                                                   ColumnValueExp(column))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(ColumnValueExp(column), BinaryOperation::kEquals,
                          ConstantValueExp(Value(literal))));
}

TEST(SqlTemplateTest, MultiWithWithLiteralsRefusesTemplateBinding) {
  // WithQueries() is an unordered_map: the declaration order of two or more
  // CTE bodies is unrecoverable, so binding them positionally could swap
  // literals between bodies. Such statements must refuse template binding
  // (the engine falls back to parsing verbatim) instead of risking a swap.
  auto cached = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k"))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(3))));
  cached->AddWithQuery("zeta", LiteralCteBody("x", 1));
  cached->AddWithQuery("alpha", LiteralCteBody("y", 2));
  // Exact parameter count so only the ordering guard can throw.
  EXPECT_THROW(BindStatementLiterals(*cached, {Value(7), Value(8), Value(9)}),
               std::runtime_error);
}

TEST(SqlTemplateTest, BindsMultipleLiteralFreeWithQueries) {
  // Without literals the WITH bind order cannot mis-assign anything, so the
  // statement stays template-bindable.
  auto plain_body = [](const char* column) {
    return std::make_shared<SelectStatement>(
        std::vector<NamedExpression>{
            NamedExpression(column, ColumnValueExp(column))},
        std::vector<std::string>{"t"}, Expression{});
  };
  auto cached = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression("k", ColumnValueExp("k"))},
      std::vector<std::string>{"t"},
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(1))));
  cached->AddWithQuery("zeta", plain_body("x"));
  cached->AddWithQuery("alpha", plain_body("y"));
  auto bound = BindStatementLiterals(*cached, {Value(42)});
  const auto& select = dynamic_cast<const SelectStatement&>(*bound);
  EXPECT_EQ(select.WithQueries().size(), 2U);
  ASSERT_TRUE(select.WhereClause());
  EXPECT_EQ(select.WhereClause()->ToString(), "(k = 42)");
}

}  // namespace
}  // namespace tinylamb
