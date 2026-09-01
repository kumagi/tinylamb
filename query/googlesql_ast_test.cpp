/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/googlesql_ast.hpp"

#include <gtest/gtest.h>

#include <exception>
#include <array>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/status_or.hpp"
#include "common/constants.hpp"
#include "expression/array_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/in_expression.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

std::unique_ptr<Statement> VisitSql(std::string_view sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  EXPECT_TRUE(parsed.ok) << parsed.error;
  if (!parsed.ok) { return nullptr;
}
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  EXPECT_EQ(ast.GetStatus(), Status::kSuccess);
  if (!ast.HasValue()) { return nullptr;
}
  return GoogleSqlAstVisitor::Visit(*ast.Value());
}

std::unique_ptr<Statement> VisitSqlOrThrow(std::string_view sql) {
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  EXPECT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  EXPECT_EQ(ast.GetStatus(), Status::kSuccess);
  if (!ast.HasValue()) { return nullptr;
}
  return GoogleSqlAstVisitor::Visit(*ast.Value());
}

}  // namespace

TEST(GoogleSqlAstTest, PreservesNestedSetOperationKinds) {
  const std::string sql =
      "(SELECT 1 UNION ALL SELECT 1) INTERSECT DISTINCT SELECT 1;";
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  ASSERT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  std::unique_ptr<Statement> statement =
      GoogleSqlAstVisitor::Visit(*ast.Value(), sql);
  ASSERT_NE(statement, nullptr);
  const auto* select = dynamic_cast<const SelectStatement*>(statement.get());
  ASSERT_NE(select, nullptr);
  ASSERT_EQ(select->SetOperationKinds().size(), 2U);
  EXPECT_EQ(select->SetOperationKinds()[0], SetOperationKind::kUnionAll);
  EXPECT_EQ(select->SetOperationKinds()[1], SetOperationKind::kIntersect);
  ASSERT_NE(select->GetSetOperationTree(), nullptr);
  EXPECT_TRUE(select->GetSetOperationTree()->grouped);
}

TEST(GoogleSqlAstTest, PreservesExplicitNullOrdering) {
  const std::string sql =
      "SELECT x FROM nls ORDER BY x ASC NULLS LAST, x DESC NULLS FIRST;";
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  ASSERT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  std::unique_ptr<Statement> statement =
      GoogleSqlAstVisitor::Visit(*ast.Value(), sql);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.OrderBy().size(), 2U);
  ASSERT_TRUE(select.OrderBy()[0].nulls_first.has_value());
  EXPECT_FALSE(*select.OrderBy()[0].nulls_first);
  ASSERT_TRUE(select.OrderBy()[1].nulls_first.has_value());
  EXPECT_TRUE(*select.OrderBy()[1].nulls_first);
}

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

TEST(GoogleSqlAstTest, SelectStarAndAllLiteralKinds) {
  auto statement = VisitSql(
      "SELECT *, 1, 1.5, TRUE, FALSE, NULL, DATE '2020-01-01', 'hello' "
      "FROM t;");
  ASSERT_TRUE(statement);
  ASSERT_EQ(statement->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 8);
  EXPECT_EQ(select.FromClause().size(), 1);
  EXPECT_EQ(select.FromClause()[0], "t");

  EXPECT_EQ(select.SelectList()[0].expression->Type(), TypeTag::kColumnValue);
  EXPECT_EQ(select.SelectList()[0].expression->ToString(), "*");
  EXPECT_EQ(select.SelectList()[1].expression->ToString(), "1");
  EXPECT_DOUBLE_EQ(select.SelectList()[2]
                       .expression->AsConstantValue()
                       .GetValue()
                       .value.double_value,
                   1.5);
  EXPECT_EQ(select.SelectList()[3].expression->AsConstantValue().GetValue(),
            Value(true));
  EXPECT_EQ(select.SelectList()[4].expression->AsConstantValue().GetValue(),
            Value(false));
  EXPECT_TRUE(select.SelectList()[5]
                  .expression->AsConstantValue()
                  .GetValue()
                  .IsNull());
  EXPECT_EQ(select.SelectList()[6].expression->AsConstantValue().GetValue(),
            Value::Date("2020-01-01"));
  EXPECT_EQ(select.SelectList()[7].expression->AsConstantValue().GetValue(),
            Value("hello"));
}

TEST(GoogleSqlAstTest, WhereClauseOperatorForms) {
  auto statement = VisitSql(
      "SELECT * FROM t WHERE a LIKE 'x%' AND b NOT LIKE 'y%' AND c IS NULL "
      "AND d IS NOT NULL AND e NOT BETWEEN 1 AND 2 AND f IN (1, 2, 3) AND "
      "g NOT IN (4, 5);");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_TRUE(select.WhereClause());
  const std::string where = select.WhereClause()->ToString();

  EXPECT_NE(where.find("LIKE"), std::string::npos) << where;
  EXPECT_NE(where.find("NOT LIKE"), std::string::npos) << where;
  EXPECT_NE(where.find("IS NULL"), std::string::npos) << where;
  EXPECT_NE(where.find("IS NOT NULL"), std::string::npos) << where;
  EXPECT_NE(where.find("IN (1, 2, 3)"), std::string::npos) << where;
  EXPECT_NE(where.find("NOT"), std::string::npos) << where;
}

TEST(GoogleSqlAstTest, NotBetweenAndInListSemantics) {
  auto not_between = VisitSql(
      "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 2;");
  ASSERT_TRUE(not_between);
  const auto& not_between_select =
      dynamic_cast<const SelectStatement&>(*not_between);
  ASSERT_TRUE(not_between_select.WhereClause());
  EXPECT_EQ(not_between_select.WhereClause()->Type(), TypeTag::kUnaryExp);

  auto in_list = VisitSql("SELECT * FROM t WHERE a IN (1, 2, 3);");
  ASSERT_TRUE(in_list);
  const auto& in_select = dynamic_cast<const SelectStatement&>(*in_list);
  ASSERT_TRUE(in_select.WhereClause());
  EXPECT_EQ(in_select.WhereClause()->Type(), TypeTag::kInExp);
  EXPECT_EQ(in_select.WhereClause()->ToString(), "a IN (1, 2, 3)");
  EXPECT_EQ(in_select.WhereClause()->AsInExpression().list_.size(), 3);
}

TEST(GoogleSqlAstTest, InSubqueryAndScalarSubquery) {
  auto in_subquery = VisitSql(
      "SELECT * FROM t WHERE a IN (SELECT b FROM t2);");
  ASSERT_TRUE(in_subquery);
  const auto& in_select = dynamic_cast<const SelectStatement&>(*in_subquery);
  ASSERT_TRUE(in_select.WhereClause());
  EXPECT_EQ(in_select.WhereClause()->Type(), TypeTag::kQueryExp);
  EXPECT_TRUE(in_select.RequiresRelationalEvaluation());

  auto scalar = VisitSql("SELECT (SELECT 1);");
  ASSERT_TRUE(scalar);
  const auto& scalar_select = dynamic_cast<const SelectStatement&>(*scalar);
  ASSERT_EQ(scalar_select.SelectList().size(), 1);
  EXPECT_EQ(scalar_select.SelectList()[0].expression->Type(),
            TypeTag::kQueryExp);

  auto exists = VisitSql("SELECT * FROM t WHERE EXISTS (SELECT 1);");
  ASSERT_TRUE(exists);
  const auto& exists_select = dynamic_cast<const SelectStatement&>(*exists);
  ASSERT_TRUE(exists_select.WhereClause());
  EXPECT_EQ(exists_select.WhereClause()->Type(), TypeTag::kQueryExp);
  EXPECT_TRUE(exists_select.RequiresRelationalEvaluation());
}

TEST(GoogleSqlAstTest, CaseExtractIntervalAndUnary) {
  auto case_stmt = VisitSql(
      "SELECT CASE WHEN a > 1 THEN 'big' ELSE 'small' END;");
  ASSERT_TRUE(case_stmt);
  const auto& case_select = dynamic_cast<const SelectStatement&>(*case_stmt);
  ASSERT_EQ(case_select.SelectList().size(), 1);
  EXPECT_EQ(case_select.SelectList()[0].expression->Type(),
            TypeTag::kCaseExp);
  EXPECT_NE(case_select.SelectList()[0].expression->ToString().find("CASE"),
            std::string::npos);

  auto extract = VisitSql("SELECT EXTRACT(YEAR FROM '2020-01-01');");
  ASSERT_TRUE(extract);
  const auto& extract_select =
      dynamic_cast<const SelectStatement&>(*extract);
  ASSERT_EQ(extract_select.SelectList().size(), 1);
  EXPECT_EQ(extract_select.SelectList()[0].expression->Type(),
            TypeTag::kFunctionCallExp);
  EXPECT_EQ(extract_select.SelectList()[0].expression->ToString(),
            "extract_year(\"2020-01-01\")");
  EXPECT_TRUE(extract_select.RequiresRelationalEvaluation());

  auto interval = VisitSql("SELECT INTERVAL 1 DAY;");
  ASSERT_TRUE(interval);
  const auto& interval_select =
      dynamic_cast<const SelectStatement&>(*interval);
  ASSERT_EQ(interval_select.SelectList().size(), 1);
  EXPECT_EQ(interval_select.SelectList()[0].expression->Type(),
            TypeTag::kIntervalExp);
  EXPECT_TRUE(interval_select.RequiresRelationalEvaluation());

  auto unary = VisitSql("SELECT -5;");
  ASSERT_TRUE(unary);
  const auto& unary_select = dynamic_cast<const SelectStatement&>(*unary);
  ASSERT_EQ(unary_select.SelectList().size(), 1);
  EXPECT_EQ(unary_select.SelectList()[0].expression->Type(),
            TypeTag::kUnaryExp);
  EXPECT_EQ(unary_select.SelectList()[0].expression->ToString(), "(-5)");
}

TEST(GoogleSqlAstTest, NonAggregateFunctionCalls) {
  auto statement = VisitSql("SELECT CONCAT('a', 'b'), SUBSTR('hello', 2);");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 2);
  EXPECT_EQ(select.SelectList()[0].expression->Type(),
            TypeTag::kFunctionCallExp);
  EXPECT_EQ(select.SelectList()[0].expression->ToString(),
            "concat(\"a\", \"b\")");
  EXPECT_EQ(select.SelectList()[1].expression->Type(),
            TypeTag::kFunctionCallExp);
  EXPECT_EQ(select.SelectList()[1].expression->ToString(),
            "substr(\"hello\", 2)");
}

TEST(GoogleSqlAstTest, AggregateFunctionsIncludingDistinct) {
  auto statement = VisitSql(
      "SELECT COUNT(*), SUM(a), AVG(b), MIN(c), MAX(d), COUNT(DISTINCT e) "
      "FROM t;");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 6);
  EXPECT_EQ(select.SelectList()[0].expression->ToString(), "COUNT(*)");
  EXPECT_EQ(select.SelectList()[1].expression->ToString(), "SUM(a)");
  EXPECT_EQ(select.SelectList()[2].expression->ToString(), "AVG(b)");
  EXPECT_EQ(select.SelectList()[3].expression->ToString(), "MIN(c)");
  EXPECT_EQ(select.SelectList()[4].expression->ToString(), "MAX(d)");
  EXPECT_EQ(select.SelectList()[5].expression->ToString(),
            "COUNT(DISTINCT e)");
}

TEST(GoogleSqlAstTest, FoldBooleanChains) {
  auto statement = VisitSql(
      "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3;");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_TRUE(select.WhereClause());
  EXPECT_NE(select.WhereClause()->ToString().find("a = 1"), std::string::npos);
  EXPECT_NE(select.WhereClause()->ToString().find("b = 2"), std::string::npos);
  EXPECT_NE(select.WhereClause()->ToString().find("c = 3"), std::string::npos);

  auto or_stmt = VisitSql("SELECT * FROM t WHERE a = 1 OR b = 2;");
  ASSERT_TRUE(or_stmt);
  const auto& or_select = dynamic_cast<const SelectStatement&>(*or_stmt);
  ASSERT_TRUE(or_select.WhereClause());
  EXPECT_EQ(or_select.WhereClause()->Type(), TypeTag::kBinaryExp);
}

TEST(GoogleSqlAstTest, JoinsAndTableSubqueries) {
  auto join = VisitSql("SELECT * FROM t JOIN t2 ON t.a = t2.a;");
  ASSERT_TRUE(join);
  const auto& join_select = dynamic_cast<const SelectStatement&>(*join);
  ASSERT_EQ(join_select.Sources().size(), 2);
  EXPECT_EQ(join_select.Sources()[0].table, "t");
  EXPECT_EQ(join_select.Sources()[1].table, "t2");
  // Phase 8: plain cross/inner joins stay on the cost-based optimizer path;
  // the engine folds the ON condition into the WHERE conjunction.
  EXPECT_FALSE(join_select.RequiresRelationalEvaluation());
  EXPECT_TRUE(join_select.Sources()[1].join_condition);

  auto left_join = VisitSql(
      "SELECT * FROM t LEFT JOIN t2 ON t.a = t2.a;");
  ASSERT_TRUE(left_join);
  const auto& left_select = dynamic_cast<const SelectStatement&>(*left_join);
  ASSERT_EQ(left_select.Sources().size(), 2);
  EXPECT_EQ(left_select.Sources()[1].join_type, JoinType::kLeft);

  auto subquery = VisitSql("SELECT * FROM (SELECT 1 AS x) AS s;");
  ASSERT_TRUE(subquery);
  const auto& sub_select = dynamic_cast<const SelectStatement&>(*subquery);
  ASSERT_EQ(sub_select.Sources().size(), 1);
  EXPECT_TRUE(sub_select.Sources()[0].query);
  EXPECT_EQ(sub_select.Sources()[0].alias, "s");
  EXPECT_TRUE(sub_select.RequiresRelationalEvaluation());
}

TEST(GoogleSqlAstTest, WithClause) {
  auto statement = VisitSql(
      "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte;");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  EXPECT_EQ(select.WithQueries().size(), 1);
  EXPECT_TRUE(select.RequiresRelationalEvaluation());
}

TEST(GoogleSqlAstTest, CreateTableColumnTypes) {
  auto statement = VisitSql(
      "CREATE TABLE t (i INT, f FLOAT, n NUMERIC, d DOUBLE, s STRING, "
      "c CHAR, ts TIMESTAMP, dt DATETIME, dd DATE, b BOOL);");
  ASSERT_TRUE(statement);
  ASSERT_EQ(statement->Type(), StatementType::kCreateTable);
  const auto& create = dynamic_cast<const CreateTableStatement&>(*statement);
  EXPECT_EQ(create.TableName(), "t");
  ASSERT_EQ(create.Columns().size(), 10);
  EXPECT_EQ(create.Columns()[0].Type(), ValueType::kInt64);
  EXPECT_EQ(create.Columns()[1].Type(), ValueType::kDouble);
  EXPECT_EQ(create.Columns()[2].Type(), ValueType::kDouble);
  EXPECT_EQ(create.Columns()[3].Type(), ValueType::kDouble);
  EXPECT_EQ(create.Columns()[4].Type(), ValueType::kVarChar);
  EXPECT_EQ(create.Columns()[5].Type(), ValueType::kVarChar);
  EXPECT_EQ(create.Columns()[6].Type(), ValueType::kVarChar);
  EXPECT_EQ(create.Columns()[7].Type(), ValueType::kVarChar);
  EXPECT_EQ(create.Columns()[8].Type(), ValueType::kDate);
  EXPECT_EQ(create.Columns()[9].Type(), ValueType::kInt64);
}

TEST(GoogleSqlAstTest, InsertWithAndWithoutColumnList) {
  auto with_columns = VisitSql("INSERT INTO t (a, b) VALUES (1, 2);");
  ASSERT_TRUE(with_columns);
  ASSERT_EQ(with_columns->Type(), StatementType::kInsert);
  const auto& insert =
      dynamic_cast<const InsertStatement&>(*with_columns);
  EXPECT_EQ(insert.TableName(), "t");
  EXPECT_EQ(insert.Columns().size(), 2);
  EXPECT_EQ(insert.Columns()[0], "a");
  EXPECT_EQ(insert.Columns()[1], "b");
  ASSERT_EQ(insert.Values().size(), 1);
  EXPECT_EQ(insert.Values()[0].size(), 2);

  auto multi_row = VisitSql(
      "INSERT INTO t VALUES (1, 'x'), (2, 'y');");
  ASSERT_TRUE(multi_row);
  const auto& multi = dynamic_cast<const InsertStatement&>(*multi_row);
  EXPECT_TRUE(multi.Columns().empty());
  ASSERT_EQ(multi.Values().size(), 2);
  EXPECT_EQ(multi.Values()[0].size(), 2);
  EXPECT_EQ(multi.Values()[1].size(), 2);
}

TEST(GoogleSqlAstTest, UpdateAndDeleteStatements) {
  auto update = VisitSql("UPDATE t SET a = 1 WHERE b = 2;");
  ASSERT_TRUE(update);
  ASSERT_EQ(update->Type(), StatementType::kUpdate);
  const auto& update_stmt = dynamic_cast<const UpdateStatement&>(*update);
  EXPECT_EQ(update_stmt.TableName(), "t");
  ASSERT_EQ(update_stmt.SetClause().size(), 1);
  EXPECT_EQ(update_stmt.SetClause()[0].first.name, "a");
  ASSERT_TRUE(update_stmt.WhereClause());
  EXPECT_EQ(update_stmt.WhereClause()->ToString(), "(b = 2)");

  auto delete_stmt = VisitSql("DELETE FROM t WHERE a = 1;");
  ASSERT_TRUE(delete_stmt);
  ASSERT_EQ(delete_stmt->Type(), StatementType::kDelete);
  const auto& delete_st = dynamic_cast<const DeleteStatement&>(*delete_stmt);
  EXPECT_EQ(delete_st.TableName(), "t");
  ASSERT_TRUE(delete_st.WhereClause());
  EXPECT_EQ(delete_st.WhereClause()->ToString(), "(a = 1)");
}

TEST(GoogleSqlAstTest, DropTableStatement) {
  auto statement = VisitSql("DROP TABLE t;");
  ASSERT_TRUE(statement);
  ASSERT_EQ(statement->Type(), StatementType::kDropTable);
  const auto& drop = dynamic_cast<const DropTableStatement&>(*statement);
  EXPECT_EQ(drop.TableName(), "t");
}

TEST(GoogleSqlAstTest, CaseWhenConditionNeedsRelationalEvaluation) {
  auto statement = VisitSql(
      "SELECT CASE WHEN EXTRACT(YEAR FROM '2020-01-01') > 2000 THEN 'new' "
      "ELSE 'old' END;");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 1);
  EXPECT_EQ(select.SelectList()[0].expression->Type(), TypeTag::kCaseExp);
  EXPECT_TRUE(select.RequiresRelationalEvaluation());
}

TEST(GoogleSqlAstTest, ArrayConstructor) {
  auto statement = VisitSqlOrThrow("SELECT ARRAY[1, 2];");
  ASSERT_EQ(statement->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 1);
  EXPECT_EQ(select.SelectList()[0].expression->Type(), TypeTag::kArrayExp);
  const Value got =
      select.SelectList()[0].expression->Evaluate(Row(), Schema());
  ASSERT_TRUE(got.IsArray());
  EXPECT_EQ(got.ArrayElementSqlType(), "INT64");
  ASSERT_EQ(got.ArrayElements().size(), 2);
  EXPECT_EQ(got.ArrayElements()[0], Value(int64_t{1}));
  EXPECT_EQ(got.ArrayElements()[1], Value(int64_t{2}));
}

TEST(GoogleSqlAstTest, UnsupportedConstructsThrow) {
  // Unknown column types are rejected while building CREATE TABLE.
  EXPECT_THROW(VisitSqlOrThrow("CREATE TABLE t (x BLOB);"),
               std::runtime_error);
  // Only DROP TABLE is translated; other DROP kinds are unsupported.
  EXPECT_THROW(VisitSqlOrThrow("DROP DATABASE t;"), std::runtime_error);
}

namespace {

template <typename Fn>
std::string ThrowMessage(Fn&& fn) {
  try {
    fn();
  } catch (const std::exception& e) {
    // Visitor errors are runtime_error, but catch std::exception so a stray
    // logic_error (e.g. from a numeric parse) fails the message assertion
    // instead of aborting the test binary.
    return e.what();
  }
  return {};
}

}  // namespace

TEST(GoogleSqlAstTest, BacktickIdentifiersAreDecoded) {
  // The GoogleSQL AST keeps backticks in the Identifier detail for names that
  // are not plain identifiers (for example names containing a space). The
  // visitor must strip them both for columns, aliases, and table paths.
  auto statement =
      VisitSql("SELECT `my col` AS `alias name` FROM `table name`;");
  ASSERT_TRUE(statement);
  ASSERT_EQ(statement->Type(), StatementType::kSelect);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 1);
  EXPECT_EQ(select.SelectList()[0].name, "alias name");
  EXPECT_EQ(select.SelectList()[0].expression->ToString(), "my col");
  ASSERT_EQ(select.FromClause().size(), 1);
  EXPECT_EQ(select.FromClause()[0], "table name");
}

TEST(GoogleSqlAstTest, TripleQuotedStringDecodesEmbeddedQuote) {
  // GoogleSQL allows a triple-quoted string literal that contains an escaped
  // single quote (''), which the visitor must decode into one quote character.
  auto statement = VisitSql("SELECT '''a''b''';");
  ASSERT_TRUE(statement);
  const auto& select = dynamic_cast<const SelectStatement&>(*statement);
  ASSERT_EQ(select.SelectList().size(), 1);
  EXPECT_EQ(select.SelectList()[0].expression->AsConstantValue().GetValue(),
            Value("a'b"));
}

TEST(GoogleSqlAstTest, InsertSelectMapsToQuerySource) {
  // INSERT ... SELECT parses into an InsertStatement with a Query child; the
  // visitor maps it to a query-backed InsertStatement instead of rejecting it.
  auto statement = VisitSqlOrThrow("INSERT INTO t SELECT * FROM s;");
  const auto* insert = dynamic_cast<const InsertStatement*>(statement.get());
  ASSERT_NE(insert, nullptr);
  EXPECT_EQ(insert->TableName(), "t");
  EXPECT_NE(insert->Query(), nullptr);
}

TEST(GoogleSqlAstTest, TableValuedFunctionSourceThrows) {
  // A table-valued function in FROM parses into a TVF node, which is not a
  // recognized table source kind.
  const std::string message =
      ThrowMessage([] { VisitSqlOrThrow("SELECT * FROM my_func(1);"); });
  EXPECT_NE(message.find("unsupported table source TVF"), std::string::npos)
      << message;
}

TEST(GoogleSqlAstTest, UnsupportedStatementKindsThrow) {
  // Statements beyond SELECT / CREATE TABLE / INSERT / UPDATE / DELETE / DROP
  // TABLE have no translation and must be rejected explicitly.
  for (const char* sql : {"EXPLAIN SELECT 1;", "SHOW TABLES;", "BEGIN;",
                          "COMMIT;", "CREATE INDEX i ON t(a);",
                          "ALTER TABLE t ADD COLUMN b INT64;",
                          "CREATE SCHEMA s;", "DESCRIBE t;"}) {
    const std::string message = ThrowMessage([sql] { VisitSqlOrThrow(sql); });
    EXPECT_NE(message.find("unsupported statement"), std::string::npos)
        << sql << "\n" << message;
  }
}

TEST(GoogleSqlAstTest, UnsupportedExpressionKindsThrow) {
  // Expression node kinds without a mapping must be rejected with a precise
  // diagnostic instead of silently producing a malformed plan.
  for (const char* sql : {"SELECT f(x => 1);"}) {
    const std::string message = ThrowMessage([sql] { VisitSqlOrThrow(sql); });
    EXPECT_NE(message.find("unsupported expression"), std::string::npos)
        << sql << "\n" << message;
  }
}

TEST(GoogleSqlAstTest, DeeplyNestedExpressionsFailWithDiagnostic) {
  // The parser accepts arbitrarily deep expressions; visitation must reject
  // nesting beyond the depth cap with an exception instead of overflowing
  // the C++ stack (a stack overflow is unrecoverable).
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  std::string sql = "SELECT 1";
  for (int i = 0; i < 600; ++i) { sql += "+1";
}
  sql += ";";
  const std::string message =
      ThrowMessage([&sql] { VisitSqlOrThrow(sql); });
  // Either layer must reject the input instead of overflowing the stack: the
  // ZetaSQL parser ("binary expression arity") or the visitor depth cap
  // ("nesting exceeds").
  EXPECT_TRUE(message.find("nesting exceeds") != std::string::npos ||
              message.find("arity") != std::string::npos)
      << message;
}

TEST(GoogleSqlAstTest, ExplicitZeroLimitDistinguishesFromAbsent) {
  auto zero = VisitSql("SELECT a FROM t LIMIT 0;");
  ASSERT_TRUE(zero);
  const auto& zero_select = dynamic_cast<const SelectStatement&>(*zero);
  EXPECT_TRUE(zero_select.HasLimit());
  EXPECT_EQ(zero_select.Limit(), 0U);

  auto none = VisitSql("SELECT a FROM t;");
  ASSERT_TRUE(none);
  const auto& none_select = dynamic_cast<const SelectStatement&>(*none);
  EXPECT_FALSE(none_select.HasLimit());
}

// Tests below are derived from googlesql_ast_fuzzer: the AST dump arrives
// from an external parser process, so the dump parser and the visitor must
// treat arbitrary text as untrusted input - reject, never crash.
TEST(GoogleSqlAstTest, MalformedDumpsAreRejectedWithoutCrashing) {
  const std::array<std::string_view, 6> broken_dumps = {
      "",                             // empty: no root
      "\n",                           // only blank lines
      "  bad\n",                      // odd indentation
      "a\nb\n",                       // two roots at depth zero
      "root\n   three-space\n",       // odd indent below root
      "root\n  child\n grandchild\n"  // depth jumps by one level only
  };
  for (const std::string_view dump : broken_dumps) {
    SCOPED_TRACE(testing::Message() << "dump=[" << dump << "]");
    const StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
        GoogleSqlAstParser::Parse(dump);
    EXPECT_FALSE(ast.HasValue());
  }
}

TEST(GoogleSqlAstTest, LabelsWithoutRangeSuffixAreAcceptedLeniently) {
  // A location suffix only counts when the label ends in ']'; anything else
  // becomes the node's literal kind. Leniency pins from the fuzzer corpus:
  // unparsable ranges and unbalanced parens survive as opaque nodes.
  for (const std::string_view dump :
       {"q [0-1] extra\n", "x [not-a-range]\n", "y(-\n"}) {
    SCOPED_TRACE(testing::Message() << "dump=[" << dump << "]");
    const StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
        GoogleSqlAstParser::Parse(dump);
    ASSERT_TRUE(ast.HasValue());
    EXPECT_EQ(ast.Value()->children.size(), 0U);
  }
}

TEST(GoogleSqlAstTest, DeeplyIndentedDumpParsesIteratively) {
  // The fuzzer explores very deep trees; parsing must stay iterative so a
  // deep dump cannot exhaust the stack. Depth 2000 is well beyond anything
  // the real parser emits but cheap here.
  std::string dump = "root\n";
  for (int i = 0; i < 2000; ++i) {
    dump.append(static_cast<size_t>(i + 1) * 2, ' ');
    dump.append("node\n");
  }
  const StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(dump);
  ASSERT_TRUE(ast.HasValue());
  EXPECT_EQ(ast.Value()->kind, "root");
  EXPECT_EQ(ast.Value()->children.size(), 1U);
}

TEST(GoogleSqlAstTest, UnknownStatementKindThrowsDiagnosticForVisitor) {
  // The visitor reports trees whose root it does not understand by throwing;
  // sql_engine catches. Pin that contract including the message shape.
  const StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse("Statement\n  WeirdThing(zzz)\n");
  ASSERT_TRUE(ast.HasValue());
  try {
    const std::unique_ptr<Statement> statement =
        GoogleSqlAstVisitor::Visit(*ast.Value());
    FAIL() << "expected a throw for an unsupported statement kind";
  } catch (const std::exception& error) {
    EXPECT_NE(std::string(error.what()).find("unsupported"), std::string::npos);
  }
}

TEST(GoogleSqlAstTest, ChildAccessorOccurrenceSemantics) {
  // Visitor code relies on Child(kind, n) returning the n-th matching child
  // and nullptr past the last one; pin it with same-kind siblings.
  const StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(
          "root\n"
          "  leaf(a)\n"
          "  other(x)\n"
          "  leaf(b)\n");
  ASSERT_TRUE(ast.HasValue());
  const GoogleSqlAstNode& root = *ast.Value();
  ASSERT_NE(root.Child("leaf", 0), nullptr);
  EXPECT_EQ(root.Child("leaf", 0)->detail, "a");
  ASSERT_NE(root.Child("leaf", 1), nullptr);
  EXPECT_EQ(root.Child("leaf", 1)->detail, "b");
  EXPECT_EQ(root.Child("leaf", 2), nullptr);
  EXPECT_EQ(root.Child("missing", 0), nullptr);
  EXPECT_EQ(root.Children("leaf").size(), 2U);
}

TEST(GoogleSqlAstTest, ParsesLateralTableSubquery) {
  const std::string sql =
      "SELECT o.name, i.score FROM lat_outer o, LATERAL (SELECT score FROM lat_inner WHERE outer_id = o.id) i;";
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  ASSERT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  std::unique_ptr<Statement> stmt = GoogleSqlAstVisitor::Visit(*ast.Value());
  ASSERT_NE(stmt, nullptr);
  auto* select = dynamic_cast<SelectStatement*>(stmt.get());
  ASSERT_NE(select, nullptr);
  ASSERT_EQ(select->Sources().size(), 2U);
  EXPECT_TRUE(select->Sources()[1].is_lateral);
}

TEST(GoogleSqlAstTest, DistinctOnAstVisitor) {
  const std::string sql =
      "SELECT DISTINCT ON (dept) dept, salary FROM employees ORDER BY dept, salary DESC;";
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  ASSERT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  std::unique_ptr<Statement> stmt = GoogleSqlAstVisitor::Visit(*ast.Value());
  ASSERT_NE(stmt, nullptr);
  auto* select = dynamic_cast<SelectStatement*>(stmt.get());
  ASSERT_NE(select, nullptr);
  EXPECT_TRUE(select->Distinct());
  EXPECT_TRUE(select->HasDistinctOn());
  ASSERT_EQ(select->DistinctOn().size(), 1U);
}

TEST(GoogleSqlAstTest, FetchFirstWithTiesAstVisitor) {
  const std::string sql =
      "SELECT name, score FROM students ORDER BY score DESC FETCH FIRST 3 ROWS WITH TIES;";
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  ASSERT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  std::unique_ptr<Statement> stmt = GoogleSqlAstVisitor::Visit(*ast.Value());
  ASSERT_NE(stmt, nullptr);
  auto* select = dynamic_cast<SelectStatement*>(stmt.get());
  ASSERT_NE(select, nullptr);
  EXPECT_TRUE(select->WithTies());
  EXPECT_EQ(select->Limit(), 3U);
}

TEST(GoogleSqlAstTest, GroupByAllAndDistinctAstVisitor) {
  const std::string sql_all =
      "SELECT dept, job, count(1) AS c FROM employees GROUP BY ALL;";
  GoogleSqlParseResult parsed_all = GoogleSqlFrontend::Parse(sql_all);
  ASSERT_TRUE(parsed_all.ok) << parsed_all.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast_all =
      GoogleSqlAstParser::Parse(parsed_all.ast);
  ASSERT_TRUE(ast_all.HasValue());
  std::unique_ptr<Statement> stmt_all =
      GoogleSqlAstVisitor::Visit(*ast_all.Value());
  ASSERT_NE(stmt_all, nullptr);
  auto* select_all = dynamic_cast<SelectStatement*>(stmt_all.get());
  ASSERT_NE(select_all, nullptr);
  EXPECT_EQ(select_all->GroupBy().size(), 2U);

  const std::string sql_distinct =
      "SELECT dept, count(1) AS c FROM employees GROUP BY DISTINCT dept;";
  GoogleSqlParseResult parsed_dist = GoogleSqlFrontend::Parse(sql_distinct);
  ASSERT_TRUE(parsed_dist.ok) << parsed_dist.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast_dist =
      GoogleSqlAstParser::Parse(parsed_dist.ast);
  ASSERT_TRUE(ast_dist.HasValue());
  std::unique_ptr<Statement> stmt_dist =
      GoogleSqlAstVisitor::Visit(*ast_dist.Value());
  ASSERT_NE(stmt_dist, nullptr);
  auto* select_dist = dynamic_cast<SelectStatement*>(stmt_dist.get());
  ASSERT_NE(select_dist, nullptr);
  EXPECT_EQ(select_dist->GroupBy().size(), 1U);
}

TEST(GoogleSqlAstTest, QualifyAstVisitor) {
  const std::string sql =
      "SELECT name, row_number() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM emp QUALIFY rn <= 2;";
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  ASSERT_TRUE(parsed.ok) << parsed.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
      GoogleSqlAstParser::Parse(parsed.ast);
  ASSERT_TRUE(ast.HasValue());
  std::unique_ptr<Statement> stmt = GoogleSqlAstVisitor::Visit(*ast.Value());
  ASSERT_NE(stmt, nullptr);
  auto* select = dynamic_cast<SelectStatement*>(stmt.get());
  ASSERT_NE(select, nullptr);
  EXPECT_NE(select->Qualify(), nullptr);
}

TEST(GoogleSqlAstTest, PivotAndUnpivotAstVisitor) {
  const std::string pivot_sql =
      "SELECT * FROM sales PIVOT(sum(amount) FOR quarter IN ('Q1', 'Q2')) AS p;";
  GoogleSqlParseResult parsed_p = GoogleSqlFrontend::Parse(pivot_sql);
  ASSERT_TRUE(parsed_p.ok) << parsed_p.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast_p =
      GoogleSqlAstParser::Parse(parsed_p.ast);
  ASSERT_TRUE(ast_p.HasValue());
  std::unique_ptr<Statement> stmt_p = GoogleSqlAstVisitor::Visit(*ast_p.Value());
  ASSERT_NE(stmt_p, nullptr);
  auto* select_p = dynamic_cast<SelectStatement*>(stmt_p.get());
  ASSERT_NE(select_p, nullptr);
  ASSERT_FALSE(select_p->Sources().empty());
  EXPECT_NE(select_p->Sources()[0].query, nullptr);

  const std::string unpivot_sql =
      "SELECT * FROM widetab UNPIVOT(val FOR col IN (q1, q2)) AS u;";
  GoogleSqlParseResult parsed_u = GoogleSqlFrontend::Parse(unpivot_sql);
  ASSERT_TRUE(parsed_u.ok) << parsed_u.error;
  StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast_u =
      GoogleSqlAstParser::Parse(parsed_u.ast);
  ASSERT_TRUE(ast_u.HasValue());
  std::unique_ptr<Statement> stmt_u = GoogleSqlAstVisitor::Visit(*ast_u.Value());
  ASSERT_NE(stmt_u, nullptr);
  auto* select_u = dynamic_cast<SelectStatement*>(stmt_u.get());
  ASSERT_NE(select_u, nullptr);
  ASSERT_FALSE(select_u->Sources().empty());
  EXPECT_NE(select_u->Sources()[0].query, nullptr);
}

}  // namespace tinylamb
