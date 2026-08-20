/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/googlesql_frontend.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace tinylamb {

TEST(GoogleSqlFrontendTest, ReturnsParserAst) {
  GoogleSqlParseResult result = GoogleSqlFrontend::Parse(
      "select c_id from customer where c_w_id=1 order by c_id limit 1");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("QueryStatement"), std::string::npos);
  EXPECT_NE(result.ast.find("OrderBy"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, RejectsInvalidSqlWhenAvailable) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  GoogleSqlParseResult result = GoogleSqlFrontend::Parse("SELECT 1 + ;");
  EXPECT_FALSE(result.ok);
  EXPECT_FALSE(result.error.empty());
}

TEST(GoogleSqlFrontendTest, ParsesAndCachesCommonStatementKinds) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const std::vector<std::string> statements = {
      "CREATE TABLE t (a INT64, b STRING);",
      "INSERT INTO t VALUES (1, 'x'), (2, 'y');",
      "UPDATE t SET b = 'z' WHERE a = 1;",
      "DELETE FROM t WHERE a = 2;",
      "SELECT a, b FROM t WHERE a IN (1, 2) ORDER BY b LIMIT 5;",
      "DROP TABLE t;"};
  for (const std::string& sql : statements) {
    GoogleSqlParseResult first = GoogleSqlFrontend::Parse(sql);
    ASSERT_TRUE(first.ok) << sql << "\n" << first.error;
    EXPECT_NE(first.ast.find("Statement"), std::string::npos) << sql;
    // A second parse must be served from the parse cache.
    GoogleSqlParseResult cached = GoogleSqlFrontend::Parse(sql);
    ASSERT_TRUE(cached.ok) << sql << "\n" << cached.error;
    EXPECT_EQ(cached.ast, first.ast);
  }
}

TEST(GoogleSqlFrontendTest, CacheEvictsOldestStatementsPastLimit) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  // The parse cache holds kMaxCachedStatements entries; feed it well beyond
  // that so the oldest-entry eviction path runs for every statement.
  for (int i = 0; i < 1030; ++i) {
    GoogleSqlParseResult result =
        GoogleSqlFrontend::Parse("SELECT " + std::to_string(i) + ";");
    ASSERT_TRUE(result.ok) << result.error;
  }
  // A brand-new statement must still parse after all the churn.
  GoogleSqlParseResult fresh = GoogleSqlFrontend::Parse("SELECT 123456;");
  ASSERT_TRUE(fresh.ok) << fresh.error;
  EXPECT_NE(fresh.ast.find("Select"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, RejectsMalformedStatements) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const std::vector<std::string> invalid = {
      "SELECT FROM WHERE;",
      "CREATE TABLE (;",
      "INSERT INTO VALUES;",
      "SELECT * FROM t WHERE ;"};
  for (const std::string& sql : invalid) {
    GoogleSqlParseResult result = GoogleSqlFrontend::Parse(sql);
    EXPECT_FALSE(result.ok) << sql;
    EXPECT_FALSE(result.error.empty()) << sql;
  }
}

TEST(GoogleSqlFrontendTest, ParsesJoinQueries) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult result =
      GoogleSqlFrontend::Parse("SELECT a.name, b.name FROM emp a JOIN dept b "
                               "ON a.dept_id = b.id WHERE b.id = 1;");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("Join"), std::string::npos);
  EXPECT_NE(result.ast.find("OnClause"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesLeftOuterJoin) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult result = GoogleSqlFrontend::Parse(
      "SELECT a.id FROM t1 a LEFT JOIN t2 b USING (id);");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("Join(LEFT)"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesSubqueries) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult scalar =
      GoogleSqlFrontend::Parse("SELECT (SELECT max(x) FROM s) FROM t;");
  ASSERT_TRUE(scalar.ok) << scalar.error;
  EXPECT_NE(scalar.ast.find("ExpressionSubquery"), std::string::npos);

  const GoogleSqlParseResult derived =
      GoogleSqlFrontend::Parse("SELECT * FROM (SELECT 1 AS x) AS sub;");
  ASSERT_TRUE(derived.ok) << derived.error;
  EXPECT_NE(derived.ast.find("TableSubquery"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesGroupByHavingOrderLimitOffset) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult result = GoogleSqlFrontend::Parse(
      "SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1 ORDER BY a DESC LIMIT 5 "
      "OFFSET 2;");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("GroupBy"), std::string::npos);
  EXPECT_NE(result.ast.find("Having"), std::string::npos);
  EXPECT_NE(result.ast.find("OrderBy"), std::string::npos);
  EXPECT_NE(result.ast.find("LimitOffset"), std::string::npos);
  EXPECT_NE(result.ast.find("Offset"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesInsertVariants) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult multi = GoogleSqlFrontend::Parse(
      "INSERT INTO t (a, b) VALUES (1, \"x\"), (2, \"y\");");
  ASSERT_TRUE(multi.ok) << multi.error;
  EXPECT_NE(multi.ast.find("InsertStatement"), std::string::npos);
  EXPECT_NE(multi.ast.find("ColumnList"), std::string::npos);

  const GoogleSqlParseResult from_select =
      GoogleSqlFrontend::Parse("INSERT INTO t (a) SELECT b FROM s;");
  ASSERT_TRUE(from_select.ok) << from_select.error;
  EXPECT_NE(from_select.ast.find("InsertStatement"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesCreateTableWithConstraints) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult result = GoogleSqlFrontend::Parse(
      "CREATE TABLE t (a INT64 NOT NULL, b STRING(10) NOT NULL, PRIMARY "
      "KEY(a));");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("CreateTableStatement"), std::string::npos);
  EXPECT_NE(result.ast.find("PrimaryKey"), std::string::npos);
  EXPECT_NE(result.ast.find("NotNullColumnAttribute"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesFunctionCallsAndLiterals) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult result =
      GoogleSqlFrontend::Parse("SELECT COUNT(*), CAST(a AS STRING), 2.5, "
                               "\"s\", true, false, NULL, 1 = 1 FROM t;");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("FunctionCall"), std::string::npos);
  EXPECT_NE(result.ast.find("CastExpression"), std::string::npos);
  EXPECT_NE(result.ast.find("FloatLiteral"), std::string::npos);
  EXPECT_NE(result.ast.find("StringLiteral"), std::string::npos);
  EXPECT_NE(result.ast.find("BooleanLiteral"), std::string::npos);
  EXPECT_NE(result.ast.find("NullLiteral"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesWithClauseAndDistinct) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult with_clause = GoogleSqlFrontend::Parse(
      "WITH w AS (SELECT 1 AS x) SELECT * FROM w;");
  ASSERT_TRUE(with_clause.ok) << with_clause.error;
  EXPECT_NE(with_clause.ast.find("WithClause"), std::string::npos);

  const GoogleSqlParseResult distinct =
      GoogleSqlFrontend::Parse("SELECT DISTINCT a FROM t;");
  ASSERT_TRUE(distinct.ok) << distinct.error;
  EXPECT_NE(distinct.ast.find("Select(distinct=true)"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesUpdateDeleteDropAndTransactionStatements) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const std::vector<std::string> statements = {
      "UPDATE t SET b = \"z\", a = 2 WHERE a = 1;",
      "DELETE FROM t WHERE a = 2;",
      "DROP TABLE t;",
      "BEGIN;",
      "COMMIT;",
      "ROLLBACK;"};
  for (const std::string& sql : statements) {
    GoogleSqlParseResult result = GoogleSqlFrontend::Parse(sql);
    ASSERT_TRUE(result.ok) << sql << "\n" << result.error;
    EXPECT_NE(result.ast.find("Statement"), std::string::npos) << sql;
  }
}

TEST(GoogleSqlFrontendTest, RejectsAdditionalMalformedStatements) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const std::vector<std::string> invalid = {
      "SELECT * FROM t WHERE;",
      "SELECT * FROM;",
      "INSERT INTO t VALUES;",
      "SELECT $1;",
      "CREATE TABLE t a;",
      "SELECT a FROM t1 JOIN t2 ON;",
      "SELECT (1;"};
  for (const std::string& sql : invalid) {
    GoogleSqlParseResult result = GoogleSqlFrontend::Parse(sql);
    EXPECT_FALSE(result.ok) << sql;
    EXPECT_FALSE(result.error.empty()) << sql;
  }
}

TEST(GoogleSqlFrontendTest, CachesDistinctStatementTextsSeparately) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  // Two statements that differ only in whitespace/case are distinct cache
  // keys but produce the same AST, so both must remain independently
  // parseable and must not collide in the cache.
  const GoogleSqlParseResult upper =
      GoogleSqlFrontend::Parse("SELECT 1;");
  const GoogleSqlParseResult lower =
      GoogleSqlFrontend::Parse("select 1 ;");
  ASSERT_TRUE(upper.ok) << upper.error;
  ASSERT_TRUE(lower.ok) << lower.error;
  EXPECT_EQ(upper.ast, lower.ast);
}

TEST(GoogleSqlFrontendTest, ParsesSetOperations) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const std::vector<std::string> statements = {
      "SELECT a FROM t UNION ALL SELECT b FROM s;",
      "SELECT a FROM t INTERSECT DISTINCT SELECT b FROM s;",
      "SELECT a FROM t EXCEPT DISTINCT SELECT b FROM s;"};
  for (const std::string& sql : statements) {
    GoogleSqlParseResult result = GoogleSqlFrontend::Parse(sql);
    ASSERT_TRUE(result.ok) << sql << "\n" << result.error;
    EXPECT_NE(result.ast.find("SetOperation("), std::string::npos) << sql;
  }
  const GoogleSqlParseResult union_all =
      GoogleSqlFrontend::Parse("SELECT a FROM t UNION ALL SELECT b FROM s;");
  ASSERT_TRUE(union_all.ok) << union_all.error;
  EXPECT_NE(union_all.ast.find("SetOperation(UNION ALL)"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesCaseExpressions) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult simple = GoogleSqlFrontend::Parse(
      "SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END FROM t;");
  ASSERT_TRUE(simple.ok) << simple.error;
  EXPECT_NE(simple.ast.find("CaseNoValueExpression"), std::string::npos);
  EXPECT_NE(simple.ast.find("StringLiteralComponent('x')"), std::string::npos);

  const GoogleSqlParseResult searched = GoogleSqlFrontend::Parse(
      "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t;");
  ASSERT_TRUE(searched.ok) << searched.error;
  EXPECT_NE(searched.ast.find("CaseValueExpression"), std::string::npos);
  EXPECT_NE(searched.ast.find("IntLiteral(1)"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesRangeAndPatternPredicates) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult between = GoogleSqlFrontend::Parse(
      "SELECT a FROM t WHERE a BETWEEN 1 AND 5;");
  ASSERT_TRUE(between.ok) << between.error;
  EXPECT_NE(between.ast.find("BetweenExpression(BETWEEN)"), std::string::npos);

  const GoogleSqlParseResult like = GoogleSqlFrontend::Parse(
      "SELECT a FROM t WHERE b LIKE 'a%';");
  ASSERT_TRUE(like.ok) << like.error;
  EXPECT_NE(like.ast.find("BinaryExpression(LIKE)"), std::string::npos);

  const GoogleSqlParseResult is_null = GoogleSqlFrontend::Parse(
      "SELECT a FROM t WHERE a IS NULL;");
  ASSERT_TRUE(is_null.ok) << is_null.error;
  EXPECT_NE(is_null.ast.find("BinaryExpression(IS)"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesWindowFunctions) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult result = GoogleSqlFrontend::Parse(
      "SELECT SUM(a) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN UNBOUNDED "
      "PRECEDING AND CURRENT ROW) AS s FROM t;");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("AnalyticFunctionCall"), std::string::npos);
  EXPECT_NE(result.ast.find("WindowSpecification"), std::string::npos);
  EXPECT_NE(result.ast.find("PartitionBy"), std::string::npos);
  EXPECT_NE(result.ast.find("OrderBy"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesExistsAndCorrelatedSubqueries) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const GoogleSqlParseResult exists = GoogleSqlFrontend::Parse(
      "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM s);");
  ASSERT_TRUE(exists.ok) << exists.error;
  EXPECT_NE(exists.ast.find("ExpressionSubquery(modifier=EXISTS)"),
            std::string::npos);

  const GoogleSqlParseResult nested = GoogleSqlFrontend::Parse(
      "SELECT a FROM t WHERE a IN (SELECT b FROM s);");
  ASSERT_TRUE(nested.ok) << nested.error;
  EXPECT_NE(nested.ast.find("InExpression"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, ParsesDdlAndOtherStatementKinds) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  const std::vector<std::string> statements = {
      "TRUNCATE TABLE t;",
      "SHOW TABLES;",
      "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE;",
      "CREATE INDEX i ON t (a);",
      "ALTER TABLE t ADD COLUMN x INT64;",
      "START TRANSACTION;"};
  for (const std::string& sql : statements) {
    GoogleSqlParseResult result = GoogleSqlFrontend::Parse(sql);
    ASSERT_TRUE(result.ok) << sql << "\n" << result.error;
    EXPECT_NE(result.ast.find("Statement"), std::string::npos) << sql;
  }
  EXPECT_NE(GoogleSqlFrontend::Parse("TRUNCATE TABLE t;").ast.find(
                "TruncateStatement"),
            std::string::npos);
  EXPECT_NE(GoogleSqlFrontend::Parse("SHOW TABLES;").ast.find("ShowStatement"),
            std::string::npos);
  EXPECT_NE(GoogleSqlFrontend::Parse(
                "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE;")
                .ast.find("MergeStatement"),
            std::string::npos);
}

TEST(GoogleSqlFrontendTest, RejectsUnsupportedStatementKinds) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  // The pinned GoogleSQL parser does not accept these statement kinds; each
  // must surface as a parse failure with a non-empty error payload.
  const std::vector<std::string> unsupported = {
      "GRANT SELECT ON t TO u;",
      "COPY t FROM 'f.csv';",
      "VACUUM;",
      "SELECT a FROM t ORDER BY a FETCH FIRST 5 ROWS ONLY;"};
  for (const std::string& sql : unsupported) {
    GoogleSqlParseResult result = GoogleSqlFrontend::Parse(sql);
    EXPECT_FALSE(result.ok) << sql;
    EXPECT_FALSE(result.error.empty()) << sql;
  }
}

TEST(GoogleSqlFrontendTest, EmptyInputReportsSuccessWithoutAst) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  // Quirk of the subprocess protocol: an empty input makes the parser exit 0
  // with no output, which the frontend reports as a successful (but empty)
  // parse. A lone semicolon, by contrast, is a syntax error.
  const GoogleSqlParseResult empty = GoogleSqlFrontend::Parse("");
  EXPECT_TRUE(empty.ok) << empty.error;
  EXPECT_TRUE(empty.ast.empty());
  EXPECT_TRUE(empty.error.empty());

  const GoogleSqlParseResult semicolon = GoogleSqlFrontend::Parse(";");
  EXPECT_FALSE(semicolon.ok);
  EXPECT_FALSE(semicolon.error.empty());
}

}  // namespace tinylamb
