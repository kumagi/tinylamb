/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Parser/AST-visitor coverage sweep: literal forms (hex, bytes, raw and
// escaped strings), every expression node kind the visitor lowers, nested
// query shapes, ordinal ORDER/GROUP BY, multi-column USING, and the
// diagnostics for malformed input.  Expectations were pinned against the
// engine's ground truth.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  // One retry for empty diagnostics: a failed Execute with no LastError is
  // the external parse child being transiently unavailable under load.
  StatusOr<QueryResult> result = engine->Execute(*ctx, sql);
  if (!result.HasValue()) {
    if (engine->LastError().empty()) {
      result = engine->Execute(*ctx, sql);
    }
    if (!result.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  std::vector<Row> rows;
  QueryResult drained = result.MoveValue();
  Row row;
  while (drained.Next(&row)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = drained.GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}
}  // namespace

class SqlParserFeatureCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("sql_parser_feature_coverage_test-" + RandomString())
            .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    Run("CREATE TABLE p (a INT64, s STRING)");
    Run("INSERT INTO p (a, s) VALUES (1, 'one'), (2, 'two'), (3, 'three')");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::vector<Row> Run(const std::string& sql) {
    return RunSql(engine_.get(), context_.get(), sql);
  }
  Value Scalar(const std::string& sql) {
    const auto rows = Run(sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "query did not return one row: " << sql;
      return {};
    }
    return rows[0][0];
  }
  void ExpectScalar(const std::string& sql, const Value& expected) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(Scalar(sql), expected);
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlParserFeatureCoverageTest, LiteralFormsDecode) {
  ExpectScalar("SELECT 0x1F", Value(int64_t{31}));
  ExpectScalar("SELECT b'abc'", Value(std::string("abc")));
  // Raw strings skip escape processing; plain strings process \t and friends.
  ExpectScalar("SELECT r'raw\\n'", Value(std::string("raw\\n")));
  ExpectScalar("SELECT 'esc\\tab'", Value(std::string("esc\tab")));
  ExpectScalar("SELECT -(-1)", Value(int64_t{1}));
  ExpectScalar("SELECT CAST(b'hi' AS STRING)", Value(std::string("hi")));
}

TEST_F(SqlParserFeatureCoverageTest, ExpressionNodeKinds) {
  ExpectScalar("SELECT 5 BETWEEN 1 AND 10", Value(true));
  ExpectScalar("SELECT 5 NOT BETWEEN 1 AND 4", Value(true));
  ExpectScalar("SELECT NOT FALSE", Value(true));
  ExpectScalar("SELECT CASE WHEN 1 = 1 THEN 'y' ELSE 'n' END",
               Value(std::string("y")));
  ExpectScalar("SELECT CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END",
               Value(std::string("b")));
  ExpectScalar("SELECT IF(TRUE, 1, 2)", Value(int64_t{1}));
  ExpectScalar("SELECT EXTRACT(DAY FROM DATE '2026-09-18')",
               Value(int64_t{18}));
  ExpectScalar("SELECT NULLIF(1, 2)", Value(int64_t{1}));
  ExpectScalar("SELECT COALESCE(NULL, 3)", Value(int64_t{3}));
  ExpectScalar("SELECT (SELECT 1)", Value(int64_t{1}));
  ExpectScalar("SELECT [1, 2, 3] = [1, 2, 3]", Value(true));
  ExpectScalar("SELECT CAST(3.7 AS INT64)", Value(int64_t{4}));
  ExpectScalar("SELECT SAFE_CAST('zz' AS INT64) IS NULL", Value(true));
}

TEST_F(SqlParserFeatureCoverageTest, ColumnAndAliasShapes) {
  ExpectScalar("SELECT p.a FROM p WHERE p.a = 2", Value(int64_t{2}));
  ExpectScalar("SELECT a FROM p AS px WHERE a = 1", Value(int64_t{1}));
  ExpectScalar("SELECT a FROM p px WHERE px.a = 3", Value(int64_t{3}));
  ExpectScalar("SELECT px.a FROM p AS px WHERE px.a = 2", Value(int64_t{2}));
  ExpectScalar("SELECT COUNT(*) FROM p", Value(int64_t{3}));
  ExpectScalar("SELECT a + 1 AS nxt FROM p WHERE a = 2", Value(int64_t{3}));
}

TEST_F(SqlParserFeatureCoverageTest, OrdinalsAndSetClauseShapes) {
  ExpectScalar("SELECT s FROM p ORDER BY 1 DESC LIMIT 1",
               Value(std::string("two")));
  // Ordinal GROUP BY: lengths are {3, 3, 5}; only l=5 has COUNT(*) = 1.
  ExpectScalar(
      "SELECT COUNT(*) FROM (SELECT LENGTH(s) AS l, COUNT(*) FROM p "
      "GROUP BY 1 HAVING COUNT(*) = 1) q",
      Value(int64_t{1}));
  ExpectScalar(
      "SELECT COUNT(*) FROM (SELECT a FROM p GROUP BY a "
      "HAVING a > 1) q",
      Value(int64_t{2}));
}

TEST_F(SqlParserFeatureCoverageTest, JoinClauseKeywordVariants) {
  ExpectScalar("SELECT COUNT(*) FROM p x JOIN p y USING (a)",
               Value(int64_t{3}));
  ExpectScalar("SELECT COUNT(*) FROM p x CROSS JOIN p y", Value(int64_t{9}));
  ExpectScalar("SELECT COUNT(*) FROM p x INNER JOIN p y ON x.a = y.a",
               Value(int64_t{3}));
  ExpectScalar("SELECT COUNT(*) FROM p x LEFT OUTER JOIN p y ON x.a = y.a + 1",
               Value(int64_t{3}));
  // USING with multiple columns.
  ExpectScalar("SELECT COUNT(*) FROM p x JOIN p y USING (a, s)",
               Value(int64_t{3}));
}

TEST_F(SqlParserFeatureCoverageTest, NestedQueryShapes) {
  ExpectScalar(
      "WITH o AS (SELECT a FROM p), i AS (SELECT a FROM o "
      "WHERE a > 1) SELECT COUNT(*) FROM i",
      Value(int64_t{2}));
  ExpectScalar(
      "SELECT COUNT(*) FROM (WITH z AS (SELECT a FROM p) "
      "SELECT * FROM z) q",
      Value(int64_t{3}));
  ExpectScalar("SELECT (SELECT COUNT(*) FROM p) AS c", Value(int64_t{3}));
  ExpectScalar(
      "SELECT a FROM p WHERE a IN (SELECT a FROM p WHERE a > 1) "
      "ORDER BY a LIMIT 1",
      Value(int64_t{2}));
  ExpectScalar("SELECT (SELECT MAX(a) FROM p) = 3", Value(true));
}

TEST_F(SqlParserFeatureCoverageTest, MalformedInputProducesDiagnostics) {
  EXPECT_THROW(Run("SELECT * FROM p WHERE"), std::runtime_error);
  // GROUPING SETS / ROLLUP / CUBE now lower through the UNION-ALL expansion;
  // their malformed shapes still reject with a dedicated diagnostic.
  EXPECT_THROW(Run("SELECT a FROM p GROUP BY GROUPING SETS ((a), ()) ORDER BY "
                   "GROUPING(a)"),
               std::runtime_error);
  EXPECT_THROW(Run("SELECT a FROM missing_table"), std::runtime_error);
  EXPECT_THROW(Run("INSERT INTO p VALUES ('x')"), std::runtime_error);
  // These fail during evaluation; the row stream carries the status.
  EXPECT_THROW(Run("SELECT CAST('zz' AS INT64)"), std::runtime_error);
  EXPECT_THROW(Run("SELECT 1 / 0"), std::runtime_error);
}

}  // namespace tinylamb
