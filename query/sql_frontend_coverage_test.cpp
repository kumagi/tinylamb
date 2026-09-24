/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Grammar sweep over the GoogleSQL AST visitor: every statement shape below
// is one whose visitor branch carried no coverage.  Expectations were pinned
// against the engine itself.

#include <gtest/gtest.h>

#include <memory>
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
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    if (engine->LastError().empty()) {
      prepared = engine->Prepare(*ctx, sql);
    }
    if (!prepared.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

}  // namespace

class SqlFrontendCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_frontend_coverage_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    ExpectQuery("CREATE TABLE t(k INT64, s STRING(MAX), f FLOAT64, d DATE)",
                {Row({Value("CREATE TABLE"), Value(0)})});
    ExpectQuery(
        "INSERT INTO t (k, s, f, d) VALUES "
        "(1, 'a', 1.5, DATE '2026-01-01'), (2, 'b', 2.5, DATE '2026-01-02'), "
        "(3, 'a', 3.5, DATE '2026-01-03'), (4, NULL, NULL, NULL)",
        {Row({Value("Insert Rows"), Value(4)})});
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  void ExpectQuery(const std::string& sql, const std::vector<Row>& expected) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      EXPECT_EQ(rows[i], expected[i]);
    }
  }
  void ExpectScalar(const std::string& sql, Value expected) {
    SCOPED_TRACE(sql);
    ExpectQuery(sql, {Row({std::move(expected)})});
  }
  void ExpectError(const std::string& sql) {
    SCOPED_TRACE(sql);
    EXPECT_THROW(
        { RunSql(engine_.get(), context_.get(), sql); }, std::exception);
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

// ------------------------------------------------------------- set ops -----

TEST_F(SqlFrontendCoverageTest, SetOperationForms) {
  ExpectQuery(
      "SELECT k FROM t WHERE k = 1 UNION ALL "
      "SELECT k FROM t WHERE k = 2 ORDER BY k",
      {Row({Value(1)}), Row({Value(2)})});
  ExpectScalar(
      "SELECT k FROM t WHERE k = 1 UNION DISTINCT "
      "SELECT k FROM t WHERE k = 1",
      Value(1));
  ExpectQuery(
      "SELECT k FROM t INTERSECT DISTINCT SELECT k FROM t ORDER BY k",
      {Row({Value(1)}), Row({Value(2)}), Row({Value(3)}), Row({Value(4)})});
  ExpectQuery(
      "SELECT k FROM t EXCEPT ALL SELECT k FROM t WHERE k = 2 "
      "ORDER BY k",
      {Row({Value(1)}), Row({Value(3)}), Row({Value(4)})});
  ExpectQuery(
      "SELECT c FROM (SELECT COUNT(*) AS c FROM t) UNION ALL "
      "SELECT c FROM (SELECT COUNT(*) AS c FROM t WHERE k = 2)",
      {Row({Value(4)}), Row({Value(1)})});
  ExpectQuery(
      "SELECT COUNT(*) FROM (SELECT k FROM t LIMIT 2 OFFSET 1) "
      "UNION ALL SELECT * FROM UNNEST([9])",
      {Row({Value(2)}), Row({Value(9)})});
}

// ------------------------------------------------------ window functions ---

TEST_F(SqlFrontendCoverageTest, WindowFunctionFrames) {
  ExpectQuery(
      "SELECT k, SUM(k) OVER (PARTITION BY s ORDER BY k "
      "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY k",
      {Row({Value(1), Value(1)}), Row({Value(2), Value(2)}),
       Row({Value(3), Value(4)}), Row({Value(4), Value(4)})});
  ExpectQuery(
      "SELECT k, SUM(k) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED "
      "PRECEDING AND CURRENT ROW) FROM t ORDER BY k",
      {Row({Value(1), Value(1)}), Row({Value(2), Value(3)}),
       Row({Value(3), Value(6)}), Row({Value(4), Value(10)})});
  ExpectQuery(
      "SELECT k, COUNT(*) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND "
      "UNBOUNDED FOLLOWING) FROM t ORDER BY k",
      {Row({Value(1), Value(4)}), Row({Value(2), Value(4)}),
       Row({Value(3), Value(4)}), Row({Value(4), Value(4)})});
  ExpectQuery(
      "SELECT k, ROW_NUMBER() OVER (PARTITION BY s ORDER BY k) FROM t "
      "ORDER BY k",
      {Row({Value(1), Value(1)}), Row({Value(2), Value(1)}),
       Row({Value(3), Value(2)}), Row({Value(4), Value(1)})});
  ExpectQuery("SELECT k, RANK() OVER (ORDER BY s) FROM t ORDER BY k",
              {Row({Value(1), Value(2)}), Row({Value(2), Value(4)}),
               Row({Value(3), Value(2)}), Row({Value(4), Value(1)})});
  ExpectQuery(
      "SELECT k, LAG(k) OVER (ORDER BY k), "
      "LEAD(k, 1, 0) OVER (ORDER BY k) FROM t ORDER BY k",
      {Row({Value(1), Value(), Value(2)}), Row({Value(2), Value(1), Value(3)}),
       Row({Value(3), Value(2), Value(4)}),
       Row({Value(4), Value(3), Value(0)})});
  ExpectQuery(
      "SELECT k, FIRST_VALUE(k) OVER (ORDER BY k), "
      "LAST_VALUE(k) OVER (ORDER BY k) FROM t ORDER BY k",
      {Row({Value(1), Value(1), Value(1)}), Row({Value(2), Value(1), Value(2)}),
       Row({Value(3), Value(1), Value(3)}),
       Row({Value(4), Value(1), Value(4)})});
  ExpectQuery("SELECT k, NTILE(2) OVER (ORDER BY k) FROM t ORDER BY k",
              {Row({Value(1), Value(1)}), Row({Value(2), Value(1)}),
               Row({Value(3), Value(2)}), Row({Value(4), Value(2)})});
  ExpectQuery("SELECT k, SUM(k) OVER () FROM t ORDER BY k",
              {Row({Value(1), Value(10)}), Row({Value(2), Value(10)}),
               Row({Value(3), Value(10)}), Row({Value(4), Value(10)})});
}

// ---------------------------------------------------------- aggregates -----

TEST_F(SqlFrontendCoverageTest, AggregateFunctionForms) {
  ExpectScalar("SELECT ARRAY_AGG(k ORDER BY k DESC) FROM t",
               Value::Array({Value(4), Value(3), Value(2), Value(1)}, "INT64"));
  ExpectScalar("SELECT STRING_AGG(s, ',' ORDER BY k) FROM t", Value("a,b,a"));
  ExpectScalar("SELECT STRING_AGG(s, ',') FROM t", Value("a,b,a"));
  ExpectScalar("SELECT COUNT(DISTINCT s) FROM t", Value(2));
  ExpectScalar("SELECT APPROX_TOP_SUM(k, k, 2) FROM t",
               Value::Array({Value("{4, 4}"), Value("{3, 3}")}, "STRING"));
}

// -------------------------------------------------------------- UNNEST -----

TEST_F(SqlFrontendCoverageTest, UnnestForms) {
  ExpectQuery("SELECT * FROM UNNEST([1, 2, 3])",
              {Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  ExpectQuery("SELECT x FROM UNNEST([1, 2, 3]) AS x ORDER BY x",
              {Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  ExpectQuery(
      "SELECT k, u FROM t CROSS JOIN UNNEST([10, 20]) AS u "
      "ORDER BY k, u",
      {Row({Value(1), Value(10)}), Row({Value(1), Value(20)}),
       Row({Value(2), Value(10)}), Row({Value(2), Value(20)}),
       Row({Value(3), Value(10)}), Row({Value(3), Value(20)}),
       Row({Value(4), Value(10)}), Row({Value(4), Value(20)})});
  ExpectQuery(
      "SELECT k, u FROM t LEFT JOIN UNNEST([10]) AS u ON TRUE "
      "ORDER BY k",
      {Row({Value(1), Value(10)}), Row({Value(2), Value(10)}),
       Row({Value(3), Value(10)}), Row({Value(4), Value(10)})});
}

// ---------------------------------------------------------------- PIVOT ----

TEST_F(SqlFrontendCoverageTest, PivotForms) {
  ExpectQuery("SELECT s, k FROM t PIVOT(SUM(k) FOR s IN ('a', 'b'))",
              {Row({Value(R"({"a":4,"b":2})"), Value(R"({"a":4,"b":2})")})});
  ExpectScalar(
      "SELECT * FROM (SELECT k, s FROM t WHERE k < 3) "
      "PIVOT(COUNT(k) FOR s IN ('a'))",
      Value(1));
}

// ---------------------------------------------------------------- joins ----

TEST_F(SqlFrontendCoverageTest, JoinForms) {
  ExpectQuery(
      "SELECT a.k AS ak, b.k AS bk FROM t a JOIN t b ON a.k < b.k "
      "ORDER BY ak, bk",
      {Row({Value(1), Value(2)}), Row({Value(1), Value(3)}),
       Row({Value(1), Value(4)}), Row({Value(2), Value(3)}),
       Row({Value(2), Value(4)}), Row({Value(3), Value(4)})});
  ExpectQuery(
      "SELECT t.k FROM t FULL OUTER JOIN t t2 ON t.k = t2.k + 10 "
      "ORDER BY t.k",
      {Row({Value()}), Row({Value()}), Row({Value()}), Row({Value()}),
       Row({Value(1)}), Row({Value(2)}), Row({Value(3)}), Row({Value(4)})});
  ExpectScalar("SELECT COUNT(*) FROM t, t t2 WHERE t.k = t2.k", Value(4));
  ExpectQuery(
      "SELECT t.k FROM t JOIN t t2 USING (k) ORDER BY k",
      {Row({Value(1)}), Row({Value(2)}), Row({Value(3)}), Row({Value(4)})});
  // NATURAL JOIN is rejected rather than degenerating into a cross product.
  ExpectError("SELECT k FROM t NATURAL JOIN t t2");
}

// ----------------------------------------------------------------- CTEs ----

TEST_F(SqlFrontendCoverageTest, RecursiveAndChainedCtes) {
  ExpectScalar(
      "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
      "FROM cte WHERE n < 5) SELECT SUM(n) FROM cte",
      Value(15));
  ExpectScalar(
      "WITH a AS (SELECT k FROM t), b AS (SELECT s FROM t) "
      "SELECT COUNT(*) FROM a, b",
      Value(16));
}

// ---------------------------------------------------- CASE and predicates --

TEST_F(SqlFrontendCoverageTest, CaseAndPredicateForms) {
  ExpectQuery(
      "SELECT CASE k WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' "
      "END FROM t ORDER BY k",
      {Row({Value("one")}), Row({Value("two")}), Row({Value("many")}),
       Row({Value("many")})});
  ExpectQuery("SELECT CASE WHEN k > 2 THEN 'big' END FROM t ORDER BY k",
              {Row({Value()}), Row({Value()}), Row({Value("big")}),
               Row({Value("big")})});
  ExpectQuery(
      "SELECT k IS TRUE, k IS NOT TRUE, TRUE IS FALSE, NULL IS NULL, "
      "NULL IS NOT NULL FROM t WHERE k = 1",
      {Row({Value(1), Value(0), Value(0), Value(1), Value(0)})});
  ExpectQuery("SELECT k FROM t WHERE k BETWEEN 1 AND 2 ORDER BY k",
              {Row({Value(1)}), Row({Value(2)})});
  ExpectQuery("SELECT k FROM t WHERE k NOT BETWEEN 1 AND 2 ORDER BY k",
              {Row({Value(3)}), Row({Value(4)})});
}

// ------------------------------------------------------------- ordering ----

TEST_F(SqlFrontendCoverageTest, OrderByNullsFirstAndLast) {
  ExpectQuery("SELECT s FROM t ORDER BY s NULLS FIRST, k",
              {Row({Value()}), Row({Value("a")}), Row({Value("a")}),
               Row({Value("b")})});
  ExpectQuery("SELECT s FROM t ORDER BY s NULLS LAST, k",
              {Row({Value("a")}), Row({Value("a")}), Row({Value("b")}),
               Row({Value()})});
  ExpectScalar("SELECT COUNT(*) FROM t GROUP BY ()", Value(4));
  ExpectScalar("SELECT MIN(f) FROM t HAVING MIN(f) > 0", Value(1.5));
}

// --------------------------------------------------------- scalar subqueries

TEST_F(SqlFrontendCoverageTest, SubqueryForms) {
  ExpectQuery(
      "SELECT k FROM t WHERE k IN (SELECT k FROM t WHERE k < 3) "
      "ORDER BY k",
      {Row({Value(1)}), Row({Value(2)})});
  ExpectQuery("SELECT k FROM t WHERE EXISTS (SELECT 1) ORDER BY k LIMIT 1",
              {Row({Value(1)})});
  ExpectQuery("SELECT k FROM t WHERE k = ANY (SELECT k FROM t WHERE k = 2)",
              {Row({Value(2)})});
  ExpectScalar("SELECT (SELECT MAX(k) FROM t)", Value(4));
  ExpectScalar("SELECT ARRAY(SELECT k FROM t WHERE k < 3 ORDER BY k)",
               Value::Array({Value(1), Value(2)}, "INT64"));
}

// ----------------------------------------------------------------- UDFs ----

TEST_F(SqlFrontendCoverageTest, SqlUdfLifecycle) {
  ExpectQuery("CREATE FUNCTION add_one(x INT64) AS (x + 1)", {Row({Value(0)})});
  ExpectScalar("SELECT add_one(41)", Value(42));
  // Arity is checked per call, not at creation time.
  ExpectError("SELECT add_one(1, 2)");
  // Self-recursive UDFs hit the expansion-depth limit instead of looping.
  ExpectQuery("CREATE FUNCTION rec(x INT64) AS (rec(x))", {Row({Value(0)})});
  ExpectError("SELECT rec(1)");
  ExpectError("SELECT no_such_function(1)");
}

// ------------------------------------------------------- DDL and ANALYZE ---

TEST_F(SqlFrontendCoverageTest, DdlAndAnalyze) {
  ExpectQuery(
      "CREATE TABLE child(id INT64, pid INT64, FOREIGN KEY (pid) "
      "REFERENCES t(k) NOT ENFORCED)",
      {Row({Value("CREATE TABLE"), Value(0)})});
  ExpectQuery("ANALYZE t", {Row({Value("ANALYZE"), Value("t"), Value(4)})});
  ExpectError("CREATE TABLE t(k INT64)");
  ExpectError("INSERT INTO t (nope) VALUES (1)");
}

// ---------------------------------------------------- rejected constructs --

TEST_F(SqlFrontendCoverageTest, RejectedSyntax) {
  ExpectError("SELECT no_such_relation.* FROM t");
  ExpectError("SELECT s FROM t WHERE s ILIKE 'A%'");
  // Deeply nested expressions hit the visitor's depth guard.
  std::string deep = "SELECT";
  for (int i = 0; i < 120; ++i) {
    deep += " CASE WHEN TRUE THEN";
  }
  deep += " 1";
  for (int i = 0; i < 120; ++i) {
    deep += " END";
  }
  ExpectError(deep);
}

// ------------------------------------------------------------ misc sweep ---

TEST_F(SqlFrontendCoverageTest, MiscStatementShapes) {
  ExpectQuery("SELECT DISTINCT s FROM t ORDER BY s",
              {Row({Value()}), Row({Value("a")}), Row({Value("b")})});
  ExpectScalar(
      "SELECT COUNT(*) FROM t UNION ALL SELECT * FROM UNNEST([1]) "
      "LIMIT 1 OFFSET 1",
      Value(1));
  ExpectScalar("SELECT SAFE_CAST('x' AS INT64)", Value());
  ExpectScalar("SELECT EXTRACT(HOUR FROM DATETIME '2026-01-02 03:04:05')",
               Value(3));
  ExpectScalar("SELECT CAST(d AS STRING) FROM t WHERE d IS NOT NULL LIMIT 1",
               Value("2026-01-01"));
  ExpectScalar("SELECT k AS kk FROM t ORDER BY kk LIMIT 1", Value(1));
  ExpectError("SELECT k/0 FROM t");
  ExpectError("SELECT SUM(s) FROM t");
  ExpectError("SELECT no_such_col FROM t");
  ExpectError("SELECT k FROM no_such_table");
}

}  // namespace tinylamb
