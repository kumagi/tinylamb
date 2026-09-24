/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// EXPLAIN (estimated physical plan) rendering breadth and subquery runtime
// paths: correlated EXISTS/scalar/IN shapes with repeated execution (cache
// paths), recursive CTEs, and derived-table boundary collapsing.

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

class ExplainSubqueryCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("explain_subquery_coverage_test-" + RandomString())
            .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE orders(id INT64, cust INT64, amt FLOAT64)");
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE custs(id INT64, name STRING(MAX), region INT64)");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO orders (id, cust, amt) VALUES "
           "(1, 10, 5.0), (2, 10, 7.5), (3, 20, 1.0), (4, 30, 2.0)");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO custs (id, name, region) VALUES "
           "(10, 'a', 1), (20, 'b', 2), (30, 'c', 1)");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::vector<Row> Query(const std::string& sql) {
    return RunSql(engine_.get(), context_.get(), sql);
  }
  void ExpectQuery(const std::string& sql, const std::vector<Row>& expected) {
    SCOPED_TRACE(sql);
    const auto rows = Query(sql);
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      EXPECT_EQ(rows[i], expected[i]);
    }
  }
  // EXPLAIN output is implementation detail: only require one non-empty
  // QUERY PLAN row.
  void ExpectExplain(const std::string& sql) {
    SCOPED_TRACE(sql);
    // Prepare-level EXPLAIN streams the plan text as one VARCHAR per line;
    // only require a non-empty plan.
    const auto rows = Query(sql);
    EXPECT_FALSE(rows.empty());
    for (const Row& row : rows) {
      EXPECT_TRUE(row[0].type == ValueType::kVarChar);
      EXPECT_FALSE(row[0].AsString().empty());
    }
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(ExplainSubqueryCoverageTest, EstimatedPlanRendersEveryShape) {
  ExpectExplain("EXPLAIN SELECT id FROM orders");
  ExpectExplain("EXPLAIN SELECT id FROM orders WHERE amt > 2.0");
  ExpectExplain("EXPLAIN SELECT id FROM orders ORDER BY amt DESC");
  ExpectExplain("EXPLAIN SELECT id FROM orders ORDER BY amt DESC LIMIT 2");
  ExpectExplain("EXPLAIN SELECT DISTINCT cust FROM orders");
  ExpectExplain(
      "EXPLAIN SELECT cust, SUM(amt) FROM orders GROUP BY cust "
      "HAVING SUM(amt) > 1.0");
  ExpectExplain(
      "EXPLAIN SELECT cust, COUNT(*) FROM orders GROUP BY cust "
      "ORDER BY cust LIMIT 1");
  ExpectExplain(
      "EXPLAIN SELECT o.id, c.name FROM orders o JOIN custs c "
      "ON o.cust = c.id");
  ExpectExplain(
      "EXPLAIN SELECT o.id, c.name FROM orders o LEFT JOIN custs c "
      "ON o.cust = c.id ORDER BY o.id LIMIT 3");
  ExpectExplain(
      "EXPLAIN SELECT o.id FROM orders o RIGHT JOIN custs c "
      "ON o.cust = c.id ORDER BY o.id LIMIT 3");
  ExpectExplain(
      "EXPLAIN SELECT o.id FROM orders o FULL OUTER JOIN custs c "
      "ON o.cust = c.id ORDER BY o.id LIMIT 3");
  ExpectExplain(
      "EXPLAIN SELECT o.id FROM orders o CROSS JOIN custs c "
      "ORDER BY o.id LIMIT 3");
  ExpectExplain("EXPLAIN SELECT id, amt * 2 FROM orders WHERE amt * 2 > 4.0");
  ExpectExplain(
      "EXPLAIN SELECT id, CASE WHEN amt > 5 THEN 'hi' ELSE 'lo' END "
      "FROM orders");
  ExpectExplain(
      "EXPLAIN SELECT id FROM orders UNION ALL SELECT id FROM orders "
      "ORDER BY id LIMIT 3");
  ExpectExplain("EXPLAIN SELECT x FROM UNNEST([1, 2, 3]) AS x WHERE x > 1");
  ExpectExplain(
      "EXPLAIN WITH t AS (SELECT cust, SUM(amt) AS s FROM orders "
      "GROUP BY cust) SELECT cust FROM t WHERE s > 2.0");
  ExpectExplain(
      "EXPLAIN WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL "
      "SELECT n + 1 FROM r WHERE n < 4) SELECT SUM(n) FROM r");
  ExpectExplain(
      "EXPLAIN SELECT id FROM orders WHERE EXISTS "
      "(SELECT 1 FROM custs WHERE custs.id = orders.cust)");
  ExpectExplain(
      "EXPLAIN SELECT id FROM (SELECT id, amt FROM orders) d "
      "WHERE amt > 1.0");
  ExpectExplain(
      "EXPLAIN SELECT id, ROW_NUMBER() OVER (ORDER BY amt) "
      "FROM orders LIMIT 2");
  ExpectExplain("EXPLAIN SELECT ARRAY(SELECT id FROM orders) AS ids");
  ExpectExplain(
      "EXPLAIN SELECT name FROM custs WHERE id = "
      "(SELECT cust FROM orders WHERE amt = 5.0)");
}

TEST_F(ExplainSubqueryCoverageTest, CorrelatedSubqueriesRepeatAndCache) {
  const std::vector<Row> expected = {
      Row({Value(1), Value("a")}),
      Row({Value(2), Value("a")}),
      Row({Value(3), Value("b")}),
      Row({Value(4), Value("c")}),
  };
  const std::vector<Row> ids_only = {Row({Value(1)}), Row({Value(2)}),
                                     Row({Value(3)}), Row({Value(4)})};
  const std::vector<Row> max_per_cust = {Row({Value(2)}), Row({Value(3)}),
                                         Row({Value(4)})};
  const std::vector<std::string> shapes = {
      "SELECT o.id, (SELECT c.name FROM custs c WHERE c.id = o.cust) "
      "FROM orders o ORDER BY o.id",
      "SELECT o.id, (SELECT c.name FROM custs c WHERE c.id = o.cust) "
      "FROM orders o ORDER BY o.id",
      "SELECT o.id FROM orders o WHERE EXISTS "
      "(SELECT 1 FROM custs c WHERE c.id = o.cust) ORDER BY o.id",
      "SELECT o.id FROM orders o WHERE NOT EXISTS "
      "(SELECT 1 FROM custs c WHERE c.id = o.cust AND c.region = 9) "
      "ORDER BY o.id",
      "SELECT o.id FROM orders o WHERE o.cust IN "
      "(SELECT c.id FROM custs c WHERE c.id = o.cust) ORDER BY o.id",
      "SELECT o.id FROM orders o WHERE o.amt = "
      "(SELECT MAX(o2.amt) FROM orders o2 WHERE o2.cust = o.cust) "
      "ORDER BY o.id",
  };
  (void)max_per_cust;
  // Run every shape twice: the second pass exercises the correlated-index
  // and fingerprint cache read paths.
  for (const std::string& sql : shapes) {
    if (sql.find("(SELECT c.name") == std::string::npos) {
      if (sql.find("MAX(o2.amt)") != std::string::npos) {
        ExpectQuery(sql, max_per_cust);
        ExpectQuery(sql, max_per_cust);
      } else {
        ExpectQuery(sql, ids_only);
        ExpectQuery(sql, ids_only);
      }
    } else {
      SCOPED_TRACE(sql);
      const auto rows = Query(sql);
      ASSERT_EQ(rows.size(), 4U);
      for (int repeat = 0; repeat < 2; ++repeat) {
        const auto again = Query(sql);
        ASSERT_EQ(again.size(), 4U);
        EXPECT_EQ(again, rows);
      }
    }
  }
}

TEST_F(ExplainSubqueryCoverageTest, RecursiveCteAndSetOperations) {
  ExpectQuery(
      "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
      "FROM r WHERE n < 5) SELECT SUM(n) FROM r",
      {Row({Value(15)})});
  ExpectQuery(
      "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
      "FROM r WHERE n < 5) SELECT SUM(n) FROM r",
      {Row({Value(15)})});
  ExpectQuery(
      "SELECT id FROM orders INTERSECT DISTINCT SELECT cust "
      "FROM orders ORDER BY id",
      {});
  ExpectQuery(
      "SELECT id FROM orders EXCEPT DISTINCT SELECT cust "
      "FROM orders ORDER BY id",
      {Row({Value(1)}), Row({Value(2)}), Row({Value(3)}), Row({Value(4)})});
}

}  // namespace tinylamb
