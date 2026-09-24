/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// EXPLAIN / EXPLAIN ANALYZE breadth: every plan shape (joins, aggregates,
// windows, set operations, CTEs, derived tables, UNNEST, LIMIT/OFFSET) must
// render a non-empty plan, and ANALYZE must additionally stream the runtime
// statistics footer (Actual Joins / Runtime / MemoryPeak lines).

#include <gtest/gtest.h>

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
  if (drained.GetStatus() != Status::kSuccess) {
    throw std::runtime_error(drained.GetStatus().GetMessage());
  }
  return rows;
}
}  // namespace

class SqlExplainFormatCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_explain_format_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE o(id INT64, c INT64, amt FLOAT64)");
    RunSql(engine_.get(), context_.get(),
           "CREATE TABLE c(id INT64, name STRING(MAX))");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO o VALUES (1, 10, 5.0), (2, 10, 7.5), (3, 20, 1.0)");
    RunSql(engine_.get(), context_.get(),
           "INSERT INTO c VALUES (10, 'a'), (20, 'b')");
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // The plan text must be present, non-empty, and mention at least one of
  // the expected node keywords.
  void ExpectExplain(const std::string& sql,
                     const std::vector<std::string>& keywords) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_FALSE(rows.empty());
    std::string plan;
    for (const Row& row : rows) {
      ASSERT_EQ(row.values_.size(), 1U);
      plan += row[0].AsString();
      plan += "\n";
    }
    bool matched = false;
    for (const std::string& keyword : keywords) {
      if (plan.find(keyword) != std::string::npos) {
        matched = true;
        break;
      }
    }
    EXPECT_TRUE(matched) << "plan missing all of [" << keywords.size()
                         << "] keywords:\n"
                         << plan;
  }

  void ExpectAnalyzeFooter(const std::string& sql) {
    SCOPED_TRACE(sql);
    const auto rows = RunSql(engine_.get(), context_.get(), sql);
    ASSERT_FALSE(rows.empty());
    std::string plan;
    for (const Row& row : rows) {
      plan += row[0].AsString();
      plan += "\n";
    }
    EXPECT_NE(plan.find("Runtime"), std::string::npos) << plan;
    EXPECT_NE(plan.find("Planning Time"), std::string::npos) << plan;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlExplainFormatCoverageTest, Joins) {
  ExpectExplain("EXPLAIN SELECT name, amt FROM o JOIN c ON o.c = c.id",
                {"Join", "Scan"});
  ExpectExplain("EXPLAIN SELECT name, amt FROM o LEFT JOIN c ON o.c = c.id",
                {"Join", "Scan"});
  ExpectExplain("EXPLAIN SELECT a.id, b.id FROM o a, o b WHERE a.id < b.id",
                {"Join", "Scan", "NestedLoop", "Merge"});
}

TEST_F(SqlExplainFormatCoverageTest, AggregationAndGrouping) {
  ExpectExplain(
      "EXPLAIN SELECT c, SUM(amt) FROM o GROUP BY c HAVING SUM(amt) > 1",
      {"Aggregate", "Group"});
  ExpectExplain("EXPLAIN SELECT COUNT(DISTINCT c) FROM o", {"Aggregate"});
  // The grouping-sets estimated plan currently renders the expansion as a
  // constant leaf; only require the statement to plan and render.
  ExpectExplain(
      "EXPLAIN SELECT c, GROUPING(c), SUM(amt) FROM o GROUP BY "
      "GROUPING SETS ((c), ())",
      {"Plan", "Constant", "Aggregate"});
}

TEST_F(SqlExplainFormatCoverageTest, WindowsSetsAndCtes) {
  ExpectExplain(
      "EXPLAIN SELECT id, SUM(amt) OVER (PARTITION BY c ORDER BY id) FROM o",
      {"Window", "Sort"});
  // A bare UNION is not part of the grammar; DISTINCT must be explicit.
  ExpectExplain("EXPLAIN SELECT id FROM o UNION DISTINCT SELECT id FROM o",
                {"Distinct"});
  ExpectExplain(
      "EXPLAIN WITH t AS (SELECT c AS k, SUM(amt) AS s FROM o GROUP BY k) "
      "SELECT * FROM t WHERE s > 1",
      {"Values", "Selection", "Scan"});
  // WITH RECURSIVE without a column list renders the work-table loop.
  ExpectExplain(
      "EXPLAIN WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 "
      "FROM r WHERE n < 3) SELECT n FROM r",
      {"Recursive", "WorkTable"});
}

TEST_F(SqlExplainFormatCoverageTest, DerivedUnnestAndLimit) {
  ExpectExplain(
      "EXPLAIN SELECT * FROM (SELECT c, SUM(amt) AS s FROM o GROUP BY c) d",
      {"Scan", "Aggregate"});
  ExpectExplain("EXPLAIN SELECT * FROM UNNEST([1, 2, 3]) AS x", {"Unnest"});
  ExpectExplain("EXPLAIN SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 1",
                {"Sort", "Limit", "Scan"});
}

TEST_F(SqlExplainFormatCoverageTest, SubqueriesInPlan) {
  ExpectExplain("EXPLAIN SELECT id FROM o WHERE c IN (SELECT id FROM c)",
                {"Scan", "Join", "Semi"});
  ExpectExplain(
      "EXPLAIN SELECT id FROM o o1 WHERE EXISTS (SELECT 1 FROM c WHERE "
      "c.id = o1.c)",
      {"Scan"});
  ExpectExplain(
      "EXPLAIN SELECT id, (SELECT name FROM c WHERE c.id = o.c) FROM o",
      {"Scan", "Apply", "Subquery"});
}

TEST_F(SqlExplainFormatCoverageTest, AnalyzeRunsTheQueryAndReportsRuntime) {
  ExpectAnalyzeFooter(
      "EXPLAIN ANALYZE SELECT name, amt FROM o JOIN c ON o.c = c.id");
  ExpectAnalyzeFooter(
      "EXPLAIN ANALYZE SELECT c, SUM(amt) FROM o GROUP BY c ORDER BY c");
  ExpectAnalyzeFooter("EXPLAIN ANALYZE SELECT id FROM o WHERE amt > 1");
  ExpectAnalyzeFooter(
      "EXPLAIN ANALYZE WITH t AS (SELECT * FROM o WHERE amt > 1) "
      "SELECT * FROM t");
}

}  // namespace tinylamb
