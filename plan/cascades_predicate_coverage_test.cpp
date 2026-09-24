/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Optimizer coverage sweep: drives the Cascades predicate-analysis and rule
// bodies through a zoo of predicate shapes (range contradictions,
// transitive equalities, NULL/bool tests, IN lists, residual OR trees),
// join shapes (semijoin, antijoin, self-join transitive closure, multi-way
// joins), aggregation HAVING, and index-scan selections.  Every query is
// executed and checked against hand-computed row counts so the sweep stays a
// correctness test even though its main purpose is rule reachability.

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
#include "index/index_schema.hpp"
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
  return result.MoveValue().Collect();
}
}  // namespace

class CascadesPredicateCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ =
        Database::Create("cascades_predicate_coverage_test-" + RandomString())
            .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    Run("CREATE TABLE t (a INT64, b INT64, c FLOAT64, s STRING)");
    // Indexes are created through the database API; the SQL frontend rejects
    // CREATE INDEX DDL with an unsupported-statement diagnostic.
    ASSERT_EQ(
        database_
            ->CreateIndex(*context_, "t",
                          IndexSchema("t_a", {0}, {}, IndexMode::kNonUnique))
            .ok(),
        true);
    // a: {1, 2, 3, NULL, 2}
    Run("INSERT INTO t (a, b, c, s) VALUES "
        "(1, 10, 0.5, 'x'), (2, 20, 1.5, 'y'), (3, 30, 2.5, 'z'), "
        "(NULL, 40, 3.5, 'w'), (2, 50, NULL, 'v')");
    Run("CREATE TABLE u (x INT64, y INT64)");
    // x: {1, 2, 2, 9}
    Run("INSERT INTO u (x, y) VALUES (1, 100), (2, 200), (2, 300), (9, 900)");
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
  int64_t Count(const std::string& sql) {
    const auto rows = Run(sql);
    if (rows.size() != 1U) {
      ADD_FAILURE() << "aggregate query returned " << rows.size()
                    << " rows: " << sql;
      return -1;
    }
    return rows[0][0].value.int_value;
  }
  std::string Explain(const std::string& sql) {
    std::string plan;
    for (const Row& row : Run(sql)) {
      plan += row[0].AsString();
      plan += "\n";
    }
    EXPECT_FALSE(plan.empty());
    return plan;
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(CascadesPredicateCoverageTest, RangePredicateAnalysis) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a > 5 AND a < 3"), 0);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a >= 1 AND a <= 2"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a = 2 AND a = 3"), 0);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a > 1 AND a > 2"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a BETWEEN 2 AND 3"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a NOT BETWEEN 2 AND 3"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a = 2 OR a = 1"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE NOT (a = 2)"), 2);
}

TEST_F(CascadesPredicateCoverageTest, NullTestAnalysis) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IS NULL"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IS NOT NULL"), 4);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IS NULL AND a > 0"), 0);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IS NULL OR a > 2"), 2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE c IS NULL AND s = 'v'"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE b IS NOT NULL AND b >= 40"), 2);
}

TEST_F(CascadesPredicateCoverageTest, InListAnalysis) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IN (1, 2)"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IN (2) AND a = 3"), 0);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IN (1, 9) AND a = 1"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IN (2, 2, 3)"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a NOT IN (2)"), 2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE s IN ('x', 'z')"), 2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a IN (1) OR a IN (3)"), 2);
}

TEST_F(CascadesPredicateCoverageTest, FloatAndStringResidualPredicates) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE c > 1.0"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE c BETWEEN 1.0 AND 2.5"), 2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE c IS NULL"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE s < 'y'"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE s LIKE 'x%'"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE s LIKE '%w%'"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE s = 'x' OR c > 2.0"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE s != 'x' AND s != 'y'"), 3);
}

TEST_F(CascadesPredicateCoverageTest, JoinShapesWithPredicateClosure) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t JOIN u ON t.a = u.x WHERE u.y > 250"),
            2);
  // The a = 1 conjunct closes transitively onto the u side.
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t JOIN u ON t.a = u.x AND t.a = 1"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t p, t q WHERE p.a = q.a"), 6);
  // LEFT JOIN reduced by a right-side NULL rejection.
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t LEFT JOIN u ON t.a = u.x "
                  "WHERE u.x IS NOT NULL"),
            5);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t LEFT JOIN u ON t.a = u.x "
                  "WHERE u.x IS NULL"),
            2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE NOT EXISTS "
                  "(SELECT 1 FROM u WHERE u.x = t.a)"),
            2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE t.a IN "
                  "(SELECT x FROM u WHERE y >= 200)"),
            2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t p JOIN u ON p.a = u.x "
                  "JOIN t q ON q.a = u.x"),
            9);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t JOIN u ON t.a = u.x "
                  "WHERE t.b = u.y"),
            0);
}

TEST_F(CascadesPredicateCoverageTest, AggregateGroupAndHavingShapes) {
  // Groups: a=1 SUM 10, a=2 SUM 70, a=3 SUM 30, NULL SUM 40.
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT a FROM t GROUP BY a "
                  "HAVING SUM(b) > 30) g"),
            2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT a, COUNT(*) AS n FROM t "
                  "GROUP BY a HAVING COUNT(*) >= 2) g"),
            1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT a, SUM(b) AS s FROM t "
                  "GROUP BY a ORDER BY s DESC LIMIT 1) g"),
            1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT s FROM t WHERE b > 15 "
                  "GROUP BY s HAVING MAX(c) > 1.0) g"),
            3);
}

TEST_F(CascadesPredicateCoverageTest, IndexScanSelections) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a = 2"), 2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a > 1"), 3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE a = 2 AND b = 50"), 1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT a FROM t WHERE a = 2) p"), 2);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT a FROM t ORDER BY a "
                  "LIMIT 2) p"),
            2);
  // The EXPLAIN of an equality on the indexed column renders a plan.
  Explain("EXPLAIN SELECT a FROM t WHERE a = 2");
  Explain("EXPLAIN SELECT a FROM t WHERE a > 1 AND a < 3");
  Explain("EXPLAIN SELECT b FROM t WHERE a = 1 OR a = 3");
}

TEST_F(CascadesPredicateCoverageTest, SubqueryAndSetOpShapes) {
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT x FROM u WHERE x <= 2 "
                  "UNION ALL SELECT 42) z"),
            4);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT x FROM u "
                  "INTERSECT DISTINCT SELECT 2) z"),
            1);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT DISTINCT a FROM t WHERE "
                  "a IS NOT NULL) z"),
            3);
  // a IN (1, 2, 9) keeps a = 1 and both a = 2 rows; NULL and 3 drop out.
  EXPECT_EQ(Count("SELECT COUNT(*) FROM (SELECT a AS v FROM t) z WHERE v IN "
                  "(SELECT x FROM u)"),
            3);
  EXPECT_EQ(Count("SELECT COUNT(*) FROM t WHERE EXISTS "
                  "(SELECT 1 FROM u WHERE u.x = t.a AND u.y > 250)"),
            2);
}

}  // namespace tinylamb
