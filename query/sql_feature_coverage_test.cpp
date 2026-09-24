/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// End-to-end coverage tests for engine/executor paths that the regression
// suites do not reach: WITH TIES and LIMIT edge shapes, DISTINCT ON, distinct
// aggregates over typed columns, set operations, CTE self-references (the
// MaterializePlan spool path), outer joins, correlated subqueries, QUALIFY,
// window frames, UNNEST shapes, and the parallel morsel-scan aggregation over
// a table large enough to trigger the multi-worker path.

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
  return result.MoveValue().Collect();
}
}  // namespace

class SqlFeatureCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_feature_coverage_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    Run("CREATE TABLE f (k INT64, v FLOAT64, s STRING, d DATE)");
    Run("INSERT INTO f (k, v, s, d) VALUES "
        "(1, 10.0, 'a', DATE '2026-01-01'), "
        "(2, 20.0, 'b', DATE '2026-01-02'), "
        "(2, 30.0, 'c', DATE '2026-01-02'), "
        "(3, NULL, NULL, NULL)");
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
  void ExpectEq(const std::string& sql, const Value& expected) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(Scalar(sql), expected);
  }
  std::vector<Value> Column(const std::string& sql) {
    std::vector<Value> out;
    for (const Row& row : Run(sql)) {
      out.push_back(row[0]);
    }
    return out;
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

// ------------------------------------------------------- limit / ordering ---

TEST_F(SqlFeatureCoverageTest, LimitOffsetAndWithTiesShapes) {
  auto one = Column("SELECT k FROM f ORDER BY k ASC LIMIT 1 OFFSET 1");
  ASSERT_EQ(one.size(), 1U);
  EXPECT_EQ(one[0], Value(int64_t{2}));

  // The boundary value 2 is tied: WITH TIES carries the duplicate.
  auto ties = Column("SELECT k FROM f ORDER BY k LIMIT 2 WITH TIES");
  ASSERT_EQ(ties.size(), 3U);
  EXPECT_EQ(ties[0], Value(int64_t{1}));
  EXPECT_EQ(ties[1], Value(int64_t{2}));
  EXPECT_EQ(ties[2], Value(int64_t{2}));

  EXPECT_TRUE(Column("SELECT k FROM f LIMIT 0").empty());
  EXPECT_EQ(Column("SELECT k FROM f LIMIT 10").size(), 4U);
}

TEST_F(SqlFeatureCoverageTest, DistinctOnKeepsFirstRowPerKey) {
  const auto rows = Run("SELECT DISTINCT ON (d) k, d FROM f ORDER BY d, k");
  ASSERT_EQ(rows.size(), 3U);
  // The NULL key sorts first and keeps its (only) row; each date keeps the
  // lowest k.
  EXPECT_EQ(rows[0][0], Value(int64_t{3}));
  EXPECT_TRUE(rows[0][1].IsNull());
  EXPECT_EQ(rows[1][0], Value(int64_t{1}));
  EXPECT_EQ(rows[2][0], Value(int64_t{2}));
}

// --------------------------------------------------- distinct aggregates ---

TEST_F(SqlFeatureCoverageTest, DistinctAggregatesAcrossTypes) {
  ExpectEq("SELECT COUNT(DISTINCT v) FROM f WHERE k = 2", Value(int64_t{2}));
  ExpectEq("SELECT AVG(DISTINCT v) FROM f WHERE k = 2", Value(25.0));
  ExpectEq("SELECT MIN(DISTINCT v) FROM f WHERE k = 2", Value(20.0));
  ExpectEq("SELECT MAX(DISTINCT v) FROM f WHERE k = 2", Value(30.0));
  ExpectEq("SELECT COUNT(DISTINCT k) FROM f", Value(int64_t{3}));
  ExpectEq("SELECT SUM(DISTINCT k) FROM f", Value(int64_t{6}));
  // v holds {10, 20, 30, NULL}: three distinct members.
  ExpectEq("SELECT COUNT(DISTINCT v) FROM f", Value(int64_t{3}));
}

TEST_F(SqlFeatureCoverageTest, ArrayAggKeepsElementTypeOverAllNullGroups) {
  // ARRAY_AGG over an all-NULL group keeps the column's declared element
  // type; runtime-only inference produced ARRAY<> here.
  const auto rows =
      Run("SELECT ARRAY_AGG(v ORDER BY k), ARRAY_AGG(s ORDER BY k), "
          "ARRAY_AGG(d ORDER BY k) FROM f WHERE k = 3");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0].AsString(), "ARRAY<FLOAT64>[NULL]");
  EXPECT_EQ(rows[0][1].AsString(), "ARRAY<STRING>[NULL]");
  EXPECT_EQ(rows[0][2].AsString(), "ARRAY<DATE>[NULL]");
  const auto grouped =
      Run("SELECT k, ARRAY_AGG(v) FROM f WHERE k IN (2, 3) GROUP BY k "
          "ORDER BY k");
  ASSERT_EQ(grouped.size(), 2U);
  EXPECT_EQ(grouped[0][1].AsString(), "ARRAY<FLOAT64>[20, 30]");
  EXPECT_EQ(grouped[1][1].AsString(), "ARRAY<FLOAT64>[NULL]");
}

// -------------------------------------------------------- set operations ---

TEST_F(SqlFeatureCoverageTest, SetOperationMatrix) {
  EXPECT_EQ(Column("SELECT 1 AS x UNION ALL SELECT 2").size(), 2U);
  EXPECT_EQ(Column("SELECT 1 AS x UNION DISTINCT SELECT 1").size(), 1U);
  EXPECT_EQ(Column("SELECT 2 AS y INTERSECT DISTINCT SELECT 2").size(), 1U);
  EXPECT_TRUE(Column("SELECT 1 AS z EXCEPT DISTINCT SELECT 1").empty());
  EXPECT_EQ(Column("SELECT k FROM f WHERE k <= 2 UNION ALL SELECT 9").size(),
            4U);
  EXPECT_EQ(Column("SELECT DISTINCT k FROM f UNION ALL SELECT 4").size(), 4U);
  EXPECT_EQ(Column("SELECT DISTINCT k FROM f UNION DISTINCT SELECT 4").size(),
            4U);
}

// ------------------------------------------------------------ CTE spool ---

TEST_F(SqlFeatureCoverageTest, CteSelfJoinAndRecursiveSum) {
  // A CTE referenced twice by one query routes through the materialize/spool
  // plan (and its executor) rather than re-reading the source.
  // f.k holds {1, 2, 2, 3}: the self-join keeps 1*1 + 2*2 + 1*1 pairs.
  EXPECT_EQ(Scalar("WITH x AS (SELECT k FROM f), "
                   "y AS (SELECT a.k AS k1, b.k AS k2 FROM x a JOIN x b "
                   "ON a.k = b.k) SELECT COUNT(*) FROM y"),
            Value(int64_t{6}));
  EXPECT_EQ(Scalar("WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL "
                   "SELECT n + 1 FROM c WHERE n < 5) SELECT SUM(n) FROM c"),
            Value(int64_t{15}));
}

// ------------------------------------------------------------ join shapes ---

TEST_F(SqlFeatureCoverageTest, OuterJoinShapesPadWithNulls) {
  EXPECT_EQ(Column("SELECT g.s FROM f LEFT JOIN (SELECT 1 AS k, 'r' AS s) g "
                   "USING (k)")
                .size(),
            4U);
  EXPECT_EQ(
      Column("SELECT 1 FROM f RIGHT JOIN (SELECT 1 AS k) g USING (k)").size(),
      1U);
  // FULL OUTER keeps unmatched rows from both sides.
  EXPECT_EQ(
      Column("SELECT 1 FROM f FULL OUTER JOIN (SELECT 9 AS k) g ON f.k = g.k")
          .size(),
      5U);
  // IS NOT DISTINCT FROM joins match through NULLs.
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM f a JOIN f b "
                   "ON a.s IS NOT DISTINCT FROM b.s WHERE a.k = 3"),
            Value(int64_t{1}));
}

// --------------------------------------------------------- subquery shapes ---

TEST_F(SqlFeatureCoverageTest, CorrelatedSubqueryShapes) {
  auto exists = Column(
      "SELECT k FROM f WHERE EXISTS "
      "(SELECT 1 FROM f g WHERE g.k = f.k AND g.v > 25.0) "
      "ORDER BY k");
  ASSERT_EQ(exists.size(), 2U);
  EXPECT_EQ(exists[0], Value(int64_t{2}));

  EXPECT_EQ(Scalar("SELECT (SELECT COUNT(*) FROM f g WHERE g.k = f.k) "
                   "FROM f WHERE k = 1"),
            Value(int64_t{1}));

  // IN with a NULL key still finds the NULL-v row's k.
  auto in_sub =
      Column("SELECT k FROM f WHERE k IN (SELECT k FROM f WHERE v IS NULL)");
  ASSERT_EQ(in_sub.size(), 1U);
  EXPECT_EQ(in_sub[0], Value(int64_t{3}));
}

// --------------------------------------------------------- qualify/window ---

TEST_F(SqlFeatureCoverageTest, QualifyFiltersWindowOutput) {
  auto rows = Column(
      "SELECT k FROM f QUALIFY "
      "ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) = 1 "
      "ORDER BY k");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0], Value(int64_t{1}));
  EXPECT_EQ(rows[2], Value(int64_t{3}));
}

TEST_F(SqlFeatureCoverageTest, WindowFrameAggregation) {
  EXPECT_EQ(Scalar("SELECT SUM(v) OVER (ORDER BY k "
                   "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
                   "FROM f WHERE k <= 2 LIMIT 1"),
            Value(10.0));
  EXPECT_EQ(Scalar("SELECT COUNT(*) OVER () FROM f WHERE k = 1"),
            Value(int64_t{1}));
  // RANK over k = {1, 2, 2, 3} is 1, 2, 2, 4.
  auto ranks = Column("SELECT RANK() OVER (ORDER BY k) FROM f ORDER BY k");
  ASSERT_EQ(ranks.size(), 4U);
  EXPECT_EQ(ranks[0], Value(int64_t{1}));
  EXPECT_EQ(ranks[1], Value(int64_t{2}));
  EXPECT_EQ(ranks[3], Value(int64_t{4}));
  EXPECT_EQ(Column("SELECT LAG(v) OVER (ORDER BY k) FROM f WHERE k = 1").size(),
            1U);
}

// ----------------------------------------------------------------- UNNEST ---

TEST_F(SqlFeatureCoverageTest, UnnestShapes) {
  auto series = Column(
      "SELECT num FROM UNNEST(GENERATE_ARRAY(1, 3)) AS num ORDER BY num");
  ASSERT_EQ(series.size(), 3U);
  EXPECT_EQ(series[2], Value(int64_t{3}));
  // Comma-join against a literal array multiplies each row.
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM f, UNNEST([1, 2]) AS u"),
            Value(int64_t{8}));
}

// --------------------------------------------- parallel morsel aggregation ---

TEST_F(SqlFeatureCoverageTest, ParallelAggregationOverLargeTable) {
  // INSERT ... SELECT over UNNEST keeps the wrapped unnest row shape, so the
  // 10k-row table is built from a 4-way cross join of a 10-row seed table.
  Run("CREATE TABLE seed (n INT64)");
  Run("INSERT INTO seed (n) VALUES "
      "(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
  Run("CREATE TABLE big (num INT64)");
  Run("INSERT INTO big (num) "
      "SELECT a.n * 1000 + b.n * 100 + c.n * 10 + d.n "
      "FROM seed a, seed b, seed c, seed d");
  // Whole-table aggregates exercise the typed parallel scan when the table
  // spans enough morsels for multiple workers.
  ExpectEq("SELECT COUNT(*) FROM big", Value(int64_t{10000}));
  ExpectEq("SELECT SUM(num) FROM big", Value(int64_t{61105000}));
  ExpectEq("SELECT MIN(num) FROM big", Value(int64_t{1111}));
  ExpectEq("SELECT MAX(num) FROM big", Value(int64_t{11110}));
  ExpectEq("SELECT AVG(num) FROM big", Value(6110.5));
  // Residual filter (not expressible as a simple compare) over the same
  // parallel machinery.
  ExpectEq("SELECT COUNT(*) FROM big WHERE MOD(num, 2) = 0 OR num > 11110",
           Value(int64_t{5000}));
  // Grouped aggregates must agree with the ungrouped equivalent.
  ExpectEq(
      "SELECT SUM(num) FROM big WHERE MOD(num, 7) = 3 GROUP BY MOD(num, 7)",
      Scalar("SELECT SUM(num) FROM big WHERE MOD(num, 7) = 3"));
  ExpectEq(
      "SELECT AVG(num) FROM big WHERE MOD(num, 7) = 3 GROUP BY MOD(num, 7)",
      Scalar("SELECT AVG(num) FROM big WHERE MOD(num, 7) = 3"));
  ExpectEq(
      "SELECT COUNT(*) FROM (SELECT MOD(num, 7) AS g, COUNT(*) AS c "
      "FROM big GROUP BY g) t",
      Value(int64_t{7}));
}

// ----------------------------------------------------------------- EXPLAIN ---

TEST_F(SqlFeatureCoverageTest, ExplainCoversPlanShapes) {
  for (const std::string sql :
       {"EXPLAIN WITH x AS (SELECT k FROM f) "
        "SELECT a.k FROM x a JOIN x b ON a.k = b.k",
        "EXPLAIN SELECT k FROM f UNION ALL SELECT 9",
        "EXPLAIN SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING "
        "AND CURRENT ROW) FROM f",
        "EXPLAIN SELECT DISTINCT ON (d) k FROM f ORDER BY d, k",
        "EXPLAIN SELECT COUNT(*) FROM f, UNNEST([1, 2]) AS u",
        "EXPLAIN SELECT k FROM f",
        "EXPLAIN SELECT CASE WHEN k = 1 THEN 10 ELSE 20 END + "
        "CASE WHEN k = 1 THEN 10 ELSE 20 END FROM f",
        "EXPLAIN SELECT g.cnt FROM "
        "(SELECT k, COUNT(*) AS cnt FROM f GROUP BY k) g WHERE g.cnt > 1",
        "EXPLAIN ANALYZE SELECT k, SUM(v) FROM f GROUP BY k ORDER BY k",
        "EXPLAIN ANALYZE SELECT DISTINCT s FROM f WHERE k = 2"}) {
    SCOPED_TRACE(sql);
    Explain(sql);
  }
}

}  // namespace tinylamb
