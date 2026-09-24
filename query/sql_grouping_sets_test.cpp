/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// GROUPING SETS / ROLLUP / CUBE surface: the AST visitor expands the
// constructors into a UNION ALL of plain GROUP BY branches, NULL-pads
// inactive keys, folds GROUPING() to per-branch constants and keeps the
// statement-level ORDER BY / LIMIT on the wrapper. Expectations pinned
// against GoogleSQL semantics.

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

class SqlGroupingSetsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("sql_grouping_sets_test-" + RandomString())
                    .MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
    // a: {1, 1, 2, NULL}; b: {p1, p2, q1, n1}; x: {10, 20, 30, 40}.
    Run("CREATE TABLE g (a INT64, b STRING, x INT64)");
    Run("INSERT INTO g (a, b, x) VALUES (1, 'p1', 10), (1, 'p2', 20), "
        "(2, 'q1', 30), (NULL, 'n1', 40)");
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
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

// ROLLUP(a) over the base data: (a)-groups plus the grand total with the key
// NULL-padded.
TEST_F(SqlGroupingSetsTest, RollupSingleKeyProducesSubtotalAndGrandTotal) {
  const auto rows =
      Run("SELECT a, SUM(x) AS s FROM g GROUP BY ROLLUP(a) "
          "ORDER BY a NULLS LAST");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{30}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(int64_t{30}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_EQ(rows[2][1], Value(int64_t{40}));
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(int64_t{100}));
}

// ROLLUP(a, b) hierarchy: (a,b), (a) subtotals and the () grand total.
TEST_F(SqlGroupingSetsTest, RollupTwoKeysProducesHierarchy) {
  const auto rows =
      Run("SELECT a, b, SUM(x) AS s, GROUPING(a) AS ga, GROUPING(b) AS gb "
          "FROM g GROUP BY ROLLUP(a, b) "
          "ORDER BY ga, gb, a NULLS LAST, b NULLS LAST");
  ASSERT_EQ(rows.size(), 8U);
  // Set (a, b): four pair rows, ga=gb=0; a NULLS LAST puts NULL a last.
  EXPECT_EQ(rows[0][3], Value(int64_t{0}));
  EXPECT_EQ(rows[3][3], Value(int64_t{0}));
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(std::string("p1")));
  EXPECT_EQ(rows[0][2], Value(int64_t{10}));
  EXPECT_EQ(rows[1][1], Value(std::string("p2")));
  EXPECT_EQ(rows[1][2], Value(int64_t{20}));
  EXPECT_EQ(rows[2][1], Value(std::string("q1")));
  EXPECT_EQ(rows[2][2], Value(int64_t{30}));
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(std::string("n1")));
  EXPECT_EQ(rows[3][2], Value(int64_t{40}));
  // Set (a): subtotals with b NULL-padded (gb=1, ga=0).
  EXPECT_EQ(rows[4][3], Value(int64_t{0}));
  EXPECT_EQ(rows[4][4], Value(int64_t{1}));
  EXPECT_TRUE(rows[4][1].IsNull());
  EXPECT_EQ(rows[4][2], Value(int64_t{30}));
  EXPECT_TRUE(rows[5][1].IsNull());
  EXPECT_EQ(rows[5][2], Value(int64_t{30}));
  EXPECT_TRUE(rows[6][1].IsNull());
  EXPECT_EQ(rows[6][2], Value(int64_t{40}));
  // Set (): grand total with both keys NULL (ga=gb=1).
  EXPECT_EQ(rows[7][3], Value(int64_t{1}));
  EXPECT_EQ(rows[7][4], Value(int64_t{1}));
  EXPECT_TRUE(rows[7][0].IsNull());
  EXPECT_TRUE(rows[7][1].IsNull());
  EXPECT_EQ(rows[7][2], Value(int64_t{100}));
}

// GROUPING() folds to 0/1 constants per branch: 0 when the argument is an
// active key, 1 when it is NULL-padded.
TEST_F(SqlGroupingSetsTest, GroupingFunctionFoldsPerBranch) {
  const auto rows =
      Run("SELECT a, GROUPING(a) AS ga FROM g GROUP BY "
          "ROLLUP(a) ORDER BY ga, a NULLS LAST");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][1], Value(int64_t{0}));
  EXPECT_EQ(rows[1][1], Value(int64_t{0}));
  EXPECT_EQ(rows[2][1], Value(int64_t{0}));
  EXPECT_EQ(rows[3][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[3][0].IsNull());
}

// Multi-argument GROUPING shares GROUPING_ID's GoogleSQL bit-vector order:
// the leftmost argument is the most significant bit, so
// GROUPING(a, b) == 2*GROUPING(a) + GROUPING(b).
TEST_F(SqlGroupingSetsTest, GroupingMultiArgBuildsBitmask) {
  const auto rows =
      Run("SELECT a, b, GROUPING(a, b) AS gid FROM g "
          "GROUP BY GROUPING SETS ((a, b), (a), (b), ()) ORDER BY gid");
  ASSERT_EQ(rows.size(), 12U);
  // Set (a, b) => gid 0 (four pair rows).
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(rows[i][2], Value(int64_t{0}));
  }
  // Set (a): b not grouped => gid 1.
  for (size_t i = 4; i < 7; ++i) {
    EXPECT_EQ(rows[i][2], Value(int64_t{1}));
    EXPECT_TRUE(rows[i][1].IsNull());
  }
  // Set (b): a not grouped => gid 2.
  for (size_t i = 7; i < 11; ++i) {
    EXPECT_EQ(rows[i][2], Value(int64_t{2}));
    EXPECT_TRUE(rows[i][0].IsNull());
  }
  // Set (): gid 3; it sorts last under gid ordering.
  EXPECT_EQ(rows[11][2], Value(int64_t{3}));
}

// GROUPING_ID folds with the same GoogleSQL bit order as multi-argument
// GROUPING: GROUPING_ID(a, b) == 2*GROUPING(a) + GROUPING(b).
TEST_F(SqlGroupingSetsTest, GroupingIdBuildsBitVector) {
  const auto rows =
      Run("SELECT a, b, GROUPING_ID(a, b) AS gid FROM g "
          "GROUP BY GROUPING SETS ((a, b), (a), (b), ()) ORDER BY gid");
  ASSERT_EQ(rows.size(), 12U);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(rows[i][2], Value(int64_t{0}));
  }
  for (size_t i = 4; i < 7; ++i) {
    EXPECT_EQ(rows[i][2], Value(int64_t{1}));
    EXPECT_TRUE(rows[i][1].IsNull());
  }
  for (size_t i = 7; i < 11; ++i) {
    EXPECT_EQ(rows[i][2], Value(int64_t{2}));
    EXPECT_TRUE(rows[i][0].IsNull());
  }
  EXPECT_EQ(rows[11][2], Value(int64_t{3}));
}

// CUBE(a, b) enumerates all subsets: (a,b), (a), (b), ().
TEST_F(SqlGroupingSetsTest, CubeEnumeratesAllSubsets) {
  const auto rows =
      Run("SELECT COUNT(*) FROM ("
          "  SELECT a, b, COUNT(*) AS c FROM g GROUP BY CUBE(a, b)"
          ")");
  ASSERT_EQ(rows.size(), 1U);
  // 4 pair rows + 3 a-groups + 4 b-groups + 1 grand total.
  EXPECT_EQ(rows[0][0], Value(int64_t{12}));
}

// The empty grouping set aggregates even empty input into one row.
TEST_F(SqlGroupingSetsTest, EmptyGroupingSetYieldsGrandTotalRow) {
  Run("CREATE TABLE empty_g (a INT64, x INT64)");
  const auto rows =
      Run("SELECT COUNT(*) AS c, SUM(x) AS s FROM empty_g "
          "GROUP BY GROUPING SETS (())");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{0}));
  EXPECT_TRUE(rows[0][1].IsNull());
}

// A single-set GROUPING SETS is a plain GROUP BY; no union needed.
TEST_F(SqlGroupingSetsTest, SingleGroupingSetBehavesAsPlainGroupBy) {
  const auto rows =
      Run("SELECT a, COUNT(*) AS c FROM g GROUP BY GROUPING "
          "SETS ((a)) ORDER BY a NULLS LAST");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(int64_t{1}));
}

// Mixed plain items join every constructor set: GROUP BY a, ROLLUP(b) is
// GROUPING SETS ((a, b), (a)) -- no () grand total.
TEST_F(SqlGroupingSetsTest, PlainKeysIntersectEveryRollupSet) {
  const auto rows =
      Run("SELECT a, b, COUNT(*) AS c FROM g GROUP BY a, ROLLUP(b) "
          "ORDER BY a NULLS LAST, b NULLS LAST");
  ASSERT_EQ(rows.size(), 7U);
  // (a, b) rows first (b NULLS LAST within each a), then the (a) subtotals.
  EXPECT_EQ(rows[0][1], Value(std::string("p1")));
  EXPECT_EQ(rows[0][2], Value(int64_t{1}));
  EXPECT_TRUE(rows[2][1].IsNull());
  EXPECT_EQ(rows[2][2], Value(int64_t{2}));
  EXPECT_TRUE(rows[4][1].IsNull());
  EXPECT_EQ(rows[4][2], Value(int64_t{1}));
  EXPECT_TRUE(rows[6][1].IsNull());
  EXPECT_EQ(rows[6][2], Value(int64_t{1}));
}

// WHERE applies inside every branch before grouping.
TEST_F(SqlGroupingSetsTest, WhereFiltersEveryBranch) {
  const auto rows =
      Run("SELECT a, SUM(x) AS s FROM g WHERE x >= 20 "
          "GROUP BY ROLLUP(a) ORDER BY a NULLS LAST");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{20}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(int64_t{30}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_EQ(rows[2][1], Value(int64_t{40}));
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(int64_t{90}));
}

// HAVING sees the branch's GROUPING() constants and aggregates.
TEST_F(SqlGroupingSetsTest, HavingUsesGroupingConstants) {
  const auto rows =
      Run("SELECT a, SUM(x) AS s FROM g GROUP BY ROLLUP(a) "
          "HAVING GROUPING(a) = 0 OR SUM(x) >= 70 "
          "ORDER BY a NULLS LAST");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(int64_t{100}));
}

// GROUP BY may reference select aliases inside the constructors; the
// aliased entry NULL-pads in branches where the alias key is inactive.
TEST_F(SqlGroupingSetsTest, AliasKeyRollupNullPadsAliasedEntry) {
  const auto rows =
      Run("SELECT a AS k, COUNT(*) AS c FROM g GROUP BY "
          "ROLLUP(k) ORDER BY k NULLS LAST");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_EQ(rows[2][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(int64_t{4}));
}

// Expression keys participate like column keys.
TEST_F(SqlGroupingSetsTest, ExpressionKeyRollup) {
  const auto rows =
      Run("SELECT a + 1 AS k, COUNT(*) AS c FROM g GROUP BY "
          "ROLLUP(a + 1) ORDER BY k NULLS LAST");
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{2}));
  EXPECT_EQ(rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(rows[1][0], Value(int64_t{3}));
  EXPECT_EQ(rows[1][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_EQ(rows[2][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(int64_t{4}));
}

// The wrapper keeps ORDER BY / LIMIT semantics over the concatenated rows.
TEST_F(SqlGroupingSetsTest, WrapperLimitAppliesAfterUnion) {
  const auto rows =
      Run("SELECT a, b, COUNT(*) AS c FROM g GROUP BY ROLLUP(a, "
          "b) ORDER BY a NULLS LAST, b NULLS LAST LIMIT 3");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(std::string("p1")));
}

// JOIN sources and DISTINCT aggregates run per branch with full grouping
// semantics.
TEST_F(SqlGroupingSetsTest, JoinAndDistinctAggregateWithRollup) {
  const auto rows =
      Run("SELECT g1.a, COUNT(DISTINCT g1.b) AS c FROM g g1 "
          "JOIN g g2 ON g1.a = g2.a GROUP BY ROLLUP(g1.a) "
          "ORDER BY g1.a NULLS LAST");
  ASSERT_EQ(rows.size(), 3U);
  // a=1 joins 2x2 with two distinct b; a=2 joins 1x1; NULL keys never match.
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_EQ(rows[2][1], Value(int64_t{3}));
}

// Ordinal GROUP BY items resolve inside the constructors too.
TEST_F(SqlGroupingSetsTest, OrdinalKeysInsideRollup) {
  const auto rows =
      Run("SELECT a, COUNT(*) AS c FROM g GROUP BY ROLLUP(1) "
          "ORDER BY a NULLS LAST");
  // Ordinal 1 resolves to `a`: the (a) groups plus the () grand total.
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  EXPECT_EQ(rows[0][1], Value(int64_t{2}));
  EXPECT_EQ(rows[1][0], Value(int64_t{2}));
  EXPECT_EQ(rows[1][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[2][0].IsNull());
  EXPECT_EQ(rows[2][1], Value(int64_t{1}));
  EXPECT_TRUE(rows[3][0].IsNull());
  EXPECT_EQ(rows[3][1], Value(int64_t{4}));
}

// Shapes the expansion refuses, each with a dedicated diagnostic.
TEST_F(SqlGroupingSetsTest, UnsupportedShapesRejectWithDiagnostics) {
  EXPECT_THROW(Run("SELECT DISTINCT a, COUNT(*) AS c FROM g GROUP BY "
                   "ROLLUP(a)"),
               std::runtime_error);
  EXPECT_THROW(Run("SELECT a, RANK() OVER (ORDER BY a) FROM g GROUP BY "
                   "ROLLUP(a)"),
               std::runtime_error);
  // GROUPING without a set constructor never folds; the evaluator rejects
  // the unknown function instead of guessing a semantics.
  EXPECT_THROW(Run("SELECT GROUPING(a) FROM g GROUP BY a"), std::runtime_error);
  // Wrapper ORDER BY may only use GROUPING through a select-list entry.
  EXPECT_THROW(Run("SELECT a, COUNT(*) AS c FROM g GROUP BY ROLLUP(a) "
                   "ORDER BY GROUPING(a)"),
               std::runtime_error);
  // GROUPING arguments must be grouping keys.
  EXPECT_THROW(Run("SELECT a, GROUPING(b) AS gb FROM g GROUP BY ROLLUP(a)"),
               std::runtime_error);
}

}  // namespace tinylamb
