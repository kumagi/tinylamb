/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// End-to-end regressions for optimizer / executor bugs found by the
// tinylamb-vs-DuckDB differential fuzzing campaign (2026-10).  Every test
// pins the result multiset of one minimized query; the expected values were
// cross-checked against DuckDB.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class SqlBugfixRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "GoogleSQL frontend unavailable";
    }
    database_ =
        Database::Create("sql_bugfix_regression-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  void Exec(const std::string& sql) {
    StatusOr<QueryResult> result = engine_->Execute(*context_, sql);
    ASSERT_TRUE(result.HasValue()) << sql << ": " << engine_->LastError();
    result.Value().Drain();
    ASSERT_EQ(result.Value().GetStatus(), Status::kSuccess) << sql;
  }

  // Runs a query and returns its rows as sorted strings (multiset compare).
  std::vector<std::string> Query(const std::string& sql) {
    StatusOr<QueryResult> result = engine_->Execute(*context_, sql);
    EXPECT_TRUE(result.HasValue()) << sql << ": " << engine_->LastError();
    if (!result.HasValue()) {
      return {"<error>"};
    }
    std::vector<std::string> rows;
    result.Value().ForEach([&](const Row& row) {
      std::string text;
      for (size_t i = 0; i < row.Count(); ++i) {
        if (i > 0) {
          text += ",";
        }
        std::ostringstream cell;
        cell << row[i];
        text += row[i].IsNull() ? "NULL" : cell.str();
      }
      rows.push_back(std::move(text));
    });
    EXPECT_EQ(result.Value().GetStatus(), Status::kSuccess) << sql;
    std::ranges::sort(rows);
    return rows;
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

// Single-table aggregation compiled its scan filter against the unqualified
// scan schema, so a residual conjunct (function call) naming the alias-
// qualified column failed with "column x.b not found".
TEST_F(SqlBugfixRegressionTest, AliasedAggregateEvaluatesFunctionFilter) {
  Exec("CREATE TABLE t (k INT64 PRIMARY KEY, a INT64, b INT64)");
  Exec("INSERT INTO t VALUES (1, 1, 1), (2, 2, NULL), (3, NULL, 3)");
  EXPECT_EQ(Query("SELECT COUNT(*) FROM t x WHERE COALESCE(x.b, 1) = 1"),
            (std::vector<std::string>{"2"}));
  EXPECT_EQ(Query("SELECT x.k, COUNT(*) FROM t x "
                  "WHERE x.b >= COALESCE(x.b, 1) GROUP BY x.k"),
            (std::vector<std::string>{"1,1", "3,1"}));
}

// or_to_union branches over a join pushed their partition predicates into
// the SHARED base scan group, ANDing every branch's predicate together.
TEST_F(SqlBugfixRegressionTest, OrSplitOverCrossJoinKeepsEveryBranch) {
  Exec("CREATE TABLE t0 (k INT64 PRIMARY KEY, a INT64)");
  Exec("CREATE TABLE t1 (k INT64 PRIMARY KEY, a INT64)");
  Exec("INSERT INTO t0 VALUES (1, 1), (2, 2), (3, NULL), (4, 7)");
  Exec("INSERT INTO t1 VALUES (1, 1), (2, 5)");
  const std::vector<std::string> expected{"1,1", "1,2", "4,1", "4,2"};
  EXPECT_EQ(Query("SELECT x0.k, x1.k FROM t0 x0 CROSS JOIN t1 x1 "
                  "WHERE x0.a < 2 OR x0.a > 6"),
            expected);
  EXPECT_EQ(Query("SELECT x0.k, x1.k FROM t0 x0 CROSS JOIN t1 x1 "
                  "WHERE x0.a NOT BETWEEN 2 AND 6"),
            expected);
}

// A runtime key filter derived for one alias of a self-joined table was
// stashed under the base table name and re-applied to every other alias.
TEST_F(SqlBugfixRegressionTest, SelfJoinKeyFilterStaysWithItsAlias) {
  Exec("CREATE TABLE t1 (k INT64, a INT64, b INT64)");
  Exec(
      "INSERT INTO t1 VALUES (0, 6, NULL), (1, -1, 0), (2, 2, 1), "
      "(3, NULL, 2), (4, 3, 1), (5, 7, 0)");
  EXPECT_EQ(Query("SELECT COUNT(*) FROM t1 x0 JOIN t1 x1 ON x0.k = x1.b "
                  "CROSS JOIN t1 x2"),
            (std::vector<std::string>{"30"}));
  EXPECT_EQ(
      Query("SELECT x2.k, COUNT(*) FROM t1 x0 JOIN t1 x1 "
            "ON x0.k = x1.b CROSS JOIN t1 x2 GROUP BY x2.k"),
      (std::vector<std::string>{"0,5", "1,5", "2,5", "3,5", "4,5", "5,5"}));
}

// Two correlation equalities binding the same outer column produced a left
// child projection with that column twice; resolving the join keys emitted
// every matching offset, so the key vectors had different lengths.
TEST_F(SqlBugfixRegressionTest, SemiJoinWithRepeatedOuterKeyColumn) {
  Exec("CREATE TABLE t1 (k INT64, a INT64, b INT64)");
  Exec("CREATE TABLE t2 (k INT64, a INT64, b INT64)");
  Exec("INSERT INTO t1 VALUES (0, 5, 5), (1, 3, 3), (2, 1, 2)");
  Exec("INSERT INTO t2 VALUES (0, 0, 5), (1, 0, 3), (2, 0, 1), (3, 0, 2)");
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE x0.b IN "
                  "(SELECT sq.b FROM t1 sq WHERE sq.a = x0.b)"),
            (std::vector<std::string>{"0", "1"}));
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE EXISTS (SELECT 1 FROM t1 sq "
                  "WHERE sq.a = x0.b AND sq.b = x0.b)"),
            (std::vector<std::string>{"0", "1"}));
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE NOT EXISTS (SELECT 1 FROM t1 "
                  "sq WHERE sq.a = x0.b AND sq.b = x0.b)"),
            (std::vector<std::string>{"2", "3"}));
}

// The SQL facade routed shapes to the decorrelating optimizer that its
// TryDecorrelate then rejected, stranding the subquery on a context-free
// filter ("query expression requires relational evaluation").
TEST_F(SqlBugfixRegressionTest, NonDecorrelatableSubqueriesStayRelational) {
  Exec("CREATE TABLE t1 (k INT64, a INT64, b INT64)");
  Exec("CREATE TABLE t2 (k INT64, a INT64, b INT64)");
  Exec(
      "INSERT INTO t1 VALUES (0, 6, NULL), (1, -1, 0), (2, 2, 1), "
      "(3, NULL, 2), (4, 3, 1), (5, 7, 0)");
  Exec(
      "INSERT INTO t2 VALUES (0, 6, NULL), (1, -1, 0), (2, 2, 1), "
      "(3, NULL, 2), (4, 3, 1), (5, 7, 0)");
  // Correlated NOT IN.
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE x0.b NOT IN "
                  "(SELECT sq.a FROM t1 sq WHERE sq.k = x0.k)"),
            (std::vector<std::string>{"1", "2", "4", "5"}));
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE x0.a NOT IN "
                  "(SELECT sq.a FROM t1 sq WHERE sq.k = 4 AND sq.k = x0.k)"),
            (std::vector<std::string>{"0", "1", "2", "3", "5"}));
  // Outer-only equality inside the subquery.
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE EXISTS (SELECT 1 FROM t1 sq "
                  "WHERE sq.k = x0.k AND x0.k = x0.k)"),
            (std::vector<std::string>{"0", "1", "2", "3", "4", "5"}));
  EXPECT_EQ(Query("SELECT x0.k FROM t2 x0 WHERE x0.a IN "
                  "(SELECT sq.a FROM t1 sq WHERE x0.k = x0.b)"),
            (std::vector<std::string>{}));
}

// distinct_and_group_by_interchange keyed its derived projection group by the
// target COUNT only, so the per-branch aggregates of an or_to_union split
// shared one projection group and both branches read the same partition.
TEST_F(SqlBugfixRegressionTest, DistinctOverOrSplitKeepsBothPartitions) {
  Exec("CREATE TABLE t0 (k INT64 PRIMARY KEY, a INT64)");
  Exec("CREATE TABLE t1 (k INT64, a INT64)");
  Exec("INSERT INTO t0 VALUES (0, 1), (1, 2), (7, 3)");
  Exec("INSERT INTO t1 VALUES (0, 3), (1, 6), (2, -2)");
  Exec("ANALYZE");
  const std::vector<std::string> expected{"1", "2", "3"};
  EXPECT_EQ(Query("SELECT DISTINCT a FROM t0 WHERE k < 6 OR k > 3"), expected);
  EXPECT_EQ(Query("SELECT DISTINCT x0.a FROM t0 x0 CROSS JOIN t1 x1 "
                  "WHERE x0.k NOT BETWEEN 6 AND 3"),
            expected);
}

// push_filter_through_left_join_left_side added its filtered outer join to
// the SHARED join group, leaking one or_to_union branch's predicate into the
// other branch.
TEST_F(SqlBugfixRegressionTest, LeftJoinFilterPushDoesNotLeakAcrossBranches) {
  Exec("CREATE TABLE t0 (k INT64 PRIMARY KEY, b INT64, s STRING, f BOOL)");
  Exec(
      "INSERT INTO t0 VALUES (0, 4, 'zz', FALSE), (1, 3, '', FALSE), "
      "(2, 5, 'b', TRUE), (3, -2, 'ab', TRUE), (6, NULL, '', NULL)");
  Exec("ANALYZE");
  EXPECT_EQ(Query("SELECT DISTINCT x0.f FROM t0 x0 LEFT JOIN t0 x1 "
                  "ON x0.k = x1.b WHERE (x0.f OR x0.s <> '')"),
            (std::vector<std::string>{"0", "1"}));
}

// PropagateEqualityConstants only recognised `column = constant`; the
// mirrored `5 = b` was dropped as "implied by the class constant" without
// ever being compared with it, so contradictory filters matched rows and a
// DELETE removed them.
TEST_F(SqlBugfixRegressionTest, MirroredConstantEqualityIsNotDropped) {
  Exec("CREATE TABLE t1 (k INT64, b INT64)");
  Exec("INSERT INTO t1 VALUES (0, 0), (1, 5), (2, 6), (3, NULL)");
  EXPECT_EQ(Query("SELECT k FROM t1 WHERE 5 = b AND b = 6"),
            (std::vector<std::string>{}));
  EXPECT_EQ(Query("SELECT k FROM t1 WHERE k = b AND b = 0 AND 1 = k"),
            (std::vector<std::string>{}));
  EXPECT_EQ(Query("SELECT k FROM t1 WHERE 6 = b AND b = 6"),
            (std::vector<std::string>{"2"}));
  Exec("DELETE FROM t1 WHERE 5 = b AND b = 6");
  EXPECT_EQ(Query("SELECT COUNT(*) FROM t1"), (std::vector<std::string>{"4"}));
}

// The memo derived NOT NULL for every PRIMARY KEY / NOT NULL column of an
// expression's output schema, including the NULL-supplying side of an outer
// join, so `WHERE r.k IS NOT NULL` above a LEFT/FULL join was eliminated.
TEST_F(SqlBugfixRegressionTest, OuterJoinPaddedKeyIsNotProvenNotNull) {
  Exec("CREATE TABLE t0 (k INT64 PRIMARY KEY, a INT64, b INT64, s STRING)");
  Exec("CREATE TABLE t1 (k INT64 PRIMARY KEY, a INT64, b INT64, s STRING)");
  Exec(
      "INSERT INTO t0 VALUES (0, 1, 2, 'x'), (1, 2, NULL, NULL), "
      "(2, NULL, 7, 'y')");
  Exec(
      "INSERT INTO t1 VALUES (0, 2, 2, 'p'), (1, 5, NULL, 'q'), "
      "(2, 7, 7, NULL)");
  Exec("ANALYZE");
  EXPECT_EQ(Query("SELECT x0.k, x1.k FROM t0 x0 FULL JOIN t1 x1 "
                  "ON x0.b = x1.a WHERE x1.k IS NOT NULL"),
            (std::vector<std::string>{"0,0", "2,2", "NULL,1"}));
  EXPECT_EQ(Query("SELECT x0.s FROM t0 x0 FULL JOIN t1 x1 ON x0.b = x1.a "
                  "WHERE x0.k IS NOT NULL"),
            (std::vector<std::string>{"\"x\"", "\"y\"", "NULL"}));
  EXPECT_EQ(Query("SELECT x0.k, x1.k FROM t0 x0 LEFT JOIN t1 x1 "
                  "ON x0.b = x1.a WHERE x1.k IS NOT NULL"),
            (std::vector<std::string>{"0,0", "2,2"}));
}

// A predicate derived from one or_to_union branch was pushed from the
// left-join-filter group (built over a CROSS JOIN) into the shared base scan.
TEST_F(SqlBugfixRegressionTest, BranchPredicateNeverReachesSharedScan) {
  Exec("CREATE TABLE t1 (k INT64, a INT64, b INT64)");
  Exec("CREATE TABLE t2 (k INT64, a INT64, b INT64)");
  Exec("INSERT INTO t1 VALUES (0, 1, 2), (1, 2, NULL), (7, NULL, 7)");
  Exec("INSERT INTO t2 VALUES (0, 2, 2), (1, 5, NULL)");
  EXPECT_EQ(Query("SELECT COUNT(*) FROM t1 x0 CROSS JOIN t1 x1 LEFT JOIN t2 "
                  "x2 ON x1.b = x2.a WHERE x0.k < 2 OR x0.k > 6"),
            (std::vector<std::string>{"9"}));
}

// The relational executor folded a multi-source WHERE conjunct into an
// inner join that a later RIGHT JOIN NULL-supplies; padded rows then passed
// a null-accepting test (`f(a, b) IS NULL`).
TEST_F(SqlBugfixRegressionTest, ConjunctStaysAboveLaterRightJoin) {
  Exec("CREATE TABLE t0 (k INT64 PRIMARY KEY, a INT64)");
  Exec("CREATE TABLE t1 (k INT64, a INT64, b INT64)");
  Exec("CREATE TABLE t2 (k INT64, a INT64)");
  Exec("INSERT INTO t0 VALUES (0, 0), (1, -2), (2, 9)");
  Exec("INSERT INTO t1 VALUES (0, NULL, 0), (1, 2, -2)");
  Exec("INSERT INTO t2 VALUES (0, 1), (1, 1)");
  // x0 k=1 (a=-2) only matches x1 row 1 (a=2): every joined row fails the
  // IS NULL test, so it must NOT come back as a NULL-padded row.
  EXPECT_EQ(Query("SELECT x2.k, x0.k, x1.k FROM t2 x0 CROSS JOIN t1 x1 RIGHT "
                  "JOIN t0 x2 ON x1.b = x2.a WHERE (x0.k * x1.a) IS NULL"),
            (std::vector<std::string>{"0,0,0", "0,1,0", "2,NULL,NULL"}));
}

// LateMaterializationJoinPlan's statistics covered only its left input, so a
// Selection above it indexed past the column statistics (out_of_range).
TEST_F(SqlBugfixRegressionTest, LateMaterializationJoinStatisticsCoverPayload) {
  Exec("CREATE TABLE t0 (k INT64 PRIMARY KEY, a INT64, b INT64, f BOOL)");
  Exec("CREATE TABLE t2 (k INT64 PRIMARY KEY, a INT64, b INT64, f BOOL)");
  Exec(
      "INSERT INTO t0 VALUES (0, -3, 2, NULL), (1, NULL, 0, TRUE), "
      "(2, 1, 3, FALSE), (3, 2, 5, NULL), (4, NULL, 3, NULL), (5, 4, -1, "
      "TRUE)");
  std::string insert = "INSERT INTO t2 VALUES ";
  for (int i = 0; i < 25; ++i) {
    insert += (i > 0 ? ", (" : "(") + std::to_string(i) + ", " +
              std::to_string(i % 6) + ", " + std::to_string(i % 4) + ", TRUE)";
  }
  Exec(insert);
  Exec("ANALYZE");
  const std::vector<std::string> rows =
      Query("SELECT x0.f FROM t0 x0 JOIN t2 x1 ON x0.k = x1.a WHERE 3 <> -2");
  EXPECT_EQ(rows.size(), 25U);
}

// A stale index (own DELETE + INSERT in the transaction) made the index scan
// fall back to an unordered heap scan below an elided ORDER BY / band join.
TEST_F(SqlBugfixRegressionTest, StaleIndexFallbackKeepsKeyOrder) {
  Exec("CREATE TABLE t2 (k INT64 PRIMARY KEY, f BOOL)");
  Exec(
      "INSERT INTO t2 VALUES (0, TRUE), (1, FALSE), (2, TRUE), (3, FALSE), "
      "(4, NULL)");
  Exec("DELETE FROM t2 WHERE NOT (t2.f)");
  Exec("INSERT INTO t2 VALUES (21, FALSE)");
  StatusOr<QueryResult> ordered =
      engine_->Execute(*context_, "SELECT k FROM t2 ORDER BY k");
  ASSERT_TRUE(ordered.HasValue()) << engine_->LastError();
  std::vector<int64_t> keys;
  ordered.Value().ForEach(
      [&](const Row& row) { keys.push_back(row[0].value.int_value); });
  EXPECT_EQ(keys, (std::vector<int64_t>{0, 2, 4, 21}));
  EXPECT_EQ(
      Query("SELECT x1.k, x2.k FROM t2 x1 JOIN t2 x2 ON x2.k > x1.k"),
      (std::vector<std::string>{"0,2", "0,21", "0,4", "2,21", "2,4", "4,21"}));
}

}  // namespace tinylamb
