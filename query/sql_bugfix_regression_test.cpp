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

// A non-correlated `col IN (SELECT ...)` pushdown stashes the membership set
// as raw int64 bits. Those bits are a superset of SQL equality, which is
// sound only while the equality-IN re-check survives: with dense bits and
// >= 4096 distinct keys the predicate was dropped, publishing the superset's
// false matches. UINT64 membership rows store 2^64-1 as the int64 bits -1, so
// a signed probe column value of -1 was "in" the set although SQL never calls
// them equal (and the same in reverse for an unsigned probe column).
TEST_F(SqlBugfixRegressionTest, UnsignedInSubqueryNeverMatchesSignedProbe) {
  Exec("CREATE TABLE sgn17 (c BIGINT)");
  Exec("INSERT INTO sgn17 VALUES (-2000), (-1), (0), (5)");
  Exec("CREATE TABLE m17 (u UINT64)");
  // 4100 distinct UINT64 keys whose int64 bits form a dense run (-1..-4100):
  // enough for the dense-bits + >= 4096 gate that drops the predicate.
  for (int base = 0; base < 4100; base += 820) {
    std::string insert = "INSERT INTO m17 VALUES ";
    for (int k = 0; k < 820; ++k) {
      const uint64_t value =
          18446744073709551615ULL - static_cast<uint64_t>(base + k);
      insert += (k > 0 ? ", (" : "(") + std::to_string(value) + ")";
    }
    Exec(insert);
  }
  EXPECT_TRUE(
      Query("SELECT c FROM sgn17 WHERE c IN (SELECT u FROM m17)").empty());
  // Control: shrink the membership below the gate and the predicate survives,
  // so the same query must stay empty (it always did).
  Exec("CREATE TABLE m1000 (u UINT64)");
  {
    std::string insert = "INSERT INTO m1000 VALUES ";
    for (int k = 0; k < 1000; ++k) {
      const uint64_t value = 18446744073709551615ULL - static_cast<uint64_t>(k);
      insert += (k > 0 ? ", (" : "(") + std::to_string(value) + ")";
    }
    Exec(insert);
  }
  EXPECT_TRUE(
      Query("SELECT c FROM sgn17 WHERE c IN (SELECT u FROM m1000)").empty());
  // Reverse direction: a signed membership with dense negative bits must not
  // prune an unsigned probe column carrying the very same bits.
  Exec("CREATE TABLE usgn (c UINT64)");
  Exec(
      "INSERT INTO usgn VALUES (18446744073709551615), "
      "(18446744073709551614), (5)");
  Exec("CREATE TABLE msgn (v BIGINT)");
  {
    std::string insert = "INSERT INTO msgn VALUES ";
    for (int k = 0; k < 4100; ++k) {
      insert += (k > 0 ? ", (" : "(") + std::to_string(-1 - k) + ")";
    }
    Exec(insert);
  }
  EXPECT_TRUE(
      Query("SELECT c FROM usgn WHERE c IN (SELECT v FROM msgn)").empty());
  // A genuine match still comes back, signed probe over signed membership.
  Exec("CREATE TABLE s2 (c BIGINT)");
  Exec("INSERT INTO s2 VALUES (0), (5), (7)");
  EXPECT_EQ(Query("SELECT c FROM s2 WHERE c IN (SELECT c FROM s2 WHERE c < 6)"),
            (std::vector<std::string>{"0", "5"}));
}

// The CREATE TABLE visitor dropped the parser's is_if_not_exists /
// is_or_replace detail flags, so CREATE TABLE IF NOT EXISTS on an existing
// name failed with a bare Conflicts and CREATE OR REPLACE TABLE could never
// replace. The flags now ride the statement and ExecuteCreateTable honors
// them: no-op success for IF NOT EXISTS, drop-then-create for OR REPLACE.
TEST_F(SqlBugfixRegressionTest, CreateTableIfNotExistsAndOrReplaceFlags) {
  Exec("CREATE TABLE t (k INT64 PRIMARY KEY, v INT64)");
  Exec("INSERT INTO t VALUES (1, 10)");
  // IF NOT EXISTS on an existing table is a no-op that keeps the old shape
  // and data (the offered narrower schema must not replace anything).
  Exec("CREATE TABLE IF NOT EXISTS t (k INT64)");
  EXPECT_EQ(Query("SELECT k, v FROM t"), (std::vector<std::string>{"1,10"}));
  // Without the flag the duplicate still fails.
  EXPECT_FALSE(
      engine_->Execute(*context_, "CREATE TABLE t (k INT64)").HasValue());
  // OR REPLACE drops and recreates with the new shape.
  Exec("CREATE OR REPLACE TABLE t (k INT64, w STRING(MAX))");
  EXPECT_TRUE(Query("SELECT k, w FROM t").empty());
  Exec("INSERT INTO t VALUES (2, 'two')");
  EXPECT_EQ(Query("SELECT k, w FROM t"), (std::vector<std::string>{"2,two"}));
  // The parser accepts both flags together; the visitor rejects the
  // ambiguous combination explicitly instead of picking one silently.
  EXPECT_FALSE(
      engine_
          ->Execute(*context_,
                    "CREATE OR REPLACE TABLE IF NOT EXISTS t (x INT64)")
          .HasValue());

  // The flags also apply to the CREATE TABLE AS SELECT form.
  Exec("CREATE TABLE IF NOT EXISTS t2 AS SELECT 1 AS one");
  Exec("CREATE TABLE IF NOT EXISTS t2 AS SELECT 2 AS two");
  EXPECT_EQ(Query("SELECT one FROM t2"), (std::vector<std::string>{"1"}));
  Exec("CREATE OR REPLACE TABLE t2 AS SELECT 9 AS nine");
  EXPECT_EQ(Query("SELECT nine FROM t2"), (std::vector<std::string>{"9"}));

  // A name held by a view counts as existing: IF NOT EXISTS is a no-op and
  // the view survives; OR REPLACE without a physical table keeps the
  // ordinary already-exists failure instead of deleting the view.
  Exec("CREATE TABLE holder (k INT64)");
  Exec("INSERT INTO holder VALUES (7)");
  Exec("CREATE VIEW vv AS SELECT k FROM holder");
  Exec("CREATE TABLE IF NOT EXISTS vv (x INT64)");
  EXPECT_EQ(Query("SELECT k FROM vv"), (std::vector<std::string>{"7"}));
  EXPECT_FALSE(
      engine_->Execute(*context_, "CREATE OR REPLACE TABLE vv (x INT64)")
          .HasValue());
  EXPECT_EQ(Query("SELECT k FROM vv"), (std::vector<std::string>{"7"}));
}

// The varchar prefilter swallowed the INTERVAL/STRING type mismatch that the
// checked evaluator raises, so `WHERE s = INTERVAL 1 DAY` returned a silently
// empty result and the OR variant let the surviving disjunct answer rows.
// The compile gate now leaves interval-tagged constants to the residual,
// which raises exactly like the projection arm always did.
TEST_F(SqlBugfixRegressionTest, IntervalStringComparisonRaisesInFilter) {
  Exec("CREATE TABLE it (k INT64, s STRING(MAX))");
  Exec("INSERT INTO it VALUES (1, 'a'), (2, '1 day 0:0:0')");
  for (const char* sql :
       {"SELECT k FROM it WHERE s = INTERVAL 1 DAY",
        "SELECT k FROM it WHERE s = INTERVAL 1 DAY OR k > 0"}) {
    SCOPED_TRACE(sql);
    StatusOr<QueryResult> result = engine_->Execute(*context_, sql);
    ASSERT_FALSE(result.HasValue()) << sql;
    const std::string& error = engine_->LastError();
    EXPECT_TRUE(error.find("INTERVAL") != std::string::npos ||
                error.find("mismatch") != std::string::npos)
        << error;
  }
  // The projection arm already raised before the fix; it must keep raising.
  EXPECT_FALSE(engine_->Execute(*context_, "SELECT s = INTERVAL 1 DAY FROM it")
                   .HasValue());
}

// INSERT IGNORE / OR REPLACE keyed their conflict gate on the compliance-mock
// PK flag alone, so a table with a declared PRIMARY KEY rejected every
// conflict clause with "table does not have a primary key". The gate now
// accepts a declared PK and the duplicate detection uses the PK's own
// columns instead of always column 0.
TEST_F(SqlBugfixRegressionTest, InsertConflictModesUseDeclaredPrimaryKey) {
  Exec("CREATE TABLE pk (k INT64 PRIMARY KEY, v INT64)");
  Exec("INSERT INTO pk VALUES (1, 10)");
  // A plain duplicate still fails (the storage-level unique index).
  EXPECT_FALSE(
      engine_->Execute(*context_, "INSERT INTO pk VALUES (1, 99)").HasValue());
  // IGNORE skips the duplicate instead of erroring about the key.
  Exec("INSERT IGNORE INTO pk VALUES (1, 99)");
  EXPECT_EQ(Query("SELECT k, v FROM pk"), (std::vector<std::string>{"1,10"}));
  // OR REPLACE overwrites through the declared PK.
  Exec("INSERT OR REPLACE INTO pk VALUES (1, 20)");
  EXPECT_EQ(Query("SELECT k, v FROM pk"), (std::vector<std::string>{"1,20"}));
  // A table without any key keeps rejecting conflict clauses.
  Exec("CREATE TABLE nopk (k INT64, v INT64)");
  EXPECT_FALSE(
      engine_->Execute(*context_, "INSERT IGNORE INTO nopk VALUES (1, 2)")
          .HasValue());
  // A non-first-column PK drives the detection (not column 0).
  Exec("CREATE TABLE pk2 (v INT64, k INT64 PRIMARY KEY)");
  Exec("INSERT INTO pk2 VALUES (100, 7)");
  Exec("INSERT IGNORE INTO pk2 VALUES (999, 7)");
  EXPECT_EQ(Query("SELECT v, k FROM pk2"), (std::vector<std::string>{"100,7"}));
  Exec("INSERT OR REPLACE INTO pk2 VALUES (555, 7)");
  EXPECT_EQ(Query("SELECT v, k FROM pk2"), (std::vector<std::string>{"555,7"}));
}

}  // namespace tinylamb
