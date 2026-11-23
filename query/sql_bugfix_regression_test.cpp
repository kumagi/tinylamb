/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// End-to-end regressions for optimizer / executor bugs found by the
// tinylamb-vs-DuckDB differential fuzzing campaign (2026-10).  Every test
// pins the result multiset of one minimized query; the expected values were
// cross-checked against DuckDB.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/apply.hpp"
#include "executor/constant_executor.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
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
    if (const auto error = ErrorOf(sql)) {
      throw std::runtime_error(sql + ": " + *error);
    }
  }

  // Execution is lazy: a statement may prepare fine and fail while its
  // cursor is drained (DML conflicts, runtime type errors). Returns the
  // first error text, or nullopt when the statement fully succeeded.
  std::optional<std::string> ErrorOf(const std::string& sql) {
    StatusOr<QueryResult> result = engine_->Execute(*context_, sql);
    if (!result.HasValue()) {
      return engine_->LastError() + " " + ToString(result.GetStatus());
    }
    result.Value().Drain();
    if (result.Value().GetStatus() != Status::kSuccess) {
      return ToString(result.Value().GetStatus());
    }
    return std::nullopt;
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
      text.reserve(row.Count());
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

TEST_F(SqlBugfixRegressionTest, CoalesceRejectsIncompatibleTypesInEitherOrder) {
  EXPECT_TRUE(ErrorOf("SELECT COALESCE(NULL, 1, 'a')").has_value());
  EXPECT_TRUE(ErrorOf("SELECT COALESCE(NULL, 'b', 1)").has_value());
}

TEST_F(SqlBugfixRegressionTest, ProjectionDoesNotHoistGuardedDivision) {
  Exec("CREATE TABLE cse_rows (x INT64)");
  Exec("INSERT INTO cse_rows VALUES (0), (2)");
  EXPECT_EQ(Query("SELECT IF(x = 0, 0, 6 / x), IF(x = 0, 1, 6 / x) FROM cse_rows ORDER BY x"),
            (std::vector<std::string>{"0,1", "3,3"}));
}

TEST_F(SqlBugfixRegressionTest, JoinDiscardsOverflowingSumGroup) {
  Exec("CREATE TABLE fact (k INT64, v INT64)");
  Exec("CREATE TABLE dim (k INT64 PRIMARY KEY)");
  Exec("INSERT INTO fact VALUES (1, 9223372036854775807), (1, 1), (2, 7)");
  Exec("INSERT INTO dim VALUES (2)");
  const std::string sql =
      "SELECT d.k, SUM(f.v) FROM fact f JOIN dim d ON f.k = d.k GROUP BY d.k";
  EXPECT_EQ(Query(sql), (std::vector<std::string>{"2,7"}));
  Exec("INSERT INTO dim VALUES (1)");
  const auto error = ErrorOf(sql);
  ASSERT_TRUE(error.has_value());
  EXPECT_NE(error.value().find("overflow"), std::string::npos) << error.value();
}

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

// P178: the outer_to_anti_join rule published the query's target list onto the
// AntiJoin alternative verbatim; when it named right-side columns
// (`SELECT b.v ... WHERE b.k IS NULL`) the emitted AntiJoin's readers were
// handed a schema its preserved-side emission does not satisfy — results
// only matched because every surviving row is NULL-extended.  Conversion
// must refuse when the published targets touch right-side columns; this pin
// locks the answers and the shape stability of all three target flavours.
TEST_F(SqlBugfixRegressionTest, AntiJoinConversionRejectsRightSideTargets) {
  Exec("CREATE TABLE a (k INT64, v INT64)");
  Exec("CREATE TABLE b (k INT64, v INT64)");
  Exec("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
  Exec("INSERT INTO b VALUES (1, 11), (3, 33)");
  EXPECT_EQ(Query("SELECT b.v FROM a LEFT JOIN b ON a.k = b.k "
                  "WHERE b.k IS NULL"),
            (std::vector<std::string>{"NULL"}));
  EXPECT_EQ(Query("SELECT b.v + 1 FROM a LEFT JOIN b ON a.k = b.k "
                  "WHERE b.k IS NULL"),
            (std::vector<std::string>{"NULL"}));
  EXPECT_EQ(Query("SELECT a.k FROM a LEFT JOIN b ON a.k = b.k "
                  "WHERE b.k IS NULL"),
            (std::vector<std::string>{"2"}));
  EXPECT_EQ(Query("SELECT COUNT(*) FROM a LEFT JOIN b ON a.k = b.k "
                  "WHERE b.k IS NULL"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(Query("SELECT a.k, b.k FROM a LEFT JOIN b ON a.k = b.k "
                  "WHERE b.k IS NULL"),
            (std::vector<std::string>{"2,NULL"}));
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
  insert.reserve(25);
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
    // Each tuple contains a UINT64 literal plus punctuation and spacing.
    insert.reserve(insert.size() + 24 * 820);
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
    insert.reserve(4100);
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
  // Query() renders cells through operator<<, which quotes strings.
  EXPECT_EQ(Query("SELECT k, w FROM t"),
            (std::vector<std::string>{"2,\"two\""}));
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

// T485: the ViewRegistry "registered at (database, epoch)" stamp is
// per-thread, not per engine instance. Both sides must hold: a freshly
// constructed engine on the same thread/db resolves views through the
// shared registry (a fast-path hit must not turn into an unknown-table
// failure), and a DROP VIEW bumps the epoch so even the shared stamp is
// invalidated for every engine on that thread.
TEST_F(SqlBugfixRegressionTest, ViewRegistryFastPathIsPerThreadNotPerEngine) {
  Exec("CREATE TABLE vp (k INT64)");
  Exec("INSERT INTO vp VALUES (3)");
  Exec("CREATE VIEW vv AS SELECT k FROM vp");
  {
    SqlEngine fresh(*database_);
    StatusOr<QueryResult> result = fresh.Execute(*context_, "SELECT k FROM vv");
    ASSERT_TRUE(result.HasValue()) << fresh.LastError();
    std::vector<std::string> rows;
    result.Value().ForEach([&](const Row& row) {
      std::ostringstream cell;
      cell << row[0];
      rows.push_back(cell.str());
    });
    EXPECT_EQ(rows, (std::vector<std::string>{"3"}));
    EXPECT_EQ(result.Value().GetStatus(), Status::kSuccess);
  }
  Exec("DROP VIEW vv");
  {
    SqlEngine fresh2(*database_);
    StatusOr<QueryResult> gone = fresh2.Execute(*context_, "SELECT k FROM vv");
    EXPECT_FALSE(gone.HasValue());
  }
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
    const std::optional<std::string> failure = ErrorOf(sql);
    if (!failure.has_value()) {
      ADD_FAILURE() << sql;
      return;
    }
    const std::string& error = *failure;
    EXPECT_TRUE(error.find("INTERVAL") != std::string::npos ||
                error.find("mismatch") != std::string::npos)
        << error;
  }
  // The projection arm already raised before the fix; it must keep raising.
  EXPECT_TRUE(ErrorOf("SELECT s = INTERVAL 1 DAY FROM it").has_value());
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
  EXPECT_TRUE(ErrorOf("INSERT INTO pk VALUES (1, 99)").has_value());
  // IGNORE skips the duplicate instead of erroring about the key.
  Exec("INSERT IGNORE INTO pk VALUES (1, 99)");
  EXPECT_EQ(Query("SELECT k, v FROM pk"), (std::vector<std::string>{"1,10"}));
  // OR REPLACE overwrites through the declared PK.
  Exec("INSERT OR REPLACE INTO pk VALUES (1, 20)");
  EXPECT_EQ(Query("SELECT k, v FROM pk"), (std::vector<std::string>{"1,20"}));
  // A table without any key keeps rejecting conflict clauses.
  Exec("CREATE TABLE nopk (k INT64, v INT64)");
  EXPECT_TRUE(ErrorOf("INSERT IGNORE INTO nopk VALUES (1, 2)").has_value());
  // A non-first-column PK drives the detection (not column 0).
  Exec("CREATE TABLE pk2 (v INT64, k INT64 PRIMARY KEY)");
  Exec("INSERT INTO pk2 VALUES (100, 7)");
  Exec("INSERT IGNORE INTO pk2 VALUES (999, 7)");
  EXPECT_EQ(Query("SELECT v, k FROM pk2"), (std::vector<std::string>{"100,7"}));
  Exec("INSERT OR REPLACE INTO pk2 VALUES (555, 7)");
  EXPECT_EQ(Query("SELECT v, k FROM pk2"), (std::vector<std::string>{"555,7"}));
}

// T3699: the INSERT INT64->DOUBLE coercion reinterpreted the two's-complement
// bits of an unsigned value as signed, so UINT64 values >= 2^63 landed as
// negative doubles (UINT64_MAX became -1.0).
TEST_F(SqlBugfixRegressionTest, InsertUnsignedIntoDoubleUsesUnsignedBits) {
  Exec("CREATE TABLE d (v FLOAT64)");
  Exec("INSERT INTO d VALUES (CAST(18446744073709551615 AS UINT64))");
  Exec("INSERT INTO d VALUES (CAST(3 AS UINT64))");
  EXPECT_EQ(Query("SELECT v < 0 FROM d"), (std::vector<std::string>{"0", "0"}));
  EXPECT_EQ(Query("SELECT v >= 18446744073709551615.0 FROM d"),
            (std::vector<std::string>{"0", "1"}));
}

// T4097: STRING(TIMESTAMP, zone) renders an instant, so the zone's offset
// must be resolved AT that instant (like FORMAT_TIMESTAMP's at_utc arm), not
// as though the UTC wall-clock fields were local time in that zone.
TEST_F(SqlBugfixRegressionTest, StringTimestampZoneResolvesOffsetAtInstant) {
  // 2024-03-10 06:30 UTC is 01:30 EST in New York (DST starts at 07:00
  // UTC); reading 06:30 as New York wall time answers 02:30-04 instead.
  EXPECT_EQ(Query("SELECT STRING(TIMESTAMP '2024-03-10 06:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"2024-03-10 01:30:00-05\""}));
  // Past the transition the two readings agree -- pin them too.
  EXPECT_EQ(Query("SELECT STRING(TIMESTAMP '2024-03-10 07:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"2024-03-10 03:30:00-04\""}));
  EXPECT_EQ(Query("SELECT STRING(TIMESTAMP '2024-06-15 18:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"2024-06-15 14:30:00-04\""}));
}

// T5030: same family as T4097 -- TIME()/DATETIME() over a TIMESTAMP render
// the instant in the zone, so the zone offset must be resolved AT the UTC
// instant, not by reading the UTC fields as New York wall time (which picks
// the EDT offset an hour before the transition and answers 02:30).
TEST_F(SqlBugfixRegressionTest, TimeAndDatetimeZoneResolveOffsetAtInstant) {
  EXPECT_EQ(Query("SELECT TIME(TIMESTAMP '2024-03-10 06:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"01:30:00\""}));
  EXPECT_EQ(Query("SELECT DATETIME(TIMESTAMP '2024-03-10 06:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"2024-03-10 01:30:00\""}));
  // Past the transition both readings agree; keep them pinned too.
  EXPECT_EQ(Query("SELECT TIME(TIMESTAMP '2024-03-10 07:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"03:30:00\""}));
  EXPECT_EQ(Query("SELECT DATETIME(TIMESTAMP '2024-03-10 07:30:00+00', "
                  "'America/New_York')"),
            (std::vector<std::string>{"\"2024-03-10 03:30:00\""}));
}

// T4157: EliminateUnusedOuterJoinsForIdempotentResult read a later ON's
// column set through TouchedColumns(), which for a kQueryExp only reports
// the Test() operand -- a correlated subquery's inner references stayed
// invisible and its source got deleted, dangling the correlation.
TEST_F(SqlBugfixRegressionTest, UnusedLeftJoinSurvivesLaterOnSubquery) {
  Exec("CREATE TABLE elj_a (x INT64, k INT64)");
  Exec("CREATE TABLE elj_b (k INT64, v INT64)");
  Exec("CREATE TABLE elj_c (k INT64)");
  Exec("INSERT INTO elj_a VALUES (7, 1), (8, 2)");
  Exec("INSERT INTO elj_b VALUES (1, 100), (2, 200)");
  Exec("INSERT INTO elj_c VALUES (100), (200)");
  // DISTINCT makes row multiplicity irrelevant, so elj_b is an elimination
  // candidate; the third source's ON correlates to elj_b inside a subquery.
  // Before T4157 Prepare died with "column elj_b.v not found".
  EXPECT_EQ(Query("SELECT DISTINCT elj_a.x FROM elj_a "
                  "LEFT JOIN elj_b ON elj_a.k = elj_b.k "
                  "LEFT JOIN elj_c ON elj_c.k = "
                  "(SELECT MAX(s.v) FROM elj_b s WHERE s.v = elj_b.v)"),
            (std::vector<std::string>{"7", "8"}));
}

// T4494: ExecutorBase::Next buffers a batch for row-at-a-time fetching;
// mixing Next with the batch consumers must neither lose nor repeat rows.
TEST_F(SqlBugfixRegressionTest, RowFetchMixesWithBatchConsumers) {
  Exec("CREATE TABLE mix (k INT64)");
  Exec("INSERT INTO mix VALUES (1), (2), (3), (4), (5)");
  StatusOr<QueryResult> result =
      engine_->Execute(*context_, "SELECT k FROM mix ORDER BY k");
  ASSERT_TRUE(result.HasValue()) << engine_->LastError();
  Row first;
  ASSERT_TRUE(result.Value().Next(&first));
  EXPECT_EQ(first[0].value.int_value, 1);
  std::vector<Row> rest = result.Value().Collect();
  ASSERT_EQ(rest.size(), 4);
  EXPECT_EQ(rest[0][0].value.int_value, 2);
  EXPECT_EQ(rest[3][0].value.int_value, 5);
  EXPECT_FALSE(result.Value().Next(&first));
}

// T3668: the ApplyExecutor route that drives a compiled inner plan
// (inner_query_ == nullptr) left inner_schema_/combined_schema_ empty, so
// the route predicate evaluated against a zero-width schema.
TEST_F(SqlBugfixRegressionTest,
       ApplyCompiledInnerRouteEvaluatesOnCombinedSchema) {
  Schema left_schema("l", {Column("k", ValueType::kInt64)});
  Schema right_schema("r", {Column("k", ValueType::kInt64)});
  auto outer = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(int64_t{1})}), Row({Value(int64_t{2})})});
  auto inner = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(int64_t{1})}), Row({Value(int64_t{3})})});
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l", "k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("r", "k")));
  ApplyExecutor apply(*context_, left_schema, std::move(outer), nullptr, "",
                      std::move(inner), std::move(predicate), JoinKind::kInner,
                      left_schema + right_schema, right_schema);
  std::vector<Row> out;
  Row row;
  while (apply.Next(&row)) {
    out.push_back(row);
    row = Row();
  }
  ASSERT_EQ(apply.GetStatus(), Status::kSuccess) << apply.GetStatus();
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out[0][0].value.int_value, 1);
}

// T3929: "%Ez" is only interpreted by glibc strptime; the shared offset-
// preference decision must fold the alias into "%z" so the input's own
// offset wins over the third-argument zone on every platform.
TEST_F(SqlBugfixRegressionTest, ParseTimestampEzAliasPrefersInputOffset) {
  EXPECT_EQ(Query("SELECT STRING(PARSE_TIMESTAMP("
                  "'%Y-%m-%d %H:%M:%S %Ez', '2024-03-10 06:30:00 +0000', "
                  "'America/New_York'))"),
            (std::vector<std::string>{"\"2024-03-10 06:30:00+00\""}));
  // An escaped %%Ez is literal text, not an offset spec: the zone argument
  // applies (2024-03-10 06:30 New York is already EDT, i.e. -04).
  EXPECT_EQ(Query("SELECT STRING(PARSE_TIMESTAMP("
                  "'%%Ez %Y-%m-%d %H:%M:%S', '%Ez 2024-03-10 06:30:00', "
                  "'America/New_York'))"),
            (std::vector<std::string>{"\"2024-03-10 10:30:00+00\""}));
}

// T3886: GREATEST/LEAST fall back to Value::Try{Less,Greater} for non-
// numeric operands; those must honour the collation tag, matching ORDER BY.
TEST_F(SqlBugfixRegressionTest, GreatestLeastRespectCollationTag) {
  // Under 'und:ci' the order is 'a' > 'B' (raw bytes would pick 'a' as the
  // smaller, i.e. invert both choices).
  EXPECT_EQ(Query("SELECT GREATEST(COLLATE('B', 'und:ci'), "
                  "COLLATE('a', 'und:ci'))"),
            (std::vector<std::string>{"\"B\""}));
  EXPECT_EQ(Query("SELECT LEAST(COLLATE('B', 'und:ci'), "
                  "COLLATE('a', 'und:ci'))"),
            (std::vector<std::string>{"\"a\""}));
}

// T2235: REGEXP_SUBSTR is the GoogleSQL synonym of REGEXP_EXTRACT.
TEST_F(SqlBugfixRegressionTest, RegexpSubstrSynonymMatchesRegexpExtract) {
  EXPECT_EQ(Query("SELECT REGEXP_SUBSTR('hello world', 'o.w'), "
                  "REGEXP_EXTRACT('hello world', 'o.w')"),
            (std::vector<std::string>{"\"o w\",\"o w\""}));
  EXPECT_EQ(Query("SELECT REGEXP_SUBSTR(NULL, 'o.w')"),
            (std::vector<std::string>{"NULL"}));
}

// T2239 / T2240: TO_BASE32 / FROM_BASE32 (RFC 4648 vectors, all five
// remainder classes, case-insensitive and whitespace-tolerant decode like
// the base64 arms).
TEST_F(SqlBugfixRegressionTest, Base32RoundTripAndVectors) {
  EXPECT_EQ(
      Query("SELECT TO_BASE32('a'), TO_BASE32('ab'), "
            "TO_BASE32('abc'), TO_BASE32('foobar')"),
      (std::vector<std::string>{
          "\"ME======\",\"MFRA====\",\"MFRGG===\",\"MZXW6YTBOI======\""}));
  EXPECT_EQ(Query("SELECT FROM_BASE32('MFRGG==='), FROM_BASE32('mfrgg===')"),
            (std::vector<std::string>{"\"abc\",\"abc\""}));
  EXPECT_EQ(Query("SELECT FROM_BASE32(TO_BASE32('round trip 42!'))"),
            (std::vector<std::string>{"\"round trip 42!\""}));
  StatusOr<QueryResult> bad =
      engine_->Execute(*context_, "SELECT FROM_BASE32('MFRG1===')");
  ASSERT_TRUE(bad.HasValue());
  bad.Value().Drain();
  EXPECT_NE(bad.Value().GetStatus().GetMessage().find("FROM_BASE32"),
            std::string::npos)
      << bad.Value().GetStatus().GetMessage();
}

// T2398: __struct_set recurses per dot-segment of a user path; the depth
// must be capped (64 passes, 65 is an error, not a stack overflow).
TEST_F(SqlBugfixRegressionTest, StructSetPathDepthIsCapped) {
  const std::string deep_64 = [] {
    std::string p;
    for (int i = 0; i < 64; ++i) {
      if (i > 0) {
        p += ".";
      }
      p += "a" + std::to_string(i);
    }
    return p;
  }();
  EXPECT_EQ(Query("SELECT IF(LENGTH(__struct_set(CAST('{}' AS JSON), '" +
                  deep_64 + "', 2)) > 400, 1, 0)"),
            (std::vector<std::string>{"1"}));
  StatusOr<QueryResult> over =
      engine_->Execute(*context_, "SELECT __struct_set(CAST('{}' AS JSON), '" +
                                      deep_64 + ".a64.a65', 2)");
  ASSERT_TRUE(over.HasValue());
  over.Value().Drain();
  ASSERT_NE(over.Value().GetStatus(), Status::kSuccess);
  // Deep __struct_set paths fail during projection evaluation; depending on
  // where the fold/eval boundary lands the message rides either on the
  // result status or on the engine's last_error_ string.
  const std::string detail = over.Value().GetStatus().GetMessage().find(
                                 "exceeds 64 segments") != std::string::npos
                                 ? over.Value().GetStatus().GetMessage()
                                 : engine_->LastError();
  EXPECT_NE(detail.find("exceeds 64 segments"), std::string::npos)
      << detail << " / " << engine_->LastError();
}

// T2399: the proto path route shares the same 64-segment cap.
TEST_F(SqlBugfixRegressionTest, ProtoSetPathDepthIsCapped) {
  const std::string deep_65 = [] {
    std::string p;
    for (int i = 0; i < 65; ++i) {
      if (i > 0) {
        p += ".";
      }
      p += "f" + std::to_string(i);
    }
    return p;
  }();
  StatusOr<QueryResult> over = engine_->Execute(
      *context_, "SELECT __proto_set('int64_key_1: 1', '" + deep_65 + "', 9)");
  ASSERT_TRUE(over.HasValue());
  over.Value().Drain();
  // Like the StructSet sibling above, the diagnostic may ride either the
  // result status or the engine's last_error_ depending on where the fold
  // boundary lands.
  const std::string detail =
      over.Value().GetStatus().GetMessage().find("exceeds 64 segments") !=
              std::string::npos
          ? over.Value().GetStatus().GetMessage()
          : engine_->LastError();
  EXPECT_NE(detail.find("proto field path exceeds 64 segments"),
            std::string::npos)
      << detail << " / " << engine_->LastError();
  EXPECT_EQ(Query("SELECT __proto_set('f0 { f1 { f2: 1 } }', 'f0.f1.f2', 9)"),
            (std::vector<std::string>{"\"f0 { f1 { f2: 9 } }\""}));
}

// T3754: the AST CURRENT_TIMESTAMP arm moved from ostringstream+put_time
// to a strftime buffer; pin the canonical UTC text shape.
TEST_F(SqlBugfixRegressionTest, CurrentTimestampCanonicalTextLength) {
  EXPECT_EQ(Query("SELECT LENGTH(STRING(CURRENT_TIMESTAMP()))"),
            (std::vector<std::string>{"22"}));
}

// Issue 2553: a persistent view must not capture a session-local SQL UDF.
// The defining text is cataloged for every session, but the UDF lives only in
// the creating thread's registry and session bucket, so any other reader (or
// a read-worker offload of its own query) expands the view into "unknown
// function".  CREATE VIEW now refuses the capture up front.
TEST_F(SqlBugfixRegressionTest, CreateViewRejectsSessionLocalUdfCapture) {
  Exec("CREATE TABLE base(k INT64)");
  Exec("INSERT INTO base VALUES (1), (2)");
  Exec("CREATE FUNCTION double_it(x INT64) AS (x * 2)");

  const std::optional<std::string> err =
      ErrorOf("CREATE VIEW v AS SELECT double_it(k) FROM base");
  if (!err.has_value()) {
    ADD_FAILURE() << "expected CREATE VIEW to fail";
    return;
  }
  EXPECT_NE(err->find("session-local"), std::string::npos) << *err;
  // The rejection must not leave the name occupied in the catalog.
  Exec("CREATE VIEW v AS SELECT k + 1 AS k FROM base");
  EXPECT_EQ(Query("SELECT k FROM v"), (std::vector<std::string>{"2", "3"}));

  // Nested references (inside CASE) are captures too.
  EXPECT_TRUE(ErrorOf("CREATE VIEW v2 AS SELECT CASE WHEN k > 1 THEN "
                      "double_it(k) ELSE k END FROM base")
                  .has_value());

  // A name the rewriter maps to a builtin before consulting the UDF registry
  // can never resolve to the UDF at read time, so shadowing it is not a
  // capture and must stay creatable.
  Exec("CREATE FUNCTION ucase(x STRING) AS (CONCAT(x, '!'))");
  Exec("CREATE TABLE names(s STRING)");
  Exec("INSERT INTO names VALUES ('ab')");
  Exec("CREATE VIEW nv AS SELECT ucase(s) FROM names");
  EXPECT_FALSE(ErrorOf("SELECT * FROM nv").has_value());

  // TEMP views are session objects and MAY capture session-local UDFs
  // (SqlUdfTest.TempViewBodyExpandsUdfCalls depends on it).
  Exec("CREATE TEMP FUNCTION tf(x INT64) AS (x + 1)");
  Exec("CREATE TEMP VIEW tv AS SELECT tf(k) AS n FROM base");
  EXPECT_EQ(Query("SELECT n FROM tv"), (std::vector<std::string>{"2", "3"}));
}

// A single declared field on ARRAY<STRUCT<a INT64>> must rename the eagerly
// folded {"f1":...} key; the >= 2 gate used to leave .a as NULL.
TEST_F(SqlBugfixRegressionTest, SingleDeclaredStructArrayFieldGetsItsName) {
  EXPECT_EQ(Query("SELECT (ARRAY<STRUCT<a INT64>>[STRUCT(1)])[OFFSET(0)].a"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(Query("SELECT (ARRAY<STRUCT<a INT64, b STRING>>[STRUCT(1, 'x')])"
                  "[OFFSET(0)].a"),
            (std::vector<std::string>{"1"}));
}

// A collated field inside a STRUCT must not lowercase every string field of
// the folded value: projection/get_field sees the literal's original bytes.
TEST_F(SqlBugfixRegressionTest, StructWithCollatedFieldKeepsOriginalText) {
  EXPECT_EQ(Query("SELECT STRUCT(COLLATE('AbC', 'und:ci') AS x, 'XyZ' AS y).x"),
            (std::vector<std::string>{"\"AbC\""}));
  EXPECT_EQ(Query("SELECT STRUCT(COLLATE('AbC', 'und:ci') AS x, 'XyZ' AS y).y"),
            (std::vector<std::string>{"\"XyZ\""}));
}

// Name-resolution errors must be reported on BOTH evaluator routes (default
// lowering and the forced relational fallback).  Regressions for griffin_fuzz
// GRIFFIN-RELATIONAL-PATH MISMATCH findings, where the default route ran
// queries the reference GoogleSQL analyzer rejects:
//  * `SELECT u, u FROM ix ORDER BY u`  -> "Column name u is ambiguous"
namespace {

struct RouteOutcome {
  bool ran{false};
  std::string error;
};

RouteOutcome RunOnRoute(Database* db, TransactionContext* ctx,
                        const std::string& sql, bool relational) {
  RouteOutcome out;
  SqlEngine engine(*db);
  engine.SetForceRelationalFallback(relational);
  StatusOr<QueryResult> result = engine.Execute(*ctx, sql);
  if (!result.HasValue()) {
    std::ostringstream reason;
    reason << result.GetStatus() << " " << engine.LastError();
    out.error = reason.str();
    return out;
  }
  Row row;
  while (result.Value().Next(&row)) {
  }
  if (result.Value().GetStatus() != Status::kSuccess) {
    std::ostringstream reason;
    reason << result.Value().GetStatus() << " " << engine.LastError();
    out.error = reason.str();
    return out;
  }
  out.ran = true;
  return out;
}

class RouteNameResolutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << "execute_query unavailable";
    }
    db_ = Database::Create("route_names_" + RandomString()).MoveValue();
    ctx_ = std::make_unique<TransactionContext>(db_->BeginContext());
    for (const char* sql : {
             "CREATE TABLE ix (u INT64, v INT64, s STRING)",
             "CREATE INDEX ix_u ON ix(u)",
             "INSERT INTO ix VALUES (0, 10, 'a'), (2, 20, 'b'), (NULL, 30, "
             "'c')",
             "CREATE TABLE l (lk INT64, lv STRING)",
             "INSERT INTO l VALUES (1, 'a'), (2, NULL), (NULL, 'c'), (2, 'd')",
             "CREATE TABLE r (rk INT64, rv INT64)",
             "INSERT INTO r VALUES (2, 20), (2, 21), (NULL, 0), (3, 30)",
             "CREATE TABLE rs (r STRING, c STRING, n INT64)",
             "INSERT INTO rs VALUES ('x', 'p', 1), ('y', NULL, 3)",
         }) {
      const RouteOutcome o = RunOnRoute(db_.get(), ctx_.get(), sql, false);
      ASSERT_TRUE(o.ran) << sql << ": " << o.error;
    }
  }
  void TearDown() override {
    ctx_.reset();
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
  }

  void ExpectRejectedOnBothRoutes(const std::string& sql) {
    for (const bool relational : {false, true}) {
      const RouteOutcome o = RunOnRoute(db_.get(), ctx_.get(), sql, relational);
      EXPECT_FALSE(o.ran) << (relational ? "relational" : "default")
                          << " route accepted: " << sql;
    }
  }

  void ExpectAcceptedOnBothRoutes(const std::string& sql) {
    for (const bool relational : {false, true}) {
      const RouteOutcome o = RunOnRoute(db_.get(), ctx_.get(), sql, relational);
      EXPECT_TRUE(o.ran) << (relational ? "relational" : "default")
                         << " route rejected: " << sql << ": " << o.error;
    }
  }

  std::unique_ptr<Database> db_;
  std::unique_ptr<TransactionContext> ctx_;
};

}  // namespace

TEST_F(RouteNameResolutionTest, DuplicateSelectAliasInOrderBy) {
  ExpectRejectedOnBothRoutes("SELECT u, u FROM ix ORDER BY u");
  ExpectRejectedOnBothRoutes("SELECT v, v FROM ix ORDER BY v");
  // Distinct aliases, or ordering by an unambiguous key, stay legal.
  ExpectAcceptedOnBothRoutes("SELECT u, u AS u2 FROM ix ORDER BY u");
  ExpectAcceptedOnBothRoutes("SELECT u, u FROM ix ORDER BY v");
  // Ordinals address the select-list position, even when output labels
  // repeat.
  ExpectAcceptedOnBothRoutes("SELECT u, u FROM ix ORDER BY 1");
}

// sql_aggregate_fuzz: GoogleSQL rejects DISTINCT aggregates whose inner
// ORDER BY names anything but a function argument; the engine used to run
// them and order collapsed duplicates arbitrarily.
TEST_F(RouteNameResolutionTest, DistinctAggregateOrderByMustNameArgument) {
  ExpectRejectedOnBothRoutes("SELECT ARRAY_AGG(DISTINCT u ORDER BY v) FROM ix");
  ExpectRejectedOnBothRoutes(
      "SELECT ARRAY_AGG(DISTINCT u ORDER BY v DESC LIMIT 2) FROM ix");
  ExpectRejectedOnBothRoutes(
      "SELECT STRING_AGG(DISTINCT s, ',' ORDER BY u) FROM ix");
  ExpectAcceptedOnBothRoutes(
      "SELECT ARRAY_AGG(DISTINCT u ORDER BY u DESC LIMIT 1) FROM ix");
  ExpectAcceptedOnBothRoutes(
      "SELECT STRING_AGG(DISTINCT s, ',' ORDER BY s) FROM ix");
  ExpectAcceptedOnBothRoutes("SELECT ARRAY_AGG(u ORDER BY v) FROM ix");
}

}  // namespace tinylamb
