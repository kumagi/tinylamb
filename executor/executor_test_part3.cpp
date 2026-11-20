/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdlib>

#include "executor/executor_test.hpp"

namespace tinylamb {
TEST_F(ExecutorTest, RelationalCorrelatedExistsKeepsSpilledRows) {
  // Regression guard: under a finite query-memory budget the correlated index
  // must be built from the full source relation including spilled rows, so
  // EXISTS probes still find the 49 qualifying keys (151..199).
  CreateWideTable(*rs_, "WideExists", 200);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.key FROM WideExists AS o WHERE EXISTS (SELECT 1 FROM "
      "WideExists WHERE o.key = key AND key > 150);");
  EXPECT_EQ(rows.size(), 49U);
}

TEST_F(ExecutorTest, RelationalInSubqueryKeepsSpilledRows) {
  // Regression guard: the uncorrelated IN-subquery membership cache must be
  // built from spilled rows as well; every outer key passes the IN test.
  CreateWideTable(*rs_, "WideIn", 200);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_, "SELECT key FROM WideIn WHERE key IN (SELECT key FROM WideIn);");
  EXPECT_EQ(rows.size(), 200U);
}

TEST_F(ExecutorTest, RelationalNotInSubqueryNullBuildKeyFiltersEverything) {
  // Regression guard: an uncorrelated NOT IN whose subquery emits a NULL key
  // turns every miss into UNKNOWN, so no outer row survives.  The NULL flag
  // is recorded once while the membership hash is built; per-probe NULL
  // ambiguity must never rescan the build relation (Q22-sized builds make a
  // per-row rescan quadratic).
  {
    TransactionContext ctx = rs_->BeginContext();
    Schema schema{"NullBuild", {Column("key", ValueType::kInt64)}};
    StatusOr<Table> created = rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(created.GetStatus());
    ASSERT_SUCCESS(created.Value()
                       .Insert(ctx.txn_, Row(std::vector{Value(int64_t{1})}))
                       .GetStatus());
    ASSERT_SUCCESS(created.Value()
                       .Insert(ctx.txn_, Row(std::vector{Value()}))
                       .GetStatus());
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  CreateWideTable(*rs_, "WideNotInNull", 3);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_,
      "SELECT key FROM WideNotInNull WHERE key NOT IN (SELECT key FROM "
      "NullBuild);");
  EXPECT_TRUE(rows.empty());
  // The NULL-free build keeps the complement: only non-members survive.
  const auto clean = RelationalRun(
      *rs_,
      "SELECT key FROM WideNotInNull WHERE key NOT IN (SELECT key FROM "
      "WideNotInNull WHERE key > 1);");
  ASSERT_EQ(clean.size(), 2U);
}

TEST_F(ExecutorTest, RelationalInSubqueryMixedDomainAndDecisiveHashMiss) {
  // Two invariants of the uncorrelated IN/NOT IN membership path:
  // 1. A hash miss over a NULL-free same-domain build is decisive; the
  //    canonical rescan must not run (a per-miss rescan is quadratic over
  //    Q22-sized builds).
  // 2. A mixed numeric domain (INT64 probe over a DOUBLE build and the
  //    reverse) must not answer from the type-strict hash: `=` promotion
  //    decides, so `1 IN (SELECT 1.0)` is TRUE.
  {
    TransactionContext ctx = rs_->BeginContext();
    Schema schema{"MixedDom", {Column("val", ValueType::kDouble)}};
    StatusOr<Table> created = rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(created.GetStatus());
    for (const double v : {1.0, 2.0}) {
      ASSERT_SUCCESS(created.Value()
                         .Insert(ctx.txn_, Row(std::vector{Value(v)}))
                         .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }
  CreateWideTable(*rs_, "WideMixedIn", 3);
  ScopedQueryMemory memory(65536);
  // INT64 probe over a DOUBLE build: promotion decides both directions.
  const auto int_over_double = RelationalRun(
      *rs_,
      "SELECT key FROM WideMixedIn WHERE key IN (SELECT val FROM MixedDom);");
  ASSERT_EQ(int_over_double.size(), 2U);
  const auto int_not_in_double = RelationalRun(
      *rs_,
      "SELECT key FROM WideMixedIn WHERE key NOT IN (SELECT val FROM "
      "MixedDom);");
  ASSERT_EQ(int_not_in_double.size(), 1U);
  // DOUBLE probe over an INT64 build.
  const auto double_over_int = RelationalRun(
      *rs_,
      "SELECT val FROM MixedDom WHERE val IN (SELECT key FROM WideMixedIn);");
  ASSERT_EQ(double_over_int.size(), 2U);
  // Same-domain miss over a NULL-free build stays decisive TRUE for NOT IN.
  const auto same_domain = RelationalRun(
      *rs_,
      "SELECT val FROM MixedDom WHERE val NOT IN (SELECT val FROM MixedDom "
      "WHERE val > 1.5);");
  ASSERT_EQ(same_domain.size(), 1U);
}

TEST_F(ExecutorTest, RelationalWindowFunctionKeepsSpilledRows) {
  // PRODUCTION BUG (fixed): ApplyWindows collected only input.rows, so a
  // memory-budget spill silently dropped every spilled input row and the
  // window query returned a fraction of (or zero) rows.
  CreateWideTable(*rs_, "WideWin", 200);
  ScopedQueryMemory memory(65536);
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT key, SUM(key) OVER (ORDER BY key ROWS BETWEEN "
                    "UNBOUNDED PRECEDING AND CURRENT ROW) FROM WideWin;");
  ASSERT_EQ(rows.size(), 200U);
  EXPECT_FALSE(rows[199][1].IsNull());
}

TEST_F(ExecutorTest, RelationalGroupBySpillKeepsNullKeys) {
  // PRODUCTION BUG (fixed): the partitioned aggregation spilled rows via
  // EncodeMemcomparableFormat, which throws on NULL group keys; the same
  // query grouped NULLs normally while resident.  The partitioner now uses
  // the same '\0' tag as the parallel hash join.
  CreateWideTable(*rs_, "WideNullGroup", 200);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_,
      "SELECT name, COUNT(*) FROM (SELECT CAST(NULL AS STRING) AS name, "
      "key FROM WideNullGroup) GROUP BY name;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_TRUE(rows[0][0].IsNull());
  EXPECT_EQ(rows[0][1], Value(int64_t{200}));
}

// The stale comment above ("Segfaults ... before enabling") described a
// former crash that the HAVING/aggregate-preparation fixes resolved; the
// test below has been running enabled since then.
TEST_F(ExecutorTest, RelationalCorrelatedExistsCrossJoinSpillsUnderBudget) {
  // A correlated EXISTS under a finite budget where the inner source still
  // fits in memory; the correlated index stays populated and the result cache
  // absorbs the 40000 repeated probes.
  CreateWideTable(*rs_, "WideCrossExists", 200);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_,
      "SELECT o.key FROM WideCrossExists AS o CROSS JOIN WideCrossExists "
      "AS b WHERE EXISTS (SELECT COUNT(*) FROM WideCrossExists WHERE "
      "o.key = key HAVING COUNT(*) > 0);");
  EXPECT_EQ(rows.size(), 40000U);
  const std::string plan = RelationalExplain(
      *rs_,
      "SELECT o.key FROM WideCrossExists AS o CROSS JOIN WideCrossExists "
      "AS b WHERE EXISTS (SELECT COUNT(*) FROM WideCrossExists WHERE "
      "o.key = key HAVING COUNT(*) > 0);",
      /*analyze=*/true);
  EXPECT_GT(StatsValue(plan, "correlated_result_cache_hits="), 0);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

TEST_F(ExecutorTest, RelationalAggregateSumEmptyAndDouble) {
  // SUM over an empty (all-filtered) input yields NULL, not 0.
  const auto empty =
      RelationalRun(*rs_, "SELECT SUM(score) FROM SampleTable WHERE key = 99;");
  ASSERT_EQ(empty.size(), 1U);
  EXPECT_TRUE(empty[0][0].IsNull());

  // SUM over a DOUBLE column returns a double; over INT64 an integer.
  const auto doubles =
      RelationalRun(*rs_, "SELECT SUM(score) FROM SampleTable;");
  ASSERT_EQ(doubles.size(), 1U);
  EXPECT_NEAR(doubles[0][0].value.double_value, 22.44, 1e-9);

  const auto ints = RelationalRun(*rs_, "SELECT SUM(key) FROM SampleTable;");
  ASSERT_EQ(ints.size(), 1U);
  EXPECT_EQ(ints[0][0], Value(6));
}

TEST_F(ExecutorTest, RelationalExplainResultOnlyAndDerivedAlias) {
  const std::string result_only =
      RelationalExplain(*rs_, "SELECT INTERVAL 3 DAY;");
  EXPECT_NE(result_only.find("Result rows~1"), std::string::npos)
      << result_only;

  const std::string aliased = RelationalExplain(
      *rs_, "SELECT * FROM (SELECT * FROM SampleTable) AS d;");
  EXPECT_NE(aliased.find("SubqueryScan AS d"), std::string::npos) << aliased;
}

TEST_F(ExecutorTest, RelationalExplainWithClauseCteScan) {
  // A tiny multiply-referenced CTE lifts into shared eager cells (M4): both
  // sites scan the same rows with no CTE-scan node. The map-scoped rescan
  // path stays covered by over-budget shapes (see
  // QueryTest.SqlEngineOverBudgetCteKeepsRescanPath).
  const std::string plan = RelationalExplain(
      *rs_,
      "WITH w AS (SELECT 1 AS x) SELECT * FROM w AS a JOIN w AS b ON "
      "a.x = b.x;");
  EXPECT_EQ(plan.find("CteOrMissingScan"), std::string::npos) << plan;
  EXPECT_NE(plan.find("ValuesExecutor"), std::string::npos) << plan;
}

TEST_F(ExecutorTest, RelationalExplainNestedLoopUnknownRows) {
  const std::string plan = RelationalExplain(
      *rs_,
      "SELECT * FROM (SELECT * FROM SampleTable) AS a JOIN SampleTable AS "
      "b ON a.key > b.key;");
  EXPECT_NE(plan.find("NestedLoopJoin"), std::string::npos) << plan;
  EXPECT_NE(plan.find("rows~unknown"), std::string::npos) << plan;
}

// T1794: pins that the QueryMemory annotation tracks the ACTIVE lane.
struct ScopedExecEnv {  // NOLINT(cppcoreguidelines-special-member-functions)
  ScopedExecEnv() : saved_(std::getenv("TINYLAMB_EXEC_ENGINE") != nullptr) {
    if (saved_) {
      original_ = std::getenv("TINYLAMB_EXEC_ENGINE");
    }
  }
  ~ScopedExecEnv() {
    if (saved_) {
      (void)setenv("TINYLAMB_EXEC_ENGINE", original_.c_str(), 1);
    } else {
      (void)unsetenv("TINYLAMB_EXEC_ENGINE");
    }
  }
  std::string original_;
  bool saved_;
};

TEST_F(ExecutorTest, RelationalExplainMemoryLimitBranches) {
  // EXISTS marker: keeps these joins on the relational engine so the
  // QueryMemory reporting branches stay covered (see
  // RelationalHybridHashJoinSpillsUnderBudget).
  const std::string sql =
      "SELECT * FROM SampleTable AS a JOIN SampleTable AS b ON a.key = b.key "
      "WHERE EXISTS (SELECT 1 FROM SampleTable);";
  {
    // T1794: the default lane reports the process-wide budget that
    // admission and spills actually size against, labelled as such.
    const std::string plan = RelationalExplain(*rs_, sql);
    EXPECT_NE(plan.find("(process;"), std::string::npos) << plan;
  }
  {
    // The legacy lane keeps reporting the global QueryMemoryBudget; pin
    // all three branches (unlimited / mid / byte-granular) there.
    ScopedExecEnv env;
    (void)setenv("TINYLAMB_EXEC_ENGINE", "legacy", 1);
    {
      ScopedQueryMemory unlimited(0);
      const std::string plan = RelationalExplain(*rs_, sql);
      EXPECT_NE(plan.find("QueryMemory limit=unlimited (legacy global)"),
                std::string::npos)
          << plan;
    }
    {
      ScopedQueryMemory kib(2048);
      const std::string plan = RelationalExplain(*rs_, sql);
      EXPECT_NE(plan.find("QueryMemory limit=2.0KiB"), std::string::npos)
          << plan;
    }
    {
      ScopedQueryMemory bytes(128);
      const std::string plan = RelationalExplain(*rs_, sql);
      EXPECT_NE(plan.find("QueryMemory limit=128B"), std::string::npos) << plan;
    }
  }
}

TEST_F(ExecutorTest, RelationalExplainSortLimitAnnotations) {
  // T2665: the Sort line must render the full ordering contract (key,
  // direction, NULLS placement), not just a count.  T2659: LIMIT ... WITH
  // TIES has to be distinguishable from a plain LIMIT.  T2658: the TopN
  // annotation must reflect the real work set (heap capacity
  // offset+limit; WITH TIES takes the full-sort route and no heap).
  // EXISTS keeps the query on the relational renderer.
  const std::string ties = RelationalExplain(
      *rs_,
      "SELECT name FROM SampleTable AS a WHERE EXISTS (SELECT 1 FROM "
      "SampleTable) ORDER BY name DESC NULLS LAST LIMIT 2 WITH TIES;");
  EXPECT_NE(ties.find("Sort keys=1: name DESC NULLS LAST"), std::string::npos)
      << ties;
  EXPECT_NE(ties.find("Limit count=2 offset=0 with_ties"), std::string::npos)
      << ties;
  EXPECT_NE(ties.find("TopN limit=2 with_ties(full_sort)"), std::string::npos)
      << ties;
  const std::string paged = RelationalExplain(
      *rs_,
      "SELECT name FROM SampleTable AS a WHERE EXISTS (SELECT 1 FROM "
      "SampleTable) ORDER BY key ASC NULLS LAST LIMIT 10 OFFSET 95;");
  EXPECT_NE(paged.find("TopN limit=10 offset=95 heap=105"), std::string::npos)
      << paged;
  EXPECT_NE(paged.find("Limit count=10 offset=95"), std::string::npos) << paged;
}

TEST_F(ExecutorTest, RelationalInSubqueryMissingTableThrows) {
  ExpectMessageContains(RelationalThrow(*rs_,
                                        "SELECT key FROM SampleTable WHERE "
                                        "key IN (SELECT key FROM "
                                        "missing_table);"),
                        "table missing_table not found");
}

TEST_F(ExecutorTest, RelationalDateSubAndDateColumnArithmetic) {
  const auto sub =
      RelationalRun(*rs_, "SELECT DATE_SUB('2026-03-01', INTERVAL 2 DAY);");
  ASSERT_EQ(sub.size(), 1U);
  EXPECT_EQ(sub[0][0], Value("2026-02-27"));

  RelationalRun(*rs_, "CREATE TABLE RelDateArith (d DATE, v INT64);");
  RelationalRun(*rs_,
                "INSERT INTO RelDateArith VALUES (DATE '2026-01-01', 1);");
  const auto add = RelationalRun(
      *rs_, "SELECT DATE_ADD(d, INTERVAL 1 MONTH) FROM RelDateArith;");
  ASSERT_EQ(add.size(), 1U);
  EXPECT_EQ(add[0][0].AsString(), "2026-02-01");
}

TEST_F(ExecutorTest, RelationalWithClauseMaterializesCte) {
  // WITH queries are evaluated once and shared by copy (Relation::operator=)
  // every time a source references them.
  const auto single = RelationalRun(
      *rs_, "WITH w AS (SELECT key FROM SampleTable) SELECT * FROM w;");
  ASSERT_EQ(single.size(), 4U);

  const auto joined = RelationalRun(
      *rs_,
      "WITH w AS (SELECT key FROM SampleTable) SELECT a.key FROM w AS a "
      "JOIN w AS b ON a.key = b.key;");
  ASSERT_EQ(joined.size(), 4U);
}

TEST_F(ExecutorTest, RelationalStringKeyHybridHashJoinSpillsUnderBudget) {
  // A join on the VARCHAR column takes the string-key hybrid hash join path
  // (no integer fast path), spilling both build and probe sides.
  CreateWideTable(*rs_, "WideStrJoin", 400);
  ScopedQueryMemory memory(65536);
  // EXISTS marker: keeps this on the relational engine (see
  // RelationalHybridHashJoinSpillsUnderBudget).
  const auto rows = RelationalRun(
      *rs_,
      "SELECT a.key FROM WideStrJoin AS a JOIN WideStrJoin AS b ON "
      "a.name = b.name WHERE EXISTS (SELECT 1 FROM WideStrJoin);");
  ASSERT_EQ(rows.size(), 400U);
  const std::string plan = RelationalExplain(
      *rs_,
      "SELECT a.key FROM WideStrJoin AS a JOIN WideStrJoin AS b ON "
      "a.name = b.name WHERE EXISTS (SELECT 1 FROM WideStrJoin);",
      /*analyze=*/true);
  EXPECT_EQ(StatsValue(plan, "hybrid_hash_joins="), 1);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

TEST_F(ExecutorTest, RelationalStringKeyHybridLeftJoinSpillsUnderBudget) {
  // String-key LEFT JOIN: no right row matches b.key = 999, so the hybrid join
  // emits null-extended rows from both the resident and replayed partitions.
  // The EXISTS marker forces the relational engine (M6 routes plain LEFT
  // joins to the cost-based optimizer); it is semantically neutral.
  CreateWideTable(*rs_, "WideStrLeft", 400);
  ScopedQueryMemory memory(65536);
  const auto rows = RelationalRun(
      *rs_,
      "SELECT a.key FROM WideStrLeft AS a LEFT JOIN WideStrLeft AS b ON "
      "a.name = b.name AND b.key = 999 WHERE EXISTS (SELECT 1 FROM "
      "WideStrLeft);");
  ASSERT_EQ(rows.size(), 400U);
  const std::string plan = RelationalExplain(
      *rs_,
      "SELECT a.key FROM WideStrLeft AS a LEFT JOIN WideStrLeft AS b ON "
      "a.name = b.name AND b.key = 999 WHERE EXISTS (SELECT 1 FROM "
      "WideStrLeft);",
      /*analyze=*/true);
  EXPECT_EQ(StatsValue(plan, "hybrid_hash_joins="), 1);
  EXPECT_GT(StatsValue(plan, "relation_spills="), 0);
}

TEST_F(ExecutorTest, RelationalLikeWildcardBacktracking) {
  // '%' and '_' wildcards that require backtracking through the Like matcher
  // (relational.cpp Like), plus a mid-pattern '%' that is not a trailing match.
  const auto back = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE name LIKE 'h%o' ORDER BY key;");
  ASSERT_EQ(back.size(), 1U);
  EXPECT_EQ(back[0][0], Value(0));

  const auto suffix = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE name LIKE '%lo' ORDER BY key;");
  ASSERT_EQ(suffix.size(), 1U);
  EXPECT_EQ(suffix[0][0], Value(0));

  const auto single = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE name LIKE 'h_llo' ORDER BY key;");
  ASSERT_EQ(single.size(), 1U);
  EXPECT_EQ(single[0][0], Value(0));
}

TEST_F(ExecutorTest, RelationalNumericAndVarcharBinaryOperators) {
  // GROUP BY forces the relational evaluator (a plain SELECT would use the
  // plan-based executor and never touch relational.cpp's Binary()).
  const auto numeric = RelationalRun(
      *rs_,
      "SELECT key + 1, key - 1, key * 2, key / 2 FROM SampleTable GROUP "
      "BY key;");
  ASSERT_EQ(numeric.size(), 4U);
  std::unordered_set<int64_t> seen;
  for (const Row& row : numeric) {
    seen.insert(row[0].value.int_value - 1);  // undo "key + 1"
    EXPECT_EQ(row[0].value.int_value, row[1].value.int_value + 2);
    EXPECT_EQ(row[2].value.int_value, (row[0].value.int_value - 1) * 2);
  }
  EXPECT_EQ(seen, std::unordered_set<int64_t>({0, 1, 2, 3}));

  // Double multiply inside a relational WHERE clause.
  const auto double_where = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE score * 2 > 4 GROUP BY key;");
  ASSERT_EQ(double_where.size(), 3U);

  // Non-numeric (VARCHAR) comparison operators in a relational filter.
  const auto below = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE name < 'm' GROUP BY key;");
  EXPECT_EQ(below.size(), 2U);
  const auto above_or_equal = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE name >= 'a' GROUP BY key;");
  EXPECT_EQ(above_or_equal.size(), 4U);
  const auto not_equals = RelationalRun(
      *rs_, "SELECT key FROM SampleTable WHERE name != 'hello' GROUP BY key;");
  EXPECT_EQ(not_equals.size(), 3U);
}

// ===== HashJoin executor spill-to-disk coverage =====
TEST_F(ExecutorTest, HashJoinDisjointKeysProducesEmptyOutput) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(3)}), Row({Value(4)})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory, 2);
  Row got;
  RowPosition pos;
  ASSERT_FALSE(join.Next(&got, &pos));
  DataChunk chunk;
  EXPECT_EQ(join.NextBatch(&chunk, 8), 0U);
}

TEST_F(ExecutorTest, HashJoinMultiMatchOnRightSide) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("l1")}), Row({Value(2), Value("l2")})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("r1")}), Row({Value(1), Value("r2")})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory, 2);
  std::unordered_set<Row> expected(
      {Row({Value(1), Value("l1"), Value(1), Value("r1")}),
       Row({Value(1), Value("l1"), Value(1), Value("r2")})});
  Row got;
  size_t count = 0;
  while (join.Next(&got, nullptr)) {
    ASSERT_TRUE(expected.erase(got));
    ++count;
  }
  EXPECT_EQ(count, 2U);
  EXPECT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, HashJoinCompositeStringKeyMatchesExactly) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("alice")}), Row({Value(1), Value("bob")})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("alice")}), Row({Value(2), Value("bob")})});
  HashJoin join(left, {0, 1}, right, {0, 1}, HashJoinMode::kInMemory, 2);
  Row got;
  ASSERT_TRUE(join.Next(&got, nullptr));
  EXPECT_EQ(got, Row({Value(1), Value("alice"), Value(1), Value("alice")}));
  ASSERT_FALSE(join.Next(&got, nullptr));
}

TEST_F(ExecutorTest, HashJoinEmptyInputs) {
  auto left = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory, 4);
  Row got;
  RowPosition pos;
  ASSERT_FALSE(join.Next(&got, &pos));
  DataChunk chunk;
  EXPECT_EQ(join.NextBatch(&chunk, 8), 0U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("PartitionedHashJoin"), std::string::npos);
}

TEST_F(ExecutorTest, HashJoinNullJoinKeysDoNotMatch) {
  // Regression (§6.2): a NULL join key cannot be memcomparable-encoded and
  // used to throw "Cannot encode unknown type.".  SQL semantics say NULL
  // never equals anything, so NULL-key rows are skipped on both sides.
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value()}), Row({Value(2)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value()}), Row({Value(2)})});

  // Pipelined probe path.
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory, 2);
  std::unordered_set<Row> matches;
  Row got;
  while (join.Next(&got, nullptr)) {
    ASSERT_EQ(got[0], got[1]);
    matches.insert(got);
  }
  EXPECT_EQ(matches, (std::unordered_set<Row>{Row({Value(1), Value(1)}),
                                              Row({Value(2), Value(2)})}));

  // Reactive-spill path: the same rows joined through spill partitions.
  {
    ScopedQueryMemory memory(256);
    auto spill_left = std::make_shared<ConstantExecutor>(
        std::vector<Row>{Row({Value(1)}), Row({Value()}), Row({Value(2)})});
    auto spill_right = std::make_shared<ConstantExecutor>(
        std::vector<Row>{Row({Value(1)}), Row({Value()}), Row({Value(2)})});
    HashJoin spilled(spill_left, {0}, spill_right, {0}, HashJoinMode::kInMemory,
                     2);
    size_t count = 0;
    while (spilled.Next(&got, nullptr)) {
      ASSERT_EQ(got[0], got[1]);
      ASSERT_FALSE(got[0].IsNull());
      ++count;
    }
    EXPECT_EQ(count, 2U);
  }

  // Hybrid path: partition-0 resident plus spilled partitions.
  {
    ScopedQueryMemory memory(256);
    auto hybrid_left = std::make_shared<ConstantExecutor>(
        std::vector<Row>{Row({Value(1)}), Row({Value()}), Row({Value(2)})});
    auto hybrid_right = std::make_shared<ConstantExecutor>(
        std::vector<Row>{Row({Value(1)}), Row({Value()}), Row({Value(2)})});
    HashJoin hybrid(hybrid_left, {0}, hybrid_right, {0}, HashJoinMode::kHybrid,
                    2);
    size_t count = 0;
    while (hybrid.Next(&got, nullptr)) {
      ASSERT_EQ(got[0], got[1]);
      ASSERT_FALSE(got[0].IsNull());
      ++count;
    }
    EXPECT_EQ(count, 2U);
  }
}

TEST_F(ExecutorTest, HashJoinNaNKeysDoNotMatch) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(nan)}), Row({Value(1.0)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(nan)}), Row({Value(1.0)})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory, 2);
  std::vector<Row> rows;
  Row row;
  while (join.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(1.0));
  EXPECT_EQ(rows[0][1], Value(1.0));
}

TEST_F(ExecutorTest, HashJoinNullSafeNaNKeysMatch) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(nan)}), Row({Value(1.0)})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(nan)}), Row({Value(1.0)})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory,
                JoinKind::kInner, 2);
  join.SetJoinAnnotations({true}, Expression());
  std::vector<Row> rows;
  Row row;
  while (join.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_TRUE(std::ranges::any_of(rows, [](const Row& item) {
    return item[0].type == ValueType::kDouble &&
           std::isnan(item[0].value.double_value);
  }));
}

TEST_F(ExecutorTest, HashJoinUnsignedBoundaryDoesNotCollide) {
  const Value signed_minus_one(static_cast<int64_t>(-1));
  const Value unsigned_minus_one =
      Value(std::numeric_limits<int64_t>::min()).WithUnsigned();
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({signed_minus_one}), Row({Value(5)})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({unsigned_minus_one}), Row({Value(5).WithUnsigned()})});
  HashJoin join(left, {0}, right, {0}, HashJoinMode::kInMemory, 2);
  std::vector<Row> rows;
  Row row;
  while (join.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(5));
  EXPECT_EQ(rows[0][1], Value(5).WithUnsigned());
}

TEST_F(ExecutorTest, HashJoinNextBatchSplitsOutput) {
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 5; ++i) {
    left_rows.emplace_back(Row({Value(i)}));
    right_rows.emplace_back(Row({Value(i)}));
  }
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kInMemory, 2);
  std::unordered_set<Row> expected;
  for (int64_t i = 0; i < 5; ++i) {
    expected.insert(Row({Value(i), Value(i)}));
  }
  DataChunk chunk;
  size_t total = 0;
  while (true) {
    const size_t got = join.NextBatch(&chunk, 2);
    if (got == 0) {
      break;
    }
    EXPECT_LE(got, 2U);
    for (size_t r = 0; r < got; ++r) {
      ASSERT_TRUE(expected.erase(chunk.RowAt(r)));
    }
    total += got;
  }
  EXPECT_EQ(total, 5U);
  EXPECT_TRUE(expected.empty());
}

TEST_F(ExecutorTest, HashJoinReactiveSpillIntKeyUnderBudget) {
  // 400 x ~232B rows exceed the 65536B soft budget, so MaterializeInMemory
  // reactively spills the build side to disk and probes against it.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 400; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    left_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
    right_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  class PositionedRows final : public ExecutorBase {
   public:
    explicit PositionedRows(std::vector<Row> rows) : rows_(std::move(rows)) {}
    size_t NextBatch(DataChunk* dst, size_t max_rows) override {
      dst->Reset();
      while (offset_ < rows_.size() && dst->Size() < max_rows) {
        dst->Append(rows_[offset_],
                    RowPosition(7, static_cast<slot_t>(offset_)));
        ++offset_;
      }
      return dst->Size();
    }
    void Dump(std::ostream& out, int /*indent*/) const override {
      out << "PositionedRows";
    }

   private:
    std::vector<Row> rows_{};
    size_t offset_{0};
  };
  ScopedQueryMemory memory(65536);
  HashJoin join(std::make_shared<PositionedRows>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kInMemory, 4);
  std::unordered_set<int64_t> keys;
  std::unordered_set<slot_t> positions;
  Row got;
  RowPosition pos;
  size_t count = 0;
  while (join.Next(&got, &pos)) {
    ASSERT_EQ(got[0], got[2]);
    ASSERT_EQ(got[1], got[3]);
    keys.insert(got[0].value.int_value);
    positions.insert(pos.slot);
    ++count;
  }
  EXPECT_EQ(count, 400U);
  EXPECT_EQ(keys.size(), 400U);
  EXPECT_EQ(positions.size(), 400U);
  EXPECT_TRUE(positions.contains(0));
  EXPECT_TRUE(positions.contains(399));
}

TEST_F(ExecutorTest, HashJoinReactiveSpillRightSideTriggersFlush) {
  // Left side fits in memory; the larger right side exhausts the budget, so
  // the spill flush branch must move the resident left_rows AND right_rows
  // into their spill partitions before probing from disk.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 50; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    left_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  for (int64_t i = 0; i < 400; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    right_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  ScopedQueryMemory memory(65536);
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kInMemory, 4);
  Row got;
  std::unordered_set<int64_t> keys;
  size_t count = 0;
  while (join.Next(&got, nullptr)) {
    ASSERT_EQ(got[0], got[2]);
    keys.insert(got[0].value.int_value);
    ++count;
  }
  EXPECT_EQ(count, 50U);
  EXPECT_TRUE(keys.contains(0));
  EXPECT_TRUE(keys.contains(49));
}

TEST_F(ExecutorTest, HashJoinReactiveSpillStringKeyUnderBudget) {
  // String (VARCHAR) join key: both build and probe sides spill, then each
  // hash partition is joined from its spill file.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 300; ++i) {
    std::string name = "name-" + std::to_string(i);
    name.resize(96, 'x');
    left_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(name))});
    right_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(name))});
  }
  ScopedQueryMemory memory(65536);
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {1},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {1},
                HashJoinMode::kInMemory, 4);
  Row got;
  size_t count = 0;
  while (join.Next(&got, nullptr)) {
    ASSERT_EQ(got[0], got[2]);
    ASSERT_EQ(got[1], got[3]);
    ++count;
  }
  EXPECT_EQ(count, 300U);
}

TEST_F(ExecutorTest, HashJoinHybridAllSpilledUnderBudget) {
  // 256B budget: even partition 0 cannot stay resident, so every build and
  // probe row is spilled and all 32 partitions are joined back from disk.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 200; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    left_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
    right_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  ScopedQueryMemory memory(256);
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kHybrid, 4);
  Row got;
  size_t count = 0;
  while (join.Next(&got, nullptr)) {
    ASSERT_EQ(got[0], got[2]);
    ++count;
  }
  EXPECT_EQ(count, 200U);
  std::stringstream ss;
  join.Dump(ss, 0);
  EXPECT_NE(ss.str().find("HybridHashJoin"), std::string::npos);
}

TEST_F(ExecutorTest, HashJoinHybridResidentProbeImmediate) {
  // Unlimited budget: partition 0 stays resident and left rows are probed
  // against it immediately; every other partition spills to disk.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 200; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    left_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
    right_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  ScopedQueryMemory unlimited(0);
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kHybrid, 4);
  Row got;
  size_t count = 0;
  while (join.Next(&got, nullptr)) {
    ASSERT_EQ(got[0], got[2]);
    ++count;
  }
  EXPECT_EQ(count, 200U);
}

TEST_F(ExecutorTest, HashJoinHybridResidentOverflowFoldsBuild) {
  // 4096B budget with 2000 rows: partition 0 fills its resident space and then
  // overflows into right_spill[0]; the resident build rows are folded into the
  // spilled partition before the join.
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int64_t i = 0; i < 2000; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    left_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
    right_rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  ScopedQueryMemory memory(4096);
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kHybrid, 4);
  Row got;
  size_t count = 0;
  while (join.Next(&got, nullptr)) {
    ASSERT_EQ(got[0], got[2]);
    ++count;
  }
  EXPECT_EQ(count, 2000U);
}

// ===== Sort executor spill-to-disk coverage =====
TEST_F(ExecutorTest, SortIntAscendingAndDescending) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto asc_input = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(3)}), Row({Value(1)}), Row({Value(2)}), Row({Value(1)})});
  SortExecutor asc(asc_input, schema,
                   {{ColumnValueExp("value"), true, std::nullopt}});
  Row row;
  RowPosition pos;
  std::vector<int64_t> asc_order;
  while (asc.Next(&row, &pos)) {
    asc_order.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(asc_order, (std::vector<int64_t>{1, 1, 2, 3}));

  auto desc_input = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(3)}), Row({Value(1)}), Row({Value(2)}), Row({Value(1)})});
  SortExecutor desc(desc_input, schema,
                    {{ColumnValueExp("value"), false, std::nullopt}});
  std::vector<int64_t> desc_order;
  while (desc.Next(&row, &pos)) {
    desc_order.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(desc_order, (std::vector<int64_t>{3, 2, 1, 1}));
}

TEST_F(ExecutorTest, SortMultipleKeysTieBreak) {
  const Schema schema("synthetic", {Column("a", ValueType::kInt64),
                                    Column("b", ValueType::kVarChar)});
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(1), Value("b")}), Row({Value(2), Value("a")}),
      Row({Value(1), Value("a")}), Row({Value(2), Value("b")})});
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("a"), true, std::nullopt},
                     {ColumnValueExp("b"), true, std::nullopt}});
  Row row;
  RowPosition pos;
  std::vector<Row> out;
  while (sort.Next(&row, &pos)) {
    out.push_back(row);
  }
  EXPECT_EQ(out,
            std::vector<Row>(
                {Row({Value(1), Value("a")}), Row({Value(1), Value("b")}),
                 Row({Value(2), Value("a")}), Row({Value(2), Value("b")})}));
}

TEST_F(ExecutorTest, SortVarcharKey) {
  const Schema schema("synthetic", {Column("name", ValueType::kVarChar)});
  auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value("pear")}), Row({Value("apple")}), Row({Value("banana")})});
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("name"), true, std::nullopt}});
  Row row;
  RowPosition pos;
  std::vector<std::string> out;
  while (sort.Next(&row, &pos)) {
    out.emplace_back(row[0].value.varchar_value);
  }
  EXPECT_EQ(out, (std::vector<std::string>{"apple", "banana", "pear"}));
}

TEST_F(ExecutorTest, SortDoubleAndDateKeys) {
  const Schema dbl("synthetic", {Column("score", ValueType::kDouble)});
  auto dbl_input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1.5)}), Row({Value(-2.0)}), Row({Value(0.5)}),
                       Row({Value(-2.0)})});
  SortExecutor dbl_sort(dbl_input, dbl,
                        {{ColumnValueExp("score"), true, std::nullopt}});
  Row row;
  RowPosition pos;
  std::vector<double> out;
  while (dbl_sort.Next(&row, &pos)) {
    out.push_back(row[0].value.double_value);
  }
  ASSERT_EQ(out.size(), 4U);
  for (size_t i = 1; i < out.size(); ++i) {
    EXPECT_LE(out[i - 1], out[i]);
  }

  const Schema date("synthetic", {Column("d", ValueType::kDate),
                                  Column("v", ValueType::kInt64)});
  auto date_input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value::Date("2026-03-01"), Value(1)}),
                       Row({Value::Date("2026-01-15"), Value(2)}),
                       Row({Value::Date("2025-12-31"), Value(3)}),
                       Row({Value::Date("2026-01-15"), Value(4)})});
  SortExecutor date_sort(date_input, date,
                         {{ColumnValueExp("d"), true, std::nullopt}});
  std::vector<int64_t> v;
  while (date_sort.Next(&row, &pos)) {
    v.push_back(row[1].value.int_value);
  }
  // Equal dates keep their input order (stable sort).
  EXPECT_EQ(v, (std::vector<int64_t>{3, 2, 4, 1}));
}

TEST_F(ExecutorTest, SortNullsAscendingFirstDescendingLast) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto collect = [&](bool ascending) {
    auto input = std::make_shared<ConstantExecutor>(std::vector<Row>{
        Row({Value()}), Row({Value(1)}), Row({Value()}), Row({Value(0)})});
    SortExecutor sort(input, schema,
                      {{ColumnValueExp("value"), ascending, std::nullopt}});
    Row row;
    RowPosition pos;
    std::vector<int64_t> ints;
    std::vector<bool> null_flags;
    while (sort.Next(&row, &pos)) {
      null_flags.push_back(row[0].IsNull());
      if (!row[0].IsNull()) {
        ints.push_back(row[0].value.int_value);
      }
    }
    return std::make_pair(ints, null_flags);
  };

  const auto asc = collect(true);
  EXPECT_EQ(asc.first, (std::vector<int64_t>{0, 1}));
  EXPECT_EQ(asc.second, (std::vector<bool>{true, true, false, false}));

  const auto desc = collect(false);
  EXPECT_EQ(desc.first, (std::vector<int64_t>{1, 0}));
  EXPECT_EQ(desc.second, (std::vector<bool>{false, false, true, true}));
}

// Explicit NULLS LAST must place NULLs after VARCHAR/DOUBLE keys: the null
// sentinel byte must not collide with the memcomparable type tags.
TEST_F(ExecutorTest, SortVarcharNullsLastAfterValues) {
  const Schema schema("synthetic", {Column("s", ValueType::kVarChar)});
  auto collect = [&](bool nulls_first) {
    auto input = std::make_shared<ConstantExecutor>(
        std::vector<Row>{Row({Value("b")}), Row({Value()}), Row({Value("a")})});
    SortExecutor sort(input, schema,
                      {{ColumnValueExp("s"), true, nulls_first}});
    Row row;
    RowPosition pos;
    std::vector<std::string> order;
    while (sort.Next(&row, &pos)) {
      order.push_back(
          row[0].IsNull() ? "<NULL>" : std::string(row[0].value.varchar_value));
    }
    return order;
  };

  EXPECT_EQ(collect(false), (std::vector<std::string>{"a", "b", "<NULL>"}));
  EXPECT_EQ(collect(true), (std::vector<std::string>{"<NULL>", "a", "b"}));
}

// Same contract for DOUBLE keys: the old '\x02' null sentinel sorted before
// the '\x03' double type tag, flipping NULLS LAST into NULLS FIRST.
TEST_F(ExecutorTest, SortDoubleNullsLastAfterValues) {
  const Schema schema("synthetic", {Column("d", ValueType::kDouble)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(2.5)}), Row({Value()}), Row({Value(1.5)})});
  SortExecutor sort(input, schema, {{ColumnValueExp("d"), true, false}});
  Row row;
  RowPosition pos;
  std::vector<double> values;
  std::vector<bool> null_flags;
  while (sort.Next(&row, &pos)) {
    null_flags.push_back(row[0].IsNull());
    if (!row[0].IsNull()) {
      values.push_back(row[0].value.double_value);
    }
  }
  EXPECT_EQ(values, (std::vector<double>{1.5, 2.5}));
  EXPECT_EQ(null_flags, (std::vector<bool>{false, false, true}));
}

TEST_F(ExecutorTest, SortNextBatchSplitsSortedOutput) {
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(5)}), Row({Value(1)}), Row({Value(4)}),
                       Row({Value(2)}), Row({Value(3)})});
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("value"), true, std::nullopt}});
  DataChunk chunk;
  EXPECT_EQ(sort.NextBatch(&chunk, 2), 2U);
  EXPECT_EQ(chunk.RowAt(0), Row({Value(1)}));
  EXPECT_EQ(chunk.RowAt(1), Row({Value(2)}));
  EXPECT_EQ(sort.NextBatch(&chunk, 2), 2U);
  EXPECT_EQ(sort.NextBatch(&chunk, 2), 1U);
  EXPECT_EQ(sort.NextBatch(&chunk, 2), 0U);
}

TEST_F(ExecutorTest, SortExternalSpillAscendingMergesRuns) {
  // 3000 rows against a 16384B budget: Materialize spills sorted runs to disk
  // and merges them with a priority queue.
  const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
  auto input = std::make_shared<SyntheticBatchExecutor>(3000);
  ScopedQueryMemory memory(16384);
  SortExecutor sort(input, schema,
                    {{ColumnValueExp("value"), true, std::nullopt}});
  int64_t previous = -1;
  size_t count = 0;
  Row row;
  RowPosition pos;
  while (sort.Next(&row, &pos)) {
    EXPECT_GE(row[0].value.int_value, previous);
    previous = row[0].value.int_value;
    ++count;
  }
  EXPECT_EQ(count, 3000U);
  EXPECT_EQ(previous, 99);
}

TEST_F(ExecutorTest, SortExternalSpillDescendingPreservesPositions) {
  // 300 wide rows descending under a 16384B budget; every row's RowPosition
  // must survive the spill/merge round trip.
  std::vector<Row> rows;
  for (int64_t i = 0; i < 300; ++i) {
    std::string payload = "row-" + std::to_string(i);
    payload.resize(96, 'x');
    rows.emplace_back(
        std::vector<Value>{Value(i), Value(std::string(payload))});
  }
  const Schema schema("wide", {Column("key", ValueType::kInt64),
                               Column("name", ValueType::kVarChar)});
  class PositionedRows final : public ExecutorBase {
   public:
    explicit PositionedRows(std::vector<Row> rows) : rows_(std::move(rows)) {}
    size_t NextBatch(DataChunk* dst, size_t max_rows) override {
      dst->Reset();
      while (offset_ < rows_.size() && dst->Size() < max_rows) {
        dst->Append(rows_[offset_],
                    RowPosition(7, static_cast<slot_t>(offset_)));
        ++offset_;
      }
      return dst->Size();
    }
    void Dump(std::ostream& out, int /*indent*/) const override {
      out << "PositionedRows";
    }

   private:
    std::vector<Row> rows_{};
    size_t offset_{0};
  };
  ScopedQueryMemory memory(16384);
  SortExecutor sort(std::make_shared<PositionedRows>(std::move(rows)), schema,
                    {{ColumnValueExp("key"), false, std::nullopt}});
  int64_t previous = 300;
  size_t count = 0;
  Row row;
  RowPosition pos;
  while (sort.Next(&row, &pos)) {
    EXPECT_LT(row[0].value.int_value, previous);
    EXPECT_EQ(static_cast<int64_t>(pos.slot), row[0].value.int_value);
    EXPECT_EQ(pos.page_id, static_cast<page_id_t>(7));
    previous = row[0].value.int_value;
    ++count;
  }
  EXPECT_EQ(count, 300U);
}

TEST_F(ExecutorTest, HashJoinVarcharAndDoubleKeysAndGrowth) {
  TransactionContext ctx = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, left_tbl,
      rs_->CreateTable(ctx, Schema{"LeftTbl",
                                   {Column("vkey", ValueType::kVarChar),
                                    Column("dkey", ValueType::kDouble),
                                    Column("val", ValueType::kInt64)}}));
  ASSIGN_OR_ASSERT_FAIL(
      Table, right_tbl,
      rs_->CreateTable(ctx, Schema{"RightTbl",
                                   {Column("vkey", ValueType::kVarChar),
                                    Column("dkey", ValueType::kDouble),
                                    Column("val", ValueType::kInt64)}}));
  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  for (int i = 0; i < 30; ++i) {
    std::string v = "long_varchar_key_" + std::to_string(i);
    double d = static_cast<double>(i) * 1.5;
    left_rows.emplace_back(
        Row({Value(std::string(v)), Value(d), Value(int64_t{i})}));
    if (i % 2 == 0) {
      right_rows.emplace_back(
          Row({Value(std::string(v)), Value(d), Value(int64_t{i} * 10)}));
    }
  }
  for (const auto& r : left_rows) {
    ASSERT_SUCCESS(left_tbl.Insert(ctx.txn_, r).GetStatus());
  }
  for (const auto& r : right_rows) {
    ASSERT_SUCCESS(right_tbl.Insert(ctx.txn_, r).GetStatus());
  }

  // Hash join on varchar key (> 8 chars, triggers grow and multi-block
  // encoding)
  HashJoin hj_varchar(std::make_shared<FullScan>(ctx.txn_, left_tbl), {0},
                      std::make_shared<FullScan>(ctx.txn_, right_tbl), {0});
  size_t count_v = 0;
  Row r;
  while (hj_varchar.Next(&r, nullptr)) {
    ++count_v;
  }
  EXPECT_EQ(count_v, 15U);

  // Hash join on double key
  HashJoin hj_double(std::make_shared<FullScan>(ctx.txn_, left_tbl), {1},
                     std::make_shared<FullScan>(ctx.txn_, right_tbl), {1});
  size_t count_d = 0;
  while (hj_double.Next(&r, nullptr)) {
    ++count_d;
  }
  EXPECT_EQ(count_d, 15U);
}

TEST_F(ExecutorTest, AggregationTypedConstantFastPath) {
  std::vector<Row> rows;
  rows.reserve(100);
  for (int64_t i = 0; i < 100; ++i) {
    rows.emplace_back(std::vector<Value>{Value(i)});
  }
  const Schema schema("s", {Column("v", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression(
          "sum_c", AggregateExpressionExp(AggregationType::kSum,
                                          ConstantValueExp(Value(int64_t{1})))),
      NamedExpression("count_c", AggregateExpressionExp(
                                     AggregationType::kCount,
                                     ConstantValueExp(Value(int64_t{42})))),
      NamedExpression(
          "min_c", AggregateExpressionExp(AggregationType::kMin,
                                          ConstantValueExp(Value(int64_t{5})))),
      NamedExpression("max_c", AggregateExpressionExp(
                                   AggregationType::kMax,
                                   ConstantValueExp(Value(int64_t{5}))))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates), 64);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(int64_t{100}));
  EXPECT_EQ(result[1], Value(int64_t{100}));
  EXPECT_EQ(result[2], Value(int64_t{5}));
  EXPECT_EQ(result[3], Value(int64_t{5}));
}

TEST_F(ExecutorTest, AggregationUnsignedInt64) {
  std::vector<Row> rows = {
      Row({Value(int64_t{10}).WithUnsigned()}),
      Row({Value(static_cast<int64_t>(18446744073709551600ULL))
               .WithUnsigned()}),
      Row({Value(int64_t{5}).WithUnsigned()})};
  Column col("u", ValueType::kInt64);
  col.SetUnsigned(true);
  const Schema schema("s", {col});
  std::vector<NamedExpression> aggregates = {
      NamedExpression("min_u", AggregateExpressionExp(AggregationType::kMin,
                                                      ColumnValueExp("u"))),
      NamedExpression("max_u", AggregateExpressionExp(AggregationType::kMax,
                                                      ColumnValueExp("u"))),
      NamedExpression("sum_u", AggregateExpressionExp(AggregationType::kSum,
                                                      ColumnValueExp("u"))),
      NamedExpression("bit_and_u",
                      AggregateExpressionExp(AggregationType::kBitAnd,
                                             ColumnValueExp("u"))),
      NamedExpression("bit_or_u",
                      AggregateExpressionExp(AggregationType::kBitOr,
                                             ColumnValueExp("u")))};
  AggregationExecutor aggregate(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates), 64);
  Row result;
  ASSERT_TRUE(aggregate.Next(&result, nullptr));
  EXPECT_TRUE(result[0].IsUnsigned());
  EXPECT_EQ(result[0].AsString(), "5");
  EXPECT_TRUE(result[1].IsUnsigned());
  EXPECT_EQ(result[1].AsString(), "18446744073709551600");
  EXPECT_TRUE(result[2].IsUnsigned());
  EXPECT_EQ(result[2].AsString(), "18446744073709551615");
  EXPECT_TRUE(result[3].IsUnsigned());
  EXPECT_EQ(result[3].AsString(), "0");
  EXPECT_TRUE(result[4].IsUnsigned());
  EXPECT_EQ(result[4].AsString(), "18446744073709551615");
}

TEST_F(ExecutorTest, ParallelAggregationLogicalAndOr) {
  std::vector<Row> rows = {Row({Value(int64_t{1}), Value(int64_t{1})}),
                           Row({Value(int64_t{1}), Value(int64_t{0})}),
                           Row({Value(int64_t{1}), Value(int64_t{1})})};
  const Schema schema(
      "s", {Column("c1", ValueType::kInt64), Column("c2", ValueType::kInt64)});
  std::vector<NamedExpression> aggregates = {
      NamedExpression(
          "and1",
          AggregateExpressionExp(
              AggregationType::kLogicalAnd,
              BinaryExpressionExp(ColumnValueExp("c1"), BinaryOperation::kAdd,
                                  ConstantValueExp(Value(int64_t{0}))))),
      NamedExpression(
          "and2",
          AggregateExpressionExp(
              AggregationType::kLogicalAnd,
              BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kAdd,
                                  ConstantValueExp(Value(int64_t{0}))))),
      NamedExpression(
          "or2",
          AggregateExpressionExp(
              AggregationType::kLogicalOr,
              BinaryExpressionExp(ColumnValueExp("c2"), BinaryOperation::kAdd,
                                  ConstantValueExp(Value(int64_t{0})))))};
  ParallelAggregationExecutor agg(
      std::make_shared<ConstantExecutor>(std::move(rows)), schema,
      std::move(aggregates), 64);
  Row result;
  ASSERT_TRUE(agg.Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(int64_t{1}));
  EXPECT_EQ(result[1], Value(int64_t{0}));
  EXPECT_EQ(result[2], Value(int64_t{1}));
}

TEST_F(ExecutorTest, ProjectionCseEliminatesRepeatedBinaryExpression) {
  // CSE (Common Subexpression Elimination) should detect and factor out
  // repeated binary expressions (e.g. a.x + a.y used twice).
  std::vector<Row> rows;
  rows.reserve(100);
  for (int64_t i = 0; i < 100; ++i) {
    rows.emplace_back(std::vector<Value>{Value(i), Value(i * 10)});
  }
  const Schema schema(
      "cse", {Column("x", ValueType::kInt64), Column("y", ValueType::kInt64)});
  auto src = std::make_shared<ConstantExecutor>(std::move(rows));
  // Expression: x + y (appears twice)
  auto sum_expr = BinaryExpressionExp(
      ColumnValueExp("x"), BinaryOperation::kAdd, ColumnValueExp("y"));
  // Projection: [x + y, x + y]
  std::vector<NamedExpression> projections = {NamedExpression("s1", sum_expr),
                                              NamedExpression("s2", sum_expr)};
  Projection proj(std::move(projections), schema, std::move(src));

  // Verify CSE was detected
  EXPECT_GT(proj.CseUseCounts().size(), 0U);

  // Verify results
  Row got;
  size_t count = 0;
  while (proj.Next(&got, nullptr)) {
    EXPECT_EQ(got[0], got[1]);
    EXPECT_EQ(got[0], Value(static_cast<int64_t>(count * 11)));
    ++count;
  }
  EXPECT_EQ(count, 100U);
}

TEST_F(ExecutorTest, ProjectionCseHandlesSingleOccurrenceExpression) {
  // Expressions that appear only once should NOT be CSE'd.
  std::vector<Row> rows;
  rows.reserve(10);
  for (int64_t i = 0; i < 10; ++i) {
    rows.emplace_back(std::vector<Value>{Value(i)});
  }
  const Schema schema("no_cse", {Column("v", ValueType::kInt64)});
  auto src = std::make_shared<ConstantExecutor>(std::move(rows));
  // Projection: [v + 1, v + 2] - no shared subexpression
  std::vector<NamedExpression> projections = {
      NamedExpression(
          "a", BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kAdd,
                                   ConstantValueExp(Value(int64_t{1})))),
      NamedExpression(
          "b", BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kAdd,
                                   ConstantValueExp(Value(int64_t{2}))))};
  Projection proj(std::move(projections), schema, std::move(src));

  // No CSE should be detected
  EXPECT_EQ(proj.CseUseCounts().size(), 0U);

  // Results should still be correct
  Row got;
  size_t count = 0;
  while (proj.Next(&got, nullptr)) {
    EXPECT_EQ(got[0], Value(static_cast<int64_t>(count + 1)));
    EXPECT_EQ(got[1], Value(static_cast<int64_t>(count + 2)));
    ++count;
  }
  EXPECT_EQ(count, 10U);
}

// D5 (docs/design.md) acceptance 1: run the SAME inputs through the direct
// Optimizer route and the relational route (the semantically neutral EXISTS
// marker forces the relational engine) and require identical row sets, NULL
// handling and schema for the shapes that previously exposed rule bugs:
// COUNT(*), outer join + IS NULL anti-selection, and a three-table join
// under WHERE.
TEST_F(ExecutorTest, OptimizerAndRelationalPathsAgree) {
  RelationalRun(*rs_, "CREATE TABLE D5A (id INT64, k INT64);");
  RelationalRun(*rs_, "CREATE TABLE D5B (id INT64, v INT64);");
  RelationalRun(*rs_, "CREATE TABLE D5C (id INT64, w INT64);");
  RelationalRun(*rs_,
                "INSERT INTO D5A VALUES (1, 10), (2, 20), (3, NULL), "
                "(4, 40), (5, NULL);");
  RelationalRun(*rs_, "INSERT INTO D5B VALUES (1, 100), (2, 200), (6, 600);");
  RelationalRun(*rs_, "INSERT INTO D5C VALUES (1, 7), (2, 8), (3, 9);");

  const std::vector<std::pair<std::string, std::string>> pairs{
      {"SELECT COUNT(*) FROM D5A;",
       "SELECT COUNT(*) FROM D5A WHERE EXISTS (SELECT 1 FROM D5A);"},
      {"SELECT a.id FROM D5A AS a LEFT JOIN D5B AS b ON a.id = b.id "
       "WHERE b.id IS NULL;",
       "SELECT a.id FROM D5A AS a LEFT JOIN D5B AS b ON a.id = b.id "
       "WHERE b.id IS NULL AND EXISTS (SELECT 1 FROM D5C);"},
      {"SELECT a.id, b.v, c.w FROM D5A AS a JOIN D5B AS b ON a.id = b.id "
       "JOIN D5C AS c ON a.id = c.id WHERE a.k > 5;",
       "SELECT a.id, b.v, c.w FROM D5A AS a JOIN D5B AS b ON a.id = b.id "
       "JOIN D5C AS c ON a.id = c.id WHERE a.k > 5 AND "
       "EXISTS (SELECT 1 FROM D5C AS e WHERE e.id = a.id);"},
  };
  for (const auto& [optimizer_sql, relational_sql] : pairs) {
    const auto normalize = [](const std::vector<Row>& rows) {
      std::vector<std::string> lines;
      for (const Row& row : rows) {
        std::ostringstream out;
        out << row;
        lines.push_back(out.str());
      }
      std::ranges::sort(lines);
      return lines;
    };
    const auto optimized = normalize(RelationalRun(*rs_, optimizer_sql));
    const auto relational = normalize(RelationalRun(*rs_, relational_sql));
    EXPECT_EQ(optimized, relational) << "divergence for: " << optimizer_sql;
    EXPECT_FALSE(optimized.empty());
  }
}

// Row-independent subqueries in a scan filter fold to constants at filter
// compile time (FoldRowIndependentSubqueries).  avg(score) over the fixture =
// (1.2 + 12.2 + 4.9 + 4.14) / 4 = 5.61, so exactly key 3 qualifies.
TEST_F(ExecutorTest, RelationalUncorrelatedScalarSubqueryFoldKeepsResults) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE score > "
                    "(SELECT avg(score) FROM SampleTable) ORDER BY key;");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(3));
}

// Uncorrelated membership and EXISTS probes share the same fold contract;
// their folded constants must reproduce the three-valued per-row verdicts.
TEST_F(ExecutorTest, RelationalUncorrelatedMembershipAndExistsFold) {
  const auto not_in = RelationalRun(
      *rs_,
      "SELECT key FROM SampleTable WHERE key NOT IN "
      "(SELECT key FROM SampleTable WHERE key <= 1) ORDER BY key;");
  ASSERT_EQ(not_in.size(), 2U);
  EXPECT_EQ(not_in[0][0], Value(2));
  EXPECT_EQ(not_in[1][0], Value(3));

  const auto exists =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE key = 0 AND EXISTS "
                    "(SELECT 1 FROM SampleTable WHERE key = 3);");
  ASSERT_EQ(exists.size(), 1U);
  EXPECT_EQ(exists[0][0], Value(0));
}

// Correlated subqueries must never fold: the per-row verdict decides the row.
// min(score of the OTHER rows) is 1.2 for every outer row, so keys 1, 2, 3
// (score >= 1.2) qualify while key 0 (1.2 > 1.2 is false) does not.  A wrong
// compile-time fold would use one global constant instead and keep key 0.
TEST_F(ExecutorTest, RelationalCorrelatedSubqueryStaysPerRow) {
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT a.key FROM SampleTable a WHERE a.score >= "
                    "(SELECT min(b.score) FROM SampleTable b "
                    "WHERE b.key <> a.key) ORDER BY a.key;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(1));
  EXPECT_EQ(rows[1][0], Value(2));
  EXPECT_EQ(rows[2][0], Value(3));
}

// `substr(name, 1, 2) IN (...)` compiles to the byte-prefix membership
// predicate: ASCII prefixes keep the byte compare equivalent to the AST's
// code-point substr, including NULL, short, and multi-byte-leading values.
TEST_F(ExecutorTest, RelationalPrefixInPeelMatchesSubstrVerdicts) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema schema{"PrefixTable",
                  {Column("key", ValueType::kInt64),
                   Column("name", ValueType::kVarChar)}};
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    BulkInsert(writer.txn_, table,
               {{Row({Value(0), Value("hello")})},  // prefix "he" -> match
                {Row({Value(1), Value("he")})},     // exact prefix -> match
                {Row({Value(2), Value("h")})},      // shorter than prefix
                {Row({Value(3), Value("hex")})},    // prefix "he" -> match
                {Row({Value(4), Value("héllo")})},  // multi-byte 2nd cp
                {Row({Value(5), Value("πe")})},     // multi-byte 1st cp
                {Row({Value(6), Value()})}});       // NULL -> UNKNOWN
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT key FROM PrefixTable WHERE "
                    "substr(name, 1, 2) IN ('he', 'ho') ORDER BY key;");
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0][0], Value(0));
  EXPECT_EQ(rows[1][0], Value(1));
  EXPECT_EQ(rows[2][0], Value(3));

  // A start offset other than 1 stays on the AST residual path and must keep
  // its exact semantics: substr(name, 2, 2) matches only "hello" -> "el".
  const auto offset_rows =
      RelationalRun(*rs_,
                    "SELECT key FROM SampleTable WHERE "
                    "substr(name, 2, 2) IN ('el') ORDER BY key;");
  ASSERT_EQ(offset_rows.size(), 1U);
  EXPECT_EQ(offset_rows[0][0], Value(0));
}

// `col [NOT] IN (uncorrelated subquery)` peels to a compile-time value set
// (PrebuiltMembershipPredicate).  These pin the exact three-valued semantics
// the peel must reproduce: NULL members, NULL probes, and the decisive
// empty set.
TEST_F(ExecutorTest, RelationalPrebuiltMembershipMatchesThreeValuedIn) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema outer_schema{
        "MemOuter",
        {Column("key", ValueType::kInt64), Column("d", ValueType::kDate)}};
    ASSIGN_OR_ASSERT_FAIL(Table, outer, rs_->CreateTable(writer, outer_schema));
    BulkInsert(writer.txn_, outer,
               {{Row({Value(1), Value::DateFromDays(100)})},
                {Row({Value(2), Value::DateFromDays(200)})},
                {Row({Value(3), Value::DateFromDays(300)})},
                {Row({Value(), Value::DateFromDays(400)})}});
    Schema inner_schema{
        "MemInner",
        {Column("k", ValueType::kInt64), Column("dd", ValueType::kDate)}};
    ASSIGN_OR_ASSERT_FAIL(Table, inner, rs_->CreateTable(writer, inner_schema));
    // Members 1, 2 and one NULL: IN keeps {1}; NOT IN rejects everything
    // (the NULL member turns every miss UNKNOWN).
    BulkInsert(writer.txn_, inner,
               {{Row({Value(1), Value::DateFromDays(100)})},
                {Row({Value(2), Value::DateFromDays(200)})},
                {Row({Value(), Value()})}});
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto in_rows = RelationalRun(*rs_,
                                     "SELECT key FROM MemOuter WHERE key IN "
                                     "(SELECT k FROM MemInner) ORDER BY key;");
  // Members are {1, 2, NULL}: keys 1 and 2 match.
  ASSERT_EQ(in_rows.size(), 2U);
  EXPECT_EQ(in_rows[0][0], Value(1));
  EXPECT_EQ(in_rows[1][0], Value(2));

  const auto not_in_rows =
      RelationalRun(*rs_,
                    "SELECT key FROM MemOuter WHERE key NOT IN "
                    "(SELECT k FROM MemInner);");
  EXPECT_TRUE(not_in_rows.empty());

  // NULL probe: `NULL IN (...)` is UNKNOWN for any non-empty set.
  const auto null_probe =
      RelationalRun(*rs_,
                    "SELECT d FROM MemOuter WHERE key IS NULL AND key IN "
                    "(SELECT k FROM MemInner);");
  EXPECT_TRUE(null_probe.empty());

  // DATE probes over a DATE member set take the same peel; members are
  // days {100, 200, NULL}.
  const auto date_rows =
      RelationalRun(*rs_,
                    "SELECT key FROM MemOuter WHERE d IN "
                    "(SELECT dd FROM MemInner) ORDER BY key;");
  ASSERT_EQ(date_rows.size(), 2U);
  EXPECT_EQ(date_rows[0][0], Value(1));
  EXPECT_EQ(date_rows[1][0], Value(2));
}

// The empty member set is decisive without consulting the probe: `x IN ()`
// is FALSE and `x NOT IN ()` is TRUE even for a NULL x.
TEST_F(ExecutorTest, RelationalPrebuiltMembershipEmptySetIsDecisive) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema outer_schema{"EmptyOuter", {Column("key", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, outer, rs_->CreateTable(writer, outer_schema));
    BulkInsert(writer.txn_, outer, {{Row({Value(1)})}, {Row({Value()})}});
    Schema inner_schema{"EmptyInner", {Column("k", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, inner, rs_->CreateTable(writer, inner_schema));
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto all = RelationalRun(*rs_,
                                 "SELECT key FROM EmptyOuter WHERE key NOT IN "
                                 "(SELECT k FROM EmptyInner) ORDER BY key;");
  ASSERT_EQ(all.size(), 2U);
  // ASC orders NULL first (the engine default).
  EXPECT_TRUE(all[0][0].IsNull());
  EXPECT_EQ(all[1][0], Value(1));
  const auto none = RelationalRun(*rs_,
                                  "SELECT key FROM EmptyOuter WHERE key IN "
                                  "(SELECT k FROM EmptyInner);");
  EXPECT_TRUE(none.empty());
}

// `col op (correlated min/max scalar subquery)` peels to a pre-aggregated
// key->value map (PrebuiltScalarComparePredicate): one GROUP BY execution
// replaces the per-key correlated applies.  These pin the per-key values,
// the empty-group scalar NULL, and the NULL-probe rejection.
TEST_F(ExecutorTest, RelationalPrebuiltScalarCompareMatchesCorrelatedMin) {
  {
    TransactionContext writer = rs_->BeginContext();
    Schema part_schema{
        "ScalarPart",
        {Column("pid", ValueType::kInt64), Column("price", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, part, rs_->CreateTable(writer, part_schema));
    BulkInsert(writer.txn_, part,
               {{Row({Value(1), Value(10)})},
                {Row({Value(2), Value(20)})},
                {Row({Value(3), Value(30)})},
                {Row({Value(), Value(40)})}});
    Schema supply_schema{
        "ScalarSupply",
        {Column("pid", ValueType::kInt64), Column("cost", ValueType::kInt64),
         Column("flag", ValueType::kInt64)}};
    ASSIGN_OR_ASSERT_FAIL(Table, supply,
                          rs_->CreateTable(writer, supply_schema));
    // Per-pid MIN(cost) over flag=1 rows: pid1 -> 5, pid2 -> 20 (its only
    // flag=1 row), pid3 -> no flag=1 rows (scalar NULL).
    BulkInsert(writer.txn_, supply,
               {{Row({Value(1), Value(9), Value(0)})},
                {Row({Value(1), Value(5), Value(1)})},
                {Row({Value(1), Value(7), Value(1)})},
                {Row({Value(2), Value(20), Value(1)})},
                {Row({Value(2), Value(1), Value(0)})},
                {Row({Value(3), Value(1), Value(0)})},
                {Row({Value(), Value(1), Value(1)})}});
    ASSERT_SUCCESS(writer.txn_.PreCommit());
  }
  const auto min_rows =
      RelationalRun(*rs_,
                    "SELECT pid FROM ScalarPart WHERE price = "
                    "(SELECT MIN(cost) FROM ScalarSupply "
                    "WHERE ScalarSupply.pid = ScalarPart.pid AND flag = 1) "
                    "ORDER BY pid;");
  // pid1: 10 != MIN(5,7)=5.  pid2: 20 = 20.  pid3: scalar NULL -> UNKNOWN.
  // NULL probe -> UNKNOWN.
  ASSERT_EQ(min_rows.size(), 1U);
  EXPECT_EQ(min_rows[0][0], Value(2));

  const auto gt_rows =
      RelationalRun(*rs_,
                    "SELECT pid FROM ScalarPart WHERE price > "
                    "(SELECT MIN(cost) FROM ScalarSupply "
                    "WHERE ScalarSupply.pid = ScalarPart.pid AND flag = 1) "
                    "ORDER BY pid;");
  ASSERT_EQ(gt_rows.size(), 1U);
  EXPECT_EQ(gt_rows[0][0], Value(1));  // 10 > 5.

  // MAX over the same correlation: pid1 -> 7, pid2 -> 20, pid3 -> NULL.
  const auto max_rows =
      RelationalRun(*rs_,
                    "SELECT pid FROM ScalarPart WHERE price = "
                    "(SELECT MAX(cost) FROM ScalarSupply "
                    "WHERE ScalarSupply.pid = ScalarPart.pid AND flag = 1) "
                    "ORDER BY pid;");
  // pid1: 10 != MAX(5,7)=7.  pid2: 20 = 20.  pid3: scalar NULL.
  ASSERT_EQ(max_rows.size(), 1U);
  EXPECT_EQ(max_rows[0][0], Value(2));
}

TEST_F(ExecutorTest, TypedMorselTopNKeepsNullOrderStableTiesAndPositions) {
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, table,
        rs_->CreateTable(
            writer,
            Schema("TypedTop", {Column("k", ValueType::kInt64),
                                Column("id", ValueType::kInt64),
                                Column("padding", ValueType::kVarChar)})));
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({i % 9 == 0 ? Value() : Value(i % 5), Value(i),
                           Value(std::string(12000, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("TypedTop"));
  const std::vector<slot_t> projection{0, 1};
  const Schema schema("TypedTop", {Column("k", ValueType::kInt64),
                                   Column("id", ValueType::kInt64)});
  for (const bool ascending : {true, false}) {
    for (const bool nulls_first : {true, false}) {
      const std::vector<TopNExecutor::Key> keys{
          {ColumnValueExp(ColumnName("k")), ascending, nulls_first}};
      auto parallel =
          std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1, projection);
      auto serial = std::make_shared<FullScan>(
          reader.txn_, *table, std::numeric_limits<size_t>::max(), false, true,
          std::nullopt, projection);
      TopNExecutor actual(parallel, schema, keys, 11, 3);
      TopNExecutor expected(serial, schema, keys, 11, 3);
      Row row, reference;
      RowPosition position, reference_position;
      while (expected.Next(&reference, &reference_position)) {
        ASSERT_TRUE(actual.Next(&row, &position));
        EXPECT_EQ(row, reference);
        EXPECT_EQ(position, reference_position);
      }
      EXPECT_FALSE(actual.Next(&row, &position));
      EXPECT_TRUE(actual.GetStatus().ok());
      EXPECT_TRUE(parallel->WasTypedFolded());
    }
  }
  struct RestoreBudget {  // NOLINT(cppcoreguidelines-special-member-functions)
    size_t limit = QueryMemoryBudget::Global().Limit();
    ~RestoreBudget() { QueryMemoryBudget::Global().ResetForTest(limit); }
  } restore;
  QueryMemoryBudget::Global().ResetForTest(128);
  auto parallel =
      std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1, projection);
  TopNExecutor fallback(
      parallel, schema,
      {{ColumnValueExp(ColumnName("id")), false, std::nullopt}}, 1, 0);
  Row row;
  ASSERT_TRUE(fallback.Next(&row, nullptr));
  EXPECT_EQ(row[1], Value(99));
  EXPECT_FALSE(parallel->WasTypedFolded());
  EXPECT_EQ(QueryMemoryBudget::Global().Used(), 0U);
}

TEST_F(ExecutorTest, TypedSerialFoldHonorsLimitsAndDeclinesAfterConsumption) {
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTableName));
  FullScan scan(reader.txn_, *table, 2, false, true, std::nullopt,
                std::vector<slot_t>{0});
  size_t count = 0;
  auto folded =
      scan.TryFoldTypedBatches([&](size_t, const TypedCellBatch& batch) {
        count += batch.rows;
        return Status::kSuccess;
      });
  ASSERT_TRUE(folded.HasValue());
  EXPECT_TRUE(folded.Value());
  EXPECT_EQ(count, 2U);
  Row row;
  EXPECT_FALSE(scan.Next(&row, nullptr));
  FullScan consumed(reader.txn_, *table, 3, false, true, std::nullopt,
                    std::vector<slot_t>{0});
  ASSERT_TRUE(consumed.Next(&row, nullptr));
  auto declined = consumed.TryFoldTypedBatches(
      [](size_t, const TypedCellBatch&) { return Status::kSuccess; });
  ASSERT_TRUE(declined.HasValue());
  EXPECT_FALSE(declined.Value());
  EXPECT_TRUE(consumed.Next(&row, nullptr));
  FullScan unsupported(reader.txn_, *table);
  auto no = unsupported.TryFoldTypedBatches(
      [](size_t, const TypedCellBatch&) { return Status::kSuccess; });
  ASSERT_TRUE(no.HasValue());
  EXPECT_FALSE(no.Value());
  EXPECT_TRUE(unsupported.Next(&row, nullptr));
}

TEST_F(ExecutorTest, FullScanExecutesLSTableFragmentOverPaxChunk) {
  TransactionContext reader = rs_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable(kTableName));
  FullScan scan(reader.txn_, *table);

  Fragment fragment;
  fragment.access = AccessMode::kFilterScan;
  Conjunct ge_one;
  ge_one.kind = ConjunctKind::kCompare;
  ge_one.col = 1;  // column 1 in lstable_schema is "key" (0 is "__rowid")
  ge_one.op = CompareOp::kGe;
  ge_one.literal_kind = LiteralKind::kInt64;
  ge_one.int64_literal = 1;
  fragment.conjuncts.push_back(ge_one);
  Aggregate sum_key;
  sum_key.kind = AggregateKind::kSum;
  sum_key.col = 1;
  fragment.aggregate = sum_key;

  StatusOr<ExecResult> exec_or =
      scan.ExecuteLSTableFragment(fragment, Runtime::kNative);
  ASSERT_TRUE(exec_or.HasValue());
  const ExecResult& res = exec_or.Value();
  EXPECT_FALSE(res.rejected);
  EXPECT_EQ(res.status, Status::kSuccess);
  EXPECT_EQ(res.scalar_count, 3U);
  EXPECT_EQ(res.scalar_value, 6);
}

TEST_F(ExecutorTest, AffineSumFoldsShareColumnsAndPreserveNullsAndOverflow) {
  const int64_t hi = std::numeric_limits<int64_t>::max();
  const std::vector<std::vector<Value>> cases{
      {Value(1), Value(), Value(4), Value(7)},
      {Value(-10), Value(-5), Value()},
      {Value(), Value()},
      {},
      {Value(hi - 1), Value(1)},
      {Value(hi)},
      {Value(hi), Value(1), Value(-1)}};
  for (size_t test = 0; test < cases.size(); ++test) {
    SCOPED_TRACE(test);
    const std::string name = "AffineSum" + std::to_string(test);
    const Schema schema(name, {Column("value", ValueType::kInt64)});
    std::vector<Row> reference;
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
      for (const auto& value : cases[test]) {
        reference.emplace_back(std::vector<Value>{value});
        ASSERT_SUCCESS(table.Insert(writer.txn_, reference.back()).GetStatus());
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    const std::vector<NamedExpression> aggregates{
        NamedExpression("base",
                        AggregateExpressionExp(AggregationType::kSum,
                                               ColumnValueExp("value"))),
        NamedExpression("plus",
                        AggregateExpressionExp(
                            AggregationType::kSum,
                            BinaryExpressionExp(ColumnValueExp("value"),
                                                BinaryOperation::kAdd,
                                                ConstantValueExp(Value(1))))),
        NamedExpression("minus",
                        AggregateExpressionExp(
                            AggregationType::kSum,
                            BinaryExpressionExp(ColumnValueExp("value"),
                                                BinaryOperation::kSubtract,
                                                ConstantValueExp(Value(-1)))))};
    WaitForChainFreePages();
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
    AggregationExecutor actual(scan, schema, aggregates);
    AggregationExecutor expected(std::make_shared<ConstantExecutor>(reference),
                                 schema, aggregates);
    Row row, wanted;
    EXPECT_EQ(actual.Next(&row, nullptr), expected.Next(&wanted, nullptr));
    EXPECT_EQ(actual.GetStatus(), expected.GetStatus());
    if (actual.GetStatus().ok()) {
      EXPECT_EQ(row, wanted);
    }
    if (test < 3) {
      EXPECT_TRUE(actual.AffineFolded());
    }
    if (test >= 5) {
      EXPECT_FALSE(actual.AffineFolded());
    }
    EXPECT_FALSE(actual.Next(&row, nullptr));
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(ExecutorTest, AffineSumRetainsSnapshotVisibility) {
  const Schema schema("AffineSnapshot", {Column("value", ValueType::kInt64)});
  RowPosition position;
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    ASSIGN_OR_ASSERT_FAIL(RowPosition, inserted,
                          table.Insert(writer.txn_, Row({Value(10)})));
    position = inserted;
    ASSERT_SUCCESS(writer.PreCommit());
  }
  auto old = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        old.GetTable("AffineSnapshot"));
  auto scan = std::make_shared<ParallelScan>(old.txn_, *table, 2, 1);
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(
        table->Update(writer.txn_, position, Row({Value(90)})).GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  AggregationExecutor aggregate(
      scan, schema,
      {NamedExpression("s",
                       AggregateExpressionExp(
                           AggregationType::kSum,
                           BinaryExpressionExp(ColumnValueExp("value"),
                                               BinaryOperation::kAdd,
                                               ConstantValueExp(Value(5)))))});
  Row row;
  ASSERT_TRUE(aggregate.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(15)}));
  EXPECT_FALSE(aggregate.AffineFolded());
  ASSERT_SUCCESS(aggregate.GetStatus());
  ASSERT_SUCCESS(old.PreCommit());
}

TEST_F(ExecutorTest, BlockTopNFiltersStringsAndKeepsStablePositions) {
  const Schema schema("StringTop", {Column("text", ValueType::kVarChar),
                                    Column("id", ValueType::kInt64),
                                    Column("payload", ValueType::kVarChar)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(
                  writer.txn_,
                  Row({i % 17 == 0 ? Value() : Value(i % 2 ? "alpha" : "Alpha"),
                       Value(i), Value(std::string(600, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("StringTop"));
  for (const auto op : {BinaryOperation::kNotEquals, BinaryOperation::kLike,
                        BinaryOperation::kNotLike}) {
    const std::vector<Expression> predicates{BinaryExpressionExp(
        ColumnValueExp("text"), op,
        ConstantValueExp(
            Value(op == BinaryOperation::kNotEquals ? "" : "%lph%")))};
    const auto filter =
        relational_detail::CompileScanFilter(predicates, schema);
    std::vector<Row> reference;
    std::vector<RowPosition> positions;
    FullScan original(reader.txn_, *table);
    Row row;
    RowPosition position;
    while (original.Next(&row, &position)) {
      if (relational_detail::MatchSimpleCompare(row, filter.simple.at(0))) {
        reference.push_back(row);
        positions.push_back(position);
      }
    }
    for (const bool ascending : {true, false}) {
      auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
      scan->SetRowFilter(
          predicates, filter,
          relational_detail::CompileWorkerFilter(filter, schema));
      const std::vector<TopNExecutor::Key> keys{{ColumnValueExp("text"),
                                                 ascending, false,
                                                 Collation::kCaseInsensitive}};
      TopNExecutor actual(scan, schema, keys, 9, 4);
      TopNExecutor expected(std::make_shared<ConstantExecutor>(reference),
                            schema, keys, 9, 4);
      Row wanted;
      while (expected.Next(&wanted, nullptr)) {
        ASSERT_TRUE(actual.Next(&row, &position));
        EXPECT_EQ(row, wanted);
        const auto found = std::ranges::find(reference, row);
        ASSERT_NE(found, reference.end());
        EXPECT_EQ(position,
                  positions[static_cast<size_t>(found - reference.begin())]);
      }
      EXPECT_FALSE(actual.Next(&row, nullptr));
      ASSERT_SUCCESS(actual.GetStatus());
      EXPECT_TRUE(scan->WasTypedFolded());
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest,
       ProjectedTopNLateMaterializesPayloadAndKeepsStablePositions) {
  const Schema schema("StringTop", {Column("text", ValueType::kVarChar),
                                    Column("id", ValueType::kInt64),
                                    Column("payload", ValueType::kVarChar)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(
                  writer.txn_,
                  Row({i % 17 == 0 ? Value() : Value(i % 2 ? "alpha" : "Alpha"),
                       Value(i), Value(std::string(600, 'x'))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("StringTop"));
  for (const auto op : {BinaryOperation::kNotEquals, BinaryOperation::kLike,
                        BinaryOperation::kNotLike}) {
    const std::vector<Expression> predicates{BinaryExpressionExp(
        ColumnValueExp("text"), op,
        ConstantValueExp(
            Value(op == BinaryOperation::kNotEquals ? "" : "%lph%")))};
    const auto filter =
        relational_detail::CompileScanFilter(predicates, schema);
    std::vector<Row> reference;
    std::vector<RowPosition> positions;
    FullScan original(reader.txn_, *table);
    Row row;
    RowPosition position;
    while (original.Next(&row, &position)) {
      if (relational_detail::MatchSimpleCompare(row, filter.simple.at(0))) {
        reference.push_back(row);
        positions.push_back(position);
      }
    }
    for (const bool ascending : {true, false}) {
      table->SetLSTableStore(nullptr);
      table->SetProjectedLSTableStore(nullptr);
      table->RejectLSTableCapacity(table->LayoutGeneration());
      auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
      scan->SetRowFilter(
          predicates, filter,
          relational_detail::CompileWorkerFilter(filter, schema));
      const std::vector<TopNExecutor::Key> keys{{ColumnValueExp("text"),
                                                 ascending, false,
                                                 Collation::kCaseInsensitive}};
      TopNExecutor actual(scan, schema, keys, 9, 4);
      TopNExecutor expected(std::make_shared<ConstantExecutor>(reference),
                            schema, keys, 9, 4);
      Row wanted;
      while (expected.Next(&wanted, nullptr)) {
        ASSERT_TRUE(actual.Next(&row, &position));
        EXPECT_EQ(row, wanted);
        const auto found = std::ranges::find(reference, row);
        ASSERT_NE(found, reference.end());
        EXPECT_EQ(position,
                  positions[static_cast<size_t>(found - reference.begin())]);
      }
      EXPECT_FALSE(actual.Next(&row, nullptr));
      ASSERT_SUCCESS(actual.GetStatus());
      EXPECT_TRUE(scan->WasTypedFolded());
      ASSERT_NE(scan->GetLSTableStore(), nullptr);
      EXPECT_TRUE(scan->GetLSTableStore()->HasColumn(0));
      EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(2));
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, DictionaryTopNUsesProvenOrderAndPreservesSecondaryTies) {
  for (bool enums : {false, true}) {
    const std::string name = enums ? "EnumOrderedTop" : "DictionaryOrderedTop";
    const Schema schema(name, {Column("s", ValueType::kVarChar),
                               Column("id", ValueType::kInt64),
                               Column("padding", ValueType::kVarChar),
                               Column("g", ValueType::kInt64)});
    std::vector<Row> reference;
    {
      auto writer = rs_->BeginContext();
      ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
      for (int64_t i = 0; i < 8192; ++i) {
        std::string text = "key" + std::to_string(i / 16);
        if (enums && i < 32) {
          text = i < 16 ? "TESTENUMNEGATIVE" : "TESTENUM0";
        }
        Row row({i == 8191 ? Value() : Value(std::move(text)), Value(i),
                 Value(std::string(512, 'x')),
                 i == 8191 ? Value() : Value(i / 16)});
        ASSERT_SUCCESS(table.Insert(writer.txn_, row).GetStatus());
        reference.push_back(std::move(row));
      }
      ASSERT_SUCCESS(writer.PreCommit());
    }
    WaitForChainFreePages();
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table, reader.GetTable(name));
    for (bool ascending : {false, true}) {
      for (bool collated : {false, true}) {
        auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1);
        const std::vector<TopNExecutor::Key> keys{
            {ColumnValueExp("s"), ascending, true,
             collated ? Collation::kCaseInsensitive : Collation::kNone},
            {ColumnValueExp("id"), false, false}};
        TopNExecutor actual(scan, schema, keys, 9, 4);
        TopNExecutor expected(std::make_shared<ConstantExecutor>(reference),
                              schema, keys, 9, 4);
        Row wanted, row;
        while (expected.Next(&wanted, nullptr)) {
          ASSERT_TRUE(actual.Next(&row, nullptr));
          EXPECT_EQ(row, wanted);
        }
        EXPECT_FALSE(actual.Next(&row, nullptr));
        ASSERT_SUCCESS(actual.GetStatus());
        EXPECT_TRUE(scan->WasTypedFolded());
        ASSERT_NE(scan->GetLSTableStore(), nullptr);
        const auto& dictionary = scan->GetLSTableStore()->dictionaries[0];
        EXPECT_NE(dictionary.wide_code_bytes, 0);
        EXPECT_EQ(dictionary.codes_follow_string_order, !enums);
        ASSERT_EQ(dictionary.code_counts.size(), dictionary.entries.size() + 1);
        EXPECT_EQ(std::accumulate(dictionary.code_counts.begin(),
                                  dictionary.code_counts.end(), int64_t{0}),
                  8192);
        EXPECT_EQ(dictionary.code_counts.back(), 1);
        EXPECT_EQ(dictionary.code_first_row.back(), 8191U);
        ASSERT_FALSE(dictionary.encounter_codes.empty());
        EXPECT_EQ(dictionary.code_first_row[dictionary.encounter_codes.front()],
                  0U);
      }
    }
    for (const auto operation :
         {BinaryOperation::kLike, BinaryOperation::kNotLike,
          BinaryOperation::kEquals, BinaryOperation::kNotEquals}) {
      for (const bool reject : {false, true}) {
        std::vector<Expression> predicates{BinaryExpressionExp(
            ColumnValueExp("s"), operation, ConstantValueExp(Value("%key1%")))};
        if (reject) {
          predicates.push_back(ConstantValueExp(Value(0)));
        }
        const auto filter =
            relational_detail::CompileScanFilter(predicates, schema);
        auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 4, 1);
        scan->SetRowFilter(
            predicates, filter,
            relational_detail::CompileWorkerFilter(filter, schema));
        AggregationExecutor actual(
            scan, schema,
            {NamedExpression("n",
                             AggregateExpressionExp(AggregationType::kCount,
                                                    ColumnValueExp("*")))});
        int64_t count = 0;
        if (!reject) {
          for (const auto& row : reference) {
            count += static_cast<int64_t>(relational_detail::MatchSimpleCompare(
                row, filter.simple.front()));
          }
        }
        Row row;
        ASSERT_TRUE(actual.Next(&row, nullptr));
        EXPECT_EQ(row, Row({Value(count)}));
        EXPECT_FALSE(actual.Next(&row, nullptr));
        ASSERT_SUCCESS(actual.GetStatus());
      }
    }
    const auto old_store = table->GetLSTableStore();
    ASSERT_NE(old_store, nullptr);
    const auto count_distinct = [&](TransactionContext& context,
                                    int64_t integer_count) {
      for (const std::string column : {"s", "g"}) {
        for (bool filtered : {false, true}) {
          auto scan =
              std::make_shared<ParallelScan>(context.txn_, *table, 4, 1);
          if (filtered) {
            std::vector<Expression> predicates{BinaryExpressionExp(
                ColumnValueExp("id"), BinaryOperation::kLessThan,
                ConstantValueExp(Value(64)))};
            const auto filter =
                relational_detail::CompileScanFilter(predicates, schema);
            scan->SetRowFilter(
                predicates, filter,
                relational_detail::CompileWorkerFilter(filter, schema));
          }
          AggregationExecutor count(
              scan, schema,
              {NamedExpression(
                  "n", AggregateExpressionExp(AggregationType::kCount,
                                              ColumnValueExp(column), true))});
          Row row;
          ASSERT_TRUE(count.Next(&row, nullptr));
          EXPECT_EQ(row, Row({Value(filtered        ? 4
                                    : column == "s" ? 512
                                                    : integer_count)}));
          EXPECT_FALSE(count.Next(&row, nullptr));
          ASSERT_SUCCESS(count.GetStatus());
        }
      }
    };
    const auto count_key = [&](TransactionContext& context, int64_t expected,
                               const LSTableStore* expected_store = nullptr) {
      std::vector<Expression> predicates{
          BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kEquals,
                              ConstantValueExp(Value("key100")))};
      const auto filter =
          relational_detail::CompileScanFilter(predicates, schema);
      auto scan = std::make_shared<ParallelScan>(context.txn_, *table, 4, 1);
      scan->SetRowFilter(
          predicates, filter,
          relational_detail::CompileWorkerFilter(filter, schema));
      AggregationExecutor count(
          scan, schema,
          {NamedExpression("n", AggregateExpressionExp(AggregationType::kCount,
                                                       ColumnValueExp("*")))});
      Row row;
      ASSERT_TRUE(count.Next(&row, nullptr));
      EXPECT_EQ(row, Row({Value(expected)}));
      EXPECT_FALSE(count.Next(&row, nullptr));
      ASSERT_SUCCESS(count.GetStatus());
      if (expected_store != nullptr) {
        EXPECT_EQ(scan->GetLSTableStore(), expected_store);
      }
    };
    count_key(reader, 16);
    count_distinct(reader, 512);
    {
      auto writer = rs_->BeginContext();
      ASSERT_SUCCESS(
          table
              ->Insert(writer.txn_,
                       Row({Value("key100"), Value(8192),
                            Value(std::string(512, 'x')), Value(513)}))
              .GetStatus());
      ASSERT_SUCCESS(writer.PreCommit());
    }
    // A prepared frequency summary cannot expose a later committed append
    // through the old snapshot. A fresh snapshot must observe that append.
    count_key(reader, 16);
    count_distinct(reader, 512);
    ASSERT_SUCCESS(reader.PreCommit());
    WaitForChainFreePages();
    auto fresh = rs_->BeginReadOnlyContext();
    count_key(fresh, 17);  // A stale full store falls back until rebuilt.
    count_distinct(fresh, 513);
    const auto new_store = BuildLSTableStore(fresh.txn_, *table);
    ASSERT_NE(new_store, nullptr);
    count_key(fresh, 17, new_store.get());
    count_distinct(fresh, 513);
    EXPECT_NE(new_store, old_store);
    EXPECT_EQ(std::accumulate(old_store->dictionaries[0].code_counts.begin(),
                              old_store->dictionaries[0].code_counts.end(),
                              int64_t{0}),
              8192);
    EXPECT_EQ(std::accumulate(new_store->dictionaries[0].code_counts.begin(),
                              new_store->dictionaries[0].code_counts.end(),
                              int64_t{0}),
              8193);
    ASSERT_SUCCESS(fresh.PreCommit());
  }
}

TEST_F(ExecutorTest, BlockDistinctUsesCanonicalStringEqualityAndNulls) {
  const Schema schema("BlockDistinct", {Column("s", ValueType::kVarChar),
                                        Column("n", ValueType::kInt64)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    const std::vector<Value> strings{Value(),
                                     Value(""),
                                     Value("A"),
                                     Value("a"),
                                     Value("4-th avenue"),
                                     Value("4-rd avenue"),
                                     Value("0-12 0 0:0:0"),
                                     Value("1-0 0 0:0:0"),
                                     Value("4-th avenue")};
    for (const auto& text : strings) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({text, Value(std::numeric_limits<int64_t>::min())}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("BlockDistinct"));
  for (const std::string column : {"s", "n"}) {
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
    AggregationExecutor aggregate(
        scan, schema,
        {NamedExpression(
            "d", AggregateExpressionExp(AggregationType::kCount,
                                        ColumnValueExp(column), true))});
    Row row;
    ASSERT_TRUE(aggregate.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(column == "s" ? 7 : 1)}));
    EXPECT_TRUE(scan->WasTypedFolded());
    ASSERT_SUCCESS(aggregate.GetStatus());
  }
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  Expression collated = FunctionCallExp(
      "collate", {ColumnValueExp("s"), ConstantValueExp(Value("und:ci"))});
  AggregationExecutor aggregate(
      scan, schema,
      {NamedExpression("d",
                       AggregateExpressionExp(AggregationType::kCount,
                                              std::move(collated), true))});
  Row row;
  ASSERT_TRUE(aggregate.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(6)}));
  EXPECT_FALSE(scan->WasTypedFolded());
  ASSERT_SUCCESS(aggregate.GetStatus());
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, BlockDateExtremaKeepDateTypeAndNullableBoundaries) {
  const Schema schema("DateExtrema", {Column("d", ValueType::kDate)});
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (const Value& date :
         {Value(), Value::Date("1969-12-31"), Value::Date("2024-02-29")}) {
      ASSERT_SUCCESS(table.Insert(writer.txn_, Row({date})).GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("DateExtrema"));
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  AggregationExecutor aggregate(
      scan, schema,
      {NamedExpression("lo", AggregateExpressionExp(AggregationType::kMin,
                                                    ColumnValueExp("d"))),
       NamedExpression("hi", AggregateExpressionExp(AggregationType::kMax,
                                                    ColumnValueExp("d")))});
  Row row;
  ASSERT_TRUE(aggregate.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value::Date("1969-12-31"), Value::Date("2024-02-29")}));
  EXPECT_EQ(row[0].type, ValueType::kDate);
  EXPECT_EQ(row[1].type, ValueType::kDate);
  EXPECT_TRUE(scan->WasTypedFolded());
  ASSERT_SUCCESS(aggregate.GetStatus());
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, WholeColumnStoreFoldKeepsScanGuardsAndErrors) {
  const Schema schema("WholeStore", {Column("n", ValueType::kInt64)});
  RowPosition first;
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t n = 0; n < 400; ++n) {
      ASSIGN_OR_ASSERT_FAIL(RowPosition, position,
                            table.Insert(writer.txn_, Row({Value(n)})));
      if (n == 0) {
        first = position;
      }
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("WholeStore"));
  size_t calls = 0;
  const auto consume = [&](const LSTableStore& store) {
    ++calls;
    EXPECT_EQ(store.total_rows, 400U);
    return Status::kSuccess;
  };
  {
    ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{0});
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_TRUE(folded);
    EXPECT_TRUE(scan.WasTypedFolded());
    EXPECT_EQ(calls, 1U);
    ASSIGN_OR_ASSERT_FAIL(bool, repeated, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(repeated);
    Row row;
    EXPECT_FALSE(scan.Next(&row, nullptr));
    EXPECT_FALSE(scan.Next(&row, nullptr));
    ASSERT_SUCCESS(scan.GetStatus());
  }
  for (const bool throws : {false, true}) {
    ParallelScan scan(reader.txn_, *table, 2, 1);
    auto result = scan.TryFoldLSTableStore([&](const LSTableStore&) -> Status {
      if (throws) {
        throw std::runtime_error("fold callback error");
      }
      return Status::kCorrupt;
    });
    EXPECT_FALSE(result.HasValue());
    EXPECT_NE(scan.GetStatus(), Status::kSuccess);
    Row row;
    EXPECT_FALSE(scan.Next(&row, nullptr));
    EXPECT_FALSE(scan.Next(&row, nullptr));
  }
  for (const bool attached : {false, true}) {
    ParallelScan scan(reader.txn_, *table, 2, 1, std::nullopt,
                      attached ? std::vector<IntegerPeekCompare>{}
                               : std::vector<IntegerPeekCompare>{
                                     {.column = 0,
                                      .op = BinaryOperation::kEquals,
                                      .constant = 1}});
    if (attached) {
      const std::vector<Expression> predicates{
          BinaryExpressionExp(ColumnValueExp("n"), BinaryOperation::kEquals,
                              ConstantValueExp(Value(1)))};
      const auto filter =
          relational_detail::CompileScanFilter(predicates, schema);
      scan.SetRowFilter(predicates, filter,
                        relational_detail::CompileWorkerFilter(filter, schema));
    }
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(folded);
    Row row;
    ASSERT_TRUE(scan.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(1)}));
    EXPECT_FALSE(scan.Next(&row, nullptr));
    ASSERT_SUCCESS(scan.GetStatus());
  }
  {
    ParallelScan scan(reader.txn_, *table, 2, 1);
    Row row;
    ASSERT_TRUE(scan.Next(&row, nullptr));
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(folded);
  }
  {
    ParallelScan scan(reader.txn_, *table, 2, 1, std::vector<slot_t>{1});
    ASSIGN_OR_ASSERT_FAIL(bool, folded, scan.TryFoldLSTableStore(consume));
    EXPECT_FALSE(folded);
  }
  // A store obtained before a concurrent update must not bypass the old
  // reader's version-chain traversal, even though it is still owned here.
  ParallelScan old_scan(reader.txn_, *table, 2, 1);
  ASSERT_NE(old_scan.GetLSTableStore(), nullptr);
  {
    auto writer = rs_->BeginContext();
    ASSERT_SUCCESS(
        table->Update(writer.txn_, first, Row({Value(999)})).GetStatus());
    ASSERT_SUCCESS(writer.PreCommit());
  }
  ASSIGN_OR_ASSERT_FAIL(bool, old_folded,
                        old_scan.TryFoldLSTableStore(consume));
  EXPECT_FALSE(old_folded);
  int64_t sum = 0;
  Row row;
  while (old_scan.Next(&row, nullptr)) {
    sum += row[0].value.int_value;
  }
  EXPECT_EQ(sum, 399 * 400 / 2);
  ASSERT_SUCCESS(old_scan.GetStatus());
  EXPECT_EQ(calls, 1U);
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, WideDictionaryDistinctAndMembershipUseVisibleRows) {
  const Schema schema("EncodedDistinct", {Column("s", ValueType::kVarChar),
                                          Column("n", ValueType::kInt64)});
  std::vector<Value> texts{Value(), Value(""), Value("0-12 0 0:0:0"),
                           Value("1-0 0 0:0:0")};
  for (size_t i = 0; i < 300; ++i) {
    std::string text(80, 'x');
    text += std::to_string(i);
    texts.emplace_back(std::move(text));
  }
  constexpr int64_t kRows = 12280;
  {
    auto writer = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, table, rs_->CreateTable(writer, schema));
    for (int64_t i = 0; i < kRows; ++i) {
      ASSERT_SUCCESS(
          table
              .Insert(writer.txn_,
                      Row({texts[static_cast<size_t>(i) % texts.size()],
                           Value(i * 3)}))
              .GetStatus());
    }
    ASSERT_SUCCESS(writer.PreCommit());
  }
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("EncodedDistinct"));
  for (const int64_t bound : {int64_t{0}, int64_t{30}, kRows * 3}) {
    std::unordered_set<Value> expected;
    for (int64_t i = 0; i * 3 < bound; ++i) {
      const auto& text = texts[static_cast<size_t>(i) % texts.size()];
      if (!text.IsNull()) {
        expected.insert(text);
      }
    }
    auto scan = std::make_shared<ParallelScan>(
        reader.txn_, *table, 2, 1, std::nullopt,
        std::vector<IntegerPeekCompare>{{.column = 1,
                                         .op = BinaryOperation::kLessThan,
                                         .constant = bound}});
    ASSERT_NE(scan->GetLSTableStore(), nullptr);
    EXPECT_EQ(scan->GetLSTableStore()->dictionaries[0].wide_code_bytes, 2);
    EXPECT_TRUE(scan->GetLSTableStore()->dictionaries[0].bytewise_equality);
    AggregationExecutor aggregate(
        scan, schema,
        {NamedExpression("d",
                         AggregateExpressionExp(AggregationType::kCount,
                                                ColumnValueExp("s"), true))});
    Row row;
    ASSERT_TRUE(aggregate.Next(&row, nullptr));
    EXPECT_EQ(row, Row({Value(static_cast<int64_t>(expected.size()))}));
    EXPECT_TRUE(scan->WasTypedFolded());
    ASSERT_SUCCESS(aggregate.GetStatus());
  }
  const auto store = table->GetLSTableStore();
  ASSERT_NE(store, nullptr);
  int64_t absent = 1;
  while (store->MayContainInteger(1, absent) && absent < kRows * 3) {
    absent += 3;
  }
  ASSERT_LT(absent, kRows * 3);
  for (const bool attached : {false, true}) {
    for (const int64_t value : {absent, int64_t{0}, (kRows - 1) * 3}) {
      const auto make_scan = [&] {
        if (attached) {
          auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
          const std::vector<Expression> predicates{
              BinaryExpressionExp(ColumnValueExp("n"), BinaryOperation::kEquals,
                                  ConstantValueExp(Value(value)))};
          const auto filter =
              relational_detail::CompileScanFilter(predicates, schema);
          scan->SetRowFilter(
              predicates, filter,
              relational_detail::CompileWorkerFilter(filter, schema));
          return scan;
        }
        return std::make_shared<ParallelScan>(
            reader.txn_, *table, 2, 1, std::nullopt,
            std::vector<IntegerPeekCompare>{{.column = 1,
                                             .op = BinaryOperation::kEquals,
                                             .constant = value}});
      };
      auto scan = make_scan();
      Row row;
      if (value != absent) {
        ASSERT_TRUE(scan->Next(&row, nullptr));
        EXPECT_EQ(row[1], Value(value));
      }
      EXPECT_FALSE(scan->Next(&row, nullptr));
      EXPECT_FALSE(scan->Next(&row, nullptr));
      ASSERT_SUCCESS(scan->GetStatus());
      std::atomic<size_t> rows{0};
      auto folded = make_scan()->TryFoldLSTableBlocks(
          2,
          [&](size_t, const LSTablePageBlock&, const uint32_t*, size_t count) {
            rows += count;
            return Status::kSuccess;
          });
      ASSERT_TRUE(folded.HasValue());
      EXPECT_TRUE(folded.Value());
      EXPECT_EQ(rows.load(), value == absent ? 0 : 1);
    }
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, TypedGroupingHandlesConstantsEmptyInputAndAverageCasts) {
  RelationalRun(*rs_, "CREATE TABLE ConstantGroups (k STRING, n INT64)");
  RelationalRun(*rs_,
                "INSERT INTO ConstantGroups VALUES ('a',1), ('a',3), "
                "('b',NULL), ('b',7)");
  const auto rows =
      RelationalRun(*rs_,
                    "SELECT 1, k, AVG(CAST(n AS FLOAT64)), COUNT(*) "
                    "FROM ConstantGroups GROUP BY 1,k ORDER BY k");
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value("a"), Value(2.0), Value(2)}));
  EXPECT_EQ(rows[1], Row({Value(1), Value("b"), Value(7.0), Value(2)}));
  EXPECT_TRUE(
      RelationalRun(*rs_,
                    "SELECT 'constant', COUNT(*) FROM ConstantGroups "
                    "WHERE n > 100 GROUP BY 'constant' ORDER BY COUNT(*)")
          .empty());
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT AVG(CAST(n AS FLOAT64)) FROM ConstantGroups"),
      std::vector<Row>{Row({Value(11.0 / 3.0)})});
}

TEST_F(ExecutorTest, ProjectedParallelFoldConvertsExceptionsToStickyStatus) {
  RelationalRun(*rs_, "CREATE TABLE FoldFailure(unused STRING, k INT64)");
  RelationalRun(*rs_,
                "INSERT INTO FoldFailure VALUES ('unused',1),('unused',2)");
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("FoldFailure"));
  table->SetLSTableStore(nullptr);
  table->SetProjectedLSTableStore(nullptr);
  table->RejectLSTableCapacity(table->LayoutGeneration());
  const std::vector<slot_t> needed{1};
  for (const bool allocation_failure : {true, false}) {
    auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
    EXPECT_EQ(scan->GetLSTableStore(), nullptr);
    StatusOr<bool> folded(false);
    EXPECT_NO_THROW(
        folded = scan->TryFoldLSTableBlocks(
            2,
            [&](size_t /*worker*/, const LSTablePageBlock& /*block*/,
                const uint32_t* /*rows*/, size_t /*count*/) -> Status {
              if (allocation_failure) {
                throw std::bad_alloc();
              }
              throw std::runtime_error("injected fold failure");
            },
            &needed));
    const Status expected =
        allocation_failure ? Status::kNoSpace : Status::kRuntimeError;
    ASSERT_FALSE(folded.HasValue());
    EXPECT_EQ(folded.GetStatus(), expected);
    EXPECT_EQ(scan->GetStatus(), expected);
    ASSERT_NE(scan->GetLSTableStore(), nullptr);
    EXPECT_TRUE(scan->GetLSTableStore()->HasColumn(1));
    EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(0));
    DataChunk output;
    EXPECT_EQ(scan->NextBatch(&output), 0U);
    EXPECT_EQ(scan->GetStatus(), expected);
  }
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, ProjectedTopNRestoresReorderedOutputColumns) {
  RelationalRun(
      *rs_,
      "CREATE TABLE TopProjection(k INT64, unused STRING, payload STRING)");
  RelationalRun(*rs_,
                "INSERT INTO TopProjection VALUES "
                "(2,'x','two'),(1,'y','one'),(NULL,'z','nil')");
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("TopProjection"));
  table->SetLSTableStore(nullptr);
  table->SetProjectedLSTableStore(nullptr);
  table->RejectLSTableCapacity(table->LayoutGeneration());
  const Schema output("TopProjection", {table->GetSchema().GetColumn(2),
                                        table->GetSchema().GetColumn(0)});
  // Seed a narrower cached projection to exercise extending required columns.
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  ASSIGN_OR_ASSERT_FAIL(bool, prepared, scan->PrepareLSTableColumns({0}));
  ASSERT_TRUE(prepared);
  auto projected = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1,
                                                  std::vector<slot_t>{2, 0});
  ASSIGN_OR_ASSERT_FAIL(bool, replaced, projected->PrepareLSTableColumns({1}));
  ASSERT_TRUE(replaced);
  TopNExecutor top(projected, output, {{ColumnValueExp("k"), true, false}}, 2,
                   0);
  Row row;
  ASSERT_TRUE(top.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value("one"), Value(1)}));
  ASSERT_TRUE(top.Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value("two"), Value(2)}));
  EXPECT_FALSE(top.Next(&row, nullptr));
  ASSERT_SUCCESS(top.GetStatus());
  EXPECT_TRUE(projected->WasTypedFolded());
  ASSERT_NE(projected->GetLSTableStore(), nullptr);
  EXPECT_TRUE(projected->GetLSTableStore()->HasColumn(0));
  EXPECT_FALSE(projected->GetLSTableStore()->HasColumn(2));
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, PreparedNarrowFoldCanFallBackToFullRows) {
  RelationalRun(*rs_, "CREATE TABLE NarrowFallback(k INT64, payload STRING)");
  RelationalRun(*rs_, "INSERT INTO NarrowFallback VALUES (2,'two'),(1,'one')");
  WaitForChainFreePages();
  auto reader = rs_->BeginReadOnlyContext();
  ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                        reader.GetTable("NarrowFallback"));
  table->SetLSTableStore(nullptr);
  table->SetProjectedLSTableStore(nullptr);
  table->RejectLSTableCapacity(table->LayoutGeneration());
  auto scan = std::make_shared<ParallelScan>(reader.txn_, *table, 2, 1);
  ASSIGN_OR_ASSERT_FAIL(bool, prepared, scan->PrepareLSTableColumns({0}));
  ASSERT_TRUE(prepared);
  ASSERT_NE(scan->GetLSTableStore(), nullptr);
  EXPECT_FALSE(scan->GetLSTableStore()->HasColumn(1));
  Row row;
  ASSERT_TRUE(scan->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(2), Value("two")}));
  ASSERT_TRUE(scan->Next(&row, nullptr));
  EXPECT_EQ(row, Row({Value(1), Value("one")}));
  EXPECT_FALSE(scan->Next(&row, nullptr));
  ASSERT_SUCCESS(scan->GetStatus());
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(ExecutorTest, NarrowEqualitySynopsisKeepsMatchingPayloadAndFreshWrites) {
  const Schema schema(
      "EqualityPayload",
      {Column("k", ValueType::kInt64), Column("payload", ValueType::kVarChar)});
  RelationalRun(*rs_, "CREATE TABLE EqualityPayload(k INT64,payload STRING)");
  RelationalRun(*rs_, "INSERT INTO EqualityPayload VALUES (1,'one'),(2,'two')");
  WaitForChainFreePages();
  for (const int64_t wanted : {int64_t{1}, int64_t{333}}) {
    auto reader = rs_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          reader.GetTable("EqualityPayload"));
    table->SetLSTableStore(nullptr);
    table->SetProjectedLSTableStore(nullptr);
    table->RejectLSTableCapacity(table->LayoutGeneration());
    ParallelScan scan(reader.txn_, *table, 2, 1);
    const std::vector<Expression> predicates{
        BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                            ConstantValueExp(Value(wanted)))};
    auto filter = relational_detail::CompileScanFilter(predicates, schema);
    scan.SetRowFilter(predicates, filter,
                      relational_detail::CompileWorkerFilter(filter, schema));
    ASSERT_NE(scan.GetLSTableStore(), nullptr);
    EXPECT_FALSE(scan.GetLSTableStore()->HasColumn(1));
    Row row;
    EXPECT_EQ(scan.Next(&row, nullptr), wanted == 1);
    if (wanted == 1) {
      EXPECT_EQ(row, Row({Value(1), Value("one")}));
      EXPECT_FALSE(scan.Next(&row, nullptr));
    }
    ASSERT_SUCCESS(scan.GetStatus());
    ASSERT_SUCCESS(reader.PreCommit());
  }
  // A membership rejection from the previous generation cannot hide a new
  // matching row, whether its MVCC chain is still live or already retired.
  RelationalRun(*rs_, "INSERT INTO EqualityPayload VALUES (333,'new')");
  EXPECT_EQ(
      RelationalRun(*rs_, "SELECT payload FROM EqualityPayload WHERE k=333"),
      std::vector<Row>{Row({Value("new")})});
}

TEST_F(ExecutorTest,
       RelationalHybridHashJoinRepartitionsAndHandlesSingleKeySkewOuter) {
  // Part 1: multi-level salted repartitioning when distinct keys exceed
  // partition_budget_bytes() under a 16 KiB limit.
  CreateWideTable(*rs_, "RepartL", 400);
  CreateWideTable(*rs_, "RepartR", 400);
  const std::string inner_repart_sql =
      "SELECT a.key, b.key FROM RepartL AS a JOIN RepartR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM RepartL);";
  std::vector<std::pair<int64_t, int64_t>> repart_ref;
  {
    ScopedQueryMemory unlimited(0);
    repart_ref = KeyPairCounts(RelationalRun(*rs_, inner_repart_sql));
  }
  ASSERT_EQ(repart_ref.size(), 400U);
  {
    ScopedQueryMemory memory(16384);
    EXPECT_EQ(KeyPairCounts(RelationalRun(*rs_, inner_repart_sql)), repart_ref);
  }

  // Part 2: all rows share key=7 on both sides plus a few unmatched rows so
  // salted repartitioning hits kMaxHybridDepth and falls back to chunked skew
  // probing across INNER, LEFT, RIGHT, and FULL OUTER joins.
  auto seed_skew_table = [&](std::string_view name, int64_t unmatched_base) {
    TransactionContext ctx = rs_->BeginContext();
    Schema schema{std::string(name),
                  {Column("key", ValueType::kInt64),
                   Column("payload", ValueType::kVarChar)}};
    StatusOr<Table> created = rs_->CreateTable(ctx, schema);
    ASSERT_SUCCESS(created.GetStatus());
    std::string wide_payload(192, 's');
    for (int i = 0; i < 60; ++i) {
      ASSERT_SUCCESS(
          created.Value()
              .Insert(ctx.txn_,
                      Row(std::vector{Value(int64_t{7}),
                                      Value(wide_payload + std::to_string(i))}))
              .GetStatus());
    }
    for (int i = 0; i < 4; ++i) {
      ASSERT_SUCCESS(
          created.Value()
              .Insert(ctx.txn_,
                      Row(std::vector{Value(unmatched_base + i),
                                      Value(std::string(wide_payload))}))
              .GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  };
  seed_skew_table("SkewSingleL", 900);
  seed_skew_table("SkewSingleR", 800);

  const std::vector<std::string> skew_sqls{
      "SELECT a.key, b.key FROM SkewSingleL AS a JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);",
      "SELECT a.key, b.key FROM SkewSingleL AS a LEFT JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);",
      "SELECT a.key, b.key FROM SkewSingleL AS a RIGHT JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);",
      "SELECT a.key, b.key FROM SkewSingleL AS a FULL JOIN SkewSingleR AS b "
      "ON a.key = b.key WHERE EXISTS (SELECT 1 FROM SkewSingleL);"};
  for (const std::string& sql : skew_sqls) {
    std::vector<std::pair<int64_t, int64_t>> ref;
    {
      ScopedQueryMemory unlimited(0);
      ref = KeyPairCounts(RelationalRun(*rs_, sql));
    }
    ASSERT_GE(ref.size(), 3600U) << sql;
    {
      ScopedQueryMemory memory(16384);
      EXPECT_EQ(KeyPairCounts(RelationalRun(*rs_, sql)), ref) << sql;
    }
  }
}

TEST_F(ExecutorTest, RelationalPartitionAggRecursiveSplitUnderTightBudget) {
  CreateWideTable(*rs_, "WideRecAgg", 400);
  const std::string sql =
      "SELECT key, COUNT(*), SUM(LENGTH(name)) FROM WideRecAgg "
      "GROUP BY key HAVING COUNT(*) > 0 ORDER BY key;";
  std::vector<Row> reference;
  {
    ScopedQueryMemory unlimited(0);
    reference = RelationalRun(*rs_, sql);
  }
  ASSERT_EQ(reference.size(), 400U);
  {
    ScopedQueryMemory memory(32768);
    const std::vector<Row> spilled = RelationalRun(*rs_, sql);
    EXPECT_EQ(spilled, reference);
  }
}

TEST_F(ExecutorTest, RelationalOrderByExternalMergeAndApplyOrderByUnderBudget) {
  CreateWideTable(*rs_, "WideOrderMerge", 500);
  ScopedQueryMemory memory(32768);

  // Full external merge sort via Project's ExternalKeyedSorter.
  const auto full_desc = RelationalRun(
      *rs_,
      "SELECT key, name FROM WideOrderMerge "
      "WHERE EXISTS (SELECT 1 FROM WideOrderMerge) ORDER BY key DESC;");
  ASSERT_EQ(full_desc.size(), 500U);
  EXPECT_EQ(full_desc.front()[0], Value(int64_t{499}));
  EXPECT_EQ(full_desc.back()[0], Value(int64_t{0}));
  for (size_t i = 0; i < full_desc.size(); ++i) {
    EXPECT_EQ(full_desc[i][0], Value(static_cast<int64_t>(499 - i)));
  }

  // Bounded Top-K heap path with OFFSET.
  const auto topk = RelationalRun(*rs_,
                                  "SELECT key FROM WideOrderMerge "
                                  "WHERE EXISTS (SELECT 1 FROM WideOrderMerge) "
                                  "ORDER BY key DESC LIMIT 5 OFFSET 2;");
  ASSERT_EQ(topk.size(), 5U);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(topk[i][0], Value(static_cast<int64_t>(497 - i)));
  }

  // Set-operation + ORDER BY routes directly through ApplyOrderBy. Setting a
  // large LIMIT that fails CanReserve(5000 * 160) under 32 KiB exercises the
  // non-heap external merge sort path inside ApplyOrderBy.
  const auto apply_order =
      RelationalRun(*rs_,
                    "SELECT key, name FROM WideOrderMerge "
                    "UNION ALL "
                    "SELECT key + 1000, name FROM WideOrderMerge "
                    "ORDER BY key DESC LIMIT 5000;");
  ASSERT_EQ(apply_order.size(), 1000U);
  EXPECT_EQ(apply_order.front()[0], Value(int64_t{1499}));
  EXPECT_EQ(apply_order[499][0], Value(int64_t{1000}));
  EXPECT_EQ(apply_order[500][0], Value(int64_t{499}));
  EXPECT_EQ(apply_order.back()[0], Value(int64_t{0}));
}

TEST_F(ExecutorTest, RelationalAndGroupedFinishNextBatchStreamSpilledRelation) {
  CreateWideTable(*rs_, "WideBatchStream", 300);
  auto run_batched = [&](std::string_view sql, size_t batch_size,
                         size_t min_batches) -> std::vector<Row> {
    TransactionContext context = rs_->BeginContext();
    SqlEngine engine(*rs_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    std::vector<Row> rows;
    if (!prepared.HasValue()) {
      ADD_FAILURE() << sql << "\n" << engine.LastError();
      context.Abort();
      return rows;
    }
    DataChunk chunk;
    size_t batches = 0;
    while (prepared.Value()->NextBatch(&chunk, batch_size) > 0) {
      ++batches;
      EXPECT_LE(chunk.Size(), batch_size);
      for (size_t i = 0; i < chunk.Size(); ++i) {
        rows.push_back(chunk.RowAt(i));
      }
    }
    EXPECT_GE(batches, min_batches) << sql;
    EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
    EXPECT_EQ(context.PreCommit(), Status::kSuccess);
    return rows;
  };

  ScopedQueryMemory memory(32768);
  const std::string rel_sql =
      "SELECT key, name FROM WideBatchStream "
      "WHERE EXISTS (SELECT 1 FROM WideBatchStream) ORDER BY key;";
  const auto rel_rows = run_batched(rel_sql, 13, 20);
  ASSERT_EQ(rel_rows.size(), 300U);
  for (size_t i = 0; i < rel_rows.size(); ++i) {
    EXPECT_EQ(rel_rows[i][0], Value(static_cast<int64_t>(i)));
  }
  EXPECT_GT(StatsValue(RelationalExplain(*rs_, rel_sql, /*analyze=*/true),
                       "relation_spills="),
            0);

  // Directly test GroupedFinishExecutor::NextBatch over a spilled core output.
  {
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<Table>, table,
                          ctx.GetTable("WideBatchStream"));
    ASSIGN_OR_ASSERT_FAIL(std::shared_ptr<TableStatistics>, stats,
                          ctx.GetStats("WideBatchStream"));
    auto core_plan = std::make_shared<FullScanPlan>(*table, *stats);
    auto stmt = std::make_shared<SelectStatement>(
        std::vector<NamedExpression>{
            NamedExpression("key", ColumnValueExp("key")),
            NamedExpression("name", ColumnValueExp("name"))},
        std::vector<std::string>{"WideBatchStream"}, nullptr);
    stmt->SetGroupBy({ColumnValueExp("key"), ColumnValueExp("name")});
    stmt->SetOrderBy(
        {SelectStatement::OrderByTerm{.expression = ColumnValueExp("key"),
                                      .ascending = true,
                                      .nulls_first = false}});
    Executor grouped_finish =
        EmitGroupedFinishExecutor(ctx, std::move(core_plan), std::move(stmt));
    std::vector<Row> grp_rows;
    DataChunk chunk;
    size_t batches = 0;
    while (grouped_finish->NextBatch(&chunk, 17) > 0) {
      ++batches;
      EXPECT_LE(chunk.Size(), 17U);
      for (size_t i = 0; i < chunk.Size(); ++i) {
        grp_rows.push_back(chunk.RowAt(i));
      }
    }
    EXPECT_GE(batches, 15U);
    ASSERT_SUCCESS(grouped_finish->GetStatus());
    std::ostringstream dump;
    grouped_finish->Dump(dump, 0);
    EXPECT_NE(dump.str().find("GroupByFinish"), std::string::npos)
        << dump.str();
    ASSERT_EQ(grp_rows.size(), 300U);
    for (size_t i = 0; i < grp_rows.size(); ++i) {
      EXPECT_EQ(grp_rows[i][0], Value(static_cast<int64_t>(i)));
    }
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

}  // namespace tinylamb
