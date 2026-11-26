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

#include "common/platform/process.hpp"
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
  ScopedExecEnv() : saved_(::tinylamb::platform::GetEnv("TINYLAMB_EXEC_ENGINE") != nullptr) {
    if (saved_) {
      original_ = ::tinylamb::platform::GetEnv("TINYLAMB_EXEC_ENGINE");
    }
  }
  ~ScopedExecEnv() {
    if (saved_) {
      (void)::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", original_.c_str(), 1);
    } else {
      (void)::tinylamb::platform::UnsetEnv("TINYLAMB_EXEC_ENGINE");
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
    (void)::tinylamb::platform::SetEnv("TINYLAMB_EXEC_ENGINE", "legacy", 1);
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
  seen.reserve(numeric.size());
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
  left_rows.reserve(5);
  std::vector<Row> right_rows;
  right_rows.reserve(5);
  for (int64_t i = 0; i < 5; ++i) {
    left_rows.emplace_back(Row({Value(i)}));
    right_rows.emplace_back(Row({Value(i)}));
  }
  HashJoin join(std::make_shared<ConstantExecutor>(std::move(left_rows)), {0},
                std::make_shared<ConstantExecutor>(std::move(right_rows)), {0},
                HashJoinMode::kInMemory, 2);
  std::unordered_set<Row> expected;
  expected.reserve(5);
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
  left_rows.reserve(400);
  std::vector<Row> right_rows;
  right_rows.reserve(400);
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
  left_rows.reserve(50);
  std::vector<Row> right_rows;
  right_rows.reserve(400);
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
  left_rows.reserve(300);
  std::vector<Row> right_rows;
  right_rows.reserve(300);
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
  left_rows.reserve(200);
  std::vector<Row> right_rows;
  right_rows.reserve(200);
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
  left_rows.reserve(200);
  std::vector<Row> right_rows;
  right_rows.reserve(200);
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
  left_rows.reserve(2000);
  std::vector<Row> right_rows;
  right_rows.reserve(2000);
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
  rows.reserve(300);
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
  left_rows.reserve(30);
  std::vector<Row> right_rows;
  right_rows.reserve(30);
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
}  // namespace tinylamb
