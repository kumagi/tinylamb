/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_oracle_fuzzer.hpp"

#include <dirent.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {

namespace {

std::vector<std::string> ListTestFiles(const std::string& dir) {
  std::vector<std::string> files;
  DIR* handle = opendir(dir.c_str());
  if (handle == nullptr) {
    return files;
  }
  while (const dirent* entry = readdir(handle)) {
    const std::string name = entry->d_name;
    if (name.size() > 5 && name.substr(name.size() - 5) == ".test") {
      std::string path = dir;
      path += "/";
      path += name;
      files.push_back(std::move(path));
    }
  }
  closedir(handle);
  return files;
}

}  // namespace

// Seeded metamorphic-oracle runs (TLP + NoREC, see sql_oracle_fuzzer.hpp).
// A non-empty report means the engine returned inconsistent results for a
// query and its oracle-equivalent forms: a logic bug in the optimizer or the
// executor.  Skips (unsupported generated syntax) are allowed but counted so
// a silently-all-skipping harness fails this test.
namespace {

void RunSeededOracleRange(uint32_t begin_seed, uint32_t end_seed) {
  int tlp_ran = 0;
  int tlp_agg_ran = 0;
  int unionall_ran = 0;
  int or_partition_ran = 0;
  int subq_ran = 0;
  int norec_ran = 0;
  int pqs_ran = 0;
  int idx_ran = 0;
  int skip_ran = 0;
  int window_ran = 0;
  int topties_ran = 0;
  int ordinal_order_ran = 0;
  int ordinal_derived_ran = 0;
  int ordinal_cte_ran = 0;
  int ordinal_setop_ran = 0;
  int dqe_ran = 0;
  int troc_ran = 0;
  const int count = static_cast<int>(end_seed - begin_seed);
  for (uint32_t seed = begin_seed; seed < end_seed; ++seed) {
    std::mt19937 rng(seed);
    OracleIterationStats stats;
    OracleTrace trace;
    const bool verbose = std::getenv("TINYLAMB_FUZZ_VERBOSE") != nullptr;
    std::string report = RunOracleIteration(rng, verbose, &stats, &trace);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    EXPECT_GE(trace.norec.size(), 32U) << "seed " << seed;
    EXPECT_GE(trace.norec_content.size(), 4U) << "seed " << seed;
    EXPECT_EQ(trace.norec.size() % 2, 0U);
    EXPECT_EQ(trace.norec_content.size() % 2, 0U);
    if (!trace.ordinal_order.empty()) {
      const std::string& query = trace.ordinal_order.front();
      if (query.find("WITH q AS") != std::string::npos) {
        ++ordinal_cte_ran;
      } else if (query.find(" UNION ALL ") != std::string::npos) {
        ++ordinal_setop_ran;
      } else {
        ++ordinal_derived_ran;
      }
    }
    tlp_ran += stats.tlp_ran ? 1 : 0;
    tlp_agg_ran += stats.tlp_agg_ran ? 1 : 0;
    unionall_ran += stats.unionall_ran ? 1 : 0;
    or_partition_ran += stats.or_partition_ran ? 1 : 0;
    subq_ran += stats.subq_ran ? 1 : 0;
    norec_ran += stats.norec_ran ? 1 : 0;
    pqs_ran += stats.pqs_ran ? 1 : 0;
    idx_ran += stats.idx_ran ? 1 : 0;
    skip_ran += stats.skip_ran ? 1 : 0;
    window_ran += stats.window_ran ? 1 : 0;
    topties_ran += stats.topties_ran ? 1 : 0;
    ordinal_order_ran += stats.ordinal_order_ran ? 1 : 0;
    dqe_ran += stats.dqe_ran ? 1 : 0;
    troc_ran += stats.troc_ran ? 1 : 0;
  }
  EXPECT_GT(tlp_ran, count / 2);
  EXPECT_GT(tlp_agg_ran, count / 2);
  EXPECT_GT(unionall_ran, count / 2);
  EXPECT_GT(or_partition_ran, count / 2);
  EXPECT_GE(subq_ran, count / 4);
  EXPECT_GT(norec_ran, count / 2);
  EXPECT_GT(pqs_ran, count / 2);
  EXPECT_GT(idx_ran, count / 2);
  EXPECT_GT(skip_ran, count / 2);
  EXPECT_GT(window_ran, count / 2);
  EXPECT_GT(topties_ran, count / 2);
  EXPECT_GT(ordinal_order_ran, count / 2);
  EXPECT_GT(ordinal_derived_ran, 0);
  EXPECT_GT(ordinal_cte_ran, 0);
  EXPECT_GT(ordinal_setop_ran, 0);
  EXPECT_GT(dqe_ran, count / 2);
  EXPECT_GT(troc_ran, count / 2);
}

void RunPlanFeedbackRange(uint32_t begin_seed, uint32_t end_seed) {
  OracleSession session;
  for (uint32_t seed = begin_seed; seed < end_seed; ++seed) {
    std::mt19937 rng(seed + 1000);
    std::string report =
        RunOracleIteration(rng, false, nullptr, nullptr, &session);
    ASSERT_EQ(report, "") << "failing seed=" << (seed + 1000) << "\n" << report;
  }
  EXPECT_GT(session.PlansSeen(), 1U);
}

}  // namespace

// Only skip the fixed shards when the override is a valid iteration count;
// a malformed value must not silently shrink the sweep to shard0 alone.
// NOLINTNEXTLINE(misc-use-internal-linkage) intentional pattern in this code
bool OracleItersEnvValid() {
  const char* env = std::getenv("TINYLAMB_ORACLE_ITERS");
  if (env == nullptr) {
    return false;
  }
  char* end = nullptr;
  errno = 0;
  const long parsed = std::strtol(env, &end, 10);
  return errno == 0 && end != env && *end == '\0' && parsed > 0 &&
         std::in_range<uint32_t>(parsed);
}

TEST(SqlOracleFuzzer, SeededIterationsHoldOraclesShard0) {
  uint32_t end_seed = 16;
  if (const char* env = std::getenv("TINYLAMB_ORACLE_ITERS")) {
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(env, &end, 10);
    if (errno == 0 && end != env && *end == '\0' && parsed > 0 &&
        std::in_range<uint32_t>(parsed)) {
      end_seed = static_cast<uint32_t>(parsed);
    }
  }
  RunSeededOracleRange(0, end_seed);
}

TEST(SqlOracleFuzzer, SeededIterationsHoldOraclesShard1) {
  if (OracleItersEnvValid()) {
    return;
  }
  RunSeededOracleRange(16, 32);
}

TEST(SqlOracleFuzzer, SeededIterationsHoldOraclesShard2) {
  if (OracleItersEnvValid()) {
    return;
  }
  RunSeededOracleRange(32, 48);
}

TEST(SqlOracleFuzzer, SeededIterationsHoldOraclesShard3) {
  if (OracleItersEnvValid()) {
    return;
  }
  RunSeededOracleRange(48, 64);
}

// Plan feedback: across a run the bandit must observe several distinct plan
// shapes from EXPLAIN.
TEST(SqlOracleFuzzer, PlanFeedbackObservesPlansShard0) {
  RunPlanFeedbackRange(0, 16);
}

TEST(SqlOracleFuzzer, PlanFeedbackObservesPlansShard1) {
  RunPlanFeedbackRange(16, 32);
}

TEST(SqlOracleFuzzer, PlanFeedbackObservesPlansShard2) {
  RunPlanFeedbackRange(32, 48);
}

// AMOEBA: equivalent predicates over a 2000-row table must not diverge
// beyond the conservative thresholds.
TEST(SqlOracleFuzzer, AmoebaEquivalenceHolds) {
  for (uint32_t seed = 0; seed < 8; ++seed) {
    std::mt19937 rng(seed);
    std::string report = RunAmoebaIteration(rng, false);
    ASSERT_EQ(report, "") << "seed=" << seed << " " << report;
  }
}

// Resource pressure (no throwaway database) ran no query at all.  The report
// is "" there, so the iteration must say it was skipped instead of reporting
// every generated family as "ran" -- a run that executed nothing would
// otherwise look like full coverage.
TEST(SqlOracleFuzzer, ResourcePressureCountsAsSkippedNotRan) {
  ASSERT_EQ(setenv("TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE", "1", 1), 0);
  std::mt19937 rng(0);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
  OracleIterationStats stats;
  OracleTrace trace;
  EXPECT_EQ(RunOracleIteration(rng, false, &stats, &trace), "") << "seed 0";
  EXPECT_TRUE(stats.skipped);
  for (const bool ran :
       {stats.tlp_ran, stats.unionall_ran, stats.subq_ran, stats.norec_ran,
        stats.pqs_ran, stats.skip_ran, stats.window_ran, stats.recursive_ran,
        stats.cte_ran}) {
    EXPECT_FALSE(ran) << "family reported as ran with no database";
  }
  EXPECT_EQ(unsetenv("TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE"), 0);
  // The hatch is off again: the next iteration must run and count normally.
  OracleIterationStats again;
  EXPECT_EQ(RunOracleIteration(rng, false, &again, nullptr), "");
  EXPECT_FALSE(again.skipped);
  EXPECT_TRUE(again.tlp_ran || again.unionall_ran || again.subq_ran);
}

namespace {

// Drains `sql`.  NoREC skips a rejected spelling, so a shape test that only
// replays would stay green while checking nothing.
::testing::AssertionResult ExpectRuns(Database& db, TransactionContext& ctx,
                                      const std::string& sql) {
  SqlEngine engine(db);
  StatusOr<QueryResult> result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    return ::testing::AssertionFailure() << engine.LastError() << "\n" << sql;
  }
  Row row;
  while (result.Value().Next(&row)) {
  }
  if (result.Value().GetStatus() != Status::kSuccess) {
    return ::testing::AssertionFailure()
           << result.Value().GetStatus().GetMessage() << "\n"
           << sql;
  }
  return ::testing::AssertionSuccess();
}

}  // namespace

// The canonical NoREC identities (single-table rewrites, joins, GROUP BY,
// subquery decorrelation, row contents) must execute and agree, including
// after the indexes that make the WHERE side a range or lookup.
TEST(SqlOracleFuzzer, NoRecCanonicalShapesAgree) {
  OracleTrace trace;
  trace.table = "norec_t";
  trace.predicate = "(a > 0)";
  trace.setup = {
      "CREATE TABLE norec_t (u INT64, a INT64, b INT64, flag BOOL, s "
      "VARCHAR(8));",
      "CREATE TABLE norec_t_s (u INT64, a INT64, b INT64, flag BOOL, s "
      "VARCHAR(8));",
      "CREATE TABLE norec_t_j (x INT64, y VARCHAR(8));",
      "INSERT INTO norec_t VALUES (0, 1, NULL, TRUE, 'a');",
      "INSERT INTO norec_t VALUES (1, NULL, 2, FALSE, NULL);",
      "INSERT INTO norec_t VALUES (2, -1, 0, NULL, 'bb');",
      "INSERT INTO norec_t VALUES (3, 2, 2, TRUE, '');",
      "INSERT INTO norec_t_s VALUES (10, 1, 0, TRUE, 'a');",
      "INSERT INTO norec_t_s VALUES (11, NULL, NULL, NULL, 'zz');",
      "INSERT INTO norec_t_j VALUES (1, 'a');",
      "INSERT INTO norec_t_j VALUES (NULL, 'bb');",
      "INSERT INTO norec_t_j VALUES (4, NULL);",
  };
  const std::string t = "norec_t";
  const std::string p = "(a > 0)";
  trace.norec = {
      "SELECT COUNT(*) FROM " + t + " WHERE (" + p + ");",
      "SELECT COALESCE(SUM(CASE WHEN (" + p + ") THEN 1 ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM (SELECT u FROM " + t + " WHERE (" + p + ")) q;",
      "SELECT COALESCE(SUM(CASE WHEN (" + p + ") THEN 1 ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM (SELECT u, a, b, flag, s FROM " + t +
          ") d WHERE (" + p + ");",
      "SELECT COALESCE(SUM(CASE WHEN (" + p +
          ") THEN 1 ELSE 0 END), 0) FROM (SELECT u, a, b, flag, s FROM " + t +
          ") d;",
      "SELECT COALESCE(SUM(c), 0) FROM (SELECT COUNT(*) AS c FROM " + t +
          " WHERE (" + p + ") GROUP BY a) g;",
      "SELECT COALESCE(SUM(c), 0) FROM (SELECT SUM(CASE WHEN (" + p +
          ") THEN 1 ELSE 0 END) AS c FROM " + t + " GROUP BY a) g;",
      "SELECT COUNT(*) FROM (SELECT u, a FROM " + t + " WHERE (" + p +
          ") ORDER BY a, u) q;",
      "SELECT COALESCE(SUM(CASE WHEN (" + p + ") THEN 1 ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE 0 < a;",
      "SELECT COALESCE(SUM(CASE WHEN a > 0 THEN 1 ELSE 0 END), 0) FROM " + t +
          ";",
      "SELECT COUNT(*) FROM " + t + " WHERE a IN (1, 2, -1);",
      "SELECT COALESCE(SUM(CASE WHEN (a = 1) OR (a = 2) OR (a = -1) THEN 1 "
      "ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE a BETWEEN -1 AND 1;",
      "SELECT COALESCE(SUM(CASE WHEN (a >= -1) AND (a <= 1) THEN 1 ELSE 0 "
      "END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE s LIKE 'a%';",
      "SELECT COALESCE(SUM(CASE WHEN s LIKE 'a%' THEN 1 ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE a IS NULL;",
      "SELECT COALESCE(SUM(CASE WHEN a IS NULL THEN 1 ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE flag IS NOT TRUE;",
      "SELECT COALESCE(SUM(CASE WHEN flag IS NOT TRUE THEN 1 ELSE 0 END), 0) "
      "FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE (a + 1) > 1;",
      "SELECT COALESCE(SUM(CASE WHEN (a + 1) > 1 THEN 1 ELSE 0 END), 0) FROM " +
          t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE u = 2;",
      "SELECT COALESCE(SUM(CASE WHEN u = 2 THEN 1 ELSE 0 END), 0) FROM " + t +
          ";",
      "SELECT COUNT(*) FROM " + t + " AS l JOIN " + t +
          " AS r ON l.a = r.a WHERE (l.a > 0);",
      "SELECT COALESCE(SUM(CASE WHEN (l.a > 0) THEN 1 ELSE 0 END), 0) FROM " +
          t + " AS l JOIN " + t + " AS r ON l.a = r.a;",
      "SELECT COUNT(*) FROM " + t + " AS l JOIN " + t +
          "_s AS r ON l.a = r.a AND (l.a > 0);",
      "SELECT COALESCE(SUM(CASE WHEN (l.a = r.a) AND (l.a > 0) THEN 1 ELSE 0 "
      "END), 0) FROM " +
          t + " AS l CROSS JOIN " + t + "_s AS r;",
      "SELECT COUNT(*) FROM " + t + " AS l LEFT JOIN " + t +
          "_s AS r ON l.a = r.a WHERE (l.b IS NULL);",
      "SELECT COALESCE(SUM(CASE WHEN (l.b IS NULL) THEN 1 ELSE 0 END), 0) "
      "FROM " +
          t + " AS l LEFT JOIN " + t + "_s AS r ON l.a = r.a;",
      "SELECT COUNT(*) FROM " + t + " WHERE a IN (SELECT x FROM " + t + "_j);",
      "SELECT COALESCE(SUM(CASE WHEN a IN (SELECT x FROM " + t +
          "_j) THEN 1 ELSE 0 END), 0) FROM " + t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE a NOT IN (SELECT x FROM " + t +
          "_j);",
      "SELECT COALESCE(SUM(CASE WHEN a NOT IN (SELECT x FROM " + t +
          "_j) THEN 1 ELSE 0 END), 0) FROM " + t + ";",
      "SELECT COUNT(*) FROM " + t + " WHERE EXISTS (SELECT 1 FROM " + t +
          "_j WHERE " + t + "_j.x = " + t + ".a);",
      "SELECT COALESCE(SUM(CASE WHEN EXISTS (SELECT 1 FROM " + t + "_j WHERE " +
          t + "_j.x = " + t + ".a) THEN 1 ELSE 0 END), 0) FROM " + t + ";",
  };
  trace.norec_content = {
      "SELECT u, a, b, flag, s FROM " + t + " WHERE (" + p + ");",
      "SELECT u, a, b, flag, s, CASE WHEN (" + p + ") THEN 1 ELSE 0 END FROM " +
          t + ";",
      "SELECT u, a, b FROM " + t + " WHERE (a >= -1) AND (a <= 1);",
      "SELECT u, a, b, CASE WHEN (a >= -1) AND (a <= 1) THEN 1 ELSE 0 END "
      "FROM " +
          t + ";",
  };
  trace.index_ddl = {
      "CREATE INDEX idx_norec_a ON norec_t(a);",
      "CREATE INDEX idx_norec_b ON norec_t(b);",
      "CREATE INDEX idx_norec_s ON norec_t(s);",
      "CREATE UNIQUE INDEX idx_norec_u ON norec_t(u);",
      "CREATE INDEX idx_norec_sa ON norec_t_s(a);",
      "CREATE INDEX idx_norec_jx ON norec_t_j(x);",
  };
  trace.index_probe = "SELECT COUNT(*) FROM norec_t WHERE (a > 0);";

  EXPECT_EQ(ReplayOracleTrace(trace, true), "");

  const std::string text = SerializeOracleTest(11, trace, "");
  uint64_t seed = 0;
  OracleTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseOracleTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(parsed, trace);

  ScopedDb sdb("norec_shapes");
  ASSERT_NE(sdb.get(), nullptr);
  Database& db = *sdb;
  TransactionContext ctx = db.BeginContext();
  for (const std::string& sql : trace.setup) {
    ASSERT_TRUE(ExpectRuns(db, ctx, sql));
  }
  // T4621: the setup replay must be durable-visible to the fresh context
  // below; a failing PreCommit means the rig broke, not the engine, and the
  // old (void) cast hid that.
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess) << "seed=" << seed;
  ctx = db.BeginContext();
  for (const std::string& sql : trace.norec) {
    ASSERT_TRUE(ExpectRuns(db, ctx, sql));
  }
  for (const std::string& sql : trace.norec_content) {
    ASSERT_TRUE(ExpectRuns(db, ctx, sql));
  }
  for (const std::string& ddl : trace.index_ddl) {
    ASSERT_TRUE(ApplyIndexSpec(db, ctx, ddl)) << ddl;
  }
  for (const char* table : {"norec_t", "norec_t_s", "norec_t_j"}) {
    ASSERT_TRUE(ExpectRuns(db, ctx, std::string("ANALYZE ") + table + ";"));
  }
  for (const std::string& sql : trace.norec) {
    ASSERT_TRUE(ExpectRuns(db, ctx, sql));
  }
  for (const std::string& sql : trace.norec_content) {
    ASSERT_TRUE(ExpectRuns(db, ctx, sql));
  }

  OracleTrace broken = trace;
  broken.norec[1] =
      "SELECT COALESCE(SUM(CASE WHEN a > 100 THEN 1 ELSE 0 END), 0) FROM "
      "norec_t;";
  const std::string scalar = ReplayOracleTrace(broken, false);
  EXPECT_NE(scalar.find("[NoREC MISMATCH]"), std::string::npos) << scalar;

  OracleTrace rows = trace;
  rows.norec_content[1] =
      "SELECT u, a, b, flag, s, CASE WHEN a > 100 THEN 1 ELSE 0 END FROM "
      "norec_t;";
  const std::string content = ReplayOracleTrace(rows, false);
  EXPECT_NE(content.find("kind: rows"), std::string::npos) << content;
}

TEST(SqlOracleFuzzer, DerivedTableOrderByOrdinalUsesProjectedColumn) {
  OracleTrace trace;
  trace.setup = {
      "CREATE TABLE ordinal_t (u INT64, a INT64);",
      "INSERT INTO ordinal_t VALUES (2, 1), (0, NULL), (1, 2);",
  };
  trace.ordinal_order = {
      "SELECT * FROM (SELECT u, a FROM ordinal_t) q ORDER BY 1;",
  };
  trace.ordinal_order_expect = {"[0, NULL]", "[1, 2]", "[2, 1]"};
  EXPECT_EQ(ReplayOracleTrace(trace, true), "");
}

// End-to-end check of the failure->file->replay pipeline: a hand-crafted
// trace with a deliberately inconsistent oracle must serialize, parse and
// replay into a mismatch, and its consistent twin must replay clean.
TEST(SqlOracleFuzzer, TestFileRoundTripDetectsMismatch) {
  OracleTrace broken;
  broken.setup = {
      "CREATE TABLE t0 (a INT64, b INT64, flag BOOL, s VARCHAR(8));",
      "INSERT INTO t0 VALUES (1, -2, TRUE, 'a');",
      "INSERT INTO t0 VALUES (0, 3, NULL, 'bb');",
  };
  broken.predicate = "(a >= 0) AND (b > 100)";
  broken.tlp = {
      "SELECT * FROM t0 WHERE (a >= 0) AND (b > 100);",  // 0 rows: b <= 3
      "SELECT * FROM t0 WHERE ((a >= 0) AND (b > 100)) AND (a > 1000);",
      "SELECT * FROM t0 WHERE (a >= 0) AND NOT (b > 100);",  // 2 rows
      "SELECT * FROM t0 WHERE ((a >= 0) AND (b > 100)) AND (b > 100) IS NULL;",
  };
  broken.norec = {
      "SELECT COUNT(*) FROM t0 WHERE (a >= 0);",
      "SELECT SUM(CASE WHEN (a >= 0) THEN 1 ELSE 0 END) FROM t0;",
  };

  const std::string text =
      SerializeOracleTest(7, broken, "synthetic mismatch for pipeline test");
  uint64_t seed = 0;
  OracleTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseOracleTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(seed, 7U);
  EXPECT_EQ(parsed, broken);
  EXPECT_NE(ReplayOracleTrace(parsed, true), "");

  // The consistent twin: original query matches the partition union.
  OracleTrace consistent = broken;
  consistent.tlp[0] = "SELECT * FROM t0 WHERE (a >= 0) AND NOT (b > 100);";
  const std::string consistent_text = SerializeOracleTest(8, consistent, "");
  uint64_t seed2 = 0;
  OracleTrace parsed2;
  std::string summary2;
  ASSERT_TRUE(ParseOracleTest(consistent_text, &seed2, &parsed2, &summary2));
  EXPECT_EQ(ReplayOracleTrace(parsed2), "");
}

TEST(SqlOracleFuzzer, OrPartitionPreservesDuplicateRows) {
  OracleTrace trace;
  trace.setup = {
      "CREATE TABLE or_bag_oracle (a INT64, b INT64);",
      "INSERT INTO or_bag_oracle VALUES (1, 2), (1, 2), (1, NULL), "
      "(NULL, 2), (NULL, 2), (3, NULL), (4, 0);",
  };
  trace.or_partition = {
      "SELECT a, b FROM or_bag_oracle WHERE a = 1 OR b = 2;",
      "SELECT a, b FROM or_bag_oracle WHERE (a = 1) IS TRUE UNION ALL "
      "SELECT a, b FROM or_bag_oracle WHERE (b = 2) IS TRUE AND "
      "(a = 1) IS NOT TRUE;",
  };
  EXPECT_EQ(ReplayOracleTrace(trace, false), "");
}

// The window oracle must catch a wrong model: perturb the EXCLUDE CURRENT ROW
// flag on the second window probe so the replayed model no longer matches.
TEST(SqlOracleFuzzer, WindowOracleDetectsWrongModel) {
  OracleTrace trace;
  trace.table = "t9";
  trace.setup = {
      "CREATE TABLE t9 (u INT64, a INT64, b INT64, flag BOOL, s VARCHAR(8));",
      "INSERT INTO t9 VALUES (0, 1, NULL, TRUE, 'a');",
      "INSERT INTO t9 VALUES (1, 1, 2, FALSE, '');",
      "INSERT INTO t9 VALUES (2, NULL, -3, NULL, 'zz');",
      "INSERT INTO t9 VALUES (3, 4, 4, TRUE, 'bb');",
  };
  trace.predicate = "a > 0";
  trace.window_probes = {
      "0|0|0|2|-1|0|0|SELECT u, ROW_NUMBER() OVER (ORDER BY u) FROM t9 "
      "ORDER BY u;",
      "10|1|0|0|0|0|0|SELECT u, SUM(b) OVER (PARTITION BY a ORDER BY u ROWS "
      "BETWEEN 1 PRECEDING AND 0 FOLLOWING) FROM t9 ORDER BY u;",
  };
  trace.topties_limit = 2;
  trace.topties = "SELECT u, a FROM t9 ORDER BY a LIMIT 2 WITH TIES;";
  trace.skip_probes = {"SELECT COUNT(DISTINCT a) FROM t9;",
                       "SELECT DISTINCT a FROM t9 ORDER BY a;"};
  EXPECT_EQ(ReplayOracleTrace(trace, true), "");

  OracleTrace wrong = trace;
  // Frame becomes 2 PRECEDING: row u=1's expected SUM changes 2 -> 2 (NULL+2
  // stays), u=0 keeps NULL; use the ROW_NUMBER probe instead, where shifting
  // p1 flips nothing... use frame probe with EXCLUDE CURRENT ROW flag.
  wrong.window_probes[1] =
      "10|1|0|0|0|1|0|SELECT u, SUM(b) OVER (PARTITION BY a ORDER BY u ROWS "
      "BETWEEN 1 PRECEDING AND 0 FOLLOWING) FROM t9 ORDER BY u;";
  EXPECT_NE(ReplayOracleTrace(wrong, false), "");
}

// A committed file whose recorded arms all fail to execute must not pass
// vacuously: the Check* "oracle skipped" bool used to be discarded, so a
// truncated or tampered trace replayed clean with an empty report. Every
// skip is now surfaced as a report of its own.
TEST(SqlOracleFuzzer, ReplayReportsAllArmsSkipped) {
  OracleTrace trace;
  trace.setup = {"CREATE TABLE t9 (a INT64, b INT64);",
                 "INSERT INTO t9 VALUES (1, 2), (3, 4);"};
  trace.tlp = {"SELECT * FROM missing_table;", "SELECT * FROM missing_table;",
               "SELECT * FROM missing_table;", "SELECT * FROM missing_table;"};
  trace.subq = {"SELECT * FROM missing_table;", "SELECT * FROM missing_table;",
                "SELECT * FROM missing_table;"};
  trace.norec = {"SELECT * FROM missing_table;",
                 "SELECT * FROM missing_table;"};
  const std::string report = ReplayOracleTrace(trace, false);
  EXPECT_NE(report.find("skipped recorded oracle arm"), std::string::npos)
      << report;
  EXPECT_NE(report.find("TLP"), std::string::npos) << report;
}

// Execution-failure paths of CheckSkip/CheckTopTies must report "oracle
// skipped", and CheckTlpAgg's row-shape guard must use the cell count (not the
// serialized byte length) so a short aggregate row cannot reach r[1]/r[2].
TEST(SqlOracleFuzzer, ReplayReportsSkippedSkipToptiesAndShortAggRows) {
  OracleTrace trace;
  trace.table = "t9";
  trace.setup = {"CREATE TABLE t9 (u INT64, a INT64);",
                 "INSERT INTO t9 VALUES (1, 2), (3, 4);"};
  // Valid setup, but every recorded probe/query references a missing table.
  trace.skip_probes = {"SELECT COUNT(DISTINCT a) FROM missing_table;"};
  trace.topties =
      "SELECT u, a FROM missing_table ORDER BY a LIMIT 2 WITH TIES;";
  trace.topties_limit = 2;
  // Four valid one-cell result sets: the old Size()-based guard let them
  // through into out-of-bounds r[1]/r[2] reads.
  trace.tlp_agg = {"SELECT u FROM t9;", "SELECT u FROM t9;",
                   "SELECT u FROM t9;", "SELECT u FROM t9;"};
  const std::string report = ReplayOracleTrace(trace, false);
  EXPECT_NE(report.find("skipped recorded oracle arm"), std::string::npos)
      << report;
  EXPECT_NE(report.find("TLP_AGG"), std::string::npos) << report;
  EXPECT_NE(report.find("SKIP"), std::string::npos) << report;
  EXPECT_NE(report.find("TOP_TIES"), std::string::npos) << report;
}

// A failed Create/CrashAndReopen leaves db_ null while WAL segments,
// checkpoint temps and LSM/TLCS companions may already be on disk. The sweep
// must cover the whole Database::DeleteAll() family, not just five suffixes.
TEST(SqlOracleFuzzer, ScopedDbRemovesFullArtifactFamily) {
  ScopedDb sdb("scoped_db_family");
  ASSERT_TRUE(static_cast<bool>(sdb));
  const std::string name = sdb.name();
  sdb.Reset();  // close the live database before laying down its artifact set
  const std::vector<std::string> files = {name + ".db",
                                          name + ".tmp",
                                          name + ".db.tmp",
                                          name + ".db.ckpt",
                                          name + ".db.ckpt.tmp",
                                          name + ".ckpt",
                                          name + ".ckpt.tmp",
                                          name + ".log",
                                          name + ".log.meta",
                                          name + ".log.meta.tmp",
                                          name + ".log.0",
                                          name + ".log.12",
                                          name + ".last_checkpoint",
                                          name + ".last_checkpoint.tmp",
                                          name + ".db.blocks",
                                          name + ".db.blocks.3",
                                          name + ".db.lsm",
                                          name + ".db.lsm.2",
                                          name + ".db.tlcs.7",
                                          name + ".blocks.1",
                                          name + ".lsm.4",
                                          name + ".tlcs.9"};
  for (const std::string& f : files) {
    std::ofstream out(f);
    out << "x";
  }
  sdb.RemoveDbFiles();
  for (const std::string& f : files) {
    EXPECT_FALSE(std::filesystem::exists(f)) << f;
  }
}

// Any committed sql_oracle_fuzz-*.test file replays as a permanent
// regression guard: the recorded oracles must hold now (i.e. the bug the
// file captured has been fixed).  Point TINYLAMB_ORACLE_REGRESSION_DIR at a
// directory to enable; with no directory the test passes trivially.
TEST(SqlOracleFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_ORACLE_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "skipped";
  }
  const std::vector<std::string> paths = ListTestFiles(dir);
  // T3944: an existing-but-empty directory (checkout miss, wrong path) must
  // not report success while replaying zero guards; mirrors the ooc /
  // griffin / join regression replays.
  EXPECT_FALSE(paths.empty()) << "no .test files in " << dir;
  for (const std::string& path : paths) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    uint64_t seed = 0;
    OracleTrace trace;
    std::string summary;
    ASSERT_TRUE(ParseOracleTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    std::string report = ReplayOracleTrace(trace, false);
    EXPECT_EQ(report, "") << "regression " << path << " still reproduces:\n"
                          << report << summary;
  }
}

TEST(SqlOracleFuzzer, EmitFuzzFindingPersistsAndContinuesWhenConfigured) {
  const std::string summary = FormatFuzzFailureSummary(
      "auto-generated by sql_oracle_fuzzer",
      "[TLP MISMATCH]\n  query: SELECT * FROM t0;\n  expected (1):\n    [1]\n");
  EXPECT_EQ(summary.find('\n'), std::string::npos);
  EXPECT_NE(summary.find("[TLP MISMATCH]"), std::string::npos);

  const std::filesystem::path tmp_dir =
      std::filesystem::temp_directory_path() /
      ("tinylamb_fuzz_finding_test_" + RandomString(6));
  std::filesystem::create_directories(tmp_dir);
  const char* prev_continue = std::getenv("TINYLAMB_FUZZ_CONTINUE");
  const std::string saved_continue =
      prev_continue != nullptr ? prev_continue : "";
  const char* prev_findings = std::getenv("TINYLAMB_FUZZ_FINDINGS_DIR");
  const std::string saved_findings =
      prev_findings != nullptr ? prev_findings : "";
  ASSERT_EQ(setenv("TINYLAMB_FUZZ_CONTINUE", "1", 1), 0);
  ASSERT_EQ(setenv("TINYLAMB_FUZZ_FINDINGS_DIR", tmp_dir.c_str(), 1), 0);

  const std::string fname =
      "sql_oracle_fuzz-repro-test-" + RandomString(6) + ".test";
  EmitFuzzFinding("sql_oracle_fuzz", 42, fname, "-- tinylamb-oracle-test v1\n",
                  "[TLP MISMATCH]\n", "");

  EXPECT_TRUE(std::filesystem::exists(tmp_dir / fname));
  const std::string stem = fname.substr(0, fname.size() - 5);
  EXPECT_TRUE(std::filesystem::exists(tmp_dir / (stem + ".report.txt")));

  std::filesystem::remove(fname);
  std::filesystem::remove(stem + ".report.txt");
  std::filesystem::remove_all(tmp_dir);
  if (prev_continue != nullptr) {
    setenv("TINYLAMB_FUZZ_CONTINUE", saved_continue.c_str(), 1);
  } else {
    unsetenv("TINYLAMB_FUZZ_CONTINUE");
  }
  if (prev_findings != nullptr) {
    setenv("TINYLAMB_FUZZ_FINDINGS_DIR", saved_findings.c_str(), 1);
  } else {
    unsetenv("TINYLAMB_FUZZ_FINDINGS_DIR");
  }
}

TEST(SqlOracleFuzzer, LSTableDictAllPassExcludesNullRows) {
  OracleTrace trace;
  trace.table = "dict_null_t";
  trace.setup = {
      "CREATE TABLE dict_null_t (u INT64, a INT64, b INT64, flag BOOL, "
      "s VARCHAR(8));",
      "INSERT INTO dict_null_t VALUES (0, 2, -2, TRUE, 'a');",
      "INSERT INTO dict_null_t VALUES (1, 1, 2, NULL, '');",
      "INSERT INTO dict_null_t VALUES (2, 1, NULL, FALSE, NULL);",
      "INSERT INTO dict_null_t VALUES (3, NULL, 1, TRUE, 'zz');",
      "INSERT INTO dict_null_t VALUES (4, -1, 0, NULL, NULL);",
      "INSERT INTO dict_null_t VALUES (5, 0, -3, FALSE, 'bb');",
  };
  trace.predicate = "(s != '') OR (s != 'bb')";
  trace.tlp = {
      "SELECT COALESCE(a, 0), b FROM dict_null_t WHERE (s != '') OR "
      "(s != 'bb');",
      "SELECT COALESCE(a, 0), b FROM dict_null_t WHERE ((s != '') OR "
      "(s != 'bb')) AND (s IS NOT NULL);",
      "SELECT COALESCE(a, 0), b FROM dict_null_t WHERE ((s != '') OR "
      "(s != 'bb')) AND NOT (s IS NOT NULL);",
      "SELECT COALESCE(a, 0), b FROM dict_null_t WHERE ((s != '') OR "
      "(s != 'bb')) AND (s IS NOT NULL) IS NULL;",
  };
  EXPECT_EQ(ReplayOracleTrace(trace, true), "");
}

}  // namespace tinylamb
