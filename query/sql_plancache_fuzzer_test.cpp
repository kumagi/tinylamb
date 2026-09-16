/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_plancache_fuzzer.hpp"

#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(SqlPlanCacheFuzzer, SeededPhasesAgree) {
  int compared = 0;
  const char* scale = std::getenv("TINYLAMB_FUZZ_ITERS");
  const int kIterations = (scale != nullptr) ? std::atoi(scale) : 32;
  for (uint32_t seed = 0; seed < static_cast<uint32_t>(kIterations); ++seed) {
    std::mt19937 rng(seed);
    PlanCacheStats stats;
    std::string report = RunPlanCacheIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    compared += stats.queries_compared;
  }
  EXPECT_GT(compared, kIterations * 4) << "differential barely ran";
}

TEST(SqlPlanCacheFuzzer, TestFileRoundTrip) {
  PlanCacheTrace trace;
  trace.setup = {
      "CREATE TABLE t (u INT64, k INT64, a INT64);",
      "INSERT INTO t VALUES (0, 1, 10);",
      "INSERT INTO t VALUES (1, 2, NULL);",
  };
  trace.queries = {"SELECT u FROM t WHERE u < 5;",
                   "SELECT u, a FROM t WHERE k = 1;"};
  trace.mutations = {"UPDATE t SET a = 7 WHERE u = 1;"};
  trace.expected = {"|0|,", "|0|,|10|,", "|1|,"};

  const std::string text =
      SerializePlanCacheTest(7, trace, "synthetic for pipeline test");
  uint64_t seed = 0;
  PlanCacheTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParsePlanCacheTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(seed, 7U);
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(ReplayPlanCacheTrace(parsed), "") << "honest trace mismatched";
}

}  // namespace tinylamb
