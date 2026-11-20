/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_plan_cache_fuzzer.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <utility>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(SqlPlanCacheFuzzer, SeededPhasesAgree) {
  int compared = 0;
  const char* scale = std::getenv("TINYLAMB_FUZZ_ITERS");
  int kIterations = 32;
  if (scale != nullptr) {
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(scale, &end, 10);
    if (errno == 0 && end != scale && *end == '\0' && parsed > 0 &&
        parsed <= std::numeric_limits<int>::max()) {
      kIterations = static_cast<int>(parsed);
    }
  }
  for (uint32_t seed = 0; std::cmp_less(seed, kIterations); ++seed) {
    std::mt19937 rng(seed);
    PlanCacheStats stats;
    std::string report = RunPlanCacheIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    compared += stats.queries_compared;
  }
  EXPECT_GT(compared, kIterations * 4) << "differential barely ran";
}

TEST(SqlPlanCacheFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_PLANCACHE_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "skipped";
  }
  std::vector<std::string> paths;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.path().extension() == ".test") {
      paths.push_back(entry.path().string());
    }
  }
  std::sort(paths.begin(), paths.end());
  EXPECT_FALSE(paths.empty()) << "no .test files in " << dir;
  for (const std::string& path : paths) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    uint64_t seed = 0;
    PlanCacheTrace trace;
    std::string summary;
    ASSERT_TRUE(ParsePlanCacheTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    EXPECT_EQ(ReplayPlanCacheTrace(trace, false), "")
        << "regression " << path << " still reproduces:\n"
        << summary;
  }
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

  // A broken expectation must be detected, otherwise the replay oracle is
  // self-satisfying and cannot guard committed .test regressions.
  PlanCacheTrace tampered = parsed;
  ASSERT_FALSE(tampered.expected.empty());
  tampered.expected[0] = "|99999|,";
  EXPECT_NE(ReplayPlanCacheTrace(tampered), "")
      << "tampered expectation was not detected";
}

}  // namespace tinylamb
