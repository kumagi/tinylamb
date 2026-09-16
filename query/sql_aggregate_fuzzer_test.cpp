/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_aggregate_fuzzer.hpp"

#include <dirent.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

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
      files.push_back(dir + "/" + name);
    }
  }
  closedir(handle);
  return files;
}

}  // namespace

TEST(SqlAggregateFuzzer, SeededAggregatesMatchMirror) {
  int group_queries = 0;
  int window_queries = 0;
  const char* scale = std::getenv("TINYLAMB_FUZZ_ITERS");
  const int kIterations = (scale != nullptr) ? std::atoi(scale) : 48;
  for (uint32_t seed = 0; seed < static_cast<uint32_t>(kIterations); ++seed) {
    std::mt19937 rng(seed);
    AggStats stats;
    AggTrace trace;
    std::string report;
    try {
      report = RunAggregateIteration(rng, false, &stats, &trace);
    } catch (const std::exception& ex) {
      report = std::string(
                   "[AGGREGATE MISMATCH] exception escaped "
                   "RunAggregateIteration: ") +
               ex.what();
    }
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n"
                          << SerializeAggregateTest(seed, trace, "seeded run")
                          << "\n"
                          << report;
    group_queries += stats.group_queries;
    window_queries += stats.window_queries;
  }
  EXPECT_GT(group_queries, kIterations) << "GROUP BY oracle almost never ran";
  EXPECT_GT(window_queries, 0) << "window oracle never ran";
}

TEST(SqlAggregateFuzzer, TestFileRoundTripDetectsMismatch) {
  AggTrace trace;
  trace.setup = {
      "CREATE TABLE t (u INT64, a INT64, b INT64, s VARCHAR(8));",
      "INSERT INTO t VALUES (0, 1, -2, 'x');",
      "INSERT INTO t VALUES (1, NULL, 3, NULL);",
  };
  trace.queries = {
      {"SELECT a, COUNT(*), SUM(a) FROM t GROUP BY a;",
       {"NULL,|1|,NULL,", "|1|,|1|,|1|,"}},
      {"SELECT u, ROW_NUMBER() OVER (PARTITION BY b ORDER BY u) FROM t;",
       {"|0|,|1|,", "|1|,|1|,"}},
  };
  const std::string text =
      SerializeAggregateTest(5, trace, "synthetic for pipeline test");
  uint64_t seed = 0;
  AggTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseAggregateTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(seed, 5U);
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(ReplayAggregateTrace(parsed), "") << "honest trace mismatched";

  AggTrace broken = trace;
  broken.queries[1].expected = {"|0|,|9|,", "|1|,|9|,"};
  const std::string broken_text = SerializeAggregateTest(6, broken, "");
  uint64_t seed2 = 0;
  AggTrace parsed2;
  std::string summary2;
  ASSERT_TRUE(ParseAggregateTest(broken_text, &seed2, &parsed2, &summary2));
  EXPECT_NE(ReplayAggregateTrace(parsed2), "")
      << "tampered trace replayed clean";
}

// Any committed sql_aggregate_fuzz-*.test file replays as a permanent
// regression guard: the recorded expectations must hold now.  Point
// TINYLAMB_AGGREGATE_REGRESSION_DIR at a directory to enable; with no
// directory the test passes trivially.
TEST(SqlAggregateFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_AGGREGATE_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << true;
  }
  for (const std::string& path : ListTestFiles(dir)) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    uint64_t seed = 0;
    AggTrace trace;
    std::string summary;
    ASSERT_TRUE(ParseAggregateTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    EXPECT_EQ(ReplayAggregateTrace(trace), "")
        << "regression " << path << " still reproduces:\n"
        << summary;
  }
}

}  // namespace tinylamb
