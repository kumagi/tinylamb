/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "common/platform/process.hpp"
#include "query/sql_aggregate_fuzzer.hpp"

#include <filesystem>
#include <system_error>
#include <dirent.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {

namespace {

std::vector<std::string> ListTestFiles(const std::string& dir) {
  std::vector<std::string> files;
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  const std::filesystem::directory_iterator end;
  for (; !ec && it != end; it.increment(ec)) {
    const std::filesystem::path path = it->path();
    if (path.filename().string().ends_with(".test")) {
      files.push_back(path.string());
    }
  }
  return files;
}

void RunSeededAggregateRange(uint32_t begin_seed, uint32_t end_seed) {
  int group_queries = 0;
  int window_queries = 0;
  int primary_key_group_queries = 0;
  const int count = static_cast<int>(end_seed - begin_seed);
  for (uint32_t seed = begin_seed; seed < end_seed; ++seed) {
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
    primary_key_group_queries += stats.primary_key_group_queries;
  }
  EXPECT_GT(group_queries, count) << "GROUP BY oracle almost never ran";
  EXPECT_GT(window_queries, 0) << "window oracle never ran";
  EXPECT_EQ(primary_key_group_queries, count);
}

}  // namespace

TEST(SqlAggregateFuzzer, SeededAggregatesMatchMirrorShard0) {
  uint32_t end_seed = 24;
  if (const char* scale = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_ITERS")) {
    char* end = nullptr;
    errno = 0;
    const unsigned long parsed = std::strtoul(scale, &end, 10);
    if (errno == 0 && end != scale && *end == '\0' && parsed > 0 &&
        parsed <= std::numeric_limits<uint32_t>::max()) {
      end_seed = static_cast<uint32_t>(parsed);
    }
  }
  RunSeededAggregateRange(0, end_seed);
}

TEST(SqlAggregateFuzzer, SeededAggregatesMatchMirrorShard1) {
  // Keep the fixed 24..48 range even when shard0's env override is set;
  // skipping it silently used to drop those regression seeds entirely.
  RunSeededAggregateRange(24, 48);
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
  const char* dir = ::tinylamb::platform::GetEnv("TINYLAMB_AGGREGATE_REGRESSION_DIR");
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
    AggTrace trace;
    std::string summary;
    ASSERT_TRUE(ParseAggregateTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    EXPECT_EQ(ReplayAggregateTrace(trace), "")
        << "regression " << path << " still reproduces:\n"
        << summary;
  }
}

TEST(SqlAggregateFuzzer,
     CrashSeedsFromTypedParallelStreamAggregateVarcharCount) {
  const std::vector<std::vector<uint8_t>> crash_inputs = {
      {236, 153, 18, 136, 137, 188, 196, 13, 45, 64, 247, 37, 250, 153, 236},
      {215, 38, 167, 223, 166, 176, 217, 176},
      {140, 12, 12, 12, 12, 12, 3, 174},
      {234, 234, 234, 73, 234},
      {2, 104, 47, 96, 72, 91, 202, 250, 2, 104},
  };
  for (const auto& bytes : crash_inputs) {
    AggregateFuzzTry(bytes.data(), bytes.size(), false);
  }
}

}  // namespace tinylamb
