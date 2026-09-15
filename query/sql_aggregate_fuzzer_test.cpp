/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_aggregate_fuzzer.hpp"

#include <cstdint>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(SqlAggregateFuzzer, SeededAggregatesMatchMirror) {
  int group_queries = 0;
  int window_queries = 0;
  constexpr int kIterations = 48;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    AggStats stats;
    std::string report = RunAggregateIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
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

}  // namespace tinylamb
