/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_join_fuzzer.hpp"

#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(SqlJoinFuzzer, SeededJoinsMatchMirror) {
  int queries = 0;
  int left_joins = 0;
  const char* scale = std::getenv("TINYLAMB_FUZZ_ITERS");
  const int kIterations = (scale != nullptr) ? std::atoi(scale) : 48;
  for (uint32_t seed = 0; seed < static_cast<uint32_t>(kIterations); ++seed) {
    std::mt19937 rng(seed);
    JoinStats stats;
    std::string report = RunJoinIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    queries += stats.queries;
    left_joins += stats.left_joins;
  }
  EXPECT_GT(queries, kIterations) << "join oracle almost never ran";
  EXPECT_GT(left_joins, 0) << "LEFT JOIN never exercised";
}

TEST(SqlJoinFuzzer, TestFileRoundTripDetectsMismatch) {
  JoinTrace trace;
  trace.setup = {
      "CREATE TABLE fact (u INT64, k INT64, a INT64);",
      "CREATE TABLE dim (k INT64, tag VARCHAR(8));",
      "INSERT INTO fact VALUES (0, 1, 10);",
      "INSERT INTO dim VALUES (1, 'x');",
  };
  trace.queries = {
      {"SELECT f.u FROM fact f JOIN dim d ON f.k = d.k;", {"|0|,"}},
      {"SELECT f.u, d.tag FROM fact f LEFT JOIN dim d ON f.k = d.k;",
       {"|0|,|\"x\"|,"}},
  };
  const std::string text =
      SerializeJoinTest(3, trace, "synthetic for pipeline test");
  uint64_t seed = 0;
  JoinTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseJoinTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(seed, 3U);
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(ReplayJoinTrace(parsed), "") << "honest trace mismatched";

  JoinTrace broken = trace;
  broken.queries[0].expected = {"|7|,"};
  const std::string broken_text = SerializeJoinTest(4, broken, "");
  uint64_t seed2 = 0;
  JoinTrace parsed2;
  std::string summary2;
  ASSERT_TRUE(ParseJoinTest(broken_text, &seed2, &parsed2, &summary2));
  EXPECT_NE(ReplayJoinTrace(parsed2), "") << "tampered trace replayed clean";
}

}  // namespace tinylamb
