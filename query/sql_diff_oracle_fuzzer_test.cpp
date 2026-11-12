/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_diff_oracle_fuzzer.hpp"

#include <random>
#include <string>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(DiffOracleFuzzer, TqsBitmapAgreesAndRoundTrips) {
  for (uint32_t seed = 0; seed < 16; ++seed) {
    std::mt19937 rng(seed);
    DiffOracleStats stats;
    DiffOracleTrace trace;
    const std::string report = RunTqsIteration(rng, false, &stats, &trace);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    EXPECT_EQ(stats.tqs_queries, 4);
    EXPECT_EQ(ReplayDiffOracleTrace(trace), "");
    const std::string text = SerializeDiffOracleTest(seed, trace, "");
    uint64_t parsed_seed = 0;
    DiffOracleTrace parsed;
    std::string summary;
    ASSERT_TRUE(ParseDiffOracleTest(text, &parsed_seed, &parsed, &summary))
        << text;
    EXPECT_EQ(parsed_seed, seed);
    EXPECT_EQ(parsed, trace);
  }
  // Keep the generated regression trace deterministic.
  // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
  std::mt19937 rng(3);
  DiffOracleTrace broken;
  ASSERT_EQ(RunTqsIteration(rng, false, nullptr, &broken), "");
  broken.expect[0] = "not-the-join";
  const std::string mismatch = ReplayDiffOracleTrace(broken, false);
  EXPECT_NE(mismatch.find("[TQS MISMATCH]"), std::string::npos) << mismatch;
}

TEST(DiffOracleFuzzer, SubsetAndExpressionRewriteHold) {
  int checks = 0;
  for (uint32_t seed = 0; seed < 24; ++seed) {
    std::mt19937 rng(seed + 20);
    DiffOracleStats stats;
    DiffOracleTrace trace;
    const std::string report = RunSubsetIteration(rng, false, &stats, &trace);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    checks += stats.subset_checks;
    EXPECT_EQ(ReplayDiffOracleTrace(trace), "");
  }
  EXPECT_GT(checks, 24);
}

TEST(DiffOracleFuzzer, DqpPathsAgree) {
  int compared = 0;
  for (uint32_t seed = 0; seed < 12; ++seed) {
    std::mt19937 rng(seed + 100);
    DiffOracleStats stats;
    DiffOracleTrace trace;
    const std::string report = RunDqpIteration(rng, false, &stats, &trace);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    compared += stats.dqp_queries;
  }
  EXPECT_GT(compared, 0);
}

TEST(DiffOracleFuzzer, RadarInstancesAgree) {
  int compared = 0;
  for (uint32_t seed = 0; seed < 12; ++seed) {
    std::mt19937 rng(seed + 200);
    DiffOracleStats stats;
    const std::string report = RunRadarIteration(rng, false, &stats, nullptr);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    compared += stats.radar_queries;
  }
  EXPECT_EQ(compared, 12 * 4);
}

}  // namespace tinylamb
