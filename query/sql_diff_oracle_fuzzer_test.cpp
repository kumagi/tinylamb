/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_diff_oracle_fuzzer.hpp"

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <random>
#include <string>

#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/unwind_guard.hpp"
#include "gtest/gtest.h"
#include "query/fuzz_scoped_db.hpp"

namespace tinylamb {

TEST(SqlDiffOracleFuzzer, TqsBitmapAgreesAndRoundTrips) {
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

TEST(SqlDiffOracleFuzzer, SubsetAndExpressionRewriteHold) {
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

TEST(SqlDiffOracleFuzzer, DqpPathsAgree) {
  int compared = 0;
  for (uint32_t seed = 0; seed < 12; ++seed) {
    std::mt19937 rng(seed + 100);
    DiffOracleStats stats;
    DiffOracleTrace trace;
    const std::string report = RunDqpIteration(rng, false, &stats, &trace);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    EXPECT_EQ(ReplayDiffOracleTrace(trace), "")
        << "generated trace fails to replay (seed " << seed << ")";
    compared += stats.dqp_queries;
  }
  EXPECT_GT(compared, 0);
}

TEST(SqlDiffOracleFuzzer, RadarInstancesAgree) {
  int compared = 0;
  int skipped = 0;
  for (uint32_t seed = 0; seed < 12; ++seed) {
    std::mt19937 rng(seed + 200);
    DiffOracleStats stats;
    const std::string report = RunRadarIteration(rng, false, &stats, nullptr);
    ASSERT_EQ(report, "") << "seed " << seed << "\n" << report;
    compared += stats.radar_queries;
    skipped += stats.skipped;
  }
  // Disk-quota pressure makes ScopedDb fail and that arm contributes no
  // query -- a skip, not a disagreement.  Require all four queries only for
  // the seeds that actually ran.
  EXPECT_EQ(compared, 4 * (12 - skipped));
}

TEST(SqlDiffOracleFuzzer, ResourcePressureCountsAsSkippedNotAgreed) {
  // A ScopedDb creation failure (disk-quota pressure) runs no query at all.
  // The report is "" there, so an arm that would bulk-add its whole query set
  // on an empty report has to be told it was skipped instead -- otherwise a
  // run that executed nothing looks like a fully-agreed sweep.
  constexpr const char* kForceFailure =
      "TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE";
  const char* current = ::tinylamb::platform::GetEnv(kForceFailure);
  const std::optional<std::string> previous =
      current != nullptr ? std::optional<std::string>(current) : std::nullopt;
  ASSERT_EQ(platform::SetEnv(kForceFailure, "1"), 0);
  // An ASSERT or an exception below must not leak the hatch into the next
  // test in this process; restore it on every exit path.
  tinylamb::UnwindGuard restore_env([&] {
    EXPECT_EQ(previous ? platform::SetEnv(kForceFailure, previous->c_str())
                       : platform::UnsetEnv(kForceFailure), 0);
  });
  // Fixed seed: the trace must stay reproducible, only the database is gone.
  std::mt19937 rng(0);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
  for (int arm = 0; arm < 4; ++arm) {
    DiffOracleStats stats;
    DiffOracleTrace trace;
    std::string report;
    switch (arm) {
      case 0:
        report = RunTqsIteration(rng, false, &stats, &trace);
        break;
      case 1:
        report = RunSubsetIteration(rng, false, &stats, &trace);
        break;
      case 2:
        report = RunDqpIteration(rng, false, &stats, &trace);
        break;
      default:
        report = RunRadarIteration(rng, false, &stats, &trace);
        break;
    }
    EXPECT_EQ(report, "") << "arm " << arm << " (" << trace.kind << ")\n"
                          << report;
    EXPECT_EQ(stats.skipped, 1) << "arm " << arm << " (" << trace.kind << ")";
    EXPECT_EQ(stats.tqs_queries + stats.subset_checks + stats.dqp_queries +
                  stats.radar_queries,
              0)
        << "arm " << arm << " (" << trace.kind
        << ") counted queries that never ran";
  }
  ASSERT_EQ(platform::UnsetEnv(kForceFailure), 0);
  // The hatch is off again: the very next sweep must run and count normally.
  DiffOracleStats stats;
  DiffOracleTrace trace;
  EXPECT_EQ(RunTqsIteration(rng, false, &stats, &trace), "") << trace.kind;
  EXPECT_EQ(stats.skipped, 0);
  EXPECT_EQ(stats.tqs_queries, 4);
}

TEST(SqlDiffOracleFuzzer, SkippedVerificationPreservesTraceAndContinues) {
  const std::filesystem::path base = std::filesystem::temp_directory_path() /
                                     ("tinylamb_diff_unverified_" + RandomString());
  const std::string regression = base.string() + ".test";
  const std::string unverified = base.string() + ".unverified";
  const std::string report = unverified + ".report.txt";
  UnwindGuard cleanup([&] {
    std::error_code error;
    std::filesystem::remove(regression, error);
    std::filesystem::remove(unverified, error);
    std::filesystem::remove(report, error);
  });

  ASSERT_EXIT(
      {
        ::tinylamb::platform::UnsetEnv("TINYLAMB_FUZZ_CONTINUE");
        ::tinylamb::platform::UnsetEnv("TINYLAMB_FUZZ_FINDINGS_DIR");
        EmitFuzzFinding("sql_diff_oracle_fuzz", 42, regression,
                        "-- tinylamb-diff-oracle v1\n", "first run differed\n",
                        "replay skipped", /*replay_skipped=*/true);
        std::_Exit(0);
      },
      ::testing::ExitedWithCode(0), "unverified trace written");
  EXPECT_FALSE(std::filesystem::exists(regression));
  EXPECT_TRUE(std::filesystem::exists(unverified));
  EXPECT_TRUE(std::filesystem::exists(report));
}

}  // namespace tinylamb
