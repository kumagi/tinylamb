/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstdint>
#include <cstdlib>
#include <dirent.h>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "query/sql_oracle_fuzzer.hpp"

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

// Seeded metamorphic-oracle runs (TLP + NoREC, see sql_oracle_fuzzer.hpp).
// A non-empty report means the engine returned inconsistent results for a
// query and its oracle-equivalent forms: a logic bug in the optimizer or the
// executor.  Skips (unsupported generated syntax) are allowed but counted so
// a silently-all-skipping harness fails this test.
TEST(SqlOracleFuzzer, SeededIterationsHoldOracles) {
  int tlp_ran = 0;
  int norec_ran = 0;
  constexpr int kIterations = 64;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    OracleIterationStats stats;
    std::string report = RunOracleIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    tlp_ran += stats.tlp_ran ? 1 : 0;
    norec_ran += stats.norec_ran ? 1 : 0;
  }
  EXPECT_GT(tlp_ran, kIterations / 2)
      << "TLP oracle almost never ran; harness is broken";
  EXPECT_GT(norec_ran, kIterations / 2)
      << "NoREC oracle almost never ran; harness is broken";
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
  EXPECT_NE(ReplayOracleTrace(parsed, true), "")
      << "inconsistent trace replayed clean; pipeline is broken";

  // The consistent twin: original query matches the partition union.
  OracleTrace consistent = broken;
  consistent.tlp[0] = "SELECT * FROM t0 WHERE (a >= 0) AND NOT (b > 100);";
  const std::string consistent_text = SerializeOracleTest(8, consistent, "");
  uint64_t seed2 = 0;
  OracleTrace parsed2;
  std::string summary2;
  ASSERT_TRUE(ParseOracleTest(consistent_text, &seed2, &parsed2, &summary2));
  EXPECT_EQ(ReplayOracleTrace(parsed2), "")
      << "consistent trace reported a mismatch";
}

// Any committed sql_oracle_fuzz-*.test file replays as a permanent
// regression guard: the recorded oracles must hold now (i.e. the bug the
// file captured has been fixed).  Point TINYLAMB_ORACLE_REGRESSION_DIR at a
// directory to enable; with no directory the test passes trivially.
TEST(SqlOracleFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_ORACLE_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "set TINYLAMB_ORACLE_REGRESSION_DIR to replay .test files";
  }
  for (const std::string& path : ListTestFiles(dir)) {
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

}  // namespace tinylamb
