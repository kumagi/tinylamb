/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_session_fuzzer.hpp"

#include <dirent.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
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
    if (name.size() > 5 && name.ends_with(".test")) {
      std::string path = dir;
      path += '/';
      path += name;
      files.push_back(std::move(path));
    }
  }
  closedir(handle);
  return files;
}

}  // namespace

// Seeded state-tracking sessions: after every statement the full-table dump
// must equal the C++ mirror.  A mismatch means some layer lost or invented a
// committed mutation.
TEST(SqlSessionFuzzer, SeededSessionsHoldState) {
  int total_checks = 0;
  int indexes_created = 0;
  int tables_dropped = 0;
  int commits = 0;
  constexpr int kIterations = 24;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    SessionStats stats;
    std::string report = RunSessionIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    total_checks += stats.checks;
    indexes_created += stats.indexes_created;
    tables_dropped += stats.tables_dropped;
    commits += stats.commits;
  }
  EXPECT_GT(total_checks, 0) << "no state checks ran; harness is broken";
  EXPECT_GT(indexes_created, 0) << "index DDL never ran; harness is broken";
  EXPECT_GT(tables_dropped, 0) << "DROP TABLE never ran; harness is broken";
  EXPECT_GT(commits, 0) << "no commit boundaries; harness is broken";
}

// Round trip: a trace with a tampered expected dump must serialize, parse and
// replay into a mismatch; the honest trace must replay clean.
TEST(SqlSessionFuzzer, TestFileRoundTripDetectsMismatch) {
  SessionTrace trace;
  trace.steps = {
      {.sql = "CREATE TABLE tbl0 (u INT64, c1 INT64);"},
      {.sql = "INSERT INTO tbl0 VALUES (0, 1);"},
      {.sql = "INSERT INTO tbl0 VALUES (1, 2);"},
  };
  SessionCheck check;
  check.table = "tbl0";
  check.after_step = 2;
  check.expected = {"[0, 1]", "[1, 2]"};
  trace.checks.push_back(check);

  const std::string text =
      SerializeSessionTest(5, trace, "synthetic for pipeline test");
  uint64_t seed = 0;
  SessionTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseSessionTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(seed, 5U);
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(ReplaySessionTrace(parsed), "")
      << "honest trace reported a mismatch";

  SessionTrace broken = trace;
  broken.checks[0].expected[1] = "[1, 999]";
  const std::string broken_text = SerializeSessionTest(6, broken, "");
  uint64_t seed2 = 0;
  SessionTrace parsed2;
  std::string summary2;
  ASSERT_TRUE(ParseSessionTest(broken_text, &seed2, &parsed2, &summary2));
  EXPECT_NE(ReplaySessionTrace(parsed2), "")
      << "tampered dump replayed clean; pipeline is broken";
}

// Any committed sql_session_fuzz-*.test file replays as a permanent
// regression guard: the recorded dumps must hold now.
TEST(SqlSessionFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_SESSION_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "set TINYLAMB_SESSION_REGRESSION_DIR to replay .test files";
  }
  for (const std::string& path : ListTestFiles(dir)) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    uint64_t seed = 0;
    SessionTrace trace;
    std::string summary;
    ASSERT_TRUE(ParseSessionTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    std::string report = ReplaySessionTrace(trace, false);
    EXPECT_EQ(report, "") << "regression " << path << " still reproduces:\n"
                          << report << summary;
  }
}

}  // namespace tinylamb
namespace tinylamb {}  // namespace tinylamb
