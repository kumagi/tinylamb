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
  int crashes = 0;
  int checkpoints = 0;
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
    crashes += stats.crashes;
    checkpoints += stats.checkpoints;
  }
  EXPECT_GT(total_checks, 0) << "no state checks ran; harness is broken";
  EXPECT_GT(indexes_created, 0) << "index DDL never ran; harness is broken";
  EXPECT_GT(tables_dropped, 0) << "DROP TABLE never ran; harness is broken";
  EXPECT_GT(commits, 0) << "no commit boundaries; harness is broken";
  EXPECT_GT(crashes, 0) << "no crash boundaries; harness is broken";
  EXPECT_GT(checkpoints, 0) << "no checkpoints ran; harness is broken";
}

// Minimal probe for the abort-undo phantom-row bug: a committed empty table,
// then a transaction that inserts rows and builds an index, aborted — a scan
// afterwards must see an empty table.
TEST(SqlSessionFuzzer, AbortWithIndexLeavesNoPhantomRows) {
  SessionTrace trace;
  trace.steps = {
      {.sql =
           "CREATE TABLE t (u INT64, c1 VARCHAR(8), c2 INT64, c3 VARCHAR(8));"},
      {.is_commit = true},
      {.sql = "INSERT INTO t VALUES (0, 'a', -2, 'x');"},
      {.sql = "INSERT INTO t VALUES (1, 'bb', 1, 'y');"},
      {.is_ddl = true, .sql = "CREATE INDEX idx ON t KEY(3)"},
      {.is_abort = true},
  };
  SessionCheck check;
  check.table = "t";
  check.after_step = trace.steps.size() - 1;
  check.expected = {};
  trace.checks.push_back(check);
  EXPECT_EQ(ReplaySessionTrace(trace, true), "");
}

// Regression (Bug 33): txn B holds an uncommitted UPDATE on a row; txn A's
// UPDATE of the same row must lose the write-intent race.  The replayed
// statement reuses B's identical compiled plan, so it executes through the
// plan-cache RetainedExecutor — which used to swallow the inner Update
// executor's kConflicts status and report success.
TEST(SqlSessionFuzzer, ConcurrentUpdateSameRowLosesIntentRace) {
  SessionTrace trace;
  trace.steps = {
      {.sql = "CREATE TABLE t (u INT64, c1 INT64, c2 INT64);"},
      {.sql = "INSERT INTO t VALUES (0, 3, 0);"},
      {.is_commit = true},
      {.is_begin = true, .txn = 1},
      {.txn = 1,
       .sql = "UPDATE t SET c2 = (CASE WHEN (c1 IS NOT NULL) THEN u ELSE c2 "
              "END) WHERE t.u = 0;"},
      {.txn = 1, .sql = "UPDATE t SET c1 = c1 WHERE t.u = 0;"},
      {.txn = 0,
       .must_fail = true,
       .sql = "UPDATE t SET c1 = c1 WHERE t.u = 0;"},
  };
  // What does txn 0 see before its update?  u=0 must still be visible
  // under A's snapshot despite B's two uncommitted writes.
  SessionCheck see;
  see.table = "t";
  see.after_step = 5;
  see.expected = {"[0, 3, 0]"};
  trace.checks.push_back(see);
  EXPECT_EQ(ReplaySessionTrace(trace, true), "");
}

// Minimal probe for the post-commit snapshot regression: txn B begins while
// txn A's DELETE is uncommitted; after A commits, B's snapshot must still
// see the deleted row.
TEST(SqlSessionFuzzer, SnapshotOutlivesConcurrentDeleteCommit) {
  SessionTrace trace;
  trace.steps = {
      {.sql = "CREATE TABLE t (u INT64, c1 INT64);"},
      {.sql = "INSERT INTO t VALUES (0, 0);"},
      {.sql = "INSERT INTO t VALUES (4, 4);"},
      {.is_commit = true},
      {.is_begin = true, .txn = 1},
      {.txn = 0, .sql = "DELETE FROM t WHERE t.u = 4;"},
      {.is_commit = true, .txn = 0},
      // B inserts into the same table: the slot freed by A's committed
      // delete is still visible under B's older snapshot and must not be
      // reused — reuse would mask the old row behind B's staged insert.
      {.txn = 1, .sql = "INSERT INTO t VALUES (5, 5);"},
  };
  SessionCheck before;
  before.table = "t";
  before.txn = 1;
  before.after_step = 5;  // pre-commit: B must see the row A staged away
  before.expected = {"[0, 0]", "[4, 4]"};
  trace.checks.push_back(before);
  SessionCheck see;
  see.table = "t";
  see.txn = 1;
  see.after_step = trace.steps.size() - 1;
  see.expected = {"[0, 0]", "[4, 4]", "[5, 5]"};
  trace.checks.push_back(see);
  EXPECT_EQ(ReplaySessionTrace(trace, true), "");
}

// Stress one seed repeatedly: nondeterministic engine bugs (e.g. abort undo
// leaving a half-visible row) may only fire under specific timing, so a
// single iteration is not enough.  Opt-in via env vars.
TEST(SqlSessionFuzzer, RepeatSeedForNondeterministicBugs) {
  const char* seed_env = std::getenv("TINYLAMB_SESSION_SEED");
  const char* count_env = std::getenv("TINYLAMB_SESSION_REPEAT");
  if (seed_env == nullptr || count_env == nullptr) {
    GTEST_SKIP() << "set TINYLAMB_SESSION_SEED + TINYLAMB_SESSION_REPEAT";
  }
  const uint64_t seed = std::strtoull(seed_env, nullptr, 10);
  const int repeat = std::atoi(count_env);
  for (int i = 0; i < repeat; ++i) {
    // Same fold as SessionFuzzTry so a .test file's -- seed: value can be
    // passed verbatim.
    std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
    SessionStats stats;
    SessionTrace trace;
    std::string report = RunSessionIteration(rng, false, &stats, &trace);
    if (!report.empty()) {
      const std::string path =
          "sql_session_fuzz-repro-" + std::to_string(seed) + ".test";
      std::ofstream out(path);
      out << SerializeSessionTest(seed, trace, report);
      std::cerr << "wrote " << path << "\n";
    }
    ASSERT_EQ(report, "") << "seed=" << seed << " iteration=" << i << "\n"
                          << report;
  }
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
