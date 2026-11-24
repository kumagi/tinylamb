/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "common/platform/process.hpp"
#include "query/sql_session_fuzzer.hpp"

#include <filesystem>
#include <system_error>
#include <dirent.h>

#include <algorithm>
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

void RunSeededSessionRange(uint64_t begin_seed, uint64_t end_seed,
                           bool check_coverage) {
  int total_checks = 0;
  int indexes_created = 0;
  int tables_dropped = 0;
  int commits = 0;
  int crashes = 0;
  int checkpoints = 0;
  for (uint64_t seed = begin_seed; seed < end_seed; ++seed) {
    // Same fold as SessionFuzzTry (identical for seeds below 2^32): a recorded
    // full-width libFuzzer seed must replay the same session.
    std::mt19937 rng(static_cast<uint32_t>(seed ^ (seed >> 32)));
    SessionStats stats;
    std::string report = RunSessionIteration(
        rng, ::tinylamb::platform::GetEnv("TINYLAMB_SESSION_VERBOSE") != nullptr, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    total_checks += stats.checks;
    indexes_created += stats.indexes_created;
    tables_dropped += stats.tables_dropped;
    commits += stats.commits;
    crashes += stats.crashes;
    checkpoints += stats.checkpoints;
  }
  if (check_coverage) {
    EXPECT_GT(total_checks, 0) << "no state checks ran; harness is broken";
    EXPECT_GT(indexes_created, 0) << "index DDL never ran; harness is broken";
    EXPECT_GT(tables_dropped, 0) << "DROP TABLE never ran; harness is broken";
    EXPECT_GT(commits, 0) << "no commit boundaries; harness is broken";
    EXPECT_GT(crashes, 0) << "no crash boundaries; harness is broken";
    EXPECT_GT(checkpoints, 0) << "no checkpoints ran; harness is broken";
  }
}

void ReplayCommittedRegressionShard(size_t shard, size_t total_shards) {
  const char* dir = ::tinylamb::platform::GetEnv("TINYLAMB_SESSION_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "set TINYLAMB_SESSION_REGRESSION_DIR to replay .test files";
  }
  const std::vector<std::string> files = ListTestFiles(dir);
  // T3944: an existing-but-empty directory (checkout miss, wrong path) must
  // not report success while replaying zero guards; mirrors the ooc /
  // griffin / join regression replays.
  EXPECT_FALSE(files.empty()) << "no .test files in " << dir;
  for (size_t i = shard; i < files.size(); i += total_shards) {
    const std::string& path = files[i];
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

}  // namespace

// Seeded state-tracking sessions: after every statement the full-table dump
// must equal the C++ mirror.  A mismatch means some layer lost or invented a
// committed mutation.
TEST(SqlSessionFuzzer, SeededSessionsHoldStateShard0) {
  constexpr uint64_t kHalfIterations = 32;
  const char* only = ::tinylamb::platform::GetEnv("TINYLAMB_SESSION_ONLY_SEED");
  // A seed asked for via TINYLAMB_SESSION_ONLY_SEED runs even when it
  // lies outside the sweep range (libFuzzer seeds are full-width; the
  // old uint32 stoul + 64-iteration loop turned such asks into "run
  // nothing", tripping the sanity floor below and hiding the repro).
  uint64_t only_seed = 0;
  if (only != nullptr) {
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(only, &end, 10);
    if (end == only || *end != '\0' || errno == ERANGE || only[0] == '-') {
      // An invalid repro request must not silently run zero seeds.
      ADD_FAILURE() << "invalid TINYLAMB_SESSION_ONLY_SEED: " << only;
    } else {
      only_seed = static_cast<uint64_t>(parsed);
    }
  }
  const uint64_t first = only != nullptr ? only_seed : 0;
  const uint64_t last =
      only != nullptr ? (only_seed == UINT64_MAX ? only_seed : only_seed + 1)
                      : kHalfIterations;
  // The coverage floor guards the sweep; a single-seed repro run
  // (TINYLAMB_SESSION_ONLY_SEED) legitimately exercises a fraction of
  // the boundary types.
  RunSeededSessionRange(first, last, only == nullptr);
}

TEST(SqlSessionFuzzer, SeededSessionsHoldStateShard1) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_SESSION_ONLY_SEED") != nullptr) {
    return;
  }
  RunSeededSessionRange(32, 64, true);
}

TEST(SqlSessionFuzzer, ResourcePressureSkipsWithoutFailure) {
  ASSERT_EQ(::tinylamb::platform::SetEnv("TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE", "1", 1), 0);
  // The seed is fixed on purpose: this trace must be reproducible.
  std::mt19937 rng(0);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
  SessionTrace trace;
  EXPECT_EQ(RunSessionIteration(rng, false, nullptr, &trace), "");
  EXPECT_TRUE(trace.steps.empty());
  EXPECT_EQ(::tinylamb::platform::UnsetEnv("TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE"), 0);
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

// Regression guard for concurrent transactions mutating the same secondary
// non-unique index key followed by abort, heap slot reuse, commit, and crash
// recovery:
// 1. Aborting txn 0 after txn 1 touched the same non-unique key must restore
//    txn 0's deleted entry and remove txn 0's aborted insert so a subsequent
//    INSERT that reuses txn 0's heap slot under a different index key is not
//    emitted twice by IndexScan.
// 2. Committing txn 1 while txn 0's insert on the same key is uncommitted (and
//    later aborted or lost across crash recovery) must not bake txn 0's dead
//    RowPosition into committed_mem / recovered LSM state where IndexOnlyScan
//    would over-count it.
TEST(SqlSessionFuzzer,
     DualTxnNonUniqueIndexCommitAbortSlotReuseAndCrashRecovery) {
  SessionTrace trace;
  trace.steps = {
      {.sql = "CREATE TABLE t (u INT64, c1 INT64, c2 INT64);"},
      {.sql = "CREATE INDEX idx_t_c1 ON t (c1);"},
      {.sql = "INSERT INTO t VALUES (1, 10, 100);"},
      {.sql = "INSERT INTO t VALUES (2, 10, 200);"},
      {.is_commit = true},
      {.is_begin = true, .txn = 0},
      {.is_begin = true, .txn = 1},
      // Txn 0 deletes u=1 (key c1=10) and inserts u=3 (also key c1=10).
      {.txn = 0, .sql = "DELETE FROM t WHERE t.u = 1;"},
      {.txn = 0, .sql = "INSERT INTO t VALUES (3, 10, 300);"},
      // Txn 1 concurrently inserts u=4 with the same secondary key c1=10.
      {.txn = 1, .sql = "INSERT INTO t VALUES (4, 10, 400);"},
      // Txn 1 commits while txn 0 is still uncommitted; txn 0 then aborts.
      {.is_commit = true, .txn = 1},
      {.is_abort = true, .txn = 0},
      // New txn reuses the heap slot vacated by txn 0's aborted u=3, but with
      // a DIFFERENT secondary index key c1=20.
      {.is_begin = true, .txn = 0},
      {.txn = 0, .sql = "INSERT INTO t VALUES (5, 20, 500);"},
      {.is_commit = true, .txn = 0},
      // Crash and recover: neither IndexScan nor IndexOnlyScan may see dead or
      // duplicate entries from txn 0's aborted mutations on c1=10.
      {.is_crash = true},
  };
  for (size_t step_idx : {size_t{10}, size_t{11}, size_t{14}, size_t{15}}) {
    SessionCheck chk;
    chk.table = "t";
    chk.txn = 0;
    chk.after_step = step_idx;
    if (step_idx == 10) {
      chk.expected = {"[2, 10, 200]", "[3, 10, 300]"};
    } else if (step_idx == 11) {
      chk.expected = {"[1, 10, 100]", "[2, 10, 200]", "[4, 10, 400]"};
    } else {
      chk.expected = {"[1, 10, 100]", "[2, 10, 200]", "[4, 10, 400]",
                      "[5, 20, 500]"};
    }
    trace.checks.push_back(chk);
  }
  EXPECT_EQ(ReplaySessionTrace(trace, true), "");
}

// Stress one seed repeatedly: nondeterministic engine bugs (e.g. abort undo
// leaving a half-visible row) may only fire under specific timing, so a
// single iteration is not enough.  Opt-in via env vars.
TEST(SqlSessionFuzzer, RepeatSeedForNondeterministicBugs) {
  const char* seed_env = ::tinylamb::platform::GetEnv("TINYLAMB_SESSION_SEED");
  const char* count_env = ::tinylamb::platform::GetEnv("TINYLAMB_SESSION_REPEAT");
  if (seed_env == nullptr || count_env == nullptr) {
    GTEST_SKIP() << "set TINYLAMB_SESSION_SEED + TINYLAMB_SESSION_REPEAT";
  }
  char* seed_end = nullptr;
  errno = 0;
  const unsigned long long parsed_seed = std::strtoull(seed_env, &seed_end, 10);
  if (errno != 0 || seed_end == seed_env || *seed_end != '\0') {
    GTEST_SKIP() << "invalid TINYLAMB_SESSION_SEED";
  }
  char* count_end = nullptr;
  errno = 0;
  const long parsed_repeat = std::strtol(count_env, &count_end, 10);
  if (errno != 0 || count_end == count_env || *count_end != '\0' ||
      parsed_repeat <= 0 || parsed_repeat > std::numeric_limits<int>::max()) {
    GTEST_SKIP() << "invalid TINYLAMB_SESSION_REPEAT";
  }
  const auto seed = static_cast<uint64_t>(parsed_seed);
  const int repeat = static_cast<int>(parsed_repeat);
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
      out.close();
      // T4514: the repro file is the promotion path to a regression guard;
      // never claim it landed when the open/write failed.
      if (out) {
        std::cerr << "wrote " << path << "\n";
      } else {
        std::cerr << "FAILED to write repro file " << path
                  << " (disk full or unwritable cwd) -- save seed=" << seed
                  << " manually\n";
      }
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

TEST(SqlSessionFuzzer,
     PlanCacheRetainsSubqueryTablesAcrossDeleteReplayAndCheckpoint) {
  SessionTrace trace;
  trace.steps = {
      {.sql =
           "CREATE TABLE tbl2 (u INT64, c1 INT64, c2 INT64, c3 VARCHAR(8));"},
      {.sql =
           "CREATE TABLE tbl4 (u INT64, c1 INT64, c2 INT64, c3 VARCHAR(8));"},
      {.sql = "INSERT INTO tbl2 VALUES (0, 1, 10, 'k1');"},
      {.sql = "INSERT INTO tbl4 VALUES (0, 7, 70, 'k1');"},
      {.sql = "INSERT INTO tbl4 VALUES (1, 8, 80, 'k2');"},
      {.is_commit = true},
      {.sql =
           "DELETE FROM tbl4 WHERE (EXISTS (SELECT 1 FROM tbl2 WHERE tbl2.c1 = "
           "tbl4.c1));"},
      {.is_checkpoint = true},
      {.is_abort = true},
      {.sql =
           "DELETE FROM tbl4 WHERE (EXISTS (SELECT 1 FROM tbl2 WHERE tbl2.c1 = "
           "tbl4.c1));"},
      {.sql =
           "DELETE FROM tbl4 WHERE (EXISTS (SELECT 1 FROM tbl2 WHERE tbl2.c3 = "
           "tbl4.c3));"},
  };
  SessionCheck remaining;
  remaining.table = "tbl4";
  remaining.after_step = trace.steps.size() - 1;
  remaining.expected = {"[1, 8, 80, \"k2\"]"};
  trace.checks.push_back(remaining);
  EXPECT_EQ(ReplaySessionTrace(trace, true), "");
}

// Any committed sql_session_fuzz-*.test file replays as a permanent
// regression guard: the recorded dumps must hold now.
class SqlSessionRegressionTest : public ::testing::TestWithParam<size_t> {};
constexpr size_t kRegressionShards = 16;

TEST_P(SqlSessionRegressionTest, ReplayCommittedRegressionFiles) {
  ReplayCommittedRegressionShard(GetParam(), kRegressionShards);
}

// Name the instances after the old Shard0..3 tests so ctest -R filters and
// grep on the previous names still select (rather than silently skip) the
// regression replay coverage.  The suite prefix changed to
// RegressionShards/SqlSessionRegressionTest with the parameterized rewrite.
INSTANTIATE_TEST_SUITE_P(RegressionShards, SqlSessionRegressionTest,
                         ::testing::Range(size_t{0}, kRegressionShards),
                         [](const ::testing::TestParamInfo<size_t>& info) {
                           return "ReplayCommittedRegressionFilesShard" +
                                  std::to_string(info.param);
                         });

}  // namespace tinylamb
