/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_ooc_fuzzer.hpp"
#include "query/sql_ooc_query_gen.hpp"
#include <stdexcept>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

TEST(OocRngTest, EmptyChoicesRaiseAndSingletonReturnsMember) {
  OocRng rng(nullptr, 0, 42);
  EXPECT_THROW(rng.OneOf(std::vector<int>{}), std::out_of_range);
  const std::vector<int> one{7};
  EXPECT_EQ(&rng.OneOf(one), &one.front());
}

bool Verbose() { return std::getenv("TINYLAMB_OOC_FUZZ_VERBOSE") != nullptr; }

std::vector<std::string> ListTestFiles(const std::string& dir) {
  std::vector<std::string> files;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    const std::string path = entry.path().string();
    if (path.ends_with(".test")) {
      files.push_back(path);
    }
  }
  std::ranges::sort(files);
  return files;
}

int FlavorIndex(const std::string& name) {
  const std::vector<std::string> names = OocFlavorNames();
  const auto it = std::ranges::find(names, name);
  return it == names.end() ? -1 : static_cast<int>(it - names.begin());
}

// Each gtest case is its own ctest unit (60 s cap, also in Debug and ASan),
// so each shard runs a single seeded session to stay well below the timeout
// even if one expensive query overshoots the nominal per-session budget.
constexpr uint64_t kSeedsPerShard = 1;
constexpr double kSecondsPerSession = 7.0;

// `-- failure:` header in the form scripts/ooc_fuzz.py files findings under
// ("<signature> :: <report on one line>"), so `ooc_fuzz.py replay` can tell
// STILL-FAILS from CHANGED for a trace a gtest shard wrote.
std::string FailureHeader(const std::string& signature,
                          const std::string& report) {
  std::string line = signature + " :: " + report;
  std::ranges::replace(line, '\n', ' ');
  return line.substr(0, 600);
}

std::string TracePath(uint64_t seed) {
  return "sql_ooc_fuzz-seed-" + std::to_string(seed) + ".test";
}

// A session op that never returns would otherwise end as a bare ctest
// timeout with no trace.  The watchdog keeps the ops executed so far and,
// when one of them runs past the limit, writes the trace, names the op and
// fails the test process (TINYLAMB_OOC_FUZZ_OP_TIMEOUT seconds, default 45:
// below the 60 s cap of a ctest unit; 0 disables it).
class OpWatchdog {
 public:
  explicit OpWatchdog(uint64_t seed) : seed_(seed) {
    const char* env = std::getenv("TINYLAMB_OOC_FUZZ_OP_TIMEOUT");
    timeout_ = 45.0;
    if (env != nullptr) {
      char* end = nullptr;
      const double parsed = std::strtod(env, &end);
      if (end != env && *end == '\0' && std::isfinite(parsed) && parsed > 0.0) {
        timeout_ = parsed;
      }
    }
    if (timeout_ > 0) {
      thread_ = std::thread([this]() { Run(); });
    }
  }
  OpWatchdog(const OpWatchdog&) = delete;
  OpWatchdog& operator=(const OpWatchdog&) = delete;
  OpWatchdog(OpWatchdog&&) = delete;
  OpWatchdog& operator=(OpWatchdog&&) = delete;
  ~OpWatchdog() {
    stop_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void SetProfile(const OocProfile& profile) {
    std::scoped_lock lock(mu_);
    trace_.profile = profile;
  }
  void Tick(const std::string& op) {
    std::scoped_lock lock(mu_);
    trace_.ops.push_back(op);
    started_ = std::chrono::steady_clock::now();
    armed_ = true;
  }

 private:
  void Run() {
    while (!stop_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::scoped_lock lock(mu_);
      if (!armed_ || std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - started_)
                             .count() < timeout_) {
        continue;
      }
      const std::string& op = trace_.ops.back();
      const std::string signature = "hang / " + op.substr(0, op.find(' '));
      const std::string report = "op ran longer than " +
                                 std::to_string(static_cast<int>(timeout_)) +
                                 " s (hang or livelock): " + op.substr(0, 400);
      {
        std::ofstream out(TracePath(seed_));
        out << SerializeOocTest(seed_, trace_,
                                FailureHeader(signature, report));
        if (!out) {
          std::cerr << "failed to write trace " << TracePath(seed_) << "\n";
        }
      }
      std::cerr << "failing seed=" << seed_ << " (trace attempted at "
                << TracePath(seed_) << ")\n"
                << report << "\n"
                << std::flush;
      std::_Exit(1);
    }
  }

  uint64_t seed_;
  double timeout_{0};
  std::mutex mu_;
  OocTrace trace_;
  std::chrono::steady_clock::time_point started_;
  bool armed_{false};
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

// Runs `count` seeded sessions of one flavour (-1 = drawn from the seed) and
// returns the merged statistics; a failing seed leaves its trace behind.
OocStats RunSeeds(uint64_t first, uint64_t count, int flavor,
                  double budget_seconds = kSecondsPerSession,
                  bool* any_pool_capped = nullptr) {
  OocStats total;
  for (uint64_t seed = first; seed < first + count; ++seed) {
    OocStats stats;
    OocTrace trace;
    OocRunOptions options;
    options.verbose = Verbose();
    options.budget_seconds = budget_seconds;
    options.flavor = flavor;
    OpWatchdog watchdog(seed);
    options.on_profile = [&](const OocProfile& p) {
      if (any_pool_capped != nullptr && p.pool_bytes > 0) {
        *any_pool_capped = true;
      }
      watchdog.SetProfile(p);
    };
    options.on_op = [&](const std::string& op) { watchdog.Tick(op); };
    const std::string report =
        RunOocSession(nullptr, 0, seed, options, &stats, &trace);
    if (!report.empty()) {
      {
        std::ofstream out(TracePath(seed));
        out << SerializeOocTest(
            seed, trace, FailureHeader(OocFailureSignature(report), report));
        if (!out) {
          ADD_FAILURE() << "failed to write trace " << TracePath(seed);
        }
      }
      ADD_FAILURE() << "failing seed=" << seed << " (trace written to "
                    << TracePath(seed) << ")\n"
                    << report;
      break;
    }
    total.Merge(stats);
  }
  return total;
}

void RunShard(uint64_t shard) {
  const char* only = std::getenv("TINYLAMB_OOC_FUZZ_ONLY_SEED");
  if (only != nullptr) {
    if (shard != 0) {
      GTEST_SKIP() << "single-seed run handled by shard 0";
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(only, &end, 10);
    if (errno != 0 || end == only || *end != '\0') {
      GTEST_SKIP() << "invalid TINYLAMB_OOC_FUZZ_ONLY_SEED";
    }
    RunSeeds(parsed, 1, -1);
    return;
  }
  bool any_pool_capped = false;
  const OocStats total = RunSeeds(shard * kSeedsPerShard, kSeedsPerShard, -1,
                                  kSecondsPerSession, &any_pool_capped);
  if (::testing::Test::HasFailure()) {
    return;
  }
  EXPECT_GT(total.checks, 0) << "no checks ran; harness is broken";
  EXPECT_GT(total.commits, 0) << "no commits ran; harness is broken";
  EXPECT_GT(total.crashes, 0) << "epilogue crash never ran";
  EXPECT_GT(total.rows_inserted, 0) << "no data was loaded";
  if (any_pool_capped) {
    EXPECT_GT(total.evicted_pages, 0U)
        << "no page was ever evicted: the shrunken pool is not biting";
  }
}

}  // namespace

TEST(SqlOocFuzzer, SeededSessionsShard0) { RunShard(0); }
TEST(SqlOocFuzzer, SeededSessionsShard1) { RunShard(1); }
TEST(SqlOocFuzzer, SeededSessionsShard2) { RunShard(2); }
TEST(SqlOocFuzzer, SeededSessionsShard3) { RunShard(3); }
TEST(SqlOocFuzzer, SeededSessionsShard4) { RunShard(4); }
TEST(SqlOocFuzzer, SeededSessionsShard5) { RunShard(5); }
TEST(SqlOocFuzzer, SeededSessionsShard6) { RunShard(6); }
TEST(SqlOocFuzzer, SeededSessionsShard7) { RunShard(7); }
TEST(SqlOocFuzzer, SeededSessionsShard8) { RunShard(8); }
TEST(SqlOocFuzzer, SeededSessionsShard9) { RunShard(9); }
TEST(SqlOocFuzzer, SeededSessionsShard10) { RunShard(10); }
TEST(SqlOocFuzzer, SeededSessionsShard11) { RunShard(11); }
TEST(SqlOocFuzzer, SeededSessionsShard12) { RunShard(12); }
TEST(SqlOocFuzzer, SeededSessionsShard13) { RunShard(13); }
TEST(SqlOocFuzzer, SeededSessionsShard14) { RunShard(14); }
TEST(SqlOocFuzzer, SeededSessionsShard15) { RunShard(15); }
TEST(SqlOocFuzzer, SeededSessionsShard16) { RunShard(16); }
TEST(SqlOocFuzzer, SeededSessionsShard17) { RunShard(17); }
TEST(SqlOocFuzzer, SeededSessionsShard18) { RunShard(18); }
TEST(SqlOocFuzzer, SeededSessionsShard19) { RunShard(19); }
TEST(SqlOocFuzzer, SeededSessionsShard20) { RunShard(20); }
TEST(SqlOocFuzzer, SeededSessionsShard21) { RunShard(21); }
TEST(SqlOocFuzzer, SeededSessionsShard22) { RunShard(22); }
TEST(SqlOocFuzzer, SeededSessionsShard23) { RunShard(23); }

// The query flavour must really drive the budget-invariance oracle through
// spilling executions, not just generate SQL.  Allow a slightly larger
// per-session wall budget so concurrent `-j` Debug runs reach pipeline sinks
// reliably while staying well under ctest's 60 s cap.
TEST(SqlOocFuzzer, QueryFlavorReachesTheSpillPaths) {
  const OocStats total = RunSeeds(1000, 2, FlavorIndex("query"), 12.0);
  if (::testing::Test::HasFailure()) {
    return;
  }
  EXPECT_GT(total.diff_queries, 20) << "too few differential queries";
  EXPECT_GT(total.diff_variants, total.diff_tolerated * 2)
      << "most pressure variants were tolerated failures: the oracle is blind";
  EXPECT_GT(total.partial_cursors, 0) << "no cursor was closed early";
  EXPECT_GT(total.spill_bytes, 0U) << "nothing ever spilled";
  EXPECT_GT(total.pure_pipeline_queries, 0)
      << "no statement ran on the pipeline sinks (legacy_source_ms = 0)";
}

// The inject flavour must reach failure points and see them fire.
TEST(SqlOocFuzzer, InjectFlavorSweepsFailurePoints) {
  const OocStats total = RunSeeds(2000, 2, FlavorIndex("inject"));
  if (::testing::Test::HasFailure()) {
    return;
  }
  EXPECT_GT(total.sweeps, 0) << "no sweep ran";
  EXPECT_GT(total.sweep_fired, 0) << "no injected failure ever fired";
}

// The huge-transaction flavour must push uncommitted pages out of the pool.
TEST(SqlOocFuzzer, BigTxnFlavorSpillsUncommittedPages) {
  const OocStats total = RunSeeds(3000, 2, FlavorIndex("bigtxn"));
  if (::testing::Test::HasFailure()) {
    return;
  }
  EXPECT_GT(total.uncommitted_spill_evictions, 0U)
      << "no uncommitted page was ever evicted to the private spill area";
}

// The textual trace is the replay contract: it must round-trip exactly.
TEST(SqlOocFuzzer, TraceRoundTrips) {
  OocTrace trace;
  trace.profile.pool_bytes = 65536;
  trace.profile.op_memory_bytes = 262144;
  trace.profile.always_spill = true;
  trace.profile.force_relational = true;
  trace.ops = {"create t0 5 10 100",
               "ins_bulk t0 1 50 7",
               "commit",
               "q ord=1 heavy=0 take=3 nv=2 vs=9 ex=1 snap=0 :: SELECT k FROM "
               "t0 ORDER BY k LIMIT 5",
               "sweep mem 20 :: check full t0",
               "check full t0"};
  const std::string text = SerializeOocTest(42, trace, "summary");
  uint64_t seed = 0;
  OocTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseOocTest(text, &seed, &parsed, &summary));
  EXPECT_EQ(seed, 42U);
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(summary, "summary");
}

TEST(SqlOocFuzzer, SignaturesBucketTheSameBugTogether) {
  EXPECT_EQ(OocFailureSignature(
                "op 12 `check full t3`: check full(t3) mismatch: SELECT k "
                "FROM t3\n  expected 5 rows, engine returned 4\n"),
            OocFailureSignature("check full(t0) mismatch: SELECT k FROM t0\n"));
  EXPECT_EQ(OocFailureSignature(
                "INSERT SELECT failed unexpectedly: INSERT INTO t1 ...\n"
                "  stream: Conflicts: insert failed on table t1\n"),
            "INSERT SELECT failed unexpectedly | stream: Conflicts");
  EXPECT_EQ(OocFailureSignature(
                "sweep mem `upd t0 all 0 0 1 0 vset 9 0 none 0 0` with failure "
                "#7: post-post-failure abort count(t0) mismatch: SELECT 1\n"),
            "sweep mem / post-post-failure abort count(t#) mismatch");
  EXPECT_NE(OocFailureSignature("crash recovery failed to reopen\n"),
            OocFailureSignature("clean reopen failed\n"));
}

// The minimizer must cut a long failing trace down to the ops the failure
// needs (`selftest` reports a fake finding once a table holds enough rows).
TEST(SqlOocFuzzer, MinimizerKeepsOnlyWhatTheFailureNeeds) {
  OocTrace trace;
  trace.profile.pool_bytes = 64 << 10;
  trace.profile.op_memory_bytes = 64 << 10;
  trace.profile.always_spill = true;
  trace.ops = {
      "create t0 5 10 40",
      "create t1 3 10 40",
      "ins_bulk t1 1 60 3",
      "commit",
      "ins_bulk t0 1 30 1",
      "check full t1",
      "upd t1 all 0 0 1 0 vset 9 0 none 0 0",
      "ins_bulk t0 31 55 2",
      "evict 0",
      "checkpoint",
      "check agg t0",
      "ins_bulk t0 56 80 4",
      "commit",
      "selftest t0 70",
      "check full t0",
      "crash",
  };
  const std::string report = ReplayOocTrace(trace, Verbose());
  ASSERT_NE(report.find("selftest threshold reached"), std::string::npos)
      << report;
  std::string final_report;
  const OocTrace minimized =
      MinimizeOocTrace(trace, report, /*budget_seconds=*/40, &final_report);
  EXPECT_NE(final_report.find("selftest threshold reached"), std::string::npos);
  // create t0, three inserts (30 + 25 + 25 >= 70 needs all of them), selftest.
  EXPECT_EQ(minimized.ops.size(), 5U) << SerializeOocTest(0, minimized, "");
  EXPECT_EQ(minimized.profile, OocProfile{});
}

// A hand-written trace touching every op kind: guards the interpreter
// itself (a malformed-op regression would otherwise only show up as a
// fuzzer "finding").
TEST(SqlOocFuzzer, HandWrittenTraceHolds) {
  OocTrace trace;
  trace.profile.pool_bytes = 32 << 10;
  trace.profile.op_memory_bytes = 64 << 10;
  trace.profile.always_spill = true;
  trace.ops = {
      "create t0 7 50 900",
      "create t1 3 4000 3000",
      "create t2 1 2 3 2 2 1",
      "index t0 ix0 uk",
      "ins_bulk t0 1 400 11",
      "ins_values t0 401 410 3",
      "ins_bulk t1 1 120 5",
      "ins_bulk t2 1 300 4",
      "commit",
      "index t1 ix1 g",
      "index t0 ix2 tk",
      "check full t0",
      "check scan t1",
      "check full t2",
      "evict 0",
      "upd t0 range 10 200 1 0 pad 1500 3 vadd 5 0",
      "upd t1 grp 1 0 1 0 gmod 4 0 tag 2 0",
      "evict 0",
      "check range t0 1 500 40 3",
      "checkpoint",
      "snap open ro",
      "del t0 rmod 1 300 3 1",
      "check agg t0",
      "snap check t0",
      "q ord=0 heavy=0 take=-1 nv=1 vs=1 ex=0 snap=1 :: SELECT k, v FROM t0",
      "commit",
      "snap check t0",
      "snap close",
      "ins_copy t1 t0 1 100 1000",
      "ins_dup t0 5",
      "check window t0",
      "check strag t1",
      "check distinct t0",
      "check aggk t1",
      "check sortpad t1 20",
      "check point t0 7",
      "check grp t1 2",
      "check tag t0 2 10",
      "check joink t0 t1",
      "check joing t0 t1",
      "check leftk t0 t1",
      "check setop t0 t1 0",
      "check setop t0 t1 1",
      "check setop t0 t1 2",
      "check topn t0 25 3",
      "check winrows t2",
      "check distrows t0",
      "check joinrows t0 t1",
      "check fullk t0 t2",
      "q ord=1 heavy=0 take=-1 nv=3 vs=7 ex=1 snap=0 :: SELECT a.g, COUNT(*), "
      "SUM(b.v) FROM t0 a JOIN t2 b ON a.k = b.k GROUP BY a.g ORDER BY a.g",
      "q ord=1 heavy=0 take=4 nv=2 vs=8 ex=0 snap=0 :: SELECT k, v FROM t0 "
      "ORDER BY v DESC, k LIMIT 50",
      "q ord=0 heavy=0 take=5 nv=1 vs=3 ex=1 snap=0 :: SELECT k, ROW_NUMBER() "
      "OVER (PARTITION BY g ORDER BY k) FROM t0",
      "sweep mem 40 :: check joinrows t0 t1",
      "sweep io 20 :: q ord=0 heavy=0 take=-1 nv=1 vs=5 ex=0 snap=0 :: SELECT "
      "DISTINCT g, t FROM t0",
      "sweep mem 30 :: upd t0 range 1 50 1 0 vadd 1 0 none 0 0",
      "knob pool 16384",
      "knob spill 0",
      "knob opmem 0",
      "knob route 1",
      "check count t0",
      "knob route 0",
      "inject mem 3",
      "upd t0 all 0 0 1 0 vset 9 0 none 0 0",
      "abort",
      "analyze t0",
      "commit",
      "compact t1",
      "upd t1 range 1 50 1 0 vadd 1 0 none 0 0",
      "check full t1",
      "crash",
      "check full t0",
      "check full t1",
      "ins_bulk t0 2000 2300 9",
      "commit",
      "reopen",
      "check full t0",
      "drop t1",
      "commit",
      "crash",
      "check full t0",
  };
  OocStats stats;
  EXPECT_EQ(ReplayOocTrace(trace, Verbose(), &stats), "");
  EXPECT_GT(stats.evictions, 0);
  EXPECT_GT(stats.evicted_pages, 0U);
  EXPECT_GT(stats.diff_variants, 0);
  EXPECT_GT(stats.sweep_points, 0);
}

// Every committed regression file must keep holding.
class OocCommittedRegressionTest : public ::testing::TestWithParam<size_t> {};

TEST_P(OocCommittedRegressionTest, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_OOC_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "set TINYLAMB_OOC_REGRESSION_DIR to replay .test files";
  }
  const auto paths = ListTestFiles(dir);
  EXPECT_FALSE(paths.empty()) << "no .test files in " << dir;
  for (size_t i = GetParam(); i < paths.size(); i += 16) {
    const std::string& path = paths[i];
    std::ifstream file(path);
    const std::string text((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
    uint64_t seed = 0;
    OocTrace trace;
    std::string summary;
    ASSERT_TRUE(ParseOocTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    EXPECT_EQ(ReplayOocTrace(trace, Verbose()), "")
        << "regression " << path << " still reproduces (" << summary << ")";
  }
}

INSTANTIATE_TEST_SUITE_P(RegressionShards, OocCommittedRegressionTest,
                         ::testing::Range(size_t{0}, size_t{16}));

}  // namespace tinylamb
