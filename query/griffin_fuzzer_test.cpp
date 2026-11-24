/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/griffin_fuzzer.hpp"

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

#include "common/platform/process.hpp"
#include "expression/bytecode.hpp"
#include "gtest/gtest.h"

namespace tinylamb {
namespace {

// Seeded end-to-end runs: crash oracle plus bytecode-vs-AST differential for
// every executed SELECT. A non-empty report is a logic bug in the bytecode
// compiler, the JIT kernels, or the executor.
TEST(GriffinFuzzer, SeededIterationsHoldOracles) {
  const char* scale = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_ITERS");
  int kIterations = 64;
  if (scale != nullptr) {
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(scale, &end, 10);
    if (errno == 0 && end != scale && *end == '\0' && parsed > 0 &&
        parsed <= std::numeric_limits<int>::max()) {
      kIterations = static_cast<int>(parsed);
    }
  }
  int ran_select = 0;
  for (uint32_t seed = 0; std::cmp_less(seed, kIterations); ++seed) {
    const uint64_t packed = (static_cast<uint64_t>(seed) << 32) | seed;
    std::mt19937 rng(seed);
    GriffinTrace trace;
    trace.seed = packed;
    std::string report = RunGriffinIteration(rng, false, &trace);
    ASSERT_EQ(report, "") << report;
    EXPECT_EQ(trace.seed, packed);
    EXPECT_FALSE(trace.statements.empty());
    if (trace.ran_select) {
      ++ran_select;
    }
  }
  // The differential oracle must actually execute SELECTs (not skip every
  // iteration), or the sweep is vacuous.
  EXPECT_GT(ran_select, kIterations / 2);
}

// Failure->file->replay pipeline: serialize/parse round-trips, replay of a
// healthy trace holds, and a tampered trace is reported, not replayed clean.
TEST(GriffinFuzzer, TestFileRoundTripAndReplay) {
  const uint32_t seed32 = 7;
  std::mt19937 rng(seed32);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
                             // deterministic seed for reproducibility
  GriffinTrace trace;
  trace.seed = (static_cast<uint64_t>(seed32) << 32) | seed32;
  ASSERT_EQ(RunGriffinIteration(rng, false, &trace), "");

  const std::string text = SerializeGriffinTest(trace);
  GriffinTrace parsed;
  ASSERT_TRUE(ParseGriffinTest(text, &parsed));
  EXPECT_EQ(parsed, trace);
  bool skipped = false;
  EXPECT_EQ(ReplayGriffinTrace(parsed, false, &skipped), "");
  EXPECT_FALSE(skipped) << "replay skipped (DB could not open): false green";

  // Tampered statements must not replay clean.
  GriffinTrace tampered = parsed;
  tampered.statements[0] = "CREATE TABLE tampered (x INT64);";
  skipped = false;
  EXPECT_NE(ReplayGriffinTrace(tampered, false, &skipped), "");
  EXPECT_FALSE(skipped) << "tampered replay skipped: no check actually ran";

  // Malformed input is refused, never half-replayed.
  GriffinTrace junk;
  EXPECT_FALSE(ParseGriffinTest("SELECT 1;", &junk));
  EXPECT_FALSE(ParseGriffinTest("-- tinylamb-griffin-test v1\n", &junk));
}

// The evaluation-path differential must be live: with bytecode disabled the
// same SELECT still executes (proving the AST fallback path is taken), so a
// clean differential sweep is evidence, not a vacuous pass.
TEST(GriffinFuzzer, BytecodeDisabledPathStillExecutes) {
  const uint32_t seed32 = 3;
  std::mt19937 rng(seed32);  // NOLINT(cert-msc32-c,cert-msc51-cpp)
                             // deterministic seed for reproducibility
  GriffinTrace trace;
  trace.seed = (static_cast<uint64_t>(seed32) << 32) | seed32;

  // The clean differential below is evidence only if the disable hook it
  // relies on is live. With TINYLAMB_DISABLE_BYTECODE set the engine must
  // actually leave the bytecode fast path; otherwise oracle (2) compares
  // bytecode against bytecode and every seeded sweep passes vacuously.
  // Snapshot both *presence* and value: an inherited `=0` must be restored as
  // `0`, not unset. Copy the value before SetEnv may invalidate getenv's
  // pointer.
  const char* prior_disable = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_BYTECODE");
  const std::string prior_value =
      prior_disable == nullptr ? std::string() : std::string(prior_disable);
  platform::SetEnv("TINYLAMB_DISABLE_BYTECODE", "1");
  const bool ast_fallback_live = !BytecodeEnabled();
  if (prior_disable == nullptr) {
    platform::UnsetEnv("TINYLAMB_DISABLE_BYTECODE");
  } else {
    platform::SetEnv("TINYLAMB_DISABLE_BYTECODE", prior_value.c_str());
  }
  EXPECT_TRUE(ast_fallback_live)
      << "TINYLAMB_DISABLE_BYTECODE no longer reaches the engine; the "
         "AST-fallback differential is vacuous";

  ASSERT_EQ(RunGriffinIteration(rng, false, &trace), "");
  ASSERT_TRUE(trace.ran_select);
  bool saw_select = false;
  for (const std::string& sql : trace.statements) {
    saw_select = saw_select || sql.starts_with("SELECT");
  }
  EXPECT_TRUE(saw_select);
}

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

// Regression guard: replay every committed `griffin_fuzz-repro-*.test`
// under TINYLAMB_GRIFFIN_REGRESSION_DIR (mirrors the sql_oracle/session
// regression-replay tests).  With no directory set the test skips.
TEST(GriffinFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = ::tinylamb::platform::GetEnv("TINYLAMB_GRIFFIN_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "skipped";
  }
  const std::vector<std::string> paths = ListTestFiles(dir);
  EXPECT_FALSE(paths.empty()) << "no .test files in " << dir;
  for (const std::string& path : paths) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    GriffinTrace trace;
    ASSERT_TRUE(ParseGriffinTest(text, &trace))
        << "malformed regression file: " << path;
    bool skipped = false;
    EXPECT_EQ(ReplayGriffinTrace(trace, false, &skipped), "")
        << "regression " << path << " still reproduces";
    EXPECT_FALSE(skipped)
        << "regression " << path << " replay skipped (DB could not open)";
  }
}

TEST(GriffinFuzzer,
     QualifiedNonexistentColumnAndUngroupedLeftJoinRepresentativeRegression) {
  // Regression for griffin_fuzz-repro-17154338944608924769,
  // 10946922447985395703, 11641915892569236690 (Root Cause 2A: nonexistent
  // table-qualified columns r.lk / gk.y must be rejected consistently by all
  // routes) and 9473164197374685349, 3047919724090122967, 12281152844797537137
  // (Root Cause 2B: bare non-grouped columns over LEFT JOIN + GROUP BY must
  // not change representative row under TINYLAMB_GROUPED_ROUTE=cascades).
  for (uint64_t seed : {
           17154338944608924769ULL,
           10946922447985395703ULL,
           11641915892569236690ULL,
           9473164197374685349ULL,
           3047919724090122967ULL,
           12281152844797537137ULL,
           2051286306716623367ULL,
           69293237138238933ULL,
       }) {
    std::mt19937 rng(static_cast<uint32_t>(seed));
    GriffinTrace trace;
    trace.seed = seed;
    EXPECT_EQ(RunGriffinIteration(rng, false, &trace), "") << "seed=" << seed;
    bool skipped = false;
    EXPECT_EQ(ReplayGriffinTrace(trace, false, &skipped), "")
        << "seed=" << seed;
    EXPECT_FALSE(skipped) << "seed=" << seed
                          << ": replay skipped (DB could not open)";
  }
}

}  // namespace
}  // namespace tinylamb
