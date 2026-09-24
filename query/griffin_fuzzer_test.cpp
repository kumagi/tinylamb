/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/griffin_fuzzer.hpp"

#include <dirent.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

// Seeded end-to-end runs: crash oracle plus bytecode-vs-AST differential for
// every executed SELECT. A non-empty report is a logic bug in the bytecode
// compiler, the JIT kernels, or the executor.
TEST(GriffinFuzzer, SeededIterationsHoldOracles) {
  const char* scale = std::getenv("TINYLAMB_FUZZ_ITERS");
  const int kIterations =
      (scale != nullptr) ? static_cast<int>(std::strtol(scale, nullptr, 10))
                         : 64;
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
  EXPECT_EQ(ReplayGriffinTrace(parsed, false), "");

  // Tampered statements must not replay clean.
  GriffinTrace tampered = parsed;
  tampered.statements[0] = "CREATE TABLE tampered (x INT64);";
  EXPECT_NE(ReplayGriffinTrace(tampered, false), "");

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
  DIR* handle = opendir(dir.c_str());
  if (handle == nullptr) {
    return files;
  }
  while (const dirent* entry = readdir(handle)) {
    const std::string name = entry->d_name;
    if (name.size() > 5 && name.substr(name.size() - 5) == ".test") {
      std::string path = dir;
      path += "/";
      path += name;
      files.push_back(path);
    }
  }
  closedir(handle);
  return files;
}

// Regression guard: replay every committed `griffin_fuzz-repro-*.test`
// under TINYLAMB_GRIFFIN_REGRESSION_DIR (mirrors the sql_oracle/session
// regression-replay tests).  With no directory set the test skips.
TEST(GriffinFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_GRIFFIN_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "skipped";
  }
  for (const std::string& path : ListTestFiles(dir)) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    GriffinTrace trace;
    ASSERT_TRUE(ParseGriffinTest(text, &trace))
        << "malformed regression file: " << path;
    EXPECT_EQ(ReplayGriffinTrace(trace, false), "")
        << "regression " << path << " still reproduces";
  }
}

}  // namespace
}  // namespace tinylamb
