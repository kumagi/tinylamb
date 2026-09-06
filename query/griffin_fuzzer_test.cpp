/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/griffin_fuzzer.hpp"

#include <cstdint>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

// Seeded end-to-end runs: crash oracle plus bytecode-vs-AST differential for
// every executed SELECT. A non-empty report is a logic bug in the bytecode
// compiler, the JIT kernels, or the executor.
TEST(GriffinFuzzer, SeededIterationsHoldOracles) {
  constexpr int kIterations = 64;
  int ran_select = 0;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    const uint64_t packed = (static_cast<uint64_t>(seed) << 32) | seed;
    std::mt19937 rng(seed);
    GriffinTrace trace;
    trace.seed = packed;
    std::string report = RunGriffinIteration(rng, false, &trace);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    EXPECT_EQ(trace.seed, packed);
    EXPECT_FALSE(trace.statements.empty()) << "seed=" << seed;
    if (trace.ran_select) {
      ++ran_select;
    }
  }
  // The differential oracle must actually execute SELECTs (not skip every
  // iteration), or the sweep is vacuous.
  EXPECT_GT(ran_select, kIterations / 2)
      << "differential oracle almost never ran; harness is broken";
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
  EXPECT_EQ(ReplayGriffinTrace(parsed, false), "")
      << "healthy trace replayed as a mismatch";

  // Tampered statements must not replay clean.
  GriffinTrace tampered = parsed;
  tampered.statements[0] = "CREATE TABLE tampered (x INT64);";
  EXPECT_NE(ReplayGriffinTrace(tampered, false), "")
      << "tampered trace replayed clean; pipeline is broken";

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

}  // namespace
}  // namespace tinylamb
