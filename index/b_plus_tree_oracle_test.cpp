/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "index/b_plus_tree_oracle.hpp"

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

// Seeded operation-stream sweep: every generated stream must keep point
// reads, structural soundness and ordered scans consistent with the model.
// Each seed uses its own temp directory; a failure reproduces from the seed
// alone.
TEST(BPlusTreeOracle, SeededIterationsMatchModel) {
  constexpr int kIterations = 60;
  int ran = 0;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    const std::vector<BPlusTreeOp> ops = GenerateBPlusTreeOps(rng);
    const std::string tag = "seed-" + std::to_string(seed);
    EXPECT_TRUE(CheckBPlusTreeEquivalence(ops, tag).empty())
        << true << (seed != 0u) << true
        << CheckBPlusTreeEquivalence(ops, tag);
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
}

// The generator is deterministic: reseeding reproduces the stream exactly,
// which is what lets a failure replay from the seed alone.
TEST(BPlusTreeOracle, SameSeedReproducesSameStream) {
  for (uint32_t seed : {0U, 1U, 7U, 42U, 12345U}) {
    std::mt19937 first(seed);
    std::mt19937 second(seed);
    const std::vector<BPlusTreeOp> a = GenerateBPlusTreeOps(first);
    const std::vector<BPlusTreeOp> b = GenerateBPlusTreeOps(second);
    ASSERT_EQ(a.size(), b.size()) << "seed=" << seed;
    for (size_t i = 0; i < a.size(); ++i) {
      EXPECT_EQ(a[i].kind, b[i].kind) << "seed=" << seed;
      EXPECT_EQ(a[i].key, b[i].key) << "seed=" << seed;
      EXPECT_EQ(a[i].value, b[i].value) << "seed=" << seed;
    }
  }
}

// The shrinker never hides a mismatch and never grows the stream.
TEST(BPlusTreeOracle, ShrinkNeverHidesMismatchNorGrows) {
  for (uint32_t seed : {0U, 1U, 7U}) {
    std::mt19937 rng(seed);
    const std::vector<BPlusTreeOp> ops = GenerateBPlusTreeOps(rng);
    const std::string tag = "shrink-" + std::to_string(seed);
    const std::vector<BPlusTreeOp> shrunk = ShrinkBPlusTreeOps(ops, tag);
    EXPECT_LE(shrunk.size(), ops.size()) << "seed=" << seed;
    EXPECT_EQ(CheckBPlusTreeEquivalence(ops, tag).empty(),
              CheckBPlusTreeEquivalence(shrunk, tag).empty())
        << "seed=" << seed;
  }
}

}  // namespace
}  // namespace tinylamb
