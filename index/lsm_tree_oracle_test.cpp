/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "index/lsm_tree_oracle.hpp"

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "index/b_plus_tree_oracle.hpp"

namespace tinylamb {
namespace {

// Seeded operation-stream sweep: reads, view scans, reopen persistence and
// merge persistence must all match the model. Each seed uses its own temp
// directory; a failure reproduces from the seed alone.
TEST(LsmTreeOracle, SeededIterationsMatchModel) {
  constexpr int kIterations = 30;
  int ran = 0;
  for (uint32_t seed = 0; seed < kIterations; ++seed) {
    std::mt19937 rng(seed);
    const std::vector<BPlusTreeOp> ops = GenerateLsmTreeOps(rng);
    const std::string tag = "seed-" + std::to_string(seed);
    EXPECT_TRUE(CheckLsmTreeEquivalence(ops, tag).empty())
        << true << (seed != 0u) << true
        << CheckLsmTreeEquivalence(ops, tag);
    ++ran;
  }
  EXPECT_EQ(ran, kIterations);
}

// The generator is deterministic: reseeding reproduces the stream exactly,
// which is what lets a failure replay from the seed alone.
TEST(LsmTreeOracle, SameSeedReproducesSameStream) {
  for (uint32_t seed : {0U, 1U, 7U, 42U}) {
    std::mt19937 first(seed);
    std::mt19937 second(seed);
    const std::vector<BPlusTreeOp> a = GenerateLsmTreeOps(first);
    const std::vector<BPlusTreeOp> b = GenerateLsmTreeOps(second);
    ASSERT_EQ(a.size(), b.size()) << "seed=" << seed;
    for (size_t i = 0; i < a.size(); ++i) {
      EXPECT_EQ(a[i].kind, b[i].kind) << "seed=" << seed;
      EXPECT_EQ(a[i].key, b[i].key) << "seed=" << seed;
      EXPECT_EQ(a[i].value, b[i].value) << "seed=" << seed;
    }
  }
}

}  // namespace
}  // namespace tinylamb
