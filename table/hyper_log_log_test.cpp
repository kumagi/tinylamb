/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/hyper_log_log.hpp"

#include <cmath>
#include <string>
#include <vector>

#include "common/reservoir_sample.hpp"
#include "gtest/gtest.h"
#include "type/value.hpp"

namespace tinylamb {

TEST(HyperLogLogTest, EstimateCardinality) {
  HyperLogLog hll(10);
  EXPECT_EQ(hll.RegisterCount(), 1024);

  // Add 10,000 distinct values
  constexpr size_t kDistinctCount = 10000;
  for (size_t i = 0; i < kDistinctCount; ++i) {
    hll.Add("value_" + std::to_string(i));
  }

  const double estimate = hll.Estimate();
  // Standard error for m=1024 is ~1.04 / sqrt(1024) = 3.25%
  // Expect estimate within 10%
  EXPECT_GT(estimate, kDistinctCount * 0.90);
  EXPECT_LT(estimate, kDistinctCount * 1.10);
}

TEST(HyperLogLogTest, MergeHll) {
  HyperLogLog hll1(8);
  HyperLogLog hll2(8);

  for (size_t i = 0; i < 5000; ++i) {
    hll1.Add("item_" + std::to_string(i));
  }
  for (size_t i = 5000; i < 10000; ++i) {
    hll2.Add("item_" + std::to_string(i));
  }

  hll1.Merge(hll2);
  const double estimate = hll1.Estimate();
  EXPECT_GT(estimate, 10000 * 0.85);
  EXPECT_LT(estimate, 10000 * 1.15);
}

TEST(HyperLogLogTest, AddValuesAndNulls) {
  HyperLogLog hll(10);
  for (int64_t i = 0; i < 1000; ++i) {
    hll.Add(Value(i));
  }
  hll.Add(Value());  // NULL value
  const double estimate = hll.Estimate();
  EXPECT_GT(estimate, 800);
  EXPECT_LT(estimate, 1200);
}

TEST(ReservoirSampleTest, CollectsSampleUniformly) {
  ReservoirSample<int> sample(100, 12345);
  for (int i = 0; i < 10000; ++i) {
    sample.Add(i);
  }

  EXPECT_EQ(sample.SeenCount(), 10000);
  EXPECT_EQ(sample.GetSample().size(), 100);

  // Check all elements in sample are valid
  for (int val : sample.GetSample()) {
    EXPECT_GE(val, 0);
    EXPECT_LT(val, 10000);
  }
}

}  // namespace tinylamb
