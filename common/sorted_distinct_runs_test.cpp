/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/sorted_distinct_runs.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace tinylamb {
TEST(SortedDistinctRunsTest, ExactUnsignedUnionAcrossWorkersAndRuns) {
  SortedDistinctRuns left;
  SortedDistinctRuns right;
  ASSERT_SUCCESS(left.Add(std::array<uint64_t, 4>{0, 0, 7, UINT64_MAX}));
  ASSERT_SUCCESS(left.Add(std::array<uint64_t, 3>{1, 7, uint64_t{1} << 63}));
  ASSERT_SUCCESS(right.Add(std::array<uint64_t, 3>{0, 9, UINT64_MAX}));
  ASSERT_SUCCESS(left.Absorb(std::move(right)));
  std::vector<uint64_t> result;
  ASSERT_SUCCESS(
      left.ForEach([&](uint64_t value) { result.push_back(value); }));
  EXPECT_EQ(result,
            (std::vector<uint64_t>{0, 1, 7, 9, uint64_t{1} << 63, UINT64_MAX}));
  EXPECT_TRUE(left.Empty());
  ASSERT_SUCCESS(left.ForEach([](uint64_t) { FAIL(); }));
}

TEST(SortedDistinctRunsTest, FiveMillionInputsUseBoundedRuns) {
  SortedDistinctRuns runs;
  constexpr size_t kRows = 5000000;
  constexpr uint64_t kDomain = 50003;
  std::vector<uint64_t> chunk;
  chunk.reserve(65536);
  for (size_t base = 0; base < kRows; base += 65536) {
    chunk.clear();
    for (size_t row = base; row < std::min(kRows, base + 65536); ++row) {
      chunk.push_back((row * 73) % kDomain);
    }
    std::ranges::sort(chunk);
    const auto duplicates = std::ranges::unique(chunk);
    chunk.erase(duplicates.begin(), duplicates.end());
    ASSERT_SUCCESS(runs.Add(chunk));
  }
  uint64_t expected = 0;
  ASSERT_SUCCESS(
      runs.ForEach([&](uint64_t value) { EXPECT_EQ(value, expected++); }));
  EXPECT_EQ(expected, kDomain);
}

TEST(SortedDistinctRunsTest, CallbackExceptionsBecomeStatus) {
  SortedDistinctRuns runs;
  ASSERT_SUCCESS(runs.Add(std::array<uint64_t, 1>{1}));
  EXPECT_FALSE(
      runs.ForEach([](uint64_t) { throw std::runtime_error("stop"); }).ok());
  EXPECT_TRUE(runs.Empty());
}
}  // namespace tinylamb
