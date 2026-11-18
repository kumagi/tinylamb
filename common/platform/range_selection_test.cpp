/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/range_selection.hpp"

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace tinylamb::platform {
TEST(RangeSelectionTest, SignedBoundsTailsAndUnalignedBuffers) {
  constexpr int64_t low = std::numeric_limits<int64_t>::min();
  constexpr int64_t high = std::numeric_limits<int64_t>::max();
  uint64_t seed = 42;
  std::vector<int64_t> input(1026);
  for (size_t i = 0; i < input.size(); ++i) {
    seed = (seed * 6364136223846793005ULL) + 1442695040888963407ULL;
    input[i] = i % 7 == 0    ? low
               : i % 11 == 0 ? high
               : i % 3 == 0  ? static_cast<int64_t>(i % 17) - 8
                             : std::bit_cast<int64_t>(seed);
  }
  const std::vector<std::pair<int64_t, int64_t>> ranges{
      {low, high},      {low, low},        {high, high}, {low, -1},
      {0, high},        {-3, 5},           {0, 0},       {5, -3},
      {low, low + 100}, {high - 100, high}};
  for (uint32_t count = 0; count <= 1024; ++count) {
    for (const auto [lower, upper] : ranges) {
      std::vector<uint32_t> expected;
      for (uint32_t row = 0; row < count; ++row) {
        if (input[row + 1] >= lower && input[row + 1] <= upper) {
          expected.push_back(row);
}
}
      std::vector<uint32_t> output(count + 2, UINT32_MAX);
      const uint32_t size = SelectInt64ClosedRange(
          input.data() + 1, count, lower, upper, output.data() + 1);
      ASSERT_EQ(size, expected.size());
      EXPECT_EQ(output.front(), UINT32_MAX);
      EXPECT_EQ(output.back(), UINT32_MAX);
      EXPECT_EQ(
          std::vector<uint32_t>(output.begin() + 1, output.begin() + 1 + size),
          expected);
    }
  }
  EXPECT_EQ(SelectInt64ClosedRange(nullptr, 0, low, high, nullptr), 0);
}
TEST(RangeSelectionTest, DoubleCompactionKeepsOrderNanInfinityAndTails) {
  const double inf = std::numeric_limits<double>::infinity();
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const std::vector<double> values{nan, -inf, inf, -0.0, 0.0, -1, 1, 2, 0.5};
  std::vector<int64_t> cells;
  cells.reserve(values.size());
for (const double v : values) { cells.push_back(std::bit_cast<int64_t>(v));
}
  for (uint32_t count = 0; count < 128; ++count) {
    for (const double lo : values) {
      for (const double hi : values) {
        for (bool lower_inclusive : {false, true}) {
          for (bool upper_inclusive : {false, true}) {
            std::vector<uint32_t> positions(count + 2, UINT32_MAX), expected;
            for (uint32_t i = 0; i < count; ++i) {
              const uint32_t row = (i * 7 + 3) % values.size();
              positions[i + 1] = row;
              const double v = values[row];
              if ((lower_inclusive ? v >= lo : v > lo) &&
                  (upper_inclusive ? v <= hi : v < hi)) {
                expected.push_back(row);
}
            }
            const uint32_t size =
                CompactDoubleRange(cells.data(), positions.data() + 1, count,
                                   lo, hi, lower_inclusive, upper_inclusive);
            ASSERT_EQ(size, expected.size());
            ASSERT_EQ(positions.front(), UINT32_MAX);
            ASSERT_EQ(positions.back(), UINT32_MAX);
            ASSERT_EQ(std::vector<uint32_t>(positions.begin() + 1,
                                            positions.begin() + 1 + size),
                      expected);
          }
        }
      }
    }
  }
  EXPECT_EQ(CompactDoubleRange(nullptr, nullptr, 0, -inf, inf, true, true), 0);
}

}  // namespace tinylamb::platform
