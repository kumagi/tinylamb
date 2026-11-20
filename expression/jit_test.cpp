/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/jit.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/regex_guard.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(JitTest, DelimitedCaptureAgreesWithGreedyRegexReplacement) {
  constexpr std::string_view pattern =
      R"(^https?://(?:www\.)?([^/]+)/[\s\S]*$)";
  constexpr std::string_view replacement = R"(\1)";
  auto description = DescribeDelimitedRegexCapture(pattern, replacement);
  ASSERT_TRUE(description.HasValue());
  ASSERT_TRUE(description.Value());
  const auto jit = JitDelimitedCapture::Compile(*description.Value());
  if (!jit) {
    GTEST_SKIP() << "LLVM JIT unavailable";
  }
  std::string scratch;
  const auto compare = [&](std::string_view input) {
    auto expected =
        ReplaceCheckedRegexView(input, pattern, replacement, &scratch);
    ASSERT_TRUE(expected.HasValue());
    EXPECT_EQ(jit->Replace(input), expected.Value()) << input;
  };
  for (std::string_view prefix :
       {"http://", "https://", "http://www.", "https://www.", "httpx://", ""}) {
    for (size_t n = 0; n <= 100; ++n) {
      std::string input(prefix);
      input.append(n, 'a');
      compare(input);
      input += "/\nmore/path";
      compare(input);
    }
  }
  compare(std::string_view("http://x\0y/z", 12));
  auto unsupported = DescribeDelimitedRegexCapture("^[^a]+$", "x");
  ASSERT_TRUE(unsupported.HasValue());
  EXPECT_FALSE(unsupported.Value());
}

TEST(JitTest, GroupHashSimdAndDatePartMatchScalarForPartialVectors) {
  const auto plain = JitGroupHash::Compile();
  const auto hour = JitDatePartHash::Compile(11);
  const auto minute = JitDatePartHash::Compile(14);
  if (!plain || !hour || !minute) {
    GTEST_SKIP() << "LLVM JIT unavailable";
  }
  const auto mix = [](uint64_t v) {
    v += 0x9e3779b97f4a7c15ULL;
    v = (v ^ (v >> 30)) * 0xbf58476d1ce4e5b9ULL;
    v = (v ^ (v >> 27)) * 0x94d049bb133111ebULL;
    return v ^ (v >> 31);
  };
  constexpr uint64_t seed = 0x517cc1b727220a95ULL;
  std::string raw;
  std::array<uint32_t, 19> starts{};
  std::array<int64_t, 19> groups{};
  std::array<int64_t, 19> parts{};
  std::array<uint64_t, 19> hashes{};
  for (size_t i = 0; i < starts.size(); ++i) {
    starts[i] = static_cast<uint32_t>(raw.size());
    raw += "2026-11-20 01:2";
    raw += static_cast<char>('0' + (i % 10));
    raw += i % 2 == 0 ? ":42" : ":42+00";
    groups[i] = static_cast<int64_t>(i) - 9;
  }
  plain->Hash(groups.data(), hashes.data(), groups.size(), seed);
  for (size_t i = 0; i < groups.size(); ++i) {
    EXPECT_EQ(hashes[i], mix(seed ^ mix(static_cast<uint64_t>(groups[i]))));
  }
  for (const bool use_minute : {false, true}) {
    const auto& kernel = use_minute ? *minute : *hour;
    kernel.Hash(groups.data(), raw.data(), starts.data(), parts.data(),
                hashes.data(), groups.size(), seed);
    for (size_t i = 0; i < groups.size(); ++i) {
      const uint64_t part = use_minute ? 20 + i % 10 : 1U;
      EXPECT_EQ(parts[i], part);
      EXPECT_EQ(hashes[i],
                mix(mix(seed ^ mix(static_cast<uint64_t>(groups[i]))) ^
                    (mix(part) + 0x9e3779b97f4a7c15ULL)));
    }
  }
}

TEST(JitTest, StringFingerprintMatchesScalarAtBothBufferEnds) {
  const auto kernel = JitStringFingerprint::Compile();
  if (!kernel) {
    GTEST_SKIP() << "LLVM JIT unavailable";
  }
  std::string bytes;
  std::array<uint32_t, 19> starts{};
  std::array<uint32_t, 19> ends{};
  std::array<uint64_t, 19> hashes{};
  for (size_t i = 0; i < starts.size(); ++i) {
    starts[i] = static_cast<uint32_t>(bytes.size());
    const size_t len = i < 15 ? (i % 11) : (18 - i);
    for (size_t j = 0; j < len; ++j) {
      bytes.push_back(static_cast<char>((i + j) * 17));
    }
    ends[i] = static_cast<uint32_t>(bytes.size());
  }
  starts[5] = ends[5] + 1;
  ends[12] = static_cast<uint32_t>(bytes.size() + 1);
  constexpr uint64_t seed = 1234567;
  kernel->Hash(bytes.data(), bytes.size(), starts.data(), ends.data(),
               hashes.data(), hashes.size(), seed);
  for (size_t i = 0; i < starts.size(); ++i) {
    const std::string_view expected =
        starts[i] < ends[i] && ends[i] <= bytes.size()
            ? std::string_view(bytes.data() + starts[i], ends[i] - starts[i])
            : std::string_view{};
    EXPECT_EQ(hashes[i], JitStringFingerprint::Scalar(expected, seed)) << i;
  }
}

TEST(JitTest, FusedFilterProjectSumPreservesIncomingTotalAndCardinality) {
  for (bool conditional_load : {false, true}) {
    auto kernel = JitInt64Kernels::CompileFilterProjectSum(
        BinaryOperation::kGreaterThan, conditional_load);
    if (!kernel) {
      GTEST_SKIP() << "LLVM JIT unavailable";
    }
    std::array<int64_t, 1031> keys{};
    std::array<int64_t, 1031> values{};
    int64_t expected = -19;
    uint64_t expected_count = 0;
    for (size_t row = 0; row < keys.size(); ++row) {
      keys[row] = static_cast<int64_t>(row % 4);
      values[row] = static_cast<int64_t>(row % 19) - 9;
      if (keys[row] > 1) {
        expected += (values[row] * -3) + 5;
        ++expected_count;
      }
    }
    uint64_t selected = 0;
    EXPECT_EQ(kernel->FilterProjectSum(keys.data(), values.data(), keys.size(),
                                       1, -3, 5, -19, &selected),
              expected);
    EXPECT_EQ(selected, expected_count);
    EXPECT_EQ(kernel->FilterProjectSum(keys.data(), values.data(), keys.size(),
                                       3, -3, 5, -19, &selected),
              -19);
    EXPECT_EQ(selected, 0U);
  }
}

TEST(JitTest, FusedSumRequiresSafeMultiplyAddAndRunningPrefixes) {
  EXPECT_TRUE(JitInt64Kernels::CanUseFilterProjectSum(-9, 9, -3, 5, -19, 1031));
  EXPECT_FALSE(JitInt64Kernels::CanUseFilterProjectSum(INT64_MAX, INT64_MAX, 2,
                                                       -INT64_MAX, 0, 1));
  EXPECT_FALSE(JitInt64Kernels::CanUseFilterProjectSum(INT64_MAX, INT64_MAX, 1,
                                                       1, 0, 1));
  EXPECT_FALSE(
      JitInt64Kernels::CanUseFilterProjectSum(-1, 1, 1, 0, INT64_MAX, 2));
}

TEST(JitTest, LiteralSearchMatchesScalarAtEveryVectorOffsetAndTail) {
  if (!JitInt64Kernels::CompileFilter(BinaryOperation::kEquals)) {
    GTEST_SKIP() << "JIT disabled";
  }
  for (const std::string_view needle :
       {std::string_view("google"), std::string_view("\xff\0", 2),
        std::string_view("abcdefghijklmnop")}) {
    const auto kernel = JitLiteralSearch::Compile(needle);
    if (!kernel.has_value()) {
      ADD_FAILURE() << "literal search compilation failed";
      return;
    }
    for (size_t start = 0; start < 70; ++start) {
      std::string bytes(start, needle.front());
      bytes.append(needle);
      bytes.append(35, needle.back());
      for (size_t size = 0; size <= bytes.size(); ++size) {
        const std::string_view slice(bytes.data(), size);
        const size_t expected = slice.find(needle);
        EXPECT_EQ(kernel->Find(std::span(
                      reinterpret_cast<const uint8_t*>(bytes.data()), size)),
                  expected == std::string_view::npos ? size : expected)
            << "start=" << start << " size=" << size;
      }
    }
  }
}

TEST(JitTest, JitInt64Kernels_WhenCompiled_MatchScalarResults) {
  std::vector<int64_t> input(4096);
  std::iota(input.begin(), input.end(), int64_t{-2048});
  // JIT is optional; skip the kernel tests when LLVM is unavailable.
  if (!JitInt64Kernels::CompileFilter(BinaryOperation::kEquals).has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }

  auto filter =
      JitInt64Kernels::CompileFilter(BinaryOperation::kGreaterThanEquals);
  auto projection = JitInt64Kernels::CompileProjection();
  auto sum = JitInt64Kernels::CompileSum();
  if (!filter.has_value() || !projection.has_value() || !sum.has_value()) {
    GTEST_FAIL() << "failed";
    return;
  }
  std::vector<uint8_t> selected(input.size());
  filter->Filter(input.data(), selected.data(), input.size(), 17);
  std::vector<int64_t> projected(input.size());
  projection->Project(input.data(), projected.data(), input.size(), 3, 7);

  for (size_t index = 0; index < input.size(); ++index) {
    EXPECT_EQ(selected[index], input[index] >= 17);
    EXPECT_EQ(projected[index], (input[index] * 3) + 7);
  }
  EXPECT_EQ(sum->Sum(input.data(), input.size()),
            std::accumulate(input.begin(), input.end(), int64_t{0}));
  EXPECT_GT(filter->CompileMilliseconds(), 0.0);
}

TEST(JitTest, CompileFilter_ForEveryComparisonOperator_MatchesScalarReference) {
  std::vector<int64_t> input(1024);
  std::iota(input.begin(), input.end(), int64_t{-512});
  // JIT is optional; skip the kernel tests when LLVM is unavailable.
  if (!JitInt64Kernels::CompileFilter(BinaryOperation::kEquals).has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }
  const int64_t constant = 17;

  const auto scalar_match = [&](BinaryOperation op, int64_t lhs) {
    switch (op) {
      case BinaryOperation::kEquals:
        return lhs == constant;
      case BinaryOperation::kNotEquals:
        return lhs != constant;
      case BinaryOperation::kLessThan:
        return lhs < constant;
      case BinaryOperation::kLessThanEquals:
        return lhs <= constant;
      case BinaryOperation::kGreaterThan:
        return lhs > constant;
      case BinaryOperation::kGreaterThanEquals:
        return lhs >= constant;
      default:
        return false;
    }
  };

  const std::vector<BinaryOperation> operations{
      BinaryOperation::kEquals,      BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,    BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals,
  };

  for (const BinaryOperation op : operations) {
    auto kernel = JitInt64Kernels::CompileFilter(op);
    if (!kernel.has_value()) {
      GTEST_FAIL() << "kernel compilation failed";
      return;
    }
    std::vector<uint8_t> selected(input.size());
    kernel->Filter(input.data(), selected.data(), input.size(), constant);
    for (size_t index = 0; index < input.size(); ++index) {
      EXPECT_EQ(selected[index], scalar_match(op, input[index])) << index;
    }
  }
}

TEST(JitTest, JitKernel_MoveAssignment_TransfersKernel) {
  auto filter = JitInt64Kernels::CompileFilter(BinaryOperation::kEquals);
  auto projection = JitInt64Kernels::CompileProjection();
  if (!filter.has_value() || !projection.has_value()) {
  // JIT is optional; skip the kernel tests when LLVM is unavailable.
  if (!JitInt64Kernels::CompileFilter(BinaryOperation::kEquals).has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }
    GTEST_FAIL() << "failed";
    return;
  }
  std::vector<int64_t> input{1, 2, 3};
  std::vector<int64_t> output(input.size());

  projection = std::move(filter);

  // The no-exception contract turns kernel-kind misuse into a CHECK abort.
  EXPECT_DEATH(
      projection->Project(input.data(), output.data(), input.size(), 2, 1),
      "not a projection kernel");
}

TEST(JitTest,
     CompileFilter_WhenCalledRepeatedly_ReusesCompiledKernelsProcessWide) {
  // JIT is optional; skip the kernel tests when LLVM is unavailable.
  if (!JitInt64Kernels::CompileFilter(BinaryOperation::kEquals).has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }
  auto first = JitInt64Kernels::CompileFilter(BinaryOperation::kEquals);
  if (!first.has_value()) {
    GTEST_FAIL() << "failed";
    return;
  }
  const double first_ms = first->CompileMilliseconds();

  auto second = JitInt64Kernels::CompileFilter(BinaryOperation::kEquals);
  if (!second.has_value()) {
    GTEST_FAIL() << "failed";
    return;
  }

  EXPECT_DOUBLE_EQ(second->CompileMilliseconds(), first_ms);
  EXPECT_GT(first_ms, 0.0);
}

TEST(JitTest, CompileFilter_WithNonComparisonOperator_ReturnsNullopt) {
  EXPECT_FALSE(JitInt64Kernels::CompileFilter(BinaryOperation::kAdd));
  EXPECT_FALSE(JitInt64Kernels::CompileFilter(BinaryOperation::kAnd));
}

TEST(JitTest, JitKernel_WhenInvokingWrongAccessor_ThrowsLogicError) {
  // JIT is optional; skip the kernel tests when LLVM is unavailable.
  if (!JitInt64Kernels::CompileFilter(BinaryOperation::kEquals).has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }
  auto filter = JitInt64Kernels::CompileFilter(BinaryOperation::kLessThan);
  auto projection = JitInt64Kernels::CompileProjection();
  auto sum = JitInt64Kernels::CompileSum();
  if (!filter.has_value() || !projection.has_value() || !sum.has_value()) {
    GTEST_FAIL() << "failed";
    return;
  }
  std::vector<int64_t> input{1, 2, 3};
  std::vector<uint8_t> selected(input.size());
  std::vector<int64_t> output(input.size());

  EXPECT_DEATH(
      projection->Filter(input.data(), selected.data(), input.size(), 0),
      "not a filter kernel");
  EXPECT_DEATH(sum->Filter(input.data(), selected.data(), input.size(), 0),
               "not a filter kernel");
  EXPECT_DEATH(std::ignore = filter->Sum(input.data(), input.size()),
               "not a sum kernel");
  EXPECT_DEATH(sum->Project(input.data(), output.data(), input.size(), 1, 1),
               "not a projection kernel");
}

}  // namespace tinylamb
