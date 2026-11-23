/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/jit.hpp"

#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/regex_guard.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(JitTest, BranchlessProgramsSurviveCacheEviction) {
  const Schema schema("input", {Column("x", ValueType::kInt64)});
  std::vector<JitBranchlessProgram> programs;
  for (int64_t i = 0; i < 70; ++i) {
    auto expression = BinaryExpressionExp(
        ColumnValueExp("x"), BinaryOperation::kAdd, ConstantValueExp(Value(i)));
    auto bytecode = BytecodeCompiler::Compile(expression, schema);
    ASSERT_TRUE(bytecode.has_value());
    auto compiled = JitBranchlessProgram::Compile(*bytecode, schema);
    if (!compiled) {
      GTEST_SKIP() << "LLVM JIT unavailable";
    }
    programs.push_back(std::move(*compiled));
  }
  const int64_t value = 12;
  const std::array<JitColumnInput, 1> columns{{{.values = &value}}};
  for (size_t i = 0; i < programs.size(); ++i) {
    int64_t output = 0;
    uint8_t is_null = 0;
    bool any_null = false;
    ASSERT_TRUE(programs[i].Evaluate(columns, 1, &output, &is_null, &any_null));
    EXPECT_EQ(output, 12 + static_cast<int64_t>(i));
    EXPECT_FALSE(any_null);
  }
}

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
      const uint64_t part = use_minute ? 20 + (i % 10) : 1U;
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
    // JIT is optional; skip when LLVM is unavailable (no filter kernel).
    // Reaching here with a filter but no projection is a real failure.
    if (!filter.has_value()) {
      GTEST_SKIP() << "JIT disabled";
    }
    GTEST_FAIL() << "projection kernel compilation failed";
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

TEST(JitTest, JitDenseSplitRadixAgg_ExtractScatterAccum) {
  auto scatter = JitDenseSplitRadixAgg::CompileScatter(/*part_shift=*/2);
  auto accum_avg = JitDenseSplitRadixAgg::CompileAccum(/*is_avg=*/true,
                                                       /*check_inf=*/false);
  auto accum_inf = JitDenseSplitRadixAgg::CompileAccum(/*is_avg=*/false,
                                                       /*check_inf=*/true);
  if (!scatter.has_value() || !accum_avg.has_value() ||
      !accum_inf.has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }

  // Test kMulDoubleInt across 2 partitions (part_shift=2 -> 4 local gids/part).
  auto ext_mul =
      JitDenseSplitRadixAgg::CompileExtract(JitDenseSplitRadixAgg::ExtractSpec{
          .op = JitDenseSplitRadixAgg::ExprOp::kMulDoubleInt,
          .use_sel = true,
          .jk_has_null = true,
          .pk0_has_null = true,
          .val_null_mask = 0x03U,
          .part_shift = 2,
      });
  auto ext_coal =
      JitDenseSplitRadixAgg::CompileExtract(JitDenseSplitRadixAgg::ExtractSpec{
          .op = JitDenseSplitRadixAgg::ExprOp::kCoalesceMulZero,
          .use_sel = false,
          .jk_has_null = false,
          .pk0_has_null = false,
          .val_null_mask = 0x03U,
          .part_shift = 2,
      });
  ASSERT_TRUE(ext_mul.has_value());
  ASSERT_TRUE(ext_coal.has_value());
  constexpr uint8_t kMaxShift = JitDenseSplitRadixAgg::kMaxPartShift;
  EXPECT_TRUE(JitDenseSplitRadixAgg::CompileScatter(kMaxShift).has_value());
  EXPECT_TRUE(JitDenseSplitRadixAgg::CompileExtract({
                  .part_shift = kMaxShift,
              }).has_value());

  // 2 parts * 4 local gids/part = 8 total gids (r_card=4, pk0_card=2).
  // jk_min = 10, key_to_rsubgid: [0, UINT32_MAX, 2, 3] (10->0, 11->skip, 12->2,
  // 13->3) pk0_min = 100, pk0_subgid: [0, 1] (100->0, 101->1) Expect gid =
  // r_subgid * 2 + p_subgid:
  //   (10,100)->gid 0 (part 0, lg 0)
  //   (12,101)->gid 5 (part 1, lg 1)
  //   (13,101)->gid 7 (part 1, lg 3)
  const std::vector<uint32_t> key_to_r{0, UINT32_MAX, 2, 3};
  const std::vector<uint32_t> pk0_lut{0, 1};

  const std::vector<int64_t> jk_vals{10, 10, 11, 12, 13, 12, 10, 99};
  const std::vector<uint8_t> jk_nulls{0, 1, 0, 0, 0, 0, 0, 0};
  const std::vector<int64_t> pk0_vals{100, 100, 100, 101, 101, 101, 100, 100};
  const std::vector<uint8_t> pk0_nulls{0, 0, 0, 0, 1, 0, 0, 0};
  const std::vector<int64_t> v0{
      std::bit_cast<int64_t>(2.5), std::bit_cast<int64_t>(9.0),
      std::bit_cast<int64_t>(9.0), std::bit_cast<int64_t>(4.0),
      std::bit_cast<int64_t>(9.0), std::bit_cast<int64_t>(1.5),
      std::bit_cast<int64_t>(5.0), std::bit_cast<int64_t>(9.0),
  };
  const std::vector<uint8_t> n0{0, 0, 0, 0, 0, 1, 0, 0};
  const std::vector<int64_t> v1{3, 9, 9, 5, 9, 7, 2, 9};
  const std::vector<uint8_t> n1{0, 0, 0, 0, 0, 0, 0, 0};
  // sel touches rows 0..7 (skips NULL jk[1], miss jk[2], NULL pk0[4],
  // out-of-range jk[7]). Valid rows: 0 (gid 0, 2.5*3=7.5), 3 (gid 5, 4*5=20.0),
  // 5 (gid 5, NULL value -> contributes no total), 6 (gid 0, 5*2=10.0).
  // hist[1] is 2 because rows 3 and 5 both map to gid 5; only row 3 has a
  // non-NULL value there.
  const std::vector<uint32_t> sel{0, 1, 2, 3, 4, 6, 7, 5};
  std::vector<uint32_t> out_tg(sel.size(), 0);
  std::vector<double> out_v(sel.size(), 0.0);
  std::array<uint32_t, 2> hist{0, 0};

  // Empty count = 0 check.
  EXPECT_EQ(ext_mul->Run(0, 8, sel.data(), jk_vals.data(), jk_nulls.data(), 10,
                         key_to_r.size(), key_to_r.data(), pk0_vals.data(),
                         pk0_nulls.data(), 100, pk0_lut.size(), pk0_lut.data(),
                         2, v0.data(), n0.data(), v1.data(), n1.data(), nullptr,
                         nullptr, nullptr, nullptr, 1.0, out_tg.data(),
                         out_v.data(), hist),
            0U);

  const auto run_selected = [&](std::span<const uint32_t> selected,
                                std::span<uint32_t> histogram) {
    return ext_mul->Run(
        static_cast<uint32_t>(selected.size()), 8, selected.data(),
        jk_vals.data(), jk_nulls.data(), 10, key_to_r.size(), key_to_r.data(),
        pk0_vals.data(), pk0_nulls.data(), 100, pk0_lut.size(), pk0_lut.data(), 2,
        v0.data(), n0.data(), v1.data(), n1.data(), nullptr, nullptr, nullptr,
        nullptr, 1.0, out_tg.data(), out_v.data(), histogram);
  };
  const std::array<uint32_t, 1> invalid_row{8};
  const std::array<uint32_t, 1> row_in_second_partition{3};
  EXPECT_EQ(run_selected(invalid_row, hist),
            JitDenseSplitRadixAgg::kInvalidExtractCount);
  EXPECT_EQ(run_selected(row_in_second_partition, std::span(hist).first(1)),
            JitDenseSplitRadixAgg::kInvalidExtractCount);
  const uint32_t m = run_selected(sel, hist);
  ASSERT_EQ(m, 4U);
  EXPECT_EQ(hist[0], 2U);  // gid 0 twice
  EXPECT_EQ(hist[1], 2U);  // gid 5 twice (one valid, one NULL)

  std::array<std::vector<uint32_t>, 2> part_tg{std::vector<uint32_t>(hist[0]),
                                               std::vector<uint32_t>(hist[1])};
  std::array<std::vector<double>, 2> part_v{std::vector<double>(hist[0]),
                                            std::vector<double>(hist[1])};
  std::array<uint32_t*, 2> dst_gids{part_tg[0].data(), part_tg[1].data()};
  std::array<double*, 2> dst_vals{part_v[0].data(), part_v[1].data()};
  std::array<uint32_t, 2> dst_pos{0, 0};
  scatter->Run(0, out_tg.data(), out_v.data(), dst_gids.data(), dst_vals.data(),
               dst_pos.data());
  scatter->Run(m, out_tg.data(), out_v.data(), dst_gids.data(), dst_vals.data(),
               dst_pos.data());
  EXPECT_EQ(dst_pos[0], hist[0]);
  EXPECT_EQ(dst_pos[1], hist[1]);

  // Verify accum on part 0 & part 1 plus all-NULL group lg=3.
  for (size_t p = 0; p < 2; ++p) {
    if (p == 1) {
      // Inject an all-NULL tuple for local gid 3 (sign bit set).
      part_tg[1].push_back(0x80000003U);
      part_v[1].push_back(999.0);
    }
    std::array<double, 4> sums{};
    std::array<uint64_t, 4> counts{};
    uint64_t mb = 0;
    uint64_t nb = 0;
    uint8_t inf = 0;
    accum_avg->Run(0, part_tg[p].data(), part_v[p].data(), sums.data(),
                   counts.data(), &mb, &nb, &inf);
    EXPECT_EQ(mb, 0ULL);
    accum_avg->Run(static_cast<uint32_t>(part_tg[p].size()), part_tg[p].data(),
                   part_v[p].data(), sums.data(), counts.data(), &mb, &nb,
                   &inf);
    if (p == 0) {
      EXPECT_EQ(mb, 1ULL);
      EXPECT_EQ(nb, 1ULL);
      EXPECT_DOUBLE_EQ(sums[0], 17.5);
      EXPECT_EQ(counts[0], 2ULL);
    } else {
      EXPECT_EQ(mb, (uint64_t{1} << 1U) | (uint64_t{1} << 3U));
      EXPECT_EQ(nb, (uint64_t{1} << 1U));  // lg=3 was NULL-only -> nonnull=0
      EXPECT_DOUBLE_EQ(sums[1], 20.0);
      EXPECT_EQ(counts[1], 1ULL);
    }
  }

  // Test kCoalesceMulZero: NULL value produces 0.0 and valid nonnull=1 bit!
  const std::vector<int64_t> c_jk{10};
  const std::vector<int64_t> c_pk{101};
  const std::vector<int64_t> c_v0{std::bit_cast<int64_t>(9.5)};
  const std::vector<uint8_t> c_n0{1};  // NULL!
  const std::vector<int64_t> c_v1{4};
  const std::vector<uint8_t> c_n1{0};
  std::vector<uint32_t> c_tg(1);
  std::vector<double> c_val(1);
  std::array<uint32_t, 2> c_hist{0, 0};
  ASSERT_EQ(
      ext_coal->Run(1, 1, nullptr, c_jk.data(), nullptr, 10, key_to_r.size(),
                    key_to_r.data(), c_pk.data(), nullptr, 100, pk0_lut.size(),
                    pk0_lut.data(), 2, c_v0.data(), c_n0.data(), c_v1.data(),
                    c_n1.data(), nullptr, nullptr, nullptr, nullptr, 1.0,
                    c_tg.data(), c_val.data(), c_hist),
      1U);
  EXPECT_EQ(c_tg[0], 1U);  // sign bit NOT set for COALESCE
  EXPECT_DOUBLE_EQ(c_val[0], 0.0);

  // Test check_inf=true overflow detection (finite + finite -> +inf).
  std::array<uint32_t, 2> inf_gids{2U, 2U};
  std::array<double, 2> inf_vals{1e308, 1e308};
  std::array<double, 4> inf_sums{};
  uint64_t imb = 0;
  uint64_t inb = 0;
  uint8_t inf_flag = 0;
  accum_inf->Run(2, inf_gids.data(), inf_vals.data(), inf_sums.data(), nullptr,
                 &imb, &inb, &inf_flag);
  EXPECT_EQ(inf_flag, 1U);
  EXPECT_TRUE(std::isinf(inf_sums[2]));
}

TEST(JitTest, JitPacked2x32HashProbe_ProbeBloomAndBias) {
  auto probe_nobias =
      JitPacked2x32HashProbe::Compile(false, false, false, false);
  auto probe_full = JitPacked2x32HashProbe::Compile(true, true, true, true);
  if (!probe_nobias.has_value() || !probe_full.has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }

  constexpr size_t kHtCap = 16;
  constexpr uint64_t kHtMask = kHtCap - 1;
  constexpr size_t kBloomWords = 4;
  constexpr uint64_t kBloomMask = kBloomWords - 1;
  std::vector<uint64_t> ht_keys(kHtCap, 0);
  std::vector<uint32_t> ht_heads(kHtCap, UINT32_MAX);
  std::vector<uint64_t> bloom(kBloomWords, 0);

  const auto pack_pair = [](int32_t a, int32_t b) noexcept -> uint64_t {
    return (static_cast<uint64_t>(static_cast<uint32_t>(a)) << 32U) |
           static_cast<uint64_t>(static_cast<uint32_t>(b));
  };
  const std::vector<std::pair<int32_t, int32_t>> build_pairs{
      {0, 0},
      {-1, -1},
      {std::numeric_limits<int32_t>::min(),
       std::numeric_limits<int32_t>::max()},
      {42, 7},
  };
  for (uint32_t bid = 0; bid < build_pairs.size(); ++bid) {
    const uint64_t pk =
        pack_pair(build_pairs[bid].first, build_pairs[bid].second);
    const uint64_t h = HashPacked2x32Key(pk);
    InsertPacked2x32Bloom(h, bloom.data(), kBloomMask);
    auto slot = static_cast<size_t>(h & kHtMask);
    while (ht_heads[slot] != UINT32_MAX) {
      slot = (slot + 1) & kHtMask;
    }
    ht_keys[slot] = pk;
    ht_heads[slot] = bid;
  }

  // Test no-bias empty + hit/miss including boundary int32 keys & out-of-int32.
  std::vector<uint32_t> out(8, 123);
  probe_nobias->Probe(0, nullptr, nullptr, nullptr, nullptr, 0, 0, nullptr, 0,
                      ht_keys.data(), ht_heads.data(), kHtMask, out.data());
  EXPECT_EQ(out[0], 123U);

  const std::vector<int64_t> p0{
      0, -1, std::numeric_limits<int32_t>::min(), 42, int64_t{1} << 33, 99};
  const std::vector<int64_t> p1{0, -1, std::numeric_limits<int32_t>::max(),
                                7, 0,  99};
  probe_nobias->Probe(static_cast<uint32_t>(p0.size()), p0.data(), nullptr,
                      p1.data(), nullptr, 0, 0, nullptr, 0, ht_keys.data(),
                      ht_heads.data(), kHtMask, out.data());
  EXPECT_EQ(out[0], 0U);
  EXPECT_EQ(out[1], 1U);
  EXPECT_EQ(out[2], 2U);
  EXPECT_EQ(out[3], 3U);
  EXPECT_EQ(out[4], UINT32_MAX);
  EXPECT_EQ(out[5], UINT32_MAX);

  // Test nulls + bloom + bias (including signed int32/int64 bias + overflow).
  const std::vector<int64_t> bp0{-10, -11, 32,
                                 0,   0,   std::numeric_limits<int32_t>::max()};
  const std::vector<uint8_t> bn0{0, 0, 0, 1, 0, 0};
  const std::vector<int64_t> bp1{-5, -6, 2, 0, 0, 0};
  const std::vector<uint8_t> bn1{0, 0, 0, 0, 1, 0};
  // bias0 = +10, bias1 = +5 -> row 0 becomes (0,0), row 1 becomes (-1,-1),
  // row 2 becomes (42,7), rows 3&4 are NULL, row 5 overflows int32 after bias!
  std::vector<uint32_t> out_full(bp0.size(), 999);
  probe_full->Probe(static_cast<uint32_t>(bp0.size()), bp0.data(), bn0.data(),
                    bp1.data(), bn1.data(), 10, 5, bloom.data(), kBloomMask,
                    ht_keys.data(), ht_heads.data(), kHtMask, out_full.data());
  EXPECT_EQ(out_full[0], 0U);
  EXPECT_EQ(out_full[1], 1U);
  EXPECT_EQ(out_full[2], 3U);
  EXPECT_EQ(out_full[3], UINT32_MAX);
  EXPECT_EQ(out_full[4], UINT32_MAX);
  EXPECT_EQ(out_full[5], UINT32_MAX);
}

TEST(JitTest, JitPacked2x32RadixDedup_PackScatterAndGrowResume) {
  auto pack_nosel = JitPacked2x32RadixDedup::CompilePack(
      /*has_null0=*/true, /*has_null1=*/true, /*use_sel=*/false,
      /*part_shift=*/62);  // 4 partitions (0..3)
  auto pack_sel = JitPacked2x32RadixDedup::CompilePack(
      /*has_null0=*/false, /*has_null1=*/true, /*use_sel=*/true,
      /*part_shift=*/62);
  auto scatter = JitPacked2x32RadixDedup::CompileScatter(/*part_shift=*/62);
  auto dedup = JitPacked2x32RadixDedup::CompileOpenAddrDedup();
  if (!pack_nosel.has_value() || !pack_sel.has_value() ||
      !scatter.has_value() || !dedup.has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }

  // Exercise duplicates, (0,0), (-1,-1), NULLs, and out-of-int32.
  const std::vector<int64_t> k0{0,  -1, 0, 10, 11, 12, 13, 14, int64_t{1} << 40,
                                -1, 10, 0};
  const std::vector<uint8_t> n0{0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  const std::vector<int64_t> k1{0, -1, 5, 20, 21, 22, 23, 24, 9, -1, 20, 0};
  const std::vector<uint8_t> n1{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0};

  std::vector<uint64_t> out_packed(k0.size());
  std::array<uint32_t, 4> hist{};
  std::vector<uint32_t> fb_rows(k0.size());
  uint32_t packed_cnt = 99;
  uint32_t fb_cnt = 99;

  // Empty count = 0 check.
  pack_nosel->Run(0, nullptr, k0.data(), n0.data(), k1.data(), n1.data(),
                  out_packed.data(), hist.data(), fb_rows.data(), &packed_cnt,
                  &fb_cnt);
  EXPECT_EQ(packed_cnt, 0U);
  EXPECT_EQ(fb_cnt, 0U);

  pack_nosel->Run(static_cast<uint32_t>(k0.size()), nullptr, k0.data(),
                  n0.data(), k1.data(), n1.data(), out_packed.data(),
                  hist.data(), fb_rows.data(), &packed_cnt, &fb_cnt);
  EXPECT_EQ(fb_cnt, 3U);  // rows 2 (n0), 5 (n1), 8 (out-of-int32)
  EXPECT_EQ(fb_rows[0], 2U);
  EXPECT_EQ(fb_rows[1], 5U);
  EXPECT_EQ(fb_rows[2], 8U);
  EXPECT_EQ(packed_cnt, 9U);
  EXPECT_EQ(hist[0] + hist[1] + hist[2] + hist[3], packed_cnt);

  // Selection vector run on non-null k0 rows {0, 1, 5, 10}.
  const std::vector<uint32_t> sel{0, 1, 5, 10};
  std::vector<uint64_t> sel_packed(sel.size());
  std::array<uint32_t, 4> sel_hist{};
  std::vector<uint32_t> sel_fb(sel.size());
  uint32_t sel_pc = 0;
  uint32_t sel_fc = 0;
  pack_sel->Run(static_cast<uint32_t>(sel.size()), sel.data(), k0.data(),
                nullptr, k1.data(), n1.data(), sel_packed.data(),
                sel_hist.data(), sel_fb.data(), &sel_pc, &sel_fc);
  EXPECT_EQ(sel_fc, 1U);
  EXPECT_EQ(sel_fb[0], 5U);
  EXPECT_EQ(sel_pc, 3U);

  // Scatter into per-partition buffers, then dedup with forced grow/resume.
  std::array<std::vector<uint64_t>, 4> parts;
  std::array<uint64_t*, 4> dst_ptrs{};
  std::array<uint32_t, 4> dst_pos{0, 0, 0, 0};
  for (size_t p = 0; p < 4; ++p) {
    parts[p].resize(hist[p]);
    dst_ptrs[p] = parts[p].empty() ? nullptr : parts[p].data();
  }
  scatter->Run(packed_cnt, out_packed.data(), dst_ptrs.data(), dst_pos.data());
  for (size_t p = 0; p < 4; ++p) {
    EXPECT_EQ(dst_pos[p], hist[p]);
  }

  // Test DedupKernel::Run with tiny initial cap=4 (max_load=2) to force
  // grow/resume.
  std::vector<uint64_t> keys_all(out_packed.begin(),
                                 out_packed.begin() + packed_cnt);
  size_t ht_cap = 4;
  std::vector<uint64_t> ht_keys(ht_cap, 0);
  std::vector<uint32_t> ht_gens(ht_cap, 0);
  uint32_t cur_gen = 1;
  auto max_load = static_cast<uint32_t>((ht_cap * 7) / 10);  // 2
  uint32_t n_occ = 0;
  std::vector<int64_t> uk0(max_load);
  std::vector<int64_t> uk1(max_load);
  size_t off = 0;
  size_t resizes = 0;
  while (off < keys_all.size()) {
    const uint32_t done =
        dedup->Run(static_cast<uint32_t>(keys_all.size() - off),
                   keys_all.data() + off, ht_keys.data(), ht_gens.data(),
                   cur_gen, static_cast<uint64_t>(ht_cap - 1), max_load, &n_occ,
                   uk0.data(), uk1.data());
    off += done;
    if (off < keys_all.size()) {
      ++resizes;
      ht_cap *= 2;
      ht_keys.assign(ht_cap, 0);
      ht_gens.assign(ht_cap, 0);
      const auto nmask = static_cast<uint64_t>(ht_cap - 1);
      for (uint32_t j = 0; j < n_occ; ++j) {
        const uint64_t pk =
            (static_cast<uint64_t>(
                 static_cast<uint32_t>(static_cast<int32_t>(uk0[j])))
             << 32U) |
            static_cast<uint64_t>(
                static_cast<uint32_t>(static_cast<int32_t>(uk1[j])));
        auto slot = static_cast<size_t>(HashPacked2x32Key(pk) & nmask);
        while (ht_gens[slot] == cur_gen) {
          slot = (slot + 1) & static_cast<size_t>(nmask);
        }
        ht_keys[slot] = pk;
        ht_gens[slot] = cur_gen;
      }
      max_load = static_cast<uint32_t>((ht_cap * 7) / 10);
      uk0.resize(max_load);
      uk1.resize(max_load);
    }
  }
  EXPECT_GE(resizes, 1U);
  EXPECT_EQ(n_occ, 6U);  // {(0,0), (-1,-1), (10,20), (11,21), (13,23), (14,24)}
}

TEST(JitTest, JitRadixKeyDoubleAgg_AccumulateGrowAndInf) {
  auto agg = JitRadixKeyDoubleAgg::Compile(/*check_inf=*/false);
  auto agg_inf = JitRadixKeyDoubleAgg::Compile(/*check_inf=*/true);
  if (!agg.has_value() || !agg_inf.has_value()) {
    GTEST_SKIP() << "JIT disabled";
  }

  // Test valid + invalid tuples across 12 rows (more than prefetch tail 8)
  // forcing table resize from ht_cap=4 (max_load=2).
  const std::vector<uint64_t> in_keys{100, 200, 100, 300, 400, 500,
                                      600, 200, 500, 700, 100, 600};
  const std::vector<double> in_vals{1.5,  2.0, 2.5, 9.9, 0.5, 4.0,
                                    99.0, 3.0, 1.0, 7.0, 6.0, 88.0};
  const std::vector<uint8_t> in_valids{1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 0};

  size_t cap = 4;
  std::vector<uint64_t> ht_keys(cap, 0);
  std::vector<double> ht_sums(cap, 0.0);
  std::vector<uint64_t> ht_counts(cap, 0);
  std::vector<uint32_t> ht_gens(cap, 0);
  uint32_t cur_gen = 1;
  auto max_load = static_cast<uint32_t>((cap * 7) / 10);  // 2
  std::vector<uint32_t> occupied(max_load);
  uint32_t n_occ = 0;

  // Empty count = 0 check.
  EXPECT_EQ(agg->Accumulate(0, in_keys.data(), in_vals.data(), in_valids.data(),
                            ht_keys.data(), ht_sums.data(), ht_counts.data(),
                            ht_gens.data(), nullptr, cur_gen,
                            static_cast<uint64_t>(cap - 1), max_load,
                            occupied.data(), &n_occ),
            0U);

  size_t off = 0;
  size_t resizes = 0;
  while (off < in_keys.size()) {
    const uint32_t done = agg->Accumulate(
        static_cast<uint32_t>(in_keys.size() - off), in_keys.data() + off,
        in_vals.data() + off, in_valids.data() + off, ht_keys.data(),
        ht_sums.data(), ht_counts.data(), ht_gens.data(), nullptr, cur_gen,
        static_cast<uint64_t>(cap - 1), max_load, occupied.data(), &n_occ);
    off += done;
    if (off < in_keys.size()) {
      ++resizes;
      const size_t new_cap = cap * 2;
      std::vector<uint64_t> nk(new_cap, 0);
      std::vector<double> ns(new_cap, 0.0);
      std::vector<uint64_t> nc(new_cap, 0);
      std::vector<uint32_t> ng(new_cap, 0);
      const auto nml = static_cast<uint32_t>((new_cap * 7) / 10);
      std::vector<uint32_t> nocc(nml);
      const size_t nmask = new_cap - 1;
      for (uint32_t j = 0; j < n_occ; ++j) {
        const uint32_t op = occupied[j];
        const uint64_t k = ht_keys[op];
        auto pos = static_cast<size_t>(HashPacked2x32Key(k)) & nmask;
        while (ng[pos] == cur_gen) {
          pos = (pos + 1) & nmask;
        }
        nk[pos] = k;
        ns[pos] = ht_sums[op];
        nc[pos] = ht_counts[op];
        ng[pos] = cur_gen;
        nocc[j] = static_cast<uint32_t>(pos);
      }
      ht_keys.swap(nk);
      ht_sums.swap(ns);
      ht_counts.swap(nc);
      ht_gens.swap(ng);
      occupied.swap(nocc);
      cap = new_cap;
      max_load = nml;
    }
  }
  EXPECT_GE(resizes, 2U);
  ASSERT_EQ(n_occ, 7U);

  std::unordered_map<uint64_t, std::pair<double, uint64_t>> got;
  for (uint32_t j = 0; j < n_occ; ++j) {
    const uint32_t pos = occupied[j];
    got[ht_keys[pos]] = {ht_sums[pos], ht_counts[pos]};
  }
  EXPECT_DOUBLE_EQ(got[100].first, 10.0);
  EXPECT_EQ(got[100].second, 3ULL);
  EXPECT_DOUBLE_EQ(got[200].first, 5.0);
  EXPECT_EQ(got[200].second, 2ULL);
  EXPECT_DOUBLE_EQ(got[300].first, 0.0);
  EXPECT_EQ(got[300].second, 0ULL);  // valid=0 group created with count=0
  EXPECT_DOUBLE_EQ(got[600].first, 0.0);
  EXPECT_EQ(got[600].second, 0ULL);
  EXPECT_DOUBLE_EQ(got[700].first, 7.0);
  EXPECT_EQ(got[700].second, 1ULL);

  // Generation reuse + check_inf=true overflow check.
  ++cur_gen;
  std::vector<uint8_t> ht_inf(cap, 0);
  n_occ = 0;
  occupied.resize(max_load);
  const std::vector<uint64_t> inf_k{42, 42};
  const std::vector<double> inf_v{1e308, 1e308};
  const std::vector<uint8_t> inf_vld{1, 1};
  ASSERT_EQ(agg_inf->Accumulate(2, inf_k.data(), inf_v.data(), inf_vld.data(),
                                ht_keys.data(), ht_sums.data(),
                                ht_counts.data(), ht_gens.data(), ht_inf.data(),
                                cur_gen, static_cast<uint64_t>(cap - 1),
                                max_load, occupied.data(), &n_occ),
            2U);
  ASSERT_EQ(n_occ, 1U);
  EXPECT_EQ(ht_inf[occupied[0]], 1U);
  EXPECT_TRUE(std::isinf(ht_sums[occupied[0]]));
}

}  // namespace tinylamb
