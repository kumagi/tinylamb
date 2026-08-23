/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/jit.hpp"

#include <cstdint>
#include <cstddef>
#include <numeric>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(JitTest, FilterProjectionAndAggregateKernelsMatchScalarResults) {
  std::vector<int64_t> input(4096);
  std::iota(input.begin(), input.end(), int64_t{-2048});
  auto filter = JitInt64Kernels::CompileFilter(
      BinaryOperation::kGreaterThanEquals);
  auto projection = JitInt64Kernels::CompileProjection();
  auto sum = JitInt64Kernels::CompileSum();
  if (!filter.has_value() || !projection.has_value() || !sum.has_value()) {
    GTEST_FAIL() << "kernel compilation failed";
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

TEST(JitTest, FilterKernelsForEveryComparisonOperator) {
  std::vector<int64_t> input(1024);
  std::iota(input.begin(), input.end(), int64_t{-512});
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
      BinaryOperation::kEquals,          BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,        BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan,     BinaryOperation::kGreaterThanEquals,
  };

  // Act + Assert -- every comparison operator compiles and agrees with the
  // scalar reference implementation.
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

TEST(JitTest, MoveAssignmentTransfersKernel) {
  auto filter = JitInt64Kernels::CompileFilter(BinaryOperation::kEquals);
  auto projection = JitInt64Kernels::CompileProjection();
  if (!filter.has_value() || !projection.has_value()) {
    GTEST_FAIL() << "kernel compilation failed";
    return;
  }

  // Act -- move-assign the filter kernel over the projection kernel.
  projection = std::move(filter);

  // Assert -- the surviving object now holds the filter kernel, so calling
  // Project on it throws instead of miscompiling.
  std::vector<int64_t> input{1, 2, 3};
  std::vector<int64_t> output(input.size());
  EXPECT_THROW(projection->Project(input.data(), output.data(), input.size(),
                                   2, 1),
               std::logic_error);
}

TEST(JitTest, ReusesCompiledKernelsProcessWide) {
  auto first = JitInt64Kernels::CompileFilter(BinaryOperation::kEquals);
  if (!first.has_value()) {
    GTEST_FAIL() << "kernel compilation failed";
    return;
  }
  const double first_ms = first->CompileMilliseconds();
  auto second = JitInt64Kernels::CompileFilter(BinaryOperation::kEquals);
  if (!second.has_value()) {
    GTEST_FAIL() << "kernel compilation failed";
    return;
  }
  EXPECT_DOUBLE_EQ(second->CompileMilliseconds(), first_ms);
  EXPECT_GT(first_ms, 0.0);
}

TEST(JitTest, NonComparisonFilterIsRejected) {
  // Act + Assert -- non-comparison operations have no JIT kernel and must
  // fall back to std::nullopt instead of compiling.
  EXPECT_FALSE(JitInt64Kernels::CompileFilter(BinaryOperation::kAdd));
  EXPECT_FALSE(JitInt64Kernels::CompileFilter(BinaryOperation::kAnd));
}

TEST(JitTest, WrongKernelUsageThrows) {
  auto filter = JitInt64Kernels::CompileFilter(BinaryOperation::kLessThan);
  auto projection = JitInt64Kernels::CompileProjection();
  auto sum = JitInt64Kernels::CompileSum();
  if (!filter.has_value() || !projection.has_value() || !sum.has_value()) {
    GTEST_FAIL() << "kernel compilation failed";
    return;
  }

  std::vector<int64_t> input{1, 2, 3};
  std::vector<uint8_t> selected(input.size());
  std::vector<int64_t> output(input.size());

  // Act + Assert -- invoking a kernel through the wrong accessor throws.
  EXPECT_THROW(projection->Filter(input.data(), selected.data(), input.size(),
                                  0),
               std::logic_error);
  EXPECT_THROW(sum->Filter(input.data(), selected.data(), input.size(), 0),
               std::logic_error);
  EXPECT_THROW(std::ignore = filter->Sum(input.data(), input.size()),
               std::logic_error);
  EXPECT_THROW(sum->Project(input.data(), output.data(), input.size(), 1, 1),
               std::logic_error);
}

}  // namespace tinylamb
