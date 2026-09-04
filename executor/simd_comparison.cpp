/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/simd_comparison.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "executor/selection_vector.hpp"
#include "expression/binary_expression.hpp"

namespace tinylamb {

namespace {

template <typename T, typename Cmp>
void EvaluateScalarComparison(const T* data, size_t count, T target,
                              Cmp compare, ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const size_t words = (count + 63) / 64;
  for (size_t w = 0; w < words; ++w) {
    uint64_t word_mask = 0;
    const size_t base = w * 64;
    const size_t limit = std::min(count - base, size_t{64});

    for (size_t i = 0; i < limit; ++i) {
      if (compare(data[base + i], target)) {
        word_mask |= (1ULL << i);
      }
    }
    if (word_mask != 0) {
      for (size_t i = 0; i < limit; ++i) {
        if ((word_mask >> i) & 1ULL) {
          out_mask->SetBit(base + i);
        }
      }
    }
  }
}

template <typename T, typename Cmp>
void EvaluateVectorComparison(const T* lhs, const T* rhs, size_t count,
                              Cmp compare, ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const size_t words = (count + 63) / 64;
  for (size_t w = 0; w < words; ++w) {
    uint64_t word_mask = 0;
    const size_t base = w * 64;
    const size_t limit = std::min(count - base, size_t{64});

    for (size_t i = 0; i < limit; ++i) {
      if (compare(lhs[base + i], rhs[base + i])) {
        word_mask |= (1ULL << i);
      }
    }
    if (word_mask != 0) {
      for (size_t i = 0; i < limit; ++i) {
        if ((word_mask >> i) & 1ULL) {
          out_mask->SetBit(base + i);
        }
      }
    }
  }
}

}  // namespace

void SimdComparisonKernel::CompareInt64(const int64_t* data, size_t count,
                                        BinaryOperation op, int64_t target,
                                        ValidityBitmap* out_mask) {
  assert(out_mask != nullptr);
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateScalarComparison(data, count, target, std::equal_to<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateScalarComparison(data, count, target,
                               std::not_equal_to<int64_t>(), out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateScalarComparison(data, count, target, std::less<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarComparison(data, count, target, std::less_equal<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarComparison(data, count, target, std::greater<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarComparison(data, count, target,
                               std::greater_equal<int64_t>(), out_mask);
      break;
    default:
      out_mask->Reset(count, false);
      break;
  }
}

void SimdComparisonKernel::CompareDouble(const double* data, size_t count,
                                         BinaryOperation op, double target,
                                         ValidityBitmap* out_mask) {
  assert(out_mask != nullptr);
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateScalarComparison(data, count, target, std::equal_to<double>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateScalarComparison(data, count, target, std::not_equal_to<double>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateScalarComparison(data, count, target, std::less<double>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarComparison(data, count, target, std::less_equal<double>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarComparison(data, count, target, std::greater<double>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarComparison(data, count, target,
                               std::greater_equal<double>(), out_mask);
      break;
    default:
      out_mask->Reset(count, false);
      break;
  }
}

void SimdComparisonKernel::CompareStringPrefix(const std::string_view* data,
                                               size_t count, BinaryOperation op,
                                               std::string_view target,
                                               ValidityBitmap* out_mask) {
  assert(out_mask != nullptr);
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateScalarComparison(data, count, target,
                               std::equal_to<std::string_view>(), out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateScalarComparison(data, count, target,
                               std::not_equal_to<std::string_view>(), out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateScalarComparison(data, count, target,
                               std::less<std::string_view>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarComparison(data, count, target,
                               std::less_equal<std::string_view>(), out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarComparison(data, count, target,
                               std::greater<std::string_view>(), out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarComparison(data, count, target,
                               std::greater_equal<std::string_view>(),
                               out_mask);
      break;
    default:
      out_mask->Reset(count, false);
      break;
  }
}

void SimdComparisonKernel::CompareInt64Vectors(const int64_t* lhs,
                                               const int64_t* rhs, size_t count,
                                               BinaryOperation op,
                                               ValidityBitmap* out_mask) {
  assert(out_mask != nullptr);
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::equal_to<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::not_equal_to<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateVectorComparison(lhs, rhs, count, std::less<int64_t>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::less_equal<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateVectorComparison(lhs, rhs, count, std::greater<int64_t>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::greater_equal<int64_t>(),
                               out_mask);
      break;
    default:
      out_mask->Reset(count, false);
      break;
  }
}

void SimdComparisonKernel::CompareDoubleVectors(const double* lhs,
                                                const double* rhs, size_t count,
                                                BinaryOperation op,
                                                ValidityBitmap* out_mask) {
  assert(out_mask != nullptr);
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::equal_to<double>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::not_equal_to<double>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateVectorComparison(lhs, rhs, count, std::less<double>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::less_equal<double>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateVectorComparison(lhs, rhs, count, std::greater<double>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::greater_equal<double>(),
                               out_mask);
      break;
    default:
      out_mask->Reset(count, false);
      break;
  }
}

}  // namespace tinylamb
