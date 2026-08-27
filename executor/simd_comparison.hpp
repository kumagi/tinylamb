/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_SIMD_COMPARISON_HPP
#define TINYLAMB_EXECUTOR_SIMD_COMPARISON_HPP

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "executor/selection_vector.hpp"
#include "expression/binary_expression.hpp"

namespace tinylamb {

// SIMD and autovectorization-optimized vectorized comparison kernels for
// primitive column vectors (int64_t, double, string prefix).
class SimdComparisonKernel {
 public:
  // Vector-constant comparisons
  static void CompareInt64(const int64_t* data, size_t count,
                           BinaryOperation op, int64_t target,
                           ValidityBitmap* out_mask);

  static void CompareDouble(const double* data, size_t count,
                            BinaryOperation op, double target,
                            ValidityBitmap* out_mask);

  static void CompareStringPrefix(const std::string_view* data, size_t count,
                                  BinaryOperation op, std::string_view target,
                                  ValidityBitmap* out_mask);

  // Vector-vector comparisons
  static void CompareInt64Vectors(const int64_t* lhs, const int64_t* rhs,
                                  size_t count, BinaryOperation op,
                                  ValidityBitmap* out_mask);

  static void CompareDoubleVectors(const double* lhs, const double* rhs,
                                   size_t count, BinaryOperation op,
                                   ValidityBitmap* out_mask);
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_SIMD_COMPARISON_HPP
