/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/simd_comparison.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "common/constants.hpp"
#include "executor/selection_vector.hpp"

namespace tinylamb {

namespace {

inline void StoreWordMask(ValidityBitmap* out_mask, size_t word_index,
                          uint64_t word_mask, size_t valid_bits) {
  auto& words = out_mask->Words();
  if (valid_bits == 64) {
    words[word_index] = word_mask;
    return;
  }
  const uint64_t valid_mask = (uint64_t{1} << valid_bits) - 1;
  words[word_index] = word_mask & valid_mask;
}

inline void OrWordMask(ValidityBitmap* out_mask, size_t word_index,
                       uint64_t word_mask, size_t valid_bits) {
  auto& words = out_mask->Words();
  const uint64_t valid_mask = (uint64_t{1} << valid_bits) - 1;
  words[word_index] |= word_mask & valid_mask;
}

template <typename T, typename Cmp>
void EvaluateScalarComparison(const T* data, size_t count, T target,
                              Cmp compare, ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const size_t full_words = count / 64;
  for (size_t w = 0; w < full_words; ++w) {
    uint64_t word_mask = 0;
    const size_t base = w * 64;
    for (size_t i = 0; i < 64; ++i) {
      word_mask |= static_cast<uint64_t>(compare(data[base + i], target)) << i;
    }
    StoreWordMask(out_mask, w, word_mask, 64);
  }
  const size_t tail = count % 64;
  if (tail != 0) {
    uint64_t word_mask = 0;
    const size_t base = full_words * 64;
    for (size_t i = 0; i < tail; ++i) {
      word_mask |= static_cast<uint64_t>(compare(data[base + i], target)) << i;
    }
    StoreWordMask(out_mask, full_words, word_mask, tail);
  }
}

template <typename T, typename Cmp>
void EvaluateVectorComparison(const T* lhs, const T* rhs, size_t count,
                              Cmp compare, ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const size_t full_words = count / 64;
  for (size_t w = 0; w < full_words; ++w) {
    uint64_t word_mask = 0;
    const size_t base = w * 64;
    for (size_t i = 0; i < 64; ++i) {
      const size_t index = base + i;
      word_mask |= static_cast<uint64_t>(compare(lhs[index], rhs[index])) << i;
    }
    StoreWordMask(out_mask, w, word_mask, 64);
  }
  const size_t tail = count % 64;
  if (tail != 0) {
    uint64_t word_mask = 0;
    const size_t base = full_words * 64;
    for (size_t i = 0; i < tail; ++i) {
      const size_t index = base + i;
      word_mask |= static_cast<uint64_t>(compare(lhs[index], rhs[index])) << i;
    }
    StoreWordMask(out_mask, full_words, word_mask, tail);
  }
}

#if defined(__x86_64__) || defined(__i386__)
#if defined(__GNUC__) || defined(__clang__)
#define TINYLAMB_AVX2_TARGET __attribute__((target("avx2")))
#else
#define TINYLAMB_AVX2_TARGET
#endif

TINYLAMB_AVX2_TARGET
void EvaluateScalarInt64Avx2(const int64_t* data, size_t count,
                             BinaryOperation op, int64_t target,
                             ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const __m256i target_vector = _mm256_set1_epi64x(target);
  const size_t vector_count = count & ~size_t{3};
  for (size_t base = 0; base < vector_count; base += 4) {
    const __m256i values =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + base));
    __m256i comparison;
    switch (op) {
      case BinaryOperation::kEquals:
        comparison = _mm256_cmpeq_epi64(values, target_vector);
        break;
      case BinaryOperation::kNotEquals:
        comparison = _mm256_xor_si256(_mm256_cmpeq_epi64(values, target_vector),
                                      _mm256_set1_epi64x(-1));
        break;
      case BinaryOperation::kLessThan:
        comparison = _mm256_cmpgt_epi64(target_vector, values);
        break;
      case BinaryOperation::kLessThanEquals:
        comparison = _mm256_xor_si256(_mm256_cmpgt_epi64(values, target_vector),
                                      _mm256_set1_epi64x(-1));
        break;
      case BinaryOperation::kGreaterThan:
        comparison = _mm256_cmpgt_epi64(values, target_vector);
        break;
      case BinaryOperation::kGreaterThanEquals:
        comparison = _mm256_xor_si256(_mm256_cmpgt_epi64(target_vector, values),
                                      _mm256_set1_epi64x(-1));
        break;
      default:
        return;
    }
    const auto mask = static_cast<uint64_t>(
        _mm256_movemask_pd(_mm256_castsi256_pd(comparison)));
    out_mask->Words()[base / 64] |= mask << (base % 64);
  }
  if (vector_count != count) {
    const size_t base = vector_count;
    const size_t remaining = count - vector_count;
    uint64_t word_mask = 0;
    for (size_t i = 0; i < remaining; ++i) {
      const int64_t value = data[base + i];
      const bool matches =
          (op == BinaryOperation::kEquals && value == target) ||
          (op == BinaryOperation::kNotEquals && value != target) ||
          (op == BinaryOperation::kLessThan && value < target) ||
          (op == BinaryOperation::kLessThanEquals && value <= target) ||
          (op == BinaryOperation::kGreaterThan && value > target) ||
          (op == BinaryOperation::kGreaterThanEquals && value >= target);
      word_mask |= static_cast<uint64_t>(matches) << i;
    }
    OrWordMask(out_mask, vector_count / 64, word_mask << (vector_count % 64),
               (vector_count % 64) + remaining);
  }
}

TINYLAMB_AVX2_TARGET
void EvaluateVectorInt64Avx2(const int64_t* lhs, const int64_t* rhs,
                             size_t count, BinaryOperation op,
                             ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const size_t vector_count = count & ~size_t{3};
  for (size_t base = 0; base < vector_count; base += 4) {
    const __m256i left =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + base));
    const __m256i right =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + base));
    __m256i comparison;
    switch (op) {
      case BinaryOperation::kEquals:
        comparison = _mm256_cmpeq_epi64(left, right);
        break;
      case BinaryOperation::kNotEquals:
        comparison = _mm256_xor_si256(_mm256_cmpeq_epi64(left, right),
                                      _mm256_set1_epi64x(-1));
        break;
      case BinaryOperation::kLessThan:
        comparison = _mm256_cmpgt_epi64(right, left);
        break;
      case BinaryOperation::kLessThanEquals:
        comparison = _mm256_xor_si256(_mm256_cmpgt_epi64(left, right),
                                      _mm256_set1_epi64x(-1));
        break;
      case BinaryOperation::kGreaterThan:
        comparison = _mm256_cmpgt_epi64(left, right);
        break;
      case BinaryOperation::kGreaterThanEquals:
        comparison = _mm256_xor_si256(_mm256_cmpgt_epi64(right, left),
                                      _mm256_set1_epi64x(-1));
        break;
      default:
        return;
    }
    const auto mask = static_cast<uint64_t>(
        _mm256_movemask_pd(_mm256_castsi256_pd(comparison)));
    out_mask->Words()[base / 64] |= mask << (base % 64);
  }
  if (vector_count != count) {
    const size_t base = vector_count;
    const size_t remaining = count - vector_count;
    uint64_t word_mask = 0;
    for (size_t i = 0; i < remaining; ++i) {
      const int64_t left = lhs[base + i];
      const int64_t right = rhs[base + i];
      const bool matches =
          (op == BinaryOperation::kEquals && left == right) ||
          (op == BinaryOperation::kNotEquals && left != right) ||
          (op == BinaryOperation::kLessThan && left < right) ||
          (op == BinaryOperation::kLessThanEquals && left <= right) ||
          (op == BinaryOperation::kGreaterThan && left > right) ||
          (op == BinaryOperation::kGreaterThanEquals && left >= right);
      word_mask |= static_cast<uint64_t>(matches) << i;
    }
    OrWordMask(out_mask, vector_count / 64, word_mask << (vector_count % 64),
               (vector_count % 64) + remaining);
  }
}

template <int Comparison>
TINYLAMB_AVX2_TARGET void EvaluateScalarDoubleAvx2Op(const double* data,
                                                     size_t count,
                                                     double target,
                                                     BinaryOperation op,
                                                     ValidityBitmap* out_mask) {
  out_mask->Reset(count, false);
  const __m256d target_vector = _mm256_set1_pd(target);
  const size_t vector_count = count & ~size_t{3};
  for (size_t base = 0; base < vector_count; base += 4) {
    const __m256d values = _mm256_loadu_pd(data + base);
    const __m256d comparison = _mm256_cmp_pd(values, target_vector, Comparison);
    const auto mask = static_cast<uint64_t>(_mm256_movemask_pd(comparison));
    out_mask->Words()[base / 64] |= mask << (base % 64);
  }
  if (vector_count != count) {
    const size_t base = vector_count;
    const size_t remaining = count - vector_count;
    uint64_t word_mask = 0;
    for (size_t i = 0; i < remaining; ++i) {
      const double value = data[base + i];
      const bool matches =
          (op == BinaryOperation::kEquals && value == target) ||
          (op == BinaryOperation::kNotEquals && value != target) ||
          (op == BinaryOperation::kLessThan && value < target) ||
          (op == BinaryOperation::kLessThanEquals && value <= target) ||
          (op == BinaryOperation::kGreaterThan && value > target) ||
          (op == BinaryOperation::kGreaterThanEquals && value >= target);
      word_mask |= static_cast<uint64_t>(matches) << i;
    }
    OrWordMask(out_mask, vector_count / 64, word_mask << (vector_count % 64),
               (vector_count % 64) + remaining);
  }
}

TINYLAMB_AVX2_TARGET
void EvaluateScalarDoubleAvx2(const double* data, size_t count,
                              BinaryOperation op, double target,
                              ValidityBitmap* out_mask) {
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateScalarDoubleAvx2Op<_CMP_EQ_OQ>(data, count, target, op, out_mask);
      return;
    case BinaryOperation::kNotEquals:
      EvaluateScalarDoubleAvx2Op<_CMP_NEQ_UQ>(data, count, target, op,
                                              out_mask);
      return;
    case BinaryOperation::kLessThan:
      EvaluateScalarDoubleAvx2Op<_CMP_LT_OQ>(data, count, target, op, out_mask);
      return;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarDoubleAvx2Op<_CMP_LE_OQ>(data, count, target, op, out_mask);
      return;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarDoubleAvx2Op<_CMP_GT_OQ>(data, count, target, op, out_mask);
      return;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarDoubleAvx2Op<_CMP_GE_OQ>(data, count, target, op, out_mask);
      return;
    default:
      out_mask->Reset(count, false);
      return;
  }
}

bool HasAvx2() {
  static const bool supported = __builtin_cpu_supports("avx2");
  return supported;
}
#endif

}  // namespace

void SimdComparisonKernel::CompareInt64(const int64_t* data, size_t count,
                                        BinaryOperation op, int64_t target,
                                        ValidityBitmap* out_mask) {
  assert(out_mask != nullptr);
#if defined(__x86_64__) || defined(__i386__)
  if (HasAvx2()) {
    EvaluateScalarInt64Avx2(data, count, op, target, out_mask);
    return;
  }
#endif
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateScalarComparison(data, count, target, std::equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateScalarComparison(data, count, target, std::not_equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateScalarComparison(data, count, target, std::less<>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarComparison(data, count, target, std::less_equal<>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarComparison(data, count, target, std::greater<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarComparison(data, count, target, std::greater_equal<>(),
                               out_mask);
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
#if defined(__x86_64__) || defined(__i386__)
  if (HasAvx2()) {
    EvaluateScalarDoubleAvx2(data, count, op, target, out_mask);
    return;
  }
#endif
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateScalarComparison(data, count, target, std::equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateScalarComparison(data, count, target, std::not_equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateScalarComparison(data, count, target, std::less<>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarComparison(data, count, target, std::less_equal<>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarComparison(data, count, target, std::greater<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarComparison(data, count, target, std::greater_equal<>(),
                               out_mask);
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
      EvaluateScalarComparison(data, count, target, std::equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateScalarComparison(data, count, target, std::not_equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateScalarComparison(data, count, target, std::less<>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateScalarComparison(data, count, target, std::less_equal<>(),
                               out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateScalarComparison(data, count, target, std::greater<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateScalarComparison(data, count, target, std::greater_equal<>(),
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
#if defined(__x86_64__) || defined(__i386__)
  if (HasAvx2()) {
    EvaluateVectorInt64Avx2(lhs, rhs, count, op, out_mask);
    return;
  }
#endif
  switch (op) {
    case BinaryOperation::kEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::equal_to<>(), out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::not_equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateVectorComparison(lhs, rhs, count, std::less<>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::less_equal<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateVectorComparison(lhs, rhs, count, std::greater<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::greater_equal<>(),
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
      EvaluateVectorComparison(lhs, rhs, count, std::equal_to<>(), out_mask);
      break;
    case BinaryOperation::kNotEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::not_equal_to<>(),
                               out_mask);
      break;
    case BinaryOperation::kLessThan:
      EvaluateVectorComparison(lhs, rhs, count, std::less<>(), out_mask);
      break;
    case BinaryOperation::kLessThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::less_equal<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThan:
      EvaluateVectorComparison(lhs, rhs, count, std::greater<>(), out_mask);
      break;
    case BinaryOperation::kGreaterThanEquals:
      EvaluateVectorComparison(lhs, rhs, count, std::greater_equal<>(),
                               out_mask);
      break;
    default:
      out_mask->Reset(count, false);
      break;
  }
}

}  // namespace tinylamb
