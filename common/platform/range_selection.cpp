/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/range_selection.hpp"

#include <bit>
#include <cstdint>

#if (defined(__x86_64__) || defined(__i386__)) && \
    (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#define TINYLAMB_RANGE_X86 1
#endif

namespace tinylamb::platform {
namespace {

uint32_t SelectScalar(const int64_t* values, uint32_t count, int64_t lower,
                      int64_t upper, uint32_t* output) {
  uint32_t selected = 0;
  const auto lo = static_cast<uint64_t>(lower);
  const uint64_t span = static_cast<uint64_t>(upper) - lo;
  for (uint32_t row = 0; row < count; ++row) {
    if (static_cast<uint64_t>(values[row]) - lo <= span) {
      output[selected++] = row;
}
  }
  return selected;
}

template <bool lower_inclusive, bool upper_inclusive>
uint32_t CompactDoubleScalar(const int64_t* values, uint32_t* positions,
                             uint32_t count, double lower, double upper) {
  uint32_t out = 0;
  for (uint32_t i = 0; i < count; ++i) {
    const uint32_t row = positions[i];
    const auto v = std::bit_cast<double>(values[row]);
    const bool hit = (lower_inclusive ? v >= lower : v > lower) &
                     (upper_inclusive ? v <= upper : v < upper);
    positions[out] = row;
    out += static_cast<uint32_t>(hit);
  }
  return out;
}

#ifdef TINYLAMB_RANGE_X86
__attribute__((target("avx2"))) uint32_t SelectAvx2(const int64_t* values,
                                                    uint32_t count,
                                                    int64_t lower,
                                                    int64_t upper,
                                                    uint32_t* output) {
  const __m256i lo = _mm256_set1_epi64x(lower);
  const __m256i hi = _mm256_set1_epi64x(upper);
  uint32_t selected = 0, row = 0;
  const uint32_t vector_end = count & ~uint32_t{3};
  for (; row < vector_end; row += 4) {
    const __m256i v =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + row));
    const __m256i rejected =
        _mm256_or_si256(_mm256_cmpgt_epi64(lo, v), _mm256_cmpgt_epi64(v, hi));
    uint32_t mask = static_cast<uint32_t>(
                        ~_mm256_movemask_pd(_mm256_castsi256_pd(rejected))) &
                    15;
    while (mask != 0) {
      output[selected++] = row + std::countr_zero(mask);
      mask &= mask - 1;
    }
  }
  for (; row < count; ++row) {
    if (values[row] >= lower && values[row] <= upper) { output[selected++] = row;
}
}
  return selected;
}

__attribute__((target("avx512f,avx512vl"))) uint32_t
SelectAvx512(const int64_t* values, uint32_t count, int64_t lower,
             int64_t upper, uint32_t* output) {
  const __m512i lo = _mm512_set1_epi64(lower);
  const __m512i hi = _mm512_set1_epi64(upper);
  const __m256i offsets = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
  uint32_t selected = 0, row = 0;
  const uint32_t vector_end = count & ~uint32_t{7};
  for (; row < vector_end; row += 8) {
    const __m512i v = _mm512_loadu_si512(values + row);
    const __mmask8 mask = _mm512_cmp_epi64_mask(v, lo, _MM_CMPINT_GE) &
                          _mm512_cmp_epi64_mask(v, hi, _MM_CMPINT_LE);
    const __m256i positions = _mm256_add_epi32(
        offsets, _mm256_set1_epi32(std::bit_cast<int32_t>(row)));
    _mm256_mask_compressstoreu_epi32(output + selected, mask, positions);
    selected += std::popcount(static_cast<uint32_t>(mask));
  }
  for (; row < count; ++row) {
    if (values[row] >= lower && values[row] <= upper) { output[selected++] = row;
}
}
  return selected;
}
template <bool lower_inclusive, bool upper_inclusive>
__attribute__((target("avx512f,avx512vl"))) uint32_t
CompactDoubleAvx512(const int64_t* values, uint32_t* positions, uint32_t count,
                    double lower, double upper) {
  const __m512d lo = _mm512_set1_pd(lower), hi = _mm512_set1_pd(upper);
  uint32_t out = 0, i = 0;
  const uint32_t end = count & ~uint32_t{7};
  for (; i < end; i += 8) {
    const __m256i rows =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(positions + i));
    // Use unsigned 32-bit positions even when the caller spans >2^31 rows.
    const __m512i offsets = _mm512_cvtepu32_epi64(rows);
    const __m512d v =
        _mm512_castsi512_pd(_mm512_i64gather_epi64(offsets, values, 8));
    const __mmask8 mask =
        _mm512_cmp_pd_mask(v, lo, lower_inclusive ? _CMP_GE_OQ : _CMP_GT_OQ) &
        _mm512_cmp_pd_mask(v, hi, upper_inclusive ? _CMP_LE_OQ : _CMP_LT_OQ);
    _mm256_mask_compressstoreu_epi32(positions + out, mask, rows);
    out += std::popcount(static_cast<uint32_t>(mask));
  }
  for (; i < count; ++i) {
    const uint32_t row = positions[i];
    const auto v = std::bit_cast<double>(values[row]);
    if ((lower_inclusive ? v >= lower : v > lower) &
        (upper_inclusive ? v <= upper : v < upper)) {
      positions[out++] = row;
}
  }
  return out;
}

#endif

}  // namespace

uint32_t SelectInt64ClosedRange(const int64_t* values, uint32_t count,
                                int64_t lower, int64_t upper,
                                uint32_t* output) {
  if (lower > upper || count == 0) { return 0;
}
  using Kernel = decltype(&SelectScalar);
  static const Kernel kernel = []() -> Kernel {
#ifdef TINYLAMB_RANGE_X86
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl")) {
      return SelectAvx512;
}
    if (__builtin_cpu_supports("avx2")) { return SelectAvx2;
}
#endif
    return SelectScalar;
  }();
  return kernel(values, count, lower, upper, output);
}

uint32_t CompactDoubleRange(const int64_t* values, uint32_t* positions,
                            uint32_t count, double lower, double upper,
                            bool lower_inclusive, bool upper_inclusive) {
  using Kernel = decltype(&CompactDoubleScalar<true, true>);
  static const auto kernels = [] {
    struct Kernels {
      // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
      Kernel functions[4];
    };
#ifdef TINYLAMB_RANGE_X86
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl")) {
      return Kernels{
          {CompactDoubleAvx512<false, false>, CompactDoubleAvx512<false, true>,
           CompactDoubleAvx512<true, false>, CompactDoubleAvx512<true, true>}};
}
#endif
    return Kernels{
        {CompactDoubleScalar<false, false>, CompactDoubleScalar<false, true>,
         CompactDoubleScalar<true, false>, CompactDoubleScalar<true, true>}};
  }();
  return kernels.functions[(2 * static_cast<int>(lower_inclusive)) + static_cast<int>(upper_inclusive)](
      values, positions, count, lower, upper);
}

}  // namespace tinylamb::platform
