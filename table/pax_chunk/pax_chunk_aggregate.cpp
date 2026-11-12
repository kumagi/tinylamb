/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <bit>
#include <utility>

#include "table/pax_chunk/pax_chunk.hpp"
#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb {

bool TryAccumulateScaledSum(int64_t coefficient, __int128 subtotal,
                            __int128* total) {
  constexpr __int128 kMin = std::numeric_limits<__int128>::min();
  constexpr __int128 kMax = std::numeric_limits<__int128>::max();
  // Check before multiplying: even the intermediate must fit in 128 bits.
  // Handling -1 separately also avoids the undefined kMin / -1 division.
  if (coefficient == -1) {
    if (subtotal == kMin) {
      return false;
    }
  } else if (coefficient > 1) {
    if (subtotal > kMax / coefficient || subtotal < kMin / coefficient) {
      return false;
    }
  } else if (coefficient < -1) {
    if (subtotal > kMin / coefficient || subtotal < kMax / coefficient) {
      return false;
    }
  }
  const __int128 product = subtotal * coefficient;
  if ((product > 0 && *total > kMax - product) ||
      (product < 0 && *total < kMin - product)) {
    return false;
  }
  *total += product;
  return true;
}

// ---------------------------------------------------------------------------
// Vectorized scan engine (compressed-data processing)
// ---------------------------------------------------------------------------

namespace {

inline bool ScanMatch(int64_t v, ScanOp op, int64_t lit) {
  switch (op) {
    case ScanOp::kEq:
      return v == lit;
    case ScanOp::kLe:
      return v <= lit;
    case ScanOp::kLt:
      return v < lit;
    case ScanOp::kGe:
      return v >= lit;
    case ScanOp::kGt:
      return v > lit;
  }
  return false;
}

// Vectorized form of the scalar row loop
//   for i in [0,n): if (ScanMatch(vals[i], op, lit)) bits[i>>6] |= bit(i)
// used by BuildSelectionVector's generic (decode-then-compare) fallback.
// Four int64 lanes per compare, movemask to 4 result bits, then one
// shift+or per lane folds them into the 64-bit word. Ops outside the five
// ScanMatch cases return false so the caller keeps scalar semantics
// (unknown ops match nothing). `bits` holds ceil(n/64) row-major words and
// is assumed zero-initialized.
#ifdef __AVX2__
namespace predicate_mask {
enum class Cmp { kEq, kLt, kLe, kGt, kGe };

#ifdef __x86_64__
// Runtime gate for the 8-lane kernels below. The library is compiled with
// -mavx2, so the AVX-512 bodies live behind target attributes and are only
// entered on CPUs that report F/BW/VL (Zen 4/5, Ice Lake+).
inline bool HasAvx512() {
  static const bool kHas = __builtin_cpu_supports("avx512f") &&
                           __builtin_cpu_supports("avx512bw") &&
                           __builtin_cpu_supports("avx512vl");
  return kHas;
}

// GCC resolves the AVX-512 intrinsics at template-body parse time, so the
// ISA switch has to be a pragma region rather than a per-function target
// attribute.
//
// DO NOT DROP the __attribute__((target(...))) on these three bodies. Clang
// ignores `#pragma GCC target` outright -- it warns "unknown pragma ignored"
// and then compiles the intrinsics with NO ISA feature, so every _mm512_* use
// fails with "requires target feature 'avx512f'" and the library does not
// build at all. clang is this project's default compiler and what CI builds,
// and the same file already uses the attribute form on DecodePlainAvx512, so
// the pragma region (kept for GCC) and the attribute (honoured by clang) are
// complementary: same ISA switch, same HasAvx512() runtime gate, no behaviour
// difference between the two compilers. The attribute goes AFTER the
// template-head, which is the only position where it applies to the body. Clang
// defers intrinsic resolution and rejects push_options, so the region is
// GCC-only; every function below carries its own target attribute, which
// suffices there.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC push_options
#pragma GCC target("avx512f,avx512bw,avx512vl")
#endif
template <Cmp C>
__attribute__((target("avx512f,avx512bw,avx512vl"))) inline uint32_t Mask8(
    __m512i v, __m512i vlit) {
  if constexpr (C == Cmp::kEq) {
    return static_cast<uint32_t>(_mm512_cmpeq_epi64_mask(v, vlit));
  } else if constexpr (C == Cmp::kLt) {  // v < lit <=> lit > v
    return static_cast<uint32_t>(_mm512_cmpgt_epi64_mask(vlit, v));
  } else if constexpr (C == Cmp::kLe) {  // v <= lit <=> !(v > lit)
    return static_cast<uint32_t>(~_mm512_cmpgt_epi64_mask(v, vlit)) & 0xFFU;
  } else if constexpr (C == Cmp::kGt) {
    return static_cast<uint32_t>(_mm512_cmpgt_epi64_mask(v, vlit));
  } else {  // kGe: v >= lit <=> !(lit > v)
    return static_cast<uint32_t>(~_mm512_cmpgt_epi64_mask(vlit, v)) & 0xFFU;
  }
}

// 8 int64 lanes per compare with the mask register delivering the result
// bits directly (no movemask round-trip). Tail rows reuse the AVX2/scalar
// code below so only the 64-row body is ISA-specific.
template <Cmp C>
__attribute__((target("avx512f,avx512bw,avx512vl"))) void FillMaskAvx512(
    const int64_t* vals, uint32_t n, ScanOp op, int64_t lit, uint64_t* bits) {
  const __m512i vlit = _mm512_set1_epi64(lit);
  uint32_t i = 0;
  size_t out = 0;
  for (; i + 64 <= n; i += 64, ++out) {
    uint64_t word = 0;
    for (int j = 0; j < 8; ++j) {
      const __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(
          vals + i + (static_cast<ptrdiff_t>(8) * j)));
      word |= static_cast<uint64_t>(Mask8<C>(v, vlit)) << (8 * j);
    }
    bits[out] = word;
  }
  for (; i < n; ++i) {
    if (ScanMatch(vals[i], op, lit)) {
      bits[i >> 6] |= 1ULL << (i & 63);
    }
  }
}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif
#endif  // __x86_64__

template <Cmp C>
inline int Mask4(__m256i v, __m256i vlit) {
  if constexpr (C == Cmp::kEq) {
    return _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(v, vlit)));
  } else if constexpr (C == Cmp::kLt) {  // v <  lit  <=>  lit > v
    return _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(vlit, v)));
  } else if constexpr (C == Cmp::kLe) {  // v <= lit  <=>  !(v > lit)
    return ~_mm256_movemask_pd(
               _mm256_castsi256_pd(_mm256_cmpgt_epi64(v, vlit))) &
           0xF;
  } else if constexpr (C == Cmp::kGt) {
    return _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(v, vlit)));
  } else {  // kGe: v >= lit <=> !(lit > v)
    return ~_mm256_movemask_pd(
               _mm256_castsi256_pd(_mm256_cmpgt_epi64(vlit, v))) &
           0xF;
  }
}

template <Cmp C>
void FillMask(const int64_t* vals, uint32_t n, ScanOp op, int64_t lit,
              uint64_t* bits) {
  const __m256i vlit = _mm256_set1_epi64x(lit);
  uint32_t i = 0;
  for (; i + 64 <= n; i += 64) {
    uint64_t word = 0;
    for (int j = 0; j < 16; ++j) {
      const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(
          vals + i + (static_cast<ptrdiff_t>(4) * j)));
      word |= static_cast<uint64_t>(static_cast<uint16_t>(Mask4<C>(v, vlit)))
              << (4 * j);
    }
    bits[i >> 6] = word;
  }
  for (; i < n; ++i) {
    if (ScanMatch(vals[i], op, lit)) {
      bits[i >> 6] |= 1ULL << (i & 63);
    }
  }
}
}  // namespace predicate_mask

// Dispatch + op gate; false means the caller must use the scalar loop.
bool FillPredicateMaskAvx2(const int64_t* vals, uint32_t n, ScanOp op,
                           int64_t lit, uint64_t* bits) {
  using predicate_mask::Cmp;
  using predicate_mask::FillMask;
#ifdef __x86_64__
  if (predicate_mask::HasAvx512()) {
    switch (op) {
      case ScanOp::kEq:
        predicate_mask::FillMaskAvx512<Cmp::kEq>(vals, n, op, lit, bits);
        return true;
      case ScanOp::kLt:
        predicate_mask::FillMaskAvx512<Cmp::kLt>(vals, n, op, lit, bits);
        return true;
      case ScanOp::kLe:
        predicate_mask::FillMaskAvx512<Cmp::kLe>(vals, n, op, lit, bits);
        return true;
      case ScanOp::kGt:
        predicate_mask::FillMaskAvx512<Cmp::kGt>(vals, n, op, lit, bits);
        return true;
      case ScanOp::kGe:
        predicate_mask::FillMaskAvx512<Cmp::kGe>(vals, n, op, lit, bits);
        return true;
    }
    return false;
  }
#endif
  switch (op) {
    case ScanOp::kEq:
      FillMask<Cmp::kEq>(vals, n, op, lit, bits);
      return true;
    case ScanOp::kLt:
      FillMask<Cmp::kLt>(vals, n, op, lit, bits);
      return true;
    case ScanOp::kLe:
      FillMask<Cmp::kLe>(vals, n, op, lit, bits);
      return true;
    case ScanOp::kGt:
      FillMask<Cmp::kGt>(vals, n, op, lit, bits);
      return true;
    case ScanOp::kGe:
      FillMask<Cmp::kGe>(vals, n, op, lit, bits);
      return true;
  }
  return false;
}

struct PlainPredicateMask {
  using Cmp = predicate_mask::Cmp;

#ifdef __x86_64__
  // 8-row body of the BE-region kernel: one 64B load + one byte-swap
  // shuffle + one vpcmp with a direct mask-register result. Same tail
  // discipline as FillMaskAvx512: only full 64-row words are ISA-specific.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC push_options
#pragma GCC target("avx512f,avx512bw,avx512vl")
#endif
  template <Cmp C>
  __attribute__((target("avx512f,avx512bw,avx512vl"))) static void
  FillPlainMaskAvx512(const char* region, uint32_t n, ScanOp op, int64_t lit,
                      uint64_t* bits) {
    // vpshufb works per 128-bit lane, so each 16-byte control group is the
    // AVX2 byte-swap pattern ([7..0, 15..8]); _mm512_set_epi8 names bytes
    // from 63 down, hence the reversed sequence repeated four times.
    const __m512i bswap = _mm512_set_epi8(
        8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1,
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7);
    const __m512i vlit = _mm512_set1_epi64(lit);
    uint32_t i = 0;
    size_t out = 0;
    for (; i + 64 <= n; i += 64, ++out) {
      uint64_t word = 0;
      for (uint32_t j = 0; j < 8; ++j) {
        const __m512i v = _mm512_shuffle_epi8(
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(
                region +
                (static_cast<size_t>(i + (8U * j)) * sizeof(int64_t)))),
            bswap);
        word |= static_cast<uint64_t>(predicate_mask::Mask8<C>(v, vlit))
                << (8U * j);
      }
      bits[out] = word;
    }
    // Remainder: the AVX2 body already stops at 64-row words, so its
    // 16/4/1-row tails are reused verbatim (called directly: the partial
    // word is never worth a second runtime dispatch).
    FillPlainMaskAvx2Body<C>(
        region + (static_cast<size_t>(i) * sizeof(int64_t)), n - i, op, lit,
        bits + out);
  }
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif

  template <Cmp C>
  static void FillPlainMask(const char* region, uint32_t n, ScanOp op,
                            int64_t lit, uint64_t* bits) {
    FillPlainMaskAvx512Guard<C>(region, n, op, lit, bits);
  }

  // Shared entry: dispatches to the AVX-512 body when the CPU reports it,
  // otherwise runs the historical AVX2 body (the AVX-512 body's own tail
  // calls the AVX2 body directly, so there is no second dispatch).
  template <Cmp C>
  static void FillPlainMaskAvx512Guard(const char* region, uint32_t n,
                                       ScanOp op, int64_t lit, uint64_t* bits) {
    if (predicate_mask::HasAvx512()) {
      FillPlainMaskAvx512<C>(region, n, op, lit, bits);
      return;
    }
    FillPlainMaskAvx2Body<C>(region, n, op, lit, bits);
  }
#else
  template <Cmp C>
  static void FillPlainMask(const char* region, uint32_t n, ScanOp op,
                            int64_t lit, uint64_t* bits) {
    FillPlainMaskAvx2Body<C>(region, n, op, lit, bits);
  }
#endif

  template <Cmp C>
  static void FillPlainMaskAvx2Body(const char* region, uint32_t n, ScanOp op,
                                    int64_t lit, uint64_t* bits) {
    const __m256i bswap =
        _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
                         7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    const __m256i vlit = _mm256_set1_epi64x(lit);
    const auto load = [&](uint32_t row) {
      return _mm256_shuffle_epi8(
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(
              region + (static_cast<size_t>(row) * sizeof(int64_t)))),
          bswap);
    };
    const auto mask16 = [&](uint32_t row) -> uint16_t {
      const uint32_t m0 =
          static_cast<uint16_t>(predicate_mask::Mask4<C>(load(row), vlit));
      const uint32_t m1 =
          static_cast<uint16_t>(predicate_mask::Mask4<C>(load(row + 4), vlit));
      const uint32_t m2 =
          static_cast<uint16_t>(predicate_mask::Mask4<C>(load(row + 8), vlit));
      const uint32_t m3 =
          static_cast<uint16_t>(predicate_mask::Mask4<C>(load(row + 12), vlit));
      return static_cast<uint16_t>(m0 | (m1 << 4) | (m2 << 8) | (m3 << 12));
    };
    const auto make_word = [&](uint32_t row) {
      return static_cast<uint64_t>(mask16(row)) |
             (static_cast<uint64_t>(mask16(row + 16)) << 16) |
             (static_cast<uint64_t>(mask16(row + 32)) << 32) |
             (static_cast<uint64_t>(mask16(row + 48)) << 48);
    };
    uint32_t i = 0;
    size_t out = 0;
    for (; i + 64 <= n; i += 64, ++out) {
      bits[out] = make_word(i);
    }
    for (; i + 16 <= n; i += 16) {
      bits[out] |= static_cast<uint64_t>(mask16(i)) << (i & 63U);
    }
    for (; i + 4 <= n; i += 4) {
      bits[out] |= static_cast<uint64_t>(static_cast<uint16_t>(
                       predicate_mask::Mask4<C>(load(i), vlit)))
                   << (i & 63U);
    }
    for (; i < n; ++i) {
      if (ScanMatch(static_cast<int64_t>(ReadBE64(
                        region + (static_cast<size_t>(i) * sizeof(int64_t)))),
                    op, lit)) {
        bits[out] |= 1ULL << (i & 63U);
      }
    }
  }

  static bool FillPlainPredicateMaskAvx2(const char* region, uint32_t n,
                                         ScanOp op, int64_t lit,
                                         uint64_t* bits) {
    switch (op) {
      case ScanOp::kEq:
        FillPlainMask<Cmp::kEq>(region, n, op, lit, bits);
        return true;
      case ScanOp::kLt:
        FillPlainMask<Cmp::kLt>(region, n, op, lit, bits);
        return true;
      case ScanOp::kLe:
        FillPlainMask<Cmp::kLe>(region, n, op, lit, bits);
        return true;
      case ScanOp::kGt:
        FillPlainMask<Cmp::kGt>(region, n, op, lit, bits);
        return true;
      case ScanOp::kGe:
        FillPlainMask<Cmp::kGe>(region, n, op, lit, bits);
        return true;
    }
    return false;
  }
};
#endif

// AVX2 byte-aligned unpack for FOR-BA (values are `bpv` LE bytes + base).
// Accelerates the dominant scan path; scalar tail handles the remainder.
#ifdef __AVX2__
void UnpackForBaAvx2(const unsigned char* src, uint32_t n, uint32_t bpv,
                     int64_t base, int64_t* out) {
  // Byte-aligned FOR-BA caps bpv at 8 (validator contract); the scalar tail
  // shifts by 8*b, so a bpv past 8 would be a shift-by->=64 UB. Callers'
  // hostile-footer paths bail before reaching here, but clamp locally too.
  if (bpv == 0 || bpv > 8) {
    std::fill(out, out + n, 0);
    return;
  }
  const unsigned char* vp = src + 8;  // skip base (i64 BE)
  const __m256i vbase = _mm256_set1_epi64x(base);
  uint32_t i = 0;
  if (bpv == 1) {
    for (; i + 32 <= n; i += 32) {
      const __m256i b =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(vp + i));
      const __m256i lo16 = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(b));
      const __m256i hi16 = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(b, 1));
      const __m256i l0 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(lo16));
      const __m256i l1 =
          _mm256_cvtepu16_epi32(_mm256_extracti128_si256(lo16, 1));
      const __m256i h0 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(hi16));
      const __m256i h1 =
          _mm256_cvtepu16_epi32(_mm256_extracti128_si256(hi16, 1));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i),
          _mm256_add_epi64(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(l0)),
                           vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 4),
          _mm256_add_epi64(
              _mm256_cvtepu32_epi64(_mm256_extracti128_si256(l0, 1)), vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 8),
          _mm256_add_epi64(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(l1)),
                           vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 12),
          _mm256_add_epi64(
              _mm256_cvtepu32_epi64(_mm256_extracti128_si256(l1, 1)), vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 16),
          _mm256_add_epi64(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(h0)),
                           vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 20),
          _mm256_add_epi64(
              _mm256_cvtepu32_epi64(_mm256_extracti128_si256(h0, 1)), vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 24),
          _mm256_add_epi64(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(h1)),
                           vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 28),
          _mm256_add_epi64(
              _mm256_cvtepu32_epi64(_mm256_extracti128_si256(h1, 1)), vbase));
    }
  } else if (bpv == 2) {
    for (; i + 16 <= n; i += 16) {
      const __m256i b = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(vp + (static_cast<size_t>(i) * 2)));
      const __m256i lo = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(b));
      const __m256i hi = _mm256_cvtepu16_epi32(_mm256_extracti128_si256(b, 1));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i),
          _mm256_add_epi64(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(lo)),
                           vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 4),
          _mm256_add_epi64(
              _mm256_cvtepu32_epi64(_mm256_extracti128_si256(lo, 1)), vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 8),
          _mm256_add_epi64(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(hi)),
                           vbase));
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + i + 12),
          _mm256_add_epi64(
              _mm256_cvtepu32_epi64(_mm256_extracti128_si256(hi, 1)), vbase));
    }
  } else if (bpv == 4) {
    for (; i + 8 <= n; i += 8) {
      const __m256i b = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(vp + (static_cast<size_t>(i) * 4)));
      const __m256i lo = _mm256_add_epi64(
          _mm256_cvtepu32_epi64(_mm256_castsi256_si128(b)), vbase);
      const __m256i hi = _mm256_add_epi64(
          _mm256_cvtepu32_epi64(_mm256_extracti128_si256(b, 1)), vbase);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i), lo);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i + 4), hi);
    }
  } else if (bpv == 8) {
    for (; i + 4 <= n; i += 4) {
      const __m256i b = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(vp + (static_cast<size_t>(i) * 8)));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i),
                          _mm256_add_epi64(b, vbase));
    }
  } else {
    // bpv in {3,5,6,7}: two values fit in a 16-byte window; pshufb spreads
    // the bpv-byte LE fields into two i64 lanes (high bytes zeroed via the
    // 0x80 control). Every window stays inside the value array -- the tail
    // below owns the last values whose window would cross the region end.
    alignas(16) std::array<unsigned char, 16> ctlb{};
    std::memset(ctlb.data(), 0x80, sizeof(ctlb));
    for (uint32_t k = 0; k < bpv; ++k) {
      ctlb[k] = static_cast<unsigned char>(k);
      ctlb[8 + k] = static_cast<unsigned char>(bpv + k);
    }
    const __m128i ctl =
        _mm_load_si128(reinterpret_cast<const __m128i*>(ctlb.data()));
    const __m128i vbase2 = _mm_set1_epi64x(base);
    const uint64_t nbytes = static_cast<uint64_t>(n) * bpv;
    for (; i + 2 <= n && ((static_cast<uint64_t>(i) + 2) * bpv) + 16 <= nbytes;
         i += 4) {
      const __m128i a =
          _mm_shuffle_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(
                               vp + (static_cast<size_t>(i) * bpv))),
                           ctl);
      const __m128i b =
          _mm_shuffle_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(
                               vp + (static_cast<size_t>(i + 2) * bpv))),
                           ctl);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i),
                          _mm256_add_epi64(_mm256_set_m128i(b, a), vbase));
    }
    for (; i + 2 <= n && (static_cast<uint64_t>(i) * bpv) + 16 <= nbytes;
         i += 2) {
      const __m128i a =
          _mm_shuffle_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(
                               vp + (static_cast<size_t>(i) * bpv))),
                           ctl);
      _mm_storeu_si128(reinterpret_cast<__m128i*>(out + i),
                       _mm_add_epi64(a, vbase2));
    }
  }
  // Masked u64 loads while the 8-byte window stays inside the value array;
  // byte assembly for the last <ceil(8/bpv) values.
  const uint64_t m = (bpv >= 8) ? ~0ULL : ((1ULL << (8U * bpv)) - 1);
  for (; i < n && static_cast<uint64_t>(n - i) * bpv >= 8; ++i) {
    uint64_t raw{};
    std::memcpy(&raw, vp + (static_cast<size_t>(i) * bpv), 8);
    out[i] = static_cast<int64_t>(static_cast<uint64_t>(base) + (raw & m));
  }
  for (; i < n; ++i) {
    uint64_t off = 0;
    for (uint32_t b = 0; b < bpv; ++b) {
      off |= static_cast<uint64_t>(vp[(i * bpv) + b]) << (8 * b);
    }
    out[i] = static_cast<int64_t>(static_cast<uint64_t>(base) + off);
  }
}
#endif

// Build the match mask (all-ones where decoded value satisfies op against lit).
#ifdef __AVX2__
template <ScanOp Op>
inline __m256i ScanMatchMaskFixed(__m256i v, __m256i vlit) {
  if constexpr (Op == ScanOp::kEq) {
    return _mm256_cmpeq_epi64(v, vlit);
  } else if constexpr (Op == ScanOp::kLe) {
    return _mm256_xor_si256(_mm256_cmpgt_epi64(v, vlit),
                            _mm256_set1_epi64x(-1));
  } else if constexpr (Op == ScanOp::kLt) {
    return _mm256_cmpgt_epi64(vlit, v);
  } else if constexpr (Op == ScanOp::kGe) {
    return _mm256_xor_si256(_mm256_cmpgt_epi64(vlit, v),
                            _mm256_set1_epi64x(-1));
  } else {
    return _mm256_cmpgt_epi64(v, vlit);
  }
}

inline __m256i ScanMatchMask(__m256i v, ScanOp op, __m256i vlit) {
  switch (op) {
    case ScanOp::kEq:
      return ScanMatchMaskFixed<ScanOp::kEq>(v, vlit);
    case ScanOp::kLe:
      return ScanMatchMaskFixed<ScanOp::kLe>(v, vlit);
    case ScanOp::kLt:
      return ScanMatchMaskFixed<ScanOp::kLt>(v, vlit);
    case ScanOp::kGe:
      return ScanMatchMaskFixed<ScanOp::kGe>(v, vlit);
    case ScanOp::kGt:
      return ScanMatchMaskFixed<ScanOp::kGt>(v, vlit);
  }
  return _mm256_setzero_si256();
}

bool PlainSum128Avx2(const char* region, uint32_t n, __int128* sum_out) {
  const __m256i bswap =
      _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7,
                       6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
  const __m256i low_mask = _mm256_set1_epi64x(0xFFFFFFFF);
  const __m256i sign_mask = _mm256_set1_epi64x(INT64_C(0x80000000));
  const auto sign_extend_high = [sign_mask](__m256i value) {
    const __m256i sign = _mm256_and_si256(value, sign_mask);
    return _mm256_sub_epi64(value, _mm256_slli_epi64(sign, 1));
  };
  __m256i low0 = _mm256_setzero_si256();
  __m256i low1 = _mm256_setzero_si256();
  __m256i low2 = _mm256_setzero_si256();
  __m256i low3 = _mm256_setzero_si256();
  __m256i high0 = _mm256_setzero_si256();
  __m256i high1 = _mm256_setzero_si256();
  __m256i high2 = _mm256_setzero_si256();
  __m256i high3 = _mm256_setzero_si256();
  const auto* bytes = reinterpret_cast<const unsigned char*>(region);
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const __m256i v0 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes + (i * 8))),
        bswap);
    const __m256i v1 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(bytes + (i * 8) + 32)),
        bswap);
    const __m256i v2 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(bytes + (i * 8) + 64)),
        bswap);
    const __m256i v3 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(bytes + (i * 8) + 96)),
        bswap);
    const __m256i l0 = _mm256_and_si256(v0, low_mask);
    const __m256i l1 = _mm256_and_si256(v1, low_mask);
    const __m256i l2 = _mm256_and_si256(v2, low_mask);
    const __m256i l3 = _mm256_and_si256(v3, low_mask);
    const __m256i h0 = sign_extend_high(_mm256_srli_epi64(v0, 32));
    const __m256i h1 = sign_extend_high(_mm256_srli_epi64(v1, 32));
    const __m256i h2 = sign_extend_high(_mm256_srli_epi64(v2, 32));
    const __m256i h3 = sign_extend_high(_mm256_srli_epi64(v3, 32));
    low0 = _mm256_add_epi64(low0, l0);
    low1 = _mm256_add_epi64(low1, l1);
    low2 = _mm256_add_epi64(low2, l2);
    low3 = _mm256_add_epi64(low3, l3);
    high0 = _mm256_add_epi64(high0, h0);
    high1 = _mm256_add_epi64(high1, h1);
    high2 = _mm256_add_epi64(high2, h2);
    high3 = _mm256_add_epi64(high3, h3);
  }
  for (; i + 4 <= n; i += 4) {
    const __m256i v = _mm256_shuffle_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes + (i * 8))),
        bswap);
    const __m256i l = _mm256_and_si256(v, low_mask);
    const __m256i h = sign_extend_high(_mm256_srli_epi64(v, 32));
    low0 = _mm256_add_epi64(low0, l);
    high0 = _mm256_add_epi64(high0, h);
  }
  alignas(32) std::array<uint64_t, 16> low_lanes{};
  alignas(32) std::array<uint64_t, 16> high_lanes{};
  _mm256_store_si256(reinterpret_cast<__m256i*>(low_lanes.data()), low0);
  _mm256_store_si256(reinterpret_cast<__m256i*>(low_lanes.data() + 4), low1);
  _mm256_store_si256(reinterpret_cast<__m256i*>(low_lanes.data() + 8), low2);
  _mm256_store_si256(reinterpret_cast<__m256i*>(low_lanes.data() + 12), low3);
  _mm256_store_si256(reinterpret_cast<__m256i*>(high_lanes.data()), high0);
  _mm256_store_si256(reinterpret_cast<__m256i*>(high_lanes.data() + 4), high1);
  _mm256_store_si256(reinterpret_cast<__m256i*>(high_lanes.data() + 8), high2);
  _mm256_store_si256(reinterpret_cast<__m256i*>(high_lanes.data() + 12), high3);
  uint64_t low = 0;
  int64_t high = 0;
  for (size_t lane = 0; lane < 16; ++lane) {
    low += low_lanes[lane];
    high += static_cast<int64_t>(high_lanes[lane]);
  }
  for (; i < n; ++i) {
    const uint64_t value = ReadBE64(region + (i * 8));
    const auto high_bits = static_cast<uint32_t>(value >> 32);
    low += value & 0xFFFFFFFFULL;
    high += static_cast<int64_t>(high_bits) -
            ((high_bits & UINT32_C(0x80000000)) != 0 ? INT64_C(1) << 32 : 0);
  }
  *sum_out = (static_cast<__int128>(high) * (INT64_C(1) << 32)) +
             static_cast<__int128>(low);
  return true;
}

template <ScanOp Op>
bool ScanPlainCountSumAvx2Impl(const char* region, uint32_t n, ScanOp op,
                               int64_t lit, uint64_t* count_out,
                               uint64_t* sum_out) {
  const __m256i bswap =
      _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7,
                       6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
  const __m256i vlit = _mm256_set1_epi64x(lit);
  const __m256i zero = _mm256_setzero_si256();
  __m256i sum0 = zero;
  __m256i sum1 = zero;
  __m256i sum2 = zero;
  __m256i sum3 = zero;
  uint64_t count0 = 0;
  uint64_t count1 = 0;
  uint64_t count2 = 0;
  uint64_t count3 = 0;
  const auto* bytes = reinterpret_cast<const unsigned char*>(region);
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const __m256i v0 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes + (i * 8))),
        bswap);
    const __m256i v1 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(bytes + (i * 8) + 32)),
        bswap);
    const __m256i v2 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(bytes + (i * 8) + 64)),
        bswap);
    const __m256i v3 = _mm256_shuffle_epi8(
        _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(bytes + (i * 8) + 96)),
        bswap);
    const __m256i m0 = ScanMatchMaskFixed<Op>(v0, vlit);
    const __m256i m1 = ScanMatchMaskFixed<Op>(v1, vlit);
    const __m256i m2 = ScanMatchMaskFixed<Op>(v2, vlit);
    const __m256i m3 = ScanMatchMaskFixed<Op>(v3, vlit);
    count0 += static_cast<uint64_t>(__builtin_popcount(
        static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(m0)))));
    count1 += static_cast<uint64_t>(__builtin_popcount(
        static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(m1)))));
    count2 += static_cast<uint64_t>(__builtin_popcount(
        static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(m2)))));
    count3 += static_cast<uint64_t>(__builtin_popcount(
        static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(m3)))));
    sum0 = _mm256_add_epi64(sum0, _mm256_and_si256(v0, m0));
    sum1 = _mm256_add_epi64(sum1, _mm256_and_si256(v1, m1));
    sum2 = _mm256_add_epi64(sum2, _mm256_and_si256(v2, m2));
    sum3 = _mm256_add_epi64(sum3, _mm256_and_si256(v3, m3));
  }
  for (; i + 4 <= n; i += 4) {
    const __m256i v = _mm256_shuffle_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes + (i * 8))),
        bswap);
    const __m256i m = ScanMatchMaskFixed<Op>(v, vlit);
    count0 += static_cast<uint64_t>(__builtin_popcount(
        static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(m)))));
    sum0 = _mm256_add_epi64(sum0, _mm256_and_si256(v, m));
  }
  alignas(32) std::array<uint64_t, 16> sum_lanes{};
  _mm256_store_si256(reinterpret_cast<__m256i*>(sum_lanes.data()), sum0);
  _mm256_store_si256(reinterpret_cast<__m256i*>(sum_lanes.data() + 4), sum1);
  _mm256_store_si256(reinterpret_cast<__m256i*>(sum_lanes.data() + 8), sum2);
  _mm256_store_si256(reinterpret_cast<__m256i*>(sum_lanes.data() + 12), sum3);
  uint64_t sum = 0;
  for (const uint64_t lane : sum_lanes) {
    sum += lane;
  }
  uint64_t count = count0 + count1 + count2 + count3;
  for (; i < n; ++i) {
    const uint64_t value = ReadBE64(region + (i * 8));
    if (ScanMatch(static_cast<int64_t>(value), op, lit)) {
      ++count;
      sum += value;
    }
  }
  *count_out = count;
  *sum_out = sum;
  return true;
}

bool ScanPlainCountSumAvx2(const char* region, uint32_t n, ScanOp op,
                           int64_t lit, uint64_t* count_out,
                           uint64_t* sum_out) {
  switch (op) {
    case ScanOp::kEq:
      return ScanPlainCountSumAvx2Impl<ScanOp::kEq>(region, n, op, lit,
                                                    count_out, sum_out);
    case ScanOp::kLe:
      return ScanPlainCountSumAvx2Impl<ScanOp::kLe>(region, n, op, lit,
                                                    count_out, sum_out);
    case ScanOp::kLt:
      return ScanPlainCountSumAvx2Impl<ScanOp::kLt>(region, n, op, lit,
                                                    count_out, sum_out);
    case ScanOp::kGe:
      return ScanPlainCountSumAvx2Impl<ScanOp::kGe>(region, n, op, lit,
                                                    count_out, sum_out);
    case ScanOp::kGt:
      return ScanPlainCountSumAvx2Impl<ScanOp::kGt>(region, n, op, lit,
                                                    count_out, sum_out);
  }
  return false;
}
#endif

inline bool DecodeXorForByteAligned(const char* packed, uint32_t n,
                                    uint32_t width, const char* end,
                                    int64_t* out) {
  if (n <= 1 || width == 0 || width > 64 || (width & 7U) != 0 ||
      packed == nullptr || end == nullptr || end < packed) {
    return false;
  }
  const size_t bytes = width / 8U;
  const auto available = static_cast<size_t>(end - packed);
  const size_t needed = static_cast<size_t>(n - 1) * bytes;
  if (needed > available) {
    return false;
  }
#ifdef __AVX2__
  if (width == 56) {
    const __m128i gather =
        _mm_setr_epi8(6, 5, 4, 3, 2, 1, 0, -1, 13, 12, 11, 10, 9, 8, 7, -1);
    const __m256i upper_mask = _mm256_set_epi64x(-1, -1, 0, 0);
    auto x = static_cast<uint64_t>(out[0]);
    size_t i = 0;
    for (; i + 4 <= n - 1 && (i * 7) + 30 <= available; i += 4) {
      const size_t offset = i * 7;
      const __m128i first = _mm_shuffle_epi8(
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(packed + offset)),
          gather);
      const __m128i second = _mm_shuffle_epi8(
          _mm_loadu_si128(
              reinterpret_cast<const __m128i*>(packed + offset + 14)),
          gather);
      __m256i values = _mm256_set_m128i(second, first);
      values = _mm256_xor_si256(values, _mm256_slli_si256(values, 8));
      const __m128i low = _mm256_castsi256_si128(values);
      const auto carry = static_cast<uint64_t>(_mm_extract_epi64(low, 1));
      values = _mm256_xor_si256(
          values,
          _mm256_and_si256(_mm256_set1_epi64x(static_cast<long long>(carry)),
                           upper_mask));
      values = _mm256_xor_si256(values,
                                _mm256_set1_epi64x(static_cast<long long>(x)));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i + 1), values);
      x = static_cast<uint64_t>(_mm256_extract_epi64(values, 3));
    }
    for (; i < n - 1; ++i) {
      uint64_t value = 0;
      for (size_t b = 0; b < 7; ++b) {
        value = (value << 8) | static_cast<unsigned char>(packed[(i * 7) + b]);
      }
      x ^= value;
      out[i + 1] = static_cast<int64_t>(x);
    }
    return true;
  }
#endif
  const int shift = 64 - static_cast<int>(width);
  auto x = static_cast<uint64_t>(out[0]);
  size_t i = 0;
  for (; i < n - 1; ++i) {
    const size_t offset = i * bytes;
    if (offset + sizeof(uint64_t) > available) {
      break;
    }
    uint64_t word{};
    std::memcpy(&word, packed + offset, sizeof(word));
    x ^= be64toh(word) >> shift;
    out[i + 1] = static_cast<int64_t>(x);
  }
  for (; i < n - 1; ++i) {
    const size_t offset = i * bytes;
    uint64_t word = 0;
    for (size_t b = 0; b < bytes; ++b) {
      word = (word << 8) | static_cast<unsigned char>(packed[offset + b]);
    }
    x ^= word;
    out[i + 1] = static_cast<int64_t>(x);
  }
  return true;
}

#if defined(__AVX2__) && (defined(__x86_64__) || defined(__i386__))
__attribute__((target("avx512f,avx512bw"))) void DecodePlainAvx512(
    const char* region, uint32_t n, int64_t* out) {
  alignas(64) static constexpr std::array<uint8_t, 64> kBswap = {
      7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
      7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
      7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
      7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8};
  const __m512i bswap = _mm512_load_si512(kBswap.data());
  uint32_t i = 0;
  for (; i + 16 <= n; i += 16) {
    __m512i v0 = _mm512_loadu_si512(region + (static_cast<size_t>(i) * 8));
    __m512i v1 = _mm512_loadu_si512(region + (static_cast<size_t>(i + 8) * 8));
    v0 = _mm512_shuffle_epi8(v0, bswap);
    v1 = _mm512_shuffle_epi8(v1, bswap);
    _mm512_storeu_si512(out + i, v0);
    _mm512_storeu_si512(out + i + 8, v1);
  }
  for (; i + 8 <= n; i += 8) {
    const __m512i v = _mm512_shuffle_epi8(
        _mm512_loadu_si512(region + (static_cast<size_t>(i) * 8)), bswap);
    _mm512_storeu_si512(out + i, v);
  }
  for (; i < n; ++i) {
    out[i] =
        static_cast<int64_t>(ReadBE64(region + (static_cast<size_t>(i) * 8)));
  }
}

bool DecodePlainAvx512Available() {
  return __builtin_cpu_supports("avx512f") &&
         __builtin_cpu_supports("avx512bw");
}
#endif

}  // namespace

bool PaxChunk::IsCellNull(uint32_t g, slot_t c, uint32_t local) const {
  return CellIsNull(footer_, g, c, local);
}

bool PaxChunk::CellHasNull(uint32_t g, slot_t c) const {
  return RgColHasNull(footer_, g, c);
}

const uint8_t* PaxChunk::CellNullBits(uint32_t g, slot_t c) const {
  return RgColNullBits(footer_, g, c);
}

double PaxChunk::SchemeDecodeCost(Scheme s) {
  return SchemeDecodeCostInternal(s);
}

void PaxChunk::DecodeInt64ColumnRg(slot_t col, uint32_t g, int64_t* out) const {
  // Public entry with a hand-buildable footer: validate indices before any
  // pointer arithmetic (previously an unchecked g/col indexed five parallel
  // arrays and dereferenced an arbitrary data_ offset).
  if (col >= footer_.column_count || g >= footer_.row_group_count ||
      static_cast<size_t>(g) >= footer_.row_counts.size()) {
    throw std::out_of_range("DecodeInt64ColumnRg: group/column out of range");
  }
  const uint32_t off_idx = (g * footer_.column_count) + col;
  if (static_cast<size_t>(off_idx) >= footer_.column_offsets.size() ||
      static_cast<size_t>(off_idx) >= footer_.encodings.size() ||
      static_cast<size_t>(off_idx) >= footer_.params.size()) {
    throw std::out_of_range("DecodeInt64ColumnRg: footer arrays misaligned");
  }
  const uint16_t rc = footer_.row_counts[g];
  if (rc == 0) {
    return;  // every scheme below writes out[0] unconditionally
  }
  if (decoded_int_cache_ready_ &&
      static_cast<size_t>(off_idx) + 1 < decoded_int_offsets_.size() &&
      decoded_int_offsets_[off_idx + 1] > decoded_int_offsets_[off_idx]) {
    const size_t begin = decoded_int_offsets_[off_idx];
    if (begin + rc <= decoded_int_cache_.size()) {
      std::memcpy(out, decoded_int_cache_.data() + begin,
                  static_cast<size_t>(rc) * sizeof(int64_t));
      return;
    }
  }
  // Offset-VALUE validation: the array checks above only prove the entry
  // exists; a hostile value (e.g. 0xFFFFFFFF) must not form the pointer.
  const char* region = RegionStartAt(footer_, g, col, data_, 0);
  if (region == nullptr) {
    throw std::out_of_range("DecodeInt64ColumnRg: hostile column offset");
  }
  const auto s = static_cast<Scheme>(footer_.encodings[off_idx]);
  const uint32_t prm = footer_.params[off_idx];
  if (s == Scheme::kRleDelta) {
    const char* rle_delta_end = RegionEndAt(footer_, g, col, data_);
    if (!DecodeRleDeltaAll(region, rc, prm, rle_delta_end, out)) {
      std::fill(out, out + rc, 0);
    }
    return;
  }
  if (s == Scheme::kRle8) {
    const char* rle8_end = RegionEndAt(footer_, g, col, data_);
    if (!DecodeRle8All(region, rc, prm, rle8_end, out)) {
      std::fill(out, out + rc, 0);
    }
    return;
  }
#ifdef __AVX2__
  // The SIMD fast paths read the header-derived extent in one shot; a
  // hand-built footer must not turn that into an OOB sweep. Take the fast
  // path only when the extent provably fits inside the region; otherwise
  // fall through to the generic loop below.
  const char* simd_end = RegionEndAt(footer_, g, col, data_);
  if (s == Scheme::kForBp && prm >= 1 && prm <= 8 && simd_end != nullptr &&
      region + 8 + (static_cast<size_t>(rc) * prm) <= simd_end) {
    UnpackForBaAvx2(reinterpret_cast<const unsigned char*>(region), rc, prm,
                    static_cast<int64_t>(ReadBE64(region)), out);
    return;
  }
  if (s == Scheme::kPlain && simd_end != nullptr &&
      region + (static_cast<size_t>(rc) * 8) <= simd_end) {
#if defined(__x86_64__) || defined(__i386__)
    if (DecodePlainAvx512Available()) {
      DecodePlainAvx512(region, rc, out);
      return;
    }
#endif
    // SIMD byteswap 8 x u64 at a time (32-byte YMM x 2, pshufb per-128-lane
    // reversing the two 8-byte chunks inside each lane).
    const __m256i bswap =
        _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
                         7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    const auto* p = reinterpret_cast<const uint8_t*>(region);
    size_t i = 0;
    for (; i + 8 <= rc; i += 8) {
      const __m256i v0 =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + (i * 8)));
      const __m256i v1 = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(p + (i * 8) + 32));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i),
                          _mm256_shuffle_epi8(v0, bswap));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i + 4),
                          _mm256_shuffle_epi8(v1, bswap));
    }
    for (; i < rc; ++i) {
      out[i] = static_cast<int64_t>(
          ReadBE64(reinterpret_cast<const char*>(p + (i * 8))));
    }
    return;
  }
#endif
  if (s == Scheme::kRle && prm <= rc) {
    const char* rle_end = RegionEndAt(footer_, g, col, data_);
    if (rle_end != nullptr && rle_end >= region) {
      const auto available = static_cast<size_t>(rle_end - region);
      if (available >= 4 && prm <= (available - 4) / 12 &&
          DecodeInnerI64All(region, static_cast<uint8_t>(s), prm, rc, rle_end,
                            out)) {
        return;
      }
    }
  }
  if (s == Scheme::kXorFor) {
    // Single-pass O(n) decode. XOR deltas are independent and packed MSB-first
    // at fixed width `prm`; extract them from a 64-bit read window instead of
    // the per-bit UnpackBitAt loop (the previous O(n^2) hot path).
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    if (reg_end == nullptr || region + 8 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    // The validator pins XOR-FOR params to [1,64]; a hand-built footer can
    // carry any u32. prm >= 2^31 casts to a NEGATIVE width in br.get() below
    // (negative-count shifts / int overflow: UB), and prm in (64, 2^31)
    // silently decodes every delta as 0. Clamp like the sibling schemes.
    if (prm > 64) {
      std::fill(out, out + rc, 0);
      return;
    }
    uint64_t x = ReadBE64(region);
    out[0] = static_cast<int64_t>(x);
    const char* packed = region + 8;
    const size_t bit_len = (rc > 1) ? static_cast<size_t>(rc - 1) * prm : 0;
    // Clamp the header-derived window by the region: a hostile prm must not
    // walk the reader past the buffer (the loop then yields 0-bit reads).
    const char* end = packed + ((bit_len + 7) / 8);
    end = std::min(end, reg_end);
    if (DecodeXorForByteAligned(packed, rc, prm, end, out)) {
      return;
    }
    WordBitReader br(packed, end);
    for (uint32_t t = 1; t < rc; ++t) {
      x ^= br.get(static_cast<int>(prm));
      out[t] = static_cast<int64_t>(x);
    }
    return;
  }
  if (s == Scheme::kGorilla) {
    // Single-pass O(n) decode of the XOR stream. The control bit + lz(5) +
    // tz(6)
    // + value(mw) fields are read from a 64-bit window in one shot, replacing
    // the byte-by-byte BitReader::get1 loop.
    const char* gor_end = RegionEndAt(footer_, g, col, data_);
    if (gor_end == nullptr || region + 8 > gor_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    uint64_t prev = ReadBE64(region);
    out[0] = static_cast<int64_t>(prev);
    const char* packed = region + 8;
    // Upper bound on the stream length; decode always stops before this.
    const size_t bit_len =
        (rc > 1) ? static_cast<size_t>(rc - 1) * (1 + 5 + 6 + 64) : 0;
    const char* end = packed + ((bit_len + 7) / 8);
    // Region clamp: see kXorFor above.
    end = std::min(end, gor_end);
    WordBitReader br(packed, end);
    for (uint32_t i = 1; i < rc; ++i) {
      if (br.get1() != 0) {
        const int lz = static_cast<int>(br.get(5));
        const int tz = static_cast<int>(br.get(6));
        // Valid Gorilla guarantees lz + tz <= 63 (mw >= 1). A corrupt
        // stream can yield mw < 0, which makes WordBitReader::get do
        // `1ULL << w` / `>> (64 - w)` with a negative count (UB). Clamp;
        // mw == 64 also shifts by tz == 0 (lz+tz == 0), so the plain shift
        // is well-defined in every reachable case.
        int mw = 64 - lz - tz;
        mw = std::max(mw, 0);
        mw = std::min(mw, 64);
        const uint64_t m = br.get(mw);
        prev ^= (m << tz);
      }
      out[i] = static_cast<int64_t>(prev);
    }
    return;
  }
  if (s == Scheme::kAlp) {
    // [exp u8][backend u8][nexc u16][exc 10B x nexc][inner region]. Mantissa
    // values decode through the backend's bulk path, then exception
    // positions patch their rows with the stored full values.
    const char* alp_end = RegionEndAt(footer_, g, col, data_);
    if (alp_end == nullptr || region + 4 > alp_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const int exp = static_cast<uint8_t>(region[0]);
    const auto backend = static_cast<Scheme>(static_cast<uint8_t>(region[1]));
    const uint32_t nexc = ReadBE16(region + 2);
    if (exp > 18 || nexc > rc) {
      std::fill(out, out + rc, 0);
      return;
    }
    const char* exc = region + 4;
    if (exc + (static_cast<size_t>(nexc) * 10) > alp_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const char* inner = exc + (static_cast<size_t>(nexc) * 10);
    if (backend == Scheme::kBitWeave) {
      if (inner + 9 > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      const int w = static_cast<int>(static_cast<uint8_t>(inner[8]));
      const size_t pack_bytes =
          (static_cast<size_t>(rc) * static_cast<size_t>(w) + 7) / 8;
      if (w <= 0 || w > 64 || inner + 9 + pack_bytes > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      const auto base_v = static_cast<int64_t>(ReadBE64(inner));
      for (uint32_t i = 0; i < rc; ++i) {
        const uint64_t moff = UnpackBitAt(
            inner + 9, static_cast<uint64_t>(i) * static_cast<uint64_t>(w), w);
        const auto m =
            static_cast<int64_t>(static_cast<uint64_t>(base_v) + moff);
        const double back = static_cast<double>(m) / AlpFactorOf(exp);
        uint64_t b{};
        std::memcpy(&b, &back, sizeof(b));
        out[i] = static_cast<int64_t>(b);
      }
    } else if (backend == Scheme::kConst) {
      if (inner + 8 > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      const auto m = static_cast<int64_t>(ReadBE64(inner));
      const double back = static_cast<double>(m) / AlpFactorOf(exp);
      uint64_t b{};
      std::memcpy(&b, &back, sizeof(b));
      const auto v = static_cast<int64_t>(b);
      for (uint32_t i = 0; i < rc; ++i) {
        out[i] = v;
      }
    } else if (backend == Scheme::kRle) {
      if (inner + 4 > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      const uint32_t runs = ReadBE32(inner);
      if (runs == 0 || runs > rc ||
          inner + 4 + (static_cast<size_t>(runs) * 12) > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      // Run expansion in region order (mirrors the kRle bulk semantics);
      // rows past the covered extent decode as 0.
      uint64_t acc = 0;
      const char* q = inner + 4;
      for (uint32_t r2 = 0; r2 < runs && acc < rc; ++r2) {
        const auto rv = static_cast<int64_t>(ReadBE64(q));
        const uint64_t len = ReadBE32(q + 8);
        const uint64_t next = acc + len;
        const uint64_t run_end = (next > rc) ? rc : next;
        const double back = static_cast<double>(rv) / AlpFactorOf(exp);
        uint64_t b{};
        std::memcpy(&b, &back, sizeof(b));
        const auto v = static_cast<int64_t>(b);
        for (uint64_t i = acc; i < run_end; ++i) {
          out[static_cast<size_t>(i)] = v;
        }
        acc = next;
        q += 12;
      }
      for (uint64_t i = acc; i < rc; ++i) {
        out[static_cast<size_t>(i)] = 0;
      }
    } else if (backend == Scheme::kDict) {
      if (inner + 2 > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      const uint16_t ncodes = ReadBE16(inner);
      const int cw = BitsNeeded(ncodes > 0 ? ncodes - 1 : 0);
      const size_t dict_base =
          2 + ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
      if (ncodes == 0 ||
          inner + dict_base + (static_cast<size_t>(ncodes) * 8) > alp_end) {
        std::fill(out, out + rc, 0);
        return;
      }
      const char* dict = inner + dict_base;
      for (uint32_t i = 0; i < rc; ++i) {
        const uint64_t code = UnpackBitAt(
            inner + 2, static_cast<uint64_t>(i) * static_cast<uint64_t>(cw),
            cw);
        // Codes past the dictionary decode as 0 (mirrors kDict).
        const int64_t m = (code < ncodes)
                              ? static_cast<int64_t>(ReadBE64(
                                    dict + (static_cast<size_t>(code) * 8)))
                              : 0;
        const double back = static_cast<double>(m) / AlpFactorOf(exp);
        uint64_t b{};
        std::memcpy(&b, &back, sizeof(b));
        out[i] = static_cast<int64_t>(b);
      }
    } else {
      std::fill(out, out + rc, 0);
      return;
    }
    for (uint32_t k = 0; k < nexc; ++k) {
      const uint32_t pos = ReadBE16(exc + (static_cast<size_t>(k) * 10));
      if (pos < rc) {
        out[pos] = static_cast<int64_t>(
            ReadBE64(exc + (static_cast<size_t>(k) * 10) + 2));
      }
    }
    return;
  }
  if (s == Scheme::kDeltaFp) {
    // Ascending deltas with a single starting value and checkpoints. The
    // per-element Int64RegionAt is O(idx) (cumulative), so the generic
    // full-scan path is O(n^2). Fold the deltas in a single pass from a
    // 64-bit word window.
    const uint32_t C = kDeltaCheckpointEvery;
    const uint32_t cks = (rc + C - 1) / C;
    const char* dfp_end = RegionEndAt(footer_, g, col, data_);
    // Header + checkpoint table before any read (mirrors Int64RegionAt).
    if (dfp_end == nullptr || region + 8 > dfp_end ||
        region + 8 + (static_cast<size_t>(cks) * 8) > dfp_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    // Same hostile-param clamp as kXorFor above: the raw footer u32 must not
    // reach br.get() as a negative bit width.
    if (prm > 64) {
      std::fill(out, out + rc, 0);
      return;
    }
    uint64_t v = ReadBE64(region);
    out[0] = static_cast<int64_t>(v);
    if (rc > 1) {
      // Layout: [first u64][checkpoints cks*u64][packed deltas (rc-1)*w bits].
      // The decoder derives cks from n (=rc) and kDeltaCheckpointEvery.
      const char* packed = region + 8 + (static_cast<size_t>(cks) * 8);
      const size_t bit_len = static_cast<size_t>(rc - 1) * prm;
      const char* end = packed + ((bit_len + 7) / 8);
      // Region clamp: see kXorFor above.
      end = std::min(end, dfp_end);
      WordBitReader br(packed, end);
      for (uint32_t i = 1; i < rc; ++i) {
        v += br.get(static_cast<int>(prm));  // wrapping: deltas may overflow
        out[i] = static_cast<int64_t>(v);
      }
    }
    return;
  }
  if (s == Scheme::kDict || s == Scheme::kOpDict) {
    // Bit-packed dictionary codes (width cw, MSB-first). Unpack them from a
    // 64-bit window instead of UnpackBitAt per element. kOpDict shares the
    // byte layout (sorted dict, rank codes) and therefore the bulk path.
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    if (reg_end == nullptr || region + 2 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const uint16_t ncodes = ReadBE16(region);
    // ncodes == 0 made `ncodes - 1` wrap to 0xFFFFFFFFFFFF: cw became 64
    // and every code read garbage. Guard like Int64RegionAt.
    if (ncodes == 0) {
      std::fill(out, out + rc, 0);
      return;
    }
    const int cw = BitsNeeded(ncodes - 1);
    const char* packed = region + 2;
    const size_t dict_base =
        2 + ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
    const char* dict = region + dict_base;
    // The validator floors the region at 4B/entry but int64 dicts need
    // 8B/entry: a region built at the floor would read code*8 bytes past
    // the region. Zero-fill instead (matches the clamped-code decode).
    if (dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    WordBitReader br(packed, dict);
    for (uint32_t i = 0; i < rc; ++i) {
      const uint64_t code = br.get(cw);
      // Clamp corrupt high codes (see Int64RegionAt kDict note).
      out[i] =
          (code >= ncodes)
              ? 0
              : static_cast<int64_t>(ReadBE64(dict + (code * sizeof(int64_t))));
    }
    return;
  }
  if (s == Scheme::kBitWeave) {
    // Byte-aligned base + w-bit bit-packed offsets (MSB-first).
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    if (reg_end == nullptr || region + 9 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const auto base_v = static_cast<int64_t>(ReadBE64(region));
    const int w = static_cast<int>(static_cast<uint8_t>(region[8]));
    // A hostile width byte > 64 made WordBitReader::get read/wrap UB; the
    // footer gate caps prm at 64 for validated chunks — clamp the header.
    if (w > 64) {
      std::fill(out, out + rc, base_v);
      return;
    }
    const char* packed = region + 9;
    const size_t bit_len = static_cast<size_t>(rc) * static_cast<size_t>(w);
    const char* end = packed + ((bit_len + 7) / 8);
    // Clamp the header-derived extent by the region (hostile header).
    end = std::min(end, reg_end);
    WordBitReader br(packed, end);
    for (uint32_t i = 0; i < rc; ++i) {
      out[i] = static_cast<int64_t>(static_cast<uint64_t>(base_v) + br.get(w));
    }
    return;
  }
  if (s == Scheme::kEliasFano) {
    // Low bits are packed `l`-wide (MSB-first); high bits via SelectBit.
    const char* ef_end = RegionEndAt(footer_, g, col, data_);
    if (ef_end == nullptr || region + 17 > ef_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const auto base_v = static_cast<int64_t>(ReadBE64(region));
    const uint64_t maxd = ReadBE64(region + 8);
    const int l = static_cast<int>(static_cast<uint8_t>(region[16]));
    const uint64_t high_max = (l >= 64) ? 0 : (maxd >> l);
    // hlen must not wrap (mirrors Int64RegionAt): a hostile maxd near 2^64
    // wrapped size_t addition and walked the bitmap reads wild. Reserve 7
    // bits of headroom like the scalar arm: the boundary high_max ==
    // UINT64_MAX - rc makes hlen == UINT64_MAX, whose (hlen + 7) / 8 wraps
    // to 0 so lowbits aliases hbmp and SelectBit marches off the buffer.
    if (high_max > UINT64_MAX - static_cast<uint64_t>(rc) - 7) {
      std::fill(out, out + rc, base_v);
      return;
    }
    const size_t hlen = static_cast<size_t>(rc) + static_cast<size_t>(high_max);
    const char* hbmp = region + 17;
    const char* lowbits = hbmp + ((hlen + 7) / 8);
    const size_t bit_len = static_cast<size_t>(rc) * static_cast<size_t>(l);
    const char* end = lowbits + ((bit_len + 7) / 8);
    // Region clamp: a hostile header must not walk SelectBit / the low-bit
    // reader past the buffer.
    if (end > ef_end) {
      std::fill(out, out + rc, base_v);
      return;
    }
    WordBitReader br(lowbits, end);
    // Sequential-select cursor: rows visit i in ascending order and
    // SelectBit is monotone in idx, so carrying (word, seen) forward is
    // bit-identical to calling SelectBit(hbmp, hlen, i) per row while
    // turning the per-row word-0 rescan into an amortized O(1) advance.
    uint64_t sw = 0, sseen = 0, sword = 0, tp = 0;
    bool sloaded = false, in_tail = false, exhausted = false;
    auto ef_select = [&](uint64_t i) -> uint64_t {
      if (exhausted) {
        return hlen;
      }
      for (; !in_tail && (sw + 1) * 64 <= hlen; ++sw) {
        if (!sloaded) {
          sword = ReadBE64(hbmp + (sw * 8));
          sloaded = true;
        }
        const auto cnt = static_cast<uint64_t>(__builtin_popcountll(sword));
        if (cnt != 0 && sseen + cnt > i) {
          return (sw * 64) + SelectFromWord(sword, i - sseen);
        }
        sseen += cnt;
        sloaded = false;
      }
      if (!in_tail) {
        in_tail = true;
        tp = sw * 64;
      }
      for (; tp < hlen; ++tp) {
        const uint64_t bit =
            (static_cast<uint8_t>(hbmp[tp >> 3]) >> (7 - (tp & 7))) & 1;
        if (bit != 0) {
          if (sseen == i) {
            return tp;
          }
          ++sseen;
        }
      }
      exhausted = true;
      return hlen;
    };
    for (uint32_t i = 0; i < rc; ++i) {
      const uint64_t low = br.get(l);
      const uint64_t pos = ef_select(static_cast<uint64_t>(i));
      const uint64_t h = pos - i;
      const uint64_t hpart = (l < 64) ? (h << l) : 0;
      out[i] =
          static_cast<int64_t>(static_cast<uint64_t>(base_v) + (hpart | low));
    }
    return;
  }
  if (s == Scheme::kDeltaFor || s == Scheme::kDeltaBitweave) {
    // Checkpointed deltas with a running sum: read the checkpoint base once per
    // block and accumulate deltas in order (O(n) instead of O(n*C)).
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    if (reg_end == nullptr || region + 2 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const uint32_t ncp = ReadBE16(region);
    const uint32_t C = kDeltaCheckpointEvery;
    // Checkpoint-count guard (mirrors Int64RegionAt / kDdRice): a hostile
    // ncp=0 or a table shorter than the last block's checkpoint would read
    // the base out of the inner payload. The table extent gate below also
    // stops a hostile ncp from walking the checkpoint reads ~512KB past
    // the region (the gate used to run only AFTER the first ReadBE64).
    if (ncp == 0 || (static_cast<uint32_t>(rc) - 1) / C >= ncp ||
        region + 2 + (static_cast<size_t>(ncp) * sizeof(int64_t)) > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const char* ck = region + 2;
    const char* ir = region + 2 + (static_cast<size_t>(ncp) * sizeof(int64_t));
    // Inner-payload extent for the FULL group must stay inside the region:
    // ForBp 8B base + rc*param bytes; BitWeave 9B header + ceil(rc*bw/8)
    // (bw validated below before the packed extent is used).
    if (s == Scheme::kDeltaFor) {
      if (prm > 8 || ir + 8 + (static_cast<size_t>(rc) * prm) > reg_end) {
        std::fill(out, out + rc, 0);
        return;
      }
    } else if (ir + 9 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const auto inner_base = static_cast<int64_t>(ReadBE64(ir));
    WordBitReader brb;
    if (s == Scheme::kDeltaBitweave) {
      const int bw = static_cast<int>(static_cast<uint8_t>(ir[8]));
      if (bw > 64) {
        std::fill(out, out + rc, 0);
        return;
      }
      const char* packed = ir + 9;
      const size_t bit_len = static_cast<size_t>(rc) * static_cast<size_t>(bw);
      const char* end = packed + ((bit_len + 7) / 8);
      end = std::min(end, reg_end);
      brb = WordBitReader(packed, end);
      // deltas[0] is unused (MakeDeltas sets it to 0); consume it to keep the
      // contiguous word-window reader aligned with rows 1..rc-1.
      (void)brb.get(bw);
    }
    uint64_t v = ReadBE64(ck);
    out[0] = static_cast<int64_t>(v);
    for (uint32_t j = 1; j < rc; ++j) {
      uint64_t d{};
      if (s == Scheme::kDeltaBitweave) {
        d = static_cast<uint64_t>(inner_base) +
            brb.get(static_cast<int>(static_cast<uint8_t>(ir[8])));
      } else {
        const auto* vp = reinterpret_cast<const unsigned char*>(
            ir + 8 + (static_cast<size_t>(j) * prm));
        uint64_t off = 0;
        if (vp + 8 <= reinterpret_cast<const unsigned char*>(reg_end)) {
          // Masked u64 load (window inside the region); the byte loop only
          // runs for the last values whose window crosses the region end.
          uint64_t raw{};
          std::memcpy(&raw, vp, 8);
          off = raw & (prm >= 8 ? ~0ULL : ((1ULL << (8U * prm)) - 1));
        } else {
          for (uint32_t b = 0; b < prm; ++b) {
            off |= static_cast<uint64_t>(vp[b]) << (b * 8);
          }
        }
        d = static_cast<uint64_t>(inner_base) + off;
      }
      if (j % C == 0) {
        v = ReadBE64(ck + ((j / C) * sizeof(int64_t)));
      } else {
        v += d;  // wrapping accumulation in the unsigned domain
      }
      out[j] = static_cast<int64_t>(v);
    }
    return;
  }
  if (s == Scheme::kWavelet || s == Scheme::kWaveletR) {
    // O(n*W) full scan via per-level cumulative popcount (rank) tables.
    // The generic per-element path costs O(n^2*W) because PopcountBits(bmp, p)
    // scans up to p bytes for each (level, index).
    const char* wv_end = RegionEndAt(footer_, g, col, data_);
    // Clamp the width byte and bound the W-bitmap extent (mirrors the
    // scalar decoder: a hostile W walked W bitmaps past the region). The
    // header extent gate also covers the min/W reads below.
    if (wv_end == nullptr || region + 9 > wv_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const auto mn = static_cast<int64_t>(ReadBE64(region));
    const int W = static_cast<int>(static_cast<uint8_t>(region[8]));
    const size_t stride = (static_cast<size_t>(rc) + 7) / 8;
    const char* base = region + 9;
    if (W <= 0 || W > 64 || base + (static_cast<size_t>(W) * stride) > wv_end) {
      std::fill(out, out + rc, mn);
      return;
    }
    // Rank tables are per 64-bit WORD (not per byte): popcount is
    // byte-order independent, so whole words build and consume 8x fewer
    // table entries, and both the build and the walk load one u64 instead
    // of touching per-byte partials.
    const size_t wwords = (stride + 7) / 8;
    std::vector<uint32_t> cum(static_cast<size_t>(W) * (wwords + 1));
    // The final bitmap byte holds rc - (stride-1)*8 valid bits; its padding
    // must not be counted. A corrupt tail byte with 1-bits in the padding
    // inflated total_ones, made `rc - total_ones` wrap to ~4e9, and the next
    // level's bmp[p >> 3] then read up to ~512 MB past the bitmap. Masking
    // the tail byte keeps bulk totals identical to the scalar path's
    // PopcountBits(bmp, n) for every input.
    const auto tail_mask = static_cast<uint8_t>(
        (rc % 8 == 0) ? 0xFFU : (0xFFU << (8U - (rc % 8))));
    for (int k = 0; k < W; ++k) {
      const auto* bmp = reinterpret_cast<const unsigned char*>(
          base + (static_cast<size_t>(k) * stride));
      uint32_t* c = &cum[static_cast<size_t>(k) * (wwords + 1)];
      c[0] = 0;
      for (size_t j = 0; j < wwords; ++j) {
        const size_t b0 = j * 8;
        uint64_t w{};
        if (b0 + 8 <= stride) {
          std::memcpy(&w, bmp + b0, sizeof(w));
        } else {
          // Partial final word: zero-pad bytes past the bitmap extent and
          // mask the final byte's padding bits.
          w = 0;
          for (size_t b = b0; b < stride; ++b) {
            unsigned byte = bmp[b];
            if (b + 1 == stride) {
              byte &= tail_mask;
            }
            w = (w << 8) | byte;
          }
        }
        c[j + 1] = c[j] + static_cast<uint32_t>(__builtin_popcountll(w));
      }
    }
    for (uint32_t i = 0; i < rc; ++i) {
      uint32_t p = i;
      uint64_t value = 0;
      for (int k = 0; k < W; ++k) {
        const auto* bmp = reinterpret_cast<const unsigned char*>(
            base + (static_cast<size_t>(k) * stride));
        const uint32_t* c = &cum[static_cast<size_t>(k) * (wwords + 1)];
        const size_t wi = p >> 6;
        const uint32_t inW = p & 63U;
        const size_t b0 = wi * 8;
        uint64_t w{};
        if (b0 + 8 <= stride) {
          std::memcpy(&w, bmp + b0, sizeof(w));
          w = __builtin_bswap64(w);  // MSB-first stream position = bit index
        } else {
          // Partial final word: zero-pad bytes past the bitmap extent and
          // mask the final byte's padding bits. The bytes must land in the
          // TOP of the word (first byte's MSB at bit 63, exactly like the
          // bswap of a full word): a bottom-aligned partial word shifted
          // every bit of the last (stride % 8) bytes -- e.g. every 928-row
          // trailing row group decoded garbage there.
          w = 0;
          for (size_t b = b0; b < stride; ++b) {
            unsigned byte = bmp[b];
            if (b + 1 == stride) {
              byte &= tail_mask;
            }
            w = (w << 8) | byte;
          }
          w <<= 64U - static_cast<unsigned>((stride - b0) * 8U);
        }
        const auto bit = static_cast<uint32_t>((w >> (63U - inW)) & 1U);
        value = (value << 1) | bit;
        uint32_t r1 = c[wi];
        if (inW != 0) {
          r1 += static_cast<uint32_t>(__builtin_popcountll(w >> (64 - inW)));
        }
        if (bit == 0) {
          p = p - r1;
        } else {
          const uint32_t total_ones = c[wwords];
          p = (static_cast<uint32_t>(rc) - total_ones) + r1;
        }
      }
      out[i] = static_cast<int64_t>(static_cast<uint64_t>(mn) + value);
    }
    return;
  }
  if (s == Scheme::kPBitS) {
    const char* pbits_end = RegionEndAt(footer_, g, col, data_);
    PBitSRegion p;
    if (pbits_end == nullptr ||
        !ParsePBitSRegion(region, rc, prm, pbits_end, &p)) {
      std::fill(out, out + rc, 0);
      return;
    }
    DecodePBitSAll(p, out);
    return;
  }
  if (s == Scheme::kPfor) {
    // Bulk PFOR decode: narrow packed scan from a word window, then scatter
    // the (sorted, tiny) exception list over the output.
    const char* exc_end = RegionEndAt(footer_, g, col, data_);
    // Header extent before any read: a degenerate region used to have its
    // first 11 bytes read from the footer/adjacent memory.
    if (exc_end == nullptr || region + 11 > exc_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const auto base_v = static_cast<int64_t>(ReadBE64(region));
    const int w = static_cast<int>(static_cast<uint8_t>(region[8]));
    if (w <= 0 || w > 64) {
      std::fill(out, out + rc, base_v);
      return;
    }
    const uint32_t nexc = ReadBE16(region + 9);
    const char* packed = region + 11;
    const size_t bit_len = static_cast<size_t>(rc) * static_cast<size_t>(w);
    const char* end = packed + ((bit_len + 7) / 8);
    // Clamp the packed-bit reader by the region: the former end was derived
    // from the header alone, so a hostile short region made the reader walk
    // up to rc*w/8 bytes past it.
    end = std::min(end, exc_end);
    WordBitReader br(packed, end);
    for (uint32_t i = 0; i < rc; ++i) {
      out[i] = static_cast<int64_t>(static_cast<uint64_t>(base_v) + br.get(w));
    }
    const char* exc = packed + ((bit_len + 7) / 8);
    // The header-derived exception extent must stay inside the region
    // before any read: a hostile nexc would otherwise walk 10*nexc bytes
    // past it (the `pos >= rc` break only fires after the read).
    if (exc + (static_cast<size_t>(nexc) * 10) > exc_end) {
      std::fill(out, out + rc, base_v);
      return;
    }
    for (uint32_t k = 0; k < nexc; ++k) {
      const uint32_t pos = ReadBE16(exc + (static_cast<size_t>(k) * 10));
      if (pos >= rc) {
        break;  // hostile region: validator rejects, stay safe
      }
      out[pos] = static_cast<int64_t>(
          ReadBE64(exc + (static_cast<size_t>(k) * 10) + 2));
    }
    return;
  }
  if (s == Scheme::kDdRice) {
    // Single-pass Rice decode: checkpoints seed (value, delta), then one
    // zigzag dd per row. Bounded by the region end (hostile streams stop
    // at QMAX per value and at the region end overall).
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    if (reg_end == nullptr || region + 2 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const uint32_t ncp = ReadBE16(region);
    // Full header extent before the k-byte / checkpoint reads: the former
    // order read region[2 + ncp*20] BEFORE any bound check, ~1.3MB past
    // the region for a hostile ncp.
    if (ncp == 0 ||
        region + 2 + (static_cast<size_t>(ncp) * 20) + 1 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const char* ck = region + 2;
    const int k = static_cast<int>(
        static_cast<uint8_t>(region[2 + (static_cast<size_t>(ncp) * 20)]));
    const char* stream = region + 2 + (static_cast<size_t>(ncp) * 20) + 1;
    if (k < 0 || k > 64 || reg_end <= stream) {
      std::fill(out, out + rc, 0);
      return;
    }
    WordBitReader br(stream, reg_end);
    uint64_t v = ReadBE64(ck);
    uint64_t d1 = ReadBE64(ck + 8);
    out[0] = static_cast<int64_t>(v);
    for (uint32_t j = 1; j < rc; ++j) {
      uint64_t q = 0;
      while (br.get1() != 0) {
        if (++q > kRiceQMax) {
          // Mirror the scalar walk: a replay crossing the QMax cell returns
          // 0 for that row AND every row after it (any later-block replay
          // crosses the same stream position). The old bulk form kept
          // folding the capped quotient into v, so rows after a hostile
          // cell disagreed with Int64RegionAt on the same region.
          std::fill(out + j, out + rc, 0);
          return;
        }
      }
      uint64_t rem = 0;
      if (k > 0 && k <= 64) {
        rem = br.get(k);
      }
      const uint64_t zz = (k >= 64) ? rem : ((q << k) | rem);
      d1 += (zz >> 1) ^ (0ULL - (zz & 1ULL));
      v += d1;
      out[j] = static_cast<int64_t>(v);
    }
    return;
  }
  if (s == Scheme::kDeltaVarint) {
    // Single-pass LEB128 delta decode from the block-0 base value.
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    // Header extent ([ncp u16][base u64]) before any read.
    if (reg_end == nullptr || region + 10 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const uint32_t ncp = ReadBE16(region);
    if (ncp == 0) {
      std::fill(out, out + rc, 0);
      return;
    }
    uint64_t v = ReadBE64(region + 2);
    out[0] = static_cast<int64_t>(v);
    // Bound the header tables before forming the stream pointer: a hostile
    // ncp (up to 65535) would otherwise form `stream` ~786 KB past the
    // region (the per-byte loop guard bounds only dereferences). Encoder
    // regions always carry the 1-byte deltas[0] placeholder, so this never
    // rejects a valid region.
    if (region + 2 + (static_cast<size_t>(ncp) * 12) + 1 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const char* stream =
        region + 2 + (static_cast<size_t>(ncp) * 12) + 1;  // skip deltas[0]
    const char* p = stream;
    for (uint32_t j = 1; j < rc; ++j) {
      uint64_t z = 0;
      bool done = false;
      for (int b = 0; b < 10; ++b) {
        if (reg_end != nullptr && p + b >= reg_end) {
          break;
        }
        const auto byte = static_cast<uint8_t>(p[b]);
        z |= static_cast<uint64_t>(byte & 0x7F) << (7 * b);
        if ((byte & 0x80) == 0) {
          p += b + 1;
          done = true;
          break;
        }
      }
      if (!done) {
        // Fail closed like the scalar walk does for truncated bytes: this
        // row and the tail answer as raw 0 instead of the old form's
        // silently-truncated prefix with unwritten remaining cells.
        std::fill(out + j, out + rc, 0);
        return;
      }
      v += (z >> 1) ^ (0ULL - (z & 1ULL));
      out[j] = static_cast<int64_t>(v);
    }
    return;
  }
  if (s == Scheme::kChimp) {
    // Single-pass Chimp decode (mirrors the Gorilla bulk path).
    const char* reg_end = RegionEndAt(footer_, g, col, data_);
    if (reg_end == nullptr || region + 8 > reg_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    uint64_t prev = ReadBE64(region);
    out[0] = static_cast<int64_t>(prev);
    int prev_lz = 0, prev_tz = 64;
    const char* packed = region + 8;
    const size_t bit_len =
        (rc > 1) ? static_cast<size_t>(rc - 1) * (2 + 5 + 6 + 64) : 0;
    const char* end =
        (reg_end != nullptr) ? reg_end : packed + ((bit_len + 7) / 8);
    WordBitReader br(packed, end);
    for (uint32_t i = 1; i < rc; ++i) {
      if (br.get1() == 0) {
        // identical
      } else if (br.get1() == 0) {
        const int width = 64 - prev_lz - prev_tz;
        if (width > 0 && width <= 64) {
          const uint64_t m = br.get(width);
          prev ^= (m << prev_tz);
        }
      } else {
        const int lz = static_cast<int>(br.get(5));
        const int tz = static_cast<int>(br.get(6));
        int mw = 64 - lz - tz;
        mw = std::max(mw, 1);
        mw = std::min(mw, 64);
        const uint64_t m = br.get(mw);
        prev ^= (mw >= 64 ? m : (m << tz));
        prev_lz = lz;
        prev_tz = tz;
      }
      out[i] = static_cast<int64_t>(prev);
    }
    return;
  }
  if (s == Scheme::kRleDict || s == Scheme::kOpRle) {
    // Single-pass run-table expansion (the per-row Int64RegionAt walk is
    // O(runs) per row). Clamped codes and rows past the run walk decode as
    // raw 0, matching the scalar arm. kOpRle shares the byte layout.
    const char* rd_end = RegionEndAt(footer_, g, col, data_);
    if (rd_end == nullptr || region + 2 > rd_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const uint16_t d = ReadBE16(region);
    if (d == 0) {
      std::fill(out, out + rc, 0);
      return;
    }
    const int cb = (BitsNeeded(d - 1) + 7) / 8;
    const char* dict = region + 2;
    const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
    if (runs_hdr + 4 > rd_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    const uint32_t runs = ReadBE32(runs_hdr);
    if (runs_hdr + 4 +
            (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
        rd_end) {
      std::fill(out, out + rc, 0);
      return;
    }
    uint32_t row = 0;
    const char* q = runs_hdr + 4;
    for (uint32_t run = 0; run < runs && row < rc; ++run) {
      uint64_t code = 0;
      for (int b = 0; b < cb; ++b) {
        code = (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
      }
      const int64_t value =
          (code >= d) ? 0
                      : static_cast<int64_t>(
                            ReadBE64(dict + (static_cast<size_t>(code) * 8)));
      const uint64_t len = ReadBE32(q + cb);
      const uint64_t next = static_cast<uint64_t>(row) + len;
      const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
      std::fill(out + row, out + end, value);
      row = end;
      q += static_cast<size_t>(cb) + 4;
    }
    std::fill(out + row, out + rc, 0);
    return;
  }
  const char* reg_end = RegionEndAt(footer_, g, col, data_);
  if (DecodeCascadeI64All(region, s, prm, rc, reg_end, out)) {
    return;
  }
  for (uint32_t i = 0; i < rc; ++i) {
    out[i] = Int64RegionAt(region, s, prm, rc, i, reg_end);
  }
}

bool ByteSliceColumnSum(const PaxChunk& chunk, slot_t col, uint32_t g,
                        int64_t* sum_out) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  // Public API guard, same contract as ReduceColumn / BuildSelectionVector:
  // g and the per-cell footer arrays are caller/hand-built-footer controlled;
  // an out-of-range index reads out of bounds before any row work.
  if (col >= f.column_count || g >= f.row_group_count ||
      g >= f.row_counts.size()) {
    return false;
  }
  const uint32_t off_idx = (g * f.column_count) + col;
  if (off_idx >= f.column_offsets.size() || off_idx >= f.encodings.size() ||
      off_idx >= f.params.size()) {
    return false;
  }
  if (static_cast<Scheme>(f.encodings[off_idx]) != Scheme::kByteSlice) {
    return false;
  }
  // Form the region pointer through the verified-offset helper: indexing
  // Data().data() with the raw footer value formed a wild pointer before any
  // deref (the reg_end gate below then ran on garbage).
  const char* region = RegionStartAt(f, g, col, chunk.Data(), 1);
  // The slice layout is driven by the region's own width byte, so bound the
  // walk against the actual region extent (the validator enforces this for
  // validated chunks; a hand-built footer must not over-read into the
  // footer/adjacent regions).
  const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
  if (region == nullptr || reg_end == nullptr || reg_end <= region) {
    return false;
  }
  const uint16_t rc = f.row_counts[g];
  const auto w = static_cast<uint8_t>(region[0]);
  // Same width clamp as Int64RegionAt / ByteSliceFilteredSum: w past 64 has
  // no decodable int64 meaning, and the __int128 shift below is UB for
  // b >= 128 (a hostile width byte reached 255).
  if (w == 0 || w > 64) {
    return false;
  }
  const auto stride = static_cast<size_t>((rc + 7U) / 8U);
  if (static_cast<size_t>(reg_end - region) <
      1 + (static_cast<size_t>(w) * stride)) {
    return false;
  }
  __int128 sum = 0;
  // Popcount only the rc DATA bits: the final partial stride byte's padding
  // bits are zero in builder output but garbage in corrupt/hand-built
  // regions, and counting them inflated the sum by up to 7*(2^w-1) per
  // slice. full = number of complete bytes; the tail byte is masked.
  const size_t full = static_cast<size_t>(rc) >> 3;
  const auto tail_bits = static_cast<uint8_t>(rc & 7U);
  // MSB-first keep-mask for the tail byte's `tail_bits` data bits.
  static const std::array<uint8_t, 8> kTailMask = {0x00, 0x80, 0xC0, 0xE0,
                                                   0xF0, 0xF8, 0xFC, 0xFE};
  const uint8_t tail_mask = kTailMask[tail_bits & 7U];
#ifdef __AVX2__
  // Vectorized popcount over 32-byte lanes; weighted-sum the per-bit counts
  // without ever materializing a value (the byte-slice SUM identity).
  const __m256i pop_lut =
      _mm256_setr_epi8(0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 0, 1, 1,
                       2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
  const __m256i nib_mask = _mm256_set1_epi8(0x0F);
  const __m256i zero = _mm256_setzero_si256();
  for (uint8_t b = 0; b < w; ++b) {
    const unsigned char* sl = reinterpret_cast<const unsigned char*>(region) +
                              1 + (static_cast<size_t>(b) * stride);
    __m256i acc = _mm256_setzero_si256();
    size_t i = 0;
    for (; i + 32 <= full; i += 32) {
      __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sl + i));
      __m256i lo = _mm256_and_si256(v, nib_mask);
      __m256i hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), nib_mask);
      __m256i pc = _mm256_add_epi8(_mm256_shuffle_epi8(pop_lut, lo),
                                   _mm256_shuffle_epi8(pop_lut, hi));
      acc = _mm256_add_epi64(acc, _mm256_sad_epu8(pc, zero));
    }
    uint64_t cnt = 0;
    std::array<uint64_t, 4> lanes{};
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes.data()), acc);
    cnt = lanes[0] + lanes[1] + lanes[2] + lanes[3];
    for (; i < full; ++i) {
      cnt += static_cast<uint64_t>(__builtin_popcount(
          static_cast<unsigned>(static_cast<unsigned char>(sl[i]))));
    }
    if (tail_bits != 0U) {
      cnt += static_cast<uint64_t>(__builtin_popcount(
          static_cast<unsigned>(static_cast<uint8_t>(sl[full]) & tail_mask)));
    }
    sum += static_cast<__int128>(cnt) * (static_cast<__int128>(1) << b);
  }
#else
  for (uint8_t b = 0; b < w; ++b) {
    const char* sl = region + 1 + static_cast<size_t>(b) * stride;
    uint64_t cnt = 0;
    for (size_t i = 0; i < full; ++i) {
      cnt += static_cast<uint64_t>(__builtin_popcount(
          static_cast<unsigned>(static_cast<unsigned char>(sl[i]))));
    }
    if (tail_bits != 0U) {
      cnt += __builtin_popcount(
          static_cast<unsigned>(static_cast<uint8_t>(sl[full]) & tail_mask));
    }
    sum += static_cast<__int128>(cnt) * (static_cast<__int128>(1) << b);
  }
#endif
  *sum_out = static_cast<int64_t>(sum);
  return true;
}

bool PBitSColumnSum(const PaxChunk& chunk, slot_t col, uint32_t g,
                    int64_t* sum_out) {
  if (sum_out == nullptr) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || g >= f.row_group_count ||
      g >= f.row_counts.size()) {
    return false;
  }
  const uint32_t off_idx = (g * f.column_count) + col;
  if (off_idx >= f.column_offsets.size() || off_idx >= f.encodings.size() ||
      off_idx >= f.params.size() ||
      static_cast<Scheme>(f.encodings[off_idx]) != Scheme::kPBitS) {
    return false;
  }
  const char* region = RegionStartAt(f, g, col, chunk.Data(), 11);
  const char* end = RegionEndAt(f, g, col, chunk.Data());
  if (region == nullptr || end == nullptr || end <= region) {
    return false;
  }
  PBitSRegion p;
  if (!ParsePBitSRegion(region, f.row_counts[g], f.params[off_idx], end, &p)) {
    return false;
  }
  *sum_out = PaxChunk::PBitSFilteredSum(region, f.params[off_idx], p.n, end,
                                        nullptr, nullptr);
  return true;
}

bool Chimp128ColumnCountSum(const PaxChunk& chunk, slot_t col,
                            PaxChunk::CompareOp op, double literal,
                            uint64_t* count_out, double* sum_out,
                            const Schema* sc) {
  if (count_out == nullptr || sum_out == nullptr) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return false;
  }
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kDouble)) {
    return false;
  }
  const auto matches = [](double value, PaxChunk::CompareOp compare,
                          double operand) {
    switch (compare) {
      case PaxChunk::CompareOp::kEq:
        return value == operand;
      case PaxChunk::CompareOp::kLe:
        return value <= operand;
      case PaxChunk::CompareOp::kLt:
        return value < operand;
      case PaxChunk::CompareOp::kGe:
        return value >= operand;
      case PaxChunk::CompareOp::kGt:
        return value > operand;
    }
    return false;
  };
  const auto all_match = [&](double minimum, double maximum) {
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        return minimum == maximum && minimum == literal;
      case PaxChunk::CompareOp::kLe:
        return maximum <= literal;
      case PaxChunk::CompareOp::kLt:
        return maximum < literal;
      case PaxChunk::CompareOp::kGe:
        return minimum >= literal;
      case PaxChunk::CompareOp::kGt:
        return minimum > literal;
    }
    return false;
  };
  const auto none_match = [&](double minimum, double maximum) {
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        return literal < minimum || literal > maximum;
      case PaxChunk::CompareOp::kLe:
        return minimum > literal;
      case PaxChunk::CompareOp::kLt:
        return minimum >= literal;
      case PaxChunk::CompareOp::kGe:
        return maximum < literal;
      case PaxChunk::CompareOp::kGt:
        return maximum <= literal;
    }
    return true;
  };
  uint64_t count = 0;
  double sum = 0.0;
  bool has_chimp128 = false;
  for (uint32_t group = 0; group < f.row_group_count; ++group) {
    const uint16_t rows = f.row_counts[group];
    if (rows > PaxChunk::kRowGroupSize) {
      return false;
    }
    if (rows == 0) {
      continue;
    }
    const size_t offset = (static_cast<size_t>(group) * f.column_count) + col;
    if (static_cast<Scheme>(f.encodings[offset]) != Scheme::kChimp128) {
      std::array<int64_t, PaxChunk::kRowGroupSize> decoded{};
      chunk.DecodeInt64ColumnRg(col, group, decoded.data());
      for (uint32_t row = 0; row < rows; ++row) {
        if (CellIsNull(f, group, col, row)) {
          continue;
        }
        const double value =
            Chimp128Double(static_cast<uint64_t>(decoded[row]));
        if (!matches(value, op, literal)) {
          continue;
        }
        ++count;
        sum += value;
      }
      continue;
    }
    has_chimp128 = true;
    const char* region = RegionStartAt(f, group, col, chunk.Data(), 4);
    const char* end = RegionEndAt(f, group, col, chunk.Data());
    Chimp128Region parsed;
    if (region == nullptr || end == nullptr ||
        !ParseChimp128Region(region, rows, f.params[offset], end, &parsed) ||
        (!chunk.HasValidatedChimp128Metadata() &&
         !Chimp128MetadataMatches(parsed))) {
      return false;
    }
    for (uint32_t block = 0; block < parsed.block_count; ++block) {
      const Chimp128Block& metadata = parsed.blocks[block];
      const uint32_t start = block * kChimp128BlockRows;
      uint32_t nulls = 0;
      for (uint32_t row = 0; row < metadata.rows; ++row) {
        nulls += CellIsNull(f, group, col, start + row) ? 1U : 0U;
      }
      if (metadata.numeric_metadata) {
        const double minimum = Chimp128Double(metadata.minimum);
        const double maximum = Chimp128Double(metadata.maximum);
        if (none_match(minimum, maximum)) {
          continue;
        }
        if (all_match(minimum, maximum) && nulls == 0) {
          count += metadata.rows;
          sum += Chimp128Double(metadata.sum);
          continue;
        }
      }
      std::array<uint64_t, kChimp128BlockRows> decoded{};
      if (!DecodeChimp128Block(metadata, decoded.data())) {
        return false;
      }
      for (uint32_t row = 0; row < metadata.rows; ++row) {
        if (CellIsNull(f, group, col, start + row)) {
          continue;
        }
        if (!matches(Chimp128Double(decoded[row]), op, literal)) {
          continue;
        }
        ++count;
        sum += Chimp128Double(decoded[row]);
      }
    }
  }
  if (!has_chimp128) {
    return false;
  }
  *count_out = count;
  *sum_out = sum;
  return true;
}

bool AlpRdTansColumnCountSum(const PaxChunk& chunk, slot_t col,
                             PaxChunk::CompareOp op, double literal,
                             uint64_t* count_out, double* sum_out,
                             const Schema* sc) {
  if (count_out == nullptr || sum_out == nullptr) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return false;
  }
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kDouble)) {
    return false;
  }
  const auto matches = [](double value, PaxChunk::CompareOp compare,
                          double operand) {
    switch (compare) {
      case PaxChunk::CompareOp::kEq:
        return value == operand;
      case PaxChunk::CompareOp::kLe:
        return value <= operand;
      case PaxChunk::CompareOp::kLt:
        return value < operand;
      case PaxChunk::CompareOp::kGe:
        return value >= operand;
      case PaxChunk::CompareOp::kGt:
        return value > operand;
    }
    return false;
  };
  const auto all_match = [&](double minimum, double maximum) {
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        return minimum == maximum && minimum == literal;
      case PaxChunk::CompareOp::kLe:
        return maximum <= literal;
      case PaxChunk::CompareOp::kLt:
        return maximum < literal;
      case PaxChunk::CompareOp::kGe:
        return minimum >= literal;
      case PaxChunk::CompareOp::kGt:
        return minimum > literal;
    }
    return false;
  };
  const auto none_match = [&](double minimum, double maximum) {
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        return literal < minimum || literal > maximum;
      case PaxChunk::CompareOp::kLe:
        return minimum > literal;
      case PaxChunk::CompareOp::kLt:
        return minimum >= literal;
      case PaxChunk::CompareOp::kGe:
        return maximum < literal;
      case PaxChunk::CompareOp::kGt:
        return maximum <= literal;
    }
    return true;
  };
  uint64_t count = 0;
  double sum = 0.0;
  bool has_alp_rd_tans = false;
  for (uint32_t group = 0; group < f.row_group_count; ++group) {
    const uint16_t rows = f.row_counts[group];
    if (rows > PaxChunk::kRowGroupSize) {
      return false;
    }
    if (rows == 0) {
      continue;
    }
    const size_t offset = (static_cast<size_t>(group) * f.column_count) + col;
    if (static_cast<Scheme>(f.encodings[offset]) != Scheme::kAlpRdTans) {
      std::array<int64_t, PaxChunk::kRowGroupSize> decoded{};
      chunk.DecodeInt64ColumnRg(col, group, decoded.data());
      for (uint32_t row = 0; row < rows; ++row) {
        if (CellIsNull(f, group, col, row)) {
          continue;
        }
        const double value =
            AlpRdTansDouble(static_cast<uint64_t>(decoded[row]));
        if (!matches(value, op, literal)) {
          continue;
        }
        ++count;
        sum += value;
      }
      continue;
    }
    has_alp_rd_tans = true;
    const char* region =
        RegionStartAt(f, group, col, chunk.Data(), kAlpRdTansHeaderBytes);
    const char* end = RegionEndAt(f, group, col, chunk.Data());
    AlpRdTansRegion parsed;
    if (region == nullptr || end == nullptr ||
        !ParseAlpRdTansRegion(region, rows, f.params[offset], end, &parsed) ||
        (!chunk.HasValidatedAlpRdTansMetadata() &&
         !AlpRdTansMetadataMatches(parsed))) {
      return false;
    }
    for (uint32_t block = 0; block < parsed.block_count; ++block) {
      const AlpRdTansBlock& metadata = parsed.blocks[block];
      const uint32_t start = block * kAlpRdTansBlockRows;
      uint32_t nulls = 0;
      for (uint32_t row = 0; row < metadata.rows; ++row) {
        nulls += CellIsNull(f, group, col, start + row) ? 1U : 0U;
      }
      if ((metadata.flags & 1U) != 0) {
        const double minimum = AlpRdTansDouble(metadata.minimum);
        const double maximum = AlpRdTansDouble(metadata.maximum);
        if (none_match(minimum, maximum)) {
          continue;
        }
        if (all_match(minimum, maximum) && nulls == 0) {
          count += metadata.rows;
          sum += AlpRdTansDouble(metadata.sum);
          continue;
        }
      }
      std::vector<uint64_t> decoded;
      if (!AlpRdTansDecodeBlock(parsed, block, &decoded)) {
        return false;
      }
      for (uint32_t row = 0; row < metadata.rows; ++row) {
        if (CellIsNull(f, group, col, start + row)) {
          continue;
        }
        const double value = AlpRdTansDouble(decoded[row]);
        if (!matches(value, op, literal)) {
          continue;
        }
        ++count;
        sum += value;
      }
    }
  }
  if (!has_alp_rd_tans) {
    return false;
  }
  *count_out = count;
  *sum_out = sum;
  return true;
}

bool PatasColumnCountSum(const PaxChunk& chunk, slot_t col,
                         PaxChunk::CompareOp op, double literal,
                         uint64_t* count_out, double* sum_out,
                         const Schema* sc) {
  if (count_out == nullptr || sum_out == nullptr) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return false;
  }
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kDouble)) {
    return false;
  }
  const auto matches = [](double value, PaxChunk::CompareOp compare,
                          double operand) {
    switch (compare) {
      case PaxChunk::CompareOp::kEq:
        return value == operand;
      case PaxChunk::CompareOp::kLe:
        return value <= operand;
      case PaxChunk::CompareOp::kLt:
        return value < operand;
      case PaxChunk::CompareOp::kGe:
        return value >= operand;
      case PaxChunk::CompareOp::kGt:
        return value > operand;
    }
    return false;
  };
  const auto all_match = [&](double minimum, double maximum) {
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        return minimum == maximum && minimum == literal;
      case PaxChunk::CompareOp::kLe:
        return maximum <= literal;
      case PaxChunk::CompareOp::kLt:
        return maximum < literal;
      case PaxChunk::CompareOp::kGe:
        return minimum >= literal;
      case PaxChunk::CompareOp::kGt:
        return minimum > literal;
    }
    return false;
  };
  const auto none_match = [&](double minimum, double maximum) {
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        return literal < minimum || literal > maximum;
      case PaxChunk::CompareOp::kLe:
        return minimum > literal;
      case PaxChunk::CompareOp::kLt:
        return minimum >= literal;
      case PaxChunk::CompareOp::kGe:
        return maximum < literal;
      case PaxChunk::CompareOp::kGt:
        return maximum <= literal;
    }
    return true;
  };
  uint64_t count = 0;
  double sum = 0.0;
  bool has_patas = false;
  for (uint32_t group = 0; group < f.row_group_count; ++group) {
    const uint16_t rows = f.row_counts[group];
    if (rows > PaxChunk::kRowGroupSize) {
      return false;
    }
    if (rows == 0) {
      continue;
    }
    const size_t offset = (static_cast<size_t>(group) * f.column_count) + col;
    if (static_cast<Scheme>(f.encodings[offset]) != Scheme::kPatas) {
      std::array<int64_t, PaxChunk::kRowGroupSize> decoded{};
      chunk.DecodeInt64ColumnRg(col, group, decoded.data());
      for (uint32_t row = 0; row < rows; ++row) {
        if (CellIsNull(f, group, col, row)) {
          continue;
        }
        const double value = PatasDouble(static_cast<uint64_t>(decoded[row]));
        if (!matches(value, op, literal)) {
          continue;
        }
        ++count;
        sum += value;
      }
      continue;
    }
    has_patas = true;
    const char* region =
        RegionStartAt(f, group, col, chunk.Data(), kPatasHeaderBytes);
    const char* end = RegionEndAt(f, group, col, chunk.Data());
    PatasRegion parsed;
    if (region == nullptr || end == nullptr ||
        !ParsePatasRegion(region, rows, f.params[offset], end, &parsed) ||
        (!chunk.HasValidatedPatasMetadata() && !PatasMetadataMatches(parsed))) {
      return false;
    }
    for (uint32_t block = 0; block < parsed.block_count; ++block) {
      const PatasBlock& metadata = parsed.blocks[block];
      const uint32_t start = block * kPatasBlockRows;
      uint32_t nulls = 0;
      for (uint32_t row = 0; row < metadata.rows; ++row) {
        nulls += CellIsNull(f, group, col, start + row) ? 1U : 0U;
      }
      if ((metadata.flags & 1U) != 0) {
        const double minimum = PatasDouble(metadata.minimum);
        const double maximum = PatasDouble(metadata.maximum);
        if (none_match(minimum, maximum)) {
          continue;
        }
        if (all_match(minimum, maximum) && nulls == 0) {
          count += metadata.rows;
          sum += PatasDouble(metadata.sum);
          continue;
        }
      }
      std::array<uint64_t, kPatasBlockRows> decoded{};
      if (!DecodePatasBlock(metadata, decoded.data())) {
        return false;
      }
      for (uint32_t row = 0; row < metadata.rows; ++row) {
        if (CellIsNull(f, group, col, start + row)) {
          continue;
        }
        const double value = PatasDouble(decoded[row]);
        if (!matches(value, op, literal)) {
          continue;
        }
        ++count;
        sum += value;
      }
    }
  }
  if (!has_patas) {
    return false;
  }
  *count_out = count;
  *sum_out = sum;
  return true;
}

uint64_t ApproxCountDistinct(const PaxChunk& chunk, slot_t col) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (!f.hll_present || col >= f.column_count) {
    return 0;
  }
  const uint32_t cc = f.column_count;
  const uint32_t m = 64;  // registers per (rg,col) sketch
  std::vector<uint8_t> M(m, 0);
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const size_t base = (static_cast<size_t>(g) * cc + col) * m;
    if (base + m > f.hll_sketches.size()) {
      break;
    }
    for (uint32_t r = 0; r < m; ++r) {
      M[r] = std::max(M[r], f.hll_sketches[base + r]);
    }
  }
  double sum_inv = 0;
  uint32_t zeros = 0;
  for (uint32_t r = 0; r < m; ++r) {
    // `1ULL << 64` is UB. Legacy chunks written before the encoder was
    // clamped may carry a register value of 64 (only reachable for hash
    // output h == 0, e.g. a single int64 key of value 0). Treat 64 as 63
    // so the estimator stays finite; that contribution is negligible.
    const uint8_t bits = M[r] > 63 ? 63 : M[r];
    sum_inv += 1.0 / static_cast<double>(1ULL << bits);
    if (M[r] == 0) {
      ++zeros;
    }
  }
  const double alpha = 0.709;  // HLL constant for m = 64
  double E = alpha * static_cast<double>(m) * m / sum_inv;
  if (E < 2.5 * m && zeros > 0) {  // small-range (linear counting) correction
    E = static_cast<double>(m) * std::log(static_cast<double>(m) / zeros);
  }
  return static_cast<uint64_t>(std::llround(E));
}

// Forward declaration: defined below next to the compressed-domain predicate
// kernels, but ReduceColumn (above it) has to ask it whether a mask can be
// answered without decoding.
namespace {
bool TryFastPredicateMask(const PaxChunk& chunk, const Predicate& p, uint32_t g,
                          uint64_t* out);

// Build a selection bitmask for a SINGLE predicate on an ALREADY-DECODED
// column. This is the decode-free half of BuildSelectionVector: the caller
// already paid for the decode, so all that is left is the comparison.
//
// The caller must mask off the bits at or beyond `rc` afterwards. The AVX2
// kernel is a fill, not an accumulate, so unlike the accumulate path it may
// leave the tail word's high bits set; FillPredicateMaskAvx2 documents what it
// writes, and the scalar path only ever sets bits below `rc`.
PaxSelectionVector SelectionFromDecodedValues(const int64_t* vals, uint16_t rc,
                                              const Predicate& p) {
  PaxSelectionVector sv;
  const size_t nwords = (static_cast<size_t>(rc) + 63U) / 64U;
  sv.bits.assign(nwords, 0);
  if (rc == 0) {
    return sv;
  }
  uint64_t* const out = sv.bits.data();
  bool filled = false;
#ifdef __AVX2__
  filled = FillPredicateMaskAvx2(vals, rc, p.op, p.lit, out);
#endif
  if (!filled) {
    for (uint16_t i = 0; i < rc; ++i) {
      if (ScanMatch(vals[i], p.op, p.lit)) {
        out[i >> 6] |= 1ULL << (i & 63U);
      }
    }
  }
  // Clear bits at or beyond the last row so the popcount below -- and any
  // consumer that trusts `count` -- never sees phantom rows.
  if ((rc & 63U) != 0U) {
    out[rc >> 6] &= (1ULL << (rc & 63U)) - 1ULL;
  }
  uint64_t total = 0;
  for (size_t w = 0; w < nwords; ++w) {
    total += static_cast<uint64_t>(__builtin_popcountll(out[w]));
  }
  sv.count = total;
  return sv;
}

}  // namespace

ReduceResult ReduceColumn(const PaxChunk& chunk, const ReducePlan& plan,
                          bool parallel, int nthreads, uint32_t scan_sweeps,
                          const Schema* sc, WorkloadTracker* workload) {
  (void)parallel;
  (void)nthreads;
  ReduceResult res;
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint32_t rg = f.row_group_count;
  // Public API guard: plan.col indexes column_offsets and the region
  // decoder below; an out-of-range column read out of bounds. Predicate
  // columns flow into BuildSelectionVector -> DecodeInt64ColumnRg the same
  // way, so validate them too. With a schema supplied, additionally gate
  // every referenced column on its DECLARED type: decoding a varchar
  // region as int64 yields plausible garbage, not an error.
  if (plan.col >= f.column_count) {
    return ReduceResult{};
  }
  for (const auto& p : plan.preds) {
    if (p.col >= f.column_count) {
      return ReduceResult{};
    }
  }
  // Hand-built-footer guard: every loop below indexes row_counts[g] and the
  // per-cell footer arrays before any validation. Declared-but-absent
  // arrays would read out of bounds; report the chunk unusable instead.
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return ReduceResult{};
  }
  if (sc != nullptr) {
    const auto i64 = [sc](slot_t c) {
      return c < sc->ColumnCount() &&
             sc->GetColumn(c).Type() == ValueType::kInt64;
    };
    if (!i64(plan.col)) {
      return ReduceResult{};
    }
    for (const auto& p : plan.preds) {
      if (!i64(p.col)) {
        return ReduceResult{};
      }
    }
  }
  if (workload != nullptr && sc != nullptr) {
    workload->RecordAggregate(*sc, plan.col);
    for (const auto& p : plan.preds) {
      workload->RecordFilter(*sc, p.col);
    }
  }

  // APPROX_COUNT_DISTINCT: answered entirely from the footer HLL sketches
  // — but only for an UNPREDICATED plan. The old fast path ran before any
  // predicate evaluation and returned the whole-column estimate regardless
  // of the WHERE clause.
  if (plan.op == AggOp::kApproxDistinct && plan.preds.empty()) {
    res.approx_distinct = ApproxCountDistinct(chunk, plan.col);
    res.valid = true;
    return res;
  }

  // MIN/MAX over an unpredicated column: pure zone-map metadata (no decode).
  if (plan.preds.empty() &&
      (plan.op == AggOp::kMin || plan.op == AggOp::kMax)) {
    int64_t mn = 0, mx = 0;
    if (chunk.ColumnMinMax(plan.col, &mn, &mx) == Status::kSuccess) {
      const uint64_t total = chunk.TotalRowCount();
      res.count = total;
      res.min_val = mn;
      res.max_val = mx;
      res.valid = total > 0;
      return res;
    }
  }

  const uint32_t sweeps = std::max<uint32_t>(scan_sweeps, 1);
  bool minmax_seen = false;
  // Predicated APPROX_COUNT_DISTINCT falls back to an EXACT distinct count
  // over the decoded selected values (the footer HLL cannot be filtered).
  std::unordered_set<int64_t> distinct_seen;
  const bool predicates_select_all =
      std::all_of(plan.preds.begin(), plan.preds.end(), [](const Predicate& p) {
        return (p.op == ScanOp::kGe && p.lit == INT64_MIN) ||
               (p.op == ScanOp::kLe && p.lit == INT64_MAX);
      });
  (void)predicates_select_all;
  for (uint32_t sw = 0; sw < sweeps; ++sw) {
    for (uint32_t g = 0; g < rg; ++g) {
      const uint16_t rc = f.row_counts[g];
#ifdef __AVX2__
      if (predicates_select_all && plan.op == AggOp::kSum &&
          rc <= PaxChunk::kRowGroupSize) {
        const uint32_t toff = (g * f.column_count) + plan.col;
        if (static_cast<Scheme>(f.encodings[toff]) == Scheme::kPlain &&
            RgColNullBits(f, g, plan.col) == nullptr) {
          const char* region = RegionStartAt(f, g, plan.col, chunk.Data(), 0);
          const char* region_end = RegionEndAt(f, g, plan.col, chunk.Data());
          const size_t required = static_cast<size_t>(rc) * sizeof(int64_t);
          if (region != nullptr && region_end != nullptr &&
              std::cmp_greater_equal(region_end - region, required)) {
            __int128 sum = 0;
            if (PlainSum128Avx2(region, rc, &sum)) {
              res.count += rc;
              if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
                return ReduceResult{};
              }
              continue;
            }
          }
        }
      }
#endif
      // Fused decode for `SELECT sum(x) ... WHERE x <op> lit` on a column with
      // no compressed-domain predicate kernel -- the shape this decode
      // dominates.
      //
      // Both halves of that query want the SAME decoded column: the selection
      // mask has to decode x to compare it, and the fold has to decode x again
      // to aggregate it. Decoding is the single most expensive step in a scan
      // (it materialises 8 KB of int64 per row group), so the pair was paying
      // for it twice. Decode once into a stack buffer, build the mask from it,
      // and hand the very same buffer to the fold below.
      //
      // Gated to a single predicate on the aggregate column, and only when
      // TryFastPredicateMask DECLINES: if a compressed kernel can answer the
      // mask without decoding, decoding it here would throw that win away.
      std::array<int64_t, PaxChunk::kRowGroupSize> fused_stack{};
      const int64_t* fused_vals = nullptr;
      // A compressed-domain mask already computed for THIS cell by THIS
      // predicate. TryFastPredicateMask used to run here purely as a probe and
      // the mask it wrote was thrown away, with BuildSelectionVector calling
      // the identical kernel on the identical cell immediately afterwards --
      // so the single most expensive step of a predicated compressed-execution
      // reduce (walking the whole code/run/word stream) was done TWICE per row
      // group. Profiling the low-cardinality DICT SUM put 36% of the whole
      // reduce in DictPassMaskByteExpanded for exactly this reason.
      PaxSelectionVector sel;
      bool sel_ready = false;
      if (plan.preds.size() == 1 && plan.preds[0].col == plan.col &&
          rc <= PaxChunk::kRowGroupSize) {
        // A full row group's worth of words, because a DECLINING
        // TryFastPredicateMask may have written part of one before it bailed.
        std::array<uint64_t, (PaxChunk::kRowGroupSize / 64) + 1> probe{};
        std::memset(probe.data(), 0, sizeof(probe));
        if (TryFastPredicateMask(chunk, plan.preds[0], g, probe.data())) {
          const size_t nwords = (static_cast<size_t>(rc) + 63U) / 64U;
          sel.bits.assign(probe.data(), probe.data() + nwords);
          // The kernels only ever SET bits, so the memset above leaves the
          // rest clear -- but a kernel may have set bits at or past rc in the
          // final word, and BuildSelectionVector trims those before its
          // popcount. Reusing the probe means trimming them here, or `count`
          // and every selection test would see rows that do not exist.
          if ((rc & 63U) != 0U) {
            sel.bits[nwords - 1] &= (1ULL << (rc & 63U)) - 1ULL;
          }
          sel.count = 0;
          for (size_t w = 0; w < nwords; ++w) {
            sel.count +=
                static_cast<uint64_t>(__builtin_popcountll(sel.bits[w]));
          }
          sel_ready = true;
        } else {
          chunk.DecodeInt64ColumnRg(plan.col, g, fused_stack.data());
          fused_vals = fused_stack.data();
        }
      }
      // M2 selection vector: predicates fused, decode only the reduced column.
      if (!sel_ready) {
        sel = fused_vals != nullptr
                  ? SelectionFromDecodedValues(fused_vals, rc, plan.preds[0])
                  : BuildSelectionVector(chunk, plan.preds, g, false);
      }
      res.count += sel.count;
      if (plan.op == AggOp::kCount) {
        continue;
      }
      if (sel.count == 0) {
        continue;
      }
      // Compressed-execution target kernels: SUM/MIN/MAX evaluated over
      // CONST/RLE runs and DICT dictionaries directly. NULL cells store raw
      // 0 in the region (exactly what the generic path decodes), so a
      // nonzero value implies a non-null row; only the zero bucket needs
      // the null bitmap before feeding MIN/MAX.
      const uint32_t toff = (g * f.column_count) + plan.col;
      const auto ts = static_cast<Scheme>(f.encodings[toff]);
      // Offset-VALUE validation: the array-length guards at the top of this
      // function prove only that the footer entry exists; a hand-built
      // offset value formed a wild pointer that the kConst/kDict paths
      // below dereferenced before their per-scheme extent gates. Form the
      // pointer via RegionStartAt and require room for the largest fixed
      // header (kConst's 8B value; kDict's count and kRle's runs are gated
      // again per scheme). An unusable region falls through to the generic
      // decode, which clamps every cell.
      const char* tr = RegionStartAt(f, g, plan.col, chunk.Data(), 0);
      const char* treg_end = RegionEndAt(f, g, plan.col, chunk.Data());
      if (ts == Scheme::kBp128 && plan.op == AggOp::kSum && rc > 0) {
        Bp128Region parsed;
        if (tr != nullptr && treg_end != nullptr &&
            ParseBp128Region(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          const bool trusted = chunk.HasValidatedBp128Metadata();
          __int128 sum = 0;
          for (uint32_t block_index = 0; block_index < parsed.block_count;
               ++block_index) {
            const Bp128Block& block = parsed.blocks[block_index];
            const uint32_t start = block_index * kBp128BlockRows;
            const uint64_t selected =
                SelNonNullCount(sel, tnulls, start, start + block.rows);
            if (selected == 0) {
              continue;
            }
            if (selected == block.rows && trusted) {
              sum += Bp128BlockSum(block);
              continue;
            }
            std::array<int64_t, kBp128BlockRows> decoded{};
            DecodeBp128Block(parsed, block_index, decoded.data());
            for (uint32_t row = 0; row < block.rows; ++row) {
              const uint32_t global_row = start + row;
              if (((sel.bits[global_row >> 6] >> (global_row & 63U)) & 1ULL) ==
                      0 ||
                  (tnulls != nullptr &&
                   ((static_cast<unsigned>(tnulls[global_row >> 3]) >>
                     (7U - (global_row & 7U))) &
                    1U) != 0)) {
                continue;
              }
              sum += decoded[row];
            }
          }
          if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kFastPfor && plan.op == AggOp::kSum && rc > 0) {
        FastPforRegion parsed;
        if (tr != nullptr && treg_end != nullptr &&
            ParseFastPforRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          const bool trusted = chunk.HasValidatedFastPforMetadata();
          __int128 sum = 0;
          for (uint32_t block_index = 0; block_index < parsed.block_count;
               ++block_index) {
            const FastPforBlock& block = parsed.blocks[block_index];
            const uint32_t start = block_index * kFastPforBlockRows;
            const uint64_t selected =
                SelNonNullCount(sel, tnulls, start, start + block.rows);
            if (selected == 0) {
              continue;
            }
            // The stored brackets cover ALL rows (spikes included), so a
            // fully-selected block's SUM is its metadata word -- the same
            // zero-read shape as the kBp128 arm above.
            if (selected == block.rows && trusted) {
              sum += block.sum;
              continue;
            }
            std::array<int64_t, kFastPforBlockRows> decoded{};
            FastPforDecodeBlock(block, decoded.data());
            for (uint32_t row = 0; row < block.rows; ++row) {
              const uint32_t global_row = start + row;
              if (((sel.bits[global_row >> 6] >> (global_row & 63U)) & 1ULL) ==
                      0 ||
                  (tnulls != nullptr &&
                   ((static_cast<unsigned>(tnulls[global_row >> 3]) >>
                     (7U - (global_row & 7U))) &
                    1U) != 0)) {
                continue;
              }
              sum += decoded[row];
            }
          }
          if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kDeltaFastPfor && plan.op == AggOp::kSum && rc > 0) {
        DeltaFastPforRegion parsed;
        if (tr != nullptr && treg_end != nullptr &&
            ParseDeltaFastPforRegion(tr, rc, f.params[toff], treg_end,
                                     &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          const bool trusted = chunk.HasValidatedDeltaFastPforMetadata();
          __int128 sum = 0;
          for (uint32_t block_index = 0; block_index < parsed.block_count;
               ++block_index) {
            const DeltaFastPforBlock& block = parsed.blocks[block_index];
            const uint32_t start = block_index * kDeltaFastPforBlockRows;
            const uint64_t selected =
                SelNonNullCount(sel, tnulls, start, start + block.rows);
            if (selected == 0) {
              continue;
            }
            // The stored brackets cover ALL rows (spikes included), so a
            // fully-selected block's SUM is its metadata word -- the same
            // zero-read shape as the kFastPfor arm above.
            if (selected == block.rows && trusted) {
              sum += block.sum;
              continue;
            }
            std::array<int64_t, kDeltaFastPforBlockRows> decoded{};
            DeltaFastPforDecodeBlock(block, decoded.data());
            for (uint32_t row = 0; row < block.rows; ++row) {
              const uint32_t global_row = start + row;
              if (((sel.bits[global_row >> 6] >> (global_row & 63U)) & 1ULL) ==
                      0 ||
                  (tnulls != nullptr &&
                   ((static_cast<unsigned>(tnulls[global_row >> 3]) >>
                     (7U - (global_row & 7U))) &
                    1U) != 0)) {
                continue;
              }
              sum += decoded[row];
            }
          }
          if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kDeltaPfor && plan.op == AggOp::kSum && rc > 0) {
        DeltaPForRegion parsed;
        if (tr != nullptr && treg_end != nullptr &&
            ParseDeltaPForRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          const uint64_t selected_nonnull =
              tnulls == nullptr ? sel.count
                                : SelNonNullCount(sel, tnulls, 0, rc);
          __int128 sum = 0;
          for (uint32_t block = 0; block < parsed.blocks; ++block) {
            const uint32_t start = block * kDeltaPForBlockRows;
            const uint32_t rows =
                std::min<uint32_t>(kDeltaPForBlockRows, parsed.n - start);
            const uint64_t selected =
                SelPopcountRange(sel, start, start + rows);
            if (selected == 0) {
              continue;
            }
            if (selected == rows && selected_nonnull == rc) {
              const char* bracket =
                  parsed.brackets +
                  (static_cast<size_t>(block) * kDeltaPForBracketBytes);
              sum += DeltaPForBracketSum(bracket);
              continue;
            }
            std::array<int64_t, kDeltaPForBlockRows> decoded{};
            DecodeDeltaPForBlock(parsed, block, decoded.data());
            for (uint32_t i = 0; i < rows; ++i) {
              const uint32_t row = start + i;
              if (((sel.bits[row >> 6] >> (row & 63U)) & 1ULL) == 0) {
                continue;
              }
              if (tnulls != nullptr &&
                  ((static_cast<unsigned>(tnulls[row >> 3]) >>
                    (7U - (row & 7U))) &
                   1U) != 0) {
                continue;
              }
              sum += decoded[i];
            }
          }
          if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kQpp && plan.op == AggOp::kSum) {
        QppRegion parsed;
        if (tr != nullptr && treg_end != nullptr &&
            ParseQppRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* nulls = RgColNullBits(f, g, plan.col);
          if (nulls == nullptr && sel.count == rc) {
            if (!TryAccumulateScaledSum(plan.coeff, QppRegionSumAll(parsed),
                                        &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
          __int128 sum = 0;
          for (uint32_t i = 0; i < rc; ++i) {
            if (((sel.bits[i >> 6] >> (i & 63U)) & 1ULL) == 0) {
              continue;
            }
            if (nulls != nullptr &&
                ((static_cast<unsigned>(nulls[i >> 3]) >> (7U - (i & 7U))) &
                 1U) != 0) {
              continue;
            }
            sum += QppValueAt(parsed, i);
          }
          if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kArith && plan.op == AggOp::kSum && sel.count == rc &&
          tr != nullptr && treg_end != nullptr && tr + 23 <= treg_end) {
        // Closed-form SUM: S = sum of the model values a + i*d over all rc
        // rows (wrapping u64, exact mod 2^64) minus one (M_p - v) patch per
        // exception -- O(nexc), no per-row decode. Exact ONLY over the full
        // selection: any predicate subset (or a null cell, which decodes as
        // a real 0 rather than "no contribution") falls through to the
        // generic decode.
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        if (SelNonNullCount(sel, tnulls, 0, rc) == rc) {
          const uint64_t a = ReadBE64(tr);
          const uint64_t d = ReadBE64(tr + 8);
          const uint32_t nexc = ReadBE16(tr + 21);
          const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
          if (nexc <= rc && tr + 23 + bmp_bytes <= treg_end) {
            uint64_t s = (a * static_cast<uint64_t>(rc)) +
                         (d * (static_cast<uint64_t>(rc) *
                               (static_cast<uint64_t>(rc) - 1) / 2));
            if (nexc > 0) {
              std::vector<int64_t> exc(nexc, 0);
              DecodeInnerI64All(tr + 23 + bmp_bytes,
                                static_cast<uint8_t>(tr[16]), ReadBE32(tr + 17),
                                nexc, treg_end, exc.data());
              uint32_t eidx = 0;
              for (uint32_t i = 0; i < rc && eidx < nexc; ++i) {
                if (((static_cast<uint8_t>(tr[23 + (i >> 3)]) >>
                      (7 - (i & 7))) &
                     1) != 0) {
                  continue;  // on-line row
                }
                const uint64_t m =
                    a + (static_cast<uint64_t>(i) * d);  // patched-out value
                s += static_cast<uint64_t>(exc[eidx]) - m;
                ++eidx;
              }
            }
            if (!TryAccumulateScaledSum(plan.coeff, static_cast<int64_t>(s),
                                        &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
        }
      }
      if (ts == Scheme::kPla && plan.op == AggOp::kSum && sel.count == rc &&
          tr != nullptr && treg_end != nullptr) {
        // Closed-form SUM over the segment lines: S = sum_j (len_j*first_j +
        // d_j*len_j*(len_j-1)/2) in wrapping u64, then one (v - M) patch per
        // exception -- O(nseg + nexc), no per-row decode. Exact ONLY over
        // the full selection (same contract as the kArith arm).
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        if (SelNonNullCount(sel, tnulls, 0, rc) == rc) {
          PlaRegion pl;
          if (ParsePlaRegion(tr, rc, f.params[toff], treg_end, &pl)) {
            const uint32_t nexc = ReadBE16(pl.hdr + 5);
            std::vector<int64_t> exc(static_cast<size_t>(nexc), 0);
            if (nexc > 0) {
              DecodeInnerI64All(pl.bmp + ((static_cast<size_t>(rc) + 7) / 8),
                                static_cast<uint8_t>(pl.hdr[0]),
                                ReadBE32(pl.hdr + 1), nexc, treg_end,
                                exc.data());
            }
            uint64_t s = 0;
            for (uint32_t j = 0; j < pl.nseg; ++j) {
              const uint32_t seg_start =
                  j == 0 ? 0
                         : ReadBE32(pl.segs +
                                    (static_cast<size_t>(j - 1) * 20) + 16);
              const uint32_t seg_end =
                  ReadBE32(pl.segs + (static_cast<size_t>(j) * 20) + 16);
              const uint64_t len = seg_end - seg_start;
              const uint64_t first =
                  ReadBE64(pl.segs + (static_cast<size_t>(j) * 20));
              const uint64_t d =
                  ReadBE64(pl.segs + (static_cast<size_t>(j) * 20) + 8);
              s += (first * len) + (d * (len * (len - 1) / 2));
            }
            // Patch every exception: replace the model value M_i by v_i.
            if (nexc > 0) {
              uint32_t j = 0;
              uint32_t start = 0;
              uint32_t eidx = 0;
              for (uint32_t i = 0; i < rc && eidx < nexc; ++i) {
                while (i >=
                       ReadBE32(pl.segs + (static_cast<size_t>(j) * 20) + 16)) {
                  start =
                      ReadBE32(pl.segs + (static_cast<size_t>(j) * 20) + 16);
                  ++j;
                }
                if (((pl.bmp[i >> 3] >> (7 - (i & 7))) & 1) != 0) {
                  continue;
                }
                const uint64_t model =
                    ReadBE64(pl.segs + (static_cast<size_t>(j) * 20)) +
                    (static_cast<uint64_t>(i - start) *
                     ReadBE64(pl.segs + (static_cast<size_t>(j) * 20) + 8));
                s += static_cast<uint64_t>(exc[eidx]) - model;
                ++eidx;
              }
            }
            if (!TryAccumulateScaledSum(plan.coeff, static_cast<int64_t>(s),
                                        &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
        }
      }
      if (ts == Scheme::kMod && plan.op == AggOp::kSum && sel.count == rc &&
          tr != nullptr && treg_end != nullptr && tr + 31 <= treg_end) {
        // Closed-form SUM over the wrapped model: sum of the residues
        // (a + b*i) mod m is the un-modded arithmetic sum a*n + b*n(n-1)/2
        // minus m * floor_sum (the Euclidean-descent prefix count, O(log),
        // no per-row decode); one (v - M) patch per exception -- O(nexc).
        // Wrapping u64 is exact mod 2^64 (same contract as the kArith
        // arm), and the full-selection-only rule matches too.
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        if (SelNonNullCount(sel, tnulls, 0, rc) == rc) {
          const uint64_t a = ReadBE64(tr);
          const uint64_t b = ReadBE64(tr + 8);
          const uint64_t m = ReadBE64(tr + 16);
          const auto ischeme = static_cast<uint8_t>(tr[24]);
          const uint32_t iparam = ReadBE32(tr + 25);
          const uint32_t nexc = ReadBE16(tr + 29);
          const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
          if (m >= 2 && m <= (UINT64_C(1) << 63) && a < m && b < m && b != 0 &&
              nexc <= rc && tr + 31 + bmp_bytes <= treg_end &&
              (nexc == 0 || InnerSchemeOk(ischeme))) {
            uint64_t s = (a * static_cast<uint64_t>(rc)) +
                         (b * (static_cast<uint64_t>(rc) *
                               (static_cast<uint64_t>(rc) - 1) / 2)) -
                         (m * static_cast<uint64_t>(ModFloorSum(
                                  static_cast<unsigned __int128>(rc),
                                  static_cast<unsigned __int128>(a),
                                  static_cast<unsigned __int128>(b),
                                  static_cast<unsigned __int128>(m))));
            if (nexc > 0) {
              std::vector<int64_t> exc(nexc, 0);
              DecodeInnerI64All(tr + 31 + bmp_bytes, ischeme, iparam, nexc,
                                treg_end, exc.data());
              uint32_t eidx = 0;
              uint64_t cur = a;
              for (uint32_t i = 0; i < rc && eidx < nexc; ++i) {
                if (((static_cast<uint8_t>(tr[31 + (i >> 3)]) >>
                      (7 - (i & 7))) &
                     1) == 0) {
                  s += static_cast<uint64_t>(exc[eidx]) - cur;
                  ++eidx;
                }
                cur += b;
                if (cur >= m) {
                  cur -= m;
                }
              }
            }
            if (!TryAccumulateScaledSum(plan.coeff, static_cast<int64_t>(s),
                                        &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
        }
      }
      if (ts == Scheme::kLane && plan.op == AggOp::kSum && sel.count == rc &&
          tr != nullptr && treg_end != nullptr && tr + 40 <= treg_end) {
        // Closed-form SUM over the lane grid: each lane contributes
        // cnt*base + step*cnt(cnt-1)/2 (wrapped; exact mod 2^64, the
        // kArith/kMod contract), then one (v - model) patch per exception
        // -- O(nd + nexc), no per-row decode. The full-selection-only rule
        // (a subset has no window here because the selection is in row
        // space) and the no-null rule match the kMod arm.
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        if (SelNonNullCount(sel, tnulls, 0, rc) == rc) {
          const uint32_t nd = static_cast<uint8_t>(tr[0]);
          const size_t hdr = 8 + (static_cast<size_t>(nd) * 16);
          const auto ischeme = static_cast<uint8_t>(tr[hdr - 7]);
          const uint32_t iparam = ReadBE32(tr + hdr - 6);
          const uint32_t nexc = ReadBE16(tr + hdr - 2);
          const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
          if (nd >= 2 && nd <= 16 && nexc == f.params[toff] && nexc <= rc &&
              tr + hdr + bmp_bytes <= treg_end &&
              (nexc == 0 || InnerSchemeOk(ischeme))) {
            uint64_t s = 0;
            for (uint32_t l = 0; l < nd; ++l) {
              const uint64_t base =
                  ReadBE64(tr + 1 + (static_cast<size_t>(l) * 16));
              const uint64_t step =
                  ReadBE64(tr + 1 + (static_cast<size_t>(l) * 16) + 8);
              // Rows of lane l: l, l+nd, ... < rc. The u64 accumulation is
              // the wrapped lane sum (tick counts run 0..cnt-1).
              uint64_t cnt = 0;
              if (rc > l) {
                cnt = ((static_cast<uint64_t>(rc) - 1 - l) / nd) + 1;
              }
              s += (base * cnt) + (step * (cnt * (cnt - 1) / 2));
            }
            if (nexc > 0) {
              std::vector<int64_t> exc(nexc, 0);
              DecodeInnerI64All(tr + hdr + bmp_bytes, ischeme, iparam, nexc,
                                treg_end, exc.data());
              uint32_t eidx = 0;
              uint32_t l = 0;
              uint64_t tick = 0;
              for (uint32_t i = 0; i < rc && eidx < nexc; ++i) {
                if (((static_cast<uint8_t>(tr[hdr + (i >> 3)]) >>
                      (7 - (i & 7))) &
                     1) == 0) {
                  const uint64_t model =
                      ReadBE64(tr + 1 + (static_cast<size_t>(l) * 16)) +
                      (ReadBE64(tr + 1 + (static_cast<size_t>(l) * 16) + 8) *
                       tick);
                  s += static_cast<uint64_t>(exc[eidx]) - model;
                  ++eidx;
                }
                if (++l == nd) {
                  l = 0;
                  ++tick;
                }
              }
            }
            if (!TryAccumulateScaledSum(plan.coeff, static_cast<int64_t>(s),
                                        &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
        }
      }
      if (ts == Scheme::kSortDict && plan.op == AggOp::kSum &&
          sel.count == rc && tr != nullptr && treg_end != nullptr) {
        // Closed-form SUM: the psum table's LAST word is the wrapped total of
        // the sorted view, and the rank plane is a bijection, so the row-order
        // sum equals it -- one load, no decode (the first closed-form SUM for
        // an UNORDERED column family). Exact ONLY over the full selection
        // (same contract as the kArith arm: a subset has no window here
        // because the selection is in row space, the table in sorted space).
        SortDictRegion parsed;
        if (ParseSortDictRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          if (SelNonNullCount(sel, tnulls, 0, rc) == rc) {
            if (!TryAccumulateScaledSum(
                    plan.coeff,
                    static_cast<int64_t>(SortDictPsum(parsed, parsed.nb)),
                    &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
        }
      }
      if (ts == Scheme::kPostings && plan.op == AggOp::kSum &&
          sel.count == rc && tr != nullptr && treg_end != nullptr) {
        // Closed-form SUM: sum(dict[c]*count[c]) over the d lists in
        // wrapping u64 -- the exact histogram IS the aggregate, no row
        // decode, any row order. Full-selection-only, the kSortDict arm's
        // contract (a subset has no window here either).
        PostingsRegion parsed;
        if (ParsePostingsRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          if (SelNonNullCount(sel, tnulls, 0, rc) == rc) {
            uint64_t s = 0;
            for (uint32_t c = 0; c < parsed.d; ++c) {
              s += static_cast<uint64_t>(PostingsDictAt(parsed, c)) *
                   PostingsCountAt(parsed, c);
            }
            if (!TryAccumulateScaledSum(plan.coeff, static_cast<int64_t>(s),
                                        &res.sum)) {
              return ReduceResult{};
            }
            continue;
          }
        }
      }
      if (ts == Scheme::kWaveletR && plan.op == AggOp::kSum &&
          sel.count == rc) {
        WaveletRRegion parsed;
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        if (tr != nullptr && treg_end != nullptr &&
            ParseWaveletRRegion(tr, rc, f.params[toff], treg_end, &parsed) &&
            SelNonNullCount(sel, tnulls, 0, rc) == rc) {
          const uint64_t normalized = WaveletRSumRange(parsed, 0, 0, rc);
          const auto sum = static_cast<int64_t>(
              (static_cast<uint64_t>(parsed.minimum) * rc) + normalized);
          if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kPBitS && plan.op == AggOp::kSum && rc <= 1024) {
        PBitSRegion parsed;
        if (tr != nullptr && treg_end != nullptr &&
            ParsePBitSRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
          const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
          const int64_t sm = PaxChunk::PBitSFilteredSum(
              tr, f.params[toff], rc, treg_end, tnulls, sel.bits.data());
          if (!TryAccumulateScaledSum(plan.coeff, sm, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kRle8 && plan.op == AggOp::kSum && rc <= 1024) {
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        __int128 sm = 0;
        if (Rle8SelectedSum(tr, rc, f.params[toff], treg_end, tnulls,
                            sel.bits.data(), &sm)) {
          if (!TryAccumulateScaledSum(plan.coeff, sm, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kRleDelta && plan.op == AggOp::kSum && rc <= 1024) {
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        __int128 sm = 0;
        if (RleDeltaSelectedSum(tr, rc, f.params[toff], treg_end, tnulls,
                                sel.bits.data(), &sm)) {
          if (!TryAccumulateScaledSum(plan.coeff, sm, &res.sum)) {
            return ReduceResult{};
          }
          continue;
        }
      }
      if (ts == Scheme::kWah && plan.op == AggOp::kSum && rc > 0) {
        // Closed-form SUM over the words: SUM = Σ tv·ones + fv·zeros with
        // ones = popcount(sel & literal-bits) per span and fill spans
        // contributing their pass count in one popcount. No value decode.
        // NULL rows would need the raw-0 contribution rule, so a null
        // bitmap falls through to the generic decode.
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        if (tnulls == nullptr && tr != nullptr && treg_end != nullptr &&
            tr + 16 <= treg_end && f.params[toff] > 0) {
          const auto tv = static_cast<int64_t>(ReadBE64(tr));
          const auto fv = static_cast<int64_t>(ReadBE64(tr + 8));
          const uint32_t words = f.params[toff];
          if (tr + 16 + (static_cast<size_t>(words) * 8) > treg_end) {
            // Hostile extent: fall through to the bounded generic decode.
          } else {
            __int128 sum = 0;
            uint32_t pos = 0;
            const char* wp = tr + 16;
            bool ok = true;
            for (uint32_t k = 0; k < words && pos < rc; ++k, wp += 8) {
              const uint64_t w = ReadBE64(wp);
              if ((w >> 63) == 0) {  // literal span
                const uint32_t span = (rc - pos < 63U) ? (rc - pos) : 63U;
                const uint64_t le = WahLiteralBitsLE(wp);
                uint64_t sel_bits = sel.bits[pos >> 6U] >> (pos & 63U);
                if ((pos & 63U) + span > 64U) {
                  sel_bits |= sel.bits[(pos >> 6U) + 1U] << (64 - (pos & 63U));
                }
                sel_bits &= (span == 64U) ? ~0ULL : ((1ULL << span) - 1ULL);
                const auto cnt =
                    static_cast<uint64_t>(__builtin_popcountll(sel_bits));
                const auto ones =
                    static_cast<uint64_t>(__builtin_popcountll(sel_bits & le));
                sum +=
                    (static_cast<__int128>(tv) * static_cast<__int128>(ones)) +
                    (static_cast<__int128>(fv) *
                     static_cast<__int128>(cnt - ones));
                pos += span;
              } else {  // fill span
                const uint64_t len = w & kWahLenMask;
                if (len == 0 || len > UINT64_MAX / kWahSpan) {
                  ok = false;
                  break;
                }
                uint64_t rows = len * kWahSpan;
                if (rows > static_cast<uint64_t>(rc - pos)) {
                  rows = rc - pos;
                }
                const uint64_t cnt = SelPopcountRange(
                    sel, pos, pos + static_cast<uint32_t>(rows));
                const int64_t v = ((w >> 62) & 1ULL) != 0 ? tv : fv;
                sum += static_cast<__int128>(v) * static_cast<__int128>(cnt);
                pos += static_cast<uint32_t>(rows);
              }
            }
            if (ok) {
              if (!TryAccumulateScaledSum(plan.coeff, sum, &res.sum)) {
                return ReduceResult{};
              }
              continue;
            }
          }
        }
      }
      const bool agg_compressed =
          (ts == Scheme::kConst || ts == Scheme::kRle || ts == Scheme::kRleBp ||
           ts == Scheme::kDict || ts == Scheme::kOpDict ||
           ts == Scheme::kRleDict || ts == Scheme::kOpRle ||
           ts == Scheme::kTopk || ts == Scheme::kHuffDict ||
           ts == Scheme::kTileDict) &&
          (plan.op == AggOp::kSum || plan.op == AggOp::kMin ||
           plan.op == AggOp::kMax) &&
          tr != nullptr && treg_end != nullptr && tr + 8 <= treg_end;
      if (agg_compressed) {
        const uint8_t* tnulls = RgColNullBits(f, g, plan.col);
        auto consider = [&](int64_t v) {
          if (plan.op == AggOp::kMin) {
            if (!minmax_seen || v < res.min_val) {
              res.min_val = v;
            }
          } else {
            if (!minmax_seen || v > res.max_val) {
              res.max_val = v;
            }
          }
          minmax_seen = true;
        };
        if (ts == Scheme::kConst) {
          const auto v = static_cast<int64_t>(ReadBE64(tr));
          if (plan.op == AggOp::kSum) {
            // NULL rows contribute 0, mirroring the generic path's null
            // mask (same contract the MIN/MAX arm and the RLE/DICT arms
            // honor). sel.count alone would credit the constant to null
            // cells: unreachable from the builder -- a null cell profiles
            // as 0 and breaks the all-same gate -- but a hand-built footer
            // with null_present + kConst must not disagree with the
            // generic decode of the identical chunk.
            const uint64_t nn = SelNonNullCount(sel, tnulls, 0, rc);
            if (!TryAccumulateScaledSum(
                    plan.coeff, v * static_cast<__int128>(nn), &res.sum)) {
              return ReduceResult{};
            }
          } else if (SelNonNullCount(sel, tnulls, 0, rc) > 0) {
            consider(v);
          }
          continue;
        }
        if (ts == Scheme::kRle) {
          // Hand-built-footer guard: the run walk is bounded by the footer
          // param; a param beyond the actual region would read into the
          // footer. Skip this group's contributions rather than fold in
          // garbage (the DICT block below must not see an RLE region, so
          // this arm always continues).
          if (treg_end != nullptr &&
              tr + 4 + (static_cast<size_t>(f.params[toff]) * 12) <= treg_end) {
            uint32_t pos = 0;
            const char* q = tr + 4;
            for (uint32_t r = 0; r < f.params[toff] && pos < rc; ++r) {
              const auto v = static_cast<int64_t>(ReadBE64(q));
              // u64 extent math: a hostile len wrapped pos+len in uint32,
              // regressed `pos` and re-folded the same rows into the
              // aggregate.
              const uint64_t next =
                  static_cast<uint64_t>(pos) + ReadBE32(q + 8);
              const uint32_t end =
                  (next > rc) ? rc : static_cast<uint32_t>(next);
              const uint64_t w = SelPopcountRange(sel, pos, end);
              if (w > 0) {
                if (plan.op == AggOp::kSum) {
                  if (!TryAccumulateScaledSum(
                          plan.coeff, v * static_cast<__int128>(w), &res.sum)) {
                    return ReduceResult{};
                  }
                } else if (v != 0 ||
                           SelNonNullCount(sel, tnulls, pos, end) > 0) {
                  consider(v);
                }
              }
              pos = end;
              q += 12;
            }
            // A structurally valid region can still leave rows past the
            // last run (runs sum < rc). The decoders answer those rows as
            // raw 0 (Int64RegionAt kRle falls off the run walk to `return
            // 0`) and TryFastPredicateMask / GroupBySumColumn fold the tail
            // accordingly; MIN/MAX must too, or the compressed kernel
            // disagrees with the generic path on the same chunk.
            if (pos < rc && SelNonNullCount(sel, tnulls, pos, rc) > 0) {
              consider(0);
            }
          }
          continue;
        }
        if (ts == Scheme::kRleBp) {
          RleBpRegion parsed;
          if (ParseRleBpRegion(tr, rc, f.params[toff], treg_end, &parsed)) {
            uint32_t pos = 0;
            for (uint32_t r = 0; r < parsed.runs; ++r) {
              const uint32_t length = RleBpLengthAt(parsed, r);
              const int64_t value = RleBpValueAt(parsed, r);
              const uint32_t end = pos + length;
              const uint64_t selected =
                  plan.op == AggOp::kSum
                      ? SelNonNullCount(sel, tnulls, pos, end)
                      : SelPopcountRange(sel, pos, end);
              if (selected > 0) {
                if (plan.op == AggOp::kSum) {
                  if (!TryAccumulateScaledSum(
                          plan.coeff, value * static_cast<__int128>(selected),
                          &res.sum)) {
                    return ReduceResult{};
                  }
                } else if (value != 0 ||
                           SelNonNullCount(sel, tnulls, pos, end) > 0) {
                  consider(value);
                }
              }
              pos = end;
            }
            continue;
          }
        }
        if (ts == Scheme::kRleDict || ts == Scheme::kOpRle) {
          // Run walk with dictionary dereference: SUM folds as
          // sum(dict[code] * selected-run-weight), MIN/MAX consider each
          // run's dict value once. Extent gates mirror the kRle arm above;
          // clamp codes and rows past the run walk are raw 0. kOpRle shares
          // the byte layout (its sorted dict only matters to predicates).
          if (tr + 2 > treg_end) {
            continue;
          }
          const uint16_t d = ReadBE16(tr);
          if (d == 0) {
            continue;
          }
          const int cb = (BitsNeeded(d - 1) + 7) / 8;
          const char* tdict = tr + 2;
          const char* runs_hdr = tdict + (static_cast<size_t>(d) * 8);
          const uint32_t runs = f.params[toff];
          if (runs_hdr + 4 > treg_end || ReadBE32(runs_hdr) != runs ||
              runs_hdr + 4 +
                      (static_cast<size_t>(runs) *
                       (static_cast<size_t>(cb) + 4)) >
                  treg_end) {
            continue;
          }
          auto dict_at = [&](uint64_t code) {
            return static_cast<int64_t>(
                ReadBE64(tdict + (static_cast<size_t>(code) * 8)));
          };
          uint64_t pos = 0;
          const char* q = runs_hdr + 4;
          for (uint32_t r = 0; r < runs && pos < rc; ++r) {
            uint64_t code = 0;
            for (int b = 0; b < cb; ++b) {
              code = (code << 8) |
                     static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
            }
            const uint64_t len = ReadBE32(q + cb);
            const uint64_t next = pos + len;
            const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
            const int64_t v = (code < d) ? dict_at(code) : 0;
            if (end > pos) {
              const uint64_t w =
                  SelPopcountRange(sel, static_cast<uint32_t>(pos), end);
              if (w > 0) {
                if (plan.op == AggOp::kSum) {
                  if (!TryAccumulateScaledSum(
                          plan.coeff, v * static_cast<__int128>(w), &res.sum)) {
                    return ReduceResult{};
                  }
                } else if (v != 0 || SelNonNullCount(sel, tnulls,
                                                     static_cast<uint32_t>(pos),
                                                     end) > 0) {
                  consider(v);
                }
              }
            }
            pos = end;
            q += static_cast<size_t>(cb) + 4;
          }
          if (pos < rc && SelNonNullCount(sel, tnulls,
                                          static_cast<uint32_t>(pos), rc) > 0) {
            consider(0);
          }
          continue;
        }
        if (ts == Scheme::kHuffDict) {
          // Exact-histogram fold. The stored per-symbol frequencies let the
          // all-selected, null-free cell answer SUM as
          // sum(dict[s]*freq[s]) and MIN/MAX as the first/last dictionary
          // entries -- O(d), zero bitstream reads. A mixed selection or a
          // null present needs per-row selection weights, so it walks the
          // bitstream ONCE for symbols (no per-row value decode: symbols
          // resolve through the dictionary only for codes the selection
          // touched), then reduces that histogram exactly like the DICT arm
          // below.
          HuffDictRegion hp;
          if (ParseHuffDictRegion(tr, rc, f.params[toff], treg_end, &hp)) {
            if (sel.count == rc && SelNonNullCount(sel, tnulls, 0, rc) == rc) {
              if (plan.op == AggOp::kSum) {
                __int128 s = 0;
                for (uint32_t sym = 0; sym < hp.d; ++sym) {
                  s += static_cast<__int128>(HuffDictDictAt(hp, sym)) *
                       static_cast<__int128>(HuffDictFreqAt(hp, sym));
                }
                if (!TryAccumulateScaledSum(plan.coeff, s, &res.sum)) {
                  return ReduceResult{};
                }
              } else {
                consider(HuffDictDictAt(hp, 0));
                consider(HuffDictDictAt(hp, hp.d - 1));
              }
              continue;
            }
            HuffDictTables ht;
            HuffDictBuildTables(hp, &ht);
            WordBitReader br(
                reinterpret_cast<const char*>(hp.stream),
                reinterpret_cast<const char*>(hp.stream + hp.stream_len));
            std::vector<uint64_t> sfreq(hp.d, 0);
            bool zero_nonnull = false;
            for (uint32_t row = 0; row < rc; ++row) {
              const uint32_t sym = HuffDictDecodeSym(ht, br);
              if (sym >= hp.d) {
                continue;  // unreachable on a complete table
              }
              if ((sel.bits[row >> 6] >> (row & 63) & 1ULL) == 0) {
                continue;
              }
              if (tnulls != nullptr &&
                  ((static_cast<unsigned>(tnulls[row >> 3]) >>
                    (7U - (row & 7U))) &
                   1U) != 0) {
                continue;  // NULL cells participate in nothing (generic path)
              }
              ++sfreq[sym];
              if (HuffDictDictAt(hp, sym) == 0) {
                zero_nonnull = true;
              }
            }
            if (plan.op == AggOp::kSum) {
              for (uint32_t sym = 0; sym < hp.d; ++sym) {
                if (sfreq[sym] == 0) {
                  continue;
                }
                if (!TryAccumulateScaledSum(
                        plan.coeff,
                        static_cast<__int128>(HuffDictDictAt(hp, sym)) *
                            static_cast<__int128>(sfreq[sym]),
                        &res.sum)) {
                  return ReduceResult{};
                }
              }
            } else {
              for (uint32_t sym = 0; sym < hp.d; ++sym) {
                if (sfreq[sym] == 0) {
                  continue;
                }
                const int64_t v = HuffDictDictAt(hp, sym);
                if (v != 0) {
                  consider(v);
                }
              }
              if (zero_nonnull) {
                consider(0);
              }
            }
            continue;
          }
          // Unparseable region: fall through to the generic decode below,
          // which fail-closes the same way.
        }
        if (ts == Scheme::kTopk) {
          // Exact-histogram fold over the hot head. The all-selected,
          // null-free cell answers SUM as sum(hot[c]*freq[c]) + one inner
          // pass over the exception tail -- O(15 + nexc), no nibble reads;
          // MIN/MAX need the exceptions too (they live outside the hot set,
          // so hot[0]/hot[d-1] alone can bound nothing). A mixed selection
          // or a null present walks the nibbles once, histogramming codes
          // the selection touched and evaluating exception rows directly.
          constexpr size_t kAggTopkHeader =
              2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;  // 159
          const uint32_t d = ReadBE16(tr);
          const uint32_t nexc = ReadBE16(tr + 2);
          const auto ischeme = static_cast<uint8_t>(tr[154]);
          const uint32_t iparam = ReadBE32(tr + 155);
          const size_t nib_bytes = (static_cast<size_t>(rc) + 1) / 2;
          if (d == 0 || d > 15 || nexc != f.params[toff] ||
              tr + kAggTopkHeader + nib_bytes > treg_end ||
              (nexc > 0 && !InnerSchemeOk(ischeme))) {
            // Header/footer disagreement: unreachable after ValidateWalk
            // (which pins the same invariants), so skip this group's
            // contributions rather than fold garbage (kRle's hostile-param
            // precedent). Falling through would misread the region as DICT.
            continue;
          }
          if (sel.count == rc && SelNonNullCount(sel, tnulls, 0, rc) == rc) {
            if (plan.op == AggOp::kSum) {
              __int128 s = 0;
              for (uint32_t c = 0; c < d; ++c) {
                s += static_cast<__int128>(static_cast<int64_t>(
                         ReadBE64(tr + 4 + (static_cast<size_t>(c) * 8)))) *
                     static_cast<__int128>(
                         ReadBE16(tr + 124 + (static_cast<size_t>(c) * 2)));
              }
              if (nexc > 0) {
                std::vector<int64_t> exc(nexc, 0);
                DecodeInnerI64All(tr + kAggTopkHeader + nib_bytes, ischeme,
                                  iparam, nexc, treg_end, exc.data());
                for (int64_t v : exc) {
                  s += v;
                }
              }
              if (!TryAccumulateScaledSum(plan.coeff, s, &res.sum)) {
                return ReduceResult{};
              }
            } else {
              for (uint32_t c = 0; c < d; ++c) {
                consider(static_cast<int64_t>(
                    ReadBE64(tr + 4 + (static_cast<size_t>(c) * 8))));
              }
              if (nexc > 0) {
                std::vector<int64_t> exc(nexc, 0);
                DecodeInnerI64All(tr + kAggTopkHeader + nib_bytes, ischeme,
                                  iparam, nexc, treg_end, exc.data());
                for (int64_t v : exc) {
                  consider(v);
                }
              }
            }
            continue;
          }
          {
            std::vector<int64_t> exc(nexc, 0);
            if (nexc > 0) {
              DecodeInnerI64All(tr + kAggTopkHeader + nib_bytes, ischeme,
                                iparam, nexc, treg_end, exc.data());
            }
            std::array<uint64_t, 15> sfreq{};
            bool zero_nonnull = false;
            uint32_t eidx = 0;
            for (uint32_t row = 0; row < rc; ++row) {
              const auto byte =
                  static_cast<uint8_t>(tr[kAggTopkHeader + (row >> 1)]);
              const uint32_t code =
                  (row & 1) == 0 ? (byte >> 4) : (byte & 0x0F);
              const bool selected =
                  (sel.bits[row >> 6] >> (row & 63) & 1ULL) != 0;
              const bool is_null = tnulls != nullptr &&
                                   ((static_cast<unsigned>(tnulls[row >> 3]) >>
                                     (7U - (row & 7U))) &
                                    1U) != 0;
              if (code < d) {
                if (selected && !is_null) {
                  ++sfreq[code];
                  if (static_cast<int64_t>(ReadBE64(
                          tr + 4 + (static_cast<size_t>(code) * 8))) == 0) {
                    zero_nonnull = true;
                  }
                }
              } else if (code == 15) {
                const bool count_this = selected && !is_null && eidx < nexc;
                const int64_t v = eidx < nexc ? exc[eidx] : 0;
                ++eidx;
                if (count_this) {
                  if (plan.op == AggOp::kSum) {
                    if (!TryAccumulateScaledSum(plan.coeff, v, &res.sum)) {
                      return ReduceResult{};
                    }
                  } else {
                    if (v != 0) {
                      consider(v);
                    }
                    if (v == 0) {
                      zero_nonnull = true;
                    }
                  }
                }
              }
            }
            if (plan.op == AggOp::kSum) {
              for (uint32_t c = 0; c < d; ++c) {
                if (sfreq[c] == 0) {
                  continue;
                }
                if (!TryAccumulateScaledSum(
                        plan.coeff,
                        static_cast<__int128>(static_cast<int64_t>(
                            ReadBE64(tr + 4 + (static_cast<size_t>(c) * 8)))) *
                            static_cast<__int128>(sfreq[c]),
                        &res.sum)) {
                  return ReduceResult{};
                }
              }
            } else {
              for (uint32_t c = 0; c < d; ++c) {
                if (sfreq[c] == 0) {
                  continue;
                }
                const auto v = static_cast<int64_t>(
                    ReadBE64(tr + 4 + (static_cast<size_t>(c) * 8)));
                if (v != 0) {
                  consider(v);
                }
              }
              if (zero_nonnull) {
                consider(0);
              }
            }
            continue;
          }
        }
        if (ts == Scheme::kTileDict) {
          // Per-tile code histogram (d <= 64, one stack array per tile):
          // SUM folds sum(dict[c] * freq[c]) over occurring codes and
          // MIN/MAX consider each occurring dictionary value once -- no
          // value decode anywhere, the codes ride <= 6-bit extractions. The
          // selection/null gates ride the row walk. Chain or footer
          // disagreement: skip the group rather than fold garbage (the
          // kTopk hostile-region precedent -- falling through would misread
          // the region as DICT).
          TileDictCursor c;
          if (treg_end == nullptr || treg_end <= tr || f.params[toff] != 0 ||
              !TileDictFirst(tr, rc, treg_end, &c)) {
            continue;
          }
          {
            TileDictCursor probe = c;
            uint32_t covered = 0;
            for (;;) {
              covered += probe.rows;
              if (!TileDictNext(&probe)) {
                break;
              }
            }
            if (covered != rc) {
              continue;
            }
          }
          bool zero_nonnull = false;
          for (;;) {
            std::array<uint64_t, kTileDictMaxD> freq{};
            for (uint32_t j = 0; j < c.rows; ++j) {
              const uint32_t row = c.tile_start + j;
              const bool selected =
                  (sel.bits[row >> 6] >> (row & 63) & 1ULL) != 0;
              const bool is_null = tnulls != nullptr &&
                                   ((static_cast<unsigned>(tnulls[row >> 3]) >>
                                     (7U - (row & 7U))) &
                                    1U) != 0;
              if (!selected || is_null) {
                continue;
              }
              const uint64_t code =
                  c.w == 0 ? 0
                           : UnpackBitAt(c.codes,
                                         static_cast<uint64_t>(j) *
                                             static_cast<uint64_t>(c.w),
                                         c.w);
              if (code < c.d) {
                ++freq[static_cast<size_t>(code)];
              } else {
                zero_nonnull = true;  // hostile clamp bucket decodes as 0
              }
            }
            if (plan.op != AggOp::kSum) {
              // Raw-0 participation: 0 may sit in the dictionary itself;
              // its rank bisects to one entry (the MIN/MAX contract counts
              // it as seen only when a selected non-null row carried it).
              uint32_t za = 0;
              uint32_t zb = c.d;
              while (za < zb) {
                const uint32_t zm = za + ((zb - za) / 2);
                if (static_cast<int64_t>(
                        ReadBE64(c.dict + (static_cast<size_t>(zm) * 8))) < 0) {
                  za = zm + 1;
                } else {
                  zb = zm;
                }
              }
              if (za < c.d &&
                  static_cast<int64_t>(
                      ReadBE64(c.dict + (static_cast<size_t>(za) * 8))) == 0 &&
                  freq[za] > 0) {
                zero_nonnull = true;
              }
            }
            if (plan.op == AggOp::kSum) {
              __int128 s = 0;
              for (uint32_t k = 0; k < c.d; ++k) {
                if (freq[k] == 0) {
                  continue;
                }
                s += static_cast<__int128>(static_cast<int64_t>(
                         ReadBE64(c.dict + (static_cast<size_t>(k) * 8)))) *
                     static_cast<__int128>(freq[k]);
              }
              if (!TryAccumulateScaledSum(plan.coeff, s, &res.sum)) {
                return ReduceResult{};
              }
            } else {
              for (uint32_t k = 0; k < c.d; ++k) {
                if (freq[k] == 0) {
                  continue;
                }
                consider(static_cast<int64_t>(
                    ReadBE64(c.dict + (static_cast<size_t>(k) * 8))));
              }
              if (zero_nonnull) {
                consider(0);
              }
            }
            if (!TileDictNext(&c)) {
              break;
            }
          }
          continue;
        }
        // DICT: histogram the selected codes once, aggregate over the
        // dictionary (LowCardinality-style). Codes past ncodes decode to 0.
        const uint16_t ncodes = ReadBE16(tr);
        const size_t dict_base =
            2 + ((static_cast<size_t>(rc) * static_cast<size_t>(BitsNeeded(
                                                ncodes > 0 ? ncodes - 1 : 0)) +
                  7) /
                 8);
        const char* dict = tr + dict_base;
        // The validator floors the region at 4B/entry but an int64 dict
        // consumes 8B/entry: a region built at the floor (or a hand-built
        // footer) would read ncodes*8 bytes past the region into the
        // footer. Fall through to the generic decode path, which clamps
        // every cell of this region to raw 0.
        const bool dict_ok =
            ncodes > 0 && treg_end != nullptr &&
            dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) <= treg_end;
        if (dict_ok) {
          const int cw = BitsNeeded(ncodes - 1);
          // The histogram is a handful of counters for the low-cardinality
          // columns this kernel exists for, and it was a heap allocation per
          // row group per query; keep it on the stack whenever the dictionary
          // is small enough (the DICT validator's own kHuffDict-family ceiling
          // is far above this, so the heap path still covers wide regions).
          std::array<uint64_t, 257> freq_stack{};
          std::vector<uint64_t> freq_heap;
          const size_t nfreq = static_cast<size_t>(ncodes) + 1;
          uint64_t* freq = freq_stack.data();
          if (nfreq > sizeof(freq_stack) / sizeof(freq_stack[0])) {
            freq_heap.assign(nfreq, 0);
            freq = freq_heap.data();
          } else {
            std::memset(freq_stack.data(), 0, nfreq * sizeof(freq_stack[0]));
          }
          // SUM takes the specialised histogram (no lambda, so the strength
          // reduction is uncontested); MIN/MAX keep the generic fold.
          if (plan.op == AggOp::kSum &&
              DictPassByteExpandable(static_cast<uint32_t>(cw))) {
            const uint8_t* codes = reinterpret_cast<const uint8_t*>(tr) + 2;
            switch (8U / static_cast<uint32_t>(cw)) {
              case 8:
                DictSelectedCodeHistogramRun<8>(codes, rc, ncodes,
                                                sel.bits.data(), freq);
                break;
              case 4:
                DictSelectedCodeHistogramRun<4>(codes, rc, ncodes,
                                                sel.bits.data(), freq);
                break;
              default:
                DictSelectedCodeHistogramRun<2>(codes, rc, ncodes,
                                                sel.bits.data(), freq);
                break;
            }
            for (uint16_t d = 0; d < ncodes; ++d) {
              if (freq[d] == 0) {
                continue;
              }
              const auto v =
                  static_cast<int64_t>(ReadBE64(dict + ((size_t)d * 8)));
              if (!TryAccumulateScaledSum(plan.coeff,
                                          v * static_cast<__int128>(freq[d]),
                                          &res.sum)) {
                return ReduceResult{};
              }
            }
            // Codes past the dictionary decode to raw 0 and contribute 0 to a
            // SUM, so the clamp bucket needs no term. COUNT comes from `sel`
            // above, which counted it correctly.
            continue;
          }
          bool zero_nonnull = false;
          // Histogram the selected rows' codes, then reduce that histogram
          // against the dictionary: kMin/kMax see one value per distinct code
          // instead of one per selected row.
          DictSelectedCodeFold(
              tr, rc, static_cast<uint32_t>(cw), ncodes, sel.bits.data(),
              [&](uint32_t row, uint32_t code) {
                ++freq[code];
                if (plan.op == AggOp::kSum || zero_nonnull) {
                  return;
                }
                const int64_t v =
                    code == ncodes
                        ? 0
                        : static_cast<int64_t>(
                              ReadBE64(dict + (static_cast<size_t>(code) * 8)));
                // A selected row only counts as "a real 0 seen" when it is not
                // a NULL cell: the generic path skips nulls, so the compressed
                // fold must too.
                if (v == 0 && (tnulls == nullptr ||
                               ((static_cast<unsigned>(tnulls[row >> 3]) >>
                                 (7U - (row & 7U))) &
                                1U) == 0)) {
                  zero_nonnull = true;
                }
              });
          if (plan.op == AggOp::kSum) {
            for (uint16_t d = 0; d < ncodes; ++d) {
              if (freq[d] == 0) {
                continue;
              }
              const auto v =
                  static_cast<int64_t>(ReadBE64(dict + ((size_t)d * 8)));
              if (!TryAccumulateScaledSum(plan.coeff,
                                          v * static_cast<__int128>(freq[d]),
                                          &res.sum)) {
                return ReduceResult{};
              }
            }
          } else {
            for (uint16_t d = 0; d < ncodes; ++d) {
              if (freq[d] == 0) {
                continue;
              }
              const auto v =
                  static_cast<int64_t>(ReadBE64(dict + ((size_t)d * 8)));
              if (v != 0) {
                consider(v);
              }
            }
            if (zero_nonnull) {
              consider(0);
            }
          }
          continue;
        }  // dict_ok: else fall through to the generic decode below
      }
      // Scratch for the generic fold. This is the LAST resort, reached only
      // for a (scheme, op) pair with no compressed-domain fold, so its cost is
      // the whole cost of such a query -- and a `std::vector<int64_t> vals(rc)`
      // made it one heap allocation PLUS a zero-fill of 8 KB FOR EVERY ROW
      // GROUP, on top of the decode that overwrites all of it. The mask path
      // above already keeps this buffer on the stack for exactly this reason;
      // do the same here, and keep the heap for a hand-built rc that exceeds a
      // physical row group (only reachable from an unvalidated footer).
      // Scratch for the generic fold. This is the LAST resort, reached only
      // for a (scheme, op) pair with no compressed-domain fold, so its cost is
      // the whole cost of such a query -- and a `std::vector<int64_t> vals(rc)`
      // made it one heap allocation PLUS a zero-fill of 8 KB FOR EVERY ROW
      // GROUP, on top of the decode that overwrites all of it. The mask path
      // already keeps this buffer on the stack for exactly this reason; do the
      // same here, and keep the heap for a hand-built rc that exceeds a
      // physical row group (only reachable from an unvalidated footer).
      //
      // `vals_stack` must outlive this block: `vals` is read by the fold loop
      // below, so scoping the array to the `else` arm leaves it dangling.
      std::array<int64_t, PaxChunk::kRowGroupSize> vals_stack{};
      std::vector<int64_t> vals_heap;
      const int64_t* vals = nullptr;
      if (fused_vals != nullptr) {
        vals = fused_vals;  // already decoded for the mask above
      } else {
        vals = vals_stack.data();
        if (rc > PaxChunk::kRowGroupSize) {
          vals_heap.resize(rc);
          vals = vals_heap.data();
        }
        chunk.DecodeInt64ColumnRg(plan.col, g, const_cast<int64_t*>(vals));
      }
      const uint8_t* gnulls = RgColNullBits(f, g, plan.col);
      for (uint64_t w = 0; w < sel.bits.size(); ++w) {
        uint64_t bits = sel.bits[w];
        if (bits == 0) {
          continue;
        }
        const auto base = static_cast<uint16_t>(w * 64);
        while (bits != 0U) {
          const auto b = static_cast<uint16_t>(__builtin_ctzll(bits));
          bits &= bits - 1;
          uint16_t i = base + b;
          if (i >= rc) {
            break;
          }
          // NULL cells decode as raw 0 and would pollute MIN/MAX (column
          // [5, 10, NULL] reported min=0); the metadata fast path above
          // reads the null-aware zone maps, so mask nulls here too.
          if (gnulls != nullptr &&
              ((static_cast<unsigned>(gnulls[i >> 3]) >> (7U - (i & 7U))) &
               1U) != 0) {
            continue;
          }
          const int64_t v = vals[i];
          switch (plan.op) {
            case AggOp::kSum:
              if (!TryAccumulateScaledSum(plan.coeff, v, &res.sum)) {
                return ReduceResult{};
              }
              break;
            case AggOp::kApproxDistinct:
              distinct_seen.insert(v);
              break;
            case AggOp::kMin:
              if (!minmax_seen) {
                res.min_val = v;
                minmax_seen = true;
              } else if (v < res.min_val) {
                res.min_val = v;
              }
              break;
            case AggOp::kMax:
              if (!minmax_seen) {
                res.max_val = v;
                minmax_seen = true;
              } else if (v > res.max_val) {
                res.max_val = v;
              }
              break;
            default:
              break;
          }
        }
      }
    }
  }
  if (plan.op == AggOp::kApproxDistinct) {
    res.approx_distinct = distinct_seen.size();
    res.valid = true;
    return res;
  }
  res.valid = (plan.op != AggOp::kMin && plan.op != AggOp::kMax) || minmax_seen;
  return res;
}

bool FastColumnSum(const PaxChunk& chunk, slot_t col, int64_t* sum_out,
                   uint64_t* count_out) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (!f.rg_sums_present || col >= f.column_count) {
    return false;
  }
  // col indexes col_sum_valid unchecked below: a hand-built footer can carry
  // a shorter array (VectorizedScanColumn checks the size; this path did
  // not and read out of bounds).
  if (col >= f.col_sum_valid.size() || (f.col_sum_valid[col] == 0U)) {
    return false;
  }
  // Hand-built footer guards: row_counts shorter than row_group_count, and
  // the rg_sums index derived from them, must stay in bounds.
  if (f.row_counts.size() < f.row_group_count ||
      f.rg_sums.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count) {
    return false;
  }
  // Unsigned accumulate: adversarial column sums overflow int64 (UB).
  uint64_t s = 0;
  uint64_t c = 0;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const size_t zi = (static_cast<size_t>(g) * f.column_count) + col;
    s += static_cast<uint64_t>(f.rg_sums[zi]);
    c += f.row_counts[g];
  }
  *sum_out = static_cast<int64_t>(s);
  *count_out = c;
  return true;
}

ScanResult VectorizedScanColumn(const PaxChunk& chunk, slot_t col, ScanOp op,
                                int64_t lit, bool parallel, int nthreads,
                                uint32_t scan_sweeps, const Schema* sc,
                                WorkloadTracker* workload) {
  // Clamp scan_sweeps like ReduceColumn: 0 would otherwise fall through both
  // the parallel gate and the sequential sweep loop, silently returning an
  // all-zero ScanResult for a perfectly healthy chunk.
  scan_sweeps = std::max<uint32_t>(scan_sweeps, 1);
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint32_t rg = f.row_group_count;
  const uint16_t cc = f.column_count;
  // Public API: col indexes col_sum_valid / column_offsets / all_zone_mins
  // below — an out-of-range col read out of bounds before any row work.
  if (col >= cc) {
    return ScanResult{};
  }
  // Hand-built-footer guard: the loops index row_counts[g] and the per-cell
  // arrays; declared-but-absent arrays would read out of bounds.
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return ScanResult{};
  }
  // The kernel decodes the region as raw int64. When the caller supplies a
  // schema, enforce the int64 contract: a varchar/double column's region
  // decoded as int64 produced plausible-but-wrong hit counts/sums.
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kInt64)) {
    return ScanResult{};
  }
  if (workload != nullptr && sc != nullptr) {
    workload->RecordFilter(*sc, col);
  }

  int64_t plo = INT64_MIN, phi = INT64_MAX;
  bool range_empty = false;  // predicate is unsatisfiable regardless of data
  switch (op) {
    case ScanOp::kEq:
      plo = phi = lit;
      break;
    case ScanOp::kLe:
      phi = lit;
      break;
    case ScanOp::kLt:
      // v < INT64_MIN matches nothing; avoid signed-overflow UB in lit - 1.
      if (lit == INT64_MIN) {
        range_empty = true;
      } else {
        phi = lit - 1;
      }
      break;
    case ScanOp::kGe:
      plo = lit;
      break;
    case ScanOp::kGt:
      // v > INT64_MAX matches nothing; avoid signed-overflow UB in lit + 1.
      if (lit == INT64_MAX) {
        range_empty = true;
      } else {
        plo = lit + 1;
      }
      break;
    default:
      range_empty = false;  // unknown op: stay conservative (decode each row)
      break;
  }

  auto scan_rg = [&](uint32_t g, ScanResult& r) {
    if (range_empty) {
      return;
    }
    const uint16_t rc = f.row_counts[g];
    const size_t zi = (static_cast<size_t>(g) * cc) + col;
    if (f.all_zm_present && zi < f.all_zone_mins.size() &&
        zi < f.all_zone_maxs.size()) {
      // Zone pruning: skip row groups that cannot contain a match. The
      // zone maps cover NON-null values only, while NULL cells decode as
      // raw 0 — a group with nulls can match through value 0 even when its
      // non-null zone misses the predicate, so pruning must be disabled.
      if (!RgColHasNull(f, g, col) &&
          (f.all_zone_mins[zi] > phi || f.all_zone_maxs[zi] < plo)) {
        return;
      }
      // Fully-inside fast path: fold in precomputed partial sum, no decode.
      // Skipped when this (rg,col) contains nulls (see RgColHasNull).
      // col_sum_valid may be EMPTY on a footer built without the v5 stats
      // section (FastColumnSum guards for this; the scan path used to index
      // it unchecked on the hand-built-footer path).
      if (f.rg_sums_present && col < f.col_sum_valid.size() &&
          f.col_sum_valid[col] && f.rg_sums.size() > zi &&
          !RgColHasNull(f, g, col)) {
        bool fully = false;
        switch (op) {
          case ScanOp::kEq:
            fully = (f.all_zone_mins[zi] == f.all_zone_maxs[zi] &&
                     f.all_zone_mins[zi] == lit);
            break;
          case ScanOp::kLe:
            fully = (f.all_zone_maxs[zi] <= lit);
            break;
          case ScanOp::kLt:
            fully = (f.all_zone_maxs[zi] < lit);
            break;
          case ScanOp::kGe:
            fully = (f.all_zone_mins[zi] >= lit);
            break;
          case ScanOp::kGt:
            fully = (f.all_zone_mins[zi] > lit);
            break;
        }
        if (fully) {
          r.hit_count += rc;
          // Unsigned accumulate: int64 += on adversarial sums is UB.
          r.hit_sum =
              static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                   static_cast<uint64_t>(f.rg_sums[zi]));
          return;
        }
      }
    }
    // Inverted-index EQ path: posting size is the count, value x count the
    // sum. Complete per-cell postings make absence a proven zero (no decode,
    // no hashing). Raw semantics (nulls as 0) match the decode path below.
    if (op == ScanOp::kEq) {
      const InvertedCell* cell = FindInvertedCell(f, g, col);
      if (cell != nullptr) {
        const InvertedPosting* p = FindInvertedPosting(*cell, lit);
        if (p != nullptr) {
          const uint64_t cnt = p->rows.size();
          r.hit_count += cnt;
          // Wrapping multiply: UB-free two's-complement truncation.
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           (static_cast<uint64_t>(lit) * cnt));
        }
        return;
      }
      // Xor8 pre-filter: a definite "absent" skips the whole group with no
      // decode; "maybe" falls through to the paths below.
      if (const XorCell* xc = FindXorCell(f, g, col)) {
        if (!XorCellMayContain(*xc, lit)) {
          return;
        }
      }
    }
    {
      const size_t bp_off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bp_off]) == Scheme::kBp128 &&
          rc > 0) {
        const char* bp_region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* bp_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t count = 0;
        int64_t sum = 0;
        Bp128Region parsed;
        if (bp_region != nullptr && bp_end != nullptr &&
            ParseBp128Region(bp_region, rc, f.params[bp_off], bp_end,
                             &parsed) &&
            (chunk.HasValidatedBp128Metadata() ||
             Bp128MetadataMatches(parsed)) &&
            Bp128RegionCountSumParsed(parsed,
                                      static_cast<PaxChunk::CompareOp>(op), lit,
                                      &count, &sum)) {
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sum));
          return;
        }
      }
    }
    {
      const size_t fp_off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[fp_off]) == Scheme::kFastPfor &&
          rc > 0) {
        const char* fp_region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* fp_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t count = 0;
        int64_t sum = 0;
        FastPforRegion parsed;
        if (fp_region != nullptr && fp_end != nullptr &&
            ParseFastPforRegion(fp_region, rc, f.params[fp_off], fp_end,
                                &parsed) &&
            (chunk.HasValidatedFastPforMetadata() ||
             FastPforMetadataMatches(parsed)) &&
            FastPforRegionCountSumParsed(parsed,
                                         static_cast<PaxChunk::CompareOp>(op),
                                         lit, &count, &sum)) {
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sum));
          return;
        }
      }
    }
    {
      const size_t dfp_off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[dfp_off]) == Scheme::kDeltaFastPfor &&
          rc > 0) {
        const char* dfp_region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* dfp_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t count = 0;
        int64_t sum = 0;
        DeltaFastPforRegion parsed;
        if (dfp_region != nullptr && dfp_end != nullptr &&
            ParseDeltaFastPforRegion(dfp_region, rc, f.params[dfp_off], dfp_end,
                                     &parsed) &&
            (chunk.HasValidatedDeltaFastPforMetadata() ||
             DeltaFastPforMetadataMatches(parsed)) &&
            DeltaFastPforRegionCountSumParsed(
                parsed, static_cast<PaxChunk::CompareOp>(op), lit, &count,
                &sum)) {
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sum));
          return;
        }
      }
    }
    {
      const size_t qpp_off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[qpp_off]) == Scheme::kQpp && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* region_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t count = 0;
        int64_t sum = 0;
        if (region != nullptr && region_end != nullptr &&
            QppRegionCountSum(region, rc, f.params[qpp_off], region_end,
                              static_cast<PaxChunk::CompareOp>(op), lit, &count,
                              &sum)) {
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sum));
          return;
        }
      }
    }
    {
      const size_t dpf_off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[dpf_off]) == Scheme::kDeltaPfor &&
          rc > 0) {
        const char* dpf_region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* dpf_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t count = 0;
        int64_t sum = 0;
        if (dpf_region != nullptr && dpf_end != nullptr &&
            DeltaPForRegionCountSum(dpf_region, rc, f.params[dpf_off], dpf_end,
                                    static_cast<PaxChunk::CompareOp>(op), lit,
                                    &count, &sum)) {
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sum));
          return;
        }
      }
    }
    {
      const size_t pbits_off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[pbits_off]) == Scheme::kPBitS &&
          rc <= 1024) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 11);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        PBitSRegion parsed;
        if (region != nullptr && reg_end != nullptr &&
            ParsePBitSRegion(region, rc, f.params[pbits_off], reg_end,
                             &parsed)) {
          std::array<uint64_t, 16> bits = {0};
          PaxChunk::PBitSCompareMask(region, f.params[pbits_off], rc, reg_end,
                                     static_cast<PaxChunk::CompareOp>(op), lit,
                                     nullptr, bits.data());
          uint64_t count = 0;
          const size_t words = (static_cast<size_t>(rc) + 63) / 64;
          for (size_t i = 0; i < words; ++i) {
            count += static_cast<uint64_t>(__builtin_popcountll(bits[i]));
          }
          const int64_t sm = PaxChunk::PBitSFilteredSum(
              region, f.params[pbits_off], rc, reg_end, nullptr, bits.data());
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sm));
          return;
        }
      }
    }
    {
      const size_t off = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[off]) == Scheme::kWaveletR &&
          rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t count = 0;
        int64_t sum = 0;
        if (region != nullptr && reg_end != nullptr &&
            WaveletRRegionCountSum(region, rc, f.params[off], reg_end,
                                   static_cast<PaxChunk::CompareOp>(op), lit,
                                   &count, &sum)) {
          r.hit_count += count;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sum));
          return;
        }
      }
    }
    // BSI decode-free path: slice-parallel mask + predicated popcount sum.
    // Nulls participate as raw 0 (identical to the decode path below, which
    // reports null cells as 0), so both mask and sum run with null_bmp ==
    // nullptr.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kByteSlice &&
          rc > 0) {
        // Validate the offset VALUE before dereferencing region[0] (the
        // array-size gate at the top only proves the entry exists).
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end > region) {
          const auto w = static_cast<uint8_t>(region[0]);
          const size_t stride = (static_cast<size_t>(rc) + 7) / 8;
          // mbits is sized for kRowGroupSize=1024 rows: ByteSliceCompareMask
          // writes ceil(rc/64) words, so a hand-built rc up to 65535 with a
          // big-enough region overwrote ~3.9KB of stack. Match
          // ByteSliceNullAwareCount's rc <= 1024 gate.
          if (rc <= 1024 && w != 0 && w <= 64 &&
              w == static_cast<uint8_t>(f.params[bso]) &&
              reg_end - region >= static_cast<ptrdiff_t>(
                                      1 + (static_cast<size_t>(w) * stride))) {
            std::array<uint64_t, 16> mbits = {0};
            PaxChunk::ByteSliceCompareMask(region, rc,
                                           static_cast<PaxChunk::CompareOp>(op),
                                           lit, nullptr, mbits.data());
            uint64_t cnt = 0;
            const size_t words = (static_cast<size_t>(rc) + 63) / 64;
            for (size_t k = 0; k < words; k++) {
              cnt += static_cast<uint64_t>(__builtin_popcountll(mbits[k]));
            }
            const int64_t sm = PaxChunk::ByteSliceFilteredSum(
                region, rc, nullptr, mbits.data());
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // OP-DICT decode-free path: the predicate is a rank window over the
    // sorted dictionary; count + filtered sum fold straight off the packed
    // codes (no pass table, no decode). Nulls participate as raw 0 exactly
    // like the decode path below.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kOpDict && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end > region) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (OpDictRegionCountSum(region, rc, f.params[bso], reg_end,
                                   static_cast<PaxChunk::CompareOp>(op), lit,
                                   &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // kPostings decode-free path: the exact count table folds count +
    // filtered sum over the lists the predicate passes -- O(log d + window),
    // zero bitstream reads, no row materialized whatever the row order.
    // Nulls participate as raw 0 exactly like the decode path below.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kPostings &&
          rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end > region) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (PostingsRegionCountSum(region, rc, f.params[bso], reg_end,
                                     static_cast<PaxChunk::CompareOp>(op), lit,
                                     &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // RLE-DICT decode-free path: the predicate pass table is built once per
    // region, then the run stream folds per run -- count and filtered sum
    // in O(runs), no row materialized. Nulls/clamps participate as raw 0
    // exactly like the decode path below.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kRleDict && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end - region >= 2) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (RleDictRegionCountSum(region, rc, f.params[bso], reg_end,
                                    static_cast<PaxChunk::CompareOp>(op), lit,
                                    &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kRle8 && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t cnt = 0;
        int64_t sm = 0;
        if (region != nullptr && reg_end != nullptr && reg_end > region &&
            Rle8RegionCountSum(region, rc, f.params[bso], reg_end,
                               static_cast<PaxChunk::CompareOp>(op), lit, &cnt,
                               &sm)) {
          r.hit_count += cnt;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sm));
          return;
        }
      }
    }
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kRleDelta &&
          rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        uint64_t cnt = 0;
        int64_t sm = 0;
        if (region != nullptr && reg_end != nullptr && reg_end > region &&
            RleDeltaRegionCountSum(region, rc, f.params[bso], reg_end,
                                   static_cast<PaxChunk::CompareOp>(op), lit,
                                   &cnt, &sm)) {
          r.hit_count += cnt;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           static_cast<uint64_t>(sm));
          return;
        }
      }
    }
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kRleBp && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end > region) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (RleBpRegionCountSum(region, rc, f.params[bso], reg_end,
                                  static_cast<PaxChunk::CompareOp>(op), lit,
                                  &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // kRleIdx decode-free path: the sorted run values map the predicate to ONE
    // run window (two binary searches), COUNT is the restart-index difference
    // across it and SUM folds value*length over just that window -- no run
    // outside the window is decoded and no row is materialized.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kRleIdx && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end > region) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (RleIdxRegionCountSum(region, rc, f.params[bso], reg_end,
                                   static_cast<PaxChunk::CompareOp>(op), lit,
                                   &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // OP-RLE decode-free path: the sorted dict maps the predicate to a
    // code window (two binary searches); the run stream folds per run --
    // O(runs), no pass table.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kOpRle && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end - region >= 2) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (OpRleRegionCountSum(region, rc, f.params[bso], reg_end,
                                  static_cast<PaxChunk::CompareOp>(op), lit,
                                  &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // HUFF-DICT decode-free path: the inline frequency table is an exact
    // histogram, so count and filtered sum fold in closed form over the d
    // symbols -- O(d), zero bitstream reads, zero row materialization, and
    // no run/progression structure required. Nulls participate as raw 0
    // exactly like the decode path below.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kHuffDict &&
          rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end - region >= 4) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (HuffDictRegionCountSum(region, rc, f.params[bso], reg_end,
                                     static_cast<PaxChunk::CompareOp>(op), lit,
                                     &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // TOP-K decode-free path: the inline frequency table folds the hot head
    // in closed form over <= 15 entries (zero nibble reads) and the
    // exception tail folds through its inner region -- O(15 + nexc), no row
    // decode, no run/progression structure required. Nulls participate as
    // raw 0 exactly like the decode path.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kTopk && rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr && reg_end - region >= 4) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (TopKRegionCountSum(region, rc, f.params[bso], reg_end,
                                 static_cast<PaxChunk::CompareOp>(op), lit,
                                 &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
    // TILE-DICT decode-free path: the per-tile brackets prune whole tiles
    // (outside tiles cost nothing, inside tiles contribute their row count
    // with zero code reads) and straddling tiles walk <= 6-bit codes. Nulls
    // participate as raw 0 exactly like the decode path.
    {
      const size_t bso = (static_cast<size_t>(g) * cc) + col;
      if (static_cast<Scheme>(f.encodings[bso]) == Scheme::kTileDict &&
          rc > 0) {
        const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
        const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
        if (region != nullptr && reg_end != nullptr) {
          uint64_t cnt = 0;
          int64_t sm = 0;
          if (TileDictRegionCountSum(region, rc, f.params[bso], reg_end,
                                     static_cast<PaxChunk::CompareOp>(op), lit,
                                     &cnt, &sm)) {
            r.hit_count += cnt;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             static_cast<uint64_t>(sm));
            return;
          }
        }
      }
    }
#ifdef __AVX2__
    if (static_cast<Scheme>(f.encodings[zi]) == Scheme::kPlain) {
      const char* plain_region = RegionStartAt(f, g, col, chunk.Data(), 0);
      const char* plain_end = RegionEndAt(f, g, col, chunk.Data());
      const size_t plain_bytes = static_cast<size_t>(rc) * sizeof(int64_t);
      if (plain_region != nullptr && plain_end != nullptr &&
          plain_end >= plain_region &&
          std::cmp_greater_equal(plain_end - plain_region, plain_bytes)) {
        uint64_t cnt = 0;
        uint64_t sm = 0;
        if (ScanPlainCountSumAvx2(plain_region, rc, op, lit, &cnt, &sm)) {
          r.hit_count += cnt;
          r.hit_sum =
              static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
          return;
        }
      }
    }
#endif
    alignas(64) std::array<int64_t, PaxChunk::kRowGroupSize> stack_buf{};
    std::vector<int64_t> heap_buf;
    int64_t* buf = stack_buf.data();
    if (rc > PaxChunk::kRowGroupSize) {
      heap_buf.resize(rc);
      buf = heap_buf.data();
    }
    chunk.DecodeInt64ColumnRg(col, g, buf);
    // Accumulate in uint64: signed int64 += on adversarial data is UB
    // (UBSan traps in the sanitizer build). Wrap-around in the unsigned
    // domain reproduces the documented two's-complement truncation.
    uint64_t cnt = 0;
    uint64_t sm = 0;
#ifdef __AVX2__
    const __m256i vlit = _mm256_set1_epi64x(lit);
    uint32_t i = 0;
    for (; i + 4 <= rc; i += 4) {
      const __m256i v =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buf + i));
      const __m256i m = ScanMatchMask(v, op, vlit);
      cnt += static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned int>(
          _mm256_movemask_pd(_mm256_castsi256_pd(m)))));
      const __m256i vm = _mm256_and_si256(v, m);
      alignas(32) std::array<int64_t, 4> tmp{};
      _mm256_store_si256(reinterpret_cast<__m256i*>(tmp.data()), vm);
      sm += static_cast<uint64_t>(tmp[0]) + static_cast<uint64_t>(tmp[1]) +
            static_cast<uint64_t>(tmp[2]) + static_cast<uint64_t>(tmp[3]);
    }
    for (; i < rc; ++i) {
      if (ScanMatch(buf[i], op, lit)) {
        ++cnt;
        sm += static_cast<uint64_t>(buf[i]);
      }
    }
#else
    for (uint32_t i = 0; i < rc; ++i)
      if (ScanMatch(buf[i], op, lit)) {
        ++cnt;
        sm += static_cast<uint64_t>(buf[i]);
      }
#endif
    r.hit_count += cnt;
    r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
  };

  ScanResult total;
  if (parallel && rg > 1 && scan_sweeps > 0) {
    if (nthreads <= 0) {
      nthreads = static_cast<int>(std::thread::hardware_concurrency());
    }
    nthreads = std::min(nthreads, static_cast<int>(rg));
    nthreads = std::max(nthreads, 1);
    // Persistent pool: threads are spawned ONCE and reused across all sweeps
    // (re-spawning per sweep would dominate the cost for small row groups).
    std::vector<std::thread> ths;
    ths.reserve(static_cast<size_t>(nthreads));
    std::vector<ScanResult> res(static_cast<size_t>(nthreads));
    // DecodeInt64ColumnRg throws std::out_of_range on hostile offsets; an
    // exception escaping a std::thread invocable calls std::terminate, so
    // park it in the worker's OWN slot (a shared exception_ptr written by
    // two simultaneous throwers would itself be a data race) and rethrow
    // on the caller's thread after the join.
    std::vector<std::exception_ptr> worker_errors(
        static_cast<size_t>(nthreads));
    for (int t = 0; t < nthreads; ++t) {
      ths.emplace_back([&, t]() {
        try {
          for (uint32_t sw = 0; sw < scan_sweeps; ++sw) {
            for (auto g = static_cast<uint32_t>(t); g < rg;
                 g += static_cast<uint32_t>(nthreads)) {
              scan_rg(g, res[static_cast<size_t>(t)]);
            }
          }
        } catch (...) {
          worker_errors[static_cast<size_t>(t)] = std::current_exception();
        }
      });
    }
    for (auto& th : ths) {
      th.join();
    }
    for (const auto& ep : worker_errors) {
      if (ep != nullptr) {
        std::rethrow_exception(ep);
      }
    }
    for (auto& r : res) {
      total.hit_count += r.hit_count;
      // uint64 fold: hit_sum wraps by contract (two's-complement), so the
      // cross-thread reduction must not reintroduce signed-overflow UB that
      // the per-row-group accumulation carefully avoids.
      total.hit_sum =
          static_cast<int64_t>(static_cast<uint64_t>(total.hit_sum) +
                               static_cast<uint64_t>(r.hit_sum));
    }
  } else {
    for (uint32_t sw = 0; sw < scan_sweeps; ++sw) {
      for (uint32_t g = 0; g < rg; ++g) {
        scan_rg(g, total);
      }
    }
  }
  return total;
}

ScanResult VectorizedScanColumnInList(const PaxChunk& chunk, slot_t col,
                                      std::span<const int64_t> lits,
                                      bool parallel, int nthreads,
                                      const Schema* sc,
                                      WorkloadTracker* workload) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint32_t rg = f.row_group_count;
  const uint16_t cc = f.column_count;
  // Same public-API gates as VectorizedScanColumn: col indexes the per-cell
  // arrays below; hand-built footers may declare arrays that are absent.
  if (col >= cc) {
    return ScanResult{};
  }
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return ScanResult{};
  }
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kInt64)) {
    return ScanResult{};
  }
  if (workload != nullptr && sc != nullptr) {
    workload->RecordFilter(*sc, col);
  }

  // Sorted unique literal set: dedup gives IN-set semantics (a repeated
  // literal does not double-count a row) and makes membership a binary
  // search.
  std::vector<int64_t> slits(lits.begin(), lits.end());
  std::sort(slits.begin(), slits.end());
  slits.erase(std::unique(slits.begin(), slits.end()), slits.end());
  if (slits.empty()) {
    return ScanResult{};
  }
  const auto member = [&slits](int64_t v) {
    return std::binary_search(slits.begin(), slits.end(), v);
  };
  const bool zero_member = member(0);

  auto scan_rg = [&](uint32_t g, ScanResult& r) {
    const uint16_t rc = f.row_counts[g];
    if (rc == 0) {
      return;
    }
    const size_t zi = (static_cast<size_t>(g) * cc) + col;
    const bool has_null = RgColHasNull(f, g, col);
    if (f.all_zm_present && zi < f.all_zone_mins.size() &&
        zi < f.all_zone_maxs.size()) {
      // A group can match iff some literal falls inside its non-null zone
      // or (when the group holds nulls) 0 is in the set -- null cells
      // decode as raw 0, same as the decode path.
      const auto it =
          std::lower_bound(slits.begin(), slits.end(), f.all_zone_mins[zi]);
      const bool lit_in_zone = it != slits.end() && *it <= f.all_zone_maxs[zi];
      if (!lit_in_zone && !(has_null && zero_member)) {
        return;
      }
    }
    // Inverted-index path: complete postings make absence a proven zero;
    // one posting lookup per literal, no decode (the EQ path generalized).
    const InvertedCell* cell = FindInvertedCell(f, g, col);
    if (cell != nullptr) {
      for (const int64_t lit : slits) {
        if (const InvertedPosting* p = FindInvertedPosting(*cell, lit)) {
          const uint64_t cnt = p->rows.size();
          r.hit_count += cnt;
          r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                           (static_cast<uint64_t>(lit) * cnt));
        }
      }
      return;
    }
    const auto s = static_cast<Scheme>(f.encodings[zi]);
    const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
    const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
    if (s == Scheme::kBp128 && region != nullptr && reg_end != nullptr) {
      bool usable = true;
      uint64_t count = 0;
      uint64_t sum = 0;
      for (int64_t literal : slits) {
        uint64_t matches = 0;
        if (!Bp128RegionCountSum(region, rc, f.params[zi], reg_end,
                                 PaxChunk::CompareOp::kEq, literal, &matches,
                                 nullptr)) {
          usable = false;
          break;
        }
        count += matches;
        sum += static_cast<uint64_t>(literal) * matches;
      }
      if (usable) {
        r.hit_count += count;
        r.hit_sum =
            static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sum);
        return;
      }
    }
    if (s == Scheme::kFastPfor && region != nullptr && reg_end != nullptr) {
      bool usable = true;
      uint64_t count = 0;
      uint64_t sum = 0;
      FastPforRegion parsed;
      if (!ParseFastPforRegion(region, rc, f.params[zi], reg_end, &parsed) ||
          (!chunk.HasValidatedFastPforMetadata() &&
           !FastPforMetadataMatches(parsed))) {
        usable = false;
      } else {
        for (int64_t literal : slits) {
          uint64_t matches = 0;
          if (!FastPforRegionCountSumParsed(parsed, PaxChunk::CompareOp::kEq,
                                            literal, &matches, nullptr)) {
            usable = false;
            break;
          }
          count += matches;
          sum += static_cast<uint64_t>(literal) * matches;
        }
      }
      if (usable) {
        r.hit_count += count;
        r.hit_sum =
            static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sum);
        return;
      }
    }
    if (s == Scheme::kDeltaFastPfor && region != nullptr &&
        reg_end != nullptr) {
      bool usable = true;
      uint64_t count = 0;
      uint64_t sum = 0;
      DeltaFastPforRegion parsed;
      if (!ParseDeltaFastPforRegion(region, rc, f.params[zi], reg_end,
                                    &parsed) ||
          (!chunk.HasValidatedDeltaFastPforMetadata() &&
           !DeltaFastPforMetadataMatches(parsed))) {
        usable = false;
      } else {
        for (int64_t literal : slits) {
          uint64_t matches = 0;
          if (!DeltaFastPforRegionCountSumParsed(parsed,
                                                 PaxChunk::CompareOp::kEq,
                                                 literal, &matches, nullptr)) {
            usable = false;
            break;
          }
          count += matches;
          sum += static_cast<uint64_t>(literal) * matches;
        }
      }
      if (usable) {
        r.hit_count += count;
        r.hit_sum =
            static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sum);
        return;
      }
    }
    if (s == Scheme::kQpp && region != nullptr && reg_end != nullptr) {
      QppRegion parsed;
      if (ParseQppRegion(region, rc, f.params[zi], reg_end, &parsed)) {
        uint64_t cnt = 0;
        uint64_t sm = 0;
        for (uint32_t i = 0; i < rc; ++i) {
          const int64_t value = QppValueAt(parsed, i);
          if (member(value)) {
            ++cnt;
            sm += static_cast<uint64_t>(value);
          }
        }
        r.hit_count += cnt;
        r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
        return;
      }
    }
    // kDict/kOpDict pass table: translate the literal set into a code
    // bitmap once per cell, then scan the packed codes once. Codes >= d
    // decode as raw 0 and share the clamp bucket.
    if ((s == Scheme::kDict || s == Scheme::kOpDict) && region != nullptr &&
        reg_end != nullptr && reg_end - region >= 2) {
      const uint16_t d = ReadBE16(region);
      const int cw = BitsNeeded(d > 0 ? d - 1 : 0);
      const size_t dict_base =
          2 + ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
      const char* dict = region + dict_base;
      if (d != 0 && d <= 1024 && d == f.params[zi] &&
          dict + (static_cast<size_t>(d) * 8) <= reg_end) {
        std::vector<uint8_t> pass(static_cast<size_t>(d) + 1, 0);
        for (uint32_t e = 0; e < d; ++e) {
          pass[e] = member(static_cast<int64_t>(
                        ReadBE64(dict + (static_cast<size_t>(e) * 8))))
                        ? 1
                        : 0;
        }
        pass[d] = zero_member ? 1 : 0;
        WordBitReader br(region + 2, dict);
        uint64_t cnt = 0;
        uint64_t sm = 0;
        for (uint32_t i = 0; i < rc; ++i) {
          const uint64_t code = br.get(cw);
          const uint64_t cb = code < d ? code : d;
          if (pass[cb] == 0) {
            continue;
          }
          ++cnt;
          if (code < d) {
            sm += ReadBE64(dict + (code * sizeof(int64_t)));
          }
        }
        r.hit_count += cnt;
        r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
        return;
      }
    }
    // kRleDict/kOpRle run fold: the pass table is built once per
    // dictionary entry, then each run contributes pass[code] * len without
    // touching rows. (kOpRle's dict is sorted but the pass table is
    // order-agnostic, so both share this arm.)
    if ((s == Scheme::kRleDict || s == Scheme::kOpRle) && region != nullptr &&
        reg_end != nullptr && reg_end - region >= 2) {
      const uint16_t d = ReadBE16(region);
      // The footer param is the RUN count (the validator pins it to the
      // region's runs field), not the dictionary size.
      if (d != 0 && d <= 1024 &&
          region + 2 + (static_cast<size_t>(d) * 8) + 4 <= reg_end) {
        const char* dict = region + 2;
        const char* runs_p = dict + (static_cast<size_t>(d) * 8);
        const uint32_t runs = ReadBE32(runs_p);
        const size_t cb =
            (static_cast<size_t>(BitsNeeded(d - 1)) + 7) / 8;  // 1 or 2
        const char* q = runs_p + 4;
        if (runs == f.params[zi] &&
            q + (static_cast<size_t>(runs) * (cb + 4)) <= reg_end) {
          std::vector<uint8_t> pass(static_cast<size_t>(d) + 1, 0);
          for (uint32_t e = 0; e < d; ++e) {
            pass[e] = member(static_cast<int64_t>(
                          ReadBE64(dict + (static_cast<size_t>(e) * 8))))
                          ? 1
                          : 0;
          }
          pass[d] = zero_member ? 1 : 0;  // clamp / tail bucket
          uint64_t cnt = 0;
          uint64_t sm = 0;
          uint64_t acc = 0;
          for (uint32_t i = 0; i < runs && acc < rc; ++i) {
            uint64_t code = 0;
            for (size_t b = 0; b < cb; ++b) {
              code = (code << 8) |
                     static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
            }
            const uint32_t len = ReadBE32(q + cb);
            const uint64_t next = acc + len;
            const uint32_t take =
                (next > rc) ? rc - static_cast<uint32_t>(acc) : len;
            const uint64_t cb2 = code < d ? code : d;
            if (pass[cb2] != 0) {
              cnt += take;
              if (code < d) {
                sm += ReadBE64(dict + (code * sizeof(int64_t))) * take;
              }
            }
            acc += take;
            q += cb + 4;
          }
          // Tail rows past the run walk decode as raw 0 (kRle parity).
          if (acc < rc && zero_member) {
            cnt += rc - acc;
          }
          r.hit_count += cnt;
          r.hit_sum =
              static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
          return;
        }
      }
    }
    if (s == Scheme::kWaveletR && region != nullptr && reg_end != nullptr) {
      bool usable = true;
      uint64_t count = 0;
      uint64_t sum = 0;
      for (int64_t literal : slits) {
        uint64_t matches = 0;
        if (!WaveletRRegionCountSum(region, rc, f.params[zi], reg_end,
                                    PaxChunk::CompareOp::kEq, literal, &matches,
                                    nullptr)) {
          usable = false;
          break;
        }
        count += matches;
        sum += static_cast<uint64_t>(literal) * matches;
      }
      if (usable) {
        r.hit_count += count;
        r.hit_sum =
            static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sum);
        return;
      }
    }
    if (s == Scheme::kRleBp && region != nullptr && reg_end != nullptr) {
      RleBpRegion parsed;
      if (ParseRleBpRegion(region, rc, f.params[zi], reg_end, &parsed)) {
        for (uint32_t i = 0; i < parsed.runs; ++i) {
          const uint32_t length = RleBpLengthAt(parsed, i);
          const int64_t value = RleBpValueAt(parsed, i);
          if (member(value)) {
            r.hit_count += length;
            r.hit_sum =
                static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                     (static_cast<uint64_t>(value) * length));
          }
        }
        return;
      }
    }
    if (s == Scheme::kDeltaPfor && region != nullptr && reg_end != nullptr) {
      DeltaPForRegion parsed;
      if (ParseDeltaPForRegion(region, rc, f.params[zi], reg_end, &parsed)) {
        uint64_t cnt = 0;
        uint64_t sm = 0;
        for (uint32_t block = 0; block < parsed.blocks; ++block) {
          const char* bracket = parsed.brackets + (static_cast<size_t>(block) *
                                                   kDeltaPForBracketBytes);
          const auto first = static_cast<int64_t>(ReadBE64(bracket));
          const auto last = static_cast<int64_t>(ReadBE64(bracket + 8));
          auto lit_begin = std::lower_bound(slits.begin(), slits.end(), first);
          auto lit_end = std::upper_bound(slits.begin(), slits.end(), last);
          if (lit_begin == lit_end) {
            continue;
          }
          const uint32_t start = block * kDeltaPForBlockRows;
          const uint32_t rows =
              std::min<uint32_t>(kDeltaPForBlockRows, parsed.n - start);
          if (first == last) {
            if (member(first)) {
              cnt += rows;
              sm += static_cast<uint64_t>(first) * rows;
            }
            continue;
          }
          std::array<int64_t, kDeltaPForBlockRows> decoded{};
          DecodeDeltaPForBlock(parsed, block, decoded.data());
          size_t li = static_cast<size_t>(lit_begin - slits.begin());
          const size_t lend = static_cast<size_t>(lit_end - slits.begin());
          for (uint32_t i = 0; i < rows; ++i) {
            while (li < lend && slits[li] < decoded[i]) {
              ++li;
            }
            if (li < lend && slits[li] == decoded[i]) {
              ++cnt;
              sm += static_cast<uint64_t>(decoded[i]);
              ++li;
            }
          }
        }
        r.hit_count += cnt;
        r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
        return;
      }
    }
    // kConst: one membership test covers the whole group.
    if (s == Scheme::kConst && region != nullptr && reg_end != nullptr &&
        reg_end - region >= 8) {
      if (member(static_cast<int64_t>(ReadBE64(region)))) {
        r.hit_count += rc;
        r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                         (ReadBE64(region) * rc));
      }
      return;
    }
    // kRle run fold: one membership test per run.
    if (s == Scheme::kRle && region != nullptr && reg_end != nullptr &&
        reg_end - region >= 4) {
      const uint32_t runs = ReadBE32(region);
      if (runs == f.params[zi] &&
          region + 4 + (static_cast<size_t>(runs) * 12) <= reg_end) {
        const char* q = region + 4;
        uint64_t acc = 0;
        for (uint32_t i = 0; i < runs && acc < rc; ++i) {
          const auto v = static_cast<int64_t>(ReadBE64(q));
          const uint32_t len = ReadBE32(q + 8);
          const uint64_t next = acc + len;
          const uint32_t take =
              (next > rc) ? rc - static_cast<uint32_t>(acc) : len;
          if (member(v)) {
            r.hit_count += take;
            r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) +
                                             (static_cast<uint64_t>(v) * take));
          }
          acc += take;
          q += 12;
        }
        if (acc < rc && zero_member) {
          r.hit_count += rc - acc;
        }
        return;
      }
    }
    // Generic: decode the group once, binary-search the literal set per
    // row. Null cells decode as raw 0, matching every other path.
    std::vector<int64_t> buf(rc);
    chunk.DecodeInt64ColumnRg(col, g, buf.data());
    uint64_t cnt = 0;
    uint64_t sm = 0;
    for (uint32_t i = 0; i < rc; ++i) {
      if (member(buf[i])) {
        ++cnt;
        sm += static_cast<uint64_t>(buf[i]);
      }
    }
    r.hit_count += cnt;
    r.hit_sum = static_cast<int64_t>(static_cast<uint64_t>(r.hit_sum) + sm);
  };

  ScanResult total;
  if (parallel && rg > 1) {
    if (nthreads <= 0) {
      nthreads = static_cast<int>(std::thread::hardware_concurrency());
    }
    nthreads = std::min(nthreads, static_cast<int>(rg));
    nthreads = std::max(nthreads, 1);
    // Same threading contract as VectorizedScanColumn: per-worker result
    // slots and a per-worker exception_ptr (a shared slot written by two
    // simultaneous throwers would itself race).
    std::vector<std::thread> ths;
    ths.reserve(static_cast<size_t>(nthreads));
    std::vector<ScanResult> res(static_cast<size_t>(nthreads));
    std::vector<std::exception_ptr> worker_errors(
        static_cast<size_t>(nthreads));
    for (int t = 0; t < nthreads; ++t) {
      ths.emplace_back([&, t]() {
        try {
          for (auto g = static_cast<uint32_t>(t); g < rg;
               g += static_cast<uint32_t>(nthreads)) {
            scan_rg(g, res[static_cast<size_t>(t)]);
          }
        } catch (...) {
          worker_errors[static_cast<size_t>(t)] = std::current_exception();
        }
      });
    }
    for (auto& th : ths) {
      th.join();
    }
    for (const auto& ep : worker_errors) {
      if (ep != nullptr) {
        std::rethrow_exception(ep);
      }
    }
    for (auto& r : res) {
      total.hit_count += r.hit_count;
      total.hit_sum =
          static_cast<int64_t>(static_cast<uint64_t>(total.hit_sum) +
                               static_cast<uint64_t>(r.hit_sum));
    }
  } else {
    for (uint32_t g = 0; g < rg; ++g) {
      scan_rg(g, total);
    }
  }
  return total;
}

Status PaxChunk::ColumnMinMax(slot_t col_idx, int64_t* min_out,
                              int64_t* max_out) const {
  if (!footer_.all_zm_present || col_idx >= footer_.column_count) {
    return Status::kNotExists;
  }
  // Hand-built footer: row_counts may be shorter than row_group_count.
  if (footer_.row_counts.size() < footer_.row_group_count) {
    return Status::kNotExists;
  }
  int64_t mn = INT64_MAX, mx = INT64_MIN;
  for (uint32_t g = 0; g < footer_.row_group_count; ++g) {
    if (footer_.row_counts[g] == 0) {
      continue;
    }
    const size_t zi = (static_cast<size_t>(g) * footer_.column_count) + col_idx;
    if (zi >= footer_.all_zone_mins.size() ||
        zi >= footer_.all_zone_maxs.size()) {
      return Status::kNotExists;  // incomplete bounds cannot prove the column
    }
    mn = std::min(footer_.all_zone_mins[zi], mn);
    mx = std::max(footer_.all_zone_maxs[zi], mx);
  }
  if (mn > mx) {
    return Status::kNotExists;
  }
  *min_out = mn;
  *max_out = mx;
  return Status::kSuccess;
}

namespace {

inline void SvSetRangeRaw(uint64_t* bits, uint32_t start, uint32_t end) {
  uint32_t i = start;
  if (i < end && ((i & 63) != 0U)) {
    const uint32_t edge = std::min<uint32_t>(end, (i | 63U) + 1U);
    for (; i < edge; ++i) {
      bits[i >> 6] |= (1ULL << (i & 63));
    }
  }
  while (i + 64 <= end) {
    bits[i >> 6] = ~0ULL;
    i += 64;
  }
  for (; i < end; ++i) {
    bits[i >> 6] |= (1ULL << (i & 63));
  }
}

// ---------------------------------------------------------------------------
// Dictionary-family predicate mask: per-BYTE pass-table expansion.
//
// The bit-at-a-time form -- read a cw-bit code, look up pass[code] (with a
// clamp for out-of-range codes), OR one bit into the 64-bit mask word -- is
// the ENTIRE cost of a predicate on a low-cardinality column, and every
// predicated scan and aggregate sits on top of it (BuildSelectionVector ->
// TryFastPredicateMask), so it is the kernel worth optimizing.
//
// A low-cardinality column uses cw = BitsNeeded(distinct-1) in 1..4, so ONE
// INPUT BYTE already carries 8/cw complete codes. Expanding the verdict for a
// whole byte through a 256-entry table therefore produces 2..8 mask bits per
// table lookup instead of one bit per row, which removes three things at once:
//   * three of every four (or seven of eight) bit reads,
//   * the per-row clamp compare -- the table is indexed by the full 2^cw code
//     space, so an out-of-range code resolves to the clamp verdict by
//     construction and the row loop has no branch,
//   * the per-row read-modify-write of the mask word -- the word accumulates
//     in a register and is stored once per 64 rows.
//
// Measured on the low-cardinality shape (4 distinct values, cw = 2, 2M rows):
// 1.6 G rows/s for the bit-at-a-time loop, 4.1 G rows/s here, against 7.2 G
// for the AVX2 PLAIN path -- i.e. the dictionary kernel was 4.5x slower than
// simply decoding 8-byte values and comparing them.
//
// The 256-entry expansion depends only on (cw, pass[]), and a low-cardinality
// column carries the SAME dictionary in every row group -- so the expansion is
// identical across the whole group loop. Rebuilding it per group costs as much
// as the scan it feeds (256 table entries against at most 128 input bytes for
// a 1024-row group), so it is cached. The key is the pass table's CONTENT, not
// any identity, so a stale verdict is impossible even when the caller walks
// different columns, literals or ops; the check is a 16-byte compare. It is
// thread_local for the same reason the encoder scratch buffers are (see
// EncPBitSI64): no locking on a per-scan hot path, no shared mutable state.
inline const uint8_t* DictPassTableCached(uint32_t cw, const uint8_t* pass) {
  struct Cache {
    uint32_t cw{0};
    bool valid{false};
    std::array<uint8_t, 16> pass{};
    std::array<uint8_t, 256> table{};
  };
  thread_local Cache cache;
  if (cache.valid && cache.cw == cw &&
      std::memcmp(cache.pass.data(), pass, 16) == 0) {
    return cache.table.data();
  }
  const uint32_t per_byte = 8U / cw;
  const uint32_t code_mask = (1U << cw) - 1U;
  for (uint32_t byte = 0; byte < 256; ++byte) {
    uint32_t bits = 0;
    for (uint32_t k = 0; k < per_byte; ++k) {
      // The stream is MSB-first, so code k of the byte is the (per_byte-1-k)-th
      // slice from the low end.
      const uint32_t code = (byte >> (8U - cw * (k + 1U))) & code_mask;
      bits |= static_cast<uint32_t>(pass[code]) << k;
    }
    cache.table[byte] = static_cast<uint8_t>(bits);
  }
  cache.cw = cw;
  std::memcpy(cache.pass.data(), pass, 16);
  cache.valid = true;
  return cache.table.data();
}

// `pass` MUST hold 2^cw entries: codes >= the real code count are expected to
// be pre-resolved to the clamp verdict by the caller (exactly what the old
// `pass[ncodes]` bucket did). `out_bits` is OR-ed, so the caller zeroes it.
// Returns false when DictPassByteExpandable(cw) is false.
// Templated on kPerByte (8/cw, so {2,4,8}) so that `i / per_byte` and
// `j / per_byte` COMPILE to shifts. With per_byte as a runtime value the
// compiler cannot strength-reduce them and emits an integer division per code:
// perf put two `divl` in the predicated-SUM inner loop at ~10% of all samples,
// plus the mov/xor setup feeding them, and the same shape was in the mask
// builder. The dispatch below costs one switch per call (per row group) and
// then unrolls the inner loop 8/16/32 ways.
template <uint32_t kPerByte>
void DictPassMaskByteExpandedRun(const uint8_t* src, const uint8_t* table,
                                 uint32_t n, uint64_t* out_bits) {
  // 64 is a multiple of every kPerByte, so each full block starts and ends on
  // a byte boundary of the code stream: no straddling read.
  uint32_t i = 0;
  while (i + 64 <= n) {
    uint64_t word = 0;
    for (uint32_t j = 0; j < 64; j += kPerByte) {
      word |= static_cast<uint64_t>(table[src[(i + j) / kPerByte]]) << j;
    }
    out_bits[i >> 6] |= word;
    i += 64;
  }
  if (i < n) {
    // Tail (< 64 rows): the last byte may be partially used, and it may be
    // the only byte the region really owns, so index it through the same
    // table instead of reading past it.
    uint64_t word = 0;
    for (uint32_t j = 0; j < n - i; ++j) {
      const uint32_t byte = src[(i + j) / kPerByte];
      word |= static_cast<uint64_t>((table[byte] >> (j % kPerByte)) & 1U) << j;
    }
    out_bits[i >> 6] |= word;
  }
}

bool DictPassMaskByteExpanded(const char* codes, uint32_t n, uint32_t cw,
                              const uint8_t* pass, uint64_t* out_bits) {
  if (!DictPassByteExpandable(cw)) {
    return false;
  }
  const uint8_t* table = DictPassTableCached(cw, pass);
  const auto* src = reinterpret_cast<const uint8_t*>(codes);
  switch (8U / cw) {
    case 8:
      DictPassMaskByteExpandedRun<8>(src, table, n, out_bits);
      return true;
    case 4:
      DictPassMaskByteExpandedRun<4>(src, table, n, out_bits);
      return true;
    default:
      DictPassMaskByteExpandedRun<2>(src, table, n, out_bits);
      return true;
  }
}

}  // namespace

uint64_t Rle8MaskRange(const uint64_t* bits, uint32_t start, uint32_t end) {
  uint64_t count = 0;
  while (start < end && (start & 63U) != 0) {
    count += (bits[start >> 6] >> (start & 63U)) & 1ULL;
    ++start;
  }
  while (start + 64U <= end) {
    count += static_cast<uint64_t>(__builtin_popcountll(bits[start >> 6]));
    start += 64U;
  }
  while (start < end) {
    count += (bits[start >> 6] >> (start & 63U)) & 1ULL;
    ++start;
  }
  return count;
}

bool Rle8FillPredicateMask(const Rle8Region& p, PaxChunk::CompareOp op,
                           int64_t literal, const uint8_t* null_bmp,
                           uint64_t* out_bits) {
  if (out_bits == nullptr || p.n == 0 || p.n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const size_t words = (static_cast<size_t>(p.n) + 63U) / 64U;
  std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> valid{};
  std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> less{};
  std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> equal{};
  std::fill(valid.begin(), valid.begin() + words, ~0ULL);
  if ((p.n & 63U) != 0) {
    valid[(p.n - 1U) >> 6U] = (1ULL << (p.n & 63U)) - 1ULL;
  }
  equal = valid;

  for (uint32_t plane = 0; plane < 8; ++plane) {
    const uint32_t first = p.plane_offsets[plane];
    const uint32_t count = p.plane_runs[plane];
    const auto plane_literal =
        static_cast<uint8_t>(((static_cast<uint64_t>(literal) ^ (1ULL << 63)) >>
                              (56U - 8U * plane)) &
                             0xFFU);
    std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> plane_less{};
    std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> plane_equal{};
    for (uint32_t run = 0; run < count; ++run) {
      const Rle8Run& item = p.runs[first + run];
      if (item.value < plane_literal) {
        SvSetRangeRaw(plane_less.data(), item.start, item.start + item.length);
      } else if (item.value == plane_literal) {
        SvSetRangeRaw(plane_equal.data(), item.start, item.start + item.length);
      }
    }
    for (size_t word = 0; word < words; ++word) {
      const uint64_t prior_equal = equal[word];
      less[word] |= prior_equal & plane_less[word];
      equal[word] = prior_equal & plane_equal[word];
    }
  }

  std::fill_n(out_bits, words, 0ULL);
  for (size_t word = 0; word < words; ++word) {
    uint64_t result = 0;
    switch (op) {
      case PaxChunk::CompareOp::kEq:
        result = equal[word];
        break;
      case PaxChunk::CompareOp::kLe:
        result = less[word] | equal[word];
        break;
      case PaxChunk::CompareOp::kLt:
        result = less[word];
        break;
      case PaxChunk::CompareOp::kGe:
        result = valid[word] & ~less[word];
        break;
      case PaxChunk::CompareOp::kGt:
        result = valid[word] & ~(less[word] | equal[word]);
        break;
      default:
        return false;
    }
    out_bits[word] = result;
  }

  if (null_bmp != nullptr) {
    const size_t bytes = (static_cast<size_t>(p.n) + 7U) / 8U;
    for (size_t byte = 0; byte < bytes; ++byte) {
      const auto shift = static_cast<unsigned>(8U * (byte & 7U));
      out_bits[byte >> 3U] &=
          ~(static_cast<uint64_t>(Rle8ReverseByte(null_bmp[byte])) << shift);
    }
  }
  return true;
}

bool Rle8SumMaskWide(const Rle8Region& p, const uint64_t* bits,
                     __int128* sum_out) {
  if (bits == nullptr || sum_out == nullptr) {
    return false;
  }
  __int128 ordered_sum = 0;
  const uint64_t count = Rle8MaskRange(bits, 0, p.n);
  for (uint32_t plane = 0; plane < 8; ++plane) {
    const uint32_t first = p.plane_offsets[plane];
    const uint32_t runs = p.plane_runs[plane];
    for (uint32_t run = 0; run < runs; ++run) {
      const Rle8Run& item = p.runs[first + run];
      const uint64_t selected =
          Rle8MaskRange(bits, item.start, item.start + item.length);
      if (selected == 0) {
        continue;
      }
      ordered_sum += static_cast<__int128>(selected) *
                     (static_cast<uint64_t>(item.value) << (56U - 8U * plane));
    }
  }
  ordered_sum -= static_cast<__int128>(count) << 63;
  *sum_out = ordered_sum;
  return true;
}

bool Rle8SumMask(const Rle8Region& p, const uint64_t* bits, int64_t* sum_out) {
  if (sum_out == nullptr) {
    return false;
  }
  __int128 wide = 0;
  if (!Rle8SumMaskWide(p, bits, &wide)) {
    return false;
  }
  *sum_out = static_cast<int64_t>(wide);
  return true;
}

namespace {

bool Rle8CompareMask(const char* region, uint32_t n, uint32_t param,
                     const char* region_end, PaxChunk::CompareOp op,
                     int64_t literal, const uint8_t* null_bmp,
                     uint64_t* out_bits) {
  Rle8Region parsed;
  if (!ParseRle8Region(region, n, param, region_end, &parsed)) {
    return false;
  }
  return Rle8FillPredicateMask(parsed, op, literal, null_bmp, out_bits);
}

bool RleDeltaCompareMask(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t literal, uint64_t* out_bits) {
  if (out_bits == nullptr) {
    return false;
  }
  RleDeltaRegion parsed;
  if (!ParseRleDeltaRegion(region, n, param, region_end, &parsed)) {
    return false;
  }
  for (uint32_t run = 0; run < parsed.run_count; ++run) {
    const RleDeltaRun& item = parsed.runs[run];
    if (RleDictMatch(op, item.value, literal)) {
      SvSetRangeRaw(out_bits, item.start, item.start + item.length);
    }
  }
  return true;
}

}  // namespace

bool Rle8SelectedSum(const char* region, uint32_t n, uint32_t param,
                     const char* region_end, const uint8_t* null_bmp,
                     const uint64_t* selected, __int128* sum_out) {
  Rle8Region parsed;
  if (!ParseRle8Region(region, n, param, region_end, &parsed)) {
    return false;
  }
  const size_t words = (static_cast<size_t>(n) + 63U) / 64U;
  const size_t bytes = (static_cast<size_t>(n) + 7U) / 8U;
  std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> active{};
  for (size_t word = 0; word < words; ++word) {
    const auto start = static_cast<uint32_t>(word * 64U);
    const uint32_t rows = std::min<uint32_t>(64U, n - start);
    uint64_t bits = rows == 64U ? ~0ULL : (1ULL << rows) - 1ULL;
    if (selected != nullptr) {
      bits &= selected[word];
    }
    if (null_bmp != nullptr) {
      const size_t first_byte = word * 8U;
      for (size_t byte = first_byte; byte < std::min(bytes, first_byte + 8U);
           ++byte) {
        const auto shift = static_cast<unsigned>(8U * (byte & 7U));
        bits &=
            ~(static_cast<uint64_t>(Rle8ReverseByte(null_bmp[byte])) << shift);
      }
    }
    active[word] = bits;
  }
  return Rle8SumMaskWide(parsed, active.data(), sum_out);
}

bool RleDeltaSelectedSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, const uint8_t* null_bmp,
                         const uint64_t* selected, __int128* sum_out) {
  if (sum_out == nullptr || n == 0 || n > PaxChunk::kRowGroupSize) {
    return false;
  }
  RleDeltaRegion parsed;
  if (!ParseRleDeltaRegion(region, n, param, region_end, &parsed)) {
    return false;
  }
  bool all_selected = null_bmp == nullptr;
  const size_t words = (static_cast<size_t>(n) + 63U) / 64U;
  if (selected != nullptr) {
    all_selected = true;
    for (size_t word = 0; word < words; ++word) {
      const auto start = static_cast<uint32_t>(word * 64U);
      const uint32_t rows = std::min<uint32_t>(64U, n - start);
      const uint64_t valid = rows == 64U ? ~0ULL : (1ULL << rows) - 1ULL;
      if (selected[word] != valid) {
        all_selected = false;
        break;
      }
    }
  }
  if (all_selected) {
    __int128 total = 0;
    for (uint32_t run = 0; run < parsed.run_count; ++run) {
      const RleDeltaRun& item = parsed.runs[run];
      total += static_cast<__int128>(item.value) * item.length;
    }
    *sum_out = total;
    return true;
  }
  const size_t bytes = (static_cast<size_t>(n) + 7U) / 8U;
  std::array<uint64_t, PaxChunk::kRowGroupSize / 64U> active{};
  for (size_t word = 0; word < words; ++word) {
    const auto start = static_cast<uint32_t>(word * 64U);
    const uint32_t rows = std::min<uint32_t>(64U, n - start);
    uint64_t bits = rows == 64U ? ~0ULL : (1ULL << rows) - 1ULL;
    if (selected != nullptr) {
      bits &= selected[word];
    }
    if (null_bmp != nullptr) {
      const size_t first_byte = word * 8U;
      for (size_t byte = first_byte; byte < std::min(bytes, first_byte + 8U);
           ++byte) {
        const auto shift = static_cast<unsigned>(8U * (byte & 7U));
        bits &=
            ~(static_cast<uint64_t>(Rle8ReverseByte(null_bmp[byte])) << shift);
      }
    }
    active[word] = bits;
  }
  __int128 sum = 0;
  for (uint32_t run = 0; run < parsed.run_count; ++run) {
    const RleDeltaRun& item = parsed.runs[run];
    const uint64_t selected_count =
        Rle8MaskRange(active.data(), item.start, item.start + item.length);
    if (selected_count != 0) {
      sum += static_cast<__int128>(item.value) * selected_count;
    }
  }
  *sum_out = sum;
  return true;
}

// Compressed-execution predicate masking (technique: query-on-compressed).
// For low-cardinality regions (CONST/RLE/DICT int64) the predicate is
// evaluated ONCE PER DISTINCT VALUE, and rows are marked directly, without
// materializing an int64 value per row. NULL cells are stored as raw 0 in the
// region (see RgColHasNull), so the run/dict value equals what the generic
// DecodeInt64ColumnRg path would compare, keeping results identical. Returns
// false for schemes without a compressed kernel (caller decodes normally).
namespace {
bool TryFastPredicateMask(const PaxChunk& chunk, const Predicate& p, uint32_t g,
                          uint64_t* out) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint16_t rc = f.row_counts[g];
  const uint32_t off_idx = (g * f.column_count) + p.col;
  const auto s = static_cast<Scheme>(f.encodings[off_idx]);
  // Validate the offset VALUE before any dereference (including the header
  // reads below): the array-size gate in the caller only proves the entry
  // exists, and a hostile value must not form a wild pointer.
  const char* region = RegionStartAt(f, g, p.col, chunk.Data(), 0);
  if (region == nullptr) {
    return false;
  }
#ifdef __AVX2__
  if (s == Scheme::kPlain) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end != nullptr && reg_end >= region &&
        static_cast<size_t>(reg_end - region) >=
            static_cast<size_t>(rc) * sizeof(int64_t)) {
      if (PlainPredicateMask::FillPlainPredicateMaskAvx2(region, rc, p.op,
                                                         p.lit, out)) {
        return true;
      }
    }
  }
#endif
  if (s == Scheme::kConst) {
    // Unlike the kRle/kDict/kByteSlice paths there is no later extent check
    // here: bound the 8-byte constant read directly.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 8 > reg_end) {
      return false;
    }
    const auto v = static_cast<int64_t>(ReadBE64(region));
    if (ScanMatch(v, p.op, p.lit)) {
      SvSetRangeRaw(out, 0, rc);
    }
    return true;
  }
  if (s == Scheme::kBitmap) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    const size_t bitmap_bytes = (static_cast<size_t>(rc) + 7U) / 8U;
    if (reg_end == nullptr || reg_end < region ||
        static_cast<size_t>(reg_end - region) < 16U + bitmap_bytes) {
      return false;
    }
    const auto true_value = static_cast<int64_t>(ReadBE64(region));
    const auto false_value = static_cast<int64_t>(ReadBE64(region + 8));
    const bool true_passes = ScanMatch(true_value, p.op, p.lit);
    const bool false_passes = ScanMatch(false_value, p.op, p.lit);
    if (true_passes == false_passes) {
      if (true_passes) {
        SvSetRangeRaw(out, 0, rc);
      }
      return true;
    }
    const auto* bitmap = reinterpret_cast<const unsigned char*>(region + 16);
    uint32_t row = 0;
    for (; row + 64U <= rc; row += 64U) {
      uint64_t word = 0;
      for (uint32_t byte = 0; byte < 8U; ++byte) {
        word |=
            static_cast<uint64_t>(Rle8ReverseByte(bitmap[(row >> 3U) + byte]))
            << (byte * 8U);
      }
      out[row >> 6U] |= true_passes ? word : ~word;
    }
    if (row < rc) {
      const uint32_t remaining = rc - row;
      const uint32_t bytes = (remaining + 7U) / 8U;
      uint64_t word = 0;
      for (uint32_t byte = 0; byte < bytes; ++byte) {
        word |=
            static_cast<uint64_t>(Rle8ReverseByte(bitmap[(row >> 3U) + byte]))
            << (byte * 8U);
      }
      const uint64_t valid = (1ULL << remaining) - 1ULL;
      out[row >> 6U] |= (true_passes ? word : ~word) & valid;
    }
    return true;
  }
  if (s == Scheme::kWah) {
    // WAH IS the selection bitmap in run form: literal words are 63 mask
    // bits each (one bit-reverse + shift), fills are range sets. No value
    // decode, no per-row pass table.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    const uint32_t words = f.params[off_idx];
    if (words == 0 || reg_end == nullptr || reg_end < region ||
        static_cast<size_t>(reg_end - region) <
            16U + (static_cast<size_t>(words) * 8U)) {
      return false;
    }
    const auto tv = static_cast<int64_t>(ReadBE64(region));
    const auto fv = static_cast<int64_t>(ReadBE64(region + 8));
    const bool true_passes = ScanMatch(tv, p.op, p.lit);
    const bool false_passes = ScanMatch(fv, p.op, p.lit);
    if (true_passes == false_passes) {
      if (true_passes) {
        SvSetRangeRaw(out, 0, rc);
      }
      return true;
    }
    uint32_t row = 0;
    const char* wp = region + 16;
    for (uint32_t k = 0; k < words && row < rc; ++k, wp += 8) {
      const uint64_t w = ReadBE64(wp);
      if ((w >> 63) == 0) {  // literal span: 63 mask bits, MSB-first
        const uint64_t le = WahLiteralBitsLE(wp);
        const uint32_t span = (rc - row < 63U) ? (rc - row) : 63U;
        const uint64_t spanmask = span == 64U ? ~0ULL : ((1ULL << span) - 1ULL);
        const uint64_t m = true_passes ? (le & spanmask) : (~le & spanmask);
        const uint32_t widx = row >> 6U;
        const uint32_t bit_off = row & 63U;
        out[widx] |= m << bit_off;
        if (bit_off + span > 64U) {
          out[widx + 1] |= m >> (64 - bit_off);
        }
        row += span;
      } else {  // fill span
        const uint64_t len = w & kWahLenMask;
        if (len == 0 || len > UINT64_MAX / kWahSpan) {
          return false;
        }
        uint64_t rows = len * kWahSpan;
        if (rows > static_cast<uint64_t>(rc - row)) {
          rows = rc - row;
        }
        // The fill carries the "true" (max) value when its bit is set,
        // the "false" value otherwise; exactly one of them passes here.
        const bool fill_passes =
            ((w >> 62) & 1ULL) != 0 ? true_passes : false_passes;
        if (fill_passes) {
          SvSetRangeRaw(out, row, row + static_cast<uint32_t>(rows));
        }
        row += static_cast<uint32_t>(rows);
      }
    }
    // Uncovered tail rows decode as raw 0 (the documented zero-participation
    // semantics); mask them the same way the kRle arm does.
    if (row < rc && ScanMatch(static_cast<int64_t>(0), p.op, p.lit)) {
      SvSetRangeRaw(out, row, rc);
    }
    return true;
  }
  if (s == Scheme::kRle) {
    const uint32_t runs = f.params[off_idx];
    // The run walk is bounded by the footer param; a hand-built footer with
    // runs beyond the actual region would read into the footer. Decline the
    // fast path so the caller decodes via the bounds-checked generic path.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr ||
        region + 4 + (static_cast<size_t>(runs) * 12) > reg_end) {
      return false;
    }
    uint32_t idx = 0;
    const char* q = region + 4;  // skip runs header
    for (uint32_t r = 0; r < runs && idx < rc; ++r) {
      const auto v = static_cast<int64_t>(ReadBE64(q));
      const uint32_t len = ReadBE32(q + 8);
      // u64 extent math: a hostile len wrapped idx+len in uint32 and set
      // mask bits at wrong row ranges.
      const uint64_t next = static_cast<uint64_t>(idx) + len;
      if (ScanMatch(v, p.op, p.lit)) {
        SvSetRangeRaw(out, idx, (next > rc) ? rc : static_cast<uint32_t>(next));
      }
      idx = (next > rc) ? rc : static_cast<uint32_t>(next);
      q += 12;
    }
    // A truncated region covers fewer than rc rows; Int64RegionAt decodes
    // those tail rows as raw 0 (the documented zero-participation
    // semantics). Mask them the same way instead of silently leaving them
    // unselected -- an `== 0` predicate missed every tail row here.
    if (idx < rc && ScanMatch(static_cast<int64_t>(0), p.op, p.lit)) {
      SvSetRangeRaw(out, idx, rc);
    }
    return true;
  }
  if (s == Scheme::kRleBp) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    RleBpRegion parsed;
    if (reg_end == nullptr ||
        !ParseRleBpRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    uint32_t pos = 0;
    for (uint32_t run = 0; run < parsed.runs; ++run) {
      const uint32_t length = RleBpLengthAt(parsed, run);
      if (ScanMatch(RleBpValueAt(parsed, run), p.op, p.lit)) {
        SvSetRangeRaw(out, pos, pos + length);
      }
      pos += length;
    }
    return true;
  }
  if (s == Scheme::kRleIdx) {
    // Run-window mask: the sorted run values make the passing runs one
    // contiguous window whose endpoints are two bisections over the run
    // values, and the window becomes a single SvSetRange -- the same per-run
    // shape as kRleBp, but out-of-window runs are never visited at all.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    RleIdxRegion parsed;
    if (reg_end == nullptr ||
        !ParseRleIdxRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    uint32_t first = 0;
    uint32_t last = 0;
    if (!RleIdxPassingWindow(parsed, static_cast<PaxChunk::CompareOp>(p.op),
                             p.lit, &first, &last)) {
      return false;
    }
    if (first >= last) {
      return true;  // empty window: nothing selected
    }
    const uint32_t row_first = RleIdxRowBefore(parsed, first);
    const uint32_t row_last = RleIdxRowBefore(parsed, last);
    if (row_last > row_first) {
      SvSetRangeRaw(out, row_first, row_last);
    }
    return true;
  }
  if (s == Scheme::kRle8) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    return Rle8CompareMask(region, rc, f.params[off_idx], reg_end,
                           static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                           nullptr, out);
  }
  if (s == Scheme::kRleDelta) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    return RleDeltaCompareMask(region, rc, f.params[off_idx], reg_end,
                               static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                               out);
  }
  if (s == Scheme::kSimple8b) {
    // Word-adaptive folding: each word's selector fixes its value range, so
    // words that cannot match are skipped untouched, zero-width words
    // (240/120 copies of the base) become one range set, and only words that
    // straddle the literal decode value-by-value.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    const uint32_t words = f.params[off_idx];
    if (words == 0 || reg_end == nullptr || reg_end < region ||
        static_cast<size_t>(reg_end - region) <
            8U + (static_cast<size_t>(words) * 8U)) {
      return false;
    }
    const uint64_t base_u = ReadBE64(region);
    uint32_t row = 0;
    const char* wp = region + 8;
    for (uint32_t k = 0; k < words && row < rc; ++k, wp += 8) {
      const uint64_t word = ReadBE64(wp);
      const auto sel = static_cast<uint32_t>(word >> 60);
      const uint32_t cnt = kSimple8bSelCount[sel];
      const uint32_t bits = kSimple8bSelBits[sel];
      const uint32_t avail = rc - row;
      const uint32_t take = cnt < avail ? cnt : avail;
      switch (Simple8bWordVerdict(bits, base_u, p.lit, p.op)) {
        case Simple8bVerdict::kNone:
          break;
        case Simple8bVerdict::kAll:
          SvSetRangeRaw(out, row, row + take);
          break;
        default: {
          const uint64_t mask = bits == 0 ? 0 : ((UINT64_C(1) << bits) - 1);
          for (uint32_t j = 0; j < take; ++j) {
            const uint64_t off = (word >> (bits * (cnt - 1 - j))) & mask;
            if (Simple8bPasses(static_cast<int64_t>(base_u + off), p.op,
                               p.lit)) {
              out[(row + j) >> 6] |= UINT64_C(1) << ((row + j) & 63);
            }
          }
          break;
        }
      }
      row += take;
    }
    // Truncated-region tail decodes as raw 0 (zero-participation semantics).
    if (row < rc && Simple8bPasses(static_cast<int64_t>(0), p.op, p.lit)) {
      SvSetRangeRaw(out, row, rc);
    }
    return true;
  }
  if (s == Scheme::kBp128) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    Bp128Region parsed;
    if (reg_end == nullptr ||
        !ParseBp128Region(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    if (!chunk.HasValidatedBp128Metadata() && !Bp128MetadataMatches(parsed)) {
      return false;
    }
    for (uint32_t block_index = 0; block_index < parsed.block_count;
         ++block_index) {
      const Bp128Block& block = parsed.blocks[block_index];
      const uint32_t start = block_index * kBp128BlockRows;
      if (p.op == ScanOp::kEq && block.minimum == block.maximum) {
        if (block.minimum == p.lit) {
          SvSetRangeRaw(out, start, start + block.rows);
        }
        continue;
      }
      if (p.op != ScanOp::kEq) {
        const bool minimum_hit = ScanMatch(block.minimum, p.op, p.lit);
        const bool maximum_hit = ScanMatch(block.maximum, p.op, p.lit);
        if (!minimum_hit && !maximum_hit) {
          continue;
        }
        if (minimum_hit && maximum_hit) {
          SvSetRangeRaw(out, start, start + block.rows);
          continue;
        }
      } else if (p.lit < block.minimum || p.lit > block.maximum) {
        continue;
      }
      std::array<int64_t, kBp128BlockRows> decoded{};
      DecodeBp128Block(parsed, block_index, decoded.data());
      for (uint32_t row = 0; row < block.rows; ++row) {
        if (ScanMatch(decoded[row], p.op, p.lit)) {
          out[(start + row) >> 6] |= 1ULL << ((start + row) & 63U);
        }
      }
    }
    return true;
  }
  if (s == Scheme::kFastPfor) {
    // The kBp128 bracket fold with the exception dimension already folded in:
    // the stored brackets cover ALL rows (the encoder takes min/max over the
    // block's full values, spikes included), and FastPforDecodeBlock patches
    // exceptions before the compare, so a boundary block is one decode plus a
    // plain per-row match -- no bitmap-aware correction is ever needed here.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    FastPforRegion parsed;
    if (reg_end == nullptr ||
        !ParseFastPforRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    if (!chunk.HasValidatedFastPforMetadata() &&
        !FastPforMetadataMatches(parsed)) {
      return false;
    }
    for (uint32_t block_index = 0; block_index < parsed.block_count;
         ++block_index) {
      const FastPforBlock& block = parsed.blocks[block_index];
      const uint32_t start = block_index * kFastPforBlockRows;
      if (p.op == ScanOp::kEq && block.minimum == block.maximum) {
        if (block.minimum == p.lit) {
          SvSetRangeRaw(out, start, start + block.rows);
        }
        continue;
      }
      if (p.op != ScanOp::kEq) {
        const bool minimum_hit = ScanMatch(block.minimum, p.op, p.lit);
        const bool maximum_hit = ScanMatch(block.maximum, p.op, p.lit);
        if (!minimum_hit && !maximum_hit) {
          continue;
        }
        if (minimum_hit && maximum_hit) {
          SvSetRangeRaw(out, start, start + block.rows);
          continue;
        }
      } else if (p.lit < block.minimum || p.lit > block.maximum) {
        continue;
      }
      std::array<int64_t, kFastPforBlockRows> decoded{};
      FastPforDecodeBlock(block, decoded.data());
      for (uint32_t row = 0; row < block.rows; ++row) {
        if (ScanMatch(decoded[row], p.op, p.lit)) {
          out[(start + row) >> 6] |= 1ULL << ((start + row) & 63U);
        }
      }
    }
    return true;
  }
  if (s == Scheme::kDeltaFastPfor) {
    // The FastPFOR bracket fold with the delta dimension already folded in:
    // the stored brackets cover ALL rows (the encoder takes min/max over the
    // block's full values, base row included), and DeltaFastPforDecodeBlock
    // patches exceptions before its replay, so a boundary block is one
    // decode plus a plain per-row match -- no bitmap-aware correction is
    // ever needed here.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    DeltaFastPforRegion parsed;
    if (reg_end == nullptr ||
        !ParseDeltaFastPforRegion(region, rc, f.params[off_idx], reg_end,
                                  &parsed)) {
      return false;
    }
    if (!chunk.HasValidatedDeltaFastPforMetadata() &&
        !DeltaFastPforMetadataMatches(parsed)) {
      return false;
    }
    for (uint32_t block_index = 0; block_index < parsed.block_count;
         ++block_index) {
      const DeltaFastPforBlock& block = parsed.blocks[block_index];
      const uint32_t start = block_index * kDeltaFastPforBlockRows;
      if (p.op == ScanOp::kEq && block.minimum == block.maximum) {
        if (block.minimum == p.lit) {
          SvSetRangeRaw(out, start, start + block.rows);
        }
        continue;
      }
      if (p.op != ScanOp::kEq) {
        const bool minimum_hit = ScanMatch(block.minimum, p.op, p.lit);
        const bool maximum_hit = ScanMatch(block.maximum, p.op, p.lit);
        if (!minimum_hit && !maximum_hit) {
          continue;
        }
        if (minimum_hit && maximum_hit) {
          SvSetRangeRaw(out, start, start + block.rows);
          continue;
        }
      } else if (p.lit < block.minimum || p.lit > block.maximum) {
        continue;
      }
      std::array<int64_t, kDeltaFastPforBlockRows> decoded{};
      DeltaFastPforDecodeBlock(block, decoded.data());
      for (uint32_t row = 0; row < block.rows; ++row) {
        if (ScanMatch(decoded[row], p.op, p.lit)) {
          out[(start + row) >> 6] |= 1ULL << ((start + row) & 63U);
        }
      }
    }
    return true;
  }
  if (s == Scheme::kQpp) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    QppRegion parsed;
    if (reg_end == nullptr ||
        !ParseQppRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    QppFillRangeMask(parsed, p.op, p.lit, out);
    return true;
  }
  if (s == Scheme::kRleDict) {
    // Pass table over the dictionary (built once), then one SvSetRange per
    // RUN: the compressed-domain fold that kRleDict exists for. Extent gates
    // mirror the kRle arm above; clamp codes and rows past the run walk
    // decode as raw 0 (the kDict clamp-bucket convention).
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 2 > reg_end) {
      return false;
    }
    const uint16_t d = ReadBE16(region);
    if (d == 0) {
      return false;
    }
    const int cb = (BitsNeeded(d - 1) + 7) / 8;
    const char* dict = region + 2;
    const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
    const uint32_t runs = f.params[off_idx];
    if (runs_hdr + 4 > reg_end || ReadBE32(runs_hdr) != runs ||
        runs_hdr + 4 +
                (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
            reg_end) {
      return false;
    }
    std::array<uint8_t, 1024> pass_stack{};
    // Sized at construction even on the stack path: a default-constructed
    // vector's data() merges into the same `pass` phi below, and GCC's
    // -Wstringop-overflow models that merged pointer as a write into a
    // size-0 region in Release.
    const bool heap_pass = static_cast<size_t>(d) + 1 > sizeof(pass_stack);
    std::vector<uint8_t> pass_heap(heap_pass ? static_cast<size_t>(d) + 1 : 1U);
    uint8_t* pass = heap_pass ? pass_heap.data() : pass_stack.data();
    for (uint16_t e = 0; e < d; ++e) {
      pass[e] = ScanMatch(static_cast<int64_t>(
                              ReadBE64(dict + (static_cast<size_t>(e) * 8))),
                          p.op, p.lit)
                    ? 1
                    : 0;
    }
    pass[d] = ScanMatch(static_cast<int64_t>(0), p.op, p.lit) ? 1 : 0;
    uint64_t acc = 0;
    const char* q = runs_hdr + 4;
    for (uint32_t r = 0; r < runs && acc < rc; ++r) {
      uint64_t code = 0;
      for (int b = 0; b < cb; ++b) {
        code = (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
      }
      const uint64_t len = ReadBE32(q + cb);
      const uint64_t next = acc + len;
      const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
      if (end > acc && (code < d ? pass[code] : pass[d]) != 0) {
        SvSetRangeRaw(out, static_cast<uint32_t>(acc), end);
      }
      acc = next;
      q += static_cast<size_t>(cb) + 4;
    }
    if (acc < rc && pass[d] != 0) {
      SvSetRangeRaw(out, static_cast<uint32_t>(acc), rc);
    }
    return true;
  }
  if (s == Scheme::kDict) {
    // The 2-byte code-count header precedes every extent check: a
    // zero-length cell at the data-area end would read the next region's
    // bytes. Gate the header like the kConst arm above.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 2 > reg_end) {
      return false;
    }
    const uint16_t ncodes = ReadBE16(region);
    const int cw = BitsNeeded(ncodes > 0 ? ncodes - 1 : 0);
    const size_t dict_base =
        2 + ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
    const char* dict = region + dict_base;
    // The validator floors the region at 4B/entry but an int64 dict consumes
    // 8B/entry: a region built at the floor (or a hand-built footer) would
    // read ncodes*8 bytes past the region into the footer. Decline the fast
    // path so the caller decodes via DecodeInt64ColumnRg, which clamps.
    if (dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) > reg_end) {
      return false;
    }
    // Pass table over the WHOLE 2^cw code space: codes >= ncodes decode to
    // raw 0 (DecodeInt64ColumnRg's rule), so they take the same verdict as
    // the value 0. Resolving the clamp HERE rather than per row is what lets
    // the mask loop below drop its compare entirely.
    if (!DictPassByteExpandable(static_cast<uint32_t>(cw))) {
      // Not byte-expandable: a byte-indexed pass table would mis-decode a
      // straddling cw (see DictPassByteExpandable), and a 2^cw table would
      // not fit anyway. Keep the bit-at-a-time loop with the ncodes+1 clamp
      // bucket the kDict arm has always carried.
      std::array<uint8_t, 1024> pass_stack{};
      // Sized at construction even on the stack path: a default-constructed
      // vector's data() merges into the same `pass` phi below, and GCC's
      // -Wstringop-overflow models that merged pointer as a write into a
      // size-0 region in Release.
      const bool heap_pass =
          static_cast<size_t>(ncodes) + 1 > sizeof(pass_stack);
      std::vector<uint8_t> pass_heap(heap_pass ? static_cast<size_t>(ncodes) + 1
                                               : 1U);
      uint8_t* pass = heap_pass ? pass_heap.data() : pass_stack.data();
      for (uint32_t d = 0; d < ncodes; ++d) {
        pass[d] = static_cast<uint8_t>(ScanMatch(
            static_cast<int64_t>(ReadBE64(dict + (static_cast<size_t>(d) * 8))),
            p.op, p.lit));
      }
      const uint8_t clamp_ok =
          ScanMatch(static_cast<int64_t>(0), p.op, p.lit) ? 1 : 0;
      pass[ncodes] = clamp_ok;
      WordBitReader br(region + 2, region + dict_base);
      for (uint32_t i = 0; i < rc; ++i) {
        const uint64_t code = br.get(cw);
        if (pass[code < ncodes ? code : ncodes] != 0) {
          out[i >> 6] |= (1ULL << (i & 63));
        }
      }
      return true;
    }
    const auto cw32 = static_cast<uint32_t>(cw);
    const uint32_t slots = 1U << cw32;
    std::array<uint8_t, 16> pass{};
    for (uint32_t d = 0; d < ncodes; ++d) {
      pass[d] = static_cast<uint8_t>(ScanMatch(
          static_cast<int64_t>(ReadBE64(dict + (static_cast<size_t>(d) * 8))),
          p.op, p.lit));
    }
    const uint8_t clamp_ok =
        ScanMatch(static_cast<int64_t>(0), p.op, p.lit) ? 1 : 0;
    for (uint32_t c = ncodes; c < slots; ++c) {
      pass[c] = clamp_ok;
    }
    DictPassMaskByteExpanded(region + 2, rc, cw32, pass.data(), out);
    return true;
  }
  if (s == Scheme::kOpDict) {
    // Sorted dictionary -> the predicate is a contiguous rank window. Two
    // binary searches map `lit op` to [lo, span) once; each row is a bit
    // unpack plus a branch-free window test. No pass table, no dictionary
    // evaluation, no decode. Region gates mirror the kDict arm above.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 2 > reg_end) {
      return false;
    }
    const uint16_t ncodes = ReadBE16(region);
    if (ncodes == 0) {
      return false;
    }
    const int cw = BitsNeeded(ncodes - 1);
    const size_t dict_base =
        2 + ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
    const char* dict = region + dict_base;
    if (dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) > reg_end) {
      return false;
    }
    uint64_t lo = 0, span = 0;
    bool clamp_pass = false;
    OpDictWindowForValue(region, rc, ncodes,
                         static_cast<PaxChunk::CompareOp>(p.op), p.lit, &lo,
                         &span, &clamp_pass);
    if (DictPassByteExpandable(static_cast<uint32_t>(cw))) {
      // Same byte-expanded pass-table expansion as the kDict arm above; here
      // the "pass table" is just the rank window written over the whole 2^cw
      // code space, so the row loop needs neither the window test nor the
      // clamp compare.
      std::array<uint8_t, 16> pass{};
      for (uint32_t c = 0; c < (1U << cw); ++c) {
        uint8_t verdict = clamp_pass ? 1 : 0;
        if (c < ncodes) {
          verdict = (c - lo < span) ? 1 : 0;
        }
        pass[c] = verdict;
      }
      DictPassMaskByteExpanded(region + 2, rc, static_cast<uint32_t>(cw),
                               pass.data(), out);
      return true;
    }
    WordBitReader br(region + 2, region + dict_base);
    if ((1ULL << cw) == ncodes) {
      // Exact power-of-two code space: no clamp bucket can occur.
      for (uint32_t i = 0; i < rc; ++i) {
        const uint64_t code = br.get(cw);
        if (code - lo < span) {
          out[i >> 6] |= (1ULL << (i & 63));
        }
      }
    } else {
      for (uint32_t i = 0; i < rc; ++i) {
        const uint64_t code = br.get(cw);
        const bool ok = code < ncodes ? (code - lo < span) : clamp_pass;
        if (ok) {
          out[i >> 6] |= (1ULL << (i & 63));
        }
      }
    }
    return true;
  }
  if (s == Scheme::kByteSlice) {
    // BSI slice-parallel mask over the raw region (nulls arrive as raw 0,
    // exactly what the generic decode path compares, so no null skipping).
    const auto w = static_cast<uint8_t>(region[0]);
    const size_t stride = (static_cast<size_t>(rc) + 7) / 8;
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || w == 0 || w > 64 ||
        w != static_cast<uint8_t>(f.params[off_idx]) ||
        reg_end - region <
            static_cast<ptrdiff_t>(1 + (static_cast<size_t>(w) * stride))) {
      return false;
    }
    PaxChunk::ByteSliceCompareMask(region, rc,
                                   static_cast<PaxChunk::CompareOp>(p.op),
                                   p.lit, nullptr, out);
    return true;
  }
  if (s == Scheme::kPBitS && rc <= 1024) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    PBitSRegion parsed;
    if (reg_end == nullptr ||
        !ParsePBitSRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    std::array<uint64_t, 16> bits = {0};
    PaxChunk::PBitSCompareMask(region, f.params[off_idx], rc, reg_end,
                               static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                               nullptr, bits.data());
    for (uint32_t i = 0; i < rc; ++i) {
      if (((bits[i >> 6] >> (i & 63U)) & 1ULL) != 0) {
        out[i >> 6] |= 1ULL << (i & 63U);
      }
    }
    return true;
  }
  if (s == Scheme::kWaveletR) {
#ifdef __AVX2__
    alignas(64) std::array<int64_t, PaxChunk::kRowGroupSize> stack_values{};
    std::vector<int64_t> heap_values;
    int64_t* values = stack_values.data();
    if (rc > PaxChunk::kRowGroupSize) {
      heap_values.resize(rc);
      values = heap_values.data();
    }
    chunk.DecodeInt64ColumnRg(p.col, g, values);
    if (FillPredicateMaskAvx2(values, rc, p.op, p.lit, out)) {
      return true;
    }
#endif
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    WaveletRRegion parsed;
    if (!ParseWaveletRRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    uint64_t low = 0;
    uint64_t high = 0;
    if (!WaveletRNormalizedRange(parsed, static_cast<PaxChunk::CompareOp>(p.op),
                                 p.lit, &low, &high)) {
      return false;
    }
    for (uint32_t i = 0; i < rc; ++i) {
      const uint64_t value = WaveletRValueAt(parsed, i);
      if (value >= low && value <= high) {
        out[i >> 6] |= 1ULL << (i & 63U);
      }
    }
    return true;
  }
  if (s == Scheme::kAlp) {
    // ALP compressed-domain predicate: value-keyed mantissa backends
    // (CONST/RLE/DICT) evaluate the predicate ONCE PER DISTINCT mantissa --
    // each distinct value is reconstructed with the shared (double)m / 10^e
    // division (bitwise round-trip verified by the encoder) and compared on
    // its bitcast int64, so results equal the per-row decode exactly.
    // Exception rows patch against their stored raw values (set OR clear:
    // exception slots hold mantissa 0). BitWeave mantissas have no
    // distinct-value structure -> fall back to the generic decode path.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 4 > reg_end) {
      return false;
    }
    const int exp = static_cast<uint8_t>(region[0]);
    const auto backend = static_cast<Scheme>(static_cast<uint8_t>(region[1]));
    const uint32_t nexc = ReadBE16(region + 2);
    if (exp > 18 || nexc > rc) {
      return false;
    }
    const char* exc = region + 4;
    if (exc + (static_cast<size_t>(nexc) * 10) > reg_end) {
      return false;
    }
    const double f10 = AlpFactorOf(exp);
    const auto mantissa_match = [&](int64_t m) {
      const double back = static_cast<double>(m) / f10;
      uint64_t b{};
      std::memcpy(&b, &back, sizeof(b));
      return ScanMatch(static_cast<int64_t>(b), p.op, p.lit);
    };
    if (backend == Scheme::kConst) {
      const char* iconst = exc + (static_cast<size_t>(nexc) * 10);
      if (iconst + 8 > reg_end) {
        return false;
      }
      if (mantissa_match(static_cast<int64_t>(ReadBE64(iconst)))) {
        SvSetRangeRaw(out, 0, rc);
      }
    } else if (backend == Scheme::kRle) {
      const char* inner = exc + (static_cast<size_t>(nexc) * 10);
      if (inner + 4 > reg_end) {
        return false;
      }
      const uint32_t runs = ReadBE32(inner);
      if (runs == 0 || runs > rc ||
          inner + 4 + (static_cast<size_t>(runs) * 12) > reg_end) {
        return false;
      }
      uint32_t idx2 = 0;
      const char* q = inner + 4;
      for (uint32_t r2 = 0; r2 < runs && idx2 < rc; ++r2) {
        const auto m = static_cast<int64_t>(ReadBE64(q));
        const uint64_t len = ReadBE32(q + 8);
        const uint64_t next = static_cast<uint64_t>(idx2) + len;
        if (mantissa_match(m)) {
          SvSetRangeRaw(out, idx2,
                        (next > rc) ? rc : static_cast<uint32_t>(next));
        }
        idx2 = (next > rc) ? rc : static_cast<uint32_t>(next);
        q += 12;
      }
      // Run lengths may sum short of rc (the validator pins run count and
      // extent, not lengths). Every decoder defines those tail rows as
      // mantissa 0 (the per-value decoder falls off the run walk to 0; the
      // bulk decoder zero-fills), so a predicate matching 0 must select
      // them -- mirrors the scheme-2 arm above and the WASM twin.
      if (idx2 < rc && mantissa_match(0)) {
        SvSetRangeRaw(out, idx2, rc);
      }
    } else if (backend == Scheme::kDict) {
      const char* inner = exc + (static_cast<size_t>(nexc) * 10);
      if (inner + 2 > reg_end) {
        return false;
      }
      const uint16_t ncodes = ReadBE16(inner);
      const int cw = BitsNeeded(ncodes > 0 ? ncodes - 1 : 0);
      const size_t dict_base =
          2 + ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
      const char* dict = inner + dict_base;
      if (ncodes == 0 || dict + (static_cast<size_t>(ncodes) * 8) > reg_end) {
        return false;
      }
      // Pass table over dictionary mantissas; slot ncodes is the clamp
      // bucket (mantissa 0 -> +0.0 -> bitcast 0, mirroring the decoder).
      std::array<uint8_t, 1024> pass_stack{};
      // Sized at construction even on the stack path: a default-constructed
      // vector's data() merges into the same `pass` phi below, and GCC's
      // -Wstringop-overflow models that merged pointer as a write into a
      // size-0 region in Release.
      const bool heap_pass =
          static_cast<size_t>(ncodes) + 1 > sizeof(pass_stack);
      std::vector<uint8_t> pass_heap(heap_pass ? static_cast<size_t>(ncodes) + 1
                                               : 1U);
      uint8_t* pass = heap_pass ? pass_heap.data() : pass_stack.data();
      for (uint16_t d = 0; d < ncodes; ++d) {
        pass[d] = mantissa_match(static_cast<int64_t>(
                      ReadBE64(dict + (static_cast<size_t>(d) * 8))))
                      ? 1
                      : 0;
      }
      pass[ncodes] = ScanMatch(0, p.op, p.lit) ? 1 : 0;
      for (uint32_t i = 0; i < rc; ++i) {
        const uint64_t code =
            (cw == 0)
                ? 0
                : UnpackBitAt(
                      inner + 2,
                      static_cast<uint64_t>(i) * static_cast<uint64_t>(cw), cw);
        if (pass[code < ncodes ? code : ncodes] != 0U) {
          out[i >> 6] |= 1ULL << (i & 63);
        }
      }
    } else {
      return false;
    }
    // Exception rows: exact compare against the stored raw value.
    for (uint32_t k = 0; k < nexc; ++k) {
      const uint32_t pos = ReadBE16(exc + (static_cast<size_t>(k) * 10));
      if (pos >= rc) {
        continue;
      }
      if (ScanMatch(static_cast<int64_t>(
                        ReadBE64(exc + (static_cast<size_t>(k) * 10) + 2)),
                    p.op, p.lit)) {
        out[pos >> 6] |= 1ULL << (pos & 63);
      } else {
        out[pos >> 6] &= ~(1ULL << (pos & 63));
      }
    }
    return true;
  }
  if (s == Scheme::kFreq) {
    // Dominant-value answer once + per-exception pass flags, then one
    // bitmap walk: EQ on `top` degenerates to copying the bitmap.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 15 > reg_end) {
      return false;
    }
    const auto top = static_cast<int64_t>(ReadBE64(region));
    const auto ischeme = static_cast<uint8_t>(region[8]);
    const uint32_t iparam = ReadBE32(region + 9);
    const uint32_t nexc = ReadBE16(region + 13);
    const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
    // Footer/header agreement, mirroring the kRleDict arm's runs check.
    if (nexc != f.params[off_idx] || region + 15 + bmp_bytes > reg_end ||
        !InnerSchemeOk(ischeme)) {
      return false;
    }
    const char* bmp = region + 15;
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      exc = exc_stack.data();
      std::fill(exc, exc + nexc, 0);
    } else {
      exc_heap.assign(nexc, 0);
      exc = exc_heap.data();
    }
    DecodeInnerI64All(bmp + bmp_bytes, ischeme, iparam, nexc, reg_end, exc);
    std::array<uint8_t, 256> epass_stack{};
    std::vector<uint8_t> epass_heap;
    uint8_t* epass = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      epass = epass_stack.data();
    } else {
      epass_heap.assign(nexc, 0);
      epass = epass_heap.data();
    }
    for (uint32_t e = 0; e < nexc; ++e) {
      epass[e] = ScanMatch(exc[e], p.op, p.lit) ? 1 : 0;
    }
    const uint8_t tpass = ScanMatch(top, p.op, p.lit) ? 1 : 0;
    // Clear bits past nexc decode as raw 0 (scalar arm's clamp).
    const uint8_t zpass =
        ScanMatch(static_cast<int64_t>(0), p.op, p.lit) ? 1 : 0;
    uint32_t eidx = 0;
    for (uint32_t i = 0; i < rc; ++i) {
      const auto bit = static_cast<uint8_t>(
          (static_cast<uint8_t>(bmp[i >> 3]) >> (7 - (i & 7))) & 1);
      uint8_t ok = zpass;
      if (bit != 0) {
        ok = tpass;
      } else if (eidx < nexc) {
        ok = epass[eidx];
      }
      if (bit == 0) {
        ++eidx;
      }
      if (ok != 0) {
        out[i >> 6] |= 1ULL << (i & 63);
      }
    }
    return true;
  }
  if (s == Scheme::kTopk) {
    // One pass evaluation per HOT ENTRY (<= 15) + per exception, then one
    // nibble walk: EQ on a hot value folds to nibble == code. The pass
    // table makes the per-row work a table load either way, so the shape
    // matches kFreq's bitmap walk minus the bitmap.
    constexpr size_t kTopkHeader = 2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;  // 159
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + kTopkHeader > reg_end) {
      return false;
    }
    const uint32_t d = ReadBE16(region);
    const uint32_t nexc = ReadBE16(region + 2);
    const auto ischeme = static_cast<uint8_t>(region[154]);
    const uint32_t iparam = ReadBE32(region + 155);
    const size_t nib_bytes = (static_cast<size_t>(rc) + 1) / 2;
    if (d == 0 || d > 15 || nexc != f.params[off_idx] ||
        region + kTopkHeader + nib_bytes > reg_end ||
        (nexc > 0 && !InnerSchemeOk(ischeme))) {
      return false;
    }
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      exc = exc_stack.data();
      std::fill(exc, exc + nexc, 0);
    } else {
      exc_heap.assign(nexc, 0);
      exc = exc_heap.data();
    }
    if (nexc > 0) {
      DecodeInnerI64All(region + kTopkHeader + nib_bytes, ischeme, iparam, nexc,
                        reg_end, exc);
    }
    std::array<uint8_t, 256> epass_stack{};
    std::vector<uint8_t> epass_heap;
    uint8_t* epass = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      epass = epass_stack.data();
      std::fill(epass, epass + nexc, 0);
    } else {
      epass_heap.assign(nexc, 0);
      epass = epass_heap.data();
    }
    for (uint32_t e = 0; e < nexc; ++e) {
      epass[e] = ScanMatch(exc[e], p.op, p.lit) ? 1 : 0;
    }
    std::array<uint8_t, 15> cpass{};
    for (uint32_t c = 0; c < 15; ++c) {
      cpass[c] =
          c < d && ScanMatch(static_cast<int64_t>(ReadBE64(
                                 region + 4 + (static_cast<size_t>(c) * 8))),
                             p.op, p.lit)
              ? 1
              : 0;
    }
    // Clear bits past nexc decode as raw 0 (the scalar arm's clamp).
    const uint8_t zpass =
        ScanMatch(static_cast<int64_t>(0), p.op, p.lit) ? 1 : 0;
    uint32_t eidx = 0;
    for (uint32_t i = 0; i < rc; ++i) {
      const auto byte = static_cast<uint8_t>(region[kTopkHeader + (i >> 1)]);
      const uint32_t code = (i & 1) == 0 ? (byte >> 4) : (byte & 0x0F);
      uint8_t ok = 0;
      if (code == 15) {
        ok = eidx < nexc ? epass[eidx] : zpass;
        if (eidx < nexc) {
          ++eidx;
        }
      } else {
        ok = code < d ? cpass[code] : 0;
      }
      if (ok != 0) {
        out[i >> 6] |= 1ULL << (i & 63);
      }
    }
    return true;
  }
  if (s == Scheme::kTileDict) {
    // Per-tile bracket fold: the ascending tile dictionary maps the
    // predicate to ONE code window; a wholly-inside tile is a single range
    // set and a wholly-outside tile is skipped -- zero code reads for
    // either. Only a window-straddling tile walks its codes (<= 6 bits per
    // row of memory traffic, no value decode anywhere).
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || reg_end <= region || f.params[off_idx] != 0) {
      return false;
    }
    TileDictCursor c;
    if (!TileDictFirst(region, rc, reg_end, &c)) {
      return false;
    }
    for (;;) {
      uint64_t lo = 0;
      uint64_t span = 0;
      bool clamp_pass = false;
      TileDictWindowForValue(c, static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                             &lo, &span, &clamp_pass);
      if (span == 0 && !clamp_pass) {
        // Wholly outside: the bracket skips the tile's codes entirely.
      } else if (lo == 0 && span == c.d) {
        SvSetRangeRaw(out, c.tile_start, c.tile_start + c.rows);
      } else {
        for (uint32_t j = 0; j < c.rows; ++j) {
          const uint64_t code =
              c.w == 0 ? 0
                       : UnpackBitAt(c.codes,
                                     static_cast<uint64_t>(j) *
                                         static_cast<uint64_t>(c.w),
                                     c.w);
          const bool hit = code < c.d ? (code - lo < span) : clamp_pass;
          if (hit) {
            const uint32_t row = c.tile_start + j;
            out[row >> 6] |= UINT64_C(1) << (row & 63);
          }
        }
      }
      if (!TileDictNext(&c)) {
        break;
      }
    }
    return true;
  }
  if (s == Scheme::kSortDict) {
    // Sorted-window fold: the predicate becomes ONE [lo, hi) window over the
    // sorted view by bracket bisect + a <= 63-gap walk. A window that MISSES
    // or COVERS the whole region answers in O(log nb) with zero row reads --
    // the group is pruned (or accepted) without touching a value. A PARTIAL
    // window declines: the mask would cost one rank extraction per row
    // (measured ~500us per 64K rows in bench_sortdict), while the generic
    // decode-and-compare path the caller falls back to runs the same
    // predicate through the AVX2 kernels at ~40us -- materializing a worse
    // mask here would throw that win away.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    SortDictRegion parsed;
    if (reg_end == nullptr ||
        !ParseSortDictRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    uint32_t wlo = 0;
    uint32_t whi = 0;
    if (!SortDictPredicateWindow(parsed, static_cast<PaxChunk::CompareOp>(p.op),
                                 p.lit, &wlo, &whi)) {
      return false;
    }
    if (wlo >= whi) {
      return true;  // empty window: nothing selected
    }
    if (wlo == 0 && whi >= rc) {
      SvSetRangeRaw(out, 0, rc);
      return true;
    }
    return false;  // partial window: the generic decode path is faster
  }
  if (s == Scheme::kPostings) {
    // Selective-posting materialization: the predicate passes a contiguous
    // list window (EQ is one bisect, the ordered ops a prefix/suffix), the
    // count table prices it in closed form, and only when it is SELECTIVE
    // does the kernel walk the passing lists' Rice codes to set bits. Two
    // declines keep it honest: a dense match (more than rc/8 rows -- past
    // that the caller's AVX2 decode kernels are faster than a Rice walk,
    // the measured kSortDict decline) and a window that touches nothing
    // (returns true with no bits set -- the caller's popcount answers 0
    // without a decode).
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    PostingsRegion parsed;
    if (reg_end == nullptr ||
        !ParsePostingsRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    uint32_t lo = 0;
    uint32_t hi = parsed.d;
    switch (p.op) {
      case ScanOp::kEq:
        lo = PostingsLowerBoundList(parsed, p.lit);
        hi = PostingsUpperBoundList(parsed, p.lit);
        break;
      case ScanOp::kLe:
        hi = PostingsUpperBoundList(parsed, p.lit);
        break;
      case ScanOp::kLt:
        hi = PostingsLowerBoundList(parsed, p.lit);
        break;
      case ScanOp::kGe:
        lo = PostingsLowerBoundList(parsed, p.lit);
        break;
      case ScanOp::kGt:
        lo = PostingsUpperBoundList(parsed, p.lit);
        break;
    }
    uint64_t matched = 0;
    for (uint32_t c = lo; c < hi; ++c) {
      matched += PostingsCountAt(parsed, c);
    }
    if (matched == 0) {
      return true;  // nothing selected, zero decode
    }
    if (std::cmp_greater(matched, rc / 8)) {
      return false;  // dense: the AVX2 decode wins
    }
    // Jump straight to the first passing list's sub-stream (the bitoffs
    // table + byte-aligned sub-streams): only the passing lists' Rice codes
    // are decoded, so the walk is O(matched), not O(rows).
    PostingsBitCursor cur;
    for (uint32_t c = lo; c < hi; ++c) {
      const uint32_t cnt = PostingsCountAt(parsed, c);
      if (cnt == 1) {
        continue;
      }
      cur.Init(parsed.stream + (PostingsBitOffAt(parsed, c) / 8),
               parsed.region_end);
      uint32_t row = PostingsFirstAt(parsed, c);
      out[row >> 6] |= UINT64_C(1) << (row & 63);
      for (uint32_t i = 1; i < cnt; ++i) {
        uint32_t next = 0;
        if (!PostingsNextRow(parsed, &cur, c, row, &next)) {
          return false;
        }
        row = next;
        out[row >> 6] |= UINT64_C(1) << (row & 63);
      }
    }
    return true;
  }
  if (s == Scheme::kMod) {
    // EQ folds to the literal's residue class in Z/mZ: on-model matches are
    // i = (k-a)/g * (b/g)^-1 mod q (g = gcd(b,m), q = m/g), one arithmetic
    // progression stepping q -- O(n/q) bit sets after one modular inverse.
    // The other ops have no closed-form POSITION set (a residue interval
    // scatters over the rows), so they decline to the generic path.
    if (p.op != ScanOp::kEq) {
      return false;
    }
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 31 > reg_end) {
      return false;
    }
    const uint64_t a = ReadBE64(region);
    const uint64_t b = ReadBE64(region + 8);
    const uint64_t m = ReadBE64(region + 16);
    const auto ischeme = static_cast<uint8_t>(region[24]);
    const uint32_t iparam = ReadBE32(region + 25);
    const uint32_t nexc = ReadBE16(region + 29);
    const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
    if (nexc != f.params[off_idx] || nexc > rc || m < 2 ||
        m > (UINT64_C(1) << 63) || a >= m || b >= m || b == 0 ||
        region + 31 + bmp_bytes > reg_end ||
        (nexc > 0 && !InnerSchemeOk(ischeme))) {
      return false;
    }
    // Bitmap canonicality: exactly nexc clear bits (tail masked). The
    // class walk never re-evaluates clear-bit rows -- every one of them
    // must carry an exception -- so a hostile over-counting bitmap declines
    // instead of silently dropping rows that decode as raw 0.
    {
      const auto* bmp = reinterpret_cast<const uint8_t*>(region + 31);
      uint64_t ones = 0;
      for (size_t i = 0; i < bmp_bytes; ++i) {
        uint64_t byte = bmp[i];
        if (i + 1 == bmp_bytes && (rc & 7) != 0) {
          byte &= static_cast<uint64_t>(0xFFU << (8 - (rc & 7))) & 0xFF;
        }
        ones += static_cast<uint64_t>(__builtin_popcountll(byte));
      }
      if (ones != static_cast<uint64_t>(rc) - nexc) {
        return false;
      }
    }
    // Residue class: b*i ≡ lit - a (mod m). On-model values live in
    // [0, m), so a negative literal or one >= m can only match exceptions.
    const int64_t lit = p.lit;
    if (lit >= 0 && std::cmp_less(lit, m)) {
      const auto k = static_cast<uint64_t>(lit);
      const uint64_t diff = (k % m + m - a) % m;
      const uint64_t bg = ModGcd(b, m);
      if (diff % bg == 0) {
        const uint64_t q = m / bg;
        const auto step = static_cast<uint64_t>(
            static_cast<unsigned __int128>(diff / bg) *
            static_cast<unsigned __int128>(ModInv(b / bg, q)) %
            static_cast<unsigned __int128>(q));
        for (uint64_t i = step; i < rc; i += q) {
          out[i >> 6] |= UINT64_C(1) << (i & 63);
        }
      }
    }
    // Exception patches, byte-skipping like the kArith arm: all-on bytes
    // are pure residue-class territory.
    const auto* bmp = reinterpret_cast<const uint8_t*>(region + 31);
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      exc = exc_stack.data();
      std::fill(exc, exc + nexc, 0);
    } else {
      exc_heap.assign(nexc, 0);
      exc = exc_heap.data();
    }
    if (nexc > 0) {
      DecodeInnerI64All(region + 31 + bmp_bytes, ischeme, iparam, nexc, reg_end,
                        exc);
    }
    uint32_t eidx = 0;
    for (size_t byte_i = 0; byte_i < bmp_bytes; ++byte_i) {
      const uint8_t byte = bmp[byte_i];
      if (byte == 0xFF) {
        continue;  // every row on-model
      }
      const auto base = static_cast<uint32_t>(byte_i * 8);
      for (uint32_t bit = 0; bit < 8 && base + bit < rc; ++bit) {
        if (((byte >> (7 - bit)) & 1) != 0) {
          continue;
        }
        const int64_t v = (eidx < nexc) ? exc[eidx] : 0;
        ++eidx;
        if (ScanMatch(v, p.op, p.lit)) {
          out[(base + bit) >> 6] |= 1ULL << ((base + bit) & 63);
        } else {
          out[(base + bit) >> 6] &= ~(1ULL << ((base + bit) & 63));
        }
      }
    }
    return true;
  }
  if (s == Scheme::kArith) {
    // On-line rows answer as a closed-form INDEX WINDOW: v(i) = a + i*d is
    // monotone, so `lit op` maps to one contiguous range via floor/ceil
    // division -- one SvSetRange, then per-EXCEPTION bit patches (u16 of
    // them). No per-row evaluation of any on-line row. Declines (returns
    // false -> generic path) whenever the model could wrap inside the row
    // group: the window math needs true monotonicity.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 23 > reg_end) {
      return false;
    }
    const auto a = static_cast<int64_t>(ReadBE64(region));
    const auto d = static_cast<int64_t>(ReadBE64(region + 8));
    const auto ischeme = static_cast<uint8_t>(region[16]);
    const uint32_t iparam = ReadBE32(region + 17);
    const uint32_t nexc = ReadBE16(region + 21);
    const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
    if (nexc != f.params[off_idx] || nexc > rc ||
        region + 23 + bmp_bytes > reg_end ||
        (nexc > 0 && !InnerSchemeOk(ischeme))) {
      return false;
    }
    // Window eligibility: v(rc-1) = a + (rc-1)*d must stay inside int64
    // (wrapping breaks monotonicity); d == 0 is never emitted.
    const __int128 last =
        static_cast<__int128>(a) +
        (static_cast<__int128>(static_cast<uint64_t>(rc) - 1) *
         static_cast<__int128>(d));
    if (d == 0 || last < static_cast<__int128>(INT64_MIN) ||
        last > static_cast<__int128>(INT64_MAX)) {
      return false;
    }
    // Predicate -> value interval; the bounds stay inside int128, so the
    // division helpers below cannot overflow.
    auto plo = static_cast<__int128>(INT64_MIN);
    auto phi = static_cast<__int128>(INT64_MAX);
    switch (p.op) {
      case ScanOp::kEq:
        plo = phi = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kLe:
        phi = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kLt:
        if (p.lit == INT64_MIN) {
          phi = static_cast<__int128>(INT64_MIN) - 1;  // unsatisfiable
        } else {
          phi = static_cast<__int128>(p.lit) - 1;
        }
        break;
      case ScanOp::kGe:
        plo = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kGt:
        if (p.lit == INT64_MAX) {
          plo = static_cast<__int128>(INT64_MAX) + 1;  // unsatisfiable
        } else {
          plo = static_cast<__int128>(p.lit) + 1;
        }
        break;
      default:
        // An op outside the enum (hostile caller) must fail closed like
        // ScanMatch, not fall through to the "everything" bounds.
        return false;
    }
    const auto floordiv = [](__int128 x, int64_t y) {
      __int128 q = x / y;
      if (x % y != 0 && (x < 0) != (y < 0)) {
        --q;
      }
      return q;
    };
    // ceil(x/d) == -floor(-x/d). d < 0 flips which bound drives which end.
    __int128 w_lo = 0, w_hi = 0;  // satisfying index range, [w_lo, w_hi)
    if (d > 0) {
      w_lo = std::max<__int128>(0, -floordiv(-(plo - a), d));
      w_hi = std::min<__int128>(rc, floordiv(phi - a, d) + 1);
    } else {
      w_lo = std::max<__int128>(0, -floordiv(-(phi - a), d));
      w_hi = std::min<__int128>(rc, floordiv(plo - a, d) + 1);
    }
    if (w_hi > w_lo) {
      SvSetRangeRaw(out, static_cast<uint32_t>(w_lo),
                    static_cast<uint32_t>(w_hi));
    }
    // Exception patches: position = clear bitmap bit, value = next inner
    // entry; tail clear bits (eidx >= nexc) decode as raw 0. Byte-skipping:
    // all-on bytes are pure window territory, so the patch cost scales with
    // the EXCEPTION count, not the row count.
    const auto* bmp = reinterpret_cast<const uint8_t*>(region + 23);
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      exc = exc_stack.data();
      std::fill(exc, exc + nexc, 0);
    } else {
      exc_heap.assign(nexc, 0);
      exc = exc_heap.data();
    }
    if (nexc > 0) {
      DecodeInnerI64All(region + 23 + bmp_bytes, ischeme, iparam, nexc, reg_end,
                        exc);
    }
    uint32_t eidx = 0;
    for (size_t byte_i = 0; byte_i < bmp_bytes; ++byte_i) {
      const uint8_t byte = bmp[byte_i];
      if (byte == 0xFF) {
        continue;  // every row on-line
      }
      const auto base = static_cast<uint32_t>(byte_i * 8);
      for (uint32_t b = 0; b < 8 && base + b < rc; ++b) {
        if (((byte >> (7 - b)) & 1) != 0) {
          continue;
        }
        const int64_t v = (eidx < nexc) ? exc[eidx] : 0;
        ++eidx;
        if (ScanMatch(v, p.op, p.lit)) {
          out[(base + b) >> 6] |= 1ULL << ((base + b) & 63);
        } else {
          out[(base + b) >> 6] &= ~(1ULL << ((base + b) & 63));
        }
      }
    }
    return true;
  }
  if (s == Scheme::kLane) {
    // Per-lane closed-form index windows (the kArith fold, nd of them):
    // the predicate maps to one satisfying tick range per lane via
    // floor/ceil division, and lane l's rows live at l + nd*t. Declines
    // (returns false -> generic path) whenever any lane's line could wrap
    // inside the row group (the window math needs true monotonicity) and
    // when a PARTIAL window would touch more than a quarter of the rows --
    // scattered stride-sets lose to the caller's AVX2 decode kernels on
    // dense predicates, the same trade-off the kSortDict window decline
    // measures. Full-window (every lane covered) folds to one range set.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 40 > reg_end) {
      return false;
    }
    const uint32_t nd = static_cast<uint8_t>(region[0]);
    if (nd < 2 || nd > 16) {
      return false;
    }
    const size_t hdr = 8 + (static_cast<size_t>(nd) * 16);
    const auto ischeme = static_cast<uint8_t>(region[hdr - 7]);
    const uint32_t iparam = ReadBE32(region + hdr - 6);
    const uint32_t nexc = ReadBE16(region + hdr - 2);
    const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
    if (nexc != f.params[off_idx] || nexc > rc ||
        region + hdr + bmp_bytes > reg_end ||
        (nexc > 0 && !InnerSchemeOk(ischeme))) {
      return false;
    }
    std::array<int64_t, 16> base{};
    std::array<int64_t, 16> step{};
    std::array<uint64_t, 16> cnt{};
    for (uint32_t l = 0; l < nd; ++l) {
      base[l] = static_cast<int64_t>(
          ReadBE64(region + 1 + (static_cast<size_t>(l) * 16)));
      step[l] = static_cast<int64_t>(
          ReadBE64(region + 1 + (static_cast<size_t>(l) * 16) + 8));
      cnt[l] = rc > l ? ((static_cast<uint64_t>(rc) - 1 - l) / nd) + 1 : 0;
      // Window eligibility: the lane's signed line must stay inside int64
      // across the row group (monotone, so no wrap; the encoder emits
      // wrapped grids, so this is a real decline, kArith's check per lane).
      if (cnt[l] > 0) {
        const __int128 last = static_cast<__int128>(base[l]) +
                              (static_cast<__int128>(step[l]) *
                               static_cast<__int128>(cnt[l] - 1));
        if (last < static_cast<__int128>(INT64_MIN) ||
            last > static_cast<__int128>(INT64_MAX)) {
          return false;
        }
      }
    }
    // Predicate -> value interval; the bounds stay inside int128, so the
    // division helper below cannot overflow (kArith's switch verbatim).
    auto plo = static_cast<__int128>(INT64_MIN);
    auto phi = static_cast<__int128>(INT64_MAX);
    switch (p.op) {
      case ScanOp::kEq:
        plo = phi = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kLe:
        phi = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kLt:
        if (p.lit == INT64_MIN) {
          phi = static_cast<__int128>(INT64_MIN) - 1;  // unsatisfiable
        } else {
          phi = static_cast<__int128>(p.lit) - 1;
        }
        break;
      case ScanOp::kGe:
        plo = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kGt:
        if (p.lit == INT64_MAX) {
          plo = static_cast<__int128>(INT64_MAX) + 1;  // unsatisfiable
        } else {
          plo = static_cast<__int128>(p.lit) + 1;
        }
        break;
      default:
        return false;
    }
    const auto floordiv = [](__int128 x, int64_t y) {
      __int128 q = x / y;
      if (x % y != 0 && (x < 0) != (y < 0)) {
        --q;
      }
      return q;
    };
    std::array<uint32_t, 16> wlo{};
    std::array<uint32_t, 16> whi{};
    uint64_t total = 0;
    bool all_full = true;
    for (uint32_t l = 0; l < nd; ++l) {
      wlo[l] = 0;
      whi[l] = 0;
      if (cnt[l] == 0) {
        continue;  // hostile tail region: lane past rc
      }
      const auto a = static_cast<__int128>(base[l]);
      const int64_t d = step[l];
      __int128 lo = 0;
      __int128 hi = 0;
      if (d > 0) {
        lo = std::max<__int128>(0, -floordiv(-(plo - a), d));
        hi = std::min<__int128>(static_cast<__int128>(cnt[l]),
                                floordiv(phi - a, d) + 1);
      } else if (d < 0) {
        lo = std::max<__int128>(0, -floordiv(-(phi - a), d));
        hi = std::min<__int128>(static_cast<__int128>(cnt[l]),
                                floordiv(plo - a, d) + 1);
      } else if (a >= plo && a <= phi) {
        hi = static_cast<__int128>(cnt[l]);  // constant lane: all or nothing
      }
      if (hi > lo) {
        wlo[l] = static_cast<uint32_t>(lo);
        whi[l] = static_cast<uint32_t>(hi);
        total += whi[l] - wlo[l];
        all_full = all_full && wlo[l] == 0 && whi[l] == cnt[l];
      }
    }
    if (!all_full && total * 4 > rc) {
      return false;  // dense partial declines
    }
    if (all_full) {
      SvSetRangeRaw(out, 0, rc);
    } else {
      for (uint32_t l = 0; l < nd; ++l) {
        for (uint32_t t = wlo[l]; t < whi[l]; ++t) {
          const uint32_t i = l + (nd * t);
          out[i >> 6] |= UINT64_C(1) << (i & 63);
        }
      }
    }
    // Exception patches: position = clear bitmap bit, value = next inner
    // entry; tail clear bits (eidx >= nexc) decode as raw 0. Byte-skipping:
    // all-on bytes are pure window territory, so the patch cost scales with
    // the EXCEPTION count, not the row count (kArith's walk verbatim).
    const auto* bmp = reinterpret_cast<const uint8_t*>(region + hdr);
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      exc = exc_stack.data();
      std::fill(exc, exc + nexc, 0);
    } else {
      exc_heap.assign(nexc, 0);
      exc = exc_heap.data();
    }
    if (nexc > 0) {
      DecodeInnerI64All(region + hdr + bmp_bytes, ischeme, iparam, nexc,
                        reg_end, exc);
    }
    uint32_t eidx = 0;
    for (size_t byte_i = 0; byte_i < bmp_bytes; ++byte_i) {
      const uint8_t byte = bmp[byte_i];
      if (byte == 0xFF) {
        continue;  // every row on-model
      }
      const auto pos = static_cast<uint32_t>(byte_i * 8);
      for (uint32_t b = 0; b < 8 && pos + b < rc; ++b) {
        if (((byte >> (7 - b)) & 1) != 0) {
          continue;
        }
        const int64_t v = (eidx < nexc) ? exc[eidx] : 0;
        ++eidx;
        if (ScanMatch(v, p.op, p.lit)) {
          out[(pos + b) >> 6] |= 1ULL << ((pos + b) & 63);
        } else {
          out[(pos + b) >> 6] &= ~(1ULL << ((pos + b) & 63));
        }
      }
    }
    return true;
  }
  if (s == Scheme::kPla) {
    // Per-segment closed-form index windows: each segment's line v(i) =
    // first + (i - start)*d is monotone in i, so the predicate maps to one
    // contiguous window per segment (miss / full-cover / partial via
    // floor/ceil division) -- O(nseg) range sets, then <= u16 exception
    // patches. No per-row evaluation of any on-line row. Declines whenever
    // a segment endpoint leaves int64 (wrapping breaks monotonicity),
    // mirroring the kArith arm.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    PlaRegion pl;
    if (reg_end == nullptr ||
        !ParsePlaRegion(region, rc, f.params[off_idx], reg_end, &pl)) {
      return false;
    }
    auto plo = static_cast<__int128>(INT64_MIN);
    auto phi = static_cast<__int128>(INT64_MAX);
    switch (p.op) {
      case ScanOp::kEq:
        plo = phi = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kLe:
        phi = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kLt:
        if (p.lit == INT64_MIN) {
          phi = static_cast<__int128>(INT64_MIN) - 1;  // unsatisfiable
        } else {
          phi = static_cast<__int128>(p.lit) - 1;
        }
        break;
      case ScanOp::kGe:
        plo = static_cast<__int128>(p.lit);
        break;
      case ScanOp::kGt:
        if (p.lit == INT64_MAX) {
          plo = static_cast<__int128>(INT64_MAX) + 1;  // unsatisfiable
        } else {
          plo = static_cast<__int128>(p.lit) + 1;
        }
        break;
      default:
        // An op outside the enum (hostile caller) must fail closed like
        // ScanMatch, not fall through to the "everything" bounds.
        return false;
    }
    const auto floordiv = [](__int128 x, int64_t y) {
      __int128 q = x / y;
      if (x % y != 0 && (x < 0) != (y < 0)) {
        --q;
      }
      return q;
    };
    for (uint32_t j = 0; j < pl.nseg; ++j) {
      const uint32_t seg_start =
          j == 0 ? 0
                 : ReadBE32(pl.segs + (static_cast<size_t>(j - 1) * 20) + 16);
      const uint32_t seg_end =
          ReadBE32(pl.segs + (static_cast<size_t>(j) * 20) + 16);
      const __int128 first = static_cast<int64_t>(
          ReadBE64(pl.segs + (static_cast<size_t>(j) * 20)));
      const __int128 d = static_cast<int64_t>(
          ReadBE64(pl.segs + (static_cast<size_t>(j) * 20) + 8));
      const __int128 last =
          first + (static_cast<__int128>(seg_end - seg_start - 1) * d);
      if (last < static_cast<__int128>(INT64_MIN) ||
          last > static_cast<__int128>(INT64_MAX)) {
        return false;
      }
      const __int128 vlo = std::min(first, last);
      const __int128 vhi = std::max(first, last);
      if (vhi < plo || vlo > phi) {
        continue;  // whole segment misses
      }
      if (plo <= vlo && vhi <= phi) {
        SvSetRangeRaw(out, seg_start, seg_end);  // whole segment passes
        continue;
      }
      if (d == 0) {
        continue;  // a point value is pass-all or pass-none
      }
      const __int128 len = seg_end - seg_start;
      __int128 w_lo = 0, w_hi = 0;  // satisfying index range within the segment
      if (d > 0) {
        w_lo = seg_start +
               std::max<__int128>(
                   0, -floordiv(-(plo - first), static_cast<int64_t>(d)));
        w_hi = seg_start +
               std::min<__int128>(
                   len, floordiv(phi - first, static_cast<int64_t>(d)) + 1);
      } else {
        w_lo = seg_start +
               std::max<__int128>(
                   0, -floordiv(-(phi - first), static_cast<int64_t>(d)));
        w_hi = seg_start +
               std::min<__int128>(
                   len, floordiv(plo - first, static_cast<int64_t>(d)) + 1);
      }
      if (w_hi > w_lo) {
        SvSetRangeRaw(out, static_cast<uint32_t>(w_lo),
                      static_cast<uint32_t>(w_hi));
      }
    }
    // Exception patches own every clear bitmap bit (the windows above only
    // set on-line bits); identical walk to the kArith arm.
    const uint32_t nexc = ReadBE16(pl.hdr + 5);
    const size_t bmp_bytes = (static_cast<size_t>(rc) + 7) / 8;
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (static_cast<size_t>(nexc) <= 256) {
      exc = exc_stack.data();
      std::fill(exc, exc + nexc, 0);
    } else {
      exc_heap.assign(nexc, 0);
      exc = exc_heap.data();
    }
    if (nexc > 0) {
      DecodeInnerI64All(pl.bmp + bmp_bytes, static_cast<uint8_t>(pl.hdr[0]),
                        ReadBE32(pl.hdr + 1), nexc, reg_end, exc);
    }
    uint32_t eidx = 0;
    const auto* pl_bmp = reinterpret_cast<const uint8_t*>(pl.bmp);
    for (size_t byte_i = 0; byte_i < bmp_bytes; ++byte_i) {
      const uint8_t byte = pl_bmp[byte_i];
      if (byte == 0xFF) {
        continue;  // every row on-line
      }
      const auto base = static_cast<uint32_t>(byte_i * 8);
      for (uint32_t b = 0; b < 8 && base + b < rc; ++b) {
        if (((byte >> (7 - b)) & 1) != 0) {
          continue;
        }
        const int64_t v = (eidx < nexc) ? exc[eidx] : 0;
        ++eidx;
        if (ScanMatch(v, p.op, p.lit)) {
          out[(base + b) >> 6] |= 1ULL << ((base + b) & 63);
        } else {
          out[(base + b) >> 6] &= ~(1ULL << ((base + b) & 63));
        }
      }
    }
    return true;
  }
  if (s == Scheme::kInterp) {
    // Bracket pruning + boundary-block tree walks: blocks whose checkpoint
    // bracket misses the predicate window contribute nothing (their mask
    // bits stay clear), fully-inside brackets are ONE SvSetRange, and only
    // boundary blocks read their bitstream. Mirrors InterpRegionCountSum's
    // block-level zone map at mask granularity.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    InterpRegion parsed;
    if (reg_end == nullptr ||
        !ParseInterpRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    __int128 plo = 0, phi = 0;
    if (!InterpWindowForValue(static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                              &plo, &phi)) {
      return false;
    }
    for (uint32_t b = 0; b < parsed.ncp; ++b) {
      const char* ck = parsed.ckpts + (static_cast<size_t>(b) * 16);
      // SIGNED bracket domain (see ParseInterpRegion).
      const auto first =
          static_cast<__int128>(static_cast<int64_t>(ReadBE64(ck)));
      const auto last =
          static_cast<__int128>(static_cast<int64_t>(ReadBE64(ck + 8)));
      if (last < plo || first > phi) {
        continue;  // no row in this block passes
      }
      const uint32_t bs = b * kInterpBlockRows;
      const uint32_t bt = std::min<uint32_t>(rc, (b + 1) * kInterpBlockRows);
      if (first >= plo && last <= phi) {
        SvSetRangeRaw(out, bs, bt);
        continue;
      }
      uint64_t ignored = 0;
      InterpEvalBlock(parsed, b, plo, phi, out, false, &ignored, nullptr);
    }
    return true;
  }
  if (s == Scheme::kDeltaPfor) {
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    DeltaPForRegion parsed;
    if (reg_end == nullptr ||
        !ParseDeltaPForRegion(region, rc, f.params[off_idx], reg_end,
                              &parsed)) {
      return false;
    }
    __int128 low_value = 0;
    __int128 high_value = 0;
    if (!InterpWindowForValue(static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                              &low_value, &high_value)) {
      return false;
    }
    for (uint32_t block = 0; block < parsed.blocks; ++block) {
      const char* bracket = parsed.brackets + (static_cast<size_t>(block) *
                                               kDeltaPForBracketBytes);
      const auto first = static_cast<int64_t>(ReadBE64(bracket));
      const auto last = static_cast<int64_t>(ReadBE64(bracket + 8));
      if (static_cast<__int128>(last) < low_value ||
          static_cast<__int128>(first) > high_value) {
        continue;
      }
      const uint32_t start = block * kDeltaPForBlockRows;
      const uint32_t rows =
          std::min<uint32_t>(kDeltaPForBlockRows, parsed.n - start);
      if (static_cast<__int128>(first) >= low_value &&
          static_cast<__int128>(last) <= high_value) {
        SvSetRangeRaw(out, start, start + rows);
        continue;
      }
      std::array<int64_t, kDeltaPForBlockRows> decoded{};
      DecodeDeltaPForBlock(parsed, block, decoded.data());
      for (uint32_t i = 0; i < rows; ++i) {
        const __int128 value = decoded[i];
        if (value >= low_value && value <= high_value) {
          const uint32_t row = start + i;
          out[row >> 6] |= 1ULL << (row & 63U);
        }
      }
    }
    return true;
  }
  if (s == Scheme::kSfor) {
    // SFOR is the double-column analogue of the kInterp/kDeltaPfor brackets,
    // one level stronger: the group directory partitions the column into
    // DISJOINT ASCENDING value ranges [msb<<s, (msb<<s) | (2^s-1)] AND the
    // values ascend INSIDE each group (lsbits are ascending under a shared
    // prefix), so a predicate window folds to whole-group range sets plus
    // two in-group bisects per boundary group -- the answer is O(ng + log
    // rows) unpacks, no value array and not even a boundary scan. Values are
    // raw bitcast bits (the encoder gates the column non-negative, so raw
    // order is value order and the generic int64 fallback agrees bit for
    // bit).
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    SforRegion parsed;
    if (reg_end == nullptr ||
        !ParseSforRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    __int128 plo = 0, phi = 0;
    if (!InterpWindowForValue(static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                              &plo, &phi)) {
      return false;
    }
    // The brackets are disjoint and ascending. Let A = the first group with
    // glo >= plo and B = the first group with ghi > phi (two directory
    // bisects). Groups [A, B) are wholly inside the window and collapse into
    // ONE SvSetRange; group A-1 straddles plo from below and group B
    // straddles phi, and because the values ALSO ascend inside a group, each
    // resolves into one contiguous run by an in-group bisect. Net cost:
    // O(log ng + log rows) unpacks -- no value array, no scan.
    auto bracket = [&](uint32_t gr, __int128* lo128, __int128* hi128) {
      const __int128 base = static_cast<__int128>(SforMsb(parsed, gr))
                            << parsed.s;
      *lo128 = base;
      *hi128 = parsed.s == 63 ? ~__int128(0)
                              : base | ((__int128(1) << parsed.s) - 1);
    };
    uint32_t alo = 0, ahi = parsed.ng;
    while (alo < ahi) {
      const uint32_t mid = alo + ((ahi - alo) / 2);
      __int128 glo = 0, ghi = 0;
      bracket(mid, &glo, &ghi);
      if (glo < plo) {
        alo = mid + 1;
      } else {
        ahi = mid;
      }
    }
    const uint32_t g_first_inside = alo;  // first group with glo >= plo
    uint32_t blo2 = 0, bhi2 = parsed.ng;
    while (blo2 < bhi2) {
      const uint32_t mid = blo2 + ((bhi2 - blo2) / 2);
      __int128 glo = 0, ghi = 0;
      bracket(mid, &glo, &ghi);
      if (ghi <= phi) {
        blo2 = mid + 1;
      } else {
        bhi2 = mid;
      }
    }
    const uint32_t g_end_inside = blo2;  // first group with ghi > phi
    // The brackets are disjoint, so exactly three shapes exist: A == B
    // (the window falls in a GAP between brackets or outside the column
    // entirely -- nothing passes), A == B + 1 (ONE group's bracket
    // straddles both window edges), and A < B (wholly-inside groups
    // [A, B) plus one straddling group on each side, when present).
    if (g_first_inside == g_end_inside + 1) {
      const uint32_t gr = g_end_inside;  // < ng by construction
      const uint64_t boff = SforGroupOffset(parsed, gr);
      const uint32_t first = SforGroupLowerBound(parsed, gr, boff, plo);
      const uint32_t last = SforGroupLowerBound(parsed, gr, boff, phi + 1);
      if (last > first) {
        const uint32_t rs = SforRowStart(parsed, gr);
        SvSetRangeRaw(out, rs + first, rs + last);
      }
      return true;
    }
    if (g_first_inside > g_end_inside) {
      return true;  // A > B + 1 is impossible; == B means a gap
    }
    if (g_first_inside > 0) {
      // Lower boundary group: its tail is fully inside (its bracket ends at
      // or below group g_first_inside's start <= phi); only the head needs
      // the window's lower edge.
      const uint32_t gr = g_first_inside - 1;
      const uint64_t boff = SforGroupOffset(parsed, gr);
      const uint32_t first = SforGroupLowerBound(parsed, gr, boff, plo);
      const uint32_t rs = SforRowStart(parsed, gr);
      const uint32_t rows = SforGroupRows(parsed, gr);
      if (first < rows) {
        SvSetRangeRaw(out, rs + first, rs + rows);
      }
    }
    if (g_end_inside > g_first_inside) {
      // Every group in [g_first_inside, g_end_inside) is whole-inside: one
      // contiguous row run. g_end_inside == ng means even the last group's
      // bracket ends at or below phi, so the run ends at the row count --
      // there is no directory entry ng to read.
      const uint32_t interior_end = g_end_inside == parsed.ng
                                        ? parsed.n
                                        : SforRowStart(parsed, g_end_inside);
      SvSetRangeRaw(out, SforRowStart(parsed, g_first_inside), interior_end);
    }
    if (g_end_inside < parsed.ng) {
      // Upper boundary group: from its head (wholly inside: glo >= plo) to
      // its last passing row.
      const uint32_t gr = g_end_inside;
      const uint64_t boff = SforGroupOffset(parsed, gr);
      const uint32_t last = SforGroupLowerBound(parsed, gr, boff, phi + 1);
      if (last > 0) {
        const uint32_t rs = SforRowStart(parsed, gr);
        SvSetRangeRaw(out, rs, rs + last);
      }
    }
    return true;
  }
  if (s == Scheme::kExpFor) {
    // EXPFOR is the general-double analogue of the kSfor brackets: each RUN
    // of same-band rows carries a bracket [(gkey<<52)|base, base + 2^w) that
    // is a ZONE MAP in value space -- independent of row order (no monotone
    // gate, the generalization kSfor's gate forbids). The directory is in
    // ROW ORDER and brackets of runs sharing a band OVERLAP, so the fold
    // scans it linearly: a run wholly outside the window costs zero reads,
    // one wholly inside collapses to a single SvSetRange, and only a
    // straddling run walks its rows -- at w bits per row instead of 8B
    // values. The literal rides the int64 machinery as raw bitcast bits
    // compared SIGNED, so the window maps to <= 2 key intervals (one per
    // sign side of zero) and every comparison happens in the key domain,
    // which is order-isomorphic to the doubles -- bit-for-bit agreement with
    // the generic path.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    ExpForRegion parsed;
    if (reg_end == nullptr ||
        !ParseExpForRegion(region, rc, f.params[off_idx], reg_end, &parsed)) {
      return false;
    }
    __int128 plo = 0, phi = 0;
    if (!InterpWindowForValue(static_cast<PaxChunk::CompareOp>(p.op), p.lit,
                              &plo, &phi)) {
      return false;
    }
    auto fold = [&](uint64_t klo, uint64_t khi) {
      // One linear pass with the payload offset ACCUMULATED: calling
      // ExpForGroupOffset per straddling run would rewalk the directory
      // every time -- O(ng^2) across a fold (the signed-osc bench caught
      // this at 8x decode cost before the accumulator).
      uint64_t boff = 0;
      for (uint32_t gr = 0; gr < parsed.ng; ++gr) {
        const uint64_t head =
            (ExpForGKey(parsed, gr) << 52) | ExpForBase(parsed, gr);
        const uint32_t rs = ExpForRowStart(parsed, gr);
        const uint32_t rows = ExpForGroupRows(parsed, gr);
        // The base is an arbitrary mantissa, NOT aligned to w bits (kSfor's
        // msb<<s is), so the loose upper bracket is base + 2^w - 1 by
        // ADDITION; base | mask would round the bound DOWN past rows the
        // run really holds.
        const uint64_t w = ExpForWidth(parsed, gr);
        const uint64_t bhi = head + (w == 0 ? 0 : ((UINT64_C(1) << w) - 1));
        if (bhi >= klo && head <= khi) {
          if (head >= klo && bhi <= khi) {
            SvSetRangeRaw(out, rs, rs + rows);
          } else {
            for (uint32_t t = 0; t < rows; ++t) {
              const uint64_t key = head + ExpForOffsetAt(parsed, gr, t, boff);
              if (key >= klo && key <= khi) {
                out[(rs + t) >> 6] |= 1ULL << ((rs + t) & 63U);
              }
            }
          }
        }
        boff += (static_cast<uint64_t>(rows) * w + 7) / 8;
      }
    };
    // Map the signed raw-bit window [plo, phi] to <= 2 key intervals.
    // Negative bits (unsigned >= 2^63) REFLECT under the transform
    // (key = ~bits); positive bits shift (key = bits ^ 2^63). A window
    // touching only one side of zero yields one interval.
    if (plo < 0) {
      const __int128 phi_neg = phi < -1 ? phi : -1;
      fold(~static_cast<uint64_t>(phi_neg), ~static_cast<uint64_t>(plo));
    }
    if (phi >= 0) {
      const __int128 plo_pos = plo > 0 ? plo : 0;
      fold(static_cast<uint64_t>(plo_pos) ^ UINT64_C(0x8000000000000000),
           static_cast<uint64_t>(phi) ^ UINT64_C(0x8000000000000000));
    }
    return true;
  }
  if (s == Scheme::kRleX) {
    // Run fold over inner-encoded streams: decode each inner region once,
    // then one SvSetRange per run (the kRleDict shape generalized).
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 18 > reg_end) {
      return false;
    }
    const auto vs = static_cast<uint8_t>(region[0]);
    const auto cs = static_cast<uint8_t>(region[1]);
    const uint32_t runs = ReadBE32(region + 2);
    const uint32_t vparam = ReadBE32(region + 6);
    const uint32_t cparam = ReadBE32(region + 10);
    const uint64_t vlen = ReadBE32(region + 14);
    if (runs != f.params[off_idx] || runs == 0 || runs > rc ||
        std::cmp_greater(vlen, reg_end - (region + 18)) || !InnerSchemeOk(vs) ||
        !InnerSchemeOk(cs)) {
      return false;
    }
    const char* vreg = region + 18;
    const char* creg = vreg + vlen;
    std::vector<int64_t> run_vals(runs, 0), run_lens(runs, 0);
    DecodeInnerI64All(vreg, vs, vparam, runs, creg, run_vals.data());
    DecodeInnerI64All(creg, cs, cparam, runs, reg_end, run_lens.data());
    uint64_t acc = 0;
    for (uint32_t r2 = 0; r2 < runs && acc < rc; ++r2) {
      const uint64_t next = acc + static_cast<uint64_t>(run_lens[r2]);
      const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
      if (end > acc && ScanMatch(run_vals[r2], p.op, p.lit)) {
        SvSetRangeRaw(out, static_cast<uint32_t>(acc), end);
      }
      acc = next;
    }
    // Tail rows past the run walk decode as raw 0 (same rule as kRle).
    if (acc < rc && ScanMatch(static_cast<int64_t>(0), p.op, p.lit)) {
      SvSetRangeRaw(out, static_cast<uint32_t>(acc), rc);
    }
    return true;
  }
  if (s == Scheme::kDictX) {
    // Pass table over the flat dictionary + one inner decode of the code
    // stream -- the kDict arm's shape with a cascaded code region.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 11 > reg_end) {
      return false;
    }
    const uint16_t d = ReadBE16(region);
    if (d == 0 || d != f.params[off_idx]) {
      return false;
    }
    const auto cs = static_cast<uint8_t>(region[2]);
    const uint32_t cparam = ReadBE32(region + 3);
    const uint64_t clen = ReadBE32(region + 7);
    const char* creg = region + 11;
    if (std::cmp_greater(clen, reg_end - creg) || !InnerSchemeOk(cs)) {
      return false;
    }
    const char* pool_hdr = creg + clen;
    if (pool_hdr + 1 > reg_end || static_cast<uint8_t>(pool_hdr[0]) != 0 ||
        pool_hdr + 1 + (static_cast<size_t>(d) * 8) > reg_end) {
      return false;
    }
    const char* dict = pool_hdr + 1;
    std::array<uint8_t, 1024> pass_stack{};
    std::vector<uint8_t> pass_heap;
    uint8_t* pass = nullptr;
    if (static_cast<size_t>(d) + 1 <= sizeof(pass_stack)) {
      pass = pass_stack.data();
      std::fill(pass, pass + static_cast<size_t>(d) + 1, 0);
    } else {
      pass_heap.assign(static_cast<size_t>(d) + 1, 0);
      pass = pass_heap.data();
    }
    for (uint16_t e = 0; e < d; ++e) {
      pass[e] = ScanMatch(static_cast<int64_t>(
                              ReadBE64(dict + (static_cast<size_t>(e) * 8))),
                          p.op, p.lit)
                    ? 1
                    : 0;
    }
    pass[d] = ScanMatch(static_cast<int64_t>(0), p.op, p.lit)
                  ? 1
                  : 0;  // clamp bucket
    std::vector<int64_t> codes(rc, 0);
    DecodeInnerI64All(creg, cs, cparam, rc, pool_hdr, codes.data());
    for (uint32_t i = 0; i < rc; ++i) {
      const auto code = static_cast<uint64_t>(codes[i]);
      if (pass[code < d ? code : d] != 0) {
        out[i >> 6] |= 1ULL << (i & 63);
      }
    }
    return true;
  }
  if (s == Scheme::kSparse) {
    // O(nnz) mask: every unlisted row is null and decodes as raw 0, so the
    // mask starts at the match(0) verdict for the whole group, then each
    // listed (position, value) pair overwrites its row's bit.
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 14 > reg_end) {
      return false;
    }
    const auto vs = static_cast<uint8_t>(region[0]);
    const uint32_t vprm = ReadBE32(region + 1);
    const uint32_t pbpv = static_cast<uint8_t>(region[9]);
    const uint32_t nnz = ReadBE32(region + 10);
    if (nnz != f.params[off_idx] || nnz == 0 || nnz > rc || pbpv < 1 ||
        pbpv > 8 || !InnerSchemeOk(vs)) {
      return false;
    }
    const size_t pos_bytes = 8 + (static_cast<size_t>(nnz) * pbpv);
    if (region + 14 + pos_bytes > reg_end) {
      return false;
    }
    const char* posr = region + 14;
    const char* vreg = posr + pos_bytes;
    if (ScanMatch(0, p.op, p.lit)) {
      SvSetRangeRaw(out, 0, rc);
    }
    for (uint32_t r = 0; r < nnz; ++r) {
      const int64_t pv =
          Int64RegionAt(posr, Scheme::kForBp, pbpv, nnz, r, reg_end);
      if (pv < 0 || std::cmp_greater_equal(pv, rc)) {
        continue;  // hostile position: skip the row
      }
      const int64_t v =
          Int64RegionAt(vreg, static_cast<Scheme>(vs), vprm, nnz, r, reg_end);
      if (ScanMatch(v, p.op, p.lit)) {
        out[static_cast<uint32_t>(pv) >> 6] |=
            1ULL << (static_cast<uint32_t>(pv) & 63);
      } else {
        out[static_cast<uint32_t>(pv) >> 6] &=
            ~(1ULL << (static_cast<uint32_t>(pv) & 63));
      }
    }
    return true;
  }
  if (s == Scheme::kOpRle) {
    // Code-window fold: the sorted dict maps the predicate to a code
    // range [lo, hi) with two binary searches -- no pass table (the whole
    // point of the scheme vs kRleDict's d-entry scan).
    const char* reg_end = RegionEndAt(f, g, p.col, chunk.Data());
    if (reg_end == nullptr || region + 2 > reg_end) {
      return false;
    }
    const uint16_t d = ReadBE16(region);
    if (d == 0) {
      return false;
    }
    const int cb = (BitsNeeded(d - 1) + 7) / 8;
    const char* dict = region + 2;
    const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
    const uint32_t runs = f.params[off_idx];
    if (runs_hdr + 4 > reg_end || ReadBE32(runs_hdr) != runs ||
        runs_hdr + 4 +
                (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
            reg_end) {
      return false;
    }
    const auto dict_at = [&](uint32_t e) {
      return static_cast<int64_t>(
          ReadBE64(dict + (static_cast<size_t>(e) * 8)));
    };
    // lower_bound / upper_bound over the BE64 dictionary entries.
    const auto lb = [&](int64_t key) {  // first e with dict[e] >= key
      uint32_t l = 0, h = d;
      while (l < h) {
        const uint32_t mid = l + ((h - l) / 2);
        if (dict_at(mid) < key) {
          l = mid + 1;
        } else {
          h = mid;
        }
      }
      return l;
    };
    const auto ub = [&](int64_t key) {  // first e with dict[e] > key
      uint32_t l = 0, h = d;
      while (l < h) {
        const uint32_t mid = l + ((h - l) / 2);
        if (key < dict_at(mid)) {
          h = mid;
        } else {
          l = mid + 1;
        }
      }
      return l;
    };
    uint32_t lo = 0, hi = d;  // code window [lo, hi)
    switch (p.op) {
      case ScanOp::kEq: {
        const uint32_t it = lb(p.lit);
        const bool found = it < d && dict_at(it) == p.lit;
        lo = it;
        hi = found ? lo + 1 : lo;
        break;
      }
      case ScanOp::kLe:
        hi = ub(p.lit);
        break;
      case ScanOp::kLt:
        hi = lb(p.lit);
        break;
      case ScanOp::kGe:
        lo = lb(p.lit);
        break;
      case ScanOp::kGt:
        lo = ub(p.lit);
        break;
      default:
        return false;  // hostile op: fail closed like ScanMatch
    }
    const bool zero_pass = ScanMatch(static_cast<int64_t>(0), p.op, p.lit);
    uint64_t acc = 0;
    const char* q = runs_hdr + 4;
    for (uint32_t r = 0; r < runs && acc < rc; ++r) {
      uint64_t code = 0;
      for (int b = 0; b < cb; ++b) {
        code = (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
      }
      const uint64_t len = ReadBE32(q + cb);
      const uint64_t next = acc + len;
      const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
      const bool hit = (code >= lo && code < hi) || (code >= d && zero_pass);
      if (end > acc && hit) {
        SvSetRangeRaw(out, static_cast<uint32_t>(acc), end);
      }
      acc = next;
      q += static_cast<size_t>(cb) + 4;
    }
    // Rows past the run walk decode as raw 0 (kRle parity).
    if (acc < rc && zero_pass) {
      SvSetRangeRaw(out, static_cast<uint32_t>(acc), rc);
    }
    return true;
  }
  return false;
}

// Fuse `preds` into a single selection bitmask over row group `g`. When
// `all_inside` is true (the caller has proven via zone maps that the whole
// group satisfies every predicate) or `preds` is empty, every row is selected
// without decoding any column. Per predicate column: the compressed-domain
// path (TryFastPredicateMask) fills each predicate's mask without decoding
// rows; predicates without a fast kernel decode the column once and compare
// per row. Masks AND across predicates and columns.
}  // namespace

namespace {
constexpr size_t kSelStackWords = 16;  // covers kRowGroupSize (1024) rows

// Word-count fill for the selection masks: these buffers are tiny (<=16
// words for a full row group), where std::fill expands to a `rep stosq`
// whose setup cost dominates. Explicit 4-word vector stores keep it to a
// couple of instructions per group; the scalar tail covers short masks.
inline void FillWords(uint64_t* p, size_t n, uint64_t v) {
#ifdef __AVX2__
  const __m256i vv = _mm256_set1_epi64x(static_cast<long long>(v));
  size_t i = 0;
  for (; i + 4 <= n; i += 4) {
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(p + i), vv);
  }
  for (; i < n; ++i) {
    p[i] = v;
  }
#else
  for (size_t i = 0; i < n; ++i) {
    p[i] = v;
  }
#endif
}

// Bit-mask scratch for the selection path: stack-backed for ordinary row
// groups, heap only for oversized (hand-built) footers. Avoids up to four
// malloc/free pairs per BuildSelectionVector call.
class SelWordBuf {
 public:
  SelWordBuf(size_t words, uint64_t fill) : heap_(words > kSelStackWords) {
    if (heap_) {
      heapv_.resize(words);
      FillWords(heapv_.data(), words, fill);
    } else {
      n_ = words;
      FillWords(stack_.data(), words, fill);
    }
  }
  uint64_t* data() { return heap_ ? heapv_.data() : stack_.data(); }
  [[nodiscard]] const uint64_t* data() const {
    return heap_ ? heapv_.data() : stack_.data();
  }
  [[nodiscard]] size_t size() const { return heap_ ? heapv_.size() : n_; }

 private:
  bool heap_;
  size_t n_ = 0;
  std::array<uint64_t, kSelStackWords> stack_ = {};
  std::vector<uint64_t> heapv_;
};
}  // namespace

PaxSelectionVector BuildSelectionVector(const PaxChunk& chunk,
                                        const std::vector<Predicate>& preds,
                                        uint32_t g, bool all_inside) {
  PaxSelectionVector sv;
  const PaxChunk::Footer& f = chunk.GetFooter();
  // Public API: g and the per-cell footer arrays are caller/hand-built
  // footer controlled; out-of-range values read out of bounds before any
  // row work. An unusable cell selects nothing.
  if (g >= f.row_group_count || g >= f.row_counts.size() ||
      (static_cast<size_t>(g) * f.column_count) + f.column_count >
          f.column_offsets.size() ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return sv;
  }
  const uint16_t rc = f.row_counts[g];
  const auto nwords = static_cast<size_t>((rc + 63U) / 64U);
  sv.bits.resize(nwords);  // new storage: zero-init compiles to memset
  if (preds.empty() || all_inside) {
    // All rows inside: fill whole words, then the partial tail word.
    FillWords(sv.bits.data(), static_cast<size_t>(rc) >> 6, ~0ULL);
    if ((rc & 63U) != 0U) {
      sv.bits[static_cast<size_t>(rc) >> 6] = (1ULL << (rc & 63U)) - 1ULL;
    }
    sv.count = rc;
    return sv;
  }
  std::vector<slot_t> pcols;
  for (const auto& p : preds) {
    if (std::find(pcols.begin(), pcols.end(), p.col) == pcols.end()) {
      pcols.push_back(p.col);
    }
  }
  std::sort(pcols.begin(), pcols.end());

  // AND one per-column mask per predicate column into the final selection.
  SelWordBuf acc(nwords, ~0ULL);
  if ((rc & 63U) != 0U) {  // clear bits >= rc in the last word
    acc.data()[rc >> 6] = (1ULL << (rc & 63U)) - 1ULL;
  }
  // Hoisted once: the AND/popcount passes below run per row group, and
  // re-evaluating the stack-vs-heap branch in every word was measurable.
  for (slot_t col : pcols) {
    // A predicate on a column outside the chunk's footer is unusable data:
    // fail closed (empty selection) instead of indexing encodings/params
    // past the per-cell arrays or throwing out of DecodeInt64ColumnRg.
    if (col >= f.column_count) {
      sv.bits.assign(nwords, 0);
      sv.count = 0;
      return sv;
    }
    size_t npred = 0;
    for (const auto& p : preds) {
      if (p.col == col) {
        ++npred;
      }
    }
    // A column with a single predicate (the common case) fuses the kernel
    // output straight into `acc`, saving a whole nwords fill plus a second
    // AND pass per column per row group. Two or more predicates on one
    // column still need a per-column accumulator to AND them together.
    SelWordBuf cm_buf(nwords, ~0ULL);
    uint64_t* cm_p = npred > 1 ? cm_buf.data() : nullptr;
    uint64_t* acc_p = acc.data();
    if (cm_p != nullptr && ((rc & 63U) != 0U)) {
      cm_p[(static_cast<size_t>(rc) - 1) >> 6] &= (1ULL << (rc & 63U)) - 1ULL;
    }
    // Decoded-values scratch for THIS column only: it must be rebuilt per
    // column, or every later predicate would compare against the first
    // column's values (the col-4 range predicates silently evaluated on
    // col-1 data and zeroed the whole selection).
    std::array<int64_t, PaxChunk::kRowGroupSize> dec_stack{};
    std::vector<int64_t> dec_heap;
    int64_t* decoded = nullptr;
    for (const auto& p : preds) {
      if (p.col != col) {
        continue;
      }
      SelWordBuf pm(nwords, 0);
      uint64_t* pm_p = pm.data();
      if (!TryFastPredicateMask(chunk, p, g, pm_p)) {
        // Generic fallback: decode the column once per row group and compare
        // per row (TryFastPredicateMask declined: no compressed kernel for
        // this scheme/op, or a hostile region made the fast path unsafe).
        if (decoded == nullptr) {
          if (rc <= PaxChunk::kRowGroupSize) {
            decoded = dec_stack.data();
          } else {
            dec_heap.resize(rc);
            decoded = dec_heap.data();
          }
          if (rc > 0) {
            chunk.DecodeInt64ColumnRg(col, g, decoded);
          }
        }
        if (rc > 0) {
#ifdef __AVX2__
          if (!FillPredicateMaskAvx2(decoded, rc, p.op, p.lit, pm_p)) {
#endif
            for (uint16_t i = 0; i < rc; ++i) {
              if (ScanMatch(decoded[i], p.op, p.lit)) {
                pm_p[i >> 6] |= 1ULL << (i & 63);
              }
            }
#ifdef __AVX2__
          }
#endif
        }
      }
      uint64_t* dst = cm_p != nullptr ? cm_p : acc_p;
      for (size_t w = 0; w < nwords; ++w) {
        dst[w] &= pm_p[w];
      }
    }
    if (cm_p != nullptr) {
      for (size_t w = 0; w < nwords; ++w) {
        acc_p[w] &= cm_p[w];
      }
    }
  }
  const uint64_t* acc_p = acc.data();
  uint64_t total = 0;
  for (size_t w = 0; w < nwords; ++w) {
    total += static_cast<uint64_t>(__builtin_popcountll(acc_p[w]));
  }
  sv.bits.resize(nwords);  // already sized above: no reallocation, no fill
  // nwords == 0 (a zero-row row group) leaves acc_p and sv.bits.data() null,
  // and memcpy with a null argument is UB even for a zero byte count -- the
  // footer-section fuzzer reported exactly this as "null pointer passed as
  // argument 1, which is declared to never be null" here.
  if (nwords != 0) {
    std::memcpy(sv.bits.data(), acc_p, nwords * sizeof(uint64_t));
  }
  sv.count = total;
  return sv;
}

AggResult PaxChunk::ScanAggregate(const PaxChunk& chunk, const AggPlan& plan,
                                  bool parallel, int nthreads,
                                  uint32_t scan_sweeps, const Schema* sc,
                                  WorkloadTracker* workload) {
  // Same clamp as VectorizedScanColumn / ReduceColumn: 0 sweeps must not
  // short-circuit into an empty AggResult.
  scan_sweeps = std::max<uint32_t>(scan_sweeps, 1);
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint32_t rg = f.row_group_count;
  const uint16_t cc = f.column_count;

  // Hand-built-footer guard: the group loops index row_counts[g] and the
  // per-cell arrays; declared-but-absent arrays would read out of bounds.
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return AggResult{};
  }
  // Public API guard: predicate/term columns index all_zone_mins and the
  // region decoder below; an out-of-range column read out of bounds.
  for (const auto& p : plan.preds) {
    if (p.col >= cc) {
      return AggResult{};
    }
  }
  for (const auto& t : plan.terms) {
    for (slot_t c : t.cols) {
      if (c >= cc) {
        return AggResult{};
      }
    }
  }
  // With a schema supplied, enforce the int64-only decode contract on
  // every referenced column (non-int64 regions decode to plausible
  // garbage rather than an error).
  if (sc != nullptr) {
    const auto i64 = [sc](slot_t c) {
      return c < sc->ColumnCount() &&
             sc->GetColumn(c).Type() == ValueType::kInt64;
    };
    for (const auto& p : plan.preds) {
      if (!i64(p.col)) {
        return AggResult{};
      }
    }
    for (const auto& t : plan.terms) {
      for (slot_t c : t.cols) {
        if (!i64(c)) {
          return AggResult{};
        }
      }
    }
  }
  if (workload != nullptr && sc != nullptr) {
    for (const auto& p : plan.preds) {
      workload->RecordFilter(*sc, p.col);
    }
    for (const auto& t : plan.terms) {
      for (slot_t c : t.cols) {
        workload->RecordAggregate(*sc, c);
      }
    }
  }

  // Metadata fast paths (no predicates): reuse footer statistics.
  if (plan.preds.empty()) {
    if (plan.count_only || plan.terms.empty()) {
      AggResult r;
      r.count = chunk.TotalRowCount();
      return r;
    }
    if (plan.terms.size() == 1 && plan.terms[0].coeff == 1 &&
        plan.terms[0].cols.size() == 1 && plan.out_scale == 0) {
      int64_t s = 0;
      uint64_t c = 0;
      if (FastColumnSum(chunk, plan.terms[0].cols[0], &s, &c)) {
        AggResult r;
        r.sum = s;
        r.count = c;
        return r;
      }
      // Fall through to the general path if stats are unavailable.
    }
  }

  auto agg_rg = [&](uint32_t g, AggResult& r) {
    const uint16_t rc = f.row_counts[g];

    // Zone-map evaluation: prune the whole group if any predicate provably
    // cannot match, and detect the "fully inside" case (every row provably
    // satisfies every predicate) so predicate columns need not be decoded.
    const bool has_preds = !plan.preds.empty();
    // Without zone maps we cannot prove anything, so we must decode and
    // evaluate every row (all_inside stays false). Defaulting it to true on a
    // footer lacking zone maps would silently ignore the WHERE clause.
    bool all_inside = has_preds && f.all_zm_present;
    if (has_preds && f.all_zm_present) {
      for (const auto& p : plan.preds) {
        int64_t plo = INT64_MIN, phi = INT64_MAX;
        bool range_empty = false;
        switch (p.op) {
          case ScanOp::kEq:
            plo = phi = p.lit;
            break;
          case ScanOp::kLe:
            phi = p.lit;
            break;
          case ScanOp::kLt:
            if (p.lit == INT64_MIN) {
              range_empty = true;  // v < INT64_MIN matches nothing
            } else {
              phi = p.lit - 1;
            }
            break;
          case ScanOp::kGe:
            plo = p.lit;
            break;
          case ScanOp::kGt:
            if (p.lit == INT64_MAX) {
              range_empty = true;  // v > INT64_MAX matches nothing
            } else {
              plo = p.lit + 1;
            }
            break;
          default:
            // Unknown op: cannot prove the range. Do not prune, and do not
            // claim "fully inside" -- force a per-row decode + evaluation.
            all_inside = false;
            continue;
        }
        if (range_empty) {
          return;  // this predicate kills the whole group
        }
        const size_t zi = (static_cast<size_t>(g) * cc) + p.col;
        if (zi >= f.all_zone_mins.size() || zi >= f.all_zone_maxs.size()) {
          all_inside = false;
          continue;
        }
        // A group with nulls in the predicate column cannot be pruned on the
        // non-null zone map (NULL cells decode as raw 0 and can match), nor
        // proven "fully inside" (nulls are invisible to the zone map but
        // real rows here).
        const bool col_has_null = RgColHasNull(f, g, p.col);
        if (!col_has_null &&
            (f.all_zone_mins[zi] > phi || f.all_zone_maxs[zi] < plo)) {
          return;
        }
        if (f.all_zone_mins[zi] < plo || f.all_zone_maxs[zi] > phi) {
          all_inside = false;
        }
        if (col_has_null) {
          all_inside = false;
        }
      }
    }

    // M2: fuse all predicates into one selection vector (decoding only the
    // predicate columns), then materialize term columns lazily -- only for the
    // rows that survived the filter.
    PaxSelectionVector sel =
        BuildSelectionVector(chunk, plan.preds, g, all_inside);
    r.count += sel.count;
    if (plan.count_only || plan.terms.empty()) {
      return;
    }
    if (sel.count == 0) {
      return;  // late materialization: nothing to aggregate
    }

    std::vector<slot_t> term_cols;
    for (const auto& t : plan.terms) {
      for (slot_t c : t.cols) {
        if (std::find(term_cols.begin(), term_cols.end(), c) ==
            term_cols.end()) {
          term_cols.push_back(c);
        }
      }
    }
    std::sort(term_cols.begin(), term_cols.end());
    std::vector<std::vector<int64_t>> tbufs(term_cols.size());
    for (size_t i = 0; i < term_cols.size(); ++i) {
      tbufs[i].resize(rc);
      chunk.DecodeInt64ColumnRg(term_cols[i], g, tbufs[i].data());
    }
    auto tcol = [&](slot_t c) -> const std::vector<int64_t>& {
      size_t idx = static_cast<size_t>(
          std::lower_bound(term_cols.begin(), term_cols.end(), c) -
          term_cols.begin());
      return tbufs[idx];
    };

    // Accumulate only the selected rows (iterate set bits).
    const uint64_t nwords = sel.bits.size();
    for (uint64_t w = 0; w < nwords; ++w) {
      uint64_t bits = sel.bits[w];
      if (bits == 0) {
        continue;
      }
      const auto base = static_cast<uint16_t>(w * 64);
      while (bits) {
        int b = __builtin_ctzll(bits);
        bits &= bits - 1;
        uint16_t i = base + static_cast<uint16_t>(b);
        if (i >= rc) {
          break;
        }
        __int128 acc = 0;
        for (const auto& t : plan.terms) {
          __int128 term = t.coeff;
          for (slot_t c : t.cols) {
            term *= tcol(c)[i];
          }
          acc += term;
        }
        r.sum += acc;
      }
    }
  };

  AggResult total;
  if (parallel && rg > 1 && scan_sweeps > 0) {
    if (nthreads <= 0) {
      nthreads = static_cast<int>(std::thread::hardware_concurrency());
    }
    nthreads = std::min(nthreads, static_cast<int>(rg));
    nthreads = std::max(nthreads, 1);
    std::vector<std::thread> ths;
    ths.reserve(static_cast<size_t>(nthreads));
    std::vector<AggResult> res(static_cast<size_t>(nthreads));
    // Same contract as VectorizedScanColumn: decode errors on hostile
    // footers surface as exceptions; catch into the worker's own slot (no
    // shared exception_ptr: two simultaneous throwers would race) and
    // rethrow after the join instead of letting one escape into
    // std::terminate.
    std::vector<std::exception_ptr> worker_errors(
        static_cast<size_t>(nthreads));
    for (int t = 0; t < nthreads; ++t) {
      ths.emplace_back([&, t]() {
        try {
          for (uint32_t sw = 0; sw < scan_sweeps; ++sw) {
            for (auto g = static_cast<uint32_t>(t); g < rg;
                 g += static_cast<uint32_t>(nthreads)) {
              agg_rg(g, res[static_cast<size_t>(t)]);
            }
          }
        } catch (...) {
          worker_errors[static_cast<size_t>(t)] = std::current_exception();
        }
      });
    }
    for (auto& th : ths) {
      th.join();
    }
    for (const auto& ep : worker_errors) {
      if (ep != nullptr) {
        std::rethrow_exception(ep);
      }
    }
    for (auto& r : res) {
      total.sum += r.sum;
      total.count += r.count;
    }
  } else {
    for (uint32_t sw = 0; sw < scan_sweeps; ++sw) {
      for (uint32_t g = 0; g < rg; ++g) {
        agg_rg(g, total);
      }
    }
  }
  return total;
}

std::vector<GroupByEntry> GroupBySumColumn(const PaxChunk& chunk,
                                           slot_t group_col, slot_t sum_col,
                                           const Schema* sc,
                                           WorkloadTracker* workload) {
  std::vector<GroupByEntry> out;
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint16_t cc = f.column_count;
  if (group_col >= cc || sum_col >= cc) {
    return out;
  }
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return out;
  }
  if (sc != nullptr) {
    const auto i64 = [sc](slot_t c) {
      return c < sc->ColumnCount() &&
             sc->GetColumn(c).Type() == ValueType::kInt64;
    };
    if (!i64(group_col) || !i64(sum_col)) {
      return out;
    }
  }
  if (workload != nullptr && sc != nullptr) {
    workload->RecordAggregate(*sc, group_col);
    workload->RecordAggregate(*sc, sum_col);
  }
  std::unordered_map<int64_t, size_t> index;
  std::vector<__int128> sums;
  const auto emit = [&](int64_t key, int64_t sum, uint64_t cnt) {
    if (cnt == 0) {
      return;
    }
    auto it = index.find(key);
    if (it == index.end()) {
      const size_t pos = out.size();
      index.emplace(key, pos);
      out.push_back(GroupByEntry{.key = key, .sum = 0, .count = 0});
      sums.push_back(0);
      out[pos].sum = 0;
      sums[pos] += sum;
      out[pos].count += cnt;
    } else {
      sums[it->second] += sum;
      out[it->second].count += cnt;
    }
  };

  std::vector<int64_t> sbuf;
  std::vector<uint32_t> gcodes;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const uint16_t rc = f.row_counts[g];
    if (rc == 0) {
      continue;
    }
    const uint32_t goff = (g * cc) + group_col;
    const uint32_t soff = (g * cc) + sum_col;
    const auto gs = static_cast<Scheme>(f.encodings[goff]);
    const auto ss = static_cast<Scheme>(f.encodings[soff]);
    // Validate the offset VALUEs before forming the pointers at all (the
    // array-size gate above only proves the entries exist). A hostile
    // offset makes the whole group unusable: skip it, like the other
    // hand-built-footer guards here.
    const char* gr = RegionStartAt(f, g, group_col, chunk.Data(), 0);
    const char* sr = RegionStartAt(f, g, sum_col, chunk.Data(), 0);
    if (gr == nullptr || sr == nullptr) {
      continue;
    }
    const char* gend = RegionEndAt(f, g, group_col, chunk.Data());
    const char* send = RegionEndAt(f, g, sum_col, chunk.Data());

    // Materialize the sum column once per group unless it folds
    // arithmetically below (CONST/RLE/DICT). sbuf[i] holds row i's value.
    sbuf.clear();
    bool sum_ready = false;
    int64_t const_sum = 0;
    std::vector<std::pair<int64_t, uint32_t>> sum_runs;
    std::vector<uint64_t> sum_freq;
    const char* sum_dict = nullptr;
    uint16_t sum_ncodes = 0;
    if (ss == Scheme::kConst && send != nullptr &&
        sr + static_cast<size_t>(8) <= send) {
      const_sum = static_cast<int64_t>(ReadBE64(sr));
      sum_ready = true;
    } else if (ss == Scheme::kRle && send != nullptr &&
               sr + 4 + (static_cast<size_t>(f.params[soff]) * 12) <= send) {
      const char* q = sr + 4;
      uint32_t acc = 0;
      bool ok = true;
      for (uint32_t r = 0; r < f.params[soff] && acc < rc; ++r) {
        const uint32_t len = ReadBE32(q + 8);
        if (len == 0 || acc + len < acc) {
          ok = false;
          break;
        }
        sum_runs.emplace_back(static_cast<int64_t>(ReadBE64(q)),
                              std::min<uint32_t>(acc + len, rc) - acc);
        acc += len;
        q += 12;
      }
      sum_ready = ok && acc >= rc;
      if (!sum_ready) {
        sum_runs.clear();
      }
    } else if ((ss == Scheme::kRleDict || ss == Scheme::kOpRle) &&
               send != nullptr && send - sr >= 2) {
      // Same sum_runs materialization as kRle above, but each run's value
      // comes from the dictionary (clamp codes decode as raw 0). Downstream
      // arms consume sum_runs unchanged. kOpRle shares the byte layout.
      const uint16_t d = ReadBE16(sr);
      const int cb = (BitsNeeded(d > 0 ? d - 1 : 0) + 7) / 8;
      const char* dict = sr + 2;
      const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
      const uint32_t runs = f.params[soff];
      if (d > 0 && runs_hdr + 4 <= send && ReadBE32(runs_hdr) == runs &&
          runs_hdr + 4 +
                  (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) <=
              send) {
        const char* q = runs_hdr + 4;
        uint64_t acc = 0;
        bool ok = true;
        for (uint32_t r = 0; r < runs && acc < rc; ++r) {
          uint64_t code = 0;
          for (int b = 0; b < cb; ++b) {
            code =
                (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
          }
          const uint64_t len = ReadBE32(q + cb);
          if (len == 0 || acc + len < acc) {
            ok = false;
            break;
          }
          const int64_t v =
              (code < d) ? static_cast<int64_t>(
                               ReadBE64(dict + (static_cast<size_t>(code) * 8)))
                         : 0;
          sum_runs.emplace_back(
              v,
              static_cast<uint32_t>(std::min<uint64_t>(acc + len, rc) - acc));
          acc += len;
          q += static_cast<size_t>(cb) + 4;
        }
        sum_ready = ok && acc >= rc;
        if (!sum_ready) {
          sum_runs.clear();
        }
      }
    } else if ((ss == Scheme::kDict || ss == Scheme::kOpDict) &&
               send != nullptr &&
               DictCodeHistogram(sr, rc, f.params[soff], send, &sum_freq)) {
      sum_ncodes = ReadBE16(sr);
      const int cw = BitsNeeded(sum_ncodes - 1);
      sum_dict = sr + 2 +
                 ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
      sum_ready = true;
    }
    // Decode fallback for other sum schemes (or failed fast paths).
    if (!sum_ready && ss != Scheme::kConst && ss != Scheme::kRle &&
        ss != Scheme::kDict && ss != Scheme::kOpDict) {
      sbuf.assign(rc, 0);
      chunk.DecodeInt64ColumnRg(sum_col, g, sbuf.data());
      sum_ready = true;
    }

    if (gs == Scheme::kConst && gend != nullptr && gr + 8 <= gend) {
      // One group for the whole row group.
      const auto key = static_cast<int64_t>(ReadBE64(gr));
      if (ss == Scheme::kConst && sum_ready) {
        // __int128 multiply like the kRle arm below: const_sum * rc wraps
        // int64 for |const_sum| >= 2^53 with a full 1024-row group (scaled
        // decimal data), and signed overflow is UB under UBSan.
        const __int128 s =
            static_cast<__int128>(const_sum) * static_cast<__int128>(rc);
        emit(key, static_cast<int64_t>(s), rc);
        continue;
      }
      if (ss == Scheme::kRle && sum_ready && !sum_runs.empty()) {
        __int128 s = 0;
        for (const auto& [v, len] : sum_runs) {
          s += static_cast<__int128>(v) * len;
        }
        emit(key, static_cast<int64_t>(s), rc);
        continue;
      }
      if ((ss == Scheme::kDict || ss == Scheme::kOpDict) && sum_ready) {
        int64_t s = 0;
        if (DictColumnSum(sr, rc, f.params[soff], send, &s)) {
          emit(key, s, rc);
          continue;
        }
      }
      if (!sum_ready || sbuf.empty()) {
        sbuf.assign(rc, 0);
        chunk.DecodeInt64ColumnRg(sum_col, g, sbuf.data());
      }
      __int128 s = 0;
      for (int64_t v : sbuf) {
        s += v;
      }
      emit(key, static_cast<int64_t>(s), rc);
      continue;
    }
    if ((gs == Scheme::kDict || gs == Scheme::kOpDict) && gend != nullptr) {
      // Histogram group codes once; fold the sum column per code.
      std::vector<uint64_t> freq;
      if (DictCodeHistogram(gr, rc, f.params[goff], gend, &freq)) {
        const uint16_t ncodes = ReadBE16(gr);
        const int cw = BitsNeeded(ncodes - 1);
        const char* dict =
            gr + 2 +
            ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8);
        // Row-major code extraction for per-row sum folding.
        gcodes.assign(rc, ncodes);
        WordBitReader br(
            gr + 2,
            gr + 2 +
                ((static_cast<size_t>(rc) * static_cast<size_t>(cw) + 7) / 8));
        for (uint32_t i = 0; i < rc; ++i) {
          const uint64_t c = br.get(cw);
          gcodes[i] = static_cast<uint32_t>(c < ncodes ? c : ncodes);
        }
        std::vector<__int128> acc(static_cast<size_t>(ncodes) + 1, 0);
        std::vector<uint64_t> cnt(static_cast<size_t>(ncodes) + 1, 0);
        if (ss == Scheme::kConst && sum_ready) {
          for (uint32_t i = 0; i < rc; ++i) {
            acc[gcodes[i]] += const_sum;
            ++cnt[gcodes[i]];
          }
        } else if (ss == Scheme::kRle && sum_ready && !sum_runs.empty()) {
          uint32_t pos = 0;
          for (const auto& [v, len] : sum_runs) {
            for (uint32_t k = 0; k < len && pos < rc; ++k, ++pos) {
              acc[gcodes[pos]] += v;
              ++cnt[gcodes[pos]];
            }
          }
        } else if ((ss == Scheme::kDict || ss == Scheme::kOpDict) &&
                   sum_ready) {
          const int scw = BitsNeeded(sum_ncodes - 1);
          WordBitReader sbr(sr + 2, sum_dict);
          for (uint32_t i = 0; i < rc; ++i) {
            const uint64_t scode = sbr.get(scw);
            const int64_t v =
                (scode < sum_ncodes)
                    ? static_cast<int64_t>(
                          ReadBE64(sum_dict + (static_cast<size_t>(scode) * 8)))
                    : 0;
            acc[gcodes[i]] += v;
            ++cnt[gcodes[i]];
          }
        } else {
          if (!sum_ready || sbuf.empty()) {
            sbuf.assign(rc, 0);
            chunk.DecodeInt64ColumnRg(sum_col, g, sbuf.data());
          }
          for (uint32_t i = 0; i < rc; ++i) {
            acc[gcodes[i]] += sbuf[i];
            ++cnt[gcodes[i]];
          }
        }
        for (uint32_t d = 0; d <= ncodes; ++d) {
          if (cnt[d] == 0) {
            continue;
          }
          const int64_t key =
              (d < ncodes) ? static_cast<int64_t>(
                                 ReadBE64(dict + (static_cast<size_t>(d) * 8)))
                           : 0;  // clamped codes decode to raw 0
          emit(key, static_cast<int64_t>(acc[d]), cnt[d]);
        }
        continue;
      }
      // else fall through to generic
    }
    if (gs == Scheme::kRle && gend != nullptr) {
      const uint32_t runs = f.params[goff];
      if (gr + 4 + (static_cast<size_t>(runs) * 12) <= gend) {
        // Expand the sum column once (runs/code passes are O(n) narrow
        // ops), then fold per group run with zero hashing.
        if (sbuf.empty()) {
          sbuf.assign(rc, 0);
          if (ss == Scheme::kConst && sum_ready) {
            std::fill(sbuf.begin(), sbuf.end(), const_sum);
          } else if (ss == Scheme::kRle && sum_ready && !sum_runs.empty()) {
            uint32_t pos = 0;
            for (const auto& [v, len] : sum_runs) {
              for (uint32_t k = 0; k < len && pos < rc; ++k, ++pos) {
                sbuf[pos] = v;
              }
            }
          } else if ((ss == Scheme::kDict || ss == Scheme::kOpDict) &&
                     sum_ready) {
            const int scw = BitsNeeded(sum_ncodes - 1);
            WordBitReader sbr(sr + 2, sum_dict);
            for (uint32_t i = 0; i < rc; ++i) {
              const uint64_t scode = sbr.get(scw);
              sbuf[i] = (scode < sum_ncodes)
                            ? static_cast<int64_t>(ReadBE64(
                                  sum_dict + (static_cast<size_t>(scode) * 8)))
                            : 0;
            }
          } else {
            chunk.DecodeInt64ColumnRg(sum_col, g, sbuf.data());
          }
        }
        const char* q = gr + 4;
        uint32_t pos = 0;
        for (uint32_t r = 0; r < runs && pos < rc; ++r) {
          const auto key = static_cast<int64_t>(ReadBE64(q));
          // u64 extent math: a hostile len wrapped pos+len in uint32, so
          // `end < pos` regressed the walk and emitted a ~4-billion count.
          const uint64_t next = static_cast<uint64_t>(pos) + ReadBE32(q + 8);
          const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
          __int128 s = 0;
          for (uint32_t i = pos; i < end; ++i) {
            s += sbuf[i];
          }
          emit(key, static_cast<int64_t>(s), end - pos);
          pos = end;
          q += 12;
        }
        // Truncated region tail: Int64RegionAt decodes rows past the run
        // walk as raw 0, so group them under key 0 exactly like the generic
        // path would (they used to vanish from the group-by output).
        for (; pos < rc; ++pos) {
          emit(0, sbuf[pos], 1);
        }
        continue;
      }
      // else fall through to generic
    }
    if ((gs == Scheme::kRleDict || gs == Scheme::kOpRle) && gend != nullptr &&
        gend - gr >= 2) {
      // Same shape as the kRle arm above: expand the sum column once, then
      // fold per group run with zero hashing. The run's group key is
      // dict[code] (clamp codes decode as raw 0, mirroring every reader).
      // kOpRle shares the byte layout.
      const uint16_t gd = ReadBE16(gr);
      const int gcb = (BitsNeeded(gd > 0 ? gd - 1 : 0) + 7) / 8;
      const char* gdict = gr + 2;
      const char* gruns_hdr = gdict + (static_cast<size_t>(gd) * 8);
      const uint32_t gruns = f.params[goff];
      if (gd > 0 && gruns_hdr + 4 <= gend && ReadBE32(gruns_hdr) == gruns &&
          gruns_hdr + 4 +
                  (static_cast<size_t>(gruns) *
                   (static_cast<size_t>(gcb) + 4)) <=
              gend) {
        if (sbuf.empty()) {
          sbuf.assign(rc, 0);
          if (ss == Scheme::kConst && sum_ready) {
            std::fill(sbuf.begin(), sbuf.end(), const_sum);
          } else if (ss == Scheme::kRle && sum_ready && !sum_runs.empty()) {
            uint32_t pos = 0;
            for (const auto& [v, len] : sum_runs) {
              for (uint32_t k = 0; k < len && pos < rc; ++k, ++pos) {
                sbuf[pos] = v;
              }
            }
          } else if ((ss == Scheme::kDict || ss == Scheme::kOpDict) &&
                     sum_ready) {
            const int scw = BitsNeeded(sum_ncodes - 1);
            WordBitReader sbr(sr + 2, sum_dict);
            for (uint32_t i = 0; i < rc; ++i) {
              const uint64_t scode = sbr.get(scw);
              sbuf[i] = (scode < sum_ncodes)
                            ? static_cast<int64_t>(ReadBE64(
                                  sum_dict + (static_cast<size_t>(scode) * 8)))
                            : 0;
            }
          } else {
            chunk.DecodeInt64ColumnRg(sum_col, g, sbuf.data());
          }
        }
        const char* q = gruns_hdr + 4;
        uint64_t pos = 0;
        for (uint32_t r = 0; r < gruns && pos < rc; ++r) {
          uint64_t code = 0;
          for (int b = 0; b < gcb; ++b) {
            code =
                (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
          }
          const uint64_t len = ReadBE32(q + gcb);
          const uint64_t next = pos + len;
          const uint32_t end = (next > rc) ? rc : static_cast<uint32_t>(next);
          const int64_t key =
              (code < gd) ? static_cast<int64_t>(ReadBE64(
                                gdict + (static_cast<size_t>(code) * 8)))
                          : 0;
          __int128 s = 0;
          for (auto i = static_cast<uint32_t>(pos); i < end; ++i) {
            s += sbuf[i];
          }
          if (end > pos) {
            emit(key, static_cast<int64_t>(s), end - pos);
          }
          pos = end;
          q += static_cast<size_t>(gcb) + 4;
        }
        for (; pos < rc; ++pos) {
          emit(0, sbuf[pos], 1);
        }
        continue;
      }
      // else fall through to generic
    }
    // Generic: decode both columns, hash on values.
    {
      std::vector<int64_t> gv(rc), sv(rc);
      chunk.DecodeInt64ColumnRg(group_col, g, gv.data());
      chunk.DecodeInt64ColumnRg(sum_col, g, sv.data());
      for (uint32_t i = 0; i < rc; ++i) {
        emit(gv[i], sv[i], 1);
      }
    }
  }
  for (size_t k = 0; k < out.size(); ++k) {
    out[k].sum = static_cast<int64_t>(sums[k]);
  }
  return out;
}

// The hash of a composite group key. splitmix64's finalizer, mixed once per
// column: the columns are independent, so a pair of equal columns in a
// different order hashes differently (the key comparison is by VALUE, so
// the hash only has to spread, not to order).
namespace {

uint64_t HashGroupKey(const std::vector<int64_t>& key) {
  uint64_t h = 0x9E3779B97F4A7C15ULL;
  for (int64_t v : key) {
    auto x = static_cast<uint64_t>(v);
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDULL;
    x ^= x >> 33;
    x *= 0xC4CEB9FE1A85EC53ULL;
    x ^= x >> 33;
    h ^= x + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
  }
  return h;
}

}  // namespace

StatusOr<std::vector<GroupByEntryN>> GroupBySumColumns(
    const PaxChunk& chunk, const std::vector<slot_t>& group_cols,
    slot_t sum_col, const Schema* sc, WorkloadTracker* workload) try {
  std::vector<GroupByEntryN> out;
  const PaxChunk::Footer& f = chunk.GetFooter();
  const uint16_t cc = f.column_count;
  if (group_cols.empty() || sum_col >= cc) {
    return Status::kInvalidArgument;
  }
  for (slot_t c : group_cols) {
    if (c >= cc) {
      return Status::kInvalidArgument;
    }
  }
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return Status::kInvalidArgument;
  }
  if (sc != nullptr) {
    const auto i64 = [sc](slot_t c) {
      return c < sc->ColumnCount() &&
             sc->GetColumn(c).Type() == ValueType::kInt64;
    };
    for (slot_t c : group_cols) {
      if (!i64(c)) {
        return Status::kInvalidArgument;
      }
    }
    if (!i64(sum_col)) {
      return Status::kInvalidArgument;
    }
  }
  if (workload != nullptr && sc != nullptr) {
    for (slot_t c : group_cols) {
      workload->RecordAggregate(*sc, c);
    }
    workload->RecordAggregate(*sc, sum_col);
  }

  // The composite key -> accumulator index. The entries are kept in
  // first-seen order so the output is deterministic.
  //
  // The KEY is not the map's key: a vector key would allocate once per
  // ROW (the map's node owns a copy), and a 500k-row group by would spend
  // its whole budget in the allocator. The map is keyed by the key's HASH
  // and every candidate under that hash is compared against the actual
  // values, so the answer stays exact while the per-row cost is a hash and
  // a short walk.
  struct GroupEntry {
    std::vector<int64_t> key;
    int64_t sum = 0;
    uint64_t count = 0;
  };
  std::vector<GroupEntry> entries;
  std::unordered_map<uint64_t, std::vector<uint32_t>> by_hash;
  by_hash.reserve(1024);

  const size_t nkeys = group_cols.size();
  std::vector<std::vector<int64_t>> gbuf(nkeys);
  std::vector<int64_t> sbuf;
  std::vector<int64_t> key;
  key.reserve(nkeys);
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const uint16_t rc = f.row_counts[g];
    if (rc == 0) {
      continue;
    }
    for (size_t ki = 0; ki < nkeys; ++ki) {
      gbuf[ki].assign(rc, 0);
      chunk.DecodeInt64ColumnRg(group_cols[ki], g, gbuf[ki].data());
    }
    sbuf.assign(rc, 0);
    chunk.DecodeInt64ColumnRg(sum_col, g, sbuf.data());
    for (uint32_t i = 0; i < rc; ++i) {
      key.clear();
      for (size_t ki = 0; ki < nkeys; ++ki) {
        key.push_back(gbuf[ki][i]);
      }
      const uint64_t h = HashGroupKey(key);
      size_t pos = SIZE_MAX;
      auto it = by_hash.find(h);
      if (it != by_hash.end()) {
        for (uint32_t cand : it->second) {
          if (entries[cand].key == key) {
            pos = cand;
            break;
          }
        }
      }
      if (pos == SIZE_MAX) {
        pos = entries.size();
        entries.push_back(GroupEntry{.key = key, .sum = 0, .count = 0});
        by_hash[h].push_back(static_cast<uint32_t>(pos));
        out.push_back(GroupByEntryN{.key = key, .sum = 0, .count = 0});
      }
      entries[pos].sum =
          std::bit_cast<int64_t>(static_cast<uint64_t>(entries[pos].sum) +
                                 static_cast<uint64_t>(sbuf[i]));
      entries[pos].count += 1;
      out[pos].sum = entries[pos].sum;
      out[pos].count = entries[pos].count;
    }
  }
  return out;
} catch (const std::exception& error) {
  return StatusError(StatusCode::kCorruption, error.what());
}

std::vector<TopKEntry> TopKColumn(const PaxChunk& chunk, slot_t col, size_t k,
                                  bool descending, const Schema* sc,
                                  WorkloadTracker* workload) {
  std::vector<TopKEntry> out;
  if (k == 0) {
    return out;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return out;
  }
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kInt64)) {
    return out;
  }
  if (workload != nullptr && sc != nullptr) {
    workload->RecordSortKey(*sc, col);
  }
  // Bounded heap of the current winners; threshold = worst winner.
  // Max-heap for ascending (top = largest winner), min-heap for descending.
  struct Cmp {
    bool descending;
    bool operator()(const TopKEntry& a, const TopKEntry& b) const {
      if (a.value != b.value) {
        return descending ? (a.value > b.value) : (a.value < b.value);
      }
      return a.row < b.row;
    }
  };
  // std::priority_queue keeps the "largest" (per Cmp) on top; with the Cmp
  // above the top is the current WORST winner (threshold holder).
  std::priority_queue<TopKEntry, std::vector<TopKEntry>, Cmp> heap(
      Cmp{descending});
  bool full = false;
  int64_t thresh = descending ? INT64_MIN : INT64_MAX;
  uint64_t base_row = 0;
  std::vector<int64_t> buf;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const uint16_t rc = f.row_counts[g];
    const size_t zi = (static_cast<size_t>(g) * f.column_count) + col;
    bool pruned = false;
    if (full && f.all_zm_present && zi < f.all_zone_mins.size() &&
        zi < f.all_zone_maxs.size() && !RgColHasNull(f, g, col)) {
      // Block-max (block-min) pruning: rows arrive in global order, so a
      // later row never wins a tie against the heap; equal-to-threshold
      // rows are provably useless. Null-bearing groups never prune: nulls
      // decode as raw 0 and can always beat an ascending threshold.
      pruned = descending ? (f.all_zone_maxs[zi] <= thresh)
                          : (f.all_zone_mins[zi] >= thresh);
    }
    if (!pruned) {
      buf.assign(rc, 0);
      chunk.DecodeInt64ColumnRg(col, g, buf.data());
      for (uint16_t i = 0; i < rc; ++i) {
        const TopKEntry e{.value = buf[i], .row = base_row + i};
        if (!full) {
          heap.push(e);
          if (heap.size() >= k) {
            full = true;
            thresh = heap.top().value;
          }
        } else {
          // Replacement must use the full (value, row) order: an entry
          // EQUAL in value to the threshold but with a smaller global row
          // wins the documented tie-break, yet the value-only comparison
          // discarded it.
          const TopKEntry& worst = heap.top();
          const bool beats = descending
                                 ? (e.value > thresh ||
                                    (e.value == thresh && e.row < worst.row))
                                 : (e.value < thresh ||
                                    (e.value == thresh && e.row < worst.row));
          if (beats) {
            heap.pop();
            heap.push(e);
            thresh = heap.top().value;
          }
        }
      }
    }
    base_row += rc;
  }
  out.reserve(heap.size());
  while (!heap.empty()) {
    out.push_back(heap.top());
    heap.pop();
  }
  std::sort(out.begin(), out.end(),
            [descending](const TopKEntry& a, const TopKEntry& b) {
              if (a.value != b.value) {
                return descending ? (a.value > b.value) : (a.value < b.value);
              }
              return a.row < b.row;
            });
  return out;
}

uint64_t PgmPredict(const PaxChunk::Footer& f, int64_t key, uint64_t total) {
  if (!f.pgm_present || f.pgm_segments.empty() || total == 0) {
    return total;
  }
  // Last segment with key0 <= key (segments partition ascending keys).
  size_t lo = 0, hi = f.pgm_segments.size();
  while (lo < hi) {
    const size_t mid = lo + ((hi - lo) / 2);
    if (f.pgm_segments[mid].key <= key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  const PgmSegment& s = f.pgm_segments[lo == 0 ? 0 : lo - 1];
  // Signed comparison first (a negative key below a non-negative segment
  // key must yield dx = 0, not a wrapped u64 difference); the difference
  // itself is then exact in the unsigned domain.
  const uint64_t dx =
      (key >= s.key) ? static_cast<uint64_t>(key) - static_cast<uint64_t>(s.key)
                     : 0ULL;
  const double pred =
      static_cast<double>(s.pos) + (s.slope * static_cast<double>(dx));
  if (!(pred >= 0.0)) {
    return 0;  // NaN-safe floor
  }
  // A hostile slope (ValidateWalk never bounds it) can push pred past
  // uint64 range (or to +inf); casting that to uint64_t is UB, so clamp
  // to the answer's domain first.
  if (pred >= static_cast<double>(total)) {
    return total - 1;
  }
  const auto p = static_cast<uint64_t>(pred);
  return p;
}

const InvertedCell* FindInvertedCell(const PaxChunk::Footer& f, uint32_t g,
                                     slot_t c) {
  if (!f.inv_idx_present) {
    return nullptr;
  }
  // Cells are appended in (group, column) order by the builder.
  for (const InvertedCell& cell : f.inv_index) {
    if (cell.group == g && cell.column == c) {
      return &cell;
    }
    if (cell.group > g) {
      break;
    }
  }
  return nullptr;
}

const InvertedPosting* FindInvertedPosting(const InvertedCell& cell,
                                           int64_t value) {
  size_t lo = 0, hi = cell.postings.size();
  while (lo < hi) {
    const size_t mid = lo + ((hi - lo) / 2);
    if (cell.postings[mid].value < value) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo < cell.postings.size() && cell.postings[lo].value == value) {
    return &cell.postings[lo];
  }
  return nullptr;
}

bool InvertedEqCount(const PaxChunk& chunk, slot_t col, int64_t value,
                     uint64_t* count_out) {
  if (count_out == nullptr) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || f.row_counts.size() < f.row_group_count) {
    return false;
  }
  uint64_t total = 0;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const InvertedCell* cell = FindInvertedCell(f, g, col);
    if (cell == nullptr) {
      return false;
    }
    const InvertedPosting* p = FindInvertedPosting(*cell, value);
    if (p != nullptr) {
      total += p->rows.size();
    }
  }
  *count_out = total;
  return true;
}

bool InvertedInCount(const PaxChunk& chunk, slot_t col, const int64_t* values,
                     size_t nvalues, uint64_t* count_out) {
  if (count_out == nullptr || (values == nullptr && nvalues > 0)) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count || f.row_counts.size() < f.row_group_count) {
    return false;
  }
  uint64_t total = 0;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const InvertedCell* cell = FindInvertedCell(f, g, col);
    if (cell == nullptr) {
      return false;
    }
    for (size_t v = 0; v < nvalues; ++v) {
      const InvertedPosting* p = FindInvertedPosting(*cell, values[v]);
      if (p != nullptr) {
        total += p->rows.size();
      }
    }
  }
  *count_out = total;
  return true;
}

const XorCell* FindXorCell(const PaxChunk::Footer& f, uint32_t g, slot_t c) {
  if (!f.xor_present) {
    return nullptr;
  }
  for (const XorCell& cell : f.xor_cells) {
    if (cell.group == g && cell.column == c) {
      return &cell;
    }
    if (cell.group > g) {
      break;
    }
  }
  return nullptr;
}

bool XorCellMayContain(const XorCell& cell, int64_t value) {
  const size_t m = cell.table.size();
  if (m == 0) {
    return true;  // degenerate: pass safe rather than mis-prune
  }
  const uint64_t h = XorHash64(value, cell.seed);
  const uint64_t h0 = h % m;
  const uint64_t h1 = XorRotL(h, 21) % m;
  const uint64_t h2 = XorRotL(h, 42) % m;
  const auto fp = static_cast<uint8_t>(h & 0xFF);
  return static_cast<uint8_t>(cell.table[h0] ^ cell.table[h1] ^
                              cell.table[h2]) == fp;
}

std::vector<uint64_t> XorRgMask(const PaxChunk& chunk, slot_t col,
                                int64_t value) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  const size_t nwords = (static_cast<size_t>(f.row_group_count) + 63) / 64;
  std::vector<uint64_t> mask(nwords, 0);
  if (col >= f.column_count) {
    return mask;
  }
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const XorCell* cell = FindXorCell(f, g, col);
    if (cell == nullptr || XorCellMayContain(*cell, value)) {
      mask[g >> 6] |= (1ULL << (g & 63));
    }
  }
  return mask;
}

bool InvertedAndCount(const PaxChunk& chunk, slot_t col_a, int64_t val_a,
                      slot_t col_b, int64_t val_b, uint64_t* count_out) {
  if (count_out == nullptr) {
    return false;
  }
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col_a >= f.column_count || col_b >= f.column_count ||
      f.row_counts.size() < f.row_group_count) {
    return false;
  }
  uint64_t total = 0;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const InvertedCell* ca = FindInvertedCell(f, g, col_a);
    const InvertedCell* cb = FindInvertedCell(f, g, col_b);
    if (ca == nullptr || cb == nullptr) {
      return false;
    }
    const InvertedPosting* pa = FindInvertedPosting(*ca, val_a);
    const InvertedPosting* pb = FindInvertedPosting(*cb, val_b);
    if (pa == nullptr || pb == nullptr) {
      continue;
    }
    // Sorted-merge intersection over row ids (both postings ascending).
    size_t ia = 0, ib = 0;
    while (ia < pa->rows.size() && ib < pb->rows.size()) {
      if (pa->rows[ia] == pb->rows[ib]) {
        ++total;
        ++ia;
        ++ib;
      } else if (pa->rows[ia] < pb->rows[ib]) {
        ++ia;
      } else {
        ++ib;
      }
    }
  }
  *count_out = total;
  return true;
}

}  // namespace tinylamb
