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
#include <cmath>
#include <ranges>

#include "table/pax_chunk/pax_chunk.hpp"
#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb {

bool QppRegionCountSum(const char* region, uint32_t n, uint32_t param,
                       const char* region_end, PaxChunk::CompareOp op,
                       int64_t literal, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  QppRegion parsed;
  if (!ParseQppRegion(region, n, param, region_end, &parsed)) {
    return false;
  }
  __int128 low = INT64_MIN;
  __int128 high = INT64_MAX;
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      low = high = literal;
      break;
    case PaxChunk::CompareOp::kLe:
      high = literal;
      break;
    case PaxChunk::CompareOp::kLt:
      high = static_cast<__int128>(literal) - 1;
      break;
    case PaxChunk::CompareOp::kGe:
      low = literal;
      break;
    case PaxChunk::CompareOp::kGt:
      low = static_cast<__int128>(literal) + 1;
      break;
    default:
      return false;
  }
  int64_t count = 0;
  __int128 sum = 0;
  for (uint32_t p = 0; p < parsed.pages; ++p) {
    const QppPage& page = parsed.page[p];
    std::array<QppRange, 2> ranges{};
    const size_t range_count = QppModelRanges(page, low, high, ranges);
    for (size_t i = 0; i < range_count; ++i) {
      count += ranges[i].end - ranges[i].begin;
      sum += QppModelSum(page, ranges[i].begin, ranges[i].end);
    }
  }
  QppPatchExceptionCount(parsed, op, literal, low, high, &count, &sum);
  *count_out = static_cast<uint64_t>(count);
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool WaveletRRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t literal, uint64_t* count_out,
                            int64_t* sum_out) {
  WaveletRRegion p;
  if (!ParseWaveletRRegion(region, n, param, region_end, &p)) {
    return false;
  }
  return WaveletRRegionFold(p, op, literal, count_out, sum_out);
}

void PaxChunk::ByteSliceCompareMask(const char* region, uint32_t n,
                                    CompareOp op, int64_t lit,
                                    const uint8_t* null_bmp,
                                    uint64_t* out_bits) {
  if (n == 0) {
    return;
  }
  const auto w = static_cast<uint8_t>(region[0]);
  if (w == 0 || w > 64) {
    return;
  }
  const auto set_all_non_null = [&] {
    for (uint32_t i = 0; i < n; ++i) {
      if (null_bmp != nullptr &&
          ((static_cast<unsigned>(null_bmp[i >> 3]) >> (7U - (i & 7U))) & 1U) !=
              0U) {
        continue;
      }
      out_bits[i >> 6] |= (1ULL << (i & 63));
    }
  };
  // Narrow unsigned domain (w < 64 => values in [0, 2^w)): out-of-domain
  // literals decide every row without touching a slice.
  if (w < 64) {
    const uint64_t dom = ((1ULL << w) - 1ULL);
    if (lit < 0) {
      if (op == CompareOp::kGe || op == CompareOp::kGt) {
        set_all_non_null();
      }
      return;
    }
    if (std::cmp_greater(lit, dom)) {
      if (op == CompareOp::kLe || op == CompareOp::kLt) {
        set_all_non_null();
      }
      return;
    }
  }
  const size_t stride = (static_cast<size_t>(n) + 7) / 8;
  const auto* r = reinterpret_cast<const unsigned char*>(region);
  // Signed ordering for w == 64: flip the sign bit of literal and slice 63,
  // turning the signed compare into an unsigned slice walk.
  const bool flip_top = (w == 64);
  const uint64_t lu =
      static_cast<uint64_t>(lit) ^ (flip_top ? (1ULL << 63) : 0ULL);
  const auto sbyte = [&](uint32_t b, size_t i) -> uint8_t {
    uint8_t v = r[1 + (static_cast<size_t>(b) * stride) + i];
    if (flip_top && b == 63) {
      v = static_cast<uint8_t>(~v);
    }
    return v;
  };
  uint8_t tail = 0xFF;
  if ((n & 7U) != 0U) {
    tail = static_cast<uint8_t>(0xFFU << (8 - (n & 7U)));
  }
  constexpr size_t kStackStride = (PaxChunk::kRowGroupSize + 7U) / 8U;
  std::array<uint8_t, kStackStride> eq_stack{};
  std::array<uint8_t, kStackStride> res_stack{};
  std::vector<uint8_t> eq_heap;
  std::vector<uint8_t> res_heap;
  uint8_t* eq = eq_stack.data();
  uint8_t* res = res_stack.data();
  if (stride > kStackStride) {
    eq_heap.resize(stride);
    res_heap.resize(stride);
    eq = eq_heap.data();
    res = res_heap.data();
  }
  // kind: 0 = EQ bytes in eq, 1 = GT bytes in res, 2 = LT bytes in res,
  // 3 = GE (complement of LT), 4 = LE (complement of GT).
  int kind = 0;
  switch (op) {
    case CompareOp::kEq:
      kind = 0;
      break;
    case CompareOp::kGt:
      kind = 1;
      break;
    case CompareOp::kLt:
      kind = 2;
      break;
    case CompareOp::kGe:
      kind = 3;
      break;
    case CompareOp::kLe:
      kind = 4;
      break;
  }
  const bool want_eq = (kind == 0);
  const bool want_gt = (kind == 1 || kind == 4);
  const bool want_lt = (kind == 2 || kind == 3);
  if (want_eq) {
    std::fill(eq, eq + stride, 0xFF);
    for (uint32_t b = 0; b < w; ++b) {
      const uint64_t bit = (lu >> b) & 1ULL;
      for (size_t i = 0; i < stride; ++i) {
        eq[i] &= bit != 0U ? sbyte(b, i) : static_cast<uint8_t>(~sbyte(b, i));
      }
    }
    eq[stride - 1] &= tail;
  }
  if (want_gt || want_lt) {
    std::fill(res, res + stride, 0);
    std::fill(eq, eq + stride, 0xFF);
    eq[stride - 1] &= tail;
    for (int b = static_cast<int>(w) - 1; b >= 0; --b) {
      const auto bb = static_cast<uint32_t>(b);
      const uint64_t bit = (lu >> bb) & 1ULL;
      if (want_gt) {
        if (bit != 0U) {
          for (size_t i = 0; i < stride; ++i) {
            eq[i] &= sbyte(bb, i);
          }
        } else {
          for (size_t i = 0; i < stride; ++i) {
            res[i] = static_cast<uint8_t>(res[i] | (eq[i] & sbyte(bb, i)));
            eq[i] &= static_cast<uint8_t>(~sbyte(bb, i));
          }
        }
      } else {  // want_lt
        if (bit != 0U) {
          for (size_t i = 0; i < stride; ++i) {
            res[i] = static_cast<uint8_t>(
                res[i] | (eq[i] & static_cast<uint8_t>(~sbyte(bb, i))));
            eq[i] &= sbyte(bb, i);
          }
        } else {
          for (size_t i = 0; i < stride; ++i) {
            eq[i] &= static_cast<uint8_t>(~sbyte(bb, i));
          }
        }
      }
    }
    res[stride - 1] &= tail;
  }
  const uint8_t* src = want_eq ? eq : res;
  const bool invert = (kind == 3 || kind == 4);
  for (uint32_t i = 0; i < n; ++i) {
    if (null_bmp != nullptr &&
        ((static_cast<unsigned>(null_bmp[i >> 3]) >> (7U - (i & 7U))) & 1U) !=
            0U) {
      continue;
    }
    const uint32_t hit =
        (static_cast<unsigned>(src[i >> 3]) >> (7U - (i & 7U))) & 1U;
    if ((hit != 0U) != invert) {
      out_bits[i >> 6] |= (1ULL << (i & 63));
    }
  }
}

int64_t PaxChunk::ByteSliceFilteredSum(const char* region, uint32_t n,
                                       const uint8_t* null_bmp,
                                       const uint64_t* sel_bits) {
  if (n == 0) {
    return 0;
  }
  const auto w = static_cast<uint8_t>(region[0]);
  if (w == 0 || w > 64) {
    return 0;
  }
  const size_t stride = (static_cast<size_t>(n) + 7) / 8;
  const auto* r = reinterpret_cast<const unsigned char*>(region);
  // Combined mask in the slice (MSB-first) domain: selection AND NOT null.
  constexpr size_t kStackStride = (PaxChunk::kRowGroupSize + 7U) / 8U;
  std::array<uint8_t, kStackStride> m_stack{};
  std::vector<uint8_t> m_heap;
  uint8_t* m = m_stack.data();
  if (stride > kStackStride) {
    m_heap.resize(stride);
    m = m_heap.data();
  }
  std::fill(m, m + stride, 0xFF);
  if ((n & 7U) != 0U) {
    m[stride - 1] = static_cast<uint8_t>(0xFFU << (8 - (n & 7U)));
  }
  if (sel_bits != nullptr) {
    std::fill(m, m + stride, 0);
    for (uint32_t i = 0; i < n; ++i) {
      if (((sel_bits[i >> 6] >> (i & 63)) & 1ULL) != 0ULL) {
        m[i >> 3] = static_cast<uint8_t>(m[i >> 3] | (0x80 >> (i & 7)));
      }
    }
  }
  if (null_bmp != nullptr) {
    for (size_t i = 0; i < stride; ++i) {
      m[i] &= static_cast<uint8_t>(~null_bmp[i]);
    }
    if ((n & 7U) != 0U) {
      m[stride - 1] &= static_cast<uint8_t>(0xFFU << (8 - (n & 7U)));
    }
  }
  __int128 sum = 0;
  for (uint32_t b = 0; b < w; ++b) {
    const unsigned char* sl = r + 1 + (static_cast<size_t>(b) * stride);
    uint64_t cnt = 0;
    for (size_t i = 0; i < stride; ++i) {
      cnt += static_cast<uint64_t>(
          __builtin_popcount(static_cast<unsigned int>(sl[i] & m[i])));
    }
    // Unsigned weight: for w == 64 the sign bit weighs +2^63 mod 2^64,
    // which equals -2^63 under the final truncation (two's complement).
    sum += static_cast<__int128>(cnt) * (static_cast<__int128>(1) << b);
  }
  return static_cast<int64_t>(sum);
}

namespace {

std::vector<uint8_t> PBitSActiveBytes(const PBitSRegion& p,
                                      const uint8_t* null_bmp,
                                      const uint64_t* sel_bits) {
  std::vector<uint8_t> active(p.stride, 0xFF);
  if ((p.n & 7U) != 0 && !active.empty()) {
    active.back() = static_cast<uint8_t>(0xFFU << (8U - (p.n & 7U)));
  }
  if (sel_bits != nullptr) {
    std::fill(active.begin(), active.end(), 0);
    for (uint32_t i = 0; i < p.n; ++i) {
      if ((sel_bits[i >> 6] & (1ULL << (i & 63U))) == 0) {
        continue;
      }
      active[i >> 3] =
          static_cast<uint8_t>(active[i >> 3] | (0x80U >> (i & 7U)));
    }
  }
  if (null_bmp != nullptr) {
    for (size_t i = 0; i < p.stride; ++i) {
      active[i] = static_cast<uint8_t>(active[i] & ~null_bmp[i]);
    }
  }
  if (p.exception_bmp != nullptr) {
    for (size_t i = 0; i < p.stride; ++i) {
      active[i] = static_cast<uint8_t>(
          active[i] & static_cast<uint8_t>(~p.exception_bmp[i]));
    }
  }
  return active;
}

}  // namespace

void PaxChunk::PBitSCompareMask(const char* region, uint32_t param, uint32_t n,
                                const char* region_end, CompareOp op,
                                int64_t lit, const uint8_t* null_bmp,
                                uint64_t* out_bits) {
  if (out_bits == nullptr) {
    return;
  }
  PBitSRegion p;
  if (!ParsePBitSRegion(region, n, param, region_end, &p)) {
    return;
  }
  const uint64_t literal = PBitSOrderValue(lit);
  for (uint32_t i = 0; i < p.n; ++i) {
    if (null_bmp != nullptr && (null_bmp[i >> 3] & (0x80U >> (i & 7U))) != 0) {
      continue;
    }
    if (p.exception_bmp != nullptr &&
        (p.exception_bmp[i >> 3] & (0x80U >> (i & 7U))) != 0) {
      continue;
    }
    uint64_t q = 0;
    for (uint32_t b = 0; b < p.width; ++b) {
      const unsigned char* plane =
          p.planes + (static_cast<size_t>(b) * p.stride);
      q = (q << 1) |
          static_cast<uint64_t>(
              (static_cast<unsigned>(plane[i >> 3]) >> (7U - (i & 7U))) & 1U);
    }
    if (PBitSOrderedMatch(op, p.base + q, literal)) {
      out_bits[i >> 6] |= 1ULL << (i & 63U);
    }
  }
  for (uint32_t k = 0; k < p.exceptions; ++k) {
    const char* entry = p.exception_values + (static_cast<size_t>(k) * 10);
    const uint32_t pos = ReadBE16(entry);
    if (pos >= p.n || (null_bmp != nullptr &&
                       (null_bmp[pos >> 3] & (0x80U >> (pos & 7U))) != 0)) {
      continue;
    }
    if (PBitSOrderedMatch(op, ReadBE64(entry + 2), literal)) {
      out_bits[pos >> 6] |= 1ULL << (pos & 63U);
    }
  }
}

uint32_t PaxChunk::PBitSNullAwareCount(const char* region, uint32_t param,
                                       uint32_t n, const char* region_end,
                                       const uint8_t* null_bmp, CompareOp op,
                                       int64_t lit) {
  if (n == 0) {
    return 0;
  }
  std::vector<uint64_t> bits((static_cast<size_t>(n) + 63) / 64, 0);
  PBitSCompareMask(region, param, n, region_end, op, lit, null_bmp,
                   bits.data());
  uint32_t count = 0;
  for (uint64_t word : bits) {
    count += static_cast<uint32_t>(__builtin_popcountll(word));
  }
  return count;
}

int64_t PaxChunk::PBitSFilteredSum(const char* region, uint32_t param,
                                   uint32_t n, const char* region_end,
                                   const uint8_t* null_bmp,
                                   const uint64_t* sel_bits) {
  PBitSRegion p;
  if (!ParsePBitSRegion(region, n, param, region_end, &p)) {
    return 0;
  }
  const std::vector<uint8_t> active = PBitSActiveBytes(p, null_bmp, sel_bits);
  uint64_t normal_count = 0;
  for (uint32_t i = 0; i < p.n; ++i) {
    if ((active[i >> 3] & (0x80U >> (i & 7U))) == 0) {
      continue;
    }
    ++normal_count;
    uint64_t q = 0;
    for (uint32_t b = 0; b < p.width; ++b) {
      const unsigned char* plane =
          p.planes + (static_cast<size_t>(b) * p.stride);
      q = (q << 1) |
          static_cast<uint64_t>(
              (static_cast<unsigned>(plane[i >> 3]) >> (7U - (i & 7U))) & 1U);
    }
  }
  const __int128 base_extended =
      (p.base & (1ULL << 63)) != 0
          ? static_cast<__int128>(p.base & ~(1ULL << 63)) +
                (static_cast<__int128>(1) << 63)
          : static_cast<__int128>(p.base);
  __int128 ordered_sum = base_extended * static_cast<__int128>(normal_count);
  for (uint32_t b = 0; b < p.width; ++b) {
    const unsigned char* plane = p.planes + (static_cast<size_t>(b) * p.stride);
    uint64_t count = 0;
    for (size_t i = 0; i < p.stride; ++i) {
      count += static_cast<uint64_t>(
          __builtin_popcount(static_cast<unsigned int>(plane[i] & active[i])));
    }
    ordered_sum += static_cast<__int128>(count) *
                   (static_cast<__int128>(1) << (p.width - 1 - b));
  }
  __int128 sum = ordered_sum - (static_cast<__int128>(normal_count) << 63);
  for (uint32_t k = 0; k < p.exceptions; ++k) {
    const char* entry = p.exception_values + (static_cast<size_t>(k) * 10);
    const uint32_t pos = ReadBE16(entry);
    if (pos >= p.n || (null_bmp != nullptr &&
                       (null_bmp[pos >> 3] & (0x80U >> (pos & 7U))) != 0)) {
      continue;
    }
    if (sel_bits != nullptr &&
        (sel_bits[pos >> 6] & (1ULL << (pos & 63U))) == 0) {
      continue;
    }
    sum += static_cast<int64_t>(ReadBE64(entry + 2) ^ (1ULL << 63));
  }
  return static_cast<int64_t>(sum);
}

// Only cw in {1, 2, 4} may be byte-expanded, and the reason is a CORRECTNESS
// one, not a speed one: the expansion decodes a whole number of codes out of
// each byte, which requires cw to divide 8. cw == 3 does not -- two codes
// straddle a byte boundary (code 0 takes bits 7..5, code 1 bits 4..2, code 2
// bits 1..0 of the next byte's top bit) -- so a byte-indexed pass table would
// both mis-decode and desynchronize every following code. cw > 4 leaves fewer
// than two codes per byte, where the table buys nothing anyway. Callers keep
// their plain bit-at-a-time loop for every other width.

// Predicated dictionary fold: for every SELECTED row, hand `emit` its code,
// clamped into [0, ncodes]. Codes past the dictionary decode to raw 0, so
// ncodes is both the clamp bucket and a real dictionary index -- `emit` sees
// exactly those two cases apart. `sel_bits` is the row-major selection mask
// with ceil(n/64) words, already trimmed to n bits by the caller.
//
// Same two savings as DictPassMaskByteExpanded, which this mirrors: the
// selection test reads one register-resident word per 64 rows instead of
// re-loading sel_bits[i>>6] for every row, and the code comes from a byte
// split so one byte load feeds 8/cw codes. Widths that do not divide 8 keep
// the bit reader (see DictPassByteExpandable).
// Selection-aware code HISTOGRAM, specialised on kPerByte (8/cw, so {2,4,8}).
// The kDict SUM's per-row work is exactly two things -- test the selection bit,
// bump a counter -- so the divisions a runtime per_byte forces
// (`base / per_byte + j / per_byte`, two `divl` per row and ~10% of all samples
// in the predicated DICT SUM profile) dominate it, and specialising is worth
// the code. Templating on kPerByte turns them into shifts and lets the loop
// unroll 8/16/32 ways.
//
// Deliberately ONLY the SUM. kMin/kMax reach the same codes through
// DictSelectedCodeFold with a lambda that does real per-row work (the
// zero-value-vs-NULL test), and giving that three inlined copies measured ~2%
// slower on bench_tpch's reduce_min / reduce_max -- a worse trade than the
// specialisation won there -- so MIN/MAX keep the generic fold.
bool DictCodeHistogram(const char* region, uint32_t n, uint32_t param,
                       const char* region_end,
                       std::vector<uint64_t>* freq_out) {
  if (region == nullptr || region_end == nullptr || freq_out == nullptr ||
      region_end <= region || static_cast<size_t>(region_end - region) < 2) {
    // A sub-2-byte cell has no [ncodes u16] header to read: fail closed
    // instead of assembling ncodes from the next region's / footer's bytes.
    return false;
  }
  const uint16_t ncodes = ReadBE16(region);
  if (ncodes == 0 || ncodes > 1024 || param != ncodes) {
    return false;
  }
  const int cw = BitsNeeded(ncodes - 1);
  const size_t dict_base =
      2 + ((static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8);
  const char* dict = region + dict_base;
  // int64 dicts need 8B/entry (the validator floors at 4B/entry for the
  // varchar case); refuse rather than read into the footer.
  if (dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) > region_end) {
    return false;
  }
  freq_out->assign(static_cast<size_t>(ncodes) + 1, 0);
  const auto cw32 = static_cast<uint32_t>(cw);
  if (DictPassByteExpandable(cw32)) {
    // Byte-split extraction: one byte load feeds per_byte codes, so the fold
    // costs ~1 op per row instead of an 8-byte load + bswap + shift + mask
    // per row, and the clamp is a table-sized compare against a value the
    // split already produced. Only widths that divide 8 qualify (a straddling
    // cw would desynchronize), so every other width keeps the reader loop.
    const uint32_t per_byte = 8U / cw32;
    const uint32_t code_mask = (1U << cw32) - 1U;
    const uint8_t* src = reinterpret_cast<const uint8_t*>(region) + 2;
    const uint32_t whole = n / per_byte * per_byte;
    for (uint32_t i = 0; i < whole; i += per_byte) {
      const uint32_t byte = src[i / per_byte];
      for (uint32_t k = 0; k < per_byte; ++k) {
        const uint32_t code = (byte >> (8U - cw32 * (k + 1U))) & code_mask;
        ++(*freq_out)[code < ncodes ? code : ncodes];
      }
    }
    // Tail codes (n % per_byte) share the final byte; the last one may be a
    // partial slice, which the same shift expression yields correctly.
    if (whole < n) {
      const uint32_t byte = src[whole / per_byte];
      const uint32_t tail = std::min(n - whole, per_byte);
      for (uint32_t k = 0; k < tail; ++k) {
        const uint32_t code = (byte >> (8U - cw32 * (k + 1U))) & code_mask;
        ++(*freq_out)[code < ncodes ? code : ncodes];
      }
    }
    return true;
  }
  WordBitReader br(region + 2, region + dict_base);
  for (uint32_t i = 0; i < n; ++i) {
    const uint64_t code = br.get(cw);
    ++(*freq_out)[code < ncodes ? code : ncodes];
  }
  return true;
}

bool DictColumnSum(const char* region, uint32_t n, uint32_t param,
                   const char* region_end, int64_t* sum_out) {
  if (sum_out == nullptr) {
    return false;
  }
  std::vector<uint64_t> freq;
  if (!DictCodeHistogram(region, n, param, region_end, &freq)) {
    return false;
  }
  const uint16_t ncodes = ReadBE16(region);
  const int cw = BitsNeeded(ncodes - 1);
  const char* dict =
      region + 2 + ((static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8);
  __int128 sum = 0;
  for (uint16_t d = 0; d < ncodes; ++d) {
    if (freq[d] == 0) {
      continue;
    }
    sum += static_cast<__int128>(static_cast<int64_t>(
               ReadBE64(dict + (static_cast<size_t>(d) * 8)))) *
           static_cast<__int128>(freq[d]);
  }
  // Clamped codes decode to raw 0: no contribution.
  *sum_out = static_cast<int64_t>(sum);
  return true;
}

// ---- Order-preserving dictionary (kOpDict) compressed-domain kernels ------
//
// kOpDict assigns each distinct value the rank of its position in the
// ascending dictionary, so `v OP lit` is equivalent to `code OP code_of(lit)`
// for every ordered comparison: the predicate collapses to a contiguous
// window [lo_code, hi_code) of the code space, computed ONCE per region with
// two binary searches over the dictionary (or one, for equality). The
// per-row work is then a bit-unpack plus a single branch-free window test
// `(uint64_t)(code - lo) < span` -- no dictionary evaluation, no pass table,
// and no value decode.

// Maps `lit op` to the rank window. `lo`/`span` use unsigned semantics:
// a code hits iff `(uint64_t)(code - lo) < span`. `clamp_pass` reports
// whether the clamp bucket (codes >= ncodes, decoded as raw 0) satisfies
// the predicate. `region` must be a structurally valid int64 OP-DICT cell
// (header + dict inside region_end): callers gate that before calling.
void OpDictWindowForValue(const char* region, uint32_t n, uint32_t param,
                          PaxChunk::CompareOp op, int64_t lit, uint64_t* lo,
                          uint64_t* span, bool* clamp_pass) {
  const int cw = BitsNeeded(param > 0 ? param - 1 : 0);
  const char* dict =
      region + 2 + ((static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8);
  auto dict_at = [&](uint16_t d) {
    return static_cast<int64_t>(
        ReadBE64(dict + (static_cast<size_t>(d) * sizeof(int64_t))));
  };
  // lower_bound(lit): first entry >= lit; upper_bound(lit): first entry >
  // lit. Binary searches (<= 11 steps at the 1024-code cap) replace the
  // per-query linear dictionary evaluation a pass table needs.
  auto lower_bound = [&](int64_t key) {
    uint16_t a = 0, b = static_cast<uint16_t>(param);
    while (a < b) {
      const auto m = static_cast<uint16_t>(a + ((b - a) / 2));
      if (dict_at(m) < key) {
        a = static_cast<uint16_t>(m + 1);
      } else {
        b = m;
      }
    }
    return a;
  };
  auto upper_bound = [&](int64_t key) {
    uint16_t a = 0, b = static_cast<uint16_t>(param);
    while (a < b) {
      const auto m = static_cast<uint16_t>(a + ((b - a) / 2));
      if (dict_at(m) <= key) {
        a = static_cast<uint16_t>(m + 1);
      } else {
        b = m;
      }
    }
    return a;
  };
  uint64_t wlo = 0;
  uint64_t whi = param;
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      wlo = lower_bound(lit);
      whi = (wlo < param && dict_at(static_cast<uint16_t>(wlo)) == lit)
                ? wlo + 1
                : wlo;
      break;
    case PaxChunk::CompareOp::kLe:
      whi = upper_bound(lit);
      break;
    case PaxChunk::CompareOp::kLt:
      whi = lower_bound(lit);
      break;
    case PaxChunk::CompareOp::kGe:
      wlo = lower_bound(lit);
      break;
    case PaxChunk::CompareOp::kGt:
      wlo = upper_bound(lit);
      break;
  }
  *lo = wlo;
  *span = whi - wlo;
  // Clamp codes decode as raw 0: they hit iff the rank 0 would take falls
  // inside the window (also false when the window is empty).
  const uint64_t zero_rank = lower_bound(0);
  *clamp_pass = zero_rank >= *lo && zero_rank < *lo + *span;
}

bool OpDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                          const char* region_end, PaxChunk::CompareOp op,
                          int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr || region == nullptr || region_end == nullptr ||
      region_end <= region || static_cast<size_t>(region_end - region) < 2) {
    return false;  // no [ncodes u16] header to read: fail closed
  }
  const uint16_t ncodes = ReadBE16(region);
  if (ncodes == 0 || ncodes > 1024 || param != ncodes) {
    return false;
  }
  const int cw = BitsNeeded(ncodes - 1);
  const size_t dict_base =
      2 + ((static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8);
  const char* dict = region + dict_base;
  // int64 dicts need 8B/entry; refuse rather than read into the footer
  // (mirrors DictCodeHistogram's extent gate).
  if (dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) > region_end) {
    return false;
  }
  uint64_t lo = 0, span = 0;
  bool clamp_pass = false;
  OpDictWindowForValue(region, n, param, op, lit, &lo, &span, &clamp_pass);
  WordBitReader br(region + 2, region + dict_base);
  uint64_t cnt = 0;
  uint64_t sum = 0;
  if ((1ULL << cw) == ncodes) {
    // Exact power-of-two code space: every cw-bit pattern is a valid code,
    // so the per-row fold is a single branch-free window test.
    for (uint32_t i = 0; i < n; ++i) {
      const uint64_t code = br.get(cw);
      const bool hit = code - lo < span;
      cnt += hit ? 1 : 0;
      if (hit && sum_out != nullptr) {
        sum += ReadBE64(dict + (code * sizeof(int64_t)));
      }
    }
  } else {
    // Non-exact widths can carry codes >= ncodes; they decode as raw 0 and
    // participate exactly when 0 satisfies the predicate.
    for (uint32_t i = 0; i < n; ++i) {
      const uint64_t code = br.get(cw);
      const bool hit = code < ncodes ? (code - lo < span) : clamp_pass;
      cnt += hit ? 1 : 0;
      if (hit && code < ncodes && sum_out != nullptr) {
        sum += ReadBE64(dict + (code * sizeof(int64_t)));
      }
    }
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

// ---- Run-length over dictionary codes (kRleDict) compressed-domain kernel -
//
// The predicate is evaluated ONCE per dictionary entry into a pass table
// (d <= 65535 entries, encoder-gated at 1024), then the RUN stream folds per
// run: count += pass[code] * len, sum += dict[code] * pass[code] * len. No
// row is materialized and the packed per-row code space (kDict/kOpDict's
// n*cw-bit stream) is never touched, so run-structured columns answer in
// O(runs) instead of O(rows).

// `v OP lit` over the CompareOp domain. Shared by the pass-table build and
// the clamp-bucket decision (both run once per call, never per row).
bool RleDictMatch(PaxChunk::CompareOp op, int64_t v, int64_t lit) {
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      return v == lit;
    case PaxChunk::CompareOp::kLe:
      return v <= lit;
    case PaxChunk::CompareOp::kLt:
      return v < lit;
    case PaxChunk::CompareOp::kGe:
      return v >= lit;
    case PaxChunk::CompareOp::kGt:
      return v > lit;
  }
  return false;
}

// ---- Huffman dictionary (kHuffDict) compressed-domain kernel ---------------
//
// The inline frequency table turns the region into an exact histogram, so ANY
// predicate folds in closed form over the d symbols -- the O(d) scan below
// never touches the bitstream and never materializes a row, whatever the row
// order (kRleDict needs run structure, kOpDict's window still walks the packed
// codes per row). d <= 1024 caps the fold at one pass over the dictionary and
// frequency tables.
bool HuffDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out,
                            int64_t* sum_out) {
  HuffDictRegion p;
  if (count_out == nullptr || region == nullptr || region_end == nullptr ||
      !ParseHuffDictRegion(region, n, param, region_end, &p)) {
    return false;
  }
  uint64_t cnt = 0;
  uint64_t sum = 0;
  for (uint32_t s = 0; s < p.d; ++s) {
    const int64_t v = HuffDictDictAt(p, s);
    if (!RleDictMatch(op, v, lit)) {
      continue;
    }
    const uint64_t f = HuffDictFreqAt(p, s);
    cnt += f;
    if (sum_out != nullptr) {
      sum += static_cast<uint64_t>(v) * f;
    }
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

// kTopk compressed-domain fold. The 15 ascending hot entries with their
// exact inline counts are an exact histogram (kHuffDict's trick), so the
// predicate passes a contiguous window of codes and COUNT / filtered SUM
// fold over the window with ZERO nibble reads; the exception tail then
// folds through its own inner region -- O(15 + nexc), no row decode, no
// run/progression structure required. For EQ the two halves are mutually
// exclusive BY CONSTRUCTION: every occurrence of a hot value carries its
// code, so an exception value is never hot -- a literal inside the hot
// table answers from one frequency read alone, and a literal outside it
// lives only in the tail. Nulls participate as raw 0 exactly like the
// decode path (a null cell evaluates the same predicate).
bool TopKRegionCountSum(const char* region, uint32_t n, uint32_t param,
                        const char* region_end, PaxChunk::CompareOp op,
                        int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  constexpr size_t kTopkHeader = 2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;  // 159
  if (count_out == nullptr || region == nullptr || region_end == nullptr ||
      region_end - region < static_cast<std::ptrdiff_t>(kTopkHeader)) {
    return false;
  }
  const uint32_t d = ReadBE16(region);
  const uint32_t nexc = ReadBE16(region + 2);
  if (d == 0 || d > 15 || nexc != param) {
    return false;
  }
  const auto ischeme = static_cast<uint8_t>(region[154]);
  const uint32_t iparam = ReadBE32(region + 155);
  const size_t nib_bytes = (static_cast<size_t>(n) + 1) / 2;
  if (region + kTopkHeader + nib_bytes > region_end ||
      (nexc > 0 && !InnerSchemeOk(ischeme))) {
    return false;
  }
  const auto hot_at = [&](uint32_t c) {
    return static_cast<int64_t>(
        ReadBE64(region + 4 + (static_cast<size_t>(c) * 8)));
  };
  // Tail fold, decoded lazily: the EQ-on-hot closed form must not pay for
  // it (stack-resident for the head-gate's typical tail sizes, one heap
  // vector for the rare long tail -- a per-query allocation dominates the
  // fold otherwise).
  bool tail_done = false;
  uint64_t tail_cnt = 0;
  uint64_t tail_sum = 0;  // two's-complement u64 accumulator
  const auto eval_tail = [&]() {
    if (tail_done) {
      return;
    }
    tail_done = true;
    if (nexc == 0) {
      return;
    }
    std::array<int64_t, 256> exc_stack{};
    std::vector<int64_t> exc_heap;
    int64_t* exc = nullptr;
    if (nexc <= 256) {
      exc = exc_stack.data();
    } else {
      exc_heap.resize(nexc);
      exc = exc_heap.data();
    }
    DecodeInnerI64All(region + kTopkHeader + nib_bytes, ischeme, iparam, nexc,
                      region_end, exc);
    for (uint32_t e = 0; e < nexc; ++e) {
      if (RleDictMatch(op, exc[e], lit)) {
        ++tail_cnt;
        tail_sum += static_cast<uint64_t>(exc[e]);
      }
    }
  };
  uint64_t cnt = 0;
  uint64_t sum = 0;  // two's-complement u64 accumulator
  if (op == PaxChunk::CompareOp::kEq) {
    // EQ is one binary search + one frequency read when the literal is hot
    // (the tail cannot match: exceptions are non-hot by construction), and
    // the tail alone when it is not -- never both halves.
    uint32_t lo = 0;
    uint32_t hi = d;
    while (lo < hi) {
      const uint32_t mid = lo + ((hi - lo) / 2);
      if (hot_at(mid) < lit) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo < d && hot_at(lo) == lit) {
      const uint64_t fq =
          ReadBE16(region + 124 + (static_cast<size_t>(lo) * 2));
      cnt = fq;
      sum = static_cast<uint64_t>(hot_at(lo)) * fq;
    } else {
      eval_tail();
      cnt = tail_cnt;
      sum = tail_sum;
    }
  } else {
    // Hot fold: each entry is one compare + one count read; the ascending
    // order makes the passing codes one contiguous run but a linear scan is
    // already O(15), so no bisect is worth it.
    for (uint32_t c = 0; c < d; ++c) {
      const int64_t v = hot_at(c);
      if (!RleDictMatch(op, v, lit)) {
        continue;
      }
      const uint64_t fq = ReadBE16(region + 124 + (static_cast<size_t>(c) * 2));
      cnt += fq;
      sum += static_cast<uint64_t>(v) * fq;
    }
    // Cold tail: exception values live outside the hot set but can still
    // sit inside the predicate window.
    eval_tail();
    cnt += tail_cnt;
    sum += tail_sum;
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

// Per-tile predicate window over the ascending tile dictionary: the rank
// math OpDictWindowForValue applies region-wide, scoped to d <= 64 entries.
// Passing entries are one contiguous rank range [lo, lo+span); `clamp_pass`
// is the verdict for hostile codes >= d (they decode as raw 0) and never
// fires on encoder output or a validated region.
void TileDictWindowForValue(const TileDictCursor& c, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* lo, uint64_t* span,
                            bool* clamp_pass) {
  auto dict_at = [&](uint32_t i) {
    return static_cast<int64_t>(
        ReadBE64(c.dict + (static_cast<size_t>(i) * 8)));
  };
  auto lower_bound = [&](int64_t key) {
    uint32_t a = 0;
    uint32_t b = c.d;
    while (a < b) {
      const uint32_t m = a + ((b - a) / 2);
      if (dict_at(m) < key) {
        a = m + 1;
      } else {
        b = m;
      }
    }
    return a;
  };
  auto upper_bound = [&](int64_t key) {
    uint32_t a = 0;
    uint32_t b = c.d;
    while (a < b) {
      const uint32_t m = a + ((b - a) / 2);
      if (dict_at(m) <= key) {
        a = m + 1;
      } else {
        b = m;
      }
    }
    return a;
  };
  uint64_t wlo = 0;
  uint64_t whi = c.d;
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      wlo = lower_bound(lit);
      whi = (wlo < c.d && dict_at(static_cast<uint32_t>(wlo)) == lit) ? wlo + 1
                                                                      : wlo;
      break;
    case PaxChunk::CompareOp::kLe:
      whi = upper_bound(lit);
      break;
    case PaxChunk::CompareOp::kLt:
      whi = lower_bound(lit);
      break;
    case PaxChunk::CompareOp::kGe:
      wlo = lower_bound(lit);
      break;
    case PaxChunk::CompareOp::kGt:
      wlo = upper_bound(lit);
      break;
  }
  *lo = wlo;
  *span = whi - wlo;
  const uint64_t zero_rank = lower_bound(0);
  *clamp_pass = zero_rank >= wlo && zero_rank < whi;
}

// kTileDict compressed-domain fold. Each tile's bracket [dict[0],
// dict[d-1]] is a tile-local zone map: a tile wholly outside the predicate
// window costs nothing, a wholly-inside tile contributes its full row count
// with ZERO code reads, and only a window-straddling tile walks its codes --
// at <= 6 bits per row of memory traffic instead of 8B values. COUNT and
// filtered SUM fold in one pass; nulls participate as raw 0 exactly like
// the decode path.
bool TileDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out,
                            int64_t* sum_out) {
  // region_end == nullptr is the ByteSliceNullAwareCount convention for a
  // pre-validated region: the chain walk stays bounded by n and every read
  // by the parsed d/rows, only the end-vs-region gate drops.
  if (count_out == nullptr || region == nullptr || param != 0) {
    return false;
  }
  TileDictCursor c;
  if (!TileDictFirst(region, n, region_end, &c)) {
    return false;
  }
  uint64_t cnt = 0;
  uint64_t sum = 0;  // two's-complement u64 accumulator
  for (;;) {
    uint64_t lo = 0;
    uint64_t span = 0;
    bool clamp_pass = false;
    TileDictWindowForValue(c, op, lit, &lo, &span, &clamp_pass);
    if (span == 0 && !clamp_pass) {
      // Wholly outside: the bracket skips the tile's codes entirely.
    } else if (lo == 0 && span == c.d) {
      // Wholly inside: COUNT is the row count, no code read. SUM still
      // needs the per-code counts, so it walks the tile once.
      cnt += c.rows;
      if (sum_out != nullptr) {
        if (c.w == 0) {
          sum += static_cast<uint64_t>(static_cast<int64_t>(ReadBE64(c.dict))) *
                 c.rows;
        } else {
          for (uint32_t j = 0; j < c.rows; ++j) {
            sum += static_cast<uint64_t>(TileDictValueAt(c, j));
          }
        }
      }
    } else {
      for (uint32_t j = 0; j < c.rows; ++j) {
        const uint64_t code = c.w == 0
                                  ? 0
                                  : UnpackBitAt(c.codes,
                                                static_cast<uint64_t>(j) *
                                                    static_cast<uint64_t>(c.w),
                                                c.w);
        const bool hit = code < c.d ? (code - lo < span) : clamp_pass;
        if (hit) {
          ++cnt;
          if (sum_out != nullptr) {
            sum += static_cast<uint64_t>(TileDictValueAt(c, j));
          }
        }
      }
    }
    if (!TileDictNext(&c)) {
      break;
    }
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}
// kRleIdx compressed-domain fold. Because the run values are sorted, the runs
// passing a comparison are ONE contiguous window, whose endpoints are two value
// bounds found by bisection -- so no run outside the window is ever touched.
// That is the whole point of the monotone gate: kRleBp / kRle8 / kRleDict /
// kRleX must walk and test every run. COUNT is then the restart-index
// difference row_before(last) - row_before(first) (two O(1) lookups plus a
// <= 15-run walk each), and SUM folds value*length over the window only.
bool RleIdxRegionCountSum(const char* region, uint32_t n, uint32_t param,
                          const char* region_end, PaxChunk::CompareOp op,
                          int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  RleIdxRegion p;
  if (!ParseRleIdxRegion(region, n, param, region_end, &p)) {
    return false;
  }
  uint32_t first = 0;
  uint32_t last = 0;
  if (!RleIdxPassingWindow(p, op, lit, &first, &last)) {
    return false;
  }
  if (first >= last) {
    *count_out = 0;
    if (sum_out != nullptr) {
      *sum_out = 0;
    }
    return true;
  }
  // COUNT without touching the runs: the restart table holds the row index of
  // every 16th run, so both bounds cost a lookup plus a <= 15-run walk.
  const uint32_t row_first = RleIdxRowBefore(p, first);
  const uint32_t row_last = RleIdxRowBefore(p, last);
  *count_out = row_last > row_first ? row_last - row_first : 0;
  if (sum_out != nullptr) {
    uint64_t sum = 0;  // two's-complement u64 accumulator
    for (uint32_t run = first; run < last; ++run) {
      const auto value = static_cast<uint64_t>(RleIdxValueAt(p, run));
      sum += value * RleIdxLengthAt(p, run);
    }
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool Bp128RegionCountSumParsed(const Bp128Region& parsed,
                               PaxChunk::CompareOp op, int64_t literal,
                               uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  uint64_t count = 0;
  __int128 sum = 0;
  for (uint32_t block_index = 0; block_index < parsed.block_count;
       ++block_index) {
    const Bp128Block& block = parsed.blocks[block_index];
    if (op == PaxChunk::CompareOp::kEq && block.minimum == block.maximum) {
      if (literal != block.minimum) {
        continue;
      }
      count += block.rows;
      if (sum_out != nullptr) {
        sum += Bp128BlockSum(block);
      }
      continue;
    }
    if (op != PaxChunk::CompareOp::kEq) {
      const bool minimum_hit = RleDictMatch(op, block.minimum, literal);
      const bool maximum_hit = RleDictMatch(op, block.maximum, literal);
      if (!minimum_hit && !maximum_hit) {
        continue;
      }
      if (minimum_hit && maximum_hit) {
        count += block.rows;
        if (sum_out != nullptr) {
          sum += Bp128BlockSum(block);
        }
        continue;
      }
    } else if (literal < block.minimum || literal > block.maximum) {
      continue;
    }
    std::array<int64_t, kBp128BlockRows> decoded{};
    DecodeBp128Block(parsed, block_index, decoded.data());
    for (uint32_t row = 0; row < block.rows; ++row) {
      if (!RleDictMatch(op, decoded[row], literal)) {
        continue;
      }
      ++count;
      if (sum_out != nullptr) {
        sum += decoded[row];
      }
    }
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool Bp128RegionCountSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t literal, uint64_t* count_out,
                         int64_t* sum_out) {
  if (region == nullptr || region_end == nullptr || region_end <= region ||
      count_out == nullptr) {
    return false;
  }
  Bp128Region parsed;
  if (!ParseBp128Region(region, n, param, region_end, &parsed) ||
      !Bp128MetadataMatches(parsed)) {
    return false;
  }
  return Bp128RegionCountSumParsed(parsed, op, literal, count_out, sum_out);
}

// kFastPfor (scheme 66) count/sum: the kBp128 bracket fold, exception-safe by
// construction -- the stored block brackets cover ALL rows (the encoder takes
// min/max over the block's full values, spikes included), so a bracket miss
// also excludes its exceptions, and a boundary block decodes once through
// FastPforDecodeBlock (exceptions patched) before the per-row compare.
bool FastPforRegionCountSumParsed(const FastPforRegion& parsed,
                                  PaxChunk::CompareOp op, int64_t literal,
                                  uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  uint64_t count = 0;
  __int128 sum = 0;
  for (uint32_t b = 0; b < parsed.block_count; ++b) {
    const FastPforBlock& blk = parsed.blocks[b];
    if (op == PaxChunk::CompareOp::kEq && blk.minimum == blk.maximum) {
      if (literal != blk.minimum) {
        continue;
      }
      count += blk.rows;
      if (sum_out != nullptr) {
        sum += blk.sum;
      }
      continue;
    }
    if (op != PaxChunk::CompareOp::kEq) {
      const bool minimum_hit = RleDictMatch(op, blk.minimum, literal);
      const bool maximum_hit = RleDictMatch(op, blk.maximum, literal);
      if (!minimum_hit && !maximum_hit) {
        continue;
      }
      if (minimum_hit && maximum_hit) {
        count += blk.rows;
        if (sum_out != nullptr) {
          sum += blk.sum;
        }
        continue;
      }
    } else if (literal < blk.minimum || literal > blk.maximum) {
      continue;
    }
    std::array<int64_t, kFastPforBlockRows> decoded{};
    FastPforDecodeBlock(blk, decoded.data());
    for (uint32_t t = 0; t < blk.rows; ++t) {
      if (!RleDictMatch(op, decoded[t], literal)) {
        continue;
      }
      ++count;
      if (sum_out != nullptr) {
        sum += decoded[t];
      }
    }
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool FastPforRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t literal, uint64_t* count_out,
                            int64_t* sum_out) {
  if (region == nullptr || region_end == nullptr || region_end <= region ||
      count_out == nullptr) {
    return false;
  }
  FastPforRegion parsed;
  if (!ParseFastPforRegion(region, n, param, region_end, &parsed) ||
      !FastPforMetadataMatches(parsed)) {
    return false;
  }
  return FastPforRegionCountSumParsed(parsed, op, literal, count_out, sum_out);
}

// kDeltaFastPfor (scheme 67) count/sum: the FastPFOR bracket fold with the
// delta dimension already folded in -- the stored block brackets cover ALL
// rows (the encoder takes min/max over the block's full values, base row
// included), and DeltaFastPforDecodeBlock patches exceptions before its
// replay, so a boundary block is one decode plus a plain per-row compare.
bool DeltaFastPforRegionCountSumParsed(const DeltaFastPforRegion& parsed,
                                       PaxChunk::CompareOp op, int64_t literal,
                                       uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  uint64_t count = 0;
  __int128 sum = 0;
  for (uint32_t b = 0; b < parsed.block_count; ++b) {
    const DeltaFastPforBlock& blk = parsed.blocks[b];
    if (op == PaxChunk::CompareOp::kEq && blk.minimum == blk.maximum) {
      if (literal != blk.minimum) {
        continue;
      }
      count += blk.rows;
      if (sum_out != nullptr) {
        sum += blk.sum;
      }
      continue;
    }
    if (op != PaxChunk::CompareOp::kEq) {
      const bool minimum_hit = RleDictMatch(op, blk.minimum, literal);
      const bool maximum_hit = RleDictMatch(op, blk.maximum, literal);
      if (!minimum_hit && !maximum_hit) {
        continue;
      }
      if (minimum_hit && maximum_hit) {
        count += blk.rows;
        if (sum_out != nullptr) {
          sum += blk.sum;
        }
        continue;
      }
    } else if (literal < blk.minimum || literal > blk.maximum) {
      continue;
    }
    std::array<int64_t, kDeltaFastPforBlockRows> decoded{};
    DeltaFastPforDecodeBlock(blk, decoded.data());
    for (uint32_t t = 0; t < blk.rows; ++t) {
      if (!RleDictMatch(op, decoded[t], literal)) {
        continue;
      }
      ++count;
      if (sum_out != nullptr) {
        sum += decoded[t];
      }
    }
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool DeltaFastPforRegionCountSum(const char* region, uint32_t n, uint32_t param,
                                 const char* region_end, PaxChunk::CompareOp op,
                                 int64_t literal, uint64_t* count_out,
                                 int64_t* sum_out) {
  if (region == nullptr || region_end == nullptr || region_end <= region ||
      count_out == nullptr) {
    return false;
  }
  DeltaFastPforRegion parsed;
  if (!ParseDeltaFastPforRegion(region, n, param, region_end, &parsed) ||
      !DeltaFastPforMetadataMatches(parsed)) {
    return false;
  }
  return DeltaFastPforRegionCountSumParsed(parsed, op, literal, count_out,
                                           sum_out);
}

bool RleDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                           const char* region_end, PaxChunk::CompareOp op,
                           int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr || region == nullptr || region_end == nullptr ||
      region_end <= region || static_cast<size_t>(region_end - region) < 2) {
    return false;  // no [d u16] header to read: fail closed
  }
  const uint16_t d = ReadBE16(region);
  if (d == 0 || param == 0) {
    return false;
  }
  const int cb = (BitsNeeded(d - 1) + 7) / 8;
  const char* dict = region + 2;
  const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
  // Header cross-check: the footer's param IS the region's run count
  // (mirrors OpDictRegionCountSum's `param != ncodes` gate).
  if (runs_hdr + 4 > region_end || ReadBE32(runs_hdr) != param) {
    return false;
  }
  const uint32_t runs = param;
  if (runs_hdr + 4 +
          (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
      region_end) {
    return false;
  }
  // Pass table over the dictionary; slot d is the clamp bucket (codes >= d
  // and rows past the run walk decode as raw 0).
  std::vector<uint8_t> pass(static_cast<size_t>(d) + 1);
  for (uint16_t e = 0; e < d; ++e) {
    pass[e] =
        RleDictMatch(
            op,
            static_cast<int64_t>(ReadBE64(dict + (static_cast<size_t>(e) * 8))),
            lit)
            ? 1
            : 0;
  }
  pass[d] = RleDictMatch(op, static_cast<int64_t>(0), lit) ? 1 : 0;
  uint64_t cnt = 0;
  __int128 sum = 0;
  uint64_t acc = 0;
  const char* q = runs_hdr + 4;
  for (uint32_t i = 0; i < runs && acc < n; ++i) {
    uint64_t code = 0;
    for (int b = 0; b < cb; ++b) {
      code = (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
    }
    const uint64_t len = ReadBE32(q + cb);
    // Hostile/overflow lengths clamp to the region's row budget (mirrors
    // the kRle arms everywhere else).
    const uint64_t next = acc + len;
    const uint64_t end = (next > n) ? n : next;
    if (end > acc) {
      const uint8_t ok = code < d ? pass[code] : pass[d];
      if (ok != 0) {
        cnt += end - acc;
        if (sum_out != nullptr && code < d) {
          sum += static_cast<__int128>(static_cast<int64_t>(
                     ReadBE64(dict + (static_cast<size_t>(code) * 8)))) *
                 static_cast<__int128>(end - acc);
        }
      }
    }
    acc = next;
    q += static_cast<size_t>(cb) + 4;
  }
  // Rows past the run walk decode as raw 0; they hit exactly when the clamp
  // bucket passes (mirrors the kRle predicate arm's tail handling).
  if (acc < n && pass[d] != 0) {
    cnt += n - acc;
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool RleBpRegionCountSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  RleBpRegion p;
  if (!ParseRleBpRegion(region, n, param, region_end, &p)) {
    return false;
  }
  uint64_t count = 0;
  __int128 sum = 0;
  for (uint32_t run = 0; run < p.runs; ++run) {
    const uint32_t length = RleBpLengthAt(p, run);
    const int64_t value = RleBpValueAt(p, run);
    if (!RleDictMatch(op, value, lit)) {
      continue;
    }
    count += length;
    if (sum_out != nullptr) {
      sum += static_cast<__int128>(value) * length;
    }
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool Simple8bRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out,
                            int64_t* sum_out) {
  if (count_out == nullptr || n == 0 || param == 0) {
    return false;
  }
  if (region_end != nullptr &&
      region + 8 + (static_cast<size_t>(param) * 8) > region_end) {
    return false;
  }
  const uint64_t base_u = ReadBE64(region);
  const auto s_op = static_cast<ScanOp>(op);
  uint64_t count = 0;
  uint64_t sum = 0;  // two's-complement u64 accumulator
  uint64_t remaining = n;
  const char* p = region + 8;
  for (uint32_t w = 0; w < param && remaining > 0; ++w, p += 8) {
    const uint64_t word = ReadBE64(p);
    const auto sel = static_cast<uint32_t>(word >> 60);
    const uint32_t cnt = kSimple8bSelCount[sel];
    const uint32_t bits = kSimple8bSelBits[sel];
    const auto avail = static_cast<uint32_t>(remaining);
    const uint32_t take = cnt < avail ? cnt : avail;
    // Word-level verdict from the [base, base+mask] value range: a word
    // that cannot match costs one endpoint compare instead of a decode.
    const Simple8bVerdict verdict =
        Simple8bWordVerdict(bits, base_u, lit, s_op);
    if (verdict == Simple8bVerdict::kNone) {
      remaining -= take;
      continue;
    }
    if (verdict == Simple8bVerdict::kAll && sum_out == nullptr) {
      // Whole word passes: count it from the selector alone. Zero-width
      // words (bits == 0) hit here too -- a 240-row run of the base value
      // folds in O(1).
      count += take;
      remaining -= take;
      continue;
    }
    const uint64_t mask = bits == 0 ? 0 : ((UINT64_C(1) << bits) - 1);
    if (verdict == Simple8bVerdict::kAll) {
      // SUM over a passing word still needs the offsets, but skip the
      // per-value predicate: every row counts.
      for (uint32_t j = 0; j < take; ++j) {
        const uint64_t off = (word >> (bits * (cnt - 1 - j))) & mask;
        sum += base_u + off;
      }
      count += take;
    } else {
      for (uint32_t j = 0; j < take; ++j) {
        const uint64_t off = (word >> (bits * (cnt - 1 - j))) & mask;
        if (Simple8bPasses(static_cast<int64_t>(base_u + off), s_op, lit)) {
          ++count;
          sum += base_u + off;
        }
      }
    }
    remaining -= take;
  }
  // Rows past the word walk decode as raw 0 (Int64RegionAt's fail-closed
  // zeros); fold them like the kRle tail so count/sum match the decode path.
  if (remaining > 0 && Simple8bPasses(static_cast<int64_t>(0), s_op, lit)) {
    count += remaining;
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

bool Rle8RegionCountSum(const char* region, uint32_t n, uint32_t param,
                        const char* region_end, PaxChunk::CompareOp op,
                        int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  Rle8Region parsed;
  if (!ParseRle8Region(region, n, param, region_end, &parsed)) {
    return false;
  }
  std::array<uint64_t, 16> bits{};
  if (!Rle8FillPredicateMask(parsed, op, lit, nullptr, bits.data())) {
    return false;
  }
  *count_out = Rle8MaskRange(bits.data(), 0, n);
  return sum_out == nullptr || Rle8SumMask(parsed, bits.data(), sum_out);
}

bool RleDeltaRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out,
                            int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  RleDeltaRegion parsed;
  if (!ParseRleDeltaRegion(region, n, param, region_end, &parsed)) {
    return false;
  }
  uint64_t count = 0;
  __int128 sum = 0;
  for (uint32_t run = 0; run < parsed.run_count; ++run) {
    const RleDeltaRun& item = parsed.runs[run];
    if (!RleDictMatch(op, item.value, lit)) {
      continue;
    }
    count += item.length;
    if (sum_out != nullptr) {
      sum += static_cast<__int128>(item.value) * item.length;
    }
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

// ---- Order-preserving run x dictionary (kOpRle) compressed-domain kernel --
//
// Same byte layout as kRleDict, but the dictionary is sorted ascending, so
// the predicate maps to a code window [lo, hi) via two binary searches --
// no pass table. Each run then folds with a single window compare
// (O(runs), same shape as RleDictRegionCountSum minus the d-entry scan).
bool OpRleRegionCountSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr || region == nullptr || region_end == nullptr ||
      region_end <= region || static_cast<size_t>(region_end - region) < 2) {
    return false;  // no [d u16] header to read: fail closed
  }
  const uint16_t d = ReadBE16(region);
  if (d == 0 || param == 0) {
    return false;
  }
  const int cb = (BitsNeeded(d - 1) + 7) / 8;
  const char* dict = region + 2;
  const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
  // Header cross-check: the footer's param IS the run count (mirrors the
  // kRleDict arm).
  if (runs_hdr + 4 > region_end || ReadBE32(runs_hdr) != param) {
    return false;
  }
  const uint32_t runs = param;
  if (runs_hdr + 4 +
          (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
      region_end) {
    return false;
  }
  const auto dict_at = [&](uint32_t e) {
    return static_cast<int64_t>(ReadBE64(dict + (static_cast<size_t>(e) * 8)));
  };
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
  switch (op) {
    case PaxChunk::CompareOp::kEq: {
      const uint32_t it = lb(lit);
      const bool found = it < d && dict_at(it) == lit;
      lo = it;
      hi = found ? lo + 1 : lo;
      break;
    }
    case PaxChunk::CompareOp::kLe:
      hi = ub(lit);
      break;
    case PaxChunk::CompareOp::kLt:
      hi = lb(lit);
      break;
    case PaxChunk::CompareOp::kGe:
      lo = lb(lit);
      break;
    case PaxChunk::CompareOp::kGt:
      lo = ub(lit);
      break;
    default:
      return false;
  }
  // Codes >= d decode as raw 0 and participate exactly when 0 passes.
  const bool zero_pass = RleDictMatch(op, static_cast<int64_t>(0), lit);
  uint64_t cnt = 0;
  __int128 sum = 0;
  uint64_t acc = 0;
  const char* q = runs_hdr + 4;
  for (uint32_t i = 0; i < runs && acc < n; ++i) {
    uint64_t code = 0;
    for (int b = 0; b < cb; ++b) {
      code = (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
    }
    const uint64_t len = ReadBE32(q + cb);
    const uint64_t next = acc + len;
    const uint64_t end = (next > n) ? n : next;
    if (end > acc) {
      const bool hit = (code >= lo && code < hi) || (code >= d && zero_pass);
      if (hit) {
        cnt += end - acc;
        if (sum_out != nullptr && code < d) {
          sum += static_cast<__int128>(dict_at(static_cast<uint32_t>(code))) *
                 static_cast<__int128>(end - acc);
        }
      }
    }
    acc = next;
    q += static_cast<size_t>(cb) + 4;
  }
  if (acc < n && zero_pass) {
    cnt += n - acc;
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

// ---- Binary interpolative coding (kInterp) compressed-domain kernel -------
bool InterpRegionCountSum(const char* region, uint32_t n, uint32_t param,
                          const char* region_end, PaxChunk::CompareOp op,
                          int64_t lit, uint64_t* count_out, int64_t* sum_out) {
  if (count_out == nullptr || region == nullptr || region_end == nullptr) {
    return false;
  }
  __int128 plo = 0, phi = 0;
  if (!InterpWindowForValue(op, lit, &plo, &phi)) {
    return false;
  }
  InterpRegion p;
  if (!ParseInterpRegion(region, n, param, region_end, &p)) {
    return false;
  }
  uint64_t cnt = 0;
  __int128 sum = 0;
  for (uint32_t b = 0; b < p.ncp; ++b) {
    const char* ck = p.ckpts + (static_cast<size_t>(b) * 16);
    // SIGNED bracket domain: a sign-crossing block's u64 words sort
    // backwards (see ParseInterpRegion).
    const auto first =
        static_cast<__int128>(static_cast<int64_t>(ReadBE64(ck)));
    const auto last =
        static_cast<__int128>(static_cast<int64_t>(ReadBE64(ck + 8)));
    // The decoded values of block b provably lie inside [first, last] (the
    // tree bounds close on them), so the bracket alone decides "cannot
    // match" and -- when no sum is requested -- "all match" too.
    if (last < plo || first > phi) {
      continue;
    }
    if (sum_out == nullptr && first >= plo && last <= phi) {
      const uint32_t s = b * kInterpBlockRows;
      const uint32_t t = std::min(p.n, (b + 1) * kInterpBlockRows);
      cnt += t - s;
      continue;
    }
    InterpEvalBlock(p, b, plo, phi, nullptr, sum_out != nullptr, &cnt, &sum);
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

__int128 DeltaPForBracketSum(const char* bracket) {
  const __int128 high = static_cast<int64_t>(ReadBE64(bracket + 16));
  const uint64_t low_bits = ReadBE64(bracket + 24);
  return (high * (static_cast<__int128>(1) << 64)) +
         static_cast<__int128>(low_bits);
}

bool DeltaPForRegionCountSum(const char* region, uint32_t n, uint32_t param,
                             const char* region_end, PaxChunk::CompareOp op,
                             int64_t lit, uint64_t* count_out,
                             int64_t* sum_out) {
  if (count_out == nullptr) {
    return false;
  }
  __int128 low_value = 0;
  __int128 high_value = 0;
  if (!InterpWindowForValue(op, lit, &low_value, &high_value)) {
    return false;
  }
  DeltaPForRegion parsed;
  if (!ParseDeltaPForRegion(region, n, param, region_end, &parsed)) {
    return false;
  }

  uint64_t count = 0;
  __int128 sum = 0;
  for (uint32_t block = 0; block < parsed.blocks; ++block) {
    const char* bracket =
        parsed.brackets + (static_cast<size_t>(block) * kDeltaPForBracketBytes);
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
      count += rows;
      if (sum_out != nullptr) {
        sum += DeltaPForBracketSum(bracket);
      }
      continue;
    }
    std::array<int64_t, kDeltaPForBlockRows> decoded{};
    DecodeDeltaPForBlock(parsed, block, decoded.data());
    for (uint32_t i = 0; i < rows; ++i) {
      const __int128 value = decoded[i];
      if (value < low_value || value > high_value) {
        continue;
      }
      ++count;
      if (sum_out != nullptr) {
        sum += value;
      }
    }
  }
  *count_out = count;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

namespace codec_benchmark {

bool OpDictRangeFold(const EncodedBlock& block, int64_t low, int64_t high,
                     uint32_t row_offset, std::span<uint64_t> output_mask,
                     uint64_t* count_out) {
  if (count_out == nullptr || block.kind != ValueKind::kInt64 ||
      block.scheme != Scheme::kOpDict || block.bytes.size() < 2 ||
      (!output_mask.empty() &&
       static_cast<uint64_t>(row_offset) + block.row_count >
           output_mask.size() * 64ULL)) {
    return false;
  }
  const char* region = block.bytes.data();
  const char* region_end = region + block.bytes.size();
  const uint16_t ncodes = ReadBE16(region);
  if (ncodes == 0 || ncodes > 1024 || block.param != ncodes) {
    return false;
  }
  const int code_width = BitsNeeded(ncodes - 1);
  const size_t dictionary_offset =
      2 +
      ((static_cast<size_t>(block.row_count) * static_cast<size_t>(code_width) +
        7) /
       8);
  const char* dictionary = region + dictionary_offset;
  if (dictionary + (static_cast<size_t>(ncodes) * sizeof(int64_t)) >
      region_end) {
    return false;
  }

  uint64_t lower_code = 0;
  uint64_t upper_code = 0;
  if (low < high) {
    uint64_t ignored_span = 0;
    bool ignored_clamp = false;
    OpDictWindowForValue(region, block.row_count, block.param,
                         PaxChunk::CompareOp::kGe, low, &lower_code,
                         &ignored_span, &ignored_clamp);
    uint64_t ignored_low = 0;
    OpDictWindowForValue(region, block.row_count, block.param,
                         PaxChunk::CompareOp::kLt, high, &ignored_low,
                         &upper_code, &ignored_clamp);
  }
  const uint64_t window = upper_code > lower_code ? upper_code - lower_code : 0;
  const bool clamp_pass = low <= 0 && 0 < high;
  WordBitReader reader(region + 2, dictionary);
  uint64_t count = 0;
  for (uint32_t row = 0; row < block.row_count; ++row) {
    const uint64_t code = reader.get(code_width);
    const bool hit = code < ncodes ? code - lower_code < window : clamp_pass;
    if (!hit) {
      continue;
    }
    ++count;
    if (!output_mask.empty()) {
      const uint64_t output_row = static_cast<uint64_t>(row_offset) + row;
      output_mask[output_row >> 6] |= 1ULL << (output_row & 63ULL);
    }
  }
  *count_out = count;
  return true;
}

}  // namespace codec_benchmark

uint32_t PaxChunk::ByteSliceNullAwareCount(const char* region, Scheme s,
                                           uint32_t param, uint32_t n,
                                           const uint8_t* null_bmp,
                                           CompareOp op, int64_t lit) {
  // BSI fast path: slice-parallel mask + popcount, no value materialization.
  if (s == Scheme::kByteSlice && n > 0 && n <= 1024) {
    const auto w = static_cast<uint8_t>(region[0]);
    if (w != 0 && w <= 64 && w == static_cast<uint8_t>(param)) {
      std::array<uint64_t, 16> bits{};
      ByteSliceCompareMask(region, n, op, lit, null_bmp, bits.data());
      uint64_t cnt = 0;
      const size_t words = (static_cast<size_t>(n) + 63) / 64;
      for (size_t k = 0; k < words; ++k) {
        cnt += static_cast<uint64_t>(__builtin_popcountll(bits[k]));
      }
      return static_cast<uint32_t>(cnt);
    }
  }
  // Simple-8b fast path: word-level range skip + zero-word range sets, no
  // per-value decode for pruned words. Null rows fall back to the generic
  // loop below (the word fold has no per-row null gate).
  if (s == Scheme::kSimple8b && n > 0 && null_bmp == nullptr) {
    uint64_t cnt = 0;
    if (Simple8bRegionCountSum(region, n, param, nullptr, op, lit, &cnt,
                               nullptr)) {
      return static_cast<uint32_t>(cnt);
    }
  }
  // kRleIdx fast path: the sorted run values make the matching runs one
  // contiguous window, so COUNT is a restart-index difference over two binary
  // searches -- no per-row decode. Null rows fall back to the generic loop
  // below (the run fold has no per-row null gate).
  if (s == Scheme::kRleIdx && n > 0 && null_bmp == nullptr) {
    uint64_t cnt = 0;
    if (RleIdxRegionCountSum(region, n, param, nullptr, op, lit, &cnt,
                             nullptr)) {
      return static_cast<uint32_t>(cnt);
    }
  }
  // kHuffDict fast path: the exact frequency table answers COUNT as a closed
  // form over the d symbols -- zero bitstream reads, zero row decode, no
  // run/progression structure required. Null rows fall back to the generic
  // loop below (the histogram fold has no per-row null gate).
  if (s == Scheme::kHuffDict && n > 0 && null_bmp == nullptr) {
    uint64_t cnt = 0;
    if (HuffDictRegionCountSum(region, n, param, nullptr, op, lit, &cnt,
                               nullptr)) {
      return static_cast<uint32_t>(cnt);
    }
  }
  // kPostings fast path: the exact count table answers COUNT as a closed
  // form over the lists the predicate passes -- zero bitstream reads, zero
  // row decode, any row order. A null bitmap is corrected analytically: the
  // closed form folds the RAW-value histogram (nulls are raw 0 by the
  // ComputeReference convention), and the null-aware count excludes null
  // rows, so the null rows are subtracted exactly when raw 0 matches the
  // literal. (The generic loop below cannot serve this scheme with a null
  // region_end: the Rice stream has no self-described length.)
  if (s == Scheme::kPostings && n > 0) {
    uint64_t cnt = 0;
    if (PostingsRegionCountSum(region, n, param, nullptr, op, lit, &cnt,
                               nullptr)) {
      if (null_bmp != nullptr) {
        uint64_t nulls = 0;
        for (uint32_t i = 0; i < n; ++i) {
          if ((null_bmp[i >> 3] >> (7 - (i & 7)) & 1) != 0) {
            ++nulls;
          }
        }
        bool zero_matches = false;
        switch (op) {
          case CompareOp::kEq:
            zero_matches = lit == 0;
            break;
          case CompareOp::kLe:
            zero_matches = lit >= 0;
            break;
          case CompareOp::kLt:
            zero_matches = lit > 0;
            break;
          case CompareOp::kGe:
            zero_matches = lit <= 0;
            break;
          case CompareOp::kGt:
            zero_matches = lit < 0;
            break;
        }
        if (zero_matches && cnt >= nulls) {
          cnt -= nulls;
        }
      }
      return static_cast<uint32_t>(cnt);
    }
  }
  // kTileDict fast path: bracket-pruned per-tile fold -- a wholly-outside
  // tile costs nothing, a wholly-inside tile is its row count with zero
  // code reads, a straddling tile walks <= 6-bit codes. Null rows fall
  // back to the generic loop below (the fold has no per-row null gate).
  if (s == Scheme::kTileDict && n > 0 && null_bmp == nullptr) {
    uint64_t cnt = 0;
    if (TileDictRegionCountSum(region, n, param, nullptr, op, lit, &cnt,
                               nullptr)) {
      return static_cast<uint32_t>(cnt);
    }
  }
  uint32_t count = 0;
  for (uint32_t i = 0; i < n; ++i) {
    if (null_bmp != nullptr && ((null_bmp[i >> 3] >> (7 - (i & 7)) & 1) != 0)) {
      continue;
    }
    const int64_t v = Int64RegionAt(region, s, param, n, i, nullptr);
    bool hit = false;
    switch (op) {
      case CompareOp::kEq:
        hit = (v == lit);
        break;
      case CompareOp::kLe:
        hit = (v <= lit);
        break;
      case CompareOp::kLt:
        hit = (v < lit);
        break;
      case CompareOp::kGe:
        hit = (v >= lit);
        break;
      case CompareOp::kGt:
        hit = (v > lit);
        break;
    }
    count += static_cast<uint32_t>(hit);
  }
  return count;
}

// =============================================================================
// Block-compression layer (item 6): a tiny LZ77-style codec over a raw byte
// region. Correctness and decode speed matter more than ratio here.
// Token stream:
//   literal run : control byte (high bit 0) = (run_len - 1); then run_len
//   bytes. match       : control byte (high bit 1) = 0x80 | (len - 3);
//                 then 2 LE bytes offset (1..65535). len in [3,130].
// =============================================================================
namespace {
struct LzDict {
  static constexpr size_t kMaxOff = 65535;
  std::vector<int32_t> tbl;
  LzDict() : tbl(1 << 16, -1) {}
  uint32_t hash(const char* p) const {
    uint32_t h = 0;
    for (int i = 0; i < 4; ++i) {
      h = (h * 31U) + static_cast<uint8_t>(p[i]);
    }
    return h & static_cast<uint32_t>(tbl.size() - 1);
  }
};
}  // namespace

std::vector<char> PaxChunk::CompressRegion(std::string_view in) {
  std::vector<char> out;
  const size_t n = in.size();
  if (n == 0) {
    return out;
  }
  const char* p = in.data();
  LzDict dict;
  size_t i = 0;
  size_t lit_start = 0;
  auto flush_lit = [&](size_t lit_end) {
    while (lit_start < lit_end) {
      // Cap literal runs at 127 so the control byte (run-1) stays in [0,126]
      // with the high bit CLEAR; values >=128 would collide with the match
      // token marker (high bit set).
      size_t run = std::min<size_t>(lit_end - lit_start, 127);
      if (run == 0) {
        break;
      }
      out.push_back(static_cast<char>(run - 1));
      out.insert(out.end(), p + lit_start, p + lit_start + run);
      lit_start += run;
    }
  };
  while (i < n) {
    size_t best_len = 0;
    size_t best_off = 0;
    if (i + 4 <= n) {
      uint32_t h = dict.hash(p + i);
      int32_t cand = dict.tbl[h];
      if (cand >= 0) {
        const auto cu = static_cast<size_t>(cand);
        if (cu + LzDict::kMaxOff >= i) {
          size_t maxlen = std::min<size_t>(n - i, 130);
          size_t l = 0;
          while (l < maxlen && p[cu + l] == p[i + l]) {
            ++l;
          }
          if (l >= 4) {
            best_len = l;
            best_off = i - cu;
          }
        }
      }
      dict.tbl[h] = static_cast<int32_t>(i);
    }
    if (best_len >= 4) {
      flush_lit(i);
      out.push_back(static_cast<char>(0x80 | ((best_len - 3) & 0x7F)));
      out.push_back(static_cast<char>(best_off & 0xFF));
      out.push_back(static_cast<char>((best_off >> 8) & 0xFF));
      i += best_len;
      lit_start = i;
    } else {
      ++i;
    }
  }
  flush_lit(n);
  return out;
}

std::vector<char> PaxChunk::DecompressRegion(std::string_view in,
                                             size_t uncompressed_size) {
  std::vector<char> out;
  out.reserve(uncompressed_size);
  const char* p = in.data();
  const size_t n = in.size();
  size_t pos = 0;
  while (pos < n && out.size() < uncompressed_size) {
    auto ctrl = static_cast<uint8_t>(p[pos++]);
    if ((ctrl & 0x80) == 0) {
      size_t run = ctrl + 1;
      // Inner bound on out.size(): a literal token carries up to 128 bytes
      // and the outer guard is only checked once per token, so the last
      // token of a hostile stream used to overshoot uncompressed_size by
      // up to ~128 bytes.
      for (size_t k = 0; k < run && pos < n && out.size() < uncompressed_size;
           ++k) {
        out.push_back(p[pos++]);
      }
    } else {
      size_t ml = (ctrl & 0x7F) + 3;
      if (pos + 2 > n) {
        break;  // truncated offset field
      }
      size_t off = static_cast<uint8_t>(p[pos]) |
                   (static_cast<size_t>(static_cast<uint8_t>(p[pos + 1])) << 8);
      pos += 2;
      // A corrupt/hostile match token can carry off == 0 or off > bytes
      // already emitted; `out.size() - off` would underflow to a huge index
      // and out[src++] would read/write out of bounds. Stop on such input.
      if (off == 0 || off > out.size()) {
        break;
      }
      size_t src = out.size() - off;
      for (size_t k = 0; k < ml && out.size() < uncompressed_size; ++k) {
        out.push_back(out[src++]);
      }
    }
  }
  return out;
}

StatusOr<std::vector<char>> PaxChunk::DecompressRegionExact(
    std::string_view in, size_t uncompressed_size) {
  // Allocation bound first: callers inflate pre-validation footer bytes, so a
  // hostile length must not become an unbounded reservation. Real footers are
  // O(rows) indexes over chunks of at most kHardMaxRows rows per pack record
  // (hash dominates at ~48B/row); 1 GiB covers even multi-million-row
  // standalone chunks with wide margin.
  constexpr size_t kMaxExactOutput = 1ULL * 1024 * 1024 * 1024;
  if (uncompressed_size > kMaxExactOutput) {
    return Status::kCorruption;
  }
  std::vector<char> out;
  if (uncompressed_size == 0) {
    return in.empty() ? StatusOr<std::vector<char>>(std::move(out))
                      : Status::kCorruption;
  }
  out.reserve(uncompressed_size);
  const char* p = in.data();
  const size_t n = in.size();
  size_t pos = 0;
  while (pos < n && out.size() < uncompressed_size) {
    auto ctrl = static_cast<uint8_t>(p[pos++]);
    if ((ctrl & 0x80) == 0) {
      size_t run = static_cast<size_t>(ctrl) + 1;
      // Exact size: a literal run must fit both the input remainder and the
      // output remainder, otherwise the stream is truncated or over-long.
      if (run > n - pos || run > uncompressed_size - out.size()) {
        return Status::kCorruption;
      }
      out.insert(out.end(), p + pos, p + pos + run);
      pos += run;
    } else {
      size_t ml = (static_cast<size_t>(ctrl & 0x7F)) + 3;
      if (pos + 2 > n) {
        return Status::kCorruption;  // truncated offset field
      }
      size_t off = static_cast<uint8_t>(p[pos]) |
                   (static_cast<size_t>(static_cast<uint8_t>(p[pos + 1])) << 8);
      pos += 2;
      if (off == 0 || off > out.size()) {
        return Status::kCorruption;
      }
      if (ml > uncompressed_size - out.size()) {
        return Status::kCorruption;
      }
      size_t src = out.size() - off;
      for (size_t k = 0; k < ml; ++k) {
        out.push_back(out[src++]);
      }
    }
  }
  // Fail-closed: exact byte count AND full stream consumption. Trailing
  // garbage, truncation, and overshoot all land here.
  if (out.size() != uncompressed_size || pos != n) {
    return Status::kCorruption;
  }
  return {std::move(out)};
}

// Xor8 peel scratch records. Same math and update order as the original
// cnt[]/xors[]/positions[]/hash[] formulation, so the emitted prefilter
// tables are unchanged. Positions fit u16 (table m <= 32 + 1.23*kRowGroupSize
// < 1300) and key indices fit u16 (n <= kRowGroupSize); the splitmix hash is
// recomputed only for the winning seed's table pass instead of being stored
// per seed -- keeping the whole working set inside L1 matters more than the
// 1024 extra hashes on ~8% of seed passes.
namespace {

#ifdef __x86_64__
// AVX-512 form of the xor8 per-seed hash loop: 8 keys per iteration run the
// two-round splitmix (vpmullq, AVX512DQ) and the three Barrett mods (the
// high-64 products come from 32x32->64 partial products, since vpmuludq is
// the only widening integer multiply). Bit-exact against
// XorHash64/FastMod64 -- the emitted seed, refs, and bucket counts are
// identical, so the peel and the stored table bytes do not change.
inline bool HasAvx512Dq() {
  static const bool kHas =
      __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw") &&
      __builtin_cpu_supports("avx512vl") && __builtin_cpu_supports("avx512dq");
  return kHas;
}

// DO NOT DROP the __attribute__((target(...))) on the AVX-512 bodies below.
// Clang ignores `#pragma GCC target` outright -- it warns "unknown pragma
// ignored" and then compiles the intrinsics with NO ISA feature, so every
// _mm512_* use fails with "requires target feature 'avx512f'" and the library
// does not build. clang is this project's default compiler and what CI builds,
// and the same file already uses the attribute form on DecodePlainAvx512, so
// the pragma region (kept for GCC) and the attribute (honoured by clang) are
// complementary: same ISA switch, same HasAvx512()/HasAvx512Dq() runtime
// gate, no behaviour difference between the two compilers. On a
// `template <...>` head the attribute goes AFTER the template parameter list,
// which is the only position where it applies to the body.
#define TINYLAMB_AVX512_TARGET \
  __attribute__((target("avx512f,avx512bw,avx512vl")))
#define TINYLAMB_AVX512DQ_TARGET \
  __attribute__((target("avx512f,avx512bw,avx512vl,avx512dq")))

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC push_options
#pragma GCC target("avx512f,avx512bw,avx512vl,avx512dq")
#endif
TINYLAMB_AVX512DQ_TARGET
inline __m512i MulHi64Avx512(__m512i x, uint64_t M) {
  // Keep the LOW 32 bits of each 64-bit lane (vpmuludq reads those only).
  const __m512i m32 =
      _mm512_setr_epi32(-1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0, -1, 0);
  const __m512i xlo = _mm512_and_si512(x, m32);
  const __m512i xhi = _mm512_srli_epi64(x, 32);
  const __m512i b0 =
      _mm512_set1_epi64(static_cast<long long>(M & 0xFFFFFFFFULL));
  const __m512i b1 = _mm512_set1_epi64(static_cast<long long>(M >> 32));
  const __m512i p00 = _mm512_mul_epu32(xlo, b0);
  const __m512i p01 = _mm512_mul_epu32(xlo, b1);
  const __m512i p10 = _mm512_mul_epu32(xhi, b0);
  const __m512i p11 = _mm512_mul_epu32(xhi, b1);
  const __m512i mid = _mm512_add_epi64(
      _mm512_add_epi64(_mm512_srli_epi64(p00, 32), _mm512_and_si512(p01, m32)),
      _mm512_and_si512(p10, m32));
  return _mm512_add_epi64(
      _mm512_add_epi64(p11, _mm512_srli_epi64(p01, 32)),
      _mm512_add_epi64(_mm512_srli_epi64(p10, 32), _mm512_srli_epi64(mid, 32)));
}

// FastMod64 over a vector: rem = x - hi64(recip*x)*m, one conditional
// subtract (the Barrett reciprocal lands on floor or floor-1).
TINYLAMB_AVX512DQ_TARGET
inline __m512i FastModVec(__m512i x, uint64_t m, uint64_t recip) {
  const __m512i q = MulHi64Avx512(x, recip);
  __m512i rem = _mm512_sub_epi64(
      x, _mm512_mullo_epi64(q, _mm512_set1_epi64(static_cast<long long>(m))));
  const __mmask8 ge = _mm512_cmpge_epu64_mask(
      rem, _mm512_set1_epi64(static_cast<long long>(m)));
  return _mm512_mask_sub_epi64(rem, ge, rem,
                               _mm512_set1_epi64(static_cast<long long>(m)));
}

// h = XorMix64(key + seed*GOLDEN); b0 = mod(h), b1 = mod(rotl(h,21)),
// b2 = mod(rotl(h,42)). refs_out holds (b0,b1,b2) per key (u16, m < 2^16);
// bucket counts are incremented in place.
TINYLAMB_AVX512DQ_TARGET
inline void Xor8HashBucketsAvx512(const uint64_t* keys, size_t n, uint32_t seed,
                                  uint64_t m, uint64_t recip,
                                  uint16_t* refs_out, uint16_t* buckets,
                                  uint64_t* hash_out) {
  const __m512i sk = _mm512_set1_epi64(static_cast<long long>(
      static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL));
  const __m512i c1 =
      _mm512_set1_epi64(static_cast<long long>(0xFF51AFD7ED558CCDULL));
  const __m512i c2 =
      _mm512_set1_epi64(static_cast<long long>(0xC4CEB9FE1A85EC53ULL));
  size_t k = 0;
  for (; k + 8 <= n; k += 8) {
    __m512i h = _mm512_add_epi64(
        _mm512_loadu_si512(reinterpret_cast<const __m512i*>(keys + k)), sk);
    h = _mm512_xor_si512(h, _mm512_srli_epi64(h, 33));
    h = _mm512_mullo_epi64(h, c1);
    h = _mm512_xor_si512(h, _mm512_srli_epi64(h, 33));
    h = _mm512_mullo_epi64(h, c2);
    h = _mm512_xor_si512(h, _mm512_srli_epi64(h, 33));
    const __m512i b0 = FastModVec(h, m, recip);
    const __m512i b1 = FastModVec(_mm512_rol_epi64(h, 21), m, recip);
    const __m512i b2 = FastModVec(_mm512_rol_epi64(h, 42), m, recip);
    if (hash_out != nullptr) {
      _mm512_storeu_si512(reinterpret_cast<__m512i*>(hash_out + k), h);
    }
    std::array<uint32_t, 16> t0{};
    std::array<uint32_t, 16> t1{};
    std::array<uint32_t, 16> t2{};  // 64B stores need the full register
    _mm512_storeu_si512(reinterpret_cast<__m512i*>(t0.data()), b0);
    _mm512_storeu_si512(reinterpret_cast<__m512i*>(t1.data()), b1);
    _mm512_storeu_si512(reinterpret_cast<__m512i*>(t2.data()), b2);
    for (size_t j = 0; j < 8; ++j) {
      // vpmuludq/vpmullq results live in the LOW 32 bits of each 64-bit lane.
      const uint32_t v0 = t0[j * 2], v1 = t1[j * 2], v2 = t2[j * 2];
      refs_out[(k + j) * 3U] = static_cast<uint16_t>(v0);
      refs_out[((k + j) * 3U) + 1] = static_cast<uint16_t>(v1);
      refs_out[((k + j) * 3U) + 2] = static_cast<uint16_t>(v2);
      ++buckets[v0];
      ++buckets[v1];
      ++buckets[v2];
    }
  }
  for (; k < n; ++k) {
    const uint64_t h = XorHash64(static_cast<int64_t>(keys[k]), seed);
    if (hash_out != nullptr) {
      hash_out[k] = h;
    }
    const uint32_t b0 = FastMod64(m)(h);
    const uint32_t b1 = FastMod64(m)(XorRotL(h, 21));
    const uint32_t b2 = FastMod64(m)(XorRotL(h, 42));
    refs_out[(k * 3) + 0] = static_cast<uint16_t>(b0);
    refs_out[(k * 3) + 1] = static_cast<uint16_t>(b1);
    refs_out[(k * 3) + 2] = static_cast<uint16_t>(b2);
    ++buckets[b0];
    ++buckets[b1];
    ++buckets[b2];
  }
}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif
#endif  // __x86_64__

struct XorKeyRef {
  uint16_t b0;
  uint16_t b1;
  uint16_t b2;
};

}  // namespace

StatusOr<PaxChunk> PaxChunk::BuildFromSortedRun(const SortedRun& run,
                                                const Schema& sc,
                                                bool compress) try {
  if (sc.ColumnCount() == 0 || sc.KeyColumnIndex() >= sc.ColumnCount()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "PAX requires a key column");
  }
  for (size_t i = 0; i < sc.ColumnCount(); ++i) {
    if (sc.GetColumn(i).IsInterval()) {
      return StatusError(StatusCode::kNotImplemented,
                         "PAX interval columns require tagged string kernels");
    }
  }
  // Verify the run contract before publishing binary-search zone maps.
  // Validate lengths here: Entry::BuildKey's compatibility clamp is not a
  // corruption check and must not turn a truncated key into a valid one.
  const auto& blob = run.Blob();
  std::string_view previous;
  bool have_previous = false;
  for (size_t i = 0; i < run.Size(); ++i) {
    const auto entry = run.GetEntry(i);
    const size_t offset = entry.key_offset_;
    if (offset > blob.size() || blob.size() - offset < sizeof(uint32_t)) {
      return StatusError(StatusCode::kCorrupt, "invalid sorted-run key offset");
    }
    uint32_t encoded_length = 0;
    std::memcpy(&encoded_length, blob.data() + offset, sizeof(encoded_length));
    const size_t length = be32toh(encoded_length);
    if (length > blob.size() - offset - sizeof(uint32_t)) {
      return StatusError(StatusCode::kCorrupt, "truncated sorted-run key");
    }
    const std::string_view key(blob.data() + offset + sizeof(uint32_t), length);
    if (have_previous && key < previous) {
      return StatusError(StatusCode::kCorrupt,
                         "sorted-run keys are out of order");
    }
    previous = key;
    have_previous = true;
    if (!entry.IsDeleted() && entry.row_offset_ >= blob.size()) {
      return StatusError(StatusCode::kCorrupt, "invalid sorted-run row offset");
    }
  }
  // Read all rows from the run in ascending order, collect into row groups of
  // kRowGroupSize. Skip deleted entries. For each row group, encode each
  // column contiguously. Record per-row-group per-column offsets in the
  // footer. Write footer inline at the end of data.
  std::vector<char> data;
  Footer footer;
  footer.schema_version = sc.SchemaVersion();
  footer.column_count = sc.ColumnCount();
  footer.key_column_index = sc.KeyColumnIndex();
  footer.key_column_type =
      static_cast<uint8_t>(sc.GetColumn(sc.KeyColumnIndex()).Type());
  // Secondary-index capacity opt-outs. Read once per build (getenv per call,
  // never cached in a function-local static, so setenv toggles in tests and
  // benchmarks take effect on the next build). Readers treat absent sections
  // as "answer by scan": native falls through to decode (VectorizedScanColumn
  // / XorRgMask pass-safe), WASM returns ~0ULL (inv) or a pass-all mask
  // (xor), so skipping these sections is never wrong, only slower on the
  // queries they accelerate. Documented in docs/05-pax-chunk.md.
  const bool kNoInvIdx = std::getenv("TINYLAMB_NO_INV_IDX") != nullptr;
  const bool kNoXor8 = std::getenv("TINYLAMB_NO_XOR8") != nullptr;
  // Adaptive-encoder env knobs, read once per build (getenv per call, never
  // cached in a function-local static, so setenv toggles in tests and
  // benchmarks take effect on the next build) and threaded into every row
  // group's EncodeColumnAdaptive without re-probing the environment.
  const AdaptiveEnv env{
      .force_plain = ForcePlain(),
      .no_new_schemes = std::getenv("TINYLAMB_NO_NEW_SCHEMES") != nullptr,
      .no_inv_idx = kNoInvIdx,
      .forced_scheme = std::getenv("TINYLAMB_FORCE_SCHEME")};

  // Front-load the big allocations: the column region's PLAIN bound and the
  // serialized hash-index table dominate the final chunk size, and letting
  // them double into place pays an mmap + full copy + munmap plus a page
  // fault per 4 KiB of every new block (~7k faults for a 30 MB block).
  // reserve() only fixes capacity, so over-estimating costs address space,
  // not resident pages; growth still works when the bound is beaten.
  {
    size_t reserve_bytes =
        (run.Size() * static_cast<size_t>(footer.column_count) * 8) +
        (64 << 10);
    if (footer.key_column_type == static_cast<uint8_t>(ValueType::kInt64) &&
        std::getenv("TINYLAMB_NO_HASH_IDX") == nullptr) {
      const size_t slots_per_row =
          std::getenv("TINYLAMB_SMALL_HASH_IDX") != nullptr ? 2 : 4;
      size_t slots = 1;
      while (slots < run.Size() * slots_per_row) {
        slots <<= 1;
      }
      reserve_bytes += slots * 12;
    }
    data.reserve(reserve_bytes);
  }

  SortedRun::Iterator it = run.Begin(sc);
  // Column-major cell scratch: fixed-width-only schemas decode rows straight
  // into these arrays so the profiler's cell reads stay on sequential
  // streams (see RgCells). Schemas with varchar columns keep the Row pool,
  // whose cells carry owned string storage.
  const bool use_soa = [sc]() {
    for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
      const ValueType t = sc.GetColumn(c).Type();
      if (t != ValueType::kInt64 && t != ValueType::kDouble) {
        return false;
      }
    }
    return sc.ColumnCount() > 0;
  }();
  RgCells soa;
  std::vector<ValueType> soa_col_types;
  if (use_soa) {
    soa.payload.resize(static_cast<size_t>(sc.ColumnCount()));
    soa.types.resize(static_cast<size_t>(sc.ColumnCount()));
    soa_col_types.reserve(static_cast<size_t>(sc.ColumnCount()));
    for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
      soa.payload[c].resize(kRowGroupSize);
      soa.types[c].resize(kRowGroupSize);
      soa_col_types.push_back(sc.GetColumn(c).Type());
    }
  }
  // Row POOL, not a per-group vector: rows are decoded in place into
  // group_rows[0..group_fill) and the pool persists across row groups, so
  // after the first group every DeserializeBounded reuses an existing cell
  // buffer instead of allocating a fresh vector<Value> per row. group_fill
  // counts the live rows of the group being built; encoding below sees the
  // pool as a span of that length.
  std::vector<Row> group_rows;
  if (!use_soa) {
    group_rows.reserve(kRowGroupSize);
  }
  size_t group_fill = 0;
  size_t total_rows = 0;
  // PGM learned index (v15): raw key-column values in global row order.
  // Disabled on any non-int64 (incl. NULL) key cell; sortedness is verified
  // at finalize time.
  std::vector<int64_t> pgm_keys;
  bool pgm_usable =
      sc.KeyColumnIndex() < sc.ColumnCount() &&
      (sc.GetColumn(sc.KeyColumnIndex()).Type() == ValueType::kInt64);
  // Front-loaded to the full key count: without it the key stream's
  // doublings repeat the data region's mmap/copy/fault churn.
  pgm_keys.reserve(kRowGroupSize);
  if (pgm_usable && run.Size() > kRowGroupSize) {
    pgm_keys.reserve(run.Size());
  }
  // Extended column stats (v12): per-column MIN/MAX / null count / value
  // frequency (bounded) accumulated across every row group, finalized into a
  // mode + histogram just before serialization. The profiler feeds it inline
  // during the fused row-group pass (kMaxModeKeys lives beside the profiler).
  std::vector<FlatMapI64> col_freq;
  col_freq.reserve(static_cast<size_t>(footer.column_count));
  for (slot_t cf = 0; cf < footer.column_count; ++cf) {
    col_freq.emplace_back(kMaxModeKeys);
  }
  // Per-column direct-mapped front cache for the mode map (see
  // ModeFrontEntry): created and reset with col_freq, whose counts it
  // mirrors lazily.
  std::vector<std::vector<ModeFrontEntry>> mode_front(
      footer.column_count, std::vector<ModeFrontEntry>(kModeFrontSize));
  // Incremental per-column mode tracking (first key to reach the running
  // max count); finalized into the footer after the row-group loop.
  std::vector<int64_t> col_mode_val(footer.column_count, 0);
  std::vector<uint64_t> col_mode_cnt(footer.column_count, 0);
  footer.ext_stats_present = true;
  footer.col_mins.assign(footer.column_count, INT64_MAX);
  footer.col_maxs.assign(footer.column_count, INT64_MIN);
  footer.col_nulls.assign(footer.column_count, 0);
  // Scratch reused by every row group: the profiler's per-column value arrays
  // and flat hash tables are sized once for the whole chunk.
  std::vector<RgColumnProfile> prof;
  // Xor8 peel scratch, reused by every (row group, column) build: bucket
  // counts, a bucket->key adjacency built by counting sort per seed, and
  // per-key hash/position records. The peel is pure math over these arrays,
  // so reuse cannot change the emitted table bytes.
  std::vector<uint64_t> xor_keys;
  std::vector<uint16_t> xor_buckets;
  std::vector<uint32_t> xor_start;
  std::vector<uint32_t> xor_fill;
  std::vector<uint16_t> xor_keylist;
  std::vector<XorKeyRef> xor_keyrefs;
  std::vector<uint32_t> xor_stack;
  std::vector<size_t> xor_order;
  std::vector<uint32_t> xor_peel_bucket;
  std::vector<char> xor_done;
  // Per-key XorHash64 words of the CURRENT attempt: the final scatter below
  // replays the winning seed's hashes, which its own hash pass already
  // computed. Grow-only like the other scratch.
  std::vector<uint64_t> xor_hash;
  while (it.IsValid()) {
    if (!it.IsDeleted()) {
      if (!use_soa) {
        if (group_fill == group_rows.size()) {
          group_rows.emplace_back();
        }
        if (!it.GetEntry().BuildRowInto(run.Blob(), sc,
                                        &group_rows[group_fill])) {
          return StatusError(StatusCode::kCorrupt, "invalid sorted-run row");
        }
        ++group_fill;
        ++total_rows;
      } else {
        BuildCellsInto(it.GetEntry(), run.Blob(), sc, soa_col_types, group_fill,
                       &soa);
        ++group_fill;
        ++total_rows;
      }
    }
    ++it;
    if (group_fill == kRowGroupSize || !it.IsValid()) {
      if (group_fill > 0) {
        soa.n = group_fill;
        const std::span<const Row> live_rows(
            use_soa ? std::span<const Row>()
                    : std::span<const Row>(group_rows.data(), group_fill));
        // Encode this row group: for each column, append contiguous array.
        footer.column_offsets.reserve(footer.column_offsets.size() +
                                      static_cast<size_t>(footer.column_count));
        // Footer offsets are uint32: a chunk whose data region crossed
        // 4 GiB would silently wrap every subsequent column_offset and
        // produce a self-consistent-looking but unreadable chunk. Return
        // an error before publishing any wrapped offsets.
        if (data.size() >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
          return StatusError(StatusCode::kCorrupt,
                             "PAX chunk exceeds uint32 data offset limit");
        }
        // Fused single-pass profile: every encoder input and footer-section
        // accumulator for this row group comes out of one row-major sweep
        // (no later stage re-walks Row objects).
        ProfileRowGroup(live_rows, sc, prof, col_freq, col_mode_val,
                        col_mode_cnt, pgm_keys, pgm_usable,
                        use_soa ? &soa : nullptr, mode_front);
        for (slot_t c = 0; c < footer.column_count; ++c) {
          auto col_off = static_cast<uint32_t>(data.size());
          footer.column_offsets.push_back(col_off);
          EncodedCol enc = EncodeColumnAdaptive(c, sc, live_rows, prof[c], env,
                                                use_soa ? &soa : nullptr);
          footer.encodings.push_back(static_cast<uint8_t>(enc.scheme));
          footer.params.push_back(enc.param);
          data.insert(data.end(), enc.buf.begin(), enc.buf.end());
        }
        footer.row_counts.push_back(static_cast<uint16_t>(group_fill));
        // Per-(RG,col) null bitmap (byte-slice / null-aware scan support).
        for (slot_t c = 0; c < footer.column_count; ++c) {
          if (prof[c].has_null) {
            footer.null_present = true;
          }
          footer.null_bitmaps.insert(footer.null_bitmaps.end(),
                                     prof[c].null_bmp.begin(),
                                     prof[c].null_bmp.end());
        }
        // Column statistics (v5): count/sum per int64 column, accumulated
        // across every row group. Non-int64 columns store zero placeholders.
        if (!footer.col_stats_present) {
          footer.col_stats_present = true;
          footer.col_sum_valid.assign(footer.column_count, 0);
          footer.col_counts.assign(footer.column_count, 0);
          footer.col_sums.assign(footer.column_count, 0);
        }
        footer.all_zm_present = true;
        footer.bloom_present = true;
        // Grow bloom array for this new group. The first group must allocate
        // exactly column_count entries (rg*cc total across all groups); do NOT
        // pre-fill here or the serialized section doubles in size and the
        // reader (which expects rg*cc) mis-parses every later footer section.
        const size_t prev_sz = footer.rg_blooms.size();
        footer.rg_blooms.resize(prev_sz +
                                static_cast<size_t>(footer.column_count));
        for (size_t i = prev_sz; i < footer.rg_blooms.size(); ++i) {
          footer.rg_blooms[i] = 0;
        }

        footer.rg_sums_present = true;
        // Current 0-based row-group index (row_counts is pushed per group).
        const size_t g_idx = footer.row_counts.size() - 1;
        for (slot_t c = 0; c < footer.column_count; ++c) {
          const RgColumnProfile& P = prof[c];
          const bool is_i64 = P.is_i64;
          footer.col_sum_valid[c] = is_i64 ? 1 : 0;
          footer.col_counts[c] += P.count;
          if (is_i64) {
            footer.col_nulls[c] += P.null_count;
          }
          if (is_i64) {
            footer.col_sums[c] =
                static_cast<int64_t>(static_cast<uint64_t>(footer.col_sums[c]) +
                                     static_cast<uint64_t>(P.sum));
          }
          if (is_i64) {
            footer.col_mins[c] = std::min(P.cmin, footer.col_mins[c]);
            footer.col_maxs[c] = std::max(P.cmax, footer.col_maxs[c]);
          }
          // The profile's integer bounds do not describe VARCHAR/DOUBLE
          // cells. Keep those domains unbounded so RGOverlapsCol cannot
          // discard their row groups using the placeholder [0, 0].
          footer.all_zone_mins.push_back(is_i64 ? P.mn : INT64_MIN);
          footer.all_zone_maxs.push_back(is_i64 ? P.mx : INT64_MAX);
          const size_t bi = (g_idx * footer.column_count) + c;
          if (bi < footer.rg_blooms.size()) {
            footer.rg_blooms[bi] = P.bloom;
          }
          // Per-RG sums fold the profile accumulator directly (NULL cells
          // contribute nothing, matching the historical computation).
          footer.rg_sums.push_back(P.sum);
        }
        // HLL sketch per (rg,col): registers arrive fused in the profile.
        if (!footer.hll_present) {
          footer.hll_present = true;
          footer.hll_sketches.assign(
              static_cast<size_t>(footer.column_count) * 64, 0);
        }
        // Current 0-based row-group index (row_counts already pushed for this
        // group).
        const size_t hll_base =
            g_idx * static_cast<size_t>(footer.column_count) * 64;
        if (footer.hll_sketches.size() <
            hll_base + (static_cast<size_t>(footer.column_count) * 64)) {
          footer.hll_sketches.resize(
              hll_base + (static_cast<size_t>(footer.column_count) * 64), 0);
        }
        for (slot_t c = 0; c < footer.column_count; ++c) {
          if (!prof[c].is_i64) {
            continue;
          }
          uint8_t* regs =
              &footer.hll_sketches[hll_base + (static_cast<size_t>(c) * 64)];
          for (int r = 0; r < 64; ++r) {
            regs[r] = std::max(prof[c].hll[r], regs[r]);
          }
        }
        // Inverted secondary index (v13): postings arrive fused in the
        // profile (same abort threshold); only the cell assembly (sort +
        // publish) remains here. TINYLAMB_NO_INV_IDX skips the whole section.
        {
          const auto rg_idx =
              static_cast<uint32_t>(footer.row_counts.size() - 1);
          for (slot_t c = 0; c < footer.column_count && !kNoInvIdx; ++c) {
            if (!prof[c].is_i64) {
              continue;
            }
            // A kPostings cell IS the inverted index: the codec carries the
            // posting lists compressed behind the exact count table, so the
            // raw footer copy would be dead weight (every query path this
            // index serves falls back to the scan kernels, which hit the
            // closed forms on these cells).
            const size_t ecell =
                (static_cast<size_t>(rg_idx) * footer.column_count) + c;
            if (static_cast<Scheme>(footer.encodings[ecell]) ==
                Scheme::kPostings) {
              continue;
            }
            const auto& post = prof[c].postings;
            if (prof[c].postings_big || post.key_count < 2) {
              continue;
            }
            InvertedCell cell;
            cell.group = rg_idx;
            cell.column = c;
            cell.postings.reserve(post.key_count);
            for (size_t k = 0; k < post.key_count; ++k) {
              InvertedPosting p;
              p.value = post.keys[k];
              p.rows.assign(post.rows.begin() + post.offsets[k],
                            post.rows.begin() + post.offsets[k + 1]);
              cell.postings.push_back(std::move(p));
            }
            std::sort(cell.postings.begin(), cell.postings.end(),
                      [](const InvertedPosting& a, const InvertedPosting& b) {
                        return a.value < b.value;
                      });
            footer.inv_idx_present = true;
            footer.inv_index.push_back(std::move(cell));
          }
        }
        // Xor8 pre-filter (v14): fingerprint tables for int64 cells the
        // inverted index skips (fewer than 2 or more than 64 distinct raw
        // values). Standard peeling construction over splitmix positions;
        // retries with a new seed on cycles (the seed is stored per cell).
        // TINYLAMB_NO_XOR8 skips the whole section. Note the inv_covered scan
        // below sees an empty inv_index when TINYLAMB_NO_INV_IDX is set, so a
        // no-inv build routes low-cardinality cells into xor tables unless
        // both flags are set; the flags are orthogonal by design.
        {
          const auto rg_idx =
              static_cast<uint32_t>(footer.row_counts.size() - 1);
          // Scratch is chunk-scoped (declared beside `prof`): the peel
          // tables and the kidx map are reused by every (row group, column)
          // build below, so the hot ~70 KiB working set stays put instead
          // of being malloc'd, filled cold, and freed ~3 times per row
          // group.
          for (slot_t c = 0; c < footer.column_count && !kNoXor8; ++c) {
            if (!prof[c].is_i64) {
              continue;
            }
            // Skip cells the inverted index already covers exactly. Cells
            // are pushed in ascending (group, column) order, so this group's
            // cells sit at the tail: scanning backwards reaches them in a
            // handful of steps where the forward scan walked every cell of
            // every earlier row group first.
            bool inv_covered = false;
            for (auto& rit : std::ranges::reverse_view(footer.inv_index)) {
              if (rit.group < rg_idx) {
                break;
              }
              if (rit.group == rg_idx && rit.column == c) {
                inv_covered = true;
                break;
              }
            }
            if (inv_covered) {
              continue;
            }
            // Distinct keys arrive fused in the profile (row-order
            // insertion, same content as the historical dedup pass).
            std::vector<uint64_t>& keys = xor_keys;
            keys.clear();
            keys.reserve(prof[c].xorset.size());
            prof[c].xorset.ForEach(
                [&keys](uint64_t k, uint64_t) { keys.push_back(k); });
            if (keys.empty()) {
              continue;
            }
            const size_t n = keys.size();
            // Every attempt runs at the wide load (m/n = 1.6). The
            // historical base load (m/n ~ 1.23) sits right at the 3-XOR
            // 2-core percolation threshold: measured on the tpch builder
            // shapes, ~90% of cells failed their base-load attempts, so
            // keeping one base try cost a full build pass per cell for a
            // ~30%-narrower table on the ~10% of cells that peeled there.
            // Readers take m from cell.table.size(), so the wire stays
            // compatible; the peel and emitted bytes per (seed, m) are
            // unchanged, only the seed-0 load moved.
            size_t m = 32 + ((160 * n + 99) / 100);
            XorCell built;
            bool ok = false;
            auto& buckets = xor_buckets;
            auto& start = xor_start;
            auto& fill = xor_fill;
            auto& keylist = xor_keylist;
            auto& refs = xor_keyrefs;
            auto& stack = xor_stack;
            auto& order = xor_order;
            auto& peel_bucket = xor_peel_bucket;
            auto& done = xor_done;
            for (uint32_t seed = 0; seed < 32 && !ok; ++seed) {
              const FastMod64 mod(m);
              // Per-seed state: bucket counts, a bucket->key adjacency built
              // by counting sort (replaces the hash-map resolve the peel
              // used to run per popped bucket), and per-key hash/positions.
              buckets.assign(m, 0);
              refs.resize(n);
              if (xor_hash.size() < n) {
                xor_hash.resize(n);
              }
#ifdef __x86_64__
              if (HasAvx512Dq()) {
                Xor8HashBucketsAvx512(keys.data(), n, seed, m, ~0ULL / m,
                                      reinterpret_cast<uint16_t*>(refs.data()),
                                      buckets.data(), xor_hash.data());
              } else
#endif
              {
                for (size_t k = 0; k < n; ++k) {
                  const uint64_t h =
                      XorHash64(static_cast<int64_t>(keys[k]), seed);
                  xor_hash[k] = h;
                  const uint32_t b0 = mod(h);
                  const uint32_t b1 = mod(XorRotL(h, 21));
                  const uint32_t b2 = mod(XorRotL(h, 42));
                  refs[k] = {.b0 = static_cast<uint16_t>(b0),
                             .b1 = static_cast<uint16_t>(b1),
                             .b2 = static_cast<uint16_t>(b2)};
                  ++buckets[b0];
                  ++buckets[b1];
                  ++buckets[b2];
                }
              }
              // One pass fills the exclusive prefix (start); the per-bucket
              // placement cursor (fill) is a byte-identical memcpy of it,
              // which the vectorized copy runs ~3x cheaper than a second
              // scalar walk. resize() instead of assign(): the loop below
              // rewrites every slot, so the zero-fill would be pure waste
              // on the seeds that reuse a same-sized table.
              start.resize(m + 1);
              fill.resize(m);
              // The singleton-bucket stack seeds in the same pass that
              // computes the exclusive prefix: both walk buckets in
              // ascending order, and the peel only consumes the finished
              // stack after the scatter below, so fusing them is invisible.
              stack.reserve(m);
              uint32_t* const stk = stack.data();
              uint32_t sp = 0;
              uint32_t cursor = 0;
              for (uint32_t b = 0; b < m; ++b) {
                start[b] = cursor;
                cursor += buckets[b];
                if (buckets[b] == 1) {
                  stk[sp++] = b;
                }
              }
              start[m] = cursor;
              std::memcpy(fill.data(), start.data(), m * sizeof(uint32_t));
              // Grow-only: the scatter below rewrites exactly the 3n slots
              // it uses, so once the scratch holds a prior cell's larger
              // size, keeping it skips resize's zero-fill of the tail.
              if (keylist.size() < n * 3) {
                keylist.resize(n * 3);
              }
              for (size_t k = 0; k < n; ++k) {
                const XorKeyRef& r = refs[k];
                keylist[fill[r.b0]++] = static_cast<uint16_t>(k);
                keylist[fill[r.b1]++] = static_cast<uint16_t>(k);
                keylist[fill[r.b2]++] = static_cast<uint16_t>(k);
              }
              // Raw-array stack and peel order: every peel step pushed 1-3
              // times through vector::push_back's capacity branch and finish
              // pointer, and the peel is pure array math over the reused
              // chunk-scoped scratch. Capacities are one reserve() compare
              // after the first cell; push order (and therefore the emitted
              // table bytes, which depend on peel order) is unchanged.
              // (The stack was already seeded by the fused prefix pass.)
              order.reserve(keys.size());
              size_t* const ord = order.data();
              uint32_t osz = 0;
              // Grow-only, same convention as keylist: the peel stamps
              // peel_bucket for every key it orders before the size check
              // below reads it, and a failed attempt re-runs the whole
              // pass, so no slot is ever read before it is written.
              if (peel_bucket.size() < keys.size()) {
                peel_bucket.resize(keys.size());
              }
              // done[] stays a parallel bitmap: key ids are consumed once but
              // appear in THREE adjacency lists, so stamping only the found
              // keylist slot leaves the other two lists still offering the
              // consumed key (re-peel, wrapped bucket counts, hang). One
              // random load per scanned entry is the price of that safety.
              done.assign(keys.size(), 0);
              while (sp != 0) {
                const uint32_t b = stk[--sp];
                if (buckets[b] != 1) {
                  continue;
                }
                // The count says one live placement sits at b; find it in
                // the bucket's adjacency list.
                uint32_t k = UINT32_MAX;
                for (uint32_t idx = start[b]; idx < start[b + 1]; ++idx) {
                  const uint16_t cand = keylist[idx];
                  if (done[cand] == 0) {
                    k = cand;
                    break;
                  }
                }
                if (k == UINT32_MAX) {
                  continue;  // stale bucket
                }
                done[k] = 1;
                peel_bucket[k] = b;
                ord[osz++] = k;
                const XorKeyRef& ref = refs[k];
                if (--buckets[ref.b0] == 1) {
                  stk[sp++] = ref.b0;
                }
                if (--buckets[ref.b1] == 1) {
                  stk[sp++] = ref.b1;
                }
                if (--buckets[ref.b2] == 1) {
                  stk[sp++] = ref.b2;
                }
              }
              if (osz != keys.size()) {
                continue;  // cycle: new seed
              }
              built.group = rg_idx;
              built.column = c;
              built.seed = seed;
              built.table.assign(m, 0);
              for (uint32_t t = osz; t-- > 0;) {
                const size_t k = ord[t];
                const XorKeyRef& ref = refs[k];
                // This seed's splitmix output, stashed by the hash pass
                // above (the winning attempt IS the last attempt).
                const uint64_t h = xor_hash[k];
                const auto fp = static_cast<uint8_t>(h & 0xFF);
                const uint32_t b0 = peel_bucket[k];
                uint8_t v = fp;
                if (ref.b0 != b0) {
                  v ^= built.table[ref.b0];
                }
                if (ref.b1 != b0) {
                  v ^= built.table[ref.b1];
                }
                if (ref.b2 != b0) {
                  v ^= built.table[ref.b2];
                }
                built.table[b0] = v;
              }
              ok = true;
            }
            if (!ok) {
              continue;
            }
            footer.xor_present = true;
            footer.xor_cells.push_back(std::move(built));
          }
        }
        // Hash index: build from int64 key column (O(1) point lookups).
        // Opt-out for huge bulk chunks where 12B x 4xrows outweighs data:
        // set TINYLAMB_NO_HASH_IDX=1. Halve the table (load <= 0.5, ~24B/row)
        // with TINYLAMB_SMALL_HASH_IDX=1: same wire format (readers derive the
        // size from hash_idx_log2), only longer probe chains. With neither
        // flag the PGM learned index (v15, tens of bytes) plus zone-map
        // binary search serves point lookups instead (see PointLookup).
        // Read per call (see kNoNewSchemes): a function-local static would
        // latch the first build's env answer.
        const bool kNoHashIdx = std::getenv("TINYLAMB_NO_HASH_IDX") != nullptr;
        const bool kSmallHashIdx =
            std::getenv("TINYLAMB_SMALL_HASH_IDX") != nullptr;
        if (!kNoHashIdx && !footer.hash_idx_present &&
            sc.GetColumn(sc.KeyColumnIndex()).Type() == ValueType::kInt64) {
          footer.hash_idx_present = true;
          const size_t kHashSlotsPerRow = kSmallHashIdx ? 2 : 4;
          size_t sz = 1;
          while (sz < run.Size() * kHashSlotsPerRow) {
            sz <<= 1;
          }
          footer.hash_idx_log2 = 0;
          while ((1ULL << footer.hash_idx_log2) < sz) {
            ++footer.hash_idx_log2;
          }
          footer.hash_keys.assign(sz, INT64_MIN);
          footer.hash_rows.assign(sz, UINT32_MAX);
        }
        if (footer.hash_idx_present &&
            sc.GetColumn(sc.KeyColumnIndex()).Type() == ValueType::kInt64) {
          const auto mask =
              static_cast<uint32_t>((1ULL << footer.hash_idx_log2) - 1);
          const size_t base_row = total_rows - group_fill;
          const RgColumnProfile& KP = prof[sc.KeyColumnIndex()];
          // NULL key cells have no int64 value: skip them for the hash
          // index (point lookups on int64 keys never match NULL). A
          // mistyped non-null cell reproduces the historical throwing walk
          // instead of the fused array loop.
          if (KP.typed_clean) {
            const size_t nk = KP.vals.size();
            // The slot of key ri is computable from the key alone, so the
            // table lines for key ri+dist are prefetched while the current
            // insert's murmur chain runs: the inserts are ~1M random touches
            // into a table several times L2, and the probe loop is otherwise
            // a serial miss per row. Same keys, same order, same table.
            constexpr size_t kHashPfDist = 16;
            // Locals, not the KP members: the per-row stores into
            // hash_keys/hash_rows can alias the profile in the compiler's
            // view, which pinned the has_null load + branch inside the loop
            // (one reload per row on the hottest build path). Snapshotting
            // the flag and the bitmap pointer into locals lets both live in
            // registers; same rows enter the table either way.
            const bool kp_has_null = KP.has_null;
            const uint8_t* const kp_null_bmp = KP.null_bmp.data();
            for (size_t ri = 0; ri < nk; ++ri) {
              if (ri + kHashPfDist < nk) {
                const int64_t kpf = KP.vals[ri + kHashPfDist];
                auto hpf = static_cast<uint64_t>(kpf);
                hpf ^= hpf >> 33;
                hpf *= 0xFF51AFD7ED558CCDULL;
                hpf ^= hpf >> 33;
                const auto spf = static_cast<uint32_t>(hpf & mask);
                __builtin_prefetch(&footer.hash_rows[spf]);
                __builtin_prefetch(&footer.hash_keys[spf]);
              }
              // A null-free key column's bitmap is all zeros, so gating the
              // per-row bitmap test on has_null removes the load+shift+test
              // from every row. Same rows enter the table either way.
              if (kp_has_null &&
                  ((kp_null_bmp[ri >> 3] >> (7 - (ri & 7))) & 1) != 0) {
                continue;
              }
              const int64_t k = KP.vals[ri];
              auto h = static_cast<uint64_t>(k);
              h ^= h >> 33;
              h *= 0xFF51AFD7ED558CCDULL;
              h ^= h >> 33;
              auto s = static_cast<uint32_t>(h & mask);
              while (footer.hash_rows[s] != UINT32_MAX &&
                     footer.hash_keys[s] != k) {
                s = (s + 1) & mask;
              }
              // Duplicate keys within a chunk resolve to the FIRST row
              // (matching PointLookup's lower-bound semantics): keep the
              // earliest occurrence instead of overwriting with the later one.
              if (footer.hash_rows[s] != UINT32_MAX) {
                continue;
              }
              footer.hash_keys[s] = k;
              footer.hash_rows[s] = static_cast<uint32_t>(base_row + ri);
            }
          } else {
            for (size_t ri = 0; ri < group_fill; ++ri) {
              if (group_rows[ri][sc.KeyColumnIndex()].IsNull()) {
                continue;
              }
              const int64_t k = group_rows[ri][sc.KeyColumnIndex()].AsInt64();
              auto h = static_cast<uint64_t>(k);
              h ^= h >> 33;
              h *= 0xFF51AFD7ED558CCDULL;
              h ^= h >> 33;
              auto s = static_cast<uint32_t>(h & mask);
              while (footer.hash_rows[s] != UINT32_MAX &&
                     footer.hash_keys[s] != k) {
                s = (s + 1) & mask;
              }
              if (footer.hash_rows[s] != UINT32_MAX) {
                continue;
              }
              footer.hash_keys[s] = k;
              footer.hash_rows[s] = static_cast<uint32_t>(base_row + ri);
            }
          }
        }
        // Zone map: rows arrive ascending by key from the merge, so the
        // first/last key of the group are its min/max. Only for int64 keys.
        // NULL key cells cannot be ordered: compute min/max over non-null
        // keys, and leave this group's zone entry out (disabling the map
        // for the chunk) when no orderable key exists. Bounds arrive fused
        // in the key column profile (a mistyped cell already threw during
        // profiling, exactly like the historical walk).
        if (sc.GetColumn(footer.key_column_index).Type() == ValueType::kInt64) {
          const RgColumnProfile& KP = prof[footer.key_column_index];
          const bool has_key = KP.mn <= KP.mx;
          const int64_t mn = KP.mn, mx = KP.mx;
          if (has_key) {
            // Keep group_min/max arrays aligned with row_groups: once a
            // group without keys disables the map, it stays disabled (later
            // groups cannot re-enable with a shorter array).
            if (footer.row_counts.size() == 1 || footer.zone_map_present) {
              footer.zone_map_present = true;
              footer.group_min_keys.push_back(mn);
              footer.group_max_keys.push_back(mx);
            }
          } else {
            // No orderable key in this group: zone-map pruning would be
            // unsound, so disable it for the whole chunk.
            footer.zone_map_present = false;
            footer.group_min_keys.clear();
            footer.group_max_keys.clear();
          }
        }
        footer.row_group_count++;
        group_fill = 0;  // pool rows (and their buffers) survive for reuse
      }
    }
  }
  // Finalize the PGM learned index (v15): ShrinkingCone piecewise-linear fit
  // over the ascending key column (error eps rows). Partitioned segments;
  // each covers [start, i) with a slope inside the surviving cone.
  {
    constexpr uint32_t kPgmEps = 16;
    bool ascending = pgm_usable && !pgm_keys.empty();
    for (size_t i = 1; ascending && i < pgm_keys.size(); ++i) {
      if (pgm_keys[i] < pgm_keys[i - 1]) {
        ascending = false;
      }
    }
    if (ascending) {
      // Double, not long double: the cone arithmetic only feeds a slope
      // whose predicted-position error is bounded by (rows/2^53) -- for any
      // row count that fits a chunk this is orders of magnitude below the
      // kPgmEps=16-row search slack, so segment feasibility decisions are
      // unchanged except at rounding-tie boundaries, and the x87 long
      // double divide pair per point dominated this loop on large keys.
      std::vector<PgmSegment> segs;
      size_t start = 0;
      int64_t key0 = pgm_keys[0];
      double low = -std::numeric_limits<double>::infinity();
      double high = std::numeric_limits<double>::infinity();
      const size_t n = pgm_keys.size();
      for (size_t i = 1; i < n; ++i) {
        if (pgm_keys[i] == key0) {
          if (i - start > kPgmEps) {
            const double slope = (low + high) / 2;
            segs.push_back({key0, start, std::isfinite(slope) ? slope : 0.0});
            start = i;
            key0 = pgm_keys[i];
            low = -std::numeric_limits<double>::infinity();
            high = std::numeric_limits<double>::infinity();
          }
          continue;
        }
        const auto dx = static_cast<double>(static_cast<uint64_t>(pgm_keys[i]) -
                                            static_cast<uint64_t>(key0));
        const auto rel = static_cast<double>(i - start);
        const double slo = (rel - static_cast<double>(kPgmEps)) / dx;
        const double shi = (rel + static_cast<double>(kPgmEps)) / dx;
        low = std::max(slo, low);
        high = std::min(shi, high);
        if (low > high) {
          // Recompute the cone bound without point i (it starts the next
          // segment); the emitted slope fits [start, i).
          low = -std::numeric_limits<double>::infinity();
          high = std::numeric_limits<double>::infinity();
          for (size_t j = start + 1; j < i; ++j) {
            if (pgm_keys[j] == key0) {
              continue;
            }
            const auto dx2 =
                static_cast<double>(static_cast<uint64_t>(pgm_keys[j]) -
                                    static_cast<uint64_t>(key0));
            const auto rel2 = static_cast<double>(j - start);
            const double s2 = (rel2 - static_cast<double>(kPgmEps)) / dx2;
            const double s3 = (rel2 + static_cast<double>(kPgmEps)) / dx2;
            low = std::max(s2, low);
            high = std::min(s3, high);
          }
          const double slope = (low + high) / 2;
          segs.push_back({key0, start, std::isfinite(slope) ? slope : 0.0});
          start = i;
          key0 = pgm_keys[i];
          low = -std::numeric_limits<double>::infinity();
          high = std::numeric_limits<double>::infinity();
        }
      }
      const double slope = (low + high) / 2;
      segs.push_back({key0, start, std::isfinite(slope) ? slope : 0.0});
      footer.pgm_present = true;
      footer.pgm_eps = kPgmEps;
      footer.pgm_segments = std::move(segs);
    }
  }
  // Finalize extended column stats: mode + 16-bin equi-width histogram over
  // each column's [min,max], from the bounded frequency map.
  footer.col_modes.assign(footer.column_count, 0);
  footer.col_mode_counts.assign(footer.column_count, 0);
  footer.col_hists.assign(
      static_cast<size_t>(footer.column_count) * Footer::ColumnStats::HIST_BINS,
      0);
  // Fold every still-cached mode-front delta back into col_freq: the
  // histogram below reads the map contents, so the caches must be empty of
  // unsynced counts before it runs.
  for (slot_t c = 0; c < footer.column_count; ++c) {
    auto& fm = col_freq[c];
    for (const ModeFrontEntry& fe : mode_front[c]) {
      if (fe.count != fe.synced) {
        *fm.UpsertSlot(static_cast<uint64_t>(fe.key)) += fe.count - fe.synced;
      }
    }
  }
  for (slot_t c = 0; c < footer.column_count; ++c) {
    const bool is_i64 = sc.GetColumn(c).Type() == ValueType::kInt64;
    if (!is_i64) {
      continue;
    }
    const int64_t mn = footer.col_mins[c];
    const int64_t mx = footer.col_maxs[c];
    // mn > mx only for all-NULL columns (no values inserted); col_freq is
    // empty in that case and we skip to avoid the signed-overflow on
    // (mx - mn) and a meaningless binning.
    if (mx < mn) {
      continue;
    }
    // Mode arrived from the profiler's incremental tracking: the first key
    // to reach the max count wins, which is deterministic (the historical
    // extraction read it off hash-bucket iteration order).
    footer.col_modes[c] = col_mode_val[c];
    footer.col_mode_counts[c] = col_mode_cnt[c];
    constexpr int B = Footer::ColumnStats::HIST_BINS;
    col_freq[c].ForEach([&](uint64_t key, uint64_t cnt) {
      const uint64_t idx = HistogramBin(static_cast<int64_t>(key), mn, mx, B);
      footer.col_hists[(static_cast<size_t>(c) * B) + idx] += cnt;
    });
  }
  // Write inline footer at end of data (v4): schema_version(8) +
  // row_group_count(4) + column_count(2) + key_column_index(2) +
  // key_column_type(1) + zone_map_present(1) + generation(8) +
  // row_counts (row_group_count * 2) + column_offsets (row_group_count *
  // column_count * 4) + [group_min_keys + group_max_keys (row_group_count *
  // 8 each, if zone map present)] + encodings (rg*col bytes) +
  // params (rg*col * 4) + footer_size(4). All big-endian.
  size_t footer_start = data.size();
  // Read per call (see kNoNewSchemes): a function-local static would latch
  // the first build's answer, silently ignoring later setenv toggles.
  const bool kNoCrc = std::getenv("TINYLAMB_NO_CRC") != nullptr;
  footer.format_version = kNoCrc ? 1U : 2U;
  AppendBE64(data, footer.schema_version);
  AppendBE32(data, footer.format_version);
  AppendBE32(data, footer.row_group_count);
  AppendBE16(data, footer.column_count);
  AppendBE16(data, footer.key_column_index);
  data.push_back(static_cast<char>(
      static_cast<uint8_t>(sc.GetColumn(footer.key_column_index).Type())));
  data.push_back(static_cast<char>(footer.zone_map_present ? 1 : 0));
  footer.generation = run.Generation();
  AppendBE64(data, footer.generation);
  for (uint16_t rc : footer.row_counts) {
    AppendBE16(data, rc);
  }
  for (uint32_t off : footer.column_offsets) {
    AppendBE32(data, off);
  }
  if (footer.zone_map_present) {
    for (int64_t mn : footer.group_min_keys) {
      AppendBE64(data, static_cast<uint64_t>(mn));
    }
    for (int64_t mx : footer.group_max_keys) {
      AppendBE64(data, static_cast<uint64_t>(mx));
    }
  }
  for (uint8_t enc : footer.encodings) {
    data.push_back(static_cast<char>(enc));
  }
  for (uint32_t prm : footer.params) {
    AppendBE32(data, prm);
  }
  data.push_back(static_cast<char>(footer.col_stats_present ? 1 : 0));
  if (footer.col_stats_present) {
    for (slot_t c = 0; c < footer.column_count; ++c) {
      data.push_back(static_cast<char>(footer.col_sum_valid[c]));
      AppendBE64(data, footer.col_counts[c]);
      AppendBE64(data, static_cast<uint64_t>(footer.col_sums[c]));
    }
  }
  // All-column zone maps (v6): [present u8][rg*col*(min 8B + max 8B)] BE.
  data.push_back(static_cast<char>(footer.all_zm_present ? 1 : 0));
  if (footer.all_zm_present) {
    for (size_t i = 0; i < footer.all_zone_mins.size(); ++i) {
      AppendBE64(data, static_cast<uint64_t>(footer.all_zone_mins[i]));
      AppendBE64(data, static_cast<uint64_t>(footer.all_zone_maxs[i]));
    }
  }
  // Bloom filters (v7): [present u8][rg*col × u64 BE].
  data.push_back(static_cast<char>(footer.bloom_present ? 1 : 0));
  if (footer.bloom_present) {
    for (uint64_t bloom_bits : footer.rg_blooms) {
      AppendBE64(data, bloom_bits);
    }
  }
  // Per-RG partial sums: [present u8][rg*col × i64 BE].
  data.push_back(static_cast<char>(footer.rg_sums_present ? 1 : 0));
  if (footer.rg_sums_present) {
    for (int64_t sv : footer.rg_sums) {
      AppendBE64(data, static_cast<uint64_t>(sv));
    }
  }
  // HLL sketches (v8): [present u8][rg*col*64B].
  data.push_back(static_cast<char>(footer.hll_present ? 1 : 0));
  if (footer.hll_present) {
    data.insert(data.end(), footer.hll_sketches.begin(),
                footer.hll_sketches.end());
  }
  // Hash index (v9): [present u8][log2 u8][keys i64 LE × size][rows u32 LE ×
  // size].
  data.push_back(static_cast<char>(footer.hash_idx_present ? 1 : 0));
  if (footer.hash_idx_present) {
    data.push_back(static_cast<char>(footer.hash_idx_log2));
    const size_t sz = 1ULL << footer.hash_idx_log2;
    // Explicit LE: the declared format is little-endian (the WASM reader
    // consumes it), but the raw arrays hold NATIVE bytes, so a big-endian
    // host must persist byte-swapped entries. On the LE hosts that run the
    // encoders the whole array is already in wire order and goes out as one
    // contiguous insert (values are byte-swapped identically per element).
    if constexpr (std::endian::native == std::endian::little) {
      const auto* k8 = reinterpret_cast<const char*>(footer.hash_keys.data());
      data.insert(data.end(), k8, k8 + (sz * sizeof(int64_t)));
      const auto* r4 = reinterpret_cast<const char*>(footer.hash_rows.data());
      data.insert(data.end(), r4, r4 + (sz * sizeof(uint32_t)));
    } else {
      for (size_t i = 0; i < sz; ++i) {
        const uint64_t kle =
            htole64(static_cast<uint64_t>(footer.hash_keys[i]));
        const auto* k8 = reinterpret_cast<const char*>(&kle);
        data.insert(data.end(), k8, k8 + 8);
      }
      for (size_t i = 0; i < sz; ++i) {
        const uint32_t rle = htole32(footer.hash_rows[i]);
        const auto* r4 = reinterpret_cast<const char*>(&rle);
        data.insert(data.end(), r4, r4 + 4);
      }
    }
  }
  // Null bitmaps (v11): [present u8][concatenated (RG,col) bitmaps, each
  // ceil(rc/8) bytes in RG-major, col-minor order].
  data.push_back(static_cast<char>(footer.null_present ? 1 : 0));
  if (footer.null_present) {
    data.insert(data.end(), footer.null_bitmaps.begin(),
                footer.null_bitmaps.end());
  }
  // Extended column stats (v12): [flag u8][per col: min i64 + max i64 +
  // null_count u64 + mode i64 + mode_count u64 + HIST_BINS u64] BE. Added
  // after all earlier sections so v11-era readers skip it as an unknown
  // trailing section.
  data.push_back(static_cast<char>(footer.ext_stats_present ? 1 : 0));
  if (footer.ext_stats_present) {
    constexpr int B = Footer::ColumnStats::HIST_BINS;
    for (slot_t c = 0; c < footer.column_count; ++c) {
      AppendBE64(data, static_cast<uint64_t>(footer.col_mins[c]));
      AppendBE64(data, static_cast<uint64_t>(footer.col_maxs[c]));
      AppendBE64(data, footer.col_nulls[c]);
      AppendBE64(data, static_cast<uint64_t>(footer.col_modes[c]));
      AppendBE64(data, footer.col_mode_counts[c]);
      for (int b = 0; b < B; ++b) {
        AppendBE64(
            data,
            footer.col_hists[(static_cast<size_t>(c) * static_cast<size_t>(B)) +
                             static_cast<size_t>(b)]);
      }
    }
  }
  // Inverted secondary index (v13): [present u8][ncell u32 BE] then per cell
  // [g u32 BE][c u32 BE][nval u32 BE] and per posting [value i64 BE][ctype u8]
  // with ctype 0 = array [k u16 BE][rowid u16 BE x k], 1 = bitmap
  // [16 x u64 BE], 2 = run [nr u16 BE][start u16 BE][len u16 BE x nr].
  // Appended after every earlier section so older readers ignore it as
  // trailing bytes. Container choice minimizes serialized bytes per posting.
  data.push_back(static_cast<char>(footer.inv_idx_present ? 1 : 0));
  if (footer.inv_idx_present) {
    AppendBE32(data, static_cast<uint32_t>(footer.inv_index.size()));
    for (const InvertedCell& cell : footer.inv_index) {
      AppendBE32(data, cell.group);
      AppendBE32(data, static_cast<uint32_t>(cell.column));
      AppendBE32(data, static_cast<uint32_t>(cell.postings.size()));
      for (const InvertedPosting& p : cell.postings) {
        AppendBE64(data, static_cast<uint64_t>(p.value));
        // Runs over the sorted row ids.
        uint16_t nruns = 0;
        for (size_t k = 0; k < p.rows.size();) {
          size_t e = k + 1;
          while (e < p.rows.size() && p.rows[e] == p.rows[e - 1] + 1) {
            ++e;
          }
          ++nruns;
          k = e;
        }
        const size_t run_bytes = 2 + (static_cast<size_t>(nruns) * 4);
        const size_t arr_bytes = 2 + (p.rows.size() * 2);
        uint8_t ctype = 1;  // bitmap default
        if (run_bytes <= arr_bytes && run_bytes <= 128) {
          ctype = 2;
        } else if (arr_bytes <= 128) {
          ctype = 0;
        }
        data.push_back(static_cast<char>(ctype));
        if (ctype == 0) {
          AppendBE16(data, static_cast<uint16_t>(p.rows.size()));
          for (uint16_t r : p.rows) {
            AppendBE16(data, r);
          }
        } else if (ctype == 2) {
          AppendBE16(data, nruns);
          for (size_t k = 0; k < p.rows.size();) {
            size_t e = k + 1;
            while (e < p.rows.size() && p.rows[e] == p.rows[e - 1] + 1) {
              ++e;
            }
            AppendBE16(data, p.rows[k]);
            AppendBE16(data, static_cast<uint16_t>(e - k));
            k = e;
          }
        } else {
          std::array<uint64_t, 16> words{};
          for (uint16_t r : p.rows) {
            words[r >> 6] |= (1ULL << (r & 63));
          }
          for (uint64_t w : words) {
            AppendBE64(data, w);
          }
        }
      }
    }
  }
  // Xor8 pre-filter (v14): [present u8][ncell u32 BE] then per cell
  // [g u32 BE][c u32 BE][m u32 BE][seed u32 BE][table m bytes]. Appended
  // after every earlier section so older readers ignore it as trailing bytes.
  data.push_back(static_cast<char>(footer.xor_present ? 1 : 0));
  if (footer.xor_present) {
    AppendBE32(data, static_cast<uint32_t>(footer.xor_cells.size()));
    for (const XorCell& cell : footer.xor_cells) {
      AppendBE32(data, cell.group);
      AppendBE32(data, static_cast<uint32_t>(cell.column));
      AppendBE32(data, static_cast<uint32_t>(cell.table.size()));
      AppendBE32(data, cell.seed);
      data.insert(data.end(), cell.table.begin(), cell.table.end());
    }
  }
  // PGM learned index (v15): [present u8][eps u32 BE][nseg u32 BE] then per
  // segment [key i64 BE][pos u64 BE][slope f64 bits BE]. Trailing section.
  data.push_back(static_cast<char>(footer.pgm_present ? 1 : 0));
  if (footer.pgm_present) {
    AppendBE32(data, footer.pgm_eps);
    AppendBE32(data, static_cast<uint32_t>(footer.pgm_segments.size()));
    for (const PgmSegment& s : footer.pgm_segments) {
      AppendBE64(data, static_cast<uint64_t>(s.key));
      AppendBE64(data, s.pos);
      uint64_t bits = 0;
      static_assert(sizeof(bits) == sizeof(s.slope));
      memcpy(&bits, &s.slope, sizeof(bits));
      AppendBE64(data, bits);
    }
  }
  if (footer.format_version >= 2) {
    footer.crc_data = Crc32cOf(data.data(), footer_start);
    AppendBE32(data, footer.crc_data);
    const size_t footer_len_no_crc = data.size() - footer_start;
    footer.crc_footer = Crc32cOf(data.data() + footer_start, footer_len_no_crc);
    AppendBE32(data, footer.crc_footer);
  }
  // Footer offsets are uint32: the per-row hash-index section alone can add
  // ~48 B/row, so a huge run can push the FOOTER (not the data region, which
  // the per-row-group guard above rejects) past 4 GiB. A wrapped footer_size
  // then misparses every reader's data_end. Return an error, exactly like
  // the data-region cap.
  if (data.size() - footer_start >
      static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return StatusError(StatusCode::kCorrupt,
                       "PAX chunk exceeds uint32 footer size limit");
  }
  footer.footer_size = static_cast<uint32_t>(data.size() - footer_start);
  AppendBE32(data, footer.footer_size);
  footer.schema_version = sc.SchemaVersion();
  std::vector<char> compressed;
  if (compress) {
    const size_t region = data.size() - footer.footer_size - 4;
    compressed = CompressRegion(std::string_view(data.data(), region));
  }
  PaxChunk chunk = compress ? PaxChunk{std::move(data), std::move(footer),
                                       std::move(compressed), true}
                            : PaxChunk{std::move(data), std::move(footer)};
  chunk.bp128_metadata_trusted_ = true;
  chunk.chimp128_metadata_trusted_ = true;
  chunk.alp_rd_tans_metadata_trusted_ = true;
  chunk.patas_metadata_trusted_ = true;
  chunk.fastpfor_metadata_trusted_ = true;
  chunk.deltafastpfor_metadata_trusted_ = true;
  return chunk;
} catch (const std::exception& error) {
  return StatusError(StatusCode::kCorrupt,
                     std::string("PAX construction failed: ") + error.what());
}

void PaxChunk::BuildRowCountPrefix() {
  rg_prefix_.clear();
  if (footer_.row_counts.size() < footer_.row_group_count) {
    return;
  }
  rg_prefix_.reserve(static_cast<size_t>(footer_.row_group_count) + 1);
  rg_prefix_.push_back(0);
  uint64_t total = 0;
  for (uint32_t i = 0; i < footer_.row_group_count; ++i) {
    total += footer_.row_counts[i];
    rg_prefix_.push_back(total);
  }
}

void PaxChunk::BuildNullBitmapSummary() {
  footer_.null_summary_valid = false;
  footer_.null_any = false;
  footer_.null_uniform_groups = 0;
  footer_.null_uniform_stride = 0;
  if (!footer_.null_present) {
    footer_.null_summary_valid = true;
    return;
  }
  if (footer_.row_counts.size() < footer_.row_group_count) {
    return;
  }
  uint64_t total = 0;
  uint64_t first_stride = 0;
  uint32_t uniform_groups = 0;
  for (uint32_t g = 0; g < footer_.row_group_count; ++g) {
    const uint64_t stride =
        static_cast<uint64_t>(footer_.column_count) *
        ((static_cast<uint64_t>(footer_.row_counts[g]) + 7U) / 8U);
    total += stride;
    if (g == 0) {
      first_stride = stride;
      uniform_groups = 1;
    } else if (uniform_groups != 0 && stride == first_stride) {
      ++uniform_groups;
    } else {
      uniform_groups = 0;
    }
  }
  if (total != footer_.null_bitmaps.size()) {
    return;
  }
  footer_.null_uniform_groups = uniform_groups;
  footer_.null_uniform_stride = first_stride;
  for (uint8_t byte : footer_.null_bitmaps) {
    if (byte != 0) {
      footer_.null_any = true;
      break;
    }
  }
  footer_.null_summary_valid = true;
}

// Build the Elias-Fano select directories (see pax_chunk.hpp). Everything is
// bounds-checked against the same region clamps the decoders use: a cell
// whose header/bitmap extent cannot be established simply gets no directory
// and keeps the stateless SelectBit fallback.
void PaxChunk::BuildEfSelectDirs() {
  ef_sel_off_.clear();
  ef_sel_flat_.clear();
  const size_t cells =
      static_cast<size_t>(footer_.row_group_count) * footer_.column_count;
  if (cells == 0 || footer_.column_count == 0 ||
      footer_.row_counts.size() < footer_.row_group_count ||
      footer_.column_offsets.size() < cells ||
      footer_.encodings.size() < cells || footer_.params.size() < cells) {
    return;
  }
  if (data_.size() <= static_cast<size_t>(footer_.footer_size) + 4) {
    return;
  }
  const size_t data_end = data_.size() - footer_.footer_size - 4;
  ef_sel_off_.resize(cells + 1, 0);
  for (size_t i = 0; i < cells; ++i) {
    ef_sel_off_[i] = static_cast<uint32_t>(ef_sel_flat_.size());
    // u32 offsets: past ~4G samples (a multi-GB EF chunk), keep the entries
    // already built and shrink the offset table so later cells see no
    // directory (nullptr fallback) instead of a wrapping span.
    if (ef_sel_flat_.size() > 0xFFFFFF00ULL) {
      ef_sel_off_.resize(i + 1);
      return;
    }
    if (footer_.encodings[i] != static_cast<uint8_t>(Scheme::kEliasFano)) {
      continue;
    }
    const uint32_t rc = footer_.row_counts[i / footer_.column_count];
    if (rc == 0) {
      continue;
    }
    const size_t off = footer_.column_offsets[i];
    if (off > data_end) {
      continue;
    }
    const char* r = data_.data() + off;
    const char* region_end =
        (i + 1 < cells && footer_.column_offsets[i + 1] > off &&
         footer_.column_offsets[i + 1] <= data_end)
            ? data_.data() + footer_.column_offsets[i + 1]
            : data_.data() + data_end;
    if (r + 17 > region_end) {
      continue;
    }
    const int l = static_cast<int>(static_cast<uint8_t>(r[16]));
    const uint64_t maxd = ReadBE64(r + 8);
    const uint64_t high_max = (l >= 64) ? 0 : (maxd >> l);
    if (high_max > UINT64_MAX - static_cast<uint64_t>(rc) - 7) {
      continue;
    }
    const size_t hlen = rc + static_cast<size_t>(high_max);
    // Sample positions are stored as u32; skip absurd (hostile) extents.
    if (hlen > 0xFFFFFFFFULL) {
      continue;
    }
    const char* hbmp = r + 17;
    if (hbmp + ((hlen + 7) / 8) > region_end) {
      continue;
    }
    // Walk the set bits in order; sample j is the position of the (8j)-th.
    const size_t nsamp = ((static_cast<size_t>(rc) - 1) >> 3) + 1;
    const size_t start = ef_sel_flat_.size();
    const size_t full_words = hlen >> 6;
    size_t seen = 0;
    for (size_t w = 0; w < full_words; ++w) {
      uint64_t x = ReadBE64(hbmp + (w * 8));
      while (x != 0) {
        // MSB-first position q lives at word-bit (63 - q), so clzll(x) IS
        // the set bit's position inside the word. The former form pushed
        // `63 - clzll(x)` -- the word-bit, not the position -- so whole-word
        // samples pointed at the mirrored bit; SelectBitDir then resumed
        // from garbage and the per-cell EF decode returned wrong rows (and
        // walked into clzll(0) UB).
        const int q = __builtin_clzll(x);
        if ((seen & 7) == 0) {
          ef_sel_flat_.push_back(
              static_cast<uint32_t>((w * 64) + static_cast<uint64_t>(q)));
        }
        ++seen;
        x &= (static_cast<uint64_t>(1) << (63 - q)) - 1;
      }
    }
    for (uint64_t p = full_words << 6; p < hlen; ++p) {
      if (((static_cast<uint8_t>(hbmp[p >> 3]) >> (7 - (p & 7))) & 1) != 0) {
        if ((seen & 7) == 0) {
          ef_sel_flat_.push_back(static_cast<uint32_t>(p));
        }
        ++seen;
      }
    }
    // A corrupt bitmap (fewer set bits than rows) gets no directory: the
    // decode falls back to SelectBit, which already fail-closes there.
    if (ef_sel_flat_.size() - start != nsamp) {
      ef_sel_flat_.resize(start);
    }
  }
  ef_sel_off_[cells] = static_cast<uint32_t>(ef_sel_flat_.size());
}

void PaxChunk::BuildFsstDirs() {
  fsst_meta_index_.clear();
  fsst_meta_.clear();
  fsst_symbol_offsets_.clear();
  fsst_symbol_lengths_.clear();
  const size_t cells =
      static_cast<size_t>(footer_.row_group_count) * footer_.column_count;
  if (cells == 0 || footer_.column_count == 0 ||
      footer_.row_counts.size() < footer_.row_group_count ||
      footer_.column_offsets.size() < cells ||
      footer_.encodings.size() < cells || footer_.params.size() < cells ||
      cells >= std::numeric_limits<uint32_t>::max() ||
      cells > data_.size() + 1) {
    return;
  }
  size_t fsst_cells = 0;
  for (size_t i = 0; i < cells; ++i) {
    if (footer_.encodings[i] == static_cast<uint8_t>(Scheme::kFsst)) {
      ++fsst_cells;
    }
  }
  if (fsst_cells == 0) {
    return;
  }
  fsst_meta_index_.assign(cells + 1, 0);
  fsst_meta_.reserve(fsst_cells);
  const size_t data_end =
      data_.size() > static_cast<size_t>(footer_.footer_size) + 4
          ? data_.size() - footer_.footer_size - 4
          : 0;
  if (data_end == 0) {
    return;
  }
  for (size_t i = 0; i < cells; ++i) {
    fsst_meta_index_[i] = static_cast<uint32_t>(fsst_meta_.size());
    if (footer_.encodings[i] != static_cast<uint8_t>(Scheme::kFsst)) {
      continue;
    }
    const auto g = static_cast<uint32_t>(i / footer_.column_count);
    const auto c = static_cast<uint32_t>(i % footer_.column_count);
    const uint16_t n = footer_.row_counts[g];
    if (n == 0) {
      continue;
    }
    const size_t off = footer_.column_offsets[i];
    if (off > data_end) {
      continue;
    }
    const char* region = data_.data() + off;
    const char* region_end = RegionEndAt(footer_, g, c, data_);
    if (region_end == nullptr || region_end < region) {
      continue;
    }
    const auto region_len = static_cast<size_t>(region_end - region);
    if (region_len < 6) {
      continue;
    }
    const uint16_t nsym = ReadBE16(region);
    if (nsym > 256) {
      continue;
    }
    const uint32_t tsl = ReadBE32(region + 2);
    const size_t symbol_start = fsst_symbol_offsets_.size();
    size_t p = 6;
    bool valid = true;
    for (uint16_t s = 0; s < nsym; ++s) {
      if (p >= region_len) {
        valid = false;
        break;
      }
      const auto slen = static_cast<uint8_t>(region[p++]);
      if (slen > region_len - p) {
        valid = false;
        break;
      }
      fsst_symbol_offsets_.push_back(static_cast<uint32_t>(p));
      fsst_symbol_lengths_.push_back(slen);
      p += slen;
    }
    const size_t table_bytes = static_cast<size_t>(n) * 4;
    if (valid && table_bytes > region_len - p) {
      valid = false;
    }
    if (valid) {
      p += table_bytes;
    }
    if (valid && tsl > region_len - p) {
      valid = false;
    }
    if (valid) {
      uint32_t previous = 0;
      for (uint16_t row = 0; row < n; ++row) {
        const uint32_t current =
            ReadBE32(region + p - table_bytes + (static_cast<size_t>(row) * 4));
        if (current > tsl || (row != 0 && current < previous)) {
          valid = false;
          break;
        }
        previous = current;
      }
    }
    if (!valid || p > std::numeric_limits<uint32_t>::max() ||
        symbol_start > std::numeric_limits<uint32_t>::max()) {
      fsst_symbol_offsets_.resize(symbol_start);
      fsst_symbol_lengths_.resize(symbol_start);
      continue;
    }
    FsstDecodeMeta meta;
    meta.symbol_index = static_cast<uint32_t>(symbol_start);
    meta.offset_table = static_cast<uint32_t>(p - table_bytes);
    meta.stream = static_cast<uint32_t>(p);
    meta.stream_len = tsl;
    meta.symbol_count = nsym;
    meta.row_count = n;
    meta.valid = true;
    fsst_meta_.push_back(meta);
  }
  fsst_meta_index_[cells] = static_cast<uint32_t>(fsst_meta_.size());
}

// Is this scheme one whose values are worth PRE-DECODING for the row-at-a-time
// scan path?
//
// The cache exists so a sequential scan reads an array instead of re-entering
// Int64AtFast once per VALUE (a ~50-way switch dispatch per value, which
// profiled at ~35% of a full scan). Which schemes QUALIFY is a separate
// question from whether they decode, and the two exclusions are both about
// waste rather than correctness:
//
//   * kPlain and kConst are already a single load (or a single constant) in
//     Int64AtFast, so caching them would spend 8 B/value of RAM to replace a
//     load that is already free -- and these are the schemes a chunk falls
//     back to, so it is exactly the common case that would bloat.
//   * kFsst and kFrontCode are the VARCHAR codecs. Their regions have no
//     int64 interpretation, and asking DecodeInt64ColumnRg to read one is at
//     best wasted work and at worst a throw -- and a throw here clears the
//     WHOLE cache, so it would silently disable the fast path for every other
//     column in the chunk.
//
// Everything else is admitted, including the double codecs (kGorilla, kAlp,
// kChimpC, ...): ReadRowIntoImpl consumes a cached cell by reinterpreting the
// int64 bits as a double, which is byte for byte what the uncached path does
// with Int64AtFast's result, so caching them is consistent, not a new
// semantic. The consumer additionally gates on the declared column type, so a
// cell cached for a column the caller never reads as int64 is simply unused.
namespace {

bool DecodableIntCacheScheme(Scheme scheme) {
  switch (scheme) {
    case Scheme::kPlain:
    case Scheme::kConst:
    case Scheme::kFsst:
    case Scheme::kFrontCode:
      return false;
    default:
      return true;
  }
}

}  // namespace

void PaxChunk::BuildDecodedIntCache() {
  decoded_int_cache_.clear();
  decoded_int_offsets_.clear();
  decoded_int_cache_ready_ = false;
  const size_t cells =
      static_cast<size_t>(footer_.row_group_count) * footer_.column_count;
  if (cells == 0 || footer_.row_counts.size() < footer_.row_group_count ||
      footer_.column_offsets.size() < cells ||
      footer_.encodings.size() < cells || footer_.params.size() < cells) {
    return;
  }
  constexpr size_t kMaxDecodedIntCells = size_t{1} << 20;
  if (cells > kMaxDecodedIntCells) {
    return;
  }
  constexpr size_t kMaxDecodedIntCacheValues = size_t{1} << 20;
  // The budget is spent CELL BY CELL rather than all-or-nothing. Widening the
  // eligible set made the old "one cell over the cap drops the whole cache"
  // behaviour a real regression: a chunk holding, say, a kDeltaFor column
  // exactly at the cap PLUS several kBitWeave columns used to keep the cache
  // and would now have lost it entirely. Leaving the over-budget cells with an
  // empty offset span is safe and needs NO consumer change --
  // ReadRowIntoImpl already requires a NON-EMPTY span before it reads the
  // cache, and falls back to Int64AtFast otherwise.
  std::vector<uint8_t> cacheable(cells, 0);
  size_t cached_values = 0;
  for (size_t i = 0; i < cells; ++i) {
    const auto scheme = static_cast<Scheme>(footer_.encodings[i]);
    if (!DecodableIntCacheScheme(scheme)) {
      continue;
    }
    const size_t rows = footer_.row_counts[i / footer_.column_count];
    if (rows == 0 || rows > kMaxDecodedIntCacheValues - cached_values) {
      continue;  // over budget: leave this cell uncached, keep the others
    }
    cacheable[i] = 1;
    cached_values += rows;
  }
  decoded_int_offsets_.assign(cells + 1, 0);
  decoded_int_cache_.reserve(cached_values);
  std::vector<int64_t> decoded;
  for (size_t i = 0; i < cells; ++i) {
    decoded_int_offsets_[i] = decoded_int_cache_.size();
    const auto g = static_cast<uint32_t>(i / footer_.column_count);
    const auto c = static_cast<slot_t>(i % footer_.column_count);
    const uint16_t rows = footer_.row_counts[g];
    if (rows != 0 && cacheable[i] != 0) {
      decoded.resize(rows);
      try {
        DecodeInt64ColumnRg(c, g, decoded.data());
      } catch (...) {
        decoded_int_cache_.clear();
        decoded_int_offsets_.clear();
        return;
      }
      decoded_int_cache_.insert(decoded_int_cache_.end(), decoded.begin(),
                                decoded.end());
    }
  }
  decoded_int_offsets_[cells] = decoded_int_cache_.size();
  decoded_int_cache_ready_ = true;
}

int PaxChunk::ResolveRowGroup(size_t global_row_index, uint32_t* g_out,
                              uint32_t* local_out) const {
  if (rg_prefix_.empty()) {
    return 1;  // inconsistent footer: caller fails closed
  }
  if (global_row_index >= rg_prefix_.back()) {
    return 2;
  }
  // The row lives in the group whose [prefix[g], prefix[g+1]) contains it.
  // Small chunk counts walk linearly: this sits on the per-row read path,
  // where the linear scan beats binary search up to a few dozen groups.
  // Both variants skip zero-row groups (equal consecutive prefix entries).
  if (rg_prefix_.size() <= 32) {
    for (size_t i = 0; i + 1 < rg_prefix_.size(); ++i) {
      if (global_row_index < rg_prefix_[i + 1]) {
        *g_out = static_cast<uint32_t>(i);
        *local_out = static_cast<uint32_t>(global_row_index - rg_prefix_[i]);
        return 0;
      }
    }
    return 2;
  }
  const auto it =
      std::upper_bound(rg_prefix_.begin(), rg_prefix_.end(), global_row_index);
  const uint32_t g = static_cast<uint32_t>(it - rg_prefix_.begin()) - 1;
  *g_out = g;
  *local_out = static_cast<uint32_t>(global_row_index - rg_prefix_[g]);
  return 0;
}

namespace {

// Bulk-decode one (row group, varchar column): fills `out` with rc strings in
// group-local row order (null cells stay empty strings to keep alignment).
// Shared by FillVarcharGroupCache; mirrors the per-row decoder's extent
// checks and fails closed on any anomaly.
bool DecodeVarcharColumnRgInto(const PaxChunk& chunk, uint32_t g, slot_t c,
                               std::vector<std::string>* out) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (g >= f.row_group_count || g >= f.row_counts.size()) {
    return false;
  }
  const size_t off_idx =
      (static_cast<size_t>(g) * static_cast<size_t>(f.column_count)) + c;
  if (off_idx >= f.column_offsets.size() || off_idx >= f.encodings.size() ||
      off_idx >= f.params.size()) {
    return false;
  }
  const char* region = RegionStartAt(f, g, c, chunk.Data(), 0);
  if (region == nullptr) {
    return false;
  }
  const char* region_end = nullptr;
  if (off_idx + 1 < f.column_offsets.size()) {
    region_end = RegionEndAt(f, g, c, chunk.Data());
    if (region_end == nullptr || region_end <= region) {
      region_end = region;
    }
  } else if (chunk.Data().size() > f.footer_size + 4) {
    region_end = chunk.Data().data() + chunk.Data().size() - f.footer_size - 4;
  } else {
    region_end = region;
  }
  const auto s = static_cast<Scheme>(f.encodings[off_idx]);
  const uint32_t prm = f.params[off_idx];
  const uint16_t rc = f.row_counts[g];
  out->resize(rc);
  const FsstDecodeView fsst = chunk.FsstViewAt(off_idx);
  if (s == Scheme::kFrontCode) {
    // One sequential pass: the restart chain decodes every entry exactly
    // once (the per-row arm replays the block for each row instead).
    const uint32_t re = prm & 0x7FFFU;
    if (re == 0 || region + 6 > region_end) {
      return false;
    }
    const uint32_t nrestart = ReadBE16(region);
    const uint32_t stream_len = ReadBE32(region + 2);
    const char* rtable = region + 6;
    if (rtable + (static_cast<size_t>(nrestart) * 4) > region_end) {
      return false;
    }
    const char* stream = rtable + (static_cast<size_t>(nrestart) * 4);
    if (stream + stream_len > region_end) {
      return false;
    }
    const char* send = stream + stream_len;
    std::string cur;
    auto cur_block = static_cast<uint32_t>(-1);
    const char* q = nullptr;
    for (uint32_t i = 0; i < rc; ++i) {
      const uint32_t b = i / re;
      if (b != cur_block) {
        if (b >= nrestart) {
          return false;
        }
        const uint32_t ro = ReadBE32(rtable + (static_cast<size_t>(b) * 4));
        if (ro > stream_len) {
          return false;
        }
        q = stream + ro;
        cur.clear();
        cur_block = b;
      }
      if (q + 4 > send) {
        return false;
      }
      const uint32_t hdr = ReadBE32(q);
      const uint32_t plen = hdr >> 16;
      const uint32_t slen = hdr & 0xFFFFU;
      if (plen > cur.size() || slen > static_cast<uint64_t>(send - q) - 4U) {
        return false;
      }
      cur.resize(plen);
      cur.append(q + 4, slen);
      q += 4 + slen;
      (*out)[i] = cur;
    }
    return true;
  }
  for (uint32_t i = 0; i < rc; ++i) {
    if (CellIsNull(f, g, c, i)) {
      continue;  // placeholder ""
    }
    (*out)[i] =
        VarcharRegionAt(region, s, prm, rc, i, region_end, &fsst).MoveString();
  }
  return true;
}

// Refresh a sequential-scan varchar cache for row group `g`. Only full
// success marks the cache valid; partial state is never observable.
// Serve per-row decodes until the reader has walked this many rows of one
// group; only then pay the whole-group bulk fill. Short range seeks (a few
// rows per seek) must not trade a handful of restart replays for a full
// 1024-row column decode.
constexpr uint32_t kVarcharCacheMinRows = 256;

void FillVarcharGroupCache(uint32_t g, const Schema& sc,
                           PaxChunk::VarcharGroupCache* cache,
                           const PaxChunk& chunk) {
  cache->valid = false;
  cache->group = g;
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (f.row_counts.size() < static_cast<size_t>(f.row_group_count)) {
    return;
  }
  if (sc.ColumnCount() < f.column_count) {
    return;
  }
  cache->by_col.assign(f.column_count, {});
  for (slot_t c = 0; c < f.column_count; ++c) {
    if (!cache->IsColumnSelected(c)) {
      continue;
    }
    const ValueType vt = sc.GetColumn(c).Type();
    if (vt == ValueType::kInt64 || vt == ValueType::kDouble) {
      continue;
    }
    if (!DecodeVarcharColumnRgInto(chunk, g, c, &cache->by_col[c])) {
      return;
    }
  }
  cache->valid = true;
}

// Flip resolved kind-2 int/double cursors to kind 1 by bulk-decoding each
// whole group column once. Runs under the same rows_seen amortization gate
// as the varchar fill: short range seeks must not pay a full-column decode
// for a handful of rows. The decode goes through the validated public
// DecodeInt64ColumnRg (the one-bulk-decoder contract the random-access
// Int64RegionAt is tested to match cell for cell), so a flipped column
// serves values identical to the kind-2 branch it replaces; any anomaly
// leaves that column on kind 2 and it keeps decoding per row.
void FillIntSeqCursors(const PaxChunk& chunk,
                       PaxChunk::VarcharGroupCache* cache) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  cache->int_by_col.resize(f.column_count);
  const uint16_t rc = cache->seq_rc;
  if (rc == 0) {
    return;
  }
  const size_t cols = std::min<size_t>(cache->seq_cols.size(), f.column_count);
  for (size_t c = 0; c < cols; ++c) {
    PaxChunk::VarcharGroupCache::SeqCol& cur = cache->seq_cols[c];
    if (cur.kind != 2) {
      continue;
    }
    std::vector<int64_t>& vals = cache->int_by_col[c];
    vals.resize(rc);
    bool decoded = true;
    try {
      chunk.DecodeInt64ColumnRg(static_cast<slot_t>(c), cache->seq_group,
                                vals.data());
    } catch (...) {
      decoded = false;  // hostile footer: keep the per-row decoder
    }
    if (!decoded) {
      continue;
    }
    cur.vals = vals.data();
    cur.span = rc;
    cur.kind = 1;
  }
}

// Serve one kD2Pfor cell through a sequential-cursor state (armed/loc/val/sd
// owned by the caller: the iterator's SeqCol or a thread-local column-read
// cursor). The checkpoint replay mirrors D2PforValueAt exactly (same
// unsigned-wrapping accumulators and guard set -- the scheme's contract),
// then arms the running (v, sd) pair so the next loc == prev + 1 advances
// by two adds instead of replaying from the checkpoint again. Rows that hit
// the checkpoint shortcut (loc == block start) cannot arm: the stored pair
// would not yet satisfy the invariant.
uint64_t D2PforValueAtArmed(const D2PforRegion& p, uint32_t loc, bool* armed,
                            uint32_t* run_loc, uint64_t* run_val,
                            uint64_t* run_sd) {
  const uint32_t b = loc / kD2PforCheckpointEvery;
  if (b >= p.ncp) {
    *armed = false;
    return 0;
  }
  uint64_t val = ReadBE64(p.ckpt + (static_cast<size_t>(b) * 16));
  uint64_t sd = ReadBE64(p.ckpt + (static_cast<size_t>(b) * 16) + 8);
  const uint32_t r = std::min<uint32_t>(b * kD2PforCheckpointEvery, p.n - 1);
  if (loc <= r) {
    *armed = false;
    return val;
  }
  if (r != 0) {
    sd += static_cast<uint64_t>(D2PforSecondDiff(p, r - 1));
  }
  for (uint32_t m = r; m < loc; ++m) {
    val += sd;
    if (m + 2 < p.n) {
      sd += static_cast<uint64_t>(D2PforSecondDiff(p, m));
    }
  }
  // After the loop: (val, sd) == (v[loc], sd[loc]) whenever loc+1 < n (the
  // final iteration's guarded dd update produced sd[loc]); only that state
  // may arm the cursor.
  *armed = (static_cast<size_t>(loc) + 1 < p.n);
  *run_val = val;
  *run_sd = sd;
  *run_loc = loc;
  return val;
}

}  // namespace

Status PaxChunk::ReadRowInto(size_t global_row_index, const Schema& sc,
                             Row* row_out) const {
  return ReadRowIntoImpl(global_row_index, sc, nullptr, row_out);
}

Status PaxChunk::ReadRowIntoCached(size_t global_row_index, const Schema& sc,
                                   VarcharGroupCache* cache,
                                   Row* row_out) const {
  return ReadRowIntoImpl(global_row_index, sc, cache, row_out);
}

Status PaxChunk::ReadRowIntoImpl(size_t global_row_index, const Schema& sc,
                                 VarcharGroupCache* cache, Row* row_out) const {
  if (row_out == nullptr) {
    return Status::kCorruption;
  }
  // The decode loop indexes sc.GetColumn(c) for every footer column; a
  // schema shorter than the chunk would read out of bounds (version-skew
  // between chunk and schema). ReadColumnValue carries the same guard.
  if (sc.ColumnCount() < footer_.column_count) {
    return Status::kCorruption;
  }
  // Sequential fast path: the group's per-column cursors are already
  // resolved, so the per-row work reduces to the null-bitmap test plus the
  // value store -- no per-row group resolution, data-area clamp, footer
  // bounds or offset arithmetic (all row-group-invariant). Semantics mirror
  // the legacy loop below branch for branch; the two paths must be kept in
  // lockstep. ResolveSeqCursors is fail-closed: on any anomaly the cursors
  // stay unresolved and every row keeps taking the legacy path.
  if (cache != nullptr && cache->seq_ready &&
      global_row_index >= cache->seq_lo && global_row_index < cache->seq_hi) {
    // Varchar fill timing must match the legacy path exactly, or short
    // sweeps that never reach kVarcharCacheMinRows would regress to per-row
    // restart-chain replays.
    ++cache->rows_seen;
    if (cache->seq_fill_pending && cache->rows_seen >= kVarcharCacheMinRows) {
      if (!cache->valid) {
        FillVarcharGroupCache(cache->seq_group, sc, cache, *this);
        if (!cache->valid) {
          cache->rows_seen = 0;
        }
      }
      if (!cache->seq_int_filled) {
        FillIntSeqCursors(*this, cache);
        cache->seq_int_filled = true;
      }
      if (cache->valid && cache->seq_int_filled) {
        cache->seq_fill_pending = false;
      }
    }
    const auto loc = static_cast<uint32_t>(global_row_index - cache->seq_lo);
    row_out->values_.resize(footer_.column_count);
    for (slot_t c = 0; c < footer_.column_count; ++c) {
      // Non-const: the D2Pfor sequential cursor carries its (v, sd) state
      // through this loop (the cache is iterator-owned).
      VarcharGroupCache::SeqCol& cur = cache->seq_cols[c];
      Value& slot = row_out->values_[c];
      if (cur.kind == 0) {
        if (!slot.IsNull()) {
          slot = Value();
        }
        continue;
      }
      if (cur.has_null_bits &&
          ((cur.null_bits[loc >> 3] >> (7U - (loc & 7U))) & 1U) != 0) {
        if (!slot.IsNull()) {
          slot = Value();
        }
        continue;
      }
      if (cur.kind == 1) {
        if (cur.is_double) {
          double d = NAN;
          const auto bits = static_cast<uint64_t>(cur.vals[loc]);
          memcpy(&d, &bits, sizeof(d));
          slot.SetDouble(d);
        } else {
          slot.SetInt64(cur.vals[loc]);
        }
        continue;
      }
      if (cur.kind == 2) {
        // D2Pfor sequential cursor: loc == prev+1 serves off the carried
        // (v, sd) prefix-sum state; everything else replays the checkpoint
        // walk from the resolve-time parse (no per-row re-parse). Bits
        // match the generic Int64AtFast arm exactly (same replay contract).
        if (cur.d2p_ok) {
          const D2PforRegion p{.ckpt = cur.d2p_ckpt,
                               .inner = cur.d2p_inner,
                               .n = cur.d2p_n,
                               .ncp = cur.d2p_ncp};
          uint64_t bits = 0;
          if (cur.d2p_armed && loc == cur.d2p_loc + 1) {
            const uint64_t v = cur.d2p_val + cur.d2p_sd;
            if (static_cast<size_t>(cur.d2p_loc) + 2 < cur.d2p_n) {
              cur.d2p_sd +=
                  static_cast<uint64_t>(D2PforSecondDiff(p, cur.d2p_loc));
            }
            cur.d2p_val = v;
            cur.d2p_loc = loc;
            bits = v;
          } else {
            bits = D2PforValueAtArmed(p, loc, &cur.d2p_armed, &cur.d2p_loc,
                                      &cur.d2p_val, &cur.d2p_sd);
          }
          if (cur.is_double) {
            double d = NAN;
            memcpy(&d, &bits, sizeof(d));
            slot.SetDouble(d);
          } else {
            slot.SetInt64(static_cast<int64_t>(bits));
          }
          continue;
        }
        const auto bits = static_cast<uint64_t>(
            Int64AtFast(cur.region, cur.scheme, cur.prm, cache->seq_rc, loc,
                        cur.region_end, cur.ef_dir));
        if (cur.is_double) {
          double d = NAN;
          memcpy(&d, &bits, sizeof(d));
          slot.SetDouble(d);
        } else {
          slot.SetInt64(static_cast<int64_t>(bits));
        }
        continue;
      }
      // kind == 3 (varchar): serve from the whole-group cache once the bulk
      // fill has landed, replay the per-row decoder otherwise.
      if (cache->valid && loc < cache->by_col[c].size()) {
        slot.SetStringView(cache->by_col[c][loc]);
        continue;
      }
      if (cur.scheme == Scheme::kFrontCode) {
        if (DecodeFrontCodeAtInto(cur.region, cur.prm, cache->seq_rc, loc,
                                  cur.region_end, &slot.owned_data.Mutable())) {
          slot.type = ValueType::kVarChar;
          slot.value.varchar_value = slot.owned_data;
        } else {
          slot = Value();
        }
        continue;
      }
      Value tv = VarcharRegionAt(cur.region, cur.scheme, cur.prm, cache->seq_rc,
                                 loc, cur.region_end, &cur.fsst);
      if (!tv.IsNull()) {
        slot.SetString(tv.MoveString());
      }
    }
    return Status::kSuccess;
  }
  // Find the row group and local row index. The prefix table turns the old
  // O(row_group_count) walk into an O(log n) search; it is unavailable only
  // for the hand-built short-row_counts footer, which keeps the old
  // kCorruption verdict.
  uint32_t g = 0;
  uint32_t local = 0;
  if (footer_.row_group_count == 1) {
    if (rg_prefix_.empty()) {
      return Status::kCorruption;
    }
    if (global_row_index >= rg_prefix_.back()) {
      return Status::kNotExists;
    }
    local = static_cast<uint32_t>(global_row_index);
  } else {
    const int res = ResolveRowGroup(global_row_index, &g, &local);
    if (res == 1) {
      return Status::kCorruption;
    }
    if (res == 2) {
      return Status::kNotExists;
    }
  }
  const uint16_t rc = footer_.row_counts[g];
  // Hoisted per-call state: the data-area end (the span clamp needs it for
  // every cell) and this row group's null-bitmap base (a per-cell null test
  // would otherwise walk all preceding groups every cell; the base depends
  // only on g).
  const size_t data_end =
      (data_.size() > static_cast<size_t>(footer_.footer_size) + 4)
          ? data_.size() - footer_.footer_size - 4
          : 0;
  // Per-row null-bitmap state: the group's bitmap base is shared by every
  // column, so the O(row_groups) offset walk runs once per row instead of
  // once per column (CellIsNull in the loop made ReadRow quadratic in the
  // group count whenever null bitmaps were present). null_ok=false mirrors
  // CellIsNull's "not null" answer for feature-off / truncated footers.
  bool null_ok = false;
  size_t null_base = 0;
  size_t null_bytes = 0;
  if (footer_.null_present && g < footer_.row_counts.size() &&
      footer_.row_counts.size() >=
          static_cast<size_t>(footer_.row_group_count)) {
    const uint32_t cc = footer_.column_count;
    size_t off = 0;
    for (uint32_t gg = 0; gg < g; ++gg) {
      off += static_cast<size_t>(cc) *
             static_cast<size_t>((footer_.row_counts[gg] + 7U) / 8U);
    }
    null_base = off;
    null_bytes = static_cast<size_t>((footer_.row_counts[g] + 7U) / 8U);
    null_ok = true;
  }
  // Sequential-scan cache: decode this group's varchar columns once the
  // reader has walked enough rows for the bulk fill to amortize; on anomaly
  // the cache stays invalid and rows fall back to the per-row decoder below
  // (fail closed; the fill retries after another kVarcharCacheMinRows rows).
  if (cache != nullptr) {
    if (cache->group != g) {
      cache->group = g;
      cache->valid = false;
      cache->rows_seen = 0;
      cache->seq_ready = false;
    }
    ++cache->rows_seen;
    if (!cache->seq_ready) {
      const Status seq_status = ResolveSeqCursors(g, sc, cache);
      if (seq_status != Status::kSuccess) {
        return seq_status;
      }
    }
    if (cache->seq_fill_pending && cache->rows_seen >= kVarcharCacheMinRows) {
      if (!cache->valid) {
        FillVarcharGroupCache(g, sc, cache, *this);
        if (!cache->valid) {
          cache->rows_seen = 0;
        }
      }
      if (!cache->seq_int_filled) {
        FillIntSeqCursors(*this, cache);
        cache->seq_int_filled = true;
      }
      if (cache->valid && cache->seq_int_filled) {
        cache->seq_fill_pending = false;
      }
    }
  }
  // One exact-size allocation: the per-column decode writes slots in place
  // (Value::SetInt64/SetDouble/SetString) instead of growing the vector and
  // move-constructing every value past its final address.
  row_out->values_.resize(footer_.column_count);
  for (slot_t c = 0; c < footer_.column_count; ++c) {
    Value& slot = row_out->values_[c];
    if (cache != nullptr && !cache->IsColumnSelected(c)) {
      if (!slot.IsNull()) {
        slot = Value();
      }
      continue;
    }
    const size_t off_idx = (g * footer_.column_count) + c;
    const ValueType vt = sc.GetColumn(c).Type();
    if (decoded_int_cache_ready_ &&
        (vt == ValueType::kInt64 || vt == ValueType::kDouble) &&
        off_idx + 1 < decoded_int_offsets_.size() &&
        decoded_int_offsets_[off_idx + 1] > decoded_int_offsets_[off_idx] &&
        static_cast<size_t>(local) <
            decoded_int_offsets_[off_idx + 1] - decoded_int_offsets_[off_idx]) {
      const size_t index = decoded_int_offsets_[off_idx] + local;
      const size_t null_byte =
          null_base + (static_cast<size_t>(c) * null_bytes) + (local >> 3);
      const bool is_null =
          null_ok && null_byte < footer_.null_bitmaps.size() &&
          ((static_cast<unsigned>(footer_.null_bitmaps[null_byte]) >>
            (7U - (local & 7U))) &
           1U) != 0;
      Value& cached_slot = row_out->values_[c];
      if (is_null) {
        cached_slot = Value();
      } else if (vt == ValueType::kInt64) {
        cached_slot.SetInt64(decoded_int_cache_[index]);
      } else {
        double value = NAN;
        const auto bits = static_cast<uint64_t>(decoded_int_cache_[index]);
        memcpy(&value, &bits, sizeof(value));
        cached_slot.SetDouble(value);
      }
      continue;
    }
    if (off_idx >= footer_.column_offsets.size() ||
        off_idx >= footer_.encodings.size() ||
        off_idx >= footer_.params.size()) {
      return Status::kCorruption;
    }
    const size_t off = footer_.column_offsets[off_idx];
    if (off > data_end) {
      return Status::kCorruption;
    }
    const char* region = data_.data() + off;
    const char* region_end = nullptr;
    if (off_idx + 1 < footer_.column_offsets.size() &&
        footer_.column_offsets[off_idx + 1] <= data_end) {
      const size_t succ = footer_.column_offsets[off_idx + 1];
      region_end = (succ > off) ? data_.data() + succ : region;
    } else {
      region_end = (data_end > 0) ? data_.data() + data_end : region;
    }
    const auto s = static_cast<Scheme>(footer_.encodings[off_idx]);
    const uint32_t prm = footer_.params[off_idx];
    bool is_null = false;
    if (null_ok) {
      const size_t null_off = null_base + (static_cast<size_t>(c) * null_bytes);
      const size_t byte_idx = null_off + (local >> 3);
      if (byte_idx < footer_.null_bitmaps.size()) {
        is_null =
            (((footer_.null_bitmaps[byte_idx] >> (7 - (local & 7))) & 1) != 0);
      }
    }
    if (is_null) {
      slot = Value();
      continue;
    }
    if (vt == ValueType::kInt64) {
      slot.SetInt64(Int64AtFast(region, s, prm, rc, local, region_end,
                                EfSelectDirAt(off_idx)));
    } else if (vt == ValueType::kDouble) {
      auto bits = static_cast<uint64_t>(Int64AtFast(
          region, s, prm, rc, local, region_end, EfSelectDirAt(off_idx)));
      double d = NAN;
      memcpy(&d, &bits, sizeof(double));
      slot.SetDouble(d);
    } else {  // varchar / null
      if (cache != nullptr && cache->valid && local < cache->by_col[c].size()) {
        // Cache hit: the group was decoded once; copy the value instead of
        // replaying the restart chain for this row.
        slot.SetStringView(cache->by_col[c][local]);
        continue;
      }
      if (s == Scheme::kFrontCode) {
        if (DecodeFrontCodeAtInto(region, prm, rc, local, region_end,
                                  &slot.owned_data.Mutable())) {
          slot.type = ValueType::kVarChar;
          slot.value.varchar_value = slot.owned_data;
        } else {
          slot = Value();
        }
        continue;
      }
      const FsstDecodeView fsst_view = FsstViewAt(off_idx);
      Value tv =
          VarcharRegionAt(region, s, prm, rc, local, region_end, &fsst_view);
      if (!tv.IsNull()) {
        slot.SetString(tv.MoveString());
      }
    }
  }
  return Status::kSuccess;
}

Status PaxChunk::ResolveSeqCursors(uint32_t g, const Schema& sc,
                                   VarcharGroupCache* cache) const {
  cache->seq_ready = false;
  // Cursor kinds reset to their resolve-time values here; a stale
  // seq_int_filled would keep the flipped kind-1 pointers of the previous
  // group alive past the re-resolve. The deferred fills re-arm with it.
  cache->seq_int_filled = false;
  cache->seq_fill_pending = true;
  const uint32_t cc = footer_.column_count;
  if (g >= footer_.row_counts.size() || rg_prefix_.size() <= g) {
    return Status::kCorruption;
  }
  const uint16_t rc = footer_.row_counts[g];
  const size_t data_end =
      (data_.size() > static_cast<size_t>(footer_.footer_size) + 4)
          ? data_.size() - footer_.footer_size - 4
          : 0;
  // Null-bitmap base for the group (mirrors the legacy loop's hoist). A
  // bitmap slice that spills past the section end keeps the legacy per-row
  // bounds-checked test: cursors assume the whole group's slice is in range.
  bool null_ok = false;
  size_t null_base = 0;
  size_t null_bytes = 0;
  if (footer_.null_present &&
      footer_.row_counts.size() >=
          static_cast<size_t>(footer_.row_group_count)) {
    for (uint32_t gg = 0; gg < g; ++gg) {
      null_base += static_cast<size_t>(cc) *
                   static_cast<size_t>((footer_.row_counts[gg] + 7U) / 8U);
    }
    null_bytes = static_cast<size_t>((rc + 7U) / 8U);
    null_ok = true;
    if (null_base + (static_cast<size_t>(cc) * null_bytes) >
        footer_.null_bitmaps.size()) {
      return Status::kSuccess;  // unresolved: legacy path keeps the checks
    }
  }
  if (cache->seq_cols.size() != static_cast<size_t>(cc)) {
    cache->seq_cols.resize(cc);
  }
  for (uint32_t c = 0; c < cc; ++c) {
    VarcharGroupCache::SeqCol& cur = cache->seq_cols[c];
    cur = VarcharGroupCache::SeqCol{};
    if (!cache->IsColumnSelected(static_cast<slot_t>(c))) {
      continue;  // kind 0: projection drop
    }
    // Null test per row only when this column's slice carries a set bit; an
    // all-zero slice is pre-collapsed (null-free column).
    if (null_ok) {
      const uint8_t* bits = footer_.null_bitmaps.data() + null_base +
                            (static_cast<size_t>(c) * null_bytes);
      uint8_t any = 0;
      for (size_t i = 0; i < null_bytes; ++i) {
        any |= bits[i];
      }
      if (any != 0) {
        cur.has_null_bits = true;
        cur.null_bits = bits;
      }
    }
    const ValueType vt = sc.GetColumn(c).Type();
    const bool is_num = (vt == ValueType::kInt64 || vt == ValueType::kDouble);
    cur.is_double = (vt == ValueType::kDouble);
    // Decoded-int cache spans are whole-cell (a cell is either fully cached
    // or not at all, see BuildDecodedIntCache), so a full-row span covers
    // every local index the legacy `local < span` check admitted.
    if (is_num && decoded_int_cache_ready_ &&
        (static_cast<size_t>(g) * cc) + c + 1 < decoded_int_offsets_.size() &&
        decoded_int_offsets_[(static_cast<size_t>(g) * cc) + c + 1] -
                decoded_int_offsets_[(static_cast<size_t>(g) * cc) + c] ==
            static_cast<size_t>(rc) &&
        rc != 0) {
      cur.kind = 1;
      cur.vals = decoded_int_cache_.data() +
                 decoded_int_offsets_[(static_cast<size_t>(g) * cc) + c];
      cur.span = rc;
      continue;
    }
    // Region descriptors: same bounds and clamps as the legacy per-row path.
    const size_t off_idx = (static_cast<size_t>(g) * cc) + c;
    if (off_idx >= footer_.column_offsets.size() ||
        off_idx >= footer_.encodings.size() ||
        off_idx >= footer_.params.size()) {
      return Status::kCorruption;
    }
    const size_t off = footer_.column_offsets[off_idx];
    if (off > data_end) {
      return Status::kCorruption;
    }
    cur.kind = is_num ? uint8_t{2} : uint8_t{3};
    cur.region = data_.data() + off;
    if (off_idx + 1 < footer_.column_offsets.size() &&
        footer_.column_offsets[off_idx + 1] <= data_end) {
      const size_t succ = footer_.column_offsets[off_idx + 1];
      cur.region_end = (succ > off) ? data_.data() + succ : cur.region;
    } else {
      cur.region_end = (data_end > 0) ? data_.data() + data_end : cur.region;
    }
    cur.scheme = static_cast<Scheme>(footer_.encodings[off_idx]);
    cur.prm = footer_.params[off_idx];
    cur.off_idx = off_idx;
    cur.ef_dir = EfSelectDirAt(off_idx);
    if (!is_num) {
      cur.fsst = FsstViewAt(off_idx);
    }
    // D2Pfor sequential cursor: hoist the region parse out of the per-row
    // path. On a parse failure the column keeps taking the generic
    // Int64AtFast arm (which re-parses and fail-closes the same way).
    if (cur.scheme == Scheme::kD2Pfor) {
      D2PforRegion p;
      if (ParseD2PforRegion(cur.region, rc, cur.prm, cur.region_end, &p)) {
        cur.d2p_ok = true;
        cur.d2p_ckpt = p.ckpt;
        cur.d2p_inner = p.inner;
        cur.d2p_n = p.n;
        cur.d2p_ncp = p.ncp;
      }
    }
  }
  cache->seq_group = g;
  cache->seq_lo = rg_prefix_[g];
  cache->seq_hi = cache->seq_lo + rc;
  cache->seq_rc = rc;
  cache->seq_ready = true;
  return Status::kSuccess;
}

StatusOr<Row> PaxChunk::ReadRow(size_t global_row_index,
                                const Schema& sc) const {
  Row row;
  const Status status = ReadRowInto(global_row_index, sc, &row);
  if (status != Status::kSuccess) {
    return status;
  }
  return row;
}

namespace {
// Single-column decode for one resolved (row group, local row). Shared by
// ReadColumnValue's multi-group prefix path.
StatusOr<Value> ReadColumnValueIn(const PaxChunk& chunk, uint32_t g,
                                  uint32_t local, uint16_t rc, slot_t col_idx,
                                  const Schema& sc) {
  const PaxChunk::Footer& footer = chunk.GetFooter();
  const size_t off_idx = (g * footer.column_count) + col_idx;
  // Array-index validation, then offset-VALUE validation (see ReadRow:
  // a hostile offset value must not form a wild pointer).
  if (off_idx >= footer.column_offsets.size() ||
      off_idx >= footer.encodings.size() || off_idx >= footer.params.size()) {
    return Status::kCorruption;
  }
  const char* region = RegionStartAt(footer, g, col_idx, chunk.Data(), 0);
  if (region == nullptr) {
    return Status::kCorruption;
  }
  // A hostile successor offset must not enlarge the span past the data
  // area, which would defeat every `> region_end` decoder guard.
  const char* region_end = nullptr;
  if (off_idx + 1 < footer.column_offsets.size()) {
    region_end = RegionEndAt(footer, g, col_idx, chunk.Data());
    if (region_end == nullptr || region_end <= region) {
      region_end = region;
    }
  } else if (chunk.Data().size() > footer.footer_size + 4) {
    region_end =
        chunk.Data().data() + chunk.Data().size() - footer.footer_size - 4;
  } else {
    region_end = region;
  }
  const auto s = static_cast<Scheme>(footer.encodings[off_idx]);
  const uint32_t prm = footer.params[off_idx];
  if (CellIsNull(footer, g, col_idx, local)) {
    return Value();
  }
  const ValueType vt = sc.GetColumn(col_idx).Type();
  if (vt == ValueType::kInt64) {
    return Value(Int64AtFast(region, s, prm, rc, local, region_end,
                             chunk.EfSelectDirAt(off_idx)));
  }
  if (vt == ValueType::kDouble) {
    auto bits = static_cast<uint64_t>(Int64AtFast(
        region, s, prm, rc, local, region_end, chunk.EfSelectDirAt(off_idx)));
    double d = NAN;
    memcpy(&d, &bits, sizeof(double));
    return Value(d);
  }
  const FsstDecodeView fsst_view = chunk.FsstViewAt(off_idx);
  return VarcharRegionAt(region, s, prm, rc, local, region_end, &fsst_view);
}
}  // namespace

StatusOr<Value> PaxChunk::ReadColumnValue(size_t global_row_index,
                                          slot_t col_idx,
                                          const Schema& sc) const {
  // Public API: a col_idx past the chunk's column array would index
  // column_offsets/encodings out of bounds before any row lookup happens.
  // Also guard the schema length: sc.GetColumn(col_idx) below reads the
  // column's declared type, and a schema shorter than the chunk (version
  // skew) would index out of bounds.
  if (col_idx >= footer_.column_count || col_idx >= sc.ColumnCount()) {
    return Status::kNotExists;
  }
  // Hand-built footers can declare more row groups than row_counts entries.
  if (footer_.row_counts.size() < footer_.row_group_count) {
    return Status::kCorruption;
  }
  // Decoded-int fast path for the single-group shape (the dominant stored
  // layout): a fully cached cell reads as one load plus the null test,
  // mirroring ReadRowIntoImpl's cached arm. Like the legacy paths, a NULL
  // cell reports null (callers such as LsmView's key refresh and the merge
  // join's null-key skip depend on that verdict); the cache itself stores
  // the raw-0 clamped value, so the bitmap gate decides which of the two is
  // served. Partial spans fall through to the region decoders below:
  // BuildDecodedIntCache materializes whole cells only, so span == rc means
  // every local row of the group is covered.
  if (decoded_int_cache_ready_ && footer_.row_group_count == 1 &&
      decoded_int_offsets_.size() > static_cast<size_t>(col_idx) + 1 &&
      !footer_.row_counts.empty()) {
    const ValueType vt = sc.GetColumn(col_idx).Type();
    if (vt == ValueType::kInt64 || vt == ValueType::kDouble) {
      const size_t span =
          decoded_int_offsets_[col_idx + 1] - decoded_int_offsets_[col_idx];
      const uint16_t rc = footer_.row_counts[0];
      if (span == static_cast<size_t>(rc) && rc != 0 &&
          global_row_index < span) {
        if (footer_.null_present &&
            footer_.row_counts.size() >= footer_.row_group_count) {
          const size_t null_bytes = (static_cast<size_t>(rc) + 7) / 8;
          const size_t null_byte = (static_cast<size_t>(col_idx) * null_bytes) +
                                   (global_row_index >> 3);
          if (null_byte < footer_.null_bitmaps.size() &&
              ((static_cast<unsigned>(footer_.null_bitmaps[null_byte]) >>
                (7U - (global_row_index & 7))) &
               1U) != 0) {
            return Value();
          }
        }
        const int64_t cached =
            decoded_int_cache_[decoded_int_offsets_[col_idx] +
                               global_row_index];
        if (vt == ValueType::kInt64) {
          return Value(cached);
        }
        double d = NAN;
        const auto bits = static_cast<uint64_t>(cached);
        memcpy(&d, &bits, sizeof(d));
        return Value(d);
      }
    }
  }
  // Multi-group chunks resolve via the prefix table (O(log rg) instead of
  // walking every group per row). The single-group shape — the dominant
  // stored layout and a per-row hot path for column scans — falls into the
  // loop below, whose first iteration resolves immediately.
  if (footer_.row_group_count > 1 && !rg_prefix_.empty()) {
    uint32_t g = 0;
    uint32_t local = 0;
    const int res = ResolveRowGroup(global_row_index, &g, &local);
    if (res == 2) {
      return Status::kNotExists;
    }
    if (res == 0) {
      return ReadColumnValueIn(*this, g, local, footer_.row_counts[g], col_idx,
                               sc);
    }
    // res == 1 cannot happen (the guard above keeps the prefix populated).
  }
  size_t cumulative = 0;
  for (uint32_t g = 0; g < footer_.row_group_count; ++g) {
    uint16_t rc = footer_.row_counts[g];
    if (global_row_index < cumulative + rc) {
      size_t local = global_row_index - cumulative;
      const size_t off_idx = (g * footer_.column_count) + col_idx;
      if (off_idx >= footer_.column_offsets.size() ||
          off_idx >= footer_.encodings.size() ||
          off_idx >= footer_.params.size()) {
        return Status::kCorruption;
      }
      // Inlined RegionStartAt/RegionEndAt: this runs once per row, and the
      // two helper calls (each recomputing the data-area end) were a
      // measurable per-row cost in the sanitizer build.
      if (data_.size() <= static_cast<size_t>(footer_.footer_size) + 4) {
        return Status::kCorruption;
      }
      const size_t data_end = data_.size() - footer_.footer_size - 4;
      const size_t off = footer_.column_offsets[off_idx];
      if (off > data_end) {
        return Status::kCorruption;
      }
      const char* region = data_.data() + off;
      const char* region_end = nullptr;
      if (off_idx + 1 < footer_.column_offsets.size()) {
        const size_t succ = footer_.column_offsets[off_idx + 1];
        region_end =
            (succ > off && succ <= data_end) ? data_.data() + succ : region;
      } else {
        region_end = data_.data() + data_end;
      }
      const auto s = static_cast<Scheme>(footer_.encodings[off_idx]);
      const uint32_t prm = footer_.params[off_idx];
      // Inline the null-present early-out: chunks without null bitmaps (the
      // common case) skip the helper call on every row.
      if (footer_.null_present &&
          CellIsNull(footer_, g, static_cast<uint32_t>(col_idx),
                     static_cast<uint32_t>(local))) {
        return Value();
      }
      const ValueType vt = sc.GetColumn(col_idx).Type();
      // kD2Pfor sequential cursor: the chunk is immutable, so the only
      // mutable state is this thread's cursor, keyed on the region identity
      // (same region pointers + params + row count imply the same parse --
      // whether the chunk is the same object or a byte-identical reload).
      // Sequential column walks (the columnar-scan shape) then cost two
      // adds per row instead of a checkpoint replay; any jump re-parses
      // nothing and just replays the walk (same bits as Int64AtFast).
      if (s == Scheme::kD2Pfor) {
        struct ColCursor {
          bool parse_ok = false;
          bool armed = false;
          uint32_t loc = 0;
          uint64_t val = 0;
          uint64_t sd = 0;
          uint32_t rc = 0;
          uint32_t prm = 0;
          const char* region = nullptr;
          const char* region_end = nullptr;
          D2PforRegion p{};
        };
        static thread_local ColCursor cur;
        if (cur.region != region || cur.region_end != region_end ||
            cur.prm != prm || cur.rc != rc) {
          cur.region = region;
          cur.region_end = region_end;
          cur.prm = prm;
          cur.rc = rc;
          cur.armed = false;
          cur.parse_ok = ParseD2PforRegion(region, rc, prm, region_end, &cur.p);
        }
        if (cur.parse_ok) {
          uint64_t bits = 0;
          if (cur.armed && local == cur.loc + 1) {
            const uint64_t v = cur.val + cur.sd;
            if (static_cast<size_t>(cur.loc) + 2 < cur.p.n) {
              cur.sd += static_cast<uint64_t>(D2PforSecondDiff(cur.p, cur.loc));
            }
            cur.val = v;
            cur.loc = static_cast<uint32_t>(local);
            bits = v;
          } else {
            bits = D2PforValueAtArmed(cur.p, static_cast<uint32_t>(local),
                                      &cur.armed, &cur.loc, &cur.val, &cur.sd);
          }
          if (vt == ValueType::kInt64) {
            return Value(static_cast<int64_t>(bits));
          }
          double d = NAN;
          memcpy(&d, &bits, sizeof(d));
          return Value(d);
        }
      }
      if (vt == ValueType::kInt64) {
        return Value(Int64AtFast(region, s, prm, rc,
                                 static_cast<uint32_t>(local), region_end,
                                 EfSelectDirAt(off_idx)));
      }
      if (vt == ValueType::kDouble) {
        auto bits = static_cast<uint64_t>(
            Int64AtFast(region, s, prm, rc, static_cast<uint32_t>(local),
                        region_end, EfSelectDirAt(off_idx)));
        double d = NAN;
        memcpy(&d, &bits, sizeof(double));
        return Value(d);
      }
      const FsstDecodeView fsst_view = FsstViewAt(off_idx);
      return VarcharRegionAt(region, s, prm, rc, static_cast<uint32_t>(local),
                             region_end, &fsst_view);
    }
    cumulative += rc;
  }
  return Status::kNotExists;
}

namespace {
// Windowed lower-bound search over global rows [lo, hi] through the
// type-checked row reader. Sets *pos to the first row whose key column is >=
// target. Aborts (false) on null keys or read errors so callers fall back to
// the zone-map path instead of trusting a hostile model.
bool PgmWindowLowerBound(const PaxChunk& chunk, slot_t kci, const Schema& sc,
                         int64_t target, size_t lo, size_t hi, size_t* pos) {
  size_t a = lo, b = hi + 1;
  while (a < b) {
    const size_t mid = a + ((b - a) / 2);
    auto v = chunk.ReadColumnValue(mid, kci, sc);
    if (!v.HasValue() || v.Value().IsNull()) {
      return false;
    }
    if (v.Value().AsInt64() < target) {
      a = mid + 1;
    } else {
      b = mid;
    }
  }
  *pos = a;
  return true;
}
}  // namespace

Status PaxChunk::PointLookup(int64_t key_value, const Schema& sc,
                             size_t* row_index_out,
                             WorkloadTracker* workload) const {
  slot_t kci = footer_.key_column_index;
  if (kci >= footer_.column_count || kci >= sc.ColumnCount() ||
      sc.GetColumn(kci).Type() != ValueType::kInt64 ||
      footer_.key_column_type != static_cast<uint8_t>(ValueType::kInt64)) {
    return Status::kNotImplemented;
  }
  if (workload != nullptr) {
    workload->RecordPointLookup(sc, kci);
  }
  // Fast path: hash index first (O(1), zone-map independent).
  {
    size_t hidx = 0;
    if (HashLookup(key_value, &hidx) == Status::kSuccess) {
      *row_index_out = hidx;
      return Status::kSuccess;
    }
  }
  // Learned path: PGM prediction + windowed binary search (log eps probes
  // instead of log n region decodes). Only early-returns on hits; misses
  // fall through to the zone-map path (a lying model must never invent an
  // absence). Skipped when the hash index answered: HashLookup already
  // covers both hits and verified misses there.
  if (!footer_.hash_idx_present && footer_.pgm_present &&
      !footer_.pgm_segments.empty()) {
    const size_t total = TotalRowCount();
    if (total > 0) {
      const uint64_t pred = PgmPredict(footer_, key_value, total);
      const uint64_t slack = static_cast<uint64_t>(footer_.pgm_eps) + 1;
      const size_t lo = (pred > slack) ? pred - slack : 0;
      size_t hi = pred + slack;
      if (hi >= total) {
        hi = total - 1;
      }
      size_t found = 0;
      if (PgmWindowLowerBound(*this, kci, sc, key_value, lo, hi, &found) &&
          found <= hi) {
        auto vv = ReadColumnValue(found, kci, sc);
        if (vv.HasValue() && !vv.Value().IsNull() &&
            vv.Value().AsInt64() == key_value) {
          // Duplicates resolve to the FIRST row (contract shared with
          // LowerBoundIndex / HashLookup). The PGM window can land mid-run,
          // so walk back to the run's head before answering.
          while (found > 0) {
            auto pv = ReadColumnValue(found - 1, kci, sc);
            if (!pv.HasValue() || pv.Value().IsNull() ||
                pv.Value().AsInt64() != key_value) {
              break;
            }
            --found;
          }
          *row_index_out = found;
          return Status::kSuccess;
        }
      }
    }
  }
  // Select the candidate row group. Groups are ascending and disjoint.
  // When the zone map is absent (empty chunk, corrupt footer, or a chunk
  // built before zone maps existed), fall back to scanning every group
  // instead of reporting kNotExists for keys that exist.
  bool use_zone_map =
      footer_.zone_map_present &&
      footer_.group_min_keys.size() == footer_.row_group_count &&
      footer_.group_max_keys.size() == footer_.row_group_count;
  if (!use_zone_map) {
    // A hand-built footer can declare row groups with no backing arrays;
    // indexing row_counts/column_offsets below would read out of bounds.
    if (footer_.row_counts.size() < footer_.row_group_count ||
        footer_.column_offsets.size() <
            static_cast<size_t>(footer_.row_group_count) *
                footer_.column_count ||
        footer_.encodings.size() < footer_.column_offsets.size() ||
        footer_.params.size() < footer_.column_offsets.size()) {
      return Status::kCorruption;
    }
    for (uint32_t g = 0; g < footer_.row_group_count; ++g) {
      uint32_t rc = footer_.row_counts[g];
      const size_t off_idx = (g * footer_.column_count) + kci;
      // Offset-VALUE validation: the array checks above only prove the
      // entry exists; a hostile value must not form a wild pointer.
      const char* region = RegionStartAt(footer_, g, kci, data_, 0);
      if (region == nullptr) {
        return Status::kCorruption;
      }
      const auto s = static_cast<Scheme>(footer_.encodings[off_idx]);
      const char* reg_end = RegionEndAt(footer_, g, kci, data_);
      uint32_t local = 0;
      if (!RegionLowerBound(region, s, footer_.params[off_idx], rc, key_value,
                            &local, reg_end, EfSelectDirAt(off_idx))) {
        continue;
      }
      // NULL key cells encode raw 0: they must never satisfy a point
      // lookup (the hash-index builder skips them; the region path has to
      // agree or key 0 phantom-matches a NULL row). Walk to the first
      // non-null cell that still decodes to the key.
      const uint8_t* nulls = RgColNullBits(footer_, g, kci);
      bool hit = false;
      for (; local < rc; ++local) {
        if (nulls != nullptr &&
            ((nulls[local >> 3] >> (7 - (local & 7))) & 1) != 0) {
          continue;
        }
        // Sorted-region contract: the first non-null cell at/after the
        // landing point decides (its value is >= key; a mismatch ends the
        // probe).
        hit = Int64RegionAt(region, s, footer_.params[off_idx], rc, local,
                            reg_end, EfSelectDirAt(off_idx)) == key_value;
        break;
      }
      if (!hit) {
        continue;
      }
      size_t cumulative = 0;
      for (uint32_t i = 0; i < g; ++i) {
        cumulative += footer_.row_counts[i];
      }
      *row_index_out = cumulative + local;
      return Status::kSuccess;
    }
    return Status::kNotExists;
  }
  int64_t candidate = -1;  // group index, or -1 when pruned everywhere
  // Binary search: first group whose max >= key, then range check.
  {
    size_t lo = 0, hi = footer_.row_group_count;
    while (lo < hi) {
      size_t mid = lo + ((hi - lo) / 2);
      if (footer_.group_max_keys[mid] < key_value) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo < footer_.row_group_count &&
        footer_.group_min_keys[lo] <= key_value) {
      candidate = static_cast<int64_t>(lo);
    }
  }
  if (candidate < 0) {
    return Status::kNotExists;
  }
  // Lower-bound seek inside the candidate group's (possibly encoded) key
  // region, then require an exact match.
  auto g = static_cast<uint32_t>(candidate);
  if (footer_.row_counts.size() < footer_.row_group_count ||
      footer_.column_offsets.size() <
          static_cast<size_t>(footer_.row_group_count) * footer_.column_count ||
      footer_.encodings.size() < footer_.column_offsets.size() ||
      footer_.params.size() < footer_.column_offsets.size()) {
    return Status::kCorruption;
  }
  uint32_t rc = footer_.row_counts[g];
  const size_t off_idx = (g * footer_.column_count) + kci;
  // Offset-VALUE validation: see the no-zone-map path above.
  const char* region = RegionStartAt(footer_, g, kci, data_, 0);
  if (region == nullptr) {
    return Status::kCorruption;
  }
  const auto s = static_cast<Scheme>(footer_.encodings[off_idx]);
  const char* reg_end = RegionEndAt(footer_, g, kci, data_);
  uint32_t local = 0;
  if (!RegionLowerBound(region, s, footer_.params[off_idx], rc, key_value,
                        &local, reg_end, EfSelectDirAt(off_idx))) {
    return Status::kNotExists;
  }
  // NULL key cells encode raw 0: skip them like the no-zone-map path (and
  // the hash index) does.
  const uint8_t* nulls = RgColNullBits(footer_, g, kci);
  bool hit = false;
  for (; local < rc; ++local) {
    if (nulls != nullptr &&
        ((nulls[local >> 3] >> (7 - (local & 7))) & 1) != 0) {
      continue;
    }
    hit = Int64RegionAt(region, s, footer_.params[off_idx], rc, local, reg_end,
                        EfSelectDirAt(off_idx)) == key_value;
    break;
  }
  if (!hit) {
    return Status::kNotExists;
  }
  size_t cumulative = 0;
  for (uint32_t i = 0; i < g; ++i) {
    cumulative += footer_.row_counts[i];
  }
  *row_index_out = cumulative + local;
  return Status::kSuccess;
}

StatusOr<Row> PaxChunk::ReadRowByKey(int64_t key_value,
                                     const Schema& sc) const {
  size_t row_index = 0;
  Status st = PointLookup(key_value, sc, &row_index);
  if (st != Status::kSuccess) {
    return st;
  }
  return ReadRow(row_index, sc);
}

bool PaxChunk::RGOverlapsCol(int32_t g, slot_t col, int64_t lo,
                             int64_t hi) const {
  const size_t zi = (static_cast<size_t>(g) * footer_.column_count) + col;
  if (!footer_.all_zm_present || zi >= footer_.all_zone_mins.size() ||
      zi >= footer_.all_zone_maxs.size()) {
    return true;  // safe default when no zone info
  }
  // Zone maps cover NON-NULL values only: an all-NULL cell stores
  // INT64_MAX/INT64_MIN sentinels and would report "no overlap" even though
  // NULL cells decode as raw 0 (the invariant every consumer of this class
  // relies on). A cell with nulls therefore overlaps any range containing 0.
  if (RgColHasNull(footer_, static_cast<uint32_t>(g),
                   static_cast<uint32_t>(col)) &&
      lo <= 0 && 0 <= hi) {
    return true;
  }
  return footer_.all_zone_mins[zi] <= hi && footer_.all_zone_maxs[zi] >= lo;
}

Status PaxChunk::ColumnStats(slot_t col_idx, const Schema& sc,
                             Footer::ColumnStats* out) const {
  // Every sibling array this function indexes must cover col_idx: a
  // hand-built footer can carry mismatched section lengths, and the former
  // col_counts-only gate left col_sum_valid / col_sums / the ext-stats
  // vectors unguarded.
  const auto ci = static_cast<size_t>(col_idx);
  if (!footer_.col_stats_present || col_idx >= footer_.column_count ||
      col_idx >= sc.ColumnCount() ||
      sc.GetColumn(col_idx).Type() != ValueType::kInt64 ||
      ci >= footer_.col_counts.size() || ci >= footer_.col_sum_valid.size() ||
      ci >= footer_.col_sums.size() || (footer_.col_sum_valid[col_idx] == 0U)) {
    return Status::kNotImplemented;
  }
  out->count = footer_.col_counts[col_idx];
  out->sum = footer_.col_sums[col_idx];
  constexpr int B = Footer::ColumnStats::HIST_BINS;
  if (footer_.ext_stats_present && ci < footer_.col_mins.size() &&
      ci < footer_.col_maxs.size() && ci < footer_.col_nulls.size() &&
      ci < footer_.col_modes.size() && ci < footer_.col_mode_counts.size() &&
      footer_.col_hists.size() >= (ci + 1) * static_cast<size_t>(B)) {
    out->min = footer_.col_mins[col_idx];
    out->max = footer_.col_maxs[col_idx];
    out->null_count = footer_.col_nulls[col_idx];
    out->mode_value = footer_.col_modes[col_idx];
    out->mode_count = footer_.col_mode_counts[col_idx];
    for (int b = 0; b < B; ++b) {
      out->hist_counts[b] = footer_.col_hists[(static_cast<size_t>(col_idx) *
                                               static_cast<size_t>(B)) +
                                              static_cast<size_t>(b)];
    }
  }
  return Status::kSuccess;
}

// Metadata-only aggregates for an int64 column (single chunk, no key
// composition needed). Exact unless stated otherwise.
//
// COUNT(col) non-null: count - null_count (requires v12 ext stats).
Status PaxChunk::CountNonNull(slot_t col_idx, const Schema& sc,
                              uint64_t* out) const {
  if (!footer_.col_stats_present || col_idx >= footer_.column_count ||
      col_idx >= sc.ColumnCount() ||
      sc.GetColumn(col_idx).Type() != ValueType::kInt64 ||
      static_cast<size_t>(col_idx) >= footer_.col_counts.size()) {
    return Status::kNotImplemented;
  }
  uint64_t nulls = (footer_.ext_stats_present &&
                    static_cast<size_t>(col_idx) < footer_.col_nulls.size())
                       ? footer_.col_nulls[col_idx]
                       : 0;
  // A hostile ext-stats section can carry a null count above the observed
  // row total; the unsigned subtraction used to wrap to a huge count.
  nulls = std::min(nulls, footer_.col_counts[col_idx]);
  *out = footer_.col_counts[col_idx] - nulls;
  return Status::kSuccess;
}

// Equality COUNT(col = lit). Exact when lit is the column's mode (returns
// exact=true); otherwise an estimate from the containing histogram bin
// (exact=false).
Status PaxChunk::CountEqualFromStats(slot_t col_idx, const Schema& sc,
                                     int64_t lit, uint64_t* count_out,
                                     bool* exact_out) const {
  // Every array indexed below must cover col_idx (hand-built footers can
  // carry mismatched section lengths).
  const auto ci = static_cast<size_t>(col_idx);
  constexpr int B = Footer::ColumnStats::HIST_BINS;
  if (!footer_.ext_stats_present || col_idx >= footer_.column_count ||
      col_idx >= sc.ColumnCount() ||
      sc.GetColumn(col_idx).Type() != ValueType::kInt64 ||
      ci >= footer_.col_modes.size() || ci >= footer_.col_mode_counts.size() ||
      ci >= footer_.col_mins.size() || ci >= footer_.col_maxs.size() ||
      footer_.col_hists.size() < (ci + 1) * static_cast<size_t>(B)) {
    return Status::kNotImplemented;
  }
  if (lit == footer_.col_modes[col_idx]) {
    *count_out = footer_.col_mode_counts[col_idx];
    *exact_out = true;
    return Status::kSuccess;
  }
  // Estimate: the equi-width bin containing lit, normalized to the observed
  // total (bounded frequency map may undercount rare values). Reject
  // literals outside [mn, mx] up-front: HistogramBin computes
  // `(v - mn) * B / (mx - mn + 1)`, and a below-minimum literal wraps the
  // unsigned subtraction to a huge value that lands in some arbitrary bin,
  // returning a nonzero count for a value the column does not contain.
  const int64_t mn = footer_.col_mins[col_idx];
  const int64_t mx = footer_.col_maxs[col_idx];
  if (mx < mn || lit < mn || lit > mx) {
    *count_out = 0;
    *exact_out = false;
    return Status::kSuccess;
  }
  const uint64_t idx = HistogramBin(lit, mn, mx, B);
  *count_out = footer_.col_hists[(static_cast<size_t>(col_idx) * B) + idx];
  *exact_out = false;
  return Status::kSuccess;
}

// Range COUNT(col) in [lo,hi] estimate: sum of every histogram bin touching
// the range (whole bins, so boundary bins are OVER-counted). A proportional
// split of the two boundary bins would be more accurate but is not
// implemented; this stays a coarse estimate by contract (exact only for
// ranges aligned to bin edges).
Status PaxChunk::CountRangeFromStats(slot_t col_idx, const Schema& sc,
                                     int64_t lo, int64_t hi,
                                     uint64_t* count_out) const {
  // Every array indexed below must cover col_idx (hand-built footers can
  // carry mismatched section lengths).
  const auto ci = static_cast<size_t>(col_idx);
  constexpr int B = Footer::ColumnStats::HIST_BINS;
  if (!footer_.ext_stats_present || col_idx >= footer_.column_count ||
      col_idx >= sc.ColumnCount() ||
      sc.GetColumn(col_idx).Type() != ValueType::kInt64 ||
      ci >= footer_.col_mins.size() || ci >= footer_.col_maxs.size() ||
      footer_.col_hists.size() < (ci + 1) * static_cast<size_t>(B) || lo > hi) {
    return Status::kNotImplemented;
  }
  const int64_t mn = footer_.col_mins[col_idx];
  const int64_t mx = footer_.col_maxs[col_idx];
  if (mx < mn || hi < mn || lo > mx) {
    *count_out = 0;
    return Status::kSuccess;
  }
  // Clamp [lo,hi] into [mn,mx] before binning. Without this the unsigned
  // `v - mn` inside HistogramBin wraps for lo < mn, landing on an
  // arbitrary bin index (clamped to B-1), so the range loop returns 0 or
  // just the last bin -- silent undercounts for ranges that extend below
  // the observed min (or above the max, by symmetry).
  const int64_t clo = (lo < mn) ? mn : lo;
  const int64_t chi = (hi > mx) ? mx : hi;
  const auto bin_of = [&](int64_t v) -> uint64_t {
    return HistogramBin(v, mn, mx, static_cast<uint64_t>(B));
  };
  uint64_t bl = bin_of(clo), bh = bin_of(chi);
  uint64_t sum = 0;
  for (uint64_t b = bl; b <= bh && b < static_cast<uint64_t>(B); ++b) {
    sum += footer_.col_hists[(static_cast<size_t>(col_idx) * B) + b];
  }
  *count_out = sum;
  return Status::kSuccess;
}

// Metadata-only range aggregate across multiple chunks (composition step).
//
// The LSM read path merges several chunks whose key ranges may overlap (older
// generations shadowed by newer ones). When every chunk that intersects the
// query range [lo,hi] (1) is pairwise disjoint in key space and (2) sits fully
// inside [lo,hi], no key can be duplicated across chunks and no boundary row
// needs decoding: SUM(column) and COUNT(*) are answered exactly from per-chunk
// column sums and row counts. Any overlap or partial boundary invalidates the
// metadata answer and returns false (the caller falls back to a runtime merge
// scan). Chunks must use int64 keys with per-row-group zone maps.
bool MetadataRangeAggregate(const std::vector<const PaxChunk*>& chunks,
                            const Schema& sc, int64_t lo, int64_t hi,
                            slot_t col, int64_t* sum_out, uint64_t* count_out,
                            uint64_t max_gen) {
  if (lo > hi) {
    return false;
  }
  // Public API guard: col indexes sc.GetColumn below and the per-chunk
  // column sums; an out-of-range col read out of bounds.
  if (col >= sc.ColumnCount()) {
    return false;
  }
  const bool is_i64_col = sc.GetColumn(col).Type() == ValueType::kInt64;
  // (lo, hi) key range, per-chunk full range, per-chunk int64 column sum.
  struct Contrib {
    int64_t c_lo, c_hi;
    int64_t sum;
    uint64_t count;
  };
  std::vector<Contrib> contribs;
  for (const PaxChunk* c : chunks) {
    if (c == nullptr) {
      continue;
    }
    // MVCC: a chunk flushed after the read version is invisible. The
    // documented contract is that MetadataRangeAggregate answers exactly
    // for the *visible* chunk set; callers combine this result with their
    // own memtable / older-run reads at the same version. Treating a hidden
    // chunk as "no intersecting chunk exists" is intentional, not a bug.
    if (max_gen != UINT64_MAX && c->Generation() > max_gen) {
      continue;
    }
    const PaxChunk::Footer& f = c->GetFooter();
    // A hand-built footer can carry a key index past the schema; GetColumn
    // would throw std::out_of_range out of this bool API.
    if (f.key_column_index >= sc.ColumnCount()) {
      return false;
    }
    // Consistency gate every sibling read path has (FastColumnSum,
    // ReduceColumn, VectorizedScanColumn, ...): a row_counts array shorter
    // than the declared row_group_count is a hostile/incomplete footer.
    // Answering from it would pair a partial count against a full-column
    // sum and report the mismatched pair as "exact".
    if (f.row_counts.size() < f.row_group_count) {
      return false;
    }
    if (f.group_min_keys.size() < f.row_group_count ||
        f.group_max_keys.size() < f.row_group_count) {
      return false;
    }
    if (!f.zone_map_present || f.group_min_keys.empty() ||
        f.group_max_keys.size() != f.group_min_keys.size() ||
        sc.GetColumn(f.key_column_index).Type() != ValueType::kInt64) {
      return false;  // cannot determine the chunk's key range
    }
    const int64_t c_lo = f.group_min_keys.front();
    const int64_t c_hi = f.group_max_keys.back();
    if (c_hi < lo || c_lo > hi) {
      continue;  // no intersection with [lo,hi]
    }
    // Partial boundary: the chunk straddles lo or hi, so its column sum cannot
    // be restricted to the query range from metadata alone.
    if (c_lo < lo || c_hi > hi) {
      return false;
    }
    // Whole chunk in range: SUM and COUNT are exact from per-chunk stats.
    // Unsigned accumulate: per-group sums can each be near INT64_MAX on
    // adversarial data; a signed += here is UB (the cross-chunk fold below
    // already uses the same wrapping-u64 rule).
    uint64_t s = 0;
    const bool col_in_range = static_cast<size_t>(col) < f.column_count &&
                              static_cast<size_t>(col) < f.col_sum_valid.size();
    if (is_i64_col && col_in_range && f.col_stats_present &&
        (f.col_sum_valid[col] != 0U) &&
        static_cast<size_t>(col) < f.col_sums.size()) {
      s = static_cast<uint64_t>(f.col_sums[col]);
    } else if (is_i64_col && col_in_range && f.rg_sums_present) {
      for (size_t g = 0; g < f.row_group_count; ++g) {
        const size_t zi = (g * f.column_count) + col;
        if (zi >= f.rg_sums.size()) {
          return false;
        }
        s += static_cast<uint64_t>(f.rg_sums[zi]);
      }
    } else {
      return false;  // no per-chunk sum available for this column
    }
    // NULL key cells are never key values (PointLookup/HashLookup skip
    // them), so a chunk holding one cannot answer "exact SUM/COUNT over a
    // key range" from metadata: its col_sums/rg_sums fold the NULL-key
    // row's values in and its row count includes it. Fail closed -- the
    // footer already carries everything needed to detect this.
    if (f.ext_stats_present && f.key_column_index < f.col_nulls.size() &&
        f.col_nulls[f.key_column_index] > 0) {
      return false;
    }
    if (f.null_present) {
      for (uint32_t g = 0; g < f.row_group_count; ++g) {
        if (RgColHasNull(f, g, f.key_column_index)) {
          return false;
        }
      }
    }
    uint64_t cnt = 0;
    for (uint16_t rc : f.row_counts) {
      cnt += rc;
    }
    contribs.push_back({c_lo, c_hi, static_cast<int64_t>(s), cnt});
  }
  if (contribs.empty()) {
    *sum_out = 0;
    *count_out = 0;
    return true;  // empty range over the visible chunk set, exact
  }
  // Pairwise disjointness: sort by low key and require strict separation.
  std::sort(contribs.begin(), contribs.end(),
            [](const Contrib& a, const Contrib& b) { return a.c_lo < b.c_lo; });
  for (size_t i = 1; i < contribs.size(); ++i) {
    if (contribs[i - 1].c_hi >= contribs[i].c_lo) {
      return false;  // overlap
    }
  }
  // Unsigned accumulate: per-chunk sums can each be near INT64_MAX; a
  // signed += across chunks is UB on adversarial data.
  uint64_t total_sum = 0;
  uint64_t total_cnt = 0;
  for (const Contrib& cc : contribs) {
    total_sum += static_cast<uint64_t>(cc.sum);
    total_cnt += cc.count;
  }
  *sum_out = static_cast<int64_t>(total_sum);
  *count_out = total_cnt;
  return true;
}

Status PaxChunk::HashLookup(int64_t key_value, size_t* row_index_out) const {
  if (!footer_.hash_idx_present || (footer_.hash_idx_log2 == 0U) ||
      footer_.hash_idx_log2 > 31 || footer_.hash_keys.empty()) {
    return Status::kNotImplemented;
  }
  // Reject a corrupt/hand-built footer whose table sizes disagree with the
  // declared log2 capacity (would make the probe below read out of bounds or
  // loop forever with no EMPTY slot reachable).
  const size_t cap = static_cast<size_t>(1U) << footer_.hash_idx_log2;
  if (footer_.hash_keys.size() != cap || footer_.hash_rows.size() != cap) {
    return Status::kNotImplemented;
  }

  auto h = static_cast<uint64_t>(key_value);
  h ^= h >> 33;
  h *= 0xFF51AFD7ED558CCDULL;
  h ^= h >> 33;
  const uint32_t mask = (1U << footer_.hash_idx_log2) - 1;
  auto slot = static_cast<uint32_t>(h & mask);

  constexpr uint32_t EMPTY = UINT32_MAX;
  const size_t total = TotalRowCount();
  for (size_t probe = 0; probe < cap; ++probe) {
    if (footer_.hash_rows[slot] == EMPTY) {
      return Status::kNotExists;
    }
    if (footer_.hash_keys[slot] == key_value) {
      // A hand-built footer can store a row number past the chunk end.
      // Report corruption instead of a successful out-of-range index.
      if (footer_.hash_rows[slot] >= total) {
        return Status::kCorruption;
      }
      *row_index_out = footer_.hash_rows[slot];
      return Status::kSuccess;
    }
    slot = (slot + 1) & mask;
  }
  return Status::kNotExists;  // table was full: key provably absent
}

Status PaxChunk::LowerBoundIndex(int64_t key_value, const Schema& sc,
                                 size_t* index_out,
                                 WorkloadTracker* workload) const {
  slot_t kci = footer_.key_column_index;
  if (kci >= footer_.column_count || kci >= sc.ColumnCount() ||
      sc.GetColumn(kci).Type() != ValueType::kInt64 ||
      footer_.key_column_type != static_cast<uint8_t>(ValueType::kInt64)) {
    return Status::kNotImplemented;
  }
  if (workload != nullptr) {
    workload->RecordSortKey(sc, kci);
  }
  // Learned path: the PGM window is airtight on well-formed chunks (true
  // lower bound inside [pred-eps-1, pred+eps+1]), so a completed window
  // search IS the global lower bound with log(eps) probes. Anything
  // inconclusive (out-of-window, nulls, hostile model) falls through.
  if (footer_.pgm_present && !footer_.pgm_segments.empty()) {
    const size_t total = TotalRowCount();
    if (total > 0) {
      const uint64_t pred = PgmPredict(footer_, key_value, total);
      const uint64_t slack = static_cast<uint64_t>(footer_.pgm_eps) + 1;
      const size_t lo = (pred > slack) ? pred - slack : 0;
      size_t hi = pred + slack;
      if (hi >= total) {
        hi = total - 1;
      }
      size_t found = 0;
      if (PgmWindowLowerBound(*this, kci, sc, key_value, lo, hi, &found) &&
          found <= hi) {
        // Soundness against a lying model: the row before the window must
        // be below the key, or the window is not a valid lower-bound frame.
        bool frame_ok = (lo == 0);
        if (!frame_ok) {
          auto pv = ReadColumnValue(lo - 1, kci, sc);
          frame_ok = pv.HasValue() && !pv.Value().IsNull() &&
                     pv.Value().AsInt64() < key_value;
        }
        if (frame_ok) {
          *index_out = found;
          return Status::kSuccess;
        }
      }
    }
  }
  // The lower bound lives in the first group whose max key >= key_value
  // (groups are ascending and disjoint). Without a zone map, scan groups
  // in order and return the first group-local lower bound.
  bool use_zone_map =
      footer_.zone_map_present &&
      footer_.group_min_keys.size() == footer_.row_group_count &&
      footer_.group_max_keys.size() == footer_.row_group_count;
  if (!use_zone_map) {
    // Hand-built-footer guard: same array-size contract as PointLookup.
    if (footer_.row_counts.size() < footer_.row_group_count ||
        footer_.column_offsets.size() <
            static_cast<size_t>(footer_.row_group_count) *
                footer_.column_count ||
        footer_.encodings.size() < footer_.column_offsets.size() ||
        footer_.params.size() < footer_.column_offsets.size()) {
      return Status::kCorruption;
    }
    size_t cumulative = 0;
    for (uint32_t g = 0; g < footer_.row_group_count; ++g) {
      const size_t off_idx = (g * footer_.column_count) + kci;
      // Offset-VALUE validation: see PointLookup.
      const char* region = RegionStartAt(footer_, g, kci, data_, 0);
      if (region == nullptr) {
        return Status::kCorruption;
      }
      uint32_t local = 0;
      if (RegionLowerBound(
              region, static_cast<Scheme>(footer_.encodings[off_idx]),
              footer_.params[off_idx], footer_.row_counts[g], key_value, &local,
              RegionEndAt(footer_, g, kci, data_), EfSelectDirAt(off_idx))) {
        // NULL key cells encode raw 0 and are not key values: a seek that
        // lands on them (key <= 0, NULLs sort at the 0 position) advances
        // to the first real value.
        const uint8_t* nulls = RgColNullBits(footer_, g, kci);
        while (local < footer_.row_counts[g] && nulls != nullptr &&
               ((nulls[local >> 3] >> (7 - (local & 7))) & 1) != 0) {
          ++local;
        }
        if (local < footer_.row_counts[g]) {
          *index_out = cumulative + local;
          return Status::kSuccess;
        }
        // All cells from the landing point on are NULL: no non-null lower
        // bound in this group, keep scanning.
      }
      cumulative += footer_.row_counts[g];
    }
    return Status::kNotExists;
  }
  size_t lo = 0;
  size_t hi = footer_.row_group_count;
  while (lo < hi) {
    size_t mid = lo + ((hi - lo) / 2);
    if (footer_.group_max_keys[mid] < key_value) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  const auto candidate = static_cast<uint32_t>(lo);
  if (candidate >= footer_.row_group_count) {
    return Status::kNotExists;
  }
  if (footer_.row_counts.size() < footer_.row_group_count ||
      footer_.column_offsets.size() <
          static_cast<size_t>(footer_.row_group_count) * footer_.column_count ||
      footer_.encodings.size() < footer_.column_offsets.size() ||
      footer_.params.size() < footer_.column_offsets.size()) {
    return Status::kCorruption;
  }
  const size_t off_idx = (candidate * footer_.column_count) + kci;
  // Offset-VALUE validation: see PointLookup.
  const char* region = RegionStartAt(footer_, candidate, kci, data_, 0);
  if (region == nullptr) {
    return Status::kCorruption;
  }
  uint32_t local = 0;
  // Guaranteed to find: the group's max key >= key_value.
  if (!RegionLowerBound(region, static_cast<Scheme>(footer_.encodings[off_idx]),
                        footer_.params[off_idx], footer_.row_counts[candidate],
                        key_value, &local,
                        RegionEndAt(footer_, candidate, kci, data_),
                        EfSelectDirAt(off_idx))) {
    return Status::kNotExists;
  }
  // NULL key cells encode raw 0: skip them like the no-zone-map path.
  {
    const uint8_t* nulls = RgColNullBits(footer_, candidate, kci);
    while (local < footer_.row_counts[candidate] && nulls != nullptr &&
           ((nulls[local >> 3] >> (7 - (local & 7))) & 1) != 0) {
      ++local;
    }
    if (local >= footer_.row_counts[candidate]) {
      return Status::kNotExists;
    }
  }
  size_t cumulative = 0;
  for (uint32_t i = 0; i < candidate; ++i) {
    cumulative += footer_.row_counts[i];
  }
  *index_out = cumulative + local;
  return Status::kSuccess;
}

std::ostream& operator<<(std::ostream& o, const PaxChunk& c) {
  o << "PaxChunk{ data=" << c.data_.size()
    << " bytes, row_groups=" << c.footer_.row_group_count
    << " columns=" << c.footer_.column_count
    << " total_rows=" << c.TotalRowCount()
    << " schema_version=" << c.footer_.schema_version << " }";
  return o;
}

}  // namespace tinylamb

namespace tinylamb {
// ---------------------------------------------------------------------------
// Structural validation of serialized chunk bytes.
//
// Mirrors the WASM parse_footer walk: same order, same gates, same
// overflow-safe arithmetic -- the two implementations are kept deliberately
// redundant so a differential test can compare their verdicts.
// ---------------------------------------------------------------------------
namespace {

constexpr uint32_t kMaxSaneColumns = 1024;
constexpr uint32_t kMaxSaneRowGroups = 1U << 20;

struct WalkResult {
  bool ok{false};
  uint32_t format_version{0};
  size_t footer_start{0};
  size_t footer_len{0};
};

WalkResult ValidateWalk(const std::vector<char>& b) {
  const bool dbg = std::getenv("TINYLAMB_VALDBG") != nullptr;
  auto trace = [&](int stage, uint64_t v) {
    if (dbg) {
      std::cerr << "validate: FAIL stage=" << stage << " val=" << v << "\n";
    }
  };
  WalkResult w;
  const size_t len = b.size();
  if (len < 4) {
    trace(999, 344);
    return w;
  }
  const uint64_t footer_size =
      (static_cast<uint64_t>(static_cast<unsigned char>(b[len - 4])) << 24) |
      (static_cast<uint64_t>(static_cast<unsigned char>(b[len - 3])) << 16) |
      (static_cast<uint64_t>(static_cast<unsigned char>(b[len - 2])) << 8) |
      static_cast<uint64_t>(static_cast<unsigned char>(b[len - 1]));
  // Overflow-safe: footer_size must leave room for its own trailer and for
  // at least the fixed header.
  // The 30B fixed header lives inside the FOOTER; the data region itself
  // may legitimately be any size (even empty).
  // 34 = 30B fixed header minimum + 4B row-count trailer; WASM parse_footer
  // gates on the same bound (the old native `< 30` accepted footers the
  // WASM reader would reject, diverging the differential verdict).
  if (footer_size < 34 || footer_size > len || len - footer_size < 4) {
    trace(1, footer_size);
    return w;
  }
  const size_t footer_off = len - footer_size - 4;
  const auto rd8 = [&](size_t off) -> uint64_t {
    return static_cast<uint64_t>(static_cast<unsigned char>(b[off]));
  };
  const auto rd = [&](size_t off, int n) -> uint64_t {  // big-endian, n<=8
    uint64_t v = 0;
    for (int i = 0; i < n; ++i) {
      v = (v << 8) | rd8(off + static_cast<size_t>(i));
    }
    return v;
  };
  const uint64_t fmtver = rd(footer_off + 8, 4);
  if (fmtver != 1 && fmtver != 2) {
    trace(999, 1376);
    return w;
  }
  const uint64_t rg = rd(footer_off + 12, 4);
  const uint64_t cc = rd(footer_off + 16, 2);
  const uint64_t kci = rd(footer_off + 18, 2);
  if (cc == 0 || cc > kMaxSaneColumns || kci >= cc || rg > kMaxSaneRowGroups) {
    trace(999, 1607);
    return w;
  }
  // Mandatory section sizes must fit within footer_size (u64 math).
  uint64_t need = 30;
  need += static_cast<uint64_t>(rg) * 2;
  if (need > footer_size) {
    trace(999, 1775);
    return w;
  }
  const uint64_t co_end = need + (static_cast<uint64_t>(rg) * cc * 4);
  if (co_end > footer_size) {
    trace(999, 1882);
    return w;
  }
  const uint64_t zm_flag = rd(footer_off + 21, 1);
  if (zm_flag > 1) {
    trace(999, 1962);
    return w;
  }
  uint64_t pos = co_end;
  if (zm_flag != 0) {
    pos += static_cast<uint64_t>(rg) * 16;
    if (pos > footer_size) {
      trace(999, 2089);
      return w;
    }
  }
  // encodings + params (mandatory)
  const uint64_t enc_pos = pos;
  pos += static_cast<uint64_t>(rg) * cc;
  if (pos > footer_size) {
    trace(999, 2205);
    return w;
  }
  pos += static_cast<uint64_t>(rg) * cc * 4;
  if (pos > footer_size) {
    trace(999, 2285);
    return w;
  }
  // ---- Per-cell parameter + region-size sanity (decode-side UB gate) ----
  // The old gate checked only that a region START sits at/below data_end,
  // plus a few minimum sizes measured against the WHOLE data area. A
  // mutated chunk could therefore validate with, say, a Wavelet cell whose
  // region is 9 bytes but whose decoder walks 9 + W*ceil(n/8) bytes,
  // reading across every neighbour region and past the footer into
  // unbacked memory. Here each cell's true worst-case read extent is
  // computed from the scheme/params and required to fit inside
  // [off, succ), where succ is the NEXT cell's offset (or the data-area
  // end for the final cell) -- regions are written back to back, so the
  // successor offset is an exact bound. Header cross-checks (RLE run
  // count, DICT code count, EF low-bits width) catch footer/region
  // disagreements before any decoder trusts them. Must stay in lockstep
  // with the WASM parse_footer gates.
  {
    const uint64_t data_end = static_cast<uint64_t>(len) - footer_size - 4;
    const uint64_t co_base = footer_off + 30 + (rg * 2);
    auto rc_of = [&](uint64_t g) -> uint64_t {
      return rd(footer_off + 30 + (static_cast<size_t>(g) * 2), 2);
    };
    // Minimum byte extent of a whitelisted leaf-scheme inner region holding
    // `cnt` entries at absolute offset `ioff` (`avail` = bytes remaining in
    // the cell). Returns UINT64_MAX on any structural violation: whitelist
    // miss, bad param, or a header that disagrees with the stored inner
    // param. Lockstep with WASM inner_extent.
    auto inner_extent = [&](uint64_t ioff, uint64_t avail, uint64_t ischeme,
                            uint64_t iprm, uint64_t cnt) -> uint64_t {
      switch (ischeme) {
        case 0:  // PLAIN: cnt x 8B
          return cnt * 8;
        case 1:  // CONST: 8B
          return 8;
        case 2: {  // RLE: header run count must echo the stored param
          if (iprm == 0 || iprm > cnt) {
            return UINT64_MAX;
          }
          if (avail >= 4 && rd(ioff, 4) != iprm) {
            return UINT64_MAX;
          }
          return 4 + (12 * iprm);
        }
        case 3: {  // FOR-BA: [base 8B][cnt x bpv B]
          if (iprm < 1 || iprm > 8) {
            return UINT64_MAX;
          }
          return 8 + (cnt * iprm);
        }
        case 53: {  // SIMD-FOR: [base 8B][groups of 4 aligned lanes]
          if (iprm == 0 || iprm > 64 ||
              !SimdForWidthOk(static_cast<uint32_t>(iprm))) {
            return UINT64_MAX;
          }
          return SimdForExtent(static_cast<uint32_t>(cnt),
                               static_cast<uint32_t>(iprm));
        }
        case 4: {  // DICT: header code count must echo the stored param
          if (iprm == 0 || iprm > 65535) {
            return UINT64_MAX;
          }
          if (avail >= 2 && rd(ioff, 2) != iprm) {
            return UINT64_MAX;
          }
          const auto cw = static_cast<uint64_t>(BitsNeeded(iprm - 1));
          return 2 + ((cnt * cw + 7) / 8) + (8 * iprm);
        }
        case 8: {  // BitWeave: header width byte must echo the stored param
          if (iprm == 0 || iprm > 64) {
            return UINT64_MAX;
          }
          if (avail >= 9 && rd8(ioff + 8) != iprm) {
            return UINT64_MAX;
          }
          return 9 + ((cnt * iprm + 7) / 8);
        }
        case 16: {  // PFOR: [base 8B][w u8][nexc u16][cnt x w bits][exc]
          if (iprm == 0 || iprm > 64) {
            return UINT64_MAX;
          }
          if (avail >= 11 && rd8(ioff + 8) != iprm) {
            return UINT64_MAX;
          }
          if (avail >= 11) {
            const uint64_t nexc = rd(ioff + 9, 2);
            return 11 + ((cnt * iprm + 7) / 8) + (nexc * 10);
          }
          return UINT64_MAX;
        }
        default:
          return UINT64_MAX;
      }
    };
    uint64_t prev_off = 0;
    for (uint64_t cell = 0; cell < rg * cc; ++cell) {
      const uint64_t g = cell / cc;
      const uint64_t scheme = rd(footer_off + enc_pos + cell, 1);
      const uint64_t prm = rd(footer_off + enc_pos + (rg * cc) + (cell * 4), 4);
      const uint64_t off = rd(co_base + (cell * 4), 4);
      const uint64_t succ =
          (cell + 1 < rg * cc) ? rd(co_base + ((cell + 1) * 4), 4) : data_end;
      // Offsets must be non-decreasing and inside the data area.
      if (off < prev_off || off > succ || succ > data_end) {
        trace(80, off);
        return w;
      }
      prev_off = off;
      const uint64_t n = rc_of(g);
      if (n > 1024) {  // kRowGroupSize contract
        trace(94, n);
        return w;
      }
      const uint64_t have = succ - off;
      // Renamed from `need`: the outer scope's footer-size accumulator uses
      // the same name, and -Wshadow rightly flags the reuse.
      uint64_t region_need = 0;
      switch (scheme) {
        case 0:  // PLAIN: int64 8B/row or varchar 4B len + bytes per row
          // The footer does not carry per-column types, so only the varchar
          // 4B/row floor is provable here: an int64 PLAIN cell could pass
          // validation at half its true size. The decoders re-check the
          // 8B/row extent against region_end (Int64RegionAt / the bulk
          // paths) and yield defined zeros instead of reading into the
          // successor cell.
          region_need = 4 * n;
          break;
        case 1:  // CONST: one [8B i64] or [u32 len + bytes] entry
          region_need = 4;
          break;
        case 2:  // RLE: [runs u32][{value 8B}{len u32}] x runs
          if (prm == 0 || prm > n) {
            trace(81, prm);
            return w;
          }
          if (have >= 4 && rd(off, 4) != prm) {
            trace(81, rd(off, 4));
            return w;
          }
          region_need = 4 + (12 * prm);
          break;
        case 3:  // FOR-BA: [base 8B][rc*bpv LE offsets]
          if (prm == 0 || prm > 8) {
            trace(82, prm);
            return w;
          }
          region_need = 8 + (n * prm);
          break;
        case 53: {  // SIMD-FOR: [base 8B][group: 4 lanes x 8B BE]
          // The width must be one the four-lane layout can express, and the
          // extent is DERIVED from n and w rather than read from the bytes, so
          // there is no count field to under-state: a region that decodes is
          // exactly SimdForExtent(n, prm) bytes, and anything shorter is
          // rejected here rather than walked off the end later.
          if (prm == 0 || prm > 64 ||
              !SimdForWidthOk(static_cast<uint32_t>(prm))) {
            trace(156, prm);
            return w;
          }
          if (have < kSimdForHeaderBytes) {
            trace(156, have);
            return w;
          }
          region_need = SimdForExtent(static_cast<uint32_t>(n),
                                      static_cast<uint32_t>(prm));
          break;
        }
        case 4: {  // DICT: [ncodes u16][packed codes][dict entries]
          if (prm == 0 || prm > 65535) {
            trace(83, prm);
            return w;
          }
          if (have >= 2 && rd(off, 2) != prm) {
            trace(83, rd(off, 2));
            return w;
          }
          const int cw = BitsNeeded(prm - 1);
          // int64 dicts pay 8B/entry, varchar dicts 4B+len; floor at 4B.
          region_need =
              2 + ((n * static_cast<uint64_t>(cw) + 7) / 8) + (4 * prm);
          break;
        }
        case 55: {  // S4: [base 8B][nblk u8][w x nblk][lo x nblk][ck x
                    // nblk-1][payload]
          // nblk is DERIVED from n and pinned here, so a hostile region cannot
          // claim a different block count. Every width is checked before it is
          // used to size a payload, and the extent is computed FROM the width
          // table, so a region that decodes is exactly as long as its own
          // widths say.
          if (have < 9) {
            trace(157, have);
            return w;
          }
          const uint64_t nblk = (n + kS4Block - 1) / kS4Block;
          if (nblk == 0 || rd(off + 8, 1) != nblk) {
            trace(157, rd(off + 8, 1));
            return w;
          }
          region_need = 9 + nblk + (8 * nblk) + (8 * (nblk - 1));
          if (region_need > have) {
            trace(157, have);
            return w;
          }
          uint64_t payload = 0;
          for (uint64_t blk = 0; blk < nblk; ++blk) {
            const uint64_t bw = rd(off + 9 + blk, 1);
            if (bw < 1 || bw > 64) {
              trace(157, bw);
              return w;
            }
            const uint64_t rows =
                std::min<uint64_t>(kS4Block, n - (blk * kS4Block));
            payload += (rows * bw + 7) / 8;
          }
          region_need += payload;
          if (region_need > have) {
            trace(157, have);
            return w;
          }
          // The semantic replay. This scheme's appeal is that a point lookup
          // READS its seek word and trusts it, so a seek word that is not the
          // value of its block's first row would make a point read return a
          // different value than a scan of the same row -- and no other check
          // in this file can see that. Replaying the region and pinning every
          // seek word to the running value is what makes the O(1) probe safe.
          //
          // The replay also re-derives each block's widest offset and requires
          // it to match the stored width. That is a canonicality rule, the same
          // shape as kHuffDict's Kraft check: it refuses a region padded with
          // bits it does not need, so a hostile image cannot buy slack width
          // and a reader cannot disagree with the encoder about how wide the
          // block really is.
          {
            const char* packed =
                b.data() + off + 9 + nblk + (8 * nblk) + (8 * (nblk - 1));
            const auto base = static_cast<int64_t>(rd(off, 8));
            int64_t run = base;
            uint64_t boff = 0;  // BYTE offset into `packed`
            for (uint64_t blk = 0; blk < nblk; ++blk) {
              const int bwid = static_cast<int>(rd(off + 9 + blk, 1));
              const auto lo =
                  static_cast<int64_t>(rd(off + 9 + nblk + (8 * blk), 8));
              const uint64_t rows =
                  std::min<uint64_t>(kS4Block, n - (blk * kS4Block));
              if (blk != 0) {
                const auto seek = static_cast<int64_t>(
                    rd(off + 9 + nblk + (8 * nblk) + (8 * (blk - 1)), 8));
                if (seek != run) {
                  trace(157, static_cast<uint64_t>(seek));
                  return w;
                }
              }
              // Block payloads are BYTE-aligned (the encoder packs each block
              // separately), so the walk advances a BYTE offset. Scaling it by
              // the WIDTH instead agrees only when w == 8.
              uint64_t maxw = 0;
              // Unsigned, like the decoders: a region that walks off both int64
              // ends must still replay deterministically here, or validation
              // would reject an image the encoder legitimately produced.
              auto prev = static_cast<uint64_t>(blk == 0 ? base : run);
              const auto ulo = static_cast<uint64_t>(lo);
              for (uint64_t t = 0; t < rows; ++t) {
                const uint64_t word = UnpackBitAt(
                    packed, (boff * 8) + (t * static_cast<uint64_t>(bwid)),
                    bwid);
                maxw = std::max(word, maxw);
                prev += ulo + word;
              }
              run = static_cast<int64_t>(prev);
              const uint64_t want =
                  BitsNeeded(maxw) == 0
                      ? uint64_t{1}
                      : static_cast<uint64_t>(BitsNeeded(maxw));
              if (std::cmp_not_equal(want, bwid)) {
                trace(157, static_cast<uint64_t>(bwid));
                return w;
              }
              boff += (rows * static_cast<uint64_t>(bwid) + 7) / 8;
            }
          }
          break;
        }
        case 56: {  // TOPK: [d u16][nexc u16][hot 15x8B][freq 15xu16]
                    // [ischeme u8][iparam u32][nibbles ceil(n/2)][inner]
          // Every downstream extent the decoders derive comes from the
          // header fields, so all of them are pinned here -- including a
          // FULL nibble-stream walk histogrammed against the stored
          // frequency table (plus the sentinel count against nexc): the
          // closed-form kernels and the per-row pass tables trust
          // `freq`/`nexc` without deriving them from the stream.
          constexpr uint64_t kWalkHot = 15;
          constexpr uint64_t kWalkHeader =
              2 + 2 + (kWalkHot * 8) + (kWalkHot * 2) + 1 + 4;  // 159
          if (n == 0 || have < kWalkHeader) {
            trace(158, have);
            return w;
          }
          const uint64_t d = rd(off, 2);
          const uint64_t nexc = rd(off + 2, 2);
          // Footer param IS the exception count; d == 0 degenerates to
          // CONST (never emitted) and nexc > n is impossible.
          if (d == 0 || d > kWalkHot || nexc != prm || nexc > n) {
            trace(158, d);
            return w;
          }
          const uint64_t nib = (n + 1) / 2;
          if (kWalkHeader + nib > have) {
            trace(158, have);
            return w;
          }
          // Hot slots: entries < d ascending with no duplicates (the code =
          // rank convention is what makes range predicates a code window);
          // slots >= d are zero padding with a zero count.
          for (uint64_t c = 0; c < d; ++c) {
            const auto v = static_cast<int64_t>(rd(off + 4 + (c * 8), 8));
            const uint64_t fq = rd(off + 124 + (c * 2), 2);
            if (fq == 0 || fq > n) {
              trace(158, fq);
              return w;
            }
            if (c > 0) {
              const auto prev_v =
                  static_cast<int64_t>(rd(off + 4 + ((c - 1) * 8), 8));
              if (v <= prev_v) {
                trace(158, static_cast<uint64_t>(v));
                return w;
              }
            }
          }
          uint64_t fsum = 0;
          for (uint64_t c = 0; c < kWalkHot; ++c) {
            fsum += rd(off + 124 + (c * 2), 2);
          }
          if (fsum + nexc != n) {
            trace(158, fsum);
            return w;
          }
          // Inner whitelist + extent (kFreq's arm, minus the bitmap).
          const uint64_t ischeme = rd(off + 154, 1);
          const uint64_t iprm = rd(off + 155, 4);
          if (nexc > 0) {
            const uint64_t ioff = off + kWalkHeader + nib;
            const uint64_t iavail = off + have - ioff;
            const uint64_t ineed =
                inner_extent(ioff, iavail, ischeme, iprm, nexc);
            if (ineed == UINT64_MAX || ineed > iavail) {
              trace(158, ineed);
              return w;
            }
            region_need = kWalkHeader + nib + ineed;
          } else {
            // nexc == 0: no inner bytes may hide past the nibble stream.
            if (ischeme != 0 || iprm != 0) {
              trace(158, ischeme);
              return w;
            }
            region_need = kWalkHeader + nib;
          }
          // One-time semantic walk: histogram every nibble; codes must sit
          // in [0, d) or be the sentinel, sentinels must number exactly
          // nexc, and each hot code must occur exactly freq[c] times. (The
          // odd-n tail nibble is padding and never read: the walk indexes
          // rows, not nibble slots.)
          std::array<uint64_t, 16> hist{};
          for (uint64_t i = 0; i < n; ++i) {
            const auto byte =
                static_cast<uint8_t>(rd8(off + kWalkHeader + (i >> 1)));
            const uint64_t code = (i % 2 == 0)
                                      ? static_cast<uint64_t>(byte >> 4)
                                      : static_cast<uint64_t>(byte & 0x0F);
            if (code != 15 && code >= d) {
              trace(158, code);
              return w;
            }
            ++hist[code];
          }
          if (hist[15] != nexc) {
            trace(158, hist[15]);
            return w;
          }
          for (uint64_t c = 0; c < d; ++c) {
            if (hist[c] != rd(off + 124 + (c * 2), 2)) {
              trace(158, hist[c]);
              return w;
            }
          }
          break;
        }
        case 57: {  // SFOR: [s u8][ng u16][dir ng x (msb 8B][base 8B]
                    //   [row_start u16][w u8])][per-group byte-aligned
                    //   payloads]
          // The header drives every extent a reader derives, so it is all
          // pinned here -- plus a FULL replay: the group table must tile
          // [0, n) with strictly ascending msb (the disjointness the
          // predicate brackets prune on), each stored width must equal its
          // group's widest delta (the kS4 canonicality rule -- no slack
          // bits), and base+maxdelta must stay under 2^s (the reconstruction
          // must not carry into the msb, or brackets and values disagree).
          if (n == 0 || have < 3) {
            trace(159, have);
            return w;
          }
          const uint64_t s = rd(off, 1);
          const uint64_t ng = rd(off + 1, 2);
          if (s < 1 || s > 63 || prm != s || ng == 0 || ng > n) {
            trace(159, s);
            return w;
          }
          if (3 + (19 * ng) > have) {
            trace(159, have);
            return w;
          }
          const uint64_t lsb_mask = (UINT64_C(1) << s) - 1;
          uint64_t payload_bytes = 0;
          uint64_t prev_rs = 0;
          uint64_t prev_msb = 0;
          const uint64_t dirbase = off + 3;
          for (uint64_t gidx = 0; gidx < ng; ++gidx) {
            const uint64_t ent = dirbase + (gidx * 19);
            const uint64_t msb = rd(ent, 8);
            const uint64_t base = rd(ent + 8, 8);
            const uint64_t rs = rd(ent + 16, 2);
            const uint64_t gw = rd(ent + 18, 1);
            if (gw > s || base > lsb_mask) {
              trace(159, gw);
              return w;
            }
            if (gidx == 0) {
              if (rs != 0) {
                trace(159, rs);
                return w;
              }
            } else {
              if (rs <= prev_rs || msb <= prev_msb) {
                trace(159, rs);
                return w;
              }
            }
            prev_rs = rs;
            prev_msb = msb;
            const uint64_t rows =
                (gidx + 1 == ng) ? n - rs : rd(ent + 19 + 16, 2) - rs;
            payload_bytes += (rows * gw + 7) / 8;
          }
          if (prev_rs >= n) {
            trace(159, prev_rs);
            return w;
          }
          region_need = 3 + (19 * ng) + payload_bytes;
          if (region_need > have) {
            trace(159, have);
            return w;
          }
          // Semantic replay: unpack each group's deltas, require the widest
          // to match the stored width exactly, and require base+maxdelta to
          // fit under 2^s. The random-access probe trusts base/w/msb, so a
          // hand-built footer padded with slack width (or a base whose group
          // overflows into the prefix) must not validate.
          {
            uint64_t boff = 0;
            for (uint64_t gidx = 0; gidx < ng; ++gidx) {
              const uint64_t ent = dirbase + (gidx * 19);
              const uint64_t base = rd(ent + 8, 8);
              const uint64_t gw = rd(ent + 18, 1);
              const uint64_t rs = rd(ent + 16, 2);
              const uint64_t rows =
                  (gidx + 1 == ng) ? n - rs : rd(ent + 19 + 16, 2) - rs;
              if (gw != 0) {
                uint64_t maxd = 0;
                for (uint64_t t = 0; t < rows; ++t) {
                  const uint64_t delta =
                      UnpackBitAt(b.data() + off + 3 + (19 * ng),
                                  (boff * 8) + (t * gw), static_cast<int>(gw));
                  maxd = std::max(delta, maxd);
                }
                if (std::cmp_not_equal(BitsNeeded(maxd), gw)) {
                  trace(159, maxd);
                  return w;
                }
                if (base + maxd > lsb_mask) {
                  trace(159, maxd);
                  return w;
                }
              }
              boff += (rows * gw + 7) / 8;
            }
          }
          break;
        }
        case 60: {  // EXPFOR: [ng u16][dir ng x (gkey u16][base u64]
                    //   [row_start u16][w u8])][per-run byte-aligned payloads]
          // The header drives every extent a reader derives, so it is all
          // pinned here -- plus a FULL replay mirroring case 57: the run
          // table must tile [0, n) (the directory is IN ROW ORDER -- bands
          // of an unsorted column interleave, so there is NO gkey ordering
          // to pin), each gkey must be a finite band (0xffe cap: 0xfff is
          // exponent 0x7ff, NaN/Inf), each stored width must equal its run's
          // widest mantissa offset (the kS4/kSfor canonicality rule -- no slack
          // bits), and base + maxoffset must stay under 2^52 (the
          // reconstruction must not carry into the band bits, or brackets and
          // values disagree).
          if (n == 0 || have < 2) {
            trace(160, have);
            return w;
          }
          const uint64_t ng = rd(off, 2);
          if (prm != 0 || ng == 0 || ng > n) {
            trace(160, ng);
            return w;
          }
          if (2 + (13 * ng) > have) {
            trace(160, have);
            return w;
          }
          uint64_t payload_bytes = 0;
          uint64_t prev_rs = 0;
          const uint64_t dirbase = off + 2;
          for (uint64_t gidx = 0; gidx < ng; ++gidx) {
            const uint64_t ent = dirbase + (gidx * 13);
            const uint64_t gkey = rd(ent, 2);
            const uint64_t base = rd(ent + 2, 8);
            const uint64_t rs = rd(ent + 10, 2);
            const uint64_t gw = rd(ent + 12, 1);
            if (gkey > 0xffe || base > (UINT64_C(1) << 52) - 1 || gw > 52) {
              trace(160, gw);
              return w;
            }
            if (gidx == 0) {
              if (rs != 0) {
                trace(160, rs);
                return w;
              }
            } else {
              if (rs <= prev_rs) {
                trace(160, rs);
                return w;
              }
            }
            prev_rs = rs;
            const uint64_t rows =
                (gidx + 1 == ng) ? n - rs : rd(ent + 13 + 10, 2) - rs;
            payload_bytes += (rows * gw + 7) / 8;
          }
          if (prev_rs >= n) {
            trace(160, prev_rs);
            return w;
          }
          region_need = 2 + (13 * ng) + payload_bytes;
          if (region_need > have) {
            trace(160, have);
            return w;
          }
          // Semantic replay: unpack each group's offsets, require the widest
          // to match the stored width exactly, and require base+maxoffset to
          // fit under 2^52. The bracket fold and the probe both trust
          // base/w/gkey, so a hand-built footer padded with slack width (or
          // a group whose offsets carry into the band) must not validate.
          {
            uint64_t boff = 0;
            for (uint64_t gidx = 0; gidx < ng; ++gidx) {
              const uint64_t ent = dirbase + (gidx * 13);
              const uint64_t base = rd(ent + 2, 8);
              const uint64_t gw = rd(ent + 12, 1);
              const uint64_t rs = rd(ent + 10, 2);
              const uint64_t rows =
                  (gidx + 1 == ng) ? n - rs : rd(ent + 13 + 10, 2) - rs;
              if (gw != 0) {
                uint64_t maxd = 0;
                for (uint64_t t = 0; t < rows; ++t) {
                  const uint64_t delta =
                      UnpackBitAt(b.data() + off + 2 + (13 * ng),
                                  (boff * 8) + (t * gw), static_cast<int>(gw));
                  maxd = std::max(delta, maxd);
                }
                if (std::cmp_not_equal(BitsNeeded(maxd), gw)) {
                  trace(160, maxd);
                  return w;
                }
                if (base + maxd > (UINT64_C(1) << 52) - 1) {
                  trace(160, maxd);
                  return w;
                }
              }
              boff += (rows * gw + 7) / 8;
            }
          }
          break;
        }
        case 61: {  // SORT-DICT: [nb u8][rw u8][nb x (w u8][first 8B])]
                    // [(nb+1) x 8B psum][rank ceil(n*rw/8)][gap planes]
          // Every downstream extent derives from (n, nb, the width table),
          // so all of them are pinned here -- plus a FULL replay: the gap
          // walk re-derives each block's widest gap (the kS4 canonicality
          // rule), pins every first (ascending, non-strict: duplicates may
          // span a block boundary) and every psum against the wrapped
          // running sum, and the rank plane must BE a permutation of
          // [0, n) -- the closed-form SUM loads psum[nb] and every reader
          // gathers through the rank plane, so neither may lie.
          if (n == 0) {
            trace(162, n);
            return w;
          }
          const uint64_t wnb = (n + kSortDictBlock - 1) / kSortDictBlock;
          if (have < 2 || rd(off, 1) != wnb || prm != wnb) {
            trace(162, rd(off, 1));
            return w;
          }
          const auto wrw =
              static_cast<uint64_t>(BitsNeeded(static_cast<uint64_t>(n) - 1));
          if (rd(off + 1, 1) != wrw) {
            trace(162, rd(off + 1, 1));
            return w;
          }
          uint64_t gap_bytes = 0;
          for (uint64_t blk = 0; blk < wnb; ++blk) {
            const uint64_t bw = rd(off + 2 + blk, 1);  // widths plane
            if (bw > 64) {
              trace(162, bw);
              return w;
            }
            const uint64_t rows =
                std::min<uint64_t>(kSortDictBlock, n - (blk * kSortDictBlock));
            gap_bytes += ((rows - 1) * bw + 7) / 8;
          }
          region_need =
              2 + (wnb * 9) + ((wnb + 1) * 8) + ((n * wrw + 7) / 8) + gap_bytes;
          if (region_need > have) {
            trace(162, have);
            return w;
          }
          {
            const uint64_t rank_base = off + 2 + (wnb * 9) + ((wnb + 1) * 8);
            const uint64_t gap_base = rank_base + ((n * wrw + 7) / 8);
            // Permutation check over the rank plane (n <= 1024, so a 16-word
            // seen bitmap bounds the walk).
            std::array<uint64_t, 16> seen{};
            for (uint64_t i = 0; i < n; ++i) {
              const uint64_t rk = UnpackBitAt(b.data() + rank_base, i * wrw,
                                              static_cast<int>(wrw));
              if (rk >= n || ((seen[rk >> 6] >> (rk & 63)) & 1) != 0) {
                trace(162, rk);
                return w;
              }
              seen[rk >> 6] |= UINT64_C(1) << (rk & 63);
            }
            // Semantic replay over the gap planes.
            uint64_t running = 0;  // wrapped prefix sum up to the block start
            int64_t prev_last = 0;
            uint64_t gbyte = 0;
            for (uint64_t blk = 0; blk < wnb; ++blk) {
              const int bwid =
                  static_cast<int>(rd(off + 2 + blk, 1));  // widths plane
              const auto first =
                  static_cast<int64_t>(rd(off + 2 + wnb + (blk * 8), 8));
              const uint64_t psum =
                  rd(off + 2 + (wnb * 9) + (blk * 8), 8);  // psums plane
              const uint64_t rows = std::min<uint64_t>(
                  kSortDictBlock, n - (blk * kSortDictBlock));
              if (psum != running) {
                trace(162, psum);
                return w;
              }
              if (blk != 0 && first < prev_last) {
                trace(162, static_cast<uint64_t>(first));
                return w;
              }
              // Unsigned accumulate: the defined mod-2^64 walk (the decoders
              // do the same), so a region that wraps through both int64
              // extremes still replays deterministically here.
              auto v = static_cast<uint64_t>(first);
              uint64_t bsum = v;
              uint64_t maxgap = 0;
              for (uint64_t t = 1; t < rows; ++t) {
                const uint64_t gap = UnpackBitAt(
                    b.data() + gap_base,
                    (gbyte * 8) + ((t - 1) * static_cast<uint64_t>(bwid)),
                    bwid);
                maxgap = std::max(gap, maxgap);
                v += gap;
                bsum += v;
              }
              const auto want = static_cast<uint64_t>(BitsNeeded(maxgap));
              if (std::cmp_not_equal(want, bwid)) {
                trace(162, static_cast<uint64_t>(bwid));
                return w;
              }
              running += bsum;
              prev_last = static_cast<int64_t>(v);
              gbyte += ((rows - 1) * static_cast<uint64_t>(bwid) + 7) / 8;
            }
            // The total sentinel closes the table: this is the word the
            // closed-form SUM kernel loads.
            if (rd(off + 2 + (wnb * 9) + (wnb * 8), 8) != running) {
              trace(162, running);
              return w;
            }
          }
          break;
        }
        case 59: {  // TILE-DICT: headerless chain of 64-row tiles
                    //   [d u8][dict d x 8B BE ascending][codes w bits each]
          // The chain IS the extent, so the walk derives it: tiles must
          // cover n exactly (no slack rows -- a truncated chain decodes
          // clamp-bucket zeros, which the kernels must never need), each
          // dictionary strictly ascending (the code-window fold bisects
          // it), and every code < d (the clamp bucket never fires on
          // encoder output, so the kernels are entitled to trust their
          // windows). param carries no meaning: 0, always.
          if (n == 0 || prm != 0) {
            trace(160, prm);
            return w;
          }
          uint64_t tile_start = 0;
          uint64_t tile_need = 0;
          for (;;) {
            const uint64_t tp = off + tile_need;
            if (tp + 1 > off + have) {
              trace(160, have);
              return w;
            }
            const uint64_t d = rd8(tp);
            if (d == 0 || d > kTileDictMaxD) {
              trace(160, d);
              return w;
            }
            const uint64_t rows = (n - tile_start < kTileDictRows)
                                      ? n - tile_start
                                      : kTileDictRows;
            const int cw = TileDictWidth(static_cast<uint32_t>(d));
            const uint64_t cb = (rows * static_cast<uint64_t>(cw) + 7) / 8;
            const uint64_t dict_at = tp + 1;
            const uint64_t codes_at = dict_at + (d * 8);
            if (codes_at + cb > off + have) {
              trace(160, have);
              return w;
            }
            int64_t prev_v = 0;
            for (uint64_t c = 0; c < d; ++c) {
              const auto v = static_cast<int64_t>(rd(dict_at + (c * 8), 8));
              if (c > 0 && v <= prev_v) {
                trace(160, static_cast<uint64_t>(v));
                return w;
              }
              prev_v = v;
            }
            if (cw > 0) {
              for (uint64_t j = 0; j < rows; ++j) {
                const uint64_t code = UnpackBitAt(
                    b.data() + codes_at, j * static_cast<uint64_t>(cw), cw);
                if (code >= d) {
                  trace(160, code);
                  return w;
                }
              }
            }
            tile_need += 1 + (d * 8) + cb;
            tile_start += rows;
            if (tile_start >= n) {
              break;
            }
          }
          region_need = tile_need;
          if (region_need > have) {
            trace(160, have);
            return w;
          }
          break;
        }
        case 62: {  // LANES: [nd u8][nd x (base 8B][step 8B)][ischeme u8]
                    // [iprm u32][nexc u16][bitmap ceil(n/8)][inner region
                    //   of nexc exceptions; no inner bytes at nexc == 0]
          // kMod's envelope door check with the lane-plane extent: nd is
          // pinned to [2, 16] (nd == 1 is kArith's niche and nd > 16 cannot
          // win the header race), the footer param echoes nexc, and the
          // bitmap popcount must equal n - nexc exactly -- the mask kernel,
          // the closed-form SUM, and every exception rank trust the
          // on-model/clear split. The per-lane model needs no semantic
          // replay beyond the door check: values are COMPUTED from
          // (base, step), never read from tables.
          if (have < 40) {
            trace(163, have);
            return w;
          }
          const uint64_t nd = rd(off, 1);
          if (nd < 2 || nd > 16) {
            trace(163, nd);
            return w;
          }
          const uint64_t hdr = 8 + (nd * 16);
          const uint64_t bmp = (n + 7) / 8;
          if (hdr + bmp > have) {
            trace(163, have);
            return w;
          }
          const uint64_t ischeme = rd(off + hdr - 7, 1);
          const uint64_t iprm = rd(off + hdr - 6, 4);
          const uint64_t nexc = rd(off + hdr - 2, 2);
          // Footer param IS the exception count; nexc > n is impossible.
          if (nexc != prm || nexc > n) {
            trace(163, nexc);
            return w;
          }
          // Bitmap consistency: exactly n-nexc set bits (tail padding
          // masked), the same door check as case 58.
          uint64_t ones = 0;
          for (uint64_t i = 0; i < bmp; ++i) {
            uint64_t byte = rd8(off + hdr + i);
            if (i + 1 == bmp && (n & 7) != 0) {
              byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
            }
            ones += static_cast<uint64_t>(__builtin_popcountll(byte));
          }
          if (ones != n - nexc) {
            trace(163, ones);
            return w;
          }
          uint64_t ineed = 0;
          if (nexc > 0) {
            const uint64_t ioff = off + hdr + bmp;
            const uint64_t iavail = off + have - ioff;
            ineed = inner_extent(ioff, iavail, ischeme, iprm, nexc);
            if (ineed == UINT64_MAX || ineed > iavail) {
              trace(163, ineed);
              return w;
            }
          }
          region_need = hdr + bmp + ineed;
          break;
        }
        case 63: {  // POSTINGS: [d u16][d x 8B dict][d x u16 counts]
                    // [ceil(d/2) k-nibbles][d x u16 firsts]
                    // [d x u16 bitoffs][Rice bitstream]
          // FULL replay: the count table IS the exact histogram the
          // closed-form COUNT/SUM kernels trust, so the walk re-derives it
          // from the bitstream -- every list's Rice codes replayed, each row
          // id marked in a seen bitmap (a row in two lists, or in none, or
          // past n, fails), every dict entry pinned strictly ascending, the
          // stored bitoffs pinned to the replayed per-list bit positions,
          // and the consumed bits must land inside the region. A region
          // that passes this is entitled to answer every predicate from
          // its tables alone.
          if (n == 0 || have < 2) {
            trace(164, have);
            return w;
          }
          const uint64_t d = rd(off, 2);
          if (d == 0 || d > kPostingsMaxLists || d > n || prm != d) {
            trace(164, d);
            return w;
          }
          const uint64_t hdr = 2 + (d * 8) + (d * 2) + ((d + 1) / 2) + (d * 4);
          if (hdr > have) {
            trace(164, have);
            return w;
          }
          const uint64_t counts_base = off + 2 + (d * 8);
          const uint64_t ks_base = counts_base + (d * 2);
          const uint64_t firsts_base = ks_base + ((d + 1) / 2);
          const uint64_t bitoffs_base = firsts_base + (d * 2);
          const uint64_t stream_base = bitoffs_base + (d * 2);
          // Tables: counts sum to n, each >= 1, firsts in range, dict
          // strictly ascending, bitoffs[0] == 0 and non-decreasing.
          uint64_t total = 0;
          for (uint64_t c = 0; c < d; ++c) {
            const uint64_t cnt = rd(counts_base + (c * 2), 2);
            if (cnt == 0 || cnt > n) {
              trace(164, cnt);
              return w;
            }
            total += cnt;
            if (rd(firsts_base + (c * 2), 2) >= n) {
              trace(164, rd(firsts_base + (c * 2), 2));
              return w;
            }
            if (c > 0) {
              const auto prev =
                  static_cast<int64_t>(rd(off + 2 + ((c - 1) * 8), 8));
              const auto cur = static_cast<int64_t>(rd(off + 2 + (c * 8), 8));
              if (prev >= cur) {
                trace(164, static_cast<uint64_t>(cur));
                return w;
              }
              if (rd(bitoffs_base + (c * 2), 2) <
                  rd(bitoffs_base + ((c - 1) * 2), 2)) {
                trace(164, rd(bitoffs_base + (c * 2), 2));
                return w;
              }
            } else if (rd(bitoffs_base, 2) != 0) {
              trace(164, rd(bitoffs_base, 2));
              return w;
            }
          }
          if (total != n) {
            trace(164, total);
            return w;
          }
          // Full bitstream replay: the codes must reproduce the count table
          // exactly (row bijection over [0, n)) and stop inside the region.
          {
            std::array<uint64_t, 16> seen{};
            uint64_t bitpos = 0;
            uint64_t marked = 0;
            for (uint64_t c = 0; c < d; ++c) {
              const auto k = static_cast<uint32_t>(
                  c % 2 == 0 ? rd8(ks_base + (c / 2)) & 0x0F
                             : rd8(ks_base + (c / 2)) >> 4);
              if (rd(bitoffs_base + (c * 2), 2) != bitpos) {
                trace(164, bitpos);
                return w;
              }
              PostingsBitCursor cur;
              cur.Init(b.data() + stream_base + (bitpos / 8),
                       b.data() + off + have);
              uint64_t row = rd(firsts_base + (c * 2), 2);
              if (((seen[row >> 6] >> (row & 63)) & 1) != 0) {
                trace(164, row);
                return w;
              }
              seen[row >> 6] |= UINT64_C(1) << (row & 63);
              ++marked;
              const uint64_t cnt = rd(counts_base + (c * 2), 2);
              // A count-1 list carries no codes: its sub-stream is empty and
              // its recorded offset legitimately sits at the end-of-stream
              // position (the tail of a zipf head is exactly this shape), so
              // the in-range check only guards lists that will read codes.
              if (cnt > 1 && bitpos / 8 >= have - (stream_base - off)) {
                trace(164, bitpos);
                return w;
              }
              for (uint64_t t = 1; t < cnt; ++t) {
                uint64_t x = 0;
                if (!cur.Rice(k, &x)) {
                  trace(164, t);
                  return w;
                }
                const uint64_t gap = x + 1;
                if (gap > n || row + gap >= n) {
                  trace(164, gap);
                  return w;
                }
                row += gap;
                if (((seen[row >> 6] >> (row & 63)) & 1) != 0) {
                  trace(164, row);
                  return w;
                }
                seen[row >> 6] |= UINT64_C(1) << (row & 63);
                ++marked;
                bitpos += (x >> k) + 1 + k;
              }
              bitpos = (bitpos + 7) & ~UINT64_C(7);  // byte-aligned lists
            }
            if (marked != n) {
              trace(164, marked);
              return w;
            }
            region_need = (stream_base - off) + ((bitpos + 7) / 8);
            if (region_need > have) {
              trace(164, have);
              return w;
            }
          }
          break;
        }
        case 64: {  // TEMPLATE: [nseg u8][nexc u16][widths nseg u8]
                    // [(nseg+1) x (u16 len + bytes)][bmp ceil(n/8)]
                    // [nseg x (ischeme u8][iparam u32][vlen u32][inner vlen])]
                    // [nexc x (u32 len + bytes)]
          // Structural walk with the same arithmetic as
          // ParseTemplateRegion, re-derived byte-independently: literal
          // lengths bounded, the bitmap popcount pinned to n - nexc (the
          // contract that separates digit-plane rows from exception rows),
          // every digit plane's inner extent measured with the shared
          // inner_extent helper (a hostile footer cannot relocate segment
          // bytes), and the exception chain tiling the tail exactly.
          if (n == 0 || have < 3) {
            trace(165, have);
            return w;
          }
          const uint64_t nseg = rd8(off);
          const uint64_t nexc = rd(off + 1, 2);
          if (nseg < 1 || nseg > 8 || nexc != prm || nexc > n) {
            trace(165, nseg);
            return w;
          }
          uint64_t tpos = 3;
          if (tpos + nseg > have) {
            trace(165, have);
            return w;
          }
          for (uint64_t s = 0; s < nseg; ++s) {
            const uint64_t wdt = rd8(off + tpos + s);
            if (wdt < 1 || wdt > 18) {
              trace(165, wdt);
              return w;
            }
          }
          tpos += nseg;
          for (uint64_t k = 0; k <= nseg; ++k) {
            if (tpos + 2 > have) {
              trace(165, have);
              return w;
            }
            tpos += 2 + rd(off + tpos, 2);
            if (tpos > have) {
              trace(165, tpos);
              return w;
            }
          }
          const uint64_t bmp_bytes = (n + 7) / 8;
          if (tpos + bmp_bytes > have) {
            trace(165, have);
            return w;
          }
          // Bitmap consistency (tail padding masked, case 27's door check).
          {
            uint64_t ones = 0;
            for (uint64_t i = 0; i < bmp_bytes; ++i) {
              uint64_t byte = rd8(off + tpos + i);
              if (i + 1 == bmp_bytes && (n & 7) != 0) {
                byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
              }
              ones += static_cast<uint64_t>(__builtin_popcountll(byte));
            }
            if (ones != n - nexc) {
              trace(165, ones);
              return w;
            }
          }
          tpos += bmp_bytes;
          // Digit planes: nseg inner regions of (n - nexc) values each.
          const uint64_t nconf = n - nexc;
          for (uint64_t s = 0; s < nseg; ++s) {
            if (tpos + 9 > have) {
              trace(165, have);
              return w;
            }
            const uint64_t ischeme = rd8(off + tpos);
            const uint64_t iprm = rd(off + tpos + 1, 4);
            const uint64_t vlen = rd(off + tpos + 5, 4);
            const uint64_t ioff = off + tpos + 9;
            const uint64_t iavail = off + have - ioff;
            const uint64_t ineed =
                inner_extent(ioff, iavail, ischeme, iprm, nconf);
            if (ineed == UINT64_MAX || ineed != vlen || vlen > iavail) {
              trace(165, vlen);
              return w;
            }
            tpos = ioff + vlen - off;
          }
          // Exception chain: nexc plain strings must tile the tail exactly.
          for (uint64_t e = 0; e < nexc; ++e) {
            if (tpos + 4 > have) {
              trace(165, have);
              return w;
            }
            tpos += 4 + rd(off + tpos, 4);
            if (tpos > have) {
              trace(165, tpos);
              return w;
            }
          }
          region_need = tpos;
          if (region_need > have) {
            trace(165, have);
            return w;
          }
          break;
        }
        case 65: {  // DSTREAM: [nsym u8][symbols nsym x (u8 len + bytes)]
                    // [ndir u16][stream_len u32][restarts ndir x u32]
                    // [stream (n-nexc) x (u8 clen + clen bytes)]
                    // [bmp ceil(n/8)][exceptions nexc x (u32 len + bytes)]
          // Structural walk with the same arithmetic as ParseDStreamRegion,
          // re-derived byte-independently: the symbol table bounded, the
          // restart table pinned to the derived count with [0] == 0, every
          // record of every block replayed so each block ends exactly at
          // the next restart and the last exactly at stream_len (this is
          // what entitles the memcmp fold to trust clen and the restarts
          // without re-checking them per query), the bitmap popcount pinned
          // to n - nexc, and the exception chain tiling the tail exactly.
          // The exception count is NOT in the header -- it arrives as the
          // footer param (the kArith convention).
          if (n == 0 || have < 7 || prm > n) {
            trace(166, have);
            return w;
          }
          const uint64_t nexc = prm;
          const uint64_t nsym = rd8(off);
          if (nsym < 1 || nsym > 255) {
            trace(166, nsym);
            return w;
          }
          uint64_t tpos = 1;
          for (uint64_t c = 0; c < nsym; ++c) {
            if (tpos + 1 > have) {
              trace(166, have);
              return w;
            }
            const uint64_t slen = rd8(off + tpos);
            if (slen < 1 || slen > 8) {
              trace(166, slen);
              return w;
            }
            tpos += 1 + slen;
            if (tpos > have) {
              trace(166, tpos);
              return w;
            }
          }
          const uint64_t nconf = n - nexc;
          if (tpos + 6 > have) {
            trace(166, have);
            return w;
          }
          const uint64_t ndir = rd(off + tpos, 2);
          const uint64_t stream_len = rd(off + tpos + 2, 4);
          tpos += 6;
          if (ndir != (nconf + 15) / 16) {
            trace(166, ndir);
            return w;
          }
          if (tpos + (ndir * 4) > have) {
            trace(166, have);
            return w;
          }
          // The restart table lives AFTER the (ndir, stream_len) header.
          const uint64_t rdir = tpos;
          if (ndir > 0 && rd(off + rdir, 4) != 0) {
            trace(166, rd(off + rdir, 4));
            return w;
          }
          tpos += ndir * 4;
          const uint64_t stream_pos = tpos;
          tpos += stream_len;
          if (tpos > have) {
            trace(166, have);
            return w;
          }
          const uint64_t bmp_pos = tpos;
          const uint64_t bmp_bytes = (n + 7) / 8;
          if (tpos + bmp_bytes > have) {
            trace(166, have);
            return w;
          }
          // Bitmap consistency (tail padding masked, case 27's door check).
          {
            uint64_t ones = 0;
            for (uint64_t i = 0; i < bmp_bytes; ++i) {
              uint64_t byte = rd8(off + bmp_pos + i);
              if (i + 1 == bmp_bytes && (n & 7) != 0) {
                byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
              }
              ones += static_cast<uint64_t>(__builtin_popcountll(byte));
            }
            if (ones != nconf) {
              trace(166, ones);
              return w;
            }
          }
          tpos += bmp_bytes;
          // Record replay: walk every block's records from its restart
          // offset, pinning each block's end to the next restart and the
          // last block's end to stream_len.
          {
            uint64_t left = nconf;
            for (uint64_t blk = 0; blk < ndir; ++blk) {
              const uint64_t ro = rd(off + rdir + (blk * 4), 4);
              if (ro > stream_len) {
                trace(166, ro);
                return w;
              }
              uint64_t q = stream_pos + ro;
              const uint64_t rows = std::min<uint64_t>(16, left);
              for (uint64_t k = 0; k < rows; ++k) {
                if (q + 1 > stream_pos + stream_len) {
                  trace(166, q);
                  return w;
                }
                q += 1 + rd8(off + q);
                if (q > stream_pos + stream_len) {
                  trace(166, q);
                  return w;
                }
              }
              const uint64_t end_off = q - stream_pos;
              if (blk + 1 < ndir) {
                if (rd(off + rdir + ((blk + 1) * 4), 4) != end_off) {
                  trace(166, end_off);
                  return w;
                }
              } else if (end_off != stream_len) {
                trace(166, end_off);
                return w;
              }
              left -= rows;
            }
            if (left != 0) {
              trace(166, left);
              return w;
            }
          }
          // Exception chain: nexc plain strings must tile the tail exactly.
          for (uint64_t e = 0; e < nexc; ++e) {
            if (tpos + 4 > have) {
              trace(166, have);
              return w;
            }
            tpos += 4 + rd(off + tpos, 4);
            if (tpos > have) {
              trace(166, tpos);
              return w;
            }
          }
          region_need = tpos;
          if (region_need > have) {
            trace(166, have);
            return w;
          }
          break;
        }
        case 66: {  // FAST-PFOR: nblk x [min 8B][max 8B][sum 16B][w 1B][nexc
                    // u16]] + per-block [bmp][packed lanes][exc values]
          // nblk is DERIVED from n and pinned to the footer param; every
          // payload extent is derived from the metadata walk, so a region
          // that decodes is exactly as long as its own metadata says. The
          // replay then verifies the brackets the kernels trust: decoded
          // values (min + lane bits for regular rows, stored values for
          // exceptions) must reproduce min/max/sum exactly -- which is what
          // pins the encoder's regular-vs-exception classification (a row
          // whose true offset overflows 2^w but was not marked would decode
          // truncated and fail the metadata match).
          FastPforRegion parsed;
          if (!ParseFastPforRegion(b.data() + off, static_cast<uint32_t>(n),
                                   static_cast<uint32_t>(prm),
                                   b.data() + off + have, &parsed) ||
              !FastPforMetadataMatches(parsed)) {
            trace(167, have);
            return w;
          }
          region_need = parsed.encoded_size;
          break;
        }
        case 67: {  // DELTA-FASTPFOR: nblk x [base 8B][min 8B][max 8B]
                    // [sum 16B][w 1B][nexc u16][dmin 8B]] + per-block
                    // [bmp][packed delta lanes][exc deltas]
          // nblk is DERIVED from n and pinned to the footer param; every
          // payload extent is derived from the metadata walk, so a region
          // that decodes is exactly as long as its own metadata says. The
          // replay then verifies the brackets the kernels trust: replayed
          // values (base + dmin + lane, exception deltas patched) must
          // reproduce min/max/sum exactly -- which is what pins the
          // encoder's regular-vs-exception classification (a delta whose
          // true (d - dmin) overflows 2^w but was not marked would decode
          // truncated and fail the metadata match) and the base-inside-
          // brackets invariant the parse already enforces.
          DeltaFastPforRegion parsed;
          if (!ParseDeltaFastPforRegion(
                  b.data() + off, static_cast<uint32_t>(n),
                  static_cast<uint32_t>(prm), b.data() + off + have, &parsed) ||
              !DeltaFastPforMetadataMatches(parsed)) {
            trace(168, have);
            return w;
          }
          region_need = parsed.encoded_size;
          break;
        }
        case 58: {  // MOD: [a 8B][b 8B][m 8B][ischeme u8][iprm u32][nexc u16]
                    // [bitmap ceil(n/8)][inner region of nexc exceptions;
                    //   no inner bytes at nexc == 0]
          // kArith's arm with the modulus sanity the model arithmetic
          // needs: m in [2, 2^63] (one conditional subtract per step, u128
          // products under 2^74), a and b reduced residues, b != 0 (the
          // constant sequence is kConst's niche). Values are COMPUTED from
          // (a, b, m) rather than read from tables, so no semantic replay
          // is needed beyond the bitmap consistency every envelope scheme
          // pins.
          if (have < 31) {
            trace(161, have);
            return w;
          }
          const uint64_t ma = rd(off, 8);
          const uint64_t mb = rd(off + 8, 8);
          const uint64_t mm = rd(off + 16, 8);
          if (mm < 2 || mm > (UINT64_C(1) << 63) || ma >= mm || mb >= mm ||
              mb == 0) {
            trace(161, mm);
            return w;
          }
          const uint64_t ischeme = rd(off + 24, 1);
          const uint64_t iprm = rd(off + 25, 4);
          const uint64_t nexc = rd(off + 29, 2);
          // Footer param IS the exception count; nexc > n is impossible.
          if (nexc != prm || nexc > n) {
            trace(161, nexc);
            return w;
          }
          const uint64_t bmp = (n + 7) / 8;
          if (31 + bmp > have) {
            trace(161, have);
            return w;
          }
          // Bitmap consistency: exactly n-nexc set bits (tail padding
          // masked), the same door check as case 27.
          uint64_t ones = 0;
          for (uint64_t i = 0; i < bmp; ++i) {
            uint64_t byte = rd8(off + 31 + i);
            if (i + 1 == bmp && (n & 7) != 0) {
              byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
            }
            ones += static_cast<uint64_t>(__builtin_popcountll(byte));
          }
          if (ones != n - nexc) {
            trace(161, ones);
            return w;
          }
          uint64_t ineed = 0;
          if (nexc > 0) {
            const uint64_t ioff = off + 31 + bmp;
            const uint64_t iavail = off + have - ioff;
            ineed = inner_extent(ioff, iavail, ischeme, iprm, nexc);
            if (ineed == UINT64_MAX || ineed > iavail) {
              trace(161, ineed);
              return w;
            }
          }
          region_need = 31 + bmp + ineed;
          break;
        }
        case 5: {  // DELTA-FP: [first 8B][cks checkpoints][packed deltas]
          if (prm == 0 || prm > 64) {
            trace(84, prm);
            return w;
          }
          const uint64_t cks =
              (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery;
          region_need = 8 + (8 * cks) + (((n > 0 ? (n - 1) * prm : 0) + 7) / 8);
          break;
        }
        case 6:  // BITMAP: [tv 8B][fv 8B][ceil(n/8) bitmap]
          region_need = 16 + ((n + 7) / 8);
          break;
        case 7:  // XOR-FOR: [first 8B][packed xors]
          if (prm == 0 || prm > 64) {
            trace(84, prm);
            return w;
          }
          region_need = 8 + (((n > 0 ? (n - 1) * prm : 0) + 7) / 8);
          break;
        case 8: {  // BitWeave: [base 8B][w u8][packed offsets]
          if (prm == 0 || prm > 64) {
            trace(86, prm);
            return w;
          }
          if (have < 9) {
            trace(86, have);
            return w;
          }
          // Cross-check the region's width byte against the footer param:
          // the decoder walks n*header_w bits, so a header/param mismatch
          // (hostile or stale region) walks past the region end.
          if (rd(off + 8, 1) != prm) {
            trace(86, rd(off + 8, 1));
            return w;
          }
          region_need = 9 + ((n * prm + 7) / 8);
          break;
        }
        case 9: {  // Elias-Fano: [base][maxd][l][high bitmap][low bits]
          if (prm > 64) {
            trace(87, prm);
            return w;
          }
          if (have < 17) {
            trace(87, have);
            return w;
          }
          if (rd(off + 16, 1) != prm) {
            trace(87, rd(off + 16, 1));
            return w;
          }
          const uint64_t maxd = rd(off + 8, 8);
          const uint64_t high_max = (prm >= 64) ? 0 : (maxd >> prm);
          // hlen = n + high_max must not wrap -- and neither may the
          // byte-ceil (hlen + 7) / 8: the boundary high_max == UINT64_MAX - n
          // made hlen == UINT64_MAX, whose +7 wrapped the division to 0 and
          // shrank region_need below the bitmap's true size. Reserve 7 bits
          // of headroom (the decoders reject the same header).
          if (high_max > UINT64_MAX - n - 7) {
            trace(87, high_max);
            return w;
          }
          const uint64_t hlen = n + high_max;
          region_need = 17 + ((hlen + 7) / 8) + ((n * prm + 7) / 8);
          break;
        }
        case 10: {  // Wavelet: [min 8B][W u8][W bitmaps of ceil(n/8)]
          if (prm == 0 || prm > 1024) {
            trace(88, prm);
            return w;
          }
          if (have < 9) {
            trace(88, have);
            return w;
          }
          const uint64_t W = rd(off + 8, 1);
          if (W == 0 || W > 64) {
            trace(88, W);
            return w;
          }
          region_need = 9 + (W * ((n + 7) / 8));
          break;
        }
        case 11:  // Delta-For: [ncp u16][checkpoints][FOR-BA inner]
          if (prm == 0 || prm > 8) {
            trace(89, prm);
            return w;
          }
          if (have < 2) {
            trace(89, have);
            return w;
          }
          // Pin the checkpoint count to the builder's formula
          // (EncDeltaCompositeI64: ceil(n/128)+1). Without the pin a region
          // could declare ncp=0 (or fewer checkpoints than the highest block
          // index), and the decoders would read `base` out of the inner
          // payload for idx >= 128.
          if (rd(off, 2) !=
              ((n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) + 1) {
            trace(89, rd(off, 2));
            return w;
          }
          region_need =
              2 + (8 * rd(off, 2)) + 8 + (n * prm);  // inner = [base 8B][n*bpv]
          break;
        case 12:  // Delta-BitWeave: [ncp u16][checkpoints][BitWeave inner]
          if (prm == 0 || prm > 64) {
            trace(90, prm);
            return w;
          }
          if (have < 2) {
            trace(90, have);
            return w;
          }
          // Same checkpoint-count pin as case 11.
          if (rd(off, 2) !=
              ((n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) + 1) {
            trace(90, rd(off, 2));
            return w;
          }
          // Cross-check the inner BitWeave width byte against the footer
          // param (the decoder walks n*header_w bits driven by the header,
          // so a mismatch over-reads the region; cf. cases 8/9/13).
          {
            const uint64_t ncp = rd(off, 2);
            const uint64_t inner_off = off + 2 + (ncp * 8) + 8;
            if (inner_off >= off + have || rd(inner_off, 1) != prm) {
              trace(90, inner_off);
              return w;
            }
          }
          region_need = 2 + (8 * rd(off, 2)) + 9 + ((n * prm + 7) / 8);
          break;
        case 54: {  // DELTA-SIMD: [ncp u16][ckpt ncp x 8B BE]
                    //         [inner: a complete kSimdFor region over the n
                    //         deltas]
          // Same checkpoint-count pin as cases 11/12, so a hand-built footer
          // cannot under-count the seek table. The inner width comes from the
          // footer param ALONE (kSimdFor stores no width byte in its region,
          // unlike BitWeave's header_w), so there is nothing to cross-check
          // against -- the param is validated as a width the four-lane layout
          // can express, and the extent is then derived from (n, param) with
          // no count field to under-state.
          if (prm == 0 || prm > 64 ||
              !SimdForWidthOk(static_cast<uint32_t>(prm))) {
            trace(158, prm);
            return w;
          }
          if (have < 2) {
            trace(158, have);
            return w;
          }
          if (rd(off, 2) !=
              ((n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) + 1) {
            trace(158, rd(off, 2));
            return w;
          }
          region_need = 2 + (8 * rd(off, 2)) +
                        SimdForExtent(static_cast<uint32_t>(n),
                                      static_cast<uint32_t>(prm));
          break;
        }
        // NOTE: the guard below MUST stay on its OWN line, never merged into
        // this comment. It was once written on the `case` line, where the `//`
        // swallowed its `{` and commented the whole `if` out -- so the arm ran
        // `trace; return w;` unconditionally and the validator rejected EVERY
        // kByteSlice cell, including ones the encoder had just written in the
        // same process. It hid because the adaptive selector rarely chose
        // kByteSlice, so the only ways in were a shape that made it win the
        // cost race, or an explicit TINYLAMB_FORCE_SCHEME=BYTESLICE.
        case 13: {  // ByteSlice: [w u8][w slices of ceil(n/8)]
          if (prm == 0 || prm > 64) {
            trace(91, prm);
            return w;
          }
          if (have < 1) {
            trace(91, have);
            return w;
          }
          // Cross-check the region's slice-width byte against the footer
          // param (decoder reads w slices driven by the header byte).
          if (rd(off, 1) != prm) {
            trace(91, rd(off, 1));
            return w;
          }
          region_need = 1 + (prm * ((n + 7) / 8));
          break;
        }
        case 14:  // Gorilla: [first 8B][XOR bitstream]. The stream is
                  // self-delimiting: the decoder consumes EXACTLY the bits
                  // the encoder wrote per value, so the real region size is
                  // unknowable without decoding. A worst-case bound would
                  // reject legitimate chunks (streams compress hard), and
                  // byte flips inside the region are caught by crc_data
                  // before any decoder sees them. Gate the fixed header
                  // only.
          region_need = 8;
          break;
        case 15: {  // FSST: [nsym u16][tsl u32][symbols][n offsets][stream]
          if (prm == 0 || prm > 254) {
            trace(93, prm);
            return w;
          }
          if (have < 2) {
            trace(93, have);
            return w;
          }
          // Cross-check the region's symbol count against the footer param
          // (the encoder emits param == nsym). Without this a mutated
          // region could declare nsym up to 65535 while readers size their
          // symbol tables for prm <= 254 (WASM decoders stack-allocate
          // 256-entry tables). Must stay in lockstep with the WASM
          // parse_footer case 15 gate.
          if (rd(off, 2) != prm) {
            trace(93, rd(off, 2));
            return w;
          }
          // The symbol table is variable-length, so walk it (bounded by
          // `have`) to pin the offset-table + stream extent. The former
          // header+offset-table-only bound passed regions whose symbol or
          // stream bytes ran past the cell; every reader bailed at runtime,
          // but the validator verdict claimed the region sound.
          uint64_t sp = 6;  // past [nsym u16][tsl u32]
          for (uint64_t c = 0; c < prm; ++c) {
            if (sp + 1 > have) {
              trace(93, have);
              return w;
            }
            sp += 1 + rd(off + static_cast<size_t>(sp), 1);
            if (sp > have) {
              trace(93, have);
              return w;
            }
          }
          const uint64_t tsl = rd(off + 2, 4);
          region_need = sp + (static_cast<uint64_t>(n) * 4) + tsl;
          break;
        }
        case 16: {  // PFOR: [base 8B][w u8][nexc u16][packed][pos+val x nexc]
          if (prm == 0 || prm > 64) {
            trace(96, prm);
            return w;
          }
          if (have < 11) {
            trace(96, have);
            return w;
          }
          if (rd(off + 8, 1) != prm) {
            trace(96, rd(off + 8, 1));
            return w;
          }
          {
            const uint64_t nexc = rd(off + 9, 2);
            if (nexc > n) {
              trace(96, nexc);
              return w;
            }
            region_need = 11 + ((n * prm + 7) / 8) + (nexc * 10);
            // Exception positions must be in-range and sorted. Gate the
            // walk extent BEFORE it runs: the loop reads 10*nexc bytes at
            // off+11+pack_bytes, and `region_need > have` is only compared
            // after the switch -- a hostile nexc in an 11-byte region at
            // the data end read ~18KB past the buffer. Lockstep with the
            // WASM parse_footer case 16 gate.
            const uint64_t pack_bytes = (n * prm + 7) / 8;
            if (11 + pack_bytes + (nexc * 10) > have) {
              trace(96, have);
              return w;
            }
            uint64_t prev = 0;
            for (uint64_t k = 0; k < nexc; ++k) {
              const uint64_t epos = rd(off + 11 + pack_bytes + (k * 10), 2);
              if (epos >= n || (k > 0 && epos <= prev)) {
                trace(96, epos);
                return w;
              }
              prev = epos;
            }
          }
          break;
        }
        case 17: {  // DdRice: [ncp u16][ncp x(val,d1)][ncp x u32 offs][k u8]
          if (prm > 64) {
            trace(97, prm);
            return w;
          }
          if (have < 3) {
            trace(97, have);
            return w;
          }
          // Checkpoint count is a pure function of n (C = 128 deltas).
          const uint64_t ncp_want = (n == 0) ? 0 : ((n - 1) / 128) + 1;
          const uint64_t ncp = rd(off, 2);
          if (ncp != ncp_want || ncp == 0 || ncp > 1024) {
            trace(97, ncp);
            return w;
          }
          const uint64_t k_off = off + 2 + (ncp * 20);
          if (k_off >= off + have || rd(k_off, 1) != prm) {
            trace(97, k_off);
            return w;
          }
          region_need = 2 + (ncp * 20) + 1;
          break;
        }
        case 18:  // Chimp: [first 8B][bitstream], self-delimiting like Gorilla
          region_need = 8;
          break;
        case 19: {  // DeltaVarint: [ncp u16][ncp x 8B ck][ncp x u32 offs][LEB]
          if (prm != 0) {
            trace(98, prm);
            return w;
          }
          if (have < 2) {
            trace(98, have);
            return w;
          }
          const uint64_t ncp_want = (n == 0) ? 0 : ((n - 1) / 128) + 1;
          const uint64_t ncp = rd(off, 2);
          if (ncp != ncp_want || ncp == 0 || ncp > 1024) {
            trace(98, ncp);
            return w;
          }
          region_need = 2 + (ncp * 12);
          break;
        }
        case 20: {  // ALP: [exp u8][backend u8][nexc u16][exc 10B x][inner]
          if (have < 4) {
            trace(99, have);
            return w;
          }
          const uint64_t expb = rd(off, 1);
          if (expb > 18) {
            trace(99, expb);
            return w;
          }
          const uint64_t backend = rd(off + 1, 1);
          if (backend != 1 && backend != 2 && backend != 4 && backend != 8) {
            trace(99, backend);
            return w;
          }
          const uint64_t nexc = rd(off + 2, 2);
          if (nexc > n) {
            trace(99, nexc);
            return w;
          }
          // Gate the exception walk BEFORE it runs (cf. case 16): the loop
          // below reads 10*nexc bytes at off+4, and region_need is only
          // compared against `have` after the switch.
          if (4 + (nexc * 10) > have) {
            trace(99, have);
            return w;
          }
          uint64_t prev_epos = 0;
          for (uint64_t k = 0; k < nexc; ++k) {
            const uint64_t epos = rd(off + 4 + (k * 10), 2);
            if (epos >= n || (k > 0 && epos <= prev_epos)) {
              trace(99, epos);
              return w;
            }
            prev_epos = epos;
          }
          // Inner region header walk per backend (the inner headers are
          // authoritative; the footer param is unused for scheme 20).
          const uint64_t inner_off = off + 4 + (nexc * 10);
          uint64_t inner_need = 0;
          if (backend == 8) {  // BitWeave: [base 8B][w u8][packed]
            if (inner_off + 9 > off + have) {
              trace(99, have);
              return w;
            }
            const uint64_t iw = rd(inner_off + 8, 1);
            if (iw == 0 || iw > 64) {
              trace(99, iw);
              return w;
            }
            inner_need = 9 + ((n * iw + 7) / 8);
          } else if (backend == 4) {  // DICT: [ncodes u16][codes][dict 8B/e]
            if (inner_off + 2 > off + have) {
              trace(99, have);
              return w;
            }
            const uint64_t ncodes = rd(inner_off, 2);
            if (ncodes == 0) {
              trace(99, ncodes);
              return w;
            }
            const int cw = BitsNeeded((ncodes > 0 ? ncodes - 1 : 0));
            inner_need = 2 + ((n * static_cast<uint64_t>(cw) + 7) / 8) +
                         (8 * ncodes);  // int64 mantissa dict: 8B/entry
          } else if (backend == 2) {    // RLE: [runs u32][12B x runs]
            if (inner_off + 4 > off + have) {
              trace(99, have);
              return w;
            }
            const uint64_t runs = rd(inner_off, 4);
            if (runs == 0 || runs > n) {
              trace(99, runs);
              return w;
            }
            inner_need = 4 + (12 * runs);
          } else {  // CONST: [8B]
            inner_need = 8;
          }
          region_need = 4 + (nexc * 10) + inner_need;
          break;
        }
        case 21: {  // FrontCode: [nrestart u16][stream_len u32]
                    // [restart u32 x nrestart][entry stream]
          const uint64_t re = prm & 0x7FFFU;
          if (re == 0 || re > 1024) {
            trace(100, prm);
            return w;
          }
          if (have < 6) {
            trace(100, have);
            return w;
          }
          // Restart count is pinned to the builder formula ceil(n / re):
          // a short table would leave the later blocks' decoders reading
          // their start offset out of the entry stream.
          const uint64_t nrestart = rd(off, 2);
          if (nrestart != (n + re - 1) / re) {
            trace(100, nrestart);
            return w;
          }
          const uint64_t stream_len = rd(off + 2, 4);
          region_need = 6 + (nrestart * 4) + stream_len;
          // Gate the restart-table walk BEFORE region_need is compared
          // against `have` (cf. cases 16/20): the loop below reads
          // 4*nrestart bytes at off+6. Lockstep with the WASM parse_footer
          // case 21 gate.
          if (6 + (nrestart * 4) > have) {
            trace(100, have);
            return w;
          }
          uint64_t prev_ro = 0;
          for (uint64_t k = 0; k < nrestart; ++k) {
            const uint64_t ro = rd(off + 6 + (k * 4), 4);
            // restart[0] leads the first chain (plen must be 0 there, which
            // the clamped decoders enforce), offsets are non-decreasing and
            // stay inside the declared stream.
            if (ro > stream_len || (k == 0 && ro != 0) ||
                (k > 0 && ro < prev_ro)) {
              trace(100, ro);
              return w;
            }
            prev_ro = ro;
          }
          break;
        }
        case 22: {  // OP-DICT: kDict's byte layout with a sorted dictionary.
                    // Sortedness itself is not structural (a hostile cell
                    // with an unsorted dict decodes losslessly but reports
                    // wrong windows), so the gates mirror case 4 exactly.
          if (prm == 0 || prm > 65535) {
            trace(101, prm);
            return w;
          }
          if (have >= 2 && rd(off, 2) != prm) {
            trace(101, rd(off, 2));
            return w;
          }
          const int cw = BitsNeeded(prm - 1);
          // int64 dicts pay 8B/entry, varchar dicts 4B+len; floor at 4B.
          region_need =
              2 + ((n * static_cast<uint64_t>(cw) + 7) / 8) + (4 * prm);
          break;
        }
        case 23: {  // RLE-DICT: [d u16][dict d x 8B][runs u32]
                    // [{code cb B}{len u32}] x runs, cb = ceil(cw/8).
          // param IS the run count (mirrors case 2's run-count gate).
          if (prm == 0 || prm > n) {
            trace(102, prm);
            return w;
          }
          if (have < 2 + 8 + 4) {
            trace(102, have);
            return w;
          }
          const uint64_t d = rd(off, 2);
          if (d == 0) {
            trace(102, d);
            return w;
          }
          const auto cb =
              static_cast<uint64_t>((BitsNeeded(d - 1) + 7) / 8);  // 1 or 2
          // Cross-check the region's run count against the footer param
          // (the run walk is bounded by it; a mismatch over-reads).
          const uint64_t runs_off = off + 2 + (d * 8);
          if (runs_off + 4 > off + have || rd(runs_off, 4) != prm) {
            trace(102, runs_off);
            return w;
          }
          region_need = 2 + (d * 8) + 4 + (prm * (cb + 4));
          break;
        }
        case 24: {  // FREQ: [top 8B][ischeme u8][iparam u32][nexc u16]
                    // [bitmap ceil(n/8)][inner region of nexc exceptions]
          if (have < 15) {
            trace(103, have);
            return w;
          }
          const uint64_t ischeme = rd(off + 8, 1);
          const uint64_t iprm = rd(off + 9, 4);
          const uint64_t nexc = rd(off + 13, 2);
          // Footer param IS the exception count; nexc==0 degenerates to
          // CONST (never emitted) and nexc > n is impossible.
          if (nexc != prm || nexc == 0 || nexc > n) {
            trace(103, nexc);
            return w;
          }
          const uint64_t bmp = (n + 7) / 8;
          if (15 + bmp > have) {
            trace(103, have);
            return w;
          }
          // Bitmap consistency: exactly n-nexc set bits (tail padding
          // masked). A hostile bitmap would decode rows as 0/phantom
          // exceptions -- safe but wrong; reject at the door instead.
          uint64_t ones = 0;
          for (uint64_t i = 0; i < bmp; ++i) {
            uint64_t byte = rd8(off + 15 + i);
            if (i + 1 == bmp && (n & 7) != 0) {
              byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
            }
            ones += static_cast<uint64_t>(__builtin_popcountll(byte));
          }
          if (ones != n - nexc) {
            trace(103, ones);
            return w;
          }
          const uint64_t ioff = off + 15 + bmp;
          const uint64_t iavail = off + have - ioff;
          const uint64_t ineed =
              inner_extent(ioff, iavail, ischeme, iprm, nexc);
          if (ineed == UINT64_MAX || ineed > iavail) {
            trace(103, ineed);
            return w;
          }
          region_need = 15 + bmp + ineed;
          break;
        }
        case 25: {  // RLE-X: [vs u8][cs u8][runs u32][vparam u32]
                    // [cparam u32][vlen u32][values region][counts region]
          if (prm == 0 || prm > n) {
            trace(104, prm);
            return w;
          }
          if (have < 18) {
            trace(104, have);
            return w;
          }
          const uint64_t vs = rd(off, 1);
          const uint64_t cs = rd(off + 1, 1);
          if (rd(off + 2, 4) != prm) {  // header echoes the footer run count
            trace(104, rd(off + 2, 4));
            return w;
          }
          // vw/cw are the header's vparam/cparam (values- vs counts-region
          // inner-scheme params; the struct comment above names them).
          // Deliberately NOT named `*prm`/`*_param`: that shape reads as
          // swappable with the outer `prm` passed as `cnt`
          // (readability-suspicious-call-argument).
          const uint64_t vw = rd(off + 6, 4);
          const uint64_t cw = rd(off + 10, 4);
          const uint64_t vlen = rd(off + 14, 4);
          if (vlen > have - 18) {
            trace(104, vlen);
            return w;
          }
          const uint64_t vneed = inner_extent(off + 18, vlen, vs, vw, prm);
          if (vneed == UINT64_MAX || vneed > vlen) {
            trace(104, vneed);
            return w;
          }
          const uint64_t coff = off + 18 + vlen;
          const uint64_t cneed =
              inner_extent(coff, off + have - coff, cs, cw, prm);
          if (cneed == UINT64_MAX || cneed > off + have - coff) {
            trace(104, cneed);
            return w;
          }
          region_need = 18 + vneed + cneed;
          break;
        }
        case 26: {  // DICT-X: [d u16][cs u8][cprm u32][clen u32]
                    // [codes region: n entries][pool_scheme u8][pool]
          if (prm == 0 || prm > 65535) {
            trace(105, prm);
            return w;
          }
          if (have < 11) {
            trace(105, have);
            return w;
          }
          if (rd(off, 2) != prm) {  // header echoes the footer dict size
            trace(105, rd(off, 2));
            return w;
          }
          const uint64_t cs = rd(off + 2, 1);
          const uint64_t cprm = rd(off + 3, 4);
          const uint64_t clen = rd(off + 7, 4);
          if (clen > have - 11) {
            trace(105, clen);
            return w;
          }
          const uint64_t cneed = inner_extent(off + 11, clen, cs, cprm, n);
          if (cneed == UINT64_MAX || cneed > clen) {
            trace(105, cneed);
            return w;
          }
          const uint64_t poff = off + 11 + clen;
          if (poff + 1 > off + have) {
            trace(105, poff);
            return w;
          }
          const uint64_t ps = rd(poff, 1);
          uint64_t pool_need = 0;
          if (ps == 0) {
            // int64 dicts pay 8B/entry, varchar 4B+len: floor at 4B (same
            // convention as cases 4/22).
            pool_need = 4 * prm;
          } else if (ps == 15) {
            // FSST pool of `prm` strings: same structural walk as case 15
            // (symbols then [d x u32 offsets][tsl stream]), but nsym comes
            // from the pool header, not the footer.
            if (poff + 1 + 6 > off + have) {
              trace(105, poff);
              return w;
            }
            const uint64_t nsym = rd(poff + 1, 2);
            if (nsym > 256) {  // decoder stack table is 256 entries
              trace(105, nsym);
              return w;
            }
            // sp is an offset into the pool region INCLUDING the ps byte
            // (7 = 1 ps + [nsym u16][tsl u32]); bounded by the cell end.
            const uint64_t pool_have = off + have - poff;
            uint64_t sp = 7;
            for (uint64_t c = 0; c < nsym; ++c) {
              if (sp + 1 > pool_have) {
                trace(105, pool_have);
                return w;
              }
              sp += 1 + rd(poff + sp, 1);
              if (sp > pool_have) {
                trace(105, pool_have);
                return w;
              }
            }
            const uint64_t tsl = rd(poff + 3, 4);
            pool_need = sp + (prm * 4) + tsl;
          } else {
            trace(105, ps);
            return w;
          }
          region_need = 11 + clen + pool_need;
          break;
        }
        case 27: {  // ARITH: [a 8B][d 8B][ischeme u8][iprm u32][nexc u16]
                    // [bitmap ceil(n/8)][inner region of nexc exceptions;
                    //   no inner bytes at nexc == 0]
          if (have < 23) {
            trace(106, have);
            return w;
          }
          if (rd(off + 8, 8) == 0) {  // d == 0 is never emitted
            trace(106, 0);
            return w;
          }
          const uint64_t ischeme = rd(off + 16, 1);
          const uint64_t iprm = rd(off + 17, 4);
          const uint64_t nexc = rd(off + 21, 2);
          // Footer param IS the exception count; nexc > n is impossible.
          if (nexc != prm || nexc > n) {
            trace(106, nexc);
            return w;
          }
          const uint64_t bmp = (n + 7) / 8;
          if (23 + bmp > have) {
            trace(106, have);
            return w;
          }
          // Bitmap consistency: exactly n-nexc set bits (tail padding
          // masked), the same door check as case 24.
          uint64_t ones = 0;
          for (uint64_t i = 0; i < bmp; ++i) {
            uint64_t byte = rd8(off + 23 + i);
            if (i + 1 == bmp && (n & 7) != 0) {
              byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
            }
            ones += static_cast<uint64_t>(__builtin_popcountll(byte));
          }
          if (ones != n - nexc) {
            trace(106, ones);
            return w;
          }
          uint64_t ineed = 0;
          if (nexc > 0) {
            const uint64_t ioff = off + 23 + bmp;
            const uint64_t iavail = off + have - ioff;
            ineed = inner_extent(ioff, iavail, ischeme, iprm, nexc);
            if (ineed == UINT64_MAX || ineed > iavail) {
              trace(106, ineed);
              return w;
            }
          }
          region_need = 23 + bmp + ineed;
          break;
        }
        case 28: {  // SPARSE: [vs u8][vprm u32][vlen u32][pos_bpv u8]
                    // [nnz u32][FOR-BA positions: base 8B + nnz x pos_bpv]
                    // [values region of nnz entries]
          if (have < 14) {
            trace(106, have);
            return w;
          }
          const uint64_t vs = rd(off, 1);
          const uint64_t vprm = rd(off + 1, 4);
          const uint64_t vlen = rd(off + 5, 4);
          const uint64_t pbpv = rd(off + 9, 1);
          const uint64_t nnz = rd(off + 10, 4);
          // Footer param IS the non-null count (echoed in the header).
          if (nnz != prm || prm == 0 || prm > n || pbpv < 1 || pbpv > 8) {
            trace(106, prm);
            return w;
          }
          const uint64_t pos_need = 8 + (prm * pbpv);
          if (14 + pos_need > have) {
            trace(106, have);
            return w;
          }
          // Positions must be strictly increasing in-range row indices:
          // the binary-search decode relies on it (and a hostile duplicate
          // would alias two ranks to one row).
          const uint64_t pbase = rd(off + 14, 8);
          uint64_t prev = 0;
          for (uint64_t i = 0; i < prm; ++i) {
            uint64_t offv = 0;
            for (uint64_t pb = 0; pb < pbpv; ++pb) {
              offv |= rd8(off + 22 + (i * pbpv) + pb) << (pb * 8);
            }
            const uint64_t p = pbase + offv;  // wrapping u64, like FOR-BA
            if (p >= n || (i > 0 && p <= prev)) {
              trace(106, p);
              return w;
            }
            prev = p;
          }
          const uint64_t voff = off + 14 + pos_need;
          if (vlen > off + have - voff) {
            trace(106, vlen);
            return w;
          }
          const uint64_t iprm = vprm;
          const uint64_t cnt = prm;
          const uint64_t vneed =
              inner_extent(voff, off + have - voff, vs, iprm, cnt);
          if (vneed == UINT64_MAX || vneed > vlen) {
            trace(106, vneed);
            return w;
          }
          region_need = 14 + pos_need + vlen;
          break;
        }
        case 29: {  // ANS-FOR: [base 8B][bpv u8][prec u8][nsym u16]
                    // [nsym x (sym u8, freq u16)][ncp u16]
                    // [ncp x (state u32, pos u32)][slen u32][stream]
          if (have < 12) {
            trace(107, have);
            return w;
          }
          const uint64_t bpv = rd(off + 8, 1);
          const uint64_t prec = rd(off + 9, 1);
          const uint64_t nsym = rd(off + 10, 2);
          // Footer param IS the byte width (like case 1's FOR-BA bpv);
          // prec outside [8,15] would let M exceed the u32 state math.
          if (bpv != prm || bpv < 1 || bpv > 8 || prec < 8 || prec > 15 ||
              nsym == 0 || nsym > 256) {
            trace(107, prm);
            return w;
          }
          const uint64_t hdr = 12 + (nsym * 3) + 2;
          if (hdr > have) {
            trace(107, have);
            return w;
          }
          // Strictly ascending unique symbols, nonzero shares summing to
          // exactly 1<<prec -- a short sum leaves dead slots that decode
          // to a phantom symbol.
          uint64_t acc = 0;
          uint64_t prev = 0;
          for (uint64_t i = 0; i < nsym; ++i) {
            const uint64_t sym = rd(off + 12 + (i * 3), 1);
            const uint64_t fr = rd(off + 12 + (i * 3) + 1, 2);
            if (fr == 0 || (i > 0 && sym <= prev)) {
              trace(107, sym);
              return w;
            }
            prev = sym;
            acc += fr;
          }
          if (acc != (uint64_t{1} << prec)) {
            trace(107, acc);
            return w;
          }
          const uint64_t ncp = rd(off + 12 + (nsym * 3), 2);
          if (ncp != (n + 63) / 64) {
            trace(107, ncp);
            return w;
          }
          const uint64_t ckoff = off + 12 + (nsym * 3) + 2;
          const uint64_t slen_off = ckoff + (ncp * 8);
          if (slen_off + 4 > off + have) {
            trace(107, have);
            return w;
          }
          const uint64_t slen = rd(slen_off, 4);
          // States in [L, 256*L); cursors within the stream and
          // non-increasing, ckpt[0] anchored at stream end (row 0 decodes
          // from the last pushed byte backwards).
          uint64_t prev_pos = slen;
          for (uint64_t k = 0; k < ncp; ++k) {
            const uint64_t st = rd(ckoff + (k * 8), 4);
            const uint64_t ps = rd(ckoff + (k * 8) + 4, 4);
            if (st < (uint64_t{1} << 23) || st >= (uint64_t{1} << 31) ||
                ps > slen || (k == 0 ? ps != slen : ps > prev_pos)) {
              trace(107, st);
              return w;
            }
            prev_pos = ps;
          }
          region_need = hdr + (ncp * 8) + 4 + slen;
          break;
        }
        case 30: {  // OP-RLE: kRleDict's byte layout ([d u16][dict d x 8B]
                    // [runs u32][{code cb}{len u32}] x runs) with a sorted
                    // dictionary. Sortedness itself is not structural (a
                    // hostile cell with an unsorted dict decodes
                    // losslessly but reports wrong code windows), so the
                    // gates mirror case 23 exactly.
          if (prm == 0 || prm > n) {
            trace(108, prm);
            return w;
          }
          if (have < 2 + 8 + 4) {
            trace(108, have);
            return w;
          }
          const uint64_t d = rd(off, 2);
          if (d == 0) {
            trace(108, d);
            return w;
          }
          const auto cb =
              static_cast<uint64_t>((BitsNeeded(d - 1) + 7) / 8);  // 1 or 2
          const uint64_t runs_off = off + 2 + (d * 8);
          if (runs_off + 4 > off + have || rd(runs_off, 4) != prm) {
            trace(108, runs_off);
            return w;
          }
          region_need = 2 + (d * 8) + 4 + (prm * (cb + 4));
          break;
        }
        case 31: {  // XOR-FOR-C: [ncp u16][ckpts ncp x 8B][w u8][packed]
          if (prm == 0 || prm > 64) {
            trace(109, prm);
            return w;
          }
          if (have < 2) {
            trace(109, have);
            return w;
          }
          // Pin the checkpoint count to the builder's formula
          // (EncXorForCF64: ceil(n/128); cf. case 11's pin).
          if (rd(off, 2) !=
              (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) {
            trace(109, rd(off, 2));
            return w;
          }
          const uint64_t ncp = rd(off, 2);
          // Cross-check the region's width byte against the footer param:
          // the decoders walk (n-1)*prm bits from the packed area, so a
          // header/param disagreement over-reads the region (cf. case 8).
          if (2 + (ncp * 8) + 1 > have || rd(off + 2 + (ncp * 8), 1) != prm) {
            trace(109, ncp);
            return w;
          }
          region_need =
              2 + (ncp * 8) + 1 + (((n > 0 ? (n - 1) * prm : 0) + 7) / 8);
          break;
        }
        case 32:    // GORILLA-C: [ncp u16][ckpts ncp x 8B]
        case 33: {  // CHIMP-C:    [bitoffs ncp x 4B][first 8B][stream]
          if (prm != 0) {
            trace(110, prm);
            return w;
          }
          if (have < 2) {
            trace(110, have);
            return w;
          }
          if (rd(off, 2) !=
              (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) {
            trace(110, rd(off, 2));
            return w;
          }
          const uint64_t ncp = rd(off, 2);
          const uint64_t tbl = 2 + (ncp * 12) + 8;
          if (tbl > have) {
            trace(110, have);
            return w;
          }
          // Every resume bit offset must sit inside the bitstream and be
          // non-decreasing; bitoffs[0] is fixed at 0 by the builder.
          {
            const uint64_t offs_off = off + 2 + (ncp * 8);
            const uint64_t stream_bits = (have - tbl) * 8;
            uint64_t prev_bit = 0;
            for (uint64_t bi = 0; bi < ncp; ++bi) {
              const uint64_t ob = rd(offs_off + (bi * 4), 4);
              // ob == stream_bits is legitimate for a final block holding
              // only its checkpoint row (the offset is never consumed); the
              // bounded BitReader fails closed on any real over-seek.
              if (ob < prev_bit || (bi == 0 && ob != 0) || ob > stream_bits) {
                trace(110, ob);
                return w;
              }
              prev_bit = ob;
            }
          }
          region_need = tbl;  // the bitstream itself is variably sized
          break;
        }
        case 34: {
          if (prm == 0 || prm > 64 || have < 11) {
            trace(111, prm);
            return w;
          }
          const uint64_t pbits_width = rd(off + 8, 1);
          const uint64_t nexc = rd(off + 9, 2);
          if (pbits_width != prm || nexc > n) {
            trace(112, pbits_width);
            return w;
          }
          const uint64_t stride = (n + 7) / 8;
          const uint64_t pbits_need = 11 + (stride * pbits_width) +
                                      (nexc == 0 ? 0 : stride + (nexc * 10));
          if (pbits_need > have) {
            trace(113, pbits_need);
            return w;
          }
          if (nexc != 0) {
            if (11 + stride > have) {
              trace(114, have);
              return w;
            }
            uint64_t ones = 0;
            for (uint64_t i = 0; i < stride; ++i) {
              uint64_t byte = rd8(off + 11 + i);
              if (i + 1 == stride && (n & 7U) != 0) {
                const uint64_t tail =
                    (0xFFULL << (8U - static_cast<unsigned>(n & 7U))) & 0xFFULL;
                if ((byte & ~tail) != 0) {
                  trace(115, byte);
                  return w;
                }
                byte &= tail;
              }
              ones += static_cast<uint64_t>(__builtin_popcountll(byte));
            }
            if (ones != nexc) {
              trace(116, ones);
              return w;
            }
            const uint64_t entries = off + 11 + stride;
            uint64_t previous = 0;
            for (uint64_t k = 0; k < nexc; ++k) {
              const uint64_t pbits_pos = rd(entries + (k * 10), 2);
              if (pbits_pos >= n || (k != 0 && pbits_pos <= previous)) {
                trace(117, pbits_pos);
                return w;
              }
              if ((rd(off + 11 + (pbits_pos >> 3), 1) &
                   (0x80U >> (pbits_pos & 7U))) == 0) {
                trace(117, pbits_pos);
                return w;
              }
              previous = pbits_pos;
            }
          }
          region_need = pbits_need;
          break;
        }
        case 35: {  // INTERP: [ncp u16][ckpts ncp x (first 8B, last 8B)]
                    // [bitoffs ncp x u32 BE][BIC bitstream MSB-first].
                    // param IS ncp. The predicate kernels lean on global
                    // non-decreasing order, so the gates pin it here: the
                    // brackets must satisfy first[b] <= last[b] <=
                    // first[b+1], the resume offsets must be inside the
                    // stream, and every block must fully decode to values
                    // inside its bracket (a hostile bit pattern that breaks
                    // monotonicity lands outside the bracket and is
                    // rejected; blocks whose decode agrees with their
                    // bracket boundaries reconstruct exactly the values the
                    // encoder captured).
          if (n == 0 || prm == 0 ||
              prm != (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) {
            trace(118, prm);
            return w;
          }
          const uint64_t fixed = 2 + (prm * 20);
          if (have < fixed) {
            trace(119, have);
            return w;
          }
          for (uint64_t bi = 0; bi < prm; ++bi) {
            // SIGNED order: the u64 words of a sign-crossing block sort
            // backwards (see ParseInterpRegion).
            const auto first = static_cast<int64_t>(rd(off + 2 + (bi * 16), 8));
            const auto last =
                static_cast<int64_t>(rd(off + 2 + (bi * 16) + 8, 8));
            if (last < first) {
              trace(120, static_cast<uint64_t>(last));
              return w;
            }
            if (bi + 1 < prm) {
              const auto next_first =
                  static_cast<int64_t>(rd(off + 2 + ((bi + 1) * 16), 8));
              if (next_first < last) {
                trace(121, static_cast<uint64_t>(first));
                return w;
              }
            }
          }
          {
            const uint64_t stream_bits = (have - fixed) * 8;
            uint64_t prev_bit = 0;
            for (uint64_t bi = 0; bi < prm; ++bi) {
              const uint64_t ob = rd(off + 2 + (prm * 16) + (bi * 4), 4);
              if ((bi == 0 && ob != 0) || ob < prev_bit || ob > stream_bits) {
                trace(122, ob);
                return w;
              }
              prev_bit = ob;
            }
          }
          {
            // Full-decode consistency: replay each block tree and demand
            // the bracket boundaries plus intra-block monotonicity (same
            // walk the decoders run; n <= 1024 keeps this O(rows)).
            InterpRegion parsed;
            const char* base = b.data() + off;
            if (!ParseInterpRegion(base, static_cast<uint32_t>(n),
                                   static_cast<uint32_t>(prm), base + have,
                                   &parsed)) {
              trace(123, have);
              return w;
            }
            std::vector<int64_t> block(kDeltaCheckpointEvery, 0);
            for (uint32_t blk = 0; blk < parsed.ncp; ++blk) {
              std::fill(block.begin(), block.end(), 0);
              InterpDecodeBlock(parsed, blk, block.data());
              const uint32_t s = blk * kDeltaCheckpointEvery;
              const uint32_t t = static_cast<uint32_t>(
                  std::min<uint64_t>(n, s + kDeltaCheckpointEvery));
              const uint64_t block_off = static_cast<uint64_t>(blk) * 16;
              const auto first =
                  static_cast<int64_t>(rd(off + 2 + block_off, 8));
              const auto last =
                  static_cast<int64_t>(rd(off + 2 + block_off + 8, 8));
              if (block[0] != first || block[t - 1 - s] != last) {
                trace(124, static_cast<uint64_t>(first));
                return w;
              }
              for (uint32_t i = 1; i + s < t; ++i) {
                if (block[i] < block[i - 1]) {
                  trace(125, static_cast<uint64_t>(block[i]));
                  return w;
                }
              }
            }
          }
          region_need = fixed;  // the bitstream itself is variably sized
          break;
        }
        case 36: {
          if (n == 0 || prm == 0 || prm > n || have < 14) {
            trace(126, prm);
            return w;
          }
          const uint64_t vw = rd(off + 8, 1);
          const uint64_t lw = rd(off + 9, 1);
          const uint64_t runs = rd(off + 10, 4);
          if (vw == 0 || vw > 64 || lw == 0 || lw > 32 || runs != prm) {
            trace(127, vw);
            return w;
          }
          const uint64_t value_bytes = (runs * vw + 7) / 8;
          const uint64_t length_bytes = (runs * lw + 7) / 8;
          if (14 + value_bytes + length_bytes > have) {
            trace(128, have);
            return w;
          }
          const char* lengths =
              b.data() + off + 14 + static_cast<size_t>(value_bytes);
          uint64_t covered = 0;
          for (uint64_t i = 0; i < runs; ++i) {
            const auto length = static_cast<uint32_t>(
                UnpackBitAt(lengths, i * lw, static_cast<int>(lw)));
            if (length == 0 || length > n - covered) {
              trace(129, length);
              return w;
            }
            covered += length;
          }
          if (covered != n) {
            trace(130, covered);
            return w;
          }
          region_need = 14 + value_bytes + length_bytes;
          break;
        }
        case 37: {
          if (n == 0 || prm > 63 || have < 11 || rd(off + 8, 1) != prm) {
            trace(131, prm);
            return w;
          }
          const uint64_t nexc = rd(off + 9, 2);
          const uint64_t blocks =
              (n + kDeltaPForBlockRows - 1) / kDeltaPForBlockRows;
          const uint64_t packed_bytes = ((n - 1) * prm + 7) / 8;
          region_need = 11 + (blocks * kDeltaPForBracketBytes) + packed_bytes +
                        (nexc * 10);
          if (nexc > n - 1 || region_need > have) {
            trace(132, have);
            return w;
          }
          const char* base = b.data() + off;
          DeltaPForRegion parsed;
          if (!ParseDeltaPForRegion(base, static_cast<uint32_t>(n),
                                    static_cast<uint32_t>(prm), base + have,
                                    &parsed)) {
            trace(133, have);
            return w;
          }
          std::array<int64_t, PaxChunk::kRowGroupSize> decoded{};
          DecodeDeltaPForAll(parsed, decoded.data());
          uint64_t minimum = n == 1 ? 0 : UINT64_MAX;
          for (uint32_t i = 1; i < n; ++i) {
            if (decoded[i] < decoded[i - 1]) {
              trace(134, static_cast<uint64_t>(decoded[i]));
              return w;
            }
            minimum =
                std::min(minimum, static_cast<uint64_t>(decoded[i]) -
                                      static_cast<uint64_t>(decoded[i - 1]));
          }
          if (minimum != parsed.delta_min) {
            trace(135, minimum);
            return w;
          }
          for (uint32_t block = 0; block < parsed.blocks; ++block) {
            const char* bracket =
                parsed.brackets +
                (static_cast<size_t>(block) * kDeltaPForBracketBytes);
            const uint32_t start = block * kDeltaPForBlockRows;
            const uint32_t end = static_cast<uint32_t>(std::min<uint64_t>(
                n, static_cast<uint64_t>(start) + kDeltaPForBlockRows));
            __int128 sum = 0;
            for (uint32_t i = start; i < end; ++i) {
              sum += decoded[i];
            }
            const auto bits = static_cast<unsigned __int128>(sum);
            if (ReadBE64(bracket) != static_cast<uint64_t>(decoded[start]) ||
                ReadBE64(bracket + 8) !=
                    static_cast<uint64_t>(decoded[end - 1]) ||
                ReadBE64(bracket + 16) != static_cast<uint64_t>(bits >> 64) ||
                ReadBE64(bracket + 24) != static_cast<uint64_t>(bits)) {
              trace(136, block);
              return w;
            }
          }
          break;
        }
        case 38: {
          if (n == 0 || prm != n || have < 9) {
            trace(137, prm);
            return w;
          }
          const uint64_t width = rd(off + 8, 1);
          if (width == 0 || width > 64) {
            trace(138, width);
            return w;
          }
          const uint64_t stride = (n + 7) / 8;
          const uint32_t rank_count =
              WaveletRRankCount(static_cast<uint32_t>(n));
          region_need = 9 + (width * stride) + (width * rank_count * 2);
          if (region_need > have) {
            trace(139, have);
            return w;
          }
          WaveletRRegion parsed;
          if (!ParseWaveletRRegion(b.data() + off, static_cast<uint32_t>(n),
                                   static_cast<uint32_t>(prm),
                                   b.data() + off + have, &parsed)) {
            trace(140, have);
            return w;
          }
          break;
        }
        case 39: {
          QppRegion parsed;
          if (!ParseQppRegion(b.data() + off, static_cast<uint32_t>(n),
                              static_cast<uint32_t>(prm), b.data() + off + have,
                              &parsed)) {
            trace(141, have);
            return w;
          }
          region_need = have;
          break;
        }
        case 40: {
          Bp128Region parsed;
          if (!ParseBp128Region(b.data() + off, static_cast<uint32_t>(n),
                                static_cast<uint32_t>(prm),
                                b.data() + off + have, &parsed) ||
              !Bp128MetadataMatches(parsed)) {
            trace(142, have);
            return w;
          }
          region_need = parsed.encoded_size;
          break;
        }
        case 41: {
          Rle8Region parsed;
          if (!ParseRle8Region(b.data() + off, static_cast<uint32_t>(n),
                               static_cast<uint32_t>(prm),
                               b.data() + off + have, &parsed)) {
            trace(143, have);
            return w;
          }
          region_need = 16 + (3 * static_cast<uint64_t>(prm));
          break;
        }
        case 42: {
          RleDeltaRegion parsed;
          if (!ParseRleDeltaRegion(b.data() + off, static_cast<uint32_t>(n),
                                   static_cast<uint32_t>(prm),
                                   b.data() + off + have, &parsed)) {
            trace(144, have);
            return w;
          }
          region_need = have;
          break;
        }
        case 43: {
          Chimp128Region parsed;
          if (!ParseChimp128Region(b.data() + off, static_cast<uint32_t>(n),
                                   static_cast<uint32_t>(prm),
                                   b.data() + off + have, &parsed) ||
              !Chimp128MetadataMatches(parsed)) {
            trace(145, have);
            return w;
          }
          region_need = parsed.encoded_size;
          break;
        }
        case 44: {
          AlpRdTansRegion parsed;
          if (!ParseAlpRdTansRegion(b.data() + off, static_cast<uint32_t>(n),
                                    static_cast<uint32_t>(prm),
                                    b.data() + off + have, &parsed) ||
              !AlpRdTansMetadataMatches(parsed)) {
            trace(146, have);
            return w;
          }
          region_need = parsed.encoded_size;
          break;
        }
        case 45: {
          PatasRegion parsed;
          if (!ParsePatasRegion(b.data() + off, static_cast<uint32_t>(n),
                                static_cast<uint32_t>(prm),
                                b.data() + off + have, &parsed) ||
              !PatasMetadataMatches(parsed)) {
            trace(147, have);
            return w;
          }
          region_need = parsed.encoded_size;
          break;
        }
        case 46: {  // WAH: [tv 8B][fv 8B][prm words x 8B BE]
          if (prm == 0 || prm > (n + 62) / 63) {
            trace(148, prm);
            return w;
          }
          region_need = 16 + (8 * prm);
          if (region_need > have) {
            // Gate the word walk before comparing region_need: the loop
            // below reads every word regardless of the outer extent check.
            trace(148, have);
            return w;
          }
          uint64_t covered = 0;
          for (uint64_t k = 0; k < prm; ++k) {
            const uint64_t word = rd(off + 16 + (k * 8), 8);
            if ((word >> 63) ==
                0) {  // literal span of 63 rows (tail: n-covered)
              if (covered >= n) {
                trace(148, covered);
                return w;
              }
              covered += (n - covered < 63) ? (n - covered) : 63;
            } else {  // fill span: bits 61..0 = block count, each covering 63
              const uint64_t flen = word & ((1ULL << 62) - 1);
              if (flen == 0 || flen > (n - covered) / 63) {
                trace(148, flen);
                return w;
              }
              covered += 63 * flen;
            }
          }
          if (covered != n) {  // words must cover the row group exactly
            trace(148, covered);
            return w;
          }
          break;
        }
        case 47: {  // PLA: [nseg u16][nseg x (first 8B, d 8B, end 4B)]
                    // [ischeme u8][iprm u32][nexc u16]
                    // [bitmap ceil(n/8)][inner region of nexc exceptions;
                    //   no inner bytes at nexc == 0]
          if (have < 9) {
            trace(149, have);
            return w;
          }
          const uint64_t nseg = rd(off, 2);
          if (nseg == 0 || nseg != prm || nseg > n) {
            trace(149, nseg);
            return w;
          }
          if (9 + (20 * nseg) > have) {
            trace(149, have);
            return w;
          }
          // Segment table consistency: ends strictly increase and tile
          // [0, n] exactly (the last segment's end covers exception rows
          // too -- the bitmap decides on-line, not the table).
          uint64_t prev_end = 0;
          for (uint64_t j = 0; j < nseg; ++j) {
            const uint64_t e = rd(off + 2 + (j * 20) + 16, 4);
            if (e <= prev_end || e > n) {
              trace(149, e);
              return w;
            }
            prev_end = e;
          }
          if (prev_end != n) {
            trace(149, prev_end);
            return w;
          }
          const uint64_t nexc = rd(off + 9 + (20 * nseg) - 2, 2);
          const uint64_t bmp = (n + 7) / 8;
          if (9 + (20 * nseg) + bmp > have) {
            trace(149, have);
            return w;
          }
          // Bitmap consistency: exactly n-nexc set bits (tail padding
          // masked), the same door check as case 27.
          uint64_t ones = 0;
          for (uint64_t i = 0; i < bmp; ++i) {
            uint64_t byte = rd8(off + 9 + (20 * nseg) + i);
            if (i + 1 == bmp && (n & 7) != 0) {
              byte &= static_cast<uint64_t>(0xFFU << (8 - (n & 7))) & 0xFF;
            }
            ones += static_cast<uint64_t>(__builtin_popcountll(byte));
          }
          if (ones != n - nexc) {
            trace(149, ones);
            return w;
          }
          uint64_t ineed = 0;
          if (nexc > 0) {
            const uint64_t ischeme = rd(off + 9 + (20 * nseg) - 7, 1);
            const uint64_t iprm = rd(off + 9 + (20 * nseg) - 6, 4);
            const uint64_t ioff = off + 9 + (20 * nseg) + bmp;
            const uint64_t iavail = off + have - ioff;
            ineed = inner_extent(ioff, iavail, ischeme, iprm, nexc);
            if (ineed == UINT64_MAX || ineed > iavail) {
              trace(149, ineed);
              return w;
            }
          }
          region_need = 9 + (20 * nseg) + bmp + ineed;
          break;
        }
        case 48: {  // SIMPLE-8B: [base 8B][prm words x 8B BE]
          // Every word covers >= 1 row, so the word count cannot exceed the
          // row count (mirrors the RLE runs <= n door).
          if (prm == 0 || prm > n) {
            trace(150, prm);
            return w;
          }
          region_need = 8 + (8 * prm);
          if (region_need > have) {
            trace(150, have);
            return w;
          }
          // Word-walk integrity: non-final words must lie fully inside n and
          // the words must cover every row (the final word may pad past n to
          // its selector's full count, exactly as the encoder emits it).
          uint64_t covered = 0;
          for (uint64_t k = 0; k < prm; ++k) {
            const uint64_t word = rd(off + 8 + (k * 8), 8);
            const uint64_t cnt = kSimple8bSelCount[word >> 60];
            if (k + 1 < prm && covered + cnt > n) {
              trace(150, covered);
              return w;
            }
            covered += cnt;
          }
          if (covered < n) {
            trace(150, covered);
            return w;
          }
          break;
        }
        case 49: {  // RLE-IDX: [runs u32][nidx u32][values FOR-BA]
                    //         [lengths FOR-BA][restart nidx x u32 BE]
          // The two payload streams are byte-aligned FOR-BA regions (the
          // scheme-3 layout), so their extents are exact functions of `runs`
          // and the header bpv bytes -- no stream walking needed to size the
          // cell, and the WASM parse_footer case 49 derives the same number.
          if (prm == 0 || prm > n) {
            trace(151, prm);
            return w;
          }
          if (have < 8) {
            trace(151, have);
            return w;
          }
          if (rd(off, 4) != prm) {  // header echoes the footer run count
            trace(151, rd(off, 4));
            return w;
          }
          const uint64_t nidx = rd(off + 4, 4);
          if (nidx == 0 || nidx != (prm + 15) / 16 || nidx > n) {
            trace(151, nidx);
            return w;
          }
          if (have < 8 + 9) {
            trace(151, have);
            return w;
          }
          const uint64_t vbpv = rd(off + 8 + 8, 1);
          if (vbpv == 0 || vbpv > 8) {
            trace(151, vbpv);
            return w;
          }
          const uint64_t loff = off + 8 + 9 + (prm * vbpv);
          if (loff + 9 > off + have) {
            trace(151, loff);
            return w;
          }
          const uint64_t lbpv = rd(loff + 8, 1);
          if (lbpv == 0 || lbpv > 8) {
            trace(151, lbpv);
            return w;
          }
          const uint64_t roff = loff + 9 + (prm * lbpv);
          if (roff + (nidx * 4) > off + have) {
            trace(151, roff);
            return w;
          }
          region_need = 8 + 9 + (prm * vbpv) + 9 + (prm * lbpv) + (nidx * 4);
          // Integrity walk (O(runs + nidx), the same order the encoder paid):
          // the lengths must be non-zero and tile n, and -- the invariant the
          // read paths binary search on -- the run values must be
          // non-decreasing. The restart table must agree with the lengths.
          const uint64_t vbase = rd(off + 8, 8);
          const uint64_t lbase = rd(loff, 8);
          uint64_t covered = 0;
          int64_t previous_value = 0;
          for (uint64_t k = 0; k < prm; ++k) {
            uint64_t voff = 0;
            for (uint64_t vb = 0; vb < vbpv; ++vb) {
              voff |= rd8(off + 8 + 9 + (k * vbpv) + vb) << (vb * 8);
            }
            uint64_t loff_v = 0;
            for (uint64_t lb = 0; lb < lbpv; ++lb) {
              loff_v |= rd8(loff + 9 + (k * lbpv) + lb) << (lb * 8);
            }
            const uint64_t length = lbase + loff_v;
            if (length == 0 || length > n - covered) {
              trace(151, covered);
              return w;
            }
            // Wrapping add, exactly as the decoders assemble the value: the
            // signed comparison below is then the same one the binary search
            // performs at read time.
            const auto value = static_cast<int64_t>(vbase + voff);
            if (k != 0 && value < previous_value) {
              trace(151, static_cast<uint64_t>(value));
              return w;
            }
            previous_value = value;
            covered += length;
          }
          if (covered != n) {
            trace(151, covered);
            return w;
          }
          for (uint64_t e = 0; e < nidx; ++e) {
            const uint64_t row = rd(roff + (e * 4), 4);
            if (row > n || (e != 0 && row < rd(roff + ((e - 1) * 4), 4))) {
              trace(151, row);
              return w;
            }
          }
          if (rd(roff, 4) != 0) {
            trace(151, rd(roff, 4));
            return w;
          }
          break;
        }
        case 52: {  // D2-PFOR: [ncp u16][ckpt ncp x (val 8B, sdp 8B)]
                    //         [inner: a complete PFOR region over n-2 dd]
          if (n < 3 || prm == 0 || prm > 64 || have < 2) {
            trace(154, n);
            return w;
          }
          // Pin the seek table to the encoder's formula: without it a region
          // could declare fewer checkpoints than the highest block index and
          // D2PforValueAt would read the inner region as a seek table.
          if (rd(off, 2) !=
              ((n + kD2PforCheckpointEvery - 1) / kD2PforCheckpointEvery) + 1) {
            trace(154, rd(off, 2));
            return w;
          }
          const uint64_t ncp = rd(off, 2);
          const uint64_t inner_off = off + 2 + (16 * ncp);
          if (inner_off > off + have) {
            trace(154, inner_off);
            return w;
          }
          const uint64_t avail = off + have - inner_off;
          const uint64_t ineed = inner_extent(inner_off, avail, 16, prm, n - 2);
          if (ineed == UINT64_MAX || ineed > avail) {
            trace(154, ineed);
            return w;
          }
          region_need = 2 + (16 * ncp) + ineed;
          break;
        }
        case 50: {  // STREAM-VBYTE: [stream_len u32 BE][nblk u16]
                    //               [block_off nblk x u16 BE]
                    //               [ctrl ceil(n/4)][payload stream_len]
          // The footer param IS the payload length, so the header must echo
          // it (the same cross-check cases 1/23/24/27/29/30 make). nblk and
          // the control plane are fixed by n alone, so the extent is exact and
          // the WASM parse_footer case 50 derives the same number from the same
          // facts.
          if (n == 0 || prm == 0 || prm > n * kStreamVByteMaxWidth) {
            trace(152, prm);
            return w;
          }
          if (have < 6) {
            trace(152, have);
            return w;
          }
          if (rd(off, 4) != prm) {
            trace(152, rd(off, 4));
            return w;
          }
          const uint64_t nblk = (n + kStreamVByteBlock - 1) / kStreamVByteBlock;
          const uint64_t ctrl_len =
              (n + kStreamVBytePerCtrl - 1) / kStreamVBytePerCtrl;
          const uint64_t slen = prm;
          if (rd(off + 4, 2) != nblk) {
            trace(152, rd(off + 4, 2));
            return w;
          }
          if (6 + (2 * nblk) + ctrl_len + slen > have) {
            trace(152, have);
            return w;
          }
          // One width walk validates the block index AND the extent: every
          // block_off must equal the walked prefix at its block boundary and
          // the total must land exactly on slen, so no per-value offset the
          // readers derive from the index can leave the cell.
          uint64_t sum = 0;
          for (uint64_t i = 0; i < n; ++i) {
            if (i % kStreamVByteBlock == 0 &&
                rd(off + 6 + ((i / kStreamVByteBlock) * 2), 2) != sum) {
              trace(152, i);
              return w;
            }
            sum += ((rd8(off + 6 + (2 * nblk) + (i / kStreamVBytePerCtrl)) >>
                     (6 - 2 * (i % kStreamVBytePerCtrl))) &
                    0x3) +
                   1;
            if (sum > slen) {
              trace(152, sum);
              return w;
            }
          }
          if (sum != slen) {
            trace(152, sum);
            return w;
          }
          region_need = 6 + (2 * nblk) + ctrl_len + slen;
          break;
        }
        case 51: {  // HUFF-DICT: [d u16][dict 8d][lens (d+1)/2][freq 2d]
                    //             [ncp u16][ckpt 4ncp][stream_len u32][stream]
          // The footer param IS the dictionary size, echoed in the header
          // (cases 4/15/50 shape). Every downstream extent the decoders
          // derive comes from the header fields, so all of them are pinned
          // here -- including a FULL canonical decode of the bitstream,
          // histogrammed against the stored frequency table: the closed-form
          // kernels trust `freq` without reading the stream, so the region
          // must prove that trust once at Deserialize.
          if (prm == 0 || prm > kHuffDictMaxCodes || n == 0) {
            trace(153, prm);
            return w;
          }
          if (have < 2) {
            trace(153, have);
            return w;
          }
          if (rd(off, 2) != prm) {
            trace(153, rd(off, 2));
            return w;
          }
          const uint64_t d = prm;
          const uint64_t ncp =
              (n + kHuffDictCheckpoint - 1) / kHuffDictCheckpoint;
          const uint64_t lens_off = off + 2 + (8 * d);
          const uint64_t freq_off = lens_off + ((d + 1) / 2);
          const uint64_t ncp_off = freq_off + (2 * d);
          const uint64_t ckpt_off = ncp_off + 2;
          const uint64_t slen_off = ckpt_off + (4 * ncp);
          if (slen_off + 4 > off + have) {
            trace(153, have);
            return w;
          }
          if (rd(ncp_off, 2) != ncp) {
            trace(153, rd(ncp_off, 2));
            return w;
          }
          const uint64_t slen = rd(slen_off, 4);
          if (slen == 0 || slen_off + 4 + slen > off + have) {
            trace(153, slen);
            return w;
          }
          // Lengths in [1,15] with an EXACTLY complete Kraft sum: the
          // canonical decoder resolves every bit pattern only on a complete
          // tree, so completeness is what makes hostile streams safe.
          uint64_t kraft = 0;
          for (uint64_t s = 0; s < d; ++s) {
            const auto byte = static_cast<uint8_t>(rd8(lens_off + (s / 2)));
            const uint64_t nlen = (s % 2 == 0) ? (byte >> 4) : (byte & 0xF);
            if (nlen < 1 || nlen > kHuffDictMaxLen) {
              trace(153, nlen);
              return w;
            }
            kraft += UINT64_C(1) << (kHuffDictMaxLen - nlen);
          }
          if (kraft != (UINT64_C(1) << kHuffDictMaxLen)) {
            trace(153, kraft);
            return w;
          }
          // Frequencies: every distinct value occurs at least once and the
          // counts sum to the row count (a lying table would poison the
          // closed-form COUNT/SUM answers).
          uint64_t fsum = 0;
          for (uint64_t s = 0; s < d; ++s) {
            const uint64_t fq = rd(freq_off + (2 * s), 2);
            if (fq == 0 || fq > n) {
              trace(153, fq);
              return w;
            }
            fsum += fq;
          }
          if (fsum != n) {
            trace(153, fsum);
            return w;
          }
          // Checkpoints: ckpt[0] == 0, non-decreasing, each a real bit
          // position inside the stream.
          uint64_t prev = 0;
          for (uint64_t k = 0; k < ncp; ++k) {
            const uint64_t bit = rd(ckpt_off + (4 * k), 4);
            if ((k == 0 && bit != 0) || bit < prev ||
                bit >= slen * 8) {  // u64 math: slen <= cell <= chunk size
              trace(153, bit);
              return w;
            }
            prev = bit;
          }
          // One-time semantic walk: decode all n symbols with the canonical
          // tables and require the histogram to equal the stored frequency
          // table exactly.
          {
            HuffDictTables vt;
            HuffDictRegion hr;
            hr.d = static_cast<uint32_t>(d);
            hr.n = static_cast<uint32_t>(n);
            hr.ncp = static_cast<uint32_t>(ncp);
            hr.stream_len = static_cast<uint32_t>(slen);
            // The tables only read the lens nibbles through HuffDictLenAtRaw;
            // point the struct at the validated offset (no extent use).
            hr.lens = reinterpret_cast<const uint8_t*>(b.data() + lens_off);
            // ...and BUILD them. This call was missing, so `vt` stayed
            // default-initialised and the walk below decoded every symbol
            // through an empty table: the histogram never matched `freq`, and
            // the arm rejected EVERY kHuffDict chunk -- including ones the
            // encoder had just written. It hid because the adaptive selector
            // only reaches kHuffDict on multi-modal skewed integer columns, so
            // the only routes in were TINYLAMB_FORCE_SCHEME=HUFFDICT or a shape
            // that happened to win the cost race. Found by fuzz_encode_agree,
            // which asserts that whatever the encoder emits VALIDATES -- a
            // property no byte-corruption harness can check, since it never
            // asks whether the encoder's own output is valid.
            HuffDictBuildTables(hr, &vt);
            std::vector<uint64_t> hist(static_cast<size_t>(d), 0);
            WordBitReader br(b.data() + slen_off + 4,
                             b.data() + slen_off + 4 + slen);
            br.bitpos = 0;
            for (uint64_t i = 0; i < n; ++i) {
              const uint32_t sym = HuffDictDecodeSym(vt, br);
              if (sym >= d) {
                trace(153, sym);
                return w;
              }
              ++hist[sym];
            }
            for (uint64_t s = 0; s < d; ++s) {
              if (hist[static_cast<size_t>(s)] != rd(freq_off + (2 * s), 2)) {
                trace(153, s);
                return w;
              }
            }
          }
          region_need = slen_off + 4 + slen - off;
          break;
        }
        default: {
          trace(85, scheme);
          return w;
        }  // unknown scheme
      }
      if (region_need > have) {
        trace(95, region_need);
        return w;
      }
    }
  }
  // Optional flag-gated sections; each flag byte must be 0 or 1 and each
  // declared payload must fit.
  const auto section = [&](uint64_t payload_per_unit, uint64_t units,
                           const char* what) -> bool {
    if (pos >= footer_size) {
      trace(70, pos);
      return false;
    }
    const uint64_t flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (flag > 1) {
      trace(71, flag);
      return false;
    }
    ++pos;
    if (flag != 0) {
      pos += units * payload_per_unit;
      if (pos > footer_size) {
        trace(72, pos);
        std::cerr << "validate: overflow in " << what << "\n";
        return false;
      }
    }
    return true;
  };
  if (!section(17, cc, "stats")) {
    trace(999, 2991);
    return w;
  }  // col stats
  if (!section(16, rg * cc, "azm")) {
    trace(999, 3056);
    return w;
  }  // all-column zone maps
  if (!section(8, rg * cc, "blooms")) {
    trace(999, 3131);
    return w;
  }  // blooms
  // Writer emits rg_sums BETWEEN blooms and HLL (SerializeFooter order).
  if (!section(8, rg * cc, "rgsums")) {
    trace(999, 3264);
    return w;
  }  // rg partial sums
  if (!section(64, rg * cc, "hll")) {
    trace(999, 3331);
    return w;
  }  // HLL sketches
  // Hash index (v9): [flag][log2 u8][keys 8B x sz][rows 4B x sz]
  if (pos >= footer_size) {
    trace(999, 3453);
    return w;
  }
  {
    const uint64_t hi_flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (hi_flag > 1) {
      trace(999, 3563);
      return w;
    }
    ++pos;
    if (hi_flag != 0) {
      if (pos >= footer_size) {
        trace(999, 3638);
        return w;
      }
      const uint64_t log2v = rd(footer_off + static_cast<size_t>(pos), 1);
      ++pos;
      if (log2v > 31) {
        trace(999, 3758);
        return w;
      }
      const uint64_t sz = 1ULL << log2v;
      pos += sz * 12;
      if (pos > footer_size) {
        trace(999, 3860);
        return w;
      }
    }
  }
  // Null bitmaps (v11): [present u8][concatenated rg*col bitmaps, each
  // ceil(rc_g/8) bytes in RG-major, col-minor order].
  {
    if (pos >= footer_size) {
      trace(999, 4810);
      return w;
    }
    const uint64_t nb_flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (nb_flag > 1) {
      trace(999, 4820);
      return w;
    }
    ++pos;
    if (nb_flag != 0) {
      uint64_t total = 0;
      for (uint64_t g = 0; g < rg; ++g) {
        const uint64_t rc =
            rd(footer_off + 30 + (static_cast<size_t>(g) * 2), 2);
        const uint64_t cell = (rc + 7) / 8;
        // Guard against overflow in total + ensure it fits.
        if (cell > (footer_size - pos) / cc) {
          trace(99, cell);
          return w;
        }
        total += cell * cc;
      }
      pos += total;
      if (pos > footer_size) {
        trace(999, 4830);
        return w;
      }
    }
  }
  // Extended column stats (v12): [flag u8][cc x (5*8 + HIST_BINS*8) bytes].
  {
    if (pos >= footer_size) {
      trace(999, 4840);
      return w;
    }
    const uint64_t es_flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (es_flag > 1) {
      trace(999, 4850);
      return w;
    }
    ++pos;
    if (es_flag != 0) {
      const uint64_t per_col =
          (5 * 8) + (PaxChunk::Footer::ColumnStats::HIST_BINS * 8);
      pos += cc * per_col;
      if (pos > footer_size) {
        trace(999, 4860);
        return w;
      }
    }
  }
  // Inverted secondary index (v13): absent (no room for the flag) on older
  // chunks; otherwise [flag u8][ncell u32][cells...]. Every count, container
  // tag, and row id is gated; row ids must fall inside their row group.
  if (pos < footer_size) {
    const uint64_t iv_flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (iv_flag > 1) {
      trace(999, 4870);
      return w;
    }
    ++pos;
    if (iv_flag != 0) {
      if (pos + 4 > footer_size) {
        trace(999, 4871);
        return w;
      }
      const uint64_t ncell = rd(footer_off + static_cast<size_t>(pos), 4);
      pos += 4;
      if (ncell > rg * cc) {
        trace(999, 4872);
        return w;
      }
      for (uint64_t ci = 0; ci < ncell; ++ci) {
        if (pos + 12 > footer_size) {
          trace(999, 4873);
          return w;
        }
        const uint64_t gg = rd(footer_off + static_cast<size_t>(pos), 4);
        const uint64_t ccol = rd(footer_off + static_cast<size_t>(pos) + 4, 4);
        const uint64_t nval = rd(footer_off + static_cast<size_t>(pos) + 8, 4);
        pos += 12;
        if (gg >= rg || ccol >= cc || nval > 1024) {
          trace(999, 4874);
          return w;
        }
        const uint64_t rc =
            rd(footer_off + 30 + (static_cast<size_t>(gg) * 2), 2);
        for (uint64_t vi = 0; vi < nval; ++vi) {
          if (pos + 9 > footer_size) {
            trace(999, 4875);
            return w;
          }
          const uint64_t ctype =
              rd(footer_off + static_cast<size_t>(pos) + 8, 1);
          if (ctype > 2) {
            trace(999, 4876);
            return w;
          }
          pos += 9;
          if (ctype == 0) {  // array
            if (pos + 2 > footer_size) {
              trace(999, 4877);
              return w;
            }
            const uint64_t k = rd(footer_off + static_cast<size_t>(pos), 2);
            pos += 2;
            if (k > 1024 || pos + (2 * k) > footer_size) {
              trace(999, 4878);
              return w;
            }
            for (uint64_t t = 0; t < k; ++t) {
              if (rd(footer_off + static_cast<size_t>(pos) + (t * 2), 2) >=
                  rc) {
                trace(999, 4879);
                return w;
              }
            }
            pos += 2 * k;
          } else if (ctype == 2) {  // run
            if (pos + 2 > footer_size) {
              trace(999, 4880);
              return w;
            }
            const uint64_t nr = rd(footer_off + static_cast<size_t>(pos), 2);
            pos += 2;
            if (nr > 1024 || pos + (4 * nr) > footer_size) {
              trace(999, 4881);
              return w;
            }
            for (uint64_t t = 0; t < nr; ++t) {
              const uint64_t st =
                  rd(footer_off + static_cast<size_t>(pos) + (t * 4), 2);
              const uint64_t ln =
                  rd(footer_off + static_cast<size_t>(pos) + (t * 4) + 2, 2);
              if (ln == 0 || st >= rc || st + ln > rc) {
                trace(999, 4882);
                return w;
              }
            }
            pos += 4 * nr;
          } else {  // bitmap: 16 x u64
            if (pos + 128 > footer_size) {
              trace(999, 4883);
              return w;
            }
            // A bitmap encodes raw row ids: bits at or beyond the group's
            // row count are hostile data the count kernels would trust.
            // Array/run containers are range-checked above; gate the
            // bitmap's popcount the same way (cheap sufficient bound).
            uint64_t set_bits = 0;
            for (int bw = 0; bw < 16; ++bw) {
              set_bits += static_cast<uint64_t>(__builtin_popcountll(
                  rd(footer_off + static_cast<size_t>(pos) +
                         (static_cast<size_t>(bw) * 8),
                     8)));
            }
            if (set_bits > rc) {
              trace(999, 4884);
              return w;
            }
            pos += 128;
          }
        }
      }
    }
  }
  // Xor8 pre-filter (v14): absent (no room for the flag) on older chunks;
  // otherwise [flag u8][ncell u32][cells...]. Table sizes are capped so a
  // hostile m cannot walk the reader past the footer.
  if (pos < footer_size) {
    const uint64_t xf_flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (xf_flag > 1) {
      trace(999, 4890);
      return w;
    }
    ++pos;
    if (xf_flag != 0) {
      if (pos + 4 > footer_size) {
        trace(999, 4891);
        return w;
      }
      const uint64_t ncell = rd(footer_off + static_cast<size_t>(pos), 4);
      pos += 4;
      if (ncell > rg * cc) {
        trace(999, 4892);
        return w;
      }
      for (uint64_t ci = 0; ci < ncell; ++ci) {
        if (pos + 16 > footer_size) {
          trace(999, 4893);
          return w;
        }
        const uint64_t gg = rd(footer_off + static_cast<size_t>(pos), 4);
        const uint64_t ccol = rd(footer_off + static_cast<size_t>(pos) + 4, 4);
        const uint64_t m = rd(footer_off + static_cast<size_t>(pos) + 8, 4);
        pos += 16;
        if (gg >= rg || ccol >= cc || m == 0 || m > 2048) {
          trace(999, 4894);
          return w;
        }
        if (pos + m > footer_size) {
          trace(999, 4895);
          return w;
        }
        pos += m;
      }
    }
  }
  // PGM learned index (v15): absent (no room for the flag) on older chunks;
  // otherwise [flag u8][eps u32][nseg u32][segments...]. Segments must be
  // key-ordered with positions inside the table.
  if (pos < footer_size) {
    const uint64_t pg_flag = rd(footer_off + static_cast<size_t>(pos), 1);
    if (pg_flag > 1) {
      trace(999, 4900);
      return w;
    }
    ++pos;
    if (pg_flag != 0) {
      if (pos + 8 > footer_size) {
        trace(999, 4901);
        return w;
      }
      const uint64_t eps = rd(footer_off + static_cast<size_t>(pos), 4);
      const uint64_t nseg = rd(footer_off + static_cast<size_t>(pos) + 4, 4);
      pos += 8;
      if (eps > 65536 || nseg == 0 || nseg > rg * 1024) {
        trace(999, 4902);
        return w;
      }
      if (pos + (nseg * 24) > footer_size) {
        trace(999, 4903);
        return w;
      }
      uint64_t total = 0;
      for (uint64_t g = 0; g < rg; ++g) {
        total += rd(footer_off + 30 + (static_cast<size_t>(g) * 2), 2);
      }
      int64_t prev_key = 0;
      uint64_t prev_pos = 0;
      for (uint64_t si = 0; si < nseg; ++si) {
        const auto sk = static_cast<int64_t>(
            rd(footer_off + static_cast<size_t>(pos) + (si * 24), 8));
        const uint64_t sp =
            rd(footer_off + static_cast<size_t>(pos) + (si * 24) + 8, 8);
        if ((si > 0 && sk < prev_key) || sp < prev_pos ||
            (total > 0 && sp >= total)) {
          trace(999, 4904);
          return w;
        }
        prev_key = sk;
        prev_pos = sp;
      }
      pos += nseg * 24;
    }
  }

  w.footer_start = footer_off;
  w.footer_len = footer_size;
  w.format_version = static_cast<uint32_t>(fmtver);

  // CRC32C verification (format v2): data region + footer-minus-its-crc.
  if (fmtver >= 2) {
    if (footer_size < 38) {
      trace(999, 4208);
      return w;
    }  // two crc words must fit
    const uint64_t crc_data_stored = rd(footer_off + footer_size - 8, 4);
    const uint64_t crc_footer_stored = rd(footer_off + footer_size - 4, 4);
    if (Crc32cOf(b.data(), footer_off) !=
        static_cast<uint32_t>(crc_data_stored)) {
      trace(90, crc_data_stored);
      {
        trace(999, 4527);
        return w;
      }
    }
    if (Crc32cOf(b.data() + footer_off, footer_size - 4) !=
        static_cast<uint32_t>(crc_footer_stored)) {
      trace(91, crc_footer_stored);
      {
        trace(999, 4697);
        return w;
      }
    }
  }
  w.ok = true;
  {
    trace(999, 4734);
    return w;
  }
}

}  // namespace

Status PaxChunk::ValidateChunkBytes(const std::vector<char>& bytes) {
  return ValidateWalk(bytes).ok ? Status::kSuccess : Status::kCorruption;
}

StatusOr<PaxChunk> PaxChunk::Deserialize(std::vector<char> bytes) {
  const WalkResult walk = ValidateWalk(bytes);
  if (!walk.ok) {
    return Status::kCorruption;
  }

  Footer f;
  const size_t footer_begin = walk.footer_start;
  const size_t footer_end = footer_begin + walk.footer_len;
  size_t p = footer_begin;
  auto u8 = [&]() -> uint8_t { return static_cast<unsigned char>(bytes[p++]); };
  auto u16 = [&]() -> uint16_t {
    const uint16_t v = ReadBE16(bytes.data() + p);
    p += 2;
    return v;
  };
  auto u32 = [&]() -> uint32_t {
    const uint32_t v = ReadBE32(bytes.data() + p);
    p += 4;
    return v;
  };
  auto u64 = [&]() -> uint64_t {
    const uint64_t v = ReadBE64(bytes.data() + p);
    p += 8;
    return v;
  };
  auto i64 = [&]() -> int64_t { return static_cast<int64_t>(u64()); };

  // ValidateWalk has already proved every count and extent below.  Keep this
  // parser deliberately linear and in exactly the writer's section order.
  f.schema_version = u64();
  f.format_version = u32();
  f.row_group_count = u32();
  f.column_count = u16();
  f.key_column_index = u16();
  f.key_column_type = u8();
  f.zone_map_present = u8() != 0;
  f.generation = u64();

  f.row_counts.reserve(f.row_group_count);
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    f.row_counts.push_back(u16());
  }
  const size_t cell_count =
      static_cast<size_t>(f.row_group_count) * f.column_count;
  f.column_offsets.reserve(cell_count);
  for (size_t i = 0; i < cell_count; ++i) {
    f.column_offsets.push_back(u32());
  }
  if (f.zone_map_present) {
    f.group_min_keys.reserve(f.row_group_count);
    f.group_max_keys.reserve(f.row_group_count);
    for (uint32_t g = 0; g < f.row_group_count; ++g) {
      f.group_min_keys.push_back(i64());
    }
    for (uint32_t g = 0; g < f.row_group_count; ++g) {
      f.group_max_keys.push_back(i64());
    }
  }
  f.encodings.reserve(cell_count);
  for (size_t i = 0; i < cell_count; ++i) {
    f.encodings.push_back(u8());
  }
  f.params.reserve(cell_count);
  for (size_t i = 0; i < cell_count; ++i) {
    f.params.push_back(u32());
  }

  f.col_stats_present = u8() != 0;
  if (f.col_stats_present) {
    f.col_sum_valid.reserve(f.column_count);
    f.col_counts.reserve(f.column_count);
    f.col_sums.reserve(f.column_count);
    for (slot_t c = 0; c < f.column_count; ++c) {
      f.col_sum_valid.push_back(u8());
      f.col_counts.push_back(u64());
      f.col_sums.push_back(i64());
    }
  }
  f.all_zm_present = u8() != 0;
  if (f.all_zm_present) {
    f.all_zone_mins.reserve(cell_count);
    f.all_zone_maxs.reserve(cell_count);
    for (size_t i = 0; i < cell_count; ++i) {
      f.all_zone_mins.push_back(i64());
      f.all_zone_maxs.push_back(i64());
    }
  }
  f.bloom_present = u8() != 0;
  if (f.bloom_present) {
    f.rg_blooms.reserve(cell_count);
    for (size_t i = 0; i < cell_count; ++i) {
      f.rg_blooms.push_back(u64());
    }
  }
  f.rg_sums_present = u8() != 0;
  if (f.rg_sums_present) {
    f.rg_sums.reserve(cell_count);
    for (size_t i = 0; i < cell_count; ++i) {
      f.rg_sums.push_back(i64());
    }
  }
  f.hll_present = u8() != 0;
  if (f.hll_present) {
    f.hll_sketches.reserve(cell_count * 64);
    for (size_t i = 0; i < cell_count * 64; ++i) {
      f.hll_sketches.push_back(u8());
    }
  }

  f.hash_idx_present = u8() != 0;
  if (f.hash_idx_present) {
    f.hash_idx_log2 = u8();
    const size_t hash_size = size_t{1} << f.hash_idx_log2;
    f.hash_keys.reserve(hash_size);
    f.hash_rows.reserve(hash_size);
    for (size_t i = 0; i < hash_size; ++i) {
      uint64_t raw = 0;
      memcpy(&raw, bytes.data() + p, sizeof(raw));
      p += sizeof(raw);
      f.hash_keys.push_back(static_cast<int64_t>(le64toh(raw)));
    }
    for (size_t i = 0; i < hash_size; ++i) {
      uint32_t raw = 0;
      memcpy(&raw, bytes.data() + p, sizeof(raw));
      p += sizeof(raw);
      f.hash_rows.push_back(le32toh(raw));
    }
  }

  f.null_present = u8() != 0;
  if (f.null_present) {
    size_t bitmap_bytes = 0;
    for (uint16_t rc : f.row_counts) {
      bitmap_bytes += static_cast<size_t>(f.column_count) *
                      ((static_cast<size_t>(rc) + 7) / 8);
    }
    f.null_bitmaps.reserve(bitmap_bytes);
    for (size_t i = 0; i < bitmap_bytes; ++i) {
      f.null_bitmaps.push_back(u8());
    }
  }

  f.ext_stats_present = u8() != 0;
  if (f.ext_stats_present) {
    constexpr size_t kBins = Footer::ColumnStats::HIST_BINS;
    f.col_mins.reserve(f.column_count);
    f.col_maxs.reserve(f.column_count);
    f.col_nulls.reserve(f.column_count);
    f.col_modes.reserve(f.column_count);
    f.col_mode_counts.reserve(f.column_count);
    f.col_hists.reserve(static_cast<size_t>(f.column_count) * kBins);
    for (slot_t c = 0; c < f.column_count; ++c) {
      f.col_mins.push_back(i64());
      f.col_maxs.push_back(i64());
      f.col_nulls.push_back(u64());
      f.col_modes.push_back(i64());
      f.col_mode_counts.push_back(u64());
      for (size_t b = 0; b < kBins; ++b) {
        f.col_hists.push_back(u64());
      }
    }
  }

  // v13+ sections are optional so chunks emitted by older writers remain
  // readable.  CRC words are not section bytes.
  const size_t sections_end =
      footer_end - (f.format_version >= 2 ? size_t{8} : size_t{0});
  if (p < sections_end) {
    f.inv_idx_present = u8() != 0;
    if (f.inv_idx_present) {
      const uint32_t ncell = u32();
      f.inv_index.reserve(ncell);
      for (uint32_t ci = 0; ci < ncell; ++ci) {
        InvertedCell cell;
        cell.group = u32();
        cell.column = static_cast<slot_t>(u32());
        const uint32_t nval = u32();
        cell.postings.reserve(nval);
        const uint16_t rc = f.row_counts[cell.group];
        for (uint32_t vi = 0; vi < nval; ++vi) {
          InvertedPosting posting;
          posting.value = i64();
          const uint8_t container = u8();
          if (container == 0) {
            const uint16_t nrow = u16();
            posting.rows.reserve(nrow);
            for (uint16_t ri = 0; ri < nrow; ++ri) {
              posting.rows.push_back(u16());
            }
          } else if (container == 2) {
            const uint16_t nruns = u16();
            for (uint16_t ri = 0; ri < nruns; ++ri) {
              const uint16_t start = u16();
              const uint16_t length = u16();
              for (uint16_t j = 0; j < length; ++j) {
                posting.rows.push_back(static_cast<uint16_t>(start + j));
              }
            }
          } else {
            for (uint16_t word = 0; word < 16; ++word) {
              const uint64_t bits = u64();
              for (uint16_t bit = 0; bit < 64; ++bit) {
                const auto row = static_cast<uint16_t>((word * 64) + bit);
                if (row < rc && ((bits >> bit) & 1ULL) != 0) {
                  posting.rows.push_back(row);
                }
              }
            }
          }
          cell.postings.push_back(std::move(posting));
        }
        f.inv_index.push_back(std::move(cell));
      }
    }
  }
  if (p < sections_end) {
    f.xor_present = u8() != 0;
    if (f.xor_present) {
      const uint32_t ncell = u32();
      f.xor_cells.reserve(ncell);
      for (uint32_t ci = 0; ci < ncell; ++ci) {
        XorCell cell;
        cell.group = u32();
        cell.column = static_cast<slot_t>(u32());
        const uint32_t table_size = u32();
        cell.seed = u32();
        cell.table.reserve(table_size);
        for (uint32_t i = 0; i < table_size; ++i) {
          cell.table.push_back(u8());
        }
        f.xor_cells.push_back(std::move(cell));
      }
    }
  }
  if (p < sections_end) {
    f.pgm_present = u8() != 0;
    if (f.pgm_present) {
      f.pgm_eps = u32();
      const uint32_t nseg = u32();
      f.pgm_segments.reserve(nseg);
      for (uint32_t i = 0; i < nseg; ++i) {
        PgmSegment seg;
        seg.key = i64();
        seg.pos = u64();
        const uint64_t bits = u64();
        memcpy(&seg.slope, &bits, sizeof(bits));
        f.pgm_segments.push_back(seg);
      }
    }
  }
  if (f.format_version >= 2) {
    f.crc_data = ReadBE32(bytes.data() + footer_end - 8);
    f.crc_footer = ReadBE32(bytes.data() + footer_end - 4);
  }
  f.footer_size = static_cast<uint32_t>(walk.footer_len);
  PaxChunk chunk(std::move(bytes), std::move(f));
  chunk.bp128_metadata_trusted_ = true;
  chunk.chimp128_metadata_trusted_ = true;
  chunk.alp_rd_tans_metadata_trusted_ = true;
  chunk.patas_metadata_trusted_ = true;
  chunk.fastpfor_metadata_trusted_ = true;
  chunk.deltafastpfor_metadata_trusted_ = true;
  return chunk;
}

}  // namespace tinylamb
