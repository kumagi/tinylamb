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
#include <array>
#include <utility>

#include "table/pax_chunk/pax_chunk.hpp"
#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb::pax_detail {

// Logical int64 value at local index idx (doubles arrive bitcast).
// region_end bounds the region for schemes that derive their payload extent
// from the region itself (DICT dictionary); nullptr disables the bound for
// legacy callers that cannot supply one (validated regions only).
// NOTE: these helpers duplicate the arms inside Int64RegionAt below
// (kPlain/kConst/kForBp/kBitWeave/kEliasFano here, plus the kArith/kPla
// twins right above Int64AtFast). The canonical arms stay
// inline in the switch so every Int64RegionAt caller (binary-search
// fallbacks, bulk scans, RegionLowerBound) keeps the tuned
// single-function codegen; the row-reassembly path goes through
// Int64AtFast -> helpers instead. When changing an arm, change both.
// Canonical Int64RegionAt kPlain arm, split out for the row path.
int64_t Int64AtPlain(const char* r, uint32_t /*unused*/, uint32_t /*unused*/,
                     uint32_t idx, const char* region_end,
                     const uint32_t* /*unused*/) {
  // region_end may be null only for callers that validated the region
  // already; with a bound, reject a cell whose 8B payload extends past
  // it (a hand-built footer can claim a 4B varchar-floor region).
  if (region_end != nullptr &&
      r + (static_cast<size_t>(idx) * 8) + 8 > region_end) {
    return 0;
  }
  return static_cast<int64_t>(ReadBE64(r + (static_cast<size_t>(idx) * 8)));
}

// Canonical Int64RegionAt kConst arm, split out for the row path.
int64_t Int64AtConst(const char* r, uint32_t /*unused*/, uint32_t /*unused*/,
                     uint32_t /*unused*/, const char* region_end,
                     const uint32_t* /*unused*/) {
  if (region_end != nullptr && r + 8 > region_end) {
    return 0;
  }
  return static_cast<int64_t>(ReadBE64(r));
}

// Canonical Int64RegionAt kForBp arm, split out for the row path.
int64_t Int64AtForBp(const char* r, uint32_t param, uint32_t /*unused*/,
                     uint32_t idx, const char* region_end,
                     const uint32_t* /*unused*/) {
  // The offset assembly below shifts by b*8 for b < param: param past
  // 8 (the validator's [1,8] cap) is hostile-metadata-only territory
  // and shifts by >= 64 is UB. Fail closed (the generic decode treats
  // the cell as 0, matching other out-of-contract regions).
  if (param == 0 || param > 8) {
    return 0;
  }
  if (region_end != nullptr &&
      r + 8 + ((static_cast<size_t>(idx) + 1) * param) > region_end) {
    return 0;
  }
  const auto base_v = static_cast<int64_t>(ReadBE64(r));
  const auto* vp = reinterpret_cast<const unsigned char*>(
      r + 8 + (static_cast<size_t>(idx) * param));
  uint64_t off = 0;
  if (region_end != nullptr &&
      vp + 8 <= reinterpret_cast<const unsigned char*>(region_end)) {
    // One masked u64 load replaces the byte-wise assembly; the window
    // provably stays inside the region (else the byte loop below runs).
    uint64_t raw = 0;
    std::memcpy(&raw, vp, 8);
    off = raw & (param >= 8 ? ~0ULL : ((1ULL << (8U * param)) - 1));
  } else {
    for (uint32_t b = 0; b < param; ++b) {
      off |= static_cast<uint64_t>(vp[b]) << (b * 8U);
    }
  }
  // Two's-complement wrap: base + offset legitimately wraps for extreme
  // bases; signed + would be UB (UBSan traps on [INT64_MIN, x] data).
  return static_cast<int64_t>(static_cast<uint64_t>(base_v) + off);
}

// Canonical Int64RegionAt kBitWeave arm, split out for the row path.
int64_t Int64AtBitWeave(const char* r, uint32_t /*unused*/, uint32_t /*unused*/,
                        uint32_t idx, const char* region_end,
                        const uint32_t* /*unused*/) {
  if (region_end != nullptr && r + 9 > region_end) {
    return 0;
  }
  const auto base_v = static_cast<int64_t>(ReadBE64(r));
  const int w = static_cast<int>(static_cast<uint8_t>(r[8]));
  // A hostile region can carry w > 64: UnpackBitAt would walk idx*w + w
  // bits deep past the region (reads ~32 bytes per value OOB). The
  // footer gate caps prm at 64 for validated chunks; clamp the header
  // byte too so unvalidated decoders stay memory-safe.
  if (w > 64) {
    return 0;
  }
  // Bound the packed-offset extent before UnpackBitAt: a hostile region
  // smaller than ceil((idx+1)*w/8) must not be read past.
  if (region_end != nullptr &&
      r + 9 +
              ((static_cast<size_t>(idx) * static_cast<size_t>(w) +
                static_cast<size_t>(w) + 7) /
               8) >
          region_end) {
    return 0;
  }
  const uint64_t bit_off =
      static_cast<uint64_t>(idx) * static_cast<uint64_t>(w);
  const char* vp = r + 9 + (bit_off >> 3);
  const auto sh = static_cast<uint32_t>(bit_off & 7);
  uint64_t off = 0;
  if (sh + static_cast<uint32_t>(w) <= 64 && region_end != nullptr &&
      vp + 8 <= region_end) {
    // One aligned-window load replaces the byte-assembly: same MSB-first
    // extraction UnpackBitAt performs, without the per-byte loop.
    const uint64_t word = ReadBE64(vp);
    off = (word >> (64 - sh - static_cast<uint32_t>(w))) &
          (w == 64 ? ~0ULL : ((1ULL << w) - 1));
  } else {
    off = UnpackBitAt(r + 9, bit_off, w);
  }
  return static_cast<int64_t>(static_cast<uint64_t>(base_v) + off);
}

// Canonical Int64RegionAt kEliasFano arm, split out for the row path.
int64_t Int64AtEliasFano(const char* r, uint32_t /*unused*/, uint32_t n,
                         uint32_t idx, const char* region_end,
                         const uint32_t* ef_dir) {
  if (region_end != nullptr && r + 17 > region_end) {
    return 0;
  }
  const auto base_v = static_cast<int64_t>(ReadBE64(r));
  const uint64_t maxd = ReadBE64(r + 8);
  const int l = static_cast<int>(static_cast<uint8_t>(r[16]));
  const uint64_t high_max = (l >= 64) ? 0 : (maxd >> l);
  // hlen = n + high_max must not wrap -- and neither may the byte-ceil
  // (hlen + 7) / 8: the former `high_max > UINT64_MAX - n` guard still
  // admitted the boundary high_max == UINT64_MAX - n (hlen == UINT64_MAX,
  // whose +7 wrapped the division to 0), aliasing lowbits onto hbmp and
  // sending SelectBit marching past the region. Reserve 7 bits of
  // headroom so the byte-ceil stays exact. Fail closed instead.
  if (high_max > UINT64_MAX - static_cast<uint64_t>(n) - 7) {
    return 0;
  }
  const size_t hlen = n + static_cast<size_t>(high_max);
  const char* hbmp = r + 17;
  const char* lowbits = hbmp + ((hlen + 7) / 8);
  // Region clamp: a hostile header must not walk SelectBit / the
  // low-bit reader past the region.
  if (region_end != nullptr &&
      lowbits + ((static_cast<size_t>(idx) * static_cast<size_t>(l) +
                  static_cast<size_t>(l) + 7) /
                 8) >
          region_end) {
    return 0;
  }
  const uint64_t low = UnpackBitAt(
      lowbits, static_cast<uint64_t>(idx) * static_cast<uint64_t>(l), l);
  // The select directory (when the chunk built one) skips the from-word-0
  // bitmap scan; both arms answer identically.
  const uint64_t pos = (ef_dir != nullptr)
                           ? SelectBitDir(hbmp, hlen, idx, ef_dir)
                           : SelectBit(hbmp, hlen, idx);
  const uint64_t h = pos - idx;
  // l == 64 never occurs from the encoder, but a hostile region can
  // carry it: `h << 64` is UB (and the bulk decoder below yields 0 for
  // the high part). Guard the scalar path identically.
  const uint64_t hpart = (l >= 64) ? 0 : (h << l);
  return static_cast<int64_t>(static_cast<uint64_t>(base_v) + (hpart | low));
}

// Piecewise-linear exact model (kPla) region: segment table + kArith's
// exception envelope. Shared by the scalar/bulk/predicate/SUM paths.

// Full structural parse for the BATCH paths: extent bounds, footer param
// agreement, and ends that strictly increase and tile [0, n]. The scalar
// Int64RegionAt arm re-checks the fields it trusts with O(1) clamps instead
// of paying this O(nseg) walk per probe.
bool ParsePlaRegion(const char* r, uint32_t n, uint32_t param,
                    const char* region_end, PlaRegion* out) {
  if (r == nullptr || region_end == nullptr || region_end < r ||
      static_cast<size_t>(region_end - r) < 9) {
    return false;
  }
  const uint32_t nseg = ReadBE16(r);
  if (nseg == 0 || nseg != param || nseg > n) {
    return false;
  }
  if (static_cast<size_t>(region_end - r) <
      9U + (static_cast<size_t>(nseg) * 20U)) {
    return false;
  }
  out->nseg = nseg;
  out->segs = r + 2;
  out->hdr = out->segs + (static_cast<size_t>(nseg) * 20);
  out->bmp = out->hdr + 7;
  const uint32_t nexc = ReadBE16(out->hdr + 5);
  if (nexc > n ||
      (nexc > 0 && !InnerSchemeOk(static_cast<uint8_t>(out->hdr[0])))) {
    return false;
  }
  if (static_cast<size_t>(region_end - out->bmp) <
      (static_cast<size_t>(n) + 7) / 8) {
    return false;
  }
  uint32_t prev = 0;
  for (uint32_t j = 0; j < nseg; ++j) {
    const uint32_t e = ReadBE32(out->segs + (static_cast<size_t>(j) * 20) + 16);
    if (e <= prev || e > n) {
      return false;
    }
    prev = e;
  }
  return prev == n;
}

// Segment line value at absolute row `idx`; caller has validated the region
// (ParsePlaRegion) and that start <= idx < end.
int64_t PlaSegmentValueAt(const PlaRegion& p, uint32_t j, uint32_t start,
                          uint32_t idx) {
  const uint64_t first = ReadBE64(p.segs + (static_cast<size_t>(j) * 20));
  const uint64_t d = ReadBE64(p.segs + (static_cast<size_t>(j) * 20) + 8);
  return static_cast<int64_t>(first + (static_cast<uint64_t>(idx - start) * d));
}

// ---- Simple-8b (scheme 48) shared helpers ----------------------------------
// Canonical Anh & Moffat 2005 selector table for 64-bit words (4-bit selector
// + 60-bit payload): index = selector, value = {bits per offset, offsets per
// word}. Selector 0/1 are the two zero-width entries (240/120 copies of the
// base value); 7x8 and 8x7 are the two under-full entries the original table
// keeps so every width below 10 has a word shape.

// Per-value predicate test for the SIMPLE-8B folds. Mirrors ScanMatch; kept
// next to the verdict helper because the region kernels live before
// ScanMatch's definition site in this file.
bool Simple8bPasses(int64_t v, ScanOp op, int64_t lit) {
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

// Word-level predicate verdict: the word's offsets lie in [0, mask], so its
// possible values form the u64 interval [base, base+mask]. Projected to the
// SIGNED order that ScanMatch uses, that interval is the contiguous range
// [smin, smax] -- except where it crosses the u64/signed boundary or wraps
// past 2^64, where the projection spans everything (monotone superset spans
// INT64_MIN..INT64_MAX and no word-level pruning is sound). Monotone
// predicates then resolve to all/none from the span endpoints alone; kEq
// with bits > 0 always decodes. `kAll` callers still need the values for
// SUM (kAll with bits > 0 does not know Sigma offset); the count-only path
// folds kAll as `count += take`.

Simple8bVerdict Simple8bWordVerdict(uint32_t bits, uint64_t base_u, int64_t lit,
                                    ScanOp op) {
  const uint64_t mask = bits == 0 ? 0 : ((UINT64_C(1) << bits) - 1);
  const uint64_t lo_u = base_u;
  const uint64_t hi_u = base_u + mask;  // may wrap the u64 circle
  int64_t smin = 0;
  int64_t smax = 0;
  if (hi_u >= lo_u) {
    // Unwrapped interval [lo_u, hi_u]. The u64 -> signed projection
    // preserves order when the interval is entirely positive or entirely
    // negative (same branch body either way).
    if (hi_u < UINT64_C(0x8000000000000000) ||
        lo_u >= UINT64_C(0x8000000000000000)) {
      smin = static_cast<int64_t>(lo_u);
      smax = static_cast<int64_t>(hi_u);
    } else {
      // Crosses the u64 2^63 boundary: the signed projection spans the
      // whole range.
      smin = INT64_MIN;
      smax = INT64_MAX;
    }
  } else {
    // Wrapped: covers u64 0 and 2^64-1, i.e. the full signed range.
    smin = INT64_MIN;
    smax = INT64_MAX;
  }
  if (bits == 0) {
    // All offsets are 0: the word's values are all exactly the base.
    const auto base_s = static_cast<int64_t>(base_u);
    return base_s == lit ? Simple8bVerdict::kAll : Simple8bVerdict::kNone;
  }
  switch (op) {
    case ScanOp::kEq:
      // Values are a subset of [smin, smax]; outside the span no row can
      // match, inside it the word must be decoded to count occurrences.
      return (lit >= smin && lit <= smax) ? Simple8bVerdict::kSome
                                          : Simple8bVerdict::kNone;
    case ScanOp::kLe:
      if (smax <= lit) {
        return Simple8bVerdict::kAll;
      }
      if (smin > lit) {
        return Simple8bVerdict::kNone;
      }
      return Simple8bVerdict::kSome;
    case ScanOp::kLt:
      if (smax < lit) {
        return Simple8bVerdict::kAll;
      }
      if (smin >= lit) {
        return Simple8bVerdict::kNone;
      }
      return Simple8bVerdict::kSome;
    case ScanOp::kGe:
      if (smin >= lit) {
        return Simple8bVerdict::kAll;
      }
      if (smax < lit) {
        return Simple8bVerdict::kNone;
      }
      return Simple8bVerdict::kSome;
    case ScanOp::kGt:
      if (smin > lit) {
        return Simple8bVerdict::kAll;
      }
      if (smax <= lit) {
        return Simple8bVerdict::kNone;
      }
      return Simple8bVerdict::kSome;
  }
  return Simple8bVerdict::kSome;
}

// Canonical Int64RegionAt kArith arm, split out for the row path (lockstep
// with the switch arm: exception rank + inner-region recursion included).
int64_t Int64AtArith(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                     const char* region_end, const uint32_t* /*unused*/) {
  // [a i64][d i64][ischeme u8][iparam u32][nexc u16]
  // [bitmap ceil(n/8) B MSB-first][inner region of nexc exceptions].
  if (region_end != nullptr && r + 23 > region_end) {
    return 0;
  }
  const uint64_t a = ReadBE64(r);
  const uint64_t d = ReadBE64(r + 8);
  const auto ischeme = static_cast<uint8_t>(r[16]);
  const uint32_t iparam = ReadBE32(r + 17);
  const uint32_t nexc = ReadBE16(r + 21);
  const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
  if (region_end != nullptr && r + 23 + bmp_bytes > region_end) {
    return 0;
  }
  if (nexc != param || (nexc > 0 && !InnerSchemeOk(ischeme))) {
    return 0;
  }
  const auto bit = static_cast<uint8_t>(
      (static_cast<uint8_t>(r[23 + (idx >> 3)]) >> (7 - (idx & 7))) & 1);
  if (bit != 0) {
    return static_cast<int64_t>(a + (static_cast<uint64_t>(idx) * d));
  }
  if (nexc == 0) {
    return 0;  // clear bit in a pure ramp is inconsistent
  }
  const auto* bmp = reinterpret_cast<const uint8_t*>(r + 23);
  uint32_t set_before = 0;
  const size_t bfull = idx / 8;
  for (size_t w = 0; w < bfull; ++w) {
    set_before += static_cast<uint32_t>(__builtin_popcount(bmp[w]));
  }
  for (uint32_t b = 0; b < idx % 8; ++b) {
    set_before += static_cast<uint32_t>((bmp[bfull] >> (7 - b)) & 1);
  }
  const uint32_t eidx = idx - set_before;
  if (eidx >= nexc) {
    return 0;
  }
  const char* inner = r + 23 + bmp_bytes;
  return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc, eidx,
                       region_end, nullptr);
}

// Canonical Int64RegionAt kPla arm, split out for the row path (lockstep
// with the switch arm: O(1) header bounds + segment binary search +
// bitmap/exception handling included).
int64_t Int64AtPla(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                   const char* region_end, const uint32_t* /*unused*/) {
  // [nseg u16][nseg x (first 8B, d 8B, end 4B)][ischeme u8][iparam u32]
  // [nexc u16][bitmap ceil(n/8)][inner region of nexc exceptions].
  if (region_end != nullptr && r + 2 > region_end) {
    return 0;
  }
  const uint32_t nseg = ReadBE16(r);
  if (nseg == 0 || nseg != param) {
    return 0;
  }
  if (region_end != nullptr && static_cast<size_t>(region_end - r) <
                                   9U + (static_cast<size_t>(nseg) * 20U)) {
    return 0;
  }
  const char* segs = r + 2;
  const char* hdr = segs + (static_cast<size_t>(nseg) * 20);
  const auto ischeme = static_cast<uint8_t>(hdr[0]);
  const uint32_t iparam = ReadBE32(hdr + 1);
  const uint32_t nexc = ReadBE16(hdr + 5);
  if (nexc != 0 && !InnerSchemeOk(ischeme)) {
    return 0;
  }
  // First segment whose exclusive end is past idx.
  uint32_t lo = 0;
  uint32_t hi = nseg;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (ReadBE32(segs + (static_cast<size_t>(mid) * 20) + 16) <= idx) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo >= nseg) {
    return 0;
  }
  const uint32_t start =
      lo == 0 ? 0 : ReadBE32(segs + (static_cast<size_t>(lo - 1) * 20) + 16);
  if (idx < start) {
    return 0;  // hostile ends regression
  }
  const size_t bmp_off = 9U + (static_cast<size_t>(nseg) * 20U);
  if (region_end != nullptr &&
      r + bmp_off + ((static_cast<size_t>(n) + 7) / 8) > region_end) {
    return 0;
  }
  const auto bit = static_cast<uint8_t>(
      (static_cast<uint8_t>(r[bmp_off + (idx >> 3)]) >> (7 - (idx & 7))) & 1);
  if (bit != 0) {
    return PlaSegmentValueAt(
        {.nseg = nseg, .segs = segs, .hdr = hdr, .bmp = r + bmp_off}, lo, start,
        idx);
  }
  if (nexc == 0) {
    return 0;  // clear bit in a pure model is inconsistent
  }
  // Exception rank = clear bits before idx (kArith's popcount walk).
  const auto* bmp = reinterpret_cast<const uint8_t*>(r + bmp_off);
  uint32_t set_before = 0;
  const size_t bfull = idx / 8;
  for (size_t w = 0; w < bfull; ++w) {
    set_before += static_cast<uint32_t>(__builtin_popcount(bmp[w]));
  }
  for (uint32_t b = 0; b < idx % 8; ++b) {
    set_before += static_cast<uint32_t>((bmp[bfull] >> (7 - b)) & 1);
  }
  const uint32_t eidx = idx - set_before;
  if (eidx >= nexc) {
    return 0;  // bitmap over-counts exceptions
  }
  const char* inner = r + bmp_off + ((static_cast<size_t>(n) + 7) / 8);
  return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc, eidx,
                       region_end, nullptr);
}

// ---- Modular affine model (kMod, scheme 58) shared helpers ------------------
//
// The region is an exact arithmetic progression in Z/mZ over its on-model
// rows. Everything below works on the parsed view; the hostile-input clamps
// mirror the kPla arm (O(1) header bounds in the scalar path, full parse in
// the batch paths).

// Full structural parse for the BATCH paths: extent bounds, footer param
// agreement, m range, and the exception envelope. The scalar Int64RegionAt
// arm re-checks the fields it trusts with O(1) clamps instead of paying this
// per probe.
bool ParseModRegion(const char* r, uint32_t n, uint32_t param,
                    const char* region_end, ModRegion* out) {
  if (r == nullptr || region_end == nullptr || region_end < r ||
      static_cast<size_t>(region_end - r) < 31) {
    return false;
  }
  out->a = ReadBE64(r);
  out->b = ReadBE64(r + 8);
  out->m = ReadBE64(r + 16);
  // The model arithmetic needs m in [2, 2^63]: residues are a mod-add apart
  // (a single conditional subtract per step) and the u128 products stay
  // under 2^74. m == 1 would make every residue 0 (kConst's niche) and
  // m >= 2^63 is rejected at encode time too.
  if (out->m < 2 || out->m > (UINT64_C(1) << 63) || out->a >= out->m ||
      out->b >= out->m || out->b == 0) {
    return false;
  }
  out->hdr = r + 24;
  out->bmp = out->hdr + 7;
  const uint32_t nexc = ReadBE16(out->hdr + 5);
  if (nexc != param || nexc > n ||
      (nexc > 0 && !InnerSchemeOk(static_cast<uint8_t>(out->hdr[0])))) {
    return false;
  }
  return static_cast<size_t>(region_end - out->bmp) >=
         (static_cast<size_t>(n) + 7) / 8;
}

// On-model value at absolute row `idx`; caller has validated the region and
// idx < n. One u128 mulmod -- b*idx < 2^63 * 2^10 = 2^73 fits comfortably.
int64_t ModValueAt(const ModRegion& p, uint32_t idx) {
  const unsigned __int128 t = static_cast<unsigned __int128>(p.b) *
                              static_cast<unsigned __int128>(idx) %
                              static_cast<unsigned __int128>(p.m);
  const uint64_t v = p.a + static_cast<uint64_t>(t);
  return static_cast<int64_t>(v >= p.m ? v - p.m : v);
}

// Sum_{0 <= i < n} floor((a + b*i) / m) over the non-negative integers, by
// the Euclidean descent (ACL's floor_sum shape: reduce the slope/intercept,
// then recurse on the crossing count with (slope, modulus) swapped). Every
// quantity stays under 2^82 for the region sizes this file calls it with
// (n <= 1024, a,b < m <= 2^63), so unsigned __int128 cannot overflow.
// (`a` is the intercept, `b` the slope.)
unsigned __int128 ModFloorSum(unsigned __int128 n, unsigned __int128 a,
                              unsigned __int128 b, unsigned __int128 m) {
  unsigned __int128 ans = 0;
  while (true) {
    if (b >= m) {
      ans += (n / 2) * (n - 1) * (b / m);
      b %= m;
    }
    if (a >= m) {
      ans += n * (a / m);
      a %= m;
    }
    // Now a < m and b < m: the largest numerator is b*(n-1) + a.
    const unsigned __int128 y_max = (b * n) + a;
    if (y_max < m) {
      break;
    }
    n = y_max / m;    // rows past the first crossing keep contributing
    a = y_max % m;    // the new intercept
    std::swap(b, m);  // the new slope is the old modulus
  }
  return ans;
}

// gcd over u64 (Euclid); m >= 2 so the result is >= 1.
uint64_t ModGcd(uint64_t x, uint64_t y) {
  while (y != 0) {
    const uint64_t t = x % y;
    x = y;
    y = t;
  }
  return x;
}

// Modular inverse of u mod q (q >= 1, gcd(u, q) == 1), extended Euclid over
// signed __int128 to dodge the u64 overflow in the coefficient products.
uint64_t ModInv(uint64_t u, uint64_t q) {
  if (q == 1) {
    return 0;
  }
  __int128 t = 0;
  __int128 newt = 1;
  auto r = static_cast<__int128>(q);
  auto newr = static_cast<__int128>(u % q);
  while (newr != 0) {
    const __int128 quotient = r / newr;
    const __int128 tmp = t - (quotient * newt);
    t = newt;
    newt = tmp;
    const __int128 rm = r - (quotient * newr);
    r = newr;
    newr = rm;
  }
  const __int128 res = t % static_cast<__int128>(q);
  return static_cast<uint64_t>(res < 0 ? res + static_cast<__int128>(q) : res);
}

// Canonical Int64RegionAt kMod arm, split out for the row path (lockstep
// with the switch arm: O(1) header bounds + one mulmod + bitmap/exception
// handling included).
int64_t Int64AtMod(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                   const char* region_end, const uint32_t* /*unused*/) {
  // [a 8B][b 8B][m 8B][ischeme u8][iparam u32][nexc u16]
  // [bitmap ceil(n/8) B MSB-first][inner region of nexc exceptions].
  if (region_end != nullptr && r + 31 > region_end) {
    return 0;
  }
  ModRegion p;
  p.a = ReadBE64(r);
  p.b = ReadBE64(r + 8);
  p.m = ReadBE64(r + 16);
  if (p.m < 2 || p.m > (UINT64_C(1) << 63) || p.a >= p.m || p.b >= p.m ||
      p.b == 0) {
    return 0;
  }
  const auto ischeme = static_cast<uint8_t>(r[24]);
  const uint32_t iparam = ReadBE32(r + 25);
  const uint32_t nexc = ReadBE16(r + 29);
  const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
  if (region_end != nullptr && r + 31 + bmp_bytes > region_end) {
    return 0;
  }
  if (nexc != param || (nexc > 0 && !InnerSchemeOk(ischeme))) {
    return 0;
  }
  const auto bit = static_cast<uint8_t>(
      (static_cast<uint8_t>(r[31 + (idx >> 3)]) >> (7 - (idx & 7))) & 1);
  if (bit != 0) {
    return ModValueAt(p, idx);
  }
  if (nexc == 0) {
    return 0;  // clear bit in a pure model is inconsistent
  }
  const auto* bmp = reinterpret_cast<const uint8_t*>(r + 31);
  uint32_t set_before = 0;
  const size_t bfull = idx / 8;
  for (size_t w = 0; w < bfull; ++w) {
    set_before += static_cast<uint32_t>(__builtin_popcount(bmp[w]));
  }
  for (uint32_t b = 0; b < idx % 8; ++b) {
    set_before += static_cast<uint32_t>((bmp[bfull] >> (7 - b)) & 1);
  }
  const uint32_t eidx = idx - set_before;
  if (eidx >= nexc) {
    return 0;
  }
  const char* inner = r + 31 + bmp_bytes;
  return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc, eidx,
                       region_end, nullptr);
}

// ---- Interleaved arithmetic lanes (kLane, scheme 62) shared helpers --------
//
// The region is K independent arithmetic progressions interleaved row-major:
// row i belongs to lane i % K at tick i / K. Same envelope discipline as
// kMod: O(1) header bounds in the scalar path, full parse in the batch
// paths, hostile-input clamps mirroring the kPla arm.

size_t LaneHeaderBytes(uint32_t nd) {
  return 8 + (static_cast<size_t>(nd) * 16);
}

// Full structural parse for the BATCH paths: extent bounds, footer param
// agreement, the nd range, and the exception envelope. The scalar
// Int64RegionAt arm re-checks the fields it trusts with O(1) clamps instead
// of paying this per probe.
bool ParseLaneRegion(const char* r, uint32_t n, uint32_t param,
                     const char* region_end, LaneRegion* out) {
  if (r == nullptr || region_end == nullptr || region_end < r ||
      static_cast<size_t>(region_end - r) < 40) {
    return false;
  }
  const uint32_t nd = static_cast<uint8_t>(r[0]);
  if (nd < 2 || nd > 16) {
    return false;
  }
  out->nd = nd;
  out->base = r + 1;
  const size_t hdr = LaneHeaderBytes(nd);
  out->ischeme = static_cast<uint8_t>(r[hdr - 7]);
  out->iparam = ReadBE32(r + hdr - 6);
  out->nexc = ReadBE16(r + hdr - 2);
  if (out->nexc != param || out->nexc > n ||
      (out->nexc > 0 && !InnerSchemeOk(out->ischeme))) {
    return false;
  }
  out->bmp = r + hdr;
  return static_cast<size_t>(region_end - out->bmp) >=
         (static_cast<size_t>(n) + 7) / 8;
}

// On-model value: lane idx % nd at tick idx / nd, wrapped arithmetic (the
// u64 multiply-add is bit-exact for every int64 input, the kArith/kMod
// contract).
int64_t LaneValueAt(const LaneRegion& p, uint32_t idx) {
  const size_t off = static_cast<size_t>(idx % p.nd) * 16;
  const uint64_t v =
      ReadBE64(p.base + off) + (ReadBE64(p.base + off + 8) * (idx / p.nd));
  return static_cast<int64_t>(v);
}

// Canonical Int64RegionAt kLane arm, split out for the row path (lockstep
// with the switch arm: O(1) header bounds + one lane model eval on the
// on-model bit + kArith's popcount exception rank otherwise).
int64_t Int64AtLane(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                    const char* region_end, const uint32_t* /*unused*/) {
  // [nd u8][nd x (base 8B][step 8B)][ischeme u8][iparam u32][nexc u16]
  // [bitmap ceil(n/8) B MSB-first][inner region of nexc exceptions].
  if (region_end != nullptr && r + 40 > region_end) {
    return 0;
  }
  LaneRegion p;
  p.nd = static_cast<uint8_t>(r[0]);
  if (p.nd < 2 || p.nd > 16) {
    return 0;
  }
  p.base = r + 1;
  const size_t hdr = LaneHeaderBytes(p.nd);
  p.ischeme = static_cast<uint8_t>(r[hdr - 7]);
  p.iparam = ReadBE32(r + hdr - 6);
  p.nexc = ReadBE16(r + hdr - 2);
  const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
  if (region_end != nullptr && r + hdr + bmp_bytes > region_end) {
    return 0;
  }
  if (p.nexc != param || (p.nexc > 0 && !InnerSchemeOk(p.ischeme))) {
    return 0;
  }
  const auto bit = static_cast<uint8_t>(
      (static_cast<uint8_t>(r[hdr + (idx >> 3)]) >> (7 - (idx & 7))) & 1);
  if (bit != 0) {
    return LaneValueAt(p, idx);
  }
  if (p.nexc == 0) {
    return 0;  // clear bit in a pure model is inconsistent
  }
  const auto* bmp = reinterpret_cast<const uint8_t*>(r + hdr);
  uint32_t set_before = 0;
  const size_t bfull = idx / 8;
  for (size_t w = 0; w < bfull; ++w) {
    set_before += static_cast<uint32_t>(__builtin_popcount(bmp[w]));
  }
  for (uint32_t b = 0; b < idx % 8; ++b) {
    set_before += static_cast<uint32_t>((bmp[bfull] >> (7 - b)) & 1);
  }
  const uint32_t eidx = idx - set_before;
  if (eidx >= p.nexc) {
    return 0;
  }
  const char* inner = r + hdr + bmp_bytes;
  return Int64RegionAt(inner, static_cast<Scheme>(p.ischeme), p.iparam, p.nexc,
                       eidx, region_end, nullptr);
}

// ---- Tile dictionary (kTileDict, scheme 59) shared helpers -----------------

// Sequential tile cursor. The region is a tile chain with no header, so the
// ONLY way to place tile t is to walk tiles 0..t-1 (<= 16 per full row
// group); every step bounds its reads against `end` so a hostile footer's
// truncated chain fails closed instead of dereferencing past the cell.

int TileDictWidth(uint32_t d) { return BitsNeeded(d - 1); }

bool TileDictParseTile(const char* p, const char* end, uint32_t n,
                       uint32_t tile_start, TileDictCursor* c) {
  // end == nullptr follows the Int64RegionAt convention for pre-validated
  // regions (ByteSliceNullAwareCount's fallback): the walk stays bounded by
  // n (tiles are non-empty, <= ceil(n/64) steps) and every READ is bounded
  // by the parsed d/rows, only the end-vs-region check is dropped.
  if (end != nullptr && p + 1 > end) {
    return false;
  }
  const uint32_t d = static_cast<uint8_t>(p[0]);
  if (d == 0 || d > kTileDictMaxD) {
    return false;
  }
  const int w = TileDictWidth(d);
  const uint32_t rows =
      (n - tile_start < kTileDictRows) ? n - tile_start : kTileDictRows;
  const size_t codes_bytes =
      (static_cast<size_t>(rows) * static_cast<size_t>(w) + 7) / 8;
  const char* dict = p + 1;
  const char* codes = dict + (static_cast<size_t>(d) * 8);
  if (end != nullptr &&
      (codes > end || std::cmp_greater(codes_bytes, end - codes))) {
    return false;
  }
  c->p = p;
  c->end = end;
  c->n = n;
  c->tile_start = tile_start;
  c->d = d;
  c->w = w;
  c->rows = rows;
  c->dict = dict;
  c->codes = codes;
  c->codes_bytes = codes_bytes;
  return true;
}

bool TileDictFirst(const char* r, uint32_t n, const char* end,
                   TileDictCursor* c) {
  if (n == 0) {
    return false;
  }
  return TileDictParseTile(r, end, n, 0, c);
}

bool TileDictNext(TileDictCursor* c) {
  const uint32_t next_start = c->tile_start + c->rows;
  if (next_start >= c->n) {
    return false;
  }
  const char* p = c->codes + c->codes_bytes;
  return TileDictParseTile(p, c->end, c->n, next_start, c);
}

// Places the cursor on the tile holding row `idx` (caller checked idx < n).
bool TileDictSeek(const char* r, uint32_t n, const char* end, uint32_t idx,
                  TileDictCursor* c) {
  if (!TileDictFirst(r, n, end, c)) {
    return false;
  }
  while (idx >= c->tile_start + c->rows) {
    if (!TileDictNext(c)) {
      return false;
    }
  }
  return true;
}

int64_t TileDictValueAt(const TileDictCursor& c, uint32_t local_row) {
  const uint64_t code = c.w == 0
                            ? 0
                            : UnpackBitAt(c.codes,
                                          static_cast<uint64_t>(local_row) *
                                              static_cast<uint64_t>(c.w),
                                          c.w);
  if (code >= c.d) {
    return 0;  // clamp bucket: hostile codes decode as raw 0
  }
  return static_cast<int64_t>(
      ReadBE64(c.dict + (static_cast<size_t>(code) * 8)));
}

// Row-reassembly dispatch (ReadRowIntoImpl): a scan's per-column scheme is
// loop-invariant, so this compare chain predicts perfectly while the 40-way
// jump table inside Int64RegionAt mispredicts on every column alternation.
int64_t Int64AtFast(const char* r, Scheme s, uint32_t param, uint32_t n,
                    uint32_t idx, const char* region_end,
                    const uint32_t* ef_dir) {
  if (UNLIKELY(n == 0 || idx >= n)) {
    return 0;  // canonical entry guard
  }
  switch (s) {
    case Scheme::kEliasFano:
      return Int64AtEliasFano(r, param, n, idx, region_end, ef_dir);
    case Scheme::kBitWeave:
      return Int64AtBitWeave(r, param, n, idx, region_end, ef_dir);
    case Scheme::kS4: {
      // One checkpoint load + one bit extraction. The header parse bounds
      // both the checkpoint table and the packed extent against region_end
      // before either is touched, so a hostile n/param cannot walk off the
      // region.
      S4Region p;
      if (!ParseS4Region(r, n, param, region_end, &p)) {
        return 0;
      }
      return S4ValueAt(p, idx);
    }
    case Scheme::kPlain:
      return Int64AtPlain(r, param, n, idx, region_end, ef_dir);
    case Scheme::kForBp:
      return Int64AtForBp(r, param, n, idx, region_end, ef_dir);
    case Scheme::kConst:
      return Int64AtConst(r, param, n, idx, region_end, ef_dir);
    case Scheme::kArith:
      return Int64AtArith(r, param, n, idx, region_end, ef_dir);
    case Scheme::kPla:
      return Int64AtPla(r, param, n, idx, region_end, ef_dir);
    case Scheme::kMod:
      return Int64AtMod(r, param, n, idx, region_end, ef_dir);
    case Scheme::kLane:
      return Int64AtLane(r, param, n, idx, region_end, ef_dir);
    default:
      return Int64RegionAt(r, s, param, n, idx, region_end, ef_dir);
  }
}

int64_t Int64RegionAt(const char* r, Scheme s, uint32_t param, uint32_t n,
                      uint32_t idx, const char* region_end,
                      const uint32_t* ef_dir) {
  // Every scheme below indexes an n-row payload; an idx past the end is a
  // caller bug or hostile metadata. Fail closed instead of letting the
  // per-scheme extent checks be the only defense.
  if (n == 0 || idx >= n) {
    return 0;
  }
  switch (s) {
    case Scheme::kPlain:
      // region_end may be null only for callers that validated the region
      // already; with a bound, reject a cell whose 8B payload extends past
      // it (a hand-built footer can claim a 4B varchar-floor region).
      if (region_end != nullptr &&
          r + (static_cast<size_t>(idx) * 8) + 8 > region_end) {
        return 0;
      }
      return static_cast<int64_t>(ReadBE64(r + (static_cast<size_t>(idx) * 8)));
    case Scheme::kConst:
      if (region_end != nullptr && r + 8 > region_end) {
        return 0;
      }
      return static_cast<int64_t>(ReadBE64(r));
    case Scheme::kRle: {
      const uint32_t runs = param;
      // The run walk reads 12B per run; a hostile param (up to 2^32-1)
      // used to march gigabytes past the region even though region_end was
      // in hand (callers relied on a clamp that did not exist).
      if (region_end != nullptr &&
          r + 4 + (static_cast<size_t>(runs) * 12) > region_end) {
        return 0;
      }
      uint64_t acc = 0;
      const char* p = r + 4;
      for (uint32_t i = 0; i < runs; ++i) {
        const auto v = static_cast<int64_t>(ReadBE64(p));
        // u64 extent math: a hostile len (e.g. 0xFFFFFFFF after a short
        // run) wrapped the uint32 accumulator, regressed the walk and
        // returned 0 instead of the run's value (fixed the same way in
        // ReduceColumn / TryFastPredicateMask / GroupBySumColumn).
        const uint64_t len = ReadBE32(p + 8);
        if (static_cast<uint64_t>(idx) < acc + len) {
          return v;
        }
        acc += len;
        p += 12;
      }
      return 0;
    }
    case Scheme::kRleBp: {
      RleBpRegion p;
      if (!ParseRleBpRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      uint64_t row = 0;
      for (uint32_t run = 0; run < p.runs; ++run) {
        const uint64_t length = RleBpLengthAt(p, run);
        if (idx < row + length) {
          return RleBpValueAt(p, run);
        }
        row += length;
      }
      return 0;
    }
    case Scheme::kRleIdx: {
      // Restart-indexed: the run holding `idx` is a binary search over the
      // table plus a <= 15-length walk, so a probe costs O(log runs) instead
      // of the O(runs) walk every other run scheme pays.
      RleIdxRegion p;
      if (!ParseRleIdxRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      const uint32_t run = RleIdxRunAt(p, idx);
      if (run >= p.runs) {
        return 0;
      }
      return RleIdxValueAt(p, run);
    }
    case Scheme::kStreamVByte: {
      // Byte-addressable: the payload offset is one control-byte load plus at
      // most three width adds, so this is O(1) like kForBp / kEliasFano and
      // unlike the kSimple8b or run-family probes, which replay from the head.
      StreamVByteRegion p;
      if (!ParseStreamVByteRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return StreamVByteValueAt(p, idx);
    }
    case Scheme::kSimdFor: {
      SimdForRegion p{};
      if (!ParseSimdForRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return SimdForValueAt(p, idx);
    }
    case Scheme::kS4: {
      // One checkpoint load + at most kS4Block-1 replayed deltas. The header
      // parse bounds the width/lo/checkpoint tables and the packed extent
      // against region_end before either is touched, so a hostile n cannot
      // walk off the region (the same shape as the kS4 arm above).
      S4Region p;
      if (!ParseS4Region(r, n, param, region_end, &p)) {
        return 0;
      }
      return S4ValueAt(p, idx);
    }
    case Scheme::kSfor: {
      // One directory bisect + one width unpack (the enum comment explains
      // why there is no RegionLowerBound arm: that API is int64-key, and the
      // raw-bit order this scheme bisects is only value order under the
      // non-negative gate).
      SforRegion p;
      if (!ParseSforRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return SforValueAt(p, idx);
    }
    case Scheme::kExpFor: {
      // Same probe shape as kSfor: one row_start bisect plus one width
      // unpack, with the total-order transform inverted at the end so the
      // returned cell is the raw bitcast bits (the convention every double
      // codec hands the int64 machinery).
      ExpForRegion p;
      if (!ParseExpForRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return ExpForValueAt(p, idx);
    }
    case Scheme::kD2Pfor: {
      // Checkpoint jump + at most kDeltaCheckpointEvery inner second-difference
      // reads, the same probe shape as kDeltaFor, priced at the same tier.
      D2PforRegion p;
      if (!ParseD2PforRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return D2PforValueAt(p, idx);
    }
    case Scheme::kHuffDict: {
      // Checkpoint seek + at most kHuffDictCheckpoint canonical replays. The
      // probe rebuilds the (len, sym) tables per call -- d <= 1024 makes that
      // a few hundred ns, the price of leaving the region cache-free.
      HuffDictRegion p;
      if (!ParseHuffDictRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return HuffDictValueAt(p, idx);
    }
    case Scheme::kRle8:
      // The ~96KB parsed region stays inside Rle8ValueAtParsed's frame;
      // materializing it here would grow the whole 40-way dispatcher (see
      // the wrappers above ParseRle8Region's users).
      return Rle8ValueAtParsed(r, n, param, region_end, idx);
    case Scheme::kRleDelta: {
      RleDeltaRegion p;
      if (!ParseRleDeltaRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return RleDeltaValueAt(p, idx);
    }
    case Scheme::kForBp: {
      // The offset assembly below shifts by b*8 for b < param: param past
      // 8 (the validator's [1,8] cap) is hostile-metadata-only territory
      // and shifts by >= 64 is UB. Fail closed (the generic decode treats
      // the cell as 0, matching other out-of-contract regions).
      if (param == 0 || param > 8) {
        return 0;
      }
      if (region_end != nullptr &&
          r + 8 + ((static_cast<size_t>(idx) + 1) * param) > region_end) {
        return 0;
      }
      const auto base_v = static_cast<int64_t>(ReadBE64(r));
      const auto* vp = reinterpret_cast<const unsigned char*>(
          r + 8 + (static_cast<size_t>(idx) * param));
      uint64_t off = 0;
      if (region_end != nullptr &&
          vp + 8 <= reinterpret_cast<const unsigned char*>(region_end)) {
        // One masked u64 load replaces the byte-wise assembly; the window
        // provably stays inside the region (else the byte loop below runs).
        uint64_t raw = 0;
        std::memcpy(&raw, vp, 8);
        off = raw & (param >= 8 ? ~0ULL : ((1ULL << (8U * param)) - 1));
      } else {
        for (uint32_t b = 0; b < param; ++b) {
          off |= static_cast<uint64_t>(vp[b]) << (b * 8U);
        }
      }
      // Two's-complement wrap: base + offset legitimately wraps for extreme
      // bases; signed + would be UB (UBSan traps on [INT64_MIN, x] data).
      return static_cast<int64_t>(static_cast<uint64_t>(base_v) + off);
    }
    case Scheme::kDict:
    case Scheme::kOpDict: {
      // kOpDict shares kDict's byte layout (its dictionary is merely sorted
      // and codes are ranks), so random access is the identical walk.
      // The header bytes and the packed-code extent must be inside the
      // region BEFORE UnpackBitAt: a hostile ncodes (up to 2^16-1) derived
      // cw up to 16 and read ~2KB of "codes" out of a 4-byte region. One
      // gate over the full layout ([count 2B][packed codes][dict n*8B]
      // sized from the caller's n) covers header, codes, and dictionary.
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint16_t ncodes = ReadBE16(r);
      const int cw = BitsNeeded(ncodes > 0 ? ncodes - 1 : 0);
      // Layout: [count 2B][packed codes ceil(rows*cw/8) B][dict n x 8B].
      const size_t dict_base =
          2 + ((static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8);
      const char* dict = r + dict_base;
      // The validator's region_need floors the dict at 4B/entry, but an
      // int64 dict consumes 8B/entry: a region built exactly at the floor
      // (or a hand-built footer) would read code*8 bytes past the region
      // into the footer. Refuse instead of reading foreign bytes.
      if (region_end != nullptr &&
          dict + (static_cast<size_t>(ncodes) * sizeof(int64_t)) > region_end) {
        return 0;
      }
      const uint64_t code = UnpackBitAt(
          r + 2, static_cast<uint64_t>(idx) * static_cast<uint64_t>(cw), cw);
      // A cw-wide code can encode values past ncodes (cw rounds up): clamp.
      if (ncodes == 0 || code >= ncodes) {
        return 0;
      }
      return static_cast<int64_t>(
          ReadBE64(dict + (static_cast<size_t>(code) * sizeof(int64_t))));
    }
    case Scheme::kRleDict:
    case Scheme::kOpRle: {  // kRleDict layout; OpRle adds a sorted dict,
                            // which the decoder never notices.
      // [d u16][dict d x 8B][runs u32][{code cb B}{len u32}] x runs. The run
      // walk reads cb+4 bytes per run; a hostile param (up to 2^32-1) must
      // not march past the region (mirrors the kRle arm's gate).
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t d = ReadBE16(r);
      if (d == 0) {
        return 0;
      }
      const int cb = (BitsNeeded(d - 1) + 7) / 8;
      const char* runs_hdr = r + 2 + (static_cast<size_t>(d) * 8);
      if (region_end != nullptr && runs_hdr + 4 > region_end) {
        return 0;
      }
      const uint32_t runs = ReadBE32(runs_hdr);
      if (region_end != nullptr &&
          runs_hdr + 4 +
                  (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
              region_end) {
        return 0;
      }
      const char* dict = r + 2;
      const char* q = runs_hdr + 4;
      uint64_t acc = 0;
      for (uint32_t i = 0; i < runs; ++i) {
        uint64_t code = 0;
        for (int b = 0; b < cb; ++b) {
          code =
              (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
        }
        const int64_t v =
            (code >= d) ? 0  // clamp bucket decodes as raw 0 (mirrors kDict)
                        : static_cast<int64_t>(
                              ReadBE64(dict + (static_cast<size_t>(code) * 8)));
        const uint64_t len = ReadBE32(q + cb);
        if (static_cast<uint64_t>(idx) < acc + len) {
          return v;
        }
        acc += len;
        q += static_cast<size_t>(cb) + 4;
      }
      return 0;
    }
    case Scheme::kDeltaFp: {
      const uint32_t cks =
          (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery;
      const uint32_t ci = idx / kDeltaCheckpointEvery;
      // Checkpoint table must be inside the region before any read.
      if (region_end != nullptr &&
          r + 8 + (static_cast<size_t>(cks) * 8) > region_end) {
        return 0;
      }
      uint64_t v = ReadBE64(r + 8 + (static_cast<size_t>(ci) * 8));
      const uint32_t start = ci * kDeltaCheckpointEvery;
      const char* packed = r + 8 + (static_cast<size_t>(cks) * 8);
      // The walk reads bits [0, idx*param) past `packed` (deltas 1..idx,
      // deltas[0] is the checkpoint value), so the exact byte ceil of
      // idx*param bounds it. The former (idx+1)*param ceil rejected
      // exact-fit regions whenever a byte boundary fell in the last delta
      // (always for prm=8): the final row of a full group silently decoded
      // as 0. Mirrors the kXorFor bound below.
      if (region_end != nullptr &&
          packed + ((static_cast<size_t>(idx) * param + 7) / 8) > region_end) {
        return 0;
      }
      for (uint32_t t = start + 1; t <= idx; ++t) {
        v += UnpackBitAt(packed, static_cast<uint64_t>(t - 1) * param,
                         static_cast<int>(param));
      }
      return static_cast<int64_t>(v);
    }
    case Scheme::kBitmap: {
      if (region_end != nullptr &&
          r + 16 + (static_cast<size_t>(idx) >> 3) + 1 > region_end) {
        return 0;
      }
      const auto tv = static_cast<int64_t>(ReadBE64(r));
      const uint64_t bit = UnpackBitAt(r + 16, idx, 1);
      return bit != 0 ? tv : static_cast<int64_t>(ReadBE64(r + 8));
    }
    case Scheme::kWah: {
      // Walk words until the span holding `idx` (param = word count). Word
      // extent math is guarded for hostile regions: a fill length near 2^62
      // must not overflow 63*L before the coverage checks run.
      if (region_end != nullptr &&
          r + 16 + (static_cast<size_t>(param) * 8) > region_end) {
        return 0;
      }
      if (param == 0) {
        return 0;
      }
      const auto tv = static_cast<int64_t>(ReadBE64(r));
      const auto fv = static_cast<int64_t>(ReadBE64(r + 8));
      const char* wp = r + 16;
      uint64_t remaining = idx;
      for (uint32_t k = 0; k < param; ++k, wp += 8) {
        const uint64_t w = ReadBE64(wp);
        if ((w >> 63) == 0) {  // literal span of kWahSpan rows
          if (remaining < kWahSpan) {
            return ((w >> (kWahSpan - 1 - remaining)) & 1ULL) != 0 ? tv : fv;
          }
          remaining -= kWahSpan;
        } else {
          const uint64_t len = w & kWahLenMask;
          if (len > UINT64_MAX / kWahSpan) {
            return 0;
          }
          const uint64_t rows = len * kWahSpan;
          if (remaining < rows) {
            return ((w >> 62) & 1ULL) != 0 ? tv : fv;
          }
          remaining -= rows;
        }
      }
      return 0;  // idx beyond the encoded coverage
    }
    case Scheme::kXorFor: {
      if (region_end != nullptr && r + 8 > region_end) {
        return 0;
      }
      uint64_t x = ReadBE64(r);
      const char* packed = r + 8;
      // Bound the XOR-walk extent the same way as kDeltaFp above (exact
      // byte ceil of idx*param bits).
      if (region_end != nullptr &&
          packed + ((static_cast<size_t>(idx) * param + 7) / 8) > region_end) {
        return 0;
      }
      for (uint32_t t = 1; t <= idx; ++t) {
        x ^= UnpackBitAt(packed, static_cast<uint64_t>(t - 1) * param,
                         static_cast<int>(param));
      }
      return static_cast<int64_t>(x);
    }
    case Scheme::kBitWeave: {
      if (region_end != nullptr && r + 9 > region_end) {
        return 0;
      }
      const auto base_v = static_cast<int64_t>(ReadBE64(r));
      const int w = static_cast<int>(static_cast<uint8_t>(r[8]));
      // A hostile region can carry w > 64: UnpackBitAt would walk idx*w + w
      // bits deep past the region (reads ~32 bytes per value OOB). The
      // footer gate caps prm at 64 for validated chunks; clamp the header
      // byte too so unvalidated decoders stay memory-safe.
      if (w > 64) {
        return 0;
      }
      // Bound the packed-offset extent before UnpackBitAt: a hostile region
      // smaller than ceil((idx+1)*w/8) must not be read past.
      if (region_end != nullptr &&
          r + 9 +
                  ((static_cast<size_t>(idx) * static_cast<size_t>(w) +
                    static_cast<size_t>(w) + 7) /
                   8) >
              region_end) {
        return 0;
      }
      const uint64_t bit_off =
          static_cast<uint64_t>(idx) * static_cast<uint64_t>(w);
      const char* vp = r + 9 + (bit_off >> 3);
      const auto sh = static_cast<uint32_t>(bit_off & 7);
      uint64_t off = 0;
      if (sh + static_cast<uint32_t>(w) <= 64 && region_end != nullptr &&
          vp + 8 <= region_end) {
        // One aligned-window load replaces the byte-assembly: same MSB-first
        // extraction UnpackBitAt performs, without the per-byte loop.
        const uint64_t word = ReadBE64(vp);
        off = (word >> (64 - sh - static_cast<uint32_t>(w))) &
              (w == 64 ? ~0ULL : ((1ULL << w) - 1));
      } else {
        off = UnpackBitAt(r + 9, bit_off, w);
      }
      return static_cast<int64_t>(static_cast<uint64_t>(base_v) + off);
    }
    case Scheme::kSimple8b: {
      // Self-delimiting words: the selector fixes each word's value count,
      // so random access replays words from the region head (like the kRle
      // run walk; O(param)). The full region_need (8 + 8*param) bounds the
      // walk before it starts.
      if (param == 0 ||
          (region_end != nullptr &&
           r + 8 + (static_cast<size_t>(param) * 8) > region_end)) {
        return 0;
      }
      const uint64_t base_u = ReadBE64(r);
      uint64_t remaining = static_cast<uint64_t>(idx) + 1;
      const char* p = r + 8;
      for (uint32_t w = 0; w < param; ++w, p += 8) {
        const uint64_t word = ReadBE64(p);
        const auto sel = static_cast<uint32_t>(word >> 60);
        const uint32_t cnt = kSimple8bSelCount[sel];
        if (cnt >= remaining) {
          const uint32_t bits = kSimple8bSelBits[sel];
          if (bits == 0) {
            return static_cast<int64_t>(base_u);
          }
          const uint64_t off =
              (word >> (bits * (cnt - static_cast<uint32_t>(remaining)))) &
              ((UINT64_C(1) << bits) - 1);
          return static_cast<int64_t>(base_u + off);
        }
        remaining -= cnt;
      }
      return 0;  // words cover fewer rows than idx+1: fail closed
    }
    case Scheme::kPfor: {
      if (region_end != nullptr && r + 11 > region_end) {
        return 0;
      }
      const auto base_v = static_cast<int64_t>(ReadBE64(r));
      const int w = static_cast<int>(static_cast<uint8_t>(r[8]));
      if (w < 0 || w > 64) {
        return 0;
      }
      const uint32_t nexc = ReadBE16(r + 9);
      const char* packed = r + 11;
      const size_t pack_bytes =
          (static_cast<size_t>(n) * static_cast<size_t>(w) + 7) / 8;
      const char* exc = packed + pack_bytes;
      // The header-derived extent must stay inside the region: a hostile
      // nexc (up to 65535) would otherwise walk 10*nexc bytes past it.
      if (region_end != nullptr &&
          exc + (static_cast<size_t>(nexc) * 10) > region_end) {
        return 0;
      }
      // Exception list is sorted by position: linear scan (nexc is tiny).
      for (uint32_t k = 0; k < nexc; ++k) {
        const uint32_t pos = ReadBE16(exc + (static_cast<size_t>(k) * 10));
        if (pos == idx) {
          return static_cast<int64_t>(
              ReadBE64(exc + (static_cast<size_t>(k) * 10) + 2));
        }
        if (pos > idx) {
          break;
        }
      }
      if (w >= 64) {
        return static_cast<int64_t>(
            static_cast<uint64_t>(base_v) +
            UnpackBitAt(packed, static_cast<uint64_t>(idx) * 64, 64));
      }
      const uint64_t off = UnpackBitAt(
          packed, static_cast<uint64_t>(idx) * static_cast<uint64_t>(w), w);
      return static_cast<int64_t>(static_cast<uint64_t>(base_v) + off);
    }
    case Scheme::kFastPfor: {
      // One metadata walk (<= 8 blocks per row group), one lane extraction,
      // and a popcount rank into the exception list when the row is one.
      FastPforRegion p;
      if (!ParseFastPforRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return FastPforValueAt(p, idx);
    }
    case Scheme::kDeltaFastPfor: {
      // One metadata walk (<= 8 blocks per row group) plus a <= 127 step
      // in-block replay of the first differences (exception deltas patched
      // from the list, others dmin + lane) -- the delta chain's price for
      // random access, priced at FastPFOR's probe tier.
      DeltaFastPforRegion p;
      if (!ParseDeltaFastPforRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return DeltaFastPforValueAt(p, idx);
    }
    case Scheme::kDeltaPfor: {
      DeltaPForRegion p;
      if (!ParseDeltaPForRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return DeltaPForValueAt(p, idx);
    }
    case Scheme::kDdRice: {
      // Checkpoint jump + Rice walk over zigzagged second differences.
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t C = kDeltaCheckpointEvery;
      const uint32_t ncp = ReadBE16(r);
      if (ncp == 0 || idx / C >= ncp) {
        return 0;
      }
      // Full header extent before any read: [ncp u16][checkpoints ncp*16B]
      // [bit offsets ncp*4B][k u8]. The former ncp*16 gate left the offset
      // table and the k byte unbounded (hostile ncp read ~1.3MB past the
      // region).
      if (region_end != nullptr &&
          r + 2 + (static_cast<size_t>(ncp) * 20) + 1 > region_end) {
        return 0;
      }
      const uint32_t block = idx / C;
      const char* ck = r + 2;
      uint64_t v = ReadBE64(ck + (static_cast<size_t>(block) * 16));
      uint64_t d1 = ReadBE64(ck + (static_cast<size_t>(block) * 16) + 8);
      if (idx == block * C) {
        return static_cast<int64_t>(v);
      }
      const char* offs = r + 2 + (static_cast<size_t>(ncp) * 16);
      const uint32_t start_bit =
          ReadBE32(offs + (static_cast<size_t>(block) * 4));
      const int k = static_cast<int>(
          static_cast<uint8_t>(r[2 + (static_cast<size_t>(ncp) * 20)]));
      if (k < 0 || k > 64) {
        return 0;
      }
      const char* stream = r + 2 + (static_cast<size_t>(ncp) * 20) + 1;
      // A hostile start_bit must not form a reader past the region (the
      // BitReader would only stop at its first get1, but the pointer math
      // itself runs up to 512MB past `stream`).
      if (region_end != nullptr &&
          std::cmp_greater(start_bit / 8, region_end - stream)) {
        return 0;
      }
      // Unbounded reader (mirrors the Gorilla case): validated chunks size
      // the stream exactly; hostile streams terminate via the QMAX cap.
      BitReader br(stream + (start_bit / 8), region_end);
      // Bit offset inside the first byte: consume leading bits MSB-first.
      uint32_t skip = start_bit % 8;
      for (uint32_t t = 0; t < skip; ++t) {
        br.get1();
      }
      const uint32_t first = (block == 0) ? 1 : (block * C) + 1;
      for (uint32_t j = first; j <= idx; ++j) {
        uint64_t q = 0;
        while (br.get1() != 0) {
          if (++q > kRiceQMax) {
            return 0;
          }
        }
        uint64_t rem = 0;
        for (int t = 0; t < k; ++t) {
          rem = (rem << 1) | static_cast<uint64_t>(br.get1());
        }
        // k == 64 makes (q << k) UB; rem already holds all 64 bits then.
        const uint64_t zz = (k >= 64) ? rem : ((q << k) | rem);
        const uint64_t dd =
            (zz >> 1) ^ (0ULL - (zz & 1ULL));  // zigzag -> wrap pattern
        d1 += dd;
        if (j > block * C) {
          v += d1;
        }
      }
      return static_cast<int64_t>(v);
    }
    case Scheme::kChimp128: {
      Chimp128Region parsed;
      if (!ParseChimp128Region(r, n, param, region_end, &parsed)) {
        return 0;
      }
      const uint32_t block_index = idx / kChimp128BlockRows;
      if (block_index >= parsed.block_count) {
        return 0;
      }
      std::array<uint64_t, kChimp128BlockRows> decoded{};
      if (!DecodeChimp128Block(parsed.blocks[block_index], decoded.data())) {
        return 0;
      }
      return static_cast<int64_t>(decoded[idx % kChimp128BlockRows]);
    }
    case Scheme::kAlpRdTans: {
      AlpRdTansRegion parsed;
      if (!ParseAlpRdTansRegion(r, n, param, region_end, &parsed)) {
        return 0;
      }
      const uint32_t block_index = idx / kAlpRdTansBlockRows;
      if (block_index >= parsed.block_count) {
        return 0;
      }
      std::vector<uint64_t> decoded;
      if (!AlpRdTansDecodeBlock(parsed, block_index, &decoded) ||
          decoded.size() <= idx % kAlpRdTansBlockRows) {
        return 0;
      }
      return static_cast<int64_t>(decoded[idx % kAlpRdTansBlockRows]);
    }
    case Scheme::kPatas: {
      PatasRegion parsed;
      if (!ParsePatasRegion(r, n, param, region_end, &parsed)) {
        return 0;
      }
      const uint32_t block_index = idx / kPatasBlockRows;
      if (block_index >= parsed.block_count) {
        return 0;
      }
      std::array<uint64_t, kPatasBlockRows> decoded{};
      if (!DecodePatasBlock(parsed.blocks[block_index], decoded.data())) {
        return 0;
      }
      return static_cast<int64_t>(decoded[idx % kPatasBlockRows]);
    }
    case Scheme::kChimp: {
      if (region_end != nullptr && r + 8 > region_end) {
        return 0;
      }
      uint64_t prev = ReadBE64(r);
      if (idx == 0) {
        return static_cast<int64_t>(prev);
      }
      int prev_lz = 0, prev_tz = 64;
      BitReader br(r + 8, region_end);
      for (uint32_t i = 1; i <= idx; ++i) {
        if (br.get1() == 0) {
          continue;
        }
        if (br.get1() == 0) {  // reuse previous window
          const int width = 64 - prev_lz - prev_tz;
          // width <= 0 only on hostile streams (the encoder never emits
          // '10' before a real window exists); skip instead of shifting
          // by >= 64 (UB).
          if (width > 0 && width <= 64) {
            uint64_t m = 0;
            for (int t = 0; t < width; ++t) {
              m = (m << 1) | static_cast<uint64_t>(br.get1());
            }
            prev ^= (m << prev_tz);
          }
        } else {
          const int lz = static_cast<int>(br.get(5));
          const int tz = static_cast<int>(br.get(6));
          int mw = 64 - lz - tz;
          mw = std::max(mw, 1);
          mw = std::min(mw, 64);
          uint64_t m = br.get(mw);
          prev ^= (mw >= 64 ? m : (m << tz));
          prev_lz = lz;
          prev_tz = tz;
        }
      }
      return static_cast<int64_t>(prev);
    }
    case Scheme::kXorForC: {
      // [ncp u16][ckpts ncp x u64][w u8][xors]. Jump to the block
      // checkpoint then XOR <= 127 deltas (mirrors the kDeltaVarint arm's
      // checkpoint-table extent gate).
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t ncp = ReadBE16(r);
      const uint32_t block = idx / kDeltaCheckpointEvery;
      if (ncp == 0 || block >= ncp) {
        return 0;
      }
      if (region_end != nullptr &&
          r + 3 + (static_cast<size_t>(ncp) * 8) > region_end) {
        return 0;
      }
      uint64_t x = ReadBE64(r + 2 + (static_cast<size_t>(block) * 8));
      if (idx == block * kDeltaCheckpointEvery) {
        return static_cast<int64_t>(x);
      }
      const char* packed = r + 3 + (static_cast<size_t>(ncp) * 8);
      const uint32_t start = block * kDeltaCheckpointEvery;
      // Bound the XOR-walk extent the same way as kXorFor above.
      if (region_end != nullptr &&
          packed + ((static_cast<size_t>(idx) * param + 7) / 8) > region_end) {
        return 0;
      }
      for (uint32_t t = start + 1; t <= idx; ++t) {
        x ^= UnpackBitAt(packed, static_cast<uint64_t>(t - 1) * param,
                         static_cast<int>(param));
      }
      return static_cast<int64_t>(x);
    }
    case Scheme::kGorillaC:
    case Scheme::kChimpC: {
      // [ncp u16][ckpts ncp x u64][bitoffs ncp x u32][first u64][stream].
      // Checkpoint jump to the block at-or-below idx, then a bounded
      // stream walk (mirrors the kChimp/kGorilla arms' control-bit decode).
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t ncp = ReadBE16(r);
      const uint32_t block = idx / kDeltaCheckpointEvery;
      if (ncp == 0 || block >= ncp) {
        return 0;
      }
      if (region_end != nullptr &&
          r + 2 + (static_cast<size_t>(ncp) * 12) + 8 > region_end) {
        return 0;
      }
      const char* ck = r + 2;
      const char* offs = ck + (static_cast<size_t>(ncp) * 8);
      const uint64_t bitoff = ReadBE32(offs + (static_cast<size_t>(block) * 4));
      // +8 skips the redundant [first u64] word (checkpoints carry v[0]).
      const char* stream = offs + (static_cast<size_t>(ncp) * 4) + 8;
      uint64_t prev = ReadBE64(ck + (static_cast<size_t>(block) * 8));
      const uint32_t start = block * kDeltaCheckpointEvery;
      if (idx == start) {
        return static_cast<int64_t>(prev);
      }
      // Seek to the resume bit offset: whole bytes + leading bits. Bound
      // bitoff before pointer math -- a hostile u32 offset must not form a
      // pointer past the region (the bounded reader then yields 0 bits).
      if (region_end != nullptr &&
          bitoff / 8 > static_cast<uint64_t>(region_end - stream)) {
        return 0;
      }
      BitReader br(stream + (bitoff / 8), region_end);
      for (uint32_t t = 0; t < bitoff % 8; ++t) {
        br.get1();
      }
      if (s == Scheme::kGorillaC) {
        for (uint32_t i = start + 1; i <= idx; ++i) {
          if (br.get1() == 0) {
            // identical to previous value
          } else {
            const int lz = static_cast<int>(br.get(5));
            const int tz = static_cast<int>(br.get(6));
            int mw = 64 - lz - tz;
            mw = std::max(mw, 0);
            mw = std::min(mw, 64);
            const uint64_t m = br.get(mw);
            prev ^= (mw >= 64 ? m : (m << tz));
          }
        }
      } else {  // kChimpC: '11' codes re-establish the window; '10' reuses.
        int prev_lz = 0, prev_tz = 64;  // forced-'11' contract at resumes
        for (uint32_t i = start + 1; i <= idx; ++i) {
          if (br.get1() == 0) {
            continue;
          }
          if (br.get1() == 0) {  // reuse previous window
            const int width = 64 - prev_lz - prev_tz;
            if (width > 0 && width <= 64) {
              uint64_t m = 0;
              for (int t = 0; t < width; ++t) {
                m = (m << 1) | static_cast<uint64_t>(br.get1());
              }
              prev ^= (m << prev_tz);
            }
          } else {
            const int lz = static_cast<int>(br.get(5));
            const int tz = static_cast<int>(br.get(6));
            int mw = 64 - lz - tz;
            mw = std::max(mw, 1);
            mw = std::min(mw, 64);
            prev ^= (mw >= 64 ? br.get(mw) : (br.get(mw) << tz));
            prev_lz = lz;
            prev_tz = tz;
          }
        }
      }
      return static_cast<int64_t>(prev);
    }
    case Scheme::kDeltaVarint: {
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t C = kDeltaCheckpointEvery;
      const uint32_t ncp = ReadBE16(r);
      if (ncp == 0 || idx / C >= ncp) {
        return 0;
      }
      // Checkpoint table + bit-offset table before any read: the offset
      // entry at offs+block*4 reaches r+2+ncp*12, so the former ncp*8 gate
      // left the last table reads unbounded (hostile ncp).
      if (region_end != nullptr &&
          r + 2 + (static_cast<size_t>(ncp) * 12) > region_end) {
        return 0;
      }
      const uint32_t block = idx / C;
      const char* ck = r + 2;
      uint64_t v = ReadBE64(ck + (static_cast<size_t>(block) * 8));
      if (idx == block * C) {
        return static_cast<int64_t>(v);
      }
      const char* offs = r + 2 + (static_cast<size_t>(ncp) * 8);
      const uint32_t start = ReadBE32(offs + (static_cast<size_t>(block) * 4));
      // A hostile start (up to 2^32-1) must not FORM a pointer ~4 GiB past
      // the region: the loop's per-byte guard only bounds dereferences, not
      // the pointer arithmetic itself (same pre-gate as the kDdRice
      // start_bit arm). Valid encoders pin start <= stream size, i.e. inside
      // [base, region_end]; the table-extent check above already proved
      // base <= region_end.
      const char* base = r + 2 + (static_cast<size_t>(ncp) * 12);
      if (region_end != nullptr && std::cmp_greater(start, region_end - base)) {
        return 0;
      }
      const char* p = base + start;
      for (uint32_t j = (block * C) + 1; j <= idx; ++j) {
        uint64_t z = 0;
        for (int b = 0; b < 10; ++b) {
          // Bound the LEB128 walk: a hostile stream must not read past the
          // region (at most 10 bytes per delta, but the checkpoint block
          // replay walks up to C deltas unchecked).
          if (region_end != nullptr && p + b >= region_end) {
            return 0;
          }
          const auto byte = static_cast<uint8_t>(p[b]);
          z |= static_cast<uint64_t>(byte & 0x7F) << (7 * b);
          if ((byte & 0x80) == 0) {
            p += b + 1;
            break;
          }
          if (b == 9) {
            return 0;  // unterminated LEB128
          }
        }
        const uint64_t d = (z >> 1) ^ (0ULL - (z & 1ULL));
        v += d;
      }
      return static_cast<int64_t>(v);
    }
    case Scheme::kAlp: {
      // [exp u8][backend u8][nexc u16][exc (pos u16 + val 8B) x nexc]
      // [inner region]. The mantissa backend dispatches through the inner
      // scheme's ordinary Int64RegionAt arm; exception rows return their
      // stored full value.
      if (region_end != nullptr && r + 4 > region_end) {
        return 0;
      }
      const int exp = static_cast<uint8_t>(r[0]);
      if (exp > 18) {
        return 0;
      }
      const auto backend = static_cast<Scheme>(static_cast<uint8_t>(r[1]));
      if (backend != Scheme::kConst && backend != Scheme::kRle &&
          backend != Scheme::kDict && backend != Scheme::kBitWeave) {
        return 0;
      }
      const uint32_t nexc = ReadBE16(r + 2);
      if (nexc > n) {
        return 0;
      }
      const char* exc = r + 4;
      // Full exception extent before the walk (mirrors kPfor).
      if (region_end != nullptr &&
          exc + (static_cast<size_t>(nexc) * 10) > region_end) {
        return 0;
      }
      for (uint32_t k = 0; k < nexc; ++k) {
        const uint32_t pos = ReadBE16(exc + (static_cast<size_t>(k) * 10));
        if (pos == idx) {
          return static_cast<int64_t>(
              ReadBE64(exc + (static_cast<size_t>(k) * 10) + 2));
        }
        if (pos > idx) {
          break;
        }
      }
      const char* inner = exc + (static_cast<size_t>(nexc) * 10);
      // Inner param: RLE carries its run count in a u32 header; DICT and
      // BitWeave read their own headers (param unused); CONST has none.
      uint32_t inner_param = 0;
      if (backend == Scheme::kRle) {
        if (region_end != nullptr && inner + 4 > region_end) {
          return 0;
        }
        inner_param = ReadBE32(inner);
      }
      const int64_t m =
          Int64RegionAt(inner, backend, inner_param, n, idx, region_end);
      const double back = static_cast<double>(m) / AlpFactorOf(exp);
      uint64_t b = 0;
      std::memcpy(&b, &back, sizeof(b));
      return static_cast<int64_t>(b);
    }
    case Scheme::kEliasFano: {
      if (region_end != nullptr && r + 17 > region_end) {
        return 0;
      }
      const auto base_v = static_cast<int64_t>(ReadBE64(r));
      const uint64_t maxd = ReadBE64(r + 8);
      const int l = static_cast<int>(static_cast<uint8_t>(r[16]));
      const uint64_t high_max = (l >= 64) ? 0 : (maxd >> l);
      // hlen = n + high_max must not wrap -- and neither may the byte-ceil
      // (hlen + 7) / 8: the former `high_max > UINT64_MAX - n` guard still
      // admitted the boundary high_max == UINT64_MAX - n (hlen == UINT64_MAX,
      // whose +7 wrapped the division to 0), aliasing lowbits onto hbmp and
      // sending SelectBit marching past the region. Reserve 7 bits of
      // headroom so the byte-ceil stays exact. Fail closed instead.
      if (high_max > UINT64_MAX - static_cast<uint64_t>(n) - 7) {
        return 0;
      }
      const size_t hlen = n + static_cast<size_t>(high_max);
      const char* hbmp = r + 17;
      const char* lowbits = hbmp + ((hlen + 7) / 8);
      // Region clamp: a hostile header must not walk SelectBit / the
      // low-bit reader past the region.
      if (region_end != nullptr &&
          lowbits + ((static_cast<size_t>(idx) * static_cast<size_t>(l) +
                      static_cast<size_t>(l) + 7) /
                     8) >
              region_end) {
        return 0;
      }
      const uint64_t low = UnpackBitAt(
          lowbits, static_cast<uint64_t>(idx) * static_cast<uint64_t>(l), l);
      // The select directory (when the chunk built one) skips the from-word-0
      // bitmap scan; both arms answer identically.
      const uint64_t pos = (ef_dir != nullptr)
                               ? SelectBitDir(hbmp, hlen, idx, ef_dir)
                               : SelectBit(hbmp, hlen, idx);
      const uint64_t h = pos - idx;
      // l == 64 never occurs from the encoder, but a hostile region can
      // carry it: `h << 64` is UB (and the bulk decoder below yields 0 for
      // the high part). Guard the scalar path identically.
      const uint64_t hpart = (l >= 64) ? 0 : (h << l);
      return static_cast<int64_t>(static_cast<uint64_t>(base_v) +
                                  (hpart | low));
    }
    case Scheme::kWavelet: {
      if (region_end != nullptr && r + 9 > region_end) {
        return 0;
      }
      const auto min_v = static_cast<int64_t>(ReadBE64(r));
      const int W = static_cast<int>(static_cast<uint8_t>(r[8]));
      // A hostile W byte walks W bitmaps of ceil(n/8) bytes each. The footer
      // gate caps prm at 1024 and the header at 64 for validated chunks;
      // clamp here so unvalidated regions stay memory-safe.
      if (W <= 0 || W > 64) {
        return 0;
      }
      const size_t bmp_bytes = (n + 7) / 8;
      const char* base_bmp = r + 9;
      // Region clamp: the W bitmap extent must stay inside the region.
      if (region_end != nullptr &&
          base_bmp + (static_cast<size_t>(W) * bmp_bytes) > region_end) {
        return 0;
      }
      uint64_t value = 0;
      uint64_t p = idx;
      for (int k = 0; k < W; ++k) {
        const char* bmp = base_bmp + (static_cast<size_t>(k) * bmp_bytes);
        const uint64_t rank1p = PopcountBits(bmp, static_cast<uint32_t>(p));
        const uint64_t r0 = p - rank1p;  // rank0 in [0,p)
        const uint64_t bit =
            (static_cast<uint8_t>(bmp[p >> 3]) >> (7 - (p & 7))) & 1;
        value = (value << 1) | bit;
        if (bit == 0) {
          p = r0;
        } else {
          const uint64_t total_ones = PopcountBits(bmp, n);
          const uint64_t ztot = static_cast<uint64_t>(n) - total_ones;
          p = ztot + rank1p;
        }
      }
      return static_cast<int64_t>(static_cast<uint64_t>(min_v) + value);
    }
    case Scheme::kWaveletR: {
      WaveletRRegion p;
      if (!ParseWaveletRRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return static_cast<int64_t>(static_cast<uint64_t>(p.minimum) +
                                  WaveletRValueAt(p, idx));
    }
    case Scheme::kDeltaFor:
    case Scheme::kDeltaBitweave:
    case Scheme::kDeltaSimd: {
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t ncp = ReadBE16(r);
      const uint32_t C = kDeltaCheckpointEvery;
      const uint32_t block = idx / C;
      // Same checkpoint-count guard as kDdRice / kDeltaVarint: without it a
      // hostile ncp=0 region decodes `base` from inner-payload bytes (and
      // the bulk path below reads checkpoints past the table).
      if (ncp == 0 || block >= ncp) {
        return 0;
      }
      // The checkpoint table must be inside the region before ReadBE64:
      // a hostile ncp used to walk the checkpoint read ~512KB past it.
      if (region_end != nullptr &&
          r + 2 + (static_cast<size_t>(ncp) * 8) > region_end) {
        return 0;
      }
      const char* ck = r + 2;
      const auto base_val =
          static_cast<int64_t>(ReadBE64(ck + (static_cast<size_t>(block) * 8)));
      const char* ir = r + 2 + (static_cast<size_t>(ncp) * 8);
      Scheme inner = Scheme::kSimdFor;
      if (s == Scheme::kDeltaFor) {
        inner = Scheme::kForBp;
      } else if (s == Scheme::kDeltaBitweave) {
        inner = Scheme::kBitWeave;
      }
      // Inner payload extent for the deepest index we will decode: ForBp
      // stores 8B base + (idx+1)*param bytes; BitWeave 9B header +
      // ceil((idx+1)*w/8) bytes. Must stay inside the outer region.
      if (region_end != nullptr) {
        // kSimdFor derives its whole extent from (n, w), so there is no
        // idx-dependent prefix to size: bound the COMPLETE inner region
        // instead.
        size_t ir_need =
            9 + ((static_cast<size_t>(idx) * param + param + 7) / 8);
        if (inner == Scheme::kForBp) {
          ir_need = 8 + ((static_cast<size_t>(idx) + 1) * param);
        } else if (inner == Scheme::kSimdFor) {
          ir_need = SimdForExtent(n, param);
        }
        if (ir + ir_need > region_end) {
          return 0;
        }
      }
      uint64_t sum = 0;
      for (uint32_t j = (block * C) + 1; j <= idx; ++j) {
        sum += static_cast<uint64_t>(
            Int64RegionAt(ir, inner, param, n, j, region_end));
      }
      return static_cast<int64_t>(static_cast<uint64_t>(base_val) + sum);
    }
    case Scheme::kByteSlice: {
      if (region_end != nullptr && r + 1 > region_end) {
        return 0;
      }
      const auto w = static_cast<uint8_t>(r[0]);
      const size_t stride = (n + 7) / 8;
      // The one unclamped bit-width header: w up to 255 walked 255 slices
      // (~32KB) past the region on hostile input. Clamp like kBitWeave /
      // kPfor, then bound the slice extent outright.
      if (w == 0 || w > 64) {
        return 0;
      }
      if (region_end != nullptr &&
          r + 1 + (static_cast<size_t>(w - 1) * stride) + (idx >> 3) + 1 >
              region_end) {
        return 0;
      }
      uint64_t value = 0;
      for (uint8_t b = 0; b < w; ++b) {
        const char* sl = r + 1 + (static_cast<size_t>(b) * stride);
        const auto byte = static_cast<uint8_t>(sl[idx >> 3]);
        const auto bit = static_cast<uint8_t>((byte >> (7 - (idx & 7))) & 1);
        value |= static_cast<uint64_t>(bit) << b;
      }
      return static_cast<int64_t>(value);
    }
    case Scheme::kGorilla: {
      if (region_end != nullptr && r + 8 > region_end) {
        return 0;
      }
      uint64_t prev = ReadBE64(r);
      if (idx == 0) {
        return static_cast<int64_t>(prev);
      }
      BitReader br(r + 8, region_end);
      for (uint32_t i = 1; i <= idx; ++i) {
        if (br.get1() == 0) {
          // identical to previous value
        } else {
          const int lz = static_cast<int>(br.get(5));
          const int tz = static_cast<int>(br.get(6));
          // Valid Gorilla guarantees lz + tz <= 63 (mw >= 1); a corrupt
          // stream can yield mw < 0, making BitReader::get read a negative
          // width (UB). Clamp to the full 64-bit window, matching the
          // O(n) decode path and the WASM decoder.
          int mw = 64 - lz - tz;
          mw = std::max(mw, 0);
          mw = std::min(mw, 64);
          const uint64_t m = br.get(mw);
          const uint64_t xorv = mw >= 64 ? m : (m << tz);
          prev ^= xorv;
        }
      }
      return static_cast<int64_t>(prev);
    }
    case Scheme::kFreq: {
      // [top i64][inner_scheme u8][inner_param u32][nexc u16]
      // [bitmap ceil(n/8) B MSB-first][inner region of nexc exceptions].
      // bit i set -> value is `top`; clear -> next exception in row order.
      if (region_end != nullptr && r + 15 > region_end) {
        return 0;
      }
      const auto top = static_cast<int64_t>(ReadBE64(r));
      const auto ischeme = static_cast<uint8_t>(r[8]);
      const uint32_t iparam = ReadBE32(r + 9);
      const uint32_t nexc = ReadBE16(r + 13);
      const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
      if (region_end != nullptr && r + 15 + bmp_bytes > region_end) {
        return 0;
      }
      const auto bit = static_cast<uint8_t>(
          (static_cast<uint8_t>(r[15 + (idx >> 3)]) >> (7 - (idx & 7))) & 1);
      if (bit != 0) {
        return top;
      }
      // Rank of this exception = number of clear bitmap bits before idx.
      uint32_t eidx = 0;
      for (uint32_t i = 0; i < idx; ++i) {
        if (((static_cast<uint8_t>(r[15 + (i >> 3)]) >> (7 - (i & 7))) & 1) ==
            0) {
          ++eidx;
        }
      }
      if (eidx >= nexc) {
        return 0;  // bitmap over-counts exceptions
      }
      const char* inner = r + 15 + bmp_bytes;
      if (!InnerSchemeOk(ischeme)) {
        return 0;
      }
      return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc,
                           eidx, region_end);
    }
    case Scheme::kTopk: {
      // [d u16][nexc u16][hot 15 x 8B BE][freq 15 x u16 BE]
      // [ischeme u8][iparam u32][nibbles ceil(n/2) B][inner region].
      // Nibble at row i: < d -> hot[d] value; 15 -> next exception in row
      // order (rank = SWAR popcount of sentinel nibbles before i).
      constexpr size_t kTopkHeader =
          2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;  // 159
      if (region_end != nullptr && r + kTopkHeader > region_end) {
        return 0;
      }
      const uint32_t d = ReadBE16(r);
      const uint32_t nexc = ReadBE16(r + 2);
      if (d == 0 || d > 15 || nexc != param) {
        return 0;
      }
      const auto ischeme = static_cast<uint8_t>(r[154]);
      const uint32_t iparam = ReadBE32(r + 155);
      const size_t nib_bytes = (static_cast<size_t>(n) + 1) / 2;
      if (region_end != nullptr && r + kTopkHeader + nib_bytes > region_end) {
        return 0;
      }
      const auto byte = static_cast<uint8_t>(r[kTopkHeader + (idx >> 1)]);
      const uint32_t code = (idx & 1) == 0 ? (byte >> 4) : (byte & 0x0F);
      if (code < d) {
        return static_cast<int64_t>(
            ReadBE64(r + 4 + (static_cast<size_t>(code) * 8)));
      }
      if (code != 15) {
        return 0;  // codes d..14 are never emitted
      }
      // Rank of this exception = number of sentinel nibbles before idx:
      // complete bytes popcounted word-wise, then the boundary byte's high
      // nibble when idx sits in a low nibble.
      uint32_t eidx = 0;
      {
        const size_t full = idx >> 1;  // complete nibble bytes before idx
        size_t b = 0;
        for (; b + 8 <= full; b += 8) {
          uint64_t w = 0;
          std::memcpy(&w, r + kTopkHeader + b, 8);
          eidx += TopKSentinelCount(w);
        }
        for (; b < full; ++b) {
          eidx += TopKSentinelCount(static_cast<uint8_t>(r[kTopkHeader + b]));
        }
        if ((idx & 1) != 0) {
          eidx += TopKSentinelCount(
              static_cast<uint8_t>(r[kTopkHeader + full] & 0xF0));
        }
      }
      if (eidx >= nexc) {
        return 0;  // sentinel over-counts exceptions
      }
      const char* inner = r + kTopkHeader + nib_bytes;
      if (nexc > 0 && !InnerSchemeOk(ischeme)) {
        return 0;
      }
      return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc,
                           eidx, region_end);
    }
    case Scheme::kTileDict: {
      // Tile walk (<= 16 tiles per full row group) + one bit extraction +
      // one dictionary load. The cursor bounds every step against
      // region_end, so a truncated chain fails closed.
      TileDictCursor c;
      if (!TileDictSeek(r, n, region_end, idx, &c)) {
        return 0;
      }
      return TileDictValueAt(c, idx - c.tile_start);
    }
    case Scheme::kSortDict: {
      // One rank extraction + the block's first + at most 63 replayed gaps
      // (the kS4 probe shape, on the sorted view instead of the row order).
      // The parse bounds every extent; a rank >= n is the clamp bucket.
      SortDictRegion p;
      if (!ParseSortDictRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      const uint32_t j = SortDictRankAt(p, idx);
      if (j >= p.n) {
        return 0;
      }
      return SortDictValueAtSorted(p, j);
    }
    case Scheme::kPostings: {
      // Row -> list has no table shortcut (the lists interleave in row
      // space, so firsts are not a partition): the probe sorts the lists by
      // first posting (the kHuffDict per-call table-rebuild precedent,
      // d <= kPostingsMaxLists) and walks candidates in first order, each
      // jumped to by its bitoffs entry. A candidate's postings are ASCENDING,
      // so walking past idx is a definitive miss -- the walk stops at the
      // first candidate whose first posting is beyond idx. Poor random
      // access, priced like the run family (the closed-form kernels never
      // come here: COUNT/SUM answer from the tables, masks from the passing
      // lists alone). The walk reads the Rice stream, which has no
      // self-described length: a null region_end (the pre-validated
      // convention) cannot bound the cursor, so it declines.
      if (region_end == nullptr) {
        return 0;
      }
      PostingsRegion p;
      if (!ParsePostingsRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      // (first, list) sorted by first: insertion into a small stack array.
      std::array<std::pair<uint32_t, uint32_t>, kPostingsMaxLists> cand{};
      uint32_t ncand = 0;
      for (uint32_t c = 0; c < p.d; ++c) {
        if (PostingsFirstAt(p, c) > idx) {
          continue;  // first > idx: no hit
        }
        uint32_t lo = 0;
        while (lo < ncand && cand[lo].first <= PostingsFirstAt(p, c)) {
          ++lo;
        }
        for (uint32_t t = ncand; t > lo; --t) {
          cand[t] = cand[t - 1];
        }
        cand[lo] = {PostingsFirstAt(p, c), c};
        ++ncand;
      }
      PostingsBitCursor cur;
      for (uint32_t t = 0; t < ncand; ++t) {
        const uint32_t c = cand[t].second;
        uint32_t row = cand[t].first;
        if (row == idx) {
          return PostingsDictAt(p, c);
        }
        const uint32_t cnt = PostingsCountAt(p, c);
        if (cnt == 1) {
          continue;
        }
        cur.Init(p.stream + (PostingsBitOffAt(p, c) / 8), p.region_end);
        for (uint32_t i = 1; i < cnt; ++i) {
          uint32_t next = 0;
          if (!PostingsNextRow(p, &cur, c, row, &next)) {
            return 0;
          }
          row = next;
          if (row == idx) {
            return PostingsDictAt(p, c);
          }
          if (row > idx) {
            break;  // ascending postings: definitive miss
          }
        }
      }
      return 0;  // hostile over-counting table or truncated stream
    }
    case Scheme::kRleX: {
      // [vscheme u8][cscheme u8][runs u32][vparam u32][cparam u32]
      // [vlen u32][values region: runs entries][counts region: runs entries]
      if (region_end != nullptr && r + 18 > region_end) {
        return 0;
      }
      const auto vs = static_cast<uint8_t>(r[0]);
      const auto cs = static_cast<uint8_t>(r[1]);
      const uint32_t runs = ReadBE32(r + 2);
      const uint32_t vparam = ReadBE32(r + 6);
      const uint32_t cparam = ReadBE32(r + 10);
      const uint64_t vlen = ReadBE32(r + 14);
      // Run-count sanity is a pure value check: apply it even when no
      // region_end was supplied (an unbounded run walk is O(runs) inner
      // decodes of a hostile length).
      if (runs == 0 || runs > n) {
        return 0;
      }
      if (region_end != nullptr &&
          std::cmp_greater(vlen, region_end - (r + 18))) {
        return 0;
      }
      const char* vreg = r + 18;
      const char* creg = vreg + vlen;
      if (!InnerSchemeOk(vs) || !InnerSchemeOk(cs)) {
        return 0;
      }
      uint64_t acc = 0;
      for (uint32_t i = 0; i < runs; ++i) {
        const auto len = static_cast<uint64_t>(Int64RegionAt(
            creg, static_cast<Scheme>(cs), cparam, runs, i, region_end));
        if (static_cast<uint64_t>(idx) < acc + len) {
          // The values region is exactly vlen bytes: pass its own end so a
          // hostile inner header cannot reach into the counts stream.
          return Int64RegionAt(vreg, static_cast<Scheme>(vs), vparam, runs, i,
                               creg);
        }
        acc += len;
        if (acc >= n) {
          break;  // tail rows decode as 0, matching kRle
        }
      }
      return 0;
    }
    case Scheme::kDictX: {
      // [d u16][code_scheme u8][code_param u32][clen u32]
      // [codes region: n entries][pool_scheme u8][dict d x 8B]
      if (region_end != nullptr && r + 11 > region_end) {
        return 0;
      }
      const uint16_t d = ReadBE16(r);
      const auto cs = static_cast<uint8_t>(r[2]);
      const uint32_t cparam = ReadBE32(r + 3);
      const uint64_t clen = ReadBE32(r + 7);
      const char* creg = r + 11;
      if (region_end != nullptr && std::cmp_greater(clen, region_end - creg)) {
        return 0;
      }
      const char* pool_hdr = creg + clen;
      if (region_end != nullptr && pool_hdr + 1 > region_end) {
        return 0;
      }
      const auto ps = static_cast<uint8_t>(pool_hdr[0]);
      if (ps != 0 || d == 0) {
        return 0;  // int64 pool is the flat entry list
      }
      const char* dict = pool_hdr + 1;
      if (region_end != nullptr &&
          dict + (static_cast<size_t>(d) * sizeof(int64_t)) > region_end) {
        return 0;
      }
      if (!InnerSchemeOk(cs)) {
        return 0;
      }
      const auto code = static_cast<uint64_t>(Int64RegionAt(
          creg, static_cast<Scheme>(cs), cparam, n, idx, pool_hdr));
      if (code >= d) {
        return 0;  // clamp bucket decodes as raw 0 (kDict rule)
      }
      return static_cast<int64_t>(
          ReadBE64(dict + (static_cast<size_t>(code) * sizeof(int64_t))));
    }
    case Scheme::kArith: {
      // [a i64][d i64][ischeme u8][iparam u32][nexc u16]
      // [bitmap ceil(n/8) B MSB-first][inner region of nexc exceptions].
      // bit i set -> value is a + i*d (wrapping); clear -> next exception
      // in row order. The exception rank is popcounted by word instead of
      // per-bit (kFreq's walk): PointLookup binary-searches this arm.
      if (region_end != nullptr && r + 23 > region_end) {
        return 0;
      }
      const uint64_t a = ReadBE64(r);
      const uint64_t d = ReadBE64(r + 8);
      const auto ischeme = static_cast<uint8_t>(r[16]);
      const uint32_t iparam = ReadBE32(r + 17);
      const uint32_t nexc = ReadBE16(r + 21);
      const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
      if (region_end != nullptr && r + 23 + bmp_bytes > region_end) {
        return 0;
      }
      // Header/footer agreement and the nexc == 0 layout (no inner bytes).
      if (nexc != param || (nexc > 0 && !InnerSchemeOk(ischeme))) {
        return 0;
      }
      const auto bit = static_cast<uint8_t>(
          (static_cast<uint8_t>(r[23 + (idx >> 3)]) >> (7 - (idx & 7))) & 1);
      if (bit != 0) {
        return static_cast<int64_t>(a + (static_cast<uint64_t>(idx) * d));
      }
      if (nexc == 0) {
        return 0;  // clear bit in a pure ramp is inconsistent
      }
      // Rank of this exception = number of clear bits before idx =
      // idx - set bits before idx (full bytes popcounted + idx's tail).
      const auto* bmp = reinterpret_cast<const uint8_t*>(r + 23);
      uint32_t set_before = 0;
      const size_t bfull = idx / 8;  // bytes strictly below idx's byte
      for (size_t w = 0; w < bfull; ++w) {
        set_before += static_cast<uint32_t>(__builtin_popcount(bmp[w]));
      }
      for (uint32_t b = 0; b < idx % 8; ++b) {
        set_before += static_cast<uint32_t>((bmp[bfull] >> (7 - b)) & 1);
      }
      const uint32_t eidx = idx - set_before;
      if (eidx >= nexc) {
        return 0;  // bitmap over-counts exceptions
      }
      const char* inner = r + 23 + bmp_bytes;
      return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc,
                           eidx, region_end);
    }
    case Scheme::kSparse: {
      // [vscheme u8][vparam u32][vlen u32][pos_bpv u8][nnz u32 BE]
      // [FOR-BA positions: base i64 + nnz x pos_bpv LE][values region]
      // Row idx is non-null iff it appears in the strictly increasing
      // position list; its value is values[rank]. Binary search over the
      // FOR-BA positions (validator pins monotonicity); a hostile
      // duplicate/order violation just answers like an absent row.
      if (region_end != nullptr && r + 14 > region_end) {
        return 0;
      }
      const auto vs = static_cast<uint8_t>(r[0]);
      const uint32_t vprm = ReadBE32(r + 1);
      const uint32_t pbpv = static_cast<uint8_t>(r[9]);
      const uint32_t nnz = ReadBE32(r + 10);
      if (nnz != param || nnz == 0 || nnz > n || pbpv < 1 || pbpv > 8 ||
          !InnerSchemeOk(vs)) {
        return 0;
      }
      const size_t pos_bytes = 8 + (static_cast<size_t>(nnz) * pbpv);
      if (region_end != nullptr && r + 14 + pos_bytes > region_end) {
        return 0;
      }
      const char* posr = r + 14;
      uint32_t lo = 0, hi = nnz;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) / 2);
        const int64_t p =
            Int64RegionAt(posr, Scheme::kForBp, pbpv, nnz, mid, region_end);
        if (std::cmp_less(p, idx)) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      if (lo >= nnz ||
          std::cmp_not_equal(
              Int64RegionAt(posr, Scheme::kForBp, pbpv, nnz, lo, region_end),
              idx)) {
        return 0;  // unlisted row: null, decodes as raw 0
      }
      return Int64RegionAt(posr + pos_bytes, static_cast<Scheme>(vs), vprm, nnz,
                           lo, region_end);
    }
    case Scheme::kBp128: {
      Bp128Region p;
      if (!ParseBp128Region(r, n, param, region_end, &p)) {
        return 0;
      }
      return Bp128ValueAt(p, idx);
    }
    case Scheme::kPBitS: {
      PBitSRegion p;
      if (!ParsePBitSRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return PBitSValueAt(p, idx);
    }
    case Scheme::kInterp: {
      // [ncp u16][ckpts ncp x (first,last)][bitoffs ncp x u32][stream].
      // Checkpoint jump to the block at-or-below idx, then replay its
      // <= 128-row tree (mirrors the kGorillaC/kChimpC block-replay arms).
      InterpRegion p;
      if (!ParseInterpRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      std::array<int64_t, kInterpBlockRows> block{};
      InterpDecodeBlock(p, idx / kInterpBlockRows, block.data());
      return block[idx % kInterpBlockRows];
    }
    case Scheme::kAnsFor: {
      // Checkpoint jump: start at the row-64 boundary at-or-below idx and
      // decode forward, discarding symbols until idx's offset bytes.
      AnsForRegion A;
      if (!AnsForParse(r, region_end, n, param, &A)) {
        return 0;
      }
      const uint32_t k = idx / kAnsCkptRows;
      uint32_t x = ReadBE32(A.ckpt + (static_cast<size_t>(k) * 8));
      uint32_t pos = ReadBE32(A.ckpt + (static_cast<size_t>(k) * 8) + 4);
      const uint32_t start = k * kAnsCkptRows * A.bpv;
      const uint32_t stop = (idx * A.bpv) + A.bpv;
      uint64_t off = 0;
      for (uint32_t j = start; j < stop; ++j) {
        uint8_t sym = 0;
        if (!AnsForStep(A, &x, &pos, &sym)) {
          return 0;
        }
        if (j >= idx * A.bpv) {
          off |= static_cast<uint64_t>(sym) << (8 * (j - idx * A.bpv));
        }
      }
      return static_cast<int64_t>(static_cast<uint64_t>(A.base) + off);
    }
    case Scheme::kQpp: {
      QppRegion p;
      if (!ParseQppRegion(r, n, param, region_end, &p)) {
        return 0;
      }
      return QppValueAt(p, idx);
    }
    case Scheme::kPla: {
      // [nseg u16][nseg x (first 8B, d 8B, end 4B)][ischeme u8][iparam u32]
      // [nexc u16][bitmap ceil(n/8)][inner region of nexc exceptions].
      // PointLookup/LowerBound binary-searches this arm, so the O(nseg)
      // ParsePlaRegion walk is replaced by O(1) header bounds + clamps:
      // the segment binary search only trusts `end`, and a hostile
      // non-increasing table collapses into the idx < start clamp.
      if (region_end != nullptr && r + 2 > region_end) {
        return 0;
      }
      const uint32_t nseg = ReadBE16(r);
      if (nseg == 0 || nseg != param) {
        return 0;
      }
      if (region_end != nullptr && static_cast<size_t>(region_end - r) <
                                       9U + (static_cast<size_t>(nseg) * 20U)) {
        return 0;
      }
      const char* segs = r + 2;
      const char* hdr = segs + (static_cast<size_t>(nseg) * 20);
      const auto ischeme = static_cast<uint8_t>(hdr[0]);
      const uint32_t iparam = ReadBE32(hdr + 1);
      const uint32_t nexc = ReadBE16(hdr + 5);
      if (nexc != 0 && !InnerSchemeOk(ischeme)) {
        return 0;
      }
      // First segment whose exclusive end is past idx.
      uint32_t lo = 0;
      uint32_t hi = nseg;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) / 2);
        if (ReadBE32(segs + (static_cast<size_t>(mid) * 20) + 16) <= idx) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      if (lo >= nseg) {
        return 0;
      }
      const uint32_t start =
          lo == 0 ? 0
                  : ReadBE32(segs + (static_cast<size_t>(lo - 1) * 20) + 16);
      if (idx < start) {
        return 0;  // hostile ends regression
      }
      const size_t bmp_off = 9U + (static_cast<size_t>(nseg) * 20U);
      if (region_end != nullptr &&
          r + bmp_off + ((static_cast<size_t>(n) + 7) / 8) > region_end) {
        return 0;
      }
      const auto bit = static_cast<uint8_t>(
          (static_cast<uint8_t>(r[bmp_off + (idx >> 3)]) >> (7 - (idx & 7))) &
          1);
      if (bit != 0) {
        return PlaSegmentValueAt(
            {.nseg = nseg, .segs = segs, .hdr = hdr, .bmp = r + bmp_off}, lo,
            start, idx);
      }
      if (nexc == 0) {
        return 0;  // clear bit in a pure model is inconsistent
      }
      // Exception rank = clear bits before idx (kArith's popcount walk).
      const auto* bmp = reinterpret_cast<const uint8_t*>(r + bmp_off);
      uint32_t set_before = 0;
      const size_t bfull = idx / 8;
      for (size_t w = 0; w < bfull; ++w) {
        set_before += static_cast<uint32_t>(__builtin_popcount(bmp[w]));
      }
      for (uint32_t b = 0; b < idx % 8; ++b) {
        set_before += static_cast<uint32_t>((bmp[bfull] >> (7 - b)) & 1);
      }
      const uint32_t eidx = idx - set_before;
      if (eidx >= nexc) {
        return 0;  // bitmap over-counts exceptions
      }
      const char* inner = r + bmp_off + ((static_cast<size_t>(n) + 7) / 8);
      return Int64RegionAt(inner, static_cast<Scheme>(ischeme), iparam, nexc,
                           eidx, region_end);
    }
    case Scheme::kMod:
      // Canonical arm split out for the row path (Int64AtMod): O(1) header
      // bounds, one mulmod on the on-model bit, kArith's popcount exception
      // rank otherwise.
      return Int64AtMod(r, param, n, idx, region_end, ef_dir);
    case Scheme::kLane:
      // Canonical arm split out for the row path (Int64AtLane): O(1) header
      // bounds, one lane model eval on the on-model bit, kArith's popcount
      // exception rank otherwise.
      return Int64AtLane(r, param, n, idx, region_end, ef_dir);
    case Scheme::kFsst:
    case Scheme::kFrontCode:
    case Scheme::kTemplate:
    case Scheme::kDStream:
      break;  // varchar-only schemes: never stored for an int64 column
  }
  return 0;
}

bool ParsePBitSRegion(const char* r, uint32_t n, uint32_t param,
                      const char* end, PBitSRegion* out) {
  if (r == nullptr || end == nullptr || end < r || n > UINT16_MAX ||
      static_cast<size_t>(end - r) < 11) {
    return false;
  }
  const uint32_t width = static_cast<uint8_t>(r[8]);
  const uint32_t exceptions = ReadBE16(r + 9);
  if (width == 0 || width > 64 || width != param || exceptions > n) {
    return false;
  }
  const size_t stride = (static_cast<size_t>(n) + 7) / 8;
  const size_t exception_bytes = exceptions == 0 ? 0 : stride;
  const size_t values_bytes = static_cast<size_t>(exceptions) * 10;
  const size_t need = 11 + exception_bytes + values_bytes +
                      (static_cast<size_t>(width) * stride);
  if (std::cmp_greater(need, end - r)) {
    return false;
  }
  PBitSRegion p;
  p.r = r;
  p.end = end;
  p.n = n;
  p.width = width;
  p.exceptions = exceptions;
  p.stride = stride;
  p.base = ReadBE64(r);
  p.exception_bmp = exceptions == 0
                        ? nullptr
                        : reinterpret_cast<const unsigned char*>(r + 11);
  p.exception_values = exceptions == 0 ? r + 11 : r + 11 + stride;
  p.planes =
      reinterpret_cast<const unsigned char*>(p.exception_values + values_bytes);
  if (exceptions != 0) {
    uint32_t ones = 0;
    for (size_t i = 0; i < stride; ++i) {
      uint8_t byte = p.exception_bmp[i];
      if (i + 1 == stride && (n & 7U) != 0) {
        const auto tail = static_cast<uint8_t>(0xFFU << (8U - (n & 7U)));
        if ((byte & static_cast<uint8_t>(~tail)) != 0) {
          return false;
        }
        byte = static_cast<uint8_t>(byte & tail);
      }
      ones += static_cast<uint32_t>(
          __builtin_popcount(static_cast<unsigned int>(byte)));
    }
    if (ones != exceptions) {
      return false;
    }
    uint32_t previous = 0;
    for (uint32_t k = 0; k < exceptions; ++k) {
      const char* entry = p.exception_values + (static_cast<size_t>(k) * 10);
      const uint32_t pos = ReadBE16(entry);
      if (pos >= n || (k != 0 && pos <= previous)) {
        return false;
      }
      if ((p.exception_bmp[pos >> 3] & (0x80U >> (pos & 7U))) == 0) {
        return false;
      }
      previous = pos;
    }
  }
  *out = p;
  return true;
}

uint64_t PBitSOrderValue(int64_t value) {
  return static_cast<uint64_t>(value) ^ (1ULL << 63);
}

int64_t PBitSValueAt(const PBitSRegion& p, uint32_t idx) {
  if (idx >= p.n) {
    return 0;
  }
  if (p.exception_bmp != nullptr &&
      (p.exception_bmp[idx >> 3] & (0x80U >> (idx & 7U))) != 0) {
    for (uint32_t k = 0; k < p.exceptions; ++k) {
      const char* entry = p.exception_values + (static_cast<size_t>(k) * 10);
      if (ReadBE16(entry) == idx) {
        return static_cast<int64_t>(ReadBE64(entry + 2) ^ (1ULL << 63));
      }
      if (ReadBE16(entry) > idx) {
        break;
      }
    }
    return 0;
  }
  uint64_t q = 0;
  for (uint32_t bit = 0; bit < p.width; ++bit) {
    const unsigned char* plane =
        p.planes + (static_cast<size_t>(bit) * p.stride);
    q = (q << 1) |
        static_cast<uint64_t>(
            (static_cast<unsigned>(plane[idx >> 3]) >> (7U - (idx & 7U))) & 1U);
  }
  return static_cast<int64_t>((p.base + q) ^ (1ULL << 63));
}

void DecodePBitSAll(const PBitSRegion& p, int64_t* out) {
  for (uint32_t i = 0; i < p.n; ++i) {
    out[i] = p.exception_bmp != nullptr &&
                     (p.exception_bmp[i >> 3] & (0x80U >> (i & 7U))) != 0
                 ? 0
                 : PBitSValueAt(p, i);
  }
  for (uint32_t k = 0; k < p.exceptions; ++k) {
    const char* entry = p.exception_values + (static_cast<size_t>(k) * 10);
    const uint32_t pos = ReadBE16(entry);
    if (pos < p.n) {
      out[pos] = static_cast<int64_t>(ReadBE64(entry + 2) ^ (1ULL << 63));
    }
  }
}

// ---- kInterp region parse + decode -----------------------------------------

// Bounds-checked header parse shared by the point, batch, and predicate
// readers. Beyond the extent math it pins the structural facts the
// predicate kernels lean on: the checkpoint brackets are globally
// non-decreasing (first[b] <= last[b] <= first[b+1]), bitoffs[0] == 0, the
// offsets are non-decreasing, and every resume position sits inside the
// bitstream. Given monotone brackets, a block's decoded values provably lie
// inside its bracket (codes are masked to hi-lo), so the whole column is
// non-decreasing by construction.
bool ParseInterpRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, InterpRegion* out) {
  if (r == nullptr || end == nullptr || end < r || n == 0 || n > 1024 ||
      static_cast<size_t>(end - r) < 2) {
    return false;
  }
  const uint32_t ncp = ReadBE16(r);
  const uint32_t want_ncp = (n + kInterpBlockRows - 1) / kInterpBlockRows;
  if (ncp == 0 || ncp != param || ncp != want_ncp) {
    return false;
  }
  const size_t fixed =
      2 + (static_cast<size_t>(ncp) * 16) + (static_cast<size_t>(ncp) * 4);
  if (std::cmp_less(end - r, fixed)) {
    return false;
  }
  const char* ckpts = r + 2;
  const char* bitoffs = ckpts + (static_cast<size_t>(ncp) * 16);
  for (uint32_t b = 0; b < ncp; ++b) {
    const char* ck = ckpts + (static_cast<size_t>(b) * 16);
    // Compare brackets in SIGNED order: the encoder's u64 wrap arithmetic
    // makes a sign-crossing block (e.g. [-816, INT64_MAX-88]) perfectly
    // representable, and its u64 words sort backwards.
    const auto first = static_cast<int64_t>(ReadBE64(ck));
    const auto last = static_cast<int64_t>(ReadBE64(ck + 8));
    if (last < first) {
      return false;
    }
    if (b + 1 < ncp) {
      const auto next_first = static_cast<int64_t>(ReadBE64(ck + 16));
      if (next_first < last) {
        return false;
      }
    }
  }
  const char* stream = r + fixed;
  const uint64_t stream_bits = static_cast<uint64_t>(end - stream) * 8;
  uint64_t prev_bit = 0;
  for (uint32_t b = 0; b < ncp; ++b) {
    const uint64_t ob = ReadBE32(bitoffs + (static_cast<size_t>(b) * 4));
    if ((b == 0 && ob != 0) || ob < prev_bit || ob > stream_bits) {
      return false;
    }
    prev_bit = ob;
  }
  InterpRegion p;
  p.n = n;
  p.ncp = ncp;
  p.ckpts = ckpts;
  p.bitoffs = bitoffs;
  p.stream = stream;
  p.stream_end = end;
  *out = p;
  return true;
}

// In-order decode of block b's rows into out[0 .. t-s) in BLOCK-LOCAL order
// (row s+j lands in out[j]), so callers can replay one block into a
// kInterpBlockRows scratch buffer. Explicit-stack DFS that pops the middle
// (reads its code), then the left subtree, then the right -- the exact bit
// order InterpEmit wrote. The bounded BitReader fails closed on over-seek
// like every other scheme's replay arm.
void InterpDecodeBlock(const InterpRegion& p, uint32_t b, int64_t* out) {
  const uint32_t s = b * kInterpBlockRows;
  const uint32_t t = std::min(p.n, (b + 1) * kInterpBlockRows);
  if (s >= t || b >= p.ncp) {
    return;
  }
  const char* ck = p.ckpts + (static_cast<size_t>(b) * 16);
  const uint64_t lo = ReadBE64(ck);
  const uint64_t hi = ReadBE64(ck + 8);
  const uint64_t bitoff = ReadBE32(p.bitoffs + (static_cast<size_t>(b) * 4));
  // Word-at-a-time reader: the block replay is the batch-decode hot path,
  // and the byte-at-a-time BitReader costs ~10x on narrow codes. Same
  // MSB-first get(w) contract; the initial partial byte is consumed as a
  // sub-byte field.
  WordBitReader br(p.stream + (bitoff / 8), p.stream_end);
  if (bitoff % 8 != 0) {
    br.get(static_cast<int>(bitoff % 8));
  }
  struct Frame {
    uint32_t i0, i1;
    uint64_t lo, hi;
  };
  std::array<Frame, kInterpMaxDepth> stack{};
  int top = 0;
  stack[top++] = {.i0 = s, .i1 = t, .lo = lo, .hi = hi};
  while (top > 0) {
    const Frame f = stack[--top];
    if (f.i0 >= f.i1) {
      continue;
    }
    const uint32_t mid = f.i0 + ((f.i1 - f.i0) / 2);
    const int bits = BitsNeeded(f.hi - f.lo);
    const uint64_t code = bits > 0 ? br.get(bits) : 0;
    out[mid - s] = static_cast<int64_t>(f.lo + code);
    if (top + 2 > kInterpMaxDepth) {
      return;  // unreachable: depth <= 8
    }
    const auto vm = static_cast<uint64_t>(out[mid - s]);
    stack[top++] = {.i0 = mid + 1,
                    .i1 = f.i1,
                    .lo = vm,
                    .hi = f.hi};  // right: values in [vm, hi]
    stack[top++] = {.i0 = f.i0,
                    .i1 = mid,
                    .lo = f.lo,
                    .hi = vm};  // left: values in [lo, vm]
  }
}

void DecodeInterpAll(const InterpRegion& p, int64_t* out) {
  for (uint32_t b = 0; b < p.ncp; ++b) {
    InterpDecodeBlock(p, b, out + (static_cast<size_t>(b) * kInterpBlockRows));
  }
}

// Map `lit OP` to the inclusive value window [plo, phi] over __int128 (the
// extreme-bound neighbors go one past int64, mirroring the kArith arm's
// window math). Returns false for an op outside the enum: a hostile caller
// must fail closed like ScanMatch, not fall through to "everything".
bool InterpWindowForValue(PaxChunk::CompareOp op, int64_t lit, __int128* plo,
                          __int128* phi) {
  *plo = static_cast<__int128>(INT64_MIN);
  *phi = static_cast<__int128>(INT64_MAX);
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      *plo = *phi = static_cast<__int128>(lit);
      break;
    case PaxChunk::CompareOp::kLe:
      *phi = static_cast<__int128>(lit);
      break;
    case PaxChunk::CompareOp::kLt:
      *phi = static_cast<__int128>(lit) - 1;  // INT64_MIN -> unsatisfiable
      break;
    case PaxChunk::CompareOp::kGe:
      *plo = static_cast<__int128>(lit);
      break;
    case PaxChunk::CompareOp::kGt:
      *plo = static_cast<__int128>(lit) + 1;  // INT64_MAX -> unsatisfiable
      break;
    default:
      return false;
  }
  return true;
}

// One-block tree walk over the value window [plo, phi] (inclusive). Sets bit
// mid in `bits` (nullable) for every passing row and accumulates the pass
// count -- plus the passing sum when `want_sum`. The shared stream keeps the
// reader sequential (a wholly-in/out SUBTREE cannot be skipped without
// desyncing), but callers skip blocks whose checkpoint BRACKET misses the
// window whole, so a clustered monotone column answers a range predicate by
// walking only the boundary blocks.
void InterpEvalBlock(const InterpRegion& p, uint32_t b, __int128 plo,
                     __int128 phi, uint64_t* bits, bool want_sum,
                     uint64_t* cnt_out, __int128* sum_out) {
  const uint32_t s = b * kInterpBlockRows;
  const uint32_t t = std::min(p.n, (b + 1) * kInterpBlockRows);
  if (s >= t || b >= p.ncp) {
    return;
  }
  const char* ck = p.ckpts + (static_cast<size_t>(b) * 16);
  const uint64_t lo = ReadBE64(ck);
  const uint64_t hi = ReadBE64(ck + 8);
  const uint64_t bitoff = ReadBE32(p.bitoffs + (static_cast<size_t>(b) * 4));
  // Word-at-a-time reader: the block replay is the batch-decode hot path,
  // and the byte-at-a-time BitReader costs ~10x on narrow codes. Same
  // MSB-first get(w) contract; the initial partial byte is consumed as a
  // sub-byte field.
  WordBitReader br(p.stream + (bitoff / 8), p.stream_end);
  if (bitoff % 8 != 0) {
    br.get(static_cast<int>(bitoff % 8));
  }
  struct Frame {
    uint32_t i0, i1;
    uint64_t lo, hi;
  };
  std::array<Frame, kInterpMaxDepth> stack{};
  int top = 0;
  stack[top++] = {.i0 = s, .i1 = t, .lo = lo, .hi = hi};
  uint64_t cnt = 0;
  __int128 sum = 0;
  while (top > 0) {
    const Frame f = stack[--top];
    if (f.i0 >= f.i1) {
      continue;
    }
    const uint32_t mid = f.i0 + ((f.i1 - f.i0) / 2);
    const int w = BitsNeeded(f.hi - f.lo);
    const uint64_t code = w > 0 ? br.get(w) : 0;
    const auto v = static_cast<int64_t>(f.lo + code);
    if (static_cast<__int128>(v) >= plo && static_cast<__int128>(v) <= phi) {
      ++cnt;
      if (want_sum) {
        sum += v;
      }
      if (bits != nullptr) {
        bits[mid >> 6] |= 1ULL << (mid & 63);
      }
    }
    if (top + 2 > kInterpMaxDepth) {
      return;  // unreachable: depth <= 8
    }
    const auto vm = static_cast<uint64_t>(v);
    stack[top++] = {.i0 = mid + 1, .i1 = f.i1, .lo = vm, .hi = f.hi};
    stack[top++] = {.i0 = f.i0, .i1 = mid, .lo = f.lo, .hi = vm};
  }
  *cnt_out += cnt;
  if (sum_out != nullptr) {
    *sum_out += sum;
  }
}

bool PBitSOrderedMatch(PaxChunk::CompareOp op, uint64_t value,
                       uint64_t literal) {
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      return value == literal;
    case PaxChunk::CompareOp::kLe:
      return value <= literal;
    case PaxChunk::CompareOp::kLt:
      return value < literal;
    case PaxChunk::CompareOp::kGe:
      return value >= literal;
    case PaxChunk::CompareOp::kGt:
      return value > literal;
  }
  return false;
}

bool CellIsNull(const PaxChunk::Footer& f, uint32_t g, uint32_t c,
                uint32_t local) {
  if (!f.null_present) {
    return false;
  }
  if (f.null_summary_valid && !f.null_any) {
    return false;
  }
  // A hand-built footer whose row_counts/null_bitmaps are shorter than the
  // declared row_group_count would index OOB below: report "not null" (the
  // region itself is validated, so reading the raw value is memory-safe;
  // only the null claim is unavailable).
  if (g >= f.row_counts.size() ||
      f.row_counts.size() < static_cast<size_t>(f.row_group_count)) {
    return false;
  }
  const uint32_t cc = f.column_count;
  size_t off = 0;
  if (f.null_summary_valid && g < f.null_uniform_groups) {
    off = static_cast<size_t>(g) * static_cast<size_t>(f.null_uniform_stride);
  } else {
    for (uint32_t gg = 0; gg < g; ++gg) {
      off += static_cast<size_t>(cc) *
             static_cast<size_t>((f.row_counts[gg] + 7U) / 8U);
    }
  }
  off +=
      static_cast<size_t>(c) * static_cast<size_t>((f.row_counts[g] + 7U) / 8U);
  if (off + (local >> 3) >= f.null_bitmaps.size()) {
    return false;  // truncated
  }
  const uint8_t b = f.null_bitmaps[off + (local >> 3)];
  return ((b >> (7 - (local & 7))) & 1) != 0;
}

// True if column c of row group g has any NULL cell. The all-column zone
// maps are built over NON-null values (nulls `continue` before min/max), yet
// nulls are encoded as raw 0 and DecodeInt64ColumnRg reports them as 0 -- so
// the "fully inside" aggregate fast path must only be taken when a group has
// no nulls, otherwise it would count null rows the row-evaluation path (and
// the WASM module) would reject.
bool RgColHasNull(const PaxChunk::Footer& f, uint32_t g, uint32_t c) {
  if (!f.null_present) {
    return false;
  }
  if (f.null_summary_valid && !f.null_any) {
    return false;
  }
  const uint32_t cc = f.column_count;
  // row_counts can be shorter than row_group_count in a hand-built footer:
  // the offset walk below indexes row_counts[gg] for gg < g.
  if (c >= cc || g >= f.row_group_count || g >= f.row_counts.size()) {
    return true;  // be conservative
  }
  const uint32_t rc = f.row_counts[g];
  if (rc == 0) {
    return false;
  }
  size_t off = 0;
  if (f.null_summary_valid && g < f.null_uniform_groups) {
    off = static_cast<size_t>(g) * static_cast<size_t>(f.null_uniform_stride);
  } else {
    for (uint32_t gg = 0; gg < g; ++gg) {
      off += static_cast<size_t>(cc) *
             static_cast<size_t>((f.row_counts[gg] + 7U) / 8U);
    }
  }
  off += static_cast<size_t>(c) * static_cast<size_t>((rc + 7U) / 8U);
  const auto bytes = static_cast<size_t>((rc + 7U) / 8U);
  if (off + bytes > f.null_bitmaps.size()) {
    return true;  // defensive
  }
  for (size_t i = 0; i < bytes; ++i) {
    if (static_cast<uint8_t>(f.null_bitmaps[off + i]) != 0) {
      return true;
    }
  }
  return false;
}

// Null-bitmap pointer for cell (g, c), or nullptr when the cell provably
// has no nulls (feature off, or a truncated footer where CellIsNull would
// also report "not null"). Replaces per-row CellIsNull calls in scan loops:
// CellIsNull re-walks all preceding row groups for its offset, so calling
// it per row is O(row_groups) per row.
const uint8_t* RgColNullBits(const PaxChunk::Footer& f, uint32_t g,
                             uint32_t c) {
  if (!f.null_present || c >= f.column_count || g >= f.row_group_count ||
      g >= f.row_counts.size()) {
    return nullptr;
  }
  if (f.null_summary_valid && !f.null_any) {
    return nullptr;
  }
  const uint32_t cc = f.column_count;
  size_t off = 0;
  if (f.null_summary_valid && g < f.null_uniform_groups) {
    off = static_cast<size_t>(g) * static_cast<size_t>(f.null_uniform_stride);
  } else {
    for (uint32_t gg = 0; gg < g; ++gg) {
      off += static_cast<size_t>(cc) *
             static_cast<size_t>((f.row_counts[gg] + 7U) / 8U);
    }
  }
  off +=
      static_cast<size_t>(c) * static_cast<size_t>((f.row_counts[g] + 7U) / 8U);
  const auto bytes = static_cast<size_t>((f.row_counts[g] + 7U) / 8U);
  if (off + bytes > f.null_bitmaps.size()) {
    return nullptr;
  }
  return f.null_bitmaps.data() + off;
}

// End of the encoded region for cell (g, col): the successor cell's offset,
// or the end of the data area for the final cell. Returns nullptr only when
// the footer is too malformed to derive ANY bound (callers must then take
// the generic/defensive path instead of trusting the region). A hostile
// SUCCESSOR OFFSET VALUE (past the data area) clamps to the data-area end:
// the decoders interpret nullptr as "unbounded, caller pre-validated", so
// returning nullptr here would silently disarm every per-scheme extent
// check in Int64RegionAt / RegionLowerBound and let a hand-built footer
// read hundreds of KB past the buffer.
const char* RegionEndAt(const PaxChunk::Footer& f, uint32_t g, uint32_t col,
                        const std::vector<char>& data) {
  if (col >= f.column_count) {
    return nullptr;
  }
  const size_t off_idx =
      (static_cast<size_t>(g) * static_cast<size_t>(f.column_count)) + col;
  if (off_idx >= f.column_offsets.size()) {
    return nullptr;
  }
  if (data.size() <= static_cast<size_t>(f.footer_size) + 4) {
    return nullptr;
  }
  const size_t data_end = data.size() - f.footer_size - 4;
  if (off_idx + 1 < f.column_offsets.size()) {
    if (f.column_offsets[off_idx + 1] > data_end) {
      // Hostile successor offset: clamp to the data-area end (see comment
      // above) instead of returning nullptr, which the region decoders
      // treat as "no bound".
      return data.data() + data_end;
    }
    return data.data() + f.column_offsets[off_idx + 1];
  }
  return data.data() + data_end;
}

// Bulk-decode one whitelisted inner region (see InnerSchemeOk) into
// out[0..cnt). Returns false on any structural surprise; callers then fall
// back to per-element Int64RegionAt, which fails closed identically.
bool DecodeInnerI64All(const char* r, uint8_t scheme, uint32_t param,
                       uint32_t cnt, const char* end, int64_t* out) {
  if (!InnerSchemeOk(scheme)) {
    return false;
  }
  if (static_cast<Scheme>(scheme) == Scheme::kRle) {
    // Single-pass run expansion; the per-element Int64RegionAt walk is
    // O(runs) per row.
    if (r + 4 > end || r + 4 + (static_cast<size_t>(param) * 12) > end) {
      return false;
    }
    uint32_t row = 0;
    const char* p = r + 4;
    for (uint32_t i = 0; i < param && row < cnt; ++i) {
      const auto v = static_cast<int64_t>(ReadBE64(p));
      const uint64_t len = ReadBE32(p + 8);
      const uint32_t e =
          (row + len > cnt) ? cnt : static_cast<uint32_t>(row + len);
      std::fill(out + row, out + e, v);
      row = e;
      p += 12;
    }
    std::fill(out + row, out + cnt, 0);
    return true;
  }
  for (uint32_t i = 0; i < cnt; ++i) {
    out[i] = Int64RegionAt(r, static_cast<Scheme>(scheme), param, cnt, i, end);
  }
  return true;
}

// Bulk decode for the cascaded composites (kFreq / kRleX / kDictX), shared
// by DecodeInt64All and DecodeInt64ColumnRg. Returns false when `s` is not
// one of the three schemes (caller continues its per-scheme cascade), true
// once `out` is filled -- including the fail-closed all-zeros cases that
// mirror Int64RegionAt's clamps.
bool DecodeCascadeI64All(const char* region, Scheme s, uint32_t param,
                         uint32_t n, const char* region_end, int64_t* out) {
  const auto zero = [&] { std::fill(out, out + n, 0); };
  if (s == Scheme::kHuffDict) {
    HuffDictRegion p;
    if (!ParseHuffDictRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeHuffDictAll(p, out);
    return true;
  }
  if (s == Scheme::kByteSlice) {
    if (!DecodeByteSliceAll(region, n, region_end, out)) {
      zero();
    }
    return true;
  }
  if (s == Scheme::kS4) {
    // Worth a bulk path for the same reason kBp128 has one: the per-element
    // arm re-parses the header and re-reads the checkpoint word n times, so
    // a full-cell scan would pay a redundant bounds check per row.
    S4Region parsed;
    if (!ParseS4Region(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    DecodeS4All(parsed, out);
    return true;
  }
  if (s == Scheme::kSfor) {
    // Same shape as the kS4 arm: one parse, then a sequential group walk
    // (the per-row arm would re-bisect the directory per value).
    SforRegion parsed;
    if (!ParseSforRegion(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    DecodeSforAll(parsed, out);
    return true;
  }
  if (s == Scheme::kExpFor) {
    // Same shape as the kSfor arm: one parse, then a sequential group walk.
    ExpForRegion parsed;
    if (!ParseExpForRegion(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    DecodeExpForAll(parsed, out);
    return true;
  }
  if (s == Scheme::kSortDict) {
    // Two phases: one sequential sorted walk into a cache-resident scratch,
    // then the rank gather -- the permutation applied value-side, so each
    // sorted value is decoded exactly once (the per-row arm would replay the
    // block gaps once per row).
    SortDictRegion p;
    if (!ParseSortDictRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    thread_local std::vector<int64_t> sorted;
    sorted.resize(n);
    {
      uint32_t j = 0;
      for (uint32_t b = 0; b < p.nb; ++b) {
        const int w = SortDictWidth(p, b);
        const uint64_t bit = SortDictGapOffset(p, b) * 8;
        auto v = static_cast<uint64_t>(SortDictFirst(p, b));
        const uint32_t rows = SortDictBlockRows(p, b);
        for (uint32_t t = 0; t < rows; ++t) {
          sorted[j++] = static_cast<int64_t>(v);
          if (w != 0 && t + 1 < rows) {
            v += UnpackBitAt(
                p.gaps,
                bit + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w)), w);
          }
        }
      }
    }
    for (uint32_t i = 0; i < n; ++i) {
      const uint32_t r = SortDictRankAt(p, i);
      out[i] = r < n ? sorted[r] : 0;  // hostile-rank clamp bucket
    }
    return true;
  }
  if (s == Scheme::kPostings) {
    // One sequential pass over the d sub-streams scattering values to rows
    // (the per-row arm would re-walk the count table per probe).
    PostingsRegion p;
    if (!ParsePostingsRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodePostingsAll(p, out);
    return true;
  }
  if (s == Scheme::kBp128) {
    Bp128Region parsed;
    if (!ParseBp128Region(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    DecodeBp128All(parsed, out);
    return true;
  }
  if (s == Scheme::kSimdFor) {
    SimdForRegion p{};
    if (!ParseSimdForRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeSimdForAll(p, out);
    return true;
  }
  if (s == Scheme::kD2Pfor) {
    D2PforRegion p;
    if (!ParseD2PforRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeD2PforAll(p, out);
    return true;
  }
  if (s == Scheme::kFastPfor) {
    // Same shape as the kBp128 arm: one parse, then a per-block lane decode
    // (the AVX2 kernels where compiled in) plus a bitmap-driven exception
    // patch (the per-row arm would re-walk the metadata per probe).
    FastPforRegion parsed;
    if (!ParseFastPforRegion(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    DecodeFastPforAll(parsed, out);
    return true;
  }
  if (s == Scheme::kDeltaFastPfor) {
    // Same shape as the kFastPfor arm: one parse, then a per-block lane
    // decode (AVX2 kernels) plus an exception patch and the sequential
    // in-block replay the delta chain requires (<= 127 dependent adds).
    DeltaFastPforRegion parsed;
    if (!ParseDeltaFastPforRegion(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    DecodeDeltaFastPforAll(parsed, out);
    return true;
  }
  if (s == Scheme::kSimple8b) {
    if (param == 0 || region_end == nullptr ||
        region + 8 + (static_cast<size_t>(param) * 8) > region_end) {
      zero();
      return true;
    }
    const uint64_t base_u = ReadBE64(region);
    uint32_t row = 0;
    const char* p = region + 8;
    for (uint32_t w = 0; w < param && row < n; ++w, p += 8) {
      const uint64_t word = ReadBE64(p);
      const auto sel = static_cast<uint32_t>(word >> 60);
      const uint32_t cnt = kSimple8bSelCount[sel];
      const uint32_t bits = kSimple8bSelBits[sel];
      const uint32_t avail = n - row;
      const uint32_t take = cnt < avail ? cnt : avail;
      if (bits == 0) {
        // Zero-width word: every covered row holds the base value.
        std::fill(out + row, out + row + take, static_cast<int64_t>(base_u));
      } else {
        const uint64_t mask = (UINT64_C(1) << bits) - 1;
        for (uint32_t j = 0; j < take; ++j) {
          const uint64_t off = (word >> (bits * (cnt - 1 - j))) & mask;
          out[row + j] = static_cast<int64_t>(base_u + off);
        }
      }
      row += take;
    }
    std::fill(out + row, out + n, 0);  // truncated tail: decode-as-0 contract
    return true;
  }
  if (s == Scheme::kQpp) {
    QppRegion p;
    if (!ParseQppRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeQppAll(p, out);
    return true;
  }
  if (s == Scheme::kDeltaPfor) {
    DeltaPForRegion p;
    if (!ParseDeltaPForRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeDeltaPForAll(p, out);
    return true;
  }
  if (s == Scheme::kRleBp) {
    RleBpRegion p;
    if (!ParseRleBpRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    uint64_t row = 0;
    for (uint32_t run = 0; run < p.runs; ++run) {
      const uint32_t length = RleBpLengthAt(p, run);
      const int64_t value = RleBpValueAt(p, run);
      std::fill(out + row, out + row + length, value);
      row += length;
    }
    return true;
  }
  if (s == Scheme::kRleIdx) {
    // Single-pass run expansion (the kRleBp shape, but the two payloads are
    // byte-aligned FOR-BA streams so each run is two byte-assembly loads
    // instead of two bit-unpacks). Rows past the last run -- only reachable
    // from a malformed stream, since ValidateWalk proves the runs tile n --
    // decode as raw 0, matching the kRle / kRleBp rule.
    RleIdxRegion p;
    if (!ParseRleIdxRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    uint32_t run = 0;
    uint64_t row = 0;
    while (run < p.runs && row < n) {
      const uint32_t length = RleIdxLengthAt(p, run);
      const int64_t value = RleIdxValueAt(p, run);
      const uint32_t end =
          (row + length > n) ? n : static_cast<uint32_t>(row + length);
      std::fill(out + row, out + end, value);
      row = end;
      ++run;
    }
    if (row < n) {
      std::fill(out + row, out + n, 0);
    }
    return true;
  }
  if (s == Scheme::kStreamVByte) {
    // One forward pass: the width selector and the payload cursor advance
    // together, so the whole cell costs one control-byte load and one
    // width-sized payload load per value -- no bit unpacking and no walk
    // from a checkpoint.
    StreamVByteRegion p;
    if (!ParseStreamVByteRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeStreamVByteAll(p, out);
    return true;
  }
  if (s == Scheme::kChimp128) {
    Chimp128Region parsed;
    if (!ParseChimp128Region(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    std::array<uint64_t, kChimp128BlockRows> decoded{};
    for (uint32_t block = 0; block < parsed.block_count; ++block) {
      if (!DecodeChimp128Block(parsed.blocks[block], decoded.data())) {
        zero();
        return true;
      }
      const uint32_t start = block * kChimp128BlockRows;
      const uint32_t rows = parsed.blocks[block].rows;
      for (uint32_t row = 0; row < rows; ++row) {
        out[start + row] = static_cast<int64_t>(decoded[row]);
      }
    }
    return true;
  }
  if (s == Scheme::kAlpRdTans) {
    AlpRdTansRegion parsed;
    if (!ParseAlpRdTansRegion(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    for (uint32_t block = 0; block < parsed.block_count; ++block) {
      std::vector<uint64_t> decoded;
      if (!AlpRdTansDecodeBlock(parsed, block, &decoded)) {
        zero();
        return true;
      }
      const uint32_t start = block * kAlpRdTansBlockRows;
      for (uint32_t row = 0; row < decoded.size(); ++row) {
        out[start + row] = static_cast<int64_t>(decoded[row]);
      }
    }
    return true;
  }
  if (s == Scheme::kPatas) {
    PatasRegion parsed;
    if (!ParsePatasRegion(region, n, param, region_end, &parsed)) {
      zero();
      return true;
    }
    std::array<uint64_t, kPatasBlockRows> decoded{};
    for (uint32_t block = 0; block < parsed.block_count; ++block) {
      if (!DecodePatasBlock(parsed.blocks[block], decoded.data())) {
        zero();
        return true;
      }
      const uint32_t start = block * kPatasBlockRows;
      const uint32_t rows = parsed.blocks[block].rows;
      for (uint32_t row = 0; row < rows; ++row) {
        out[start + row] = static_cast<int64_t>(decoded[row]);
      }
    }
    return true;
  }
  if (s == Scheme::kRleDelta) {
    RleDeltaRegion p;
    if (!ParseRleDeltaRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    DecodeRleDeltaAll(region, n, param, region_end, out);
    return true;
  }
  if (s == Scheme::kFreq) {
    // [top i64][ischeme u8][iparam u32][nexc u16][bitmap][inner region].
    if (region_end == nullptr || region + 15 > region_end) {
      zero();
      return true;
    }
    const auto top = static_cast<int64_t>(ReadBE64(region));
    const auto ischeme = static_cast<uint8_t>(region[8]);
    const uint32_t iparam = ReadBE32(region + 9);
    const uint32_t nexc = ReadBE16(region + 13);
    const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
    if (region + 15 + bmp_bytes > region_end || !InnerSchemeOk(ischeme)) {
      zero();
      return true;
    }
    const char* bmp = region + 15;
    const char* inner = bmp + bmp_bytes;
    std::vector<int64_t> exc(nexc, 0);
    DecodeInnerI64All(inner, ischeme, iparam, nexc, region_end, exc.data());
    // Clear bits beyond nexc decode as 0 (the scalar arm's eidx >= nexc
    // clamp), so the walk does NOT pre-fill with top.
    uint32_t eidx = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const auto bit = static_cast<uint8_t>(
          (static_cast<uint8_t>(bmp[i >> 3]) >> (7 - (i & 7))) & 1);
      if (bit != 0) {
        out[i] = top;
      } else {
        out[i] = (eidx < nexc) ? exc[eidx] : 0;
        ++eidx;
      }
    }
    return true;
  }
  if (s == Scheme::kTopk) {
    // [d u16][nexc u16][hot 15x8B][freq 15xu16][ischeme u8][iparam u32]
    // [nibbles ceil(n/2)][inner region]. Nibble < d -> hot[d]; 15 -> next
    // exception. Codes d..14 never appear from the encoder; a hostile one
    // decodes as 0 like the clamp arms.
    constexpr size_t kTopkHeader = 2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;  // 159
    if (region_end == nullptr || region + kTopkHeader > region_end) {
      zero();
      return true;
    }
    const uint32_t d = ReadBE16(region);
    const uint32_t nexc = ReadBE16(region + 2);
    const auto ischeme = static_cast<uint8_t>(region[154]);
    const uint32_t iparam = ReadBE32(region + 155);
    const size_t nib_bytes = (static_cast<size_t>(n) + 1) / 2;
    if (d == 0 || d > 15 || region + kTopkHeader + nib_bytes > region_end ||
        (nexc > 0 && !InnerSchemeOk(ischeme))) {
      zero();
      return true;
    }
    std::vector<int64_t> exc(nexc, 0);
    if (nexc > 0) {
      DecodeInnerI64All(region + kTopkHeader + nib_bytes, ischeme, iparam, nexc,
                        region_end, exc.data());
    }
    uint32_t eidx = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const auto byte = static_cast<uint8_t>(region[kTopkHeader + (i >> 1)]);
      const uint32_t code = (i & 1) == 0 ? (byte >> 4) : (byte & 0x0F);
      if (code < d) {
        out[i] = static_cast<int64_t>(
            ReadBE64(region + 4 + (static_cast<size_t>(code) * 8)));
      } else if (code == 15 && eidx < nexc) {
        out[i] = exc[eidx++];
      } else {
        out[i] = 0;  // hostile code or sentinel over-run: raw-0 clamp
      }
    }
    return true;
  }
  if (s == Scheme::kTileDict) {
    // One tile parse + one bit extraction + one dict load per row. The
    // cursor's own bounds checks gate the walk; a chain that fails them
    // (or dies partway) zero-fills the tail, the same clamp convention as
    // every composite's hostile-header arm.
    std::fill(out, out + n, 0);
    TileDictCursor c;
    if (!TileDictFirst(region, n, region_end, &c)) {
      return true;
    }
    for (;;) {
      if (c.w == 0) {
        const auto v = static_cast<int64_t>(ReadBE64(c.dict));
        std::fill(out + c.tile_start, out + c.tile_start + c.rows, v);
      } else {
        for (uint32_t j = 0; j < c.rows; ++j) {
          out[c.tile_start + j] = TileDictValueAt(c, j);
        }
      }
      if (!TileDictNext(&c)) {
        break;
      }
    }
    return true;
  }
  if (s == Scheme::kRleX) {
    if (region_end == nullptr || region + 18 > region_end) {
      zero();
      return true;
    }
    const auto vs = static_cast<uint8_t>(region[0]);
    const auto cs = static_cast<uint8_t>(region[1]);
    const uint32_t runs = ReadBE32(region + 2);
    const uint32_t vparam = ReadBE32(region + 6);
    const uint32_t cparam = ReadBE32(region + 10);
    const uint64_t vlen = ReadBE32(region + 14);
    if (std::cmp_greater(vlen, region_end - (region + 18)) || runs > n ||
        runs == 0 || !InnerSchemeOk(vs) || !InnerSchemeOk(cs)) {
      zero();
      return true;
    }
    const char* vreg = region + 18;
    const char* creg = vreg + vlen;
    std::vector<int64_t> run_vals(runs, 0), run_lens(runs, 0);
    DecodeInnerI64All(vreg, vs, vparam, runs, creg, run_vals.data());
    DecodeInnerI64All(creg, cs, cparam, runs, region_end, run_lens.data());
    uint64_t row = 0;
    for (uint32_t i = 0; i < runs && row < n; ++i) {
      const auto len = static_cast<uint64_t>(run_lens[i]);
      const uint32_t e = (row + len > n) ? n : static_cast<uint32_t>(row + len);
      std::fill(out + row, out + e, run_vals[i]);
      row = e;
    }
    std::fill(out + row, out + n, 0);  // tail rows decode as 0 (kRle rule)
    return true;
  }
  if (s == Scheme::kDictX) {
    if (region_end == nullptr || region + 11 > region_end) {
      zero();
      return true;
    }
    const uint16_t d = ReadBE16(region);
    const auto cs = static_cast<uint8_t>(region[2]);
    const uint32_t cparam = ReadBE32(region + 3);
    const uint64_t clen = ReadBE32(region + 7);
    const char* creg = region + 11;
    if (d == 0 || std::cmp_greater(clen, region_end - creg) ||
        !InnerSchemeOk(cs)) {
      zero();
      return true;
    }
    const char* pool_hdr = creg + clen;
    if (pool_hdr + 1 > region_end || static_cast<uint8_t>(pool_hdr[0]) != 0 ||
        pool_hdr + 1 + (static_cast<size_t>(d) * 8) > region_end) {
      zero();
      return true;
    }
    const char* dict = pool_hdr + 1;
    std::vector<int64_t> codes(n, 0);
    DecodeInnerI64All(creg, cs, cparam, n, pool_hdr, codes.data());
    for (uint32_t i = 0; i < n; ++i) {
      const auto code = static_cast<uint64_t>(codes[i]);
      out[i] = (code >= d) ? 0
                           : static_cast<int64_t>(ReadBE64(
                                 dict + (static_cast<size_t>(code) * 8)));
    }
    return true;
  }
  if (s == Scheme::kArith) {
    // [a i64][d i64][ischeme u8][iparam u32][nexc u16][bitmap][inner].
    if (region_end == nullptr || region + 23 > region_end) {
      zero();
      return true;
    }
    const uint64_t a = ReadBE64(region);
    const uint64_t d = ReadBE64(region + 8);
    const auto ischeme = static_cast<uint8_t>(region[16]);
    const uint32_t iparam = ReadBE32(region + 17);
    const uint32_t nexc = ReadBE16(region + 21);
    const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
    if (region + 23 + bmp_bytes > region_end ||
        (nexc > 0 && !InnerSchemeOk(ischeme))) {
      zero();
      return true;
    }
    const auto* bmp = reinterpret_cast<const uint8_t*>(region + 23);
    std::vector<int64_t> exc(nexc, 0);
    if (nexc > 0) {
      DecodeInnerI64All(region + 23 + bmp_bytes, ischeme, iparam, nexc,
                        region_end, exc.data());
    }
    // Clear bits beyond nexc decode as raw 0 (the scalar arm's clamp), so
    // the walk does not read past the exception list.
    uint32_t eidx = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const auto bit = static_cast<uint8_t>((bmp[i >> 3] >> (7 - (i & 7))) & 1);
      if (bit != 0) {
        out[i] = static_cast<int64_t>(a + (static_cast<uint64_t>(i) * d));
      } else if (eidx < nexc) {
        out[i] = exc[eidx];
      } else {
        out[i] = 0;
      }
      if (bit == 0) {
        ++eidx;
      }
    }
    return true;
  }
  if (s == Scheme::kMod) {
    // [a 8B][b 8B][m 8B][ischeme u8][iparam u32][nexc u16][bitmap][inner].
    // One add-and-wrap walk for the on-model rows, exceptions inner-decoded
    // once up front. ParseModRegion pins the header fields (m range, param
    // echo, inner whitelist) that the scalar arm re-derives with clamps.
    ModRegion p;
    if (region_end == nullptr ||
        !ParseModRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    const uint32_t nexc = ReadBE16(p.hdr + 5);
    std::vector<int64_t> exc(static_cast<size_t>(nexc), 0);
    if (nexc > 0) {
      DecodeInnerI64All(p.bmp + ((static_cast<size_t>(n) + 7) / 8),
                        static_cast<uint8_t>(p.hdr[0]), ReadBE32(p.hdr + 1),
                        nexc, region_end, exc.data());
    }
    uint32_t eidx = 0;
    uint64_t cur = p.a;
    for (uint32_t i = 0; i < n; ++i) {
      const auto bit =
          static_cast<uint8_t>((p.bmp[i >> 3] >> (7 - (i & 7))) & 1);
      if (bit != 0) {
        out[i] = static_cast<int64_t>(cur);
      } else if (eidx < nexc) {
        out[i] = exc[eidx];
        ++eidx;
      } else {
        out[i] = 0;
      }
      cur += p.b;
      if (cur >= p.m) {
        cur -= p.m;
      }
    }
    return true;
  }
  if (s == Scheme::kLane) {
    // [nd u8][nd x (base 8B][step 8B)][ischeme u8][iparam u32][nexc u16]
    // [bitmap][inner]. One add-per-row walk over K per-lane running cursors,
    // exceptions inner-decoded once up front. ParseLaneRegion pins the
    // header fields (nd range, param echo, inner whitelist) that the scalar
    // arm re-derives with clamps.
    LaneRegion p;
    if (region_end == nullptr ||
        !ParseLaneRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    std::vector<int64_t> exc(static_cast<size_t>(p.nexc), 0);
    if (p.nexc > 0) {
      DecodeInnerI64All(p.bmp + ((static_cast<size_t>(n) + 7) / 8), p.ischeme,
                        p.iparam, p.nexc, region_end, exc.data());
    }
    std::array<uint64_t, 16> cur{};
    std::array<uint64_t, 16> step{};
    for (uint32_t l = 0; l < p.nd; ++l) {
      cur[l] = ReadBE64(p.base + (static_cast<size_t>(l) * 16));
      step[l] = ReadBE64(p.base + (static_cast<size_t>(l) * 16) + 8);
    }
    uint32_t eidx = 0;
    uint32_t l = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const auto bit =
          static_cast<uint8_t>((p.bmp[i >> 3] >> (7 - (i & 7))) & 1);
      if (bit != 0) {
        out[i] = static_cast<int64_t>(cur[l]);
      } else if (eidx < p.nexc) {
        out[i] = exc[eidx];
        ++eidx;
      } else {
        out[i] = 0;
      }
      cur[l] += step[l];  // the lane grid advances through exception rows too
      if (++l == p.nd) {
        l = 0;
      }
    }
    return true;
  }
  if (s == Scheme::kPla) {
    // [nseg u16][nseg x (first, d, end)][ischeme u8][iparam u32][nexc u16]
    // [bitmap][inner]. One cursor walk: the segment cursor only moves
    // forward, exceptions inner-decode once up front, on-line rows are an
    // FMA off the current segment line.
    PlaRegion p;
    if (region_end == nullptr ||
        !ParsePlaRegion(region, n, param, region_end, &p)) {
      zero();
      return true;
    }
    const uint32_t nexc = ReadBE16(p.hdr + 5);
    std::vector<int64_t> exc(static_cast<size_t>(nexc), 0);
    if (nexc > 0) {
      DecodeInnerI64All(p.bmp + ((static_cast<size_t>(n) + 7) / 8),
                        static_cast<uint8_t>(p.hdr[0]), ReadBE32(p.hdr + 1),
                        nexc, region_end, exc.data());
    }
    uint32_t j = 0;
    uint32_t start = 0;
    uint32_t eidx = 0;
    for (uint32_t i = 0; i < n; ++i) {
      while (i >= ReadBE32(p.segs + (static_cast<size_t>(j) * 20) + 16)) {
        start = ReadBE32(p.segs + (static_cast<size_t>(j) * 20) + 16);
        ++j;
      }
      const auto bit = static_cast<uint8_t>(
          (static_cast<uint8_t>(p.bmp[i >> 3]) >> (7 - (i & 7))) & 1);
      if (bit != 0) {
        out[i] = PlaSegmentValueAt(p, j, start, i);
      } else if (eidx < nexc) {
        out[i] = exc[eidx];
        ++eidx;
      } else {
        out[i] = 0;  // clear bits beyond nexc decode as raw 0 (scalar clamp)
      }
    }
    return true;
  }
  if (s == Scheme::kSparse) {
    // [vs u8][vprm u32][vlen u32][pos_bpv u8][nnz u32 BE]
    // [FOR-BA positions][values]. O(nnz): zero-fill, decode the nnz
    // positions and dense values, scatter. Bit-equal to per-row
    // Int64RegionAt (binary-search rank per row).
    if (region_end == nullptr || region + 14 > region_end) {
      zero();
      return true;
    }
    const auto vs = static_cast<uint8_t>(region[0]);
    const uint32_t vprm = ReadBE32(region + 1);
    const uint32_t pbpv = static_cast<uint8_t>(region[9]);
    const uint32_t nnz = ReadBE32(region + 10);
    if (nnz == 0 || nnz > n || pbpv < 1 || pbpv > 8 || !InnerSchemeOk(vs)) {
      zero();
      return true;
    }
    const size_t pos_bytes = 8 + (static_cast<size_t>(nnz) * pbpv);
    if (region + 14 + pos_bytes > region_end) {
      zero();
      return true;
    }
    const char* posr = region + 14;
    const char* vreg = posr + pos_bytes;
    zero();
    for (uint32_t r = 0; r < nnz; ++r) {
      const int64_t p =
          Int64RegionAt(posr, Scheme::kForBp, pbpv, nnz, r, region_end);
      if (p < 0 || std::cmp_greater_equal(p, n)) {
        continue;  // hostile pos
      }
      out[p] = Int64RegionAt(vreg, static_cast<Scheme>(vs), vprm, nnz, r,
                             region_end);
    }
    return true;
  }
  if (s == Scheme::kAnsFor) {
    // One forward pass from checkpoint 0 (which covers row 0): decode the
    // n*bpv offset bytes and assemble each value. Bit-equal to the per-row
    // Int64RegionAt arm, which enters at the nearest row-64 checkpoint.
    // No footer param is threaded here (same constraint as kSparse): pass
    // the header's own bpv as the width so AnsForParse's echo check holds.
    AnsForRegion A;
    const uint32_t hbpv = (region_end != nullptr && region + 9 <= region_end)
                              ? static_cast<uint8_t>(region[8])
                              : 0;
    if (!AnsForParse(region, region_end, n, hbpv, &A)) {
      zero();
      return true;
    }
    uint32_t x = ReadBE32(A.ckpt);
    uint32_t pos = ReadBE32(A.ckpt + 4);
    for (uint32_t r = 0; r < n; ++r) {
      uint64_t off = 0;
      for (uint32_t b = 0; b < A.bpv; ++b) {
        uint8_t sym = 0;
        if (!AnsForStep(A, &x, &pos, &sym)) {
          zero();
          return true;  // hostile stream: fail closed like the other arms
        }
        off |= static_cast<uint64_t>(sym) << (8 * b);
      }
      out[r] = static_cast<int64_t>(static_cast<uint64_t>(A.base) + off);
    }
    return true;
  }
  if (s == Scheme::kInterp) {
    // [ncp u16][ckpts ncp x (first,last)][bitoffs ncp x u32][BIC stream].
    // One in-order tree decode per checkpoint block; bit-equal to the
    // per-row Int64RegionAt arm, which replays the same block. No footer
    // param is threaded on the batch path (same constraint as kSparse /
    // kAnsFor): the header's own ncp drives the echo check.
    const uint32_t hncp = (region_end != nullptr && region + 2 <= region_end)
                              ? ReadBE16(region)
                              : 0;
    InterpRegion p;
    if (!ParseInterpRegion(region, n, hncp, region_end, &p)) {
      zero();
      return true;
    }
    DecodeInterpAll(p, out);
    return true;
  }
  if (s == Scheme::kXorForC) {
    // [ncp u16][ckpts ncp x u64][w u8][xors]. One linear XOR pass; the
    // checkpoint table only matters for point access (bit-equal to the
    // per-row arm, which jumps to ckpts[idx/128]).
    if (region_end == nullptr || region + 3 > region_end) {
      zero();
      return true;
    }
    const uint32_t ncp = ReadBE16(region);
    if (ncp != (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery ||
        region + 3 + (static_cast<size_t>(ncp) * 8) > region_end) {
      zero();
      return true;
    }
    const uint32_t w = static_cast<uint8_t>(region[2 + (ncp * 8)]);
    if (w < 1 || w > 64 ||
        region + 3 + (static_cast<size_t>(ncp) * 8) +
                ((static_cast<size_t>(n > 0 ? n - 1 : 0) * w + 7) / 8) >
            region_end) {
      zero();
      return true;
    }
    const char* packed = region + 3 + (static_cast<size_t>(ncp) * 8);
    out[0] = static_cast<int64_t>(ReadBE64(region + 2));
    auto x = static_cast<uint64_t>(out[0]);
    for (uint32_t i = 1; i < n; ++i) {
      x ^= UnpackBitAt(packed, static_cast<uint64_t>(i - 1) * w,
                       static_cast<int>(w));
      out[i] = static_cast<int64_t>(x);
    }
    return true;
  }
  if (s == Scheme::kGorillaC || s == Scheme::kChimpC) {
    // [ncp u16][ckpts ncp x u64][bitoffs ncp x u32][first u64][stream].
    // Linear pass from row 0; the seek tables are validated (monotonic,
    // inside the stream) but unused on the batch path.
    if (region_end == nullptr || region + 10 > region_end) {
      zero();
      return true;
    }
    const uint32_t ncp = ReadBE16(region);
    if (ncp != (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery ||
        region + 2 + (static_cast<size_t>(ncp) * 12) + 8 > region_end) {
      zero();
      return true;
    }
    const char* offs = region + 2 + (static_cast<size_t>(ncp) * 8);
    const char* firstp = offs + (static_cast<size_t>(ncp) * 4);
    const char* stream = firstp + 8;
    const uint64_t stream_bits = static_cast<uint64_t>(region_end - stream) * 8;
    uint32_t prev_off = 0;
    for (uint32_t b = 0; b < ncp; ++b) {
      const uint32_t o = ReadBE32(offs + (static_cast<size_t>(b) * 4));
      if (o < prev_off || o > stream_bits || (b == 0 && o != 0)) {
        zero();
        return true;
      }
      prev_off = o;
    }
    uint64_t prev = ReadBE64(firstp);
    out[0] = static_cast<int64_t>(prev);
    BitReader br(stream, region_end);
    if (s == Scheme::kGorillaC) {
      for (uint32_t i = 1; i < n; ++i) {
        if (br.get1() != 0) {
          const int lz = static_cast<int>(br.get(5));
          const int tz = static_cast<int>(br.get(6));
          int mw = 64 - lz - tz;
          mw = std::max(mw, 0);
          mw = std::min(mw, 64);
          const uint64_t m = br.get(mw);
          prev ^= (mw >= 64 ? m : (m << tz));
        }
        out[i] = static_cast<int64_t>(prev);
      }
    } else {
      int prev_lz = 0, prev_tz = 64;  // mirrors the encoder's forced '11'
      for (uint32_t i = 1; i < n; ++i) {
        if (br.get1() == 0) {
          // identical
        } else if (br.get1() == 0) {
          const int width = 64 - prev_lz - prev_tz;
          if (width > 0 && width <= 64) {
            uint64_t m = 0;
            for (int t = 0; t < width; ++t) {
              m = (m << 1) | static_cast<uint64_t>(br.get1());
            }
            prev ^= (m << prev_tz);
          }
        } else {
          const int lz = static_cast<int>(br.get(5));
          const int tz = static_cast<int>(br.get(6));
          int mw = 64 - lz - tz;
          mw = std::max(mw, 1);
          mw = std::min(mw, 64);
          prev ^= (mw >= 64 ? br.get(mw) : (br.get(mw) << tz));
          prev_lz = lz;
          prev_tz = tz;
        }
        out[i] = static_cast<int64_t>(prev);
      }
    }
    return true;
  }
  if (s == Scheme::kWah) {
    // [tv 8B][fv 8B][words x 8B]. One word walk expands every span; the
    // per-row fallback would re-walk the words for every row.
    if (region_end == nullptr || param == 0 ||
        region + 16 + (static_cast<size_t>(param) * 8) > region_end) {
      zero();
      return true;
    }
    const auto tv = static_cast<int64_t>(ReadBE64(region));
    const auto fv = static_cast<int64_t>(ReadBE64(region + 8));
    const char* wp = region + 16;
    uint32_t pos = 0;
    for (uint32_t k = 0; k < param && pos < n; ++k, wp += 8) {
      const uint64_t w = ReadBE64(wp);
      if ((w >> 63) == 0) {
        const uint64_t le = WahLiteralBitsLE(wp);
        const uint32_t span = (n - pos < 63U) ? (n - pos) : 63U;
        for (uint32_t t = 0; t < span; ++t) {
          out[pos + t] = ((le >> t) & 1ULL) != 0 ? tv : fv;
        }
        pos += span;
      } else {
        const uint64_t len = w & kWahLenMask;
        if (len == 0 || len > UINT64_MAX / kWahSpan) {
          zero();
          return true;
        }
        uint64_t rows = len * kWahSpan;
        if (rows > static_cast<uint64_t>(n - pos)) {
          rows = n - pos;
        }
        const int64_t v = ((w >> 62) & 1ULL) != 0 ? tv : fv;
        std::fill(out + pos, out + pos + static_cast<size_t>(rows), v);
        pos += static_cast<uint32_t>(rows);
      }
    }
    std::fill(out + pos, out + n, 0);  // uncovered tail rows decode as 0
    return true;
  }
  return false;
}
// offset VALUE lands inside the data area (or exactly at its end for a
// zero-size region). The array-index checks several callers already perform
// only prove the offset exists; a hostile offset VALUE (e.g. 0xFFFFFFFF)
// still formed a wild pointer before any dereference. Returns nullptr on a
// hostile offset so callers fail closed (kCorruption / skip) instead of
// dereferencing. need_bytes is a caller's minimum payload (0 = only the
// pointer-formation check); fine-grained per-scheme bounds live in the
// decoders via region_end, since the minimum region size is scheme- and
// rowcount-dependent (a 1-row ByteSlice cell is 2 bytes).
const char* RegionStartAt(const PaxChunk::Footer& f, uint32_t g, uint32_t col,
                          const std::vector<char>& data, size_t need_bytes) {
  if (col >= f.column_count) {
    return nullptr;
  }
  const size_t off_idx =
      (static_cast<size_t>(g) * static_cast<size_t>(f.column_count)) + col;
  if (off_idx >= f.column_offsets.size()) {
    return nullptr;
  }
  if (data.size() <= static_cast<size_t>(f.footer_size) + 4) {
    return nullptr;
  }
  const size_t data_end = data.size() - f.footer_size - 4;
  const size_t off = f.column_offsets[off_idx];
  if (off > data_end || data_end - off < need_bytes) {
    return nullptr;
  }
  return data.data() + off;
}

// Popcount of selection bits on [start, end).
uint64_t SelPopcountRange(const PaxSelectionVector& sel, uint32_t start,
                          uint32_t end) {
  uint64_t cnt = 0;
  uint32_t i = start;
  while (i + 64 <= end) {
    cnt += static_cast<uint64_t>(__builtin_popcountll(sel.bits[i >> 6]));
    i += 64;
  }
  for (; i < end; ++i) {
    if (((sel.bits[i >> 6] >> (i & 63)) & 1ULL) != 0U) {
      ++cnt;
    }
  }
  return cnt;
}

// Selected rows on [start, end) that are NOT null. With no bitmap every
// selected row is non-null (nulls are exactly the bitmap-set rows).
uint64_t SelNonNullCount(const PaxSelectionVector& sel, const uint8_t* nulls,
                         uint32_t start, uint32_t end) {
  if (nulls == nullptr) {
    return SelPopcountRange(sel, start, end);
  }
  uint64_t cnt = 0;
  for (uint32_t i = start; i < end; ++i) {
    // static_cast: a bare uint8_t promotes to int, and -Wsign-conversion
    // flags the int -> unsigned conversion at the `& 1U` below.
    if (((sel.bits[i >> 6] >> (i & 63)) & 1ULL) != 0 &&
        ((static_cast<unsigned>(nulls[i >> 3]) >> (7U - (i & 7U))) & 1U) == 0) {
      ++cnt;
    }
  }
  return cnt;
}

Value VarcharRegionAtFsstFallback(const char* r, uint32_t n, uint32_t idx,
                                  const char* region_end) {
  if (r == nullptr || region_end == nullptr || region_end < r || idx >= n) {
    return Value(std::string());
  }
  const auto region_len = static_cast<size_t>(region_end - r);
  if (region_len < 6) {
    return Value(std::string());
  }
  const uint16_t nsym = ReadBE16(r);
  const uint32_t tsl = ReadBE32(r + 2);
  if (nsym > 256) {
    return Value(std::string());
  }
  std::array<std::string_view, 256> symbols{};
  size_t pos = 6;
  for (uint16_t c = 0; c < nsym; ++c) {
    if (pos >= region_len) {
      return Value(std::string());
    }
    const auto slen = static_cast<uint8_t>(r[pos++]);
    if (slen > region_len - pos) {
      return Value(std::string());
    }
    symbols[c] = std::string_view(r + pos, slen);
    pos += slen;
  }
  const size_t table_bytes = static_cast<size_t>(n) * 4;
  if (table_bytes > region_len - pos) {
    return Value(std::string());
  }
  const char* otable = r + pos;
  pos += table_bytes;
  if (tsl > region_len - pos) {
    return Value(std::string());
  }
  const char* stream = r + pos;
  const uint32_t code_off = ReadBE32(otable + (static_cast<size_t>(idx) * 4));
  const uint32_t next_off =
      (idx + 1 < n) ? ReadBE32(otable + (static_cast<size_t>(idx + 1) * 4))
                    : tsl;
  if (code_off > next_off || next_off > tsl) {
    return Value(std::string());
  }
  std::string out;
  const auto encoded_len = static_cast<size_t>(next_off - code_off);
  if (encoded_len > out.capacity() && encoded_len <= 65536) {
    out.reserve(encoded_len);
  }
  const auto* cp = reinterpret_cast<const uint8_t*>(stream + code_off);
  const auto* end = reinterpret_cast<const uint8_t*>(stream + next_off);
  while (cp < end) {
    const uint8_t code = *cp++;
    if (code == 0) {
      if (cp >= end) {
        break;
      }
      out.push_back(static_cast<char>(*cp++));
    } else if (code <= nsym) {
      out.append(symbols[code - 1].data(), symbols[code - 1].size());
    }
  }
  return Value(std::move(out));
}

bool DecodeFrontCodeAtInto(const char* r, uint32_t param, uint32_t n,
                           uint32_t idx, const char* region_end,
                           std::string* out) {
  if (r == nullptr || region_end == nullptr || region_end < r ||
      out == nullptr || idx >= n) {
    return false;
  }
  const uint32_t re = param & 0x7FFFU;
  if (re == 0 || r + 6 > region_end) {
    return false;
  }
  const uint32_t nrestart = ReadBE16(r);
  const uint32_t stream_len = ReadBE32(r + 2);
  const char* rtable = r + 6;
  if (rtable + (static_cast<size_t>(nrestart) * 4) > region_end) {
    return false;
  }
  const char* stream = rtable + (static_cast<size_t>(nrestart) * 4);
  if (stream + stream_len > region_end) {
    return false;
  }
  const uint32_t block = idx / re;
  if (block >= nrestart) {
    return false;
  }
  const uint32_t ro = ReadBE32(rtable + (static_cast<size_t>(block) * 4));
  if (ro > stream_len) {
    return false;
  }
  const char* send = stream + stream_len;
  const char* q = stream + ro;
  out->clear();
  for (uint32_t i = block * re; i <= idx; ++i) {
    if (q + 4 > send) {
      return false;
    }
    const uint32_t hdr = ReadBE32(q);
    const uint32_t plen = hdr >> 16;
    const uint32_t slen = hdr & 0xFFFFU;
    if (plen > out->size() || slen > static_cast<uint64_t>(send - q) - 4U) {
      return false;
    }
    out->resize(plen);
    out->append(q + 4, slen);
    q += 4 + slen;
  }
  return true;
}

// ---------------------------------------------------------------------------
// kTemplate (scheme 64): structured strings as a literal skeleton plus
// fixed-width digit runs stored as int64 leaf regions. One parse serves the
// random-access decoder, the EQ kernel, and ValidateWalk case 64, so all
// three share the same offset arithmetic (the encoder/parser/validator
// single-parse convention the other composite schemes follow).
// ---------------------------------------------------------------------------

// Bounds-checked structural parse. `param` must echo nexc (the kArith
// convention). Returns false on any disagreement; callers fail closed.
bool ParseTemplateRegion(const char* r, uint32_t n, uint32_t param,
                         const char* region_end, TemplateRegion* out) {
  if (r == nullptr || region_end == nullptr || region_end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const auto have = static_cast<size_t>(region_end - r);
  if (have < 3) {
    return false;
  }
  const uint32_t nseg = static_cast<uint8_t>(r[0]);
  const uint32_t nexc = ReadBE16(r + 1);
  if (nseg < 1 || nseg > kTemplateMaxSegments) {
    return false;
  }
  if (nexc > n || nexc != param) {
    return false;
  }
  size_t pos = 3;
  // Digit widths.
  if (pos + nseg > have) {
    return false;
  }
  std::array<uint32_t, kTemplateMaxSegments> widths{};
  for (uint32_t s = 0; s < nseg; ++s) {
    widths[s] = static_cast<uint8_t>(r[pos + s]);
    if (widths[s] < 1 || widths[s] > kTemplateMaxWidth) {
      return false;
    }
  }
  pos += nseg;
  // Literal skeleton: (nseg+1) length-prefixed pieces, stored once.
  std::array<const char*, kTemplateMaxSegments + 1> lit{};
  std::array<uint32_t, kTemplateMaxSegments + 1> lit_len{};
  for (uint32_t k = 0; k <= nseg; ++k) {
    if (pos + 2 > have) {
      return false;
    }
    const uint32_t len = ReadBE16(r + pos);
    pos += 2;
    if (pos + len > have) {
      return false;
    }
    lit[k] = r + pos;
    lit_len[k] = len;
    pos += len;
  }
  // Conformance bitmap (bit set = row conforms, the kArith convention).
  const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
  if (pos + bmp_bytes > have) {
    return false;
  }
  const auto* bmp = reinterpret_cast<const uint8_t*>(r + pos);
  pos += bmp_bytes;
  // Digit planes: nseg self-contained inner regions of nconf values.
  const uint32_t nconf = n - nexc;
  std::array<const char*, kTemplateMaxSegments> seg{};
  std::array<Scheme, kTemplateMaxSegments> seg_scheme{};
  std::array<uint32_t, kTemplateMaxSegments> seg_param{};
  std::array<const char*, kTemplateMaxSegments> seg_end{};
  for (uint32_t s = 0; s < nseg; ++s) {
    if (pos + 9 > have) {
      return false;
    }
    const auto ischeme = static_cast<uint8_t>(r[pos]);
    if (!InnerSchemeOk(ischeme)) {
      return false;
    }
    const uint32_t iparam = ReadBE32(r + pos + 1);
    const uint32_t vlen = ReadBE32(r + pos + 5);
    pos += 9;
    if (pos + vlen > have) {
      return false;
    }
    seg[s] = r + pos;
    seg_scheme[s] = static_cast<Scheme>(ischeme);
    seg_param[s] = iparam;
    seg_end[s] = r + pos + vlen;
    pos += vlen;
  }
  // Exception chain: nexc plain length-prefixed strings tiling the tail.
  const char* exc = r + pos;
  for (uint32_t e = 0; e < nexc; ++e) {
    if (static_cast<size_t>(region_end - exc) < 4) {
      return false;
    }
    const uint32_t len = ReadBE32(exc);
    if (static_cast<size_t>(region_end - exc) - 4 < len) {
      return false;
    }
    exc += 4 + len;
  }
  if (exc != region_end) {
    return false;
  }
  out->n = n;
  out->nseg = nseg;
  out->nexc = nexc;
  out->nconf = nconf;
  for (uint32_t s = 0; s < nseg; ++s) {
    out->widths[s] = widths[s];
    out->seg[s] = seg[s];
    out->seg_scheme[s] = seg_scheme[s];
    out->seg_param[s] = seg_param[s];
    out->seg_end[s] = seg_end[s];
  }
  for (uint32_t k = 0; k <= nseg; ++k) {
    out->lit[k] = lit[k];
    out->lit_len[k] = lit_len[k];
  }
  out->bmp = bmp;
  out->exc =
      r + pos;  // exception chain start (now proven to end at region_end)
  return true;
}

// Rank helpers over the conformance bitmap. Bitmap bits are MSB-first per
// byte (the file-wide convention).
bool TemplateConforms(const TemplateRegion& t, uint32_t idx) {
  return ((t.bmp[idx >> 3] >> (7 - (idx & 7))) & 1) != 0;
}

// Number of set (conforming) bits strictly before idx. The rank is paid per
// random access, so the full bytes popcount word-wise (one unaligned 8-byte
// load per 8 bitmap bytes instead of one popcount each).
uint32_t TemplateSetBefore(const TemplateRegion& t, uint32_t idx) {
  uint32_t set = 0;
  size_t w = 0;
  const size_t words = idx >> 6;
  for (; w < words; ++w) {
    uint64_t word = 0;
    std::memcpy(&word, t.bmp + (w * 8), sizeof(word));
    set += static_cast<uint32_t>(__builtin_popcountll(word));
  }
  for (size_t b = w * 8; b < idx / 8; ++b) {
    set += static_cast<uint32_t>(__builtin_popcount(t.bmp[b]));
  }
  for (uint32_t b = 0; b < (idx & 7); ++b) {
    set += static_cast<uint32_t>((t.bmp[idx / 8] >> (7 - b)) & 1);
  }
  return set;
}

// Tight per-value walk of an inner digit plane. The encoder only ever emits
// the EncodeInnerI64 whitelist members {kPlain, kConst, kRle, kForBp, kDict,
// kBitWeave} and all of them have a direct sequential read, so the EQ fold
// never pays DecodeInnerI64All's per-row Int64RegionAt dispatch. Returns
// false for any other scheme (hostile or future inners; the caller falls
// back to the generic bulk decoder, which fails closed identically).
bool TemplateSegmentBulk(const char* r, Scheme s, uint32_t param, uint32_t cnt,
                         const char* end, int64_t* out) {
  switch (s) {
    case Scheme::kPlain: {
      if (end != nullptr && r + (static_cast<size_t>(cnt) * 8) > end) {
        return false;
      }
      for (uint32_t i = 0; i < cnt; ++i) {
        out[i] =
            static_cast<int64_t>(ReadBE64(r + (static_cast<size_t>(i) * 8)));
      }
      return true;
    }
    case Scheme::kConst: {
      if (end != nullptr && r + 8 > end) {
        return false;
      }
      const auto v = static_cast<int64_t>(ReadBE64(r));
      for (uint32_t i = 0; i < cnt; ++i) {
        out[i] = v;
      }
      return true;
    }
    case Scheme::kRle: {
      if (end != nullptr && r + 4 + (static_cast<size_t>(param) * 12) > end) {
        return false;
      }
      uint32_t row = 0;
      const char* p = r + 4;
      for (uint32_t i = 0; i < param && row < cnt; ++i) {
        const auto v = static_cast<int64_t>(ReadBE64(p));
        const uint64_t len = ReadBE32(p + 8);
        const uint32_t e =
            (row + len > cnt) ? cnt : static_cast<uint32_t>(row + len);
        std::fill(out + row, out + e, v);
        row = e;
        p += 12;
      }
      std::fill(out + row, out + cnt, 0);
      return true;
    }
    case Scheme::kForBp: {
      if (param < 1 || param > 8) {
        return false;
      }
      if (end != nullptr && r + 8 + (static_cast<size_t>(cnt) * param) > end) {
        return false;
      }
      const auto base = static_cast<int64_t>(ReadBE64(r));
      for (uint32_t i = 0; i < cnt; ++i) {
        uint64_t off = 0;
        std::memcpy(&off, r + 8 + (static_cast<size_t>(i) * param), param);
        out[i] = base + static_cast<int64_t>(off);
      }
      return true;
    }
    case Scheme::kBitWeave: {
      if (end != nullptr && r + 9 > end) {
        return false;
      }
      const auto base = static_cast<int64_t>(ReadBE64(r));
      const uint32_t w = static_cast<uint8_t>(r[8]);
      if (w == 0 || w > 64) {
        return false;
      }
      if (end != nullptr &&
          r + 9 + ((static_cast<size_t>(cnt) * w + 7) / 8) > end) {
        return false;
      }
      const char* packed = r + 9;
      uint64_t bit = 0;
      for (uint32_t i = 0; i < cnt; ++i) {
        out[i] = base + static_cast<int64_t>(
                            UnpackBitAt(packed, bit, static_cast<int>(w)));
        bit += w;
      }
      return true;
    }
    case Scheme::kDict: {
      if (end != nullptr && r + 2 > end) {
        return false;
      }
      const uint16_t d = ReadBE16(r);
      const int cw = BitsNeeded(d > 0 ? d - 1 : 0);
      const size_t dict_base =
          2 + ((static_cast<size_t>(cnt) * static_cast<size_t>(cw) + 7) / 8);
      if (end != nullptr &&
          r + dict_base + (static_cast<size_t>(d) * 8) > end) {
        return false;
      }
      const char* dict = r + dict_base;
      uint64_t bit = 0;
      for (uint32_t i = 0; i < cnt; ++i) {
        const uint64_t code = UnpackBitAt(r + 2, bit, static_cast<int>(cw));
        bit += static_cast<uint64_t>(cw);
        out[i] = code < d ? static_cast<int64_t>(ReadBE64(
                                dict + (static_cast<size_t>(code) * 8)))
                          : 0;  // clamp bucket decodes as raw 0 (kDict rule)
      }
      return true;
    }
    default:
      return false;
  }
}

// Rebuild row idx's string from the template. Returns false on a structural
// surprise; callers fail closed to "".
bool TemplateValueAt(const TemplateRegion& t, uint32_t idx, std::string* out) {
  if (idx >= t.n) {
    return false;
  }
  const uint32_t set_before = TemplateSetBefore(t, idx);
  if (!TemplateConforms(t, idx)) {
    // Rank among the exception strings = clear bits before idx.
    const uint32_t rank = idx - set_before;
    if (rank >= t.nexc) {
      return false;
    }
    const char* q = t.exc;
    for (uint32_t e = 0; e < rank; ++e) {
      q += 4 + ReadBE32(q);
    }
    const uint32_t len = ReadBE32(q);
    out->assign(q + 4, len);
    return true;
  }
  const uint32_t rank = set_before;  // conforming rows ahead of idx
  if (rank >= t.nconf) {
    return false;
  }
  out->clear();
  for (uint32_t s = 0; s < t.nseg; ++s) {
    out->append(t.lit[s], t.lit_len[s]);
    const int64_t v = Int64RegionAt(t.seg[s], t.seg_scheme[s], t.seg_param[s],
                                    t.nconf, rank, t.seg_end[s]);
    // Zero-pad the segment back to its stored digit width (the width is
    // what preserves leading zeros bit-honestly).
    auto u = static_cast<uint64_t>(v);
    std::array<char, kTemplateMaxWidth> digits{};
    for (uint32_t d = t.widths[s]; d > 0; --d) {
      digits[d - 1] = static_cast<char>('0' + (u % 10));
      u /= 10;
    }
    out->append(digits.data(), t.widths[s]);
  }
  out->append(t.lit[t.nseg], t.lit_len[t.nseg]);
  return true;
}

// ---------------------------------------------------------------------------
// kDStream (scheme 65): mined symbol dictionary with greedy longest-match
// codes stored as length-delimited records. The wire contract lives in the
// Scheme enum block; these helpers are shared by the value decoder, the
// EQ/NE fold, and ValidateWalk.
// ---------------------------------------------------------------------------

// Bounds-checked structural parse (the read-path contract: every extent must
// be provable inside the region before a pointer is formed; semantic
// consistency -- popcount, restart tiling -- is ValidateWalk's job and the
// per-row walks re-check as they go). `param` must echo nexc (the kArith
// convention). Returns false on any disagreement; callers fail closed.
bool ParseDStreamRegion(const char* r, uint32_t n, uint32_t param,
                        const char* region_end, DStreamRegion* out) {
  if (r == nullptr || region_end == nullptr || region_end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const auto have = static_cast<size_t>(region_end - r);
  if (have < 7) {
    return false;  // nsym + ndir + stream_len minimums
  }
  const uint32_t nsym = static_cast<uint8_t>(r[0]);
  if (nsym < 1 || nsym > kDStreamMaxSymbols) {
    return false;
  }
  size_t pos = 1;
  for (uint32_t c = 0; c < nsym; ++c) {
    if (pos + 1 > have) {
      return false;
    }
    const uint32_t len = static_cast<uint8_t>(r[pos]);
    if (len < 1 || len > kDStreamMaxSymbolLen) {
      return false;
    }
    if (pos + 1 + len > have) {
      return false;
    }
    out->sym[c] = r + pos + 1;
    out->sym_len[c] = static_cast<uint8_t>(len);
    pos += 1 + len;
  }
  if (pos + 6 > have) {
    return false;
  }
  const uint32_t ndir = ReadBE16(r + pos);
  pos += 2;
  const uint32_t stream_len = ReadBE32(r + pos);
  pos += 4;
  const uint32_t nexc = param;
  if (nexc > n) {
    return false;
  }
  const uint32_t nconf = n - nexc;
  // The encoder always emits one restart per kDStreamRestart conforming
  // rows (possibly one for zero). A region that disagrees with the derived
  // count has no owner among our encoders.
  if (ndir != (nconf + kDStreamRestart - 1) / kDStreamRestart) {
    return false;
  }
  if (pos + (static_cast<size_t>(ndir) * 4) > have) {
    return false;
  }
  out->dir = r + pos;
  pos += static_cast<size_t>(ndir) * 4;
  if (pos + stream_len > have) {
    return false;
  }
  out->stream = r + pos;
  pos += stream_len;
  const size_t bmp_bytes = (static_cast<size_t>(n) + 7) / 8;
  if (pos + bmp_bytes > have) {
    return false;
  }
  out->bmp = reinterpret_cast<const uint8_t*>(r + pos);
  pos += bmp_bytes;
  // Exception chain: nexc plain length-prefixed strings tiling the tail.
  const char* exc = r + pos;
  for (uint32_t e = 0; e < nexc; ++e) {
    if (static_cast<size_t>(region_end - exc) < 4) {
      return false;
    }
    const uint32_t len = ReadBE32(exc);
    if (static_cast<size_t>(region_end - exc) - 4 < len) {
      return false;
    }
    exc += 4 + len;
  }
  if (exc != region_end) {
    return false;
  }
  out->n = n;
  out->nexc = nexc;
  out->nconf = nconf;
  out->nsym = nsym;
  out->ndir = ndir;
  out->stream_len = stream_len;
  out->exc = r + pos;
  return true;
}

// Bitmap helpers over the conformance bitmap, MSB-first per byte (the
// file-wide convention); identical arithmetic to the Template pair.
bool DStreamConforms(const DStreamRegion& t, uint32_t idx) {
  return ((t.bmp[idx >> 3] >> (7 - (idx & 7))) & 1) != 0;
}

uint32_t DStreamSetBefore(const DStreamRegion& t, uint32_t idx) {
  uint32_t set = 0;
  size_t w = 0;
  const size_t words = idx >> 6;
  for (; w < words; ++w) {
    uint64_t word = 0;
    std::memcpy(&word, t.bmp + (w * 8), sizeof(word));
    set += static_cast<uint32_t>(__builtin_popcountll(word));
  }
  for (size_t b = w * 8; b < idx / 8; ++b) {
    set += static_cast<uint32_t>(__builtin_popcount(t.bmp[b]));
  }
  for (uint32_t b = 0; b < (idx & 7); ++b) {
    set += static_cast<uint32_t>((t.bmp[idx / 8] >> (7 - b)) & 1);
  }
  return set;
}

// Locate the rank-th conforming record (0-based over conforming rows) and
// hand out its code bytes. Bounds-checked: a hostile restart table or a
// clen that runs past the stream fails closed.
bool DStreamRecordAt(const DStreamRegion& t, uint32_t rank, const char** codes,
                     uint32_t* clen) {
  if (rank >= t.nconf) {
    return false;
  }
  const uint32_t block = rank / kDStreamRestart;
  if (block >= t.ndir) {
    return false;
  }
  const uint32_t off = ReadBE32(t.dir + (static_cast<size_t>(block) * 4));
  if (off > t.stream_len) {
    return false;
  }
  const char* p = t.stream + off;
  const uint32_t in_block = rank % kDStreamRestart;
  for (uint32_t k = 0; k <= in_block; ++k) {
    if (p + 1 > t.stream + t.stream_len) {
      return false;
    }
    const uint32_t len = static_cast<uint8_t>(*p++);
    if (std::cmp_less(t.stream + t.stream_len - p, len)) {
      return false;
    }
    if (k == in_block) {
      *codes = p;
      *clen = len;
      return true;
    }
    p += len;
  }
  return false;
}

// Rebuild row idx's string. Returns false on a structural surprise; callers
// fail closed to "".
bool DStreamValueAt(const DStreamRegion& t, uint32_t idx, std::string* out) {
  if (idx >= t.n) {
    return false;
  }
  const uint32_t set_before = DStreamSetBefore(t, idx);
  if (!DStreamConforms(t, idx)) {
    // Rank among the exception strings = clear bits before idx.
    const uint32_t rank = idx - set_before;
    if (rank >= t.nexc) {
      return false;
    }
    const char* q = t.exc;
    for (uint32_t e = 0; e < rank; ++e) {
      q += 4 + ReadBE32(q);
    }
    const uint32_t len = ReadBE32(q);
    out->assign(q + 4, len);
    return true;
  }
  const uint32_t rank = set_before;  // conforming rows ahead of idx
  const char* codes = nullptr;
  uint32_t clen = 0;
  if (!DStreamRecordAt(t, rank, &codes, &clen)) {
    return false;
  }
  out->clear();
  out->reserve(clen);
  for (uint32_t i = 0; i < clen; ++i) {
    const uint32_t c = static_cast<uint8_t>(codes[i]);
    if (c == 0) {
      // Escape: the raw byte follows. A trailing escape with no byte is a
      // structural surprise -- the record walk proved the byte exists
      // inside the stream, but stay defensive.
      if (i + 1 >= clen) {
        return false;
      }
      out->push_back(codes[++i]);
    } else if (c <= t.nsym) {
      out->append(t.sym[c - 1], t.sym_len[c - 1]);
    } else {
      return false;  // code past the table: hostile or torn region
    }
  }
  return true;
}

// Encode `s` through the region's stored symbol table with the encoder's
// greedy longest-match rule (the match is unique -- symbols are distinct
// byte strings -- so encoder-side and read-side implementations always
// agree). Returns false only on an internal inconsistency; equal strings
// always produce equal code sequences, which is what makes the memcmp fold
// below exact.
bool DStreamEncodeNeedle(const DStreamRegion& t, std::string_view s,
                         std::string* out) {
  out->clear();
  size_t pos = 0;
  while (pos < s.size()) {
    const size_t max_l = std::min<size_t>(kDStreamMaxSymbolLen, s.size() - pos);
    size_t hit = 0;  // longest match length, 0 = none
    uint32_t hit_code = 0;
    // Linear scan over the (<= 255) symbol records; the needle is encoded
    // once per query, so the map-free walk wins on simplicity.
    for (uint32_t c = 0; c < t.nsym; ++c) {
      const size_t l = t.sym_len[c];
      if (l <= hit || l > max_l) {
        continue;  // keep only STRICTLY longer hits
      }
      if (s.compare(pos, l, t.sym[c], l) == 0) {
        hit = l;
        hit_code = c + 1;
      }
    }
    if (hit == 0) {
      out->push_back(static_cast<char>(0));  // escape
      out->push_back(s[pos]);
      pos += 1;
    } else {
      out->push_back(static_cast<char>(static_cast<uint8_t>(hit_code)));
      pos += hit;
    }
  }
  return true;
}

// Varchar value at local index idx (PLAIN/CONST/DICT/FSST/FRONT-CODE).
Value VarcharRegionAt(const char* r, Scheme s, uint32_t param, uint32_t n,
                      uint32_t idx, const char* region_end,
                      const FsstDecodeView* fsst_view) {
  const char* p = r;
  // Defense in depth: length chains are only trustworthy for validated
  // chunks. Never step past the region boundary -- return empty instead of
  // reading out of bounds on hostile bytes.
  switch (s) {
    case Scheme::kConst:
      break;  // single entry
    case Scheme::kPlain:
      for (uint32_t i = 0; i < idx && p + 4 <= region_end; ++i) {
        const uint32_t len = ReadBE32(p);
        if (p + 4 + len > region_end) {
          p = region_end;
          break;
        }
        p += 4 + len;
      }
      break;
    case Scheme::kDict:
    case Scheme::kOpDict: {
      // kOpDict shares kDict's byte layout (sorted dictionary, rank codes);
      // positionally walking the entries to `code` is the identical walk.
      // Mirror Int64RegionAt's kDict gates: the header and the packed-code
      // extent must be inside the region BEFORE UnpackBitAt. A hostile
      // ncodes (up to 2^16-1) derived cw up to 16 and made idx*cw reach
      // ~131KB past region_end for a trailing cell.
      if (r + 2 > region_end) {
        return Value(std::string());
      }
      const uint16_t ncodes = ReadBE16(r);
      if (ncodes == 0) {
        return Value(std::string());
      }
      const int cw = BitsNeeded(ncodes - 1);
      // Layout: [count 2B][packed codes ceil(rows*cw/8) B][dictionary].
      const size_t packed_bits =
          static_cast<size_t>(n) * static_cast<uint64_t>(cw);
      const size_t dict_base = (packed_bits + 7U) / 8U;
      if (r + 2 + dict_base > region_end) {
        return Value(std::string());
      }
      const uint64_t code = UnpackBitAt(
          r + 2, static_cast<uint64_t>(idx) * static_cast<uint64_t>(cw), cw);
      // A cw-wide code can encode values past ncodes (cw rounds up): clamp.
      if (code >= ncodes) {
        return Value(std::string());
      }
      const char* d = r + 2 + dict_base;
      for (uint16_t i = 0; i < code && d + 4 <= region_end; ++i) {
        const uint32_t l = ReadBE32(d);
        if (d + 4 + l > region_end) {
          return Value(std::string());
        }
        d += sizeof(uint32_t) + l;
      }
      p = d;
      break;
    }
    case Scheme::kFsst: {
      if (r == nullptr || region_end == nullptr || region_end < r || idx >= n) {
        return Value(std::string());
      }
      const auto region_len = static_cast<size_t>(region_end - r);
      const FsstDecodeMeta* meta =
          fsst_view != nullptr ? fsst_view->meta : nullptr;
      if (meta == nullptr || !meta->valid || meta->row_count != n) {
        return VarcharRegionAtFsstFallback(r, n, idx, region_end);
      }
      const size_t table_bytes = static_cast<size_t>(n) * 4;
      if (static_cast<size_t>(meta->offset_table) > region_len ||
          table_bytes > region_len - meta->offset_table ||
          static_cast<size_t>(meta->stream) > region_len ||
          static_cast<size_t>(meta->stream_len) > region_len - meta->stream ||
          (meta->symbol_count != 0 && (fsst_view->symbol_offsets == nullptr ||
                                       fsst_view->symbol_lengths == nullptr))) {
        return VarcharRegionAtFsstFallback(r, n, idx, region_end);
      }
      const uint16_t nsym = meta->symbol_count;
      const uint32_t tsl = meta->stream_len;
      const char* otable = r + meta->offset_table;
      const char* stream = r + meta->stream;
      const uint32_t code_off =
          ReadBE32(otable + (static_cast<size_t>(idx) * 4));
      const uint32_t next_off =
          (idx + 1 < n) ? ReadBE32(otable + (static_cast<size_t>(idx + 1) * 4))
                        : tsl;
      if (code_off > next_off || next_off > tsl) {
        return Value(std::string());
      }
      std::string out;
      const auto encoded_len = static_cast<size_t>(next_off - code_off);
      if (encoded_len > out.capacity() && encoded_len <= 65536) {
        out.reserve(encoded_len);
      }
      const auto* cp = reinterpret_cast<const uint8_t*>(stream + code_off);
      const auto* end = reinterpret_cast<const uint8_t*>(stream + next_off);
      while (cp < end) {
        const uint8_t code = *cp++;
        if (code == 0) {
          if (cp >= end) {
            break;
          }
          out.push_back(static_cast<char>(*cp++));
        } else if (code <= nsym) {
          if (fsst_view->symbol_offsets == nullptr ||
              fsst_view->symbol_lengths == nullptr ||
              std::cmp_greater_equal(code - 1, nsym)) {
            return Value(std::string());
          }
          const uint32_t soff = fsst_view->symbol_offsets[code - 1];
          const uint8_t slen = fsst_view->symbol_lengths[code - 1];
          if (static_cast<size_t>(soff) > region_len ||
              slen > region_len - soff) {
            return Value(std::string());
          }
          out.append(r + soff, slen);
        }
      }
      return Value(std::move(out));
    }
    case Scheme::kFrontCode: {
      // Restart-chain decode: jump to the entry's block, then replay the
      // plen chain forward. Every extent is bounded by the region; a
      // hostile plen (past the chained prefix) or slen (past the stream)
      // fails closed to "" exactly like the other varchar clamps.
      const uint32_t re = param & 0x7FFFU;
      if (re == 0 || r + 6 > region_end) {
        return Value(std::string());
      }
      const uint32_t nrestart = ReadBE16(r);
      const uint32_t stream_len = ReadBE32(r + 2);
      const char* rtable = r + 6;
      if (rtable + (static_cast<size_t>(nrestart) * 4) > region_end) {
        return Value(std::string());
      }
      const char* stream = rtable + (static_cast<size_t>(nrestart) * 4);
      if (stream + stream_len > region_end) {
        return Value(std::string());
      }
      const uint32_t block = idx / re;
      if (block >= nrestart) {
        return Value(std::string());
      }
      const uint32_t ro = ReadBE32(rtable + (static_cast<size_t>(block) * 4));
      if (ro > stream_len) {
        return Value(std::string());
      }
      const char* send = stream + stream_len;
      const char* q = stream + ro;
      std::string cur;
      for (uint32_t i = block * re; i <= idx; ++i) {
        // One 4B header load covers both halves ([plen u16][slen u16] BE);
        // its extent check subsumes the bare q+4 gate.
        if (q + 4 > send) {
          return Value(std::string());
        }
        const uint32_t hdr = ReadBE32(q);
        const uint32_t plen = hdr >> 16;
        const uint32_t slen = hdr & 0xFFFFU;
        if (plen > cur.size() || slen > static_cast<uint64_t>(send - q) - 4U) {
          return Value(std::string());
        }
        cur.resize(plen);
        cur.append(q + 4, slen);
        q += 4 + slen;
      }
      return Value(std::move(cur));
    }
    case Scheme::kDictX: {
      // [d u16][code_scheme u8][code_param u32][clen u32][codes region: n]
      // [pool_scheme u8][pool]. Codes come from the inner region; pool 0 is
      // the flat entry list (same walk as kDict), pool 15 is an FSST region
      // whose "rows" are the dictionary entries.
      if (r + 11 > region_end) {
        return Value(std::string());
      }
      const uint16_t dx_d = ReadBE16(r);
      if (dx_d == 0) {
        return Value(std::string());
      }
      const auto dx_cs = static_cast<uint8_t>(r[2]);
      const uint32_t dx_cparam = ReadBE32(r + 3);
      const uint64_t dx_clen = ReadBE32(r + 7);
      const char* dx_creg = r + 11;
      if (std::cmp_greater(dx_clen, region_end - dx_creg)) {
        return Value(std::string());
      }
      const char* dx_pool_hdr = dx_creg + dx_clen;
      if (dx_pool_hdr + 1 > region_end) {
        return Value(std::string());
      }
      if (!InnerSchemeOk(dx_cs)) {
        return Value(std::string());
      }
      const auto dx_code = static_cast<uint64_t>(Int64RegionAt(
          dx_creg, static_cast<Scheme>(dx_cs), dx_cparam, n, idx, dx_pool_hdr));
      if (dx_code >= dx_d) {
        return Value(std::string());
      }
      const auto dx_ps = static_cast<uint8_t>(dx_pool_hdr[0]);
      const char* dx_pool = dx_pool_hdr + 1;
      if (dx_ps == static_cast<uint8_t>(Scheme::kFsst)) {
        // Ordinary FSST region over the dictionary entries (kFsst is a
        // leaf, so this recursion is depth-1 bounded).
        if (dx_pool + 2 > region_end) {
          return Value(std::string());
        }
        return VarcharRegionAt(dx_pool, Scheme::kFsst, ReadBE16(dx_pool), dx_d,
                               static_cast<uint32_t>(dx_code), region_end);
      }
      if (dx_ps != 0) {
        return Value(std::string());
      }
      const char* dd = dx_pool;
      for (uint64_t i = 0; i < dx_code && dd + 4 <= region_end; ++i) {
        const uint32_t l = ReadBE32(dd);
        if (dd + 4 + l > region_end) {
          return Value(std::string());
        }
        dd += sizeof(uint32_t) + l;
      }
      p = dd;
      break;
    }
    case Scheme::kTemplate: {
      // One shared parse (also used by the EQ kernel and ValidateWalk),
      // then either the digit-plane reconstruction or the exception chain.
      TemplateRegion t;
      if (!ParseTemplateRegion(r, n, param, region_end, &t)) {
        return Value(std::string());
      }
      std::string out;
      if (!TemplateValueAt(t, idx, &out)) {
        return Value(std::string());
      }
      return Value(std::move(out));
    }
    case Scheme::kDStream: {
      // One shared parse (also used by the EQ/NE fold and ValidateWalk),
      // then either the code-record expansion or the exception chain.
      DStreamRegion t;
      if (!ParseDStreamRegion(r, n, param, region_end, &t)) {
        return Value(std::string());
      }
      std::string out;
      if (!DStreamValueAt(t, idx, &out)) {
        return Value(std::string());
      }
      return Value(std::move(out));
    }
    default:
      return {};
  }
  if (p + 4 > region_end) {
    return Value(std::string());
  }
  const uint32_t len = ReadBE32(p);
  if (p + 4 + len > region_end) {
    return Value(std::string());
  }
  // Empty varchars must decode as empty kVarChar values, not as nulls --
  // all-empty columns (CONST) and embedded empties (PLAIN/DICT) hit this.
  if (len == 0) {
    return Value(std::string());
  }
  return Value(std::string(p + sizeof(uint32_t), len));
}

// First local index whose logical value >= key; false when all < key.
bool RegionLowerBound(const char* r, Scheme s, uint32_t param, uint32_t n,
                      int64_t key, uint32_t* pos, const char* region_end,
                      const uint32_t* ef_dir) {
  switch (s) {
    case Scheme::kPlain:
    case Scheme::kConst:
    case Scheme::kDict:
    case Scheme::kOpDict:
    case Scheme::kForBp:
    case Scheme::kBitmap:
    case Scheme::kBitWeave:
    case Scheme::kPfor:
    case Scheme::kFastPfor:
    case Scheme::kDeltaFastPfor:
    case Scheme::kDdRice:
    case Scheme::kDeltaVarint:
    case Scheme::kEliasFano:
    case Scheme::kWavelet:
    case Scheme::kWaveletR:
    case Scheme::kByteSlice:
    case Scheme::kDeltaFor:
    case Scheme::kDeltaBitweave:
    // kDeltaSimd (54): reconstructed from a checkpoint plus a replayed delta
    // prefix, so it is NOT order-preserving in its stored form and must NOT
    // get a bisect arm. It joins the generic binary-search-over-the-decoder
    // group with the rest of the first-difference family.
    case Scheme::kDeltaSimd:
    case Scheme::kAlp:
    case Scheme::kFreq:
    case Scheme::kTopk:
    case Scheme::kDictX:
    case Scheme::kArith:
    case Scheme::kSparse:
    case Scheme::kAnsFor:
    case Scheme::kQpp:
    case Scheme::kAlpRdTans:
    case Scheme::kPatas:
    case Scheme::kWah:
    case Scheme::kPla:
    case Scheme::kInterp:
    // kMod (58) is a wrapped model: values live in Z/mZ and wrap at the
    // modulus, so stored order is not value order even for a clean encoder.
    // The generic bisect stays exact whenever the caller's column really is
    // sorted (this API's contract is key columns, which a wrapping sequence
    // cannot be).
    case Scheme::kMod:
    // kSfor (57) is raw-value order-preserving ONLY under the encoder's
    // non-negative gate; a hand-built region could carry sign-bit values
    // whose bitcast order inverts, and this API's key domain is int64
    // (double keys resolve through the hash index, not here). Join the
    // generic bisect-over-the-decoder group so a caller on an int64-
    // interpreted cell still gets a correct answer.
    case Scheme::kSfor:
    // kExpFor (60) groups in the IEEE total-order domain, which agrees with
    // value order but NOT with the signed int64 key order this API bisects
    // (negatives live below positives there, not above) -- the same reason
    // kSfor stays out of the order-preserving group.
    case Scheme::kExpFor:
    // kTileDict (59) sorts WITHIN a tile only: tiles themselves follow the
    // column's arbitrary row order, so a region-wide value order does not
    // exist and a bisect over the decoder is the only correct lower bound
    // (the same reason kSimple8b sits outside the order-preserving group).
    case Scheme::kTileDict:
    // kSortDict (61) deliberately SCRAMBLES the row order -- the rank plane
    // maps each row into a value-sorted view -- so stored row order is not
    // value order by construction either. The same generic bisect applies
    // whenever the caller's column really is row-sorted (this API's
    // key-column contract; a genuinely sorted column loses the byte race to
    // the delta family anyway, so the encoder rarely emits it there).
    case Scheme::kSortDict:
    // kLane (62) interleaves K independent lane orderings row-major, so the
    // stored row order follows no single value order (the same reason
    // kSortDict stays out of the order-preserving group).
    case Scheme::kLane:
    // kPostings (63) stores row ids grouped by VALUE -- the stored row
    // order is the column's arbitrary arrival order, so a region-wide
    // value order does not exist (the same reason kSortDict/kTileDict
    // stay out of the order-preserving group).
    case Scheme::kPostings:
    case Scheme::kHuffDict: {
      // Random-accessible: plain binary search over the decoder. The hot
      // schemes go through the compact compare-chain dispatch (Int64AtFast):
      // the generic 40-way switch's cold arms cost an I-cache stall per
      // probe on this ~log2(n)-iteration loop.
      uint32_t lo = 0, hi = n;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) / 2);
        if (Int64AtFast(r, s, param, n, mid, region_end, ef_dir) < key) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      *pos = lo;
      return lo < n;
    }
    case Scheme::kRle8:
      // Same frame concern as Int64RegionAt's kRle8 arm: the parsed region
      // lives in Rle8RegionLowerBound's own frame.
      return Rle8RegionLowerBound(r, n, param, region_end, key, pos);
    case Scheme::kRleDelta: {
      RleDeltaRegion parsed;
      if (n == 0 || !ParseRleDeltaRegion(r, n, param, region_end, &parsed)) {
        *pos = n;
        return false;
      }
      uint32_t lo = 0;
      uint32_t hi = n;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) / 2);
        if (RleDeltaValueAt(parsed, mid) < key) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      *pos = lo;
      return lo < n;
    }
    case Scheme::kBp128: {
      Bp128Region parsed;
      if (n == 0 || !ParseBp128Region(r, n, param, region_end, &parsed)) {
        *pos = n;
        return false;
      }
      uint32_t lo = 0;
      uint32_t hi = n;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) / 2);
        if (Bp128ValueAt(parsed, mid) < key) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      *pos = lo;
      return lo < n;
    }
    case Scheme::kPBitS: {
      if (n == 0) {
        *pos = 0;
        return false;
      }
      for (uint32_t i = 0; i < n; ++i) {
        if (Int64RegionAt(r, s, param, n, i, region_end) >= key) {
          *pos = i;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kDeltaPfor: {
      DeltaPForRegion p;
      if (n == 0 || !ParseDeltaPForRegion(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      uint32_t low = 0;
      uint32_t high = p.blocks;
      while (low < high) {
        const uint32_t mid = low + ((high - low) / 2);
        const auto last = static_cast<int64_t>(
            ReadBE64(p.brackets +
                     (static_cast<size_t>(mid) * kDeltaPForBracketBytes) + 8));
        if (last < key) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      if (low == p.blocks) {
        *pos = n;
        return false;
      }
      const uint32_t start = low * kDeltaPForBlockRows;
      const uint32_t rows =
          std::min<uint32_t>(kDeltaPForBlockRows, p.n - start);
      std::array<int64_t, kDeltaPForBlockRows> decoded{};
      DecodeDeltaPForBlock(p, low, decoded.data());
      uint32_t lo = 0;
      uint32_t hi = rows;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) / 2);
        if (decoded[mid] < key) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      *pos = start + lo;
      return lo < rows;
    }
    case Scheme::kRleBp: {
      RleBpRegion p;
      if (n == 0 || !ParseRleBpRegion(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      uint64_t row = 0;
      for (uint32_t run = 0; run < p.runs; ++run) {
        const int64_t value = RleBpValueAt(p, run);
        if (value >= key) {
          *pos = static_cast<uint32_t>(row);
          return row < n;
        }
        row += RleBpLengthAt(p, run);
      }
      *pos = n;
      return false;
    }
    case Scheme::kRleIdx: {
      // The one run-length arm that binary searches: the run values are sorted
      // (monotone-input gate), so the first run at-or-after `key` is O(log
      // runs) and its row index is a restart lookup. The kRle / kRleBp / kRle8
      // / kRleDict / kRleX arms above all walk every run instead.
      RleIdxRegion p;
      if (n == 0 || !ParseRleIdxRegion(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      const uint32_t run = RleIdxLowerBound(p, key);
      const uint32_t row = RleIdxRowBefore(p, run);
      *pos = row > n ? n : row;
      return *pos < n;
    }
    case Scheme::kSimdFor: {
      // Order-preserving (plain frame-of-reference over the raw values), so a
      // plain bisect is correct. It gets its OWN arm rather than joining the
      // generic list above so the parse happens once instead of per probe.
      SimdForRegion p{};
      if (n == 0 || !ParseSimdForRegion(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      const uint32_t lo = SimdForLowerBound(p, key);
      *pos = lo > n ? n : lo;
      return *pos < n;
    }
    case Scheme::kD2Pfor: {
      // Same reasoning as the kStreamVByte arm below: bisect, but parse the
      // seek table once instead of re-entering Int64RegionAt per probe.
      D2PforRegion p;
      if (n == 0 || !ParseD2PforRegion(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      const uint32_t lo = D2PforLowerBound(p, key);
      *pos = lo > n ? n : lo;
      return *pos < n;
    }
    case Scheme::kStreamVByte: {
      // Not order-aware: the values are stored in row order, so the answer is
      // a plain lower-bound bisection. It still gets its OWN arm rather than
      // joining the generic list above, because that list re-enters
      // Int64RegionAt on every probe and Int64RegionAt re-runs
      // ParseStreamVByteRegion -- a full O(n) width walk. Parsing once here
      // makes the seek O(log n) probes over an O(1) value read instead.
      StreamVByteRegion p;
      if (n == 0 || !ParseStreamVByteRegion(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      const uint32_t lo = StreamVByteLowerBound(p, key);
      *pos = lo;
      return lo < n;
    }
    case Scheme::kRle: {
      // u64 extent math: a hostile len wrapped the uint32 accumulator and
      // produced an out-of-range *pos (or a regressed walk) — mirror the
      // fix in Int64RegionAt's kRle arm.
      // An empty row group (n == 0) must fail closed: the run walk below
      // would otherwise report a hit at pos 0 for the first run value, and
      // PointLookup would then compare key 0 against Int64RegionAt's
      // out-of-range 0 and falsely match. Every other scheme's generic arm
      // already returns lo < n (false when n == 0).
      if (n == 0) {
        *pos = 0;
        return false;
      }
      uint64_t acc = 0;
      const char* p = r + 4;
      for (uint32_t i = 0; i < param; ++i) {
        // The run walk reads 12B per run; a hostile param used to march
        // gigabytes past the region (the region_end the caller supplied
        // was ignored on this path). Mirror Int64RegionAt's gate.
        if (region_end != nullptr && p + 12 > region_end) {
          break;
        }
        const auto v = static_cast<int64_t>(ReadBE64(p));
        const uint64_t len = ReadBE32(p + 8);
        if (v >= key) {
          // A prior hostile run length can push acc past n: "found at n" is
          // past-the-end (Int64RegionAt would return its out-of-range 0 and
          // PointLookup would phantom-match key 0 / hand back the next
          // group's first row). Fail closed; well-formed regions always hit
          // with acc < n.
          if (acc >= n) {
            *pos = n;
            return false;
          }
          *pos = static_cast<uint32_t>(acc);
          return true;
        }
        acc += len;
        p += 12;
      }
      *pos = n;
      return false;
    }
    case Scheme::kRleDict:
    case Scheme::kOpRle: {
      // Run walk with dictionary dereference: the run value is dict[code],
      // everything else mirrors the kRle arm above (hostile-len u64 math,
      // empty-cell fail-closed, acc >= n guard).
      if (n == 0) {
        *pos = 0;
        return false;
      }
      if (region_end != nullptr && r + 2 > region_end) {
        *pos = n;
        return false;
      }
      const uint32_t d = ReadBE16(r);
      if (d == 0) {
        *pos = n;
        return false;
      }
      const int cb = (BitsNeeded(d - 1) + 7) / 8;
      const char* runs_hdr = r + 2 + (static_cast<size_t>(d) * 8);
      if (region_end != nullptr && runs_hdr + 4 > region_end) {
        *pos = n;
        return false;
      }
      const uint32_t runs = ReadBE32(runs_hdr);
      if (region_end != nullptr &&
          runs_hdr + 4 +
                  (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
              region_end) {
        *pos = n;
        return false;
      }
      const char* dict = r + 2;
      const char* q = runs_hdr + 4;
      uint64_t acc = 0;
      for (uint32_t i = 0; i < runs; ++i) {
        if (region_end != nullptr &&
            q + static_cast<size_t>(cb) + 4 > region_end) {
          break;
        }
        uint64_t code = 0;
        for (int b = 0; b < cb; ++b) {
          code =
              (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
        }
        const int64_t v =
            (code >= d) ? 0
                        : static_cast<int64_t>(
                              ReadBE64(dict + (static_cast<size_t>(code) * 8)));
        const uint64_t len = ReadBE32(q + cb);
        if (v >= key) {
          if (acc >= n) {
            *pos = n;
            return false;
          }
          *pos = static_cast<uint32_t>(acc);
          return true;
        }
        acc += len;
        q += static_cast<size_t>(cb) + 4;
      }
      *pos = n;
      return false;
    }
    case Scheme::kRleX: {
      // Same run-walk shape as kRle/kRleDict, but run values and lengths
      // come from the inner regions (hostile-len u64 math, empty-cell
      // fail-closed, acc >= n guard all mirror the kRle arm).
      if (n == 0) {
        *pos = 0;
        return false;
      }
      if (region_end == nullptr || r + 18 > region_end) {
        *pos = n;
        return false;
      }
      const auto vs = static_cast<uint8_t>(r[0]);
      const auto cs = static_cast<uint8_t>(r[1]);
      const uint32_t runs = ReadBE32(r + 2);
      const uint32_t vparam = ReadBE32(r + 6);
      const uint32_t cparam = ReadBE32(r + 10);
      const uint64_t vlen = ReadBE32(r + 14);
      if (std::cmp_greater(vlen, region_end - (r + 18)) || runs > n ||
          runs == 0) {
        *pos = n;
        return false;
      }
      if (!InnerSchemeOk(vs) || !InnerSchemeOk(cs)) {
        *pos = n;
        return false;
      }
      const char* vreg = r + 18;
      const char* creg = vreg + vlen;
      uint64_t acc = 0;
      for (uint32_t i = 0; i < runs; ++i) {
        const int64_t v =
            Int64RegionAt(vreg, static_cast<Scheme>(vs), vparam, runs, i, creg);
        if (v >= key) {
          if (acc >= n) {
            *pos = n;
            return false;
          }
          *pos = static_cast<uint32_t>(acc);
          return true;
        }
        const auto len = static_cast<uint64_t>(Int64RegionAt(
            creg, static_cast<Scheme>(cs), cparam, runs, i, region_end));
        acc += len;
      }
      *pos = n;
      return false;
    }
    case Scheme::kDeltaFp: {
      // Empty cell: the header-only extent checks below cannot validate a
      // checkpoint read, and returning "found at 0" sent PointLookup chasing
      // an Int64RegionAt the n==0 gate then rejected -- a phantom key hit.
      // Mirror the kRle arm's explicit guard.
      if (n == 0) {
        *pos = 0;
        return false;
      }
      // Jump to the LAST checkpoint strictly below the key: guarantees every
      // skipped prefix row is < key even when duplicates straddle the
      // checkpoint boundary.
      const uint32_t cks =
          (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery;
      // Header + checkpoint-table extent before any read, and the delta
      // walk bound: this path used to ignore region_end entirely (hostile
      // n/param walked the reads far past the region).
      if (region_end != nullptr &&
          (r + 8 + (static_cast<size_t>(cks) * 8) > region_end ||
           r + 8 + (static_cast<size_t>(cks) * 8) +
                   ((static_cast<size_t>(n - 1) * param + 7) / 8) >
               region_end)) {
        *pos = n;
        return false;
      }
      uint32_t ci = 0;
      while (ci + 1 < cks) {
        const auto next_ck = static_cast<int64_t>(
            ReadBE64(r + 8 + (static_cast<size_t>(ci + 1) * 8)));
        if (next_ck >= key) {
          break;
        }
        ++ci;
      }
      uint64_t v = ReadBE64(r + 8 + (static_cast<size_t>(ci) * 8));
      uint32_t start = ci * kDeltaCheckpointEvery;
      const auto sv0 = static_cast<int64_t>(v);
      if (sv0 >= key) {
        *pos = start;
        return true;
      }
      const char* packed = r + 8 + (static_cast<size_t>(cks) * 8);
      for (uint32_t t = start + 1; t < n; ++t) {
        v += UnpackBitAt(packed, static_cast<uint64_t>(t - 1) * param,
                         static_cast<int>(param));
        const auto sv = static_cast<int64_t>(v);
        if (sv >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kS4: {
      // Empty cell: the seek read below would dereference a region with no
      // rows, and reporting "found at 0" for it is the phantom key hit the
      // kDeltaFp arm documents.
      if (n == 0) {
        *pos = 0;
        return false;
      }
      S4Region p;
      if (!ParseS4Region(r, n, param, region_end, &p)) {
        *pos = n;
        return false;
      }
      // Bisect the seek table for the last block whose first-row value is
      // strictly below the key. On non-decreasing input (the precondition for
      // any lower_bound) the seek values ascend, so every row before that block
      // is < key and the whole prefix is skipped without decoding a value.
      uint32_t lo_b = 1, hi_b = p.nblk;
      if (p.nblk > 1) {
        while (lo_b < hi_b) {
          const uint32_t mid = lo_b + ((hi_b - lo_b) / 2);
          const auto ck_val = static_cast<int64_t>(
              ReadBE64(p.ckpt + (8 * static_cast<size_t>(mid - 1))));
          if (ck_val < key) {
            lo_b = mid + 1;
          } else {
            hi_b = mid;
          }
        }
      }
      const uint32_t start = (lo_b - 1) * kS4Block;
      // Linear scan of one block. Block values are not sorted, so this is a
      // scan rather than a binary search -- correct for a lower_bound on
      // non-decreasing input, the same precondition kDeltaFp's replay has.
      for (uint32_t t = start; t < n; ++t) {
        if (S4ValueAt(p, t) >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kXorFor: {
      // Empty cell: n - 1 below underflows, and the first-value compare
      // would report "found at 0" for a region with no rows (see kDeltaFp).
      if (n == 0) {
        *pos = 0;
        return false;
      }
      // The XOR walk touches ceil((n-1)*param/8) bytes past `packed`; bound
      // it by the region before any read (this path used to ignore
      // region_end).
      if (region_end != nullptr &&
          r + 8 + ((static_cast<size_t>(n - 1) * param + 7) / 8) > region_end) {
        *pos = n;
        return false;
      }
      uint64_t x = ReadBE64(r);
      const char* packed = r + 8;
      const auto sx0 = static_cast<int64_t>(x);
      if (sx0 >= key) {
        *pos = 0;
        return true;
      }
      for (uint32_t t = 1; t < n; ++t) {
        x ^= UnpackBitAt(packed, static_cast<uint64_t>(t - 1) * param,
                         static_cast<int>(param));
        const auto sx = static_cast<int64_t>(x);
        if (sx >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kChimp: {
      // Empty cell: the first-value compare would report "found at 0" for a
      // region with no rows (see kDeltaFp).
      if (n == 0) {
        *pos = 0;
        return false;
      }
      // Sequential Chimp walk (mirrors the kGorilla case below).
      uint64_t prev = ReadBE64(r);
      const auto sp0 = static_cast<int64_t>(prev);
      if (sp0 >= key) {
        *pos = 0;
        return true;
      }
      int pl_z = 0, pt_z = 64;
      BitReader br(r + 8, region_end);
      for (uint32_t t = 1; t < n; ++t) {
        if (br.get1() == 0) {
          // identical
        } else if (br.get1() == 0) {
          const int width = 64 - pl_z - pt_z;
          if (width > 0 && width <= 64) {
            uint64_t m = 0;
            for (int k = 0; k < width; ++k) {
              m = (m << 1) | static_cast<uint64_t>(br.get1());
            }
            prev ^= (m << pt_z);
          }
        } else {
          const int lz = static_cast<int>(br.get(5));
          const int tz = static_cast<int>(br.get(6));
          int mw = 64 - lz - tz;
          mw = std::max(mw, 1);
          mw = std::min(mw, 64);
          prev ^= (mw >= 64 ? br.get(mw) : (br.get(mw) << tz));
          pl_z = lz;
          pt_z = tz;
        }
        const auto sp = static_cast<int64_t>(prev);
        if (sp >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kGorilla: {
      // Empty cell: the first-value compare would report "found at 0" for a
      // region with no rows (see kDeltaFp).
      if (n == 0) {
        *pos = 0;
        return false;
      }
      uint64_t prev = ReadBE64(r);
      const auto sp0 = static_cast<int64_t>(prev);
      if (sp0 >= key) {
        *pos = 0;
        return true;
      }
      BitReader br(r + 8, region_end);
      for (uint32_t t = 1; t < n; ++t) {
        if (br.get1() != 0) {
          const int lz = static_cast<int>(br.get(5));
          const int tz = static_cast<int>(br.get(6));
          // Same corrupt-stream clamp as Int64RegionAt (mw >= 0).
          int mw = 64 - lz - tz;
          mw = std::max(mw, 0);
          mw = std::min(mw, 64);
          const uint64_t m = br.get(mw);
          prev ^= (mw >= 64 ? m : (m << tz));
        }
        const auto sp = static_cast<int64_t>(prev);
        if (sp >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kXorForC: {
      // [ncp u16][ckpts ncp x u64][w u8][xors]. Lower bound is a linear
      // scan either way (unsorted values); checkpointing only changes the
      // per-block base: x resets to ckpts[b] at each block boundary.
      if (n == 0) {
        *pos = 0;
        return false;
      }
      if (region_end != nullptr && r + 3 > region_end) {
        *pos = n;
        return false;
      }
      const uint32_t ncp = ReadBE16(r);
      if (ncp == 0 || (region_end != nullptr &&
                       r + 3 + (static_cast<size_t>(ncp) * 8) > region_end)) {
        *pos = n;
        return false;
      }
      const char* packed = r + 3 + (static_cast<size_t>(ncp) * 8);
      if (region_end != nullptr &&
          packed + ((static_cast<size_t>(n - 1) * param + 7) / 8) >
              region_end) {
        *pos = n;
        return false;
      }
      uint64_t x = ReadBE64(r + 2);
      const auto sx0 = static_cast<int64_t>(x);
      if (sx0 >= key) {
        *pos = 0;
        return true;
      }
      for (uint32_t t = 1; t < n; ++t) {
        if (t % kDeltaCheckpointEvery == 0 && t / kDeltaCheckpointEvery < ncp) {
          x = ReadBE64(r + 2 +
                       (static_cast<size_t>(t / kDeltaCheckpointEvery) * 8));
        } else {
          x ^= UnpackBitAt(packed, static_cast<uint64_t>(t - 1) * param,
                           static_cast<int>(param));
        }
        const auto sx = static_cast<int64_t>(x);
        if (sx >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kGorillaC:
    case Scheme::kChimpC: {
      // [ncp u16][ckpts ncp x u64][bitoffs ncp x u32][first u64][stream].
      // Sequential scan from row 0 (lower bound cannot skip unsorted
      // blocks); block boundaries re-load prev from the checkpoint table.
      if (n == 0) {
        *pos = 0;
        return false;
      }
      if (region_end != nullptr && r + 10 > region_end) {
        *pos = n;
        return false;
      }
      const uint32_t ncp = ReadBE16(r);
      if (ncp == 0 ||
          (region_end != nullptr &&
           r + 2 + (static_cast<size_t>(ncp) * 12) + 8 > region_end)) {
        *pos = n;
        return false;
      }
      const char* ck = r + 2;
      // +8 skips the redundant [first u64] word after the offset table.
      const char* stream = ck + (static_cast<size_t>(ncp) * 12) + 8;
      uint64_t prev = ReadBE64(ck);
      const auto sp0 = static_cast<int64_t>(prev);
      if (sp0 >= key) {
        *pos = 0;
        return true;
      }
      BitReader br(stream, region_end);
      int pl_z = 0, pt_z = 64;  // forced-'11' contract (kChimpC)
      // Every row >= 1 has a stream code (a block's checkpoint row is the
      // LAST code of the previous block's range), so the linear walk
      // consumes them all; the tables serve random access only.
      for (uint32_t t = 1; t < n; ++t) {
        if (s == Scheme::kGorillaC) {
          if (br.get1() != 0) {
            const int lz = static_cast<int>(br.get(5));
            const int tz = static_cast<int>(br.get(6));
            int mw = 64 - lz - tz;
            mw = std::max(mw, 0);
            mw = std::min(mw, 64);
            const uint64_t m = br.get(mw);
            prev ^= (mw >= 64 ? m : (m << tz));
          }
        } else {
          if (br.get1() != 0) {
            if (br.get1() == 0) {
              const int width = 64 - pl_z - pt_z;
              if (width > 0 && width <= 64) {
                uint64_t m = 0;
                for (int k = 0; k < width; ++k) {
                  m = (m << 1) | static_cast<uint64_t>(br.get1());
                }
                prev ^= (m << pt_z);
              }
            } else {
              const int lz = static_cast<int>(br.get(5));
              const int tz = static_cast<int>(br.get(6));
              int mw = 64 - lz - tz;
              mw = std::max(mw, 1);
              mw = std::min(mw, 64);
              prev ^= (mw >= 64 ? br.get(mw) : (br.get(mw) << tz));
              pl_z = lz;
              pt_z = tz;
            }
          }
        }
        const auto sp = static_cast<int64_t>(prev);
        if (sp >= key) {
          *pos = t;
          return true;
        }
      }
      *pos = n;
      return false;
    }
    case Scheme::kFsst:
    case Scheme::kFrontCode:
    case Scheme::kTemplate:
    case Scheme::kDStream:
    case Scheme::kChimp128:
    // SIMPLE-8B regions have no monotonicity contract, so a binary search
    // over the decoder would be unsound; report "no bound" and let the
    // caller fall back (same door as the float streams above).
    case Scheme::kSimple8b:
      break;
  }
  return false;
}

}  // namespace tinylamb::pax_detail
