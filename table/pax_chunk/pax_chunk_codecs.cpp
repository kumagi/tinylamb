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
#include <cmath>
#include <utility>

#include "table/pax_chunk/pax_chunk.hpp"
#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb::pax_detail {

bool ParseRleBpRegion(const char* r, uint32_t n, uint32_t param,
                      const char* end, RleBpRegion* out) {
  if (r == nullptr || end == nullptr || end < r || n == 0 ||
      static_cast<size_t>(end - r) < 14) {
    return false;
  }
  const uint32_t value_width = static_cast<uint8_t>(r[8]);
  const uint32_t length_width = static_cast<uint8_t>(r[9]);
  const uint32_t runs = ReadBE32(r + 10);
  if (value_width == 0 || value_width > 64 || length_width == 0 ||
      length_width > 32 || runs == 0 || runs != param || runs > n) {
    return false;
  }
  const uint64_t value_bits = static_cast<uint64_t>(runs) * value_width;
  const uint64_t length_bits = static_cast<uint64_t>(runs) * length_width;
  const size_t value_bytes =
      (value_bits / 8) + static_cast<size_t>(value_bits % 8 != 0);
  const size_t length_bytes =
      (length_bits / 8) + static_cast<size_t>(length_bits % 8 != 0);
  if (value_bytes > static_cast<size_t>(end - r) - 14 ||
      length_bytes > static_cast<size_t>(end - r) - 14 - value_bytes) {
    return false;
  }
  RleBpRegion p;
  p.values = r + 14;
  p.lengths = p.values + value_bytes;
  p.n = n;
  p.runs = runs;
  p.value_width = value_width;
  p.length_width = length_width;
  p.base = static_cast<int64_t>(ReadBE64(r));
  uint64_t covered = 0;
  for (uint32_t i = 0; i < runs; ++i) {
    const auto length = static_cast<uint32_t>(
        UnpackBitAt(p.lengths, static_cast<uint64_t>(i) * length_width,
                    static_cast<int>(length_width)));
    if (length == 0 || static_cast<uint64_t>(length) > n - covered) {
      return false;
    }
    covered += length;
  }
  if (covered != n) {
    return false;
  }
  *out = p;
  return true;
}

int64_t RleBpValueAt(const RleBpRegion& p, uint32_t run) {
  if (run >= p.runs) {
    return 0;
  }
  const uint64_t offset =
      UnpackBitAt(p.values, static_cast<uint64_t>(run) * p.value_width,
                  static_cast<int>(p.value_width));
  return static_cast<int64_t>(static_cast<uint64_t>(p.base) + offset);
}

uint32_t RleBpLengthAt(const RleBpRegion& p, uint32_t run) {
  if (run >= p.runs) {
    return 0;
  }
  return static_cast<uint32_t>(
      UnpackBitAt(p.lengths, static_cast<uint64_t>(run) * p.length_width,
                  static_cast<int>(p.length_width)));
}

// ---------------------------------------------------------------------------
// kRleIdx (scheme 49): monotone run-length with a 16-run restart index.
//
//   [runs u32 BE][nidx u32 BE]
//   [values:  FOR-BA, runs entries: base 8B BE, bpv u8, runs*bpv LE offsets]
//   [lengths: FOR-BA, runs entries: base 8B BE, bpv u8, runs*bpv LE offsets]
//   [restart: nidx x u32 BE]  nidx == ceil(runs/16); entry e is the row index
//                             of run 16*e.
//
// Both payload streams reuse the byte-aligned FOR-BA layout (scheme 3) exactly,
// so the native/WASM decoders, the inner-extent arithmetic in ValidateWalk and
// the WASM `forba_off` helper are shared with the existing scheme instead of
// being re-derived here.
//
// The encoder gates on NON-DECREASING input, so the run values are themselves
// sorted and the restart table turns row -> run into a binary search plus a
// <= 15-run walk. That is what the read paths below rely on; the sortedness
// invariant itself is enforced by EncRleIdxI64 and re-checked by ValidateWalk
// (the same split Elias-Fano makes for its own binary-searchable arm), so the
// read-side parse stays O(nidx) -- nidx <= 64 for a 1,024-row region -- instead
// of re-walking every run.

// Byte-aligned FOR-BA offset load. `bpv` is already capped to [1,8] by the
// parser and the caller bounds `p` against the region, so the assembly loop is
// shift-safe by construction.
uint64_t RleIdxForBaOff(const char* p, uint32_t bpv) {
  uint64_t off = 0;
  for (uint32_t b = 0; b < bpv; ++b) {
    off |= static_cast<uint64_t>(static_cast<unsigned char>(p[b])) << (b * 8U);
  }
  return off;
}

bool ParseRleIdxRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, RleIdxRegion* out) {
  if (r == nullptr || end == nullptr || end < r || out == nullptr || n == 0 ||
      n > PaxChunk::kRowGroupSize || param == 0) {
    return false;
  }
  if (static_cast<size_t>(end - r) < 8) {
    return false;
  }
  const uint32_t runs = ReadBE32(r);
  const uint32_t nidx = ReadBE32(r + 4);
  if (runs == 0 || runs > n || runs != param || nidx == 0) {
    return false;
  }
  if (nidx != (runs + kRleIdxRestartStride - 1) / kRleIdxRestartStride) {
    return false;
  }
  // Both FOR-BA headers live at fixed offsets: values at +8, lengths right
  // after the value stream, whose extent is a function of `runs` and `bpv`.
  const char* vhead = r + 8;
  if (vhead + 9 > end) {
    return false;
  }
  const uint32_t value_bpv = static_cast<uint8_t>(vhead[8]);
  if (value_bpv == 0 || value_bpv > 8) {
    return false;
  }
  const size_t value_bytes = static_cast<size_t>(runs) * value_bpv;
  const char* lhead = vhead + 9 + value_bytes;
  if (lhead + 9 > end) {
    return false;
  }
  const uint32_t length_bpv = static_cast<uint8_t>(lhead[8]);
  if (length_bpv == 0 || length_bpv > 8) {
    return false;
  }
  const size_t length_bytes = static_cast<size_t>(runs) * length_bpv;
  const char* restart = lhead + 9 + length_bytes;
  if (restart + (static_cast<size_t>(nidx) * 4) > end) {
    return false;
  }

  RleIdxRegion p;
  p.values = vhead;
  p.lengths = lhead;
  p.restart = restart;
  p.n = n;
  p.runs = runs;
  p.nidx = nidx;
  p.value_bpv = value_bpv;
  p.length_bpv = length_bpv;
  p.value_base = static_cast<int64_t>(ReadBE64(vhead));
  p.length_base = static_cast<int64_t>(ReadBE64(lhead));
  // Restart-table integrity: entry 0 is row 0, entries are non-decreasing and
  // inside the region. This is the table every bounded walk below anchors on,
  // so a hostile value here must not be able to move a walk's start.
  uint32_t previous = 0;
  for (uint32_t e = 0; e < nidx; ++e) {
    const uint32_t row = ReadBE32(restart + (static_cast<size_t>(e) * 4));
    if (row < previous || row > n) {
      return false;
    }
    previous = row;
  }
  if (ReadBE32(restart) != 0) {
    return false;
  }
  *out = p;
  return true;
}

int64_t RleIdxValueAt(const RleIdxRegion& p, uint32_t run) {
  if (run >= p.runs) {
    return 0;
  }
  // FOR-BA payload starts one byte past the header (base 8B + bpv 1B).
  const uint64_t offset = RleIdxForBaOff(
      p.values + 9 + (static_cast<size_t>(run) * p.value_bpv), p.value_bpv);
  return static_cast<int64_t>(static_cast<uint64_t>(p.value_base) + offset);
}

uint32_t RleIdxLengthAt(const RleIdxRegion& p, uint32_t run) {
  if (run >= p.runs) {
    return 0;
  }
  const uint64_t offset = RleIdxForBaOff(
      p.lengths + 9 + (static_cast<size_t>(run) * p.length_bpv), p.length_bpv);
  return static_cast<uint32_t>(static_cast<uint64_t>(p.length_base) + offset);
}

// Row index at which run `run` starts. O(1) from the restart table plus a walk
// of at most kRleIdxRestartStride - 1 lengths; the walk is clamped at n so a
// malformed stream degrades to "no such run" instead of reading past n.
uint32_t RleIdxRowBefore(const RleIdxRegion& p, uint32_t run) {
  if (run == 0) {
    return 0;
  }
  if (run >= p.runs) {
    return p.n;
  }
  const uint32_t entry = run / kRleIdxRestartStride;
  uint64_t row = ReadBE32(p.restart + (static_cast<size_t>(entry) * 4));
  for (uint32_t i = entry * kRleIdxRestartStride; i < run; ++i) {
    row += RleIdxLengthAt(p, i);
  }
  return static_cast<uint32_t>(row > p.n ? p.n : row);
}

// Run holding `row`: binary search over the restart table (O(log nidx)) plus a
// bounded walk. Returns `runs` when the row falls past every run.
uint32_t RleIdxRunAt(const RleIdxRegion& p, uint32_t row) {
  if (row >= p.n) {
    return p.runs;
  }
  uint32_t lo = 0;
  uint32_t hi = p.nidx;  // first entry whose row > `row`
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (ReadBE32(p.restart + (static_cast<size_t>(mid) * 4)) <= row) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo == 0) {
    return 0;
  }
  uint32_t run = (lo - 1) * kRleIdxRestartStride;
  uint64_t at = ReadBE32(p.restart + (static_cast<size_t>(lo - 1) * 4));
  while (run < p.runs && at <= row) {
    at += RleIdxLengthAt(p, run);
    if (at > row) {
      break;
    }
    ++run;
  }
  return run;
}

// First run whose value is >= `key` (std::lower_bound over the sorted run
// values). Relies on the monotone-input invariant (encoder + ValidateWalk),
// exactly like the Elias-Fano arm: a hand-built footer that bypasses
// ValidateWalk can only make this return a valid run index, never an
// out-of-region read.
uint32_t RleIdxLowerBound(const RleIdxRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.runs;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (RleIdxValueAt(p, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// First run whose value is > `key` (std::upper_bound).
uint32_t RleIdxUpperBound(const RleIdxRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.runs;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (RleIdxValueAt(p, mid) <= key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// The passing runs form ONE contiguous run window for every CompareOp, because
// the run values are sorted: each comparison's truth set is an interval in the
// run index, and the interval's endpoints are exactly the two value bounds
// around `lit`. EQ is the degenerate case [lower_bound, upper_bound) -- with a
// run-length encoding the run values are strictly increasing, so that window
// holds at most one run, but the two-bound form is used anyway because it stays
// correct if a future encoder ever emits equal adjacent run values.
//
// (Bisecting on `passes(mid)` directly would be wrong for EQ: the truth set
// true,false,false,... is not monotone increasing, so that bisection would
// converge past the matching run.)
bool RleIdxPassingWindow(const RleIdxRegion& p, PaxChunk::CompareOp op,
                         int64_t lit, uint32_t* first, uint32_t* last) {
  uint32_t f = 0;
  uint32_t l = p.runs;
  switch (op) {
    case PaxChunk::CompareOp::kLe:
      l = RleIdxUpperBound(p, lit);
      break;
    case PaxChunk::CompareOp::kLt:
      l = RleIdxLowerBound(p, lit);
      break;
    case PaxChunk::CompareOp::kGe:
      f = RleIdxLowerBound(p, lit);
      break;
    case PaxChunk::CompareOp::kGt:
      f = RleIdxUpperBound(p, lit);
      break;
    case PaxChunk::CompareOp::kEq:
      f = RleIdxLowerBound(p, lit);
      l = RleIdxUpperBound(p, lit);
      break;
    default:
      return false;
  }
  *first = f;
  *last = l;
  return true;
}

// ---------------------------------------------------------------------------
// kStreamVByte (scheme 50): byte-aligned variable-byte ("group varint") with a
// separate control plane and a block index.
//
//   [stream_len u32 BE][nblk u16 BE][block_off: nblk x u16 BE]
//   [ctrl: ceil(n/4) bytes][payload: stream_len bytes]
//
// ctrl byte c holds the 2-bit width selectors of values 4c..4c+3, MSB-first
// (selector s => the value occupies s+1 bytes). The payload is those values
// back to back, each little-endian in its own width, holding the value's
// ZIGZAG encoding truncated to that width. block_off[b] is the payload offset
// of value b*kStreamVByteBlock, so a probe is that one lookup plus a walk of at
// most kStreamVByteBlock-1 widths inside the block.
//
// Two properties the rest of the engine does not have:
//
//  * BYTE-ADDRESSABLE VALUES. Every other 2-4-byte int codec bit-packs, so a
//    value straddles byte boundaries and a reader must reproduce this engine's
//    exact bit order (the reason the ValidateWalk / WASM parse_footer cases
//    have to stay in lockstep). Here a value is whole bytes: one unaligned
//    load plus `u>>1 ^ -(u&1)`.
//  * A BOUNDED, INDEXED PROBE. The control plane alone cannot place value i
//    (its payload offset is the prefix sum of ALL preceding widths, so reading
//    it is O(n)); the block index is what turns that into O(<=15) -- the same
//    trick kRleIdx's 16-run restart, kDeltaFor's 128-row checkpoints and
//    kXorForC's seek table play, at 2 bytes per 16 values (0.125 B/row).
//    That is the difference from kSimple8b, the nearest rival in size, whose
//    probe replays up to 240 rows of words from the region head.
//
// stream_len is the exact cell extent (a function of n and the walked width
// sum), so ValidateWalk pins the header AND every block_off against the walk,
// and the WASM parse_footer derives the identical number. The width sum is at
// most 4n <= 4096, which is why the block offsets fit in u16. A hand-built
// footer can therefore only make the parse fail closed, never over-read: every
// extent below is checked against `end` before a single load.

// Zigzag: maps int64 onto uint64 order-preservingly, so a truncated low word
// still decodes exactly (the encoder bails above 4 bytes, so nothing is lost).
uint64_t StreamVByteZigZag(int64_t v) {
  return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}
int64_t StreamVByteUnZigZag(uint64_t z) {
  return static_cast<int64_t>((z >> 1) ^ (~(z & 1) + 1));
}

// Width in bytes of value `i` (1..4). `i < n` is a caller contract; the
// control plane is ctrl_len = ceil(n/4) bytes, validated once by the parse.
uint32_t StreamVByteWidth(const StreamVByteRegion& v, uint32_t i) {
  const uint32_t c = v.ctrl[i / kStreamVBytePerCtrl];
  return ((c >> (6 - 2 * (i % kStreamVBytePerCtrl))) & 0x3) + 1;
}

// Assemble the little-endian zigzag word of value `off` in the payload.
int64_t StreamVByteLoad(const StreamVByteRegion& v, uint32_t off, uint32_t w) {
  uint32_t raw = 0;
  for (uint32_t b = 0; b < w; ++b) {
    raw |= static_cast<uint32_t>(v.payload[off + b]) << (8 * b);
  }
  return StreamVByteUnZigZag(static_cast<uint64_t>(raw));
}

// Payload offset of value `i`: the block index gives the base, then at most
// kStreamVByteBlock-1 width adds place the value inside its own block.
uint32_t StreamVByteOffset(const StreamVByteRegion& v, uint32_t i) {
  const size_t block = i / kStreamVByteBlock;
  const uint32_t first = static_cast<uint32_t>(block) * kStreamVByteBlock;
  uint32_t off = (static_cast<uint32_t>(v.block_off[block * 2]) << 8) |
                 static_cast<uint32_t>(v.block_off[(block * 2) + 1]);
  for (uint32_t j = first; j < i; ++j) {
    off += StreamVByteWidth(v, j);
  }
  return off;
}

int64_t StreamVByteValueAt(const StreamVByteRegion& v, uint32_t i) {
  const uint32_t w = StreamVByteWidth(v, i);
  const uint32_t off = StreamVByteOffset(v, i);
  if (off + w > v.stream_len) {
    return 0;  // fail closed
  }
  return StreamVByteLoad(v, off, w);
}

// Bounds-checked header parse. Mirrors ValidateWalk's case 50 and the WASM
// `streamvbyte_parse` field for field: the header must echo the footer's
// stream_len, nblk must equal ceil(n/16), the control plane is exactly
// ceil(n/4) bytes, every block_off must equal the walked prefix sum at its
// block boundary, and the total must land on stream_len. `end` is the cell's
// successor offset, so a region that only *looks* big enough fails here rather
// than in a decoder.
bool ParseStreamVByteRegion(const char* r, uint32_t n, uint32_t param,
                            const char* end, StreamVByteRegion* out) {
  if (r == nullptr || out == nullptr || end == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  if (end - r < 6) {
    return false;
  }
  const uint32_t stream_len = ReadBE32(r);
  if (stream_len != param) {
    return false;
  }
  // Selector 3 is the widest value, so the payload can never exceed 4n.
  if (stream_len == 0 || stream_len > n * kStreamVByteMaxWidth) {
    return false;
  }
  const uint32_t nblk = (n + kStreamVByteBlock - 1) / kStreamVByteBlock;
  const uint32_t ctrl_len = (n + kStreamVBytePerCtrl - 1) / kStreamVBytePerCtrl;
  const uint64_t need =
      6ULL + (2ULL * nblk) + static_cast<uint64_t>(ctrl_len) + stream_len;
  if (std::cmp_greater(need, end - r)) {
    return false;
  }
  const auto* base = reinterpret_cast<const uint8_t*>(r);
  const uint8_t* block_off = base + 6;
  if (ReadBE16(r + 4) != nblk) {
    return false;
  }
  const uint8_t* ctrl = block_off + (2ULL * nblk);
  const uint8_t* payload = ctrl + ctrl_len;
  // One width walk validates three things at once: every block_off equals the
  // walked prefix at its boundary, the total equals stream_len, and no value
  // can run past the payload (so every per-value offset the readers compute
  // from the index stays inside the cell).
  uint64_t sum = 0;
  for (uint32_t i = 0; i < n; ++i) {
    if (i % kStreamVByteBlock == 0) {
      const size_t b = i / kStreamVByteBlock;
      const uint32_t stored = (static_cast<uint32_t>(block_off[b * 2]) << 8) |
                              static_cast<uint32_t>(block_off[(b * 2) + 1]);
      if (stored != sum) {
        return false;
      }
    }
    sum += ((static_cast<unsigned>(ctrl[i / kStreamVBytePerCtrl]) >>
             (6 - 2 * (i % kStreamVBytePerCtrl))) &
            0x3U) +
           1U;
    if (sum > stream_len) {
      return false;
    }
  }
  if (sum != stream_len) {
    return false;
  }
  out->block_off = block_off;
  out->ctrl = ctrl;
  out->payload = payload;
  out->n = n;
  out->stream_len = stream_len;
  out->nblk = nblk;
  out->ctrl_len = ctrl_len;
  return true;
}

// Whole-cell decode: one forward pass, so the block index is not consulted
// (its whole job is the random probe). Rows the width walk could not place
// (only reachable from a hand-built footer that bypassed ValidateWalk) decode
// as raw 0, the same zero-participation rule the kRle / kRleBp / kRleIdx arms
// use.
void DecodeStreamVByteAll(const StreamVByteRegion& v, int64_t* out) {
  uint32_t off = 0;
  for (uint32_t i = 0; i < v.n; ++i) {
    const uint32_t w = StreamVByteWidth(v, i);
    if (off + w > v.stream_len) {
      std::fill(out + i, out + v.n, 0);
      return;
    }
    out[i] = StreamVByteLoad(v, off, w);
    off += w;
  }
}

// Values are stored in row order with no ordering structure, so a lower bound
// is a plain binary search whose probe cost is the indexed StreamVByteValueAt.
uint32_t StreamVByteLowerBound(const StreamVByteRegion& v, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = v.n;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (StreamVByteValueAt(v, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

bool ParseRle8Region(const char* r, uint32_t n, uint32_t param, const char* end,
                     Rle8Region* out) {
  if (r == nullptr || end == nullptr || end < r || out == nullptr || n == 0 ||
      n > PaxChunk::kRowGroupSize || param < 8 || param > 8U * n) {
    return false;
  }
  const size_t expected = 16U + (3U * static_cast<size_t>(param));
  if (std::cmp_not_equal(end - r, expected)) {
    return false;
  }

  Rle8Region& parsed = *out;
  parsed.n = n;
  parsed.total_runs = 0;
  const char* cursor = r;
  for (uint32_t plane = 0; plane < 8; ++plane) {
    if (static_cast<size_t>(end - cursor) < 2) {
      return false;
    }
    const uint16_t runs = ReadBE16(cursor);
    cursor += 2;
    if (runs == 0 || runs > n) {
      return false;
    }
    parsed.plane_runs[plane] = runs;
    parsed.plane_offsets[plane] = parsed.total_runs;
    uint32_t covered = 0;
    uint8_t previous = 0;
    for (uint32_t run = 0; run < runs; ++run) {
      if (static_cast<size_t>(end - cursor) < 3) {
        return false;
      }
      const auto value = static_cast<uint8_t>(cursor[0]);
      const uint16_t length = ReadBE16(cursor + 1);
      cursor += 3;
      if (length == 0 || static_cast<uint32_t>(length) > n - covered) {
        return false;
      }
      if (run != 0 && value == previous) {
        return false;
      }
      parsed.runs[parsed.total_runs++] =
          Rle8Run{.start = covered, .length = length, .value = value};
      covered += length;
      previous = value;
    }
    if (covered != n) {
      return false;
    }
  }
  return parsed.total_runs == param && cursor == end;
}

bool ValidateRle8Region(const char* r, uint32_t n, uint32_t param,
                        const char* end, std::array<const char*, 8>* plane_data,
                        std::array<uint32_t, 8>* plane_counts,
                        uint32_t* total_runs) {
  if (r == nullptr || end == nullptr || end < r || plane_data == nullptr ||
      plane_counts == nullptr || total_runs == nullptr || n == 0 ||
      n > PaxChunk::kRowGroupSize || param < 8 || param > 8U * n) {
    return false;
  }
  const size_t expected = 16U + (3U * static_cast<size_t>(param));
  if (std::cmp_not_equal(end - r, expected)) {
    return false;
  }

  const char* cursor = r;
  uint32_t total = 0;
  for (uint32_t plane = 0; plane < 8; ++plane) {
    if (static_cast<size_t>(end - cursor) < 2) {
      return false;
    }
    const uint32_t runs = ReadBE16(cursor);
    cursor += 2;
    if (runs == 0 || runs > n) {
      return false;
    }
    if (static_cast<size_t>(runs) > (static_cast<size_t>(end - cursor) / 3U)) {
      return false;
    }
    (*plane_counts)[plane] = runs;
    (*plane_data)[plane] = cursor;
    const char* plane_end = cursor + (3U * static_cast<size_t>(runs));
    auto value = static_cast<uint8_t>(cursor[0]);
    uint32_t length = ReadBE16(cursor + 1);
    if (length == 0 || length > n) {
      return false;
    }
    cursor += 3;
    uint32_t covered = length;
    for (uint32_t run = 1; run < runs; ++run) {
      const auto next_value = static_cast<uint8_t>(cursor[0]);
      length = ReadBE16(cursor + 1);
      cursor += 3;
      if (length == 0 || length > n - covered || next_value == value) {
        return false;
      }
      covered += length;
      value = next_value;
    }
    cursor = plane_end;
    if (covered != n) {
      return false;
    }
    total += runs;
  }
  if (total != param || cursor != end) {
    return false;
  }
  *total_runs = total;
  return true;
}

uint32_t ReadRle8Length(const char* run) {
  return (static_cast<uint32_t>(static_cast<uint8_t>(run[1])) << 8) |
         static_cast<uint8_t>(run[2]);
}

int64_t Rle8ValueAt(const Rle8Region& p, uint32_t index) {
  if (index >= p.n) {
    return 0;
  }
  uint64_t ordered = 0;
  for (uint32_t plane = 0; plane < 8; ++plane) {
    const uint32_t first = p.plane_offsets[plane];
    const uint32_t count = p.plane_runs[plane];
    uint32_t lo = 0;
    uint32_t hi = count;
    while (lo < hi) {
      const uint32_t mid = lo + ((hi - lo) / 2);
      if (p.runs[first + mid].start <= index) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo == 0) {
      return 0;
    }
    const Rle8Run& run = p.runs[first + lo - 1];
    if (index >= run.start + run.length) {
      return 0;
    }
    ordered = (ordered << 8) | static_cast<uint64_t>(run.value);
  }
  return static_cast<int64_t>(ordered ^ (1ULL << 63));
}

// The parsed kRle8 region is ~96KB (8192 runs x 12B). When ParseRle8Region
// was inlined into the scalar dispatchers (Int64RegionAt / RegionLowerBound),
// every caller's frame grew to hold it: one stack-clash probe walk of ~24
// pages per call, paid by ALL 40 schemes. These out-of-line wrappers own the
// frames so the dispatchers stay small; kRle8 columns pay one call each.
// noinline is load-bearing: with a single call site GCC would re-inline
// these (frame and all) into the dispatcher.
__attribute__((noinline)) int64_t Rle8ValueAtParsed(const char* r, uint32_t n,
                                                    uint32_t param,
                                                    const char* region_end,
                                                    uint32_t idx) {
  Rle8Region p;
  if (!ParseRle8Region(r, n, param, region_end, &p)) {
    return 0;
  }
  return Rle8ValueAt(p, idx);
}

// First index whose logical kRle8 value >= key; false when all < key.
__attribute__((noinline)) bool Rle8RegionLowerBound(const char* r, uint32_t n,
                                                    uint32_t param,
                                                    const char* region_end,
                                                    int64_t key,
                                                    uint32_t* pos) {
  Rle8Region parsed;
  if (n == 0 || !ParseRle8Region(r, n, param, region_end, &parsed)) {
    *pos = n;
    return false;
  }
  uint32_t lo = 0;
  uint32_t hi = n;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (Rle8ValueAt(parsed, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  *pos = lo;
  return lo < n;
}

uint64_t Rle8Compose(const std::array<uint8_t, 8>& values) {
  return (static_cast<uint64_t>(values[0]) << 56) |
         (static_cast<uint64_t>(values[1]) << 48) |
         (static_cast<uint64_t>(values[2]) << 40) |
         (static_cast<uint64_t>(values[3]) << 32) |
         (static_cast<uint64_t>(values[4]) << 24) |
         (static_cast<uint64_t>(values[5]) << 16) |
         (static_cast<uint64_t>(values[6]) << 8) |
         static_cast<uint64_t>(values[7]);
}

void Rle8TransposeStore(const std::array<uint64_t, 8>& plane_rows,
                        int64_t* out) {
  for (uint32_t lane = 0; lane < 8; ++lane) {
    const uint32_t shift = lane * 8U;
    const uint64_t ordered = (((plane_rows[0] >> shift) & 0xFFU) << 56) |
                             (((plane_rows[1] >> shift) & 0xFFU) << 48) |
                             (((plane_rows[2] >> shift) & 0xFFU) << 40) |
                             (((plane_rows[3] >> shift) & 0xFFU) << 32) |
                             (((plane_rows[4] >> shift) & 0xFFU) << 24) |
                             (((plane_rows[5] >> shift) & 0xFFU) << 16) |
                             (((plane_rows[6] >> shift) & 0xFFU) << 8) |
                             ((plane_rows[7] >> shift) & 0xFFU);
    out[lane] = static_cast<int64_t>(ordered ^ (1ULL << 63));
  }
}

bool DecodeRle8All(const char* r, uint32_t n, uint32_t param, const char* end,
                   int64_t* out) {
  std::array<const char*, 8> plane_data{};
  std::array<uint32_t, 8> plane_counts{};
  uint32_t total_runs = 0;
  if (!ValidateRle8Region(r, n, param, end, &plane_data, &plane_counts,
                          &total_runs)) {
    return false;
  }

  std::array<const char*, 8> cursor = plane_data;
  std::array<uint8_t, 8> values{};
  std::array<uint32_t, 8> boundaries{};
  for (uint32_t plane = 0; plane < 8; ++plane) {
    const char* q = plane_data[plane];
    values[plane] = static_cast<uint8_t>(q[0]);
    boundaries[plane] = ReadRle8Length(q);
    cursor[plane] = q + 3;
  }

  if (total_runs <= n / 2) {
    uint32_t row = 0;
    while (row < n) {
      uint32_t next_row = n;
      for (uint32_t plane = 0; plane < 8; ++plane) {
        const char*& q = cursor[plane];
        while (boundaries[plane] <= row) {
          values[plane] = static_cast<uint8_t>(q[0]);
          boundaries[plane] += ReadRle8Length(q);
          q += 3;
        }
        next_row = std::min(next_row, boundaries[plane]);
      }
      if (next_row <= row) {
        return false;
      }
      const auto value =
          static_cast<int64_t>(Rle8Compose(values) ^ (1ULL << 63));
      std::fill(out + row, out + next_row, value);
      row = next_row;
    }
    return true;
  }

  constexpr uint64_t kByteLanes = 0x0101010101010101ULL;
  uint32_t row = 0;
  if (total_runs > 2U * n) {
    for (; row < n; ++row) {
      for (uint32_t plane = 0; plane < 8; ++plane) {
        const char*& q = cursor[plane];
        while (boundaries[plane] <= row) {
          values[plane] = static_cast<uint8_t>(q[0]);
          boundaries[plane] += ReadRle8Length(q);
          q += 3;
        }
      }
      out[row] = static_cast<int64_t>(Rle8Compose(values) ^ (1ULL << 63));
    }
    return true;
  }

  for (; row + 8 <= n; row += 8) {
    std::array<uint64_t, 8> plane_rows{};
    for (uint32_t plane = 0; plane < 8; ++plane) {
      const char*& q = cursor[plane];
      uint32_t lane = 0;
      while (lane < 8) {
        const uint32_t target = row + lane;
        while (boundaries[plane] <= target) {
          values[plane] = static_cast<uint8_t>(q[0]);
          boundaries[plane] += ReadRle8Length(q);
          q += 3;
        }
        const uint32_t take = std::min(boundaries[plane] - target, 8U - lane);
        const uint32_t bits = take * 8U;
        const uint64_t range = bits == 64 ? ~0ULL : ((1ULL << bits) - 1ULL);
        plane_rows[plane] |= ((kByteLanes * values[plane]) & range)
                             << (lane * 8U);
        lane += take;
      }
    }
    Rle8TransposeStore(plane_rows, out + row);
  }
  for (; row < n; ++row) {
    for (uint32_t plane = 0; plane < 8; ++plane) {
      const char*& q = cursor[plane];
      while (boundaries[plane] <= row) {
        values[plane] = static_cast<uint8_t>(q[0]);
        boundaries[plane] += ReadRle8Length(q);
        q += 3;
      }
    }
    out[row] = static_cast<int64_t>(Rle8Compose(values) ^ (1ULL << 63));
  }
  return true;
}

bool DecodeByteSliceAll(const char* region, uint32_t n, const char* end,
                        int64_t* out) {
  if (region == nullptr || end == nullptr || end < region || out == nullptr) {
    return false;
  }
  if (n == 0) {
    return true;
  }
  if (region + 1 > end) {
    return false;
  }
  const uint32_t width = static_cast<uint8_t>(region[0]);
  const size_t stride = (static_cast<size_t>(n) + 7) / 8;
  if (width == 0 || width > 64 ||
      region + 1 + (static_cast<size_t>(width) * stride) > end) {
    return false;
  }
  std::fill(out, out + n, int64_t{0});
  uint32_t row = 0;
#ifdef __BMI2__
  constexpr uint64_t kLaneBits = 0x0101010101010101ULL;
  for (; row + 8 <= n; row += 8) {
    const size_t byte = row >> 3;
    for (uint32_t base = 0; base < width; base += 8) {
      uint64_t packed = 0;
      const uint32_t lanes = std::min(8U, width - base);
      for (uint32_t lane = 0; lane < lanes; ++lane) {
        const size_t offset =
            1 + (static_cast<size_t>(base + lane) * stride) + byte;
        packed |= static_cast<uint64_t>(static_cast<uint8_t>(region[offset]))
                  << (lane * 8);
      }
      const uint64_t target = 0xffULL << base;
      for (uint32_t lane = 0; lane < 8; ++lane) {
        const uint64_t value = _pext_u64(packed, kLaneBits << (7U - lane));
        out[row + lane] |= static_cast<int64_t>(_pdep_u64(value, target));
      }
    }
  }
#endif
  for (; row < n; ++row) {
    uint64_t value = 0;
    for (uint32_t bit = 0; bit < width; ++bit) {
      const size_t offset =
          1 + (static_cast<size_t>(bit) * stride) + (row >> 3);
      const auto set = static_cast<uint64_t>(
          (static_cast<uint8_t>(region[offset]) >> (7U - (row & 7U))) & 1U);
      value |= set << bit;
    }
    out[row] = static_cast<int64_t>(value);
  }
  return true;
}

// NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)

uint64_t EncodeZigZag64(uint64_t value) {
  return (value << 1) ^ (0ULL - (value >> 63));
}

uint64_t DecodeZigZag64(uint64_t value) {
  return (value >> 1) ^ (0ULL - (value & 1ULL));
}

bool ParseRleDeltaRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, RleDeltaRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize || param == 0 || param > n ||
      static_cast<size_t>(end - r) < 14) {
    return false;
  }
  const uint32_t value_width = static_cast<uint8_t>(r[8]);
  const uint32_t length_width = static_cast<uint8_t>(r[9]);
  const uint32_t runs = ReadBE32(r + 10);
  if (value_width == 0 || value_width > 64 || length_width == 0 ||
      length_width > 32 || runs != param || runs > n) {
    return false;
  }
  const uint64_t value_bits = static_cast<uint64_t>(runs - 1) * value_width;
  const uint64_t length_bits = static_cast<uint64_t>(runs) * length_width;
  const uint64_t value_bytes = (value_bits + 7) / 8;
  const uint64_t length_bytes = (length_bits + 7) / 8;
  const uint64_t expected = 14 + value_bytes + length_bytes;
  if (std::cmp_not_equal(end - r, expected)) {
    return false;
  }

  RleDeltaRegion& parsed = *out;
  parsed.n = n;
  parsed.run_count = runs;
  parsed.value_width = value_width;
  parsed.length_width = length_width;
  const char* values = r + 14;
  const char* lengths = values + static_cast<size_t>(value_bytes);
  uint64_t value = ReadBE64(r);
  uint32_t covered = 0;
  for (uint32_t run = 0; run < runs; ++run) {
    const auto length = static_cast<uint32_t>(
        UnpackBitAt(lengths, static_cast<uint64_t>(run) * length_width,
                    static_cast<int>(length_width)));
    if (length == 0 || length > n - covered) {
      return false;
    }
    if (run != 0) {
      const uint64_t delta =
          UnpackBitAt(values, static_cast<uint64_t>(run - 1) * value_width,
                      static_cast<int>(value_width));
      value += DecodeZigZag64(delta);
    }
    parsed.runs[run] = RleDeltaRun{.start = covered,
                                   .length = length,
                                   .value = static_cast<int64_t>(value)};
    covered += length;
  }
  return covered == n;
}

int64_t RleDeltaValueAt(const RleDeltaRegion& region, uint32_t index) {
  if (index >= region.n) {
    return 0;
  }
  uint32_t low = 0;
  uint32_t high = region.run_count;
  while (low < high) {
    const uint32_t middle = low + ((high - low) / 2);
    if (region.runs[middle].start <= index) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  if (low == 0) {
    return 0;
  }
  return region.runs[low - 1].value;
}

bool DecodeRleDeltaAll(const char* r, uint32_t n, uint32_t param,
                       const char* end, int64_t* out) {
  RleDeltaRegion parsed;
  if (!ParseRleDeltaRegion(r, n, param, end, &parsed)) {
    return false;
  }
  for (uint32_t run = 0; run < parsed.run_count; ++run) {
    const RleDeltaRun& item = parsed.runs[run];
    std::fill(out + item.start, out + item.start + item.length, item.value);
  }
  return true;
}

// kD2Pfor (scheme 52): signed delta-of-delta + PFor, with a 256-row
// (val, sdp) seek table. See the enum comment for why the second
// difference is the pair that was missing from the derivative x inner-codec
// table (11 = delta+FOR-BA, 12 = delta+BitWeave, 37 = delta+PFor, 17 =
// dd+Rice).
//
//   [ncp u16 BE][ckpt: ncp x (val 8B BE, sdp 8B BE)]
//   [inner: a complete kPfor region over the n-2 second differences]
//
// Notation, all indices 0-based:
//   sd[k]  = v[k+1] - v[k]        for k in [0, n-1)   first differences
//   dd[k]  = sd[k+1] - sd[k]      for k in [0, n-2)   second differences
// Checkpoint b sits on row r = min(b*256, n-1) and stores v[r] plus
// `sdp` = sd[r-1] (or sd[0] when r == 0, which has no predecessor). That is
// the only asymmetry in the format, and it is why the walk below branches on
// r once instead of on every row.
//
// A probe is one checkpoint load plus at most kD2PforCheckpointEvery inner
// PFor reads; the forward decode is two prefix sums. Every load is bounded
// against `end` before it happens, and n <= PaxChunk::kRowGroupSize caps both
// the seek table and the inner region.
// n <= PaxChunk::kRowGroupSize caps the seek table and the inner region; the
// n >= 3 floor gives the inner PFor at least one value.
//
// The interval is 256, not the 128 the first-difference composites share. The
// second differences are an order of magnitude narrower than the first, so the
// 16 B checkpoint pair is a much larger share of THIS region than of a
// kDeltaFor one, and halving the entry count is worth more here than the
// shorter probe: measured on a 1024-row column it moves the seek table from
// 9 x 16 = 144 B (0.14 B/row) to 5 x 16 = 80 B (0.08 B/row), which is the
// difference between losing to kQpp on a clean quadratic and winning.

// Reads dd[k] straight out of the inner kPfor region. Mirrors the kPfor arm
// of Int64RegionAt: base + (w-bit offset | exception patch).
int64_t D2PforSecondDiff(const D2PforRegion& p, uint32_t k) {
  const char* r = p.inner;
  if (p.n < 3 || k > p.n - 3) {
    return 0;
  }
  const uint32_t cnt = p.n - 2;
  const auto base_v = static_cast<int64_t>(ReadBE64(r));
  const int w = static_cast<int>(static_cast<uint8_t>(r[8]));
  const uint32_t nexc = ReadBE16(r + 9);
  const char* packed = r + 11;
  const size_t pack_bytes =
      (static_cast<size_t>(cnt) * static_cast<size_t>(w) + 7) / 8;
  const char* exc = packed + pack_bytes;
  // Exception list is sorted by position; nexc is tiny for a smooth series.
  for (uint32_t e = 0; e < nexc; ++e) {
    const uint32_t pos = ReadBE16(exc + (static_cast<size_t>(e) * 10));
    if (pos == k) {
      return static_cast<int64_t>(
          ReadBE64(exc + (static_cast<size_t>(e) * 10) + 2));
    }
    if (pos > k) {
      break;
    }
  }
  if (w >= 64) {
    return static_cast<int64_t>(
        static_cast<uint64_t>(base_v) +
        UnpackBitAt(packed, static_cast<uint64_t>(k) * 64, 64));
  }
  const uint64_t off = UnpackBitAt(
      packed, static_cast<uint64_t>(k) * static_cast<uint64_t>(w), w);
  return static_cast<int64_t>(static_cast<uint64_t>(base_v) + off);
}

// `param` is the inner PFor width and must be echoed by the inner header, as
// in every other composite scheme. The checkpoint count is pinned to
// EncD2PforI64's formula so a hand-built footer cannot declare fewer entries
// than the seek walk will index.
bool ParseD2PforRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, D2PforRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n < 3 ||
      n > PaxChunk::kRowGroupSize || param > 64) {
    return false;
  }
  if (static_cast<size_t>(end - r) < 2) {
    return false;
  }
  const uint32_t ncp = ReadBE16(r);
  if (ncp != ((n + kD2PforCheckpointEvery - 1) / kD2PforCheckpointEvery) + 1) {
    return false;
  }
  const size_t header = 2 + (16 * static_cast<size_t>(ncp));
  if (std::cmp_less(end - r, header)) {
    return false;
  }
  // Inner = a kPfor region over n-2 values. Extent: 11 + ceil((n-2)*w/8) +
  // nexc*10, with the width byte echoed against `param`.
  const char* inner = r + header;
  const auto avail = static_cast<size_t>(end - inner);
  if (avail < 11) {
    return false;
  }
  const int w = static_cast<int>(static_cast<uint8_t>(inner[8]));
  if (std::cmp_not_equal(w, param)) {
    return false;
  }
  const uint32_t nexc = ReadBE16(inner + 9);
  const size_t cnt = static_cast<size_t>(n) - 2;
  const size_t need = 11 + ((cnt * static_cast<size_t>(w) + 7) / 8) +
                      (static_cast<size_t>(nexc) * 10);
  if (need > avail) {
    return false;
  }
  out->ckpt = r + 2;
  out->inner = inner;
  out->n = n;
  out->ncp = ncp;
  return true;
}

int64_t D2PforValueAt(const D2PforRegion& p, uint32_t idx) {
  if (idx >= p.n) {
    return 0;
  }
  const uint32_t b = idx / kD2PforCheckpointEvery;
  if (b >= p.ncp) {
    return 0;
  }
  // Unsigned-wrapping accumulators; see the long comment in DecodeD2PforAll.
  uint64_t val = ReadBE64(p.ckpt + (static_cast<size_t>(b) * 16));
  uint64_t sd = ReadBE64(p.ckpt + (static_cast<size_t>(b) * 16) + 8);
  const uint32_t r = std::min<uint32_t>(b * kD2PforCheckpointEvery, p.n - 1);
  if (idx <= r) {
    return static_cast<int64_t>(val);
  }
  // Row 0's checkpoint stored sd[0] itself; every later one stored sd[r-1],
  // so the first step needs dd[r-1] to become sd[r].
  // Invariant while walking: (val, sd) == (v[m], sd[m]). One step is
  // v[m+1] = v[m] + sd[m], then sd[m+1] = sd[m] + dd[m] to arm the next one.
  // dd only exists for m <= n-3, and the LAST step of the walk needs no dd
  // (it only produces v[idx]), so the read is guarded rather than left to
  // D2PforSecondDiff's own bounds check.
  //
  // Both accumulators are uint64_t on purpose: the scheme's contract is
  // UNSIGNED-WRAPPING arithmetic (the encoder subtracts through uint64_t, and
  // the tests round-trip a column that walks straight past INT64_MAX), so
  // signed `+=` here is undefined behaviour, not merely a slow path. The
  // decoder held int64_t and a column of large-magnitude values overflowed the
  // signed add on every row -- reported by the default UBSan build, and
  // liable to be "optimised" on the strength of no-overflow. This is the same
  // unsigned walk kDdRice and kDeltaFor already use; only the store back is a
  // reinterpretation, which is what the wraparound is for.
  if (r != 0) {
    sd += static_cast<uint64_t>(D2PforSecondDiff(p, r - 1));
  }
  for (uint32_t m = r; m < idx; ++m) {
    val += sd;
    if (m + 2 < p.n) {
      sd += static_cast<uint64_t>(D2PforSecondDiff(p, m));
    }
  }
  return static_cast<int64_t>(val);
}

void DecodeD2PforAll(const D2PforRegion& p, int64_t* out) {
  // Two prefix sums in one pass: sd[k+1] = sd[k] + dd[k] and
  // v[k+1] = v[k] + sd[k]. Starting from the row-0 checkpoint, whose `sdp` is
  // already sd[0], the walk needs no dd read for the first step.
  // Both prefix sums accumulate in the UNSIGNED domain, which is the scheme's
  // documented contract: the encoder derives sd/dd by subtracting through
  // uint64_t, so the inverse of that subtraction is a wrapping unsigned add.
  // Accumulating in int64_t instead is signed-overflow UB -- a column whose
  // values walk past INT64_MAX (exactly what the adaptive encoder selects this
  // scheme for: a large-magnitude ramp with narrow second differences)
  // overflows on every row. That is not theoretical: the default build turns
  // on -fsanitize=undefined, and UBSan reported it here while the round-trip
  // test still printed PASSED, because UBSan is non-fatal. Beyond the
  // diagnostic, the loop-carried `val += sd` chain is precisely the shape an
  // optimiser may rewrite on an assumed no-overflow premise. Matches the
  // existing unsigned walk in the kDdRice and kDeltaFor decoders.
  uint64_t val = ReadBE64(p.ckpt);
  uint64_t sd = ReadBE64(p.ckpt + 8);
  out[0] = static_cast<int64_t>(val);
  if (p.n == 1) {
    return;
  }
  for (uint32_t k = 0; k + 1 < p.n; ++k) {
    val += sd;
    out[k + 1] = static_cast<int64_t>(val);
    if (k + 2 < p.n) {
      sd += static_cast<uint64_t>(D2PforSecondDiff(p, k));
    }
  }
}

uint32_t D2PforLowerBound(const D2PforRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.n;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (D2PforValueAt(p, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// ---- kSimdFor (scheme 53) -------------------------------------------------
//
// Frame-of-reference bit-packing in a four-lane interleaved layout. See the
// enum comment for the wire format and the rationale; the short version is
// that a group is 32 bytes = four 8-byte lanes, and value i of the group lives
// in lane (i mod 4) at bit offset (i div 4) * w. Because w always divides 64,
// each lane holds exactly 64/w values with no slack, so the four lane loads are
// independent aligned 8-byte reads.
//
// ANY width 1..64 is supported: a lane holds floor(64/w) values, so the
// high bits of a lane go unused when w does not divide 64. That costs a few
// bits per 32 values and keeps all four lane reads aligned 8-byte loads, which
// is the entire point -- see SimdForPerLane for why flooring beats demanding
// an exact division.

// Lanes per group. Each is exactly one 8-byte word, so a group is 32 bytes.

// A width this scheme admits: any 1..64.
bool SimdForWidthOk(uint32_t w) { return w >= 1 && w <= 64; }

// Values per lane: floor(64/w), so EVERY width works and a lane is never
// overrun. floor, not an exact division, and that is the whole trick: requiring
// w | 64 (w in 1,2,4,8,16,32,64) would force the encoder to round a spread of
// 63 up from 6 bits to 8, which is exactly the case where this scheme is
// supposed to beat a byte-offset FOR codec -- it would lose to it. With floor
// the cost of an awkward width is at most a few unused high bits per lane
// (w = 6 leaves 2, w = 63 leaves 1) rather than a whole width step.
uint32_t SimdForPerLane(uint32_t w) { return 64 / w; }
uint32_t SimdForGroupValues(uint32_t w) {
  return kSimdForLanes * SimdForPerLane(w);
}
// Groups needed for n values, and the exact region extent they occupy.
uint64_t SimdForGroups(uint32_t n, uint32_t w) {
  const uint64_t gv = SimdForGroupValues(w);
  return (static_cast<uint64_t>(n) + gv - 1) / gv;
}
uint64_t SimdForExtent(uint32_t n, uint32_t w) {
  return kSimdForHeaderBytes +
         (static_cast<uint64_t>(kSimdForGroupBytes) * SimdForGroups(n, w));
}

uint64_t SimdForMask(uint32_t w) {
  return w >= 64 ? ~static_cast<uint64_t>(0)
                 : ((static_cast<uint64_t>(1) << w) - 1);
}

// Every extent is checked against `end`, and the width gate is the same
// predicate the encoder uses, so a hand-built footer can only make this fail
// closed. The extent is DERIVED from n and w (never read from the bytes),
// exactly like kBitWeave's, so there is no count field to under-state.
bool ParseSimdForRegion(const char* r, uint32_t n, uint32_t param,
                        const char* end, SimdForRegion* out) {
  if (param == 0 || param > 64 || !SimdForWidthOk(param)) {
    return false;
  }
  const uint64_t need = SimdForExtent(n, param);
  if (end == nullptr) {
    out->groups = r + kSimdForHeaderBytes;
    out->base = static_cast<int64_t>(ReadBE64(r));
    out->w = param;
    out->n = n;
    return true;
  }
  if (std::cmp_less(end - r, need)) {
    return false;
  }
  out->groups = r + kSimdForHeaderBytes;
  out->base = static_cast<int64_t>(ReadBE64(r));
  out->w = param;
  out->n = n;
  return true;
}

// One value. The four-lane shape means the read is: pick the group, pick the
// lane, ONE aligned 8-byte load, then a shift by (i div 4) * w.
int64_t SimdForValueAt(const SimdForRegion& p, uint32_t idx) {
  if (idx >= p.n) {
    return 0;
  }
  const uint32_t gv = SimdForGroupValues(p.w);
  const uint32_t g = idx / gv;
  const uint32_t r = idx % gv;
  const char* lane = p.groups + (static_cast<size_t>(g) * kSimdForGroupBytes) +
                     (static_cast<size_t>(r & 3U) * 8U);
  const uint64_t word = ReadBE64(lane);
  const unsigned shift = (r >> 2) * p.w;
  const uint64_t d = (word >> shift) & SimdForMask(p.w);
  return static_cast<int64_t>(static_cast<uint64_t>(p.base) + d);
}

void DecodeSimdForAll(const SimdForRegion& p, int64_t* out) {
  const uint32_t w = p.w;
  const uint64_t gv = SimdForGroupValues(w);
  const auto base_u = static_cast<uint64_t>(p.base);
  const uint64_t mask = SimdForMask(w);
  for (uint32_t idx = 0; idx < p.n; ++idx) {
    const auto g = static_cast<uint32_t>(idx / gv);
    const auto r = static_cast<uint32_t>(idx % gv);
    const char* lane = p.groups +
                       (static_cast<size_t>(g) * kSimdForGroupBytes) +
                       (static_cast<size_t>(r & 3U) * 8U);
    const uint64_t word = ReadBE64(lane);
    out[idx] = static_cast<int64_t>(base_u + ((word >> ((r >> 2) * w)) & mask));
  }
}

// Bisect over SimdForValueAt. Valid without a separate arm because kSimdFor is
// plain frame-of-reference over the raw values, so it IS order-preserving --
// the opposite of a transform like zigzag, where a lower bound would have to
// scan instead.
uint32_t SimdForLowerBound(const SimdForRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.n;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (SimdForValueAt(p, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// WAH (scheme 46) word fields: rows per literal span, and the 62-bit fill
// block count.

uint8_t Rle8ReverseByte(uint8_t value) {
  value =
      static_cast<uint8_t>(((value & 0x55U) << 1U) | ((value & 0xAAU) >> 1U));
  value =
      static_cast<uint8_t>(((value & 0x33U) << 2U) | ((value & 0xCCU) >> 2U));
  return static_cast<uint8_t>((value << 4U) | (value >> 4U));
}

// Reverses a BE WAH word into LSB-first row order: bit t = row t of the
// 63-row span (the type bit shifts out).
uint64_t WahLiteralBitsLE(const char* wp) {
  uint64_t le = 0;
  for (uint32_t byte = 0; byte < 8U; ++byte) {
    le |= static_cast<uint64_t>(Rle8ReverseByte(static_cast<uint8_t>(wp[byte])))
          << (byte * 8U);
  }
  return le >> 1;
}

bool ParseDeltaPForRegion(const char* region, uint32_t n, uint32_t param,
                          const char* end, DeltaPForRegion* out) {
  if (region == nullptr || end == nullptr || end < region || n == 0 ||
      n > PaxChunk::kRowGroupSize || param > 63 ||
      static_cast<size_t>(end - region) < 11) {
    return false;
  }
  const uint32_t width = static_cast<uint8_t>(region[8]);
  const uint32_t exception_count = ReadBE16(region + 9);
  const uint32_t blocks = (n + kDeltaPForBlockRows - 1) / kDeltaPForBlockRows;
  if (width != param || exception_count > n - 1) {
    return false;
  }
  const size_t fixed =
      11 + (static_cast<size_t>(blocks) * kDeltaPForBracketBytes);
  const size_t packed_bytes = (static_cast<size_t>(n - 1) * width + 7) / 8;
  const size_t exception_bytes = static_cast<size_t>(exception_count) * 10;
  if (std::cmp_greater(fixed, end - region) ||
      packed_bytes > static_cast<size_t>(end - region) - fixed ||
      exception_bytes >
          static_cast<size_t>(end - region) - fixed - packed_bytes) {
    return false;
  }

  const auto delta_min = static_cast<int64_t>(ReadBE64(region));
  if (delta_min < 0) {
    return false;
  }
  DeltaPForRegion parsed;
  parsed.brackets = region + 11;
  parsed.packed = parsed.brackets + fixed - 11;
  parsed.exceptions = parsed.packed + packed_bytes;
  parsed.n = n;
  parsed.blocks = blocks;
  parsed.width = width;
  parsed.exceptions_count = exception_count;
  parsed.delta_min = static_cast<uint64_t>(delta_min);

  for (uint32_t b = 0; b < blocks; ++b) {
    const char* bracket =
        parsed.brackets + (static_cast<size_t>(b) * kDeltaPForBracketBytes);
    const auto first = static_cast<int64_t>(ReadBE64(bracket));
    const auto last = static_cast<int64_t>(ReadBE64(bracket + 8));
    if (last < first) {
      return false;
    }
    if (b + 1 < blocks) {
      const auto next_first =
          static_cast<int64_t>(ReadBE64(bracket + kDeltaPForBracketBytes));
      if (next_first < last) {
        return false;
      }
    }
  }

  uint32_t previous = 0;
  for (uint32_t k = 0; k < exception_count; ++k) {
    const char* entry = parsed.exceptions + (static_cast<size_t>(k) * 10);
    const uint32_t pos = ReadBE16(entry);
    const auto delta = static_cast<int64_t>(ReadBE64(entry + 2));
    if (pos == 0 || pos >= n || (k != 0 && pos <= previous) || delta < 0 ||
        std::cmp_less(delta, parsed.delta_min)) {
      return false;
    }
    const uint64_t offset = static_cast<uint64_t>(delta) - parsed.delta_min;
    if ((offset >> width) == 0 ||
        UnpackBitAt(parsed.packed, static_cast<uint64_t>(pos - 1) * width,
                    static_cast<int>(width)) != 0) {
      return false;
    }
    previous = pos;
  }

  *out = parsed;
  return true;
}

uint32_t DeltaPForExceptionAtOrAfter(const DeltaPForRegion& p, uint32_t pos) {
  uint32_t lo = 0;
  uint32_t hi = p.exceptions_count;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    const uint32_t candidate =
        ReadBE16(p.exceptions + (static_cast<size_t>(mid) * 10));
    if (candidate < pos) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// Max Rice quotient the DdRice encoder emits (and every decoder scans).
// Bounds the unary run so hostile streams always terminate; columns needing
// more are better served by another scheme (the candidate is declined).

// Bound for the chunk-global per-column frequency maps feeding mode +
// histogram finalization (exact for low-cardinality columns; rare values
// beyond the cap are not histogrammed).

// Cap for the inverted-index candidate maps: cells reaching this many
// distinct values fall back to the Xor pre-filter instead.

// Exact 64-bit remainder by Barrett reduction. With M = floor((2^64-1)/m),
// q = high64(M * n) lands on floor(n/m) or one below it, so a single
// conditional subtract turns n - q*m into n % m. The Xor8 prefilter reduces
// three rotated hashes of every key modulo its table size, and the compiler
// emitted three hardware `divq` instructions for it -- 64-bit division is
// neither cheap nor pipelined, and it was the single hottest line in the
// chunk-build profile. Two multiplies replace them and produce bit-identical
// positions, so the emitted prefilter table is unchanged.

// Open-addressed int64 -> uint64 map for the encoder hot paths (dict code
// assignment, distinct counting, xor-set membership, key indexing). One flat
// table replaces the per-entry heap node of std::unordered_map; the
// constructor's 2x capacity cap keeps linear probing at load factor <= 1/2,
// so Find/InsertNew never rehash and never probe a full table.

// Adaptive-selector byte budget. The selector minimises
// bytes * decode_cost * hint_mult, so from the running best cost it can derive
// the largest region a candidate could still have and still win. `budget` is
// exactly that ceiling: an encoder that is already provably past it returns
// an empty region ("infeasible") instead of finishing a buffer it cannot win
// with. Because the cut is `size * m >= best_cost` -- the same strict
// inequality the selector's `<` applies -- the surviving candidate, and hence
// the emitted chunk, is bit-identical to encoding every candidate in full.

// Number of maximal equal-value runs in a value array. Used to price the RLE
// family (12 B/run) before building the run table, so a run-saturated column
// is rejected without materialising it.
size_t CountRuns(const std::vector<int64_t>& vals) {
  if (vals.empty()) {
    return 0;
  }
  size_t runs = 1;
  for (size_t i = 1; i < vals.size(); ++i) {
    runs += static_cast<size_t>(vals[i] != vals[i - 1]);
  }
  return runs;
}

// Select the position of the (idx+1)-th set bit in a bitmap of `len` bits.

// Full 64-bit reversal (byte swaps alone do not reverse bit order).
uint64_t BitRev64(uint64_t x) {
  x = ((x & 0x5555555555555555ULL) << 1) | ((x >> 1) & 0x5555555555555555ULL);
  x = ((x & 0x3333333333333333ULL) << 2) | ((x >> 2) & 0x3333333333333333ULL);
  x = ((x & 0x0f0f0f0f0f0f0f0fULL) << 4) | ((x >> 4) & 0x0f0f0f0f0f0f0f0fULL);
  return __builtin_bswap64(x);
}

// Position (MSB-first, 0..63) of the (k+1)-th set bit of a BE-loaded word;
// requires k < popcount(word). On the fully bit-reversed word, stream
// position q sits at bit q, so one pdep+tzcnt names it; the portable
// fallback clears the k highest set bits one at a time (same answer).
uint64_t SelectFromWord(uint64_t word, uint64_t k) {
#ifdef __BMI2__
  return static_cast<uint64_t>(
      __builtin_ctzll(_pdep_u64(1ULL << k, BitRev64(word))));
#else
  uint64_t x = word;
  while (k > 0) {
    const int hi = 63 - __builtin_clzll(x);
    x &= (static_cast<uint64_t>(1) << hi) - 1;
    --k;
  }
  return static_cast<uint64_t>(__builtin_clzll(x));
#endif
}

// Directory-guided variant (see definition): same answer, jumps to the
// every-8th-set-bit sample instead of scanning from word 0.

// Equi-width histogram bin index. Safe across the full i64 range: uses u64
// for the span (avoids signed overflow UB when mn/mx straddle INT64_MIN/MAX)
// and __uint128 for the bin multiply (avoids wrapping when diff*B > 2^64).
uint64_t HistogramBin(int64_t v, int64_t mn, int64_t mx, uint64_t B) {
  const auto mn_u = static_cast<uint64_t>(mn);
  const auto mx_u = static_cast<uint64_t>(mx);
  const auto v_u = static_cast<uint64_t>(v);
  const uint64_t span = mx_u - mn_u;
  if (span == 0) {
    return 0;
  }
  // Denominator widened to 128-bit: a column spanning the full i64 domain
  // ([INT64_MIN, INT64_MAX]) yields span == UINT64_MAX, so span + 1 would
  // wrap to 0 and divide by zero.
  const unsigned __int128 num = static_cast<unsigned __int128>(v_u - mn_u) * B;
  auto idx =
      static_cast<uint64_t>(num / (static_cast<unsigned __int128>(span) + 1));
  if (idx >= B) {
    idx = B - 1;
  }
  return idx;
}

// Count 1-bits in MSB-first bitmap `b` over positions [0, p) (exclusive).
uint32_t PopcountBits(const char* b, uint32_t p) {
  const uint32_t full = p >> 3;
  uint32_t cnt = 0;
  for (uint32_t i = 0; i < full; ++i) {
    cnt += static_cast<uint32_t>(__builtin_popcount(
        static_cast<unsigned int>(static_cast<uint8_t>(b[i]))));
  }
  const uint32_t rem = p & 7;
  // Only touch the partial byte when bits actually remain: when p is a
  // whole number of bytes AND equals the bitmap length, `b[full]` is one
  // byte past the bitmap (unconditional read = latent OOB).
  if (rem != 0) {
    const auto last = static_cast<uint8_t>(b[full]);
    for (uint32_t i = 0; i < rem; ++i) {
      cnt += static_cast<uint32_t>((last >> (7 - i)) & 1);
    }
  }
  return cnt;
}

// Count nibbles equal to 0xF in a little-endian-loaded word of kTopk's
// nibble stream: AND the four bit rows of every 4-bit lane, keep the lanes'
// lowest bits, popcount. Nibbles are self-contained 4-bit groups, so shifts
// of < 4 never leak across a lane and the same formula works on a promoted
// single byte (used for the partial-word tails).
uint32_t TopKSentinelCount(uint64_t w) {
  const uint64_t lanes =
      w & (w >> 1) & (w >> 2) & (w >> 3) & 0x1111111111111111ULL;
  return static_cast<uint32_t>(__builtin_popcountll(lanes));
}

uint32_t WaveletRRankCount(uint32_t n) {
  return (n / kWaveletRRankEvery) + 1 + (n % kWaveletRRankEvery != 0 ? 1 : 0);
}

bool ParseWaveletRRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, WaveletRRegion* out) {
  if (r == nullptr || end == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize || param != n ||
      static_cast<size_t>(end - r) < 9) {
    return false;
  }
  const uint32_t width = static_cast<uint8_t>(r[8]);
  if (width == 0 || width > 64) {
    return false;
  }
  const uint32_t stride = (n + 7) / 8;
  const uint32_t rank_count = WaveletRRankCount(n);
  const uint64_t need = 9 + (static_cast<uint64_t>(width) * stride) +
                        (static_cast<uint64_t>(width) * rank_count * 2);
  if (std::cmp_greater(need, end - r)) {
    return false;
  }

  WaveletRRegion p;
  p.bitmaps = r + 9;
  p.ranks = p.bitmaps + (static_cast<size_t>(width) * stride);
  p.minimum = static_cast<int64_t>(ReadBE64(r));
  p.n = n;
  p.width = width;
  p.stride = stride;
  p.rank_count = rank_count;
  for (uint32_t level = 0; level < width; ++level) {
    const char* bitmap = p.bitmaps + (static_cast<size_t>(level) * stride);
    const char* table = p.ranks + (static_cast<size_t>(level) * rank_count * 2);
    for (uint32_t block = 0; block < rank_count; ++block) {
      const uint32_t position =
          block * kWaveletRRankEvery >= n ? n : block * kWaveletRRankEvery;
      if (ReadBE16(table + (static_cast<size_t>(block) * 2)) !=
          PopcountBits(bitmap, position)) {
        return false;
      }
    }
  }
  *out = p;
  return true;
}

uint32_t WaveletRRankOne(const WaveletRRegion& p, uint32_t level,
                         uint32_t position) {
  const uint32_t block = position / kWaveletRRankEvery;
  const uint32_t start = block * kWaveletRRankEvery;
  const char* bitmap = p.bitmaps + (static_cast<size_t>(level) * p.stride);
  const char* table = p.ranks + (static_cast<size_t>(level) * p.rank_count * 2);
  uint32_t rank = ReadBE16(table + (static_cast<size_t>(block) * 2));
  const uint32_t offset = position - start;
  const uint32_t words = offset / 64;
  for (uint32_t i = 0; i < words; ++i) {
    rank += static_cast<uint32_t>(__builtin_popcountll(
        ReadBE64(bitmap + (start / 8) + (static_cast<size_t>(i) * 8))));
  }
  const uint32_t bits = offset & 63U;
  if (bits != 0) {
    const char* partial =
        bitmap + (start / 8) + (static_cast<size_t>(words) * 8);
    for (uint32_t i = 0; i < bits; ++i) {
      rank += static_cast<uint8_t>(partial[i >> 3] >> (7U - (i & 7U))) & 1U;
    }
  }
  return rank;
}

uint64_t WaveletRValueAt(const WaveletRRegion& p, uint32_t index) {
  uint64_t value = 0;
  uint32_t position = index;
  for (uint32_t level = 0; level < p.width; ++level) {
    const char* bitmap = p.bitmaps + (static_cast<size_t>(level) * p.stride);
    const uint32_t rank = WaveletRRankOne(p, level, position);
    const uint32_t bit =
        static_cast<uint8_t>(bitmap[position >> 3] >> (7U - (position & 7U))) &
        1U;
    value = (value << 1) | bit;
    if (bit == 0) {
      position -= rank;
    } else {
      position = p.n - WaveletRRankOne(p, level, p.n) + rank;
    }
  }
  return value;
}

bool WaveletRNormalizedRange(const WaveletRRegion& p, PaxChunk::CompareOp op,
                             int64_t literal, uint64_t* low_out,
                             uint64_t* high_out) {
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
  const __int128 minimum = p.minimum;
  const uint64_t domain_max =
      p.width == 64 ? UINT64_MAX : ((1ULL << p.width) - 1ULL);
  const __int128 normalized_low = std::max(low, minimum) - minimum;
  const __int128 normalized_high =
      std::min(high, minimum + static_cast<__int128>(domain_max)) - minimum;
  if (normalized_low > normalized_high) {
    *low_out = 1;
    *high_out = 0;
    return true;
  }
  *low_out = static_cast<uint64_t>(normalized_low);
  *high_out = static_cast<uint64_t>(normalized_high);
  return true;
}

uint64_t WaveletRPrefixCount(const WaveletRRegion& p, uint64_t bound,
                             bool inclusive) {
  uint32_t left = 0;
  uint32_t right = p.n;
  uint64_t count = 0;
  for (uint32_t level = 0; level < p.width && left < right; ++level) {
    const uint32_t shift = p.width - 1 - level;
    const auto bit = static_cast<uint32_t>((bound >> shift) & 1U);
    const uint32_t left_rank = WaveletRRankOne(p, level, left);
    const uint32_t right_rank = WaveletRRankOne(p, level, right);
    const uint32_t zeros = p.n - WaveletRRankOne(p, level, p.n);
    if (bit == 0) {
      left -= left_rank;
      right -= right_rank;
    } else {
      count += right - left - (right_rank - left_rank);
      left = zeros + left_rank;
      right = zeros + right_rank;
    }
  }
  if (inclusive && left < right) {
    count += right - left;
  }
  return count;
}

uint64_t WaveletRSumRange(const WaveletRRegion& p, uint32_t level,
                          uint32_t left, uint32_t right) {
  if (left >= right || level == p.width) {
    return 0;
  }
  const uint32_t left_rank = WaveletRRankOne(p, level, left);
  const uint32_t right_rank = WaveletRRankOne(p, level, right);
  const uint32_t zeros = p.n - WaveletRRankOne(p, level, p.n);
  const uint32_t zero_left = left - left_rank;
  const uint32_t zero_right = right - right_rank;
  const uint32_t one_left = zeros + left_rank;
  const uint32_t one_right = zeros + right_rank;
  const uint32_t shift = p.width - 1 - level;
  const uint64_t weight = 1ULL << shift;
  uint64_t sum = static_cast<uint64_t>(right_rank - left_rank) * weight;
  sum += WaveletRSumRange(p, level + 1, zero_left, zero_right);
  sum += WaveletRSumRange(p, level + 1, one_left, one_right);
  return sum;
}

uint64_t WaveletRRangeSum(const WaveletRRegion& p, uint32_t level,
                          uint32_t left, uint32_t right, uint64_t prefix,
                          uint32_t remaining, uint64_t low, uint64_t high) {
  if (left >= right) {
    return 0;
  }
  if (remaining == 0) {
    return prefix >= low && prefix <= high
               ? static_cast<uint64_t>(right - left) * prefix
               : 0;
  }
  const uint64_t node_max =
      remaining == 64 ? UINT64_MAX : prefix | ((1ULL << remaining) - 1ULL);
  if (node_max < low || prefix > high) {
    return 0;
  }
  if (low <= prefix && node_max <= high) {
    return (static_cast<uint64_t>(right - left) * prefix) +
           WaveletRSumRange(p, level, left, right);
  }
  const uint32_t left_rank = WaveletRRankOne(p, level, left);
  const uint32_t right_rank = WaveletRRankOne(p, level, right);
  const uint32_t zeros = p.n - WaveletRRankOne(p, level, p.n);
  const uint32_t zero_left = left - left_rank;
  const uint32_t zero_right = right - right_rank;
  const uint32_t one_left = zeros + left_rank;
  const uint32_t one_right = zeros + right_rank;
  const uint32_t next_remaining = remaining - 1;
  const uint64_t one_prefix = prefix | (1ULL << next_remaining);
  return WaveletRRangeSum(p, level + 1, zero_left, zero_right, prefix,
                          next_remaining, low, high) +
         WaveletRRangeSum(p, level + 1, one_left, one_right, one_prefix,
                          next_remaining, low, high);
}

bool WaveletRRegionFold(const WaveletRRegion& p, PaxChunk::CompareOp op,
                        int64_t literal, uint64_t* count_out,
                        int64_t* sum_out) {
  uint64_t low = 0;
  uint64_t high = 0;
  if (!WaveletRNormalizedRange(p, op, literal, &low, &high)) {
    return false;
  }
  if (low > high) {
    if (count_out != nullptr) {
      *count_out = 0;
    }
    if (sum_out != nullptr) {
      *sum_out = 0;
    }
    return true;
  }
  uint64_t count = WaveletRPrefixCount(p, high, true);
  if (low != 0) {
    count -= WaveletRPrefixCount(p, low, false);
  }
  uint64_t normalized = 0;
  if (sum_out != nullptr) {
    normalized = WaveletRRangeSum(p, 0, 0, p.n, 0, p.width, low, high);
  }
  if (count_out != nullptr) {
    *count_out = count;
  }
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>((static_cast<uint64_t>(p.minimum) * count) +
                                    normalized);
  }
  return true;
}

__int128 QppModelValue(const QppPage& p, uint32_t index) {
  const __int128 i = index;
  return static_cast<__int128>(p.a) + (i * static_cast<__int128>(p.d1)) +
         ((i * (i - 1) / 2) * static_cast<__int128>(p.d2));
}

bool ParseQppRegion(const char* region, uint32_t n, uint32_t param,
                    const char* end, QppRegion* out) {
  if (region == nullptr || end == nullptr || end < region || n == 0 ||
      n > PaxChunk::kRowGroupSize || region + 2 > end) {
    return false;
  }
  const uint32_t pages = ReadBE16(region);
  const uint32_t want_pages = (n + kQppPageRows - 1) / kQppPageRows;
  if (pages == 0 || pages != param || pages != want_pages) {
    return false;
  }

  const char* cursor = region + 2;
  QppRegion parsed;
  parsed.n = n;
  parsed.pages = pages;
  parsed.end = end;
  for (uint32_t page_index = 0; page_index < pages; ++page_index) {
    if (cursor + 26 > end) {
      return false;
    }
    const uint32_t start = page_index * kQppPageRows;
    const uint32_t rows = std::min<uint32_t>(kQppPageRows, n - start);
    QppPage page;
    page.a = static_cast<int64_t>(ReadBE64(cursor));
    page.d1 = static_cast<int64_t>(ReadBE64(cursor + 8));
    page.d2 = static_cast<int64_t>(ReadBE64(cursor + 16));
    page.exception_count = ReadBE16(cursor + 24);
    page.rows = rows;
    cursor += 26;
    if (page.exception_count > rows) {
      return false;
    }
    const size_t bitmap_bytes = page.exception_count == 0 ? 0 : (rows + 7) / 8;
    if (std::cmp_greater(bitmap_bytes, end - cursor)) {
      return false;
    }
    if (page.exception_count != 0) {
      page.bitmap = reinterpret_cast<const uint8_t*>(cursor);
      page.exceptions = cursor + bitmap_bytes;
      if (page.exceptions + (static_cast<size_t>(page.exception_count) * 10) >
          end) {
        return false;
      }
      uint32_t ones = 0;
      for (uint32_t byte_index = 0; byte_index < bitmap_bytes; ++byte_index) {
        auto byte = static_cast<uint8_t>(page.bitmap[byte_index]);
        if (byte_index + 1 == bitmap_bytes && (rows & 7U) != 0) {
          const auto tail = static_cast<uint8_t>(0xFFU << (8U - (rows & 7U)));
          if ((byte & static_cast<uint8_t>(~tail)) != 0) {
            return false;
          }
          byte = static_cast<uint8_t>(byte & tail);
        }
        ones += static_cast<uint32_t>(
            __builtin_popcount(static_cast<unsigned int>(byte)));
      }
      if (ones != page.exception_count) {
        return false;
      }
      uint32_t previous = 0;
      for (uint32_t k = 0; k < page.exception_count; ++k) {
        const char* entry = page.exceptions + (static_cast<size_t>(k) * 10);
        const uint32_t position = ReadBE16(entry);
        const auto value = static_cast<int64_t>(ReadBE64(entry + 2));
        if (position >= rows || (k != 0 && position <= previous) ||
            (page.bitmap[position >> 3] & (0x80U >> (position & 7U))) == 0 ||
            QppModelValue(page, position) == value) {
          return false;
        }
        previous = position;
      }
      cursor =
          page.exceptions + (static_cast<size_t>(page.exception_count) * 10);
    }
    for (uint32_t i = 0; i < rows; ++i) {
      const __int128 value = QppModelValue(page, i);
      if (value < static_cast<__int128>(INT64_MIN) ||
          value > static_cast<__int128>(INT64_MAX)) {
        return false;
      }
    }
    parsed.page[page_index] = page;
  }
  if (cursor != end) {
    return false;
  }
  *out = parsed;
  return true;
}

int64_t QppValueAt(const QppRegion& region, uint32_t index) {
  if (index >= region.n) {
    return 0;
  }
  const uint32_t page_index = index / kQppPageRows;
  const uint32_t local = index % kQppPageRows;
  const QppPage& page = region.page[page_index];
  if (page.exception_count != 0 &&
      (page.bitmap[local >> 3] & (0x80U >> (local & 7U))) != 0) {
    uint32_t low = 0;
    uint32_t high = page.exception_count;
    while (low < high) {
      const uint32_t middle = low + ((high - low) / 2);
      const uint32_t position =
          ReadBE16(page.exceptions + (static_cast<size_t>(middle) * 10));
      if (position < local) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    if (low < page.exception_count) {
      const char* entry = page.exceptions + (static_cast<size_t>(low) * 10);
      if (ReadBE16(entry) == local) {
        return static_cast<int64_t>(ReadBE64(entry + 2));
      }
    }
    return 0;
  }
  return static_cast<int64_t>(QppModelValue(page, local));
}

bool QppMatch(PaxChunk::CompareOp op, int64_t literal, int64_t value) {
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

size_t QppSegments(const QppPage& page, std::array<QppSegment, 2>& segments) {
  if (page.rows == 0) {
    return 0;
  }
  if (page.rows == 1 || page.d2 == 0) {
    segments[0] =
        QppSegment{.begin = 0, .end = page.rows, .increasing = page.d1 >= 0};
    return 1;
  }
  bool found = false;
  uint32_t turn = page.rows - 1;
  for (uint32_t i = 0; i + 1 < page.rows; ++i) {
    const __int128 delta =
        static_cast<__int128>(page.d1) + (static_cast<__int128>(i) * page.d2);
    if ((page.d2 > 0 && delta >= 0) || (page.d2 < 0 && delta <= 0)) {
      turn = i;
      found = true;
      break;
    }
  }
  if (!found) {
    segments[0] =
        QppSegment{.begin = 0, .end = page.rows, .increasing = page.d2 < 0};
    return 1;
  }
  if (turn == 0) {
    segments[0] =
        QppSegment{.begin = 0, .end = page.rows, .increasing = page.d2 > 0};
    return 1;
  }
  segments[0] = QppSegment{.begin = 0, .end = turn, .increasing = page.d2 < 0};
  segments[1] =
      QppSegment{.begin = turn, .end = page.rows, .increasing = page.d2 > 0};
  return 2;
}

uint32_t QppFirstGe(const QppPage& page, const QppSegment& segment,
                    __int128 target) {
  if (segment.begin >= segment.end) {
    return segment.begin;
  }
  if (!segment.increasing) {
    return QppModelValue(page, segment.begin) >= target ? segment.begin
                                                        : segment.end;
  }
  if (QppModelValue(page, segment.begin) >= target) {
    return segment.begin;
  }
  if (QppModelValue(page, segment.end - 1) < target) {
    return segment.end;
  }
  uint32_t low = segment.begin;
  uint32_t high = segment.end;
  while (low < high) {
    const uint32_t middle = low + ((high - low) / 2);
    if (QppModelValue(page, middle) >= target) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
}

uint32_t QppFirstGt(const QppPage& page, const QppSegment& segment,
                    __int128 target) {
  if (segment.begin >= segment.end) {
    return segment.begin;
  }
  if (!segment.increasing) {
    return QppModelValue(page, segment.begin) > target ? segment.begin
                                                       : segment.end;
  }
  if (QppModelValue(page, segment.begin) > target) {
    return segment.begin;
  }
  if (QppModelValue(page, segment.end - 1) <= target) {
    return segment.end;
  }
  uint32_t low = segment.begin;
  uint32_t high = segment.end;
  while (low < high) {
    const uint32_t middle = low + ((high - low) / 2);
    if (QppModelValue(page, middle) > target) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
}

__int128 QppChoose3(uint32_t value) {
  if (value < 3) {
    return 0;
  }
  const __int128 x = value;
  return x * (x - 1) * (x - 2) / 6;
}

__int128 QppModelSum(const QppPage& page, uint32_t begin, uint32_t end) {
  if (begin >= end) {
    return 0;
  }
  const __int128 count = end - begin;
  const __int128 first = begin;
  const __int128 last = end - 1;
  const __int128 index_sum = count * (first + last) / 2;
  const __int128 combination_sum = QppChoose3(end) - QppChoose3(begin);
  return (count * page.a) + (index_sum * page.d1) + (combination_sum * page.d2);
}

uint32_t QppFirstLe(const QppPage& page, const QppSegment& segment,
                    __int128 target) {
  if (segment.begin >= segment.end) {
    return segment.begin;
  }
  if (!segment.increasing) {
    if (QppModelValue(page, segment.begin) <= target) {
      return segment.begin;
    }
    if (QppModelValue(page, segment.end - 1) > target) {
      return segment.end;
    }
    uint32_t low = segment.begin;
    uint32_t high = segment.end;
    while (low < high) {
      const uint32_t middle = low + ((high - low) / 2);
      if (QppModelValue(page, middle) <= target) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }
    return low;
  }
  return QppFirstGt(page, segment, target);
}

uint32_t QppFirstLt(const QppPage& page, const QppSegment& segment,
                    __int128 target) {
  if (segment.begin >= segment.end) {
    return segment.begin;
  }
  if (!segment.increasing) {
    if (QppModelValue(page, segment.begin) < target) {
      return segment.begin;
    }
    if (QppModelValue(page, segment.end - 1) >= target) {
      return segment.end;
    }
    uint32_t low = segment.begin;
    uint32_t high = segment.end;
    while (low < high) {
      const uint32_t middle = low + ((high - low) / 2);
      if (QppModelValue(page, middle) < target) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }
    return low;
  }
  if (QppModelValue(page, segment.begin) < target) {
    return segment.begin;
  }
  if (QppModelValue(page, segment.end - 1) >= target) {
    return segment.end;
  }
  uint32_t low = segment.begin;
  uint32_t high = segment.end;
  while (low < high) {
    const uint32_t middle = low + ((high - low) / 2);
    if (QppModelValue(page, middle) < target) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
}

size_t QppModelRanges(const QppPage& page, __int128 low, __int128 high,
                      std::array<QppRange, 2>& ranges) {
  std::array<QppSegment, 2> segments{};
  const size_t segment_count = QppSegments(page, segments);
  size_t range_count = 0;
  for (size_t i = 0; i < segment_count; ++i) {
    const QppSegment& segment = segments[i];
    const uint32_t left = segment.increasing ? QppFirstGe(page, segment, low)
                                             : QppFirstLe(page, segment, high);
    const uint32_t right = segment.increasing ? QppFirstGt(page, segment, high)
                                              : QppFirstLt(page, segment, low);
    const uint32_t begin = std::max(left, segment.begin);
    const uint32_t end = std::min(right, segment.end);
    if (begin < end) {
      ranges[range_count++] = QppRange{.begin = begin, .end = end};
    }
  }
  return range_count;
}

void QppPatchExceptionCount(const QppRegion& region, PaxChunk::CompareOp op,
                            int64_t literal, __int128 low, __int128 high,
                            int64_t* count, __int128* sum) {
  for (uint32_t p = 0; p < region.pages; ++p) {
    const QppPage& page = region.page[p];
    for (uint32_t k = 0; k < page.exception_count; ++k) {
      const char* entry = page.exceptions + (static_cast<size_t>(k) * 10);
      const uint32_t local = ReadBE16(entry);
      const auto actual = static_cast<int64_t>(ReadBE64(entry + 2));
      const auto predicted = static_cast<int64_t>(QppModelValue(page, local));
      const bool old_hit = predicted >= low && predicted <= high;
      const bool new_hit = QppMatch(op, literal, actual);
      if (old_hit) {
        --*count;
        *sum -= predicted;
      }
      if (new_hit) {
        ++*count;
        *sum += actual;
      }
    }
  }
}

bool QppScanMatch(int64_t value, ScanOp op, int64_t literal) {
  switch (op) {
    case ScanOp::kEq:
      return value == literal;
    case ScanOp::kLe:
      return value <= literal;
    case ScanOp::kLt:
      return value < literal;
    case ScanOp::kGe:
      return value >= literal;
    case ScanOp::kGt:
      return value > literal;
  }
  return false;
}

void QppSetMaskRange(uint64_t* out, uint32_t begin, uint32_t end) {
  if (begin >= end) {
    return;
  }
  const uint32_t first_word = begin >> 6;
  const uint32_t last_word = (end - 1) >> 6;
  const uint64_t first_mask = ~0ULL << (begin & 63U);
  const uint64_t last_mask = ~0ULL >> (63U - ((end - 1) & 63U));
  if (first_word == last_word) {
    out[first_word] |= first_mask & last_mask;
    return;
  }
  out[first_word] |= first_mask;
  for (uint32_t word = first_word + 1; word < last_word; ++word) {
    out[word] = ~0ULL;
  }
  out[last_word] |= last_mask;
}

__int128 QppRegionSumAll(const QppRegion& region) {
  __int128 sum = 0;
  for (uint32_t p = 0; p < region.pages; ++p) {
    const QppPage& page = region.page[p];
    sum += QppModelSum(page, 0, page.rows);
    for (uint32_t k = 0; k < page.exception_count; ++k) {
      const char* entry = page.exceptions + (static_cast<size_t>(k) * 10);
      const uint32_t local = ReadBE16(entry);
      const auto actual = static_cast<int64_t>(ReadBE64(entry + 2));
      sum += actual - QppModelValue(page, local);
    }
  }
  return sum;
}

void QppFillRangeMask(const QppRegion& region, ScanOp op, int64_t literal,
                      uint64_t* out) {
  __int128 low = INT64_MIN;
  __int128 high = INT64_MAX;
  switch (op) {
    case ScanOp::kEq:
      low = high = literal;
      break;
    case ScanOp::kLe:
      high = literal;
      break;
    case ScanOp::kLt:
      high = static_cast<__int128>(literal) - 1;
      break;
    case ScanOp::kGe:
      low = literal;
      break;
    case ScanOp::kGt:
      low = static_cast<__int128>(literal) + 1;
      break;
    default:
      return;
  }
  for (uint32_t p = 0; p < region.pages; ++p) {
    const QppPage& page = region.page[p];
    std::array<QppRange, 2> ranges{};
    const size_t range_count = QppModelRanges(page, low, high, ranges);
    for (size_t i = 0; i < range_count; ++i) {
      QppSetMaskRange(out, (p * kQppPageRows) + ranges[i].begin,
                      (p * kQppPageRows) + ranges[i].end);
    }
  }
  for (uint32_t p = 0; p < region.pages; ++p) {
    const QppPage& page = region.page[p];
    for (uint32_t k = 0; k < page.exception_count; ++k) {
      const char* entry = page.exceptions + (static_cast<size_t>(k) * 10);
      const uint32_t local = ReadBE16(entry);
      const auto actual = static_cast<int64_t>(ReadBE64(entry + 2));
      const auto predicted = static_cast<int64_t>(QppModelValue(page, local));
      const bool old_hit = predicted >= low && predicted <= high;
      const bool new_hit = QppScanMatch(actual, op, literal);
      if (old_hit == new_hit) {
        continue;
      }
      const uint32_t row = (p * kQppPageRows) + local;
      if (new_hit) {
        out[row >> 6] |= 1ULL << (row & 63U);
      } else {
        out[row >> 6] &= ~(1ULL << (row & 63U));
      }
    }
  }
}

void DecodeQppAll(const QppRegion& region, int64_t* out) {
  for (uint32_t i = 0; i < region.n; ++i) {
    out[i] = QppValueAt(region, i);
  }
}

// Bit-writer/reader used by the Gorilla XOR-stream. Bits are appended/consumed
// MSB-first so the decoder reconstructs values identically without any length
// prefix (the region row count n bounds the stream).

// Windowed MSB-first bit reader. Tracks the stream position as an absolute
// bit offset; every field is extracted with one shift+mask from a big-endian
// 8-byte window load (overlapping loads hit L1 and cost far less than the
// old byte-at-a-time refill/serve loop, which dominated Gorilla/Chimp/BitWeave/
// PFor/EliasFano/DICT decode profiles). Windows that would read past `end` or
// straddle the 64-bit extract fall back to a byte-at-a-time path whose
// truncation semantics are bit-identical to the old reader (partial fields
// return their collected bits low-aligned, zero-fill beyond physical end).
// ---- kHuffDict (scheme 51): canonical Huffman over order-preserving dict
// codes ------------------------------------------------------------
//
// Layout (all big-endian, param == d):
//   [d u16][dict d x 8B, ascending][lens ceil(d/2) B, nibble per symbol]
//   [freq d x u16][ncp u16][ckpt ncp x u32 bit offsets][stream_len u32]
//   [stream stream_len B, canonical MSB-first codes in row order]

// ---- kPostings (scheme 63) shared bound ------------------------------------
//
// The encoder emits d <= 64 lists; the parse cap keeps headroom while bounding
// the per-probe candidate scratch (Int64RegionAt's cand table) and every
// d-sized table walk. ValidateWalk's case 63 pins the same bound so a region
// the walk blesses is never one the parse declines.

uint32_t HuffDictLenAtRaw(const uint8_t* lens, uint32_t s) {
  const uint8_t byte = lens[s >> 1];
  return ((s & 1) != 0) ? (byte & 0x0FU) : static_cast<uint32_t>(byte >> 4);
}

uint32_t HuffDictLenAt(const HuffDictRegion& r, uint32_t s) {
  return HuffDictLenAtRaw(r.lens, s);
}

int64_t HuffDictDictAt(const HuffDictRegion& r, uint32_t s) {
  return static_cast<int64_t>(
      ReadBE64(reinterpret_cast<const char*>(r.dict) +
               (static_cast<size_t>(s) * sizeof(int64_t))));
}

uint32_t HuffDictFreqAt(const HuffDictRegion& r, uint32_t s) {
  return ReadBE16(reinterpret_cast<const char*>(r.freq) +
                  (static_cast<size_t>(s) * 2));
}

// Canonical decode tables rebuilt from the nibble lengths: inflate's
// per-length counts plus a (len, symbol)-sorted symbol list, and an 8-bit
// peek LUT for the short-code fast path (steep skew puts the hot symbols at
// 1..8 bits, so most decodes are one table lookup; longer codes rewind and
// take the inflate walk). d <= 1024 keeps the rebuild ~a microsecond, so
// probes pay it per call rather than caching per chunk.

void HuffDictBuildTables(const HuffDictRegion& r, HuffDictTables* t) {
  for (uint32_t len = 0; len <= kHuffDictMaxLen; ++len) {
    t->count[len] = 0;
    t->offset[len] = 0;
  }
  for (uint32_t s = 0; s < r.d; ++s) {
    ++t->count[HuffDictLenAt(r, s)];
  }
  uint16_t running = 0;
  for (uint32_t len = 1; len <= kHuffDictMaxLen; ++len) {
    t->offset[len] = running;
    running = static_cast<uint16_t>(running + t->count[len]);
  }
  std::array<uint16_t, kHuffDictMaxLen + 1> fill{};
  for (uint32_t len = 1; len <= kHuffDictMaxLen; ++len) {
    fill[len] = t->offset[len];
  }
  for (uint32_t s = 0; s < r.d; ++s) {
    t->sym_sorted[fill[HuffDictLenAt(r, s)]++] = static_cast<uint16_t>(s);
  }
  // Canonical code assignment (RFC 1951 3.2.2) replayed to populate the LUT.
  std::array<uint32_t, kHuffDictMaxLen + 1> bl_count{};
  for (uint32_t len = 1; len <= kHuffDictMaxLen; ++len) {
    bl_count[len] = t->count[len];
  }
  std::array<uint32_t, kHuffDictMaxLen + 1> next_code{};
  uint32_t code = 0;
  for (uint32_t bits = 1; bits <= kHuffDictMaxLen; ++bits) {
    code = (code + bl_count[bits - 1]) << 1;
    next_code[bits] = code;
  }
  for (uint32_t len = 1; len <= 8; ++len) {
    for (uint32_t i = 0; i < t->count[len]; ++i) {
      const uint32_t sym = t->sym_sorted[t->offset[len] + i];
      const uint32_t c = next_code[len]++;
      const uint32_t base = c << (8 - len);
      for (uint32_t e = base; e < base + (1U << (8 - len)); ++e) {
        t->lut_len[e] = static_cast<uint8_t>(len);
        t->lut_sym[e] = static_cast<uint16_t>(sym);
      }
    }
  }
}

// One canonical symbol. The table is Kraft-complete (parse-gated), so every
// bit pattern resolves within <= kHuffDictMaxLen steps and the trailing
// return-0 is hostile-input-only.
uint32_t HuffDictDecodeSym(const HuffDictTables& t, WordBitReader& br) {
  const uint64_t pos = br.bitpos;
  uint64_t peek = br.get(8);
  // Zero-pad the window when the stream ends mid-peek: get_slow returns the
  // collected bits WITHOUT shifting in the missing zero padding, and the LUT
  // indexes the code's true 8-bit projection.
  if (br.bitpos < pos + 8) {
    peek <<= (pos + 8) - br.bitpos;
  }
  if (t.lut_len[peek] != 0) {
    br.bitpos = pos + t.lut_len[peek];  // net consumption = the code length
    return t.lut_sym[peek];
  }
  br.bitpos = pos;  // rewind: ambiguous prefix, take the walk
  uint32_t code = 0;
  uint32_t first = 0;
  uint32_t index = 0;
  for (uint32_t len = 1; len <= kHuffDictMaxLen; ++len) {
    code = (code << 1) | static_cast<uint32_t>(br.get1());
    const uint32_t count = t.count[len];
    if (code < first + count) {
      return t.sym_sorted[index + (code - first)];
    }
    index += count;
    first = (first + count) << 1;
  }
  return 0;
}

// Bounds-checked structural parse, mirroring ValidateWalk's case 51: header
// echo (d == param), in-range nibble lengths with an exactly complete Kraft
// sum, checkpoint monotonicity inside the stream, and frequencies in
// [1, n] summing to n. The freq-vs-stream histogram match is the validator's
// one-time job; this parse only proves no reader can walk out of the cell.
// `end` may be null for callers that validated the cell already (the
// ByteSliceNullAwareCount precondition): extent checks pass and the
// structural checks still run.
bool ParseHuffDictRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, HuffDictRegion* out) {
  if (r == nullptr || out == nullptr || (end != nullptr && end < r) ||
      (end != nullptr && end - r < 4) || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const uint32_t d = ReadBE16(r);
  if (d == 0 || d != param || d > kHuffDictMaxCodes) {
    return false;
  }
  const uint64_t ncp_want =
      static_cast<uint64_t>(n + kHuffDictCheckpoint - 1) / kHuffDictCheckpoint;
  const uint64_t lens_off = 2 + (8ULL * d);
  const uint64_t freq_off = lens_off + ((d + 1) / 2);
  const uint64_t ncp_off = freq_off + (2ULL * d);
  const uint64_t ckpt_off = ncp_off + 2;
  const uint64_t slen_off = ckpt_off + (4ULL * ncp_want);
  const uint64_t have =
      (end != nullptr) ? static_cast<uint64_t>(end - r) : UINT64_MAX;
  if (slen_off + 4 > have) {
    return false;
  }
  const uint32_t ncp = ReadBE16(r + ncp_off);
  if (ncp == 0 || ncp != ncp_want) {
    return false;
  }
  const uint32_t stream_len = ReadBE32(r + slen_off);
  // n >= 1 rows always emit >= 1 bit.
  if (stream_len == 0 || slen_off + 4 + stream_len > have) {
    return false;
  }
  const auto* base = reinterpret_cast<const uint8_t*>(r);
  const uint8_t* lens = base + lens_off;
  uint64_t kraft = 0;
  for (uint32_t s = 0; s < d; ++s) {
    const uint32_t len = HuffDictLenAtRaw(lens, s);
    if (len < 1 || len > kHuffDictMaxLen) {
      return false;
    }
    kraft += UINT64_C(1) << (kHuffDictMaxLen - len);
  }
  if (kraft != (UINT64_C(1) << kHuffDictMaxLen)) {
    return false;
  }
  const uint8_t* ckpt = base + ckpt_off;
  uint64_t prev = 0;
  for (uint32_t k = 0; k < ncp; ++k) {
    const uint64_t bit = ReadBE32(reinterpret_cast<const char*>(ckpt) +
                                  (static_cast<size_t>(k) * 4));
    if (k == 0 && bit != 0) {
      return false;
    }
    if (bit < prev || bit >= static_cast<uint64_t>(stream_len) * 8) {
      return false;
    }
    prev = bit;
  }
  const uint8_t* freq = base + freq_off;
  uint64_t fsum = 0;
  for (uint32_t s = 0; s < d; ++s) {
    const uint32_t f = ReadBE16(reinterpret_cast<const char*>(freq) +
                                (static_cast<size_t>(s) * 2));
    if (f == 0 || f > n) {
      return false;
    }
    fsum += f;
  }
  if (fsum != n) {
    return false;
  }
  out->d = d;
  out->n = n;
  out->ncp = ncp;
  out->stream_len = stream_len;
  out->dict = base + 2;
  out->lens = lens;
  out->freq = freq;
  out->ckpt = ckpt;
  out->stream = base + slen_off + 4;
  return true;
}

// Value `i`: seek the row's checkpoint, replay at most kHuffDictCheckpoint
// symbols, take the last. The complete tree decodes every bit pattern to a
// valid symbol, so a truncated hostile stream still stays in-bounds.
int64_t HuffDictValueAt(const HuffDictRegion& v, uint32_t i) {
  if (i >= v.n) {
    return 0;
  }
  HuffDictTables t;
  HuffDictBuildTables(v, &t);
  WordBitReader br(reinterpret_cast<const char*>(v.stream),
                   reinterpret_cast<const char*>(v.stream + v.stream_len));
  br.bitpos = ReadBE32(reinterpret_cast<const char*>(v.ckpt) +
                       (static_cast<size_t>(i / kHuffDictCheckpoint) * 4));
  const uint32_t replay = (i % kHuffDictCheckpoint) + 1;
  uint32_t sym = 0;
  for (uint32_t k = 0; k < replay; ++k) {
    sym = HuffDictDecodeSym(t, br);
  }
  if (sym >= v.d) {
    return 0;  // unreachable on a Kraft-complete table
  }
  return HuffDictDictAt(v, sym);
}

// Whole-cell decode: one forward pass from bit 0 (the checkpoints exist for
// the random probe and are not consulted here).
void DecodeHuffDictAll(const HuffDictRegion& v, int64_t* out) {
  HuffDictTables t;
  HuffDictBuildTables(v, &t);
  WordBitReader br(reinterpret_cast<const char*>(v.stream),
                   reinterpret_cast<const char*>(v.stream + v.stream_len));
  for (uint32_t i = 0; i < v.n; ++i) {
    const uint32_t sym = HuffDictDecodeSym(t, br);
    out[i] = sym < v.d ? HuffDictDictAt(v, sym) : 0;
  }
}

// ---- kS4 (scheme 55): per-block frame of reference on first differences ----
// S4's integer transform (Simonson, Suzuki, Absebassi & Merrill, SIGMOD 2014):
// first differences, then a frame of reference and a bit width PER BLOCK
// rather than per region, plus a per-block seek table.
//
// The per-block framing is the entire point, and it is worth being precise
// about what it buys, because the obvious version of this idea does not work.
// A single region-wide width is dominated by its worst block: on a 1024-row
// column that is piecewise linear with a slope kink every 64 rows, one shared
// width measures 712 bytes where per-block widths measure 401. The variant
// this file first tried -- a RUNNING-MAX predictor with a per-block width --
// is worse still and was measured before being replaced: 1433 bytes on that
// same column, and it lost to plain kDeltaFp on every shape measured, because
// the distance from a running max accumulates the whole post-dip climb back to
// the high-water mark while the first difference does not. Per-block FOR on
// the first difference is what actually wins, and it is also what S4
// specifies.
//
// The second thing it buys is that it does not require monotone input.
// kDeltaFp bails out of the encoding entirely on a single negative delta
// (`if (static_cast<int64_t>(d) < 0) return {}`); a per-block base absorbs
// dips, because a block that steps down frames against its own minimum.
//
// The third is the seek interval. A point lookup jumps to a 64-row block and
// replays at most 63 deltas, against kDeltaFp's kDeltaCheckpointEvery (128) and
// kHuffDict's 64. It is not free and not O(1) -- see S4ValueAt for why the
// delta transform rules that out -- but the block is half kDeltaFp's, and the
// per-block width means the replay never has to re-derive a frame.

// Header: 8B base + 1B nblk + nblk width bytes + nblk 8B bases + (nblk-1) 8B
// seek words. At nblk == 16 (a full 1024-row group) that is 273 bytes, about
// 2.1 bits/row -- the price of O(1) seek plus a per-block frame, and the
// reason kDeltaFp still wins on a clean ramp (328 vs 409 bytes measured).
uint64_t S4HeaderBytes(uint32_t n) {
  const uint64_t nblk = (n + kS4Block - 1) / kS4Block;
  return 9 + nblk + (8 * nblk) + (8 * (nblk - 1));
}

uint32_t S4BlockRows(const S4Region& p, uint32_t b) {
  const uint64_t start = static_cast<uint64_t>(b) * kS4Block;
  return start >= p.n
             ? 0
             : static_cast<uint32_t>(std::min<uint64_t>(kS4Block, p.n - start));
}

int S4Width(const S4Region& p, uint32_t b) {
  return static_cast<int>(static_cast<uint8_t>(p.widths[b]));
}

int64_t S4Base(const S4Region& p, uint32_t b) {
  return static_cast<int64_t>(ReadBE64(p.lows + (8 * static_cast<size_t>(b))));
}

// Byte offset of block b's payload. Blocks are byte-aligned, so this is pure
// integer arithmetic over the width table -- no bit accumulator, no value
// decode, at most kS4Block-count steps.
uint64_t S4BlockOffset(const S4Region& p, uint32_t b) {
  uint64_t off = 0;
  for (uint32_t j = 0; j < b; ++j) {
    off += (static_cast<uint64_t>(S4BlockRows(p, j)) *
                static_cast<uint64_t>(S4Width(p, j)) +
            7) /
           8;
  }
  return off;
}

uint64_t S4Extent(const S4Region& p) {
  return S4HeaderBytes(p.n) + S4BlockOffset(p, p.nblk);
}

// The seek value for the block containing row i: v[0] for block 0 (whose first
// delta is 0), else the stored value of the row BEFORE the block, which is the
// last row of the previous block. A replay starts here and sums forward.
int64_t S4Checkpoint(const S4Region& p, uint32_t i) {
  const uint32_t b = i / kS4Block;
  return b == 0 ? p.base
                : static_cast<int64_t>(
                      ReadBE64(p.ckpt + (8 * static_cast<size_t>(b - 1))));
}

// Bounds-checked header parse. Mirrors ValidateWalk's case 55 and the WASM
// s4_parse: nblk must equal the value DERIVED from n, every width must be
// expressible, and the whole extent must fit before any payload is touched.
bool ParseS4Region(const char* r, uint32_t n, uint32_t param, const char* end,
                   S4Region* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const uint32_t nblk = (n + kS4Block - 1) / kS4Block;
  if (S4HeaderBytes(n) > static_cast<uint64_t>(end - r)) {
    return false;
  }
  if (static_cast<uint32_t>(static_cast<uint8_t>(r[8])) != nblk) {
    return false;
  }
  S4Region& p = *out;
  p.n = n;
  p.nblk = nblk;
  p.base = static_cast<int64_t>(ReadBE64(r));
  p.widths = r + 9;
  p.lows = p.widths + nblk;
  p.ckpt = p.lows + (8 * static_cast<size_t>(nblk));
  p.payload = r + S4HeaderBytes(n);
  for (uint32_t b = 0; b < nblk; ++b) {
    const int w = static_cast<uint8_t>(p.widths[b]);
    if (w < 1 || w > 64) {
      return false;
    }
  }
  if (S4Extent(p) > static_cast<uint64_t>(end - r)) {
    return false;
  }
  // No single width parameterizes this scheme (the widths are per block and
  // live in the region), so the footer param is pinned to 0 rather than left
  // to carry a value no reader would agree on.
  return param == 0;
}

// Row `i`: seek the block's word, then replay at most kS4Block-1 deltas.
//
// NOT O(1), and the wire format is the reason. The stored words are deltas
// from the PREVIOUS ROW, which is what makes them narrow, so a block's seek
// word (the value of the row before the block) does not pin any row inside the
// block -- the deltas from the block's first row onwards still have to be
// summed. An earlier version of this scheme stored offsets from the block's
// first row instead, which would have been O(1), and was measured and dropped:
// it costs 1297 bytes on a clean ramp where this form costs 409, because a
// within-block delta shrinks with the step size while an offset from the block
// head does not. The bounded 63-row replay is the price paid for the size.
int64_t S4ValueAt(const S4Region& p, uint32_t i) {
  if (i >= p.n) {
    return 0;
  }
  const uint32_t b = i / kS4Block;
  const int w = S4Width(p, b);
  const int64_t lo = S4Base(p, b);
  const uint64_t boff = S4BlockOffset(p, b) * 8;
  // The running sum is done in UNSIGNED arithmetic and reinterpreted, because
  // a delta stream that visits both int64 extremes overflows a signed
  // accumulator (UBSan caught exactly that on the {INT64_MIN, ..., INT64_MAX}
  // shape). Every other cumulative scheme in this file does the same: the sum
  // is a fixed point of a mod-2^64 walk, so wrapping is the DEFINED behaviour
  // and two's-complement round-trips bit for bit.
  auto v = static_cast<uint64_t>(S4Checkpoint(p, i));
  const auto ulo = static_cast<uint64_t>(lo);
  for (uint32_t t = 0; t <= i % kS4Block; ++t) {
    v += ulo + UnpackBitAt(
                   p.payload,
                   boff + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w)),
                   w);
  }
  return static_cast<int64_t>(v);
}

// ---- SFOR (scheme 57): sorted NON-NEGATIVE doubles as a prefix-group FOR.
// See the enum comment in pax_chunk.hpp for the layout and the gates; the
// helpers below mirror the kS4 block (bounds-checked parse pinning everything
// a reader derives, O(log ng) row lookup, per-group byte-aligned payloads).

uint64_t SforMsb(const SforRegion& p, uint32_t g) {
  return ReadBE64(p.dir + (static_cast<size_t>(g) * kSforDirEntry));
}

uint64_t SforBase(const SforRegion& p, uint32_t g) {
  return ReadBE64(p.dir + (static_cast<size_t>(g) * kSforDirEntry) + 8);
}

uint32_t SforRowStart(const SforRegion& p, uint32_t g) {
  return ReadBE16(p.dir + (static_cast<size_t>(g) * kSforDirEntry) + 16);
}

uint32_t SforWidth(const SforRegion& p, uint32_t g) {
  return static_cast<uint32_t>(static_cast<uint8_t>(
      p.dir[(static_cast<size_t>(g) * kSforDirEntry) + 18]));
}

// Rows in group g: row_start[g+1] - row_start[g], n for the last group.
uint32_t SforGroupRows(const SforRegion& p, uint32_t g) {
  return g + 1 == p.ng ? p.n - SforRowStart(p, g)
                       : SforRowStart(p, g + 1) - SforRowStart(p, g);
}

// Byte offset of group g's payload. Groups are byte-aligned, so this is pure
// integer arithmetic over the directory -- at most ng-1 steps, no bit state.
uint64_t SforGroupOffset(const SforRegion& p, uint32_t g) {
  uint64_t off = 0;
  for (uint32_t j = 0; j < g; ++j) {
    off += (static_cast<uint64_t>(SforGroupRows(p, j)) *
                static_cast<uint64_t>(SforWidth(p, j)) +
            7) /
           8;
  }
  return off;
}

// Bounds-checked parse. Mirrors ValidateWalk's case 57: the footer param IS
// the split s (kFreq-style echo), row_start must tile [0, n) with no empty
// group, every base must fit under 2^s, and the whole extent must fit before
// any payload is touched.
bool ParseSforRegion(const char* r, uint32_t n, uint32_t param, const char* end,
                     SforRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  if (static_cast<uint64_t>(end - r) < 3) {
    return false;
  }
  const auto s = static_cast<uint32_t>(static_cast<uint8_t>(r[0]));
  const uint32_t ng = ReadBE16(r + 1);
  if (s < 1 || s > 63 || param != s) {
    return false;
  }
  if (ng == 0 || ng > n) {
    return false;
  }
  if (3 + (kSforDirEntry * ng) > static_cast<uint64_t>(end - r)) {
    return false;
  }
  SforRegion& p = *out;
  p.n = n;
  p.s = s;
  p.ng = ng;
  p.dir = r + 3;
  p.end = end;
  p.payload = p.dir + (kSforDirEntry * ng);
  const uint64_t mask = (UINT64_C(1) << s) - 1;
  // One pass pins the directory AND accumulates the payload extent (calling
  // SforExtent here would be a second O(ng) sweep on every parse; the
  // predicate kernel parses per call, so the constant matters).
  uint64_t payload_bytes = 0;
  for (uint32_t g = 0; g < ng; ++g) {
    const uint32_t w = SforWidth(p, g);
    if (w > s) {
      return false;
    }
    if (SforBase(p, g) > mask) {
      return false;
    }
    const uint32_t rs = SforRowStart(p, g);
    if (g == 0) {
      if (rs != 0) {
        return false;
      }
    } else {
      if (rs <= SforRowStart(p, g - 1)) {
        return false;
      }
    }
    payload_bytes += (static_cast<uint64_t>(SforGroupRows(p, g)) * w + 7) / 8;
  }
  if (SforRowStart(p, ng - 1) >= n) {
    return false;
  }
  if (3 + (kSforDirEntry * ng) + payload_bytes >
      static_cast<uint64_t>(end - r)) {
    return false;
  }
  return true;
}

uint64_t SforDeltaAt(const SforRegion& p, uint32_t g, uint32_t t,
                     uint64_t boff) {
  const uint32_t w = SforWidth(p, g);
  if (w == 0) {
    return 0;
  }
  const uint64_t bits =
      (boff * 8) + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w));
  if (p.end != nullptr &&
      bits / 8 >= static_cast<uint64_t>(p.end - p.payload)) {
    return 0;
  }
  return UnpackBitAt(p.payload, bits, static_cast<int>(w));
}

// Within group g the values are ASCENDING (equal msb, ascending lsbits), so a
// predicate window resolves inside a boundary group by bisect over the deltas
// into ONE contiguous passing run -- the kernel never walks rows. Probes are
// O(log rows) unpacks; `boff` is the group's payload byte offset.
uint32_t SforGroupLowerBound(const SforRegion& p, uint32_t g, uint64_t boff,
                             __int128 x) {
  const uint32_t rows = SforGroupRows(p, g);
  const uint64_t head = (SforMsb(p, g) << p.s) | SforBase(p, g);
  uint32_t lo = 0, hi = rows;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    const __int128 v = static_cast<__int128>(head) +
                       static_cast<__int128>(SforDeltaAt(p, g, mid, boff));
    if (v < x) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// Row `i`: bisect the row_start table (O(log ng)), then unpack one delta.
int64_t SforValueAt(const SforRegion& p, uint32_t i) {
  if (i >= p.n) {
    return 0;
  }
  uint32_t lo = 0, hi = p.ng;
  while (lo + 1 < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (SforRowStart(p, mid) <= i) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  const uint64_t boff = SforGroupOffset(p, lo);
  const uint64_t v =
      (SforMsb(p, lo) << p.s) |
      (SforBase(p, lo) + SforDeltaAt(p, lo, i - SforRowStart(p, lo), boff));
  return static_cast<int64_t>(v);
}

// Whole-cell decode: groups in order, deltas unpacked sequentially inside a
// group (no per-row directory re-derivation).
void DecodeSforAll(const SforRegion& p, int64_t* out) {
  uint64_t boff = 0;
  for (uint32_t g = 0; g < p.ng; ++g) {
    const uint32_t rs = SforRowStart(p, g);
    const uint32_t rows = SforGroupRows(p, g);
    const uint64_t head = (SforMsb(p, g) << p.s) | SforBase(p, g);
    const uint32_t w = SforWidth(p, g);
    for (uint32_t t = 0; t < rows; ++t) {
      const uint64_t delta =
          w == 0 ? 0
                 : UnpackBitAt(p.payload,
                               (boff * 8) + (static_cast<uint64_t>(t) * w),
                               static_cast<int>(w));
      out[rs + t] = static_cast<int64_t>(head + delta);
    }
    boff += (static_cast<uint64_t>(rows) * w + 7) / 8;
  }
}

// Whole-cell decode: one forward pass. The seek words are not consulted (as in
// DecodeHuffDictAll), but ValidateWalk's replay requires each of them to equal
// the running value, so a scan and a point read always agree.
void DecodeS4All(const S4Region& p, int64_t* out) {
  // ACCUMULATE. The stored words are deltas from the PREVIOUS ROW, so the
  // running value is the accumulator and it must carry ACROSS rows and blocks,
  // not restart at each block's seek word. (An earlier version read the block
  // anchor once and added lo+word to it for every row in the block, which
  // silently gave every row of a block the SAME value -- 0,1,1,1,... on a
  // column of 0,1,2,3. The point-access path accumulates correctly, so a
  // per-value probe never saw it; the native/WASM parity run did, which is
  // what a differential check is for.)
  auto v = static_cast<uint64_t>(p.base);  // unsigned: see S4ValueAt
  for (uint32_t b = 0; b < p.nblk; ++b) {
    const int w = S4Width(p, b);
    const auto ulo = static_cast<uint64_t>(S4Base(p, b));
    const uint64_t boff = S4BlockOffset(p, b) * 8;
    const uint32_t rows = S4BlockRows(p, b);
    for (uint32_t t = 0; t < rows; ++t) {
      v += ulo +
           UnpackBitAt(
               p.payload,
               boff + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w)), w);
      out[(b * kS4Block) + t] = static_cast<int64_t>(v);
    }
  }
}

// ---- EXPFOR (scheme 60): ANY finite doubles as a sign+exponent grouped FOR.
// See the enum comment in pax_chunk.hpp for the layout and the gates; the
// helpers below mirror the SforRegion block (bounds-checked parse pinning
// everything a reader derives, O(log ng) row lookup, per-group byte-aligned
// payloads) with the IEEE total-order transform folded into every
// reconstruction.
// The encoder's structure gate: bands alternating faster than this burn
// more directory than payload and degrade the linear fold (see
// EncExpForF64). 64 = ~1KB worst-case directory and a bounded fold scan;
// the measured call points sit at 8 (clustered bands) to 33 (decimal
// sawtooth), while oscillating noise lands at 83+ and declines.

// The IEEE-754 total-order map: key order == value order for ALL finite
// doubles (negatives land below 2^63, positives above). Inverse is
// ExpForKeyToBits.
uint64_t ExpForBitsToKey(uint64_t bits) {
  return bits ^
         ((bits >> 63) != 0 ? ~UINT64_C(0) : UINT64_C(0x8000000000000000));
}

uint64_t ExpForKeyToBits(uint64_t key) {
  return key ^ ((key >> 63) != 0 ? UINT64_C(0x8000000000000000) : ~UINT64_C(0));
}

uint64_t ExpForGKey(const ExpForRegion& p, uint32_t g) {
  return ReadBE16(p.dir + (static_cast<size_t>(g) * kExpForDirEntry));
}

uint64_t ExpForBase(const ExpForRegion& p, uint32_t g) {
  return ReadBE64(p.dir + (static_cast<size_t>(g) * kExpForDirEntry) + 2);
}

uint32_t ExpForRowStart(const ExpForRegion& p, uint32_t g) {
  return ReadBE16(p.dir + (static_cast<size_t>(g) * kExpForDirEntry) + 10);
}

uint32_t ExpForWidth(const ExpForRegion& p, uint32_t g) {
  return static_cast<uint32_t>(static_cast<uint8_t>(
      p.dir[(static_cast<size_t>(g) * kExpForDirEntry) + 12]));
}

// Rows in group g: row_start[g+1] - row_start[g], n for the last group.
uint32_t ExpForGroupRows(const ExpForRegion& p, uint32_t g) {
  return g + 1 == p.ng ? p.n - ExpForRowStart(p, g)
                       : ExpForRowStart(p, g + 1) - ExpForRowStart(p, g);
}

// Byte offset of group g's payload (groups are byte-aligned; pure integer
// arithmetic over the directory, at most ng-1 steps).
uint64_t ExpForGroupOffset(const ExpForRegion& p, uint32_t g) {
  uint64_t off = 0;
  for (uint32_t j = 0; j < g; ++j) {
    off += (static_cast<uint64_t>(ExpForGroupRows(p, j)) *
                static_cast<uint64_t>(ExpForWidth(p, j)) +
            7) /
           8;
  }
  return off;
}

// Bounds-checked parse. Mirrors ValidateWalk's case 60: param is pinned to 0
// (the split is structural), every gkey must be a finite exponent band (the
// directory is IN ROW ORDER -- unsorted columns make the bands interleave),
// row_start must tile [0, n) with no empty run, every base must fit under
// 2^52, every width under 53, and the whole extent must fit before any
// payload is touched.
bool ParseExpForRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, ExpForRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  if (static_cast<uint64_t>(end - r) < 2) {
    return false;
  }
  const uint32_t ng = ReadBE16(r);
  if (param != 0) {
    return false;
  }
  if (ng == 0 || ng > n) {
    return false;
  }
  if (2 + (kExpForDirEntry * ng) > static_cast<uint64_t>(end - r)) {
    return false;
  }
  ExpForRegion& p = *out;
  p.n = n;
  p.ng = ng;
  p.dir = r + 2;
  p.end = end;
  p.payload = p.dir + (kExpForDirEntry * ng);
  // One pass pins the directory AND accumulates the payload extent (the same
  // single-sweep shape as ParseSforRegion: the predicate kernel parses per
  // call, so the constant matters).
  uint64_t payload_bytes = 0;
  for (uint32_t g = 0; g < ng; ++g) {
    const uint64_t gkey = ExpForGKey(p, g);
    const uint32_t w = ExpForWidth(p, g);
    if (gkey > UINT64_C(0xffe)) {
      return false;  // 0xfff = exponent 0x7ff: NaN/Inf
    }
    if (ExpForBase(p, g) > kExpForMantMask) {
      return false;
    }
    if (w > 52) {
      return false;
    }
    const uint32_t rs = ExpForRowStart(p, g);
    if (g == 0) {
      if (rs != 0) {
        return false;
      }
    } else {
      if (rs <= ExpForRowStart(p, g - 1)) {
        return false;
      }
    }
    payload_bytes += (static_cast<uint64_t>(ExpForGroupRows(p, g)) * w + 7) / 8;
  }
  if (ExpForRowStart(p, ng - 1) >= n) {
    return false;
  }
  if (2 + (kExpForDirEntry * ng) + payload_bytes >
      static_cast<uint64_t>(end - r)) {
    return false;
  }
  return true;
}

uint64_t ExpForOffsetAt(const ExpForRegion& p, uint32_t g, uint32_t t,
                        uint64_t boff) {
  const uint32_t w = ExpForWidth(p, g);
  if (w == 0) {
    return 0;
  }
  const uint64_t bits =
      (boff * 8) + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w));
  if (p.end != nullptr &&
      bits / 8 >= static_cast<uint64_t>(p.end - p.payload)) {
    return 0;
  }
  return UnpackBitAt(p.payload, bits, static_cast<int>(w));
}

// Row `i`: bisect the row_start table (O(log ng)), unpack one offset, and
// invert the total-order transform -- the returned cell is the RAW bitcast
// bits, exactly what every other double codec hands the int64 machinery.
int64_t ExpForValueAt(const ExpForRegion& p, uint32_t i) {
  if (i >= p.n) {
    return 0;
  }
  uint32_t lo = 0, hi = p.ng;
  while (lo + 1 < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (ExpForRowStart(p, mid) <= i) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  const uint64_t boff = ExpForGroupOffset(p, lo);
  const uint64_t key = (ExpForGKey(p, lo) << 52) |
                       (ExpForBase(p, lo) +
                        ExpForOffsetAt(p, lo, i - ExpForRowStart(p, lo), boff));
  return static_cast<int64_t>(ExpForKeyToBits(key));
}

// Whole-cell decode: groups in order, offsets unpacked sequentially inside a
// group (no per-row directory re-derivation).
void DecodeExpForAll(const ExpForRegion& p, int64_t* out) {
  uint64_t boff = 0;
  for (uint32_t g = 0; g < p.ng; ++g) {
    const uint32_t rs = ExpForRowStart(p, g);
    const uint32_t rows = ExpForGroupRows(p, g);
    const uint64_t head = (ExpForGKey(p, g) << 52) | ExpForBase(p, g);
    const uint32_t w = ExpForWidth(p, g);
    for (uint32_t t = 0; t < rows; ++t) {
      const uint64_t offset =
          w == 0 ? 0
                 : UnpackBitAt(p.payload,
                               (boff * 8) + (static_cast<uint64_t>(t) * w),
                               static_cast<int>(w));
      out[rs + t] = static_cast<int64_t>(ExpForKeyToBits(head + offset));
    }
    boff += (static_cast<uint64_t>(rows) * w + 7) / 8;
  }
}

// ---- SORT-DICT (scheme 61): sorted-view permutation layout -----------------
//
// Values are re-sorted WITHIN the row group and stored as a 64-row-block delta
// stream (the kS4 wire discipline: per-block width + byte-aligned payload); a
// ceil(log2 n)-bit rank plane maps row -> sorted index, so the row order is
// one gather away. The block firsts are an ascending bracket table and the
// psum table carries the wrapped prefix sums of the sorted values -- that is
// what the compressed-domain kernels are built on: an EQ/RANGE predicate
// resolves to ONE sorted window by bracket bisect + a <= 63-gap walk, an
// unpredicated SUM is the single total word, and MIN/MAX are the first/last
// brackets.

// Header: 2 + nb * 9 + (nb + 1) * 8. At nb == 16 (a full row group) that is
// 2 + 144 + 136 = 282 bytes -- the price of the bracket table, the prefix-sum
// table and the rank width.
uint64_t SortDictHeaderBytes(uint32_t n) {
  const uint64_t nb = (n + kSortDictBlock - 1) / kSortDictBlock;
  return 2 + (nb * 9) + ((nb + 1) * 8);
}

uint32_t SortDictBlockRows(const SortDictRegion& p, uint32_t blk) {
  const uint64_t start = static_cast<uint64_t>(blk) * kSortDictBlock;
  return start >= p.n ? 0
                      : static_cast<uint32_t>(
                            std::min<uint64_t>(kSortDictBlock, p.n - start));
}

int SortDictWidth(const SortDictRegion& p, uint32_t blk) {
  return static_cast<int>(
      static_cast<uint8_t>(p.widths[static_cast<size_t>(blk)]));
}

int64_t SortDictFirst(const SortDictRegion& p, uint32_t blk) {
  return static_cast<int64_t>(
      ReadBE64(p.firsts + (8 * static_cast<size_t>(blk))));
}

uint64_t SortDictPsum(const SortDictRegion& p, uint32_t blk) {
  return ReadBE64(p.psums + (8 * static_cast<size_t>(blk)));
}

// Byte offset of block `blk`'s gap plane. Blocks are byte-aligned (the kS4
// convention), so this is pure integer arithmetic over the width table.
uint64_t SortDictGapOffset(const SortDictRegion& p, uint32_t blk) {
  uint64_t off = 0;
  for (uint32_t j = 0; j < blk; ++j) {
    off += (static_cast<uint64_t>(SortDictBlockRows(p, j) - 1) *
                static_cast<uint64_t>(SortDictWidth(p, j)) +
            7) /
           8;
  }
  return off;
}

uint64_t SortDictExtent(const SortDictRegion& p) {
  uint64_t gaps = 0;
  for (uint32_t b = 0; b < p.nb; ++b) {
    gaps += (static_cast<uint64_t>(SortDictBlockRows(p, b) - 1) *
                 static_cast<uint64_t>(SortDictWidth(p, b)) +
             7) /
            8;
  }
  return SortDictHeaderBytes(p.n) +
         ((static_cast<uint64_t>(p.n) * static_cast<uint64_t>(p.rw) + 7) / 8) +
         gaps;
}

// Bounds-checked header parse. Mirrors ValidateWalk's case 61: nb is DERIVED
// from n and pinned against the footer param, rw is pinned to its canonical
// value, every width is checked, and the whole extent must fit before any
// payload is touched.
bool ParseSortDictRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, SortDictRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const uint32_t nb = (n + kSortDictBlock - 1) / kSortDictBlock;
  if (SortDictHeaderBytes(n) > static_cast<uint64_t>(end - r)) {
    return false;
  }
  if (static_cast<uint32_t>(static_cast<uint8_t>(r[0])) != nb) {
    return false;
  }
  if (param != nb) {
    return false;
  }
  const int rw = BitsNeeded(static_cast<uint64_t>(n) - 1);
  if (std::cmp_not_equal(static_cast<uint8_t>(r[1]), rw)) {
    return false;
  }
  SortDictRegion& p = *out;
  p.n = n;
  p.nb = nb;
  p.rw = rw;
  p.widths = r + 2;
  p.firsts = p.widths + nb;
  p.psums = p.firsts + (8 * static_cast<size_t>(nb));
  p.rank = p.psums + (8 * (static_cast<size_t>(nb) + 1));
  p.gaps =
      p.rank + ((static_cast<uint64_t>(n) * static_cast<uint64_t>(rw) + 7) / 8);
  for (uint32_t b = 0; b < nb; ++b) {
    // 0 is legal: an all-equal block stores no gap bytes at all.
    if (SortDictWidth(p, b) > 64) {
      return false;
    }
  }
  return SortDictExtent(p) <= static_cast<uint64_t>(end - r);
}

// Sorted index -> value: the block's first plus at most 63 replayed gaps.
// Unsigned accumulate, like every cumulative scheme in this file: the gaps
// are non-negative by construction, but the running value may wrap through
// both int64 extremes on a hostile image, and mod-2^64 is the defined walk.
int64_t SortDictValueAtSorted(const SortDictRegion& p, uint32_t j) {
  if (j >= p.n) {
    return 0;
  }
  const uint32_t blk = j / kSortDictBlock;
  const int w = SortDictWidth(p, blk);
  auto v = static_cast<uint64_t>(SortDictFirst(p, blk));
  if (w != 0) {
    const uint64_t boff = SortDictGapOffset(p, blk) * 8;
    const uint32_t local = j - (blk * kSortDictBlock);
    for (uint32_t t = 0; t < local; ++t) {
      v += UnpackBitAt(
          p.gaps, boff + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w)),
          w);
    }
  }
  return static_cast<int64_t>(v);
}

// Row -> sorted index. The rank plane is a permutation of [0, n) on any
// region the validator accepted; callers that can see unvalidated bytes must
// range-check the result (the clamp-bucket convention).
uint32_t SortDictRankAt(const SortDictRegion& p, uint32_t row) {
  return static_cast<uint32_t>(UnpackBitAt(
      p.rank, static_cast<uint64_t>(row) * static_cast<uint64_t>(p.rw), p.rw));
}

// lower_bound over the sorted view: bisect the ascending first table, then
// walk at most one 64-row block. Block firsts are NON-STRICTLY ascending
// (duplicates may span a block boundary), so after the bisect the answer is
// either inside the previous block or exactly at its end.
uint32_t SortDictLowerBoundSorted(const SortDictRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.nb;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (SortDictFirst(p, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo == 0) {
    return 0;  // key <= sorted[0]
  }
  const uint32_t blk = lo - 1;
  const uint32_t rows = SortDictBlockRows(p, blk);
  const int w = SortDictWidth(p, blk);
  const uint64_t boff = SortDictGapOffset(p, blk) * 8;
  auto v = static_cast<uint64_t>(SortDictFirst(p, blk));
  for (uint32_t t = 0; t < rows; ++t) {
    const auto sv = static_cast<int64_t>(v);
    if (sv >= key) {
      return (blk * kSortDictBlock) + t;
    }
    if (t + 1 < rows) {
      v += UnpackBitAt(
          p.gaps, boff + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w)),
          w);
    }
  }
  return (blk * kSortDictBlock) + rows;
}

// upper_bound over the sorted view (the first sorted index strictly > key).
uint32_t SortDictUpperBoundSorted(const SortDictRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.nb;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (SortDictFirst(p, mid) <= key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo == 0) {
    return 0;  // sorted[0] > key
  }
  const uint32_t blk = lo - 1;
  const uint32_t rows = SortDictBlockRows(p, blk);
  const int w = SortDictWidth(p, blk);
  const uint64_t boff = SortDictGapOffset(p, blk) * 8;
  auto v = static_cast<uint64_t>(SortDictFirst(p, blk));
  for (uint32_t t = 0; t < rows; ++t) {
    const auto sv = static_cast<int64_t>(v);
    if (sv > key) {
      return (blk * kSortDictBlock) + t;
    }
    if (t + 1 < rows) {
      v += UnpackBitAt(
          p.gaps, boff + (static_cast<uint64_t>(t) * static_cast<uint64_t>(w)),
          w);
    }
  }
  return (blk * kSortDictBlock) + rows;
}

// Predicate -> passing sorted window [wlo, whi). Returns false for the ops
// the window cannot express (the RleIdxPassingWindow convention).
bool SortDictPredicateWindow(const SortDictRegion& p, PaxChunk::CompareOp op,
                             int64_t lit, uint32_t* wlo, uint32_t* whi) {
  switch (op) {
    case PaxChunk::CompareOp::kEq:
      *wlo = SortDictLowerBoundSorted(p, lit);
      *whi = SortDictUpperBoundSorted(p, lit);
      break;
    case PaxChunk::CompareOp::kLe:
      *wlo = 0;
      *whi = SortDictUpperBoundSorted(p, lit);
      break;
    case PaxChunk::CompareOp::kLt:
      *wlo = 0;
      *whi = SortDictLowerBoundSorted(p, lit);
      break;
    case PaxChunk::CompareOp::kGe:
      *wlo = SortDictLowerBoundSorted(p, lit);
      *whi = p.n;
      break;
    case PaxChunk::CompareOp::kGt:
      *wlo = SortDictUpperBoundSorted(p, lit);
      *whi = p.n;
      break;
    default:
      return false;
  }
  return true;
}

// ---- kPostings (inverted dictionary) region access -------------------------
//
// The layout is six separate planes (the kS4 discipline: the parse defines
// every offset once and the encoder / validator / kernels all read through
// it), followed by the Rice-coded posting bitstream:
//   [d u16 BE][d x 8B BE dict ascending][d x u16 BE counts]
//   [ceil(d/2) x u8 k-nibbles][d x u16 BE firsts][d x u16 BE bitoffs]
//   [bitstream]
// Every extent derives from (n, d); the footer param echoes d. The bitoffs
// plane is what makes a selective mask O(matched): the cursor jumps straight
// to the first passing list's sub-stream instead of decoding the skipped
// lists' gaps.

uint64_t PostingsHeaderBytes(uint32_t d) {
  return 2 + (static_cast<uint64_t>(d) * 8) + (static_cast<uint64_t>(d) * 2) +
         ((static_cast<uint64_t>(d) + 1) / 2) + (static_cast<uint64_t>(d) * 4);
}

uint32_t PostingsCountAt(const PostingsRegion& p, uint32_t c) {
  return ReadBE16(p.counts + (static_cast<size_t>(c) * 2));
}

uint32_t PostingsFirstAt(const PostingsRegion& p, uint32_t c) {
  return ReadBE16(p.firsts + (static_cast<size_t>(c) * 2));
}

uint32_t PostingsBitOffAt(const PostingsRegion& p, uint32_t c) {
  return ReadBE16(p.bitoffs + (static_cast<size_t>(c) * 2));
}

uint32_t PostingsKAt(const PostingsRegion& p, uint32_t c) {
  const auto byte = static_cast<uint8_t>(p.ks[static_cast<size_t>(c) >> 1]);
  return (c & 1) == 0 ? (byte & 0x0F) : (byte >> 4);
}

int64_t PostingsDictAt(const PostingsRegion& p, uint32_t c) {
  return static_cast<int64_t>(
      ReadBE64(p.dict + (static_cast<size_t>(c) * sizeof(int64_t))));
}

// Bounds-checked structural parse. The cheap per-list invariants the closed
// forms need (counts sum to n, dict strictly ascending) are checked here so
// every kernel can trust them; the bitstream itself is only replayed by
// ValidateWalk's case 63 (a kernel never touches it without walking lists).
// region_end == nullptr is the ByteSliceNullAwareCount convention for a
// pre-validated region: the table reads stay bounded by the parsed d, only
// the end-vs-extent gate drops (kernels that WALK THE STREAM require a real
// end themselves).
bool ParsePostingsRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, PostingsRegion* out) {
  if (r == nullptr || out == nullptr || (end != nullptr && end < r) || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  if (end != nullptr && static_cast<uint64_t>(end - r) < 2) {
    return false;
  }
  const uint32_t d = ReadBE16(r);
  // d above the parse cap cannot amortize its tables under a row-group
  // ceiling of n <= 1024 rows (d <= n is checked next); the cap keeps every
  // per-list scratch bounded.
  if (d == 0 || d > kPostingsMaxLists || d > n || param != d) {
    return false;
  }
  const uint64_t have =
      (end != nullptr) ? static_cast<uint64_t>(end - r) : UINT64_MAX;
  if (PostingsHeaderBytes(d) > have) {
    return false;
  }
  PostingsRegion& p = *out;
  p.n = n;
  p.d = d;
  p.dict = r + 2;
  p.counts = p.dict + (static_cast<size_t>(d) * 8);
  p.ks = p.counts + (static_cast<size_t>(d) * 2);
  p.firsts = p.ks + ((static_cast<size_t>(d) + 1) / 2);
  p.bitoffs = p.firsts + (static_cast<size_t>(d) * 2);
  p.stream = p.bitoffs + (static_cast<size_t>(d) * 2);
  p.region_end = end;
  uint64_t total = 0;
  for (uint32_t c = 0; c < d; ++c) {
    const uint32_t cnt = PostingsCountAt(p, c);
    if (cnt == 0 || cnt > n) {
      return false;
    }
    total += cnt;
    if (PostingsFirstAt(p, c) >= n) {
      return false;
    }
    if (c == 0) {
      if (PostingsBitOffAt(p, 0) != 0) {
        return false;
      }
    } else {
      if (PostingsDictAt(p, c - 1) >= PostingsDictAt(p, c)) {
        return false;  // the code-window bisects need strict ascent
      }
      if (PostingsBitOffAt(p, c) < PostingsBitOffAt(p, c - 1)) {
        return false;  // the jump table may not relocate a sub-stream
      }
    }
  }
  return total == n;  // the exact histogram the closed-form folds trust
}

// MSB-first bit cursor over the posting stream, shared by every consumer
// that walks Rice codes (bulk decode, ValidateWalk's replay, the mask
// kernel's materialization). bit 0 of the stream is the MSB of byte 0 (the
// UnpackBitAt / AppendPackBits convention: a big-endian 64-bit load puts it
// at word bit 63).

// The row id of the next posting in list c's sub-stream (or the list's
// `first` before any code is consumed).
bool PostingsNextRow(const PostingsRegion& p, PostingsBitCursor* cur,
                     uint32_t c, uint32_t row, uint32_t* next) {
  uint64_t x = 0;
  if (!cur->Rice(PostingsKAt(p, c), &x)) {
    return false;
  }
  const uint64_t gap = x + 1;  // gaps are >= 1: one row, one list
  if (gap > p.n || row + gap >= p.n) {
    return false;
  }
  *next = row + static_cast<uint32_t>(gap);
  return true;
}

// Bulk decode: one pass over the d sub-streams (jumped by the bitoffs
// table -- sub-streams are byte-aligned, so a cursor starts flush), each
// list's value scattered to its rows. The per-row arm (Int64RegionAt) never
// runs this -- it reads the count table alone.
void DecodePostingsAll(const PostingsRegion& p, int64_t* out) {
  PostingsBitCursor cur;
  for (uint32_t c = 0; c < p.d; ++c) {
    const int64_t v = PostingsDictAt(p, c);
    uint32_t row = PostingsFirstAt(p, c);
    out[row] = v;
    const uint32_t cnt = PostingsCountAt(p, c);
    if (cnt == 1) {
      continue;
    }
    cur.Init(p.stream + (PostingsBitOffAt(p, c) / 8), p.region_end);
    for (uint32_t i = 1; i < cnt; ++i) {
      uint32_t next = 0;
      if (!PostingsNextRow(p, &cur, c, row, &next)) {
        return;  // truncated stream: tail keeps prior bytes (fail soft)
      }
      row = next;
      out[row] = v;
    }
  }
}

// lower/upper bound over the ascending dictionary: the list window a
// predicate passes. EQ folds to [lb, ub) (empty when the literal is
// absent); Le/Lt/Ge/Gt fold to a prefix/suffix.
uint32_t PostingsLowerBoundList(const PostingsRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.d;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (PostingsDictAt(p, mid) < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

uint32_t PostingsUpperBoundList(const PostingsRegion& p, int64_t key) {
  uint32_t lo = 0;
  uint32_t hi = p.d;
  while (lo < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (PostingsDictAt(p, mid) <= key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

// Closed-form COUNT + filtered SUM over the exact histogram: the predicate
// passes a contiguous list window and the fold reads the count table only.
// O(log d) for EQ, O(passing lists) for a range, zero bitstream reads, no
// row materialized whatever the row order. Only the header planes are read,
// so region_end == nullptr (the ByteSliceNullAwareCount pre-validated
// convention) is fine here.
bool PostingsRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out,
                            int64_t* sum_out) {
  PostingsRegion p;
  if (count_out == nullptr || region == nullptr ||
      !ParsePostingsRegion(region, n, param, region_end, &p)) {
    return false;
  }
  uint64_t cnt = 0;
  uint64_t sum = 0;
  if (op == PaxChunk::CompareOp::kEq) {
    const uint32_t lo = PostingsLowerBoundList(p, lit);
    if (lo < p.d && PostingsDictAt(p, lo) == lit) {
      cnt = PostingsCountAt(p, lo);
      sum = static_cast<uint64_t>(lit) * cnt;
    }
  } else {
    uint32_t lo = 0;
    uint32_t hi = p.d;
    switch (op) {
      case PaxChunk::CompareOp::kLe:
        hi = PostingsUpperBoundList(p, lit);
        break;
      case PaxChunk::CompareOp::kLt:
        hi = PostingsLowerBoundList(p, lit);
        break;
      case PaxChunk::CompareOp::kGe:
        lo = PostingsLowerBoundList(p, lit);
        break;
      case PaxChunk::CompareOp::kGt:
        lo = PostingsUpperBoundList(p, lit);
        break;
      default:
        return false;
    }
    for (uint32_t c = lo; c < hi; ++c) {
      const uint64_t f = PostingsCountAt(p, c);
      cnt += f;
      sum += static_cast<uint64_t>(PostingsDictAt(p, c)) * f;
    }
  }
  *count_out = cnt;
  if (sum_out != nullptr) {
    *sum_out = static_cast<int64_t>(sum);
  }
  return true;
}

double Chimp128Double(uint64_t bits) {
  double value = NAN;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

uint64_t Chimp128Bits(double value) {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

uint64_t Chimp128CanonicalBits(double value) {
  if (std::isnan(value)) {
    return UINT64_C(0x7ff8000000000000);
  }
  return Chimp128Bits(value);
}

bool ParseChimp128Region(const char* region, uint32_t n, uint32_t param,
                         const char* end, Chimp128Region* out) {
  if (region == nullptr || end == nullptr || end < region || out == nullptr ||
      n == 0 || n > PaxChunk::kRowGroupSize ||
      static_cast<size_t>(end - region) < 4) {
    return false;
  }
  const uint32_t blocks = (n + kChimp128BlockRows - 1) / kChimp128BlockRows;
  if (static_cast<uint8_t>(region[0]) != 1 || region[1] != 0 ||
      ReadBE16(region + 2) != blocks || param != blocks ||
      blocks > kChimp128MaxBlocks) {
    return false;
  }
  const char* cursor = region + 4;
  Chimp128Region parsed;
  parsed.n = n;
  parsed.block_count = blocks;
  for (uint32_t block_index = 0; block_index < blocks; ++block_index) {
    if (std::cmp_less(end - cursor, kChimp128BlockMetadataBytes)) {
      return false;
    }
    Chimp128Block& block = parsed.blocks[block_index];
    const uint32_t expected_rows =
        std::min(kChimp128BlockRows, n - (block_index * kChimp128BlockRows));
    const auto flags = static_cast<uint8_t>(cursor[1]);
    block.rows = static_cast<uint8_t>(cursor[0]);
    block.code_bits = ReadBE32(cursor + 2);
    block.first = ReadBE64(cursor + 6);
    block.minimum = ReadBE64(cursor + 14);
    block.maximum = ReadBE64(cursor + 22);
    block.sum = ReadBE64(cursor + 30);
    block.numeric_metadata = (flags & 1U) != 0;
    if (block.rows != expected_rows || (flags & ~1U) != 0 ||
        (block.code_bits < expected_rows - 1) ||
        block.code_bits > static_cast<uint64_t>(expected_rows - 1) * 78) {
      return false;
    }
    const size_t stream_bytes = (static_cast<size_t>(block.code_bits) + 7) / 8;
    if (stream_bytes >
        static_cast<size_t>(end - cursor) - kChimp128BlockMetadataBytes) {
      return false;
    }
    block.stream = cursor + kChimp128BlockMetadataBytes;
    cursor = block.stream + stream_bytes;
  }
  if (cursor != end) {
    return false;
  }
  parsed.encoded_size = static_cast<size_t>(cursor - region);
  *out = parsed;
  return true;
}

bool DecodeChimp128Block(const Chimp128Block& block, uint64_t* output) {
  if (output == nullptr || block.stream == nullptr || block.rows == 0 ||
      block.rows > kChimp128BlockRows ||
      block.code_bits > static_cast<uint64_t>(block.rows - 1) * 78) {
    return false;
  }
  const size_t stream_bytes = (static_cast<size_t>(block.code_bits) + 7) / 8;
  std::array<uint64_t, kChimp128BlockRows> history{};
  CachedBitReader reader(block.stream, block.code_bits);
  uint64_t previous = block.first;
  history[0] = previous;
  output[0] = previous;
  uint32_t head = 1;
  int previous_lz = 0;
  int previous_tz = 64;
  bool window_valid = false;
  for (uint32_t local = 1; local < block.rows; ++local) {
    if (reader.pos >= block.code_bits) {
      return false;
    }
    uint64_t current = previous;
    if (reader.get1() != 0) {
      if (reader.pos >= block.code_bits) {
        return false;
      }
      if (reader.get1() == 0) {
        if (!window_valid) {
          return false;
        }
        const int width = 64 - previous_lz - previous_tz;
        if (width <= 0 || width > 64 ||
            static_cast<uint64_t>(width) > block.code_bits - reader.pos) {
          return false;
        }
        const uint64_t center = reader.get(width);
        current ^= width == 64 ? center : center << previous_tz;
      } else {
        if (reader.pos >= block.code_bits) {
          return false;
        }
        bool history_reference = reader.get1() != 0;
        uint64_t reference = previous;
        if (history_reference) {
          if (7 > block.code_bits - reader.pos) {
            return false;
          }
          const auto age = static_cast<uint32_t>(reader.get(7));
          if (age >= local) {
            return false;
          }
          reference = history[(head - 1 - age) & 127U];
        }
        if (11 > block.code_bits - reader.pos) {
          return false;
        }
        const int lz = static_cast<int>(reader.get(5));
        const int tz = static_cast<int>(reader.get(6));
        if (lz > 31 || tz > 63 || lz + tz >= 64) {
          return false;
        }
        const int width = 64 - lz - tz;
        if (static_cast<uint64_t>(width) > block.code_bits - reader.pos) {
          return false;
        }
        const uint64_t center = reader.get(width);
        current = reference ^ (width == 64 ? center : center << tz);
        if (!history_reference) {
          previous_lz = lz;
          previous_tz = tz;
          window_valid = true;
        }
      }
    }
    history[head & 127U] = current;
    ++head;
    output[local] = current;
    previous = current;
  }
  if (reader.pos != block.code_bits) {
    return false;
  }
  if (block.code_bits % 8 != 0) {
    const uint32_t padding = 8 - (block.code_bits % 8);
    const auto mask = static_cast<uint8_t>((1U << padding) - 1U);
    if ((static_cast<uint8_t>(block.stream[stream_bytes - 1]) & mask) != 0) {
      return false;
    }
  }
  return true;
}

bool Chimp128MetadataMatches(const Chimp128Region& region) {
  std::array<uint64_t, kChimp128BlockRows> decoded{};
  for (uint32_t block_index = 0; block_index < region.block_count;
       ++block_index) {
    const Chimp128Block& block = region.blocks[block_index];
    if (!DecodeChimp128Block(block, decoded.data())) {
      return false;
    }
    double sum = Chimp128Double(block.first);
    bool has_numeric = false;
    bool numeric_metadata = true;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    for (uint32_t row = 0; row < block.rows; ++row) {
      const double value = Chimp128Double(decoded[row]);
      sum += row == 0 ? 0.0 : value;
      if (std::isnan(value)) {
        numeric_metadata = false;
        continue;
      }
      if (!has_numeric) {
        minimum = decoded[row];
        maximum = decoded[row];
        has_numeric = true;
      } else {
        if (value < Chimp128Double(minimum)) {
          minimum = decoded[row];
        }
        if (value > Chimp128Double(maximum)) {
          maximum = decoded[row];
        }
      }
    }
    if (Chimp128CanonicalBits(sum) != block.sum ||
        numeric_metadata != block.numeric_metadata ||
        (numeric_metadata && (!has_numeric || minimum != block.minimum ||
                              maximum != block.maximum))) {
      return false;
    }
  }
  return true;
}

uint64_t AlpRdTansBits(double value) {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

double AlpRdTansDouble(uint64_t bits) {
  double value = NAN;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

uint64_t AlpRdTansCanonicalBits(double value) {
  if (std::isnan(value)) {
    return UINT64_C(0x7ff8000000000000);
  }
  return AlpRdTansBits(value);
}

bool AlpRdTansComputeMetadata(const uint64_t* values, uint32_t rows,
                              uint64_t* first, uint64_t* minimum,
                              uint64_t* maximum, uint64_t* sum, bool* numeric) {
  if (values == nullptr || first == nullptr || minimum == nullptr ||
      maximum == nullptr || sum == nullptr || numeric == nullptr || rows == 0) {
    return false;
  }
  *first = values[0];
  double total = 0.0;
  bool has_numeric = false;
  bool all_numeric = true;
  uint64_t lo = 0;
  uint64_t hi = 0;
  for (uint32_t row = 0; row < rows; ++row) {
    const double value = AlpRdTansDouble(values[row]);
    total += value;
    if (std::isnan(value)) {
      all_numeric = false;
      continue;
    }
    if (!has_numeric) {
      lo = values[row];
      hi = values[row];
      has_numeric = true;
    } else {
      if (value < AlpRdTansDouble(lo)) {
        lo = values[row];
      }
      if (value > AlpRdTansDouble(hi)) {
        hi = values[row];
      }
    }
  }
  *sum = AlpRdTansCanonicalBits(total);
  if (!all_numeric || !has_numeric) {
    *minimum = 0;
    *maximum = 0;
  } else {
    *minimum = lo;
    *maximum = hi;
  }
  *numeric = all_numeric && has_numeric;
  return true;
}

std::vector<char> AlpRdTansPackBits(const std::vector<uint8_t>& bits) {
  std::vector<char> out((bits.size() + 7) / 8, '\0');
  for (size_t i = 0; i < bits.size(); ++i) {
    if (bits[i] != 0) {
      out[i >> 3] = static_cast<char>(static_cast<uint8_t>(out[i >> 3]) |
                                      (0x80U >> (i & 7U)));
    }
  }
  return out;
}

bool AlpRdTansBuildModel(const std::vector<std::vector<uint8_t>>& payloads,
                         AlpRdTansModel* model) {
  if (model == nullptr || payloads.empty()) {
    return false;
  }
  std::array<uint64_t, 256> counts{};
  uint64_t total = 0;
  for (const auto& payload : payloads) {
    for (uint8_t byte : payload) {
      ++counts[byte];
      ++total;
    }
  }
  if (total == 0) {
    return false;
  }
  std::array<uint8_t, 256> used{};
  uint32_t used_count = 0;
  for (uint32_t sym = 0; sym < 256; ++sym) {
    if (counts[sym] != 0) {
      used[sym] = 1;
      ++used_count;
    }
  }
  if (used_count < 2 || used_count > kAlpRdTansM) {
    return false;
  }
  std::array<uint16_t, 256> frequencies{};
  uint32_t assigned = 0;
  for (uint32_t sym = 0; sym < 256; ++sym) {
    if (used[sym] == 0) {
      continue;
    }
    uint64_t value = counts[sym] * kAlpRdTansM / total;
    if (value == 0) {
      value = 1;
    }
    value = std::min<uint64_t>(value, kAlpRdTansM);
    frequencies[sym] = static_cast<uint16_t>(value);
    assigned += static_cast<uint32_t>(value);
  }
  while (assigned < kAlpRdTansM) {
    uint32_t best = 256;
    for (uint32_t sym = 0; sym < 256; ++sym) {
      if (used[sym] == 0) {
        continue;
      }
      if (best == 256 || counts[sym] > counts[best] ||
          (counts[sym] == counts[best] && sym < best)) {
        best = sym;
      }
    }
    if (best == 256 || frequencies[best] == kAlpRdTansM) {
      return false;
    }
    ++frequencies[best];
    ++assigned;
  }
  while (assigned > kAlpRdTansM) {
    uint32_t best = 256;
    for (uint32_t sym = 0; sym < 256; ++sym) {
      if (used[sym] == 0 || frequencies[sym] <= 1) {
        continue;
      }
      if (best == 256 || frequencies[sym] > frequencies[best] ||
          (frequencies[sym] == frequencies[best] && sym < best)) {
        best = sym;
      }
    }
    if (best == 256) {
      return false;
    }
    --frequencies[best];
    --assigned;
  }
  if (assigned != kAlpRdTansM) {
    return false;
  }
  model->symbols.fill(0);
  model->frequencies.fill(0);
  model->cumulative.fill(0);
  uint16_t cumulative = 0;
  uint16_t model_count = 0;
  for (uint32_t sym = 0; sym < 256; ++sym) {
    if (frequencies[sym] == 0) {
      continue;
    }
    model->symbols[model_count] = static_cast<uint8_t>(sym);
    model->frequencies[model_count] = frequencies[sym];
    model->cumulative[model_count] = cumulative;
    cumulative = static_cast<uint16_t>(cumulative + frequencies[sym]);
    ++model_count;
  }
  model->count = model_count;
  return model_count != 0 && cumulative == kAlpRdTansM;
}

bool AlpRdTansEncode(const std::vector<uint8_t>& payload,
                     const AlpRdTansModel& model, std::vector<char>* stream,
                     uint32_t* stream_bits) {
  if (stream == nullptr || stream_bits == nullptr || payload.empty()) {
    return false;
  }
  std::array<uint32_t, 256> frequency{};
  std::array<uint32_t, 256> cumulative{};
  for (uint32_t i = 0; i < model.count; ++i) {
    const uint32_t sym = model.symbols[i];
    if (sym >= 256 || model.frequencies[i] == 0) {
      return false;
    }
    frequency[sym] = model.frequencies[i];
    cumulative[sym] = model.cumulative[i];
  }
  uint32_t current = 256;
  std::vector<uint8_t> spill;
  for (size_t i = payload.size(); i-- > 0;) {
    const uint32_t sym = payload[i];
    const uint32_t f = frequency[sym];
    if (f == 0) {
      return false;
    }
    while (current >= 2 * f) {
      spill.push_back(static_cast<uint8_t>(current & 1U));
      current >>= 1;
    }
    if (current < f || current >= 2 * f) {
      return false;
    }
    current = 256 + cumulative[sym] + (current - f);
    if (current < 256 || current >= 512) {
      return false;
    }
  }
  if (current < 256 || current >= 512) {
    return false;
  }
  std::vector<uint8_t> bits;
  bits.reserve(kAlpRdTansStateBits + spill.size());
  for (int bit = static_cast<int>(kAlpRdTansStateBits) - 1; bit >= 0; --bit) {
    bits.push_back(static_cast<uint8_t>((current >> bit) & 1U));
  }
  bits.insert(bits.end(), spill.begin(), spill.end());
  if (bits.size() > UINT32_MAX) {
    return false;
  }
  *stream_bits = static_cast<uint32_t>(bits.size());
  *stream = AlpRdTansPackBits(bits);
  return true;
}

bool AlpRdTansDecode(const char* stream, size_t stream_len,
                     uint32_t stream_bits, const AlpRdTansModel& model,
                     size_t max_output, std::vector<uint8_t>* output) {
  if (stream == nullptr || output == nullptr || model.count == 0 ||
      model.count > 256 || stream_bits < kAlpRdTansStateBits ||
      stream_len != (static_cast<size_t>(stream_bits) + 7) / 8) {
    return false;
  }
  const unsigned padding = stream_bits & 7U;
  if (padding != 0) {
    const auto mask = static_cast<uint8_t>((1U << (8U - padding)) - 1U);
    if ((static_cast<uint8_t>(stream[stream_len - 1]) & mask) != 0) {
      return false;
    }
  }
  std::array<uint8_t, 256> state_symbol{};
  std::array<uint32_t, 256> state_previous{};
  std::array<uint8_t, 256> state_valid{};
  uint16_t cumulative = 0;
  for (uint32_t i = 0; i < model.count; ++i) {
    const uint32_t sym = model.symbols[i];
    const uint32_t f = model.frequencies[i];
    if (sym >= 256 || f == 0 || cumulative + f > kAlpRdTansM ||
        (i != 0 && sym <= model.symbols[i - 1])) {
      return false;
    }
    for (uint32_t r = 0; r < f; ++r) {
      const uint32_t slot = cumulative + r;
      state_symbol[slot] = static_cast<uint8_t>(sym);
      state_previous[slot] = f + r;
      state_valid[slot] = 1;
    }
    cumulative = static_cast<uint16_t>(cumulative + f);
  }
  if (cumulative != kAlpRdTansM) {
    return false;
  }
  uint32_t state = 0;
  for (uint32_t bit = 0; bit < kAlpRdTansStateBits; ++bit) {
    const uint32_t position = bit;
    const uint32_t value = (static_cast<uint8_t>(stream[position >> 3]) >>
                            (7U - (position & 7U))) &
                           1U;
    state = (state << 1) | value;
  }
  if (state < 256 || state >= 512) {
    return false;
  }
  int64_t next_bit = static_cast<int64_t>(stream_bits) - 1;
  output->clear();
  while (output->size() <= max_output) {
    const uint32_t slot = state - 256;
    if (state_valid[slot] == 0U) {
      return false;
    }
    output->push_back(state_symbol[slot]);
    uint32_t previous = state_previous[slot];
    while (previous < 256) {
      if (std::cmp_less(next_bit, kAlpRdTansStateBits)) {
        return false;
      }
      const auto bit = static_cast<uint32_t>(next_bit);
      const uint32_t value =
          (static_cast<uint8_t>(stream[bit >> 3]) >> (7U - (bit & 7U))) & 1U;
      previous = (previous << 1) | value;
      --next_bit;
    }
    if (previous < 256 || previous >= 512) {
      return false;
    }
    state = previous;
    if (state == 256 &&
        next_bit == static_cast<int64_t>(kAlpRdTansStateBits) - 1) {
      return true;
    }
  }
  return false;
}

bool AlpRdTansDecodePfor(const char* leaf, size_t leaf_len, uint32_t raw_len,
                         uint32_t width, std::vector<int64_t>* values) {
  if (leaf == nullptr || values == nullptr || width == 0 || width > 64 ||
      raw_len == 0 || leaf_len < 11 || static_cast<uint8_t>(leaf[8]) != width) {
    return false;
  }
  const uint32_t exceptions = ReadBE16(leaf + 9);
  if (exceptions > raw_len) {
    return false;
  }
  const uint64_t packed_bytes =
      (static_cast<uint64_t>(raw_len) * width + 7U) / 8U;
  const uint64_t expected =
      11U + packed_bytes + (static_cast<uint64_t>(exceptions) * 10U);
  if (expected != leaf_len) {
    return false;
  }
  const uint64_t base = ReadBE64(leaf);
  values->assign(raw_len, 0);
  for (uint32_t i = 0; i < raw_len; ++i) {
    const uint64_t offset = UnpackBitAt(
        leaf + 11, static_cast<uint64_t>(i) * width, static_cast<int>(width));
    (*values)[i] = static_cast<int64_t>(base + offset);
  }
  const char* exception = leaf + 11 + static_cast<size_t>(packed_bytes);
  uint32_t previous = 0;
  for (uint32_t i = 0; i < exceptions; ++i) {
    const uint32_t position = ReadBE16(exception);
    if (position >= raw_len || (i != 0 && position <= previous)) {
      return false;
    }
    (*values)[position] = static_cast<int64_t>(ReadBE64(exception + 2));
    previous = position;
    exception += 10;
  }
  return true;
}

bool AlpRdTansDecodeBp128(const char* leaf, size_t leaf_len, uint32_t raw_len,
                          uint32_t leaf_param, std::vector<int64_t>* values) {
  if (leaf == nullptr || values == nullptr || raw_len == 0 ||
      raw_len > PaxChunk::kRowGroupSize || leaf_len == 0) {
    return false;
  }
  const uint32_t blocks =
      (raw_len + kAlpRdTansBlockRows - 1) / kAlpRdTansBlockRows;
  if (blocks != leaf_param || blocks > kAlpRdTansMaxBlocks) {
    return false;
  }
  Bp128Region parsed;
  if (!ParseBp128Region(leaf, raw_len, blocks, leaf + leaf_len, &parsed) ||
      parsed.encoded_size != leaf_len || !Bp128MetadataMatches(parsed)) {
    return false;
  }
  values->assign(raw_len, 0);
  DecodeBp128All(parsed, values->data());
  return true;
}

bool AlpRdTansDecodeBlock(const AlpRdTansRegion& region, uint32_t block,
                          std::vector<uint64_t>* values) {
  if (values == nullptr || block >= region.block_count) {
    return false;
  }
  const AlpRdTansBlock& descriptor = region.blocks[block];
  const uint32_t rows = descriptor.rows;
  if (rows == 0 || descriptor.raw_len < 2U * rows ||
      descriptor.raw_len > 4U * rows ||
      ((descriptor.raw_len - 2U * rows) & 1U) != 0) {
    return false;
  }
  const uint32_t exception_count = (descriptor.raw_len - 2U * rows) / 2U;
  size_t max_leaf = 0;
  if (descriptor.leaf_id == static_cast<uint8_t>(Scheme::kPfor)) {
    if (descriptor.leaf_param == 0 || descriptor.leaf_param > 64) {
      return false;
    }
    max_leaf = 11U +
               ((static_cast<size_t>(descriptor.raw_len) * 64U + 7U) / 8U) +
               (static_cast<size_t>(descriptor.raw_len) * 10U);
  } else if (descriptor.leaf_id == static_cast<uint8_t>(Scheme::kBp128)) {
    const uint32_t blocks =
        (descriptor.raw_len + kAlpRdTansBlockRows - 1) / kAlpRdTansBlockRows;
    if (descriptor.leaf_param != blocks) {
      return false;
    }
    max_leaf = (static_cast<size_t>(blocks) * kBp128MetadataBytes) +
               ((static_cast<size_t>(descriptor.raw_len) * 64U + 7U) / 8U);
  } else {
    return false;
  }
  std::vector<uint8_t> leaf;
  leaf.reserve(max_leaf);
  if (!AlpRdTansDecode(descriptor.stream, descriptor.stream_len,
                       descriptor.stream_bits, region.model, max_leaf, &leaf)) {
    return false;
  }
  std::vector<int64_t> integer_values;
  if (descriptor.leaf_id == static_cast<uint8_t>(Scheme::kPfor)) {
    if (!AlpRdTansDecodePfor(reinterpret_cast<const char*>(leaf.data()),
                             leaf.size(), descriptor.raw_len,
                             descriptor.leaf_param, &integer_values)) {
      return false;
    }
  } else if (!AlpRdTansDecodeBp128(reinterpret_cast<const char*>(leaf.data()),
                                   leaf.size(), descriptor.raw_len,
                                   descriptor.leaf_param, &integer_values)) {
    return false;
  }
  if (integer_values.size() != descriptor.raw_len) {
    return false;
  }
  const uint32_t tail_base = rows;
  const uint32_t exception_pos_base = 2U * rows;
  const uint32_t exception_value_base = exception_pos_base + exception_count;
  const uint64_t mask = region.right_width == 64
                            ? ~UINT64_C(0)
                            : ((UINT64_C(1) << region.right_width) - 1U);
  std::array<uint8_t, kAlpRdTansBlockRows> is_exception{};
  uint32_t previous = 0;
  for (uint32_t i = 0; i < exception_count; ++i) {
    const int64_t position = integer_values[exception_pos_base + i];
    if (position < 0 || std::cmp_greater_equal(position, rows) ||
        (i != 0 && std::cmp_less_equal(position, previous))) {
      return false;
    }
    is_exception[static_cast<uint32_t>(position)] = 1;
    if (integer_values[static_cast<uint32_t>(position)] != 0) {
      return false;
    }
    previous = static_cast<uint32_t>(position);
  }
  values->assign(rows, 0);
  uint32_t exception_index = 0;
  for (uint32_t row = 0; row < rows; ++row) {
    if (is_exception[row] != 0) {
      (*values)[row] = static_cast<uint64_t>(
          integer_values[exception_value_base + exception_index]);
      ++exception_index;
      continue;
    }
    const int64_t code = integer_values[row];
    const int64_t tail = integer_values[tail_base + row];
    if (code < 0 || std::cmp_greater_equal(code, region.dict_count) ||
        tail < 0 || std::cmp_greater(tail, mask)) {
      return false;
    }
    const uint64_t front = region.fronts[static_cast<uint32_t>(code)];
    const uint64_t shifted =
        region.right_width >= 64 ? 0 : (front << region.right_width);
    (*values)[row] = shifted | static_cast<uint64_t>(tail);
  }
  return exception_index == exception_count;
}

bool ParseAlpRdTansRegion(const char* region, uint32_t n, uint32_t param,
                          const char* end, AlpRdTansRegion* out) {
  if (region == nullptr || end == nullptr || end < region || out == nullptr ||
      n == 0 || n > PaxChunk::kRowGroupSize || param == 0 ||
      std::cmp_less(end - region, kAlpRdTansHeaderBytes)) {
    return false;
  }
  const uint32_t blocks = (n + kAlpRdTansBlockRows - 1) / kAlpRdTansBlockRows;
  if (static_cast<uint8_t>(region[0]) != 1 ||
      static_cast<uint8_t>(region[1]) != 0 || ReadBE16(region + 2) != blocks ||
      blocks > kAlpRdTansMaxBlocks || param != blocks ||
      static_cast<uint8_t>(region[6]) != 8 ||
      static_cast<uint8_t>(region[7]) != 0 || ReadBE16(region + 10) != 0) {
    return false;
  }
  const uint32_t right_width = static_cast<uint8_t>(region[4]);
  const uint32_t dict_count = static_cast<uint8_t>(region[5]);
  const uint32_t model_count = ReadBE16(region + 8);
  if (right_width < 44 || right_width > 56 || dict_count == 0 ||
      dict_count > 8 || model_count < 2 || model_count > 256) {
    return false;
  }
  const uint64_t fixed =
      kAlpRdTansHeaderBytes + (static_cast<uint64_t>(dict_count) * 2U) +
      (static_cast<uint64_t>(model_count) * 3U) +
      (static_cast<uint64_t>(blocks) * kAlpRdTansDescriptorBytes);
  if (std::cmp_greater(fixed, end - region)) {
    return false;
  }
  const char* cursor = region + kAlpRdTansHeaderBytes;
  std::array<uint16_t, 8> fronts{};
  uint16_t previous_front = 0;
  for (uint32_t i = 0; i < dict_count; ++i) {
    fronts[i] = ReadBE16(cursor);
    if (i != 0 && fronts[i] <= previous_front) {
      return false;
    }
    previous_front = fronts[i];
    cursor += 2;
  }
  AlpRdTansModel model;
  uint16_t cumulative = 0;
  uint8_t previous_symbol = 0;
  for (uint32_t i = 0; i < model_count; ++i) {
    const auto sym = static_cast<uint8_t>(*cursor++);
    const uint16_t frequency = ReadBE16(cursor);
    cursor += 2;
    if (frequency == 0 || (i != 0 && sym <= previous_symbol) ||
        std::cmp_greater(cumulative + frequency, kAlpRdTansM)) {
      return false;
    }
    model.symbols[i] = sym;
    model.frequencies[i] = frequency;
    model.cumulative[i] = cumulative;
    cumulative = static_cast<uint16_t>(cumulative + frequency);
    previous_symbol = sym;
  }
  if (cumulative != kAlpRdTansM) {
    return false;
  }
  model.count = static_cast<uint16_t>(model_count);
  const char* streams =
      cursor + (static_cast<size_t>(blocks) * kAlpRdTansDescriptorBytes);
  uint64_t stream_cursor = 0;
  for (uint32_t block = 0; block < blocks; ++block) {
    AlpRdTansBlock& item = out->blocks[block];
    const char* descriptor =
        cursor + (static_cast<size_t>(block) * kAlpRdTansDescriptorBytes);
    if (std::cmp_less(end - descriptor, kAlpRdTansDescriptorBytes)) {
      return false;
    }
    const uint32_t expected_rows =
        std::min(kAlpRdTansBlockRows, n - (block * kAlpRdTansBlockRows));
    const auto rows = static_cast<uint8_t>(descriptor[0]);
    const auto flags = static_cast<uint8_t>(descriptor[1]);
    const auto leaf_id = static_cast<uint8_t>(descriptor[4]);
    const uint32_t leaf_param = ReadBE32(descriptor + 8);
    const uint32_t raw_len = ReadBE32(descriptor + 12);
    const uint32_t stream_offset = ReadBE32(descriptor + 48);
    const uint32_t stream_len = ReadBE32(descriptor + 52);
    const uint32_t stream_bits = ReadBE32(descriptor + 56);
    if (rows != expected_rows || flags == 0 || (flags & ~3U) != 0 ||
        (flags & 2U) == 0 || ReadBE16(descriptor + 2) != 0 ||
        descriptor[5] != 0 || descriptor[6] != 0 || descriptor[7] != 0 ||
        ReadBE32(descriptor + 60) != 0 ||
        (leaf_id != static_cast<uint8_t>(Scheme::kPfor) &&
         leaf_id != static_cast<uint8_t>(Scheme::kBp128)) ||
        raw_len < 2U * rows || raw_len > 4U * rows ||
        ((raw_len - 2U * rows) & 1U) != 0 || stream_offset != stream_cursor ||
        stream_bits < kAlpRdTansStateBits ||
        stream_len != (static_cast<uint64_t>(stream_bits) + 7U) / 8U) {
      return false;
    }
    if (leaf_id == static_cast<uint8_t>(Scheme::kPfor) &&
        (leaf_param == 0 || leaf_param > 64)) {
      return false;
    }
    if (leaf_id == static_cast<uint8_t>(Scheme::kBp128) &&
        leaf_param !=
            (raw_len + kAlpRdTansBlockRows - 1) / kAlpRdTansBlockRows) {
      return false;
    }
    const uint64_t next_stream = stream_cursor + stream_len;
    if (next_stream < stream_cursor ||
        std::cmp_greater(next_stream, end - streams)) {
      return false;
    }
    item.descriptor = descriptor;
    item.stream = streams + stream_cursor;
    item.rows = rows;
    item.flags = flags;
    item.leaf_id = leaf_id;
    item.leaf_param = leaf_param;
    item.raw_len = raw_len;
    item.stream_len = stream_len;
    item.stream_bits = stream_bits;
    item.first = ReadBE64(descriptor + 16);
    item.minimum = ReadBE64(descriptor + 24);
    item.maximum = ReadBE64(descriptor + 32);
    item.sum = ReadBE64(descriptor + 40);
    stream_cursor = next_stream;
  }
  if (streams + stream_cursor != end) {
    return false;
  }
  out->n = n;
  out->block_count = blocks;
  out->right_width = right_width;
  out->dict_count = dict_count;
  out->fronts = fronts;
  out->streams = streams;
  out->model = model;
  out->encoded_size = static_cast<size_t>(end - region);
  return true;
}

bool AlpRdTansMetadataMatches(const AlpRdTansRegion& region) {
  for (uint32_t block = 0; block < region.block_count; ++block) {
    std::vector<uint64_t> values;
    if (!AlpRdTansDecodeBlock(region, block, &values)) {
      return false;
    }
    uint64_t first = 0;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    uint64_t sum = 0;
    bool numeric = false;
    if (!AlpRdTansComputeMetadata(values.data(),
                                  static_cast<uint32_t>(values.size()), &first,
                                  &minimum, &maximum, &sum, &numeric)) {
      return false;
    }
    const AlpRdTansBlock& descriptor = region.blocks[block];
    const bool metadata_numeric = (descriptor.flags & 1U) != 0;
    if (first != descriptor.first || sum != descriptor.sum ||
        metadata_numeric != numeric ||
        (numeric &&
         (minimum != descriptor.minimum || maximum != descriptor.maximum)) ||
        (!numeric && (descriptor.minimum != 0 || descriptor.maximum != 0))) {
      return false;
    }
  }
  return true;
}

double PatasDouble(uint64_t bits) {
  double value = NAN;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

uint64_t PatasCanonicalBits(double value) {
  if (std::isnan(value)) {
    return UINT64_C(0x7ff8000000000000);
  }
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

uint32_t PatasTrailingZeros(uint64_t value) {
  return static_cast<uint32_t>(__builtin_ctzll(value));
}

uint32_t PatasPayloadBytes(uint64_t value) {
  if (value == 0) {
    return 0;
  }
  const uint32_t span = 64U - static_cast<uint32_t>(__builtin_clzll(value)) -
                        PatasTrailingZeros(value);
  return (span + 7U) / 8U;
}

bool PatasComputeMetadata(const uint64_t* values, uint32_t rows,
                          uint64_t* first, uint64_t* minimum, uint64_t* maximum,
                          uint64_t* sum, bool* numeric) {
  if (values == nullptr || first == nullptr || minimum == nullptr ||
      maximum == nullptr || sum == nullptr || numeric == nullptr || rows == 0) {
    return false;
  }
  *first = values[0];
  double total = 0.0;
  bool has_numeric = false;
  bool all_numeric = true;
  uint64_t lo = 0;
  uint64_t hi = 0;
  for (uint32_t row = 0; row < rows; ++row) {
    const double value = PatasDouble(values[row]);
    total += value;
    if (std::isnan(value)) {
      all_numeric = false;
      continue;
    }
    if (!has_numeric) {
      lo = values[row];
      hi = values[row];
      has_numeric = true;
    } else {
      if (value < PatasDouble(lo)) {
        lo = values[row];
      }
      if (value > PatasDouble(hi)) {
        hi = values[row];
      }
    }
  }
  *sum = PatasCanonicalBits(total);
  if (!all_numeric || !has_numeric) {
    *minimum = 0;
    *maximum = 0;
  } else {
    *minimum = lo;
    *maximum = hi;
  }
  *numeric = all_numeric && has_numeric;
  return true;
}

bool PatasReadXor(const char*& cursor, const char* end, uint32_t length,
                  uint32_t tz, uint64_t* out) {
  if (length == 0 || length > 8 || tz > 63 ||
      (static_cast<uint64_t>(length) * 8U) + tz > kPatasMaxSpan) {
    return false;
  }
  if (end < cursor || std::cmp_less(end - cursor, length)) {
    return false;
  }
  uint64_t q = 0;
  for (uint32_t i = 0; i < length; ++i) {
    q |= static_cast<uint64_t>(static_cast<unsigned char>(cursor[i]))
         << (i * 8U);
  }
  cursor += length;
  if ((q >> ((length - 1U) * 8U)) == 0) {
    return false;
  }
  const uint64_t x = q << tz;
  if (x == 0) {
    return false;
  }
  if (PatasPayloadBytes(x) != length || PatasTrailingZeros(x) != tz) {
    return false;
  }
  *out = x;
  return true;
}

bool ParsePatasRegion(const char* region, uint32_t n, uint32_t param,
                      const char* end, PatasRegion* out) {
  if (region == nullptr || end == nullptr || end < region || out == nullptr ||
      n == 0 || n > PaxChunk::kRowGroupSize ||
      std::cmp_less(end - region, kPatasHeaderBytes)) {
    return false;
  }
  const uint32_t blocks = (n + kPatasBlockRows - 1) / kPatasBlockRows;
  if (blocks == 0 || blocks > kPatasMaxBlocks || param != blocks ||
      static_cast<unsigned char>(region[0]) != 0x50 ||
      static_cast<unsigned char>(region[1]) != 0x54 ||
      static_cast<unsigned char>(region[2]) != 1 ||
      static_cast<unsigned char>(region[3]) != kPatasBlockRows ||
      static_cast<unsigned char>(region[4]) != 0 || ReadBE16(region + 5) != 0 ||
      ReadBE16(region + 7) != blocks || ReadBE32(region + 9) != 0) {
    return false;
  }
  const auto region_len = static_cast<uint64_t>(end - region);
  const uint64_t fixed =
      static_cast<uint64_t>(kPatasHeaderBytes) +
      (static_cast<uint64_t>(blocks) * kPatasDescriptorBytes);
  if (region_len < fixed) {
    return false;
  }
  uint64_t meta_total = 0;
  for (uint32_t block = 0; block < blocks; ++block) {
    meta_total +=
        ReadBE32(region + kPatasHeaderBytes +
                 (static_cast<size_t>(block) * kPatasDescriptorBytes) + 44);
  }
  if (fixed + meta_total > region_len) {
    return false;
  }
  uint64_t meta_cursor = fixed;
  uint64_t payload_cursor = fixed + meta_total;
  PatasRegion parsed;
  parsed.n = n;
  parsed.block_count = blocks;
  for (uint32_t block = 0; block < blocks; ++block) {
    const char* descriptor =
        region + kPatasHeaderBytes +
        (static_cast<size_t>(block) * kPatasDescriptorBytes);
    const uint32_t expected_rows =
        std::min(kPatasBlockRows, n - (block * kPatasBlockRows));
    const auto rows = static_cast<unsigned char>(descriptor[0]);
    const auto flags = static_cast<unsigned char>(descriptor[1]);
    const uint64_t meta_offset = ReadBE32(descriptor + 40);
    const uint64_t meta_len = ReadBE32(descriptor + 44);
    const uint64_t payload_offset = ReadBE32(descriptor + 48);
    const uint64_t payload_len = ReadBE32(descriptor + 52);
    if (rows != expected_rows || (flags & ~3U) != 0 || (flags & 2U) == 0 ||
        ReadBE16(descriptor + 2) != 0 || ReadBE32(descriptor + 4) != 0 ||
        ReadBE64(descriptor + 56) != 0 || meta_offset != meta_cursor ||
        payload_offset != payload_cursor || meta_offset < fixed ||
        meta_offset + meta_len > region_len ||
        payload_offset + payload_len > region_len) {
      return false;
    }
    PatasBlock& item = parsed.blocks[block];
    item.meta = region + meta_offset;
    item.meta_end = region + meta_offset + meta_len;
    item.payload = region + payload_offset;
    item.payload_end = region + payload_offset + payload_len;
    item.rows = rows;
    item.flags = flags;
    item.first = ReadBE64(descriptor + 8);
    item.minimum = ReadBE64(descriptor + 16);
    item.maximum = ReadBE64(descriptor + 24);
    item.sum = ReadBE64(descriptor + 32);
    meta_cursor += meta_len;
    payload_cursor += payload_len;
  }
  if (payload_cursor != region_len) {
    return false;
  }
  parsed.encoded_size = static_cast<size_t>(region_len);
  *out = parsed;
  return true;
}

bool DecodePatasBlock(const PatasBlock& block, uint64_t* output) {
  if (output == nullptr || block.meta == nullptr || block.meta_end == nullptr ||
      block.payload == nullptr || block.payload_end == nullptr ||
      block.meta_end < block.meta || block.payload_end < block.payload ||
      block.rows == 0 || block.rows > kPatasBlockRows) {
    return false;
  }
  const char* meta = block.meta;
  const char* payload = block.payload;
  const char* payload_end = block.payload_end;
  uint64_t previous = block.first;
  output[0] = previous;
  uint32_t window_length = 0;
  uint32_t window_tz = 0;
  for (uint32_t local = 1; local < block.rows; ++local) {
    if (meta >= block.meta_end) {
      return false;
    }
    const auto control = static_cast<unsigned char>(*meta++);
    uint64_t reference = previous;
    uint64_t x = 0;
    uint32_t length = 0;
    uint32_t tz = 0;
    bool explicit_window = false;
    switch (control) {
      case kPatasRepeat:
        output[local] = previous;
        continue;
      case kPatasWindow:
        if (window_length == 0) {
          return false;
        }
        length = window_length;
        tz = window_tz;
        break;
      case kPatasPrev:
      case kPatasHist: {
        if (block.meta_end - meta < 2) {
          return false;
        }
        const auto length_byte = static_cast<unsigned char>(*meta++);
        const auto tz_byte = static_cast<unsigned char>(*meta++);
        length = static_cast<uint32_t>(length_byte) + 1U;
        tz = tz_byte;
        if (control == kPatasHist) {
          if (meta >= block.meta_end) {
            return false;
          }
          const auto age = static_cast<unsigned char>(*meta++);
          if (age == 0 || static_cast<uint32_t>(age) > local ||
              static_cast<uint32_t>(age) > kPatasMaxAge) {
            return false;
          }
          reference = output[local - static_cast<uint32_t>(age)];
        }
        explicit_window = true;
        break;
      }
      default:
        return false;
    }
    if (!PatasReadXor(payload, payload_end, length, tz, &x)) {
      return false;
    }
    output[local] = reference ^ x;
    if (explicit_window) {
      window_length = length;
      window_tz = tz;
    }
    previous = output[local];
  }
  return meta == block.meta_end && payload == payload_end;
}

bool PatasMetadataMatches(const PatasRegion& region) {
  std::array<uint64_t, kPatasBlockRows> decoded{};
  for (uint32_t block = 0; block < region.block_count; ++block) {
    const PatasBlock& descriptor = region.blocks[block];
    if (!DecodePatasBlock(descriptor, decoded.data())) {
      return false;
    }
    uint64_t first = 0;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    uint64_t sum = 0;
    bool numeric = false;
    if (!PatasComputeMetadata(decoded.data(), descriptor.rows, &first, &minimum,
                              &maximum, &sum, &numeric)) {
      return false;
    }
    const bool metadata_numeric = (descriptor.flags & 1U) != 0;
    if (first != descriptor.first || sum != descriptor.sum ||
        metadata_numeric != numeric ||
        (numeric &&
         (minimum != descriptor.minimum || maximum != descriptor.maximum)) ||
        (!numeric && (descriptor.minimum != 0 || descriptor.maximum != 0))) {
      return false;
    }
  }
  return true;
}

int64_t DeltaPForValueAt(const DeltaPForRegion& p, uint32_t idx) {
  if (idx >= p.n) {
    return 0;
  }
  const uint32_t block = idx / kDeltaPForBlockRows;
  const uint32_t start = block * kDeltaPForBlockRows;
  const char* bracket =
      p.brackets + (static_cast<size_t>(block) * kDeltaPForBracketBytes);
  uint64_t value = ReadBE64(bracket);
  uint32_t exception = DeltaPForExceptionAtOrAfter(p, start + 1);
  const size_t packed_bytes = (static_cast<size_t>(p.n - 1) * p.width + 7) / 8;
  WordBitReader reader(p.packed, p.packed + packed_bytes);
  reader.bitpos = static_cast<uint64_t>(start) * p.width;
  for (uint32_t pos = start + 1; pos <= idx; ++pos) {
    const uint64_t offset = reader.get(static_cast<int>(p.width));
    if (exception < p.exceptions_count &&
        ReadBE16(p.exceptions + (static_cast<size_t>(exception) * 10)) == pos) {
      value +=
          ReadBE64(p.exceptions + (static_cast<size_t>(exception) * 10) + 2);
      ++exception;
    } else {
      value += p.delta_min + offset;
    }
  }
  return static_cast<int64_t>(value);
}

void DecodeDeltaPForBlock(const DeltaPForRegion& p, uint32_t block,
                          int64_t* out) {
  if (block >= p.blocks) {
    return;
  }
  const size_t packed_bytes = (static_cast<size_t>(p.n - 1) * p.width + 7) / 8;
  WordBitReader reader(p.packed, p.packed + packed_bytes);
  const uint32_t start = block * kDeltaPForBlockRows;
  const uint32_t rows = std::min<uint32_t>(kDeltaPForBlockRows, p.n - start);
  reader.bitpos = static_cast<uint64_t>(start) * p.width;
  uint32_t exception = DeltaPForExceptionAtOrAfter(p, start + 1);
  const char* bracket =
      p.brackets + (static_cast<size_t>(block) * kDeltaPForBracketBytes);
  uint64_t value = ReadBE64(bracket);
  out[0] = static_cast<int64_t>(value);
  for (uint32_t local = 1; local < rows; ++local) {
    const uint32_t pos = start + local;
    const uint64_t offset = reader.get(static_cast<int>(p.width));
    if (exception < p.exceptions_count &&
        ReadBE16(p.exceptions + (static_cast<size_t>(exception) * 10)) == pos) {
      value +=
          ReadBE64(p.exceptions + (static_cast<size_t>(exception) * 10) + 2);
      ++exception;
    } else {
      value += p.delta_min + offset;
    }
    out[local] = static_cast<int64_t>(value);
  }
}

void DecodeDeltaPForAll(const DeltaPForRegion& p, int64_t* out) {
  for (uint32_t b = 0; b < p.blocks; ++b) {
    DecodeDeltaPForBlock(p, b,
                         out + (static_cast<size_t>(b) * kDeltaPForBlockRows));
  }
}

// ALP (scheme 20) decimal factors 10^0..10^18. Every value here is exactly
// representable as a double (5^e < 2^53 for e <= 22), so the encode-side
// verify and the decode-side reconstruct division are deterministic IEEE754
// ops that reproduce the original bits bit-for-bit.
double AlpFactorOf(int exp) {
  static constexpr std::array<double, 19> kFactor = {
      1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8, 1e9,
      1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};
  return kFactor[exp];
}

// --- rANS (kAnsFor) ----------------------------------------------------------
// rANS constants: states live in [kAnsL, 256*kAnsL); one renormalization
// emits/consumes one byte. prec is the frequency precision (M = 1<<prec
// slots per state cycle); the encoder emits prec = 12.

// Parsed kAnsFor header shared by the point, batch, and predicate readers.

// Bounds-checked header parse; mirrors ValidateWalk's case-29 checks so the
// readers fail closed on hand-built footers without depending on prior
// validation.
bool AnsForParse(const char* r, const char* region_end, uint32_t n,
                 uint32_t param, AnsForRegion* A) {
  if (region_end == nullptr || r + 16 > region_end) {
    return false;
  }
  A->base = static_cast<int64_t>(ReadBE64(r));
  A->bpv = static_cast<uint8_t>(r[8]);
  A->prec = static_cast<uint8_t>(r[9]);
  A->nsym = ReadBE16(r + 10);
  if (A->bpv != param || A->bpv < 1 || A->bpv > 8 || A->prec < 8 ||
      A->prec > 15 || A->nsym == 0 || A->nsym > 256) {
    return false;
  }
  A->M = 1U << A->prec;
  if (r + 12 + (static_cast<size_t>(A->nsym) * 3) + 2 > region_end) {
    return false;
  }
  const char* p = r + 12;
  uint32_t acc = 0;
  uint8_t prev = 0;
  for (uint32_t i = 0; i < A->nsym; ++i) {
    const auto sym = static_cast<uint8_t>(p[0]);
    const uint32_t fr = ReadBE16(p + 1);
    // Strictly ascending unique symbols (the encoder's emit order) and a
    // nonzero share; together they pin the cum[] walk to the M slots.
    if (fr == 0 || (i > 0 && sym <= prev)) {
      return false;
    }
    prev = sym;
    A->f[sym] = fr;
    A->cum[sym] = acc;
    A->ck_sym[i] = acc;
    A->ck_id[i] = sym;
    acc += fr;
    p += 3;
  }
  if (acc != A->M) {
    return false;
  }
  const uint32_t ncp = (n + kAnsCkptRows - 1) / kAnsCkptRows;
  A->ncp = ReadBE16(p);
  p += 2;
  if (A->ncp != ncp) {
    return false;
  }
  if (p + (static_cast<size_t>(ncp) * 8) + 4 > region_end) {
    return false;
  }
  A->ckpt = p;
  p += static_cast<size_t>(ncp) * 8;
  A->slen = ReadBE32(p);
  p += 4;
  if (p + A->slen > region_end) {
    return false;
  }
  A->stream = reinterpret_cast<const uint8_t*>(p);
  // Checkpoint sanity: states in [L, 256*L), cursors non-increasing (later
  // rows start deeper into the consumed stream) and ckpt[0] at stream end.
  uint32_t prev_pos = A->slen;
  for (uint32_t k = 0; k < ncp; ++k) {
    const uint32_t st = ReadBE32(A->ckpt + (static_cast<size_t>(k) * 8));
    const uint32_t ps = ReadBE32(A->ckpt + (static_cast<size_t>(k) * 8) + 4);
    if (st < kAnsL || st >= (1U << 31) || ps > A->slen) {
      return false;
    }
    if (k == 0 ? ps != A->slen : ps > prev_pos) {
      return false;
    }
    prev_pos = ps;
  }
  return true;
}

// One rANS decode step: recover the symbol from x's low `prec` bits, then
// renormalize by reading stream bytes backwards. Returns false on an
// exhausted stream (hostile/truncated input; callers fail closed).
bool AnsForStep(const AnsForRegion& A, uint32_t* x, uint32_t* pos,
                uint8_t* sym_out) {
  const uint32_t slot = *x & (A.M - 1);
  // Binary search the slot ranges: largest entry with ck_sym <= slot.
  uint32_t lo = 0, hi = A.nsym;
  while (lo + 1 < hi) {
    const uint32_t mid = lo + ((hi - lo) / 2);
    if (A.ck_sym[mid] <= slot) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  const uint8_t sym = A.ck_id[lo];
  *x = (A.f[sym] * (*x >> A.prec)) + slot - A.cum[sym];
  while (*x < kAnsL) {
    if (*pos == 0) {
      return false;
    }
    *x = (*x << 8) | A.stream[--(*pos)];
  }
  *sym_out = sym;
  return true;
}

// Inner-scheme whitelist for the cascaded composites (kFreq exceptions,
// kRleX value/count streams, kDictX code stream, kDeltaSimd's delta stream).
// Leaf schemes only: the whitelist deliberately excludes the composites
// themselves (and the varchar-only/double-only encodings), so a cascade is
// bounded at depth 1 and Int64RegionAt's recursive inner calls can never
// re-enter a cascaded case.
bool InnerSchemeOk(uint8_t s) {
  switch (static_cast<Scheme>(s)) {
    case Scheme::kPlain:
    case Scheme::kConst:
    case Scheme::kRle:
    case Scheme::kForBp:
    case Scheme::kDict:
    case Scheme::kBitWeave:
    // kSimdFor is a LEAF (no inner region of its own), so admitting it keeps
    // the depth-1 bound intact. It is the first non-sequential packer on this
    // list: the others were all the byte-offset / bit-stream packers, and
    // kSimdFor is the 4-lane interleaved one, which is what made the
    // derivative x inner-codec table's sd x (4-lane) cell unavailable.
    case Scheme::kSimdFor:
      return true;
    default:
      return false;
  }
}

// kInterp: parsed region + block decode are defined below (after the PBitS
// helpers); Int64RegionAt's replay arm needs the declarations only.
bool ParseBp128Region(const char* region, uint32_t n, uint32_t param,
                      const char* end, Bp128Region* out) {
  if (region == nullptr || n == 0 || n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const uint32_t block_count = (n + kBp128BlockRows - 1) / kBp128BlockRows;
  if (param != block_count || block_count > kBp128MaxBlocks) {
    return false;
  }
  const size_t fixed = static_cast<size_t>(block_count) * kBp128MetadataBytes;
  size_t available = SIZE_MAX;
  if (end != nullptr) {
    if (end < region) {
      return false;
    }
    available = static_cast<size_t>(end - region);
  }
  if (fixed > available) {
    return false;
  }

  Bp128Region parsed{};
  parsed.n = n;
  parsed.block_count = block_count;
  const char* payload = region + fixed;
  for (uint32_t block = 0; block < block_count; ++block) {
    const char* metadata =
        region + (static_cast<size_t>(block) * kBp128MetadataBytes);
    const uint32_t start = block * kBp128BlockRows;
    Bp128Block& decoded = parsed.blocks[block];
    decoded.rows = std::min<uint32_t>(kBp128BlockRows, n - start);
    decoded.minimum = static_cast<int64_t>(ReadBE64(metadata));
    decoded.maximum = static_cast<int64_t>(ReadBE64(metadata + 8));
    const unsigned __int128 sum_bits =
        (static_cast<unsigned __int128>(ReadBE64(metadata + 16)) << 64) |
        ReadBE64(metadata + 24);
    decoded.sum = static_cast<__int128>(sum_bits);
    decoded.width = static_cast<uint8_t>(metadata[32]);
    const uint64_t range = static_cast<uint64_t>(decoded.maximum) -
                           static_cast<uint64_t>(decoded.minimum);
    if (decoded.minimum > decoded.maximum || decoded.width > 64 ||
        std::cmp_greater(BitsNeeded(range), decoded.width)) {
      return false;
    }
    const size_t packed_bytes =
        (static_cast<size_t>(decoded.rows) * decoded.width + 7) / 8;
    if (packed_bytes > available ||
        (end != nullptr && std::cmp_less(end - payload, packed_bytes))) {
      return false;
    }
    decoded.packed = payload;
    decoded.packed_end = payload + packed_bytes;
    payload += packed_bytes;
    available -= packed_bytes;
  }
  parsed.encoded_size = static_cast<size_t>(payload - region);
  *out = parsed;
  return true;
}

__int128 Bp128BlockSum(const Bp128Block& block) { return block.sum; }

int64_t Bp128ValueAt(const Bp128Region& region, uint32_t index) {
  if (index >= region.n) {
    return 0;
  }
  const uint32_t block_index = index / kBp128BlockRows;
  if (block_index >= region.block_count) {
    return 0;
  }
  const Bp128Block& block = region.blocks[block_index];
  const uint32_t local = index % kBp128BlockRows;
  if (local >= block.rows || block.width == 0) {
    return block.minimum;
  }
  const uint64_t offset =
      UnpackBitAt(block.packed, static_cast<uint64_t>(local) * block.width,
                  static_cast<int>(block.width));
  return static_cast<int64_t>(static_cast<uint64_t>(block.minimum) + offset);
}

#if defined(__AVX2__) && defined(__BMI2__)
// The 64-bit frame: every value is 8-byte aligned, so the block is a plain
// vector load + add. The narrow frames (8/16/32) are the pdep path below;
// the 33..63 frames are the word-parallel path under it.
void DecodeBp128Block64(const Bp128Block& decoded, int64_t* output) {
  const uint32_t rows = decoded.rows;
  const __m256i min_v = _mm256_set1_epi64x(decoded.minimum);
  uint32_t row = 0;
  const char* p = decoded.packed;
  const char* const end = decoded.packed_end;
  for (; row + 4 <= rows; row += 4) {
    if (p + 32 > end) {
      break;
    }
    __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    // The lanes are big-endian in memory: the byte swap is the pshufb
    // pattern the plain decoder uses.
    const __m256i bswap =
        _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
                         7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    v = _mm256_shuffle_epi8(v, bswap);
    v = _mm256_add_epi64(v, min_v);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + row), v);
    p += 32;
  }
  for (; row < rows; ++row) {
    const uint64_t v = ReadBE64(p);
    output[row] =
        static_cast<int64_t>(static_cast<uint64_t>(decoded.minimum) + v);
    p += 8;
  }
}
#endif

void DecodeBp128Block(const Bp128Region& region, uint32_t block,
                      int64_t* output) {
  if (block >= region.block_count) {
    return;
  }
  const Bp128Block& decoded = region.blocks[block];
  uint32_t row = 0;
#if defined(__AVX2__) && defined(__BMI2__)
  if (decoded.width == 64) {
    DecodeBp128Block64(decoded, output);
    return;
  }
  if (decoded.width != 0 && decoded.width <= 32) {
    const uint32_t width = decoded.width;
    const uint32_t lanes = width <= 8 ? 4 : 2;
    const uint64_t field_mask = (1ULL << width) - 1ULL;
    uint64_t select_base = 0;
    for (uint32_t lane = 0; lane < lanes; ++lane) {
      select_base |= field_mask << (64 - (lane + 1) * width);
    }
    const __m256i minimum = _mm256_set1_epi64x(decoded.minimum);
    while (lanes <= decoded.rows - row) {
      const uint64_t bit_offset = static_cast<uint64_t>(row) * width;
      if (width > 16 && (2ULL * width) + (bit_offset & 7) > 64) {
        break;
      }
      const char* const source = decoded.packed + (bit_offset >> 3);
      if (source + 8 > decoded.packed_end) {
        break;
      }
      const uint64_t raw = ReadBE64(source);
      const uint64_t select = select_base >> (bit_offset & 7);
      if (width <= 16) {
        const uint64_t packed = _pext_u64(raw, select);
        if (width <= 8) {
          __m256i values = _mm256_set_epi64x(
              static_cast<long long>(packed & field_mask),
              static_cast<long long>((packed >> width) & field_mask),
              static_cast<long long>((packed >> (2 * width)) & field_mask),
              static_cast<long long>((packed >> (3 * width)) & field_mask));
          values = _mm256_add_epi64(values, minimum);
          _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + row), values);
        } else {
          const __m128i offsets =
              _mm_set_epi64x(static_cast<long long>(packed & field_mask),
                             static_cast<long long>(packed >> width));
          const __m128i decoded_values =
              _mm_add_epi64(offsets, _mm_set1_epi64x(decoded.minimum));
          _mm_storeu_si128(reinterpret_cast<__m128i*>(output + row),
                           decoded_values);
        }
      } else {
        const uint64_t packed = _pext_u64(raw, select);
        const __m128i offsets =
            _mm_set_epi64x(static_cast<long long>(packed & field_mask),
                           static_cast<long long>(packed >> width));
        const __m128i value =
            _mm_add_epi64(offsets, _mm_set1_epi64x(decoded.minimum));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(output + row), value);
      }
      row += lanes;
    }
  }
#endif
  const auto minimum = static_cast<uint64_t>(decoded.minimum);
  if (decoded.width > 32) {
    // The WIDE frames (33..63 bits): the field spans two 64-bit words at an
    // arbitrary bit offset, so the extraction is one 8-byte load plus a
    // shift -- the byte-at-a-time walk this replaces issued one checked
    // byte load per BIT. The field's tail (when it crosses the word
    // boundary) comes from the 9th byte, combined through a 128-bit shift
    // so no shift-by-64 edge exists.
    const uint32_t w = decoded.width;
    const uint64_t mask = (UINT64_C(1) << w) - 1U;
    while (row < decoded.rows) {
      const uint64_t bit_off = static_cast<uint64_t>(row) * w;
      const uint64_t byte_off = bit_off >> 3;
      const auto sh = static_cast<unsigned>(bit_off & 7U);
      // The 8-byte window must be inside the region; the tail rows fall
      // back to the byte-at-a-time walk below.
      if (decoded.packed + byte_off + 8 > decoded.packed_end) {
        break;
      }
      uint64_t w0 = 0;
      std::memcpy(&w0, decoded.packed + byte_off, 8);
      w0 = be64toh(w0);
      uint64_t value = 0;
      if (sh + w <= 64) {
        value = (w0 >> (64U - sh - w)) & mask;
      } else {
        // The field crosses into the 9th byte: the HIGH-order bits of the
        // value live in byte9's TOP bits and the low-order bits in w0's
        // LOW bits. (The window's bit j holds the value's bit
        // w-1+sh-j, so the value's bit f sits at w0's bit 64-w-sh+f --
        // the shift is a LEFT one once the field crosses the word
        // boundary, and the high part comes from byte9's top bits.)
        if (decoded.packed + byte_off + 9 > decoded.packed_end) {
          break;
        }
        const uint32_t h = sh + w - 64U;  // the bits that live in byte9
        // The byte is read as UNSIGNED: the packed bytes are bit patterns,
        // so a signed char would sign-extend.
        const auto byte9 = static_cast<uint64_t>(
            static_cast<unsigned char>(decoded.packed[byte_off + 8]));
        const uint64_t high = (byte9 >> (8U - h)) & ((UINT64_C(1) << h) - 1U);
        const uint64_t low = (w0 << h) & ~((UINT64_C(1) << h) - 1U);
        value = (high | low) & mask;
      }
      output[row] = static_cast<int64_t>(minimum + value);
      ++row;
    }
  }
  for (; row < decoded.rows; ++row) {
    const uint64_t offset =
        UnpackBitAt(decoded.packed, static_cast<uint64_t>(row) * decoded.width,
                    static_cast<int>(decoded.width));
    output[row] = static_cast<int64_t>(minimum + offset);
  }
}

void DecodeBp128All(const Bp128Region& region, int64_t* output) {
  for (uint32_t block = 0; block < region.block_count; ++block) {
    DecodeBp128Block(region, block,
                     output + (static_cast<size_t>(block) * kBp128BlockRows));
  }
}

bool Bp128MetadataMatches(const Bp128Region& region) {
  for (uint32_t block_index = 0; block_index < region.block_count;
       ++block_index) {
    const Bp128Block& block = region.blocks[block_index];
    int64_t minimum = INT64_MAX;
    int64_t maximum = INT64_MIN;
    __int128 sum = 0;
    for (uint32_t row = 0; row < block.rows; ++row) {
      const int64_t value =
          Bp128ValueAt(region, (block_index * kBp128BlockRows) + row);
      minimum = std::min(minimum, value);
      maximum = std::max(maximum, value);
      sum += value;
    }
    if (minimum != block.minimum || maximum != block.maximum ||
        sum != Bp128BlockSum(block)) {
      return false;
    }
  }
  return true;
}

// ---- kFastPfor (scheme 66) -------------------------------------------------
// Block-local PATCHED frame of reference: kBp128's per-block metadata envelope
// plus kPfor's exception patching, which is exactly the combination neither
// scheme offers (kBp128 pays one outlier on every row of its block; kPfor pays
// its global width on every row of the region). See the enum comment in
// pax_chunk.hpp for the wire and the niche. Placed after the Bp128 block so
// the bulk decode can reuse DecodeBp128Block's AVX2 lane kernels directly.
// [min 8B][max 8B][sum 16B][w 1B][nexc 2B]

// The block bitmap as at most two LSB-first row-order words: bit t = local row
// t is an exception. Same load shape as the kBitmap predicate arm (Rle8
// reversal makes the stored MSB-first bytes row-ascending words).

FastPforBitmap FastPforBitmapLoad(const FastPforBlock& blk) {
  FastPforBitmap out;
  const uint32_t bytes = (blk.rows + 7U) / 8U;
  for (uint32_t b = 0; b < bytes; ++b) {
    out.w[b >> 3] |=
        static_cast<uint64_t>(Rle8ReverseByte(static_cast<uint8_t>(blk.bmp[b])))
        << ((b & 7U) * 8U);
  }
  if (const uint32_t tail = blk.rows & 63U; tail != 0) {
    out.w[blk.rows >> 6] &= (UINT64_C(1) << tail) - 1U;
  }
  return out;
}

bool FastPforBitIsException(const FastPforBitmap& bmp, uint32_t t) {
  return ((bmp.w[t >> 6] >> (t & 63U)) & 1U) != 0;
}

// Exceptions strictly before local row t: the popcount rank into the row-order
// exception list. t - 64 stays in [0, 63] on the second word, so the masks
// never shift by 64.
uint32_t FastPforExceptionRank(const FastPforBitmap& bmp, uint32_t t) {
  if (t >= 64U) {
    return static_cast<uint32_t>(__builtin_popcountll(bmp.w[0])) +
           static_cast<uint32_t>(__builtin_popcountll(
               bmp.w[1] & ((UINT64_C(1) << (t - 64U)) - 1U)));
  }
  return static_cast<uint32_t>(
      __builtin_popcountll(bmp.w[0] & ((UINT64_C(1) << t) - 1U)));
}

// Walks the per-block metadata, derives every payload extent, and bounds each
// block against `end` before its pointers are recorded. PINS the structural
// invariants the kernels trust: popcount(bitmap) == nexc and w <= 64. Returns
// the total encoded size, or UINT64_MAX on any violation.
uint64_t FastPforWalk(const char* r, uint32_t n, uint32_t nblk, const char* end,
                      FastPforRegion* out) {
  auto avail = static_cast<size_t>(end - r);
  if (avail < static_cast<size_t>(nblk) * kFastPforMetaBytes) {
    return UINT64_MAX;
  }
  const char* meta = r;
  const char* payload = r + (static_cast<size_t>(nblk) * kFastPforMetaBytes);
  size_t left = avail - (static_cast<size_t>(nblk) * kFastPforMetaBytes);
  uint64_t total = static_cast<uint64_t>(nblk) * kFastPforMetaBytes;
  FastPforRegion& p = *out;
  p.n = n;
  p.block_count = nblk;
  for (uint32_t b = 0; b < nblk; ++b) {
    FastPforBlock& blk = p.blocks[b];
    const uint32_t start = b * kFastPforBlockRows;
    blk.rows = start >= n ? 0 : std::min(kFastPforBlockRows, n - start);
    blk.minimum = static_cast<int64_t>(ReadBE64(meta));
    blk.maximum = static_cast<int64_t>(ReadBE64(meta + 8));
    const unsigned __int128 sum_bits =
        (static_cast<unsigned __int128>(ReadBE64(meta + 16)) << 64) |
        ReadBE64(meta + 24);
    blk.sum = static_cast<__int128>(sum_bits);
    blk.w = static_cast<uint8_t>(meta[32]);
    blk.nexc = ReadBE16(meta + 33);
    if (blk.rows == 0 || blk.w > 64 || blk.minimum > blk.maximum ||
        blk.nexc > blk.rows) {
      return UINT64_MAX;
    }
    const size_t bmp_bytes = (blk.rows + 7U) / 8U;
    const size_t packed_bytes = (static_cast<size_t>(blk.rows) * blk.w + 7) / 8;
    const size_t need =
        bmp_bytes + packed_bytes + (static_cast<size_t>(blk.nexc) * 8);
    if (need > left) {
      return UINT64_MAX;
    }
    blk.bmp = payload;
    blk.packed = payload + bmp_bytes;
    blk.exc = blk.packed + packed_bytes;
    // The popcount pin runs AFTER the pointers are recorded (the load
    // dereferences blk.bmp); on failure the whole region is rejected, so no
    // stale pointer is ever consumed.
    const FastPforBitmap bmp = FastPforBitmapLoad(blk);
    if (std::cmp_not_equal(
            __builtin_popcountll(bmp.w[0]) + __builtin_popcountll(bmp.w[1]),
            blk.nexc)) {
      return UINT64_MAX;
    }
    payload += need;
    left -= need;
    total += need;
    meta += kFastPforMetaBytes;
  }
  p.encoded_size = static_cast<size_t>(total);
  return total;
}

bool ParseFastPforRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, FastPforRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const uint32_t nblk = (n + kFastPforBlockRows - 1) / kFastPforBlockRows;
  if (param != nblk || nblk > kFastPforMaxBlocks) {
    return false;
  }
  return FastPforWalk(r, n, nblk, end, out) != UINT64_MAX;
}

int64_t FastPforValueAt(const FastPforRegion& p, uint32_t idx) {
  if (idx >= p.n) {
    return 0;
  }
  const uint32_t b = idx / kFastPforBlockRows;
  const FastPforBlock& blk = p.blocks[b];
  const uint32_t t = idx - (b * kFastPforBlockRows);
  const FastPforBitmap bmp = FastPforBitmapLoad(blk);
  if (FastPforBitIsException(bmp, t)) {
    return static_cast<int64_t>(ReadBE64(
        blk.exc + (static_cast<size_t>(FastPforExceptionRank(bmp, t)) * 8)));
  }
  const uint64_t off =
      blk.w == 0 ? 0
                 : UnpackBitAt(blk.packed, static_cast<uint64_t>(t) * blk.w,
                               static_cast<int>(blk.w));
  return static_cast<int64_t>(static_cast<uint64_t>(blk.minimum) + off);
}

// One block's lanes (AVX2 kernels where compiled in) plus the bitmap-driven
// exception patch. Shared by the bulk decode, the predicate mask, and the
// count/sum kernel's boundary blocks.
void FastPforDecodeBlock(const FastPforBlock& blk, int64_t* dst) {
  if (blk.w == 0) {
    // Every regular offset is 0: the lanes carry no bits at all.
    for (uint32_t t = 0; t < blk.rows; ++t) {
      dst[t] = blk.minimum;
    }
  } else {
    Bp128Region view;
    view.block_count = 1;
    Bp128Block& leaf = view.blocks[0];
    leaf.packed = blk.packed;
    leaf.packed_end =
        blk.packed + ((static_cast<size_t>(blk.rows) * blk.w + 7) / 8);
    leaf.rows = blk.rows;
    leaf.width = blk.w;
    leaf.minimum = blk.minimum;
    leaf.maximum = blk.maximum;
    leaf.sum = blk.sum;
    DecodeBp128Block(view, 0, dst);
  }
  // Patch exceptions over the lanes (their lane words hold low bits only).
  const FastPforBitmap bmp = FastPforBitmapLoad(blk);
  uint32_t rank = 0;
  for (uint32_t word = 0; word < 2; ++word) {
    uint64_t bits = bmp.w[word];
    while (bits != 0) {
      const uint32_t t =
          (word * 64U) + static_cast<uint32_t>(__builtin_ctzll(bits));
      bits &= bits - 1;
      if (t >= blk.rows) {
        break;  // dead (the load masks the tail); defense
      }
      dst[t] = static_cast<int64_t>(
          ReadBE64(blk.exc + (static_cast<size_t>(rank) * 8)));
      ++rank;
    }
  }
}

// One parse, then a per-block lane decode (kBp128Block's AVX2 kernels where
// compiled in) plus a bitmap-driven exception patch.
void DecodeFastPforAll(const FastPforRegion& p, int64_t* out) {
  for (uint32_t b = 0; b < p.block_count; ++b) {
    FastPforDecodeBlock(p.blocks[b],
                        out + (static_cast<size_t>(b) * kFastPforBlockRows));
  }
}

// Full-replay metadata check, the kBp128 fallback for chunks whose metadata
// was not set trusted at build/validate time. Every row is reconstructed
// (min + lane bits for regular rows, the stored exception values otherwise)
// and the block's min/max/sum brackets are matched exactly -- the same
// entitlement ValidateWalk's case 66 grants the bracket kernels on validated
// chunks. A regular-vs-exception misclassification decodes truncated and
// fails this match; the exception ranks are consumed exactly nexc times by
// construction (the parse pinned popcount(bitmap) == nexc).
bool FastPforMetadataMatches(const FastPforRegion& p) {
  for (uint32_t b = 0; b < p.block_count; ++b) {
    const FastPforBlock& blk = p.blocks[b];
    int64_t mn = INT64_MAX;
    int64_t mx = INT64_MIN;
    __int128 sum = 0;
    const FastPforBitmap bmp = FastPforBitmapLoad(blk);
    uint32_t rank = 0;
    for (uint32_t t = 0; t < blk.rows; ++t) {
      int64_t v = 0;
      if (FastPforBitIsException(bmp, t)) {
        v = static_cast<int64_t>(
            ReadBE64(blk.exc + (static_cast<size_t>(rank) * 8)));
        ++rank;
      } else {
        const uint64_t off =
            blk.w == 0
                ? 0
                : UnpackBitAt(blk.packed, static_cast<uint64_t>(t) * blk.w,
                              static_cast<int>(blk.w));
        v = static_cast<int64_t>(static_cast<uint64_t>(blk.minimum) + off);
      }
      mn = std::min(mn, v);
      mx = std::max(mx, v);
      sum += v;
    }
    if (mn != blk.minimum || mx != blk.maximum || sum != blk.sum) {
      return false;
    }
  }
  return true;
}

// ---- kDeltaFastPfor (scheme 67) ---------------------------------------------
// Block-local PATCHED frame of reference over FIRST DIFFERENCES: the last
// cell of the (transform x locality x patching) table. kDeltaPfor (37)
// transforms but keeps kPfor's GLOBAL width; kS4 (55) transforms and
// localizes but has no patching and no brackets (one huge increment
// re-widens every delta of its block, and its predicates have nothing to
// prune with); kFastPfor (66) patches and localizes but frames RAW values,
// so a cumulative series pays its per-block climb in width. This scheme
// frames the first differences per 128-row block, patches delta spikes as
// (position bitmap + full delta), and stores the block's true value
// min/max/sum so the FastPFOR bracket kernels work unchanged. See the enum
// comment in pax_chunk.hpp for the wire. Placed after the FastPfor block so
// the bulk decode can reuse DecodeBp128Block's AVX2 lane kernels for the
// delta lanes.
// [base 8B][min 8B][max 8B][sum 16B][w 1B][nexc 2B][dmin 8B]

// The block bitmap as at most two LSB-first delta-order words: bit t = delta
// t (row t+1's first difference) is an exception. Same load shape as
// FastPforBitmap (Rle8 reversal makes the stored MSB-first bytes
// row-ascending words); at most 127 bits are live.

DeltaFastPforBitmap DeltaFastPforBitmapLoad(const DeltaFastPforBlock& blk) {
  DeltaFastPforBitmap out;
  const uint32_t deltas = blk.rows - 1;
  const uint32_t bytes = (deltas + 7U) / 8U;
  for (uint32_t b = 0; b < bytes; ++b) {
    out.w[b >> 3] |=
        static_cast<uint64_t>(Rle8ReverseByte(static_cast<uint8_t>(blk.bmp[b])))
        << ((b & 7U) * 8U);
  }
  if (const uint32_t tail = deltas & 63U; tail != 0) {
    out.w[deltas >> 6] &= (UINT64_C(1) << tail) - 1U;
  }
  return out;
}

bool DeltaFastPforBitIsException(const DeltaFastPforBitmap& bmp, uint32_t t) {
  return ((bmp.w[t >> 6] >> (t & 63U)) & 1U) != 0;
}

// Exceptions strictly before delta t: the popcount rank into the row-order
// exception list. t - 64 stays in [0, 63] on the second word, so the masks
// never shift by 64.
uint32_t DeltaFastPforExceptionRank(const DeltaFastPforBitmap& bmp,
                                    uint32_t t) {
  if (t >= 64U) {
    return static_cast<uint32_t>(__builtin_popcountll(bmp.w[0])) +
           static_cast<uint32_t>(__builtin_popcountll(
               bmp.w[1] & ((UINT64_C(1) << (t - 64U)) - 1U)));
  }
  return static_cast<uint32_t>(
      __builtin_popcountll(bmp.w[0] & ((UINT64_C(1) << t) - 1U)));
}

// Walks the per-block metadata, derives every payload extent, and bounds
// each block against `end` before its pointers are recorded. PINS the
// structural invariants the kernels trust: popcount(bitmap) == nexc,
// w <= 64, brackets ordered, base inside its bracket. Returns the total
// encoded size, or UINT64_MAX on any violation.
uint64_t DeltaFastPforWalk(const char* r, uint32_t n, uint32_t nblk,
                           const char* end, DeltaFastPforRegion* out) {
  auto avail = static_cast<size_t>(end - r);
  if (avail < static_cast<size_t>(nblk) * kDeltaFastPforMetaBytes) {
    return UINT64_MAX;
  }
  const char* meta = r;
  const char* payload =
      r + (static_cast<size_t>(nblk) * kDeltaFastPforMetaBytes);
  size_t left = avail - (static_cast<size_t>(nblk) * kDeltaFastPforMetaBytes);
  uint64_t total = static_cast<uint64_t>(nblk) * kDeltaFastPforMetaBytes;
  DeltaFastPforRegion& p = *out;
  p.n = n;
  p.block_count = nblk;
  for (uint32_t b = 0; b < nblk; ++b) {
    DeltaFastPforBlock& blk = p.blocks[b];
    const uint32_t start = b * kDeltaFastPforBlockRows;
    blk.rows = start >= n ? 0 : std::min(kDeltaFastPforBlockRows, n - start);
    if (blk.rows == 0) {
      return UINT64_MAX;
    }
    blk.base = static_cast<int64_t>(ReadBE64(meta));
    blk.minimum = static_cast<int64_t>(ReadBE64(meta + 8));
    blk.maximum = static_cast<int64_t>(ReadBE64(meta + 16));
    const unsigned __int128 sum_bits =
        (static_cast<unsigned __int128>(ReadBE64(meta + 24)) << 64) |
        ReadBE64(meta + 32);
    blk.sum = static_cast<__int128>(sum_bits);
    blk.w = static_cast<uint8_t>(meta[40]);
    blk.nexc = ReadBE16(meta + 41);
    blk.dmin = ReadBE64(meta + 43);
    // The base row is a block value, so the TRUE brackets must hold it; a
    // hostile footer that moves the base outside them would replay values
    // the bracket kernels would mis-trust (the full replay in
    // DeltaFastPforMetadataMatches catches this too, fail closed here).
    if (blk.w > 64 || blk.minimum > blk.maximum || blk.base < blk.minimum ||
        blk.base > blk.maximum) {
      return UINT64_MAX;
    }
    const uint32_t deltas = blk.rows - 1;
    if (blk.nexc > deltas) {
      return UINT64_MAX;
    }
    const size_t bmp_bytes = (deltas + 7U) / 8U;
    const size_t packed_bytes = (static_cast<size_t>(deltas) * blk.w + 7) / 8;
    const size_t need =
        bmp_bytes + packed_bytes + (static_cast<size_t>(blk.nexc) * 8);
    if (need > left) {
      return UINT64_MAX;
    }
    blk.bmp = payload;
    blk.packed = payload + bmp_bytes;
    blk.exc = blk.packed + packed_bytes;
    // The popcount pin runs AFTER the pointers are recorded (the load
    // dereferences blk.bmp); on failure the whole region is rejected, so no
    // stale pointer is ever consumed.
    const DeltaFastPforBitmap bmp = DeltaFastPforBitmapLoad(blk);
    if (std::cmp_not_equal(
            __builtin_popcountll(bmp.w[0]) + __builtin_popcountll(bmp.w[1]),
            blk.nexc)) {
      return UINT64_MAX;
    }
    payload += need;
    left -= need;
    total += need;
    meta += kDeltaFastPforMetaBytes;
  }
  p.encoded_size = static_cast<size_t>(total);
  return total;
}

bool ParseDeltaFastPforRegion(const char* r, uint32_t n, uint32_t param,
                              const char* end, DeltaFastPforRegion* out) {
  if (r == nullptr || end == nullptr || out == nullptr || end < r || n == 0 ||
      n > PaxChunk::kRowGroupSize) {
    return false;
  }
  const uint32_t nblk =
      (n + kDeltaFastPforBlockRows - 1) / kDeltaFastPforBlockRows;
  if (param != nblk || nblk > kDeltaFastPforMaxBlocks) {
    return false;
  }
  return DeltaFastPforWalk(r, n, nblk, end, out) != UINT64_MAX;
}

// Replays one block's value sequence into dst[0..rows): v_0 = base, then
// v_t = v_{t-1} + delta_t with exception deltas patched from the list and
// regular deltas read as dmin + lane. The delta lanes unpack through
// DecodeBp128Block's AVX2 kernels where compiled in (a one-block Bp128Region
// view over the rows-1 deltas; that kernel never reads the view's
// maximum/sum, so they stay unset). The replay itself is an unsigned-wrapping
// sequential chain -- the one thing a delta transform cannot vectorize away.
void DeltaFastPforDecodeBlock(const DeltaFastPforBlock& blk, int64_t* dst) {
  const uint32_t deltas = blk.rows - 1;
  std::array<int64_t, kDeltaFastPforBlockRows> delta{};
  if (blk.w == 0) {
    // Every regular delta is dmin itself: the lanes carry no bits at all.
    const auto dmin = static_cast<int64_t>(blk.dmin);
    for (uint32_t t = 0; t < deltas; ++t) {
      delta[t] = dmin;
    }
  } else {
    Bp128Region view;
    view.block_count = 1;
    Bp128Block& leaf = view.blocks[0];
    leaf.packed = blk.packed;
    leaf.packed_end =
        blk.packed + ((static_cast<size_t>(deltas) * blk.w + 7) / 8);
    leaf.rows = deltas;
    leaf.width = blk.w;
    leaf.minimum = static_cast<int64_t>(blk.dmin);
    DecodeBp128Block(view, 0, delta.data());
  }
  // Patch exception deltas over the lanes (their lane words hold low bits
  // only).
  const DeltaFastPforBitmap bmp = DeltaFastPforBitmapLoad(blk);
  uint32_t rank = 0;
  for (uint32_t word = 0; word < 2; ++word) {
    uint64_t bits = bmp.w[word];
    while (bits != 0) {
      const uint32_t t =
          (word * 64U) + static_cast<uint32_t>(__builtin_ctzll(bits));
      bits &= bits - 1;
      if (t >= deltas) {
        break;  // dead (the load masks the tail); defense
      }
      delta[t] = static_cast<int64_t>(
          ReadBE64(blk.exc + (static_cast<size_t>(rank) * 8)));
      ++rank;
    }
  }
  auto v = static_cast<uint64_t>(blk.base);
  dst[0] = static_cast<int64_t>(v);
  for (uint32_t t = 0; t < deltas; ++t) {
    v += static_cast<uint64_t>(delta[t]);
    dst[t + 1] = static_cast<int64_t>(v);
  }
}

// One parse, then a per-block lane decode (kBp128Block's AVX2 kernels where
// compiled in), an exception patch, and the in-block replay.
void DecodeDeltaFastPforAll(const DeltaFastPforRegion& p, int64_t* out) {
  for (uint32_t b = 0; b < p.block_count; ++b) {
    DeltaFastPforDecodeBlock(
        p.blocks[b], out + (static_cast<size_t>(b) * kDeltaFastPforBlockRows));
  }
}

int64_t DeltaFastPforValueAt(const DeltaFastPforRegion& p, uint32_t idx) {
  if (idx >= p.n) {
    return 0;
  }
  const uint32_t b = idx / kDeltaFastPforBlockRows;
  const DeltaFastPforBlock& blk = p.blocks[b];
  const uint32_t t = idx - (b * kDeltaFastPforBlockRows);
  auto v = static_cast<uint64_t>(blk.base);
  if (t == 0) {
    return static_cast<int64_t>(v);
  }
  // Unpack the block's delta lanes ONCE (DecodeBp128Block's AVX2 kernels
  // where compiled in) and replay to t: per-delta byte-window extraction
  // measures ~3x the probe cost of one batched unpack plus the same
  // <= 127 dependent adds. Exception deltas hold garbage in the unpacked
  // lanes (their low bits + dmin), so the replay takes them from the
  // exception list by popcount rank.
  const uint32_t deltas = blk.rows - 1;
  std::array<int64_t, kDeltaFastPforBlockRows> delta{};
  if (blk.w == 0) {
    const auto dmin = static_cast<int64_t>(blk.dmin);
    for (uint32_t d = 0; d < deltas; ++d) {
      delta[d] = dmin;
    }
  } else {
    Bp128Region view;
    view.block_count = 1;
    Bp128Block& leaf = view.blocks[0];
    leaf.packed = blk.packed;
    leaf.packed_end =
        blk.packed + ((static_cast<size_t>(deltas) * blk.w + 7) / 8);
    leaf.rows = deltas;
    leaf.width = blk.w;
    leaf.minimum = static_cast<int64_t>(blk.dmin);
    DecodeBp128Block(view, 0, delta.data());
  }
  const DeltaFastPforBitmap bmp = DeltaFastPforBitmapLoad(blk);
  for (uint32_t d = 0; d < t; ++d) {
    if (DeltaFastPforBitIsException(bmp, d)) {
      v += ReadBE64(
          blk.exc +
          (static_cast<size_t>(DeltaFastPforExceptionRank(bmp, d)) * 8));
    } else {
      v += static_cast<uint64_t>(delta[d]);
    }
  }
  return static_cast<int64_t>(v);
}

// Full-replay metadata check, the FastPFOR fallback for chunks whose metadata
// was not set trusted at build/validate time. Every block is replayed through
// DeltaFastPforDecodeBlock (the same path the kernels take) and the stored
// brackets matched exactly -- the entitlement ValidateWalk's case 67 grants
// the bracket kernels on validated chunks. A regular-vs-exception
// misclassification decodes truncated and fails this match; the exception
// ranks are consumed exactly nexc times by construction (the parse pinned
// popcount(bitmap) == nexc), and a base outside the stored brackets fails
// the minimum/maximum match.
bool DeltaFastPforMetadataMatches(const DeltaFastPforRegion& p) {
  for (uint32_t b = 0; b < p.block_count; ++b) {
    const DeltaFastPforBlock& blk = p.blocks[b];
    std::array<int64_t, kDeltaFastPforBlockRows> decoded{};
    DeltaFastPforDecodeBlock(blk, decoded.data());
    int64_t mn = INT64_MAX;
    int64_t mx = INT64_MIN;
    __int128 sum = 0;
    for (uint32_t t = 0; t < blk.rows; ++t) {
      mn = std::min(mn, decoded[t]);
      mx = std::max(mx, decoded[t]);
      sum += decoded[t];
    }
    if (mn != blk.minimum || mx != blk.maximum || sum != blk.sum) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb::pax_detail
