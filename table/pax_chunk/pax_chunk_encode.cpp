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
#include <utility>

#include "table/pax_chunk/pax_chunk.hpp"
#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb {
namespace pax_detail {

// Benchmark A/B hook: TINYLAMB_FORCE_PLAIN=1 bypasses the chooser entirely so
// the same dataset can be measured with and without lightweight encodings.
bool ForcePlain() { return std::getenv("TINYLAMB_FORCE_PLAIN") != nullptr; }

EncodedCol EncConstI64(int64_t v) {
  EncodedCol e;
  e.scheme = Scheme::kConst;
  AppendI64(e.buf, v);
  return e;
}

EncodedCol EncRleI64(const std::vector<int64_t>& vals, size_t budget) {
  // NOTE: an all-distinct hint check was measured here and REJECTED: with
  // the early return in place before CountRuns, bench_tpch paid +40% total
  // instructions (the dictionary-family encoders downstream re-ran their
  // full attempts), despite identical chunk bytes -- leave the pricing
  // pass alone.
  // Price the region before materialising it: 4-byte run count + 12 B/run.
  const size_t predicted_runs = CountRuns(vals);
  if (4 + (12 * predicted_runs) > budget) {
    return {};
  }
  std::vector<std::pair<int64_t, uint32_t>> runs;
  runs.reserve(predicted_runs);
  for (int64_t v : vals) {
    if (!runs.empty() && runs.back().first == v) {
      ++runs.back().second;
    } else {
      runs.emplace_back(v, 1);
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kRle;
  e.param = static_cast<uint32_t>(runs.size());
  AppendBE32(e.buf, static_cast<uint32_t>(runs.size()));
  for (const auto& [v, len] : runs) {
    AppendI64(e.buf, v);
    AppendBE32(e.buf, len);
  }
  return e;
}

EncodedCol EncRleBpI64(const std::vector<int64_t>& vals, size_t budget,
                       int known_distinct) {
  if (vals.size() < 2 || vals.size() > UINT32_MAX) {
    return {};
  }
  // runs == vals.size() is rejected after the run loop; the profiled
  // distinct count proves that shape up front.
  if (known_distinct > 0 &&
      static_cast<size_t>(known_distinct) == vals.size()) {
    return {};
  }
  std::vector<std::pair<int64_t, uint32_t>> runs;
  runs.reserve((vals.size() / 2) + 1);
  for (int64_t value : vals) {
    if (!runs.empty() && runs.back().first == value) {
      ++runs.back().second;
    } else {
      runs.emplace_back(value, 1);
    }
  }
  if (runs.size() < 2 || runs.size() == vals.size()) {
    return {};
  }
  int64_t minimum = runs.front().first;
  for (const auto& [value, length] : runs) {
    (void)length;
    minimum = std::min(minimum, value);
  }
  uint64_t max_offset = 0;
  uint32_t max_length = 0;
  for (const auto& [value, length] : runs) {
    max_offset = std::max(max_offset, static_cast<uint64_t>(value) -
                                          static_cast<uint64_t>(minimum));
    max_length = std::max(max_length, length);
  }
  const int value_width = BitsNeeded(max_offset);
  const int length_width = BitsNeeded(max_length);
  if (value_width <= 0 || value_width > 64 || length_width <= 0 ||
      length_width > 32) {
    return {};
  }
  // Exact region size: base + both widths + run count + two packed streams.
  if (14 + ((runs.size() * static_cast<size_t>(value_width) + 7) / 8) +
          ((runs.size() * static_cast<size_t>(length_width) + 7) / 8) >
      budget) {
    return {};
  }
  std::vector<uint64_t> value_offsets;
  std::vector<uint64_t> lengths;
  value_offsets.reserve(runs.size());
  lengths.reserve(runs.size());
  for (const auto& [value, length] : runs) {
    value_offsets.push_back(static_cast<uint64_t>(value) -
                            static_cast<uint64_t>(minimum));
    lengths.push_back(length);
  }
  EncodedCol e;
  e.scheme = Scheme::kRleBp;
  e.param = static_cast<uint32_t>(runs.size());
  AppendI64(e.buf, minimum);
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(value_width)));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(length_width)));
  AppendBE32(e.buf, e.param);
  AppendPackBits(e.buf, value_offsets, value_width);
  AppendPackBits(e.buf, lengths, length_width);
  return e;
}

// Byte-aligned FOR-BA payload writer shared by kRleIdx's two streams: an
// 8-byte BE base + a 1-byte width + `count` little-endian offsets. `base` is
// the stream's own origin (the FOR-BA value it re-adds on decode), NOT the
// first offset.
void AppendForBaStream(std::vector<char>& buf, uint64_t base,
                       const std::vector<uint64_t>& offs, uint32_t bpv) {
  AppendI64(buf, static_cast<int64_t>(base));
  buf.push_back(static_cast<char>(static_cast<uint8_t>(bpv)));
  const size_t off0 = buf.size();
  BufTail(buf, static_cast<size_t>(bpv) * offs.size());
  char* wp = buf.data() + off0;
  for (const uint64_t off : offs) {
    for (uint32_t b = 0; b < bpv; ++b) {
      *wp++ = static_cast<char>((off >> (b * 8U)) & 0xFF);
    }
  }
}

// Width of a byte-aligned FOR-BA stream covering offsets [0, max_offset].
uint32_t ForBaStreamWidth(uint64_t max_offset) {
  uint32_t bpv = 1;
  while (bpv < 8 && (max_offset >> (bpv * 8U)) != 0) {
    ++bpv;
  }
  return bpv;
}

// kRleIdx (scheme 49): monotone run-length with a 16-run restart index.
//   [runs u32 BE][nidx u32 BE]
//   [values FOR-BA: base 8B BE, bpv u8, runs*bpv LE]
//   [lengths FOR-BA: base 8B BE, bpv u8, runs*bpv LE]
//   [restart: nidx x u32 BE]   entry e = row index of run 16*e
// The non-decreasing gate is what makes the run values sorted, which is in turn
// what lets every read path binary search instead of walking runs.
EncodedCol EncRleIdxI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.size() < 2 || vals.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  std::vector<int64_t> run_vals;
  std::vector<uint32_t> run_lens;
  run_vals.reserve((vals.size() / 2) + 1);
  run_lens.reserve((vals.size() / 2) + 1);
  for (int64_t value : vals) {
    // Non-decreasing gate (see the function comment): the read paths binary
    // search the run values, which is only sound when they are sorted.
    if (!run_vals.empty() && value < run_vals.back()) {
      return {};
    }
    if (!run_vals.empty() && run_vals.back() == value) {
      ++run_lens.back();
    } else {
      run_vals.push_back(value);
      run_lens.push_back(1);
    }
  }
  const auto runs = static_cast<uint32_t>(run_vals.size());
  if (runs < 2) {
    return {};
  }
  const uint32_t nidx =
      (runs + kRleIdxRestartStride - 1) / kRleIdxRestartStride;
  int64_t value_base = run_vals.front();
  uint64_t length_base = run_lens.front();
  uint64_t max_value_offset = 0;
  uint64_t max_length_offset = 0;
  for (uint32_t i = 0; i < runs; ++i) {
    max_value_offset =
        std::max(max_value_offset, static_cast<uint64_t>(run_vals[i]) -
                                       static_cast<uint64_t>(value_base));
    max_length_offset = std::max(
        max_length_offset, static_cast<uint64_t>(run_lens[i]) - length_base);
  }
  const uint32_t value_bpv = ForBaStreamWidth(max_value_offset);
  const uint32_t length_bpv = ForBaStreamWidth(max_length_offset);
  std::vector<uint64_t> value_offsets;
  std::vector<uint64_t> length_offsets;
  value_offsets.reserve(runs);
  length_offsets.reserve(runs);
  for (uint32_t i = 0; i < runs; ++i) {
    value_offsets.push_back(static_cast<uint64_t>(run_vals[i]) -
                            static_cast<uint64_t>(value_base));
    length_offsets.push_back(static_cast<uint64_t>(run_lens[i]) - length_base);
  }
  // Exact region size: 8-byte header + the two FOR-BA streams + the restart
  // table. No per-run floor applies (both streams may collapse to 8+1 bytes).
  const size_t size = 8 + (9 + static_cast<size_t>(runs) * value_bpv) +
                      (9 + static_cast<size_t>(runs) * length_bpv) +
                      (static_cast<size_t>(nidx) * 4);
  if (size > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kRleIdx;
  e.param = runs;
  AppendBE32(e.buf, runs);
  AppendBE32(e.buf, nidx);
  AppendForBaStream(e.buf, static_cast<uint64_t>(value_base), value_offsets,
                    value_bpv);
  AppendForBaStream(e.buf, length_base, length_offsets, length_bpv);
  uint32_t row = 0;
  for (uint32_t idx_entry = 0; idx_entry < nidx; ++idx_entry) {
    const uint32_t run = idx_entry * kRleIdxRestartStride;
    if (run != 0) {
      for (uint32_t i = 0; i < kRleIdxRestartStride; ++i) {
        row += run_lens[run - kRleIdxRestartStride + i];
      }
    }
    AppendBE32(e.buf, row);
  }
  return e;
}

// kStreamVByte (scheme 50): byte-aligned variable-byte with a separate control
// plane and a 16-value block index.
//   [stream_len u32 BE][nblk u16 BE][block_off nblk x u16 BE]
//   [ctrl: ceil(n/4) bytes][payload: stream_len bytes]
// Each value contributes a 2-bit width selector (0..3 => 1..4 bytes) to its
// control byte and its little-endian zigzag, truncated to that width, to the
// payload. Exact size: 6 + 2*ceil(n/16) + ceil(n/4) + sum(widths).
//
// Applicability: every value must zigzag into 32 bits, which is exactly the
// int32 range [-2^31, 2^31-1] (INT32_MIN included -- zigzag(INT32_MIN) is
// 2^32-1, the largest value that still fits). Past that a fixed-width codec is
// no longer competitive anyway, so bailing costs the size race nothing -- and
// it is what keeps every width a single selector, hence the exact extent
// ValidateWalk can pin without a full decode. Zigzag (not the raw value) is
// what makes one scheme cover signed columns: the low w bytes of zigzag(v)
// determine v exactly, for either sign.
EncodedCol EncStreamVByteI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty() || vals.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  const auto n = static_cast<uint32_t>(vals.size());
  const uint32_t nblk = (n + kStreamVByteBlock - 1) / kStreamVByteBlock;
  const uint32_t ctrl_len = (n + kStreamVBytePerCtrl - 1) / kStreamVBytePerCtrl;
  // Adaptive selection re-runs this encoder for every row group, so the
  // control-plane scratch is thread_local (grown once) instead of paying a
  // malloc pair per attempt.
  thread_local std::vector<uint8_t> ctrl, payload, block_off;
  ctrl.assign(ctrl_len, 0);
  payload.clear();
  payload.reserve(static_cast<size_t>(n) * 2);
  block_off.assign(static_cast<size_t>(nblk) * 2, 0);
  for (uint32_t i = 0; i < n; ++i) {
    if (i % kStreamVByteBlock == 0) {
      const size_t b = i / kStreamVByteBlock;
      const auto at = static_cast<uint32_t>(payload.size());
      block_off[b * 2] = static_cast<uint8_t>(at >> 8);
      block_off[(b * 2) + 1] = static_cast<uint8_t>(at & 0xFF);
    }
    const uint64_t z = StreamVByteZigZag(vals[i]);
    // > 4 bytes: the value cannot be represented (see the function comment).
    if (z > 0xFFFFFFFFULL) {
      return {};
    }
    uint32_t w = 1;
    while (w < kStreamVByteMaxWidth && (z >> (8 * w)) != 0) {
      ++w;
    }
    ctrl[i / kStreamVBytePerCtrl] = static_cast<uint8_t>(
        ctrl[i / kStreamVBytePerCtrl] |
        static_cast<uint8_t>((w - 1) << (6 - 2 * (i % kStreamVBytePerCtrl))));
    for (uint32_t b = 0; b < w; ++b) {
      payload.push_back(static_cast<uint8_t>((z >> (8 * b)) & 0xFF));
    }
    // Every remaining value still emits >= 1 payload byte, so when the
    // bytes written plus that minimum cross the budget the final size (which
    // only adds the header, control and offset terms) cannot fit. Checked
    // every 32 rows to keep the steady-state branch rare.
    if ((i & 31) == 0 && payload.size() + (n - i) > budget) {
      return {};
    }
  }
  const size_t size = 6 + (static_cast<size_t>(nblk) * 2) +
                      static_cast<size_t>(ctrl_len) + payload.size();
  if (size > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kStreamVByte;
  e.param = static_cast<uint32_t>(payload.size());
  AppendBE32(e.buf, static_cast<uint32_t>(payload.size()));
  AppendBE16(e.buf, static_cast<uint16_t>(nblk));
  e.buf.insert(e.buf.end(), block_off.begin(), block_off.end());
  e.buf.insert(e.buf.end(), ctrl.begin(), ctrl.end());
  e.buf.insert(e.buf.end(), payload.begin(), payload.end());
  return e;
}

EncodedCol EncRle8I64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty() || vals.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  // Each plane is run-length coded independently (1B value + 2B length per
  // run, 2B run count). Plane-at-a-time costs one extra streaming pass over a
  // row-group-sized (L1-resident) array but lets an over-budget column stop
  // after the first plane instead of after all eight.
  std::array<std::vector<std::pair<uint8_t, uint16_t>>, 8> planes;
  uint32_t total_runs = 0;
  for (auto& runs : planes) {
    runs.reserve((vals.size() / 4) + 1);
  }
  for (int64_t value : vals) {
    const uint64_t ordered = static_cast<uint64_t>(value) ^ (1ULL << 63);
    // One pass feeds all eight planes: the per-plane sweep paid the zigzag
    // xor and the loop bookkeeping eight times per row, and its budget
    // check ran only once per plane.
    for (uint32_t plane = 0; plane < 8; ++plane) {
      const auto byte =
          static_cast<uint8_t>((ordered >> (56U - 8U * plane)) & 0xFFU);
      auto& runs = planes[plane];
      if (!runs.empty() && runs.back().first == byte) {
        ++runs.back().second;
      } else {
        runs.emplace_back(byte, static_cast<uint16_t>(1));
        ++total_runs;
        // Monotone: the final total only grows, so once the running sum is
        // over budget the whole region is. Same bound the per-plane sweep
        // applied (16 + 3 * total_runs), checked per new run instead of per
        // completed plane -- strictly earlier abort, and regions that fit
        // the budget still walk the identical bytes.
        if (total_runs >= 8 && 16 + (3ULL * total_runs) > budget) {
          return {};
        }
      }
    }
  }
  if (total_runs < 8 || total_runs > 8U * PaxChunk::kRowGroupSize) {
    return {};
  }
  EncodedCol encoded;
  encoded.scheme = Scheme::kRle8;
  encoded.param = total_runs;
  for (const auto& plane : planes) {
    AppendBE16(encoded.buf, static_cast<uint16_t>(plane.size()));
    for (const auto& [value, length] : plane) {
      encoded.buf.push_back(static_cast<char>(value));
      AppendBE16(encoded.buf, length);
    }
  }
  return encoded;
}

bool Rle8AdaptiveApplicable(const std::vector<int64_t>& vals,
                            const EncodedCol& encoded) {
  if (vals.size() < 8 || vals.size() > PaxChunk::kRowGroupSize) {
    return false;
  }
  if (encoded.buf.empty()) {
    return false;
  }
  if (encoded.buf.size() * 5 > vals.size() * 8 * 4) {
    return false;
  }
  uint32_t stable_planes = 0;
  for (uint32_t plane = 0; plane < 8; ++plane) {
    uint32_t runs = 0;
    uint8_t previous = 0;
    bool have_previous = false;
    for (int64_t value : vals) {
      const uint64_t ordered = static_cast<uint64_t>(value) ^ (1ULL << 63);
      const auto byte =
          static_cast<uint8_t>((ordered >> (56U - 8U * plane)) & 0xFFU);
      if (!have_previous || byte != previous) {
        ++runs;
        previous = byte;
        have_previous = true;
      }
    }
    if (static_cast<size_t>(runs) * 2 <= vals.size()) {
      ++stable_planes;
    }
  }
  return stable_planes >= 2;
}

EncodedCol EncRleDeltaI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty() || vals.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  std::vector<std::pair<int64_t, uint32_t>> runs;
  runs.reserve((vals.size() / 4) + 1);
  for (int64_t value : vals) {
    if (!runs.empty() && runs.back().first == value) {
      ++runs.back().second;
    } else {
      runs.emplace_back(value, 1);
    }
  }
  uint64_t max_zigzag = 0;
  for (size_t i = 1; i < runs.size(); ++i) {
    const uint64_t diff = static_cast<uint64_t>(runs[i].first) -
                          static_cast<uint64_t>(runs[i - 1].first);
    const uint64_t zigzag = EncodeZigZag64(diff);
    max_zigzag = std::max(max_zigzag, zigzag);
  }
  uint32_t max_length = 0;
  for (const auto& run : runs) {
    max_length = std::max(max_length, run.second);
  }
  const uint32_t value_width =
      std::max<uint32_t>(1, static_cast<uint32_t>(BitsNeeded(max_zigzag)));
  const uint32_t length_width =
      std::max<uint32_t>(1, static_cast<uint32_t>(BitsNeeded(max_length)));
  if (value_width > 64 || length_width > 32) {
    return {};
  }
  // Exact region size: base + both widths + run count + two packed streams.
  if (14 + (((runs.size() - 1) * value_width + 7) / 8) +
          ((runs.size() * static_cast<size_t>(length_width) + 7) / 8) >
      budget) {
    return {};
  }

  std::vector<uint64_t> deltas;
  deltas.reserve(runs.empty() ? 0 : runs.size() - 1);
  for (size_t i = 1; i < runs.size(); ++i) {
    const uint64_t diff = static_cast<uint64_t>(runs[i].first) -
                          static_cast<uint64_t>(runs[i - 1].first);
    deltas.push_back(EncodeZigZag64(diff));
  }
  std::vector<uint64_t> lengths;
  lengths.reserve(runs.size());
  for (const auto& run : runs) {
    lengths.push_back(run.second);
  }

  EncodedCol encoded;
  encoded.scheme = Scheme::kRleDelta;
  encoded.param = static_cast<uint32_t>(runs.size());
  AppendI64(encoded.buf, runs.front().first);
  encoded.buf.push_back(static_cast<char>(static_cast<uint8_t>(value_width)));
  encoded.buf.push_back(static_cast<char>(static_cast<uint8_t>(length_width)));
  AppendBE32(encoded.buf, encoded.param);
  AppendPackBits(encoded.buf, deltas, static_cast<int>(value_width));
  AppendPackBits(encoded.buf, lengths, static_cast<int>(length_width));
  return encoded;
}

bool RleDeltaAdaptiveApplicable(const std::vector<int64_t>& vals,
                                const EncodedCol& encoded) {
  if (vals.size() < 4 || vals.size() > PaxChunk::kRowGroupSize) {
    return false;
  }
  size_t runs = 0;
  for (size_t i = 0; i < vals.size(); ++i) {
    if (i == 0 || vals[i] != vals[i - 1]) {
      ++runs;
    }
  }
  if (runs < 2 || runs * 4 > vals.size() * 3) {
    return false;
  }
  return !encoded.buf.empty() && encoded.buf.size() * 5 < vals.size() * 8 * 4;
}

// Byte-aligned Frame-of-Reference (FOR-BA): base + LE offsets.
// Each value stored as bpv whole bytes (LE), enabling direct memory loads
// during scan with zero bit manipulation.
//   Region layout: [base i64 BE][offset_0 bpv B LE][offset_1]...
//   param stores bytes-per-value (1, 2, 4, or 8).
EncodedCol EncForBA(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  int64_t mn = vals.front(), mx = vals.front();
  for (int64_t v : vals) {
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
  const uint64_t range = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
  uint32_t bpv = 1;
  while (bpv < 8 && range >> (bpv * 8) != 0) {
    ++bpv;
  }
  // Exact region size: 8-byte base plus bpv whole bytes per value.
  if (8 + (static_cast<size_t>(bpv) * vals.size()) > budget) {
    return {};
  }

  EncodedCol e;
  e.scheme = Scheme::kForBp;  // reuse ID 3; semantics now byte-aligned
  e.param = bpv;
  AppendI64(e.buf, mn);  // base at bytes 0..7 (BE)
  // The offset loop used to push_back one byte at a time; the size is already
  // known, so write straight through a sized tail.
  const size_t off0 = e.buf.size();
  BufTail(e.buf, static_cast<size_t>(bpv) * vals.size());
  char* wp = e.buf.data() + off0;
  for (int64_t v : vals) {
    const uint64_t off = static_cast<uint64_t>(v) - static_cast<uint64_t>(mn);
    for (uint32_t b = 0; b < bpv; ++b) {
      *wp++ = static_cast<char>((off >> (b * 8)) & 0xFF);
    }
  }
  return e;
}

EncodedCol EncDictI64(const std::vector<int64_t>& vals, size_t budget,
                      int known_distinct) {
  if (vals.empty()) {
    return {};
  }
  // Exact region size once the distinct count is known: count + packed code
  // stream + 8 B per dictionary entry. The row-group profiler already counted
  // distinct, so the u16-dictionary candidate can be priced before the hash
  // pass that builds it.
  if (known_distinct > 0) {
    const int cw = BitsNeeded(static_cast<uint64_t>(known_distinct - 1));
    if (2 + ((vals.size() * static_cast<size_t>(cw) + 7) / 8) +
            (8 * static_cast<size_t>(known_distinct)) >
        budget) {
      return {};
    }
  }
  // Thread-local scratch: the dictionary path is re-run for every row group
  // and (via EncodeInnerI64) for composite substreams.
  thread_local std::vector<int64_t> dict;
  dict.clear();
  if (known_distinct > 0) {
    dict.reserve(static_cast<size_t>(known_distinct));
  }
  thread_local FlatMapI64 code(0);
  code.ResetFor(vals.size());
  thread_local std::vector<uint64_t> codes;
  codes.clear();
  codes.reserve(vals.size());
  for (int64_t v : vals) {
    if (const uint64_t* found = code.Find(v)) {
      codes.push_back(*found);
    } else {
      const size_t next_distinct = dict.size() + 1;
      const int next_width = BitsNeeded(next_distinct - 1);
      if (2 + ((vals.size() * static_cast<size_t>(next_width) + 7) / 8) +
              (8 * next_distinct) >
          budget) {
        return {};
      }
      code.InsertNew(v, dict.size());
      codes.push_back(dict.size());
      dict.push_back(v);
    }
  }
  const int cw = BitsNeeded(dict.size() - 1);
  EncodedCol e;
  e.scheme = Scheme::kDict;
  e.param = static_cast<uint32_t>(dict.size());
  AppendBE16(e.buf, static_cast<uint16_t>(dict.size()));
  AppendPackBits(e.buf, codes, cw);
  for (int64_t v : dict) {
    AppendI64(e.buf, v);
  }
  return e;
}

// Order-preserving dictionary (kOpDict): byte-identical size to EncDictI64
// (same header, code width, and dictionary payload) with the dictionary
// sorted ascending and each code = the rank of its value. Value order equals
// code order, so range predicates evaluate as a code window directly on the
// packed code stream (OpDictRegionCount / TryFastPredicateMask) without
// touching the dictionary or decoding rows.
EncodedCol EncOpDictI64(const std::vector<int64_t>& vals, size_t budget,
                        int known_distinct) {
  if (vals.empty()) {
    return {};
  }
  // u16 code space; EncodeColumnAdaptive gates candidates at 1024 distinct.
  // Same exact-size shortcut as EncDictI64 -- it retires the candidate before
  // the sort below, which is the dominant cost here.
  if (known_distinct > 0) {
    const int cw = BitsNeeded(static_cast<uint64_t>(known_distinct - 1));
    if (2 + ((vals.size() * static_cast<size_t>(cw) + 7) / 8) +
            (8 * static_cast<size_t>(known_distinct)) >
        budget) {
      return {};
    }
  }
  std::vector<int64_t> distinct(vals.begin(), vals.end());
  // Row-order sorted columns (clustered keys) skip the sort entirely; the
  // check walks off after the first descent on everything else.
  if (!std::is_sorted(distinct.begin(), distinct.end())) {
    std::sort(distinct.begin(), distinct.end());
  }
  distinct.erase(std::unique(distinct.begin(), distinct.end()), distinct.end());
  if (distinct.size() > UINT16_MAX) {
    return {};
  }
  std::vector<uint64_t> codes;
  codes.reserve(vals.size());
  for (int64_t v : vals) {
    codes.push_back(static_cast<uint64_t>(
        std::lower_bound(distinct.begin(), distinct.end(), v) -
        distinct.begin()));
  }
  const int cw = BitsNeeded(distinct.size() - 1);
  EncodedCol e;
  e.scheme = Scheme::kOpDict;
  e.param = static_cast<uint32_t>(distinct.size());
  AppendBE16(e.buf, static_cast<uint16_t>(distinct.size()));
  AppendPackBits(e.buf, codes, cw);
  for (int64_t v : distinct) {
    AppendI64(e.buf, v);  // ascending
  }
  return e;
}

// Huffman code LENGTHS for the kHuffDict encoder: min-heap Huffman over the
// per-symbol frequencies, then a Kraft-completeness verification. Natural
// depth over n <= 1024 rows is bounded at 13 (the Fibonacci frequency worst
// case), so the 15-bit cap never engages on valid input -- the check is the
// same defensive bail the validator enforces, kept here so a future row-group
// growth fails loudly instead of emitting an undecodable region.
bool HuffDictCodeLengths(const std::vector<uint32_t>& freq,
                         std::vector<uint8_t>* lens) {
  const size_t d = freq.size();
  if (d < 2 || d > kHuffDictMaxCodes) {
    return false;
  }
  struct HeapItem {
    uint64_t weight;
    int32_t node;
  };
  struct HeapCmp {
    bool operator()(const HeapItem& a, const HeapItem& b) const {
      // Deterministic tie-break (lower node id) keeps the lengths stable
      // across platforms; only the lengths reach the wire, and the codes are
      // canonical from the lengths alone.
      return a.weight != b.weight ? a.weight > b.weight : a.node > b.node;
    }
  };
  std::priority_queue<HeapItem, std::vector<HeapItem>, HeapCmp> pq;
  std::vector<int32_t> parent;
  parent.reserve(2 * d);
  for (size_t i = 0; i < d; ++i) {
    if (freq[i] == 0) {
      return false;  // every distinct value occurs >= once
    }
    parent.push_back(-1);
    pq.push({freq[i], static_cast<int32_t>(i)});
  }
  while (pq.size() > 1) {
    const HeapItem a = pq.top();
    pq.pop();
    const HeapItem b = pq.top();
    pq.pop();
    parent[static_cast<size_t>(a.node)] = parent[static_cast<size_t>(b.node)] =
        static_cast<int32_t>(parent.size());
    parent.push_back(-1);
    pq.push({a.weight + b.weight, static_cast<int32_t>(parent.size() - 1)});
  }
  lens->assign(d, 0);
  uint64_t kraft = 0;
  for (size_t i = 0; i < d; ++i) {
    // Depth = edge count from the root: one increment per parent hop (a leaf
    // whose parent is the root is depth 1).
    uint32_t len = 0;
    for (int32_t p = parent[i]; p >= 0; p = parent[static_cast<size_t>(p)]) {
      ++len;
    }
    if (len > kHuffDictMaxLen) {
      return false;  // unreachable at d <= 1024/n
    }
    (*lens)[i] = static_cast<uint8_t>(len);
    kraft += UINT64_C(1) << (kHuffDictMaxLen - len);
  }
  // Huffman trees are exactly complete; anything else is a bug upstream.
  return kraft == (UINT64_C(1) << kHuffDictMaxLen);
}

// Canonical Huffman over an order-preserving dictionary (kHuffDict): kOpDict's
// ascending dictionary with the fixed-width rank stream replaced by a
// canonical MSB-first Huffman bitstream, plus an exact per-symbol frequency
// table. The bitstream pays the symbol ENTROPY instead of log2(d) bits per
// row, which beats the fixed-width dictionaries once the code distribution is
// MULTI-MODAL skewed (a few hot ranks plus a long tail -- single-mode skew
// belongs to kFreq, clustered codes to the run family, uniform columns keep
// kOpDict because the tables cost ~2.25 B per distinct). The frequency table
// is the compressed-domain payoff: predicates fold to
// sum(freq[s]*pass[s]) / sum(dict[s]*freq[s]*pass[s]) over the d symbols with
// zero bitstream reads, EQ COUNT is one binary search plus one table read,
// and MIN/MAX are the first/last dictionary entries.
//   Region layout: [d u16 BE][dict d x 8B BE, ascending]
//                  [lens ceil(d/2) B, nibbles][freq d x u16 BE]
//                  [ncp u16 BE][ckpt ncp x u32 BE][stream_len u32 BE][stream]
//   param stores d (mirrors kDict/kOpDict).
EncodedCol EncHuffDictI64(const std::vector<int64_t>& vals, size_t budget,
                          int known_distinct) {
  if (vals.empty()) {
    return {};
  }
  // Exact-size floor from the profiler's distinct count: every wire term is
  // final once d is known, so the same strict cut the post-count check makes
  // can run before the hash pass (same winner, no counting work).
  if (known_distinct >= 2) {
    const auto d0 = static_cast<size_t>(known_distinct);
    const auto n0 = static_cast<uint32_t>(vals.size());
    const uint32_t ncp0 = (n0 + kHuffDictCheckpoint - 1) / kHuffDictCheckpoint;
    if (2 + (8 * d0) + ((d0 + 1) / 2) + (2 * d0) + 2 +
            (static_cast<size_t>(ncp0) * 4) + 4 +
            ((static_cast<size_t>(n0) + 7) / 8) >
        budget) {
      return {};
    }
  }
  // Value -> (freq | rank<<16): the first pass counts in one hash probe per
  // value, then the distinct/freq arrays are sorted out and the same map is
  // rewritten to rank form so the second pass is another single probe per
  // value (the historical route was an O(log d) binary search per value).
  // d <= kHuffDictMaxCodes (1024) and freq <= n < 2^16 (the wire format
  // stores u16 frequencies), so both fields fit their halves.
  // Thread-local scratch: the adaptive cascade re-runs this encoder for
  // every row group, so the table grows once and resets per attempt.
  thread_local FlatMapI64 rank_of(0);
  rank_of.ResetFor(vals.size());
  for (int64_t v : vals) {
    ++*rank_of.UpsertSlot(static_cast<uint64_t>(v));
  }
  if (rank_of.size() < 2 || rank_of.size() > kHuffDictMaxCodes) {
    return {};  // kConst owns d == 1
  }
  const size_t d0 = rank_of.size();
  // Exact-size floor with the minimum 1 bit per code: every other term is
  // final once d is known, so a floor past the budget retires the candidate
  // before the Huffman build and the bitstream pass (same strict cut as the
  // post-encode check, hence the same winner).
  const auto n0 = static_cast<uint32_t>(vals.size());
  const uint32_t ncp0 = (n0 + kHuffDictCheckpoint - 1) / kHuffDictCheckpoint;
  if (2 + (8 * d0) + ((d0 + 1) / 2) + (2 * d0) + 2 +
          (static_cast<size_t>(ncp0) * 4) + 4 +
          ((static_cast<size_t>(n0) + 7) / 8) >
      budget) {
    return {};
  }
  std::vector<std::pair<int64_t, uint64_t>> counted;
  counted.reserve(rank_of.size());
  rank_of.ForEach([&counted](uint64_t key, uint64_t freq) {
    counted.emplace_back(static_cast<int64_t>(key), freq);
  });
  std::sort(counted.begin(), counted.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  const size_t d = counted.size();
  std::vector<int64_t> distinct(d);
  std::vector<uint32_t> freq(d);
  for (size_t s = 0; s < d; ++s) {
    distinct[s] = counted[s].first;
    freq[s] = static_cast<uint32_t>(counted[s].second);
    rank_of.InsertNew(static_cast<uint64_t>(counted[s].first), s);
  }
  std::vector<uint16_t> rank(vals.size());
  for (size_t i = 0; i < vals.size(); ++i) {
    rank[i] =
        static_cast<uint16_t>(*rank_of.Find(static_cast<uint64_t>(vals[i])));
  }
  std::vector<uint8_t> lens;
  if (!HuffDictCodeLengths(freq, &lens)) {
    return {};
  }

  // Canonical code assignment (RFC 1951 3.2.2): symbols walked by (length,
  // id) receive the increasing MSB-first codes. The decoder rebuilds the same
  // tables from the stored lengths alone.
  std::array<uint32_t, kHuffDictMaxLen + 1> bl_count{};
  for (uint8_t l : lens) {
    ++bl_count[l];
  }
  std::array<uint32_t, kHuffDictMaxLen + 1> next_code{};
  uint32_t code = 0;
  for (uint32_t bits = 1; bits <= kHuffDictMaxLen; ++bits) {
    code = (code + bl_count[bits - 1]) << 1;
    next_code[bits] = code;
  }
  std::vector<uint32_t> huff_code(d);
  for (uint32_t bits = 1; bits <= kHuffDictMaxLen; ++bits) {
    for (size_t s = 0; s < d; ++s) {
      if (lens[s] == bits) {
        huff_code[s] = next_code[bits]++;
      }
    }
  }

  // Bitstream in row order with a 64-row checkpoint of the absolute bit
  // offset (a probe replays at most 63 codes after its seek). The writer
  // feeds a 64-bit MSB-first accumulator and flushes whole bytes, producing
  // exactly the historical bit-by-bit stream.
  const auto n = static_cast<uint32_t>(vals.size());
  const uint32_t ncp = (n + kHuffDictCheckpoint - 1) / kHuffDictCheckpoint;
  const size_t stream_cap = (static_cast<size_t>(n) * kHuffDictMaxLen + 7) / 8;
  std::vector<uint8_t> stream(stream_cap);
  std::vector<uint32_t> ckpt(ncp, 0);
  uint64_t acc = 0;
  uint32_t nbits = 0;
  uint64_t abs_bits = 0;
  uint32_t next_ckpt = 0;
  uint8_t* wp = stream.data();
  for (uint32_t i = 0; i < n; ++i) {
    if (i == next_ckpt) {
      // Absolute bit position in the logical stream: flushed bytes plus the
      // bits still pending in the accumulator (the historical
      // stream.size()*8 + fill).
      ckpt[next_ckpt / kHuffDictCheckpoint] = static_cast<uint32_t>(abs_bits);
      next_ckpt += kHuffDictCheckpoint;
    }
    const uint16_t r = rank[i];
    const uint32_t c = huff_code[r];
    const uint32_t w = lens[r];
    acc = (acc << w) | c;
    nbits += w;
    abs_bits += w;
    while (nbits >= 8) {
      *wp++ = static_cast<uint8_t>(acc >> (nbits - 8));
      nbits -= 8;
    }
  }
  auto stream_size = static_cast<size_t>(wp - stream.data());
  if (nbits != 0) {
    // The historical writer left-aligned the tail bits in the final byte.
    stream[stream_size] = static_cast<uint8_t>(acc << (8 - nbits));
    ++stream_size;
  }

  const size_t size = 2 + (8 * d) + ((d + 1) / 2) + (2 * d) + 2 +
                      (static_cast<size_t>(ncp) * 4) + 4 + stream_size;
  if (size > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kHuffDict;
  e.param = static_cast<uint32_t>(d);
  AppendBE16(e.buf, static_cast<uint16_t>(d));
  for (int64_t v : distinct) {
    AppendI64(e.buf, v);  // ascending
  }
  for (size_t j = 0; j < d; j += 2) {
    auto byte = static_cast<uint8_t>(lens[j] << 4);
    if (j + 1 < d) {
      byte |= lens[j + 1];
    }
    e.buf.push_back(static_cast<char>(byte));
  }
  for (uint32_t f : freq) {
    AppendBE16(e.buf, static_cast<uint16_t>(f));
  }
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t c : ckpt) {
    AppendBE32(e.buf, c);
  }
  AppendBE32(e.buf, static_cast<uint32_t>(stream_size));
  e.buf.insert(e.buf.end(), reinterpret_cast<const char*>(stream.data()),
               reinterpret_cast<const char*>(stream.data() + stream_size));
  return e;
}

// Run-length over dictionary codes (kRleDict): the Parquet RLE_DICTIONARY
// hybrid. Runs are stored as (dict code, length) pairs instead of kRle's
// (full 8B value, length), so a run costs 5-6B instead of 12B while the
// dictionary adds 8B per distinct value ONCE per region. Compressed-domain
// predicates evaluate the dictionary once into a pass table and then fold
// per run (see RleDictRegionCountSum), so run-structured low-cardinality
// columns get kRle's O(runs) scan shape at a fraction of its bytes.
//   Region layout: [d u16 BE][dict d x 8B BE][runs u32 BE]
//                  [{code ceil(cw/8) B BE}{len u32 BE}] x runs
//   param stores the run count (mirrors kRle).
EncodedCol EncRleDictI64(const std::vector<int64_t>& vals, size_t budget,
                         int known_distinct) {
  if (vals.empty()) {
    return {};
  }
  // Exact region size once the distinct count and the run count are known:
  // count + 8 B/dictionary entry + run count + (code byte + 4 B length)/run.
  if (known_distinct > 0) {
    const int cb =
        (BitsNeeded(static_cast<uint64_t>(known_distinct - 1)) + 7) / 8;
    const size_t runs = CountRuns(vals);
    if (6 + (8 * static_cast<size_t>(known_distinct)) +
            (runs * (static_cast<size_t>(cb) + 4)) >
        budget) {
      return {};
    }
  }
  // u16 code space; EncodeColumnAdaptive gates candidates at 1024 distinct
  // (mirrors EncDictI64 / EncOpDictI64).
  FlatMapI64 code(vals.size());
  std::vector<int64_t> dict;
  std::vector<std::pair<uint16_t, uint32_t>> runs;
  for (int64_t v : vals) {
    uint16_t cv = 0;
    if (const uint64_t* found = code.Find(v)) {
      cv = static_cast<uint16_t>(*found);
    } else {
      cv = static_cast<uint16_t>(dict.size());
      code.InsertNew(v, cv);
      dict.push_back(v);
    }
    if (!runs.empty() && runs.back().first == cv) {
      ++runs.back().second;
    } else {
      runs.emplace_back(cv, 1);
    }
  }
  const int cw = BitsNeeded(dict.size() - 1);
  const int cb = (cw + 7) / 8;  // code bytes per run: 1 or 2
  EncodedCol e;
  e.scheme = Scheme::kRleDict;
  e.param = static_cast<uint32_t>(runs.size());
  AppendBE16(e.buf, static_cast<uint16_t>(dict.size()));
  for (int64_t v : dict) {
    AppendI64(e.buf, v);
  }
  AppendBE32(e.buf, static_cast<uint32_t>(runs.size()));
  for (const auto& [c, len] : runs) {
    for (int b = cb - 1; b >= 0; --b) {
      e.buf.push_back(
          static_cast<char>((static_cast<uint64_t>(c) >> (b * 8)) & 0xFF));
    }
    AppendBE32(e.buf, len);
  }
  return e;
}

// Order-preserving run x dictionary (kOpRle): kRleDict's byte layout but
// the dictionary is sorted ascending, so codes are rank-ordered (code <
// code' <=> dict[code] < dict[code']). Range predicates fold to a code
// window over the sorted dict (no pass table) and the run stream still
// collapses run-structured low-cardinality columns.
EncodedCol EncOpRleI64(const std::vector<int64_t>& vals, size_t budget,
                       int known_distinct) {
  if (vals.size() <= 1) {
    return {};
  }
  // Same all-distinct shortcut as kRle/kRleBp: the encoder's own
  // runs == vals.size() decline, proven by the profiled distinct count
  // before the CountRuns pass, the dictionary sort and the per-value
  // binary searches.
  if (known_distinct > 0 &&
      static_cast<size_t>(known_distinct) == vals.size()) {
    return {};
  }
  // Priced before the sort + per-value binary search below, which is the
  // dominant cost of this candidate.
  if (known_distinct > 0) {
    const int cb =
        (BitsNeeded(static_cast<uint64_t>(known_distinct - 1)) + 7) / 8;
    const size_t runs = CountRuns(vals);
    if (6 + (8 * static_cast<size_t>(known_distinct)) +
            (runs * (static_cast<size_t>(cb) + 4)) >
        budget) {
      return {};
    }
  }
  std::vector<int64_t> dict(vals.begin(), vals.end());
  // Same pre-sorted skip as EncOpDictI64: clustered columns arrive sorted.
  if (!std::is_sorted(dict.begin(), dict.end())) {
    std::sort(dict.begin(), dict.end());
  }
  dict.erase(std::unique(dict.begin(), dict.end()), dict.end());
  // u16 code space; EncodeColumnAdaptive gates candidates at 1024 distinct
  // (mirrors EncRleDictI64 / EncOpDictI64).
  if (dict.size() > 1024) {
    return {};
  }
  std::vector<std::pair<uint16_t, uint32_t>> runs;
  runs.reserve((vals.size() / 8) + 1);
  for (int64_t v : vals) {
    const uint16_t c = static_cast<uint16_t>(
        std::lower_bound(dict.begin(), dict.end(), v) - dict.begin());
    if (!runs.empty() && runs.back().first == c) {
      ++runs.back().second;
    } else {
      runs.emplace_back(c, 1);
    }
  }
  // kRle emits the same byte stream minus the dictionary: when every run
  // is length 1 a dict is pure overhead; the size comparison decides.
  if (runs.size() == vals.size()) {
    return {};
  }
  const int cw = BitsNeeded(dict.size() - 1);
  const int cb = (cw + 7) / 8;  // code bytes per run: 1 or 2
  EncodedCol e;
  e.scheme = Scheme::kOpRle;
  e.param = static_cast<uint32_t>(runs.size());
  AppendBE16(e.buf, static_cast<uint16_t>(dict.size()));
  for (int64_t v : dict) {
    AppendI64(e.buf, v);
  }
  AppendBE32(e.buf, static_cast<uint32_t>(runs.size()));
  for (const auto& [c, len] : runs) {
    for (int b = cb - 1; b >= 0; --b) {
      e.buf.push_back(
          static_cast<char>((static_cast<uint64_t>(c) >> (b * 8)) & 0xFF));
    }
    AppendBE32(e.buf, len);
  }
  return e;
}

EncodedCol EncDeltaFp(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  std::vector<uint64_t> deltas;
  deltas.reserve(vals.size() - 1);
  uint64_t maxd = 0;
  for (size_t i = 1; i < vals.size(); ++i) {
    const uint64_t d =
        static_cast<uint64_t>(vals[i]) - static_cast<uint64_t>(vals[i - 1]);
    if (static_cast<int64_t>(d) < 0) {
      return {};  // not ascending
    }
    deltas.push_back(d);
    maxd = std::max(maxd, d);
  }
  int w = BitsNeeded(maxd);
  // A constant column has all-zero deltas => w == 0. The validator rejects
  // prm == 0 for DELTA-FP, so widen to a 1-bit zero stream (lossless).
  if (w == 0) {
    w = 1;
  }
  const uint32_t cks =
      (static_cast<uint32_t>(vals.size()) + kDeltaCheckpointEvery - 1) /
      kDeltaCheckpointEvery;
  // Exact region size: first value + checkpoint array + packed deltas.
  if (8 + (8 * static_cast<size_t>(cks)) +
          ((deltas.size() * static_cast<size_t>(w) + 7) / 8) >
      budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kDeltaFp;
  e.param = static_cast<uint32_t>(w);
  AppendI64(e.buf, vals.front());
  for (uint32_t j = 0; j < cks; ++j) {
    const size_t idx = static_cast<size_t>(j) * kDeltaCheckpointEvery;
    AppendI64(e.buf, vals[idx]);
  }
  AppendPackBits(e.buf, deltas, w);
  return e;
}

EncodedCol EncBitmap(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  int64_t mn = vals.front(), mx = vals.front();
  for (int64_t v : vals) {
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
  // Exact region size: the two boundary values plus a full-column bitmap.
  if (16 + ((vals.size() + 7) / 8) > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kBitmap;
  e.param = 0;
  AppendI64(e.buf, mx);  // "true" = max value
  AppendI64(e.buf, mn);
  const size_t bytes = (vals.size() + 7) / 8;
  e.buf.insert(e.buf.end(), bytes, '\0');
  for (size_t i = 0; i < vals.size(); ++i) {
    if (vals[i] == mx) {
      e.buf[16 + (i / 8)] =
          static_cast<char>(e.buf[16 + (i / 8)] | (0x80 >> (i & 7)));
    }
  }
  return e;
}

// Word-Aligned Hybrid bitmap (scheme 46): kBitmap's two-value contract with
// run-compressed 63-row spans. A span of equal rows becomes one fill word
// (8B for up to 63*L rows) instead of kBitmap's 8B per 64 rows, so clustered
// flag columns shrink below both kBitmap's flat n/8 and kRle's 12B/run. The
// predicate kernel needs no value decode: literal words ARE runs of the
// selection bitmap, and fills are SvSetRange calls.
EncodedCol EncWahI64(const std::vector<int64_t>& vals, size_t budget) {
  const size_t n = vals.size();
  if (n < 2) {
    return {};
  }
  int64_t mx = vals.front(), mn = vals.front();
  for (int64_t v : vals) {
    mx = std::max(mx, v);
    mn = std::min(mn, v);
  }
  if (mx == mn) {
    return {};  // kConst territory
  }
  std::vector<uint64_t> words;
  words.reserve((n + kWahSpan - 1) / kWahSpan);
  size_t i = 0;
  for (; i + kWahSpan <= n; i += kWahSpan) {
    uint64_t lit = 0;
    for (size_t t = 0; t < kWahSpan; ++t) {
      if (vals[i + t] == mx) {
        lit |= 1ULL << (kWahSpan - 1 - t);
      }
    }
    if ((lit & kWahMask63) == kWahMask63 || lit == 0) {
      const uint64_t fillbit = lit != 0 ? 1ULL : 0ULL;
      if (!words.empty() && (words.back() >> 63) != 0 &&
          ((words.back() >> 62) & 1ULL) == fillbit &&
          (words.back() & kWahLenMask) < kWahLenMask) {
        ++words.back();  // extend the fill run (62-bit length, cannot wrap)
      } else {
        words.push_back((1ULL << 63) | (fillbit << 62) | 1ULL);
      }
    } else {
      words.push_back(lit);
    }
  }
  if (i < n) {  // trailing partial span: literal, zero-padded (never a fill)
    uint64_t lit = 0;
    for (size_t t = 0; i + t < n; ++t) {
      if (vals[i + t] == mx) {
        lit |= 1ULL << (kWahSpan - 1 - t);
      }
    }
    words.push_back(lit);
  }
  if (16 + (words.size() * 8) > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kWah;
  e.param = static_cast<uint32_t>(words.size());
  AppendI64(e.buf, mx);
  AppendI64(e.buf, mn);
  for (uint64_t w : words) {
    AppendBE64(e.buf, w);
  }
  return e;
}

// Bit-sliced int64: store each bit position as its own bitmap (LSB-first). A
// value is sum_b bit(i,b)·2^b, so SUM over the column is Σ_b
// 2^b·popcount(slice_b) — a handful of popcounts instead of decoding and adding
// every value. Width w is the minimum bits needed to represent all values (1
// for all-zero).
EncodedCol EncByteSliceI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  uint64_t maxpat = 0;
  for (int64_t v : vals) {
    const auto u = static_cast<uint64_t>(v);
    maxpat = std::max(u, maxpat);  // negatives map to large uint64 -> w=64
  }
  const uint8_t w =
      maxpat == 0 ? 1 : static_cast<uint8_t>(64 - __builtin_clzll(maxpat));
  const size_t stride = (n + 7) / 8;
  // Exact region size: one width byte plus w full-column bitmaps. A column
  // holding a negative value lands on w == 64 and can never fit, which is
  // precisely the case the budget check retires for free.
  if (1 + (static_cast<size_t>(w) * stride) > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kByteSlice;
  e.param = w;
  e.buf.push_back(static_cast<char>(w));
  // 8x8 bit transpose: gather 8 values, then for each plane build the whole
  // bitmap byte at once. The old per-(value, plane) branchy read-modify-write
  // turned an O(n*w) bit gather into O(n*w) unpredictable branches; the
  // transpose turns it into O(n*w/8) straight-line stores.
  const size_t planes_off = e.buf.size();
  e.buf.resize(planes_off + (static_cast<size_t>(w) * stride), '\0');
  char* const planes = e.buf.data() + planes_off;
  uint32_t i = 0;
  for (; i + 8 <= n; i += 8) {
    std::array<uint64_t, 8> lane{};
    for (uint32_t t = 0; t < 8; ++t) {
      lane[t] = static_cast<uint64_t>(vals[i + t]);
    }
    for (uint8_t b = 0; b < w; ++b) {
      uint8_t byte = 0;
      for (uint32_t t = 0; t < 8; ++t) {
        if (((lane[t] >> b) & 1) != 0U) {
          byte |= static_cast<uint8_t>(0x80U >> t);
        }
      }
      planes[(static_cast<size_t>(b) * stride) + (i >> 3)] =
          static_cast<char>(byte);
    }
  }
  // Tail (n % 8 rows): the straightforward per-bit form.
  for (; i < n; ++i) {
    const auto u = static_cast<uint64_t>(vals[i]);
    for (uint8_t b = 0; b < w; ++b) {
      if (((u >> b) & 1) != 0U) {
        const size_t off =
            planes_off + (static_cast<size_t>(b) * stride) + (i >> 3);
        e.buf[off] = static_cast<char>(e.buf[off] | (0x80 >> (i & 7)));
      }
    }
  }
  return e;
}

EncodedCol EncPBitSI64(const std::vector<int64_t>& vals, size_t budget,
                       bool sorted_hint) {
  const size_t n = vals.size();
  if (n == 0 || n > UINT16_MAX) {
    return {};
  }
  // Adaptive selection tries this encoder for every row group, so the
  // scratch buffers are thread-local and grown once: a fresh 2 x 16 KiB
  // allocation pair per attempt otherwise feeds the allocator for nothing
  // when another scheme wins.
  thread_local std::vector<uint64_t> values_ordered;
  thread_local std::vector<uint64_t> sorted_values;
  thread_local std::vector<uint32_t> exception_pos;
  thread_local std::vector<uint64_t> exception_values;
  values_ordered.clear();
  values_ordered.reserve(n);
  for (int64_t value : vals) {
    values_ordered.push_back(PBitSOrderValue(value));
  }
  // A monotone column maps to a monotone order-value stream (the xor sign
  // flip preserves int64 order), so the sort degenerates to identity. The
  // caller's profile computed `mono` over this exact stream.
  const std::vector<uint64_t>& sorted =
      sorted_hint ? values_ordered : sorted_values;
  if (!sorted_hint) {
    sorted_values = values_ordered;
    // Range-adaptive sort: std::sort's branchy partitioning costs ~40x a
    // counting pass on the row-group shapes that dominate adaptive selection
    // (narrow ranges fit the 64 Ki histogram). The sorted ORDER is identical,
    // so the frame search and every encoded byte stay the same.
    uint64_t mn = sorted_values[0];
    uint64_t mx = mn;
    for (const uint64_t v : sorted_values) {
      mn = v < mn ? v : mn;
      mx = v > mx ? v : mx;
    }
    const uint64_t range = mx - mn;
    if (range <= 8191) {
      thread_local std::vector<uint32_t> hist;
      hist.assign(static_cast<size_t>(range) + 1, 0);
      for (const uint64_t v : sorted_values) {
        ++hist[v - mn];
      }
      size_t out = 0;
      for (size_t b = 0; b <= range; ++b) {
        const uint64_t key = mn + b;
        for (uint32_t c = hist[b]; c != 0; --c) {
          sorted_values[out++] = key;
        }
      }
    } else {
      // LSD byte radix instead of std::sort: the sorted content is the same
      // multiset either way, so the frame search and every encoded byte stay
      // identical. Passes whose byte plane is uniform across all values are
      // skipped (the min/max scan above already computed the OR/AND words),
      // which collapses consecutive-range columns to two or three passes.
      thread_local std::vector<uint64_t> radix_scratch;
      radix_scratch.resize(n);
      uint64_t orx = 0;
      uint64_t anx = ~0ULL;
      for (const uint64_t v : sorted_values) {
        orx |= v;
        anx &= v;
      }
      uint64_t* src = sorted_values.data();
      uint64_t* dst = radix_scratch.data();
      for (int pass = 0; pass < 8; ++pass) {
        const int sh = pass * 8;
        if ((((orx ^ anx) >> sh) & 0xFFU) != 0U) {
          std::array<uint32_t, 256> cnt{};
          for (size_t i = 0; i < n; ++i) {
            ++cnt[(src[i] >> sh) & 0xFFU];
          }
          uint32_t acc = 0;
          for (unsigned int& b : cnt) {
            const uint32_t c = b;
            b = acc;
            acc += c;
          }
          for (size_t i = 0; i < n; ++i) {
            dst[cnt[(src[i] >> sh) & 0xFFU]++] = src[i];
          }
          std::swap(src, dst);
        }
      }
      if (src != sorted_values.data()) {
        std::memcpy(sorted_values.data(), src, n * sizeof(uint64_t));
      }
    }
  }
  // All-distinct order values: a frame spans 2^width distinct values and
  // duplicates are the only way a frame can hold more rows than that, so on
  // an all-distinct column the 2^width row cap is exact and the exception
  // load of any width is at least n - 2^width. The adjacent scan below
  // exits early on the first repeat.
  bool all_distinct = true;
  for (size_t i = 1; i < n; ++i) {
    if (sorted[i] == sorted[i - 1]) {
      all_distinct = false;
      break;
    }
  }
  const size_t stride = (n + 7) / 8;
  size_t best_bytes = SIZE_MAX;
  uint32_t best_width = 64;
  uint64_t best_base = sorted.front();
  size_t best_in_frame = 0;
  // Distinct-boundary scratch for the frame walks (built lazily on the
  // first walk that runs; see the loop body).
  thread_local std::vector<uint32_t> nxt;
  bool nxt_built = false;
  // Widest useful width: one bit more than the value range. Past it the
  // smallest base already frames every value (no exceptions) and the packed
  // term only grows, so nothing wider can win.
  const uint32_t max_width = std::min<uint32_t>(
      64,
      static_cast<uint32_t>(BitsNeeded(sorted.back() - sorted.front())) + 1);
  for (uint32_t width = 1; width <= max_width; ++width) {
    // The packed planes alone already exceed the budget, and the exception
    // term only adds to it, so no wider width can win: stop the search.
    if (11 + (static_cast<size_t>(width) * stride) > budget) {
      break;
    }
    // The packed term is a hard lower bound on the region size (the exception
    // term is >= 0), so once it reaches the best size found so far no wider
    // width can strictly beat it: every later width pays a larger packed term
    // against a best that only shrinks. The frame walk below is skipped
    // entirely for those widths.
    if (11 + (static_cast<size_t>(width) * stride) >= best_bytes) {
      break;
    }
    // All-distinct columns: the frame row cap bounds every frame, so a
    // width whose guaranteed exception load (n - 2^width rows, each 10B +
    // its bitmap bit + the bitmap itself) already reaches the incumbent
    // cannot win. Wider widths have a larger cap, so this skips only this
    // width. Duplicate-bearing columns must not assume it (a frame can hold
    // more rows than 2^width) and run the walk instead.
    if (all_distinct && (size_t{1} << width) < n) {
      const size_t lb = 11 + (static_cast<size_t>(width) * stride) + stride +
                        ((n - (size_t{1} << width)) * 10);
      if (lb >= best_bytes) {
        continue;
      }
    }
    // Per left the region size is 11 + width*stride + (n - in_frame)*10,
    // strictly decreasing in in_frame, so the only candidate that can beat
    // the incumbent is this width's first argmax frame (its bytes value
    // dominates every other left's, and equal bytes never update under the
    // strict '<' the original per-left comparison used). The walk therefore
    // tracks just the running maximum frame and its base; the size
    // comparison runs once per width.
    // A 64-bit frame spans every value: the walk below would set right=n on
    // the first left and stop, i.e. base = sorted.front(), in_frame = n,
    // zero exceptions. Settle it here and skip the per-left loop entirely
    // (the width==64 test inside the loop cost a compare per left).
    if (width == 64) {
      const size_t bytes = 11 + (static_cast<size_t>(width) * stride);
      if (bytes < best_bytes) {
        best_bytes = bytes;
        best_width = width;
        best_base = sorted.front();
        best_in_frame = n;
      }
      break;
    }
    // The smallest frame that still beats the incumbent: a partial-frame
    // region costs 11 + width*stride + stride + (n - in_frame)*10, so a
    // frame below F_req cannot win and the walk can stop the moment every
    // remaining start is too late to host one. Zero when even a full frame
    // plus the exception terms cannot get under best_bytes -- only then a
    // full frame (left 0, right == n) can win, which the first step checks.
    size_t frame_need = 1;
    {
      const size_t denom =
          best_bytes - 11 - (static_cast<size_t>(width) * stride) - stride;
      frame_need = denom >= (n - 1) * 10
                       ? 1
                       : n - (denom / 10) + 1;  // strictly-winning floor
    }
    size_t left = 0;
    size_t right = 0;
    size_t max_frame = 0;
    uint64_t max_base = 0;
    while (left < n) {
      // One distinct-boundary table per call, built on the first walk that
      // actually runs: nxt[i] = first index past i holding a strictly
      // larger value. The duplicate skip below becomes a single jump
      // regardless of run length (and degenerates to left+1 when every
      // value is its own base). Columns whose budget break retires them
      // before any walk never pay for it.
      if (!nxt_built) {
        nxt_built = true;
        nxt.resize(n);
        nxt[n - 1] = static_cast<uint32_t>(n);
        for (size_t i = n - 1; i-- > 0;) {
          nxt[i] = sorted[i] == sorted[i + 1] ? nxt[i + 1]
                                              : static_cast<uint32_t>(i + 1);
        }
      }
      right = std::max(right, left);
      const uint64_t base = sorted[left];
      {
        const uint64_t span = 1ULL << width;
        // NOTE: the frame end is a linear ++right walk on purpose. A
        // gallop+bisect variant was measured SLOWER on the tpch-style
        // shapes: their widths frame short runs (2-3 values), so the
        // bisect's extra probes cost more than the 2-3 linear steps they
        // replace. Dense long frames would win, short frames lose.
        while (right < n && sorted[right] - base < span) {
          ++right;
        }
      }
      const size_t in_frame = right - left;
      if (in_frame > max_frame) {
        max_frame = in_frame;
        max_base = base;
      }
      if (right == n) {
        break;
      }
      // No later start can host a STRICTLY larger frame: a frame beginning
      // at index i holds at most n - i rows, so once the next distinct
      // boundary reaches n - max_frame every remaining frame can only tie.
      // Ties never update the incumbent (strict '<' in the size compare),
      // so the walk can stop with the same max_frame/max_base it would have
      // reported after scanning the rest. The frame_need test is the same
      // argument against the incumbent's byte count: frames smaller than
      // frame_need lose to best_bytes no matter how large they are, and
      // once no remaining start can host one the width is settled.
      const uint32_t nl = nxt[left];
      if (static_cast<size_t>(nl) + max_frame >= n) {
        break;
      }
      if (static_cast<size_t>(nl) + frame_need > n) {
        break;
      }
      left = nl;
    }
    const size_t exceptions = n - max_frame;
    const size_t bytes = 11 + (static_cast<size_t>(width) * stride) +
                         (exceptions == 0 ? 0 : stride + (exceptions * 10));
    if (bytes < best_bytes) {
      best_bytes = bytes;
      best_width = width;
      best_base = max_base;
      best_in_frame = max_frame;
    }
  }
  if (best_in_frame == 0) {
    return {};
  }
  if (best_bytes > budget) {
    return {};
  }
  exception_pos.clear();
  exception_values.clear();
  exception_pos.reserve(n - best_in_frame);
  exception_values.reserve(n - best_in_frame);
  const uint64_t span = best_width == 64 ? 0 : (1ULL << best_width);
  for (size_t i = 0; i < n; ++i) {
    const bool in_frame =
        best_width == 64 || (values_ordered[i] >= best_base &&
                             values_ordered[i] - best_base < span);
    if (!in_frame) {
      exception_pos.push_back(static_cast<uint32_t>(i));
      exception_values.push_back(values_ordered[i]);
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kPBitS;
  e.param = best_width;
  e.buf.reserve(
      11 + (static_cast<size_t>(best_width) * stride) +
      (exception_pos.empty() ? 0 : stride + (exception_pos.size() * 10)));
  AppendBE64(e.buf, best_base);
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(best_width)));
  AppendBE16(e.buf, static_cast<uint16_t>(exception_pos.size()));
  const size_t exception_bmp_off = e.buf.size();
  if (!exception_pos.empty()) {
    e.buf.insert(e.buf.end(), stride, '\0');
    for (size_t k = 0; k < exception_pos.size(); ++k) {
      AppendBE16(e.buf, static_cast<uint16_t>(exception_pos[k]));
      AppendBE64(e.buf, exception_values[k]);
    }
  }
  const size_t planes_off = e.buf.size();
  e.buf.insert(e.buf.end(), static_cast<size_t>(best_width) * stride, '\0');
  // exception_pos was built in ascending i order, so a merge cursor answers
  // "is i an exception" in one compare instead of a binary search per row.
  size_t exc_cursor = 0;
  for (size_t i = 0; i < n; ++i) {
    const size_t bit = i >> 3;
    const auto mask = static_cast<uint8_t>(0x80U >> (i & 7U));
    if (exc_cursor < exception_pos.size() &&
        exception_pos[exc_cursor] == static_cast<uint32_t>(i)) {
      ++exc_cursor;
      e.buf[exception_bmp_off + bit] = static_cast<char>(
          e.buf[exception_bmp_off + bit] | static_cast<char>(mask));
      continue;
    }
    const uint64_t q = values_ordered[i] - best_base;
    // Walk only the set bits: an in-frame value has most high planes zero
    // and the old loop paid one iteration per plane. The left-aligned
    // window's set-bit position p maps to plane 63-p; ctz iteration clears
    // exactly the reported bit.
    uint64_t bits = q << (64 - best_width);
    while (bits != 0) {
      const uint32_t b = 63 - static_cast<uint32_t>(__builtin_ctzll(bits));
      bits &= bits - 1;
      const size_t off = planes_off + (static_cast<size_t>(b) * stride) + bit;
      e.buf[off] = static_cast<char>(e.buf[off] | static_cast<char>(mask));
    }
  }
  return e;
}

EncodedCol EncXorF64(const std::vector<uint64_t>& bits, size_t budget) {
  static_cast<void>(budget);
  if (bits.empty()) {
    return {};
  }
  // Adaptive selection re-runs this encoder for every row group, so the xor
  // scratch lives in a thread-local buffer grown once.
  thread_local std::vector<uint64_t> xors;
  xors.clear();
  xors.reserve(bits.size() - 1);
  uint64_t maxx = 0;
  for (size_t i = 1; i < bits.size(); ++i) {
    const uint64_t x = bits[i] ^ bits[i - 1];
    xors.push_back(x);
    maxx = std::max(maxx, x);
  }
  int w = BitsNeeded(maxx);
  // A constant column yields all-zero xors => w == 0. The validator rejects
  // prm == 0 for XOR-FOR (and UnpackBitAt with w=0 is fine but the chunk
  // would fail ValidateChunkBytes), so widen to a 1-bit stream of zeros.
  if (w == 0) {
    w = 1;
  }
  EncodedCol e;
  e.scheme = Scheme::kXorFor;
  e.param = static_cast<uint32_t>(w);
  AppendBE64(e.buf, bits.front());
  AppendPackBits(e.buf, xors, w);
  return e;
}

// --- ClickHouse "Gorilla" float codec (scheme 14) ---------------------------
// Bit-writer/reader used by the XOR-stream. Bits are appended/consumed
// MSB-first so the decoder reconstructs values identically without any
// length prefix (the region row count n bounds the stream).
// bits: bitcast double values (u64). Layout: [first u64 BE][Gorilla bitstream].
EncodedCol EncGorillaF64(const std::vector<uint64_t>& bits) {
  EncodedCol e;
  e.scheme = Scheme::kGorilla;
  if (bits.empty()) {
    AppendBE64(e.buf, 0);
    return e;
  }
  AppendBE64(e.buf, bits.front());
  BitWriter w;
  uint64_t prev = bits.front();
  for (size_t i = 1; i < bits.size(); ++i) {
    const uint64_t x = bits[i] ^ prev;
    if (x == 0) {
      w.put1(0);  // control: identical to previous
    } else {
      w.put1(1);  // control: XOR delta follows
      int lz = __builtin_clzll(x);
      int tz = __builtin_ctzll(x);
      lz = std::min(lz, 31);  // 5-bit field clamp; extra top zeros are harmless
      const int mw = 64 - lz - tz;
      w.put((uint64_t)lz, 5);
      w.put((uint64_t)tz, 6);
      w.put(x >> tz, mw);  // meaningful bits, MSB-first
    }
    prev = bits[i];
  }
  w.finish();
  e.buf.insert(e.buf.end(), w.buf.begin(), w.buf.end());
  return e;
}

// --- Order-preserving dictionary varchar codec (scheme 22) -------------------
// kDict's byte layout with the dictionary sorted ascending (unsigned-byte
// lexicographic, the same order std::string_view and StrPredicate compare
// in) and each code = the rank of its value. Equal size to kDict, but range
// predicates collapse to a code window: VarcharRegionMatchCount binary-
// searches the window bounds once and then scans codes -- no per-query
// dictionary evaluation or pass table.
EncodedCol EncOpDictStr(const std::vector<std::string_view>& sv) {
  if (sv.empty()) {
    return {};
  }
  std::set<std::string_view> distinct(sv.begin(), sv.end());
  if (distinct.size() > 256) {
    return {};  // same code cap as kDict-varchar
  }
  std::unordered_map<std::string_view, uint64_t> rank;
  rank.reserve(distinct.size());
  uint64_t next = 0;
  for (std::string_view s : distinct) {
    rank.emplace(s, next++);
  }
  std::vector<uint64_t> codes;
  codes.reserve(sv.size());
  for (std::string_view s : sv) {
    codes.push_back(rank[s]);
  }
  const int cw = BitsNeeded(distinct.size() - 1);
  EncodedCol e;
  e.scheme = Scheme::kOpDict;
  e.param = static_cast<uint32_t>(distinct.size());
  AppendBE16(e.buf, static_cast<uint16_t>(distinct.size()));
  AppendPackBits(e.buf, codes, cw);
  for (std::string_view s : distinct) {  // ascending
    AppendBE32(e.buf, static_cast<uint32_t>(s.size()));
    e.buf.insert(e.buf.end(), s.begin(), s.end());
  }
  return e;
}

// --- DuckDB FSST (Fast Static Symbol Table) varchar codec (scheme 15) --------
// Build a static symbol table of frequent substrings (length 2..8), then encode
// each string as a sequence of 1-byte codes: code 0 is an escape for one
// literal byte; codes 1..nsym emit the corresponding symbol string. Layout:
//   [nsym u16 BE][symbols: [len u8][bytes]...]
//   [n u32 BE offsets into code stream][code stream (u8 per code)]
// Random access uses the per-string offset table. Symbols are decoded into an
// owned std::string (no aliasing), so callers may hold the Value safely.
EncodedCol EncFsstStr(const std::vector<std::string_view>& sv) {
  EncodedCol e;
  e.scheme = Scheme::kFsst;
  const auto n = static_cast<uint32_t>(sv.size());
  if (n == 0) {
    AppendBE16(e.buf, 0);
    return e;
  }
  // Mine candidate symbols: every substring of length 2..8, scored by
  // freq * (len - 1) (benefit over literal encoding). A candidate is keyed
  // by its little-endian byte load (the upper bytes of a shorter substring
  // are zero), with one open-addressed map per length -- no std::string is
  // materialized per candidate, and the u64 keys keep lookups cache-local.
  // The maps grow on demand from a small table instead of sizing for the
  // worst case up front: distinct candidates are far fewer than the
  // total_candidates bound, and a 7x multi-megabyte table set turns every
  // per-position probe into a cache miss.
  std::array<FlatMapI64, 7> freq_by_len{
      FlatMapI64(16), FlatMapI64(16), FlatMapI64(16), FlatMapI64(16),
      FlatMapI64(16), FlatMapI64(16), FlatMapI64(16)};
  for (const std::string_view s : sv) {
    const char* p = s.data();
    const size_t L = s.size();
    for (size_t pos = 0; pos < L; ++pos) {
      const size_t maxL = std::min<size_t>(8, L - pos);
      uint64_t raw = 0;
      std::memcpy(&raw, p + pos, maxL);  // zero-padded beyond maxL
      for (size_t l = 2; l <= maxL; ++l) {
        // (1ULL << 64) is UB (shifts to 1): an 8-byte key needs no mask.
        const uint64_t key = l == 8 ? raw : (raw & ((1ULL << (8 * l)) - 1ULL));
        ++*freq_by_len[l - 2].UpsertSlot(key);
      }
    }
  }
  // Score and pick the 254 best symbols. A deterministic tie-break on
  // (score, key) keeps the symbol table stable across runs; the format is
  // self-describing, so any deterministic choice encodes and decodes fine.
  // Only the top nsym candidates are consumed, so a bounded min-heap of the
  // best-so-far set (front = worst kept) replaces the full materialize +
  // partial_sort: one comparison per candidate plus nsym log nsym heap work.
  struct Candidate {
    uint64_t key;
    uint64_t score;
    uint32_t len;
  };
  const auto cand_better = [](const Candidate& a, const Candidate& b) {
    if (a.score != b.score) {
      return a.score > b.score;
    }
    return a.key > b.key;
  };
  constexpr size_t kMaxSymbols = 254;
  std::vector<Candidate> top;
  top.reserve(kMaxSymbols);
  for (int l = 2; l <= 8; ++l) {
    freq_by_len[l - 2].ForEach([&](uint64_t key, uint64_t freq) {
      const Candidate cand{.key = key,
                           .score = freq * static_cast<uint64_t>(l - 1),
                           .len = static_cast<uint32_t>(l)};
      if (top.size() < kMaxSymbols) {
        top.push_back(cand);
        std::push_heap(top.begin(), top.end(), cand_better);
        return;
      }
      if (cand_better(cand, top.front())) {
        std::pop_heap(top.begin(), top.end(), cand_better);
        top.back() = cand;
        std::push_heap(top.begin(), top.end(), cand_better);
      }
    });
  }
  const auto nsym = static_cast<uint32_t>(top.size());
  std::sort_heap(top.begin(), top.end(), cand_better);
  // Encode each string into a code vector: longest symbol match wins. The
  // symbol table is probed by u64 key instead of a linear scan over 254
  // std::strings.
  std::array<FlatMapI64, 7> code_by_len{
      FlatMapI64(254), FlatMapI64(254), FlatMapI64(254), FlatMapI64(254),
      FlatMapI64(254), FlatMapI64(254), FlatMapI64(254)};
  for (uint32_t c = 0; c < nsym; ++c) {
    const Candidate& cand = top[c];
    code_by_len[cand.len - 2].InsertNew(cand.key, static_cast<uint64_t>(c) + 1);
  }
  // Concatenated code stream plus a per-string offset table: one scratch
  // buffer instead of a heap allocation per string, and the stream is
  // appended to the region with two bulk copies instead of per-byte
  // push_backs.
  std::vector<uint8_t> stream;
  stream.reserve(1024);
  std::vector<uint32_t> ends(n);
  for (uint32_t i = 0; i < n; ++i) {
    const std::string_view s = sv[i];
    const char* p = s.data();
    const size_t L = s.size();
    size_t pos = 0;
    while (pos < L) {
      int bestL = 0;
      const size_t maxL = std::min<size_t>(8, L - pos);
      uint64_t raw = 0;
      std::memcpy(&raw, p + pos, maxL);
      for (int l = static_cast<int>(maxL); l >= 2; --l) {
        const uint64_t key = l == 8 ? raw : (raw & ((1ULL << (8 * l)) - 1ULL));
        if (const uint64_t* code = code_by_len[l - 2].Find(key)) {
          stream.push_back(static_cast<uint8_t>(*code));
          bestL = l;
          break;
        }
      }
      if (bestL == 0) {
        stream.push_back(0);  // escape
        stream.push_back(static_cast<uint8_t>(p[pos]));
        pos += 1;
      } else {
        pos += static_cast<size_t>(bestL);
      }
    }
    ends[i] = static_cast<uint32_t>(stream.size());
  }

  size_t sym_bytes = 0;
  for (uint32_t c = 0; c < nsym; ++c) {
    sym_bytes += 1 + top[c].len;
  }
  e.buf.reserve(2 + 4 + sym_bytes + (static_cast<size_t>(n) * 4) +
                stream.size());
  AppendBE16(e.buf, static_cast<uint16_t>(nsym));
  const size_t tsl_off = e.buf.size();
  AppendBE32(e.buf, 0);  // total code-stream length, filled at end
  for (uint32_t c = 0; c < nsym; ++c) {
    e.buf.push_back(static_cast<char>(top[c].len));
    const char* sym = reinterpret_cast<const char*>(&top[c].key);
    e.buf.insert(e.buf.end(), sym, sym + top[c].len);
  }
  const size_t offset_table_off = e.buf.size();
  e.buf.resize(offset_table_off + (static_cast<size_t>(n) * 4));
  e.buf.insert(e.buf.end(), reinterpret_cast<const char*>(stream.data()),
               reinterpret_cast<const char*>(stream.data() + stream.size()));
  // Per-string stream offsets (bytes before this string's codes) written
  // straight into the reserved table.
  uint32_t cur = 0;
  for (uint32_t i = 0; i < n; ++i) {
    char* off = e.buf.data() + offset_table_off + (static_cast<size_t>(i) * 4);
    off[0] = static_cast<char>((cur >> 24) & 0xFF);
    off[1] = static_cast<char>((cur >> 16) & 0xFF);
    off[2] = static_cast<char>((cur >> 8) & 0xFF);
    off[3] = static_cast<char>(cur & 0xFF);
    cur = ends[i];
  }
  e.buf[tsl_off] = static_cast<char>((cur >> 24) & 0xFF);
  e.buf[tsl_off + 1] = static_cast<char>((cur >> 16) & 0xFF);
  e.buf[tsl_off + 2] = static_cast<char>((cur >> 8) & 0xFF);
  e.buf[tsl_off + 3] = static_cast<char>(cur & 0xFF);
  e.param = nsym;
  return e;
}

// Front coding (incremental/prefix encoding): each value stores only the
// suffix after the prefix shared with the previous value in row order, plus
// periodic restart entries that store the full string. Wins on sorted
// sequences and on prefix-heavy strings (URLs, paths, hierarchical IDs,
// timestamp strings) where FSST's substring mining has nothing to grip and
// DICT hits its code cap; degenerates to PLAIN's exact size (plen 0 means
// the 4B entry header duplicates PLAIN's 4B length prefix) when nothing
// is shared.
//   Region layout:
//     [nrestart u16 BE][stream_len u32 BE]
//     [restart u32 BE x nrestart]   stream offsets of the full-string entries
//     [entry stream: {plen u16 BE}{slen u16 BE}{suffix bytes}] x n
//   value_i = prev_value[0..plen) + suffix; entries at i % restart_every == 0
//   carry plen = 0. param = restart_every, bit 15 set when the sequence is
//   non-decreasing (the predicate kernels then answer EQ/order counts by
//   compressed-domain binary search; the sequential walk stays available for
//   any shape). Strings over 65535 bytes (slen u16 cap; the memtable caps
//   varchars at 64 KiB) leave the scheme infeasible.
EncodedCol EncFrontCodeStr(const std::vector<std::string_view>& sv,
                           uint32_t restart_every) {
  EncodedCol e;
  e.scheme = Scheme::kFrontCode;
  if (restart_every == 0 || restart_every > 1024) {
    return {};
  }
  const auto n = static_cast<uint32_t>(sv.size());
  if (n == 0) {
    e.param = restart_every;
    AppendBE16(e.buf, 0);  // nrestart
    AppendBE32(e.buf, 0);  // stream_len
    return e;
  }
  std::vector<char> entries;
  std::vector<uint32_t> restarts;
  restarts.reserve((n + restart_every - 1) / restart_every);
  std::string prev;
  prev.reserve(65536);
  bool sorted = true;
  for (uint32_t i = 0; i < n; ++i) {
    const std::string_view s = sv[i];
    if (s.size() > 65535) {
      return {};
    }
    if (s < std::string_view(prev)) {
      sorted = false;
    }
    uint32_t plen = 0;
    if (i % restart_every != 0) {
      // Shared bytes with the previous value, capped at the u16 plen field.
      // std::mismatch stops at the shorter range, so common <= prev.size().
      const size_t common = static_cast<size_t>(
          std::mismatch(s.begin(), s.end(), prev.begin(), prev.end()).first -
          s.begin());
      plen = static_cast<uint32_t>(std::min<size_t>(common, 65535));
    }
    const uint32_t slen = static_cast<uint32_t>(s.size()) - plen;
    if (i % restart_every == 0) {
      restarts.push_back(static_cast<uint32_t>(entries.size()));
    }
    AppendBE16(entries, static_cast<uint16_t>(plen));
    AppendBE16(entries, static_cast<uint16_t>(slen));
    entries.insert(entries.end(), s.begin() + plen, s.end());
    prev.assign(s.begin(), s.end());
  }
  e.param = restart_every | (sorted ? 0x8000U : 0U);
  AppendBE16(e.buf, static_cast<uint16_t>(restarts.size()));
  AppendBE32(e.buf, static_cast<uint32_t>(entries.size()));
  for (uint32_t off : restarts) {
    AppendBE32(e.buf, off);
  }
  e.buf.insert(e.buf.end(), entries.begin(), entries.end());
  return e;
}

// True column bit-packing (BitWeaving): base + w-bit offsets, where w is the
// minimal bit width (not restricted to whole bytes like FOR-BA). Layout:
//   [base i64 BE][w u8][packed offsets: w*n bits, MSB-first]
//   param = w.
EncodedCol EncBitWeaveI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  int64_t mn = vals.front(), mx = vals.front();
  for (int64_t v : vals) {
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
  uint64_t range = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
  int w = BitsNeeded(range);
  if (w == 0) {
    w = 1;  // all-equal degenerates to w=1
  }
  // Exact region size: 8-byte base + width byte + the w-bit packed stream.
  if (9 + ((vals.size() * static_cast<size_t>(w) + 7) / 8) > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kBitWeave;
  e.param = static_cast<uint32_t>(w);
  AppendBE64(e.buf, static_cast<uint64_t>(mn));
  e.buf.push_back(static_cast<char>(w));
  std::vector<uint64_t> offs;
  offs.reserve(vals.size());
  for (int64_t v : vals) {
    offs.push_back(static_cast<uint64_t>(v) - static_cast<uint64_t>(mn));
  }
  AppendPackBits(e.buf, offs, w);
  return e;
}

EncodedCol EncSimple8bI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty() || vals.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  // One min/max pass settles the 60-bit offset cap: offsets are non-negative,
  // so every offset fits iff max - min does (the old form walked the values
  // twice -- once for the min, once for the cap).
  int64_t mn = vals.front();
  int64_t mx = vals.front();
  for (int64_t v : vals) {
    mn = mn < v ? mn : v;
    mx = mx > v ? mx : v;
  }
  // Offsets are capped at 60 bits: selector 15 packs exactly one 60-bit
  // value per word, so a wider range has no word shape to fall back to.
  const uint64_t maxoff = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
  if (maxoff > kSimple8bMaxOffset) {
    return {};
  }
  // Word-count floor before any packing: a value whose offset needs w bits
  // can only ride a selector with bits >= w, and a word is uniform-width,
  // so for ANY threshold t the words that contain one of the W_t values of
  // width >= t hold at most C(t) values each (C(t) = the largest selector
  // count among shapes with bits >= t). Words without such a value only
  // add. total >= max_t ceil(W_t / C(t)) -- exact, so over-budget shapes
  // are retired without the packing walk. Only worth the width pass once
  // offsets are wide enough for the tight shapes to matter.
  const auto bmax = static_cast<uint32_t>(BitsNeeded(maxoff));
  if (bmax >= 16) {
    // kMaxCntForBits[t] = the largest selector count among word shapes with
    // bits >= t (from kSimple8bSelBits/kSimple8bSelCount): 240x0, 120x0,
    // 60x1, 30x2, 20x3, 15x4, 12x5, 10x6, 8x7, 7x8, 6x10, 5x12, 4x15, 3x20,
    // 2x30, 1x60.
    static constexpr std::array<uint32_t, 61> kMaxCntForBits = {
        240, 60, 30, 20, 15, 12, 10, 8, 7, 6, 6, 5, 5,  // t 0..12
        4,   4,  4,  3,  3,  3,  3,  3,                 // t 13..20
        2,   2,  2,  2,  2,  2,  2,  2, 2, 2,           // t 21..30
        1,   1,  1,  1,  1,  1,  1,  1, 1, 1,           // t 31..40
        1,   1,  1,  1,  1,  1,  1,  1, 1, 1,           // t 41..50
        1,   1,  1,  1,  1,  1,  1,  1, 1, 1};          // t 51..60
    static_assert(kMaxCntForBits.size() == 61,
                  "one entry per offset width 0..60");
    static_assert(kMaxCntForBits[0] == 240 && kMaxCntForBits[1] == 60 &&
                      kMaxCntForBits[16] == 3 && kMaxCntForBits[30] == 2 &&
                      kMaxCntForBits[31] == 1 && kMaxCntForBits[60] == 1,
                  "shape thresholds must match the selector tables");
    std::array<uint32_t, 61>
        wgt{};  // wgt[w] = values whose offset needs w bits
    for (int64_t v : vals) {
      ++wgt[static_cast<uint32_t>(BitsNeeded(static_cast<uint64_t>(v) -
                                             static_cast<uint64_t>(mn))) &
            63U];
    }
    size_t min_words = 0;
    size_t above = 0;  // values with width > t, accumulated from the top
    for (uint32_t t = bmax;; --t) {
      // Words covering the width-t group: they also carry everything wider,
      // so the capacity bound uses the full above+t population.
      const size_t with_t = above + wgt[t];
      const size_t words_t =
          (with_t + kMaxCntForBits[t] - 1) / kMaxCntForBits[t];
      min_words = std::max(words_t, min_words);
      above = with_t;
      if (t == 0) {
        break;
      }
    }
    if (8 + (min_words * 8) > budget) {
      return {};
    }
  }
  // Exact region size: base + one BE64 word per emitted word (checked after
  // packing -- the word count is a function of the data, unlike the fixed
  // per-row widths of the FOR/BitWeave family). The scratch is thread_local:
  // adaptive selection re-runs this encoder for every row group, so a fresh
  // vector per attempt would feed the allocator for nothing.
  thread_local std::vector<uint64_t> words;
  words.clear();
  words.reserve((vals.size() / 8) + 1);
  size_t i = 0;
  while (i < vals.size()) {
    const size_t remaining = vals.size() - i;
    // First-fit by descending value count = the canonical greedy: the
    // earliest selector that covers min(count, remaining) offsets packs the
    // most rows per word. Selector 15 (1 x 60 bits) always fits, so the
    // loop always makes progress.
    for (uint32_t sel = 0; sel < 16; ++sel) {
      const uint32_t cnt = kSimple8bSelCount[sel];
      const uint32_t bits = kSimple8bSelBits[sel];
      const auto avail = static_cast<uint32_t>(remaining);
      const uint32_t take = cnt < avail ? cnt : avail;
      uint64_t payload = 0;
      bool ok = true;
      for (uint32_t j = 0; j < take; ++j) {
        const uint64_t off =
            static_cast<uint64_t>(vals[i + j]) - static_cast<uint64_t>(mn);
        if ((off >> bits) != 0) {
          ok = false;
          break;
        }
        payload |= off << (bits * (cnt - 1 - j));
      }
      if (!ok) {
        continue;
      }
      words.push_back((static_cast<uint64_t>(sel) << 60) | payload);
      i += take;
      break;
    }
  }
  if (8 + (words.size() * 8) > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kSimple8b;
  e.param = static_cast<uint32_t>(words.size());
  AppendBE64(e.buf, static_cast<uint64_t>(mn));
  for (uint64_t word : words) {
    AppendBE64(e.buf, word);
  }
  return e;
}

EncodedCol EncBp128I64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty() || vals.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  const uint32_t blocks =
      (static_cast<uint32_t>(vals.size()) + kBp128BlockRows - 1) /
      kBp128BlockRows;
  std::vector<int64_t> minima(blocks);
  std::vector<int64_t> maxima(blocks);
  std::vector<__int128> sums(blocks, 0);
  std::vector<uint32_t> widths(blocks, 0);
  for (uint32_t block = 0; block < blocks; ++block) {
    const size_t start = static_cast<size_t>(block) * kBp128BlockRows;
    const size_t end = std::min(vals.size(), start + kBp128BlockRows);
    int64_t minimum = vals[start];
    int64_t maximum = vals[start];
    __int128 sum = 0;
    for (size_t row = start; row < end; ++row) {
      minimum = std::min(minimum, vals[row]);
      maximum = std::max(maximum, vals[row]);
      sum += vals[row];
    }
    minima[block] = minimum;
    maxima[block] = maximum;
    sums[block] = sum;
    widths[block] = static_cast<uint32_t>(BitsNeeded(
        static_cast<uint64_t>(maximum) - static_cast<uint64_t>(minimum)));
  }

  // Exact region size: per-block metadata (first, last, 128-bit sum, width)
  // plus one packed stream per block. Widths are known from the block scan.
  {
    size_t exact = static_cast<size_t>(blocks) * kBp128MetadataBytes;
    for (uint32_t block = 0; block < blocks; ++block) {
      const size_t start = static_cast<size_t>(block) * kBp128BlockRows;
      const size_t end = std::min(vals.size(), start + kBp128BlockRows);
      exact += (end - start) * widths[block] / 8;
    }
    if (exact > budget) {
      return {};
    }
  }
  EncodedCol encoded;
  encoded.scheme = Scheme::kBp128;
  encoded.param = blocks;
  encoded.buf.reserve((static_cast<size_t>(blocks) * kBp128MetadataBytes) +
                      vals.size());
  for (uint32_t block = 0; block < blocks; ++block) {
    AppendBE64(encoded.buf, static_cast<uint64_t>(minima[block]));
    AppendBE64(encoded.buf, static_cast<uint64_t>(maxima[block]));
    const auto sum_bits = static_cast<unsigned __int128>(sums[block]);
    AppendBE64(encoded.buf, static_cast<uint64_t>(sum_bits >> 64));
    AppendBE64(encoded.buf, static_cast<uint64_t>(sum_bits));
    encoded.buf.push_back(static_cast<char>(widths[block]));
  }
  for (uint32_t block = 0; block < blocks; ++block) {
    const size_t start = static_cast<size_t>(block) * kBp128BlockRows;
    const size_t end = std::min(vals.size(), start + kBp128BlockRows);
    std::vector<uint64_t> offsets;
    offsets.reserve(end - start);
    for (size_t row = start; row < end; ++row) {
      offsets.push_back(static_cast<uint64_t>(vals[row]) -
                        static_cast<uint64_t>(minima[block]));
    }
    AppendPackBits(encoded.buf, offsets, static_cast<int>(widths[block]));
  }
  return encoded;
}

// --- Generic cascaded composites (schemes 24-26) -----------------------------
// BtrBlocks-style recursive compression: a composite's substreams (RLE value
// and length lists, dictionary code streams, frequency exceptions) are each
// stored as an ordinary self-contained inner region, so the leaf schemes keep
// one canonical byte format and the readers recurse exactly once.
//
// EncodeInnerI64 re-runs the leaf chooser on a substream and returns the
// smallest encoding. Empty input yields an empty region (callers treat that
// as infeasible -- a composite with no substream is meaningless).

EncodedCol EncodeInnerI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  if (vals.size() == 1) {
    EncodedCol plain;
    plain.scheme = Scheme::kPlain;
    const uint64_t be = htobe64(static_cast<uint64_t>(vals.front()));
    plain.buf.resize(sizeof(be));
    std::memcpy(plain.buf.data(), &be, sizeof(be));
    return plain;
  }

  const bool all_same =
      std::all_of(vals.begin(), vals.end(),
                  [&](int64_t value) { return value == vals[0]; });
  if (all_same) {
    return EncConstI64(vals.front());
  }

  const size_t plain_size = vals.size() * 8;
  size_t best_size = plain_size;
  EncodedCol best;
  auto consider = [&](EncodedCol candidate) {
    if (!candidate.buf.empty() && candidate.buf.size() < best_size) {
      best_size = candidate.buf.size();
      best = std::move(candidate);
    }
  };
  consider(EncRleI64(vals, budget));
  consider(EncForBA(vals, budget));
  consider(EncDictI64(vals, budget));
  consider(EncBitWeaveI64(vals, budget));
  if (!best.buf.empty()) {
    return best;
  }

  best.scheme = Scheme::kPlain;
  best.buf.resize(plain_size);
  char* wp = best.buf.data();
  for (int64_t value : vals) {
    const uint64_t be = htobe64(static_cast<uint64_t>(value));
    std::memcpy(wp, &be, sizeof(be));
    wp += sizeof(be);
  }
  return best;
}

// Frequency encoding (kFreq): dominant value + exception bitmap + inner-
// encoded exception list. Wins on skewed columns where one value dominates
// but more than two distinct values exist (kBitmap's niche) -- exceptions
// are stored once each in a self-contained region instead of inflating a
// dictionary.
//   [top i64 BE][inner_scheme u8][inner_param u32 BE][nexc u16 BE]
//   [bitmap ceil(n/8) B MSB-first][inner region: nexc values, row order]
//   param = nexc.
EncodedCol EncFreqI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n == 0) {
    return {};
  }
  // Mode via a flat open-addressed count table: distinct is already profiled
  // small for dict-family columns, but FREQ is also profitable on long tails,
  // so there is no distinct cap here beyond the u16 exception field. The flat
  // table avoids one heap node per value that std::unordered_map allocated on
  // every row group of every skewed column. The table itself is a
  // thread_local reused across attempts: ResetFor re-establishes the same
  // capacity contract the constructor gave without re-allocating.
  thread_local FlatMapI64 freq(0);
  freq.ResetFor(vals.size());
  int64_t top = vals.front();
  uint32_t top_cnt = 0;
  for (int64_t v : vals) {
    uint64_t* slot = freq.Find(v);
    uint32_t c = 0;
    if (slot == nullptr) {
      c = 1;
      freq.InsertNew(v, 1);
    } else {
      c = static_cast<uint32_t>(++*slot);
    }
    if (c > top_cnt) {
      top_cnt = c;
      top = v;
    }
  }
  const uint64_t nexc = static_cast<uint64_t>(n) - top_cnt;
  if (nexc == 0 || nexc > UINT16_MAX) {
    return {};  // 0 exceptions: kConst wins
  }
  // Floor on the region: 8B top + scheme/param/nexc header + a full-column
  // bitmap, so a long-tail column is retired before the exception list and its
  // inner encode are built.
  const size_t stride = (n + 7) / 8;
  if (15 + stride > budget) {
    return {};
  }
  thread_local std::vector<int64_t> exc;
  exc.clear();
  exc.reserve(nexc);
  for (int64_t v : vals) {
    if (v != top) {
      exc.push_back(v);
    }
  }
  // The inner budget tightens by the committed 15-byte header + bitmap: an
  // inner region past the remainder pushes the total over budget, and an
  // over-budget candidate can never win the budgeted cascade (its cost
  // exceeds prune_cost, the incumbent the budget derives from). Forced-scheme
  // calls run with no budget, so this retires losing work only.
  EncodedCol inner = EncodeInnerI64(exc, budget - 15 - stride);
  if (inner.buf.empty()) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kFreq;
  e.param = static_cast<uint32_t>(nexc);
  AppendI64(e.buf, top);
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner.scheme)));
  AppendBE32(e.buf, inner.param);
  AppendBE16(e.buf, static_cast<uint16_t>(nexc));
  const size_t bmp_off = e.buf.size();
  e.buf.insert(e.buf.end(), (n + 7) / 8, '\0');
  for (uint32_t i = 0; i < n; ++i) {
    if (vals[i] == top) {
      e.buf[bmp_off + (i >> 3)] =
          static_cast<char>(e.buf[bmp_off + (i >> 3)] | (0x80 >> (i & 7)));
    }
  }
  e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  return e;
}

// Top-K frequency encoding (kTopk): the K=15 most frequent distinct values
// ("hot") stored ascending with an exact per-slot count, one NIBBLE per row
// (code = rank of the value among the hot set, 15 = exception sentinel), and
// the exceptions in a self-contained inner region. kFreq generalized to a
// hot HEAD -- the shape a dictionary family scheme cannot afford (d in the
// hundreds costs 10B/entry) but a zipf tail still concentrates into 15 slots
// covering most rows at 4 bits each. No exception bitmap: the sentinel code
// marks the exception rows, so the nibble stream alone decides hot vs cold.
// The inline exact frequency table turns any predicate over the hot head
// into a 15-entry code-window fold with zero nibble reads (kHuffDict's
// histogram trick at dict size).
//   [d u16 BE][nexc u16 BE][hot 15 x 8B BE ascending, slots >= d zero]
//   [freq 15 x u16 BE, freq[c] >= 1 for c < d, 0 above]
//   [ischeme u8][iparam u32 BE]
//   [nibbles ceil(n/2) B MSB-nibble-first: codes 0..d-1 hot, 15 exception]
//   [inner region: nexc exception values in row order; omitted at nexc==0]
//   param = nexc.
EncodedCol EncTopKI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  constexpr uint32_t kHot = 15;
  if (n < 2) {
    return {};
  }
  // Floor before the frequency pass: header + hot/freq tables + the nibble
  // plane are committed on every emission, so a budget below them retires
  // the candidate before a single value is hashed (same strict cut the
  // post-select check makes, hence the same winner).
  constexpr size_t kTopkHeaderEarly = 2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;
  if (kTopkHeaderEarly + ((static_cast<size_t>(n) + 1) / 2) > budget) {
    return {};
  }
  thread_local FlatMapI64 freq(0);
  freq.ResetFor(n);
  // Distinct values in first-appearance order: `ord` breaks count ties
  // deterministically (stable under hash-seed and iteration-order changes,
  // matching EncDictI64's first-appearance code assignment).
  thread_local std::vector<int64_t> seen;
  seen.clear();
  seen.reserve(64);
  for (int64_t v : vals) {
    uint64_t* slot = freq.Find(v);
    if (slot == nullptr) {
      freq.InsertNew(v, 1);
      seen.push_back(v);
    } else {
      ++*slot;
    }
  }
  if (seen.size() < 2) {
    return {};  // all-equal: kConst's, not a "top" of any
  }
  struct HotCand {
    uint32_t cnt;
    uint32_t ord;
    int64_t v;
  };
  thread_local std::vector<HotCand> cands;
  cands.clear();
  cands.reserve(seen.size());
  for (uint32_t i = 0; i < seen.size(); ++i) {
    cands.push_back(HotCand{.cnt = static_cast<uint32_t>(*freq.Find(seen[i])),
                            .ord = i,
                            .v = seen[i]});
  }
  // Count desc, then first appearance asc. Only the hot window is consulted
  // below (everything past the first kHot candidates is dead), so a bounded
  // heap-select plus a 15-element sort produces the identical top-15 in
  // O(d + k^2) instead of a full O(d log d) introsort over up to 1024
  // candidates.
  const auto hot_cmp = [](const HotCand& a, const HotCand& b) {
    if (a.cnt != b.cnt) {
      return a.cnt > b.cnt;
    }
    return a.ord < b.ord;
  };
  if (cands.size() > kHot) {
    std::partial_sort(cands.begin(), cands.begin() + kHot, cands.end(),
                      hot_cmp);
  } else {
    std::sort(cands.begin(), cands.end(), hot_cmp);
  }
  const uint32_t d = static_cast<uint32_t>(
      std::min<size_t>(cands.size(), kHot));  // hot slots on the wire
  uint64_t cov = 0;
  for (uint32_t c = 0; c < d; ++c) {
    cov += cands[c].cnt;
  }
  // Head gate: below two thirds of the block the dead half of the nibble
  // plane plus an O(nexc) tail evaluation behind every range fold make the
  // region exception payload with a dead hot table -- a uniform-30 column
  // sits at ~52% and must not emit (the dictionary family wins size AND
  // range on it). The closed-form kernels are only honest above this line.
  if (cov * 3 <= static_cast<uint64_t>(n) * 2) {
    return {};
  }
  const uint64_t nexc = static_cast<uint64_t>(n) - cov;
  if (nexc > UINT16_MAX) {
    return {};  // header cap, same as kFreq
  }
  // Floor on the region: 159B of hot/freq/nexc/inner-scheme header + the
  // nibble stream.
  constexpr size_t kTopkHeader = 2 + 2 + (15 * 8) + (15 * 2) + 1 + 4;  // 159
  if (kTopkHeader + ((static_cast<size_t>(n) + 1) / 2) > budget) {
    return {};
  }
  // Ascending hot order => code = rank => range predicates fold as one
  // contiguous code window (kOpDict/kHuffDict convention).
  std::sort(cands.begin(), cands.begin() + d,
            [](const HotCand& a, const HotCand& b) { return a.v < b.v; });
  FlatMapI64 code_of(static_cast<size_t>(kHot) * 2);
  for (uint32_t c = 0; c < d; ++c) {
    code_of.InsertNew(cands[c].v, c);
  }
  const auto nexc32 = static_cast<uint32_t>(nexc);
  thread_local std::vector<int64_t> exc;
  exc.clear();
  if (nexc32 > 0) {
    exc.reserve(nexc32);
  }
  EncodedCol e;
  e.buf.reserve(kTopkHeader + ((static_cast<size_t>(n) + 1) / 2) +
                (static_cast<size_t>(nexc32) * 8));
  AppendBE16(e.buf, static_cast<uint16_t>(d));
  AppendBE16(e.buf, static_cast<uint16_t>(nexc));
  for (uint32_t c = 0; c < kHot; ++c) {
    AppendI64(e.buf, c < d ? cands[c].v : 0);
  }
  for (uint32_t c = 0; c < kHot; ++c) {
    AppendBE16(e.buf, c < d ? static_cast<uint16_t>(cands[c].cnt) : 0);
  }
  const size_t ischeme_off = e.buf.size();
  e.buf.insert(e.buf.end(), 5, '\0');  // [ischeme u8][iparam u32], filled below
  const size_t nib_off = e.buf.size();
  e.buf.insert(e.buf.end(), (static_cast<size_t>(n) + 1) / 2, '\0');
  for (uint32_t i = 0; i < n; ++i) {
    const uint64_t* slot = code_of.Find(vals[i]);
    const uint64_t code = slot != nullptr ? *slot : 15;
    if (slot == nullptr) {
      exc.push_back(vals[i]);
    }
    const auto byte = static_cast<uint8_t>(e.buf[nib_off + (i >> 1)]);
    e.buf[nib_off + (i >> 1)] = static_cast<char>(
        (i & 1) == 0 ? (byte & 0x0F) | static_cast<uint8_t>(code << 4)
                     : (byte & 0xF0) | static_cast<uint8_t>(code));
  }
  if (nexc32 > 0) {
    // The inner budget tightens by the committed region so far (header +
    // hot table + nibble plane): an inner region past the remainder pushes
    // the total over budget, and an over-budget candidate can never win the
    // budgeted cascade. Forced-scheme calls run with no budget.
    if (e.buf.size() + 8 > budget) {
      return {};
    }
    EncodedCol inner = EncodeInnerI64(exc, budget - e.buf.size());
    if (inner.buf.empty()) {
      return {};
    }
    e.buf[ischeme_off] = static_cast<char>(static_cast<uint8_t>(inner.scheme));
    const uint32_t be = htobe32(inner.param);
    std::memcpy(e.buf.data() + ischeme_off + 1, &be, sizeof(be));
    e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  } else {
    e.buf[ischeme_off] = 0;  // kPlain: no inner bytes exist to decode
  }
  e.scheme = Scheme::kTopk;
  e.param = nexc32;
  return e;
}

// Block-local dictionary encoding (kTileDict): the region is a headerless
// chain of 64-row tiles, each `[d u8][dict d x 8B BE ascending][codes at
// BitsNeeded(d-1) bits MSB-first]`. Declines when any tile exceeds 64
// distinct values (the single-u64 pass-mask bound the kernels lean on) or
// when n <= 64 (a single-tile region is kDict's layout minus one byte; that
// must stay kDict's niche). Values never repeat GLOBALLY here -- the win is
// locality, which the size race against kDict/PLAIN prices directly.
EncodedCol EncTileDictI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n <= kTileDictRows) {
    return {};
  }
  const uint32_t ntiles = (n + kTileDictRows - 1) / kTileDictRows;
  // Pass 1: per-tile distinct sets in first-appearance order (linear scans:
  // the set is capped at 64, so each membership test is <= 64 compares).
  std::vector<std::vector<int64_t>> seen(ntiles);
  uint32_t multi_valued = 0;
  size_t total = 0;
  for (uint32_t t = 0; t < ntiles; ++t) {
    const uint32_t begin = t * kTileDictRows;
    const uint32_t tile_rows =
        (n - begin < kTileDictRows) ? n - begin : kTileDictRows;
    std::vector<int64_t>& s = seen[t];
    for (uint32_t i = begin; i < begin + tile_rows; ++i) {
      bool found = false;
      for (const int64_t v : s) {
        if (v == vals[i]) {
          found = true;
          break;
        }
      }
      if (!found) {
        if (s.size() >= kTileDictMaxD) {
          return {};
        }
        if (s.size() == 1) {
          ++multi_valued;
        }
        s.push_back(vals[i]);
      }
    }
    // A tile's byte total is final once its distinct set is built, and the
    // totals only grow, so once the accumulated total is over budget the
    // whole region is: retire before the remaining tiles' passes run. Same
    // verdict as the original end-of-pass-1 comparison.
    const auto d = static_cast<uint32_t>(s.size());
    total += 1 + (8 * static_cast<size_t>(d)) +
             ((static_cast<size_t>(tile_rows) *
                   static_cast<size_t>(TileDictWidth(d)) +
               7) /
              8);
    if (total > budget) {
      return {};
    }
  }
  // An ALL-constant-tile region is runs of length >= 32 -- the kRle/kRleX/
  // kConst family stores the same rows smaller and its niche must stay
  // theirs (the kMod "wrap-free window stays kArith's" rule). A region with
  // at least one multi-valued tile is the dictionary's own shape.
  if (multi_valued == 0) {
    return {};
  }
  EncodedCol e;
  e.buf.reserve(total);
  FlatMapI64 code_of(2 * static_cast<size_t>(kTileDictMaxD));
  for (uint32_t t = 0; t < ntiles; ++t) {
    if (!std::is_sorted(seen[t].begin(), seen[t].end())) {
      std::sort(seen[t].begin(), seen[t].end());
    }
    const auto d = static_cast<uint32_t>(seen[t].size());
    e.buf.push_back(static_cast<char>(d));
    for (uint32_t c = 0; c < d; ++c) {
      AppendI64(e.buf, seen[t][c]);
      code_of.InsertNew(seen[t][c], c);
    }
    const uint32_t tile_rows = (n - (t * kTileDictRows) < kTileDictRows)
                                   ? n - (t * kTileDictRows)
                                   : kTileDictRows;
    const int w = TileDictWidth(d);
    const size_t codes_off = e.buf.size();
    e.buf.insert(
        e.buf.end(),
        (static_cast<size_t>(tile_rows) * static_cast<size_t>(w) + 7) / 8,
        '\0');
    for (uint32_t j = 0; j < tile_rows; ++j) {
      const uint64_t* slot =
          code_of.Find(vals[(static_cast<size_t>(t) * kTileDictRows) + j]);
      const uint64_t code = slot != nullptr ? *slot : 0;  // d==1: w==0 anyway
      const uint64_t bit_base =
          static_cast<uint64_t>(j) * static_cast<uint64_t>(w);
      for (int b = 0; b < w; ++b) {
        if (((code >> (w - 1 - b)) & 1U) != 0) {
          const uint64_t bit = bit_base + static_cast<uint64_t>(b);
          e.buf[codes_off + (bit >> 3)] = static_cast<char>(
              static_cast<uint8_t>(e.buf[codes_off + (bit >> 3)]) |
              (0x80U >> (bit & 7U)));
        }
      }
    }
    if (t + 1 < ntiles) {
      code_of.Reset();
    }
  }
  e.scheme = Scheme::kTileDict;
  e.param = 0;
  return e;
}

// Sorted-view permutation encoding (kSortDict): sort the values, store them
// as a per-64-row-block delta stream, and keep a ceil(log2 n)-bit rank plane
// mapping row -> sorted index. See the enum comment for the layout and the
// kernels the sorted view buys. Gates: n >= 2, and MOSTLY-DISTINCT -- at
// least half the rows unique (distinct_hint * 2 >= n); below that the
// dictionary family stores the same rows smaller and keeps its niche (the
// kTileDict wrap-free-window rule).
// (value, row) pair sort for the SortDict/Postings pair arrays. Rows are
// unique, so the pair order is total and ANY correct sort yields the same
// array -- a narrow-range counting sort (range <= 8191, where the histogram
// pass amortizes) is byte-identical to std::sort and ~20x cheaper on the
// low-cardinality shapes those encoders gate on. Uniform high bytes skip
// their radix passes, mirroring EncPBitS.
void SortValueRowPairs(std::vector<std::pair<int64_t, uint32_t>>& order) {
  const size_t n = order.size();
  if (n < 2) {
    return;
  }
  int64_t mn = order[0].first;
  int64_t mx = order[0].first;
  for (const auto& p : order) {
    mn = p.first < mn ? p.first : mn;
    mx = p.first > mx ? p.first : mx;
  }
  const uint64_t range = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
  if (range <= 8191) {
    thread_local std::vector<uint32_t> hist;
    hist.assign(static_cast<size_t>(range) + 1, 0);
    for (const auto& p : order) {
      ++hist[static_cast<uint64_t>(p.first) - static_cast<uint64_t>(mn)];
    }
    uint32_t acc = 0;
    for (uint64_t b = 0; b <= range; ++b) {
      const uint32_t c = hist[b];
      hist[b] = acc;
      acc += c;
    }
    thread_local std::vector<std::pair<int64_t, uint32_t>> scratch;
    scratch.resize(n);
    for (const auto& p : order) {
      scratch[hist[static_cast<uint64_t>(p.first) -
                   static_cast<uint64_t>(mn)]++] = p;
    }
    order.swap(scratch);
    return;
  }
  // LSD byte radix on the two's-complement bits, skipping planes where all
  // values agree. Non-negative values only: a negative value's sign bit
  // would order it after every positive one, and unlike the PBitS order
  // value these pairs carry raw int64 keys with no monotone transform.
  if (mn < 0) {
    std::sort(order.begin(), order.end());
    return;
  }
  uint64_t orx = 0;
  uint64_t anx = ~0ULL;
  for (const auto& p : order) {
    const auto v = static_cast<uint64_t>(p.first);
    orx |= v;
    anx &= v;
  }
  thread_local std::vector<std::pair<int64_t, uint32_t>> radix_scratch;
  radix_scratch.resize(n);
  std::pair<int64_t, uint32_t>* src_p = order.data();
  std::pair<int64_t, uint32_t>* dst_p = radix_scratch.data();
  for (int pass = 0; pass < 8; ++pass) {
    const int sh = pass * 8;
    if ((((orx ^ anx) >> sh) & 0xFFU) != 0U) {
      std::array<uint32_t, 256> cnt{};
      for (size_t i = 0; i < n; ++i) {
        ++cnt[(static_cast<uint64_t>(src_p[i].first) >> sh) & 0xFFU];
      }
      uint32_t acc = 0;
      for (unsigned int& b : cnt) {
        const uint32_t c = b;
        b = acc;
        acc += c;
      }
      for (size_t i = 0; i < n; ++i) {
        dst_p[cnt[(static_cast<uint64_t>(src_p[i].first) >> sh) & 0xFFU]++] =
            src_p[i];
      }
      std::swap(src_p, dst_p);
    }
  }
  if (src_p != order.data()) {
    std::copy(src_p, src_p + n, order.begin());
  }
}

EncodedCol EncSortDictI64(const std::vector<int64_t>& vals, size_t budget,
                          int distinct_hint) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n < 2 || n > PaxChunk::kRowGroupSize) {
    return {};
  }
  if (distinct_hint >= 0 &&
      static_cast<uint64_t>(distinct_hint) * 2 < static_cast<uint64_t>(n)) {
    return {};
  }
  // Floor before the sort: header + rank plane + one bit per gap. Below this
  // no shape of the data can fit, so the budgeted cascade must not pay the
  // sort for a region that is already over.
  const uint32_t nb = (n + kSortDictBlock - 1) / kSortDictBlock;
  const int rw = BitsNeeded(static_cast<uint64_t>(n) - 1);
  if (SortDictHeaderBytes(n) +
          ((static_cast<uint64_t>(n) * static_cast<uint64_t>(rw) + 7) / 8) +
          ((n - 1) / 8) >
      budget) {
    return {};
  }
  // Sort (value, row) pairs: the sorted view and the rank assignment in one
  // pass. Ties break by row index, which keeps the encoder deterministic.
  thread_local std::vector<std::pair<int64_t, uint32_t>> order;
  order.clear();
  order.resize(n);
  for (uint32_t i = 0; i < n; ++i) {
    order[i] = {vals[i], i};
  }
  // vals non-decreasing makes the pair array sorted outright (ties break by
  // ascending row), which is exactly the clustered-key shape.
  if (!std::is_sorted(order.begin(), order.end())) {
    SortValueRowPairs(order);
  }
  // Block scan: widths and gap-plane bytes (the exact extent, needed for the
  // budget check before any byte is written).
  uint64_t gap_bytes = 0;
  std::array<uint8_t, kSortDictMaxBlocks> widths{};
  for (uint32_t b = 0; b < nb; ++b) {
    const uint32_t begin = b * kSortDictBlock;
    const uint32_t rows = std::min<uint32_t>(kSortDictBlock, n - begin);
    uint64_t maxgap = 0;
    for (uint32_t t = 1; t < rows; ++t) {
      const uint64_t gap = static_cast<uint64_t>(order[begin + t].first) -
                           static_cast<uint64_t>(order[begin + t - 1].first);
      maxgap = std::max(gap, maxgap);
    }
    const int w = BitsNeeded(maxgap);  // 0 for an all-equal block
    widths[b] = static_cast<uint8_t>(w);
    gap_bytes +=
        (static_cast<uint64_t>(rows - 1) * static_cast<uint64_t>(w) + 7) / 8;
  }
  const uint64_t total =
      SortDictHeaderBytes(n) +
      ((static_cast<uint64_t>(n) * static_cast<uint64_t>(rw) + 7) / 8) +
      gap_bytes;
  if (total > budget) {
    return {};
  }
  EncodedCol e;
  e.buf.reserve(total);
  // The header is sized once and written through a raw pointer (the PLAIN
  // baseline's convention): nb + rw + the three planes, then the packed
  // planes append through AppendPackBits within the reserved capacity.
  e.buf.resize(SortDictHeaderBytes(n));
  {
    char* wp = e.buf.data();
    *wp++ = static_cast<char>(nb);
    *wp++ = static_cast<char>(rw);
    // The kS4 plane discipline: the width table first, then the first-value
    // table, then the prefix-sum table -- three separate planes, the layout
    // ParseSortDictRegion (and ValidateWalk's case 61) read back.
    for (uint32_t b = 0; b < nb; ++b) {
      *wp++ = static_cast<char>(widths[b]);
    }
    for (uint32_t b = 0; b < nb; ++b) {
      const uint64_t be = htobe64(static_cast<uint64_t>(
          order[static_cast<size_t>(b) * kSortDictBlock].first));
      std::memcpy(wp, &be, sizeof(be));
      wp += sizeof(be);
    }
    // psum[0] == 0, psum[b] = wrapped sum of sorted[0 .. 64b), psum[nb] =
    // the wrapped total -- the word the unpredicated SUM kernel loads.
    uint64_t running = 0;
    const uint64_t zero = 0;
    std::memcpy(wp, &zero, sizeof(zero));
    wp += sizeof(zero);
    for (uint32_t b = 0; b < nb; ++b) {
      const uint32_t begin = b * kSortDictBlock;
      const uint32_t rows = std::min<uint32_t>(kSortDictBlock, n - begin);
      for (uint32_t t = 0; t < rows; ++t) {
        running += static_cast<uint64_t>(order[begin + t].first);
      }
      const uint64_t be = htobe64(running);
      std::memcpy(wp, &be, sizeof(be));
      wp += sizeof(be);
    }
  }
  // Rank plane: rank[order[j].second] = j.
  thread_local std::vector<uint64_t> ranks;
  ranks.clear();
  ranks.resize(n);
  for (uint32_t j = 0; j < n; ++j) {
    ranks[order[j].second] = j;
  }
  AppendPackBits(e.buf, ranks, rw);
  // Per-block byte-aligned gap planes.
  for (uint32_t b = 0; b < nb; ++b) {
    const uint32_t begin = b * kSortDictBlock;
    const uint32_t rows = std::min<uint32_t>(kSortDictBlock, n - begin);
    if (widths[b] == 0) {
      continue;  // all-equal block: no gap bytes
    }
    thread_local std::vector<uint64_t> gaps;
    gaps.clear();
    gaps.resize(rows - 1);
    for (uint32_t t = 1; t < rows; ++t) {
      gaps[t - 1] = static_cast<uint64_t>(order[begin + t].first) -
                    static_cast<uint64_t>(order[begin + t - 1].first);
    }
    AppendPackBits(e.buf, gaps, widths[b]);
  }
  e.scheme = Scheme::kSortDict;
  e.param = nb;
  return e;
}

// Inverted dictionary (kPostings): each distinct value owns a sorted posting
// list of its row ids, Rice-coded, behind a count table that IS an exact
// histogram. The transpose of the dictionary: predicates fold to a code
// window over the sorted dict and COUNT/SUM answer from the count table in
// closed form (no row reads), selective masks materialize at O(matched), and
// random access walks the count table instead of replaying bits.
//   [d u16 BE][d x 8B BE dict][d x u16 BE counts][ceil(d/2) x u8 k-nibbles]
//   [d x u16 BE firsts][Rice bitstream: per list (count-1) codes of gap-1]
//   param = d.
// Structural gates: n >= 8, 4 <= d <= 64 (the footer inverted index's own
// coverage band -- this scheme IS that index, compressed, in the codec),
// d <= n/8 (below 4 the two-value/kFreq family owns the column; above the
// cap the per-list tables stop amortizing), and the sort is over (value,
// row) pairs so a list's postings
// come out ascending (ties by row -- the SortDict pair-sort convention).
// No economic gate: the cost model arbitrates (a uniform column pays MORE
// than kDict's flat codes and loses at every hint tier; skew shrinks the
// Rice stream below the code width and the closed forms earn their keep).
EncodedCol EncPostingsI64(const std::vector<int64_t>& vals, size_t budget,
                          int known_distinct) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n < 8 || n > PaxChunk::kRowGroupSize) {
    return {};
  }
  // Distinct probe: the structural gates and the header floor both key on d.
  // The profiled distinct count (prof.xorset.size()) is the exact same
  // number the sort-unique probe computes -- both count the distinct raw
  // cell domain including null-as-0 -- so the hint skips a full copy +
  // sort + unique of every declined attempt.
  uint32_t distinct = 0;
  if (known_distinct > 0) {
    distinct = static_cast<uint32_t>(known_distinct);
  } else {
    thread_local std::vector<int64_t> probe;
    probe.assign(vals.begin(), vals.end());
    std::sort(probe.begin(), probe.end());
    distinct = static_cast<uint32_t>(std::unique(probe.begin(), probe.end()) -
                                     probe.begin());
  }
  // The [4, 64] band is the footer inverted index's own coverage: inside
  // it, a column would otherwise pay the codec AND the raw ~2 B/row
  // postings copy in the footer, and this scheme is that index folded into
  // the encoding (the build skips the footer copy for kPostings cells).
  // Above 64 nothing is saved; below 4 the two-value/kFreq family owns the
  // column.
  if (distinct < 4 || distinct > 64 || distinct > n / 8) {
    return {};
  }
  // Floor before any sort: header + one bit per gap (the smallest Rice code
  // is a single stop bit). Over budget here, no shape of the data can fit.
  const uint64_t floor_bytes =
      PostingsHeaderBytes(distinct) + (((n - distinct) + 7) / 8);
  if (floor_bytes > budget) {
    return {};
  }
  // Sort (value, row) pairs: dict order AND each list's posting order (ties
  // break by row) in one pass -- the SortDict pair-sort convention.
  thread_local std::vector<std::pair<int64_t, uint32_t>> order;
  order.clear();
  order.resize(n);
  for (uint32_t i = 0; i < n; ++i) {
    order[i] = {vals[i], i};
  }
  if (!std::is_sorted(order.begin(), order.end())) {
    SortValueRowPairs(order);
  }
  // List scan: counts, firsts, Rice parameters, and the per-list byte
  // offsets (each list's sub-stream is byte-aligned so the u16 jump table
  // hands out flush cursors). The exact extent is needed for the budget
  // check and the economic gate before any byte is written.
  thread_local std::vector<uint32_t> counts;
  thread_local std::vector<uint32_t> firsts;
  thread_local std::vector<uint32_t> ks;
  thread_local std::vector<uint32_t> offs;
  counts.clear();
  firsts.clear();
  ks.clear();
  offs.clear();
  uint64_t stream_bits = 0;
  for (uint32_t i = 0; i < n;) {
    const int64_t v = order[i].first;
    const uint32_t start = i;
    while (i < n && order[i].first == v) {
      ++i;
    }
    const uint32_t cnt = i - start;
    counts.push_back(cnt);
    firsts.push_back(order[start].second);
    offs.push_back(static_cast<uint32_t>(stream_bits));
    // Rice parameter from the mean gap: k = floor(log2(mean_gap + 1)),
    // clamped to the nibble. count == 1 stores k = 0 (no codes at all).
    uint32_t k = 0;
    if (cnt > 1) {
      const uint64_t mean = ((static_cast<uint64_t>(order[i - 1].second) -
                              static_cast<uint64_t>(order[start].second)) /
                             cnt) +
                            1;
      k = static_cast<uint32_t>(BitsNeeded(mean + 1)) - 1;
      k = std::min<uint32_t>(k, 15);
    }
    ks.push_back(k);
    for (uint32_t t = start + 1; t < i; ++t) {
      const uint64_t gap = static_cast<uint64_t>(order[t].second) -
                           static_cast<uint64_t>(order[t - 1].second);
      // The code below emits (gap - 1) as (q ones, stop, k bits) with
      // q = (gap - 1) >> k -- the scan must price THAT code, not
      // (gap >> k): at gaps that are exact multiples of 2^k the two differ
      // by one bit per occurrence and every recorded offset drifts.
      stream_bits += ((gap - 1) >> k) + 1 + k;
    }
    stream_bits = (stream_bits + 7) & ~UINT64_C(7);  // byte-align the list
  }
  // The jump table is u16 bit offsets: a stream past 64 Ki bit cannot
  // address its own lists (and cannot fit a row group's budget anyway).
  if (stream_bits > UINT64_C(65535)) {
    return {};
  }
  const uint64_t total = PostingsHeaderBytes(distinct) + (stream_bits / 8);
  if (total > budget) {
    return {};
  }
  // No economic gate against kDict: on a uniform column the flat code width
  // wins the byte race and the cost model keeps the dictionary family's
  // niche (postings price at a higher decode tier); where skew makes
  // per-list Rice far narrower than log2(d), the size race itself decides.
  // This mirrors kHuffDict, which enters the race on any skewed categorical
  // and only wins where its entropy bitstream is competitive.
  EncodedCol e;
  e.buf.reserve(total);
  // Header planes through a raw pointer inside the reserved capacity (the
  // PLAIN baseline convention): the layout ParsePostingsRegion reads back.
  e.buf.resize(PostingsHeaderBytes(distinct));
  {
    char* wp = e.buf.data();
    *wp++ = static_cast<char>(static_cast<uint8_t>(distinct >> 8));
    *wp++ = static_cast<char>(static_cast<uint8_t>(distinct & 0xFF));
    for (uint32_t c = 0; c < distinct; ++c) {
      const uint64_t be = htobe64(static_cast<uint64_t>(vals[firsts[c]]));
      std::memcpy(wp, &be, sizeof(be));
      wp += sizeof(be);
    }
    for (uint32_t c = 0; c < distinct; ++c) {
      *wp++ = static_cast<char>(static_cast<uint8_t>(counts[c] >> 8));
      *wp++ = static_cast<char>(static_cast<uint8_t>(counts[c] & 0xFF));
    }
    for (uint32_t c = 0; c < distinct; c += 2) {
      const auto lo = static_cast<uint8_t>(ks[c]);
      const uint8_t hi = c + 1 < distinct ? static_cast<uint8_t>(ks[c + 1]) : 0;
      *wp++ = static_cast<char>(static_cast<uint8_t>((hi << 4) | lo));
    }
    for (uint32_t c = 0; c < distinct; ++c) {
      *wp++ = static_cast<char>(static_cast<uint8_t>(firsts[c] >> 8));
      *wp++ = static_cast<char>(static_cast<uint8_t>(firsts[c] & 0xFF));
    }
    for (uint32_t c = 0; c < distinct; ++c) {
      *wp++ = static_cast<char>(static_cast<uint8_t>(offs[c] >> 8));
      *wp++ = static_cast<char>(static_cast<uint8_t>(offs[c] & 0xFF));
    }
  }
  // Rice stream: one byte-aligned sub-stream per list, gaps from the
  // ascending posting order the pair sort produced. q one-bits, a zero
  // stop, then k remainder bits -- the exact code PostingsBitCursor::Rice
  // replays.
  e.buf.resize(total);
  {
    char* wp = e.buf.data() + PostingsHeaderBytes(distinct);
    uint64_t acc = 0;  // pending bits (bit63 = next to emit)
    uint32_t held = 0;
    const auto emit = [&](uint32_t bit) {
      if (bit != 0) {
        acc |= UINT64_C(1) << (63 - held);
      }
      if (++held == 8) {
        *wp++ = static_cast<char>(acc >> 56);
        acc <<= 8;
        held = 0;
      }
    };
    uint32_t i = 0;
    for (uint32_t c = 0; c < distinct; ++c) {
      const uint32_t cnt = counts[c];
      const uint32_t k = ks[c];
      for (uint32_t t = 1; t < cnt; ++t) {
        const uint64_t x = static_cast<uint64_t>(order[i + t].second) -
                           static_cast<uint64_t>(order[i + t - 1].second) - 1;
        for (uint32_t b = 0; b < (x >> k); ++b) {
          emit(1);
        }
        emit(0);
        for (uint32_t b = 0; b < k; ++b) {
          emit(static_cast<uint32_t>((x >> (k - 1 - b)) & 1));
        }
      }
      i += cnt;
      // Pad to the byte boundary (a count-1 list pads nothing): the next
      // list's sub-stream starts flush, which is exactly the position its
      // jump-table offset records.
      while (held != 0) {
        emit(0);
      }
    }
  }
  e.scheme = Scheme::kPostings;
  e.param = distinct;
  return e;
}

// Exact arithmetic-progression model (kArith): a + i*d with a u16-capped
// exception list. The LECO-family linear model with EXACT-model enforcement:
// rows off the line are stored verbatim in an inner region, so the on-line
// part answers predicates as a closed-form index window, EQ as one candidate
// index, and SUM as n*a + d*n(n-1)/2 -- capabilities no delta scheme offers
// without materializing the prefix. Wins on fixed-rate clocks, sequence
// columns, and ramps with rare outliers (kDeltaBitweave's dd == 0 input).
//   [a i64 BE][d i64 BE][ischeme u8][iparam u32 BE][nexc u16 BE]
//   [bitmap ceil(n/8) B MSB-first: bit i = 1 iff row i is on-line]
//   [inner region: nexc exception values in row order; omitted at nexc==0]
//   param = nexc.
// Applicability: d is the MODE of the consecutive differences (anchored at
// the first run of that step, so ramps may start mid-block), and the line
// must cover at least HALF the block -- below that, delta schemes win on
// size anyway and the region would be exception payload with a dead model.
// d == 0 always bails: identical bytes to kFreq (minus the dead d field)
// and kConst owns the all-same case.
EncodedCol EncArithI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n < 2) {
    return {};
  }
  if (n > 2) {
    const uint64_t step =
        static_cast<uint64_t>(vals[1]) - static_cast<uint64_t>(vals[0]);
    bool exact = step != 0;
    for (uint32_t i = 1; exact && i < n; ++i) {
      exact =
          static_cast<uint64_t>(vals[i]) - static_cast<uint64_t>(vals[i - 1]) ==
          step;
    }
    if (exact) {
      EncodedCol e;
      e.scheme = Scheme::kArith;
      e.param = 0;
      AppendI64(e.buf, vals[0]);
      AppendI64(e.buf, static_cast<int64_t>(step));
      e.buf.push_back(static_cast<char>(Scheme::kConst));
      AppendBE32(e.buf, 0);
      AppendBE16(e.buf, 0);
      const size_t bitmap_bytes = (static_cast<size_t>(n) + 7) / 8;
      e.buf.resize(e.buf.size() + bitmap_bytes, static_cast<char>(0xFF));
      if ((n & 7U) != 0) {
        e.buf.back() = static_cast<char>(0xFFU << (8U - (n & 7U)));
      }
      return e;
    }
  }
  // Wrapping u64 model arithmetic: a + i*d for extreme inputs wraps instead
  // of invoking signed-overflow UB, and the decoder mirrors it exactly.
  // Flat open-addressed counts instead of std::unordered_map: the mode scan
  // visits every diff of every row group, and the node-per-key map paid an
  // allocation on each new step value. The mode selection reads only the
  // running counts in row order (strict '>'), never iteration order, so any
  // map with the same upsert semantics yields the same mode_d/mode_cnt.
  thread_local FlatMapI64 diff_freq(0);
  diff_freq.ResetFor(n);
  uint64_t mode_d = 0;
  uint32_t mode_cnt = 0;
  for (uint32_t i = 0; i + 1 < n; ++i) {
    const uint64_t di =
        static_cast<uint64_t>(vals[i + 1]) - static_cast<uint64_t>(vals[i]);
    if (di == 0) {
      continue;  // constant step is kFreq/kConst territory
    }
    uint64_t* slot = diff_freq.Find(static_cast<int64_t>(di));
    uint32_t c = 0;
    if (slot == nullptr) {
      c = 1;
      diff_freq.InsertNew(static_cast<int64_t>(di), 1);
    } else {
      c = static_cast<uint32_t>(++*slot);
    }
    if (c > mode_cnt) {
      mode_cnt = c;
      mode_d = di;
    }
  }
  // Fewer than two occurrences of one step cannot put >= 2 consecutive
  // rows on a line: nothing worth modeling.
  if (mode_cnt < 2) {
    return {};
  }
  // Anchor on the first run of the modal step: on-line rows are i with
  // v_i == a + i*d, and a run starting at j fixes a = vals[j] - j*d.
  size_t j = 0;
  while (j + 1 < n &&
         static_cast<uint64_t>(vals[j + 1]) - static_cast<uint64_t>(vals[j]) !=
             mode_d) {
    ++j;
  }
  const uint64_t a =
      static_cast<uint64_t>(vals[j]) - (static_cast<uint64_t>(j) * mode_d);
  uint32_t nexc = 0;
  for (uint32_t i = 0; i < n; ++i) {
    if (static_cast<uint64_t>(vals[i]) !=
        a + (static_cast<uint64_t>(i) * mode_d)) {
      ++nexc;
    }
  }
  // Applicability gate: the model must explain the majority of rows.
  if (nexc * 2 >= n) {
    return {};
  }
  if (nexc > UINT16_MAX) {
    return {};  // header cap, same as kFreq
  }
  thread_local std::vector<int64_t> exc;
  exc.clear();
  exc.reserve(nexc);
  for (uint32_t i = 0; i < n; ++i) {
    if (static_cast<uint64_t>(vals[i]) !=
        a + (static_cast<uint64_t>(i) * mode_d)) {
      exc.push_back(vals[i]);
    }
  }
  EncodedCol inner;
  inner.scheme = Scheme::kConst;  // nexc == 0: no inner region at all
  if (nexc > 0) {
    inner = EncodeInnerI64(exc, budget);
    if (inner.buf.empty()) {
      return {};
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kArith;
  e.param = nexc;
  AppendI64(e.buf, static_cast<int64_t>(a));
  AppendI64(e.buf, static_cast<int64_t>(mode_d));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner.scheme)));
  AppendBE32(e.buf, inner.param);
  AppendBE16(e.buf, static_cast<uint16_t>(nexc));
  const size_t bmp_off = e.buf.size();
  e.buf.insert(e.buf.end(), (static_cast<size_t>(n) + 7) / 8, '\0');
  for (uint32_t i = 0; i < n; ++i) {
    if (static_cast<uint64_t>(vals[i]) ==
        a + (static_cast<uint64_t>(i) * mode_d)) {
      e.buf[bmp_off + (i >> 3)] =
          static_cast<char>(e.buf[bmp_off + (i >> 3)] | (0x80 >> (i & 7)));
    }
  }
  e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  return e;
}

// Piecewise-linear exact model (kPla): kArith's per-region line generalized
// to nseg variable-length lines. Wins where kArith bails at its
// majority-off-line gate: multi-rate series (slope changes, plateau+ramp
// mixes, sawtooth clocks) collapse to 20B per segment, and every predicate
// still folds to closed-form index windows -- O(nseg + nexc) per row group,
// no per-row evaluation. Segmentation follows the greedy maximal equal-step
// runs, then (1) runs shorter than 3 rows are absorbed into the preceding
// segment as exceptions (a lone outlier must not cost a 20B table entry)
// and (2) adjacent segments that end up collinear are merged -- without the
// merge, a noisy ramp splits at every absorbed outlier and nseg explodes.
// Gates: nseg >= 2 (a single line IS kArith; keeps the niches disjoint),
// nseg <= n/4 (average span >= 4 rows or the table cannot amortize),
// nexc*2 < n (the model must explain the majority, mirroring kArith).
//   [nseg u16 BE][nseg x (first i64 BE, d i64 BE, end u32 BE)]
//   [ischeme u8][iparam u32 BE][nexc u16 BE]
//   [bitmap ceil(n/8) B MSB-first: bit i = 1 iff row i is on-line]
//   [inner region: nexc exception values in row order; omitted at nexc==0]
//   param = nseg. `end` is exclusive and spans exception rows too; the
//   bitmap, not the table, decides on-line.
EncodedCol EncPlaI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n < 8) {
    return {};
  }
  // Wrapping u64 diffs: the model arithmetic is two's-complement (kArith's
  // convention), so a wrapping sequence is still one exact run.
  struct PlaRun {
    uint32_t start;
    uint32_t end;  // exclusive, from the greedy pass
    uint64_t first;
    uint64_t d;
  };
  std::vector<PlaRun> runs;
  for (uint32_t i = 0; i < n;) {
    const auto first = static_cast<uint64_t>(vals[i]);
    uint64_t d = 0;
    uint32_t j = i + 1;
    if (j < n) {
      d = static_cast<uint64_t>(vals[j]) - first;
      ++j;
      while (j < n && static_cast<uint64_t>(vals[j]) -
                              static_cast<uint64_t>(vals[j - 1]) ==
                          d) {
        ++j;
      }
    }
    runs.push_back(PlaRun{.start = i, .end = j, .first = first, .d = d});
    i = j;
  }
  // Short-run absorption + collinear merge in one pass over the kept runs.
  // A kept run's `end` extends over absorbed spans (those rows become
  // exceptions via the bitmap); `first`/`d` stay anchored at the run start,
  // so kept-run rows remain exactly on-line.
  std::vector<PlaRun> kept;
  kept.reserve(runs.size());
  for (const PlaRun& run : runs) {
    if (run.end - run.start < 3 && !kept.empty()) {
      kept.back().end = run.end;  // absorbed: <= 2 exception rows
      continue;
    }
    // Merge into the previous kept segment when collinear with a shared
    // junction point: same step AND the junction row lies on the previous
    // line (then every on-line row here lies on it too).
    if (!kept.empty()) {
      PlaRun& prev = kept.back();
      const uint64_t junction =
          prev.first + (static_cast<uint64_t>(run.start - prev.start) * prev.d);
      if (prev.d == run.d && junction == run.first) {
        prev.end = run.end;
        continue;
      }
    }
    kept.push_back(run);
  }
  const auto nseg = static_cast<uint32_t>(kept.size());
  if (nseg < 2 || nseg > UINT16_MAX || nseg > n / 4) {
    return {};
  }
  // One cursor walk classifies every row: on-line (bitmap bit set) or an
  // exception. Kept starts are non-decreasing and tile [0, n] through the
  // extended ends, so the cursor advances monotonically.
  std::vector<uint8_t> bmp((static_cast<size_t>(n) + 7) / 8, 0);
  std::vector<int64_t> exc;
  exc.reserve(n / 16);
  {
    size_t j = 0;
    uint32_t start = 0;
    for (uint32_t i = 0; i < n; ++i) {
      while (i >= kept[j].end) {
        start = kept[j].end;
        ++j;
      }
      const auto model = static_cast<int64_t>(
          kept[j].first + (static_cast<uint64_t>(i - start) * kept[j].d));
      if (vals[i] == model) {
        bmp[i >> 3] = static_cast<uint8_t>(bmp[i >> 3] | (0x80 >> (i & 7)));
      } else {
        exc.push_back(vals[i]);
      }
    }
  }
  const auto nexc = static_cast<uint32_t>(exc.size());
  if (nexc * 2U >= n || nexc > UINT16_MAX) {
    return {};
  }
  EncodedCol inner;
  inner.scheme = Scheme::kConst;  // nexc == 0: no inner region at all
  if (nexc > 0) {
    inner = EncodeInnerI64(exc, budget);
    if (inner.buf.empty()) {
      return {};
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kPla;
  e.param = nseg;
  AppendBE16(e.buf, static_cast<uint16_t>(nseg));
  for (const PlaRun& seg : kept) {
    AppendI64(e.buf, static_cast<int64_t>(seg.first));
    AppendI64(e.buf, static_cast<int64_t>(seg.d));
    AppendBE32(e.buf, seg.end);
  }
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner.scheme)));
  AppendBE32(e.buf, inner.param);
  AppendBE16(e.buf, static_cast<uint16_t>(nexc));
  e.buf.insert(e.buf.end(), reinterpret_cast<const char*>(bmp.data()),
               reinterpret_cast<const char*>(bmp.data()) + bmp.size());
  e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  return e;
}

// Modular affine exact model (kMod): v_i = (a + b*i) mod m on every on-model
// row, kArith's exception envelope (bitmap + inner region) around it. The
// wire format and encoder gate are pinned by Scheme::kMod's enum comment.
// Candidate moduli come from opposite-sign diff pairs: consecutive on-model
// rows differ by +b or by b - m (one wrap), so any positive/negative diff
// pair fixes m = pos - neg and b = pos exactly. The first kModMaxDistinct
// distinct diffs are kept and every opposite-sign pair among them becomes a
// candidate (deduped, capped); each candidate is verified by one add-and-wrap
// walk and the fewest-exception wrapped candidate wins. Gates: n >= 8, every
// value non-negative, m in [2, 2^63], nexc*2 < n (kArith's majority-on-model
// rule), and at least one ON-MODEL wrap -- a wrap-free window is exactly
// kArith's affine sequence and must stay its niche.
EncodedCol EncModI64(const std::vector<int64_t>& vals, size_t budget) {
  constexpr size_t kModMaxDistinct = 16;
  constexpr size_t kModMaxCandidates = 8;
  const auto n = static_cast<uint32_t>(vals.size());
  if (n < 8) {
    return {};
  }
  // Region floor: a/b/m (3x8B) + ischeme/iparam/nexc (7B) + the on-model
  // bitmap; exceptions only add. Under-budget candidates are retired before
  // the per-candidate walks.
  if (31 + ((static_cast<size_t>(n) + 7) / 8) > budget) {
    return {};
  }
  for (int64_t v : vals) {
    if (v < 0) {
      return {};
    }
  }
  std::array<int64_t, kModMaxDistinct> distinct{};
  size_t ndistinct = 0;
  for (uint32_t i = 0; i + 1 < n && ndistinct < kModMaxDistinct; ++i) {
    const auto di = static_cast<int64_t>(static_cast<uint64_t>(vals[i + 1]) -
                                         static_cast<uint64_t>(vals[i]));
    bool seen = false;
    for (size_t j = 0; j < ndistinct && !seen; ++j) {
      seen = distinct[j] == di;
    }
    if (!seen) {
      distinct[ndistinct++] = di;
    }
  }
  struct ModCand {
    uint64_t b;
    uint64_t m;
  };
  std::array<ModCand, kModMaxCandidates> cands{};
  size_t ncands = 0;
  for (size_t p = 0; p < ndistinct && ncands < kModMaxCandidates; ++p) {
    if (distinct[p] <= 0) {
      continue;
    }
    const auto dp = static_cast<uint64_t>(distinct[p]);
    for (size_t q = 0; q < ndistinct && ncands < kModMaxCandidates; ++q) {
      if (distinct[q] >= 0) {
        continue;
      }
      const uint64_t m = dp - static_cast<uint64_t>(distinct[q]);
      // b < m holds structurally (q < 0), but the u64 subtraction can wrap
      // for extreme pairs; reject anything outside the storable range.
      if (m < 2 || m > (UINT64_C(1) << 63)) {
        continue;
      }
      bool dup = false;
      for (size_t c = 0; c < ncands && !dup; ++c) {
        dup = cands[c].b == dp && cands[c].m == m;
      }
      if (dup) {
        continue;
      }
      cands[ncands].b = dp;
      cands[ncands].m = m;
      ++ncands;
    }
  }
  uint32_t best_exc = UINT32_MAX;
  uint64_t best_a = 0;
  uint64_t best_b = 0;
  uint64_t best_m = 0;
  for (size_t c = 0; c < ncands; ++c) {
    const uint64_t b = cands[c].b;
    const uint64_t m = cands[c].m;
    // The phase `a` is (vals[j] - b*j) mod m for ANY on-model row j; row 0
    // is often an exception itself (a bumped head), so try a handful of
    // early anchors and keep the fewest-exception phase per candidate.
    for (uint32_t anchor = 0; anchor < 4 && anchor < n; ++anchor) {
      const uint64_t a =
          (static_cast<uint64_t>(vals[anchor]) + m -
           static_cast<uint64_t>(static_cast<unsigned __int128>(b) * anchor %
                                 static_cast<unsigned __int128>(m))) %
          m;
      // The model trajectory is a, a+b, a+2b, ... with one conditional
      // subtract per step, so `cur` is strictly increasing until it first
      // reaches m and can only wrap after that. If even a + b*(n-1) never
      // reaches m, cur rises over the whole window and the wrapped verdict
      // -- the only path into `best` -- can never fire, so this anchor's
      // walk would be discarded no matter what exception count it posted.
      // Skip the walk; `best` stays byte-identical.
      if (static_cast<unsigned __int128>(a) +
              (static_cast<unsigned __int128>(b) * (n - 1)) <
          static_cast<unsigned __int128>(m)) {
        continue;
      }
      uint32_t exc = 0;
      bool wrapped = false;
      bool prev_on = false;
      uint64_t prev_val = 0;
      uint64_t cur = a;
      // An anchor only wins by ending strictly below BOTH the majority gate
      // (best_exc * 2 < n) and the running best; once the live count crosses
      // that bound this (candidate, anchor) pair can no longer be selected,
      // so stop the scan. The stored best stays identical: a pair that
      // cannot win never updates it, and its wrapped-model verdict is only
      // consulted for selection.
      uint32_t exc_limit = n / 2U;
      exc_limit = std::min(best_exc, exc_limit);
      for (uint32_t i = 0; i < n; ++i) {
        const auto v = static_cast<uint64_t>(vals[i]);
        if (v == cur) {
          if (prev_on && cur < prev_val) {
            wrapped = true;
          }
          prev_on = true;
          prev_val = cur;
        } else {
          ++exc;
          prev_on = false;
          if (exc >= exc_limit) {
            break;
          }
        }
        cur += b;  // cur < m and b < m: one conditional subtract steps it
        if (cur >= m) {
          cur -= m;
        }
      }
      if (exc >= exc_limit) {
        continue;
      }
      if (wrapped && exc < best_exc) {
        best_exc = exc;
        best_a = a;
        best_b = b;
        best_m = m;
      }
      // A wrapped phase with zero exceptions is the global optimum (strict
      // '<' can never be beaten), so no further anchor can improve it.
      if (best_exc == 0) {
        break;
      }
    }
  }
  // The majority-on-model gate (kArith's) plus the header cap.
  if (best_exc == UINT32_MAX || best_exc * 2U >= n || best_exc > UINT16_MAX) {
    return {};
  }
  // Recollect the exception rows against the winning candidate.
  std::vector<int64_t> exc;
  exc.reserve(best_exc);
  uint64_t cur = best_a;
  for (uint32_t i = 0; i < n; ++i) {
    const auto v = static_cast<uint64_t>(vals[i]);
    if (v != cur) {
      exc.push_back(vals[i]);
    }
    cur += best_b;
    if (cur >= best_m) {
      cur -= best_m;
    }
  }
  EncodedCol inner;
  inner.scheme = Scheme::kConst;  // nexc == 0: no inner region at all
  if (best_exc > 0) {
    inner = EncodeInnerI64(exc, budget);
    if (inner.buf.empty()) {
      return {};
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kMod;
  e.param = best_exc;
  AppendBE64(e.buf, best_a);
  AppendBE64(e.buf, best_b);
  AppendBE64(e.buf, best_m);
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner.scheme)));
  AppendBE32(e.buf, inner.param);
  AppendBE16(e.buf, static_cast<uint16_t>(best_exc));
  const size_t bmp_off = e.buf.size();
  e.buf.insert(e.buf.end(), (static_cast<size_t>(n) + 7) / 8, '\0');
  cur = best_a;
  for (uint32_t i = 0; i < n; ++i) {
    const auto v = static_cast<uint64_t>(vals[i]);
    if (v == cur) {
      e.buf[bmp_off + (i >> 3)] =
          static_cast<char>(e.buf[bmp_off + (i >> 3)] | (0x80 >> (i & 7)));
    }
    cur += best_b;
    if (cur >= best_m) {
      cur -= best_m;
    }
  }
  e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  return e;
}

// Interleaved arithmetic lanes (kLane): the row-major interleave of K
// arithmetic progressions, kArith's exception envelope around the lane grid.
// Candidate lane counts 2..min(16, n/4) are each verified by one wrapped
// walk that aborts at the majority gate; the smallest estimated wire (the
// 16-bytes-per-lane plane plus a 2-bytes-per-exception inner estimate)
// wins, then the winner is collected and encoded for real. Gates: n >= 16,
// every lane carries at least 4 rows (nd <= n/4, so the per-lane header
// amortizes), and nexc * 2 < n (kArith's majority-on-model rule) -- below
// the gates the column belongs to kArith (nd == 1), the flat bit-packers
// (heavy noise), or the dictionary family (small alphabets).
EncodedCol EncLaneI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n < 16) {
    return {};
  }
  // Region floor: the smallest shape is nd=2 (1B lane count + 2x16B
  // base/step pairs + 7B inner-scheme header) plus the on-model bitmap. A
  // budget under this can never fit any lane encoding, so the 2..nd_cap
  // walk sweep is skipped whole.
  if (1 + 32 + 7 + ((static_cast<size_t>(n) + 7) / 8) > budget) {
    return {};
  }
  const uint32_t nd_cap = std::min<uint32_t>(16, n / 4);
  uint32_t best_nd = 0;
  uint32_t best_exc = UINT32_MAX;
  uint32_t best_score = UINT32_MAX;
  std::array<uint64_t, 16> best_base{};
  std::array<uint64_t, 16> best_step{};
  // One wrapped lane walk for a compile-time lane count: the first ND rows
  // pin the bases, the next ND pin the steps, and the rest verify with the
  // per-lane state (base/step/cur) held in registers -- the historical
  // runtime-ND walk paid a tick array branch and a modulo per row for the
  // same arithmetic. exc == n/2 aborts exactly like the majority gate.
  auto lane_walk = [&](auto nd_tag, uint64_t* base_out, uint64_t* step_out) {
    constexpr uint32_t ND = decltype(nd_tag)::value;
    const auto* const v = reinterpret_cast<const uint64_t*>(vals.data());
    std::array<uint64_t, ND> base{};
    std::array<uint64_t, ND> step{};
    std::array<uint64_t, ND> cur{};
    for (uint32_t l = 0; l < ND; ++l) {
      base[l] = v[l];
      cur[l] = v[l];
    }
    for (uint32_t i = ND; i < 2 * ND; ++i) {
      const uint32_t l = i - ND;
      step[l] = v[i] - cur[l];
      cur[l] = v[i];
    }
    const uint32_t lim = n / 2U;
    uint32_t exc = 0;
#ifdef __AVX2__
    if constexpr ((ND & (ND - 1U)) == 0U && ND >= 4U) {
      // AVX2 block walk for power-of-two lane counts: one block covers one
      // tick of every lane (rows i..i+ND-1 map to lanes 0..ND-1), so the
      // model expectation is a whole-register cur+step and cur advances by
      // step unconditionally -- exactly the scalar body's update. The
      // majority-gate break is per-row in the scalar form; when a block
      // crosses `lim` the block is replayed scalar from its start so the
      // recorded exc (and the exact break point feeding the
      // exc*2 >= n scoring gate) stays identical to the historical walk.
      // ND == 2 keeps the scalar path: a 2x i64 block saves nothing over
      // the two scalar iterations it replaces.
      constexpr uint32_t R = ND / 4U;
      std::array<__m256i, R> cv{};
      std::array<__m256i, R> sv{};
      for (uint32_t r = 0; r < R; ++r) {
        // The register state starts at the PINNING cursor, not the row
        // origin: the scalar walk's cur[] already advanced to v[ND..2ND)
        // (tick 1) while base[] still holds tick 0, so seeding cv from base
        // would run the whole block walk one tick behind the observation.
        cv[r] = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(cur.data() + (4U * r)));
        sv[r] = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(step.data() + (4U * r)));
      }
      uint32_t i = 2U * ND;
      bool crossed = false;
      for (; i + ND <= n; i += ND) {
        const uint32_t before = exc;
        std::array<__m256i, R> ex{};
        uint32_t mism = 0;
        uint32_t b = i;
        for (uint32_t r = 0; r < R; ++r) {
          const __m256i vv =
              _mm256_loadu_si256(reinterpret_cast<const __m256i*>(v + b));
          ex[r] = _mm256_add_epi64(cv[r], sv[r]);
          // cmpeq is 1s on match; mismatches per 4-lane reg = 4 - popcount.
          const uint32_t m4 =
              static_cast<uint32_t>(_mm256_movemask_pd(
                  _mm256_castsi256_pd(_mm256_cmpeq_epi64(vv, ex[r])))) &
              0xFU;
          mism += 4U - static_cast<uint32_t>(__builtin_popcount(m4));
          b += 4U;
        }
        exc += mism;
        if (exc >= lim) {
          exc = before;
          crossed = true;
          break;
        }
        for (uint32_t r = 0; r < R; ++r) {
          cv[r] = ex[r];
        }
      }
      if (crossed) {
        // Replay the crossing block row by row from the last committed
        // state so the recorded exc lands exactly where the scalar walk
        // stops (exc == lim) and the cur bookkeeping stays identical.
        for (uint32_t r = 0; r < R; ++r) {
          _mm256_storeu_si256(reinterpret_cast<__m256i*>(cur.data() + (4U * r)),
                              cv[r]);
        }
        uint32_t l = 0;
        for (uint32_t j = i; j < n; ++j) {
          const uint64_t exp = cur[l] + step[l];
          if (v[j] != exp) {
            ++exc;
            if (exc >= lim) {
              break;
            }
          }
          cur[l] = exp;
          if (++l == ND) {
            l = 0;
          }
        }
        std::copy(base.begin(), base.end(), base_out);
        std::copy(step.begin(), step.end(), step_out);
        return exc;
      }
      for (uint32_t r = 0; r < R; ++r) {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(cur.data() + (4U * r)),
                            cv[r]);
      }
      uint32_t l = 0;
      for (; i < n; ++i) {
        const uint64_t exp = cur[l] + step[l];
        if (v[i] != exp) {
          ++exc;
          if (exc >= lim) {
            break;
          }
        }
        cur[l] = exp;
        if (++l == ND) {
          l = 0;
        }
      }
      std::copy(base.begin(), base.end(), base_out);
      std::copy(step.begin(), step.end(), step_out);
      return exc;
    }
#endif
    uint32_t l = 0;
    for (uint32_t i = 2 * ND; i < n; ++i) {
      const uint64_t exp = cur[l] + step[l];
      if (v[i] != exp) {
        ++exc;
        if (exc >= lim) {
          break;
        }
      }
      cur[l] = exp;
      if (++l == ND) {
        l = 0;
      }
    }
    std::copy(base.begin(), base.end(), base_out);
    std::copy(step.begin(), step.end(), step_out);
    return exc;
  };
  for (uint32_t nd = 2; nd <= nd_cap; ++nd) {
    std::array<uint64_t, 16> base{};
    std::array<uint64_t, 16> step{};
    uint32_t exc = 0;
    switch (nd) {
      case 2:
        exc = lane_walk(std::integral_constant<uint32_t, 2>{}, base.data(),
                        step.data());
        break;
      case 3:
        exc = lane_walk(std::integral_constant<uint32_t, 3>{}, base.data(),
                        step.data());
        break;
      case 4:
        exc = lane_walk(std::integral_constant<uint32_t, 4>{}, base.data(),
                        step.data());
        break;
      case 5:
        exc = lane_walk(std::integral_constant<uint32_t, 5>{}, base.data(),
                        step.data());
        break;
      case 6:
        exc = lane_walk(std::integral_constant<uint32_t, 6>{}, base.data(),
                        step.data());
        break;
      case 7:
        exc = lane_walk(std::integral_constant<uint32_t, 7>{}, base.data(),
                        step.data());
        break;
      case 8:
        exc = lane_walk(std::integral_constant<uint32_t, 8>{}, base.data(),
                        step.data());
        break;
      case 9:
        exc = lane_walk(std::integral_constant<uint32_t, 9>{}, base.data(),
                        step.data());
        break;
      case 10:
        exc = lane_walk(std::integral_constant<uint32_t, 10>{}, base.data(),
                        step.data());
        break;
      case 11:
        exc = lane_walk(std::integral_constant<uint32_t, 11>{}, base.data(),
                        step.data());
        break;
      case 12:
        exc = lane_walk(std::integral_constant<uint32_t, 12>{}, base.data(),
                        step.data());
        break;
      case 13:
        exc = lane_walk(std::integral_constant<uint32_t, 13>{}, base.data(),
                        step.data());
        break;
      case 14:
        exc = lane_walk(std::integral_constant<uint32_t, 14>{}, base.data(),
                        step.data());
        break;
      case 15:
        exc = lane_walk(std::integral_constant<uint32_t, 15>{}, base.data(),
                        step.data());
        break;
      default:
        exc = lane_walk(std::integral_constant<uint32_t, 16>{}, base.data(),
                        step.data());
        break;
    }
    if (exc * 2U >= n || exc > UINT16_MAX) {
      continue;
    }
    // Wire estimate: the lane plane dominates, the exception inner region
    // is priced at 2B per exception (between a compressed and a raw list).
    const uint32_t score = (16U * nd) + (2U * exc);
    if (best_nd == 0 || score < best_score) {
      best_nd = nd;
      best_exc = exc;
      best_score = score;
      best_base = base;
      best_step = step;
    }
  }
  if (best_nd == 0) {
    return {};
  }
  // Recollect the exception values and the on-model bitmap against the
  // winning lane grid (one walk; ticks 0 and 1 are structurally on-model).
  // The RECOLLECTED list is what the wire carries -- the decode contract is
  // "bitmap bit ? base+step*tick : next exception value" over ALL rows -- so
  // its count is authoritative. The scoring walk above aborts at exc == n/2
  // as a cost heuristic, and past that break the model keeps drifting while
  // the recollection keeps scoring, so the two counts can disagree exactly
  // when the walk stopped early (fuzz seed: nd=2 over a period-9 column,
  // n=63, walk exc=31 vs recollected 54 -- param said 31, the bitmap had 9
  // on-model bits, ValidateWalk's popcount pin rejected the cell). Re-runs
  // the majority gate on the honest count; a column whose per-lane model
  // degrades past the halfway mark simply declines.
  std::vector<int64_t> exc;
  exc.reserve(best_exc);
  std::string bmp(static_cast<size_t>(n + 7) / 8, '\0');
  {
    std::array<uint32_t, 16> tick{};
    uint32_t l = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const uint32_t t = tick[l];
      if (t < 2 ||
          static_cast<uint64_t>(vals[i]) == best_base[l] + (best_step[l] * t)) {
        bmp[i >> 3] = static_cast<char>(bmp[i >> 3] | (0x80 >> (i & 7)));
      } else {
        exc.push_back(vals[i]);
      }
      ++tick[l];
      if (++l == best_nd) {
        l = 0;
      }
    }
  }
  if (exc.size() > UINT16_MAX || exc.size() * 2U >= n) {
    return {};
  }
  EncodedCol inner;
  inner.scheme = Scheme::kConst;  // nexc == 0: no inner region at all
  if (best_exc > 0) {
    inner = EncodeInnerI64(exc, budget);
    if (inner.buf.empty()) {
      return {};
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kLane;
  e.param = static_cast<uint32_t>(exc.size());
  e.buf.push_back(static_cast<char>(best_nd));
  for (uint32_t l = 0; l < best_nd; ++l) {
    AppendBE64(e.buf, best_base[l]);
    AppendBE64(e.buf, best_step[l]);
  }
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner.scheme)));
  AppendBE32(e.buf, inner.param);
  AppendBE16(e.buf, static_cast<uint16_t>(best_exc));
  e.buf.insert(e.buf.end(), bmp.begin(), bmp.end());
  e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  return e;
}

// Sparse null-majority encoding (kSparse): stores only the non-null rows as
// a strictly increasing FOR-BA position list plus a dense inner-encoded
// value list. Nulls still decode as raw 0 (the engine-wide convention), so
// the win is purely capacity: a column that is 95% NULL pays ~2B per live
// row instead of a full payload slot for every row.
//   [vscheme u8][vparam u32 BE][vlen u32 BE][pos_bpv u8][nnz u32 BE]
//   [positions region: FOR-BA layout (base i64 BE + nnz x pos_bpv LE)]
//   [values region: vlen bytes, leaf-scheme inner region, nnz entries]
//   param = nnz (echoed in the header, like kRleX's runs).
EncodedCol EncSparseI64(const std::vector<int64_t>& vals,
                        const std::vector<uint8_t>& null_bmp, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  // A missing/short bitmap means "no nulls recorded": without it every row
  // would land in the position list and the scheme is pure overhead.
  if (n == 0 || null_bmp.size() < (static_cast<size_t>(n) + 7) / 8) {
    return {};
  }
  std::vector<int64_t> pos;
  std::vector<int64_t> dense;
  pos.reserve(n);
  dense.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    const bool is_null = (null_bmp[i >> 3] & (0x80 >> (i & 7))) != 0;
    if (!is_null) {
      pos.push_back(static_cast<int64_t>(i));
      dense.push_back(vals[i]);
    }
  }
  const auto nnz = static_cast<uint32_t>(pos.size());
  // nnz == n is unreachable (the caller gates on has_null); nnz == 0 is an
  // all-null column where kConst's 8B is unbeatable. Both bail.
  if (nnz == 0 || nnz == n) {
    return {};
  }
  // 14-byte header + the FOR-BA position list (>= 8 + nnz bytes) + a
  // non-empty values region. The values region may be kConst, so nnz bytes
  // is all that can be claimed as a floor.
  if (14 + 8 + static_cast<size_t>(nnz) + 1 > budget) {
    return {};
  }
  EncodedCol pe = EncForBA(pos, budget);
  if (pe.buf.empty()) {
    return {};
  }
  EncodedCol ve = EncodeInnerI64(dense, budget);
  if (ve.buf.empty()) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kSparse;
  e.param = nnz;
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(ve.scheme)));
  AppendBE32(e.buf, ve.param);
  AppendBE32(e.buf, static_cast<uint32_t>(ve.buf.size()));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(pe.param)));
  AppendBE32(e.buf, nnz);
  e.buf.insert(e.buf.end(), pe.buf.begin(), pe.buf.end());
  e.buf.insert(e.buf.end(), ve.buf.begin(), ve.buf.end());
  return e;
}

// Entropy-coded FOR offsets (kAnsFor): rANS over the row-major
// little-endian byte stream of the FOR-BA offset array -- the only scheme
// that prices each byte at its marginal frequency instead of storing a
// fixed width per offset. Wins on skewed offsets (most rows near base, a
// few large) and on bpv slots whose high byte is constant (~0 bits).
// Layout:
//   [base i64 BE][bpv u8][prec u8][nsym u16 BE]
//   [nsym x (sym u8, freq u16 BE)]        -- strictly ascending syms,
//                                          freq >= 1, sum == 1<<prec
//   [ncp u16 BE][ncp x (state u32 BE, pos u32 BE)]
//   [slen u32 BE][rANS byte stream]
//   param = bpv. ncp = ceil(n/64). ckpt[k] = (decoder state, remaining-
//   byte cursor) immediately before decoding symbol 64k*bpv; the decoder
//   reads the stream backwards, so pos counts bytes not yet consumed.
EncodedCol EncAnsForI64(const std::vector<int64_t>& vals, size_t budget) {
  static_cast<void>(budget);
  const auto n = static_cast<uint32_t>(vals.size());
  if (n == 0) {
    return {};
  }
  int64_t mn = vals.front(), mx = vals.front();
  for (int64_t v : vals) {
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
  const uint64_t range = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
  if (range == 0) {
    return {};  // kConst owns the all-equal case
  }
  const auto bpv = static_cast<uint32_t>((BitsNeeded(range) + 7) / 8);
  const size_t nsym_total = static_cast<size_t>(n) * bpv;
  if (nsym_total > UINT32_MAX) {
    return {};
  }
  // Byte histogram over the little-endian offset stream.
  std::array<uint32_t, 256> cnt{};
  for (int64_t v : vals) {
    const uint64_t off = static_cast<uint64_t>(v) - static_cast<uint64_t>(mn);
    for (uint32_t b = 0; b < bpv; ++b) {
      ++cnt[static_cast<uint8_t>(off >> (8 * b))];
    }
  }
  // Normalize to M slots; every used symbol keeps freq >= 1.
  const uint32_t M = 1U << kAnsPrec;
  std::array<uint32_t, 256> f{};
  uint32_t sum = 0;
  for (int s = 0; s < 256; ++s) {
    if (cnt[s] == 0) {
      continue;
    }
    f[s] = static_cast<uint32_t>(
        std::max<uint64_t>(static_cast<uint64_t>(cnt[s]) * M / nsym_total, 1));
    sum += f[s];
  }
  // Fix the rounding residual by adjusting the largest bucket; f stays >= 1
  // for used symbols (dropping to 0 would make the symbol unencodable). The
  // candidate scan visits only used symbols -- one compact pass over cnt's
  // non-zero entries instead of all 256 buckets per adjustment.
  std::array<uint8_t, 256> used{};
  int nused = 0;
  for (int s = 0; s < 256; ++s) {
    if (cnt[s] != 0) {
      used[nused++] = static_cast<uint8_t>(s);
    }
  }
  // The residual normalizer picks the largest f (ties: smallest s) on every
  // adjustment; a priority queue over the used symbols replaces the linear
  // scan per adjustment. The linear form could never re-pick an entry it
  // skipped: sum only decreases while it is > M, so an f==1 entry skipped on
  // that side of the loop stays un-pickable until the loop ends, and f==0
  // never enters (only used symbols are enqueued). Ties on f pick the
  // smallest s because the queue orders by (f, 255 - s) -- same winner as
  // the strict '>' linear scan.
  std::priority_queue<std::pair<uint32_t, uint32_t>> norm;
  for (int i = 0; i < nused; ++i) {
    const int s = used[i];
    norm.emplace(f[s], 255U - static_cast<uint32_t>(s));
  }
  while (sum != M) {
    while (sum > M && !norm.empty() && norm.top().first == 1U) {
      norm.pop();
    }
    if (norm.empty()) {
      return {};
    }
    const uint32_t pick_key = norm.top().second;
    norm.pop();
    const int pick = static_cast<int>(255U - pick_key);
    if (sum > M) {
      --f[pick];
      --sum;
    } else {
      ++f[pick];
      ++sum;
    }
    norm.emplace(f[pick], 255U - static_cast<uint32_t>(pick));
  }
  // Entropy floor before the rANS encode: every push consumes
  // log2(M / f[s]) state bits and the state register absorbs at most ~8
  // (23-bit start minus 31-bit cap), so the emitted stream is at least
  // total_bits/8 bytes. floor(log2 f) underestimates each symbol by < 1
  // bit and the remaining slack absorbs that, so a floor past the budget
  // is provably unwinnable and skips the n*bpv push loop (the encoder is
  // attempted for every row group; wide-range noise columns always retired
  // here in practice).
  if (budget != kNoEncodeBudget) {
    uint64_t total_bits = 0;
    uint32_t used_syms = 0;
    for (int s = 0; s < 256; ++s) {
      if (cnt[s] == 0) {
        continue;
      }
      ++used_syms;
      total_bits += (static_cast<uint64_t>(cnt[s]) *
                     static_cast<uint64_t>(kAnsPrec + 1)) -
                    static_cast<uint64_t>(31 - __builtin_clz(f[s]));
    }
    const uint32_t ncp = (n + kAnsCkptRows - 1) / kAnsCkptRows;
    const uint64_t floor_bytes = (total_bits >> 3) + 18U +
                                 (3U * static_cast<uint64_t>(used_syms)) +
                                 (8U * static_cast<uint64_t>(ncp));
    // Slack: the <1 bit/symbol floor-log2 underestimate (n/8 bytes), plus
    // boundary slack so a candidate within a few bytes of the budget still
    // encodes and ties exactly as before.
    if (floor_bytes + (n >> 3) + 72 > budget) {
      return {};
    }
  }
  std::array<uint32_t, 256> cum{};
  std::array<uint32_t, 256> xmax{};
  uint32_t acc = 0;
  int nsym = 0;
  for (int s = 0; s < 256; ++s) {
    cum[s] = acc;
    acc += f[s];
    // x_max = ((L >> prec) << 8) * f[s]: below it, one push keeps the state
    // inside [L, 256*L) (the renorm invariant).
    xmax[s] = (f[s] != 0) ? ((kAnsL >> kAnsPrec) << 8) * f[s] : 0;
    if (f[s] != 0) {
      ++nsym;
    }
  }
  // Encode in reverse (rANS is a stack). After pushing the symbol at index
  // j, the state is exactly the decoder state at the START of decoding
  // symbol j, and the bytes emitted so far equal the decoder's remaining-
  // bytes cursor -- record that pair as the checkpoint for row j/bpv.
  const uint32_t ncp = (n + kAnsCkptRows - 1) / kAnsCkptRows;
  // Adaptive selection re-runs this encoder for every row group, so the
  // checkpoint tables and the byte stream live in thread_local buffers
  // grown once: the per-attempt malloc/free triplet would feed the
  // allocator for nothing whenever another scheme wins the column.
  thread_local std::vector<uint32_t> ck_state, ck_pos;
  thread_local std::vector<uint8_t> stream;
  ck_state.assign(ncp, 0);
  ck_pos.assign(ncp, 0);
  stream.clear();
  stream.reserve(nsym_total);
  uint32_t x = kAnsL;
  // Row-major walk (j = row*bpv + b): folds the historical per-symbol j/bpv
  // and j%bpv (divisions by a loop-invariant runtime value) into loop
  // indices, and the checkpoint modulo factorizes to b == 0 with
  // row % kAnsCkptRows == 0 (same recorded states, same order).
  uint32_t next_ckpt_row = ((n - 1) / kAnsCkptRows) * kAnsCkptRows;
  for (uint32_t row = n; row-- > 0;) {
    const uint64_t off =
        static_cast<uint64_t>(vals[row]) - static_cast<uint64_t>(mn);
    const bool ck_row = (row == next_ckpt_row);
    if (ck_row && row > 0) {
      next_ckpt_row -= kAnsCkptRows;
    }
    for (uint32_t b = bpv; b-- > 0;) {
      const auto sym = static_cast<uint8_t>(off >> (8 * b));
      while (x >= xmax[sym]) {
        stream.push_back(static_cast<uint8_t>(x & 0xFF));
        x >>= 8;
        // The stream is the bulk of the region and only grows, so once it
        // exceeds the budget on its own the candidate cannot win.
        if ((stream.size() & 0xFFF) == 0 && stream.size() > budget) {
          return {};
        }
      }
      x = (M * (x / f[sym])) + (x % f[sym]) + cum[sym];
      if (ck_row && b == 0) {
        const uint32_t k = row / kAnsCkptRows;
        ck_state[k] = x;
        ck_pos[k] = static_cast<uint32_t>(stream.size());
      }
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kAnsFor;
  e.param = bpv;
  AppendI64(e.buf, mn);
  e.buf.push_back(static_cast<char>(bpv));
  e.buf.push_back(static_cast<char>(kAnsPrec));
  AppendBE16(e.buf, static_cast<uint16_t>(nsym));
  for (int s = 0; s < 256; ++s) {
    if (f[s] == 0) {
      continue;
    }
    e.buf.push_back(static_cast<char>(s));
    AppendBE16(e.buf, static_cast<uint16_t>(f[s]));
  }
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t k = 0; k < ncp; ++k) {
    AppendBE32(e.buf, ck_state[k]);
    AppendBE32(e.buf, ck_pos[k]);
  }
  AppendBE32(e.buf, static_cast<uint32_t>(stream.size()));
  e.buf.insert(e.buf.end(), stream.begin(), stream.end());
  return e;
}

// Cascaded RLE (kRleX): splits the run table into a value list and a length
// list and compresses each as an inner region (values -> DICT on repeated
// run heads, counts -> FOR-BA/RLE on skewed run lengths). Beats kRle's flat
// 12B/run and kRleDict's fixed (code,len) pairs whenever the substreams
// compress further.
//   [vscheme u8][cscheme u8][runs u32 BE][vparam u32 BE][cparam u32 BE]
//   [vlen u32 BE][values region][counts region]
//   param = runs.
EncodedCol EncRleXI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  // Thread-local scratch: adaptive selection re-runs this encoder for every
  // row group, so the run tables are grown once and refilled per attempt.
  thread_local std::vector<int64_t> run_vals;
  thread_local std::vector<int64_t> run_lens;
  run_vals.clear();
  run_lens.clear();
  run_vals.reserve((vals.size() / 4) + 1);
  run_lens.reserve((vals.size() / 4) + 1);
  for (int64_t v : vals) {
    if (!run_vals.empty() && run_vals.back() == v) {
      ++run_lens.back();
    } else {
      run_vals.push_back(v);
      run_lens.push_back(1);
    }
  }
  const auto runs = static_cast<uint32_t>(run_vals.size());
  // Floor: the fixed 18-byte header + the 4-byte inner-length prefix. Both
  // inner regions can collapse to kConst (8 B), so no per-run term is a valid
  // lower bound here; the leaf chooser's own budget does the pruning.
  if (22 + 8 + 8 > budget) {
    return {};
  }
  EncodedCol ve = EncodeInnerI64(run_vals, budget);
  if (ve.buf.empty() || ve.buf.size() > UINT32_MAX) {
    return {};
  }
  // The length stream's budget tightens by the value region that is already
  // committed: total = 18 header bytes + ve + ce, so a ce candidate beyond
  // the remainder would push the region over budget. A region over budget
  // can never win the budgeted cascade (its cost exceeds prune_cost, the
  // incumbent the budget derives from), and forced-scheme calls run with no
  // budget, so retiring those attempts here changes no selection.
  if (ve.buf.size() + 18 > budget) {
    return {};
  }
  const size_t ce_budget = budget - ve.buf.size() - 18;
  EncodedCol ce = EncodeInnerI64(run_lens, ce_budget);
  if (ce.buf.empty()) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kRleX;
  e.param = runs;
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(ve.scheme)));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(ce.scheme)));
  AppendBE32(e.buf, runs);
  AppendBE32(e.buf, ve.param);
  AppendBE32(e.buf, ce.param);
  AppendBE32(e.buf, static_cast<uint32_t>(ve.buf.size()));
  e.buf.insert(e.buf.end(), ve.buf.begin(), ve.buf.end());
  e.buf.insert(e.buf.end(), ce.buf.begin(), ce.buf.end());
  return e;
}

// Cascaded dictionary (kDictX): the code stream is an inner int64 region
// (RLE on clustered codes, FOR-BA on dense low codes, ...) and the
// dictionary is a flat 8B-entry pool. Beats kDict whenever codes compress
// below the packed-cw floor.
//   [d u16 BE][code_scheme u8][code_param u32 BE][clen u32 BE]
//   [codes region: n entries][pool_scheme u8 = 0][dict d x 8B BE]
//   param = d.
EncodedCol EncDictXI64(const std::vector<int64_t>& vals, size_t budget,
                       int known_distinct) {
  if (vals.empty()) {
    return {};
  }
  if (known_distinct > 0 &&
      20 + (8 * static_cast<size_t>(known_distinct)) > budget) {
    return {};
  }
  // Thread-local scratch: this encoder re-runs for every row group.
  thread_local std::vector<int64_t> dict;
  dict.clear();
  FlatMapI64 code(vals.size());
  thread_local std::vector<int64_t> codes;
  codes.clear();
  codes.reserve(vals.size());
  for (int64_t v : vals) {
    if (const uint64_t* found = code.Find(v)) {
      codes.push_back(static_cast<int64_t>(*found));
    } else {
      if (dict.size() >= UINT16_MAX) {
        return {};
      }
      const uint64_t cv = dict.size();
      code.InsertNew(v, cv);
      dict.push_back(v);
      codes.push_back(static_cast<int64_t>(cv));
    }
  }
  // Exact floor: count + inner descriptor + 8 B/dictionary entry. The inner
  // budget tightens by that committed prefix (7 + 8d + ce is the whole
  // region; an over-budget candidate can never win the budgeted cascade,
  // and forced-scheme calls run with no budget).
  if (7 + (8 * dict.size()) > budget) {
    return {};
  }
  EncodedCol ce = EncodeInnerI64(codes, budget - 7 - (8 * dict.size()));
  if (ce.buf.empty() || ce.buf.size() > UINT32_MAX) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kDictX;
  e.param = static_cast<uint32_t>(dict.size());
  AppendBE16(e.buf, static_cast<uint16_t>(dict.size()));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(ce.scheme)));
  AppendBE32(e.buf, ce.param);
  AppendBE32(e.buf, static_cast<uint32_t>(ce.buf.size()));
  e.buf.insert(e.buf.end(), ce.buf.begin(), ce.buf.end());
  e.buf.push_back(0);  // pool_scheme: flat int64 dictionary
  for (int64_t v : dict) {
    AppendI64(e.buf, v);
  }
  return e;
}

// Varchar variant (kDictX): same cascaded code stream; the dictionary is
// either the flat [len u32][bytes] list (pool_scheme 0, same as kDict) or an
// FSST region over the distinct entries (pool_scheme 15) -- the paper's
// Dict+FSST cascade, compressing the string pool itself.
EncodedCol EncDictXStr(const std::vector<std::string_view>& sv) {
  if (sv.empty()) {
    return {};
  }
  std::vector<std::string_view> dict;
  std::unordered_map<std::string_view, uint16_t> code;
  std::vector<int64_t> codes;
  codes.reserve(sv.size());
  for (const auto& s : sv) {
    auto it = code.find(s);
    if (it == code.end()) {
      if (dict.size() >= UINT16_MAX) {
        return {};
      }
      it = code.emplace(s, static_cast<uint16_t>(dict.size())).first;
      dict.push_back(s);
    }
    codes.push_back(static_cast<int64_t>(it->second));
  }
  EncodedCol ce = EncodeInnerI64(codes);
  if (ce.buf.empty() || ce.buf.size() > UINT32_MAX) {
    return {};
  }
  // Pool choice: flat entry list vs FSST-compressed pool, whichever smaller.
  std::vector<char> flat_pool;
  flat_pool.push_back(0);  // pool_scheme = flat
  for (const auto& s : dict) {
    AppendBE32(flat_pool, static_cast<uint32_t>(s.size()));
    flat_pool.insert(flat_pool.end(), s.begin(), s.end());
  }
  EncodedCol fsst_pool = EncFsstStr(dict);
  const bool use_fsst =
      !fsst_pool.buf.empty() && fsst_pool.buf.size() + 1 < flat_pool.size();
  EncodedCol e;
  e.scheme = Scheme::kDictX;
  e.param = static_cast<uint32_t>(dict.size());
  AppendBE16(e.buf, static_cast<uint16_t>(dict.size()));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(ce.scheme)));
  AppendBE32(e.buf, ce.param);
  AppendBE32(e.buf, static_cast<uint32_t>(ce.buf.size()));
  e.buf.insert(e.buf.end(), ce.buf.begin(), ce.buf.end());
  if (use_fsst) {
    e.buf.push_back(static_cast<char>(static_cast<uint8_t>(Scheme::kFsst)));
    e.buf.insert(e.buf.end(), fsst_pool.buf.begin(), fsst_pool.buf.end());
  } else {
    e.buf.insert(e.buf.end(), flat_pool.begin(), flat_pool.end());
  }
  return e;
}

// TEMPLATE (scheme 64) encoder: infer a literal + fixed-width-digit-run
// template from row 0, verify every row against it (deviators become
// exception strings), and store each digit run as an ordinary int64 leaf
// region. See the kTemplate enum block for the wire and the gate rationale.
EncodedCol EncTemplateStr(const std::vector<std::string_view>& sv) {
  const auto n = static_cast<uint32_t>(sv.size());
  if (n < 8) {
    return {};
  }
  // Distinct-count fence: below 97 distinct the dictionary family stores
  // the same strings smaller AND already answers EQ from its pass table --
  // this scheme exists for the columns past the dictionary's reach.
  {
    std::vector<std::string_view> uniq(sv);
    std::sort(uniq.begin(), uniq.end());
    uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());
    if (uniq.size() < 97) {
      return {};
    }
  }
  // Template inference from row 0: maximal digit runs become segments; the
  // bytes between them are the literal skeleton, stored as (offset, length)
  // pairs INTO sv[0] so the skeleton never lives in two places.
  const std::string_view t0 = sv[0];
  std::array<uint32_t, kTemplateMaxSegments> widths{};
  std::array<size_t, kTemplateMaxSegments> dig_begin{};
  std::array<size_t, kTemplateMaxSegments> dig_end{};
  uint32_t nseg = 0;
  for (size_t i = 0; i < t0.size();) {
    if (t0[i] >= '0' && t0[i] <= '9') {
      if (nseg >= kTemplateMaxSegments) {
        return {};
      }
      dig_begin[nseg] = i;
      while (i < t0.size() && t0[i] >= '0' && t0[i] <= '9') {
        ++i;
      }
      dig_end[nseg] = i;
      if (i - dig_begin[nseg] > kTemplateMaxWidth) {
        return {};
      }
      widths[nseg] = static_cast<uint32_t>(dig_end[nseg] - dig_begin[nseg]);
      ++nseg;
    } else {
      ++i;
    }
  }
  if (nseg == 0) {
    return {};  // no digit run: kConst / kDict territory
  }
  std::array<size_t, kTemplateMaxSegments + 1> lit_begin{};
  std::array<size_t, kTemplateMaxSegments + 1> lit_len{};
  for (uint32_t s = 0; s <= nseg; ++s) {
    const size_t begin = s == 0 ? 0 : dig_end[s - 1];
    const size_t end = s == nseg ? t0.size() : dig_begin[s];
    lit_begin[s] = begin;
    lit_len[s] = end - begin;
    if (lit_len[s] > UINT16_MAX) {
      return {};  // literals are u16-prefixed
    }
  }
  // Classify every row against the template; collect digit values for the
  // conforming rows in row order. Digit runs must hold exactly `widths[s]`
  // ASCII digits and parse below 2^63 (width <= 18 guarantees it).
  uint32_t nexc = 0;
  std::array<std::vector<int64_t>, kTemplateMaxSegments> seg_vals;
  std::vector<uint8_t> bmp((static_cast<size_t>(n) + 7) / 8, 0);
  std::vector<std::string_view> exceptions;
  for (uint32_t row = 0; row < n; ++row) {
    const std::string_view s = sv[row];
    size_t pos = 0;
    std::array<int64_t, kTemplateMaxSegments> vals{};
    bool ok = true;
    for (uint32_t seg = 0; seg < nseg && ok; ++seg) {
      if (s.size() < pos + lit_len[seg] ||
          s.compare(pos, lit_len[seg],
                    t0.substr(lit_begin[seg], lit_len[seg])) != 0) {
        ok = false;
        break;
      }
      pos += lit_len[seg];
      if (s.size() < pos + widths[seg]) {
        ok = false;
        break;
      }
      uint64_t v = 0;
      for (uint32_t d = 0; d < widths[seg]; ++d) {
        const char c = s[pos + d];
        if (c < '0' || c > '9') {
          ok = false;
          break;
        }
        v = (v * 10) + static_cast<uint64_t>(c - '0');
      }
      if (!ok) {
        break;
      }
      pos += widths[seg];
      vals[seg] = static_cast<int64_t>(v);
    }
    if (ok && (s.size() != pos + lit_len[nseg] ||
               s.compare(pos, lit_len[nseg],
                         t0.substr(lit_begin[nseg], lit_len[nseg])) != 0)) {
      ok = false;
    }
    if (!ok) {
      ++nexc;
      exceptions.push_back(s);
      continue;
    }
    bmp[row >> 3] |= static_cast<uint8_t>(0x80U >> (row & 7));
    for (uint32_t seg = 0; seg < nseg; ++seg) {
      seg_vals[seg].push_back(vals[seg]);
    }
  }
  // The kArith majority rule: at least three quarters of the rows conform,
  // else the exception strings erode the win.
  if (nexc * 4 > n) {
    return {};
  }
  // Assemble: [nseg u8][nexc u16][widths][literals][bmp][segments][exceptions]
  EncodedCol e;
  e.scheme = Scheme::kTemplate;
  e.param = nexc;
  e.buf.push_back(static_cast<char>(nseg));
  AppendBE16(e.buf, static_cast<uint16_t>(nexc));
  for (uint32_t seg = 0; seg < nseg; ++seg) {
    e.buf.push_back(static_cast<char>(widths[seg]));
  }
  for (uint32_t k = 0; k <= nseg; ++k) {
    AppendBE16(e.buf, static_cast<uint16_t>(lit_len[k]));
    e.buf.insert(
        e.buf.end(), t0.begin() + static_cast<std::ptrdiff_t>(lit_begin[k]),
        t0.begin() + static_cast<std::ptrdiff_t>(lit_begin[k] + lit_len[k]));
  }
  e.buf.insert(e.buf.end(), reinterpret_cast<const char*>(bmp.data()),
               reinterpret_cast<const char*>(bmp.data() + bmp.size()));
  for (uint32_t seg = 0; seg < nseg; ++seg) {
    EncodedCol inner = EncodeInnerI64(seg_vals[seg]);
    if (inner.buf.empty() || inner.buf.size() > UINT32_MAX) {
      return {};
    }
    e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner.scheme)));
    AppendBE32(e.buf, inner.param);
    AppendBE32(e.buf, static_cast<uint32_t>(inner.buf.size()));
    e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  }
  for (const std::string_view x : exceptions) {
    AppendBE32(e.buf, static_cast<uint32_t>(x.size()));
    e.buf.insert(e.buf.end(), x.begin(), x.end());
  }
  return e;
}

// DSTREAM (scheme 65) encoder: mine a symbol table (every frequent single
// byte plus the FSST 2..8 substring pool), greedily longest-match every row
// into code bytes, and store them as length-delimited records behind a
// 16-row restart table -- FSST's mined dictionary with a leaner per-row
// index (u8 clen + codes instead of a 4B offset table) and an exact memcmp
// EQ fold. See the kDStream enum block for the wire and the gate rationale.
EncodedCol EncDStreamStr(const std::vector<std::string_view>& sv) {
  const auto n = static_cast<uint32_t>(sv.size());
  if (n < 8) {
    return {};
  }
  // Distinct-count fence: below 97 distinct the dictionary family stores
  // the same strings smaller AND already answers EQ from its pass table --
  // this scheme exists for the columns past the dictionary's reach.
  {
    std::vector<std::string_view> uniq(sv);
    std::sort(uniq.begin(), uniq.end());
    uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());
    if (uniq.size() < 97) {
      return {};
    }
  }
  // Byte frequencies first: singles are the escape killers (one covered
  // byte value saves 1B per occurrence), and the multi-byte pool is scored
  // against them. 64 single-byte slots (a text column's covered alphabet
  // rarely exceeds this) leave ~195 slots for the substring pool.
  std::array<uint64_t, 256> byte_freq{};
  for (const std::string_view s : sv) {
    for (const char ch : s) {
      ++byte_freq[static_cast<uint8_t>(ch)];
    }
  }
  struct Candidate {
    uint64_t key;
    uint64_t score;
    uint32_t len;
  };
  const auto cand_better = [](const Candidate& a, const Candidate& b) {
    if (a.score != b.score) {
      return a.score > b.score;
    }
    if (a.len != b.len) {
      return a.len < b.len;
    }
    return a.key > b.key;
  };
  std::vector<Candidate> symbols;
  symbols.reserve(kDStreamMaxSymbols);
  constexpr uint32_t kMaxSingles = 64;
  uint32_t nsingles = 0;
  {
    // Deterministic order: descending frequency, then ascending byte value.
    std::array<uint32_t, 256> order{};
    for (uint32_t b = 0; b < 256; ++b) {
      order[b] = b;
    }
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
      if (byte_freq[a] != byte_freq[b]) {
        return byte_freq[a] > byte_freq[b];
      }
      return a < b;
    });
    for (uint32_t b = 0; b < 256 && nsingles < kMaxSingles; ++b) {
      if (byte_freq[order[b]] == 0) {
        break;
      }
      symbols.push_back({order[b], byte_freq[order[b]], 1});
      ++nsingles;
    }
  }
  // Substring pool (2..8), FSST's mining: freq * (len - 1) benefit, one
  // open-addressed map per length, bounded min-heap for the remaining
  // symbol budget.
  std::array<FlatMapI64, kDStreamMaxSymbolLen - 1> freq_by_len{
      FlatMapI64(16), FlatMapI64(16), FlatMapI64(16), FlatMapI64(16),
      FlatMapI64(16), FlatMapI64(16), FlatMapI64(16)};
  for (const std::string_view s : sv) {
    const char* p = s.data();
    const size_t L = s.size();
    for (size_t pos = 0; pos < L; ++pos) {
      const size_t maxL = std::min<size_t>(kDStreamMaxSymbolLen, L - pos);
      uint64_t raw = 0;
      std::memcpy(&raw, p + pos, maxL);  // zero-padded beyond maxL
      for (size_t l = 2; l <= maxL; ++l) {
        const uint64_t key = l == 8 ? raw : (raw & ((1ULL << (8 * l)) - 1ULL));
        ++*freq_by_len[l - 2].UpsertSlot(key);
      }
    }
  }
  const uint32_t multi_budget = kDStreamMaxSymbols - nsingles;
  std::vector<Candidate> top;
  top.reserve(multi_budget);
  for (int l = 2; std::cmp_less_equal(l, kDStreamMaxSymbolLen); ++l) {
    freq_by_len[l - 2].ForEach([&](uint64_t key, uint64_t freq) {
      const Candidate cand{.key = key,
                           .score = freq * static_cast<uint64_t>(l - 1),
                           .len = static_cast<uint32_t>(l)};
      if (top.size() < multi_budget) {
        top.push_back(cand);
        std::push_heap(top.begin(), top.end(), cand_better);
        return;
      }
      if (cand_better(cand, top.front())) {
        std::pop_heap(top.begin(), top.end(), cand_better);
        top.back() = cand;
        std::push_heap(top.begin(), top.end(), cand_better);
      }
    });
  }
  symbols.insert(symbols.end(), top.begin(), top.end());
  // Code assignment in the deterministic selection order (singles first --
  // low codes, then multis by score). Greedy longest match needs a
  // per-length code lookup; the singles live in their own map slot.
  const auto nsym = static_cast<uint32_t>(symbols.size());
  std::array<FlatMapI64, kDStreamMaxSymbolLen> code_by_len{
      FlatMapI64(64),  FlatMapI64(254), FlatMapI64(254), FlatMapI64(254),
      FlatMapI64(254), FlatMapI64(254), FlatMapI64(254), FlatMapI64(254)};
  for (uint32_t c = 0; c < nsym; ++c) {
    code_by_len[symbols[c].len - 1].InsertNew(symbols[c].key,
                                              static_cast<uint64_t>(c) + 1);
  }
  // Encode each row; records over the u8 code-length cap fall to the
  // exception list (plain strings, row order, behind the kArith bitmap).
  std::vector<uint8_t> stream;
  stream.reserve(1024);
  std::vector<uint32_t> restarts;
  std::vector<uint8_t> bmp((static_cast<size_t>(n) + 7) / 8, 0);
  std::vector<std::string_view> exceptions;
  uint32_t nexc = 0;
  uint32_t nconf = 0;
  std::array<uint8_t, 256>
      buf{};  // per-row code scratch (the u8 clen caps records at 255)
  for (uint32_t i = 0; i < n; ++i) {
    const std::string_view s = sv[i];
    const char* p = s.data();
    const size_t L = s.size();
    size_t pos = 0;
    size_t clen = 0;
    bool over = false;
    while (pos < L) {
      int bestL = 0;
      const size_t maxL = std::min<size_t>(kDStreamMaxSymbolLen, L - pos);
      uint64_t raw = 0;
      std::memcpy(&raw, p + pos, maxL);
      for (int l = static_cast<int>(maxL); l >= 1; --l) {
        const uint64_t key = l == 8 ? raw : (raw & ((1ULL << (8 * l)) - 1ULL));
        if (const uint64_t* code = code_by_len[l - 1].Find(key)) {
          if (clen >= 255) {
            over = true;
            break;
          }
          buf[clen++] = static_cast<uint8_t>(*code);
          bestL = l;
          break;
        }
      }
      if (over) {
        break;
      }
      if (bestL == 0) {
        // Escape: two code bytes for one raw byte.
        if (clen + 2 > 255) {
          over = true;
          break;
        }
        buf[clen++] = 0;
        buf[clen++] = static_cast<uint8_t>(p[pos]);
        pos += 1;
      } else {
        pos += static_cast<size_t>(bestL);
      }
    }
    if (over) {
      ++nexc;
      exceptions.push_back(s);
      continue;
    }
    bmp[i >> 3] |= static_cast<uint8_t>(0x80U >> (i & 7));
    if (nconf % kDStreamRestart == 0) {
      restarts.push_back(static_cast<uint32_t>(stream.size()));
    }
    stream.push_back(static_cast<uint8_t>(clen));
    stream.insert(stream.end(), buf.begin(), buf.begin() + clen);
    ++nconf;
  }
  // The kArith majority rule: only rows past the code-length cap land in
  // the exception list, so a rejection here means the column is mostly
  // >255-code-length strings -- not this scheme's shape.
  if (nexc * 4 > n) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kDStream;
  e.param = nexc;
  e.buf.push_back(static_cast<char>(nsym));
  for (uint32_t c = 0; c < nsym; ++c) {
    e.buf.push_back(static_cast<char>(symbols[c].len));
    const char* sym = reinterpret_cast<const char*>(&symbols[c].key);
    e.buf.insert(e.buf.end(), sym, sym + symbols[c].len);
  }
  AppendBE16(e.buf, static_cast<uint16_t>(restarts.size()));
  AppendBE32(e.buf, static_cast<uint32_t>(stream.size()));
  for (const uint32_t off : restarts) {
    AppendBE32(e.buf, off);
  }
  e.buf.insert(e.buf.end(), reinterpret_cast<const char*>(stream.data()),
               reinterpret_cast<const char*>(stream.data() + stream.size()));
  e.buf.insert(e.buf.end(), reinterpret_cast<const char*>(bmp.data()),
               reinterpret_cast<const char*>(bmp.data() + bmp.size()));
  for (const std::string_view x : exceptions) {
    AppendBE32(e.buf, static_cast<uint32_t>(x.size()));
    e.buf.insert(e.buf.end(), x.begin(), x.end());
  }
  return e;
}

// Patched FOR (PFOR) with exceptions: base + narrow w-bit packed offsets,
// outliers stored separately. Layout:
//   [base 8B BE][w u8][nexc u16 BE][packed low-w offsets: n*w bits]
//   [exc_pos u16 BE x nexc][exc_val 8B BE x nexc].
// Packed part holds (v-base) & ((1<<w)-1); exception slots overwrite with
// the full value on decode. w is chosen to minimize total bytes.
EncodedCol EncPforI64(const std::vector<int64_t>& vals, size_t budget) {
  if (vals.empty()) {
    return {};
  }
  int64_t mn = vals.front();
  for (int64_t v : vals) {
    mn = std::min(mn, v);
  }
  const auto mn_u = static_cast<uint64_t>(mn);
  // Adaptive selection re-runs this encoder for every row group, so the
  // offset buffer lives in a thread-local scratch grown once.
  thread_local std::vector<uint64_t> offs;
  offs.resize(vals.size());
  {
    uint64_t* op = offs.data();
    for (int64_t v : vals) {
      *op++ = static_cast<uint64_t>(v) - mn_u;
    }
  }
  const size_t n = vals.size();
  // One pass buckets every offset by its bit width; the exception count for
  // width w (offsets with a bit set at or above w) is a suffix sum over the
  // buckets, replacing the O(n) scan the old loop ran per width.
  std::array<uint32_t, 65> hist{};
  for (uint64_t o : offs) {
    ++hist[BitsNeeded(o)];
  }
  std::array<size_t, 65> suf{};
  suf[64] = 0;
  for (int b = 64; b >= 1; --b) {
    suf[b - 1] = suf[b] + hist[b];
  }
  int best_w = 64;
  size_t best_bytes = SIZE_MAX;
  for (int w = 1; w <= 64; ++w) {
    const size_t packed = (n * static_cast<size_t>(w) + 7) / 8;
    // Packed part alone exceeds the budget => no width can win.
    if (11 + packed > budget) {
      break;
    }
    // The packed part is a hard lower bound on the region size (the
    // exception term is >= 0), so once it reaches the best size found so far
    // no wider width can strictly beat it.
    if (11 + packed >= best_bytes) {
      break;
    }
    const size_t nexc = suf[w];
    const size_t bytes = 11 + packed + (nexc * 10);
    if (bytes < best_bytes) {
      best_bytes = bytes;
      best_w = w;
    }
    if (nexc == 0) {
      break;  // wider w only grows the packed part
    }
  }
  if (best_bytes > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kPfor;
  e.param = static_cast<uint32_t>(best_w);
  AppendBE64(e.buf, mn_u);
  e.buf.push_back(static_cast<char>(best_w));
  std::vector<std::pair<uint16_t, int64_t>> exc;
  std::vector<uint64_t> low;
  low.reserve(n);
  uint64_t lomask = ~0ULL;
  if (best_w < 64) {
    lomask = (best_w == 0) ? 0ULL : ((1ULL << best_w) - 1ULL);
  }
  for (size_t i = 0; i < n; ++i) {
    low.push_back(offs[i] & lomask);
    if (best_w < 64 && (offs[i] >> best_w) != 0) {
      exc.emplace_back(static_cast<uint16_t>(i), vals[i]);
    }
  }
  AppendBE16(e.buf, static_cast<uint16_t>(exc.size()));
  AppendPackBits(e.buf, low, best_w);
  for (const auto& [pos, v] : exc) {
    AppendBE16(e.buf, pos);
    AppendI64(e.buf, v);
  }
  return e;
}

// kFastPfor (scheme 66) encoder: kBp128's per-block envelope plus kPfor's
// exception patching. Each 128-row block stores its min/max/sum brackets, the
// narrowest width w whose REGULAR offsets (v - block min) all fit, and the
// offsets that overflow 2^w as exceptions (position bitmap + full values).
// The width sweep is kPfor's histogram-suffix-sums trick applied per block;
// the packed lanes keep ALL rows' low bits (dense lanes are what lets the
// bulk decode reuse kBp128Block's AVX2 kernels).
EncodedCol EncFastPforI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n == 0) {
    return {};
  }
  const uint32_t nblk = (n + kFastPforBlockRows - 1) / kFastPforBlockRows;
  if (nblk > kFastPforMaxBlocks) {
    return {};
  }
  // Adaptive selection re-runs this encoder for every row group, so the
  // offset buffer lives in a thread-local scratch grown once (kPfor's trick).
  thread_local std::vector<uint64_t> offs;
  offs.resize(n);
  // The wire lays ALL block metadata first (fixed kFastPforMetaBytes stride,
  // the kBp128 envelope) and the payloads after; compute every block's
  // parameters before the first byte is written.
  struct FastPforPlan {
    int64_t mn;
    int64_t mx;
    uint64_t sum_hi;
    uint64_t sum_lo;
    uint32_t w;
    uint16_t nexc;
  };
  std::vector<FastPforPlan> plan(nblk);
  for (uint32_t b = 0; b < nblk; ++b) {
    const uint32_t start = b * kFastPforBlockRows;
    const uint32_t rows = std::min(kFastPforBlockRows, n - start);
    int64_t mn = INT64_MAX;
    int64_t mx = INT64_MIN;
    __int128 sum = 0;
    for (uint32_t t = 0; t < rows; ++t) {
      const int64_t v = vals[start + t];
      mn = std::min(v, mn);
      mx = std::max(v, mx);
      sum += v;
    }
    const auto mn_u = static_cast<uint64_t>(mn);
    std::array<uint32_t, 65> hist{};
    for (uint32_t t = 0; t < rows; ++t) {
      const uint64_t off = static_cast<uint64_t>(vals[start + t]) - mn_u;
      offs[start + t] = off;
      ++hist[BitsNeeded(off)];
    }
    std::array<size_t, 65> suf{};
    suf[64] = 0;
    for (int bw = 64; bw >= 1; --bw) {
      suf[bw - 1] = suf[bw] + hist[bw];
    }
    // suf[w] counts offsets >= 2^w: the exceptions width w cannot hold.
    int best_w = 64;
    size_t best_bytes = SIZE_MAX;
    for (int w = 0; w <= 64; ++w) {
      const size_t nexc = suf[w];
      const size_t bytes =
          kFastPforMetaBytes + ((rows + 7U) / 8U) +
          ((static_cast<size_t>(rows) * static_cast<size_t>(w) + 7) / 8) +
          (nexc * 8);
      if (bytes < best_bytes) {
        best_bytes = bytes;
        best_w = w;
      }
      if (nexc == 0) {
        break;  // wider w only grows the packed part
      }
    }
    FastPforPlan& bp = plan[b];
    bp.mn = mn;
    bp.mx = mx;
    const auto sum_u = static_cast<unsigned __int128>(sum);
    bp.sum_hi = static_cast<uint64_t>(sum_u >> 64);
    bp.sum_lo = static_cast<uint64_t>(sum_u);
    bp.w = static_cast<uint32_t>(best_w);
    bp.nexc = static_cast<uint16_t>(
        best_w >= 64 ? 0 : suf[best_w]);  // offsets >= 2^w
  }
  EncodedCol e;
  e.scheme = Scheme::kFastPfor;
  e.param = nblk;
  e.buf.reserve((static_cast<size_t>(nblk) * kFastPforMetaBytes) +
                static_cast<size_t>(n));
  for (const FastPforPlan& bp : plan) {
    AppendBE64(e.buf, static_cast<uint64_t>(bp.mn));
    AppendBE64(e.buf, static_cast<uint64_t>(bp.mx));
    AppendBE64(e.buf, bp.sum_hi);
    AppendBE64(e.buf, bp.sum_lo);
    e.buf.push_back(static_cast<char>(static_cast<uint8_t>(bp.w)));
    AppendBE16(e.buf, bp.nexc);
  }
  // Payloads, blocks in order: [bmp][packed lanes][exception values].
  std::vector<uint64_t> low;
  low.reserve(kFastPforBlockRows);
  std::vector<uint64_t> exc_vals;
  exc_vals.reserve(kFastPforBlockRows);
  std::vector<char> bmp;
  for (uint32_t b = 0; b < nblk; ++b) {
    const FastPforPlan& bp = plan[b];
    const uint32_t start = b * kFastPforBlockRows;
    const uint32_t rows = std::min(kFastPforBlockRows, n - start);
    const uint64_t lomask =
        bp.w >= 64 ? ~UINT64_C(0) : ((UINT64_C(1) << bp.w) - 1U);
    // Bitmap: MSB-first per byte, set = exception (the kArith/kBitmap
    // convention the kernels load with Rle8ReverseByte).
    bmp.assign((rows + 7U) / 8U, '\0');
    low.clear();
    exc_vals.clear();
    for (uint32_t t = 0; t < rows; ++t) {
      const uint64_t off = offs[start + t];
      low.push_back(off & lomask);
      if (bp.w < 64 && (off >> bp.w) != 0) {
        bmp[t >> 3] |=
            static_cast<char>(static_cast<uint8_t>(0x80U >> (t & 7U)));
        exc_vals.push_back(static_cast<uint64_t>(vals[start + t]));
      }
    }
    if (exc_vals.size() != bp.nexc) {
      return {};  // internal consistency
    }
    e.buf.insert(e.buf.end(), bmp.begin(), bmp.end());
    AppendPackBits(e.buf, low, static_cast<int>(bp.w));
    for (const uint64_t v : exc_vals) {
      AppendBE64(e.buf, v);
    }
  }
  if (e.buf.size() > budget) {
    return {};
  }
  return e;
}

// True when the column's first differences span no more than 15 (a single
// 4-bit delta code covers them, exceptions impossible) -- the
// sequential-replay delta family's home turf (see the consider() fence).
bool DeltaFastPforStationaryDeltas(const std::vector<int64_t>& vals) {
  if (vals.size() < 2) {
    return false;
  }
  uint64_t dmin = 0;
  uint64_t dmax = 0;
  bool first = true;
  for (size_t i = 1; i < vals.size(); ++i) {
    const uint64_t d =
        static_cast<uint64_t>(vals[i]) - static_cast<uint64_t>(vals[i - 1]);
    if (first) {
      dmin = dmax = d;
      first = false;
      continue;
    }
    if (static_cast<int64_t>(d) < static_cast<int64_t>(dmin)) {
      dmin = d;
    }
    if (static_cast<int64_t>(d) > static_cast<int64_t>(dmax)) {
      dmax = d;
    }
  }
  if (first) {
    return false;
  }
  // dmax >= dmin in SIGNED order, so the wrapping subtract is the true span.
  return dmax - dmin < 16;
}

// kDeltaFastPfor (scheme 67) encoder: kS4's first-difference transform plus
// kPfor's exception patching plus kBp128/FastPFOR's per-block bracket
// envelope. Each 128-row block stores its base (the first value), the TRUE
// value min/max/sum brackets, its minimum delta dmin (SIGNED order, stored
// as the uint64 bit pattern), the narrowest width w whose REGULAR
// (d - dmin) all fit, and the deltas that overflow 2^w as exceptions
// (position bitmap + full delta values). The width sweep is kPfor's
// histogram-suffix-sums trick applied per block over the block's deltas; the
// packed lanes keep ALL deltas' low bits (dense lanes are what lets the bulk
// decode reuse kBp128Block's AVX2 kernels). The replay arithmetic is
// unsigned-wrapping, so a column visiting both int64 extremes round-trips
// bit for bit (the kS4/D2PFOR convention).
EncodedCol EncDeltaFastPforI64(const std::vector<int64_t>& vals,
                               size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n == 0) {
    return {};
  }
  const uint32_t nblk =
      (n + kDeltaFastPforBlockRows - 1) / kDeltaFastPforBlockRows;
  if (nblk > kDeltaFastPforMaxBlocks) {
    return {};
  }
  // Adaptive selection re-runs this encoder for every row group, so the
  // delta buffer lives in a thread-local scratch grown once (kPfor's trick).
  thread_local std::vector<uint64_t> deltas;
  deltas.resize(n);
  // The wire lays ALL block metadata first (fixed kDeltaFastPforMetaBytes
  // stride, the kBp128 envelope) and the payloads after; compute every
  // block's parameters before the first byte is written.
  struct DeltaFastPforPlan {
    int64_t base;
    int64_t mn;
    int64_t mx;
    uint64_t sum_hi;
    uint64_t sum_lo;
    uint64_t dmin;
    uint32_t w;
    uint16_t nexc;
  };
  std::vector<DeltaFastPforPlan> plan(nblk);
  for (uint32_t b = 0; b < nblk; ++b) {
    const uint32_t start = b * kDeltaFastPforBlockRows;
    const uint32_t rows = std::min(kDeltaFastPforBlockRows, n - start);
    const uint32_t deltas_n = rows - 1;
    int64_t mn = INT64_MAX;
    int64_t mx = INT64_MIN;
    __int128 sum = 0;
    for (uint32_t t = 0; t < rows; ++t) {
      const int64_t v = vals[start + t];
      mn = std::min(v, mn);
      mx = std::max(v, mx);
      sum += v;
      if (t != 0) {
        deltas[start + t] = static_cast<uint64_t>(vals[start + t]) -
                            static_cast<uint64_t>(vals[start + t - 1]);
      }
    }
    // dmin: the block's minimum delta in SIGNED order (the unsigned-wrapping
    // subtract d - dmin then lands every regular delta in [0, max-min]).
    // rows == 1 keeps dmin == 0: there are no deltas, the field is unused.
    uint64_t dmin = 0;
    for (uint32_t t = 1; t < rows; ++t) {
      if (t == 1 || static_cast<int64_t>(deltas[start + t]) <
                        static_cast<int64_t>(dmin)) {
        dmin = deltas[start + t];
      }
    }
    std::array<uint32_t, 65> hist{};
    for (uint32_t t = 1; t < rows; ++t) {
      ++hist[BitsNeeded(deltas[start + t] - dmin)];
    }
    std::array<size_t, 65> suf{};
    suf[64] = 0;
    for (int bw = 64; bw >= 1; --bw) {
      suf[bw - 1] = suf[bw] + hist[bw];
    }
    // suf[w] counts (d - dmin) >= 2^w: the exceptions width w cannot hold.
    int best_w = 64;
    size_t best_bytes = SIZE_MAX;
    for (int w = 0; w <= 64; ++w) {
      const size_t nexc = suf[w];
      const size_t bytes =
          kDeltaFastPforMetaBytes + ((deltas_n + 7U) / 8U) +
          ((static_cast<size_t>(deltas_n) * static_cast<size_t>(w) + 7) / 8) +
          (nexc * 8);
      if (bytes < best_bytes) {
        best_bytes = bytes;
        best_w = w;
      }
      if (nexc == 0) {
        break;  // wider w only grows the packed part
      }
    }
    DeltaFastPforPlan& bp = plan[b];
    bp.base = vals[start];
    bp.mn = mn;
    bp.mx = mx;
    const auto sum_u = static_cast<unsigned __int128>(sum);
    bp.sum_hi = static_cast<uint64_t>(sum_u >> 64);
    bp.sum_lo = static_cast<uint64_t>(sum_u);
    bp.dmin = dmin;
    bp.w = static_cast<uint32_t>(best_w);
    bp.nexc = static_cast<uint16_t>(
        best_w >= 64 ? 0 : suf[best_w]);  // (d - dmin) >= 2^w
  }
  EncodedCol e;
  e.scheme = Scheme::kDeltaFastPfor;
  e.param = nblk;
  e.buf.reserve((static_cast<size_t>(nblk) * kDeltaFastPforMetaBytes) +
                static_cast<size_t>(n));
  for (const DeltaFastPforPlan& bp : plan) {
    AppendBE64(e.buf, static_cast<uint64_t>(bp.base));
    AppendBE64(e.buf, static_cast<uint64_t>(bp.mn));
    AppendBE64(e.buf, static_cast<uint64_t>(bp.mx));
    AppendBE64(e.buf, bp.sum_hi);
    AppendBE64(e.buf, bp.sum_lo);
    e.buf.push_back(static_cast<char>(static_cast<uint8_t>(bp.w)));
    AppendBE16(e.buf, bp.nexc);
    AppendBE64(e.buf, bp.dmin);
  }
  // Payloads, blocks in order: [bmp][packed lanes][exception deltas].
  std::vector<uint64_t> low;
  low.reserve(kDeltaFastPforBlockRows);
  std::vector<uint64_t> exc_vals;
  exc_vals.reserve(kDeltaFastPforBlockRows);
  std::vector<char> bmp;
  for (uint32_t b = 0; b < nblk; ++b) {
    const DeltaFastPforPlan& bp = plan[b];
    const uint32_t start = b * kDeltaFastPforBlockRows;
    const uint32_t rows = std::min(kDeltaFastPforBlockRows, n - start);
    const uint64_t lomask =
        bp.w >= 64 ? ~UINT64_C(0) : ((UINT64_C(1) << bp.w) - 1U);
    // Bitmap over deltas: MSB-first per byte, bit t-1 = delta t is an
    // exception (the kArith/kBitmap convention the kernels load with
    // Rle8ReverseByte).
    bmp.assign(((rows - 1U) + 7U) / 8U, '\0');
    low.clear();
    exc_vals.clear();
    for (uint32_t t = 1; t < rows; ++t) {
      const uint64_t d = deltas[start + t];
      const uint64_t rel = d - bp.dmin;
      low.push_back(rel & lomask);
      if (bp.w < 64 && (rel >> bp.w) != 0) {
        bmp[(t - 1) >> 3] |=
            static_cast<char>(static_cast<uint8_t>(0x80U >> ((t - 1) & 7U)));
        exc_vals.push_back(d);
      }
    }
    if (exc_vals.size() != bp.nexc) {
      return {};  // internal consistency
    }
    e.buf.insert(e.buf.end(), bmp.begin(), bmp.end());
    AppendPackBits(e.buf, low, static_cast<int>(bp.w));
    for (const uint64_t v : exc_vals) {
      AppendBE64(e.buf, v);
    }
  }
  if (e.buf.size() > budget) {
    return {};
  }
  return e;
}

EncodedCol EncDeltaPForI64(const std::vector<int64_t>& values, size_t budget) {
  const size_t n = values.size();
  if (n == 0 || n > static_cast<size_t>(UINT16_MAX) + 1) {
    return {};
  }
  for (size_t i = 1; i < n; ++i) {
    if (values[i] < values[i - 1]) {
      return {};
    }
  }

  // Thread-local scratch: adaptive selection re-runs this encoder for every
  // row group. resize() only zero-fills grown slots, so delta 0 is written
  // explicitly (the fill loop below starts at i == 1).
  thread_local std::vector<uint64_t> deltas;
  deltas.resize(n);
  deltas[0] = 0;
  uint64_t delta_min = 0;
  for (size_t i = 1; i < n; ++i) {
    deltas[i] =
        static_cast<uint64_t>(values[i]) - static_cast<uint64_t>(values[i - 1]);
    if (deltas[i] > static_cast<uint64_t>(INT64_MAX)) {
      return {};
    }
    if (i == 1 || deltas[i] < delta_min) {
      delta_min = deltas[i];
    }
  }

  const size_t blocks = (n + kDeltaPForBlockRows - 1) / kDeltaPForBlockRows;
  // One pass buckets every shifted delta by bit width; the exception count
  // for a width is the suffix sum above that width's bucket.
  std::array<uint32_t, 65> dhist{};
  for (size_t i = 1; i < n; ++i) {
    ++dhist[BitsNeeded(deltas[i] - delta_min)];
  }
  std::array<size_t, 65> dsuf{};
  dsuf[64] = 0;
  for (int b = 64; b >= 1; --b) {
    dsuf[b - 1] = dsuf[b] + dhist[b];
  }
  int best_width = 0;
  size_t best_bytes = SIZE_MAX;
  for (int width = 0; width <= 63; ++width) {
    const size_t packed = ((n - 1) * static_cast<size_t>(width) + 7) / 8;
    // Bracket table + packed deltas already exceed the budget: no wider
    // width can win, so stop the O(64*n) exception count.
    if (11 + (blocks * kDeltaPForBracketBytes) + packed > budget) {
      break;
    }
    // The packed part is a hard lower bound on the region size, so once it
    // reaches the best size found so far no wider width can strictly win.
    if (11 + (blocks * kDeltaPForBracketBytes) + packed >= best_bytes) {
      break;
    }
    const size_t exception_count = dsuf[width];
    const size_t bytes = 11 + (blocks * kDeltaPForBracketBytes) + packed +
                         (exception_count * 10);
    if (bytes < best_bytes) {
      best_bytes = bytes;
      best_width = width;
    }
    if (exception_count == 0) {
      break;
    }
  }

  const uint64_t low_mask =
      best_width == 0 ? 0 : (uint64_t{1} << best_width) - 1;
  std::vector<uint64_t> low_offsets;
  std::vector<std::pair<uint16_t, int64_t>> exceptions;
  low_offsets.reserve(n - 1);
  for (size_t i = 1; i < n; ++i) {
    const uint64_t offset = deltas[i] - delta_min;
    if ((offset >> best_width) != 0) {
      low_offsets.push_back(0);
      exceptions.emplace_back(static_cast<uint16_t>(i),
                              static_cast<int64_t>(deltas[i]));
    } else {
      low_offsets.push_back(offset & low_mask);
    }
  }

  EncodedCol encoded;
  encoded.scheme = Scheme::kDeltaPfor;
  encoded.param = static_cast<uint32_t>(best_width);
  AppendBE64(encoded.buf, delta_min);
  encoded.buf.push_back(static_cast<char>(best_width));
  AppendBE16(encoded.buf, static_cast<uint16_t>(exceptions.size()));
  for (size_t block = 0; block < blocks; ++block) {
    const size_t start = block * kDeltaPForBlockRows;
    const size_t end = std::min(n, start + kDeltaPForBlockRows);
    __int128 sum = 0;
    for (size_t i = start; i < end; ++i) {
      sum += values[i];
    }
    const auto bits = static_cast<unsigned __int128>(sum);
    AppendBE64(encoded.buf, static_cast<uint64_t>(values[start]));
    AppendBE64(encoded.buf, static_cast<uint64_t>(values[end - 1]));
    AppendBE64(encoded.buf, static_cast<uint64_t>(bits >> 64));
    AppendBE64(encoded.buf, static_cast<uint64_t>(bits));
  }
  AppendPackBits(encoded.buf, low_offsets, best_width);
  for (const auto& [position, delta] : exceptions) {
    AppendBE16(encoded.buf, position);
    AppendBE64(encoded.buf, static_cast<uint64_t>(delta));
  }
  return encoded;
}

// Elias-Fano for monotonic non-decreasing int64 columns. Layout:
//   [base i64 BE][maxd i64 BE][l u8]
//   [high bitmap: (n + (maxd>>l)) bits][low bits: n*l bits, MSB-first]
//   param = l. decode via select() on the high bitmap.
EncodedCol EncEliasFanoI64(const std::vector<int64_t>& vals, size_t budget) {
  static_cast<void>(budget);
  const size_t n = vals.size();
  if (n < 2) {
    return {};
  }
  for (size_t i = 1; i < n; ++i) {
    if (vals[i] < vals[i - 1]) {
      return {};  // not monotonic: infeasible
    }
  }
  const int64_t base = vals.front();
  uint64_t maxd =
      static_cast<uint64_t>(vals.back()) - static_cast<uint64_t>(base);
  const int hi_bits = BitsNeeded(maxd);
  const int l =
      hi_bits == 0
          ? 0
          : std::max(0, hi_bits - BitsNeeded(std::max<uint64_t>(n, 1) - 1));
  const uint64_t high_max = (l >= 64) ? 0 : (maxd >> l);
  const size_t hlen = n + static_cast<size_t>(high_max);
  std::vector<char> hbmp((hlen + 7) / 8, '\0');
  std::vector<uint64_t> lows;
  lows.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    // Unsigned domain: `vals[i] - base` as SIGNED int64 overflows UB when
    // the column straddles the extremes ([INT64_MIN, 0] hit this under
    // UBSan via the schema fuzzer). Two's-complement wrap is exactly the
    // delta EF expects.
    const uint64_t d =
        static_cast<uint64_t>(vals[i]) - static_cast<uint64_t>(base);
    const uint64_t h = (l >= 64) ? 0 : (d >> l);
    const size_t pos = i + static_cast<size_t>(h);
    hbmp[pos >> 3] = static_cast<char>(hbmp[pos >> 3] | (0x80 >> (pos & 7)));
    lows.push_back(d & ((l == 0) ? 0 : ((1ULL << l) - 1)));
  }
  EncodedCol e;
  e.scheme = Scheme::kEliasFano;
  e.param = static_cast<uint32_t>(l);
  AppendBE64(e.buf, static_cast<uint64_t>(base));
  AppendBE64(e.buf, maxd);
  e.buf.push_back(static_cast<char>(l));
  e.buf.insert(e.buf.end(), hbmp.begin(), hbmp.end());
  AppendPackBits(e.buf, lows, l);
  return e;
}

// Select the position of the (idx+1)-th set bit in a bitmap of `len` bits.
uint64_t SelectBit(const char* hbmp, uint64_t len, uint64_t idx) {
  // Word-wise skip: the bitmap is MSB-first, so a BE-loaded 64-bit word
  // covers bits [64w, 64w+64) with bit q at (63 - q). Popcount skips whole
  // words instead of testing bit-by-bit -- this sits on the per-row
  // Elias-Fano decode path (ReadColumnValue/ReadRow), where the old
  // one-bit-per-iteration scan dominated full scans.
  uint64_t seen = 0;
  uint64_t w = 0;
  for (; (w + 1) * 64 <= len; ++w) {
    const uint64_t word = ReadBE64(hbmp + (w * 8));
    if (word == 0) {
      continue;
    }
    const auto cnt = static_cast<uint64_t>(__builtin_popcountll(word));
    if (seen + cnt > idx) {
      // The (idx+1)-th set bit overall is the (idx-seen)-th set bit of this
      // word, MSB-first.
      return (w * 64) + SelectFromWord(word, idx - seen);
    }
    seen += cnt;
  }
  // Bit-exact tail for the trailing partial word.
  for (uint64_t p = w * 64; p < len; ++p) {
    const uint64_t bit =
        (static_cast<uint8_t>(hbmp[p >> 3]) >> (7 - (p & 7))) & 1;
    if (bit != 0U) {
      if (seen == idx) {
        return p;
      }
      ++seen;
    }
  }
  return len;  // should not happen for well-formed EF
}

// Select the (idx+1)-th set bit starting from the every-8th-set-bit sample
// directory built by PaxChunk::BuildEfSelectDirs: dir[idx>>3] is the position
// of the (idx & ~7)-th set bit, so at most 7 more set bits need to be walked
// (one or two words) instead of re-scanning from word 0. Bit-exact equal to
// SelectBit for every directory the builder emits.
uint64_t SelectBitDir(const char* hbmp, uint64_t len, uint64_t idx,
                      const uint32_t* dir) {
  const uint64_t p0 = dir[idx >> 3];
  if (p0 >= len) {
    return len;
  }
  const uint64_t base = idx & ~uint64_t{7};
  if (idx == base) {
    return p0;
  }
  uint64_t seen = base + 1;
  const uint64_t w0 = p0 >> 6;
  if ((w0 + 1) * 64 <= len) {
    const auto off = static_cast<unsigned>(p0 & 63);
    uint64_t x = ReadBE64(hbmp + (w0 * 8)) &
                 ((static_cast<uint64_t>(1) << (63 - off)) - 1);
    while (x != 0) {
      const int hi = 63 - __builtin_clzll(x);
      if (seen == idx) {
        return (w0 * 64) + static_cast<uint64_t>(63 - hi);
      }
      ++seen;
      x &= (static_cast<uint64_t>(1) << hi) - 1;
    }
    uint64_t w = w0 + 1;
    for (; (w + 1) * 64 <= len; ++w) {
      const uint64_t word = ReadBE64(hbmp + (w * 8));
      if (word == 0) {
        continue;
      }
      const auto cnt = static_cast<uint64_t>(__builtin_popcountll(word));
      if (seen + cnt > idx) {
        return (w * 64) + SelectFromWord(word, idx - seen);
      }
      seen += cnt;
    }
    for (uint64_t p = w * 64; p < len; ++p) {
      const uint64_t bit =
          (static_cast<uint8_t>(hbmp[p >> 3]) >> (7 - (p & 7))) & 1;
      if (bit != 0U) {
        if (seen == idx) {
          return p;
        }
        ++seen;
      }
    }
    return len;
  }
  for (uint64_t p = p0 + 1; p < len; ++p) {
    const uint64_t bit =
        (static_cast<uint8_t>(hbmp[p >> 3]) >> (7 - (p & 7))) & 1;
    if (bit != 0U) {
      if (seen == idx) {
        return p;
      }
      ++seen;
    }
  }
  return len;
}

// Wavelet Tree over (val - min), MSB-first bit decomposition, permuted layout
// (bitmaps rebuilt in the recursively-partitioned order). Rank is computed at
// decode time by popcounting the bitmap (no materialized prefix array).
//   [min i64 BE][W u8][bitmap B_0 (n bits, MSB-first)] ... [bitmap B_{W-1}]
//   param = n (also passed to Int64RegionAt).
EncodedCol EncWaveletI64(const std::vector<int64_t>& vals, size_t budget) {
  const size_t n = vals.size();
  if (n == 0) {
    return {};
  }
  int64_t mn = vals.front(), mx = vals.front();
  for (int64_t v : vals) {
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
  uint64_t range = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
  int W = BitsNeeded(range);
  if (W == 0) {
    W = 1;
  }
  const size_t bmp_bytes = (n + 7) / 8;
  // Exact region size: base + level count + W full-column bitmaps. The
  // per-level pass below is O(W*n) with a partition per level, so retiring an
  // over-budget column here removes the single most expensive candidate on
  // wide-range data.
  if (9 + (static_cast<size_t>(W) * bmp_bytes) > budget) {
    return {};
  }
  std::vector<uint64_t> norm;
  norm.reserve(n);
  const auto mn_u = static_cast<uint64_t>(mn);
  for (int64_t v : vals) {
    norm.push_back(static_cast<uint64_t>(v) - mn_u);
  }

  EncodedCol e;
  e.scheme = Scheme::kWavelet;
  e.param = static_cast<uint32_t>(n);
  AppendBE64(e.buf, static_cast<uint64_t>(mn));
  e.buf.push_back(static_cast<char>(W));

  std::vector<size_t> order(n);
  for (size_t i = 0; i < n; ++i) {
    order[i] = i;
  }
  // Level scratch reused across the W levels: the previous code allocated a
  // fresh bitmap, bit vector and next-order vector per level (up to 3*W heap
  // allocations per cell) and grew the order vector by push_back each time.
  std::vector<uint8_t> bits(n);
  std::vector<size_t> next_order(n);
  e.buf.resize(9 + (static_cast<size_t>(W) * bmp_bytes), '\0');
  char* level_out = e.buf.data() + 9;
  for (int k = 0; k < W; ++k) {
    const int shift = W - 1 - k;
    char* bmp = level_out + (static_cast<size_t>(k) * bmp_bytes);
    std::memset(bmp, 0, bmp_bytes);
    for (size_t j = 0; j < n; ++j) {
      const uint64_t bit = (norm[order[j]] >> shift) & 1;
      bits[j] = static_cast<uint8_t>(bit);
      if (bit != 0U) {
        bmp[j >> 3] = static_cast<char>(bmp[j >> 3] | (0x80 >> (j & 7)));
      }
    }
    // Stable partition: zeros first, then ones, in one pass each.
    size_t zeros = 0;
    for (size_t j = 0; j < n; ++j) {
      if (bits[j] == 0) {
        next_order[zeros++] = order[j];
      }
    }
    for (size_t j = 0, t = zeros; j < n; ++j) {
      if (bits[j] == 1) {
        next_order[t++] = order[j];
      }
    }
    order.swap(next_order);
  }
  return e;
}

EncodedCol EncWaveletRI64(const std::vector<int64_t>& vals) {
  EncodedCol e = EncWaveletI64(vals);
  if (e.buf.empty()) {
    return {};
  }
  const uint32_t n = e.param;
  const uint32_t width = static_cast<uint8_t>(e.buf[8]);
  const size_t stride = (static_cast<size_t>(n) + 7) / 8;
  const uint32_t rank_count = WaveletRRankCount(n);
  e.scheme = Scheme::kWaveletR;
  e.buf.reserve(e.buf.size() + (static_cast<size_t>(width) * rank_count * 2));
  for (uint32_t level = 0; level < width; ++level) {
    const char* bitmap =
        e.buf.data() + 9 + (static_cast<size_t>(level) * stride);
    uint32_t rank = 0;
    uint32_t previous = 0;
    AppendBE16(e.buf, 0);
    for (uint32_t boundary = kWaveletRRankEvery; boundary <= n;
         boundary += kWaveletRRankEvery) {
      rank += PopcountBits(bitmap + (previous / 8), boundary - previous);
      AppendBE16(e.buf, static_cast<uint16_t>(rank));
      previous = boundary;
    }
    if (previous < n) {
      rank += PopcountBits(bitmap + (previous / 8), n - previous);
      AppendBE16(e.buf, static_cast<uint16_t>(rank));
    }
  }
  return e;
}

// Composite integer encodings: delta-transform the column, then compress the
// delta vector with a random-accessible inner scheme (FOR-BA or BitWeave).
// Random access is preserved by storing cumulative checkpoints every
// kDeltaCheckpointEvery values, so value[i] = checkpoints[i/C] +
// sum(deltas[i/C*C+1 .. i]) with only O(C) inner decodes. The delta vector is
// length n with deltas[0]=0.
// Out-param form of MakeDeltas: lets the encoder trial loop reuse one
// scratch across row groups (resize only zero-fills on growth, so the
// steady state pays no per-trial 8 KB memset + alloc). Values match the
// historical by-value form exactly.
void MakeDeltasInto(const std::vector<int64_t>& vals,
                    std::vector<int64_t>* deltas) {
  const size_t n = vals.size();
  deltas->resize(n);
  if (n == 0) {
    return;
  }
  (*deltas)[0] = 0;
  for (size_t i = 1; i < n; ++i) {
    (*deltas)[i] = static_cast<int64_t>(static_cast<uint64_t>(vals[i]) -
                                        static_cast<uint64_t>(vals[i - 1]));
  }
}

// `deltas` must equal MakeDeltasInto(vals); `inner_enc` must be the encoding of
// `deltas` produced by the inner scheme. `outer` names the scheme this wrapper
// publishes as -- it is passed rather than derived, because the mapping from
// inner to outer stopped being a function of two cases when kDeltaSimd joined
// the family (delta + kSimdFor), and a silent mis-derivation here would write
// a region labelled as one scheme and decoded as another.
EncodedCol EncDeltaCompositeI64(const std::vector<int64_t>& vals,
                                const std::vector<int64_t>& deltas,
                                Scheme outer, const EncodedCol& inner_enc,
                                size_t budget) {
  const size_t n = deltas.size();
  if (n == 0 || inner_enc.buf.empty()) {
    return {};
  }
  // Exact region size: 2-byte checkpoint count + 8 B per checkpoint + the
  // inner region verbatim.
  const auto ncp_total = static_cast<uint32_t>(
      ((n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) + 1);
  if (2 + (8 * static_cast<size_t>(ncp_total)) + inner_enc.buf.size() >
      budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = outer;
  e.param = inner_enc.param;  // bpv (FOR-BA), w (BitWeave), w (kSimdFor)
  const auto ncp = static_cast<uint32_t>(
      ((n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery) + 1);
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t idx =
        std::min<size_t>(static_cast<size_t>(b) * kDeltaCheckpointEvery, n - 1);
    AppendBE64(e.buf, static_cast<uint64_t>(vals[idx]));
  }
  e.buf.insert(e.buf.end(), inner_enc.buf.begin(), inner_enc.buf.end());
  return e;
}

EncodedCol EncDeltaForI64(const std::vector<int64_t>& vals, size_t budget) {
  // One scratch per call site: the delta composite's inner encoder never
  // nests another MakeDeltas call, so the thread_local is never aliased.
  static thread_local std::vector<int64_t> d;
  MakeDeltasInto(vals, &d);
  return EncDeltaCompositeI64(vals, d, Scheme::kDeltaFor, EncForBA(d, budget),
                              budget);
}
EncodedCol EncDeltaBitweaveI64(const std::vector<int64_t>& vals,
                               size_t budget) {
  static thread_local std::vector<int64_t> d;
  MakeDeltasInto(vals, &d);
  return EncDeltaCompositeI64(vals, d, Scheme::kDeltaBitweave,
                              EncBitWeaveI64(d, budget), budget);
}

// kSimdFor (scheme 53): frame-of-reference + four-lane interleaved bit-packing.
// See the enum comment for the format. The encoder's only real decision is the
// width: the smallest ADMITTED width (a divisor of 64) that covers the spread.
// It does not fall back to the next power of two for a width the layout cannot
// express -- that would silently inflate the output -- it declines, and the
// selector picks something else.
EncodedCol EncSimdForI64(const std::vector<int64_t>& vals, size_t budget) {
  const auto n = static_cast<uint32_t>(vals.size());
  if (n == 0) {
    return {};
  }
  int64_t base = vals[0];
  for (const int64_t v : vals) {
    base = std::min(v, base);
  }
  uint64_t maxd = 0;
  for (const int64_t v : vals) {
    const uint64_t d = static_cast<uint64_t>(v) - static_cast<uint64_t>(base);
    maxd = std::max(d, maxd);
  }
  // The smallest width that covers the spread, stepping ONE bit at a time.
  // Rounding to a power of two would inflate the output by up to 2x on a
  // spread that needs, say, 6 or 10 bits, and size is the reason this scheme
  // exists.
  uint32_t w = 1;
  while (w < 64 && SimdForMask(w) < maxd) {
    ++w;
  }
  if (!SimdForWidthOk(w)) {
    return {};
  }
  const uint64_t total = SimdForExtent(n, w);
  if (total > budget) {
    return {};
  }
  const uint32_t gv = SimdForGroupValues(w);
  const uint64_t ngroups = SimdForGroups(n, w);
  const auto base_u = static_cast<uint64_t>(base);
  EncodedCol e;
  e.scheme = Scheme::kSimdFor;
  e.param = w;
  e.buf.reserve(static_cast<size_t>(total));
  AppendBE64(e.buf, base_u);
  for (uint64_t g = 0; g < ngroups; ++g) {
    // Four lane words, then four 8-byte BE appends: the same bytes the
    // decoder's four aligned loads read back.
    std::array<uint64_t, kSimdForLanes> lane = {0, 0, 0, 0};
    for (uint32_t t = 0; t < gv; ++t) {
      const uint64_t i = (g * gv) + t;
      if (i >= n) {
        break;
      }
      const uint64_t d = static_cast<uint64_t>(vals[i]) - base_u;
      lane[t & 3U] |= d << ((t >> 2) * w);
    }
    for (unsigned long j : lane) {
      AppendBE64(e.buf, j);
    }
  }
  if (e.buf.size() != total) {
    return {};
  }
  return e;
}

// kDeltaSimd (scheme 54): first differences packed by kSimdFor's four-lane
// interleaved frame-of-reference. Identical seek-table shell to
// EncDeltaForI64 -- the delta vector is what kSimdFor compresses here, so the
// 4-lane layout is applied to narrow deltas instead of raw values.
EncodedCol EncDeltaSimdI64(const std::vector<int64_t>& vals, size_t budget) {
  static thread_local std::vector<int64_t> d;
  MakeDeltasInto(vals, &d);
  return EncDeltaCompositeI64(vals, d, Scheme::kDeltaSimd,
                              EncSimdForI64(d, budget), budget);
}

// Encoder for SFOR (scheme 57): try every split 1..63 and keep the smallest
// encoding; the msb table is strictly ascending by construction (a new group
// starts whenever the prefix changes), and a group's delta width is the width
// of its LAST delta (the lsbits are ascending inside a group, so the last row
// carries the widest). Applicability gate (caller-enforced, re-checked here):
// non-negative monotone cells only -- with a negative (sign bit set) anywhere,
// the bitcast order is not value order and the group brackets stop pruning.
EncodedCol EncSforF64(const std::vector<uint64_t>& bits, size_t budget) {
  const size_t n = bits.size();
  if (n < 2 || n > PaxChunk::kRowGroupSize) {
    return {};
  }
  for (size_t i = 0; i < n; ++i) {
    if ((bits[i] >> 63) != 0) {
      return {};  // negative (or -0.0): sign bit set
    }
    if (i > 0 && bits[i] < bits[i - 1]) {
      return {};  // not non-decreasing
    }
  }
  size_t best_bytes = SIZE_MAX;
  uint32_t best_s = 1;
  std::vector<uint64_t> best_start, best_msb, best_base, best_width;
  std::vector<uint64_t> gstart, gmsb, gbase, gwidth;
  for (uint32_t s = 1; s <= 63; ++s) {
    gstart.clear();
    gmsb.clear();
    gbase.clear();
    gwidth.clear();
    size_t total = 0;
    for (size_t i = 0; i < n;) {
      // Full-width msb: truncating to 32 bits both corrupted the stored
      // directory (a value with bits >> s >= 2^32 decoded with its high
      // bits dropped -- silent data loss on mixed-magnitude monotone
      // columns) and, via the == comparison against the truncated word,
      // split every such row into a bogus singleton group.
      const uint64_t msb = bits[i] >> s;
      const size_t start = i;
      const uint64_t base = bits[i] & ((UINT64_C(1) << s) - 1);
      uint64_t last = base;
      for (++i; i < n && (bits[i] >> s) == msb; ++i) {
        last = bits[i] & ((UINT64_C(1) << s) - 1);
      }
      const auto width =
          static_cast<uint64_t>(BitsNeeded(last - base));  // 0 == constant
      gstart.push_back(start);
      gmsb.push_back(msb);
      gbase.push_back(base);
      gwidth.push_back(width);
      total += 3 + kSforDirEntry + (((i - start) * width + 7) / 8);
    }
    if (total < best_bytes) {
      best_bytes = total;
      best_s = s;
      best_start = gstart;
      best_msb = gmsb;
      best_base = gbase;
      best_width = gwidth;
    }
  }
  const auto ng = static_cast<uint32_t>(best_start.size());
  const size_t payload =
      best_bytes - (static_cast<size_t>(ng) * (3 + kSforDirEntry));
  if (3 + (static_cast<size_t>(ng) * kSforDirEntry) + payload > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kSfor;
  e.param = best_s;  // the split, echoed for ValidateWalk's pin
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(best_s)));
  AppendBE16(e.buf, static_cast<uint16_t>(ng));
  for (uint32_t g = 0; g < ng; ++g) {
    AppendBE64(e.buf, best_msb[g]);
    AppendBE64(e.buf, best_base[g]);
    AppendBE16(e.buf, static_cast<uint16_t>(best_start[g]));
    e.buf.push_back(static_cast<char>(static_cast<uint8_t>(best_width[g])));
  }
  for (uint32_t g = 0; g < ng; ++g) {
    if (best_width[g] == 0) {
      continue;  // constant group: no payload bytes
    }
    const size_t rows = (g + 1 == ng ? n : best_start[g + 1]) - best_start[g];
    std::vector<uint64_t> deltas(rows);
    for (size_t t = 0; t < rows; ++t) {
      const uint64_t lsb =
          bits[best_start[g] + t] & ((UINT64_C(1) << best_s) - 1);
      deltas[t] = lsb - best_base[g];
    }
    AppendPackBits(e.buf, deltas, static_cast<int>(best_width[g]));
  }
  return e;
}

// Encoder for EXPFOR (scheme 60): runs of consecutive rows sharing an
// IEEE total-order band (top 12 key bits = sign + biased exponent); each run
// stores its minimum mantissa as a FOR base and the mantissa offsets at the
// run's widest-offset width. No split search: the split is structural. The
// directory is in row order (unsorted columns interleave bands, so gkey is
// NOT ascending across runs). Applicability gate (caller re-checks nulls):
// n >= 2 and every cell finite -- NaN/Inf (exponent 0x7ff) have no
// meaningful order bracket.
EncodedCol EncExpForF64(const std::vector<uint64_t>& bits, size_t budget) {
  const size_t n = bits.size();
  if (n < 2 || n > PaxChunk::kRowGroupSize) {
    return {};
  }
  std::vector<uint64_t> keys(n);
  for (size_t i = 0; i < n; ++i) {
    if (((bits[i] >> 52) & UINT64_C(0x7ff)) == UINT64_C(0x7ff)) {
      return {};
    }
    keys[i] = ExpForBitsToKey(bits[i]);
  }
  // Single sweep: runs form wherever the 12-bit band changes between
  // consecutive rows. Rows arrive in ARBITRARY order (the whole point vs
  // kSfor), so each run's width comes from its widest offset max - base,
  // not the last row's.
  std::vector<uint64_t> gkey, gbase, gwidth;
  std::vector<uint32_t> gstart;
  uint64_t payload = 0;
  for (size_t i = 0; i < n;) {
    const uint64_t key = keys[i] >> 52;
    const size_t start = i;
    uint64_t base = keys[i] & kExpForMantMask;
    uint64_t mx = base;
    for (++i; i < n && (keys[i] >> 52) == key; ++i) {
      const uint64_t lo52 = keys[i] & kExpForMantMask;
      base = std::min(lo52, base);
      mx = std::max(lo52, mx);
    }
    const auto width = static_cast<uint64_t>(BitsNeeded(mx - base));
    gkey.push_back(key);
    gbase.push_back(base);
    gwidth.push_back(width);
    gstart.push_back(static_cast<uint32_t>(start));
    payload += ((i - start) * width + 7) / 8;
  }
  const auto ng = static_cast<uint32_t>(gstart.size());
  // Structure gate (the kTopk head-gate rule): the run partition must stay
  // BOUNDED -- ng > kExpForMaxRuns means the data alternates magnitude
  // bands faster than the run structure can pay for AND degrades every
  // compressed-domain kernel: the predicate fold scans ng entries linearly
  // and a probe's parse is O(ng), so per-query cost grows with the run
  // count even when the bytes win the race. Measured (bench_expfor, 65,536
  // rows, one run per row-group pair): the oscillating shape at ng=83
  // emitted 7.4B/row (2% under CHIMP128) but its range COUNT ran 6.8x
  // PLAIN -- parse-dominated -- while the clustered shape the gate admits
  // (ng = 8-16) emits 5.1B/row (37% under the next best) at 1.7x PLAIN's
  // COUNT. An ABSOLUTE bound, not a relative one: small cells (an 11-row
  // tail with one run) are exactly where the 15-byte header amortizes, and
  // a <= 64-entry scan is a bounded ~0.9KB regardless of n.
  if (ng > kExpForMaxRuns) {
    return {};
  }
  const size_t total =
      2 + (static_cast<size_t>(ng) * kExpForDirEntry) + payload;
  if (total > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kExpFor;
  e.param = 0;  // the split is structural; nothing to echo
  AppendBE16(e.buf, static_cast<uint16_t>(ng));
  for (uint32_t g = 0; g < ng; ++g) {
    AppendBE16(e.buf, static_cast<uint16_t>(gkey[g]));
    AppendBE64(e.buf, gbase[g]);
    AppendBE16(e.buf, static_cast<uint16_t>(gstart[g]));
    e.buf.push_back(static_cast<char>(static_cast<uint8_t>(gwidth[g])));
  }
  for (uint32_t g = 0; g < ng; ++g) {
    if (gwidth[g] == 0) {
      continue;  // constant group: no payload bytes
    }
    const size_t rows = (g + 1 == ng ? n : gstart[g + 1]) - gstart[g];
    std::vector<uint64_t> offsets(rows);
    for (size_t t = 0; t < rows; ++t) {
      offsets[t] = (keys[gstart[g] + t] & kExpForMantMask) - gbase[g];
    }
    AppendPackBits(e.buf, offsets, static_cast<int>(gwidth[g]));
  }
  return e;
}

// kS4 (scheme 55): per-block frame of reference on first differences. See the
// block comment above for why the width and the FOR base are per block rather
// than per region, and why the predictor is the first difference rather than a
// running max (the running-max variant was implemented, measured, and dropped
// -- it lost to kDeltaFp on every shape tried).
EncodedCol EncS4I64(const std::vector<int64_t>& vals, size_t budget) {
  const size_t n = vals.size();
  if (n < 2 || n > PaxChunk::kRowGroupSize) {
    return {};
  }
  const auto nblk = static_cast<uint32_t>((n + kS4Block - 1) / kS4Block);

  std::vector<uint8_t> widths(nblk, 1);
  std::vector<int64_t> lows(nblk, 0);
  std::vector<int64_t> ckpt(nblk, 0);
  std::vector<std::vector<uint64_t>> blocks(nblk);
  uint64_t total = 0;
  int64_t run = vals[0];  // the value of the previous row
  for (uint32_t b = 0; b < nblk; ++b) {
    const size_t lo = static_cast<size_t>(b) * kS4Block;
    const size_t hi = std::min(n, lo + kS4Block);
    // Block 0's first row has no predecessor, so its delta is measured against
    // itself (0) and its seek value is `base`.
    if (b != 0) {
      ckpt[b] = run;
    }
    std::vector<uint64_t>& words = blocks[b];
    words.reserve(hi - lo);
    int64_t mn = INT64_MAX, mx = INT64_MIN;
    for (size_t i = lo; i < hi; ++i) {
      // Unsigned subtraction, then reinterpret: a column that steps from
      // INT64_MIN to INT64_MAX overflows a signed difference (kDeltaFp's
      // encoder does the same, and gates on the result being non-negative).
      const int64_t d =
          i == 0 ? 0
                 : static_cast<int64_t>(static_cast<uint64_t>(vals[i]) -
                                        static_cast<uint64_t>(vals[i - 1]));
      words.push_back(static_cast<uint64_t>(d));
      mn = std::min(mn, d);
      mx = std::max(mx, d);
      run = vals[i];
    }
    lows[b] = mn;
    // Width 0 would mean the block is a single repeated delta; pin it to 1 so
    // a block is never an empty payload and the extent stays exact. This is
    // also the canonicality rule ValidateWalk re-checks.
    // Wrapping span for the same reason: mx - mn overflows when a block spans
    // both int64 extremes.
    const uint64_t span = static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
    const int w = BitsNeeded(span) == 0 ? 1 : BitsNeeded(span);
    widths[b] = static_cast<uint8_t>(w);
    total += ((hi - lo) * widths[b] + 7) / 8;
  }

  if (S4HeaderBytes(static_cast<uint32_t>(n)) + total > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kS4;
  // No single width parameterizes this scheme (the widths are per block and
  // live in the region), so the footer param is 0. The cost model uses
  // bytes x decode_cost, and the byte count comes from the region itself.
  e.param = 0;
  AppendBE64(e.buf, static_cast<uint64_t>(vals[0]));
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(nblk)));
  for (uint32_t b = 0; b < nblk; ++b) {
    e.buf.push_back(static_cast<char>(widths[b]));
  }
  for (uint32_t b = 0; b < nblk; ++b) {
    AppendBE64(e.buf, static_cast<uint64_t>(lows[b]));
  }
  for (uint32_t b = 1; b < nblk; ++b) {
    AppendBE64(e.buf, static_cast<uint64_t>(ckpt[b]));
  }
  for (uint32_t b = 0; b < nblk; ++b) {
    std::vector<uint64_t> shifted(blocks[b]);
    for (uint64_t& word : shifted) {
      word -= static_cast<uint64_t>(lows[b]);
    }
    AppendPackBits(e.buf, shifted, static_cast<int>(widths[b]));
  }
  return e;
}

// kD2Pfor (scheme 52): second differences -> an inner kPfor region, wrapped
// in kDeltaFor's 128-row seek table. n >= 3 (the inner region needs at least
// one second difference).
//
// The checkpoint pair is (v[row], sdp) where sdp = sd[row-1] for row > 0 and
// sd[0] for row 0 -- see the decode walk for why that single asymmetry is
// enough. Written in unsigned arithmetic throughout: sd and dd are wrapping
// subtractions, which is exactly the arithmetic the decoder's `val += sd`
// replay performs, so a column that walks off the end of int64 round-trips
// bit for bit instead of trapping.
EncodedCol EncD2PforI64(const std::vector<int64_t>& vals, size_t budget) {
  const size_t n = vals.size();
  if (n < 3 || n > static_cast<size_t>(UINT16_MAX) + 1) {
    return {};
  }
  const auto ncp = static_cast<uint32_t>(
      ((n + kD2PforCheckpointEvery - 1) / kD2PforCheckpointEvery) + 1);
  // Second differences: dd[k] = v[k+2] - 2*v[k+1] + v[k] = sd[k+1] - sd[k].
  std::vector<int64_t> dd(n - 2);
  for (size_t k = 0; k + 2 < n; ++k) {
    const auto left = static_cast<int64_t>(static_cast<uint64_t>(vals[k + 2]) -
                                           static_cast<uint64_t>(vals[k + 1]));
    const auto right = static_cast<int64_t>(static_cast<uint64_t>(vals[k + 1]) -
                                            static_cast<uint64_t>(vals[k]));
    dd[k] = static_cast<int64_t>(static_cast<uint64_t>(left) -
                                 static_cast<uint64_t>(right));
  }
  // The inner region must be a SELF-CONTAINED kPfor image, so it is encoded
  // with its own budget: what matters is that the whole composite fits.
  EncodedCol inner = EncPforI64(dd, budget);
  if (inner.scheme != Scheme::kPfor || inner.buf.empty()) {
    return {};
  }
  const size_t total = 2 + (16 * static_cast<size_t>(ncp)) + inner.buf.size();
  if (total > budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kD2Pfor;
  e.param = inner.param;  // the inner PFor width, like kPfor's own param
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t row = std::min<size_t>(
        static_cast<size_t>(b) * kD2PforCheckpointEvery, n - 1);
    AppendBE64(e.buf, static_cast<uint64_t>(vals[row]));
    const int64_t sdp =
        (row == 0) ? static_cast<int64_t>(static_cast<uint64_t>(vals[1]) -
                                          static_cast<uint64_t>(vals[0]))
                   : static_cast<int64_t>(static_cast<uint64_t>(vals[row]) -
                                          static_cast<uint64_t>(vals[row - 1]));
    AppendBE64(e.buf, static_cast<uint64_t>(sdp));
  }
  e.buf.insert(e.buf.end(), inner.buf.begin(), inner.buf.end());
  return e;
}

// Zigzag: signed domain -> unsigned (small magnitudes stay small). Uses only
// unsigned arithmetic (INT64_MIN-safe).
uint64_t ZigZagEnc(int64_t v) {
  return (static_cast<uint64_t>(v) << 1) ^
         static_cast<uint64_t>(-(v < 0 ? 1 : 0));
}
// Delta-of-delta + Rice for regular int64 sequences (timestamps). Stores the
// first value/delta per checkpoint block and Rice(k)-codes zigzagged second
// differences (dd[1] = d1[1]). Layout: [ncp u16][checkpoints ncp x (val 8B,
// d1 8B)][bitoffs ncp x u32 BE][k u8][bitstream]. param = k.
EncodedCol EncDdRiceI64(const std::vector<int64_t>& vals, size_t budget) {
  const size_t n = vals.size();
  if (n == 0) {
    return {};
  }
  const uint32_t C = kDeltaCheckpointEvery;
  const auto ncp = static_cast<uint32_t>(((n - 1) / C) + 1);
  // Second differences in zigzag domain (dd[1] = first delta itself).
  std::vector<uint64_t> zz(n, 0);
  uint64_t prev_d1 = 0;
  for (size_t i = 1; i < n; ++i) {
    const uint64_t d1 =
        static_cast<uint64_t>(vals[i]) - static_cast<uint64_t>(vals[i - 1]);
    const uint64_t dd = (i == 1) ? d1 : d1 - prev_d1;
    prev_d1 = d1;
    zz[i] = ZigZagEnc(static_cast<int64_t>(dd));
  }
  int best_k = 0;
  size_t best_bits = SIZE_MAX;
  // Each of the n-1 coded values costs at least 1 + k bits (a zero quotient
  // plus the k unary bits), so a k whose floor already blows the byte budget
  // cannot win: skip its O(n) count without scanning.
  const size_t ddrice_header =
      2 + (16 * static_cast<size_t>(ncp)) + (4 * static_cast<size_t>(ncp)) + 1;
  const size_t ddrice_bit_budget =
      budget > ddrice_header ? (budget - ddrice_header) * 8 : 0;
  for (int k = 0; k <= 20; ++k) {
    if ((n - 1) * static_cast<size_t>(k + 1) > ddrice_bit_budget) {
      continue;
    }
    size_t bits = 0;
    bool bad = false;
    for (size_t i = 1; i < n; ++i) {
      const uint64_t q = (k >= 64) ? 0 : (zz[i] >> k);
      if (q > kRiceQMax) {
        bad = true;
        break;
      }
      bits += static_cast<size_t>(q) + 1 + static_cast<size_t>(k);
    }
    if (bad) {
      continue;
    }
    if (bits < best_bits) {
      best_bits = bits;
      best_k = k;
    }
  }
  if (best_bits == SIZE_MAX) {
    return {};  // quotients exceed the scan bound
  }
  // Exact region size: checkpoint count + (value, d1) per checkpoint +
  // bit offsets + Rice parameter + the unary-prefixed bitstream.
  if (2 + (16 * static_cast<size_t>(ncp)) + (4 * static_cast<size_t>(ncp)) + 1 +
          ((best_bits + 7) / 8) >
      budget) {
    return {};
  }
  EncodedCol e;
  e.scheme = Scheme::kDdRice;
  e.param = static_cast<uint32_t>(best_k);
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t idx = static_cast<size_t>(b) * C;
    AppendBE64(e.buf, static_cast<uint64_t>(vals[idx]));
    const uint64_t d1 = (idx == 0) ? 0
                                   : static_cast<uint64_t>(vals[idx]) -
                                         static_cast<uint64_t>(vals[idx - 1]);
    AppendBE64(e.buf, d1);
  }
  const size_t offs_pos = e.buf.size();
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE32(e.buf, 0);  // patched below
  }
  e.buf.push_back(static_cast<char>(best_k));
  BitWriter w;
  // offs[b] = bit position of dd[b*C+1] (first code block b needs); offs[0]
  // is 0 by construction. Single-row tail blocks (b*C == n-1) never read
  // theirs and keep the stream-end backfill below.
  std::vector<uint32_t> offs(ncp, UINT32_MAX);
  offs[0] = 0;
  const auto bitpos = [&]() -> uint32_t {
    return static_cast<uint32_t>((w.buf.size() * 8) +
                                 static_cast<size_t>(w.cnt));
  };
  for (size_t i = 1; i < n; ++i) {
    if (i % C == 1 && i / C < ncp) {
      offs[i / C] = bitpos();
    }
    const uint64_t q = (best_k >= 64) ? 0 : (zz[i] >> best_k);
    for (uint64_t t = 0; t < q; ++t) {
      w.put1(1);
    }
    w.put1(0);
    if (best_k > 0) {
      const uint64_t mask = (best_k >= 64) ? ~0ULL : ((1ULL << best_k) - 1ULL);
      w.put(zz[i] & mask, best_k);
    }
  }
  w.finish();
  const auto end_pos = static_cast<uint32_t>(w.buf.size() * 8);
  for (uint32_t b = 0; b < ncp; ++b) {
    if (offs[b] == UINT32_MAX) {
      offs[b] = end_pos;
    }
  }
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t o = offs_pos + (static_cast<size_t>(b) * 4);
    e.buf[o] = static_cast<char>((offs[b] >> 24) & 0xFF);
    e.buf[o + 1] = static_cast<char>((offs[b] >> 16) & 0xFF);
    e.buf[o + 2] = static_cast<char>((offs[b] >> 8) & 0xFF);
    e.buf[o + 3] = static_cast<char>(offs[b] & 0xFF);
  }
  e.buf.insert(e.buf.end(), w.buf.begin(), w.buf.end());
  return e;
}

// Checkpointed LEB128 deltas (byte-stream varint): first-order zigzag
// differences with implicit boundaries, so random access needs only a block
// base value plus a byte offset. Layout: [ncp u16][checkpoints ncp*8B]
// [byteoffs ncp*u32 BE][LEB128 zigzag deltas, deltas[0]=0]. param = 0.
EncodedCol EncDeltaVarintI64(const std::vector<int64_t>& vals, size_t budget) {
  static_cast<void>(budget);
  const size_t n = vals.size();
  if (n == 0) {
    return {};
  }
  const uint32_t C = kDeltaCheckpointEvery;
  const auto ncp = static_cast<uint32_t>(((n - 1) / C) + 1);
  EncodedCol e;
  e.scheme = Scheme::kDeltaVarint;
  e.param = 0;
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE64(e.buf, static_cast<uint64_t>(vals[static_cast<size_t>(b) * C]));
  }
  const size_t offs_pos = e.buf.size();
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE32(e.buf, 0);
  }
  // The scratch stream and offset table are thread_local: adaptive
  // selection re-runs this encoder for every row group, so per-attempt
  // malloc/free pairs would feed the allocator for nothing.
  thread_local std::vector<char> stream;
  thread_local std::vector<uint32_t> offs;
  stream.clear();
  stream.reserve(n - 1);
  offs.assign(ncp, 0);
  // deltas[0] = 0 occupies the first byte, so block 0 starts reading at 1.
  stream.push_back('\0');
  offs[0] = 1;
  for (size_t i = 1; i < n; ++i) {
    if (i % C == 1 && i / C < ncp && i > 1) {
      offs[i / C] = static_cast<uint32_t>(stream.size());
    }
    uint64_t z = ZigZagEnc(static_cast<int64_t>(
        static_cast<uint64_t>(vals[i]) - static_cast<uint64_t>(vals[i - 1])));
    do {
      auto byte = static_cast<uint8_t>(z & 0x7F);
      z >>= 7;
      if (z != 0) {
        byte = static_cast<uint8_t>(byte | 0x80);
      }
      stream.push_back(static_cast<char>(byte));
    } while (z != 0);
    // Every delta emits at least one byte, so the final stream is never
    // shorter than the bytes written so far: once that prefix alone is over
    // budget the candidate cannot win. Checked every C rows, which bounds
    // the abort lag by C bytes while keeping the per-row branch off the
    // steady-state path.
    if (i % 32 == 0 && stream.size() > budget) {
      return {};
    }
  }
  // Single-row tail blocks never read their offset; point it at the end.
  for (uint32_t b = 0; b < ncp; ++b) {
    if (offs[b] == 0) {
      offs[b] = static_cast<uint32_t>(stream.size());
    }
  }
  offs[0] = 1;  // deltas[0] always occupies byte 0
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t o = offs_pos + (static_cast<size_t>(b) * 4);
    e.buf[o] = static_cast<char>((offs[b] >> 24) & 0xFF);
    e.buf[o + 1] = static_cast<char>((offs[b] >> 16) & 0xFF);
    e.buf[o + 2] = static_cast<char>((offs[b] >> 8) & 0xFF);
    e.buf[o + 3] = static_cast<char>(offs[b] & 0xFF);
  }
  e.buf.insert(e.buf.end(), stream.begin(), stream.end());
  return e;
}

// ---- Binary interpolative coding (kInterp) --------------------------------
//
// Moffat & Stuiver 2000 ("Binary Interpolative Coding for Effective Index
// Compression"), the posting-list codec of Lucene/PISA-style engines, over
// NON-DECREASING int64 sequences. A block of rows [i0, i1) whose values lie
// inside the bracket [lo, hi] stores its middle element as the offset
// v[mid] - lo in BitsNeeded(hi - lo) bits, then recurses into the left half
// (bracket [lo, v[mid]]) and the right half (bracket [v[mid], hi]). Widths
// shrink toward the leaves, so values clustered inside a narrow span cost a
// handful of bits each while a single huge gap is paid a few times near the
// root -- Elias-Fano / DELTA-FP instead pad EVERY row to the max-gap width.
// Each block is encoded as its own tree rooted at the block's checkpoint
// bracket, so a range predicate can skip whole blocks (bracket misses the
// window) and answer fully-inside blocks from the header alone.

// Recursive emit for rows [i0, i1) of a non-decreasing run inside [lo, hi]
// (u64 two's-complement order, so INT64 extremes wrap correctly). Bit order
// is node, left subtree, right subtree -- the decoder mirrors it exactly.
void InterpEmit(const std::vector<int64_t>& vals, uint32_t i0, uint32_t i1,
                uint64_t lo, uint64_t hi, BitWriter* w) {
  if (i0 >= i1) {
    return;
  }
  const uint32_t mid = i0 + ((i1 - i0) / 2);
  const int bits = BitsNeeded(hi - lo);
  if (bits > 0) {
    w->put(static_cast<uint64_t>(vals[mid]) - lo, bits);
  }
  InterpEmit(vals, i0, mid, lo, static_cast<uint64_t>(vals[mid]), w);
  InterpEmit(vals, mid + 1, i1, static_cast<uint64_t>(vals[mid]), hi, w);
}

EncodedCol EncInterpI64(const std::vector<int64_t>& vals, size_t budget) {
  static_cast<void>(budget);
  const size_t n = vals.size();
  if (n == 0) {
    return {};
  }
  for (size_t i = 1; i < n; ++i) {
    if (vals[i] < vals[i - 1]) {
      return {};  // non-decreasing input only
    }
  }
  const auto ncp =
      static_cast<uint32_t>((n + kInterpBlockRows - 1) / kInterpBlockRows);
  EncodedCol e;
  e.scheme = Scheme::kInterp;
  e.param = ncp;
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t s = static_cast<size_t>(b) * kInterpBlockRows;
    const size_t t = std::min(n, s + kInterpBlockRows);
    AppendBE64(e.buf, static_cast<uint64_t>(vals[s]));
    AppendBE64(e.buf, static_cast<uint64_t>(vals[t - 1]));
  }
  const size_t offs_pos = e.buf.size();
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE32(e.buf, 0);  // patched below
  }
  BitWriter w;
  const auto bitpos = [&]() -> uint32_t {
    return static_cast<uint32_t>((w.buf.size() * 8) +
                                 static_cast<size_t>(w.cnt));
  };
  std::vector<uint32_t> bitoffs(ncp, 0);
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t s = static_cast<size_t>(b) * kInterpBlockRows;
    const size_t t = std::min(n, s + kInterpBlockRows);
    bitoffs[b] = bitpos();
    InterpEmit(vals, static_cast<uint32_t>(s), static_cast<uint32_t>(t),
               static_cast<uint64_t>(vals[s]),
               static_cast<uint64_t>(vals[t - 1]), &w);
  }
  w.finish();
  e.buf.insert(e.buf.end(), w.buf.begin(), w.buf.end());
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t o = offs_pos + (static_cast<size_t>(b) * 4);
    e.buf[o] = static_cast<char>((bitoffs[b] >> 24) & 0xFF);
    e.buf[o + 1] = static_cast<char>((bitoffs[b] >> 16) & 0xFF);
    e.buf[o + 2] = static_cast<char>((bitoffs[b] >> 8) & 0xFF);
    e.buf[o + 3] = static_cast<char>(bitoffs[b] & 0xFF);
  }
  return e;
}

bool QppFitsI64(__int128 value) {
  return value >= static_cast<__int128>(INT64_MIN) &&
         value <= static_cast<__int128>(INT64_MAX);
}

bool QppAdaptiveApplicable(const std::vector<int64_t>& values) {
  if (values.size() < 3) {
    return false;
  }
  // The old form materialized every second difference and ran nth_element on
  // a 16-byte-per-element vector for every row group. The upper median of the
  // m differences is zero exactly when the negative count is <= m/2 and the
  // non-positive count is >= m/2 + 1 (the sorted element at index m/2 sits
  // below-or-at zero iff nonpos >= m/2+1 and at-or-above zero iff neg <=
  // m/2), so a single allocation-free counting pass answers the same gate.
  size_t neg = 0;
  size_t nonpos = 0;
  for (size_t i = 2; i < values.size(); ++i) {
    const __int128 sd = static_cast<__int128>(values[i]) -
                        (2 * static_cast<__int128>(values[i - 1])) +
                        static_cast<__int128>(values[i - 2]);
    neg += static_cast<size_t>(sd < 0);
    nonpos += static_cast<size_t>(sd <= 0);
  }
  const size_t m = values.size() - 2;
  const size_t k = m / 2;
  return neg > k || nonpos < k + 1;
}

EncodedCol EncQppI64(const std::vector<int64_t>& vals, size_t budget) {
  static_cast<void>(budget);
  const size_t n = vals.size();
  if (n == 0 || n > PaxChunk::kRowGroupSize) {
    return {};
  }
  const auto pages =
      static_cast<uint32_t>((n + kQppPageRows - 1) / kQppPageRows);
  EncodedCol encoded;
  encoded.scheme = Scheme::kQpp;
  encoded.param = pages;
  AppendBE16(encoded.buf, static_cast<uint16_t>(pages));

  for (uint32_t page_index = 0; page_index < pages; ++page_index) {
    const size_t start = static_cast<size_t>(page_index) * kQppPageRows;
    const size_t rows = std::min<size_t>(kQppPageRows, n - start);
    __int128 a = vals[start];
    __int128 d1 = 0;
    __int128 d2 = 0;
    if (rows == 2) {
      d1 = static_cast<__int128>(vals[start + 1]) - a;
    }
    if (rows >= 3) {
      std::vector<__int128> second_differences;
      second_differences.reserve(rows - 2);
      for (size_t local = 2; local < rows; ++local) {
        const auto current = static_cast<__int128>(vals[start + local]);
        const auto previous = static_cast<__int128>(vals[start + local - 1]);
        const auto before = static_cast<__int128>(vals[start + local - 2]);
        second_differences.push_back(current - (2 * previous) + before);
      }
      const size_t middle = second_differences.size() / 2;
      std::nth_element(
          second_differences.begin(),
          second_differences.begin() + static_cast<std::ptrdiff_t>(middle),
          second_differences.end());
      d2 = second_differences[middle];
      if (!QppFitsI64(d2)) {
        return {};
      }

      size_t best_exceptions = SIZE_MAX;
      __int128 best_cost = 0;
      bool found = false;
      for (size_t anchor = 0; anchor + 1 < rows; ++anchor) {
        const auto anchor_value = static_cast<__int128>(vals[start + anchor]);
        const auto next_value = static_cast<__int128>(vals[start + anchor + 1]);
        const __int128 candidate_d1 =
            next_value - anchor_value - (static_cast<__int128>(anchor) * d2);
        const __int128 candidate_a =
            anchor_value - (static_cast<__int128>(anchor) * candidate_d1) -
            ((static_cast<__int128>(anchor) *
              (static_cast<__int128>(anchor) - 1) / 2) *
             d2);
        if (!QppFitsI64(candidate_a) || !QppFitsI64(candidate_d1)) {
          continue;
        }
        size_t exception_count = 0;
        __int128 cost = 0;
        bool fits = true;
        for (size_t local = 0; local < rows; ++local) {
          const auto i = static_cast<__int128>(local);
          const __int128 predicted =
              candidate_a + (i * candidate_d1) + ((i * (i - 1) / 2) * d2);
          if (!QppFitsI64(predicted)) {
            fits = false;
            break;
          }
          const auto actual = static_cast<__int128>(vals[start + local]);
          if (predicted != actual) {
            ++exception_count;
            const __int128 delta = predicted - actual;
            cost += delta < 0 ? -delta : delta;
          }
        }
        if (!fits) {
          continue;
        }
        if (exception_count < best_exceptions ||
            (exception_count == best_exceptions && cost < best_cost)) {
          found = true;
          best_exceptions = exception_count;
          best_cost = cost;
          a = candidate_a;
          d1 = candidate_d1;
        }
      }
      if (!found) {
        return {};
      }
    }
    if (!QppFitsI64(a) || !QppFitsI64(d1)) {
      return {};
    }

    std::vector<uint8_t> bitmap;
    std::vector<std::pair<uint16_t, int64_t>> exceptions;
    for (size_t local = 0; local < rows; ++local) {
      const auto i = static_cast<__int128>(local);
      const __int128 predicted = a + (i * d1) + ((i * (i - 1) / 2) * d2);
      if (!QppFitsI64(predicted)) {
        return {};
      }
      if (predicted != static_cast<__int128>(vals[start + local])) {
        exceptions.emplace_back(static_cast<uint16_t>(local),
                                vals[start + local]);
      }
    }

    AppendBE64(encoded.buf, static_cast<uint64_t>(a));
    AppendBE64(encoded.buf, static_cast<uint64_t>(d1));
    AppendBE64(encoded.buf, static_cast<uint64_t>(d2));
    AppendBE16(encoded.buf, static_cast<uint16_t>(exceptions.size()));
    if (exceptions.empty()) {
      continue;
    }

    bitmap.assign((rows + 7) / 8, 0);
    for (const auto& [position, value] : exceptions) {
      (void)value;
      bitmap[position >> 3] = static_cast<uint8_t>(bitmap[position >> 3] |
                                                   (0x80U >> (position & 7U)));
    }
    encoded.buf.insert(
        encoded.buf.end(), reinterpret_cast<const char*>(bitmap.data()),
        reinterpret_cast<const char*>(bitmap.data()) + bitmap.size());
    for (const auto& [position, value] : exceptions) {
      AppendBE16(encoded.buf, position);
      AppendBE64(encoded.buf, static_cast<uint64_t>(value));
    }
  }
  return encoded;
}

// Chimp double codec (bitcast u64): '0' identical to previous, '10' reuses
// the previous (lz, tz) window for the XOR center bits, '11' + 5-bit lz +
// 6-bit tz + center bits (mw = 64-lz-tz, derived like Gorilla, so mw == 64
// needs no over-wide length field). Layout: [first u64 BE][bitstream].
// param unused.
EncodedCol EncChimpF64(const std::vector<uint64_t>& bits) {
  EncodedCol e;
  e.scheme = Scheme::kChimp;
  e.param = 0;
  if (bits.empty()) {
    AppendBE64(e.buf, 0);
    return e;
  }
  AppendBE64(e.buf, bits.front());
  BitWriter w;
  uint64_t prev = bits.front();
  int prev_lz = 0, prev_tz = 64;  // impossible window forces '11' first
  for (size_t i = 1; i < bits.size(); ++i) {
    const uint64_t x = bits[i] ^ prev;
    if (x == 0) {
      w.put1(0);
    } else {
      int lz = __builtin_clzll(x);
      int tz = __builtin_ctzll(x);
      lz = std::min(lz, 31);
      if (lz >= prev_lz && tz >= prev_tz && prev_lz + prev_tz < 64) {
        w.put1(1);
        w.put1(0);
        const int width = 64 - prev_lz - prev_tz;
        w.put(x >> static_cast<unsigned>(prev_tz), width);
      } else {
        w.put1(1);
        w.put1(1);
        w.put(static_cast<uint64_t>(lz), 5);
        w.put(static_cast<uint64_t>(tz), 6);
        w.put(x >> tz, 64 - lz - tz);
        prev_lz = lz;
        prev_tz = tz;
      }
    }
    prev = bits[i];
  }
  w.finish();
  e.buf.insert(e.buf.end(), w.buf.begin(), w.buf.end());
  return e;
}

// --- Checkpointed XOR-FOR (scheme 31) ----------------------------------------
// The scheme-7 stream is serial: value i needs xors[0..i-1] accumulated from
// row 0. The checkpointed variant stores v[b*128] verbatim, so random access
// XORs at most 127 deltas. Layout: [ncp u16][ckpts ncp x u64 BE][w u8]
// [xors (n-1)*w bits MSB-first]; param = w.
EncodedCol EncXorForCF64(const std::vector<uint64_t>& bits, size_t budget) {
  static_cast<void>(budget);
  if (bits.size() <= 1) {
    return {};
  }
  const size_t n = bits.size();
  const uint32_t C = kDeltaCheckpointEvery;
  const auto ncp = static_cast<uint32_t>((n + C - 1) / C);
  // Thread-local scratch: this encoder is re-run for every row group.
  thread_local std::vector<uint64_t> xors;
  xors.clear();
  xors.reserve(n - 1);
  uint64_t maxx = 0;
  for (size_t i = 1; i < n; ++i) {
    const uint64_t x = bits[i] ^ bits[i - 1];
    xors.push_back(x);
    maxx = std::max(maxx, x);
  }
  int w = BitsNeeded(maxx);
  if (w == 0) {
    w = 1;  // validator pins param to [1,64] (mirrors EncXorF64)
  }
  EncodedCol e;
  e.scheme = Scheme::kXorForC;
  e.param = static_cast<uint32_t>(w);
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE64(e.buf, bits[static_cast<size_t>(b) * C]);
  }
  e.buf.push_back(static_cast<char>(w));
  AppendPackBits(e.buf, xors, w);
  return e;
}

// --- Checkpointed Gorilla (scheme 32) ----------------------------------------
// Same XOR stream as EncGorillaF64, preceded by a (value, bit-offset) table
// per 128-row block (the DdRice checkpoint shape). bitoffs[b] is the stream
// bit position where row b*128+1's code begins; offs[0] = 0.
EncodedCol EncGorillaCF64(const std::vector<uint64_t>& bits) {
  if (bits.size() <= 1) {
    return {};
  }
  const size_t n = bits.size();
  const uint32_t C = kDeltaCheckpointEvery;
  const auto ncp = static_cast<uint32_t>((n + C - 1) / C);
  EncodedCol e;
  e.scheme = Scheme::kGorillaC;
  e.param = 0;
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE64(e.buf, bits[static_cast<size_t>(b) * C]);
  }
  const size_t offs_pos = e.buf.size();
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE32(e.buf, 0);  // patched below
  }
  AppendBE64(e.buf, bits.front());
  BitWriter w;
  uint64_t prev = bits.front();
  std::vector<uint32_t> offs(ncp, UINT32_MAX);
  offs[0] = 0;
  const auto bitpos = [&]() -> uint32_t {
    return static_cast<uint32_t>((w.buf.size() * 8) +
                                 static_cast<size_t>(w.cnt));
  };
  for (size_t i = 1; i < n; ++i) {
    if (i % C == 1 && i / C < ncp) {
      offs[i / C] = bitpos();
    }
    const uint64_t x = bits[i] ^ prev;
    if (x == 0) {
      w.put1(0);
    } else {
      w.put1(1);
      int lz = __builtin_clzll(x);
      int tz = __builtin_ctzll(x);
      lz = std::min(lz, 31);
      const int mw = 64 - lz - tz;
      w.put(static_cast<uint64_t>(lz), 5);
      w.put(static_cast<uint64_t>(tz), 6);
      w.put(x >> tz, mw);
    }
    prev = bits[i];
  }
  w.finish();
  const auto end_pos = static_cast<uint32_t>(w.buf.size() * 8);
  for (uint32_t b = 0; b < ncp; ++b) {
    if (offs[b] == UINT32_MAX) {
      offs[b] = end_pos;
    }
  }
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t o = offs_pos + (static_cast<size_t>(b) * 4);
    e.buf[o] = static_cast<char>((offs[b] >> 24) & 0xFF);
    e.buf[o + 1] = static_cast<char>((offs[b] >> 16) & 0xFF);
    e.buf[o + 2] = static_cast<char>((offs[b] >> 8) & 0xFF);
    e.buf[o + 3] = static_cast<char>(offs[b] & 0xFF);
  }
  e.buf.insert(e.buf.end(), w.buf.begin(), w.buf.end());
  return e;
}

// --- Checkpointed Chimp (scheme 33) ------------------------------------------
// Same envelope as kGorillaC. Chimp's '10' code reuses the previous row's
// (lz,tz) window; a decoder resuming at bitoffs[b] has no window, so the
// encoder emits a full '11' code for every row at a block-start + 1
// boundary (i % 128 == 1).
EncodedCol EncChimpCF64(const std::vector<uint64_t>& bits) {
  if (bits.size() <= 1) {
    return {};
  }
  const size_t n = bits.size();
  const uint32_t C = kDeltaCheckpointEvery;
  const auto ncp = static_cast<uint32_t>((n + C - 1) / C);
  EncodedCol e;
  e.scheme = Scheme::kChimpC;
  e.param = 0;
  AppendBE16(e.buf, static_cast<uint16_t>(ncp));
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE64(e.buf, bits[static_cast<size_t>(b) * C]);
  }
  const size_t offs_pos = e.buf.size();
  for (uint32_t b = 0; b < ncp; ++b) {
    AppendBE32(e.buf, 0);  // patched below
  }
  AppendBE64(e.buf, bits.front());
  BitWriter w;
  uint64_t prev = bits.front();
  int prev_lz = 0, prev_tz = 64;  // impossible window forces '11' first
  std::vector<uint32_t> offs(ncp, UINT32_MAX);
  offs[0] = 0;
  const auto bitpos = [&]() -> uint32_t {
    return static_cast<uint32_t>((w.buf.size() * 8) +
                                 static_cast<size_t>(w.cnt));
  };
  for (size_t i = 1; i < n; ++i) {
    if (i % C == 1 && i / C < ncp) {
      offs[i / C] = bitpos();
      // The decoder resumes at this row without a window: force '11'.
      prev_lz = 0;
      prev_tz = 64;
    }
    const uint64_t x = bits[i] ^ prev;
    if (x == 0) {
      w.put1(0);
    } else {
      int lz = __builtin_clzll(x);
      int tz = __builtin_ctzll(x);
      lz = std::min(lz, 31);
      if (lz >= prev_lz && tz >= prev_tz && prev_lz + prev_tz < 64) {
        w.put1(1);
        w.put1(0);
        const int width = 64 - prev_lz - prev_tz;
        w.put(x >> static_cast<unsigned>(prev_tz), width);
      } else {
        w.put1(1);
        w.put1(1);
        w.put(static_cast<uint64_t>(lz), 5);
        w.put(static_cast<uint64_t>(tz), 6);
        w.put(x >> tz, 64 - lz - tz);
        prev_lz = lz;
        prev_tz = tz;
      }
    }
    prev = bits[i];
  }
  w.finish();
  const auto end_pos = static_cast<uint32_t>(w.buf.size() * 8);
  for (uint32_t b = 0; b < ncp; ++b) {
    if (offs[b] == UINT32_MAX) {
      offs[b] = end_pos;
    }
  }
  for (uint32_t b = 0; b < ncp; ++b) {
    const size_t o = offs_pos + (static_cast<size_t>(b) * 4);
    e.buf[o] = static_cast<char>((offs[b] >> 24) & 0xFF);
    e.buf[o + 1] = static_cast<char>((offs[b] >> 16) & 0xFF);
    e.buf[o + 2] = static_cast<char>((offs[b] >> 8) & 0xFF);
    e.buf[o + 3] = static_cast<char>(offs[b] & 0xFF);
  }
  e.buf.insert(e.buf.end(), w.buf.begin(), w.buf.end());
  return e;
}

EncodedCol EncChimp128F64(const std::vector<uint64_t>& bits) {
  if (bits.empty() || bits.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  const auto blocks = static_cast<uint32_t>(
      (bits.size() + kChimp128BlockRows - 1) / kChimp128BlockRows);
  EncodedCol encoded;
  encoded.scheme = Scheme::kChimp128;
  encoded.param = blocks;
  encoded.buf.push_back(static_cast<char>(1));
  encoded.buf.push_back(static_cast<char>(0));
  AppendBE16(encoded.buf, static_cast<uint16_t>(blocks));
  for (uint32_t block_index = 0; block_index < blocks; ++block_index) {
    const uint32_t start = block_index * kChimp128BlockRows;
    const uint32_t rows = static_cast<uint32_t>(
        std::min<size_t>(kChimp128BlockRows, bits.size() - start));
    bool numeric_metadata = true;
    bool has_numeric = false;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    double sum = Chimp128Double(bits[start]);
    for (uint32_t local = 0; local < rows; ++local) {
      const uint64_t raw = bits[start + local];
      const double value = Chimp128Double(raw);
      if (local != 0) {
        sum += value;
      }
      if (std::isnan(value)) {
        numeric_metadata = false;
        continue;
      }
      if (!has_numeric) {
        minimum = raw;
        maximum = raw;
        has_numeric = true;
      } else {
        if (value < Chimp128Double(minimum)) {
          minimum = raw;
        }
        if (value > Chimp128Double(maximum)) {
          maximum = raw;
        }
      }
    }
    encoded.buf.push_back(static_cast<char>(rows));
    encoded.buf.push_back(
        static_cast<char>(has_numeric && numeric_metadata ? 1 : 0));
    const size_t code_bits_offset = encoded.buf.size();
    AppendBE32(encoded.buf, 0);
    AppendBE64(encoded.buf, bits[start]);
    AppendBE64(encoded.buf, has_numeric ? minimum : 0);
    AppendBE64(encoded.buf, has_numeric ? maximum : 0);
    AppendBE64(encoded.buf, Chimp128CanonicalBits(sum));
    BitWriter writer;
    std::array<uint64_t, kChimp128BlockRows> history{};
    uint64_t previous = bits[start];
    history[0] = previous;
    uint32_t head = 1;
    int previous_lz = 0;
    int previous_tz = 64;
    bool window_valid = false;
    for (uint32_t local = 1; local < rows; ++local) {
      const uint64_t current = bits[start + local];
      if (current == previous) {
        writer.put1(0);
      } else {
        const uint64_t xor_previous = current ^ previous;
        int lz = __builtin_clzll(xor_previous);
        int tz = __builtin_ctzll(xor_previous);
        lz = std::min(lz, 31);
        if (window_valid && previous_lz + previous_tz < 64 &&
            lz >= previous_lz && tz >= previous_tz) {
          writer.put1(1);
          writer.put1(0);
          const int width = 64 - previous_lz - previous_tz;
          writer.put(xor_previous >> static_cast<unsigned>(previous_tz), width);
        } else {
          int best_width = 64 - lz - tz;
          int best_lz = lz;
          int best_tz = tz;
          uint32_t best_age = 0;
          for (uint32_t age = 1; age < local; ++age) {
            const uint64_t reference = history[(head - 1 - age) & 127U];
            const uint64_t candidate = current ^ reference;
            if (candidate == 0) {
              continue;
            }
            int candidate_lz = std::min(__builtin_clzll(candidate), 31);
            const int candidate_tz = __builtin_ctzll(candidate);
            const int candidate_width = 64 - candidate_lz - candidate_tz;
            if (candidate_width < best_width) {
              best_width = candidate_width;
              best_lz = candidate_lz;
              best_tz = candidate_tz;
              best_age = age;
            }
          }
          if (best_age == 0) {
            writer.put1(1);
            writer.put1(1);
            writer.put1(0);
            writer.put(static_cast<uint64_t>(best_lz), 5);
            writer.put(static_cast<uint64_t>(best_tz), 6);
            writer.put(xor_previous >> best_tz, 64 - best_lz - best_tz);
            previous_lz = best_lz;
            previous_tz = best_tz;
            window_valid = true;
          } else {
            writer.put1(1);
            writer.put1(1);
            writer.put1(1);
            writer.put(static_cast<uint64_t>(best_age), 7);
            writer.put(static_cast<uint64_t>(best_lz), 5);
            writer.put(static_cast<uint64_t>(best_tz), 6);
            const uint64_t reference = history[(head - 1 - best_age) & 127U];
            writer.put((current ^ reference) >> best_tz,
                       64 - best_lz - best_tz);
          }
        }
      }
      history[head & 127U] = current;
      ++head;
      previous = current;
    }
    const auto code_bits = static_cast<uint32_t>(
        (writer.buf.size() * 8) + static_cast<size_t>(writer.cnt));
    writer.finish();
    const uint32_t be = htobe32(code_bits);
    std::memcpy(encoded.buf.data() + code_bits_offset, &be, sizeof(be));
    encoded.buf.insert(encoded.buf.end(), writer.buf.begin(), writer.buf.end());
  }
  return encoded;
}

// --- ALP (Adaptive Lossless floating-Point, SIGMOD'24) float codec -----------
// (scheme 20). Exact decimal factorization: every value that can be written
// v == (double)m / 10^e with an int64 mantissa m is stored as (m, shared e).
// The mantissa vector is compressed by a composed integer backend (BitWeave /
// DICT / RLE / CONST -- whichever encodes smallest). Values that do not factor
// exactly (non-decimal noise, -0.0, NaN/Inf, magnitudes past int64) become
// PFOR-style exceptions carrying the full 8B value. Region layout:
//   [exp u8 BE][backend u8 BE][nexc u16 BE]
//   [exc (pos u16 BE + val 8B BE) x nexc]
//   [inner region: backend scheme's ordinary self-contained region]
// backend holds the inner scheme ID (1/2/4/8); param is unused (the inner
// headers are authoritative and the validator walks them). The encode side
// verifies the round-trip (double)llround(v*10^e) / 10^e == v bitwise per
// value, so decode reproduces the original bits exactly; both sides divide by
// the same AlpFactorOf(exp) constant.
EncodedCol EncAlpF64(const std::vector<uint64_t>& bits) {
  if (bits.empty()) {
    return {};
  }
  const size_t n = bits.size();
  constexpr int kMaxExp = 18;
  size_t best_bytes = SIZE_MAX;
  int best_exp = 0;
  std::vector<int64_t> best_mant;
  std::vector<uint32_t> best_exc_pos;
  std::vector<uint64_t> best_exc_val;
  std::vector<int64_t> mant(n);
  std::vector<uint32_t> exc_pos;
  std::vector<uint64_t> exc_val;
  for (int exp = 0; exp <= kMaxExp; ++exp) {
    const double f = AlpFactorOf(exp);
    exc_pos.clear();
    exc_val.clear();
    // Factorization: v == (double)m / 10^exp with m = round(v * 10^exp).
    // The bound keeps v * 10^exp inside int64 with headroom (else llround
    // is UB); NaN/Inf fail both comparisons and take the exception path.
    const double lim = 9.0e18 / f;
    for (size_t i = 0; i < n; ++i) {
      double d = NAN;
      std::memcpy(&d, &bits[i], sizeof(d));
      bool ok = false;
      int64_t m = 0;
      if (d > -lim && d < lim) {
        const long long mr = std::llround(d * f);
        // Divide (exact power-of-ten divisor), never multiply by an inexact
        // 1/f: the decode side runs the identical division, so this bitwise
        // verify guarantees decode reproduces the original bits.
        const double back = static_cast<double>(mr) / f;
        uint64_t bb = 0;
        std::memcpy(&bb, &back, sizeof(bb));
        if (bb == bits[i]) {
          m = static_cast<int64_t>(mr);
          ok = true;
        }
      }
      if (ok) {
        mant[i] = m;
      } else {
        mant[i] = 0;
        exc_pos.push_back(static_cast<uint32_t>(i));
        exc_val.push_back(bits[i]);
      }
    }
    int64_t mn = mant.front(), mx = mant.front();
    for (int64_t v : mant) {
      mn = std::min(mn, v);
      mx = std::max(mx, v);
    }
    const uint64_t range =
        static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn);
    int w = BitsNeeded(range);
    if (w == 0) {
      w = 1;
    }
    const size_t bytes =
        4 + (exc_pos.size() * 10) + 9 + ((n * static_cast<size_t>(w) + 7) / 8);
    if (bytes < best_bytes) {
      best_bytes = bytes;
      best_exp = exp;
      best_mant = mant;
      best_exc_pos = exc_pos;
      best_exc_val = exc_val;
    }
  }
  // Mantissa backend composition: the shared decimal transform feeds any
  // integer scheme. BitWeave is the general default; DICT wins on low-
  // cardinality prices, RLE on runs, CONST on constants. Pick by actual
  // encoded size over the same mantissa vector.
  std::vector<EncodedCol> backends;
  backends.push_back(EncBitWeaveI64(best_mant));
  {
    std::unordered_set<int64_t> distinct;
    for (int64_t v : best_mant) {
      distinct.insert(v);
    }
    if (distinct.size() <= 1024) {
      backends.push_back(EncDictI64(best_mant));
    }
  }
  backends.push_back(EncRleI64(best_mant));
  {
    bool all_same = true;
    for (int64_t v : best_mant) {
      if (v != best_mant.front()) {
        all_same = false;
        break;
      }
    }
    if (all_same) {
      backends.push_back(EncConstI64(best_mant.front()));
    }
  }
  const EncodedCol* inner = &backends.front();
  for (const auto& b : backends) {
    if (!b.buf.empty() && b.buf.size() < inner->buf.size()) {
      inner = &b;
    }
  }
  EncodedCol e;
  e.scheme = Scheme::kAlp;
  e.buf.push_back(static_cast<char>(best_exp));
  // Backend byte holds the inner scheme's ID (1 CONST / 2 RLE / 4 DICT /
  // 8 BitWeave); the inner region is that scheme's ordinary self-contained
  // region, so every reader dispatches through its existing decoders.
  e.buf.push_back(static_cast<char>(static_cast<uint8_t>(inner->scheme)));
  AppendBE16(e.buf, static_cast<uint16_t>(best_exc_pos.size()));
  for (size_t k = 0; k < best_exc_pos.size(); ++k) {
    AppendBE16(e.buf, static_cast<uint16_t>(best_exc_pos[k]));
    AppendBE64(e.buf, best_exc_val[k]);
  }
  e.param = 0;  // inner headers are authoritative (validator walks them)
  e.buf.insert(e.buf.end(), inner->buf.begin(), inner->buf.end());
  return e;
}

EncodedCol EncAlpRdTansF64(const std::vector<uint64_t>& bits) {
  if (bits.empty() || bits.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  struct CandidateBlock {
    std::vector<int64_t> values;
    std::vector<uint8_t> payload;
    uint8_t leaf_id = 0;
    uint32_t leaf_param = 0;
    uint8_t flags = 0;
    uint64_t first = 0;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    uint64_t sum = 0;
    std::vector<char> stream;
    uint32_t stream_bits = 0;
  };
  const uint32_t block_count =
      (static_cast<uint32_t>(bits.size()) + kAlpRdTansBlockRows - 1) /
      kAlpRdTansBlockRows;
  EncodedCol best;
  size_t best_size = SIZE_MAX;
  for (uint32_t right_width = 44; right_width <= 56; ++right_width) {
    std::unordered_map<uint32_t, uint32_t> frequencies;
    for (uint64_t raw : bits) {
      const auto front = static_cast<uint32_t>(raw >> right_width);
      if (front <= UINT16_MAX) {
        ++frequencies[front];
      }
    }
    if (frequencies.empty()) {
      continue;
    }
    std::vector<std::pair<uint32_t, uint32_t>> ranked;
    ranked.reserve(frequencies.size());
    for (const auto& entry : frequencies) {
      ranked.emplace_back(entry.first, entry.second);
    }
    std::sort(ranked.begin(), ranked.end(),
              [](const auto& left, const auto& right) {
                if (left.second != right.second) {
                  return left.second > right.second;
                }
                return left.first < right.first;
              });
    if (ranked.size() > 8) {
      ranked.resize(8);
    }
    std::sort(ranked.begin(), ranked.end(),
              [](const auto& left, const auto& right) {
                return left.first < right.first;
              });
    std::array<uint16_t, 8> fronts{};
    for (size_t i = 0; i < ranked.size(); ++i) {
      fronts[i] = static_cast<uint16_t>(ranked[i].first);
    }
    std::vector<CandidateBlock> candidates(block_count);
    bool feasible = true;
    for (uint32_t block = 0; block < block_count; ++block) {
      const size_t start = static_cast<size_t>(block) * kAlpRdTansBlockRows;
      const size_t end = std::min(bits.size(), start + kAlpRdTansBlockRows);
      const auto rows = static_cast<uint32_t>(end - start);
      std::vector<int64_t>& values = candidates[block].values;
      values.reserve(static_cast<size_t>(4) * rows);
      std::vector<int64_t> codes;
      std::vector<int64_t> tails;
      std::vector<int64_t> exceptions;
      std::vector<int64_t> exception_values;
      codes.reserve(rows);
      tails.reserve(rows);
      for (size_t row = start; row < end; ++row) {
        const uint64_t raw = bits[row];
        const auto front = static_cast<uint32_t>(raw >> right_width);
        const uint64_t tail = raw & ((UINT64_C(1) << right_width) - 1U);
        int rank = -1;
        for (size_t d = 0; d < ranked.size(); ++d) {
          if (fronts[d] == front) {
            rank = static_cast<int>(d);
            break;
          }
        }
        codes.push_back(rank < 0 ? 0 : rank);
        tails.push_back(static_cast<int64_t>(tail));
        if (rank < 0) {
          exceptions.push_back(static_cast<int64_t>(row - start));
          exception_values.push_back(static_cast<int64_t>(raw));
        }
      }
      values.insert(values.end(), codes.begin(), codes.end());
      values.insert(values.end(), tails.begin(), tails.end());
      values.insert(values.end(), exceptions.begin(), exceptions.end());
      values.insert(values.end(), exception_values.begin(),
                    exception_values.end());
      EncodedCol pfor = EncPforI64(values);
      EncodedCol bp128 = EncBp128I64(values);
      EncodedCol* leaf = nullptr;
      if (!pfor.buf.empty() &&
          (bp128.buf.empty() || pfor.buf.size() <= bp128.buf.size())) {
        leaf = &pfor;
      } else if (!bp128.buf.empty()) {
        leaf = &bp128;
      }
      if (leaf == nullptr) {
        feasible = false;
        break;
      }
      CandidateBlock& candidate = candidates[block];
      candidate.leaf_id = static_cast<uint8_t>(leaf->scheme);
      candidate.leaf_param = leaf->param;
      candidate.payload.resize(leaf->buf.size());
      if (!leaf->buf.empty()) {
        std::memcpy(candidate.payload.data(), leaf->buf.data(),
                    leaf->buf.size());
      }
      uint64_t first = 0;
      uint64_t minimum = 0;
      uint64_t maximum = 0;
      uint64_t sum = 0;
      bool numeric = false;
      if (!AlpRdTansComputeMetadata(bits.data() + start, rows, &first, &minimum,
                                    &maximum, &sum, &numeric)) {
        feasible = false;
        break;
      }
      candidate.flags = static_cast<uint8_t>(2U | (numeric ? 1U : 0U));
      candidate.first = first;
      candidate.minimum = minimum;
      candidate.maximum = maximum;
      candidate.sum = sum;
    }
    if (!feasible) {
      continue;
    }
    std::vector<std::vector<uint8_t>> payloads;
    payloads.reserve(candidates.size());
    for (const CandidateBlock& candidate : candidates) {
      payloads.push_back(candidate.payload);
    }
    AlpRdTansModel model;
    if (!AlpRdTansBuildModel(payloads, &model)) {
      continue;
    }
    for (CandidateBlock& candidate : candidates) {
      if (!AlpRdTansEncode(candidate.payload, model, &candidate.stream,
                           &candidate.stream_bits)) {
        feasible = false;
        break;
      }
    }
    if (!feasible) {
      continue;
    }
    std::vector<char> descriptors(
        static_cast<size_t>(block_count) * kAlpRdTansDescriptorBytes, '\0');
    std::vector<char> streams;
    uint32_t stream_offset = 0;
    auto put8 = [](char* p, uint8_t value) { *p = static_cast<char>(value); };
    auto put32 = [](char* p, uint32_t value) {
      const uint32_t be = htobe32(value);
      std::memcpy(p, &be, sizeof(be));
    };
    auto put64 = [](char* p, uint64_t value) {
      const uint64_t be = htobe64(value);
      std::memcpy(p, &be, sizeof(be));
    };
    for (uint32_t block = 0; block < block_count; ++block) {
      const CandidateBlock& candidate = candidates[block];
      char* descriptor = descriptors.data() + (static_cast<size_t>(block) *
                                               kAlpRdTansDescriptorBytes);
      const uint32_t rows = static_cast<uint32_t>(std::min<size_t>(
          kAlpRdTansBlockRows,
          bits.size() - (static_cast<size_t>(block) * kAlpRdTansBlockRows)));
      put8(descriptor, static_cast<uint8_t>(rows));
      put8(descriptor + 1, candidate.flags);
      put32(descriptor + 8, candidate.leaf_param);
      put32(descriptor + 12, static_cast<uint32_t>(candidate.values.size()));
      put8(descriptor + 4, candidate.leaf_id);
      put64(descriptor + 16, candidate.first);
      put64(descriptor + 24, candidate.minimum);
      put64(descriptor + 32, candidate.maximum);
      put64(descriptor + 40, candidate.sum);
      put32(descriptor + 48, stream_offset);
      put32(descriptor + 52, static_cast<uint32_t>(candidate.stream.size()));
      put32(descriptor + 56, candidate.stream_bits);
      if (candidate.stream.size() > UINT32_MAX - stream_offset) {
        feasible = false;
        break;
      }
      stream_offset += static_cast<uint32_t>(candidate.stream.size());
      streams.insert(streams.end(), candidate.stream.begin(),
                     candidate.stream.end());
    }
    if (!feasible) {
      continue;
    }
    EncodedCol encoded;
    encoded.scheme = Scheme::kAlpRdTans;
    encoded.param = block_count;
    encoded.buf.push_back(static_cast<char>(1));
    encoded.buf.push_back(static_cast<char>(0));
    AppendBE16(encoded.buf, static_cast<uint16_t>(block_count));
    encoded.buf.push_back(static_cast<char>(right_width));
    encoded.buf.push_back(static_cast<char>(ranked.size()));
    encoded.buf.push_back(static_cast<char>(8));
    encoded.buf.push_back(static_cast<char>(0));
    AppendBE16(encoded.buf, model.count);
    AppendBE16(encoded.buf, 0);
    for (size_t i = 0; i < ranked.size(); ++i) {
      AppendBE16(encoded.buf, fronts[i]);
    }
    for (uint32_t i = 0; i < model.count; ++i) {
      encoded.buf.push_back(static_cast<char>(model.symbols[i]));
      AppendBE16(encoded.buf, model.frequencies[i]);
    }
    encoded.buf.insert(encoded.buf.end(), descriptors.begin(),
                       descriptors.end());
    encoded.buf.insert(encoded.buf.end(), streams.begin(), streams.end());
    if (encoded.buf.size() >= best_size) {
      continue;
    }
    best_size = encoded.buf.size();
    best = std::move(encoded);
  }
  if (best.buf.empty()) {
    return {};
  }
  AlpRdTansRegion parsed;
  if (!ParseAlpRdTansRegion(best.buf.data(), static_cast<uint32_t>(bits.size()),
                            block_count, best.buf.data() + best.buf.size(),
                            &parsed) ||
      !AlpRdTansMetadataMatches(parsed)) {
    return {};
  }
  return best;
}

EncodedCol EncPatasF64(const std::vector<uint64_t>& bits) {
  if (bits.empty() || bits.size() > PaxChunk::kRowGroupSize) {
    return {};
  }
  struct CandidateBlock {
    std::vector<uint8_t> meta;
    std::vector<uint8_t> payload;
    uint8_t flags = 0;
    uint64_t first = 0;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    uint64_t sum = 0;
  };
  const auto n = static_cast<uint32_t>(bits.size());
  const uint32_t block_count = (n + kPatasBlockRows - 1U) / kPatasBlockRows;
  std::vector<CandidateBlock> candidates(block_count);
  for (uint32_t block = 0; block < block_count; ++block) {
    const size_t start = static_cast<size_t>(block) * kPatasBlockRows;
    const uint32_t rows = static_cast<uint32_t>(
        std::min<size_t>(kPatasBlockRows, bits.size() - start));
    CandidateBlock& candidate = candidates[block];
    candidate.meta.reserve(static_cast<size_t>(rows) * 4U);
    candidate.payload.reserve(static_cast<size_t>(rows) * 8U);
    PatasHistory history;
    history.Reset();
    uint64_t previous = bits[start];
    history.Push(previous);
    uint32_t window_length = 0;
    uint32_t window_tz = 0;
    for (uint32_t local = 1; local < rows; ++local) {
      const uint64_t current = bits[start + local];
      const uint64_t xor_previous = current ^ previous;
      if (xor_previous == 0) {
        candidate.meta.push_back(static_cast<uint8_t>(kPatasRepeat));
        previous = current;
        history.Push(current);
        continue;
      }
      uint32_t best_rank = 1;
      uint32_t best_length = PatasPayloadBytes(xor_previous);
      uint32_t best_tz = PatasTrailingZeros(xor_previous);
      uint32_t best_age = 0;
      uint32_t best_cost = 3U + best_length;
      if (window_length != 0 && best_length == window_length &&
          best_tz == window_tz) {
        best_rank = 0;
        best_cost = 1U + window_length;
      }
      const uint32_t oldest = std::min<uint32_t>(local, kPatasMaxAge);
      for (uint32_t age = 1; age <= oldest; ++age) {
        const uint32_t position = local - age;
        if (!history.Holds(position)) {
          continue;
        }
        const uint64_t x = current ^ history.At(position);
        const uint32_t length = PatasPayloadBytes(x);
        if (length == 0) {
          continue;
        }
        const uint32_t cost = 4U + length;
        if (cost >= best_cost) {
          continue;
        }
        best_cost = cost;
        best_rank = 2;
        best_length = length;
        best_tz = PatasTrailingZeros(x);
        best_age = age;
      }
      if (best_rank == 2) {
        candidate.meta.push_back(static_cast<uint8_t>(kPatasHist));
        candidate.meta.push_back(static_cast<uint8_t>(best_length - 1U));
        candidate.meta.push_back(static_cast<uint8_t>(best_tz));
        candidate.meta.push_back(static_cast<uint8_t>(best_age));
        window_length = best_length;
        window_tz = best_tz;
      } else {
        if (best_rank == 0) {
          candidate.meta.push_back(static_cast<uint8_t>(kPatasWindow));
        } else {
          candidate.meta.push_back(static_cast<uint8_t>(kPatasPrev));
          candidate.meta.push_back(static_cast<uint8_t>(best_length - 1U));
          candidate.meta.push_back(static_cast<uint8_t>(best_tz));
          window_length = best_length;
          window_tz = best_tz;
        }
      }
      const uint64_t x = best_rank == 2
                             ? (current ^ history.At(local - best_age))
                             : xor_previous;
      for (uint32_t i = 0; i < best_length; ++i) {
        const uint32_t shift = best_tz + (i * 8U);
        candidate.payload.push_back(static_cast<uint8_t>((x >> shift) & 0xFFU));
      }
      previous = current;
      history.Push(current);
    }
    uint64_t first = 0;
    uint64_t minimum = 0;
    uint64_t maximum = 0;
    uint64_t sum = 0;
    bool numeric = false;
    if (!PatasComputeMetadata(bits.data() + start, rows, &first, &minimum,
                              &maximum, &sum, &numeric)) {
      return {};
    }
    candidate.flags = static_cast<uint8_t>(2U | (numeric ? 1U : 0U));
    candidate.first = first;
    candidate.minimum = minimum;
    candidate.maximum = maximum;
    candidate.sum = sum;
  }
  uint64_t meta_total = 0;
  for (const CandidateBlock& candidate : candidates) {
    meta_total += candidate.meta.size();
  }
  uint64_t payload_total = 0;
  for (const CandidateBlock& candidate : candidates) {
    payload_total += candidate.payload.size();
  }
  const uint64_t region_size =
      static_cast<uint64_t>(kPatasHeaderBytes) +
      (static_cast<uint64_t>(block_count) * kPatasDescriptorBytes) +
      meta_total + payload_total;
  if (meta_total > UINT32_MAX || payload_total > UINT32_MAX ||
      region_size > UINT32_MAX) {
    return {};
  }
  EncodedCol encoded;
  encoded.scheme = Scheme::kPatas;
  encoded.param = block_count;
  encoded.buf.resize(static_cast<size_t>(region_size));
  auto put8 = [](char* p, uint8_t value) { *p = static_cast<char>(value); };
  auto put16 = [](char* p, uint16_t value) {
    const uint16_t be = htobe16(value);
    std::memcpy(p, &be, sizeof(be));
  };
  auto put32 = [](char* p, uint32_t value) {
    const uint32_t be = htobe32(value);
    std::memcpy(p, &be, sizeof(be));
  };
  auto put64 = [](char* p, uint64_t value) {
    const uint64_t be = htobe64(value);
    std::memcpy(p, &be, sizeof(be));
  };
  char* out = encoded.buf.data();
  put8(out, 0x50);
  put8(out + 1, 0x54);
  put8(out + 2, 1);
  put8(out + 3, static_cast<uint8_t>(kPatasBlockRows));
  put8(out + 4, 0);
  put16(out + 5, 0);
  put16(out + 7, static_cast<uint16_t>(block_count));
  put32(out + 9, 0);
  const uint64_t meta_base =
      static_cast<uint64_t>(kPatasHeaderBytes) +
      (static_cast<uint64_t>(block_count) * kPatasDescriptorBytes);
  uint64_t meta_cursor = meta_base;
  uint64_t payload_cursor = meta_base + meta_total;
  for (uint32_t block = 0; block < block_count; ++block) {
    const CandidateBlock& candidate = candidates[block];
    const uint32_t rows = static_cast<uint32_t>(std::min<size_t>(
        kPatasBlockRows,
        bits.size() - (static_cast<size_t>(block) * kPatasBlockRows)));
    char* descriptor = out + kPatasHeaderBytes +
                       (static_cast<size_t>(block) * kPatasDescriptorBytes);
    put8(descriptor, static_cast<uint8_t>(rows));
    put8(descriptor + 1, candidate.flags);
    put16(descriptor + 2, 0);
    put32(descriptor + 4, 0);
    put64(descriptor + 8, candidate.first);
    put64(descriptor + 16, candidate.minimum);
    put64(descriptor + 24, candidate.maximum);
    put64(descriptor + 32, candidate.sum);
    put32(descriptor + 40, static_cast<uint32_t>(meta_cursor));
    put32(descriptor + 44, static_cast<uint32_t>(candidate.meta.size()));
    put32(descriptor + 48, static_cast<uint32_t>(payload_cursor));
    put32(descriptor + 52, static_cast<uint32_t>(candidate.payload.size()));
    put64(descriptor + 56, 0);
    if (!candidate.meta.empty()) {
      std::memcpy(out + meta_cursor, candidate.meta.data(),
                  candidate.meta.size());
    }
    if (!candidate.payload.empty()) {
      std::memcpy(out + payload_cursor, candidate.payload.data(),
                  candidate.payload.size());
    }
    meta_cursor += candidate.meta.size();
    payload_cursor += candidate.payload.size();
  }
  if (encoded.buf.size() >= static_cast<size_t>(n) * 8U) {
    return {};
  }
  PatasRegion parsed;
  if (!ParsePatasRegion(encoded.buf.data(), n, block_count,
                        encoded.buf.data() + encoded.buf.size(), &parsed) ||
      !PatasMetadataMatches(parsed)) {
    return {};
  }
  return encoded;
}

// Fused single-pass row-group profiler. One row-major sweep over group_rows
// extracts everything the column encoders and every footer section need, so
// no later stage re-walks Row objects. Previously each of these was its own
// pass: null bitmaps, stats (count/sum/zone/bloom/freq), HLL registers,
// inverted/xor key sets, PGM keys, encoder vals/ubits, and the
// distinct/mono/all_same scans inside the adaptive selector.
//
// Semantics mirror the historical loops exactly, including two quirks that
// must be preserved bit-for-bit: (a) stats-domain values count non-int64
// columns as 0 (zone maps/blooms/sums over zeros), while the encode domain
// follows the cell-type switch; (b) index keys join NULL cells as raw 0,
// exactly what the decode path reports.

// Direct-mapped front cache for the per-column mode map (col_freq). The
// mode update runs once per cell of every row group, and the map's probe is
// the profile's most expensive memory op: an int64 hash degenerates to a
// prime-modulo DIV plus a heap-node chain walk per access. The front cache
// absorbs the hot-key repeats behind one load+compare; on a miss it flushes
// the evicted key's unsynced delta back into the map and services the new
// key through the exact map path (cap rule included), so every mode_seen
// value -- and therefore mode_val/mode_cnt -- is bit-identical to the
// uncached walk.

// Column-major cell scratch for one row group. The row-group loop writes
// deserialized cells straight into these arrays (one sequential walk of the
// sorted-run payload blob) so the profiler reads per-column streams instead
// of chasing group_rows[i]'s heap-allocated cell vector once per cell --
// the AoS read pattern was latency-bound on a dependent
// Row -> values_ pointer -> Value payload load chain on every cell.
// Only fixed-width schemas take this path: varchar cells need their owned
// string storage, and those builds keep the Row pool.

// Row-group cell sink: validates and decodes one SortedRun payload row into
// out's column arrays at row_idx. The validation walk mirrors
// Row::DeserializeBounded exactly (same guards, same throw strings) so a
// corrupted blob surfaces the same error from either decode path; the
// varchar branch is unreachable because builds only select this path for
// int64/double schemas.
void BuildCellsInto(const SortedRun::Entry& e, const std::vector<char>& blob,
                    const Schema& sc, const std::vector<ValueType>& col_types,
                    size_t row_idx, RgCells* out) {
  if (e.IsDeleted() || e.row_offset_ >= blob.size()) {
    return;
  }
  constexpr slot_t kRowNullMapFlagLocal = 0x8000;
  const char* src = blob.data() + e.row_offset_;
  const size_t max_len = blob.size() - e.row_offset_;
  if (max_len < sizeof(uint16_t)) {
    throw std::runtime_error("truncated row header");
  }
  uint16_t count_raw = 0;
  DeserializeSlot(src, &count_raw);
  const bool has_map = (count_raw & kRowNullMapFlagLocal) != 0;
  const auto count = static_cast<uint16_t>(count_raw & ~kRowNullMapFlagLocal);
  if (count != sc.ColumnCount()) {
    throw std::runtime_error("column count mismatch");
  }
  const char* cursor = src + sizeof(uint16_t);
  size_t avail = max_len - sizeof(uint16_t);
  const char* nullmap = cursor;
  if (has_map) {
    const size_t bmp_bytes = (static_cast<size_t>(count) + 7) / 8;
    if (avail < bmp_bytes) {
      throw std::runtime_error("truncated row nullmap");
    }
    cursor += bmp_bytes;
    avail -= bmp_bytes;
  }
  for (uint16_t c = 0; c < count; ++c) {
    if (has_map && (nullmap[c / 8] & static_cast<char>(1U << (c % 8))) != 0) {
      out->types[c][row_idx] = static_cast<uint8_t>(ValueType::kNull);
      out->payload[c][row_idx] = 0;
      continue;
    }
    if (avail < sizeof(int64_t)) {
      throw std::runtime_error("row exceeds available bytes");
    }
    out->types[c][row_idx] = static_cast<uint8_t>(col_types[c]);
    uint64_t bits = 0;
    DeserializeU64(cursor, &bits);
    out->payload[c][row_idx] = static_cast<int64_t>(bits);
    cursor += sizeof(int64_t);
    avail -= sizeof(int64_t);
  }
}

void ProfileRowGroup(std::span<const Row> group_rows, const Schema& sc,
                     std::vector<RgColumnProfile>& prof,
                     std::vector<FlatMapI64>& col_freq,
                     std::vector<int64_t>& col_mode_val,
                     std::vector<uint64_t>& col_mode_cnt,
                     std::vector<int64_t>& pgm_keys, bool& pgm_usable,
                     const RgCells* cells,
                     std::vector<std::vector<ModeFrontEntry>>& mode_front) {
  // The flag lives in a local for the sweep: through the reference the
  // compiler must reload it on every cell (the row loop stores through
  // potentially-aliasing vectors).
  bool pgm = pgm_usable;
  // The columnar path carries no Row span: the live row count travels in
  // the cell scratch (group_rows is empty there by design).
  const auto n =
      static_cast<uint32_t>(cells != nullptr ? cells->n : group_rows.size());
  const slot_t cc = sc.ColumnCount();
  // Reuse the caller's profile storage across row groups: assign() would
  // deep-copy a fresh RgColumnProfile (two flat hash tables plus four
  // vectors) for every group.
  if (prof.size() != static_cast<size_t>(cc)) {
    prof.clear();
    prof.resize(cc);
  } else {
    for (RgColumnProfile& P : prof) {
      P.Reset();
    }
  }
  const size_t bmp_bytes = (n + 7) / 8;
  for (slot_t c = 0; c < cc; ++c) {
    RgColumnProfile& P = prof[c];
    P.type = sc.GetColumn(c).Type();
    P.is_i64 = (P.type == ValueType::kInt64);
    P.is_double = (P.type == ValueType::kDouble);
    if (P.is_i64 || P.is_double) {
      // Sized up front so the sweep below can index instead of push_back:
      // every cell of the row-major loop writes vals[i]/ubits[i] exactly
      // once, and the capacity branch + finish-pointer churn of a reserved
      // push_back sat on ~2 stores per cell. The one-time zero-fill is
      // cheaper than the per-cell bookkeeping it replaces.
      P.vals.resize(n);
      P.ubits.resize(n);
    }
    P.null_bmp.assign(bmp_bytes, 0);
  }
  const slot_t kci = sc.KeyColumnIndex();
  // Column-major sweep. The historical walk was row-major (for each row, for
  // each column), which re-fetched &prof[c], its type flags and the column's
  // frequency map on every cell and kept ~15 profile accumulators live across
  // the widest branch fan-out the cell body has; the profile spent more cycles
  // on spill reloads than on its arithmetic. Walking one column at a time
  // keeps every accumulator in a register, hoists the column type out of the
  // cell loop entirely (int64 columns run a loop with no double/string
  // machinery at all), and changes nothing observable: every accumulator is
  // per-column and each column still sees its cells in row order.
  for (slot_t c = 0; c < cc; ++c) {
    RgColumnProfile& P = prof[c];
    const bool p_i64 = P.is_i64;
    const bool p_dbl = P.is_double;
    const bool is_key = (c == kci);
    bool do_pgm = pgm && is_key;
    // count == n for every walk above (each cell increments exactly once),
    // so the writeback stores n directly and no per-cell counter runs.
    uint64_t null_count = 0;
    int64_t sum = 0;
    int64_t mn = INT64_MAX, mx = INT64_MIN;
    int64_t emin = INT64_MAX, emax = INT64_MIN;
    int64_t cmin = INT64_MAX, cmax = INT64_MIN;
    uint64_t bloom = 0;
    bool mono = true;
    bool has_null = false;
    bool tc = true;
    uint64_t mode_cnt = col_mode_cnt[c];
    int64_t mode_val = col_mode_val[c];
    uint8_t* const bmp = P.null_bmp.data();
    int64_t* const vals_p = (p_i64 || p_dbl) ? P.vals.data() : nullptr;
    uint64_t* const ubits_p = (p_i64 || p_dbl) ? P.ubits.data() : nullptr;
    uint8_t* const hll = P.hll.data();
    auto& fm = col_freq[c];
    auto& xs = P.xorset;
    auto& ds = P.distinct;
    if (p_i64) {
      // One XorMix64 per cell feeds the bloom bit, the mode front index, the
      // HLL register AND both map probes (the prehashed upserts below take it
      // instead of recomputing the mix chain two and three times per cell).
      // XorMix64(0) is a compile-time-constant fold for the NULL-cell path.
      const uint64_t zero_mix = XorMix64(0ULL);
      // Hoisted past the loop: fm's stores cannot alias mode_front[c]'s data
      // pointer, but through the vector member the compiler had to reload it
      // on every cell.
      ModeFrontEntry* const front = mode_front[c].data();
      if (cells != nullptr) {
        // Columnar cells: payload words stream sequentially (hardware
        // prefetch-friendly) and the per-cell Row -> values_ pointer chase
        // drops out entirely. Accumulator updates are byte-for-byte the
        // generic loop's; only the cell fetch differs.
        const uint64_t* const pay = cells->payload[c].data();
        const uint8_t* const typ = cells->types[c].data();
        int64_t prev = 0;
        bool pgm_alive = do_pgm;
        for (uint32_t i = 0; i < n; ++i) {
          const uint8_t t = typ[i];
          if (t == static_cast<uint8_t>(ValueType::kNull)) {
            bmp[i >> 3] = static_cast<uint8_t>(bmp[i >> 3] | (0x80 >> (i & 7)));
            has_null = true;
            ++null_count;
            mono = mono && (i == 0 || 0 >= prev);
            prev = 0;
            emin = 0 < emin ? 0 : emin;
            emax = 0 > emax ? 0 : emax;
            vals_p[i] = 0;
            ubits_p[i] = 0;
            ++*xs.UpsertSlotPrehashed(0ULL, zero_mix);
            if (pgm_alive) {
              pgm = false;
              pgm_keys.clear();
              pgm_alive = false;
            }
            continue;
          }
          const bool cell_i64 = (t == static_cast<uint8_t>(ValueType::kInt64));
          const auto full = static_cast<int64_t>(pay[i]);
          if (!cell_i64) {
            // A double cell in an int64 column leaves the stats domain
            // through AsInt64's throw, exactly like the generic loop.
            Value::ThrowAsInt64();
          }
          mono = mono && (i == 0 || full >= prev);
          prev = full;
          emin = full < emin ? full : emin;
          emax = full > emax ? full : emax;
          vals_p[i] = full;
          ubits_p[i] = static_cast<uint64_t>(full);
          sum = static_cast<int64_t>(static_cast<uint64_t>(sum) +
                                     static_cast<uint64_t>(full));
          const int64_t smin = full < mn ? full : mn;
          const int64_t smax = full > mx ? full : mx;
          mn = smin;
          mx = smax;
          const uint64_t mixed = XorMix64(static_cast<uint64_t>(full));
          bloom |= 1ULL << (mixed & 63);
          ModeFrontEntry& fe =
              front[(mixed >> (64 - kModeFrontBits)) & (kModeFrontSize - 1)];
          uint64_t mode_seen = 0;
          if (fe.key == full && fe.count != 0) {
            // Hot-key repeat: the front cache is authoritative for this key.
            mode_seen = ++fe.count;
          } else {
            if (fe.count != fe.synced) {
              // Evict: fold the unsynced delta back into the map first.
              *fm.UpsertSlotPrehashed(static_cast<uint64_t>(fe.key), fe.hash) +=
                  fe.count - fe.synced;
            }
            if (fm.size() < kMaxModeKeys) {
              mode_seen =
                  ++*fm.UpsertSlotPrehashed(static_cast<uint64_t>(full), mixed);
              fe.key = full;
              fe.hash = mixed;
              fe.count = mode_seen;
              fe.synced = mode_seen;
              fe.in_fm = 1;
            } else {
              uint64_t* const hit =
                  fm.FindPrehashed(static_cast<uint64_t>(full), mixed);
              if (hit == nullptr) {
                mode_seen = 0U;
                fe.key = full;
                fe.hash = mixed;
                fe.count = 0;
                fe.synced = 0;
                fe.in_fm = 0;
              } else {
                mode_seen = ++*hit;
                fe.key = full;
                fe.hash = mixed;
                fe.count = mode_seen;
                fe.synced = mode_seen;
                fe.in_fm = 1;
              }
            }
          }
          if (mode_seen > mode_cnt) {
            mode_cnt = mode_seen;
            mode_val = full;
          }
          const auto idx = static_cast<uint32_t>(mixed >> 58);
          const uint8_t clz =
              ((mixed << 6) != 0U)
                  ? static_cast<uint8_t>(__builtin_clzll(mixed << 6) + 1)
                  : static_cast<uint8_t>(63);
          const uint8_t cur = hll[idx];
          hll[idx] = clz > cur ? clz : cur;
          ++*xs.UpsertSlotPrehashed(static_cast<uint64_t>(full), mixed);
        }
        // PGM keys are exactly the encode-domain values while the walk
        // survives: a NULL (or, in the row path, non-int64) cell kills the
        // index for the whole chunk, so surviving to here means every cell of
        // this group contributed vals_p[i] in order. The historical per-cell
        // push_back carried a capacity branch per cell; one append does it.
        if (pgm_alive) {
          pgm_keys.insert(pgm_keys.end(), vals_p, vals_p + n);
        }
      } else {
        // Previous cell's encode-domain value for the mono check: the loop
        // below just wrote vals_p[i-1], so a register copy replaces the
        // store-to-load round trip.
        int64_t prev = 0;
        bool pgm_alive = do_pgm;
        for (uint32_t i = 0; i < n; ++i) {
          const Value& v = group_rows[i][c];
          const bool is_null = (v.type == ValueType::kNull);
          if (is_null) {
            bmp[i >> 3] = static_cast<uint8_t>(bmp[i >> 3] | (0x80 >> (i & 7)));
            has_null = true;
            ++null_count;
            // Encode domain: null cells join as raw 0 (vals/ubits/emin/emax/
            // mono see 0 exactly like the historical cmov path).
            mono = mono && (i == 0 || 0 >= prev);
            prev = 0;
            emin = 0 < emin ? 0 : emin;
            emax = 0 > emax ? 0 : emax;
            vals_p[i] = 0;
            ubits_p[i] = 0;
            // Index domain: NULL joins key 0 like the decode path reports.
            ++*xs.UpsertSlotPrehashed(0ULL, zero_mix);
            if (pgm_alive) {
              pgm = false;
              pgm_keys.clear();
              pgm_alive = false;
            }
            continue;
          }
          const bool cell_i64 = (v.type == ValueType::kInt64);
          const int64_t full = cell_i64 ? v.AsInt64() : 0;
          mono = mono && (i == 0 || full >= prev);
          prev = full;
          emin = full < emin ? full : emin;
          emax = full > emax ? full : emax;
          vals_p[i] = full;
          ubits_p[i] = static_cast<uint64_t>(full);
          // Stats domain: a non-int64 cell in an int64 column still throws via
          // AsInt64 exactly like the historical expression.
          const int64_t sv = cell_i64 ? full : v.AsInt64();
          sum = static_cast<int64_t>(static_cast<uint64_t>(sum) +
                                     static_cast<uint64_t>(sv));
          const int64_t smin = sv < mn ? sv : mn;
          const int64_t smax = sv > mx ? sv : mx;
          mn = smin;
          mx = smax;
          // One shared mix feeds the bloom bit, the mode map and the HLL
          // register below (identical to the fused row-major form).
          const uint64_t mixed = XorMix64(static_cast<uint64_t>(sv));
          bloom |= 1ULL << (mixed & 63);
          ModeFrontEntry& fe =
              front[(mixed >> (64 - kModeFrontBits)) & (kModeFrontSize - 1)];
          uint64_t mode_seen = 0;
          if (fe.key == sv && fe.count != 0) {
            mode_seen = ++fe.count;
          } else {
            if (fe.count != fe.synced) {
              *fm.UpsertSlotPrehashed(static_cast<uint64_t>(fe.key), fe.hash) +=
                  fe.count - fe.synced;
            }
            if (fm.size() < kMaxModeKeys) {
              mode_seen =
                  ++*fm.UpsertSlotPrehashed(static_cast<uint64_t>(sv), mixed);
              fe.key = sv;
              fe.hash = mixed;
              fe.count = mode_seen;
              fe.synced = mode_seen;
              fe.in_fm = 1;
            } else {
              uint64_t* const hit =
                  fm.FindPrehashed(static_cast<uint64_t>(sv), mixed);
              if (hit == nullptr) {
                mode_seen = 0U;
                fe.key = sv;
                fe.hash = mixed;
                fe.count = 0;
                fe.synced = 0;
                fe.in_fm = 0;
              } else {
                mode_seen = ++*hit;
                fe.key = sv;
                fe.hash = mixed;
                fe.count = mode_seen;
                fe.synced = mode_seen;
                fe.in_fm = 1;
              }
            }
          }
          if (mode_seen > mode_cnt) {
            mode_cnt = mode_seen;
            mode_val = sv;
          }
          const auto idx = static_cast<uint32_t>(mixed >> 58);
          const uint8_t clz =
              ((mixed << 6) != 0U)
                  ? static_cast<uint8_t>(__builtin_clzll(mixed << 6) + 1)
                  : static_cast<uint8_t>(63);
          const uint8_t cur = hll[idx];
          hll[idx] = clz > cur ? clz : cur;
          ++*xs.UpsertSlotPrehashed(static_cast<uint64_t>(full), mixed);
          if (!cell_i64) {
            tc = false;
          }
        }
        if (pgm_alive) {
          pgm_keys.insert(pgm_keys.end(), vals_p, vals_p + n);
        }
      }
      // In every i64 loop above the stats bounds track exactly the same
      // min/max as mn/mx (NULL cells skip both), so the historical per-cell
      // cmin/cmax moves were redundant; one assignment each does it.
      cmin = mn;
      cmax = mx;
    } else {
      if (cells != nullptr) {
        // Columnar fixed-width cells. Encode domain: int64 cells join as
        // their int64 value and double cells as their bit pattern -- both
        // are the raw payload word, so no type dispatch feeds eraw.
        const uint64_t* const pay = cells->payload[c].data();
        const uint8_t* const typ = cells->types[c].data();
        int64_t prev = 0;
        for (uint32_t i = 0; i < n; ++i) {
          const uint8_t t = typ[i];
          const bool is_null = (t == static_cast<uint8_t>(ValueType::kNull));
          if (is_null) {
            bmp[i >> 3] = static_cast<uint8_t>(bmp[i >> 3] | (0x80 >> (i & 7)));
            has_null = true;
          }
          if (p_dbl) {
            const int64_t eraw = is_null ? 0 : static_cast<int64_t>(pay[i]);
            mono = mono && (i == 0 || eraw >= prev);
            prev = eraw;
            emin = eraw < emin ? eraw : emin;
            emax = eraw > emax ? eraw : emax;
            if (ds.Find(eraw) == nullptr) {
              ds.InsertNew(eraw, 0);
            }
            vals_p[i] = eraw;
            ubits_p[i] = static_cast<uint64_t>(eraw);
          }
          if (!is_null) {
            mn = 0 < mn ? 0 : mn;
            mx = 0 > mx ? 0 : mx;
            bloom |= 1ULL << (XorMix64(static_cast<uint64_t>(0)) & 63);
          }
          if (do_pgm) {
            if (t != static_cast<uint8_t>(ValueType::kInt64)) {
              pgm = false;
              pgm_keys.clear();
              do_pgm = false;
            } else {
              pgm_keys.push_back(static_cast<int64_t>(pay[i]));
            }
          }
          if (is_key && !is_null &&
              t != static_cast<uint8_t>(ValueType::kInt64)) {
            tc = false;
          }
        }
      } else {
        int64_t prev = 0;
        for (uint32_t i = 0; i < n; ++i) {
          const Value& v = group_rows[i][c];
          const bool is_null = (v.type == ValueType::kNull);
          if (is_null) {
            bmp[i >> 3] = static_cast<uint8_t>(bmp[i >> 3] | (0x80 >> (i & 7)));
            has_null = true;
          }
          if (p_dbl) {
            // Encode domain: int64 cells join as their int64 value, double
            // cells as their bit pattern, everything else (incl. NULL) as 0.
            int64_t eraw = 0;
            if (!is_null) {
              if (v.type == ValueType::kInt64) {
                eraw = v.AsInt64();
              } else if (v.type == ValueType::kDouble) {
                double d = v.AsDouble();
                memcpy(&eraw, &d, sizeof(double));
              }
            }
            mono = mono && (i == 0 || eraw >= prev);
            prev = eraw;
            emin = eraw < emin ? eraw : emin;
            emax = eraw > emax ? eraw : emax;
            if (ds.Find(eraw) == nullptr) {
              ds.InsertNew(eraw, 0);
            }
            vals_p[i] = eraw;
            ubits_p[i] = static_cast<uint64_t>(eraw);
          }
          // Stats domain: non-int64 columns accumulate the historical
          // zero-valued stats (sum stays 0, bounds fold toward 0) and still set
          // their bloom bit per non-null cell.
          if (!is_null) {
            mn = 0 < mn ? 0 : mn;
            mx = 0 > mx ? 0 : mx;
            bloom |= 1ULL << (XorMix64(static_cast<uint64_t>(0)) & 63);
          }
          if (do_pgm) {
            if (v.type != ValueType::kInt64) {
              pgm = false;
              pgm_keys.clear();
              do_pgm = false;
            } else {
              pgm_keys.push_back(v.AsInt64());
            }
          }
          if (is_key && !is_null && v.type != ValueType::kInt64) {
            tc = false;
          }
        }
      }
    }
    P.count = n;
    P.null_count = null_count;
    P.sum = sum;
    P.mn = mn;
    P.mx = mx;
    P.emin = emin;
    P.emax = emax;
    P.cmin = cmin;
    P.cmax = cmax;
    P.bloom = bloom;
    P.mono = mono;
    P.has_null = has_null;
    P.typed_clean = tc;
    col_mode_cnt[c] = mode_cnt;
    col_mode_val[c] = mode_val;
  }
  for (slot_t c = 0; c < cc; ++c) {
    RgColumnProfile& P = prof[c];
    // int64 columns carry their distinct set in `xorset` (identical key set,
    // see the fused loop above); only double columns populate `distinct`.
    if (!P.is_i64 || P.xorset.size() < 2 || P.xorset.size() > kMaxIdxDistinct) {
      continue;
    }
    // CSR build: offsets come straight from the exact per-key counts the
    // profile sweep left in `xorset`, so the row loop below only assigns row
    // ids through a <= 64-entry direct-mapped code table (one XorMix + one
    // probe, whole table L1-resident).
    auto& csr = P.postings;
    csr.rows.resize(n);
    csr.key_count = P.xorset.size();
    std::array<uint64_t, kMaxIdxDistinct> slot_cnt{};
    std::array<uint64_t, 4 * kMaxIdxDistinct> code_key{};
    std::array<uint32_t, 4 * kMaxIdxDistinct> code_slot{};
    std::array<uint8_t, 4 * kMaxIdxDistinct> code_used{};
    {
      constexpr size_t kMask = (4 * kMaxIdxDistinct) - 1;
      size_t slot = 0;
      P.xorset.ForEach([&](uint64_t key, uint64_t cnt) {
        size_t j = XorMix64(key) & kMask;
        while (code_used[j] != 0 && code_key[j] != key) {
          j = (j + 1) & kMask;
        }
        code_used[j] = 1;
        code_key[j] = key;
        code_slot[j] = static_cast<uint32_t>(slot);
        csr.keys[slot] = static_cast<int64_t>(key);
        slot_cnt[slot] = cnt;
        ++slot;
      });
      uint32_t acc = 0;
      for (size_t k = 0; k < slot; ++k) {
        csr.offsets[k] = acc;
        acc += static_cast<uint32_t>(slot_cnt[k]);
      }
      csr.offsets[slot] = acc;
    }
    {
      constexpr size_t kMask = (4 * kMaxIdxDistinct) - 1;
      std::array<uint32_t, kMaxIdxDistinct> fill{};
      uint16_t* const rows = csr.rows.data();
      const uint32_t* const offs = csr.offsets.data();
      // Columnar path: the key domain here is int64-or-null (a mistyped
      // cell already threw in the profile sweep above), so the raw payload
      // word IS the key and NULL joins key 0 exactly like the Row path.
      const uint64_t* const pay =
          cells != nullptr ? cells->payload[c].data() : nullptr;
      const uint8_t* const typ =
          cells != nullptr ? cells->types[c].data() : nullptr;
      for (uint32_t i = 0; i < n; ++i) {
        uint64_t key = 0;
        if (cells != nullptr) {
          if (typ[i] != static_cast<uint8_t>(ValueType::kNull)) {
            key = pay[i];
          }
        } else {
          const Value& v = group_rows[i][c];
          key = static_cast<uint64_t>(v.type == ValueType::kInt64 ? v.AsInt64()
                                                                  : 0);
        }
        size_t j = XorMix64(key) & kMask;
        while (code_used[j] == 0 || code_key[j] != key) {
          j = (j + 1) & kMask;
        }
        const uint32_t slot = code_slot[j];
        rows[offs[slot] + fill[slot]] = static_cast<uint16_t>(i);
        ++fill[slot];
      }
    }
  }
  pgm_usable = pgm;
}

// Adaptive encoder: exact-encodes each feasible candidate and keeps the
// smallest buffer (ties resolved by candidate priority order below). PLAIN
// is always the baseline fallback.
// Per-build environment knobs, read once per PaxChunk::BuildFromSortedRun
// call and threaded down into every row-group encode. The values are exactly
// what reading the variables inside would produce (same call stack), but
// five getenv probes per (row group, column) sat on the encode hot path --
// and setenv-before-build semantics are preserved because the read happens
// per build, never in a function-local static.
// Per-scheme linear decode cost, normalized to PLAIN = 1.0. The table
// is the encoder's cost model (see the comment block below for the
// measured basis); the fragment IR reads it through
// PaxChunk::SchemeDecodeCost so the estimate and the encoder never
// disagree about what a scheme costs to expand.
double SchemeDecodeCostInternal(PaxChunk::Scheme s) {
  switch (s) {
    // Measured (range_1000_dense scan t/s, normalized to PLAIN = 1.0):
    case PaxChunk::Scheme::kPlain:
      return 1.00;
    case PaxChunk::Scheme::kForBp:
      return 1.01;  // 765K t/s
    case PaxChunk::Scheme::kBitWeave:
      return 1.04;  // 745K t/s
    case PaxChunk::Scheme::kBp128:
      return 1.10;
    case PaxChunk::Scheme::kPfor:
    case PaxChunk::Scheme::kFastPfor:
    case PaxChunk::Scheme::kDeltaFastPfor:
    case PaxChunk::Scheme::kPBitS:
    case PaxChunk::Scheme::kSimple8b:
      return 1.20;  // narrow packed scan + tiny exception patch
    case PaxChunk::Scheme::kEliasFano:
      return 2.29;  // 338K t/s
    case PaxChunk::Scheme::kWavelet:
    case PaxChunk::Scheme::kWaveletR:
      return 2.24;  // 346K t/s
    // Estimated (not yet micro-benchmarked):
    case PaxChunk::Scheme::kConst:
      return 1.00;
    case PaxChunk::Scheme::kBitmap:
    case PaxChunk::Scheme::kWah:
      // WAH shares BITMAP's family decode shape (expand 63-row spans);
      // its popcount COUNT/SUM kernels are priced through the hint
      // discounts below, not the linear decode tier.
      return 1.50;
    case PaxChunk::Scheme::kDict:
    case PaxChunk::Scheme::kOpDict:
    case PaxChunk::Scheme::kTileDict:
    case PaxChunk::Scheme::kSortDict:
      // kOpDict shares kDict's byte layout; window predicates skip the
      // pass table, linear decode cost is identical (a strict '<' keeps
      // kDict on the tie in the unfiltered case). kTileDict is the same
      // shape per tile -- one bit extraction plus one dictionary load --
      // with a <= 16-tile walk on top of a full row group, so it prices
      // in the same dictionary tier; the window/bracket capability is
      // priced in the hint discount below. kSortDict's linear decode is
      // one sequential sorted walk plus a rank gather (random reads inside
      // an 8 KB cache-resident scratch) -- the same one-load-per-value
      // shape as a dictionary code expansion; its bracket/window
      // capability is priced in the hint discount below.
      return 1.30;
    case PaxChunk::Scheme::kRle:
    case PaxChunk::Scheme::kRleBp:
    case PaxChunk::Scheme::kOpRle:
    case PaxChunk::Scheme::kRleDict:
    case PaxChunk::Scheme::kRle8:
    case PaxChunk::Scheme::kRleDelta:
      // kRleDict has the same query shape as kRle (run walk), so the same
      // cost: within the run family the encoded SIZE decides (5-6B/run vs
      // 12B/run), and the hint discounts price the run-fold capability vs
      // DICT. Matching kRle keeps the matrix tests' pinned 2-run key
      // columns on kRle, where the dictionary overhead outweighs the
      // per-run savings.
      return 3.00;  // run walk, poor random access
    case PaxChunk::Scheme::kFreq:
    case PaxChunk::Scheme::kTopk:
      // Same exception-patch family (1.20): kFreq's linear decode is a
      // bitmap scan + rare inner lookups, kTopk's a nibble unpack +
      // dictionary load per row (two rows per byte, no bit-reader state),
      // and exceptions patch the tail through the inner region. Pricing the
      // family as one tier keeps the matrix tests' pinned schemes where they
      // are; the closed-form hot-window capability is priced in the hint
      // discounts below.
      return 1.20;
    case PaxChunk::Scheme::kRleX:
    case PaxChunk::Scheme::kRleIdx:
      // Sequential decode is a run walk (kRleX over inner-encoded streams,
      // kRleIdx over two byte-aligned FOR-BA streams), so both sit in the
      // kRle linear tier. What the index buys kRleIdx is random access,
      // priced by the hint multipliers below -- the same split
      // kBp128/kDeltaPfor make.
      return 3.00;
    case PaxChunk::Scheme::kStreamVByte:
      // Two byte-assembly loads per value (the 2-bit selector out of the
      // control byte, then the little-endian payload) plus a zigzag, and no
      // cross-row carry -- the same shape as the FOR-BA tier, a touch
      // cheaper because the widths are already byte multiples and there is
      // no bit-shuffling across a 64-bit word. Sits with kForBp (1.01) and
      // kBitWeave (1.04), a little above them for the per-value branch on
      // the width.
      return 1.12;
    case PaxChunk::Scheme::kDictX:
      // kDict (1.30) + one inner-region indirection per code; random
      // access stays O(1), but two-stage decode costs more than a flat
      // dictionary. Below kRle's 3.0 so it still wins when the inner
      // code stream compresses well, above it when kRle is smaller.
      return 1.60;
    case PaxChunk::Scheme::kHuffDict:
      // Canonical bit-walk decode with an 8-bit peek LUT: bench_huffdict
      // measures 4.8-6.4x PLAIN on full-region decode (the LUT resolves the
      // hot short codes in one lookup; the long tail still walks). Priced
      // between the checkpointed serial walks (kAnsFor 2.50) and the raw
      // cumulative tiers (kGorilla 6.00). Its hint discounts below price
      // the closed-form histogram capability, not this tier.
      return 5.00;
    case PaxChunk::Scheme::kPostings:
      // Rice-coded postings decode is one bit-walk plus one scatter per
      // row, inside a cache-resident region -- the dictionary tier's work
      // shape with a bit reader in place of the code extraction. Priced
      // between kDict (1.30) and the serial-walk schemes: the bench
      // measures the full decode at roughly half again PLAIN. The hint
      // discounts below price the closed-form histogram and O(matched)
      // mask capability, not this tier.
      return 1.60;
    case PaxChunk::Scheme::kArith:
    case PaxChunk::Scheme::kPla:
    case PaxChunk::Scheme::kMod:
    case PaxChunk::Scheme::kLane:
      // Same exception-bitmap family as kFreq (1.20): linear decode is
      // one FMA per row, exceptions patch the tail. Matching kFreq keeps
      // the matrix tests' pinned schemes where they are and prices the
      // closed-form window capability through the hint discounts below.
      // kPla shares kArith's sequential shape (one FMA per row over a
      // segment cursor plus the same exception patching); the size race
      // vs kArith is decided by the wire bytes alone (nseg == 1 never
      // reaches this code), so the two are priced as one tier. kMod's
      // walk is the same shape with one conditional subtract per row.
      // kLane's walk is the same shape with a per-lane cursor array (one
      // add per row plus a lane counter wrap).
      return 1.20;
    case PaxChunk::Scheme::kQpp:
    case PaxChunk::Scheme::kSparse:
      return 1.60;
    case PaxChunk::Scheme::kAnsFor:
      // Checkpoint jump (64 rows) + a serial rANS step per offset byte:
      // point access decodes up to 64*bpv symbols, batch decode is one
      // state update per byte. Priced like the other checkpointed
      // serial-walk schemes.
      return 2.50;
    case PaxChunk::Scheme::kDeltaFp:
      return 4.00;  // serial cumulative + unpack
    case PaxChunk::Scheme::kXorFor:
      return 6.00;  // serial cumulative XOR
    case PaxChunk::Scheme::kXorForC:
      // Checkpoint jump + <= 127 XOR steps: still a bit-serial unpack per
      // row but ~48x less cumulative work than the scheme-7 walk.
      return 2.60;
    case PaxChunk::Scheme::kGorillaC:
    case PaxChunk::Scheme::kChimpC:
    case PaxChunk::Scheme::kChimp128:
      // Checkpoint jump + <= 127 variable-length codes.
      return 3.00;
    case PaxChunk::Scheme::kDeltaFor:
    case PaxChunk::Scheme::kDeltaSimd:
      // Measured ~2.6x PLAIN t/s. kDeltaSimd is exactly kDeltaFor's probe:
      // one checkpoint load plus at most 128 inner decodes to replay the
      // delta prefix, and the inner decode is kSimdFor's (one aligned lane
      // load + one shift). The same 2.60 tier is the honest price -- the
      // inner packer is cheaper than FOR-BA's, but the 128-step replay
      // dominates and that is identical.
      return 2.60;
    case PaxChunk::Scheme::kDeltaPfor:
      return 3.00;
    case PaxChunk::Scheme::kS4:
      // A seek word plus at most kS4Block-1 (63) replayed deltas, each one
      // bit extraction and one add. That is the same shape of work as
      // kDeltaFor's 128-row replay, halved by the shorter block, so it
      // belongs in that tier rather than with the flat packers: an earlier
      // version of this scheme claimed the kSimdFor tier on the strength of
      // an O(1) probe that the delta wire format does not actually allow,
      // and the price was corrected when the reader was fixed.
      return 2.60;
    case PaxChunk::Scheme::kSimdFor:
      // Four values per aligned 8-byte lane load plus one shift each: the
      // cheapest per-value walk in the file, cheaper than the flat
      // bit-packers (which pay a byte-serial bit walk) and far cheaper than
      // anything cumulative. Any width 1..64 is expressible (a lane holds
      // floor(64/w) values), and the size side is where it earns its place:
      // no offset table, so at w = 1 it is 8x smaller than kForBp.
      return 1.30;
    case PaxChunk::Scheme::kSfor:  // same tier, one branch (branch-clone): the
                                   // two
    case PaxChunk::Scheme::kExpFor:  // group-FOR walks are identical work
      // The bulk decode is kBitWeave's unpack loop (one width extraction
      // and one add per row) plus a one-time O(ng) directory parse per
      // cell, so both sit in the packed-scan tier (kPfor/kPBitS/
      // kSimple8b), a hair above the flat bit-packers. bench_sfor: the
      // chunk read path (decoded-int cache + per-row arm) is at PLAIN
      // parity, and the cache-free unpack shares kBitWeave's inner loop
      // (1.04) with the parse on top. EXPFOR is the same walk one
      // transform deeper (a total-order XOR per row), so pricing the two
      // alike keeps them distinguishable by SIZE alone -- kSfor's split
      // search refines the grouping on monotone columns, EXPFOR's
      // structural split wins elsewhere -- and keeps every SFOR-pinned
      // test where it is.
      return 1.20;
    case PaxChunk::Scheme::kD2Pfor:
      // One more prefix sum per row than kDeltaFor (two instead of one) and
      // the same 128-row seek walk, so it sits just above that tier and
      // well above the flat bit-packers. The size side of the trade is what
      // this scheme exists for: a second-difference stream on a smooth
      // series is far narrower than the first-difference stream kDeltaFor
      // packs, and the encoded-bytes term in the cost model decides.
      return 2.80;  // estimated: kDeltaFor tier + one add per row
    case PaxChunk::Scheme::kDeltaBitweave:
      return 1.90;  // measured: ~1.9x PLAIN t/s
    case PaxChunk::Scheme::kByteSlice:
      return 1.60;  // per-value O(w) slice gather; byte-slice SUM/COUNT fast
    case PaxChunk::Scheme::kGorilla:
      return 6.00;  // serial XOR-delta (double codec)
    case PaxChunk::Scheme::kFsst:
      return 2.00;  // string decode over small symbol table
    case PaxChunk::Scheme::kDdRice:
      return 3.00;  // checkpoint jump + Rice unary walk
    case PaxChunk::Scheme::kChimp:
      return 6.00;  // serial XOR-delta (double codec)
    case PaxChunk::Scheme::kDeltaVarint:
    case PaxChunk::Scheme::kInterp:
      // Checkpoint jump + a <= 128-node bit-serial tree replay for point
      // access; batch decode walks every code once (measured between
      // DELTA-FP and ELIAS-FANO on ramps, bench_interp). Same
      // checkpointed-serial tier as kGorillaC/kChimpC/kDdRice.
      return 3.00;  // checkpointed serial walk
    case PaxChunk::Scheme::kAlp:
      return 1.20;  // BitWeave-packed mantissa + one multiply per value
    case PaxChunk::Scheme::kAlpRdTans:
      return 3.00;
    case PaxChunk::Scheme::kPatas:
      return 1.80;
    case PaxChunk::Scheme::kFrontCode:
    case PaxChunk::Scheme::kTemplate:
    case PaxChunk::Scheme::kDStream:
      return 1.50;  // varchar-only schemes: the numeric pipeline never
                    // emits them, the cases pin the exhaustive switch
  }
  return 16.0;
}

EncodedCol EncodeColumnAdaptive(slot_t col_idx, const Schema& sc,
                                std::span<const Row> rows,
                                const RgColumnProfile& prof,
                                const AdaptiveEnv& env, const RgCells* cells) {
  const ValueType vt = sc.GetColumn(col_idx).Type();
  // Env hook: opt out of schemes >= 8 (e.g. stale cross-runtime readers).
  // Read per build, NOT a function-local static: a static latches the first
  // call's answer for the whole process, so a setenv between chunk builds
  // silently did nothing (the Pfor regression test relies on toggling it).
  const bool kNoNewSchemes = env.no_new_schemes;
  if (env.force_plain) {
    // A/B baseline: dense PLAIN regions regardless of data shape. Sized once
    // and written through a raw pointer; the per-cell vector::insert calls
    // dominated this loop at 1024 rows per group.
    EncodedCol best;
    best.scheme = Scheme::kPlain;
    const bool str_col = (vt == ValueType::kVarChar || vt == ValueType::kNull);
    if (cells != nullptr) {
      // Columnar fixed-width cells: the same BE64 words the row walk writes
      // (payload word == AsInt64 for int64 cells and the bit pattern for
      // double cells; NULL cells store 0), streamed from the per-column
      // scratch. Only reachable for int64/double schemas.
      const size_t nrows = prof.vals.size();
      const uint64_t* const pay = cells->payload[col_idx].data();
      best.buf.resize(nrows * 8);
      char* wp = best.buf.data();
      for (size_t i = 0; i < nrows; ++i) {
        const uint64_t be = htobe64(pay[i]);
        std::memcpy(wp, &be, 8);
        wp += 8;
      }
      return best;
    }
    size_t total = 0;
    if (str_col) {
      for (const auto& rw : rows) {
        const Value& v = rw[col_idx];
        total +=
            4 + (v.type == ValueType::kVarChar ? v.AsStringView().size() : 0);
      }
    } else {
      total = rows.size() * 8;
    }
    best.buf.resize(total);
    char* wp = best.buf.data();
    for (const auto& rw : rows) {
      const Value& v = rw[col_idx];
      if (str_col) {
        std::string_view s = v.type == ValueType::kVarChar ? v.AsStringView()
                                                           : std::string_view{};
        const auto len = static_cast<uint32_t>(s.size());
        const uint32_t be = htobe32(len);
        std::memcpy(wp, &be, 4);
        wp += 4;
        // Skip the copy for a zero-length piece: an empty std::string_view
        // has a null data() pointer, and memcpy(dst, nullptr, 0) is undefined
        // even though it writes nothing. UBSan flags it whenever a chunk
        // carries an empty string (or a non-varchar cell, which contributes an
        // empty view here).
        if (len != 0) {
          std::memcpy(wp, s.data(), len);
        }
        wp += len;
      } else {
        uint64_t raw64 = 0;
        if (v.type == ValueType::kInt64) {
          raw64 = static_cast<uint64_t>(v.AsInt64());
        } else if (v.type == ValueType::kDouble) {
          double d = v.AsDouble();
          memcpy(&raw64, &d, sizeof(double));
        }
        const uint64_t be = htobe64(raw64);
        std::memcpy(wp, &be, 8);
        wp += 8;
      }
    }
    return best;
  }
  if (vt == ValueType::kVarChar || vt == ValueType::kNull) {
    // String pipeline: CONST / DICT-VARCHAR (cap 256 codes) / FSST /
    // FRONT-CODE, smallest buffer wins, PLAIN as the fallback.
    std::vector<std::string_view> sv;
    sv.reserve(rows.size());
    for (const auto& rw : rows) {
      const Value& v = rw[col_idx];
      sv.push_back(v.type == ValueType::kVarChar ? v.AsStringView()
                                                 : std::string_view{});
    }
    EncodedCol best;
    best.scheme = Scheme::kPlain;
    // One exact-size allocation, then pointer writes: the per-string
    // length-prefix + payload loop ran through vector::insert twice per row.
    {
      size_t total = 0;
      for (const auto& s : sv) {
        total += 4 + s.size();
      }
      best.buf.resize(total);
      char* wp = best.buf.data();
      for (const auto& s : sv) {
        const auto len = static_cast<uint32_t>(s.size());
        const uint32_t be = htobe32(len);
        std::memcpy(wp, &be, 4);
        wp += 4;
        // Skip the copy for a zero-length piece: an empty std::string_view
        // has a null data() pointer, and memcpy(dst, nullptr, 0) is undefined
        // even though it writes nothing. UBSan flags it whenever a chunk
        // carries an empty string (or a non-varchar cell, which contributes an
        // empty view here).
        if (len != 0) {
          std::memcpy(wp, s.data(), len);
        }
        wp += len;
      }
    }
    EncodedCol plain_cand = best;  // raw PLAIN, kept for FORCE_SCHEME
    bool all_same = !sv.empty();
    for (const auto& s : sv) {
      if (s != sv.front()) {
        all_same = false;
        break;
      }
    }
    std::vector<EncodedCol> candidates;  // priority order; strict '<' below
    candidates.reserve(8);
    if (all_same) {
      EncodedCol c;
      c.scheme = Scheme::kConst;
      AppendBE32(c.buf, static_cast<uint32_t>(sv.front().size()));
      c.buf.insert(c.buf.end(), sv.front().begin(), sv.front().end());
      candidates.push_back(std::move(c));
    } else {
      // Order-preserving dictionary first: byte-identical to kDict (same
      // header/code width/entry payload) but its sorted dictionary makes
      // every predicate -- equality AND range -- a code-window test, so at
      // an equal size it strictly dominates for native readers. opt-out
      // (kNoNewSchemes) keeps kDict, which stale cross-runtime readers know.
      if (!kNoNewSchemes) {
        candidates.push_back(EncOpDictStr(sv));
      }
      // Cascaded dictionary: inner-encoded code stream + flat or FSST pool
      // (BtrBlocks Dict+FSST). Wins whenever the codes or the string pool
      // compress below kDict's packed layout; the size check keeps it
      // honest when they do not.
      if (!kNoNewSchemes) {
        candidates.push_back(EncDictXStr(sv));
      }
      std::vector<std::string_view> dict;
      std::unordered_map<std::string_view, uint16_t> code;
      std::vector<uint64_t> codes;
      codes.reserve(sv.size());
      bool feasible = true;
      for (const auto& s : sv) {
        auto it = code.find(s);
        if (it == code.end()) {
          if (dict.size() >= 256) {
            feasible = false;
            break;
          }
          it = code.emplace(s, static_cast<uint16_t>(dict.size())).first;
          dict.push_back(s);
        }
        codes.push_back(it->second);
      }
      if (feasible) {
        const int cw = BitsNeeded(dict.size() - 1);
        EncodedCol d;
        d.scheme = Scheme::kDict;
        d.param = static_cast<uint32_t>(dict.size());
        AppendBE16(d.buf, static_cast<uint16_t>(dict.size()));
        AppendPackBits(d.buf, codes, cw);
        for (const auto& s : dict) {
          AppendBE32(d.buf, static_cast<uint32_t>(s.size()));
          d.buf.insert(d.buf.end(), s.begin(), s.end());
        }
        candidates.push_back(std::move(d));
      }
      // FSST: competitive when strings repeat long substrings (e.g. enum-like
      // or templated text). Costs a bit more to decode than DICT but often
      // wins on size for free-form text with shared suffixes/prefixes.
      if (!kNoNewSchemes) {
        candidates.push_back(EncFsstStr(sv));
        // Front coding (scheme 21): exploits shared prefixes + row order
        // (chunk rows arrive key-ordered). Wins on sorted sequences and
        // prefix-heavy strings where DICT hits its code cap and FSST's
        // substring mining has nothing to grip; degenerates to PLAIN's
        // exact size when nothing is shared, so the size comparison alone
        // keeps it honest. Sorted regions additionally answer predicate
        // counts by compressed-domain binary search (see
        // VarcharRegionMatchCount).
        candidates.push_back(EncFrontCodeStr(sv));
        // TEMPLATE (64): structured strings as a literal skeleton plus
        // fixed-width digit runs stored as int64 leaf regions -- the
        // full-string cardinality past every dictionary's reach whose rows
        // share a template. Its gates (the >= 97-distinct fence, the
        // fixed-width digit runs, the 3/4-conformity rule) keep the
        // dictionary family's home turf; the size race prices the rest.
        candidates.push_back(EncTemplateStr(sv));
        // DSTREAM (65): FSST's mined dictionary over length-delimited code
        // records with a 16-row restart table -- the leaner per-row index
        // (u8 clen + codes instead of a 4B offset table) wins wherever
        // symbol mining has no multi-byte grip (high-entropy short/mid
        // strings), and the memcmp EQ fold answers predicates without
        // materializing a conforming string. The >= 97-distinct fence keeps
        // the dictionary family's home turf; the size race prices the rest.
        candidates.push_back(EncDStreamStr(sv));
      }
    }
    for (const auto& cand : candidates) {
      if (cand.buf.empty()) {
        continue;
      }
      if (cand.buf.size() < best.buf.size()) {
        best = cand;
      }
    }
    // Test/benchmark hook, mirroring the numeric pipeline: force a specific
    // scheme regardless of size so every varchar decoder path stays
    // measurable even when economically dominated. A recognized-but-
    // infeasible force (CONST on non-constant data, DICT past 256 codes,
    // ...) warns once and keeps the adaptive choice instead of silently
    // benchmarking the wrong encoder.
    if (const char* fv = env.forced_scheme) {
      static const std::array<std::pair<const char*, Scheme>, 9> vmap = {{
          {"CONST", Scheme::kConst},
          {"DICT", Scheme::kDict},
          {"OPDICT", Scheme::kOpDict},
          {"FSST", Scheme::kFsst},
          {"FRONTCODE", Scheme::kFrontCode},
          {"PLAIN", Scheme::kPlain},
          {"DICTX", Scheme::kDictX},
          {"TEMPLATE", Scheme::kTemplate},
          {"DSTREAM", Scheme::kDStream},
      }};
      for (const auto& [name, want] : vmap) {
        if (std::strcmp(fv, name) != 0) {
          continue;
        }
        if (want == Scheme::kPlain) {
          return plain_cand;
        }
        for (const auto& cand : candidates) {
          if (cand.scheme == want && !cand.buf.empty()) {
            return cand;
          }
        }
        break;  // name recognized, but the scheme is infeasible for this column
      }
      static std::atomic<bool> vwarned{false};
      bool expected = false;
      if (vwarned.compare_exchange_strong(expected, true)) {
        LOG(WARN) << "TINYLAMB_FORCE_SCHEME=" << fv
                  << " not applicable to this varchar column; keeping the"
                     " adaptive choice";
      }
    }
    return best;
  }

  // Numeric pipeline (int64 native; double bitcast to u64). Values, shape
  // flags, and the exact distinct set arrive from the fused row-group
  // profiler; no Row re-walk happens here.
  const bool is_double = (vt == ValueType::kDouble);
  const std::vector<int64_t>& vals = prof.vals;
  const std::vector<uint64_t>& ubits = prof.ubits;

  std::vector<EncodedCol> candidates;
  const char* forced_scheme = env.forced_scheme;
  static const std::array<std::pair<const char*, Scheme>, 66> forced_map = {{
      {"CONST", Scheme::kConst},
      {"RLE", Scheme::kRle},
      {"FORBP", Scheme::kForBp},
      {"FORBA", Scheme::kForBp},
      {"DICT", Scheme::kDict},
      {"OPDICT", Scheme::kOpDict},
      {"DELTAFP", Scheme::kDeltaFp},
      {"BITMAP", Scheme::kBitmap},
      {"XORFOR", Scheme::kXorFor},
      {"PLAIN", Scheme::kPlain},
      {"BITWEAVE", Scheme::kBitWeave},
      {"ELIASFANO", Scheme::kEliasFano},
      {"WAVELET", Scheme::kWavelet},
      {"WAVELETR", Scheme::kWaveletR},
      {"BP128", Scheme::kBp128},
      {"DELTAFOR", Scheme::kDeltaFor},
      {"DELTABITWEAVE", Scheme::kDeltaBitweave},
      {"BYTESLICE", Scheme::kByteSlice},
      {"GORILLA", Scheme::kGorilla},
      {"FSST", Scheme::kFsst},
      {"PFOR", Scheme::kPfor},
      {"FASTPFOR", Scheme::kFastPfor},
      {"DELTAFASTPFOR", Scheme::kDeltaFastPfor},
      {"DDRICE", Scheme::kDdRice},
      {"CHIMP", Scheme::kChimp},
      {"DELTAVARINT", Scheme::kDeltaVarint},
      {"ALP", Scheme::kAlp},
      {"RLEDICT", Scheme::kRleDict},
      {"RLEBP", Scheme::kRleBp},
      {"FREQ", Scheme::kFreq},
      {"TOPK", Scheme::kTopk},
      {"RLEX", Scheme::kRleX},
      {"DICTX", Scheme::kDictX},
      {"ARITH", Scheme::kArith},
      {"QPP", Scheme::kQpp},
      {"SPARSE", Scheme::kSparse},
      {"ANSFOR", Scheme::kAnsFor},
      {"OPRLE", Scheme::kOpRle},
      {"XORFORC", Scheme::kXorForC},
      {"GORILLAC", Scheme::kGorillaC},
      {"CHIMPC", Scheme::kChimpC},
      {"PBITS", Scheme::kPBitS},
      {"INTERP", Scheme::kInterp},
      {"DELTAPFOR", Scheme::kDeltaPfor},
      {"RLE8", Scheme::kRle8},
      {"RLEDELTA", Scheme::kRleDelta},
      {"CHIMP128", Scheme::kChimp128},
      {"ALPRDTANS", Scheme::kAlpRdTans},
      {"PATAS", Scheme::kPatas},
      {"WAH", Scheme::kWah},
      {"PLA", Scheme::kPla},
      {"SIMPLE8B", Scheme::kSimple8b},
      {"RLEIDX", Scheme::kRleIdx},
      {"STREAMVBYTE", Scheme::kStreamVByte},
      {"HUFFDICT", Scheme::kHuffDict},
      {"D2PFOR", Scheme::kD2Pfor},
      {"SIMDFOR", Scheme::kSimdFor},
      {"DELTASIMD", Scheme::kDeltaSimd},
      {"S4", Scheme::kS4},
      {"SFOR", Scheme::kSfor},
      {"MOD", Scheme::kMod},
      {"TILEDICT", Scheme::kTileDict},
      {"EXPFOR", Scheme::kExpFor},
      {"SORTDICT", Scheme::kSortDict},
      {"LANES", Scheme::kLane},
      {"POSTINGS", Scheme::kPostings},
  }};
  Scheme forced_kind = Scheme::kPlain;
  bool forced_known = false;
  if (forced_scheme != nullptr) {
    for (const auto& [name, scheme] : forced_map) {
      if (std::strcmp(forced_scheme, name) == 0) {
        forced_kind = scheme;
        forced_known = true;
        break;
      }
    }
  }
  if (forced_scheme != nullptr) {
    candidates.reserve(48);
  }

  // PLAIN baseline: sized once and written through a raw pointer (one 8-byte
  // bswap store per value instead of AppendBE64's per-field bookkeeping).
  EncodedCol best;
  best.scheme = Scheme::kPlain;
  best.buf.resize(ubits.size() * 8);
  {
    char* wp = best.buf.data();
    for (uint64_t raw64 : ubits) {
      const uint64_t be = htobe64(raw64);
      std::memcpy(wp, &be, sizeof(be));
      wp += sizeof(be);
    }
  }
  // Priority order below => strict '<' keeps the earliest winner on ties.
  // Cost-model selection: minimize (encoded bytes) * decode_cost, where
  // decode_cost is DERIVED FROM MEASURED scan throughput (bench_new_schemes,
  // range_1000_dense, tuples/s normalized to PLAIN = 1.0) rather than
  // hand-tuned guesses. A per-column AccessHint then biases the choice toward
  // the encoding family that pays off under the expected query pattern: a
  // filter target rewards pushdown-capable schemes (EliasFano/Wavelet) since
  // they can answer predicates without materializing every value; an
  // aggregate/scanned column rewards fast linear decode (FOR-BA/BitWeave); a
  // sort key rewards order-preserving schemes (EliasFano/Delta-*). This makes
  // the selector evaluate compositions and price *capability*, not just raw
  // size.
  const AccessHint hint = sc.GetColumn(col_idx).GetHint();
  // Total-byte accounting for the footer inverted index (v13): the chunk
  // build publishes a raw postings cell for every int64 column with 2..64
  // distinct values (kMaxIdxDistinct, the >64 abort sets postings_big)
  // unless the codec IS the inverted dictionary -- kPostings carries those
  // posting lists compressed behind the exact count table, and the build
  // skips the footer copy for its cells (the Inverted* footer APIs fall
  // back to the scan kernels, which hit the closed forms on these cells).
  // The cost model prices CHUNK bytes, so every candidate pays the footer
  // cell its region would carry and kPostings pays none -- that delta is
  // the scheme's reason to exist, and ignoring it would let the codec race
  // double-count storage every d <= 64 int64 column pays today. The
  // estimate mirrors the serializer (12 B cell header + 9 B per posting
  // entry + the row-id container at its array-or-bitmap bound; the runs
  // container can only be smaller, which errs in the index's favor).
  // The charge applies on FILTER and AGGREGATE columns only: those are the
  // workloads the footer index exists to serve (EQ/IN/range counts), so
  // replacing the raw copy with the codec is a like-for-like trade there.
  // On kUnknown/kSortKey/kPointLookup columns the index keeps serving under
  // whichever codec won the byte race, and the codec race alone decides
  // (the scheme stays out of their way).
  const bool kNoInvIdx = env.no_inv_idx;
  const bool inv_would_build =
      (hint == AccessHint::kFilter || hint == AccessHint::kAggregate) &&
      !kNoInvIdx && prof.is_i64 && !prof.postings_big &&
      prof.postings.key_count >= 2 &&
      prof.postings.key_count <= kMaxIdxDistinct;
  uint64_t inv_bytes = 0;
  if (inv_would_build) {
    inv_bytes = 12;
    for (size_t k = 0; k < prof.postings.key_count; ++k) {
      const uint64_t arr =
          2 + ((prof.postings.offsets[k + 1] - prof.postings.offsets[k]) * 2);
      inv_bytes += 9 + std::min<uint64_t>(arr, 130);
    }
  }
  auto decode_cost = [](Scheme s) -> double {
    return SchemeDecodeCostInternal(s);
  };
  auto hint_mult = [](AccessHint h, Scheme s) -> double {
    switch (h) {
      case AccessHint::kFilter:
        // pushdown-capable: can answer range/count without full materialization
        if (s == Scheme::kEliasFano || s == Scheme::kWavelet) {
          return 0.35;
        }
        // kOpDict answers EQ/range counts as a code-window scan -- no
        // dictionary evaluation, no pass table, no decode. Same bytes as
        // kDict, so this multiplier alone decides their tie on filter
        // columns.
        if (s == Scheme::kOpDict) {
          return 0.60;
        }
        // kTileDict folds the predicate per tile through the ascending
        // tile dictionary: wholly-inside tiles become one range set and
        // wholly-outside tiles cost nothing (zero code reads), so a
        // tile-locality column answers from the straddling tiles alone.
        // Same window tier as kOpDict -- the pruning granularity is a tile
        // (64 rows) rather than the whole region, which the locality gate
        // (the encoder declines tiles over 64 distinct) keeps honest.
        if (s == Scheme::kTileDict) {
          return 0.60;
        }
        // kSortDict resolves ANY predicate to one sorted window by bracket
        // bisect (the block firsts are an ascending bracket table): a window
        // that misses or covers the region answers in O(log nb) with zero
        // row reads, and a partial window falls to the caller's generic
        // decode -- which bench_sortdict measures at PLAIN parity (the AVX2
        // kernels), while the flat packers that own these columns pay their
        // own decode first (PFOR ~1.4x PLAIN) or a bit-sliced walk
        // (PBitS 60x PLAIN on wide domains). The discount is the
        // kRleIdx indexed-fold tier: the window resolution is O(log nb)
        // and the materialization never costs more than a decode.
        if (s == Scheme::kSortDict) {
          return 0.45;
        }
        // kPostings folds ANY predicate over the exact count table (EQ one
        // bisect, ranges the passing lists) with zero row reads and zero
        // bitstream reads -- the kHuffDict closed form without the entropy
        // tier's decode price -- and a SELECTIVE predicate materializes its
        // mask at O(matched) straight off the bitoffs jump table. Priced at
        // the kSortDict/kRleIdx indexed-fold tier: the fold is O(log d +
        // window) and the mask kernel declines past rc/8 matched rows (the
        // measured crossover where the AVX2 decode kernels win).
        if (s == Scheme::kPostings) {
          return 0.45;
        }
        // kRleDict folds the predicate per RUN once the dictionary pass
        // table is built (O(runs) per query) -- worth a discount on filter
        // columns, like kOpDict's window but weaker (pass table per query).
        if (s == Scheme::kRleDict) {
          return 0.70;
        }
        if (s == Scheme::kRleBp) {
          return 0.65;
        }
        if (s == Scheme::kRle8) {
          return 0.55;
        }
        if (s == Scheme::kRleDelta) {
          return 0.60;
        }
        // kRleIdx bisects the sorted run values into ONE window and emits it
        // as a single range set, so out-of-window runs are never decoded --
        // the strongest run-family pushdown, and the only one that is
        // O(log runs + window) instead of O(runs).
        if (s == Scheme::kRleIdx) {
          return 0.45;
        }
        // kOpRle folds the predicate per RUN as a code window over the
        // sorted dict: two binary searches + O(runs), no pass table at all.
        if (s == Scheme::kOpRle) {
          return 0.55;
        }
        // kHuffDict answers EQ as one binary search + one frequency read and
        // ANY range as a closed-form histogram fold over the d symbols with
        // zero bitstream reads: bench_huffdict measures the range fold at
        // 8.1us vs 145-674us for the dict/run family (40-80x). The discount
        // prices that fold at the kWavelet pushdown tier, discounted for
        // EQ-heavy mixes where every dictionary scheme is already ~1us -- so
        // it only wins when the entropy bitstream is also competitive in
        // bytes (multi-modal skew), never on uniform columns.
        if (s == Scheme::kHuffDict) {
          return 0.16;
        }
        // kFreq answers EQ on the dominant value as a bitmap popcount,
        // but range predicates still walk the full bitmap plus the
        // exception pass flags -- weaker pushdown than OP-DICT's pure
        // code-window scan, so a smaller discount.
        if (s == Scheme::kFreq) {
          return 0.75;
        }
        // kTopk folds ANY predicate over the hot head as a 15-entry code
        // window with zero nibble reads (the exact inline frequency table)
        // plus one pass over the nexc tail: the 2/3 head gate bounds the
        // tail at a third of the block, so the fold is at worst O(15 + n/3)
        // against kFreq's full-column bitmap walk and at the typical >= 90%
        // coverage O(15 + 0.1n). Priced at kOpDict's window tier (0.60),
        // not kFreq's 0.75 (whose single top rarely covers this much) and
        // not kHuffDict's 0.16 (whose histogram covers ALL rows, no tail).
        if (s == Scheme::kTopk) {
          return 0.60;
        }
        // WAH builds the selection mask by copying literal words verbatim
        // and answering fills as range sets -- no value decode at all --
        // and folds COUNT/SUM as popcounts over the same words. Strongest
        // pushdown shape of the two-value family, but gated to exactly-2-
        // distinct columns like kBitmap/kConst.
        if (s == Scheme::kWah) {
          return 0.55;
        }
        if (s == Scheme::kPBitS) {
          return 0.50;
        }
        if (s == Scheme::kBp128) {
          return 0.65;
        }
        // kFastPfor HAS the kBp128 bracket kernel (block min/max prune,
        // stored sums, boundary decode), but on its own niche data the
        // brackets rarely prune: a block carrying even one spike spans the
        // full spike range, so most blocks of a spiky column are boundary
        // blocks. bench_fastpfor measures the mask path at 1.2-6x the fused
        // AVX2 generic decode there, so unlike kBp128 this scheme takes NO
        // filter discount -- it competes on wire bytes alone. (A
        // spike-free-block mixed column would prune like kBp128; the hint
        // prices the niche shape.)
        // kDeltaFastPfor (67) shares that bracket kernel AND its niche: the
        // same delta-spike column keeps every spiked block's stored bracket
        // wide, and its boundary decode adds a <= 127-step replay on top of
        // FastPFOR's lane decode. No discount either.
        if (s == Scheme::kDeltaFastPfor) {
          return 1.00;
        }
        if (s == Scheme::kChimp128) {
          return 0.35;
        }
        if (s == Scheme::kAlpRdTans) {
          return 0.03;
        }
        if (s == Scheme::kPatas) {
          return 0.50;
        }
        // kArith answers ANY range predicate as a closed-form index window
        // (one SvSetRange + <= 65536 exception patches) and EQ as a single
        // candidate index -- the strongest pushdown shape, but only on
        // arithmetic-progression columns (narrow niche, see EncArithI64).
        // Same discount tier as kFreq: strong but structure-bound.
        if (s == Scheme::kArith) {
          return 0.75;
        }
        // kLane generalizes that closed form to one window PER LANE: an
        // interleaved-ramp column answers a range predicate from O(nd)
        // strided window sets + exception patches with no row decode, and
        // folds to one contiguous range set whenever every lane's window
        // covers its whole lane. Same family, same structure-bound caveat
        // (only fires on interleaved equal-step grids) and the same tier
        // as kArith/kPla -- the dense-partial decline (windows touching
        // more than a quarter of the rows fall back to the AVX2 decode
        // kernels) is priced like kSortDict's window decline.
        if (s == Scheme::kLane) {
          return 0.75;
        }
        // kPla generalizes that closed form to one window PER SEGMENT: a
        // multi-rate column answers a range predicate from O(nseg) range
        // sets + exception patches with no row decode. Same family, same
        // structure-bound caveat (only fires on equal-step runs) and the
        // same tier as kArith -- matching tiers keep the sawtooth pins
        // (one wrap per row group) on kArith, whose single window does the
        // same job for less, while genuine rate switches have no kArith
        // candidate to lose to.
        if (s == Scheme::kPla) {
          return 0.75;
        }
        if (s == Scheme::kQpp) {
          return 0.70;
        }
        // kInterp prunes whole blocks on the checkpoint brackets and
        // answers fully-inside blocks without reading a bit, so a clustered
        // monotone column answers a range predicate from the boundary
        // blocks only. Weaker than kArith's closed form (boundary blocks
        // still walk), stronger than a flat decode.
        if (s == Scheme::kInterp) {
          return 0.70;
        }
        // kSfor answers ANY range predicate from the directory alone: the
        // brackets are disjoint and ascending, so the window's groups fold
        // to ONE SvSetRange and the (at most two) straddling groups resolve
        // by two in-group bisects -- O(log ng + log rows) unpacks, no scan.
        // bench_sfor: the 5%-window range COUNT is the fastest of every
        // scheme on every sorted shape measured (16.3-17.4us vs 24-30us for
        // the sequential-decode double codecs and 17.7-19.0us for PLAIN).
        // Structure-bound like kArith (fires only on monotone non-negative
        // doubles), priced at the kOpDict/kTopk window tier.
        if (s == Scheme::kSfor) {
          return 0.60;
        }
        // kExpFor folds ANY range predicate over the magnitude-band brackets:
        // whole runs collapse to one range set or zero reads, and only the
        // straddling runs walk -- at w bits per row instead of 8B values.
        // Priced at the kInterp tier (0.70), NOT kSfor/kTileDict's 0.60: the
        // fold's run scan is linear and its straddle walk has no in-group
        // bisect (rows are unordered), while kSfor bisects its boundary
        // groups and kTileDict folds per tile -- and the tighter discount
        // keeps PATAS's pinned oscillating-history shapes (its own
        // block-bracket kernels, bench-calibrated) where they were.
        if (s == Scheme::kExpFor) {
          return 0.70;
        }
        // kSimple8b skips whole words on the selector-implied value range
        // and folds zero-width words as range sets -- pruning granularity
        // is a word (<= 240 rows), so whole-block bracket schemes beat it
        // when they apply, but it beats a flat decode on mixed widths.
        if (s == Scheme::kSimple8b) {
          return 0.70;
        }
        if (s == Scheme::kDeltaPfor) {
          return 0.55;
        }
        // kSparse evaluates a predicate over the nnz live rows only
        // (O(nnz) mask build); unlisted rows share the match(0) verdict.
        if (s == Scheme::kSparse) {
          return 0.85;
        }
        return 1.0;
      case AccessHint::kAggregate:
        if (s == Scheme::kForBp || s == Scheme::kBitWeave ||
            s == Scheme::kPlain || s == Scheme::kAlp) {
          return 0.80;
        }
        if (s == Scheme::kXorFor || s == Scheme::kDeltaFp) {
          return 1.20;
        }
        // Run-structured SUM folds as sum(dict[c] * len) over runs.
        if (s == Scheme::kRleDict || s == Scheme::kOpRle) {
          return 0.90;
        }
        if (s == Scheme::kRleBp) {
          return 0.90;
        }
        if (s == Scheme::kRle8) {
          return 0.85;
        }
        if (s == Scheme::kRleDelta) {
          return 4.00;
        }
        // A filtered SUM over a sorted run column folds sum(v*len) over the
        // predicate's run window only; an unpredicated SUM folds all runs.
        // Both are O(runs in window) / O(runs) with no row materialized, so
        // this sits with kRleDict/kOpRle rather than the closed-form tier
        // (kArith/kPla/WAH) that skips the walk entirely.
        if (s == Scheme::kRleIdx) {
          return 0.80;
        }
        if (s == Scheme::kChimp128) {
          return 0.40;
        }
        if (s == Scheme::kAlpRdTans) {
          return 0.03;
        }
        if (s == Scheme::kPatas) {
          return 0.50;
        }
        if (s == Scheme::kPBitS) {
          return 0.85;
        }
        if (s == Scheme::kBp128) {
          return 0.85;
        }
        // kFastPfor stores kBp128's exact per-block sum word, so a
        // fully-selected block folds without reading a lane. bench_fastpfor
        // measures the unpredicated SUM at PLAIN's fused-AVX2 tier (10.2us
        // vs PLAIN 9.2 / kBp128 4.5 / kPfor 55) -- the metadata works, the
        // per-block parse on top costs what kBp128's fixed stride doesn't.
        if (s == Scheme::kFastPfor) {
          return 0.80;
        }
        // kDeltaFastPfor stores the same per-block sum word (over its true
        // values, brackets included), so a fully-selected block folds
        // without replaying its delta chain. bench_deltafastpfor measures
        // the unpredicated SUM at 14.8us against PLAIN's 8.5 -- the zero
        // reads work, the wider 51B/block metadata walk on top of the
        // query costs what FastPFOR's 35B walk doesn't -- so a step less
        // generous than kFastPfor's 0.80: kBp128/kPBitS's 0.85.
        if (s == Scheme::kDeltaFastPfor) {
          return 0.85;
        }
        // kArith folds an unpredicated SUM in closed form (n*a + d*n(n-1)/2
        // minus exception patches) -- aggregate columns running it pay
        // O(nexc), not O(n).
        if (s == Scheme::kArith) {
          return 0.80;
        }
        // kLane folds it as the sum of K closed-form lane sums
        // (cnt*base + step*cnt(cnt-1)/2 per lane) minus exception patches
        // -- O(nd + nexc), the kArith closed-form tier.
        if (s == Scheme::kLane) {
          return 0.80;
        }
        // kPla folds it per segment: sum_j len*first + d*len*(len-1)/2 minus
        // exception patches -- O(nseg + nexc), same closed-form tier.
        if (s == Scheme::kPla) {
          return 0.80;
        }
        // kHuffDict folds an unpredicated SUM as sum(dict[s]*freq[s]) over
        // the d symbols -- the closed-form tier, but structure-free (any row
        // order, d <= 1024) and with zero bitstream reads, so it prices with
        // the strongest metadata-grade discounts (kOpDict's 0.60 tier).
        if (s == Scheme::kHuffDict) {
          return 0.60;
        }
        // kTopk folds an unpredicated SUM as sum(hot[c]*freq[c]) over the
        // 15 hot entries plus one inner pass over the nexc tail -- the same
        // closed-form tier, priced with kHuffDict (the tail is the only
        // walk and the head gate keeps it short).
        if (s == Scheme::kTopk) {
          return 0.60;
        }
        // kSortDict folds an unpredicated SUM as the psum table's LAST word
        // -- one load, zero reads of value bytes, the strongest closed-form
        // shape in the file (the rank bijection makes the row-order sum
        // equal the sorted sum). Same tier as kHuffDict/kTopk.
        if (s == Scheme::kSortDict) {
          return 0.60;
        }
        // kPostings folds SUM (predicated or not) as sum(dict[c]*count[c])
        // over the lists the predicate passes -- the kHuffDict histogram
        // fold with the count table in place of the frequency table: O(d)
        // reads of two small tables, zero row bytes, any row order.
        if (s == Scheme::kPostings) {
          return 0.60;
        }
        // WAH folds an unpredicated SUM in closed form too: one popcount
        // walk gives the true/false row split, SUM = tv*ones + fv*(n-ones).
        if (s == Scheme::kWah) {
          return 0.80;
        }
        if (s == Scheme::kQpp) {
          return 0.80;
        }
        // kSparse sums only the nnz live values (unlisted rows contribute
        // 0) -- on a mostly-null column the SUM is O(nnz), not O(n).
        if (s == Scheme::kSparse) {
          return 0.70;
        }
        if (s == Scheme::kDeltaPfor) {
          return 0.40;
        }
        // kAnsFor sums through the serial rANS chain (no closed form);
        // same penalty tier as the other cumulative codecs.
        if (s == Scheme::kAnsFor) {
          return 1.20;
        }
        return 1.0;
      case AccessHint::kPointLookup:
        // kStreamVByte is the one O(1) probe in the packable-width tier: the
        // payload offset is one control-byte load plus three width adds, so a
        // probe never replays from the region head. Priced with the other
        // genuinely indexed probes rather than the 1.0 default, but above
        // kForBp's implicit 1.0 because each probe still assembles a
        // variable-width little-endian word instead of a fixed one.
        if (s == Scheme::kStreamVByte) {
          return 0.85;
        }
        // kSimple8b has no random-access directory: a probe replays words
        // from the region head, the same O(param) walk as the run family.
        if (s == Scheme::kSimple8b) {
          return 1.50;
        }
        if (s == Scheme::kRle) {
          return 1.50;  // poor random access
        }
        if (s == Scheme::kRle8) {
          return 1.20;
        }
        if (s == Scheme::kRleDelta) {
          return 1.35;
        }
        if (s == Scheme::kRleDict) {
          return 1.50;
        }
        if (s == Scheme::kOpRle) {
          return 1.50;
        }
        if (s == Scheme::kRleX) {
          return 1.50;
        }
        // The restart table is the one random-access directory in the run
        // family: a probe is a binary search over nidx entries plus a <= 15-run
        // walk, i.e. O(1) in the row count where every arm above is O(runs).
        // Priced next to the other indexed probes (kDeltaPfor 0.90 / kBp128
        // 1.10); the parse is O(nidx), not O(1) in the strict sense, hence
        // slightly above kDeltaPfor's block bracket.
        if (s == Scheme::kRleIdx) {
          return 0.95;
        }
        // kAnsFor jumps to a 64-row checkpoint then walks the rANS chain
        // byte-at-a-time -- bounded but still serial.
        if (s == Scheme::kAnsFor) {
          return 1.30;
        }
        // kHuffDict jumps to a 64-row checkpoint then replays at most 63
        // canonical codes; same bounded-serial tier as kAnsFor.
        if (s == Scheme::kHuffDict) {
          return 1.30;
        }
        if (s == Scheme::kBp128) {
          return 1.10;
        }
        if (s == Scheme::kChimp128) {
          return 1.05;
        }
        if (s == Scheme::kAlpRdTans) {
          return 6.00;
        }
        if (s == Scheme::kPatas) {
          return 1.30;
        }
        // kInterp replays a <= 128-node block tree per point probe.
        if (s == Scheme::kInterp) {
          return 1.30;
        }
        // kSortDict extracts one rank then replays at most 63 gaps from the
        // block's first -- the kAnsFor/kHuffDict bounded-serial tier.
        if (s == Scheme::kSortDict) {
          return 1.30;
        }
        // kSfor bisects the row_start table (O(log ng)) and unpacks one
        // width; the parse is O(ng) like kRleIdx's restart walk, so it
        // prices a hair above kRleIdx's indexed-probe tier.
        if (s == Scheme::kSfor) {
          return 1.05;
        }
        // kPostings has NO table shortcut from row to list (the lists
        // interleave in row space): a probe sorts the d candidates by first
        // posting and walks their postings -- poor random access, the run
        // family's tier.
        if (s == Scheme::kPostings) {
          return 1.50;
        }
        // kExpFor: the identical directory bisect + one unpack.
        if (s == Scheme::kExpFor) {
          return 1.05;
        }
        // kLane: one lane model eval (a load + a multiply-add) plus a
        // <= 16-word bitmap popcount for the exception rank -- pure
        // arithmetic, no replay, so it prices with the indexed-probe tier
        // (the popcount walk is the parse, and it is bounded by the u16
        // exception cap's bitmap, not by n).
        if (s == Scheme::kLane) {
          return 1.05;
        }
        // kFastPfor has NO row -> payload shortcut: the lanes start after
        // every prior block's variable-length payload, so a probe re-walks
        // the whole per-block metadata (<= 8 x 35B header reads + bitmap
        // popcounts) before its one lane extraction. bench_fastpfor measures
        // the probe sweep at ~12x PLAIN -- worse than simple-8b's word
        // replay (1.50), better than the serial chains (kAlpRdTans 6.00).
        if (s == Scheme::kFastPfor) {
          return 2.20;
        }
        // kDeltaFastPfor pays the same metadata re-walk PLUS one batched
        // lane unpack and a <= 127-step replay (the delta chain).
        // bench_deltafastpfor measures the probe sweep at ~24x PLAIN
        // (FastPFOR ~12x at 2.20): the walk dominates, the unpack+replay
        // roughly doubles it. Below the serial-chain tier (kAlpRdTans
        // 6.00) -- the replay is bounded, not a full-chain re-derive.
        if (s == Scheme::kDeltaFastPfor) {
          return 3.00;
        }
        if (s == Scheme::kDeltaPfor) {
          return 0.90;
        }
        return 1.0;
      case AccessHint::kSortKey:
        if (s == Scheme::kEliasFano || s == Scheme::kDeltaFor ||
            s == Scheme::kDeltaBitweave || s == Scheme::kDdRice ||
            s == Scheme::kDeltaVarint || s == Scheme::kInterp ||
            s == Scheme::kQpp || s == Scheme::kDeltaPfor ||
            s == Scheme::kRleBp || s == Scheme::kRle8 ||
            s == Scheme::kRleDelta) {
          return 0.70;
        }
        // kRleIdx additionally gives the sort key an O(log runs) lower bound
        // (the other run schemes walk every run), so it earns the strongest
        // sort-key tier.
        if (s == Scheme::kRleIdx) {
          return 0.55;
        }
        if (s == Scheme::kBp128) {
          return 0.90;
        }
        return 1.0;
      case AccessHint::kUnknown:
      default:
        return 1.0;
    }
  };
  // The footer cell is RAW STORAGE: it is charged at tier 1.0 (one added
  // term, NOT multiplied by the codec's decode tier) so the charge shifts
  // every non-postings candidate by the same constant and the relative
  // order the byte race established is preserved exactly. prune_charged
  // tracks whether prune_cost currently carries the charge, so the byte
  // budgets below can strip it back out (a postings incumbent carries none,
  // and subtracting it there would over-prune candidates that could still
  // win).
  double best_cost =
      static_cast<double>(best.buf.size()) + static_cast<double>(inv_bytes);
  EncodedCol plain_cand = best;
  const bool budgeted = (forced_scheme == nullptr);
  double prune_cost = best_cost;
  bool prune_is_running_best = true;
  auto budget_for = [&](Scheme s) -> size_t {
    if (!budgeted) {
      return kNoEncodeBudget;
    }
    const double m = decode_cost(s) * hint_mult(hint, s);
    if (!(m > 0.0)) {
      return kNoEncodeBudget;
    }
    // The prune cost carries the +inv_bytes storage charge whenever the
    // incumbent is a non-postings scheme; whether the CHALLENGER pays the
    // same charge is a property of the challenger's scheme, so the budget
    // strips the challenger's own charge term: a kPostings challenger
    // compares wire x tier against the full prune cost, every other
    // challenger compares wire x tier + charge against it. Region budgets
    // are compared against WIRE bytes either way.
    const double limit =
        (prune_cost -
         (s == Scheme::kPostings ? 0.0 : static_cast<double>(inv_bytes))) /
        m;
    if (limit <= 0.0) {
      return 0;
    }
    if (!(limit < static_cast<double>(SIZE_MAX))) {
      return kNoEncodeBudget;
    }
    return static_cast<size_t>(prune_is_running_best ? limit
                                                     : std::ceil(limit));
  };
  // kAlp (ALP) and kAlpRdTans sizes are cross-checked here; kAlp is always
  // generated before kAlpRdTans, so the running-incumbent form of the
  // original two-pass code is exact.
  size_t alp_size = SIZE_MAX;
  EncodedCol alprdtans_cand;
  auto consider = [&](EncodedCol cand) {
    if (cand.buf.empty()) {
      return;
    }
    // Copy, not move: the cost evaluation below still needs cand's bytes, and
    // this path only runs under TINYLAMB_FORCE_SCHEME.
    if (!budgeted) {
      candidates.push_back(cand);
    }
    if (cand.scheme == Scheme::kAlp) {
      alp_size = cand.buf.size();
    }
    if (cand.scheme == Scheme::kAlpRdTans) {
      alprdtans_cand = cand;
    }
    if (cand.scheme == Scheme::kWaveletR) {
      return;
    }
    // ALPRDTANS enters the cost race only when it beats BOTH the PLAIN
    // baseline and plain ALP; at an equal or larger size ALP's cheaper
    // mantissa packing is the better buy.
    if (cand.scheme == Scheme::kAlpRdTans &&
        (cand.buf.size() >= plain_cand.buf.size() ||
         (alp_size != SIZE_MAX && cand.buf.size() >= alp_size))) {
      return;
    }
    const double charged =
        cand.scheme == Scheme::kPostings ? 0.0 : static_cast<double>(inv_bytes);
    const double c = (static_cast<double>(cand.buf.size()) *
                      decode_cost(cand.scheme) * hint_mult(hint, cand.scheme)) +
                     charged;
    if (c < best_cost) {
      if (!prune_is_running_best || c <= prune_cost) {
        prune_cost = c;
        prune_is_running_best = true;
      }
      best = std::move(cand);
      best_cost = c;
    }
  };
  // An empty region must not index vals[0] (unreachable: groups are never
  // empty, but keep the historical guard shape).
  const bool all_same = !vals.empty() && prof.emin == prof.emax;
  if (forced_scheme == nullptr) {
    if (all_same) {
      return EncConstI64(vals[0]);
    }
    if (!kNoNewSchemes && !is_double && vals.size() > 2) {
      const uint64_t step =
          static_cast<uint64_t>(vals[1]) - static_cast<uint64_t>(vals[0]);
      bool exact = step != 0;
      for (size_t i = 1; exact && i < vals.size(); ++i) {
        exact = static_cast<uint64_t>(vals[i]) -
                    static_cast<uint64_t>(vals[i - 1]) ==
                step;
      }
      if (exact) {
        EncodedCol arithmetic = EncArithI64(vals);
        if (!arithmetic.buf.empty()) {
          return arithmetic;
        }
      }
    }
  }
  const int distinct_hint =
      static_cast<int>(prof.is_i64 ? prof.xorset.size() : prof.distinct.size());
  if (forced_scheme != nullptr && forced_known &&
      (!kNoNewSchemes || static_cast<uint8_t>(forced_kind) < 8U)) {
    EncodedCol direct;
    switch (forced_kind) {
      case Scheme::kPlain:
        direct = plain_cand;
        break;
      case Scheme::kConst:
        if (all_same) {
          direct = EncConstI64(vals[0]);
        }
        break;
      case Scheme::kBitmap:
        if (distinct_hint == 2) {
          direct = EncBitmap(vals);
        }
        break;
      case Scheme::kWah:
        if (distinct_hint == 2 && vals.size() > 1) {
          direct = EncWahI64(vals);
        }
        break;
      case Scheme::kRle:
        direct = EncRleI64(vals);
        break;
      case Scheme::kForBp:
        direct = EncForBA(vals);
        break;
      case Scheme::kDict:
        if (distinct_hint <= 1024) {
          direct = EncDictI64(vals);
        }
        break;
      case Scheme::kDeltaFp:
        if (!is_double) {
          direct = EncDeltaFp(vals);
        }
        break;
      case Scheme::kXorFor:
        if (vals.size() > 1) {
          direct = EncXorF64(ubits);
        }
        break;
      case Scheme::kBitWeave:
        if (!is_double) {
          direct = EncBitWeaveI64(vals);
        }
        break;
      case Scheme::kEliasFano:
        if (!is_double && prof.mono && vals.size() > 1) {
          direct = EncEliasFanoI64(vals);
        }
        break;
      case Scheme::kWavelet:
        if (!is_double && vals.size() > 1) {
          direct = EncWaveletI64(vals);
        }
        break;
      case Scheme::kDeltaFor:
        if (!is_double && vals.size() > 1) {
          direct = EncDeltaForI64(vals);
        }
        break;
      case Scheme::kDeltaBitweave:
        if (!is_double && vals.size() > 1) {
          direct = EncDeltaBitweaveI64(vals);
        }
        break;
      case Scheme::kDeltaSimd:
        if (!is_double && vals.size() > 1) {
          direct = EncDeltaSimdI64(vals);
        }
        break;
      case Scheme::kS4:
        if (!is_double && vals.size() > 1) {
          direct = EncS4I64(vals);
        }
        break;
      case Scheme::kByteSlice:
        if (!is_double) {
          direct = EncByteSliceI64(vals);
        }
        break;
      case Scheme::kGorilla:
        if (is_double && vals.size() > 1) {
          direct = EncGorillaF64(ubits);
        }
        break;
      case Scheme::kSfor:
        // Same gate as the adaptive candidate above: monotone non-negative
        // bitcast cells, no nulls (raw 0 would fake monotonicity).
        if (is_double && !prof.has_null && vals.size() > 1 && prof.mono &&
            prof.emin >= 0) {
          direct = EncSforF64(ubits);
        }
        break;
      case Scheme::kExpFor:
        // Finite cells only (NaN/Inf decline); the null gate is the family
        // convention -- a raw-0 null cell would silently join the +0.0 band.
        if (is_double && !prof.has_null && vals.size() > 1) {
          direct = EncExpForF64(ubits);
        }
        break;
      case Scheme::kSortDict:
        // int64 only; the mostly-distinct gate is the encoder's own.
        if (!is_double && vals.size() > 1) {
          direct = EncSortDictI64(vals);
        }
        break;
      case Scheme::kPfor:
        if (!is_double && vals.size() > 1) {
          direct = EncPforI64(vals);
        }
        break;
      case Scheme::kFastPfor:
        // n == 1 encodes fine (a single w == 0 block), so unlike the
        // neighbors the gate is only emptiness -- the trial-list force path
        // accepts one-row cells and this direct path must agree.
        if (!is_double && !vals.empty()) {
          direct = EncFastPforI64(vals);
        }
        break;
      case Scheme::kDeltaFastPfor:
        // Same gate as kFastPfor's: n == 1 encodes fine (a single block
        // with no deltas at all), so the gate is only emptiness and the
        // trial-list force path must agree.
        if (!is_double && !vals.empty()) {
          direct = EncDeltaFastPforI64(vals);
        }
        break;
      case Scheme::kDdRice:
        if (!is_double && vals.size() > 1) {
          direct = EncDdRiceI64(vals);
        }
        break;
      case Scheme::kChimp:
        if (is_double && vals.size() > 1) {
          direct = EncChimpF64(ubits);
        }
        break;
      case Scheme::kDeltaVarint:
        if (!is_double && vals.size() > 1) {
          direct = EncDeltaVarintI64(vals);
        }
        break;
      case Scheme::kAlp:
        if (is_double && vals.size() > 1) {
          direct = EncAlpF64(ubits);
        }
        break;
      case Scheme::kRleDict:
        if (distinct_hint <= 1024) {
          direct = EncRleDictI64(vals);
        }
        break;
      case Scheme::kRleBp:
        if (vals.size() > 1) {
          direct = EncRleBpI64(vals, kNoEncodeBudget, distinct_hint);
        }
        break;
      case Scheme::kFreq:
        if (vals.size() > 1) {
          direct = EncFreqI64(vals);
        }
        break;
      case Scheme::kTopk:
        if (!is_double && vals.size() > 1) {
          direct = EncTopKI64(vals);
        }
        break;
      case Scheme::kTileDict:
        // int64 only; the encoder declines single-tile regions and tiles
        // over the 64-entry pass-mask bound itself.
        if (!is_double && vals.size() > 1) {
          direct = EncTileDictI64(vals);
        }
        break;
      case Scheme::kRleX:
        direct = EncRleXI64(vals);
        break;
      case Scheme::kDictX:
        if (distinct_hint <= 1024) {
          direct = EncDictXI64(vals);
        }
        break;
      case Scheme::kArith:
        if (!is_double && vals.size() > 1) {
          direct = EncArithI64(vals);
        }
        break;
      case Scheme::kPla:
        if (!is_double && vals.size() > 1) {
          direct = EncPlaI64(vals);
        }
        break;
      case Scheme::kMod:
        // int64 only; n >= 8, non-negative, wrapped-model gates are the
        // encoder's own.
        if (!is_double && vals.size() > 1) {
          direct = EncModI64(vals);
        }
        break;
      case Scheme::kLane:
        // int64 only; n >= 16, nd <= n/4, majority-on-model gates are the
        // encoder's own.
        if (!is_double && vals.size() > 1) {
          direct = EncLaneI64(vals);
        }
        break;
      case Scheme::kPostings:
        // int64 only; the structural gates (4 <= d <= 64, d <= n/8) are the
        // encoder's own and stay under force.
        if (!is_double && vals.size() > 1) {
          direct = EncPostingsI64(vals, kNoEncodeBudget, distinct_hint);
        }
        break;
      case Scheme::kQpp:
        if (!is_double && vals.size() > 2) {
          direct = EncQppI64(vals);
        }
        break;
      case Scheme::kSparse:
        if (prof.has_null) {
          direct = EncSparseI64(vals, prof.null_bmp);
        }
        break;
      case Scheme::kAnsFor:
        if (!is_double && vals.size() > 1) {
          direct = EncAnsForI64(vals);
        }
        break;
      case Scheme::kOpDict:
        if (!is_double && distinct_hint <= 1024) {
          direct = EncOpDictI64(vals);
        }
        break;
      case Scheme::kOpRle:
        if (!is_double && distinct_hint <= 1024) {
          direct = EncOpRleI64(vals);
        }
        break;
      case Scheme::kXorForC:
        if (vals.size() > 1) {
          direct = EncXorForCF64(ubits);
        }
        break;
      case Scheme::kGorillaC:
        if (is_double && vals.size() > 1) {
          direct = EncGorillaCF64(ubits);
        }
        break;
      case Scheme::kChimpC:
        if (is_double && vals.size() > 1) {
          direct = EncChimpCF64(ubits);
        }
        break;
      case Scheme::kPBitS:
        if (!is_double && !vals.empty()) {
          direct = EncPBitSI64(vals);
        }
        break;
      case Scheme::kSimple8b:
        if (!is_double && !vals.empty()) {
          direct = EncSimple8bI64(vals);
        }
        break;
      case Scheme::kRleIdx:
        if (!is_double && !vals.empty()) {
          direct = EncRleIdxI64(vals);
        }
        break;
      case Scheme::kStreamVByte:
        if (!is_double && !vals.empty()) {
          direct = EncStreamVByteI64(vals);
        }
        break;
      case Scheme::kHuffDict:
        if (!is_double && vals.size() > 1) {
          direct = EncHuffDictI64(vals);
        }
        break;
      case Scheme::kSimdFor:
        if (!is_double && vals.size() > 1) {
          direct = EncSimdForI64(vals);
        }
        break;
      case Scheme::kD2Pfor:
        if (!is_double && vals.size() > 1) {
          direct = EncD2PforI64(vals);
        }
        break;
      case Scheme::kInterp:
        if (!is_double && prof.mono && vals.size() > 1) {
          direct = EncInterpI64(vals);
        }
        break;
      case Scheme::kDeltaPfor:
        if (!is_double && prof.mono && vals.size() > 1) {
          direct = EncDeltaPForI64(vals);
        }
        break;
      case Scheme::kWaveletR:
        if (!is_double && vals.size() > 1) {
          direct = EncWaveletRI64(vals);
        }
        break;
      case Scheme::kBp128:
        if (!is_double && !vals.empty()) {
          direct = EncBp128I64(vals);
        }
        break;
      case Scheme::kRle8:
        if (!is_double && !vals.empty()) {
          direct = EncRle8I64(vals);
        }
        break;
      case Scheme::kRleDelta:
        if (!is_double && !vals.empty()) {
          direct = EncRleDeltaI64(vals);
        }
        break;
      case Scheme::kChimp128:
        if (is_double && !prof.has_null) {
          direct = EncChimp128F64(ubits);
        }
        break;
      case Scheme::kAlpRdTans:
        if (is_double && !prof.has_null) {
          direct = EncAlpRdTansF64(ubits);
        }
        break;
      case Scheme::kPatas:
        if (is_double && !prof.has_null) {
          direct = EncPatasF64(ubits);
        }
        break;
      case Scheme::kFsst:
      case Scheme::kFrontCode:
      case Scheme::kTemplate:
      case Scheme::kDStream:
        break;  // varchar-only schemes: the numeric force path never sees them
    }
    if (!direct.buf.empty()) {
      return direct;
    }
  }
  EncodedCol bitweave_cand;
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    bitweave_cand = EncBitWeaveI64(vals);
    if (budgeted && !bitweave_cand.buf.empty()) {
      const double c = (static_cast<double>(bitweave_cand.buf.size()) *
                        decode_cost(Scheme::kBitWeave) *
                        hint_mult(hint, Scheme::kBitWeave)) +
                       static_cast<double>(inv_bytes);
      if (c < prune_cost) {
        prune_cost = c;
        prune_is_running_best = false;
      }
    }
  }
  if (all_same) {
    consider(EncConstI64(vals[0]));
  }

  if (distinct_hint == 2) {
    consider(EncBitmap(vals, budget_for(Scheme::kBitmap)));
  }
  // Word-Aligned Hybrid: the run-compressed member of the two-value family.
  // The size comparison against kBitmap/kRle/kDict decides; the fill words
  // win once spans cluster (63 rows per 8B vs 12B per kRle run).
  if (distinct_hint == 2 && !kNoNewSchemes && vals.size() > 1) {
    consider(EncWahI64(vals, budget_for(Scheme::kWah)));
  }
  consider(EncRleI64(vals, budget_for(Scheme::kRle)));
  if (!kNoNewSchemes && vals.size() > 1) {
    consider(EncRleBpI64(vals, budget_for(Scheme::kRleBp), distinct_hint));
  }
  // Monotone run-length with a 16-run restart index. The encoder's own
  // non-decreasing gate rejects the rest, so this costs one comparison per row
  // on non-monotone columns; where it applies it trades 8 + runs/4 bytes over
  // kRleBp for the run family's only O(log) lower bound, O(1) row -> run
  // probe, and O(log + window) range COUNT/SUM. The cost model above decides
  // (see the kSortKey / kPointLookup / kFilter multipliers).
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncRleIdxI64(vals, budget_for(Scheme::kRleIdx)));
  }
  // Byte-aligned variable-byte. Only int64 (a bitcast double never lands in
  // four bytes, so the encoder would always bail) and never wider than 4
  // bytes per value. It has no ordering or run structure, so the cost model
  // settles the race purely on bytes x decode tier: it wins over
  // kForBp/kBitWeave exactly when the per-value widths are mixed, and loses
  // by the quarter-byte control overhead when they are uniform.
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    consider(EncStreamVByteI64(vals, budget_for(Scheme::kStreamVByte)));
  }
  const bool force_rle8 =
      forced_scheme != nullptr && std::strcmp(forced_scheme, "RLE8") == 0;
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    // Encode once and let the applicability probe judge that encoding: the
    // predicate used to re-run the full encoder and the caller then ran it a
    // second time, so every row group paid for RLE8 twice.
    EncodedCol rle8 = EncRle8I64(vals, budget_for(Scheme::kRle8));
    if (force_rle8 || (vals.size() > 1 && Rle8AdaptiveApplicable(vals, rle8))) {
      consider(std::move(rle8));
    }
  }
  const bool force_rle_delta =
      forced_scheme != nullptr && std::strcmp(forced_scheme, "RLEDELTA") == 0;
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    EncodedCol rle_delta = EncRleDeltaI64(vals, budget_for(Scheme::kRleDelta));
    if (force_rle_delta || RleDeltaAdaptiveApplicable(vals, rle_delta)) {
      consider(std::move(rle_delta));
    }
  }
  // Run-length over dictionary codes: kRle's O(runs) query shape at 5-6B per
  // run instead of 12B. The size comparison alone decides between the three
  // run/dict-family candidates; gates mirror EncDictI64 (u16 code space,
  // adaptive cap 1024 distinct).
  if (!kNoNewSchemes && distinct_hint <= 1024) {
    consider(EncRleDictI64(vals, budget_for(Scheme::kRleDict), distinct_hint));
  }
  // Cascaded RLE (BtrBlocks recursive compression): values and run lengths
  // are each inner-encoded (values -> DICT, counts -> FOR-BA/RLE), so it can
  // undercut both kRle and kRleDict when the substreams compress further.
  if (!kNoNewSchemes) {
    consider(EncRleXI64(vals, budget_for(Scheme::kRleX)));
  }
  if (distinct_hint <= 1024) {
    consider(EncDictI64(vals, budget_for(Scheme::kDict), distinct_hint));
  }
  // Cascaded dictionary: the code stream itself is an inner region (RLE on
  // clustered codes, FOR-BA on dense low codes, ...). Same distinct gate as
  // kDict -- the encoder itself caps at u16 codes.
  if (!kNoNewSchemes && distinct_hint <= 1024) {
    consider(EncDictXI64(vals, budget_for(Scheme::kDictX), distinct_hint));
  }
  // Frequency encoding: dominant value + exception bitmap + inner-encoded
  // exceptions. Fills the gap between kBitmap (exactly 2 distinct) and
  // kDict on skewed columns; EQ COUNT on the dominant value is a popcount.
  if (!kNoNewSchemes && vals.size() > 1) {
    consider(EncFreqI64(vals, budget_for(Scheme::kFreq)));
  }
  // Top-K frequency: kFreq generalized to the 15 most frequent values with
  // an exact inline count table. High-cardinality skewed columns where every
  // d-entry dictionary pays more overhead than the column (d >= ~100) but
  // the zipf head still covers most rows at 4 bits each; predicates fold
  // over the hot window in closed form. int64 only.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncTopKI64(vals, budget_for(Scheme::kTopk)));
  }
  // Block-local dictionary: kDict re-scoped to 64-row tiles, for columns
  // whose GLOBAL cardinality is far beyond any dictionary's reach but whose
  // values repeat WITHIN a tile (rows grouped by the entity the values
  // name). Predicates fold per tile through the ascending tile dictionary
  // -- wholly-inside/outside tiles answer from the bracket alone. int64
  // only; the encoder declines tiles over 64 distinct (the single-u64
  // pass-mask bound) and single-tile regions (kDict's niche).
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncTileDictI64(vals, budget_for(Scheme::kTileDict)));
  }
  // Sorted-view permutation (scheme 61): the dictionary family pushed past
  // its code cap. Columns where MOST values are unique have no dictionary
  // home (kDict/kOpDict cap at 1024 codes, kTileDict at 64 per tile) and
  // every predicate costs a full decode on the flat packers that store them
  // today; SORT-DICT re-sorts the row group and answers predicates from the
  // ascending block brackets, with the wrapped prefix-sum table folding an
  // unpredicated SUM in one load. The mostly-distinct gate (inside the
  // encoder) keeps the dictionary family's niche -- below half-unique the
  // dict stores the same rows smaller -- and the byte race against
  // PBitS/BitWeave decides the rest (uniform columns pay the ~10-bit rank
  // tax; outlier-inflated columns SAVE, because per-block widths absorb the
  // rows that inflate a flat packer's global width). int64 only.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(
        EncSortDictI64(vals, budget_for(Scheme::kSortDict), distinct_hint));
  }
  // Inverted dictionary (scheme 63): the dictionary TRANSPOSED -- each
  // distinct value owns a Rice-coded posting list of its row ids behind an
  // exact count table. ANY predicate's COUNT and SUM fold in closed form
  // over the lists it passes (O(log d) for EQ, zero bitstream reads), a
  // selective predicate materializes its mask at O(matched), and random
  // access walks the count table without touching a bit. The encoder's
  // economic gate (coded postings must beat the kDict layout) keeps the
  // uniform low-cardinality dictionary niche theirs -- postings earn their
  // bytes only where per-list Rice is narrower than the flat log2(d) code
  // width (skewed mid-cardinality columns). int64 only.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(
        EncPostingsI64(vals, budget_for(Scheme::kPostings), distinct_hint));
  }
  // Exact arithmetic progression: a + i*d with a u16-capped exception list.
  // Fixed-rate clocks / sequence columns / ramps with rare outliers; range
  // predicates fold to a closed-form index window and SUM to a closed form.
  // int64 only -- a bitcast double column has no meaningful step d.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncArithI64(vals, budget_for(Scheme::kArith)));
  }
  // Piecewise-linear exact model: kArith generalized to nseg variable-length
  // lines. Columns made of several long equal-step runs with different steps
  // (rate switches, plateau+ramp mixes, sawtooth clocks) that kArith rejects
  // at its majority-off-line gate; predicates fold to one closed-form window
  // per segment and SUM to the sum of segment closed forms. int64 only.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncPlaI64(vals, budget_for(Scheme::kPla)));
  }
  // Modular affine exact model: kArith generalized over Z/mZ. Columns that
  // WRAP -- i % N round-robin keys, (c*i) mod N fillers, clock fields --
  // where kArith pays one exception per wrap and kPla splits into per-wrap
  // segments. The encoder's wrap gate keeps wrap-free windows (kArith's
  // exact niche) out. int64 only.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncModI64(vals, budget_for(Scheme::kMod)));
  }
  // K interleaved arithmetic lanes: kArith generalized to K interleaved
  // lines. Sharded sequences / round-robin multi-stream sensors sampled in
  // rotation produce exactly this column; kArith/kPla need the ramp
  // contiguous in row order and kMod needs one global modulus, so nothing
  // else owns the shape. Predicates fold to one closed-form window per lane
  // and SUM to the sum of lane closed forms. int64 only.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncLaneI64(vals, budget_for(Scheme::kLane)));
  }
  const bool force_qpp =
      forced_scheme != nullptr && std::strcmp(forced_scheme, "QPP") == 0;
  if (!kNoNewSchemes && !is_double && vals.size() > 2 &&
      (force_qpp || QppAdaptiveApplicable(vals))) {
    consider(EncQppI64(vals, budget_for(Scheme::kQpp)));
  }
  // Sparse null-majority: only non-null rows are stored (position list +
  // dense inner values). The size comparison gates it to columns where
  // nulls are frequent enough that dropping their payload slots pays.
  if (!kNoNewSchemes && prof.has_null) {
    consider(EncSparseI64(vals, prof.null_bmp, budget_for(Scheme::kSparse)));
  }
  // Entropy-coded FOR offsets (rANS over the offset byte stream): the only
  // candidate that prices each byte at its marginal frequency. Wins on
  // skewed offsets where the fixed bpv slot wastes high-order bytes.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncAnsForI64(vals, budget_for(Scheme::kAnsFor)));
  }
  // Order-preserving dictionary: byte-identical to kDict (strict '<' keeps
  // kDict on the tie) but priced by the cost model below: a filter-target
  // column rewards the code-window capability and OPDICT wins the tie there.
  // int64 only: the order it preserves is the raw integer order, which a
  // bitcast double column does not map to.
  if (!kNoNewSchemes && !is_double && distinct_hint <= 1024) {
    consider(EncOpDictI64(vals, budget_for(Scheme::kOpDict), distinct_hint));
  }
  // Order-preserving run x dictionary: kRleDict's byte size with a sorted
  // dict, so range predicates fold to a code window instead of a pass
  // table. Wins on sorted/low-cardinality filter columns.
  if (!kNoNewSchemes && !is_double && distinct_hint <= 1024) {
    consider(EncOpRleI64(vals, budget_for(Scheme::kOpRle), distinct_hint));
  }
  // Entropy-coded order-preserving dictionary (kHuffDict): the per-symbol
  // frequency table makes every predicate's COUNT/SUM a closed form over the
  // d symbols, and the Huffman bitstream beats the fixed rank width on
  // multi-modal skewed categoricals. The table overhead (~2.25 B/distinct
  // plus 4 B per 64-row checkpoint) loses to kOpDict/kDict on uniform or
  // single-mode columns -- the cost model arbitrates, exactly as for the
  // other capability-priced members.
  if (!kNoNewSchemes && !is_double && distinct_hint >= 2 &&
      distinct_hint <= 1024) {
    consider(
        EncHuffDictI64(vals, budget_for(Scheme::kHuffDict), distinct_hint));
  }
  if (!is_double) {
    consider(EncDeltaFp(vals, budget_for(Scheme::kDeltaFp)));
  }
  // Opt-out for environments whose readers lack the WASM decoders for
  // schemes >= 8 (e.g., a stale wasm_scan_module.wasm). Native reads are
  // always fine; this only guards cross-runtime deployments.
  if (!bitweave_cand.buf.empty()) {
    consider(std::move(bitweave_cand));
  } else if (!kNoNewSchemes && !is_double) {
    consider(EncBitWeaveI64(vals, budget_for(Scheme::kBitWeave)));
  }
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    consider(EncBp128I64(vals, budget_for(Scheme::kBp128)));
  }
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncPforI64(vals, budget_for(Scheme::kPfor)));
  }
  // Block-local patched FOR (scheme 66): kBp128's per-block envelope plus
  // kPfor's exception patching -- the shape neither owns (block-local ranges
  // WITH outliers: kBp128 pays one spike on every row of its block, kPfor
  // pays the global width on every row of the region). Clean blocks emit the
  // same lanes as kBp128 plus 2B + 16B/block of bracket/bitmap overhead, so
  // the byte race keeps kBp128's niche theirs wherever nothing overflows.
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    consider(EncFastPforI64(vals, budget_for(Scheme::kFastPfor)));
  }
  // Block-local patched frame over FIRST DIFFERENCES (scheme 67): the
  // delta-transformed sibling of the kFastPfor block above -- kDeltaPfor
  // keeps kPfor's global width, kS4 has no patch and no brackets, kFastPfor
  // frames raw values and pays a cumulative series' per-block climb in
  // width. The 16B/block of extra metadata (base + dmin on top of
  // FastPFOR's envelope) keeps stationary-delta columns with kDeltaPfor and
  // mean-reverting columns with kFastPfor; the byte race decides.
  //
  // Home-turf fence: when the delta stream is STATIONARY and exception-free
  // (span <= 15), the sequential-replay delta family owns the column --
  // DELTA-FP / DELTA-PFOR / D2PFOR pay exactly one narrow delta code per
  // row while this scheme adds 51B/block of envelope, and the byte race
  // cannot arbitrate that trade because the family's honest decode tier
  // (kD2Pfor 2.80) is 2.3x this scheme's flat-pack one (1.20): on a smooth
  // base+step+jitter series the cost model would flip a 35%-smaller D2PFOR
  // region to a 52%-bigger one purely on decode speed. The fence prices
  // neutral workloads storage-first. The forced direct path bypasses it
  // (the measurement contract), so TINYLAMB_FORCE_SCHEME still round-trips
  // ramps and every other shape.
  if (!kNoNewSchemes && !is_double && !vals.empty() &&
      !DeltaFastPforStationaryDeltas(vals)) {
    consider(EncDeltaFastPforI64(vals, budget_for(Scheme::kDeltaFastPfor)));
  }
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    consider(EncPBitSI64(vals, budget_for(Scheme::kPBitS), prof.mono));
  }
  // Simple-8b word-adaptive packing (scheme 48): per-word width adaptation
  // for NON-monotone columns whose value widths have a long tail (most rows
  // narrow, a few wide -- latencies, durations, sizes). BitWeave/BP128 pay
  // the region/block MAX width on every row and PFOR pays pos+8B per
  // exception, while simple-8b pays the selector tax (4/60 bits) once per
  // word; the size comparison against those candidates decides. Predicate
  // folds skip whole words on the selector-implied range.
  if (!kNoNewSchemes && !is_double && !vals.empty()) {
    consider(EncSimple8bI64(vals, budget_for(Scheme::kSimple8b)));
  }
  // Bit-sliced: competitive for non-negative / low-range high-cardinality ints,
  // and enables decode-free SUM via popcount over the slices.
  if (!kNoNewSchemes && !is_double) {
    consider(EncByteSliceI64(vals, budget_for(Scheme::kByteSlice)));
  }
  consider(EncForBA(vals, budget_for(Scheme::kForBp)));
  if (!kNoNewSchemes && vals.size() > 1 && !is_double) {
    consider(EncWaveletI64(vals, budget_for(Scheme::kWavelet)));
  }
  if (!kNoNewSchemes && vals.size() > 1 && !is_double &&
      forced_scheme != nullptr && std::strcmp(forced_scheme, "WAVELETR") == 0) {
    consider(EncWaveletRI64(vals));
  }
  {
    const bool mono = prof.mono;
    if (!kNoNewSchemes && mono && vals.size() > 1 && !is_double) {
      consider(EncEliasFanoI64(vals, budget_for(Scheme::kEliasFano)));
    }
    // Binary interpolative coding: the monotone candidate for CLUSTERED
    // (bursty-gap) sequences. Elias-Fano prices every row at l = log2
    // (max-gap/n) low bits, so one huge gap inflates the whole column;
    // interpolative coding narrows the code widths toward the leaves and
    // pays a wide gap only near the root of its block tree. The size
    // comparison against EF/DELTA-FP/DELTA-FOR decides, as everywhere.
    if (!kNoNewSchemes && mono && vals.size() > 1 && !is_double) {
      consider(EncInterpI64(vals, budget_for(Scheme::kInterp)));
    }
    if (!kNoNewSchemes && mono && vals.size() > 1 && !is_double) {
      consider(EncDeltaPForI64(vals, budget_for(Scheme::kDeltaPfor)));
    }
  }
  // Composition exploration: delta-transform then compress the deltas with a
  // random-accessible inner scheme (FOR-BA / BitWeave). These are evaluated on
  // equal footing with the atomic candidates by the cost model below.
  if (!kNoNewSchemes && !is_double && vals.size() > 1) {
    consider(EncDeltaForI64(vals, budget_for(Scheme::kDeltaFor)));
    consider(EncDeltaBitweaveI64(vals, budget_for(Scheme::kDeltaBitweave)));
    // The sd x (4-lane interleaved FOR) cell: the same first differences,
    // packed by kSimdFor's layout instead of a byte-offset one.
    consider(EncDeltaSimdI64(vals, budget_for(Scheme::kDeltaSimd)));
    // Running-max frame of reference (scheme 55): the predictor absorbs dips
    // and plateaus that set a wide frame for every later row of a
    // first-difference codec, which is the gap on jittery monotonic columns.
    // Priced with the 4-lane packer because its seek table is read, not
    // replayed, so the size race below is the only thing deciding.
    if (!kNoNewSchemes && !is_double && vals.size() > 1) {
      consider(EncS4I64(vals, budget_for(Scheme::kS4)));
    }
    consider(EncDdRiceI64(vals, budget_for(Scheme::kDdRice)));
    consider(EncDeltaVarintI64(vals, budget_for(Scheme::kDeltaVarint)));
    // Second difference instead of first: the composite that is missing from
    // the derivative x inner-codec table. Costs one more prefix sum per probe
    // (two instead of one) for a dd stream that is typically an order of
    // magnitude smaller than sd, so the size comparison decides, as always.
    consider(EncSimdForI64(vals, budget_for(Scheme::kSimdFor)));
    consider(EncD2PforI64(vals, budget_for(Scheme::kD2Pfor)));
  }
  if (vals.size() > 1) {
    consider(EncXorF64(ubits, budget_for(Scheme::kXorFor)));
  }
  // Checkpointed variants: same streams plus a 128-row seek table, so
  // point access drops from O(i) to O(128). Slightly larger than the
  // scheme-7/14/18 originals; the cost model arbitrates.
  if (!kNoNewSchemes && vals.size() > 1) {
    consider(EncXorForCF64(ubits, budget_for(Scheme::kXorForC)));
  }
  if (!kNoNewSchemes && is_double && vals.size() > 1) {
    consider(EncGorillaF64(ubits));
    consider(EncChimpF64(ubits));
    consider(EncAlpF64(ubits));
    consider(EncGorillaCF64(ubits));
    consider(EncChimpCF64(ubits));
    if (!prof.has_null) {
      consider(EncChimp128F64(ubits));
    }
    // SFOR (57): the only sorted-aware double candidate. The gate mirrors the
    // encoder's own: monotone bitcast cells AND no negative (sign-bit) value
    // anywhere -- raw order must be value order for the group brackets to
    // prune. Nulls are excluded like the other recent double codecs (a raw-0
    // null cell would silently make an unsorted column look monotone).
    if (!prof.has_null && prof.mono && prof.emin >= 0) {
      consider(EncSforF64(ubits, budget_for(Scheme::kSfor)));
    }
    // EXPFOR (60): the general-double group-FOR -- every band grouping
    // kSfor's monotone gate excludes (unsorted, signed, mixed magnitudes)
    // plus the monotone ones the byte race decides. Finite cells only and
    // no nulls (the family convention).
    if (!prof.has_null) {
      consider(EncExpForF64(ubits, budget_for(Scheme::kExpFor)));
    }
  }
  if (is_double && vals.size() == 1 && !prof.has_null &&
      forced_scheme != nullptr && std::strcmp(forced_scheme, "CHIMP128") == 0) {
    consider(EncChimp128F64(ubits));
  }
  bool alp_rd_tans_applicable = false;
  if (is_double && !prof.has_null && !kNoNewSchemes && distinct_hint > 256) {
    alp_rd_tans_applicable = true;
    size_t changed_bits = 0;
    size_t block_count = 0;
    for (size_t block = 0; block < ubits.size(); block += kAlpRdTansBlockRows) {
      const size_t end = std::min(ubits.size(), block + kAlpRdTansBlockRows);
      const uint64_t anchor = ubits[block];
      uint64_t changed = 0;
      for (size_t row = block; row < end; ++row) {
        const uint64_t raw = ubits[row];
        if ((raw & UINT64_C(0x7ff0000000000000)) ==
            UINT64_C(0x7ff0000000000000)) {
          alp_rd_tans_applicable = false;
          break;
        }
        changed |= raw ^ anchor;
      }
      if (!alp_rd_tans_applicable) {
        break;
      }
      changed_bits += static_cast<size_t>(__builtin_popcountll(changed));
      ++block_count;
    }
    if (alp_rd_tans_applicable && changed_bits >= 45U * block_count) {
      alp_rd_tans_applicable = false;
    }
  }
  if (is_double && !prof.has_null &&
      ((forced_scheme != nullptr &&
        std::strcmp(forced_scheme, "ALPRDTANS") == 0) ||
       alp_rd_tans_applicable)) {
    EncodedCol alp_rd_tans = EncAlpRdTansF64(ubits);
    if (forced_scheme != nullptr ||
        (!alp_rd_tans.buf.empty() && alp_rd_tans.buf.size() >= 6 &&
         static_cast<uint8_t>(alp_rd_tans.buf[5]) <= 2)) {
      consider(std::move(alp_rd_tans));
    }
  }
  const bool force_patas =
      forced_scheme != nullptr && std::strcmp(forced_scheme, "PATAS") == 0;
  if (is_double && !prof.has_null && (force_patas || !kNoNewSchemes)) {
    EncodedCol patas = EncPatasF64(ubits);
    if (force_patas || !patas.buf.empty()) {
      consider(std::move(patas));
    }
  }

  // Test/benchmark hook: force a specific scheme regardless of size so every
  // decoder path stays measurable even when economically dominated.
  if (forced_scheme != nullptr) {
    if (forced_known) {
      for (const auto& cand : candidates) {
        if (cand.scheme == forced_kind && !cand.buf.empty()) {
          return cand;
        }
      }
    }
    // A silently-ignored force is a measurement trap: the JS scheme driver
    // reads the actual enc byte out of every footer, but a C++ benchmark
    // that trusts the env label would profile the wrong encoder (BITMAP
    // needs exactly 2 distinct values, CONST all-equal, ...). Every path
    // that applied the force returned above, so reaching this point means
    // the name was unknown or the scheme infeasible -- warn once.
    static std::atomic<bool> warned{false};
    bool expected = false;
    if (warned.compare_exchange_strong(expected, true)) {
      LOG(WARN) << "TINYLAMB_FORCE_SCHEME=" << forced_scheme
                << " not applicable to this column; keeping the adaptive"
                   " choice";
    }
  }
  if ((hint == AccessHint::kFilter || hint == AccessHint::kAggregate) &&
      distinct_hint == 2) {
    if (!alprdtans_cand.buf.empty() &&
        alprdtans_cand.buf.size() < plain_cand.buf.size() &&
        (alp_size == SIZE_MAX || alprdtans_cand.buf.size() < alp_size)) {
      return alprdtans_cand;
    }
  }
  return best;
}

}  // namespace pax_detail

namespace codec_benchmark {

namespace {

std::optional<EncodedBlock> WrapEncoded(EncodedCol encoded, uint32_t n,
                                        ValueKind kind) {
  if (encoded.buf.empty()) {
    return std::nullopt;
  }
  EncodedBlock block;
  block.bytes = std::move(encoded.buf);
  block.scheme = encoded.scheme;
  block.param = encoded.param;
  block.row_count = n;
  block.kind = kind;
  return block;
}

EncodedCol PlainNumeric(std::span<const uint64_t> values) {
  EncodedCol encoded;
  encoded.scheme = Scheme::kPlain;
  for (uint64_t value : values) {
    AppendBE64(encoded.buf, value);
  }
  return encoded;
}

}  // namespace

std::optional<EncodedBlock> EncodeInt64Block(Scheme scheme,
                                             std::span<const int64_t> values) {
  if (values.empty() || values.size() > UINT32_MAX) {
    return std::nullopt;
  }
  // These codecs were designed for physical row groups and may carry u16
  // counts/positions internally. Until a codec certifies a wider envelope,
  // decline large experimental blocks instead of emitting truncated headers.
  if (values.size() > UINT16_MAX &&
      (scheme != Scheme::kPlain && scheme != Scheme::kConst &&
       scheme != Scheme::kRle)) {
    return std::nullopt;
  }
  const std::vector<int64_t> input(values.begin(), values.end());
  EncodedCol encoded;
  switch (scheme) {
    case Scheme::kPlain: {
      std::vector<uint64_t> raw;
      raw.reserve(input.size());
      for (int64_t value : input) {
        raw.push_back(static_cast<uint64_t>(value));
      }
      encoded = PlainNumeric(raw);
      break;
    }
    case Scheme::kConst:
      if (!std::all_of(input.begin(), input.end(),
                       [&](int64_t value) { return value == input.front(); })) {
        return std::nullopt;
      }
      encoded = EncConstI64(input.front());
      break;
    case Scheme::kRle:
      encoded = EncRleI64(input);
      break;
    case Scheme::kRleBp:
      encoded = EncRleBpI64(input);
      break;
    case Scheme::kRleIdx:
      encoded = EncRleIdxI64(input);
      break;
    case Scheme::kStreamVByte:
      encoded = EncStreamVByteI64(input);
      break;
    case Scheme::kRle8:
      encoded = EncRle8I64(input);
      break;
    case Scheme::kRleDelta:
      encoded = EncRleDeltaI64(input);
      break;
    case Scheme::kForBp:
      encoded = EncForBA(input);
      break;
    case Scheme::kDict: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() > UINT16_MAX) {
        return std::nullopt;
      }
      encoded = EncDictI64(input);
      break;
    }
    case Scheme::kOpDict: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() > UINT16_MAX) {
        return std::nullopt;
      }
      encoded = EncOpDictI64(input);
      break;
    }
    case Scheme::kHuffDict: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() < 2 || distinct.size() > kHuffDictMaxCodes) {
        return std::nullopt;
      }
      encoded = EncHuffDictI64(input);
      break;
    }
    case Scheme::kRleDict: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() > UINT16_MAX) {
        return std::nullopt;
      }
      encoded = EncRleDictI64(input);
      break;
    }
    case Scheme::kOpRle: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() > UINT16_MAX) {
        return std::nullopt;
      }
      encoded = EncOpRleI64(input);
      break;
    }
    case Scheme::kRleX:
      encoded = EncRleXI64(input);
      break;
    case Scheme::kDictX: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() > UINT16_MAX) {
        return std::nullopt;
      }
      encoded = EncDictXI64(input);
      break;
    }
    case Scheme::kFreq:
      encoded = EncFreqI64(input);
      break;
    case Scheme::kTopk:
      encoded = EncTopKI64(input);
      break;
    case Scheme::kTileDict:
      encoded = EncTileDictI64(input);
      break;
    case Scheme::kSortDict:
      encoded = EncSortDictI64(input);
      break;
    case Scheme::kPostings:
      encoded = EncPostingsI64(input);
      break;
    case Scheme::kArith:
      encoded = EncArithI64(input);
      break;
    case Scheme::kPla:
      encoded = EncPlaI64(input);
      break;
    case Scheme::kMod:
      encoded = EncModI64(input);
      break;
    case Scheme::kLane:
      encoded = EncLaneI64(input);
      break;
    case Scheme::kQpp:
      encoded = EncQppI64(input);
      break;
    case Scheme::kSparse:
      // Needs the row-group null bitmap (a value span cannot express which
      // cells are null); the benchmark block API has no such input.
      return std::nullopt;
    case Scheme::kAnsFor:
      encoded = EncAnsForI64(input);
      break;
    case Scheme::kDeltaFp:
      encoded = EncDeltaFp(input);
      break;
    case Scheme::kBitmap: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() != 2) {
        return std::nullopt;
      }
      encoded = EncBitmap(input);
      break;
    }
    case Scheme::kWah: {
      std::unordered_set<int64_t> distinct(input.begin(), input.end());
      if (distinct.size() != 2 || input.size() < 2) {
        return std::nullopt;
      }
      encoded = EncWahI64(input);
      break;
    }
    case Scheme::kBitWeave:
      encoded = EncBitWeaveI64(input);
      break;
    case Scheme::kBp128:
      encoded = EncBp128I64(input);
      break;
    case Scheme::kEliasFano:
      encoded = EncEliasFanoI64(input);
      break;
    case Scheme::kWavelet:
      encoded = EncWaveletI64(input);
      break;
    case Scheme::kWaveletR:
      encoded = EncWaveletRI64(input);
      break;
    case Scheme::kDeltaFor:
      encoded = EncDeltaForI64(input);
      break;
    case Scheme::kDeltaBitweave:
      encoded = EncDeltaBitweaveI64(input);
      break;
    case Scheme::kByteSlice:
      encoded = EncByteSliceI64(input);
      break;
    case Scheme::kPfor:
      encoded = EncPforI64(input);
      break;
    case Scheme::kFastPfor:
      encoded = EncFastPforI64(input);
      break;
    case Scheme::kDeltaFastPfor:
      encoded = EncDeltaFastPforI64(input);
      break;
    case Scheme::kDeltaPfor:
      encoded = EncDeltaPForI64(input);
      break;
    case Scheme::kPBitS:
      encoded = EncPBitSI64(input);
      break;
    case Scheme::kSimple8b:
      encoded = EncSimple8bI64(input);
      break;
    case Scheme::kInterp:
      // EncInterpI64 returns an empty buffer for non-monotone input;
      // WrapEncoded turns that into std::nullopt for the caller.
      encoded = EncInterpI64(input);
      break;
    case Scheme::kDdRice:
      encoded = EncDdRiceI64(input);
      break;
    case Scheme::kSimdFor:
      encoded = EncSimdForI64(input);
      break;
    case Scheme::kD2Pfor:
      encoded = EncD2PforI64(input);
      break;
    case Scheme::kDeltaSimd:
      encoded = EncDeltaSimdI64(input);
      break;
    case Scheme::kS4:
      encoded = EncS4I64(input);
      break;
    case Scheme::kDeltaVarint:
      encoded = EncDeltaVarintI64(input);
      break;
    case Scheme::kXorFor:
    case Scheme::kGorilla:
    case Scheme::kChimp:
    case Scheme::kXorForC:
    case Scheme::kGorillaC:
    case Scheme::kChimpC:
    case Scheme::kChimp128:
    case Scheme::kAlpRdTans:
    case Scheme::kPatas:
    case Scheme::kAlp:
    case Scheme::kSfor:
    case Scheme::kExpFor:
    case Scheme::kFsst:
    case Scheme::kFrontCode:
    case Scheme::kTemplate:
    case Scheme::kDStream:
      return std::nullopt;
  }
  return WrapEncoded(std::move(encoded), static_cast<uint32_t>(input.size()),
                     ValueKind::kInt64);
}

std::optional<EncodedBlock> EncodeDoubleBitsBlock(
    Scheme scheme, std::span<const uint64_t> values) {
  if (values.empty() || values.size() > UINT32_MAX) {
    return std::nullopt;
  }
  // These codecs were designed for physical row groups and may carry u16
  // counts/positions internally. Until a codec certifies a wider envelope,
  // decline large experimental blocks instead of emitting truncated headers.
  if (values.size() > UINT16_MAX &&
      (scheme != Scheme::kPlain && scheme != Scheme::kConst)) {
    return std::nullopt;
  }
  const std::vector<uint64_t> input(values.begin(), values.end());
  EncodedCol encoded;
  switch (scheme) {
    case Scheme::kPlain:
      encoded = PlainNumeric(input);
      break;
    case Scheme::kXorFor:
      encoded = EncXorF64(input);
      break;
    case Scheme::kGorilla:
      encoded = EncGorillaF64(input);
      break;
    case Scheme::kChimp:
      encoded = EncChimpF64(input);
      break;
    case Scheme::kXorForC:
      encoded = EncXorForCF64(input);
      break;
    case Scheme::kGorillaC:
      encoded = EncGorillaCF64(input);
      break;
    case Scheme::kChimpC:
      encoded = EncChimpCF64(input);
      break;
    case Scheme::kChimp128:
      encoded = EncChimp128F64(input);
      break;
    case Scheme::kAlpRdTans:
      encoded = EncAlpRdTansF64(input);
      break;
    case Scheme::kPatas:
      encoded = EncPatasF64(input);
      break;
    case Scheme::kAlp:
      encoded = EncAlpF64(input);
      break;
    case Scheme::kSfor:
      // Sorted non-negative monotone doubles only; the encoder declines
      // anything else and the caller falls back to std::nullopt.
      encoded = EncSforF64(input);
      break;
    case Scheme::kExpFor:
      // Any finite double; NaN/Inf decline and the caller falls back.
      encoded = EncExpForF64(input);
      break;
    case Scheme::kFreq:
    case Scheme::kTopk:
    case Scheme::kRleX:
    case Scheme::kDictX: {
      // The composites operate on raw bit patterns; bitcast the doubles.
      std::vector<int64_t> ints;
      ints.reserve(input.size());
      for (uint64_t b : input) {
        ints.push_back(static_cast<int64_t>(b));
      }
      switch (scheme) {
        case Scheme::kFreq:
          encoded = EncFreqI64(ints);
          break;
        case Scheme::kTopk:
          encoded = EncTopKI64(ints);
          break;
        case Scheme::kRleX:
          encoded = EncRleXI64(ints);
          break;
        default:
          encoded = EncDictXI64(ints);
          break;
      }
      break;
    }
    default:
      return std::nullopt;
  }
  return WrapEncoded(std::move(encoded), static_cast<uint32_t>(input.size()),
                     ValueKind::kDoubleBits);
}

std::optional<EncodedBlock> EncodeVarcharBlock(
    Scheme scheme, std::span<const std::string> values) {
  if (values.empty() || values.size() > UINT32_MAX) {
    return std::nullopt;
  }
  // These codecs were designed for physical row groups and may carry u16
  // counts/positions internally. Until a codec certifies a wider envelope,
  // decline large experimental blocks instead of emitting truncated headers.
  if (values.size() > UINT16_MAX && (scheme != Scheme::kPlain)) {
    return std::nullopt;
  }
  EncodedCol encoded;
  switch (scheme) {
    case Scheme::kPlain:
      encoded.scheme = Scheme::kPlain;
      for (const std::string& value : values) {
        AppendBE32(encoded.buf, static_cast<uint32_t>(value.size()));
        encoded.buf.insert(encoded.buf.end(), value.begin(), value.end());
      }
      break;
    case Scheme::kFsst: {
      std::vector<std::string_view> views;
      views.reserve(values.size());
      for (const std::string& value : values) {
        views.push_back(value);
      }
      encoded = EncFsstStr(views);
      break;
    }
    case Scheme::kFrontCode: {
      std::vector<std::string_view> views;
      views.reserve(values.size());
      for (const std::string& value : values) {
        views.push_back(value);
      }
      encoded = EncFrontCodeStr(views);
      break;
    }
    case Scheme::kDictX: {
      std::vector<std::string_view> views;
      views.reserve(values.size());
      for (const std::string& value : values) {
        views.push_back(value);
      }
      encoded = EncDictXStr(views);
      break;
    }
    case Scheme::kTemplate: {
      std::vector<std::string_view> views;
      views.reserve(values.size());
      for (const std::string& value : values) {
        views.push_back(value);
      }
      encoded = EncTemplateStr(views);
      break;
    }
    case Scheme::kDStream: {
      std::vector<std::string_view> views;
      views.reserve(values.size());
      for (const std::string& value : values) {
        views.push_back(value);
      }
      encoded = EncDStreamStr(views);
      break;
    }
    default:
      return std::nullopt;
  }
  return WrapEncoded(std::move(encoded), static_cast<uint32_t>(values.size()),
                     ValueKind::kVarchar);
}

int64_t DecodeInt64At(const EncodedBlock& block, uint32_t index) {
  if (block.kind == ValueKind::kVarchar || index >= block.row_count ||
      block.bytes.empty()) {
    return 0;
  }
  return Int64RegionAt(block.bytes.data(), block.scheme, block.param,
                       block.row_count, index,
                       block.bytes.data() + block.bytes.size());
}

void DecodeInt64All(const EncodedBlock& block, std::span<int64_t> output) {
  const uint32_t n = block.row_count;
  if (block.kind == ValueKind::kVarchar || output.size() < n || n == 0 ||
      block.bytes.empty()) {
    return;
  }
  const char* const region = block.bytes.data();
  const char* const region_end = region + block.bytes.size();

  if (block.scheme == Scheme::kRle) {
    uint32_t row = 0;
    const char* p = region + 4;
    for (uint32_t run = 0; run < block.param && row < n; ++run) {
      if (p + 12 > region_end) {
        break;
      }
      const auto value = static_cast<int64_t>(ReadBE64(p));
      const uint32_t len = ReadBE32(p + 8);
      const uint32_t end = std::min<uint32_t>(n, row + len);
      std::fill(output.begin() + row, output.begin() + end, value);
      row = end;
      p += 12;
    }
    std::fill(output.begin() + row, output.begin() + n, 0);
    return;
  }

  if (block.scheme == Scheme::kRle8) {
    if (!DecodeRle8All(region, n, block.param, region_end, output.data())) {
      std::fill(output.begin(), output.begin() + n, 0);
    }
    return;
  }

  if (block.scheme == Scheme::kRleDelta) {
    if (!DecodeRleDeltaAll(region, n, block.param, region_end, output.data())) {
      std::fill(output.begin(), output.begin() + n, 0);
    }
    return;
  }

  if (block.scheme == Scheme::kRleDict) {
    // [d u16][dict d x 8B][runs u32][{code cb B}{len u32}] x runs. Single
    // pass over the run table, filling dict[code] per run (clamped codes
    // decode as raw 0, rows past the run walk as 0 -- mirrors kRle above).
    if (region + 2 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const uint16_t d = ReadBE16(region);
    if (d == 0) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const int cb = (BitsNeeded(d - 1) + 7) / 8;
    const char* dict = region + 2;
    const char* runs_hdr = dict + (static_cast<size_t>(d) * 8);
    if (runs_hdr + 4 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const uint32_t runs = ReadBE32(runs_hdr);
    if (runs_hdr + 4 +
            (static_cast<size_t>(runs) * (static_cast<size_t>(cb) + 4)) >
        region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    uint32_t row = 0;
    const char* q = runs_hdr + 4;
    for (uint32_t run = 0; run < runs && row < n; ++run) {
      uint64_t code = 0;
      for (int b = 0; b < cb; ++b) {
        code = (code << 8) | static_cast<uint64_t>(static_cast<uint8_t>(q[b]));
      }
      const int64_t value =
          (code >= d) ? 0
                      : static_cast<int64_t>(
                            ReadBE64(dict + (static_cast<size_t>(code) * 8)));
      const uint32_t len = ReadBE32(q + cb);
      const uint32_t end = std::min<uint32_t>(n, row + len);
      std::fill(output.begin() + row, output.begin() + end, value);
      row = end;
      q += static_cast<size_t>(cb) + 4;
    }
    std::fill(output.begin() + row, output.begin() + n, 0);
    return;
  }

  if (block.scheme == Scheme::kXorFor) {
    uint64_t value = ReadBE64(region);
    output[0] = static_cast<int64_t>(value);
    for (uint32_t i = 1; i < n; ++i) {
      value ^=
          UnpackBitAt(region + 8, static_cast<uint64_t>(i - 1) * block.param,
                      static_cast<int>(block.param));
      output[i] = static_cast<int64_t>(value);
    }
    return;
  }

  if (block.scheme == Scheme::kDeltaFp) {
    const uint32_t checkpoints =
        (n + kDeltaCheckpointEvery - 1) / kDeltaCheckpointEvery;
    const char* packed = region + 8 + (static_cast<size_t>(checkpoints) * 8);
    if (packed > region_end || block.param > 64) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    uint64_t value = ReadBE64(region);
    output[0] = static_cast<int64_t>(value);
    for (uint32_t i = 1; i < n; ++i) {
      value += UnpackBitAt(packed, static_cast<uint64_t>(i - 1) * block.param,
                           static_cast<int>(block.param));
      output[i] = static_cast<int64_t>(value);
    }
    return;
  }

  if (block.scheme == Scheme::kDeltaFor ||
      block.scheme == Scheme::kDeltaBitweave) {
    if (region + 2 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const uint32_t checkpoints = ReadBE16(region);
    const char* checkpoint = region + 2;
    const char* inner = checkpoint + (static_cast<size_t>(checkpoints) * 8);
    if (checkpoints == 0 || inner > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const auto delta_base = static_cast<int64_t>(ReadBE64(inner));
    const int bit_width = block.scheme == Scheme::kDeltaBitweave
                              ? static_cast<int>(static_cast<uint8_t>(inner[8]))
                              : 0;
    uint64_t value = ReadBE64(checkpoint);
    output[0] = static_cast<int64_t>(value);
    for (uint32_t i = 1; i < n; ++i) {
      uint64_t delta_offset = 0;
      if (block.scheme == Scheme::kDeltaFor) {
        const auto* bytes = reinterpret_cast<const unsigned char*>(
            inner + 8 + (static_cast<size_t>(i) * block.param));
        for (uint32_t byte = 0; byte < block.param; ++byte) {
          delta_offset |= static_cast<uint64_t>(bytes[byte]) << (byte * 8U);
        }
      } else {
        delta_offset = UnpackBitAt(
            inner + 9,
            static_cast<uint64_t>(i) * static_cast<uint64_t>(bit_width),
            bit_width);
      }
      const uint64_t delta = static_cast<uint64_t>(delta_base) + delta_offset;
      if (i % kDeltaCheckpointEvery == 0) {
        value = ReadBE64(checkpoint +
                         (static_cast<size_t>(i / kDeltaCheckpointEvery) * 8));
      } else {
        value += delta;
      }
      output[i] = static_cast<int64_t>(value);
    }
    return;
  }

  if (block.scheme == Scheme::kDdRice) {
    if (region + 2 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const uint32_t checkpoints = ReadBE16(region);
    const char* checkpoint = region + 2;
    const char* parameter =
        region + 2 + (static_cast<size_t>(checkpoints) * 20);
    if (checkpoints == 0 || parameter >= region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const int rice_k = static_cast<int>(static_cast<uint8_t>(*parameter));
    BitReader reader(parameter + 1, region_end);
    uint64_t value = ReadBE64(checkpoint);
    uint64_t delta = ReadBE64(checkpoint + 8);
    output[0] = static_cast<int64_t>(value);
    for (uint32_t i = 1; i < n; ++i) {
      uint64_t quotient = 0;
      while (reader.get1() != 0) {
        if (++quotient > kRiceQMax) {
          std::fill(output.begin() + i, output.begin() + n, 0);
          return;
        }
      }
      const uint64_t remainder = reader.get(rice_k);
      const uint64_t zigzag =
          rice_k >= 64 ? remainder : ((quotient << rice_k) | remainder);
      delta += (zigzag >> 1) ^ (0ULL - (zigzag & 1ULL));
      value += delta;
      output[i] = static_cast<int64_t>(value);
    }
    return;
  }

  if (block.scheme == Scheme::kDeltaVarint) {
    if (region + 2 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const uint32_t checkpoints = ReadBE16(region);
    const char* checkpoint = region + 2;
    const char* cursor =
        region + 2 + (static_cast<size_t>(checkpoints) * 12) + 1;
    if (checkpoints == 0 || cursor > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    uint64_t value = ReadBE64(checkpoint);
    output[0] = static_cast<int64_t>(value);
    for (uint32_t i = 1; i < n; ++i) {
      uint64_t zigzag = 0;
      bool complete = false;
      for (int byte = 0; byte < 10 && cursor < region_end; ++byte) {
        const auto current = static_cast<uint8_t>(*cursor++);
        zigzag |= static_cast<uint64_t>(current & 0x7F) << (7 * byte);
        if ((current & 0x80) == 0) {
          complete = true;
          break;
        }
      }
      if (!complete) {
        std::fill(output.begin() + i, output.begin() + n, 0);
        return;
      }
      value += (zigzag >> 1) ^ (0ULL - (zigzag & 1ULL));
      output[i] = static_cast<int64_t>(value);
    }
    return;
  }

  if (block.scheme == Scheme::kGorilla) {
    uint64_t previous = ReadBE64(region);
    output[0] = static_cast<int64_t>(previous);
    BitReader reader(region + 8, region_end);
    for (uint32_t i = 1; i < n; ++i) {
      if (reader.get1() != 0) {
        const int leading = static_cast<int>(reader.get(5));
        const int trailing = static_cast<int>(reader.get(6));
        const int width = std::clamp(64 - leading - trailing, 0, 64);
        const uint64_t middle = reader.get(width);
        previous ^= width >= 64 ? middle : (middle << trailing);
      }
      output[i] = static_cast<int64_t>(previous);
    }
    return;
  }

  if (block.scheme == Scheme::kChimp) {
    uint64_t previous = ReadBE64(region);
    output[0] = static_cast<int64_t>(previous);
    int previous_leading = 0;
    int previous_trailing = 64;
    BitReader reader(region + 8, region_end);
    for (uint32_t i = 1; i < n; ++i) {
      if (reader.get1() != 0) {
        if (reader.get1() == 0) {
          const int width = 64 - previous_leading - previous_trailing;
          if (width > 0 && width <= 64) {
            const uint64_t middle = reader.get(width);
            previous ^= middle << previous_trailing;
          }
        } else {
          const int leading = static_cast<int>(reader.get(5));
          const int trailing = static_cast<int>(reader.get(6));
          const int width = std::clamp(64 - leading - trailing, 1, 64);
          const uint64_t middle = reader.get(width);
          previous ^= width >= 64 ? middle : (middle << trailing);
          previous_leading = leading;
          previous_trailing = trailing;
        }
      }
      output[i] = static_cast<int64_t>(previous);
    }
    return;
  }

  if (block.scheme == Scheme::kPBitS) {
    PBitSRegion p;
    if (!ParsePBitSRegion(region, n, block.param, region_end, &p)) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    DecodePBitSAll(p, output.data());
    return;
  }

  if (block.scheme == Scheme::kPfor) {
    if (region + 11 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const auto base = static_cast<int64_t>(ReadBE64(region));
    const int width = static_cast<int>(static_cast<uint8_t>(region[8]));
    const uint32_t exceptions = ReadBE16(region + 9);
    const char* packed = region + 11;
    for (uint32_t i = 0; i < n; ++i) {
      const uint64_t offset = UnpackBitAt(
          packed, static_cast<uint64_t>(i) * static_cast<uint64_t>(width),
          width);
      output[i] = static_cast<int64_t>(static_cast<uint64_t>(base) + offset);
    }
    const size_t packed_bytes =
        (static_cast<size_t>(n) * static_cast<size_t>(width) + 7) / 8;
    const char* exception = packed + packed_bytes;
    for (uint32_t i = 0; i < exceptions && exception + 10 <= region_end; ++i) {
      const uint32_t position = ReadBE16(exception);
      if (position < n) {
        output[position] = static_cast<int64_t>(ReadBE64(exception + 2));
      }
      exception += 10;
    }
    return;
  }

  if (block.scheme == Scheme::kWavelet) {
    if (region + 9 > region_end) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    const auto minimum = static_cast<int64_t>(ReadBE64(region));
    const int width = static_cast<int>(static_cast<uint8_t>(region[8]));
    const size_t stride = (static_cast<size_t>(n) + 7) / 8;
    const char* bitmaps = region + 9;
    if (width <= 0 || width > 64 ||
        bitmaps + (static_cast<size_t>(width) * stride) > region_end) {
      std::fill(output.begin(), output.begin() + n, minimum);
      return;
    }
    std::vector<uint32_t> cumulative(static_cast<size_t>(width) * (stride + 1));
    const auto tail_mask =
        static_cast<uint8_t>((n % 8 == 0) ? 0xFFU : (0xFFU << (8U - (n % 8))));
    for (int level = 0; level < width; ++level) {
      const auto* bitmap = reinterpret_cast<const unsigned char*>(
          bitmaps + (static_cast<size_t>(level) * stride));
      uint32_t* prefix = &cumulative[static_cast<size_t>(level) * (stride + 1)];
      prefix[0] = 0;
      for (size_t byte = 0; byte < stride; ++byte) {
        const unsigned value =
            (byte + 1 == stride) ? (bitmap[byte] & tail_mask) : bitmap[byte];
        prefix[byte + 1] =
            prefix[byte] + static_cast<uint32_t>(__builtin_popcount(value));
      }
    }
    const auto rank_one = [&](uint32_t level, uint32_t position) {
      const uint32_t* prefix =
          &cumulative[static_cast<size_t>(level) * (stride + 1)];
      const uint32_t byte = position >> 3;
      const uint32_t bit = position & 7U;
      uint32_t rank = prefix[byte];
      if (bit != 0) {
        const auto* bitmap = reinterpret_cast<const unsigned char*>(
            bitmaps + (static_cast<size_t>(level) * stride));
        const auto mask = static_cast<uint8_t>(0xFFU << (8U - bit));
        rank += static_cast<uint32_t>(
            __builtin_popcount(static_cast<unsigned>(bitmap[byte] & mask)));
      }
      return rank;
    };
    for (uint32_t i = 0; i < n; ++i) {
      uint32_t position = i;
      uint64_t value = 0;
      for (int level = 0; level < width; ++level) {
        const auto* bitmap = reinterpret_cast<const unsigned char*>(
            bitmaps + (static_cast<size_t>(level) * stride));
        const uint32_t bit = (static_cast<uint32_t>(bitmap[position >> 3]) >>
                              (7U - (position & 7U))) &
                             1U;
        value = (value << 1) | bit;
        const uint32_t ones_before =
            rank_one(static_cast<uint32_t>(level), position);
        if (bit == 0) {
          position -= ones_before;
        } else {
          const uint32_t total_ones =
              cumulative[(static_cast<size_t>(level) * (stride + 1)) + stride];
          position = (n - total_ones) + ones_before;
        }
      }
      output[i] = static_cast<int64_t>(static_cast<uint64_t>(minimum) + value);
    }
    return;
  }

  if (block.scheme == Scheme::kWaveletR) {
    WaveletRRegion p;
    if (!ParseWaveletRRegion(region, n, block.param, region_end, &p)) {
      std::fill(output.begin(), output.begin() + n, 0);
      return;
    }
    for (uint32_t i = 0; i < n; ++i) {
      output[i] = static_cast<int64_t>(static_cast<uint64_t>(p.minimum) +
                                       WaveletRValueAt(p, i));
    }
    return;
  }

  if (DecodeCascadeI64All(region, block.scheme, block.param, n, region_end,
                          output.data())) {
    return;
  }

  for (uint32_t i = 0; i < n; ++i) {
    output[i] =
        Int64RegionAt(region, block.scheme, block.param, n, i, region_end);
  }
}

uint32_t LowerBoundInt64(const EncodedBlock& block, int64_t value) {
  if (block.kind == ValueKind::kVarchar || block.bytes.empty()) {
    return block.row_count;
  }
  uint32_t position = block.row_count;
  (void)RegionLowerBound(block.bytes.data(), block.scheme, block.param,
                         block.row_count, value, &position,
                         block.bytes.data() + block.bytes.size());
  return position;
}

std::string DecodeVarcharAt(const EncodedBlock& block, uint32_t index) {
  if (block.kind != ValueKind::kVarchar || index >= block.row_count ||
      block.bytes.empty()) {
    return {};
  }
  Value value = VarcharRegionAt(block.bytes.data(), block.scheme, block.param,
                                block.row_count, index,
                                block.bytes.data() + block.bytes.size());
  if (value.type != ValueType::kVarChar) {
    return {};
  }
  return std::string(value.AsStringView());
}

uint32_t LowerBoundVarchar(const EncodedBlock& block, std::string_view value) {
  if (block.kind != ValueKind::kVarchar) {
    return block.row_count;
  }
  uint32_t low = 0;
  uint32_t high = block.row_count;
  while (low < high) {
    const uint32_t middle = low + ((high - low) / 2);
    if (DecodeVarcharAt(block, middle) < value) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
}

bool VarcharRangeFold(const EncodedBlock& block, std::string_view low,
                      std::string_view high, uint32_t row_offset,
                      std::span<uint64_t> output_mask, uint64_t* count_out) {
  if (count_out == nullptr || block.kind != ValueKind::kVarchar ||
      block.bytes.empty() ||
      (!output_mask.empty() &&
       static_cast<uint64_t>(row_offset) + block.row_count >
           output_mask.size() * 64ULL)) {
    return false;
  }
  const char* region = block.bytes.data();
  const char* region_end = region + block.bytes.size();
  uint64_t count = 0;
  const auto match = [&](uint32_t row, std::string_view value) {
    if (value < low || value >= high) {
      return;
    }
    ++count;
    if (!output_mask.empty()) {
      const uint64_t output_row = static_cast<uint64_t>(row_offset) + row;
      output_mask[output_row >> 6] |= 1ULL << (output_row & 63ULL);
    }
  };

  if (block.scheme == Scheme::kPlain) {
    const char* cursor = region;
    for (uint32_t row = 0; row < block.row_count; ++row) {
      if (cursor + 4 > region_end) {
        return false;
      }
      const uint32_t length = ReadBE32(cursor);
      cursor += 4;
      if (cursor + length > region_end) {
        return false;
      }
      match(row, std::string_view(cursor, length));
      cursor += length;
    }
  } else if (block.scheme == Scheme::kFsst) {
    if (region + 6 > region_end) {
      return false;
    }
    const uint16_t symbol_count = ReadBE16(region);
    if (symbol_count > 256) {
      return false;
    }
    const uint32_t stream_length = ReadBE32(region + 2);
    const char* cursor = region + 6;
    std::vector<std::string_view> symbols(symbol_count);
    for (uint16_t symbol = 0; symbol < symbol_count; ++symbol) {
      if (cursor + 1 > region_end) {
        return false;
      }
      const auto length = static_cast<uint8_t>(*cursor++);
      if (cursor + length > region_end) {
        return false;
      }
      symbols[symbol] = std::string_view(cursor, length);
      cursor += length;
    }
    const char* offsets = cursor;
    if (offsets + (static_cast<size_t>(block.row_count) * 4) > region_end) {
      return false;
    }
    const char* stream = offsets + (static_cast<size_t>(block.row_count) * 4);
    if (stream + stream_length > region_end) {
      return false;
    }
    std::string scratch;
    for (uint32_t row = 0; row < block.row_count; ++row) {
      const uint32_t begin = ReadBE32(offsets + (static_cast<size_t>(row) * 4));
      const uint32_t end =
          row + 1 < block.row_count
              ? ReadBE32(offsets + ((static_cast<size_t>(row) + 1) * 4))
              : stream_length;
      if (begin > end || end > stream_length) {
        return false;
      }
      scratch.clear();
      const auto* code = reinterpret_cast<const uint8_t*>(stream + begin);
      const auto* code_end = reinterpret_cast<const uint8_t*>(stream + end);
      while (code < code_end) {
        const uint8_t symbol = *code++;
        if (symbol == 0) {
          if (code >= code_end) {
            return false;
          }
          scratch.push_back(static_cast<char>(*code++));
        } else if (symbol <= symbol_count) {
          scratch.append(symbols[symbol - 1]);
        }
      }
      match(row, scratch);
    }
  } else if (block.scheme == Scheme::kFrontCode) {
    const uint32_t restart_every = block.param & 0x7FFFU;
    if (restart_every == 0 || region + 6 > region_end) {
      return false;
    }
    const uint16_t restart_count = ReadBE16(region);
    const uint32_t stream_length = ReadBE32(region + 2);
    const char* restart_table = region + 6;
    if (restart_table + (static_cast<size_t>(restart_count) * 4) > region_end) {
      return false;
    }
    const char* cursor =
        restart_table + (static_cast<size_t>(restart_count) * 4);
    const char* stream_end = cursor + stream_length;
    if (stream_end > region_end) {
      return false;
    }
    std::string current;
    for (uint32_t row = 0; row < block.row_count; ++row) {
      if (cursor + 4 > stream_end) {
        return false;
      }
      const uint16_t prefix_length = ReadBE16(cursor);
      const uint16_t suffix_length = ReadBE16(cursor + 2);
      cursor += 4;
      if (prefix_length > current.size() ||
          cursor + suffix_length > stream_end) {
        return false;
      }
      current.resize(prefix_length);
      current.append(cursor, suffix_length);
      cursor += suffix_length;
      match(row, current);
    }
  } else {
    return false;
  }
  *count_out = count;
  return true;
}

}  // namespace codec_benchmark

}  // namespace tinylamb
