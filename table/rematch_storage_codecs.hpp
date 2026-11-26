/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TABLE_REMATCH_STORAGE_CODECS_HPP
#define TINYLAMB_TABLE_REMATCH_STORAGE_CODECS_HPP

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tinylamb::rematch_storage {

// Zigzag + ULEB128 wire primitives shared by QueryRewrites.ColumnCodecs proofs.
[[nodiscard]] inline uint64_t ZigZagEncode(int64_t v) noexcept {
  return (static_cast<uint64_t>(v) << 1U) ^ static_cast<uint64_t>(v >> 63U);
}

[[nodiscard]] inline int64_t ZigZagDecode(uint64_t u) noexcept {
  return static_cast<int64_t>((u >> 1U) ^ (~(u & 1ULL) + 1ULL));
}

inline void AppendUleb128(uint64_t v, std::vector<uint8_t>* out) {
  do {
    auto byte = static_cast<uint8_t>(v & 0x7FU);
    v >>= 7U;
    if (v != 0) {
      byte |= 0x80U;
    }
    out->push_back(byte);
  } while (v != 0);
}

[[nodiscard]] inline bool ReadUleb128(std::span<const uint8_t> buf, size_t* pos,
                                      uint64_t* out) noexcept {
  uint64_t result = 0;
  unsigned shift = 0;
  while (*pos < buf.size() && shift < 64U) {
    const uint8_t byte = buf[(*pos)++];
    result |= static_cast<uint64_t>(byte & 0x7FU) << shift;
    if ((byte & 0x80U) == 0U) {
      *out = result;
      return true;
    }
    shift += 7U;
  }
  return false;
}

// ============================================================================
// 1. QueryRewrites.ColumnCodecs.SignedDateDeltaRoundtrip.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/SignedDateDeltaRoundtrip.lean
// (sound) Anchor commitdate and receiptdate as zigzag-ULEB128 deltas from
// shipdate.
// ============================================================================
inline void EncodeShipAnchoredDates(int64_t ship, int64_t commit,
                                    int64_t receipt,
                                    std::vector<uint8_t>* out) {
  AppendUleb128(ZigZagEncode(commit - ship), out);
  AppendUleb128(ZigZagEncode(receipt - ship), out);
}

[[nodiscard]] inline bool DecodeShipAnchoredDates(
    int64_t ship, std::span<const uint8_t> buf, size_t* pos,
    int64_t* commit_out, int64_t* receipt_out) noexcept {
  uint64_t a = 0;
  uint64_t b = 0;
  if (!ReadUleb128(buf, pos, &a) || !ReadUleb128(buf, pos, &b)) {
    return false;
  }
  *commit_out = ship + ZigZagDecode(a);
  *receipt_out = ship + ZigZagDecode(b);
  return true;
}

// ============================================================================
// 2. QueryRewrites.ColumnCodecs.RunEndOrderKeyLookup.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/RunEndOrderKeyLookup.lean
// (sound) Run-end indexed RLE lookup for sorted orderkey runs.
// ============================================================================
struct RunEndIndex {
  std::vector<size_t> ends;
  std::vector<uint64_t> keys;

  void Build(std::span<const std::pair<uint64_t, size_t>> runs) {
    ends.clear();
    keys.clear();
    ends.reserve(runs.size());
    keys.reserve(runs.size());
    size_t acc = 0;
    for (const auto& [key, len] : runs) {
      acc += len;
      ends.push_back(acc);
      keys.push_back(key);
    }
  }

  [[nodiscard]] std::optional<uint64_t> Lookup(size_t row_idx) const noexcept {
    const auto it = std::ranges::upper_bound(ends, row_idx);
    if (it == ends.end()) {
      return std::nullopt;
    }
    return keys[static_cast<size_t>(it - ends.begin())];
  }
};

// ============================================================================
// 3. QueryRewrites.ColumnCodecs.GridKeysWithSparseExceptions.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/GridKeysWithSparseExceptions.lean
// (sound) Implicit `(item, warehouse)` grid `((start+i)/W, (start+i)%W)` +
// exceptions.
// ============================================================================
struct GridWithExceptions {
  uint64_t start{0};
  uint64_t warehouses{1};
  std::vector<std::pair<size_t, std::pair<uint64_t, uint64_t>>> exceptions;

  [[nodiscard]] std::pair<uint64_t, uint64_t> At(size_t i) const noexcept {
    for (const auto& [idx, val] : exceptions) {
      if (idx == i) {
        return val;
      }
    }
    const uint64_t pos = start + i;
    return {pos / warehouses, pos % warehouses};
  }
};

// ============================================================================
// 4. QueryRewrites.ColumnCodecs.NullableRankIndexedResiduals.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/NullableRankIndexedResiduals.lean
// (sound) Separate nullable key column into validity mask + base-anchored dense
// stream indexed by prefix popcount rank.
// ============================================================================
struct RankIndexedNullableColumn {
  uint64_t base{0};
  std::vector<uint8_t> valid_mask;
  std::vector<uint64_t> residuals;

  void Build(std::span<const std::optional<uint64_t>> rows, uint64_t min_base) {
    base = min_base;
    valid_mask.assign(rows.size(), 0);
    residuals.clear();
    for (size_t i = 0; i < rows.size(); ++i) {
      if (const std::optional<uint64_t>& cell = rows[i]; cell.has_value()) {
        valid_mask[i] = 1;
        residuals.push_back(*cell - base);
      }
    }
  }

  [[nodiscard]] std::optional<uint64_t> At(size_t i) const noexcept {
    if (i >= valid_mask.size() || valid_mask[i] == 0U) {
      return std::nullopt;
    }
    size_t rank = 0;
    for (size_t j = 0; j < i; ++j) {
      rank += valid_mask[j];
    }
    return base + residuals[rank];
  }
};

// ============================================================================
// 5. QueryRewrites.ColumnCodecs.SparseOffsetsWithLocalLengthLookup.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/SparseOffsetsWithLocalLengthLookup.lean
// (sound) Strided anchor offsets + per-row lengths for random string point
// access.
// ============================================================================
struct StridedStringColumn {
  size_t stride{64};
  std::vector<size_t> anchors;
  std::vector<uint32_t> lengths;
  std::string payload;

  void Build(std::span<const std::string_view> rows, size_t anchor_stride) {
    stride = std::max<size_t>(1, anchor_stride);
    anchors.clear();
    lengths.clear();
    payload.clear();
    lengths.reserve(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      if (i % stride == 0) {
        anchors.push_back(payload.size());
      }
      lengths.push_back(static_cast<uint32_t>(rows[i].size()));
      payload.append(rows[i]);
    }
  }

  [[nodiscard]] std::string_view At(size_t i) const noexcept {
    const size_t block = i / stride;
    size_t off = anchors[block];
    for (size_t j = block * stride; j < i; ++j) {
      off += lengths[j];
    }
    return {payload.data() + off, lengths[i]};
  }
};

// ============================================================================
// 6. QueryRewrites.ColumnCodecs.MedianClockResidualRoundtrip.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/MedianClockResidualRoundtrip.lean
// (sound) 3-clock per-row median anchor + signed residuals.
// ============================================================================
[[nodiscard]] inline int64_t MedianOfThree(int64_t a, int64_t b,
                                           int64_t c) noexcept {
  std::array<int64_t, 3> v = {a, b, c};
  std::ranges::sort(v);
  return v[1];
}

// ============================================================================
// 7. QueryRewrites.ColumnCodecs.ByteLaneTransposeRoundtrip.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/ByteLaneTransposeRoundtrip.lean
// (sound) Byte-lane transposition by `pos % width` for better entropy
// compression.
// ============================================================================
[[nodiscard]] inline std::vector<uint8_t> TransposeByteLanes(
    std::span<const uint8_t> page, size_t width) {
  if (width == 0) {
    return {};
  }
  std::vector<uint8_t> out;
  out.reserve(page.size());
  for (size_t lane = 0; lane < width; ++lane) {
    for (size_t i = lane; i < page.size(); i += width) {
      out.push_back(page[i]);
    }
  }
  return out;
}

[[nodiscard]] inline uint8_t ReadTransposedByte(
    std::span<const uint8_t> transposed, size_t page_len, size_t width,
    size_t i) noexcept {
  const size_t lane = i % width;
  size_t lane_begin = 0;
  for (size_t l = 0; l < lane; ++l) {
    lane_begin += (page_len > l) ? (((page_len - 1 - l) / width) + 1) : 0;
  }
  return transposed[lane_begin + (i / width)];
}

// ============================================================================
// 8. QueryRewrites.ColumnCodecs.IndexedFrameRangeRead.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/IndexedFrameRangeRead.lean
// (sound) Independent frame span reader over logical byte offsets.
// ============================================================================
struct CompressedFrameEntry {
  size_t logical_start{0};
  size_t logical_len{0};
  std::vector<uint8_t> encoded;
};

template <typename DecodeFn>
[[nodiscard]] std::vector<uint8_t> ReadLogicalFrameSpan(
    std::span<const CompressedFrameEntry> frames, size_t start, size_t len,
    DecodeFn&& decode) {
  std::vector<uint8_t> out;
  out.reserve(len);
  const size_t end = start + len;
  for (const CompressedFrameEntry& f : frames) {
    if (f.logical_start + f.logical_len <= start || end <= f.logical_start) {
      continue;
    }
    const std::vector<uint8_t> raw = decode(f.encoded);
    const size_t lo = (start > f.logical_start) ? (start - f.logical_start) : 0;
    const size_t hi = std::min(f.logical_len, end - f.logical_start);
    for (size_t i = lo; i < hi; ++i) {
      out.push_back(raw[i]);
    }
  }
  return out;
}

// ============================================================================
// 9. QueryRewrites.ColumnCodecs.SharedUrlPrefixRoundtrip.Goal
// Lean proof:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/ColumnCodecs/SharedUrlPrefixRoundtrip.lean
// (sound) Encode `original` against `url` using common prefix length + suffix
// frame.
// ============================================================================
inline void EncodeCommonPrefixResidual(std::string_view url,
                                       std::string_view original,
                                       std::vector<uint8_t>* out) {
  size_t prefix = 0;
  const size_t lim = std::min(url.size(), original.size());
  while (prefix < lim && url[prefix] == original[prefix]) {
    ++prefix;
  }
  const std::string_view suffix = original.substr(prefix);
  AppendUleb128(prefix, out);
  AppendUleb128(suffix.size(), out);
  for (const char ch : suffix) {
    out->push_back(static_cast<uint8_t>(ch));
  }
}

[[nodiscard]] inline bool DecodeCommonPrefixResidual(
    std::string_view url, std::span<const uint8_t> buf, size_t* pos,
    std::string* original_out) {
  uint64_t prefix = 0;
  uint64_t suffix_len = 0;
  if (!ReadUleb128(buf, pos, &prefix) || !ReadUleb128(buf, pos, &suffix_len) ||
      prefix > url.size() || *pos + suffix_len > buf.size()) {
    return false;
  }
  original_out->assign(url.substr(0, static_cast<size_t>(prefix)));
  original_out->append(reinterpret_cast<const char*>(buf.data() + *pos),
                       static_cast<size_t>(suffix_len));
  *pos += static_cast<size_t>(suffix_len);
  return true;
}

}  // namespace tinylamb::rematch_storage

#endif  // TINYLAMB_TABLE_REMATCH_STORAGE_CODECS_HPP
