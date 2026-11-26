/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_CLICKSTREAM_EXEC_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_CLICKSTREAM_EXEC_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// ClickstreamLayout / Execution
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_ClockRange.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_ConcatSearch.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_AnchorDemand.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_SparseMark.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_DefaultGroups.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_LengthRollup.lean
// (all sound)
// ============================================================================

// ClockRange.Goal: decide half-open range `lo <= v < hi` for a dependent
// timestamp `(base + residual)` without decoding the residual whenever the
// residual interval `[dmin, dmax]` lies strictly inside or outside `[lo, hi)`.
enum class ClockSlotKind : uint8_t { kMissing, kDelta, kAbsolute };

struct ClockSlot {
  ClockSlotKind kind{ClockSlotKind::kMissing};
  int64_t payload{0};
};

struct ClockProbeResult {
  bool matched{false};
  bool decoded_dependent{false};
};

[[nodiscard]] inline ClockProbeResult ProbeDependentClockRange(
    int64_t lo, int64_t hi, int64_t dmin, int64_t dmax, int64_t base,
    ClockSlot slot) noexcept {
  switch (slot.kind) {
    case ClockSlotKind::kMissing:
      return {.matched = false, .decoded_dependent = false};
    case ClockSlotKind::kAbsolute:
      return {.matched = (lo <= slot.payload && slot.payload < hi),
              .decoded_dependent = true};
    case ClockSlotKind::kDelta:
      if (base + dmax < lo || hi <= base + dmin) {
        return {.matched = false, .decoded_dependent = false};
      }
      if (lo <= base + dmin && base + dmax < hi) {
        return {.matched = true, .decoded_dependent = false};
      }
      {
        const int64_t v = base + slot.payload;
        return {.matched = (lo <= v && v < hi), .decoded_dependent = true};
      }
  }
  return {};
}

// ConcatSearch.Goal: search one concatenated byte buffer for `needle` and
// attribute hits to row IDs via cumulative `ends`, rejecting cross-row matches
// and preserving ascending unique row order.
[[nodiscard]] inline std::vector<size_t> SearchConcatStreamRowIds(
    std::string_view concat_bytes, std::span<const size_t> ends,
    std::span<const uint8_t> is_null, std::string_view needle) {
  std::vector<size_t> hits;
  if (needle.empty()) {
    for (size_t i = 0; i < ends.size(); ++i) {
      if (i >= is_null.size() || is_null[i] == 0) {
        hits.push_back(i);
      }
    }
    return hits;
  }
  size_t pos = 0;
  while (pos < concat_bytes.size()) {
    const size_t found = concat_bytes.find(needle, pos);
    if (found == std::string_view::npos) {
      break;
    }
    const auto it = std::upper_bound(ends.begin(), ends.end(), found);
    if (it == ends.end()) {
      break;
    }
    const auto row_idx = static_cast<size_t>(it - ends.begin());
    if (found + needle.size() <= *it &&
        (row_idx >= is_null.size() || is_null[row_idx] == 0)) {
      if (hits.empty() || hits.back() != row_idx) {
        hits.push_back(row_idx);
      }
      pos = *it;  // Already matched this row; jump to next row boundary.
    } else {
      pos = found + 1;
    }
  }
  return hits;
}

// AnchorDemand.Goal: compute the least sufficient set of anchor frame IDs
// required to materialize `requested_row_ids` given per-row `copy_base` flags.
template <typename FrameOfFn>
[[nodiscard]] std::vector<size_t> ComputeMinimalAnchorFrames(
    std::span<const uint8_t> copy_base_tags,
    std::span<const size_t> requested_row_ids, FrameOfFn&& frame_of) {
  std::unordered_set<size_t> seen;
  std::vector<size_t> frames;
  for (const size_t id : requested_row_ids) {
    if (id < copy_base_tags.size() && copy_base_tags[id] != 0) {
      const size_t f = frame_of(id);
      if (seen.insert(f).second) {
        frames.push_back(f);
      }
    }
  }
  return frames;
}

// SparseMark.Goal: exact O(1) decision procedure for whether a 2-column
// lex-clustered granule `[first, last]` can intersect `[xl, xh] x [yl, yh]`.
struct LexPairKey {
  uint64_t x{0};
  uint64_t y{0};
  constexpr bool operator==(const LexPairKey&) const = default;
};

[[nodiscard]] constexpr bool LexPairLe(LexPairKey a, LexPairKey b) noexcept {
  return a.x < b.x || (a.x == b.x && a.y <= b.y);
}

[[nodiscard]] constexpr LexPairKey LeastKeyInRectAboveFence(
    LexPairKey fence, uint64_t x_lo, uint64_t y_lo, uint64_t y_hi) noexcept {
  if (fence.x < x_lo) {
    return {.x = x_lo, .y = y_lo};
  }
  if (fence.y <= y_hi) {
    return {.x = fence.x, .y = std::max(y_lo, fence.y)};
  }
  return {.x = fence.x + 1, .y = y_lo};
}

[[nodiscard]] constexpr bool GranuleIntersectsRect(LexPairKey first,
                                                   LexPairKey last,
                                                   uint64_t x_lo, uint64_t x_hi,
                                                   uint64_t y_lo,
                                                   uint64_t y_hi) noexcept {
  if (x_lo > x_hi || y_lo > y_hi) {
    return false;
  }
  const LexPairKey cand = LeastKeyInRectAboveFence(first, x_lo, y_lo, y_hi);
  return cand.x <= x_hi && LexPairLe(cand, last);
}

// DefaultGroups.Goal: compute exact GROUP BY multiplicity of a target tuple
// from sparse deviation lists by visiting only touched row IDs and crediting
// `(row_count - |touched|)` to the all-default tuple in O(1).
struct SparseDefaultLane {
  std::optional<int64_t> default_val;
  std::unordered_map<size_t, std::optional<int64_t>> exceptions;
};

[[nodiscard]] inline size_t CountTupleWithDefaultShortCut(
    std::span<const SparseDefaultLane> lanes, size_t row_count,
    std::span<const std::optional<int64_t>> target_key) {
  if (lanes.size() != target_key.size()) {
    return 0;
  }
  std::unordered_set<size_t> touched;
  for (const auto& lane : lanes) {
    for (const auto& [row_id, _] : lane.exceptions) {
      if (row_id < row_count) {
        touched.insert(row_id);
      }
    }
  }
  size_t count = 0;
  for (const size_t row_id : touched) {
    bool match = true;
    for (size_t c = 0; c < lanes.size(); ++c) {
      const auto it = lanes[c].exceptions.find(row_id);
      const std::optional<int64_t> v =
          it == lanes[c].exceptions.end() ? lanes[c].default_val : it->second;
      if (v != target_key[c]) {
        match = false;
        break;
      }
    }
    if (match) {
      ++count;
    }
  }
  bool target_is_default = true;
  for (size_t c = 0; c < lanes.size(); ++c) {
    if (lanes[c].default_val != target_key[c]) {
      target_is_default = false;
      break;
    }
  }
  if (target_is_default) {
    count += row_count - touched.size();
  }
  return count;
}

// LengthRollup.Goal: compute `(byte_sum, nonempty_count)` across arbitrary row
// slices `(start, len)` directly from cumulative `ends` and `present` prefix
// sums without materializing string payloads (ClickBench Q28).
struct LengthRollupTotals {
  uint64_t byte_sum{0};
  uint64_t nonempty_count{0};
  constexpr bool operator==(const LengthRollupTotals&) const = default;
};

[[nodiscard]] inline LengthRollupTotals RollupNonEmptyLengthsFromPrefixSums(
    std::span<const uint64_t> prefix_ends,
    std::span<const uint64_t> prefix_present, size_t n_rows,
    std::span<const std::pair<size_t, size_t>> slices) noexcept {
  LengthRollupTotals out;
  for (const auto& [start, len] : slices) {
    const size_t lo = std::min(n_rows, start);
    const size_t hi = std::min(n_rows, start + len);
    if (hi < prefix_ends.size() && lo < prefix_ends.size()) {
      out.byte_sum += prefix_ends[hi] - prefix_ends[lo];
    }
    if (hi < prefix_present.size() && lo < prefix_present.size()) {
      out.nonempty_count += prefix_present[hi] - prefix_present[lo];
    }
  }
  return out;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_CLICKSTREAM_EXEC_PROOFS_HPP
