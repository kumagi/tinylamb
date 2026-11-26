/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ENGINE_DECISIONS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ENGINE_DECISIONS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <unordered_set>
#include <utility>
#include <vector>

#include "type/value.hpp"

namespace tinylamb::engine_decisions {

// ============================================================================
// 1. EngineDecisions.Optimizer.TupleAnti
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_TupleAnti.lean
//     (TupleAnti.Goal)
// Lowers 2-column `WHERE (x, y) NOT IN (...)` (3-valued SQL null-aware anti)
// into O(1) presence probes partitioned by NULL pattern.
// ============================================================================
struct ValueKeyHash {
  size_t operator()(const Value& v) const noexcept {
    return std::hash<Value>{}(v);
  }
};

struct ValueKeyEqual {
  bool operator()(const Value& a, const Value& b) const noexcept {
    if (a.IsNull() || b.IsNull()) {
      return a.IsNull() && b.IsNull();
    }
    return a == b;
  }
};

struct ValuePairHash {
  size_t operator()(const std::pair<Value, Value>& p) const noexcept {
    const size_t h1 = std::hash<Value>{}(p.first);
    const size_t h2 = std::hash<Value>{}(p.second);
    return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6U) + (h1 >> 2U));
  }
};

struct ValuePairEqual {
  bool operator()(const std::pair<Value, Value>& a,
                  const std::pair<Value, Value>& b) const noexcept {
    const ValueKeyEqual eq;
    return eq(a.first, b.first) && eq(a.second, b.second);
  }
};

class TupleAntiIndex {
 public:
  void Insert(const Value& a, const Value& b) {
    nonempty_ = true;
    const bool a_null = a.IsNull();
    const bool b_null = b.IsNull();
    if (a_null && b_null) {
      both_null_ = true;
      missing_left_ = true;
      missing_right_ = true;
      return;
    }
    if (a_null) {
      missing_left_ = true;
      right_only_.insert(b);
      rights_.insert(b);
      return;
    }
    if (b_null) {
      missing_right_ = true;
      left_only_.insert(a);
      lefts_.insert(a);
      return;
    }
    lefts_.insert(a);
    rights_.insert(b);
    complete_.emplace(a, b);
  }

  [[nodiscard]] bool Keep(const Value& a, const Value& b) const {
    const bool a_null = a.IsNull();
    const bool b_null = b.IsNull();
    if (a_null && b_null) {
      return !nonempty_;
    }
    if (!a_null && b_null) {
      return !missing_left_ && !lefts_.contains(a);
    }
    if (a_null && !b_null) {
      return !missing_right_ && !rights_.contains(b);
    }
    return !both_null_ && !left_only_.contains(a) && !right_only_.contains(b) &&
           !complete_.contains(std::make_pair(a, b));
  }

  [[nodiscard]] bool Empty() const noexcept { return !nonempty_; }

 private:
  bool nonempty_{false};
  bool both_null_{false};
  bool missing_left_{false};
  bool missing_right_{false};
  std::unordered_set<Value, ValueKeyHash, ValueKeyEqual> left_only_;
  std::unordered_set<Value, ValueKeyHash, ValueKeyEqual> right_only_;
  std::unordered_set<Value, ValueKeyHash, ValueKeyEqual> lefts_;
  std::unordered_set<Value, ValueKeyHash, ValueKeyEqual> rights_;
  std::unordered_set<std::pair<Value, Value>, ValuePairHash, ValuePairEqual>
      complete_;
};

// ============================================================================
// 2. EngineDecisions.Executor.FlushChoice
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Exec_FlushChoice.lean
//     (FlushChoice.Goal)
// Exact DP over staged OOC partitions selecting the minimum-I/O subset that
// reclaims at least `need_units` of memory.
// ============================================================================
struct FlushBucket {
  size_t reclaim_units{0};
  uint64_t io_cost{0};
};

struct FlushPlan {
  uint64_t total_io_cost{0};
  std::vector<size_t> selected_indices;
};

inline std::optional<FlushPlan> SelectFlushSubset(
    std::span<const FlushBucket> buckets, size_t need_units) {
  if (need_units == 0) {
    return FlushPlan{.total_io_cost = 0, .selected_indices = {}};
  }
  const size_t n = buckets.size();
  constexpr uint64_t kInf = std::numeric_limits<uint64_t>::max();
  // dp[i][rem] = min IO cost using buckets[i..n-1] to cover `rem` units.
  const size_t cols = need_units + 1;
  std::vector<uint64_t> dp((n + 1) * cols, kInf);
  std::vector<uint8_t> take(n * cols, 0);
  dp[n * cols] = 0;
  for (size_t i = n; i-- > 0;) {
    dp[i * cols] = 0;
    const size_t b_units = buckets[i].reclaim_units;
    const uint64_t b_io = buckets[i].io_cost;
    for (size_t rem = 1; rem <= need_units; ++rem) {
      const uint64_t skip_cost = dp[((i + 1) * cols) + rem];
      const size_t next_rem = (rem > b_units) ? (rem - b_units) : 0;
      const uint64_t sub = dp[((i + 1) * cols) + next_rem];
      const uint64_t take_cost =
          (sub == kInf || b_io > kInf - sub) ? kInf : (sub + b_io);
      if (take_cost < skip_cost) {
        dp[(i * cols) + rem] = take_cost;
        take[(i * cols) + rem] = 1;
      } else {
        dp[(i * cols) + rem] = skip_cost;
      }
    }
  }
  const uint64_t best = dp[need_units];
  if (best == kInf) {
    return std::nullopt;
  }
  FlushPlan plan;
  plan.total_io_cost = best;
  size_t rem = need_units;
  for (size_t i = 0; i < n && rem > 0; ++i) {
    if (take[(i * cols) + rem] != 0) {
      plan.selected_indices.push_back(i);
      const size_t b_units = buckets[i].reclaim_units;
      rem = (rem > b_units) ? (rem - b_units) : 0;
    }
  }
  return plan;
}

// ============================================================================
// 3. ClickstreamLayout.Execution.LengthRollup
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/LengthSubtotalRollup.lean
//     (LengthRollup.Goal)
// Prefix-sum index over string ends and non-empty ranks for O(1)/slice
// SUM(BYTE_LENGTH) and COUNT(non-empty) without materializing strings.
// ============================================================================
struct LengthRollupSlice {
  uint64_t byte_sum{0};
  uint64_t non_empty_count{0};
};

class LengthRollupIndex {
 public:
  void Build(std::span<const uint32_t> ends, const uint8_t* nulls) {
    const size_t n = ends.size();
    byte_prefix_.assign(n + 1, 0);
    present_prefix_.assign(n + 1, 0);
    uint32_t prev_end = 0;
    for (size_t i = 0; i < n; ++i) {
      const uint32_t cur_end = ends[i];
      const bool is_null = (nulls != nullptr && nulls[i] != 0);
      const uint32_t len =
          (!is_null && cur_end >= prev_end) ? (cur_end - prev_end) : 0U;
      byte_prefix_[i + 1] = byte_prefix_[i] + len;
      present_prefix_[i + 1] = present_prefix_[i] + (len != 0 ? 1ULL : 0ULL);
      prev_end = cur_end;
    }
    row_count_ = n;
  }

  [[nodiscard]] LengthRollupSlice QuerySlice(size_t start,
                                             size_t count) const noexcept {
    const size_t lo = std::min(start, row_count_);
    const size_t hi = std::min(lo + count, row_count_);
    return {.byte_sum = byte_prefix_[hi] - byte_prefix_[lo],
            .non_empty_count = present_prefix_[hi] - present_prefix_[lo]};
  }

 private:
  std::vector<uint64_t> byte_prefix_;
  std::vector<uint64_t> present_prefix_;
  size_t row_count_{0};
};

// ============================================================================
// 4. EngineDecisions.Optimizer.NullableGroupKey
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_NullableGroupKey.lean
//     (NullableGroupKey.Goal)
// Compacts a functionally-dependent GROUP BY pair `(det, dep)` where `det`
// may be NULL: non-NULL `det` keys only on `(det, NULL)` while storing `dep`
// in the group representative; NULL `det` preserves `(NULL, dep)`.
// ============================================================================
inline std::pair<Value, Value> CompactNullableDependentKey(const Value& det,
                                                           const Value& dep) {
  if (!det.IsNull()) {
    return {det, Value()};
  }
  return {det, dep};
}

// ============================================================================
// 5. EngineDecisions.Executor.HotRunTiles
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Exec_HotRunTiles.lean
//     (HotRunTiles.Goal)
// Splits a single hot equal-key merge run (left_count x right_count) in
// pair-ordinal space across workers and reconstructs exact FULL OUTER
// unmatched left/right indices from the union of matched pairs.
// ============================================================================
struct HotRunOuterResult {
  std::vector<std::pair<size_t, size_t>> matched_pairs;
  std::vector<size_t> unmatched_left;
  std::vector<size_t> unmatched_right;
};

inline size_t HotRunTileCount(size_t left_count, size_t right_count,
                              size_t tile_width) noexcept {
  if (tile_width == 0 || left_count == 0 || right_count == 0) {
    return 0;
  }
  const size_t total = left_count * right_count;
  return (total + tile_width - 1) / tile_width;
}

inline std::vector<std::pair<size_t, size_t>> HotRunTilePairs(
    size_t left_count, size_t right_count, size_t tile_width, size_t tile_idx) {
  std::vector<std::pair<size_t, size_t>> out;
  if (tile_width == 0 || left_count == 0 || right_count == 0) {
    return out;
  }
  const size_t total = left_count * right_count;
  const size_t start = tile_idx * tile_width;
  if (start >= total) {
    return out;
  }
  const size_t count = std::min(tile_width, total - start);
  out.reserve(count);
  for (size_t j = 0; j < count; ++j) {
    const size_t ord = start + j;
    out.emplace_back(ord / right_count, ord % right_count);
  }
  return out;
}

inline HotRunOuterResult FinishHotRunOuter(
    size_t left_count, size_t right_count,
    std::span<const std::pair<size_t, size_t>> hits) {
  HotRunOuterResult res;
  res.matched_pairs.assign(hits.begin(), hits.end());
  std::vector<uint8_t> left_seen(left_count, 0);
  std::vector<uint8_t> right_seen(right_count, 0);
  for (const auto& [i, j] : hits) {
    if (i < left_count) {
      left_seen[i] = 1;
    }
    if (j < right_count) {
      right_seen[j] = 1;
    }
  }
  for (size_t i = 0; i < left_count; ++i) {
    if (left_seen[i] == 0) {
      res.unmatched_left.push_back(i);
    }
  }
  for (size_t j = 0; j < right_count; ++j) {
    if (right_seen[j] == 0) {
      res.unmatched_right.push_back(j);
    }
  }
  return res;
}

// ============================================================================
// 6. EngineDecisions.Executor.SealedBuild
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Exec_SealedBuild.lean
//     (SealedBuild.Goal)
// Early semi/anti decision before the global build barrier:
//   - NULL probe -> kMatchFalse
//   - key present in built set -> kMatchTrue
//   - key's partition already sealed -> kMatchFalse
//   - otherwise -> kPending
// ============================================================================
enum class SealedBuildVerdict : uint8_t {
  kPending = 0,
  kMatchTrue,
  kMatchFalse,
};

[[nodiscard]] inline SealedBuildVerdict DecideSealedBuild(
    bool is_null, bool in_built, bool partition_sealed) noexcept {
  if (is_null) {
    return SealedBuildVerdict::kMatchFalse;
  }
  if (in_built) {
    return SealedBuildVerdict::kMatchTrue;
  }
  if (partition_sealed) {
    return SealedBuildVerdict::kMatchFalse;
  }
  return SealedBuildVerdict::kPending;
}

// ============================================================================
// 7. ClickstreamLayout.Execution.SparseMark
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/SparseGroupMarkerBounds.lean
//     (SparseMark.Goal)
// Necessary-and-sufficient 2-column rectangular range pruning over a
// lexicographically clustered granule with inclusive bounds `[first, last]`.
// ============================================================================
struct LexMarkKey {
  int64_t x{0};
  int64_t y{0};
};

[[nodiscard]] constexpr bool LexMarkLE(LexMarkKey a, LexMarkKey b) noexcept {
  return (a.x < b.x) || (a.x == b.x && a.y <= b.y);
}

[[nodiscard]] constexpr LexMarkKey SparseMarkLeast(LexMarkKey fence,
                                                   int64_t x_lo, int64_t y_lo,
                                                   int64_t y_hi) noexcept {
  if (fence.x < x_lo) {
    return {.x = x_lo, .y = y_lo};
  }
  if (fence.y <= y_hi) {
    return {.x = fence.x, .y = std::max(y_lo, fence.y)};
  }
  return {.x = fence.x + 1, .y = y_lo};
}

[[nodiscard]] constexpr bool SparseMarkMayOverlap(LexMarkKey first,
                                                  LexMarkKey last, int64_t x_lo,
                                                  int64_t x_hi, int64_t y_lo,
                                                  int64_t y_hi) noexcept {
  if (x_lo > x_hi || y_lo > y_hi) {
    return false;
  }
  const LexMarkKey cand = SparseMarkLeast(first, x_lo, y_lo, y_hi);
  return cand.x <= x_hi && LexMarkLE(cand, last);
}

// ============================================================================
// 8. ClickstreamLayout.Execution.DefaultGroups
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/DefaultGroupComplement.lean
//     (DefaultGroups.Goal)
// Exact GROUP BY multiplicity over sparse lanes (each lane has a default value
// and exception `(row_id, val)` overrides): untouched rows contribute `n -
// |touched|` to the all-default group in O(1).
// ============================================================================
struct SparseGroupLane {
  Value default_value;
  std::vector<std::pair<size_t, Value>> exceptions;
};

struct DefaultGroupEntry {
  std::vector<Value> key;
  size_t count{0};
};

inline std::vector<DefaultGroupEntry> ComputeDefaultGroupsMultiplicity(
    std::span<const SparseGroupLane> lanes, size_t total_rows) {
  const size_t num_lanes = lanes.size();
  std::vector<size_t> touched;
  for (const SparseGroupLane& lane : lanes) {
    for (const auto& [row_id, val] : lane.exceptions) {
      if (row_id < total_rows) {
        touched.push_back(row_id);
      }
    }
  }
  std::ranges::sort(touched);
  const auto dup_range = std::ranges::unique(touched);
  touched.erase(dup_range.begin(), dup_range.end());

  auto tuple_at = [&](size_t row_id) {
    std::vector<Value> key;
    key.reserve(num_lanes);
    for (const SparseGroupLane& lane : lanes) {
      const Value* found = nullptr;
      for (const auto& [rid, v] : lane.exceptions) {
        if (rid == row_id) {
          found = &v;
          break;
        }
      }
      key.push_back(found != nullptr ? *found : lane.default_value);
    }
    return key;
  };

  auto keys_equal = [](const std::vector<Value>& a,
                       const std::vector<Value>& b) -> bool {
    if (a.size() != b.size()) {
      return false;
    }
    const ValueKeyEqual eq;
    for (size_t i = 0; i < a.size(); ++i) {
      if (!eq(a[i], b[i])) {
        return false;
      }
    }
    return true;
  };

  std::vector<DefaultGroupEntry> result;
  auto add_key = [&](std::vector<Value> key, size_t delta) {
    if (delta == 0) {
      return;
    }
    for (DefaultGroupEntry& e : result) {
      if (keys_equal(e.key, key)) {
        e.count += delta;
        return;
      }
    }
    result.push_back(DefaultGroupEntry{.key = std::move(key), .count = delta});
  };

  for (size_t row_id : touched) {
    add_key(tuple_at(row_id), 1);
  }
  const size_t default_rows =
      total_rows > touched.size() ? (total_rows - touched.size()) : 0;
  if (default_rows > 0) {
    std::vector<Value> def_key;
    def_key.reserve(num_lanes);
    for (const SparseGroupLane& lane : lanes) {
      def_key.push_back(lane.default_value);
    }
    add_key(std::move(def_key), default_rows);
  }
  return result;
}

}  // namespace tinylamb::engine_decisions

#endif  // TINYLAMB_EXECUTOR_DETAIL_ENGINE_DECISIONS_HPP
