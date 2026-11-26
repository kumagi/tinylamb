/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ENGINE_DECISIONS_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ENGINE_DECISIONS_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// EngineDecisions (remaining 10 proofs -> 32/32 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_RunOffset.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_HotRunTiles.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_DistinctExcludeTies.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_RangeFingers.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_FlushChoice.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_SealedBuild.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_DemandCache.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Exec_DemandCache.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_ExistsDurability.lean
// lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_NullableGroupKey.lean
// (all sound)
// ============================================================================

// Goal_RunOffset: skip sorting entire ordered runs covered by OFFSET and stop
// once `limit` rows have been emitted across boundary runs.
template <typename RowT, typename Cmp>
[[nodiscard]] std::vector<RowT> SkipRunsBeforeOffsetAndTake(
    const std::vector<std::vector<RowT>>& runs, size_t offset, size_t limit,
    Cmp&& cmp) {
  std::vector<RowT> out;
  if (limit == 0) {
    return out;
  }
  out.reserve(limit);
  size_t rem_off = offset;
  for (const auto& run : runs) {
    if (out.size() >= limit) {
      break;
    }
    if (run.size() <= rem_off) {
      rem_off -= run.size();
      continue;
    }
    std::vector<RowT> sorted = run;
    std::stable_sort(sorted.begin(), sorted.end(), cmp);
    const size_t take = std::min(sorted.size() - rem_off, limit - out.size());
    out.insert(out.end(), sorted.begin() + static_cast<std::ptrdiff_t>(rem_off),
               sorted.begin() + static_cast<std::ptrdiff_t>(rem_off + take));
    rem_off = 0;
  }
  return out;
}

// Goal_HotRunTiles: partition a hot-key Cartesian product `L x R` into
// pair-ordinal tiles of width `tile_width` for parallel worker evaluation.
[[nodiscard]] inline std::vector<std::pair<size_t, size_t>>
EnumerateHotKeyTilePairs(size_t left_count, size_t right_count,
                         size_t tile_width, size_t tile_idx) {
  std::vector<std::pair<size_t, size_t>> pairs;
  if (left_count == 0 || right_count == 0 || tile_width == 0) {
    return pairs;
  }
  const size_t total = left_count * right_count;
  const size_t start = tile_idx * tile_width;
  if (start >= total) {
    return pairs;
  }
  const size_t end = std::min(total, start + tile_width);
  pairs.reserve(end - start);
  for (size_t ord = start; ord < end; ++ord) {
    pairs.emplace_back(ord / right_count, ord % right_count);
  }
  return pairs;
}

// Goal_DistinctExcludeTies: compute `COUNT(DISTINCT x)` over a window frame
// with `EXCLUDE TIES` from frame frequency and current-peer frequency maps.
[[nodiscard]] inline size_t CountDistinctExcludeTies(
    const std::unordered_map<int64_t, size_t>& frame_freq,
    const std::unordered_map<int64_t, size_t>& peer_freq,
    int64_t current_row_val) {
  size_t distinct = 0;
  for (const auto& [val, f_cnt] : frame_freq) {
    if (f_cnt == 0) {
      continue;
    }
    const auto pit = peer_freq.find(val);
    const size_t p_cnt = pit == peer_freq.end() ? 0 : pit->second;
    const size_t excluded =
        (val == current_row_val && p_cnt > 0) ? (p_cnt - 1) : p_cnt;
    if (f_cnt > excluded) {
      ++distinct;
    }
  }
  return distinct;
}

// Goal_RangeFingers: monotone two-finger band-join range lookup `[lo, hi]`
// over sorted right keys for sorted left probes in O(|left| + |right|).
[[nodiscard]] inline std::vector<std::pair<size_t, size_t>>
ComputeSortedRangeFingerSpans(std::span<const int64_t> sorted_left,
                              std::span<const int64_t> sorted_right,
                              int64_t lo_delta, int64_t hi_delta) {
  std::vector<std::pair<size_t, size_t>> spans;
  spans.reserve(sorted_left.size());
  size_t lo_idx = 0;
  size_t hi_idx = 0;
  for (const int64_t x : sorted_left) {
    const int64_t lo_key = x + lo_delta;
    const int64_t hi_key = x + hi_delta;
    while (lo_idx < sorted_right.size() && sorted_right[lo_idx] < lo_key) {
      ++lo_idx;
    }
    if (hi_idx < lo_idx) {
      hi_idx = lo_idx;
    }
    while (hi_idx < sorted_right.size() && sorted_right[hi_idx] <= hi_key) {
      ++hi_idx;
    }
    spans.emplace_back(lo_idx, hi_idx);
  }
  return spans;
}

// Goal_FlushChoice: 0/1 knapsack DP choosing the minimum-cost subset of
// flushable buffers whose reclaimed bytes are at least `need`.
struct FlushBufferCandidate {
  size_t bytes{0};
  uint64_t cost{0};
};

[[nodiscard]] inline std::optional<uint64_t> MinFlushCostToReclaim(
    std::span<const FlushBufferCandidate> buffers, size_t need) {
  if (need == 0) {
    return uint64_t{0};
  }
  constexpr uint64_t kInf = std::numeric_limits<uint64_t>::max() / 4;
  std::vector<uint64_t> dp(need + 1, kInf);
  dp[0] = 0;
  for (const auto& b : buffers) {
    for (size_t r = need + 1; r-- > 0;) {
      if (dp[r] == kInf) {
        continue;
      }
      const size_t nr = std::min(need, r + b.bytes);
      dp[nr] = std::min(dp[nr], dp[r] + b.cost);
    }
  }
  if (dp[need] == kInf) {
    return std::nullopt;
  }
  return dp[need];
}

// Goal_SealedBuild: decide semi-join probe membership before the global build
// barrier whenever the key's partition is already sealed (or key already seen).
[[nodiscard]] inline std::optional<bool> DecideBeforeBuildBarrier(
    size_t partition_id, const std::unordered_set<size_t>& sealed_partitions,
    const std::unordered_set<uint64_t>& built_keys, uint64_t probe_key) {
  if (built_keys.contains(probe_key)) {
    return true;
  }
  if (sealed_partitions.contains(partition_id)) {
    return false;
  }
  return std::nullopt;
}

// Goal_DemandCache + Goal_Exec_DemandCache: memoize correlated subquery
// evaluation once per distinct demanded key while preserving probe order.
template <typename EvalFn>
[[nodiscard]] std::vector<int> EvalWithDemandCache(
    std::span<const uint64_t> keys, EvalFn&& eval_fn, size_t* eval_count_out) {
  std::unordered_map<uint64_t, int> cache;
  std::vector<int> out;
  out.reserve(keys.size());
  size_t evals = 0;
  for (const uint64_t k : keys) {
    auto [it, inserted] = cache.emplace(k, 0);
    if (inserted) {
      it->second = eval_fn(k);
      ++evals;
    }
    out.push_back(it->second);
  }
  if (eval_count_out != nullptr) {
    *eval_count_out = evals;
  }
  return out;
}

// Goal_ExistsDurability: minimum durability WAL fence for an EXISTS predicate
// is the end LSN of the last committed event that flipped truth value.
struct ExistsWalEvent {
  uint64_t wal_end_lsn{0};
  bool committed{false};
  bool new_truth{false};
};

[[nodiscard]] inline uint64_t MinExistsDurabilityFence(
    bool initial_truth, std::span<const ExistsWalEvent> log) noexcept {
  bool cur = initial_truth;
  uint64_t fence = 0;
  for (const auto& ev : log) {
    if (!ev.committed) {
      continue;
    }
    if (ev.new_truth != cur) {
      cur = ev.new_truth;
      fence = ev.wal_end_lsn;
    }
  }
  return fence;
}

// Goal_NullableGroupKey: verify whether observed `(key, dep)` pairs satisfy
// the SQL-null-aware functional dependency `key -> dep`, licensing a narrower
// GROUP BY hash key without `dep`.
[[nodiscard]] inline bool ObservedNullableFunctionalDependency(
    std::span<const std::pair<std::optional<int64_t>, std::optional<int64_t>>>
        pairs) {
  std::optional<std::optional<int64_t>> null_key_dep;
  std::unordered_map<int64_t, std::optional<int64_t>> nonnull_dep;
  for (const auto& [k, dep] : pairs) {
    if (!k.has_value()) {
      if (!null_key_dep.has_value()) {
        null_key_dep = dep;
      } else if (*null_key_dep != dep) {
        return false;
      }
    } else {
      const auto [it, inserted] = nonnull_dep.emplace(*k, dep);
      if (!inserted && it->second != dep) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_ENGINE_DECISIONS_PROOFS_HPP
