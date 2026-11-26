/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_BENCHMARK_REMATCH_HPP
#define TINYLAMB_EXECUTOR_DETAIL_BENCHMARK_REMATCH_HPP

#include <algorithm>
#include <array>
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
// 1. BenchmarkRematch.Analytics (J3, C1, C2, H2, H4, H5, L2, L4, L5, B3..B5)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Analytics_*.lean
// ============================================================================

// Analytics.B3DuckDB: bijective 3-key dense integer encoding
// `(region * categories + category) * year_span + year_off`.
[[nodiscard]] inline size_t EncodeThreeKeyDense(size_t r, size_t c, size_t y,
                                                size_t categories,
                                                size_t year_span) noexcept {
  return (((r * categories) + c) * year_span) + y;
}

// Analytics.H2DuckDB: bijective 2-key dense integer encoding `a * width + b`.
[[nodiscard]] inline size_t EncodeTwoKeyDense(size_t a, size_t b,
                                              size_t width) noexcept {
  return (a * width) + b;
}

// Analytics.H5DuckDB / C1DuckDB / L4DuckDB / B5ClickHouse:
// Bilateral weighted nullable SUM synthesis from collapsed left/right totals.
struct NullableSumPair {
  std::optional<int64_t> left_sum;
  std::optional<int64_t> right_sum;
};

[[nodiscard]] inline NullableSumPair CombineBilateralNullableSums(
    size_t left_cnt, std::optional<int64_t> left_nsum, size_t right_cnt,
    std::optional<int64_t> right_nsum) noexcept {
  NullableSumPair out;
  if (right_cnt > 0 && left_nsum.has_value()) {
    out.left_sum = (*left_nsum) * static_cast<int64_t>(right_cnt);
  }
  if (left_cnt > 0 && right_nsum.has_value()) {
    out.right_sum = (*right_nsum) * static_cast<int64_t>(left_cnt);
  }
  return out;
}

// Analytics.J3ClickHouse / C2DuckDB:
// Aggregate fact multiplicities by dimension key once before multiplying by
// dimension payload sums/counts.
struct WeightedDimAggregate {
  uint64_t count{0};
  int64_t sum{0};
};

[[nodiscard]] inline WeightedDimAggregate FoldFrequencyWeightedDim(
    const std::unordered_map<uint64_t, uint64_t>& key_freq,
    const std::unordered_map<uint64_t, WeightedDimAggregate>& dim_totals) {
  WeightedDimAggregate out;
  for (const auto& [k, n] : key_freq) {
    const auto it = dim_totals.find(k);
    if (it != dim_totals.end()) {
      out.count += n * it->second.count;
      out.sum += static_cast<int64_t>(n) * it->second.sum;
    }
  }
  return out;
}

// Analytics.B4DuckDB: partition OR predicate `a || b` into `a` + `(!a && b)`
// so expensive text predicate `b` runs only on rows failing cheap predicate
// `a`.
struct DisjunctionPartitionTotals {
  uint64_t count{0};
  int64_t sum{0};
};

template <typename CheapPred, typename ExpensivePred, typename PayloadFn>
[[nodiscard]] DisjunctionPartitionTotals EvalShortCircuitOrSum(
    size_t n, CheapPred&& cheap, ExpensivePred&& expensive,
    PayloadFn&& payload) {
  DisjunctionPartitionTotals out;
  for (size_t i = 0; i < n; ++i) {
    if (cheap(i) || expensive(i)) {
      ++out.count;
      if (const std::optional<int64_t> v = payload(i); v.has_value()) {
        out.sum += *v;
      }
    }
  }
  return out;
}

// ============================================================================
// 2. BenchmarkRematch.Tpch (Q01, Q03, Q07, Q08, Q15)
// Lean proofs: lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpch_*.lean
// ============================================================================

// Tpch.Q03DuckDB & Click.Q39DuckDB: per-shard local Top-K before global merge.
template <typename T>
[[nodiscard]] std::vector<T> MergeShardTopK(
    std::span<const std::vector<T>> shards, size_t k) {
  std::vector<T> merged;
  for (const auto& shard : shards) {
    std::vector<T> local = shard;
    const size_t take = std::min(k, local.size());
    std::partial_sort(local.begin(),
                      local.begin() + static_cast<std::ptrdiff_t>(take),
                      local.end());
    merged.insert(merged.end(), local.begin(),
                  local.begin() + static_cast<std::ptrdiff_t>(take));
  }
  const size_t take = std::min(k, merged.size());
  std::partial_sort(merged.begin(),
                    merged.begin() + static_cast<std::ptrdiff_t>(take),
                    merged.end());
  merged.resize(take);
  return merged;
}

// Tpch.Q15DuckDB: retain all local-maxima ties per shard before global max.
[[nodiscard]] inline std::vector<std::pair<uint64_t, int64_t>>
SelectGlobalMaximaViaShardCandidates(
    std::span<const std::vector<std::pair<uint64_t, int64_t>>> shards) {
  std::vector<std::pair<uint64_t, int64_t>> candidates;
  for (const auto& shard : shards) {
    if (shard.empty()) {
      continue;
    }
    int64_t local_max = shard[0].second;
    for (const auto& [id, val] : shard) {
      local_max = std::max(local_max, val);
    }
    for (const auto& entry : shard) {
      if (entry.second == local_max) {
        candidates.push_back(entry);
      }
    }
  }
  if (candidates.empty()) {
    return {};
  }
  int64_t global_max = candidates[0].second;
  for (const auto& [id, val] : candidates) {
    global_max = std::max(global_max, val);
  }
  std::vector<std::pair<uint64_t, int64_t>> winners;
  for (const auto& entry : candidates) {
    if (entry.second == global_max) {
      winners.push_back(entry);
    }
  }
  return winners;
}

// ============================================================================
// 3. BenchmarkRematch.Tpcds (Q04, Q16, Q17, Q40/Q75, Q49, Q62/Q99, Q81, Q86)
// Lean proofs: lean/TinylambOpt/ResearchProofs/BenchmarkRematch/Tpcds_*.lean
// ============================================================================

// Tpcds.Q04DuckDB: 6-slot `(channel < 3, year < 2)` bijection `2*c + y`.
[[nodiscard]] inline size_t EncodeChannelYearSlot(size_t channel,
                                                  size_t year_idx) noexcept {
  return (2U * channel) + year_idx;
}

// Tpcds.Q16DuckDB: check whether an order has a distinct non-NULL warehouse
// using only `(min_wh, max_wh)` vs target `w`.
[[nodiscard]] inline bool HasOtherNonNullWarehouse(
    std::optional<uint64_t> min_wh, std::optional<uint64_t> max_wh,
    uint64_t w) noexcept {
  return (min_wh.has_value() && *min_wh < w) ||
         (max_wh.has_value() && w < *max_wh);
}

// Tpcds.Q62DuckDB / Q99DuckDB: single bucket classification for shipping delay
// thresholds `[30, 60, 90, 120]` -> `{0, 1, 2, 3, 4}`.
[[nodiscard]] inline uint8_t ClassifyShippingDelayBucket(
    int64_t days) noexcept {
  if (days <= 30) {
    return 0;
  }
  if (days <= 60) {
    return 1;
  }
  if (days <= 90) {
    return 2;
  }
  if (days <= 120) {
    return 3;
  }
  return 4;
}

// Tpcds.Q40DuckDB / Q75DuckDB: left-join net delta without materializing
// unmatched rows: `max(1, refund_cnt) * base_val - refund_sum`.
[[nodiscard]] inline int64_t LeftJoinNetDelta(int64_t base_val,
                                              size_t refund_cnt,
                                              int64_t refund_sum) noexcept {
  const auto mult = static_cast<int64_t>(std::max<size_t>(1, refund_cnt));
  return (mult * base_val) - refund_sum;
}

// ============================================================================
// 4. BenchmarkRematch.Click (Q12, Q14, Q18, Q28, Q37, Q42, Q43) & Cloud
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/BenchmarkRematch/{Click,Cloud}_*.lean
// ============================================================================

// Click.Q28ClickHouse: non-empty string length sum and count from raw lengths
// without decoding string bodies (`sum(len != 0) == sum(len)`).
struct NonEmptyLengthSummary {
  uint64_t sum{0};
  uint64_t non_empty_count{0};
};

[[nodiscard]] inline NonEmptyLengthSummary SummarizeNonEmptyLengths(
    std::span<const uint32_t> lengths) noexcept {
  NonEmptyLengthSummary out;
  for (const uint32_t len : lengths) {
    out.sum += len;
    if (len != 0) {
      ++out.non_empty_count;
    }
  }
  return out;
}

// Click.Q37DuckDB: prune candidate group `i` when its upper bound
// (`seen_count + remaining_possible`) is strictly below the 10th-largest seen
// count threshold.
[[nodiscard]] inline bool CanPruneTopKGroup(uint64_t seen_count,
                                            uint64_t remaining_upper,
                                            uint64_t kth_threshold) noexcept {
  return seen_count + remaining_upper < kth_threshold;
}

// Click.Q42DuckDB: short-circuit OFFSET >= distinct_group_count to empty
// result.
[[nodiscard]] inline bool OffsetExceedsDistinctGroups(size_t distinct_groups,
                                                      size_t offset) noexcept {
  return distinct_groups <= offset;
}

// Cloud.Public108ClickHouse: whole-page MIN/MAX summary bypasses row decode
// when `all_pass` holds for the page.
struct PageExtrema {
  std::optional<int64_t> min_val;
  std::optional<int64_t> max_val;

  void Merge(const PageExtrema& other) noexcept {
    if (other.min_val.has_value()) {
      min_val = min_val.has_value() ? std::min(*min_val, *other.min_val)
                                    : other.min_val;
    }
    if (other.max_val.has_value()) {
      max_val = max_val.has_value() ? std::max(*max_val, *other.max_val)
                                    : other.max_val;
    }
  }
};

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_BENCHMARK_REMATCH_HPP
