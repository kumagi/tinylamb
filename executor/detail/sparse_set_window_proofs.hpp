/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SPARSE_SET_WINDOW_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SPARSE_SET_WINDOW_PROOFS_HPP

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// QueryRewrites / SparseSetAndWindowEvaluation (13/13 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/NameDayBitmapTriplePopulation.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/PeerRankBoundsFromPartitionExtrema.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/ReturnStateThresholdSortedSuffix.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/RollupParentTotalsFromLeafPartitions.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/SharedDimensionFilterBeforeTimeSplit.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/ShiftedStoreWeekIndexWithBoundaryHalo.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_NameDayBitmapTriplePopulation.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_PeerRankBoundsFromPartitionExtrema.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_ReturnStateThresholdSortedSuffix.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_RollupParentTotalsFromLeafPartitions.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_SharedDimensionFilterBeforeTimeSplit.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_ShiftedStoreWeekIndexWithBoundaryHalo.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SparseSetAndWindowEvaluation/Proof_SparseSetAndWindowEvaluation_ZeroPriorSalesGroupEarlyAbandon.lean
// ============================================================================

// NameDayBitmapTriplePopulation (TPC-DS Q38): popcount of bitwise AND of three
// bounded date bitmaps equals the exact 3-channel intersection cardinality.
[[nodiscard]] inline size_t TripleDayBitmapIntersectPopcount(
    std::span<const uint64_t> store_days,
    std::span<const uint64_t> catalog_days,
    std::span<const uint64_t> web_days) noexcept {
  const size_t words =
      std::min({store_days.size(), catalog_days.size(), web_days.size()});
  size_t pop = 0;
  for (size_t w = 0; w < words; ++w) {
    pop += static_cast<size_t>(
        std::popcount(store_days[w] & catalog_days[w] & web_days[w]));
  }
  return pop;
}

// PeerRankBoundsFromPartitionExtrema (TPC-DS Q44/Q86): a page whose max < probe
// contributes its full size to SQL RANK(); a page with min >= probe contributes
// zero.
struct RankPageMeta {
  uint64_t min_key{0};
  uint64_t max_key{0};
  size_t row_count{0};
};

[[nodiscard]] inline size_t CountStrictlySmallerWithPageExtrema(
    std::span<const RankPageMeta> pages,
    std::span<const std::vector<uint64_t>> page_keys, uint64_t probe) {
  size_t smaller = 0;
  for (size_t i = 0; i < pages.size() && i < page_keys.size(); ++i) {
    if (pages[i].row_count == 0 || pages[i].min_key >= probe) {
      continue;
    }
    if (pages[i].max_key < probe) {
      smaller += pages[i].row_count;
      continue;
    }
    for (const uint64_t k : page_keys[i]) {
      if (k < probe) {
        ++smaller;
      }
    }
  }
  return smaller;
}

// ReturnStateThresholdSortedSuffix (TPC-DS Q01/Q30/Q81): on sorted customer
// totals, strict `5 * n * x > 6 * sum` selects an exact suffix starting at
// `std::upper_bound`.
[[nodiscard]] inline std::span<const int64_t> SelectAboveAverageThresholdSuffix(
    std::span<const int64_t> sorted_totals, int64_t total_sum) noexcept {
  if (sorted_totals.empty()) {
    return {};
  }
  const auto n = static_cast<int64_t>(sorted_totals.size());
  const auto it =
      std::upper_bound(sorted_totals.begin(), sorted_totals.end(), total_sum,
                       [n](int64_t s, int64_t x) { return 5 * n * x > 6 * s; });
  return sorted_totals.subspan(static_cast<size_t>(it - sorted_totals.begin()));
}

// RollupParentTotalsFromLeafPartitions (TPC-DS Q14/Q86): merge nullable leaf
// subtotals (including a real NULL class key) into the parent category total.
[[nodiscard]] inline std::optional<int64_t> RollupNullableLeafPartitions(
    std::span<const std::pair<std::optional<uint64_t>, std::optional<int64_t>>>
        leaves) noexcept {
  std::optional<int64_t> acc;
  for (const auto& [_, val] : leaves) {
    if (val.has_value()) {
      acc = acc.value_or(0) + *val;
    }
  }
  return acc;
}

// SharedDimensionFilterBeforeTimeSplit (TPC-DS Q90): apply shared dimension
// predicate once before splitting counts into morning/evening time bands.
struct AmPmSplitCounts {
  size_t am_count{0};
  size_t pm_count{0};
  constexpr bool operator==(const AmPmSplitCounts&) const = default;
};

template <typename HourFn>
[[nodiscard]] AmPmSplitCounts CountSharedDimAmPmSplit(
    std::span<const std::pair<uint64_t, uint8_t>> fact_time_and_dim_ok,
    HourFn&& hour_of, uint64_t am_lo, uint64_t am_hi, uint64_t pm_lo,
    uint64_t pm_hi) {
  AmPmSplitCounts out;
  for (const auto& [time_sk, dim_ok] : fact_time_and_dim_ok) {
    if (dim_ok == 0) {
      continue;
    }
    const uint64_t h = hour_of(time_sk);
    if (am_lo <= h && h <= am_hi) {
      ++out.am_count;
    } else if (pm_lo <= h && h <= pm_hi) {
      ++out.pm_count;
    }
  }
  return out;
}

// ShiftedStoreWeekIndexWithBoundaryHalo (TPC-DS Q59): when left week keys lie
// in `[lo, hi]`, restrict right-side index build to `[lo + shift, hi + shift]`.
[[nodiscard]] inline std::vector<uint64_t> FilterShiftedWeekHalo(
    std::span<const uint64_t> right_weeks, uint64_t lo, uint64_t hi,
    uint64_t shift) {
  std::vector<uint64_t> kept;
  for (const uint64_t w : right_weeks) {
    if (w >= lo + shift && w <= hi + shift) {
      kept.push_back(w);
    }
  }
  return kept;
}

// ZeroPriorSalesGroupEarlyAbandon (TPC-DS Q04/Q11/Q74): abandon a customer
// group early before reading year-2 measures when `seen_sum + pending_cnt *
// per_row_upper_bound <= 0`.
[[nodiscard]] inline bool CanAbandonNonPositivePriorYearGroup(
    int64_t seen_sum, size_t pending_rows,
    int64_t per_row_upper_bound) noexcept {
  return seen_sum +
             (static_cast<int64_t>(pending_rows) * per_row_upper_bound) <=
         0;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_SPARSE_SET_WINDOW_PROOFS_HPP
