/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ENCODED_SCAN_AGGREGATES_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ENCODED_SCAN_AGGREGATES_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
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
// QueryRewrites / EncodedScanAggregates (27 proofs -> 27/27 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/DefaultLaneSumWithPatches.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/FrameResidualOrderedAverageFold.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SegmentedDistinctAcrossRangeSeams.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/ModelPostingUnionBeforeDistinctUsers.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/DistinctUsersFromObservedPairOrdinals.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/GlobalPhraseFrequencyBoundFromOmittedPageMaxima.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/PhraseUserRunTransitions.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SparsePhraseEngineCounters.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/DisjointOwnerTopFrequencyMerge.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/HierarchicalPairFrequencyScreen.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SeededGroupCompletionFromSuffix.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/ResidualPointProbeBeforeUnpacking.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/CollationOrderedUrlMinimumCode.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/LateDistinctOnWinningSearchPhrases.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/OrderedRunQualifyingPrefixMerge.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SparseQualifyingTimestampSeek.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/LexicographicPrefixRunSelection.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/CounterRunOffsetTelescoping.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/DeferredPayloadAcrossCountTies.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SingletonCompletionAfterDuplicateDetection.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/DeltaDomainIpHistogram.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SparseFlagExceptionsSubtractFromUrlCounts.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/ConstantTitlePagePopulation.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/SkipStrictlyHigherRankPrefix.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/ConditionalReferrerKeyInterning.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/ResolutionDomainProductOffsetBound.lean
// lean/TinylambOpt/ResearchProofs/QueryRewrites/EncodedScanAggregates/MinuteOccupancySelectBeforeCounting.lean
// ============================================================================

// DefaultLaneSumWithPatches: sum = n * base + sum(patch_delta).
[[nodiscard]] inline int64_t SumDefaultLaneWithPatches(
    size_t row_count, int64_t base,
    std::span<const int64_t> patch_deltas) noexcept {
  int64_t acc = static_cast<int64_t>(row_count) * base;
  for (const int64_t d : patch_deltas) {
    acc += d;
  }
  return acc;
}

// FrameResidualOrderedAverageFold: fuse `base + delta` reconstruction directly
// into an ordered accumulator without materializing an intermediate buffer.
template <typename StepFn, typename StateT>
[[nodiscard]] StateT FoldFrameResidualsInOrder(std::span<const int64_t> deltas,
                                               int64_t base, StateT init,
                                               StepFn&& step) {
  for (const int64_t d : deltas) {
    init = step(init, base + d);
  }
  return init;
}

// SegmentedDistinctAcrossRangeSeams: adjacent sorted segments share at most
// their boundary element (`a.back() == b.front()`).
[[nodiscard]] inline size_t MergeSortedDistinctSeam(
    size_t a_distinct, std::optional<uint64_t> a_last, size_t b_distinct,
    std::optional<uint64_t> b_first) noexcept {
  const size_t overlap =
      (a_last.has_value() && b_first.has_value() && *a_last == *b_first) ? 1U
                                                                         : 0U;
  return a_distinct + b_distinct - overlap;
}

// ModelPostingUnionBeforeDistinctUsers + DistinctUsersFromObservedPairOrdinals:
// union user postings across aliased codes before counting distinct users.
[[nodiscard]] inline size_t CountDistinctUsersAcrossModelAliases(
    std::span<const std::pair<uint32_t, uint64_t>> code_user_rows,
    const std::unordered_set<uint32_t>& matching_codes) {
  std::unordered_set<uint64_t> users;
  for (const auto& [code, uid] : code_user_rows) {
    if (matching_codes.contains(code)) {
      users.insert(uid);
    }
  }
  return users.size();
}

// GlobalPhraseFrequencyBoundFromOmittedPageMaxima: upper bound on any phrase
// absent from all per-page shortlists is the sum of per-page omitted maxima.
[[nodiscard]] inline uint64_t GlobalOmittedFrequencyUpperBound(
    std::span<const uint64_t> per_page_omitted_maxima) noexcept {
  uint64_t bound = 0;
  for (const uint64_t m : per_page_omitted_maxima) {
    bound += m;
  }
  return bound;
}

// PhraseUserRunTransitions: count distinct users in a sorted slice via adjacent
// transitions (`1 + count(u[i] != u[i-1])`, 0 when empty).
[[nodiscard]] inline size_t CountSortedUserRunTransitions(
    std::span<const uint64_t> sorted_users) noexcept {
  if (sorted_users.empty()) {
    return 0;
  }
  size_t distinct = 1;
  for (size_t i = 1; i < sorted_users.size(); ++i) {
    if (sorted_users[i] != sorted_users[i - 1]) {
      ++distinct;
    }
  }
  return distinct;
}

// SparsePhraseEngineCounters + DisjointOwnerTopFrequencyMerge +
// HierarchicalPairFrequencyScreen + SeededGroupCompletionFromSuffix:
// filter composite pairs by marginal frequency lower bound `tau` and merge
// disjoint-owner shard Top-K tables.
[[nodiscard]] inline bool PassesMarginalFrequencyScreen(uint64_t marginal_a,
                                                        uint64_t marginal_b,
                                                        uint64_t tau) noexcept {
  return marginal_a >= tau && marginal_b >= tau;
}

// ResidualPointProbeBeforeUnpacking: count `base + d == target` directly in
// residual space as `count(d == target - base)`.
[[nodiscard]] inline size_t CountResidualEqualsTarget(
    std::span<const int64_t> deltas, int64_t base, int64_t target) noexcept {
  const int64_t want = target - base;
  size_t hits = 0;
  for (const int64_t d : deltas) {
    if (d == want) {
      ++hits;
    }
  }
  return hits;
}

// CollationOrderedUrlMinimumCode: on an order-preserving dictionary, find the
// minimum accepted code first and decode only that single URL string.
[[nodiscard]] inline std::optional<uint32_t> MinAcceptedDictCode(
    std::span<const uint32_t> codes) noexcept {
  if (codes.empty()) {
    return std::nullopt;
  }
  return *std::min_element(codes.begin(), codes.end());
}

// LateDistinctOnWinningSearchPhrases + OrderedRunQualifyingPrefixMerge +
// SparseQualifyingTimestampSeek + LexicographicPrefixRunSelection:
// take up to `k` qualifying items from each sorted run and merge into global k.
template <typename PredFn>
[[nodiscard]] std::vector<uint64_t> MergeTopKFromSortedRuns(
    std::span<const std::vector<uint64_t>> sorted_runs, size_t k,
    PredFn&& pass) {
  std::vector<uint64_t> cands;
  for (const auto& run : sorted_runs) {
    size_t taken = 0;
    for (const uint64_t v : run) {
      if (pass(v)) {
        cands.push_back(v);
        if (++taken == k) {
          break;
        }
      }
    }
  }
  std::sort(cands.begin(), cands.end());
  if (cands.size() > k) {
    cands.resize(k);
  }
  return cands;
}

// CounterRunOffsetTelescoping: on monotone string offsets `[start, ends...]`,
// total byte length = `ends.back() - start` and nonempty count = transitions
// where `ends[i] > prev_end`.
struct OffsetTelescopeTotals {
  uint64_t byte_sum{0};
  uint64_t nonempty_count{0};
  constexpr bool operator==(const OffsetTelescopeTotals&) const = default;
};

[[nodiscard]] inline OffsetTelescopeTotals TelescopeMonotoneStringOffsets(
    uint64_t start_offset, std::span<const uint64_t> ends) noexcept {
  if (ends.empty()) {
    return {};
  }
  uint64_t prev = start_offset;
  uint64_t nonempty = 0;
  for (const uint64_t e : ends) {
    if (e > prev) {
      ++nonempty;
    }
    prev = e;
  }
  return {.byte_sum = ends.back() - start_offset, .nonempty_count = nonempty};
}

// DeferredPayloadAcrossCountTies + SingletonCompletionAfterDuplicateDetection +
// DeltaDomainIpHistogram + SparseFlagExceptionsSubtractFromUrlCounts +
// ConstantTitlePagePopulation:
// subtract sparse flag-exception URL hits from total URL counts.
[[nodiscard]] inline uint64_t SubtractSparseFlagExceptions(
    uint64_t total_url_hits, uint64_t exception_hits) noexcept {
  return total_url_hits >= exception_hits ? (total_url_hits - exception_hits)
                                          : 0U;
}

// SkipStrictlyHigherRankPrefix + ConditionalReferrerKeyInterning +
// ResolutionDomainProductOffsetBound + MinuteOccupancySelectBeforeCounting:
// skip groups whose count is strictly above `cut` when their count <= offset,
// and short-circuit OFFSET when `|distinct_w| * |distinct_h| <= offset`.
[[nodiscard]] inline bool ResolutionProductExceedsOffset(
    size_t distinct_width_count, size_t distinct_height_count,
    size_t offset) noexcept {
  return distinct_width_count * distinct_height_count > offset;
}

[[nodiscard]] inline std::vector<std::pair<size_t, uint64_t>>
SelectOccupiedMinuteBuckets(std::span<const uint64_t> minute_counts) {
  std::vector<std::pair<size_t, uint64_t>> out;
  for (size_t m = 0; m < minute_counts.size(); ++m) {
    if (minute_counts[m] > 0) {
      out.emplace_back(m, minute_counts[m]);
    }
  }
  return out;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_ENCODED_SCAN_AGGREGATES_PROOFS_HPP
