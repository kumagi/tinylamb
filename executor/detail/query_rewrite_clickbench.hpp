/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_CLICKBENCH_HPP
#define TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_CLICKBENCH_HPP

#include <algorithm>
#include <bit>
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

namespace tinylamb::query_rewrites {

// ============================================================================
// ClickBench QueryRewrites (20 proved theorems)
// Lean models:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean
// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/QueryRewrites/DictionaryFiltersAndRanking/
// ============================================================================

// Q12: PartitionedUserBitmapDistinct.Goal
// Partitions user IDs into fixed-width `w`-bit pages (`page = u / w, bit = u %
// w`) and sums per-page popcounts to compute exact COUNT(DISTINCT UserID).
[[nodiscard]] inline uint64_t CountPartitionedUserBitmapDistinct(
    std::span<const uint64_t> users, uint64_t page_width = 64U) {
  if (page_width == 0) {
    return 0;
  }
  if (page_width == 64U) {
    std::unordered_map<uint64_t, uint64_t> pages;
    for (const uint64_t u : users) {
      pages[u >> 6U] |= (uint64_t{1} << (u & 63U));
    }
    uint64_t distinct = 0;
    for (const auto& [pg, bits] : pages) {
      (void)pg;
      distinct += static_cast<uint64_t>(std::popcount(bits));
    }
    return distinct;
  }
  std::unordered_map<uint64_t, std::vector<uint8_t>> pages;
  for (const uint64_t u : users) {
    auto& bits = pages[u / page_width];
    if (bits.empty()) {
      bits.assign(static_cast<size_t>(page_width), 0U);
    }
    bits[static_cast<size_t>(u % page_width)] = 1U;
  }
  uint64_t distinct = 0;
  for (const auto& [pg, bits] : pages) {
    (void)pg;
    for (const uint8_t b : bits) {
      distinct += b;
    }
  }
  return distinct;
}

// Q14: DistinctTopKPruningByRemainingRows.Goal
// Prunes a group from Top-`k` DISTINCT contention when `seen_distinct +
// remaining_rows` is strictly smaller than at least `k` finalized groups.
[[nodiscard]] inline bool CanPruneDistinctGroupByRemainingRows(
    uint64_t seen_distinct, uint64_t remaining_rows,
    std::span<const uint64_t> finalized_distinct_counts,
    size_t k = 10U) noexcept {
  const uint64_t upper = seen_distinct + remaining_rows;
  size_t beats = 0;
  for (const uint64_t c : finalized_distinct_counts) {
    if (c > upper) {
      ++beats;
      if (beats >= k) {
        return true;
      }
    }
  }
  return false;
}

// Q16: CandidateTopKWithOmittedCountBound.Goal
// Certifies that the Top-`k` keys ranked by `(count DESC, key ASC)` can be
// finalized strictly within `candidates` whenever at least `k` candidates have
// `count >= threshold` and every omitted key has `count < threshold`.
[[nodiscard]] inline std::optional<std::vector<uint64_t>>
ResolveCandidateTopKWithOmittedBound(
    const std::unordered_map<uint64_t, uint64_t>& candidate_counts,
    uint64_t omitted_max_count_exclusive, size_t k = 10U) {
  std::vector<std::pair<uint64_t, uint64_t>> items(candidate_counts.begin(),
                                                   candidate_counts.end());
  size_t above_or_eq = 0;
  for (const auto& [key, cnt] : items) {
    (void)key;
    if (cnt >= omitted_max_count_exclusive) {
      ++above_or_eq;
    }
  }
  if (above_or_eq < k) {
    return std::nullopt;
  }
  std::ranges::sort(items, [](const auto& a, const auto& b) {
    return a.second > b.second || (a.second == b.second && a.first < b.first);
  });
  if (items.size() > k) {
    items.resize(k);
  }
  std::vector<uint64_t> winners;
  winners.reserve(items.size());
  for (const auto& [key, cnt] : items) {
    (void)cnt;
    winners.push_back(key);
  }
  return winners;
}

// Q18: RunLengthGroupCounts.Goal
// Accumulates `(user_id, phrase_code)` group counts from run-length encoded
// pairs with one hash-table addition per run.
struct PairRun {
  uint64_t first{0};
  uint64_t second{0};
  uint64_t count{0};
};

[[nodiscard]] inline uint64_t RunLengthPairGroupCount(
    std::span<const PairRun> runs, uint64_t target_first,
    uint64_t target_second) noexcept {
  uint64_t total = 0;
  for (const auto& run : runs) {
    if (run.first == target_first && run.second == target_second) {
      total += run.count;
    }
  }
  return total;
}

// Q20: SortedEqualRangeCount.Goal
// Computes exact count of `target` in a sorted slice via `upper_bound -
// lower_bound` in O(log n).
[[nodiscard]] inline size_t SortedEqualRangeCount(
    std::span<const int64_t> sorted_ids, int64_t target) noexcept {
  const auto lo = std::ranges::lower_bound(sorted_ids, target);
  const auto hi = std::ranges::upper_bound(sorted_ids, target);
  return static_cast<size_t>(hi - lo);
}

// Q21: ChunkedSubstringBoundaryCarry.Goal
// Evaluates `(a ++ b).contains(needle)` without concatenating `a` and `b`,
// carrying only `needle.size() - 1` trailing bytes of `a` across the boundary.
[[nodiscard]] inline bool ChunkedContainsWithBoundaryCarry(
    std::string_view a, std::string_view b, std::string_view needle) {
  if (needle.empty()) {
    return true;
  }
  if (a.find(needle) != std::string_view::npos ||
      b.find(needle) != std::string_view::npos) {
    return true;
  }
  const size_t carry = needle.size() - 1U;
  if (carry == 0 || a.empty() || b.empty()) {
    return false;
  }
  const std::string_view tail =
      a.size() > carry ? a.substr(a.size() - carry) : a;
  const std::string_view head = b.size() > carry ? b.substr(0, carry) : b;
  std::string bridge;
  bridge.reserve(tail.size() + head.size());
  bridge.append(tail);
  bridge.append(head);
  return bridge.find(needle) != std::string_view::npos;
}

// Q22: CountTopKBeforePayloadMinimum.Goal
// Restricts MIN(URL) tracking to phrase keys whose match count meets `cutoff`
// (preserving all ties at the Top-K boundary).
[[nodiscard]] inline std::optional<std::string> FilteredCountCutoffMinPayload(
    std::span<const std::pair<uint64_t, std::string>> matched,
    uint64_t target_phrase, uint64_t cutoff) {
  std::unordered_map<uint64_t, uint64_t> counts;
  for (const auto& [phrase, url] : matched) {
    (void)url;
    ++counts[phrase];
  }
  const auto it = counts.find(target_phrase);
  if (it == counts.end() || it->second < cutoff) {
    return std::nullopt;
  }
  std::optional<std::string> best;
  for (const auto& [phrase, url] : matched) {
    if (phrase == target_phrase && (!best.has_value() || url < *best)) {
      best = url;
    }
  }
  return best;
}

// Q23: DictionaryPairPredicateMemoization.Goal
// Memoizes `title.contains("Google") && !url.contains(".google.")` per unique
// `(title_code, url_code)` pair so repeated pairs are evaluated once.
struct CodePairHash {
  size_t operator()(const std::pair<uint32_t, uint32_t>& p) const noexcept {
    return (static_cast<uint64_t>(p.first) << 32U) | p.second;
  }
};

template <typename TitleFn, typename UrlFn>
[[nodiscard]] std::vector<std::pair<uint32_t, uint32_t>>
FilterMemoizedTitleUrlCodePairs(
    std::span<const std::pair<uint32_t, uint32_t>> pairs, TitleFn&& title_of,
    UrlFn&& url_of) {
  std::unordered_map<std::pair<uint32_t, uint32_t>, bool, CodePairHash> memo;
  std::vector<std::pair<uint32_t, uint32_t>> out;
  for (const auto& p : pairs) {
    auto [it, inserted] = memo.emplace(p, false);
    if (inserted) {
      const std::string_view t = title_of(p.first);
      const std::string_view u = url_of(p.second);
      it->second = (t.find("Google") != std::string_view::npos) &&
                   (u.find(".google.") == std::string_view::npos);
    }
    if (it->second) {
      out.push_back(p);
    }
  }
  return out;
}

// Q24: TimestampFenceBeforeSubstringRead.Goal
// Skips reading URL/LIKE payloads for a page whose minimum timestamp `lower`
// is strictly greater than `cutoff` once at least `k` (10) rows have `<=
// cutoff`.
[[nodiscard]] inline bool CanSkipPageByTimestampFence(
    std::span<const uint64_t> previous_timestamps, uint64_t cutoff,
    uint64_t page_lower_bound, size_t k = 10U) noexcept {
  if (page_lower_bound <= cutoff) {
    return false;
  }
  size_t le_count = 0;
  for (const uint64_t ts : previous_timestamps) {
    if (ts <= cutoff) {
      ++le_count;
      if (le_count >= k) {
        return true;
      }
    }
  }
  return false;
}

// Q27: TimestampTieBoundaryForSecondarySort.Goal
// Scans a timestamp-sorted stream only up through the `k`-th timestamp's tie
// boundary `t`, then secondary-sorts `(timestamp, secondary_key)` within that
// prefix.
[[nodiscard]] inline std::vector<std::pair<uint64_t, uint64_t>>
TopKWithinTimestampTieBoundary(
    std::span<const std::pair<uint64_t, uint64_t>> ts_sorted_rows,
    uint64_t tie_timestamp, size_t k = 10U) {
  std::vector<std::pair<uint64_t, uint64_t>> prefix;
  for (const auto& row : ts_sorted_rows) {
    if (row.first > tie_timestamp) {
      break;
    }
    prefix.push_back(row);
  }
  std::ranges::sort(prefix, [](const auto& a, const auto& b) {
    return a.first < b.first || (a.first == b.first && a.second < b.second);
  });
  if (prefix.size() > k) {
    prefix.resize(k);
  }
  return prefix;
}

// Q28: TotalFrequencyBoundsNonemptyHaving.Goal
// Contrapositive: if a group's total row count is `<= threshold` (100000), its
// non-empty URL count cannot exceed `threshold`, so HAVING fails immediately.
[[nodiscard]] inline bool CanPassNonemptyHavingByTotalFreq(
    uint64_t total_group_rows,
    uint64_t having_gt_threshold = 100000U) noexcept {
  return total_group_rows > having_gt_threshold;
}

// Q31: SplitIpKeyReconstruction.Goal
// Bijectively splits `(engine, ip)` into `(engine, ip >> 8, ip & 0xFF)` for
// prefix-grouped low-byte array aggregation.
struct SplitIpKey {
  uint64_t engine{0};
  uint64_t ip_hi{0};
  uint8_t ip_lo{0};
  bool operator==(const SplitIpKey& other) const noexcept = default;
};

[[nodiscard]] inline SplitIpKey SplitEngineIpKey(uint64_t engine,
                                                 uint64_t ip) noexcept {
  return {.engine = engine,
          .ip_hi = ip >> 8U,
          .ip_lo = static_cast<uint8_t>(ip & 0xFFU)};
}

[[nodiscard]] inline std::pair<uint64_t, uint64_t> ReconstructEngineIpKey(
    const SplitIpKey& k) noexcept {
  return {k.engine, (k.ip_hi << 8U) | k.ip_lo};
}

// Q32: SingleValueGroupWithConflictFallback.Goal
// Stores the first IP per WatchID in a single inline slot and spills only
// conflicting IPs (`ip != first`) into an exception table.
struct SingleValueWithExceptions {
  uint64_t first_val{0};
  uint64_t total_count{0};
  std::unordered_map<uint64_t, uint64_t> exceptions;

  void Insert(uint64_t val) {
    if (total_count == 0) {
      first_val = val;
      total_count = 1;
      return;
    }
    ++total_count;
    if (val != first_val) {
      ++exceptions[val];
    }
  }

  [[nodiscard]] uint64_t CountOf(uint64_t query) const noexcept {
    if (total_count == 0) {
      return 0;
    }
    if (query == first_val) {
      uint64_t exc_total = 0;
      for (const auto& [k, c] : exceptions) {
        (void)k;
        exc_total += c;
      }
      return total_count - exc_total;
    }
    const auto it = exceptions.find(query);
    return it != exceptions.end() ? it->second : 0U;
  }
};

// Q36: DerivedIpKeysAfterGrouping.Goal
// Groups by `ClientIP` alone and computes `(ip, ip - 1, ip - 2, ip - 3)` only
// after group counts are finalized.
struct DerivedIpGroupRow {
  int64_t ip0{0};
  int64_t ip1{0};
  int64_t ip2{0};
  int64_t ip3{0};
  uint64_t count{0};
  bool operator==(const DerivedIpGroupRow& other) const noexcept = default;
};

[[nodiscard]] inline std::vector<DerivedIpGroupRow>
AttachDerivedIpKeysAfterGrouping(std::span<const int64_t> ips) {
  std::unordered_map<int64_t, uint64_t> freq;
  std::vector<int64_t> order;
  for (const int64_t ip : ips) {
    auto [it, inserted] = freq.emplace(ip, 0U);
    if (inserted) {
      order.push_back(ip);
    }
    ++it->second;
  }
  std::vector<DerivedIpGroupRow> out;
  out.reserve(order.size());
  for (const int64_t ip : order) {
    out.push_back({.ip0 = ip,
                   .ip1 = ip - 1,
                   .ip2 = ip - 2,
                   .ip3 = ip - 3,
                   .count = freq[ip]});
  }
  return out;
}

// Q37: IntervalCountFromSortedPostings.Goal
// Counts posting dates in closed interval `[lo, hi]` over sorted postings in
// O(log n) using `upper_bound(hi) - lower_bound(lo)`.
[[nodiscard]] inline size_t SortedPostingsIntervalCount(
    std::span<const uint64_t> sorted_days, uint64_t lo, uint64_t hi) noexcept {
  if (lo > hi) {
    return 0;
  }
  const auto first = std::ranges::lower_bound(sorted_days, lo);
  const auto last = std::ranges::upper_bound(sorted_days, hi);
  return static_cast<size_t>(last - first);
}

// Q38: PageFrequencyBoundsForTitleCandidates.Goal
// If every non-candidate title occurs at most `cap` times per page across
// `page_count` pages, then any title with global count `> cap * page_count`
// MUST belong to `candidates`.
[[nodiscard]] inline bool MustBeInTitleCandidatesByPageCap(
    uint64_t per_page_non_candidate_cap, uint64_t page_count,
    uint64_t global_count) noexcept {
  return (static_cast<unsigned __int128>(per_page_non_candidate_cap) *
          static_cast<unsigned __int128>(page_count)) <
         static_cast<unsigned __int128>(global_count);
}

// Q39: OffsetSelectionByCountBuckets.Goal
// Given a descending-count bucket `cutoff` whose strictly-higher count is
// `higher <= offset` and `offset + limit <= higher + tied.size()`, slices the
// `[offset, offset + limit)` window strictly inside `tied`.
[[nodiscard]] inline std::optional<std::vector<std::pair<uint64_t, uint64_t>>>
SelectOffsetWindowFromCountBuckets(
    std::span<const std::pair<uint64_t, uint64_t>> groups, uint64_t cutoff,
    size_t offset = 1000U, size_t limit = 10U) {
  size_t higher = 0;
  std::vector<std::pair<uint64_t, uint64_t>> tied;
  for (const auto& g : groups) {
    if (g.second > cutoff) {
      ++higher;
    } else if (g.second == cutoff) {
      tied.push_back(g);
    }
  }
  if (higher > offset || offset + limit > higher + tied.size()) {
    return std::nullopt;
  }
  const size_t local_off = offset - higher;
  std::vector<std::pair<uint64_t, uint64_t>> out(
      tied.begin() + static_cast<std::ptrdiff_t>(local_off),
      tied.begin() + static_cast<std::ptrdiff_t>(local_off + limit));
  return out;
}

// Q40: DisjointReferrerBranchKeys.Goal
// Separates the `(s == 0 && a == 0, referrer)` branch from the `(s, a, "")`
// branch into disjoint key spaces so non-zero `(s, a)` rows never materialize
// or hash `Referer`.
struct ReferrerBranchRow {
  uint64_t search_engine_id{0};
  uint64_t adv_engine_id{0};
  std::string referrer;
};

[[nodiscard]] inline uint64_t CountDisjointReferrerBranchKey(
    std::span<const ReferrerBranchRow> rows, uint64_t s, uint64_t a,
    std::string_view src) noexcept {
  if (s == 0 && a == 0) {
    uint64_t cnt = 0;
    for (const auto& r : rows) {
      if (r.search_engine_id == 0 && r.adv_engine_id == 0 &&
          r.referrer == src) {
        ++cnt;
      }
    }
    return cnt;
  }
  if (!src.empty()) {
    return 0;
  }
  uint64_t cnt = 0;
  for (const auto& r : rows) {
    if (r.search_engine_id == s && r.adv_engine_id == a) {
      ++cnt;
    }
  }
  return cnt;
}

// Q42: RowCountBoundsDistinctOffset.Goal
// Short-circuits DISTINCT + OFFSET to empty whenever the pre-distinct
// surviving row count is `<= offset` (avoiding resolution-pair decode).
[[nodiscard]] inline bool RowCountForcesEmptyDistinctOffset(
    size_t surviving_rows, size_t offset = 10000U) noexcept {
  return surviving_rows <= offset;
}

// Q43: MinuteTruncationFromBlockBounds.Goal
// Uses Lean floor-division semantics (`lo / 60 == hi / 60` for non-negative
// epoch seconds, or Euclidean floor div for general signed ints) so when a
// block's `[min_sec, max_sec]` falls in one minute, row-wise `/ 60` is elided.
[[nodiscard]] inline int64_t FloorDiv60(int64_t sec) noexcept {
  int64_t q = sec / 60;
  const int64_t r = sec % 60;
  if (r < 0) {
    --q;
  }
  return q;
}

[[nodiscard]] inline std::optional<int64_t> TryConstantBlockMinuteTruncation(
    int64_t min_sec, int64_t max_sec) noexcept {
  if (min_sec > max_sec) {
    return std::nullopt;
  }
  const int64_t lo_min = FloorDiv60(min_sec);
  const int64_t hi_min = FloorDiv60(max_sec);
  return lo_min == hi_min ? std::optional<int64_t>(lo_min) : std::nullopt;
}

}  // namespace tinylamb::query_rewrites

#endif  // TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_CLICKBENCH_HPP
