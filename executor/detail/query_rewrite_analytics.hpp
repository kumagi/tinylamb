/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_ANALYTICS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_ANALYTICS_HPP

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

#include "executor/detail/query_rewrite_tpcds.hpp"

namespace tinylamb::query_rewrites {

// ============================================================================
// Analytics (16) & Cloud/BI (2) QueryRewrites — 18 proved theorems
// Lean models:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean
// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/QueryRewrites/AnalyticalJoins/
//   lean/TinylambOpt/ResearchProofs/QueryRewrites/FileRangesAndExtrema/
// ============================================================================

// JOB J1: CompanyWeightedMovieYearTotals.Goal
// Weights each `(movie_id, production_year)` row by its qualified
// `movie_companies` multiplicity instead of expanding the join.
struct CountAndSum {
  uint64_t count{0};
  int64_t sum{0};
  bool operator==(const CountAndSum& other) const noexcept = default;
};

[[nodiscard]] inline CountAndSum CompanyWeightedMovieYearTotals(
    std::span<const std::pair<uint64_t, int64_t>> movie_years,
    std::span<const uint64_t> companies) {
  std::unordered_map<uint64_t, uint64_t> freq;
  for (const uint64_t mid : companies) {
    ++freq[mid];
  }
  CountAndSum out;
  for (const auto& [mid, year] : movie_years) {
    const auto it = freq.find(mid);
    if (it != freq.end()) {
      out.count += it->second;
      out.sum += year * static_cast<int64_t>(it->second);
    }
  }
  return out;
}

// JOB J2: RatingAndGenreJoinMultiplicity.Goal
// Probes `drama` only for titles present in `ratings`, accumulating
// `drama.count(k) * ratings.count(k)` without expanding intermediate pairs.
[[nodiscard]] inline uint64_t RatingAndGenreJoinMultiplicity(
    std::span<const uint64_t> titles, std::span<const uint64_t> drama,
    std::span<const uint64_t> ratings) {
  std::unordered_map<uint64_t, uint64_t> rating_freq;
  for (const uint64_t r : ratings) {
    ++rating_freq[r];
  }
  std::unordered_map<uint64_t, uint64_t> drama_freq;
  for (const uint64_t d : drama) {
    if (rating_freq.contains(d)) {
      ++drama_freq[d];
    }
  }
  uint64_t total = 0;
  for (const uint64_t t : titles) {
    const auto rit = rating_freq.find(t);
    if (rit == rating_freq.end()) {
      continue;
    }
    const auto dit = drama_freq.find(t);
    if (dit != drama_freq.end()) {
      total += dit->second * rit->second;
    }
  }
  return total;
}

// JOB J3: OptionalYearByteCodeTotals.Goal
// Packs an optional year in `[1970, 2010]` into 1 byte (`0` = ineligible,
// `1..41` = `year - 1969`), and recovers `(hits, sum_years)` directly from
// `(hits, code_sum + 1969 * hits)`.
[[nodiscard]] inline uint8_t EncodeOptionalYearByteCode(
    std::optional<uint32_t> year) noexcept {
  if (!year.has_value() || *year < 1970U || *year > 2010U) {
    return 0U;
  }
  return static_cast<uint8_t>(*year - 1969U);
}

[[nodiscard]] inline CountAndSum SumOptionalYearByteCodes(
    std::span<const uint8_t> codes) noexcept {
  uint64_t hits = 0;
  uint64_t code_sum = 0;
  for (const uint8_t c : codes) {
    if (c != 0U) {
      ++hits;
      code_sum += c;
    }
  }
  return {.count = hits,
          .sum = static_cast<int64_t>(code_sum + (1969ULL * hits))};
}

// JOB J5: AliasPrefixHalfOpenRange.Goal
// Rewrites prefix check `starts_with("aka-1")` into half-open lexicographic
// range `["aka-1", "aka-2")` under identical byte-order collation.
[[nodiscard]] inline bool MatchesAka1PrefixRange(
    std::string_view alias) noexcept {
  return alias >= "aka-1" && alias < "aka-2";
}

// STATS-CEB C1: ScoreAndViewCountZonePruning.Goal
// Prunes `posts` pages whose `max(score) <= 5 || min(view_count) >= 10000`
// while preserving qualifying post ID order.
struct PostZoneRow {
  int64_t score{0};
  int64_t view_count{0};
  uint64_t post_id{0};
};

[[nodiscard]] inline std::vector<uint64_t> PrunePostPagesByScoreAndViews(
    std::span<const std::vector<PostZoneRow>> pages) {
  std::vector<uint64_t> out;
  for (const auto& page : pages) {
    if (page.empty()) {
      continue;
    }
    int64_t max_score = page[0].score;
    int64_t min_views = page[0].view_count;
    for (const auto& r : page) {
      max_score = std::max(max_score, r.score);
      min_views = std::min(min_views, r.view_count);
    }
    if (max_score <= 5 || min_views >= 10000) {
      continue;
    }
    for (const auto& r : page) {
      if (r.score > 5 && r.view_count < 10000) {
        out.push_back(r.post_id);
      }
    }
  }
  return out;
}

// STATS-CEB C2: OwnerCommentFrequencyJoinCount.Goal
// Collapses an owner's qualifying posts into a frequency table before summing
// `post_freq * passed_comments.count(post_id)`.
[[nodiscard]] inline uint64_t OwnerCommentFrequencyJoinCount(
    std::span<const std::pair<uint64_t, uint64_t>> owner_posts,
    std::span<const uint64_t> passed_comments, uint64_t target_owner) {
  std::unordered_map<uint64_t, uint64_t> owned_freq;
  for (const auto& [owner, post_id] : owner_posts) {
    if (owner == target_owner) {
      ++owned_freq[post_id];
    }
  }
  std::unordered_map<uint64_t, uint64_t> comment_freq;
  for (const uint64_t pid : passed_comments) {
    if (owned_freq.contains(pid)) {
      ++comment_freq[pid];
    }
  }
  uint64_t total = 0;
  for (const auto& [pid, mult] : owned_freq) {
    const auto it = comment_freq.find(pid);
    if (it != comment_freq.end()) {
      total += mult * it->second;
    }
  }
  return total;
}

// H2O H2: StableDescendingCountBuckets.Goal
// Scatters completed `(group_id, count)` entries into `0..max_count` buckets
// and emits them in descending count order, matching stable mergeSort.
[[nodiscard]] inline std::vector<std::pair<uint64_t, uint64_t>>
StableDescendingCountBucketSort(
    std::span<const std::pair<uint64_t, uint64_t>> groups, uint64_t max_count) {
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> buckets(
      static_cast<size_t>(max_count) + 1U);
  for (const auto& g : groups) {
    buckets[static_cast<size_t>(g.second)].push_back(g);
  }
  std::vector<std::pair<uint64_t, uint64_t>> out;
  out.reserve(groups.size());
  for (size_t b = buckets.size(); b-- > 0;) {
    out.insert(out.end(), buckets[b].begin(), buckets[b].end());
  }
  return out;
}

// H2O H4: OrderedNullableValuesFromSelectedRuns.Goal
// Passes only contiguous runs with `id3 < 4` into the non-NULL value stream,
// preserving exact input order for floating-point/exact AVG.
struct KeyedNullableRun {
  uint64_t key{0};
  std::vector<std::optional<int64_t>> values;
};

[[nodiscard]] inline std::vector<int64_t> OrderedNullableValuesFromSelectedRuns(
    std::span<const KeyedNullableRun> runs, uint64_t key_upper_exclusive = 4U) {
  std::vector<int64_t> out;
  for (const auto& run : runs) {
    if (run.key >= key_upper_exclusive) {
      continue;
    }
    for (const auto& v : run.values) {
      if (v.has_value()) {
        out.push_back(*v);
      }
    }
  }
  return out;
}

// H2O H5: BoundedCartesianProductTiles.Goal
// Partitions `xs x ys` into tiles of at most `cap` pairs each whose
// concatenation equals the exact Cartesian product in row-major order.
[[nodiscard]] inline std::vector<std::vector<std::pair<int64_t, int64_t>>>
BuildBoundedCartesianProductTiles(std::span<const int64_t> xs,
                                  std::span<const int64_t> ys, size_t cap) {
  if (xs.empty() || ys.empty() || cap == 0) {
    return {};
  }
  const size_t total = xs.size() * ys.size();
  const size_t n_tiles = (total + cap - 1U) / cap;
  std::vector<std::vector<std::pair<int64_t, int64_t>>> tiles(n_tiles);
  for (size_t t = 0; t < n_tiles; ++t) {
    const size_t start = t * cap;
    const size_t len = std::min(cap, total - start);
    tiles[t].reserve(len);
    for (size_t j = 0; j < len; ++j) {
      const size_t p = start + j;
      tiles[t].emplace_back(xs[p / ys.size()], ys[p % ys.size()]);
    }
  }
  return tiles;
}

// LDBC SNB L2: ForumMembershipSemijoinWeights.Goal
// Semijoins `(forum_id, person_id)` membership against active `forums` before
// weighting by `forums.count(forum_id) * age_matches(person_id)`.
template <typename AgeMatchFn>
[[nodiscard]] uint64_t ForumMembershipSemijoinWeights(
    std::span<const std::pair<uint64_t, uint64_t>> members,
    std::span<const uint64_t> forums, AgeMatchFn&& age_matches) {
  std::unordered_map<uint64_t, uint64_t> forum_freq;
  for (const uint64_t f : forums) {
    ++forum_freq[f];
  }
  uint64_t total = 0;
  for (const auto& [forum_id, person_id] : members) {
    const auto it = forum_freq.find(forum_id);
    if (it != forum_freq.end()) {
      total += it->second * age_matches(person_id);
    }
  }
  return total;
}

// LDBC SNB L3: ParentReplyCountAndScoreTotals.Goal
// Pre-aggregates positive-score child replies by `parent_id` into `(count,
// sum)` and multiplies by parent multiplicity.
[[nodiscard]] inline CountAndSum ParentReplyCountAndScoreTotals(
    std::span<const std::pair<std::optional<uint64_t>, int64_t>> children,
    std::span<const uint64_t> parents) {
  std::unordered_map<uint64_t, CountAndSum> reply_stats;
  for (const auto& [parent_opt, score] : children) {
    if (parent_opt.has_value() && score > 0) {
      auto& st = reply_stats[*parent_opt];
      ++st.count;
      st.sum += score;
    }
  }
  std::unordered_map<uint64_t, uint64_t> parent_freq;
  for (const uint64_t p : parents) {
    ++parent_freq[p];
  }
  CountAndSum out;
  for (const auto& [p, mult] : parent_freq) {
    const auto it = reply_stats.find(p);
    if (it != reply_stats.end()) {
      out.count += mult * it->second.count;
      out.sum += static_cast<int64_t>(mult) * it->second.sum;
    }
  }
  return out;
}

// LDBC SNB L4: NullableParentAndYearPositionGather.Goal
// Intersects `year == 2012` with `parent.has_value()` validity bits to gather
// nullable scores in position order without decoding parent IDs.
struct YearParentScoreRow {
  uint64_t year{0};
  std::optional<uint64_t> parent_id;
  std::optional<int64_t> score;
};

[[nodiscard]] inline std::vector<std::optional<int64_t>>
GatherScoresByYearAndValidParent(std::span<const YearParentScoreRow> rows,
                                 uint64_t target_year = 2012U) {
  std::vector<size_t> indices;
  for (size_t i = 0; i < rows.size(); ++i) {
    if (rows[i].year == target_year && rows[i].parent_id.has_value()) {
      indices.push_back(i);
    }
  }
  std::vector<std::optional<int64_t>> out;
  out.reserve(indices.size());
  for (const size_t idx : indices) {
    out.push_back(rows[idx].score);
  }
  return out;
}

// LDBC SNB L5: PlaceGroupedCountryAgeTotals.Goal
// Aggregates `(total_rows, nullable_sum, non_null_count)` per `place_id` first,
// then resolves `country(place_id) == target_country` once per distinct place.
struct PlaceAgeAggregate {
  uint64_t total_count{0};
  std::optional<int64_t> nullable_sum;
  uint64_t non_null_count{0};
  bool operator==(const PlaceAgeAggregate& other) const noexcept = default;
};

template <typename CountryFn>
[[nodiscard]] PlaceAgeAggregate PlaceGroupedCountryAgeTotals(
    std::span<const std::pair<uint64_t, std::optional<int64_t>>> edge_ages,
    CountryFn&& country_of, uint64_t target_country) {
  std::unordered_map<uint64_t, PlaceAgeAggregate> by_place;
  for (const auto& [place, age] : edge_ages) {
    auto& agg = by_place[place];
    ++agg.total_count;
    if (age.has_value()) {
      agg.nullable_sum =
          agg.nullable_sum.has_value() ? (*agg.nullable_sum + *age) : *age;
      ++agg.non_null_count;
    }
  }
  PlaceAgeAggregate out;
  for (const auto& [place, agg] : by_place) {
    if (country_of(place) == target_country) {
      out.total_count += agg.total_count;
      out.non_null_count += agg.non_null_count;
      if (agg.nullable_sum.has_value()) {
        out.nullable_sum = out.nullable_sum.has_value()
                               ? (*out.nullable_sum + *agg.nullable_sum)
                               : *agg.nullable_sum;
      }
    }
  }
  return out;
}

// BigBench B3: PhysicalKeySubtotalRollup.Goal
// Primary-aggregates nullable sales by physical `(item_sk, store_sk)` key, then
// rolls up to logical `(category, region)` labels once per distinct pair.
template <typename LabelFn>
[[nodiscard]] std::optional<int64_t> RollupPhysicalKeySubtotals(
    std::span<
        const std::pair<std::pair<uint64_t, uint64_t>, std::optional<int64_t>>>
        sales,
    LabelFn&& label_of, std::pair<uint64_t, uint64_t> target_label) {
  std::unordered_map<std::pair<uint64_t, uint64_t>, std::optional<int64_t>,
                     PairU64Hash>
      by_phys;
  for (const auto& [pk, val] : sales) {
    auto& slot = by_phys[pk];
    if (val.has_value()) {
      slot = slot.has_value() ? (*slot + *val) : *val;
    }
  }
  bool has_any = false;
  int64_t acc = 0;
  for (const auto& [pk, subtotal] : by_phys) {
    if (label_of(pk) == target_label && subtotal.has_value()) {
      acc += *subtotal;
      has_any = true;
    }
  }
  return has_any ? std::optional<int64_t>(acc) : std::nullopt;
}

// BigBench B4: DictionarySubstringOrFlagSelection.Goal
// Evaluates `phrase.contains("data")` once per unique dictionary phrase and
// returns the deduplicated row indices matching `flag ||
// dict_contains(phrase)`.
[[nodiscard]] inline std::vector<size_t> DictionarySubstringOrFlagSelection(
    std::span<const std::pair<bool, std::string>> rows,
    std::string_view needle = "data") {
  std::unordered_set<std::string_view> matching_dict;
  std::unordered_set<std::string_view> seen;
  for (const auto& [flag, phrase] : rows) {
    (void)flag;
    if (seen.insert(phrase).second &&
        phrase.find(needle) != std::string::npos) {
      matching_dict.insert(phrase);
    }
  }
  std::vector<size_t> out;
  for (size_t i = 0; i < rows.size(); ++i) {
    if (rows[i].first || matching_dict.contains(rows[i].second)) {
      out.push_back(i);
    }
  }
  return out;
}

// BigBench B5: SortedPriceSuffixKeyCounts.Goal
// Binary-searches a price-sorted `(price, item_key)` index for `price > cutoff`
// (default 100) and counts occurrences of `target_key` in the suffix.
[[nodiscard]] inline size_t SortedPriceSuffixKeyCount(
    std::span<const std::pair<int64_t, uint64_t>> price_sorted_items,
    uint64_t target_key, int64_t price_cutoff = 100) noexcept {
  const auto it = std::ranges::upper_bound(
      price_sorted_items, price_cutoff, {},
      [](const std::pair<int64_t, uint64_t>& r) { return r.first; });
  size_t cnt = 0;
  for (auto cur = it; cur != price_sorted_items.end(); ++cur) {
    if (cur->second == target_key) {
      ++cnt;
    }
  }
  return cnt;
}

// Public BI MLB Q108: PairedExtremaReduction.Goal
// Computes `(min, max)` over non-NULL values using pairwise comparison
// (`<= 3n/2 - 2` comparisons instead of `2n - 2`).
struct MinMaxPair {
  std::optional<int64_t> min_val;
  std::optional<int64_t> max_val;
  bool operator==(const MinMaxPair& other) const noexcept = default;
};

[[nodiscard]] inline MinMaxPair PairedExtremaReduction(
    std::span<const std::optional<int64_t>> values) noexcept {
  std::optional<int64_t> lo;
  std::optional<int64_t> hi;
  std::optional<int64_t> pending;
  for (const auto& opt : values) {
    if (!opt.has_value()) {
      continue;
    }
    if (!pending.has_value()) {
      pending = opt;
      continue;
    }
    const int64_t a = *pending;
    const int64_t b = *opt;
    pending.reset();
    const int64_t small = a <= b ? a : b;
    const int64_t large = a <= b ? b : a;
    lo = lo.has_value() ? std::min(*lo, small) : small;
    hi = hi.has_value() ? std::max(*hi, large) : large;
  }
  if (pending.has_value()) {
    lo = lo.has_value() ? std::min(*lo, *pending) : *pending;
    hi = hi.has_value() ? std::max(*hi, *pending) : *pending;
  }
  return {.min_val = lo, .max_val = hi};
}

// Iceberg SQL base: ContiguousFileRowIdTotals.Goal
// Synthesizes `(row_count, rid_sum)` from contiguous per-file `(file_id, base,
// n)` RID runs weighted by manifest file multiplicity via arithmetic-series
// formula `n * (2 * base + n - 1) / 2`.
struct ContiguousFileRun {
  uint64_t file_id{0};
  uint64_t base_rid{0};
  uint64_t count{0};
};

struct FileRowIdTotals {
  uint64_t row_count{0};
  std::optional<uint64_t> rid_sum;
  bool operator==(const FileRowIdTotals& other) const noexcept = default;
};

[[nodiscard]] inline FileRowIdTotals ContiguousFileRowIdTotals(
    std::span<const ContiguousFileRun> runs,
    std::span<const uint64_t> manifest_files) {
  std::unordered_map<uint64_t, uint64_t> file_mult;
  for (const uint64_t f : manifest_files) {
    ++file_mult[f];
  }
  uint64_t total_rows = 0;
  unsigned __int128 total_sum = 0;
  for (const auto& run : runs) {
    if (run.count == 0) {
      continue;
    }
    const auto it = file_mult.find(run.file_id);
    if (it == file_mult.end()) {
      continue;
    }
    const uint64_t m = it->second;
    total_rows += m * run.count;
    const unsigned __int128 arith =
        (static_cast<unsigned __int128>(run.count) *
         ((2U * static_cast<unsigned __int128>(run.base_rid)) + run.count -
          1U)) /
        2U;
    total_sum += static_cast<unsigned __int128>(m) * arith;
  }
  return {.row_count = total_rows,
          .rid_sum = total_rows > 0 ? std::optional<uint64_t>(
                                          static_cast<uint64_t>(total_sum))
                                    : std::nullopt};
}

}  // namespace tinylamb::query_rewrites

#endif  // TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_ANALYTICS_HPP
