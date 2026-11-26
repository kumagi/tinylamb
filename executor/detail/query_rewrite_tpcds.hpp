/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_TPCDS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_TPCDS_HPP

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "executor/detail/query_rewrite_tpch.hpp"

namespace tinylamb::query_rewrites {

// ============================================================================
// TPC-DS QueryRewrites (37 proved theorems)
// Lean models:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean
// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/QueryRewrites/SalesAndReturns/
// ============================================================================

// Q2: DateKeyPreaggregationBeforeWeekLookup.Goal
// Pre-aggregates nullable sums by `sold_date_sk` first, then probes `week_day`
// once per distinct date key to accumulate the target `(week, day)` cell.
template <typename WeekDayFn>
[[nodiscard]] std::optional<int64_t> PreaggregateDateKeysThenRollupCell(
    std::span<const std::pair<uint64_t, std::optional<int64_t>>> sales,
    WeekDayFn&& week_day, std::pair<uint64_t, uint64_t> target_cell) {
  std::unordered_map<uint64_t, std::optional<int64_t>> by_date;
  for (const auto& [dsk, val] : sales) {
    auto& slot = by_date[dsk];
    if (val.has_value()) {
      slot = slot.has_value() ? (*slot + *val) : *val;
    }
  }
  bool has_any = false;
  int64_t acc = 0;
  for (const auto& [dsk, subtotal] : by_date) {
    if (week_day(dsk) == target_cell && subtotal.has_value()) {
      acc += *subtotal;
      has_any = true;
    }
  }
  return has_any ? std::optional<int64_t>(acc) : std::nullopt;
}

// Q4: PositivePriorYearDenominatorFilter.Goal
// Filters second-year sales rows to customers whose prior-year total is
// strictly positive (`> 0`).
[[nodiscard]] inline std::vector<std::pair<uint64_t, std::optional<int64_t>>>
FilterSalesByPositivePriorYear(
    const std::unordered_map<uint64_t, int64_t>& prior_year_totals,
    std::span<const std::pair<uint64_t, std::optional<int64_t>>> sales) {
  std::vector<std::pair<uint64_t, std::optional<int64_t>>> out;
  for (const auto& row : sales) {
    const auto it = prior_year_totals.find(row.first);
    if (it != prior_year_totals.end() && it->second > 0) {
      out.push_back(row);
    }
  }
  return out;
}

// Q11: PositiveDenominatorRatioCrossProduct.Goal
// Compares `a / b > c / d` for positive denominators (`b > 0, d > 0`) using
// exact widened 128-bit integer cross multiplication `a * d > c * b`.
[[nodiscard]] inline bool ComparePositiveDenominatorRatios(int64_t a, int64_t b,
                                                           int64_t c,
                                                           int64_t d) noexcept {
  return (static_cast<__int128>(a) * static_cast<__int128>(d)) >
         (static_cast<__int128>(c) * static_cast<__int128>(b));
}

// Q14: HavingFilteredNullableRollup.Goal
// Rolls up only leaf groups whose nullable sum strictly exceeds `threshold`.
[[nodiscard]] inline std::optional<int64_t> RollupHavingFilteredNullableGroups(
    std::span<const std::vector<std::optional<int64_t>>> cells,
    int64_t threshold) noexcept {
  bool has_any = false;
  int64_t acc = 0;
  for (const auto& cell : cells) {
    const std::optional<int64_t> gsum = NullableSum(cell);
    if (gsum.has_value() && *gsum > threshold) {
      acc += *gsum;
      has_any = true;
    }
  }
  return has_any ? std::optional<int64_t>(acc) : std::nullopt;
}

// Q15: PostalPrefixDictionaryFilter.Goal
// Precomputes allowed 5-character ZIP prefixes over the address dictionary
// once, then filters row codes via O(1) dictionary membership lookup.
[[nodiscard]] inline std::vector<uint32_t> FilterCodesByPostalPrefixDict(
    std::span<const std::string> zip_dict,
    std::span<const std::string> allowed_prefixes,
    std::span<const uint32_t> codes) {
  std::unordered_set<std::string_view> allowed(allowed_prefixes.begin(),
                                               allowed_prefixes.end());
  std::vector<uint8_t> good(zip_dict.size(), 0U);
  for (size_t i = 0; i < zip_dict.size(); ++i) {
    const std::string_view prefix(zip_dict[i].data(),
                                  std::min<size_t>(5U, zip_dict[i].size()));
    if (allowed.contains(prefix)) {
      good[i] = 1U;
    }
  }
  std::vector<uint32_t> out;
  for (const uint32_t c : codes) {
    if (c < good.size() && good[c] != 0U) {
      out.push_back(c);
    }
  }
  return out;
}

// Q16: CandidateRestrictedNullAwareReturnAntijoin.Goal
// Builds the return exclusion set restricted to non-NULL candidate order keys,
// preserving SQL NULL semantics (NULL order never matches any return).
[[nodiscard]] inline std::vector<std::optional<uint64_t>>
CandidateRestrictedNullAwareAntijoin(
    std::span<const std::optional<uint64_t>> orders,
    std::span<const std::optional<uint64_t>> returns) {
  std::unordered_set<uint64_t> cand_non_null;
  for (const auto& o : orders) {
    if (o.has_value()) {
      cand_non_null.insert(*o);
    }
  }
  std::unordered_set<uint64_t> excluded;
  for (const auto& r : returns) {
    if (r.has_value() && cand_non_null.contains(*r)) {
      excluded.insert(*r);
    }
  }
  std::vector<std::optional<uint64_t>> out;
  for (const auto& o : orders) {
    if (!o.has_value() || !excluded.contains(*o)) {
      out.push_back(o);
    }
  }
  return out;
}

// Q17: ShiftedVarianceNumerator.Goal
// Computes `n * sum(x^2) - (sum x)^2` shifted by `origin` (`d = x - origin`),
// which is algebraically identical while keeping intermediate products small.
[[nodiscard]] inline __int128 ShiftedVarianceNumerator(
    std::span<const int64_t> values, int64_t origin) noexcept {
  __int128 sum_d = 0;
  __int128 sum_d2 = 0;
  for (const int64_t x : values) {
    const __int128 d = static_cast<__int128>(x) - static_cast<__int128>(origin);
    sum_d += d;
    sum_d2 += d * d;
  }
  return (static_cast<__int128>(values.size()) * sum_d2) - (sum_d * sum_d);
}

// Q21: EligibleItemRangeBlockPruning.Goal
// Prunes inventory blocks whose `[min_item, max_item]` range contains no
// eligible item key from `eligible_items`.
[[nodiscard]] inline std::vector<uint64_t> PruneBlocksByEligibleItemRanges(
    std::span<const std::vector<uint64_t>> blocks,
    std::span<const uint64_t> eligible_items) {
  std::unordered_set<uint64_t> eligible_set(eligible_items.begin(),
                                            eligible_items.end());
  std::vector<uint64_t> out;
  for (const auto& blk : blocks) {
    if (blk.empty()) {
      continue;
    }
    const auto [min_it, max_it] = std::minmax_element(blk.begin(), blk.end());
    const bool possible = std::ranges::any_of(eligible_items, [&](uint64_t k) {
      return *min_it <= k && k <= *max_it;
    });
    if (!possible) {
      continue;
    }
    for (const uint64_t item : blk) {
      if (eligible_set.contains(item)) {
        out.push_back(item);
      }
    }
  }
  return out;
}

// Q25: SalesKeyFilterOnReturnJoin.Goal
// Restricts returns to `(customer, item, ticket)` keys present in sales before
// probing catalog multiplicities.
struct CustItemTicketKey {
  uint64_t customer{0};
  uint64_t item{0};
  uint64_t ticket{0};
  bool operator==(const CustItemTicketKey& other) const noexcept = default;
};

struct CustItemTicketHash {
  size_t operator()(const CustItemTicketKey& k) const noexcept {
    return (k.customer * 11400714819323198485ULL) ^
           (k.item * 14029467366897019727ULL) ^ k.ticket;
  }
};

template <typename CatalogCountFn>
[[nodiscard]] uint64_t CountSalesFilteredReturnCatalogJoin(
    std::span<const CustItemTicketKey> sales,
    std::span<const CustItemTicketKey> returns,
    CatalogCountFn&& catalog_count) {
  std::unordered_map<CustItemTicketKey, uint64_t, CustItemTicketHash>
      sales_freq;
  for (const auto& k : sales) {
    ++sales_freq[k];
  }
  uint64_t total = 0;
  for (const auto& r : returns) {
    const auto it = sales_freq.find(r);
    if (it != sales_freq.end()) {
      total += it->second * catalog_count(r.customer, r.item);
    }
  }
  return total;
}

// Q29: CachedCatalogQuantityByCustomerItem.Goal
// Caches the nullable catalog quantity sum per unique `(customer, item)` pair
// and multiplies by matched-ticket multiplicity.
struct PairU64Hash {
  size_t operator()(const std::pair<uint64_t, uint64_t>& p) const noexcept {
    return (p.first * 11400714819323198485ULL) ^
           (p.second + 0x9e3779b97f4a7c15ULL);
  }
};

template <typename CatalogLookupFn>
[[nodiscard]] std::optional<int64_t> SumCachedCatalogQuantityByPair(
    std::span<const std::pair<uint64_t, uint64_t>> matched_tickets,
    CatalogLookupFn&& catalog_quantities) {
  std::unordered_map<std::pair<uint64_t, uint64_t>, uint64_t, PairU64Hash> freq;
  for (const auto& p : matched_tickets) {
    ++freq[p];
  }
  bool has_any = false;
  int64_t acc = 0;
  for (const auto& [p, mult] : freq) {
    const std::optional<int64_t> pair_sum = NullableSum(catalog_quantities(p));
    if (pair_sum.has_value()) {
      acc += (*pair_sum) * static_cast<int64_t>(mult);
      has_any = true;
    }
  }
  return has_any ? std::optional<int64_t>(acc) : std::nullopt;
}

// Q30: AverageRatioComparisonByCrossProduct.Goal
// Exact comparison `v > 1.2 * (state_sum / non_null_count)` via
// `5 * non_null_count * v > 6 * state_sum` (requires `non_null_count > 0`).
[[nodiscard]] inline bool ExceedsOnePointTwoStateAverage(
    int64_t v, int64_t state_sum, uint64_t non_null_count) noexcept {
  if (non_null_count == 0) {
    return false;
  }
  return (static_cast<__int128>(5) * static_cast<__int128>(non_null_count) *
          static_cast<__int128>(v)) >
         (static_cast<__int128>(6) * static_cast<__int128>(state_sum));
}

// Q35: StoreCustomerRestrictedExistenceMarks.Goal
// Builds web/catalog EXISTS marks restricted to customers that already exist
// in store sales.
[[nodiscard]] inline std::vector<uint64_t> FilterStoreCustomersWithRemoteMarks(
    std::span<const uint64_t> customers, std::span<const uint64_t> store,
    std::span<const uint64_t> web, std::span<const uint64_t> catalog) {
  std::unordered_set<uint64_t> store_set(store.begin(), store.end());
  std::unordered_set<uint64_t> demand;
  for (const uint64_t c : customers) {
    if (store_set.contains(c)) {
      demand.insert(c);
    }
  }
  std::unordered_set<uint64_t> remote_marks;
  for (const uint64_t c : web) {
    if (demand.contains(c)) {
      remote_marks.insert(c);
    }
  }
  for (const uint64_t c : catalog) {
    if (demand.contains(c)) {
      remote_marks.insert(c);
    }
  }
  std::vector<uint64_t> out;
  for (const uint64_t c : customers) {
    if (remote_marks.contains(c)) {
      out.push_back(c);
    }
  }
  return out;
}

// Q38: NameCodeIntersectionCardinality.Goal
// Normalizes customer names through an injective dictionary code before taking
// the three-way `(name_code, date)` INTERSECT cardinality.
template <typename NameCodeFn>
[[nodiscard]] size_t CountNameCodeThreeWayIntersection(
    std::span<const std::pair<std::string, uint64_t>> store,
    std::span<const std::pair<std::string, uint64_t>> catalog,
    std::span<const std::pair<std::string, uint64_t>> web,
    NameCodeFn&& encode_name) {
  auto encode_set = [&](std::span<const std::pair<std::string, uint64_t>> src) {
    std::unordered_set<std::pair<uint64_t, uint64_t>, PairU64Hash> s;
    for (const auto& [name, dt] : src) {
      s.emplace(encode_name(name), dt);
    }
    return s;
  };
  const auto s_set = encode_set(store);
  const auto c_set = encode_set(catalog);
  const auto w_set = encode_set(web);
  size_t count = 0;
  for (const auto& k : s_set) {
    if (c_set.contains(k) && w_set.contains(k)) {
      ++count;
    }
  }
  return count;
}

// Q39: ConstantGroupVarianceRejection.Goal
// When `min_val == max_val` (both present), the group variance is identically
// 0 and cannot pass any strictly positive coefficient-of-variation threshold.
[[nodiscard]] inline bool IsConstantGroupZeroVariance(
    std::optional<int64_t> min_val, std::optional<int64_t> max_val) noexcept {
  return min_val.has_value() && max_val.has_value() && *min_val == *max_val;
}

// Q40: OuterJoinBuildRestrictedToSalesKeys.Goal
// Restricts the right-side returns build table to keys present in `sold`,
// preserving NULL-extended rows for unmatched sales keys.
[[nodiscard]] inline std::vector<std::optional<int64_t>>
RestrictedLeftJoinReturns(
    std::span<const std::pair<uint64_t, uint64_t>> sold,
    std::span<const std::pair<std::pair<uint64_t, uint64_t>, int64_t>>
        returns) {
  std::unordered_set<std::pair<uint64_t, uint64_t>, PairU64Hash> sold_set(
      sold.begin(), sold.end());
  std::unordered_map<std::pair<uint64_t, uint64_t>, std::vector<int64_t>,
                     PairU64Hash>
      ret_map;
  for (const auto& [k, val] : returns) {
    if (sold_set.contains(k)) {
      ret_map[k].push_back(val);
    }
  }
  std::vector<std::optional<int64_t>> out;
  for (const auto& k : sold) {
    const auto it = ret_map.find(k);
    if (it == ret_map.end() || it->second.empty()) {
      out.emplace_back(std::nullopt);
    } else {
      for (const int64_t v : it->second) {
        out.emplace_back(v);
      }
    }
  }
  return out;
}

// Q41: CorrelatedPositiveCountAsMembership.Goal
// Rewrites correlated `COUNT(*) > 0` with SQL NULL equality into a membership
// probe against the deduplicated non-NULL qualified manufacturer set.
[[nodiscard]] inline std::vector<std::optional<std::string>>
FilterByQualifiedMakerMembership(
    std::span<const std::optional<std::string>> outer_keys,
    std::span<const std::pair<std::optional<std::string>, bool>> items) {
  std::unordered_set<std::string_view> qualified;
  for (const auto& [name, ok] : items) {
    if (ok && name.has_value()) {
      qualified.insert(*name);
    }
  }
  std::vector<std::optional<std::string>> out;
  for (const auto& k : outer_keys) {
    if (k.has_value() && qualified.contains(*k)) {
      out.push_back(k);
    }
  }
  return out;
}

// Q45: ExpandPhysicalKeysByLogicalItem.Goal
// Expands seed physical item keys to ALL historical physical keys sharing a
// non-NULL logical `item_id`, then filters facts with `zip_ok || expanded(k)`.
[[nodiscard]] inline std::unordered_set<uint64_t>
ExpandItemHistoryKeysByLogicalItem(
    std::span<const std::pair<uint64_t, std::optional<std::string>>> versions,
    std::span<const uint64_t> seed_keys) {
  std::unordered_set<uint64_t> seed_set(seed_keys.begin(), seed_keys.end());
  std::unordered_set<std::string_view> logical_ids;
  for (const auto& [phys_k, item_id] : versions) {
    if (seed_set.contains(phys_k) && item_id.has_value()) {
      logical_ids.insert(*item_id);
    }
  }
  std::unordered_set<uint64_t> expanded;
  for (const auto& [phys_k, item_id] : versions) {
    if (item_id.has_value() && logical_ids.contains(*item_id)) {
      expanded.insert(phys_k);
    }
  }
  return expanded;
}

// Q49: NullRejectingRefundOuterJoinReduction.Goal
// Drops NULL-extended outer-join rows immediately when a strict lower bound
// (`> 10000`) rejects NULL, reducing the outer join to an inner join.
template <typename RefundLookupFn>
[[nodiscard]] std::vector<int64_t> ReduceNullRejectingRefundOuterJoin(
    std::span<const uint64_t> sold, RefundLookupFn&& refunds_for,
    int64_t min_exclusive = 10000) {
  std::vector<int64_t> out;
  for (const uint64_t k : sold) {
    for (const std::optional<int64_t>& r : refunds_for(k)) {
      if (r.has_value() && *r > min_exclusive) {
        out.push_back(*r);
      }
    }
  }
  return out;
}

// Q57: YearPartitionAverageBeforeNeighborLookup.Goal
// Separates target-year rows (for annual average) from the `y-1/12` and
// `y+1/1` boundary rows needed only for LAG/LEAD neighbor lookups.
struct YearMonthVal {
  int64_t year{0};
  uint32_t month{0};
  std::optional<int64_t> value;
};

[[nodiscard]] inline std::vector<int64_t> ExtractTargetYearFromExtendedMonths(
    std::span<const YearMonthVal> months, int64_t target_year) {
  std::vector<int64_t> out;
  for (const auto& m : months) {
    if (m.year == target_year && m.value.has_value()) {
      out.push_back(*m.value);
    }
  }
  return out;
}

// Q58: PairwiseRatioBoundsFromExtrema.Goal
// Checks whether all pairwise ratios among `(a, b, c)` lie in `[0.9, 1.1]`
// using only `min(a, b, c)` and `max(a, b, c)`.
[[nodiscard]] inline bool ThreeChannelPairwiseWithinTenPercent(
    uint64_t a, uint64_t b, uint64_t c) noexcept {
  const uint64_t lo = std::min({a, b, c});
  const uint64_t hi = std::max({a, b, c});
  return (9ULL * hi <= 10ULL * lo) && (10ULL * hi <= 11ULL * lo);
}

// Q59: WeekPairMultiplicityProduct.Goal
// Computes the multiplicity of week `w` across `weeks x first_year(w) x
// next_year(w + 52)` as a product of counts without materializing expansions.
[[nodiscard]] inline uint64_t WeekPairMultiplicity(
    uint64_t weeks_cnt, uint64_t first_year_cnt,
    uint64_t next_year_plus52_cnt) noexcept {
  return weeks_cnt * first_year_cnt * next_year_plus52_cnt;
}

// Q62: WarehousePrefixNullableSubtotalMerge.Goal
// Aggregates `(warehouse_name, bin)` by full warehouse name first, then merges
// bin counts across warehouses sharing the same 20-char prefix.
[[nodiscard]] inline uint64_t MergeWarehousePrefixBinCounts(
    std::span<const std::pair<std::string, uint32_t>> rows,
    std::string_view prefix20, uint32_t target_bin) {
  std::unordered_map<std::string_view, uint64_t> by_warehouse;
  for (const auto& [name, bin] : rows) {
    if (bin == target_bin) {
      ++by_warehouse[name];
    }
  }
  uint64_t total = 0;
  for (const auto& [name, cnt] : by_warehouse) {
    if (name.substr(0, 20U) == prefix20) {
      total += cnt;
    }
  }
  return total;
}

// Q63: OverlappingItemPredicateUnion.Goal
// Deduplicates the union of eligible item sets `a U b` so overlapping branch
// matches contribute each fact row's price exactly once.
template <typename PriceFn>
[[nodiscard]] int64_t SumFactsMatchingItemUnion(
    std::span<const uint64_t> facts, std::span<const uint64_t> branch_a,
    std::span<const uint64_t> branch_b, PriceFn&& price_fn) {
  std::unordered_set<uint64_t> eligible(branch_a.begin(), branch_a.end());
  eligible.insert(branch_b.begin(), branch_b.end());
  int64_t sum = 0;
  for (const uint64_t k : facts) {
    if (eligible.contains(k)) {
      sum += price_fn(k);
    }
  }
  return sum;
}

// Q66: FunctionalWarehouseAttributesAfterMonthlySum.Goal
// Groups monthly facts by narrow `(warehouse_id, month)` key and attaches
// functionally dependent warehouse attributes only after aggregation.
template <typename AttrFn>
[[nodiscard]] std::pair<std::string, int64_t> SumMonthlyThenAttachWarehouseAttr(
    std::span<const std::pair<std::pair<uint64_t, uint32_t>, int64_t>> facts,
    uint64_t warehouse_id, uint32_t month, AttrFn&& attr_fn) {
  int64_t sum = 0;
  for (const auto& [wm, v] : facts) {
    if (wm.first == warehouse_id && wm.second == month) {
      sum += v;
    }
  }
  return {attr_fn(warehouse_id), sum};
}

// Q72: ReturnMultiplicityAndPromotionCounts.Goal
// Synthesizes `(no_promo_cnt, promo_cnt, total_cnt)` directly from
// `(promo_copies, return_copies)` without expanding outer-join rows.
struct PromoReturnCounts {
  uint64_t no_promo_count{0};
  uint64_t promo_count{0};
  uint64_t total_count{0};
};

[[nodiscard]] inline PromoReturnCounts SynthesizeReturnPromoCounts(
    uint64_t promo_copies, uint64_t return_copies) noexcept {
  const uint64_t ret_mult = std::max<uint64_t>(1U, return_copies);
  return {
      .no_promo_count = promo_copies == 0 ? ret_mult : 0U,
      .promo_count = promo_copies * ret_mult,
      .total_count = std::max<uint64_t>(1U, promo_copies) * ret_mult,
  };
}

// Q74: OrderedTopKAfterPriorYearRatioFilter.Goal
// Stops evaluating an already-ordered customer stream as soon as `k` (100)
// customers pass the predicate.
template <typename PassFn>
[[nodiscard]] std::vector<uint64_t> OrderedPrefixTopKFilter(
    std::span<const uint64_t> ordered_ids, PassFn&& pass, size_t k = 100U) {
  std::vector<uint64_t> out;
  out.reserve(k);
  for (const uint64_t id : ordered_ids) {
    if (pass(id)) {
      out.push_back(id);
      if (out.size() == k) {
        break;
      }
    }
  }
  return out;
}

// Q75: ChannelLocalDedupBeforeUnionDistinct.Goal
// Deduplicates full `(key, qty, amt)` tuples locally per channel before global
// UNION DISTINCT and quantity summation for `target_key`.
struct ChannelRowTuple {
  uint64_t key{0};
  int64_t qty{0};
  int64_t amt{0};
  bool operator==(const ChannelRowTuple& other) const noexcept = default;
};

struct ChannelRowTupleHash {
  size_t operator()(const ChannelRowTuple& t) const noexcept {
    return (t.key * 11400714819323198485ULL) ^
           (static_cast<uint64_t>(t.qty) * 14029467366897019727ULL) ^
           static_cast<uint64_t>(t.amt);
  }
};

[[nodiscard]] inline int64_t ChannelLocalDedupUnionQuantity(
    std::span<const std::vector<ChannelRowTuple>> channels,
    uint64_t target_key) {
  std::unordered_set<ChannelRowTuple, ChannelRowTupleHash> global_unique;
  for (const auto& ch : channels) {
    std::unordered_set<ChannelRowTuple, ChannelRowTupleHash> local(ch.begin(),
                                                                   ch.end());
    global_unique.insert(local.begin(), local.end());
  }
  int64_t qty_sum = 0;
  for (const auto& row : global_unique) {
    if (row.key == target_key) {
      qty_sum += row.qty;
    }
  }
  return qty_sum;
}

// Q79: AddressKeyElisionByFunctionalDependency.Goal
// When all rows in a `(ticket, customer, city)` partition share the same
// functional address key, elides the address key and produces the single
// partition nullable sum (empty if no rows).
[[nodiscard]] inline std::vector<std::optional<int64_t>>
ElideFunctionalAddressKeySum(
    std::span<const std::pair<uint64_t, std::optional<int64_t>>> rows) {
  if (rows.empty()) {
    return {};
  }
  bool has_any = false;
  int64_t acc = 0;
  for (const auto& [addr, val] : rows) {
    (void)addr;
    if (val.has_value()) {
      acc += *val;
      has_any = true;
    }
  }
  return {has_any ? std::optional<int64_t>(acc) : std::nullopt};
}

// Q80: NullableTotalFromChannelSubtotals.Goal
// Synthesizes the grand total across channels from per-id and per-channel
// nullable subtotals, preserving NULL when all inputs are empty or NULL.
[[nodiscard]] inline std::optional<int64_t> MergeNullableChannelSubtotals(
    std::span<const std::vector<std::vector<std::optional<int64_t>>>>
        channels) noexcept {
  bool grand_any = false;
  int64_t grand_acc = 0;
  for (const auto& ch : channels) {
    bool ch_any = false;
    int64_t ch_acc = 0;
    for (const auto& id_rows : ch) {
      const std::optional<int64_t> id_sum = NullableSum(id_rows);
      if (id_sum.has_value()) {
        ch_acc += *id_sum;
        ch_any = true;
      }
    }
    if (ch_any) {
      grand_acc += ch_acc;
      grand_any = true;
    }
  }
  return grand_any ? std::optional<int64_t>(grand_acc) : std::nullopt;
}

// Q81: StateAverageFromCustomerTotals.Goal
// Builds a `(state -> (non_null_sum, non_null_count))` cache restricted to
// states referenced by candidate customers (`is_target_customer(cust)`).
struct StateCustomerTotal {
  uint64_t state{0};
  uint64_t customer{0};
  std::optional<int64_t> total;
};

template <typename CustomerPred>
[[nodiscard]] std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>>
BuildStateTotalsCacheForCustomers(std::span<const StateCustomerTotal> totals,
                                  CustomerPred&& is_target_customer) {
  std::unordered_set<uint64_t> needed_states;
  for (const auto& r : totals) {
    if (is_target_customer(r.customer)) {
      needed_states.insert(r.state);
    }
  }
  std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>> cache;
  for (const uint64_t st : needed_states) {
    cache.emplace(st, std::pair<int64_t, uint64_t>{0, 0U});
  }
  for (const auto& r : totals) {
    if (!r.total.has_value()) {
      continue;
    }
    const auto it = cache.find(r.state);
    if (it != cache.end()) {
      it->second.first += *r.total;
      ++it->second.second;
    }
  }
  return cache;
}

// Q83: CommonItemTopKBeforeQuantityLookup.Goal
// Intersects prefix-bounded keys `<= stop` across three channels, takes the
// sorted top-`k` (default 100), and looks up quantities only for winners.
template <typename QtyFn>
[[nodiscard]] std::vector<std::pair<uint64_t, std::array<int64_t, 3>>>
PrefixCommonThreeChannelTopK(std::span<const uint64_t> sr,
                             std::span<const uint64_t> cr,
                             std::span<const uint64_t> wr, uint64_t stop,
                             QtyFn&& qty_fn, size_t k = 100U) {
  auto prefix_set = [stop](std::span<const uint64_t> xs) {
    std::unordered_set<uint64_t> s;
    for (const uint64_t x : xs) {
      if (x <= stop) {
        s.insert(x);
      }
    }
    return s;
  };
  const auto s_set = prefix_set(sr);
  const auto c_set = prefix_set(cr);
  const auto w_set = prefix_set(wr);
  std::vector<uint64_t> common;
  for (const uint64_t x : s_set) {
    if (c_set.contains(x) && w_set.contains(x)) {
      common.push_back(x);
    }
  }
  std::ranges::sort(common);
  if (common.size() > k) {
    common.resize(k);
  }
  std::vector<std::pair<uint64_t, std::array<int64_t, 3>>> out;
  out.reserve(common.size());
  for (const uint64_t x : common) {
    out.emplace_back(x, qty_fn(x));
  }
  return out;
}

// Q85: EqualNonNullDemographicCodes.Goal
// SQL non-NULL equality over injective integer demographic codes matches
// equality over the underlying `(marital, education)` string pairs.
[[nodiscard]] inline bool EqualNonNullCodeHit(
    std::optional<uint64_t> a_code, std::optional<uint64_t> b_code) noexcept {
  return a_code.has_value() && b_code.has_value() && *a_code == *b_code;
}

// Q86: RankFromDistinctValueFrequencies.Goal
// Computes `1 + count(x > value)` directly from a `(distinct_value, freq)`
// histogram.
[[nodiscard]] inline uint64_t RankFromDistinctFrequencies(
    std::span<const std::pair<int64_t, uint64_t>> distinct_freqs,
    int64_t value) noexcept {
  uint64_t strictly_greater = 0;
  for (const auto& [v, freq] : distinct_freqs) {
    if (v > value) {
      strictly_greater += freq;
    }
  }
  return 1U + strictly_greater;
}

// Q87: CandidateOnlyDifferenceMarks.Goal
// Maintains only the deduplicated store candidate set and erases keys seen in
// catalog or web, returning the remaining store-only cardinality.
[[nodiscard]] inline size_t CountStoreMinusRemoteCandidates(
    std::span<const uint64_t> store, std::span<const uint64_t> catalog,
    std::span<const uint64_t> web) {
  std::unordered_set<uint64_t> remaining(store.begin(), store.end());
  for (const uint64_t k : catalog) {
    remaining.erase(k);
  }
  for (const uint64_t k : web) {
    remaining.erase(k);
  }
  return remaining.size();
}

// Q89: MonthlyDeviationBandPruning.Goal
// Prunes an entire brand partition when `[lo, hi]` lies inside `[0.9, 1.1]`
// of the partition average `sum / n` (`9*sum <= 10*n*lo && 10*n*hi <= 11*sum`).
[[nodiscard]] inline bool CanPruneBrandWithinTenPercentBand(
    int64_t n, int64_t sum, int64_t lo, int64_t hi) noexcept {
  if (n <= 0 || sum <= 0) {
    return false;
  }
  const __int128 ten_n = static_cast<__int128>(10) * static_cast<__int128>(n);
  return (static_cast<__int128>(9) * static_cast<__int128>(sum) <=
          ten_n * static_cast<__int128>(lo)) &&
         (ten_n * static_cast<__int128>(hi) <=
          static_cast<__int128>(11) * static_cast<__int128>(sum));
}

// Q90: WeightedTimeBandJoinCounts.Goal
// Collapses fact `(time_sk, hdemo_sk, page_sk)` keys into a frequency table
// once and reuses dimension probes across both `[8, 9]` and `[19, 20]` bands.
struct TimeDemoPageKey {
  uint64_t time_sk{0};
  uint64_t hdemo_sk{0};
  uint64_t page_sk{0};
  bool operator==(const TimeDemoPageKey& other) const noexcept = default;
};

struct TimeDemoPageHash {
  size_t operator()(const TimeDemoPageKey& k) const noexcept {
    return (k.time_sk * 11400714819323198485ULL) ^
           (k.hdemo_sk * 14029467366897019727ULL) ^ k.page_sk;
  }
};

template <typename PassesFn, typename HourFn>
[[nodiscard]] std::pair<uint64_t, uint64_t> ComputeWeightedTimeBandCounts(
    std::span<const TimeDemoPageKey> keys, PassesFn&& passes_weight,
    HourFn&& hour_of) {
  std::unordered_map<TimeDemoPageKey, uint64_t, TimeDemoPageHash> freq;
  for (const auto& k : keys) {
    ++freq[k];
  }
  uint64_t am_count = 0;
  uint64_t pm_count = 0;
  for (const auto& [k, mult] : freq) {
    const uint64_t hr = hour_of(k.time_sk);
    const bool in_am = (8U <= hr && hr <= 9U);
    const bool in_pm = (19U <= hr && hr <= 20U);
    if (!in_am && !in_pm) {
      continue;
    }
    const uint64_t w = passes_weight(k);
    if (in_am) {
      am_count += mult * w;
    }
    if (in_pm) {
      pm_count += mult * w;
    }
  }
  return {am_count, pm_count};
}

// Q99: RunLengthShippingDelayHistogram.Goal
// Accumulates shipping delay bucket counts `[<=30, <=60, <=90, <=120, >120]`
// directly from run-length encoded `((sold_date, ship_date), run_len)` pairs.
struct DatePairRun {
  int64_t sold_date{0};
  int64_t ship_date{0};
  uint64_t run_len{0};
};

[[nodiscard]] inline std::array<uint64_t, 5> RunLengthShippingDelayHistogram(
    std::span<const DatePairRun> runs) noexcept {
  std::array<uint64_t, 5> hist{};
  for (const auto& run : runs) {
    const int64_t diff = run.ship_date - run.sold_date;
    size_t bin = 4U;
    if (diff <= 30) {
      bin = 0U;
    } else if (diff <= 60) {
      bin = 1U;
    } else if (diff <= 90) {
      bin = 2U;
    } else if (diff <= 120) {
      bin = 3U;
    }
    hist[bin] += run.run_len;
  }
  return hist;
}

}  // namespace tinylamb::query_rewrites

#endif  // TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_TPCDS_HPP
