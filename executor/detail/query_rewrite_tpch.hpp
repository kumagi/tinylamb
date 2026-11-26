/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_TPCH_HPP
#define TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_TPCH_HPP

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::query_rewrites {

// Exact nullable SUM helper matching Lean `QueryRewrites.Common.nsum`.
[[nodiscard]] inline std::optional<int64_t> NullableSum(
    std::span<const std::optional<int64_t>> xs) noexcept {
  bool has_any = false;
  int64_t acc = 0;
  for (const auto& x : xs) {
    if (x.has_value()) {
      acc += *x;
      has_any = true;
    }
  }
  return has_any ? std::optional<int64_t>(acc) : std::nullopt;
}

// ============================================================================
// TPC-H QueryRewrites (8 proved theorems)
// Lean models:
//   lean/TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean
// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/QueryRewrites/RevenueAndShipments/
// ============================================================================

// Q1: DateBoundBlockSubtotalReuse.Goal
// Reuses whole-block pre-aggregated nullable sum whenever `block.max_date <=
// cutoff`, scanning individual rows only for boundary blocks.
struct DateValuePage {
  std::optional<int64_t> max_date;
  std::optional<int64_t> whole_page_nsum;
  std::vector<std::pair<int64_t, std::optional<int64_t>>> rows;
};

[[nodiscard]] inline std::optional<int64_t> SumDateBoundBlockSubtotals(
    std::span<const DateValuePage> pages, int64_t cutoff) noexcept {
  bool has_any = false;
  int64_t acc = 0;
  for (const DateValuePage& page : pages) {
    if (page.max_date.has_value() && *page.max_date <= cutoff) {
      if (page.whole_page_nsum.has_value()) {
        acc += *page.whole_page_nsum;
        has_any = true;
      }
      continue;
    }
    for (const auto& [d, val] : page.rows) {
      if (d <= cutoff && val.has_value()) {
        acc += *val;
        has_any = true;
      }
    }
  }
  return has_any ? std::optional<int64_t>(acc) : std::nullopt;
}

// Q3: NonnegativeRevenueUpperBoundPruning.Goal
// Prunes an order whose nonnegative revenue upper bound is strictly below at
// least `k` already-finalized orders (default k=10).
[[nodiscard]] inline bool CanPruneNonnegativeRevenueOrder(
    std::span<const int64_t> known_revenues, int64_t pending_upper,
    size_t k = 10U) noexcept {
  size_t strictly_greater = 0;
  for (const int64_t rev : known_revenues) {
    if (rev > pending_upper) {
      ++strictly_greater;
      if (strictly_greater >= k) {
        return true;
      }
    }
  }
  return false;
}

// Q6: DiscountGroupedRevenueSum.Goal
// Groups eligible `(discount, price)` rows with `lo <= discount <= hi` by
// distinct discount rate and multiplies once per distinct discount.
[[nodiscard]] inline int64_t DiscountGroupedRevenueSum(
    std::span<const std::pair<int64_t, int64_t>> rows, int64_t lo, int64_t hi) {
  std::unordered_map<int64_t, int64_t> price_by_discount;
  for (const auto& [discount, price] : rows) {
    if (lo <= discount && discount <= hi) {
      price_by_discount[discount] += price;
    }
  }
  int64_t sum = 0;
  for (const auto& [discount, total_price] : price_by_discount) {
    sum += discount * total_price;
  }
  return sum;
}

// Q7: DirectionalNationKeyPrefilter.Goal
// Propagates opposite-nation customer orders into a key multiplicity table
// before probing supplier lines matching `supplier_flag`.
[[nodiscard]] inline uint64_t CountDirectionalNationJoinPairs(
    std::span<const std::pair<uint64_t, bool>> orders,
    std::span<const std::pair<uint64_t, bool>> lines, bool supplier_flag) {
  std::unordered_map<uint64_t, uint64_t> opp_order_counts;
  for (const auto& [okey, cust_flag] : orders) {
    if (cust_flag != supplier_flag) {
      ++opp_order_counts[okey];
    }
  }
  uint64_t pairs = 0;
  for (const auto& [okey, sup_flag] : lines) {
    if (sup_flag == supplier_flag) {
      const auto it = opp_order_counts.find(okey);
      if (it != opp_order_counts.end()) {
        pairs += it->second;
      }
    }
  }
  return pairs;
}

// Q8: SupplierYearVolumePreaggregation.Goal
// Pre-aggregates volume per supplier key before evaluating the nation
// predicate `is_target_nation(supplier)` once per distinct supplier.
template <typename Pred>
[[nodiscard]] int64_t SumSupplierYearVolumeByPredicate(
    std::span<const std::pair<uint64_t, int64_t>> sales,
    Pred&& is_target_nation) {
  std::unordered_map<uint64_t, int64_t> by_supplier;
  for (const auto& [sup, vol] : sales) {
    by_supplier[sup] += vol;
  }
  int64_t total = 0;
  for (const auto& [sup, vol] : by_supplier) {
    if (is_target_nation(sup)) {
      total += vol;
    }
  }
  return total;
}

// Q10: TopCustomersBeforeWideAttributeLookup.Goal
// Sorts customer IDs by rank first, truncates to top-`k` (default 20), and
// materializes wide string attributes only for the surviving winners.
template <typename RankFn, typename AttrFn>
[[nodiscard]] std::vector<std::pair<uint64_t, std::string>>
TopCustomersThenAttachAttrs(std::span<const uint64_t> ids, RankFn&& rank_fn,
                            AttrFn&& attr_fn, size_t k = 20U) {
  std::vector<uint64_t> ordered(ids.begin(), ids.end());
  std::stable_sort(ordered.begin(), ordered.end(), [&](uint64_t a, uint64_t b) {
    return rank_fn(a) < rank_fn(b);
  });
  if (ordered.size() > k) {
    ordered.resize(k);
  }
  std::vector<std::pair<uint64_t, std::string>> out;
  out.reserve(ordered.size());
  for (const uint64_t id : ordered) {
    out.emplace_back(id, attr_fn(id));
  }
  return out;
}

// Q19: CommonShipmentPredicateFactoring.Goal
// Factors the shared air-shipment predicate once and dispatches the brand tag
// to its `[lo, hi]` quantity band `[(5,15), (15,25), (29,39)]`.
struct AirBandRow {
  bool is_air{false};
  uint32_t brand_tag{0};
  uint32_t quantity{0};
};

[[nodiscard]] inline bool MatchesFactoredAirShipmentBand(
    const AirBandRow& row) noexcept {
  if (!row.is_air) {
    return false;
  }
  static constexpr std::array<std::pair<uint32_t, uint32_t>, 3> kBands = {
      {{5U, 15U}, {15U, 25U}, {29U, 39U}}};
  if (row.brand_tag >= kBands.size()) {
    return false;
  }
  const auto [lo, hi] = kBands[row.brand_tag];
  return lo <= row.quantity && row.quantity <= hi;
}

// Q20: CandidateRestrictedNullableSupplierQuantity.Goal
// Aggregates nullable line quantities restricted to deduplicated candidate
// `(partkey, suppkey)` pairs, preserving SQL NULL for absent/all-NULL keys.
struct PartSuppKey {
  uint64_t partkey{0};
  uint64_t suppkey{0};
  bool operator==(const PartSuppKey& other) const noexcept = default;
};

struct PartSuppKeyHash {
  size_t operator()(const PartSuppKey& k) const noexcept {
    return (k.partkey * 11400714819323198485ULL) ^
           (k.suppkey + 0x9e3779b97f4a7c15ULL);
  }
};

[[nodiscard]] inline std::unordered_map<PartSuppKey, std::optional<int64_t>,
                                        PartSuppKeyHash>
CandidateRestrictedNullableSum(
    std::span<const PartSuppKey> candidate_pairs,
    std::span<const std::pair<PartSuppKey, std::optional<int64_t>>> lines) {
  std::unordered_set<PartSuppKey, PartSuppKeyHash> allowed(
      candidate_pairs.begin(), candidate_pairs.end());
  std::unordered_map<PartSuppKey, std::optional<int64_t>, PartSuppKeyHash> out;
  out.reserve(allowed.size());
  for (const auto& [k, qty] : lines) {
    if (!allowed.contains(k) || !qty.has_value()) {
      continue;
    }
    auto& slot = out[k];
    slot = slot.has_value() ? (*slot + *qty) : *qty;
  }
  return out;
}

}  // namespace tinylamb::query_rewrites

#endif  // TINYLAMB_EXECUTOR_DETAIL_QUERY_REWRITE_TPCH_HPP
