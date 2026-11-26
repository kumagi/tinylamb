/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ANALYTICAL_OPERATORS_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ANALYTICAL_OPERATORS_PROOFS_HPP

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

// Shared nullable aggregate totals matching AnalyticsWorkloads.Totals.
struct NullableTotals {
  uint64_t cnt{0};
  uint64_t nn{0};
  int64_t sum{0};

  constexpr bool operator==(const NullableTotals&) const = default;
};

[[nodiscard]] inline NullableTotals AddTotals(NullableTotals a,
                                              NullableTotals b) noexcept {
  return {.cnt = a.cnt + b.cnt, .nn = a.nn + b.nn, .sum = a.sum + b.sum};
}

[[nodiscard]] inline NullableTotals WeightedTotal(
    uint64_t weight, std::optional<int64_t> val) noexcept {
  if (weight == 0) {
    return {};
  }
  if (!val.has_value()) {
    return {.cnt = weight, .nn = 0, .sum = 0};
  }
  return {.cnt = weight,
          .nn = weight,
          .sum = static_cast<int64_t>(weight) * (*val)};
}

// ============================================================================
// IncrementalRefreshAndFlush
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/AnalyticalOperators/IncrementalRefreshAndFlush.lean
// (sound: InsertSideState, ChangedSymbols, OverlapReplay, LaneOrSpill,
//  VisibleFlush, DirtyFlush)
// ============================================================================

struct BatchTick {
  uint64_t batch{0};
  uint64_t sym{0};
  std::optional<int64_t> bid;
};

struct BatchSymKey {
  uint64_t batch{0};
  uint64_t sym{0};
  constexpr bool operator==(const BatchSymKey&) const = default;
};

struct BatchSymKeyHash {
  size_t operator()(BatchSymKey k) const noexcept {
    return std::hash<uint64_t>{}(k.batch) ^
           (std::hash<uint64_t>{}(k.sym) * 0x9e3779b97f4a7c15ULL);
  }
};

using BatchSymGroups =
    std::unordered_map<BatchSymKey, NullableTotals, BatchSymKeyHash>;

// InsertSideState.Goal: incrementally maintain per-(batch, sym) rollup totals
// while appending fresh ticks without rescanning old batches.
inline void ApplyInsertSideState(BatchSymGroups* groups,
                                 std::span<const BatchTick> fresh) {
  for (const BatchTick& t : fresh) {
    const BatchSymKey key{.batch = t.batch, .sym = t.sym};
    auto& acc = (*groups)[key];
    acc = AddTotals(acc, WeightedTotal(1, t.bid));
  }
}

// ChangedSymbols.Goal: replace only the changed symbol's contribution in a
// published batch, erasing the group when its row count drops to zero.
[[nodiscard]] inline std::optional<NullableTotals> ReplaceSymbolState(
    NullableTotals previous, NullableTotals removed,
    NullableTotals added) noexcept {
  const uint64_t n = previous.cnt - removed.cnt + added.cnt;
  if (n == 0) {
    return std::nullopt;
  }
  return NullableTotals{
      .cnt = n,
      .nn = previous.nn - removed.nn + added.nn,
      .sum = previous.sum - removed.sum + added.sum,
  };
}

// OverlapReplay.Goal: when a retried batch [offset, offset+size) overlaps the
// already-acknowledged prefix [0, acknowledged), fold only the unacknowledged
// suffix starting at (acknowledged - offset).
inline void FoldOverlapReplaySuffix(BatchSymGroups* groups,
                                    std::span<const BatchTick> batch_slice,
                                    size_t offset, size_t acknowledged) {
  const size_t skip = acknowledged > offset ? (acknowledged - offset) : 0;
  if (skip >= batch_slice.size()) {
    return;
  }
  ApplyInsertSideState(groups, batch_slice.subspan(skip));
}

// LaneOrSpill.Goal: accumulate dense symbol keys in [base, base + width) into
// ordinal lanes and spill out-of-range keys into a hash map.
struct LaneOrSpillAccumulator {
  uint64_t base{0};
  std::vector<NullableTotals> lanes;
  std::unordered_map<uint64_t, NullableTotals> spill;

  LaneOrSpillAccumulator(uint64_t base_sym, size_t width)
      : base(base_sym), lanes(width) {}

  void Add(uint64_t sym, std::optional<int64_t> bid) {
    const NullableTotals delta = WeightedTotal(1, bid);
    if (sym >= base && (sym - base) < lanes.size()) {
      auto& slot = lanes[static_cast<size_t>(sym - base)];
      slot = AddTotals(slot, delta);
    } else {
      auto& slot = spill[sym];
      slot = AddTotals(slot, delta);
    }
  }

  [[nodiscard]] NullableTotals Lookup(uint64_t sym) const {
    if (sym >= base && (sym - base) < lanes.size()) {
      return lanes[static_cast<size_t>(sym - base)];
    }
    const auto it = spill.find(sym);
    return it == spill.end() ? NullableTotals{} : it->second;
  }
};

// VisibleFlush.Goal: coalesce ready batches up to a reader's pinned snapshot
// watermark while ignoring batches strictly above `pin`.
[[nodiscard]] inline NullableTotals CoalesceVisibleBatches(
    std::span<const std::pair<uint64_t, std::vector<BatchTick>>> chunks,
    uint64_t pin, uint64_t sym) {
  NullableTotals out;
  for (const auto& [batch_id, ticks] : chunks) {
    if (batch_id > pin) {
      continue;
    }
    for (const BatchTick& t : ticks) {
      if (t.sym == sym) {
        out = AddTotals(out, WeightedTotal(1, t.bid));
      }
    }
  }
  return out;
}

// DirtyFlush.Goal: track only keys touched by new ticks so the summary sink
// writes each modified key once.
[[nodiscard]] inline std::vector<BatchSymKey> CollectDirtyKeysAndUpdate(
    BatchSymGroups* groups, std::span<const BatchTick> fresh) {
  std::unordered_set<BatchSymKey, BatchSymKeyHash> seen;
  std::vector<BatchSymKey> dirty;
  for (const BatchTick& t : fresh) {
    const BatchSymKey key{.batch = t.batch, .sym = t.sym};
    auto& acc = (*groups)[key];
    acc = AddTotals(acc, WeightedTotal(1, t.bid));
    if (seen.insert(key).second) {
      dirty.push_back(key);
    }
  }
  return dirty;
}

// ============================================================================
// RevenueAggregatesAndJoinWitnesses
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/AnalyticalOperators/RevenueAggregatesAndJoinWitnesses.lean
// (sound: MarketShareWeights, YearRoutes, DirectionBuckets, LateSlack,
//  RetireWitness, RebucketOrders, RevenueRuns)
// ============================================================================

// MarketShareWeights.Goal: TPC-H Q8 supplier multiplicities split by target
// country preserve exact (numerator, denominator) including CASE ELSE 0.
struct MarketSharePair {
  NullableTotals numerator;
  NullableTotals denominator;
};

[[nodiscard]] inline MarketSharePair ComputeMarketShareByWeights(
    std::span<const std::pair<uint64_t, std::optional<int64_t>>> sales,
    std::span<const std::pair<uint64_t, std::optional<std::string>>> suppliers,
    std::string_view target_country) {
  std::unordered_map<uint64_t, uint64_t> all_rows;
  std::unordered_map<uint64_t, uint64_t> chosen;
  for (const auto& [suppkey, country] : suppliers) {
    ++all_rows[suppkey];
    if (country.has_value() && *country == target_country) {
      ++chosen[suppkey];
    }
  }
  MarketSharePair out;
  for (const auto& [suppkey, vol] : sales) {
    const auto all_it = all_rows.find(suppkey);
    if (all_it == all_rows.end()) {
      continue;
    }
    const uint64_t w = all_it->second;
    const auto ch_it = chosen.find(suppkey);
    const uint64_t p = ch_it == chosen.end() ? 0 : ch_it->second;
    out.numerator = AddTotals(
        out.numerator,
        AddTotals(WeightedTotal(p, vol), WeightedTotal(w - p, int64_t{0})));
    out.denominator = AddTotals(out.denominator, WeightedTotal(w, vol));
  }
  return out;
}

// YearRoutes.Goal: TPC-H Q8 route table `(order, year) -> customer
// multiplicity` collapses order+customer+region joins to one probe per
// lineitem.
struct OrderCustomerYear {
  uint64_t order_key{0};
  uint64_t cust_key{0};
  int64_t year{0};
};

[[nodiscard]] inline NullableTotals EvalYearRouteTotals(
    std::span<const OrderCustomerYear> orders,
    std::span<const uint64_t> qualified_customers,
    std::span<const std::pair<uint64_t, std::optional<int64_t>>> lines,
    int64_t target_year) {
  std::unordered_map<uint64_t, uint64_t> cust_freq;
  for (const uint64_t c : qualified_customers) {
    ++cust_freq[c];
  }
  std::unordered_map<uint64_t, uint64_t> order_weight;
  for (const auto& o : orders) {
    if (o.year != target_year) {
      continue;
    }
    const auto it = cust_freq.find(o.cust_key);
    if (it != cust_freq.end()) {
      order_weight[o.order_key] += it->second;
    }
  }
  NullableTotals out;
  for (const auto& [okey, val] : lines) {
    const auto it = order_weight.find(okey);
    if (it != order_weight.end()) {
      out = AddTotals(out, WeightedTotal(it->second, val));
    }
  }
  return out;
}

// DirectionBuckets.Goal: TPC-H Q7 compile two distinct nations `(a, b)` into
// direction bits `{0: a->b, 1: b->a}` and discard unmapped/NULL labels early.
struct NationShipment {
  std::optional<std::string> supp_nation;
  std::optional<std::string> cust_nation;
  int64_t year{0};
  std::optional<int64_t> volume;
};

[[nodiscard]] inline NullableTotals SumDirectedNationBucket(
    std::span<const NationShipment> rows, std::string_view nation_a,
    std::string_view nation_b, bool a_to_b, int64_t target_year) {
  NullableTotals out;
  for (const auto& r : rows) {
    if (r.year != target_year || !r.supp_nation.has_value() ||
        !r.cust_nation.has_value()) {
      continue;
    }
    const bool hit =
        a_to_b ? (*r.supp_nation == nation_a && *r.cust_nation == nation_b)
               : (*r.supp_nation == nation_b && *r.cust_nation == nation_a);
    if (hit) {
      out = AddTotals(out, WeightedTotal(1, r.volume));
    }
  }
  return out;
}

// LateSlack.Goal + RetireWitness.Goal: TPC-H Q4 late-line EXISTS compiled as a
// demand set of date-qualified orders retired on first late lineitem witness.
[[nodiscard]] inline std::unordered_map<uint64_t, uint64_t>
CountPriorityWithRetiredWitness(
    std::span<const std::pair<uint64_t, uint64_t>> qualified_orders,
    std::span<const uint64_t> late_order_keys) {
  std::unordered_map<uint64_t, std::vector<uint64_t>> demand;
  for (const auto& [okey, prio] : qualified_orders) {
    demand[okey].push_back(prio);
  }
  std::unordered_map<uint64_t, uint64_t> counts;
  for (const uint64_t lk : late_order_keys) {
    const auto it = demand.find(lk);
    if (it == demand.end()) {
      continue;
    }
    for (const uint64_t prio : it->second) {
      ++counts[prio];
    }
    demand.erase(it);  // Retire witness on first late lineitem.
  }
  return counts;
}

// RebucketOrders.Goal: TPC-H Q13 adjust precomputed total order-count histogram
// by subtracting only excluded orders per customer.
[[nodiscard]] inline std::unordered_map<uint64_t, uint64_t>
RebucketCustomerOrderHistogram(
    std::span<const uint64_t> customers,
    std::span<const std::pair<uint64_t, bool>> all_orders) {
  std::unordered_map<uint64_t, uint64_t> total_deg;
  std::unordered_map<uint64_t, uint64_t> excluded_deg;
  for (const auto& [cust, excluded] : all_orders) {
    ++total_deg[cust];
    if (excluded) {
      ++excluded_deg[cust];
    }
  }
  std::unordered_map<uint64_t, uint64_t> hist;
  for (const uint64_t c : customers) {
    const uint64_t tot = total_deg.contains(c) ? total_deg[c] : 0;
    const uint64_t exc = excluded_deg.contains(c) ? excluded_deg[c] : 0;
    ++hist[tot - exc];
  }
  return hist;
}

// RevenueRuns.Goal: TPC-H Q3 merge contiguous sorted order-revenue runs across
// morsels by stitching only the seam when adjacent morsels share a boundary
// key.
[[nodiscard]] inline std::vector<std::pair<uint64_t, NullableTotals>>
MergeContiguousRevenueRuns(
    std::span<const std::vector<std::pair<uint64_t, std::optional<int64_t>>>>
        chunks) {
  std::vector<std::pair<uint64_t, NullableTotals>> runs;
  for (const auto& chunk : chunks) {
    for (const auto& [okey, val] : chunk) {
      const NullableTotals delta = WeightedTotal(1, val);
      if (!runs.empty() && runs.back().first == okey) {
        runs.back().second = AddTotals(runs.back().second, delta);
      } else {
        runs.emplace_back(okey, delta);
      }
    }
  }
  return runs;
}

// ============================================================================
// SharedJoinStatesAndWeights
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/AnalyticalOperators/SharedJoinStatesAndWeights.lean
// (sound: RightStateClasses, ShardedHybrid, SourceWeights, EndpointDemand,
//  CommentQuotient, CollisionBucket, CountryProfiles)
// ============================================================================

// RightStateClasses.Goal + ShardedHybrid.Goal: H5 bilateral join equivalence
// where right keys with identical NullableTotals behave identically and radix
// shards can independently choose hash vs factorized strategies.
struct H5LeftRow {
  uint64_t group_id{0};
  int64_t join_key{0};
  std::optional<int64_t> val;
};

struct H5RightRow {
  int64_t join_key{0};
  std::optional<int64_t> val;
};

[[nodiscard]] inline std::pair<NullableTotals, NullableTotals>
EvalFactorizedH5Group(std::span<const H5LeftRow> left_rows,
                      std::span<const H5RightRow> right_rows,
                      uint64_t target_group) {
  std::unordered_map<int64_t, NullableTotals> right_state;
  for (const auto& r : right_rows) {
    auto& st = right_state[r.join_key];
    st = AddTotals(st, WeightedTotal(1, r.val));
  }
  NullableTotals sx;
  NullableTotals sy;
  for (const auto& l : left_rows) {
    if (l.group_id != target_group) {
      continue;
    }
    const auto it = right_state.find(l.join_key);
    if (it == right_state.end() || it->second.cnt == 0) {
      continue;
    }
    sx = AddTotals(sx, WeightedTotal(it->second.cnt, l.val));
    sy = AddTotals(sy, it->second);
  }
  return {sx, sy};
}

// SourceWeights.Goal + EndpointDemand.Goal: STATS-CEB C3 restrict posts to
// link endpoints and fold repeated (source, target) edges by multiplicity.
struct PostLinkEdge {
  uint64_t post_id{0};
  uint64_t related_id{0};
  uint64_t link_type{0};
};

[[nodiscard]] inline NullableTotals EvalEndpointWeightedLinkScore(
    const std::unordered_map<uint64_t, std::optional<int64_t>>& post_scores,
    const std::unordered_set<uint64_t>& valid_related_posts,
    std::span<const PostLinkEdge> edges, uint64_t target_link_type) {
  std::unordered_map<uint64_t, uint64_t> src_weight;
  for (const auto& e : edges) {
    if (e.link_type == target_link_type &&
        valid_related_posts.contains(e.related_id)) {
      ++src_weight[e.post_id];
    }
  }
  NullableTotals out;
  for (const auto& [pid, w] : src_weight) {
    const auto it = post_scores.find(pid);
    if (it != post_scores.end()) {
      out = AddTotals(out, WeightedTotal(w, it->second));
    }
  }
  return out;
}

// CommentQuotient.Goal: STATS-CEB C2 collapse comments by post_id before
// routing through owner_user_id with user multiplicity.
[[nodiscard]] inline uint64_t EvalCommentQuotientForOwner(
    std::span<const std::pair<uint64_t, uint64_t>> posts_owner_post,
    std::span<const uint64_t> qualified_comment_post_ids, uint64_t owner_id,
    uint64_t owner_multiplicity) {
  std::unordered_map<uint64_t, uint64_t> comment_freq;
  for (const uint64_t pid : qualified_comment_post_ids) {
    ++comment_freq[pid];
  }
  uint64_t sum = 0;
  for (const auto& [owner, pid] : posts_owner_post) {
    if (owner == owner_id) {
      const auto it = comment_freq.find(pid);
      if (it != comment_freq.end()) {
        sum += it->second;
      }
    }
  }
  return sum * owner_multiplicity;
}

// CollisionBucket.Goal: STATS-CEB C4 classify each hash bucket as
// single-spelling (no string compare needed on hit) vs multi-spelling.
struct HashBucketClass {
  bool single_spelling{true};
  std::string only_value;
  std::unordered_map<std::string, uint64_t> counts;
};

[[nodiscard]] inline std::unordered_map<uint64_t, HashBucketClass>
BuildClassifiedStringBuckets(std::span<const std::optional<std::string>> tags) {
  std::unordered_map<uint64_t, HashBucketClass> buckets;
  const std::hash<std::string_view> hasher;
  for (const auto& t : tags) {
    if (!t.has_value()) {
      continue;
    }
    const uint64_t h = hasher(*t);
    auto& b = buckets[h];
    if (b.counts.empty()) {
      b.only_value = *t;
      b.single_spelling = true;
    } else if (b.single_spelling && b.only_value != *t) {
      b.single_spelling = false;
    }
    ++b.counts[*t];
  }
  return buckets;
}

// CountryProfiles.Goal: LDBC L1 aggregate active friend multiplicities by
// incoming-edge country profile before summing per country.
[[nodiscard]] inline std::unordered_map<uint64_t, uint64_t>
AggregateByCountryProfile(
    std::span<const std::pair<uint64_t, uint64_t>> person_country_edges,
    std::span<const uint64_t> active_persons) {
  std::unordered_map<uint64_t, uint64_t> active_freq;
  for (const uint64_t p : active_persons) {
    ++active_freq[p];
  }
  std::unordered_map<uint64_t, uint64_t> country_counts;
  for (const auto& [person, country] : person_country_edges) {
    const auto it = active_freq.find(person);
    if (it != active_freq.end()) {
      country_counts[country] += it->second;
    }
  }
  return country_counts;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_ANALYTICAL_OPERATORS_PROOFS_HPP
