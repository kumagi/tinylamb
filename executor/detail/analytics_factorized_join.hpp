/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ANALYTICS_FACTORIZED_JOIN_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ANALYTICS_FACTORIZED_JOIN_HPP

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"

namespace tinylamb::relational_detail {

// Lean proof:
// lean/TinylambOpt/ResearchProofs/CompressedStringPrefixHashEquiv.lean (sound)
// Lossless bijection packing binary-collation strings of byte length <= 7
// into a single uint64_t (big-endian zero-padded bytes in bits [63..8] and byte
// length in bits [7..0]). UINT64_MAX (low byte 0xFF) is never produced and can
// safely serve as an empty hash-table sentinel.
inline std::optional<uint64_t> PackShortString7(std::string_view s) noexcept {
  if (s.size() > 7) {
    return std::nullopt;
  }
  auto word = static_cast<uint64_t>(s.size());
  for (size_t i = 0; i < s.size(); ++i) {
    word |= static_cast<uint64_t>(static_cast<uint8_t>(s[i]))
            << ((7U - i) * 8U);
  }
  return word;
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave8BigBench.lean (UrlGrouping_sound)
// Bijective canonical parser for "page-<N>" URLs (0 <= N <= max_id, no leading
// zeros except "page-0"), keeping non-canonical strings in a disjoint literal
// map so decode(encode(s)) == s holds identically.
inline bool TryParseCanonicalPageUrl(std::string_view sv, uint32_t* out_id,
                                     uint32_t max_id = 262144) noexcept {
  if (sv.size() < 6 || sv.size() > 11 || sv[0] != 'p' || sv[1] != 'a' ||
      sv[2] != 'g' || sv[3] != 'e' || sv[4] != '-') {
    return false;
  }
  const size_t digits = sv.size() - 5;
  if (digits > 1 && sv[5] == '0') {
    return false;
  }
  auto val = static_cast<uint32_t>(0);
  for (size_t i = 5; i < sv.size(); ++i) {
    const auto c = static_cast<unsigned char>(sv[i]);
    if (c < '0' || c > '9') {
      return false;
    }
    val = (val * 10U) + static_cast<uint32_t>(c - '0');
    if (val > max_id) {
      return false;
    }
  }
  *out_id = val;
  return true;
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/OffsetHeavyReverseTopNAndQuotientBucket.lean
// (sound)
// Selects the exact [offset, offset + limit) indices under `less_fn` using a
// bounded heap: when offset + limit > total_n / 2, inverts the comparator to
// keep only the smaller complement of size (total_n - offset), reverses, and
// takes the first `limit` items.
template <typename LessFn>
inline std::vector<size_t> SelectOffsetHeavyReverseTopNIndices(
    size_t total_n, size_t offset, size_t limit, LessFn&& less_fn) {
  if (limit == 0 || offset >= total_n) {
    return {};
  }
  const size_t avail = total_n - offset;
  const size_t take = std::min(limit, avail);
  const size_t raw_keep = offset + take;
  if (raw_keep <= total_n / 2) {
    // Standard max-heap of size raw_keep.
    std::vector<size_t> heap;
    heap.reserve(raw_keep);
    for (size_t i = 0; i < total_n; ++i) {
      if (heap.size() < raw_keep) {
        heap.push_back(i);
        std::push_heap(heap.begin(), heap.end(), less_fn);
      } else if (less_fn(i, heap.front())) {
        std::pop_heap(heap.begin(), heap.end(), less_fn);
        heap.back() = i;
        std::push_heap(heap.begin(), heap.end(), less_fn);
      }
    }
    std::sort_heap(heap.begin(), heap.end(), less_fn);
    return {heap.begin() + static_cast<std::ptrdiff_t>(offset), heap.end()};
  }
  // Offset-heavy reverse Top-N: keep the largest `avail = total_n - offset`
  // indices using an inverted comparator (min-heap on `less_fn`).
  auto rev_less = [&](size_t a, size_t b) { return less_fn(b, a); };
  std::vector<size_t> heap;
  heap.reserve(avail);
  for (size_t i = 0; i < total_n; ++i) {
    if (heap.size() < avail) {
      heap.push_back(i);
      std::push_heap(heap.begin(), heap.end(), rev_less);
    } else if (rev_less(i, heap.front())) {
      std::pop_heap(heap.begin(), heap.end(), rev_less);
      heap.back() = i;
      std::push_heap(heap.begin(), heap.end(), rev_less);
    }
  }
  std::sort_heap(heap.begin(), heap.end(), rev_less);
  // `heap` is sorted descending; reverse to ascending [offset..total_n) and
  // keep the first `take` items.
  std::reverse(heap.begin(), heap.end());
  heap.resize(take);
  return heap;
}

// Lean proof:
// lean/TinylambOpt/LazyDictionary.lean (evalOne_sound, evalOne_twice_is_hit)
// Tri-state per-code predicate cache (-1 = uncomputed, 0 = false, 1 = true)
// so a selective upstream filter evaluates dictionary predicates only for
// dictionary codes actually touched by surviving rows.
class LazyDictionaryPredicateCache {
 public:
  explicit LazyDictionaryPredicateCache(size_t dict_size)
      : state_(dict_size, -1) {}

  template <typename EvalFn>
  bool Test(size_t code, EvalFn&& eval_fn) {
    if (code >= state_.size()) {
      return false;
    }
    const int8_t cached = state_[code];
    if (cached >= 0) {
      return cached != 0;
    }
    ++eval_count_;
    const bool pass = eval_fn(code);
    state_[code] = pass ? int8_t{1} : int8_t{0};
    return pass;
  }

  [[nodiscard]] size_t EvalCount() const noexcept { return eval_count_; }

 private:
  std::vector<int8_t> state_;
  size_t eval_count_{0};
};

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
//   (DisjointRangeBucketHistogram_sound)
// Maps a non-NULL `ship - sold` day difference to its unique disjoint 5-bin
// bucket (<=30, 31..60, 61..90, 91..120, >120) in one dispatch.
constexpr uint8_t ClassifyDisjointRangeBucket5(int64_t diff) noexcept {
  if (diff <= 30) {
    return 0;
  }
  if (diff <= 60) {
    return 1;
  }
  if (diff <= 90) {
    return 2;
  }
  if (diff <= 120) {
    return 3;
  }
  return 4;
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
//   (LeftJoinNullFlagSplitCount_sound)
// Accumulates (no_match, matched, total) for a LEFT JOIN whose nullable side
// is only inspected via `right_key IS NULL` / `IS NOT NULL`.
inline void FoldLeftJoinNullFlagCounts(uint64_t right_mult, uint64_t* no_match,
                                       uint64_t* matched,
                                       uint64_t* total) noexcept {
  if (right_mult == 0) {
    *no_match += 1;
    *total += 1;
  } else {
    *matched += right_mult;
    *total += right_mult;
  }
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9ClickBenchHighCardAndFilter.lean
//   (TopKCountPruneByRemainingRows_sound)
// Returns the K-th largest count (0 if fewer than K groups exist). Once this
// threshold strictly exceeds the remaining unread rows, unseen keys cannot
// enter the Top-K.
inline uint64_t ComputeTopKRemainingPruneThreshold(
    const std::vector<uint64_t>& counts, size_t k) {
  if (k == 0 || counts.size() < k) {
    return 0;
  }
  std::vector<uint64_t> copy = counts;
  const size_t nth = k - 1;
  std::nth_element(copy.begin(),
                   copy.begin() + static_cast<std::ptrdiff_t>(nth), copy.end(),
                   std::greater<uint64_t>{});
  return copy[nth];
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/BoundedCountRadixDescSortEquiv.lean (sound)
// Order-preserving (ASC) / order-reversing (DESC) unsigned encoding of signed
// int64_t keys over the full [INT64_MIN, INT64_MAX] range.
constexpr uint64_t EncodeSignedInt64ForRadix(int64_t v,
                                             bool ascending) noexcept {
  const uint64_t u = static_cast<uint64_t>(v) ^ (uint64_t{1} << 63);
  return ascending ? u : ~u;
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/BoundedCountRadixDescSortEquiv.lean (sound)
// Stable base-256 LSB radix sort on uint64_t sort keys, skipping byte passes
// whose XOR difference mask across all items is zero.
template <typename T, typename KeyFn>
void StableRadixSortByUint64Key(std::vector<T>& items, KeyFn&& key_fn) {
  const size_t n = items.size();
  if (n < 2) {
    return;
  }
  const uint64_t first_k = key_fn(items[0]);
  uint64_t diff_mask = 0;
  for (size_t i = 1; i < n; ++i) {
    diff_mask |= (key_fn(items[i]) ^ first_k);
  }
  if (diff_mask == 0) {
    return;
  }
  std::vector<T> scratch(n);
  std::array<size_t, 256> counts{};
  for (unsigned shift = 0; shift < 64; shift += 8) {
    if (((diff_mask >> shift) & 0xFFU) == 0) {
      continue;
    }
    counts.fill(0);
    for (size_t i = 0; i < n; ++i) {
      ++counts[(key_fn(items[i]) >> shift) & 0xFFU];
    }
    size_t sum = 0;
    for (size_t b = 0; b < 256; ++b) {
      const size_t c = counts[b];
      counts[b] = sum;
      sum += c;
    }
    for (size_t i = 0; i < n; ++i) {
      const auto byte =
          static_cast<uint8_t>((key_fn(items[i]) >> shift) & 0xFFU);
      scratch[counts[byte]++] = std::move(items[i]);
    }
    items.swap(scratch);
  }
}

template <typename T, typename KeyFn, typename TieCmp>
inline void StableRadixSortByUint64KeyAndTieBreak(std::vector<T>& items,
                                                  KeyFn&& key_fn,
                                                  TieCmp&& tie_cmp) {
  StableRadixSortByUint64Key(items, key_fn);
  const size_t n = items.size();
  for (size_t i = 0; i < n;) {
    const uint64_t k = key_fn(items[i]);
    size_t j = i + 1;
    while (j < n && key_fn(items[j]) == k) {
      ++j;
    }
    if (j - i > 1) {
      std::sort(items.begin() + static_cast<std::ptrdiff_t>(i),
                items.begin() + static_cast<std::ptrdiff_t>(j), tie_cmp);
    }
    i = j;
  }
}

template <typename KeyAtFn, typename TieCmp>
inline void RadixSortIndicesByUint64Key(std::vector<size_t>& indices,
                                        KeyAtFn&& key_at, TieCmp&& tie_cmp) {
  std::vector<std::pair<uint64_t, size_t>> keyed;
  keyed.reserve(indices.size());
  for (const size_t idx : indices) {
    keyed.emplace_back(key_at(idx), idx);
  }
  StableRadixSortByUint64KeyAndTieBreak(
      keyed,
      [](const std::pair<uint64_t, size_t>& p) noexcept { return p.first; },
      [&](const std::pair<uint64_t, size_t>& a,
          const std::pair<uint64_t, size_t>& b) {
        return tie_cmp(a.second, b.second);
      });
  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = keyed[i].second;
  }
}

template <typename T, typename NullFn, typename IntFn>
inline bool TryStableRadixSortNullableInt64(std::vector<T>& items,
                                            bool ascending, bool nulls_first,
                                            NullFn&& is_null_fn,
                                            IntFn&& int_fn) {
  bool any_null = false;
  for (const auto& item : items) {
    if (is_null_fn(item)) {
      any_null = true;
      break;
    }
  }
  if (any_null) {
    for (const auto& item : items) {
      if (!is_null_fn(item)) {
        const int64_t v = int_fn(item);
        if (v == std::numeric_limits<int64_t>::min() ||
            v == std::numeric_limits<int64_t>::max()) {
          return false;
        }
      }
    }
  }
  const uint64_t null_key = nulls_first ? 0ULL : ~uint64_t{0};
  StableRadixSortByUint64Key(items, [&](const T& item) noexcept {
    if (any_null && is_null_fn(item)) {
      return null_key;
    }
    const uint64_t enc = EncodeSignedInt64ForRadix(int_fn(item), true);
    return ascending ? enc : ~enc;
  });
  return true;
}

// Collapses one side of a single-key INNER or LEFT grouped join by its join key
// before probing the key-owning side in parallel, avoiding quadratic M:N pair
// expansion (e.g. H5, C4, B2).
// Lean proofs:
//   - lean/TinylambOpt/ResearchProofs/BilateralEagerAggJoinPushdown.lean
//   (sound)
//   - lean/TinylambOpt/ResearchProofs/SemijoinGuardedCheckedEagerSum.lean
//   (sound)
//   - lean/TinylambOpt/ResearchProofs/LeftJoinRightMultiplicityCollapse.lean
//   (sound)
//   -
//   lean/TinylambOpt/ResearchProofs/InnerJoinUnreferencedSideWeightPushdown.lean
//   (sound)
//   - lean/TinylambOpt/ResearchProofs/CompressedStringPrefixHashEquiv.lean
//   (sound)
StatusOr<std::optional<Relation>> TryCollapsedRightGroupedJoin(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<EqualityKey>& equality_keys, JoinType join_type,
    const std::vector<Expression>& residuals,
    const std::vector<BytecodeProgram>& key_programs,
    const std::vector<BytecodeProgram>& child_programs, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion);

// Bottom-up weight propagation and star-hub branch folding over acyclic inner
// multi-way join trees with grouped or scalar aggregates (e.g. L1, L2, L5, C1,
// C2, C5, J4, J5).
// Lean proofs:
//   - lean/TinylambOpt/ResearchProofs/FactorizedJoinTreeCountPropagation.lean
//   (sound)
//   - lean/TinylambOpt/ResearchProofs/StarHubMultiwayWeightedProduct.lean
//   (sound)
//   -
//   lean/TinylambOpt/ResearchProofs/TransitiveEquivPredicatePushdownAndDupKeyDrop.lean
//   (sound)
//   - lean/TinylambOpt/ResearchProofs/SemijoinGuardedCheckedEagerSum.lean
//   (sound)
StatusOr<std::optional<Relation>> TryFactorizedJoinTreeAggregate(
    TransactionContext& context, const std::vector<Relation>& relations,
    const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion);

// Direct 1-pass fact scan + O(1) dense array lookup for star joins with 1..3
// small unique bounded-integer dimension tables (e.g. B3).
// Lean proof:
// lean/TinylambOpt/ResearchProofs/SmallDimDenseArrayDirectLookup.lean (sound)
StatusOr<std::optional<Relation>> TrySmallDimStarDirectAggregate(
    TransactionContext& context, const std::vector<Relation>& relations,
    const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_ANALYTICS_FACTORIZED_JOIN_HPP
