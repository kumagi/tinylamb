/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ANALYTICS_FACTORIZED_JOIN_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ANALYTICS_FACTORIZED_JOIN_HPP

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"

namespace tinylamb::relational_detail {

// Lean proof: lean/TinylambOpt/ResearchProofs/CompressedStringPrefixHashEquiv.lean (sound)
// Lossless bijection packing binary-collation strings of byte length <= 7
// into a single uint64_t (big-endian zero-padded bytes in bits [63..8] and byte
// length in bits [7..0]). UINT64_MAX (low byte 0xFF) is never produced and can
// safely serve as an empty hash-table sentinel.
inline std::optional<uint64_t> PackShortString7(std::string_view s) noexcept {
  if (s.size() > 7) {
    return std::nullopt;
  }
  uint64_t word = static_cast<uint64_t>(s.size());
  for (size_t i = 0; i < s.size(); ++i) {
    word |= static_cast<uint64_t>(static_cast<uint8_t>(s[i]))
            << ((7U - i) * 8U);
  }
  return word;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/BoundedCountRadixDescSortEquiv.lean (sound)
// Order-preserving (ASC) / order-reversing (DESC) unsigned encoding of signed
// int64_t keys over the full [INT64_MIN, INT64_MAX] range.
inline constexpr uint64_t EncodeSignedInt64ForRadix(int64_t v,
                                                    bool ascending) noexcept {
  const uint64_t u = static_cast<uint64_t>(v) ^ (uint64_t{1} << 63);
  return ascending ? u : ~u;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/BoundedCountRadixDescSortEquiv.lean (sound)
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
      const uint8_t byte = static_cast<uint8_t>((key_fn(items[i]) >> shift) & 0xFFU);
      scratch[counts[byte]++] = std::move(items[i]);
    }
    items.swap(scratch);
  }
}

// Collapses one side of a single-key INNER or LEFT grouped join by its join key
// before probing the key-owning side in parallel, avoiding quadratic M:N pair
// expansion (e.g. H5, C4, B2).
// Lean proofs:
//   - lean/TinylambOpt/ResearchProofs/BilateralEagerAggJoinPushdown.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/SemijoinGuardedCheckedEagerSum.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/LeftJoinRightMultiplicityCollapse.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/InnerJoinUnreferencedSideWeightPushdown.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/CompressedStringPrefixHashEquiv.lean (sound)
StatusOr<std::optional<Relation>> TryCollapsedRightGroupedJoin(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<EqualityKey>& equality_keys, JoinType join_type,
    const std::vector<Expression>& residuals,
    const std::vector<BytecodeProgram>& key_programs,
    const std::vector<BytecodeProgram>& child_programs,
    const Scope* outer, const CteMap& ctes, GroupedJoinFusionSpec* fusion);

// Bottom-up weight propagation and star-hub branch folding over acyclic inner
// multi-way join trees with grouped or scalar aggregates (e.g. L1, L2, L5, C1,
// C2, C5, J4, J5).
// Lean proofs:
//   - lean/TinylambOpt/ResearchProofs/FactorizedJoinTreeCountPropagation.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/StarHubMultiwayWeightedProduct.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/TransitiveEquivPredicatePushdownAndDupKeyDrop.lean (sound)
//   - lean/TinylambOpt/ResearchProofs/SemijoinGuardedCheckedEagerSum.lean (sound)
StatusOr<std::optional<Relation>> TryFactorizedJoinTreeAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const std::vector<Relation>& relations,
    const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion);

// Direct 1-pass fact scan + O(1) dense array lookup for star joins with 1..3
// small unique bounded-integer dimension tables (e.g. B3).
// Lean proof: lean/TinylambOpt/ResearchProofs/SmallDimDenseArrayDirectLookup.lean (sound)
StatusOr<std::optional<Relation>> TrySmallDimStarDirectAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const std::vector<Relation>& relations,
    const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_ANALYTICS_FACTORIZED_JOIN_HPP
