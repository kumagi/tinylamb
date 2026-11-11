/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

struct PredicateInfo {
  Expression expression;
  std::unordered_set<size_t> sources;
  bool resolved{true};
  bool contains_query{false};
};

struct EqualityKey {
  size_t left{};
  size_t right{};
  bool null_safe{false};
  // Bias for `left.col = right.col +/- constant` join keys (week-over-week
  // and year-over-year self joins): the LEFT value enters the key as
  // (value + left_offset) and the RIGHT value as (value + right_offset), so
  // both sides hash equal exactly when the SQL equality holds.  Zero for
  // plain column=column keys.  Offset keys stay in the residual list too, so
  // matched pairs are re-verified with the original predicate -- the bias
  // only ever narrows the probe, never widens the result.
  int64_t left_offset{0};
  int64_t right_offset{0};
};

std::vector<PredicateInfo> AnalyzePredicates(
    const Expression& where, const std::vector<Relation>& relations);

// True when a NULL in any touched column forces the predicate away from TRUE
// (strict comparisons, IS NOT NULL, AND of strict terms, IN over constants).
// Used to license outer-join-to-inner reduction and pre-join filter pushdown.
bool IsNullRejectingPredicate(const Expression& expression);

std::string EqualityPairKey(const ColumnName& lhs, const ColumnName& rhs);

// Strict column equalities implied by every deterministic OR branch.
// Callers must keep the original expression as a residual predicate.
std::vector<std::pair<ColumnName, ColumnName>> NecessaryDisjunctEqualities(
    const Expression& predicate);

std::vector<PredicateInfo> SynthesizeDisjunctEqualityPredicates(
    const std::vector<PredicateInfo>& analyzed,
    const std::vector<Relation>& relations);

bool SameSingleIntegerJoinKey(const Schema& left_schema,
                              const std::vector<slot_t>& left_columns,
                              const Schema& right_schema,
                              const std::vector<slot_t>& right_columns);

inline Row ConcatRows(const Row& left, const Row& right) {
  std::vector<Value> values;
  values.reserve(left.values_.size() + right.values_.size());
  values.insert(values.end(), left.values_.begin(), left.values_.end());
  values.insert(values.end(), right.values_.begin(), right.values_.end());
  return Row(std::move(values));
}

// Downgrades outer joins proven redundant by a null-rejecting WHERE conjunct
// to inner joins. `reduced_all` reports whether every outer join collapsed.
std::vector<SelectSource> ReduceOuterJoinsToInner(
    const SelectStatement& statement, const std::vector<Relation>& schemas,
    bool* reduced_all);

Expression NecessaryLocalDisjunction(const Expression& expression,
                                     size_t source,
                                     const std::vector<Relation>& relations);

bool IsColumnEqualityPredicate(const Expression& predicate);

bool MapsToEqualityKey(const Schema& left, const Schema& right,
                       const Expression& predicate);

std::vector<Expression> ResidualJoinPredicates(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates);

std::vector<EqualityKey> EqualityKeys(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates);

bool HasNullKey(const Row& row, const std::vector<slot_t>& columns);

bool SingleIntegerJoinKey(const Schema& schema,
                          const std::vector<slot_t>& columns);

int64_t IntegerJoinKey(const Row& row, slot_t column);

inline int64_t AddJoinKeyBias(int64_t key, int64_t bias) {
  return static_cast<int64_t>(static_cast<uint64_t>(key) +
                              static_cast<uint64_t>(bias));
}

struct CompositeIntKey {
  static constexpr size_t kMaxParts = 4;
  std::array<int64_t, kMaxParts> parts{};
  uint8_t count{0};
  bool operator==(const CompositeIntKey& rhs) const = default;
};

struct CompositeIntKeyHash {
  size_t operator()(const CompositeIntKey& key) const {
    size_t hash = key.count;
    for (uint8_t i = 0; i < key.count; ++i) {
      hash ^= std::hash<int64_t>{}(key.parts[i]) + 0x9e3779b97f4a7c15ULL +
              (hash << 6) + (hash >> 2);
    }
    return hash;
  }
};

bool CompositeIntJoinKey(const Schema& left_schema,
                         const std::vector<slot_t>& left_columns,
                         const Schema& right_schema,
                         const std::vector<slot_t>& right_columns);

CompositeIntKey EncodeCompositeIntKey(
    const Row& row, const std::vector<slot_t>& columns,
    const std::vector<int64_t>* offsets = nullptr);

bool HasMixedKeySignednessOrType(const Relation& left, const Relation& right,
                                 const std::vector<EqualityKey>& keys);

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns,
                          const std::vector<int64_t>* offsets = nullptr);

size_t EstimateJoinRows(const Relation& left, const Relation& right,
                        const std::vector<Expression>& predicates);

struct JoinKeyFrequencyMaps {
  bool single_int{false};
  bool composite_int{false};
  std::vector<slot_t> right_columns;
  size_t build_stride{1};
  FlatIntKeyMap<size_t> single_int_frequencies;
  std::unordered_map<std::string, size_t> frequencies;
  std::unordered_map<CompositeIntKey, size_t, CompositeIntKeyHash>
      int_frequencies;
};

size_t EstimateJoinRowsCached(
    const Relation& left, const Relation& candidate,
    const std::vector<Expression>& predicates,
    std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>>*
        cache);

// D8 (docs/design.md): the hybrid operator supports null-safe equality
// (NULL matches NULL), LEFT/RIGHT/FULL outer, and reprocesses every spilled
// partition, so spilled rows are never dropped against the resident portion.
StatusOr<Relation> HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons, bool null_safe = false, bool right_join = false);

inline constexpr size_t kFlipMinBuildRows = 8192;
inline constexpr size_t kFlipRatio = 1;

bool ShouldHybridJoin(const Relation& left, const Relation& right);

StatusOr<Relation> Join(TransactionContext& context, Relation left,
                        Relation right, const SelectSource& source,
                        const Scope* outer, const CteMap& ctes);

StatusOr<Relation> InnerJoin(TransactionContext& context, Relation left,
                             Relation right,
                             const std::vector<Expression>& predicates,
                             const Scope* outer, const CteMap& ctes);

bool IsSubset(const std::unordered_set<size_t>& values,
              const std::unordered_set<size_t>& superset);

// Statement-level description of a grouped aggregation that may be fused
// into the statement's final join step: instead of materializing the joined
// relation and grouping it afterwards, the probe loop feeds group keys and
// aggregate inputs directly into accumulators and the join output never
// exists.  Built by the statement layer (which owns alias/window/aggregation
// classification); eligibility that needs the joined schema (bytecode
// compilability) is re-checked at the join step.
struct GroupedJoinFusionSpec {
  // GROUP BY expressions, evaluated against the joined schema.
  std::vector<Expression> group_keys;
  // Aggregate call sites collected from SELECT and HAVING (order matters: it
  // mirrors the AggregateAccumulator layout the emit finishes).
  std::vector<const AggregateExpression*> aggregates;
  // Per-aggregate flags parallel to `aggregates`.
  std::vector<bool> count_star;
  // Select-list expressions evaluated per group against the representative
  // row plus the finished aggregate results.
  std::vector<Expression> select_items;
  std::vector<std::string> select_names;
  // A grouped predicate evaluated against the same finished aggregates as
  // SELECT, before any output row or TopN candidate is emitted.
  Expression having;
  std::vector<SelectStatement::OrderByTerm> output_order;
  // Statement-level eligibility decided by the caller (no DISTINCT/
  // windows, mergeable aggregate kinds, no alias-resolution dependencies).
  bool eligible{false};
  // Set by BuildInput when the final join step actually fused.
  bool fired{false};
  // Optional post-HAVING filter over a projected SUM column.
  bool post_having_active{false};
  slot_t post_having_select_col{0};
  slot_t post_having_agg_slot{0};
  BinaryOperation post_having_op{BinaryOperation::kGreaterThan};
  bool post_having_is_self_sum_factor{false};
  double post_having_constant_or_factor{0.0};
  bool post_having_applied{false};
  // Optional TopN bound (OFFSET + LIMIT) when ORDER BY is evaluated inline.
  size_t topn_limit{0};
  // Optional outer rollup over `(group_key, COUNT(...)) -> (count, COUNT(*))`.
  bool rollup_count_distribution{false};
  bool rollup_count_col_first{true};
  bool rollup_distribution_applied{false};
  std::vector<std::string> rollup_select_names;
};

StatusOr<Relation> BuildInput(TransactionContext& context,
                              const SelectStatement& statement,
                              const Scope* outer, const CteMap& ctes,
                              bool* where_fully_applied,
                              GroupedJoinFusionSpec* fusion = nullptr,
                              bool ignore_projection_star = false);

size_t SpillPartitionOf(const std::string& key, size_t partitions);

size_t SpillPartitionOf(int64_t key, size_t partitions);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP
