/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP

#include <cstddef>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

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

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns,
                          const std::vector<int64_t>* offsets = nullptr);

size_t EstimateJoinRows(const Relation& left, const Relation& right,
                        const std::vector<Expression>& predicates);

// D8 (docs/design.md): the hybrid operator supports null-safe equality
// (NULL matches NULL), LEFT/RIGHT/FULL outer, and reprocesses every spilled
// partition, so spilled rows are never dropped against the resident portion.
StatusOr<Relation> HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons, bool null_safe = false, bool right_join = false);

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
  // Aggregate call sites collected from the select list (order matters: it
  // mirrors the AggregateAccumulator layout the emit finishes).
  std::vector<const AggregateExpression*> aggregates;
  // Per-aggregate flags parallel to `aggregates`.
  std::vector<bool> count_star;
  // Select-list expressions evaluated per group against the representative
  // row plus the finished aggregate results.
  std::vector<Expression> select_items;
  std::vector<std::string> select_names;
  // Statement-level eligibility decided by the caller (no HAVING/DISTINCT/
  // windows, mergeable aggregate kinds, no alias-resolution dependencies).
  bool eligible{false};
  // Set by BuildInput when the final join step actually fused.
  bool fired{false};
};

StatusOr<Relation> BuildInput(TransactionContext& context,
                              const SelectStatement& statement,
                              const Scope* outer, const CteMap& ctes,
                              bool* where_fully_applied,
                              GroupedJoinFusionSpec* fusion = nullptr);

size_t SpillPartitionOf(const std::string& key, size_t partitions);

size_t SpillPartitionOf(int64_t key, size_t partitions);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP
