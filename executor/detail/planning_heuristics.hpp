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
  size_t left;
  size_t right;
};

std::vector<PredicateInfo> AnalyzePredicates(
    const Expression& where, const std::vector<Relation>& relations);

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

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns);

size_t EstimateJoinRows(const Relation& left, const Relation& right,
                        const std::vector<Expression>& predicates);

Relation HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons);

bool ShouldHybridJoin(const Relation& left, const Relation& right);

Relation Join(TransactionContext& context, Relation left, Relation right,
              const SelectSource& source, const Scope* outer,
              const CteMap& ctes);

Relation InnerJoin(TransactionContext& context, Relation left, Relation right,
                   const std::vector<Expression>& predicates,
                   const Scope* outer, const CteMap& ctes);

bool IsSubset(const std::unordered_set<size_t>& values,
              const std::unordered_set<size_t>& superset);

Relation BuildInput(TransactionContext& context,
                    const SelectStatement& statement, const Scope* outer,
                    const CteMap& ctes, bool* where_fully_applied);

size_t SpillPartitionOf(const std::string& key, size_t partitions);

size_t SpillPartitionOf(int64_t key, size_t partitions);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_PLANNING_HEURISTICS_HPP
