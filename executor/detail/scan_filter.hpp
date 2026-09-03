/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
class Table;
}

namespace tinylamb::relational_detail {

struct SimpleComparePredicate {
  slot_t column{0};
  BinaryOperation op{BinaryOperation::kEquals};
  Value constant;
  bool int_payload{false};
  int64_t int_constant{0};
  bool double_payload{false};
  double double_constant{0.0};
};

struct CompiledScanFilter {
  std::vector<SimpleComparePredicate> simple;
  std::vector<Expression> residual;
  // Disjunctive branches: OR(AND(pred1, pred2, ...), AND(pred3, pred4, ...), ...)
  // Each inner vector is one AND branch.  MatchScanFilter returns true if
  // ALL predicates in ANY branch pass.  Falls back to residual evaluation
  // only when the branch contains non-simple (residual) predicates.
  struct DisjunctiveBranch {
    std::vector<SimpleComparePredicate> simple;
    std::vector<Expression> residual;
  };
  std::vector<DisjunctiveBranch> disjunctive_branches;
  // Pre-computed unsigned column info to avoid per-row schema iteration.
  bool needs_unsigned_tagging{false};
  std::vector<slot_t> unsigned_columns;
};

bool MatchSimpleCompare(const Row& row, const SimpleComparePredicate& pred);

std::optional<SimpleComparePredicate> TryCompileSimpleCompare(
    const Expression& predicate, const Schema& schema);

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema);

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes);

std::vector<IntegerPeekCompare> BuildIntegerPeeks(
    const CompiledScanFilter& filter, const std::vector<slot_t>* projection,
    const Schema& full_schema);

bool TryParallelTableScan(
    TransactionContext& context, Table& table,
    const std::vector<slot_t>* projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> full_key_column, bool filter_during_scan,
    const CompiledScanFilter* scan_filter, const Schema& result_schema,
    const Scope* outer, const CteMap& ctes, Relation* result);

Relation LoadSource(TransactionContext& context, const SelectSource& source,
                    const Scope* outer, const CteMap& ctes,
                    const std::vector<slot_t>* projection = nullptr,
                    const std::vector<Expression>* scan_predicates = nullptr,
                    const std::unordered_set<int64_t>* int_key_filter = nullptr,
                    std::optional<slot_t> int_key_column = std::nullopt);

// Materializes one UNNEST'd array value into a single-column relation named
// after the source alias (struct elements expand into per-field columns,
// WITH OFFSET appends the offset column).  Pure: no context, no scope.
Relation UnnestValueToRelation(const SelectSource& source,
                               const Value& array_val);

bool ContainsQuery(const Expression& expression);

std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name);

void FilterRelation(TransactionContext& context, Relation* relation,
                    const std::vector<Expression>& predicates,
                    const Scope* outer, const CteMap& ctes);

std::vector<Expression> SplitDisjuncts(const Expression& expression);

Expression CombineDisjuncts(const std::vector<Expression>& expressions);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP
