/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
class Table;
}  // namespace tinylamb

namespace tinylamb::relational_detail {

struct SimpleComparePredicate {
  slot_t column{0};
  BinaryOperation op{BinaryOperation::kEquals};
  Value constant;
  bool int_payload{false};
  int64_t int_constant{0};
  bool double_payload{false};
  double double_constant{0.0};
  // Set at compile time when the constant is a plain (non-interval,
  // non-JSON, non-collated) string, letting MatchSimpleCompare compare
  // bytes directly instead of routing through TryEvaluateBinary per row.
  bool plain_string_payload{false};
};

struct CompiledScanFilter {
  std::vector<SimpleComparePredicate> simple;
  std::vector<Expression> residual;
  // Disjunctive branches: OR(AND(pred1, pred2, ...), AND(pred3, pred4, ...),
  // ...) Each inner vector is one AND branch.  MatchScanFilter returns true if
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
  // Bytecode-compiled residual expression for fast row-by-row evaluation.
  // When non-null, MatchScanFilter uses this instead of recursive Evaluate().
  std::optional<BytecodeProgram> residual_bytecode;
};

bool MatchSimpleCompare(const Row& row, const SimpleComparePredicate& pred);

std::optional<SimpleComparePredicate> TryCompileSimpleCompare(
    const Expression& predicate, const Schema& schema);

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema);

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes,
                     Status* error = nullptr);

// Evaluates a conjunctive filter predicate under the engine's commutative
// WHERE semantics: each conjunct is evaluated independently and a conjunct
// that cleanly rejects the row (FALSE or NULL) suppresses errors raised by
// sibling conjuncts; the first error surfaces only when no conjunct rejects
// the row. This is the same combination rule MatchScanFilter applies to its
// residual predicates. The optimizer canonicalizes conjunct order
// (plan/cascades.cpp CanonicalizeConjuncts) and pushes conjuncts to
// different evaluation sites, so every executor that applies a
// WHERE-derived conjunctive predicate must use this -- strict
// left-to-right evaluation of a reordered conjunction would let the plan
// shape decide whether an error surfaces.
//
// `eval` evaluates one conjunct against a row; callers without a
// TransactionContext pass `[&](const Expression& e) { return
// e->TryEvaluate(row, schema); }`. Join executors must only use this for
// INNER joins: outer/semi/anti predicates decide match-ness and keep
// strict error propagation (PredicateErrorPropagatesForAntiJoin).
StatusOr<bool> EvaluateConjunctsTolerant(
    const std::vector<Expression>& conjuncts,
    const std::function<StatusOr<Value>(const Expression&)>& eval);

std::vector<IntegerPeekCompare> BuildIntegerPeeks(
    const CompiledScanFilter& filter, const std::vector<slot_t>* projection,
    const Schema& full_schema);

bool TryParallelTableScan(TransactionContext& context, Table& table,
                          const std::vector<slot_t>* projection,
                          const std::unordered_set<int64_t>* key_filter,
                          std::optional<slot_t> full_key_column,
                          bool filter_during_scan,
                          const CompiledScanFilter* scan_filter,
                          const Schema& result_schema, const Scope* outer,
                          const CteMap& ctes, Relation* result,
                          Status* error = nullptr);

// Bytecode programs covering every non-simple component of a
// CompiledScanFilter, so a scan worker can decide a row without touching the
// TransactionContext.  `complete` is false when any needed program failed to
// compile (or bytecode is disabled); callers then fall back to the
// main-thread MatchScanFilter pass.
struct WorkerScanFilterPrograms {
  bool complete{false};
  std::optional<BytecodeProgram> residual;
  std::vector<std::optional<BytecodeProgram>> branch_residuals;
};

WorkerScanFilterPrograms CompileWorkerFilter(const CompiledScanFilter& filter,
                                             const Schema& schema);

// Worker-safe row decision: 0 = reject, 1 = accept, 2 = defer (a program
// raised, so the row needs the contextful MatchScanFilter re-check).
// Semantically mirrors MatchScanFilter for every decided row.
int MatchScanFilterWorker(const Row& row, const Schema& schema,
                          const CompiledScanFilter& filter,
                          const WorkerScanFilterPrograms& programs);

// Filters `source` into `result` with worker threads applying the full
// filter (simple compares plus the compiled worker programs; rows whose
// programs raise are re-checked on the main thread).  Rows are partitioned
// into contiguous index ranges and emitted in worker order, so output order
// matches a serial scan of `source`.  Returns false (touching nothing) when
// the source is small, spilled, or the filter has no worker programs.
bool TryParallelFilterRows(TransactionContext& context, const Relation& source,
                           const CompiledScanFilter& filter,
                           const Schema& match_schema, const Scope* outer,
                           const CteMap& ctes,
                           const std::unordered_set<int64_t>* key_filter,
                           std::optional<slot_t> key_column, Relation* result,
                           Status* error);

StatusOr<Relation> ApplyTableSample(TransactionContext& context,
                                    const SelectSource& source,
                                    Relation input);

StatusOr<Relation> LoadSource(
    TransactionContext& context, const SelectSource& source, const Scope* outer,
    const CteMap& ctes, const std::vector<slot_t>* projection = nullptr,
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

Status FilterRelation(TransactionContext& context, Relation* relation,
                      const std::vector<Expression>& predicates,
                      const Scope* outer, const CteMap& ctes);

std::vector<Expression> SplitDisjuncts(const Expression& expression);

Expression CombineDisjuncts(const std::vector<Expression>& expressions);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP
