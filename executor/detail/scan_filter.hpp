/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/runtime_filter.hpp"
#include "executor/detail/flat_int_key_map.hpp"
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
  // Column-vs-column compare (`l_receiptdate > l_commitdate`): when set,
  // MatchSimpleCompare reads both sides from the row positionally and
  // `constant` is unused.  Compile time only accepts like-kind numeric or
  // plain string pairs, so the positional compare cannot raise.
  bool column_payload{false};
  slot_t right_column{0};
};

// `substr(col, 1, k) IN (c1..cn)` compiled to a byte-prefix membership test.
// Every prefix must be plain ASCII: a leading multi-byte UTF-8 code point can
// then never match, and for an ASCII first byte one code point is one byte,
// so the byte-prefix verdict equals the AST's code-point substr verdict.
struct PrefixInPredicate {
  slot_t column{0};
  std::vector<std::string> prefixes;
};

// `col [NOT] IN (uncorrelated single-column subquery)` compiled to one
// immutable value set built once at filter-compile time.  Only peeled for
// INT64/DATE probes whose type tag and unsigned tag statically equal the
// build domain, so the int-value set verdict equals the per-row runtime
// membership build's exact verdict and the matcher is total: every input gets
// the SQL three-valued answer, no row defers to the contextful path.
struct PrebuiltMembershipPredicate {
  slot_t column{0};
  bool negated{false};
  ValueType domain_type{ValueType::kNull};
  // Distinct-member set over the build domain's int payloads (INT64 values /
  // DATE day counts).  The peel-time domain pin (type tag plus unsigned tag)
  // makes the int-set verdict the SQL equality here, and the flat table
  // keeps multi-hundred-thousand-member builds allocation-free.
  // NULL members are recorded in contains_null and never stored.
  std::shared_ptr<const FlatIntSet> values;
  bool contains_null{false};
};

// `col op (correlated scalar aggregate subquery)` with exactly one
// INT64/DATE correlation equality, compiled to one key->value map built at
// filter-compile time by executing the subquery rewritten as GROUP BY
// correlation column.  min/max/count/sum/avg over the same per-key rows
// produce the value the per-row correlated apply computes one probe at a
// time; a missing key or a NULL group value reproduces the empty-group
// scalar NULL (comparison UNKNOWN, row rejected).
struct PrebuiltScalarComparePredicate {
  // The comparison column (`ps_supplycost` above) and the correlation key
  // column (the outer side, `p_partkey`), both positional in the filter
  // schema.
  slot_t column{0};
  slot_t key_column{0};
  BinaryOperation op{BinaryOperation::kEquals};
  ValueType key_type{ValueType::kNull};
  std::shared_ptr<const std::unordered_map<int64_t, Value>> values;
};

// Compile-time execution environment enabling query-bearing predicate
// prebuilds (PrebuiltMembershipPredicate, PrebuiltScalarComparePredicate).
// Null for callers without a TransactionContext: those keep every query on
// the residual path.
struct FilterCompileEnv {
  TransactionContext* context{nullptr};
  const Scope* outer{nullptr};
  const CteMap* ctes{nullptr};
};

struct CompiledScanFilter {
  std::vector<SimpleComparePredicate> simple;
  std::vector<PrefixInPredicate> prefix_in;
  std::vector<PrebuiltMembershipPredicate> prebuilt_membership;
  std::vector<PrebuiltScalarComparePredicate> prebuilt_scalar_compare;
  std::vector<Expression> residual;
  // Constant-residual boundary: CompileScanFilter resolves conjuncts that are
  // already fixed at compile time (no column lookup is even possible against
  // an empty schema, no volatile call, no parameter slot, no subquery).
  // TRUE discharges such a conjunct; FALSE / NULL discharges the whole
  // conjunction (WHERE tolerates sibling errors for conjuncts that actually
  // evaluate, so the compile-time decision just takes their place).  When
  // this flag is set, no other component of the filter is meaningful and
  // every row rejects.
  bool compile_decided_reject{false};
  // Disjunctive branches: OR(AND(pred1, pred2, ...), AND(pred3, pred4, ...),
  // ...) Each inner vector is one AND branch.  MatchScanFilter returns true if
  // ALL predicates in ANY branch pass.  Falls back to residual evaluation
  // only when the branch contains non-simple (residual) predicates.
  struct DisjunctiveBranch {
    std::vector<SimpleComparePredicate> simple;
    std::vector<PrefixInPredicate> prefix_in;
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

// Exact SQL three-valued membership verdict for a prebuilt IN/NOT IN set.
// Empty member set is decisive (`x IN ()` is FALSE, `x NOT IN ()` is TRUE,
// even for a NULL x); a NULL probe or a NULL-containing set makes a miss
// UNKNOWN (row rejected); a clean miss answers FALSE for IN, TRUE for
// NOT IN.
bool MatchPrebuiltMembership(const Row& row,
                             const PrebuiltMembershipPredicate& pred);

// Exact comparison verdict against the prebuilt scalar map.  A NULL probe,
// a missing key, or a NULL group value means the original comparison is
// UNKNOWN and the row is rejected.
bool MatchPrebuiltScalarCompare(const Row& row,
                                const PrebuiltScalarComparePredicate& pred);

std::optional<SimpleComparePredicate> TryCompileSimpleCompare(
    const Expression& predicate, const Schema& schema);

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema,
                                     const FilterCompileEnv* env = nullptr);

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes,
                     Status* error = nullptr,
                     ColumnLookupCache* lookup_cache = nullptr);

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

// `runtime_filter` is the dynamic join filter (common/runtime_filter.hpp) the
// build side published for `key_column`; the scan rejects non-members before
// row decode, both in the serial iterator and inside parallel workers (the
// group is immutable once built, so sharing it across workers is safe).
bool TryParallelTableScan(TransactionContext& context, Table& table,
                          const std::vector<slot_t>* projection,
                          const RuntimeFilterGroup* runtime_filter,
                          ValueType runtime_filter_type,
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
                           const ScanRuntimeFilter* runtime_filter,
                           Relation* result, Status* error);

StatusOr<Relation> ApplyTableSample(TransactionContext& context,
                                    const SelectSource& source, Relation input);

StatusOr<Relation> LoadSource(
    TransactionContext& context, const SelectSource& source, const Scope* outer,
    const CteMap& ctes, const std::vector<slot_t>* projection = nullptr,
    const std::vector<Expression>* scan_predicates = nullptr,
    const ScanRuntimeFilter* runtime_filter = nullptr);

// Materializes one UNNEST'd array value into a single-column relation named
// after the source alias (struct elements expand into per-field columns,
// WITH OFFSET appends the offset column).  Pure: no context, no scope.
Relation UnnestValueToRelation(const SelectSource& source,
                               const Value& array_val);

bool ContainsQuery(const Expression& expression);

// Returns `predicates` with every row-independent subquery replaced by the
// constant value one probe returns (uncorrelated, volatile-free scalar and
// EXISTS queries; see FoldRowIndependentSubqueriesIn).  Nodes that raise keep
// their original form, so per-row error behavior is unchanged.  Callers pass
// the result to CompileScanFilter: folded compares become typed SimpleCompare
// predicates, and query-free predicates no longer force the contextful serial
// filter or a cached-relation snapshot.
std::vector<Expression> FoldRowIndependentSubqueries(
    TransactionContext& context, const std::vector<Expression>& predicates,
    const Scope* outer, const CteMap& ctes);

std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name);

Status FilterRelation(TransactionContext& context, Relation* relation,
                      const std::vector<Expression>& predicates,
                      const Scope* outer, const CteMap& ctes);

std::vector<Expression> SplitDisjuncts(const Expression& expression);

Expression CombineDisjuncts(const std::vector<Expression>& expressions);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_SCAN_FILTER_HPP
