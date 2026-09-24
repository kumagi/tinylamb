/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP
#define TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP

#include <array>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/status_or.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

bool Truthy(const Value& value);
StatusOr<Value> TryLookup(const ColumnName& name, const Scope& scope);

// EXC-SHIM: remove with the boundary catch (see
// no-exception-rule-migration.md).
inline Value Lookup(const ColumnName& name, const Scope& scope) {
  return ExcShimUnwrap(TryLookup(name, scope), "Lookup");
}
bool Like(std::string_view value, std::string_view pattern);
StatusOr<Value> TryBinary(BinaryOperation operation, const Value& left,
                          const Value& right);

// EXC-SHIM: remove with the boundary catch (see
// no-exception-rule-migration.md).
inline Value Binary(BinaryOperation operation, const Value& left,
                    const Value& right) {
  return ExcShimUnwrap(TryBinary(operation, left, right), "Binary");
}

bool ContainsAggregate(const Expression& expression);

void CollectAggregates(const Expression& expression,
                       std::vector<const AggregateExpression*>* aggregates,
                       std::unordered_set<const AggregateExpression*>* seen);

// One row fed into an aggregate.  `order_keys` carries the evaluated inner
// ORDER BY terms and `condition` the HAVING MAX/MIN condition value; both are
// only populated when the aggregate declares them.  `trailing_values` carries
// evaluated trailing arguments (COVAR's second input, sketch precision /
// count parameters).
struct AggregateInput {
  Value value;
  std::vector<Value> order_keys;
  Value condition;
  Value auxiliary;  // STRING_AGG delimiter
  std::vector<Value> trailing_values;
};

std::string ElementSqlTypeName(ValueType type);

// DISTINCT semantics: all NaNs collapse to one entry and -0 folds to +0.
struct DistinctValueHash {
  size_t operator()(const Value& value) const;
};
struct DistinctValueEqual {
  bool operator()(const Value& left, const Value& right) const;
};
using DistinctValueSet =
    std::unordered_set<Value, DistinctValueHash, DistinctValueEqual>;

// DISTINCT/GROUP-BY key normalization: every NaN collapses to one canonical
// bit pattern and negative zero folds to +0, matching SQL equality semantics
// where raw IEEE bit patterns would otherwise count NaNs as distinct.
[[nodiscard]] Value CanonicalDistinctValue(const Value& value);

// The layout mirrors the per-stat switch structure for readability.
// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding)
struct AggregateAccumulator {
  explicit AggregateAccumulator(const AggregateExpression* aggregate,
                                const Schema* input_schema = nullptr);
  // Distinct-set handle: the hash set is allocated lazily.  COUNT/SUM
  // (DISTINCT) over INT64/DATE values never touch it (they use the inline
  // int array below), and materializing one per group cost a heap
  // allocation per group on multi-million-group aggregations.
  [[nodiscard]] bool DistinctMode() const { return distinct_mode_; }
  DistinctValueSet& EnsureDistinctSet();

  // Row-at-a-time accumulation for streaming aggregates.
  Status Add(const Value& value);
  // Parallel-build reduction: true when the accumulator keeps only streaming
  // scalar state (no DISTINCT set, no buffered rows) and Merge is allowed.
  [[nodiscard]] bool CanMerge() const;
  // Merges `other`'s streaming state into this accumulator.  Both sides must
  // satisfy CanMerge().  Double totals may associate differently than a
  // serial pass (parallel aggregation semantics); integer overflow and SUM
  // infinity still raise exactly like Add.
  Status Merge(const AggregateAccumulator& other);
  // Total distinct int64/date values tracked (inline array + overflow set).
  [[nodiscard]] size_t DistinctIntCount() const;
  // Inserts one int64/date distinct value; false when it was already present.
  bool InsertDistinctInt(int64_t raw);
  // Full-input accumulation: buffers rows for aggregates that need whole-group
  // context (HAVING modifier, inner ORDER BY/LIMIT, ARRAY_AGG, STRING_AGG).
  Status Add(AggregateInput input);
  StatusOr<Value> TryFinish() const;
  [[nodiscard]] bool IsDone() const;

  const AggregateExpression* expression;
  int64_t count = 0;
  double total = 0.0;
  // Integer sums accumulate exactly here (int64 with overflow checks);
  // `total` only carries double inputs.
  int64_t int_total = 0;
  bool total_is_double = false;
  // UINT64 values share the INT64 storage slot; retain the SQL domain for
  // named UINT64 columns so SUM can detect unsigned overflow instead of
  // summing the signed bit pattern.
  bool sum_is_uint64 = false;
  bool sum_can_infer_uint64 = false;
  uint64_t uint_total = 0;
  // UINT64 literals in subquery UNIONs can lose their declared type before
  // reaching the accumulator.  Keep enough information to recognize the
  // distinctive UINT64_MAX bit pattern when it is paired with a nonnegative
  // DISTINCT value.
  bool sum_saw_minus_one = false;
  uint64_t sum_nonnegative_total = 0;
  Value extreme;
  // GoogleSQL MIN/MAX: a NaN input poisons the result.
  mutable bool saw_nan_{false};
  // SUM over doubles: a finite-input sum that reaches infinity overflows and
  // must raise; infinite inputs themselves are legal and stay inf.
  bool sum_saw_infinite_input_ = false;
  std::unique_ptr<DistinctValueSet> distinct;
  // COUNT(DISTINCT int64/date) state.  Most groups hold only a handful of
  // distinct values (TPC-H Q21: ~3.4 per l_orderkey group across 2.9M
  // groups), so small cardinalities live in an inline array scanned linearly
  // and only overflow promotes to a hash set; a per-group unordered_set would
  // otherwise cost several heap allocations per group.
  static constexpr size_t kInlineDistinctIntCapacity = 8;
  std::unique_ptr<std::unordered_set<int64_t>> distinct_ints;
  size_t inline_distinct_int_count{0};
  std::array<int64_t, kInlineDistinctIntCapacity> inline_distinct_ints{};

 private:
  struct BufferedRow {
    Value value;
    std::vector<Value> order_keys;
    Value condition;
    Value auxiliary;
    std::vector<Value> trailing_values;
  };
  // True when the aggregate carries DISTINCT; `distinct` materializes on
  // first non-integer distinct insert.
  bool distinct_mode_{false};
  mutable std::unique_ptr<std::vector<BufferedRow>> buffer_;
  mutable std::vector<Value> array_values_;
  mutable std::optional<std::string> delimiter_;
  // BIT_AND/OR/XOR fold state.
  mutable int64_t bit_acc_{0};
  mutable bool bit_saw_value_{false};
  mutable bool bit_is_uint64_{false};
  // ARRAY_AGG DISTINCT: a repeated NULL collapses to one element.
  mutable bool array_saw_null_{false};
  // ARRAY_AGG element type resolved against the input schema at
  // construction: runtime values cannot recover the declared type when
  // every element is NULL (an all-NULL INT64 column produced ARRAY<>).
  mutable std::string resolved_element_type_;
  // ARRAY_CONCAT_AGG declared element type (captured even for empty arrays).
  mutable std::string concat_elem_type_;
  // ELEMENTWISE_SUM/AVG positional state.
  mutable std::vector<int64_t> ew_int_sum_;
  mutable std::vector<double> ew_double_sum_;
  mutable std::vector<int64_t> ew_count_;
  mutable std::vector<bool> ew_saw_double_;
  mutable size_t ew_len_{0};
  mutable bool ew_any_input_{false};
  mutable std::string ew_input_elem_type_;

  // Extended-aggregate state. Kept in one variant-ish blob so the common
  // aggregate path stays lean.
  struct StatState {
    long double sx = 0;   // Σx (or Σy for two-input forms)
    long double sxx = 0;  // Σx²
    long double sy = 0;   // Σy
    long double syy = 0;  // Σy²
    long double sxy = 0;  // Σxy
  };
  StatState stat_;
  bool saw_any_ = false;                        // ANY_VALUE captured its value
  bool saw_null_param_ = false;                 // KLL INIT: NULL precision seen
  mutable std::vector<Value> quantile_values_;  // APPROX_QUANTILES inputs
  int64_t quantile_count_ = 0;
  bool quantile_count_valid_ = false;
  int64_t top_count_limit_ = 0;
  bool top_count_valid_ = false;
  struct SumWeight {
    Value value;
    long double sum = 0;
    int64_t int_sum = 0;
    int64_t weights = 0;  // non-null weight count (0 => NULL sum)
    bool is_double = false;
  };
  // Entries kept insertion-ordered; lookup uses NULL-aware equality because
  // NULL is a legitimate APPROX_TOP_* input and Value::operator< rejects it.
  mutable std::vector<SumWeight> top_sums_;
  mutable std::vector<Value> top_count_values_;
  mutable std::vector<int64_t> top_count_counts_;
  mutable std::vector<std::string> sketch_values_;  // HLL/KLL distinct entries
  mutable int sketch_type_ = 0;                     // 0 = unset
  mutable int64_t sketch_precision_ = 0;
  // PERCENTILE_CONT inputs and the group-wide percentile parameter.
  mutable std::vector<double> percentile_values_;
  double percentile_p_ = 0;
  bool percentile_p_valid_ = false;
  // APPROX_COUNT_DISTINCT distinct-value tracking (exact within engine).
  std::unique_ptr<std::unordered_set<Value>> approx_distinct_;

  Status TryApplyCore(const Value& value,
                      const std::vector<Value>& trailing_values = {},
                      const std::vector<Value>& order_keys = {});
  // EXC-SHIM (see no-exception-rule-migration.md).
  void ApplyCore(const Value& value, const std::vector<Value>& trailing = {},
                 const std::vector<Value>& order = {}) {
    ExcShimCheck("ApplyCore", TryApplyCore(value, trailing, order));
  }

  Status TryRecordQuantileParam(const std::vector<Value>& trailing_values);
  // EXC-SHIM (see no-exception-rule-migration.md).
  void RecordQuantileParam(const std::vector<Value>& t) {
    ExcShimCheck("RecordQuantileParam", TryRecordQuantileParam(t));
  }
  Status TryRecordLimitParam(const std::vector<Value>& trailing_values);
  // EXC-SHIM (see no-exception-rule-migration.md).
  void RecordLimitParam(const std::vector<Value>& t) {
    ExcShimCheck("RecordLimitParam", TryRecordLimitParam(t));
  }
  SumWeight& FindOrAddTopSum(const Value& value);
  int64_t& FindOrAddTopCount(const Value& value);
  Status SketchAdd(const Value& value,
                   const std::vector<Value>& trailing_values);
  Status TrySketchMerge(const Value& sketch_bytes);
  // EXC-SHIM (see no-exception-rule-migration.md).
  void SketchMerge(const Value& sketch_bytes) {
    ExcShimCheck("SketchMerge", TrySketchMerge(sketch_bytes));
  }
  // HLL_COUNT.MERGE folds EXTRACT into the aggregate; MERGE_PARTIAL returns
  // the merged sketch bytes instead.
  Value FinishSketch(bool extract_count) const;
  Status TryElementwiseApply(const Value& array);
  // EXC-SHIM (see no-exception-rule-migration.md).
  void ElementwiseApply(const Value& array) {
    ExcShimCheck("ElementwiseApply", TryElementwiseApply(array));
  }
  Value FinishApproxTop(const std::vector<BufferedRow>& rows) const;
};

using AggregateResultMap =
    std::unordered_map<const AggregateExpression*, Value>;

StatusOr<Value> TryEvaluate(const Expression& expression, const Scope& scope,
                            const AggregateResultMap* aggregates,
                            TransactionContext& context, const CteMap& ctes);

// EXC-SHIM: remove with the boundary catch (see
// no-exception-rule-migration.md).
inline Value Evaluate(const Expression& expression, const Scope& scope,
                      const AggregateResultMap* aggregates,
                      TransactionContext& context, const CteMap& ctes) {
  return ExcShimUnwrap(
      TryEvaluate(expression, scope, aggregates, context, ctes), "Evaluate");
}

Schema QualifySchema(const Schema& schema, std::string_view qualifier);

void CollectStatementColumns(const SelectStatement& statement,
                             std::unordered_set<ColumnName>* columns);

std::vector<slot_t> RequiredColumns(const SelectStatement& statement,
                                    const Schema& schema,
                                    bool ignore_star = false);

Schema ProjectSchema(const Schema& schema,
                     const std::vector<slot_t>& projection);

std::string BaseRelationCacheKey(std::string_view table,
                                 const std::vector<slot_t>* projection);

bool ReusesBaseRelation(TransactionContext& context,
                        const SelectSource& source);

std::string ProjectionName(const NamedExpression& projection, size_t index);

ValueType ValueTypeOf(const Value& value);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_EXPRESSION_EVAL_HPP
