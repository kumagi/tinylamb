/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP

#include <cstddef>
#include <exception>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "expression/named_expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Global aggregation with one independent state per worker followed by a
// deterministic merge.  DISTINCT sets are merged by value, not by combining
// partial counts, so duplicates spanning morsels remain correct.
class ParallelAggregationExecutor final : public ExecutorBase {
 public:
  ParallelAggregationExecutor(
      std::shared_ptr<ExecutorBase> child, Schema input_schema,
      std::vector<NamedExpression> aggregates,
      size_t worker_count = std::thread::hardware_concurrency());

  bool Next(Row* destination, RowPosition* position) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& out, int indent) const override;

  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }

 private:
  // How each aggregate reads its input.  kInt64Column / kDoubleColumn
  // aggregates can run directly over the chunk's raw numeric storage;
  // everything else falls back to the per-row generic path.
  enum class AggregateInputKind : uint8_t {
    kRowCount,
    kInt64Column,
    kDoubleColumn,
    kStatColumn,  // statistical aggregate over raw numeric storage
    kGeneric
  };
  struct AggregateInput {
    AggregateInputKind kind{AggregateInputKind::kGeneric};
    size_t column{0};
    // Second input of a two-argument statistical aggregate (COVAR_* / CORR).
    size_t trailing_column{0};
  };

  struct PartialState {
    std::vector<Value> values;
    std::vector<int64_t> counts;
    // Long-double partial sums for statistical aggregates, indexed like
    // values/counts.  They mirror AggregateAccumulator::StatState so merged
    // partials finalize exactly like the serial ground-truth path.
    std::vector<long double> stat_sx;   // Σ second COVAR arg (or sole input)
    std::vector<long double> stat_sxx;  // Σ of its square
    std::vector<long double> stat_sy;   // Σ first COVAR arg
    std::vector<long double> stat_syy;  // Σ of its square
    std::vector<long double> stat_sxy;  // Σ of the pairwise products
    std::vector<relational_detail::DistinctValueSet> distinct_values;
    // Every retained distinct value holds a forced global-budget reservation;
    // release the retained total when the state dies so the process-wide
    // budget cannot ratchet up across repeated aggregations.
    size_t distinct_charged_bytes{0};
    ~PartialState() {
      if (distinct_charged_bytes != 0) {
        QueryMemoryBudget::Global().Release(distinct_charged_bytes);
      }
    }
  };

  [[nodiscard]] PartialState MakeState() const;
  void Accumulate(PartialState* state, const DataChunk& chunk) const;
  // Walks `always_generic` and `fallback` (both ascending, disjoint) as one
  // ascending index sequence.
  void AccumulateGeneric(PartialState* state, const DataChunk& chunk,
                         const std::vector<size_t>& always_generic,
                         const std::vector<size_t>& fallback) const;
  void AccumulateInt64Column(PartialState* state, size_t aggregate_index,
                             const ColumnVector& column) const;
  void AccumulateDoubleColumn(PartialState* state, size_t aggregate_index,
                              const ColumnVector& column) const;
  // Fast-path statistical accumulation over raw numeric storage; `trailing`
  // is null for the single-input forms (VAR_* / STDDEV_*).
  static void AccumulateStatColumns(PartialState* state, size_t aggregate_index,
                                    const ColumnVector& child,
                                    const ColumnVector* trailing);
  // Per-row statistical accumulation for expression arguments; mirrors
  // AggregateAccumulator::ApplyCore's paired NULL semantics.
  void AccumulateStatValue(PartialState* state, size_t aggregate_index,
                           const Value& value,
                           const std::vector<Value>& trailing_values) const;
  void AccumulateValue(PartialState* state, size_t aggregate_index,
                       const Value& value, bool apply_distinct) const;
  // Applies the statistical finalize formulas (VAR/STDDEV/COVAR/CORR) to the
  // merged long-double partials; NULL for inputs too few to divide.
  [[nodiscard]] Value FinalizeStat(const PartialState& state,
                                   size_t index) const;
  void Merge(PartialState* destination, const PartialState& source) const;
  [[nodiscard]] Row Finalize(PartialState state) const;

  std::shared_ptr<ExecutorBase> child_;
  Schema input_schema_;
  std::vector<NamedExpression> aggregates_;
  std::vector<AggregateInput> inputs_;
  // Aggregate indices grouped by input kind, computed once in the
  // constructor so Accumulate never rebuilds per-chunk selection state.
  std::vector<size_t> row_count_indices_;
  std::vector<size_t> int64_column_indices_;
  std::vector<size_t> double_column_indices_;
  std::vector<size_t> stat_column_indices_;
  std::vector<size_t> generic_indices_;
  size_t worker_count_;
  bool executed_{false};
  // A failed execution must never degrade into a normal-looking empty
  // aggregate on a subsequent Next(); the original error is rethrown.
  bool errored_{false};
  std::exception_ptr error_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP
