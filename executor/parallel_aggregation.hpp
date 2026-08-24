/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP

#include <cstddef>
#include <exception>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "executor/executor_base.hpp"
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
  enum class AggregateInputKind { kRowCount, kInt64Column, kDoubleColumn,
                                  kGeneric };
  struct AggregateInput {
    AggregateInputKind kind{AggregateInputKind::kGeneric};
    size_t column{0};
  };

  struct PartialState {
    std::vector<Value> values;
    std::vector<int64_t> counts;
    std::vector<std::unordered_set<Value>> distinct_values;
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
  void AccumulateValue(PartialState* state, size_t aggregate_index,
                       const Value& value, bool apply_distinct) const;
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
  std::vector<size_t> generic_indices_;
  // Scratch for per-chunk generic fallbacks; capacity is retained across
  // chunks so the hot path stays allocation-free.
  mutable std::vector<size_t> generic_scratch_;
  size_t worker_count_;
  bool executed_{false};
  // A failed execution must never degrade into a normal-looking empty
  // aggregate on a subsequent Next(); the original error is rethrown.
  bool errored_{false};
  std::exception_ptr error_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP
