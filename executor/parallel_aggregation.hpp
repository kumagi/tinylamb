/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP

#include <cstddef>
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
  struct PartialState {
    std::vector<Value> values;
    std::vector<int64_t> counts;
    std::vector<std::unordered_set<Value>> distinct_values;
  };

  [[nodiscard]] PartialState MakeState() const;
  void Accumulate(PartialState* state, const DataChunk& chunk) const;
  void AccumulateValue(PartialState* state, size_t aggregate_index,
                       const Value& value, bool apply_distinct) const;
  void Merge(PartialState* destination, const PartialState& source) const;
  [[nodiscard]] Row Finalize(PartialState state) const;

  std::shared_ptr<ExecutorBase> child_;
  Schema input_schema_;
  std::vector<NamedExpression> aggregates_;
  size_t worker_count_;
  bool executed_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_AGGREGATION_HPP
