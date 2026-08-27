/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DISTRIBUTED_AGG_FINALIZE_HPP
#define TINYLAMB_EXECUTOR_DISTRIBUTED_AGG_FINALIZE_HPP

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Multi-node two-phase distributed aggregation finalizer.
// Consumes partial aggregation streams from multiple workers / partitions,
// merges intermediate accumulator states per group, and emits finalized rows.
class DistributedAggFinalize : public ExecutorBase, public PipelineBreaker {
 public:
  enum class AggType : uint8_t {
    kCount,
    kSum,
    kAvg,
    kMin,
    kMax,
  };

  struct AggregateSpec {
    AggType type{AggType::kSum};
    slot_t partial_val_slot{0};
    slot_t partial_count_slot{0};  // Used for merging kAvg (partial sum + partial count)
  };

  DistributedAggFinalize(std::vector<Executor> partial_sources,
                         Schema output_schema, std::vector<slot_t> group_cols,
                         std::vector<AggregateSpec> aggregates);

  ~DistributedAggFinalize() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override;
  [[nodiscard]] size_t MaterializedRowCount() const override {
    return finalized_rows_.size();
  }
  [[nodiscard]] size_t MaterializedBytes() const override {
    return charge_.Bytes();
  }

  [[nodiscard]] size_t GroupCount() const { return finalized_rows_.size(); }

 private:
  struct GroupAccumulator {
    std::vector<Value> group_values;
    struct AccumState {
      int64_t count{0};
      double sum{0.0};
      Value min_val;
      Value max_val;
      bool has_val{false};
    };
    std::vector<AccumState> states;
  };

  void EnsureMaterialized();
  void MergePartialStreams();
  void FinalizeGroups();
  [[nodiscard]] std::string EncodeGroupKey(const Row& row) const;

  std::vector<Executor> sources_;
  Schema output_schema_;
  std::vector<slot_t> group_cols_;
  std::vector<AggregateSpec> aggregates_;

  std::unordered_map<std::string, GroupAccumulator> group_map_;
  std::vector<std::pair<Row, RowPosition>> finalized_rows_;
  size_t output_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DISTRIBUTED_AGG_FINALIZE_HPP
