/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_MERGE_JOIN_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_MERGE_JOIN_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/join_kind.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Multi-threaded parallel merge join with partition range steering.
// Intakes pre-sorted inputs, splits them into disjoint key-range partitions
// (steering boundaries) without splitting equal-key clusters, and executes
// merge join concurrently across worker threads.
class ParallelMergeJoin : public ExecutorBase, public PipelineBreaker {
 public:
  struct PartitionRange {
    size_t left_start{0};
    size_t left_end{0};
    size_t right_start{0};
    size_t right_end{0};
  };

  ParallelMergeJoin(
      Executor left, std::vector<slot_t> left_cols, Executor right,
      std::vector<slot_t> right_cols,
      size_t worker_count = std::thread::hardware_concurrency(),
      JoinKind kind = JoinKind::kInner, Expression residual = Expression(),
      Schema residual_schema = Schema());

  ~ParallelMergeJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override;
  [[nodiscard]] size_t MaterializedRowCount() const override { return output_.size(); }
  [[nodiscard]] size_t MaterializedBytes() const override { return charge_.Bytes(); }

  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }
  [[nodiscard]] JoinKind Kind() const { return kind_; }
  [[nodiscard]] const std::vector<PartitionRange>& Partitions() const {
    return partitions_;
  }

 private:
  void EnsureMaterialized();
  void ComputeSteeringPartitions();
  void ExecuteParallelMerge();

  int CompareKeys(const Row& left, const Row& right) const;
  bool KeyIsNull(const Row& row, const std::vector<slot_t>& cols) const;

  Executor left_;
  std::vector<slot_t> left_cols_;
  Executor right_;
  std::vector<slot_t> right_cols_;
  size_t worker_count_{1};
  JoinKind kind_{JoinKind::kInner};
  Expression residual_;
  Schema residual_schema_;

  std::vector<std::pair<Row, RowPosition>> left_rows_;
  std::vector<std::pair<Row, RowPosition>> right_rows_;
  std::vector<PartitionRange> partitions_;
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_MERGE_JOIN_HPP
