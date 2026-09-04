/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARTIAL_SORT_HPP
#define TINYLAMB_EXECUTOR_PARTIAL_SORT_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// PartialSortExecutor: sorts only until Top-K elements or within partitioned
// blocks without sorting entire runs.
class PartialSortExecutor : public ExecutorBase, public PipelineBreaker {
 public:
  PartialSortExecutor(Executor source, Schema schema,
                      std::vector<SortExecutor::Key> keys, size_t top_k,
                      size_t offset = 0, size_t block_size = 0);

  ~PartialSortExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override;
  [[nodiscard]] size_t MaterializedRowCount() const override {
    return output_.size();
  }
  [[nodiscard]] size_t MaterializedBytes() const override {
    return charge_.Bytes();
  }

 private:
  void EnsureMaterialized();
  void ExecutePartialSort();

  Executor source_;
  Schema schema_;
  std::vector<SortExecutor::Key> keys_;
  size_t top_k_{0};
  size_t offset_{0};
  size_t block_size_{0};

  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARTIAL_SORT_HPP
