/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_INCREMENTAL_SORT_HPP
#define TINYLAMB_EXECUTOR_INCREMENTAL_SORT_HPP

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

// Incremental sort that exploits presorted leading prefix columns to sort
// only trailing suffix columns within each prefix group.
class IncrementalSortExecutor : public ExecutorBase, public PipelineBreaker {
 public:
  IncrementalSortExecutor(Executor source, Schema schema,
                          std::vector<SortExecutor::Key> prefix_keys,
                          std::vector<SortExecutor::Key> suffix_keys);

  ~IncrementalSortExecutor() override = default;

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
  void ExecuteIncrementalSort();
  [[nodiscard]] bool ArePrefixEqual(const Row& a, const Row& b) const;

  Executor source_;
  Schema schema_;
  std::vector<SortExecutor::Key> prefix_keys_;
  std::vector<SortExecutor::Key> suffix_keys_;

  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_INCREMENTAL_SORT_HPP
