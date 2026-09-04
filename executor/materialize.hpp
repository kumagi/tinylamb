/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_MATERIALIZE_HPP
#define TINYLAMB_EXECUTOR_MATERIALIZE_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// MaterializeExecutor caches child rows into memory/disk, acting as an
// explicit PipelineBreaker and allowing multi-pass reading over streaming
// inputs.
class MaterializeExecutor : public ExecutorBase, public PipelineBreaker {
 public:
  MaterializeExecutor(Executor child, Schema schema);
  ~MaterializeExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override;
  [[nodiscard]] size_t MaterializedRowCount() const override {
    return rows_.size();
  }
  [[nodiscard]] size_t MaterializedBytes() const override {
    return charge_.Bytes();
  }

  void Rewind();

 private:
  void EnsureMaterialized();

  Executor child_;
  Schema schema_;
  std::vector<std::pair<Row, RowPosition>> rows_;
  size_t read_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_MATERIALIZE_HPP
