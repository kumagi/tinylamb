/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_EXCHANGE_HPP
#define TINYLAMB_EXECUTOR_EXCHANGE_HPP

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
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

enum class ExchangeType : uint8_t {
  kHash,
  kBroadcast,
  kGather,
  kRange,
};

// ExchangeExecutor distributes or gathers rows across multiple worker/partition
// channels using Hash, Broadcast, Gather, or Range partitioning strategies.
class ExchangeExecutor : public ExecutorBase, public PipelineBreaker {
 public:
  ExchangeExecutor(
      Executor child, ExchangeType type, size_t partition_count,
      std::vector<slot_t> key_cols = {},
      std::vector<Value> range_bounds = {});

  ~ExchangeExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override;
  [[nodiscard]] size_t MaterializedRowCount() const override;
  [[nodiscard]] size_t MaterializedBytes() const override { return charge_.Bytes(); }

  [[nodiscard]] size_t PartitionCount() const { return partition_count_; }
  [[nodiscard]] ExchangeType Type() const { return type_; }
  [[nodiscard]] const std::vector<std::pair<Row, RowPosition>>& GetPartitionRows(
      size_t partition_idx) const;

  // Returns an Executor view over a single partition channel
  [[nodiscard]] Executor GetPartitionExecutor(size_t partition_idx);

 private:
  void EnsureMaterialized();
  void DistributeRows();
  [[nodiscard]] size_t RouteRow(const Row& row) const;

  Executor child_;
  ExchangeType type_{ExchangeType::kGather};
  size_t partition_count_{1};
  std::vector<slot_t> key_cols_;
  std::vector<Value> range_bounds_;

  std::vector<std::vector<std::pair<Row, RowPosition>>> partitions_;
  size_t current_gather_part_{0};
  size_t current_gather_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_EXCHANGE_HPP
