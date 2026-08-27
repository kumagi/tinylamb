/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_BATCH_NESTED_LOOP_JOIN_HPP
#define TINYLAMB_EXECUTOR_BATCH_NESTED_LOOP_JOIN_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
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

// Block-nested loop join buffering outer relation blocks and evaluating inner
// relation matches in batches to minimize rescans.
class BatchNestedLoopJoin : public ExecutorBase, public PipelineBreaker {
 public:
  BatchNestedLoopJoin(Executor left, Schema left_schema, Executor right,
                      Schema right_schema, Expression predicate = Expression(),
                      JoinKind kind = JoinKind::kInner,
                      size_t block_size = 1024);

  ~BatchNestedLoopJoin() override = default;

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

  [[nodiscard]] JoinKind Kind() const { return kind_; }
  [[nodiscard]] size_t BlockSize() const { return block_size_; }

 private:
  void EnsureMaterialized();
  void ExecuteBlockJoin();
  [[nodiscard]] bool EvaluatePredicate(const Row& left, const Row& right) const;

  Executor left_;
  Schema left_schema_;
  Executor right_;
  Schema right_schema_;
  Expression predicate_;
  JoinKind kind_{JoinKind::kInner};
  size_t block_size_{1024};
  Schema combined_schema_;

  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_BATCH_NESTED_LOOP_JOIN_HPP
