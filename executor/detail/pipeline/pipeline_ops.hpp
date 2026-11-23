/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_PIPELINE_PIPELINE_OPS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_PIPELINE_PIPELINE_OPS_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "executor/detail/ooc/spillable_store.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/distinct.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class EmptyPipelineSource final : public PipelineSource {
 public:
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override {
    (void)ctx;
    (void)max_rows;
    out->Reset();
    return false;
  }
  [[nodiscard]] std::string Describe() const override { return "Empty"; }
};

class ChunkVectorSource final : public PipelineSource {
 public:
  explicit ChunkVectorSource(std::vector<DataChunk> chunks,
                             std::string label = "ChunkVector")
      : chunks_(std::move(chunks)), label_(std::move(label)) {}

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  std::vector<DataChunk> chunks_;
  size_t idx_{0};
  size_t row_in_chunk_{0};
  std::string label_;
};

class RowVectorSource final : public PipelineSource {
 public:
  explicit RowVectorSource(std::vector<Row> rows,
                           std::string label = "RowVector")
      : rows_(std::move(rows)), label_(std::move(label)) {}

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  std::vector<Row> rows_;
  size_t idx_{0};
  std::string label_;
};

class SpillableStoreSource final : public PipelineSource {
 public:
  explicit SpillableStoreSource(std::unique_ptr<SpillableStore> store,
                                std::string label = "SpillableStore")
      : store_(std::move(store)), label_(std::move(label)) {}

  Status Open(PipelineContext* ctx) override {
    (void)ctx;
    active_chunk_.Reset();
    active_row_ = 0;
    // T3511: a null store used to report success while NextBatch() then
    // treated it as "input exhausted" (false), so a mis-wired OOC source
    // silently ended the branch with zero rows. Misconfiguration must fail
    // loudly at the Status boundary.
    if (store_ == nullptr) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SpillableStoreSource opened without a spill store");
    }
    return store_->ResetCursor();
  }
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  std::unique_ptr<SpillableStore> store_;
  DataChunk active_chunk_;
  size_t active_row_{0};
  std::string label_;
};

// Reuses tinylamb's battle-tested SIMD/JIT/Bytecode/CSE Selection evaluator on
// a pull-pushed per-chunk adapter without buffering across chunks.
class FilterOp final : public PipelineOperator {
 public:
  FilterOp(Expression predicate, Schema input_schema);

  Status Process(PipelineContext* ctx, const DataChunk& input,
                 DataChunk* output, bool* exhausted) override;
  [[nodiscard]] std::string Describe() const override { return "Selection"; }
  [[nodiscard]] const Expression& Predicate() const noexcept {
    return predicate_;
  }
  [[nodiscard]] const Schema& InputSchema() const noexcept {
    return input_schema_;
  }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  Expression predicate_;
  Schema input_schema_;
  std::unique_ptr<Selection> selection_;
};

// Reuses tinylamb's SIMD/JIT/Bytecode/CSE Projection evaluator on a
// single-chunk adapter without materializing upstream or downstream batches.
class ProjectOp final : public PipelineOperator {
 public:
  ProjectOp(std::vector<NamedExpression> expressions, Schema input_schema);

  Status Process(PipelineContext* ctx, const DataChunk& input,
                 DataChunk* output, bool* exhausted) override;
  [[nodiscard]] std::string Describe() const override { return "Projection"; }
  [[nodiscard]] const std::vector<NamedExpression>& Expressions() const noexcept {
    return expressions_;
  }
  [[nodiscard]] const Schema& InputSchema() const noexcept {
    return input_schema_;
  }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::vector<NamedExpression> expressions_;
  Schema input_schema_;
  std::unique_ptr<Projection> projection_;
};

// Metadata pass-through preserving alias boundaries in EXPLAIN trees.
// Copies the DataChunk (column buffers and payloads are deep-copied), so it
// is not zero-copy; the copy is the price of handing downstream stages an
// independent batch.
class RenameOp final : public PipelineOperator {
 public:
  RenameOp(std::string physical, std::string relation)
      : physical_(std::move(physical)), relation_(std::move(relation)) {}

  Status Process(PipelineContext* /*ctx*/, const DataChunk& input,
                 DataChunk* output, bool* /*exhausted*/) override {
    *output = input;
    return Status::kSuccess;
  }
  [[nodiscard]] std::string Describe() const override { return "Rename"; }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::string physical_;
  std::string relation_;
};

// Stateless pass-through LIMIT + OFFSET tracker that signals `*exhausted =
// true` as soon as the limit is satisfied so scans short-circuit cleanly.
class LimitOp final : public PipelineOperator {
 public:
  LimitOp(size_t limit, size_t offset = 0, bool has_limit = true)
      : limit_(limit), offset_(offset), has_limit_(has_limit) {}

  Status Process(PipelineContext* ctx, const DataChunk& input,
                 DataChunk* output, bool* exhausted) override;
  [[nodiscard]] std::string Describe() const override;
  void DumpExplain(std::ostream& out, int indent) const override;
  void Explain(std::ostream& out, int indent) const override;

 private:
  size_t limit_{0};
  size_t offset_{0};
  bool has_limit_{true};
  size_t skipped_{0};
  size_t emitted_{0};
  size_t consumed_rows_{0};
  bool early_stop_{false};
};

// Streaming adjacent duplicate suppressor for `SortDistinctPlan`.
class AdjacentDistinctOp final : public PipelineOperator {
 public:
  Status Process(PipelineContext* ctx, const DataChunk& input,
                 DataChunk* output, bool* exhausted) override;
  [[nodiscard]] std::string Describe() const override { return "SortDistinct"; }

 private:
  bool have_prev_{false};
  Row prev_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_PIPELINE_PIPELINE_OPS_HPP
