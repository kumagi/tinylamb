/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_PIPELINE_PIPELINE_IR_HPP
#define TINYLAMB_EXECUTOR_DETAIL_PIPELINE_PIPELINE_IR_HPP

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/query_grant.hpp"
#include "common/runtime_filter.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TransactionContext;

struct PipelineExecutionMetrics {
  double legacy_source_ms{0.0};
  double grant_wait_ms{0.0};
  size_t granted_bytes{0};
  size_t operator_budget_bytes{0};
  // Worker count the blocking stages are actually driven with (see
  // PipelineContext::effective_dop), not the grant-permitted DOP.
  size_t effective_dop{1};
  size_t permitted_dop{1};
  OocTelemetrySnapshot ooc;
};

// Shared execution state for one compiled PipelineGraph execution (one
// SpillStore, one admitted QueryMemoryGrant, DOP, and per-query OOC telemetry).
class PipelineContext {
 public:
  PipelineContext(TransactionContext* txn_in, QueryMemoryGrant grant_in,
                  size_t dop_in = 1, SpillStoreOptions spill_options = {});
  ~PipelineContext() = default;

  PipelineContext(const PipelineContext&) = delete;
  PipelineContext& operator=(const PipelineContext&) = delete;
  PipelineContext(PipelineContext&&) = delete;
  PipelineContext& operator=(PipelineContext&&) = delete;

  [[nodiscard]] size_t OperatorBudgetBytes(size_t b_min = 0) const noexcept;
  [[nodiscard]] PipelineExecutionMetrics Snapshot() const noexcept;

  TransactionContext* txn{nullptr};
  SpillStore spill_store;
  QueryMemoryGrant grant;
  // Workers actually used: ExecuteBlockingStage drives every sink with
  // Init(ctx, 1) / Consume(ctx, 0, ...), so this stays 1 until per-worker
  // sink scheduling exists.  EXPLAIN reports this value so `dop=` is honest.
  size_t effective_dop{1};
  // Upper bound the memory grant would allow (ComputeEffectiveDop); not yet
  // connected to execution.
  size_t permitted_dop{1};
  OocQueryMetrics ooc_metrics;
  std::atomic<uint64_t> legacy_source_ns{0};
};

// Pull-based chunk producer starting a pipeline (scan, upstream sink output,
// values, or fallback LegacySource).
class PipelineSource {
 public:
  PipelineSource() = default;
  PipelineSource(const PipelineSource&) = delete;
  PipelineSource& operator=(const PipelineSource&) = delete;
  PipelineSource(PipelineSource&&) = delete;
  PipelineSource& operator=(PipelineSource&&) = delete;
  virtual ~PipelineSource() = default;

  [[nodiscard]] virtual Status Open(PipelineContext* ctx) {
    (void)ctx;
    return Status::kSuccess;
  }
  [[nodiscard]] virtual StatusOr<bool> NextBatch(
      PipelineContext* ctx, DataChunk* out,
      size_t max_rows = kDefaultVectorSize) = 0;

  virtual bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) {
    (void)group;
    (void)column;
    (void)type;
    return false;
  }

  [[nodiscard]] virtual bool IsLegacy() const noexcept { return false; }
  [[nodiscard]] virtual std::optional<uint64_t> TryExactUnfilteredRowCount(
      PipelineContext* ctx) const {
    (void)ctx;
    return std::nullopt;
  }
  [[nodiscard]] virtual std::string Describe() const = 0;
  // Optional compatibility Dump/Explain delegation for EXPLAIN/test guards.
  virtual void DumpExplain(std::ostream& out, int indent) const {
    out << std::string(static_cast<size_t>(indent), ' ') << Describe() << "\n";
  }
  virtual void Explain(std::ostream& out, int indent) const {
    DumpExplain(out, indent);
  }
};

// Stateless / pass-through chunk transform operator inside a pipeline.
class PipelineOperator {
 public:
  PipelineOperator() = default;
  PipelineOperator(const PipelineOperator&) = delete;
  PipelineOperator& operator=(const PipelineOperator&) = delete;
  PipelineOperator(PipelineOperator&&) = delete;
  PipelineOperator& operator=(PipelineOperator&&) = delete;
  virtual ~PipelineOperator() = default;

  [[nodiscard]] virtual Status Open(PipelineContext* ctx) {
    (void)ctx;
    return Status::kSuccess;
  }
  // Transforms `input` into `output`. If `*exhausted` is set to true, the
  // pipeline stops pulling further batches from its source immediately (e.g.
  // LIMIT).
  [[nodiscard]] virtual Status Process(PipelineContext* ctx,
                                       const DataChunk& input,
                                       DataChunk* output, bool* exhausted) = 0;
  [[nodiscard]] virtual std::string Describe() const = 0;
  virtual void DumpExplain(std::ostream& out, int indent) const {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << Describe() << "\n";
  }
  virtual void Explain(std::ostream& out, int indent) const {
    DumpExplain(out, indent);
  }
};

// S2 pipeline-breaking sink interface (`TODO.md` S2): Init -> Consume* ->
// Combine -> Finalize -> pullable PipelineSource for downstream stages.
class PipelineSink {
 public:
  PipelineSink() = default;
  PipelineSink(const PipelineSink&) = delete;
  PipelineSink& operator=(const PipelineSink&) = delete;
  PipelineSink(PipelineSink&&) = delete;
  PipelineSink& operator=(PipelineSink&&) = delete;
  virtual ~PipelineSink() = default;

  [[nodiscard]] virtual Status Init(PipelineContext* ctx,
                                    size_t worker_count = 1) = 0;
  [[nodiscard]] virtual Status Consume(PipelineContext* ctx, size_t worker_id,
                                       const DataChunk& input) = 0;
  [[nodiscard]] virtual Status Combine(PipelineContext* ctx) {
    (void)ctx;
    return Status::kSuccess;
  }
  [[nodiscard]] virtual StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) = 0;
  [[nodiscard]] virtual StatusOr<std::shared_ptr<PipelineSource>>
  TryConsumeExactRowCount(PipelineContext* ctx, uint64_t row_count) {
    (void)ctx;
    (void)row_count;
    return std::shared_ptr<PipelineSource>{};
  }
  [[nodiscard]] virtual std::string Describe() const = 0;
  virtual void DumpExplain(std::ostream& out, int indent) const {
    out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
        << Describe() << "\n";
  }
  virtual void Explain(std::ostream& out, int indent) const {
    DumpExplain(out, indent);
  }
};

// Late-bound source populated when an upstream dependency stage finishes
// `PipelineSink::Finalize()`.
class DeferredPipelineSource final : public PipelineSource {
 public:
  explicit DeferredPipelineSource(std::string label = "SinkOutput")
      : label_(std::move(label)) {}

  void Bind(std::shared_ptr<PipelineSource> bound) {
    bound_ = std::move(bound);
  }
  void SetUpstreamStageId(size_t stage_id) noexcept {
    upstream_stage_id_ = stage_id;
  }
  [[nodiscard]] std::optional<size_t> UpstreamStageId() const noexcept {
    return upstream_stage_id_;
  }
  [[nodiscard]] const std::shared_ptr<PipelineSource>& Bound() const noexcept {
    return bound_;
  }

  Status Open(PipelineContext* ctx) override {
    if (!bound_) {
      return StatusError(StatusCode::kRuntimeError,
                         "DeferredPipelineSource opened before Finalize");
    }
    return bound_->Open(ctx);
  }

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override {
    if (!bound_) {
      // Reading before Finalize is a lifecycle error, not a clean EOF.
      return StatusError(StatusCode::kRuntimeError,
                         "DeferredPipelineSource read before Finalize");
    }
    return bound_->NextBatch(ctx, out, max_rows);
  }

  [[nodiscard]] std::string Describe() const override {
    return bound_ ? bound_->Describe() : label_;
  }
  void DumpExplain(std::ostream& out, int indent) const override;
  void Explain(std::ostream& out, int indent) const override;

 private:
  std::string label_;
  std::optional<size_t> upstream_stage_id_;
  std::shared_ptr<PipelineSource> bound_;
};

// Wraps a genuinely O(1)-streaming leaf ExecutorBase (FullScan, IndexScan,
// IndexOnlyScan, Values, Empty). Does not record `legacy_source_ms`.
class StreamingExecutorSource final : public PipelineSource {
 public:
  StreamingExecutorSource(Executor exec, std::string label)
      : exec_(std::move(exec)), label_(std::move(label)) {}

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) override;
  [[nodiscard]] std::optional<uint64_t> TryExactUnfilteredRowCount(
      PipelineContext* ctx) const override;
  [[nodiscard]] std::string Describe() const override { return label_; }
  void DumpExplain(std::ostream& out, int indent) const override;
  void Explain(std::ostream& out, int indent) const override;
  [[nodiscard]] const Executor& Underlying() const noexcept { return exec_; }

 private:
  Executor exec_;
  std::string label_;
};

// Explicit fallback adapter wrapping an unmigrated `Executor` tree into a
// `PipelineSource`. Wall-clock time spent pulling batches is tracked in
// `legacy_source_ms` so P3 parity/graduation gates can enforce 0 ms on migrated
// plans.
class LegacySource final : public PipelineSource {
 public:
  LegacySource(Executor exec, std::string label = "LegacySource")
      : exec_(std::move(exec)), label_(std::move(label)) {}

  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] bool IsLegacy() const noexcept override { return true; }
  [[nodiscard]] std::string Describe() const override { return label_; }
  void DumpExplain(std::ostream& out, int indent) const override;
  void Explain(std::ostream& out, int indent) const override;
  [[nodiscard]] const Executor& Underlying() const noexcept { return exec_; }

 private:
  Executor exec_;
  std::string label_;
};

struct PipelineStage {
  size_t id{0};
  std::string name;
  std::shared_ptr<PipelineSource> source;
  std::vector<std::shared_ptr<PipelineOperator>> operators;
  std::shared_ptr<PipelineSink> sink;
  std::shared_ptr<DeferredPipelineSource> output_binding;
};

struct PipelineGraph {
  // Stages sorted in topological execution order; the final stage feeds root
  // `PipelineExecutor::NextBatch()` (either directly from its streaming
  // source+operators or via its `sink->Finalize()` source).
  std::vector<PipelineStage> stages;
  Schema output_schema;
  // Optional optimizer/plan dump text rendered in Dump() before Pipeline lines.
  std::string plan_explain_text;
  size_t estimated_grant_bytes{0};
  size_t min_grant_bytes{0};
  size_t requested_dop{1};
};

// Preferred grant a lowering may request for one pipeline, clamped to
// [256 KiB, per-query grant ceiling].  The ceiling is the coordinator's
// documented per-query bound (`TINYLAMB_QUERY_MEMORY_BYTES`, default 8 GiB,
// never above the effective query-pool max); the coordinator still caps the
// request to the pool and shrinks it toward min_grant_bytes under
// contention, so asking for the working-set estimate is safe.  The old fixed
// 64 MiB request cap made the configured query budget unreachable on the
// default lane: any stage set estimated above ~2.7M rows spilled on sight.
inline size_t PipelineGrantRequestBytes(size_t estimated_bytes) noexcept {
  const size_t floor = size_t{256} << 10;
  size_t ceiling = QueryMemoryCoordinator::Global().MaxGrantFromEnv();
  if (ceiling == 0) {  // 0 is the env knob's "unlimited" sentinel.
    ceiling = std::numeric_limits<size_t>::max();
  }
  return std::clamp<size_t>(estimated_bytes, floor, std::max(floor, ceiling));
}

// Tier 0 pull-driven S2 scheduler wrapped as an `ExecutorBase` for clean
// integration with `SqlEngine` and `PostgresServer`.
class PipelineExecutor final : public ExecutorBase {
 public:
  PipelineExecutor(
      TransactionContext* txn, PipelineGraph graph,
      std::optional<QueryMemoryGrant> pre_acquired_grant = std::nullopt);
  ~PipelineExecutor() override;

  PipelineExecutor(const PipelineExecutor&) = delete;
  PipelineExecutor& operator=(const PipelineExecutor&) = delete;
  PipelineExecutor(PipelineExecutor&&) = delete;
  PipelineExecutor& operator=(PipelineExecutor&&) = delete;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;
  void Explain(std::ostream& output, int indent) const override;
  bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) override;

  [[nodiscard]] std::string ExplainPipeline() const;
  [[nodiscard]] PipelineExecutionMetrics Metrics() const;
  [[nodiscard]] const Schema& OutputSchema() const noexcept {
    return graph_.output_schema;
  }

 private:
  friend class DeferredPipelineSource;
  [[nodiscard]] Status Initialize();
  [[nodiscard]] Status ExecuteBlockingStage(PipelineStage* stage);
  [[nodiscard]] Status ApplyOperators(
      const std::vector<std::shared_ptr<PipelineOperator>>& ops,
      DataChunk* chunk, bool* exhausted);
  void RenderStageTree(size_t stage_idx, std::ostream& out, int indent,
                       bool is_dump) const;

  TransactionContext* txn_{nullptr};
  PipelineGraph graph_;
  std::optional<QueryMemoryGrant> pre_acquired_grant_;
  std::unique_ptr<PipelineContext> ctx_;

  std::shared_ptr<PipelineSource> root_source_;
  std::vector<std::shared_ptr<PipelineOperator>> root_ops_;
  bool initialized_{false};
  bool root_exhausted_{false};
  DataChunk scratch_chunk_;
  std::vector<std::string> stage_descriptions_;
  std::vector<std::string> stage_source_dumps_;
  std::string pipeline_summary_cache_;
  mutable PipelineExecutionMetrics completed_metrics_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_PIPELINE_PIPELINE_IR_HPP
