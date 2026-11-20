/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/pipeline/pipeline_ir.hpp"

#include <algorithm>
#include <chrono>
#include <sstream>

namespace tinylamb {

PipelineContext::PipelineContext(TransactionContext* txn_in,
                                 QueryMemoryGrant grant_in, size_t dop_in,
                                 SpillStoreOptions spill_options)
    : txn(txn_in),
      spill_store(std::move(spill_options)),
      grant(std::move(grant_in)),
      effective_dop(1),
      permitted_dop(std::max<size_t>(1, dop_in)) {}

size_t PipelineContext::OperatorBudgetBytes(size_t b_min) const noexcept {
  const size_t override_bytes =
      ProcessMemoryBudget::Global().OpMemoryOverrideBytes();
  if (override_bytes > 0) {
    return b_min > 0 ? std::max(override_bytes, b_min) : override_bytes;
  }
  if (!grant.Unlimited()) {
    return grant.OperatorBudgetBytes(b_min);
  }
  const size_t pool_max = ProcessMemoryBudget::Global().EffectivePoolMaxBytes(
      MemoryPoolKind::kQuery);
  if (pool_max > 0) {
    const size_t active = std::max<size_t>(1, grant.ActiveConsumerCount());
    const size_t quarter = std::max<size_t>(b_min, pool_max / (4 * active));
    return quarter;
  }
  return std::max<size_t>(b_min, size_t{64} << 20);
}

PipelineExecutionMetrics PipelineContext::Snapshot() const noexcept {
  PipelineExecutionMetrics m;
  m.legacy_source_ms =
      static_cast<double>(legacy_source_ns.load(std::memory_order_relaxed)) /
      1e6;
  m.grant_wait_ms = static_cast<double>(grant.WaitNs()) / 1e6;
  m.granted_bytes = grant.GrantedBytes();
  m.operator_budget_bytes = OperatorBudgetBytes();
  m.effective_dop = effective_dop;
  m.permitted_dop = permitted_dop;
  m.ooc = ooc_metrics.Snapshot();
  return m;
}

StatusOr<bool> StreamingExecutorSource::NextBatch(PipelineContext* /*ctx*/,
                                                  DataChunk* out,
                                                  size_t max_rows) {
  if (!exec_) {
    out->Reset();
    return false;
  }
  const size_t n = exec_->NextBatch(out, max_rows);
  if (exec_->GetStatus() != Status::kSuccess) {
    return exec_->GetStatus();
  }
  if (n == 0) {
    return false;
  }
  return true;
}

bool StreamingExecutorSource::AttachRuntimeFilter(
    const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
    ValueType type) {
  return exec_ && exec_->AttachRuntimeFilter(group, column, type);
}

std::optional<uint64_t> StreamingExecutorSource::TryExactUnfilteredRowCount(
    PipelineContext* /*ctx*/) const {
  return exec_ ? exec_->TryExactUnfilteredRowCount() : std::nullopt;
}

namespace {

thread_local const PipelineExecutor* tl_render_executor = nullptr;

class ScopedRenderExecutor {
 public:
  explicit ScopedRenderExecutor(const PipelineExecutor* exec)
      : prev_(tl_render_executor) {
    tl_render_executor = exec;
  }
  ScopedRenderExecutor(const ScopedRenderExecutor&) = delete;
  ScopedRenderExecutor& operator=(const ScopedRenderExecutor&) = delete;
  ScopedRenderExecutor(ScopedRenderExecutor&&) = delete;
  ScopedRenderExecutor& operator=(ScopedRenderExecutor&&) = delete;
  ~ScopedRenderExecutor() { tl_render_executor = prev_; }

 private:
  const PipelineExecutor* prev_{nullptr};
};

}  // namespace

void DeferredPipelineSource::DumpExplain(std::ostream& out, int indent) const {
  if (tl_render_executor != nullptr && upstream_stage_id_.has_value()) {
    tl_render_executor->RenderStageTree(*upstream_stage_id_, out, indent,
                                        /*is_dump=*/true);
    return;
  }
  out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << Describe() << "\n";
}

void DeferredPipelineSource::Explain(std::ostream& out, int indent) const {
  if (tl_render_executor != nullptr && upstream_stage_id_.has_value()) {
    tl_render_executor->RenderStageTree(*upstream_stage_id_, out, indent,
                                        /*is_dump=*/false);
    return;
  }
  out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
      << Describe() << "\n";
}

void StreamingExecutorSource::DumpExplain(std::ostream& out, int indent) const {
  if (exec_) {
    exec_->Dump(out, indent);
  } else {
    out << std::string(static_cast<size_t>(indent), ' ') << label_ << "\n";
  }
}

void StreamingExecutorSource::Explain(std::ostream& out, int indent) const {
  if (exec_) {
    exec_->Explain(out, indent);
  } else {
    out << std::string(static_cast<size_t>(indent), ' ') << label_ << "\n";
  }
}

StatusOr<bool> LegacySource::NextBatch(PipelineContext* ctx, DataChunk* out,
                                       size_t max_rows) {
  if (!exec_) {
    out->Reset();
    return false;
  }
  // Timing a legacy batch costs two vDSO clock reads plus a shared atomic
  // RMW. Make that diagnostic opt-in so ordinary queries do not pay it.
  static const bool kTimeLegacySource =
      std::getenv("TINYLAMB_PIPELINE_METRICS") != nullptr;
  const bool time_it = ctx != nullptr && kTimeLegacySource;
  const auto t0 = time_it ? std::chrono::steady_clock::now()
                          : std::chrono::steady_clock::time_point{};
  const size_t n = exec_->NextBatch(out, max_rows);
  if (time_it) {
    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
    if (dt > 0) {
      ctx->legacy_source_ns.fetch_add(static_cast<uint64_t>(dt),
                                      std::memory_order_relaxed);
    }
  }
  if (exec_->GetStatus() != Status::kSuccess) {
    return exec_->GetStatus();
  }
  if (n == 0) {
    return false;
  }
  return true;
}

void LegacySource::DumpExplain(std::ostream& out, int indent) const {
  if (exec_) {
    exec_->Dump(out, indent);
  } else {
    out << std::string(static_cast<size_t>(indent), ' ') << label_ << "\n";
  }
}

void LegacySource::Explain(std::ostream& out, int indent) const {
  if (exec_) {
    exec_->Explain(out, indent);
  } else {
    out << std::string(static_cast<size_t>(indent), ' ') << label_ << "\n";
  }
}

PipelineExecutor::PipelineExecutor(
    TransactionContext* txn, PipelineGraph graph,
    std::optional<QueryMemoryGrant> pre_acquired_grant)
    : txn_(txn),
      graph_(std::move(graph)),
      pre_acquired_grant_(std::move(pre_acquired_grant)) {
  std::ostringstream oss;
  stage_descriptions_.reserve(graph_.stages.size());
  for (size_t i = 0; i < graph_.stages.size(); ++i) {
    const PipelineStage& s = graph_.stages[i];
    std::ostringstream line;
    line << "Stage " << s.id;
    if (!s.name.empty()) {
      line << " (" << s.name << ")";
    }
    line << ": ";
    if (s.source) {
      line << s.source->Describe();
    }
    for (const auto& op : s.operators) {
      line << " -> " << op->Describe();
    }
    if (s.sink) {
      line << " -> " << s.sink->Describe();
    } else {
      line << " -> Output";
    }
    std::string stage_str = line.str();
    if (i > 0) {
      oss << " | ";
    }
    oss << stage_str;
    stage_descriptions_.push_back(std::move(stage_str));
  }
  pipeline_summary_cache_ = oss.str();
}

PipelineExecutor::~PipelineExecutor() {
  root_source_.reset();
  root_ops_.clear();
  graph_.stages.clear();
  ctx_.reset();
}

Status PipelineExecutor::ApplyOperators(
    const std::vector<std::shared_ptr<PipelineOperator>>& ops, DataChunk* chunk,
    bool* exhausted) {
  for (const auto& op : ops) {
    if (chunk->Empty()) {
      break;
    }
    scratch_chunk_.Reset();
    RETURN_IF_FAIL(op->Process(ctx_.get(), *chunk, &scratch_chunk_, exhausted));
    // Swap rather than move so `scratch_chunk_` keeps the consumed chunk's
    // allocated column storage for the next operator's Reset() reuse path
    // (a move would leave it a capacity-0 husk and re-allocate every column).
    std::swap(*chunk, scratch_chunk_);
    scratch_chunk_.Reset();
  }
  return Status::kSuccess;
}

Status PipelineExecutor::ExecuteBlockingStage(PipelineStage* stage) {
  if (!stage->source || !stage->sink) {
    return StatusError(StatusCode::kRuntimeError,
                       "blocking pipeline stage missing source or sink");
  }
  if (stage->operators.empty()) {
    if (const std::optional<uint64_t> exact_rows =
            stage->source->TryExactUnfilteredRowCount(ctx_.get());
        exact_rows.has_value()) {
      ASSIGN_OR_RETURN(
          std::shared_ptr<PipelineSource>, fast_src,
          stage->sink->TryConsumeExactRowCount(ctx_.get(), *exact_rows));
      if (fast_src != nullptr) {
        if (stage->output_binding != nullptr) {
          stage->output_binding->Bind(std::move(fast_src));
        }
        if (stage->id < stage_source_dumps_.size()) {
          ScopedRenderExecutor render_guard(this);
          std::ostringstream oss;
          RenderStageTree(stage->id, oss, /*indent=*/0, /*is_dump=*/true);
          stage_source_dumps_[stage->id] = oss.str();
        }
        stage->operators.clear();
        stage->source.reset();
        stage->sink.reset();
        return Status::kSuccess;
      }
    }
  }
  RETURN_IF_FAIL(stage->sink->Init(ctx_.get(), 1));
  RETURN_IF_FAIL(stage->source->Open(ctx_.get()));
  for (const auto& op : stage->operators) {
    RETURN_IF_FAIL(op->Open(ctx_.get()));
  }

  DataChunk batch;
  bool exhausted = false;
  while (!exhausted) {
    batch.Reset();
    ASSIGN_OR_RETURN(bool, has_batch,
                     stage->source->NextBatch(ctx_.get(), &batch));
    if (!has_batch) {
      break;
    }
    RETURN_IF_FAIL(ApplyOperators(stage->operators, &batch, &exhausted));
    if (!batch.Empty()) {
      RETURN_IF_FAIL(stage->sink->Consume(ctx_.get(), 0, batch));
    }
  }

  RETURN_IF_FAIL(stage->sink->Combine(ctx_.get()));
  ASSIGN_OR_RETURN(std::shared_ptr<PipelineSource>, out_src,
                   stage->sink->Finalize(ctx_.get()));
  if (stage->output_binding != nullptr) {
    stage->output_binding->Bind(std::move(out_src));
  }
  if (stage->id < stage_source_dumps_.size()) {
    ScopedRenderExecutor render_guard(this);
    std::ostringstream oss;
    RenderStageTree(stage->id, oss, /*indent=*/0, /*is_dump=*/true);
    stage_source_dumps_[stage->id] = oss.str();
  }
  // Release upstream operators/source immediately so completed stage memory is
  // reclaimed before downstream stages begin.
  stage->operators.clear();
  stage->source.reset();
  stage->sink.reset();
  return Status::kSuccess;
}

Status PipelineExecutor::Initialize() {
  if (initialized_) {
    return Status::kSuccess;
  }
  initialized_ = true;

  QueryMemoryGrant grant;
  if (pre_acquired_grant_.has_value()) {
    grant = std::move(*pre_acquired_grant_);
    pre_acquired_grant_.reset();
  } else if (graph_.estimated_grant_bytes > 0) {
    ASSIGN_OR_RETURN(QueryMemoryGrant, acquired,
                     QueryMemoryCoordinator::Global().AcquireGrant(
                         graph_.estimated_grant_bytes, graph_.min_grant_bytes));
    grant = std::move(acquired);
  }

  const size_t tmp_op_budget =
      ProcessMemoryBudget::Global().OpMemoryOverrideBytes() > 0
          ? ProcessMemoryBudget::Global().OpMemoryOverrideBytes()
          : (grant.Unlimited() ? (size_t{64} << 20)
                               : grant.OperatorBudgetBytes());
  const size_t min_dop_bytes =
      std::max<size_t>(size_t{64} << 10, graph_.min_grant_bytes);
  const size_t dop = QueryMemoryGrant::ComputeEffectiveDop(
      graph_.requested_dop, tmp_op_budget, min_dop_bytes);

  ctx_ = std::make_unique<PipelineContext>(txn_, std::move(grant), dop);
  OocQueryScope ooc_scope(&ctx_->ooc_metrics);

  if (graph_.stages.empty()) {
    root_exhausted_ = true;
    return Status::kSuccess;
  }

  const auto ensure_init = [&]() {
    if (graph_.stages.size() > stage_source_dumps_.size()) {
      stage_source_dumps_.resize(graph_.stages.size());
    }
  };
  ensure_init();
  for (size_t i = 0; i + 1 < graph_.stages.size(); ++i) {
    RETURN_IF_FAIL(ExecuteBlockingStage(&graph_.stages[i]));
  }

  PipelineStage& root = graph_.stages.back();
  if (root.sink != nullptr) {
    auto binding = std::make_shared<DeferredPipelineSource>("RootSinkOutput");
    root.output_binding = binding;
    RETURN_IF_FAIL(ExecuteBlockingStage(&root));
    root_source_ = binding->Bound();
    root_ops_.clear();
  } else {
    root_source_ = root.source;
    root_ops_ = root.operators;
  }

  if (root_source_) {
    RETURN_IF_FAIL(root_source_->Open(ctx_.get()));
  } else {
    root_exhausted_ = true;
  }
  for (const auto& op : root_ops_) {
    RETURN_IF_FAIL(op->Open(ctx_.get()));
  }
  completed_metrics_ = ctx_->Snapshot();
  return Status::kSuccess;
}

size_t PipelineExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0 || GetStatus() != Status::kSuccess) {
    return 0;
  }
  if (!initialized_) {
    const Status st = Initialize();
    if (st != Status::kSuccess) {
      if (ctx_ != nullptr) {
        completed_metrics_ = ctx_->Snapshot();
      }
      FailWith(st);
      return 0;
    }
  }
  if (root_exhausted_ || !root_source_) {
    return 0;
  }

  OocQueryScope ooc_scope(&ctx_->ooc_metrics);
  DataChunk batch;
  while (destination->Empty() && !root_exhausted_) {
    batch.Reset();
    auto next_or = root_source_->NextBatch(ctx_.get(), &batch, max_rows);
    if (!next_or.HasValue()) {
      completed_metrics_ = ctx_->Snapshot();
      FailWith(next_or.GetStatus());
      return 0;
    }
    if (!next_or.Value()) {
      root_exhausted_ = true;
      break;
    }
    bool exhausted = false;
    const Status op_st = ApplyOperators(root_ops_, &batch, &exhausted);
    if (op_st != Status::kSuccess) {
      completed_metrics_ = ctx_->Snapshot();
      FailWith(op_st);
      return 0;
    }
    if (exhausted) {
      root_exhausted_ = true;
    }
    if (!batch.Empty()) {
      // Swap keeps `batch`'s (now destination's old) storage for reuse by the
      // next `root_source_->NextBatch` instead of moving it into a husk.
      std::swap(*destination, batch);
    }
  }
  completed_metrics_ = ctx_->Snapshot();
  return destination->Size();
}

bool PipelineExecutor::AttachRuntimeFilter(
    const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
    ValueType type) {
  if (root_source_ && root_source_->AttachRuntimeFilter(group, column, type)) {
    return true;
  }
  return std::ranges::any_of(graph_.stages, [&](PipelineStage& stage) {
    return stage.source &&
           stage.source->AttachRuntimeFilter(group, column, type);
  });
}

std::string PipelineExecutor::ExplainPipeline() const {
  const PipelineExecutionMetrics m = Metrics();
  std::ostringstream oss;
  for (const std::string& line : stage_descriptions_) {
    oss << line << "\n";
  }
  oss << "Pipeline: stages=" << stage_descriptions_.size()
      << " dop=" << m.effective_dop << " granted=" << m.granted_bytes
      << "B op_budget=" << m.operator_budget_bytes
      << "B grant_wait_ms=" << m.grant_wait_ms
      << " legacy_source_ms=" << m.legacy_source_ms;
  return oss.str();
}

PipelineExecutionMetrics PipelineExecutor::Metrics() const {
  if (ctx_ != nullptr) {
    return ctx_->Snapshot();
  }
  return completed_metrics_;
}

void PipelineExecutor::RenderStageTree(size_t stage_idx, std::ostream& out,
                                       int indent, bool is_dump) const {
  if (stage_idx >= graph_.stages.size()) {
    return;
  }
  auto pad = [](int n) {
    return std::string(static_cast<size_t>(std::max(0, n)), ' ');
  };
  if (is_dump && stage_idx < stage_source_dumps_.size() &&
      !stage_source_dumps_[stage_idx].empty()) {
    const std::string& text = stage_source_dumps_[stage_idx];
    size_t pos = 0;
    while (pos < text.size()) {
      const size_t nl = text.find('\n', pos);
      const size_t len =
          (nl == std::string::npos) ? (text.size() - pos) : (nl - pos);
      out << pad(indent) << std::string_view(text.data() + pos, len) << '\n';
      if (nl != std::string::npos) {
        pos = nl + 1;
      } else {
        break;
      }
    }
    return;
  }
  const PipelineStage& stage = graph_.stages[stage_idx];
  int cur_indent = indent;
  if (stage.sink != nullptr) {
    if (is_dump) {
      stage.sink->DumpExplain(out, cur_indent);
    } else {
      stage.sink->Explain(out, cur_indent);
    }
    cur_indent += 2;
  }
  for (size_t i = stage.operators.size(); i > 0; --i) {
    if (is_dump) {
      stage.operators[i - 1]->DumpExplain(out, cur_indent);
    } else {
      stage.operators[i - 1]->Explain(out, cur_indent);
    }
    cur_indent += 2;
  }
  const std::shared_ptr<PipelineSource>& src =
      (stage_idx + 1 == graph_.stages.size() && root_source_) ? root_source_
                                                              : stage.source;
  if (const auto* deferred =
          dynamic_cast<const DeferredPipelineSource*>(src.get())) {
    if (deferred->UpstreamStageId().has_value()) {
      RenderStageTree(*deferred->UpstreamStageId(), out, cur_indent, is_dump);
      return;
    }
    if (is_dump) {
      deferred->DumpExplain(out, cur_indent);
    } else {
      deferred->Explain(out, cur_indent);
    }
    return;
  }
  if (src != nullptr) {
    std::ostringstream src_oss;
    if (is_dump) {
      src->DumpExplain(src_oss, 0);
    } else {
      src->Explain(src_oss, 0);
    }
    const std::string text = src_oss.str();
    size_t pos = 0;
    while (pos < text.size()) {
      const size_t nl = text.find('\n', pos);
      const size_t len =
          (nl == std::string::npos) ? (text.size() - pos) : (nl - pos);
      out << pad(cur_indent) << std::string_view(text.data() + pos, len);
      out << '\n';
      if (nl != std::string::npos) {
        pos = nl + 1;
      } else {
        break;
      }
    }
    return;
  }
  if (stage_idx < stage_source_dumps_.size() &&
      !stage_source_dumps_[stage_idx].empty()) {
    const std::string& text = stage_source_dumps_[stage_idx];
    size_t pos = 0;
    while (pos < text.size()) {
      const size_t nl = text.find('\n', pos);
      const size_t len =
          (nl == std::string::npos) ? (text.size() - pos) : (nl - pos);
      out << pad(cur_indent) << std::string_view(text.data() + pos, len)
          << '\n';
      if (nl != std::string::npos) {
        pos = nl + 1;
      } else {
        break;
      }
    }
  }
}

void PipelineExecutor::Dump(std::ostream& output, int indent) const {
  ScopedRenderExecutor render_guard(this);
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  if (!graph_.plan_explain_text.empty()) {
    output << graph_.plan_explain_text;
    if (graph_.plan_explain_text.back() != '\n') {
      output << '\n';
    }
  } else if (!graph_.stages.empty()) {
    std::ostringstream tree;
    RenderStageTree(graph_.stages.size() - 1, tree, indent, /*is_dump=*/true);
    output << tree.str();
    if (!tree.str().empty() && tree.str().back() != '\n') {
      output << '\n';
    }
  }
  const PipelineExecutionMetrics m = Metrics();
  output << pad << "Pipeline: " << pipeline_summary_cache_
         << " dop=" << m.effective_dop << " granted=" << m.granted_bytes
         << "B op_budget=" << m.operator_budget_bytes
         << "B spill_bytes=" << m.ooc.spill_bytes
         << "B grant_wait_ms=" << m.grant_wait_ms
         << " legacy_source_ms=" << m.legacy_source_ms;
}

void PipelineExecutor::Explain(std::ostream& output, int indent) const {
  ScopedRenderExecutor render_guard(this);
  if (!graph_.plan_explain_text.empty()) {
    output << graph_.plan_explain_text;
    return;
  }
  if (!graph_.stages.empty()) {
    RenderStageTree(graph_.stages.size() - 1, output, indent,
                    /*is_dump=*/false);
    return;
  }
  Dump(output, indent);
}

}  // namespace tinylamb
