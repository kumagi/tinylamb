/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/pipeline_lowering.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/full_scan.hpp"
#include "executor/index_only_scan.hpp"
#include "executor/index_scan.hpp"
#include "executor/late_materialization_join.hpp"
#include "executor/lookup_join.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/projection.hpp"
#include "executor/selection.hpp"
#include "executor/values.hpp"
#include "executor/zigzag_join.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/apply_plan.hpp"
#include "plan/band_join_plan.hpp"
#include "plan/bitmap_scan_plan.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/empty_plan.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/incremental_sort_plan.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/index_skip_scan_plan.hpp"
#include "plan/late_materialization_join_plan.hpp"
#include "plan/limit_plan.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/minmax_index_plan.hpp"
#include "plan/multi_index_covering_scan_plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/recursive_cte_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/set_operation_plan.hpp"
#include "plan/skip_scan_distinct_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/topn_plan.hpp"
#include "plan/values_plan.hpp"
#include "plan/window_plan.hpp"
#include "plan/zigzag_join_plan.hpp"

namespace tinylamb {
namespace {

bool IsStreamingLeafPlan(const PlanBase* base) noexcept {
  return dynamic_cast<const FullScanPlan*>(base) != nullptr ||
         dynamic_cast<const IndexScanPlan*>(base) != nullptr ||
         dynamic_cast<const IndexOnlyScanPlan*>(base) != nullptr ||
         dynamic_cast<const ValuesPlan*>(base) != nullptr ||
         dynamic_cast<const EmptyPlan*>(base) != nullptr ||
         dynamic_cast<const DummyScanPlan*>(base) != nullptr ||
         dynamic_cast<const SkipScanDistinctPlan*>(base) != nullptr;
}

bool SupportsScanFilterOrigin(const PlanBase* base) noexcept {
  return dynamic_cast<const FullScanPlan*>(base) != nullptr ||
         dynamic_cast<const IndexScanPlan*>(base) != nullptr ||
         dynamic_cast<const IndexOnlyScanPlan*>(base) != nullptr;
}

bool ResolveKeyOffsets(const Schema& schema,
                       const std::vector<ColumnName>& cols,
                       std::vector<slot_t>* out) {
  out->clear();
  out->reserve(cols.size());
  for (const ColumnName& col : cols) {
    const int off = schema.Offset(col);
    if (off < 0 || std::cmp_greater_equal(off, schema.ColumnCount())) {
      return false;
    }
    out->push_back(static_cast<slot_t>(off));
  }
  return true;
}

void AttachJoinDynamicFilter(const ProductPlan& prod,
                             const std::vector<slot_t>& probe_cols,
                             const LoweredStream& probe_stream,
                             OocHashJoinSpec* spec) {
  if (!prod.PrefersDynamicFilter() || probe_cols.size() != 1 ||
      !probe_stream.scan_origin.has_value()) {
    return;
  }
  if (std::ranges::any_of(spec->key_null_safe, [](bool s) { return s; })) {
    return;
  }
  const bool drops_unmatched_probe =
      spec->kind == JoinKind::kInner || spec->kind == JoinKind::kSemi ||
      (spec->kind == JoinKind::kRightOuter && !spec->build_left) ||
      (spec->kind == JoinKind::kLeftOuter && spec->build_left);
  if (!drops_unmatched_probe) {
    return;
  }

  const slot_t pslot = probe_cols[0];
  const LoweredStream::ScanOrigin& origin = *probe_stream.scan_origin;
  if (pslot >= origin.output_to_leaf_col.size()) {
    return;
  }
  const ColumnName& leaf_col = origin.output_to_leaf_col[pslot];
  if (leaf_col.name.empty()) {
    return;
  }
  const int leaf_off = origin.leaf_schema.Offset(leaf_col);
  if (leaf_off < 0 ||
      std::cmp_greater_equal(leaf_off, origin.leaf_schema.ColumnCount())) {
    return;
  }
  const Column& col_def =
      origin.leaf_schema.GetColumn(static_cast<size_t>(leaf_off));
  if (col_def.IsUnsigned() || (col_def.Type() != ValueType::kInt64 &&
                               col_def.Type() != ValueType::kDate)) {
    return;
  }
  spec->runtime_filter_probe_target = origin.leaf_source;
  spec->runtime_filter_probe_col = static_cast<slot_t>(leaf_off);
  spec->runtime_filter_probe_type = col_def.Type();
}

size_t EstimatePlanMemoryBytes(const Plan& plan) noexcept {
  if (!plan) {
    return 0;
  }
  const size_t rows = plan->EmitRowCount();
  const size_t cols = std::max<size_t>(1, plan->GetSchema().ColumnCount());
  constexpr size_t kEstCellBytes = 24;
  if (rows > std::numeric_limits<size_t>::max() / (cols * kEstCellBytes)) {
    // An overflowing estimate wants everything the grant ceiling allows.
    return std::numeric_limits<size_t>::max();
  }
  return rows * cols * kEstCellBytes;
}

bool ChildHasAnyOrder(const Plan& child) {
  if (!child) {
    return false;
  }
  const Schema& schema = child->GetSchema();
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    Expression col = ColumnValueExp(schema.GetColumn(i).Name());
    if (child->IsOrderedBy({col}, {true}) ||
        child->IsOrderedBy({col}, {false})) {
      return true;
    }
  }
  return false;
}

const Index* ZigZagIndexOf(const Plan& plan) {
  if (const auto* scan = dynamic_cast<const IndexScanPlan*>(plan.get())) {
    return &scan->GetIndex();
  }
  if (const auto* scan = dynamic_cast<const IndexOnlyScanPlan*>(plan.get())) {
    return &scan->GetIndex();
  }
  if (const auto* scan = dynamic_cast<const IndexSkipScanPlan*>(plan.get())) {
    return ZigZagIndexOf(scan->GetSource());
  }
  return nullptr;
}

class PipelineStreamExecutor final : public ExecutorBase {
 public:
  PipelineStreamExecutor(PipelineContext* pctx,
                         std::shared_ptr<PipelineSource> source,
                         std::vector<std::shared_ptr<PipelineOperator>> ops,
                         std::string explain_name)
      : pctx_(pctx),
        source_(std::move(source)),
        ops_(std::move(ops)),
        explain_name_(std::move(explain_name)) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    if (GetStatus() != Status::kSuccess || eof_) {
      return 0;
    }
    if (!opened_) {
      opened_ = true;
      if (source_ != nullptr) {
        Status st = source_->Open(pctx_);
        if (st != Status::kSuccess) {
          FailWith(st);
          return 0;
        }
      }
      for (const auto& op : ops_) {
        Status st = op->Open(pctx_);
        if (st != Status::kSuccess) {
          FailWith(st);
          return 0;
        }
      }
    }
    while (true) {
      if (pctx_ != nullptr && pctx_->txn != nullptr &&
          pctx_->txn->txn_.HasStatementTimeout()) {
        const Status ts = pctx_->txn->txn_.CheckStatementTimeout();
        if (ts != Status::kSuccess) {
          FailWith(ts);
          return 0;
        }
      }
      if (source_ == nullptr) {
        eof_ = true;
        return 0;
      }
      cur_.Reset();
      StatusOr<bool> got = source_->NextBatch(pctx_, &cur_, max_rows);
      if (!got.HasValue()) {
        FailWith(got.GetStatus());
        return 0;
      }
      if (!got.Value() || cur_.Empty()) {
        eof_ = true;
        return 0;
      }
      bool exhausted = false;
      for (const auto& op : ops_) {
        if (cur_.Empty()) {
          break;
        }
        next_.Reset();
        Status ost = op->Process(pctx_, cur_, &next_, &exhausted);
        if (ost != Status::kSuccess) {
          FailWith(ost);
          return 0;
        }
        // Swap rather than move: `next_` keeps the consumed chunk's storage
        // so each operator's output reuses allocated columns instead of
        // re-allocating them every batch (`DataChunk::Reset` reuse path).
        std::swap(cur_, next_);
        if (exhausted) {
          eof_ = true;
          break;
        }
      }
      if (!cur_.Empty()) {
        std::swap(*destination, cur_);
        return destination->Size();
      }
      if (eof_) {
        return 0;
      }
    }
  }

  void Dump(std::ostream& output, int indent) const override {
    for (const auto& op : std::views::reverse(ops_)) {
      op->DumpExplain(output, indent);
    }
    if (source_ != nullptr) {
      source_->DumpExplain(output, indent);
    } else {
      output << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
             << explain_name_ << "\n";
    }
  }

 private:
  PipelineContext* pctx_{nullptr};
  std::shared_ptr<PipelineSource> source_;
  std::vector<std::shared_ptr<PipelineOperator>> ops_;
  std::string explain_name_;
  DataChunk cur_;
  DataChunk next_;
  bool opened_{false};
  bool eof_{false};
};

class LazyAdaptedPipelineSource final : public PipelineSource {
 public:
  LazyAdaptedPipelineSource(
      std::function<StatusOr<Executor>(PipelineContext*)> factory,
      std::string label)
      : factory_(std::move(factory)), label_(std::move(label)) {}

  Status Open(PipelineContext* ctx) override {
    if (!exec_) {
      ASSIGN_OR_RETURN(Executor, built, factory_(ctx));
      exec_ = std::move(built);
    }
    return Status::kSuccess;
  }

  StatusOr<bool> NextBatch(PipelineContext* /*ctx*/, DataChunk* out,
                           size_t max_rows) override {
    if (!exec_) {
      return false;
    }
    const size_t n = exec_->NextBatch(out, max_rows);
    if (exec_->GetStatus() != Status::kSuccess) {
      return exec_->GetStatus();
    }
    return n > 0;
  }

  void DumpExplain(std::ostream& out, int indent) const override {
    if (exec_ != nullptr) {
      exec_->Dump(out, indent);
      out << "\n";
    } else {
      out << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
          << label_ << "\n";
    }
  }

  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  std::function<StatusOr<Executor>(PipelineContext*)> factory_;
  std::string label_;
  Executor exec_;
};

}  // namespace

Status PipelineLowering::BreakWithSink(PipelineGraph* graph,
                                       LoweredStream* stream,
                                       std::shared_ptr<PipelineSink> sink,
                                       const std::string& stage_name,
                                       const Schema& out_schema,
                                       bool preserve_scan_origin) {
  auto binding =
      std::make_shared<DeferredPipelineSource>(stage_name + "Output");
  const size_t stage_id = graph->stages.size();
  binding->SetUpstreamStageId(stage_id);
  graph->stages.push_back(PipelineStage{
      .id = stage_id,
      .name = stage_name,
      .source = std::move(stream->source),
      .operators = std::move(stream->operators),
      .sink = std::move(sink),
      .output_binding = binding,
  });
  stream->source = std::move(binding);
  stream->operators.clear();
  stream->schema = out_schema;
  if (!preserve_scan_origin) {
    stream->scan_origin.reset();
  }
  return Status::kSuccess;
}

Status PipelineLowering::LowerNode(const Plan& plan, TransactionContext& ctx,
                                   const CustomPlanLowerer& custom_hook,
                                   PipelineGraph* graph, LoweredStream* out) {
  if (!plan) {
    out->source = std::make_shared<EmptyPipelineSource>();
    out->operators.clear();
    out->schema = Schema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  if (custom_hook) {
    ASSIGN_OR_RETURN(bool, handled, custom_hook(plan, ctx, graph, out));
    if (handled) {
      return Status::kSuccess;
    }
  }

  const PlanBase* base = plan.get();

  // 1. Streaming leaf sources (`FullScan`, `IndexScan`, `IndexOnlyScan`,
  //    `Values`, `Empty`, `DummyScan`, `SkipScanDistinct`).
  if (IsStreamingLeafPlan(base)) {
    Executor leaf_exec = plan->EmitExecutor(ctx);
    auto src = std::make_shared<StreamingExecutorSource>(std::move(leaf_exec),
                                                         plan->ToString());
    out->source = src;
    out->operators.clear();
    out->schema = plan->GetSchema();
    out->scan_origin.reset();
    if (SupportsScanFilterOrigin(base)) {
      LoweredStream::ScanOrigin origin;
      origin.leaf_source = std::move(src);
      origin.leaf_schema = out->schema;
      origin.output_to_leaf_col.reserve(out->schema.ColumnCount());
      for (size_t i = 0; i < out->schema.ColumnCount(); ++i) {
        origin.output_to_leaf_col.push_back(out->schema.GetColumn(i).Name());
      }
      out->scan_origin = std::move(origin);
    }
    return Status::kSuccess;
  }

  // 2. Layout-transparent relation alias rename.
  if (const auto* rename = dynamic_cast<const RelationRenamePlan*>(base)) {
    RETURN_IF_FAIL(
        LowerNode(rename->GetSource(), ctx, custom_hook, graph, out));
    if (!rename->RelationName().empty() || !rename->PhysicalName().empty()) {
      out->operators.push_back(std::make_shared<RenameOp>(
          rename->PhysicalName(), rename->RelationName()));
    }
    out->schema = rename->GetSchema();
    return Status::kSuccess;
  }

  // 3. Filter (`SelectionPlan`).
  if (const auto* sel = dynamic_cast<const SelectionPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(sel->Src(), ctx, custom_hook, graph, out));
    bool enforced = sel->ChildAppliesPredicate();
    if (!enforced && out->operators.empty()) {
      if (const auto* ses =
              dynamic_cast<const StreamingExecutorSource*>(out->source.get())) {
        if (const auto* idx_scan =
                dynamic_cast<const IndexScan*>(ses->Underlying().get())) {
          enforced = idx_scan->EnforcesIntegerPredicate(sel->Predicate());
        } else if (auto* pscan =
                       dynamic_cast<ParallelScan*>(ses->Underlying().get())) {
          if (!relational_detail::ContainsQuery(sel->Predicate()) &&
              BytecodeEnabled()) {
            std::vector<Expression> conjuncts =
                SplitConjuncts(sel->Predicate());
            auto filter =
                std::make_unique<relational_detail::CompiledScanFilter>(
                    relational_detail::CompileScanFilter(
                        conjuncts, sel->Src()->GetSchema()));
            auto programs =
                std::make_unique<relational_detail::WorkerScanFilterPrograms>(
                    relational_detail::CompileWorkerFilter(
                        *filter, sel->Src()->GetSchema()));
            if (programs->complete) {
              pscan->SetRowFilter(std::move(conjuncts), std::move(*filter),
                                  std::move(*programs));
              enforced = true;
            }
          }
        }
      }
    }
    if (!enforced) {
      out->operators.push_back(std::make_shared<FilterOp>(
          sel->Predicate(), sel->Src()->GetSchema()));
    }
    out->schema = sel->GetSchema();
    return Status::kSuccess;
  }

  // 4. Projection (`ProjectionPlan`).
  if (const auto* proj = dynamic_cast<const ProjectionPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(proj->GetSource(), ctx, custom_hook, graph, out));
    if (out->scan_origin.has_value()) {
      LoweredStream::ScanOrigin next_origin = std::move(*out->scan_origin);
      std::vector<ColumnName> remapped(proj->Columns().size());
      const Schema& in_schema = proj->GetSource()->GetSchema();
      for (size_t i = 0; i < proj->Columns().size(); ++i) {
        const Expression& e = proj->Columns()[i].expression;
        if (e && e->Type() == TypeTag::kColumnValue) {
          const int off = in_schema.Offset(e->AsColumnValue().GetColumnName());
          if (off >= 0 && std::cmp_less(off, in_schema.ColumnCount()) &&
              static_cast<size_t>(off) <
                  next_origin.output_to_leaf_col.size()) {
            remapped[i] =
                next_origin.output_to_leaf_col[static_cast<size_t>(off)];
          }
        }
      }
      next_origin.output_to_leaf_col = std::move(remapped);
      out->scan_origin = std::move(next_origin);
    }
    out->operators.push_back(std::make_shared<ProjectOp>(
        proj->Columns(), proj->GetSource()->GetSchema()));
    out->schema = proj->GetSchema();
    return Status::kSuccess;
  }

  // 5. Streaming LIMIT / OFFSET (`LimitPlan`).
  if (const auto* lim = dynamic_cast<const LimitPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(lim->Src(), ctx, custom_hook, graph, out));
    // LimitCount() == 0 is the unbounded sentinel used for OFFSET-only plans.
    const bool has_limit = (lim->LimitCount() > 0);
    out->operators.push_back(std::make_shared<LimitOp>(
        lim->LimitCount(), lim->LimitOffset(), has_limit));
    out->schema = lim->GetSchema();
    return Status::kSuccess;
  }

  // 6. Ordered adjacent duplicate elimination (`SortDistinctPlan`).
  if (const auto* sort_dist = dynamic_cast<const SortDistinctPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(sort_dist->Child(), ctx, custom_hook, graph, out));
    out->operators.push_back(std::make_shared<AdjacentDistinctOp>());
    out->schema = sort_dist->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  // 7. Sort (`SortPlan`, `BufferedSortPlan`, `ExternalMergeSortPlan`).
  if (const auto* sort = dynamic_cast<const SortPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(sort->Child(), ctx, custom_hook, graph, out));
    std::vector<OocSortSink::KeySpec> keys;
    keys.reserve(sort->Keys().size());
    for (const SortKey& k : sort->Keys()) {
      keys.push_back(OocSortSink::KeySpec{
          .expression = k.expression,
          .ascending = k.ascending,
          .nulls_first = k.nulls_first,
          .collation = k.collation,
      });
    }
    auto sink = std::make_shared<OocSortSink>(
        std::move(keys), sort->Child()->GetSchema(), std::nullopt, 0,
        sort->StrategyName());
    return BreakWithSink(graph, out, std::move(sink), sort->StrategyName(),
                         sort->GetSchema());
  }

  // 8. Bounded Top-N (`TopNPlan`). `WITH TIES` stays on LegacySource in P3a.
  if (const auto* topn = dynamic_cast<const TopNPlan*>(base)) {
    if (!topn->WithTies()) {
      RETURN_IF_FAIL(LowerNode(topn->Child(), ctx, custom_hook, graph, out));
      std::vector<OocSortSink::KeySpec> keys;
      keys.reserve(topn->Keys().size());
      for (const TopNKey& k : topn->Keys()) {
        keys.push_back(OocSortSink::KeySpec{
            .expression = k.expression,
            .ascending = k.ascending,
            .nulls_first = k.nulls_first,
            .collation = k.collation,
        });
      }
      auto sink = std::make_shared<OocSortSink>(
          std::move(keys), topn->Child()->GetSchema(), topn->Limit(),
          topn->Offset(), "TopN");
      return BreakWithSink(graph, out, std::move(sink), "TopN",
                           topn->GetSchema());
    }
  }

  // 9. Hash Distinct (`DistinctPlan`, including `DISTINCT ON`).
  if (const auto* dist = dynamic_cast<const DistinctPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(dist->Child(), ctx, custom_hook, graph, out));
    // DistinctPlan advertises its child's ordering (the optimizer then omits
    // the Sort enforcer, e.g. HashDistinct over an ordered IndexOnlyScan for
    // `SELECT DISTINCT a ... ORDER BY a`).  The in-memory path keeps first-
    // seen order for free, but a spilled distinct partitions by hash, so it
    // must carry the input sequence whenever the child is ordered -- not only
    // for DISTINCT ON (sql_oracle_fuzzer under TINYLAMB_ALWAYS_SPILL returned
    // [-2];[NULL];[0]... instead of [NULL];[-2];[0]...).
    const bool preserve_order = ChildHasAnyOrder(dist->Child());
    auto sink = std::make_shared<OocDistinctSink>(
        dist->Child()->GetSchema(), std::vector<slot_t>{}, "Distinct",
        dist->DistinctOn(), preserve_order);
    return BreakWithSink(graph, out, std::move(sink), "Distinct",
                         dist->GetSchema());
  }

  // 10. Aggregation (`AggregationPlan`, `HashAggregatePlan`,
  //     `SortAggregatePlan`, `StreamAggregatePlan`), including
  //     NeedsGroupContext (`ARRAY_AGG`, `STRING_AGG`, inner `ORDER BY`,
  //     `FILTER`, `HAVING MAX/MIN`, `PERCENTILE_*`).
  if (const auto* agg = dynamic_cast<const AggregationPlan*>(base)) {
    bool supported = true;
    std::vector<Expression> agg_owners;
    agg_owners.reserve(agg->Aggregates().size());
    for (const NamedExpression& ne : agg->Aggregates()) {
      if (!ne.expression || ne.expression->Type() != TypeTag::kAggregateExp) {
        supported = false;
        break;
      }
      const AggregateExpression& ae = ne.expression->AsAggregateExpression();
      if (ae.HasInnerGroupBy()) {
        supported = false;
        break;
      }
      agg_owners.push_back(ne.expression);
    }

    if (supported) {
      RETURN_IF_FAIL(LowerNode(agg->Child(), ctx, custom_hook, graph, out));
      if (agg->GroupKeys().empty() && agg->Aggregates().size() == 1 &&
          agg->Child()->GetSchema().ColumnCount() == 1 &&
          out->operators.empty()) {
        const auto& ae =
            agg->Aggregates()[0].expression->AsAggregateExpression();
        if (ae.GetType() == AggregationType::kCount && ae.Distinct() &&
            ae.Child() && ae.Child()->Type() == TypeTag::kColumnValue &&
            agg->Child()->GetSchema().Offset(
                ae.Child()->AsColumnValue().GetColumnName()) == 0) {
          if (const auto* ses = dynamic_cast<const StreamingExecutorSource*>(
                  out->source.get())) {
            if (auto* covering =
                    dynamic_cast<IndexOnlyScan*>(ses->Underlying().get())) {
              (void)covering->TryEnableKeyDeduplication();
            }
          }
        }
      }
      std::vector<Expression> group_key_exprs;
      group_key_exprs.reserve(agg->GroupKeys().size());
      for (const NamedExpression& gk : agg->GroupKeys()) {
        group_key_exprs.push_back(gk.expression);
      }

      const Schema agg_schema = agg->GetSchema();
      auto sink = std::make_shared<OocGroupAggSink>(
          agg->Child()->GetSchema(), group_key_exprs, std::move(agg_owners),
          agg_schema, agg->StrategyName());
      sink->SetExplainHeader(agg->ToString());
      RETURN_IF_FAIL(BreakWithSink(graph, out, std::move(sink),
                                   agg->StrategyName(), agg_schema));

      // `SortAggregatePlan` and `StreamAggregatePlan` promise output ordered
      // by `GroupKeys()` ascending NULLs-first when `GroupKeys()` is non-empty.
      // Preserve that contract cleanly with an OOC sort pass so downstream
      // parents relying on `IsOrderedBy()` keep sorted rows.
      if (!agg->GroupKeys().empty() &&
          agg->Strategy() != AggregationStrategy::kHash) {
        std::vector<OocSortSink::KeySpec> order_keys;
        order_keys.reserve(agg->GroupKeys().size());
        for (size_t i = 0; i < agg->GroupKeys().size(); ++i) {
          order_keys.push_back(OocSortSink::KeySpec{
              .expression = ColumnValueExp(agg_schema.GetColumn(i).Name()),
              .ascending = true,
              .nulls_first = true,
          });
        }
        auto sort_sink =
            std::make_shared<OocSortSink>(std::move(order_keys), agg_schema,
                                          std::nullopt, 0, "AggregateOrder");
        RETURN_IF_FAIL(BreakWithSink(graph, out, std::move(sort_sink),
                                     "AggregateOrder", agg_schema));
      }

      if (agg->Having()) {
        out->operators.push_back(
            std::make_shared<FilterOp>(agg->Having(), agg_schema));
      }
      out->schema = agg_schema;
      return Status::kSuccess;
    }
  }

  // 11. Equi Hash Join (`ProductPlan` with non-empty join keys and non-lookup
  //     right plan). Cross/BNL/Lookup falls back cleanly to `LegacySource` in
  //     P3a until P3b.
  if (const auto* prod = dynamic_cast<const ProductPlan*>(base)) {
    std::vector<slot_t> left_slots;
    std::vector<slot_t> right_slots;
    const bool is_equi_hash =
        prod->Right() != nullptr && !prod->Lookup().has_value() &&
        !prod->LeftKeys().empty() && !prod->RightKeys().empty() &&
        ResolveKeyOffsets(prod->Left()->GetSchema(), prod->LeftKeys(),
                          &left_slots) &&
        ResolveKeyOffsets(prod->Right()->GetSchema(), prod->RightKeys(),
                          &right_slots);
    if (is_equi_hash) {
      OocHashJoinSpec spec;
      spec.left_cols = std::move(left_slots);
      spec.right_cols = std::move(right_slots);
      spec.left_schema = prod->Left()->GetSchema();
      spec.right_schema = prod->Right()->GetSchema();
      spec.kind = prod->Kind();
      spec.key_null_safe = prod->KeyNullSafe();
      spec.semi_reduction = prod->PrefersSemiReduction();
      const bool any_null_safe =
          std::ranges::any_of(spec.key_null_safe, [](bool s) { return s; });
      spec.dynamic_filter = prod->PrefersDynamicFilter() &&
                            spec.kind == JoinKind::kInner && !any_null_safe &&
                            spec.left_cols.size() == 1 &&
                            spec.right_cols.size() == 1;
      if (prod->ResidualNote() && (spec.kind == JoinKind::kLeftOuter ||
                                   spec.kind == JoinKind::kRightOuter ||
                                   spec.kind == JoinKind::kFullOuter ||
                                   spec.kind == JoinKind::kInner)) {
        spec.residual_predicate = prod->ResidualNote();
        spec.residual_schema = spec.left_schema + spec.right_schema;
      }

      if (spec.kind == JoinKind::kSemi || spec.kind == JoinKind::kAnti ||
          spec.kind == JoinKind::kNullAwareAnti) {
        spec.build_left = false;
      } else if (prod->PrefersDynamicFilter()) {
        spec.build_left = prod->DynamicFilterBuildsLeft();
      } else {
        const size_t left_est =
            std::max<size_t>(1, prod->Left()->EmitRowCount()) *
            std::max<size_t>(1, prod->Left()->GetSchema().ColumnCount());
        const size_t right_est =
            std::max<size_t>(1, prod->Right()->EmitRowCount()) *
            std::max<size_t>(1, prod->Right()->GetSchema().ColumnCount());
        spec.build_left = left_est < right_est;
      }

      const Plan& build_plan = spec.build_left ? prod->Left() : prod->Right();
      const Plan& probe_plan = spec.build_left ? prod->Right() : prod->Left();
      const std::vector<slot_t>& probe_cols =
          spec.build_left ? spec.right_cols : spec.left_cols;

      LoweredStream build_stream;
      RETURN_IF_FAIL(
          LowerNode(build_plan, ctx, custom_hook, graph, &build_stream));

      LoweredStream probe_stream;
      RETURN_IF_FAIL(
          LowerNode(probe_plan, ctx, custom_hook, graph, &probe_stream));

      AttachJoinDynamicFilter(*prod, probe_cols, probe_stream, &spec);

      const bool use_semi_reduction =
          spec.semi_reduction &&
          (spec.kind == JoinKind::kSemi || spec.kind == JoinKind::kAnti ||
           spec.kind == JoinKind::kNullAwareAnti);
      auto join_ctx = std::make_shared<OocHashJoinContext>(std::move(spec));
      join_ctx->build_source_weak = build_stream.source;
      join_ctx->build_ops_explain = build_stream.operators;

      if (use_semi_reduction) {
        join_ctx->probe_source_weak = probe_stream.source;
        join_ctx->probe_ops_explain = probe_stream.operators;
        auto semi_sink = std::make_shared<OocSemiProbeIntakeSink>(
            join_ctx, "SemiProbeIntake");
        const size_t semi_stage_id = graph->stages.size();
        graph->stages.push_back(PipelineStage{
            .id = semi_stage_id,
            .name = semi_sink->Describe(),
            .source = std::move(probe_stream.source),
            .operators = std::move(probe_stream.operators),
            .sink = std::move(semi_sink),
            .output_binding = nullptr,
        });
      }

      auto build_sink = std::make_shared<OocHashBuildSink>(
          join_ctx, join_ctx->spec.build_left ? "HashBuildLeft" : "HashBuild");
      const size_t build_stage_id = graph->stages.size();
      graph->stages.push_back(PipelineStage{
          .id = build_stage_id,
          .name = build_sink->Describe(),
          .source = std::move(build_stream.source),
          .operators = std::move(build_stream.operators),
          .sink = std::move(build_sink),
          .output_binding = nullptr,
      });

      if (use_semi_reduction) {
        out->source = std::make_shared<OocAdaptiveHashProbeSource>(
            std::move(join_ctx), nullptr,
            std::vector<std::shared_ptr<PipelineOperator>>{}, "HashJoin");
      } else {
        out->source = std::make_shared<OocAdaptiveHashProbeSource>(
            std::move(join_ctx), std::move(probe_stream.source),
            std::move(probe_stream.operators), "HashJoin");
      }
      out->operators.clear();
      out->schema = prod->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }

    // 11b. Non-equi / Cross Join / Block Nested Loop (`ProductPlan` with empty
    // join keys and no lookup seek).
    if (prod->Right() != nullptr && !prod->Lookup().has_value() &&
        prod->LeftKeys().empty() && prod->RightKeys().empty()) {
      LoweredStream left_stream;
      RETURN_IF_FAIL(
          LowerNode(prod->Left(), ctx, custom_hook, graph, &left_stream));
      LoweredStream right_stream;
      RETURN_IF_FAIL(
          LowerNode(prod->Right(), ctx, custom_hook, graph, &right_stream));

      auto cross_ctx = std::make_shared<OocCrossJoinContext>();
      cross_ctx->kind = prod->Kind();
      cross_ctx->left_width = prod->Left()->GetSchema().ColumnCount();
      cross_ctx->right_width = prod->Right()->GetSchema().ColumnCount();
      cross_ctx->left_schema = prod->Left()->GetSchema();
      cross_ctx->right_schema = prod->Right()->GetSchema();
      cross_ctx->combined_schema =
          cross_ctx->left_schema + cross_ctx->right_schema;
      cross_ctx->output_schema = prod->GetSchema();
      cross_ctx->residual = prod->ResidualNote();
      cross_ctx->residual_conjuncts = SplitConjuncts(cross_ctx->residual);
      cross_ctx->prefer_bnl = prod->PrefersBatchNestedLoop() ||
                              cross_ctx->residual != nullptr ||
                              cross_ctx->kind != JoinKind::kInner;
      cross_ctx->right_source_explain = right_stream.source;
      cross_ctx->right_ops_explain = right_stream.operators;

      auto build_sink = std::make_shared<OocCrossJoinBuildSink>(cross_ctx);
      const size_t build_stage_id = graph->stages.size();
      graph->stages.push_back(PipelineStage{
          .id = build_stage_id,
          .name = build_sink->Describe(),
          .source = std::move(right_stream.source),
          .operators = std::move(right_stream.operators),
          .sink = std::move(build_sink),
          .output_binding = nullptr,
      });

      OocSetOpInputStream left_input{
          .source = std::move(left_stream.source),
          .operators = std::move(left_stream.operators),
          .schema = std::move(left_stream.schema),
      };
      out->source = std::make_shared<OocCrossJoinProbeSource>(
          std::move(cross_ctx), std::move(left_input));
      out->operators.clear();
      out->schema = prod->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }

    // 11c. Index Nested Loop / Lookup (`ProductPlan` with `Lookup()`).
    if (prod->RightTable() != nullptr && prod->RightIndex() != nullptr) {
      std::vector<slot_t> lkeys;
      std::vector<slot_t> rkeys;
      const bool ok_keys = ResolveKeyOffsets(prod->Left()->GetSchema(),
                                             prod->LeftKeys(), &lkeys) &&
                           ResolveKeyOffsets(prod->RightTable()->GetSchema(),
                                             prod->RightKeys(), &rkeys);
      const bool any_null_safe =
          std::ranges::any_of(prod->KeyNullSafe(), [](bool s) { return s; });
      if (ok_keys &&
          !ctx.txn_.IndexKeysMayBeStale(prod->RightIndex()->Root()) &&
          !any_null_safe && prod->Lookup().has_value()) {
        LoweredStream left_stream;
        RETURN_IF_FAIL(
            LowerNode(prod->Left(), ctx, custom_hook, graph, &left_stream));
        const LookupSeek seek = *prod->Lookup();
        std::vector<slot_t> probe_left;
        std::vector<slot_t> probe_right;
        if (ResolveKeyOffsets(prod->Left()->GetSchema(), seek.left_columns,
                              &probe_left) &&
            ResolveKeyOffsets(prod->RightTable()->GetSchema(),
                              seek.right_columns, &probe_right)) {
          std::optional<slot_t> second_range_col;
          if (seek.second_range_left_column.has_value()) {
            const int off = prod->Left()->GetSchema().Offset(
                *seek.second_range_left_column);
            if (off >= 0) {
              second_range_col = static_cast<slot_t>(off);
            }
          }
          auto lsrc = std::move(left_stream.source);
          auto lops = std::move(left_stream.operators);
          const Table* rtbl = prod->RightTable();
          const Index* ridx = prod->RightIndex();
          out->source = std::make_shared<LazyAdaptedPipelineSource>(
              [lsrc = std::move(lsrc), lops = std::move(lops),
               probe_left = std::move(probe_left), rtbl, ridx,
               probe_right = std::move(probe_right),
               range_pos = seek.range_position, range_op = seek.range_operation,
               second_range_col, second_op = seek.second_range_operation](
                  PipelineContext* pctx) -> StatusOr<Executor> {
                auto guard =
                    pctx->txn->txn_.AcquireIndexReadGuard(ridx->Root());
                Executor left_exec = std::make_shared<PipelineStreamExecutor>(
                    pctx, lsrc, lops, "PipelineLeft");
                return Executor(std::make_shared<LookupJoin>(
                    pctx->txn->txn_, std::move(left_exec), probe_left, *rtbl,
                    *ridx, probe_right, range_pos, range_op, second_range_col,
                    second_op, std::move(guard)));
              },
              "LookupJoin");
          out->operators.clear();
          out->schema = prod->GetSchema();
          out->scan_origin.reset();
          return Status::kSuccess;
        }
      }
      if (ok_keys) {
        // Fallback for stale/null-safe/degenerate Lookup: OOC Hash Join over
        // FullScan(RightTable).
        OocHashJoinSpec spec;
        spec.left_cols = std::move(lkeys);
        spec.right_cols = std::move(rkeys);
        spec.left_schema = prod->Left()->GetSchema();
        spec.right_schema = prod->RightTable()->GetSchema();
        spec.kind = prod->Kind();
        spec.key_null_safe = prod->KeyNullSafe();
        spec.residual_predicate = prod->ResidualNote();
        spec.residual_schema = spec.left_schema + spec.right_schema;
        spec.build_left = false;
        LoweredStream probe_stream;
        RETURN_IF_FAIL(
            LowerNode(prod->Left(), ctx, custom_hook, graph, &probe_stream));
        Executor full_right =
            std::make_shared<FullScan>(ctx.txn_, *prod->RightTable());
        auto build_src = std::make_shared<StreamingExecutorSource>(
            std::move(full_right), "FullScan");
        auto join_ctx = std::make_shared<OocHashJoinContext>(std::move(spec));
        auto build_sink =
            std::make_shared<OocHashBuildSink>(join_ctx, "HashBuild");
        const size_t bsid = graph->stages.size();
        graph->stages.push_back(PipelineStage{
            .id = bsid,
            .name = build_sink->Describe(),
            .source = std::move(build_src),
            .operators = {},
            .sink = std::move(build_sink),
            .output_binding = nullptr,
        });
        out->source = std::make_shared<OocAdaptiveHashProbeSource>(
            std::move(join_ctx), std::move(probe_stream.source),
            std::move(probe_stream.operators), "HashJoin");
        out->operators.clear();
        out->schema = prod->GetSchema();
        out->scan_origin.reset();
        return Status::kSuccess;
      }
    }
  }

  // 12. Window functions (`WindowPlan`).
  if (const auto* win = dynamic_cast<const WindowPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(win->GetSource(), ctx, custom_hook, graph, out));
    const bool preserve_order =
        !win->SpillCapable() && ChildHasAnyOrder(win->GetSource());
    auto sink = std::make_shared<OocWindowSink>(
        win->GetSource()->GetSchema(), win->GetSchema(), win->WindowOutputs(),
        preserve_order, win->SpillCapable(), win->Memory().memory_budget_bytes);
    return BreakWithSink(graph, out, std::move(sink), "Window",
                         win->GetSchema());
  }

  // 13. IncrementalSort (`IncrementalSortPlan`).
  if (const auto* inc = dynamic_cast<const IncrementalSortPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(inc->Child(), ctx, custom_hook, graph, out));
    auto to_specs = [](const std::vector<SortKey>& keys) {
      std::vector<OocSortSink::KeySpec> specs;
      specs.reserve(keys.size());
      for (const SortKey& k : keys) {
        specs.push_back(OocSortSink::KeySpec{
            .expression = k.expression,
            .ascending = k.ascending,
            .nulls_first = k.nulls_first,
            .collation = k.collation,
        });
      }
      return specs;
    };
    out->source = std::make_shared<IncrementalSortSource>(
        std::move(out->source), std::move(out->operators), inc->GetSchema(),
        to_specs(inc->PrefixKeys()), to_specs(inc->SuffixKeys()));
    out->operators.clear();
    out->schema = inc->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  // 14. SetOperation (`SetOperationPlan`).
  if (const auto* setop = dynamic_cast<const SetOperationPlan*>(base)) {
    const auto& children = setop->Children();
    if (children.empty()) {
      out->source = std::make_shared<RowVectorSource>(std::vector<Row>{},
                                                      "SetOperationEmpty");
      out->operators.clear();
      out->schema = setop->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
    if (setop->Operation() == SetOperationKind::kUnionAll) {
      std::vector<OocSetOpInputStream> streams;
      streams.reserve(children.size());
      for (const Plan& child : children) {
        LoweredStream cs;
        RETURN_IF_FAIL(LowerNode(child, ctx, custom_hook, graph, &cs));
        streams.push_back(OocSetOpInputStream{
            .source = std::move(cs.source),
            .operators = std::move(cs.operators),
            .schema = std::move(cs.schema),
        });
      }
      if (setop->OrderKeys().empty()) {
        out->source = std::make_shared<OocConcatSource>(std::move(streams),
                                                        setop->GetSchema());
      } else {
        std::vector<OocSortSink::KeySpec> specs;
        specs.reserve(setop->OrderKeys().size());
        for (const SortKey& k : setop->OrderKeys()) {
          specs.push_back(OocSortSink::KeySpec{
              .expression = k.expression,
              .ascending = k.ascending,
              .nulls_first = k.nulls_first,
              .collation = k.collation,
          });
        }
        out->source = std::make_shared<OocMergeAppendSource>(
            std::move(streams), std::move(specs), setop->GetSchema());
      }
      out->operators.clear();
      out->schema = setop->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
    std::vector<Schema> child_schemas;
    child_schemas.reserve(children.size());
    for (const Plan& child : children) {
      child_schemas.push_back(child->GetSchema());
    }
    auto coord = std::make_shared<OocSetOpCoordinator>(
        setop->Operation(), setop->GetSchema(), std::move(child_schemas));
    for (size_t c = 0; c + 1 < children.size(); ++c) {
      LoweredStream cs;
      RETURN_IF_FAIL(LowerNode(children[c], ctx, custom_hook, graph, &cs));
      auto stage_sink = std::make_shared<OocSetOpInputSink>(
          coord, c, /*is_final=*/false, nullptr);
      RETURN_IF_FAIL(BreakWithSink(graph, &cs, std::move(stage_sink),
                                   "SetOperationInput", setop->GetSchema()));
      coord->RegisterExplainSource(cs.source);
    }
    LoweredStream last_cs;
    RETURN_IF_FAIL(
        LowerNode(children.back(), ctx, custom_hook, graph, &last_cs));
    auto final_sink = std::make_shared<OocSetOpInputSink>(
        coord, children.size() - 1, /*is_final=*/true, nullptr);
    RETURN_IF_FAIL(BreakWithSink(graph, &last_cs, std::move(final_sink),
                                 "SetOperation", setop->GetSchema()));
    *out = std::move(last_cs);
    return Status::kSuccess;
  }

  // 15. Multi-Index & Leaf Scans (`IndexSkipScanPlan`, `MinMaxIndexPlan`,
  //     `BitmapScanPlan`, `MultiIndexCoveringScanPlan`).
  if (const auto* skip = dynamic_cast<const IndexSkipScanPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(skip->GetSource(), ctx, custom_hook, graph, out));
    out->source = std::make_shared<IndexSkipScanSource>(
        std::move(out->source), std::move(out->operators), skip->GetSchema());
    out->operators.clear();
    out->schema = skip->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  if (const auto* mm = dynamic_cast<const MinMaxIndexPlan*>(base)) {
    RETURN_IF_FAIL(LowerNode(mm->Child(), ctx, custom_hook, graph, out));
    out->source = std::make_shared<MinMaxIndexSource>(
        std::move(out->source), std::move(out->operators), mm->GetSchema(),
        mm->ValueSlot());
    out->operators.clear();
    out->schema = mm->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  if (const auto* bms = dynamic_cast<const BitmapScanPlan*>(base)) {
    if (ContainsParameterSlot(bms->Where())) {
      Executor scan = std::make_shared<FullScan>(ctx.txn_, bms->GetTable());
      out->source = std::make_shared<StreamingExecutorSource>(std::move(scan),
                                                              "FullScan");
      out->operators.clear();
      if (bms->Where()) {
        out->operators.push_back(std::make_shared<FilterOp>(
            bms->Where(), bms->GetTable().GetSchema()));
      }
      out->schema = bms->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
    std::vector<OocIndexRange> ranges;
    ranges.reserve(bms->Ranges().size());
    for (const BitmapIndexRange& r : bms->Ranges()) {
      ranges.push_back(OocIndexRange{
          .index = r.index, .begin_key = r.begin_key, .end_key = r.end_key});
    }
    OocScanFallbackFactory fallback_factory =
        [table = &bms->GetTable(), where = bms->Where()](
            PipelineContext* pctx) -> std::shared_ptr<PipelineSource> {
      Executor scan = std::make_shared<FullScan>(pctx->txn->txn_, *table);
      if (where) {
        scan = std::make_shared<Selection>(where, table->GetSchema(),
                                           std::move(scan));
      }
      return std::make_shared<StreamingExecutorSource>(std::move(scan),
                                                       "FullScan");
    };
    out->source = std::make_shared<OocBitmapScanSource>(
        &bms->GetTable(), std::move(ranges),
        bms->Combine() == BitmapCombine::kAnd, bms->Where(), bms->GetSchema(),
        std::move(fallback_factory));
    out->operators.clear();
    out->schema = bms->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  if (const auto* mic = dynamic_cast<const MultiIndexCoveringScanPlan*>(base)) {
    std::vector<BitmapIndexRange> raw_participants = mic->Ranges();
    bool force_fallback = false;
    if (ContainsParameterSlot(mic->Where())) {
      raw_participants.clear();
      for (const BitmapIndexRange& range : mic->Ranges()) {
        ReboundIndexKeys rebound = ReboundIndexKeysFromPredicate(
            mic->Where(), mic->GetTable().GetSchema(), *range.index,
            /*want_points=*/false);
        if (rebound.begin_key.empty() && rebound.end_key.empty()) {
          force_fallback = true;
          break;
        }
        BitmapIndexRange part;
        part.index = range.index;
        part.begin_key = std::move(rebound.begin_key);
        part.end_key = std::move(rebound.end_key);
        raw_participants.push_back(std::move(part));
      }
    }
    std::vector<OocIndexRange> participants;
    participants.reserve(raw_participants.size());
    for (BitmapIndexRange& r : raw_participants) {
      participants.push_back(OocIndexRange{.index = r.index,
                                           .begin_key = std::move(r.begin_key),
                                           .end_key = std::move(r.end_key)});
    }
    OocScanFallbackFactory fallback_factory =
        [table = &mic->GetTable(), where = mic->Where(),
         out_schema = mic->GetSchema()](
            PipelineContext* pctx) -> std::shared_ptr<PipelineSource> {
      Executor scan = std::make_shared<FullScan>(pctx->txn->txn_, *table);
      if (where) {
        scan = std::make_shared<Selection>(where, table->GetSchema(),
                                           std::move(scan));
      }
      std::vector<NamedExpression> columns;
      columns.reserve(out_schema.ColumnCount());
      for (size_t i = 0; i < out_schema.ColumnCount(); ++i) {
        columns.emplace_back(out_schema.GetColumn(i).Name());
      }
      bool same_layout = columns.size() == table->GetSchema().ColumnCount();
      for (size_t i = 0; same_layout && i < columns.size(); ++i) {
        same_layout = columns[i].expression->Type() == TypeTag::kColumnValue &&
                      columns[i].expression->AsColumnValue().GetColumnName() ==
                          table->GetSchema().GetColumn(i).Name();
      }
      if (!same_layout) {
        scan = std::make_shared<Projection>(
            std::move(columns), table->GetSchema(), std::move(scan));
      }
      return std::make_shared<StreamingExecutorSource>(std::move(scan),
                                                       "FullScan");
    };
    out->source = std::make_shared<OocMultiIndexCoveringSource>(
        &mic->GetTable(), std::move(participants), mic->Where(),
        mic->CoveredSlots(), mic->GetSchema(), force_fallback,
        std::move(fallback_factory));
    out->operators.clear();
    out->schema = mic->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  // 16. MergeJoin (`MergeJoinPlan`).
  if (const auto* mj = dynamic_cast<const MergeJoinPlan*>(base)) {
    std::vector<slot_t> lcols;
    std::vector<slot_t> rcols;
    if (ResolveKeyOffsets(mj->Left()->GetSchema(), mj->LeftKeys(), &lcols) &&
        ResolveKeyOffsets(mj->Right()->GetSchema(), mj->RightKeys(), &rcols)) {
      LoweredStream lstream;
      RETURN_IF_FAIL(LowerNode(mj->Left(), ctx, custom_hook, graph, &lstream));
      LoweredStream rstream;
      RETURN_IF_FAIL(LowerNode(mj->Right(), ctx, custom_hook, graph, &rstream));
      Schema residual_schema =
          mj->Residual() ? mj->Left()->GetSchema() + mj->Right()->GetSchema()
                         : Schema();
      OocSetOpInputStream l_in{.source = std::move(lstream.source),
                               .operators = std::move(lstream.operators),
                               .schema = std::move(lstream.schema)};
      OocSetOpInputStream r_in{.source = std::move(rstream.source),
                               .operators = std::move(rstream.operators),
                               .schema = std::move(rstream.schema)};
      out->source = std::make_shared<OocMergeJoinSource>(
          std::move(l_in), std::move(r_in), std::move(lcols), std::move(rcols),
          mj->Kind(), mj->Left()->GetSchema().ColumnCount(),
          mj->Right()->GetSchema().ColumnCount(), mj->Residual(),
          std::move(residual_schema), mj->GetSchema());
      out->operators.clear();
      out->schema = mj->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
  }

  // 17. BandJoin (`BandJoinPlan`).
  if (const auto* bj = dynamic_cast<const BandJoinPlan*>(base)) {
    std::vector<slot_t> lcols;
    std::vector<slot_t> rcols;
    if (ResolveKeyOffsets(bj->Left()->GetSchema(), bj->LeftKeys(), &lcols) &&
        ResolveKeyOffsets(bj->Right()->GetSchema(), bj->RightKeys(), &rcols) &&
        lcols.size() == 1 && rcols.size() == 1) {
      LoweredStream lstream;
      RETURN_IF_FAIL(LowerNode(bj->Left(), ctx, custom_hook, graph, &lstream));
      LoweredStream rstream;
      RETURN_IF_FAIL(LowerNode(bj->Right(), ctx, custom_hook, graph, &rstream));
      const Schema combined =
          bj->Left()->GetSchema() + bj->Right()->GetSchema();
      OocSetOpInputStream l_in{.source = std::move(lstream.source),
                               .operators = std::move(lstream.operators),
                               .schema = std::move(lstream.schema)};
      OocSetOpInputStream r_in{.source = std::move(rstream.source),
                               .operators = std::move(rstream.operators),
                               .schema = std::move(rstream.schema)};
      out->source = std::make_shared<OocBandJoinSource>(
          std::move(l_in), std::move(r_in), lcols.front(), rcols.front(),
          bj->Comparison(), bj->Predicate(), combined, bj->Kind(),
          bj->Left()->GetSchema().ColumnCount(),
          bj->Right()->GetSchema().ColumnCount(), bj->GetSchema());
      out->operators.clear();
      out->schema = bj->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
  }

  // 18. RecursiveCTE (`RecursiveCtePlan`).
  if (const auto* rec = dynamic_cast<const RecursiveCtePlan*>(base)) {
    out->source = std::make_shared<OocRecursiveCteSource>(
        rec->CteName(), rec->Body(), rec->DepthSpec(), rec->GetSchema());
    out->operators.clear();
    out->schema = rec->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  // 19. Correlated Apply / Lateral Join (`ApplyPlan`).
  if (const auto* app = dynamic_cast<const ApplyPlan*>(base)) {
    LoweredStream outer_stream;
    if (app->Child() != nullptr) {
      RETURN_IF_FAIL(
          LowerNode(app->Child(), ctx, custom_hook, graph, &outer_stream));
    } else {
      Executor single_row =
          std::make_shared<ValuesExecutor>(std::vector<Row>{Row({})});
      outer_stream.source = std::make_shared<StreamingExecutorSource>(
          std::move(single_row), "Values");
      outer_stream.schema = Schema();
    }
    OocSetOpInputStream outer_in{
        .source = std::move(outer_stream.source),
        .operators = std::move(outer_stream.operators),
        .schema = app->Child() ? app->Child()->GetSchema() : Schema(),
    };
    std::optional<OocSetOpInputStream> inner_in;
    if (app->InnerChild() != nullptr && app->InnerQuery() == nullptr) {
      LoweredStream inner_stream;
      RETURN_IF_FAIL(
          LowerNode(app->InnerChild(), ctx, custom_hook, graph, &inner_stream));
      inner_in = OocSetOpInputStream{
          .source = std::move(inner_stream.source),
          .operators = std::move(inner_stream.operators),
          .schema = app->InnerChild()->GetSchema(),
      };
    }
    out->source = std::make_shared<OocApplySource>(
        std::move(outer_in), std::move(inner_in),
        app->Child() ? app->Child()->GetSchema() : Schema(), app->InnerQuery(),
        app->Alias(), app->Predicate(), app->Kind(), app->GetSchema());
    out->operators.clear();
    out->schema = app->GetSchema();
    out->scan_origin.reset();
    return Status::kSuccess;
  }

  // 20. LateMaterializationJoin (`LateMaterializationJoinPlan`) and
  //     ZigZagJoin (`ZigZagJoinPlan`).
  if (const auto* lm = dynamic_cast<const LateMaterializationJoinPlan*>(base)) {
    const bool use_lookup =
        lm->RightTable() != nullptr &&
        (lm->Identity() == LateMaterializationIdentity::kRowId ||
         (lm->RightIndex() != nullptr &&
          !ctx.txn_.IndexKeysMayBeStale(lm->RightIndex()->Root())));
    if (use_lookup) {
      LoweredStream left_stream;
      RETURN_IF_FAIL(
          LowerNode(lm->Left(), ctx, custom_hook, graph, &left_stream));
      const Table* rtbl = lm->RightTable();
      const Index* ridx = lm->RightIndex();
      std::vector<slot_t> lcols;
      std::vector<slot_t> rcols;
      if (lm->Identity() != LateMaterializationIdentity::kRowId) {
        (void)ResolveKeyOffsets(lm->Left()->GetSchema(), lm->LeftKeys(),
                                &lcols);
        (void)ResolveKeyOffsets(rtbl->GetSchema(), lm->RightKeys(), &rcols);
      }
      auto lsrc = std::move(left_stream.source);
      auto lops = std::move(left_stream.operators);
      out->source = std::make_shared<LazyAdaptedPipelineSource>(
          [lsrc = std::move(lsrc), lops = std::move(lops), rtbl, ridx,
           lcols = std::move(lcols), rcols = std::move(rcols),
           ident = lm->Identity(), rowid = lm->RowId(),
           rproj = lm->RightProjection(), pred = lm->Predicate(),
           unique = lm->UniqueWinner(), out_schema = lm->GetSchema()](
              PipelineContext* pctx) -> StatusOr<Executor> {
            Executor left_exec = std::make_shared<PipelineStreamExecutor>(
                pctx, lsrc, lops, "PipelineLeft");
            if (ident == LateMaterializationIdentity::kRowId) {
              return Executor(std::make_shared<LateMaterializationJoin>(
                  pctx->txn->txn_, std::move(left_exec), *rtbl, out_schema,
                  rowid, rproj, pred));
            }
            auto guard = pctx->txn->txn_.AcquireIndexReadGuard(ridx->Root());
            return Executor(std::make_shared<LateMaterializationJoin>(
                pctx->txn->txn_, std::move(left_exec), lcols, *rtbl, *ridx,
                rcols, out_schema, pred, unique, rproj, std::move(guard)));
          },
          "LateMaterializationJoin");
      out->operators.clear();
      out->schema = lm->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
    if (lm->RightTable() != nullptr) {
      // Stale/null index fallback: OOC block-nested-loop join over FullScan.
      const Schema& right_phys = lm->RightTable()->GetSchema();
      std::vector<Column> rcols_def;
      rcols_def.reserve(lm->GetSchema().ColumnCount() -
                        lm->Left()->GetSchema().ColumnCount());
      for (size_t i = lm->Left()->GetSchema().ColumnCount();
           i < lm->GetSchema().ColumnCount(); ++i) {
        rcols_def.push_back(lm->GetSchema().GetColumn(i));
      }
      const Schema right_out("", std::move(rcols_def));
      LoweredStream lstream;
      RETURN_IF_FAIL(LowerNode(lm->Left(), ctx, custom_hook, graph, &lstream));
      Executor right_scan =
          std::make_shared<FullScan>(ctx.txn_, *lm->RightTable());
      std::vector<NamedExpression> proj_items;
      proj_items.reserve(right_out.ColumnCount());
      for (size_t i = 0; i < right_out.ColumnCount(); ++i) {
        proj_items.emplace_back(
            right_out.GetColumn(i).Name().ToString(),
            ColumnValueExp(
                right_phys.GetColumn(lm->RightProjection()[i]).Name()));
      }
      right_scan = std::make_shared<Projection>(
          std::move(proj_items), right_phys, std::move(right_scan));
      auto rsrc = std::make_shared<StreamingExecutorSource>(
          std::move(right_scan), "FullScan");

      auto cross_ctx = std::make_shared<OocCrossJoinContext>();
      cross_ctx->kind = JoinKind::kInner;
      cross_ctx->left_width = lm->Left()->GetSchema().ColumnCount();
      cross_ctx->right_width = right_out.ColumnCount();
      cross_ctx->left_schema = lm->Left()->GetSchema();
      cross_ctx->right_schema = right_out;
      cross_ctx->combined_schema = lm->Left()->GetSchema() + right_out;
      cross_ctx->output_schema = lm->GetSchema();
      cross_ctx->residual = lm->Predicate();
      cross_ctx->residual_conjuncts = SplitConjuncts(lm->Predicate());
      cross_ctx->prefer_bnl = true;
      cross_ctx->right_source_explain = rsrc;

      auto build_sink = std::make_shared<OocCrossJoinBuildSink>(cross_ctx);
      graph->stages.push_back(PipelineStage{
          .id = graph->stages.size(),
          .name = build_sink->Describe(),
          .source = std::move(rsrc),
          .operators = {},
          .sink = std::move(build_sink),
          .output_binding = nullptr,
      });
      out->source = std::make_shared<OocCrossJoinProbeSource>(
          std::move(cross_ctx),
          OocSetOpInputStream{.source = std::move(lstream.source),
                              .operators = std::move(lstream.operators),
                              .schema = std::move(lstream.schema)});
      out->operators.clear();
      out->schema = lm->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
  }

  if (const auto* zz = dynamic_cast<const ZigZagJoinPlan*>(base)) {
    std::vector<slot_t> lcols;
    std::vector<slot_t> rcols;
    if (ResolveKeyOffsets(zz->Left()->GetSchema(), zz->LeftKeys(), &lcols) &&
        ResolveKeyOffsets(zz->Right()->GetSchema(), zz->RightKeys(), &rcols) &&
        !lcols.empty() && !rcols.empty()) {
      const Index* li = ZigZagIndexOf(zz->Left());
      const Index* ri = ZigZagIndexOf(zz->Right());
      const bool stale =
          (li != nullptr && ctx.txn_.IndexKeysMayBeStale(li->Root())) ||
          (ri != nullptr && ctx.txn_.IndexKeysMayBeStale(ri->Root()));
      LoweredStream lstream;
      RETURN_IF_FAIL(LowerNode(zz->Left(), ctx, custom_hook, graph, &lstream));
      LoweredStream rstream;
      RETURN_IF_FAIL(LowerNode(zz->Right(), ctx, custom_hook, graph, &rstream));
      if (!stale) {
        out->source = std::make_shared<LazyAdaptedPipelineSource>(
            [lsrc = std::move(lstream.source),
             lops = std::move(lstream.operators),
             rsrc = std::move(rstream.source),
             rops = std::move(rstream.operators), lcol = lcols.front(),
             rcol = rcols.front(), op = zz->Operation(), pred = zz->Predicate(),
             out_schema =
                 zz->GetSchema()](PipelineContext* pctx) -> StatusOr<Executor> {
              Executor lex = std::make_shared<PipelineStreamExecutor>(
                  pctx, lsrc, lops, "PipelineLeft");
              Executor rex = std::make_shared<PipelineStreamExecutor>(
                  pctx, rsrc, rops, "PipelineRight");
              return Executor(std::make_shared<ZigZagJoin>(
                  std::move(lex), lcol, std::move(rex), rcol, op, pred,
                  out_schema));
            },
            "ZigZagJoin");
      } else {
        auto cross_ctx = std::make_shared<OocCrossJoinContext>();
        cross_ctx->kind = JoinKind::kInner;
        cross_ctx->left_width = zz->Left()->GetSchema().ColumnCount();
        cross_ctx->right_width = zz->Right()->GetSchema().ColumnCount();
        cross_ctx->left_schema = zz->Left()->GetSchema();
        cross_ctx->right_schema = zz->Right()->GetSchema();
        cross_ctx->combined_schema =
            cross_ctx->left_schema + cross_ctx->right_schema;
        cross_ctx->output_schema = zz->GetSchema();
        cross_ctx->residual = zz->Predicate();
        cross_ctx->residual_conjuncts = SplitConjuncts(zz->Predicate());
        cross_ctx->prefer_bnl = true;
        cross_ctx->right_source_explain = rstream.source;
        cross_ctx->right_ops_explain = rstream.operators;

        auto build_sink = std::make_shared<OocCrossJoinBuildSink>(cross_ctx);
        graph->stages.push_back(PipelineStage{
            .id = graph->stages.size(),
            .name = build_sink->Describe(),
            .source = std::move(rstream.source),
            .operators = std::move(rstream.operators),
            .sink = std::move(build_sink),
            .output_binding = nullptr,
        });
        out->source = std::make_shared<OocCrossJoinProbeSource>(
            std::move(cross_ctx),
            OocSetOpInputStream{.source = std::move(lstream.source),
                                .operators = std::move(lstream.operators),
                                .schema = std::move(lstream.schema)});
      }
      out->operators.clear();
      out->schema = zz->GetSchema();
      out->scan_origin.reset();
      return Status::kSuccess;
    }
  }

  // 21. Fallback: wrap unmigrated operators cleanly in `LegacySource`.
  Executor fallback = plan->EmitExecutor(ctx);
  out->source =
      std::make_shared<LegacySource>(std::move(fallback), plan->ToString());
  out->operators.clear();
  out->schema = plan->GetSchema();
  out->scan_origin.reset();
  return Status::kSuccess;
}

StatusOr<PipelineGraph> PipelineLowering::Lower(
    const Plan& plan, TransactionContext& ctx,
    const PipelineLoweringOptions& options) {
  PipelineGraph graph;
  graph.requested_dop = std::max<size_t>(1, options.requested_dop);
  if (options.keep_plan_explain_text && plan != nullptr) {
    std::ostringstream oss;
    plan->Dump(oss, 0);
    graph.plan_explain_text = oss.str();
  }

  LoweredStream stream;
  RETURN_IF_FAIL(
      LowerNode(plan, ctx, options.custom_plan_lowerer, &graph, &stream));

  if (options.apply_post_distinct &&
      (plan == nullptr || !plan->EnforcesDistinct())) {
    // Same contract as case 9: when no post sort follows, the caller relies
    // on the plan's own ordering surviving the distinct, spilled or not.
    auto dist_sink = std::make_shared<OocDistinctSink>(
        stream.schema, std::vector<slot_t>{}, "Distinct",
        std::vector<Expression>{},
        options.post_sort_keys.empty() && ChildHasAnyOrder(plan));
    RETURN_IF_FAIL(BreakWithSink(&graph, &stream, std::move(dist_sink),
                                 "Distinct", stream.schema));
  }

  if (!options.post_sort_keys.empty()) {
    const std::string sort_label =
        options.post_limit.has_value() ? "TopN" : "Sort";
    auto sort_sink = std::make_shared<OocSortSink>(
        options.post_sort_keys, stream.schema, options.post_limit,
        options.post_offset, sort_label);
    RETURN_IF_FAIL(BreakWithSink(&graph, &stream, std::move(sort_sink),
                                 sort_label, stream.schema));
  } else if (options.post_limit.has_value() || options.post_offset > 0) {
    stream.operators.push_back(std::make_shared<LimitOp>(
        options.post_limit.value_or(0), options.post_offset,
        options.post_limit.has_value()));
  }

  if (!options.post_projection.empty()) {
    stream.operators.push_back(
        std::make_shared<ProjectOp>(options.post_projection, stream.schema));
    stream.schema = options.post_projection_schema;
  }

  const size_t root_id = graph.stages.size();
  graph.stages.push_back(PipelineStage{
      .id = root_id,
      .name = "Output",
      .source = std::move(stream.source),
      .operators = std::move(stream.operators),
      .sink = nullptr,
      .output_binding = nullptr,
  });
  graph.output_schema = std::move(stream.schema);

  const bool has_blocking_stage = graph.stages.size() > 1;
  if (has_blocking_stage) {
    const size_t raw_est = EstimatePlanMemoryBytes(plan);
    graph.estimated_grant_bytes = PipelineGrantRequestBytes(raw_est);
    graph.min_grant_bytes = size_t{64} << 10;
  } else {
    graph.estimated_grant_bytes = size_t{64} << 10;
    graph.min_grant_bytes = size_t{16} << 10;
  }
  return graph;
}

StatusOr<Executor> PipelineLowering::LowerToExecutor(
    const Plan& plan, TransactionContext& ctx,
    const PipelineLoweringOptions& options) {
  ASSIGN_OR_RETURN(PipelineGraph, graph, Lower(plan, ctx, options));
  return Executor(std::make_shared<PipelineExecutor>(&ctx, std::move(graph)));
}

}  // namespace tinylamb
