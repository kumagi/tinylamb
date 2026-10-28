/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Relational factory (improvement3.md A5 second stage / V4): every
// PlanBase::EmitExecutor implementation lives here so the plan layer carries
// logical information only and never includes executor headers. Plans expose
// an opaque Executor handle (plan/plan.hpp forward declares ExecutorBase);
// this translation unit owns the concrete executors they build.

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/set_operation.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/apply.hpp"
#include "executor/band_join.hpp"
#include "executor/batch_nested_loop_join.hpp"
#include "executor/bitmap_scan.hpp"
#include "executor/constant_executor.hpp"
#include "executor/cross_join.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/grouped_aggregate.hpp"
#include "executor/hash_join.hpp"
#include "executor/incremental_sort.hpp"
#include "executor/index_only_scan.hpp"
#include "executor/index_scan.hpp"
#include "executor/late_materialization_join.hpp"
#include "executor/limit.hpp"
#include "executor/lookup_join.hpp"
#include "executor/lsm_scan.hpp"
#include "executor/materialize.hpp"
#include "executor/max1_row.hpp"
#include "executor/merge_append.hpp"
#include "executor/merge_join.hpp"
#include "executor/minmax_index.hpp"
#include "executor/multi_index_covering_scan.hpp"
#include "executor/nested_loop_join.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/parallel_hash_join.hpp"
#include "executor/parallel_index_scan.hpp"
#include "executor/parallel_merge_join.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/projection.hpp"
#include "executor/recursive_cte.hpp"
#include "executor/relational.hpp"
#include "executor/selection.hpp"
#include "executor/set_operation.hpp"
#include "executor/skip_scan_distinct.hpp"
#include "executor/sort.hpp"
#include "executor/table_sample.hpp"
#include "executor/topn.hpp"
#include "executor/unnest.hpp"
#include "executor/values.hpp"
#include "executor/window.hpp"
#include "executor/zigzag_join.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "index/index.hpp"
#include "page/row_position.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/apply_plan.hpp"
#include "plan/band_join_plan.hpp"
#include "plan/bitmap_scan_plan.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/empty_plan.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/group_by_plan.hpp"
#include "plan/incremental_sort_plan.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/index_skip_scan_plan.hpp"
#include "plan/late_materialization_join_plan.hpp"
#include "plan/limit_plan.hpp"
#include "plan/lsm_scan_plan.hpp"
#include "plan/materialize_plan.hpp"
#include "plan/max1_row_plan.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/minmax_index_plan.hpp"
#include "plan/multi_index_covering_scan_plan.hpp"
#include "plan/parallel_thresholds.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/recursive_cte_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "plan/sample_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/set_operation_plan.hpp"
#include "plan/skip_scan_distinct_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/topn_plan.hpp"
#include "plan/unnest_plan.hpp"
#include "plan/values_plan.hpp"
#include "plan/window_plan.hpp"
#include "plan/zigzag_join_plan.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

Executor RelationalPlan::EmitExecutor(TransactionContext& context) const {
  return std::make_shared<RelationalExecutor>(context, statement_);
}

Executor GroupByPlan::EmitExecutor(TransactionContext& context) const {
  return EmitGroupedFinishExecutor(context, child_, statement_);
}

Executor UnnestPlan::EmitExecutor(TransactionContext& ctx) const {
  Schema child_schema = child_ ? child_->GetSchema() : Schema();
  Executor child_exec =
      child_ ? child_->EmitExecutor(ctx)
             : std::make_shared<ValuesExecutor>(std::vector<Row>{Row({})});
  return std::make_shared<UnnestExecutor>(
      ctx, std::move(child_schema), std::move(child_exec), unnest_expr_, alias_,
      offset_alias_, false, UnnestSection());
}

Executor ApplyPlan::EmitExecutor(TransactionContext& ctx) const {
  Schema child_schema = child_ ? child_->GetSchema() : Schema();
  Executor child_exec =
      child_ ? child_->EmitExecutor(ctx)
             : std::make_shared<ValuesExecutor>(std::vector<Row>{Row({})});
  Executor inner_exec =
      inner_child_ ? inner_child_->EmitExecutor(ctx) : nullptr;
  return std::make_shared<ApplyExecutor>(
      ctx, std::move(child_schema), std::move(child_exec), inner_query_, alias_,
      std::move(inner_exec), predicate_, kind_, schema_);
}

Executor RecursiveCtePlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<RecursiveCteExecutor>(ctx, cte_name_, body_,
                                                depth_spec_, schema_);
}

Executor WindowPlan::EmitExecutor(TransactionContext& ctx) const {
  Executor child_exec =
      child_ ? child_->EmitExecutor(ctx)
             : std::make_shared<ValuesExecutor>(std::vector<Row>{Row({})});
  return std::make_shared<WindowExecutor>(
      ctx, std::move(child_exec), GetSchema(), WindowOutputs(),
      WindowExecutor::Options{
          .memory_budget_bytes = Memory().memory_budget_bytes,
          .spill_partition_count = Memory().spill_partition_count});
}

// Streaming pass-through that surfaces a relation rename in EXPLAIN/Dump
// output; rows and row positions flow through untouched. It used to live in
// plan/relation_rename_plan.hpp and moved here with the rest of the factory.
class RelationRenameExecutor final : public ExecutorBase {
 public:
  RelationRenameExecutor(Executor src, std::string relation,
                         std::string physical)
      : src_(std::move(src)),
        relation_(std::move(relation)),
        physical_(std::move(physical)) {}

  bool Next(Row* dst, RowPosition* rp) override {
    if (!src_->Next(dst, rp)) {
      FailWithChildOf(*src_);
      return false;
    }
    return true;
  }
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    const size_t count = src_->NextBatch(destination, max_rows);
    if (count == 0) {
      FailWithChildOf(*src_);
    }
    return count;
  }
  void Dump(std::ostream& o, int indent) const override;
  // Renaming is layout-transparent (rows and positions pass through
  // untouched), so a dynamic join filter addressed by slot reaches the scan
  // below unchanged. This is what makes the filter reach the storage-level
  // pre-decode rejection instead of stopping at this decorator.
  bool AttachRuntimeFilter(
      const std::shared_ptr<const RuntimeFilterGroup>& group, slot_t column,
      ValueType type) override {
    return src_->AttachRuntimeFilter(group, column, type);
  }

 private:
  Executor src_;
  std::string relation_;
  std::string physical_;
};

void RelationRenameExecutor::Dump(std::ostream& o, int indent) const {
  o << "Rename: " << physical_ << " AS " << relation_ << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  src_->Dump(o, indent + 2);
}

Executor FullScanPlan::EmitExecutor(TransactionContext& txn) const {
  // A morsel-parallel scan with the morsel-ordered handoff still delivers
  // stored-chain order (D-9), so a clustered-order proof no longer forces the
  // single-threaded FullScan; the ordered parallel scan just refuses row
  // filter pushdown (deferred-row parking would reorder). IsOrderedBy
  // consults EmitsChainOrderedScan, so the two never disagree.
  //
  // DML source scans (lock_rows_) must resolve rows under a write intent; the
  // morsel-based ParallelScan has no such hook, so they stay on the
  // single-threaded FullScan.
  if (!lock_rows_ && MaxRows() == std::numeric_limits<size_t>::max() &&
      stats_.Rows() >= kParallelScanMinRows) {
    constexpr size_t kMaxParallelScanWorkers = 16;
    const size_t workers =
        std::min(kMaxParallelScanWorkers,
                 std::max<size_t>(1, std::thread::hardware_concurrency()));
    return std::make_shared<ParallelScan>(txn.txn_, table_, workers, 8,
                                          projection_, peek_compares_,
                                          ReportsClusteredOrder());
  }
  const slot_t clustered = ClusteredColumn();
  std::optional<ColumnName> clustered_order;
  if (clustered != kNoClusteredColumn) {
    clustered_order = table_.GetSchema().GetColumn(clustered).Name();
  }
  return std::make_shared<FullScan>(txn.txn_, table_, MaxRows(), lock_rows_,
                                    wait_for_write_intent_,
                                    std::move(clustered_order), projection_);
}

Executor SelectionPlan::EmitExecutor(TransactionContext& ctx) const {
  Executor src = src_->EmitExecutor(ctx);
  // A Selection directly above a morsel-parallel scan is the Cascades core's
  // big-table filter shape.  When the predicate lowers to context-free
  // bytecode, push it into the scan so the decode workers decide rows before
  // chunk append (the serial Selection evaluates on the single consumer
  // thread); rows a worker program cannot decide park for the same
  // context-free re-check the serial Selection would run.  Everything else
  // keeps the streaming serial Selection.
  auto* scan = dynamic_cast<ParallelScan*>(src.get());
  if (scan != nullptr && scan->CanTakeRowFilter() &&
      std::getenv("TINYLAMB_DISABLE_WORKER_SCAN_FILTER") == nullptr &&
      !relational_detail::ContainsQuery(exp_) && BytecodeEnabled()) {
    std::vector<Expression> conjuncts = SplitConjuncts(exp_);
    auto filter = std::make_unique<relational_detail::CompiledScanFilter>(
        relational_detail::CompileScanFilter(conjuncts, src_->GetSchema()));
    auto programs =
        std::make_unique<relational_detail::WorkerScanFilterPrograms>(
            relational_detail::CompileWorkerFilter(*filter, src_->GetSchema()));
    if (programs->complete) {
      scan->SetRowFilter(std::move(conjuncts), std::move(*filter),
                         std::move(*programs));
      return src;
    }
  }
  return std::make_shared<Selection>(exp_, src_->GetSchema(), std::move(src));
}

Executor ProjectionPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Projection>(columns_, src_->GetSchema(),
                                      src_->EmitExecutor(ctx));
}

Executor LimitPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<LimitExecutor>(src_->EmitExecutor(ctx), limit_count_,
                                         limit_offset_);
}

Executor SamplePlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<TableSampleExecutor>(ctx, child_->EmitExecutor(ctx),
                                               child_->GetSchema(), spec_);
}

namespace {
// Translates a plan sort key list into the operator's key list; both shapes
// (BufferedSortPlan / ExternalMergeSortPlan) and the base node use it, so the
// ORDER BY contract reaches the operator identically in every case.
// The stale-index fallbacks below replace an ordered index walk with a heap
// scan.  The plan above may already have elided a sort on the index order
// (ORDER BY, merge / band joins rely on IsOrderedBy of the index plan), so the
// fallback restores that order explicitly.  Inside one transaction a DELETE +
// INSERT is enough to make the keys "stale" and the heap order arbitrary
// (`ORDER BY k` returned 0, 21, 2, 4 and BandJoin failed with "input is not
// ordered").
Executor SortedByIndexKey(Executor scan, const Schema& output_schema,
                          const Table& table, const Index& index,
                          bool ascending) {
  std::vector<SortExecutor::Key> keys;
  keys.reserve(index.sc_.key_.size());
  for (const slot_t offset : index.sc_.key_) {
    keys.push_back(SortExecutor::Key{
        .expression =
            ColumnValueExp(table.GetSchema().GetColumn(offset).Name()),
        .ascending = ascending});
  }
  if (keys.empty()) {
    return scan;
  }
  return std::make_shared<SortExecutor>(std::move(scan), output_schema,
                                        std::move(keys));
}

std::vector<SortExecutor::Key> ExecutorSortKeys(
    const std::vector<SortKey>& keys) {
  std::vector<SortExecutor::Key> out;
  out.reserve(keys.size());
  for (const SortKey& key : keys) {
    out.push_back(SortExecutor::Key{.expression = key.expression,
                                    .ascending = key.ascending,
                                    .nulls_first = key.nulls_first,
                                    .collation = key.collation});
  }
  return out;
}

// The physical options a plan's logical memory policy (SortMemoryPolicy)
// resolves to. The operator keeps its own spill selection as the last resort,
// so a mis-estimated shape still returns the right rows in the right order.
SortExecutor::Options SortExecutorOptions(const SortPlan& plan) {
  const SortMemoryPolicy& memory = plan.Memory();
  return SortExecutor::Options{
      .spill_mode = SortExecutor::SpillMode::kAuto,
      .memory_budget_bytes = memory.memory_budget_bytes,
      .merge_window_rows = memory.merge_window_rows,
      .spill_threshold_rows = 0};
}

// The physical options a plan's logical memory policy (AggregateMemoryPolicy)
// resolves to. Shared by the three grouped shapes so they cannot disagree about
// the memory contract they were planned with.
GroupedAggregateOptions GroupedAggregateOptionsFor(
    const AggregationPlan& plan) {
  const AggregateMemoryPolicy& memory = plan.Memory();
  GroupedAggregateOptions options;
  options.memory_budget_bytes = memory.memory_budget_bytes;
  options.spill_partition_count = AggregateSpillPartitionCount(memory);
  options.merge_window_rows = memory.merge_window_rows;
  return options;
}

// Workers the hash shape may use. Same threshold and worker cap as the scalar
// parallel aggregation, so one rule decides "is this big enough to fan out" for
// every aggregation shape.
size_t GroupedAggregateWorkers(size_t child_rows) {
  if (child_rows < kParallelAggregationMinRows) {
    return 1;
  }
  constexpr size_t kMaxParallelAggregationWorkers = 16;
  return std::min(kMaxParallelAggregationWorkers,
                  std::max<size_t>(1, std::thread::hardware_concurrency()));
}

// The no-GROUP-BY-KEYS path, shared by all three shapes: the historical serial
// accumulator with its typed batch fast path, promoted to the parallel
// accumulator above the threshold. A payload without keys has exactly one
// group, so every shape routes here and the no-key behavior cannot differ
// between them.
Executor EmitScalarAggregation(const AggregationPlan& plan,
                               TransactionContext& ctx) {
  const std::vector<NamedExpression>& aggregates = plan.Aggregates();
  // ParallelAggregationExecutor keeps only streaming scalar state
  // (PartialState has no per-aggregate row buffer), so aggregates that need
  // whole-group context — HAVING MIN/MAX, inner ORDER BY/LIMIT, a secondary
  // argument — or kinds with no parallel kernel (ARRAY_AGG, STRING_AGG,
  // ANY_VALUE, BIT_*, COUNTIF, approx sketches) must stay on the serial
  // AggregationExecutor, where AggregateAccumulator buffers and replays.
  const bool parallel_safe =
      std::ranges::all_of(aggregates, [](const NamedExpression& named) {
        if (!named.expression ||
            named.expression->Type() != TypeTag::kAggregateExp) {
          return false;
        }
        const auto& agg = named.expression->AsAggregateExpression();
        if (agg.Having() != AggregateHavingModifier::kNone ||
            !agg.InnerOrderBy().empty() || agg.InnerLimit().has_value() ||
            agg.SecondaryArg() || agg.HasInnerGroupBy()) {
          return false;
        }
        switch (agg.GetType()) {
          case AggregationType::kCount:
          case AggregationType::kSum:
          case AggregationType::kAvg:
          case AggregationType::kMin:
          case AggregationType::kMax:
          case AggregationType::kLogicalAnd:
          case AggregationType::kLogicalOr:
            return agg.TrailingArgs().empty();
          default:
            return IsStatisticalAggregate(agg.GetType());
        }
      });
  if (parallel_safe &&
      plan.Child()->EmitRowCount() >= kParallelAggregationMinRows) {
    // Cap workers like the parallel table scan path; unbounded
    // hardware_concurrency() oversubscribes under nested or concurrent
    // aggregations.
    constexpr size_t kMaxParallelAggregationWorkers = 16;
    const size_t workers =
        std::min(kMaxParallelAggregationWorkers,
                 std::max<size_t>(1, std::thread::hardware_concurrency()));
    return std::make_shared<ParallelAggregationExecutor>(
        plan.Child()->EmitExecutor(ctx), plan.Child()->GetSchema(), aggregates,
        workers);
  }
  return std::make_shared<AggregationExecutor>(
      plan.Child()->EmitExecutor(ctx), plan.Child()->GetSchema(), aggregates);
}
}  // namespace

Executor SortPlan::EmitExecutor(TransactionContext& ctx) const {
  // The base node predates the shape split, so it keeps the historical wiring
  // byte for byte: no shape bound, the operator's own spill selection only.
  return std::make_shared<SortExecutor>(
      Child()->EmitExecutor(ctx), Child()->GetSchema(),
      ExecutorSortKeys(Keys()), 1, SortExecutor::Options{});
}

Executor BufferedSortPlan::EmitExecutor(TransactionContext& ctx) const {
  // In-memory sort bounded by the shape's memory budget. It never plans a
  // spill, but it keeps the operator's budget-driven fallback so a wrong row
  // estimate cannot change the answer.
  SortExecutor::Options options = SortExecutorOptions(*this);
  options.spill_mode = SortExecutor::SpillMode::kBuffered;
  return std::make_shared<SortExecutor>(Child()->EmitExecutor(ctx),
                                        Child()->GetSchema(),
                                        ExecutorSortKeys(Keys()), 1, options);
}

Executor ExternalMergeSortPlan::EmitExecutor(TransactionContext& ctx) const {
  // Sorted runs are written once the shape's row threshold is reached, and the
  // tail run is always written, so the working set stays bounded by the memory
  // budget however large the input is.
  SortExecutor::Options options = SortExecutorOptions(*this);
  options.spill_mode = SortExecutor::SpillMode::kExternalMerge;
  options.spill_threshold_rows = SpillThresholdRows();
  return std::make_shared<SortExecutor>(Child()->EmitExecutor(ctx),
                                        Child()->GetSchema(),
                                        ExecutorSortKeys(Keys()), 1, options);
}

Executor TopNPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<TopNExecutor::Key> keys;
  keys.reserve(Keys().size());
  for (const TopNKey& key : Keys()) {
    keys.push_back(TopNExecutor::Key{.expression = key.expression,
                                     .ascending = key.ascending,
                                     .nulls_first = key.nulls_first,
                                     .collation = key.collation});
  }
  return std::make_shared<TopNExecutor>(Child()->EmitExecutor(ctx),
                                        Child()->GetSchema(), std::move(keys),
                                        Limit(), Offset(), WithTies());
}

Executor DistinctPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<DistinctExecutor>(child_->EmitExecutor(ctx),
                                            child_->GetSchema(), distinct_on_);
}

Executor ValuesPlan::EmitExecutor(TransactionContext& /*ctx*/) const {
  return std::make_shared<ValuesExecutor>(Rows());
}

Executor DummyScanPlan::EmitExecutor(TransactionContext& /*ctx*/) const {
  return std::make_shared<ValuesExecutor>(std::vector<Row>{Row({})});
}

Executor LsmScanPlan::EmitExecutor(TransactionContext& /*ctx*/) const {
  // The LSM scan needs no transaction context: the tree's own snapshot view
  // is the consistency boundary (no MVCC/row locks).
  return std::make_shared<LsmScan>(tree_);
}

Executor IndexSkipScanPlan::EmitExecutor(TransactionContext& txn) const {
  return std::make_shared<IndexSkipScanExecutor>(inner_->EmitExecutor(txn));
}

Executor SkipScanDistinctPlan::EmitExecutor(TransactionContext& txn) const {
  Executor scan = std::make_shared<SkipScanDistinct>(
      txn.txn_, table_, index_, std::vector<Value>{}, std::vector<Value>{},
      ascending_, ConstantValueExp(Value(true)), table_.GetSchema(), 0);
  return std::make_shared<Projection>(select_items_, table_.GetSchema(),
                                      std::move(scan));
}

Executor SortDistinctPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<SortDistinctExecutor>(Child()->EmitExecutor(ctx));
}

Executor Max1RowPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Max1RowExecutor>(child_->EmitExecutor(ctx));
}

Executor MaterializePlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<MaterializeExecutor>(child_->EmitExecutor(ctx),
                                               child_->GetSchema());
}

Executor MinMaxIndexPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<MinMaxIndexExecutor>(child_->EmitExecutor(ctx),
                                               ValueSlot());
}

Executor IncrementalSortPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<SortExecutor::Key> prefix;
  std::vector<SortExecutor::Key> suffix;
  prefix.reserve(prefix_keys_.size());
  suffix.reserve(suffix_keys_.size());
  for (const SortKey& key : prefix_keys_) {
    prefix.push_back(SortExecutor::Key{.expression = key.expression,
                                       .ascending = key.ascending,
                                       .nulls_first = key.nulls_first,
                                       .collation = key.collation});
  }
  for (const SortKey& key : suffix_keys_) {
    suffix.push_back(SortExecutor::Key{.expression = key.expression,
                                       .ascending = key.ascending,
                                       .nulls_first = key.nulls_first,
                                       .collation = key.collation});
  }
  return std::make_shared<IncrementalSortExecutor>(
      child_->EmitExecutor(ctx), child_->GetSchema(), std::move(prefix),
      std::move(suffix));
}

Executor SetOperationPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<Executor> children;
  children.reserve(Children().size());
  for (const Plan& child : Children()) {
    children.push_back(child->EmitExecutor(ctx));
  }
  if (Operation() == SetOperationKind::kUnionAll && !OrderKeys().empty()) {
    std::vector<Schema> schemas;
    schemas.reserve(Children().size());
    for (const Plan& child : Children()) {
      schemas.push_back(child->GetSchema());
    }
    std::vector<SortExecutor::Key> keys;
    keys.reserve(OrderKeys().size());
    for (const SortKey& key : OrderKeys()) {
      keys.push_back(SortExecutor::Key{.expression = key.expression,
                                       .ascending = key.ascending,
                                       .nulls_first = key.nulls_first,
                                       .collation = key.collation});
    }
    return std::make_shared<MergeAppendExecutor>(
        std::move(children), std::move(schemas), GetSchema(), std::move(keys));
  }
  return std::make_shared<SetOperationExecutor>(std::move(children),
                                                Operation());
}

Executor AggregationPlan::EmitExecutor(TransactionContext& ctx) const {
  return EmitScalarAggregation(*this, ctx);
}

Executor HashAggregatePlan::EmitExecutor(TransactionContext& ctx) const {
  if (GroupKeys().empty()) {
    return EmitScalarAggregation(*this, ctx);
  }
  // Hash table over the group keys. Workers are only asked for above the
  // parallel-aggregation threshold, and the operator itself refuses them when
  // the payload is not positionally foldable.
  GroupedAggregateOptions options = GroupedAggregateOptionsFor(*this);
  options.worker_count = GroupedAggregateWorkers(Child()->EmitRowCount());
  return std::make_shared<HashAggregateExecutor>(
      Child()->EmitExecutor(ctx), Child()->GetSchema(), GroupKeys(),
      Aggregates(), Having(), options);
}

Executor SortAggregatePlan::EmitExecutor(TransactionContext& ctx) const {
  if (GroupKeys().empty()) {
    return EmitScalarAggregation(*this, ctx);
  }
  // Sorted runs over the group keys, then one pass over the equal runs. The
  // run threshold is the shape's memory contract, so the sort plans its own
  // spill instead of discovering one.
  GroupedAggregateOptions options = GroupedAggregateOptionsFor(*this);
  options.spill_threshold_rows = SpillThresholdRows();
  return std::make_shared<SortAggregateExecutor>(
      Child()->EmitExecutor(ctx), Child()->GetSchema(), GroupKeys(),
      Aggregates(), Having(), options);
}

Executor StreamAggregatePlan::EmitExecutor(TransactionContext& ctx) const {
  if (GroupKeys().empty()) {
    return EmitScalarAggregation(*this, ctx);
  }
  // One group resident, no hash table and no sort — valid only over an input
  // that already arrives in group-key order, which the shape verifies itself.
  GroupedAggregateOptions options = GroupedAggregateOptionsFor(*this);
  options.require_ordered_input = true;
  return std::make_shared<StreamAggregateExecutor>(
      Child()->EmitExecutor(ctx), Child()->GetSchema(), GroupKeys(),
      Aggregates(), Having(), options);
}

Executor IndexScanPlan::EmitExecutor(TransactionContext& txn) const {
  std::vector<Value> begin_key = begin_key_;
  std::vector<Value> end_key = end_key_;
  auto point_ranges = point_ranges_;
  if (ContainsParameterSlot(where_)) {
    // Seek keys were sniffed at fill time. Rebuild them from the predicate
    // after Bind() so a different literal still lands on the right rows.
    // An empty rebuild scans without a seek; the residual predicate remains
    // authoritative.
    const ReboundIndexKeys rebound = ReboundIndexKeysFromPredicate(
        where_, table_.GetSchema(), index_, !point_ranges_.empty());
    if (!point_ranges_.empty()) {
      point_ranges = rebound.point_ranges;
      if (point_ranges.empty()) {
        begin_key.clear();
        end_key.clear();
      }
    } else {
      begin_key = rebound.begin_key;
      end_key = rebound.end_key;
      point_ranges.clear();
    }
  }
  auto stale_fallback = [&]() -> Executor {
    // Fallback route scans the table directly; Selection requires a real
    // predicate, so pass the plain scan through when there is none.  The
    // lock flags survive the fallback: a DML source scan still needs its
    // head-version resolution when the index cannot serve it.
    Executor scan = std::make_shared<FullScan>(
        txn.txn_, table_, std::numeric_limits<size_t>::max(), lock_rows_,
        wait_for_write_intent_);
    if (where_) {
      scan = std::make_shared<Selection>(where_, table_.GetSchema(),
                                         std::move(scan));
    }
    // DML source scans (lock_rows_) need row positions, which a sort does
    // not carry, and never depend on the key order.
    if (lock_rows_) {
      return scan;
    }
    return SortedByIndexKey(std::move(scan), table_.GetSchema(), table_, index_,
                            ascending_);
  };
  if (txn.txn_.IndexKeysMayBeStale(index_.Root())) {
    return stale_fallback();
  }
  auto index_read_guard = txn.txn_.AcquireIndexReadGuard(index_.Root());
  if (txn.txn_.IndexKeysMayBeStale(index_.Root())) {
    return stale_fallback();
  }
  if (!point_ranges.empty()) {
    return std::make_shared<IndexScan>(
        txn.txn_, table_, index_, point_ranges, ascending_, where_, GetSchema(),
        lock_rows_, wait_for_write_intent_, std::move(index_read_guard));
  }
  // RangePartitionedScan / ParallelIndexScan: a large ascending single-range
  // scan partitions at B+Tree leaf boundaries and scans the partitions in
  // parallel.  The morsel-ordered handoff keeps the key order, so the plan's
  // ordering claims stay valid; the residual predicate rides a Selection
  // above the scan because AST evaluation is single-threaded.  DML source
  // scans (lock_rows_) stay serial.
  static const bool parallel_index_scan_disabled =
      std::getenv("TINYLAMB_DISABLE_PARALLEL_INDEX_SCAN") != nullptr;
  if (!parallel_index_scan_disabled && ascending_ && !lock_rows_ &&
      EmitRowCount() >= kParallelIndexScanMinRows) {
    constexpr size_t kMaxParallelIndexScanWorkers = 16;
    constexpr size_t kLeavesPerIndexMorsel = 8;
    const size_t workers =
        std::min(kMaxParallelIndexScanWorkers,
                 std::max<size_t>(1, std::thread::hardware_concurrency()));
    Executor scan = std::make_shared<ParallelIndexScan>(
        txn.txn_, table_, index_, begin_key, end_key, ascending_, workers,
        kLeavesPerIndexMorsel, std::move(index_read_guard));
    if (where_) {
      scan = std::make_shared<Selection>(where_, table_.GetSchema(),
                                         std::move(scan));
    }
    return scan;
  }
  return std::make_shared<IndexScan>(txn.txn_, table_, index_, begin_key,
                                     end_key, ascending_, where_, GetSchema(),
                                     lock_rows_, wait_for_write_intent_,
                                     std::move(index_read_guard));
}

Executor BitmapScanPlan::EmitExecutor(TransactionContext& context) const {
  if (ContainsParameterSlot(where_)) {
    // Bitmap ranges are sniffed constants. A full scan plus the residual
    // predicate stays correct for every later binding.
    Executor scan = std::make_shared<FullScan>(context.txn_, table_);
    return where_ ? std::make_shared<Selection>(where_, table_.GetSchema(),
                                                std::move(scan))
                  : scan;
  }
  auto stale_fallback = [&]() -> Executor {
    Executor scan = std::make_shared<FullScan>(context.txn_, table_);
    return where_ ? std::make_shared<Selection>(where_, table_.GetSchema(),
                                                std::move(scan))
                  : scan;
  };
  std::vector<TransactionManager::IndexReadGuard> index_read_guards;
  index_read_guards.reserve(ranges_.size());
  std::vector<RowPosition> positions;
  bool first = true;
  for (const BitmapIndexRange& range : ranges_) {
    if (context.txn_.IndexKeysMayBeStale(range.index->Root())) {
      return stale_fallback();
    }
    auto index_read_guard =
        context.txn_.AcquireIndexReadGuard(range.index->Root());
    if (context.txn_.IndexKeysMayBeStale(range.index->Root())) {
      return stale_fallback();
    }
    index_read_guards.emplace_back(std::move(index_read_guard));
    std::vector<RowPosition> next =
        BitmapIndexScan(context.txn_, table_, *range.index, range.begin_key,
                        range.end_key, &index_read_guards.back())
            .ScanPositions();
    if (first) {
      positions = std::move(next);
      first = false;
    } else if (combine_ == BitmapCombine::kAnd) {
      positions = BitmapAnd(positions, next);
    } else {
      positions = BitmapOr(positions, next);
    }
  }
  return std::make_shared<BitmapHeapScan>(
      context.txn_, table_, std::move(positions), where_, table_.GetSchema(),
      combine_ == BitmapCombine::kAnd ? "BitmapAnd" : "BitmapOr");
}

Executor IndexOnlyScanPlan::EmitExecutor(TransactionContext& txn) const {
  std::vector<Value> begin_key = begin_key_;
  std::vector<Value> end_key = end_key_;
  if (ContainsParameterSlot(where_)) {
    const ReboundIndexKeys rebound = ReboundIndexKeysFromPredicate(
        where_, table_.GetSchema(), index_, /*want_points=*/false);
    begin_key = rebound.begin_key;
    end_key = rebound.end_key;
  }
  auto stale_fallback = [&] {
    Executor executor = std::make_shared<FullScan>(txn.txn_, table_);
    if (where_) {
      executor = std::make_shared<Selection>(where_, table_.GetSchema(),
                                             std::move(executor));
    }
    std::vector<NamedExpression> columns;
    columns.reserve(index_.sc_.key_.size() + index_.sc_.include_.size());
    for (slot_t offset : index_.sc_.key_) {
      columns.emplace_back(table_.GetSchema().GetColumn(offset).Name());
    }
    for (slot_t offset : index_.sc_.include_) {
      columns.emplace_back(table_.GetSchema().GetColumn(offset).Name());
    }
    std::vector<Column> projected_columns;
    projected_columns.reserve(columns.size());
    for (slot_t offset : index_.sc_.key_) {
      projected_columns.push_back(table_.GetSchema().GetColumn(offset));
    }
    for (slot_t offset : index_.sc_.include_) {
      projected_columns.push_back(table_.GetSchema().GetColumn(offset));
    }
    Executor projected = std::make_shared<Projection>(
        std::move(columns), table_.GetSchema(), std::move(executor));
    return SortedByIndexKey(
        std::move(projected),
        Schema(table_.GetSchema().Name(), std::move(projected_columns)), table_,
        index_, ascending_);
  };
  if (txn.txn_.IndexKeysMayBeStale(index_.Root())) {
    return stale_fallback();
  }
  auto index_read_guard = txn.txn_.AcquireIndexReadGuard(index_.Root());
  if (txn.txn_.IndexKeysMayBeStale(index_.Root())) {
    return stale_fallback();
  }
  return std::make_shared<IndexOnlyScan>(
      txn.txn_, table_, index_, begin_key, end_key, ascending_, where_,
      table_.GetSchema(), std::move(index_read_guard));
}

Executor MultiIndexCoveringScanPlan::EmitExecutor(
    TransactionContext& ctx) const {
  // Full scan + residual predicate + projection to the covered layout: what
  // every bail-out below returns.  It answers exactly the same rows (the
  // predicate decides) and only gives up the heap-fetch avoidance.
  auto full_scan_fallback = [&]() -> Executor {
    Executor scan = std::make_shared<FullScan>(ctx.txn_, table_);
    if (where_) {
      scan = std::make_shared<Selection>(where_, table_.GetSchema(),
                                         std::move(scan));
    }
    std::vector<NamedExpression> columns;
    columns.reserve(output_schema_.ColumnCount());
    for (size_t i = 0; i < output_schema_.ColumnCount(); ++i) {
      columns.emplace_back(output_schema_.GetColumn(i).Name());
    }
    bool same_layout = columns.size() == table_.GetSchema().ColumnCount();
    for (size_t i = 0; same_layout && i < columns.size(); ++i) {
      same_layout = columns[i].expression->Type() == TypeTag::kColumnValue &&
                    columns[i].expression->AsColumnValue().GetColumnName() ==
                        table_.GetSchema().GetColumn(i).Name();
    }
    if (same_layout) {
      return scan;
    }
    return std::make_shared<Projection>(std::move(columns), table_.GetSchema(),
                                        std::move(scan));
  };
  // The compiled-plan cache rewrites literals into ParameterSlots, so at
  // emit time the sniffed participant ranges are stale by construction.
  // Rebuild each participant's seek keys from the BOUND predicate -- the same
  // rebind IndexOnlyScanPlan::EmitExecutor does -- instead of falling back:
  // without this the node is dead code for every SQL statement.  A
  // participant whose leading key lost its bound (a NULL constant, a
  // cross-typed literal, or no range at all) has no narrower walk to promise,
  // so the whole node falls back rather than scan an unbounded index.
  std::vector<BitmapIndexRange> participants = ranges_;
  if (ContainsParameterSlot(where_)) {
    participants.clear();
    for (const BitmapIndexRange& range : ranges_) {
      const ReboundIndexKeys rebound = ReboundIndexKeysFromPredicate(
          where_, table_.GetSchema(), *range.index, /*want_points=*/false);
      if (rebound.begin_key.empty() && rebound.end_key.empty()) {
        return full_scan_fallback();
      }
      BitmapIndexRange participant;
      participant.index = range.index;
      participant.begin_key = std::move(rebound.begin_key);
      participant.end_key = std::move(rebound.end_key);
      participants.push_back(std::move(participant));
    }
  }
  // Acquire every participant's read guard in one global order (sorted by
  // index root, deduplicated) so two concurrent multi-index scans cannot
  // interleave their acquisitions into a writer-starvation deadlock.
  std::vector<page_id_t> roots;
  roots.reserve(participants.size());
  for (const BitmapIndexRange& range : participants) {
    roots.push_back(range.index->Root());
  }
  std::ranges::sort(roots);
  roots.erase(std::unique(roots.begin(), roots.end()), roots.end());
  std::vector<std::pair<page_id_t, TransactionManager::IndexReadGuard>> held;
  held.reserve(roots.size());
  for (const page_id_t root : roots) {
    if (ctx.txn_.IndexKeysMayBeStale(root)) {
      return full_scan_fallback();
    }
    auto guard = ctx.txn_.AcquireIndexReadGuard(root);
    if (ctx.txn_.IndexKeysMayBeStale(root)) {
      return full_scan_fallback();
    }
    held.emplace_back(root, std::move(guard));
  }
  // Hand each participant the guard for its own index root; the scan keeps
  // them alive for its lifetime, which is what makes the index-only reads
  // sound.
  std::vector<TransactionManager::IndexReadGuard> guards;
  guards.reserve(participants.size());
  for (const BitmapIndexRange& range : participants) {
    const page_id_t root = range.index->Root();
    const auto found = std::ranges::find_if(
        held, [root](const auto& entry) { return entry.first == root; });
    CHECK_MSG(found != held.end(), "multi-index covering scan guard missing");
    guards.push_back(std::move(found->second));
  }
  return std::make_shared<MultiIndexCoveringScan>(
      ctx.txn_, table_, std::move(participants), std::move(guards), where_,
      covered_slots_, output_schema_);
}

Executor RelationRenamePlan::EmitExecutor(TransactionContext& ctx) const {
  // Rows are positional: streaming the child through unchanged is exactly a
  // rename, and row positions survive for UPDATE/DELETE consumers. The
  // wrapper exists so EXPLAIN surfaces the rename boundary.
  return std::make_shared<RelationRenameExecutor>(src_->EmitExecutor(ctx),
                                                  relation_, physical_);
}

Executor EmptyPlan::EmitExecutor(TransactionContext& /*ctx*/) const {
  return std::make_shared<EmptyResultExecutor>();
}

namespace {

// Resolves join key column names into child schema offsets. The optimizer
// resolves keys against these schemas, so a missing column is an invariant
// violation: silently shrinking the offset vectors would mis-align HashJoin
// keys and return wrong join results.
void BuildKeyOffsets(const Schema& schema,
                     const std::vector<ColumnName>& columns,
                     std::vector<slot_t>* offsets) {
  offsets->reserve(columns.size());
  for (const auto& col : columns) {
    bool found = false;
    // Exactly one offset per key: a child projection may carry the same
    // column twice (`[x.k, x.b, x.b]` when two correlation equalities both
    // bind x.b), and emitting every match made the key vectors of the two
    // sides differ in length, so the hash join compared misaligned keys.
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).Name() == col) {
        offsets->push_back(static_cast<slot_t>(i));
        found = true;
        break;
      }
    }
    if (!found) {
      CHECK_MSG(false,
                "ProductPlan: join key column not found: " + col.ToString());
    }
  }
}

const Index* ZigZagIndex(const Plan& plan) {
  if (const auto* scan = dynamic_cast<const IndexScanPlan*>(plan.get())) {
    return &scan->GetIndex();
  }
  if (const auto* scan = dynamic_cast<const IndexOnlyScanPlan*>(plan.get())) {
    return &scan->GetIndex();
  }
  if (const auto* scan = dynamic_cast<const IndexSkipScanPlan*>(plan.get())) {
    return ZigZagIndex(scan->GetSource());
  }
  return nullptr;
}

}  // namespace

Executor MergeJoinPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<slot_t> left;
  std::vector<slot_t> right;
  BuildKeyOffsets(Left()->GetSchema(), LeftKeys(), &left);
  BuildKeyOffsets(Right()->GetSchema(), RightKeys(), &right);
  // The residual is evaluated on concatenated rows, so it always sees the
  // combined schema even when the join output keeps only the probe side
  // (semi/anti).
  Schema residual_schema =
      Residual() ? Left()->GetSchema() + Right()->GetSchema() : Schema();
  // Inner equi merge joins without residuals go parallel past the
  // threshold (TODO.md item 5b): the steering partitioner needs enough rows
  // to amortize thread startup. Outer/semi/anti and residual shapes keep
  // the proven sequential executor.
  if (Kind() == JoinKind::kInner && !Residual() &&
      Left()->EmitRowCount() + Right()->EmitRowCount() >=
          kParallelMergeJoinMinRows) {
    const size_t workers = std::min<size_t>(
        16, std::max<size_t>(1, std::thread::hardware_concurrency()));
    return std::make_shared<ParallelMergeJoin>(
        Left()->EmitExecutor(ctx), std::move(left), Right()->EmitExecutor(ctx),
        std::move(right), workers, Kind(), Expression(), Schema(),
        Right()->GetSchema().ColumnCount());
  }
  return std::make_shared<MergeJoin>(
      Left()->EmitExecutor(ctx), std::move(left), Right()->EmitExecutor(ctx),
      std::move(right), Kind(), Left()->GetSchema().ColumnCount(),
      Right()->GetSchema().ColumnCount(), Residual(),
      std::move(residual_schema));
}

Executor BandJoinPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<slot_t> left;
  std::vector<slot_t> right;
  BuildKeyOffsets(Left()->GetSchema(), LeftKeys(), &left);
  BuildKeyOffsets(Right()->GetSchema(), RightKeys(), &right);
  const Schema combined = Left()->GetSchema() + Right()->GetSchema();
  const Schema output_schema = IsSemiJoinKind(Kind()) ||
                                       IsAntiJoinKind(Kind()) ||
                                       IsNullAwareAntiJoinKind(Kind())
                                   ? Left()->GetSchema()
                                   : combined;
  return std::make_shared<BandJoin>(
      Left()->EmitExecutor(ctx), std::move(left), Right()->EmitExecutor(ctx),
      std::move(right), Comparison(), Predicate(), combined, Kind(),
      Left()->GetSchema().ColumnCount(), Right()->GetSchema().ColumnCount(),
      output_schema);
}

Executor LateMaterializationJoinPlan::EmitExecutor(
    TransactionContext& ctx) const {
  CHECK_MSG(RightTable() != nullptr,
            "LateMaterializationJoinPlan requires a right table");
  const Schema& right_physical = RightTable()->GetSchema();
  CHECK_MSG(GetSchema().ColumnCount() > Left()->GetSchema().ColumnCount(),
            "LateMaterializationJoinPlan requires a right payload schema");
  std::vector<Column> right_columns;
  right_columns.reserve(GetSchema().ColumnCount() -
                        Left()->GetSchema().ColumnCount());
  for (size_t i = Left()->GetSchema().ColumnCount();
       i < GetSchema().ColumnCount(); ++i) {
    right_columns.push_back(GetSchema().GetColumn(i));
  }
  const Schema right_output("", std::move(right_columns));
  CHECK_MSG(RightProjection().size() == right_output.ColumnCount(),
            "LateMaterializationJoinPlan payload width mismatch");
  const auto fallback = [&] {
    Executor right = std::make_shared<FullScan>(ctx.txn_, *RightTable());
    bool identity_projection =
        RightProjection().size() == right_physical.ColumnCount();
    for (size_t i = 0; identity_projection && i < RightProjection().size();
         ++i) {
      identity_projection = RightProjection()[i] == static_cast<slot_t>(i);
    }
    if (!identity_projection) {
      std::vector<NamedExpression> items;
      items.reserve(right_output.ColumnCount());
      for (size_t i = 0; i < right_output.ColumnCount(); ++i) {
        const slot_t source = RightProjection()[i];
        items.emplace_back(
            right_output.GetColumn(i).Name().ToString(),
            ColumnValueExp(right_physical.GetColumn(source).Name()));
      }
      right = std::make_shared<Projection>(std::move(items), right_physical,
                                           std::move(right));
    }
    return std::make_shared<NestedLoopJoin>(
        Left()->EmitExecutor(ctx), Left()->GetSchema(), std::move(right),
        right_output, Predicate());
  };
  if (Identity() == LateMaterializationIdentity::kRowId) {
    return std::make_shared<LateMaterializationJoin>(
        ctx.txn_, Left()->EmitExecutor(ctx), *RightTable(), GetSchema(),
        RowId(), RightProjection(), Predicate());
  }
  if (RightIndex() == nullptr) {
    return fallback();
  }
  std::vector<slot_t> left;
  std::vector<slot_t> right;
  BuildKeyOffsets(Left()->GetSchema(), LeftKeys(), &left);
  BuildKeyOffsets(right_physical, RightKeys(), &right);
  if (ctx.txn_.IndexKeysMayBeStale(RightIndex()->Root())) {
    return fallback();
  }
  auto guard = ctx.txn_.AcquireIndexReadGuard(RightIndex()->Root());
  return std::make_shared<LateMaterializationJoin>(
      ctx.txn_, Left()->EmitExecutor(ctx), std::move(left), *RightTable(),
      *RightIndex(), std::move(right), GetSchema(), Predicate(), UniqueWinner(),
      RightProjection(), std::move(guard));
}

Executor ZigZagJoinPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<slot_t> left;
  std::vector<slot_t> right;
  BuildKeyOffsets(Left()->GetSchema(), {LeftKey()}, &left);
  BuildKeyOffsets(Right()->GetSchema(), {RightKey()}, &right);

  const Index* left_index = ZigZagIndex(Left());
  const Index* right_index = ZigZagIndex(Right());
  if ((left_index != nullptr &&
       ctx.txn_.IndexKeysMayBeStale(left_index->Root())) ||
      (right_index != nullptr &&
       ctx.txn_.IndexKeysMayBeStale(right_index->Root()))) {
    return std::make_shared<NestedLoopJoin>(
        Left()->EmitExecutor(ctx), Left()->GetSchema(),
        Right()->EmitExecutor(ctx), Right()->GetSchema(), Predicate());
  }

  Executor left_executor = Left()->EmitExecutor(ctx);
  Executor right_executor = Right()->EmitExecutor(ctx);
  if ((left_index != nullptr &&
       ctx.txn_.IndexKeysMayBeStale(left_index->Root())) ||
      (right_index != nullptr &&
       ctx.txn_.IndexKeysMayBeStale(right_index->Root()))) {
    return std::make_shared<NestedLoopJoin>(
        std::move(left_executor), Left()->GetSchema(),
        std::move(right_executor), Right()->GetSchema(), Predicate());
  }
  return std::make_shared<ZigZagJoin>(std::move(left_executor), left.front(),
                                      std::move(right_executor), right.front(),
                                      Operation(), Predicate(), GetSchema());
}

Executor ProductPlan::EmitExecutor(TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {
    // A cross product filtered by the full join predicate is a nested-loop
    // join; lower it to the executor that evaluates the predicate per pair.
    // LeftOuter carries the kind through so unmatched outer rows are
    // null-padded by the executor; RIGHT is normalized to LEFT by the
    // optimizer and FULL has hash/merge implementations.
    if (residual_note_ && !IsSemiJoinKind(kind_) && !IsAntiJoinKind(kind_)) {
      if (kind_ != JoinKind::kInner && !IsLeftOuterJoinKind(kind_)) {
        // Full/right-outer residuals have no tuple nested-loop
        // implementation; the blocked executor null-pads unmatched rows on
        // either side. Only the batch_nested_loop rule constructs this
        // shape, so any other kind here is still a planner bug.
        if (IsFullOuterJoinKind(kind_) || IsRightOuterJoinKind(kind_)) {
          return std::make_shared<BatchNestedLoopJoin>(
              left_src_->EmitExecutor(ctx), left_src_->GetSchema(),
              right_src_->EmitExecutor(ctx), right_src_->GetSchema(),
              residual_note_, kind_);
        }
        // Falling through to CrossJoin would silently DROP the residual and
        // emit an unfiltered product; no current rule constructs this shape,
        // so a residual with an unsupported kind is a planner bug. Fail
        // loudly (programmer-error category) instead of returning wrong rows.
        CHECK_MSG(false,
                  "ProductPlan: residual predicate on unsupported "
                  "join kind (expected INNER/LEFT OUTER)");
      }
      // The batch_nested_loop rule marks inner joins whose predicate has no
      // equi pair for the blocked executor; the tuple executor stays the
      // default so existing plans are unchanged.
      if (batch_nested_loop_) {
        return std::make_shared<BatchNestedLoopJoin>(
            left_src_->EmitExecutor(ctx), left_src_->GetSchema(),
            right_src_->EmitExecutor(ctx), right_src_->GetSchema(),
            residual_note_, kind_);
      }
      return std::make_shared<NestedLoopJoin>(
          left_src_->EmitExecutor(ctx), left_src_->GetSchema(),
          right_src_->EmitExecutor(ctx), right_src_->GetSchema(),
          residual_note_, kind_);
    }
    // Semi/anti residuals have no tuple nested-loop implementation either:
    // falling through to CrossJoin would drop the predicate and emit an
    // unfiltered product. The blocked executor evaluates the predicate per
    // pair with match tracking. Null-aware anti carries three-valued
    // short-circuit semantics the blocked executor does not implement; no
    // rule constructs that shape (the batch rule only fires without an
    // equi pair, and null-aware arises from single-key equi NOT IN).
    if (residual_note_ && (IsSemiJoinKind(kind_) || IsAntiJoinKind(kind_))) {
      CHECK_MSG(!IsNullAwareAntiJoinKind(kind_),
                "ProductPlan: residual predicate on null-aware anti join");
      return std::make_shared<BatchNestedLoopJoin>(
          left_src_->EmitExecutor(ctx), left_src_->GetSchema(),
          right_src_->EmitExecutor(ctx), right_src_->GetSchema(),
          residual_note_, kind_);
    }
    // Cross Join
    return std::make_shared<CrossJoin>(left_src_->EmitExecutor(ctx),
                                       right_src_->EmitExecutor(ctx));
  }
  std::vector<slot_t> left;
  std::vector<slot_t> right;
  BuildKeyOffsets(left_src_->GetSchema(), left_cols_, &left);
  const Schema& right_schema =
      right_tbl_ != nullptr ? right_tbl_->GetSchema() : right_src_->GetSchema();
  BuildKeyOffsets(right_schema, right_cols_, &right);
  if (right_tbl_ != nullptr) {
    Executor left_executor = left_src_->EmitExecutor(ctx);
    auto index_read_guard = ctx.txn_.AcquireIndexReadGuard(right_idx_->Root());
    const bool any_null_safe =
        std::ranges::any_of(key_null_safe_, [](bool safe) { return safe; });
    // Index nested loop: the probe columns are resolved in the index's key
    // order, so the seek key is a real prefix of the stored key image even when
    // only some of a composite index's key columns are join keys.
    //
    // Three shapes have no index probe and lower to the hash join over a full
    // scan of the right table instead: a stale index read set, NULL-safe equi
    // keys (the index seek has no NULL-key path), and an absent derived probe
    // (no leading index key column is a join key, so there is no point lookup
    // to perform in the first place).
    const auto hash_over_full_scan = [&] {
      auto join = std::make_shared<HashJoin>(
          std::move(left_executor), left,
          std::make_shared<FullScan>(ctx.txn_, *right_tbl_), right, hash_mode_,
          kind_,
          std::min<size_t>(
              16, std::max<size_t>(1, std::thread::hardware_concurrency())),
          right_schema.ColumnCount(), left_src_->GetSchema().ColumnCount());
      join->SetJoinAnnotations(key_null_safe_, residual_note_);
      if (residual_note_ &&
          (kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kRightOuter ||
           kind_ == JoinKind::kFullOuter)) {
        join->SetJoinResidual(residual_note_,
                              left_src_->GetSchema() + right_schema);
      }
      return join;
    };
    if (ctx.txn_.IndexKeysMayBeStale(right_idx_->Root()) || any_null_safe ||
        !Lookup()) {
      return hash_over_full_scan();
    }
    const LookupSeek& seek = *Lookup();
    std::vector<slot_t> probe_left;
    std::vector<slot_t> probe_right;
    probe_left.reserve(seek.left_columns.size());
    probe_right.reserve(seek.right_columns.size());
    for (const ColumnName& column : seek.left_columns) {
      std::vector<slot_t> offset;
      BuildKeyOffsets(left_src_->GetSchema(), {column}, &offset);
      probe_left.push_back(offset.front());
    }
    for (const ColumnName& column : seek.right_columns) {
      std::vector<slot_t> offset;
      BuildKeyOffsets(right_schema, {column}, &offset);
      probe_right.push_back(offset.front());
    }
    // Interval probe: the second bound's value comes from a left column that
    // is not part of the seek key vectors, so it is resolved separately.
    std::optional<slot_t> second_range_column;
    if (seek.second_range_left_column) {
      std::vector<slot_t> offset;
      BuildKeyOffsets(left_src_->GetSchema(), {*seek.second_range_left_column},
                      &offset);
      second_range_column = offset.front();
    }
    return std::make_shared<LookupJoin>(
        ctx.txn_, std::move(left_executor), std::move(probe_left), *right_tbl_,
        *right_idx_, std::move(probe_right), seek.range_position,
        seek.range_operation, second_range_column, seek.second_range_operation,
        std::move(index_read_guard));
  }
  if (kind_ != JoinKind::kInner) {
    // Semi/anti and outer hash joins retain the logical join kind.  Widths
    // are explicit because one side may be empty, in which case the executor
    // cannot infer the number of NULL padding columns from a row.
    auto join = std::make_shared<HashJoin>(
        left_src_->EmitExecutor(ctx), left, right_src_->EmitExecutor(ctx),
        right, hash_mode_, kind_,
        std::min<size_t>(
            16, std::max<size_t>(1, std::thread::hardware_concurrency())),
        right_schema.ColumnCount(), left_src_->GetSchema().ColumnCount());
    join->SetJoinAnnotations(key_null_safe_, residual_note_);
    if (semi_reduction_) {
      // Phase 2 "Hash Semi Reduction": a semi/anti join emits only the probe
      // side, so its hash build is a pure membership test and the executor may
      // shrink it. Every other kind ignores the marker.
      join->EnableSemiReduction();
    }
    if (residual_note_ &&
        (kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kRightOuter ||
         kind_ == JoinKind::kFullOuter)) {
      join->SetJoinResidual(residual_note_,
                            left_src_->GetSchema() + right_schema);
    }
    return join;
  }
  // Dynamic filter pushdown (Phase 2 "Dynamic Filter Join / SIP / Bloom"):
  // the sequential executor below can order its intake build-side-first and
  // publish a sound filter over the build keys to the probe scan, which then
  // rejects non-members before row decode. It needs the sequential executor
  // (the shared-build parallel join has no filter contract), so the dynamic
  // filter shape and the parallel shape are mutually exclusive.
  const bool dynamic_filter = PrefersDynamicFilter();
  // Inner equi hash joins go parallel past the threshold (TODO.md item
  // 5b): workers share one concurrent build table, then probe concurrently.
  // The threshold mirrors the scan/aggregation ones (thread startup only
  // pays past ~8k rows). Semi/anti/outer shapes return above and keep their
  // sequential executors. Null-safe keys (IS NOT DISTINCT FROM) stay
  // sequential too: the parallel executor has no null-safe key encoding.
  const bool any_null_safe =
      std::ranges::any_of(key_null_safe_, [](bool safe) { return safe; });
  if (!dynamic_filter && !any_null_safe &&
      left_src_->EmitRowCount() + right_src_->EmitRowCount() >=
          kParallelHashJoinMinRows) {
    const size_t workers = std::min<size_t>(
        16, std::max<size_t>(1, std::thread::hardware_concurrency()));
    return std::make_shared<SharedBuildParallelHashJoin>(
        left_src_->EmitExecutor(ctx), left, right_src_->EmitExecutor(ctx),
        right, workers, JoinKind::kInner,
        right_src_->GetSchema().ColumnCount());
  }
  auto join = std::make_shared<HashJoin>(left_src_->EmitExecutor(ctx), left,
                                         right_src_->EmitExecutor(ctx), right,
                                         hash_mode_);
  join->SetJoinAnnotations(key_null_safe_, residual_note_);
  if (dynamic_filter) {
    join->EnableDynamicFilter(DynamicFilterBuildsLeft());
  }
  return join;
}

namespace relational_detail {

StatusOr<Relation> ApplyTableSample(TransactionContext& context,
                                    const SelectSource& source,
                                    Relation input) {
  if (!source.sample) {
    return input;
  }
  RETURN_IF_FAIL(input.FinishSpill());
  RETURN_IF_FAIL(input.GetStatus());
  std::vector<Row> rows;
  RETURN_IF_FAIL(
      input.ForEachRow([&](const Row& row) { rows.push_back(row); }));
  auto executor = std::make_shared<TableSampleExecutor>(
      context, std::make_shared<ConstantExecutor>(std::move(rows)),
      input.schema, *source.sample);
  Relation output(context.execution_runtime());
  output.schema = executor->OutputSchema();
  Row row;
  RowPosition position;
  while (executor->Next(&row, &position)) {
    output.AddRow(std::move(row));
  }
  if (executor->GetStatus() != Status::kSuccess) {
    return executor->GetStatus();
  }
  CopyExecutionStats(&output, input);
  RETURN_IF_FAIL(output.FinishSpill());
  RETURN_IF_FAIL(output.GetStatus());
  output.peak_intermediate_rows =
      std::max(output.peak_intermediate_rows, output.TotalRows());
  return output;
}

}  // namespace relational_detail

}  // namespace tinylamb
