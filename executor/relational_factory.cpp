/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Relational factory (improvement3.md A5 second stage / V4): every
// PlanBase::EmitExecutor implementation lives here so the plan layer carries
// logical information only and never includes executor headers. Plans expose
// an opaque Executor handle (plan/plan.hpp forward declares ExecutorBase);
// this translation unit owns the concrete executors they build.

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/bitmap_scan.hpp"
#include "executor/constant_executor.hpp"
#include "executor/cross_join.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/incremental_sort.hpp"
#include "executor/index_join.hpp"
#include "executor/index_only_scan.hpp"
#include "executor/index_scan.hpp"
#include "executor/join_kind.hpp"
#include "executor/limit.hpp"
#include "executor/max1_row.hpp"
#include "executor/merge_append.hpp"
#include "executor/merge_join.hpp"
#include "executor/minmax_index.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/projection.hpp"
#include "executor/relational.hpp"
#include "executor/selection.hpp"
#include "executor/set_operation.hpp"
#include "executor/sort.hpp"
#include "executor/topn.hpp"
#include "executor/values.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "index/index.hpp"
#include "page/row_position.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/bitmap_scan_plan.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/empty_plan.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/group_by_plan.hpp"
#include "plan/incremental_sort_plan.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/limit_plan.hpp"
#include "plan/max1_row_plan.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/minmax_index_plan.hpp"
#include "plan/parallel_thresholds.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/set_operation_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/topn_plan.hpp"
#include "plan/values_plan.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

Executor RelationalPlan::EmitExecutor(TransactionContext& context) const {
  return std::make_shared<RelationalExecutor>(context, statement_);
}

Executor GroupByPlan::EmitExecutor(TransactionContext& context) const {
  return EmitGroupedFinishExecutor(context, child_, statement_);
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

  bool Next(Row* dst, RowPosition* rp) override { return src_->Next(dst, rp); }
  void Dump(std::ostream& o, int indent) const override;

 private:
  Executor src_;
  std::string relation_;
  std::string physical_;
};

void RelationRenameExecutor::Dump(std::ostream& o, int indent) const {
  o << "Rename: " << physical_ << " AS " << relation_ << "\n"
    << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

Executor FullScanPlan::EmitExecutor(TransactionContext& txn) const {
  if (MaxRows() == std::numeric_limits<size_t>::max() &&
      stats_.Rows() >= kParallelScanMinRows) {
    return std::make_shared<ParallelScan>(txn.txn_, table_,
                                          std::thread::hardware_concurrency(),
                                          8, std::nullopt, peek_compares_);
  }
  return std::make_shared<FullScan>(txn.txn_, table_, MaxRows());
}

Executor SelectionPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Selection>(exp_, src_->GetSchema(),
                                     src_->EmitExecutor(ctx));
}

Executor ProjectionPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Projection>(columns_, src_->GetSchema(),
                                      src_->EmitExecutor(ctx));
}

Executor LimitPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<LimitExecutor>(src_->EmitExecutor(ctx), limit_count_,
                                         limit_offset_);
}

Executor SortPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<SortExecutor::Key> keys;
  keys.reserve(Keys().size());
  for (const SortKey& key : Keys()) {
    keys.push_back(
        SortExecutor::Key{key.expression, key.ascending, key.nulls_first});
  }
  return std::make_shared<SortExecutor>(Child()->EmitExecutor(ctx),
                                        Child()->GetSchema(), std::move(keys));
}

Executor TopNPlan::EmitExecutor(TransactionContext& ctx) const {
  std::vector<TopNExecutor::Key> keys;
  keys.reserve(Keys().size());
  for (const TopNKey& key : Keys()) {
    keys.push_back(
        TopNExecutor::Key{key.expression, key.ascending, key.nulls_first});
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

Executor SortDistinctPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<SortDistinctExecutor>(Child()->EmitExecutor(ctx));
}

Executor Max1RowPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Max1RowExecutor>(child_->EmitExecutor(ctx));
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
    prefix.push_back(
        SortExecutor::Key{key.expression, key.ascending, key.nulls_first});
  }
  for (const SortKey& key : suffix_keys_) {
    suffix.push_back(
        SortExecutor::Key{key.expression, key.ascending, key.nulls_first});
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
      keys.push_back(
          SortExecutor::Key{key.expression, key.ascending, key.nulls_first});
    }
    return std::make_shared<MergeAppendExecutor>(
        std::move(children), std::move(schemas), GetSchema(), std::move(keys));
  }
  return std::make_shared<SetOperationExecutor>(std::move(children),
                                                Operation());
}

Executor AggregationPlan::EmitExecutor(TransactionContext& ctx) const {
  if (child_->EmitRowCount() >= kParallelAggregationMinRows) {
    // Cap workers like the parallel table scan path; unbounded
    // hardware_concurrency() oversubscribes under nested or concurrent
    // aggregations.
    constexpr size_t kMaxParallelAggregationWorkers = 16;
    const size_t workers =
        std::min(kMaxParallelAggregationWorkers,
                 std::max<size_t>(1, std::thread::hardware_concurrency()));
    return std::make_shared<ParallelAggregationExecutor>(
        child_->EmitExecutor(ctx), child_->GetSchema(), aggregates_, workers);
  }
  return std::make_shared<AggregationExecutor>(
      child_->EmitExecutor(ctx), child_->GetSchema(), aggregates_);
}

Executor IndexScanPlan::EmitExecutor(TransactionContext& txn) const {
  if (txn.txn_.IndexKeysMayBeStale(index_.Root())) {
    // Fallback route scans the table directly; Selection requires a real
    // predicate, so pass the plain scan through when there is none.
    Executor scan = std::make_shared<FullScan>(txn.txn_, table_);
    if (!where_) {
      return scan;
    }
    return std::make_shared<Selection>(where_, table_.GetSchema(),
                                       std::move(scan));
  }
  if (!point_ranges_.empty()) {
    return std::make_shared<IndexScan>(txn.txn_, table_, index_, point_ranges_,
                                       ascending_, where_, GetSchema(),
                                       lock_rows_, wait_for_write_intent_);
  }
  return std::make_shared<IndexScan>(txn.txn_, table_, index_, begin_key_,
                                     end_key_, ascending_, where_, GetSchema(),
                                     lock_rows_, wait_for_write_intent_);
}

Executor BitmapScanPlan::EmitExecutor(TransactionContext& context) const {
  for (const BitmapIndexRange& range : ranges_) {
    if (context.txn_.IndexKeysMayBeStale(range.index->Root())) {
      Executor scan = std::make_shared<FullScan>(context.txn_, table_);
      return where_ ? std::make_shared<Selection>(where_, table_.GetSchema(),
                                                  std::move(scan))
                    : scan;
    }
  }
  std::vector<RowPosition> positions;
  bool first = true;
  for (const BitmapIndexRange& range : ranges_) {
    std::vector<RowPosition> next =
        BitmapIndexScan(context.txn_, table_, *range.index, range.begin_key,
                        range.end_key)
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
  if (txn.txn_.IndexKeysMayBeStale(index_.Root())) {
    // Fallback route reads the table directly; Selection requires a real
    // predicate, so skip it when there is none.
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
    return std::make_shared<Projection>(std::move(columns), table_.GetSchema(),
                                        std::move(executor));
  }
  return std::make_shared<IndexOnlyScan>(txn.txn_, table_, index_, begin_key_,
                                         end_key_, ascending_, where_,
                                         table_.GetSchema());
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
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).Name() == col) {
        offsets->push_back(static_cast<slot_t>(i));
        found = true;
      }
    }
    if (!found) {
      throw std::runtime_error("ProductPlan: join key column not found: " +
                               col.ToString());
    }
  }
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
  return std::make_shared<MergeJoin>(
      Left()->EmitExecutor(ctx), std::move(left), Right()->EmitExecutor(ctx),
      std::move(right), Kind(), Left()->GetSchema().ColumnCount(),
      Right()->GetSchema().ColumnCount(), Residual(),
      std::move(residual_schema));
}

Executor ProductPlan::EmitExecutor(TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {
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
    // IndexJoin.
    return std::make_shared<IndexJoin>(ctx.txn_, left_src_->EmitExecutor(ctx),
                                       left, *right_tbl_, *right_idx_, right);
  }
  if (kind_ != JoinKind::kInner) {
    // Semi/anti and outer hash joins retain the logical join kind.  Widths
    // are explicit because one side may be empty, in which case the executor
    // cannot infer the number of NULL padding columns from a row.
    return std::make_shared<HashJoin>(
        left_src_->EmitExecutor(ctx), left, right_src_->EmitExecutor(ctx),
        right, hash_mode_, kind_, std::thread::hardware_concurrency(),
        right_schema.ColumnCount(), left_src_->GetSchema().ColumnCount());
  }
  return std::make_shared<HashJoin>(left_src_->EmitExecutor(ctx), left,
                                    right_src_->EmitExecutor(ctx), right,
                                    hash_mode_);
}

}  // namespace tinylamb
