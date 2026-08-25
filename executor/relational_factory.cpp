/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Relational factory (improvement3.md A5 second stage / V4): every
// PlanBase::EmitExecutor implementation lives here so the plan layer carries
// logical information only and never includes executor headers. Plans expose
// an opaque Executor handle (plan/plan.hpp forward declares ExecutorBase);
// this translation unit owns the concrete executors they build.

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/cross_join.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/index_join.hpp"
#include "executor/index_only_scan.hpp"
#include "executor/index_scan.hpp"
#include "executor/join_kind.hpp"
#include "executor/limit.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/projection.hpp"
#include "executor/relational.hpp"
#include "executor/selection.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "index/index.hpp"
#include "page/row_position.hpp"
#include "plan/aggregation_plan.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/limit_plan.hpp"
#include "plan/parallel_thresholds.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/relation_rename_plan.hpp"
#include "plan/relational_plan.hpp"
#include "plan/selection_plan.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

Executor RelationalPlan::EmitExecutor(TransactionContext& context) const {
  return std::make_shared<RelationalExecutor>(context, statement_);
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
    return src_->Next(dst, rp);
  }
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
  if (stats_.Rows() >= kParallelScanMinRows) {
    return std::make_shared<ParallelScan>(txn.txn_, table_);
  }
  return std::make_shared<FullScan>(txn.txn_, table_);
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
    if (!where_) { return scan;
}
    return std::make_shared<Selection>(where_, table_.GetSchema(),
                                       std::move(scan));
  }
  if (!point_ranges_.empty()) {
    return std::make_shared<IndexScan>(txn.txn_, table_, index_,
                                       point_ranges_, ascending_, where_,
                                       GetSchema(), lock_rows_,
                                       wait_for_write_intent_);
  }
  return std::make_shared<IndexScan>(txn.txn_, table_, index_, begin_key_,
                                     end_key_, ascending_, where_, GetSchema(),
                                     lock_rows_, wait_for_write_intent_);
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
    // Semi/Anti hash join (decorrelated IN / EXISTS / NOT EXISTS).
    return std::make_shared<HashJoin>(left_src_->EmitExecutor(ctx), left,
                                      right_src_->EmitExecutor(ctx), right,
                                      hash_mode_, kind_);
  }
  return std::make_shared<HashJoin>(left_src_->EmitExecutor(ctx), left,
                                    right_src_->EmitExecutor(ctx), right,
                                    hash_mode_);
}

}  // namespace tinylamb
