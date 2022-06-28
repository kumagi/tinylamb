//
// Created by kumagi on 22/06/19.
//

#include "index_scan_plan.hpp"

#include <cmath>
#include <utility>

#include "database/relation_storage.hpp"
#include "database/transaction_context.hpp"
#include "executor/index_scan.hpp"
#include "index/index.hpp"
#include "table/table.hpp"

namespace tinylamb {

IndexScanPlan::IndexScanPlan(const Table& table, const Index& index,
                             const TableStatistics& ts, Value begin, Value end,
                             bool ascending, Expression where)
    : table_(table),
      index_(index),
      stats_(ts),
      begin_(begin),
      end_(end),
      ascending_(ascending),
      where_(std::move(where)) {}

Executor IndexScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<IndexScan>(ctx.txn_, table_, index_, begin_, end_,
                                     ascending_, where_, GetSchema());
}

const Schema& IndexScanPlan::GetSchema() const { return table_.GetSchema(); }

size_t IndexScanPlan::AccessRowCount() const { return EmitRowCount() * 2; }

size_t IndexScanPlan::EmitRowCount() const {
  if (index_.IsUnique() && begin_ == end_) {
    return 1;
  }
  return std::ceil(stats_.EstimateCount(index_.sc_.key_[0], begin_, end_));
}

void IndexScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << table_.GetSchema().Name()
    << " (estimated cost: " << AccessRowCount() << ")";
}

}  // namespace tinylamb