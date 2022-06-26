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
      begin_(std::move(begin)),
      end_(std::move(end)),
      ascending_(ascending),
      where_(where) {}

Executor IndexScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<IndexScan>(ctx.txn_, table_, index_, begin_, end_,
                                     ascending_, where_, GetSchema(ctx));
}

Schema IndexScanPlan::GetSchema(TransactionContext& ctx) const {
  ASSIGN_OR_CRASH(std::shared_ptr<Table>, tbl,
                  ctx.GetTable(table_.GetSchema().Name()));
  return tbl->GetSchema();
}

size_t IndexScanPlan::AccessRowCount(TransactionContext& txn) const {
  return EmitRowCount(txn) * 2;
}

size_t IndexScanPlan::EmitRowCount(TransactionContext& /*txn*/) const {
  if (index_.IsUnique()) {
    return 2;
  }
  double ret = 0;
  for (const slot_t& col : index_.sc_.key_) {
    ret = std::min(ret, stats_.EstimateCount(col, begin_, end_));
  }
  return std::ceil(ret);
}

void IndexScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << table_.GetSchema().Name();
}

}  // namespace tinylamb