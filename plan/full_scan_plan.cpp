//
// Created by kumagi on 2022/03/01.
//

#include "full_scan_plan.hpp"

#include "database/relation_storage.hpp"
#include "database/transaction_context.hpp"
#include "executor/full_scan.hpp"
#include "table/table.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(const Table& table, TableStatistics ts)
    : table_(&table), stats_(std::move(ts)) {}

Executor FullScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<FullScan>(ctx.txn_, *table_);
}

Schema FullScanPlan::GetSchema(TransactionContext& ctx) const {
  ASSIGN_OR_CRASH(std::shared_ptr<Table>, tbl,
                  ctx.GetTable(table_->GetSchema().Name()));
  return tbl->GetSchema();
}

size_t FullScanPlan::AccessRowCount(TransactionContext& /*txn*/) const {
  return stats_.row_count_;
}

size_t FullScanPlan::EmitRowCount(TransactionContext& /*txn*/) const {
  return stats_.row_count_;
}

void FullScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->GetSchema().Name();
}

}  // namespace tinylamb