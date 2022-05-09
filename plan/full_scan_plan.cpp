//
// Created by kumagi on 2022/03/01.
//

#include "full_scan_plan.hpp"

#include "database/relation_storage.hpp"
#include "database/transaction_context.hpp"
#include "executor/full_scan.hpp"
#include "table/table.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(std::string_view table_name, TableStatistics ts)
    : table_name_(table_name), stats_(std::move(ts)) {}

Executor FullScanPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<FullScan>(ctx.txn_, ctx.GetTable(table_name_));
}

Schema FullScanPlan::GetSchema(TransactionContext& ctx) const {
  StatusOr<Table> tbl = ctx.c_->GetTable(ctx.txn_, table_name_);
  return tbl.Value().schema_;
}

size_t FullScanPlan::AccessRowCount(TransactionContext&) const {
  return stats_.row_count_;
}

size_t FullScanPlan::EmitRowCount(TransactionContext&) const {
  return stats_.row_count_;
}

void FullScanPlan::Dump(std::ostream& o, int) const {
  o << "FullScan: " << table_name_;
}

}  // namespace tinylamb