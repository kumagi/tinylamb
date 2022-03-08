//
// Created by kumagi on 2022/03/01.
//

#include "full_scan_plan.hpp"

#include "database/catalog.hpp"
#include "database/transaction_context.hpp"
#include "executor/full_scan.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(std::string_view table_name, TableStatistics ts)
    : table_name_(table_name), stats_(std::move(ts)) {}

void InitIfNeeded(std::unique_ptr<Table>& tbl, const std::string& table_name,
                  TransactionContext& ctx) {
  if (!tbl) {
    tbl = std::make_unique<Table>();
    Status s = ctx.c_->GetTable(ctx.txn_, table_name, tbl.get());
    if (s != Status::kSuccess) {
      LOG(FATAL) << "Table: " << table_name << " not found.";
      abort();
    }
  }
}

std::unique_ptr<ExecutorBase> FullScanPlan::EmitExecutor(
    TransactionContext& ctx) const {
  InitIfNeeded(tbl_, table_name_, ctx);
  tbl_->pm_ = ctx.pm_;
  return std::make_unique<FullScan>(ctx.txn_, std::move(tbl_));
}

Schema FullScanPlan::GetSchema(TransactionContext& ctx) const {
  InitIfNeeded(tbl_, table_name_, ctx);
  return tbl_->GetSchema();
}

size_t FullScanPlan::AccessRowCount(TransactionContext& ctx) const {
  return stats_.row_count_;
}

size_t FullScanPlan::EmitRowCount(TransactionContext& ctx) const {
  return stats_.row_count_;
}

void FullScanPlan::Dump(std::ostream& o, int indent) const {
  o << "FullScan: " << table_name_;
}

}  // namespace tinylamb