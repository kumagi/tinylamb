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

FullScanPlan::FullScanPlan(std::string_view table_name)
    : table_name_(table_name) {}

std::unique_ptr<ExecutorBase> FullScanPlan::EmitExecutor(
    TransactionContext& ctx) const {
  if (schema_.Empty()) {
    ctx.c_->GetTable(ctx.txn_, table_name_, &schema_, &tbl_);
    tbl_.pm_ = ctx.pm_;
  }
  ctx.c_->GetTable(ctx.txn_, table_name_, &schema_, &tbl_);
  return std::make_unique<FullScan>(ctx.txn_, &tbl_);
}

Schema FullScanPlan::GetSchema(TransactionContext& ctx) const {
  if (schema_.Empty()) {
    ctx.c_->GetTable(ctx.txn_, table_name_, &schema_, &tbl_);
    tbl_.pm_ = ctx.pm_;
  }
  return schema_;
}

void FullScanPlan::Dump(std::ostream& o, int indent) const {
  o << "FullScan: " << table_name_;
}

}  // namespace tinylamb