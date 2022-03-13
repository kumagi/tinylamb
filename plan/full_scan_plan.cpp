//
// Created by kumagi on 2022/03/01.
//

#include "full_scan_plan.hpp"

#include "database/catalog.hpp"
#include "database/transaction_context.hpp"
#include "executor/full_scan.hpp"
#include "table/table.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(std::string_view table_name, TableStatistics ts)
    : table_name_(table_name), stats_(std::move(ts)) {}

std::unique_ptr<Table> GetTable(const std::string& table_name,
                                TransactionContext& ctx) {
  std::unique_ptr<Table> tbl = std::make_unique<Table>();
  Status s = ctx.c_->GetTable(ctx.txn_, table_name, tbl.get());
  if (s != Status::kSuccess) {
    LOG(FATAL) << "Table: " << table_name << " not found.";
    abort();
  }
  return tbl;
}

Executor FullScanPlan::EmitExecutor(TransactionContext& ctx) const {
  std::unique_ptr<Table> tbl = GetTable(table_name_, ctx);
  tbl->pm_ = ctx.pm_;
  return std::make_shared<FullScan>(ctx.txn_, std::move(tbl));
}

Schema FullScanPlan::GetSchema(TransactionContext& ctx) const {
  return GetTable(table_name_, ctx)->GetSchema();
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