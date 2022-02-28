//
// Created by kumagi on 2022/03/01.
//

#include "full_scan_plan.hpp"

#include "database/catalog.hpp"
#include "executor/full_scan.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

FullScanPlan::FullScanPlan(Transaction& txn, std::string_view table_name,
                           Catalog* catalog, PageManager* pm)
    : table_name_(table_name), tbl_(pm) {
  catalog->GetTable(txn, table_name_, &schema_, &tbl_);
}

std::unique_ptr<ExecutorBase> FullScanPlan::EmitExecutor(
    Transaction& txn) const {
  return std::make_unique<FullScan>(txn, &tbl_);
}

}  // namespace tinylamb