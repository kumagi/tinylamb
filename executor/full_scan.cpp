// inFullScan(txn)) {}
//  Created by kumagi on 2022/02/21.
//

#include "executor/full_scan.hpp"

#include <cassert>

#include "table/table.hpp"

namespace tinylamb {

FullScan::FullScan(Transaction& txn, const TableInterface* table)
    : table_(table), iter_(table_->BeginFullScan(txn)) {}

bool tinylamb::FullScan::Next(Row* dst) {
  if (!iter_.IsValid()) return false;
  *dst = *iter_;
  ++iter_;
  return true;
}

}  // namespace tinylamb
