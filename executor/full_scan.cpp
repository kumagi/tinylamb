// inFullScan(txn)) {}
//  Created by kumagi on 2022/02/21.
//

#include "executor/full_scan.hpp"

#include "table/table.hpp"

namespace tinylamb {

FullScan::FullScan(Transaction& txn, Table* table)
    : table_(table), iter_(table_->BeginFullScan(txn)) {}

bool tinylamb::FullScan::Next(Row* dst, RowPosition* rp) {
  if (!iter_.IsValid()) {
    return false;
  }
  *dst = *iter_;
  if (rp != nullptr) {
    *rp = iter_.Position();
  }
  ++iter_;
  return true;
}

void FullScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->GetSchema().Name();
}

}  // namespace tinylamb
