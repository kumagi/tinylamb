// inFullScan(txn)) {}
//  Created by kumagi on 2022/02/21.
//

#include "executor/full_scan.hpp"

#include <cassert>

#include "table/table.hpp"

namespace tinylamb {

FullScan::FullScan(Transaction& txn, std::unique_ptr<TableInterface>&& table)
    : table_(std::move(table)), iter_(table_->BeginFullScan(txn)) {}

bool tinylamb::FullScan::Next(Row* dst, RowPosition* rp) {
  if (!iter_.IsValid()) return false;
  *dst = *iter_;
  // *rp = *iter_;
  ++iter_;
  return true;
}

void FullScan::Dump(std::ostream& o, int indent) const {
  o << "FullScan: " << table_->GetSchema().Name();
}

}  // namespace tinylamb
