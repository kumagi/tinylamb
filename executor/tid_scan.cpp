/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/tid_scan.hpp"

#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

TidScan::TidScan(Transaction& txn, const Table& table,
                 std::vector<RowPosition> positions, Schema schema)
    : txn_(&txn),
      table_(&table),
      positions_(std::move(positions)),
      schema_(std::move(schema)) {}

bool TidScan::Next(Row* dst, RowPosition* rp) {
  while (offset_ < positions_.size()) {
    const RowPosition pos = positions_[offset_++];
    StatusOr<Row> row_or = table_->Read(*txn_, pos);
    if (row_or.GetStatus() != Status::kSuccess) {
      continue;
    }
    *dst = std::move(row_or.Value());
    if (rp != nullptr) {
      *rp = pos;
    }
    return true;
  }
  return false;
}

void TidScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "TidScan: (" << positions_.size() << " positions)\n";
}

}  // namespace tinylamb
