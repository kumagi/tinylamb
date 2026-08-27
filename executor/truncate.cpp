/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/truncate.hpp"

#include <ostream>
#include <vector>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"

namespace tinylamb {

TruncateExecutor::TruncateExecutor(Transaction& txn, Table& table)
    : txn_(&txn), table_(&table) {}

bool TruncateExecutor::Next(Row* dst, RowPosition* rp) {
  if (executed_) {
    return false;
  }
  executed_ = true;

  // Collect all row positions in the table
  std::vector<RowPosition> positions;
  for (auto it = table_->BeginFullScan(*txn_); it.IsValid(); ++it) {
    positions.push_back(it.Position());
  }

  // Delete all rows
  deleted_count_ = 0;
  for (const RowPosition& pos : positions) {
    if (table_->Delete(*txn_, pos) == Status::kSuccess) {
      ++deleted_count_;
    }
  }

  *dst = Row({Value(static_cast<int64_t>(deleted_count_))});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  return true;
}

void TruncateExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "TruncateExecutor: (deleted " << deleted_count_ << " rows)\n";
}

}  // namespace tinylamb
