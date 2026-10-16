/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "lsm_scan.hpp"

#include <optional>
#include <ostream>
#include <string>
#include <utility>

#include "common/status_or.hpp"
#include "index/lsm_tree.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

LsmScan::LsmScan(const LSMTree& tree) : view_(tree.GetView()) {
  StatusOr<LSMView::Iterator> begin = view_.Begin();
  if (!begin.HasValue()) {
    FailWith(begin.GetStatus());
    return;
  }
  iter_.emplace(begin.MoveValue());
}

bool LsmScan::Next(Row* destination, RowPosition* /*position*/) {
  if (GetStatus() != Status::kSuccess) {
    return false;
  }
  if (!iter_.has_value() || !iter_->IsValid()) {
    // Exhaustion and failure share the false return; the iterator's sticky
    // status tells them apart (blob/cache IO can fail mid-scan).
    if (iter_.has_value() && iter_->GetStatus() != Status::kSuccess) {
      return FailWith(iter_->GetStatus());
    }
    return false;
  }
  StatusOr<std::string> key = iter_->Key();
  if (!key.HasValue()) {
    return FailWith(key.GetStatus());
  }
  StatusOr<std::string> value = iter_->Value();
  if (!value.HasValue()) {
    return FailWith(value.GetStatus());
  }
  *destination = Row({Value(key.MoveValue()), Value(value.MoveValue())});
  ++(*iter_);
  return true;
}

void LsmScan::Dump(std::ostream& output, int /*indent*/) const {
  output << "LsmScanExecutor";
}

}  // namespace tinylamb
