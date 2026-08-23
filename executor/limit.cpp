/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/limit.hpp"

#include <cstddef>
#include <ostream>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
namespace tinylamb {
bool LimitExecutor::Next(Row* dst, RowPosition* rp) {
  Row ignored;
  while (skipped_ < offset_) {
    if (!source_->Next(&ignored, nullptr)) { return false;
}
    ++skipped_;
  }
  if (limit_ != 0 && emitted_ >= limit_) { return false;
}
  if (!source_->Next(dst, rp)) { return false;
}
  ++emitted_;
  return true;
}
void LimitExecutor::Dump(std::ostream& output, int indent) const {
  output << "Limit: " << limit_ << " offset " << offset_ << "\n"
         << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
