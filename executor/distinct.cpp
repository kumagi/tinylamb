/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/distinct.hpp"

#include <cstddef>
#include <ostream>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
namespace tinylamb {
bool DistinctExecutor::Next(Row* dst, RowPosition* rp) {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (seen_.insert(row).second) {
      *dst = row;
      if (rp != nullptr) { *rp = position;
}
      return true;
    }
  }
  return false;
}
void DistinctExecutor::Dump(std::ostream& output, int indent) const {
  output << "Distinct\n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}

bool SortDistinctExecutor::Next(Row* dst, RowPosition* rp) {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (have_previous_ && row == previous_) { continue; }
    previous_ = row;
    have_previous_ = true;
    *dst = std::move(row);
    if (rp != nullptr) { *rp = position; }
    return true;
  }
  return false;
}

void SortDistinctExecutor::Dump(std::ostream& output, int indent) const {
  output << "SortDistinct\n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
