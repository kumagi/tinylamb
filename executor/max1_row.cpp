/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "max1_row.hpp"

#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <utility>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

bool Max1RowExecutor::Next(Row* destination, RowPosition* position) {
  Row row;
  RowPosition row_position;
  if (!source_->Next(&row, &row_position)) {
    FailWithChildOf(*source_);
    return false;
  }
  if (emitted_) {
    return FailWith(StatusError(StatusCode::kInvalidArgument,
                                "scalar subquery returned more than one row"));
  }
  emitted_ = true;
  *destination = std::move(row);
  if (position != nullptr) {
    *position = row_position;
  }
  return true;
}

void Max1RowExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "Max1Row\n";
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
