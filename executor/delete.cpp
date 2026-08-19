/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/delete.hpp"

#include <cstdint>
#include <ostream>

#include "table/table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
bool DeleteExecutor::Next(Row* dst, RowPosition* rp) {
  if (finished_) return false;
  int64_t count = 0;
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (target_->Delete(*txn_, position) != Status::kSuccess) break;
    ++count;
  }
  *dst = Row({Value("Delete Rows"), Value(count)});
  if (rp != nullptr) *rp = RowPosition();
  finished_ = true;
  return true;
}

void DeleteExecutor::Dump(std::ostream& output, int indent) const {
  output << "Delete: " << target_->GetSchema().Name() << "\n"
         << Indent(static_cast<size_t>(indent + 2));
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
