/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/delete.hpp"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
bool DeleteExecutor::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  std::vector<RowPosition> pending;
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    pending.push_back(position);
  }
  if (source_->GetStatus() != Status::kSuccess) {
    return FailWith(source_->GetStatus());
  }
  int64_t count = 0;
  for (const RowPosition& row_position : pending) {
    if (target_->Delete(*txn_, row_position) != Status::kSuccess) {
      // Do not report a partial delete as successful.
      return FailWith(
          StatusError(StatusCode::kConflicts,

                      "delete failed on table " +
                          std::string(target_->GetSchema().Name())));
    }
    ++count;
  }
  if (assert_rows_modified_ >= 0 && count != assert_rows_modified_) {
    return FailWith(StatusError(StatusCode::kInvalidArgument,

                                "ASSERT_ROWS_MODIFIED was specified with " +
                                    std::to_string(assert_rows_modified_) +
                                    " rows, but " + std::to_string(count) +
                                    " rows were modified"));
  }
  *dst = Row({Value("Delete Rows"), Value(count)});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  finished_ = true;
  return true;
}

void DeleteExecutor::Dump(std::ostream& output, int indent) const {
  output << "Delete: " << target_->GetSchema().Name() << "\n"
         << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
