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
  // NO ACTION enforcement: a parent row still referenced by a child row
  // the statement does not itself delete rejects the statement.  Rows in
  // `pending` are invisible to the check, matching statement-end semantics.
  if (!ref_checks_.empty()) {
    for (const RowPosition& row_position : pending) {
      StatusOr<Row> old = target_->Read(*txn_, row_position);
      if (!old.HasValue()) {
        return FailWith(old.GetStatus());
      }
      const Row& old_row = old.Value();
      for (const ReferencedByCheck& check : ref_checks_) {
        if (check.parent_offset >= old_row.values_.size()) {
          continue;
        }
        const Value& parent_key = old_row[check.parent_offset];
        if (parent_key.IsNull()) {
          continue;
        }
        if (IsParentKeyReferenced(*check.child, *txn_, check.child_offset,
                                  parent_key, pending)) {
          return FailWith(StatusError(
              StatusCode::kConflicts,
              "delete on table " + std::string(target_->GetSchema().Name()) +
                  " violates foreign key constraint (referenced by " +
                  std::string(check.child->GetSchema().Name()) + ")"));
        }
      }
    }
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
