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
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
bool DeleteExecutor::Execute(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  uint64_t pull_steps = 0;
  SpillableRowPositionList pending;
  DataChunk batch;
  while (source_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      if ((++pull_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
        const Status ts = txn_->CheckStatementTimeout();
        if (ts != Status::kSuccess) {
          return FailWith(ts);
        }
      }
      const Status st = pending.PushBack(batch.PositionAt(i));
      if (st != Status::kSuccess) {
        return FailWith(st);
      }
    }
  }
  if (source_->GetStatus() != Status::kSuccess) {
    return FailWith(source_->GetStatus());
  }
  // NO ACTION enforcement: a parent row still referenced by a child row
  // the statement does not itself delete rejects the statement.  Rows in
  // `pending` are invisible to the check, matching statement-end semantics.
  uint64_t check_steps = 0;
  if (!ref_checks_.empty() && !pending.Empty()) {
    for (const ReferencedByCheck& check : ref_checks_) {
      StatusOr<std::optional<std::string>> referenced = FindReferencedParentKey(
          *check.child, *txn_, check.child_offset,
          [&](const std::function<Status(const Value&)>& emit) -> Status {
            return pending.ForEach([&](const RowPosition& pos) -> Status {
              if ((++check_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
                const Status ts = txn_->CheckStatementTimeout();
                if (ts != Status::kSuccess) {
                  return ts;
                }
              }
              StatusOr<Row> old = target_->Read(*txn_, pos);
              if (!old.HasValue()) {
                return old.GetStatus();
              }
              const Row& old_row = old.Value();
              if (check.parent_offset < old_row.values_.size()) {
                const Value& parent_key = old_row[check.parent_offset];
                if (!parent_key.IsNull()) {
                  return emit(parent_key);
                }
              }
              return Status::kSuccess;
            });
          },
          [&](const std::function<Status(const RowPosition&)>& emit) -> Status {
            return pending.ForEach([&](const RowPosition& pos) -> Status {
              if ((++check_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
                const Status ts = txn_->CheckStatementTimeout();
                if (ts != Status::kSuccess) {
                  return ts;
                }
              }
              return emit(pos);
            });
          });
      if (!referenced.HasValue()) {
        return FailWith(referenced.GetStatus());
      }
      if (referenced.Value().has_value()) {
        return FailWith(StatusError(
            StatusCode::kConflicts,
            "delete on table " + std::string(target_->GetSchema().Name()) +
                " violates foreign key constraint (referenced by " +
                std::string(check.child->GetSchema().Name()) + ")"));
      }
    }
  }
  int64_t count = 0;
  uint64_t apply_steps = 0;
  const std::string target_name = std::string(target_->GetSchema().Name());
  const Status delete_status =
      pending.ForEach([&](const RowPosition& row_position) -> Status {
        // The source pull loop above cannot bound this apply phase: without a
        // deadline here one Execute() would delete every pending row and emit
        // its WAL even after the statement timed out.
        if ((++apply_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
          const Status ts = txn_->CheckStatementTimeout();
          if (ts != Status::kSuccess) {
            return ts;
          }
        }
        const Status failure = target_->Delete(*txn_, row_position);
        if (failure != Status::kSuccess) {
          // Do not report a partial delete as successful or mask storage/OOM
          // failures as retryable conflicts.
          return StatusError(failure.GetCode(), "delete failed on table " +
                                                    target_name + ": " +
                                                    ToString(failure));
        }
        ++count;
        return Status::kSuccess;
      });
  if (delete_status != Status::kSuccess) {
    return FailWith(delete_status);
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

size_t DeleteExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  RowPosition rp;
  if (!Execute(&row, &rp)) {
    return 0;
  }
  destination->Append(std::move(row), rp);
  return 1;
}

void DeleteExecutor::Dump(std::ostream& output, int indent) const {
  output << "Delete: " << target_->GetSchema().Name() << "\n"
         << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
