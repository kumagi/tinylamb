/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/delete.hpp"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
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
      // Emits the parent key of every pending row whose encoded key is in
      // |vacated| (empty map = every pending key).
      const auto emit_candidate_keys =
          [&](const std::function<Status(const Value&)>& emit,
              const std::unordered_map<std::string, int64_t>* vacated)
          -> Status {
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
              if (vacated != nullptr &&
                  !vacated->contains(ForeignKeyString(parent_key))) {
                return Status::kSuccess;
              }
              return emit(parent_key);
            }
          }
          return Status::kSuccess;
        });
      };
      const auto emit_ignore_positions =
          [&](const std::function<Status(const RowPosition&)>& emit) -> Status {
        // Self-referencing edge only: the ignore positions are
        // target-row positions, meaningless against another child
        // table's row space.
        if (check.child != target_) {
          return Status::kSuccess;
        }
        return pending.ForEach([&](const RowPosition& pos) -> Status {
          if ((++check_steps & 255U) == 0 && txn_->HasStatementTimeout()) {
            const Status ts = txn_->CheckStatementTimeout();
            if (ts != Status::kSuccess) {
              return ts;
            }
          }
          return emit(pos);
        });
      };
      const auto probe_children =
          [&](const std::unordered_map<std::string, int64_t>* vacated)
          -> StatusOr<std::optional<std::string>> {
        return FindReferencedParentKey(
            *check.child, *txn_, check.child_offset,
            [&](const std::function<Status(const Value&)>& emit) -> Status {
              return emit_candidate_keys(emit, vacated);
            },
            emit_ignore_positions);
      };

      // Fast path: probe every pending key against the child tables.
      StatusOr<std::optional<std::string>> referenced = probe_children(nullptr);
      if (!referenced.HasValue()) {
        return FailWith(referenced.GetStatus());
      }
      if (!referenced.Value().has_value()) {
        continue;
      }

      // The referenced column need not be unique, so a matched key may
      // still be supplied by a parent row the statement keeps
      // (p=(1,10),(1,20); DELETE WHERE tag=10 leaves a=1). On a unique
      // column no survivor can share the key and the match is final.
      const Schema& target_schema = target_->GetSchema();
      // The FK metadata may be stale (e.g. after a schema change); GetColumn
      // has no bounds check, so validate the offset before indexing it.
      if (check.parent_offset >= target_schema.ColumnCount()) {
        return FailWith(StatusError(
            StatusCode::kCorrupt,
            "foreign key parent_offset out of range for table " +
                std::string(target_schema.Name())));
      }
      if (target_schema.GetColumn(check.parent_offset)
              .GetConstraint()
              .IsUnique()) {
        return FailWith(StatusError(
            StatusCode::kConflicts,
            "delete on table " + std::string(target_schema.Name()) +
                " violates foreign key constraint (referenced by " +
                std::string(check.child->GetSchema().Name()) + ")"));
      }

      // Multiset of the pending rows' keys, then a parent scan cancels
      // each key against its surviving occurrences — the DELETE-side
      // analog of UPDATE's supplied_keys/vacated_keys correction. A key
      // erased here still exists after the statement; one left in the map
      // (count may reach zero) is truly vacated.
      std::unordered_map<std::string, int64_t> vacated;
      const Status vac_st =
          pending.ForEach([&](const RowPosition& pos) -> Status {
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
            if (check.parent_offset < old_row.values_.size() &&
                !old_row[check.parent_offset].IsNull()) {
              ++vacated[ForeignKeyString(old_row[check.parent_offset])];
            }
            return Status::kSuccess;
          });
      if (vac_st != Status::kSuccess) {
        return FailWith(vac_st);
      }
      if (!vacated.empty()) {
        const std::vector<slot_t> projection{
            static_cast<slot_t>(check.parent_offset)};
        auto current = target_->BeginFullScan(*txn_, projection);
        size_t steps = 0;
        for (; current.IsValid(); ++current) {
          if ((++steps & 255U) == 0 && txn_->HasStatementTimeout()) {
            const Status timeout = txn_->CheckStatementTimeout();
            if (timeout != Status::kSuccess) {
              return FailWith(timeout);
            }
          }
          const Row& parent = *current;
          if (parent.values_.empty() || parent[0].IsNull()) {
            continue;
          }
          auto found = vacated.find(ForeignKeyString(parent[0]));
          if (found == vacated.end()) {
            continue;
          }
          if (found->second > 0) {
            --found->second;
            continue;
          }
          vacated.erase(found);
          if (vacated.empty()) {
            break;
          }
        }
        if (current.GetStatus() != Status::kSuccess) {
          return FailWith(current.GetStatus());
        }
      }
      if (vacated.empty()) {
        continue;
      }

      // Re-probe with only the truly vacated keys.
      StatusOr<std::optional<std::string>> still_referenced =
          probe_children(&vacated);
      if (!still_referenced.HasValue()) {
        return FailWith(still_referenced.GetStatus());
      }
      if (still_referenced.Value().has_value()) {
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
