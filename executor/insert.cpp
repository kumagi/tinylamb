/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Created by kumagi on 22/05/11.
//

#include "executor/insert.hpp"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Primary-key duplicate detection stringifies the key value.  A NULL key
// gets its own sentinel: the reference engine treats two NULL primary keys
// as duplicates of each other (dml_insert.test
// insert_duplicate_new_null_row_error / insert_ignore_two_new_rows_with_null_
// primary_key), so NULLs must NOT behave as pairwise-distinct here.
std::string KeyString(const Value& value) {
  if (value.IsNull()) {
    // Sentinel starts with 0x01. Real strings whose first byte is 0x01 are
    // escaped below, so this exact byte sequence cannot be produced by a
    // non-NULL value (a VARCHAR literally equal to "\x01NULL" must not
    // masquerade as a NULL key).
    return "\x01NULL";
  }
  std::string text = value.AsString();
  if (!text.empty() && text[0] == '\x01') {
    text.insert(text.begin(), '\x01');
  }
  return text;
}

}  // namespace

bool Insert::Execute(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  // Duplicate detection keys: compliance mode mocks the primary key on
  // column 0; otherwise a declared PRIMARY KEY uses its own columns (in
  // declaration order). Conflict handling stays undefined for tables with
  // neither.
  std::vector<size_t> key_offsets;
  if (enforce_primary_key_) {
    key_offsets.push_back(0);
  } else {
    const Schema& schema = target_->GetSchema();
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).GetConstraint().ctype ==
          Constraint::kPrimaryKey) {
        key_offsets.push_back(i);
      }
    }
  }
  if (mode_ != InsertExecutionMode::kDefault && key_offsets.empty()) {
    // Conflict handling is only defined for tables with a primary key. This
    // is a malformed-statement error, not a uniqueness violation: like the
    // other input-shape checks in this function it must map to kInvalidArgument
    // (42601), not kDuplicates (23505 unique_violation).
    return FailWith(StatusError(
        StatusCode::kInvalidArgument,

        "INSERT conflict clause is not allowed because the table does not "
        "have a primary key"));
  }
  int64_t insertion_count = 0;
  Row new_row;
  std::unordered_set<std::string> existing_keys;
  std::unordered_map<std::string, RowPosition> existing_positions;
  // Keys inserted by this statement: a later row of the same statement
  // colliding with them modifies the same logical row once.
  std::unordered_set<std::string> batch_keys;
  // Length-prefixed composite so ("ab", "c") and ("a", "bc") cannot collide;
  // a single-column key stays the bare value (the kDefault diagnostic
  // prints it verbatim).
  const auto row_key = [&key_offsets](const Row& row) -> std::string {
    if (key_offsets.size() == 1) {
      return KeyString(row[key_offsets[0]]);
    }
    std::string key;
    for (const size_t offset : key_offsets) {
      const std::string part = KeyString(row[offset]);
      key += std::to_string(part.size());
      key.push_back(':');
      key += part;
    }
    return key;
  };
  // A PRIMARY KEY can reference any column (a composite key's later columns,
  // or a column not filled in by a narrow/listed INSERT source row). Only a
  // row that actually carries every key column may be passed to row_key();
  // checking values_.empty() alone lets row[offset] read past the end.
  const auto has_key_columns = [&key_offsets](const Row& row) {
    if (key_offsets.empty() || row.values_.empty()) {
      return false;
    }
    for (const size_t offset : key_offsets) {
      if (offset >= row.values_.size()) {
        return false;
      }
    }
    return true;
  };
  if (!key_offsets.empty()) {
    auto it = target_->BeginFullScan(*txn_);
    for (; it.IsValid(); ++it) {
      const Row& current = *it;
      if (!has_key_columns(current)) {
        continue;
      }
      std::string key = row_key(current);
      existing_keys.insert(key);
      existing_positions.emplace(std::move(key), it.Position());
    }
    // IsValid() is false both at the end and after a read failure; judging
    // uniqueness against a prefix of the table would admit duplicates.
    if (it.GetStatus() != Status::kSuccess) {
      return FailWith(it.GetStatus());
    }
  }
  // FOREIGN KEY child-side state: the parent key multiset is seeded once
  // from the statement snapshot; rows this statement inserts extend it so
  // self-referencing keys stay precise.
  std::vector<ForeignKeyCounts> fk_counts;
  if (!fk_checks_.empty()) {
    fk_counts.reserve(fk_checks_.size());
    for (const ForeignKeyCheck& check : fk_checks_) {
      StatusOr<ForeignKeyCounts> counts =
          BuildForeignKeyCounts(*check.parent, *txn_, check.parent_offset);
      if (!counts.HasValue()) {
        return FailWith(counts.GetStatus());
      }
      fk_counts.push_back(counts.MoveValue());
    }
  }
  const auto check_child_row = [&](const Row& row) -> Status {
    for (size_t c = 0; c < fk_checks_.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks_[c];
      if (check.child_offset >= row.values_.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "foreign key child column outside input row");
      }
      StatusOr<bool> viol =
          IsForeignKeyViolation(fk_counts[c], row[check.child_offset]);
      if (!viol.HasValue()) {
        return viol.GetStatus();
      }
      if (viol.Value()) {
        return StatusError(
            StatusCode::kConflicts,
            "insert on table " + std::string(target_->GetSchema().Name()) +
                " violates foreign key constraint (references " +
                std::string(check.parent->GetSchema().Name()) + ")");
      }
    }
    return Status::kSuccess;
  };
  // Table handles resolved through the txn cache may be distinct objects
  // for the same table (case-variant lookups), so self-references compare
  // by canonical schema name, not pointer.
  const std::string target_name = std::string(target_->GetSchema().Name());
  const auto is_self_reference = [&](const Table* other) {
    return std::string(other->GetSchema().Name()) == target_name;
  };
  const auto note_inserted_row = [&](const Row& row) -> Status {
    for (size_t c = 0; c < fk_checks_.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks_[c];
      if (!is_self_reference(check.parent) ||
          check.parent_offset >= row.values_.size()) {
        continue;
      }
      const Value& key = row[check.parent_offset];
      if (!key.IsNull()) {
        RETURN_IF_FAIL(fk_counts[c].IncrementKey(key));
      }
    }
    return Status::kSuccess;
  };
  // Upsert/replace rewrite an existing row whose old key is unknown here;
  // re-seed the self-referencing multisets instead of guessing.
  const auto reseed_self_counts = [&]() -> Status {
    for (size_t c = 0; c < fk_checks_.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks_[c];
      if (!is_self_reference(check.parent)) {
        continue;
      }
      StatusOr<ForeignKeyCounts> counts =
          BuildForeignKeyCounts(*check.parent, *txn_, check.parent_offset);
      if (!counts.HasValue()) {
        return counts.GetStatus();
      }
      fk_counts[c] = counts.MoveValue();
    }
    return Status::kSuccess;
  };
  uint64_t step_count = 0;
  DataChunk src_batch;
  while (src_->NextBatch(&src_batch) != 0) {
    for (size_t row_idx = 0; row_idx < src_batch.Size(); ++row_idx) {
      if ((++step_count & 255U) == 0 && txn_->HasStatementTimeout()) {
        const Status ts = txn_->CheckStatementTimeout();
        if (ts != Status::kSuccess) {
          return FailWith(ts);
        }
      }
      new_row = src_batch.RowAt(row_idx);
      const bool has_key = has_key_columns(new_row);
      const std::string key = has_key ? row_key(new_row) : std::string();
      const bool exists = has_key && existing_keys.contains(key);
      // Rows arrive in full schema layout; listed columns keep their
      // destination offsets.
      const bool in_batch = has_key && batch_keys.contains(key);
      if (exists) {
        switch (mode_) {
          case InsertExecutionMode::kIgnore:
            continue;
          case InsertExecutionMode::kUpsert: {
            const RowPosition& pos = existing_positions[key];
            StatusOr<Row> existing = target_->Read(*txn_, pos);
            if (!existing.HasValue()) {
              return FailWith(existing.GetStatus());
            }
            Row updated = existing.MoveValue();
            if (!upsert_offsets_.empty()) {
              for (const size_t offset : upsert_offsets_) {
                // Guard both sides: a narrow source row must not read past
                // its end, and a narrow stored row must not be grown.
                if (offset >= updated.values_.size() ||
                    offset >= new_row.values_.size()) {
                  return FailWith(
                      StatusError(StatusCode::kInvalidArgument,
                                  "upsert column outside input row"));
                }
                updated[offset] = new_row[offset];
              }
            } else {
              updated = new_row;
            }
            if (const Status fk_status = check_child_row(updated);
                fk_status != Status::kSuccess) {
              return FailWith(fk_status);
            }
            // Upsert/REPLACE resolves a pre-existing row without the locking
            // source scan ordinary UPDATE/DELETE use. Take the write intent
            // before Table::Update reads its before-image: a writer that
            // commits in that gap has already released its own intent and
            // would otherwise be silently overwritten.
            if (!txn_->AddWriteSet(pos)) {
              return FailWith(txn_->LastWriteSetFailureStatus());
            }
            if (const Status failure =
                    target_->Update(*txn_, pos, updated).GetStatus();
                failure != Status::kSuccess) {
              return FailWith(
                  StatusError(failure.GetCode(),
                              "insert failed on table " +
                                  std::string(target_->GetSchema().Name()) +
                                  ": " + ToString(failure)));
            }
            if (const Status reseed_status = reseed_self_counts();
                reseed_status != Status::kSuccess) {
              return FailWith(reseed_status);
            }
            if (!in_batch) {
              ++insertion_count;
            }
            continue;
          }
          case InsertExecutionMode::kReplace: {
            const RowPosition& pos = existing_positions[key];
            if (const Status fk_status = check_child_row(new_row);
                fk_status != Status::kSuccess) {
              return FailWith(fk_status);
            }
            if (!txn_->AddWriteSet(pos)) {
              return FailWith(txn_->LastWriteSetFailureStatus());
            }
            if (const Status failure =
                    target_->Update(*txn_, pos, new_row).GetStatus();
                failure != Status::kSuccess) {
              return FailWith(
                  StatusError(failure.GetCode(),
                              "insert failed on table " +
                                  std::string(target_->GetSchema().Name()) +
                                  ": " + ToString(failure)));
            }
            if (const Status reseed_status = reseed_self_counts();
                reseed_status != Status::kSuccess) {
              return FailWith(reseed_status);
            }
            if (!in_batch) {
              ++insertion_count;
            }
            continue;
          }
          case InsertExecutionMode::kDefault:
            return FailWith(
                StatusError(StatusCode::kDuplicates,

                            "insert failed on table: duplicate primary key (" +
                                key + ") due to previously existing row"));
        }
      }
      if (const Status fk_status = check_child_row(new_row);
          fk_status != Status::kSuccess) {
        return FailWith(fk_status);
      }
      StatusOr<RowPosition> inserted = target_->Insert(*txn_, new_row);
      if (inserted.GetStatus() != Status::kSuccess) {
        // A failed insert (e.g. UNIQUE violation) must not be counted as
        // inserted; surface the failure instead of reporting success. The
        // original message rides along when the failure carried one, so a
        // table-tail timeout is not flattened into a bare "Conflicts".
        const Status failure = inserted.GetStatus();
        std::string message = "insert failed on table " +
                              std::string(target_->GetSchema().Name());
        if (!failure.GetMessage().empty()) {
          message += ": " + failure.GetMessage();
        }
        return FailWith(StatusError(failure.GetCode(), std::move(message)));
      }
      if (has_key) {
        existing_keys.insert(key);
        batch_keys.insert(key);
        existing_positions.insert_or_assign(key, inserted.Value());
      }
      const Status note_st = note_inserted_row(new_row);
      if (note_st != Status::kSuccess) {
        return FailWith(note_st);
      }
      insertion_count++;
    }
  }
  if (src_->GetStatus() != Status::kSuccess) {
    return FailWith(src_->GetStatus());
  }
  if (assert_rows_modified_ >= 0 && insertion_count != assert_rows_modified_) {
    return FailWith(StatusError(
        StatusCode::kInvalidArgument,

        "ASSERT_ROWS_MODIFIED was specified with " +
            std::to_string(assert_rows_modified_) + " rows, but " +
            std::to_string(insertion_count) + " rows were modified"));
  }
  *dst = Row({Value("Insert Rows"), Value(insertion_count)});
  if (rp != nullptr) {
    *rp = RowPosition();  // Fill with an invalid data.
  }
  finished_ = true;
  return true;
}

size_t Insert::NextBatch(DataChunk* destination, size_t max_rows) {
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

void Insert::Dump(std::ostream& o, int indent) const {
  o << "Insert: " << target_->GetSchema().Name() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
