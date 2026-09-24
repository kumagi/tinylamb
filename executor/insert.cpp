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
    return "\x01NULL";
  }
  return value.AsString();
}

}  // namespace

bool Insert::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  if (mode_ != InsertExecutionMode::kDefault && !enforce_primary_key_) {
    // Conflict handling is only defined for tables with a primary key.
    return FailWith(StatusError(
        StatusCode::kDuplicates,

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
  if (enforce_primary_key_) {
    for (auto it = target_->BeginFullScan(*txn_); it.IsValid(); ++it) {
      const Row& current = *it;
      if (current.values_.empty()) {
        continue;
      }
      std::string key = KeyString(current[0]);
      existing_keys.insert(key);
      existing_positions.emplace(std::move(key), it.Position());
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
        continue;
      }
      if (IsForeignKeyViolation(fk_counts[c], row[check.child_offset])) {
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
  const auto note_inserted_row = [&](const Row& row) {
    for (size_t c = 0; c < fk_checks_.size(); ++c) {
      const ForeignKeyCheck& check = fk_checks_[c];
      if (!is_self_reference(check.parent) ||
          check.parent_offset >= row.values_.size()) {
        continue;
      }
      const Value& key = row[check.parent_offset];
      if (!key.IsNull()) {
        ++fk_counts[c][ForeignKeyString(key)];
      }
    }
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
  while (src_->Next(&new_row, nullptr)) {
    const bool has_key = enforce_primary_key_ && !new_row.values_.empty();
    const std::string key = has_key ? KeyString(new_row[0]) : std::string();
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
          Row updated = target_->Read(*txn_, pos).Value();
          if (!upsert_offsets_.empty()) {
            for (const size_t offset : upsert_offsets_) {
              if (offset < updated.values_.size()) {
                updated[offset] = new_row[offset];
              }
            }
          } else {
            updated = new_row;
          }
          if (const Status fk_status = check_child_row(updated);
              fk_status != Status::kSuccess) {
            return FailWith(fk_status);
          }
          if (target_->Update(*txn_, pos, updated).GetStatus() !=
              Status::kSuccess) {
            return FailWith(
                StatusError(StatusCode::kConflicts,

                            "insert failed on table " +
                                std::string(target_->GetSchema().Name())));
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
          if (target_->Update(*txn_, pos, new_row).GetStatus() !=
              Status::kSuccess) {
            return FailWith(
                StatusError(StatusCode::kConflicts,

                            "insert failed on table " +
                                std::string(target_->GetSchema().Name())));
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

                          "Failed to insert row with primary key (" + key +
                              ") due to previously existing row"));
      }
    }
    if (const Status fk_status = check_child_row(new_row);
        fk_status != Status::kSuccess) {
      return FailWith(fk_status);
    }
    StatusOr<RowPosition> inserted = target_->Insert(*txn_, new_row);
    if (inserted.GetStatus() != Status::kSuccess) {
      // A failed insert (e.g. UNIQUE violation) must not be counted as
      // inserted; surface the failure instead of reporting success.
      return FailWith(
          StatusError(StatusCode::kConflicts,

                      "insert failed on table " +
                          std::string(target_->GetSchema().Name())));
    }
    if (has_key) {
      existing_keys.insert(key);
      batch_keys.insert(key);
      existing_positions.insert_or_assign(key, inserted.Value());
    }
    note_inserted_row(new_row);
    insertion_count++;
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

void Insert::Dump(std::ostream& o, int indent) const {
  o << "Insert: " << target_->GetSchema().Name() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
