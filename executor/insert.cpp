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

std::string KeyString(const Value& value) {
  if (value.IsNull()) { return "\x01NULL"; }
  return value.AsString();
}

}  // namespace

bool Insert::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  if (mode_ != InsertExecutionMode::kDefault && !enforce_primary_key_) {
    // Conflict handling is only defined for tables with a primary key.
    throw std::runtime_error(
        "INSERT conflict clause is not allowed because the table does not "
        "have a primary key");
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
      if (current.values_.size() == 0) { continue;
}
      std::string key = KeyString(current[0]);
      existing_keys.insert(key);
      existing_positions.emplace(std::move(key), it.Position());
    }
  }
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
          if (target_->Update(*txn_, pos, updated).GetStatus() !=
              Status::kSuccess) {
            throw std::runtime_error("insert failed on table " +
                                     std::string(target_->GetSchema().Name()));
          }
          if (!in_batch) { ++insertion_count;
}
          continue;
        }
        case InsertExecutionMode::kReplace: {
          const RowPosition& pos = existing_positions[key];
          if (target_->Update(*txn_, pos, new_row).GetStatus() !=
              Status::kSuccess) {
            throw std::runtime_error("insert failed on table " +
                                     std::string(target_->GetSchema().Name()));
          }
          if (!in_batch) { ++insertion_count;
}
          continue;
        }
        case InsertExecutionMode::kDefault:
          throw std::runtime_error(
              "Failed to insert row with primary key (" + key +
              ") due to previously existing row");
      }
    }
    StatusOr<RowPosition> inserted = target_->Insert(*txn_, new_row);
    if (inserted.GetStatus() != Status::kSuccess) {
      // A failed insert (e.g. UNIQUE violation) must not be counted as
      // inserted; surface the failure instead of reporting success.
      throw std::runtime_error("insert failed on table " +
                               std::string(target_->GetSchema().Name()));
    }
    if (has_key) {
      existing_keys.insert(key);
      batch_keys.insert(key);
      existing_positions.insert_or_assign(key, inserted.Value());
    }
    insertion_count++;
  }
  if (assert_rows_modified_ >= 0 && insertion_count != assert_rows_modified_) {
    throw std::runtime_error("ASSERT_ROWS_MODIFIED was specified with " +
                             std::to_string(assert_rows_modified_) +
                             " rows, but " + std::to_string(insertion_count) +
                             " rows were modified");
  }
  *dst = Row({Value("Insert Rows"), Value(insertion_count)});
  if (rp != nullptr) {
    *rp = RowPosition();  // Fill with an invalid data.
  }
  finished_ = true;
  return true;
}

void Insert::Dump(std::ostream& o, int indent) const {
  o << "Insert: " << target_->GetSchema().Name() << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
