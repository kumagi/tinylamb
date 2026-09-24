/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "update.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "table/table.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::string UpdateKeyString(const Value& value) {
  if (value.IsNull()) {
    return "\x01NULL";
  }
  return value.AsString();
}

}  // namespace

bool Update::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  std::vector<std::pair<Row, RowPosition>> pending;
  Row new_row;
  RowPosition position;
  while (src_->Next(&new_row, &position)) {
    assert(position.IsValid());
    pending.emplace_back(std::move(new_row), position);
  }
  if (src_->GetStatus() != Status::kSuccess) {
    return FailWith(src_->GetStatus());
  }
  // Primary-key emulation: every updated row vacates its current first-column
  // key and claims a new one; claiming a key still held by another row (or
  // claimed twice within this statement) is a duplicate-key error, and NULL
  // participates like any other key value.
  std::unordered_map<std::string, int64_t> live_keys;
  std::vector<std::string> old_keys(pending.size());
  if (enforce_primary_key_) {
    for (auto it = target_->BeginFullScan(*txn_); it.IsValid(); ++it) {
      const Row& current = *it;
      if (current.values_.empty()) {
        continue;
      }
      ++live_keys[UpdateKeyString(current[0])];
    }
    for (size_t i = 0; i < pending.size(); ++i) {
      StatusOr<Row> current = target_->Read(*txn_, pending[i].second);
      old_keys[i] = (current.HasValue() && !current.Value().values_.empty())
                        ? UpdateKeyString(current.Value()[0])
                        : std::string();
      auto found = live_keys.find(old_keys[i]);
      if (found != live_keys.end()) {
        if (--found->second == 0) {
          live_keys.erase(found);
        }
      }
    }
    std::unordered_set<std::string> claimed;
    for (auto& i : pending) {
      if (i.first.values_.empty()) {
        continue;
      }
      const std::string new_key = UpdateKeyString(i.first[0]);
      if (!claimed.insert(new_key).second || live_keys.contains(new_key)) {
        return FailWith(
            StatusError(StatusCode::kDuplicates,

                        "Modification resulted in duplicate primary key (" +
                            new_key + ")"));
      }
    }
  }
  // FOREIGN KEY enforcement.  The parent multiset is seeded once; changed
  // child keys probe it, and changed parent keys probe the referencing
  // tables (NO ACTION: a referenced old key rejects the statement).
  std::vector<ForeignKeyCounts> fk_counts;
  std::vector<Row> old_rows;
  if (!fk_checks_.empty() || !ref_checks_.empty()) {
    fk_counts.reserve(fk_checks_.size());
    for (const ForeignKeyCheck& check : fk_checks_) {
      StatusOr<ForeignKeyCounts> counts =
          BuildForeignKeyCounts(*check.parent, *txn_, check.parent_offset);
      if (!counts.HasValue()) {
        return FailWith(counts.GetStatus());
      }
      fk_counts.push_back(counts.MoveValue());
    }
    old_rows.reserve(pending.size());
    for (const auto& [row, row_position] : pending) {
      StatusOr<Row> old = target_->Read(*txn_, row_position);
      if (!old.HasValue()) {
        return FailWith(old.GetStatus());
      }
      old_rows.push_back(old.MoveValue());
    }
    for (size_t i = 0; i < pending.size(); ++i) {
      const Row& updated_row = pending[i].first;
      const Row& old_row = old_rows[i];
      for (size_t c = 0; c < fk_checks_.size(); ++c) {
        const ForeignKeyCheck& check = fk_checks_[c];
        if (check.child_offset >= updated_row.values_.size() ||
            check.child_offset >= old_row.values_.size()) {
          continue;
        }
        const Value& new_key = updated_row[check.child_offset];
        const Value& old_key = old_row[check.child_offset];
        if (new_key == old_key) {
          continue;
        }
        if (IsForeignKeyViolation(fk_counts[c], new_key)) {
          return FailWith(StatusError(
              StatusCode::kConflicts,
              "update on table " + std::string(target_->GetSchema().Name()) +
                  " violates foreign key constraint (references " +
                  std::string(check.parent->GetSchema().Name()) + ")"));
        }
      }
      for (const ReferencedByCheck& check : ref_checks_) {
        if (check.parent_offset >= updated_row.values_.size() ||
            check.parent_offset >= old_row.values_.size()) {
          continue;
        }
        const Value& new_key = updated_row[check.parent_offset];
        const Value& old_key = old_row[check.parent_offset];
        if (new_key == old_key || old_key.IsNull()) {
          continue;
        }
        if (IsParentKeyReferenced(*check.child, *txn_, check.child_offset,
                                  old_key, {})) {
          return FailWith(StatusError(
              StatusCode::kConflicts,
              "update on table " + std::string(target_->GetSchema().Name()) +
                  " violates foreign key constraint (referenced by " +
                  std::string(check.child->GetSchema().Name()) + ")"));
        }
      }
    }
  }
  int64_t update_count = 0;
  const std::string target_name = std::string(target_->GetSchema().Name());
  for (size_t i = 0; i < pending.size(); ++i) {
    auto& [row, row_position] = pending[i];
    StatusOr<RowPosition> p = target_->Update(*txn_, row_position, row);
    if (p.GetStatus() != Status::kSuccess) {
      // Do not report a partial update as successful.
      return FailWith(
          StatusError(StatusCode::kConflicts,

                      "update failed on table " +
                          std::string(target_->GetSchema().Name())));
    }
    // Keep self-referencing parent multisets precise across the statement.
    if (!old_rows.empty()) {
      const Row& old_row = old_rows[i];
      for (size_t c = 0; c < fk_checks_.size(); ++c) {
        const ForeignKeyCheck& check = fk_checks_[c];
        if (std::string(check.parent->GetSchema().Name()) != target_name ||
            check.parent_offset >= row.values_.size() ||
            check.parent_offset >= old_row.values_.size()) {
          continue;
        }
        const Value& new_key = row[check.parent_offset];
        const Value& old_key = old_row[check.parent_offset];
        if (new_key == old_key) {
          continue;
        }
        if (!old_key.IsNull()) {
          auto found = fk_counts[c].find(ForeignKeyString(old_key));
          if (found != fk_counts[c].end() && --found->second <= 0) {
            fk_counts[c].erase(found);
          }
        }
        if (!new_key.IsNull()) {
          ++fk_counts[c][ForeignKeyString(new_key)];
        }
      }
    }
    update_count++;
  }
  if (assert_rows_modified_ >= 0 && update_count != assert_rows_modified_) {
    return FailWith(
        StatusError(StatusCode::kInvalidArgument,

                    "ASSERT_ROWS_MODIFIED was specified with " +
                        std::to_string(assert_rows_modified_) + " rows, but " +
                        std::to_string(update_count) + " rows were modified"));
  }
  *dst = Row({Value("Update Rows"), Value(update_count)});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  finished_ = true;
  return true;
}

void Update::Dump(std::ostream& o, int indent) const {
  o << "Update: " << target_->GetSchema().Name() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
