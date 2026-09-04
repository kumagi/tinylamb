/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "update.hpp"

#include <cassert>
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
  // Primary-key emulation: every updated row vacates its current first-column
  // key and claims a new one; claiming a key still held by another row (or
  // claimed twice within this statement) is a duplicate-key error, and NULL
  // participates like any other key value.
  std::unordered_map<std::string, int64_t> live_keys;
  std::vector<std::string> old_keys(pending.size());
  if (enforce_primary_key_) {
    for (auto it = target_->BeginFullScan(*txn_); it.IsValid(); ++it) {
      const Row& current = *it;
      if (current.values_.size() == 0) {
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
    for (size_t i = 0; i < pending.size(); ++i) {
      if (pending[i].first.values_.empty()) {
        continue;
      }
      const std::string new_key = UpdateKeyString(pending[i].first[0]);
      if (!claimed.insert(new_key).second || live_keys.contains(new_key)) {
        throw std::runtime_error(
            "Modification resulted in duplicate primary key (" + new_key + ")");
      }
    }
  }
  int64_t update_count = 0;
  for (auto& [row, row_position] : pending) {
    StatusOr<RowPosition> p = target_->Update(*txn_, row_position, row);
    if (p.GetStatus() != Status::kSuccess) {
      // Do not report a partial update as successful.
      throw std::runtime_error("update failed on table " +
                               std::string(target_->GetSchema().Name()));
    }
    update_count++;
  }
  if (assert_rows_modified_ >= 0 && update_count != assert_rows_modified_) {
    throw std::runtime_error("ASSERT_ROWS_MODIFIED was specified with " +
                             std::to_string(assert_rows_modified_) +
                             " rows, but " + std::to_string(update_count) +
                             " rows were modified");
  }
  *dst = Row({Value("Update Rows"), Value(update_count)});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  finished_ = true;
  return true;
}

void Update::Dump(std::ostream& o, int indent) const {
  o << "Update: " << target_->GetSchema().Name() << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
