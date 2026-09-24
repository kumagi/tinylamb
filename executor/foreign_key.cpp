/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/foreign_key.hpp"

#include <string>
#include <utility>
#include <vector>

#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

std::string ForeignKeyString(const Value& value) {
  // The leading type tag keeps INT64 1, DOUBLE 1.0 and VARCHAR "1" apart;
  // AsString is injective within one ValueType and DDL validates the child
  // and parent column types equal.
  return std::string(1, static_cast<char>(value.type)) + value.AsString();
}

StatusOr<ForeignKeyCounts> BuildForeignKeyCounts(Table& parent,
                                                 Transaction& txn,
                                                 size_t parent_offset) {
  ForeignKeyCounts counts;
  std::vector<slot_t> projection = {static_cast<slot_t>(parent_offset)};
  for (auto it = parent.BeginFullScan(txn, projection); it.IsValid(); ++it) {
    const Row& row = *it;
    if (row.values_.empty() || row[0].IsNull()) {
      continue;
    }
    ++counts[ForeignKeyString(row[0])];
  }
  return counts;
}

bool IsForeignKeyViolation(const ForeignKeyCounts& counts, const Value& key) {
  if (key.IsNull()) {
    return false;
  }
  const auto found = counts.find(ForeignKeyString(key));
  return found == counts.end() || found->second <= 0;
}

bool IsParentKeyReferenced(Table& child, Transaction& txn, size_t child_offset,
                           const Value& parent_key,
                           const std::vector<RowPosition>& ignore) {
  if (parent_key.IsNull()) {
    return false;
  }
  const std::string wanted = ForeignKeyString(parent_key);
  std::vector<slot_t> projection = {static_cast<slot_t>(child_offset)};
  for (auto it = child.BeginFullScan(txn, projection); it.IsValid(); ++it) {
    if (!ignore.empty()) {
      bool skipped = false;
      for (const RowPosition& pos : ignore) {
        if (it.Position() == pos) {
          skipped = true;
          break;
        }
      }
      if (skipped) {
        continue;
      }
    }
    const Row& row = *it;
    if (row.values_.empty() || row[0].IsNull()) {
      continue;
    }
    if (ForeignKeyString(row[0]) == wanted) {
      return true;
    }
  }
  return false;
}

}  // namespace tinylamb
