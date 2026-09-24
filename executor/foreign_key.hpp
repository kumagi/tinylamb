/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_FOREIGN_KEY_HPP
#define TINYLAMB_FOREIGN_KEY_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "page/row_position.hpp"

namespace tinylamb {
struct Row;
class Table;
class Transaction;
class Value;

// One enforced FOREIGN KEY edge, resolved to table handles by the query
// layer (which owns catalog access).  Plain data so DML executors stay free
// of statement-layer types.
struct ForeignKeyCheck {
  size_t child_offset{0};   // Offset of the FK column in the target row.
  Table* parent{nullptr};   // Resolved parent table (owned by the txn ctx).
  size_t parent_offset{0};  // Offset of the referenced column in parent rows.
};

// Reverse edge for NO ACTION checks on the referenced side: deleting (or
// rekeying) a parent row is rejected while a child row references its key.
struct ReferencedByCheck {
  Table* child{nullptr};    // Resolved child table (owned by the txn ctx).
  size_t child_offset{0};   // Offset of the FK column in child rows.
  size_t parent_offset{0};  // Offset of the referenced key in target rows.
};

// Multiset of non-null parent keys, encoded as a type tag plus Value::AsString
// (injective per ValueType, and child/parent types are validated equal at
// CREATE TABLE).  Counts rather than a set keep self-referencing statements
// precise when a statement both removes and adds the same key.
using ForeignKeyCounts = std::unordered_map<std::string, int64_t>;

// Encodes a non-null key value for ForeignKeyCounts.  NULL keys never match
// under SQL semantics and must not be encoded.
std::string ForeignKeyString(const Value& value);

// Seeds the parent-key multiset from a statement-snapshot scan of the
// parent table (projection-only read of the referenced column).
StatusOr<ForeignKeyCounts> BuildForeignKeyCounts(Table& parent,
                                                 Transaction& txn,
                                                 size_t parent_offset);

// True when a non-null child key has no match in the parent multiset.
bool IsForeignKeyViolation(const ForeignKeyCounts& counts, const Value& key);

// True when a non-null parent key is still referenced by a row of the child
// table.  `ignore` holds positions already removed by the running statement
// (NO ACTION is statement-end semantics: rows the statement itself deletes
// do not keep the parent alive).
bool IsParentKeyReferenced(Table& child, Transaction& txn, size_t child_offset,
                           const Value& parent_key,
                           const std::vector<RowPosition>& ignore);

}  // namespace tinylamb

#endif
