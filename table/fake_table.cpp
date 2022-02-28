//
// Created by kumagi on 2022/02/23.
//

#include "table/fake_table.hpp"

#include <cassert>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

Status FakeTable::Insert(Transaction& txn, const Row& row, RowPosition* rp) {
  table_.push_back(row);
  rp->page_id = 1;
  rp->slot = table_.size() - 1;
  return Status::kSuccess;
}

Status FakeTable::Update(Transaction& txn, RowPosition pos, const Row& row) {
  if (table_.size() <= pos.slot) return Status::kNotExists;
  table_[pos.slot] = row;
  return Status::kSuccess;
}

Status FakeTable::Delete(Transaction& txn, RowPosition pos) {
  if (table_.size() <= pos.slot) return Status::kNotExists;
  table_.erase(table_.begin() + pos.slot);
  return Status::kSuccess;
}

Status FakeTable::Read(Transaction& txn, RowPosition pos, Row* result) const {
  if (table_.size() <= pos.slot) return Status::kNotExists;
  *result = table_[pos.slot];
  return Status::kSuccess;
}

Status FakeTable::ReadByKey(Transaction& txn, std::string_view index_name,
                            const Row& keys, Row* result) const {
  return Status::kNotExists;
}

Iterator FakeTable::BeginFullScan(Transaction& txn) const {
  return Iterator(new FakeIterator(table_));
}

Iterator FakeTable::BeginIndexScan(Transaction& txn,
                                   std::string_view index_name,
                                   const Row& begin, const Row& end,
                                   bool ascending) {
  assert(!"not implemented");
}

}  // namespace tinylamb