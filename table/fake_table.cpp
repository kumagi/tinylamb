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

Status FakeTable::Insert(Transaction&, const Row& row, RowPosition* rp) {
  table_.push_back(row);
  rp->page_id = 1;
  rp->slot = table_.size() - 1;
  return Status::kSuccess;
}

Status FakeTable::Update(Transaction&, RowPosition pos, const Row& row) {
  if (table_.size() <= pos.slot) return Status::kNotExists;
  table_[pos.slot] = row;
  return Status::kSuccess;
}

Status FakeTable::Delete(Transaction&, RowPosition pos) {
  if (table_.size() <= pos.slot) return Status::kNotExists;
  table_.erase(table_.begin() + pos.slot);
  return Status::kSuccess;
}

Status FakeTable::Read(Transaction&, RowPosition pos, Row* result) const {
  if (table_.size() <= pos.slot) return Status::kNotExists;
  *result = table_[pos.slot];
  return Status::kSuccess;
}

Status FakeTable::ReadByKey(Transaction&, std::string_view, const Row&,
                            Row*) const {
  return Status::kNotExists;
}

Iterator FakeTable::BeginFullScan(Transaction&) const {
  return Iterator(new FakeIterator(table_));
}

Iterator FakeTable::BeginIndexScan(Transaction&, std::string_view, const Row&,
                                   const Row&, bool) {
  assert(!"not implemented");
}

}  // namespace tinylamb