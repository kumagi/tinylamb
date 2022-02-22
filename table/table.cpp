//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include "b_plus_tree.hpp"
#include "index_scan_iterator.hpp"
#include "page/page_manager.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

Status Table::Insert(Transaction& txn, const Row& row, RowPosition* rp) {
  PageRef ref = pm_->GetPage(last_pid_);
  std::string serialized_row(row.Size(), ' ');
  row.Serialize(serialized_row.data());
  slot_t pos;
  Status s = ref->Insert(txn, serialized_row, &pos);
  rp->page_id = ref->page_id;
  rp->slot = pos;
  if (s == Status::kNoSpace) {
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kRowPage);
    s = new_page->Insert(txn, serialized_row, &pos);
    if (s != Status::kSuccess) return s;
    rp->page_id = new_page->PageID();
    rp->slot = pos;
    ref->body.row_page.next_page_id_ = new_page->PageID();
    new_page->body.row_page.prev_page_id_ = ref->PageID();
    last_pid_ = new_page->PageID();
  }
  for (auto& idx : indices_) {
    BPlusTree bpt(idx.pid_, pm_);
    s = bpt.Insert(txn, idx.GenerateKey(row), rp->AsStringView());
    if (s != Status::kSuccess) return s;
  }
  return s;
}

Status Table::Update(Transaction& txn, RowPosition pos, const Row& row) {
  Row original_row;
  Status s = Read(txn, pos, &original_row);
  if (s != Status::kSuccess) return s;
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_, pm_);
    s = bpt.Delete(txn, idx.GenerateKey(original_row));
    if (s != Status::kSuccess) return s;
    s = bpt.Insert(txn, idx.GenerateKey(row), pos.AsStringView());
    if (s != Status::kSuccess) return s;
  }
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  return pm_->GetPage(pos.page_id)->Update(txn, pos.slot, serialized_row);
}

Status Table::Delete(Transaction& txn, RowPosition pos) {
  Row original_row;
  Status s = Read(txn, pos, &original_row);
  if (s != Status::kSuccess) return s;
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_, pm_);
    s = bpt.Delete(txn, idx.GenerateKey(original_row));
    if (s != Status::kSuccess) return s;
  }
  return pm_->GetPage(pos.page_id)->Delete(txn, pos.slot);
}

Status Table::Read(Transaction& txn, RowPosition pos, Row* result) const {
  std::string_view read_row;
  Status s = pm_->GetPage(pos.page_id)->Read(txn, pos.slot, &read_row);
  if (s != Status::kSuccess) return s;
  result->Deserialize(read_row.data(), schema_);
  return s;
}

Status Table::ReadByKey(Transaction& txn, std::string_view index_name,
                        const Row& keys, Row* result) const {
  for (const auto& index : indices_) {
    if (index.name_ != index_name) continue;
    std::string encoded_key = keys.EncodeMemcomparableFormat();
    BPlusTree bpt(index.pid_, pm_);
    std::string_view val;
    Status s = bpt.Read(txn, encoded_key, &val);
    if (s != Status::kSuccess) return s;
    RowPosition pos;
    pos.Deserialize(val.data());
    std::string_view result_payload;
    s = pm_->GetPage(pos.page_id)->Read(txn, pos.slot, &result_payload);
    if (s != Status::kSuccess) return s;
    result->Deserialize(result_payload.data(), schema_);
    return Status::kSuccess;
  }
  return Status::kNotExists;
}

Iterator Table::BeginFullScan(Transaction& txn) {
  return Iterator(new FullScanIterator(this, &txn));
}

Iterator Table::BeginIndexScan(Transaction& txn, std::string_view index_name,
                               const Row& begin, const Row& end,
                               bool ascending) {
  return Iterator(
      new IndexScanIterator(this, index_name, &txn, begin, end, ascending));
}

page_id_t Table::GetIndex(std::string_view name) {
  for (const auto& idx : indices_) {
    if (idx.name_ == name) return idx.pid_;
  }
  return -1;
}

}  // namespace tinylamb