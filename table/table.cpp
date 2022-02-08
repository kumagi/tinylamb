//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include "b_plus_tree.hpp"
#include "page/page_manager.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

Status Table::Insert(Transaction& txn, const Row& row, RowPosition* rp) {
  PageRef ref = pm_->GetPage(last_pid_);
  std::string serialized_row(row.Size(), ' ');
  row.Serialize(serialized_row.data());
  slot_t pos;
  Status result = ref->Insert(txn, serialized_row, &pos);
  rp->page_id = ref->page_id;
  rp->slot = pos;
  if (result == Status::kNoSpace) {
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kRowPage);
    Status new_result = new_page->Insert(txn, serialized_row, &pos);
    rp->page_id = new_page->PageID();
    rp->slot = pos;
    ref->body.row_page.next_page_id_ = new_page->PageID();
    new_page->body.row_page.prev_page_id_ = ref->PageID();
    last_pid_ = new_page->PageID();
  }
  for (auto& idx : indices_) {
    BPlusTree bpt(idx.pid_, pm_);
    bpt.Insert(txn, idx.GenerateKey(row), rp->AsStringView());
  }
  return Status::kSuccess;
}

Status Table::Update(Transaction& txn, RowPosition pos, const Row& row) {
  Row original_row;
  Read(txn, pos, &original_row);
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_, pm_);
    bpt.Delete(txn, idx.GenerateKey(original_row));
    bpt.Insert(txn, idx.GenerateKey(row), pos.AsStringView());
  }
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  return pm_->GetPage(pos.page_id)->Update(txn, pos.slot, serialized_row);
}

Status Table::Delete(Transaction& txn, RowPosition pos) const {
  Row original_row;
  Read(txn, pos, &original_row);
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_, pm_);
    bpt.Delete(txn, idx.GenerateKey(original_row));
  }
  return pm_->GetPage(pos.page_id)->Delete(txn, pos.slot);
}

Status Table::Read(Transaction& txn, RowPosition pos, Row* result) const {
  PageRef ref = pm_->GetPage(pos.page_id);
  std::string_view read_row;
  Status read_result = ref->Read(txn, pos.slot, &read_row);
  if (read_result != Status::kSuccess) return read_result;
  result->Deserialize(read_row.data(), schema_);
  return read_result;
}

Status Table::ReadByKey(Transaction& txn, std::string_view index_name,
                        const Row& keys, Row* result) const {
  for (const auto& index : indices_) {
    if (index.name_ != index_name) continue;
    std::string encoded_key = keys.EncodeMemcomparableFormat();
    BPlusTree bpt(index.pid_, pm_);
    std::string_view val;
    Status status = bpt.Read(txn, encoded_key, &val);
    if (status == Status::kSuccess) {
      RowPosition pos;
      pos.Deserialize(val.data());
      std::string_view result_payload;
      Status read =
          pm_->GetPage(pos.page_id)->Read(txn, pos.slot, &result_payload);
      if (read == Status::kSuccess) {
        result->Deserialize(result_payload.data(), schema_);
        return Status::kSuccess;
      } else {
        return Status::kNotExists;
      }
    } else if (status == Status::kNotExists) {
      return Status::kNotExists;
    } else {
      return Status::kConflicts;  // TODO(kumagi): is it true?
    }
  }
  return Status::kNotExists;
}

}  // namespace tinylamb