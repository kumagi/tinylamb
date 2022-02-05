//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

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
  if (result == Status::kSuccess) return result;
  if (result == Status::kNoSpace) {
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kRowPage);
    Status new_result = new_page->Insert(txn, serialized_row, &pos);
    rp->page_id = new_page->PageID();
    rp->slot = pos;
    ref->body.row_page.next_page_id_ = new_page->PageID();
    new_page->body.row_page.prev_page_id_ = ref->PageID();
    last_pid_ = new_page->PageID();
    return new_result;
  }
  throw std::runtime_error("insert failed");
}

Status Table::Update(Transaction& txn, RowPosition pos, const Row& row) {
  PageRef ref = pm_->GetPage(pos.page_id);
  std::string serialized_row(row.Size(), ' ');
  row.Serialize(serialized_row.data());
  Status update_result = ref->Update(txn, pos.slot, serialized_row);
  return update_result;
}

Status Table::Delete(Transaction& txn, RowPosition pos) {
  PageRef ref = pm_->GetPage(pos.page_id);
  Status delete_result = ref->Delete(txn, pos.slot);
  return delete_result;
}

Status Table::Read(Transaction& txn, RowPosition pos, Row* result) {
  PageRef ref = pm_->GetPage(pos.page_id);
  std::string_view read_row;
  Status read_result = ref->Read(txn, pos.slot, &read_row);
  if (read_result != Status::kSuccess) return read_result;
  result->Deserialize(read_row.data(), schema_);
  return read_result;
}

}  // namespace tinylamb