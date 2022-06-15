//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "index/b_plus_tree.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/page_manager.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

Status Table::CreateIndex(Transaction& txn, const IndexSchema& idx) {
  PageRef root_page =
      txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
  BPlusTree new_bpt(root_page->PageID());
  indexes_.emplace_back(idx.name_, idx.key_, root_page->PageID(), idx.include_,
                        idx.mode_);

  Iterator it = BeginFullScan(txn);
  while (it.IsValid()) {
    RETURN_IF_FAIL(
        new_bpt.Insert(txn, it->Extract(idx.key_).EncodeMemcomparableFormat(),
                       it.Position().Serialize()));
    ++it;
  }
  return Status::kSuccess;
}

StatusOr<RowPosition> Table::Insert(Transaction& txn, const Row& row) {
  PageRef ref = txn.PageManager()->GetPage(last_pid_);
  std::string serialized_row(row.Size(), ' ');
  row.Serialize(serialized_row.data());
  StatusOr<slot_t> pos = ref->Insert(txn, serialized_row);
  RowPosition rp;
  if (pos.HasValue()) {
    rp.page_id = ref->page_id;
    rp.slot = pos.Value();
  }
  if (pos.GetStatus() == Status::kNoSpace) {
    PageRef new_page =
        txn.PageManager()->AllocateNewPage(txn, PageType::kRowPage);
    ASSIGN_OR_RETURN(slot_t, new_pos, new_page->Insert(txn, serialized_row));
    rp.page_id = new_page->PageID();
    rp.slot = new_pos;
    ref->body.row_page.next_page_id_ = new_page->PageID();
    new_page->body.row_page.prev_page_id_ = ref->PageID();
    last_pid_ = new_page->PageID();
  }
  ref.PageUnlock();
  RETURN_IF_FAIL(IndexInsert(txn, row, rp));
  return rp;
}

StatusOr<RowPosition> Table::Update(Transaction& txn, const RowPosition& pos,
                                    const Row& row) {
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  RowPosition new_pos = pos;
  RETURN_IF_FAIL(IndexDelete(txn, pos));
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  PageRef page = txn.PageManager()->GetPage(new_pos.page_id);
  Status s = page->Update(txn, new_pos.slot, serialized_row);
  if (s == Status::kNoSpace) {
    page->Delete(txn, new_pos.slot);
    page.PageUnlock();
    PageRef new_page =
        txn.PageManager()->AllocateNewPage(txn, PageType::kRowPage);
    ASSIGN_OR_RETURN(slot_t, new_slot, new_page->Insert(txn, serialized_row));
    RowPosition new_row_pos(new_page->PageID(), new_slot);
    {
      PageRef last_page = txn.PageManager()->GetPage(last_pid_);
      last_page->body.row_page.next_page_id_ = new_page->PageID();
      new_page->body.row_page.prev_page_id_ = last_page->PageID();
    }
    new_pos = new_row_pos;
    last_pid_ = new_page->PageID();
  }
  RETURN_IF_FAIL(IndexInsert(txn, row, new_pos));
  return new_pos;
}

Status Table::Delete(Transaction& txn, RowPosition pos) {
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  RETURN_IF_FAIL(IndexDelete(txn, pos));
  return txn.PageManager()->GetPage(pos.page_id)->Delete(txn, pos.slot);
}

StatusOr<Row> Table::Read(Transaction& txn, RowPosition pos) const {
  ASSIGN_OR_RETURN(
      std::string_view, read_row,
      txn.PageManager()->GetPage(pos.page_id)->Read(txn, pos.slot));
  Row result;
  result.Deserialize(read_row.data(), schema_);
  return result;
}

Status Table::IndexInsert(Transaction& txn, const Row& new_row,
                          const RowPosition& pos) {
  for (const auto& idx : indexes_) {
    BPlusTree bpt(idx.pid_);
    if (idx.IsUnique()) {
      LOG(TRACE) << "insert: " << idx.sc_.name_ << " is unique";
      RETURN_IF_FAIL(
          bpt.Insert(txn, idx.GenerateKey(new_row), pos.Serialize()));
    } else {
      StatusOr<std::string_view> existing_value =
          bpt.Read(txn, idx.GenerateKey(new_row));
      if (existing_value.HasValue()) {
        std::string_view existing_data = existing_value.Value();
        auto rps = Decode<std::vector<RowPosition>>(existing_data);
        rps.push_back(pos);
        RETURN_IF_FAIL(bpt.Update(txn, idx.GenerateKey(new_row), Encode(rps)));
      } else {
        std::vector<RowPosition> rps{pos};
        RETURN_IF_FAIL(bpt.Insert(txn, idx.GenerateKey(new_row), Encode(rps)));
      }
    }
  }
  return Status::kSuccess;
}

Status Table::IndexDelete(Transaction& txn, const RowPosition& pos) {
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  for (const auto& idx : indexes_) {
    BPlusTree bpt(idx.pid_);
    std::string key = idx.GenerateKey(original_row);
    if (idx.IsUnique()) {
      RETURN_IF_FAIL(bpt.Delete(txn, key));
    } else {
      StatusOr<std::string_view> existing_value =
          bpt.Read(txn, idx.GenerateKey(original_row));
      std::string_view existing_data = existing_value.Value();
      auto rps = Decode<std::vector<RowPosition>>(existing_data);
      std::erase(rps, pos);
      if (rps.empty()) {
        RETURN_IF_FAIL(bpt.Delete(txn, key));
      } else {
        RETURN_IF_FAIL(bpt.Update(txn, key, Encode(rps)));
      }
    }
  }
  return Status::kSuccess;
}

Iterator Table::BeginFullScan(Transaction& txn) const {
  return Iterator(new FullScanIterator(this, &txn));
}

Iterator Table::BeginIndexScan(Transaction& txn, std::string_view index_name,
                               const Row& begin, const Row& end,
                               bool ascending) {
  return Iterator(
      new IndexScanIterator(this, index_name, &txn, begin, end, ascending));
}

Encoder& operator<<(Encoder& e, const Table& t) {
  e << t.schema_ << t.first_pid_ << t.last_pid_ << t.indexes_;
  return e;
}

Decoder& operator>>(Decoder& d, Table& t) {
  d >> t.schema_ >> t.first_pid_ >> t.last_pid_ >> t.indexes_;
  return d;
}

page_id_t Table::GetIndex(std::string_view name) {
  for (const auto& idx : indexes_) {
    if (idx.sc_.name_ == name) {
      return idx.pid_;
    }
  }
  return -1;
}

}  // namespace tinylamb