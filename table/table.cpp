//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "index/b_plus_tree.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/page_manager.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

Status Table::CreateIndex(Transaction& txn, const IndexSchema& idx) {
  PageRef root_page =
      txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
  BPlusTree new_bpt(root_page->PageID());
  indices_.emplace_back(idx.name_, idx.key_, root_page->PageID());

  Iterator it = BeginFullScan(txn);
  while (it.IsValid()) {
    Row current = *it;
    new_bpt.Insert(txn, current.Extract(idx.key_).EncodeMemcomparableFormat(),
                   it.Position().Serialize());
  }
  LOG(FATAL) << "leaf is " << root_page->PageID();
  return Status::kSuccess;
}

StatusOr<RowPosition> Table::Insert(Transaction& txn, const Row& row) {
  PageRef ref = txn.PageManager()->GetPage(last_pid_);
  std::string serialized_row(row.Size(), ' ');
  row.Serialize(serialized_row.data());
  slot_t pos;
  Status s = ref->Insert(txn, serialized_row, &pos);
  RowPosition rp;
  rp.page_id = ref->page_id;
  rp.slot = pos;
  if (s == Status::kNoSpace) {
    PageRef new_page =
        txn.PageManager()->AllocateNewPage(txn, PageType::kRowPage);
    RETURN_IF_FAIL(new_page->Insert(txn, serialized_row, &pos));
    rp.page_id = new_page->PageID();
    rp.slot = pos;
    ref->body.row_page.next_page_id_ = new_page->PageID();
    new_page->body.row_page.prev_page_id_ = ref->PageID();
    last_pid_ = new_page->PageID();
  }
  ref.PageUnlock();
  for (auto& idx : indices_) {
    BPlusTree bpt(idx.pid_);
    RETURN_IF_FAIL(bpt.Insert(txn, idx.GenerateKey(row), rp.Serialize()));
  }
  return rp;
}

StatusOr<RowPosition> Table::Update(Transaction& txn, const RowPosition& pos,
                                    const Row& row) {
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  RowPosition new_pos = pos;
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_);
    RETURN_IF_FAIL(bpt.Delete(txn, idx.GenerateKey(original_row)));
  }
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  PageRef page = txn.PageManager()->GetPage(new_pos.page_id);
  Status s = page->Update(txn, new_pos.slot, serialized_row);
  if (s == Status::kNoSpace) {
    page->Delete(txn, new_pos.slot);
    page.PageUnlock();
    PageRef new_page =
        txn.PageManager()->AllocateNewPage(txn, PageType::kRowPage);
    RowPosition new_row_pos(new_page->PageID(), -1);
    RETURN_IF_FAIL(new_page->Insert(txn, serialized_row, &new_row_pos.slot));
    {
      PageRef last_page = txn.PageManager()->GetPage(last_pid_);
      last_page->body.row_page.next_page_id_ = new_page->PageID();
      new_page->body.row_page.prev_page_id_ = last_page->PageID();
    }
    new_pos = new_row_pos;
    last_pid_ = new_page->PageID();
  }
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_);
    RETURN_IF_FAIL(bpt.Insert(txn, idx.GenerateKey(row), pos.Serialize()));
  }
  return new_pos;
}

Status Table::Delete(Transaction& txn, RowPosition pos) {
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  for (const auto& idx : indices_) {
    BPlusTree bpt(idx.pid_);
    RETURN_IF_FAIL(bpt.Delete(txn, idx.GenerateKey(original_row)));
  }
  return txn.PageManager()->GetPage(pos.page_id)->Delete(txn, pos.slot);
}

StatusOr<Row> Table::Read(Transaction& txn, RowPosition pos) const {
  std::string_view read_row;
  RETURN_IF_FAIL(
      txn.PageManager()->GetPage(pos.page_id)->Read(txn, pos.slot, &read_row));
  Row result;
  result.Deserialize(read_row.data(), schema_);
  return result;
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
  e << t.schema_ << t.first_pid_ << t.last_pid_ << t.indices_;
  return e;
}

Decoder& operator>>(Decoder& d, Table& t) {
  d >> t.schema_ >> t.first_pid_ >> t.last_pid_ >> t.indices_;
  return d;
}

page_id_t Table::GetIndex(std::string_view name) {
  for (const auto& idx : indices_) {
    if (idx.sc_.name_ == name) {
      return idx.pid_;
    }
  }
  return -1;
}

}  // namespace tinylamb