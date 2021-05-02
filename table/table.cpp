//
// Created by kumagi on 2022/02/06.
//

#include "table/table.hpp"

#include <algorithm>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "index/b_plus_tree.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/page_manager.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

Encoder& operator<<(Encoder& e, const Table::IndexValueType& v) {
  e << v.pos << v.include;
  return e;
}

Decoder& operator>>(Decoder& d, Table::IndexValueType& t) {
  d >> t.pos >> t.include;
  return d;
}

Status Table::CreateIndex(Transaction& txn, const IndexSchema& idx) {
  page_id_t new_root;
  {
    PageRef root_page =
        txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
    new_root = root_page->PageID();
    indexes_.emplace_back(idx.name_, idx.key_, root_page->PageID(),
                          idx.include_, idx.mode_);
  }

  Iterator it = BeginFullScan(txn);
  BPlusTree new_bpt(new_root);
  while (it.IsValid()) {
    RETURN_IF_FAIL(IndexInsert(txn, indexes_.back(), *it, it.Position()));
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
    rp.page_id = ref->PageID();
    rp.slot = pos.Value();
  } else if (pos.GetStatus() == Status::kNoSpace) {
    bool finished = false;
    while (ref->body.row_page.next_page_id_ != 0) {
      PageRef next =
          txn.PageManager()->GetPage(ref->body.row_page.next_page_id_);
      ref = std::move(next);
      StatusOr<slot_t> next_pos = next->Insert(txn, serialized_row);
      if (next_pos.HasValue()) {
        rp.page_id = next->PageID();
        rp.slot = next_pos.Value();
        finished = true;
        break;
      }
    }
    if (!finished) {
      PageRef new_page =
          txn.PageManager()->AllocateNewPage(txn, PageType::kRowPage);
      ASSIGN_OR_RETURN(slot_t, new_pos, new_page->Insert(txn, serialized_row));
      rp.page_id = new_page->PageID();
      rp.slot = new_pos;
      ref->body.row_page.next_page_id_ = new_page->PageID();
      new_page->body.row_page.prev_page_id_ = ref->PageID();
      last_pid_ = new_page->PageID();
    }
  }
  ref.PageUnlock();
  for (const auto& idx : indexes_) {
    RETURN_IF_FAIL(IndexInsert(txn, idx, row, rp));
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
  for (const auto& idx : indexes_) {
    RETURN_IF_FAIL(IndexDelete(txn, idx, pos));
  }
  std::string serialized_row(row.Size(), '\0');
  row.Serialize(serialized_row.data());
  PageRef page = txn.PageManager()->GetPage(new_pos.page_id);
  Status s = page->Update(txn, new_pos.slot, serialized_row);
  if (s == Status::kNoSpace) {
    page->Delete(txn, new_pos.slot);
    bool finished = false;
    while (page->body.row_page.next_page_id_ != 0) {
      page_id_t next_page = page->body.row_page.next_page_id_;
      page = txn.PageManager()->GetPage(next_page);
      StatusOr<slot_t> next_pos = page->Insert(txn, serialized_row);
      if (next_pos.HasValue()) {
        new_pos.page_id = page->PageID();
        new_pos.slot = next_pos.Value();
        finished = true;
        break;
      }
    }
    if (!finished) {
      PageRef new_page =
          txn.PageManager()->AllocateNewPage(txn, PageType::kRowPage);
      ASSIGN_OR_RETURN(slot_t, new_slot, new_page->Insert(txn, serialized_row));
      RowPosition new_row_pos(new_page->PageID(), new_slot);
      page.PageUnlock();
      {
        PageRef last_page = txn.PageManager()->GetPage(last_pid_);
        last_page->body.row_page.next_page_id_ = new_page->PageID();
        new_page->body.row_page.prev_page_id_ = last_page->PageID();
      }
      new_pos = new_row_pos;
      last_pid_ = new_page->PageID();
    }
  }
  for (const auto& idx : indexes_) {
    RETURN_IF_FAIL(IndexInsert(txn, idx, row, new_pos));
  }
  return new_pos;
}

Status Table::Delete(Transaction& txn, RowPosition pos) {
  if (!txn.AddWriteSet(pos)) {
    return Status::kConflicts;
  }
  for (const auto& idx : indexes_) {
    RETURN_IF_FAIL(IndexDelete(txn, idx, pos));
  }
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

Status Table::IndexInsert(Transaction& txn, const Index& idx,
                          const Row& new_row, const RowPosition& pos) {
  BPlusTree bpt(idx.pid_);
  std::vector<Value> include_values;
  include_values.reserve(idx.sc_.include_.size());
  for (const auto& in : idx.sc_.include_) {
    include_values.push_back(new_row[in]);
  }
  Row include(include_values);
  if (idx.IsUnique()) {
    // BTree value is encoded pair<RowPosition, vector<Value>>.
    IndexValueType val;
    val.pos = pos;
    val.include = include;
    RETURN_IF_FAIL(bpt.Insert(txn, idx.GenerateKey(new_row), Encode(val)));
  } else {
    // BTree value is encoded vector<pair<RowPosition, vector<Value>>>.
    StatusOr<std::string_view> existing_value =
        bpt.Read(txn, idx.GenerateKey(new_row));
    if (existing_value.HasValue()) {
      std::string_view existing_data = existing_value.Value();
      auto rps = Decode<std::vector<IndexValueType>>(existing_data);
      rps.push_back({pos, include});
      RETURN_IF_FAIL(bpt.Update(txn, idx.GenerateKey(new_row), Encode(rps)));
    } else {
      std::vector<IndexValueType> rps{{pos, include}};
      RETURN_IF_FAIL(bpt.Insert(txn, idx.GenerateKey(new_row), Encode(rps)));
    }
  }
  return Status::kSuccess;
}

Status Table::IndexDelete(Transaction& txn, const Index& idx,
                          const RowPosition& pos) {
  ASSIGN_OR_RETURN(Row, original_row, Read(txn, pos));
  BPlusTree bpt(idx.pid_);
  std::string key = idx.GenerateKey(original_row);
  if (idx.IsUnique()) {
    RETURN_IF_FAIL(bpt.Delete(txn, key));
  } else {
    ASSIGN_OR_RETURN(std::string_view, existing_data,
                     bpt.Read(txn, idx.GenerateKey(original_row)));
    auto values = Decode<std::vector<IndexValueType>>(existing_data);
    auto new_end =
        std::remove_if(values.begin(), values.end(),
                       [&](const IndexValueType& v) { return v.pos == pos; });
    values.erase(new_end, values.end());
    if (values.empty()) {
      RETURN_IF_FAIL(bpt.Delete(txn, key));
    } else {
      RETURN_IF_FAIL(bpt.Update(txn, key, Encode(values)));
    }
  }
  return Status::kSuccess;
}

Iterator Table::BeginFullScan(Transaction& txn) const {
  return Iterator(new FullScanIterator(this, &txn));
}

Iterator Table::BeginIndexScan(Transaction& txn, const Index& index,
                               const Value& begin, const Value& end,
                               bool ascending) const {
  return Iterator(
      new IndexScanIterator(*this, index, txn, begin, end, ascending));
}

std::unordered_map<slot_t, size_t> Table::AvailableKeyIndex() const {
  std::unordered_map<slot_t, size_t> ret;
  for (size_t i = 0; i < indexes_.size(); ++i) {
    ret.emplace(indexes_[i].sc_.key_[0], i);
  }
  return ret;
}

Encoder& operator<<(Encoder& e, const Table& t) {
  e << t.schema_ << t.first_pid_ << t.last_pid_ << t.indexes_;
  return e;
}

Decoder& operator>>(Decoder& d, Table& t) {
  d >> t.schema_ >> t.first_pid_ >> t.last_pid_ >> t.indexes_;
  return d;
}

}  // namespace tinylamb