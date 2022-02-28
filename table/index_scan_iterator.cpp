//
// Created by kumagi on 2022/02/21.
//

#include "table/index_scan_iterator.hpp"

#include "page/page_manager.hpp"
#include "table/b_plus_tree.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

IndexScanIterator::IndexScanIterator(Table* table, std::string_view index_name,
                                     Transaction* txn, const Row& begin,
                                     const Row& end, bool ascending)
    : table_(table),
      bpt_(BPlusTree(table_->GetIndex(index_name), table_->pm_)),
      txn_(txn),
      iter_(&bpt_, txn, begin.EncodeMemcomparableFormat(),
            end.EncodeMemcomparableFormat(), ascending) {
  std::string_view pos = *iter_;
  RowPosition rp;
  rp.Deserialize(pos.data());
  txn_->AddReadSet(rp);
  PageRef page = table_->pm_->GetPage(rp.page_id);
  std::string_view row;
  page->Read(*txn, rp.slot, &row);
  current_row_.Deserialize(row.data(), table_->schema_);
}

bool IndexScanIterator::IsValid() const { return iter_.IsValid(); }

void IndexScanIterator::ResolveCurrentRow() {
  if (!iter_.IsValid()) {
    current_row_.Clear();
    return;
  }
  std::string_view pos = *iter_;
  RowPosition rp;
  rp.Deserialize(pos.data());
  PageRef ref = table_->pm_->GetPage(rp.page_id);
  if (!ref.IsValid()) {
    current_row_.Clear();
    return;
  }
  txn_->AddReadSet(rp);
  std::string_view row;
  ref->Read(*txn_, rp.slot, &row);
  current_row_.Deserialize(row.data(), table_->schema_);
}

IteratorBase& IndexScanIterator::operator++() {
  ++iter_;
  ResolveCurrentRow();
  return *this;
}

IteratorBase& IndexScanIterator::operator--() {
  --iter_;
  ResolveCurrentRow();
  return *this;
}

const Row& IndexScanIterator::operator*() const { return current_row_; }

}  // namespace tinylamb