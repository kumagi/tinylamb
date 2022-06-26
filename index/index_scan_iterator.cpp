//
// Created by kumagi on 2022/02/21.
//

#include "index_scan_iterator.hpp"

#include "index/b_plus_tree.hpp"
#include "index/index.hpp"
#include "page/page_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

IndexScanIterator::IndexScanIterator(const Table& table, const Index& index,
                                     Transaction& txn, Row begin, Row end,
                                     bool ascending)
    : table_(table),
      index_(index),
      txn_(txn),
      begin_(std::move(begin)),
      end_(std::move(end)),
      ascending_(ascending),
      bpt_(index.Root()),
      iter_(&bpt_, &txn, begin_.EncodeMemcomparableFormat(),
            end_.EncodeMemcomparableFormat(), ascending) {
  std::string_view pos = iter_.Value();
  RowPosition rp;
  rp.Deserialize(pos.data());
  txn_.AddReadSet(rp);
  PageRef page = txn.PageManager()->GetPage(rp.page_id);
  ASSIGN_OR_CRASH(std::string_view, row, page->Read(txn, rp.slot));
  current_row_.Deserialize(row.data(), table_.schema_);
}

bool IndexScanIterator::IsValid() const { return iter_.IsValid(); }

RowPosition IndexScanIterator::Position() const {
  if (!iter_.IsValid()) {
    return {};
  }
  std::string_view pos = iter_.Value();
  RowPosition rp;
  rp.Deserialize(pos.data());
  return rp;
}

void IndexScanIterator::ResolveCurrentRow() {
  if (!iter_.IsValid()) {
    current_row_.Clear();
    return;
  }
  std::string_view pos = iter_.Value();
  RowPosition rp;
  rp.Deserialize(pos.data());
  PageRef ref = txn_.PageManager()->GetPage(rp.page_id);
  if (!ref.IsValid()) {
    current_row_.Clear();
    return;
  }
  txn_.AddReadSet(rp);
  ASSIGN_OR_CRASH(std::string_view, row, ref->Read(txn_, rp.slot));
  current_row_.Deserialize(row.data(), table_.schema_);
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

Row& IndexScanIterator::operator*() { return current_row_; }

void IndexScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << table_.GetSchema().Name() << ": {";
  for (size_t i = 0; i < index_.sc_.key_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << index_.sc_.key_[i];
  }
  o << "} [";
  if (ascending_) {
    o << begin_ << " -> " << end_;
  } else {
    o << end_ << " -> " << begin_;
  }
  o << "]";
}

}  // namespace tinylamb