//
// Created by kumagi on 2022/02/09.
//

#include "table/full_scan_iterator.hpp"

#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

FullScanIterator::FullScanIterator(const Table* table, Transaction* txn)
    : table_(table), txn_(txn), pos_(table_->first_pid_, 0) {
  txn_->AddReadSet(pos_);
  PageRef page = txn->PageManager()->GetPage(pos_.page_id);
  if (page->RowCount() == 0) {
    pos_.page_id = ~0LLU;
    return;
  }
  while (page->Read(*txn, pos_.slot).GetStatus() == Status::kNotExists) {
    pos_.slot++;
  }
  ASSIGN_OR_CRASH(std::string_view, row, page->Read(*txn, pos_.slot));
  current_row_.Deserialize(row.data(), table_->schema_);
}

IteratorBase& FullScanIterator::operator++() {
  PageRef ref = [&]() {
    PageRef ref = txn_->PageManager()->GetPage(pos_.page_id);
    if (++pos_.slot < ref->RowCount()) {
      return ref;
    }
    pos_.page_id = ref->body.row_page.next_page_id_;
    ref.PageUnlock();
    if (pos_.page_id == 0) {
      pos_.page_id = ~0LLU;
      return PageRef();
    }
    pos_.slot = 0;
    return txn_->PageManager()->GetPage(pos_.page_id);
  }();
  if (!ref.IsValid()) {
    current_row_.Clear();
    return *this;
  }
  txn_->AddReadSet(pos_);
  while (ref->Read(*txn_, pos_.slot).GetStatus() == Status::kNotExists) {
    pos_.slot++;
  }
  ASSIGN_OR_CRASH(std::string_view, row, ref->Read(*txn_, pos_.slot));
  current_row_.Deserialize(row.data(), table_->schema_);
  return *this;
}

IteratorBase& FullScanIterator::operator--() {
  assert(!"not implemented function");
  return *this;
}

bool FullScanIterator::IsValid() const { return pos_.IsValid(); }

const Row& FullScanIterator::operator*() const { return current_row_; }
Row& FullScanIterator::operator*() { return current_row_; }

void FullScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->schema_.Name();
}

}  // namespace tinylamb
