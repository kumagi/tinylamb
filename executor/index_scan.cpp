//
// Created by kumagi on 22/06/19.
//

#include "index_scan.hpp"

#include <utility>

#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"

namespace tinylamb {

IndexScan::IndexScan(Transaction& txn, const Table& table, const Index& index,
                     const Value& begin, const Value& end, bool ascending,
                     Expression where, const Schema& sc)
    : iter_(new IndexScanIterator(table, index, txn, begin, end, ascending)),
      cond_(std::move(where)),
      schema_(sc) {}

bool IndexScan::Next(Row* dst, RowPosition* rp) {
  do {
    if (!iter_.IsValid()) {
      return false;
    }
    RowPosition pointed_row = iter_.Position();
    if (rp != nullptr) {
      *rp = pointed_row;
    }
    *dst = *iter_;
    ++iter_;
  } while (!cond_->Evaluate(*dst, schema_).Truthy());
  return true;
}

void IndexScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << iter_ << " where " << *cond_;
}

}  // namespace tinylamb