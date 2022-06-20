//
// Created by kumagi on 22/06/19.
//

#include "index_scan.hpp"

#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"

namespace tinylamb {

IndexScan::IndexScan(Transaction& txn, Table* table, Index* index,
                     const Row& begin, const Row& end, bool ascending)
    : iter_(new IndexScanIterator(table, index, &txn, begin, end, ascending)) {}

bool IndexScan::Next(Row* dst, RowPosition* rp) {
  if (!iter_.IsValid()) {
    return false;
  }
  *rp = iter_.Position();
  *dst = *iter_;
  ++iter_;
  return true;
}

void IndexScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << iter_;
}

}  // namespace tinylamb