//
// Created by kumagi on 2022/02/09.
//

#ifndef TINYLAMB_FULL_SCAN_ITERATOR_HPP
#define TINYLAMB_FULL_SCAN_ITERATOR_HPP

#include "type/row.hpp"

namespace tinylamb {
class Table;
class Transaction;

class FullScanIterator {
 public:
  bool operator==(const FullScanIterator& rhs) const {
    return table_ == rhs.table_ && txn_ == rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }

  bool operator!=(const FullScanIterator& rhs) const {
    return !(operator==(rhs));
  }

  [[nodiscard]] bool IsValid() const;
  FullScanIterator& operator++();
  const Row& operator*() const;
  Row& operator*();

 private:
  friend class Table;
  FullScanIterator(Table* table, Transaction* txn);
  Table* table_;
  Transaction* txn_;
  RowPosition pos_;
  Row current_row_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_ITERATOR_HPP
