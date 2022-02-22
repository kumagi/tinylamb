//
// Created by kumagi on 2022/02/09.
//

#ifndef TINYLAMB_FULL_SCAN_ITERATOR_HPP
#define TINYLAMB_FULL_SCAN_ITERATOR_HPP
#include <memory>

#include "page/row_position.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Table;
class Transaction;

class FullScanIterator : public IteratorBase {
 public:
  ~FullScanIterator() override = default;
  bool operator==(const FullScanIterator& rhs) const {
    return table_ == rhs.table_ && txn_ == rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }

  bool operator!=(const FullScanIterator& rhs) const {
    return !(operator==(rhs));
  }

  [[nodiscard]] bool IsValid() const override;
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  const Row& operator*() const override;
  Row& operator*() override;

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
