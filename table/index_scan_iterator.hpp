//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_INDEX_SCAN_ITERATOR_HPP
#define TINYLAMB_INDEX_SCAN_ITERATOR_HPP

#include "b_plus_tree.hpp"
#include "b_plus_tree_iterator.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Table;
class Transaction;

class IndexScanIterator {
 public:
  bool operator==(const IndexScanIterator& rhs) const {
    return bpt_ == rhs.bpt_ && txn_ == rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }

  bool operator!=(const IndexScanIterator& rhs) const {
    return !(operator==(rhs));
  }

  [[nodiscard]] bool IsValid() const;
  IndexScanIterator& operator++();
  IndexScanIterator& operator--();
  const Row& operator*() const;
  Row& operator*();

 private:
  friend class Table;
  IndexScanIterator(Table* table, std::string_view index_name, Transaction* txn,
                    const Row& begin, const Row& end = Row(),
                    bool ascending = true);
  void ResolveCurrentRow();

  Table* table_;
  BPlusTree bpt_;
  Transaction* txn_;
  BPlusTreeIterator iter_;
  Row current_row_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_ITERATOR_HPP
