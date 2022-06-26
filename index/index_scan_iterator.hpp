//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_INDEX_SCAN_ITERATOR_HPP
#define TINYLAMB_INDEX_SCAN_ITERATOR_HPP

#include "index/b_plus_tree.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScanIterator : public IteratorBase {
 public:
  IndexScanIterator(const Table& table, const Index& index, Transaction& txn,
                    const Value& begin, const Value& end, bool ascending);
  ~IndexScanIterator() override = default;
  bool operator==(const IndexScanIterator& rhs) const {
    return bpt_ == rhs.bpt_ && &txn_ == &rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }
  [[nodiscard]] bool IsValid() const override;
  [[nodiscard]] RowPosition Position() const override;
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  const Row& operator*() const override;
  Row& operator*() override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Table;
  friend class IndexScan;
  void ResolveCurrentRow();

  const Table& table_;
  const Index& index_;
  Transaction& txn_;
  Value begin_;
  Value end_;
  bool ascending_;
  BPlusTree bpt_;
  BPlusTreeIterator iter_;
  Row current_row_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_ITERATOR_HPP
