//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_UNIQUE_INDEX_SCAN_ITERATOR_HPP
#define TINYLAMB_UNIQUE_INDEX_SCAN_ITERATOR_HPP

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
                    const Value& begin = Value(), const Value& end = Value(),
                    bool ascending = true);
  IndexScanIterator(const IndexScanIterator&) = delete;
  IndexScanIterator(IndexScanIterator&&) = delete;
  IndexScanIterator& operator=(const IndexScanIterator&) = delete;
  IndexScanIterator& operator=(IndexScanIterator&&) = delete;
  ~IndexScanIterator() override = default;
  bool operator==(const IndexScanIterator& rhs) const {
    return bpt_ == rhs.bpt_ && &txn_ == &rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }
  [[nodiscard]] bool IsValid() const override;
  [[nodiscard]] bool IsUnique() const { return is_unique_; }
  [[nodiscard]] const Row& GetKey() const { return keys_; }
  [[nodiscard]] const Row& Include() const { return include_; }
  [[nodiscard]] std::string GetValue() const;
  [[nodiscard]] RowPosition Position() const override;
  void Clear();
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  const Row& operator*() const override;
  Row& operator*() override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Table;
  friend class IndexScan;
  void UpdateIteratorState();
  void ResolveRow() const;

  const Table& table_;
  const Index& index_;
  Transaction& txn_;
  Value begin_;
  Value end_;
  bool ascending_;
  bool is_unique_;
  size_t value_offset_;
  BPlusTree bpt_;
  BPlusTreeIterator iter_;
  RowPosition pos_;
  Row keys_;
  Row include_;
  mutable Row current_row_;
  mutable bool current_row_resolved_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_UNIQUE_INDEX_SCAN_ITERATOR_HPP
