/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_UNIQUE_INDEX_SCAN_ITERATOR_HPP
#define TINYLAMB_UNIQUE_INDEX_SCAN_ITERATOR_HPP

#include <cstdint>

#include "index/b_plus_tree.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

#include <vector>

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScanIterator : public IteratorBase {
 public:
  IndexScanIterator(const Table& table, const Index& index, Transaction& txn,
                    const Value& begin = Value(), const Value& end = Value(),
                    bool ascending = true);
  IndexScanIterator(const Table& table, const Index& index, Transaction& txn,
                    const std::vector<Value>& begin_key,
                    const std::vector<Value>& end_key,
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
  // Set by Clear(): once cleared the iterator must never report valid again
  // even though the underlying BPlusTreeIterator may still iterate.
  bool invalidated_{false};
  // -1 means "not positioned"; otherwise an index into the non-unique
  // value list of the current key. Signed so the sentinel cannot wrap.
  int64_t value_offset_{-1};
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
