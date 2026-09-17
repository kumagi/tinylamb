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
#include <vector>

#include "index/b_plus_tree.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScanIterator : public IteratorBase {
 public:
  // resolve_head marks a DML source scan: ResolveRow then serves the
  // position's newest committed version when the transaction holds an
  // unstaged write intent on it.  Plain scans keep the default (pure
  // snapshot read) so an intent left by an earlier DML statement cannot
  // leak a post-snapshot commit into a SELECT.
  IndexScanIterator(const Table& table, const Index& index, Transaction& txn,
                    const Value& begin = Value(), const Value& end = Value(),
                    bool ascending = true, bool resolve_head = false);
  IndexScanIterator(const Table& table, const Index& index, Transaction& txn,
                    const std::vector<Value>& begin_key,
                    const std::vector<Value>& end_key, bool ascending = true,
                    bool resolve_head = false);
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
  [[nodiscard]] Status GetStatus() const override { return status_; }
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
  // kUnique entries normally hold a single position per key, but a key
  // carrying a NULL is stored (and must be decoded/navigated) as a
  // multi-value list: SQL says NULL != NULL, so several rows can share one
  // NULL key (see Table::IndexInsert).
  [[nodiscard]] bool StoredAsSingleValue() const;

  const Table& table_;
  const Index& index_;
  Transaction& txn_;
  Value begin_;
  Value end_;
  bool ascending_;
  bool resolve_head_;
  bool is_unique_;
  // Set by Clear(): once cleared the iterator must never report valid again
  // even though the underlying BPlusTreeIterator may still iterate.
  bool invalidated_{false};
  Status status_{Status::kSuccess};
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
