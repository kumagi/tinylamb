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

#include "common/incarnation.hpp"
#include "index/index_schema.hpp"
#include "index/lsm_index.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator_base.hpp"
#include "transaction/transaction_manager.hpp"
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
  IndexScanIterator(
      const Table& table, const Index& index, Transaction& txn,
      const Value& begin = Value(), const Value& end = Value(),
      bool ascending = true, bool resolve_head = false,
      TransactionManager::IndexReadGuard* index_read_guard = nullptr);
  IndexScanIterator(
      const Table& table, const Index& index, Transaction& txn,
      const std::vector<Value>& begin_key, const std::vector<Value>& end_key,
      bool ascending = true, bool resolve_head = false,
      TransactionManager::IndexReadGuard* index_read_guard = nullptr);
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
  [[nodiscard]] size_t ValueListsDecoded() const {
    return value_lists_decoded_;
  }
  [[nodiscard]] size_t ValueEntriesDecoded() const {
    return value_entries_decoded_;
  }
  [[nodiscard]] size_t HeapPageImagesCopied() const {
    return heap_page_images_copied_;
  }
  [[nodiscard]] size_t RowsDecoded() const { return rows_decoded_; }
  // Index-only visibility fast path (the MVCC visibility-map equivalent of
  // full_scan_iterator.cpp's PhysicalReadEligible): when the index does not
  // retain deleted entries and the entry's row page carries no version
  // chains, the B+Tree entry itself -- key columns plus INCLUDE payload --
  // is the committed image every snapshot sees, so a covering consumer can
  // be served without the heap fetch.  Returns true and fills |dst| when
  // the entry qualifies; false means the caller must dereference the
  // iterator for the MVCC-resolved row.  Callers must hold the scan's
  // stale-key gate (see IndexOnlyScanPlan::EmitExecutor) and the index read
  // guard (held for the iterator's lifetime).
  [[nodiscard]] bool TryReadIndexOnly(Row* dst) const;
  // RangePartitionedScan support: the key-count-based virtual-leaf partition
  // boundaries of [begin_key, end_key], decoded to key images (strictly
  // ascending, each strictly greater than |begin_key|).  See
  // BPlusTree::ScanPartitionBoundaries; an empty result means one partition
  // covers the whole range.
  static StatusOr<std::vector<Row>> PartitionBoundaries(
      const Index& index, Transaction& txn, const std::vector<Value>& begin_key,
      const std::vector<Value>& end_key, size_t leaves_per_partition);
  // Uses exactly the cursor's inclusive/prefix bound encoding. Unknown/null
  // bounds deliberately overlap everything, keeping stale-key fallbacks safe.
  static bool EncodedKeyMayOverlap(const Index& index,
                                   std::string_view encoded_key,
                                   const std::vector<Value>& begin_key,
                                   const std::vector<Value>& end_key);
  // Snapshot-only projected decoding. Retained indexes require full rows to
  // revalidate their historical key images and deliberately decline this.
  [[nodiscard]] Status SetProjection(const std::vector<slot_t>& columns);
  void Clear();
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  const Row& operator*() const override;
  Row& operator*() override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Table;
  friend class IndexScan;
  friend class IndexOnlyScan;
  // Skip the remainder of this encoded key's value list. The covering scan
  // calls this only after finding a visible representative for a key-only
  // DISTINCT consumer; invisible candidates must still advance row by row.
  void AdvanceToNextKey();
  void SetDistinctKeysMode(bool enable) { distinct_keys_mode_ = enable; }
  void UpdateIteratorState();
  void ResolveRow() const;
  void DecodeVisibleRow(std::string_view image) const;
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
  // Owned images, valid until the B+Tree cursor changes keys. Snapshot scans
  // hold an index read gate; DML resolve-head scans reload before each step.
  std::vector<IndexValueType> value_list_;
  bool entry_loaded_{false};
  bool distinct_keys_mode_{false};
  size_t value_lists_decoded_{0};
  size_t value_entries_decoded_{0};
  TableLease table_lease_;
  TransactionManager::IndexReadGuard index_read_lock_;
  BPlusTree bpt_;
  BPlusTreeIterator iter_;
  RowPosition pos_;
  Row keys_;
  Row include_;
  mutable Row current_row_;
  mutable bool current_row_resolved_{false};
  // Snapshot range scans own one chain-free heap page's row images. No page
  // latch or buffer-pool pointer survives a call. A later writer cannot change
  // the image this fixed read-only snapshot must see.
  bool cache_heap_images_{false};
  mutable page_id_t last_heap_page_{~page_id_t{0}};
  mutable page_id_t heap_image_page_{~page_id_t{0}};
  mutable std::string heap_images_;
  mutable std::vector<std::pair<size_t, size_t>> heap_image_slots_;
  mutable size_t heap_page_images_copied_{0};
  mutable size_t rows_decoded_{0};
  std::vector<IntegerPeekCompare> integer_peeks_;
  std::optional<std::vector<slot_t>> projection_;
  // One-slot memo for the chain probe in TryReadIndexOnly: consecutive
  // entries usually land on the same row page.  A cached verdict stays sound
  // for the scan's lifetime -- see the implementation.
  mutable page_id_t chain_checked_page_{0};
  mutable bool chain_checked_free_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_UNIQUE_INDEX_SCAN_ITERATOR_HPP
