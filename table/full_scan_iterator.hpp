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

#ifndef TINYLAMB_FULL_SCAN_ITERATOR_HPP
#define TINYLAMB_FULL_SCAN_ITERATOR_HPP
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "page/page_ref.hpp"
#include "page/row_position.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Table;
class Transaction;

// Pre-decode reject for INT64/DATE `column <cmp> constant` (full schema index).
struct IntegerPeekCompare {
  slot_t column{0};
  BinaryOperation op{BinaryOperation::kEquals};
  int64_t constant{0};
};

[[nodiscard]] inline bool MatchIntegerCompare(int64_t left, BinaryOperation op,
                                              int64_t right) {
  switch (op) {
    case BinaryOperation::kEquals:
      return left == right;
    case BinaryOperation::kNotEquals:
      return left != right;
    case BinaryOperation::kLessThan:
      return left < right;
    case BinaryOperation::kLessThanEquals:
      return left <= right;
    case BinaryOperation::kGreaterThan:
      return left > right;
    case BinaryOperation::kGreaterThanEquals:
      return left >= right;
    default:
      return false;
  }
}

// Full scan over a table's row-page chain (or an explicit page list).
//
// Visibility contract: rows are served through Transaction::ReadVersion so
// every snapshot sees its own MVCC version.  Read-only transactions may skip
// that path per page when PageLSN() <= SnapshotTimestamp() (see
// PhysicalReadEligible in full_scan_iterator.cpp): the writer-side contract
// stamps PageLSN on every completed mutation, so such pages carry no
// modification newer than the snapshot and their physical rows are served
// directly.  Pages stamped above the threshold fall back to ReadVersion.
//
// Lifetime contract: table_ and txn_ must outlive the iterator.  If
// key_filter_ / peek_compares_ are supplied, those objects must also outlive
// the iterator -- they are held as raw pointers, so passing the address of a
// temporary is a dangling-pointer bug.  Rows handed out via operator*
// own their data and stay valid after the iterator's page latch is dropped.
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
  [[nodiscard]] RowPosition Position() const override { return pos_; }
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  void DropPageLatch() override;
  const Row& operator*() const override;
  Row& operator*() override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Table;
  friend class FullScan;
  FullScanIterator(
      const Table* table, Transaction* txn,
      std::optional<std::vector<slot_t>> projection = std::nullopt,
      const std::unordered_set<int64_t>* key_filter = nullptr,
      std::optional<slot_t> key_column = std::nullopt,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr);
  FullScanIterator(
      const Table* table, Transaction* txn, std::vector<page_id_t> pages,
      std::optional<std::vector<slot_t>> projection,
      const std::unordered_set<int64_t>* key_filter = nullptr,
      std::optional<slot_t> key_column = std::nullopt,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr);

  void DeserializeCurrent(std::string_view row);
  [[nodiscard]] bool PassesPeekFilters(std::string_view row) const;
  void SeekVisibleRow();
  bool AdvancePage();

  const Table* table_;
  Transaction* txn_;
  RowPosition pos_;
  Row current_row_;
  std::unique_ptr<PageRef> page_;
  std::optional<std::vector<slot_t>> projection_;
  std::optional<std::vector<page_id_t>> pages_;
  size_t page_index_{0};
  const std::unordered_set<int64_t>* key_filter_{nullptr};
  std::optional<slot_t> key_column_;
  const std::vector<IntegerPeekCompare>* peek_compares_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_ITERATOR_HPP
