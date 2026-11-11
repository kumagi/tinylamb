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
#include "common/incarnation.hpp"
#include "common/runtime_filter.hpp"
#include "page/page_ref.hpp"
#include "page/row_position.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Table;
class Transaction;
struct LSTablePageBlock;
struct LSTableStore;

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

// Schema-typed row batch decoded straight from page images: fixed-width
// payloads land in `cells` (DOUBLE bit-cast through int64), VARCHAR bytes in
// per-slot byte arenas with per-row end offsets, one null byte per cell.
// Consumed by the executor's cache-fill fold so the hot scan path never
// constructs a Value per cell; `slot_types`/`slot_unsigned` mirror the
// projected schema so raw payloads can be turned into canonical Values only
// where the fold needs them (new group keys, materialized rows).
struct TypedCellBatch {
  size_t stride{0};
  std::vector<ValueType> slot_types;
  std::vector<bool> slot_unsigned;
  std::vector<int64_t> cells;                       // rows * stride payloads
  std::vector<uint8_t> nulls;                       // rows * stride bytes
  std::vector<std::vector<uint8_t>> varchar_bytes;  // per VARCHAR slot
  std::vector<std::vector<uint32_t>> varchar_ends;  // per VARCHAR slot, rows+1
  size_t rows{0};
  // Opt-in tuple identity for consumers such as TopN; plain folds pay no
  // position-buffer allocation or writes.
  bool capture_positions{false};
  std::vector<RowPosition> positions;

  void Configure(size_t stride_in, std::vector<ValueType> types,
                 std::vector<bool> unsigned_flags) {
    stride = stride_in;
    slot_types = std::move(types);
    slot_unsigned = std::move(unsigned_flags);
    varchar_bytes.clear();
    varchar_ends.clear();
    for (const ValueType type : slot_types) {
      if (type == ValueType::kVarChar) {
        varchar_bytes.emplace_back();
        varchar_ends.emplace_back();
      }
    }
  }
  // Drops decoded rows, keeps capacity and the schema description.  cells/
  // nulls keep their sticky full-batch size (see DecodeNextTypedBatch): the
  // decode writes every cell a consumer can reach for rows < `rows`, and
  // readers null-check (or use the varchar arenas) before touching a cell,
  // so stale payloads beyond `rows` are never observable.
  void Reset() {
    rows = 0;
    positions.clear();
    for (auto& ends : varchar_ends) {
      ends.clear();
    }
    for (auto& bytes : varchar_bytes) {
      bytes.clear();
    }
  }
  // Varchar cell (row, varchar ordinal among VARCHAR slots). Callers hold the
  // batch alive while consuming views.
  [[nodiscard]] std::string_view Varchar(size_t row,
                                         size_t varchar_slot) const {
    const uint32_t begin = row == 0 ? 0U : varchar_ends[varchar_slot][row - 1];
    return {reinterpret_cast<const char*>(varchar_bytes[varchar_slot].data()) +
                begin,
            varchar_ends[varchar_slot][row] - begin};
  }
};

// Full scan over a table's row-page chain (or an explicit page list).
//
// Visibility contract: rows are served through Transaction::ReadVersion so
// every snapshot sees its own MVCC version.  Read-only transactions may skip
// that path per page when the page carries no version chain (see
// PhysicalReadEligible in full_scan_iterator.cpp): a mutator registers its
// write set -- creating the chain -- before it can touch page bytes, so a
// chain-free page's image is a committed state, and GC erases a chain only
// once no active snapshot needs anything older than the physical image.
// Pages with chains fall back to ReadVersion as before.
//
// Lifetime contract: table_ and txn_ must outlive the iterator.  If
// key_filter_ / runtime_filter_ / peek_compares_ are supplied, those objects
// must also outlive the iterator -- they are held as raw pointers, so passing
// the address of a temporary is a dangling-pointer bug.  Rows handed out via
// operator* own their data and stay valid after the iterator's page latch is
// dropped.
class FullScanIterator : public IteratorBase {
 public:
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  FullScanIterator(const FullScanIterator&) = delete;
  FullScanIterator& operator=(const FullScanIterator&) = delete;
  FullScanIterator(FullScanIterator&&) = delete;
  FullScanIterator& operator=(FullScanIterator&&) = delete;
  ~FullScanIterator() override = default;
  bool operator==(const FullScanIterator& rhs) const {
    EnsureLSTableRowMaterialized();
    rhs.EnsureLSTableRowMaterialized();
    return table_ == rhs.table_ && txn_ == rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }

  bool operator!=(const FullScanIterator& rhs) const {
    return !(operator==(rhs));
  }

  [[nodiscard]] bool IsValid() const override;
  [[nodiscard]] Status GetStatus() const override { return status_; }
  [[nodiscard]] RowPosition Position() const override { return pos_; }
  // Dynamic join filter, installed after construction and before the scan has
  // produced its first row (IteratorBase::SetRuntimeFilter).
  void SetRuntimeFilter(const RuntimeFilterGroup* group, slot_t column,
                        ValueType type) override;
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  void DropPageLatch() override;
  const Row& operator*() const override;
  Row& operator*() override;
  size_t DecodeNextTypedBatch(TypedCellBatch* batch, size_t max_rows) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Table;
  friend class FullScan;
  FullScanIterator(
      const Table* table, Transaction* txn,
      std::optional<std::vector<slot_t>> projection = std::nullopt,
      const std::unordered_set<int64_t>* key_filter = nullptr,
      std::optional<slot_t> key_column = std::nullopt,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr,
      bool lock_rows = false, bool wait_for_write_intent = true,
      const RuntimeFilterGroup* runtime_filter = nullptr,
      ValueType runtime_filter_type = ValueType::kNull);
  FullScanIterator(
      const Table* table, Transaction* txn, std::vector<page_id_t> pages,
      std::optional<std::vector<slot_t>> projection,
      const std::unordered_set<int64_t>* key_filter = nullptr,
      std::optional<slot_t> key_column = std::nullopt,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr,
      const RuntimeFilterGroup* runtime_filter = nullptr,
      ValueType runtime_filter_type = ValueType::kNull);

  // Soundness gate for the pre-decode peek: a group is only applied when the
  // scanned column is INT64/DATE and shares the filter's comparison domain.
  [[nodiscard]] const RuntimeFilterGroup* AcceptsColumn(
      const RuntimeFilterGroup* group, std::optional<slot_t> column,
      ValueType type) const;
  void DeserializeCurrent(std::string_view row);
  // Lazily builds (and caches) the row-path decode plan for this scan's
  // projection; nullptr when the projection is malformed.
  [[nodiscard]] const RowDecodePlan* RowPlan();
  // Pre-decode integer peeks: combine fixed-width spans once per schema,
  // walking only variable-width cells for rows without a null bitmap.
  // Bitmap rows keep the canonical column walk.
  struct PlannedPeek {
    struct VariableSkip {
      ValueType type{ValueType::kNull};
      uint32_t fixed_after{0};
    };
    bool valid{false};
    slot_t column{0};
    slot_t stored_count{0};
    uint32_t offset{0};  // payload-relative
    std::vector<VariableSkip> variable_skips;
  };
  [[nodiscard]] PlannedPeek BuildPeek(slot_t column) const;
  // Direct planned peek; nullopt = NULL or corrupt count (same contract as
  // Row::TryPeekInteger). kFallback signals a bitmap row: caller retries via
  // Row::TryPeekInteger.
  struct PeekOutcome {
    enum class Kind : uint8_t { kValue, kNull, kFallback } kind;
    int64_t value;
  };
  [[nodiscard]] static PeekOutcome PeekIntegerPlanned(const PlannedPeek& peek,
                                                      const char* row);
  [[nodiscard]] int64_t PeekCompareValue(const PlannedPeek& peek,
                                         const char* row) const;
  [[nodiscard]] StatusOr<bool> PassesPeekFilters(std::string_view row) const;
  // Resolves the next visible row image (the SeekVisibleRow ladder) and
  // advances pos_ past it; nullopt at exhaustion or on a latched failure.
  [[nodiscard]] std::optional<std::string_view> NextVisibleImage();
  void SeekVisibleRow();
  [[nodiscard]] bool TryInitLSTableScan();
  [[nodiscard]] const LSTablePageBlock* CurrentLSTableBlock() const noexcept;
  [[nodiscard]] bool LSTableRowMatches(const LSTablePageBlock& blk,
                                       uint32_t row) const noexcept;
  [[nodiscard]] bool LSTableBlockAllRowsMatch(
      const LSTablePageBlock& blk) const noexcept;
  void SeekLSTableRow();
  void EnsureLSTableRowMaterialized() const;
  size_t DecodeNextTypedBatchFromLSTable(TypedCellBatch* batch,
                                         size_t max_rows);
  // Builds the projected-schema decode plan used by DecodeNextTypedBatch;
  // empty (typed decoding refused) for schemas carrying unsupported types.
  void BuildTypedDecodePlan();
  bool AdvancePage();
  // Pins `page_id` with a shared latch. On failure records the status, leaves
  // page_ empty, and returns false.
  [[nodiscard]] bool PinPage(page_id_t page_id);

  const Table* table_;
  Transaction* txn_;
  RowPosition pos_;
  mutable Row current_row_;
  TableLease table_lease_;
  std::unique_ptr<PageRef> page_;
  std::shared_ptr<const LSTableStore> lstable_store_;
  size_t lstable_page_cursor_{0};
  uint32_t lstable_row_cursor_{0};
  mutable bool lstable_row_materialized_{true};
  // Valid only while this shared page latch remains held.
  std::optional<bool> physical_read_eligible_;
  std::optional<std::vector<slot_t>> projection_;
  std::optional<std::vector<page_id_t>> pages_;
  size_t page_index_{0};
  const std::unordered_set<int64_t>* key_filter_{nullptr};
  std::optional<slot_t> key_column_;
  const std::vector<IntegerPeekCompare>* peek_compares_{nullptr};
  // Dynamic join filter (common/runtime_filter.hpp). Takes precedence over
  // key_filter_ when set: a group covers the exact-set, min/max and Bloom
  // shapes uniformly, so the pre-decode peek below is one call either way.
  const RuntimeFilterGroup* runtime_filter_{nullptr};
  std::optional<slot_t> runtime_filter_column_;
  ValueType runtime_filter_type_{ValueType::kNull};
  bool lock_rows_{false};
  bool wait_for_write_intent_{true};
  // Positions whose write intent this scan acquired and still holds; used to
  // undo scan-side locking when a later intent acquisition fails mid-scan.
  std::vector<RowPosition> scan_acquired_;
  // Projected-schema typed decode plan (empty = typed decoding refused).
  std::vector<ValueType> typed_slot_types_;
  std::vector<bool> typed_slot_unsigned_;
  std::vector<slot_t> typed_keep_slots_;
  // Row-path projected decode plan for DeserializeCurrent (built lazily;
  // row_plan_failed_ = malformed projection, keep the legacy per-row path).
  std::optional<RowDecodePlan> row_plan_;
  bool row_plan_failed_{false};
  // Planned peek column info, parallel to peek_compares_ (lazily built).
  mutable std::vector<PlannedPeek> peek_plan_;
  mutable bool peek_plan_built_{false};
  mutable PlannedPeek key_peek_plan_{};
  mutable bool key_peek_built_{false};
  // Per kept slot: VARCHAR ordinal among kept slots, or SIZE_MAX for
  // fixed-width slots. Precomputed so the decode loop never rescans the
  // plan per row to map a kept slot to its varchar arena.
  std::vector<size_t> typed_varchar_slot_;
  // Leading fixed-width (8-byte) stored columns decode at payload-relative
  // offsets that are constants of the schema; the prefix ends before the
  // first VARCHAR column (stored column 0..typed_const_prefix_-1).  The
  // payload base itself moves by the null-bitmap width, which the row's
  // null FLAG decides, so the decode derives it per row.
  slot_t typed_const_prefix_{0};
  uint32_t typed_const_prefix_end_{0};  // payload + this = cursor after prefix
  // Prefix decode fast path (no-null-bitmap rows only): typed_prefix_kept_ =
  // how many kept slots sit inside the constant prefix; typed_prefix_all_kept_
  // says the prefix covers every kept slot of this projection, and
  // typed_prefix_identity_ that the kept slots are exactly 0..n-1 (so the
  // decode is a contiguous byteswap loop vectorizable 4 cells at a time).
  size_t typed_prefix_kept_{0};
  bool typed_prefix_all_kept_{false};
  bool typed_prefix_identity_{false};
  Status status_{Status::kSuccess};
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_ITERATOR_HPP
