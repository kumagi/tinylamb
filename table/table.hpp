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

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/incarnation.hpp"
#include "common/status_or.hpp"
#include "full_scan_iterator.hpp"
#include "index/index.hpp"
#include "iterator.hpp"
#include "page/row_position.hpp"
#include "table/scan_options.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class Transaction;
class PageRef;
class Decoder;
class Encoder;
struct Row;
struct RowPosition;
struct LSTableStore;

class Table {
 public:
  using ScanMorsel = std::vector<page_id_t>;
  // IndexValueType moved to index/index_schema.hpp (improvement3 S5).  The
  // using-declaration is a compatibility bridge for TUs that still spell
  // Table::IndexValueType (table.cpp, index_scan_iterator.cpp); it adds no
  // second definition and the disk encoding is unchanged.
  using IndexValueType = tinylamb::IndexValueType;

  Table() : incarnation_state_(std::make_shared<TableIncarnation>()) {}
  Table(Schema sc, page_id_t pid, uint64_t incarnation = 0)
      : schema_(std::move(sc)),
        first_pid_(pid),
        last_pid_(pid),
        incarnation_(incarnation),
        incarnation_state_(std::make_shared<TableIncarnation>()) {}
  Table(const Table&) = default;
  Table(Table&&) = default;
  Table& operator=(const Table&) = default;
  Table& operator=(Table&&) = default;
  ~Table() = default;

  Status CreateIndex(Transaction& txn, const IndexSchema& idx);

  StatusOr<RowPosition> Insert(Transaction& txn, const Row& row);

  StatusOr<RowPosition> Update(Transaction& txn, const RowPosition& pos,
                               const Row& row);

  Status Delete(Transaction& txn, RowPosition pos);

  StatusOr<Row> Read(Transaction& txn, RowPosition pos) const;

  [[nodiscard]] StatusOr<TableLease> AcquireLease(txn_id_t owner) const {
    if (incarnation_state_ == nullptr || !incarnation_state_->Acquire(owner)) {
      return Status::kConflicts;
    }
    return TableLease(incarnation_state_, owner);
  }

  Iterator BeginFullScan(Transaction& txn,
                         const TableScanOptions& options = {}) const;
  // Backward-compatible overloads forwarding to TableScanOptions.
  Iterator BeginFullScan(
      Transaction& txn,
      const std::vector<IntegerPeekCompare>* peek_compares) const {
    TableScanOptions options;
    options.peek_compares = peek_compares;
    return BeginFullScan(txn, options);
  }
  Iterator BeginFullScan(
      Transaction& txn, const std::vector<slot_t>& projection,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr) const {
    TableScanOptions options;
    options.projection = projection;
    options.peek_compares = peek_compares;
    return BeginFullScan(txn, options);
  }
  Iterator BeginFullScan(
      Transaction& txn, const std::vector<slot_t>& projection,
      const std::unordered_set<int64_t>* key_filter, slot_t key_column,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr) const {
    TableScanOptions options;
    options.projection = projection;
    options.key_filter = key_filter;
    options.key_column = key_column;
    options.peek_compares = peek_compares;
    return BeginFullScan(txn, options);
  }
  Iterator BeginFullScan(
      Transaction& txn, const std::unordered_set<int64_t>* key_filter,
      slot_t key_column,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr) const {
    TableScanOptions options;
    options.key_filter = key_filter;
    options.key_column = key_column;
    options.peek_compares = peek_compares;
    return BeginFullScan(txn, options);
  }
  Iterator BeginMorselScan(
      Transaction& txn, const ScanMorsel& pages,
      std::optional<std::vector<slot_t>> projection = std::nullopt,
      const std::unordered_set<int64_t>* key_filter = nullptr,
      std::optional<slot_t> key_column = std::nullopt,
      const std::vector<IntegerPeekCompare>* peek_compares = nullptr,
      const RuntimeFilterGroup* runtime_filter = nullptr,
      ValueType runtime_filter_type = ValueType::kNull) const;
  [[nodiscard]] std::vector<ScanMorsel> BuildScanMorsels(
      Transaction& txn, size_t pages_per_morsel = 8) const;
  struct IntegerKeyCounts {
    std::vector<uint64_t> counts;
    uint64_t rows{0};
  };
  // Exact bounded integer-domain counts over unfiltered row pages. Returns
  // null when MVCC/page freshness cannot be proved; callers then scan normally.
  [[nodiscard]] std::shared_ptr<const IntegerKeyCounts> TryReadIntegerKeyCounts(
      Transaction& txn, const std::vector<ScanMorsel>& morsels, slot_t column,
      int64_t minimum, size_t span) const;
  // Revalidate an existing histogram using its saved complete page chain;
  // never builds a new one and never skips the page/MVCC freshness checks.
  [[nodiscard]] std::shared_ptr<const IntegerKeyCounts>
  FindCachedIntegerKeyCounts(Transaction& txn, slot_t column, int64_t minimum,
                             size_t span) const;
  Iterator BeginIndexScan(Transaction& txn, const Index& index,
                          const Value& begin = Value(),
                          const Value& end = Value(),
                          bool ascending = true) const;
  Iterator BeginIndexScan(Transaction& txn, const Index& index,
                          const std::vector<Value>& begin_key,
                          const std::vector<Value>& end_key,
                          bool ascending = true) const;

  [[nodiscard]] std::unordered_map<slot_t, size_t> AvailableKeyIndex() const;

  [[nodiscard]] const Schema& GetSchema() const { return schema_; }
  [[nodiscard]] std::shared_ptr<const LSTableStore> GetLSTableStore() const;
  void SetLSTableStore(std::shared_ptr<const LSTableStore> store) const;
  std::shared_ptr<const LSTableStore> RebuildLSTableStore(
      Transaction& txn) const;
  [[nodiscard]] size_t IndexCount() const { return indexes_.size(); }
  // Diagnostics: the row-page chain head/tail (for corruption reports).
  [[nodiscard]] page_id_t FirstPageId() const { return first_pid_; }
  [[nodiscard]] page_id_t LastPageId() const { return last_pid_; }
  [[nodiscard]] uint64_t Incarnation() const { return incarnation_; }

  // Physical row-layout bookkeeping behind the clustered-order proof in
  // table/table_statistics.hpp. Both read the shared TableIncarnation, so every
  // copy of this Table observes the same values. A table without an
  // incarnation state reports "unknown" (0 / false), which no proof matches.
  [[nodiscard]] uint64_t LayoutGeneration() const {
    return incarnation_state_ == nullptr
               ? 0
               : incarnation_state_->LayoutGeneration();
  }
  [[nodiscard]] bool RowsRewrittenInPlace() const {
    return incarnation_state_ != nullptr &&
           incarnation_state_->RowsRewrittenInPlace();
  }

  friend Encoder& operator<<(Encoder& e, const Table& t);
  friend Decoder& operator>>(Decoder& d, Table& t);
  friend std::ostream& operator<<(std::ostream& o, const Table& t);
  bool operator==(const Table& rhs) const;
  [[nodiscard]] const Index& GetIndex(size_t offset) const {
    return indexes_[offset];
  }

 private:
  [[nodiscard]] std::shared_ptr<const IntegerKeyCounts> ReadIntegerKeyCounts(
      Transaction& txn, const std::vector<ScanMorsel>& morsels, slot_t column,
      int64_t minimum, size_t span, bool cached_pages_only) const;
  // |hint_leaf| threads a BPlusTree landing-leaf cursor so consecutive index
  // operations on the same tree (e.g. the delete+insert pair of one UPDATE)
  // skip the root-to-leaf walk when the remembered leaf still covers the
  // key. nullptr keeps the plain always-descend behavior.
  [[nodiscard]] Status CheckRowConstraints(const Row& row) const;
  Status LinkReservedTableTail(Transaction& txn, PageRef& predecessor,
                               PageRef& successor) const;
  [[nodiscard]] StatusOr<Row> ReadLatest(Transaction& txn,
                                         RowPosition pos) const;
  Status IndexInsert(Transaction& txn, const Index& idx, const Row& new_row,
                     const RowPosition& pos,
                     page_id_t* hint_leaf = nullptr) const;
  // Gate-held RMW bodies; see the escalation loops in IndexInsert/IndexDelete
  // (table.cpp). Callers must hold the index write gate for idx.Root().
  Status IndexInsertGuarded(Transaction& txn, const Index& idx,
                            const Row& new_row, const RowPosition& pos,
                            page_id_t* hint_leaf,
                            RowPosition* wait_pos = nullptr) const;
  Status IndexDelete(Transaction& txn, const Index& idx,
                     const RowPosition& pos) const;
  static Status IndexDelete(Transaction& txn, const Index& idx,
                            const RowPosition& pos, const Row& original_row,
                            page_id_t* hint_leaf = nullptr);
  static Status IndexDeleteGuarded(Transaction& txn, const Index& idx,
                                   const RowPosition& pos,
                                   const Row& original_row,
                                   page_id_t* hint_leaf);

  friend class Database;
  friend class FullScanIterator;
  friend class IndexScanIterator;
  friend class TableInterface;

  void SetIncarnationState(std::shared_ptr<TableIncarnation> state) {
    incarnation_state_ = std::move(state);
  }

  // Every row mutation brackets its work with these two calls, unconditionally
  // (a failed or aborted mutation may still have moved rows, and a writer that
  // is in flight while ANALYZE scans must not leave a usable proof behind).
  // `rewrites_in_place` additionally latches the UPDATE/DELETE marker.
  void NoteRowMutationStart(bool rewrites_in_place) const {
    if (incarnation_state_ == nullptr) {
      return;
    }
    incarnation_state_->BumpLayoutGeneration();
    if (rewrites_in_place) {
      incarnation_state_->LatchRowRewrite();
    }
  }
  void NoteRowMutationEnd() const {
    if (incarnation_state_ != nullptr) {
      incarnation_state_->BumpLayoutGeneration();
    }
  }

  // RAII bracket for one row mutation, so every early return (including the
  // failure paths) still closes the generation window.
  class RowMutationScope {
   public:
    RowMutationScope(const Table& table, bool rewrites_in_place)
        : table_(&table) {
      table_->NoteRowMutationStart(rewrites_in_place);
    }
    RowMutationScope(const RowMutationScope&) = delete;
    RowMutationScope& operator=(const RowMutationScope&) = delete;
    RowMutationScope(RowMutationScope&&) = delete;
    RowMutationScope& operator=(RowMutationScope&&) = delete;
    ~RowMutationScope() { table_->NoteRowMutationEnd(); }

   private:
    const Table* table_;
  };

  Schema schema_;
  page_id_t first_pid_{};
  page_id_t last_pid_{};
  uint64_t incarnation_{0};
  std::vector<Index> indexes_{};
  std::shared_ptr<TableIncarnation> incarnation_state_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_HPP
