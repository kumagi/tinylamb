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

#ifndef TINYLAMB_FULL_SCAN_PLAN_HPP
#define TINYLAMB_FULL_SCAN_PLAN_HPP

#include <limits>
#include <vector>

#include "plan/parallel_thresholds.hpp"
#include "plan/plan.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// Sentinel for "this table has no provable physical order".
inline constexpr slot_t kNoClusteredColumn = std::numeric_limits<slot_t>::max();

// The single column a plain full scan of `table` can *prove* it emits in
// ascending physical order for, or kNoClusteredColumn.
//
// The heap is append-only at the row-page chain tail and FullScanIterator
// walks it head-to-tail, slot by slot, so the scan order is exactly the insert
// order. That is a SQL order only when the catalog can name a key and the
// stored values really do run non-decreasing along it:
//
//   * catalog: the column is the whole key of a UNIQUE index (CREATE TABLE
//     builds one per PRIMARY KEY / UNIQUE column) and is declared NOT NULL, so
//     no NULL can sit between two keys and the reported order is unambiguous
//     under either NULLS FIRST or NULLS LAST;
//   * measurement: the column's Correlation() is 1.0, i.e. ANALYZE walked the
//     whole chain and found every adjacent non-NULL pair non-decreasing;
//   * freshness: the statistics still carry the table's current row-layout
//     generation, so no INSERT/UPDATE/DELETE has moved the chain since that
//     walk, and no slot was ever rewritten in place in this process (an
//     in-place rewrite leaves a pre-image an older snapshot can resolve, and
//     no measurement of the current images can speak for it).
//
// Anything else -- a non-unique or composite key, a nullable column, an
// un-analyzed table, a table touched since ANALYZE, an updated or deleted row,
// spill/parallel scan shapes -- answers kNoClusteredColumn, i.e. "unordered".
[[nodiscard]] slot_t ClusteredOrderColumn(const Table& table,
                                          const TableStatistics& stats);

// True when any table this context has resolved right now carries a provable
// clustered order.
//
// The query engine samples this before and after optimizing a statement and
// keeps the compiled plan out of its plan cache when either sample is true.
// A clustered order is a *revocable* proof: the first row mutation kills it,
// while a plan that elided a sort on it keeps a shape (a folded LIMIT, an
// incremental sort over the clustered prefix) that a later execution of the
// cached plan would take at face value. Statements that never touch such a
// table are cached exactly as before.
[[nodiscard]] bool AnyResolvedTableHasClusteredOrder(
    const TransactionContext& ctx);

class FullScanPlan : public PlanBase {
 public:
  explicit FullScanPlan(const Table& table, TableStatistics ts,
                        size_t max_rows = std::numeric_limits<size_t>::max(),
                        bool lock_rows = false,
                        bool wait_for_write_intent = true);
  FullScanPlan(const Table& table, TableStatistics ts,
               std::vector<IntegerPeekCompare> peek_compares,
               size_t max_rows = std::numeric_limits<size_t>::max(),
               bool lock_rows = false, bool wait_for_write_intent = true);
  FullScanPlan(const FullScanPlan&) = delete;
  FullScanPlan(FullScanPlan&&) = delete;
  FullScanPlan& operator=(const FullScanPlan&) = delete;
  FullScanPlan& operator=(FullScanPlan&&) = delete;
  ~FullScanPlan() override = default;

  // Narrowed copy of `base`: emits only `projection` (table-schema slots,
  // ascending) and presents the matching schema/statistics subset. Built by
  // the projection implementation rule when everything above the scan reads
  // a column subset; every gate that rule checks (no DML source, no peek
  // compares, no max-rows, no clustered-order claim) is its responsibility.
  FullScanPlan(const FullScanPlan& base, std::vector<slot_t> projection);

  Executor EmitExecutor(TransactionContext& txn) const override;

  [[nodiscard]] const Table* ScanSource() const override { return &table_; }
  [[nodiscard]] const Schema& GetSchema() const override {
    return projection_.has_value() ? projected_schema_ : table_.GetSchema();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return projection_.has_value() ? projected_stats_ : stats_;
  }
  // The table-schema slots this narrowed scan emits, or nullopt for a
  // full-width scan.
  [[nodiscard]] const std::optional<std::vector<slot_t>>& Projection() const {
    return projection_;
  }
  // True when the projection implementation rule may build a narrowed copy:
  // a plain read-shape scan whose rows feed name-resolving consumers only.
  // DML sources need every column for the read-modify-write image, peek
  // compares / max rows are other scan shapes, and a clustered-order claim
  // must not survive a projection that may drop the claiming column.
  [[nodiscard]] bool CanNarrow() const {
    return !lock_rows_ && peek_compares_.empty() &&
           max_rows_ == std::numeric_limits<size_t>::max() &&
           !ReportsClusteredOrder() && !projection_.has_value();
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] size_t MaxRows() const { return max_rows_; }
  [[nodiscard]] const std::vector<IntegerPeekCompare>& PeekCompares() const {
    return peek_compares_;
  }
  // True when this scan reports a provable ascending physical order. Since
  // D-9 the morsel-parallel scan also delivers stored-chain order: workers
  // claim chain page groups by static rotation and the consumer drains
  // morsels in chain order (see ParallelScan's morsel-ordered handoff), and
  // an ordered parallel scan refuses row-filter pushdown whose deferred-row
  // parking would reorder. DML source scans (lock_rows_) still resolve head
  // versions under a write intent on the single-threaded FullScan only.
  [[nodiscard]] bool ReportsClusteredOrder() const {
    return ClusteredColumn() != kNoClusteredColumn;
  }
  // A clustered order is measured from the stored chain, not produced by a
  // sorted structure, so costing must not treat it as a free early stop.
  [[nodiscard]] bool OrderIsMeasured() const override {
    return ReportsClusteredOrder();
  }
  [[nodiscard]] slot_t ClusteredColumn() const;
  // True when EmitExecutor delivers rows in stored-chain order. Both the
  // single-threaded FullScan and (D-9) the morsel-parallel scan do; only a
  // DML source scan (lock_rows_) cannot, because it resolves head versions
  // under a write intent through the single-threaded path and the parallel
  // executor has no such hook.
  [[nodiscard]] bool EmitsChainOrderedScan() const { return !lock_rows_; }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  // lock_rows_ marks a DML source scan (UPDATE/DELETE): the executor takes a
  // write intent on every occupied slot before resolving its row, so the
  // yielded image is the head version rather than this transaction's
  // snapshot version -- the full-scan twin of IndexScanPlan::lock_rows_.
  bool lock_rows_{false};
  bool wait_for_write_intent_{true};
  const Table& table_;
  // Owned snapshot: callers hand both context-owned statistics and ad-hoc
  // temporaries (tests, rule synthesis), so a reference here would dangle.
  TableStatistics stats_;
  size_t max_rows_;
  std::vector<IntegerPeekCompare> peek_compares_;
  // Set only on narrowed copies: the table-schema slots this scan emits, the
  // matching output schema, and the matching statistics subset.
  // GetSchema()/GetStats() hand out the projected pair when this is set.
  std::optional<std::vector<slot_t>> projection_;
  Schema projected_schema_{Schema("", {})};
  TableStatistics projected_stats_{Schema("", {})};
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_PLAN_HPP
