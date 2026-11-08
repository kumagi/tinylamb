/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MULTI_INDEX_COVERING_SCAN_PLAN_HPP
#define TINYLAMB_MULTI_INDEX_COVERING_SCAN_PLAN_HPP

#include <cstddef>
#include <ostream>
#include <string>
#include <vector>

#include "plan/bitmap_scan_plan.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Index;
class Table;

// Multi-index covering scan: two or more indexes whose stored columns (key
// columns plus INCLUDE payload) together cover every column the scan touches.
// No single index has to be covering on its own -- that case is served by
// IndexOnlyScanPlan -- and each participant contributes a leading-key range
// derived from the filter, so the intersection of the participants' entries
// is a superset of the matching rows.
//
// The executor intersects the entries by row position and assembles the
// output row from the index images alone when the entry's row page carries no
// version chains (the same index-only visibility argument as
// IndexOnlyScanPlan); a position whose page does carry chains falls back to
// the heap read, which resolves the snapshot version.  The scan predicate is
// evaluated on the covered columns, so the rows the fast path serves need no
// heap fetch at all.
//
// The output schema is the union of the participants' covered columns in
// table slot order; the caller projects it to the required output list.
class MultiIndexCoveringScanPlan final : public PlanBase {
 public:
  MultiIndexCoveringScanPlan(const Table& table,
                             const TableStatistics& statistics,
                             std::vector<BitmapIndexRange> ranges,
                             Expression where, size_t estimated_rows,
                             size_t access_rows);

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override;
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override;
  [[nodiscard]] const Table* ScanSource() const override { return &table_; }
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return statistics_;
  }
  [[nodiscard]] size_t AccessRowCount() const override { return access_rows_; }
  [[nodiscard]] size_t EmitRowCount() const override { return estimated_rows_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

  [[nodiscard]] const Table& GetTable() const { return table_; }
  [[nodiscard]] const std::vector<BitmapIndexRange>& Ranges() const {
    return ranges_;
  }
  [[nodiscard]] const Expression& Where() const { return where_; }
  // The output columns as table slots, ascending: the union of the
  // participants' covered columns.
  [[nodiscard]] const std::vector<slot_t>& CoveredSlots() const {
    return covered_slots_;
  }

 private:
  const Table& table_;
  std::vector<BitmapIndexRange> ranges_;
  Expression where_;
  size_t estimated_rows_;
  size_t access_rows_;
  std::vector<slot_t> covered_slots_;
  Schema output_schema_;
  TableStatistics statistics_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_MULTI_INDEX_COVERING_SCAN_PLAN_HPP
