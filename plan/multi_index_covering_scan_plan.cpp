/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/multi_index_covering_scan_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "index/index.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"

namespace tinylamb {

MultiIndexCoveringScanPlan::MultiIndexCoveringScanPlan(
    const Table& table, const TableStatistics& statistics,
    std::vector<BitmapIndexRange> ranges, Expression where,
    size_t estimated_rows, size_t access_rows)
    : table_(table),
      ranges_(std::move(ranges)),
      where_(std::move(where)),
      estimated_rows_(estimated_rows),
      access_rows_(std::max<size_t>(1, access_rows)),
      statistics_(Schema("", {})) {
  // The output is the union of the participants' stored columns in table slot
  // order: a deterministic layout every participant can fill positionally.
  const Schema& table_schema = table.GetSchema();
  std::vector<bool> covered(table_schema.ColumnCount(), false);
  for (const BitmapIndexRange& range : ranges_) {
    for (const slot_t slot : range.index->CoveredColumns()) {
      if (slot < covered.size()) {
        covered[slot] = true;
      }
    }
  }
  std::vector<Column> columns;
  std::vector<ColumnStats> stats_columns;
  for (size_t slot = 0; slot < covered.size(); ++slot) {
    if (!covered[slot]) {
      continue;
    }
    const Column& column = table_schema.GetColumn(slot);
    covered_slots_.push_back(static_cast<slot_t>(slot));
    columns.push_back(column);
    stats_columns.push_back(slot < statistics.Columns()
                                ? statistics.Column(slot)
                                : ColumnStats(column.Type()));
  }
  output_schema_ = Schema("", std::move(columns));
  // The statistics carry exactly the output columns, so the operators above
  // index them by this node's schema offsets rather than the table's.
  statistics_.Assign(estimated_rows_, std::move(stats_columns));
}

void MultiIndexCoveringScanPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent))
         << "MultiIndexCoveringScan (estimated rows: " << estimated_rows_
         << ")";
  for (const BitmapIndexRange& range : ranges_) {
    output << "\n"
           << Indent(static_cast<size_t>(indent) + 2)
           << "Cover: " << range.index->sc_.name_ << " on "
           << table_.GetSchema().Name();
    if (!range.begin_key.empty() || !range.end_key.empty()) {
      output << " [";
      if (!range.begin_key.empty()) {
        output << range.begin_key.front();
      }
      output << " -> ";
      if (!range.end_key.empty()) {
        output << range.end_key.front();
      }
      output << "]";
    }
  }
  if (where_) {
    output << "\n"
           << Indent(static_cast<size_t>(indent) + 2) << "Recheck: " << *where_;
  }
}

std::string MultiIndexCoveringScanPlan::ToString() const {
  std::ostringstream output;
  Dump(output, 0);
  return output.str();
}

bool MultiIndexCoveringScanPlan::ContainsParameterSlots() const {
  return ContainsParameterSlot(where_);
}

bool MultiIndexCoveringScanPlan::InstallParameterSlots(
    const std::vector<Value>& parameters, size_t* cursor,
    std::vector<Expression>* slots) {
  return SlotizeInPlace(&where_, parameters, cursor, slots);
}

}  // namespace tinylamb
