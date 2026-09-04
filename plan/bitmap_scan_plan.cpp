/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/bitmap_scan_plan.hpp"

#include <algorithm>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

#include "index/index.hpp"
#include "table/table.hpp"

namespace tinylamb {

BitmapScanPlan::BitmapScanPlan(const Table& table,
                               const TableStatistics& statistics,
                               std::vector<BitmapIndexRange> ranges,
                               BitmapCombine combine, Expression where,
                               size_t estimated_rows, size_t access_rows)
    : table_(table),
      statistics_(statistics),
      ranges_(std::move(ranges)),
      combine_(combine),
      where_(std::move(where)),
      estimated_rows_(std::max<size_t>(1, estimated_rows)),
      access_rows_(std::max<size_t>(1, access_rows)) {}

const Schema& BitmapScanPlan::GetSchema() const { return table_.GetSchema(); }

void BitmapScanPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent)
         << (combine_ == BitmapCombine::kAnd ? "BitmapAnd" : "BitmapOr")
         << " (estimated rows: " << estimated_rows_ << ")";
  for (const BitmapIndexRange& range : ranges_) {
    output << "\n"
           << Indent(indent + 2)
           << "BitmapIndexScan: " << range.index->sc_.name_ << " on "
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
    output << "\n" << Indent(indent + 2) << "Recheck: " << *where_;
  }
}

std::string BitmapScanPlan::ToString() const {
  std::ostringstream output;
  Dump(output, 0);
  return output.str();
}

}  // namespace tinylamb
