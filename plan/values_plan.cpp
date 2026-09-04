/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "values_plan.hpp"

#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/constants.hpp"

namespace tinylamb {
namespace {

TableStatistics MakeStats(const Schema& schema, size_t rows) {
  std::vector<ColumnStats> columns;
  columns.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    columns.emplace_back(schema.GetColumn(i).Type());
  }
  TableStatistics stats(schema);
  stats.Assign(rows, std::move(columns));
  return stats;
}

}  // namespace

ValuesPlan::ValuesPlan(Schema schema, std::vector<Row> rows)
    : schema_(std::move(schema)),
      rows_(std::move(rows)),
      stats_(MakeStats(schema_, rows_.size())) {
  for (const Row& row : rows_) {
    if (row.values_.size() != schema_.ColumnCount()) {
      throw std::invalid_argument(
          "ValuesPlan row/schema width mismatch: row=" +
          std::to_string(row.values_.size()) +
          " schema=" + std::to_string(schema_.ColumnCount()));
    }
  }
}

bool ValuesPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                             const std::vector<bool>& ascending) const {
  return expressions.empty() && ascending.empty();
}

void ValuesPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "Values (rows=" << rows_.size() << ")";
}

std::string ValuesPlan::ToString() const {
  return "Values (rows=" + std::to_string(rows_.size()) + ")";
}

DummyScanPlan::DummyScanPlan()
    : schema_("", {}), stats_(MakeStats(schema_, 1)) {}

void DummyScanPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "DummyScan (one row)";
}

std::string DummyScanPlan::ToString() const { return "DummyScan (one row)"; }

}  // namespace tinylamb
