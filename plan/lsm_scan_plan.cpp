/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "lsm_scan_plan.hpp"

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "index/lsm_tree.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr std::string_view kKeyColumnName = "key";

Schema MakeSchema() {
  return Schema("", {Column("key", ValueType::kVarChar),
                     Column("value", ValueType::kVarChar)});
}

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

LsmScanPlan::LsmScanPlan(const LSMTree& tree, size_t estimated_rows)
    : tree_(tree),
      schema_(MakeSchema()),
      stats_(MakeStats(schema_, estimated_rows)),
      estimated_rows_(estimated_rows) {}

bool LsmScanPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                              const std::vector<bool>& ascending) const {
  // The view merges runs by key: one ascending key column is the only order
  // the scan provides.  Byte order for VARCHAR is the engine's binary order,
  // so the claim needs no collation gate.
  if (expressions.size() != 1 || ascending.size() != 1 || !ascending.front() ||
      !expressions.front() ||
      expressions.front()->Type() != TypeTag::kColumnValue) {
    return false;
  }
  return expressions.front()->AsColumnValue().GetColumnName().name ==
         kKeyColumnName;
}

bool LsmScanPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  static_cast<void>(nulls_first);
  return IsOrderedBy(expressions, ascending);
}

void LsmScanPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "LsmScan: " << tree_
         << " (estimated rows: " << estimated_rows_ << ")";
}

std::string LsmScanPlan::ToString() const {
  return "LsmScan (estimated rows: " + std::to_string(estimated_rows_) + ")";
}

}  // namespace tinylamb
