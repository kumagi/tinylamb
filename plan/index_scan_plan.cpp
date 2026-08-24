/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "index_scan_plan.hpp"

#include <cstddef>
#include <memory>
#include <vector>
#include <utility>
#include <ostream>
#include <string>

#include "database/transaction_context.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

#include <algorithm>
#include <ranges>

namespace tinylamb {
namespace {

bool OrderMatches(const std::vector<ColumnName>& provided, bool scan_ascending,
                  const std::vector<Expression>& expressions,
                  const std::vector<bool>& ascending) {
  if (provided.empty() || expressions.empty() ||
      expressions.size() != ascending.size() ||
      expressions.size() > provided.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (expressions[i]->Type() != TypeTag::kColumnValue) { return false;
}
    const ColumnName& column =
        expressions[i]->AsColumnValue().GetColumnName();
    if (column.name != provided[i].name) { return false;
}
    if (ascending[i] != scan_ascending) { return false;
}
  }
  return true;
}

Value FirstOrNull(const std::vector<Value>& keys) {
  return keys.empty() ? Value() : keys.front();
}

// Bounds-based estimate for point-union scans: spans [min,max] across the
// points. The residual predicate keeps results correct; the estimate stays
// conservative.
TableStatistics BoundsStats(const TableStatistics& ts, const Index& index,
                            const std::vector<
                                std::pair<std::vector<Value>,
                                          std::vector<Value>>>& ranges) {
  Value min;
  Value max;
  for (const auto& range : ranges) {
    if (range.first.empty()) { continue;
}
    const Value& key = range.first.front();
    if (min.IsNull() || key < min) { min = key;
}
    if (max.IsNull() || max < key) { max = key;
}
  }
  return ts.TransformBy(index.sc_.key_[0], min, max);
}

}  // namespace

IndexScanPlan::IndexScanPlan(const Table& table, const Index& index,
                             const TableStatistics& ts,
                             std::vector<Value> begin_key,
                             std::vector<Value> end_key, bool ascending,
                             Expression where,
                             std::vector<ColumnName> provided_order,
                             bool lock_rows, bool wait_for_write_intent)
    : table_(table),
      index_(index),
      stats_(ts.TransformBy(index.sc_.key_[0], FirstOrNull(begin_key),
                            FirstOrNull(end_key))),
      begin_key_(std::move(begin_key)),
      end_key_(std::move(end_key)),
      ascending_(ascending),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      where_(std::move(where)),
      provided_order_(std::move(provided_order)) {}

IndexScanPlan::IndexScanPlan(
    const Table& table, const Index& index, const TableStatistics& ts,
    std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges,
    bool ascending, Expression where,
    std::vector<ColumnName> provided_order, bool lock_rows,
    bool wait_for_write_intent)
    : table_(table),
      index_(index),
      stats_(BoundsStats(ts, index, ranges)),
      begin_key_(),
      end_key_(),
      point_ranges_(std::move(ranges)),
      ascending_(ascending),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      where_(std::move(where)),
      provided_order_(std::move(provided_order)) {}

// EmitExecutor lives in the relational factory (executor/relational_factory.cpp).

const Schema& IndexScanPlan::GetSchema() const { return table_.GetSchema(); }

size_t IndexScanPlan::AccessRowCount() const { return EmitRowCount(); }

size_t IndexScanPlan::EmitRowCount() const {
  if (!point_ranges_.empty()) {
    if (index_.IsUnique() && begin_key_.empty() &&
        std::ranges::all_of(point_ranges_, [this](const auto& range) {
          return range.first.size() == index_.sc_.key_.size();
        })) {
      return point_ranges_.size();
    }
    return stats_.Rows();
  }
  if (index_.IsUnique() && begin_key_ == end_key_ &&
      begin_key_.size() == index_.sc_.key_.size()) {
    return 1;
  }
  return stats_.Rows();
}

bool IndexScanPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                                const std::vector<bool>& ascending) const {
  // Concatenated point ranges only deliver a global order when a single
  // range remains (ranges are sorted and disjoint).
  if (point_ranges_.size() > 1) { return false;
}
  return OrderMatches(provided_order_, ascending_, expressions, ascending);
}

void IndexScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << table_.GetSchema().Name();
  if (!point_ranges_.empty()) {
    o << " x" << point_ranges_.size() << " points";
  }
  o << " (estimated cost: " << AccessRowCount() << ")";
}

std::string IndexScanPlan::ToString() const {
  std::string s = "IndexScan: " + std::string(table_.GetSchema().Name());
  if (!point_ranges_.empty()) {
    s += " x" + std::to_string(point_ranges_.size()) + " points";
  }
  s += " (estimated cost: " + std::to_string(AccessRowCount()) + ")";
  return s;
}

}  // namespace tinylamb
