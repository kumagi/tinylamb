/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "index_only_scan_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "index/index.hpp"
#include "index/index_schema.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool OrderMatches(const std::vector<ColumnName>& provided, bool scan_ascending,
                  const std::vector<Expression>& expressions,
                  const std::vector<bool>& ascending,
                  const std::vector<std::optional<bool>>& nulls_first = {}) {
  if (provided.empty() || expressions.empty() ||
      expressions.size() != ascending.size() ||
      expressions.size() > provided.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    // Index keys use memcomparable byte order with NULL encoded smallest.
    const bool requested = i < nulls_first.size()
                               ? nulls_first[i].value_or(ascending[i])
                               : ascending[i];
    if (requested != scan_ascending) {
      return false;
    }
    if (expressions[i]->Type() != TypeTag::kColumnValue) {
      return false;
    }
    if (expressions[i]->AsColumnValue().GetColumnName().name !=
        provided[i].name) {
      return false;
    }
    if (ascending[i] != scan_ascending) {
      return false;
    }
  }
  return true;
}

TableStatistics IndexRangeStats(const TableStatistics& source,
                                const Index& index,
                                const std::vector<Value>& begin_key,
                                const std::vector<Value>& end_key) {
  TableStatistics result = source;
  size_t equality_rows = result.Rows();
  size_t equality_keys = 0;
  const size_t key_count = std::max(begin_key.size(), end_key.size());
  for (size_t i = 0; i < key_count; ++i) {
    if (i >= index.sc_.key_.size()) {
      break;
    }
    const Value lower = i < begin_key.size() ? begin_key[i] : Value();
    const Value upper = i < end_key.size() ? end_key[i] : Value();
    result =
        result.TransformBy(static_cast<int>(index.sc_.key_[i]), lower, upper);
    if (!lower.IsNull() && !upper.IsNull() && lower == upper) {
      equality_rows = result.Rows();
      ++equality_keys;
    }
  }
  // Correlation damping applies only after an equality prefix.
  if (equality_keys != 0 && result.Rows() < equality_rows) {
    const size_t difference = equality_rows - result.Rows();
    const size_t estimate = equality_rows - (difference / 2) - (difference % 2);
    result = result.ScaleCardinality(estimate);
  }
  return result;
}

TableStatistics IndexOnlyStats(const TableStatistics& source,
                               const Schema& table_schema, const Index& index) {
  std::vector<ColumnStats> columns;
  columns.reserve(index.sc_.key_.size() + index.sc_.include_.size());
  for (const slot_t slot : index.sc_.key_) {
    if (slot < table_schema.ColumnCount() && slot < source.Columns()) {
      columns.push_back(source.Column(slot));
    } else {
      columns.emplace_back(table_schema.GetColumn(slot).Type());
    }
  }
  for (const slot_t slot : index.sc_.include_) {
    if (slot < table_schema.ColumnCount() && slot < source.Columns()) {
      columns.push_back(source.Column(slot));
    } else {
      columns.emplace_back(table_schema.GetColumn(slot).Type());
    }
  }
  TableStatistics result(Schema("", {}));
  result.Assign(source.Rows(), std::move(columns));
  return result;
}

}  // namespace

IndexOnlyScanPlan::IndexOnlyScanPlan(const Table& table, const Index& index,
                                     const TableStatistics& ts,
                                     std::vector<Value> begin_key,
                                     std::vector<Value> end_key, bool ascending,
                                     Expression where,
                                     std::vector<ColumnName> provided_order)
    : table_(table),
      index_(index),
      stats_(
          IndexOnlyStats(begin_key.empty() && end_key.empty()
                             ? ts
                             : IndexRangeStats(ts, index, begin_key, end_key),
                         table.GetSchema(), index)),
      begin_key_(std::move(begin_key)),
      end_key_(std::move(end_key)),
      ascending_(ascending),
      where_(std::move(where)),
      output_schema_(OutputSchema()),
      provided_order_(std::move(provided_order)) {
  if (index_.IsUnique() && !begin_key_.empty() && begin_key_ == end_key_ &&
      begin_key_.size() == index_.sc_.key_.size()) {
    stats_ = stats_.ScaleCardinality(1);
  }
}

Schema IndexOnlyScanPlan::OutputSchema() const {
  std::vector<Column> cols;
  cols.reserve(index_.sc_.key_.size() + index_.sc_.include_.size());
  for (const auto& key : index_.sc_.key_) {
    cols.push_back(table_.GetSchema().GetColumn(key));
  }
  for (const auto& included : index_.sc_.include_) {
    cols.push_back(table_.GetSchema().GetColumn(included));
  }
  return {"", cols};
}

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

size_t IndexOnlyScanPlan::AccessRowCount() const { return EmitRowCount(); }

size_t IndexOnlyScanPlan::EmitRowCount() const {
  if (index_.IsUnique() && begin_key_ == end_key_ &&
      begin_key_.size() == index_.sc_.key_.size()) {
    return 1;
  }
  return stats_.Rows();
}

bool IndexOnlyScanPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                                    const std::vector<bool>& ascending) const {
  return OrderMatches(provided_order_, ascending_, expressions, ascending);
}

bool IndexOnlyScanPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  return OrderMatches(provided_order_, ascending_, expressions, ascending,
                      nulls_first);
}

void IndexOnlyScanPlan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexOnlyScan: " << table_.GetSchema().Name() << " with " << index_
    << " (estimated rows: " << AccessRowCount() << ")";
}

std::string IndexOnlyScanPlan::ToString() const {
  return "IndexOnlyScan: " + std::string(table_.GetSchema().Name()) + " with " +
         index_.sc_.name_ +
         " (estimated rows: " + std::to_string(AccessRowCount()) + ")";
}

bool IndexOnlyScanPlan::ContainsParameterSlots() const {
  if (ContainsParameterSlot(where_)) {
    return true;
  }
  return std::ranges::any_of(select_, [](const NamedExpression& column) {
    return ContainsParameterSlot(column.expression);
  });
}

bool IndexOnlyScanPlan::InstallParameterSlots(
    const std::vector<Value>& parameters, size_t* cursor,
    std::vector<Expression>* slots) {
  for (NamedExpression& column : select_) {
    if (!SlotizeInPlace(&column.expression, parameters, cursor, slots)) {
      return false;
    }
  }
  return SlotizeInPlace(&where_, parameters, cursor, slots);
}

}  // namespace tinylamb
