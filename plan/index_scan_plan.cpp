/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "index_scan_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool IntConstant(const Expression& expression, int64_t* value) {
  if (!expression || expression->Type() != TypeTag::kConstantValue) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.type != ValueType::kInt64) {
    return false;
  }
  *value = constant.value.int_value;
  return true;
}

bool AffineColumn(const Expression& expression, const ColumnName& target,
                  int64_t* multiplier) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name != target.name || column.schema != target.schema) {
      return false;
    }
    return true;
  }
  if (expression->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& binary = expression->AsBinaryExpression();
  int64_t constant = 0;
  if (binary.Op() == BinaryOperation::kAdd &&
      IntConstant(binary.Right(), &constant)) {
    return AffineColumn(binary.Left(), target, multiplier);
  }
  if (binary.Op() == BinaryOperation::kSubtract &&
      IntConstant(binary.Right(), &constant)) {
    return AffineColumn(binary.Left(), target, multiplier);
  }
  if (binary.Op() == BinaryOperation::kMultiply &&
      IntConstant(binary.Right(), &constant)) {
    if (!AffineColumn(binary.Left(), target, multiplier)) {
      return false;
    }
    *multiplier *= constant;
    return true;
  }
  return false;
}

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
    // Index keys use memcomparable byte order with NULL encoded smallest, so
    // a forward scan yields NULLS FIRST and a reverse scan NULLS LAST
    // (affine post-projection moves values but not the NULL position).
    const bool requested = i < nulls_first.size()
                               ? nulls_first[i].value_or(ascending[i])
                               : ascending[i];
    if (requested != scan_ascending) {
      return false;
    }
    if (expressions[i]->Type() == TypeTag::kColumnValue) {
      const ColumnName& column =
          expressions[i]->AsColumnValue().GetColumnName();
      if (column.name != provided[i].name || ascending[i] != scan_ascending) {
        return false;
      }
      continue;
    }
    // An affine expression with a non-zero constant multiplier preserves
    // index order (or reverses it when the multiplier is negative).  The
    // projection executor evaluates the expression after the scan; no sort
    // is needed for ORDER BY id + constant.
    int64_t multiplier = 1;
    if (!AffineColumn(expressions[i], provided[i], &multiplier) ||
        multiplier == 0) {
      return false;
    }
    const bool expression_ascending =
        multiplier > 0 ? scan_ascending : !scan_ascending;
    if (ascending[i] != expression_ascending) {
      return false;
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
TableStatistics BoundsStats(
    const TableStatistics& ts, const Index& index,
    const std::vector<std::pair<std::vector<Value>, std::vector<Value>>>&
        ranges) {
  Value min;
  Value max;
  for (const auto& range : ranges) {
    if (range.first.empty()) {
      continue;
    }
    const Value& key = range.first.front();
    if (min.IsNull() || key < min) {
      min = key;
    }
    if (max.IsNull() || max < key) {
      max = key;
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
      stats_(begin_key.empty() && end_key.empty()
                 ? ts
                 : ts.TransformBy(index.sc_.key_[0], FirstOrNull(begin_key),
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
    bool ascending, Expression where, std::vector<ColumnName> provided_order,
    bool lock_rows, bool wait_for_write_intent)
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

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

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
  // Point ranges are sorted and disjoint before construction, so scanning
  // them in sequence preserves the advertised global key order.
  return OrderMatches(provided_order_, ascending_, expressions, ascending);
}

bool IndexScanPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  return OrderMatches(provided_order_, ascending_, expressions, ascending,
                      nulls_first);
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
