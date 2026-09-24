/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "index_scan_plan.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "index/index.hpp"
#include "plan/index_skip_scan_plan.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

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
    return !(column.name != target.name || column.schema != target.schema);
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

double PrefixFrequency(const TableStatistics& statistics, const Index& index,
                       const std::vector<Value>& key) {
  if (statistics.Rows() == 0 || key.empty()) {
    return 0.0;
  }
  double rows = static_cast<double>(statistics.Rows());
  const size_t prefix_size = std::min(key.size(), index.sc_.key_.size());
  for (size_t i = 0; i < prefix_size; ++i) {
    const slot_t slot = index.sc_.key_[i];
    if (slot >= statistics.Columns() || key[i].IsNull()) {
      return 0.0;
    }
    const ColumnStats& column = statistics.Column(slot);
    if (column.NonNullCount() == 0) {
      rows = 0.0;
      break;
    }
    const double frequency = column.EstimateEqual(key[i]);
    const double selectivity =
        frequency / static_cast<double>(column.NonNullCount());
    rows *= std::clamp(selectivity, 0.0, 1.0);
  }
  return std::min(rows, static_cast<double>(statistics.Rows()));
}

size_t PointUnionRows(
    const TableStatistics& statistics, const Index& index,
    const std::vector<std::pair<std::vector<Value>, std::vector<Value>>>&
        ranges) {
  if (statistics.Rows() == 0 || ranges.empty()) {
    return 0;
  }
  if (index.IsUnique() &&
      std::ranges::all_of(ranges, [&index](const auto& range) {
        return range.first.size() == index.sc_.key_.size();
      })) {
    return ranges.size();
  }
  double rows = 0.0;
  for (const auto& range : ranges) {
    rows += PrefixFrequency(statistics, index, range.first);
  }
  return static_cast<size_t>(std::ceil(rows));
}

TableStatistics PointUnionStats(
    const TableStatistics& statistics, const Index& index,
    const std::vector<std::pair<std::vector<Value>, std::vector<Value>>>&
        ranges) {
  return statistics.ScaleCardinality(PointUnionRows(statistics, index, ranges));
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
      stats_(PointUnionStats(ts, index, ranges)),
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
  o << " (estimated rows: " << AccessRowCount() << ")";
}

std::string IndexScanPlan::ToString() const {
  std::string s = "IndexScan: " + std::string(table_.GetSchema().Name());
  if (!point_ranges_.empty()) {
    s += " x" + std::to_string(point_ranges_.size()) + " points";
  }
  s += " (estimated rows: " + std::to_string(AccessRowCount()) + ")";
  return s;
}

void IndexSkipScanPlan::Dump(std::ostream& o, int indent) const {
  o << Indent(static_cast<size_t>(indent)) << "IndexSkipScan\n";
  inner_->Dump(o, indent + 2);
}

std::string IndexSkipScanPlan::ToString() const { return "IndexSkipScan"; }

bool IndexScanPlan::ContainsParameterSlots() const {
  return ContainsParameterSlot(where_);
}

bool IndexScanPlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                          size_t* cursor,
                                          std::vector<Expression>* slots) {
  return SlotizeInPlace(&where_, parameters, cursor, slots);
}

namespace {

struct BoundRange {
  std::optional<Value> min;
  std::optional<Value> max;

  enum class Direction : bool { kRight, kLeft };

  void Update(BinaryOperation operation, const Value& value,
              Direction direction) {
    if (value.IsNull()) {
      return;
    }
    switch (operation) {
      case BinaryOperation::kEquals:
        min = max = value;
        return;
      case BinaryOperation::kNotEquals:
        return;
      case BinaryOperation::kLessThan:
      case BinaryOperation::kGreaterThan: {
        const bool upper = (direction == Direction::kRight &&
                            operation == BinaryOperation::kLessThan) ||
                           (direction == Direction::kLeft &&
                            operation == BinaryOperation::kGreaterThan);
        if (upper) {
          if (!max || value < *max) {
            max = value;
          }
        } else if (!min || *min < value) {
          min = value;
        }
        return;
      }
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThanEquals: {
        const bool upper = (direction == Direction::kRight &&
                            operation == BinaryOperation::kLessThanEquals) ||
                           (direction == Direction::kLeft &&
                            operation == BinaryOperation::kGreaterThanEquals);
        if (upper) {
          if (!max || value <= *max) {
            max = value;
          }
        } else if (!min || *min <= value) {
          min = value;
        }
        return;
      }
      default:
        return;
    }
  }
};

const Value* BoundConstant(const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kConstantValue) {
    return nullptr;
  }
  return &expression->AsConstantValue().GetValue();
}

}  // namespace

ReboundIndexKeys ReboundIndexKeysFromPredicate(const Expression& predicate,
                                               const Schema& schema,
                                               const Index& index,
                                               bool want_points) {
  ReboundIndexKeys keys;
  if (!predicate) {
    return keys;
  }
  std::unordered_map<slot_t, BoundRange> ranges;
  std::unordered_map<slot_t, std::vector<Value>> point_sets;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (!conjunct) {
      continue;
    }
    if (conjunct->Type() == TypeTag::kInExp) {
      const auto& in = conjunct->AsInExpression();
      if (in.child_ == nullptr || in.child_->Type() != TypeTag::kColumnValue ||
          in.list_.empty()) {
        continue;
      }
      const int offset =
          schema.Offset(in.child_->AsColumnValue().GetColumnName());
      if (offset < 0) {
        continue;
      }
      const ValueType column_type =
          schema.GetColumn(static_cast<size_t>(offset)).Type();
      std::vector<Value> points;
      bool indexable = true;
      for (const Expression& item : in.list_) {
        const Value* point = BoundConstant(item);
        if (point == nullptr || point->IsNull()) {
          continue;
        }
        if (point->type != column_type) {
          indexable = false;
          break;
        }
        points.push_back(*point);
      }
      if (indexable && !points.empty()) {
        auto& bucket = point_sets[static_cast<slot_t>(offset)];
        bucket.insert(bucket.end(), points.begin(), points.end());
      }
      continue;
    }
    if (conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (!IsComparison(binary.Op())) {
      continue;
    }
    const ColumnValue* column = nullptr;
    const Value* constant = nullptr;
    BoundRange::Direction direction = BoundRange::Direction::kRight;
    if (binary.Left()->Type() == TypeTag::kColumnValue) {
      constant = BoundConstant(binary.Right());
      if (constant != nullptr) {
        column = &binary.Left()->AsColumnValue();
      }
    }
    if (column == nullptr && binary.Right()->Type() == TypeTag::kColumnValue) {
      constant = BoundConstant(binary.Left());
      if (constant != nullptr) {
        column = &binary.Right()->AsColumnValue();
        direction = BoundRange::Direction::kLeft;
      }
    }
    if (column == nullptr || constant == nullptr || constant->IsNull()) {
      continue;
    }
    const int offset = schema.Offset(column->GetColumnName());
    if (offset < 0 ||
        constant->type !=
            schema.GetColumn(static_cast<size_t>(offset)).Type()) {
      continue;
    }
    ranges[static_cast<slot_t>(offset)].Update(binary.Op(), *constant,
                                               direction);
  }

  if (want_points) {
    size_t point_key_offset = 0;
    std::vector<Value> equality_prefix;
    while (point_key_offset < index.sc_.key_.size()) {
      const slot_t slot = index.sc_.key_[point_key_offset];
      if (point_sets.contains(slot)) {
        break;
      }
      const auto range = ranges.find(slot);
      if (range == ranges.end() || !range->second.min || !range->second.max ||
          *range->second.min != *range->second.max) {
        point_key_offset = index.sc_.key_.size();
        break;
      }
      equality_prefix.push_back(
          *range->second.min);  // NOLINT(bugprone-unchecked-optional-access)
      ++point_key_offset;
    }
    if (point_key_offset < index.sc_.key_.size()) {
      const auto points = point_sets.find(index.sc_.key_[point_key_offset]);
      if (points != point_sets.end() &&
          !ranges.contains(index.sc_.key_[point_key_offset])) {
        std::vector<Value> values = points->second;
        std::ranges::sort(values);
        values.erase(std::ranges::unique(values).begin(), values.end());
        for (const Value& value : values) {
          std::vector<Value> key = equality_prefix;
          key.push_back(value);
          std::vector<Value> end_key = key;
          keys.point_ranges.emplace_back(std::move(key), std::move(end_key));
        }
        return keys;
      }
    }
    return keys;
  }

  for (slot_t slot : index.sc_.key_) {
    const auto range = ranges.find(slot);
    if (range == ranges.end()) {
      break;
    }
    const bool equality = range->second.min && range->second.max &&
                          *range->second.min == *range->second.max;
    if (equality) {
      keys.begin_key.push_back(*range->second.min);
      keys.end_key.push_back(*range->second.max);
      continue;
    }
    if (range->second.min) {
      keys.begin_key.push_back(*range->second.min);
      if (range->second.max) {
        keys.end_key.push_back(*range->second.max);
      }
    } else if (!keys.begin_key.empty() && range->second.max) {
      keys.end_key.push_back(*range->second.max);
    }
    break;
  }
  return keys;
}

}  // namespace tinylamb
