/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "set_operation_plan.hpp"

#include <ostream>
#include <stdexcept>
#include <vector>

#include "common/constants.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

ValueType CommonSetValueType(ValueType left, ValueType right) {
  if (left == ValueType::kNull) { return right; }
  if (right == ValueType::kNull || left == right) { return left; }
  if ((left == ValueType::kInt64 && right == ValueType::kDouble) ||
      (left == ValueType::kDouble && right == ValueType::kInt64)) {
    return ValueType::kDouble;
  }
  throw std::invalid_argument("set operation schemas have incompatible types");
}

}  // namespace

Schema SetOperationPlan::GenerateSchema() const {
  if (children_.empty()) {
    throw std::invalid_argument("set operation needs at least one child");
  }
  const Schema& first = children_.front()->GetSchema();
  std::vector<Column> columns;
  columns.reserve(first.ColumnCount());
  for (size_t column = 0; column < first.ColumnCount(); ++column) {
    ValueType type = first.GetColumn(column).Type();
    for (size_t child = 1; child < children_.size(); ++child) {
      const Schema& schema = children_[child]->GetSchema();
      if (schema.ColumnCount() != first.ColumnCount()) {
        throw std::invalid_argument(
            "set operation inputs must have the same column count");
      }
      type = CommonSetValueType(type, schema.GetColumn(column).Type());
    }
    columns.emplace_back(first.GetColumn(column).Name(), type);
  }
  return Schema(first.Name(), std::move(columns));
}

size_t SetOperationPlan::AccessRowCount() const {
  size_t rows = 0;
  for (const Plan& child : children_) { rows += child->AccessRowCount(); }
  return rows;
}

size_t SetOperationPlan::EmitRowCount() const {
  if (children_.empty()) { return 0; }
  if (operation_ == SetOperationKind::kUnionAll) {
    size_t rows = 0;
    for (const Plan& child : children_) { rows += child->EmitRowCount(); }
    return rows;
  }
  return children_.front()->EmitRowCount();
}

bool SetOperationPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  if (expressions.size() > order_keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (order_keys_[i].ascending != ascending[i] ||
        order_keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
  }
  return true;
}

void SetOperationPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << ToString() << '\n';
  for (const Plan& child : children_) { child->Dump(output, indent + 2); }
}

std::string SetOperationPlan::ToString() const {
  if (operation_ == SetOperationKind::kUnionAll && !order_keys_.empty()) {
    return "MergeAppend";
  }
  switch (operation_) {
    case SetOperationKind::kUnion: return "Union";
    case SetOperationKind::kUnionAll: return "UnionAll";
    case SetOperationKind::kIntersect: return "Intersect";
    case SetOperationKind::kIntersectAll: return "IntersectAll";
    case SetOperationKind::kExcept: return "Except";
    case SetOperationKind::kExceptAll: return "ExceptAll";
  }
  return "SetOperation";
}

}  // namespace tinylamb
