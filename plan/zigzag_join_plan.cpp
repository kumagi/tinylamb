#include "plan/zigzag_join_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"

namespace tinylamb {
namespace {

bool SupportedOperation(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

ColumnName SingleKey(std::vector<ColumnName> keys) {
  CHECK_MSG(keys.size() == 1, "ZigZagJoinPlan requires one key per input");
  return std::move(keys.front());
}

size_t DefaultEstimatedRows(const Plan& left, const Plan& right,
                            BinaryOperation operation) {
  if (operation == BinaryOperation::kEquals) {
    return std::min(left->EmitRowCount(), right->EmitRowCount());
  }
  return std::max(left->EmitRowCount(), right->EmitRowCount());
}

}  // namespace

ZigZagJoinPlan::ZigZagJoinPlan(Plan left, ColumnName left_key, Plan right,
                               ColumnName right_key, BinaryOperation operation,
                               Expression predicate,
                               std::optional<size_t> estimated_rows)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_key_(std::move(left_key)),
      right_key_(std::move(right_key)),
      operation_(operation),
      predicate_(std::move(predicate)),
      schema_(left_->GetSchema() + right_->GetSchema()),
      stats_(left_->GetStats().ScaleCardinality(estimated_rows.value_or(
          DefaultEstimatedRows(left_, right_, operation_)))) {
  CHECK_MSG(left_ != nullptr && right_ != nullptr,
            "ZigZagJoinPlan requires non-null children");
  CHECK_MSG(SupportedOperation(operation_),
            "ZigZagJoinPlan requires an ordered comparison");
  CHECK_MSG(predicate_ != nullptr, "ZigZagJoinPlan requires a predicate");
  stats_.Concat(right_->GetStats().ScaleCardinality(stats_.Rows()));
}

ZigZagJoinPlan::ZigZagJoinPlan(Plan left, std::vector<ColumnName> left_keys,
                               Plan right, std::vector<ColumnName> right_keys,
                               BinaryOperation operation, Expression predicate,
                               std::optional<size_t> estimated_rows)
    : ZigZagJoinPlan(std::move(left), SingleKey(std::move(left_keys)),
                     std::move(right), SingleKey(std::move(right_keys)),
                     operation, std::move(predicate), estimated_rows) {}

size_t ZigZagJoinPlan::AccessRowCount() const {
  return left_->AccessRowCount() + right_->AccessRowCount();
}

size_t ZigZagJoinPlan::EmitRowCount() const { return stats_.Rows(); }

bool ZigZagJoinPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const {
  if (expressions.size() != 1 || ascending.size() != 1 || !ascending.front() ||
      !expressions.front() ||
      expressions.front()->Type() != TypeTag::kColumnValue ||
      expressions.front()->AsColumnValue().GetColumnName() != left_key_) {
    return false;
  }
  return left_->IsOrderedBy(expressions, ascending);
}

bool ZigZagJoinPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() != 1 || ascending.size() != 1 || !ascending.front() ||
      !expressions.front() ||
      expressions.front()->Type() != TypeTag::kColumnValue ||
      expressions.front()->AsColumnValue().GetColumnName() != left_key_) {
    return false;
  }
  return left_->IsOrderedBy(expressions, ascending, nulls_first);
}

void ZigZagJoinPlan::Dump(std::ostream& output, int indent) const {
  output << ToString() << "\n" << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(output, indent + 2);
  output << "\n" << Indent(static_cast<size_t>(indent) + 2);
  right_->Dump(output, indent + 2);
}

std::string ZigZagJoinPlan::ToString() const {
  std::string result = "ZigZagJoin (comparison=";
  result += tinylamb::ToString(operation_);
  result += ", predicate=" + predicate_->ToString() + ")";
  return result;
}

bool ZigZagJoinPlan::ContainsParameterSlots() const {
  return left_->ContainsParameterSlots() || right_->ContainsParameterSlots() ||
         ContainsParameterSlot(predicate_);
}

bool ZigZagJoinPlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                           size_t* cursor,
                                           std::vector<Expression>* slots) {
  return left_->InstallParameterSlots(parameters, cursor, slots) &&
         right_->InstallParameterSlots(parameters, cursor, slots) &&
         SlotizeInPlace(&predicate_, parameters, cursor, slots);
}

}  // namespace tinylamb
