/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/band_join_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool IsProbeOnly(JoinKind kind) {
  return IsSemiJoinKind(kind) || IsAntiJoinKind(kind) ||
         IsNullAwareAntiJoinKind(kind);
}

size_t SaturatingProduct(size_t left, size_t right) {
  if (left == 0 || right == 0) {
    return 0;
  }
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  return left > kMax / right ? kMax : left * right;
}

bool IsOrderedComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

}  // namespace

BandJoinPlan::BandJoinPlan(Plan left, std::vector<ColumnName> left_keys,
                           Plan right, std::vector<ColumnName> right_keys,
                           JoinKind kind, BinaryOperation comparison,
                           Expression predicate,
                           std::optional<size_t> estimated_rows)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_keys_(std::move(left_keys)),
      right_keys_(std::move(right_keys)),
      kind_(kind),
      comparison_(comparison),
      predicate_(std::move(predicate)),
      schema_(IsProbeOnly(kind_) ? left_->GetSchema()
                                 : left_->GetSchema() + right_->GetSchema()),
      stats_(left_->GetStats().ScaleCardinality(
          estimated_rows.value_or(EstimatedJoinRowCount(
              kind_, left_->GetStats().Rows(), right_->GetStats().Rows(),
              SaturatingProduct(left_->GetStats().Rows(),
                                right_->GetStats().Rows()))))) {
  CHECK_MSG(left_ != nullptr && right_ != nullptr,
            "BandJoinPlan requires non-null children");
  CHECK_MSG(left_keys_.size() == 1,
            "BandJoinPlan requires one key column per input");
  CHECK_MSG(right_keys_.size() == 1,
            "BandJoinPlan requires one key column per input");
  CHECK_MSG(predicate_ != nullptr && IsOrderedComparison(comparison_),
            "BandJoinPlan requires an ordered comparison predicate");
  const bool unsupported_kind =
      kind_ == JoinKind::kMark || kind_ == JoinKind::kNullAwareAnti;
  CHECK_MSG(!unsupported_kind, "BandJoinPlan does not support this join kind");
  if (!IsProbeOnly(kind_)) {
    stats_.Concat(right_->GetStats().ScaleCardinality(stats_.Rows()));
  }
}

size_t BandJoinPlan::AccessRowCount() const {
  return left_->AccessRowCount() + right_->AccessRowCount();
}

size_t BandJoinPlan::EmitRowCount() const { return stats_.Rows(); }

bool BandJoinPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                               const std::vector<bool>& ascending) const {
  return IsOrderedBy(expressions, ascending, {});
}

bool BandJoinPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (IsRightOuterJoinKind(kind_) || IsFullOuterJoinKind(kind_) ||
      expressions.size() != 1 || ascending.size() != 1 || !ascending.front() ||
      !expressions.front() ||
      expressions.front()->Type() != TypeTag::kColumnValue ||
      expressions.front()->AsColumnValue().GetColumnName() !=
          left_keys_.front()) {
    return false;
  }
  return left_->IsOrderedBy(expressions, ascending, nulls_first);
}

void BandJoinPlan::Dump(std::ostream& o, int indent) const {
  o << ToString() << ": keys=1\n" << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  right_->Dump(o, indent + 2);
}

std::string BandJoinPlan::ToString() const {
  std::string result = "BandJoin (keys=1, comparison=";
  result += std::string(tinylamb::ToString(comparison_));
  if (predicate_) {
    result += ", predicate=" + predicate_->ToString();
  }
  result += ")";
  return result;
}

bool BandJoinPlan::ContainsParameterSlots() const {
  return left_->ContainsParameterSlots() || right_->ContainsParameterSlots() ||
         ContainsParameterSlot(predicate_);
}

bool BandJoinPlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                         size_t* cursor,
                                         std::vector<Expression>* slots) {
  return left_->InstallParameterSlots(parameters, cursor, slots) &&
         right_->InstallParameterSlots(parameters, cursor, slots) &&
         SlotizeInPlace(&predicate_, parameters, cursor, slots);
}

}  // namespace tinylamb
