/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/merge_join_plan.hpp"

#include <algorithm>
#include <ostream>
#include <stdexcept>
#include <utility>

#include "expression/column_value.hpp"

namespace tinylamb {

MergeJoinPlan::MergeJoinPlan(Plan left, std::vector<ColumnName> left_keys,
                             Plan right, std::vector<ColumnName> right_keys,
                             JoinKind kind)
    : left_(std::move(left)),
      right_(std::move(right)),
      left_keys_(std::move(left_keys)),
      right_keys_(std::move(right_keys)),
      kind_(kind),
      schema_(IsSemiJoinKind(kind_) || IsAntiJoinKind(kind_)
                  ? left_->GetSchema()
                  : left_->GetSchema() + right_->GetSchema()),
      stats_(left_->GetStats().ScaleToRows(
          std::min(left_->GetStats().Rows(), right_->GetStats().Rows()))) {
  if (left_keys_.empty() || left_keys_.size() != right_keys_.size()) {
    throw std::invalid_argument("MergeJoinPlan requires equally-sized non-empty keys");
  }
  stats_.Concat(right_->GetStats().ScaleToRows(stats_.Rows()));
}

size_t MergeJoinPlan::AccessRowCount() const {
  return left_->AccessRowCount() + right_->AccessRowCount();
}

size_t MergeJoinPlan::EmitRowCount() const {
  if (IsSemiJoinKind(kind_)) {
    return std::min(left_->EmitRowCount(), right_->EmitRowCount());
  }
  if (IsAntiJoinKind(kind_)) { return left_->EmitRowCount(); }
  if (IsLeftOuterJoinKind(kind_) || IsRightOuterJoinKind(kind_) ||
      IsFullOuterJoinKind(kind_)) {
    return std::max(left_->EmitRowCount(), right_->EmitRowCount());
  }
  return std::min(left_->EmitRowCount(), right_->EmitRowCount());
}

bool MergeJoinPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  // Right/full outer output appends unmatched right rows after the merge and
  // therefore cannot claim the left-key order. Left outer preserves the left
  // traversal order because unmatched right rows are never emitted.
  if (IsRightOuterJoinKind(kind_) || IsFullOuterJoinKind(kind_)) {
    return false;
  }
  if (expressions.size() > left_keys_.size() ||
      expressions.size() != ascending.size()) {
    return false;
  }
  std::vector<Expression> left_order;
  left_order.reserve(expressions.size());
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!expressions[i] || expressions[i]->Type() != TypeTag::kColumnValue ||
        expressions[i]->AsColumnValue().GetColumnName() != left_keys_[i] ||
        !ascending[i]) {
      return false;
    }
    left_order.push_back(expressions[i]);
  }
  return left_->IsOrderedBy(left_order, ascending);
}

void MergeJoinPlan::Dump(std::ostream& o, int indent) const {
  o << ToString() << ": keys=" << left_keys_.size() << "\n"
    << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

std::string MergeJoinPlan::ToString() const {
  std::string name = IsSemiJoinKind(kind_)
                         ? "MergeSemiJoin"
                         : IsAntiJoinKind(kind_) ? "MergeAntiJoin" : "MergeJoin";
  return name + " (keys=" + std::to_string(left_keys_.size()) + ")";
}

}  // namespace tinylamb
