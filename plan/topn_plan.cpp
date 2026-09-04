/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "topn_plan.hpp"

#include <algorithm>
#include <ostream>
#include <sstream>

#include "common/constants.hpp"

namespace tinylamb {

size_t TopNPlan::EmitRowCount() const {
  const size_t input = child_->EmitRowCount();
  if (input <= offset_) {
    return 0;
  }
  const size_t remaining = input - offset_;
  if (with_ties_) {
    return remaining;
  }
  return limit_ == 0 ? remaining : std::min(remaining, limit_);
}

bool TopNPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                           const std::vector<bool>& ascending) const {
  if (expressions.size() > keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i].ascending != ascending[i] ||
        keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
  }
  return true;
}

bool TopNPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() > keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i].ascending != ascending[i] ||
        keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
    const bool provided = keys_[i].nulls_first.value_or(keys_[i].ascending);
    const bool requested = i < nulls_first.size()
                               ? nulls_first[i].value_or(ascending[i])
                               : ascending[i];
    if (provided != requested) {
      return false;
    }
  }
  return true;
}

void TopNPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << ToString() << "\n" << Indent(indent + 2);
  child_->Dump(output, indent + 2);
}

std::string TopNPlan::ToString() const {
  std::ostringstream output;
  output << "TopN (limit=" << limit_ << ", offset=" << offset_
         << (with_ties_ ? ", with ties" : "") << ") [";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) {
      output << ", ";
    }
    output << keys_[i].expression->ToString()
           << (keys_[i].ascending ? " ASC" : " DESC");
  }
  output << "]";
  return output.str();
}

}  // namespace tinylamb
