/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "topn_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"

namespace tinylamb {

size_t TopNPlan::EmitRowCount() const {
  // LIMIT 0 yields zero rows: TopNExecutor never emits for an empty bound,
  // so the estimate must agree (the engine reads limit==0 as a real bound
  // here, unlike the unordered-limit paths that treat 0 as "unbounded").
  if (limit_ == 0 && !with_ties_) {
    return 0;
  }
  const size_t input = child_->EmitRowCount();
  if (input <= offset_) {
    return 0;
  }
  const size_t remaining = input - offset_;
  if (with_ties_) {
    return remaining;
  }
  return std::min(remaining, limit_);
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
  output << Indent(static_cast<size_t>(indent)) << ToString() << "\n"
         << Indent(static_cast<size_t>(indent) + 2);
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
