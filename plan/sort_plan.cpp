/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "sort_plan.hpp"

#include <ostream>
#include <sstream>
#include <string>

#include "common/constants.hpp"

namespace tinylamb {

bool SortPlan::IsOrderedBy(const std::vector<Expression>& expressions,
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

void SortPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "Sort: [";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) { output << ", "; }
    output << keys_[i].expression->ToString()
           << (keys_[i].ascending ? " ASC" : " DESC");
  }
  output << "]\n" << Indent(indent + 2);
  child_->Dump(output, indent + 2);
}

std::string SortPlan::ToString() const {
  std::ostringstream output;
  output << "Sort: [";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) { output << ", "; }
    output << keys_[i].expression->ToString()
           << (keys_[i].ascending ? " ASC" : " DESC");
  }
  output << "]";
  return output.str();
}

}  // namespace tinylamb
