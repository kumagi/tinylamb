/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "distinct_plan.hpp"

#include <ostream>

#include "common/constants.hpp"

namespace tinylamb {

void DistinctPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "Distinct";
  if (!distinct_on_.empty()) {
    output << " (on=[";
    for (size_t i = 0; i < distinct_on_.size(); ++i) {
      if (i > 0) output << ", ";
      output << *distinct_on_[i];
    }
    output << "])";
  }
  output << "\n";
  child_->Dump(output, indent + 2);
}

std::string DistinctPlan::ToString() const { return "Distinct"; }

}  // namespace tinylamb
