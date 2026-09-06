/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "sort_distinct_plan.hpp"

#include <cstddef>
#include <ostream>
#include <string>

#include "common/constants.hpp"

namespace tinylamb {

void SortDistinctPlan::Dump(std::ostream& output, int indent) const {
  output << "SortDistinct\n" << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(output, indent + 2);
}

std::string SortDistinctPlan::ToString() const { return "SortDistinct"; }

}  // namespace tinylamb
