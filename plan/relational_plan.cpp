/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/relational_plan.hpp"

#include <ostream>

namespace tinylamb {

void RelationalPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "RelationalPlan(memo-selected)";
}

std::string RelationalPlan::ToString() const {
  return "RelationalPlan(memo-selected)";
}

}  // namespace tinylamb
