/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/relational_plan.hpp"

#include <cstddef>
#include <ostream>
#include <string>

#include "common/constants.hpp"

namespace tinylamb {

void RelationalPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent))
         << "RelationalPlan(memo-selected)";
}

std::string RelationalPlan::ToString() const {
  return "RelationalPlan(memo-selected)";
}

}  // namespace tinylamb
