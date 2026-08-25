/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "max1_row_plan.hpp"

#include "common/constants.hpp"

namespace tinylamb {

void Max1RowPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "Max1Row\n";
  child_->Dump(output, indent + 2);
}

}  // namespace tinylamb
