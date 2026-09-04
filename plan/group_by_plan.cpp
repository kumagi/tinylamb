/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "plan/group_by_plan.hpp"

namespace tinylamb {

void GroupByPlan::Dump(std::ostream& o, int indent) const {
  o << std::string(static_cast<size_t>(indent), ' ') << "GroupByFinish:\n";
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
