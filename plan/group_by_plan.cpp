/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "plan/group_by_plan.hpp"

#include <cstddef>
#include <ostream>

#include "query/statement.hpp"

namespace tinylamb {

void GroupByPlan::Dump(std::ostream& o, int indent) const {
  o << std::string(static_cast<size_t>(indent), ' ') << "GroupByFinish:";
  // A plan built without a statement (e.g. a unit-test harness) has no
  // grouping keys to report.
  if (statement_ != nullptr) {
    o << " group_keys=" << statement_->GroupBy().size();
  }
  o << '\n';
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
