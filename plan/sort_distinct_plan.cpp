/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "sort_distinct_plan.hpp"

#include <cstddef>
#include <ostream>
#include <string>

#include "common/constants.hpp"
#include "plan/distinct_plan.hpp"

namespace tinylamb {

SortDistinctPlan::SortDistinctPlan(Plan child)
    : child_(std::move(child)), stats_(EstimateDistinctPlanStats(child_)) {}

void SortDistinctPlan::Dump(std::ostream& output, int indent) const {
  output << "SortDistinct\n" << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(output, indent + 2);
}

std::string SortDistinctPlan::ToString() const { return "SortDistinct"; }

}  // namespace tinylamb
