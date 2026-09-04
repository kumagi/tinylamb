/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "empty_plan.hpp"

#include <ostream>
#include <string>
#include <utility>

#include "common/constants.hpp"

namespace tinylamb {

EmptyPlan::EmptyPlan(Plan child)
    : child_(std::move(child)), stats_(child_->GetSchema()) {}

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

void EmptyPlan::Dump(std::ostream& o, int indent) const {
  o << Indent(indent) << "EmptyResult (estimated cost: 0)\n";
}

std::string EmptyPlan::ToString() const { return "EmptyResult"; }

}  // namespace tinylamb
