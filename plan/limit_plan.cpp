/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/limit_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// EmitExecutor lives in the relational factory (executor/relational_factory.cpp).

const Schema& LimitPlan::GetSchema() const { return src_->GetSchema(); }

size_t LimitPlan::AccessRowCount() const { return src_->AccessRowCount(); }

size_t LimitPlan::EmitRowCount() const {
  const size_t input = src_->EmitRowCount();
  if (input <= limit_offset_) {
    return 0;
  }
  const size_t remaining = input - limit_offset_;
  return limit_count_ == 0 ? remaining : std::min(remaining, limit_count_);
}

void LimitPlan::Dump(std::ostream& o, int indent) const {
  o << "Limit: " << limit_count_ << " offset " << limit_offset_ << "\n"
    << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

std::string LimitPlan::ToString() const {
  std::ostringstream out;
  out << "Limit: " << limit_count_ << " offset " << limit_offset_;
  return out.str();
}

}  // namespace tinylamb
