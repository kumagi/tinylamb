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
  const size_t capped =
      limit_count_ == 0 ? src_->EmitRowCount()
                        : std::min(src_->EmitRowCount(), limit_count_);
  return capped;
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
