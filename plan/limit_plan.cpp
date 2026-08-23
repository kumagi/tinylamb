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
#include "executor/executor_base.hpp"
#include "executor/limit.hpp"
#include "type/schema.hpp"

namespace tinylamb {

Executor LimitPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<LimitExecutor>(src_->EmitExecutor(ctx), limit_count_,
                                         limit_offset_);
}

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
