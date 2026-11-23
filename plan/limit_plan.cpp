/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/limit_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>

#include "common/constants.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

const Schema& LimitPlan::GetSchema() const { return src_->GetSchema(); }

size_t LimitPlan::AccessRowCount() const { return src_->AccessRowCount(); }

size_t LimitPlan::EmitRowCount() const {
  // T3879: limit_count_ == 0 is the engine-wide "unbounded" sentinel on the
  // UNORDERED limit route -- LimitExecutor::NextBatch (executor/limit.cpp)
  // and LimitedRows read it the same way, so the estimate matches what the
  // paired executor emits. An explicit `LIMIT 0` never plans through here:
  // SqlEngine intercepts HasLimit()&&Limit()==0 before lowering a usable
  // executor, and the ordered route goes through TopNPlan, whose executor
  // really does treat 0 as an empty bound (see topn_plan.cpp).
  const size_t input = src_->EmitRowCount();
  if (input <= limit_offset_) {
    return 0;
  }
  const size_t remaining = input - limit_offset_;
  return limit_count_ == 0 ? remaining : std::min(remaining, limit_count_);
}

void LimitPlan::Dump(std::ostream& o, int indent) const {
  o << Indent(static_cast<size_t>(std::max(indent, 0))) << "Limit: "
    << limit_count_ << " offset " << limit_offset_ << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  DumpChild(o, *src_, indent + 2);
}

std::string LimitPlan::ToString() const {
  std::ostringstream out;
  out << "Limit: " << limit_count_ << " offset " << limit_offset_;
  return out.str();
}

}  // namespace tinylamb
