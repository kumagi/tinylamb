/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/topn_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// EmitExecutor lives in the relational factory (executor/relational_factory.cpp).

const Schema& TopNPlan::GetSchema() const { return src_->GetSchema(); }

size_t TopNPlan::AccessRowCount() const { return src_->AccessRowCount(); }

size_t TopNPlan::EmitRowCount() const {
  const size_t child_rows = src_->EmitRowCount();
  const size_t after_offset =
      limit_offset_ >= child_rows ? 0 : child_rows - limit_offset_;
  return limit_count_ == 0 ? after_offset
                           : std::min(after_offset, limit_count_);
}

bool TopNPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                            const std::vector<bool>& ascending) const {
  if (expressions.size() > keys_.size()) { return false; }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i]->ToString() != expressions[i]->ToString()) { return false; }
    if (ascending_[i] != ascending[i]) { return false; }
  }
  return true;
}

void TopNPlan::Dump(std::ostream& o, int indent) const {
  o << "TopN: limit=" << limit_count_ << " offset=" << limit_offset_
    << " keys=(";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) { o << ", "; }
    o << *keys_[i];
    o << (ascending_[i] ? " ASC" : " DESC");
  }
  o << ")\n"
    << std::string(indent + 2, ' ');
  src_->Dump(o, indent + 2);
}

std::string TopNPlan::ToString() const {
  std::ostringstream out;
  out << "TopN: limit=" << limit_count_ << " offset=" << limit_offset_;
  return out.str();
}

}  // namespace tinylamb
