/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/sort_plan.hpp"

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

const Schema& SortPlan::GetSchema() const { return src_->GetSchema(); }

size_t SortPlan::AccessRowCount() const { return src_->AccessRowCount(); }

size_t SortPlan::EmitRowCount() const { return src_->EmitRowCount(); }

bool SortPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                            const std::vector<bool>& ascending) const {
  // The sort plan itself delivers exactly our sort keys.
  if (expressions.size() > keys_.size()) { return false; }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i]->ToString() != expressions[i]->ToString()) { return false; }
    if (ascending_[i] != ascending[i]) { return false; }
  }
  return true;
}

void SortPlan::Dump(std::ostream& o, int indent) const {
  o << "Sort: (";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) { o << ", "; }
    o << *keys_[i];
    o << (ascending_[i] ? " ASC" : " DESC");
  }
  o << ")\n"
    << std::string(indent + 2, ' ');
  src_->Dump(o, indent + 2);
}

std::string SortPlan::ToString() const {
  std::ostringstream out;
  out << "Sort: (";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) { out << ", "; }
    out << *keys_[i];
    out << (ascending_[i] ? " ASC" : " DESC");
  }
  out << ")";
  return out.str();
}

}  // namespace tinylamb
