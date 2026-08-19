/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/query_expression.hpp"

#include <ostream>
#include <stdexcept>

namespace tinylamb {

Value QueryExpression::Evaluate(const Row&, const Schema&) const {
  throw std::runtime_error("query expression requires relational evaluation");
}

std::unordered_set<ColumnName> QueryExpression::TouchedColumns() const {
  return test_ ? test_->TouchedColumns() : std::unordered_set<ColumnName>{};
}

std::string QueryExpression::ToString() const {
  if (exists_) return negated_ ? "NOT EXISTS(...)" : "EXISTS(...)";
  if (test_)
    return test_->ToString() + (negated_ ? " NOT IN(...)" : " IN(...)");
  return "SCALAR_SUBQUERY(...)";
}

void QueryExpression::Dump(std::ostream& output) const { output << ToString(); }

}  // namespace tinylamb
