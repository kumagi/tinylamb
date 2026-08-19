/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/interval_expression.hpp"

#include <ostream>

namespace tinylamb {

Value IntervalExpression::Evaluate(const Row&, const Schema&) const {
  return Value(std::to_string(amount_) + " " + unit_);
}

Value IntervalExpression::Evaluate(const Row*, const Schema&, const Row*,
                                   const Schema&) const {
  return Value(std::to_string(amount_) + " " + unit_);
}

tinylamb::Type IntervalExpression::ResultType(const Schema&) const {
  return tinylamb::Type(TypeTag::kVarChar);
}

tinylamb::Type IntervalExpression::ResultType(const Schema&,
                                              const Schema&) const {
  return tinylamb::Type(TypeTag::kVarChar);
}

std::string IntervalExpression::ToString() const {
  return "INTERVAL " + std::to_string(amount_) + " " + unit_;
}

void IntervalExpression::Dump(std::ostream& output) const {
  output << ToString();
}

}  // namespace tinylamb
