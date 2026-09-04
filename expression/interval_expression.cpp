/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/interval_expression.hpp"

#include <ostream>
#include <string>

#include "expression/expression.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value IntervalExpression::Evaluate(const Row& /*row*/,
                                   const Schema& /*schema*/) const {
  return Value(std::string(text_));
}

Value IntervalExpression::Evaluate(const Row* /*unused*/,
                                   const Schema& /*unused*/,
                                   const Row* /*unused*/,
                                   const Schema& /*unused*/) const {
  return Value(std::string(text_));
}

tinylamb::Type IntervalExpression::ResultType(const Schema& /*unused*/) const {
  return {TypeTag::kVarChar};
}

tinylamb::Type IntervalExpression::ResultType(const Schema& /*unused*/,
                                              const Schema& /*unused*/) const {
  return {TypeTag::kVarChar};
}

std::string IntervalExpression::ToString() const { return "INTERVAL " + text_; }

void IntervalExpression::Dump(std::ostream& output) const {
  output << ToString();
}

}  // namespace tinylamb
