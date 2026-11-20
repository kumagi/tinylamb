/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/interval_expression.hpp"

#include <ostream>
#include <string>

#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

StatusOr<Value> IntervalExpression::TryEvaluate(
    const Row& /*row*/, const Schema& /*schema*/) const {
  return Value(std::string(text_)).WithInterval();
}

Value IntervalExpression::Evaluate(const Row& /*row*/,
                                   const Schema& /*schema*/) const {
  // EXC-SHIM: unwrap through the shared status boundary like other nodes.
  return ExcShimUnwrap(TryEvaluate(Row(), Schema()),
                       "IntervalExpression::Evaluate");
}

StatusOr<Value> IntervalExpression::TryEvaluate(
    const Row* /*unused*/, const Schema& /*unused*/, const Row* /*unused*/,
    const Schema& /*unused*/) const {
  return Value(std::string(text_)).WithInterval();
}

Value IntervalExpression::Evaluate(const Row* /*unused*/,
                                   const Schema& /*unused*/,
                                   const Row* /*unused*/,
                                   const Schema& /*unused*/) const {
  return ExcShimUnwrap(
      TryEvaluate(static_cast<const Row*>(nullptr), Schema(), nullptr, Schema()),
      "IntervalExpression::Evaluate");
}

tinylamb::Type IntervalExpression::ResultType(const Schema& /*unused*/) const {
  return {TypeTag::kVarChar};
}

tinylamb::Type IntervalExpression::ResultType(const Schema& /*unused*/,
                                              const Schema& /*unused*/) const {
  return {TypeTag::kVarChar};
}

std::string IntervalExpression::ToString() const {
  // T2105: print the ORIGINAL SQL spelling, not IntervalValue::ToString()'s
  // internal canonical form ("0-0 3 0:0:0" is neither the spelling nor a
  // unit name, so debug dumps hid what the query actually said). Both forms
  // below are valid GoogleSQL interval literals again, unlike the canonical.
  if (!raw_amount_.empty()) {
    return "INTERVAL " + raw_amount_ + " " + unit_;
  }
  return "INTERVAL " + std::to_string(amount_) + " " + unit_;
}

void IntervalExpression::Dump(std::ostream& output) const {
  output << ToString();
}

}  // namespace tinylamb
