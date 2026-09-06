/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/lambda_expression.hpp"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value LambdaExpression::Evaluate(const Row& /*row*/,
                                 const Schema& /*schema*/) const {
  throw std::runtime_error("lambda must be applied by a higher-order function");
}

std::string LambdaExpression::ToString() const {
  std::ostringstream out;
  out << "(";
  for (size_t i = 0; i < parameters_.size(); ++i) {
    if (i != 0) {
      out << ", ";
    }
    out << parameters_[i];
  }
  out << ") -> ";
  out << (body_ ? body_->ToString() : std::string("NULL"));
  return out.str();
}

void LambdaExpression::Dump(std::ostream& output) const {
  output << ToString();
}

}  // namespace tinylamb
