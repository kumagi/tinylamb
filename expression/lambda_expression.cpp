/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/lambda_expression.hpp"

#include <stdexcept>
#include <utility>

namespace tinylamb {

Value LambdaExpression::Evaluate(const Row&, const Schema&) const {
  throw std::runtime_error(
      "lambda evaluated outside an array function (internal error)");
}

std::string LambdaExpression::ToString() const {
  std::string params;
  for (size_t i = 0; i < params_.size(); ++i) {
    if (i) { params += ", "; }
    params += params_[i];
  }
  if (params_.size() > 1) { params = "(" + params + ")"; }
  return params + " -> " + (body_ ? body_->ToString() : "NULL");
}

Expression LambdaExpressionExp(std::vector<std::string> params,
                               Expression body) {
  return std::make_shared<LambdaExpression>(std::move(params),
                                            std::move(body));
}

}  // namespace tinylamb
