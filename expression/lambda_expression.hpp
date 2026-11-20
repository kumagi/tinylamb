/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_LAMBDA_EXPRESSION_HPP
#define TINYLAMB_LAMBDA_EXPRESSION_HPP

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

// Inline lambda argument of a higher-order function, e.g. `e -> e = 'b'` in
// a future ARRAY_FIND-style call. No higher-order function is implemented yet.
// The node is opaque to generic expression
// walkers: its parameter references are bound only while the enclosing
// higher-order function evaluates the body per element.
class LambdaExpression : public ExpressionBase {
 public:
  LambdaExpression(std::vector<std::string> parameters, Expression body)
      : parameters_(std::move(parameters)), body_(std::move(body)) {}

  [[nodiscard]] TypeTag NodeKind() const override {
    return TypeTag::kLambdaExp;
  }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& /*row*/, const Schema& /*schema*/) const override;
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override;
  [[nodiscard]] tinylamb::Type ResultType(
      const Schema& /*unused*/) const override {
    return {TypeTag::kInvalid};
  }
  [[nodiscard]] tinylamb::Type ResultType(const Schema& /*left*/,
                                          const Schema& /*right*/) const override {
    return {TypeTag::kInvalid};
  }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& output) const override;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override {
    return {};
  }

  [[nodiscard]] const std::vector<std::string>& Parameters() const {
    return parameters_;
  }
  [[nodiscard]] const Expression& Body() const { return body_; }

 private:
  std::vector<std::string> parameters_;
  Expression body_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LAMBDA_EXPRESSION_HPP
