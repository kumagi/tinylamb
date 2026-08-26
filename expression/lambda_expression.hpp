/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_LAMBDA_EXPRESSION_HPP
#define TINYLAMB_LAMBDA_EXPRESSION_HPP

#include <string>
#include <unordered_set>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

// A SQL lambda: `(y1, y2) -> body`.  Lambdas never evaluate standalone; the
// enclosing array function (ARRAY_FILTER, ARRAY_TRANSFORM, ...) binds each
// parameter to an element (and 0-based offset for the second parameter) by
// extending the evaluation scope with a synthetic single-row frame.
class LambdaExpression : public ExpressionBase {
 public:
  LambdaExpression(std::vector<std::string> params, Expression body)
      : params_(std::move(params)), body_(std::move(body)) {}

  [[nodiscard]] TypeTag Type() const override { return TypeTag::kLambdaExp; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override {
    return body_ ? body_->ResultType(schema) : Type();
  }
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override {
    std::unordered_set<ColumnName> columns;
    if (body_) {
      for (const auto& column : body_->TouchedColumns()) {
        bool bound = false;
        for (const std::string& param : params_) {
          if (column.schema.empty() && column.name == param) {
            bound = true;
            break;
          }
        }
        if (!bound) { columns.insert(column); }
      }
    }
    return columns;
  }
  [[nodiscard]] const std::vector<std::string>& Params() const {
    return params_;
  }
  [[nodiscard]] const Expression& Body() const { return body_; }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override { o << ToString(); }

 private:
  std::vector<std::string> params_;
  Expression body_;
};

Expression LambdaExpressionExp(std::vector<std::string> params,
                               Expression body);

}  // namespace tinylamb

#endif  // TINYLAMB_LAMBDA_EXPRESSION_HPP
