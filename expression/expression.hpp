//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_EXPRESSION_HPP
#define TINYLAMB_EXPRESSION_HPP
#include <memory>
#include <utility>

#include "expression_base.hpp"

namespace tinylamb {

class Expression {
  explicit Expression(ExpressionBase* exp) : exp_(exp) {}

 public:
  explicit Expression(std::shared_ptr<ExpressionBase> exp)
      : exp_(std::move(exp)) {}
  static Expression ConstantValue(const Value& v);
  static Expression BinaryExpression(Expression&& left,
                                     BinaryOperation operation,
                                     Expression&& right);
  static Expression ColumnValue(std::string_view column_name);

  [[nodiscard]] TypeTag Type() const { return exp_->Type(); }
  Value Evaluate(const Row& row, Schema* schema) const {
    return exp_->Evaluate(row, schema);
  }

  friend std::ostream& operator<<(std::ostream& o, const Expression& exp) {
    exp.Dump(o);
    return o;
  }
  void Dump(std::ostream& o) const { exp_->Dump(o); }

  std::shared_ptr<ExpressionBase> exp_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_HPP
