//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_EXPRESSION_HPP
#define TINYLAMB_EXPRESSION_HPP
#include <memory>

#include "expression_base.hpp"
namespace tinylamb {

class Expression {
  explicit Expression(ExpressionBase* exp) : exp_(exp) {}

 public:
  static Expression ConstantValue(const Value& v);
  static Expression BinaryExpression(Expression&& left,
                                     BinaryOperation operation,
                                     Expression&& right);
  static Expression ColumnValue(std::string_view column_name);
  
  Value Evaluate(const Row& row, Schema* schema) const {
    return exp_->Evaluate(row, schema);
  }

  friend std::ostream& operator<<(std::ostream& o, const Expression& exp) {
    exp.Dump(o);
    return o;
  }
  void Dump(std::ostream& o) const { exp_->Dump(o); }

 private:
  std::shared_ptr<ExpressionBase> exp_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_HPP
