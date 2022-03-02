//
// Created by kumagi on 2022/03/03.
//

#ifndef TINYLAMB_NAMED_EXPRESSION_HPP
#define TINYLAMB_NAMED_EXPRESSION_HPP

#include <string>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

struct NamedExpression {
  explicit NamedExpression(std::string_view name_)
      : name(name_), expression(Expression::ColumnValue(name_)) {}
  NamedExpression(std::string_view name_, std::string_view column_name)
      : name(name_), expression(Expression::ColumnValue(column_name)) {}
  NamedExpression(std::string_view name_, Expression exp)
      : name(name_), expression(std::move(exp)) {}

  friend std::ostream& operator<<(std::ostream& o, const NamedExpression& ne) {
    o << ne.expression;
    if (!ne.name.empty()) {
      o << " AS " << ne.name;
    }
    return o;
  }

  std::string name;
  Expression expression;
};

}  // namespace tinylamb

#endif  // TINYLAMB_NAMED_EXPRESSION_HPP
