//
// Created by kumagi on 2022/03/03.
//

#ifndef TINYLAMB_NAMED_EXPRESSION_HPP
#define TINYLAMB_NAMED_EXPRESSION_HPP

#include <string>
#include <utility>

#include "expression/column_value.hpp"
#include "expression/expression.hpp"

namespace tinylamb {

struct NamedExpression {
  explicit NamedExpression(std::string_view name_)
      : name(name_), expression(ColumnValueExp(name_)) {}
  NamedExpression(std::string_view name_, std::string_view column_name)
      : name(name_), expression(ColumnValueExp(column_name)) {}
  NamedExpression(std::string_view name_, Expression exp)
      : name(name_), expression(std::move(exp)) {}

  friend std::ostream& operator<<(std::ostream& o, const NamedExpression& ne) {
    o << *ne.expression;
    if (ne.expression->Type() == TypeTag::kColumnValue) {
      const auto* cv =
          reinterpret_cast<const ColumnValue*>(ne.expression.get());
      if (cv->ColumnName() != ne.name) {
        o << " AS " << ne.name;
      }
    } else if (!ne.name.empty()) {
      o << " AS " << ne.name;
    }

    return o;
  }

  std::string name;
  Expression expression;
};

}  // namespace tinylamb

#endif  // TINYLAMB_NAMED_EXPRESSION_HPP
