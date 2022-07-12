//
// Created by kumagi on 2022/03/03.
//

#ifndef TINYLAMB_NAMED_EXPRESSION_HPP
#define TINYLAMB_NAMED_EXPRESSION_HPP

#include <string>
#include <utility>

#include "common/log_message.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"

namespace tinylamb {

struct NamedExpression {
  explicit NamedExpression(ColumnName name_)
      : name(), expression(ColumnValueExp(name_)) {}
  explicit NamedExpression(std::string_view name_)
      : name(), expression(ColumnValueExp(name_)) {}
  NamedExpression(std::string_view name_, ColumnName column_name)
      : name(name_), expression(ColumnValueExp(column_name)) {}
  NamedExpression(std::string_view name_, Expression exp)
      : name(name_), expression(std::move(exp)) {}

  friend std::ostream& operator<<(std::ostream& o, const NamedExpression& ne);

  std::string name;
  Expression expression;
};

}  // namespace tinylamb

#endif  // TINYLAMB_NAMED_EXPRESSION_HPP
