//
// Created by kumagi on 2022/03/02.
//
#include "expression/expression.hpp"

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "type/value.hpp"

namespace tinylamb {

Expression Expression::ConstantValue(const Value& v) {
  return Expression(new class ConstantValue(v));
}

Expression Expression::BinaryExpression(Expression&& left,
                                        BinaryOperation operation,
                                        Expression&& right) {
  return Expression(new class BinaryExpression(std::move(left.exp_), operation,
                                               std::move(right.exp_)));
}

Expression Expression::ColumnValue(std::string_view column_name) {
  return Expression(new class ColumnValue(column_name));
}

}  // namespace tinylamb