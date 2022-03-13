//
// Created by kumagi on 2022/03/14.
//

#include "expression/expression.hpp"

#include <memory>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"

namespace tinylamb {

Expression ColumnValueExp(std::string_view col_name) {
  return std::make_shared<ColumnValue>(col_name);
}

Expression ConstantValueExp(const Value& v) {
  return std::make_shared<ConstantValue>(v);
}

Expression BinaryExpressionExp(Expression left, BinaryOperation op,
                               Expression right) {
  return std::make_shared<BinaryExpression>(std::move(left), op,
                                            std::move(right));
}

}  // namespace tinylamb