//
// Created by kumagi on 2022/03/14.
//

#include "expression/expression.hpp"

#include <cassert>
#include <memory>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"

namespace tinylamb {

const ColumnValue& ExpressionBase::AsColumnValue() const {
  assert(Type() == TypeTag::kColumnValue);
  return reinterpret_cast<const ColumnValue&>(*this);
}

const BinaryExpression& ExpressionBase::AsBinaryExpression() const {
  assert(Type() == TypeTag::kBinaryExp);
  return reinterpret_cast<const BinaryExpression&>(*this);
}

const ConstantValue& ExpressionBase::AsConstantValue() const {
  assert(Type() == TypeTag::kConstantValue);
  return reinterpret_cast<const ConstantValue&>(*this);
}

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