//
// Created by kumagi on 2022/03/14.
//

#include "expression/expression.hpp"

#include <cassert>
#include <memory>
#include <unordered_set>

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

std::unordered_set<std::string> ExpressionBase::TouchedColumns() const {
  std::unordered_set<std::string> ret;
  switch (Type()) {
    case TypeTag::kBinaryExp: {
      const BinaryExpression& be = AsBinaryExpression();
      ret.merge(be.Left()->TouchedColumns());
      ret.merge(be.Right()->TouchedColumns());
      break;
    }
    case TypeTag::kColumnValue: {
      const ColumnValue& cv = AsColumnValue();
      ret.emplace(cv.ColumnName());
      break;
    }
    case TypeTag::kConstantValue:
      break;
  }
  return ret;
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