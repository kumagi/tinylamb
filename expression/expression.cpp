/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "expression/expression.hpp"

#include <cassert>
#include <memory>
#include <unordered_set>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {
const ColumnValue& ExpressionBase::AsColumnValue() const {
  assert(Type() == TypeTag::kColumnValue);
  return reinterpret_cast<const ColumnValue&>(*this);
}

ColumnValue& ExpressionBase::AsColumnValue() {
  assert(Type() == TypeTag::kColumnValue);
  return reinterpret_cast<ColumnValue&>(*this);
}

const BinaryExpression& ExpressionBase::AsBinaryExpression() const {
  assert(Type() == TypeTag::kBinaryExp);
  return reinterpret_cast<const BinaryExpression&>(*this);
}

const ConstantValue& ExpressionBase::AsConstantValue() const {
  assert(Type() == TypeTag::kConstantValue);
  return reinterpret_cast<const ConstantValue&>(*this);
}

std::unordered_set<ColumnName> ExpressionBase::TouchedColumns() const {
  std::unordered_set<ColumnName> ret;
  switch (Type()) {
    case TypeTag::kBinaryExp: {
      const BinaryExpression& be = AsBinaryExpression();
      ret.merge(be.Left()->TouchedColumns());
      ret.merge(be.Right()->TouchedColumns());
      break;
    }
    case TypeTag::kColumnValue: {
      const ColumnValue& cv = AsColumnValue();
      ret.emplace(cv.GetColumnName());
      break;
    }
    case TypeTag::kConstantValue:
      break;
  }
  return ret;
}

Expression ColumnValueExp(const ColumnName& col_name) {
  return std::make_shared<ColumnValue>(col_name);
}

Expression ColumnValueExp(const std::string_view& col_name) {
  return std::make_shared<ColumnValue>(ColumnName(col_name));
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
