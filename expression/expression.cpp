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
#include "expression/unary_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/in_expression.hpp"
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

const UnaryExpression& ExpressionBase::AsUnaryExpression() const {
  assert(Type() == TypeTag::kUnaryExp);
  return reinterpret_cast<const UnaryExpression&>(*this);
}

const AggregateExpression& ExpressionBase::AsAggregateExpression() const {
  assert(Type() == TypeTag::kAggregateExp);
  return reinterpret_cast<const AggregateExpression&>(*this);
}

const CaseExpression& ExpressionBase::AsCaseExpression() const {
  assert(Type() == TypeTag::kCaseExp);
  return reinterpret_cast<const CaseExpression&>(*this);
}

const InExpression& ExpressionBase::AsInExpression() const {
  assert(Type() == TypeTag::kInExp);
  return reinterpret_cast<const InExpression&>(*this);
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
    case TypeTag::kUnaryExp: {
      const UnaryExpression& ue = AsUnaryExpression();
      ret.merge(ue.Child()->TouchedColumns());
      break;
    }
    case TypeTag::kAggregateExp: {
      const AggregateExpression& ae = AsAggregateExpression();
      ret.merge(ae.Child()->TouchedColumns());
      break;
    }
    case TypeTag::kCaseExp: {
      const CaseExpression& ce = AsCaseExpression();
      for (const auto& when : ce.when_clauses_) {
        ret.merge(when.first->TouchedColumns());
        ret.merge(when.second->TouchedColumns());
      }
      if (ce.else_clause_) {
        ret.merge(ce.else_clause_->TouchedColumns());
      }
      break;
    }
    case TypeTag::kInExp: {
      const InExpression& ie = AsInExpression();
      ret.merge(ie.child_->TouchedColumns());
      for (const auto& item : ie.list_) {
        ret.merge(item->TouchedColumns());
      }
      break;
    }
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

Expression UnaryExpressionExp(Expression child, UnaryOperation op) {
  return std::make_shared<UnaryExpression>(std::move(child), op);
}

Expression AggregateExpressionExp(AggregationType type, Expression child) {
  return std::make_shared<AggregateExpression>(type, std::move(child));
}

Expression CaseExpressionExp(
    std::vector<std::pair<Expression, Expression>> when_clauses,
    Expression else_clause) {
  return std::make_shared<CaseExpression>(std::move(when_clauses),
                                           std::move(else_clause));
}

Expression InExpressionExp(Expression child, std::vector<Expression> list) {
  return std::make_shared<InExpression>(std::move(child), std::move(list));
}
}  // namespace tinylamb
