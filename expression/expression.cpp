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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>
#include <utility>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {

const ColumnValue& ExpressionBase::AsColumnValue() const {
  return dynamic_cast<const ColumnValue&>(*this);
}

ColumnValue& ExpressionBase::AsColumnValue() {
  return dynamic_cast<ColumnValue&>(*this);
}

const BinaryExpression& ExpressionBase::AsBinaryExpression() const {
  return dynamic_cast<const BinaryExpression&>(*this);
}

const ConstantValue& ExpressionBase::AsConstantValue() const {
  return dynamic_cast<const ConstantValue&>(*this);
}

const UnaryExpression& ExpressionBase::AsUnaryExpression() const {
  return dynamic_cast<const UnaryExpression&>(*this);
}

const AggregateExpression& ExpressionBase::AsAggregateExpression() const {
  return dynamic_cast<const AggregateExpression&>(*this);
}

const CaseExpression& ExpressionBase::AsCaseExpression() const {
  return dynamic_cast<const CaseExpression&>(*this);
}

const InExpression& ExpressionBase::AsInExpression() const {
  return dynamic_cast<const InExpression&>(*this);
}

const FunctionCallExpression& ExpressionBase::AsFunctionCallExpression() const {
  return dynamic_cast<const FunctionCallExpression&>(*this);
}

const QueryExpression& ExpressionBase::AsQueryExpression() const {
  return dynamic_cast<const QueryExpression&>(*this);
}

const IntervalExpression& ExpressionBase::AsIntervalExpression() const {
  return dynamic_cast<const IntervalExpression&>(*this);
}

std::unordered_set<ColumnName> ExpressionBase::TouchedColumns() const {
  std::unordered_set<ColumnName> result;
  return result;
}

Expression ColumnValueExp(const ColumnName& col_name) {
  return std::make_shared<ColumnValue>(col_name);
}

Expression ColumnValueExp(std::string_view col_name) {
  return std::make_shared<ColumnValue>(ColumnName(std::string(col_name)));
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

Expression AggregateExpressionExp(AggregationType type, Expression child,
                                  bool distinct) {
  return std::make_shared<AggregateExpression>(type, std::move(child),
                                               distinct);
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

Expression FunctionCallExp(std::string func_name, std::vector<Expression> args) {
  return std::make_shared<FunctionCallExpression>(std::move(func_name),
                                                  std::move(args));
}

Expression QueryExpressionExp(std::shared_ptr<SelectStatement> query,
                              Expression test, bool exists, bool negated) {
  return std::make_shared<QueryExpression>(std::move(query), std::move(test),
                                           exists, negated);
}

Expression IntervalExpressionExp(int64_t amount, std::string unit) {
  return std::make_shared<IntervalExpression>(amount, std::move(unit));
}

}  // namespace tinylamb
