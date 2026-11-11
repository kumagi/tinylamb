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
#include <typeinfo>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/lambda_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
template <typename Target, typename Source>
Target& CheckedExpressionCast(Source& expression) {
  // Most AST nodes have exactly the requested concrete type. Pointer identity
  // proves that case without walking the RTTI inheritance graph. Extensions,
  // derived constants (ParameterSlot), and mismatches keep dynamic_cast's
  // original behavior, including std::bad_cast on an incompatible node.
  if (&typeid(expression) == &typeid(Target)) {
    return static_cast<Target&>(expression);
  }
  return dynamic_cast<Target&>(expression);
}
}  // namespace

const ColumnValue& ExpressionBase::AsColumnValue() const {
  return CheckedExpressionCast<const ColumnValue>(*this);
}

ColumnValue& ExpressionBase::AsColumnValue() {
  return CheckedExpressionCast<ColumnValue>(*this);
}

const BinaryExpression& ExpressionBase::AsBinaryExpression() const {
  return CheckedExpressionCast<const BinaryExpression>(*this);
}

const ConstantValue& ExpressionBase::AsConstantValue() const {
  return CheckedExpressionCast<const ConstantValue>(*this);
}

const UnaryExpression& ExpressionBase::AsUnaryExpression() const {
  return CheckedExpressionCast<const UnaryExpression>(*this);
}

const AggregateExpression& ExpressionBase::AsAggregateExpression() const {
  return CheckedExpressionCast<const AggregateExpression>(*this);
}

const CaseExpression& ExpressionBase::AsCaseExpression() const {
  return CheckedExpressionCast<const CaseExpression>(*this);
}

const InExpression& ExpressionBase::AsInExpression() const {
  return CheckedExpressionCast<const InExpression>(*this);
}

const FunctionCallExpression& ExpressionBase::AsFunctionCallExpression() const {
  return CheckedExpressionCast<const FunctionCallExpression>(*this);
}

const QueryExpression& ExpressionBase::AsQueryExpression() const {
  return CheckedExpressionCast<const QueryExpression>(*this);
}

const IntervalExpression& ExpressionBase::AsIntervalExpression() const {
  return CheckedExpressionCast<const IntervalExpression>(*this);
}

const ArrayExpression& ExpressionBase::AsArrayExpression() const {
  return CheckedExpressionCast<const ArrayExpression>(*this);
}

const CastExpression& ExpressionBase::AsCastExpression() const {
  return CheckedExpressionCast<const CastExpression>(*this);
}

const WindowFunctionCallExpression&
ExpressionBase::AsWindowFunctionCallExpression() const {
  return CheckedExpressionCast<const WindowFunctionCallExpression>(*this);
}

const LambdaExpression& ExpressionBase::AsLambdaExpression() const {
  return CheckedExpressionCast<const LambdaExpression>(*this);
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

Expression FunctionCallExp(std::string func_name,
                           std::vector<Expression> args) {
  return std::make_shared<FunctionCallExpression>(std::move(func_name),
                                                  std::move(args));
}

Expression QueryExpressionExp(std::shared_ptr<SelectStatement> query,
                              Expression test, bool exists, bool negated,
                              BinaryOperation op, QuantifierMode mode) {
  return std::make_shared<QueryExpression>(std::move(query), std::move(test),
                                           exists, negated, op, mode);
}

Expression IntervalExpressionExp(int64_t amount, std::string unit,
                                 std::string raw_amount) {
  return std::make_shared<IntervalExpression>(amount, std::move(unit),
                                              std::move(raw_amount));
}

Expression ArrayExpressionExp(std::vector<Expression> elements,
                              std::string element_sql_type) {
  return std::make_shared<ArrayExpression>(std::move(elements),
                                           std::move(element_sql_type));
}

Expression CastExpressionExp(Expression child, std::string target_type_name,
                             bool return_null_on_error) {
  return std::make_shared<CastExpression>(
      std::move(child), std::move(target_type_name), return_null_on_error);
}

}  // namespace tinylamb
