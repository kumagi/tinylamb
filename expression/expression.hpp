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

#ifndef TINYLAMB_EXPRESSION_HPP
#define TINYLAMB_EXPRESSION_HPP

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Schema;
struct Row;
class ColumnValue;
class BinaryExpression;
class ConstantValue;
class UnaryExpression;
class AggregateExpression;
class CaseExpression;
class InExpression;
class FunctionCallExpression;

class ExpressionBase {
 public:
  ExpressionBase() = default;
  virtual ~ExpressionBase() = default;
  ExpressionBase(const ExpressionBase&) = delete;
  ExpressionBase(ExpressionBase&&) = delete;
  ExpressionBase& operator=(const ExpressionBase&) = delete;
  ExpressionBase& operator=(ExpressionBase&&) = delete;
  [[nodiscard]] virtual TypeTag Type() const = 0;
  [[nodiscard]] const ColumnValue& AsColumnValue() const;
  [[nodiscard]] ColumnValue& AsColumnValue();
  [[nodiscard]] const BinaryExpression& AsBinaryExpression() const;
  [[nodiscard]] const ConstantValue& AsConstantValue() const;
  [[nodiscard]] const UnaryExpression& AsUnaryExpression() const;
  [[nodiscard]] const AggregateExpression& AsAggregateExpression() const;
  [[nodiscard]] const CaseExpression& AsCaseExpression() const;
  [[nodiscard]] const InExpression& AsInExpression() const;
  [[nodiscard]] const FunctionCallExpression& AsFunctionCallExpression() const;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const;
  [[nodiscard]] virtual Value Evaluate(const Row& row,
                                       const Schema& schema) const = 0;
  [[nodiscard]] virtual Value Evaluate(const Row* left,
                                       const Schema& left_schema,
                                       const Row* right,
                                       const Schema& right_schema) const {
    throw std::runtime_error("not implemented");
  };
  [[nodiscard]] virtual tinylamb::Type ResultType(const Schema& schema) const {
    throw std::runtime_error("not implemented");
  }
  [[nodiscard]] virtual tinylamb::Type ResultType(const Schema& left,
                                                  const Schema& right) const {
    throw std::runtime_error("not implemented");
  }
  virtual Status Validate(TransactionContext& ctx, const Schema& schema) const {
    return Status::kSuccess;
  }
  [[nodiscard]] virtual std::string ToString() const = 0;
  virtual void Dump(std::ostream& o) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const ExpressionBase& e) {
    e.Dump(o);
    return o;
  }
};

typedef std::shared_ptr<ExpressionBase> Expression;
Expression ColumnValueExp(const ColumnName& col_name);
Expression ColumnValueExp(const std::string_view& col_name);
Expression ConstantValueExp(const Value& v);
Expression BinaryExpressionExp(Expression left, BinaryOperation op,
                               Expression right);
Expression UnaryExpressionExp(Expression child, UnaryOperation op);
Expression AggregateExpressionExp(AggregationType type, Expression child);
Expression CaseExpressionExp(
    std::vector<std::pair<Expression, Expression>> when_clauses,
    Expression else_clause);
Expression InExpressionExp(Expression child, std::vector<Expression> list);
Expression FunctionCallExp(const std::string& func_name,
                           std::vector<Expression> args);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_HPP
