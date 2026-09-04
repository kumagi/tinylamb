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
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
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
class QueryExpression;
class IntervalExpression;
class ArrayExpression;
class CastExpression;
class SelectStatement;
class EvaluationContext;

// How a QueryExpression combines projected subquery rows against its test
// value: plain membership (IN), three-valued ANY/SOME, or ALL.
enum class QuantifierMode : uint8_t {
  kIn,
  kAny,
  kAll,
};

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
  [[nodiscard]] const QueryExpression& AsQueryExpression() const;
  [[nodiscard]] const IntervalExpression& AsIntervalExpression() const;
  [[nodiscard]] const ArrayExpression& AsArrayExpression() const;
  [[nodiscard]] const CastExpression& AsCastExpression() const;

  [[nodiscard]] virtual std::unordered_set<ColumnName> TouchedColumns() const;
  [[nodiscard]] virtual Value Evaluate(const Row& row,
                                       const Schema& schema) const = 0;
  [[nodiscard]] virtual Value Evaluate(const Row*, const Schema&, const Row*,
                                       const Schema&) const {
    throw std::runtime_error("not implemented");
  }
  // Context-aware evaluation (improvement3.md A1): subqueries and aggregates
  // are resolved through the abstract EvaluationContext instead of database
  // types.  The default keeps unmigrated node types working by ignoring the
  // context; nodes override it in the recommended migration order
  // QueryExpression -> Binary/Case/In -> FunctionCall, so nested subqueries
  // route through the abstract interface as soon as every ancestor on their
  // path propagates it.
  [[nodiscard]] virtual Value Evaluate(const Row& row, const Schema& schema,
                                       EvaluationContext&) const {
    return Evaluate(row, schema);
  }
  [[nodiscard]] virtual tinylamb::Type ResultType(const Schema&) const {
    throw std::runtime_error("not implemented");
  }
  [[nodiscard]] virtual tinylamb::Type ResultType(const Schema&,
                                                  const Schema&) const {
    throw std::runtime_error("not implemented");
  }
  // Validates the expression against the schema, resolving function
  // signatures through EvaluationContext (improvement3.md A1: the database
  // type stays behind the context boundary).
  virtual Status Validate(EvaluationContext&, const Schema&) const {
    return Status::kSuccess;
  }
  [[nodiscard]] virtual std::string ToString() const = 0;
  virtual void Dump(std::ostream& o) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const ExpressionBase& e) {
    e.Dump(o);
    return o;
  }
};

using Expression = std::shared_ptr<ExpressionBase>;
Expression ColumnValueExp(const ColumnName& col_name);
Expression ColumnValueExp(std::string_view col_name);
Expression ConstantValueExp(const Value& v);
Expression BinaryExpressionExp(Expression left, BinaryOperation op,
                               Expression right);
Expression UnaryExpressionExp(Expression child, UnaryOperation op);
Expression AggregateExpressionExp(AggregationType type, Expression child,
                                  bool distinct = false);
Expression CaseExpressionExp(
    std::vector<std::pair<Expression, Expression>> when_clauses,
    Expression else_clause);
Expression InExpressionExp(Expression child, std::vector<Expression> list);
Expression FunctionCallExp(std::string func_name, std::vector<Expression> args);
Expression QueryExpressionExp(std::shared_ptr<SelectStatement> query,
                              Expression test = nullptr, bool exists = false,
                              bool negated = false,
                              BinaryOperation op = BinaryOperation::kEquals,
                              QuantifierMode mode = QuantifierMode::kIn);
Expression IntervalExpressionExp(int64_t amount, std::string unit,
                                 std::string raw_amount = "");
Expression ArrayExpressionExp(std::vector<Expression> elements,
                              std::string element_sql_type);
Expression CastExpressionExp(Expression child, std::string target_type_name,
                             bool return_null_on_error = false);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_HPP
