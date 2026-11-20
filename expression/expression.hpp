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
class WindowFunctionCallExpression;
class LambdaExpression;
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
  // Issue 1314: the expression NODE KIND (kBinaryExp / kColumnValue / ...),
  // not the SQL data type of the value this expression produces -- that is
  // ResultType()'s job (it returns a tinylamb::Type).  TypeTag deliberately
  // encodes both domains in one enum; see the note in type/type.hpp.
  [[nodiscard]] virtual TypeTag NodeKind() const = 0;
  // Deprecated alias for NodeKind() kept because the dispatch idiom
  // `expr->Type()` is used engine-wide; new code should spell NodeKind()
  // (or ResultType() when the DATA type is wanted).
  [[nodiscard]] TypeTag Type() const { return NodeKind(); }
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
  [[nodiscard]] const WindowFunctionCallExpression&
  AsWindowFunctionCallExpression() const;
  [[nodiscard]] const LambdaExpression& AsLambdaExpression() const;

  [[nodiscard]] virtual std::unordered_set<ColumnName> TouchedColumns() const;
  [[nodiscard]] virtual Value Evaluate(const Row& row,
                                       const Schema& schema) const = 0;
  [[nodiscard]] virtual Value Evaluate(const Row* /*unused*/,
                                       const Schema& /*unused*/,
                                       const Row* /*unused*/,
                                       const Schema& /*unused*/) const {
    throw std::runtime_error("not implemented");
  }
  // Context-aware evaluation: subqueries and aggregates
  // are resolved through the abstract EvaluationContext instead of database
  // types.  The default keeps unmigrated node types working by ignoring the
  // context; nodes override it in the recommended migration order
  // QueryExpression -> Binary/Case/In -> FunctionCall, so nested subqueries
  // route through the abstract interface as soon as every ancestor on their
  // path propagates it.
  [[nodiscard]] virtual Value Evaluate(const Row& row, const Schema& schema,
                                       EvaluationContext& /*unused*/) const {
    return Evaluate(row, schema);
  }

  // No-exception evaluation contract: nodes override TryEvaluate and route
  // their Evaluate through the EXC-SHIM wrapper; the default here forwards to
  // the still-throwing Evaluate. TryEvaluate is the interface callers should
  // use for Status-based error handling.
  [[nodiscard]] virtual StatusOr<Value> TryEvaluate(
      const Row& row, const Schema& schema) const {
    return Evaluate(row, schema);
  }
  [[nodiscard]] virtual StatusOr<Value> TryEvaluate(
      const Row* left, const Schema& left_schema, const Row* right,
      const Schema& right_schema) const {
    return Evaluate(left, left_schema, right, right_schema);
  }
  [[nodiscard]] virtual StatusOr<Value> TryEvaluate(
      const Row& row, const Schema& schema, EvaluationContext& context) const {
    return Evaluate(row, schema, context);
  }
  [[nodiscard]] virtual tinylamb::Type ResultType(
      const Schema& /*unused*/) const {
    throw std::runtime_error("not implemented");
  }
  [[nodiscard]] virtual tinylamb::Type ResultType(
      const Schema& /*unused*/, const Schema& /*unused*/) const {
    throw std::runtime_error("not implemented");
  }
  // Validates the expression against the schema, resolving function
  // signatures through EvaluationContext (the database
  // type stays behind the context boundary).
  virtual Status Validate(EvaluationContext& /*unused*/,
                          const Schema& /*unused*/) const {
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
