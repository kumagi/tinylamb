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

#include "expression/unary_expression.hpp"

#include <cstdint>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

StatusOr<Value> TryEvaluateUnary(UnaryOperation operation, const Value& child) {
  switch (operation) {
    case UnaryOperation::kIsNull:
      return Value(child.IsNull());
    case UnaryOperation::kIsNotNull:
      return Value(!child.IsNull());
    case UnaryOperation::kIsTrue:
    case UnaryOperation::kIsNotTrue:
    case UnaryOperation::kIsFalse:
    case UnaryOperation::kIsNotFalse: {
      // T3698: IS [NOT] TRUE/FALSE is a three-valued-logic predicate over
      // BOOL. The engine carries BOOL as INT64 0/1 (no separate ValueType),
      // so kInt64 stays accepted -- but VARCHAR/DOUBLE/DATE/ARRAY can never
      // be BOOL and used to pass through Truthy() (`'abc' IS FALSE` quietly
      // evaluated FALSE instead of failing like GoogleSQL's signature check).
      const bool non_bool =
          !child.IsNull() &&
          (child.type != ValueType::kInt64 || child.value.int_value < 0 ||
           child.value.int_value > 1);
      if (non_bool) {
        return StatusError(
            StatusCode::kInvalidArgument,
            std::string(operation == UnaryOperation::kIsTrue ||
                                operation == UnaryOperation::kIsNotTrue
                            ? "IS TRUE"
                            : "IS FALSE") +
                " requires a BOOL operand");
      }
      // Three-valued logic: the bare forms are FALSE on NULL, the negated
      // forms TRUE on NULL; over a non-NULL operand IS TRUE / IS NOT FALSE
      // follow Truthy(), IS FALSE / IS NOT TRUE its negation.
      if (child.IsNull()) {
        const bool negated = operation == UnaryOperation::kIsNotTrue ||
                             operation == UnaryOperation::kIsNotFalse;
        return Value(negated);
      }
      const bool truthy_following = operation == UnaryOperation::kIsTrue ||
                                    operation == UnaryOperation::kIsNotFalse;
      return Value(truthy_following ? child.Truthy() : !child.Truthy());
    }
    case UnaryOperation::kNot:
      return child.IsNull() ? Value() : Value(!child.Truthy());
    case UnaryOperation::kMinus:
      if (child.IsNull()) {
        return Value();
      }
      if (child.type == ValueType::kDouble) {
        return Value(-child.value.double_value);
      }
      if (child.type == ValueType::kInt64) {
        if (child.IsUnsigned()) {
          const auto u = static_cast<uint64_t>(child.value.int_value);
          if (u == 0) {
            // UnaryResultType declares kBigInt for this operand, so materialize
            // a signed zero instead of leaking the unsigned tag of `child`
            // (which broke JSON rendering and UNION type reconciliation).
            return Value(int64_t{0});
          }
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in unary minus");
        }
        if (child.value.int_value == std::numeric_limits<int64_t>::min()) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in unary minus");
        }
        return Value(-child.value.int_value);
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "unary minus requires a number");
    case UnaryOperation::kBitwiseNot:
      if (child.IsNull()) {
        return Value();
      }
      if (child.type == ValueType::kInt64) {
        Value res(static_cast<int64_t>(
            ~static_cast<uint64_t>(child.value.int_value)));
        return child.IsUnsigned() ? res.WithUnsigned() : res;
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "bitwise not requires an integer");
  }
  return StatusError(StatusCode::kRuntimeError, "invalid unary operation");
}

// EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp); bytecode.cpp
// switches to TryEvaluateUnary in the executor conversion.
Value EvaluateUnary(UnaryOperation operation, const Value& child) {
  return ExcShimUnwrap(TryEvaluateUnary(operation, child), "EvaluateUnary");
}

namespace {

Type UnaryResultType(UnaryOperation operation, const Type& child) {
  if (operation == UnaryOperation::kIsNull ||
      operation == UnaryOperation::kIsNotNull ||
      operation == UnaryOperation::kIsTrue ||
      operation == UnaryOperation::kIsNotTrue ||
      operation == UnaryOperation::kIsFalse ||
      operation == UnaryOperation::kIsNotFalse ||
      operation == UnaryOperation::kNot) {
    return {TypeTag::kBigInt};
  }
  if (operation == UnaryOperation::kMinus &&
      child.GetType() != TypeTag::kBigInt &&
      child.GetType() != TypeTag::kDouble) {
    return {TypeTag::kInvalid};
  }
  if (operation == UnaryOperation::kBitwiseNot &&
      child.GetType() != TypeTag::kBigInt) {
    return {TypeTag::kInvalid};
  }
  return child;
}

}  // namespace

std::unordered_set<ColumnName> UnaryExpression::TouchedColumns() const {
  return child_->TouchedColumns();
}

StatusOr<Value> UnaryExpression::TryEvaluate(const Row& row,
                                             const Schema& schema) const {
  ASSIGN_OR_RETURN(Value, child, child_->TryEvaluate(row, schema));
  return TryEvaluateUnary(operation_, child);
}

Value UnaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema), "UnaryExpression::Evaluate");
}

StatusOr<Value> UnaryExpression::TryEvaluate(const Row* left,
                                             const Schema& left_schema,
                                             const Row* right,
                                             const Schema& right_schema) const {
  ASSIGN_OR_RETURN(Value, child,
                   child_->TryEvaluate(left, left_schema, right, right_schema));
  return TryEvaluateUnary(operation_, child);
}

Value UnaryExpression::Evaluate(const Row* left, const Schema& left_schema,
                                const Row* right,
                                const Schema& right_schema) const {
  return ExcShimUnwrap(TryEvaluate(left, left_schema, right, right_schema),
                       "UnaryExpression::Evaluate");
}

// Context-aware form: same semantics as the plain evaluator with the
// context threaded into the child (A1 stage 2).  Without this override the
// base default drops the context into the 2-arg evaluator, so a subquery
// under NOT / IS NULL reports "query expression requires an evaluation
// context" even at a context root.
StatusOr<Value> UnaryExpression::TryEvaluate(const Row& row,
                                             const Schema& schema,
                                             EvaluationContext& context) const {
  ASSIGN_OR_RETURN(Value, child, child_->TryEvaluate(row, schema, context));
  return TryEvaluateUnary(operation_, child);
}

Value UnaryExpression::Evaluate(const Row& row, const Schema& schema,
                                EvaluationContext& context) const {
  return ExcShimUnwrap(TryEvaluate(row, schema, context),
                       "UnaryExpression::Evaluate");
}

Type UnaryExpression::ResultType(const Schema& schema) const {
  return UnaryResultType(operation_, child_->ResultType(schema));
}

Type UnaryExpression::ResultType(const Schema& left,
                                 const Schema& right) const {
  return UnaryResultType(operation_, child_->ResultType(left, right));
}

std::string UnaryExpression::ToString() const {
  if (operation_ == UnaryOperation::kMinus ||
      operation_ == UnaryOperation::kBitwiseNot) {
    return "(" + ::tinylamb::ToString(operation_) + child_->ToString() + ")";
  }
  return "(" + ::tinylamb::ToString(operation_) + " " + child_->ToString() +
         ")";
}

void UnaryExpression::Dump(std::ostream& o) const {
  if (operation_ == UnaryOperation::kMinus ||
      operation_ == UnaryOperation::kBitwiseNot) {
    o << "(" << ::tinylamb::ToString(operation_) << *child_ << ")";
  } else {
    o << "(" << ::tinylamb::ToString(operation_) << " " << *child_ << ")";
  }
}

}  // namespace tinylamb
