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

#include "binary_expression.hpp"

#include <cmath>
#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <string_view>
#include <string>
#include <unordered_set>

#include "type/column_name.hpp"
#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"
#include "type/type.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> BinaryExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result = left_->TouchedColumns();
  result.merge(right_->TouchedColumns());
  return result;
}
namespace {

bool Like(std::string_view value, std::string_view pattern) {
  size_t value_pos = 0;
  size_t pattern_pos = 0;
  size_t wildcard = std::string_view::npos;
  size_t retry = 0;
  while (value_pos < value.size()) {
    if (pattern_pos < pattern.size() &&
        (pattern[pattern_pos] == '_' ||
         pattern[pattern_pos] == value[value_pos])) {
      ++value_pos;
      ++pattern_pos;
    } else if (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
      wildcard = pattern_pos++;
      retry = value_pos;
    } else if (wildcard != std::string_view::npos) {
      pattern_pos = wildcard + 1;
      value_pos = ++retry;
    } else {
      return false;
    }
  }
  while (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
    ++pattern_pos;
  }
  return pattern_pos == pattern.size();
}

}  // namespace

Value EvaluateBinary(BinaryOperation op, const Value& left,
                     const Value& right) {
  if (op == BinaryOperation::kAnd) {
    if ((!left.IsNull() && !left.Truthy()) ||
        (!right.IsNull() && !right.Truthy())) {
      return Value(false);
    }
    if (left.IsNull() || right.IsNull()) { return {};
}
    return Value(true);
  }
  if (op == BinaryOperation::kOr) {
    if ((!left.IsNull() && left.Truthy()) ||
        (!right.IsNull() && right.Truthy())) {
      return Value(true);
    }
    if (left.IsNull() || right.IsNull()) { return {};
}
    return Value(false);
  }
  if (op == BinaryOperation::kXor) {
    if (left.IsNull() || right.IsNull()) { return {};
}
    return Value(left.Truthy() != right.Truthy());
  }
  if (left.IsNull() || right.IsNull()) { return {};
}
  if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
    if (left.type != ValueType::kVarChar || right.type != ValueType::kVarChar) {
      throw std::runtime_error("LIKE requires string operands");
    }
    const bool matched =
        Like(left.value.varchar_value, right.value.varchar_value);
    return Value(op == BinaryOperation::kLike ? matched : !matched);
  }
  const bool numeric =
      (left.type == ValueType::kInt64 || left.type == ValueType::kDouble) &&
      (right.type == ValueType::kInt64 || right.type == ValueType::kDouble);
  if (numeric && left.type != right.type) {
    const double lhs = left.type == ValueType::kDouble
                           ? left.value.double_value
                           : static_cast<double>(left.value.int_value);
    const double rhs = right.type == ValueType::kDouble
                           ? right.value.double_value
                           : static_cast<double>(right.value.int_value);
    switch (op) {
      case BinaryOperation::kAdd:
        return Value(lhs + rhs);
      case BinaryOperation::kSubtract:
        return Value(lhs - rhs);
      case BinaryOperation::kMultiply:
        return Value(lhs * rhs);
      case BinaryOperation::kDivide:
        return Value(lhs / rhs);
      case BinaryOperation::kModulo:
        return Value(std::fmod(lhs, rhs));
      case BinaryOperation::kEquals:
        return Value(lhs == rhs);
      case BinaryOperation::kNotEquals:
        return Value(lhs != rhs);
      case BinaryOperation::kLessThan:
        return Value(lhs < rhs);
      case BinaryOperation::kLessThanEquals:
        return Value(lhs <= rhs);
      case BinaryOperation::kGreaterThan:
        return Value(lhs > rhs);
      case BinaryOperation::kGreaterThanEquals:
        return Value(lhs >= rhs);
      default:
        break;
    }
  }
  if (left.type != right.type) {
    throw std::runtime_error("type mismatch");
  }
  switch (op) {
    case BinaryOperation::kAdd:
      return left + right;
    case BinaryOperation::kSubtract:
      return left - right;
    case BinaryOperation::kMultiply:
      return left * right;
    case BinaryOperation::kDivide:
      return left / right;
    case BinaryOperation::kModulo:
      return left % right;
    case BinaryOperation::kEquals:
      return Value(left == right);
    case BinaryOperation::kNotEquals:
      return Value(left != right);
    case BinaryOperation::kLessThan:
      return Value(left < right);
    case BinaryOperation::kLessThanEquals:
      return Value(left <= right);
    case BinaryOperation::kGreaterThan:
      return Value(left > right);
    case BinaryOperation::kGreaterThanEquals:
      return Value(left >= right);
    case BinaryOperation::kAnd:
    case BinaryOperation::kOr:
    case BinaryOperation::kXor:
    case BinaryOperation::kLike:
    case BinaryOperation::kNotLike:
      // Already handled above; kept for -Wswitch completeness.
      break;
  }
  throw std::logic_error("invalid binary operation");
}

Value BinaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  // AND/OR are short-circuited: the right child must not be evaluated when
  // the left operand already decides the result (three-valued logic).
  if (op_ == BinaryOperation::kAnd || op_ == BinaryOperation::kOr) {
    const Value left = left_->Evaluate(row, schema);
    if (!left.IsNull() && left.Truthy() != (op_ == BinaryOperation::kAnd)) {
      return Value(op_ == BinaryOperation::kOr);
    }
    return EvaluateBinary(op_, left, right_->Evaluate(row, schema));
  }
  return EvaluateBinary(op_, left_->Evaluate(row, schema),
                        right_->Evaluate(row, schema));
}

Value BinaryExpression::Evaluate(const Row* left, const Schema& left_schema,
                                 const Row* right,
                                 const Schema& right_schema) const {
  if (op_ == BinaryOperation::kAnd || op_ == BinaryOperation::kOr) {
    const Value left_value =
        left_->Evaluate(left, left_schema, right, right_schema);
    if (!left_value.IsNull() &&
        left_value.Truthy() != (op_ == BinaryOperation::kAnd)) {
      return Value(op_ == BinaryOperation::kOr);
    }
    return EvaluateBinary(
        op_, left_value,
        right_->Evaluate(left, left_schema, right, right_schema));
  }
  return EvaluateBinary(
      op_, left_->Evaluate(left, left_schema, right, right_schema),
      right_->Evaluate(left, left_schema, right, right_schema));
}

// Context-aware form: identical dispatch to the plain evaluator, with the
// context threaded into both children (A1 stage 2).
Value BinaryExpression::Evaluate(const Row& row, const Schema& schema,
                                 EvaluationContext& context) const {
  if (op_ == BinaryOperation::kAnd || op_ == BinaryOperation::kOr) {
    const Value left_value = left_->Evaluate(row, schema, context);
    if (!left_value.IsNull() &&
        left_value.Truthy() != (op_ == BinaryOperation::kAnd)) {
      return Value(op_ == BinaryOperation::kOr);
    }
    return EvaluateBinary(
        op_, left_value, right_->Evaluate(row, schema, context));
  }
  return EvaluateBinary(op_, left_->Evaluate(row, schema, context),
                        right_->Evaluate(row, schema, context));
}

namespace {
Type BinaryResultType(BinaryOperation operation, const Type& left,
                      const Type& right) {
  if (IsComparison(operation) || operation == BinaryOperation::kAnd ||
      operation == BinaryOperation::kOr || operation == BinaryOperation::kXor ||
      operation == BinaryOperation::kLike ||
      operation == BinaryOperation::kNotLike) {
    return {TypeTag::kBigInt};
  }
  if (left.GetType() == TypeTag::kDouble ||
      right.GetType() == TypeTag::kDouble) {
    return {TypeTag::kDouble};
  }
  if (operation == BinaryOperation::kAdd &&
      left.GetType() == TypeTag::kVarChar &&
      right.GetType() == TypeTag::kVarChar) {
    return {TypeTag::kVarChar};
  }
  return {TypeTag::kBigInt};
}
}  // namespace

Type BinaryExpression::ResultType(const Schema& schema) const {
  return BinaryResultType(op_, left_->ResultType(schema),
                          right_->ResultType(schema));
}

Type BinaryExpression::ResultType(const Schema& left,
                                  const Schema& right) const {
  return BinaryResultType(op_, left_->ResultType(left, right),
                          right_->ResultType(left, right));
}

std::string BinaryExpression::ToString() const {
  return "(" + left_->ToString() + " " +
         std::string(tinylamb::ToString(op_)) + " " + right_->ToString() + ")";
}

void BinaryExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
