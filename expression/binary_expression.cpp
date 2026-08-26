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
#include <string>
#include <string_view>
#include <unordered_set>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> BinaryExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result = left_->TouchedColumns();
  result.merge(right_->TouchedColumns());
  return result;
}
namespace {

bool IsStructJson(std::string_view s) {
  return s.size() >= 2 && s.front() == '{' && s.back() == '}';
}

std::vector<std::string> ExtractStructValues(std::string_view json) {
  std::vector<std::string> values;
  if (!IsStructJson(json)) {
    return values;
  }
  std::string_view inner = json.substr(1, json.size() - 2);
  int depth = 0;
  bool in_string = false;
  size_t start = 0;
  std::vector<std::string_view> parts;
  for (size_t i = 0; i < inner.size(); ++i) {
    char c = inner[i];
    if (in_string) {
      if (c == '\\' && i + 1 < inner.size()) {
        ++i;
        continue;
      }
      if (c == '"') {
        in_string = false;
      }
      continue;
    }
    if (c == '"') {
      in_string = true;
      continue;
    }
    if (c == '{' || c == '[' || c == '(') {
      ++depth;
      continue;
    }
    if (c == '}' || c == ']' || c == ')') {
      if (depth > 0) --depth;
      continue;
    }
    if (c == ',' && depth == 0) {
      parts.push_back(inner.substr(start, i - start));
      start = i + 1;
    }
  }
  if (start < inner.size()) {
    parts.push_back(inner.substr(start));
  }
  for (auto p : parts) {
    while (!p.empty() && std::isspace(static_cast<unsigned char>(p.front()))) {
      p.remove_prefix(1);
    }
    while (!p.empty() && std::isspace(static_cast<unsigned char>(p.back()))) {
      p.remove_suffix(1);
    }
    size_t colon = p.find(':');
    if (colon != std::string_view::npos) {
      p = p.substr(colon + 1);
      while (!p.empty() &&
             std::isspace(static_cast<unsigned char>(p.front()))) {
        p.remove_prefix(1);
      }
    }
    values.emplace_back(p);
  }
  return values;
}

bool StructJsonEqual(std::string_view lhs, std::string_view rhs) {
  if (lhs == rhs) {
    return true;
  }
  auto v1 = ExtractStructValues(lhs);
  auto v2 = ExtractStructValues(rhs);
  if (v1.empty() || v1.size() != v2.size()) {
    return false;
  }
  for (size_t i = 0; i < v1.size(); ++i) {
    if (v1[i] != v2[i]) {
      return false;
    }
  }
  return true;
}

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
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    return Value(true);
  }
  if (op == BinaryOperation::kOr) {
    if ((!left.IsNull() && left.Truthy()) ||
        (!right.IsNull() && right.Truthy())) {
      return Value(true);
    }
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    return Value(false);
  }
  if (op == BinaryOperation::kXor) {
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    return Value(left.Truthy() != right.Truthy());
  }
  if (left.IsNull() || right.IsNull()) {
    return {};
  }
  if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
    if (left.type != ValueType::kVarChar || right.type != ValueType::kVarChar) {
      throw std::runtime_error("type mismatch");
    }
    const bool matched =
        Like(left.value.varchar_value, right.value.varchar_value);
    return Value(op == BinaryOperation::kLike ? matched : !matched);
  }
  const bool numeric =
      (left.type == ValueType::kInt64 || left.type == ValueType::kDouble) &&
      (right.type == ValueType::kInt64 || right.type == ValueType::kDouble);
  if (op == BinaryOperation::kDivide && numeric) {
    const double lhs = left.type == ValueType::kDouble
                           ? left.value.double_value
                           : static_cast<double>(left.value.int_value);
    const double rhs = right.type == ValueType::kDouble
                           ? right.value.double_value
                           : static_cast<double>(right.value.int_value);
    if (rhs == 0.0) {
      throw std::runtime_error("division by zero");
    }
    const double res = lhs / rhs;
    if (std::isinf(res)) {
      throw std::runtime_error("double overflow on '/'");
    }
    if (std::isnan(res)) {
      throw std::runtime_error("division by zero");
    }
    return Value(res);
  }
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
        if (rhs == 0.0) {
          throw std::runtime_error("division by zero");
        }
        return Value(lhs / rhs);
      case BinaryOperation::kModulo:
        if (rhs == 0.0) {
          throw std::runtime_error("division by zero");
        }
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
    auto is_iv = [](std::string_view s) {
      return s.find('-') != std::string_view::npos &&
             s.find(' ') != std::string_view::npos && s.find('-') < s.find(' ');
    };
    if (left.type == ValueType::kVarChar && right.type == ValueType::kInt64 &&
        is_iv(left.value.varchar_value) && op == BinaryOperation::kMultiply) {
      IntervalValue iv = IntervalValue::Parse(left.value.varchar_value);
      return Value((iv * right.value.int_value).ToString());
    }
    if (left.type == ValueType::kInt64 && right.type == ValueType::kVarChar &&
        is_iv(right.value.varchar_value) && op == BinaryOperation::kMultiply) {
      IntervalValue iv = IntervalValue::Parse(right.value.varchar_value);
      return Value((iv * left.value.int_value).ToString());
    }
    if (left.type == ValueType::kDate && right.type == ValueType::kVarChar) {
      try {
        return EvaluateBinary(
            op, left,
            Value::DateFromDays(ParseDateDays(right.value.varchar_value)));
      } catch (...) {
      }
      // Tolerate datetime/timestamp literals: compare on the date portion.
      std::string_view sv = right.value.varchar_value;
      const size_t space = sv.find(' ');
      if (space != std::string_view::npos) {
        try {
          return EvaluateBinary(
              op, left,
              Value::DateFromDays(ParseDateDays(sv.substr(0, space))));
        } catch (...) {
        }
      }
    } else if (left.type == ValueType::kVarChar &&
               right.type == ValueType::kDate) {
      try {
        return EvaluateBinary(
            op, Value::DateFromDays(ParseDateDays(left.value.varchar_value)),
            right);
      } catch (...) {
      }
      std::string_view sv = left.value.varchar_value;
      const size_t space = sv.find(' ');
      if (space != std::string_view::npos) {
        try {
          return EvaluateBinary(
              op, Value::DateFromDays(ParseDateDays(sv.substr(0, space))),
              right);
        } catch (...) {
        }
      }
    }
    throw std::runtime_error("type mismatch");
  }
  if (left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
    auto is_iv = [](std::string_view s) {
      return s.find('-') != std::string_view::npos &&
             s.find(' ') != std::string_view::npos && s.find('-') < s.find(' ');
    };
    if (is_iv(left.value.varchar_value) && is_iv(right.value.varchar_value)) {
      IntervalValue iv1 = IntervalValue::Parse(left.value.varchar_value);
      IntervalValue iv2 = IntervalValue::Parse(right.value.varchar_value);
      switch (op) {
        case BinaryOperation::kAdd:
          return Value((iv1 + iv2).ToString());
        case BinaryOperation::kSubtract:
          return Value((iv1 - iv2).ToString());
        case BinaryOperation::kEquals:
          return Value(iv1 == iv2);
        case BinaryOperation::kNotEquals:
          return Value(iv1 != iv2);
        case BinaryOperation::kLessThan:
          return Value(iv1 < iv2);
        case BinaryOperation::kLessThanEquals:
          return Value(iv1 <= iv2);
        case BinaryOperation::kGreaterThan:
          return Value(iv1 > iv2);
        case BinaryOperation::kGreaterThanEquals:
          return Value(iv1 >= iv2);
        default:
          break;
      }
    }
  }
  switch (op) {
    case BinaryOperation::kAdd:
      return left + right;
    case BinaryOperation::kSubtract:
      return left - right;
    case BinaryOperation::kMultiply:
      return left * right;
    case BinaryOperation::kDivide: {
      if (left.type == ValueType::kDouble) {
        if (right.value.double_value == 0.0) {
          throw std::runtime_error("division by zero");
        }
        const double res = left.value.double_value / right.value.double_value;
        if (std::isinf(res)) {
          throw std::runtime_error("double overflow");
        }
        if (std::isnan(res)) {
          throw std::runtime_error("division by zero");
        }
        return Value(res);
      }
      return left / right;
    }
    case BinaryOperation::kModulo:
      return left % right;
    case BinaryOperation::kEquals:
      if (left.type == ValueType::kVarChar &&
          right.type == ValueType::kVarChar &&
          IsStructJson(left.value.varchar_value) &&
          IsStructJson(right.value.varchar_value)) {
        return Value(StructJsonEqual(left.value.varchar_value,
                                     right.value.varchar_value));
      }
      return Value(left == right);
    case BinaryOperation::kNotEquals:
      if (left.type == ValueType::kVarChar &&
          right.type == ValueType::kVarChar &&
          IsStructJson(left.value.varchar_value) &&
          IsStructJson(right.value.varchar_value)) {
        return Value(!StructJsonEqual(left.value.varchar_value,
                                      right.value.varchar_value));
      }
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
    return EvaluateBinary(op_, left_value,
                          right_->Evaluate(row, schema, context));
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
  if (operation == BinaryOperation::kDivide) {
    return {TypeTag::kDouble};
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
  return "(" + left_->ToString() + " " + std::string(tinylamb::ToString(op_)) +
         " " + right_->ToString() + ")";
}

void BinaryExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
