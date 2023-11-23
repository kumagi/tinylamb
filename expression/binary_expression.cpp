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

namespace tinylamb {
namespace {

Value Execute(BinaryOperation op, const Value& left, const Value& right) {
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
      return (left & right);
    case BinaryOperation::kOr:
      return (left | right);
    case BinaryOperation::kXor:
      return (left ^ right);
  }
}

}  // anonymous namespace

Value BinaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  return Execute(operation_, left_->Evaluate(row, schema),
                 right_->Evaluate(row, schema));
}

std::string BinaryExpression::ToString() const {
  std::stringstream ss;
  ss << left_->ToString();
  switch (operation_) {
    case BinaryOperation::kAdd:
      ss << " + ";
      break;
    case BinaryOperation::kSubtract:
      ss << " - ";
      break;
    case BinaryOperation::kMultiply:
      ss << " * ";
      break;
    case BinaryOperation::kDivide:
      ss << " / ";
      break;
    case BinaryOperation::kModulo:
      ss << " % ";
      break;
    case BinaryOperation::kEquals:
      ss << " = ";
      break;
    case BinaryOperation::kNotEquals:
      ss << " != ";
      break;
    case BinaryOperation::kLessThan:
      ss << " < ";
      break;
    case BinaryOperation::kLessThanEquals:
      ss << " <= ";
      break;
    case BinaryOperation::kGreaterThan:
      ss << " > ";
      break;
    case BinaryOperation::kGreaterThanEquals:
      ss << " >= ";
      break;
    case BinaryOperation::kAnd:
      ss << " AND ";
      break;
    case BinaryOperation::kOr:
      ss << " OR ";
      break;
    case BinaryOperation::kXor:
      ss << " XOR ";
      break;
  }
  ss << right_->ToString();
  return ss.str();
}

void BinaryExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb