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
  return Execute(op_, left_->Evaluate(row, schema),
                 right_->Evaluate(row, schema));
}

std::string BinaryExpression::ToString() const {
  std::stringstream o;
  o << "(" << *left_ << " " << tinylamb::ToString(op_) << " " << *right_ << ")";
  return o.str();
}

void BinaryExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
