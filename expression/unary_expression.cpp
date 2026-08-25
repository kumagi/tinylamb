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

Value EvaluateUnary(UnaryOperation operation, const Value& child) {
  switch (operation) {
    case UnaryOperation::kIsNull:
      return Value(child.IsNull());
    case UnaryOperation::kIsNotNull:
      return Value(!child.IsNull());
    case UnaryOperation::kIsTrue:
      return Value(!child.IsNull() && child.Truthy());
    case UnaryOperation::kIsNotTrue:
      return Value(child.IsNull() || !child.Truthy());
    case UnaryOperation::kIsFalse:
      return Value(!child.IsNull() && !child.Truthy());
    case UnaryOperation::kIsNotFalse:
      return Value(child.IsNull() || child.Truthy());
    case UnaryOperation::kNot:
      return child.IsNull() ? Value() : Value(!child.Truthy());
    case UnaryOperation::kMinus:
      if (child.IsNull()) { return {}; }
      if (child.type == ValueType::kDouble) {
        return Value(-child.value.double_value);
      }
      if (child.type == ValueType::kInt64) {
        if (child.value.int_value ==
            std::numeric_limits<int64_t>::min()) {
          throw std::runtime_error("integer overflow in unary minus");
        }
        return Value(-child.value.int_value);
      }
      throw std::runtime_error("unary minus requires a number");
  }
  throw std::logic_error("invalid unary operation");
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
  return child;
}

}  // namespace


std::unordered_set<ColumnName> UnaryExpression::TouchedColumns() const {
  return child_->TouchedColumns();
}

Value UnaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  return EvaluateUnary(operation_, child_->Evaluate(row, schema));
}

Value UnaryExpression::Evaluate(const Row* left, const Schema& left_schema,
                                const Row* right,
                                const Schema& right_schema) const {
  return EvaluateUnary(
      operation_, child_->Evaluate(left, left_schema, right, right_schema));
}

Type UnaryExpression::ResultType(const Schema& schema) const {
  return UnaryResultType(operation_, child_->ResultType(schema));
}

Type UnaryExpression::ResultType(const Schema& left,
                                 const Schema& right) const {
  return UnaryResultType(operation_, child_->ResultType(left, right));
}

std::string UnaryExpression::ToString() const {
  if (operation_ == UnaryOperation::kMinus) {
    return "(" + ::tinylamb::ToString(operation_) + child_->ToString() + ")";
  }
  return "(" + ::tinylamb::ToString(operation_) + " " + child_->ToString() +
         ")";
}

void UnaryExpression::Dump(std::ostream& o) const {
  if (operation_ == UnaryOperation::kMinus) {
    o << "(" << ::tinylamb::ToString(operation_) << *child_ << ")";
  } else {
    o << "(" << ::tinylamb::ToString(operation_) << " " << *child_ << ")";
  }
}

}  // namespace tinylamb
