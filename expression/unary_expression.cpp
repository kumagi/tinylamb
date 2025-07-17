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

#include <string>

#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value UnaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  Value child = child_->Evaluate(row, schema);
  switch (operation_) {
    case UnaryOperation::kIsNull:
      return Value(child.IsNull());
    case UnaryOperation::kIsNotNull:
      return Value(!child.IsNull());
    case UnaryOperation::kNot:
      if (child.IsNull()) {
        return Value();
      }
      return Value(!child.Truthy());
    case UnaryOperation::kMinus:
      if (child.type == ValueType::kDouble) {
        return Value(-child.value.double_value);
      }
      return Value(-child.value.int_value);
  }
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
