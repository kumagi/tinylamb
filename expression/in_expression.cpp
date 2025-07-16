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

#include "expression/in_expression.hpp"

#include <string>

#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value InExpression::Evaluate(const Row& row, const Schema& schema) const {
  Value child = child_->Evaluate(row, schema);
  for (const auto& item : list_) {
    if (child == item->Evaluate(row, schema)) {
      return Value(true);
    }
  }
  return Value(false);
}

std::string InExpression::ToString() const {
  std::string result = child_->ToString() + " IN (";
  for (size_t i = 0; i < list_.size(); ++i) {
    result += list_[i]->ToString();
    if (i < list_.size() - 1) {
      result += ", ";
    }
  }
  result += ")";
  return result;
}

void InExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
