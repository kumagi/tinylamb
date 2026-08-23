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

#include <cstddef>
#include <ostream>
#include <string>
#include <unordered_set>

#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> InExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result = child_->TouchedColumns();
  for (const auto& item : list_) {
    result.merge(item->TouchedColumns());
  }
  return result;
}

Value InExpression::Evaluate(const Row& row, const Schema& schema) const {
  Value child = child_->Evaluate(row, schema);
  bool saw_null = child.IsNull();
  for (const auto& item : list_) {
    Value candidate = item->Evaluate(row, schema);
    saw_null |= candidate.IsNull();
    if (!child.IsNull() && !candidate.IsNull() && child == candidate) {
      return Value(true);
    }
  }
  return saw_null ? Value() : Value(false);
}

Value InExpression::Evaluate(const Row* left, const Schema& left_schema,
                             const Row* right,
                             const Schema& right_schema) const {
  Value child = child_->Evaluate(left, left_schema, right, right_schema);
  bool saw_null = child.IsNull();
  for (const auto& item : list_) {
    Value candidate = item->Evaluate(left, left_schema, right, right_schema);
    saw_null |= candidate.IsNull();
    if (!child.IsNull() && !candidate.IsNull() && child == candidate) {
      return Value(true);
    }
  }
  return saw_null ? Value() : Value(false);
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
