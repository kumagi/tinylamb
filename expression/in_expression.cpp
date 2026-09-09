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

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Membership must use the same comparison as `=` (TryEvaluateBinary), which
// promotes INT64/DOUBLE and honors unsigned/collation tags. The previous
// `child == candidate` used Value::operator==, which returns false across
// types, so `1 IN (1.0)` folded to FALSE while `1 = 1.0` was TRUE.
StatusOr<bool> TryMatches(const Value& child, const Value& candidate) {
  ASSIGN_OR_RETURN(
      Value, eq, TryEvaluateBinary(BinaryOperation::kEquals, child, candidate));
  return !eq.IsNull() && eq.Truthy();
}

template <typename... Args>
StatusOr<Value> TryMembership(const ExpressionBase& child,
                              const std::vector<Expression>& list,
                              Args&&... args) {
  ASSIGN_OR_RETURN(Value, child_value, child.TryEvaluate(args...));
  bool saw_null = child_value.IsNull();
  for (const auto& item : list) {
    ASSIGN_OR_RETURN(Value, candidate, item->TryEvaluate(args...));
    saw_null |= candidate.IsNull();
    if (!child_value.IsNull() && !candidate.IsNull()) {
      ASSIGN_OR_RETURN(bool, matched, TryMatches(child_value, candidate));
      if (matched) {
        return Value(true);
      }
    }
  }
  return saw_null ? Value() : Value(false);
}
}  // namespace

std::unordered_set<ColumnName> InExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result = child_->TouchedColumns();
  for (const auto& item : list_) {
    result.merge(item->TouchedColumns());
  }
  return result;
}

StatusOr<Value> InExpression::TryEvaluate(const Row& row,
                                          const Schema& schema) const {
  return TryMembership(*child_, list_, row, schema);
}

StatusOr<Value> InExpression::TryEvaluate(const Row* left,
                                          const Schema& left_schema,
                                          const Row* right,
                                          const Schema& right_schema) const {
  return TryMembership(*child_, list_, left, left_schema, right, right_schema);
}

// Context-aware form: same three-valued membership as the plain evaluator
// with the context threaded into every child (A1 stage 2).
StatusOr<Value> InExpression::TryEvaluate(const Row& row, const Schema& schema,
                                          EvaluationContext& context) const {
  return TryMembership(*child_, list_, row, schema, context);
}

// EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
Value InExpression::Evaluate(const Row& row, const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema), "InExpression::Evaluate");
}

Value InExpression::Evaluate(const Row* left, const Schema& left_schema,
                             const Row* right,
                             const Schema& right_schema) const {
  return ExcShimUnwrap(TryEvaluate(left, left_schema, right, right_schema),
                       "InExpression::Evaluate");
}

Value InExpression::Evaluate(const Row& row, const Schema& schema,
                             EvaluationContext& context) const {
  return ExcShimUnwrap(TryEvaluate(row, schema, context),
                       "InExpression::Evaluate");
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
