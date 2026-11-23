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
#include <vector>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/expression.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
template <typename... Args>
StatusOr<Value> TryMembership(const ExpressionBase& child,
                              const std::vector<Expression>& list,
                              Args&&... args) {
  ASSIGN_OR_RETURN(Value, child_value, child.TryEvaluate(args...));
  // IN is an existential comparison: an empty candidate set has no TRUE
  // or UNKNOWN comparison, so membership is FALSE even for a NULL probe.
  // This also matches IN over an empty subquery (and NOT IN becomes TRUE).
  bool saw_null = !list.empty() && child_value.IsNull();
  // Candidates are tried in order and the first match wins: once a match is
  // found, later candidates are never evaluated, so an error or collation
  // conflict in them is not raised (`-1 IN (-1, 1/0)` is TRUE), matching the
  // bytecode/rewrite paths the differential audits pin. Without a match,
  // every candidate is evaluated and its errors propagate normally.
  for (const auto& item : list) {
    ASSIGN_OR_RETURN(Value, candidate, item->TryEvaluate(args...));
    if (!child_value.IsNull() && !candidate.IsNull() &&
        HaveConflictingCollations(child_value, candidate)) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Collation conflict between the IN operands");
    }
    saw_null |= candidate.IsNull();
    if (!child_value.IsNull() && !candidate.IsNull()) {
      ASSIGN_OR_RETURN(
          Value, match,
          TryEvaluateBinary(BinaryOperation::kEquals, child_value, candidate));
      if (match.IsNull()) {
        saw_null = true;
      } else if (match.Truthy()) {
        return Value(true);
      }
    }
  }
  return saw_null ? Value() : Value(false);
}
}  // namespace

std::unordered_set<ColumnName> InExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result;
  // Guard against null children on hand-built or rewrite-damaged trees.
  if (child_) {
    result = child_->TouchedColumns();
  }
  for (const auto& item : list_) {
    if (item) {
      result.merge(item->TouchedColumns());
    }
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
  std::string result =
      (child_ ? child_->ToString() : std::string("<null>")) + " IN (";
  for (size_t i = 0; i < list_.size(); ++i) {
    result += list_[i] ? list_[i]->ToString() : std::string("<null>");
    if (i < list_.size() - 1) {
      result += ", ";
    }
  }
  result += ")";
  return result;
}

void InExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
