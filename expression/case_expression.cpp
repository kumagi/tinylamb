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

#include "expression/case_expression.hpp"

#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> CaseExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result;
  for (const auto& when : when_clauses_) {
    result.merge(when.first->TouchedColumns());
    result.merge(when.second->TouchedColumns());
  }
  if (else_clause_) {
    result.merge(else_clause_->TouchedColumns());
  }
  return result;
}

Value CaseExpression::Evaluate(const Row& row, const Schema& schema) const {
  for (const auto& when : when_clauses_) {
    const Value condition = when.first->Evaluate(row, schema);
    if (!condition.IsNull() && condition.Truthy()) {
      return when.second->Evaluate(row, schema);
    }
  }
  if (else_clause_) {
    return else_clause_->Evaluate(row, schema);
  }
  return {};
}

namespace {

Type UnifiedBranchType(std::vector<Type> branch_types) {
  if (branch_types.empty()) {
    return {TypeTag::kInvalid};
  }
  const Type first = branch_types.front();
  for (const Type& type : branch_types) {
    if (type.GetType() != first.GetType()) {
      // Branches disagree on the result type; the declared type would not
      // match the actually returned values.
      return {TypeTag::kInvalid};
    }
  }
  return first;
}

}  // namespace

Value CaseExpression::Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const {
  for (const auto& when : when_clauses_) {
    Value condition =
        when.first->Evaluate(left, left_schema, right, right_schema);
    if (!condition.IsNull() && condition.Truthy()) {
      return when.second->Evaluate(left, left_schema, right, right_schema);
    }
  }
  return else_clause_
             ? else_clause_->Evaluate(left, left_schema, right, right_schema)
             : Value();
}

// Context-aware form: same dispatch as the plain evaluator with the context
// threaded into every child expression (A1 stage 2).
Value CaseExpression::Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const {
  for (const auto& when : when_clauses_) {
    const Value condition = when.first->Evaluate(row, schema, context);
    if (!condition.IsNull() && condition.Truthy()) {
      return when.second->Evaluate(row, schema, context);
    }
  }
  return else_clause_ ? else_clause_->Evaluate(row, schema, context) : Value();
}

Type CaseExpression::ResultType(const Schema& schema) const {
  std::vector<tinylamb::Type> branch_types;
  branch_types.reserve(when_clauses_.size() + 1);
  for (const auto& when : when_clauses_) {
    branch_types.push_back(when.second->ResultType(schema));
  }
  if (else_clause_) {
    branch_types.push_back(else_clause_->ResultType(schema));
  }
  return UnifiedBranchType(std::move(branch_types));
}

Type CaseExpression::ResultType(const Schema& left, const Schema& right) const {
  std::vector<tinylamb::Type> branch_types;
  branch_types.reserve(when_clauses_.size() + 1);
  for (const auto& when : when_clauses_) {
    branch_types.push_back(when.second->ResultType(left, right));
  }
  if (else_clause_) {
    branch_types.push_back(else_clause_->ResultType(left, right));
  }
  return UnifiedBranchType(std::move(branch_types));
}

std::string CaseExpression::ToString() const {
  std::string result = "CASE";
  for (const auto& when : when_clauses_) {
    result +=
        " WHEN " + when.first->ToString() + " THEN " + when.second->ToString();
  }
  if (else_clause_) {
    result += " ELSE " + else_clause_->ToString();
  }
  result += " END";
  return result;
}

void CaseExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
