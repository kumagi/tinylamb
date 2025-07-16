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

#include <string>

#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value CaseExpression::Evaluate(const Row& row, const Schema& schema) const {
  for (const auto& when : when_clauses_) {
    if (when.first->Evaluate(row, schema).Truthy()) {
      return when.second->Evaluate(row, schema);
    }
  }
  if (else_clause_) {
    return else_clause_->Evaluate(row, schema);
  }
  return {};
}

std::string CaseExpression::ToString() const {
  std::string result = "CASE";
  for (const auto& when : when_clauses_) {
    result += " WHEN " + when.first->ToString() + " THEN " +
              when.second->ToString();
  }
  if (else_clause_) {
    result += " ELSE " + else_clause_->ToString();
  }
  result += " END";
  return result;
}

void CaseExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
