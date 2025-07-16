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

#ifndef TINYLAMB_CASE_EXPRESSION_HPP
#define TINYLAMB_CASE_EXPRESSION_HPP

#include <memory>
#include <utility>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

class CaseExpression : public ExpressionBase {
 public:
  CaseExpression(std::vector<std::pair<Expression, Expression>> when_clauses,
                 Expression else_clause)
      : when_clauses_(std::move(when_clauses)),
        else_clause_(std::move(else_clause)) {}
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kCaseExp; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;

  std::vector<std::pair<Expression, Expression>> when_clauses_;
  Expression else_clause_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CASE_EXPRESSION_HPP
