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

#ifndef TINYLAMB_UNARY_EXPRESSION_HPP
#define TINYLAMB_UNARY_EXPRESSION_HPP

#include <memory>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

[[nodiscard]] Value EvaluateUnary(UnaryOperation operation,
                                  const Value& child);

class UnaryExpression : public ExpressionBase {
 public:
  UnaryExpression(Expression child, UnaryOperation op)
      : child_(std::move(child)), operation_(op) {}
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kUnaryExp; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& left,
                                          const Schema& right) const override;
  [[nodiscard]] const Expression& Child() const { return child_; }
  [[nodiscard]] UnaryOperation Op() const { return operation_; }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

 private:
  Expression child_;
  UnaryOperation operation_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_UNARY_EXPRESSION_HPP
