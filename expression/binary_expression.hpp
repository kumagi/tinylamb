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

#ifndef TINYLAMB_BINARY_EXPRESSION_HPP
#define TINYLAMB_BINARY_EXPRESSION_HPP

#include <memory>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

class EvaluationContext;

[[nodiscard]] Value EvaluateBinary(BinaryOperation operation, const Value& left,
                                   const Value& right);

// Lowercases ASCII letters; the fold applied by case-insensitive collation
// comparisons ('und:ci').
[[nodiscard]] std::string FoldCase(std::string_view s);

class BinaryExpression : public ExpressionBase {
 public:
  BinaryExpression(Expression left, BinaryOperation op, Expression right)
      : left_(std::move(left)), right_(std::move(right)), op_(op) {}
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kBinaryExp; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override;
  // Stage 2 of the A1 migration: threads EvaluationContext down to children
  // so nested subqueries resolve through the abstract interface.
  [[nodiscard]] Value Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& left,
                                          const Schema& right) const override;
  [[nodiscard]] BinaryOperation Op() const { return op_; }
  [[nodiscard]] const Expression& Left() const { return left_; }
  [[nodiscard]] const Expression& Right() const { return right_; }
  std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

 private:
  Expression left_;
  Expression right_;
  BinaryOperation op_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BINARY_EXPRESSION_HPP
