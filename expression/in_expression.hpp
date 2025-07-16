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

#ifndef TINYLAMB_IN_EXPRESSION_HPP
#define TINYLAMB_IN_EXPRESSION_HPP

#include <memory>
#include <utility>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

class InExpression : public ExpressionBase {
 public:
  InExpression(Expression child, std::vector<Expression> list)
      : child_(std::move(child)), list_(std::move(list)) {}
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kInExp; }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;

  Expression child_;
  std::vector<Expression> list_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_IN_EXPRESSION_HPP
