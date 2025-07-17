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

#ifndef TINYLAMB_FUNCTION_CALL_EXPRESSION_HPP
#define TINYLAMB_FUNCTION_CALL_EXPRESSION_HPP

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

class FunctionCallExpression : public ExpressionBase {
 public:
  FunctionCallExpression(std::string func_name, std::vector<Expression> args)
      : func_name_(std::move(func_name)), args_(std::move(args)) {}

  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& left,
                                          const Schema& right) const override;
  Status Validate(TransactionContext& ctx, const Schema& schema) const override;
  [[nodiscard]] const std::string& FuncName() const { return func_name_; }
  [[nodiscard]] const std::vector<Expression>& Args() const { return args_; }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kFunctionCallExp;
  }

 private:
  std::string func_name_;
  std::vector<Expression> args_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FUNCTION_CALL_EXPRESSION_HPP
