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

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

class EvaluationContext;

// Splits a JSON object body into top-level key / raw-value-text pairs.
std::vector<std::pair<std::string, std::string>> SplitJsonObjectMembers(
    const std::string& body);

// Parses one JSON scalar/object/array token into a Value.
bool JsonTextToValue(const std::string& text, Value* parsed);

// Encodes an evaluated value as struct-member JSON text (strings quoted and
// escaped, numbers bare, nested objects/arrays embedded verbatim).
std::string EncodeStructMemberJson(const Value& value);

// Sets a (possibly dotted) field on a struct-typed JSON text value and
// returns the rewritten JSON. Missing intermediates are created; a NULL base
// stays NULL.
Value StructSetField(const Value& json, const std::string& path,
                     const Value& new_value);

class FunctionCallExpression : public ExpressionBase {
 public:
  FunctionCallExpression(std::string func_name, std::vector<Expression> args)
      : func_name_(std::move(func_name)), args_(std::move(args)) {
    std::transform(
        func_name_.begin(), func_name_.end(), func_name_.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  }

  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override;
  // Stage 3 of the A1 migration: arguments resolve through the abstract
  // EvaluationContext.
  [[nodiscard]] Value Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema& left,
                                          const Schema& right) const override;
  Status Validate(EvaluationContext& context, const Schema& schema) const override;
  [[nodiscard]] const std::string& FuncName() const { return func_name_; }
  [[nodiscard]] const std::vector<Expression>& Args() const { return args_; }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kFunctionCallExp;
  }
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

 private:
  std::string func_name_;
  std::vector<Expression> args_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FUNCTION_CALL_EXPRESSION_HPP
