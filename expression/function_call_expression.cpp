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

#include "database/database.hpp"
#include "expression/function_call_expression.hpp"

#include <sstream>
#include <string>

#include "type/function.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value FunctionCallExpression::Evaluate(const Row& row,
                                       const Schema& schema) const {
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(row, schema));
  }
  if (func_name_ == "coalesce") {
    for (const auto& val : values) {
      if (!val.IsNull()) {
        return val;
      }
    }
    return {Value()};
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

std::string FunctionCallExpression::ToString() const {
  std::stringstream ss;
  ss << func_name_ << "(";
  for (size_t i = 0; i < args_.size(); ++i) {
    ss << *args_[i];
    if (i < args_.size() - 1) {
      ss << ", ";
    }
  }
  ss << ")";
  return ss.str();
}

void FunctionCallExpression::Dump(std::ostream& o) const { o << ToString(); }

Value FunctionCallExpression::Evaluate(const Row* left,
                                       const Schema& left_schema,
                                       const Row* right,
                                       const Schema& right_schema) const {
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(left, left_schema, right, right_schema));
  }
  if (func_name_ == "coalesce") {
    for (const auto& val : values) {
      if (!val.IsNull()) {
        return val;
      }
    }
    return {Value()};
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

Type FunctionCallExpression::ResultType(const Schema& schema) const {
  if (func_name_ == "coalesce") {
    return args_[0]->ResultType(schema);
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

Type FunctionCallExpression::ResultType(const Schema& left,
                                        const Schema& right) const {
  if (func_name_ == "coalesce") {
    return args_[0]->ResultType(left, right);
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

Status FunctionCallExpression::Validate(TransactionContext& ctx,
                                        const Schema& schema) const {
  for (const auto& arg : args_) {
    Status s = arg->Validate(ctx, schema);
    if (s != Status::kSuccess) {
      return s;
    }
  }
  ASSIGN_OR_RETURN(Function, func,
                   ctx.GetDB()->GetOrAddFunction(ctx, func_name_, args_.size()));
  // TODO type check here.
  return Status::kSuccess;
}

}  // namespace tinylamb
