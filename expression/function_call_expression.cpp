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

#include "expression/function_call_expression.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_set>

#include "database/database.hpp"
#include "expression/interval_expression.hpp"
#include "type/function.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/date.hpp"

namespace tinylamb {
namespace {
Value ExecuteFunction(const std::string& name,
                      const std::vector<Value>& values) {
  if (name == "coalesce") {
    for (const auto& val : values) {
      if (!val.IsNull()) return val;
    }
    return Value();
  }
  if (name == "concat") {
    std::string result;
    for (const auto& value : values) {
      if (value.IsNull()) return Value();
      if (value.type != ValueType::kVarChar) {
        throw std::runtime_error("CONCAT currently requires string arguments");
      }
      result.append(value.value.varchar_value);
    }
    return Value(std::move(result));
  }
  if (name == "substr" || name == "substring") {
    if (values.size() < 2 || values.size() > 3) {
      throw std::runtime_error("SUBSTR requires two or three arguments");
    }
    if (values[0].IsNull() || values[1].IsNull() ||
        (values.size() == 3 && values[2].IsNull())) {
      return Value();
    }
    if (values[0].type != ValueType::kVarChar ||
        values[1].type != ValueType::kInt64 ||
        (values.size() == 3 && values[2].type != ValueType::kInt64)) {
      throw std::runtime_error("SUBSTR argument type mismatch");
    }
    const std::string input(values[0].value.varchar_value);
    const int64_t start = values[1].value.int_value;
    const size_t begin = start <= 1 ? 0 : static_cast<size_t>(start - 1);
    const size_t length = values.size() == 3
                              ? static_cast<size_t>(values[2].value.int_value)
                              : std::string::npos;
    if (begin >= input.size()) return Value(std::string());
    return Value(input.substr(begin, length));
  }
  if (name == "extract_year" || name == "extract_month" ||
      name == "extract_day") {
    if (values.size() != 1) {
      throw std::runtime_error("EXTRACT requires one argument");
    }
    if (values[0].IsNull()) return Value();
    const std::string date = values[0].type == ValueType::kDate
                                 ? values[0].AsString()
                                 : std::string(values[0].value.varchar_value);
    if (date.size() < 10) throw std::runtime_error("invalid DATE value");
    if (name == "extract_year") return Value(std::stoll(date.substr(0, 4)));
    if (name == "extract_month") return Value(std::stoll(date.substr(5, 2)));
    return Value(std::stoll(date.substr(8, 2)));
  }
  if (name == "current_timestamp") {
    if (!values.empty()) {
      throw std::runtime_error("CURRENT_TIMESTAMP takes no arguments");
    }
    const std::time_t now =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm utc{};
    gmtime_r(&now, &utc);
    std::ostringstream output;
    output << std::put_time(&utc, "%Y-%m-%d %H:%M:%S");
    return Value(output.str());
  }
  throw std::runtime_error("Function calls are not yet executable: " + name);
}
}  // namespace

std::unordered_set<ColumnName> FunctionCallExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result;
  for (const auto& arg : args_) {
    result.merge(arg->TouchedColumns());
  }
  return result;
}

Value FunctionCallExpression::Evaluate(const Row& row,
                                       const Schema& schema) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    const Value date = args_[0]->Evaluate(row, schema);
    if (date.IsNull()) return Value();
    const auto& interval = args_[1]->AsIntervalExpression();
    const int64_t amount = func_name_ == "date_sub" ? -interval.Amount()
                                                     : interval.Amount();
    const int64_t days = date.type == ValueType::kDate
                             ? date.DateDays()
                             : ParseDateDays(date.value.varchar_value);
    const int64_t result = AddDateIntervalDays(days, amount, interval.Unit());
    return date.type == ValueType::kDate
               ? Value::DateFromDays(result)
               : Value(FormatDateDays(result));
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(row, schema));
  }
  return ExecuteFunction(func_name_, values);
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
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    const Value date =
        args_[0]->Evaluate(left, left_schema, right, right_schema);
    if (date.IsNull()) return Value();
    const auto& interval = args_[1]->AsIntervalExpression();
    const int64_t amount = func_name_ == "date_sub" ? -interval.Amount()
                                                     : interval.Amount();
    const int64_t days = date.type == ValueType::kDate
                             ? date.DateDays()
                             : ParseDateDays(date.value.varchar_value);
    const int64_t result = AddDateIntervalDays(days, amount, interval.Unit());
    return date.type == ValueType::kDate
               ? Value::DateFromDays(result)
               : Value(FormatDateDays(result));
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(left, left_schema, right, right_schema));
  }
  return ExecuteFunction(func_name_, values);
}

Type FunctionCallExpression::ResultType(const Schema& schema) const {
  if (func_name_ == "coalesce") {
    return args_[0]->ResultType(schema);
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "substr" || func_name_ == "substring") {
    return tinylamb::Type(TypeTag::kVarChar);
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    return args_[0]->ResultType(schema);
  }
  if (func_name_.starts_with("extract_")) {
    return tinylamb::Type(TypeTag::kBigInt);
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

Type FunctionCallExpression::ResultType(const Schema& left,
                                        const Schema& right) const {
  if (func_name_ == "coalesce") {
    return args_[0]->ResultType(left, right);
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "substr" || func_name_ == "substring") {
    return tinylamb::Type(TypeTag::kVarChar);
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    return args_[0]->ResultType(left, right);
  }
  if (func_name_.starts_with("extract_")) {
    return tinylamb::Type(TypeTag::kBigInt);
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
  ASSIGN_OR_RETURN(
      Function, func,
      ctx.GetDB()->GetOrAddFunction(ctx, func_name_, args_.size()));
  // TODO type check here.
  return Status::kSuccess;
}

}  // namespace tinylamb
