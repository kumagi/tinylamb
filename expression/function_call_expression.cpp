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
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/interval_expression.hpp"
#include "type/column_name.hpp"
#include "type/function.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/date.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {
Value AddOrSubInterval(const std::string& func_name, const Value& date,
                       const IntervalExpression& interval) {
  const int64_t amount =
      func_name == "date_sub" ? -interval.Amount() : interval.Amount();
  const int64_t days = date.type == ValueType::kDate
                           ? date.DateDays()
                           : ParseDateDays(date.value.varchar_value);
  const int64_t result = AddDateIntervalDays(days, amount, interval.Unit());
  return date.type == ValueType::kDate ? Value::DateFromDays(result)
                                       : Value(FormatDateDays(result));
}

Value ExecuteFunction(const std::string& name,
                      const std::vector<Value>& values) {
  if (name == "coalesce") {
    for (const auto& val : values) {
      if (!val.IsNull()) { return val;
}
    }
    return {};
  }
  if (name == "concat") {
    std::string result;
    for (const auto& value : values) {
      if (value.IsNull()) { return {};
}
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
      return {};
    }
    if (values[0].type != ValueType::kVarChar ||
        values[1].type != ValueType::kInt64 ||
        (values.size() == 3 && values[2].type != ValueType::kInt64)) {
      throw std::runtime_error("SUBSTR argument type mismatch");
    }
    const std::string input(values[0].value.varchar_value);
    const int64_t start = values[1].value.int_value;
    // A non-positive length yields the empty string. Casting a negative
    // length to size_t would wrap around near SIZE_MAX and return the whole
    // rest of the string (e.g. SUBSTR('abc', 1, -1) -> 'abc').
    if (values.size() == 3 && values[2].value.int_value <= 0) {
      return Value(std::string());
    }
    const size_t begin = start <= 1 ? 0 : static_cast<size_t>(start - 1);
    const size_t length = values.size() == 3
                              ? static_cast<size_t>(values[2].value.int_value)
                              : std::string::npos;
    if (begin >= input.size()) { return Value(std::string());
}
    return Value(input.substr(begin, length));
  }
  if (name == "extract_year" || name == "extract_month" ||
      name == "extract_day") {
    if (values.size() != 1) {
      throw std::runtime_error("EXTRACT requires one argument");
    }
    if (values[0].IsNull()) { return {};
}
    if (values[0].type != ValueType::kDate &&
        values[0].type != ValueType::kVarChar) {
      throw std::runtime_error("EXTRACT requires DATE or STRING");
    }
    const std::string date = values[0].type == ValueType::kDate
                                 ? values[0].AsString()
                                 : std::string(values[0].value.varchar_value);
    if (date.size() < 10) { throw std::runtime_error("invalid DATE value");
}
    int64_t part = 0;
    try {
      if (name == "extract_year") {
        part = std::stoll(date.substr(0, 4));
      } else if (name == "extract_month") {
        part = std::stoll(date.substr(5, 2));
      } else {
        part = std::stoll(date.substr(8, 2));
      }
    } catch (const std::logic_error&) {
      throw std::runtime_error("invalid DATE value: " + date);
    }
    return Value(part);
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
    if (date.IsNull()) { return {};
}
    return AddOrSubInterval(func_name_, date,
                            args_[1]->AsIntervalExpression());
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
    if (date.IsNull()) { return {};
}
    return AddOrSubInterval(func_name_, date,
                            args_[1]->AsIntervalExpression());
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(left, left_schema, right, right_schema));
  }
  return ExecuteFunction(func_name_, values);
}

// Context-aware form: same dispatch as the plain evaluator with the context
// threaded into every argument (A1 stage 3).
Value FunctionCallExpression::Evaluate(const Row& row, const Schema& schema,
                                       EvaluationContext& context) const {
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    if (args_.size() != 2 || args_[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires DATE and INTERVAL");
    }
    const Value date = args_[0]->Evaluate(row, schema, context);
    if (date.IsNull()) { return {};
}
    return AddOrSubInterval(func_name_, date,
                            args_[1]->AsIntervalExpression());
  }
  std::vector<Value> values;
  values.reserve(args_.size());
  for (const auto& arg : args_) {
    values.emplace_back(arg->Evaluate(row, schema, context));
  }
  return ExecuteFunction(func_name_, values);
}

Type FunctionCallExpression::ResultType(const Schema& schema) const {
  if (func_name_ == "coalesce") {
    if (args_.empty()) { return {TypeTag::kInvalid};
}
    return args_[0]->ResultType(schema);
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "substr" || func_name_ == "substring") {
    return {TypeTag::kVarChar};
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    return args_[0]->ResultType(schema);
  }
  if (func_name_.starts_with("extract_")) {
    return {TypeTag::kBigInt};
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

Type FunctionCallExpression::ResultType(const Schema& left,
                                        const Schema& right) const {
  if (func_name_ == "coalesce") {
    if (args_.empty()) { return {TypeTag::kInvalid};
}
    return args_[0]->ResultType(left, right);
  }
  if (func_name_ == "concat" || func_name_ == "current_timestamp" ||
      func_name_ == "substr" || func_name_ == "substring") {
    return {TypeTag::kVarChar};
  }
  if (func_name_ == "date_add" || func_name_ == "date_sub") {
    return args_[0]->ResultType(left, right);
  }
  if (func_name_.starts_with("extract_")) {
    return {TypeTag::kBigInt};
  }
  throw std::runtime_error("Function calls are not yet executable.");
}

Status FunctionCallExpression::Validate(EvaluationContext& context,
                                        const Schema& schema) const {
  for (const auto& arg : args_) {
    Status s = arg->Validate(context, schema);
    if (s != Status::kSuccess) {
      return s;
    }
  }
  // Function registration goes through the abstract context; the production
  // implementation forwards to Database::GetOrAddFunction (improvement3.md
  // A1).  Type check is still TODO.
  return context.GetOrAddFunction(func_name_,
                                  static_cast<int>(args_.size()));
}

}  // namespace tinylamb
