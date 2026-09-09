/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/array_expression.hpp"

#include <cctype>
#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/evaluation_context.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::string Upper(std::string text) {
  for (char& c : text) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return text;
}

StatusOr<Value> TryCoerceArrayElement(Value value, std::string_view sql_type) {
  if (value.IsNull()) {
    return value;
  }
  const std::string type = Upper(std::string(sql_type));
  if (type == "BOOL" || type == "BOOLEAN") {
    if (value.type == ValueType::kInt64) {
      return value;
    }
  }
  if (type == "FLOAT" || type == "FLOAT32" || type == "FLOAT64" ||
      type == "DOUBLE") {
    if (value.type == ValueType::kInt64) {
      return Value(static_cast<double>(value.value.int_value));
    }
  }
  if (type == "DATE" && value.type == ValueType::kVarChar) {
    return Value::TryDate(value.value.varchar_value);
  }
  if (type.starts_with("ARRAY<") && value.IsArray()) {
    return value;
  }
  return value;
}

}  // namespace

namespace {
template <typename... Args>
StatusOr<Value> TryBuildArray(const std::vector<Expression>& elements,
                              const std::string& declared_type,
                              Args&&... args) {
  std::vector<Value> values;
  values.reserve(elements.size());
  std::string inferred_type = declared_type;
  for (const Expression& element : elements) {
    ASSIGN_OR_RETURN(Value, v, element->TryEvaluate(args...));
    if ((inferred_type.empty() || inferred_type == "INT64") && !v.IsNull()) {
      if (v.type == ValueType::kVarChar) {
        inferred_type = "STRING";
      } else if (v.type == ValueType::kDouble) {
        inferred_type = "DOUBLE";
      } else if (v.type == ValueType::kDate) {
        inferred_type = "DATE";
      } else if (v.IsArray()) {
        inferred_type = "ARRAY<" + v.ArrayElementSqlType() + ">";
      }
    }
    values.push_back(std::move(v));
  }
  if (inferred_type.empty()) {
    inferred_type = "INT64";
  }
  for (auto& val : values) {
    ASSIGN_OR_RETURN(Value, coerced,
                     TryCoerceArrayElement(std::move(val), inferred_type));
    val = std::move(coerced);
  }
  return Value::Array(std::move(values), inferred_type);
}
}  // namespace

StatusOr<Value> ArrayExpression::TryEvaluate(const Row& row,
                                             const Schema& schema) const {
  return TryBuildArray(elements_, element_sql_type_, row, schema);
}

StatusOr<Value> ArrayExpression::TryEvaluate(const Row* left,
                                             const Schema& left_schema,
                                             const Row* right,
                                             const Schema& right_schema) const {
  return TryBuildArray(elements_, element_sql_type_, left, left_schema, right,
                       right_schema);
}

StatusOr<Value> ArrayExpression::TryEvaluate(const Row& row,
                                             const Schema& schema,
                                             EvaluationContext& context) const {
  return TryBuildArray(elements_, element_sql_type_, row, schema, context);
}

// EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
Value ArrayExpression::Evaluate(const Row& row, const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema), "ArrayExpression::Evaluate");
}

Value ArrayExpression::Evaluate(const Row* left, const Schema& left_schema,
                                const Row* right,
                                const Schema& right_schema) const {
  return ExcShimUnwrap(TryEvaluate(left, left_schema, right, right_schema),
                       "ArrayExpression::Evaluate");
}

Value ArrayExpression::Evaluate(const Row& row, const Schema& schema,
                                EvaluationContext& context) const {
  return ExcShimUnwrap(TryEvaluate(row, schema, context),
                       "ArrayExpression::Evaluate");
}

std::string ArrayExpression::ToString() const {
  std::ostringstream out;
  out << "ARRAY<" << element_sql_type_ << ">[";
  for (size_t i = 0; i < elements_.size(); ++i) {
    if (i != 0) {
      out << ", ";
    }
    out << elements_[i]->ToString();
  }
  out << "]";
  return out.str();
}

void ArrayExpression::Dump(std::ostream& o) const { o << ToString(); }

std::unordered_set<ColumnName> ArrayExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> columns;
  for (const Expression& element : elements_) {
    auto touched = element->TouchedColumns();
    columns.insert(touched.begin(), touched.end());
  }
  return columns;
}

}  // namespace tinylamb
