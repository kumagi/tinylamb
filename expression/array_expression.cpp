/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/array_expression.hpp"

#include <cctype>
#include <cstddef>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
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
      const int64_t v = value.value.int_value;
      if (v == 0 || v == 1) {
        return value;
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY<BOOL> element must be 0 or 1");
    }
    if (value.type == ValueType::kDouble) {
      // Accept the numeric BOOL literals 0.0 / 1.0 just like 0 / 1 so the
      // spelling of a literal does not decide whether ARRAY<BOOL> parses.
      const double d = value.value.double_value;
      if (d == 0.0 || d == 1.0) {
        return Value(static_cast<int64_t>(d));
      }
      return StatusError(StatusCode::kInvalidArgument,
                         "ARRAY<BOOL> element must be 0 or 1");
    }
  }
  if (type == "FLOAT" || type == "FLOAT32" || type == "FLOAT64" ||
      type == "DOUBLE") {
    if (value.type == ValueType::kInt64) {
      // An unsigned UINT64 shares the signed bit pattern; read it as uint64
      // so 2^64-1 becomes 18446744073709551616.0, not -1.0.
      return Value(value.IsUnsigned()
                       ? static_cast<double>(
                             static_cast<uint64_t>(value.value.int_value))
                       : static_cast<double>(value.value.int_value));
    }
  }
  if (type == "DATE" && value.type == ValueType::kVarChar) {
    return Value::TryDate(value.value.varchar_value);
  }
  if (type.starts_with("ARRAY<") && value.IsArray()) {
    // An ARRAY<ARRAY<T>> literal must hold arrays (and vice versa); the
    // outer shape check alone used to accept a nested declaration with
    // scalar elements, leaving a wrong inner element_sql_type tag.
    const size_t close = type.rfind('>');
    if (close != std::string::npos && close > 6) {
      const std::string inner = type.substr(6, close - 6);
      const bool inner_is_array = inner.starts_with("ARRAY<");
      for (const Value& elem : value.ArrayElements()) {
        if (!elem.IsNull() && inner_is_array != elem.IsArray()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "array element type does not match the declared element type");
        }
      }
    }
    return value;
  }
  // The inferred/declared element type must actually hold: silently keeping
  // a mismatched storage type would diverge the array's element_sql_type tag
  // from its contents (GoogleSQL rejects these mixes).  Only the
  // unambiguous scalar tags are enforced; parameterized spellings
  // (STRING(10), NUMERIC(10,2), ...) stay unvalidated at rewrite time.
  const std::string bare = type.substr(0, type.find('('));
  std::optional<ValueType> expected;
  if (bare == "INT64" || bare == "INT" || bare == "INTEGER" ||
      bare == "SMALLINT" || bare == "BIGINT" || bare == "BOOL" ||
      bare == "BOOLEAN" || bare == "UINT64" || bare == "UINT32" ||
      bare == "UINT16" || bare == "UINT8") {
    expected = ValueType::kInt64;
  } else if (bare == "FLOAT64" || bare == "FLOAT" || bare == "FLOAT32" ||
             bare == "DOUBLE" || bare == "REAL" || bare == "NUMERIC" ||
             bare == "BIGNUMERIC") {
    expected = ValueType::kDouble;
  } else if (bare == "STRING" || bare == "VARCHAR" || bare == "TEXT" ||
             bare == "CHAR" || bare == "JSON" || bare == "BYTES" ||
             bare == "TIMESTAMP" || bare == "TIME" || bare == "DATETIME" ||
             bare == "INTERVAL" || bare == "PROTO" || bare == "STRUCT") {
    expected = ValueType::kVarChar;
  } else if (bare == "DATE") {
    expected = ValueType::kDate;
  } else if (bare.starts_with("ARRAY<")) {
    expected = ValueType::kArray;
  }
  if (expected.has_value() && value.type != *expected) {
    return StatusError(
        StatusCode::kInvalidArgument,
        "array element type does not match the declared element type");
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
    if (inferred_type.empty() && !v.IsNull()) {
      if (v.type == ValueType::kVarChar) {
        // INTERVAL is a tagged kVarChar; keep its SQL label instead of
        // collapsing it to STRING (TryCoerceArrayElement accepts INTERVAL).
        inferred_type = v.IsInterval() ? "INTERVAL" : "STRING";
      } else if (v.type == ValueType::kInt64) {
        // UINT64 shares the INT64 storage tag; preserve its signedness tag.
        inferred_type = v.IsUnsigned() ? "UINT64" : "INT64";
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
