/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/expression_eval.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <regex>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/digest.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

bool Truthy(const Value& value) {
  if (value.IsNull()) {
    return false;
  }
  if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
    return value.value.int_value != 0;
  }
  if (value.type == ValueType::kDouble) {
    return value.value.double_value != 0.0;
  }
  return !value.value.varchar_value.empty();
}

std::string ElementSqlTypeName(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
      return "INT64";
    case ValueType::kDouble:
      return "FLOAT64";
    case ValueType::kVarChar:
      return "STRING";
    case ValueType::kDate:
      return "DATE";
    default:
      return {};
  }
}

size_t DistinctValueHash::operator()(const Value& value) const {
  return std::hash<Value>()(CanonicalDistinctValue(value));
}

bool DistinctValueEqual::operator()(const Value& left,
                                    const Value& right) const {
  if (left.type != right.type) { return false;
}
  if (left.type == ValueType::kDouble) {
    const double x = left.value.double_value;
    const double y = right.value.double_value;
    if (std::isnan(x) && std::isnan(y)) { return true;
}
    return x == y;
  }
  return left == right;
}

Value CanonicalDistinctValue(const Value& value) {
  if (value.type == ValueType::kDouble) {
    const double d = value.value.double_value;
    if (std::isnan(d)) {
      return Value(std::numeric_limits<double>::quiet_NaN());
    }
    if (d == 0.0) {
      return Value(0.0);  // -0.0 folds to +0.0
    }
  }
  return value;
}

namespace {

bool IdentifierEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

int FindColumn(const Schema& schema, const ColumnName& name) {
  int match = -1;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    const bool exact = !name.schema.empty() &&
                       IdentifierEquals(candidate.schema, name.schema) &&
                       IdentifierEquals(candidate.name, name.name);
    const bool unqualified =
        name.schema.empty() && IdentifierEquals(candidate.name, name.name);
    if (exact || unqualified) {
      if (match >= 0 && unqualified) {
        throw std::runtime_error("ambiguous column " + name.name);
      }
      match = static_cast<int>(i);
    }
  }
  if (match < 0 && !name.schema.empty()) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name, name.name)) {
        if (match >= 0) {
          return -1;
        }
        match = static_cast<int>(i);
      }
    }
  }
  if (match < 0 && name.schema.empty()) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().schema, name.name)) {
        if (match >= 0) {
          return -1;
        }
        match = static_cast<int>(i);
      }
    }
  }
  return match;
}

// ---- Nested field resolution ----------------------------------------------
// GoogleSQL allows dotted references into STRUCT/PROTO values (`t.Info.x`,
// `t.Info.str_value`).  Base relations store such values as text: struct
// constructors produce JSON-like objects, proto constructors the proto text
// format.  The helpers below resolve remaining path segments against those
// encodings so a single Lookup covers plain columns and nested fields.

std::string TrimFieldToken(std::string_view s) {
  size_t begin = 0;
  size_t end = s.size();
  while (begin < end && std::isspace(static_cast<unsigned char>(s[begin]))) {
    ++begin;
  }
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(s[end - 1]))) {
    --end;
  }
  return std::string(s.substr(begin, end - begin));
}

Value ScalarFromText(std::string_view raw) {
  std::string token = TrimFieldToken(raw);
  if (token == "null" || token.empty()) {
    return {};
  }
  if (token == "true") {
    return Value(int64_t{1});
  }
  if (token == "false") {
    return Value(int64_t{0});
  }
  if (token.front() == '"' && token.back() == '"' && token.size() >= 2) {
    return Value(token.substr(1, token.size() - 2));
  }
  if (token.front() == '{' && token.back() == '}') {
    return Value(std::move(token));  // nested struct stays encoded
  }
  if (token.find('.') != std::string::npos ||
      token.find('e') != std::string::npos ||
      token.find('E') != std::string::npos) {
    try {
      return Value(std::stod(token));
    } catch (...) {
    }
  }
  try {
    return Value(static_cast<int64_t>(std::stoll(token)));
  } catch (...) {
  }
  return Value(std::move(token));
}

std::string InferElementSqlType(const std::vector<Value>& elements) {
  for (const Value& element : elements) {
    if (element.IsNull()) {
      continue;
    }
    switch (element.type) {
      case ValueType::kInt64:
        return "INT64";
      case ValueType::kDouble:
        return "DOUBLE";
      case ValueType::kVarChar:
        return "STRING";
      default:
        return {};
    }
  }
  return "INT64";
}

// Extracts `key` from a JSON object string.  Returns false when the object
// has no such key.  Array values become Value arrays.
bool JsonExtractField(std::string_view json, std::string_view key, Value* out) {
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    return false;
  }
  const std::string needle = "\"" + std::string(key) + "\":";
  auto top_level_prefix = [](std::string_view text) {
    int depth = 0;
    bool in_string = false;
    for (size_t k = 0; k < text.size(); ++k) {
      char c = text[k];
      if (in_string) {
        if (c == '\\') {
          ++k;
          continue;
        }
        if (c == '"') {
          in_string = false;
        }
        continue;
      }
      if (c == '"') {
        in_string = true;
        continue;
      }
      if (c == '{' || c == '[') {
        ++depth;
      }
      if (c == '}' || c == ']') {
        --depth;
      }
      if (depth < 0) {
        return false;
      }
    }
    return depth == 0 && !in_string;
  };
  size_t search_from = 1;
  size_t pos;
  while ((pos = json.find(needle, search_from)) != std::string_view::npos &&
         !top_level_prefix(json.substr(1, pos - 1))) {
    search_from = pos + 1;
  }
  if (pos == std::string_view::npos ||
      !top_level_prefix(json.substr(1, pos - 1))) {
    return false;
  }
  size_t value_start = pos + needle.size();
  while (value_start < json.size() &&
         std::isspace(static_cast<unsigned char>(json[value_start]))) {
    ++value_start;
  }
  size_t value_end;
  size_t raw_begin = value_start;
  if (json[value_start] == '"') {
    value_end = value_start + 1;
    while (value_end < json.size() && json[value_end] != '"') {
      if (json[value_end] == '\\') {
        ++value_end;
      }
      ++value_end;
    }
    ++value_end;
  } else if (json[value_start] == '{' || json[value_start] == '[' ||
             (json.size() - value_start > 6 &&
              json.compare(value_start, 6, "ARRAY<") == 0)) {
    // Bracketed JSON array/object, or an encoded ARRAY<T>[...] struct field
    // (Value::AsString text).  Consume through the matching close of the
    // first opening bracket so embedded commas cannot truncate the value.
    size_t scan = value_start;
    if (json[scan] == 'A') {
      const size_t bracket = json.find('[', scan);
      if (bracket == std::string_view::npos) {
        return false;
      }
      raw_begin = bracket;
      scan = bracket;
    }
    const char open = json[scan];
    const char close = open == '{' ? '}' : ']';
    int nest = 0;
    bool str = false;
    value_end = scan;
    for (; value_end < json.size(); ++value_end) {
      char c = json[value_end];
      if (str) {
        if (c == '\\' && value_end + 1 < json.size()) {
          ++value_end;
          continue;
        }
        if (c == '"') {
          str = false;
        }
        continue;
      }
      if (c == '"') {
        str = true;
        continue;
      }
      if (c == open) {
        ++nest;
      } else if (c == close) {
        --nest;
        if (nest == 0) {
          ++value_end;
          break;
        }
      }
    }
  } else {
    value_end = value_start;
    while (value_end < json.size() && json[value_end] != ',') {
      ++value_end;
    }
  }
  std::string_view raw = json.substr(raw_begin, value_end - raw_begin);
  // Struct constructors encode array-valued fields with Value::AsString(),
  // i.e. "ARRAY<ELEMENT_TYPE>[e0, e1, ...]".  Decode those into real arrays
  // so downstream UNNEST / field traversal sees array values.
  if (raw.size() > 6 && raw.substr(0, 6) == "ARRAY<") {
    const size_t bracket = raw.find('[');
    if (bracket != std::string_view::npos && raw.back() == ']') {
      std::string_view inner = raw.substr(bracket + 1, raw.size() - bracket - 2);
      std::vector<Value> elements;
      int nest = 0;
      bool str = false;
      size_t start = 0;
      for (size_t k = 0; k < inner.size(); ++k) {
        char d = inner[k];
        if (str) {
          if (d == '\\') {
            ++k;
            continue;
          }
          if (d == '"') {
            str = false;
          }
          continue;
        }
        if (d == '"') {
          str = true;
          continue;
        }
        if (d == '{' || d == '[' || d == '(') {
          ++nest;
          continue;
        }
        if (d == '}' || d == ']' || d == ')') {
          if (nest > 0) {
            --nest;
          }
          continue;
        }
        if (d == ',' && nest == 0) {
          elements.push_back(ScalarFromText(inner.substr(start, k - start)));
          start = k + 1;
        }
      }
      if (!inner.empty()) {
        elements.push_back(ScalarFromText(inner.substr(start)));
      }
      *out = Value::Array(std::move(elements), InferElementSqlType(elements));
      return true;
    }
  }
  if (!raw.empty() && raw.front() == '[') {
    // Split top-level elements and decode each.
    std::string_view inner =
        raw.substr(1, raw.size() > 1 ? raw.size() - 2 : 0);
    std::vector<Value> elements;
    int nest = 0;
    bool str = false;
    size_t start = 0;
    for (size_t k = 0; k < inner.size(); ++k) {
      char d = inner[k];
      if (str) {
        if (d == '\\') {
          ++k;
          continue;
        }
        if (d == '"') {
          str = false;
        }
        continue;
      }
      if (d == '"') {
        str = true;
        continue;
      }
      if (d == '{' || d == '[') {
        ++nest;
        continue;
      }
      if (d == '}' || d == ']') {
        --nest;
        continue;
      }
      if (d == ',' && nest == 0) {
        elements.push_back(ScalarFromText(inner.substr(start, k - start)));
        start = k + 1;
      }
    }
    if (!inner.empty()) {
      elements.push_back(ScalarFromText(inner.substr(start)));
    }
    *out = Value::Array(std::move(elements), InferElementSqlType(elements));
    return true;
  }
  *out = ScalarFromText(raw);
  return true;
}

// Proto text format: repeated `field: value` entries and `field { ... }`
// message blocks separated by whitespace, strings double-quoted.  A field
// occurring once yields a scalar (or the message's inner text so chained
// traversal continues); repeated occurrences yield an array.
bool ProtoTextExtractField(std::string_view text, std::string_view key,
                           Value* out) {
  std::vector<Value> matches;
  size_t i = 0;
  while (i < text.size()) {
    while (i < text.size() &&
           std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    if (i >= text.size()) { break; }
    if (text[i] == '{' || text[i] == '}') {
      // Stray block delimiter from an outer scan: skip it.
      ++i;
      continue;
    }
    size_t name_start = i;
    while (i < text.size() && text[i] != ':' && text[i] != '{' &&
           !std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    const std::string_view field_name = text.substr(name_start, i - name_start);
    while (i < text.size() &&
           std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    if (i < text.size() && text[i] == '{') {
      // Message block: consume through the balanced close.
      const size_t body_start = i + 1;
      int nest = 1;
      bool str = false;
      size_t j = i + 1;
      for (; j < text.size(); ++j) {
        const char c = text[j];
        if (str) {
          if (c == '\\' && j + 1 < text.size()) {
            ++j;
          } else if (c == '"') {
            str = false;
          }
          continue;
        }
        if (c == '"') {
          str = true;
        } else if (c == '{') {
          ++nest;
        } else if (c == '}') {
          if (--nest == 0) { break; }
        }
      }
      const std::string_view body =
          text.substr(body_start, j > body_start ? j - body_start : 0);
      i = j < text.size() ? j + 1 : text.size();
      if (IdentifierEquals(field_name, key)) {
        matches.emplace_back(std::string(TrimFieldToken(body)));
      }
      continue;
    }
    if (i >= text.size() || text[i] != ':') {
      // Not a `name:` entry (trailing token); stop scanning this segment.
      break;
    }
    ++i;  // skip ':'
    while (i < text.size() &&
           std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    size_t value_begin = i;
    size_t value_end;
    if (i < text.size() && text[i] == '"') {
      ++i;
      while (i < text.size() && text[i] != '"') {
        if (text[i] == '\\' && i + 1 < text.size()) {
          ++i;
        }
        ++i;
      }
      value_end = std::min(text.size(), i + 1);
      i = value_end;
    } else if (i < text.size() && text[i] == '[') {
      int nest = 0;
      bool str = false;
      size_t j = i;
      for (; j < text.size(); ++j) {
        const char c = text[j];
        if (str) {
          if (c == '\\' && j + 1 < text.size()) { ++j; } else if (c == '"') {
            str = false;
          }
          continue;
        }
        if (c == '"') {
          str = true;
        } else if (c == '[') {
          ++nest;
        } else if (c == ']') {
          if (--nest == 0) { break; }
        }
      }
      value_end = std::min(text.size(), j + 1);
      i = value_end;
    } else {
      while (i < text.size() &&
             !std::isspace(static_cast<unsigned char>(text[i]))) {
        ++i;
      }
      value_end = i;
    }
    if (IdentifierEquals(field_name, key)) {
      matches.push_back(
          ScalarFromText(text.substr(value_begin, value_end - value_begin)));
    }
  }
  if (matches.empty()) {
    return false;
  }
  if (matches.size() == 1) {
    *out = std::move(matches[0]);
  } else {
    *out = Value::Array(std::move(matches), InferElementSqlType(matches));
  }
  return true;
}

Value ResolveFieldPath(const Value& base,
                       const std::vector<std::string>& fields) {
  Value current = base;
  for (const std::string& field : fields) {
    if (current.IsNull()) {
      return {};
    }
    if (current.type != ValueType::kVarChar) {
      throw std::runtime_error("cannot access field " + field +
                               " of a non-struct value");
    }
    const std::string_view text(current.value.varchar_value);
    bool resolved = JsonExtractField(text, field, &current) ||
                    ProtoTextExtractField(text, field, &current);
    if (!resolved) {
      throw std::runtime_error("field " + field + " not found");
    }
  }
  return current;
}

std::vector<std::string> SplitDottedName(const std::string& name) {
  std::vector<std::string> parts;
  size_t start = 0;
  while (true) {
    size_t dot = name.find('.', start);
    if (dot == std::string::npos) {
      parts.push_back(name.substr(start));
      break;
    }
    parts.push_back(name.substr(start, dot - start));
    start = dot + 1;
  }
  return parts;
}

// Encodes one row as a struct-like JSON object keyed by column name.  Used to
// resolve a bare alias reference (`FROM (...) t ... SELECT t`) to the whole
// row, mirroring GoogleSQL's structural treatment of rows as structs.
Value RowAsStructValue(const Row& row, const Schema& schema) {
  auto scalar_to_json = [](const Value& v) -> std::string {
    switch (v.type) {
      case ValueType::kNull:
        return "null";
      case ValueType::kInt64:
        return std::to_string(v.value.int_value);
      case ValueType::kDouble:
        return std::to_string(v.value.double_value);
      case ValueType::kVarChar:
        return "\"" + std::string(v.value.varchar_value) + "\"";
      case ValueType::kArray: {
        std::string inner;
        const std::vector<Value>& elements = v.ArrayElements();
        for (size_t i = 0; i < elements.size(); ++i) {
          if (i > 0) {
            inner += ",";
          }
          const Value& element = elements[i];
          switch (element.type) {
            case ValueType::kNull:
              inner += "null";
              break;
            case ValueType::kInt64:
              inner += std::to_string(element.value.int_value);
              break;
            case ValueType::kDouble:
              inner += std::to_string(element.value.double_value);
              break;
            case ValueType::kVarChar:
              inner += "\"" + std::string(element.value.varchar_value) + "\"";
              break;
            default:
              inner += element.AsString();
          }
        }
        return "[" + inner + "]";
      }
      default:
        return v.AsString();
    }
  };
  std::string json = "{";
  for (size_t i = 0; i < row.values_.size(); ++i) {
    if (i > 0) {
      json += ",";
    }
    std::string key;
    if (i < schema.ColumnCount()) {
      key = schema.GetColumn(i).Name().name;
    }
    if (key.empty()) {
      key = "f" + std::to_string(i + 1);
    }
    json += "\"" + key + "\":" + scalar_to_json(row.values_[i]);
  }
  json += "}";
  return Value(std::move(json));
}

}  // namespace

Value Lookup(const ColumnName& name, const Scope& scope) {
  for (const Scope* current = &scope; current != nullptr;
       current = current->outer) {
    if (current->row == nullptr || current->schema == nullptr) {
      continue;
    }
    const int offset = FindColumn(*current->schema, name);
    if (offset >= 0) {
      return (*current->row)[static_cast<size_t>(offset)];
    }
  }
  // Dotted references (`t.Info.str_value`, `s.a`, `sub.ca.a`) may address
  // nested fields of a STRUCT/PROTO value rather than a plain column.  Split
  // the full path into segments and, per scope level (innermost outward),
  // try every split point: the leading segments form a (possibly qualified)
  // column reference and the trailing segments traverse encoded field values.
  std::vector<std::string> segments = name.schema.empty()
                                          ? SplitDottedName(name.name)
                                          : [&] {
                                            std::vector<std::string> parts =
                                                SplitDottedName(name.schema);
                                            for (std::string& part :
                                                 SplitDottedName(name.name)) {
                                              parts.push_back(std::move(part));
                                            }
                                            return parts;
                                          }();
  if (segments.size() >= 2) {
    const std::vector<std::string> fields(
        segments.begin() + 1, segments.end());
    for (const Scope* current = &scope; current != nullptr;
         current = current->outer) {
      if (current->row == nullptr || current->schema == nullptr) {
        continue;
      }
      // First segment may be an alias whose whole row is the base value.
      if (name.schema.empty()) {
        bool any_match = false;
        for (size_t i = 0; i < current->schema->ColumnCount(); ++i) {
          if (IdentifierEquals(current->schema->GetColumn(i).Name().schema,
                               segments.front())) {
            any_match = true;
            break;
          }
        }
        if (any_match) {
          return ResolveFieldPath(
              RowAsStructValue(*current->row, *current->schema), fields);
        }
      }
      // Split point k: segments[0..k] form the base column reference
      // (qualifier = segments[0..k-1], bare name = segments[k]) and
      // segments[k+1..] traverse encoded field values.  Longest prefix
      // first so explicit qualifiers win over bare-name matches.
      for (size_t split = segments.size() - 1; split-- > 0;) {
        std::string qualifier;
        for (size_t i = 0; i < split; ++i) {
          if (!qualifier.empty()) {
            qualifier += '.';
          }
          qualifier += segments[i];
        }
        const ColumnName base(qualifier, segments[split]);
        const int offset = FindColumn(*current->schema, base);
        if (offset >= 0) {
          const Value& base_value =
              (*current->row)[static_cast<size_t>(offset)];
          // Only textual STRUCT/PROTO values carry traversable fields; a
          // scalar match here came from a qualifier/alias fallback, so keep
          // searching other scopes instead of failing the whole lookup.
          if (base_value.type == ValueType::kVarChar ||
              split + 1 == segments.size()) {
            return ResolveFieldPath(
                base_value,
                std::vector<std::string>(segments.begin() +
                                             static_cast<ptrdiff_t>(split + 1),
                                         segments.end()));
          }
        }
      }
    }
  }
  // A bare alias (`SELECT t FROM (...) t`, `COUNT(t)`) denotes the row of the
  // source aliased `t`; GoogleSQL treats such rows as structs.  Collect the
  // columns that carry the alias as qualifier and encode them as a struct.
  if (name.schema.empty() && name.name != "*" &&
      name.name.find('.') == std::string::npos) {
    for (const Scope* current = &scope; current != nullptr;
         current = current->outer) {
      if (current->row == nullptr || current->schema == nullptr) {
        continue;
      }
      const Schema& schema = *current->schema;
      if (schema.ColumnCount() == 0) {
        continue;
      }
      std::vector<size_t> owned;
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        if (IdentifierEquals(schema.GetColumn(i).Name().schema, name.name)) {
          owned.push_back(i);
        }
      }
      if (owned.empty()) {
        continue;
      }
      Row aliased;
      aliased.values_.reserve(owned.size());
      for (size_t i : owned) {
        aliased.values_.push_back((*current->row)[i]);
      }
      Schema owned_schema("", [&] {
        std::vector<Column> columns;
        columns.reserve(owned.size());
        for (size_t i : owned) {
          columns.push_back(schema.GetColumn(i));
        }
        return columns;
      }());
      return RowAsStructValue(aliased, owned_schema);
    }
  }
  throw std::runtime_error("column " + name.ToString() + " not found");
}

bool Like(std::string_view value, std::string_view pattern) {
  // Fast paths for the common TPC-H shapes: 'foo%', '%foo', '%foo%'.
  if (pattern == "%") {
    return true;
  }
  if (pattern.empty()) {
    return value.empty();
  }
  const bool leading = pattern.front() == '%';
  const bool trailing = pattern.back() == '%';
  if (leading || trailing) {
    std::string_view core = pattern;
    if (leading) {
      core.remove_prefix(1);
    }
    if (trailing && !core.empty()) {
      core.remove_suffix(1);
    }
    if (core.find('%') == std::string_view::npos &&
        core.find('_') == std::string_view::npos) {
      if (leading && trailing) {
        return value.find(core) != std::string_view::npos;
      }
      if (trailing) {
        return value.starts_with(core);
      }
      if (leading) {
        return value.size() >= core.size() &&
               value.substr(value.size() - core.size()) == core;
      }
    }
  }

  size_t value_pos = 0;
  size_t pattern_pos = 0;
  size_t wildcard = std::string_view::npos;
  size_t retry = 0;
  while (value_pos < value.size()) {
    if (pattern_pos < pattern.size() &&
        (pattern[pattern_pos] == '_' ||
         pattern[pattern_pos] == value[value_pos])) {
      ++value_pos;
      ++pattern_pos;
    } else if (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
      wildcard = pattern_pos++;
      retry = value_pos;
    } else if (wildcard != std::string_view::npos) {
      pattern_pos = wildcard + 1;
      value_pos = ++retry;
    } else {
      return false;
    }
  }
  while (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
    ++pattern_pos;
  }
  return pattern_pos == pattern.size();
}

Value Binary(BinaryOperation operation, const Value& left, const Value& right) {
  // Canonical evaluation rules live in the AST evaluator (EvaluateBinary,
  // binary_expression.cpp): forwarding keeps SQL three-valued logic for
  // AND/OR/XOR, int64 overflow guards and mixed-type promotion identical
  // across the bytecode, AST and scan-filter paths (improvement3.md A6/S7).
  try {
    return EvaluateBinary(operation, left, right);
  } catch (const std::runtime_error& error) {
    // Message texts pinned by executor tests for this path; the semantics
    // above are already canonical.
    const std::string_view what = error.what();
    if (what == "LIKE requires strings") {
      throw std::runtime_error("LIKE requires string operands");
    }
    if (what.starts_with("Cannot do ")) {
      throw std::runtime_error("unsupported binary operation");
    }
    throw;
  }
}

bool ContainsAggregate(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
      return true;
    case TypeTag::kBinaryExp:
      return ContainsAggregate(expression->AsBinaryExpression().Left()) ||
             ContainsAggregate(expression->AsBinaryExpression().Right());
    case TypeTag::kUnaryExp:
      return ContainsAggregate(expression->AsUnaryExpression().Child());
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      if (ContainsAggregate(value.child_)) {
        return true;
      }
      return std::ranges::any_of(
          value.list_,
          [](const Expression& item) {  // NOLINT(misc-no-recursion)
            return ContainsAggregate(item);
          });
    }
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (ContainsAggregate(condition) || ContainsAggregate(result)) {
          return true;
        }
      }
      return ContainsAggregate(value.else_clause_);
    }
    case TypeTag::kFunctionCallExp:
      for (const Expression& argument :
           expression->AsFunctionCallExpression().Args()) {
        if (ContainsAggregate(argument)) {
          return true;
        }
      }
      return false;
    case TypeTag::kArrayExp:
      return std::ranges::any_of(
          expression->AsArrayExpression().Elements(),
          [](const Expression& element) {  // NOLINT(misc-no-recursion)
            return ContainsAggregate(element);
          });
    default:
      return false;
  }
}

namespace {

Value Aggregate(const AggregateExpression& aggregate,
                const AggregateResultMap& aggregates) {
  const auto result = aggregates.find(&aggregate);
  if (result == aggregates.end()) {
    throw std::runtime_error("aggregate was not prepared");
  }
  return result->second;
}

}  // namespace

void CollectAggregates(  // NOLINT(misc-no-recursion)
    const Expression& expression,
    std::vector<const AggregateExpression*>* aggregates,
    std::unordered_set<const AggregateExpression*>* seen) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression* aggregate = &expression->AsAggregateExpression();
    if (seen->insert(aggregate).second) {
      aggregates->push_back(aggregate);
    }
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectAggregates(child, aggregates, seen);
  }
}

AggregateAccumulator::AggregateAccumulator(const AggregateExpression* aggregate)
    : expression(aggregate),
      distinct(aggregate->Distinct() ? std::make_unique<DistinctValueSet>()
                                     : nullptr) {
  if (aggregate->GetType() == AggregationType::kArrayAgg ||
      aggregate->GetType() == AggregationType::kStringAgg ||
      aggregate->NeedsGroupContext()) {
    buffer_ = std::make_unique<std::vector<BufferedRow>>();
  }
}

namespace {

// Forward declarations for helpers defined further below (sketch codec and
// long-double conversion); the accumulator methods above use them.
long double ToLongDouble(const Value& value);
std::string FormatWeightDouble(double w);

}  // namespace

void AggregateAccumulator::ApplyCore(
    const Value& value, const std::vector<Value>& trailing_values) {
  // APPROX_TOP_COUNT counts NULL inputs and APPROX_TOP_SUM tracks values
  // whose weights are all NULL; every other aggregate ignores NULL inputs.
  const AggregationType core_type = expression->GetType();
  if (core_type == AggregationType::kApproxTopCount) {
    RecordLimitParam(trailing_values);
    ++FindOrAddTopCount(value);
    return;
  }
  if (core_type == AggregationType::kApproxTopSum) {
    RecordLimitParam(trailing_values);
    SumWeight& entry = FindOrAddTopSum(value);
    const Value& weight =
        trailing_values.empty() ? Value() : trailing_values[0];
    if (!weight.IsNull()) {
      if (weight.type == ValueType::kInt64) {
        if (weight.value.int_value < 0) {
          throw std::runtime_error(
              "APPROX_TOP_SUM does not support negative or NaN weights in "
              "the second argument; got " +
              std::to_string(weight.value.int_value));
        }
        entry.sum += static_cast<long double>(weight.value.int_value);
      } else if (weight.type == ValueType::kDouble) {
        const double w = weight.value.double_value;
        if (std::isnan(w)) {
          throw std::runtime_error(
              "APPROX_TOP_SUM does not support negative or NaN weights in "
              "the second argument; got nan");
        }
        if (w < 0) {
          throw std::runtime_error(
              "APPROX_TOP_SUM does not support negative or NaN weights in "
              "the second argument; got " +
              FormatWeightDouble(w));
        }
        entry.sum += static_cast<long double>(w);
        entry.is_double = true;
      } else if (weight.type == ValueType::kNull) {
        // fall through: NULL weight contributes nothing
      } else {
        throw std::runtime_error("numeric weight required");
      }
      ++entry.weights;
    }
    return;
  }
  // ARRAY_AGG keeps NULL elements (the compliance corpus requires them);
  // DISTINCT collapses repeated NULLs into a single element.
  if (core_type == AggregationType::kArrayAgg && value.IsNull()) {
    if (!distinct || !array_saw_null_) {
      array_saw_null_ = true;
      array_values_.push_back(value);
    }
    return;
  }
  if (value.IsNull()) {
    return;
  }
  if (distinct) {
    if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
      if (!distinct_ints) {
        distinct_ints = std::make_unique<std::unordered_set<int64_t>>();
        distinct_ints->reserve(distinct->size() + 8);
        for (const Value& seen : *distinct) {
          if (seen.type == ValueType::kInt64 || seen.type == ValueType::kDate) {
            distinct_ints->insert(seen.value.int_value);
          }
        }
        // Keep `distinct`: it still tracks non-integer values seen so far,
        // so mixed-type COUNT(DISTINCT ...) cannot double count.
      }
      if (!distinct_ints->insert(value.value.int_value).second) {
        return;
      }
    } else if (!distinct->insert(CanonicalDistinctValue(value)).second) {
      return;
    }
  }
  switch (expression->GetType()) {
    case AggregationType::kCount:
      ++count;
      break;
    case AggregationType::kSum:
      if (value.type == ValueType::kDouble) {
        total += value.value.double_value;
        total_is_double = true;
      } else if (value.type == ValueType::kInt64 ||
                 value.type == ValueType::kDate) {
        // Signed accumulation keeps negative inputs exact (GoogleSQL SUM
        // raises on INT64 overflow rather than wrapping).
        int64_t updated = 0;
        if (__builtin_add_overflow(int_total, value.value.int_value,
                                   &updated)) {
          throw std::runtime_error("integer overflow in SUM");
        }
        int_total = updated;
      } else {
        throw std::runtime_error("numeric value required");
      }
      ++count;
      break;
    case AggregationType::kAvg:
      // AVG(INT64) returns FLOAT64 and sums through a double intermediate,
      // so INT64 overflow never applies here.
      if (value.type == ValueType::kDouble) {
        total += value.value.double_value;
      } else if (value.type == ValueType::kInt64 ||
                 value.type == ValueType::kDate) {
        total += static_cast<double>(value.value.int_value);
      } else {
        throw std::runtime_error("numeric value required");
      }
      ++count;
      break;
    case AggregationType::kMin:
      if (!value.IsNull() && value.type == ValueType::kDouble &&
          std::isnan(value.value.double_value)) {
        saw_nan_ = true;
      }
      if (extreme.IsNull() || value < extreme) {
        extreme = value;
      }
      break;
    case AggregationType::kMax:
      if (!value.IsNull() && value.type == ValueType::kDouble &&
          std::isnan(value.value.double_value)) {
        saw_nan_ = true;
      }
      if (extreme.IsNull() || extreme < value) {
        extreme = value;
      }
      break;
    case AggregationType::kLogicalAnd:
      if (!value.IsNull()) {
        if (extreme.IsNull()) {
          extreme = Value(value.Truthy() ? int64_t{1} : int64_t{0});
        } else {
          extreme = Value((extreme.Truthy() && value.Truthy()) ? int64_t{1}
                                                               : int64_t{0});
        }
      }
      break;
    case AggregationType::kLogicalOr:
      if (!value.IsNull()) {
        if (extreme.IsNull()) {
          extreme = Value(value.Truthy() ? int64_t{1} : int64_t{0});
        } else {
          extreme = Value((extreme.Truthy() || value.Truthy()) ? int64_t{1}
                                                               : int64_t{0});
        }
      }
      break;
    case AggregationType::kCountIf:
      if (!value.IsNull() && Truthy(value)) {
        ++count;
      }
      break;
    case AggregationType::kArrayAgg:
    case AggregationType::kStringAgg:
      array_values_.push_back(value);
      break;
    case AggregationType::kBitAnd:
    case AggregationType::kBitOr:
    case AggregationType::kBitXor: {
      const int64_t next = value.value.int_value;
      if (!bit_saw_value_) {
        bit_acc_ = next;
        bit_saw_value_ = true;
      } else if (expression->GetType() == AggregationType::kBitAnd) {
        bit_acc_ &= next;
      } else if (expression->GetType() == AggregationType::kBitOr) {
        bit_acc_ |= next;
      } else {
        bit_acc_ ^= next;
      }
      break;
    }
    case AggregationType::kArrayConcatAgg:
      if (!value.IsArray()) {
        throw std::runtime_error("ARRAY_CONCAT_AGG requires an ARRAY argument");
      }
      if (concat_elem_type_.empty()) {
        concat_elem_type_ = value.ArrayElementSqlType();
      }
      for (const Value& element : value.ArrayElements()) {
        array_values_.push_back(element);
      }
      break;
    case AggregationType::kElementwiseSum:
    case AggregationType::kElementwiseAvg:
      ElementwiseApply(value);
      break;
    case AggregationType::kAnyValue:
      // GoogleSQL leaves the choice unspecified; keep the first non-NULL
      // input so results are deterministic.
      if (!saw_any_) {
        extreme = value;
        saw_any_ = true;
      }
      break;
    case AggregationType::kVarSamp:
    case AggregationType::kVarPop:
    case AggregationType::kStddevSamp:
    case AggregationType::kStddevPop: {
      const long double x = ToLongDouble(value);
      stat_.sx += x;
      stat_.sxx += x * x;
      ++count;
      break;
    }
    case AggregationType::kCovarSamp:
    case AggregationType::kCovarPop:
    case AggregationType::kCorr: {
      if (trailing_values.empty()) {
        throw std::runtime_error(::tinylamb::ToString(expression->GetType()) +
                                 " requires two arguments");
      }
      const Value& other = trailing_values[0];
      // Paired-row semantics: rows where either side is NULL are skipped.
      if (other.IsNull()) {
        break;
      }
      const long double y = ToLongDouble(value);
      const long double x = ToLongDouble(other);
      stat_.sy += y;
      stat_.syy += y * y;
      stat_.sx += x;
      stat_.sxx += x * x;
      stat_.sxy += x * y;
      ++count;
      break;
    }
    case AggregationType::kApproxQuantiles:
      RecordQuantileParam(trailing_values);
      quantile_values_.push_back(value);
      break;
    case AggregationType::kHllInit:
    case AggregationType::kKllInitInt64:
    case AggregationType::kKllInitUint64:
    case AggregationType::kKllInitDouble:
      SketchAdd(value, trailing_values);
      break;
    case AggregationType::kHllMerge:
    case AggregationType::kHllMergePartial:
    case AggregationType::kKllMergePartial:
      SketchMerge(value);
      break;
    case AggregationType::kApproxCountDistinct:
      if (!approx_distinct_) {
        approx_distinct_ = std::make_unique<std::unordered_set<Value>>();
      }
      approx_distinct_->insert(value);
      break;
    case AggregationType::kPercentileCont: {
      if (trailing_values.empty()) {
        throw std::runtime_error("PERCENTILE_CONT requires two arguments");
      }
      const Value& percentile = trailing_values[0];
      if (percentile.IsNull() || (percentile.type != ValueType::kInt64 &&
                                  percentile.type != ValueType::kDouble)) {
        throw std::runtime_error(
            "The second argument to PERCENTILE_CONT must be numeric");
      }
      const double p = percentile.type == ValueType::kInt64
                           ? static_cast<double>(percentile.value.int_value)
                           : percentile.value.double_value;
      if (!(p >= 0.0 && p <= 1.0)) {
        throw std::runtime_error(
            "The second argument to PERCENTILE_CONT must be between 0 and 1");
      }
      percentile_p_ = p;
      percentile_p_valid_ = true;
      if (value.type == ValueType::kDouble) {
        percentile_values_.push_back(value.value.double_value);
      } else if (value.type == ValueType::kInt64) {
        percentile_values_.push_back(
            static_cast<double>(value.value.int_value));
      } else {
        throw std::runtime_error("numeric value required");
      }
      break;
    }
  }
}

void AggregateAccumulator::ElementwiseApply(const Value& arr) {
  if (!arr.IsArray()) {
    throw std::runtime_error(
        expression->GetType() == AggregationType::kElementwiseSum
            ? "ELEMENTWISE_SUM requires an ARRAY argument"
            : "ELEMENTWISE_AVG requires an ARRAY argument");
  }
  ew_any_input_ = true;
  if (ew_input_elem_type_.empty()) {
    ew_input_elem_type_ = arr.ArrayElementSqlType();
  }
  const auto& elements = arr.ArrayElements();
  if (elements.size() > ew_len_) {
    ew_len_ = elements.size();
    ew_int_sum_.resize(ew_len_, 0);
    ew_double_sum_.resize(ew_len_, 0.0);
    ew_count_.resize(ew_len_, 0);
    ew_saw_double_.resize(ew_len_, false);
  }
  for (size_t i = 0; i < elements.size(); ++i) {
    const Value& element = elements[i];
    if (element.IsNull()) {
      continue;
    }
    ++ew_count_[i];
    if (element.type == ValueType::kDouble) {
      ew_saw_double_[i] = true;
      ew_double_sum_[i] += element.value.double_value;
    } else {
      ew_int_sum_[i] += element.value.int_value;
    }
  }
}

void AggregateAccumulator::RecordQuantileParam(
    const std::vector<Value>& trailing_values) {
  if (trailing_values.empty()) {
    throw std::runtime_error("APPROX_QUANTILES requires two arguments");
  }
  const Value& number = trailing_values[0];
  if (number.IsNull()) {
    throw std::runtime_error(
        "The second argument to APPROX_QUANTILES function must not be NULL");
  }
  if (number.type != ValueType::kInt64 && number.type != ValueType::kDouble) {
    throw std::runtime_error(
        "The second argument to APPROX_QUANTILES function must be an integer");
  }
  const int64_t n = number.type == ValueType::kInt64
                        ? number.value.int_value
                        : static_cast<int64_t>(number.value.double_value);
  if (n < 1) {
    throw std::runtime_error(
        "The second argument to APPROX_QUANTILES function must be positive");
  }
  if (n > 100000) {
    throw std::runtime_error(
        "The second argument to APPROX_QUANTILES function cannot be greater "
        "than 100000");
  }
  quantile_count_ = n;
  quantile_count_valid_ = true;
}

void AggregateAccumulator::RecordLimitParam(
    const std::vector<Value>& trailing_values) {
  if (trailing_values.empty()) {
    return;
  }
  const Value& number = trailing_values.back();
  if (number.IsNull()) {
    throw std::runtime_error(
        "The second argument to APPROX_TOP function must not be NULL");
  }
  int64_t n = 0;
  if (number.type == ValueType::kInt64) {
    n = number.value.int_value;
  } else if (number.type == ValueType::kDouble) {
    n = static_cast<int64_t>(number.value.double_value);
  } else {
    throw std::runtime_error(
        "The second argument to APPROX_TOP must be an integer");
  }
  const std::string which =
      expression->GetType() == AggregationType::kApproxTopSum
          ? "APPROX_TOP_SUM"
          : "APPROX_TOP_COUNT";
  if (n < 1) {
    throw std::runtime_error("The second argument to " + which +
                             " function must be positive");
  }
  if (n > 100000) {
    throw std::runtime_error("The second argument to " + which +
                             " function cannot be greater than 100000");
  }
  top_count_limit_ = n;
  top_count_valid_ = true;
}

AggregateAccumulator::SumWeight& AggregateAccumulator::FindOrAddTopSum(
    const Value& value) {
  for (SumWeight& entry : top_sums_) {
    if ((entry.value.IsNull() && value.IsNull()) ||
        (!entry.value.IsNull() && !value.IsNull() && entry.value == value)) {
      return entry;
    }
  }
  SumWeight created;
  created.value = value;
  top_sums_.push_back(std::move(created));
  return top_sums_.back();
}

int64_t& AggregateAccumulator::FindOrAddTopCount(const Value& value) {
  for (size_t i = 0; i < top_count_values_.size(); ++i) {
    const Value& seen = top_count_values_[i];
    if ((seen.IsNull() && value.IsNull()) ||
        (!seen.IsNull() && !value.IsNull() && seen == value)) {
      return top_count_counts_[i];
    }
  }
  top_count_values_.push_back(value);
  top_count_counts_.push_back(0);
  return top_count_counts_.back();
}

namespace {

// Long-double conversion shared by the statistical accumulators.
long double ToLongDouble(const Value& value) {
  if (value.type == ValueType::kDouble) {
    return static_cast<long double>(value.value.double_value);
  }
  if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
    return static_cast<long double>(value.value.int_value);
  }
  throw std::runtime_error("numeric value required");
}

std::string FormatWeightDouble(double w) {
  if (std::isnan(w)) {
    return "nan";
  }
  if (std::isinf(w)) {
    return w > 0 ? "inf" : "-inf";
  }
  char buffer[64];
  auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), w);
  (void)ec;
  return std::string(buffer, ptr - buffer);
}

// ---------------------------------------------------------------------------
// HLL_COUNT / KLL_QUANTILES sketches.
//
// The compliance corpus pins exact small-input outputs (EXTRACT over a few
// values returns the exact distinct count / exact quantiles), so the sketch
// keeps an exact multiset of the ingested values. Payloads are hex-encoded
// before they ride through BYTES columns.
// ---------------------------------------------------------------------------

constexpr char kSketchMagic[] = "TLHX1";

int SketchTypeCode(AggregationType type, const Value& first) {
  switch (type) {
    case AggregationType::kKllInitInt64:
      return 1;
    case AggregationType::kKllInitUint64:
      return 2;
    case AggregationType::kKllInitDouble:
      return 3;
    default:
      switch (first.type) {
        case ValueType::kInt64:
          return 1;
        case ValueType::kDouble:
          return 3;
        case ValueType::kDate:
          return 6;
        case ValueType::kVarChar:
          return 4;
        default:
          return 4;
      }
  }
}

std::string SqlTypeNameForSketchCode(int code) {
  switch (code) {
    case 1:
      return "INT64";
    case 2:
      return "UINT64";
    case 3:
      return "DOUBLE";
    case 6:
      return "DATE";
    default:
      return "STRING";
  }
}

std::string HexEncode(std::string_view raw) {
  static const char kDigits[] = "0123456789abcdef";
  std::string out;
  out.reserve(raw.size() * 2);
  for (const char c : raw) {
    const unsigned char byte = static_cast<unsigned char>(c);
    out.push_back(kDigits[byte >> 4]);
    out.push_back(kDigits[byte & 0xF]);
  }
  return out;
}

int HexDigit(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  return -1;
}

bool HexDecode(std::string_view hex, std::string* out) {
  if (hex.size() % 2 != 0) {
    return false;
  }
  out->clear();
  out->reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    const int hi = HexDigit(hex[i]);
    const int lo = HexDigit(hex[i + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

void AppendU32(std::string* out, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    out->push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

uint32_t ReadU32(std::string_view raw, size_t pos) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(
        static_cast<unsigned char>(raw[pos + static_cast<size_t>(i)])
        << (i * 8));
  }
  return v;
}

void AppendU64(std::string* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

uint64_t ReadU64(std::string_view raw, size_t pos) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(
             static_cast<unsigned char>(raw[pos + static_cast<size_t>(i)]))
         << (i * 8);
  }
  return v;
}

// Encodes one sketch payload: magic + typecode + precision + entries.
std::string EncodeSketch(int typecode, int64_t precision,
                         const std::vector<std::string>& entries) {
  std::string raw;
  raw += kSketchMagic;
  raw.push_back(static_cast<char>(typecode));
  AppendU64(&raw, static_cast<uint64_t>(precision));
  AppendU64(&raw, static_cast<uint64_t>(entries.size()));
  for (const std::string& entry : entries) {
    AppendU32(&raw, static_cast<uint32_t>(entry.size()));
    raw += entry;
  }
  return HexEncode(raw);
}

bool DecodeSketch(std::string_view bytes, int* typecode, int64_t* precision,
                  std::vector<std::string>* entries) {
  std::string raw;
  if (!HexDecode(bytes, &raw)) {
    return false;
  }
  constexpr size_t kHeader = 5 + 1 + 8 + 8;
  if (raw.size() < kHeader || raw.compare(0, 5, kSketchMagic) != 0) {
    return false;
  }
  *typecode = static_cast<unsigned char>(raw[5]);
  *precision = static_cast<int64_t>(ReadU64(raw, 6));
  const uint64_t count = ReadU64(raw, 14);
  size_t pos = 22;
  entries->clear();
  entries->reserve(static_cast<size_t>(std::min<uint64_t>(count, 1u << 20)));
  for (uint64_t i = 0; i < count; ++i) {
    if (pos + 4 > raw.size()) {
      return false;
    }
    const uint32_t len = ReadU32(raw, pos);
    pos += 4;
    if (pos + len > raw.size()) {
      return false;
    }
    entries->emplace_back(raw.substr(pos, len));
    pos += len;
  }
  return true;
}

// Renders a decoded sketch entry back into a Value with the sketch's type.
Value SketchEntryToValue(int typecode, const std::string& entry) {
  switch (typecode) {
    case 1:
    case 2: {
      int64_t v = 0;
      if (entry.size() == sizeof(int64_t)) {
        std::memcpy(&v, entry.data(), sizeof(int64_t));
      }
      return Value(v);
    }
    case 3: {
      double v = 0;
      if (entry.size() == sizeof(double)) {
        std::memcpy(&v, entry.data(), sizeof(double));
      }
      return Value(v);
    }
    case 6:
      return Value::DateFromDays(entry.size() == sizeof(int64_t) ? [&] {
        int64_t v = 0;
        std::memcpy(&v, entry.data(), sizeof(int64_t));
        return v;
      }()
                                                                 : int64_t{0});
    default:
      return Value(std::string(entry));
  }
}

std::string ValueToSketchEntry(int typecode, const Value& value) {
  switch (typecode) {
    case 1:
    case 2: {
      int64_t v = value.type == ValueType::kDouble
                      ? static_cast<int64_t>(value.value.double_value)
                      : value.value.int_value;
      std::string out(sizeof(int64_t), '\0');
      std::memcpy(out.data(), &v, sizeof(int64_t));
      return out;
    }
    case 3: {
      double v = value.type == ValueType::kDouble
                     ? value.value.double_value
                     : static_cast<double>(value.value.int_value);
      std::string out(sizeof(double), '\0');
      std::memcpy(out.data(), &v, sizeof(double));
      return out;
    }
    case 6: {
      const int64_t days = value.type == ValueType::kDate
                               ? value.DateDays()
                               : value.value.int_value;
      std::string out(sizeof(int64_t), '\0');
      std::memcpy(out.data(), &days, sizeof(int64_t));
      return out;
    }
    default:
      if (value.type == ValueType::kVarChar) {
        return std::string(value.value.varchar_value);
      }
      return value.AsString();
  }
}

std::string SketchBytesOf(const Value& sketch) {
  if (sketch.type == ValueType::kVarChar) {
    return std::string(sketch.value.varchar_value);
  }
  return sketch.AsString();
}

// KLL_QUANTILES.EXTRACT_<T>: decode a sketch and emit number+1 ordered
// quantiles covering min..max.
Value ExtractSketchQuantilesStatic(const Value& sketch, int64_t number) {
  int typecode = 0;
  int64_t precision = 0;
  std::vector<std::string> entries;
  if (!DecodeSketch(SketchBytesOf(sketch), &typecode, &precision, &entries)) {
    throw std::runtime_error("Invalid or incompatible sketch");
  }
  std::vector<Value> values;
  values.reserve(entries.size());
  for (const std::string& entry : entries) {
    values.push_back(SketchEntryToValue(typecode, entry));
  }
  std::sort(values.begin(), values.end(),
            [](const Value& a, const Value& b) { return a < b; });
  const int64_t n = static_cast<int64_t>(values.size());
  std::vector<Value> picked;
  for (int64_t i = 0; i <= number; ++i) {
    const int64_t index =
        n == 0 ? -1 : std::min<int64_t>((i * n) / number, n - 1);
    picked.push_back(index >= 0 ? values[static_cast<size_t>(index)] : Value());
  }
  return Value::Array(std::move(picked), SqlTypeNameForSketchCode(typecode));
}

}  // namespace

namespace {

// Recovers UTC instant text for engine-rendered timestamps (which carry a
// session-shifted wall clock under a "+00"-style label); defined below with
// the civil-time helpers.
std::string NormalizeTimestampText(const std::string& text);

// Renders one APPROX_TOP_* entry as a positional struct literal text. The
// compliance matcher compares struct payloads field-wise against exactly
// this shape.
std::string TopEntryString(const Value& value, const std::string& metric_text) {
  std::string out = "{";
  if (value.IsNull()) {
    out += "null";
  } else if (value.type == ValueType::kVarChar) {
    // Raw content: strings match quoted expectations and BYTES match b""
    // expectations through the same unquoting rules.
    out += NormalizeTimestampText(std::string(value.value.varchar_value));
  } else if (value.type == ValueType::kDate) {
    out += FormatDateDays(value.value.int_value);
  } else {
    out += value.AsString();
  }
  out += ", ";
  out += metric_text;
  out += "}";
  return out;
}

// Element SQL type for APPROX_QUANTILES output arrays. Boolean inputs arrive
// as the strings "true"/"false" (UNNEST of ARRAY<BOOL> materializes them that
// way) and are reported with GoogleSQL BOOL naming.
std::string QuantilesElementTypeName(const std::vector<Value>& values) {
  bool any = false;
  bool all_bool_text = true;
  for (const Value& value : values) {
    if (value.IsNull()) {
      continue;
    }
    if (value.type != ValueType::kVarChar) {
      all_bool_text = false;
      break;
    }
    const std::string_view text = value.value.varchar_value;
    if (text != "true" && text != "false") {
      all_bool_text = false;
      break;
    }
    any = true;
  }
  if (any && all_bool_text) {
    return "BOOL";
  }
  for (const Value& value : values) {
    if (value.IsNull()) {
      continue;
    }
    switch (value.type) {
      case ValueType::kInt64:
        return "INT64";
      case ValueType::kDouble:
        return "DOUBLE";
      case ValueType::kVarChar:
        return "STRING";
      case ValueType::kDate:
        return "DATE";
      default:
        return "STRING";
    }
  }
  return "INT64";
}

}  // namespace

void AggregateAccumulator::SketchAdd(
    const Value& value, const std::vector<Value>& trailing_values) {
  const AggregationType type = expression->GetType();
  if (type == AggregationType::kHllInit ||
      type == AggregationType::kKllInitInt64 ||
      type == AggregationType::kKllInitUint64 ||
      type == AggregationType::kKllInitDouble) {
    // Optional trailing precision parameter.
    if (!trailing_values.empty()) {
      const Value& precision = trailing_values[0];
      if (precision.IsNull()) {
        // KLL treats a NULL precision as "no sketch at all".
        saw_null_param_ = true;
        return;
      }
      const int64_t p = precision.value.int_value;
      if (p < 1 || p > 200000000) {
        throw std::runtime_error(
            "KLL failed: Provided inv_eps:" + std::to_string(p) +
            " but inv_eps needs to be >= 1 and <= 200000000.");
      }
      sketch_precision_ = std::max(sketch_precision_, p);
    }
    if (sketch_type_ == 0) {
      sketch_type_ = SketchTypeCode(type, value);
    }
    sketch_values_.push_back(ValueToSketchEntry(sketch_type_, value));
  }
}

void AggregateAccumulator::SketchMerge(const Value& sketch_bytes) {
  int typecode = 0;
  int64_t precision = 0;
  std::vector<std::string> entries;
  if (!DecodeSketch(SketchBytesOf(sketch_bytes), &typecode, &precision,
                    &entries)) {
    throw std::runtime_error("Invalid or incompatible sketch");
  }
  if (sketch_type_ != 0 && sketch_type_ != typecode) {
    throw std::runtime_error("Invalid or incompatible sketch");
  }
  sketch_type_ = typecode;
  sketch_precision_ = std::max(sketch_precision_, precision);
  sketch_values_.insert(sketch_values_.end(),
                        std::make_move_iterator(entries.begin()),
                        std::make_move_iterator(entries.end()));
}

Value AggregateAccumulator::FinishSketch(bool extract_count) const {
  if (sketch_type_ == 0 || saw_null_param_) {
    return {};
  }
  const bool distinct_entries =
      expression->GetType() == AggregationType::kHllInit ||
      expression->GetType() == AggregationType::kHllMerge ||
      expression->GetType() == AggregationType::kHllMergePartial;
  std::vector<std::string> entries;
  if (distinct_entries) {
    std::set<std::string> distinct(sketch_values_.begin(),
                                   sketch_values_.end());
    entries.assign(distinct.begin(), distinct.end());
  } else {
    // KLL sketches are multisets: duplicate inputs shift quantiles.
    entries = sketch_values_;
    std::sort(entries.begin(), entries.end());
  }
  std::string encoded = EncodeSketch(sketch_type_, sketch_precision_, entries);
  if (extract_count) {
    return Value(static_cast<int64_t>(entries.size()));
  }
  return Value(std::move(encoded));
}

void AggregateAccumulator::Add(const AggregateInput& input) {
  if (!buffer_) {
    ApplyCore(input.value, input.trailing_values);
    return;
  }
  buffer_->push_back(BufferedRow{input.value, input.order_keys, input.condition,
                                 input.auxiliary, input.trailing_values});
}

void AggregateAccumulator::Add(const Value& value) {
  if (buffer_) {
    buffer_->push_back(BufferedRow{value, {}, Value(), Value(), {}});
    return;
  }
  ApplyCore(value);
}

Value AggregateAccumulator::Finish() const {
  // Replay buffered rows: gate through the HAVING modifier, apply inner
  // ORDER BY / LIMIT, then feed the survivors into the streaming logic
  // (or build the ARRAY/STRING payload directly).
  if (buffer_ != nullptr) {
    std::vector<BufferedRow>& rows = *buffer_;
    if (expression->Having() != AggregateHavingModifier::kNone) {
      bool saw_threshold = false;
      Value threshold;
      for (const BufferedRow& row : rows) {
        if (row.condition.IsNull()) {
          continue;
        }
        if (!saw_threshold) {
          threshold = row.condition;
          saw_threshold = true;
        } else if (expression->Having() == AggregateHavingModifier::kMax) {
          if (threshold < row.condition) {
            threshold = row.condition;
          }
        } else if (row.condition < threshold) {
          threshold = row.condition;
        }
      }
      std::vector<BufferedRow> gated;
      for (BufferedRow& row : rows) {
        if (saw_threshold && !row.condition.IsNull() &&
            row.condition == threshold) {
          gated.push_back(std::move(row));
        }
      }
      rows = std::move(gated);
    }
    if (!expression->InnerOrderBy().empty()) {
      const auto& terms = expression->InnerOrderBy();
      std::stable_sort(rows.begin(), rows.end(),
                       [&](const BufferedRow& left, const BufferedRow& right) {
                         for (size_t t = 0; t < terms.size(); ++t) {
                           const WindowOrderTerm& term = terms[t];
                           const Value& a = t < left.order_keys.size()
                                                ? left.order_keys[t]
                                                : Value();
                           const Value& b = t < right.order_keys.size()
                                                ? right.order_keys[t]
                                                : Value();
                           const bool a_null = a.IsNull();
                           const bool b_null = b.IsNull();
                           if (a_null && b_null) {
                             continue;
                           }
                           const bool nulls_first =
                               term.nulls_first.value_or(term.ascending);
                           if (a_null) {
                             return nulls_first;
                           }
                           if (b_null) {
                             return !nulls_first;
                           }
                           {
                             const int c = CompareForOrderBy(a, b);
                             if (c == 0) { return false;
}
                             const bool a_less = c < 0;
                             return term.ascending ? a_less : !a_less;
                           }
                         }
                         return false;
                       });
    }
    if (expression->InnerLimit().has_value()) {
      const size_t limit = *expression->InnerLimit();
      if (rows.size() > limit) {
        rows.resize(limit);
      }
    }
    for (const BufferedRow& row : rows) {
      // Finish() is const by contract; replay mutates only scratch state.
      const_cast<AggregateAccumulator*>(this)->ApplyCore(row.value,
                                                         row.trailing_values);
      // STRING_AGG delimiter comes from the first buffered row.
      if (expression->GetType() == AggregationType::kStringAgg &&
          !delimiter_.has_value() && !row.auxiliary.IsNull()) {
        // Raw text: AsString wraps VARCHAR values in display quotes.
        const Value& delim = row.auxiliary;
        delimiter_ = delim.type == ValueType::kVarChar
                         ? std::string(delim.value.varchar_value)
                         : delim.AsString();
      }
    }
    buffer_.reset();  // replayed
  }
  switch (expression->GetType()) {
    case AggregationType::kCount:
      return Value(count);
    case AggregationType::kAvg:
      return count == 0 ? Value()
                        : Value((total + static_cast<double>(int_total)) /
                                static_cast<double>(count));
    case AggregationType::kSum:
      if (count == 0) {
        return {};
      }
      if (!total_is_double && total == 0.0) {
        return Value(static_cast<int64_t>(int_total));
      }
      return Value(total + static_cast<double>(int_total));
    case AggregationType::kMin:
    case AggregationType::kMax:
      // GoogleSQL: any NaN in the group makes MIN/MAX NaN.
      if (saw_nan_) {
        return Value(std::numeric_limits<double>::quiet_NaN());
      }
      return extreme;
    case AggregationType::kLogicalAnd:
    case AggregationType::kLogicalOr:
      // No non-NULL input: LOGICAL_AND/OR are NULL, not vacuously true.
      return extreme.IsNull() ? Value() : extreme;
    case AggregationType::kArrayAgg: {
      // An empty input produces a NULL array, not an empty one.
      if (array_values_.empty()) {
        return {};
      }
      // Prefer the statically inferred element type (BOOL/INT32/... are
      // indistinguishable from INT64 in runtime values).
      std::string element_type = expression->ArrayElementSqlType();
      if (element_type.empty()) {
        for (const Value& value : array_values_) {
          if (!value.IsNull()) {
            element_type = ElementSqlTypeName(value.type);
            break;
          }
        }
      }
      return Value::Array(array_values_, std::move(element_type));
    }
    case AggregationType::kStringAgg: {
      // No rows: STRING_AGG is NULL, not the empty string.
      if (array_values_.empty()) {
        return {};
      }
      std::string out;
      for (size_t i = 0; i < array_values_.size(); ++i) {
        if (i) {
          out += delimiter_.value_or(",");
        }
        // Raw text, not AsString(): AsString wraps VARCHAR values in quotes.
        const Value& element = array_values_[i];
        if (element.IsNull()) { continue;
}
        out += element.type == ValueType::kVarChar
                   ? std::string(element.value.varchar_value)
                   : element.AsString();
      }
      return Value(std::move(out));
    }
    case AggregationType::kCountIf:
      return Value(count);
    case AggregationType::kAnyValue:
      return saw_any_ ? extreme : Value();
    case AggregationType::kVarPop: {
      if (count == 0) {
        return {};
      }
      const long double mean = stat_.sx / count;
      const long double ssd = stat_.sxx - count * mean * mean;
      return Value(static_cast<double>(ssd / count));
    }
    case AggregationType::kVarSamp: {
      if (count < 2) {
        return {};
      }
      const long double mean = stat_.sx / count;
      const long double ssd = stat_.sxx - count * mean * mean;
      return Value(static_cast<double>(ssd / (count - 1)));
    }
    case AggregationType::kStddevPop: {
      if (count == 0) {
        return {};
      }
      const long double mean = stat_.sx / count;
      const long double ssd = stat_.sxx - count * mean * mean;
      return Value(static_cast<double>(std::sqrt(ssd / count)));
    }
    case AggregationType::kStddevSamp: {
      if (count < 2) {
        return {};
      }
      const long double mean = stat_.sx / count;
      const long double ssd = stat_.sxx - count * mean * mean;
      return Value(static_cast<double>(std::sqrt(ssd / (count - 1))));
    }
    case AggregationType::kCovarPop: {
      if (count == 0) {
        return {};
      }
      const long double mx = stat_.sx / count;
      const long double my = stat_.sy / count;
      const long double sdd = stat_.sxy - count * mx * my;
      return Value(static_cast<double>(sdd / count));
    }
    case AggregationType::kCovarSamp: {
      if (count < 2) {
        return {};
      }
      const long double mx = stat_.sx / count;
      const long double my = stat_.sy / count;
      const long double sdd = stat_.sxy - count * mx * my;
      return Value(static_cast<double>(sdd / (count - 1)));
    }
    case AggregationType::kCorr: {
      if (count < 2) {
        return {};
      }
      const long double mx = stat_.sx / count;
      const long double my = stat_.sy / count;
      const long double sdd = stat_.sxy - count * mx * my;
      const long double vx = stat_.sxx - count * mx * mx;
      const long double vy = stat_.syy - count * my * my;
      return Value(static_cast<double>(sdd / std::sqrt(vx * vy)));
    }
    case AggregationType::kApproxQuantiles: {
      // No rows reached the aggregate (empty input): the result is a NULL
      // array rather than an error.
      if (!quantile_count_valid_) {
        return {};
      }
      const int64_t number = quantile_count_;
      if (quantile_values_.empty()) {
        return {};
      }
      std::sort(quantile_values_.begin(), quantile_values_.end(),
                [](const Value& a, const Value& b) { return a < b; });
      std::vector<Value> picked;
      const int64_t n = static_cast<int64_t>(quantile_values_.size());
      for (int64_t i = 0; i <= number; ++i) {
        const int64_t index = std::min<int64_t>((i * n) / number, n - 1);
        picked.push_back(quantile_values_[static_cast<size_t>(index)]);
      }
      std::string element_type = QuantilesElementTypeName(quantile_values_);
      if (element_type == "BOOL") {
        // Boolean inputs materialize as text; report canonical INT64
        // truth values so downstream BOOL handling sees real booleans.
        for (Value& value : picked) {
          if (value.type == ValueType::kVarChar) {
            value = Value(value.value.varchar_value == "true" ? int64_t{1}
                                                              : int64_t{0});
          }
        }
      }
      return Value::Array(std::move(picked), std::move(element_type));
    }
    case AggregationType::kApproxTopCount: {
      if (!top_count_valid_ || top_count_values_.empty()) {
        return {};
      }
      const int64_t number = top_count_limit_;
      std::vector<size_t> order(top_count_values_.size());
      for (size_t i = 0; i < order.size(); ++i) {
        order[i] = i;
      }
      std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return top_count_counts_[a] > top_count_counts_[b];
      });
      std::vector<Value> elements;
      for (size_t idx : order) {
        if (static_cast<int64_t>(elements.size()) >= number) {
          break;
        }
        elements.push_back(Value(TopEntryString(
            top_count_values_[idx], std::to_string(top_count_counts_[idx]))));
      }
      // Struct entries ride as text payloads; the "STRING" element type
      // lets the compliance matcher compare struct content field-wise.
      return Value::Array(std::move(elements), "STRING");
    }
    case AggregationType::kApproxTopSum: {
      if (!top_count_valid_ || top_sums_.empty()) {
        return {};
      }
      const int64_t number = top_count_limit_;
      std::vector<size_t> order(top_sums_.size());
      for (size_t i = 0; i < order.size(); ++i) {
        order[i] = i;
      }
      // Highest sum first; entries whose weights were all NULL rank last.
      std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        const SumWeight& left = top_sums_[a];
        const SumWeight& right = top_sums_[b];
        if (left.weights == 0 || right.weights == 0) {
          return left.weights != 0 && right.weights == 0;
        }
        return left.sum > right.sum;
      });
      bool any_double = false;
      for (const SumWeight& weight : top_sums_) {
        any_double = any_double || weight.is_double;
      }
      std::vector<Value> elements;
      for (size_t idx : order) {
        if (static_cast<int64_t>(elements.size()) >= number) {
          break;
        }
        const SumWeight& weight = top_sums_[idx];
        std::string sum_text;
        if (weight.weights == 0) {
          sum_text = "NULL";
        } else {
          const double as_double = static_cast<double>(weight.sum);
          sum_text = any_double
                         ? FormatWeightDouble(as_double)
                         : std::to_string(static_cast<int64_t>(weight.sum));
        }
        elements.push_back(
            Value(TopEntryString(weight.value, std::move(sum_text))));
      }
      return Value::Array(std::move(elements), "STRING");
    }
    case AggregationType::kBitAnd:
    case AggregationType::kBitOr:
    case AggregationType::kBitXor:
      return bit_saw_value_ ? Value(bit_acc_) : Value();
    case AggregationType::kArrayConcatAgg:
      if (concat_elem_type_.empty() && array_values_.empty()) {
        // No non-NULL input arrays reached the accumulator: NULL array.
        return {};
      }
      if (concat_elem_type_.empty()) {
        concat_elem_type_ = "INT64";
      }
      return Value::Array(array_values_, concat_elem_type_);
    case AggregationType::kElementwiseSum:
    case AggregationType::kElementwiseAvg: {
      const bool is_avg =
          expression->GetType() == AggregationType::kElementwiseAvg;
      if (!ew_any_input_) {
        // All input arrays were NULL: NULL result.
        return {};
      }
      auto mapped_elem_type = [](const std::string& input) {
        std::string upper;
        upper.reserve(input.size());
        for (char c : input) {
          upper.push_back(
              static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
        }
        if (upper.find("UINT") != std::string::npos) {
          return "UINT64";
        }
        if (upper.find("DOUBLE") != std::string::npos ||
            upper.find("FLOAT") != std::string::npos) {
          return "DOUBLE";
        }
        return "INT64";
      };
      std::vector<Value> elements;
      elements.reserve(ew_len_);
      for (size_t i = 0; i < ew_len_; ++i) {
        if (ew_count_[i] == 0) {
          elements.push_back(Value());
          continue;
        }
        if (is_avg || ew_saw_double_[i]) {
          const double sum =
              ew_double_sum_[i] + static_cast<double>(ew_int_sum_[i]);
          elements.push_back(
              is_avg ? Value(sum / static_cast<double>(ew_count_[i]))
                     : Value(sum));
        } else {
          elements.push_back(Value(ew_int_sum_[i]));
        }
      }
      const std::string elem_type =
          is_avg ? "DOUBLE" : mapped_elem_type(ew_input_elem_type_);
      return Value::Array(std::move(elements), elem_type);
    }
    case AggregationType::kHllMerge:
      if (sketch_type_ == 0) {
        return Value(int64_t{0});
      }
      return FinishSketch(/*extract_count=*/true);
    case AggregationType::kHllMergePartial:
    case AggregationType::kKllMergePartial:
      return FinishSketch(/*extract_count=*/false);
    case AggregationType::kHllInit:
    case AggregationType::kKllInitInt64:
    case AggregationType::kKllInitUint64:
    case AggregationType::kKllInitDouble:
      return FinishSketch(/*extract_count=*/false);
    case AggregationType::kApproxCountDistinct:
      return Value(static_cast<int64_t>(
          approx_distinct_ ? approx_distinct_->size() : 0));
    case AggregationType::kPercentileCont: {
      // GoogleSQL semantics: NULL inputs are ignored, NaN sorts before every
      // other value (including -inf), and the result is linear interpolation
      // over the sorted group values.
      if (!percentile_p_valid_ || percentile_values_.empty()) {
        return {};
      }
      const auto nan_first = [](double x, double y) {
        const bool xn = std::isnan(x);
        const bool yn = std::isnan(y);
        if (xn || yn) {
          return xn && !yn;
        }
        return x < y;
      };
      std::sort(percentile_values_.begin(), percentile_values_.end(),
                nan_first);
      const double position =
          percentile_p_ * static_cast<double>(percentile_values_.size() - 1);
      const size_t low = static_cast<size_t>(position);
      const size_t high = low + 1 < percentile_values_.size() ? low + 1 : low;
      const double a = percentile_values_[low];
      const double b = percentile_values_[high];
      // Guard the degenerate fractions: 0 * inf is NaN, so exact positions
      // must return the boundary value untouched. The weighted form
      // a*(1-f) + b*f matches the reference engine on infinite bounds.
      const double fraction = position - static_cast<double>(low);
      if (fraction <= 0.0) {
        return Value(a);
      }
      if (fraction >= 1.0 || a == b) {
        return Value(b);
      }
      return Value(a * (1.0 - fraction) + b * fraction);
    }
  }
  return {};
}

namespace {

struct CivilTime {
  int year{1970};
  int month{1};
  int day{1};
  int hour{0};
  int minute{0};
  int second{0};
  int64_t subsecond_nanos{0};
};

bool ParseCivilTime(std::string_view s, CivilTime* ct) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
    s.remove_prefix(1);
  }
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
    s.remove_suffix(1);
  }
  if (s.empty()) {
    return false;
  }
  int Y = 0, M = 0, D = 0;
  if (s.size() == 10 &&
      sscanf(std::string(s).c_str(), "%d-%d-%d", &Y, &M, &D) == 3) {
    ct->year = Y;
    ct->month = M;
    ct->day = D;
    ct->hour = 0;
    ct->minute = 0;
    ct->second = 0;
    ct->subsecond_nanos = 0;
    return true;
  }
  int h = 0, m = 0, sec = 0;
  char sep = ' ';
  if (sscanf(std::string(s).c_str(), "%d-%d-%d%c%d:%d:%d", &Y, &M, &D, &sep, &h,
             &m, &sec) >= 6) {
    ct->year = Y;
    ct->month = M;
    ct->day = D;
    ct->hour = h;
    ct->minute = m;
    ct->second = sec;
    ct->subsecond_nanos = 0;
    size_t dot = s.find('.', 11);
    if (dot != std::string_view::npos) {
      size_t end_digits = dot + 1;
      while (end_digits < s.size() && s[end_digits] >= '0' &&
             s[end_digits] <= '9') {
        ++end_digits;
      }
      std::string frac_str(s.substr(dot + 1, end_digits - (dot + 1)));
      while (frac_str.size() < 9) {
        frac_str.push_back('0');
      }
      if (frac_str.size() > 9) {
        frac_str = frac_str.substr(0, 9);
      }
      ct->subsecond_nanos = std::stoll(frac_str);
    }
    return true;
  }
  if (sscanf(std::string(s).c_str(), "%d:%d:%d", &h, &m, &sec) >= 3) {
    ct->year = 1970;
    ct->month = 1;
    ct->day = 1;
    ct->hour = h;
    ct->minute = m;
    ct->second = sec;
    ct->subsecond_nanos = 0;
    size_t dot = s.find('.');
    if (dot != std::string_view::npos) {
      size_t end_digits = dot + 1;
      while (end_digits < s.size() && s[end_digits] >= '0' &&
             s[end_digits] <= '9') {
        ++end_digits;
      }
      std::string frac_str(s.substr(dot + 1, end_digits - (dot + 1)));
      while (frac_str.size() < 9) {
        frac_str.push_back('0');
      }
      if (frac_str.size() > 9) {
        frac_str = frac_str.substr(0, 9);
      }
      ct->subsecond_nanos = std::stoll(frac_str);
    }
    return true;
  }
  return false;
}

CivilTime ShiftCivilTimeHours(const CivilTime& ct, int offset_hours) {
  CivilTime res = ct;
  int total_hours = res.hour + offset_hours;
  int day_diff = 0;
  if (total_hours >= 0) {
    day_diff = total_hours / 24;
    res.hour = total_hours % 24;
  } else {
    day_diff = (total_hours - 23) / 24;
    res.hour = (total_hours % 24 + 24) % 24;
  }
  if (day_diff != 0) {
    std::chrono::year_month_day ymd{
        std::chrono::year{res.year},
        std::chrono::month{static_cast<unsigned>(res.month)},
        std::chrono::day{static_cast<unsigned>(res.day)}};
    int64_t days =
        std::chrono::sys_days{ymd}.time_since_epoch().count() + day_diff;
    std::chrono::sys_days new_sd{std::chrono::days{days}};
    std::chrono::year_month_day new_ymd{new_sd};
    res.year = int(new_ymd.year());
    res.month = unsigned(new_ymd.month());
    res.day = unsigned(new_ymd.day());
  }
  return res;
}

int ParseTimeZoneOffset(std::string_view tz_str, const CivilTime* ct = nullptr,
                        int default_offset = 0) {
  if (tz_str.empty()) {
    return default_offset;
  }
  if (tz_str == "UTC" || tz_str == "GMT" || tz_str == "utc" ||
      tz_str == "gmt" || tz_str == "Z" || tz_str == "z" ||
      tz_str == "Etc/Greenwich" || tz_str == "Etc/UTC" || tz_str == "Etc/GMT") {
    return 0;
  }
  if (tz_str.starts_with("UTC+") || tz_str.starts_with("UTC-") ||
      tz_str.starts_with("GMT+") || tz_str.starts_with("GMT-")) {
    char sign = tz_str[3];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(4));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &h, &m);
    } else {
      sscanf(rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int h = 0, m = 0;
    std::string rem(tz_str.substr(1));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &h, &m);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &h, &m);
    } else {
      sscanf(rem.c_str(), "%d", &h);
    }
    return (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone) {
      int y = ct ? ct->year : 2000;
      int mon = ct ? ct->month : 1;
      int d = ct ? ct->day : 1;
      int h = ct ? ct->hour : 0;
      int min = ct ? ct->minute : 0;
      int s = ct ? ct->second : 0;
      if (y < 1970) {
        y = 1970;
      }
      std::chrono::year_month_day ymd{
          std::chrono::year{y}, std::chrono::month{static_cast<unsigned>(mon)},
          std::chrono::day{static_cast<unsigned>(d)}};
      std::chrono::local_days loc_d{ymd};
      auto loc_tp = loc_d + std::chrono::hours{h} + std::chrono::minutes{min} +
                    std::chrono::seconds{s};
      auto loc_info = zone->get_info(loc_tp);
      return static_cast<int>(loc_info.first.offset.count());
    }
  } catch (...) {
  }
  return default_offset;
}

std::string FormatTimeZoneOffset(int tz_offset_sec) {
  char buf[16];
  int abs_sec = std::abs(tz_offset_sec);
  int h = abs_sec / 3600;
  int m = (abs_sec % 3600) / 60;
  char sign = tz_offset_sec < 0 ? '-' : '+';
  if (m == 0) {
    snprintf(buf, sizeof(buf), "%c%02d", sign, h);
  } else {
    snprintf(buf, sizeof(buf), "%c%02d:%02d", sign, h, m);
  }
  return std::string(buf);
}

CivilTime ValueToCivilTime(const Value& val) {
  CivilTime ct;
  if (val.type == ValueType::kDate) {
    std::chrono::sys_days sys_d{std::chrono::days{val.DateDays()}};
    std::chrono::year_month_day ymd{sys_d};
    ct.year = int(ymd.year());
    ct.month = unsigned(ymd.month());
    ct.day = unsigned(ymd.day());
    return ct;
  }
  if (val.type == ValueType::kVarChar) {
    if (ParseCivilTime(val.value.varchar_value, &ct)) {
      return ct;
    }
  }
  std::string s = val.AsString();
  if (ParseCivilTime(s, &ct)) {
    return ct;
  }
  throw std::runtime_error("requires DATE, DATETIME or TIMESTAMP");
}

int64_t CivilTimeToNanos(const CivilTime& ct) {
  std::chrono::year_month_day ymd{
      std::chrono::year{ct.year},
      std::chrono::month{static_cast<unsigned>(ct.month)},
      std::chrono::day{static_cast<unsigned>(ct.day)}};
  int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
  int64_t secs =
      days * 86400LL + ct.hour * 3600LL + ct.minute * 60LL + ct.second;
  return secs * 1000000000LL + ct.subsecond_nanos;
}

CivilTime NanosToCivilTime(int64_t nanos) {
  auto floor_div = [](int64_t a, int64_t b) -> int64_t {
    const int64_t q = a / b;
    return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
  };
  int64_t secs = floor_div(nanos, 1000000000LL);
  int64_t sub_ns = nanos - secs * 1000000000LL;
  int64_t days = floor_div(secs, 86400LL);
  int64_t day_secs = secs - days * 86400LL;

  std::chrono::sys_days sys_d{std::chrono::days{days}};
  std::chrono::year_month_day ymd{sys_d};

  CivilTime ct;
  ct.year = int(ymd.year());
  ct.month = unsigned(ymd.month());
  ct.day = unsigned(ymd.day());
  ct.hour = day_secs / 3600;
  ct.minute = (day_secs % 3600) / 60;
  ct.second = day_secs % 60;
  ct.subsecond_nanos = sub_ns;
  return ct;
}

std::string FormatCivilTime(const CivilTime& ct, bool include_subsecond = true,
                            bool is_date_only = false) {
  char buf[64];
  if (is_date_only) {
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", ct.year, ct.month, ct.day);
    return std::string(buf);
  }
  if (include_subsecond && ct.subsecond_nanos != 0) {
    if (ct.subsecond_nanos % 1000000 == 0) {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03ld", ct.year,
               ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos / 1000000);
    } else if (ct.subsecond_nanos % 1000 == 0) {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06ld", ct.year,
               ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos / 1000);
    } else {
      snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%09ld", ct.year,
               ct.month, ct.day, ct.hour, ct.minute, ct.second,
               ct.subsecond_nanos);
    }
    return std::string(buf);
  }
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", ct.year, ct.month,
           ct.day, ct.hour, ct.minute, ct.second);
  return std::string(buf);
}

std::string NormalizeTimestampText(const std::string& text) {
  // Timestamp-shaped only: YYYY-MM-DD HH:MM:SS[.frac][+HH[:MM]|Z]
  if (text.size() < 19 || text[4] != '-' || text[7] != '-' || text[10] != ' ') {
    return text;
  }
  CivilTime ct;
  if (!ParseCivilTime(text, &ct)) {
    return text;
  }
  // Recover the UTC instant: engine text labels "+00" but carries the wall
  // clock shifted opposite the session offset.
  const int64_t nanos =
      CivilTimeToNanos(ct) +
      static_cast<int64_t>(ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, 0)) *
          1000000000LL;
  return FormatCivilTime(NanosToCivilTime(nanos),
                         /*include_subsecond=*/true) +
         "+00";
}

// Mutual recursion with Evaluate above; expression trees are the intended
// shape here.
Value EvaluateFunction(  // NOLINT(misc-no-recursion)
    const FunctionCallExpression& call, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  auto raw_str = [](const Value& val) -> std::string {
    if (val.type == ValueType::kVarChar) {
      return std::string(val.value.varchar_value);
    }
    return val.AsString();
  };
  std::string name = call.FuncName();
  for (char& c : name) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  // IF evaluates only the chosen branch: the untaken branch must not raise
  // (e.g. guarded out-of-range array accesses under IF guards).
  if (name == "if") {
    if (call.Args().size() != 3) {
      throw std::runtime_error("IF requires 3 arguments");
    }
    const Value condition =
        Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    const size_t taken = condition.Truthy() ? 1 : 2;
    return Evaluate(call.Args()[taken], scope, aggregates, context, ctes);
  }
  // COALESCE stops at the first non-NULL argument.
  if (name == "coalesce") {
    for (const Expression& argument : call.Args()) {
      Value value = Evaluate(argument, scope, aggregates, context, ctes);
      if (!value.IsNull()) {
        return value;
      }
    }
    return {};
  }
  if (name == "date_add" || name == "date_sub" || name == "datetime_add" ||
      name == "datetime_sub" || name == "timestamp_add" ||
      name == "timestamp_sub") {
    if (call.Args().size() != 2 ||
        call.Args()[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error(name + " arity");
    }
    const Value base =
        Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    if (base.IsNull()) {
      return {};
    }
    const auto& interval = call.Args()[1]->AsIntervalExpression();
    const bool is_sub = name.ends_with("_sub");
    const int64_t raw_amount = interval.Amount();
    std::string unit = std::string(interval.Unit());
    for (char& c : unit) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    CivilTime ct = ValueToCivilTime(base);
    std::string base_s = raw_str(base);
    bool is_timestamp = (base_s.find('+') != std::string::npos ||
                         base_s.find('Z') != std::string::npos ||
                         base_s.find('z') != std::string::npos);
    if (is_timestamp) {
      int tz_offset_sec =
          ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
      int64_t ns = CivilTimeToNanos(ct) + tz_offset_sec * 1000000000LL;
      ct = NanosToCivilTime(ns);
    }
    const bool is_date = (base.type == ValueType::kDate &&
                          (name == "date_add" || name == "date_sub"));

    if (unit == "year" || unit == "years" || unit == "quarter" ||
        unit == "quarters" || unit == "month" || unit == "months") {
      int64_t amount = is_sub ? -raw_amount : raw_amount;
      int64_t add_m = amount;
      if (unit.starts_with("year")) {
        add_m = amount * 12;
      } else if (unit.starts_with("quarter")) {
        add_m = amount * 3;
      }
      int64_t total_m = (ct.year * 12 + (ct.month - 1)) + add_m;
      int target_y = total_m / 12;
      int target_m = (total_m % 12) + 1;
      if (target_m <= 0) {
        target_m += 12;
        target_y -= 1;
      }
      using std::chrono::month;
      using std::chrono::month_day_last;
      using std::chrono::year;
      using std::chrono::year_month_day_last;
      year_month_day_last last_of_target{
          year{target_y},
          month_day_last{month{static_cast<unsigned>(target_m)}}};
      unsigned target_d = ct.day;
      if (target_d > unsigned(last_of_target.day())) {
        target_d = unsigned(last_of_target.day());
      }
      ct.year = target_y;
      ct.month = target_m;
      ct.day = target_d;
      if (ct.year < 1 || ct.year > 9999) {
        throw std::runtime_error("DATETIME out of range");
      }
      if (is_date) {
        std::chrono::year_month_day ymd{
            year{ct.year}, month{static_cast<unsigned>(ct.month)},
            std::chrono::day{static_cast<unsigned>(ct.day)}};
        return Value::DateFromDays(
            std::chrono::sys_days{ymd}.time_since_epoch().count());
      }
      if (is_timestamp) {
        int tz_offset_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
        int64_t ns = CivilTimeToNanos(ct) - tz_offset_sec * 1000000000LL;
        CivilTime utc_ct = NanosToCivilTime(ns);
        return Value(FormatCivilTime(utc_ct) + "+00");
      }
      return Value(FormatCivilTime(ct));
    }

    int64_t delta_days = 0;
    int64_t delta_sub_ns = 0;
    if (raw_amount == std::numeric_limits<int64_t>::min()) {
      uint64_t uamount = 9223372036854775808ULL;
      if (unit == "nanosecond" || unit == "nanoseconds") {
        delta_days = static_cast<int64_t>(uamount / (86400ULL * 1000000000ULL));
        delta_sub_ns =
            static_cast<int64_t>(uamount % (86400ULL * 1000000000ULL));
      }
      if (!is_sub) {
        delta_days = -delta_days;
        delta_sub_ns = -delta_sub_ns;
      }
    } else {
      int64_t amount = is_sub ? -raw_amount : raw_amount;
      if (unit == "day" || unit == "days") {
        delta_days = amount;
      } else if (unit == "week" || unit == "weeks") {
        delta_days = amount * 7LL;
      } else if (unit == "hour" || unit == "hours") {
        delta_days = amount / 24;
        delta_sub_ns = (amount % 24) * 3600LL * 1000000000LL;
      } else if (unit == "minute" || unit == "minutes") {
        delta_days = amount / (24 * 60);
        delta_sub_ns = (amount % (24 * 60)) * 60LL * 1000000000LL;
      } else if (unit == "second" || unit == "seconds") {
        delta_days = amount / 86400LL;
        delta_sub_ns = (amount % 86400LL) * 1000000000LL;
      } else if (unit == "millisecond" || unit == "milliseconds") {
        delta_days = amount / (86400LL * 1000LL);
        delta_sub_ns = (amount % (86400LL * 1000LL)) * 1000000LL;
      } else if (unit == "microsecond" || unit == "microseconds") {
        delta_days = amount / (86400LL * 1000000LL);
        delta_sub_ns = (amount % (86400LL * 1000000LL)) * 1000LL;
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        delta_days = amount / (86400LL * 1000000000LL);
        delta_sub_ns = amount % (86400LL * 1000000000LL);
      } else {
        throw std::runtime_error("unsupported interval unit " + unit);
      }
    }

    int64_t day_nanos =
        (ct.hour * 3600LL + ct.minute * 60LL + ct.second) * 1000000000LL +
        ct.subsecond_nanos + delta_sub_ns;
    int64_t total_day_nanos = 86400LL * 1000000000LL;
    auto floor_div = [](int64_t a, int64_t b) -> int64_t {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    int64_t extra_days = floor_div(day_nanos, total_day_nanos);
    int64_t rem_day_nanos = day_nanos - extra_days * total_day_nanos;

    std::chrono::year_month_day cur_ymd{
        std::chrono::year{ct.year},
        std::chrono::month{static_cast<unsigned>(ct.month)},
        std::chrono::day{static_cast<unsigned>(ct.day)}};
    int64_t cur_days =
        std::chrono::sys_days{cur_ymd}.time_since_epoch().count();
    int64_t target_days = cur_days + delta_days + extra_days;
    std::chrono::sys_days target_sys_days{std::chrono::days{target_days}};
    std::chrono::year_month_day target_ymd{target_sys_days};

    CivilTime res_ct;
    res_ct.year = int(target_ymd.year());
    res_ct.month = unsigned(target_ymd.month());
    res_ct.day = unsigned(target_ymd.day());
    int64_t rem_secs = rem_day_nanos / 1000000000LL;
    res_ct.subsecond_nanos = rem_day_nanos % 1000000000LL;
    res_ct.hour = rem_secs / 3600;
    res_ct.minute = (rem_secs % 3600) / 60;
    res_ct.second = rem_secs % 60;

    if (res_ct.year < 1 || res_ct.year > 9999) {
      throw std::runtime_error("DATETIME out of range");
    }
    if (is_date) {
      std::chrono::year_month_day ymd{
          std::chrono::year{res_ct.year},
          std::chrono::month{static_cast<unsigned>(res_ct.month)},
          std::chrono::day{static_cast<unsigned>(res_ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    if (is_timestamp) {
      int64_t ns = CivilTimeToNanos(res_ct) + (8 * 3600LL) * 1000000000LL;
      CivilTime utc_ct = NanosToCivilTime(ns);
      return Value(FormatCivilTime(utc_ct) + "+00");
    }
    return Value(FormatCivilTime(res_ct));
  }
  // Control flow functions must observe conditional-evaluation semantics:
  // untaken branches (and error-handled operands) are never evaluated, so a
  // division by zero or an invalid cast inside them cannot surface.
  // Branch results are normalized to the common supertype of every branch
  // (GoogleSQL coerces INT64 branch arms to FLOAT64 when any arm is a
  // float), keeping downstream comparisons type-consistent.
  auto promotes_to_double = [&](const std::vector<Expression>& exprs) {
    for (const Expression& branch : exprs) {
      if (scope.schema == nullptr) { continue; }
      try {
        if (branch->ResultType(*scope.schema).GetType() == TypeTag::kDouble) {
          return true;
        }
      } catch (const std::exception&) {
        // Static types are unavailable for subqueries/aggregates; the
        // runtime value types then stand on their own.
      }
    }
    return false;
  };
  auto normalize = [](Value value, bool to_double) {
    if (to_double && !value.IsNull() && value.type == ValueType::kInt64) {
      return Value(static_cast<double>(value.value.int_value));
    }
    return value;
  };
  if (name == "if") {
    if (call.Args().size() != 3) {
      throw std::runtime_error("IF requires 3 arguments");
    }
    const bool as_double =
        promotes_to_double({call.Args()[1], call.Args()[2]});
    const Value condition =
        Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    const bool take_then = !condition.IsNull() && Truthy(condition);
    return normalize(
        Evaluate(call.Args()[take_then ? 1 : 2], scope, aggregates, context,
                 ctes), as_double);
  }
  if (name == "iferror") {
    if (call.Args().size() != 2) {
      throw std::runtime_error("IFERROR requires 2 arguments");
    }
    const bool as_double = promotes_to_double(call.Args());
    try {
      return normalize(
          Evaluate(call.Args()[0], scope, aggregates, context, ctes),
          as_double);
    } catch (const std::exception&) {
      return normalize(
          Evaluate(call.Args()[1], scope, aggregates, context, ctes),
          as_double);
    }
  }
  if (name == "iserror") {
    if (call.Args().size() != 1) {
      throw std::runtime_error("ISERROR requires 1 argument");
    }
    try {
      Evaluate(call.Args()[0], scope, aggregates, context, ctes);
      return Value(false);
    } catch (const std::exception&) {
      return Value(true);
    }
  }
  if (name == "nulliferror") {
    if (call.Args().size() != 1) {
      throw std::runtime_error("NULLIFERROR requires 1 argument");
    }
    try {
      return Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    } catch (const std::exception&) {
      return Value();
    }
  }
  std::vector<Value> arguments;
  for (const Expression& argument : call.Args()) {
    arguments.push_back(Evaluate(argument, scope, aggregates, context, ctes));
  }

  if (name == "__struct_json__") {
    // Runtime struct constructor installed by the visitor when struct
    // fields could not be folded at plan time (aggregates, subqueries).
    // Arguments alternate name / value / quoted-flag.
    if (arguments.size() % 3 != 0) {
      throw std::runtime_error("struct constructor arity");
    }
    std::string json = "{";
    for (size_t i = 0; i < arguments.size(); i += 3) {
      if (i != 0) {
        json += ",";
      }
      std::string escaped;
      const std::string field_name = raw_str(arguments[i]);
      for (char c : field_name) {
        if (c == '"' || c == '\\') {
          escaped.push_back('\\');
        }
        escaped.push_back(c);
      }
      json += "\"" + escaped + "\":";
      const Value& field_value = arguments[i + 1];
      if (field_value.IsNull()) {
        json += "null";
        continue;
      }
      const bool quoted = Truthy(arguments[i + 2]);
      if (quoted) {
        std::string text = raw_str(field_value);
        std::string escaped_text;
        for (char c : text) {
          if (c == '"' || c == '\\') {
            escaped_text.push_back('\\');
          }
          escaped_text.push_back(c);
        }
        json += "\"" + escaped_text + "\"";
        continue;
      }
      if (field_value.type == ValueType::kInt64) {
        json += std::to_string(field_value.value.int_value);
      } else if (field_value.type == ValueType::kDouble) {
        const double d = field_value.value.double_value;
        char buffer[64];
        auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), d);
        json.append(buffer, static_cast<size_t>(ptr - buffer));
      } else if (field_value.type == ValueType::kVarChar) {
        // Flag said unquoted: emit the text without JSON quoting.
        json += raw_str(field_value);
      } else {
        json += field_value.AsString();
      }
    }
    json += "}";
    return Value(std::move(json));
  }

  auto to_lower = [](std::string_view s) -> std::string {
    std::string out(s);
    for (char& c : out) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
  };

  if (name.starts_with("extract_")) {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("EXTRACT requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string arg0_str = raw_str(arguments[0]);
    if (arguments[0].type == ValueType::kVarChar &&
        arg0_str.find('-') != std::string::npos &&
        arg0_str.find(' ') != std::string::npos &&
        arg0_str.find('-') < arg0_str.find(' ')) {
      IntervalValue iv = IntervalValue::Parse(arg0_str);
      int64_t y = iv.months / 12;
      int64_t m = iv.months % 12;
      int64_t total_sec = iv.nanos / 1000000000LL;
      int64_t sub_ns = iv.nanos % 1000000000LL;
      int64_t h = total_sec / 3600;
      int64_t min = (total_sec % 3600) / 60;
      int64_t s = total_sec % 60;
      if (name == "extract_year") {
        return Value(y);
      }
      if (name == "extract_month") {
        return Value(m);
      }
      if (name == "extract_day") {
        return Value(iv.days);
      }
      if (name == "extract_hour") {
        return Value(h);
      }
      if (name == "extract_minute") {
        return Value(min);
      }
      if (name == "extract_second") {
        return Value(s);
      }
      if (name == "extract_millisecond") {
        return Value(sub_ns / 1000000LL);
      }
      if (name == "extract_microsecond") {
        return Value(sub_ns / 1000LL);
      }
      if (name == "extract_nanosecond") {
        return Value(sub_ns);
      }
    }
    CivilTime ct = ValueToCivilTime(arguments[0]);
    if (arguments.size() == 2) {
      if (arguments[1].IsNull()) {
        return {};
      }
      std::string tz_str = raw_str(arguments[1]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct);
      ct = ShiftCivilTimeHours(ct, tz_offset_sec / 3600);
      int rem_mins = (tz_offset_sec % 3600) / 60;
      if (rem_mins != 0) {
        int total_m = ct.minute + rem_mins;
        if (total_m >= 60) {
          ct.minute = total_m - 60;
          ct = ShiftCivilTimeHours(ct, 1);
        } else if (total_m < 0) {
          ct.minute = total_m + 60;
          ct = ShiftCivilTimeHours(ct, -1);
        } else {
          ct.minute = total_m;
        }
      }
    }
    if (name == "extract_date") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    if (name == "extract_time") {
      char buf[64];
      if (ct.subsecond_nanos != 0) {
        if (ct.subsecond_nanos % 1000000 == 0) {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03ld", ct.hour, ct.minute,
                   ct.second, ct.subsecond_nanos / 1000000);
        } else if (ct.subsecond_nanos % 1000 == 0) {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld", ct.hour, ct.minute,
                   ct.second, ct.subsecond_nanos / 1000);
        } else {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%09ld", ct.hour, ct.minute,
                   ct.second, ct.subsecond_nanos);
        }
      } else {
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d", ct.hour, ct.minute,
                 ct.second);
      }
      return Value(std::string(buf));
    }
    if (name == "extract_datetime") {
      return Value(FormatCivilTime(ct));
    }
    if (name == "extract_year") {
      return Value(static_cast<int64_t>(ct.year));
    }
    if (name == "extract_quarter") {
      return Value(static_cast<int64_t>((ct.month - 1) / 3 + 1));
    }
    if (name == "extract_month") {
      return Value(static_cast<int64_t>(ct.month));
    }
    if (name == "extract_day") {
      return Value(static_cast<int64_t>(ct.day));
    }
    if (name == "extract_hour") {
      return Value(static_cast<int64_t>(ct.hour));
    }
    if (name == "extract_minute") {
      return Value(static_cast<int64_t>(ct.minute));
    }
    if (name == "extract_second") {
      return Value(static_cast<int64_t>(ct.second));
    }
    if (name == "extract_millisecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos / 1000000LL));
    }
    if (name == "extract_microsecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos / 1000LL));
    }
    if (name == "extract_nanosecond") {
      return Value(static_cast<int64_t>(ct.subsecond_nanos));
    }
    if (name == "extract_week") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      std::chrono::year_month_day year_start{std::chrono::year{ct.year},
                                             std::chrono::month{1},
                                             std::chrono::day{1}};
      int start_wd =
          std::chrono::weekday{std::chrono::sys_days{year_start}}.c_encoding();
      int yday = (sd - std::chrono::sys_days{year_start}).count();
      int week_num = (yday + start_wd) / 7;
      return Value(static_cast<int64_t>(week_num));
    }
    if (name == "extract_dayofweek") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      int wd = std::chrono::weekday{std::chrono::sys_days{ymd}}.c_encoding();
      return Value(static_cast<int64_t>(wd + 1));
    }
    if (name == "extract_dayofyear") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::year_month_day year_start{std::chrono::year{ct.year},
                                             std::chrono::month{1},
                                             std::chrono::day{1}};
      int yday =
          (std::chrono::sys_days{ymd} - std::chrono::sys_days{year_start})
              .count() +
          1;
      return Value(static_cast<int64_t>(yday));
    }
    if (name == "extract_isoyear" || name == "extract_isoweek") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      int iso_wd = std::chrono::weekday{sd}.iso_encoding();
      std::chrono::sys_days thu = sd + std::chrono::days{4 - iso_wd};
      std::chrono::year_month_day thu_ymd{thu};
      int isoyear = int(thu_ymd.year());
      if (name == "extract_isoyear") {
        return Value(static_cast<int64_t>(isoyear));
      }
      std::chrono::year_month_day iso_start{std::chrono::year{isoyear},
                                            std::chrono::month{1},
                                            std::chrono::day{4}};
      int start_iso_wd =
          std::chrono::weekday{std::chrono::sys_days{iso_start}}.iso_encoding();
      std::chrono::sys_days first_mon = std::chrono::sys_days{iso_start} -
                                        std::chrono::days{start_iso_wd - 1};
      int isoweek = (sd - first_mon).count() / 7 + 1;
      return Value(static_cast<int64_t>(isoweek));
    }
    throw std::runtime_error("unsupported extract field: " + name);
  }
  if (name == "__struct_set") {
    if (arguments.size() != 3) {
      throw std::runtime_error("__struct_set requires 3 arguments");
    }
    return StructSetField(arguments[0], raw_str(arguments[1]), arguments[2]);
  }
  if (name == "error") {
    // Unreachable in valid plans unless evaluated: raise the requested
    // runtime error (the message text is informational only).
    throw std::runtime_error(
        arguments.empty() || arguments[0].IsNull()
            ? std::string("ERROR: user-raised")
            : "generic::out_of_range: " + raw_str(arguments[0]));
  }
  if (name == "coalesce") {
    for (Value& value : arguments) {
      if (!value.IsNull()) {
        return value;
      }
    }
    return {};
  }
  if (name == "nullif") {
    if (arguments.size() != 2) {
      throw std::runtime_error("NULLIF requires 2 arguments");
    }
    if (arguments[0] == arguments[1]) {
      return {};
    }
    return arguments[0];
  }
  if (name == "ifnull") {
    if (arguments.size() != 2) {
      throw std::runtime_error("IFNULL requires 2 arguments");
    }
    return !arguments[0].IsNull() ? arguments[0] : arguments[1];
  }

  auto parse_date_val = [](const Value& val) -> int64_t {
    if (val.type == ValueType::kDate) {
      return val.DateDays();
    }
    if (val.type == ValueType::kInt64) {
      return val.value.int_value;
    }
    if (val.type == ValueType::kVarChar) {
      std::string_view s = val.value.varchar_value;
      if (s.size() >= 10 && s[4] == '-' && s[7] == '-') {
        return ParseDateDays(s.substr(0, 10));
      }
      return ParseDateDays(s);
    }
    throw std::runtime_error("requires DATE");
  };

  if (name == "unix_date") {
    if (arguments.size() != 1) {
      throw std::runtime_error("UNIX_DATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    return Value(parse_date_val(arguments[0]));
  }
  if (name == "date_from_unix_date") {
    if (arguments.size() != 1) {
      throw std::runtime_error("DATE_FROM_UNIX_DATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    int64_t days = arguments[0].type == ValueType::kInt64
                       ? arguments[0].value.int_value
                       : parse_date_val(arguments[0]);
    return Value::DateFromDays(days);
  }
  if (name == "current_time") {
    if (arguments.size() > 1) {
      throw std::runtime_error("CURRENT_TIME takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return {};
    }
    int tz_offset_sec = 0;
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      std::string tz_str = raw_str(arguments[0]);
      if (tz_str == "UTC" || tz_str == "utc" || tz_str == "Z" ||
          tz_str == "z") {
        tz_offset_sec = 0;
      } else if (!tz_str.empty() && (tz_str[0] == '+' || tz_str[0] == '-')) {
        char sign = tz_str[0];
        int h = 0, m = 0;
        if (tz_str.find(':') != std::string::npos) {
          if (sscanf(tz_str.c_str() + 1, "%d:%d", &h, &m) < 1) {
            throw std::runtime_error("invalid timezone: " + tz_str);
          }
        } else {
          if (sscanf(tz_str.c_str() + 1, "%d", &h) < 1) {
            throw std::runtime_error("invalid timezone: " + tz_str);
          }
        }
        tz_offset_sec = (h * 3600 + m * 60) * (sign == '-' ? -1 : 1);
      } else {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    char buf[32];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d", t.tm_hour, t.tm_min, t.tm_sec);
    return Value(std::string(buf));
  }
  if (name == "current_datetime") {
    if (arguments.size() > 1) {
      throw std::runtime_error("CURRENT_DATETIME takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return {};
    }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      std::string tz_str = raw_str(arguments[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
             t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min,
             t.tm_sec);
    return Value(std::string(buf));
  }
  if (name == "current_date") {
    if (arguments.size() > 1) {
      throw std::runtime_error("CURRENT_DATE takes at most 1 argument");
    }
    if (arguments.size() == 1 && arguments[0].IsNull()) {
      return {};
    }
    int tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone());
    if (arguments.size() == 1 && !arguments[0].IsNull()) {
      std::string tz_str = raw_str(arguments[0]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    time_t now = time(nullptr) + tz_offset_sec;
    struct tm t = {};
    gmtime_r(&now, &t);
    std::chrono::year_month_day ymd{
        std::chrono::year{t.tm_year + 1900},
        std::chrono::month{static_cast<unsigned>(t.tm_mon + 1)},
        std::chrono::day{static_cast<unsigned>(t.tm_mday)}};
    return Value::DateFromDays(
        std::chrono::sys_days{ymd}.time_since_epoch().count());
  }
  if (name == "string") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("STRING requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    if (arguments.size() == 2) {
      CivilTime ct = ValueToCivilTime(arguments[0]);
      std::string tz_str = raw_str(arguments[1]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct);
      ct = ShiftCivilTimeHours(ct, tz_offset_sec / 3600);
      int rem_mins = (tz_offset_sec % 3600) / 60;
      if (rem_mins != 0) {
        int total_m = ct.minute + rem_mins;
        if (total_m >= 60) {
          ct.minute = total_m - 60;
          ct = ShiftCivilTimeHours(ct, 1);
        } else if (total_m < 0) {
          ct.minute = total_m + 60;
          ct = ShiftCivilTimeHours(ct, -1);
        } else {
          ct.minute = total_m;
        }
      }
      return Value(FormatCivilTime(ct) + FormatTimeZoneOffset(tz_offset_sec));
    }
    if (arguments[0].type == ValueType::kDate) {
      return Value(FormatDateDays(arguments[0].DateDays()));
    }
    return Value(raw_str(arguments[0]));
  }
  if (name == "date") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("DATE takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    if (arguments[0].type == ValueType::kDate && arguments.size() == 1) {
      return arguments[0];
    }
    std::string s = raw_str(arguments[0]);
    if (s.size() >= 10 && s[4] == '-' && s[7] == '-') {
      if (s.size() > 10) {
        int Y = 0, M = 0, D = 0, h = 0, m = 0;
        double sec = 0;
        if (sscanf(s.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D, &h, &m, &sec) >=
            5) {
          int tz_offset_sec = 0;
          bool has_tz = false;
          size_t tz_pos = s.find_first_of("+-", 10);
          if (tz_pos != std::string::npos) {
            char sign = s[tz_pos];
            int tz_h = 0, tz_m = 0;
            if (s.find(':', tz_pos) != std::string::npos) {
              sscanf(s.c_str() + tz_pos + 1, "%d:%d", &tz_h, &tz_m);
            } else {
              sscanf(s.c_str() + tz_pos + 1, "%d", &tz_h);
            }
            tz_offset_sec = (tz_h * 3600 + tz_m * 60) * (sign == '-' ? -1 : 1);
            has_tz = true;
          } else if (s.ends_with('Z') || s.ends_with('z')) {
            has_tz = true;
          }
          if (arguments.size() == 2) {
            std::string tz2 = raw_str(arguments[1]);
            int tz2_sec = 0;
            if (!tz2.empty() && (tz2[0] == '+' || tz2[0] == '-')) {
              char sign = tz2[0];
              int tz_h = 0, tz_m = 0;
              if (tz2.find(':') != std::string::npos) {
                sscanf(tz2.c_str() + 1, "%d:%d", &tz_h, &tz_m);
              } else {
                sscanf(tz2.c_str() + 1, "%d", &tz_h);
              }
              tz2_sec = (tz_h * 3600 + tz_m * 60) * (sign == '-' ? -1 : 1);
            }
            struct tm t = {};
            t.tm_year = Y - 1900;
            t.tm_mon = M - 1;
            t.tm_mday = D;
            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = static_cast<int>(sec);
            time_t epoch = timegm(&t) - tz_offset_sec + tz2_sec;
            struct tm target = {};
            gmtime_r(&epoch, &target);
            std::chrono::year_month_day ymd{
                std::chrono::year{target.tm_year + 1900},
                std::chrono::month{static_cast<unsigned>(target.tm_mon + 1)},
                std::chrono::day{static_cast<unsigned>(target.tm_mday)}};
            return Value::DateFromDays(
                std::chrono::sys_days{ymd}.time_since_epoch().count());
          } else if (has_tz) {
            CivilTime ct_tmp{
                Y, static_cast<unsigned>(M), static_cast<unsigned>(D), h,
                m, static_cast<int>(sec)};
            int default_tz_sec =
                ParseTimeZoneOffset(GetDefaultTimeZone(), &ct_tmp, -8 * 3600);
            struct tm t = {};
            t.tm_year = Y - 1900;
            t.tm_mon = M - 1;
            t.tm_mday = D;
            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = static_cast<int>(sec);
            time_t epoch = timegm(&t) - tz_offset_sec + default_tz_sec;
            struct tm target = {};
            gmtime_r(&epoch, &target);
            std::chrono::year_month_day ymd{
                std::chrono::year{target.tm_year + 1900},
                std::chrono::month{static_cast<unsigned>(target.tm_mon + 1)},
                std::chrono::day{static_cast<unsigned>(target.tm_mday)}};
            return Value::DateFromDays(
                std::chrono::sys_days{ymd}.time_since_epoch().count());
          }
        }
      }
      return Value::Date(s.substr(0, 10));
    }
    return Value::Date(s);
  }
  if (name == "datetime") {
    if (arguments.empty() || arguments.size() > 7) {
      throw std::runtime_error("DATETIME takes 1 to 7 arguments");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments.size() == 1) {
      if (arguments[0].type == ValueType::kDate) {
        return Value(FormatDateDays(arguments[0].DateDays()) + " 00:00:00");
      }
      CivilTime ct = ValueToCivilTime(arguments[0]);
      std::string s = raw_str(arguments[0]);
      if (s.find('+') != std::string::npos ||
          s.find('Z') != std::string::npos ||
          s.find('z') != std::string::npos) {
        int64_t ns = CivilTimeToNanos(ct) + (-8 * 3600LL) * 1000000000LL;
        ct = NanosToCivilTime(ns);
      }
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() == 2) {
      std::string arg1_str = raw_str(arguments[1]);
      if (arg1_str.find(':') != std::string::npos && arg1_str.size() <= 8 &&
          arg1_str[0] != '+' && arg1_str[0] != '-') {
        std::string d = raw_str(arguments[0]);
        if (d.size() > 10) {
          d = d.substr(0, 10);
        }
        return Value(d + " " + arg1_str);
      }
      CivilTime ct = ValueToCivilTime(arguments[0]);
      int tz_offset_sec = ParseTimeZoneOffset(arg1_str, &ct, 0);
      if (tz_offset_sec != 0) {
        int64_t ns = CivilTimeToNanos(ct) + tz_offset_sec * 1000000000LL;
        ct = NanosToCivilTime(ns);
      }
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() == 3) {
      std::string d = raw_str(arguments[0]);
      std::string t = raw_str(arguments[1]);
      if (d.size() > 10) {
        d = d.substr(0, 10);
      }
      CivilTime ct;
      ParseCivilTime(d + " " + t, &ct);
      return Value(FormatCivilTime(ct));
    }
    if (arguments.size() >= 6) {
      int Y = arguments[0].type == ValueType::kInt64
                  ? arguments[0].value.int_value
                  : std::stoi(raw_str(arguments[0]));
      int M = arguments[1].type == ValueType::kInt64
                  ? arguments[1].value.int_value
                  : std::stoi(raw_str(arguments[1]));
      int D = arguments[2].type == ValueType::kInt64
                  ? arguments[2].value.int_value
                  : std::stoi(raw_str(arguments[2]));
      int h = arguments[3].type == ValueType::kInt64
                  ? arguments[3].value.int_value
                  : std::stoi(raw_str(arguments[3]));
      int m = arguments[4].type == ValueType::kInt64
                  ? arguments[4].value.int_value
                  : std::stoi(raw_str(arguments[4]));
      int s = arguments[5].type == ValueType::kInt64
                  ? arguments[5].value.int_value
                  : std::stoi(raw_str(arguments[5]));
      CivilTime ct;
      ct.year = Y;
      ct.month = M;
      ct.day = D;
      ct.hour = h;
      ct.minute = m;
      ct.second = s;
      if (arguments.size() == 7) {
        int64_t sub = arguments[6].type == ValueType::kInt64
                          ? arguments[6].value.int_value
                          : std::stoll(raw_str(arguments[6]));
        ct.subsecond_nanos = sub;
      }
      return Value(FormatCivilTime(ct));
    }
  }
  if (name == "timestamp") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("TIMESTAMP takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    CivilTime ct = ValueToCivilTime(arguments[0]);
    std::string tz_str =
        (arguments.size() == 2) ? raw_str(arguments[1]) : GetDefaultTimeZone();
    int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct, -8 * 3600);
    int64_t ns = CivilTimeToNanos(ct) - tz_offset_sec * 1000000000LL;
    CivilTime utc_ct = NanosToCivilTime(ns);
    return Value(FormatCivilTime(utc_ct) + "+00");
  }
  if (name == "time") {
    if (arguments.empty() || arguments.size() > 4) {
      throw std::runtime_error("TIME takes 1 to 4 arguments");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments.size() == 1) {
      std::string s = raw_str(arguments[0]);
      CivilTime ct;
      if (ParseCivilTime(s, &ct)) {
        size_t tz_pos = s.find_first_of("+-", 11);
        if (tz_pos != std::string::npos || s.find('Z') != std::string::npos ||
            s.find('z') != std::string::npos) {
          int tz_offset_sec = 0;
          if (tz_pos != std::string::npos) {
            tz_offset_sec = ParseTimeZoneOffset(s.substr(tz_pos), &ct, 0);
          }
          int64_t ns = CivilTimeToNanos(ct) - tz_offset_sec * 1000000000LL +
                       (-8 * 3600LL) * 1000000000LL;
          ct = NanosToCivilTime(ns);
        }
        char buf[64];
        if (ct.subsecond_nanos != 0) {
          if (ct.subsecond_nanos % 1000000 == 0) {
            snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03ld", ct.hour,
                     ct.minute, ct.second, ct.subsecond_nanos / 1000000);
          } else if (ct.subsecond_nanos % 1000 == 0) {
            snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld", ct.hour,
                     ct.minute, ct.second, ct.subsecond_nanos / 1000);
          } else {
            snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%09ld", ct.hour,
                     ct.minute, ct.second, ct.subsecond_nanos);
          }
        } else {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d", ct.hour, ct.minute,
                   ct.second);
        }
        return Value(std::string(buf));
      }
      size_t space = s.find(' ');
      if (space == std::string::npos) {
        space = s.find('T');
      }
      if (space != std::string::npos) {
        s = s.substr(space + 1);
      }
      size_t tz_pos = s.find_first_of("+-Zz");
      if (tz_pos != std::string::npos) {
        s = s.substr(0, tz_pos);
      }
      return Value(std::move(s));
    }
    if (arguments.size() == 2) {
      CivilTime ct = ValueToCivilTime(arguments[0]);
      std::string tz_str = raw_str(arguments[1]);
      int tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct, 0);
      if (tz_offset_sec != 0) {
        int64_t ns = CivilTimeToNanos(ct) + tz_offset_sec * 1000000000LL;
        ct = NanosToCivilTime(ns);
      }
      char buf[64];
      if (ct.subsecond_nanos != 0) {
        if (ct.subsecond_nanos % 1000000 == 0) {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03ld", ct.hour, ct.minute,
                   ct.second, ct.subsecond_nanos / 1000000);
        } else if (ct.subsecond_nanos % 1000 == 0) {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld", ct.hour, ct.minute,
                   ct.second, ct.subsecond_nanos / 1000);
        } else {
          snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%09ld", ct.hour, ct.minute,
                   ct.second, ct.subsecond_nanos);
        }
      } else {
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d", ct.hour, ct.minute,
                 ct.second);
      }
      return Value(std::string(buf));
    }
    if (arguments.size() >= 3) {
      int h = arguments[0].type == ValueType::kInt64
                  ? arguments[0].value.int_value
                  : std::stoi(raw_str(arguments[0]));
      int m = arguments[1].type == ValueType::kInt64
                  ? arguments[1].value.int_value
                  : std::stoi(raw_str(arguments[1]));
      int s = arguments[2].type == ValueType::kInt64
                  ? arguments[2].value.int_value
                  : std::stoi(raw_str(arguments[2]));
      char buf[64];
      if (arguments.size() == 4) {
        int sub = arguments[3].type == ValueType::kInt64
                      ? arguments[3].value.int_value
                      : std::stoi(raw_str(arguments[3]));
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", h, m, s, sub);
      } else {
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h, m, s);
      }
      return Value(std::string(buf));
    }
  }
  if (name == "__quantified__") {
    // __quantified__(lhs, array, op, ANY|ALL): three-valued SQL semantics.
    if (arguments.size() != 4) {
      throw std::runtime_error("quantified comparison requires 4 arguments");
    }
    const Value& lhs = arguments[0];
    Value arr = arguments[1];
    const std::string op = raw_str(arguments[2]);
    const std::string mode = raw_str(arguments[3]);
    // UNNEST over a NULL array produces zero rows: the comparison is vacuous
    // (ALL -> TRUE, ANY -> FALSE), not UNKNOWN.
    if (arr.IsNull()) {
      if (mode == "ANY" || mode == "SOME") {
        return Value(int64_t{0});
      }
      return Value(int64_t{1});
    }
    if (!arr.IsArray()) {
      if (arr.IsNull()) {
        // Handled above; unreachable, kept for clarity.
        throw std::runtime_error("quantified comparison requires an array");
      }
      // Field traversals collapse a single-occurrence repeated field to its
      // scalar; UNNEST iterates it as a one-element array.
      arr = Value::Array({std::move(arr)},
                         ElementSqlTypeName(arr.type).empty()
                             ? "INT64"
                             : ElementSqlTypeName(arr.type));
    }
    struct OpEntry {
      const char* text;
      BinaryOperation op;
    };
    static constexpr OpEntry kOps[] = {
        {"=", BinaryOperation::kEquals},
        {"!=", BinaryOperation::kNotEquals},
        {"<>", BinaryOperation::kNotEquals},
        {"<", BinaryOperation::kLessThan},
        {"<=", BinaryOperation::kLessThanEquals},
        {">", BinaryOperation::kGreaterThan},
        {">=", BinaryOperation::kGreaterThanEquals},
    };
    BinaryOperation operation = BinaryOperation::kEquals;
    bool found = false;
    for (const auto& entry : kOps) {
      if (op == entry.text) {
        operation = entry.op;
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error("quantified comparison: unsupported operator " +
                               op);
    }
    const bool is_any = mode == "ANY" || mode == "SOME";
    // Collation resolution: an explicit case-insensitive collator on any
    // element applies to the whole comparison set.
    bool any_ci = lhs.IsCaseInsensitive();
    for (const Value& element : arr.ArrayElements()) {
      any_ci = any_ci || element.IsCaseInsensitive();
    }
    Value test_value = lhs;
    if (any_ci && lhs.type == ValueType::kVarChar) {
      std::vector<Value> folded;
      folded.reserve(arr.ArrayElements().size());
      for (const Value& element : arr.ArrayElements()) {
        if (element.type == ValueType::kVarChar) {
          folded.push_back(Value(FoldCase(element.value.varchar_value))
                               .WithCollation(element.Collation()));
        } else {
          folded.push_back(element);
        }
      }
      arr = Value::Array(std::move(folded), arr.ArrayElementSqlType());
      test_value = Value(FoldCase(lhs.value.varchar_value))
                       .WithCollation(lhs.Collation());
    }
    bool saw_true = false;
    bool saw_null = false;
    // Collated LIKE rejects the '_' wildcard outright (validation error, not
    // a per-row UNKNOWN).
    if (operation == BinaryOperation::kLike ||
        operation == BinaryOperation::kNotLike) {
      if (any_ci) {
        for (const Value& element : arr.ArrayElements()) {
          if (!element.IsNull() && element.type == ValueType::kVarChar &&
              std::string_view(element.value.varchar_value).find('_') !=
                  std::string_view::npos) {
            throw std::runtime_error(
                "LIKE pattern has '_' which is not allowed when its operands "
                "have collation: " +
                std::string(element.value.varchar_value));
          }
        }
      }
    }
    for (const Value& element : arr.ArrayElements()) {
      Value result;
      try {
        result = Binary(operation, test_value, element);
      } catch (...) {
        result = Value();
      }
      if (result.IsNull()) {
        saw_null = true;
        continue;
      }
      if (Truthy(result)) {
        saw_true = true;
        if (is_any) {
          return Value(int64_t{1});
        }
      } else if (!is_any) {
        return Value(int64_t{0});
      }
    }
    if (is_any) {
      if (saw_true) {
        return Value(int64_t{1});
      }
      return saw_null ? Value() : Value(int64_t{0});
    }
    // ALL: true when nothing was false and no NULL blocked certainty.
    if (saw_null) {
      return {};
    }
    return Value(int64_t{1});
  }
  if (name == "array_element_offset" || name == "array_element_ordinal" ||
      name == "array_element_safe_offset" ||
      name == "array_element_safe_ordinal") {
    if (arguments.size() != 2) {
      throw std::runtime_error("array element access requires 2 arguments");
    }
    const bool safe = name.find("safe") != std::string::npos;
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return {};
    }
    if (!arr.IsArray()) {
      throw std::runtime_error("array element access requires an array");
    }
    // A NULL index yields NULL rather than an error (even for non-SAFE).
    if (arguments[1].IsNull()) {
      return {};
    }
    const auto& elements = arr.ArrayElements();
    int64_t index = arguments[1].value.int_value;
    if (name.find("ordinal") != std::string::npos) {
      --index;
    }
    if (index < 0 || index >= static_cast<int64_t>(elements.size())) {
      // Out-of-range plain accesses are errors in GoogleSQL; only the SAFE
      // variants yield NULL.
      if (!safe) {
        throw std::out_of_range("Array index " + std::to_string(index) +
                                " is out of bounds");
      }
      return {};
    }
    return elements[static_cast<size_t>(index)];
  }
  if (name == "__bit_and" || name == "__bit_or" || name == "__bit_xor" ||
      name == "__shift_left" || name == "__shift_right") {
    if (arguments.size() != 2) {
      throw std::runtime_error(name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const int64_t lhs = arguments[0].value.int_value;
    const int64_t rhs = arguments[1].value.int_value;
    if (name == "__bit_and") { return Value(lhs & rhs); }
    if (name == "__bit_or") { return Value(lhs | rhs); }
    if (name == "__bit_xor") { return Value(lhs ^ rhs); }
    if (rhs < 0 || rhs >= 64) {
      throw std::out_of_range("shift amount out of range");
    }
    const uint64_t ulhs = static_cast<uint64_t>(lhs);
    const uint64_t shifted =
        name == "__shift_left" ? ulhs << rhs : ulhs >> rhs;
    return Value(static_cast<int64_t>(shifted));
  }
  if (name == "unix_millis") {
    if (arguments.size() != 1) {
      throw std::runtime_error("unix_millis requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    // TIMESTAMP values carry micros; a millisecond grid truncates them.
    const std::string text = raw_str(arguments[0]);
    std::tm tm{};
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    long fractional = 0;
    const char* tz = nullptr;
    if (sscanf(text.c_str(), "%d-%d-%d %d:%d:%d.%ld", &year, &month, &day,
               &hour, &minute, &second, &fractional) >= 6) {
      tm.tm_year = year - 1900;
      tm.tm_mon = month - 1;
      tm.tm_mday = day;
      tm.tm_hour = hour;
      tm.tm_min = minute;
      tm.tm_sec = second;
      tm.tm_isdst = 0;
      const int64_t epoch_seconds = static_cast<int64_t>(timegm(&tm));
      const int64_t millis =
          epoch_seconds * 1000 + (fractional > 999 ? fractional / 1000 : fractional);
      return Value(millis);
    }
    tz = strpbrk(text.c_str(), "T ");
    if (tz == nullptr) {
      throw std::runtime_error("unix_millis requires a TIMESTAMP");
    }
    throw std::runtime_error("unix_millis requires a TIMESTAMP");
  }
  if (name == "__get_field_safe") {
    // Field access tolerating NULL bases / missing members (returns NULL);
    // used for dotted struct references inside DML predicates.
    if (arguments.size() != 2) {
      throw std::runtime_error("__get_field_safe requires 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string object = raw_str(arguments[0]);
    const std::string field_name = raw_str(arguments[1]);
    if (object.size() >= 2 && object.front() == '{' &&
        object.back() == '}') {
      const auto members =
          SplitJsonObjectMembers(object.substr(1, object.size() - 2));
      for (const auto& [key, text] : members) {
        if (key.size() == field_name.size() &&
            std::equal(key.begin(), key.end(), field_name.begin(),
                       [](char lhs, char rhs) {
                         return std::tolower(static_cast<unsigned char>(lhs)) ==
                                std::tolower(static_cast<unsigned char>(rhs));
                       })) {
          Value parsed;
          if (!JsonTextToValue(text, &parsed)) {
            return {};
          }
          return parsed;
        }
      }
      // Anonymous struct members (`STRUCT(2)` stores {"f1":2}) are still
      // addressable by any field reference when unambiguous: a single-member
      // object exposes its only value positionally.
      if (members.size() == 1) {
        Value parsed;
        if (!JsonTextToValue(members.front().second, &parsed)) {
          return {};
        }
        return parsed;
      }
    }
    // Proto text-format cells (`value: 1`) carry the same field semantics.
    Value proto_field;
    if (ProtoTextExtractField(object, field_name, &proto_field)) {
      return proto_field;
    }
    return {};
  }
  if (name == "get_field") {
    if (arguments.size() != 2) {
      throw std::runtime_error("get_field requires 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string s0 = raw_str(arguments[0]);
    if (s0.empty()) {
      return {};
    }
    if (s0.find("\x18\x00") != std::string::npos ||
        (s0.size() >= 2 && s0[0] == '\x18')) {
      throw std::runtime_error("invalid datetime_micros in proto");
    }
    std::string field_name = raw_str(arguments[1]);
    if (s0.starts_with("{") && s0.ends_with("}")) {
      std::string search_key = "\"" + field_name + "\":";
      size_t pos = s0.find(search_key);
      if (pos == std::string::npos) {
        // Positional fallback: unnamed two-field struct literals (the
        // APPROX_TOP_* payload shape) resolve value / count / sum by
        // position.
        if (s0.find("\":") == std::string::npos) {
          std::string inner = s0.substr(1, s0.size() - 2);
          std::vector<std::string> fields;
          int depth = 0;
          bool in_str = false;
          char quote = '\0';
          std::string current;
          for (const char c : inner) {
            if (in_str) {
              current.push_back(c);
              if (c == quote) {
                in_str = false;
              }
              continue;
            }
            if (c == '"' || c == '\'') {
              in_str = true;
              quote = c;
              current.push_back(c);
              continue;
            }
            if (c == '{' || c == '[' || c == '(') {
              ++depth;
            }
            if ((c == '}' || c == ']' || c == ')') && depth > 0) {
              --depth;
            }
            if (c == ',' && depth == 0) {
              fields.push_back(current);
              current.clear();
              continue;
            }
            current.push_back(c);
          }
          fields.push_back(current);
          auto trim_copy = [](std::string v) {
            size_t b = v.find_first_not_of(" \t\n\r");
            size_t e = v.find_last_not_of(" \t\n\r");
            return b == std::string::npos ? std::string()
                                          : v.substr(b, e - b + 1);
          };
          for (std::string& field : fields) {
            field = trim_copy(field);
          }
          const bool two_fields = fields.size() == 2;
          if (two_fields && field_name == "value") {
            return Value(std::move(fields[0]));
          }
          if (two_fields && (field_name == "count" || field_name == "sum")) {
            return Value(std::move(fields[1]));
          }
        }
        return {};
      }
      size_t val_start = pos + search_key.size();
      size_t val_end = s0.find_first_of(",}", val_start);
      std::string val_str = s0.substr(val_start, val_end - val_start);
      if (val_str == "null") {
        return {};
      }
      if (val_str.size() >= 2 && val_str.front() == '"' &&
          val_str.back() == '"') {
        val_str = val_str.substr(1, val_str.size() - 2);
      }
      return Value(std::move(val_str));
    }
    return {};
  }
  if (name == "date_diff" || name == "datetime_diff" ||
      name == "timestamp_diff") {
    if (call.Args().size() != 3) {
      throw std::runtime_error(name + " requires 3 arguments");
    }
    const Value d1 = Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    const Value d2 = Evaluate(call.Args()[1], scope, aggregates, context, ctes);
    if (d1.IsNull() || d2.IsNull()) {
      return {};
    }
    std::string unit = "day";
    if (call.Args()[2]->Type() == TypeTag::kColumnValue) {
      unit = to_lower(call.Args()[2]->AsColumnValue().GetColumnName().name);
    } else {
      const Value uval =
          Evaluate(call.Args()[2], scope, aggregates, context, ctes);
      if (!uval.IsNull()) {
        unit = to_lower(raw_str(uval));
      }
    }
    CivilTime ct1 = ValueToCivilTime(d1);
    CivilTime ct2 = ValueToCivilTime(d2);
    std::string s1 = raw_str(d1);
    std::string s2 = raw_str(d2);
    if (s1.find('+') != std::string::npos ||
        s1.find('Z') != std::string::npos ||
        s1.find('z') != std::string::npos) {
      int64_t ns = CivilTimeToNanos(ct1) + (-8 * 3600LL) * 1000000000LL;
      ct1 = NanosToCivilTime(ns);
    }
    if (s2.find('+') != std::string::npos ||
        s2.find('Z') != std::string::npos ||
        s2.find('z') != std::string::npos) {
      int64_t ns = CivilTimeToNanos(ct2) + (-8 * 3600LL) * 1000000000LL;
      ct2 = NanosToCivilTime(ns);
    }
    auto floor_div = [](int64_t a, int64_t b) -> int64_t {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    if (unit == "year" || unit == "years") {
      return Value(static_cast<int64_t>(ct1.year - ct2.year));
    }
    if (unit == "quarter" || unit == "quarters") {
      return Value(
          static_cast<int64_t>((ct1.year - ct2.year) * 4 +
                               ((ct1.month - 1) / 3 - (ct2.month - 1) / 3)));
    }
    if (unit == "month" || unit == "months") {
      return Value(static_cast<int64_t>((ct1.year - ct2.year) * 12 +
                                        (ct1.month - ct2.month)));
    }

    auto trunc_to_unit = [](CivilTime ct, std::string_view u) -> CivilTime {
      if (u == "week" || u == "weeks") {
        std::chrono::year_month_day ymd{
            std::chrono::year{ct.year},
            std::chrono::month{static_cast<unsigned>(ct.month)},
            std::chrono::day{static_cast<unsigned>(ct.day)}};
        std::chrono::sys_days sd{ymd};
        int wd = std::chrono::weekday{sd}.iso_encoding();
        int days_back = (wd == 7) ? 0 : wd;
        std::chrono::sys_days week_start = sd - std::chrono::days{days_back};
        std::chrono::year_month_day res_ymd{week_start};
        ct.year = int(res_ymd.year());
        ct.month = unsigned(res_ymd.month());
        ct.day = unsigned(res_ymd.day());
        ct.hour = 0;
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "day" || u == "days") {
        ct.hour = 0;
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "hour" || u == "hours") {
        ct.minute = 0;
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "minute" || u == "minutes") {
        ct.second = 0;
        ct.subsecond_nanos = 0;
      } else if (u == "second" || u == "seconds") {
        ct.subsecond_nanos = 0;
      } else if (u == "millisecond" || u == "milliseconds") {
        ct.subsecond_nanos = (ct.subsecond_nanos / 1000000LL) * 1000000LL;
      } else if (u == "microsecond" || u == "microseconds") {
        ct.subsecond_nanos = (ct.subsecond_nanos / 1000LL) * 1000LL;
      }
      return ct;
    };

    CivilTime t1 = trunc_to_unit(ct1, unit);
    CivilTime t2 = trunc_to_unit(ct2, unit);
    int64_t ns1 = CivilTimeToNanos(t1);
    int64_t ns2 = CivilTimeToNanos(t2);
    int64_t diff_ns = ns1 - ns2;
    if (unit == "week" || unit == "weeks") {
      return Value(diff_ns / (7LL * 86400LL * 1000000000LL));
    }
    if (unit == "day" || unit == "days") {
      return Value(diff_ns / (86400LL * 1000000000LL));
    }
    if (unit == "hour" || unit == "hours") {
      return Value(diff_ns / (3600LL * 1000000000LL));
    }
    if (unit == "minute" || unit == "minutes") {
      return Value(diff_ns / (60LL * 1000000000LL));
    }
    if (unit == "second" || unit == "seconds") {
      return Value(diff_ns / 1000000000LL);
    }
    if (unit == "millisecond" || unit == "milliseconds") {
      return Value(diff_ns / 1000000LL);
    }
    if (unit == "microsecond" || unit == "microseconds") {
      return Value(diff_ns / 1000LL);
    }
    if (unit == "nanosecond" || unit == "nanoseconds") {
      return Value(diff_ns);
    }
    throw std::runtime_error("unsupported unit in " + name + ": " + unit);
  }
  if (name == "date_trunc" || name == "datetime_trunc" ||
      name == "timestamp_trunc") {
    if (call.Args().size() < 2 || call.Args().size() > 3) {
      throw std::runtime_error(name + " requires 2 or 3 arguments");
    }
    const Value d = Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    if (d.IsNull()) {
      return {};
    }
    std::string unit = "day";
    if (call.Args()[1]->Type() == TypeTag::kColumnValue) {
      unit = to_lower(call.Args()[1]->AsColumnValue().GetColumnName().name);
    } else {
      const Value uval =
          Evaluate(call.Args()[1], scope, aggregates, context, ctes);
      if (!uval.IsNull()) {
        unit = to_lower(raw_str(uval));
      }
    }
    std::string s_orig = raw_str(d);
    bool has_explicit_tz = (s_orig.find('+') != std::string::npos ||
                            s_orig.find('Z') != std::string::npos ||
                            s_orig.find('z') != std::string::npos);
    if (!has_explicit_tz) {
      for (size_t i = 10; i < s_orig.size(); ++i) {
        if (s_orig[i] == '+' || s_orig[i] == '-') {
          has_explicit_tz = true;
          break;
        }
      }
    }
    bool is_timestamp = (name == "timestamp_trunc" || has_explicit_tz);
    std::string trunc_tz = GetDefaultTimeZone();
    if (call.Args().size() == 3) {
      const Value tz_val =
          Evaluate(call.Args()[2], scope, aggregates, context, ctes);
      if (tz_val.IsNull()) {
        return {};
      }
      std::string tz_str = raw_str(tz_val);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      trunc_tz = tz_str;
    }

    CivilTime ct = ValueToCivilTime(d);
    if (is_timestamp) {
      int64_t d_utc_ns = CivilTimeToNanos(ct);
      if (!has_explicit_tz) {
        int parse_tz_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
        d_utc_ns -= parse_tz_sec * 1000000000LL;
      }
      int trunc_tz_sec = ParseTimeZoneOffset(trunc_tz, &ct, -8 * 3600);
      CivilTime trunc_local_ct =
          NanosToCivilTime(d_utc_ns + trunc_tz_sec * 1000000000LL);
      if (unit == "year" || unit == "years") {
        trunc_local_ct.month = 1;
        trunc_local_ct.day = 1;
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "quarter" || unit == "quarters") {
        trunc_local_ct.month = ((trunc_local_ct.month - 1) / 3) * 3 + 1;
        trunc_local_ct.day = 1;
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "month" || unit == "months") {
        trunc_local_ct.day = 1;
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "week" || unit == "weeks") {
        std::chrono::year_month_day ymd{
            std::chrono::year{trunc_local_ct.year},
            std::chrono::month{static_cast<unsigned>(trunc_local_ct.month)},
            std::chrono::day{static_cast<unsigned>(trunc_local_ct.day)}};
        std::chrono::sys_days sd{ymd};
        int wd = std::chrono::weekday{sd}.iso_encoding();
        int days_back = (wd == 7) ? 0 : wd;
        std::chrono::sys_days week_start = sd - std::chrono::days{days_back};
        std::chrono::year_month_day res_ymd{week_start};
        trunc_local_ct.year = int(res_ymd.year());
        trunc_local_ct.month = unsigned(res_ymd.month());
        trunc_local_ct.day = unsigned(res_ymd.day());
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "day" || unit == "days") {
        trunc_local_ct.hour = 0;
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "hour" || unit == "hours") {
        trunc_local_ct.minute = 0;
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "minute" || unit == "minutes") {
        trunc_local_ct.second = 0;
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "second" || unit == "seconds") {
        trunc_local_ct.subsecond_nanos = 0;
      } else if (unit == "millisecond" || unit == "milliseconds") {
        trunc_local_ct.subsecond_nanos =
            (trunc_local_ct.subsecond_nanos / 1000000LL) * 1000000LL;
      } else if (unit == "microsecond" || unit == "microseconds") {
        trunc_local_ct.subsecond_nanos =
            (trunc_local_ct.subsecond_nanos / 1000LL) * 1000LL;
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        // no change
      } else {
        throw std::runtime_error("unsupported unit in " + name + ": " + unit);
      }
      int new_trunc_tz_sec =
          ParseTimeZoneOffset(trunc_tz, &trunc_local_ct, -8 * 3600);
      int64_t res_utc_ns =
          CivilTimeToNanos(trunc_local_ct) - new_trunc_tz_sec * 1000000000LL;
      CivilTime res_utc_ct = NanosToCivilTime(res_utc_ns);
      return Value(FormatCivilTime(res_utc_ct) + "+00");
    }
    if (unit == "year" || unit == "years") {
      ct.month = 1;
      ct.day = 1;
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "quarter" || unit == "quarters") {
      ct.month = ((ct.month - 1) / 3) * 3 + 1;
      ct.day = 1;
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "month" || unit == "months") {
      ct.day = 1;
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "week" || unit == "weeks") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      std::chrono::sys_days sd{ymd};
      int wd = std::chrono::weekday{sd}.iso_encoding();
      int days_back = (wd == 7) ? 0 : wd;
      std::chrono::sys_days week_start = sd - std::chrono::days{days_back};
      std::chrono::year_month_day res_ymd{week_start};
      ct.year = int(res_ymd.year());
      ct.month = unsigned(res_ymd.month());
      ct.day = unsigned(res_ymd.day());
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "day" || unit == "days") {
      ct.hour = 0;
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "hour" || unit == "hours") {
      ct.minute = 0;
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "minute" || unit == "minutes") {
      ct.second = 0;
      ct.subsecond_nanos = 0;
    } else if (unit == "second" || unit == "seconds") {
      ct.subsecond_nanos = 0;
    } else if (unit == "millisecond" || unit == "milliseconds") {
      ct.subsecond_nanos = (ct.subsecond_nanos / 1000000LL) * 1000000LL;
    } else if (unit == "microsecond" || unit == "microseconds") {
      ct.subsecond_nanos = (ct.subsecond_nanos / 1000LL) * 1000LL;
    } else if (unit == "nanosecond" || unit == "nanoseconds") {
      // no change
    } else {
      throw std::runtime_error("unsupported unit in " + name + ": " + unit);
    }
    if (d.type == ValueType::kDate && name == "date_trunc") {
      std::chrono::year_month_day ymd{
          std::chrono::year{ct.year},
          std::chrono::month{static_cast<unsigned>(ct.month)},
          std::chrono::day{static_cast<unsigned>(ct.day)}};
      return Value::DateFromDays(
          std::chrono::sys_days{ymd}.time_since_epoch().count());
    }
    return Value(FormatCivilTime(ct));
  }
  if (name == "format_date" || name == "format_datetime" ||
      name == "format_timestamp") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error(name + " takes 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    std::string fmt = raw_str(arguments[0]);
    CivilTime ct = ValueToCivilTime(arguments[1]);
    int tz_offset_sec = 0;
    if (arguments.size() == 3) {
      if (arguments[2].IsNull()) {
        return {};
      }
      std::string tz_str = raw_str(arguments[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str, &ct);
    } else if (name == "format_datetime" || name == "format_timestamp") {
      tz_offset_sec = ParseTimeZoneOffset(GetDefaultTimeZone(), &ct, -8 * 3600);
    }
    ct = ShiftCivilTimeHours(ct, tz_offset_sec / 3600);
    int rem_mins = (tz_offset_sec % 3600) / 60;
    if (rem_mins != 0) {
      int total_m = ct.minute + rem_mins;
      if (total_m >= 60) {
        ct.minute = total_m - 60;
        ct = ShiftCivilTimeHours(ct, 1);
      } else if (total_m < 0) {
        ct.minute = total_m + 60;
        ct = ShiftCivilTimeHours(ct, -1);
      } else {
        ct.minute = total_m;
      }
    }
    struct tm tm = {};
    tm.tm_year = ct.year - 1900;
    tm.tm_mon = ct.month - 1;
    tm.tm_mday = ct.day;
    tm.tm_hour = ct.hour;
    tm.tm_min = ct.minute;
    tm.tm_sec = ct.second;
    timegm(&tm);
    char buf[128];
    strftime(buf, sizeof(buf), fmt.c_str(), &tm);
    return Value(std::string(buf));
  }
  if (name == "parse_timestamp") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error("PARSE_TIMESTAMP requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    if (arguments.size() == 3 && arguments[2].IsNull()) {
      return {};
    }
    std::string fmt = raw_str(arguments[0]);
    std::string input = raw_str(arguments[1]);
    int tz_offset_sec =
        ParseTimeZoneOffset(GetDefaultTimeZone(), nullptr, -8 * 3600);
    if (arguments.size() == 3) {
      std::string tz_str = raw_str(arguments[2]);
      if (tz_str.empty() || tz_str == "invalid_time_zone") {
        throw std::runtime_error("invalid timezone: " + tz_str);
      }
      tz_offset_sec = ParseTimeZoneOffset(tz_str);
    }
    struct tm tm = {};
    tm.tm_year = 100;  // 2000 default
    tm.tm_mon = 0;
    tm.tm_mday = 1;
    char* parsed_end = strptime(input.c_str(), fmt.c_str(), &tm);
    if (parsed_end == nullptr) {
      throw std::runtime_error("PARSE_TIMESTAMP failed for: " + input);
    }
    CivilTime ct;
    ct.year = tm.tm_year + 1900;
    ct.month = tm.tm_mon + 1;
    ct.day = tm.tm_mday;
    ct.hour = tm.tm_hour;
    ct.minute = tm.tm_min;
    ct.second = tm.tm_sec;
    ct = ShiftCivilTimeHours(ct, -tz_offset_sec / 3600);
    int rem_mins = (tz_offset_sec % 3600) / 60;
    if (rem_mins != 0) {
      int total_m = ct.minute - rem_mins;
      if (total_m >= 60) {
        ct.minute = total_m - 60;
        ct = ShiftCivilTimeHours(ct, 1);
      } else if (total_m < 0) {
        ct.minute = total_m + 60;
        ct = ShiftCivilTimeHours(ct, -1);
      } else {
        ct.minute = total_m;
      }
    }
    return Value(FormatCivilTime(ct) + "+00");
  }
  if (name == "date_bucket" || name == "timestamp_bucket" ||
      name == "datetime_bucket") {
    if (call.Args().size() < 2 || call.Args().size() > 3) {
      throw std::runtime_error(name + " takes 2 or 3 arguments");
    }
    const Value d = Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    if (d.IsNull()) {
      return {};
    }
    CivilTime d_ct = ValueToCivilTime(d);
    std::string d_s = raw_str(d);
    bool d_has_tz = (d_s.find('+') != std::string::npos ||
                     d_s.find('Z') != std::string::npos ||
                     d_s.find('z') != std::string::npos);
    if (!d_has_tz) {
      for (size_t i = 10; i < d_s.size(); ++i) {
        if (d_s[i] == '+' || d_s[i] == '-') {
          d_has_tz = true;
          break;
        }
      }
    }
    bool is_ts = (name == "timestamp_bucket" || d_has_tz);

    CivilTime orig_ct{1970, 1, 1, 0, 0, 0, 0};
    bool has_origin = false;
    bool orig_has_tz = false;
    if (call.Args().size() == 3) {
      const Value orig =
          Evaluate(call.Args()[2], scope, aggregates, context, ctes);
      if (orig.IsNull()) {
        return {};
      }
      orig_ct = ValueToCivilTime(orig);
      std::string orig_s = raw_str(orig);
      orig_has_tz = (orig_s.find('+') != std::string::npos ||
                     orig_s.find('Z') != std::string::npos ||
                     orig_s.find('z') != std::string::npos);
      if (!orig_has_tz) {
        for (size_t i = 10; i < orig_s.size(); ++i) {
          if (orig_s[i] == '+' || orig_s[i] == '-') {
            orig_has_tz = true;
            break;
          }
        }
      }
      if (orig_has_tz) {
        is_ts = true;
      }
      has_origin = true;
    }
    if (call.Args()[1]->Type() == TypeTag::kIntervalExp) {
      const auto& interval = call.Args()[1]->AsIntervalExpression();
      int64_t amount = interval.Amount();
      std::string unit = to_lower(std::string(interval.Unit()));
      double sec_amount = 0.0;
      bool is_fractional_second = false;
      if (unit == "second" || unit == "seconds") {
        std::string raw_amount = interval.RawAmount();
        if (!raw_amount.empty() && raw_amount.find('.') != std::string::npos) {
          sec_amount = std::stod(raw_amount);
          is_fractional_second = true;
        }
      }
      auto floor_div = [](int64_t a, int64_t b) -> int64_t {
        const int64_t q = a / b;
        return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
      };
      if (unit == "month" || unit == "months" || unit == "quarter" ||
          unit == "quarters" || unit == "year" || unit == "years") {
        int64_t step_m = amount;
        if (unit.starts_with("year")) {
          step_m = amount * 12;
        } else if (unit.starts_with("quarter")) {
          step_m = amount * 3;
        }
        int64_t m_diff =
            (d_ct.year - orig_ct.year) * 12 + (d_ct.month - orig_ct.month);
        int64_t bucket_m = floor_div(m_diff, step_m) * step_m;
        int64_t total_m = (orig_ct.year * 12 + orig_ct.month - 1) + bucket_m;
        int bucket_y = floor_div(total_m, 12);
        int bucket_mon = total_m - bucket_y * 12 + 1;
        CivilTime res_ct = orig_ct;
        res_ct.year = bucket_y;
        res_ct.month = bucket_mon;
        if (d.type == ValueType::kDate && name == "date_bucket") {
          std::chrono::year_month_day ymd{
              std::chrono::year{res_ct.year},
              std::chrono::month{static_cast<unsigned>(res_ct.month)},
              std::chrono::day{static_cast<unsigned>(res_ct.day)}};
          return Value::DateFromDays(
              std::chrono::sys_days{ymd}.time_since_epoch().count());
        }
        if (is_ts) {
          return Value(FormatCivilTime(res_ct) + "+00");
        }
        return Value(FormatCivilTime(res_ct));
      }

      int64_t step_ns = 0;
      if (is_fractional_second) {
        step_ns = static_cast<int64_t>(std::round(sec_amount * 1000000000.0));
      } else if (unit == "nanosecond" || unit == "nanoseconds") {
        step_ns = amount;
      } else if (unit == "microsecond" || unit == "microseconds") {
        step_ns = amount * 1000LL;
      } else if (unit == "millisecond" || unit == "milliseconds") {
        step_ns = amount * 1000000LL;
      } else if (unit == "second" || unit == "seconds") {
        step_ns = amount * 1000000000LL;
      } else if (unit == "minute" || unit == "minutes") {
        step_ns = amount * 60LL * 1000000000LL;
      } else if (unit == "hour" || unit == "hours") {
        step_ns = amount * 3600LL * 1000000000LL;
      } else if (unit == "day" || unit == "days") {
        step_ns = amount * 86400LL * 1000000000LL;
        if (!has_origin && amount % 7 == 0) {
          orig_ct.year = 1970;
          orig_ct.month = 1;
          orig_ct.day = 4;  // Sunday
        }
      } else if (unit == "week" || unit == "weeks") {
        step_ns = amount * 7LL * 86400LL * 1000000000LL;
        if (!has_origin) {
          orig_ct.year = 1970;
          orig_ct.month = 1;
          orig_ct.day = 4;  // Sunday
        }
      } else {
        throw std::runtime_error("unsupported interval unit in " + name + ": " +
                                 unit);
      }

      int64_t d_ns = CivilTimeToNanos(d_ct);
      if (is_ts && !d_has_tz) {
        int tz_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &d_ct, -8 * 3600);
        d_ns -= tz_sec * 1000000000LL;
      }
      int64_t orig_ns = CivilTimeToNanos(orig_ct);
      if (is_ts && (!has_origin || !orig_has_tz)) {
        int tz_sec =
            ParseTimeZoneOffset(GetDefaultTimeZone(), &orig_ct, -8 * 3600);
        orig_ns -= tz_sec * 1000000000LL;
      }
      int64_t diff_ns = d_ns - orig_ns;
      int64_t bucket_ns = orig_ns + floor_div(diff_ns, step_ns) * step_ns;
      CivilTime res_ct = NanosToCivilTime(bucket_ns);

      if (d.type == ValueType::kDate && name == "date_bucket") {
        std::chrono::year_month_day ymd{
            std::chrono::year{res_ct.year},
            std::chrono::month{static_cast<unsigned>(res_ct.month)},
            std::chrono::day{static_cast<unsigned>(res_ct.day)}};
        return Value::DateFromDays(
            std::chrono::sys_days{ymd}.time_since_epoch().count());
      }
      if (is_ts) {
        return Value(FormatCivilTime(res_ct) + "+00");
      }
      return Value(FormatCivilTime(res_ct));
    }
    return {};
  }
  if (name == "parse_date") {
    if (arguments.size() != 2) {
      throw std::runtime_error("PARSE_DATE requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    std::string fmt = raw_str(arguments[0]);
    std::string val = raw_str(arguments[1]);
    for (char c : val) {
      if (c == '\r' || c == '\n' || c == '\0') {
        throw std::runtime_error("invalid character in PARSE_DATE: " + val);
      }
    }
    if (fmt.find(' ') == std::string::npos &&
        val.find(' ') != std::string::npos) {
      throw std::runtime_error("unexpected whitespace in PARSE_DATE: " + val);
    }
    if (fmt.find("%C") != std::string::npos ||
        fmt.find("%g") != std::string::npos) {
      throw std::runtime_error("incomplete date format in PARSE_DATE");
    }
    if (fmt == "%W%y") {
      if (val.size() < 3 || val.size() > 4) {
        throw std::runtime_error("invalid format for %W%y: " + val);
      }
      for (char c : val) {
        if (c < '0' || c > '9') {
          throw std::runtime_error("invalid digit in PARSE_DATE: " + val);
        }
      }
      int w = 0, y = 0;
      if (val.size() == 3) {
        w = std::stoi(val.substr(0, 2));
        y = std::stoi(val.substr(2, 1));
      } else {
        w = std::stoi(val.substr(0, 2));
        y = std::stoi(val.substr(2, 2));
      }
      if (w < 0 || w > 53) {
        throw std::runtime_error("week out of range in PARSE_DATE: " + val);
      }
      int full_year = y < 69 ? 2000 + y : 1900 + y;
      std::chrono::year_month_day jan1{std::chrono::year{full_year},
                                       std::chrono::month{1},
                                       std::chrono::day{1}};
      std::chrono::sys_days jan1_days{jan1};
      std::chrono::weekday wd{jan1_days};
      int jan1_wd = wd.iso_encoding();  // 1=Monday .. 7=Sunday
      int days_to_first_monday = (jan1_wd == 1) ? 0 : (8 - jan1_wd);
      std::chrono::sys_days target_days =
          jan1_days + std::chrono::days{days_to_first_monday + (w - 1) * 7};
      return Value::DateFromDays(target_days.time_since_epoch().count());
    }
    struct tm tm = {};
    tm.tm_year = 70;
    tm.tm_mday = 1;
    char* res = strptime(val.c_str(), fmt.c_str(), &tm);
    if (res == nullptr || *res != '\0') {
      throw std::runtime_error("failed to parse date: " + val);
    }
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm.tm_year + 1900,
             tm.tm_mon + 1, tm.tm_mday);
    return Value::Date(std::string(buf));
  }
  if (name == "last_day") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("LAST_DAY takes 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    int64_t days = parse_date_val(arguments[0]);
    std::string part = "month";
    if (arguments.size() == 2) {
      part = to_lower(raw_str(arguments[1]));
    }
    using std::chrono::day;
    using std::chrono::month;
    using std::chrono::month_day_last;
    using std::chrono::sys_days;
    using std::chrono::year;
    using std::chrono::year_month_day;
    using std::chrono::year_month_day_last;

    year_month_day ymd{sys_days{std::chrono::days{days}}};
    int y = int(ymd.year());
    unsigned m = unsigned(ymd.month());

    if (part == "year" || part == "years") {
      year_month_day last_ymd{year{y}, month{12}, day{31}};
      return Value::DateFromDays(sys_days{last_ymd}.time_since_epoch().count());
    }
    if (part == "month" || part == "months") {
      year_month_day last_ymd{
          year_month_day_last{year{y}, month_day_last{month{m}}}};
      return Value::DateFromDays(sys_days{last_ymd}.time_since_epoch().count());
    }
    if (part == "quarter" || part == "quarters") {
      unsigned q_last_month = ((m - 1) / 3 + 1) * 3;
      year_month_day last_ymd{
          year_month_day_last{year{y}, month_day_last{month{q_last_month}}}};
      return Value::DateFromDays(sys_days{last_ymd}.time_since_epoch().count());
    }
    if (part == "week" || part == "weeks" || part.starts_with("week(")) {
      int last_day_iso = 6;
      if (part == "week(monday)") {
        last_day_iso = 7;
      } else if (part == "week(tuesday)") {
        last_day_iso = 1;
      } else if (part == "week(wednesday)") {
        last_day_iso = 2;
      } else if (part == "week(thursday)") {
        last_day_iso = 3;
      } else if (part == "week(friday)") {
        last_day_iso = 4;
      } else if (part == "week(saturday)") {
        last_day_iso = 5;
      } else if (part == "week(sunday)") {
        last_day_iso = 6;
      }
      std::chrono::sys_days cur_days{std::chrono::days{days}};
      int cur_iso = std::chrono::weekday{cur_days}.iso_encoding();
      int add_days = (last_day_iso >= cur_iso) ? (last_day_iso - cur_iso)
                                               : (7 + last_day_iso - cur_iso);
      return Value::DateFromDays(days + add_days);
    }
    if (part == "isoyear") {
      year_month_day dec31{year{y}, month{12}, day{31}};
      sys_days dec31_days{dec31};
      int iso_wd = std::chrono::weekday{dec31_days}.iso_encoding();
      int days_to_sun =
          (iso_wd == 7) ? 0 : ((iso_wd >= 4) ? (7 - iso_wd) : -(iso_wd));
      return Value::DateFromDays((dec31_days + std::chrono::days{days_to_sun})
                                     .time_since_epoch()
                                     .count());
    }
    throw std::runtime_error("unsupported LAST_DAY date_part " + part);
  }
  if (name == "week" && arguments.size() == 1) {
    return Value("week(" + to_lower(raw_str(arguments[0])) + ")");
  }
  if (name == "add_months") {
    if (arguments.size() != 2) {
      throw std::runtime_error("ADD_MONTHS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    int64_t days = parse_date_val(arguments[0]);
    int64_t n = arguments[1].type == ValueType::kInt64
                    ? arguments[1].value.int_value
                    : std::stoll(raw_str(arguments[1]));
    using std::chrono::day;
    using std::chrono::month;
    using std::chrono::month_day_last;
    using std::chrono::sys_days;
    using std::chrono::year;
    using std::chrono::year_month_day;
    using std::chrono::year_month_day_last;

    year_month_day ymd{sys_days{std::chrono::days{days}}};
    year_month_day_last last_of_orig_month{ymd.year(),
                                           month_day_last{ymd.month()}};
    bool is_last_day_of_month = (ymd.day() == last_of_orig_month.day());

    auto floor_div = [](int64_t a, int64_t b) -> int64_t {
      const int64_t q = a / b;
      return ((a % b) != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
    };
    int64_t total_m = (int(ymd.year()) * 12 + unsigned(ymd.month()) - 1) + n;
    int target_y = floor_div(total_m, 12);
    if (target_y < 1 || target_y > 9999) {
      throw std::runtime_error("DATE value out of range");
    }
    int target_m = total_m - target_y * 12 + 1;

    year_month_day_last last_of_target{
        year{target_y}, month_day_last{month{static_cast<unsigned>(target_m)}}};
    unsigned target_day = unsigned(ymd.day());
    if (is_last_day_of_month || target_day > unsigned(last_of_target.day())) {
      target_day = unsigned(last_of_target.day());
    }
    year_month_day target_ymd{year{target_y},
                              month{static_cast<unsigned>(target_m)},
                              day{target_day}};
    int64_t res_days = sys_days{target_ymd}.time_since_epoch().count();
    if (res_days < -719162 || res_days > 2932896) {
      throw std::runtime_error("DATE value out of range");
    }

    if (arguments[0].type == ValueType::kDate) {
      return Value::DateFromDays(res_days);
    }
    std::string s0 = raw_str(arguments[0]);
    if (s0.size() > 10) {
      return Value(FormatDateDays(res_days) + s0.substr(10));
    }
    return Value::DateFromDays(res_days);
  }
  if (name == "next_day") {
    if (arguments.size() != 2) {
      throw std::runtime_error("NEXT_DAY requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    int64_t days = parse_date_val(arguments[0]);
    std::string target_day_str = to_lower(raw_str(arguments[1]));
    int target_iso = 0;
    if (target_day_str.starts_with("mon")) {
      target_iso = 1;
    } else if (target_day_str.starts_with("tue")) {
      target_iso = 2;
    } else if (target_day_str.starts_with("wed")) {
      target_iso = 3;
    } else if (target_day_str.starts_with("thu")) {
      target_iso = 4;
    } else if (target_day_str.starts_with("fri")) {
      target_iso = 5;
    } else if (target_day_str.starts_with("sat")) {
      target_iso = 6;
    } else if (target_day_str.starts_with("sun")) {
      target_iso = 7;
    } else {
      throw std::runtime_error("invalid day name in NEXT_DAY: " +
                               target_day_str);
    }

    std::chrono::sys_days cur_days{std::chrono::days{days}};
    int cur_iso = std::chrono::weekday{cur_days}.iso_encoding();
    int diff = target_iso - cur_iso;
    int add_days = (diff > 0) ? diff : (diff + 7);
    int64_t res_days = days + add_days;
    if (res_days < -719162 || res_days > 2932896) {
      throw std::runtime_error("DATE value out of range");
    }

    return Value::DateFromDays(res_days);
  }
  if (name == "months_between") {
    if (arguments.size() != 2) {
      throw std::runtime_error("MONTHS_BETWEEN requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    int64_t days1 = parse_date_val(arguments[0]);
    int64_t days2 = parse_date_val(arguments[1]);
    using std::chrono::month_day_last;
    using std::chrono::sys_days;
    using std::chrono::year_month_day;
    using std::chrono::year_month_day_last;

    year_month_day ymd1{sys_days{std::chrono::days{days1}}};
    year_month_day ymd2{sys_days{std::chrono::days{days2}}};
    year_month_day_last last1{ymd1.year(), month_day_last{ymd1.month()}};
    year_month_day_last last2{ymd2.year(), month_day_last{ymd2.month()}};

    int y1 = int(ymd1.year()), y2 = int(ymd2.year());
    int m1 = unsigned(ymd1.month()), m2 = unsigned(ymd2.month());
    int d1 = unsigned(ymd1.day()), d2 = unsigned(ymd2.day());

    double full_months = static_cast<double>((y1 - y2) * 12 + (m1 - m2));

    bool is_last1 = (d1 == unsigned(last1.day()));
    bool is_last2 = (d2 == unsigned(last2.day()));

    if ((d1 == d2) || (is_last1 && is_last2)) {
      return Value(full_months);
    }

    std::string s1 = raw_str(arguments[0]);
    std::string s2 = raw_str(arguments[1]);
    double time_frac1 = 0.0;
    double time_frac2 = 0.0;
    int h = 0, m = 0, s = 0;
    if (s1.size() > 11 &&
        sscanf(s1.c_str() + 11, "%d:%d:%d", &h, &m, &s) >= 2) {
      time_frac1 = (h * 3600 + m * 60 + s) / 86400.0;
    }
    if (s2.size() > 11 &&
        sscanf(s2.c_str() + 11, "%d:%d:%d", &h, &m, &s) >= 2) {
      time_frac2 = (h * 3600 + m * 60 + s) / 86400.0;
    }

    double day_diff = (d1 + time_frac1) - (d2 + time_frac2);
    return Value(full_months + day_diff / 31.0);
  }

  auto utf8_offsets = [](std::string_view s) -> std::vector<size_t> {
    std::vector<size_t> offsets;
    offsets.reserve(s.size() + 1);
    for (size_t i = 0; i < s.size();) {
      offsets.push_back(i);
      const unsigned char c = static_cast<unsigned char>(s[i]);
      if ((c & 0x80) == 0) {
        i += 1;
      } else if ((c & 0xE0) == 0xC0) {
        i += (i + 1 < s.size()) ? 2 : 1;
      } else if ((c & 0xF0) == 0xE0) {
        i += (i + 2 < s.size()) ? 3 : (i + 1 < s.size() ? 2 : 1);
      } else if ((c & 0xF8) == 0xF0) {
        i += (i + 3 < s.size())
                 ? 4
                 : (i + 2 < s.size() ? 3 : (i + 1 < s.size() ? 2 : 1));
      } else {
        i += 1;
      }
    }
    offsets.push_back(s.size());
    return offsets;
  };

  auto utf8_len = [&](std::string_view s) -> size_t {
    return utf8_offsets(s).size() - 1;
  };

  auto utf8_substr = [&](std::string_view s, size_t start_cp,
                         size_t count_cp) -> std::string {
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    if (start_cp >= total_cps) {
      return "";
    }
    const size_t end_cp = std::min(start_cp + count_cp, total_cps);
    const size_t byte_start = offsets[start_cp];
    const size_t byte_end = offsets[end_cp];
    return std::string(s.substr(byte_start, byte_end - byte_start));
  };

  // String functions
  if (name == "concat") {
    std::string result;
    for (const Value& value : arguments) {
      if (value.IsNull()) {
        return {};
      }
      if (value.type != ValueType::kVarChar) {
        result.append(value.AsString());
      } else {
        result.append(value.value.varchar_value);
      }
    }
    return Value(std::move(result));
  }
  if (name == "byte_length" || name == "octet_length") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    return Value(static_cast<int64_t>(raw_str(arguments[0]).size()));
  }
  if (name == "length" || name == "char_length" || name == "character_length") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    return Value(static_cast<int64_t>(utf8_len(raw_str(arguments[0]))));
  }
  if (name == "upper") {
    if (arguments.size() != 1) {
      throw std::runtime_error("UPPER requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    for (char& c : s) {
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return Value(std::move(s));
  }
  if (name == "lower") {
    if (arguments.size() != 1) {
      throw std::runtime_error("LOWER requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    for (char& c : s) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return Value(std::move(s));
  }
  if (name == "trim") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("TRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : " \t\n\r\f\v";
    const size_t start = s.find_first_not_of(cutset);
    if (start == std::string::npos) {
      return Value(std::string());
    }
    const size_t end = s.find_last_not_of(cutset);
    return Value(s.substr(start, end - start + 1));
  }
  if (name == "ltrim") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("LTRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : " \t\n\r\f\v";
    const size_t start = s.find_first_not_of(cutset);
    if (start == std::string::npos) {
      return Value(std::string());
    }
    return Value(s.substr(start));
  }
  if (name == "rtrim") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("RTRIM requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    const std::string cutset =
        arguments.size() == 2 ? raw_str(arguments[1]) : " \t\n\r\f\v";
    const size_t end = s.find_last_not_of(cutset);
    if (end == std::string::npos) {
      return Value(std::string());
    }
    return Value(s.substr(0, end + 1));
  }
  if (name == "starts_with") {
    if (arguments.size() != 2) {
      throw std::runtime_error("STARTS_WITH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    return Value(raw_str(arguments[0]).starts_with(raw_str(arguments[1])));
  }
  if (name == "ends_with") {
    if (arguments.size() != 2) {
      throw std::runtime_error("ENDS_WITH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    return Value(raw_str(arguments[0]).ends_with(raw_str(arguments[1])));
  }
  if (name == "strpos") {
    if (arguments.size() != 2) {
      throw std::runtime_error("STRPOS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string sub = raw_str(arguments[1]);
    if (sub.empty()) {
      return Value(int64_t{1});
    }
    const size_t pos = s.find(sub);
    if (pos == std::string::npos) {
      return Value(int64_t{0});
    }
    return Value(
        static_cast<int64_t>(utf8_len(std::string_view(s.data(), pos)) + 1));
  }
  if (name == "instr") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      throw std::runtime_error("INSTR requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() == 4 && arguments[3].IsNull())) {
      return {};
    }
    const std::string source = raw_str(arguments[0]);
    const std::string target = raw_str(arguments[1]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t position =
        arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;
    const int64_t occurrence =
        arguments.size() == 4 ? to_i64(arguments[3], 1) : 1;

    if (position == 0 || occurrence <= 0) {
      return Value(int64_t{0});
    }

    const auto s_offsets = utf8_offsets(source);
    const size_t s_cps = s_offsets.size() > 1 ? s_offsets.size() - 1 : 0;
    const auto t_offsets = utf8_offsets(target);
    const size_t t_cps = t_offsets.size() > 1 ? t_offsets.size() - 1 : 0;

    if (t_cps == 0) {
      if (position > 0 && static_cast<size_t>(position) <= s_cps + 1) {
        return Value(position);
      }
      return Value(int64_t{0});
    }

    std::vector<size_t> matches;
    if (position > 0) {
      size_t start_cp = static_cast<size_t>(position - 1);
      for (size_t cp = start_cp; cp + t_cps <= s_cps; ++cp) {
        size_t b_start = s_offsets[cp];
        size_t b_end = s_offsets[cp + t_cps];
        if (source.substr(b_start, b_end - b_start) == target) {
          matches.push_back(cp + 1);
        }
      }
    } else {
      int64_t target_cp = static_cast<int64_t>(s_cps) + position;
      if (target_cp >= 0) {
        for (int64_t cp = target_cp; cp >= 0; --cp) {
          if (static_cast<size_t>(cp) + t_cps <= s_cps) {
            size_t b_start = s_offsets[cp];
            size_t b_end = s_offsets[cp + t_cps];
            if (source.substr(b_start, b_end - b_start) == target) {
              matches.push_back(cp + 1);
            }
          }
        }
      }
    }

    if (static_cast<size_t>(occurrence) <= matches.size()) {
      return Value(static_cast<int64_t>(matches[occurrence - 1]));
    }
    return Value(int64_t{0});
  }
  if (name == "replace") {
    if (arguments.size() != 3) {
      throw std::runtime_error("REPLACE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    const std::string from = raw_str(arguments[1]);
    const std::string to = raw_str(arguments[2]);
    if (from.empty()) {
      return Value(std::move(s));
    }
    size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
      s.replace(pos, from.size(), to);
      pos += to.size();
    }
    return Value(std::move(s));
  }
  if (name == "repeat") {
    if (arguments.size() != 2) {
      throw std::runtime_error("REPEAT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t n = to_i64(arguments[1], 0);
    if (n < 0) {
      throw std::runtime_error(
          "Second argument (repeat count) for REPEAT cannot be negative");
    }
    if (n == 0) {
      return Value(std::string());
    }
    if (static_cast<uint64_t>(s.size()) * static_cast<uint64_t>(n) > 1000000) {
      throw std::runtime_error(
          "Output of REPEAT exceeds max allowed output size of 1MB");
    }
    std::string res;
    res.reserve(s.size() * n);
    for (int64_t i = 0; i < n; ++i) {
      res += s;
    }
    return Value(std::move(res));
  }

  if (name == "reverse") {
    if (arguments.size() != 1) {
      throw std::runtime_error("REVERSE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    std::string res;
    res.reserve(s.size());
    for (size_t i = total_cps; i > 0; --i) {
      const size_t start = offsets[i - 1];
      const size_t len = offsets[i] - start;
      res.append(s, start, len);
    }
    return Value(std::move(res));
  }
  if (name == "substr" || name == "substring") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error("SUBSTR requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return {};
    }
    const std::string input = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t start = to_i64(arguments[1], 1);
    if (arguments.size() == 3) {
      const int64_t len = to_i64(arguments[2], 0);
      if (len < 0) {
        throw std::runtime_error("SUBSTR length cannot be negative");
      }
      if (len == 0) {
        return Value(std::string());
      }
    }

    const size_t total_cps = utf8_len(input);
    int64_t actual_start = 0;
    if (start > 0) {
      actual_start = start - 1;
    } else if (start < 0) {
      actual_start = static_cast<int64_t>(total_cps) + start;
    } else {
      actual_start = 0;
    }
    if (actual_start < 0) {
      actual_start = 0;
    }
    if (actual_start >= static_cast<int64_t>(total_cps)) {
      return Value(std::string());
    }

    const size_t length =
        arguments.size() == 3
            ? static_cast<size_t>(std::max(int64_t{0}, to_i64(arguments[2], 0)))
            : total_cps;
    return Value(utf8_substr(input, static_cast<size_t>(actual_start), length));
  }

  if (name == "byte_substr") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error("SUBSTR requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return {};
    }
    const std::string input = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t start = to_i64(arguments[1], 1);
    if (arguments.size() == 3) {
      const int64_t len = to_i64(arguments[2], 0);
      if (len < 0) {
        throw std::runtime_error("SUBSTR length cannot be negative");
      }
      if (len == 0) {
        return Value(std::string());
      }
    }

    const size_t total_bytes = input.size();
    int64_t actual_start = 0;
    if (start > 0) {
      actual_start = start - 1;
    } else if (start < 0) {
      actual_start = static_cast<int64_t>(total_bytes) + start;
    } else {
      actual_start = 0;
    }
    if (actual_start < 0) {
      actual_start = 0;
    }
    if (actual_start >= static_cast<int64_t>(total_bytes)) {
      return Value(std::string());
    }

    const size_t length =
        arguments.size() == 3
            ? static_cast<size_t>(std::max(int64_t{0}, to_i64(arguments[2], 0)))
            : total_bytes;
    return Value(input.substr(static_cast<size_t>(actual_start), length));
  }

  if (name == "byte_reverse") {
    if (arguments.size() != 1) {
      throw std::runtime_error("REVERSE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    std::reverse(s.begin(), s.end());
    return Value(std::move(s));
  }

  if (name == "left") {
    if (arguments.size() != 2) {
      throw std::runtime_error("LEFT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t len = to_i64(arguments[1], 0);
    if (len < 0) {
      throw std::runtime_error(
          "Second argument (length) for LEFT cannot be negative");
    }
    if (len == 0) {
      return Value(std::string());
    }
    return Value(utf8_substr(s, 0, static_cast<size_t>(len)));
  }

  if (name == "right") {
    if (arguments.size() != 2) {
      throw std::runtime_error("RIGHT requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t len = to_i64(arguments[1], 0);
    if (len < 0) {
      throw std::runtime_error(
          "Second argument (length) for RIGHT cannot be negative");
    }
    if (len == 0) {
      return Value(std::string());
    }
    const size_t total_cps = utf8_len(s);
    const size_t start_cp = total_cps >= static_cast<size_t>(len)
                                ? total_cps - static_cast<size_t>(len)
                                : 0;
    return Value(utf8_substr(s, start_cp, static_cast<size_t>(len)));
  }

  if (name == "lpad" || name == "rpad") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error(name + " requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t target_len = to_i64(arguments[1], 0);
    if (target_len < 0) {
      throw std::runtime_error(
          "Second argument (output size) for LPAD/RPAD cannot be negative");
    }
    if (target_len > 1000000) {
      throw std::runtime_error(
          "Output of LPAD/RPAD exceeds max allowed output size of 1MB");
    }
    if (target_len == 0) {
      return Value(std::string());
    }
    const std::string pad = arguments.size() == 3 ? raw_str(arguments[2]) : " ";
    if (pad.empty()) {
      throw std::runtime_error("Pattern in LPAD/RPAD cannot be empty");
    }
    const size_t total_cps = utf8_len(s);
    if (static_cast<size_t>(target_len) <= total_cps) {
      return Value(utf8_substr(s, 0, static_cast<size_t>(target_len)));
    }
    const size_t pad_cps = utf8_len(pad);
    const size_t needed = static_cast<size_t>(target_len) - total_cps;
    std::string pad_str;
    for (size_t i = 0; i < needed / pad_cps; ++i) {
      pad_str += pad;
    }
    if (needed % pad_cps != 0) {
      pad_str += utf8_substr(pad, 0, needed % pad_cps);
    }
    return Value(name == "lpad" ? pad_str + s : s + pad_str);
  }

  if (name == "ascii") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ASCII requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    if (s.empty()) {
      return Value(int64_t{0});
    }
    return Value(static_cast<int64_t>(static_cast<unsigned char>(s[0])));
  }

  if (name == "unicode") {
    if (arguments.size() != 1) {
      throw std::runtime_error("UNICODE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    if (s.empty()) {
      return Value(int64_t{0});
    }
    const auto offsets = utf8_offsets(s);
    const size_t first_len = offsets[1];
    uint32_t code = 0;
    if (first_len == 1) {
      code = static_cast<unsigned char>(s[0]);
    } else if (first_len == 2) {
      code = ((static_cast<unsigned char>(s[0]) & 0x1F) << 6) |
             (static_cast<unsigned char>(s[1]) & 0x3F);
    } else if (first_len == 3) {
      code = ((static_cast<unsigned char>(s[0]) & 0x0F) << 12) |
             ((static_cast<unsigned char>(s[1]) & 0x3F) << 6) |
             (static_cast<unsigned char>(s[2]) & 0x3F);
    } else if (first_len == 4) {
      code = ((static_cast<unsigned char>(s[0]) & 0x07) << 18) |
             ((static_cast<unsigned char>(s[1]) & 0x3F) << 12) |
             ((static_cast<unsigned char>(s[2]) & 0x3F) << 6) |
             (static_cast<unsigned char>(s[3]) & 0x3F);
    }
    return Value(static_cast<int64_t>(code));
  }

  if (name == "chr") {
    if (arguments.size() != 1) {
      throw std::runtime_error("CHR requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const int64_t code = arguments[0].type == ValueType::kInt64
                             ? arguments[0].value.int_value
                             : 0;
    std::string res;
    if (code < 0 || code > 0x10FFFF) {
      throw std::runtime_error("CHR argument out of range");
    }
    if (code <= 0x7F) {
      res.push_back(static_cast<char>(code));
    } else if (code <= 0x7FF) {
      res.push_back(static_cast<char>(0xC0 | ((code >> 6) & 0x1F)));
      res.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    } else if (code <= 0xFFFF) {
      res.push_back(static_cast<char>(0xE0 | ((code >> 12) & 0x0F)));
      res.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
      res.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    } else {
      res.push_back(static_cast<char>(0xF0 | ((code >> 18) & 0x07)));
      res.push_back(static_cast<char>(0x80 | ((code >> 12) & 0x3F)));
      res.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
      res.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    }
    return Value(std::move(res));
  }

  if (name == "code_points_to_string") {
    if (arguments.size() != 1) {
      throw std::runtime_error("CODE_POINTS_TO_STRING requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const Value& arr = arguments[0];
    if (arr.type != ValueType::kArray) {
      return {};
    }
    std::string res;
    for (const Value& elem : arr.ArrayElements()) {
      if (elem.IsNull()) {
        return {};
      }
      int64_t cp = elem.type == ValueType::kInt64 ? elem.value.int_value : 0;
      if (cp < 0 || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
        throw std::runtime_error(
            "invalid code point for CODE_POINTS_TO_STRING: " +
            std::to_string(cp));
      }
      if (cp <= 0x7F) {
        res.push_back(static_cast<char>(cp));
      } else if (cp <= 0x7FF) {
        res.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
        res.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      } else if (cp <= 0xFFFF) {
        res.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
        res.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        res.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      } else {
        res.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
        res.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        res.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        res.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
      }
    }
    return Value(std::move(res));
  }

  if (name == "code_points_to_bytes") {
    if (arguments.size() != 1) {
      throw std::runtime_error("CODE_POINTS_TO_BYTES requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const Value& arr = arguments[0];
    if (arr.type != ValueType::kArray) {
      return {};
    }
    std::string res;
    for (const Value& elem : arr.ArrayElements()) {
      if (elem.IsNull()) {
        return {};
      }
      int64_t b = elem.type == ValueType::kInt64 ? elem.value.int_value : 0;
      if (b < 0 || b > 255) {
        throw std::runtime_error("invalid byte for CODE_POINTS_TO_BYTES: " +
                                 std::to_string(b));
      }
      res.push_back(static_cast<char>(b));
    }
    return Value(std::move(res));
  }

  if (name == "initcap") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("INITCAP requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    const bool has_delim = arguments.size() == 2;
    const std::string delims = has_delim ? raw_str(arguments[1]) : "";
    auto is_delim = [&](char c) {
      if (has_delim) {
        return delims.find(c) != std::string::npos;
      }
      return !std::isalnum(static_cast<unsigned char>(c));
    };
    bool new_word = true;
    for (char& c : s) {
      if (!is_delim(c)) {
        if (new_word) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
          new_word = false;
        } else {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
      } else {
        new_word = true;
      }
    }
    return Value(std::move(s));
  }

  if (name == "regexp_contains") {
    if (arguments.size() != 2) {
      throw std::runtime_error("REGEXP_CONTAINS requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      const std::regex re(pat);
      return Value(std::regex_search(s, re));
    } catch (...) {
      throw std::runtime_error("invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_match") {
    if (arguments.size() != 2) {
      throw std::runtime_error("REGEXP_MATCH requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      const std::regex re(pat);
      return Value(std::regex_match(s, re));
    } catch (...) {
      throw std::runtime_error("invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_instr") {
    if (arguments.size() < 2 || arguments.size() > 5) {
      throw std::runtime_error("REGEXP_INSTR requires 2 to 5 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() >= 4 && arguments[3].IsNull()) ||
        (arguments.size() == 5 && arguments[4].IsNull())) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    if (pat.empty()) {
      return Value(int64_t{0});
    }
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t pos_arg = arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;
    const int64_t occ_arg = arguments.size() >= 4 ? to_i64(arguments[3], 1) : 1;
    const int64_t return_pos =
        arguments.size() == 5 ? to_i64(arguments[4], 0) : 0;
    if (pos_arg <= 0 || occ_arg <= 0) {
      throw std::runtime_error(
          "REGEXP_INSTR position and occurrence must be positive");
    }
    const auto offsets = utf8_offsets(s);
    const size_t total_cps = offsets.size() - 1;
    if (static_cast<size_t>(pos_arg) > total_cps + 1) {
      return Value(int64_t{0});
    }
    const size_t byte_start = offsets[static_cast<size_t>(pos_arg - 1)];
    try {
      const std::regex re(pat);
      std::string search_str = s.substr(byte_start);
      std::sregex_iterator it(search_str.begin(), search_str.end(), re);
      std::sregex_iterator end;
      int64_t current_occ = 1;
      while (it != end) {
        if (current_occ == occ_arg) {
          const auto& match = *it;
          const bool has_capture = match.size() > 1 && match[1].matched;
          const size_t target_start =
              has_capture && return_pos == 0
                  ? byte_start + static_cast<size_t>(match.position(1))
                  : byte_start + static_cast<size_t>(match.position());
          const size_t target_end = byte_start +
                                    static_cast<size_t>(match.position()) +
                                    static_cast<size_t>(match.length());
          size_t match_cp_start = 1;
          size_t match_cp_end = 1;
          for (size_t i = 0; i + 1 < offsets.size(); ++i) {
            if (offsets[i] <= target_start && target_start < offsets[i + 1]) {
              match_cp_start = i + 1;
            }
            if (offsets[i] < target_end && target_end <= offsets[i + 1]) {
              match_cp_end = i + 1;
            }
          }
          if (target_end >= s.size()) {
            match_cp_end = total_cps;
          }
          return Value(static_cast<int64_t>(return_pos == 1 ? match_cp_end
                                                            : match_cp_start));
        }
        ++current_occ;
        ++it;
      }
      return Value(int64_t{0});
    } catch (...) {
      throw std::runtime_error("invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_extract_all") {
    if (arguments.size() != 2) {
      throw std::runtime_error("REGEXP_EXTRACT_ALL requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      const std::regex re(pat);
      std::vector<Value> results;
      std::sregex_iterator it(s.begin(), s.end(), re);
      std::sregex_iterator end;
      while (it != end) {
        if (it->size() > 1) {
          results.push_back(Value(it->str(1)));
        } else {
          results.push_back(Value(it->str(0)));
        }
        ++it;
      }
      return Value::Array(std::move(results), "STRING");
    } catch (...) {
      throw std::runtime_error("invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_extract") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      throw std::runtime_error("REGEXP_EXTRACT requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    try {
      const std::regex re(pat);
      std::smatch match;
      if (std::regex_search(s, match, re)) {
        if (match.size() > 1) {
          return Value(match[1].str());
        }
        return Value(match[0].str());
      }
      return {};
    } catch (...) {
      throw std::runtime_error("invalid regular expression: " + pat);
    }
  }

  if (name == "regexp_replace") {
    if (arguments.size() != 3) {
      throw std::runtime_error("REGEXP_REPLACE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string pat = raw_str(arguments[1]);
    const std::string rep = raw_str(arguments[2]);
    try {
      const std::regex re(pat);
      return Value(std::regex_replace(s, re, rep));
    } catch (...) {
      throw std::runtime_error("invalid regular expression: " + pat);
    }
  }

  if (name == "split_substr") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      throw std::runtime_error("SPLIT_SUBSTR requires 2 to 4 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull()) ||
        (arguments.size() == 4 && arguments[3].IsNull())) {
      return {};
    }
    const std::string str = raw_str(arguments[0]);
    const std::string delim = raw_str(arguments[1]);
    auto to_i64 = [](const Value& v, int64_t def) -> int64_t {
      if (v.type == ValueType::kInt64) {
        return v.value.int_value;
      }
      if (v.type == ValueType::kDouble) {
        return static_cast<int64_t>(v.value.double_value);
      }
      if (v.type == ValueType::kVarChar) {
        try {
          return std::stoll(std::string(v.value.varchar_value));
        } catch (...) {
        }
      }
      return def;
    };
    const int64_t occ = arguments.size() >= 3 ? to_i64(arguments[2], 1) : 1;

    std::vector<std::string> parts;
    if (delim.empty()) {
      parts.push_back(str);
    } else {
      size_t start = 0;
      while (true) {
        size_t pos = str.find(delim, start);
        if (pos == std::string::npos) {
          parts.push_back(str.substr(start));
          break;
        }
        parts.push_back(str.substr(start, pos - start));
        start = pos + delim.size();
      }
    }

    const int64_t len =
        arguments.size() == 4
            ? to_i64(arguments[3], 1)
            : ((occ == 0 || occ == 1) ? static_cast<int64_t>(parts.size()) : 1);
    if (len <= 0) {
      return Value(std::string());
    }

    int64_t start_idx = 0;
    if (occ > 0) {
      start_idx = occ - 1;
      if (start_idx >= static_cast<int64_t>(parts.size())) {
        return Value(std::string());
      }
    } else if (occ < 0) {
      start_idx = static_cast<int64_t>(parts.size()) + occ;
      if (start_idx < 0) {
        start_idx = 0;
      }
    } else {
      start_idx = 0;
    }

    int64_t end_idx =
        std::min(start_idx + len, static_cast<int64_t>(parts.size()));
    std::string res = parts[start_idx];
    for (int64_t i = start_idx + 1; i < end_idx; ++i) {
      res += delim + parts[i];
    }
    return Value(std::move(res));
  }

  if (name == "split") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("SPLIT requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string delim =
        arguments.size() == 2 ? raw_str(arguments[1]) : ",";
    std::vector<Value> elems;
    if (delim.empty()) {
      if (s.empty()) {
        elems.push_back(Value(std::string()));
        return Value::Array(std::move(elems), "STRING");
      }
      const auto offsets = utf8_offsets(s);
      for (size_t i = 1; i < offsets.size(); ++i) {
        elems.push_back(
            Value(s.substr(offsets[i - 1], offsets[i] - offsets[i - 1])));
      }
      return Value::Array(std::move(elems), "STRING");
    }
    if (s.empty()) {
      elems.push_back(Value(std::string()));
      return Value::Array(std::move(elems), "STRING");
    }
    size_t start = 0;
    while (true) {
      size_t pos = s.find(delim, start);
      if (pos == std::string::npos) {
        elems.push_back(Value(s.substr(start)));
        break;
      }
      elems.push_back(Value(s.substr(start, pos - start)));
      start = pos + delim.size();
    }
    return Value::Array(std::move(elems), "STRING");
  }

  if (name == "soundex") {
    if (arguments.size() != 1) {
      throw std::runtime_error("SOUNDEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string s = raw_str(arguments[0]);
    size_t first_char = 0;
    while (first_char < s.size() &&
           std::isspace(static_cast<unsigned char>(s[first_char]))) {
      ++first_char;
    }
    if (first_char >= s.size() ||
        !std::isalpha(static_cast<unsigned char>(s[first_char]))) {
      return Value(std::string());
    }
    std::string res;
    char first_letter = static_cast<char>(
        std::toupper(static_cast<unsigned char>(s[first_char])));
    res.push_back(first_letter);

    auto soundex_code = [](char c) -> char {
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      if (c == 'B' || c == 'F' || c == 'P' || c == 'V') return '1';
      if (c == 'C' || c == 'G' || c == 'J' || c == 'K' || c == 'Q' ||
          c == 'S' || c == 'X' || c == 'Z')
        return '2';
      if (c == 'D' || c == 'T') return '3';
      if (c == 'L') return '4';
      if (c == 'M' || c == 'N') return '5';
      if (c == 'R') return '6';
      if (c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U' || c == 'Y')
        return 'V';
      if (c == 'H' || c == 'W') return 'H';
      return '0';
    };

    char prev_code = soundex_code(first_letter);
    if (prev_code == 'V' || prev_code == 'H') {
      prev_code = '0';
    }
    for (size_t i = first_char + 1; i < s.size() && res.size() < 4; ++i) {
      if (!std::isalpha(static_cast<unsigned char>(s[i]))) {
        continue;
      }
      char code = soundex_code(s[i]);
      if (code == 'H') {
        continue;
      }
      if (code == 'V' || code == '0') {
        prev_code = '0';
        continue;
      }
      if (code != prev_code) {
        res.push_back(code);
        prev_code = code;
      }
    }
    while (res.size() < 4) {
      res.push_back('0');
    }
    return Value(std::move(res));
  }

  if (name == "translate") {
    if (arguments.size() != 3) {
      throw std::runtime_error("TRANSLATE requires 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        arguments[2].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    const std::string src = raw_str(arguments[1]);
    const std::string dst = raw_str(arguments[2]);
    const auto s_offsets = utf8_offsets(s);
    const auto src_offsets = utf8_offsets(src);
    const auto dst_offsets = utf8_offsets(dst);
    const size_t s_len = s_offsets.size() - 1;
    const size_t src_len = src_offsets.size() - 1;
    const size_t dst_len = dst_offsets.size() - 1;

    std::vector<std::string> src_chars;
    for (size_t i = 0; i < src_len; ++i) {
      src_chars.push_back(
          src.substr(src_offsets[i], src_offsets[i + 1] - src_offsets[i]));
    }
    std::vector<std::string> dst_chars;
    for (size_t i = 0; i < dst_len; ++i) {
      dst_chars.push_back(
          dst.substr(dst_offsets[i], dst_offsets[i + 1] - dst_offsets[i]));
    }

    std::string res;
    for (size_t i = 0; i < s_len; ++i) {
      std::string ch = s.substr(s_offsets[i], s_offsets[i + 1] - s_offsets[i]);
      bool replaced = false;
      for (size_t j = 0; j < src_chars.size(); ++j) {
        if (ch == src_chars[j]) {
          if (j < dst_chars.size()) {
            res += dst_chars[j];
          }
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        res += ch;
      }
    }
    return Value(std::move(res));
  }

  // JSON functions
  if (name == "json_extract" || name == "json_query" || name == "json_value" ||
      name == "json_extract_scalar" || name == "json_extract_array" ||
      name == "json_query_array" || name == "json_value_array" ||
      name == "json_extract_string_array") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error(name + " requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    const std::string json_str = raw_str(arguments[0]);
    const std::string path =
        arguments.size() == 2 ? raw_str(arguments[1]) : "$";
    struct JVal {
      enum T { kNull, kBool, kNum, kStr, kArr, kObj } type{kNull};
      std::string raw;
      std::string str;
      std::vector<JVal> arr;
      std::vector<std::pair<std::string, JVal>> obj;

      std::string canonical() const {
        if (type == kNull) {
          return "null";
        }
        if (type == kBool) {
          return str;
        }
        if (type == kNum) {
          return raw;
        }
        if (type == kStr) {
          std::string res = "\"";
          for (char c : str) {
            if (c == '"') {
              res += "\\\"";
            } else if (c == '\\') {
              res += "\\\\";
            } else if (c == '\b') {
              res += "\\b";
            } else if (c == '\f') {
              res += "\\f";
            } else if (c == '\n') {
              res += "\\n";
            } else if (c == '\r') {
              res += "\\r";
            } else if (c == '\t') {
              res += "\\t";
            } else if (static_cast<unsigned char>(c) < 0x20) {
              char buf[8];
              snprintf(buf, sizeof(buf), "\\u%04x",
                       static_cast<unsigned char>(c));
              res += buf;
            } else {
              res.push_back(c);
            }
          }
          res += "\"";
          return res;
        }
        if (type == kArr) {
          std::string res = "[";
          for (size_t i = 0; i < arr.size(); ++i) {
            if (i > 0) {
              res += ",";
            }
            res += arr[i].canonical();
          }
          res += "]";
          return res;
        }
        if (type == kObj) {
          std::string res = "{";
          for (size_t i = 0; i < obj.size(); ++i) {
            if (i > 0) {
              res += ",";
            }
            res += "\"" + obj[i].first + "\":" + obj[i].second.canonical();
          }
          res += "}";
          return res;
        }
        return raw;
      }
    };
    auto skip_ws = [](std::string_view s, size_t& pos) {
      while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' ||
                                s[pos] == '\n' || s[pos] == '\r')) {
        ++pos;
      }
    };
    auto parse_str = [](std::string_view s, size_t& pos, std::string& out) {
      if (pos >= s.size() || s[pos] != '"') {
        return false;
      }
      ++pos;
      out.clear();
      while (pos < s.size()) {
        if (s[pos] == '\\' && pos + 1 < s.size()) {
          char next = s[++pos];
          if (next == '"') {
            out.push_back('"');
            ++pos;
          } else if (next == '\\') {
            out.push_back('\\');
            ++pos;
          } else if (next == '/') {
            out.push_back('/');
            ++pos;
          } else if (next == 'b') {
            out.push_back('\b');
            ++pos;
          } else if (next == 'f') {
            out.push_back('\f');
            ++pos;
          } else if (next == 'n') {
            out.push_back('\n');
            ++pos;
          } else if (next == 'r') {
            out.push_back('\r');
            ++pos;
          } else if (next == 't') {
            out.push_back('\t');
            ++pos;
          } else if (next == 'u' && pos + 4 < s.size()) {
            std::string hex_str = std::string(s.substr(pos + 1, 4));
            try {
              uint32_t cp = std::stoul(hex_str, nullptr, 16);
              if (cp <= 0x7F) {
                out.push_back(static_cast<char>(cp));
              } else if (cp <= 0x7FF) {
                out.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
              } else if (cp <= 0xFFFF) {
                out.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
              } else {
                out.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
              }
              pos += 5;
            } catch (...) {
              out.push_back(next);
              ++pos;
            }
          } else {
            out.push_back(next);
            ++pos;
          }
        } else if (s[pos] == '"') {
          ++pos;
          return true;
        } else {
          out.push_back(s[pos++]);
        }
      }
      return false;
    };
    std::function<bool(std::string_view, size_t&, JVal&)> parse_j =
        [&](std::string_view s, size_t& pos, JVal& out) -> bool {
      skip_ws(s, pos);
      if (pos >= s.size()) {
        return false;
      }
      const size_t st = pos;
      if (s[pos] == '"') {
        out.type = JVal::kStr;
        if (!parse_str(s, pos, out.str)) {
          return false;
        }
        out.raw = std::string(s.substr(st, pos - st));
        return true;
      }
      if (s[pos] == '{') {
        out.type = JVal::kObj;
        ++pos;
        skip_ws(s, pos);
        if (pos < s.size() && s[pos] == '}') {
          ++pos;
          out.raw = std::string(s.substr(st, pos - st));
          return true;
        }
        while (pos < s.size()) {
          skip_ws(s, pos);
          std::string k;
          if (!parse_str(s, pos, k)) {
            return false;
          }
          skip_ws(s, pos);
          if (pos >= s.size() || s[pos] != ':') {
            return false;
          }
          ++pos;
          JVal v;
          if (!parse_j(s, pos, v)) {
            return false;
          }
          out.obj.emplace_back(std::move(k), std::move(v));
          skip_ws(s, pos);
          if (pos < s.size() && s[pos] == ',') {
            ++pos;
          } else if (pos < s.size() && s[pos] == '}') {
            ++pos;
            out.raw = std::string(s.substr(st, pos - st));
            return true;
          } else {
            return false;
          }
        }
        return false;
      }
      if (s[pos] == '[') {
        out.type = JVal::kArr;
        ++pos;
        skip_ws(s, pos);
        if (pos < s.size() && s[pos] == ']') {
          ++pos;
          out.raw = std::string(s.substr(st, pos - st));
          return true;
        }
        while (pos < s.size()) {
          JVal elem;
          if (!parse_j(s, pos, elem)) {
            return false;
          }
          out.arr.push_back(std::move(elem));
          skip_ws(s, pos);
          if (pos < s.size() && s[pos] == ',') {
            ++pos;
          } else if (pos < s.size() && s[pos] == ']') {
            ++pos;
            out.raw = std::string(s.substr(st, pos - st));
            return true;
          } else {
            return false;
          }
        }
        return false;
      }
      if (s.substr(pos, 4) == "true") {
        out.type = JVal::kBool;
        out.str = "true";
        out.raw = "true";
        pos += 4;
        return true;
      }
      if (s.substr(pos, 5) == "false") {
        out.type = JVal::kBool;
        out.str = "false";
        out.raw = "false";
        pos += 5;
        return true;
      }
      if (s.substr(pos, 4) == "null") {
        out.type = JVal::kNull;
        out.raw = "null";
        pos += 4;
        return true;
      }
      while (pos < s.size() &&
             (std::isdigit(static_cast<unsigned char>(s[pos])) ||
              s[pos] == '-' || s[pos] == '+' || s[pos] == '.' ||
              s[pos] == 'e' || s[pos] == 'E')) {
        ++pos;
      }
      out.type = JVal::kNum;
      out.raw = std::string(s.substr(st, pos - st));
      out.str = out.raw;
      return true;
    };

    size_t p = 0;
    JVal root;
    if (!parse_j(json_str, p, root)) {
      return {};
    }
    const JVal* cur = &root;
    size_t path_pos = 0;
    if (path_pos < path.size() && path[path_pos] == '$') {
      ++path_pos;
    }
    while (path_pos < path.size() && cur != nullptr) {
      if (path[path_pos] == '.') {
        ++path_pos;
        std::string key;
        if (path_pos < path.size() && path[path_pos] == '"') {
          if (!parse_str(path, path_pos, key)) {
            cur = nullptr;
            break;
          }
        } else {
          size_t end = path_pos;
          while (end < path.size() && path[end] != '.' && path[end] != '[') {
            ++end;
          }
          key = std::string(path.substr(path_pos, end - path_pos));
          path_pos = end;
        }
        if (cur->type != JVal::kObj) {
          cur = nullptr;
          break;
        }
        const JVal* next = nullptr;
        for (const auto& [k, v] : cur->obj) {
          if (k == key) {
            next = &v;
            break;
          }
        }
        cur = next;
      } else if (path[path_pos] == '[') {
        ++path_pos;
        size_t end = path_pos;
        while (end < path.size() && path[end] != ']') {
          ++end;
        }
        if (end >= path.size()) {
          cur = nullptr;
          break;
        }
        const std::string idx_str =
            std::string(path.substr(path_pos, end - path_pos));
        path_pos = end + 1;
        int64_t idx = 0;
        try {
          idx = std::stoll(idx_str);
        } catch (...) {
          cur = nullptr;
          break;
        }
        if (cur->type != JVal::kArr || idx < 0 ||
            idx >= static_cast<int64_t>(cur->arr.size())) {
          cur = nullptr;
          break;
        }
        cur = &cur->arr[idx];
      } else {
        cur = nullptr;
        break;
      }
    }
    if (cur == nullptr) {
      return {};
    }
    const bool is_array_func = name.ends_with("_array");
    if (is_array_func) {
      if (cur->type != JVal::kArr) {
        return {};
      }
      std::vector<Value> elems;
      elems.reserve(cur->arr.size());
      for (const auto& item : cur->arr) {
        if (name == "json_value_array" || name == "json_extract_string_array") {
          if (item.type == JVal::kNull) {
            elems.push_back(Value());
          } else if (item.type == JVal::kStr) {
            elems.push_back(Value(std::string(item.str)));
          } else {
            elems.push_back(Value(item.canonical()));
          }
        } else {
          elems.push_back(Value(item.canonical()));
        }
      }
      return Value::Array(std::move(elems), "STRING");
    }
    if (cur->type == JVal::kNull) {
      return {};
    }
    if (name == "json_value" || name == "json_extract_scalar") {
      if (cur->type == JVal::kArr || cur->type == JVal::kObj) {
        return {};
      }
      return Value(std::string(cur->type == JVal::kStr ? cur->str : cur->raw));
    }
    return Value(cur->canonical());
  }

  if (name == "to_json_string") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("TO_JSON_STRING requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string s = raw_str(arguments[0]);
    return Value(std::string(s));
  }

  // Math functions
  if (name == "abs") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ABS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64) {
      return Value(std::abs(arguments[0].value.int_value));
    }
    if (arguments[0].type == ValueType::kDouble) {
      return Value(std::fabs(arguments[0].value.double_value));
    }
    throw std::runtime_error("ABS requires a numeric argument");
  }
  if (name == "sign") {
    if (arguments.size() != 1) {
      throw std::runtime_error("SIGN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64) {
      const int64_t v = arguments[0].value.int_value;
      return Value(v > 0 ? int64_t{1} : (v < 0 ? int64_t{-1} : int64_t{0}));
    }
    if (arguments[0].type == ValueType::kDouble) {
      const double v = arguments[0].value.double_value;
      return Value(v > 0.0 ? 1.0 : (v < 0.0 ? -1.0 : 0.0));
    }
    throw std::runtime_error("SIGN requires a numeric argument");
  }
  if (name == "round") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error("ROUND requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    const double val = arguments[0].type == ValueType::kInt64
                           ? static_cast<double>(arguments[0].value.int_value)
                           : arguments[0].value.double_value;
    const int64_t digits = arguments.size() == 2
                               ? (arguments[1].type == ValueType::kInt64
                                      ? arguments[1].value.int_value
                                      : 0)
                               : 0;
    const double factor = std::pow(10.0, digits);
    const double res = std::round(val * factor) / factor;
    if (arguments[0].type == ValueType::kInt64 && digits <= 0) {
      return Value(static_cast<int64_t>(res));
    }
    return Value(res);
  }
  if (name == "trunc" || name == "truncate") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error(name + " requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    const double val = arguments[0].type == ValueType::kInt64
                           ? static_cast<double>(arguments[0].value.int_value)
                           : arguments[0].value.double_value;
    const int64_t digits = arguments.size() == 2
                               ? (arguments[1].type == ValueType::kInt64
                                      ? arguments[1].value.int_value
                                      : 0)
                               : 0;
    const double factor = std::pow(10.0, digits);
    const double res = std::trunc(val * factor) / factor;
    if (arguments[0].type == ValueType::kInt64 && digits <= 0) {
      return Value(static_cast<int64_t>(res));
    }
    return Value(res);
  }
  if (name == "ceil" || name == "ceiling") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64) {
      return arguments[0];
    }
    return Value(std::ceil(arguments[0].value.double_value));
  }
  if (name == "floor") {
    if (arguments.size() != 1) {
      throw std::runtime_error("FLOOR requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64) {
      return arguments[0];
    }
    return Value(std::floor(arguments[0].value.double_value));
  }
  if (name == "mod") {
    if (arguments.size() != 2) {
      throw std::runtime_error("MOD requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64 &&
        arguments[1].type == ValueType::kInt64) {
      if (arguments[1].value.int_value == 0) {
        throw std::runtime_error("division by zero in MOD");
      }
      return Value(arguments[0].value.int_value % arguments[1].value.int_value);
    }
    const double l = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    const double r = arguments[1].type == ValueType::kInt64
                         ? arguments[1].value.int_value
                         : arguments[1].value.double_value;
    return Value(std::fmod(l, r));
  }
  if (name == "pow" || name == "power") {
    if (arguments.size() != 2) {
      throw std::runtime_error(name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const double l = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    const double r = arguments[1].type == ValueType::kInt64
                         ? arguments[1].value.int_value
                         : arguments[1].value.double_value;
    return Value(std::pow(l, r));
  }
  if (name == "sqrt") {
    if (arguments.size() != 1) {
      throw std::runtime_error("SQRT requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    if (v < 0.0) {
      throw std::runtime_error("SQRT of negative number");
    }
    return Value(std::sqrt(v));
  }
  if (name == "cbrt") {
    if (arguments.size() != 1) {
      throw std::runtime_error("CBRT requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::cbrt(v));
  }
  if (name == "greatest" || name == "least") {
    if (arguments.empty()) {
      throw std::runtime_error(name + " requires at least 1 argument");
    }
    for (const auto& v : arguments) {
      if (v.IsNull()) {
        return {};
      }
    }
    Value best = arguments[0];
    for (size_t i = 1; i < arguments.size(); ++i) {
      if (name == "greatest") {
        if (arguments[i] > best) {
          best = arguments[i];
        }
      } else {
        if (arguments[i] < best) {
          best = arguments[i];
        }
      }
    }
    return best;
  }
  if (name == "ln") {
    if (arguments.size() != 1) {
      throw std::runtime_error("LN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::log(v));
  }
  if (name == "log" || name == "log10") {
    if (arguments.empty() || arguments.size() > 2) {
      throw std::runtime_error(name + " argument count mismatch");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    if (name == "log10") {
      return Value(std::log10(v));
    }
    if (arguments.size() == 1) {
      return Value(std::log(v));
    }
    const double base = arguments[1].type == ValueType::kInt64
                            ? arguments[1].value.int_value
                            : arguments[1].value.double_value;
    return Value(std::log(v) / std::log(base));
  }
  if (name == "exp") {
    if (arguments.size() != 1) {
      throw std::runtime_error("EXP requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::exp(v));
  }
  if (name == "cos") {
    if (arguments.size() != 1) {
      throw std::runtime_error("COS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::cos(v));
  }
  if (name == "sin") {
    if (arguments.size() != 1) {
      throw std::runtime_error("SIN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::sin(v));
  }
  if (name == "tan") {
    if (arguments.size() != 1) {
      throw std::runtime_error("TAN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::tan(v));
  }
  if (name == "acos") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ACOS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::acos(v));
  }
  if (name == "asin") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ASIN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::asin(v));
  }
  if (name == "atan") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ATAN requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::atan(v));
  }
  if (name == "atan2") {
    if (arguments.size() != 2) {
      throw std::runtime_error("ATAN2 requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const double y = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    const double x = arguments[1].type == ValueType::kInt64
                         ? arguments[1].value.int_value
                         : arguments[1].value.double_value;
    return Value(std::atan2(y, x));
  }
  if (name == "pi") {
    if (!arguments.empty()) {
      throw std::runtime_error("PI takes no arguments");
    }
    return Value(M_PI);
  }
  if (name == "radians") {
    if (arguments.size() != 1) {
      throw std::runtime_error("RADIANS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    auto to_double_val = [](const Value& val) -> double {
      if (val.type == ValueType::kInt64) {
        return static_cast<double>(val.value.int_value);
      }
      if (val.type == ValueType::kDouble) {
        return val.value.double_value;
      }
      if (val.type == ValueType::kVarChar) {
        return std::stod(std::string(val.value.varchar_value));
      }
      throw std::runtime_error("cannot convert value to double");
    };
    return Value(to_double_val(arguments[0]) * (M_PI / 180.0));
  }
  if (name == "degrees") {
    if (arguments.size() != 1) {
      throw std::runtime_error("DEGREES requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    auto to_double_val = [](const Value& val) -> double {
      if (val.type == ValueType::kInt64) {
        return static_cast<double>(val.value.int_value);
      }
      if (val.type == ValueType::kDouble) {
        return val.value.double_value;
      }
      if (val.type == ValueType::kVarChar) {
        return std::stod(std::string(val.value.varchar_value));
      }
      throw std::runtime_error("cannot convert value to double");
    };
    return Value(to_double_val(arguments[0]) * (180.0 / M_PI));
  }

  if (name == "cosh") {
    if (arguments.size() != 1) {
      throw std::runtime_error("COSH requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::cosh(v));
  }
  if (name == "sinh") {
    if (arguments.size() != 1) {
      throw std::runtime_error("SINH requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::sinh(v));
  }
  if (name == "tanh") {
    if (arguments.size() != 1) {
      throw std::runtime_error("TANH requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const double v = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    return Value(std::tanh(v));
  }
  if (name == "div") {
    if (arguments.size() != 2) {
      throw std::runtime_error("DIV requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64 &&
        arguments[1].type == ValueType::kInt64) {
      if (arguments[1].value.int_value == 0) {
        throw std::runtime_error("division by zero in DIV");
      }
      if (arguments[0].value.int_value == std::numeric_limits<int64_t>::min() &&
          arguments[1].value.int_value == -1) {
        throw std::runtime_error("integer overflow in DIV");
      }
      return Value(arguments[0].value.int_value / arguments[1].value.int_value);
    }
    const double l = arguments[0].type == ValueType::kInt64
                         ? static_cast<double>(arguments[0].value.int_value)
                         : arguments[0].value.double_value;
    const double r = arguments[1].type == ValueType::kInt64
                         ? static_cast<double>(arguments[1].value.int_value)
                         : arguments[1].value.double_value;
    if (r == 0.0) {
      throw std::runtime_error("division by zero in DIV");
    }
    return Value(static_cast<int64_t>(std::trunc(l / r)));
  }
  if (name == "ieee_divide" || name == "safe_divide") {
    if (arguments.size() != 2) {
      throw std::runtime_error(name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const double l = arguments[0].type == ValueType::kInt64
                         ? static_cast<double>(arguments[0].value.int_value)
                         : arguments[0].value.double_value;
    const double r = arguments[1].type == ValueType::kInt64
                         ? static_cast<double>(arguments[1].value.int_value)
                         : arguments[1].value.double_value;
    if (r == 0.0) {
      return name == "safe_divide" ? Value() : Value(l / r);
    }
    const double res = l / r;
    if (name == "safe_divide" && (std::isinf(res) || std::isnan(res))) {
      return Value();
    }
    return Value(res);
  }

  if (name == "safe_add" || name == "safe_subtract" ||
      name == "safe_multiply") {
    if (arguments.size() != 2) {
      throw std::runtime_error(name + " requires 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64 &&
        arguments[1].type == ValueType::kInt64) {
      const int64_t l = arguments[0].value.int_value;
      const int64_t r = arguments[1].value.int_value;
      if (name == "safe_add") {
        int64_t res = 0;
        if (__builtin_add_overflow(l, r, &res)) {
          return {};
        }
        return Value(res);
      }
      if (name == "safe_subtract") {
        int64_t res = 0;
        if (__builtin_sub_overflow(l, r, &res)) {
          return {};
        }
        return Value(res);
      }
      if (name == "safe_multiply") {
        int64_t res = 0;
        if (__builtin_mul_overflow(l, r, &res)) {
          return {};
        }
        return Value(res);
      }
    }
    const double l = arguments[0].type == ValueType::kInt64
                         ? arguments[0].value.int_value
                         : arguments[0].value.double_value;
    const double r = arguments[1].type == ValueType::kInt64
                         ? arguments[1].value.int_value
                         : arguments[1].value.double_value;
    if (name == "safe_add") {
      return Value(l + r);
    }
    if (name == "safe_subtract") {
      return Value(l - r);
    }
    return Value(l * r);
  }
  if (name == "safe_negate") {
    if (arguments.size() != 1) {
      throw std::runtime_error("SAFE_NEGATE requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    if (arguments[0].type == ValueType::kInt64) {
      if (arguments[0].value.int_value == std::numeric_limits<int64_t>::min()) {
        return {};
      }
      return Value(-arguments[0].value.int_value);
    }
    return Value(-arguments[0].value.double_value);
  }
  if (name == "format") {
    if (arguments.empty()) {
      throw std::runtime_error("FORMAT requires at least 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string fmt = raw_str(arguments[0]);
    std::string result;
    size_t arg_idx = 1;
    for (size_t i = 0; i < fmt.size(); ++i) {
      if (fmt[i] == '%' && i + 1 < fmt.size()) {
        ++i;
        if (fmt[i] == '%') {
          result.push_back('%');
          continue;
        }
        bool hash_flag = false;
        bool zero_pad = false;
        if (fmt[i] == '#') {
          hash_flag = true;
          ++i;
        }
        if (i < fmt.size() && fmt[i] == '0') {
          zero_pad = true;
          ++i;
        }
        int width = 0;
        while (i < fmt.size() &&
               std::isdigit(static_cast<unsigned char>(fmt[i]))) {
          width = width * 10 + (fmt[i] - '0');
          ++i;
        }
        int precision = -1;
        if (i < fmt.size() && fmt[i] == '.') {
          ++i;
          precision = 0;
          while (i < fmt.size() &&
                 std::isdigit(static_cast<unsigned char>(fmt[i]))) {
            precision = precision * 10 + (fmt[i] - '0');
            ++i;
          }
        }
        if (i >= fmt.size()) {
          break;
        }
        char spec = fmt[i];

        if (arg_idx >= arguments.size()) {
          throw std::runtime_error(
              "FORMAT: not enough arguments for format string");
        }
        const Value& arg = arguments[arg_idx++];
        std::string formatted_item;
        if (spec == 'T' || spec == 't') {
          if (arg.IsNull()) {
            formatted_item = "NULL";
          } else if (arg.type == ValueType::kVarChar) {
            std::string s = raw_str(arg);
            formatted_item += "\"";
            for (char c : s) {
              if (c == '"') {
                formatted_item += "\\\"";
              } else if (c == '\\') {
                formatted_item += "\\\\";
              } else if (c == '\n') {
                formatted_item += "\\n";
              } else if (c == '\r') {
                formatted_item += "\\r";
              } else if (c == '\t') {
                formatted_item += "\\t";
              } else {
                formatted_item.push_back(c);
              }
            }
            formatted_item += "\"";
          } else if (arg.type == ValueType::kInt64) {
            formatted_item = std::to_string(arg.value.int_value);
          } else if (arg.type == ValueType::kDouble) {
            formatted_item = std::to_string(arg.value.double_value);
          } else {
            formatted_item = arg.AsString();
          }
        } else if (spec == 's') {
          if (arg.IsNull()) {
            formatted_item = "NULL";
          } else {
            formatted_item = raw_str(arg);
          }
        } else if (spec == 'd' || spec == 'i' || spec == 'u' || spec == 'x' ||
                   spec == 'X' || spec == 'o') {
          if (arg.IsNull()) {
            formatted_item = "NULL";
          } else if (arg.type == ValueType::kInt64) {
            if (spec == 'x') {
              char buf[32];
              snprintf(buf, sizeof(buf), hash_flag ? "0x%lx" : "%lx",
                       static_cast<unsigned long>(arg.value.int_value));
              formatted_item = buf;
            } else if (spec == 'X') {
              char buf[32];
              snprintf(buf, sizeof(buf), hash_flag ? "0X%lX" : "%lX",
                       static_cast<unsigned long>(arg.value.int_value));
              formatted_item = buf;
            } else if (spec == 'o') {
              char buf[32];
              snprintf(buf, sizeof(buf), hash_flag ? "0%lo" : "%lo",
                       static_cast<unsigned long>(arg.value.int_value));
              formatted_item = buf;
            } else {
              formatted_item = std::to_string(arg.value.int_value);
            }
          } else if (arg.type == ValueType::kDouble) {
            formatted_item =
                std::to_string(static_cast<int64_t>(arg.value.double_value));
          } else {
            throw std::runtime_error(
                "FORMAT: invalid argument type for integer specifier");
          }
        } else if (spec == 'f' || spec == 'g' || spec == 'e' || spec == 'E') {
          if (arg.IsNull()) {
            formatted_item = "NULL";
          } else if (arg.type == ValueType::kDouble) {
            formatted_item = std::to_string(arg.value.double_value);
          } else if (arg.type == ValueType::kInt64) {
            formatted_item =
                std::to_string(static_cast<double>(arg.value.int_value));
          } else {
            throw std::runtime_error(
                "FORMAT: invalid argument type for float specifier");
          }
        } else {
          result.push_back('%');
          result.push_back(spec);
          continue;
        }

        if (precision >= 0 && (spec == 't' || spec == 'T' || spec == 's')) {
          if (formatted_item.size() > static_cast<size_t>(precision)) {
            formatted_item =
                formatted_item.substr(0, static_cast<size_t>(precision));
          }
        }
        if (width > 0 && formatted_item.size() < static_cast<size_t>(width)) {
          const size_t pad_len =
              static_cast<size_t>(width) - formatted_item.size();
          char pad_char = zero_pad ? '0' : ' ';
          formatted_item = std::string(pad_len, pad_char) + formatted_item;
        }
        result += formatted_item;
      } else {
        result.push_back(fmt[i]);
      }
    }
    if (arg_idx < arguments.size()) {
      throw std::runtime_error("FORMAT: too many arguments for format string");
    }
    return Value(std::move(result));
  }

  if (name == "current_timestamp") {
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d %H:%M:%S", &tm) ==
        0) {
      throw std::runtime_error("timestamp formatting failed");
    }
    return Value(std::string(buffer.data()));
  }

  if (name == "make_interval") {
    if (arguments.size() < 2) {
      throw std::runtime_error("make_interval requires at least 2 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    std::string val_str = raw_str(arguments[0]);
    std::string unit_str = raw_str(arguments[1]);
    IntervalValue iv = IntervalValue::Parse(val_str, unit_str);
    return Value(iv.ToString());
  }

  if (name == "justify_hours") {
    if (arguments.empty()) {
      throw std::runtime_error("JUSTIFY_HOURS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    IntervalValue iv = IntervalValue::Parse(raw_str(arguments[0]));
    return Value(iv.JustifyHours().ToString());
  }

  if (name == "justify_days") {
    if (arguments.empty()) {
      throw std::runtime_error("JUSTIFY_DAYS requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    IntervalValue iv = IntervalValue::Parse(raw_str(arguments[0]));
    return Value(iv.JustifyDays().ToString());
  }

  if (name == "justify_interval") {
    if (arguments.empty()) {
      throw std::runtime_error("JUSTIFY_INTERVAL requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    IntervalValue iv = IntervalValue::Parse(raw_str(arguments[0]));
    return Value(iv.JustifyInterval().ToString());
  }

  if (name == "collate") {
    // COLLATE(value, spec): attaches the collator to the value; the value
    // itself is unchanged.  1 = explicit binary collator, 2 =
    // case-insensitive (or unknown non-binary) collator.
    if (arguments.size() != 2) {
      throw std::runtime_error("COLLATE requires 2 arguments");
    }
    const Value& v = arguments[0];
    if (v.IsNull()) {
      return {};
    }
    std::string spec = raw_str(arguments[1]);
    for (char& c : spec) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    const uint8_t code = spec == "binary" ? 1 : 2;
    return v.WithCollation(code);
  }

  if (name == "array_length") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ARRAY_LENGTH requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return {};
    }
    if (!array.IsArray()) {
      throw std::runtime_error("ARRAY_LENGTH requires an array");
    }
    return Value(static_cast<int64_t>(array.ArrayElements().size()));
  }

  // OFFSET(n) / ORDINAL(n): index accessors; both yield their argument with
  // the convention checked by the caller's bounds.
  if (name == "offset" || name == "ordinal") {
    if (arguments.size() != 1 || arguments[0].IsNull()) {
      return {};
    }
    const Value& index = arguments[0];
    if (index.type == ValueType::kInt64) {
      return index;
    }
    return Value(static_cast<int64_t>(index.value.double_value));
  }

  if (name == "is_inf" || name == "is_nan") {
    if (arguments.size() != 1) {
      return {};
    }
    const Value& arg = arguments[0];
    if (arg.IsNull() || arg.type != ValueType::kDouble) {
      return Value(int64_t{0});
    }
    const double v = arg.value.double_value;
    if (name == "is_inf") {
      return Value(std::isinf(v) ? int64_t{1} : int64_t{0});
    }
    return Value(std::isnan(v) ? int64_t{1} : int64_t{0});
  }

  if (name == "hll_count.extract") {
    if (arguments.size() != 1) {
      throw std::runtime_error("HLL_COUNT.EXTRACT requires 1 argument");
    }
    const Value& sketch = arguments[0];
    if (sketch.IsNull()) {
      return Value(int64_t{0});
    }
    int typecode = 0;
    int64_t precision = 0;
    std::vector<std::string> entries;
    if (!DecodeSketch(SketchBytesOf(sketch), &typecode, &precision, &entries)) {
      throw std::runtime_error("Invalid sketch in HLL_COUNT.EXTRACT");
    }
    return Value(static_cast<int64_t>(entries.size()));
  }

  if (name.starts_with("kll_quantiles.extract_")) {
    if (arguments.size() != 2) {
      throw std::runtime_error("KLL_QUANTILES.EXTRACT requires 2 arguments");
    }
    const Value& sketch = arguments[0];
    const Value& number = arguments[1];
    if (number.IsNull() || number.value.int_value < 1) {
      throw std::runtime_error(
          "The second argument to KLL_QUANTILES.EXTRACT must be positive");
    }
    if (sketch.IsNull()) {
      return {};
    }
    return ExtractSketchQuantilesStatic(sketch, number.value.int_value);
  }

  if (name == "array_concat") {
    if (arguments.empty()) {
      throw std::runtime_error("ARRAY_CONCAT requires at least 1 argument");
    }
    std::vector<Value> merged;
    std::string element_type;
    for (const Value& arr : arguments) {
      if (arr.IsNull()) {
        return {};
      }
      if (!arr.IsArray()) {
        throw std::runtime_error("ARRAY_CONCAT requires ARRAY arguments");
      }
      if (element_type.empty()) {
        element_type = arr.ArrayElementSqlType();
      }
      const auto& elements = arr.ArrayElements();
      merged.insert(merged.end(), elements.begin(), elements.end());
    }
    return Value::Array(std::move(merged), element_type);
  }

  if (name == "array_first" || name == "array_last") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return {};
    }
    if (!arr.IsArray()) {
      throw std::runtime_error(name + " requires an array");
    }
    const auto& elements = arr.ArrayElements();
    if (elements.empty()) {
      throw std::out_of_range(name == "array_first"
                                  ? "ARRAY_FIRST cannot get the first element "
                                    "of an empty array"
                                  : "ARRAY_LAST cannot get the last element of "
                                    "an empty array");
    }
    return name == "array_first" ? elements.front() : elements.back();
  }

  if (name == "array_slice") {
    // ARRAY_SLICE(arr, offset[, size]): 1-based ORDINAL-style bounds; a
    // negative offset counts from the end. Out-of-range clamps to [].
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error("ARRAY_SLICE requires 2 or 3 arguments");
    }
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return {};
    }
    if (!arr.IsArray()) {
      throw std::runtime_error("ARRAY_SLICE requires an array");
    }
    if (arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return {};
    }
    const auto& elements = arr.ArrayElements();
    const int64_t n = static_cast<int64_t>(elements.size());
    int64_t offset = arguments[1].value.int_value;
    int64_t size = arguments.size() == 3 ? arguments[2].value.int_value : n;
    if (size <= 0 || n == 0) {
      return Value::Array({}, arr.ArrayElementSqlType());
    }
    int64_t begin = offset >= 0 ? offset - 1 : n + offset;
    int64_t end = begin + size;  // exclusive
    begin = std::max<int64_t>(begin, 0);
    end = std::min<int64_t>(end, n);
    std::vector<Value> picked;
    for (int64_t i = begin; i < end; ++i) {
      picked.push_back(elements[static_cast<size_t>(i)]);
    }
    return Value::Array(std::move(picked), arr.ArrayElementSqlType());
  }

  if (name == "array_is_distinct") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ARRAY_IS_DISTINCT requires 1 argument");
    }
    const Value& arr = arguments[0];
    if (arr.IsNull()) {
      return {};
    }
    if (!arr.IsArray()) {
      throw std::runtime_error("ARRAY_IS_DISTINCT requires an array");
    }
    const auto& elements = arr.ArrayElements();
    auto same = [](const Value& a, const Value& b) {
      if (a.IsNull() && b.IsNull()) { return true; }
      if (a.IsNull() || b.IsNull()) { return false; }
      try {
        return Binary(BinaryOperation::kEquals, a, b).Truthy();
      } catch (...) {
        return false;
      }
    };
    for (size_t i = 0; i < elements.size(); ++i) {
      for (size_t j = i + 1; j < elements.size(); ++j) {
        if (same(elements[i], elements[j])) {
          return Value(int64_t{0});
        }
      }
    }
    return Value(int64_t{1});
  }

  if (name == "generate_array") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error("GENERATE_ARRAY requires 2 or 3 arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    const bool has_double =
        arguments[0].type == ValueType::kDouble ||
        arguments[1].type == ValueType::kDouble ||
        (arguments.size() == 3 && arguments[2].type == ValueType::kDouble);
    double start_d = static_cast<double>(arguments[0].value.int_value);
    double end_d = static_cast<double>(arguments[1].value.int_value);
    if (arguments[0].type == ValueType::kDouble) { start_d = arguments[0].value.double_value; }
    if (arguments[1].type == ValueType::kDouble) { end_d = arguments[1].value.double_value; }
    double step_d = 1.0;
    if (arguments.size() == 3) {
      if (arguments[2].IsNull()) { return {}; }
      step_d = arguments[2].type == ValueType::kDouble
                   ? arguments[2].value.double_value
                   : static_cast<double>(arguments[2].value.int_value);
    }
    if (step_d == 0.0) {
      throw std::out_of_range("GENERATE_ARRAY step must be nonzero");
    }
    if ((step_d > 0 && end_d < start_d) || (step_d < 0 && end_d > start_d)) {
      return has_double ? Value::Array({}, "DOUBLE")
                        : Value::Array({}, "INT64");
    }
    const int64_t count =
        static_cast<int64_t>(std::floor((end_d - start_d) / step_d)) + 1;
    std::vector<Value> elements;
    elements.reserve(static_cast<size_t>(std::max<int64_t>(count, 0)));
    for (int64_t i = 0; i < count; ++i) {
      const double v = start_d + static_cast<double>(i) * step_d;
      elements.push_back(has_double ? Value(v)
                                    : Value(static_cast<int64_t>(v)));
    }
    return has_double ? Value::Array(std::move(elements), "DOUBLE")
                      : Value::Array(std::move(elements), "INT64");
  }

  if (name == "generate_date_array") {
    if (call.Args().size() < 2 || call.Args().size() > 3) {
      throw std::runtime_error("GENERATE_DATE_ARRAY requires 2 or 3 arguments");
    }
    const Value start = arguments.empty() ? Value() : arguments[0];
    const Value end = arguments.size() > 1 ? arguments[1] : Value();
    auto as_date = [](const Value& v) -> Value {
      if (v.type == ValueType::kVarChar) {
        return Value::Date(std::string_view(v.value.varchar_value));
      }
      return v;
    };
    const Value start_date = as_date(start);
    const Value end_date = as_date(end);
    if (start_date.IsNull() || end_date.IsNull() ||
        start_date.type != ValueType::kDate ||
        end_date.type != ValueType::kDate) {
      throw std::runtime_error("DATE value required");
    }
    int64_t step_days = 1;
    if (call.Args().size() == 3) {
      if (call.Args()[2]->Type() == TypeTag::kIntervalExp) {
        const auto& interval = call.Args()[2]->AsIntervalExpression();
        const int64_t amount = interval.Amount();
        std::string unit = to_lower(std::string(interval.Unit()));
        if (unit.starts_with("week")) {
          step_days = amount * 7;
        } else if (unit.starts_with("day")) {
          step_days = amount;
        } else {
          throw std::runtime_error(
              "unsupported GENERATE_DATE_ARRAY step unit: " + unit);
        }
      } else {
        const Value step = arguments[2];
        if (step.IsNull()) {
          return {};
        }
        if (step.type == ValueType::kVarChar) {
          // Dynamic steps arrive as make_interval(...) results encoded
          // "Y-M D H:M:S"; only pure day counts are supported here.
          long long years = 0, months = 0, days = 0, hours = 0, mins = 0,
                    secs = 0;
          if (sscanf(
                  std::string(step.value.varchar_value).c_str(),
                  "%lld-%lld %lld %lld:%lld:%lld", &years, &months, &days,
                  &hours, &mins, &secs) >= 3 &&
              years == 0 && months == 0 && hours == 0 && mins == 0 &&
              secs == 0 && days != 0) {
            step_days = static_cast<int64_t>(days);
          } else {
            throw std::runtime_error(
                "unsupported GENERATE_DATE_ARRAY step expression");
          }
        } else {
          step_days = step.value.int_value;
        }
      }
    }
    if (step_days == 0) {
      throw std::out_of_range("Sequence step cannot be 0.");
    }
    const int64_t start_days = start_date.DateDays();
    const int64_t end_days = end_date.DateDays();
    std::vector<Value> elements;
    for (int64_t d = start_days;
         step_days > 0 ? d <= end_days : d >= end_days; d += step_days) {
      elements.push_back(Value::DateFromDays(d));
    }
    return Value::Array(std::move(elements), "DATE");
  }

  if (name == "array_includes" || name == "array_includes_any" ||
      name == "array_includes_all") {
    if (arguments.size() != 2) {
      throw std::runtime_error(name + " requires 2 arguments");
    }
    const Value& input = arguments[0];
    const Value& target = arguments[1];
    // GoogleSQL: a NULL array or NULL target yields NULL.
    if (input.IsNull() || target.IsNull()) {
      return {};
    }
    if (!input.IsArray() || (name != "array_includes" && !target.IsArray())) {
      throw std::runtime_error(name + " requires ARRAY arguments");
    }
    auto equals = [](const Value& left, const Value& right) {
      try {
        return Binary(BinaryOperation::kEquals, left, right).Truthy();
      } catch (...) {
        return false;
      }
    };
    const auto& haystack = input.ArrayElements();
    bool result;
    if (name == "array_includes") {
      result = false;
      for (const Value& element : haystack) {
        if (!element.IsNull() && equals(element, target)) {
          result = true;
          break;
        }
      }
    } else {
      const auto& targets = target.ArrayElements();
      result = name != "array_includes_any";
      for (const Value& wanted : targets) {
        if (wanted.IsNull()) {
          // NULL target elements match nothing: ANY cannot use them and ALL
          // fails outright.
          if (name == "array_includes_all") {
            result = false;
          }
          continue;
        }
        bool found = false;
        for (const Value& element : haystack) {
          if (!element.IsNull() && equals(element, wanted)) {
            found = true;
            break;
          }
        }
        if (name == "array_includes_any") {
          if (found) {
            result = true;
            break;
          }
        } else if (!found) {
          result = false;
          break;
        }
      }
    }
    return Value(result);
  }

  if (name == "rand") {
    if (!arguments.empty()) {
      throw std::runtime_error("RAND requires no arguments");
    }
    static thread_local std::mt19937_64 rng(
        std::random_device{}() ^
        static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    static thread_local std::uniform_real_distribution<double> uniform(0.0,
                                                                       1.0);
    return Value(uniform(rng));
  }

  if (name == "session_user" || name == "current_user") {
    if (!arguments.empty()) {
      throw std::runtime_error(name + " requires no arguments");
    }
    return Value(std::string("tinylamb"));
  }

  if (name == "generate_uuid") {
    if (!arguments.empty()) {
      throw std::runtime_error("GENERATE_UUID requires no arguments");
    }
    static thread_local std::mt19937_64 rng(
        std::random_device{}() ^
        static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    const uint64_t r1 = rng();
    const uint64_t r2 = rng();
    std::string uuid;
    uuid.reserve(36);
    auto append_hex = [&uuid](uint64_t value, int nibbles) {
      static const char kDigits[] = "0123456789abcdef";
      for (int i = nibbles - 1; i >= 0; --i) {
        uuid.push_back(kDigits[(value >> (4 * i)) & 0xF]);
      }
    };
    append_hex(r1 >> 32, 8);
    uuid.push_back('-');
    append_hex((r1 >> 16) & 0xFFFF, 4);
    uuid.push_back('-');
    uuid.push_back('4');  // RFC 4122 version 4 (random)
    append_hex(r1 & 0x0FFF, 3);
    uuid.push_back('-');
    append_hex(0x8 + (r2 >> 62), 1);  // variant 10xx
    append_hex((r2 >> 46) & 0x3FFF, 3);
    uuid.push_back('-');
    append_hex(r2 & 0xFFFFFFFFFFFFULL, 12);
    return Value(std::move(uuid));
  }

  if (name == "md5" || name == "sha1" || name == "sha256" || name == "sha512") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string input = raw_str(arguments[0]);
    if (name == "md5") {
      return Value(digest::Md5Digest(input));
    }
    if (name == "sha1") {
      return Value(digest::Sha1Digest(input));
    }
    if (name == "sha256") {
      return Value(digest::Sha256Digest(input));
    }
    return Value(digest::Sha512Digest(input));
  }

  if (name == "to_hex") {
    if (arguments.size() != 1) {
      throw std::runtime_error("TO_HEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    return Value(digest::ToHex(raw_str(arguments[0])));
  }

  if (name == "to_base64") {
    if (arguments.size() != 1) {
      throw std::runtime_error("TO_BASE64 requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    static const char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const std::string input = raw_str(arguments[0]);
    std::string out;
    out.reserve((input.size() + 2) / 3 * 4);
    size_t i = 0;
    while (i + 3 <= input.size()) {
      const uint32_t triple = (static_cast<unsigned char>(input[i]) << 16) |
                              (static_cast<unsigned char>(input[i + 1]) << 8) |
                              static_cast<unsigned char>(input[i + 2]);
      out.push_back(kAlphabet[(triple >> 18) & 0x3F]);
      out.push_back(kAlphabet[(triple >> 12) & 0x3F]);
      out.push_back(kAlphabet[(triple >> 6) & 0x3F]);
      out.push_back(kAlphabet[triple & 0x3F]);
      i += 3;
    }
    const size_t remainder = input.size() - i;
    if (remainder == 1) {
      const uint32_t byte = static_cast<unsigned char>(input[i]);
      out.push_back(kAlphabet[(byte >> 2) & 0x3F]);
      out.push_back(kAlphabet[(byte & 0x03) << 4]);
      out += "==";
    } else if (remainder == 2) {
      const uint32_t pair =
          (static_cast<unsigned char>(input[i]) << 8) |
          static_cast<unsigned char>(input[i + 1]);
      out.push_back(kAlphabet[(pair >> 10) & 0x3F]);
      out.push_back(kAlphabet[(pair >> 4) & 0x3F]);
      out.push_back(kAlphabet[(pair & 0x0F) << 2]);
      out.push_back('=');
    }
    return Value(std::move(out));
  }

  if (name == "from_base64") {
    if (arguments.size() != 1) {
      throw std::runtime_error("FROM_BASE64 requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string input = raw_str(arguments[0]);
    auto value_of = [](char c) -> int {
      if (c >= 'A' && c <= 'Z') { return c - 'A'; }
      if (c >= 'a' && c <= 'z') { return c - 'a' + 26; }
      if (c >= '0' && c <= '9') { return c - '0' + 52; }
      if (c == '+') { return 62; }
      if (c == '/') { return 63; }
      return -1;
    };
    std::string out;
    out.reserve(input.size() / 4 * 3);
    uint32_t buffer = 0;
    int bits = 0;
    for (const char c : input) {
      if (std::isspace(static_cast<unsigned char>(c)) != 0) { continue; }
      if (c == '=') { break; }
      const int decoded = value_of(c);
      if (decoded < 0) {
        throw std::runtime_error("FROM_BASE64: invalid character in input");
      }
      buffer = (buffer << 6) | static_cast<uint32_t>(decoded);
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        out.push_back(static_cast<char>((buffer >> bits) & 0xFF));
      }
    }
    return Value(std::move(out));
  }

  if (name == "from_hex") {
    if (arguments.size() != 1) {
      throw std::runtime_error("FROM_HEX requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    std::string text = raw_str(arguments[0]);
    std::string out;
    out.reserve(text.size() / 2);
    auto nibble = [](char c) -> int {
      if (c >= '0' && c <= '9') {
        return c - '0';
      }
      if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
      }
      if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
      }
      return -1;
    };
    size_t i = 0;
    if (text.size() % 2 != 0) {
      const int high = nibble(text[0]);
      if (high < 0) {
        throw std::runtime_error("FROM_HEX: invalid hex string");
      }
      out.push_back(static_cast<char>(high));
      i = 1;
    }
    for (; i + 1 < text.size(); i += 2) {
      const int hi = nibble(text[i]);
      const int lo = nibble(text[i + 1]);
      if (hi < 0 || lo < 0) {
        throw std::runtime_error("FROM_HEX: invalid hex string");
      }
      out.push_back(static_cast<char>(hi * 16 + lo));
    }
    return Value(std::move(out));
  }

  if (name == "safe_convert_bytes_to_string") {
    if (arguments.size() != 1) {
      throw std::runtime_error(
          "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const std::string input = raw_str(arguments[0]);
    std::string out;
    out.reserve(input.size());
    const std::string replacement = "\xEF\xBF\xBD";
    for (size_t i = 0; i < input.size();) {
      const unsigned char lead = static_cast<unsigned char>(input[i]);
      size_t length = 0;
      unsigned char min_cont = 0x80;
      if (lead < 0x80) {
        length = 1;
      } else if (lead >= 0xC2 && lead <= 0xDF) {
        length = 2;
      } else if (lead >= 0xE0 && lead <= 0xEF) {
        length = 3;
        if (lead == 0xE0) {
          min_cont = 0xA0;
        }
        if (lead == 0xED) { /* ED 9F BF is the last code point */
        }
      } else if (lead >= 0xF0 && lead <= 0xF4) {
        length = 4;
        if (lead == 0xF0) {
          min_cont = 0x90;
        }
      }
      bool valid = length > 0 && i + length <= input.size();
      for (size_t j = 1; valid && j < length; ++j) {
        const unsigned char cont = static_cast<unsigned char>(input[i + j]);
        if (cont < (j == 1 ? min_cont : 0x80) || cont > 0xBF) {
          valid = false;
        }
      }
      if (valid && length == 1) {
        out.push_back(input[i]);
        ++i;
      } else if (valid) {
        out.append(input, i, length);
        i += length;
      } else {
        out += replacement;
        ++i;
      }
    }
    return Value(std::move(out));
  }

  if (name == "bit_cast_to_int64" || name == "bit_cast_to_uint64") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return {};
    }
    const Value& input = arguments[0];
    uint64_t bits = 0;
    if (input.type == ValueType::kInt64) {
      bits = static_cast<uint64_t>(input.value.int_value);
    } else if (input.type == ValueType::kDouble) {
      std::memcpy(&bits, &input.value.double_value, sizeof(bits));
    } else if (input.type == ValueType::kVarChar) {
      // BYTES payloads carry little-endian 64-bit patterns.
      const std::string bytes = raw_str(input);
      if (bytes.size() > 8) {
        throw std::runtime_error("BIT_CAST requires at most 8 bytes");
      }
      for (size_t i = 0; i < bytes.size(); ++i) {
        bits |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[i]))
                << (8 * i);
      }
    } else {
      throw std::runtime_error("unsupported BIT_CAST input type");
    }
    if (name == "bit_cast_to_int64") {
      return Value(static_cast<int64_t>(bits));
    }
    // UINT64 results above INT64_MAX have no exact in-engine representation:
    // widen to double so they compare distinct from any signed value.
    if (bits >= 0x8000000000000000ULL) {
      return Value(static_cast<double>(bits));
    }
    return Value(static_cast<int64_t>(bits));
  }

  if (name == "error") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ERROR requires 1 argument");
    }
    throw std::runtime_error(arguments[0].IsNull() ? "ERROR()"
                                                   : raw_str(arguments[0]));
  }

  if (name == "array_reverse") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ARRAY_REVERSE requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return {};
    }
    if (!array.IsArray()) {
      throw std::runtime_error("ARRAY_REVERSE requires an ARRAY argument");
    }
    std::vector<Value> reversed(array.ArrayElements().rbegin(),
                                array.ArrayElements().rend());
    return Value::Array(std::move(reversed), array.ArrayElementSqlType());
  }

  if (name == "array_is_distinct") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ARRAY_IS_DISTINCT requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return {};
    }
    if (!array.IsArray()) {
      throw std::runtime_error("ARRAY_IS_DISTINCT requires an ARRAY argument");
    }
    const auto& elements = array.ArrayElements();
    bool saw_null = false;
    for (size_t i = 0; i < elements.size(); ++i) {
      if (elements[i].IsNull()) {
        if (saw_null) {
          return Value(int64_t{0});
        }
        saw_null = true;
        continue;
      }
      for (size_t j = 0; j < i; ++j) {
        if (elements[j].IsNull()) {
          continue;
        }
        try {
          if (Binary(BinaryOperation::kEquals, elements[j], elements[i])
                  .Truthy()) {
            return Value(int64_t{0});
          }
        } catch (...) {
          // incomparable element types are simply not duplicates
        }
      }
    }
    return Value(int64_t{1});
  }

  if (name == "array_first" || name == "array_last") {
    if (arguments.size() != 1) {
      throw std::runtime_error(name + " requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return {};
    }
    if (!array.IsArray()) {
      throw std::runtime_error(name + " requires an ARRAY argument");
    }
    if (array.ArrayElements().empty()) {
      throw std::runtime_error(
          name == "array_first"
              ? "ARRAY_FIRST cannot get the first element of an empty array"
              : "ARRAY_LAST cannot get the last element of an empty array");
    }
    if (name == "array_first") {
      return array.ArrayElements().front();
    }
    return array.ArrayElements().back();
  }

  if (name == "array_slice") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error("ARRAY_SLICE requires two or three arguments");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return {};
    }
    if (!array.IsArray()) {
      throw std::runtime_error("ARRAY_SLICE requires an ARRAY argument");
    }
    auto as_index = [&](const Value& v) -> int64_t {
      if (v.IsNull() ||
          (v.type != ValueType::kInt64 && v.type != ValueType::kDouble)) {
        throw std::runtime_error("ARRAY_SLICE offsets must be integers");
      }
      return v.type == ValueType::kInt64
                 ? v.value.int_value
                 : static_cast<int64_t>(v.value.double_value);
    };
    const auto& elements = array.ArrayElements();
    const int64_t n = static_cast<int64_t>(elements.size());
    const int64_t start_raw = as_index(arguments[1]);
    const int64_t end_raw = arguments.size() == 3 ? as_index(arguments[2]) : n;
    const auto normalize = [n](int64_t offset) -> int64_t {
      if (offset < 0) {
        offset += n;
      }
      if (offset < 0) {
        return 0;
      }
      if (offset > n) {
        return n;
      }
      return offset;
    };
    int64_t start = normalize(start_raw);
    int64_t end = normalize(end_raw);
    if (start > end) {
      start = end;
    }
    std::vector<Value> sliced(elements.begin() + static_cast<size_t>(start),
                              elements.begin() + static_cast<size_t>(end));
    return Value::Array(std::move(sliced), array.ArrayElementSqlType());
  }

  if (name == "euclidean_distance") {
    if (arguments.size() != 2) {
      throw std::runtime_error("EUCLIDEAN_DISTANCE requires 2 arguments");
    }
    const Value& left = arguments[0];
    const Value& right = arguments[1];
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    if (!left.IsArray() || !right.IsArray()) {
      throw std::runtime_error("EUCLIDEAN_DISTANCE requires ARRAY arguments");
    }
    const auto& a = left.ArrayElements();
    const auto& b = right.ArrayElements();
    if (a.size() != b.size()) {
      throw std::runtime_error(
          "EUCLIDEAN_DISTANCE arrays must have equal sizes");
    }
    auto coordinate = [](const Value& v) -> double {
      if (v.IsNull()) {
        return 0.0;
      }
      if (v.type == ValueType::kDouble) {
        return v.value.double_value;
      }
      if (v.type == ValueType::kInt64) {
        return static_cast<double>(v.value.int_value);
      }
      if (v.type == ValueType::kVarChar) {
        // (key, value) struct elements carry their coordinate in the last
        // member of the textual struct form {"k":...,"value":10}.
        const std::string_view text(v.value.varchar_value);
        const size_t last_colon = text.rfind(':');
        if (text.size() >= 2 && text.front() == '{' && text.back() == '}' &&
            last_colon != std::string_view::npos &&
            last_colon + 1 < text.size()) {
          std::string tail(text.substr(last_colon + 1));
          while (!tail.empty() &&
                 (std::isspace(static_cast<unsigned char>(tail.back())) != 0 ||
                  tail.back() == '}')) {
            tail.pop_back();
          }
          char* parse_end = nullptr;
          const double parsed = std::strtod(tail.c_str(), &parse_end);
          if (parse_end != tail.c_str() && *parse_end == '\0') {
            return parsed;
          }
        }
      }
      throw std::runtime_error(
          "EUCLIDEAN_DISTANCE requires numeric array elements");
    };
    double total = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
      const double diff = coordinate(a[i]) - coordinate(b[i]);
      total += diff * diff;
    }
    return Value(std::sqrt(total));
  }

  if (name == "element") {
    if (arguments.size() != 1) {
      throw std::runtime_error("ELEMENT requires 1 argument");
    }
    const Value& array = arguments[0];
    if (array.IsNull()) {
      return {};
    }
    if (!array.IsArray()) {
      throw std::runtime_error("ELEMENT requires an ARRAY argument");
    }
    if (array.ArrayElements().size() != 1) {
      throw std::runtime_error("More than one element");
    }
    return array.ArrayElements().front();
  }

  if (name == "generate_array") {
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw std::runtime_error(name + " requires two or three arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull() ||
        (arguments.size() == 3 && arguments[2].IsNull())) {
      return {};
    }
    auto numeric = [](const Value& v) -> double {
      if (v.type == ValueType::kInt64) {
        return static_cast<double>(v.value.int_value);
      }
      if (v.type == ValueType::kDouble) {
        return v.value.double_value;
      }
      throw std::runtime_error("GENERATE_ARRAY requires numeric arguments");
    };
    const double start = numeric(arguments[0]);
    const double end = numeric(arguments[1]);
    double step = arguments.size() == 3 ? numeric(arguments[2]) : 0.0;
    if (step == 0.0) {
      if (arguments.size() == 3) {
        throw std::runtime_error("Sequence step cannot be 0.");
      }
      step = start <= end ? 1.0 : -1.0;
    }
    if ((start < end && step < 0) || (start > end && step > 0)) {
      return Value::Array({}, "INT64");
    }
    bool saw_double =
        arguments[0].type == ValueType::kDouble ||
        arguments[1].type == ValueType::kDouble ||
        (arguments.size() == 3 && arguments[2].type == ValueType::kDouble);
    bool saw_uint = false;
    for (const Expression& argument : call.Args()) {
      if (argument->Type() != TypeTag::kCastExp) {
        continue;
      }
      std::string target = argument->AsCastExpression().TargetTypeName();
      for (char& c : target) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      }
      if (target.find("UINT") != std::string::npos) {
        saw_uint = true;
      }
    }
    const bool integral =
        !saw_double &&
        start == static_cast<double>(static_cast<int64_t>(start)) &&
        step == static_cast<double>(static_cast<int64_t>(step));
    const std::string element_type =
        integral ? (saw_uint ? "UINT64" : "INT64") : "DOUBLE";
    std::vector<Value> elements;
    constexpr size_t kMaxGeneratedElements = 1000000;
    for (double value = start; elements.size() < kMaxGeneratedElements &&
                               (step > 0 ? value <= end : value >= end);
         value += step) {
      if (integral) {
        elements.push_back(Value(static_cast<int64_t>(value)));
      } else {
        elements.push_back(Value(value));
      }
    }
    return Value::Array(std::move(elements), element_type);
  }

  if (name == "generate_date_array") {
    if (arguments.size() < 2 || arguments.size() > 4) {
      throw std::runtime_error(
          "GENERATE_DATE_ARRAY requires two or more arguments");
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      return {};
    }
    auto to_days = [&raw_str](const Value& v) -> int64_t {
      if (v.type == ValueType::kDate) {
        return v.DateDays();
      }
      try {
        return Value::Date(raw_str(v)).DateDays();
      } catch (...) {
        throw std::runtime_error("GENERATE_DATE_ARRAY requires DATE arguments");
      }
    };
    int64_t step_days = 1;
    if (arguments.size() >= 3 && !arguments[2].IsNull()) {
      // INTERVAL steps ride as unevaluated IntervalExpression arguments.
      if (call.Args().size() >= 3 &&
          call.Args()[2]->Type() == TypeTag::kIntervalExp) {
        const auto& interval = call.Args()[2]->AsIntervalExpression();
        std::string unit = std::string(interval.Unit());
        for (char& c : unit) {
          c = static_cast<char>(std::tolower(c));
        }
        step_days = unit == "day" || unit == "days" ? interval.Amount() : 0;
      } else if (arguments[2].type == ValueType::kVarChar) {
        // Dynamic steps arrive as make_interval(...) results encoded
        // "Y-M D H:M:S"; only whole-day counts are supported here.
        long long years = 0, months = 0, days = 0, hours = 0, mins = 0,
                  secs = 0;
        const std::string encoded = raw_str(arguments[2]);
        if (sscanf(encoded.c_str(), "%lld-%lld %lld %lld:%lld:%lld", &years,
                   &months, &days, &hours, &mins, &secs) >= 3 &&
            years == 0 && months == 0 && hours == 0 && mins == 0 &&
            secs == 0 && days != 0) {
          step_days = static_cast<int64_t>(days);
        } else {
          throw std::runtime_error(
              "GENERATE_DATE_ARRAY requires an INTERVAL step");
        }
      } else {
        throw std::runtime_error(
            "GENERATE_DATE_ARRAY requires an INTERVAL step");
      }
      if (step_days == 0) {
        throw std::runtime_error(
            "GENERATE_DATE_ARRAY supports only whole-DAY intervals");
      }
    }
    const int64_t start_day = to_days(arguments[0]);
    const int64_t end_day = to_days(arguments[1]);
    std::vector<Value> elements;
    constexpr size_t kMaxGeneratedDates = 1000000;
    if ((step_days > 0 && start_day <= end_day) ||
        (step_days < 0 && start_day >= end_day)) {
      for (int64_t day = start_day;
           elements.size() < kMaxGeneratedDates &&
           (step_days > 0 ? day <= end_day : day >= end_day);
           day += step_days) {
        elements.push_back(Value::DateFromDays(day));
      }
    }
    return Value::Array(std::move(elements), "DATE");
  }

  throw std::runtime_error("unsupported function " + name);
}

}  // namespace

// Recursive descent over the expression tree is the intended evaluation
// strategy.
Value Evaluate(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.name == "*") {
        return Value(1);
      }
      try {
        return Lookup(name, scope);
      } catch (const std::runtime_error&) {
        std::string upper_name = name.name;
        for (char& c : upper_name) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        if (upper_name == "DAY" || upper_name == "WEEK" ||
            upper_name == "MONTH" || upper_name == "QUARTER" ||
            upper_name == "YEAR" || upper_name == "HOUR" ||
            upper_name == "MINUTE" || upper_name == "SECOND" ||
            upper_name == "MILLISECOND" || upper_name == "MICROSECOND" ||
            upper_name == "NANOSECOND" || upper_name == "MONDAY" ||
            upper_name == "TUESDAY" || upper_name == "WEDNESDAY" ||
            upper_name == "THURSDAY" || upper_name == "FRIDAY" ||
            upper_name == "SATURDAY" || upper_name == "SUNDAY" ||
            upper_name == "ISOYEAR" || upper_name == "ISOWEEK" ||
            upper_name == "DATE") {
          return Value(std::move(upper_name));
        }
        throw;
      }
    }
    case TypeTag::kConstantValue:
      return expression->AsConstantValue().GetValue();
    case TypeTag::kBinaryExp: {
      const auto& value = expression->AsBinaryExpression();
      // Canonical short-circuit dispatch (BinaryExpression::Evaluate): only a
      // non-NULL left operand that already decides the result skips the right
      // child; NULL falls through to the three-valued Binary() rules.
      if (value.Op() == BinaryOperation::kAnd ||
          value.Op() == BinaryOperation::kOr) {
        const Value left =
            Evaluate(value.Left(), scope, aggregates, context, ctes);
        if (!left.IsNull() &&
            left.Truthy() != (value.Op() == BinaryOperation::kAnd)) {
          return Value(value.Op() == BinaryOperation::kOr);
        }
        return Binary(
            value.Op(), left,
            Evaluate(value.Right(), scope, aggregates, context, ctes));
      }
      return Binary(value.Op(),
                    Evaluate(value.Left(), scope, aggregates, context, ctes),
                    Evaluate(value.Right(), scope, aggregates, context, ctes));
    }
    case TypeTag::kUnaryExp: {
      const auto& value = expression->AsUnaryExpression();
      // Canonical semantics via the AST evaluator: NOT NULL is NULL (three-
      // valued), IS [NOT] NULL never propagates NULL, minus guards overflow.
      return EvaluateUnary(value.Op(), Evaluate(value.Child(), scope,
                                                aggregates, context, ctes));
    }
    case TypeTag::kAggregateExp:
      if (aggregates == nullptr) {
        throw std::runtime_error("aggregate outside grouping");
      }
      return Aggregate(expression->AsAggregateExpression(), *aggregates);
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (Truthy(Evaluate(condition, scope, aggregates, context, ctes))) {
          return Evaluate(result, scope, aggregates, context, ctes);
        }
      }
      return value.else_clause_ ? Evaluate(value.else_clause_, scope,
                                           aggregates, context, ctes)
                                : Value();
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      // Canonical three-valued membership (InExpression::Evaluate): a match
      // decides TRUE; otherwise any NULL (test value or list item) yields
      // UNKNOWN instead of FALSE.
      const Value test =
          Evaluate(value.child_, scope, aggregates, context, ctes);
      bool found = false;
      bool saw_null = test.IsNull();
      for (const Expression& item : value.list_) {
        const Value candidate =
            Evaluate(item, scope, aggregates, context, ctes);
        saw_null = saw_null || candidate.IsNull();
        if (!found && !test.IsNull() && !candidate.IsNull() &&
            Binary(BinaryOperation::kEquals, test, candidate).Truthy()) {
          found = true;
        }
      }
      if (found) {
        return Value(true);
      }
      return saw_null ? Value() : Value(false);
    }
    case TypeTag::kFunctionCallExp:
      return EvaluateFunction(expression->AsFunctionCallExpression(), scope,
                              aggregates, context, ctes);
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Value> elements;
      elements.reserve(array.Elements().size());
      std::string inferred_type = array.ElementSqlType();
      for (const Expression& element : array.Elements()) {
        Value val = Evaluate(element, scope, aggregates, context, ctes);
        if ((inferred_type.empty() || inferred_type == "INT64") &&
            !val.IsNull()) {
          if (val.type == ValueType::kVarChar) {
            inferred_type = "STRING";
          } else if (val.type == ValueType::kDouble) {
            inferred_type = "DOUBLE";
          } else if (val.type == ValueType::kDate) {
            inferred_type = "DATE";
          } else if (val.IsArray()) {
            inferred_type = "ARRAY<" + val.ArrayElementSqlType() + ">";
          }
        }
        elements.push_back(std::move(val));
      }
      if (inferred_type.empty()) {
        inferred_type = "INT64";
      }
      return Value::Array(std::move(elements), inferred_type);
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      const Value val =
          Evaluate(cast.Child(), scope, aggregates, context, ctes);
      return CastExpressionExp(ConstantValueExp(val), cast.TargetTypeName(),
                               cast.ReturnNullOnError())
          ->Evaluate(Row(), Schema());
    }

    case TypeTag::kQueryExp: {
      const auto& value = expression->AsQueryExpression();
      // Subqueries carrying their own WITH clauses must run through
      // ExecuteQuery, which materializes the local CTEs first.
      const bool has_own_ctes = !value.Query()->WithQueries().empty();
      std::optional<Relation> indexed =
          has_own_ctes ? std::nullopt
                       : ExecuteCorrelatedSingleSource(context, *value.Query(),
                                                       scope, ctes);
      std::optional<Relation> executed;
      const Relation* relation = indexed ? &*indexed : nullptr;
      bool uncorrelated = false;
      if (relation == nullptr && !has_own_ctes) {
        relation = ExecuteCachedUncorrelated(context, *value.Query(), ctes);
        uncorrelated = relation != nullptr;
      }
      if (relation == nullptr) {
        executed = ExecuteQuery(context, *value.Query(), &scope, ctes);
        relation = &*executed;
      }
      auto& row_source = const_cast<Relation&>(*relation);
      const bool as_struct = value.Query()->AsStruct();
      // Declared select-list names feed struct field keys; without them
      // SELECT AS STRUCT rows would carry positional fN keys.
      const Schema& subquery_schema = relation->schema;
      if (value.Exists()) {
        const bool exists = relation->TotalRows() > 0;
        return Value(value.Negated() ? !exists : exists);
      }
      if (value.Test()) {
        const Value test =
            Evaluate(value.Test(), scope, aggregates, context, ctes);
        if (value.Mode() != QuantifierMode::kIn) {
          std::vector<Value> candidates;
          candidates.reserve(relation->TotalRows());
          row_source.ForEachRow([&](const Row& row) {
            candidates.push_back(
                ProjectSubqueryRow(row, as_struct, &subquery_schema));
          });
          return EvaluateQuantifiedComparison(value.Op(), value.Mode(), test,
                                              candidates);
        }
        // Canonical three-valued IN membership: a match decides TRUE, any
        // NULL (test value or row value) turns a miss into UNKNOWN.
        auto collation_conflict = [&](const Value& projected) {
          return !test.IsNull() && !projected.IsNull() &&
                 test.Collation() != 0 && projected.Collation() != 0 &&
                 test.Collation() != projected.Collation();
        };
        bool found = false;
        bool saw_null = test.IsNull();
        if (uncorrelated && context.execution_runtime() != nullptr) {
          auto [cached, inserted] =
              context.execution_runtime()->uncorrelated_membership.try_emplace(
                  value.Query().get());
          if (inserted) {
            cached->second.reserve(relation->TotalRows());
            row_source.ForEachRow([&](const Row& row) {
              const Value projected = ProjectSubqueryRow(row, as_struct, &subquery_schema);
              if (collation_conflict(projected)) {
                throw std::runtime_error(
                    "Collation conflict between the IN "
                    "operands");
              }
              if (!projected.IsNull()) {
                cached->second.insert(projected);
              }
            });
            ++context.execution_runtime()->uncorrelated_hash_builds;
          }
          ++context.execution_runtime()->uncorrelated_hash_probes;
          found = !test.IsNull() && cached->second.contains(test);
          if (!found && !test.IsNull() &&
              cached->second.size() <
                  static_cast<size_t>(relation->TotalRows())) {
            // The hash set excludes NULL keys; a size shortfall means the
            // relation may contain NULLs that turn the miss into UNKNOWN.
            row_source.ForEachRow([&](const Row& row) {
              if (!row.values_.empty() &&
                  ProjectSubqueryRow(row, as_struct, &subquery_schema).IsNull()) {
                saw_null = true;
              }
            });
          }
        } else {
          row_source.ForEachRow([&](const Row& row) {
            if (found || row.values_.empty()) {
              return;
            }
            const Value projected = ProjectSubqueryRow(row, as_struct, &subquery_schema);
            if (collation_conflict(projected)) {
              throw std::runtime_error(
                  "Collation conflict between the IN "
                  "operands");
            }
            saw_null = saw_null || projected.IsNull();
            if (Truthy(Binary(BinaryOperation::kEquals, test, projected))) {
              found = true;
            }
          });
        }
        const Value membership =
            found ? Value(true) : (saw_null ? Value() : Value(false));
        if (!value.Negated()) {
          return membership;
        }
        return membership.IsNull() ? Value() : Value(!membership.Truthy());
      }
      // ARRAY(SELECT ...): every projected row becomes one array element.
      // Empty subqueries produce an empty array, not NULL.
      if (value.ArrayResult()) {
        std::vector<Value> elements;
        row_source.ForEachRow([&](const Row& row) {
          if (row.values_.empty()) { return;
}
          elements.push_back(ProjectSubqueryRow(row, as_struct));
        });
        std::string element_type = value.ArrayElementSqlType();
        if (element_type.empty()) {
          for (const Value& element : elements) {
            if (!element.IsNull()) {
              element_type = ElementSqlTypeName(element.type);
              break;
            }
          }
        }
        if (element_type.empty()) {
          if (relation->schema.ColumnCount() > 0) {
            element_type =
                ElementSqlTypeName(relation->schema.GetColumn(0).Type());
          }
          if (element_type.empty()) { element_type = "INT64"; }
        }
        return Value::Array(std::move(elements), std::move(element_type));
      }
      std::optional<Row> first;
      size_t row_count = 0;
      row_source.ForEachRow([&](const Row& row) {
        ++row_count;
        // Scalar subquery: only the first row matters.
        if (!first) {
          first = row;
        }
      });
      if (row_count > 1) {
        // GoogleSQL: a scalar subquery must produce at most one row.
        throw std::runtime_error("Scalar subquery produced more than one row");
      }
      if (!first || first->values_.empty()) {
        // Empty subquery: scalar NULL.
        return {};
      }
      return ProjectSubqueryRow(*first, as_struct, &subquery_schema);
    }
    case TypeTag::kIntervalExp:
      return expression->Evaluate(Row(), Schema());
    default:
      throw std::runtime_error("unsupported expression type");
  }
}
Schema QualifySchema(const Schema& schema, std::string_view qualifier) {
  std::vector<Column> columns;
  columns.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& column = schema.GetColumn(i);
    std::string col_name = column.Name().name;
    if (col_name.empty() ||
        (col_name.starts_with("$expr") && schema.ColumnCount() == 1)) {
      col_name = std::string(qualifier);
    }
    columns.emplace_back(ColumnName(qualifier, col_name), column.Type());
  }
  return {"", std::move(columns)};
}

namespace {
// Mutual recursion with CollectStatementColumns below; the statement/expression
// nesting is bounded by query depth.
void CollectExpressionColumns(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::unordered_set<ColumnName>* columns) {
  if (!expression) {
    return;
  }
  std::unordered_set<ColumnName> touched = expression->TouchedColumns();
  columns->merge(touched);
  if (expression->Type() == TypeTag::kQueryExp) {
    CollectStatementColumns(*expression->AsQueryExpression().Query(), columns);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectExpressionColumns(child, columns);
  }
}
}  // namespace

void CollectStatementColumns(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement, std::unordered_set<ColumnName>* columns) {
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectExpressionColumns(projection.expression, columns);
  }
  CollectExpressionColumns(statement.WhereClause(), columns);
  for (const Expression& key : statement.GroupBy()) {
    CollectExpressionColumns(key, columns);
  }
  CollectExpressionColumns(statement.Having(), columns);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectExpressionColumns(term.expression, columns);
  }
  for (const SelectSource& source : statement.Sources()) {
    CollectExpressionColumns(source.join_condition, columns);
    // UNNEST arguments may reference sibling sources of the same FROM clause
    // or outer scopes; their columns must survive projection pruning.
    CollectExpressionColumns(source.unnest, columns);
    if (source.query) {
      CollectStatementColumns(*source.query, columns);
    }
  }
  for (const auto& [name, query] : statement.WithQueries()) {
    (void)name;
    CollectStatementColumns(*query, columns);
  }
}

std::vector<slot_t> RequiredColumns(const SelectStatement& statement,
                                    const Schema& schema, bool ignore_star) {
  const bool selects_star =
      !ignore_star &&
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return projection.expression->Type() ==
                               TypeTag::kColumnValue &&
                           projection.expression->AsColumnValue()
                                   .GetColumnName()
                                   .name == "*";
                  });
  std::unordered_set<ColumnName> referenced;
  CollectStatementColumns(statement, &referenced);
  // Deep field paths (`t.Info.str_value`) address nested fields of a base
  // column; every dotted prefix must survive projection pruning or the
  // enclosing value is lost from the scan.
  {
    std::unordered_set<ColumnName> expanded;
    expanded.reserve(referenced.size() * 2);
    for (const ColumnName& name : referenced) {
      expanded.insert(name);
      if (name.schema.find('.') == std::string::npos &&
          name.name.find('.') == std::string::npos) {
        continue;
      }
      std::vector<std::string> parts;
      for (std::string& part : SplitDottedName(name.schema)) {
        parts.push_back(std::move(part));
      }
      for (std::string& part : SplitDottedName(name.name)) {
        parts.push_back(std::move(part));
      }
      // Every proper prefix of the path is a potential base column: both as
      // an alias-qualified reference (`t`, `t.Info`) and as a bare name.
      for (size_t k = 0; k + 1 < parts.size(); ++k) {
        std::string qualifier;
        for (size_t i = 0; i < k; ++i) {
          if (!qualifier.empty()) {
            qualifier += '.';
          }
          qualifier += parts[i];
        }
        expanded.insert(ColumnName(qualifier, parts[k]));
        expanded.insert(ColumnName("", parts[k]));
      }
    }
    referenced = std::move(expanded);
  }
  std::vector<slot_t> result;
  result.reserve(schema.ColumnCount());
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    const bool needed =
        selects_star || std::ranges::any_of(referenced, [&](const auto& name) {
          if (name.name == "*") {
            return false;
          }
          // GoogleSQL identifiers are case-insensitive; references may also
          // qualify this relation by table name or any alias.  Over-
          // projecting a scan is harmless; dropping a column some scope
          // reference needs breaks evaluation.
          return IdentifierEquals(name.name, candidate.name) &&
                 (name.schema.empty() ||
                  IdentifierEquals(name.schema, candidate.schema));
        });
    if (needed) {
      result.push_back(i);
    }
  }
  return result;
}
Schema ProjectSchema(const Schema& schema,
                     const std::vector<slot_t>& projection) {
  std::vector<Column> columns;
  columns.reserve(projection.size());
  for (slot_t offset : projection) {
    columns.push_back(schema.GetColumn(offset));
  }
  return {schema.Name(), std::move(columns)};
}

std::string BaseRelationCacheKey(std::string_view table,
                                 const std::vector<slot_t>* projection) {
  std::string key(table);
  if (projection == nullptr) {
    return key;
  }
  key.push_back('#');
  for (slot_t column : *projection) {
    key += std::to_string(column);
    key.push_back(',');
  }
  return key;
}

bool ReusesBaseRelation(TransactionContext& context,
                        const SelectSource& source) {
  return context.execution_runtime() != nullptr &&
         context.execution_runtime()->reusable_base_relations.contains(
             source.table);
}

ValueType ValueTypeOf(const Value& value) { return value.type; }

std::string ProjectionName(const NamedExpression& projection, size_t index) {
  if (!projection.name.empty()) {
    return projection.name;
  }
  if (projection.expression->Type() == TypeTag::kColumnValue) {
    return projection.expression->AsColumnValue().GetColumnName().name;
  }
  return "$expr" + std::to_string(index);
}

}  // namespace tinylamb::relational_detail
