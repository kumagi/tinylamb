/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/expression_eval.hpp"

// The regexp_* branches below drive libstdc++'s <regex>, and under
// sanitizer-enabled -O2 inlining GCC 15 reports -Wmaybe-uninitialized for
// std::function state inside <regex_automaton.h>/<std_function.h> even though
// every std::regex object here is fully constructed before use. The false
// positive lives in the standard library, not in this file; suppress it for
// this translation unit only. The pragma must precede the includes it covers:
// the diagnostics are attributed to code instantiated from those headers.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#if defined(__GNUC__) && !defined(__clang__)
// GCC 15 reports -Wmaybe-uninitialized inside <regex>'s NFA construction when
// instantiating std::regex in sanitizer builds; the library code is fine.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <regex>
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/digest.hpp"
#include "common/math_utils.hpp"
#include "common/regex_guard.hpp"
#include "common/string_util.hpp"
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
#include "expression/json_doc.hpp"
#include "expression/json_text.hpp"
#include "expression/named_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/statement.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Single definition of truthiness: Value::Truthy is the semantic reference
// (AST evaluation, VectorizedExpression, and DataChunk all use it). The
// residual/scan-filter path must not diverge for DOUBLE 0.0 or "".
bool Truthy(const Value& value) { return value.Truthy(); }

size_t DistinctValueHash::operator()(const Value& value) const {
  return std::hash<Value>()(CanonicalDistinctValue(value));
}

bool DistinctValueEqual::operator()(const Value& left,
                                    const Value& right) const {
  if (left.type != right.type) {
    return false;
  }
  if (left.type == ValueType::kDouble) {
    const double x = left.value.double_value;
    const double y = right.value.double_value;
    if (std::isnan(x) && std::isnan(y)) {
      return true;
    }
    return x == y;
  }
  return left == right;
}

Value CanonicalDistinctValue(const Value& value) {
  // GROUP BY / DISTINCT / semi-anti key canonicalization. A case-insensitive
  // key is folded through the shared collation contract (type/collation.hpp)
  // so a grouped 'a'/'A' pair and an ORDER BY over the same key agree on
  // which rows are peers.
  if (CollationOf(value) == Collation::kCaseInsensitive) {
    return ApplyCollation(value, Collation::kCaseInsensitive);
  }
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

bool GroupKeyEqual::operator()(const Row& a, const Row& b) const {
  if (a.values_.size() != b.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < a.values_.size(); ++i) {
    const Value& x = a.values_[i];
    const Value& y = b.values_[i];
    if (x.type != y.type) {
      return false;
    }
    if (x.type == ValueType::kDouble) {
      const double p = x.value.double_value;
      const double q = y.value.double_value;
      if (std::isnan(p) && std::isnan(q)) {
        continue;
      }
      if (!(p == q)) {
        return false;
      }
      continue;
    }
    if (!(x == y)) {
      return false;
    }
  }
  return true;
}

size_t GroupKeyTransparentHash::operator()(
    const std::vector<Value>& values) const {
  // Reproduces std::hash<Row> bit-for-bit so a scratch key vector and the Row
  // it becomes probe the same bucket.
  uint64_t ret = 0x9e3779b97f4a7c15ULL;
  for (const auto& v : values) {
    ret = (ret << 7) | (ret >> 57);
    ret ^= std::hash<Value>()(v);
    ret *= 0x100000001b3ULL;
  }
  return ret;
}

bool GroupKeyTransparentEqual::operator()(const Row& a, const Row& b) const {
  return GroupKeyEqual{}(a, b);
}

bool GroupKeyTransparentEqual::operator()(const std::vector<Value>& a,
                                          const Row& b) const {
  if (a.size() != b.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (!(a[i] == b.values_[i])) {
      return false;
    }
  }
  return true;
}

bool GroupKeyTransparentEqual::operator()(const Row& a,
                                          const std::vector<Value>& b) const {
  return (*this)(b, a);
}

namespace {

StatusOr<int> FindColumn(const Schema& schema, const ColumnName& name,
                         bool allow_bare_fallback = true) {
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
        return StatusError(StatusCode::kInvalidArgument,
                           "ambiguous column " + name.name);
      }
      match = static_cast<int>(i);
    }
  }
  if (match < 0 && !name.schema.empty() && allow_bare_fallback) {
    // Bare-name fallback for a qualified reference is only safe when this
    // schema carries no aliases at all (degraded metadata such as raw catalog
    // rows).  Once columns are aliased, a reference qualified by another
    // alias (`t1.x` seen from inside `... AS t2`) belongs to an outer scope;
    // capturing a same-named local column silently turns correlated
    // predicates into wrong local filters.  Callers resolve every scope
    // without this fallback first (Lookup below), so an inner projected
    // schema never steals a qualified reference from the tuple it names.
    bool any_qualified = false;
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (!schema.GetColumn(i).Name().schema.empty()) {
        any_qualified = true;
        break;
      }
    }
    if (!any_qualified) {
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        if (IdentifierEquals(schema.GetColumn(i).Name().name, name.name)) {
          if (match >= 0) {
            return -1;
          }
          match = static_cast<int>(i);
        }
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

StatusOr<int> FindColumnCached(const Schema& schema, const ColumnName& name,
                               bool allow_bare_fallback,
                               ColumnLookupCache* cache) {
  if (cache == nullptr) [[unlikely]] {
    return FindColumn(schema, name, allow_bare_fallback);
  }
  auto& entries = cache->schemas[schema.LookupIdentity()];
  auto [it, inserted] = entries.try_emplace(name);
  int& cached = allow_bare_fallback ? it->second.fallback : it->second.strict;
  if (inserted || cached == -2) [[unlikely]] {
    StatusOr<int> result = FindColumn(schema, name, allow_bare_fallback);
    if (result.HasValue()) {
      cached = result.Value();
      return result;
    }
    return result;
  }
  return cached;
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
  while (begin < end &&
         (std::isspace(static_cast<unsigned char>(s[begin])) != 0)) {
    ++begin;
  }
  while (end > begin &&
         (std::isspace(static_cast<unsigned char>(s[end - 1])) != 0)) {
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
    const std::string unquoted = token.substr(1, token.size() - 2);
    constexpr std::string_view kDateMarker = "__tinylamb_date__:";
    if (unquoted.starts_with(kDateMarker)) {
      return Value::Date(unquoted.substr(kDateMarker.size()));
    }
    return Value(std::string(unquoted));
  }
  if (token.front() == '{' && token.back() == '}') {
    return Value(std::move(token));  // nested struct stays encoded
  }
  // Non-finite doubles encode as bare identifier tokens (std::to_string
  // output); recognize them before the generic string fallback.
  if (token == "inf" || token == "+inf" || token == "infinity" ||
      token == "+infinity") {
    return Value(std::numeric_limits<double>::infinity());
  }
  if (token == "-inf" || token == "-infinity") {
    return Value(-std::numeric_limits<double>::infinity());
  }
  if (token == "nan" || token == "+nan" || token == "-nan") {
    return Value(std::numeric_limits<double>::quiet_NaN());
  }
  if (token.find('.') != std::string::npos ||
      token.find('e') != std::string::npos ||
      token.find('E') != std::string::npos) {
    try {
      return Value(std::stod(token));
    } catch (const std::exception& error) {
      (void)error;
    }
  }
  try {
    return Value(static_cast<int64_t>(std::stoll(token)));
  } catch (...) {
    return Value(std::move(token));
  }
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
  size_t pos = 0;
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
         (std::isspace(static_cast<unsigned char>(json[value_start])) != 0)) {
    ++value_start;
  }
  size_t value_end = 0;
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
    while (value_end < json.size() && json[value_end] != ',' &&
           json[value_end] != '}') {
      ++value_end;
    }
  }
  std::string_view raw = json.substr(raw_begin, value_end - raw_begin);
  // Struct constructors encode array-valued fields with Value::AsString(),
  // i.e. "ARRAY<ELEMENT_TYPE>[e0, e1, ...]".  Decode those into real arrays
  // so downstream UNNEST / field traversal sees array values.
  if (raw.size() > 6 && raw.starts_with("ARRAY<")) {
    const size_t bracket = raw.find('[');
    if (bracket != std::string_view::npos && raw.back() == ']') {
      std::string_view inner =
          raw.substr(bracket + 1, raw.size() - bracket - 2);
      std::vector<Value> elements;
      const std::string element_type_name(raw.substr(6, raw.find(">[") - 6));
      auto parse_element = [&](std::string_view token) {
        if (element_type_name == "DATE") {
          return Value::Date(TrimFieldToken(token));
        }
        return ScalarFromText(token);
      };
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
          elements.push_back(parse_element(inner.substr(start, k - start)));
          start = k + 1;
        }
      }
      if (!inner.empty()) {
        elements.push_back(parse_element(inner.substr(start)));
      }
      const std::string element_type = InferElementSqlType(elements);
      *out = Value::Array(std::move(elements), element_type);
      return true;
    }
  }
  if (!raw.empty() && raw.front() == '[') {
    // Split top-level elements and decode each.
    std::string_view inner = raw.substr(1, raw.size() > 1 ? raw.size() - 2 : 0);
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
    const std::string element_type = InferElementSqlType(elements);
    *out = Value::Array(std::move(elements), element_type);
    return true;
  }
  *out = ScalarFromText(raw);
  return true;
}

// Proto text field reads (scalar / repeated / nested / has_ pseudo fields,
// DATE/TIMESTAMP format annotations and enum defaults) live in the shared
// expression-layer helper; see expression/proto_text.hpp.

StatusOr<Value> TryResolveFieldPath(const Value& base,
                                    const std::vector<std::string>& fields) {
  Value current = base;
  for (const std::string& field : fields) {
    if (current.IsNull()) {
      return Value();
    }
    if (current.type != ValueType::kVarChar) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "cannot access field " + field + " of a non-struct value");
    }
    const std::string_view text(current.value.varchar_value);
    bool resolved = JsonExtractField(text, field, &current) ||
                    TryProtoTextGetField(text, field, &current);
    if (!resolved) {
      return StatusError(StatusCode::kInvalidArgument,
                         "field " + field + " not found");
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
  auto normalize_encoded_arrays = [](std::string_view text) {
    std::string result;
    for (size_t i = 0; i < text.size();) {
      if (text.substr(i, 6) == "ARRAY<") {
        const size_t open = text.find('[', i + 6);
        if (open != std::string_view::npos) {
          size_t close = open;
          int depth = 0;
          for (; close < text.size(); ++close) {
            if (text[close] == '[') {
              ++depth;
            } else if (text[close] == ']' && --depth == 0) {
              break;
            }
          }
          if (close < text.size()) {
            result.push_back('[');
            result.append(text.substr(open + 1, close - open - 1));
            result.push_back(']');
            i = close + 1;
            continue;
          }
        }
      }
      result.push_back(text[i++]);
    }
    return result;
  };
  auto scalar_to_json =
      [&normalize_encoded_arrays](const Value& v) -> std::string {
    switch (v.type) {
      case ValueType::kNull:
        return "null";
      case ValueType::kInt64:
        return std::to_string(v.value.int_value);
      case ValueType::kDouble:
        return FormatDoubleShortest(v.value.double_value);
      case ValueType::kVarChar:
        if (v.value.varchar_value.size() >= 2 &&
            ((v.value.varchar_value.front() == '{' &&
              v.value.varchar_value.back() == '}') ||
             (v.value.varchar_value.front() == '[' &&
              v.value.varchar_value.back() == ']'))) {
          return normalize_encoded_arrays(v.value.varchar_value);
        }
        if (LooksLikeProtoText(v.value.varchar_value)) {
          return "{" + NormalizeProtoText(v.value.varchar_value).value_or("") +
                 "}";
        }
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
              inner += FormatDoubleShortest(element.value.double_value);
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

// ---- Three-valued SQL equality over struct-encoded values ------------------
// Structs live as flat JSON objects (`{"f1":1,"f2":...}`); GoogleSQL compares
// them positionally with full three-valued logic (a NULL anywhere yields
// UNKNOWN, never FALSE), and field names are not part of identity.  Plain
// scalars keep ordinary equality.

bool LooksLikeStructJson(const Value& value) {
  return value.type == ValueType::kVarChar &&
         value.value.varchar_value.size() >= 2 &&
         value.value.varchar_value.front() == '{' &&
         value.value.varchar_value.back() == '}';
}

// Splits a top-level JSON object into ordered raw field-value texts (names
// discarded: structural equality is positional).
bool JsonObjectFieldTexts(std::string_view json,
                          std::vector<std::string_view>* values) {
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    return false;
  }
  size_t i = 1;
  const size_t end = json.size() - 1;
  while (i < end) {
    // Field name.
    while (i < end &&
           (std::isspace(static_cast<unsigned char>(json[i])) != 0)) {
      ++i;
    }
    if (i < end && json[i] == '"') {
      ++i;
      while (i < end) {
        if (json[i] == '\\') {
          i += 2;
          continue;
        }
        if (json[i] == '"') {
          break;
        }
        ++i;
      }
      ++i;
    } else {
      return false;
    }
    while (i < end &&
           (std::isspace(static_cast<unsigned char>(json[i])) != 0)) {
      ++i;
    }
    if (i >= end || json[i] != ':') {
      return false;
    }
    ++i;
    while (i < end &&
           (std::isspace(static_cast<unsigned char>(json[i])) != 0)) {
      ++i;
    }
    // Raw value through the matching close.
    const size_t value_begin = i;
    // A string-encoded nested struct/array may appear either as strict JSON
    // ("{\"a\":1}", escapes keep quotes balanced) or in the legacy raw form
    // ("{"a":1}", inner quotes unescaped).  Detect the raw form by a quote
    // immediately followed by an opening bracket and scan to the matching
    // close bracket, ignoring quote state inside.
    const bool raw_embedded = i + 1 < end && json[i] == '"' &&
                              (json[i + 1] == '{' || json[i + 1] == '[');
    int depth = 0;
    bool in_string = false;
    if (raw_embedded) {
      ++i;  // opening quote
      while (i < end) {
        const char c = json[i];
        if (c == '{' || c == '[') {
          ++depth;
        } else if (c == '}' || c == ']') {
          --depth;
          if (depth == 0) {
            ++i;
            break;
          }
        }
        ++i;
      }
      if (i < end && json[i] == '"') {
        ++i;  // closing quote
      }
      values->emplace_back(json.substr(value_begin, i - value_begin));
      if (i < end && json[i] == ',') {
        ++i;
      }
      continue;
    }
    while (i < end) {
      const char c = json[i];
      if (in_string) {
        if (c == '\\') {
          ++i;
        } else if (c == '"') {
          in_string = false;
        }
      } else if (c == '"') {
        in_string = true;
      } else if (c == '{' || c == '[') {
        ++depth;
      } else if (c == '}' || c == ']') {
        --depth;
      } else if (c == ',' && depth == 0) {
        break;
      }
      ++i;
    }
    values->emplace_back(json.substr(value_begin, i - value_begin));
    if (i < end && json[i] == ',') {
      ++i;
    }
  }
  return true;
}

// 1 = TRUE, 0 = FALSE, -1 = UNKNOWN.
int ThreeValuedEqualsImpl(const Value& left, const Value& right);

int CompareEncodedField(std::string_view raw_left, std::string_view raw_right) {
  auto trim = [](std::string_view s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) {
      ++b;
    }
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) {
      --e;
    }
    return s.substr(b, e - b);
  };
  const std::string_view l = trim(raw_left);
  const std::string_view r = trim(raw_right);
  // Deferred STRUCT construction represents a literal NULL as the quoted
  // text token `"null"` in its VARCHAR argument.
  if (l == "null" || r == "null" || l == "\"null\"") {
    if (l == "\"null\"" && r == "\"null\"") {
      return 1;
    }
    return -1;
  }
  if (l.front() == '{' && l.back() == '}' && r.front() == '{' &&
      r.back() == '}') {
    std::vector<std::string_view> lv;
    std::vector<std::string_view> rv;
    if (!JsonObjectFieldTexts(l, &lv) || !JsonObjectFieldTexts(r, &rv)) {
      return l == r ? 1 : 0;
    }
    if (lv.size() != rv.size()) {
      return 0;
    }
    bool saw_unknown = false;
    for (size_t i = 0; i < lv.size(); ++i) {
      const int field = CompareEncodedField(lv[i], rv[i]);
      if (field == 0) {
        return 0;
      }
      if (field < 0) {
        saw_unknown = true;
      }
    }
    return saw_unknown ? -1 : 1;
  }
  if (l.front() == '"' && l.back() == '"' && r.front() == '"' &&
      r.back() == '"') {
    // String values: compare contents.  Contents may themselves be encoded
    // structs (nested fields), so recurse after stripping the quotes.
    const std::string_view inner_l = l.substr(1, l.size() - 2);
    const std::string_view inner_r = r.substr(1, r.size() - 2);
    if (!inner_l.empty() && !inner_r.empty() && inner_l.front() == '{' &&
        inner_l.back() == '}' && inner_r.front() == '{' &&
        inner_r.back() == '}') {
      return CompareEncodedField(inner_l, inner_r);
    }
    return inner_l == inner_r ? 1 : 0;
  }
  if (l == r) {
    return 1;
  }
  // Numeric comparison so "1" vs "1.0" agrees with scalar semantics.
  try {
    const double ld = std::stod(std::string(l));
    const double rd = std::stod(std::string(r));
    return ld == rd ? 1 : 0;
  } catch (const std::exception&) {
    return 0;
  }
}

int ThreeValuedEqualsImpl(const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) {
    return -1;
  }
  if (left.type == ValueType::kArray && right.type == ValueType::kArray) {
    const auto& le = left.ArrayElements();
    const auto& re = right.ArrayElements();
    if (le.size() != re.size()) {
      return 0;
    }
    bool saw_unknown = false;
    for (size_t i = 0; i < le.size(); ++i) {
      const int element = ThreeValuedEqualsImpl(le[i], re[i]);
      if (element == 0) {
        return 0;
      }
      if (element < 0) {
        saw_unknown = true;
      }
    }
    return saw_unknown ? -1 : 1;
  }
  if (LooksLikeStructJson(left) && LooksLikeStructJson(right)) {
    std::vector<std::string_view> lv;
    std::vector<std::string_view> rv;
    if (!JsonObjectFieldTexts(left.value.varchar_value, &lv) ||
        !JsonObjectFieldTexts(right.value.varchar_value, &rv)) {
      return left.value.varchar_value == right.value.varchar_value ? 1 : 0;
    }
    if (lv.size() != rv.size()) {
      return 0;
    }
    bool saw_unknown = false;
    for (size_t i = 0; i < lv.size(); ++i) {
      const int field = CompareEncodedField(lv[i], rv[i]);
      if (field == 0) {
        return 0;
      }
      if (field < 0) {
        saw_unknown = true;
      }
    }
    return saw_unknown ? -1 : 1;
  }
  // Mixed types take the canonical `=` path: numeric promotion decides
  // INT64-vs-DOUBLE (1 IN (SELECT 1.0) is TRUE, mirroring `1 = 1.0`), and
  // genuinely incomparable operands error, which maps to UNKNOWN here —
  // never to a decisive FALSE.
  {
    StatusOr<Value> eq = TryBinary(BinaryOperation::kEquals, left, right);
    if (!eq.HasValue()) {
      return -1;
    }
    return Truthy(eq.Value()) ? 1 : 0;
  }
}

}  // namespace

// Three-valued equality entry point shared by IN membership and quantified
// comparisons.  Struct-shaped operands take the positional JSON path; plain
// strings keep ordinary (never JSON-parsed) equality.
int ThreeValuedEquals(const Value& left, const Value& right) {
  return ThreeValuedEqualsImpl(left, right);
}

StatusOr<Value> TryLookup(const ColumnName& name, const Scope& scope) {
  // The frontend can preserve a dotted reference entirely in the qualifier
  // (`outer_table.a.repeated_field`) instead of splitting it into
  // qualifier/name components.  Resolve the longest physical column prefix
  // first so nested correlated proto/STRUCT fields bind to the right scope.
  const bool has_nested_path = name.schema.find('.') != std::string::npos ||
                               name.name.find('.') != std::string::npos;
  if (has_nested_path) {
    std::string full_name = name.schema;
    if (!full_name.empty() && !name.name.empty()) {
      full_name += ".";
    }
    full_name += name.name;
    const std::vector<std::string> full_segments = SplitDottedName(full_name);
    if (full_segments.size() >= 2) {
      for (const Scope* current = &scope; current != nullptr;
           current = current->outer) {
        if (current->row == nullptr || current->schema == nullptr) {
          continue;
        }
        for (size_t column_index = 0;
             column_index < current->schema->ColumnCount(); ++column_index) {
          const ColumnName& candidate =
              current->schema->GetColumn(column_index).Name();
          std::vector<std::string> base_segments;
          if (!candidate.schema.empty()) {
            base_segments = SplitDottedName(candidate.schema);
          }
          for (std::string part : SplitDottedName(candidate.name)) {
            base_segments.push_back(std::move(part));
          }
          if (base_segments.empty() ||
              full_segments.size() <= base_segments.size() ||
              !std::equal(
                  base_segments.begin(), base_segments.end(),
                  full_segments.begin(),
                  [](const std::string& left, const std::string& right) {
                    return IdentifierEquals(left, right);
                  })) {
            continue;
          }
          return TryResolveFieldPath(
              (*current->row)[column_index],
              std::vector<std::string>(
                  full_segments.begin() +
                      static_cast<ptrdiff_t>(base_segments.size()),
                  full_segments.end()));
        }
      }
    }
  }
  for (const Scope* current = &scope; current != nullptr;
       current = current->outer) {
    if (current->row == nullptr || current->schema == nullptr) {
      continue;
    }
    // USING joins keep one merged column per declared name even though the
    // physical schema duplicates it: bare references coalesce every match
    // instead of raising ambiguity.  Qualified references still bind to a
    // single side.
    if (current->using_columns != nullptr && name.schema.empty() &&
        name.name.find('.') == std::string::npos) {
      std::vector<size_t> matches;
      for (size_t i = 0; i < current->schema->ColumnCount(); ++i) {
        if (IdentifierEquals(current->schema->GetColumn(i).Name().name,
                             name.name)) {
          matches.push_back(i);
        }
      }
      const bool merged =
          matches.size() > 1 &&
          std::any_of(current->using_columns->begin(),
                      current->using_columns->end(),
                      [&](const std::string& using_name) {
                        return IdentifierEquals(using_name, name.name);
                      });
      if (merged) {
        for (const size_t index : matches) {
          const Value& candidate = (*current->row)[index];
          if (!candidate.IsNull()) {
            return candidate;
          }
        }
        return Value();
      }
    }
    ASSIGN_OR_RETURN(int, offset,
                     (FindColumnCached(*current->schema, name, false,
                                       (current->lookup_cache == nullptr
                                            ? active_lookup_cache
                                            : current->lookup_cache))));

    if (offset >= 0) {
      return (*current->row)[static_cast<size_t>(offset)];
    }
  }
  // Degraded-metadata pass: only after no scope bound the reference exactly
  // may a schema without any qualified column capture it by bare name.
  // Resolving inner projected schemas (ORDER BY keys see bare output columns
  // like `id`) before the outer scope would otherwise bind `p2.id` to the
  // inner `id` column of a different tuple.
  for (const Scope* current = &scope; current != nullptr;
       current = current->outer) {
    if (current->row == nullptr || current->schema == nullptr) {
      continue;
    }
    ASSIGN_OR_RETURN(int, offset,
                     (FindColumnCached(*current->schema, name, true,
                                       (current->lookup_cache == nullptr
                                            ? active_lookup_cache
                                            : current->lookup_cache))));

    if (offset >= 0) {
      return (*current->row)[static_cast<size_t>(offset)];
    }
  }
  // A proto value table has one encoded message column.  In that shape the
  // qualified star denotes the value itself (it is later wrapped by SELECT
  // AS <proto>), rather than a physical column literally named "*".
  if (name.name == "*" && !name.schema.empty()) {
    for (const Scope* current = &scope; current != nullptr;
         current = current->outer) {
      if (current->row == nullptr || current->schema == nullptr ||
          current->schema->ColumnCount() != 1) {
        continue;
      }
      const ColumnName& column = current->schema->GetColumn(0).Name();
      if (IdentifierEquals(column.schema, name.schema)) {
        return (*current->row)[0];
      }
    }
  }
  // Fast path for a one-column proto value table.  This is intentionally
  // independent of whether the parser kept `p.x` in the qualifier or in the
  // dotted column name; both spellings occur in the resolved AST.
  for (const Scope* current = &scope; current != nullptr;
       current = current->outer) {
    if (current->row == nullptr || current->schema == nullptr ||
        current->schema->ColumnCount() != 1 ||
        (*current->row)[0].type != ValueType::kVarChar) {
      continue;
    }
    const ColumnName& physical = current->schema->GetColumn(0).Name();
    const std::string qualifier =
        name.schema.empty() ? std::string() : name.schema;
    std::string field = name.name;
    if (name.schema.empty()) {
      const size_t dot = field.find('.');
      if (dot == std::string::npos) {
        if (IdentifierEquals(field, physical.schema)) {
          return (*current->row)[0];
        }
        continue;
      }
      field = field.substr(dot + 1);
    } else if (!IdentifierEquals(qualifier, physical.schema)) {
      continue;
    }
    Value decoded;
    if (TryProtoTextGetField((*current->row)[0].value.varchar_value, field,
                             &decoded)) {
      return decoded;
    }
  }
  // Dotted references (`t.Info.str_value`, `s.a`, `sub.ca.a`) may address
  // nested fields of a STRUCT/PROTO value rather than a plain column.  Split
  // the full path into segments and, per scope level (innermost outward),
  // try every split point: the leading segments form a (possibly qualified)
  // column reference and the trailing segments traverse encoded field values.
  std::vector<std::string> segments =
      name.schema.empty() ? SplitDottedName(name.name) : [&] {
        std::vector<std::string> parts = SplitDottedName(name.schema);
        for (std::string& part : SplitDottedName(name.name)) {
          parts.push_back(std::move(part));
        }
        return parts;
      }();
  if (segments.size() >= 2) {
    const std::vector<std::string> fields(segments.begin() + 1, segments.end());
    for (const Scope* current = &scope; current != nullptr;
         current = current->outer) {
      if (current->row == nullptr || current->schema == nullptr) {
        continue;
      }
      if (name.schema.empty() && current->schema->ColumnCount() == 1 &&
          (*current->row)[0].type == ValueType::kVarChar) {
        Value field;
        if (TryProtoTextGetField((*current->row)[0].value.varchar_value,
                                 fields.front(), &field)) {
          Value current_value = std::move(field);
          for (size_t i = 1; i < fields.size(); ++i) {
            ASSIGN_OR_RETURN(
                Value, next_value,
                (TryResolveFieldPath(current_value,
                                     std::vector<std::string>{fields[i]})));
            current_value = std::move(next_value);
          }
          return current_value;
        }
      }
      // Some frontend paths preserve the entire dotted base in
      // ColumnName::name (for example `outer_table.a.repeated_field`) while
      // others split it between schema and name.  Match the actual column's
      // full path before treating the qualifier as a value-table alias.
      for (size_t column_index = 0;
           column_index < current->schema->ColumnCount(); ++column_index) {
        const ColumnName& candidate =
            current->schema->GetColumn(column_index).Name();
        std::vector<std::string> base_parts;
        if (!candidate.schema.empty()) {
          base_parts = SplitDottedName(candidate.schema);
        }
        for (std::string part : SplitDottedName(candidate.name)) {
          base_parts.push_back(std::move(part));
        }
        if (base_parts.empty() || segments.size() <= base_parts.size() ||
            !std::equal(base_parts.begin(), base_parts.end(), segments.begin(),
                        [](const std::string& left, const std::string& right) {
                          return IdentifierEquals(left, right);
                        })) {
          continue;
        }
        return TryResolveFieldPath(
            (*current->row)[column_index],
            std::vector<std::string>(
                segments.begin() + static_cast<ptrdiff_t>(base_parts.size()),
                segments.end()));
      }
      // First segment may be an alias whose whole row is the base value.
      if (name.schema.empty()) {
        bool any_match = false;
        size_t matched_index = 0;
        for (size_t i = 0; i < current->schema->ColumnCount(); ++i) {
          if (IdentifierEquals(current->schema->GetColumn(i).Name().schema,
                               segments.front())) {
            any_match = true;
            matched_index = i;
            break;
          }
        }
        if (any_match) {
          if (current->schema->ColumnCount() == 1) {
            return TryResolveFieldPath((*current->row)[matched_index], fields);
          }
          return TryResolveFieldPath(
              RowAsStructValue(*current->row, *current->schema), fields);
        }
      }
      // Value-table aliases expose the fields of their sole encoded proto
      // column as if they were columns of the relation (p.int32_val1).
      // There is no physical column named int32_val1 to find, so bind the
      // qualifier to the only column carrying that relation identity.
      ASSIGN_OR_RETURN(
          int, base_column_idx,
          (FindColumnCached(
              *current->schema, ColumnName(segments.front(), segments[1]), true,
              (current->lookup_cache == nullptr ? active_lookup_cache
                                                : current->lookup_cache))));
      const bool explicit_base_column =
          segments.size() >= 2 && base_column_idx >= 0;
      if (!name.schema.empty() && !explicit_base_column) {
        std::vector<size_t> owned;
        for (size_t i = 0; i < current->schema->ColumnCount(); ++i) {
          if (IdentifierEquals(current->schema->GetColumn(i).Name().schema,
                               segments.front())) {
            owned.push_back(i);
          }
        }
        if (owned.size() == 1) {
          const ValueType col_type =
              current->schema->GetColumn(owned.front()).Type();
          const Value& col_val = (*current->row)[owned.front()];
          if (col_type == ValueType::kVarChar ||
              (col_type == ValueType::kNull &&
               (col_val.IsNull() || col_val.type == ValueType::kVarChar))) {
            return TryResolveFieldPath(col_val, fields);
          }
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
        ASSIGN_OR_RETURN(int, offset,
                         (FindColumnCached(*current->schema, base, true,
                                           (current->lookup_cache == nullptr
                                                ? active_lookup_cache
                                                : current->lookup_cache))));

        if (offset >= 0) {
          const Value& base_value =
              (*current->row)[static_cast<size_t>(offset)];
          const ValueType col_type =
              current->schema->GetColumn(static_cast<size_t>(offset)).Type();
          const bool traversable_base =
              col_type == ValueType::kVarChar ||
              (col_type == ValueType::kNull &&
               (base_value.IsNull() || base_value.type == ValueType::kVarChar));
          // Only textual STRUCT/PROTO values carry traversable fields; a
          // scalar match here came from a qualifier/alias fallback, so keep
          // searching other scopes instead of failing the whole lookup.
          // NULL bases (empty/unset protos) resolve to NULL field values
          // rather than aborting the lookup.
          if (traversable_base || split + 1 == segments.size()) {
            return TryResolveFieldPath(
                base_value,
                std::vector<std::string>(
                    segments.begin() + static_cast<ptrdiff_t>(split + 1),
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
      // A grouped-emission OUTPUT scope is not a source: its bare names are
      // output columns, aliases and aggregates only, so the whole-row
      // fallback must not fabricate a STRUCT out of an unresolvable name
      // (a typo'd ORDER BY key used to "sort" by the output row itself).
      if (current->no_row_alias) {
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
        // Relational-path scans (INSERT ... SELECT and other routes outside
        // the QueryData rewriter) carry no per-column relation identity.
        // When the alias names none of this scope's columns either, it still
        // denotes the whole row as a struct.
        bool names_a_column = false;
        for (size_t i = 0; i < schema.ColumnCount(); ++i) {
          if (IdentifierEquals(schema.GetColumn(i).Name().name, name.name)) {
            names_a_column = true;
            break;
          }
        }
        if (!names_a_column && schema.ColumnCount() == 1) {
          const Value& candidate = (*current->row)[0];
          if (!candidate.IsNull() && candidate.type == ValueType::kVarChar) {
            Value field;
            if (TryProtoTextGetField(candidate.value.varchar_value, name.name,
                                     &field)) {
              return field;
            }
            // A one-column value table's alias denotes the value itself, not
            // a synthetic STRUCT keyed by the physical column name.
            if (candidate.value.varchar_value.empty() ||
                !candidate.value.varchar_value.starts_with("{")) {
              return candidate;
            }
          }
        }
        if (!names_a_column && schema.ColumnCount() > 0) {
          return RowAsStructValue(*current->row, schema);
        }
        continue;
      }
      if (owned.size() == 1 && schema.ColumnCount() == 1) {
        return (*current->row)[owned.front()];
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
  // Bare references may also address fields of STRUCT-typed columns:
  // `FROM UNNEST([STRUCT(1 AS y, 2.0 AS x)])` keeps one encoded struct
  // column per element, yet GoogleSQL exposes its fields without a
  // qualifier.  After every plain-column strategy failed, probe JSON-
  // encoded struct values for a matching top-level field (innermost scope
  // first; leftmost column wins within a scope).
  if (name.schema.empty() && name.name != "*" &&
      name.name.find('.') == std::string::npos) {
    for (const Scope* current = &scope; current != nullptr;
         current = current->outer) {
      if (current->row == nullptr || current->schema == nullptr) {
        continue;
      }
      for (size_t i = 0; i < current->schema->ColumnCount(); ++i) {
        const Value& candidate = (*current->row)[i];
        if (candidate.IsNull() || candidate.type != ValueType::kVarChar) {
          continue;
        }
        const std::string_view text(candidate.value.varchar_value);
        if (text.size() < 2 || text.front() != '{') {
          continue;
        }
        Value field;
        if (JsonExtractField(text, name.name, &field)) {
          return field;
        }
      }
    }
  }
  // A value table stores a proto message in one physical VARCHAR column.
  // Its top-level fields are nevertheless visible as ordinary bare names.
  if (name.schema.empty() && name.name != "*") {
    for (const Scope* current = &scope; current != nullptr;
         current = current->outer) {
      if (current->row == nullptr || current->schema == nullptr ||
          current->schema->ColumnCount() != 1) {
        continue;
      }
      const Value& candidate = (*current->row)[0];
      if (!candidate.IsNull() && candidate.type == ValueType::kVarChar) {
        Value field;
        if (TryProtoTextGetField(candidate.value.varchar_value, name.name,
                                 &field)) {
          return field;
        }
      }
    }
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "column " + name.ToString() + " not found");
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
    // A pattern '%' is a wildcard even when the value character is also '%':
    // checking it before literal equality preserves the backtrack point.
    if (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
      wildcard = pattern_pos++;
      retry = value_pos;
    } else if (pattern_pos < pattern.size() &&
               (pattern[pattern_pos] == '_' ||
                pattern[pattern_pos] == value[value_pos])) {
      ++value_pos;
      ++pattern_pos;
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

StatusOr<Value> TryBinary(BinaryOperation operation, const Value& left,
                          const Value& right) {
  // Canonical evaluation rules live in the AST evaluator (TryEvaluateBinary,
  // binary_expression.cpp): forwarding keeps SQL three-valued logic for
  // AND/OR/XOR, int64 overflow guards and mixed-type promotion identical
  // across the bytecode, AST and scan-filter paths (improvement3.md A6/S7).
  StatusOr<Value> result = TryEvaluateBinary(operation, left, right);
  if (!result.HasValue()) {
    // Message texts pinned by executor tests for this path; the semantics
    // above are already canonical.
    const std::string what = ToString(result.GetStatus());
    if (what == "LIKE requires strings") {
      return StatusError(StatusCode::kInvalidArgument,
                         "LIKE requires string operands");
    }
    if (what.find("Cannot do ") != std::string::npos) {
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported binary operation");
    }
  }
  return result;
}

namespace {

StatusOr<Value> Aggregate(const AggregateExpression& aggregate,
                          const AggregateResultMap& aggregates) {
  const auto result = aggregates.find(&aggregate);
  if (result == aggregates.end()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "aggregate was not prepared");
  }
  return result->second;
}

}  // namespace

// Recursive descent over the expression tree is the intended evaluation
// strategy.
StatusOr<Value> TryEvaluate(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.name == "*" && name.schema.empty()) {
        return Value(1);
      }
      StatusOr<Value> lookup = TryLookup(name, scope);
      if (lookup.HasValue()) {
        return lookup;
      }
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
      return lookup.GetStatus();
    }
    case TypeTag::kConstantValue:
      return expression->AsConstantValue().GetValue();
    case TypeTag::kBinaryExp: {
      const auto& value = expression->AsBinaryExpression();
      // Canonical short-circuit dispatch (BinaryExpression::Evaluate): only a
      // non-NULL left operand that already decides the result skips the right
      // child; NULL falls through to the three-valued TryBinary() rules.
      if (value.Op() == BinaryOperation::kAnd ||
          value.Op() == BinaryOperation::kOr) {
        ASSIGN_OR_RETURN(
            Value, left,
            (TryEvaluate(value.Left(), scope, aggregates, context, ctes)));

        if (!left.IsNull() &&
            left.Truthy() != (value.Op() == BinaryOperation::kAnd)) {
          return Value(value.Op() == BinaryOperation::kOr);
        }
        ASSIGN_OR_RETURN(
            Value, rhs,
            (TryEvaluate(value.Right(), scope, aggregates, context, ctes)));
        return TryBinary(value.Op(), left, rhs);
      }
      ASSIGN_OR_RETURN(
          Value, lhs,
          (TryEvaluate(value.Left(), scope, aggregates, context, ctes)));
      ASSIGN_OR_RETURN(
          Value, rhs,
          (TryEvaluate(value.Right(), scope, aggregates, context, ctes)));
      return TryBinary(value.Op(), lhs, rhs);
    }
    case TypeTag::kUnaryExp: {
      const auto& value = expression->AsUnaryExpression();
      // Canonical semantics via the AST evaluator: NOT NULL is NULL (three-
      // valued), IS [NOT] NULL never propagates NULL, minus guards overflow.
      ASSIGN_OR_RETURN(
          Value, child,
          (TryEvaluate(value.Child(), scope, aggregates, context, ctes)));
      return TryEvaluateUnary(value.Op(), child);
    }
    case TypeTag::kAggregateExp: {
      // Subqueries nested inside grouped queries have no explicit aggregate
      // map of their own; they inherit the enclosing group's values through
      // the scope chain so correlated references (UNNEST(agg_alias), ...)
      // resolve.
      const AggregateResultMap* resolved = aggregates;
      if (resolved == nullptr) {
        for (const Scope* s = &scope; s != nullptr && resolved == nullptr;
             s = s->outer) {
          resolved = s->aggregates;
        }
      }
      if (resolved == nullptr) {
        return StatusError(StatusCode::kInvalidArgument,
                           "aggregate outside grouping");
      }
      return Aggregate(expression->AsAggregateExpression(), *resolved);
    }
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        ASSIGN_OR_RETURN(
            Value, when_value,
            (TryEvaluate(condition, scope, aggregates, context, ctes)));
        if (Truthy(when_value)) {
          return TryEvaluate(result, scope, aggregates, context, ctes);
        }
      }
      return value.else_clause_ ? TryEvaluate(value.else_clause_, scope,
                                              aggregates, context, ctes)
                                : Value();
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      // Canonical three-valued membership (InExpression::Evaluate): a match
      // decides TRUE; otherwise any NULL (test value or list item) yields
      // UNKNOWN instead of FALSE.
      ASSIGN_OR_RETURN(
          Value, test,
          (TryEvaluate(value.child_, scope, aggregates, context, ctes)));

      bool found = false;
      bool saw_null = test.IsNull();
      for (const Expression& item : value.list_) {
        ASSIGN_OR_RETURN(Value, candidate,
                         (TryEvaluate(item, scope, aggregates, context, ctes)));

        saw_null = saw_null || candidate.IsNull();
        if (!found && !test.IsNull() && !candidate.IsNull()) {
          // The AST reference (InExpression::TryMembership -> TryMatches)
          // PROPAGATES a failed comparison (e.g. STRING vs INT64); swallowing
          // it here diverged from the ground truth (fuzz-found: `"" IN
          // (col_i, col_i)` threw in the AST but evaluated to FALSE here).
          ASSIGN_OR_RETURN(
              Value, eq,
              (TryBinary(BinaryOperation::kEquals, test, candidate)));
          if (eq.IsNull()) {
            saw_null = true;
          } else if (eq.Truthy()) {
            found = true;
          }
        }
        if (found) {
          // A match decides TRUE; remaining items cannot change the result
          // and a throwing item (e.g. a type-mismatched comparison) must not
          // fail an already-decided predicate (mirrors the subquery IN path).
          break;
        }
      }
      if (found) {
        return Value(true);
      }
      return saw_null ? Value() : Value(false);
    }
    case TypeTag::kFunctionCallExp:
      return TryEvaluateFunction(expression->AsFunctionCallExpression(), scope,
                                 aggregates, context, ctes);
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Value> elements;
      elements.reserve(array.Elements().size());
      std::string inferred_type = array.ElementSqlType();
      for (const Expression& element : array.Elements()) {
        ASSIGN_OR_RETURN(
            Value, val,
            (TryEvaluate(element, scope, aggregates, context, ctes)));
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
      ASSIGN_OR_RETURN(
          Value, val,
          (TryEvaluate(cast.Child(), scope, aggregates, context, ctes)));

      return CastExpressionExp(ConstantValueExp(val), cast.TargetTypeName(),
                               cast.ReturnNullOnError())
          ->TryEvaluate(Row(), Schema());
    }

    case TypeTag::kQueryExp: {
      const auto& value = expression->AsQueryExpression();
      // The runtime caches below key on the subquery statement's address;
      // retaining the owning shared_ptr keeps that address unique for as
      // long as the entry can be probed (statements otherwise die with
      // their prepared executor and the allocator would recycle them).
      if (context.execution_runtime() != nullptr) {
        auto& retained = context.execution_runtime()->retained_statements;
        // Evaluated once per outer row, a statement repeats in runs; the
        // retention push (a shared_ptr refcount + append) is skipped for
        // anything already among the last few retained entries.
        bool already_retained = false;
        for (size_t k = retained.size() > 8 ? retained.size() - 8 : 0;
             k < retained.size(); ++k) {
          if (retained[k] == value.Query()) {
            already_retained = true;
            break;
          }
        }
        if (!already_retained) {
          retained.push_back(value.Query());
        }
      }
      // Scalar uncorrelated subqueries re-enter this evaluator once per
      // outer row (TPC-H Q22: 61128 probes of two AVG subqueries); the
      // projected value is invariant across those probes, so a node-keyed
      // memo answers repeats with one map hit. Same liveness contract as
      // the membership cache: retained_statements pins the node address.
      if (context.execution_runtime() != nullptr && !value.Exists() &&
          value.Test() == nullptr && !value.ArrayResult()) {
        auto& scalar_memo =
            context.execution_runtime()->uncorrelated_scalar_by_node;
        if (const auto memo_hit = scalar_memo.find(expression.get());
            memo_hit != scalar_memo.end()) {
          // A memo hit served the evaluation from a cached result; keep the
          // uncorrelated_cache_hits telemetry (and the EXPLAIN line derived
          // from it) meaningful.
          ++context.execution_runtime()->uncorrelated_cache_hits;
          return memo_hit->second;
        }
      }
      // Subqueries carrying their own WITH clauses must run through
      // ExecuteQuery, which materializes the local CTEs first.
      const bool has_own_ctes = !value.Query()->WithQueries().empty();
      const bool want_scalar_only =
          !value.Exists() && !value.Test() && !value.ArrayResult();
      std::optional<bool> exists_answer;
      std::optional<Value> scalar_answer;
      std::optional<Relation> indexed =
          has_own_ctes ? std::nullopt
                       : ExecuteCorrelatedSingleSource(
                             context, *value.Query(), scope, ctes,
                             value.Exists(), &exists_answer,
                             want_scalar_only ? &scalar_answer : nullptr);
      if (exists_answer) {
        return Value(value.Negated() ? !*exists_answer : *exists_answer);
      }
      if (scalar_answer.has_value()) {
        return std::move(*scalar_answer);
      }
      std::optional<Relation> executed;
      const Relation* relation = indexed ? &*indexed : nullptr;
      bool uncorrelated = false;
      RelationPtr correlated_apply_result;
      std::string correlated_apply_key;
      if (relation == nullptr && !has_own_ctes) {
        relation = ExecuteCachedUncorrelated(context, *value.Query(), ctes,
                                             value.Exists());
        uncorrelated = relation != nullptr;
      }
      if (relation == nullptr) {
        const bool apply_cacheable =
            scope.row != nullptr && context.execution_runtime() != nullptr &&
            !StatementContainsVolatileFunctionCached(
                context.execution_runtime(), *value.Query());
        if (apply_cacheable) {
          correlated_apply_key = CorrelatedApplyCacheKey(*value.Query(), &scope,
                                                         ctes, value.Exists());
          const auto cached_apply =
              context.execution_runtime()->correlated_apply_results.find(
                  correlated_apply_key);
          if (cached_apply !=
              context.execution_runtime()->correlated_apply_results.end()) {
            correlated_apply_result = cached_apply->second;
            relation = correlated_apply_result.get();
            ++context.execution_runtime()->correlated_result_cache_hits;
          }
        }
      }
      if (relation == nullptr) {
        // The generic path also gets the EXISTS-only normalization used by
        // the indexed and uncorrelated fast paths. ORDER BY cannot affect an
        // EXISTS boolean; cap only when LIMIT 0 is not authoritative.
        std::shared_ptr<SelectStatement> exists_query;
        const SelectStatement* execution_query = value.Query().get();
        if (value.Exists()) {
          const bool needs_cap =
              !value.Query()->HasLimit() || value.Query()->Limit() > 1;
          if (needs_cap || !value.Query()->OrderBy().empty()) {
            exists_query = std::make_shared<SelectStatement>(*value.Query());
            exists_query->SetOrderBy({});
            if (needs_cap) {
              exists_query->SetLimit(1);
            }
            execution_query = exists_query.get();
          }
        }
        StatusOr<Relation> executed_query =
            ExecuteQuery(context, *execution_query, &scope, ctes);
        if (!executed_query.HasValue()) {
          return executed_query.GetStatus();
        }
        executed = executed_query.MoveValue();
        relation = &*executed;
        if (!correlated_apply_key.empty()) {
          correlated_apply_result =
              std::make_shared<Relation>(std::move(*executed));
          context.execution_runtime()->correlated_apply_results.emplace(
              correlated_apply_key, correlated_apply_result);
          relation = correlated_apply_result.get();
        }
      }
      auto& row_source = const_cast<Relation&>(*relation);
      bool as_struct = value.Query()->AsStruct();
      if (!as_struct && value.Test() && value.Mode() == QuantifierMode::kIn &&
          relation->schema.ColumnCount() != 1) {
        // `(a, b) IN (SELECT x, y ...)` probes with an anonymous row value
        // (struct JSON) against each result row taken positionally.  A
        // scalar probe (including a decoded ARRAY) against a multi-column
        // result stays rejected: GoogleSQL requires the single output
        // column there.
        ASSIGN_OR_RETURN(
            Value, probe,
            TryEvaluate(value.Test(), scope, aggregates, context, ctes));
        if (LooksLikeStructJson(probe)) {
          // Positional fN keys line both sides up under the field-count
          // check in ThreeValuedEqualsImpl; declared names would hide a
          // width mismatch behind string inequality.
          as_struct = true;
        } else {
          return StatusError(
              StatusCode::kInvalidArgument,
              "Subquery of type IN must have only one output column");
        }
      }
      // Declared select-list names feed struct field keys; without them
      // SELECT AS STRUCT rows would carry positional fN keys.
      const Schema& subquery_schema = relation->schema;
      if (value.Exists()) {
        const bool exists = relation->TotalRows() > 0;
        return Value(value.Negated() ? !exists : exists);
      }
      if (value.Test()) {
        ASSIGN_OR_RETURN(
            Value, test,
            (TryEvaluate(value.Test(), scope, aggregates, context, ctes)));

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
        // True when the membership hash produced a final verdict (hit, or
        // miss with the build-time NULL flag); blocks the per-miss rescan.
        bool hash_decided = false;
        const bool structural_members =
            LooksLikeStructJson(test) || test.type == ValueType::kArray;
        // Hash membership is exact only when the probe sits in the build's
        // comparison domain (same type tag, same unsigned tag); a mixed
        // domain must take the rescan path where `=` promotion decides.
        auto same_domain = [](const Value& probe, const Value& stored) {
          return probe.type == stored.type &&
                 probe.IsUnsigned() == stored.IsUnsigned();
        };
        if (relation->TotalRows() == 0) {
          // An empty candidate set is decisive: `x IN ()` is FALSE and
          // `x NOT IN ()` is TRUE even when x itself is NULL (no comparison
          // exists that could turn UNKNOWN).
          saw_null = false;
          found = false;
        } else if (uncorrelated && context.execution_runtime() != nullptr &&
                   !structural_members) {
          // Probe-side fast path: the membership build is keyed on the
          // kQueryExp node pointer, so a repeat probe skips the fingerprint
          // string rebuild entirely.  The node's statement outlives the
          // runtime (retained above and owned by the executor).
          const void* membership_node = expression.get();
          auto [cached, inserted] =
              context.execution_runtime()
                  ->uncorrelated_membership_by_node.try_emplace(
                      membership_node);
          if (inserted) {
            cached->second.values.reserve(relation->TotalRows());
            Status collation_error = Status::kSuccess;
            row_source.ForEachRow([&](const Row& row) {
              if (collation_error != Status::kSuccess) {
                return;
              }
              const Value projected =
                  ProjectSubqueryRow(row, as_struct, &subquery_schema);
              if (collation_conflict(projected)) {
                collation_error =
                    StatusError(StatusCode::kInvalidArgument,
                                "Collation conflict between the IN "
                                "operands");
                return;
              }
              if (projected.IsNull()) {
                cached->second.contains_null = true;
              } else {
                if (!cached->second.has_domain) {
                  cached->second.has_domain = true;
                  cached->second.domain_sample = projected;
                } else if (!same_domain(projected,
                                        cached->second.domain_sample)) {
                  cached->second.uniform_domain = false;
                }
                cached->second.values.insert(projected);
              }
            });
            if (collation_error != Status::kSuccess) {
              return collation_error;
            }
            if (cached->second.contains_null && value.Negated()) {
              context.execution_runtime()->null_aware_anti_build_contains_null =
                  true;
            }
            ++context.execution_runtime()->uncorrelated_hash_builds;
          }
          // Mixed numeric domains (INT64 test over a DOUBLE build and the
          // reverse) are legal SQL whose equality promotes to DOUBLE; the
          // type-strict hash would wrongly answer FALSE (or UNSIGNED-vs-
          // SIGNED would answer from raw bit patterns).  A differing
          // collation would likewise let `=` semantics (case folding)
          // disagree with the type-strict hash.  Those probes defer to the
          // canonical rescan path below.
          // When the hash IS exact, its verdict is final: a miss over a
          // null-containing build is UNKNOWN (contains_null was recorded at
          // build time) and a miss over a NULL-free build is FALSE.  Neither
          // may trigger the rescan -- Q22-sized builds make a per-miss rescan
          // quadratic (filter_ms 0.8s -> 246s at SF=1).
          const bool hash_is_exact =
              cached->second.uniform_domain &&
              (test.IsNull() ||
               (cached->second.has_domain &&
                same_domain(test, cached->second.domain_sample) &&
                test.Collation() == cached->second.domain_sample.Collation()));
          if (hash_is_exact) {
            ++context.execution_runtime()->uncorrelated_hash_probes;
            found = !test.IsNull() && cached->second.values.contains(test);
            if (!found && !test.IsNull()) {
              // The hash build recorded NULL keys once; a miss over a
              // null-containing build is UNKNOWN without a rescan.
              saw_null = cached->second.contains_null;
            }
            hash_decided = true;
          }
        }
        if (!hash_decided && relation->TotalRows() != 0) {
          Status collation_error = Status::kSuccess;
          row_source.ForEachRow([&](const Row& row) {
            if (found || row.values_.empty() ||
                collation_error != Status::kSuccess) {
              return;
            }
            const Value projected =
                ProjectSubqueryRow(row, as_struct, &subquery_schema);
            if (collation_conflict(projected)) {
              collation_error = StatusError(StatusCode::kInvalidArgument,
                                            "Collation conflict between the IN "
                                            "operands");
              return;
            }
            const int tri = ThreeValuedEquals(test, projected);
            if (tri < 0) {
              saw_null = true;
            } else if (tri == 1) {
              found = true;
            }
          });
        }
        Value membership =
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
          if (row.values_.empty()) {
            return;
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
          if (element_type.empty()) {
            element_type = "INT64";
          }
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
        return StatusError(StatusCode::kInvalidArgument,
                           "Scalar subquery produced more than one row");
      }
      Value scalar =
          (!first || first->values_.empty())
              ? Value()  // Empty subquery: scalar NULL.
              : ProjectSubqueryRow(*first, as_struct, &subquery_schema);
      // Memoize only uncorrelated results: the correlated-index path yields
      // row-dependent values. See the node-keyed memo lookup above.
      if (uncorrelated && context.execution_runtime() != nullptr) {
        context.execution_runtime()->uncorrelated_scalar_by_node.emplace(
            expression.get(), scalar);
      }
      return scalar;
    }
    case TypeTag::kIntervalExp: {
      // Relational execution exposes interval literals as their logical
      // amount/unit value.  The standalone AST evaluator intentionally keeps
      // the encoded wire representation for compatibility with its direct
      // expression tests.
      const auto& interval = expression->AsIntervalExpression();
      return Value(std::to_string(interval.Amount()) + " " +
                   std::string(interval.Unit()));
    }
    default:
      return StatusError(StatusCode::kInvalidArgument,
                         "unsupported expression type");
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
    Column qualified(ColumnName(qualifier, col_name), column.Type(),
                     column.GetConstraint());
    qualified.SetUnsigned(column.IsUnsigned());
    columns.push_back(std::move(qualified));
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
  // QUALIFY predicates evaluate above the grouping/window passes but name
  // base columns: missing them pruned scan inputs down to a schema those
  // passes cannot resolve (aggregate children bound to the wrong column).
  CollectExpressionColumns(statement.Qualify(), columns);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (term.expression && term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty() &&
          std::ranges::any_of(statement.SelectList(),
                              [&](const NamedExpression& proj) {
                                return !proj.name.empty() &&
                                       IdentifierEquals(proj.name, cn.name);
                              })) {
        continue;
      }
    }
    CollectExpressionColumns(term.expression, columns);
  }
  for (const SelectSource& source : statement.Sources()) {
    CollectExpressionColumns(source.join_condition, columns);
    // UNNEST arguments may reference sibling sources of the same FROM clause
    // or outer scopes; their columns must survive projection pruning.
    CollectExpressionColumns(source.unnest, columns);
    if (source.sample) {
      CollectExpressionColumns(source.sample->size, columns);
      if (source.sample->repeatable_seed) {
        CollectExpressionColumns(*source.sample->repeatable_seed, columns);
      }
      for (const Expression& expression : source.sample->partition_by) {
        CollectExpressionColumns(expression, columns);
      }
    }
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
          // A bare reference naming this relation's qualifier (`s` for an
          // aliased scan) is a whole-row struct read: keep every column.
          if (name.schema.empty() &&
              IdentifierEquals(name.name, candidate.schema)) {
            return true;
          }
          // GoogleSQL identifiers are case-insensitive; references may also
          // qualify this relation by table name or any alias.  Over-
          // projecting a scan is harmless; dropping a column some scope
          // reference needs breaks evaluation.
          return IdentifierEquals(name.name, candidate.name) &&
                 (name.schema.empty() ||
                  IdentifierEquals(name.schema, candidate.schema));
        });
    // Proto value tables expose fields of their single encoded `$expr0`
    // column as virtual columns.  A qualified/bare field reference must keep
    // that payload in the scan even though no physical column has that name.
    const bool virtual_value_field =
        schema.ColumnCount() == 1 &&
        (std::ranges::any_of(referenced, [&](const auto& name) {
          if (!name.schema.empty() &&
              !IdentifierEquals(name.schema, candidate.schema)) {
            return false;
          }
          return !IdentifierEquals(name.name, candidate.name);
        }));
    if (needed || virtual_value_field) {
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
