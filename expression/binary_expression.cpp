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

#include "binary_expression.hpp"

#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> BinaryExpression::TouchedColumns() const {
  std::unordered_set<ColumnName> result = left_->TouchedColumns();
  result.merge(right_->TouchedColumns());
  return result;
}
namespace {

bool IsStructJson(std::string_view s) {
  return s.size() >= 2 && s.front() == '{' && s.back() == '}';
}

std::vector<std::string> ExtractStructValues(std::string_view json) {
  std::vector<std::string> values;
  if (!IsStructJson(json)) {
    return values;
  }
  std::string_view inner = json.substr(1, json.size() - 2);
  int depth = 0;
  bool in_string = false;
  size_t start = 0;
  std::vector<std::string_view> parts;
  for (size_t i = 0; i < inner.size(); ++i) {
    char c = inner[i];
    if (in_string) {
      if (c == '\\' && i + 1 < inner.size()) {
        ++i;
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
    if (c == '{' || c == '[' || c == '(') {
      ++depth;
      continue;
    }
    if (c == '}' || c == ']' || c == ')') {
      if (depth > 0) --depth;
      continue;
    }
    if (c == ',' && depth == 0) {
      parts.push_back(inner.substr(start, i - start));
      start = i + 1;
    }
  }
  if (start < inner.size()) {
    parts.push_back(inner.substr(start));
  }
  for (auto p : parts) {
    while (!p.empty() && std::isspace(static_cast<unsigned char>(p.front()))) {
      p.remove_prefix(1);
    }
    while (!p.empty() && std::isspace(static_cast<unsigned char>(p.back()))) {
      p.remove_suffix(1);
    }
    size_t colon = p.find(':');
    if (colon != std::string_view::npos) {
      p = p.substr(colon + 1);
      while (!p.empty() &&
             std::isspace(static_cast<unsigned char>(p.front()))) {
        p.remove_prefix(1);
      }
    }
    values.emplace_back(p);
  }
  return values;
}

// Row-comparison semantics for STRUCT equality (ANSI row value comparison):
// a pair of non-NULL, differing fields decides FALSE outright; any field
// pair involving NULL yields UNKNOWN unless another pair decided FALSE.
Value StructJsonCompare(std::string_view lhs, std::string_view rhs) {
  auto v1 = ExtractStructValues(lhs);
  auto v2 = ExtractStructValues(rhs);
  if (v1.empty() && v2.empty()) {
    return Value(true);
  }
  if (v1.empty() || v1.size() != v2.size()) {
    return Value(false);
  }
  bool saw_null = false;
  for (size_t i = 0; i < v1.size(); ++i) {
    if (v1[i] == "null" || v2[i] == "null") {
      saw_null = true;
      continue;
    }
    if (v1[i] != v2[i]) {
      return Value(false);
    }
  }
  return saw_null ? Value() : Value(true);
}

bool IsComparisonOp(BinaryOperation op) {
  switch (op) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kNotEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
    case BinaryOperation::kLike:
    case BinaryOperation::kNotLike:
    case BinaryOperation::kIsDistinctFrom:
    case BinaryOperation::kIsNotDistinctFrom:
      return true;
    default:
      return false;
  }
}

}  // namespace

std::string FoldCase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    const auto uc = static_cast<unsigned char>(c);
    out.push_back(uc >= 'A' && uc <= 'Z' ? static_cast<char>(uc - 'A' + 'a')
                                         : c);
  }
  return out;
}

namespace {

bool Like(std::string_view value, std::string_view pattern) {
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

}  // namespace

Value EvaluateBinary(BinaryOperation op, const Value& left,
                     const Value& right) {
  // These predicates are two-valued even when either operand is NULL.  Keep
  // them ahead of the ordinary comparison NULL propagation below.
  if (op == BinaryOperation::kIsDistinctFrom ||
      op == BinaryOperation::kIsNotDistinctFrom) {
    const bool equal = left.IsNull() || right.IsNull()
                           ? left.IsNull() && right.IsNull()
                           : left == right;
    return Value(op == BinaryOperation::kIsNotDistinctFrom ? equal : !equal);
  }
  if (op == BinaryOperation::kAnd) {
    if ((!left.IsNull() && !left.Truthy()) ||
        (!right.IsNull() && !right.Truthy())) {
      return Value(false);
    }
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    return Value(true);
  }
  if (op == BinaryOperation::kOr) {
    if ((!left.IsNull() && left.Truthy()) ||
        (!right.IsNull() && right.Truthy())) {
      return Value(true);
    }
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    return Value(false);
  }
  if (op == BinaryOperation::kXor) {
    if (left.IsNull() || right.IsNull()) {
      return {};
    }
    return Value(left.Truthy() != right.Truthy());
  }
  if (left.IsNull() || right.IsNull()) {
    return {};
  }
  // IN lists are typed by the left-hand expression in GoogleSQL.  The AST
  // represents a bare DATE literal as STRING, so coerce it when it is
  // compared with a DATE column value.
  if (IsComparisonOp(op)) {
    auto as_date = [](const Value& value) -> std::optional<int64_t> {
      if (value.type == ValueType::kDate) {
        return value.value.int_value;
      }
      if (value.type != ValueType::kVarChar) {
        return std::nullopt;
      }
      const std::string text(value.value.varchar_value);
      if (text.size() != 10 || text[4] != '-' || text[7] != '-') {
        return std::nullopt;
      }
      try {
        return ParseDateDays(text);
      } catch (...) {
        return std::nullopt;
      }
    };
    if ((left.type == ValueType::kDate || right.type == ValueType::kDate) &&
        (left.type == ValueType::kDate || right.type == ValueType::kDate)) {
      const auto lhs = as_date(left);
      const auto rhs = as_date(right);
      if (lhs && rhs) {
        const int64_t a = *lhs;
        const int64_t b = *rhs;
        switch (op) {
          case BinaryOperation::kEquals:
            return Value(a == b);
          case BinaryOperation::kNotEquals:
            return Value(a != b);
          case BinaryOperation::kLessThan:
            return Value(a < b);
          case BinaryOperation::kLessThanEquals:
            return Value(a <= b);
          case BinaryOperation::kGreaterThan:
            return Value(a > b);
          case BinaryOperation::kGreaterThanEquals:
            return Value(a >= b);
          default:
            break;
        }
      }
    }
    auto timestamp_seconds =
        [](std::string_view text) -> std::optional<time_t> {
      if (text.size() < 19 || text[4] != '-' || text[7] != '-' ||
          (text[10] != ' ' && text[10] != 'T')) {
        return std::nullopt;
      }
      int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
      if (sscanf(std::string(text.substr(0, 19)).c_str(), "%d-%d-%d %d:%d:%d",
                 &year, &month, &day, &hour, &minute, &second) != 6) {
        return std::nullopt;
      }
      std::tm tm{};
      tm.tm_year = year - 1900;
      tm.tm_mon = month - 1;
      tm.tm_mday = day;
      tm.tm_hour = hour;
      tm.tm_min = minute;
      tm.tm_sec = second;
      time_t epoch = timegm(&tm);
      const size_t zone = text.find_first_of("+-Zz", 19);
      if (zone == std::string_view::npos) {
        // The repository's default session zone is UTC-8 (the same default
        // used by CAST(TIMESTAMP)), hence local wall time is eight hours
        // ahead when represented in UTC.
        epoch += 8LL * 60LL * 60LL;
      }
      return epoch;
    };
    if (left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
      const std::string_view lhs(left.value.varchar_value);
      const std::string_view rhs(right.value.varchar_value);
      const auto looks_like_timestamp = [](std::string_view text) {
        return text.size() >= 19 && text[4] == '-' && text[7] == '-' &&
               (text[10] == ' ' || text[10] == 'T') && text[13] == ':' &&
               text[16] == ':';
      };
      const bool lhs_timestamp = looks_like_timestamp(lhs);
      const bool rhs_timestamp = looks_like_timestamp(rhs);
      if (lhs_timestamp && rhs_timestamp) {
        const auto a = timestamp_seconds(lhs);
        const auto b = timestamp_seconds(rhs);
        if (a && b) {
          // A timestamp-typed value is rendered in UTC, while a bare string
          // in a comparison is parsed in the default UTC-8 session zone.
          // Keep the civil-time fallback for values crossing a set-operation
          // boundary where the type tag is carried by the +00 suffix.
          const auto has_zone = [](std::string_view text) {
            return text.find_first_of("+-Zz", 19) != std::string_view::npos;
          };
          const auto fractional_digits = [](std::string_view text) {
            const size_t dot = text.find('.', 19);
            if (dot == std::string_view::npos) {
              return std::string_view{};
            }
            size_t end = dot + 1;
            while (end < text.size() &&
                   std::isdigit(static_cast<unsigned char>(text[end])) != 0) {
              ++end;
            }
            return text.substr(dot + 1, end - dot - 1);
          };
          const std::string_view lhs_fraction = fractional_digits(lhs);
          const std::string_view rhs_fraction = fractional_digits(rhs);
          const bool fractional_equal = lhs_fraction == rhs_fraction;
          const bool equivalent_default_zone =
              (has_zone(lhs) != has_zone(rhs)) &&
              std::llabs(*a - *b) == 8LL * 60LL * 60LL;
          const bool equal =
              (*a == *b && fractional_equal) || equivalent_default_zone;
          switch (op) {
            case BinaryOperation::kEquals:
              return Value(equal);
            case BinaryOperation::kNotEquals:
              return Value(!equal);
            case BinaryOperation::kLessThan:
              return Value(*a < *b);
            case BinaryOperation::kLessThanEquals:
              return Value(*a <= *b);
            case BinaryOperation::kGreaterThan:
              return Value(*a > *b);
            case BinaryOperation::kGreaterThanEquals:
              return Value(*a >= *b);
            default:
              break;
          }
        }
      }
    }
  }
  // Bit shifts operate on the two's-complement representation with logical
  // (unsigned) semantics, matching GoogleSQL: shift amounts >= 64 yield 0 and
  // negative amounts raise OUT_OF_RANGE.
  if (op == BinaryOperation::kShiftLeft || op == BinaryOperation::kShiftRight) {
    if (left.type != ValueType::kInt64 || right.type != ValueType::kInt64) {
      throw std::runtime_error("bitwise shift requires integer operands");
    }
    const int64_t amount = right.value.int_value;
    if (amount < 0) {
      throw std::runtime_error("Bitwise shift by negative offset.");
    }
    if (amount >= 64) {
      return Value(static_cast<int64_t>(0));
    }
    const uint64_t bits = static_cast<uint64_t>(left.value.int_value);
    const uint64_t shifted =
        op == BinaryOperation::kShiftLeft ? bits << amount : bits >> amount;
    return Value(static_cast<int64_t>(shifted));
  }
  // Collation-aware normalization: when either operand carries a
  // case-insensitive collator, both sides fold to lowercase.  GoogleSQL
  // resolves an explicit COLLATE on one side for the whole comparison.
  Value folded_left = left;
  Value folded_right = right;
  if (IsComparisonOp(op) && left.type == ValueType::kVarChar &&
      right.type == ValueType::kVarChar &&
      (left.IsCaseInsensitive() || right.IsCaseInsensitive())) {
    // Tags ride along so downstream LIKE validation still sees the collator.
    folded_left = Value(FoldCase(left.value.varchar_value))
                      .WithCollation(left.Collation());
    folded_right = Value(FoldCase(right.value.varchar_value))
                       .WithCollation(right.Collation());
  }
  if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
    if (folded_left.type != ValueType::kVarChar ||
        folded_right.type != ValueType::kVarChar) {
      throw std::runtime_error("LIKE requires string operands");
    }
    const std::string_view pattern = folded_right.value.varchar_value;
    if ((folded_left.IsCaseInsensitive() || folded_right.IsCaseInsensitive()) &&
        pattern.find('_') != std::string_view::npos) {
      throw std::runtime_error(
          "LIKE pattern has '_' which is not allowed when its operands have "
          "collation: " +
          std::string(pattern));
    }
    const bool matched = Like(folded_left.value.varchar_value, pattern);
    return Value(op == BinaryOperation::kLike ? matched : !matched);
  }
  const bool numeric =
      (left.type == ValueType::kInt64 || left.type == ValueType::kDouble) &&
      (right.type == ValueType::kInt64 || right.type == ValueType::kDouble);
  if (numeric && left.type == ValueType::kInt64 &&
      right.type == ValueType::kInt64 &&
      (left.IsUnsigned() || right.IsUnsigned())) {
    const bool mixed_signedness = left.IsUnsigned() != right.IsUnsigned();
    const uint64_t lhs_unsigned = static_cast<uint64_t>(left.value.int_value);
    const uint64_t rhs_unsigned = static_cast<uint64_t>(right.value.int_value);
    const bool lhs_negative = !left.IsUnsigned() && left.value.int_value < 0;
    const bool rhs_negative = !right.IsUnsigned() && right.value.int_value < 0;
    auto compare = [&]() {
      if (!mixed_signedness) {
        return lhs_unsigned < rhs_unsigned   ? -1
               : lhs_unsigned > rhs_unsigned ? 1
                                             : 0;
      }
      if (lhs_negative || rhs_negative) {
        return lhs_negative == rhs_negative
                   ? (lhs_negative
                          ? (left.value.int_value < right.value.int_value   ? -1
                             : left.value.int_value > right.value.int_value ? 1
                                                                            : 0)
                          : (right.value.int_value < left.value.int_value ? 1
                             : right.value.int_value > left.value.int_value
                                 ? -1
                                 : 0))
                   : (lhs_negative ? -1 : 1);
      }
      return lhs_unsigned < rhs_unsigned   ? -1
             : lhs_unsigned > rhs_unsigned ? 1
                                           : 0;
    };
    const int cmp = compare();
    switch (op) {
      case BinaryOperation::kEquals:
        return Value(cmp == 0);
      case BinaryOperation::kNotEquals:
        return Value(cmp != 0);
      case BinaryOperation::kLessThan:
        return Value(cmp < 0);
      case BinaryOperation::kLessThanEquals:
        return Value(cmp <= 0);
      case BinaryOperation::kGreaterThan:
        return Value(cmp > 0);
      case BinaryOperation::kGreaterThanEquals:
        return Value(cmp >= 0);
      default:
        break;
    }
    if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract ||
        op == BinaryOperation::kMultiply || op == BinaryOperation::kDivide ||
        op == BinaryOperation::kModulo) {
      const uint64_t lhs = static_cast<uint64_t>(left.value.int_value);
      const uint64_t rhs = static_cast<uint64_t>(right.value.int_value);
      if ((op == BinaryOperation::kDivide || op == BinaryOperation::kModulo) &&
          rhs == 0) {
        throw std::runtime_error("division by zero");
      }
      // Keep ordinary signed-looking results signed when a UINT64 value is
      // still in INT64's range.  This preserves SQL's `0 - 1 == -1` behavior,
      // while values crossing the signed boundary use UINT64 wraparound.
      if (left.IsUnsigned() && !right.IsUnsigned() &&
          lhs <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
          right.value.int_value >= 0) {
        bool fits_signed = false;
        int64_t signed_result = 0;
        switch (op) {
          case BinaryOperation::kAdd:
            fits_signed = rhs <= static_cast<uint64_t>(
                                     std::numeric_limits<int64_t>::max()) -
                                     lhs;
            if (fits_signed) {
              signed_result = static_cast<int64_t>(lhs) + right.value.int_value;
            }
            break;
          case BinaryOperation::kSubtract:
            fits_signed = true;
            signed_result = static_cast<int64_t>(lhs) - right.value.int_value;
            break;
          case BinaryOperation::kMultiply:
            fits_signed =
                rhs == 0 || lhs <= static_cast<uint64_t>(
                                       std::numeric_limits<int64_t>::max()) /
                                       rhs;
            if (fits_signed) {
              signed_result = static_cast<int64_t>(lhs) * right.value.int_value;
            }
            break;
          default:
            break;
        }
        if (fits_signed) {
          return Value(static_cast<int64_t>(signed_result));
        }
      }
      uint64_t unsigned_result = 0;
      switch (op) {
        case BinaryOperation::kAdd:
          unsigned_result = lhs + rhs;
          break;
        case BinaryOperation::kSubtract:
          unsigned_result = lhs - rhs;
          break;
        case BinaryOperation::kMultiply:
          unsigned_result = lhs * rhs;
          break;
        case BinaryOperation::kDivide:
          unsigned_result = lhs / rhs;
          break;
        case BinaryOperation::kModulo:
          unsigned_result = lhs % rhs;
          break;
        default:
          break;
      }
      return Value(static_cast<int64_t>(unsigned_result)).WithUnsigned();
    }
  }
  if (op == BinaryOperation::kDivide && numeric) {
    const double lhs = left.type == ValueType::kDouble
                           ? left.value.double_value
                           : static_cast<double>(left.value.int_value);
    const double rhs = right.type == ValueType::kDouble
                           ? right.value.double_value
                           : static_cast<double>(right.value.int_value);
    if (rhs == 0.0) {
      throw std::runtime_error("division by zero");
    }
    const double result = lhs / rhs;
    if (std::isfinite(lhs) && std::isfinite(rhs) && std::isinf(result)) {
      throw std::runtime_error("double overflow");
    }
    return Value(result);
  }
  if (numeric && left.type != right.type) {
    const double lhs = left.type == ValueType::kDouble
                           ? left.value.double_value
                           : static_cast<double>(left.value.int_value);
    const double rhs = right.type == ValueType::kDouble
                           ? right.value.double_value
                           : static_cast<double>(right.value.int_value);
    // IEEE unordered comparisons: any ordered comparison against NaN is
    // FALSE (never NULL), even NaN vs NaN (GoogleSQL BETWEEN semantics).
    const auto is_nan = [](const Value& v) {
      return v.type == ValueType::kDouble && std::isnan(v.value.double_value);
    };
    if (is_nan(folded_left) || is_nan(folded_right)) {
      switch (op) {
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
          return Value(false);
        default:
          break;
      }
    }
    switch (op) {
      case BinaryOperation::kAdd:
        return Value(lhs + rhs);
      case BinaryOperation::kSubtract:
        return Value(lhs - rhs);
      case BinaryOperation::kMultiply:
        return Value(lhs * rhs);
      case BinaryOperation::kDivide:
        if (rhs == 0.0) {
          throw std::runtime_error("division by zero");
        }
        return Value(lhs / rhs);
      case BinaryOperation::kModulo:
        if (rhs == 0.0) {
          throw std::runtime_error("division by zero");
        }
        return Value(std::fmod(lhs, rhs));
      case BinaryOperation::kEquals:
        return Value(lhs == rhs);
      case BinaryOperation::kNotEquals:
        return Value(lhs != rhs);
      case BinaryOperation::kLessThan:
        return Value(lhs < rhs);
      case BinaryOperation::kLessThanEquals:
        return Value(lhs <= rhs);
      case BinaryOperation::kGreaterThan:
        return Value(lhs > rhs);
      case BinaryOperation::kGreaterThanEquals:
        return Value(lhs >= rhs);
      default:
        break;
    }
  }
  if (left.type != right.type) {
    auto is_iv = [](std::string_view s) {
      return s.find('-') != std::string_view::npos &&
             s.find(' ') != std::string_view::npos && s.find('-') < s.find(' ');
    };
    if (left.type == ValueType::kVarChar && right.type == ValueType::kInt64 &&
        is_iv(left.value.varchar_value) && op == BinaryOperation::kMultiply) {
      IntervalValue iv = IntervalValue::Parse(left.value.varchar_value);
      return Value((iv * right.value.int_value).ToString());
    }
    if (left.type == ValueType::kInt64 && right.type == ValueType::kVarChar &&
        is_iv(right.value.varchar_value) && op == BinaryOperation::kMultiply) {
      IntervalValue iv = IntervalValue::Parse(right.value.varchar_value);
      return Value((iv * left.value.int_value).ToString());
    }
    if (left.type == ValueType::kDate && right.type == ValueType::kVarChar) {
      try {
        return EvaluateBinary(
            op, left,
            Value::DateFromDays(ParseDateDays(right.value.varchar_value)));
      } catch (const std::exception& error) {
        (void)error;
      }
    } else if (left.type == ValueType::kVarChar &&
               right.type == ValueType::kDate) {
      try {
        return EvaluateBinary(
            op, Value::DateFromDays(ParseDateDays(left.value.varchar_value)),
            right);
      } catch (const std::exception& error) {
        (void)error;
      }
    }
    throw std::runtime_error("type mismatch");
  }
  if (left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
    auto is_iv = [](std::string_view s) {
      return s.find('-') != std::string_view::npos &&
             s.find(' ') != std::string_view::npos && s.find('-') < s.find(' ');
    };
    if (is_iv(left.value.varchar_value) && is_iv(right.value.varchar_value)) {
      IntervalValue iv1 = IntervalValue::Parse(left.value.varchar_value);
      IntervalValue iv2 = IntervalValue::Parse(right.value.varchar_value);
      switch (op) {
        case BinaryOperation::kAdd:
          return Value((iv1 + iv2).ToString());
        case BinaryOperation::kSubtract:
          return Value((iv1 - iv2).ToString());
        case BinaryOperation::kEquals:
          return Value(iv1 == iv2);
        case BinaryOperation::kNotEquals:
          return Value(iv1 != iv2);
        case BinaryOperation::kLessThan:
          return Value(iv1 < iv2);
        case BinaryOperation::kLessThanEquals:
          return Value(iv1 <= iv2);
        case BinaryOperation::kGreaterThan:
          return Value(iv1 > iv2);
        case BinaryOperation::kGreaterThanEquals:
          return Value(iv1 >= iv2);
        default:
          break;
      }
    }
  }
  // IEEE unordered comparisons: any ordered comparison against a NaN is
  // FALSE (never NULL), even NaN vs NaN (GoogleSQL BETWEEN semantics).
  {
    const auto operand_is_nan = [](const Value& v) {
      return v.type == ValueType::kDouble && std::isnan(v.value.double_value);
    };
    if (operand_is_nan(folded_left) || operand_is_nan(folded_right)) {
      switch (op) {
        case BinaryOperation::kEquals:
          return Value(false);
        case BinaryOperation::kNotEquals:
          return Value(true);
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
          return Value(false);
        default:
          break;
      }
    }
  }
  try {
    auto preserve_unsigned = [&](Value result) {
      return (left.IsUnsigned() || right.IsUnsigned()) ? result.WithUnsigned()
                                                       : result;
    };
    switch (op) {
      case BinaryOperation::kAdd:
        return preserve_unsigned(left + right);
      case BinaryOperation::kSubtract:
        return preserve_unsigned(left - right);
      case BinaryOperation::kMultiply:
        return preserve_unsigned(left * right);
      case BinaryOperation::kDivide: {
        if (left.type == ValueType::kDouble) {
          if (right.value.double_value == 0.0) {
            throw std::runtime_error("division by zero");
          }
          const double result =
              left.value.double_value / right.value.double_value;
          if (std::isfinite(left.value.double_value) &&
              std::isfinite(right.value.double_value) && std::isinf(result)) {
            throw std::runtime_error("double overflow");
          }
          return preserve_unsigned(Value(result));
        }
        return preserve_unsigned(left / right);
      }
      case BinaryOperation::kModulo:
        return preserve_unsigned(left % right);
      default:
        break;
    }
  } catch (const std::runtime_error& error) {
    if (std::string_view(error.what()).starts_with("Cannot do ")) {
      throw std::runtime_error("unsupported binary operation");
    }
    throw;
  }
  switch (op) {
    case BinaryOperation::kAdd:
    case BinaryOperation::kSubtract:
    case BinaryOperation::kMultiply:
    case BinaryOperation::kDivide:
    case BinaryOperation::kModulo:
    case BinaryOperation::kEquals:
      if (folded_left.type == ValueType::kVarChar &&
          folded_right.type == ValueType::kVarChar &&
          IsStructJson(folded_left.value.varchar_value) &&
          IsStructJson(folded_right.value.varchar_value)) {
        return StructJsonCompare(folded_left.value.varchar_value,
                                 folded_right.value.varchar_value);
      }
      return Value(folded_left == folded_right);
    case BinaryOperation::kNotEquals:
      if (folded_left.type == ValueType::kVarChar &&
          folded_right.type == ValueType::kVarChar &&
          IsStructJson(folded_left.value.varchar_value) &&
          IsStructJson(folded_right.value.varchar_value)) {
        const Value equal = StructJsonCompare(folded_left.value.varchar_value,
                                              folded_right.value.varchar_value);
        if (equal.IsNull()) {
          return {};
        }
        return Value(!equal.Truthy());
      }
      return Value(folded_left != folded_right);
    case BinaryOperation::kLessThan:
      return Value(folded_left < folded_right);
    case BinaryOperation::kLessThanEquals:
      return Value(folded_left <= folded_right);
    case BinaryOperation::kGreaterThan:
      return Value(folded_left > folded_right);
    case BinaryOperation::kGreaterThanEquals:
      return Value(folded_left >= folded_right);
    case BinaryOperation::kAnd:
    case BinaryOperation::kOr:
    case BinaryOperation::kXor:
    case BinaryOperation::kLike:
    case BinaryOperation::kNotLike:
    case BinaryOperation::kShiftLeft:
    case BinaryOperation::kShiftRight:
    case BinaryOperation::kIsDistinctFrom:
    case BinaryOperation::kIsNotDistinctFrom:
      // Already handled above; kept for -Wswitch completeness.
      break;
  }

  throw std::logic_error("invalid binary operation");
}

Value BinaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  // AND/OR are short-circuited: the right child must not be evaluated when
  // the left operand already decides the result (three-valued logic).
  if (op_ == BinaryOperation::kAnd || op_ == BinaryOperation::kOr) {
    const Value left = left_->Evaluate(row, schema);
    if (!left.IsNull() && left.Truthy() != (op_ == BinaryOperation::kAnd)) {
      return Value(op_ == BinaryOperation::kOr);
    }
    return EvaluateBinary(op_, left, right_->Evaluate(row, schema));
  }
  return EvaluateBinary(op_, left_->Evaluate(row, schema),
                        right_->Evaluate(row, schema));
}

Value BinaryExpression::Evaluate(const Row* left, const Schema& left_schema,
                                 const Row* right,
                                 const Schema& right_schema) const {
  if (op_ == BinaryOperation::kAnd || op_ == BinaryOperation::kOr) {
    const Value left_value =
        left_->Evaluate(left, left_schema, right, right_schema);
    if (!left_value.IsNull() &&
        left_value.Truthy() != (op_ == BinaryOperation::kAnd)) {
      return Value(op_ == BinaryOperation::kOr);
    }
    return EvaluateBinary(
        op_, left_value,
        right_->Evaluate(left, left_schema, right, right_schema));
  }
  return EvaluateBinary(
      op_, left_->Evaluate(left, left_schema, right, right_schema),
      right_->Evaluate(left, left_schema, right, right_schema));
}

// Context-aware form: identical dispatch to the plain evaluator, with the
// context threaded into both children (A1 stage 2).
Value BinaryExpression::Evaluate(const Row& row, const Schema& schema,
                                 EvaluationContext& context) const {
  if (op_ == BinaryOperation::kAnd || op_ == BinaryOperation::kOr) {
    const Value left_value = left_->Evaluate(row, schema, context);
    if (!left_value.IsNull() &&
        left_value.Truthy() != (op_ == BinaryOperation::kAnd)) {
      return Value(op_ == BinaryOperation::kOr);
    }
    return EvaluateBinary(op_, left_value,
                          right_->Evaluate(row, schema, context));
  }
  return EvaluateBinary(op_, left_->Evaluate(row, schema, context),
                        right_->Evaluate(row, schema, context));
}

namespace {
Type BinaryResultType(BinaryOperation operation, const Type& left,
                      const Type& right) {
  if (IsComparison(operation) || operation == BinaryOperation::kAnd ||
      operation == BinaryOperation::kOr || operation == BinaryOperation::kXor ||
      operation == BinaryOperation::kLike ||
      operation == BinaryOperation::kNotLike) {
    return {TypeTag::kBigInt};
  }
  if (operation == BinaryOperation::kDivide) {
    return {TypeTag::kDouble};
  }
  if (left.GetType() == TypeTag::kDouble ||
      right.GetType() == TypeTag::kDouble) {
    return {TypeTag::kDouble};
  }

  if (operation == BinaryOperation::kAdd &&
      left.GetType() == TypeTag::kVarChar &&
      right.GetType() == TypeTag::kVarChar) {
    return {TypeTag::kVarChar};
  }
  return {TypeTag::kBigInt};
}
}  // namespace

Type BinaryExpression::ResultType(const Schema& schema) const {
  return BinaryResultType(op_, left_->ResultType(schema),
                          right_->ResultType(schema));
}

Type BinaryExpression::ResultType(const Schema& left,
                                  const Schema& right) const {
  return BinaryResultType(op_, left_->ResultType(left, right),
                          right_->ResultType(left, right));
}

std::string BinaryExpression::ToString() const {
  return "(" + left_->ToString() + " " + std::string(tinylamb::ToString(op_)) +
         " " + right_->ToString() + ")";
}

void BinaryExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
