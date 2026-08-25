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

#include "type/value.hpp"
#include <endian.h>

#include <charconv>
#include <cmath>
#include <cstdint>

#include <cstring>
#include <functional>
#include <limits>
#include <string>
#include <vector>
#include <utility>
#include <stdexcept>
#include <ostream>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/serdes.hpp"
#include "type/value_type.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"

namespace tinylamb {

struct ArrayPayload {
  std::string element_sql_type;
  std::vector<Value> elements;
};

namespace {
const std::vector<Value> kEmptyArrayElements{};
}  // namespace

std::string ToString(UnaryOperation type) {
  switch (type) {
    case UnaryOperation::kIsNull:
      return "IS NULL";
    case UnaryOperation::kIsNotNull:
      return "IS NOT NULL";
    case UnaryOperation::kIsTrue:
      return "IS TRUE";
    case UnaryOperation::kIsNotTrue:
      return "IS NOT TRUE";
    case UnaryOperation::kIsFalse:
      return "IS FALSE";
    case UnaryOperation::kIsNotFalse:
      return "IS NOT FALSE";
    case UnaryOperation::kNot:
      return "NOT";
    case UnaryOperation::kMinus:
      return "-";
  }
  return "UNKNOWN";
}


std::string ToString(AggregationType type) {
  switch (type) {
    case AggregationType::kCount:
      return "COUNT";
    case AggregationType::kSum:
      return "SUM";
    case AggregationType::kAvg:
      return "AVG";
    case AggregationType::kMin:
      return "MIN";
    case AggregationType::kMax:
      return "MAX";
    case AggregationType::kLogicalAnd:
      return "LOGICAL_AND";
    case AggregationType::kLogicalOr:
      return "LOGICAL_OR";
    case AggregationType::kArrayAgg:
      return "ARRAY_AGG";
    case AggregationType::kStringAgg:
      return "STRING_AGG";
    case AggregationType::kCountIf:
      return "COUNTIF";
    default:
      return "UNKNOWN";
  }
}

Value::Value(int int_val) {
  type = ValueType::kInt64;
  value.int_value = int_val;
}

Value::Value(int64_t int_val) {
  type = ValueType::kInt64;
  value.int_value = int_val;
}

/*
Value::Value(std::string_view varchar_val) {
  type = ValueType::kVarChar;
  value.varchar_value = varchar_val;
}
*/

Value::Value(std::string&& str_val) : owned_data(std::move(str_val)) {
  type = ValueType::kVarChar;
  value.varchar_value = owned_data;
}

Value::Value(double double_value) {
  type = ValueType::kDouble;
  value.double_value = double_value;
}

Value Value::Date(std::string_view date) {
  return DateFromDays(ParseDateDays(date));
}

Value Value::DateFromDays(int64_t days) {
  Value result;
  result.type = ValueType::kDate;
  result.value.int_value = days;
  return result;
}

int64_t Value::DateDays() const {
  if (type != ValueType::kDate) { throw std::runtime_error("DATE value required");
}
  return value.int_value;
}

Value Value::Array(std::vector<Value> elements, std::string element_sql_type) {
  Value result;
  result.type = ValueType::kArray;
  result.array_ = std::make_shared<ArrayPayload>();
  result.array_->element_sql_type = std::move(element_sql_type);
  result.array_->elements = std::move(elements);
  return result;
}

const std::vector<Value>& Value::ArrayElements() const {
  if (array_ == nullptr) { return kEmptyArrayElements; }
  return array_->elements;
}

const std::string& Value::ArrayElementSqlType() const {
  static const std::string kEmpty;
  if (array_ == nullptr) { return kEmpty; }
  return array_->element_sql_type;
}

Value::Value(const Value& o)
    : value(o.value), type(o.type), array_(o.array_) {
  if (type == ValueType::kVarChar) {
    owned_data.assign(o.value.varchar_value);
    value.varchar_value = owned_data;
  }
}

Value::Value(Value&& o) noexcept
    : value(o.value),
      type(o.type),
      owned_data(std::move(o.owned_data)),
      array_(std::move(o.array_)) {
  if (type == ValueType::kVarChar) { value.varchar_value = owned_data; }
  o.type = ValueType::kNull;
}

Value& Value::operator=(const Value& rhs) {
  if (this == &rhs) { return *this; }
  type = rhs.type;
  value = rhs.value;
  owned_data.clear();
  array_ = rhs.array_;
  if (type == ValueType::kVarChar) {
    owned_data.assign(rhs.value.varchar_value);
    value.varchar_value = owned_data;
  }
  return *this;
}

Value& Value::operator=(Value&& o) noexcept {
  if (this == &o) { return *this; }
  owned_data = std::move(o.owned_data);
  array_ = std::move(o.array_);
  type = o.type;
  value = o.value;
  if (type == ValueType::kVarChar) { value.varchar_value = owned_data; }
  o.type = ValueType::kNull;
  return *this;
}

bool Value::Truthy() const {
  if (IsNull()) {
    return false;
  }
  if (type == ValueType::kInt64) {
    return value.int_value != 0;
  }
  return true;
}

[[nodiscard]] size_t Value::Size() const {
  switch (type) {
    case ValueType::kNull:
      return 1;
    case ValueType::kInt64:
    case ValueType::kDate:
      return sizeof(int64_t);
    case ValueType::kVarChar:
      return SerializeSize(value.varchar_value);
    case ValueType::kDouble:
      return sizeof(double);
    case ValueType::kArray: {
      size_t bytes = sizeof(uint32_t) + SerializeSize(ArrayElementSqlType());
      for (const Value& element : ArrayElements()) {
        bytes += 1;
        if (!element.IsNull()) { bytes += 1 + element.Size(); }
      }
      return bytes;
    }
  }
  throw std::runtime_error("undefined type");
}

size_t Value::Serialize(char* dst) const {
  switch (type) {
    case ValueType::kNull:
      return SerializeNull(dst);
    case ValueType::kInt64:
    case ValueType::kDate:
      return SerializeInteger(dst, value.int_value);
    case ValueType::kVarChar:
      return SerializeStringView(dst, value.varchar_value);
    case ValueType::kDouble:
      return SerializeDouble(dst, value.double_value);
    case ValueType::kArray: {
      char* cursor = dst;
      const auto& elements = ArrayElements();
      cursor += SerializeU32(cursor, static_cast<uint32_t>(elements.size()));
      cursor += SerializeStringView(cursor, ArrayElementSqlType());
      for (const Value& element : elements) {
        if (element.IsNull()) {
          *cursor++ = 0;
          continue;
        }
        *cursor++ = 1;
        *cursor++ = static_cast<char>(element.type);
        cursor += element.Serialize(cursor);
      }
      return static_cast<size_t>(cursor - dst);
    }
  }
  throw std::runtime_error("undefined type");
}

size_t Value::Deserialize(const char* src, ValueType as_type) {
  type = as_type;
  switch (as_type) {
    case ValueType::kNull:
      throw std::runtime_error("Cannot parse without type.");
    case ValueType::kInt64:
    case ValueType::kDate:
      return DeserializeInteger(src, &value.int_value);
    case ValueType::kVarChar: {
      std::string_view decoded;
      const size_t consumed = DeserializeStringView(src, &decoded);
      owned_data.assign(decoded);
      value.varchar_value = owned_data;
      return consumed;
    }
    case ValueType::kDouble:
      return DeserializeDouble(src, &value.double_value);
    case ValueType::kArray: {
      array_.reset();
      const char* cursor = src;
      uint32_t count = 0;
      cursor += DeserializeU32(cursor, &count);
      std::string_view sql_type;
      cursor += DeserializeStringView(cursor, &sql_type);
      std::vector<Value> elements;
      elements.reserve(count);
      for (uint32_t i = 0; i < count; ++i) {
        const bool present = *cursor++ != 0;
        if (!present) {
          elements.emplace_back();
          continue;
        }
        const auto elem_type = static_cast<ValueType>(*cursor++);
        Value element;
        cursor += element.Deserialize(cursor, elem_type);
        elements.push_back(std::move(element));
      }
      array_ = std::make_shared<ArrayPayload>();
      array_->element_sql_type = std::string(sql_type);
      array_->elements = std::move(elements);
      return static_cast<size_t>(cursor - src);
    }
  }
  throw std::runtime_error("undefined type");
}

size_t Value::SkipSerialized(const char* src, ValueType as_type) {
  switch (as_type) {
    case ValueType::kNull:
      throw std::runtime_error("Cannot skip without type.");
    case ValueType::kInt64:
    case ValueType::kDate:
      return sizeof(int64_t);
    case ValueType::kDouble:
      return sizeof(double);
    case ValueType::kVarChar: {
      bin_size_t len = 0;
      DeserializeU16(src, &len);
      return sizeof(bin_size_t) + len;
    }
    case ValueType::kArray: {
      const char* cursor = src;
      uint32_t count = 0;
      cursor += DeserializeU32(cursor, &count);
      std::string_view sql_type;
      cursor += DeserializeStringView(cursor, &sql_type);
      for (uint32_t i = 0; i < count; ++i) {
        const bool present = *cursor++ != 0;
        if (!present) { continue; }
        const auto elem_type = static_cast<ValueType>(*cursor++);
        cursor += SkipSerialized(cursor, elem_type);
      }
      return static_cast<size_t>(cursor - src);
    }
  }
  throw std::runtime_error("undefined type");
}

[[nodiscard]] std::string Value::AsString() const {
  switch (type) {
    case ValueType::kNull:
      return "(unknown type)";
    case ValueType::kInt64:
      return std::to_string(value.int_value);
    case ValueType::kDate:
      return FormatDateDays(value.int_value);
    case ValueType::kVarChar:
      return "\"" + std::string(value.varchar_value) + "\"";
    case ValueType::kDouble: {
      if (std::isnan(value.double_value)) { return "nan"; }
      if (std::isinf(value.double_value)) {
        return value.double_value > 0 ? "inf" : "-inf";
      }
      char buffer[64];
      auto [ptr, ec] =
          std::to_chars(buffer, buffer + sizeof(buffer), value.double_value);
      return std::string(buffer, ptr - buffer);
    }


    case ValueType::kArray: {
      std::string out = "ARRAY<" + ArrayElementSqlType() + ">[";
      const auto& elements = ArrayElements();
      for (size_t i = 0; i < elements.size(); ++i) {
        if (i != 0) { out += ", "; }
        out += elements[i].IsNull() ? "NULL" : elements[i].AsString();
      }
      out += "]";
      return out;
    }
  }
  throw std::runtime_error("undefined type");
}

bool Value::operator==(const Value& rhs) const {
  if (type != rhs.type) {
    return false;
  }
  switch (type) {
    case ValueType::kNull:
      return true;
    case ValueType::kInt64:
    case ValueType::kDate:
      return value.int_value == rhs.value.int_value;
    case ValueType::kVarChar: {
      std::string_view sv1 = value.varchar_value;
      std::string_view sv2 = rhs.value.varchar_value;
      if (sv1 == sv2) { return true; }
      auto is_iv = [](std::string_view s) {
        return s.find('-') != std::string_view::npos &&
               s.find(' ') != std::string_view::npos &&
               s.find('-') < s.find(' ');
      };
      if (is_iv(sv1) && is_iv(sv2)) {
        return IntervalValue::Parse(sv1) == IntervalValue::Parse(sv2);
      }
      return false;
    }
    case ValueType::kDouble:
      // Epsilon comparison: accumulated sums must compare equal to literals
      // (e.g. SUM over doubles vs 22.44). Exact bit equality is too strict.
      return std::fabs(value.double_value - rhs.value.double_value) < 1e-9;
    case ValueType::kArray:
      return ArrayElementSqlType() == rhs.ArrayElementSqlType() &&
             ArrayElements() == rhs.ArrayElements();
  }
  throw std::runtime_error("undefined type");
}

namespace {
std::string EncodeMemcomparableFormatInteger(int64_t in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(ValueType::kInt64);  // Embeds prefix.
  const uint64_t be = htobe64(in);
  ::memcpy(ret.data() + 1, &be, 8);
  ret[1] ^= static_cast<char>(0x80);  // plus/minus sign.
  return ret;
}

size_t DecodeMemcomparableFormatInteger(const char* src, int64_t* dst) {
  uint64_t loaded = 0;
  ::memcpy(&loaded, src, sizeof(loaded));
  // Undo the sign flip the encoder applied to the top bit of the big-endian
  // image. XOR must happen after byte-swap so the fix lands on bit 63 on any
  // host endianness (XORing before the swap only works on little-endian).
  *dst = static_cast<int64_t>(be64toh(loaded) ^ (uint64_t{1} << 63));
  return sizeof(int64_t);
}

std::string EncodeMemcomparableFormatVarchar(std::string_view in) {
  if (in.empty()) {
    return {'\x02', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  }
  std::string ret(1 + ((in.size() + 7) / 8 * 9), '\0');
  ret[0] = static_cast<char>(ValueType::kVarChar);  // Embeds prefix.
  char* dst = ret.data() + 1;
  const char* src = in.data();
  const size_t size = in.size();
  for (size_t i = 0;; i += 8) {
    if (9 <= size - i) {
      ::memcpy(dst, src, 8);
      dst += 8;
      src += 8;
      *dst++ = 9;
    } else {
      // Final 1~8 bytes.
      ::memcpy(dst, src, size - i);
      dst += 8;
      *dst++ = static_cast<char>((size % 8) + (size % 8 == 0 ? 8 : 0));
      return ret;
    }
  }
}

size_t DecodeMemcomparableFormatVarchar(const char* src, std::string* dst) {
  dst->clear();
  const char* buffer = nullptr;
  const char* const initial_offset = src;
  for (size_t size = 0;;) {
    buffer = src;
    const size_t offset = dst->size();
    if (buffer[8] == 9) {
      size += 8;
      dst->resize(size);
      ::memcpy(dst->data() + offset, buffer, 8);
    } else {
      size += buffer[8];
      src += 9;
      dst->resize(size);
      ::memcpy(dst->data() + offset, buffer, buffer[8]);
      break;
    }
    src += buffer[8];
  }
  return src - initial_offset;
}

std::string EncodeMemcomparableFormatDouble(double in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(ValueType::kDouble);  // Embeds prefix.
  uint64_t bits = 0;
  std::memcpy(&bits, &in, sizeof(bits));
  uint64_t be = htobe64(bits);
  if (0 <= in) {
    be |= 0x80;
  } else {
    be = ~be;
  }
  std::memcpy(ret.data() + 1, &be, 8);
  return ret;
}

size_t DecodeMemcomparableFormatDouble(const char* src, double* dst) {
  int64_t loaded = 0;
  std::memcpy(&loaded, src, sizeof(int64_t));
  uint64_t code = be64toh(loaded);
  if (0 < (src[0] & 0x80)) {
    code ^= 1LLU << 63;
  } else {
    code = ~code;
  }
  std::memcpy(dst, &code, sizeof(*dst));
  return sizeof(double);
}
}  // anonymous namespace

std::string Value::EncodeMemcomparableFormat() const {
  switch (type) {
    case ValueType::kNull:
      throw std::runtime_error("Cannot encode unknown type.");
    case ValueType::kInt64:
      return EncodeMemcomparableFormatInteger(value.int_value);
    case ValueType::kDate: {
      std::string encoded = EncodeMemcomparableFormatInteger(value.int_value);
      encoded[0] = static_cast<char>(ValueType::kDate);
      return encoded;
    }
    case ValueType::kVarChar:
      return EncodeMemcomparableFormatVarchar(value.varchar_value);
    case ValueType::kDouble:
      return EncodeMemcomparableFormatDouble(value.double_value);
    case ValueType::kArray: {
      std::string encoded(1, static_cast<char>(ValueType::kArray));
      const auto& elements = ArrayElements();
      const uint32_t count = static_cast<uint32_t>(elements.size());
      const uint32_t be = htobe32(count);
      encoded.append(reinterpret_cast<const char*>(&be), sizeof(be));
      encoded.append(ArrayElementSqlType());
      encoded.push_back('\0');
      for (const Value& element : elements) {
        if (element.IsNull()) {
          encoded.push_back(0);
          continue;
        }
        encoded.push_back(1);
        encoded += element.EncodeMemcomparableFormat();
      }
      return encoded;
    }
  }
  throw std::runtime_error("undefined type");
}

size_t Value::DecodeMemcomparableFormat(const char* src) {
  switch (static_cast<ValueType>(*src++)) {
    case ValueType::kNull:
      throw std::runtime_error("Cannot decode unknown type.");
    case ValueType::kInt64:
      type = ValueType::kInt64;
      return DecodeMemcomparableFormatInteger(src, &value.int_value) + 1;
    case ValueType::kDate:
      type = ValueType::kDate;
      return DecodeMemcomparableFormatInteger(src, &value.int_value) + 1;
    case ValueType::kVarChar: {
      type = ValueType::kVarChar;
      size_t len = DecodeMemcomparableFormatVarchar(src, &owned_data);
      value.varchar_value = owned_data;
      return len + 1;
    }
    case ValueType::kDouble:
      type = ValueType::kDouble;
      return DecodeMemcomparableFormatDouble(src, &value.double_value) + 1;
    case ValueType::kArray: {
      type = ValueType::kArray;
      const char* cursor = src;
      uint32_t be = 0;
      std::memcpy(&be, cursor, sizeof(be));
      cursor += sizeof(be);
      const uint32_t count = be32toh(be);
      const char* type_begin = cursor;
      while (*cursor != '\0') { ++cursor; }
      std::string sql_type(type_begin, cursor);
      ++cursor;
      std::vector<Value> elements;
      elements.reserve(count);
      for (uint32_t i = 0; i < count; ++i) {
        if (*cursor++ == 0) {
          elements.emplace_back();
          continue;
        }
        Value element;
        cursor += element.DecodeMemcomparableFormat(cursor);
        elements.push_back(std::move(element));
      }
      array_ = std::make_shared<ArrayPayload>();
      array_->element_sql_type = std::move(sql_type);
      array_->elements = std::move(elements);
      return static_cast<size_t>(cursor - (src - 1));
    }
  }
  throw std::runtime_error("broken data");
}

bool Value::operator<(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be compared.");
  }
  switch (type) {
    case ValueType::kNull:
      throw std::runtime_error("Unknown type cannot be compared.");
    case ValueType::kInt64:
    case ValueType::kDate:
      return value.int_value < rhs.value.int_value;
    case ValueType::kVarChar: {
      std::string_view sv1 = value.varchar_value;
      std::string_view sv2 = rhs.value.varchar_value;
      auto is_iv = [](std::string_view s) {
        return s.find('-') != std::string_view::npos &&
               s.find(' ') != std::string_view::npos &&
               s.find('-') < s.find(' ');
      };
      if (is_iv(sv1) && is_iv(sv2)) {
        return IntervalValue::Parse(sv1) < IntervalValue::Parse(sv2);
      }
      return value.varchar_value < rhs.value.varchar_value;
    }
    case ValueType::kDouble:
      return value.double_value < rhs.value.double_value;
    case ValueType::kArray:
      return ArrayElements() < rhs.ArrayElements();
  }
  throw std::runtime_error("undefined type");
}

bool Value::operator>(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be compared.");
  }
  switch (type) {
    case ValueType::kNull:
      throw std::runtime_error("Unknown type cannot be compared.");
    case ValueType::kInt64:
    case ValueType::kDate:
      return value.int_value > rhs.value.int_value;
    case ValueType::kVarChar: {
      std::string_view sv1 = value.varchar_value;
      std::string_view sv2 = rhs.value.varchar_value;
      auto is_iv = [](std::string_view s) {
        return s.find('-') != std::string_view::npos &&
               s.find(' ') != std::string_view::npos &&
               s.find('-') < s.find(' ');
      };
      if (is_iv(sv1) && is_iv(sv2)) {
        return IntervalValue::Parse(sv1) > IntervalValue::Parse(sv2);
      }
      return value.varchar_value > rhs.value.varchar_value;
    }
    case ValueType::kDouble:
      return value.double_value > rhs.value.double_value;
    case ValueType::kArray:
      return ArrayElements() > rhs.ArrayElements();
  }
  throw std::runtime_error("undefined type");
}

Value Value::operator+(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be added.");
  }
  if (type == ValueType::kInt64) {
    int64_t result = 0;
    if (__builtin_add_overflow(value.int_value, rhs.value.int_value,
                               &result)) {
      throw std::runtime_error("integer overflow on '+'");
    }
    return Value(result);
  }
  if (type == ValueType::kDouble) {
    return Value(value.double_value + rhs.value.double_value);
  }
  if (type == ValueType::kVarChar) {
    std::string new_string(value.varchar_value);
    new_string += rhs.value.varchar_value;
    return Value(std::move(new_string));
  }
  throw std::runtime_error("Cannot do '+' against this type");
}

Value Value::operator-(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be subtracted.");
  }
  if (type == ValueType::kInt64) {
    int64_t result = 0;
    if (__builtin_sub_overflow(value.int_value, rhs.value.int_value,
                               &result)) {
      throw std::runtime_error("integer overflow on '-'");
    }
    return Value(result);
  }
  if (type == ValueType::kDouble) {
    return Value(value.double_value - rhs.value.double_value);
  }
  throw std::runtime_error("Cannot do '-' against this type");
}

Value Value::operator*(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be multiplied.");
  }
  if (type == ValueType::kInt64) {
    int64_t result = 0;
    if (__builtin_mul_overflow(value.int_value, rhs.value.int_value,
                               &result)) {
      throw std::runtime_error("integer overflow on '*'");
    }
    return Value(result);
  }
  if (type == ValueType::kDouble) {
    return Value(value.double_value * rhs.value.double_value);
  }
  throw std::runtime_error("Cannot do '*' against this type");
}

Value Value::operator/(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be divided.");
  }
  if (type == ValueType::kInt64) {
    if (rhs.value.int_value == 0) {
      throw std::runtime_error("division by zero");
    }
    // INT64_MIN / -1 overflows int64 and would raise SIGFPE.
    if (value.int_value == std::numeric_limits<int64_t>::min() &&
        rhs.value.int_value == -1) {
      throw std::runtime_error("integer overflow on '/'");
    }
    return Value(value.int_value / rhs.value.int_value);
  }
  if (type == ValueType::kDouble) {
    return Value(value.double_value / rhs.value.double_value);
  }
  throw std::runtime_error("Cannot do '/' against this type");
}

Value Value::operator%(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot do modulo.");
  }
  if (type == ValueType::kInt64) {
    if (rhs.value.int_value == 0) {
      throw std::runtime_error("modulo by zero");
    }
    // INT64_MIN % -1 would raise SIGFPE on x86 despite the mathematical
    // result (0) being representable.
    if (value.int_value == std::numeric_limits<int64_t>::min() &&
        rhs.value.int_value == -1) {
      throw std::runtime_error("integer overflow on '%'");
    }
    return Value(value.int_value % rhs.value.int_value);
  }
  throw std::runtime_error("Cannot do '%' against this type");
}

Value Value::operator&(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot do AND.");
  }
  if (type == ValueType::kInt64) {
    return Value(value.int_value & rhs.value.int_value);
  }
  throw std::runtime_error("Cannot do '&' against this type");
}

Value Value::operator|(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot do OR.");
  }
  if (type == ValueType::kInt64) {
    return Value(value.int_value | rhs.value.int_value);
  }
  throw std::runtime_error("Cannot do '|' against this type");
}

Value Value::operator^(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot do XOR.");
  }
  if (type == ValueType::kInt64) {
    return Value(value.int_value ^ rhs.value.int_value);
  }
  throw std::runtime_error("Cannot do '^' against this type");
}

std::ostream& operator<<(std::ostream& o, const Value& v) {
  o << v.AsString();
  return o;
}

Encoder& operator<<(Encoder& a, const Value& v) {
  a << v.type;
  switch (v.type) {
    case tinylamb::ValueType::kNull:
      break;
    case tinylamb::ValueType::kInt64:
    case tinylamb::ValueType::kDate:
      a << v.value.int_value;
      break;
    case tinylamb::ValueType::kVarChar:
      a << v.value.varchar_value;
      break;
    case tinylamb::ValueType::kDouble:
      a << v.value.double_value;
      break;
    case tinylamb::ValueType::kArray:
      a << v.ArrayElementSqlType() << v.ArrayElements();
      break;
  }
  return a;
}

Decoder& operator>>(Decoder& e, Value& v) {
  e >> v.type;
  switch (v.type) {
    case tinylamb::ValueType::kNull:
      break;
    case tinylamb::ValueType::kInt64:
    case tinylamb::ValueType::kDate:
      e >> v.value.int_value;
      break;
    case tinylamb::ValueType::kVarChar:
      e >> v.owned_data;
      v.value.varchar_value = v.owned_data;
      break;
    case tinylamb::ValueType::kDouble:
      e >> v.value.double_value;
      break;
    case tinylamb::ValueType::kArray: {
      std::string sql_type;
      std::vector<Value> elements;
      e >> sql_type >> elements;
      v = Value::Array(std::move(elements), std::move(sql_type));
      break;
    }
    default:
      // Corrupted stream: the raw byte read into v.type was not a ValueType.
      throw std::runtime_error("undefined type");
  }
  return e;
}
}  // namespace tinylamb

uint64_t std::hash<tinylamb::Value>::operator()(
    const tinylamb::Value& v) const {
  switch (v.type) {
    case tinylamb::ValueType::kNull:
      return 0x9e3779b97f4a7c15ULL;
    case tinylamb::ValueType::kInt64:
    case tinylamb::ValueType::kDate:
      return std::hash<int64_t>()(v.value.int_value);
    case tinylamb::ValueType::kVarChar: {
      std::string_view sv = v.value.varchar_value;
      auto is_iv = [](std::string_view s) {
        return s.find('-') != std::string_view::npos &&
               s.find(' ') != std::string_view::npos &&
               s.find('-') < s.find(' ');
      };
      if (is_iv(sv)) {
        tinylamb::IntervalValue iv = tinylamb::IntervalValue::Parse(sv);
        constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
        constexpr int64_t kMonthNanos = 30LL * kDayNanos;
        int64_t total = iv.months * kMonthNanos + iv.days * kDayNanos + iv.nanos;
        return std::hash<int64_t>()(total);
      }
      return std::hash<std::string_view>()(sv);
    }
    case tinylamb::ValueType::kDouble:
      return std::hash<double>()(v.value.double_value);
    case tinylamb::ValueType::kArray: {
      uint64_t h = std::hash<std::string>()(v.ArrayElementSqlType());
      for (const tinylamb::Value& element : v.ArrayElements()) {
        h ^= std::hash<tinylamb::Value>()(element) + 0x9e3779b97f4a7c15ULL +
             (h << 6) + (h >> 2);
      }
      return h;
    }
  }
  throw std::runtime_error("undefined type");
}
