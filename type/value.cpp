//
// Created by kumagi on 2022/02/06.
//
#include "type/value.hpp"

#include <assert.h>

#include <cstring>
#include <iomanip>
#include <iostream>

#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"

namespace tinylamb {

Value::Value(int int_val) {
  type = ValueType::kInt64;
  value.int_value = int_val;
}

Value::Value(int64_t int_val) {
  type = ValueType::kInt64;
  value.int_value = int_val;
}

Value::Value(std::string_view varchar_val) {
  type = ValueType::kVarChar;
  value.varchar_value = varchar_val.data();
}

Value::Value(double double_value) {
  type = ValueType::kDouble;
  value.double_value = double_value;
}

[[nodiscard]] size_t Value::Size() const {
  switch (type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Unknown type does not have size");
    case ValueType::kInt64:
      return sizeof(int64_t);
    case ValueType::kVarChar:
      return SerializeSize(value.varchar_value);
    case ValueType::kDouble:
      return sizeof(double);
  }
  throw std::runtime_error("undefined type");
}

size_t Value::Serialize(char* dst) const {
  switch (type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Unknown type cannot be serialized");
    case ValueType::kInt64:
      return SerializeInteger(dst, value.int_value);
    case ValueType::kVarChar:
      return SerializeStringView(dst, value.varchar_value);
    case ValueType::kDouble:
      return SerializeDouble(dst, value.double_value);
  }
  throw std::runtime_error("undefined type");
}

size_t Value::Deserialize(const char* src, ValueType as_type) {
  type = as_type;
  switch (as_type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Cannot parse without type.");
    case ValueType::kInt64:
      return DeserializeInteger(src, &value.int_value);
    case ValueType::kVarChar:
      return DeserializeStringView(src, &value.varchar_value);
    case ValueType::kDouble:
      return DeserializeDouble(src, &value.double_value);
  }
  throw std::runtime_error("undefined type");
}

[[nodiscard]] std::string Value::AsString() const {
  switch (type) {
    case ValueType::kUnknown:
      return "(unknown type)";
    case ValueType::kInt64:
      return std::to_string(value.int_value);
    case ValueType::kVarChar:
      return "\"" + std::string(value.varchar_value) + "\"";
    case ValueType::kDouble:
      return std::to_string(value.double_value);
  }
  throw std::runtime_error("undefined type");
}

bool Value::operator==(const Value& rhs) const {
  if (type != rhs.type) return false;
  switch (type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Unknown type cannot be compared.");
    case ValueType::kInt64:
      return value.int_value == rhs.value.int_value;
    case ValueType::kVarChar:
      return value.varchar_value == rhs.value.varchar_value;
    case ValueType::kDouble:
      return value.double_value == rhs.value.double_value;
  }
  throw std::runtime_error("undefined type");
}

namespace {

std::string EncodeMemcomparableFormatInteger(int64_t in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(ValueType::kInt64);  // Embeds prefix.
  const uint64_t be = htobe64(in);
  memcpy(ret.data() + 1, &be, 8);
  ret[1] ^= char(0x80);  // plus/minus sign.
  return ret;
}

size_t DecodeMemcomparableFormatInteger(const char* src, int64_t* dst) {
  *dst = be64toh(*reinterpret_cast<const int64_t*>(src) ^ 0x80);
  return sizeof(int64_t);
}

std::string EncodeMemcomparableFormatVarchar(std::string_view in) {
  std::string ret(1 + (in.size() + 7) / 8 * 9, '\0');
  ret[0] = static_cast<char>(ValueType::kVarChar);  // Embeds prefix.
  char* dst = ret.data() + 1;
  const char* src = in.data();
  const size_t size = in.size();
  for (size_t i = 0; i < size; i += 8) {
    if (9 <= size - i) {
      memcpy(dst, src, 8);
      dst += 8;
      src += 8;
      *dst++ = 9;
    } else {  // Final 1~8 bytes.
      const size_t diff = size - i;
      memcpy(dst, src, diff);
      dst += 8;
      *dst++ = char((size % 8) + (size % 8 == 0 ? 8 : 0));
      break;
    }
  }
  return ret;
}

size_t DecodeMemcomparableFormatVarchar(std::string_view src,
                                        std::string* dst) {}

std::string EncodeMemcomparableFormatDouble(double in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(ValueType::kDouble);  // Embeds prefix.
  uint64_t be = htobe64(*reinterpret_cast<const uint64_t*>(&in));
  if (0 <= in) {
    be |= 0x80;
  } else {
    be = ~be;
  }
  memcpy(ret.data() + 1, &be, 8);
  return ret;
}

size_t DecodeMemcomparableFormatDouble(const char* src, double* dst) {
  uint64_t code = be64toh(*reinterpret_cast<const int64_t*>(src));
  if (0 < (src[0] & 0x80)) {
    code ^= 1LLU << 63;
  } else {
    code = ~code;
  }
  *dst = *reinterpret_cast<double*>(&code);
  return sizeof(double);
}

}  // anonymous namespace

std::string Value::EncodeMemcomparableFormat() const {
  switch (type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Cannot encode unknown type.");
    case ValueType::kInt64:
      return EncodeMemcomparableFormatInteger(value.int_value);
    case ValueType::kVarChar:
      return EncodeMemcomparableFormatVarchar(value.varchar_value);
    case ValueType::kDouble:
      return EncodeMemcomparableFormatDouble(value.double_value);
  }
  throw std::runtime_error("undefined type");
}

size_t Value::DecodeMemcomparableFormat(const char* src) {
  const char* const initial_pos = src;
  switch (static_cast<ValueType>(*src++)) {
    case ValueType::kUnknown:
      throw std::runtime_error("Cannot decode unknown type.");
    case ValueType::kInt64:
      type = ValueType::kInt64;
      return DecodeMemcomparableFormatInteger(src, &value.int_value);
      return src - initial_pos;
    case ValueType::kVarChar: {
      type = ValueType::kVarChar;
      size_t len = DecodeMemcomparableFormatVarchar(src, &owned_data);
      value.varchar_value = owned_data;
      return len;
    }
    case ValueType::kDouble:
      type = ValueType::kDouble;
      return DecodeMemcomparableFormatDouble(src, &value.double_value);
  }
}

bool Value::operator<(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be compared.");
  }
  switch (type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Unknown type cannot be compared.");
    case ValueType::kInt64:
      return value.int_value < rhs.value.int_value;
    case ValueType::kVarChar:
      return value.varchar_value < rhs.value.varchar_value;
    case ValueType::kDouble:
      return value.double_value < rhs.value.double_value;
  }
  throw std::runtime_error("undefined type");
}

std::ostream& operator<<(std::ostream& o, const Value& v) {
  o << v.AsString();
  return o;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Value>::operator()(
    const tinylamb::Value& v) const {
  switch (v.type) {
    case tinylamb::ValueType::kUnknown:
      break;
    case tinylamb::ValueType::kInt64:
      return std::hash<int64_t>()(v.value.int_value);
    case tinylamb::ValueType::kVarChar:
      return std::hash<std::string_view>()(v.value.varchar_value);
    case tinylamb::ValueType::kDouble:
      return std::hash<double>()(v.value.double_value);
  }
  throw std::runtime_error("undefined type");
}
