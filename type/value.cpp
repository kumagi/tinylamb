//
// Created by kumagi on 2022/02/06.
//
#include "type/value.hpp"

#include <cstring>

#include "common/debug.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
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

Value::Value(const Value& o)
    : value(o.value), type(o.type), owned_data(o.owned_data) {
  if (!owned_data.empty() && type == ValueType::kVarChar) {
    value.varchar_value = owned_data;
  }
}

bool Value::Truthy() const {
  if (type == ValueType::kInt64) {
    return value.int_value != 0;
  }
  return true;
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
  if (type != rhs.type) {
    return false;
  }
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
  ret[1] ^= static_cast<char>(0x80);  // plus/minus sign.
  return ret;
}

size_t DecodeMemcomparableFormatInteger(const char* src, int64_t* dst) {
  int64_t loaded;
  memcpy(&loaded, src, 8);
  *dst = be64toh(loaded ^ 0x80);
  return sizeof(int64_t);
}

std::string EncodeMemcomparableFormatVarchar(std::string_view in) {
  if (in.empty()) {
    return {'\x02', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  }
  std::string ret(1 + (in.size() + 7) / 8 * 9, '\0');
  ret[0] = static_cast<char>(ValueType::kVarChar);  // Embeds prefix.
  char* dst = ret.data() + 1;
  const char* src = in.data();
  const size_t size = in.size();
  for (size_t i = 0;; i += 8) {
    if (9 <= size - i) {
      memcpy(dst, src, 8);
      dst += 8;
      src += 8;
      *dst++ = 9;
    } else {  // Final 1~8 bytes.
      memcpy(dst, src, size - i);
      dst += 8;
      *dst++ = static_cast<char>((size % 8) + (size % 8 == 0 ? 8 : 0));
      return ret;
    }
  }
}

size_t DecodeMemcomparableFormatVarchar(const char* src, std::string* dst) {
  dst->clear();
  const char* buffer;
  const char* const initial_offset = src;
  for (size_t size = 0;;) {
    buffer = src;
    const size_t offset = dst->size();
    if (buffer[8] == 9) {
      size += 8;
      dst->resize(size);
      memcpy(dst->data() + offset, buffer, 8);
    } else {
      size += buffer[8];
      src += 9;
      dst->resize(size);
      memcpy(dst->data() + offset, buffer, buffer[8]);
      break;
    }
    src += buffer[8];
  }
  return src - initial_offset;
}

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
  int64_t loaded;
  memcpy(&loaded, src, sizeof(int64_t));
  uint64_t code = be64toh(loaded);
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
  switch (static_cast<ValueType>(*src++)) {
    case ValueType::kUnknown:
      throw std::runtime_error("Cannot decode unknown type.");
    case ValueType::kInt64:
      type = ValueType::kInt64;
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
  }
  throw std::runtime_error("broken data");
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

bool Value::operator>(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be compared.");
  }
  switch (type) {
    case ValueType::kUnknown:
      throw std::runtime_error("Unknown type cannot be compared.");
    case ValueType::kInt64:
      return value.int_value > rhs.value.int_value;
    case ValueType::kVarChar:
      return value.varchar_value > rhs.value.varchar_value;
    case ValueType::kDouble:
      return value.double_value > rhs.value.double_value;
  }
  throw std::runtime_error("undefined type");
}

Value Value::operator+(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be added.");
  }
  if (type == ValueType::kInt64) {
    return Value(value.int_value + rhs.value.int_value);
  }
  if (type == ValueType::kDouble) {
    return Value(value.double_value + rhs.value.double_value);
  }
  if (type == ValueType::kVarChar) {
    std::string new_string(
        value.varchar_value.size() + rhs.value.varchar_value.size(), '\0');
    memcpy(new_string.data(), value.varchar_value.data(),
           value.varchar_value.size());
    memcpy(new_string.data() + value.varchar_value.size(),
           rhs.value.varchar_value.data(), rhs.value.varchar_value.size());
    Value new_value;
    new_value.owned_data = new_string;
    new_value.type = ValueType::kVarChar;
    new_value.value.varchar_value = new_value.owned_data;
    return new_value;
  }
  throw std::runtime_error("Cannot do '+' against this type");
}

Value Value::operator-(const Value& rhs) const {
  if (type != rhs.type) {
    throw std::runtime_error("Different type cannot be subtracted.");
  }
  if (type == ValueType::kInt64) {
    return Value(value.int_value - rhs.value.int_value);
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
    return Value(value.int_value * rhs.value.int_value);
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

Value& Value::operator=(const Value& rhs) {
  type = rhs.type;
  value = rhs.value;
  if (!rhs.owned_data.empty()) {
    owned_data = rhs.owned_data;
    value.varchar_value = owned_data;
  }
  return *this;
}

std::ostream& operator<<(std::ostream& o, const Value& v) {
  o << v.AsString();
  return o;
}

Encoder& operator<<(Encoder& a, const Value& v) {
  a << v.type;
  switch (v.type) {
    case tinylamb::ValueType::kUnknown:
      break;
    case tinylamb::ValueType::kInt64:
      a << v.value.int_value;
      break;
    case tinylamb::ValueType::kVarChar:
      a << v.value.varchar_value;
      break;
    case tinylamb::ValueType::kDouble:
      a << v.value.double_value;
      break;
  }
  return a;
}

Decoder& operator>>(Decoder& e, Value& v) {
  e >> v.type;
  switch (v.type) {
    case tinylamb::ValueType::kUnknown:
      break;
    case tinylamb::ValueType::kInt64:
      e >> v.value.int_value;
      break;
    case tinylamb::ValueType::kVarChar:
      e >> v.owned_data;
      v.value.varchar_value = v.owned_data;
      break;
    case tinylamb::ValueType::kDouble:
      e >> v.value.double_value;
      break;
  }
  return e;
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
