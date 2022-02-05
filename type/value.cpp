//
// Created by kumagi on 2022/02/06.
//
#include "type/value.hpp"

#include <assert.h>

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
}

[[nodiscard]] std::string Value::AsString() const {
  switch (type) {
    case ValueType::kUnknown:
      return "(unknown type)";
    case ValueType::kInt64:
      return std::to_string(value.int_value);
    case ValueType::kVarChar:
      return std::string(value.varchar_value);
    case ValueType::kDouble:
      return std::to_string(value.double_value);
  }
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
}
