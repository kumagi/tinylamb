//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_VALUE_HPP
#define TINYLAMB_VALUE_HPP

#include <cstdint>
#include "value_type.hpp"

namespace tinylamb {

class Value {
 public:
  Value() {}
  Value(int64_t int_val) {
    value.int_value = int_val;
    length = 8;
  }
  Value(std::string_view varchar_val) {
    value.varchar_value = varchar_val.data();
    length = varchar_val.length();
  }

  [[nodiscard]] size_t Size() const {
    return length;
  }
  
  template <typename T>
  T Read(ValueType as) {
    return *reinterpret_cast<const T*>(&value);
  }

  std::string AsString(ValueType type) {
    switch (type) {
      case ValueType::kUnknown:
        assert(!"unknown type cannot be string");
      case ValueType::kInt64:
        return std::to_string(value.int_value);
      case ValueType::kVarChar:
        return std::string(value.varchar_value, length);
    }
  }

  union {
    int64_t int_value;
    const char* varchar_value;
  } value;

  size_t length;
};

}  // namespace tinylamb

#endif  // TINYLAMB_VALUE_HPP
