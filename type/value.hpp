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

#ifndef TINYLAMB_VALUE_HPP
#define TINYLAMB_VALUE_HPP

#include <cstdint>

#include "common/serdes.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
class Encoder;
class Decoder;

enum class UnaryOperation : int {
  kIsNull,
  kIsNotNull,
  kNot,
  kMinus,
};

enum class AggregationType : int {
  kCount,
  kSum,
  kAvg,
  kMin,
  kMax,
};

std::string ToString(AggregationType type);
inline std::ostream& operator<<(std::ostream& o, const AggregationType& at) {
  o << ToString(at);
  return o;
}

std::string ToString(UnaryOperation type);
inline std::ostream& operator<<(std::ostream& o, const UnaryOperation& uo) {
  o << ToString(uo);
  return o;
}

class Value {
 public:
  Value() : type(ValueType::kNull) {}
  explicit Value(int int_val);
  explicit Value(int64_t int_val);
  // explicit Value(std::string_view varchar_val);
  explicit Value(std::string&& str_val);
  explicit Value(double double_value);
  Value(const Value& o);
  Value(Value&& o) noexcept;
  explicit Value(long i);
  Value& operator=(const Value& o);
  Value& operator=(Value&& o) noexcept;
  ~Value() = default;

  [[nodiscard]] bool Truthy() const;

  [[nodiscard]] size_t Size() const;

  // Read/Write without type info.
  size_t Serialize(char* dst) const;
  size_t Deserialize(const char* src, ValueType as_type);

  [[nodiscard]] std::string EncodeMemcomparableFormat() const;
  size_t DecodeMemcomparableFormat(const char* src);

  bool operator==(const Value& rhs) const;
  bool operator!=(const Value& rhs) const { return !operator==(rhs); }
  bool operator<(const Value& rhs) const;
  bool operator>(const Value& rhs) const;
  bool operator<=(const Value& rhs) const { return !operator>(rhs); }
  bool operator>=(const Value& rhs) const { return !operator<(rhs); }

  Value operator+(const Value& rhs) const;
  Value operator-(const Value& rhs) const;
  Value operator*(const Value& rhs) const;
  Value operator/(const Value& rhs) const;
  Value operator%(const Value& rhs) const;

  Value operator&(const Value& rhs) const;
  Value operator|(const Value& rhs) const;
  Value operator^(const Value& rhs) const;

  [[nodiscard]] std::string AsString() const;
  friend std::ostream& operator<<(std::ostream& o, const Value& v);

  // Read/Write with type info.
  friend Encoder& operator<<(Encoder& o, const Value& v);
  friend Decoder& operator>>(Decoder& o, Value& v);

  [[nodiscard]] bool IsNull() const { return type == ValueType::kNull; }

  union {
    int64_t int_value;
    std::string_view varchar_value;
    double double_value;
  } value{0};
  ValueType type{ValueType::kNull};
  std::string owned_data;
};

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Value> {
 public:
  uint64_t operator()(const tinylamb::Value& c) const;
};

#endif  // TINYLAMB_VALUE_HPP
