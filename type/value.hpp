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
#include <memory>
#include <vector>

#include "common/serdes.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
class Encoder;
class Decoder;
struct ArrayPayload;

enum class UnaryOperation : int {
  kIsNull,
  kIsNotNull,
  kIsTrue,
  kIsNotTrue,
  kIsFalse,
  kIsNotFalse,
  kNot,
  kMinus,
};

enum class AggregationType : int {
  kCount,
  kSum,
  kAvg,
  kMin,
  kMax,
  kLogicalAnd,
  kLogicalOr,
  kArrayAgg,
  kStringAgg,
  kCountIf,
  kAnyValue,
  kVarPop,
  kVarSamp,
  kStddevPop,
  kStddevSamp,
  kCovarPop,
  kCovarSamp,
  kCorr,
  kBitAnd,
  kBitOr,
  kBitXor,
  kElementwiseSum,
  kElementwiseAvg,
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
  explicit Value(std::string&& str_val);
  explicit Value(double double_value);
  [[nodiscard]] static Value Date(std::string_view date);
  [[nodiscard]] static Value DateFromDays(int64_t days);
  [[nodiscard]] static Value Array(std::vector<Value> elements,
                                   std::string element_sql_type);
  [[nodiscard]] int64_t DateDays() const;
  [[nodiscard]] bool IsArray() const { return type == ValueType::kArray; }
  [[nodiscard]] const std::vector<Value>& ArrayElements() const;
  [[nodiscard]] const std::string& ArrayElementSqlType() const;
  Value(const Value& o);
  Value(Value&& o) noexcept;

  template <typename I,
            typename std::enable_if<std::is_integral<I>::value, int>::type = 0>
  explicit Value(I val) : type(ValueType::kInt64) {
    value.int_value = static_cast<int64_t>(val);
  }
  Value& operator=(const Value& rhs);
  Value& operator=(Value&& o) noexcept;
  ~Value() = default;

  [[nodiscard]] bool Truthy() const;

  [[nodiscard]] size_t Size() const;

  // Read/Write without type info.
  size_t Serialize(char* dst) const;
  size_t Deserialize(const char* src, ValueType as_type);
  // Advance past a serialized value without constructing it (projection skip).
  [[nodiscard]] static size_t SkipSerialized(const char* src,
                                             ValueType as_type);

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
  friend Encoder& operator<<(Encoder& a, const Value& v);
  friend Decoder& operator>>(Decoder& e, Value& v);

  [[nodiscard]] bool IsNull() const { return type == ValueType::kNull; }

  union {
    int64_t int_value;
    std::string_view varchar_value;
    double double_value;
  } value{0};
  ValueType type{ValueType::kNull};
  std::string owned_data;
  std::shared_ptr<ArrayPayload> array_;
};

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Value> {
 public:
  uint64_t operator()(const tinylamb::Value& v) const;
};

#endif  // TINYLAMB_VALUE_HPP
