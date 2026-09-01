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

enum class UnaryOperation : uint8_t {
  kIsNull,
  kIsNotNull,
  kIsTrue,
  kIsNotTrue,
  kIsFalse,
  kIsNotFalse,
  kNot,
  kMinus,
};

enum class AggregationType : uint8_t {
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
  // Statistical aggregates (long-double accumulation).
  kAnyValue,
  kVarSamp,
  kVarPop,
  kStddevSamp,
  kStddevPop,
  kCovarSamp,
  kCovarPop,
  kCorr,
  // Approximate / sketching aggregates.
  kApproxQuantiles,
  kBitAnd,
  kBitOr,
  kBitXor,
  kArrayConcatAgg,
  kElementwiseSum,
  kElementwiseAvg,
  kApproxTopCount,
  kApproxTopSum,
  kHllInit,
  kHllMerge,
  kHllMergePartial,
  kKllInitInt64,
  kKllInitUint64,
  kKllInitDouble,
  kKllMergePartial,
  kPercentileCont,
  kApproxCountDistinct,
};

bool IsStatisticalAggregate(AggregationType type);
bool IsSketchAggregate(AggregationType type);
bool IsExtendedAggregate(AggregationType type);

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
  // Derived from < and == (not from negating >) so that NaN, which compares
  // false against everything in both < and >, stays consistent: the old
  // `!(a > b)` form made `NaN <= x` true while `NaN < x` and `NaN == x` were
  // both false.
  bool operator<=(const Value& rhs) const {
    return operator<(rhs) || operator==(rhs);
  }
  bool operator>=(const Value& rhs) const {
    return operator>(rhs) || operator==(rhs);
  }

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

// Three-way comparison with SQL ORDER BY semantics among non-NULL values:
// NULL orders below everything, NaN orders directly above NULL and below
// every other number, and cross-type or unordered operands fall back to a
// deterministic total order instead of throwing.
// Returns a negative value when a sorts before b, zero when equal under the
// ordering, positive when a sorts after b.
friend int CompareForOrderBy(const Value& a, const Value& b);

  // Read/Write with type info.
  friend Encoder& operator<<(Encoder& a, const Value& v);
  friend Decoder& operator>>(Decoder& e, Value& v);

  [[nodiscard]] bool IsNull() const { return type == ValueType::kNull; }

  // Collation attachment (COLLATE(value, spec)).  0 = none, 1 = 'binary',
  // 2 = case-insensitive ('und:ci' style).  Comparisons consult the tag when
  // either operand carries it; serialization and hashing ignore it.
  [[nodiscard]] uint8_t Collation() const { return collation_; }
  [[nodiscard]] bool IsCaseInsensitive() const { return collation_ == 2; }
  // UINT64 values share the INT64 storage representation, so retain their
  // SQL signedness in the otherwise-unused collation tag.  This lets joins
  // and comparisons distinguish UINT64(2^64-1) from INT64(-1).
  [[nodiscard]] bool IsUnsigned() const { return collation_ == 3; }
  [[nodiscard]] Value WithUnsigned() const {
    Value copy = *this;
    copy.collation_ = 3;
    return copy;
  }
  [[nodiscard]] Value WithCollation(uint8_t collation) const {
    Value copy = *this;
    copy.collation_ = collation;
    return copy;
  }

  union {
    int64_t int_value;
    std::string_view varchar_value;
    double double_value;
  } value{0};
  ValueType type{ValueType::kNull};
  std::string owned_data;
  std::shared_ptr<ArrayPayload> array_;

 private:
  uint8_t collation_{0};
};

}  // namespace tinylamb

namespace tinylamb {

// Shortest round-trip text for a DOUBLE ("17.5", "0.1", "inf", "nan").
// Shared by struct/JSON encoders so nested values render like the
// compliance goldens instead of fixed-precision "%f" output.
[[nodiscard]] std::string FormatDoubleShortest(double value);

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Value> {
 public:
  uint64_t operator()(const tinylamb::Value& v) const;
};

#endif  // TINYLAMB_VALUE_HPP
