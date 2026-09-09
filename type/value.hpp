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

#include "common/exc_shim.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
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
  [[nodiscard]] static StatusOr<Value> TryDate(std::string_view date);
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  [[nodiscard]] static Value Date(std::string_view date) {
    return ExcShimUnwrap(TryDate(date), "Value::Date");
  }
  [[nodiscard]] static Value DateFromDays(int64_t days);
  [[nodiscard]] static Value Array(std::vector<Value> elements,
                                   std::string element_sql_type);
  [[nodiscard]] int64_t DateDays() const;
  [[nodiscard]] bool IsArray() const { return type == ValueType::kArray; }
  [[nodiscard]] const std::vector<Value>& ArrayElements() const;
  [[nodiscard]] const std::string& ArrayElementSqlType() const;
  Value(const Value& o);
  Value(Value&& o) noexcept;

  template <typename I>
  explicit Value(I val)
    requires(std::is_integral<I>::value)
      : type(ValueType::kInt64) {
    value.int_value = static_cast<int64_t>(val);
  }
  Value& operator=(const Value& rhs);
  Value& operator=(Value&& o) noexcept;
  ~Value() = default;

  [[nodiscard]] bool Truthy() const;

  [[nodiscard]] size_t Size() const;
  // True when this value's image fits the on-disk widths (VARCHAR length is
  // prefixed with bin_size_t).  Row::CheckSerializable applies it per column.
  [[nodiscard]] Status CheckSerializable() const;

  // Read/Write without type info.  Serialize assumes a value whose payload
  // fits the on-disk width (see Row::CheckSerializable); the typed
  // read/skip paths report corrupt or mistyped streams as Status.
  size_t Serialize(char* dst) const;
  [[nodiscard]] StatusOr<size_t> TryDeserialize(const char* src,
                                                ValueType as_type);
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  size_t Deserialize(const char* src, ValueType as_type) {
    return ExcShimUnwrap(TryDeserialize(src, as_type), "Value::Deserialize");
  }
  // Advance past a serialized value without constructing it (projection skip).
  [[nodiscard]] static StatusOr<size_t> TrySkipSerialized(const char* src,
                                                          ValueType as_type);
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  [[nodiscard]] static size_t SkipSerialized(const char* src,
                                             ValueType as_type) {
    return ExcShimUnwrap(TrySkipSerialized(src, as_type),
                         "Value::SkipSerialized");
  }

  [[nodiscard]] StatusOr<std::string> TryEncodeMemcomparableFormat() const;
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  [[nodiscard]] std::string EncodeMemcomparableFormat() const {
    return ExcShimUnwrap(TryEncodeMemcomparableFormat(),
                         "Value::EncodeMemcomparableFormat");
  }
  // Decodes one self-delimiting chunk from the (possibly NUL-containing)
  // buffer and returns the number of bytes consumed.  Errors on truncation.
  [[nodiscard]] StatusOr<size_t> TryDecodeMemcomparableFormat(
      std::string_view src);
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  size_t DecodeMemcomparableFormat(std::string_view src) {
    return ExcShimUnwrap(TryDecodeMemcomparableFormat(src),
                         "Value::DecodeMemcomparableFormat");
  }

  bool operator==(const Value& rhs) const;
  bool operator!=(const Value& rhs) const { return !operator==(rhs); }
  [[nodiscard]] StatusOr<bool> TryLess(const Value& rhs) const;
  [[nodiscard]] StatusOr<bool> TryGreater(const Value& rhs) const;
  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
  bool operator<(const Value& rhs) const {
    return ExcShimUnwrap(TryLess(rhs), "Value::operator<");
  }
  bool operator>(const Value& rhs) const {
    return ExcShimUnwrap(TryGreater(rhs), "Value::operator>");
  }
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

  // Arithmetic and bitwise evaluation with SQL semantics: type mismatches,
  // integer overflow and division by zero surface as Status instead of
  // exceptions.
  [[nodiscard]] StatusOr<Value> TryArithmetic(const Value& rhs,
                                              BinaryOperation op) const;

  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
  Value operator+(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kAdd),
                         "Value::operator+");
  }
  Value operator-(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kSubtract),
                         "Value::operator-");
  }
  Value operator*(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kMultiply),
                         "Value::operator*");
  }
  Value operator/(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kDivide),
                         "Value::operator/");
  }
  Value operator%(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kModulo),
                         "Value::operator%");
  }
  Value operator&(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kAnd),
                         "Value::operator&");
  }
  Value operator|(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kOr),
                         "Value::operator|");
  }
  Value operator^(const Value& rhs) const {
    return ExcShimUnwrap(TryArithmetic(rhs, BinaryOperation::kXor),
                         "Value::operator^");
  }

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
