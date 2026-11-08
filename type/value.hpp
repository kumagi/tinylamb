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

#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "common/exc_shim.hpp"
#include "common/platform/endian.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
class Encoder;
class Decoder;
struct ArrayPayload;

inline size_t EncodeMemcomparableFormatIntegerTo(char* dst, int64_t in) {
  dst[0] = static_cast<char>(ValueType::kInt64);
  const uint64_t be = htobe64(static_cast<uint64_t>(in));
  std::memcpy(dst + 1, &be, 8);
  dst[1] ^= static_cast<char>(0x80);
  return 9;
}

inline size_t EncodeMemcomparableFormatUint64To(char* dst, int64_t in) {
  dst[0] = static_cast<char>(6);
  const uint64_t be = htobe64(static_cast<uint64_t>(in));
  std::memcpy(dst + 1, &be, 8);
  return 9;
}

inline size_t EncodeMemcomparableFormatDoubleTo(char* dst, double in) {
  dst[0] = static_cast<char>(ValueType::kDouble);
  if (std::isnan(in)) {
    in = std::numeric_limits<double>::quiet_NaN();
  }
  uint64_t bits = 0;
  std::memcpy(&bits, &in, sizeof(bits));
  const uint64_t code = (in < 0) ? ~bits : (bits | (1ULL << 63));
  const uint64_t be = htobe64(code);
  std::memcpy(dst + 1, &be, 8);
  return 9;
}

enum class UnaryOperation : uint8_t {
  kIsNull,
  kIsNotNull,
  kIsTrue,
  kIsNotTrue,
  kIsFalse,
  kIsNotFalse,
  kNot,
  kMinus,
  kBitwiseNot,
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
  // MODE: most frequent non-NULL value (hash-count accumulation).
  kMode,
  // Linear regression aggregates over paired (y, x) inputs; all share the
  // COVAR/CORR long-double partial sums, y as the child and x as the
  // trailing argument.
  kRegrSlope,
  kRegrIntercept,
  kRegrCount,
  kRegrR2,
  kRegrAvgX,
  kRegrAvgY,
  kRegrSxx,
  kRegrSyy,
  kRegrSxy,
};

bool IsStatisticalAggregate(AggregationType type);
bool IsSketchAggregate(AggregationType type);
bool IsExtendedAggregate(AggregationType type);
// Two-input statistical aggregates (COVAR_*/CORR/REGR_*): the child is y,
// the single trailing argument is x, and rows where either side is NULL are
// skipped.
bool IsTwoInputStatAggregate(AggregationType type);

// SWAR space-presence scan for the string comparison fast paths below: they
// run once per candidate-unequal string pair, where a memchr call per side
// shows up in profiles; a 4-byte-at-a-time branchless byte-equality test
// stays inline and short.
inline bool ViewContainsSpace(std::string_view s) noexcept {
  const char* p = s.data();
  const size_t n = s.size();
  size_t i = 0;
  for (; i + 4 <= n; i += 4) {
    uint32_t chunk = 0;
    std::memcpy(&chunk, p + i, 4);
    const uint32_t marked = chunk ^ 0x20202020U;
    if (((marked - 0x01010101U) & ~marked & 0x80808080U) != 0U) {
      return true;
    }
  }
  for (; i < n; ++i) {
    if (p[i] == ' ') {
      return true;
    }
  }
  return false;
}

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
  // The copy/move quartet below keeps scalars (kNull/kInt64/kDouble/kDate) on
  // a straight-line fast path: scalar payloads carry no heap state, so only
  // value/type/collation move.  The heap paths (VarChar/Array) live in
  // noinline cold functions in value.cpp -- hot scan/fold callers exhaust
  // their inliner budget long before reaching these, and an out-of-line
  // scalar assign showed up as a top profile entry on multi-million-row
  // aggregate folds.  A scalar overwrite leaves any stale owned_data/array_
  // payload in place: it stays unreachable behind the type tag (bounded to
  // one old payload per slot) and the implicit destructor releases it.
#define TINYLAMB_VALUE_SCALAR(t) \
  ((t) != ValueType::kVarChar && (t) != ValueType::kArray)
  [[gnu::always_inline]] Value(const Value& o) : value(o.value), type(o.type) {
    if (TINYLAMB_VALUE_SCALAR(type)) [[likely]] {
      collation_ = o.collation_;
      return;
    }
    InitCopySlow(o);
  }
  [[gnu::always_inline]] Value(Value&& o) noexcept
      : value(o.value), type(o.type) {
    if (TINYLAMB_VALUE_SCALAR(type)) [[likely]] {
      collation_ = o.collation_;
      o.type = ValueType::kNull;
      return;
    }
    InitMoveSlow(o);
  }

  template <typename I>
  explicit Value(I val)
    requires(std::is_integral<I>::value)
      : type(ValueType::kInt64) {
    value.int_value = static_cast<int64_t>(val);
  }
  [[gnu::always_inline]] Value& operator=(const Value& rhs) {
    if (this == &rhs) [[unlikely]] {
      return *this;
    }
    const ValueType copied = rhs.type;
    if (TINYLAMB_VALUE_SCALAR(copied)) [[likely]] {
      type = copied;
      value = rhs.value;
      collation_ = rhs.collation_;
      return *this;
    }
    return CopyAssignSlow(rhs);
  }
  [[gnu::always_inline]] Value& operator=(Value&& o) noexcept {
    if (this == &o) [[unlikely]] {
      return *this;
    }
    const ValueType moved = o.type;
    if (TINYLAMB_VALUE_SCALAR(moved)) [[likely]] {
      type = moved;
      value = o.value;
      collation_ = o.collation_;
      o.type = ValueType::kNull;
      return *this;
    }
    return MoveAssignSlow(o);
  }
#undef TINYLAMB_VALUE_SCALAR
  // ~Value stays the implicit member-wise destroy: scalar assigns above may
  // leave a stale owned_data/array_ payload behind (unreachable behind the
  // type tag, bounded to one old payload per slot), and the implicit dtor is
  // what releases it.

 private:
  // Cold halves of the quartet (defined out-of-line in value.cpp): keeping
  // the heap branches out of the always_inline header bodies is what makes
  // every hot call site a straight-line scalar move.
  void InitCopySlow(const Value& o);
  void InitMoveSlow(Value& o) noexcept;
  Value& CopyAssignSlow(const Value& rhs);
  Value& MoveAssignSlow(Value& o) noexcept;

 public:
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
  // Hot path (row decode): fill this Value directly from `src` without the
  // StatusOr/optional machinery on the success path.  Returns bytes consumed,
  // or 0 with *out_status set on error (this Value is then partially filled;
  // callers discard the row).  Identical stream semantics to TryDeserialize.
  size_t DeserializeInto(const char* src, ValueType as_type,
                         Status* out_status);
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  size_t Deserialize(const char* src, ValueType as_type) {
    return ExcShimUnwrap(TryDeserialize(src, as_type), "Value::Deserialize");
  }
  // Advance past a serialized value without constructing it (projection skip).
  [[nodiscard]] static StatusOr<size_t> TrySkipSerialized(const char* src,
                                                          ValueType as_type);
  // Hot path (row decode): TrySkipSerialized without StatusOr machinery.
  // Returns bytes consumed, or 0 with *out_status set on error.
  static size_t SkipSerializedInto(const char* src, ValueType as_type,
                                   Status* out_status);
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
  size_t EncodeMemcomparableFormatTo(char* dst) const;
  static size_t DecodeMemcomparableFormatBounded(const char* src, size_t avail);
  // Decodes one self-delimiting chunk from the (possibly NUL-containing)
  // buffer and returns the number of bytes consumed.  Errors on truncation.
  [[nodiscard]] StatusOr<size_t> TryDecodeMemcomparableFormat(
      std::string_view src);
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
  size_t DecodeMemcomparableFormat(std::string_view src) {
    return ExcShimUnwrap(TryDecodeMemcomparableFormat(src),
                         "Value::DecodeMemcomparableFormat");
  }

  // Hot path: the homogeneous scalar cases (the ones scan filters, join keys
  // and group keys hammer) resolve inline; Null/VarChar/Array tail into
  // EqualsSlow.
  bool operator==(const Value& rhs) const {
    if (type != rhs.type) [[unlikely]] {
      return false;
    }
    switch (type) {
      case ValueType::kInt64:
        if (IsUnsigned() != rhs.IsUnsigned()) [[unlikely]] {
          break;
        }
        return value.int_value == rhs.value.int_value;
      case ValueType::kDate:
        return value.int_value == rhs.value.int_value;
      case ValueType::kDouble: {
        const double l = value.double_value;
        const double r = rhs.value.double_value;
        if (l == r) {
          return true;
        }
        // Exact equality canonicalized like std::hash<Value>: every NaN
        // compares equal to every other NaN; -0.0 equals +0.0 via ==.
        return std::isnan(l) && std::isnan(r);
      }
      case ValueType::kVarChar: {
        // Byte-equal text is equal; byte-unequal text can still be
        // interval-equivalent, but only when both sides carry
        // interval-shaped text and every interval rendering embeds a
        // space -- so a space-free side settles the comparison without
        // the EqualsSlow call.
        const std::string_view l = value.varchar_value;
        const std::string_view r = rhs.value.varchar_value;
        if (l == r) [[likely]] {
          return true;
        }
        const auto may_be_interval = [](std::string_view s) noexcept {
          return s.size() >= 3 &&
                 ((s[0] >= '0' && s[0] <= '9') || s[0] == '-' || s[0] == '+' ||
                  s[0] == ' ') &&
                 ViewContainsSpace(s);
        };
        if (!may_be_interval(l) || !may_be_interval(r)) [[likely]] {
          return false;
        }
        break;
      }
      default:
        break;
    }
    return EqualsSlow(rhs);
  }
  [[nodiscard]] bool EqualsSlow(const Value& rhs) const;
  bool operator!=(const Value& rhs) const { return !operator==(rhs); }
  [[nodiscard]] StatusOr<bool> TryLess(const Value& rhs) const;
  [[nodiscard]] StatusOr<bool> TryGreater(const Value& rhs) const;
  // Hot path: homogeneous scalar ordering (statistics tracking, sort keys)
  // resolves inline; mismatched types, NULLs, unsigned tags, collation,
  // interval-shaped strings and arrays tail into LessSlow.
  bool operator<(const Value& rhs) const {
    if (type == rhs.type) [[likely]] {
      switch (type) {
        case ValueType::kInt64:
          if (IsUnsigned() || rhs.IsUnsigned()) [[unlikely]] {
            break;
          }
          return value.int_value < rhs.value.int_value;
        case ValueType::kDate:
          return value.int_value < rhs.value.int_value;
        case ValueType::kDouble:
          return value.double_value < rhs.value.double_value;
        case ValueType::kVarChar: {
          // Byte order first; interval equivalence can only reorder
          // interval-shaped texts and every interval rendering embeds a
          // space, so a space-free side makes the byte order final
          // without the LessSlow (StatusOr machinery) call.
          const std::string_view l = value.varchar_value;
          const std::string_view r = rhs.value.varchar_value;
          const int cmp = l.compare(r);
          if (cmp == 0) {
            return false;
          }
          if (!ViewContainsSpace(l) || !ViewContainsSpace(r)) [[likely]] {
            return cmp < 0;
          }
          break;
        }
        default:
          break;
      }
    }
    return LessSlow(rhs);
  }
  [[nodiscard]] bool LessSlow(const Value& rhs) const;
  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
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
  [[nodiscard]] int64_t AsInt64() const {
    if (type != ValueType::kInt64 && type != ValueType::kDate) ThrowAsInt64();
    return value.int_value;
  }
  [[nodiscard]] double AsDouble() const {
    if (type != ValueType::kDouble) ThrowAsDouble();
    return value.double_value;
  }
  [[nodiscard]] std::string_view AsStringView() const {
    if (type != ValueType::kVarChar) ThrowAsStringView();
    return value.varchar_value;
  }
  [[noreturn]] static void ThrowAsInt64();
  [[noreturn]] static void ThrowAsDouble();
  [[noreturn]] static void ThrowAsStringView();
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

  void SetNull() {
    type = ValueType::kNull;
    collation_ = 0;
  }
  void SetInt64(int64_t v) {
    type = ValueType::kInt64;
    value.int_value = v;
    collation_ = 0;
  }
  void SetDouble(double d) {
    type = ValueType::kDouble;
    value.double_value = d;
    collation_ = 0;
  }
  void SetString(std::string&& s) {
    owned_data = std::move(s);
    type = ValueType::kVarChar;
    value.varchar_value = owned_data;
    collation_ = 0;
  }
  void SetStringView(std::string_view s) {
    owned_data.assign(s);
    type = ValueType::kVarChar;
    value.varchar_value = owned_data;
    collation_ = 0;
  }
  [[nodiscard]] std::string MoveString() { return std::move(owned_data); }

  // Collation attachment (COLLATE(value, spec)).  0 = none, 1 = 'binary',
  // 2 = case-insensitive ('und:ci' style).  Comparisons consult the tag when
  // either operand carries it; serialization and hashing ignore it.
  [[nodiscard]] uint8_t Collation() const { return collation_; }
  void SetCollation(uint8_t collation) { collation_ = collation; }
  [[nodiscard]] bool IsCaseInsensitive() const { return collation_ == 2; }
  // UINT64 values share the INT64 storage representation, so retain their
  // SQL signedness in the otherwise-unused collation tag.  This lets joins
  // and comparisons distinguish UINT64(2^64-1) from INT64(-1).
  [[nodiscard]] bool IsUnsigned() const { return collation_ == 3; }
  // Hot path: tag in place (WithUnsigned copies the whole value).
  void SetUnsigned() { collation_ = 3; }
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

  // Initialized through the widest member so every byte is defined: `{0}`
  // would set only the 8-byte int_value and leave half of the 16-byte
  // string_view indeterminate. libstdc++ lays string_view out as {size, ptr}
  // (so a NULL value happened to read as ""), libc++ as {ptr, size} (so it
  // read a garbage length). int_value and double_value still read as 0.
  union {
    int64_t int_value;
    std::string_view varchar_value;
    double double_value;
  } value{.varchar_value = std::string_view{}};
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
