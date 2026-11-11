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

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/json_string.hpp"
#include "common/log_message.hpp"
#include "common/platform/endian.hpp"
#include "common/serdes.hpp"
#include "type/collation.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

struct ValuePayload {
  explicit ValuePayload(bool is_array) : is_array(is_array) {}
  std::atomic<size_t> references{1};
  bool is_array;
};

struct StringPayload : ValuePayload {
  explicit StringPayload(std::string text)
      : ValuePayload(false), text(std::move(text)) {}
  std::string text;
  // Zero means uncomputed. UINT64_MAX disables caching after exposing a
  // mutable string reference, which a caller may retain across hash calls.
  // Shared immutable strings may be hashed by several execution workers.
  std::atomic<uint64_t> hash{0};
};

struct ArrayPayload : ValuePayload {
  ArrayPayload() : ValuePayload(true) {}
  std::string element_sql_type;
  std::vector<Value> elements;
};

OwnedValuePayload::OwnedValuePayload(std::string text) {
  if (!text.empty()) {
    payload_ = std::make_unique<StringPayload>(std::move(text)).release();
  }
}

OwnedValuePayload::OwnedValuePayload(const OwnedValuePayload& other)
    : payload_(other.payload_) {
  if (payload_ != nullptr) {
    payload_->references.fetch_add(1, std::memory_order_relaxed);
  }
}

OwnedValuePayload& OwnedValuePayload::operator=(
    const OwnedValuePayload& other) {
  if (this == &other) {
    return *this;
  }
  if (payload_ != other.payload_) {
    ValuePayload* next = other.payload_;
    if (next != nullptr) {
      next->references.fetch_add(1, std::memory_order_relaxed);
    }
    clear();
    payload_ = next;
  }
  return *this;
}

OwnedValuePayload& OwnedValuePayload::operator=(std::string text) {
  Mutable() = std::move(text);
  return *this;
}

void OwnedValuePayload::Release() noexcept {
  ValuePayload* previous = std::exchange(payload_, nullptr);
  if (previous->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    if (previous->is_array) {
      const std::unique_ptr<ArrayPayload> last_owner(
          static_cast<ArrayPayload*>(previous));
    } else {
      const std::unique_ptr<StringPayload> last_owner(
          static_cast<StringPayload*>(previous));
    }
  }
}

bool OwnedValuePayload::empty() const {
  return payload_ == nullptr || payload_->is_array ||
         static_cast<const StringPayload*>(payload_)->text.empty();
}

OwnedValuePayload::operator std::string_view() const {
  return payload_ == nullptr || payload_->is_array
             ? std::string_view("")
             : static_cast<const StringPayload*>(payload_)->text;
}

std::string& OwnedValuePayload::Mutable() {
  if (payload_ == nullptr || payload_->is_array) {
    auto next = std::make_unique<StringPayload>(std::string{});
    clear();
    payload_ = next.release();
  } else if (payload_->references.load(std::memory_order_acquire) != 1) {
    auto next = std::make_unique<StringPayload>(
        static_cast<const StringPayload*>(payload_)->text);
    Release();
    payload_ = next.release();
  }
  auto* string = static_cast<StringPayload*>(payload_);
  string->hash.store(UINT64_MAX, std::memory_order_relaxed);
  return string->text;
}

void OwnedValuePayload::assign(std::string_view text) {
  // Copy before releasing shared storage: text can refer to our old buffer,
  // including a string nested inside an array being overwritten.
  if (payload_ != nullptr && !payload_->is_array &&
      payload_->references.load(std::memory_order_acquire) == 1) {
    auto* string = static_cast<StringPayload*>(payload_);
    if (string->hash.load(std::memory_order_relaxed) != UINT64_MAX) {
      string->hash.store(0, std::memory_order_relaxed);
    }
    string->text.assign(text);
  } else {
    OwnedValuePayload next{std::string(text)};
    *this = std::move(next);
  }
}

std::string OwnedValuePayload::Take() {
  if (payload_ == nullptr || payload_->is_array) {
    return {};
  }
  auto& text = static_cast<StringPayload*>(payload_)->text;
  if (payload_->references.load(std::memory_order_acquire) == 1) {
    static_cast<StringPayload*>(payload_)->hash.store(
        UINT64_MAX, std::memory_order_relaxed);
    return std::move(text);
  }
  return text;
}

void OwnedValuePayload::SetArray(
    std::unique_ptr<ArrayPayload> payload) noexcept {
  clear();
  payload_ = payload.release();
}

ArrayPayload* OwnedValuePayload::Array() const noexcept {
  return payload_ != nullptr && payload_->is_array
             ? static_cast<ArrayPayload*>(payload_)
             : nullptr;
}

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
    case UnaryOperation::kBitwiseNot:
      return "~";
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
    case AggregationType::kAnyValue:
      return "ANY_VALUE";
    case AggregationType::kVarSamp:
      return "VAR_SAMP";
    case AggregationType::kVarPop:
      return "VAR_POP";
    case AggregationType::kStddevSamp:
      return "STDDEV_SAMP";
    case AggregationType::kStddevPop:
      return "STDDEV_POP";
    case AggregationType::kCovarSamp:
      return "COVAR_SAMP";
    case AggregationType::kCovarPop:
      return "COVAR_POP";
    case AggregationType::kCorr:
      return "CORR";
    case AggregationType::kApproxQuantiles:
      return "APPROX_QUANTILES";
    case AggregationType::kBitAnd:
      return "BIT_AND";
    case AggregationType::kBitOr:
      return "BIT_OR";
    case AggregationType::kBitXor:
      return "BIT_XOR";
    case AggregationType::kArrayConcatAgg:
      return "ARRAY_CONCAT_AGG";
    case AggregationType::kElementwiseSum:
      return "ELEMENTWISE_SUM";
    case AggregationType::kElementwiseAvg:
      return "ELEMENTWISE_AVG";
    case AggregationType::kApproxTopCount:
      return "APPROX_TOP_COUNT";
    case AggregationType::kApproxTopSum:
      return "APPROX_TOP_SUM";
    case AggregationType::kHllInit:
      return "HLL_COUNT.INIT";
    case AggregationType::kHllMerge:
      return "HLL_COUNT.MERGE";
    case AggregationType::kHllMergePartial:
      return "HLL_COUNT.MERGE_PARTIAL";
    case AggregationType::kKllInitInt64:
      return "KLL_QUANTILES.INIT_INT64";
    case AggregationType::kKllInitUint64:
      return "KLL_QUANTILES.INIT_UINT64";
    case AggregationType::kKllInitDouble:
      return "KLL_QUANTILES.INIT_DOUBLE";
    case AggregationType::kKllMergePartial:
      return "KLL_QUANTILES.MERGE_PARTIAL";
    case AggregationType::kPercentileCont:
      return "PERCENTILE_CONT";
    case AggregationType::kApproxCountDistinct:
      return "APPROX_COUNT_DISTINCT";
    case AggregationType::kMode:
      return "MODE";
    case AggregationType::kRegrSlope:
      return "REGR_SLOPE";
    case AggregationType::kRegrIntercept:
      return "REGR_INTERCEPT";
    case AggregationType::kRegrCount:
      return "REGR_COUNT";
    case AggregationType::kRegrR2:
      return "REGR_R2";
    case AggregationType::kRegrAvgX:
      return "REGR_AVGX";
    case AggregationType::kRegrAvgY:
      return "REGR_AVGY";
    case AggregationType::kRegrSxx:
      return "REGR_SXX";
    case AggregationType::kRegrSyy:
      return "REGR_SYY";
    case AggregationType::kRegrSxy:
      return "REGR_SXY";
    default:
      return "UNKNOWN";
  }
}

bool IsStatisticalAggregate(AggregationType type) {
  switch (type) {
    case AggregationType::kVarSamp:
    case AggregationType::kVarPop:
    case AggregationType::kStddevSamp:
    case AggregationType::kStddevPop:
    case AggregationType::kCovarSamp:
    case AggregationType::kCovarPop:
    case AggregationType::kCorr:
    case AggregationType::kRegrSlope:
    case AggregationType::kRegrIntercept:
    case AggregationType::kRegrCount:
    case AggregationType::kRegrR2:
    case AggregationType::kRegrAvgX:
    case AggregationType::kRegrAvgY:
    case AggregationType::kRegrSxx:
    case AggregationType::kRegrSyy:
    case AggregationType::kRegrSxy:
      return true;
    default:
      return false;
  }
}

bool IsTwoInputStatAggregate(AggregationType type) {
  switch (type) {
    case AggregationType::kCovarSamp:
    case AggregationType::kCovarPop:
    case AggregationType::kCorr:
    case AggregationType::kRegrSlope:
    case AggregationType::kRegrIntercept:
    case AggregationType::kRegrCount:
    case AggregationType::kRegrR2:
    case AggregationType::kRegrAvgX:
    case AggregationType::kRegrAvgY:
    case AggregationType::kRegrSxx:
    case AggregationType::kRegrSyy:
    case AggregationType::kRegrSxy:
      return true;
    default:
      return false;
  }
}

bool IsSketchAggregate(AggregationType type) {
  switch (type) {
    case AggregationType::kApproxQuantiles:
    case AggregationType::kApproxTopCount:
    case AggregationType::kApproxTopSum:
    case AggregationType::kHllInit:
    case AggregationType::kHllMerge:
    case AggregationType::kHllMergePartial:
    case AggregationType::kKllInitInt64:
    case AggregationType::kKllInitUint64:
    case AggregationType::kKllInitDouble:
    case AggregationType::kKllMergePartial:
      return true;
    default:
      return false;
  }
}

// Everything from ANY_VALUE onward is an extended aggregate implemented by
// the relational accumulator only.
bool IsExtendedAggregate(AggregationType type) {
  return type >= AggregationType::kAnyValue;
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

StatusOr<Value> Value::TryDate(std::string_view date) {
  ASSIGN_OR_RETURN(int64_t, days, TryParseDateDays(date));
  return DateFromDays(days);
}

Value Value::DateFromDays(int64_t days) {
  Value result;
  result.type = ValueType::kDate;
  result.value.int_value = days;
  return result;
}

int64_t Value::DateDays() const {
  CHECK_MSG(type == ValueType::kDate, "DATE value required");
  return value.int_value;
}

Value Value::Array(std::vector<Value> elements, std::string element_sql_type) {
  Value result;
  result.type = ValueType::kArray;
  result.owned_data.SetArray(std::make_unique<ArrayPayload>());
  result.owned_data.Array()->element_sql_type = std::move(element_sql_type);
  result.owned_data.Array()->elements = std::move(elements);
  return result;
}

const std::vector<Value>& Value::ArrayElements() const {
  if (owned_data.Array() == nullptr) {
    return kEmptyArrayElements;
  }
  return owned_data.Array()->elements;
}

const std::string& Value::ArrayElementSqlType() const {
  static const std::string kEmpty;
  if (owned_data.Array() == nullptr) {
    return kEmpty;
  }
  return owned_data.Array()->element_sql_type;
}

// The scalar halves of the copy/move quartet are always_inline in value.hpp;
// these are the cold VarChar/Array halves (kept out of line so every hot call
// site is a straight-line scalar move).  VarChar keeps the materialize-on-move
// contract: a view into a page buffer must not outlive the pin, so it lands
// in owned_data here.

void Value::InitCopySlow(const Value& o) {
  collation_ = o.collation_;
  if (type == ValueType::kVarChar) {
    if (!o.owned_data.empty()) {
      owned_data = o.owned_data;
    } else {
      owned_data.assign(o.value.varchar_value);
    }
    value.varchar_value = owned_data;
  } else {
    owned_data = o.owned_data;
  }
}

void Value::InitMoveSlow(Value& o) noexcept {
  collation_ = o.collation_;
  if (type == ValueType::kVarChar) {
    owned_data = std::move(o.owned_data);
    if (owned_data.empty() && !o.value.varchar_value.empty()) {
      owned_data.assign(o.value.varchar_value);
    }
    value.varchar_value = owned_data;
  } else {
    owned_data = std::move(o.owned_data);
  }
  o.type = ValueType::kNull;
}

Value& Value::CopyAssignSlow(const Value& rhs) {
  type = rhs.type;
  value = rhs.value;
  collation_ = rhs.collation_;
  if (rhs.type == ValueType::kVarChar) {
    if (!rhs.owned_data.empty()) {
      owned_data = rhs.owned_data;
    } else {
      owned_data.assign(rhs.value.varchar_value);
    }
    value.varchar_value = owned_data;
  } else {
    owned_data = rhs.owned_data;
  }
  return *this;
}

Value& Value::MoveAssignSlow(Value& o) noexcept {
  type = o.type;
  value = o.value;
  collation_ = o.collation_;
  if (o.type == ValueType::kVarChar) {
    owned_data = std::move(o.owned_data);
    if (owned_data.empty() && !o.value.varchar_value.empty()) {
      owned_data.assign(o.value.varchar_value);
    }
    value.varchar_value = owned_data;
  } else {
    owned_data = std::move(o.owned_data);
  }
  o.type = ValueType::kNull;
  return *this;
}

std::string Value::MoveString() {
  if (type != ValueType::kVarChar) {
    ThrowAsStringView();
  }
  const std::string_view owned = owned_data;
  const std::string_view visible = value.varchar_value;
  std::string result =
      owned.data() == visible.data() && owned.size() == visible.size()
          ? owned_data.Take()
          : std::string(visible);
  type = ValueType::kNull;
  collation_ = 0;
  value.int_value = 0;
  return result;
}

bool Value::Truthy() const {
  if (IsNull()) {
    return false;
  }
  if (type == ValueType::kInt64) {
    return value.int_value != 0;
  }
  if (type == ValueType::kDouble) {
    // Zero (either sign) is false; NaN is true by IEEE semantics (it is
    // "non-zero").  Before this, DOUBLE fell into the blanket `return true`
    // below, so `(0.0 OR x)` short-circuited to TRUE in the AST while the
    // same expression on INT64 operands was FALSE (fuzz-found divergence).
    return value.double_value != 0.0;
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
        if (!element.IsNull()) {
          bytes += 1 + element.Size();
        }
      }
      return bytes;
    }
  }
  CHECK_MSG(false, "undefined type");
  return 0;
}

Status Value::CheckSerializable() const {
  switch (type) {
    case ValueType::kNull:
    case ValueType::kInt64:
    case ValueType::kDate:
    case ValueType::kDouble:
      return Status::kSuccess;
    case ValueType::kVarChar:
      if (value.varchar_value.size() > std::numeric_limits<bin_size_t>::max()) {
        return StatusError(StatusCode::kTooBigData,
                           "string too long to serialize");
      }
      return Status::kSuccess;
    case ValueType::kArray:
      if (ArrayElementSqlType().size() >
          std::numeric_limits<bin_size_t>::max()) {
        return StatusError(StatusCode::kTooBigData,
                           "array type name too long to serialize");
      }
      for (const Value& element : ArrayElements()) {
        RETURN_IF_FAIL(element.CheckSerializable());
      }
      return Status::kSuccess;
  }
  CHECK_MSG(false, "undefined type");
  return Status::kSuccess;
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
        *cursor++ = static_cast<char>(static_cast<uint8_t>(element.type) |
                                      (element.Collation() << 3));
        cursor += element.Serialize(cursor);
      }
      return static_cast<size_t>(cursor - dst);
    }
  }
  CHECK_MSG(false, "undefined type");
  return 0;
}

size_t Value::DeserializeInto(const char* src, ValueType as_type,
                              Status* out_status) {
  type = as_type;
  collation_ = 0;
  switch (as_type) {
    case ValueType::kNull:
      *out_status = StatusError(StatusCode::kInvalidArgument,
                                "Cannot parse without type.");
      return 0;
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
      static thread_local size_t deserialize_depth = 0;
      if (deserialize_depth >= 32) {
        *out_status = StatusError(StatusCode::kCorrupt,
                                  "corrupt array: nesting depth exceeded");
        return 0;
      }
      struct DepthGuard {
        DepthGuard(const DepthGuard&) = delete;
        DepthGuard(DepthGuard&&) = delete;
        DepthGuard& operator=(const DepthGuard&) = delete;
        DepthGuard& operator=(DepthGuard&&) = delete;
        DepthGuard() { ++deserialize_depth; }
        ~DepthGuard() { --deserialize_depth; }
      } guard;
      owned_data.clear();
      const char* cursor = src;
      uint32_t count = 0;
      cursor += DeserializeU32(cursor, &count);
      constexpr uint32_t kMaxEncodedArrayElements = 1U << 20;
      if (count > kMaxEncodedArrayElements) {
        *out_status =
            StatusError(StatusCode::kCorrupt, "corrupt array element count");
        return 0;
      }
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
        const auto encoded_type = static_cast<uint8_t>(*cursor++);
        if ((encoded_type >> 3) > 4) {
          *out_status = Status::kCorrupt;
          return 0;
        }
        const auto elem_type = static_cast<ValueType>(encoded_type & 7);
        Value& element = elements.emplace_back();
        const size_t consumed =
            element.DeserializeInto(cursor, elem_type, out_status);
        element.SetCollation(encoded_type >> 3);
        if (*out_status != Status::kSuccess) {
          return 0;
        }
        cursor += consumed;
      }
      owned_data.SetArray(std::make_unique<ArrayPayload>());
      owned_data.Array()->element_sql_type = std::string(sql_type);
      owned_data.Array()->elements = std::move(elements);
      return static_cast<size_t>(cursor - src);
    }
  }
  *out_status = StatusError(StatusCode::kCorrupt, "undefined type");
  return 0;
}

StatusOr<size_t> Value::TryDeserialize(const char* src, ValueType as_type) {
  Status status = Status::kSuccess;
  const size_t consumed = DeserializeInto(src, as_type, &status);
  if (status != Status::kSuccess) {
    return status;
  }
  return consumed;
}

size_t Value::SkipSerializedInto(const char* src, ValueType as_type,
                                 Status* out_status) {
  switch (as_type) {
    case ValueType::kNull:
      *out_status = StatusError(StatusCode::kInvalidArgument,
                                "Cannot skip without type.");
      return 0;
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
      static thread_local size_t skip_depth = 0;
      if (skip_depth >= 32) {
        *out_status = StatusError(StatusCode::kCorrupt,
                                  "corrupt array: nesting depth exceeded");
        return 0;
      }
      struct DepthGuard {
        DepthGuard(const DepthGuard&) = delete;
        DepthGuard(DepthGuard&&) = delete;
        DepthGuard& operator=(const DepthGuard&) = delete;
        DepthGuard& operator=(DepthGuard&&) = delete;
        DepthGuard() { ++skip_depth; }
        ~DepthGuard() { --skip_depth; }
      } guard;
      const char* cursor = src;
      uint32_t count = 0;
      cursor += DeserializeU32(cursor, &count);
      constexpr uint32_t kMaxEncodedArrayElements = 1U << 20;
      if (count > kMaxEncodedArrayElements) {
        *out_status =
            StatusError(StatusCode::kCorrupt, "corrupt array element count");
        return 0;
      }
      std::string_view sql_type;
      cursor += DeserializeStringView(cursor, &sql_type);
      for (uint32_t i = 0; i < count; ++i) {
        const bool present = *cursor++ != 0;
        if (!present) {
          continue;
        }
        const auto encoded_type = static_cast<uint8_t>(*cursor++);
        if ((encoded_type >> 3) > 4) {
          *out_status = Status::kCorrupt;
          return 0;
        }
        const auto elem_type = static_cast<ValueType>(encoded_type & 7);
        const size_t consumed =
            SkipSerializedInto(cursor, elem_type, out_status);
        if (*out_status != Status::kSuccess) {
          return 0;
        }
        cursor += consumed;
      }
      return static_cast<size_t>(cursor - src);
    }
  }
  *out_status = StatusError(StatusCode::kCorrupt, "undefined type");
  return 0;
}

StatusOr<size_t> Value::TrySkipSerialized(const char* src, ValueType as_type) {
  Status status = Status::kSuccess;
  const size_t skipped = SkipSerializedInto(src, as_type, &status);
  if (status != Status::kSuccess) {
    return status;
  }
  return skipped;
}

std::string FormatDoubleShortest(double value) {
  if (std::isnan(value)) {
    return "nan";
  }
  if (std::isinf(value)) {
    return value > 0 ? "inf" : "-inf";
  }
  std::array<char, 64> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (ec != std::errc()) {
    return std::to_string(value);
  }
  return {buffer.data(), static_cast<size_t>(ptr - buffer.data())};
}

[[nodiscard]] std::string Value::AsString() const {
  switch (type) {
    case ValueType::kNull:
      return "NULL";
    case ValueType::kInt64:
      if (IsUnsigned()) {
        return std::to_string(static_cast<uint64_t>(value.int_value));
      }
      return std::to_string(value.int_value);
    case ValueType::kDate:
      return FormatDateDays(value.int_value);
    case ValueType::kVarChar:
      return "\"" + std::string(value.varchar_value) + "\"";
    case ValueType::kDouble: {
      if (std::isnan(value.double_value)) {
        return "nan";
      }
      if (std::isinf(value.double_value)) {
        return value.double_value > 0 ? "inf" : "-inf";
      }
      std::array<char, 64> buffer{};
      auto [ptr, ec] = std::to_chars(
          buffer.data(), buffer.data() + buffer.size(), value.double_value);
      return {buffer.data(), static_cast<size_t>(ptr - buffer.data())};
    }

    case ValueType::kArray: {
      std::string out = "ARRAY<" + ArrayElementSqlType() + ">[";
      const auto& elements = ArrayElements();
      out.reserve(out.size() + (elements.size() * 8) + 2);
      for (size_t i = 0; i < elements.size(); ++i) {
        if (i != 0) {
          out += ", ";
        }
        if (elements[i].type == ValueType::kVarChar) {
          json_text::AppendJsonString(out, elements[i].AsStringView());
        } else {
          out += elements[i].IsNull() ? "NULL" : elements[i].AsString();
        }
      }
      out += "]";
      return out;
    }
  }
  CHECK_MSG(false, "undefined type");
  return "";
}

namespace {
std::optional<IntervalValue> ParseAsInterval(std::string_view s) {
  if (auto parsed = IntervalValue::TryParse(s); parsed.HasValue()) {
    return parsed.MoveValue();
  }
  {
    return std::nullopt;
  }
}
}  // namespace

uint64_t OwnedValuePayload::StringHash(std::string_view text) const {
  StringPayload* owned = payload_ != nullptr && !payload_->is_array
                             ? static_cast<StringPayload*>(payload_)
                             : nullptr;
  // Values can carry borrowed views (including slices of another value).
  // Only cache a hash when the view denotes this owner's complete string.
  if (owned != nullptr && (owned->text.data() != text.data() ||
                           owned->text.size() != text.size())) {
    owned = nullptr;
  }
  if (owned != nullptr) {
    const uint64_t cached = owned->hash.load(std::memory_order_relaxed);
    if (cached == UINT64_MAX) {
      owned = nullptr;
    } else if (cached != 0) {
      return cached;
    }
  }
  const uint64_t hash = std::hash<std::string_view>()(text);
  if (owned != nullptr && hash != UINT64_MAX) {
    owned->hash.store(hash, std::memory_order_relaxed);
  }
  return hash;
}

bool Value::EqualsSlow(const Value& rhs) const {
  if (type != rhs.type) {
    return false;
  }
  switch (type) {
    case ValueType::kNull:
      return true;
    case ValueType::kInt64:
      if (IsUnsigned() != rhs.IsUnsigned()) {
        if (value.int_value < 0 || rhs.value.int_value < 0) {
          return false;
        }
      }
      return value.int_value == rhs.value.int_value;
    case ValueType::kDate:
      return value.int_value == rhs.value.int_value;
    case ValueType::kVarChar: {
      std::string_view sv1 = value.varchar_value;
      std::string_view sv2 = rhs.value.varchar_value;
      if (IsInterval() != rhs.IsInterval()) {
        return false;
      }
      if (sv1 == sv2) {
        return true;
      }
      // Only tagged INTERVAL values use interval equivalence. Canonical
      // renderings contain a space; ordinary strings retain byte equality.
      if (!ViewContainsSpace(sv1) || !ViewContainsSpace(sv2)) {
        return false;
      }
      if (IsInterval() && rhs.IsInterval()) {
        const auto iv1 = ParseAsInterval(sv1);
        const auto iv2 = ParseAsInterval(sv2);
        if (iv1 && iv2) {
          return *iv1 == *iv2;
        }
      }
      return false;
    }
    case ValueType::kDouble: {
      const double lhs = value.double_value;
      const double rhs_double = rhs.value.double_value;
      // Exact equality, canonicalized like std::hash<Value>: every NaN
      // compares equal to every other NaN (all NaNs fold into one hash
      // bucket) and -0.0 equals +0.0.  The previous 1e-9 epsilon made
      // equality non-transitive, contradicted operator< and std::hash, and
      // merged distinct group keys; comparison sites that need fuzzy
      // matching (e.g. accumulated sums) must opt in explicitly.
      if (std::isnan(lhs) && std::isnan(rhs_double)) {
        return true;
      }
      return lhs == rhs_double;
    }
    case ValueType::kArray:
      return ArrayElementSqlType() == rhs.ArrayElementSqlType() &&
             ArrayElements() == rhs.ArrayElements();
  }
  CHECK_MSG(false, "undefined type");
  return false;
}

namespace {
std::string EncodeMemcomparableFormatInteger(int64_t in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(ValueType::kInt64);  // Embeds prefix.
  const uint64_t be = htobe64(static_cast<uint64_t>(in));
  ::memcpy(ret.data() + 1, &be, 8);
  ret[1] ^= static_cast<char>(0x80);  // plus/minus sign.
  return ret;
}

constexpr uint8_t kMemcomparableUint64 = 6;
constexpr uint8_t kMemcomparableInterval = 7;

std::string EncodeMemcomparableFormatUint64(int64_t in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(kMemcomparableUint64);  // Embeds UINT64 prefix.
  const uint64_t be = htobe64(static_cast<uint64_t>(in));
  ::memcpy(ret.data() + 1, &be, 8);
  return ret;
}

StatusOr<size_t> DecodeMemcomparableFormatInteger(const char* src,
                                                  const char* end,
                                                  int64_t* dst) {
  if (static_cast<size_t>(end - src) < sizeof(uint64_t)) {
    return StatusError(StatusCode::kCorrupt,
                       "corrupt memcomparable integer: truncated");
  }
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

StatusOr<size_t> DecodeMemcomparableFormatVarchar(const char* src,
                                                  const char* end,
                                                  std::string* dst) {
  dst->clear();
  const char* buffer = nullptr;
  const char* const initial_offset = src;
  for (size_t size = 0;;) {
    if (static_cast<size_t>(end - src) < 9) {
      return StatusError(StatusCode::kCorrupt,
                         "corrupt memcomparable varchar: truncated");
    }
    buffer = src;
    const size_t offset = dst->size();
    // The flag byte is unsigned; reading it as signed char would turn a
    // corrupt 0x80..0xFF image into a negative length and an underflowing
    // size_t accumulator.
    const auto flag = static_cast<unsigned char>(buffer[8]);
    if (flag == 9) {
      // Continuation group: eight payload bytes follow.
      size += 8;
      dst->resize(size);
      ::memcpy(dst->data() + offset, buffer, 8);
      src += 9;
    } else if (flag == 0) {
      // Empty-string encoding: the encoder emits a single all-zero group for
      // "". Consume the group and leave dst empty.
      if (src != initial_offset ||
          !std::all_of(buffer, buffer + 8, [](char c) { return c == '\0'; })) {
        return StatusError(StatusCode::kCorrupt,
                           "corrupt memcomparable varchar length");
      }
      src += 9;
      break;
    } else if (8 < flag) {
      return StatusError(StatusCode::kCorrupt,
                         "corrupt memcomparable varchar length");
    } else {
      if (!std::all_of(buffer + flag, buffer + 8,
                       [](char c) { return c == '\0'; })) {
        return StatusError(StatusCode::kCorrupt,
                           "corrupt memcomparable varchar padding");
      }
      size += flag;
      src += 9;
      dst->resize(size);
      ::memcpy(dst->data() + offset, buffer, flag);
      break;
    }
  }
  return static_cast<size_t>(src - initial_offset);
}

std::string EncodeMemcomparableFormatDouble(double in) {
  std::string ret(1 + 8, '\0');
  ret[0] = static_cast<char>(ValueType::kDouble);  // Embeds prefix.
  if (std::isnan(in)) {
    in = std::numeric_limits<double>::quiet_NaN();
  }
  uint64_t bits = 0;
  std::memcpy(&bits, &in, sizeof(bits));
  uint64_t be = htobe64(bits);
  if (in < 0) {
    // Strictly negative: invert every byte.  (NaN and -0.0 fall through to
    // the flag branch so the decoder's XOR restores their exact bits.)
    be = ~be;
  }
  std::memcpy(ret.data() + 1, &be, 8);
  // Flip the sign bit in the first byte of the big-endian image.  The old
  // `be |= 0x80` wrote the flag into the uint64's low byte, which memcpy
  // happened to land on this byte only on little-endian hosts.
  if (!(in < 0)) {
    ret[1] |= static_cast<char>(0x80);
  }
  return ret;
}

StatusOr<size_t> DecodeMemcomparableFormatDouble(const char* src,
                                                 const char* end, double* dst) {
  if (static_cast<size_t>(end - src) < sizeof(int64_t)) {
    return StatusError(StatusCode::kCorrupt,
                       "corrupt memcomparable double: truncated");
  }
  int64_t loaded = 0;
  std::memcpy(&loaded, src, sizeof(int64_t));
  uint64_t code = be64toh(static_cast<uint64_t>(loaded));
  if (0 < (src[0] & 0x80)) {
    code ^= 1LLU << 63;
  } else {
    code = ~code;
  }
  std::memcpy(dst, &code, sizeof(*dst));
  return sizeof(double);
}
}  // anonymous namespace

StatusOr<std::string> Value::TryEncodeMemcomparableFormat() const {
  switch (type) {
    case ValueType::kNull:
      // NULL keys are real index keys (the table has one row per NULL that
      // indexes the column): encode as the bare type tag.  kNull is the
      // smallest tag, so NULL entries sort below every encoded value,
      // matching SQL's "NULLS FIRST" scan order.  Callers that must reject
      // NULL keys (unique-collision checks) test the Value directly.
      return std::string(1, static_cast<char>(ValueType::kNull));
    case ValueType::kInt64:
      if (IsUnsigned()) {
        return EncodeMemcomparableFormatUint64(value.int_value);
      }
      return EncodeMemcomparableFormatInteger(value.int_value);
    case ValueType::kDate: {
      std::string encoded = EncodeMemcomparableFormatInteger(value.int_value);
      encoded[0] = static_cast<char>(ValueType::kDate);
      return encoded;
    }
    case ValueType::kVarChar:
      if (IsInterval()) {
        ASSIGN_OR_RETURN(IntervalValue, interval,
                         IntervalValue::TryParse(value.varchar_value));
        std::string encoded(1, static_cast<char>(kMemcomparableInterval));
        for (int64_t part : {interval.months, interval.days, interval.nanos}) {
          encoded += EncodeMemcomparableFormatInteger(part).substr(1);
        }
        return encoded;
      }
      return EncodeMemcomparableFormatVarchar(value.varchar_value);
    case ValueType::kDouble:
      return EncodeMemcomparableFormatDouble(value.double_value);
    case ValueType::kArray: {
      std::string encoded(1, static_cast<char>(ValueType::kArray));
      const auto& elements = ArrayElements();
      const auto count = static_cast<uint32_t>(elements.size());
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
        ASSIGN_OR_RETURN(std::string, element_encoded,
                         element.TryEncodeMemcomparableFormat());
        encoded += element_encoded;
      }
      return encoded;
    }
  }
  CHECK_MSG(false, "undefined type");
  return std::string();
}

void Value::ThrowAsInt64() {
  throw std::runtime_error("AsInt64 called on non-int64 value");
}

void Value::ThrowAsDouble() {
  throw std::runtime_error("AsDouble called on non-double value");
}

void Value::ThrowAsStringView() {
  throw std::runtime_error("AsStringView called on non-varchar value");
}

namespace {
size_t EncodeMemcomparableFormatVarcharTo(char* dst, std::string_view in) {
  if (in.empty()) {
    dst[0] = '\x02';
    std::memset(dst + 1, 0, 9);
    return 10;
  }
  dst[0] = static_cast<char>(ValueType::kVarChar);
  char* d = dst + 1;
  const char* src = in.data();
  const size_t size = in.size();
  for (size_t i = 0;; i += 8) {
    if (9 <= size - i) {
      std::memcpy(d, src, 8);
      d += 8;
      src += 8;
      *d++ = 9;
    } else {
      std::memcpy(d, src, size - i);
      std::memset(d + (size - i), 0, 8 - (size - i));
      d += 8;
      *d++ = static_cast<char>((size % 8) + (size % 8 == 0 ? 8 : 0));
      return static_cast<size_t>(d - dst);
    }
  }
}

size_t VarcharMemcomparableLength(const char* src, size_t avail) {
  const char* p = src;
  size_t left = avail;
  for (;;) {
    if (left < 9) {
      throw std::runtime_error("truncated varchar group");
    }
    const auto cnt = static_cast<uint8_t>(p[8]);
    if (cnt == 0 || cnt > 9) {
      throw std::runtime_error("broken varchar group count");
    }
    if (cnt < 8 &&
        !std::all_of(p + cnt, p + 8, [](char c) { return c == '\0'; })) {
      throw std::runtime_error("corrupt memcomparable varchar padding");
    }
    left -= 9;
    p += 9;
    if (cnt != 9) {
      break;
    }
  }
  return static_cast<size_t>(p - src);
}
}  // namespace

size_t Value::EncodeMemcomparableFormatTo(char* dst) const {
  switch (type) {
    case ValueType::kNull:
      dst[0] = static_cast<char>(ValueType::kNull);
      return 1;
    case ValueType::kInt64:
      if (IsUnsigned()) {
        return EncodeMemcomparableFormatUint64To(dst, value.int_value);
      }
      return EncodeMemcomparableFormatIntegerTo(dst, value.int_value);
    case ValueType::kDate: {
      const size_t n = EncodeMemcomparableFormatIntegerTo(dst, value.int_value);
      dst[0] = static_cast<char>(ValueType::kDate);
      return n;
    }
    case ValueType::kVarChar:
      if (IsInterval()) {
        const auto encoded = EncodeMemcomparableFormat();
        std::copy(encoded.begin(), encoded.end(), dst);
        return encoded.size();
      }
      return EncodeMemcomparableFormatVarcharTo(dst, value.varchar_value);
    case ValueType::kDouble:
      return EncodeMemcomparableFormatDoubleTo(dst, value.double_value);
    case ValueType::kArray: {
      char* cursor = dst;
      *cursor++ = static_cast<char>(ValueType::kArray);
      const auto& elements = ArrayElements();
      const uint32_t count = htobe32(static_cast<uint32_t>(elements.size()));
      std::memcpy(cursor, &count, sizeof(count));
      cursor += sizeof(count);
      const auto& sql_type = ArrayElementSqlType();
      std::memcpy(cursor, sql_type.data(), sql_type.size());
      cursor += sql_type.size();
      *cursor++ = '\0';
      for (const Value& element : elements) {
        *cursor++ = element.IsNull() ? 0 : 1;
        if (!element.IsNull()) {
          cursor += element.EncodeMemcomparableFormatTo(cursor);
        }
      }
      return static_cast<size_t>(cursor - dst);
    }
  }
  throw std::runtime_error("undefined type");
}

size_t Value::DecodeMemcomparableFormatBounded(const char* src, size_t avail) {
  if (avail < 1) {
    throw std::runtime_error("truncated tag");
  }
  const auto raw_tag = static_cast<uint8_t>(src[0]);
  if (raw_tag == static_cast<uint8_t>(ValueType::kNull)) {
    return 1;
  }
  if (raw_tag == kMemcomparableInterval) {
    if (avail < 25) {
      throw std::runtime_error("truncated interval key");
    }
    return 25;
  }
  if (raw_tag == kMemcomparableUint64) {
    if (avail < 1 + 8) {
      throw std::runtime_error("truncated numeric");
    }
    return 1 + 8;
  }
  const auto tag = static_cast<ValueType>(raw_tag);
  switch (tag) {
    case ValueType::kInt64:
    case ValueType::kDate:
    case ValueType::kDouble:
      if (avail < 1 + 8) {
        throw std::runtime_error("truncated numeric");
      }
      return 1 + 8;
    case ValueType::kVarChar: {
      const bool empty =
          (avail >= 10) &&
          std::all_of(src + 1, src + 10, [](char c) { return c == '\0'; });
      if (empty) {
        return 10;
      }
      return 1 + VarcharMemcomparableLength(src + 1, avail - 1);
    }
    case ValueType::kArray: {
      Value tmp;
      const StatusOr<size_t> res =
          tmp.TryDecodeMemcomparableFormat(std::string_view(src, avail));
      if (!res.HasValue()) {
        throw std::runtime_error(res.GetStatus().GetMessage());
      }
      return res.Value();
    }
    default:
      throw std::runtime_error("unknown memcomparable tag");
  }
}

StatusOr<size_t> Value::TryDecodeMemcomparableFormat(std::string_view src) {
  const char* cursor = src.data();
  const char* const end = src.data() + src.size();
  if (cursor == end) {
    return StatusError(StatusCode::kCorrupt,
                       "corrupt memcomparable value: empty buffer");
  }
  const auto prefix = static_cast<uint8_t>(*cursor++);
  collation_ = 0;
  switch (prefix) {
    case static_cast<uint8_t>(ValueType::kNull):
      // Mirror of the 1-byte NULL encoding above: an index key carrying a
      // NULL component decodes back to a NULL value consuming exactly one
      // byte (key-only reads never dereference `value` further).
      *this = Value();
      return 1;
    case static_cast<uint8_t>(ValueType::kInt64): {
      type = ValueType::kInt64;
      ASSIGN_OR_RETURN(
          size_t, len,
          DecodeMemcomparableFormatInteger(cursor, end, &value.int_value));
      return len + 1;
    }
    case kMemcomparableInterval: {
      IntervalValue interval;
      for (int64_t* part :
           {&interval.months, &interval.days, &interval.nanos}) {
        ASSIGN_OR_RETURN(size_t, consumed,
                         DecodeMemcomparableFormatInteger(cursor, end, part));
        cursor += consumed;
      }
      ASSIGN_OR_RETURN(std::string, text, interval.TryToString());
      *this = Value(std::move(text)).WithInterval();
      return static_cast<size_t>(cursor - src.data());
    }
    case kMemcomparableUint64: {
      type = ValueType::kInt64;
      collation_ = 3;
      if (static_cast<size_t>(end - cursor) < sizeof(uint64_t)) {
        return StatusError(StatusCode::kCorrupt,
                           "corrupt memcomparable uint64: truncated");
      }
      uint64_t loaded = 0;
      ::memcpy(&loaded, cursor, sizeof(loaded));
      value.int_value = static_cast<int64_t>(be64toh(loaded));
      return sizeof(uint64_t) + 1;
    }
    case static_cast<uint8_t>(ValueType::kDate): {
      type = ValueType::kDate;
      ASSIGN_OR_RETURN(
          size_t, len,
          DecodeMemcomparableFormatInteger(cursor, end, &value.int_value));
      return len + 1;
    }
    case static_cast<uint8_t>(ValueType::kVarChar): {
      type = ValueType::kVarChar;
      ASSIGN_OR_RETURN(
          size_t, len,
          DecodeMemcomparableFormatVarchar(cursor, end, &owned_data.Mutable()));
      value.varchar_value = owned_data;
      return len + 1;
    }
    case static_cast<uint8_t>(ValueType::kDouble): {
      type = ValueType::kDouble;
      ASSIGN_OR_RETURN(
          size_t, len,
          DecodeMemcomparableFormatDouble(cursor, end, &value.double_value));
      return len + 1;
    }
    case static_cast<uint8_t>(ValueType::kArray): {
      static thread_local size_t memcmp_decode_depth = 0;
      if (memcmp_decode_depth >= 32) {
        return StatusError(
            StatusCode::kCorrupt,
            "corrupt memcomparable array: nesting depth exceeded");
      }
      struct DepthGuard {
        DepthGuard(const DepthGuard&) = delete;
        DepthGuard(DepthGuard&&) = delete;
        DepthGuard& operator=(const DepthGuard&) = delete;
        DepthGuard& operator=(DepthGuard&&) = delete;
        DepthGuard() { ++memcmp_decode_depth; }
        ~DepthGuard() { --memcmp_decode_depth; }
      } guard;
      type = ValueType::kArray;
      const char* p = cursor;
      uint32_t be = 0;
      if (static_cast<size_t>(end - p) < sizeof(be)) {
        return StatusError(StatusCode::kCorrupt,
                           "corrupt memcomparable array: truncated");
      }
      std::memcpy(&be, p, sizeof(be));
      p += sizeof(be);
      const uint32_t count = be32toh(be);
      const char* type_begin = p;
      // Bounded scan: a corrupt image without a terminator must not walk off
      // the key buffer.  Real SQL type names are far shorter than this cap.
      while (p != end && *p != '\0') {
        if (static_cast<size_t>(p - type_begin) >= 64) {
          return StatusError(StatusCode::kCorrupt,
                             "corrupt memcomparable array type name");
        }
        ++p;
      }
      if (p == end) {
        return StatusError(StatusCode::kCorrupt,
                           "corrupt memcomparable array type name");
      }
      std::string sql_type(type_begin, p);
      ++p;
      // The count comes from the encoded image; a corrupt key may carry an
      // absurd value, so reject reservations beyond any real page-sized
      // array or beyond the remaining byte budget (each element consumes at
      // least one presence byte) instead of attempting a multi-megabyte
      // allocation per nested frame.
      constexpr uint32_t kMaxEncodedArrayElements = 1U << 20;
      if (count > kMaxEncodedArrayElements ||
          std::cmp_greater(count, end - p)) {
        return StatusError(StatusCode::kCorrupt,
                           "corrupt memcomparable array element count");
      }
      std::vector<Value> elements;
      elements.reserve(count);
      for (uint32_t i = 0; i < count; ++i) {
        if (p == end) {
          return StatusError(StatusCode::kCorrupt,
                             "corrupt memcomparable array: truncated");
        }
        const auto present = static_cast<uint8_t>(*p++);
        if (present == 0) {
          elements.emplace_back();
          continue;
        }
        if (present != 1) {
          return StatusError(StatusCode::kCorrupt,
                             "corrupt memcomparable array element presence");
        }
        Value element;
        std::string_view rest(p, static_cast<size_t>(end - p));
        ASSIGN_OR_RETURN(size_t, advanced,
                         element.TryDecodeMemcomparableFormat(rest));
        if (element.IsNull()) {
          return StatusError(
              StatusCode::kCorrupt,
              "corrupt memcomparable array non-null element is null");
        }
        p += advanced;
        elements.push_back(std::move(element));
      }
      owned_data.SetArray(std::make_unique<ArrayPayload>());
      owned_data.Array()->element_sql_type = std::move(sql_type);
      owned_data.Array()->elements = std::move(elements);
      return static_cast<size_t>(p - cursor) + 1;
    }
    default:
      break;
  }
  return StatusError(StatusCode::kCorrupt, "broken data");
}

bool Value::LessSlow(const Value& rhs) const {
  return ExcShimUnwrap(TryLess(rhs), "Value::operator<");
}

StatusOr<bool> Value::TryLess(const Value& rhs) const {
  if (type != rhs.type) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Different type cannot be compared.");
  }
  switch (type) {
    case ValueType::kNull:
      return StatusError(StatusCode::kInvalidArgument,
                         "Unknown type cannot be compared.");
    case ValueType::kInt64: {
      if (IsUnsigned() || rhs.IsUnsigned()) {
        const bool mixed = IsUnsigned() != rhs.IsUnsigned();
        const auto lhs_u = static_cast<uint64_t>(value.int_value);
        const auto rhs_u = static_cast<uint64_t>(rhs.value.int_value);
        const bool lhs_neg = !IsUnsigned() && value.int_value < 0;
        const bool rhs_neg = !rhs.IsUnsigned() && rhs.value.int_value < 0;
        if (!mixed) {
          return lhs_u < rhs_u;
        }
        if (lhs_neg) {
          return true;
        }
        if (rhs_neg) {
          return false;
        }
        return lhs_u < rhs_u;
      }
      return value.int_value < rhs.value.int_value;
    }
    case ValueType::kDate:
      return value.int_value < rhs.value.int_value;
    case ValueType::kVarChar: {
      if (IsInterval() != rhs.IsInterval()) {
        return !IsInterval();
      }
      std::string_view sv1 = value.varchar_value;
      std::string_view sv2 = rhs.value.varchar_value;
      const int cmp = sv1.compare(sv2);
      if (cmp == 0) {
        return false;
      }
      // Only tagged INTERVAL values use field ordering. Canonical
      // renderings contain a space; ordinary strings retain byte ordering.
      if (!ViewContainsSpace(sv1) || !ViewContainsSpace(sv2)) {
        return cmp < 0;
      }
      if (IsInterval() && rhs.IsInterval()) {
        const auto iv1 = ParseAsInterval(sv1);
        const auto iv2 = ParseAsInterval(sv2);
        if (iv1 && iv2) {
          return *iv1 < *iv2;
        }
      }
      return cmp < 0;
    }
    case ValueType::kDouble:
      return value.double_value < rhs.value.double_value;
    case ValueType::kArray: {
      // Lexicographic compare through the StatusOr path: std::vector::operator<
      // would call the throwing Value::operator< (ExcShimUnwrap) and let a C++
      // exception escape the Try* contract on incomparable elements (NULL or
      // mixed element types).
      const auto& lhs_elements = ArrayElements();
      const auto& rhs_elements = rhs.ArrayElements();
      const size_t common = std::min(lhs_elements.size(), rhs_elements.size());
      for (size_t i = 0; i < common; ++i) {
        if (lhs_elements[i] == rhs_elements[i]) {
          continue;
        }
        return lhs_elements[i].TryLess(rhs_elements[i]);
      }
      return lhs_elements.size() < rhs_elements.size();
    }
  }
  return StatusError(StatusCode::kInvalidArgument, "undefined type");
}

StatusOr<bool> Value::TryGreater(const Value& rhs) const {
  if (type != rhs.type) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Different type cannot be compared.");
  }
  switch (type) {
    case ValueType::kNull:
      return StatusError(StatusCode::kInvalidArgument,
                         "Unknown type cannot be compared.");
    case ValueType::kInt64: {
      if (IsUnsigned() || rhs.IsUnsigned()) {
        const bool mixed = IsUnsigned() != rhs.IsUnsigned();
        const auto lhs_u = static_cast<uint64_t>(value.int_value);
        const auto rhs_u = static_cast<uint64_t>(rhs.value.int_value);
        const bool lhs_neg = !IsUnsigned() && value.int_value < 0;
        const bool rhs_neg = !rhs.IsUnsigned() && rhs.value.int_value < 0;
        if (!mixed) {
          return lhs_u > rhs_u;
        }
        if (lhs_neg) {
          return false;
        }
        if (rhs_neg) {
          return true;
        }
        return lhs_u > rhs_u;
      }
      return value.int_value > rhs.value.int_value;
    }
    case ValueType::kDate:
      return value.int_value > rhs.value.int_value;
    case ValueType::kVarChar: {
      if (IsInterval() != rhs.IsInterval()) {
        return IsInterval();
      }
      std::string_view sv1 = value.varchar_value;
      std::string_view sv2 = rhs.value.varchar_value;
      const int cmp = sv2.compare(sv1);
      if (cmp == 0) {
        return false;
      }
      // Only tagged INTERVAL values use field ordering. Canonical
      // renderings contain a space; ordinary strings retain byte ordering.
      if (!ViewContainsSpace(sv1) || !ViewContainsSpace(sv2)) {
        return cmp < 0;
      }
      if (IsInterval() && rhs.IsInterval()) {
        const auto iv1 = ParseAsInterval(sv1);
        const auto iv2 = ParseAsInterval(sv2);
        if (iv1 && iv2) {
          return *iv1 > *iv2;
        }
      }
      return cmp < 0;
    }
    case ValueType::kDouble:
      return value.double_value > rhs.value.double_value;
    case ValueType::kArray: {
      const auto& lhs_elements = ArrayElements();
      const auto& rhs_elements = rhs.ArrayElements();
      const size_t common = std::min(lhs_elements.size(), rhs_elements.size());
      for (size_t i = 0; i < common; ++i) {
        if (lhs_elements[i] == rhs_elements[i]) {
          continue;
        }
        return lhs_elements[i].TryGreater(rhs_elements[i]);
      }
      return lhs_elements.size() > rhs_elements.size();
    }
  }
  return StatusError(StatusCode::kInvalidArgument, "undefined type");
}

StatusOr<Value> Value::TryArithmetic(const Value& rhs,
                                     BinaryOperation op) const {
  const auto invalid = [](std::string_view message) -> Status {
    return StatusError(StatusCode::kInvalidArgument, std::string(message));
  };
  // Mixed signed/unsigned division uses mathematical signs, not a cast of
  // the negative operand to UINT64. Magnitudes avoid INT64_MIN negation and
  // keep the full UINT64 range without a floating-point conversion.
  if (type == ValueType::kInt64 && rhs.type == ValueType::kInt64 &&
      IsUnsigned() != rhs.IsUnsigned() &&
      (op == BinaryOperation::kDivide || op == BinaryOperation::kModulo)) {
    const bool left_negative = !IsUnsigned() && value.int_value < 0;
    const bool right_negative = !rhs.IsUnsigned() && rhs.value.int_value < 0;
    const auto left_bits = static_cast<uint64_t>(value.int_value);
    const auto right_bits = static_cast<uint64_t>(rhs.value.int_value);
    const uint64_t left = left_negative ? uint64_t{0} - left_bits : left_bits;
    const uint64_t right =
        right_negative ? uint64_t{0} - right_bits : right_bits;
    if (right == 0) {
      return StatusError(StatusCode::kIsInfinity, "division by zero");
    }
    const bool divide = op == BinaryOperation::kDivide;
    const uint64_t magnitude = divide ? left / right : left % right;
    const bool negative =
        divide ? left_negative != right_negative : left_negative;
    if (negative && magnitude != 0) {
      if (magnitude > (uint64_t{1} << 63)) {
        return invalid("integer overflow on mixed signed/unsigned division");
      }
      return Value(std::bit_cast<int64_t>(uint64_t{0} - magnitude));
    }
    return Value(std::bit_cast<int64_t>(magnitude)).WithUnsigned();
  }
  switch (op) {
    case BinaryOperation::kAdd:
      if (type != rhs.type) {
        return invalid("Different type cannot be added.");
      }
      if (type == ValueType::kInt64) {
        if (IsUnsigned() || rhs.IsUnsigned()) {
          uint64_t result = 0;
          if (__builtin_add_overflow(static_cast<uint64_t>(value.int_value),
                                     static_cast<uint64_t>(rhs.value.int_value),
                                     &result)) {
            return invalid("integer overflow on '+'");
          }
          return Value(static_cast<int64_t>(result)).WithUnsigned();
        }
        int64_t result = 0;
        if (__builtin_add_overflow(value.int_value, rhs.value.int_value,
                                   &result)) {
          return invalid("integer overflow on '+'");
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
      return invalid("Cannot do '+' against this type");
    case BinaryOperation::kSubtract:
      if (type != rhs.type) {
        return invalid("Different type cannot be subtracted.");
      }
      if (type == ValueType::kInt64) {
        if (IsUnsigned() || rhs.IsUnsigned()) {
          uint64_t result = 0;
          if (__builtin_sub_overflow(static_cast<uint64_t>(value.int_value),
                                     static_cast<uint64_t>(rhs.value.int_value),
                                     &result)) {
            return invalid("integer overflow on '-'");
          }
          return Value(static_cast<int64_t>(result)).WithUnsigned();
        }
        int64_t result = 0;
        if (__builtin_sub_overflow(value.int_value, rhs.value.int_value,
                                   &result)) {
          return invalid("integer overflow on '-'");
        }
        return Value(result);
      }
      if (type == ValueType::kDouble) {
        return Value(value.double_value - rhs.value.double_value);
      }
      return invalid("Cannot do '-' against this type");
    case BinaryOperation::kMultiply:
      if (type != rhs.type) {
        return invalid("Different type cannot be multiplied.");
      }
      if (type == ValueType::kInt64) {
        if (IsUnsigned() || rhs.IsUnsigned()) {
          uint64_t result = 0;
          if (__builtin_mul_overflow(static_cast<uint64_t>(value.int_value),
                                     static_cast<uint64_t>(rhs.value.int_value),
                                     &result)) {
            return invalid("integer overflow on '*'");
          }
          return Value(static_cast<int64_t>(result)).WithUnsigned();
        }
        int64_t result = 0;
        if (__builtin_mul_overflow(value.int_value, rhs.value.int_value,
                                   &result)) {
          return invalid("integer overflow on '*'");
        }
        return Value(result);
      }
      if (type == ValueType::kDouble) {
        return Value(value.double_value * rhs.value.double_value);
      }
      return invalid("Cannot do '*' against this type");
    case BinaryOperation::kDivide:
      if (type != rhs.type) {
        return invalid("Different type cannot be divided.");
      }
      if (type == ValueType::kInt64) {
        if (IsUnsigned() || rhs.IsUnsigned()) {
          const auto u_rhs = static_cast<uint64_t>(rhs.value.int_value);
          if (u_rhs == 0) {
            return StatusError(StatusCode::kIsInfinity, "division by zero");
          }
          const auto u_lhs = static_cast<uint64_t>(value.int_value);
          return Value(static_cast<int64_t>(u_lhs / u_rhs)).WithUnsigned();
        }
        if (rhs.value.int_value == 0) {
          return StatusError(StatusCode::kIsInfinity, "division by zero");
        }
        // INT64_MIN / -1 overflows int64 and would raise SIGFPE.
        if (value.int_value == std::numeric_limits<int64_t>::min() &&
            rhs.value.int_value == -1) {
          return invalid("integer overflow on '/'");
        }
        return Value(value.int_value / rhs.value.int_value);
      }
      if (type == ValueType::kDouble) {
        return Value(value.double_value / rhs.value.double_value);
      }
      return invalid("Cannot do '/' against this type");
    case BinaryOperation::kModulo:
      if (type != rhs.type) {
        return invalid("Different type cannot do modulo.");
      }
      if (type == ValueType::kInt64) {
        if (IsUnsigned() || rhs.IsUnsigned()) {
          const auto u_rhs = static_cast<uint64_t>(rhs.value.int_value);
          if (u_rhs == 0) {
            return StatusError(StatusCode::kIsInfinity, "modulo by zero");
          }
          const auto u_lhs = static_cast<uint64_t>(value.int_value);
          return Value(static_cast<int64_t>(u_lhs % u_rhs)).WithUnsigned();
        }
        if (rhs.value.int_value == 0) {
          return StatusError(StatusCode::kIsInfinity, "modulo by zero");
        }
        // INT64_MIN % -1 would raise SIGFPE on x86 despite the mathematical
        // result (0) being representable.
        if (value.int_value == std::numeric_limits<int64_t>::min() &&
            rhs.value.int_value == -1) {
          return invalid("integer overflow on '%'");
        }
        return Value(value.int_value % rhs.value.int_value);
      }
      return invalid("Cannot do '%' against this type");
    case BinaryOperation::kAnd:
      if (type != rhs.type) {
        return invalid("Different type cannot do AND.");
      }
      if (type == ValueType::kInt64) {
        Value res(value.int_value & rhs.value.int_value);
        if (IsUnsigned() || rhs.IsUnsigned()) {
          return res.WithUnsigned();
        }
        return res;
      }
      return invalid("Cannot do '&' against this type");
    case BinaryOperation::kOr:
      if (type != rhs.type) {
        return invalid("Different type cannot do OR.");
      }
      if (type == ValueType::kInt64) {
        Value res(value.int_value | rhs.value.int_value);
        if (IsUnsigned() || rhs.IsUnsigned()) {
          return res.WithUnsigned();
        }
        return res;
      }
      return invalid("Cannot do '|' against this type");
    case BinaryOperation::kXor:
      if (type != rhs.type) {
        return invalid("Different type cannot do XOR.");
      }
      if (type == ValueType::kInt64) {
        Value res(value.int_value ^ rhs.value.int_value);
        if (IsUnsigned() || rhs.IsUnsigned()) {
          return res.WithUnsigned();
        }
        return res;
      }
      return invalid("Cannot do '^' against this type");
    default:
      return invalid("Cannot do arithmetic against this type");
  }
}

int CompareForOrderBy(const Value& a, const Value& b) {
  // Type rank keeps cross-type keys in a deterministic total order; matching
  // types compare by value.
  static constexpr std::array<int, 6> kRank = {
      0, 1, 4, 3, 5, 6};  // null,int,string,double,date,array
  auto rank_of = [](const Value& v) {
    return v.type == ValueType::kNull
               ? 0
               : (static_cast<int>(v.type) < 6
                      ? kRank[static_cast<size_t>(v.type)]
                      : 9);
  };
  if (a.IsNull() || b.IsNull()) {
    // NULLs compare equal here; NULLS FIRST/LAST is the caller's decision.
    if (a.IsNull() && b.IsNull()) {
      return 0;
    }
    return a.IsNull() ? -1 : 1;
  }
  const int ra = rank_of(a);
  const int rb = rank_of(b);
  if (ra != rb) {
    return ra < rb ? -1 : 1;
  }
  switch (a.type) {
    case ValueType::kInt64: {
      if (a.IsUnsigned() || b.IsUnsigned()) {
        const bool mixed = a.IsUnsigned() != b.IsUnsigned();
        const auto ua = static_cast<uint64_t>(a.value.int_value);
        const auto ub = static_cast<uint64_t>(b.value.int_value);
        const bool a_neg = !a.IsUnsigned() && a.value.int_value < 0;
        const bool b_neg = !b.IsUnsigned() && b.value.int_value < 0;
        if (!mixed) {
          return ua < ub ? -1 : (ub < ua ? 1 : 0);
        }
        if (a_neg) {
          return -1;
        }
        if (b_neg) {
          return 1;
        }
        return ua < ub ? -1 : (ub < ua ? 1 : 0);
      }
      return a.value.int_value < b.value.int_value
                 ? -1
                 : (b.value.int_value < a.value.int_value ? 1 : 0);
    }
    case ValueType::kDate:
      return a.value.int_value < b.value.int_value
                 ? -1
                 : (b.value.int_value < a.value.int_value ? 1 : 0);
    case ValueType::kDouble: {
      const double x = a.value.double_value;
      const double y = b.value.double_value;
      // GoogleSQL ordering: NULL < NaN < every other value; all NaNs are
      // equal under the ordering (DESC is the exact reverse).
      const bool nx = std::isnan(x);
      const bool ny = std::isnan(y);
      if (nx || ny) {
        return nx && ny ? 0 : (nx ? -1 : 1);
      }
      return x < y ? -1 : (y < x ? 1 : 0);
    }
    case ValueType::kVarChar:
      if (a.IsInterval() != b.IsInterval()) {
        return a.IsInterval() ? 1 : -1;
      }
      if (a.IsInterval()) {
        const auto lhs = ParseAsInterval(a.value.varchar_value);
        const auto rhs = ParseAsInterval(b.value.varchar_value);
        if (lhs && rhs) {
          return *lhs < *rhs ? -1 : (*rhs < *lhs ? 1 : 0);
        }
      }
      // Binary (engine default) VARCHAR order. The case-insensitive variant
      // and the shared ENUM ranking live in type/collation.hpp so ORDER BY,
      // filtering and grouping cannot disagree about the ordering.
      return CompareVarCharForOrderBy(a.value.varchar_value,
                                      b.value.varchar_value,
                                      /*case_insensitive=*/false);
    case ValueType::kArray: {
      const auto& xs = a.ArrayElements();
      const auto& ys = b.ArrayElements();
      const size_t n = std::min(xs.size(), ys.size());
      for (size_t i = 0; i < n; ++i) {
        const int c = CompareForOrderBy(xs[i], ys[i]);
        if (c != 0) {
          return c;
        }
      }
      return xs.size() < ys.size() ? -1 : (ys.size() < xs.size() ? 1 : 0);
    }
    default:
      return 0;
  }
}

std::ostream& operator<<(std::ostream& o, const Value& v) {
  o << v.AsString();
  return o;
}

Encoder& operator<<(Encoder& a, const Value& v) {
  a << static_cast<uint8_t>(static_cast<uint8_t>(v.type) |
                            (v.Collation() << 3));
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
  uint8_t encoded_type = 0;
  e >> encoded_type;
  v.type = static_cast<ValueType>(encoded_type & 7);
  v.collation_ = encoded_type >> 3;
  if (v.collation_ > 4) {
    e.Fail();
    return e;
  }
  switch (v.type) {
    case tinylamb::ValueType::kNull:
      break;
    case tinylamb::ValueType::kInt64:
    case tinylamb::ValueType::kDate:
      e >> v.value.int_value;
      break;
    case tinylamb::ValueType::kVarChar:
      e >> v.owned_data.Mutable();
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
      // Sticky decoder failure so callers that check Decoder::Failed() can
      // reject the stream; CHECK in debug keeps plumbing bugs loud.
      e.Fail();
      break;
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
    case tinylamb::ValueType::kVarChar:
      if (v.IsInterval()) {
        const auto interval =
            tinylamb::IntervalValue::TryParse(v.value.varchar_value);
        if (interval.HasValue()) {
          const auto& iv = interval.Value();
          return std::hash<int64_t>()(iv.nanos) ^
                 (std::hash<int64_t>()(iv.days) * 0x9e3779b97f4a7c15ULL) ^
                 (std::hash<int64_t>()(iv.months) << 1);
        }
      }
      return v.owned_data.StringHash(v.value.varchar_value);
    case tinylamb::ValueType::kDouble: {
      // Canonicalize before hashing so group-key buckets agree with SQL
      // equality: every NaN folds to one pattern and -0/+0 share a bucket.
      const double d = v.value.double_value;
      if (std::isnan(d)) {
        return std::hash<int64_t>()(0x7ff8000000000000LL);
      }
      return std::hash<double>()(d == 0.0 ? 0.0 : d);
    }
    case tinylamb::ValueType::kArray: {
      uint64_t h = std::hash<std::string>()(v.ArrayElementSqlType());
      for (const tinylamb::Value& element : v.ArrayElements()) {
        h ^= std::hash<tinylamb::Value>()(element) + 0x9e3779b97f4a7c15ULL +
             (h << 6) + (h >> 2);
      }
      return h;
    }
  }
  CHECK_MSG(false, "undefined type");
  return 0;
}
