/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/data_chunk.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "executor/selection_vector.hpp"
#include "executor/zone_map.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {
// Feeds one value of a column into its zone map straight from typed storage,
// skipping Value boxing for the common non-null case.
void UpdateZoneMapFrom(const ColumnVector& column, size_t index,
                       ZoneMap* zone_map) {
  if (column.IsNull(index)) {
    zone_map->AddNull();
    return;
  }
  if (column.IsHeterogeneous()) {
    zone_map->Add(column.ValueAt(index));
    return;
  }
  switch (column.Type()) {
    case ValueType::kInt64:
      if (column.IsUnsigned()) {
        zone_map->Add(column.ValueAt(index));
      } else {
        zone_map->AddInt(column.IntegerData()[index]);
      }
      break;
    case ValueType::kDate:
      zone_map->AddDate(column.IntegerData()[index]);
      break;
    case ValueType::kDouble:
      zone_map->AddDouble(column.DoubleData()[index]);
      break;
    case ValueType::kVarChar:
      if (column.ColumnCollation() == Collation::kCaseInsensitive) {
        zone_map->Add(column.ValueAt(index));
      } else {
        zone_map->AddString(column.StringData()[index]);
      }
      break;
    case ValueType::kArray:
      // Arrays are non-NULL values with no scalar envelope; record them so
      // the zone stays eligible (matching ZoneMap::Add on the row path).
      zone_map->AddOpaque();
      break;
    case ValueType::kNull:
      break;
  }
}
}  // namespace

ColumnVector::ColumnVector(ValueType type, size_t capacity) : type_(type) {
  Reserve(capacity);
}

void ColumnVector::ObserveSourceCollation(const ColumnVector& source) {
  if (mixed_collation_ || !source.observed_collation_) {
    return;
  }
  if (source.mixed_collation_) {
    observed_collation_ = true;
    mixed_collation_ = true;
    collation_ = Collation::kNone;
    return;
  }
  const Collation observed = source.ColumnCollation();
  if (!observed_collation_) {
    collation_ = observed;
    observed_collation_ = true;
    return;
  }
  if (observed != collation_) {
    mixed_collation_ = true;
    collation_ = Collation::kNone;
  }
}

void ColumnVector::ObserveCollation(const Value& value) {
  // VARCHAR only: for every other type Value's tag is the unsigned marker,
  // which `unsigned_` already tracks, and overwriting it would flip the
  // integer signedness.
  if (value.IsNull() || type_ != ValueType::kVarChar || mixed_collation_) {
    return;
  }
  const Collation observed = CollationOf(value);
  if (!observed_collation_) {
    collation_ = observed;
    observed_collation_ = true;
    return;
  }
  if (observed != collation_) {
    // Mixed collators in one column: fall back to the engine default instead
    // of folding part of the batch.
    mixed_collation_ = true;
    collation_ = Collation::kNone;
  }
}

void ColumnVector::PromoteToHeterogeneous() {
  if (heterogeneous_) {
    return;
  }
  std::vector<Value> boxed;
  boxed.reserve(size_);
  for (size_t i = 0; i < size_; ++i) {
    boxed.push_back(ValueAt(i));
  }
  integers_.clear();
  doubles_.clear();
  strings_.clear();
  nested_ = std::move(boxed);
  heterogeneous_ = true;
  type_ = ValueType::kNull;
}

void ColumnVector::Append(const Value& value) {
  const bool is_null = value.IsNull();
  if (value.IsInterval() && !heterogeneous_) {
    PromoteToHeterogeneous();
  }
  if (heterogeneous_) {
    EnsureNullBit(size_, is_null);
    nested_.push_back(is_null ? Value() : value);
    ++size_;
    return;
  }
  if (!is_null && type_ == ValueType::kNull) {
    type_ = value.type;
    unsigned_ = value.IsUnsigned();
    MaterializeInferredStorage();
  } else if (!is_null &&
             (type_ != value.type ||
              (type_ == ValueType::kInt64 &&
               value.IsUnsigned() != unsigned_ && size_ > 0 &&
               allow_heterogeneous_))) {
    // A different type -- or an INT64 cell whose signedness differs from the
    // column's -- cannot share the typed integer storage: the column keeps a
    // single unsigned flag, so -1 appended after a UINT64 would read back as
    // 18446744073709551615.  Mixed columns keep per-value Values instead.
    if (allow_heterogeneous_) {
      PromoteToHeterogeneous();
      EnsureNullBit(size_, false);
      nested_.push_back(value);
      ++size_;
      return;
    }
    CHECK_MSG(false, "column vector type mismatch: column=" +
                         std::string(ValueTypeToString(type_)) + " value=" +
                         std::string(ValueTypeToString(value.type)));
  }
  EnsureNullBit(size_, is_null);
  if (is_null) {
    AppendDefault();
  } else {
    ObserveCollation(value);
    switch (type_) {
      case ValueType::kInt64:
      case ValueType::kDate:
        integers_.push_back(value.value.int_value);
        break;
      case ValueType::kDouble:
        doubles_.push_back(value.value.double_value);
        break;
      case ValueType::kVarChar:
        strings_.emplace_back(value.value.varchar_value);
        break;
      case ValueType::kArray:
        nested_.push_back(value);
        break;
      case ValueType::kNull:
        break;
    }
  }
  ++size_;
}

void ColumnVector::Append(Value&& value) {
  const bool is_null = value.IsNull();
  if (value.IsInterval() && !heterogeneous_) {
    PromoteToHeterogeneous();
  }
  if (heterogeneous_) {
    EnsureNullBit(size_, is_null);
    nested_.push_back(is_null ? Value() : std::move(value));
    ++size_;
    return;
  }
  if (!is_null && type_ == ValueType::kNull) {
    type_ = value.type;
    unsigned_ = value.IsUnsigned();
    MaterializeInferredStorage();
  } else if (!is_null &&
             (type_ != value.type ||
              (type_ == ValueType::kInt64 &&
               value.IsUnsigned() != unsigned_ && size_ > 0 &&
               allow_heterogeneous_))) {
    // See Append(const Value&): mixed INT64 signedness keeps per-value cells.
    if (allow_heterogeneous_) {
      PromoteToHeterogeneous();
      EnsureNullBit(size_, false);
      nested_.push_back(std::move(value));
      ++size_;
      return;
    }
    CHECK_MSG(false, "column vector type mismatch: column=" +
                         std::string(ValueTypeToString(type_)) + " value=" +
                         std::string(ValueTypeToString(value.type)));
  }
  EnsureNullBit(size_, is_null);
  if (is_null) {
    AppendDefault();
  } else {
    ObserveCollation(value);
    switch (type_) {
      case ValueType::kInt64:
      case ValueType::kDate:
        integers_.push_back(value.value.int_value);
        break;
      case ValueType::kDouble:
        doubles_.push_back(value.value.double_value);
        break;
      case ValueType::kVarChar:
        if (value.owned_data.empty()) {
          strings_.emplace_back(value.value.varchar_value);
        } else {
          strings_.emplace_back(value.MoveString());
        }
        break;
      case ValueType::kArray:
        nested_.emplace_back(std::move(value));
        break;
      case ValueType::kNull:
        break;
    }
  }
  ++size_;
}

void ColumnVector::AppendFrom(const ColumnVector& source, size_t index) {
  if (index >= source.Size()) {
    return;
  }
  const bool is_null = source.IsNull(index);
  if (heterogeneous_ || source.heterogeneous_) {
    if (allow_heterogeneous_) {
      PromoteToHeterogeneous();
      EnsureNullBit(size_, is_null);
      nested_.push_back(is_null ? Value() : source.ValueAt(index));
      ++size_;
      return;
    }
    Append(source.ValueAt(index));
    return;
  }
  if (!is_null && type_ == ValueType::kNull) {
    type_ = source.type_;
    unsigned_ = source.unsigned_;
    MaterializeInferredStorage();
  } else if (!is_null && source.type_ != type_) {
    if (allow_heterogeneous_) {
      PromoteToHeterogeneous();
      EnsureNullBit(size_, false);
      nested_.push_back(source.ValueAt(index));
      ++size_;
      return;
    }
    CHECK_MSG(false, "column vector type mismatch");
  }
  EnsureNullBit(size_, is_null);
  if (is_null) {
    AppendDefault();
  } else {
    if (source.type_ == ValueType::kVarChar) {
      ObserveSourceCollation(source);
    }
    switch (type_) {
      case ValueType::kInt64:
      case ValueType::kDate:
        integers_.push_back(source.integers_[index]);
        break;
      case ValueType::kDouble:
        doubles_.push_back(source.doubles_[index]);
        break;
      case ValueType::kVarChar:
        strings_.emplace_back(source.strings_[index]);
        break;
      case ValueType::kArray:
        nested_.push_back(source.nested_[index]);
        break;
      case ValueType::kNull:
        break;
    }
  }
  ++size_;
}

void ColumnVector::AppendRange(const ColumnVector& source) {
  if (source.size_ == 0 || this == &source) {
    return;
  }
  if (heterogeneous_ || source.heterogeneous_) {
    for (size_t i = 0; i < source.size_; ++i) {
      AppendFrom(source, i);
    }
    return;
  }
  if (type_ == ValueType::kNull && source.type_ != ValueType::kNull) {
    type_ = source.type_;
    unsigned_ = source.unsigned_;
    MaterializeInferredStorage();
  } else if (source.type_ != ValueType::kNull && type_ != source.type_) {
    if (allow_heterogeneous_) {
      for (size_t i = 0; i < source.size_; ++i) {
        AppendFrom(source, i);
      }
      return;
    }
    CHECK_MSG(false, "column vector type mismatch");
  }
  if (source.type_ == ValueType::kVarChar) {
    ObserveSourceCollation(source);
  }
  const size_t old_size = size_;
  const size_t count = source.size_;
  size_ = old_size + count;
  null_bitmap_.resize((size_ + 63) / 64, 0);
  if (source.type_ == ValueType::kNull) {
    for (size_t i = 0; i < count; ++i) {
      null_bitmap_[(old_size + i) / 64] |= uint64_t{1} << ((old_size + i) % 64);
      AppendDefault();
    }
    return;
  }
  for (size_t i = 0; i < count; ++i) {
    if (source.IsNull(i)) {
      null_bitmap_[(old_size + i) / 64] |= uint64_t{1} << ((old_size + i) % 64);
    }
  }
  switch (type_) {
    case ValueType::kInt64:
    case ValueType::kDate:
      integers_.insert(
          integers_.end(), source.integers_.begin(),
          source.integers_.begin() + static_cast<std::ptrdiff_t>(count));
      break;
    case ValueType::kDouble:
      doubles_.insert(
          doubles_.end(), source.doubles_.begin(),
          source.doubles_.begin() + static_cast<std::ptrdiff_t>(count));
      break;
    case ValueType::kVarChar:
      strings_.insert(
          strings_.end(), source.strings_.begin(),
          source.strings_.begin() + static_cast<std::ptrdiff_t>(count));
      break;
    case ValueType::kArray:
      nested_.insert(
          nested_.end(), source.nested_.begin(),
          source.nested_.begin() + static_cast<std::ptrdiff_t>(count));
      break;
    case ValueType::kNull:
      break;
  }
}

void ColumnVector::AppendBoolBatch(const uint8_t* bits, size_t count) {
  // T1938: a null value buffer with count > 0 is a contract violation; treat
  // it as a no-op instead of dereferencing (JIT filter output is never null,
  // but the API must not hand the caller UB).
  if (count == 0 || bits == nullptr) {
    return;
  }
  if (type_ == ValueType::kNull) {
    type_ = ValueType::kInt64;
    MaterializeInferredStorage();
  }
  const size_t old_size = size_;
  size_ = old_size + count;
  null_bitmap_.resize((size_ + 63) / 64, 0);
  // Every appended cell is non-null: the new bitmap words stay zero.
  if (type_ != ValueType::kInt64 && type_ != ValueType::kDate) {
    CHECK_MSG(false, "bool batch append on non-integer column vector");
  }
  integers_.resize(size_);
  int64_t* out = integers_.data() + old_size;
  for (size_t i = 0; i < count; ++i) {
    out[i] = bits[i] != 0 ? 1 : 0;
  }
}

void ColumnVector::AppendNullBatch(size_t count, ZoneMap* zone_map) {
  if (count == 0) {
    return;
  }
  const size_t old_size = size_;
  size_ = old_size + count;
  null_bitmap_.resize((size_ + 63) / 64, 0);
  if (old_size == 0 && (count % 64) == 0) {
    std::fill(null_bitmap_.begin(), null_bitmap_.end(), ~uint64_t{0});
  } else {
    for (size_t i = 0; i < count; ++i) {
      null_bitmap_[(old_size + i) / 64] |= uint64_t{1} << ((old_size + i) % 64);
    }
  }
  if (heterogeneous_) {
    nested_.resize(size_);
  } else {
    switch (type_) {
      case ValueType::kInt64:
      case ValueType::kDate:
        integers_.resize(size_, 0);
        break;
      case ValueType::kDouble:
        doubles_.resize(size_, 0.0);
        break;
      case ValueType::kVarChar:
        strings_.resize(size_);
        break;
      case ValueType::kArray:
        nested_.resize(size_);
        break;
      case ValueType::kNull:
        break;
    }
  }
  if (zone_map != nullptr) {
    zone_map->AddNulls(count);
  }
}

void ColumnVector::AppendIntBatch(ValueType type, bool is_unsigned,
                                  const int64_t* values, const uint8_t* nulls,
                                  size_t count, ZoneMap* zone_map) {
  // T1938: values == nullptr with count > 0 no-ops instead of memcpy/read UB
  // (nulls == nullptr legitimately means "no nulls", values has no such
  // reading).
  if (count == 0 || values == nullptr) {
    return;
  }
  if (!heterogeneous_ && type_ == ValueType::kNull) {
    type_ = type;
    unsigned_ = is_unsigned;
    MaterializeInferredStorage();
  }
  if (heterogeneous_ || type_ != type || unsigned_ != is_unsigned) {
    for (size_t i = 0; i < count; ++i) {
      Value v;
      if (nulls == nullptr || nulls[i] == 0) {
        if (type == ValueType::kDate) {
          v = Value::DateFromDays(values[i]);
        } else {
          v = Value(values[i]);
          if (is_unsigned) {
            v.SetUnsigned();
          }
        }
      }
      if (zone_map != nullptr) {
        zone_map->Add(v);
      }
      Append(std::move(v));
    }
    return;
  }
  const size_t old_size = size_;
  size_ = old_size + count;
  null_bitmap_.resize((size_ + 63) / 64, 0);
  // T4752: the old `integers_.resize(size_)` value-initialized every new
  // slot to 0 and the memcpy immediately rewrote all of them -- a pure
  // second O(count*8B) pass per batch per column on the scan-materialize
  // hot path. insert() from the caller's range writes each slot exactly
  // once (null slots keep the same source bytes the memcpy gave them).
  integers_.reserve(size_);
  integers_.insert(integers_.end(), values, values + count);
  if (nulls == nullptr) {
    if (zone_map != nullptr) {
      if (is_unsigned) {
        for (size_t i = 0; i < count; ++i) {
          Value v(values[i]);
          v.SetUnsigned();
          zone_map->Add(v);
        }
      } else {
        int64_t mn = values[0];
        int64_t mx = values[0];
        for (size_t i = 1; i < count; ++i) {
          mn = std::min(mn, values[i]);
          mx = std::max(mx, values[i]);
        }
        if (type == ValueType::kDate) {
          zone_map->AddDateRange(mn, mx, count);
        } else {
          zone_map->AddIntRange(mn, mx, count);
        }
      }
    }
    return;
  }
  size_t null_cnt = 0;
  size_t non_null = 0;
  int64_t mn = 0;
  int64_t mx = 0;
  for (size_t i = 0; i < count; ++i) {
    if (nulls[i] != 0) {
      null_bitmap_[(old_size + i) / 64] |= uint64_t{1} << ((old_size + i) % 64);
      ++null_cnt;
    } else if (zone_map != nullptr) {
      if (is_unsigned) {
        Value v(values[i]);
        v.SetUnsigned();
        zone_map->Add(v);
      } else {
        if (non_null == 0) {
          mn = mx = values[i];
        } else {
          mn = std::min(mn, values[i]);
          mx = std::max(mx, values[i]);
        }
        ++non_null;
      }
    }
  }
  if (zone_map != nullptr) {
    if (null_cnt > 0) {
      zone_map->AddNulls(null_cnt);
    }
    if (!is_unsigned && non_null > 0) {
      if (type == ValueType::kDate) {
        zone_map->AddDateRange(mn, mx, non_null);
      } else {
        zone_map->AddIntRange(mn, mx, non_null);
      }
    }
  }
}

void ColumnVector::AppendDoubleBatch(const double* values, const uint8_t* nulls,
                                     size_t count, ZoneMap* zone_map) {
  if (count == 0 || values == nullptr) {
    return;  // T1938: see AppendIntBatch.
  }
  if (!heterogeneous_ && type_ == ValueType::kNull) {
    type_ = ValueType::kDouble;
    unsigned_ = false;
    MaterializeInferredStorage();
  }
  if (heterogeneous_ || type_ != ValueType::kDouble || unsigned_) {
    for (size_t i = 0; i < count; ++i) {
      Value v =
          (nulls != nullptr && nulls[i] != 0) ? Value() : Value(values[i]);
      if (zone_map != nullptr) {
        zone_map->Add(v);
      }
      Append(std::move(v));
    }
    return;
  }
  const size_t old_size = size_;
  size_ = old_size + count;
  null_bitmap_.resize((size_ + 63) / 64, 0);
  // T4752: same single-pass append as AppendIntBatch (resize+memcpy was
  // memset-then-overwrite of every slot).
  doubles_.reserve(size_);
  doubles_.insert(doubles_.end(), values, values + count);
  size_t null_cnt = 0;
  size_t non_null = 0;
  bool has_bounds = false;
  bool saw_nan = false;
  double mn = 0.0;
  double mx = 0.0;
  if (nulls == nullptr) {
    non_null = count;
    if (zone_map != nullptr) {
      for (size_t i = 0; i < count; ++i) {
        const double d = values[i];
        if (std::isnan(d)) {
          saw_nan = true;
        } else if (!has_bounds) {
          has_bounds = true;
          mn = mx = d;
        } else {
          mn = std::min(mn, d);
          mx = std::max(mx, d);
        }
      }
    }
  } else {
    for (size_t i = 0; i < count; ++i) {
      if (nulls[i] != 0) {
        null_bitmap_[(old_size + i) / 64] |= uint64_t{1}
                                             << ((old_size + i) % 64);
        ++null_cnt;
      } else {
        ++non_null;
        if (zone_map != nullptr) {
          const double d = values[i];
          if (std::isnan(d)) {
            saw_nan = true;
          } else if (!has_bounds) {
            has_bounds = true;
            mn = mx = d;
          } else {
            mn = std::min(mn, d);
            mx = std::max(mx, d);
          }
        }
      }
    }
  }
  if (zone_map != nullptr) {
    if (null_cnt > 0) {
      zone_map->AddNulls(null_cnt);
    }
    if (non_null > 0) {
      zone_map->AddDoubleRange(mn, mx, has_bounds, saw_nan, non_null);
    }
  }
}

void ColumnVector::AppendStringViewBatch(const std::string_view* values,
                                         const uint8_t* nulls, size_t count,
                                         ZoneMap* zone_map) {
  if (count == 0 || values == nullptr) {
    return;  // T1938: see AppendIntBatch.
  }
  if (!heterogeneous_ && type_ == ValueType::kNull) {
    type_ = ValueType::kVarChar;
    unsigned_ = false;
    MaterializeInferredStorage();
  }
  if (heterogeneous_ || type_ != ValueType::kVarChar) {
    for (size_t i = 0; i < count; ++i) {
      Value v = (nulls != nullptr && nulls[i] != 0)
                    ? Value()
                    : Value(std::string(values[i]));
      if (zone_map != nullptr) {
        zone_map->Add(v);
      }
      Append(std::move(v));
    }
    return;
  }
  const size_t old_size = size_;
  size_ = old_size + count;
  null_bitmap_.resize((size_ + 63) / 64, 0);
  strings_.reserve(size_);
  size_t null_cnt = 0;
  size_t non_null = 0;
  std::string_view mn;
  std::string_view mx;
  if (nulls == nullptr) {
    for (size_t i = 0; i < count; ++i) {
      strings_.emplace_back(values[i]);
      if (zone_map != nullptr) {
        if (i == 0) {
          mn = mx = values[0];
        } else {
          if (values[i] < mn) {
            mn = values[i];
          }
          if (mx < values[i]) {
            mx = values[i];
          }
        }
      }
    }
    non_null = count;
  } else {
    for (size_t i = 0; i < count; ++i) {
      if (nulls[i] != 0) {
        null_bitmap_[(old_size + i) / 64] |= uint64_t{1}
                                             << ((old_size + i) % 64);
        strings_.emplace_back();
        ++null_cnt;
      } else {
        strings_.emplace_back(values[i]);
        if (zone_map != nullptr) {
          if (non_null == 0) {
            mn = mx = values[i];
          } else {
            if (values[i] < mn) {
              mn = values[i];
            }
            if (mx < values[i]) {
              mx = values[i];
            }
          }
        }
        ++non_null;
      }
    }
  }
  if (zone_map != nullptr) {
    if (null_cnt > 0) {
      zone_map->AddNulls(null_cnt);
    }
    if (non_null > 0) {
      zone_map->AddStringRange(mn, mx, non_null);
    }
  }
}

void ColumnVector::Reset() {
  size_ = 0;
  unsigned_ = false;
  heterogeneous_ = false;
  collation_ = Collation::kNone;
  observed_collation_ = false;
  mixed_collation_ = false;
  null_bitmap_.clear();
  integers_.clear();
  doubles_.clear();
  strings_.clear();
  nested_.clear();
}

void ColumnVector::Reserve(size_t capacity) {
  null_bitmap_.reserve((capacity + 63) / 64);
  switch (type_) {
    case ValueType::kInt64:
    case ValueType::kDate:
      integers_.reserve(capacity);
      break;
    case ValueType::kDouble:
      doubles_.reserve(capacity);
      break;
    case ValueType::kVarChar:
      strings_.reserve(capacity);
      break;
    case ValueType::kArray:
      nested_.reserve(capacity);
      break;
    case ValueType::kNull:
      break;
  }
}

void ColumnVector::EnsureNullBit(size_t index, bool is_null) {
  const size_t word = index / 64;
  if (null_bitmap_.size() <= word) {
    null_bitmap_.resize(word + 1, 0);
  }
  const uint64_t mask = uint64_t{1} << (index % 64);
  if (is_null) {
    null_bitmap_[word] |= mask;
  } else {
    null_bitmap_[word] &= ~mask;
  }
}

bool ColumnVector::IsNull(size_t index) const {
  assert(index < size_ && "ColumnVector::IsNull out of range");
  return (null_bitmap_[index / 64] & (uint64_t{1} << (index % 64))) != 0;
}

void ColumnVector::AppendDefault() {
  switch (type_) {
    case ValueType::kInt64:
    case ValueType::kDate:
      integers_.push_back(0);
      break;
    case ValueType::kDouble:
      doubles_.push_back(0.0);
      break;
    case ValueType::kVarChar:
      strings_.emplace_back();
      break;
    case ValueType::kArray:
      nested_.emplace_back();
      break;
    case ValueType::kNull:
      break;
  }
}

void ColumnVector::MaterializeInferredStorage() {
  switch (type_) {
    case ValueType::kInt64:
    case ValueType::kDate:
      integers_.resize(size_);
      break;
    case ValueType::kDouble:
      doubles_.resize(size_);
      break;
    case ValueType::kVarChar:
      strings_.resize(size_);
      break;
    case ValueType::kArray:
      nested_.resize(size_);
      break;
    case ValueType::kNull:
      break;
  }
}

Value ColumnVector::ValueAt(size_t index) const {
  assert(index < size_ && "ColumnVector::ValueAt out of range");
  if (IsNull(index)) {
    return {};
  }
  if (heterogeneous_) {
    return nested_[index];
  }
  switch (type_) {
    case ValueType::kInt64:
      return unsigned_ ? Value(integers_[index]).WithUnsigned()
                       : Value(integers_[index]);
    case ValueType::kDate:
      return Value::DateFromDays(integers_[index]);
    case ValueType::kDouble:
      return Value(doubles_[index]);
    case ValueType::kVarChar: {
      const Collation collation = ColumnCollation();
      return collation == Collation::kNone
                 ? Value(std::string(strings_[index]))
                 : Value(std::string(strings_[index]))
                       .WithCollation(static_cast<uint8_t>(collation));
    }
    case ValueType::kArray:
      return nested_[index];
    case ValueType::kNull:
      return {};
  }
  return {};
}

DataChunk::DataChunk(const Schema& schema, size_t capacity) {
  Initialize(schema, capacity);
}

DataChunk::DataChunk(const std::vector<ValueType>& types, size_t capacity) {
  Initialize(types, capacity);
}

void DataChunk::Initialize(const Schema& schema, size_t capacity) {
  schema_fixed_ = false;
  rebind_on_first_row_ = false;
  // Same-layout chunks (the per-batch Reset path) keep their allocated
  // ColumnVectors: rebuilding the column vector would free and re-allocate
  // every column's storage each 1024-row batch. Only a column-count or type
  // change (a different operator tap on the same chunk object) takes the
  // rebuild path.
  if (columns_.size() == schema.ColumnCount() &&
      zone_maps_.size() == columns_.size()) {
    bool match = true;
    for (size_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].Type() != schema.GetColumn(i).Type() ||
          columns_[i].IsHeterogeneous()) {
        match = false;
        break;
      }
    }
    if (match) {
      for (size_t i = 0; i < columns_.size(); ++i) {
        columns_[i].Reset();
        columns_[i].SetAllowHeterogeneous(true);
        // ColumnVector::Reset clears the unsigned marker; restore the
        // schema's per-column flag the constructor would have set.
        columns_[i].SetUnsigned(schema.GetColumn(i).IsUnsigned());
      }
      for (ZoneMap& zone_map : zone_maps_) {
        zone_map.Reset();
      }
      positions_.clear();
      positions_.reserve(capacity);
      size_ = 0;
      return;
    }
  }
  columns_.clear();
  columns_.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    columns_.emplace_back(schema.GetColumn(i).Type(), capacity);
    columns_.back().SetAllowHeterogeneous(true);
    if (schema.GetColumn(i).IsUnsigned()) {
      columns_.back().SetUnsigned(true);
    }
  }
  zone_maps_.clear();
  zone_maps_.resize(schema.ColumnCount());
  positions_.clear();
  positions_.reserve(capacity);
  size_ = 0;
}

void DataChunk::Initialize(const std::vector<ValueType>& types,
                           size_t capacity) {
  schema_fixed_ = true;
  rebind_on_first_row_ = false;
  if (columns_.size() == types.size() && zone_maps_.size() == columns_.size()) {
    bool match = true;
    for (size_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].Type() != types[i] || columns_[i].IsHeterogeneous()) {
        match = false;
        break;
      }
    }
    if (match) {
      for (ColumnVector& column : columns_) {
        column.Reset();
        column.SetAllowHeterogeneous(false);
      }
      for (ZoneMap& zone_map : zone_maps_) {
        zone_map.Reset();
      }
      positions_.clear();
      positions_.reserve(capacity);
      size_ = 0;
      return;
    }
  }
  columns_.clear();
  columns_.reserve(types.size());
  for (ValueType type : types) {
    columns_.emplace_back(type, capacity);
    columns_.back().SetAllowHeterogeneous(false);
  }
  zone_maps_.clear();
  zone_maps_.resize(types.size());
  positions_.clear();
  positions_.reserve(capacity);
  size_ = 0;
}

void DataChunk::Reset() {
  schema_fixed_ = false;
  rebind_on_first_row_ = true;
  for (ColumnVector& column : columns_) {
    column.Reset();
    column.SetAllowHeterogeneous(true);
  }
  for (ZoneMap& zone_map : zone_maps_) {
    zone_map.Reset();
  }
  positions_.clear();
  size_ = 0;
}

void DataChunk::Reset(const Schema& schema, size_t capacity) {
  Initialize(schema, capacity);
}

bool DataChunk::HasLayout(const Schema& schema) const {
  if (ColumnCount() != schema.ColumnCount()) {
    return false;
  }
  for (size_t i = 0; i < ColumnCount(); ++i) {
    const ValueType have = columns_[i].Type();
    if (have == ValueType::kNull) {
      continue;
    }
    if (have != schema.GetColumn(i).Type()) {
      return false;
    }
  }
  return true;
}

void DataChunk::Reserve(size_t capacity) {
  for (ColumnVector& column : columns_) {
    column.Reserve(capacity);
  }
  positions_.reserve(capacity);
}

void DataChunk::EnsureLayout(const Row& row) {
  if (size_ == 0 &&
      (columns_.empty() ||
       (!schema_fixed_ && columns_.size() != row.values_.size()))) {
    std::vector<ValueType> types;
    types.reserve(row.values_.size());
    for (const Value& value : row.values_) {
      types.push_back(value.IsNull() ? ValueType::kNull : value.type);
    }
    Initialize(types);
    schema_fixed_ = false;
    for (size_t i = 0; i < row.values_.size(); ++i) {
      columns_[i].SetAllowHeterogeneous(true);
      if (row.values_[i].IsUnsigned()) {
        columns_[i].SetUnsigned(true);
      }
    }
    return;
  }
  if (size_ == 0 && !schema_fixed_) {
    for (size_t i = 0; i < row.values_.size(); ++i) {
      if (row.values_[i].IsNull() && !rebind_on_first_row_ &&
          !columns_[i].IsHeterogeneous()) {
        columns_[i].SetAllowHeterogeneous(true);
        continue;
      }
      const ValueType vt =
          row.values_[i].IsNull() ? ValueType::kNull : row.values_[i].type;
      if (columns_[i].Type() != vt ||
          columns_[i].IsUnsigned() != row.values_[i].IsUnsigned() ||
          columns_[i].IsHeterogeneous()) {
        columns_[i].RebindEmptyType(vt, row.values_[i].IsUnsigned());
      }
      columns_[i].SetAllowHeterogeneous(true);
    }
    return;
  }
  if (row.values_.size() != columns_.size()) {
    CHECK_MSG(false, "data chunk row width mismatch");
  }
}

void DataChunk::Append(const Row& row, RowPosition position) {
  EnsureLayout(row);
  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i].Append(row[i]);
    zone_maps_[i].Add(row[i]);
  }
  positions_.push_back(position);
  ++size_;
}

void DataChunk::Append(Row&& row, RowPosition position) {
  EnsureLayout(row);
  for (size_t i = 0; i < columns_.size(); ++i) {
    zone_maps_[i].Add(row[i]);
    columns_[i].Append(std::move(row[i]));
  }
  positions_.push_back(position);
  ++size_;
}

void DataChunk::AppendFrom(const DataChunk& source, size_t row_index) {
  if (row_index >= source.Size()) {
    CHECK_MSG(false, "data chunk append row out of range");
  }
  if (size_ == 0 && (columns_.empty() || !schema_fixed_)) {
    std::vector<ValueType> types;
    types.reserve(source.ColumnCount());
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      types.push_back(source.ColumnAt(i).Type());
    }
    Initialize(types);
    schema_fixed_ = false;
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      columns_[i].SetAllowHeterogeneous(true);
      if (source.ColumnAt(i).IsUnsigned()) {
        columns_[i].SetUnsigned(true);
      }
    }
  }
  if (ColumnCount() != source.ColumnCount()) {
    CHECK_MSG(false, "data chunk width mismatch");
  }
  for (size_t i = 0; i < columns_.size(); ++i) {
    const ColumnVector& column = source.ColumnAt(i);
    columns_[i].AppendFrom(column, row_index);
    UpdateZoneMapFrom(column, row_index, &zone_maps_[i]);
  }
  positions_.push_back(source.PositionAt(row_index));
  ++size_;
}

void DataChunk::AppendRowFromColumns(
    const std::vector<const ColumnVector*>& sources, size_t row_index,
    RowPosition position) {
  for (const ColumnVector* source : sources) {
    if (row_index >= source->Size()) {
      CHECK_MSG(false, "data chunk append row out of range");
    }
  }
  if (size_ == 0 && (columns_.empty() || !schema_fixed_)) {
    std::vector<ValueType> types;
    types.reserve(sources.size());
    for (const ColumnVector* source : sources) {
      types.push_back(source->Type());
    }
    Initialize(types);
    schema_fixed_ = false;
    for (size_t i = 0; i < sources.size(); ++i) {
      columns_[i].SetAllowHeterogeneous(true);
      if (sources[i]->IsUnsigned()) {
        columns_[i].SetUnsigned(true);
      }
    }
  }
  if (columns_.size() != sources.size()) {
    CHECK_MSG(false, "data chunk row width mismatch");
  }
  for (size_t i = 0; i < columns_.size(); ++i) {
    const ColumnVector* source = sources[i];
    columns_[i].AppendFrom(*source, row_index);
    UpdateZoneMapFrom(*source, row_index, &zone_maps_[i]);
  }
  positions_.push_back(position);
  ++size_;
}

void DataChunk::AppendGather(const DataChunk& source, const uint32_t* selection,
                             size_t count) {
  for (size_t i = 0; i < count; ++i) {
    if (selection[i] >= source.Size()) {
      CHECK_MSG(false, "data chunk append row out of range");
    }
  }
  if (size_ == 0 && (columns_.empty() || !schema_fixed_)) {
    std::vector<ValueType> types;
    types.reserve(source.ColumnCount());
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      types.push_back(source.ColumnAt(i).Type());
    }
    Initialize(types);
    schema_fixed_ = false;
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      columns_[i].SetAllowHeterogeneous(true);
      if (source.ColumnAt(i).IsUnsigned()) {
        columns_[i].SetUnsigned(true);
      }
    }
  }
  if (ColumnCount() != source.ColumnCount()) {
    CHECK_MSG(false, "data chunk width mismatch");
  }
  bool identity = count == source.Size();
  for (size_t i = 0; identity && i < count; ++i) {
    identity = selection[i] == i;
  }
  if (identity) {
    for (size_t c = 0; c < columns_.size(); ++c) {
      columns_[c].AppendRange(source.ColumnAt(c));
      zone_maps_[c].Merge(source.ZoneMapAt(c));
    }
    positions_.insert(positions_.end(), source.positions_.begin(),
                      source.positions_.end());
    size_ += count;
    return;
  }
  // Column-major gather: each column streams its gathered cells into one
  // contiguous output buffer, instead of hopping across all columns per row
  // (which re-faults each column's destination line for every row).
  for (size_t c = 0; c < columns_.size(); ++c) {
    const ColumnVector& column = source.ColumnAt(c);
    ColumnVector& target = columns_[c];
    for (size_t i = 0; i < count; ++i) {
      target.AppendFrom(column, selection[i]);
      UpdateZoneMapFrom(column, selection[i], &zone_maps_[c]);
    }
  }
  const size_t old_size = size_;
  positions_.resize(old_size + count);
  for (size_t i = 0; i < count; ++i) {
    positions_[old_size + i] = source.PositionAt(selection[i]);
  }
  size_ += count;
}

void DataChunk::CommitBatchAppend(size_t appended_rows) {
  if (appended_rows == 0) {
    return;
  }
  // T1940: this API trusts the caller to have grown every column's storage
  // out-of-band and only advances size_/positions_. Over-reporting used to
  // silently leave size_ ahead of ColumnVector::Size(), so RowAt/ValueAt read
  // out-of-bounds cells and zone maps drifted. Clamp to the rows every column
  // actually holds (a column-less chunk keeps the historical behaviour) so
  // the size invariant Append/AppendGather maintain cannot be broken by a
  // miscounted caller.
  for (const ColumnVector& column : columns_) {
    const size_t available = column.Size() > size_ ? column.Size() - size_ : 0;
    appended_rows = std::min(appended_rows, available);
  }
  if (appended_rows == 0) {
    return;
  }
  rebind_on_first_row_ = false;
  positions_.resize(size_ + appended_rows);
  size_ += appended_rows;
}

Row DataChunk::RowAt(size_t row_index) const {
  assert(row_index < size_ && "DataChunk::RowAt out of range");
  std::vector<Value> values;
  values.reserve(columns_.size());
  for (const ColumnVector& column : columns_) {
    values.push_back(column.ValueAt(row_index));
  }
  return Row(std::move(values));
}

Value ColumnVector::AggregateLogicalAnd(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return {};
  }
  bool has_non_null = false;
  if (sel == nullptr) {
    if (type_ == ValueType::kInt64) {
      const int64_t* data = integers_.data();
      const size_t words = (size_ + 63) / 64;
      for (size_t w = 0; w < words; ++w) {
        const size_t base = w * 64;
        const size_t limit = std::min(size_ - base, size_t{64});
        const uint64_t null_word =
            w < null_bitmap_.size() ? null_bitmap_[w] : 0;
        for (size_t i = 0; i < limit; ++i) {
          if ((null_word & (1ULL << i)) == 0) {
            has_non_null = true;
            if (data[base + i] == 0) {
              return Value(int64_t{0});
            }
          }
        }
      }
    } else {
      for (size_t i = 0; i < size_; ++i) {
        if (!IsNull(i)) {
          has_non_null = true;
          if (!ValueAt(i).Truthy()) {
            return Value(int64_t{0});
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < sel->Size(); ++i) {
      const size_t idx = (*sel)[i];
      if (idx < size_ && !IsNull(idx)) {
        has_non_null = true;
        if (!ValueAt(idx).Truthy()) {
          return Value(int64_t{0});
        }
      }
    }
  }
  if (!has_non_null) {
    return {};
  }
  return Value(int64_t{1});
}

Value ColumnVector::AggregateLogicalOr(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return {};
  }
  bool has_non_null = false;
  if (sel == nullptr) {
    if (type_ == ValueType::kInt64) {
      const int64_t* data = integers_.data();
      const size_t words = (size_ + 63) / 64;
      for (size_t w = 0; w < words; ++w) {
        const size_t base = w * 64;
        const size_t limit = std::min(size_ - base, size_t{64});
        const uint64_t null_word =
            w < null_bitmap_.size() ? null_bitmap_[w] : 0;
        for (size_t i = 0; i < limit; ++i) {
          if ((null_word & (1ULL << i)) == 0) {
            has_non_null = true;
            if (data[base + i] != 0) {
              return Value(int64_t{1});
            }
          }
        }
      }
    } else {
      for (size_t i = 0; i < size_; ++i) {
        if (!IsNull(i)) {
          has_non_null = true;
          if (ValueAt(i).Truthy()) {
            return Value(int64_t{1});
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < sel->Size(); ++i) {
      const size_t idx = (*sel)[i];
      if (idx < size_ && !IsNull(idx)) {
        has_non_null = true;
        if (ValueAt(idx).Truthy()) {
          return Value(int64_t{1});
        }
      }
    }
  }
  if (!has_non_null) {
    return {};
  }
  return Value(int64_t{0});
}

Value ColumnVector::AggregateBitAnd(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return {};
  }
  if (type_ != ValueType::kInt64) {
    CHECK_MSG(false, "BIT_AND requires int64 column");
  }
  const int64_t* data = integers_.data();
  uint64_t acc = ~uint64_t{0};
  bool has_non_null = false;

  if (sel == nullptr) {
    const size_t words = (size_ + 63) / 64;
    for (size_t w = 0; w < words; ++w) {
      const size_t base = w * 64;
      const size_t limit = std::min(size_ - base, size_t{64});
      const uint64_t null_word = w < null_bitmap_.size() ? null_bitmap_[w] : 0;
      if (null_word == 0) {
#ifdef __clang__
// Sanitizer checks inhibit this reduction's vectorization. Keep those
// checks intact and request SIMD only for uninstrumented builds.
#if !__has_feature(undefined_behavior_sanitizer) && \
    !__has_feature(address_sanitizer) && !__has_feature(thread_sanitizer)
#pragma clang loop vectorize(enable)
#endif
#endif
        for (size_t i = 0; i < limit; ++i) {
          acc &= static_cast<uint64_t>(data[base + i]);
        }
        has_non_null = true;
      } else if (null_word == ~uint64_t{0}) {
        continue;
      } else {
        for (size_t i = 0; i < limit; ++i) {
          if ((null_word & (1ULL << i)) == 0) {
            acc &= static_cast<uint64_t>(data[base + i]);
            has_non_null = true;
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < sel->Size(); ++i) {
      const size_t idx = (*sel)[i];
      if (idx < size_ && !IsNull(idx)) {
        acc &= static_cast<uint64_t>(data[idx]);
        has_non_null = true;
      }
    }
  }
  if (!has_non_null) {
    return {};
  }
  Value res_and(static_cast<int64_t>(acc));
  return unsigned_ ? res_and.WithUnsigned() : res_and;
}

Value ColumnVector::AggregateBitOr(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return {};
  }
  if (type_ != ValueType::kInt64) {
    CHECK_MSG(false, "BIT_OR requires int64 column");
  }
  const int64_t* data = integers_.data();
  uint64_t acc = 0;
  bool has_non_null = false;

  if (sel == nullptr) {
    const size_t words = (size_ + 63) / 64;
    for (size_t w = 0; w < words; ++w) {
      const size_t base = w * 64;
      const size_t limit = std::min(size_ - base, size_t{64});
      const uint64_t null_word = w < null_bitmap_.size() ? null_bitmap_[w] : 0;
      if (null_word == 0) {
#ifdef __clang__
// Sanitizer checks inhibit this reduction's vectorization. Keep those
// checks intact and request SIMD only for uninstrumented builds.
#if !__has_feature(undefined_behavior_sanitizer) && \
    !__has_feature(address_sanitizer) && !__has_feature(thread_sanitizer)
#pragma clang loop vectorize(enable)
#endif
#endif
        for (size_t i = 0; i < limit; ++i) {
          acc |= static_cast<uint64_t>(data[base + i]);
        }
        has_non_null = true;
      } else if (null_word == ~uint64_t{0}) {
        continue;
      } else {
        for (size_t i = 0; i < limit; ++i) {
          if ((null_word & (1ULL << i)) == 0) {
            acc |= static_cast<uint64_t>(data[base + i]);
            has_non_null = true;
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < sel->Size(); ++i) {
      const size_t idx = (*sel)[i];
      if (idx < size_ && !IsNull(idx)) {
        acc |= static_cast<uint64_t>(data[idx]);
        has_non_null = true;
      }
    }
  }
  if (!has_non_null) {
    return {};
  }
  Value res_or(static_cast<int64_t>(acc));
  return unsigned_ ? res_or.WithUnsigned() : res_or;
}

Value ColumnVector::AggregateBitXor(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return {};
  }
  if (type_ != ValueType::kInt64) {
    CHECK_MSG(false, "BIT_XOR requires int64 column");
  }
  const int64_t* data = integers_.data();
  uint64_t acc = 0;
  bool has_non_null = false;

  if (sel == nullptr) {
    const size_t words = (size_ + 63) / 64;
    for (size_t w = 0; w < words; ++w) {
      const size_t base = w * 64;
      const size_t limit = std::min(size_ - base, size_t{64});
      const uint64_t null_word = w < null_bitmap_.size() ? null_bitmap_[w] : 0;
      if (null_word == 0) {
#ifdef __clang__
// Sanitizer checks inhibit this reduction's vectorization. Keep those
// checks intact and request SIMD only for uninstrumented builds.
#if !__has_feature(undefined_behavior_sanitizer) && \
    !__has_feature(address_sanitizer) && !__has_feature(thread_sanitizer)
#pragma clang loop vectorize(enable)
#endif
#endif
        for (size_t i = 0; i < limit; ++i) {
          acc ^= static_cast<uint64_t>(data[base + i]);
        }
        has_non_null = true;
      } else if (null_word == ~uint64_t{0}) {
        continue;
      } else {
        for (size_t i = 0; i < limit; ++i) {
          if ((null_word & (1ULL << i)) == 0) {
            acc ^= static_cast<uint64_t>(data[base + i]);
            has_non_null = true;
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < sel->Size(); ++i) {
      const size_t idx = (*sel)[i];
      if (idx < size_ && !IsNull(idx)) {
        acc ^= static_cast<uint64_t>(data[idx]);
        has_non_null = true;
      }
    }
  }
  if (!has_non_null) {
    return {};
  }
  Value res_xor(static_cast<int64_t>(acc));
  return unsigned_ ? res_xor.WithUnsigned() : res_xor;
}

Value DataChunk::AggregateLogicalAnd(size_t col_idx,
                                     const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    CHECK_MSG(false, "DataChunk::AggregateLogicalAnd col_idx out of range");
  }
  return columns_[col_idx].AggregateLogicalAnd(sel);
}

Value DataChunk::AggregateLogicalOr(size_t col_idx,
                                    const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    CHECK_MSG(false, "DataChunk::AggregateLogicalOr col_idx out of range");
  }
  return columns_[col_idx].AggregateLogicalOr(sel);
}

Value DataChunk::AggregateBitAnd(size_t col_idx,
                                 const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    CHECK_MSG(false, "DataChunk::AggregateBitAnd col_idx out of range");
  }
  return columns_[col_idx].AggregateBitAnd(sel);
}

Value DataChunk::AggregateBitOr(size_t col_idx,
                                const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    CHECK_MSG(false, "DataChunk::AggregateBitOr col_idx out of range");
  }
  return columns_[col_idx].AggregateBitOr(sel);
}

Value DataChunk::AggregateBitXor(size_t col_idx,
                                 const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    CHECK_MSG(false, "DataChunk::AggregateBitXor col_idx out of range");
  }
  return columns_[col_idx].AggregateBitXor(sel);
}

}  // namespace tinylamb
