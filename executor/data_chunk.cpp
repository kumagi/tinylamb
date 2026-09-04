/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/data_chunk.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

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
  switch (column.Type()) {
    case ValueType::kInt64:
      zone_map->AddInt(column.IntegerData()[index]);
      break;
    case ValueType::kDate:
      zone_map->AddDate(column.IntegerData()[index]);
      break;
    case ValueType::kDouble:
      zone_map->AddDouble(column.DoubleData()[index]);
      break;
    case ValueType::kVarChar:
      zone_map->AddString(column.StringData()[index]);
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

void ColumnVector::Append(const Value& value) {
  const bool is_null = value.IsNull();
  if (!is_null && type_ == ValueType::kNull) {
    type_ = value.type;
    MaterializeInferredStorage();
  } else if (!is_null && type_ != value.type) {
    throw std::invalid_argument("column vector type mismatch");
  }
  EnsureNullBit(size_, is_null);
  if (is_null) {
    AppendDefault();
  } else {
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

void ColumnVector::AppendFrom(const ColumnVector& source, size_t index) {
  const bool is_null = source.IsNull(index);
  if (!is_null && type_ == ValueType::kNull) {
    type_ = source.type_;
    MaterializeInferredStorage();
  } else if (!is_null && source.type_ != type_) {
    throw std::invalid_argument("column vector type mismatch");
  }
  EnsureNullBit(size_, is_null);
  if (is_null) {
    AppendDefault();
  } else {
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

void ColumnVector::Reset() {
  size_ = 0;
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
  switch (type_) {
    case ValueType::kInt64:
      return Value(integers_[index]);
    case ValueType::kDate:
      return Value::DateFromDays(integers_[index]);
    case ValueType::kDouble:
      return Value(doubles_[index]);
    case ValueType::kVarChar:
      return Value(std::string(strings_[index]));
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
  std::vector<ValueType> types;
  types.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    types.push_back(schema.GetColumn(i).Type());
  }
  Initialize(types, capacity);
}

void DataChunk::Initialize(const std::vector<ValueType>& types,
                           size_t capacity) {
  columns_.clear();
  columns_.reserve(types.size());
  for (ValueType type : types) {
    columns_.emplace_back(type, capacity);
  }
  zone_maps_.clear();
  zone_maps_.resize(types.size());
  positions_.clear();
  positions_.reserve(capacity);
  size_ = 0;
}

void DataChunk::Reset() {
  for (ColumnVector& column : columns_) {
    column.Reset();
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
  if (columns_.empty() && size_ == 0) {
    std::vector<ValueType> types;
    types.reserve(row.values_.size());
    for (const Value& value : row.values_) {
      types.push_back(value.IsNull() ? ValueType::kNull : value.type);
    }
    Initialize(types);
    return;
  }
  if (row.values_.size() != columns_.size()) {
    throw std::invalid_argument("data chunk row width mismatch");
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
  // Match the lvalue overload: append into columns first so a type mismatch
  // cannot leave zone maps holding a value of the wrong type.
  for (size_t i = 0; i < columns_.size(); ++i) {
    Value value = std::move(row[i]);
    columns_[i].Append(value);
    zone_maps_[i].Add(value);
  }
  positions_.push_back(position);
  ++size_;
}

void DataChunk::Append(const DataChunk& source, size_t row_index) {
  if (row_index >= source.Size()) {
    throw std::out_of_range("data chunk append row out of range");
  }
  if (columns_.empty() && size_ == 0) {
    std::vector<ValueType> types;
    types.reserve(source.ColumnCount());
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      types.push_back(source.ColumnAt(i).Type());
    }
    Initialize(types);
  }
  if (ColumnCount() != source.ColumnCount()) {
    throw std::invalid_argument("data chunk width mismatch");
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
      throw std::out_of_range("data chunk append row out of range");
    }
  }
  if (columns_.empty() && size_ == 0) {
    std::vector<ValueType> types;
    types.reserve(sources.size());
    for (const ColumnVector* source : sources) {
      types.push_back(source->Type());
    }
    Initialize(types);
  }
  if (columns_.size() != sources.size()) {
    throw std::invalid_argument("data chunk row width mismatch");
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
      throw std::out_of_range("data chunk append row out of range");
    }
  }
  if (columns_.empty() && size_ == 0) {
    std::vector<ValueType> types;
    types.reserve(source.ColumnCount());
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      types.push_back(source.ColumnAt(i).Type());
    }
    Initialize(types);
  }
  if (ColumnCount() != source.ColumnCount()) {
    throw std::invalid_argument("data chunk width mismatch");
  }
  for (size_t i = 0; i < count; ++i) {
    const size_t row_index = selection[i];
    for (size_t c = 0; c < columns_.size(); ++c) {
      const ColumnVector& column = source.ColumnAt(c);
      columns_[c].AppendFrom(column, row_index);
      UpdateZoneMapFrom(column, row_index, &zone_maps_[c]);
    }
    positions_.push_back(source.PositionAt(row_index));
    ++size_;
  }
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
    return Value();
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
    return Value();
  }
  return Value(int64_t{1});
}

Value ColumnVector::AggregateLogicalOr(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return Value();
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
    return Value();
  }
  return Value(int64_t{0});
}

Value ColumnVector::AggregateBitAnd(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return Value();
  }
  if (type_ != ValueType::kInt64) {
    throw std::invalid_argument("BIT_AND requires int64 column");
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
#if defined(__clang__)
#pragma clang loop vectorize(enable)
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
    return Value();
  }
  return Value(static_cast<int64_t>(acc));
}

Value ColumnVector::AggregateBitOr(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return Value();
  }
  if (type_ != ValueType::kInt64) {
    throw std::invalid_argument("BIT_OR requires int64 column");
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
#if defined(__clang__)
#pragma clang loop vectorize(enable)
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
    return Value();
  }
  return Value(static_cast<int64_t>(acc));
}

Value ColumnVector::AggregateBitXor(const SelectionVector* sel) const {
  if (size_ == 0 || (sel != nullptr && sel->Empty())) {
    return Value();
  }
  if (type_ != ValueType::kInt64) {
    throw std::invalid_argument("BIT_XOR requires int64 column");
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
#if defined(__clang__)
#pragma clang loop vectorize(enable)
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
    return Value();
  }
  return Value(static_cast<int64_t>(acc));
}

Value DataChunk::AggregateLogicalAnd(size_t col_idx,
                                     const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    throw std::out_of_range(
        "DataChunk::AggregateLogicalAnd col_idx out of range");
  }
  return columns_[col_idx].AggregateLogicalAnd(sel);
}

Value DataChunk::AggregateLogicalOr(size_t col_idx,
                                    const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    throw std::out_of_range(
        "DataChunk::AggregateLogicalOr col_idx out of range");
  }
  return columns_[col_idx].AggregateLogicalOr(sel);
}

Value DataChunk::AggregateBitAnd(size_t col_idx,
                                 const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    throw std::out_of_range("DataChunk::AggregateBitAnd col_idx out of range");
  }
  return columns_[col_idx].AggregateBitAnd(sel);
}

Value DataChunk::AggregateBitOr(size_t col_idx,
                                const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    throw std::out_of_range("DataChunk::AggregateBitOr col_idx out of range");
  }
  return columns_[col_idx].AggregateBitOr(sel);
}

Value DataChunk::AggregateBitXor(size_t col_idx,
                                 const SelectionVector* sel) const {
  if (col_idx >= columns_.size()) {
    throw std::out_of_range("DataChunk::AggregateBitXor col_idx out of range");
  }
  return columns_[col_idx].AggregateBitXor(sel);
}

}  // namespace tinylamb
