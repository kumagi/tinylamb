/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/data_chunk.hpp"

#include <stdexcept>
#include <utility>

namespace tinylamb {

ColumnVector::ColumnVector(ValueType type, size_t capacity) : type_(type) {
  Reserve(capacity);
}

void ColumnVector::Append(Value value) {
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
    case ValueType::kNull:
      break;
  }
}

void ColumnVector::EnsureNullBit(size_t index, bool is_null) {
  const size_t word = index / 64;
  if (null_bitmap_.size() <= word) null_bitmap_.resize(word + 1, 0);
  const uint64_t mask = uint64_t{1} << (index % 64);
  if (is_null) {
    null_bitmap_[word] |= mask;
  } else {
    null_bitmap_[word] &= ~mask;
  }
}

bool ColumnVector::IsNull(size_t index) const {
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
    case ValueType::kNull:
      break;
  }
}

Value ColumnVector::ValueAt(size_t index) const {
  if (IsNull(index)) return Value();
  switch (type_) {
    case ValueType::kInt64:
      return Value(integers_[index]);
    case ValueType::kDate:
      return Value::DateFromDays(integers_[index]);
    case ValueType::kDouble:
      return Value(doubles_[index]);
    case ValueType::kVarChar:
      return Value(std::string(strings_[index]));
    case ValueType::kNull:
      return Value();
  }
  return Value();
}

DataChunk::DataChunk(const Schema& schema, size_t capacity) {
  Initialize(schema, capacity);
}

DataChunk::DataChunk(std::vector<ValueType> types, size_t capacity) {
  Initialize(std::move(types), capacity);
}

void DataChunk::Initialize(const Schema& schema, size_t capacity) {
  std::vector<ValueType> types;
  types.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    types.push_back(schema.GetColumn(i).Type());
  }
  Initialize(std::move(types), capacity);
}

void DataChunk::Initialize(std::vector<ValueType> types, size_t capacity) {
  columns_.clear();
  columns_.reserve(types.size());
  for (ValueType type : types) columns_.emplace_back(type, capacity);
  zone_maps_.clear();
  zone_maps_.resize(types.size());
  positions_.clear();
  positions_.reserve(capacity);
  size_ = 0;
}

void DataChunk::Reset() {
  for (ColumnVector& column : columns_) column.Reset();
  for (ZoneMap& zone_map : zone_maps_) zone_map.Reset();
  positions_.clear();
  size_ = 0;
}

void DataChunk::Reset(const Schema& schema, size_t capacity) {
  Initialize(schema, capacity);
}

bool DataChunk::HasLayout(const Schema& schema) const {
  if (ColumnCount() != schema.ColumnCount()) return false;
  for (size_t i = 0; i < ColumnCount(); ++i) {
    const ValueType have = columns_[i].Type();
    if (have == ValueType::kNull) continue;
    if (have != schema.GetColumn(i).Type()) return false;
  }
  return true;
}

void DataChunk::Reserve(size_t capacity) {
  for (ColumnVector& column : columns_) column.Reserve(capacity);
  positions_.reserve(capacity);
}

void DataChunk::EnsureLayout(const Row& row) {
  if (columns_.empty() && size_ == 0) {
    std::vector<ValueType> types;
    types.reserve(row.values_.size());
    for (const Value& value : row.values_) {
      types.push_back(value.IsNull() ? ValueType::kNull : value.type);
    }
    Initialize(std::move(types));
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
  if (columns_.empty() && size_ == 0) {
    std::vector<ValueType> types;
    types.reserve(source.ColumnCount());
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      types.push_back(source.ColumnAt(i).Type());
    }
    Initialize(std::move(types));
  }
  if (ColumnCount() != source.ColumnCount()) {
    throw std::invalid_argument("data chunk width mismatch");
  }
  for (size_t i = 0; i < columns_.size(); ++i) {
    Value value = source.ColumnAt(i).ValueAt(row_index);
    columns_[i].Append(value);
    zone_maps_[i].Add(value);
  }
  positions_.push_back(source.PositionAt(row_index));
  ++size_;
}

Row DataChunk::RowAt(size_t row_index) const {
  std::vector<Value> values;
  values.reserve(columns_.size());
  for (const ColumnVector& column : columns_) {
    values.push_back(column.ValueAt(row_index));
  }
  return Row(std::move(values));
}

}  // namespace tinylamb
