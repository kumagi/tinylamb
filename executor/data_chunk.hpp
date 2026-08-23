/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DATA_CHUNK_HPP
#define TINYLAMB_EXECUTOR_DATA_CHUNK_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "page/row_position.hpp"
#include "executor/zone_map.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

inline constexpr size_t kDefaultVectorSize = 1024;

class ColumnVector {
 public:
  explicit ColumnVector(ValueType type = ValueType::kNull,
                        size_t capacity = kDefaultVectorSize);

  void Append(const Value& value);
  // Copies one value from another column without boxing it into a Value.
  // The source must share this column's type (or be an all-null kNull column).
  void AppendFrom(const ColumnVector& source, size_t index);
  void Reset();
  void Reserve(size_t capacity);

  [[nodiscard]] ValueType Type() const { return type_; }
  [[nodiscard]] size_t Size() const { return size_; }
  [[nodiscard]] bool Empty() const { return size_ == 0; }
  [[nodiscard]] bool IsNull(size_t index) const;
  [[nodiscard]] Value ValueAt(size_t index) const;
  [[nodiscard]] const std::vector<uint64_t>& NullBitmap() const {
    return null_bitmap_;
  }
  [[nodiscard]] const std::vector<int64_t>& IntegerData() const {
    return integers_;
  }
  [[nodiscard]] const std::vector<double>& DoubleData() const {
    return doubles_;
  }
  [[nodiscard]] const std::vector<std::string>& StringData() const {
    return strings_;
  }

 private:
  void AppendDefault();
  void EnsureNullBit(size_t index, bool is_null);
  void MaterializeInferredStorage();

  ValueType type_;
  size_t size_{0};
  std::vector<uint64_t> null_bitmap_;
  std::vector<int64_t> integers_;
  std::vector<double> doubles_;
  std::vector<std::string> strings_;
};

// A fixed-schema, column-oriented batch. Row positions travel with the batch
// so mutation and index operators can retain tuple identity.
class DataChunk {
 public:
  DataChunk() = default;
  explicit DataChunk(const Schema& schema,
                     size_t capacity = kDefaultVectorSize);
  explicit DataChunk(const std::vector<ValueType>& types,
                     size_t capacity = kDefaultVectorSize);

  void Initialize(const Schema& schema,
                  size_t capacity = kDefaultVectorSize);
  void Initialize(const std::vector<ValueType>& types,
                  size_t capacity = kDefaultVectorSize);
  void Reset();
  void Reset(const Schema& schema,
             size_t capacity = kDefaultVectorSize);
  void Reserve(size_t capacity);

  void Append(const Row& row,
              RowPosition position = RowPosition());
  void Append(Row&& row, RowPosition position = RowPosition());
  void Append(const DataChunk& source, size_t row_index);
  // Appends one row assembled column-wise from `sources` without
  // materializing an intermediate Row.  On an empty chunk the layout is
  // inferred from the source column types.
  void AppendRowFromColumns(const std::vector<const ColumnVector*>& sources,
                            size_t row_index,
                            RowPosition position = RowPosition());

  [[nodiscard]] Row RowAt(size_t row_index) const;
  [[nodiscard]] const RowPosition& PositionAt(size_t row_index) const {
    assert(row_index < size_ && "DataChunk::PositionAt out of range");
    return positions_[row_index];
  }
  [[nodiscard]] size_t Size() const { return size_; }
  [[nodiscard]] bool Empty() const { return size_ == 0; }
  [[nodiscard]] size_t ColumnCount() const { return columns_.size(); }
  [[nodiscard]] bool HasLayout(const Schema& schema) const;
  [[nodiscard]] const ColumnVector& ColumnAt(size_t index) const {
    assert(index < columns_.size() && "DataChunk::ColumnAt out of range");
    return columns_[index];
  }
  [[nodiscard]] ColumnVector& ColumnAt(size_t index) {
    assert(index < columns_.size() && "DataChunk::ColumnAt out of range");
    return columns_[index];
  }
  [[nodiscard]] const ZoneMap& ZoneMapAt(size_t index) const {
    assert(index < zone_maps_.size() && "DataChunk::ZoneMapAt out of range");
    return zone_maps_[index];
  }

 private:
  void EnsureLayout(const Row& row);

  std::vector<ColumnVector> columns_;
  std::vector<ZoneMap> zone_maps_;
  std::vector<RowPosition> positions_;
  size_t size_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DATA_CHUNK_HPP
