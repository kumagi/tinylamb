/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "page/pax_block.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <unordered_map>

namespace tinylamb {
namespace {

void Pack(std::vector<uint8_t>* output, uint64_t value, size_t index,
          uint8_t width) {
  const size_t bit_offset = index * width;
  for (uint8_t bit = 0; bit < width; ++bit) {
    if ((value & (uint64_t{1} << bit)) == 0) continue;
    const size_t destination = bit_offset + bit;
    (*output)[destination / 8] |= uint8_t{1} << (destination % 8);
  }
}

}  // namespace

PaxColumnBlock PaxColumnBlock::Encode(const ColumnVector& column) {
  PaxColumnBlock result;
  result.type_ = column.Type();
  result.size_ = column.Size();
  result.null_bitmap_ = column.NullBitmap();

  if (column.Type() == ValueType::kInt64 ||
      column.Type() == ValueType::kDate) {
    bool any = false;
    int64_t minimum = std::numeric_limits<int64_t>::max();
    int64_t maximum = std::numeric_limits<int64_t>::min();
    for (size_t row = 0; row < column.Size(); ++row) {
      if (column.IsNull(row)) continue;
      any = true;
      const int64_t value = column.ValueAt(row).value.int_value;
      minimum = std::min(minimum, value);
      maximum = std::max(maximum, value);
    }
    if (any) {
      const uint64_t range = static_cast<uint64_t>(maximum) -
                             static_cast<uint64_t>(minimum);
      result.bit_width_ = static_cast<uint8_t>(std::bit_width(range));
      result.frame_base_ = minimum;
      result.encoding_ = PaxEncoding::kBitPacked;
      result.packed_.resize((column.Size() * result.bit_width_ + 7) / 8);
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) continue;
        const uint64_t delta =
            static_cast<uint64_t>(column.ValueAt(row).value.int_value) -
            static_cast<uint64_t>(minimum);
        Pack(&result.packed_, delta, row, result.bit_width_);
      }
      return result;
    }
  }

  if (column.Type() == ValueType::kVarChar) {
    std::unordered_map<std::string, uint32_t> ids;
    result.dictionary_ids_.resize(column.Size());
    size_t dictionary_bytes = 0;
    size_t plain_bytes = 0;
    for (size_t row = 0; row < column.Size(); ++row) {
      if (column.IsNull(row)) continue;
      std::string value(column.ValueAt(row).value.varchar_value);
      plain_bytes += value.size();
      auto [iter, inserted] =
          ids.emplace(value, static_cast<uint32_t>(ids.size()));
      if (inserted) {
        dictionary_bytes += value.size();
        result.dictionary_.push_back(std::move(value));
      }
      result.dictionary_ids_[row] = iter->second;
    }
    const uint8_t id_width = static_cast<uint8_t>(
        std::bit_width(std::max<size_t>(1, result.dictionary_.size()) - 1));
    const size_t packed_id_bytes = (column.Size() * id_width + 7) / 8;
    if (dictionary_bytes + packed_id_bytes < plain_bytes) {
      result.encoding_ = PaxEncoding::kDictionary;
      result.bit_width_ = id_width;
      result.packed_.resize(packed_id_bytes);
      for (size_t row = 0; row < column.Size(); ++row) {
        Pack(&result.packed_, result.dictionary_ids_[row], row,
             result.bit_width_);
      }
      result.dictionary_ids_.clear();
      return result;
    }
    result.dictionary_.clear();
    result.dictionary_ids_.clear();
  }

  result.encoding_ = PaxEncoding::kPlain;
  result.plain_.reserve(column.Size());
  for (size_t row = 0; row < column.Size(); ++row) {
    result.plain_.push_back(column.ValueAt(row));
  }
  return result;
}

uint64_t PaxColumnBlock::Unpack(size_t row) const {
  uint64_t value = 0;
  const size_t bit_offset = row * bit_width_;
  for (uint8_t bit = 0; bit < bit_width_; ++bit) {
    const size_t source = bit_offset + bit;
    if ((packed_[source / 8] & (uint8_t{1} << (source % 8))) != 0) {
      value |= uint64_t{1} << bit;
    }
  }
  return value;
}

Value PaxColumnBlock::ValueAt(size_t row) const {
  if ((null_bitmap_[row / 64] & (uint64_t{1} << (row % 64))) != 0) {
    return Value();
  }
  if (encoding_ == PaxEncoding::kDictionary) {
    return Value(std::string(dictionary_[Unpack(row)]));
  }
  if (encoding_ == PaxEncoding::kBitPacked) {
    const int64_t value = frame_base_ + static_cast<int64_t>(Unpack(row));
    return type_ == ValueType::kDate ? Value::DateFromDays(value)
                                     : Value(value);
  }
  return plain_[row];
}

size_t PaxColumnBlock::CompressedBytes() const {
  size_t bytes = null_bitmap_.size() * sizeof(uint64_t) + packed_.size() +
                 dictionary_ids_.size() * sizeof(uint32_t);
  for (const std::string& value : dictionary_) bytes += value.size();
  for (const Value& value : plain_) bytes += value.Size();
  return bytes;
}

PaxBlock PaxBlock::Encode(const DataChunk& chunk) {
  PaxBlock block;
  block.row_count_ = chunk.Size();
  block.columns_.reserve(chunk.ColumnCount());
  for (size_t column = 0; column < chunk.ColumnCount(); ++column) {
    block.columns_.push_back(PaxColumnBlock::Encode(chunk.ColumnAt(column)));
  }
  return block;
}

Row PaxBlock::RowAt(size_t row) const {
  std::vector<Value> values;
  values.reserve(columns_.size());
  for (const PaxColumnBlock& column : columns_) {
    values.push_back(column.ValueAt(row));
  }
  return Row(std::move(values));
}

size_t PaxBlock::CompressedBytes() const {
  size_t bytes = sizeof(PaxPageHeader) +
                 columns_.size() * sizeof(PaxColumnDirectory);
  for (const PaxColumnBlock& column : columns_) {
    bytes += column.CompressedBytes();
  }
  return bytes;
}

}  // namespace tinylamb
