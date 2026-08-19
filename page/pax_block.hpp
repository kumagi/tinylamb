/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PAGE_PAX_BLOCK_HPP
#define TINYLAMB_PAGE_PAX_BLOCK_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "executor/data_chunk.hpp"
#include "page/pax_layout.hpp"

namespace tinylamb {

class PaxColumnBlock {
 public:
  static PaxColumnBlock Encode(const ColumnVector& column);
  [[nodiscard]] Value ValueAt(size_t row) const;
  [[nodiscard]] size_t CompressedBytes() const;
  [[nodiscard]] PaxEncoding Encoding() const { return encoding_; }
  [[nodiscard]] ValueType Type() const { return type_; }

 private:
  [[nodiscard]] uint64_t Unpack(size_t row) const;

  ValueType type_{ValueType::kNull};
  PaxEncoding encoding_{PaxEncoding::kPlain};
  size_t size_{0};
  std::vector<uint64_t> null_bitmap_;
  int64_t frame_base_{0};
  uint8_t bit_width_{0};
  std::vector<uint8_t> packed_;
  std::vector<Value> plain_;
  std::vector<std::string> dictionary_;
  std::vector<uint32_t> dictionary_ids_;
};

class PaxBlock {
 public:
  static PaxBlock Encode(const DataChunk& chunk);
  [[nodiscard]] Row RowAt(size_t row) const;
  [[nodiscard]] size_t RowCount() const { return row_count_; }
  [[nodiscard]] size_t ColumnCount() const { return columns_.size(); }
  [[nodiscard]] size_t CompressedBytes() const;
  [[nodiscard]] const PaxColumnBlock& ColumnAt(size_t column) const {
    return columns_[column];
  }

 private:
  size_t row_count_{0};
  std::vector<PaxColumnBlock> columns_;
};

}  // namespace tinylamb
#endif
