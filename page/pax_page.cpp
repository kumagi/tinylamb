/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "page/pax_page.hpp"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "common/serdes.hpp"
#include "page/pax_layout.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

constexpr size_t kHeaderBytes = 32;
constexpr size_t kDirectoryBytes = 28;

struct Directory {
  ValueType type{ValueType::kNull};
  uint32_t data_offset{0};
  uint32_t data_length{0};
  uint32_t null_offset{0};
  uint32_t null_length{0};
};

void WriteHeader(char* out, uint16_t columns, uint16_t rows,
                 uint32_t visibility_offset, uint32_t visibility_length,
                 uint32_t directory_offset, uint32_t payload_begin,
                 uint32_t payload_end) {
  SerializeU16(out + 0, kPaxFormatVersion);
  SerializeU16(out + 2, columns);
  SerializeU16(out + 4, rows);
  SerializeU16(out + 6, rows);
  SerializeU32(out + 8, visibility_offset);
  SerializeU32(out + 12, visibility_length);
  SerializeU32(out + 16, directory_offset);
  SerializeU32(out + 20, payload_begin);
  SerializeU32(out + 24, payload_end);
  SerializeU32(out + 28, 0);
}

void WriteDirectory(char* out, const Directory& directory) {
  out[0] = static_cast<char>(directory.type);
  out[1] = static_cast<char>(PaxEncoding::kPlain);
  SerializeU16(out + 2, 0);
  SerializeU32(out + 4, directory.data_offset);
  SerializeU32(out + 8, directory.data_length);
  SerializeU32(out + 12, directory.null_offset);
  SerializeU32(out + 16, directory.null_length);
  SerializeU32(out + 20, 0);
  SerializeU32(out + 24, 0);
}

bool InBounds(uint32_t offset, uint32_t length) {
  return offset <= kPageBodySize && length <= kPageBodySize - offset;
}

bool ReadDirectory(const char* in, Directory* directory) {
  directory->type = static_cast<ValueType>(
      static_cast<unsigned char>(in[0]));
  if (static_cast<PaxEncoding>(static_cast<unsigned char>(in[1])) !=
      PaxEncoding::kPlain) {
    return false;
  }
  DeserializeU32(in + 4, &directory->data_offset);
  DeserializeU32(in + 8, &directory->data_length);
  DeserializeU32(in + 12, &directory->null_offset);
  DeserializeU32(in + 16, &directory->null_length);
  return InBounds(directory->data_offset, directory->data_length) &&
         InBounds(directory->null_offset, directory->null_length);
}

}  // namespace

void PaxPage::Initialize() {
  bytes_.fill(0);
  WriteHeader(bytes_.data(), 0, 0, kHeaderBytes, 0, kHeaderBytes,
              kHeaderBytes, kHeaderBytes);
}

Status PaxPage::Store(const DataChunk& chunk) {
  if (chunk.ColumnCount() > std::numeric_limits<uint16_t>::max() ||
      chunk.Size() > std::numeric_limits<uint16_t>::max()) {
    return Status::kTooBigData;
  }
  bytes_.fill(0);
  const uint16_t columns = static_cast<uint16_t>(chunk.ColumnCount());
  const uint16_t rows = static_cast<uint16_t>(chunk.Size());
  const uint32_t visibility_offset = kHeaderBytes;
  const uint32_t visibility_length = static_cast<uint32_t>(rows) * 8U;
  const uint32_t directory_offset = visibility_offset + visibility_length;
  const size_t directory_end =
      static_cast<size_t>(directory_offset) +
      static_cast<size_t>(columns) * kDirectoryBytes;
  if (directory_end > bytes_.size()) { return Status::kNoSpace; }

  size_t cursor = directory_end;
  for (size_t column_index = 0; column_index < columns; ++column_index) {
    const ColumnVector& column = chunk.ColumnAt(column_index);
    Directory directory;
    directory.type = column.Type();
    directory.null_offset = static_cast<uint32_t>(cursor);
    directory.null_length = static_cast<uint32_t>(PaxBitmapBytes(rows));
    if (cursor + directory.null_length > bytes_.size()) {
      return Status::kNoSpace;
    }
    std::fill_n(bytes_.data() + cursor, directory.null_length, char{0});
    for (size_t row = 0; row < rows; ++row) {
      if (column.IsNull(row)) {
        bytes_[cursor + row / 8] |= static_cast<char>(1U << (row % 8));
      }
    }
    cursor += directory.null_length;
    directory.data_offset = static_cast<uint32_t>(cursor);

    if (column.Type() == ValueType::kInt64 ||
        column.Type() == ValueType::kDate ||
        column.Type() == ValueType::kDouble) {
      directory.data_length = static_cast<uint32_t>(rows) * 8U;
      if (cursor + directory.data_length > bytes_.size()) {
        return Status::kNoSpace;
      }
      for (size_t row = 0; row < rows; ++row) {
        const Value value = column.ValueAt(row);
        uint64_t bits = 0;
        if (!value.IsNull()) {
          bits = value.type == ValueType::kDouble
                     ? std::bit_cast<uint64_t>(value.value.double_value)
                     : std::bit_cast<uint64_t>(value.value.int_value);
        }
        SerializeU64(bytes_.data() + cursor + row * 8, bits);
      }
      cursor += directory.data_length;
    } else if (column.Type() == ValueType::kVarChar) {
      const size_t offsets_bytes = (static_cast<size_t>(rows) + 1U) * 4U;
      size_t strings_bytes = 0;
      for (size_t row = 0; row < rows; ++row) {
        if (!column.IsNull(row)) {
          strings_bytes += column.StringData()[row].size();
        }
      }
      if (offsets_bytes + strings_bytes >
          std::numeric_limits<uint32_t>::max()) {
        return Status::kTooBigData;
      }
      directory.data_length =
          static_cast<uint32_t>(offsets_bytes + strings_bytes);
      if (cursor + directory.data_length > bytes_.size()) {
        return Status::kNoSpace;
      }
      size_t string_cursor = 0;
      SerializeU32(bytes_.data() + cursor, 0);
      for (size_t row = 0; row < rows; ++row) {
        if (!column.IsNull(row)) {
          const std::string& value = column.StringData()[row];
          std::memcpy(bytes_.data() + cursor + offsets_bytes + string_cursor,
                      value.data(), value.size());
          string_cursor += value.size();
        }
        SerializeU32(bytes_.data() + cursor + (row + 1U) * 4U,
                     static_cast<uint32_t>(string_cursor));
      }
      cursor += directory.data_length;
    } else if (column.Type() != ValueType::kNull) {
      return Status::kUnknownType;
    }

    WriteDirectory(bytes_.data() + directory_offset +
                       column_index * kDirectoryBytes,
                   directory);
  }

  WriteHeader(bytes_.data(), columns, rows, visibility_offset,
              visibility_length, directory_offset,
              static_cast<uint32_t>(directory_end),
              static_cast<uint32_t>(cursor));
  return Status::kSuccess;
}

StatusOr<DataChunk> PaxPage::Load() const {
  uint16_t version = 0;
  uint16_t columns = 0;
  uint16_t rows = 0;
  uint32_t directory_offset = 0;
  uint32_t payload_end = 0;
  DeserializeU16(bytes_.data() + 0, &version);
  DeserializeU16(bytes_.data() + 2, &columns);
  DeserializeU16(bytes_.data() + 4, &rows);
  DeserializeU32(bytes_.data() + 16, &directory_offset);
  DeserializeU32(bytes_.data() + 24, &payload_end);
  if (version != kPaxFormatVersion || payload_end > bytes_.size() ||
      static_cast<size_t>(directory_offset) +
              static_cast<size_t>(columns) * kDirectoryBytes >
          bytes_.size()) {
    return Status::kCorrupt;
  }

  std::vector<Directory> directories(columns);
  std::vector<ValueType> types;
  types.reserve(columns);
  for (size_t column = 0; column < columns; ++column) {
    if (!ReadDirectory(bytes_.data() + directory_offset +
                           column * kDirectoryBytes,
                       &directories[column])) {
      return Status::kCorrupt;
    }
    types.push_back(directories[column].type);
    // The null bitmap must cover every stored row; a truncated bitmap (e.g.
    // a header rewritten past CRC validation) would otherwise make the load
    // loop read past the directory's in-bounds region.
    if (directories[column].null_length < PaxBitmapBytes(rows)) {
      return Status::kCorrupt;
    }
  }

  DataChunk chunk(types, rows);
  for (size_t row = 0; row < rows; ++row) {
    std::vector<Value> values;
    values.reserve(columns);
    for (const Directory& directory : directories) {
      const bool is_null =
          (static_cast<unsigned char>(
               bytes_[directory.null_offset + row / 8]) &
           (1U << (row % 8))) != 0;
      if (is_null || directory.type == ValueType::kNull) {
        values.emplace_back();
        continue;
      }
      if (directory.type == ValueType::kInt64 ||
          directory.type == ValueType::kDate ||
          directory.type == ValueType::kDouble) {
        if (directory.data_length < (row + 1U) * 8U) {
          return Status::kCorrupt;
        }
        uint64_t bits = 0;
        DeserializeU64(bytes_.data() + directory.data_offset + row * 8,
                       &bits);
        if (directory.type == ValueType::kDouble) {
          values.emplace_back(std::bit_cast<double>(bits));
        } else if (directory.type == ValueType::kDate) {
          values.push_back(Value::DateFromDays(std::bit_cast<int64_t>(bits)));
        } else {
          values.emplace_back(std::bit_cast<int64_t>(bits));
        }
        continue;
      }
      if (directory.type == ValueType::kVarChar) {
        const size_t offsets_bytes = (static_cast<size_t>(rows) + 1U) * 4U;
        if (directory.data_length < offsets_bytes) {
          return Status::kCorrupt;
        }
        uint32_t begin = 0;
        uint32_t end = 0;
        DeserializeU32(bytes_.data() + directory.data_offset + row * 4,
                       &begin);
        DeserializeU32(bytes_.data() + directory.data_offset +
                           (row + 1U) * 4,
                       &end);
        const size_t string_bytes = directory.data_length - offsets_bytes;
        if (begin > end || end > string_bytes) {
          return Status::kCorrupt;
        }
        values.emplace_back(std::string(
            bytes_.data() + directory.data_offset + offsets_bytes + begin,
            end - begin));
        continue;
      }
      return Status::kUnknownType;
    }
    chunk.Append(Row(std::move(values)));
  }
  return chunk;
}

}  // namespace tinylamb
