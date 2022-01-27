#include "type/schema.hpp"

#include <cstring>
#include <vector>

#include "common/log_message.hpp"
#include "page/row_position.hpp"
#include "value_type.hpp"

namespace tinylamb {

Schema::Schema(std::string_view schema_name, const std::vector<Column>& columns,
               uint64_t page_id) {
  assert(schema_name.size() < 256);
  size_t payload_size = 1 + schema_name.size() + 1 + sizeof(page_id);
  for (const auto& column : columns) {
    payload_size += column.data.size();
  }
  owned_data = std::string(payload_size, '\0');
  data = std::string_view(owned_data);

  offsets.reserve(columns.size());
  size_t offset = 0;
  for (const auto& column : columns) {
    offsets.push_back(offset);
    offset += column.data.size();
  }

  size_t idx = 0;
  *reinterpret_cast<uint64_t*>(&owned_data[idx]) = page_id;  // row_page
  idx += sizeof(uint64_t);
  owned_data[idx++] = schema_name.size() & 127;  // name_length
  memcpy(&owned_data[idx], schema_name.data(), schema_name.size());  // name
  idx += schema_name.size();
  owned_data[idx++] = columns.size();
  for (const auto& column : columns) {
    memcpy(&owned_data[idx], column.data.data(), column.data.size());
    idx += column.data.size();
  }
}

Schema::Schema(char* ptr) {
  data = std::string_view(ptr, INT16_MAX);  // temporary set as max.
  offsets.reserve(ColumnCount());
  size_t offset = 0;
  for (size_t i = 0; i < ColumnCount(); ++i) {
    offsets.push_back(offset);
    offset += GetColumn(i).FootprintSize();
  }
  // sizeof(RowPage) + sizeof(NameLength) + name_length + sizeof(ColumnCount) +
  // offsets.
  data = std::string_view(ptr, 8 + 1 + NameLength() + 1 + offset);
}

Schema::Schema(const char* ptr) {
  data = std::string_view(ptr, INT16_MAX);  // temporary set as max.
  offsets.reserve(ColumnCount());
  size_t offset = 0;
  for (size_t i = 0; i < ColumnCount(); ++i) {
    offsets.push_back(offset);
    offset += GetColumn(i).FootprintSize();
  }
  // sizeof(RowPage) + sizeof(NameLength) + name_length + sizeof(ColumnCount) +
  // offsets.
  data = std::string_view(ptr, 8 + 1 + NameLength() + 1 + offset);
}

[[nodiscard]] size_t Schema::FixedRowSize() const {
  size_t ans = 0;
  for (size_t i = 0; i < ColumnCount(); ++i) {
    ans += GetColumn(i).ValueLength();
  }
  return ans;
}

[[nodiscard]] Column Schema::GetColumn(size_t idx) const {
  assert(idx < offsets.size());
  return Column(&data[10 + NameLength() + offsets[idx]]);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Schema>::operator()(
    const tinylamb::Schema& sc) const {
  uint64_t result = std::hash<std::string_view>()(sc.Name());
  for (size_t i = 0; i < sc.ColumnCount(); ++i) {
    result += std::hash<tinylamb::Column>()(sc.GetColumn(i));
  }
  return result;
}
