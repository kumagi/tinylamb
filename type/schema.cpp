#include "type/schema.hpp"

#include <cstring>
#include <vector>

#include "macro.hpp"
#include "page/row_position.hpp"
#include "value_type.hpp"

namespace tinylamb {

size_t Column::PhysicalSize() const { return sizeof(Column) + name_length; }

std::string_view Column::Name() const {
  return std::string_view(name, name_length);
}

uint64_t Column::CalcHash() const {
  uint64_t result = std::hash<std::string_view>()(Name());
  result += std::hash<uint16_t>()(static_cast<uint16_t>(type));
  result += std::hash<uint16_t>()(static_cast<uint16_t>(value_length));
  result += std::hash<uint16_t>()(static_cast<uint16_t>(restriction));
  result += std::hash<uint16_t>()(static_cast<uint32_t>(offset));
  return result;
}

Schema::Schema(std::string_view name)
    : owned_data(sizeof(table_root) + sizeof(name_length) + name.length() + 1,
                 '\0'),
      data(owned_data.data()),
      length(sizeof(table_root) + sizeof(name_length) + name.size() +
             1 /*=column_count */),
      table_root(0),
      name_length(name.length()) {
  assert(owned_data.size() < 256);
  owned_data[sizeof(table_root)] = static_cast<uint8_t>(name.length());
  memcpy(owned_data.data() + sizeof(table_root) + sizeof(name_length),
         name.data(), name.length());
  SetColumnCount(0);
  SetTableRoot(0);
}

Schema Schema::Read(char* ptr) {
  Schema ret;
  ret.data = ptr;
  ret.table_root = *reinterpret_cast<uint64_t*>(ret.data);
  ret.name_length = *reinterpret_cast<int8_t*>(ret.data + sizeof(table_root));
  ret.columns.resize(ret.ColumnCount());
  size_t offset = 0;
  for (size_t i = 0; i < ret.ColumnCount(); ++i) {
    ret.columns[i] =
        sizeof(table_root) + sizeof(name_length) + ret.name_length + 1 + offset;
    offset += sizeof(Column) + ret.GetColumn(i).name_length;
  }
  ret.length = sizeof(table_root) + 1 + ret.name_length + 1 + offset;
  return ret;
}

uint8_t Schema::ColumnCount() const { return data[9 + name_length]; }

void Schema::AddColumn(std::string_view name, ValueType type,
                       size_t value_length, uint16_t restriction) {
  MakeOwnData();
  const size_t column_size_bytes = sizeof(Column) + name.length();
  const size_t offset = owned_data.size();
  owned_data.resize(owned_data.size() + column_size_bytes);
  data = owned_data.data();
  length += column_size_bytes;
  Column& new_column = *reinterpret_cast<Column*>(owned_data.data() + offset);
  new_column.name_length = name.length();
  new_column.type = type;
  new_column.value_length = value_length;
  new_column.restriction = restriction;
  if (ColumnCount() == 0) {
    new_column.offset = 0;
  } else {
    const Column& last_column = GetColumn(ColumnCount() - 1);
    new_column.offset = last_column.offset + last_column.value_length;
  }
  columns.push_back(offset);
  memcpy(new_column.name, name.data(), name.length());
  SetColumnCount(ColumnCount() + 1);
}

void Schema::SetTableRoot(uint64_t root) {
  uint64_t& table_root_data = *reinterpret_cast<uint64_t*>(data);
  table_root_data = table_root = root;
}

[[nodiscard]] size_t Schema::FixedRowSize() const {
  size_t ans = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    ans += GetColumn(i).value_length;
  }
  return ans;
}

[[nodiscard]] std::string_view Schema::Name() const {
  return std::string_view(reinterpret_cast<char*>(data) + 9, name_length);
}

[[nodiscard]] const Column& Schema::GetColumn(size_t idx) const {
  assert(idx < columns.size());
  return *reinterpret_cast<const Column*>(data + columns[idx]);
}

uint64_t Schema::CalcChecksum() const {
  uint64_t result = std::hash<std::string_view>()(Name());
  result += std::hash<RowPosition>()(pos);
  for (size_t i = 0; i < columns.size(); ++i) {
    result += GetColumn(i).CalcHash();
  }
  return result;
}

void Schema::MakeOwnData() {
  if (!owned_data.empty()) {
    return;
  } else {
    owned_data.resize(length);
    memcpy(owned_data.data(), data, length);
  }
}
void Schema::SetColumnCount(uint8_t count) {
  data[sizeof(table_root) + sizeof(name_length) + name_length] = count;
}

}  // namespace tinylamb
