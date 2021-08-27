#include "type/row.hpp"

#include <cstring>
#include <iostream>

#include "page/row_position.hpp"
#include "schema.hpp"
#include "value.hpp"

namespace tinylamb {

Row::Row(void* d, size_t l, RowPosition p)
    : data(reinterpret_cast<char*>(d), l), pos(p) {}

Row::Row(std::string_view redo, RowPosition p) : data(redo), pos(p) {}

Row& Row::operator=(const Row& orig) {
  owned_data = orig.owned_data;
  data = orig.owned_data.empty() ? orig.data : std::string_view(owned_data);
  pos = orig.pos;
  return *this;
}

bool Row::Write(std::string_view from) {
  if (data.size() < from.size()) {
    return false;
  }
  memcpy(const_cast<char*>(data.data()), from.data(), from.size());
  return true;
}

bool Row::Read(char* ptr, size_t length, const RowPosition& row_pos) {
  data = std::string_view(ptr, length);
  owned_data = "";
  pos = row_pos;
  return true;
}

void Row::SetValue(const Schema& sc, size_t idx, const Value& v) {
  const Column& c = sc.GetColumn(idx);
  if (data.size() < c.Offset() + c.ValueLength()) {
    MakeOwned();
    owned_data.resize(c.Offset() + c.ValueLength());
    data = std::string_view(owned_data);
  }
  char* ptr = const_cast<char*>(data.data() + c.Offset());
  switch (c.Type()) {
    case ValueType::kUnknown:
      assert(!"Cannot set unknown type");
    case ValueType::kInt64: {
      uint64_t& value = *reinterpret_cast<uint64_t*>(ptr);
      value = v.value.int_value;
      break;
    }
    case ValueType::kVarChar:
      memcpy(ptr, v.value.varchar_value, v.length);
      break;
  }
}

bool Row::GetValue(Schema& sc, uint16_t idx, Value& dst) {
  const Column& c = sc.GetColumn(idx);
  if (data.size() < c.Offset() + c.ValueLength()) {
    return false;
  }
  char* ptr = const_cast<char*>(data.data() + c.Offset());
  switch (c.Type()) {
    case ValueType::kUnknown:
      assert(!"Cannot read unknown type");
    case ValueType::kInt64:
      dst.value.int_value = *reinterpret_cast<int64_t*>(ptr);
      break;
    case ValueType::kVarChar:
      dst.value.varchar_value = reinterpret_cast<char*>(ptr);
      break;
  }
  dst.length = c.ValueLength();
  return true;
}

void Row::MakeOwned() {
  if (owned_data.empty()) {
    owned_data = std::string(data);
    data = owned_data;
  }
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Row>::operator()(const tinylamb::Row& row) {
  return std::hash<std::string_view>()(row.data);
}