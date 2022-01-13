#include "type/column.hpp"

#include <iostream>

namespace tinylamb {

Column::Column(std::string_view name, ValueType type,
                              uint16_t value_length, enum Restriction rst,
                              uint16_t offset) {
  size_t expected_size = 8 + name.length();
  assert(name.length() < std::numeric_limits<uint16_t>::max());
  std::string payload;
  payload.resize(expected_size);
  uint16_t name_length = name.length();
  uint16_t idx = 0;
  memcpy(payload.data() + idx, &name_length, sizeof(name_length));
  idx += sizeof(name_length);
  memcpy(payload.data() + idx, &type, sizeof(type));
  idx += sizeof(type);
  memcpy(payload.data() + idx, &value_length, sizeof(value_length));
  idx += sizeof(value_length);
  memcpy(payload.data() + idx, &rst, sizeof(rst));
  idx += sizeof(rst);
  memcpy(payload.data() + idx, &offset, sizeof(offset));
  idx += sizeof(offset);
  memcpy(payload.data() + idx, name.data(), name.length());

  owned_data = std::move(payload);
  data = std::string_view(owned_data);
}

Column::Column(const char* ptr)
    : owned_data(), data(ptr, *reinterpret_cast<const uint16_t*>(ptr) + 8) {}

ValueType Column::Type() const { return static_cast<ValueType>(data[2]); }

uint16_t Column::ValueLength() const {
  return *reinterpret_cast<const uint16_t*>(&data[3]);
}

Restriction Column::Restriction() const {
  return static_cast<enum Restriction>(data[5]);
}

uint16_t Column::Offset() const {
  return *reinterpret_cast<const uint16_t*>(&data[6]);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Column>::operator()(
    const tinylamb::Column& c) const {
  uint64_t result = std::hash<std::string_view>()(c.Name());
  result += std::hash<uint16_t>()(static_cast<uint16_t>(c.Type()));
  result += std::hash<uint16_t>()(static_cast<uint16_t>(c.ValueLength()));
  result += std::hash<uint16_t>()(static_cast<uint16_t>(c.Restriction()));
  result += std::hash<uint16_t>()(static_cast<uint32_t>(c.Offset()));
  return result;
}
