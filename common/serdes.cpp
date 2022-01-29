//
// Created by kumagi on 2022/01/23.
//

#include "common/serdes.hpp"

#include <cstring>

namespace tinylamb {

size_t SerializeStringView(char* pos, std::string_view bin) {
  *reinterpret_cast<uint16_t*>(pos) = bin.size();
  memcpy(pos + sizeof(uint16_t), bin.data(), bin.size());
  return sizeof(uint16_t) + bin.size();
}

size_t SerializeSlot(char* pos, uint16_t slot) {
  *reinterpret_cast<uint16_t*>(pos) = slot;
  return sizeof(uint16_t);
}

size_t SerializePID(char* pos, page_id_t pid) {
  *reinterpret_cast<page_id_t*>(pos) = pid;
  return sizeof(page_id_t);
}

size_t SerializeSize(std::string_view bin) {
  return sizeof(uint16_t) + bin.size();
}

size_t DeserializeStringView(const char* pos, std::string_view* out) {
  *out = {pos + sizeof(uint16_t), *reinterpret_cast<const uint16_t*>(pos)};
  return *reinterpret_cast<const uint16_t*>(pos) + sizeof(uint16_t);
}

size_t DeserializeSlot(const char* pos, uint16_t* out) {
  *out = *reinterpret_cast<const uint16_t*>(pos);
  return sizeof(uint16_t);
}

size_t DeserializePID(const char* pos, page_id_t* out) {
  *out = *reinterpret_cast<const page_id_t*>(pos);
  return sizeof(page_id_t);
}

}  // namespace tinylamb