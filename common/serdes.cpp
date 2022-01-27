//
// Created by kumagi on 2022/01/23.
//

#include "common/serdes.hpp"

#include <cstring>

namespace tinylamb {

size_t Serialize(char* pos, std::string_view bin) {
  const uint16_t size = bin.size();
  memcpy(pos, &size, sizeof(size));
  memcpy(pos + sizeof(size), bin.data(), bin.size());
  return sizeof(size) + bin.size();
}

size_t SerializePID(char* pos, page_id_t pid) {
  constexpr uint16_t size = sizeof(page_id_t);
  *reinterpret_cast<page_id_t*>(pos) = pid;
  return sizeof(page_id_t);
}

std::string_view Deserialize(const char* pos) {
  return {pos + sizeof(uint16_t), *reinterpret_cast<const uint16_t*>(pos)};
}

}  // namespace tinylamb