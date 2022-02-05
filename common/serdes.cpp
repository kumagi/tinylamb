//
// Created by kumagi on 2022/01/23.
//

#include "common/serdes.hpp"

#include <cstring>

namespace tinylamb {

size_t SerializeStringView(char* pos, std::string_view bin) {
  *reinterpret_cast<bin_size_t*>(pos) = bin.size();
  memcpy(pos + sizeof(bin_size_t), bin.data(), bin.size());
  return sizeof(bin_size_t) + bin.size();
}

size_t SerializeSlot(char* pos, slot_t slot) {
  *reinterpret_cast<slot_t*>(pos) = slot;
  return sizeof(slot_t);
}

size_t SerializePID(char* pos, page_id_t pid) {
  *reinterpret_cast<page_id_t*>(pos) = pid;
  return sizeof(page_id_t);
}

size_t SerializeSize(std::string_view bin) {
  return sizeof(bin_size_t) + bin.size();
}

size_t SerializeInteger(char* pos, int64_t i) {
  *reinterpret_cast<int64_t*>(pos) = i;
  return sizeof(int64_t);
}

size_t SerializeDouble(char* pos, double d) {
  *reinterpret_cast<double*>(pos) = d;
  return sizeof(double);
}

size_t DeserializeStringView(const char* pos, std::string_view* out) {
  *out = {pos + sizeof(bin_size_t), *reinterpret_cast<const bin_size_t*>(pos)};
  return *reinterpret_cast<const bin_size_t*>(pos) + sizeof(bin_size_t);
}

size_t DeserializeSlot(const char* pos, slot_t* out) {
  *out = *reinterpret_cast<const slot_t*>(pos);
  return sizeof(slot_t);
}

size_t DeserializePID(const char* pos, page_id_t* out) {
  *out = *reinterpret_cast<const page_id_t*>(pos);
  return sizeof(page_id_t);
}

size_t DeserializeInteger(const char* pos, int64_t* out) {
  *out = *reinterpret_cast<const int64_t*>(pos);
  return sizeof(uint64_t);
}

size_t DeserializeDouble(const char* pos, double* out) {
  *out = *reinterpret_cast<const double*>(pos);
  return sizeof(double);
}

}  // namespace tinylamb