//
// Created by kumagi on 2022/01/23.
//

#include "common/serdes.hpp"

#include <cstring>

namespace tinylamb {

size_t SerializeStringView(char* pos, std::string_view bin) {
  bin_size_t len = bin.size();
  memcpy(pos, &len, sizeof(len));
  memcpy(pos + sizeof(len), bin.data(), bin.size());
  return sizeof(len) + bin.size();
}

size_t SerializeSlot(char* pos, slot_t slot) {
  memcpy(pos, &slot, sizeof(slot));
  return sizeof(slot_t);
}

size_t SerializePID(char* pos, page_id_t pid) {
  memcpy(pos, &pid, sizeof(pid));
  return sizeof(page_id_t);
}

size_t SerializeSize(std::string_view bin) {
  return sizeof(bin_size_t) + bin.size();
}

size_t SerializeInteger(char* pos, int64_t i) {
  memcpy(pos, &i, sizeof(i));
  return sizeof(int64_t);
}

size_t SerializeDouble(char* pos, double d) {
  memcpy(pos, &d, sizeof(d));
  return sizeof(double);
}

size_t DeserializeStringView(const char* pos, std::string_view* out) {
  bin_size_t len;
  memcpy(&len, pos, sizeof(bin_size_t));
  *out = {pos + sizeof(len), len};
  return sizeof(len) + len;
}

size_t DeserializeSlot(const char* pos, slot_t* out) {
  memcpy(out, pos, sizeof(*out));
  return sizeof(slot_t);
}

size_t DeserializePID(const char* pos, page_id_t* out) {
  memcpy(out, pos, sizeof(page_id_t));
  return sizeof(page_id_t);
}

size_t DeserializeInteger(const char* pos, int64_t* out) {
  memcpy(out, pos, sizeof(int64_t));
  return sizeof(uint64_t);
}

size_t DeserializeDouble(const char* pos, double* out) {
  memcpy(out, pos, sizeof(double));
  return sizeof(double);
}

}  // namespace tinylamb