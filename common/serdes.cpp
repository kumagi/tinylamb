/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/serdes.hpp"

#include <cstdint>
#include <cstring>
#include <string_view>

#include "constants.hpp"

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

size_t SerializeNull(char* pos) {
  *pos = '\0';
  return 1;
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
  bin_size_t len = 0;
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
