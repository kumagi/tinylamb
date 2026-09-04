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

#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>

#include "constants.hpp"

namespace tinylamb {

size_t SerializeU16(char* pos, uint16_t value) {
  pos[0] = static_cast<char>((value >> 8U) & 0xffU);
  pos[1] = static_cast<char>(value & 0xffU);
  return sizeof(value);
}

size_t SerializeU32(char* pos, uint32_t value) {
  for (size_t i = 0; i < sizeof(value); ++i) {
    pos[i] = static_cast<char>((value >> ((3U - i) * 8U)) & 0xffU);
  }
  return sizeof(value);
}

size_t SerializeU64(char* pos, uint64_t value) {
  for (size_t i = 0; i < sizeof(value); ++i) {
    pos[i] = static_cast<char>((value >> ((7U - i) * 8U)) & 0xffU);
  }
  return sizeof(value);
}

size_t DeserializeU16(const char* pos, uint16_t* out) {
  *out = (static_cast<uint16_t>(static_cast<unsigned char>(pos[0])) << 8U) |
         static_cast<uint16_t>(static_cast<unsigned char>(pos[1]));
  return sizeof(*out);
}

size_t DeserializeU32(const char* pos, uint32_t* out) {
  uint32_t value = 0;
  for (size_t i = 0; i < sizeof(value); ++i) {
    value = (value << 8U) |
            static_cast<uint32_t>(static_cast<unsigned char>(pos[i]));
  }
  *out = value;
  return sizeof(*out);
}

size_t DeserializeU64(const char* pos, uint64_t* out) {
  uint64_t value = 0;
  for (size_t i = 0; i < sizeof(value); ++i) {
    value = (value << 8U) |
            static_cast<uint64_t>(static_cast<unsigned char>(pos[i]));
  }
  *out = value;
  return sizeof(*out);
}

size_t SerializeStringView(char* pos, std::string_view bin) {
  // bin_size_t cannot represent longer strings; truncating silently would
  // corrupt the serialized image (length prefix vs payload mismatch).
  if (bin.size() > std::numeric_limits<bin_size_t>::max()) {
    throw std::runtime_error("string too long to serialize");
  }
  const auto len = static_cast<bin_size_t>(bin.size());
  SerializeU16(pos, len);
  memcpy(pos + sizeof(len), bin.data(), bin.size());
  return sizeof(len) + bin.size();
}

size_t SerializeSlot(char* pos, slot_t slot) {
  SerializeU16(pos, slot);
  return sizeof(slot_t);
}

size_t SerializePID(char* pos, page_id_t pid) {
  SerializeU64(pos, pid);
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
  SerializeU64(pos, std::bit_cast<uint64_t>(i));
  return sizeof(int64_t);
}

size_t SerializeDouble(char* pos, double d) {
  SerializeU64(pos, std::bit_cast<uint64_t>(d));
  return sizeof(double);
}

size_t DeserializeStringView(const char* pos, std::string_view* out) {
  bin_size_t len = 0;
  DeserializeU16(pos, &len);
  *out = {pos + sizeof(len), len};
  return sizeof(len) + len;
}

size_t DeserializeStringView(const char* pos, const char* end,
                             std::string_view* out) {
  if (end == nullptr || static_cast<size_t>(end - pos) < sizeof(bin_size_t)) {
    return 0;
  }
  bin_size_t len = 0;
  DeserializeU16(pos, &len);
  if (static_cast<size_t>(end - pos) - sizeof(bin_size_t) < len) {
    return 0;
  }
  *out = {pos + sizeof(len), len};
  return sizeof(len) + len;
}

size_t DeserializeSlot(const char* pos, slot_t* out) {
  DeserializeU16(pos, out);
  return sizeof(slot_t);
}

size_t DeserializePID(const char* pos, page_id_t* out) {
  DeserializeU64(pos, out);
  return sizeof(page_id_t);
}

size_t DeserializeInteger(const char* pos, int64_t* out) {
  uint64_t bits = 0;
  DeserializeU64(pos, &bits);
  *out = std::bit_cast<int64_t>(bits);
  return sizeof(int64_t);
}

size_t DeserializeDouble(const char* pos, double* out) {
  uint64_t bits = 0;
  DeserializeU64(pos, &bits);
  *out = std::bit_cast<double>(bits);
  return sizeof(double);
}
}  // namespace tinylamb
