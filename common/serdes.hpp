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

#ifndef TINYLAMB_SERDES_HPP
#define TINYLAMB_SERDES_HPP

#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Version-1 on-disk layout. All fixed-width scalars are big-endian and every
// top-level page/WAL object carries this magic/version pair. Version 0 is
// intentionally unsupported: the v1 migration is a destructive format bump.
inline constexpr uint32_t kSerdesMagic = 0x54594231U;  // "TYB1"
inline constexpr uint32_t kSerdesVersion = 1U;

// Row decode runs these per column of every scanned row, so they live inline
// here: one bswapped load beats a cross-TU call into a byte-at-a-time loop.
#if defined(__GNUC__) || defined(__clang__)
#define TINYLAMB_BSWAP16 __builtin_bswap16
#define TINYLAMB_BSWAP32 __builtin_bswap32
#define TINYLAMB_BSWAP64 __builtin_bswap64
#else
inline uint16_t TINYLAMB_BSWAP16(uint16_t v) { return v << 8U | v >> 8U; }
inline uint32_t TINYLAMB_BSWAP32(uint32_t v) {
  return (v << 24U) | (v << 8U & 0xFF0000U) | (v >> 8U & 0xFF00U) | v >> 24U;
}
inline uint64_t TINYLAMB_BSWAP64(uint64_t v) {
  return (static_cast<uint64_t>(TINYLAMB_BSWAP32(static_cast<uint32_t>(v)))
          << 32U) |
         TINYLAMB_BSWAP32(static_cast<uint32_t>(v >> 32U));
}
#endif

inline size_t SerializeU16(char* pos, uint16_t value) {
  const uint16_t flipped = TINYLAMB_BSWAP16(value);
  std::memcpy(pos, &flipped, sizeof(flipped));
  return sizeof(flipped);
}

inline size_t SerializeU32(char* pos, uint32_t value) {
  const uint32_t flipped = TINYLAMB_BSWAP32(value);
  std::memcpy(pos, &flipped, sizeof(flipped));
  return sizeof(flipped);
}

inline size_t SerializeU64(char* pos, uint64_t value) {
  const uint64_t flipped = TINYLAMB_BSWAP64(value);
  std::memcpy(pos, &flipped, sizeof(flipped));
  return sizeof(flipped);
}

inline size_t DeserializeU16(const char* pos, uint16_t* out) {
  uint16_t flipped = 0;
  std::memcpy(&flipped, pos, sizeof(flipped));
  *out = TINYLAMB_BSWAP16(flipped);
  return sizeof(flipped);
}

inline size_t DeserializeU32(const char* pos, uint32_t* out) {
  uint32_t flipped = 0;
  std::memcpy(&flipped, pos, sizeof(flipped));
  *out = TINYLAMB_BSWAP32(flipped);
  return sizeof(flipped);
}

inline size_t DeserializeU64(const char* pos, uint64_t* out) {
  uint64_t flipped = 0;
  std::memcpy(&flipped, pos, sizeof(flipped));
  *out = TINYLAMB_BSWAP64(flipped);
  return sizeof(flipped);
}

// Contract: the caller must guarantee that `pos` holds at least
// SerializeSize(bin) writable/readable bytes. DeserializeStringView trusts the
// stored length prefix and cannot bounds-check without an end pointer; page
// images are validated (checksummed) before deserialization.
size_t SerializeStringView(char* pos, std::string_view bin);
inline size_t SerializeSlot(char* pos, slot_t slot) {
  return SerializeU16(pos, slot);
}
inline size_t SerializePID(char* pos, page_id_t pid) {
  return SerializeU64(pos, pid);
}
inline size_t SerializeSize(std::string_view bin) {
  return sizeof(bin_size_t) + bin.size();
}
// A NULL is a single zero byte on the wire, not a zeroed U32.
inline size_t SerializeNull(char* pos) {
  *pos = '\0';
  return 1;
}
inline size_t SerializeInteger(char* pos, int64_t i) {
  return SerializeU64(pos, std::bit_cast<uint64_t>(i));
}
inline size_t SerializeDouble(char* pos, double d) {
  return SerializeU64(pos, std::bit_cast<uint64_t>(d));
}

inline size_t DeserializeStringView(const char* pos, std::string_view* out) {
  bin_size_t len = 0;
  DeserializeU16(pos, &len);
  *out = {pos + sizeof(len), len};
  return sizeof(len) + len;
}
// Bounds-checked variant for untrusted input: returns 0 (and leaves *out
// untouched) when the stored length would run past `end`.
size_t DeserializeStringView(const char* pos, const char* end,
                             std::string_view* out);
size_t DeserializeString(std::istream& in, std::string* out);
inline size_t DeserializeSlot(const char* pos, slot_t* out) {
  return DeserializeU16(pos, out);
}
inline size_t DeserializePID(const char* pos, page_id_t* out) {
  return DeserializeU64(pos, out);
}
inline size_t DeserializeInteger(const char* pos, int64_t* out) {
  uint64_t bits = 0;
  const size_t consumed = DeserializeU64(pos, &bits);
  *out = std::bit_cast<int64_t>(bits);
  return consumed;
}
inline size_t DeserializeDouble(const char* pos, double* out) {
  uint64_t bits = 0;
  const size_t consumed = DeserializeU64(pos, &bits);
  *out = std::bit_cast<double>(bits);
  return consumed;
}

// Serialized width of the fixed-width value images (0 = variable-width).
// Row decode walks one image per column of every scanned row; answering the
// common INT64/DATE/DOUBLE case from this table keeps the walk call-free.
constexpr size_t FixedSerializedSize(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
    case ValueType::kDate:
    case ValueType::kDouble:
      return sizeof(int64_t);
    default:
      return 0;
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_SERDES_HPP
