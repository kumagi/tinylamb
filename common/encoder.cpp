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

#include "encoder.hpp"

#include <array>
#include <cstdint>
#include <functional>
#include <limits>
#include <string_view>

#include "common/serdes.hpp"
#include "constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Encoder& Encoder::operator<<(std::string_view sv) {
  if (format_error_ != nullptr) {
    return *this;
  }
  if (sv.size() > std::numeric_limits<bin_size_t>::max()) {
    format_error_ = "string too long to encode";
    return *this;
  }
  // The length prefix (and the payload append) can reallocate the destination
  // before we read sv. A view into *any* part of the destination allocation is
  // aliased, including the reserved-but-unused tail past size(): append() may
  // reuse that capacity in place or replace it. Snapshot aliased input first;
  // std::less gives a total pointer order even when the source belongs to an
  // unrelated allocation.
  std::string snapshot;
  if (bytes_ != nullptr && !sv.empty()) {
    const char* begin = bytes_->data();
    const char* end = begin + bytes_->capacity();
    const std::less<> less;
    if (!less(sv.data(), begin) && less(sv.data(), end)) {
      snapshot.assign(sv);
      sv = snapshot;
    }
  }
  const auto sz = static_cast<bin_size_t>(sv.size());
  std::array<char, sizeof(bin_size_t)> prefix{};
  SerializeU16(prefix.data(), sz);
  Write(prefix.data(), prefix.size());
  if (!sv.empty()) {
    Write(sv.data(), sv.size());
  }
  return *this;
}

Encoder& Encoder::operator<<(uint8_t u8) {
  Write(reinterpret_cast<const char*>(&u8), sizeof(u8));
  return *this;
}

Encoder& Encoder::operator<<(uint32_t u32) {
  std::array<char, sizeof(u32)> bytes{};
  SerializeU32(bytes.data(), u32);
  Write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(slot_t slot) {
  std::array<char, sizeof(slot)> bytes{};
  SerializeSlot(bytes.data(), slot);
  Write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(int64_t i64) {
  std::array<char, sizeof(i64)> bytes{};
  SerializeInteger(bytes.data(), i64);
  Write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(uint64_t u64) {
  std::array<char, sizeof(u64)> bytes{};
  SerializeU64(bytes.data(), u64);
  Write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(double d) {
  std::array<char, sizeof(d)> bytes{};
  SerializeDouble(bytes.data(), d);
  Write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(ValueType v) {
  Write(reinterpret_cast<const char*>(&v), sizeof(v));
  return *this;
}

Encoder& Encoder::operator<<(bool v) {
  // Normalize to 0/1 instead of writing the implementation-defined bool
  // representation; keeps the on-disk format stable.
  const uint8_t normalized = v ? 1 : 0;
  Write(reinterpret_cast<const char*>(&normalized), sizeof(normalized));
  return *this;
}

}  // namespace tinylamb
