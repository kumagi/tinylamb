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

#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string_view>

#include "common/serdes.hpp"
#include "constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Encoder& Encoder::operator<<(std::string_view sv) {
  // bin_size_t cannot represent longer strings; truncating silently would
  // corrupt the stream (the length prefix would no longer match the payload).
  if (sv.size() > std::numeric_limits<bin_size_t>::max()) {
    throw std::runtime_error("string too long to encode");
  }
  const auto sz = static_cast<bin_size_t>(sv.size());
  char prefix[sizeof(bin_size_t)];
  SerializeU16(prefix, sz);
  os_->write(prefix, sizeof(prefix));
  os_->write(sv.data(), sv.size());
  return *this;
}

Encoder& Encoder::operator<<(uint8_t u8) {
  os_->write(reinterpret_cast<const char*>(&u8), sizeof(u8));
  return *this;
}

Encoder& Encoder::operator<<(uint32_t u32) {
  char bytes[sizeof(u32)];
  SerializeU32(bytes, u32);
  os_->write(bytes, sizeof(bytes));
  return *this;
}

Encoder& Encoder::operator<<(slot_t slot) {
  char bytes[sizeof(slot)];
  SerializeSlot(bytes, slot);
  os_->write(bytes, sizeof(bytes));
  return *this;
}

Encoder& Encoder::operator<<(int64_t i64) {
  char bytes[sizeof(i64)];
  SerializeInteger(bytes, i64);
  os_->write(bytes, sizeof(bytes));
  return *this;
}

Encoder& Encoder::operator<<(uint64_t u64) {
  char bytes[sizeof(u64)];
  SerializeU64(bytes, u64);
  os_->write(bytes, sizeof(bytes));
  return *this;
}

Encoder& Encoder::operator<<(double d) {
  char bytes[sizeof(d)];
  SerializeDouble(bytes, d);
  os_->write(bytes, sizeof(bytes));
  return *this;
}

Encoder& Encoder::operator<<(ValueType v) {
  os_->write(reinterpret_cast<const char*>(&v), sizeof(v));
  return *this;
}

Encoder& Encoder::operator<<(bool v) {
  // Normalize to 0/1 instead of writing the implementation-defined bool
  // representation; keeps the on-disk format stable.
  const uint8_t normalized = v ? 1 : 0;
  os_->write(reinterpret_cast<const char*>(&normalized), sizeof(normalized));
  return *this;
}

}  // namespace tinylamb
