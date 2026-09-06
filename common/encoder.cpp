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
#include <cassert>
#include <cstdint>
#include <ios>
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
  std::array<char, sizeof(bin_size_t)> prefix{};
  SerializeU16(prefix.data(), sz);
  os_->write(prefix.data(), prefix.size());
  os_->write(sv.data(), static_cast<std::streamsize>(sv.size()));
  return *this;
}

Encoder& Encoder::operator<<(uint8_t u8) {
  os_->write(reinterpret_cast<const char*>(&u8), sizeof(u8));
  return *this;
}

Encoder& Encoder::operator<<(uint32_t u32) {
  std::array<char, sizeof(u32)> bytes{};
  SerializeU32(bytes.data(), u32);
  os_->write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(slot_t slot) {
  std::array<char, sizeof(slot)> bytes{};
  SerializeSlot(bytes.data(), slot);
  os_->write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(int64_t i64) {
  std::array<char, sizeof(i64)> bytes{};
  SerializeInteger(bytes.data(), i64);
  os_->write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(uint64_t u64) {
  std::array<char, sizeof(u64)> bytes{};
  SerializeU64(bytes.data(), u64);
  os_->write(bytes.data(), bytes.size());
  return *this;
}

Encoder& Encoder::operator<<(double d) {
  std::array<char, sizeof(d)> bytes{};
  SerializeDouble(bytes.data(), d);
  os_->write(bytes.data(), bytes.size());
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
