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

#include "decoder.hpp"

#include <cstdint>
#include <ios>
#include <stdexcept>
#include <string>

#include "common/constants.hpp"
#include "common/serdes.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
Decoder& Decoder::operator>>(std::string& str) {
  bin_size_t size = 0;
  char prefix[sizeof(size)]{};
  is_->read(prefix, sizeof(prefix));
  DeserializeU16(prefix, &size);
  // A failed/corrupt length prefix must not resize with garbage; mirror the
  // vector overload and leave the string empty with failbit set.
  if (!*is_) {
    str.clear();
    return *this;
  }
  str.resize(size);
  if (size > 0) {
    is_->read(str.data(), size);
    // A truncated stream must not silently yield a zero-padded string.
    if (is_->gcount() != static_cast<std::streamsize>(size)) {
      is_->setstate(std::ios::failbit);
    }
  }
  return *this;
}

Decoder& Decoder::operator>>(uint8_t& u8) {
  is_->read(reinterpret_cast<char*>(&u8), sizeof(u8));
  return *this;
}

Decoder& Decoder::operator>>(uint32_t& u32) {
  char bytes[sizeof(u32)]{};
  is_->read(bytes, sizeof(bytes));
  DeserializeU32(bytes, &u32);
  return *this;
}

Decoder& Decoder::operator>>(slot_t& slot) {
  char bytes[sizeof(slot)]{};
  is_->read(bytes, sizeof(bytes));
  DeserializeSlot(bytes, &slot);
  return *this;
}

Decoder& Decoder::operator>>(int64_t& i64) {
  char bytes[sizeof(i64)]{};
  is_->read(bytes, sizeof(bytes));
  DeserializeInteger(bytes, &i64);
  return *this;
}

Decoder& Decoder::operator>>(uint64_t& u64) {
  char bytes[sizeof(u64)]{};
  is_->read(bytes, sizeof(bytes));
  DeserializeU64(bytes, &u64);
  return *this;
}

Decoder& Decoder::operator>>(double& d) {
  char bytes[sizeof(d)]{};
  is_->read(bytes, sizeof(bytes));
  DeserializeDouble(bytes, &d);
  return *this;
}

Decoder& Decoder::operator>>(ValueType& v) {
  // Reject out-of-range discriminants at the decode boundary instead of
  // materializing an invalid enum value that later switches must handle.
  uint8_t raw = 0;
  is_->read(reinterpret_cast<char*>(&raw), sizeof(raw));
  if (raw > static_cast<uint8_t>(ValueType::kArray)) {
    throw std::runtime_error("undefined value type in decoder");
  }
  v = static_cast<ValueType>(raw);
  return *this;
}

Decoder& Decoder::operator>>(bool& v) {
  // Normalize any non-zero byte to `true`: reading the raw byte as the object
  // representation of bool would be UB for values other than 0/1 (e.g. a
  // corrupted stream carrying 0xFF).
  uint8_t raw = 0;
  is_->read(reinterpret_cast<char*>(&raw), sizeof(raw));
  v = raw != 0;
  return *this;
}
}  // namespace tinylamb
