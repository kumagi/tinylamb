/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_CRC32C_HPP
#define TINYLAMB_CRC32C_HPP

#include <cstddef>
#include <cstdint>
#include <span>

namespace tinylamb {

namespace detail {
// Raw CRC state (no initial/final xor applied here).
[[nodiscard]] inline uint32_t Crc32CRaw(uint32_t crc,
                                        std::span<const uint8_t> data) {
  for (uint8_t byte : data) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      const uint32_t mask = -(crc & 1u);
      crc = (crc >> 1) ^ (0x82f63b78u & mask);
    }
  }
  return crc;
}
}  // namespace detail

// Castagnoli CRC-32C (reflected poly 0x82F63B78). Software bit-by-bit.
// Known vector: Crc32C("123456789") == 0xe3069283.
[[nodiscard]] inline uint32_t Crc32C(std::span<const uint8_t> data) {
  return detail::Crc32CRaw(0xffffffffu, data) ^ 0xffffffffu;
}

[[nodiscard]] inline uint32_t Crc32C(const void* data, size_t length) {
  return Crc32C(std::span<const uint8_t>(
      static_cast<const uint8_t*>(data), length));
}

// Incremental form: continue an unfinished computation. `raw` is the state
// before the final xor; begin with 0xffffffffu and finalize with
// `raw ^ 0xffffffffu`.
[[nodiscard]] inline uint32_t Crc32CExtend(uint32_t raw, const void* data,
                                           size_t length) {
  return detail::Crc32CRaw(
      raw, std::span<const uint8_t>(static_cast<const uint8_t*>(data),
                                    length));
}

}  // namespace tinylamb

#endif  // TINYLAMB_CRC32C_HPP
