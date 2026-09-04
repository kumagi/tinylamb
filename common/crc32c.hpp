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

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace tinylamb {

namespace detail {

[[nodiscard]] inline uint32_t Crc32CRawSoftware(uint32_t crc,
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

// Castagnoli CRC-32C (reflected poly 0x82F63B78) via the SSE4.2 crc32
// instructions. The crc32 u64/u32 instructions use the same reflected CRC-32C
// polynomial and bit order, so feeding them 0xffffffff and NOT-xoring the
// result is exactly equivalent to the software loop above. The target
// attribute lets this compile in translation units built without -msse4.2;
// the compiler will not inline a target mismatch, but a static call is still
// far cheaper than the 32k+ cycle bit loop for a 4 KiB page.
[[gnu::target("sse4.2")]] [[nodiscard]] inline uint32_t Crc32CRawHardware(
    uint32_t crc, std::span<const uint8_t> data) {
#if defined(__x86_64__) || defined(__i386__)
  const uint8_t* p = data.data();
  const uint8_t* const end = data.data() + data.size();
  // Process 8-byte chunks first (unaligned is fine; crc32 doesn't require
  // alignment).
  while (reinterpret_cast<uintptr_t>(p) + 8 <=
         reinterpret_cast<uintptr_t>(end)) {
    uint64_t chunk = 0;
    __builtin_memcpy(&chunk, p, sizeof(chunk));
    crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
    p += 8;
  }
  // Tail: 4-byte then 1-byte steps.
  while (reinterpret_cast<uintptr_t>(p) + 4 <=
         reinterpret_cast<uintptr_t>(end)) {
    uint32_t chunk = 0;
    __builtin_memcpy(&chunk, p, sizeof(chunk));
    crc = _mm_crc32_u32(crc, chunk);
    p += 4;
  }
  while (p < end) {
    crc = _mm_crc32_u8(crc, *p);
    ++p;
  }
  return crc;
#else
  return Crc32CRawSoftware(crc, data);
#endif
}

}  // namespace detail

// Castagnoli CRC-32C (reflected poly 0x82F63B78). Uses the hardware crc32
// instruction on x86-64 (SSE4.2) with a software bit-by-bit fallback.
// Known vector: Crc32C("123456789") == 0xe3069283.
[[nodiscard]] inline uint32_t Crc32C(std::span<const uint8_t> data) {
  return detail::Crc32CRawHardware(0xffffffffu, data) ^ 0xffffffffu;
}

[[nodiscard]] inline uint32_t Crc32C(const void* data, size_t length) {
  return Crc32C(
      std::span<const uint8_t>(static_cast<const uint8_t*>(data), length));
}

// Incremental form: continue an unfinished computation. `raw` is the state
// before the final xor; begin with 0xffffffffu and finalize with
// `raw ^ 0xffffffffu`.
[[nodiscard]] inline uint32_t Crc32CExtend(uint32_t raw, const void* data,
                                           size_t length) {
  return detail::Crc32CRawHardware(
      raw, std::span<const uint8_t>(static_cast<const uint8_t*>(data), length));
}

}  // namespace tinylamb

#endif  // TINYLAMB_CRC32C_HPP
