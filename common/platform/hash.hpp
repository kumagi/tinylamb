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

#ifndef TINYLAMB_COMMON_PLATFORM_HASH_HPP
#define TINYLAMB_COMMON_PLATFORM_HASH_HPP

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace tinylamb::platform {

// Byte-string hash with the same value on every platform. std::hash is
// implementation-defined (libstdc++ uses MurmurHash2-64A, libc++ CityHash),
// so anything whose *observable* behaviour depends on a hash -- spill
// partition assignment, and through it the order rows come back in -- must
// use this instead. It reproduces libstdc++'s std::hash<std::string>
// (std::_Hash_bytes with seed 0xc70f6907) on 64-bit little-endian targets,
// keeping Linux results unchanged.
inline uint64_t StableHashBytes(std::string_view bytes) {
  constexpr uint64_t kMul = (uint64_t{0xc6a4a793} << 32) + 0x5bd1e995;
  constexpr uint64_t kSeed = 0xc70f6907;
  const auto shift_mix = [](uint64_t v) { return v ^ (v >> 47); };
  const char* const buf = bytes.data();
  const size_t len = bytes.size();
  const size_t len_aligned = len & ~size_t{7};
  uint64_t hash = kSeed ^ (len * kMul);
  for (size_t i = 0; i < len_aligned; i += 8) {
    uint64_t word = 0;
    std::memcpy(&word, buf + i, sizeof(word));
    hash ^= shift_mix(word * kMul) * kMul;
    hash *= kMul;
  }
  if ((len & 7) != 0) {
    uint64_t tail = 0;
    for (size_t n = len & 7; n-- > 0;) {
      tail = (tail << 8) + static_cast<unsigned char>(buf[len_aligned + n]);
    }
    hash ^= tail;
    hash *= kMul;
  }
  hash = shift_mix(hash) * kMul;
  return shift_mix(hash);
}

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_HASH_HPP
