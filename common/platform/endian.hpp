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

#ifndef TINYLAMB_COMMON_PLATFORM_ENDIAN_HPP
#define TINYLAMB_COMMON_PLATFORM_ENDIAN_HPP

#if defined(__APPLE__)

#include <libkern/OSByteOrder.h>
#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)
#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)
#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#elif defined(__linux__) || defined(__GLIBC__) || defined(__gnu_linux__)

#include <endian.h>

#elif !defined(htobe16)

#include <cstdint>

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define TINYLAMB_HOST_IS_BIG_ENDIAN 1
#else
#define TINYLAMB_HOST_IS_BIG_ENDIAN 0
#endif

inline uint16_t tinylamb_bswap16(uint16_t x) {
  return static_cast<uint16_t>((x << 8) | (x >> 8));
}
inline uint32_t tinylamb_bswap32(uint32_t x) {
  return ((x << 24) & 0xff000000u) | ((x << 8) & 0x00ff0000u) |
         ((x >> 8) & 0x0000ff00u) | ((x >> 24) & 0x000000ffu);
}
inline uint64_t tinylamb_bswap64(uint64_t x) {
  return ((x << 56) & 0xff00000000000000ULL) |
         ((x << 40) & 0x00ff000000000000ULL) |
         ((x << 24) & 0x0000ff0000000000ULL) |
         ((x << 8) & 0x000000ff00000000ULL) |
         ((x >> 8) & 0x00000000ff000000ULL) |
         ((x >> 24) & 0x0000000000ff0000ULL) |
         ((x >> 40) & 0x000000000000ff00ULL) |
         ((x >> 56) & 0x00000000000000ffULL);
}

#if TINYLAMB_HOST_IS_BIG_ENDIAN
inline uint16_t htobe16(uint16_t x) { return x; }
inline uint16_t htole16(uint16_t x) { return tinylamb_bswap16(x); }
inline uint32_t htobe32(uint32_t x) { return x; }
inline uint32_t htole32(uint32_t x) { return tinylamb_bswap32(x); }
inline uint64_t htobe64(uint64_t x) { return x; }
inline uint64_t htole64(uint64_t x) { return tinylamb_bswap64(x); }
#else
inline uint16_t htobe16(uint16_t x) { return tinylamb_bswap16(x); }
inline uint16_t htole16(uint16_t x) { return x; }
inline uint32_t htobe32(uint32_t x) { return tinylamb_bswap32(x); }
inline uint32_t htole32(uint32_t x) { return x; }
inline uint64_t htobe64(uint64_t x) { return tinylamb_bswap64(x); }
inline uint64_t htole64(uint64_t x) { return x; }
#endif
inline uint16_t be16toh(uint16_t x) { return htobe16(x); }
inline uint16_t le16toh(uint16_t x) { return htole16(x); }
inline uint32_t be32toh(uint32_t x) { return htobe32(x); }
inline uint32_t le32toh(uint32_t x) { return htole32(x); }
inline uint64_t be64toh(uint64_t x) { return htobe64(x); }
inline uint64_t le64toh(uint64_t x) { return htole64(x); }

#endif

#endif  // TINYLAMB_COMMON_PLATFORM_ENDIAN_HPP
