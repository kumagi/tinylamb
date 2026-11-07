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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <span>
#include <vector>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
#endif

namespace tinylamb {

namespace detail {

[[nodiscard]] inline uint32_t Crc32CRawSoftware(uint32_t crc,
                                                std::span<const uint8_t> data) {
  for (uint8_t byte : data) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      const uint32_t mask = -(crc & 1U);
      crc = (crc >> 1) ^ (0x82f63b78U & mask);
    }
  }
  return crc;
}

#if defined(__x86_64__) || defined(__i386__)
[[gnu::target("sse4.2")]] [[nodiscard]] uint32_t Crc32CRawHardware(
    uint32_t crc, std::span<const uint8_t> data);

// The crc32 register is GF(2)-linear in its input bits, so a register advanced
// by k zero bytes is a linear image: adv(r, k) = XOR over set bits j of r of
// adv(e_j, k). Columns are derived at runtime from the crc32 chain itself (no
// external constant tables), which keeps this provably equivalent to the
// chained instruction sequence.
struct AdvanceMatrix {
  std::array<uint32_t, 32> col;
};

// Chained (no lane dispatch) form; also the seed for the advance powers.
[[gnu::target("sse4.2")]] [[nodiscard]] inline uint32_t
Crc32CRawHardwareChained(uint32_t crc, const uint8_t* p, size_t n) {
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t chunk = 0;
    std::memcpy(&chunk, p + i, sizeof(chunk));
    crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
  }
  for (; i + 4 <= n; i += 4) {
    uint32_t chunk = 0;
    std::memcpy(&chunk, p + i, sizeof(chunk));
    crc = _mm_crc32_u32(crc, chunk);
  }
  for (; i < n; ++i) {
    crc = _mm_crc32_u8(crc, p[i]);
  }
  return crc;
}

[[nodiscard]] inline uint32_t ApplyAdvanceMatrix(const AdvanceMatrix& m,
                                                 uint32_t r) {
  uint32_t out = 0;
  while (r != 0) {
    const auto j = static_cast<unsigned>(__builtin_ctz(r));
    out ^= m.col[j];
    r &= r - 1;
  }
  return out;
}

// Advance-by-k matrices as powers of two: adv(a + b) == adv(a) o adv(b), so
// P[i] = P[i-1] squared covers any k with at most popcount(k) applies and no
// per-size state (page checksum spans vary per page fill; a size-keyed cache
// rebuilt 32-chain CRCs per unseen span and stormed on diverse fills).
[[nodiscard]] inline const std::array<AdvanceMatrix, 24>& AdvancePowers() {
  static const std::array<AdvanceMatrix, 24> powers = [] {
    std::vector<uint8_t> one_zero(1, 0);
    std::array<AdvanceMatrix, 24> p{};
    for (uint32_t j = 0; j < 32; ++j) {
      p[0].col[j] =
          Crc32CRawHardwareChained(1U << j, one_zero.data(), one_zero.size());
    }
    for (size_t i = 1; i < p.size(); ++i) {
      for (uint32_t j = 0; j < 32; ++j) {
        p[i].col[j] = ApplyAdvanceMatrix(p[i - 1], p[i - 1].col[j]);
      }
    }
    return p;
  }();
  return powers;
}

// Byte-indexed expansion of a composed advance matrix: one 256-entry table
// per register byte turns ApplyAdvanceMatrix's popcount(reg) XOR chain into
// four loads and three XORs.  Built lazily per (composed) advance distance
// and kept in a 4-slot fully-associative cache — page checksums reuse a
// fixed set of distances (32 KiB spans), so the cache hit rate is ~100%
// there; diverse record-sized spans rebuild an entry (a few thousand XORs)
// and fall back to the same behavior as the matrix walk.
struct AdvanceByteTable {
  size_t bytes{0};
  std::array<uint32_t, 256> byte_col[4];
};

[[nodiscard]] inline AdvanceMatrix ComposeAdvanceMatrix(size_t bytes) {
  const auto& powers = AdvancePowers();
  AdvanceMatrix composed{};
  bool first = true;
  while (bytes != 0) {
    const AdvanceMatrix& m =
        powers[static_cast<size_t>(__builtin_ctzll(bytes))];
    if (first) {
      composed = m;
      first = false;
    } else {
      AdvanceMatrix next{};
      for (uint32_t j = 0; j < 32; ++j) {
        next.col[j] = ApplyAdvanceMatrix(composed, m.col[j]);
      }
      composed = next;
    }
    bytes &= bytes - 1;
  }
  return composed;
}

[[nodiscard]] inline uint32_t ApplyByteAdvance(const AdvanceByteTable& table,
                                               uint32_t r) {
  return table.byte_col[0][r & 0xffU] ^ table.byte_col[1][(r >> 8) & 0xffU] ^
         table.byte_col[2][(r >> 16) & 0xffU] ^ table.byte_col[3][r >> 24];
}

[[nodiscard]] inline const AdvanceByteTable& AdvanceByteTableFor(size_t bytes) {
  // 16 slots: an 8-lane page CRC probes 8 distinct distances per call, so a
  // 4-slot cache would thrash on every invocation.
  static thread_local AdvanceByteTable slots[16];
  static thread_local size_t next = 0;
  for (const AdvanceByteTable& slot : slots) {
    if (slot.bytes == bytes) {
      return slot;
    }
  }
  AdvanceByteTable& slot = slots[next];
  next = (next + 1) & 15U;
  const AdvanceMatrix composed = ComposeAdvanceMatrix(bytes);
  for (size_t b = 0; b < 4; ++b) {
    // byte_col[b][v] = XOR of composed.col[b*8+bit] over the set bits of v:
    // build it incrementally from v & (v-1) (drop lowest set bit).
    slot.byte_col[b][0] = 0;
    for (uint32_t v = 1; v < 256; ++v) {
      const unsigned low = static_cast<unsigned>(__builtin_ctz(v));
      slot.byte_col[b][v] =
          slot.byte_col[b][v & (v - 1)] ^ composed.col[b * 8 + low];
    }
  }
  slot.bytes = bytes;
  return slot;
}

[[nodiscard]] inline uint32_t AdvanceRegisterBy(uint32_t r, size_t bytes) {
  return ApplyByteAdvance(AdvanceByteTableFor(bytes), r);
}

// Combining independent register states of consecutive slices (linearity):
// R(A || B, s) == adv(R(A, s), 8|B|) ^ R(B, 0), and R(M, s) ==
// R(M, 0) ^ adv(s, 8|M|). Running eight crc32 chains over disjoint slices
// hides the 3-cycle latency of the crc32 instruction and lifts throughput
// on page-sized buffers (the crc32_u64 retire slot saturates four chains).
[[gnu::target("sse4.2")]] [[nodiscard]] inline uint32_t Crc32CRawHardwareLanes(
    uint32_t crc, const uint8_t* p, size_t n) {
  const size_t lane = (n / 8) & ~size_t(7);
  if (lane == 0) {
    return Crc32CRawHardware(crc, std::span<const uint8_t>(p, n));
  }
  const uint8_t* p0 = p;
  const uint8_t* p1 = p0 + lane;
  const uint8_t* p2 = p1 + lane;
  const uint8_t* p3 = p2 + lane;
  const uint8_t* p4 = p3 + lane;
  const uint8_t* p5 = p4 + lane;
  const uint8_t* p6 = p5 + lane;
  const uint8_t* p7 = p6 + lane;
  const size_t last = n - (7 * lane);  // 8th slice is the longer one
  uint32_t r0 = 0;
  uint32_t r1 = 0;
  uint32_t r2 = 0;
  uint32_t r3 = 0;
  uint32_t r4 = 0;
  uint32_t r5 = 0;
  uint32_t r6 = 0;
  uint32_t r7 = 0;
  size_t i = 0;
  for (; i + 8 <= lane; i += 8) {
    uint64_t v0 = 0;
    uint64_t v1 = 0;
    uint64_t v2 = 0;
    uint64_t v3 = 0;
    uint64_t v4 = 0;
    uint64_t v5 = 0;
    uint64_t v6 = 0;
    uint64_t v7 = 0;
    std::memcpy(&v0, p0 + i, 8);
    std::memcpy(&v1, p1 + i, 8);
    std::memcpy(&v2, p2 + i, 8);
    std::memcpy(&v3, p3 + i, 8);
    std::memcpy(&v4, p4 + i, 8);
    std::memcpy(&v5, p5 + i, 8);
    std::memcpy(&v6, p6 + i, 8);
    std::memcpy(&v7, p7 + i, 8);
    r0 = static_cast<uint32_t>(_mm_crc32_u64(r0, v0));
    r1 = static_cast<uint32_t>(_mm_crc32_u64(r1, v1));
    r2 = static_cast<uint32_t>(_mm_crc32_u64(r2, v2));
    r3 = static_cast<uint32_t>(_mm_crc32_u64(r3, v3));
    r4 = static_cast<uint32_t>(_mm_crc32_u64(r4, v4));
    r5 = static_cast<uint32_t>(_mm_crc32_u64(r5, v5));
    r6 = static_cast<uint32_t>(_mm_crc32_u64(r6, v6));
    r7 = static_cast<uint32_t>(_mm_crc32_u64(r7, v7));
  }
  for (; i < lane; ++i) {
    r0 = _mm_crc32_u8(r0, p0[i]);
    r1 = _mm_crc32_u8(r1, p1[i]);
    r2 = _mm_crc32_u8(r2, p2[i]);
    r3 = _mm_crc32_u8(r3, p3[i]);
    r4 = _mm_crc32_u8(r4, p4[i]);
    r5 = _mm_crc32_u8(r5, p5[i]);
    r6 = _mm_crc32_u8(r6, p6[i]);
    r7 = _mm_crc32_u8(r7, p7[i]);
  }
  (void)0;
  const size_t tail = last - lane;  // bytes of slice 8 past the loop
  uint32_t out = Crc32CRawHardwareChained(r7, p7 + lane, tail);
  out ^= AdvanceRegisterBy(r0, n - lane);
  out ^= AdvanceRegisterBy(r1, n - (2 * lane));
  out ^= AdvanceRegisterBy(r2, n - (3 * lane));
  out ^= AdvanceRegisterBy(r3, n - (4 * lane));
  out ^= AdvanceRegisterBy(r4, n - (5 * lane));
  out ^= AdvanceRegisterBy(r5, n - (6 * lane));
  out ^= AdvanceRegisterBy(r6, last);
  out ^= AdvanceRegisterBy(crc, n);
  return out;
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
  // A/B hatch: TINYLAMB_DISABLE_CRC_LANES=1 restores the plain chained loop.
  static const bool lanes_disabled =
      std::getenv("TINYLAMB_DISABLE_CRC_LANES") != nullptr;
  if (data.size() >= 256 && !lanes_disabled) {
    return Crc32CRawHardwareLanes(crc, data.data(), data.size());
  }
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
}
#elif defined(__aarch64__)
[[gnu::target("+crc")]] [[nodiscard]] inline uint32_t Crc32CRawHardware(
    uint32_t crc, std::span<const uint8_t> data) {
  const uint8_t* p = data.data();
  const uint8_t* const end = data.data() + data.size();
  while (reinterpret_cast<uintptr_t>(p) + 8 <=
         reinterpret_cast<uintptr_t>(end)) {
    uint64_t chunk = 0;
    __builtin_memcpy(&chunk, p, sizeof(chunk));
    crc = __crc32cd(crc, chunk);
    p += 8;
  }
  while (reinterpret_cast<uintptr_t>(p) + 4 <=
         reinterpret_cast<uintptr_t>(end)) {
    uint32_t chunk = 0;
    __builtin_memcpy(&chunk, p, sizeof(chunk));
    crc = __crc32cw(crc, chunk);
    p += 4;
  }
  while (p < end) {
    crc = __crc32cb(crc, *p);
    ++p;
  }
  return crc;
}
#else
[[nodiscard]] inline uint32_t Crc32CRawHardware(uint32_t crc,
                                                std::span<const uint8_t> data) {
  return Crc32CRawSoftware(crc, data);
}
#endif

}  // namespace detail

// Castagnoli CRC-32C (reflected poly 0x82F63B78). Uses the hardware crc32
// instruction on x86-64 (SSE4.2) with a software bit-by-bit fallback.
// Known vector: Crc32C("123456789") == 0xe3069283.
[[nodiscard]] inline uint32_t Crc32C(std::span<const uint8_t> data) {
  return detail::Crc32CRawHardware(0xffffffffU, data) ^ 0xffffffffU;
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

class Crc32c final {
 public:
  Crc32c() = default;

  void Update(const void* data, size_t len) {
    value_ = Crc32CExtend(value_, data, len);
  }
  [[nodiscard]] uint32_t Finalize() const { return value_; }

 private:
  uint32_t value_{0xFFFFFFFFu};
};

[[nodiscard]] inline uint32_t Crc32cOf(const void* data, size_t len) {
  Crc32c c;
  c.Update(data, len);
  return c.Finalize();
}

[[nodiscard]] inline uint32_t Crc32cOf(const std::vector<char>& v) {
  return Crc32cOf(v.data(), v.size());
}

}  // namespace tinylamb

#endif  // TINYLAMB_CRC32C_HPP
