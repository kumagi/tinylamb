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

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <ostream>
#include <queue>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "table/pax_chunk/codec_block_benchmark.hpp"
#include "table/pax_chunk/pax_chunk.hpp"

#if defined(__AVX2__) || defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/platform/endian.hpp"
#include "common/status_or.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "table/workload/workload.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

#ifndef TINYLAMB_PAX_CHUNK_INTERNAL_HPP
#define TINYLAMB_PAX_CHUNK_INTERNAL_HPP

namespace tinylamb {
namespace pax_detail {

// The big-endian append helpers below are two stores each; leaving them
// out of line cost one call per encoded field, which the encoder profile
// attributed to AppendI64 rather than to the scheme that was running.
#if defined(__GNUC__) || defined(__clang__)
#define TINYLAMB_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define TINYLAMB_ALWAYS_INLINE inline
#endif

// Grow `buf` by `n` bytes and return a writable pointer to the new tail.
// One capacity check for the whole append; the caller then stores its payload
// directly. The previous byte-at-a-time push_back loop paid vector's
// per-element bookkeeping (capacity load + _M_finish bump + placement-new)
// eight times per 8-byte field, which the encoder profile attributed to
// stl_vector.h push_back rather than to the encoder itself.
TINYLAMB_ALWAYS_INLINE char* BufTail(std::vector<char>& buf, size_t n) {
  const size_t old = buf.size();
  if (old + n > buf.capacity()) {
    buf.reserve(old + n + (old >> 1) + 16);
  }
  if (n <= 8) {
    // Small appends (the AppendBE* header writes, n a compile-time
    // constant): straight-line push_backs instead of resize(old + n) --
    // resize routes a memset PLT call for the 2-8 value-init bytes
    // (~190 ir per append on the encoder trial loops), and the caller
    // overwrites every byte anyway.
    for (size_t i = 0; i < n; ++i) buf.push_back(static_cast<char>(0));
  } else {
    // Bulk planes (AppendPackBits appends whole bit-planes): one memset
    // beats per-byte stores by orders of magnitude.
    buf.resize(old + n);
  }
  return buf.data() + old;
}

// Encode a 4-byte big-endian uint32 into the buffer.
TINYLAMB_ALWAYS_INLINE void AppendBE32(std::vector<char>& buf, uint32_t v) {
  const uint32_t be = htobe32(v);
  std::memcpy(BufTail(buf, sizeof(be)), &be, sizeof(be));
}

// Encode a 2-byte big-endian uint16.
TINYLAMB_ALWAYS_INLINE void AppendBE16(std::vector<char>& buf, uint16_t v) {
  const uint16_t be = htobe16(v);
  std::memcpy(BufTail(buf, sizeof(be)), &be, sizeof(be));
}

// Encode a 8-byte big-endian uint64.
TINYLAMB_ALWAYS_INLINE void AppendBE64(std::vector<char>& buf, uint64_t v) {
  const uint64_t be = htobe64(v);
  std::memcpy(BufTail(buf, sizeof(be)), &be, sizeof(be));
}

inline uint32_t ReadBE32(const char* p) {
  uint32_t v;
  memcpy(&v, p, sizeof(uint32_t));
  return be32toh(v);
}

inline uint16_t ReadBE16(const char* p) {
  uint16_t v;
  memcpy(&v, p, sizeof(uint16_t));
  return be16toh(v);
}

inline uint64_t ReadBE64(const char* p) {
  uint64_t v;
  memcpy(&v, p, sizeof(uint64_t));
  return be64toh(v);
}

inline int BitsNeeded(uint64_t maxv) {
  int w = 0;
  while (maxv != 0) {
    ++w;
    maxv >>= 1;
  }
  return w;
}

// Append the MSB-first bit stream of `vals` at `w` bits per value to `out`.
// Writing straight into the caller's region (instead of returning a fresh
// vector that the caller then copies in) removes one heap allocation and one
// full memcpy per packed region -- the adaptive selector packs a region for
// nearly every candidate on every row group of every column.
inline void AppendPackBits(std::vector<char>& out,
                           const std::vector<uint64_t>& vals, int w) {
  if (w == 0 || vals.empty()) return;
  const uint32_t width = static_cast<uint32_t>(w);
  const size_t total_bits = vals.size() * static_cast<size_t>(width);
  // Same MSB-first bit stream as the old __int128 shift register, but the
  // accumulator lives in one u64 kept left-aligned so a full word flushes
  // with a single bswap store instead of one shift+mask store per byte.
  const uint64_t mask = width == 64 ? ~0ULL : ((1ULL << width) - 1);
  uint64_t acc = 0;  // `fill` bits of stream, MSB-aligned
  uint32_t fill = 0;
  char* base = BufTail(out, (total_bits + 7) / 8);
  for (uint64_t value : vals) {
    const uint64_t v = value & mask;
    if (fill + width <= 64) {
      acc |= v << (64 - fill - width);
      fill += width;
      continue;
    }
    // Word boundary: fill the top `room` bits, flush 8 bytes, keep the
    // leftover bits of this value as the new left-aligned accumulator.
    const uint32_t room = 64 - fill;
    acc |= room != 0 ? (v >> (width - room)) : 0;
    const uint64_t be = htobe64(acc);
    std::memcpy(base, &be, sizeof(be));
    base += sizeof(be);
    const uint32_t rem = width - room;
    acc = rem != 0 ? (v << (64 - rem)) : 0;
    fill = rem;
  }
  // Tail: up to 7 bits (or a whole w==64 word) left in the accumulator.
  const uint32_t tail_bytes = (fill + 7) / 8;
  for (uint32_t b = 0; b < tail_bytes; ++b) {
    *base++ =
        static_cast<char>(static_cast<uint8_t>((acc >> (56 - 8 * b)) & 0xFFU));
  }
}

inline uint64_t UnpackBitAt(const char* base, uint64_t bit_off, int w) {
  if (w <= 0) return 0;
  // Byte-window fast path: assemble only the bytes the value occupies
  // (never touching a byte the bit-at-a-time loop would not have read, so
  // tail regions stay safe) and extract with one shift. This sits on the
  // per-row BitWeave/DICT/Elias-Fano-low decode paths, where the old loop
  // issued one checked byte load per bit.
  if (w <= 57) {
    const uint64_t sh = bit_off & 7;
    const char* p = base + (bit_off >> 3);
    const uint64_t need = (sh + static_cast<uint64_t>(w) + 7) / 8;
    uint64_t window = 0;
    for (uint64_t i = 0; i < need; ++i) {
      window = (window << 8) | static_cast<uint8_t>(p[i]);
    }
    return (window >> (need * 8 - sh - static_cast<uint64_t>(w))) &
           ((static_cast<uint64_t>(1) << w) - 1);
  }
  uint64_t v = 0;
  for (int b = 0; b < w; ++b) {
    const uint64_t bit = bit_off + static_cast<uint64_t>(b);
    v = (v << 1) |
        static_cast<uint64_t>(
            (static_cast<uint8_t>(base[bit >> 3]) >> (7 - (bit & 7))) & 1);
  }
  return v;
}
struct RleBpRegion {
  const char* values{nullptr};
  const char* lengths{nullptr};
  uint32_t n{0};
  uint32_t runs{0};
  uint32_t value_width{0};
  uint32_t length_width{0};
  int64_t base{0};
};
constexpr uint32_t kRleIdxRestartStride = 16;
struct RleIdxRegion {
  const char* values{nullptr};   // FOR-BA value stream
  const char* lengths{nullptr};  // FOR-BA length stream
  const char* restart{nullptr};  // nidx entries, u32 BE
  uint32_t n{0};
  uint32_t runs{0};
  uint32_t nidx{0};
  uint32_t value_bpv{0};
  uint32_t length_bpv{0};
  int64_t value_base{0};
  int64_t length_base{0};
};
constexpr uint32_t kStreamVBytePerCtrl = 4;   // values per control byte
constexpr uint32_t kStreamVByteMaxWidth = 4;  // bytes per value
constexpr uint32_t kStreamVByteBlock = 16;    // values per block-index entry
struct StreamVByteRegion {
  const uint8_t* block_off{nullptr};
  const uint8_t* ctrl{nullptr};
  const uint8_t* payload{nullptr};
  uint32_t n{0};
  uint32_t stream_len{0};
  uint32_t nblk{0};
  uint32_t ctrl_len{0};
};
struct Rle8Run {
  uint32_t start;
  uint32_t length;
  uint8_t value;
};
struct Rle8Region {
  Rle8Region()  // NOLINT(cppcoreguidelines-pro-type-member-init)
      : n(0), total_runs(0), plane_runs{}, plane_offsets{} {}

  uint32_t n;
  uint32_t total_runs;
  std::array<uint32_t, 8> plane_runs;
  std::array<uint32_t, 8> plane_offsets;
  std::array<Rle8Run, PaxChunk::kRowGroupSize * 8> runs;
};
struct RleDeltaRun {
  uint32_t start;
  uint32_t length;
  int64_t value;
};
struct RleDeltaRegion {
  RleDeltaRegion() = default;  // NOLINT(cppcoreguidelines-pro-type-member-init)

  uint32_t n{0};
  uint32_t run_count{0};
  uint32_t value_width{0};
  uint32_t length_width{0};
  std::array<RleDeltaRun, PaxChunk::kRowGroupSize> runs;
};
constexpr uint32_t kDeltaCheckpointEvery = 128;
constexpr uint32_t kInterpBlockRows = kDeltaCheckpointEvery;
constexpr uint32_t kDeltaPForBlockRows = kDeltaCheckpointEvery;
constexpr uint32_t kD2PforCheckpointEvery = 256;
struct D2PforRegion {
  const char* ckpt{nullptr};
  const char* inner{nullptr};
  uint32_t n{0};
  uint32_t ncp{0};
};
constexpr uint32_t kSimdForLanes = 4;
constexpr uint32_t kSimdForGroupBytes = 32;
constexpr uint32_t kSimdForHeaderBytes = 8;  // the frame-of-reference base
struct SimdForRegion {
  const char* groups;  // first group's first lane
  int64_t base;
  uint32_t w;
  uint32_t n;
};
constexpr size_t kDeltaPForBracketBytes = 32;
constexpr int kInterpMaxDepth = 12;
constexpr uint64_t kWahSpan = 63;
constexpr uint64_t kWahMask63 = (1ULL << kWahSpan) - 1;
constexpr uint64_t kWahLenMask = (1ULL << 62) - 1;
constexpr uint32_t kBp128BlockRows = 128;
constexpr uint32_t kBp128MaxBlocks = PaxChunk::kRowGroupSize / kBp128BlockRows;
constexpr size_t kBp128MetadataBytes = 33;
struct Bp128Block {
  const char* packed{nullptr};
  const char* packed_end{nullptr};
  uint32_t rows{0};
  uint32_t width{0};
  int64_t minimum{0};
  int64_t maximum{0};
  __int128 sum{0};
};
struct Bp128Region {
  std::array<Bp128Block, kBp128MaxBlocks> blocks{};
  uint32_t n{0};
  uint32_t block_count{0};
  size_t encoded_size{0};
};
struct DeltaPForRegion {
  const char* brackets{nullptr};
  const char* packed{nullptr};
  const char* exceptions{nullptr};
  uint32_t n{0};
  uint32_t blocks{0};
  uint32_t width{0};
  uint32_t exceptions_count{0};
  uint64_t delta_min{0};
};
constexpr uint64_t kRiceQMax = 65535;
constexpr size_t kMaxModeKeys = 4096;
constexpr size_t kMaxIdxDistinct = 64;
// Splitmix64 finalizer shared by the profiler (bloom/HLL), the Xor cell
// builder, and every Xor reader (reimplemented in the WASM module; keep the
// constants in lockstep).
inline uint64_t XorMix64(uint64_t h) {
  h ^= h >> 33;
  h *= 0xFF51AFD7ED558CCDULL;
  h ^= h >> 33;
  h *= 0xC4CEB9FE1A85EC53ULL;
  h ^= h >> 33;
  return h;
}
inline uint64_t XorHash64(int64_t v, uint32_t seed) {
  return XorMix64(static_cast<uint64_t>(v) +
                  static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL);
}
inline uint64_t XorRotL(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

class FastMod64 {
 public:
  explicit FastMod64(uint64_t m)
      : divisor_(m), reciprocal_(m <= 1 ? 0 : (~0ULL / m)) {}
  uint32_t operator()(uint64_t n) const {
    if (divisor_ <= 1) return 0;
    const uint64_t quotient = static_cast<uint64_t>(
        (static_cast<unsigned __int128>(reciprocal_) * n) >> 64);
    uint64_t remainder = n - quotient * divisor_;
    if (remainder >= divisor_) remainder -= divisor_;
    return static_cast<uint32_t>(remainder);
  }

 private:
  uint64_t divisor_;
  uint64_t reciprocal_;
};
class FlatMapI64 {
 public:
  explicit FlatMapI64(size_t max_entries) : max_entries_(max_entries) {}
  FlatMapI64(FlatMapI64&&) = default;
  FlatMapI64& operator=(FlatMapI64&&) = default;

  // Returns the stored value address, or nullptr when absent. Both key
  // signednesses are accepted (callers mix int64 values and uint64 bit
  // patterns); the cast is explicit so -Wsign-conversion stays quiet.
  uint64_t* Find(int64_t key) { return FindU(static_cast<uint64_t>(key)); }
  uint64_t* Find(uint64_t key) { return FindU(key); }
  [[nodiscard]] const uint64_t* Find(int64_t key) const {
    return FindU(static_cast<uint64_t>(key));
  }
  [[nodiscard]] const uint64_t* Find(uint64_t key) const { return FindU(key); }
  // Inserts key->val. Caller guarantees the key is absent (the encoder
  // pattern is Find() first); a violated contract overwrites one slot.
  void InsertNew(int64_t key, uint64_t val) {
    InsertNewU(static_cast<uint64_t>(key), val);
  }
  void InsertNew(uint64_t key, uint64_t val) { InsertNewU(key, val); }
  // One-probe counting upsert: returns the value slot for `key`, inserting
  // the key with value 0 when absent. Halves the hash work of the
  // Find()+InsertNew() pair on counters (FSST symbol mining, profile xor
  // sets). Grows the table at the 50% load factor, so unlike max_entries_
  // construction this path never saturates the probe space.
  uint64_t* UpsertSlot(uint64_t key) {
    if (used_.empty() || (count_ + 1) * 2 > used_.size()) GrowForUpsert();
    const size_t i = ProbeU(key);
    if (used_[i] == 0) {
      used_[i] = 1;
      slots_[i].key = key;
      slots_[i].val = 0;
      ++count_;
    }
    return &slots_[i].val;
  }
  // Prehashed variants: the caller already computed XorMix64(key) for its own
  // bookkeeping (the profile sweep feeds the same mix into the bloom bit, the
  // mode front and the HLL), so the map probe takes it instead of recomputing
  // the 2-multiply mix chain once or twice more per cell. `h` must be exactly
  // XorMix64(key); GrowForUpsert still rehashes with its own hashes.
  uint64_t* UpsertSlotPrehashed(uint64_t key, uint64_t h) {
    if (used_.empty() || (count_ + 1) * 2 > used_.size()) GrowForUpsert();
    const size_t i = ProbeUH(key, h);
    if (used_[i] == 0) {
      used_[i] = 1;
      slots_[i].key = key;
      slots_[i].val = 0;
      ++count_;
    }
    return &slots_[i].val;
  }
  uint64_t* FindPrehashed(uint64_t key, uint64_t h) {
    if (used_.empty()) return nullptr;
    const size_t i = ProbeUH(key, h);
    return used_[i] != 0 ? &slots_[i].val : nullptr;
  }
  [[nodiscard]] size_t size() const { return count_; }
  // Drop every entry but keep the slot storage. The row-group profiler reuses
  // one FlatMapI64 per (column, chunk) instead of reallocating a fresh table
  // for each of the ~row_count/1024 groups, so the per-build malloc/free and
  // zero-fill traffic (two 96 KiB tables per int64 column) disappears.
  void Reset() {
    count_ = 0;
    if (!used_.empty()) std::memset(used_.data(), 0, used_.size());
  }
  // Reset plus a capacity contract: after this, inserting up to max_entries
  // distinct keys cannot exceed the 50% load factor (the same guarantee the
  // (max_entries) constructor gives), while the slot storage is reused
  // across calls -- the per-attempt encoders stop paying a 32 KiB
  // allocation pair plus a used_-bitmap zero fill on every row group.
  void ResetFor(size_t max_entries) {
    count_ = 0;
    const size_t want = CapacityFor(max_entries);
    if (used_.size() < want) {
      slots_.reset(new Slot[want]);
      used_.assign(want, 0);
      return;
    }
    if (!used_.empty()) std::memset(used_.data(), 0, used_.size());
  }
  // Slots are materialised on the first insert: a map that is only ever
  // Find()-ed (or never used at all) costs no 64 KiB table.
  void EnsureSlots() {
    if (!used_.empty()) return;
    const size_t cap = CapacityFor(max_entries_);
    slots_.reset(new Slot[cap]);
    used_.assign(cap, 0);
  }
  template <class Fn>
  void ForEach(Fn&& fn) const {
    if (used_.empty()) return;
    // Forward once into a reference binding (a bind is not a move) and
    // invoke through it: calling std::forward<Fn>(fn) per iteration would
    // move an rvalue callable on the first slot and then use it again.
    auto&& fn_ref = std::forward<Fn>(fn);
    // Hoisted locals: the naive member walk re-loaded used_.data() and
    // used_.size() through `this` on EVERY iteration (the callback may
    // mutate a vector, so the compiler could not prove `this` stable), and
    // that two-load dependent chain was the hottest cycle block of the whole
    // chunk builder at 1024-distinct cells. Slots are visited in the same
    // ascending order as before, so every ForEach consumer sees the exact
    // same sequence.
    const uint8_t* const used = used_.data();
    const size_t n = used_.size();
    const Slot* const slots = slots_.get();
    for (size_t i = 0; i < n; ++i) {
      if (used[i] != 0) fn_ref(slots[i].key, slots[i].val);
    }
  }

 private:
  // 16-byte slot: the occupancy flag lives in a parallel byte array instead of
  // a third struct member, which padded the slot to 24 bytes and tripled the
  // bytes Reset() has to touch. `used_` is also what keeps the probe loop's
  // hot data (key) in one cache line.
  struct Slot {
    uint64_t key = 0;
    uint64_t val = 0;
  };
  uint64_t* FindU(uint64_t key) {
    if (used_.empty()) return nullptr;
    const size_t i = ProbeU(key);
    return used_[i] != 0 ? &slots_[i].val : nullptr;
  }
  const uint64_t* FindU(uint64_t key) const {
    if (used_.empty()) return nullptr;
    const size_t i = ProbeU(key);
    return used_[i] != 0 ? &slots_[i].val : nullptr;
  }
  void InsertNewU(uint64_t key, uint64_t val) {
    EnsureSlots();
    const size_t i = ProbeU(key);
    used_[i] = 1;
    slots_[i].key = key;
    slots_[i].val = val;
    ++count_;
  }
  static size_t CapacityFor(size_t max_entries) {
    size_t cap = 16;
    while (cap < (max_entries + 1) * 2) cap <<= 1;
    return cap;
  }
  // Rehash into a table twice the size (fresh tables start at 1 Ki slots:
  // 16 KiB of hot data instead of the multi-megabyte worst-case sizing the
  // constructor's max_entries forces, which is what made big counting maps
  // cache-hostile). Frequencies are copied verbatim, so counters are exact.
  void GrowForUpsert() {
    const size_t next = used_.empty() ? CapacityFor(512) : used_.size() * 2;
    auto ns = std::make_unique<Slot[]>(next);
    std::vector<uint8_t> nu(next, 0);
    const size_t mask = next - 1;
    for (size_t i = 0; i < used_.size(); ++i) {
      if (used_[i] == 0) continue;
      size_t j = XorMix64(slots_[i].key) & mask;
      while (nu[j] != 0) j = (j + 1) & mask;
      nu[j] = 1;
      ns[j] = slots_[i];
    }
    slots_ = std::move(ns);
    used_ = std::move(nu);
  }
  size_t slots_size() const { return used_.size(); }
  size_t ProbeU(uint64_t key) {
    const uint8_t* used = used_.data();
    const Slot* base = slots_.get();
    const size_t mask = used_.size() - 1;
    size_t i = XorMix64(key) & mask;
    while (used[i] != 0 && base[i].key != key) i = (i + 1) & mask;
    return i;
  }
  // ProbeU with the caller's XorMix64(key) already in hand.
  size_t ProbeUH(uint64_t key, uint64_t h) {
    const uint8_t* used = used_.data();
    const Slot* base = slots_.get();
    const size_t mask = used_.size() - 1;
    size_t i = h & mask;
    while (used[i] != 0 && base[i].key != key) i = (i + 1) & mask;
    return i;
  }
  size_t ProbeU(uint64_t key) const {
    const uint8_t* used = used_.data();
    const Slot* base = slots_.get();
    const size_t mask = used_.size() - 1;
    size_t i = XorMix64(key) & mask;
    while (used[i] != 0 && base[i].key != key) i = (i + 1) & mask;
    return i;
  }
  // Slot contents are don't-care whenever used_[i] == 0 (every read and
  // every probe step consults the occupancy byte first), so the slot array
  // is allocated raw: vector's value-init would zero-fill 16 bytes per slot
  // on every construction, and the dict-family encoders construct one of
  // these per row-group attempt (32 KiB of dead memset per call).
  std::unique_ptr<Slot[]> slots_;
  std::vector<uint8_t> used_;
  size_t max_entries_ = 0;
  size_t count_ = 0;
};
using Scheme = PaxChunk::Scheme;
constexpr size_t kNoEncodeBudget = SIZE_MAX;
constexpr uint32_t kWaveletRRankEvery = 256;
struct WaveletRRegion {
  const char* bitmaps{nullptr};
  const char* ranks{nullptr};
  int64_t minimum{0};
  uint32_t n{0};
  uint32_t width{0};
  uint32_t stride{0};
  uint32_t rank_count{0};
};
constexpr uint32_t kQppPageRows = 128;
struct QppPage {
  const uint8_t* bitmap{nullptr};
  const char* exceptions{nullptr};
  int64_t a{0};
  int64_t d1{0};
  int64_t d2{0};
  uint32_t rows{0};
  uint32_t exception_count{0};
};
struct QppRegion {
  uint32_t n{0};
  uint32_t pages{0};
  const char* end{nullptr};
  std::array<QppPage, PaxChunk::kRowGroupSize / kQppPageRows> page{};
};
struct QppSegment {
  uint32_t begin;
  uint32_t end;
  bool increasing;
};
struct QppRange {
  uint32_t begin;
  uint32_t end;
};
struct BitWriter {
  std::vector<char> buf;
  uint64_t acc{0};
  int cnt{0};
  inline void put1(int bit) {
    acc = (acc << 1) | (bit & 1);
    if (++cnt == 64) {
      for (int i = 7; i >= 0; --i)
        buf.push_back((char)((acc >> (i * 8)) & 0xFF));
      acc = 0;
      cnt = 0;
    }
  }
  inline void put(uint64_t v, int w) {
    for (int i = w - 1; i >= 0; --i) put1((int)((v >> i) & 1));
  }
  void finish() {
    // Pad to the next BYTE boundary (not 64): driving cnt to a full 64-bit
    // flush here would bloat every Gorilla stream that is not a multiple of
    // 64 bits by up to 7 bytes. Pad to 8-bit alignment instead so the
    // stream ends at the first byte boundary.
    while (cnt % 8 != 0) put1(0);
    // cnt is now a multiple of 8 in [0, 64): flush the remaining bytes
    // (MSB-first), not just full 64-bit groups.
    for (int k = cnt / 8; k >= 1; --k)
      buf.push_back((char)((acc >> ((k - 1) * 8)) & 0xFF));
    acc = 0;
    cnt = 0;
  }
};
struct BitReader {
  const char* p;
  // One past the last readable byte; nullptr keeps the legacy unbounded
  // behavior. Hostile streams (unvalidated regions) must not walk off the
  // end of the chunk buffer: reads past `end` yield 0 bits, which every
  // consumer decodes as benign data and the QMAX/width caps keep finite.
  const char* end{nullptr};
  uint64_t bitbuf{0};
  int bits{0};
  BitReader(const char* ptr, const char* e = nullptr) : p(ptr), end(e) {}
  inline int get1() {
    if (bits == 0) {
      if (end != nullptr && p >= end) return 0;
      bitbuf = static_cast<uint8_t>(*p++);
      bits = 8;
    }
    int b = static_cast<int>((bitbuf >> (bits - 1)) & 1);
    --bits;
    return b;
  }
  inline uint64_t get(int w) {
    uint64_t v = 0;
    for (int i = 0; i < w; ++i) v = (v << 1) | static_cast<uint64_t>(get1());
    return v;
  }
};
struct WordBitReader {
  const uint8_t* p;
  const uint8_t* end;
  uint64_t bitpos{0};  // absolute bit offset of the next unconsumed stream bit
  WordBitReader() : p(nullptr), end(nullptr) {}
  WordBitReader(const char* base, const char* e)
      : p(reinterpret_cast<const uint8_t*>(base)),
        end(reinterpret_cast<const uint8_t*>(e)) {}
  __attribute__((always_inline)) inline uint64_t get(int w) {
    if (w <= 0) return 0;
    const uint64_t byte = bitpos >> 3;
    const int sh = static_cast<int>(bitpos & 7);
    // Fast path: the whole field fits in one aligned window fully inside
    // [p, end) -- the shape of every steady-state decode loop.
    if (__builtin_expect(sh + w <= 64 && p + byte + 8 <= end, 1)) {
      uint64_t word;
      std::memcpy(&word, p + byte, sizeof(word));
      const int drop = 64 - sh - static_cast<int>(w);
      bitpos += static_cast<unsigned>(w);
      return (be64toh(word) >> drop) &
             ((w >= 64) ? ~static_cast<uint64_t>(0)
                        : ((static_cast<uint64_t>(1) << w) - 1));
    }
    return get_slow(w);
  }
  // Byte-at-a-time fallback; returns the same values the historical
  // refill/serve loop returned, including end-of-stream truncation.
  uint64_t get_slow(int w) {
    uint64_t v = 0;
    while (w > 0) {
      const uint64_t byte = bitpos >> 3;
      if (p + byte >= end) return v;  // zero bits past the physical end
      const int avail = 8 - static_cast<int>(bitpos & 7);
      const int take = (w < avail) ? w : avail;
      v = (v << take) | ((static_cast<uint64_t>(p[byte]) >> (avail - take)) &
                         ((static_cast<uint64_t>(1) << take) - 1));
      bitpos += static_cast<unsigned>(take);
      w -= take;
    }
    return v;
  }
  inline int get1() {
    const uint64_t byte = bitpos >> 3;
    if (p + byte + 8 <= end) {
      uint64_t word;
      std::memcpy(&word, p + byte, sizeof(word));
      const uint64_t v = (be64toh(word) >> (63 - (bitpos & 7))) & 1ULL;
      ++bitpos;
      return static_cast<int>(v);
    }
    return static_cast<int>(get_slow(1));
  }
};
constexpr uint32_t kHuffDictMaxLen = 15;
constexpr uint32_t kHuffDictCheckpoint = 64;
constexpr uint32_t kHuffDictMaxCodes = 1024;
constexpr uint32_t kPostingsMaxLists = 128;
struct HuffDictRegion {
  uint32_t d{0};
  uint32_t n{0};
  uint32_t ncp{0};
  uint32_t stream_len{0};
  const uint8_t* dict{nullptr};    // d x 8B BE, ascending
  const uint8_t* lens{nullptr};    // ceil(d/2) B, nibble per symbol
  const uint8_t* freq{nullptr};    // d x u16 BE, exact row counts
  const uint8_t* ckpt{nullptr};    // ncp x u32 BE, absolute bit offsets
  const uint8_t* stream{nullptr};  // stream_len B
};
struct HuffDictTables {
  uint16_t count[kHuffDictMaxLen + 1]{};
  uint16_t offset[kHuffDictMaxLen + 1]{};  // first sym_sorted index per length
  uint16_t sym_sorted[kHuffDictMaxCodes]{};
  // Peek LUT: entry = high 8 bits of the stream; len != 0 marks a resolved
  // code (len <= 8), len == 0 = "walk" (8-bit prefix is ambiguous).
  uint8_t lut_len[256]{};
  uint16_t lut_sym[256]{};
};
constexpr uint32_t kS4Block = 64;
struct S4Region {
  const char* widths{nullptr};  // nblk width bytes
  const char* lows{nullptr};    // nblk FOR bases, 8B BE each
  const char* ckpt{
      nullptr};  // nblk-1 words: the value of each block's first row
  const char* payload{nullptr};  // per-block payloads, each byte-aligned
  uint32_t n{0};
  uint32_t nblk{0};
  int64_t base{0};  // v[0]; also block 0's seek value
};
constexpr uint64_t kSforDirEntry =
    19;  // msb 8B + base 8B + row_start 2B + w 1B
struct SforRegion {
  const char* dir{nullptr};      // ng directory entries, 19B each
  const char* payload{nullptr};  // per-group payloads, each byte-aligned
  const char* end{nullptr};      // one past the region (bounds the bit reads)
  uint32_t n{0};
  uint32_t s{0};  // split: msb = v >> s, lsbits = v & ((1<<s)-1)
  uint32_t ng{0};
};
constexpr uint64_t kExpForDirEntry =
    13;  // gkey 2B + base 8B + row_start 2B + w 1B
constexpr uint32_t kExpForMaxRuns = 64;
constexpr uint64_t kExpForMantMask = (UINT64_C(1) << 52) - 1;
struct ExpForRegion {
  const char* dir{nullptr};      // ng directory entries, 13B each
  const char* payload{nullptr};  // per-group payloads, each byte-aligned
  const char* end{nullptr};      // one past the region (bounds the bit reads)
  uint32_t n{0};
  uint32_t ng{0};
};
constexpr uint32_t kSortDictBlock = 64;
constexpr uint32_t kSortDictMaxBlocks =
    static_cast<uint32_t>(PaxChunk::kRowGroupSize / kSortDictBlock);
struct SortDictRegion {
  const char* widths{nullptr};  // nb width bytes (w u8 each)
  const char* firsts{nullptr};  // nb x 8B BE: sorted[64b]
  const char* psums{nullptr};   // (nb+1) x 8B BE wrapped prefix sums
  const char* rank{nullptr};    // ceil(n*rw/8) B MSB-first
  const char* gaps{nullptr};    // per-block byte-aligned gap planes
  uint32_t n{0};
  uint32_t nb{0};
  int rw{0};
};
struct PostingsRegion {
  uint32_t n = 0;
  uint32_t d = 0;
  const char* dict = nullptr;        // d x 8B BE, strictly ascending
  const char* counts = nullptr;      // d x u16 BE, each >= 1, sum == n
  const char* ks = nullptr;          // ceil(d/2) x u8, low nibble = even list
  const char* firsts = nullptr;      // d x u16 BE
  const char* bitoffs = nullptr;     // d x u16 BE, bit position of each list
  const char* stream = nullptr;      // MSB-first Rice bitstream
  const char* region_end = nullptr;  // one past the last stream byte
};
struct PostingsBitCursor {
  const uint8_t* p = nullptr;  // next unread byte
  const uint8_t* end = nullptr;
  uint64_t window = 0;  // bit63 = next bit
  uint32_t held = 0;    // unconsumed bits in window

  void Init(const char* base, const char* limit) {
    p = reinterpret_cast<const uint8_t*>(base);
    end = reinterpret_cast<const uint8_t*>(limit);
    window = 0;
    held = 0;
  }

 private:
  // Refill with the next (up to) 8 bytes; a short tail zero-pads the low
  // bits, which the unary scan treats as never-one (it can only stop on a
  // zero that is still inside `held`, and the validator's consumption
  // check rules a zero padding tail out on valid regions).
  bool Refill() {
    if (p >= end) return false;
    const size_t avail = static_cast<size_t>(end - p);
    const size_t take = avail < 8 ? avail : 8;
    uint64_t raw = 0;
    std::memcpy(&raw, p, take);
    window = be64toh(raw);
    held = static_cast<uint32_t>(take) * 8;
    p += take;
    return true;
  }

 public:
  // One Rice(2^k) code: quotient in unary (q ones then a zero), then k
  // remainder bits. Fails only at a truncated stream (the validator's
  // consumption check makes that unreachable on a validated region).
  bool Rice(uint32_t k, uint64_t* out) {
    uint64_t q = 0;
    for (;;) {
      if (held == 0) {
        if (!Refill()) return false;  // unary run with no stop bit
      }
      const uint64_t inv = ~window;
      if (inv != 0) {
        const uint32_t zeros = static_cast<uint32_t>(__builtin_clzll(inv));
        if (zeros < held) {
          q += zeros;  // the stop bit consumed below
          // Drop (zeros + 1) bits: the ones and their stop.
          if (zeros + 1 == 64) {
            window = 0;
          } else {
            window <<= zeros + 1;
          }
          held -= zeros + 1;
          break;
        }
        q += held;  // all-ones window: refill and keep counting
      }
      if (!Refill()) return false;
    }
    // Remainder: k bits taken from the top of the window, refilling as
    // needed (the window is top-aligned -- bit63 is the next bit -- so a
    // short window is TOPPED UP, never shifted left: shifting would push
    // its valid bits out of the word). take is in [1, k] with k <= 15.
    uint64_t rem = 0;
    uint32_t got = 0;
    while (got < k) {
      if (held == 0 && !Refill()) return false;
      const uint32_t take = held < (k - got) ? held : (k - got);
      rem = (rem << take) | (window >> (64 - take));
      window <<= take;
      held -= take;
      got += take;
    }
    *out = (q << k) + rem;
    return true;
  }
};
struct CachedBitReader {
  const uint8_t* p{nullptr};
  uint64_t limit{0};
  uint64_t bytes{0};
  uint64_t pos{0};
  uint64_t cache_start{0};
  uint64_t cache_end{0};
  uint64_t cache{0};

  CachedBitReader(const char* base, uint64_t bits)
      : p(reinterpret_cast<const uint8_t*>(base)),
        limit(bits),
        bytes((bits + 7) / 8) {}

  __attribute__((always_inline)) inline bool refill() {
    const uint64_t byte = pos >> 3;
    if (byte >= bytes) return false;
    const uint64_t available = bytes - byte;
    uint64_t word = 0;
    const size_t load = available < 8 ? available : 8;
    std::memcpy(&word, p + byte, load);
    const int sh = static_cast<int>(pos & 7);
    cache = be64toh(word);
    cache_start = pos - static_cast<uint64_t>(sh);
    const uint64_t held = load * 8 - static_cast<uint64_t>(sh);
    const uint64_t remaining = limit - cache_start;
    cache_end = cache_start + (held < remaining ? held : remaining);
    return true;
  }

  __attribute__((always_inline)) inline uint64_t get(int w) {
    if (w <= 0) return 0;
    if (static_cast<uint64_t>(w) > limit - pos) return 0;
    if (pos + static_cast<uint64_t>(w) > cache_end) {
      if (!refill()) return 0;
      if (w > 64 - static_cast<int>(pos - cache_start)) return get_slow(w);
    }
    const int drop = 64 - static_cast<int>(pos - cache_start) - w;
    const uint64_t value = cache >> drop;
    pos += static_cast<uint64_t>(w);
    return w >= 64 ? value : value & ((static_cast<uint64_t>(1) << w) - 1);
  }

  uint64_t get_slow(int w) {
    uint64_t value = 0;
    while (w > 0) {
      const uint64_t byte = pos >> 3;
      if (byte >= bytes) return value;
      const int available = 8 - static_cast<int>(pos & 7);
      const int take = w < available ? w : available;
      value = (value << take) |
              ((static_cast<uint64_t>(p[byte]) >> (available - take)) &
               ((static_cast<uint64_t>(1) << take) - 1));
      pos += static_cast<uint64_t>(take);
      w -= take;
    }
    return value;
  }

  __attribute__((always_inline)) inline int get1() {
    if (pos >= limit) return 0;
    if (pos + 1 > cache_end && !refill()) return 0;
    const uint64_t value = cache >> (63 - static_cast<int>(pos - cache_start));
    ++pos;
    return static_cast<int>(value & 1U);
  }
};
constexpr uint32_t kChimp128BlockRows = 128;
constexpr uint32_t kChimp128MaxBlocks = PaxChunk::kRowGroupSize / 128;
constexpr size_t kChimp128BlockMetadataBytes = 38;
struct Chimp128Block {
  const char* stream{nullptr};
  uint32_t rows{0};
  uint32_t code_bits{0};
  uint64_t first{0};
  uint64_t minimum{0};
  uint64_t maximum{0};
  uint64_t sum{0};
  bool numeric_metadata{false};
};
struct Chimp128Region {
  std::array<Chimp128Block, kChimp128MaxBlocks> blocks{};
  uint32_t n{0};
  uint32_t block_count{0};
  size_t encoded_size{0};
};
constexpr uint32_t kAlpRdTansBlockRows = 128;
constexpr uint32_t kAlpRdTansMaxBlocks =
    PaxChunk::kRowGroupSize / kAlpRdTansBlockRows;
constexpr size_t kAlpRdTansHeaderBytes = 12;
constexpr size_t kAlpRdTansDescriptorBytes = 64;
constexpr uint32_t kAlpRdTansM = 256;
constexpr uint32_t kAlpRdTansStateBits = 9;
struct AlpRdTansModel {
  std::array<uint8_t, 256> symbols{};
  std::array<uint16_t, 256> frequencies{};
  std::array<uint16_t, 256> cumulative{};
  uint16_t count = 0;
};
struct AlpRdTansBlock {
  const char* descriptor{nullptr};
  const char* stream{nullptr};
  uint32_t rows{0};
  uint8_t flags{0};
  uint8_t leaf_id{0};
  uint32_t leaf_param{0};
  uint32_t raw_len{0};
  uint32_t stream_len{0};
  uint32_t stream_bits{0};
  uint64_t first{0};
  uint64_t minimum{0};
  uint64_t maximum{0};
  uint64_t sum{0};
};
struct AlpRdTansRegion {
  std::array<AlpRdTansBlock, kAlpRdTansMaxBlocks> blocks{};
  AlpRdTansModel model{};
  std::array<uint16_t, 8> fronts{};
  uint32_t n{0};
  uint32_t block_count{0};
  uint32_t right_width{0};
  uint32_t dict_count{0};
  size_t encoded_size{0};
  const char* streams{nullptr};
};
constexpr uint32_t kPatasBlockRows = 128;
constexpr uint32_t kPatasMaxBlocks = PaxChunk::kRowGroupSize / kPatasBlockRows;
constexpr size_t kPatasHeaderBytes = 13;
constexpr size_t kPatasDescriptorBytes = 64;
constexpr uint32_t kPatasMaxAge = 127;
constexpr uint64_t kPatasMaxSpan = 71;
enum PatasMode : uint8_t {
  kPatasRepeat = 0x00,
  kPatasPrev = 0x01,
  kPatasHist = 0x02,
  kPatasWindow = 0x03,
};
struct PatasBlock {
  const char* meta{nullptr};
  const char* meta_end{nullptr};
  const char* payload{nullptr};
  const char* payload_end{nullptr};
  uint32_t rows{0};
  uint8_t flags{0};
  uint64_t first{0};
  uint64_t minimum{0};
  uint64_t maximum{0};
  uint64_t sum{0};
};
struct PatasRegion {
  std::array<PatasBlock, kPatasMaxBlocks> blocks{};
  uint32_t n{0};
  uint32_t block_count{0};
  size_t encoded_size{0};
};
class PatasHistory {
 public:
  void Reset() {
    head_ = 0;
    generation_ = 0;
  }
  void Push(uint64_t value) {
    ++generation_;
    values_[head_] = value;
    stamps_[head_] = generation_;
    head_ = (head_ + 1U) & (kPatasBlockRows - 1U);
  }
  [[nodiscard]] uint64_t At(uint32_t position) const {
    return values_[position & (kPatasBlockRows - 1U)];
  }
  [[nodiscard]] bool Holds(uint32_t position) const {
    return stamps_[position & (kPatasBlockRows - 1U)] == position + 1U;
  }

 private:
  std::array<uint64_t, kPatasBlockRows> values_{};
  std::array<uint32_t, kPatasBlockRows> stamps_{};
  uint32_t head_{0};
  uint32_t generation_{0};
};
constexpr uint32_t kAnsL = 1u << 23;
constexpr uint32_t kAnsPrec = 12;
constexpr uint32_t kAnsCkptRows = 64;
struct AnsForRegion {
  int64_t base = 0;
  uint32_t bpv = 0;
  uint32_t prec = 0;
  uint32_t M = 0;
  uint32_t nsym = 0;
  uint32_t f[256] = {};
  uint32_t cum[256] = {};
  uint32_t ck_sym[256] = {};  // slot-order search keys (cum per entry)
  uint8_t ck_id[256] = {};    // symbol at each entry, ascending cum
  uint32_t ncp = 0;
  const char* ckpt = nullptr;  // ncp x 8B (state u32 BE, pos u32 BE)
  const uint8_t* stream = nullptr;
  uint32_t slen = 0;
};
struct PBitSRegion {
  const char* r{nullptr};
  const char* end{nullptr};
  uint32_t n{0};
  uint32_t width{0};
  uint32_t exceptions{0};
  size_t stride{0};
  const unsigned char* exception_bmp{nullptr};
  const char* exception_values{nullptr};
  const unsigned char* planes{nullptr};
  uint64_t base{0};
};
struct InterpRegion {
  uint32_t n{0};
  uint32_t ncp{0};
  const char* ckpts{nullptr};
  const char* bitoffs{nullptr};
  const char* stream{nullptr};
  const char* stream_end{nullptr};
};
constexpr uint32_t kFastPforBlockRows = 128;
constexpr uint32_t kFastPforMaxBlocks =
    PaxChunk::kRowGroupSize / kFastPforBlockRows;
constexpr size_t kFastPforMetaBytes = 35;
struct FastPforBlock {
  const char* bmp{nullptr};  // ceil(rows/8) bytes, MSB-first, set = exception
  const char* packed{nullptr};  // rows*w bits, MSB-first BE (all rows)
  const char* exc{nullptr};     // nexc x 8B BE full values, row order
  uint32_t rows{0};
  uint32_t w{0};
  uint32_t nexc{0};
  int64_t minimum{0};
  int64_t maximum{0};
  __int128 sum{0};
};
struct FastPforRegion {
  uint32_t n{0};
  uint32_t block_count{0};
  FastPforBlock blocks[kFastPforMaxBlocks];
  size_t encoded_size{0};
};
struct FastPforBitmap {
  uint64_t w[2]{0, 0};
};
constexpr uint32_t kDeltaFastPforBlockRows = 128;
constexpr uint32_t kDeltaFastPforMaxBlocks =
    PaxChunk::kRowGroupSize / kDeltaFastPforBlockRows;
constexpr size_t kDeltaFastPforMetaBytes = 51;
struct DeltaFastPforBlock {
  const char* bmp{
      nullptr};  // ceil((rows-1)/8) B, MSB-first, set = exception delta
  const char* packed{nullptr};  // (rows-1)*w bits, MSB-first BE (all deltas)
  const char* exc{nullptr};     // nexc x 8B BE full deltas, row order
  uint32_t rows{0};
  uint32_t w{0};
  uint32_t nexc{0};
  int64_t base{0};     // value at the block's first row (the replay seed)
  int64_t minimum{0};  // TRUE value brackets over the block, base row included
  int64_t maximum{0};
  __int128 sum{0};
  uint64_t dmin{0};  // minimum delta in SIGNED order, as its uint64 bit pattern
};
struct DeltaFastPforRegion {
  uint32_t n{0};
  uint32_t block_count{0};
  DeltaFastPforBlock blocks[kDeltaFastPforMaxBlocks];
  size_t encoded_size{0};
};
struct DeltaFastPforBitmap {
  uint64_t w[2]{0, 0};
};
struct PlaRegion {
  uint32_t nseg{0};
  const char* segs{nullptr};  // nseg x (first i64 BE, d i64 BE, end u32 BE)
  const char* hdr{nullptr};   // [ischeme u8][iparam u32 BE][nexc u16 BE]
  const char* bmp{nullptr};   // ceil(n/8) bytes MSB-first
};
constexpr uint32_t kSimple8bSelBits[16] = {0, 0, 1,  2,  3,  4,  5,  6,
                                           7, 8, 10, 12, 15, 20, 30, 60};
constexpr uint32_t kSimple8bSelCount[16] = {240, 120, 60, 30, 20, 15, 12, 10,
                                            8,   7,   6,  5,  4,  3,  2,  1};
constexpr uint64_t kSimple8bMaxOffset = (UINT64_C(1) << 60) - 1;
enum class Simple8bVerdict : uint8_t { kNone, kAll, kSome };
struct ModRegion {
  uint64_t a{0};
  uint64_t b{0};
  uint64_t m{0};
  const char* hdr{nullptr};  // [ischeme u8][iparam u32 BE][nexc u16 BE]
  const char* bmp{nullptr};  // ceil(n/8) bytes MSB-first
};
struct LaneRegion {
  uint32_t nd{0};
  const char* base{nullptr};  // nd x (base 8B][step 8B] BE, wrapped
  uint8_t ischeme{0};
  uint32_t iparam{0};
  uint32_t nexc{0};
  const char* bmp{nullptr};  // ceil(n/8) bytes MSB-first, set = on-model
};
constexpr uint32_t kTileDictRows = 64;  // rows per tile
constexpr uint32_t kTileDictMaxD = 64;  // single-u64 pass-mask bound
struct TileDictCursor {
  const char* p{nullptr};      // current tile start
  const char* end{nullptr};    // region end (may be null post-validation)
  uint32_t n{0};               // rows in the whole region
  uint32_t tile_start{0};      // first row of the current tile
  uint32_t d{0};               // distinct count of the current tile (1..64)
  int w{0};                    // code width, BitsNeeded(d-1), 0..6
  uint32_t rows{0};            // rows in the current tile (<= 64)
  const char* dict{nullptr};   // d x 8B BE, strictly ascending
  const char* codes{nullptr};  // rows * w bits MSB-first (empty at w == 0)
  size_t codes_bytes{0};
};
constexpr uint32_t kTemplateMaxSegments = 8;
constexpr uint32_t kTemplateMaxWidth = 18;
struct TemplateRegion {
  uint32_t n{0};
  uint32_t nseg{0};
  uint32_t nexc{0};
  uint32_t widths[kTemplateMaxSegments]{};
  // Literals: base pointers into the region (not NUL-terminated).
  const char* lit[kTemplateMaxSegments + 1]{};
  uint32_t lit_len[kTemplateMaxSegments + 1]{};
  const uint8_t* bmp{nullptr};
  // Per digit segment: the self-contained inner region and its end.
  const char* seg[kTemplateMaxSegments]{};
  Scheme seg_scheme[kTemplateMaxSegments]{};
  uint32_t seg_param[kTemplateMaxSegments]{};
  const char* seg_end[kTemplateMaxSegments]{};
  const char* exc{nullptr};  // first exception entry
  uint32_t nconf{0};         // == n - nexc
};
constexpr uint32_t kDStreamRestart = 16;      // rows per restart-table entry
constexpr uint32_t kDStreamMaxSymbols = 255;  // code 0 is the escape
constexpr uint32_t kDStreamMaxSymbolLen = 8;
struct DStreamRegion {
  uint32_t n{0};
  uint32_t nexc{0};
  uint32_t nconf{0};  // == n - nexc
  uint32_t nsym{0};
  uint32_t ndir{0};
  uint32_t stream_len{0};
  // Symbol table: code c (1-based) is sym[c - 1] for sym_len[c - 1] bytes.
  const char* sym[kDStreamMaxSymbols]{};
  uint8_t sym_len[kDStreamMaxSymbols]{};
  const uint8_t* bmp{nullptr};
  const char* dir{nullptr};     // ndir x u32 BE stream offsets
  const char* stream{nullptr};  // nconf x (u8 clen + clen code bytes)
  const char* exc{nullptr};     // first exception entry
};
struct EncodedCol {
  std::vector<char> buf;
  Scheme scheme{Scheme::kPlain};
  uint32_t param{0};
};
TINYLAMB_ALWAYS_INLINE void AppendI64(std::vector<char>& buf, int64_t v) {
  AppendBE64(buf, static_cast<uint64_t>(v));
}
struct RgColumnProfile {
  ValueType type = ValueType::kNull;
  bool is_i64 = false;
  bool is_double = false;
  // Encode domain (numeric columns): cell raw, null -> 0.
  std::vector<int64_t> vals;
  std::vector<uint64_t> ubits;
  int64_t emin = INT64_MAX, emax = INT64_MIN;
  bool mono = true;
  FlatMapI64 distinct{PaxChunk::kRowGroupSize};
  // Null bitmap (all columns), MSB-first.
  std::vector<uint8_t> null_bmp;
  bool has_null = false;
  // Stats domain accumulators.
  uint64_t count = 0;
  int64_t sum = 0;
  int64_t mn = INT64_MAX, mx = INT64_MIN;
  uint64_t bloom = 0;
  uint64_t null_count = 0;
  int64_t cmin = INT64_MAX, cmax = INT64_MIN;
  uint8_t hll[64] = {};
  // Inverted postings (int64 columns), CSR over the <= kMaxIdxDistinct key
  // set: row ids per key in one flat array with per-key offsets. The
  // historical per-key unordered_map paid a hash-bucket walk per CELL and a
  // node+vector pair per distinct key per row group; the CSR fills both from
  // one row sweep (counts already live in `xorset`).
  struct PostingsCsr {
    int64_t keys[kMaxIdxDistinct] = {};
    uint32_t offsets[kMaxIdxDistinct + 1] = {};
    std::vector<uint16_t> rows;
    size_t key_count = 0;
    void Clear() { key_count = 0; }
  };
  PostingsCsr postings;
  bool postings_big = false;
  // Xor key set (int64 columns).
  FlatMapI64 xorset{PaxChunk::kRowGroupSize};
  // Key-column cell hygiene: false once a non-null, non-int64 cell appears
  // (the historical hash-index loop throws on such cells; the fused array
  // loop runs only while this stays true).
  bool typed_clean = true;

  // Restore the just-constructed state in place. BuildFromSortedRun keeps one
  // profile vector for the whole chunk and calls this per row group, so the
  // two flat hash tables and the value vectors are allocated once instead of
  // once per group.
  void Reset() {
    type = ValueType::kNull;
    is_i64 = false;
    is_double = false;
    // vals/ubits deliberately keep their storage AND size: the sweep sizes
    // them to the row-group width and rewrites every slot, so resize() after
    // the first group is a no-op instead of an 8 KiB zero-fill per column
    // per group (a fresh RG would otherwise re-zero through resize's
    // value-initialization on every group after the clear).
    emin = INT64_MAX;
    emax = INT64_MIN;
    mono = true;
    distinct.Reset();
    null_bmp.clear();
    has_null = false;
    count = 0;
    sum = 0;
    mn = INT64_MAX;
    mx = INT64_MIN;
    bloom = 0;
    null_count = 0;
    cmin = INT64_MAX;
    cmax = INT64_MIN;
    std::memset(hll, 0, sizeof(hll));
    // The inverted-index builder copies each postings run out into the
    // footer cell, so the key count must drop too: a stale key range from
    // the previous row group would publish a value this group never had.
    // rows keeps its capacity (rewritten in full by the next CSR pass).
    postings.Clear();
    postings_big = false;
    xorset.Reset();
    typed_clean = true;
  }
};
struct ModeFrontEntry {
  int64_t key = 0;
  uint64_t count = 0;   // authoritative total for `key` while cached
  uint64_t synced = 0;  // portion of `count` already reflected in the map
  uint64_t hash = 0;    // XorMix64(key): the eviction fold's probe reuses it
  uint32_t in_fm = 0;   // 0: the key was never inserted (map was at its cap)
};
constexpr size_t kModeFrontBits = 10;  // 1024 entries x 24B per column
constexpr size_t kModeFrontSize = size_t{1} << kModeFrontBits;
struct RgCells {
  std::vector<std::vector<uint64_t>> payload;  // [col][row] raw cell word
  std::vector<std::vector<uint8_t>> types;     // [col][row] ValueType byte
  size_t n = 0;  // live rows of the row group being built
};
struct AdaptiveEnv {
  bool force_plain;
  bool no_new_schemes;
  bool no_inv_idx;
  const char* forced_scheme;  // null when TINYLAMB_FORCE_SCHEME is unset
};
inline bool DictPassByteExpandable(uint32_t cw) {
  return cw == 1 || cw == 2 || cw == 4;
}
template <typename Fn>
void DictSelectedCodeFold(const char* region, uint32_t n, uint32_t cw,
                          uint16_t ncodes, const uint64_t* sel_bits, Fn emit) {
  if (DictPassByteExpandable(cw)) {
    // Generic (runtime per_byte) on purpose: see the note on
    // DictSelectedCodeHistogramRun for why the MIN/MAX fold is not
    // specialised the way the SUM histogram is.
    const uint32_t per_byte = 8u / cw;
    const uint32_t code_mask = (1u << cw) - 1u;
    const uint8_t* src = reinterpret_cast<const uint8_t*>(region) + 2;
    for (uint32_t base = 0; base < n; base += 64) {
      const uint32_t take = (n - base) < 64 ? (n - base) : 64;
      const uint64_t sel_word = sel_bits[base >> 6];
      for (uint32_t j = 0; j < take; ++j) {
        if (((sel_word >> j) & 1ULL) == 0) continue;
        // 64 is a multiple of every per_byte in {1,2,4,8}, so a full block
        // never straddles a byte. The final block may: the last byte is
        // shared by its few remaining codes, and the same shift expression
        // yields the partial final slice without reading past the region.
        const uint32_t byte = src[base / per_byte + j / per_byte];
        const uint32_t code =
            (byte >> (8u - cw * ((j % per_byte) + 1u))) & code_mask;
        emit(base + j, code < ncodes ? code : static_cast<uint32_t>(ncodes));
      }
    }
    return;
  }
  WordBitReader br(region + 2, region + 2 +
                                   static_cast<ptrdiff_t>(
                                       (static_cast<size_t>(n) * cw + 7) / 8));
  for (uint32_t base = 0; base < n; base += 64) {
    const uint32_t take = (n - base) < 64 ? (n - base) : 64;
    const uint64_t sel_word = sel_bits[base >> 6];
    for (uint32_t j = 0; j < take; ++j) {
      const uint32_t code = static_cast<uint32_t>(br.get(static_cast<int>(cw)));
      if (((sel_word >> j) & 1ULL) == 0) continue;
      emit(base + j, code < ncodes ? code : static_cast<uint32_t>(ncodes));
    }
  }
}

template <uint32_t kPerByte>
inline void DictSelectedCodeHistogramRun(const uint8_t* src, uint32_t n,
                                         uint16_t ncodes,
                                         const uint64_t* sel_bits,
                                         uint64_t* freq) {
  constexpr uint32_t kCw = 8u / kPerByte;
  constexpr uint32_t kCodeMask = (1u << kCw) - 1u;
  for (uint32_t base = 0; base < n; base += 64) {
    const uint32_t take = (n - base) < 64 ? (n - base) : 64;
    const uint64_t sel_word = sel_bits[base >> 6];
    for (uint32_t j = 0; j < take; ++j) {
      if (((sel_word >> j) & 1ULL) == 0) continue;
      const uint32_t byte = src[(base + j) / kPerByte];
      const uint32_t code =
          (byte >> (8u - kCw * ((j % kPerByte) + 1u))) & kCodeMask;
      ++freq[code < ncodes ? code : static_cast<uint32_t>(ncodes)];
    }
  }
}

bool ParseRleBpRegion(const char* r, uint32_t n, uint32_t param,
                      const char* end, RleBpRegion* out);
int64_t RleBpValueAt(const RleBpRegion& p, uint32_t run);
uint32_t RleBpLengthAt(const RleBpRegion& p, uint32_t run);
uint64_t RleIdxForBaOff(const char* p, uint32_t bpv);
bool ParseRleIdxRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, RleIdxRegion* out);
int64_t RleIdxValueAt(const RleIdxRegion& p, uint32_t run);
uint32_t RleIdxLengthAt(const RleIdxRegion& p, uint32_t run);
uint32_t RleIdxRowBefore(const RleIdxRegion& p, uint32_t run);
uint32_t RleIdxRunAt(const RleIdxRegion& p, uint32_t row);
uint32_t RleIdxLowerBound(const RleIdxRegion& p, int64_t key);
uint32_t RleIdxUpperBound(const RleIdxRegion& p, int64_t key);
bool RleIdxPassingWindow(const RleIdxRegion& p, PaxChunk::CompareOp op,
                         int64_t lit, uint32_t* first, uint32_t* last);
uint64_t StreamVByteZigZag(int64_t v);
int64_t StreamVByteUnZigZag(uint64_t z);
uint32_t StreamVByteWidth(const StreamVByteRegion& v, uint32_t i);
int64_t StreamVByteLoad(const StreamVByteRegion& v, uint32_t off, uint32_t w);
uint32_t StreamVByteOffset(const StreamVByteRegion& v, uint32_t i);
int64_t StreamVByteValueAt(const StreamVByteRegion& v, uint32_t i);
bool ParseStreamVByteRegion(const char* r, uint32_t n, uint32_t param,
                            const char* end, StreamVByteRegion* out);
void DecodeStreamVByteAll(const StreamVByteRegion& v, int64_t* out);
uint32_t StreamVByteLowerBound(const StreamVByteRegion& v, int64_t key);
bool ParseRle8Region(const char* r, uint32_t n, uint32_t param, const char* end,
                     Rle8Region* out);
bool ValidateRle8Region(const char* r, uint32_t n, uint32_t param,
                        const char* end, std::array<const char*, 8>* plane_data,
                        std::array<uint32_t, 8>* plane_counts,
                        uint32_t* total_runs);
uint32_t ReadRle8Length(const char* run);
int64_t Rle8ValueAt(const Rle8Region& p, uint32_t index);
int64_t Rle8ValueAtParsed(const char* r, uint32_t n, uint32_t param,
                          const char* region_end, uint32_t idx);
bool Rle8RegionLowerBound(const char* r, uint32_t n, uint32_t param,
                          const char* region_end, int64_t key, uint32_t* pos);
uint64_t Rle8Compose(const std::array<uint8_t, 8>& values);
void Rle8TransposeStore(const std::array<uint64_t, 8>& plane_rows,
                        int64_t* out);
bool DecodeRle8All(const char* r, uint32_t n, uint32_t param, const char* end,
                   int64_t* out);
bool DecodeByteSliceAll(const char* region, uint32_t n, const char* end,
                        int64_t* out);
uint64_t EncodeZigZag64(uint64_t value);
uint64_t DecodeZigZag64(uint64_t value);
bool ParseRleDeltaRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, RleDeltaRegion* out);
int64_t RleDeltaValueAt(const RleDeltaRegion& region, uint32_t index);
bool DecodeRleDeltaAll(const char* r, uint32_t n, uint32_t param,
                       const char* end, int64_t* out);
int64_t D2PforSecondDiff(const D2PforRegion& p, uint32_t k);
bool ParseD2PforRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, D2PforRegion* out);
int64_t D2PforValueAt(const D2PforRegion& p, uint32_t idx);
void DecodeD2PforAll(const D2PforRegion& p, int64_t* out);
uint32_t D2PforLowerBound(const D2PforRegion& p, int64_t key);
bool SimdForWidthOk(uint32_t w);
uint32_t SimdForPerLane(uint32_t w);
uint32_t SimdForGroupValues(uint32_t w);
uint64_t SimdForGroups(uint32_t n, uint32_t w);
uint64_t SimdForExtent(uint32_t n, uint32_t w);
uint64_t SimdForMask(uint32_t w);
bool ParseSimdForRegion(const char* r, uint32_t n, uint32_t param,
                        const char* end, SimdForRegion* out);
int64_t SimdForValueAt(const SimdForRegion& p, uint32_t idx);
void DecodeSimdForAll(const SimdForRegion& p, int64_t* out);
uint32_t SimdForLowerBound(const SimdForRegion& p, int64_t key);
uint8_t Rle8ReverseByte(uint8_t value);
uint64_t WahLiteralBitsLE(const char* wp);
bool ParseDeltaPForRegion(const char* region, uint32_t n, uint32_t param,
                          const char* end, DeltaPForRegion* out);
uint32_t DeltaPForExceptionAtOrAfter(const DeltaPForRegion& p, uint32_t pos);
size_t CountRuns(const std::vector<int64_t>& vals);
uint64_t BitRev64(uint64_t x);
uint64_t SelectFromWord(uint64_t word, uint64_t k);
uint64_t HistogramBin(int64_t v, int64_t mn, int64_t mx, uint64_t B);
uint32_t PopcountBits(const char* b, uint32_t p);
uint32_t TopKSentinelCount(uint64_t w);
uint32_t WaveletRRankCount(uint32_t n);
bool ParseWaveletRRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, WaveletRRegion* out);
uint32_t WaveletRRankOne(const WaveletRRegion& p, uint32_t level,
                         uint32_t position);
uint64_t WaveletRValueAt(const WaveletRRegion& p, uint32_t index);
bool WaveletRNormalizedRange(const WaveletRRegion& p, PaxChunk::CompareOp op,
                             int64_t literal, uint64_t* low_out,
                             uint64_t* high_out);
uint64_t WaveletRPrefixCount(const WaveletRRegion& p, uint64_t bound,
                             bool inclusive);
uint64_t WaveletRSumRange(const WaveletRRegion& p, uint32_t level,
                          uint32_t left, uint32_t right);
uint64_t WaveletRRangeSum(const WaveletRRegion& p, uint32_t level,
                          uint32_t left, uint32_t right, uint64_t prefix,
                          uint32_t remaining, uint64_t low, uint64_t high);
bool WaveletRRegionFold(const WaveletRRegion& p, PaxChunk::CompareOp op,
                        int64_t literal, uint64_t* count_out, int64_t* sum_out);
__int128 QppModelValue(const QppPage& p, uint32_t index);
bool ParseQppRegion(const char* region, uint32_t n, uint32_t param,
                    const char* end, QppRegion* out);
int64_t QppValueAt(const QppRegion& region, uint32_t index);
bool QppMatch(PaxChunk::CompareOp op, int64_t literal, int64_t value);
size_t QppSegments(const QppPage& page, std::array<QppSegment, 2>& segments);
uint32_t QppFirstGe(const QppPage& page, const QppSegment& segment,
                    __int128 target);
uint32_t QppFirstGt(const QppPage& page, const QppSegment& segment,
                    __int128 target);
__int128 QppChoose3(uint32_t value);
__int128 QppModelSum(const QppPage& page, uint32_t begin, uint32_t end);
uint32_t QppFirstLe(const QppPage& page, const QppSegment& segment,
                    __int128 target);
uint32_t QppFirstLt(const QppPage& page, const QppSegment& segment,
                    __int128 target);
size_t QppModelRanges(const QppPage& page, __int128 low, __int128 high,
                      std::array<QppRange, 2>& ranges);
void QppPatchExceptionCount(const QppRegion& region, PaxChunk::CompareOp op,
                            int64_t literal, __int128 low, __int128 high,
                            int64_t* count, __int128* sum);
bool QppScanMatch(int64_t value, ScanOp op, int64_t literal);
void QppSetMaskRange(uint64_t* out, uint32_t begin, uint32_t end);
__int128 QppRegionSumAll(const QppRegion& region);
void QppFillRangeMask(const QppRegion& region, ScanOp op, int64_t literal,
                      uint64_t* out);
void DecodeQppAll(const QppRegion& region, int64_t* out);
uint32_t HuffDictLenAtRaw(const uint8_t* lens, uint32_t s);
uint32_t HuffDictLenAt(const HuffDictRegion& r, uint32_t s);
int64_t HuffDictDictAt(const HuffDictRegion& r, uint32_t s);
uint32_t HuffDictFreqAt(const HuffDictRegion& r, uint32_t s);
void HuffDictBuildTables(const HuffDictRegion& r, HuffDictTables* t);
uint32_t HuffDictDecodeSym(const HuffDictTables& t, WordBitReader& br);
bool ParseHuffDictRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, HuffDictRegion* out);
int64_t HuffDictValueAt(const HuffDictRegion& v, uint32_t i);
void DecodeHuffDictAll(const HuffDictRegion& v, int64_t* out);
uint64_t S4HeaderBytes(uint32_t n);
uint32_t S4BlockRows(const S4Region& p, uint32_t b);
int S4Width(const S4Region& p, uint32_t b);
int64_t S4Base(const S4Region& p, uint32_t b);
uint64_t S4BlockOffset(const S4Region& p, uint32_t b);
uint64_t S4Extent(const S4Region& p);
int64_t S4Checkpoint(const S4Region& p, uint32_t i);
bool ParseS4Region(const char* r, uint32_t n, uint32_t param, const char* end,
                   S4Region* out);
int64_t S4ValueAt(const S4Region& p, uint32_t i);
uint64_t SforMsb(const SforRegion& p, uint32_t g);
uint64_t SforBase(const SforRegion& p, uint32_t g);
uint32_t SforRowStart(const SforRegion& p, uint32_t g);
uint32_t SforWidth(const SforRegion& p, uint32_t g);
uint32_t SforGroupRows(const SforRegion& p, uint32_t g);
uint64_t SforGroupOffset(const SforRegion& p, uint32_t g);
bool ParseSforRegion(const char* r, uint32_t n, uint32_t param, const char* end,
                     SforRegion* out);
uint64_t SforDeltaAt(const SforRegion& p, uint32_t g, uint32_t t,
                     uint64_t boff);
uint32_t SforGroupLowerBound(const SforRegion& p, uint32_t g, uint64_t boff,
                             __int128 x);
int64_t SforValueAt(const SforRegion& p, uint32_t i);
void DecodeSforAll(const SforRegion& p, int64_t* out);
void DecodeS4All(const S4Region& p, int64_t* out);
uint64_t ExpForBitsToKey(uint64_t bits);
uint64_t ExpForKeyToBits(uint64_t key);
uint64_t ExpForGKey(const ExpForRegion& p, uint32_t g);
uint64_t ExpForBase(const ExpForRegion& p, uint32_t g);
uint32_t ExpForRowStart(const ExpForRegion& p, uint32_t g);
uint32_t ExpForWidth(const ExpForRegion& p, uint32_t g);
uint32_t ExpForGroupRows(const ExpForRegion& p, uint32_t g);
uint64_t ExpForGroupOffset(const ExpForRegion& p, uint32_t g);
bool ParseExpForRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, ExpForRegion* out);
uint64_t ExpForOffsetAt(const ExpForRegion& p, uint32_t g, uint32_t t,
                        uint64_t boff);
int64_t ExpForValueAt(const ExpForRegion& p, uint32_t i);
void DecodeExpForAll(const ExpForRegion& p, int64_t* out);
uint64_t SortDictHeaderBytes(uint32_t n);
uint32_t SortDictBlockRows(const SortDictRegion& p, uint32_t blk);
int SortDictWidth(const SortDictRegion& p, uint32_t blk);
int64_t SortDictFirst(const SortDictRegion& p, uint32_t blk);
uint64_t SortDictPsum(const SortDictRegion& p, uint32_t blk);
uint64_t SortDictGapOffset(const SortDictRegion& p, uint32_t blk);
uint64_t SortDictExtent(const SortDictRegion& p);
bool ParseSortDictRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, SortDictRegion* out);
int64_t SortDictValueAtSorted(const SortDictRegion& p, uint32_t j);
uint32_t SortDictRankAt(const SortDictRegion& p, uint32_t row);
uint32_t SortDictLowerBoundSorted(const SortDictRegion& p, int64_t key);
uint32_t SortDictUpperBoundSorted(const SortDictRegion& p, int64_t key);
bool SortDictPredicateWindow(const SortDictRegion& p, PaxChunk::CompareOp op,
                             int64_t lit, uint32_t* wlo, uint32_t* whi);
uint64_t PostingsHeaderBytes(uint32_t d);
uint32_t PostingsCountAt(const PostingsRegion& p, uint32_t c);
uint32_t PostingsFirstAt(const PostingsRegion& p, uint32_t c);
uint32_t PostingsBitOffAt(const PostingsRegion& p, uint32_t c);
uint32_t PostingsKAt(const PostingsRegion& p, uint32_t c);
int64_t PostingsDictAt(const PostingsRegion& p, uint32_t c);
bool ParsePostingsRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, PostingsRegion* out);
bool PostingsNextRow(const PostingsRegion& p, PostingsBitCursor* cur,
                     uint32_t c, uint32_t row, uint32_t* next);
void DecodePostingsAll(const PostingsRegion& p, int64_t* out);
uint32_t PostingsLowerBoundList(const PostingsRegion& p, int64_t key);
uint32_t PostingsUpperBoundList(const PostingsRegion& p, int64_t key);
bool PostingsRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out, int64_t* sum_out);
double Chimp128Double(uint64_t bits);
uint64_t Chimp128Bits(double value);
uint64_t Chimp128CanonicalBits(double value);
bool ParseChimp128Region(const char* region, uint32_t n, uint32_t param,
                         const char* end, Chimp128Region* out);
bool DecodeChimp128Block(const Chimp128Block& block, uint64_t* output);
bool Chimp128MetadataMatches(const Chimp128Region& region);
uint64_t AlpRdTansBits(double value);
double AlpRdTansDouble(uint64_t bits);
uint64_t AlpRdTansCanonicalBits(double value);
bool AlpRdTansComputeMetadata(const uint64_t* values, uint32_t rows,
                              uint64_t* first, uint64_t* minimum,
                              uint64_t* maximum, uint64_t* sum, bool* numeric);
std::vector<char> AlpRdTansPackBits(const std::vector<uint8_t>& bits);
bool AlpRdTansBuildModel(const std::vector<std::vector<uint8_t>>& payloads,
                         AlpRdTansModel* model);
bool AlpRdTansEncode(const std::vector<uint8_t>& payload,
                     const AlpRdTansModel& model, std::vector<char>* stream,
                     uint32_t* stream_bits);
bool AlpRdTansDecode(const char* stream, size_t stream_len,
                     uint32_t stream_bits, const AlpRdTansModel& model,
                     size_t max_output, std::vector<uint8_t>* output);
bool AlpRdTansDecodePfor(const char* leaf, size_t leaf_len, uint32_t raw_len,
                         uint32_t width, std::vector<int64_t>* values);
bool AlpRdTansDecodeBp128(const char* leaf, size_t leaf_len, uint32_t raw_len,
                          uint32_t leaf_param, std::vector<int64_t>* values);
bool AlpRdTansDecodeBlock(const AlpRdTansRegion& region, uint32_t block,
                          std::vector<uint64_t>* values);
bool ParseAlpRdTansRegion(const char* region, uint32_t n, uint32_t param,
                          const char* end, AlpRdTansRegion* out);
bool AlpRdTansMetadataMatches(const AlpRdTansRegion& region);
double PatasDouble(uint64_t bits);
uint64_t PatasCanonicalBits(double value);
uint32_t PatasTrailingZeros(uint64_t value);
uint32_t PatasPayloadBytes(uint64_t value);
bool PatasComputeMetadata(const uint64_t* values, uint32_t rows,
                          uint64_t* first, uint64_t* minimum, uint64_t* maximum,
                          uint64_t* sum, bool* numeric);
bool PatasReadXor(const char*& cursor, const char* end, uint32_t length,
                  uint32_t tz, uint64_t* out);
bool ParsePatasRegion(const char* region, uint32_t n, uint32_t param,
                      const char* end, PatasRegion* out);
bool DecodePatasBlock(const PatasBlock& block, uint64_t* output);
bool PatasMetadataMatches(const PatasRegion& region);
int64_t DeltaPForValueAt(const DeltaPForRegion& p, uint32_t idx);
void DecodeDeltaPForBlock(const DeltaPForRegion& p, uint32_t block,
                          int64_t* out);
void DecodeDeltaPForAll(const DeltaPForRegion& p, int64_t* out);
double AlpFactorOf(int exp);
bool AnsForParse(const char* r, const char* region_end, uint32_t n,
                 uint32_t param, AnsForRegion* A);
bool AnsForStep(const AnsForRegion& A, uint32_t* x, uint32_t* pos,
                uint8_t* sym_out);
bool InnerSchemeOk(uint8_t s);
bool ParseBp128Region(const char* region, uint32_t n, uint32_t param,
                      const char* end, Bp128Region* out);
__int128 Bp128BlockSum(const Bp128Block& block);
int64_t Bp128ValueAt(const Bp128Region& region, uint32_t index);
void DecodeBp128Block64(const Bp128Block& decoded, int64_t* output);
void DecodeBp128Block(const Bp128Region& region, uint32_t block,
                      int64_t* output);
void DecodeBp128All(const Bp128Region& region, int64_t* output);
bool Bp128MetadataMatches(const Bp128Region& region);
FastPforBitmap FastPforBitmapLoad(const FastPforBlock& blk);
bool FastPforBitIsException(const FastPforBitmap& bmp, uint32_t t);
uint32_t FastPforExceptionRank(const FastPforBitmap& bmp, uint32_t t);
uint64_t FastPforWalk(const char* r, uint32_t n, uint32_t nblk, const char* end,
                      FastPforRegion* out);
bool ParseFastPforRegion(const char* r, uint32_t n, uint32_t param,
                         const char* end, FastPforRegion* out);
int64_t FastPforValueAt(const FastPforRegion& p, uint32_t idx);
void FastPforDecodeBlock(const FastPforBlock& blk, int64_t* dst);
void DecodeFastPforAll(const FastPforRegion& p, int64_t* out);
bool FastPforMetadataMatches(const FastPforRegion& p);
DeltaFastPforBitmap DeltaFastPforBitmapLoad(const DeltaFastPforBlock& blk);
bool DeltaFastPforBitIsException(const DeltaFastPforBitmap& bmp, uint32_t t);
uint32_t DeltaFastPforExceptionRank(const DeltaFastPforBitmap& bmp, uint32_t t);
uint64_t DeltaFastPforWalk(const char* r, uint32_t n, uint32_t nblk,
                           const char* end, DeltaFastPforRegion* out);
bool ParseDeltaFastPforRegion(const char* r, uint32_t n, uint32_t param,
                              const char* end, DeltaFastPforRegion* out);
void DeltaFastPforDecodeBlock(const DeltaFastPforBlock& blk, int64_t* dst);
void DecodeDeltaFastPforAll(const DeltaFastPforRegion& p, int64_t* out);
int64_t DeltaFastPforValueAt(const DeltaFastPforRegion& p, uint32_t idx);
bool DeltaFastPforMetadataMatches(const DeltaFastPforRegion& p);
int64_t Int64AtPlain(const char* r, uint32_t, uint32_t, uint32_t idx,
                     const char* region_end, const uint32_t*);
int64_t Int64AtConst(const char* r, uint32_t, uint32_t, uint32_t,
                     const char* region_end, const uint32_t*);
int64_t Int64AtForBp(const char* r, uint32_t param, uint32_t, uint32_t idx,
                     const char* region_end, const uint32_t*);
int64_t Int64AtBitWeave(const char* r, uint32_t, uint32_t, uint32_t idx,
                        const char* region_end, const uint32_t*);
int64_t Int64AtEliasFano(const char* r, uint32_t, uint32_t n, uint32_t idx,
                         const char* region_end, const uint32_t* ef_dir);
bool ParsePlaRegion(const char* r, uint32_t n, uint32_t param,
                    const char* region_end, PlaRegion* out);
int64_t PlaSegmentValueAt(const PlaRegion& p, uint32_t j, uint32_t start,
                          uint32_t idx);
bool Simple8bPasses(int64_t v, ScanOp op, int64_t lit);
Simple8bVerdict Simple8bWordVerdict(uint32_t bits, uint64_t base_u, int64_t lit,
                                    ScanOp op);
int64_t Int64AtArith(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                     const char* region_end, const uint32_t*);
int64_t Int64AtPla(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                   const char* region_end, const uint32_t*);
bool ParseModRegion(const char* r, uint32_t n, uint32_t param,
                    const char* region_end, ModRegion* out);
int64_t ModValueAt(const ModRegion& p, uint32_t idx);
unsigned __int128 ModFloorSum(unsigned __int128 n, unsigned __int128 a,
                              unsigned __int128 b, unsigned __int128 m);
uint64_t ModGcd(uint64_t x, uint64_t y);
uint64_t ModInv(uint64_t u, uint64_t q);
int64_t Int64AtMod(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                   const char* region_end, const uint32_t*);
size_t LaneHeaderBytes(uint32_t nd);
bool ParseLaneRegion(const char* r, uint32_t n, uint32_t param,
                     const char* region_end, LaneRegion* out);
int64_t LaneValueAt(const LaneRegion& p, uint32_t idx);
int64_t Int64AtLane(const char* r, uint32_t param, uint32_t n, uint32_t idx,
                    const char* region_end, const uint32_t*);
int TileDictWidth(uint32_t d);
bool TileDictParseTile(const char* p, const char* end, uint32_t n,
                       uint32_t tile_start, TileDictCursor* c);
bool TileDictFirst(const char* r, uint32_t n, const char* end,
                   TileDictCursor* c);
bool TileDictNext(TileDictCursor* c);
bool TileDictSeek(const char* r, uint32_t n, const char* end, uint32_t idx,
                  TileDictCursor* c);
int64_t TileDictValueAt(const TileDictCursor& c, uint32_t local_row);
int64_t Int64AtFast(const char* r, Scheme s, uint32_t param, uint32_t n,
                    uint32_t idx, const char* region_end,
                    const uint32_t* ef_dir);
int64_t Int64RegionAt(const char* r, Scheme s, uint32_t param, uint32_t n,
                      uint32_t idx, const char* region_end,
                      const uint32_t* ef_dir = nullptr);
bool ParsePBitSRegion(const char* r, uint32_t n, uint32_t param,
                      const char* end, PBitSRegion* out);
uint64_t PBitSOrderValue(int64_t value);
int64_t PBitSValueAt(const PBitSRegion& p, uint32_t idx);
void DecodePBitSAll(const PBitSRegion& p, int64_t* out);
bool ParseInterpRegion(const char* r, uint32_t n, uint32_t param,
                       const char* end, InterpRegion* out);
void InterpDecodeBlock(const InterpRegion& p, uint32_t b, int64_t* out);
void DecodeInterpAll(const InterpRegion& p, int64_t* out);
bool InterpWindowForValue(PaxChunk::CompareOp op, int64_t lit, __int128* plo,
                          __int128* phi);
void InterpEvalBlock(const InterpRegion& p, uint32_t b, __int128 plo,
                     __int128 phi, uint64_t* bits, bool want_sum,
                     uint64_t* cnt_out, __int128* sum_out);
bool PBitSOrderedMatch(PaxChunk::CompareOp op, uint64_t value,
                       uint64_t literal);
bool CellIsNull(const PaxChunk::Footer& f, uint32_t g, uint32_t c,
                uint32_t local);
bool RgColHasNull(const PaxChunk::Footer& f, uint32_t g, uint32_t c);
const uint8_t* RgColNullBits(const PaxChunk::Footer& f, uint32_t g, uint32_t c);
const char* RegionEndAt(const PaxChunk::Footer& f, uint32_t g, uint32_t col,
                        const std::vector<char>& data);
bool DecodeInnerI64All(const char* r, uint8_t scheme, uint32_t param,
                       uint32_t cnt, const char* end, int64_t* out);
bool DecodeCascadeI64All(const char* region, Scheme s, uint32_t param,
                         uint32_t n, const char* region_end, int64_t* out);
const char* RegionStartAt(const PaxChunk::Footer& f, uint32_t g, uint32_t col,
                          const std::vector<char>& data, size_t need_bytes);
uint64_t SelPopcountRange(const PaxSelectionVector& sel, uint32_t start,
                          uint32_t end);
uint64_t SelNonNullCount(const PaxSelectionVector& sel, const uint8_t* nulls,
                         uint32_t start, uint32_t end);
Value VarcharRegionAtFsstFallback(const char* r, uint32_t n, uint32_t idx,
                                  const char* region_end);
bool DecodeFrontCodeAtInto(const char* r, uint32_t param, uint32_t n,
                           uint32_t idx, const char* region_end,
                           std::string* out);
bool ParseTemplateRegion(const char* r, uint32_t n, uint32_t param,
                         const char* region_end, TemplateRegion* out);
bool TemplateConforms(const TemplateRegion& t, uint32_t idx);
uint32_t TemplateSetBefore(const TemplateRegion& t, uint32_t idx);
bool TemplateSegmentBulk(const char* r, Scheme s, uint32_t param, uint32_t cnt,
                         const char* end, int64_t* out);
bool TemplateValueAt(const TemplateRegion& t, uint32_t idx, std::string* out);
bool ParseDStreamRegion(const char* r, uint32_t n, uint32_t param,
                        const char* region_end, DStreamRegion* out);
bool DStreamConforms(const DStreamRegion& t, uint32_t idx);
uint32_t DStreamSetBefore(const DStreamRegion& t, uint32_t idx);
bool DStreamRecordAt(const DStreamRegion& t, uint32_t rank, const char** codes,
                     uint32_t* clen);
bool DStreamValueAt(const DStreamRegion& t, uint32_t idx, std::string* out);
bool DStreamEncodeNeedle(const DStreamRegion& t, std::string_view s,
                         std::string* out);
Value VarcharRegionAt(const char* r, Scheme s, uint32_t param, uint32_t n,
                      uint32_t idx, const char* region_end,
                      const FsstDecodeView* fsst_view = nullptr);
bool RegionLowerBound(const char* r, Scheme s, uint32_t param, uint32_t n,
                      int64_t key, uint32_t* pos, const char* region_end,
                      const uint32_t* ef_dir = nullptr);
bool ForcePlain();
EncodedCol EncConstI64(int64_t v);
EncodedCol EncRleI64(const std::vector<int64_t>& vals,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncRleBpI64(const std::vector<int64_t>& vals,
                       size_t budget = kNoEncodeBudget, int known_distinct = 0);
void AppendForBaStream(std::vector<char>& buf, uint64_t base,
                       const std::vector<uint64_t>& offs, uint32_t bpv);
uint32_t ForBaStreamWidth(uint64_t max_offset);
EncodedCol EncRleIdxI64(const std::vector<int64_t>& vals,
                        size_t budget = kNoEncodeBudget);
EncodedCol EncStreamVByteI64(const std::vector<int64_t>& vals,
                             size_t budget = kNoEncodeBudget);
EncodedCol EncRle8I64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
bool Rle8AdaptiveApplicable(const std::vector<int64_t>& vals,
                            const EncodedCol& encoded);
EncodedCol EncRleDeltaI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
bool RleDeltaAdaptiveApplicable(const std::vector<int64_t>& vals,
                                const EncodedCol& encoded);
EncodedCol EncForBA(const std::vector<int64_t>& vals,
                    size_t budget = kNoEncodeBudget);
EncodedCol EncDictI64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget, int known_distinct = 0);
EncodedCol EncOpDictI64(const std::vector<int64_t>& vals,
                        size_t budget = kNoEncodeBudget,
                        int known_distinct = 0);
bool HuffDictCodeLengths(const std::vector<uint32_t>& freq,
                         std::vector<uint8_t>* lens);
EncodedCol EncHuffDictI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget,
                          int known_distinct = 0);
EncodedCol EncRleDictI64(const std::vector<int64_t>& vals,
                         size_t budget = kNoEncodeBudget,
                         int known_distinct = 0);
EncodedCol EncOpRleI64(const std::vector<int64_t>& vals,
                       size_t budget = kNoEncodeBudget, int known_distinct = 0);
EncodedCol EncDeltaFp(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncBitmap(const std::vector<int64_t>& vals,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncWahI64(const std::vector<int64_t>& vals,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncByteSliceI64(const std::vector<int64_t>& vals,
                           size_t budget = kNoEncodeBudget);
EncodedCol EncPBitSI64(const std::vector<int64_t>& vals,
                       size_t budget = kNoEncodeBudget,
                       bool sorted_hint = false);
EncodedCol EncXorF64(const std::vector<uint64_t>& bits,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncGorillaF64(const std::vector<uint64_t>& bits);
EncodedCol EncOpDictStr(const std::vector<std::string_view>& sv);
EncodedCol EncFsstStr(const std::vector<std::string_view>& sv);
EncodedCol EncFrontCodeStr(const std::vector<std::string_view>& sv,
                           uint32_t restart_every = 16);
EncodedCol EncBitWeaveI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
EncodedCol EncSimple8bI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
EncodedCol EncBp128I64(const std::vector<int64_t>& vals,
                       size_t budget = kNoEncodeBudget);
EncodedCol EncodeInnerI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
EncodedCol EncFreqI64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncTopKI64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncTileDictI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
void SortValueRowPairs(std::vector<std::pair<int64_t, uint32_t>>& order);
EncodedCol EncSortDictI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget,
                          int distinct_hint = -1);
EncodedCol EncPostingsI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget,
                          int known_distinct = 0);
EncodedCol EncArithI64(const std::vector<int64_t>& vals,
                       size_t budget = kNoEncodeBudget);
EncodedCol EncPlaI64(const std::vector<int64_t>& vals,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncModI64(const std::vector<int64_t>& vals,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncLaneI64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncSparseI64(const std::vector<int64_t>& vals,
                        const std::vector<uint8_t>& null_bmp,
                        size_t budget = kNoEncodeBudget);
EncodedCol EncAnsForI64(const std::vector<int64_t>& vals,
                        size_t budget = kNoEncodeBudget);
EncodedCol EncRleXI64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncDictXI64(const std::vector<int64_t>& vals,
                       size_t budget = kNoEncodeBudget, int known_distinct = 0);
EncodedCol EncDictXStr(const std::vector<std::string_view>& sv);
EncodedCol EncTemplateStr(const std::vector<std::string_view>& sv);
EncodedCol EncDStreamStr(const std::vector<std::string_view>& sv);
EncodedCol EncPforI64(const std::vector<int64_t>& vals,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncFastPforI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
bool DeltaFastPforStationaryDeltas(const std::vector<int64_t>& vals);
EncodedCol EncDeltaFastPforI64(const std::vector<int64_t>& vals,
                               size_t budget = kNoEncodeBudget);
EncodedCol EncDeltaPForI64(const std::vector<int64_t>& values,
                           size_t budget = kNoEncodeBudget);
EncodedCol EncEliasFanoI64(const std::vector<int64_t>& vals,
                           size_t budget = kNoEncodeBudget);
uint64_t SelectBit(const char* hbmp, uint64_t len, uint64_t idx);
uint64_t SelectBitDir(const char* hbmp, uint64_t len, uint64_t idx,
                      const uint32_t* dir);
EncodedCol EncWaveletI64(const std::vector<int64_t>& vals,
                         size_t budget = kNoEncodeBudget);
EncodedCol EncWaveletRI64(const std::vector<int64_t>& vals);
void MakeDeltasInto(const std::vector<int64_t>& vals,
                    std::vector<int64_t>* deltas);
EncodedCol EncDeltaCompositeI64(const std::vector<int64_t>& vals,
                                const std::vector<int64_t>& deltas,
                                Scheme outer, const EncodedCol& inner_enc,
                                size_t budget = kNoEncodeBudget);
EncodedCol EncDeltaForI64(const std::vector<int64_t>& vals,
                          size_t budget = kNoEncodeBudget);
EncodedCol EncDeltaBitweaveI64(const std::vector<int64_t>& vals,
                               size_t budget = kNoEncodeBudget);
EncodedCol EncSimdForI64(const std::vector<int64_t>& vals,
                         size_t budget = kNoEncodeBudget);
EncodedCol EncDeltaSimdI64(const std::vector<int64_t>& vals,
                           size_t budget = kNoEncodeBudget);
EncodedCol EncSforF64(const std::vector<uint64_t>& bits,
                      size_t budget = kNoEncodeBudget);
EncodedCol EncExpForF64(const std::vector<uint64_t>& bits,
                        size_t budget = kNoEncodeBudget);
EncodedCol EncS4I64(const std::vector<int64_t>& vals,
                    size_t budget = kNoEncodeBudget);
EncodedCol EncD2PforI64(const std::vector<int64_t>& vals,
                        size_t budget = kNoEncodeBudget);
uint64_t ZigZagEnc(int64_t v);
EncodedCol EncDdRiceI64(const std::vector<int64_t>& vals,
                        size_t budget = kNoEncodeBudget);
EncodedCol EncDeltaVarintI64(const std::vector<int64_t>& vals,
                             size_t budget = kNoEncodeBudget);
void InterpEmit(const std::vector<int64_t>& vals, uint32_t i0, uint32_t i1,
                uint64_t lo, uint64_t hi, BitWriter* w);
EncodedCol EncInterpI64(const std::vector<int64_t>& vals,
                        size_t budget = kNoEncodeBudget);
bool QppFitsI64(__int128 value);
bool QppAdaptiveApplicable(const std::vector<int64_t>& values);
EncodedCol EncQppI64(const std::vector<int64_t>& vals,
                     size_t budget = kNoEncodeBudget);
EncodedCol EncChimpF64(const std::vector<uint64_t>& bits);
EncodedCol EncXorForCF64(const std::vector<uint64_t>& bits,
                         size_t budget = kNoEncodeBudget);
EncodedCol EncGorillaCF64(const std::vector<uint64_t>& bits);
EncodedCol EncChimpCF64(const std::vector<uint64_t>& bits);
EncodedCol EncChimp128F64(const std::vector<uint64_t>& bits);
EncodedCol EncAlpF64(const std::vector<uint64_t>& bits);
EncodedCol EncAlpRdTansF64(const std::vector<uint64_t>& bits);
EncodedCol EncPatasF64(const std::vector<uint64_t>& bits);
void BuildCellsInto(const SortedRun::Entry& e, const std::vector<char>& blob,
                    const Schema& sc, const std::vector<ValueType>& col_types,
                    size_t row_idx, RgCells* out);
void ProfileRowGroup(std::span<const Row> group_rows, const Schema& sc,
                     std::vector<RgColumnProfile>& prof,
                     std::vector<FlatMapI64>& col_freq,
                     std::vector<int64_t>& col_mode_val,
                     std::vector<uint64_t>& col_mode_cnt,
                     std::vector<int64_t>& pgm_keys, bool& pgm_usable,
                     const RgCells* cells,
                     std::vector<std::vector<ModeFrontEntry>>& mode_front);
double SchemeDecodeCostInternal(PaxChunk::Scheme s);
EncodedCol EncodeColumnAdaptive(slot_t col_idx, const Schema& sc,
                                std::span<const Row> rows,
                                const RgColumnProfile& prof,
                                const AdaptiveEnv& env,
                                const RgCells* cells = nullptr);

}  // namespace pax_detail

using namespace pax_detail;

void OpDictWindowForValue(const char* region, uint32_t n, uint32_t param,
                          PaxChunk::CompareOp op, int64_t lit, uint64_t* lo,
                          uint64_t* span, bool* clamp_pass);
bool RleDictMatch(PaxChunk::CompareOp op, int64_t v, int64_t lit);
bool HuffDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out, int64_t* sum_out);
bool TopKRegionCountSum(const char* region, uint32_t n, uint32_t param,
                        const char* region_end, PaxChunk::CompareOp op,
                        int64_t lit, uint64_t* count_out, int64_t* sum_out);
void TileDictWindowForValue(const TileDictCursor& c, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* lo, uint64_t* span,
                            bool* clamp_pass);
bool TileDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out, int64_t* sum_out);
bool Bp128RegionCountSumParsed(const Bp128Region& parsed,
                               PaxChunk::CompareOp op, int64_t literal,
                               uint64_t* count_out, int64_t* sum_out);
bool FastPforRegionCountSumParsed(const FastPforRegion& parsed,
                                  PaxChunk::CompareOp op, int64_t literal,
                                  uint64_t* count_out, int64_t* sum_out);
bool FastPforRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t literal, uint64_t* count_out,
                            int64_t* sum_out);
bool DeltaFastPforRegionCountSumParsed(const DeltaFastPforRegion& parsed,
                                       PaxChunk::CompareOp op, int64_t literal,
                                       uint64_t* count_out, int64_t* sum_out);
bool DeltaFastPforRegionCountSum(const char* region, uint32_t n, uint32_t param,
                                 const char* region_end, PaxChunk::CompareOp op,
                                 int64_t literal, uint64_t* count_out,
                                 int64_t* sum_out);
uint64_t Rle8MaskRange(const uint64_t* bits, uint32_t start, uint32_t end);
bool Rle8FillPredicateMask(const Rle8Region& p, PaxChunk::CompareOp op,
                           int64_t literal, const uint8_t* null_bmp,
                           uint64_t* out_bits);
bool Rle8SumMask(const Rle8Region& p, const uint64_t* bits, int64_t* sum_out);
bool Rle8SumMaskWide(const Rle8Region& p, const uint64_t* bits,
                     __int128* sum_out);
bool Rle8SelectedSum(const char* region, uint32_t n, uint32_t param,
                     const char* region_end, const uint8_t* null_bmp,
                     const uint64_t* selected, __int128* sum_out);
bool RleDeltaSelectedSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, const uint8_t* null_bmp,
                         const uint64_t* selected, __int128* sum_out);
bool OpRleRegionCountSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t lit, uint64_t* count_out, int64_t* sum_out);
__int128 DeltaPForBracketSum(const char* bracket);

}  // namespace tinylamb

#endif  // TINYLAMB_PAX_CHUNK_INTERNAL_HPP
