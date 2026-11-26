/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/tlcs_compression.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/crc32c.hpp"

#ifdef TINYLAMB_HAS_ZSTD
#include <zstd.h>
#endif

namespace tinylamb::tlcs_compression {

namespace {

inline void AppendU32Le(std::vector<uint8_t>* out, uint32_t v) {
  out->push_back(static_cast<uint8_t>(v & 0xFFU));
  out->push_back(static_cast<uint8_t>((v >> 8U) & 0xFFU));
  out->push_back(static_cast<uint8_t>((v >> 16U) & 0xFFU));
  out->push_back(static_cast<uint8_t>((v >> 24U) & 0xFFU));
}

inline void AppendU64Le(std::vector<uint8_t>* out, uint64_t v) {
  for (unsigned shift = 0; shift < 64U; shift += 8U) {
    out->push_back(static_cast<uint8_t>((v >> shift) & 0xFFULL));
  }
}

inline bool ReadU32Le(std::span<const uint8_t> in, size_t* pos, uint32_t* out) {
  if (*pos + 4 > in.size()) {
    return false;
  }
  uint32_t v = static_cast<uint32_t>(in[*pos]) |
               (static_cast<uint32_t>(in[*pos + 1]) << 8U) |
               (static_cast<uint32_t>(in[*pos + 2]) << 16U) |
               (static_cast<uint32_t>(in[*pos + 3]) << 24U);
  *pos += 4;
  *out = v;
  return true;
}

inline bool ReadU64Le(std::span<const uint8_t> in, size_t* pos, uint64_t* out) {
  if (*pos + 8 > in.size()) {
    return false;
  }
  uint64_t v = 0;
  for (unsigned i = 0; i < 8U; ++i) {
    v |= static_cast<uint64_t>(in[*pos + i]) << (i * 8U);
  }
  *pos += 8;
  *out = v;
  return true;
}

inline void AppendVarU32(std::vector<uint8_t>* out, uint32_t v) {
  while (v >= 0x80U) {
    out->push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
    v >>= 7U;
  }
  out->push_back(static_cast<uint8_t>(v));
}

inline bool ReadVarU32(std::span<const uint8_t> in, size_t* pos,
                       uint32_t* out) {
  uint32_t result = 0;
  unsigned shift = 0;
  for (int i = 0; i < 5; ++i) {
    if (*pos >= in.size()) {
      return false;
    }
    const uint8_t b = in[(*pos)++];
    if (i == 4 && (b & 0xF0U) != 0) {
      return false;
    }
    result |= static_cast<uint32_t>(b & 0x7FU) << shift;
    if ((b & 0x80U) == 0) {
      *out = result;
      return true;
    }
    shift += 7U;
  }
  return false;
}

class BitWriter {
 public:
  explicit BitWriter(std::vector<uint8_t>* out) : out_(out) {}

  void WriteBits(uint64_t value, unsigned bit_count) {
    if (bit_count == 0) {
      return;
    }
    if (bit_count < 64U) {
      value &= (uint64_t{1} << bit_count) - 1ULL;
    }
    if (bits_in_buf_ >= 64U) {
      FlushBytes(8U);
      buf_ = 0ULL;
      bits_in_buf_ = 0;
    }
    // Emit in up to two chunks if accumulator would overflow 64 bits.
    if (bits_in_buf_ + bit_count > 64U) {
      const unsigned lo = 64U - bits_in_buf_;
      buf_ |= (value << bits_in_buf_);
      FlushBytes(8U);
      buf_ = (lo < 64U) ? (value >> lo) : 0ULL;
      bits_in_buf_ = bit_count - lo;
    } else {
      buf_ |= (value << bits_in_buf_);
      bits_in_buf_ += bit_count;
      if (bits_in_buf_ == 64U) {
        FlushBytes(8U);
        buf_ = 0ULL;
        bits_in_buf_ = 0;
      }
    }
  }

  void Finish() {
    while (bits_in_buf_ > 0) {
      out_->push_back(static_cast<uint8_t>(buf_ & 0xFFULL));
      buf_ >>= 8U;
      bits_in_buf_ = bits_in_buf_ >= 8U ? bits_in_buf_ - 8U : 0U;
    }
  }

 private:
  void FlushBytes(unsigned count) {
    for (unsigned i = 0; i < count; ++i) {
      out_->push_back(static_cast<uint8_t>((buf_ >> (i * 8U)) & 0xFFULL));
    }
  }

  std::vector<uint8_t>* out_;
  uint64_t buf_{0};
  unsigned bits_in_buf_{0};
};

struct RawByteScratch {
  // NOLINTNEXTLINE(modernize-avoid-c-arrays): runtime-sized uninitialized buf.
  std::unique_ptr<uint8_t[]> data;
  size_t capacity{0};

  void Ensure(size_t need) {
    if (need > capacity) {
      const size_t new_cap = std::max(need, capacity * 2U);
      // NOLINTNEXTLINE(modernize-avoid-c-arrays): runtime-sized uninit buf.
      data = std::make_unique_for_overwrite<uint8_t[]>(new_cap);
      capacity = new_cap;
    }
  }

  void TrimIfLarge(size_t max_cap = 4U << 20U) {
    if (capacity > max_cap) {
      data.reset();
      capacity = 0;
    }
  }
};

inline const uint8_t* FastMemmem(const uint8_t* haystack, size_t hlen,
                                 const void* needle, size_t nlen) noexcept {
  if (nlen == 0) {
    return haystack;
  }
  if (hlen < nlen) {
    return nullptr;
  }
#ifdef __linux__
  return static_cast<const uint8_t*>(::memmem(haystack, hlen, needle, nlen));
#else
  const std::string_view h(reinterpret_cast<const char*>(haystack), hlen);
  const std::string_view n(static_cast<const char*>(needle), nlen);
  const size_t pos = h.find(n);
  return pos == std::string_view::npos ? nullptr : haystack + pos;
#endif
}

class BitReader {
 public:
  BitReader(std::span<const uint8_t> in, size_t byte_pos)
      : data_(in.data()), size_(in.size()), byte_pos_(byte_pos) {}

  void Refill() noexcept {
    if constexpr (std::endian::native == std::endian::little) {
      if (byte_pos_ + 8U <= size_) {
        uint64_t word = 0;
        std::memcpy(&word, data_ + byte_pos_, sizeof(word));
        const unsigned empty_bytes = (64U - bits_in_buf_) >> 3U;
        buf_ |= (word << bits_in_buf_);
        byte_pos_ += empty_bytes;
        bits_in_buf_ += empty_bytes << 3U;
        return;
      }
    }
    while (bits_in_buf_ <= 56U && byte_pos_ < size_) {
      buf_ |= static_cast<uint64_t>(data_[byte_pos_++]) << bits_in_buf_;
      bits_in_buf_ += 8U;
    }
  }

  // Fast path for `0 < bit_count <= 32` when caller already verified total
  // payload length is sufficient for all reads.
  uint32_t ReadBits32Unchecked(unsigned bit_count, uint64_t mask) noexcept {
    if (bits_in_buf_ < bit_count) {
      Refill();
    }
    const auto v = static_cast<uint32_t>(buf_ & mask);
    buf_ >>= bit_count;
    bits_in_buf_ -= bit_count;
    return v;
  }

  bool ReadBits(unsigned bit_count, uint64_t* out) {
    if (bit_count == 0) {
      *out = 0;
      return true;
    }
    if (bit_count > 64U) {
      return false;
    }
    if (bit_count <= 56U) {
      if (bits_in_buf_ < bit_count) {
        Refill();
        if (bits_in_buf_ < bit_count) {
          return false;
        }
      }
      const uint64_t mask = (uint64_t{1} << bit_count) - 1ULL;
      *out = buf_ & mask;
      buf_ >>= bit_count;
      bits_in_buf_ -= bit_count;
      return true;
    }
    uint64_t lo = 0;
    uint64_t hi = 0;
    if (!ReadBits(32U, &lo) || !ReadBits(bit_count - 32U, &hi)) {
      return false;
    }
    *out = lo | (hi << 32U);
    return true;
  }

 private:
  const uint8_t* data_{nullptr};
  size_t size_{0};
  size_t byte_pos_{0};
  uint64_t buf_{0};
  unsigned bits_in_buf_{0};
};

// Parse exact 22-byte ASCII `YYYY-MM-DD HH:MM:SS+00` into 14-digit decimal
// integer `YYYYMMDDHHMMSS` (< 2^47).
inline bool ParseTemporalAscii22(std::string_view sv, uint64_t* packed_out) {
  if (sv.size() != kTemporalTextAsciiBytes) {
    return false;
  }
  if (sv[4] != '-' || sv[7] != '-' || sv[10] != ' ' || sv[13] != ':' ||
      sv[16] != ':' || sv[19] != '+' || sv[20] != '0' || sv[21] != '0') {
    return false;
  }
  static constexpr std::array<size_t, 14> kDigitIdx = {
      0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
  uint64_t acc = 0;
  for (size_t idx : kDigitIdx) {
    const auto ch = static_cast<unsigned char>(sv[idx]);
    if (ch < '0' || ch > '9') {
      return false;
    }
    acc = (acc * 10ULL) + static_cast<uint64_t>(ch - '0');
  }
  *packed_out = acc;
  return true;
}

inline void FormatTemporalAscii22(uint64_t packed, char* dst) noexcept {
  // Format 14-digit `YYYYMMDDHHMMSS` into `YYYY-MM-DD HH:MM:SS+00` using a
  // 2-digit lookup table (replaces 14 64-bit divisions with fast 32-bit ops).
  static constexpr std::array<char, 201> kDigits200 = {
      "0001020304050607080910111213141516171819"
      "2021222324252627282930313233343536373839"
      "4041424344454647484950515253545556575859"
      "6061626364656667686970717273747576777879"
      "8081828384858687888990919293949596979899"};
  const auto date = static_cast<uint32_t>(packed / 1000000ULL);
  const auto hms = static_cast<uint32_t>(
      packed - (static_cast<uint64_t>(date) * 1000000ULL));
  const uint32_t yyyy = date / 10000U;
  const uint32_t mmdd = date - (yyyy * 10000U);
  const uint32_t yy_hi = yyyy / 100U;
  const uint32_t yy_lo = yyyy - (yy_hi * 100U);
  const uint32_t mo = mmdd / 100U;
  const uint32_t dd = mmdd - (mo * 100U);
  const uint32_t hh = hms / 10000U;
  const uint32_t mmss = hms - (hh * 10000U);
  const uint32_t mi = mmss / 100U;
  const uint32_t ss = mmss - (mi * 100U);
  const auto copy2 = [&](size_t dst_off, uint32_t v) noexcept {
    std::memcpy(dst + dst_off,
                kDigits200.data() + (static_cast<size_t>(v) * 2U), 2U);
  };
  copy2(0U, yy_hi);
  copy2(2U, yy_lo);
  dst[4] = '-';
  copy2(5U, mo);
  dst[7] = '-';
  copy2(8U, dd);
  dst[10] = ' ';
  copy2(11U, hh);
  dst[13] = ':';
  copy2(14U, mi);
  dst[16] = ':';
  copy2(17U, ss);
  dst[19] = '+';
  dst[20] = '0';
  dst[21] = '0';
}

}  // namespace

// ============================================================================
// 1. Compact Nullmap
// ============================================================================

bool TryEncodeCompactNulls(std::span<const uint8_t> nulls,
                           std::vector<uint8_t>* out) {
  out->clear();
  const size_t n = nulls.size();
  if (n <= 2) {
    return false;
  }
  size_t null_cnt = 0;
  for (uint8_t v : nulls) {
    null_cnt += (v != 0 ? 1U : 0U);
  }
  const size_t non_null_cnt = n - null_cnt;
  const size_t bitmap_bytes = 1U + ((n + 7U) / 8U);
  const size_t sparse_null_bytes = 5U + (null_cnt * sizeof(uint32_t));
  const size_t sparse_nonnull_bytes = 5U + (non_null_cnt * sizeof(uint32_t));

  const size_t best =
      std::min({bitmap_bytes, sparse_null_bytes, sparse_nonnull_bytes});
  if (best >= n) {
    return false;
  }
  if (best == sparse_null_bytes) {
    out->reserve(sparse_null_bytes);
    out->push_back(0x02U);
    AppendU32Le(out, static_cast<uint32_t>(null_cnt));
    for (uint32_t i = 0; i < static_cast<uint32_t>(n); ++i) {
      if (nulls[i] != 0) {
        AppendU32Le(out, i);
      }
    }
    return true;
  }
  if (best == sparse_nonnull_bytes) {
    out->reserve(sparse_nonnull_bytes);
    out->push_back(0x03U);
    AppendU32Le(out, static_cast<uint32_t>(non_null_cnt));
    for (uint32_t i = 0; i < static_cast<uint32_t>(n); ++i) {
      if (nulls[i] == 0) {
        AppendU32Le(out, i);
      }
    }
    return true;
  }
  out->assign(bitmap_bytes, 0U);
  (*out)[0] = 0x01U;
  for (size_t i = 0; i < n; ++i) {
    if (nulls[i] != 0) {
      (*out)[1U + (i >> 3U)] |= static_cast<uint8_t>(1U << (i & 7U));
    }
  }
  return true;
}

bool DecodeCompactNulls(std::span<const uint8_t> encoded, uint32_t n,
                        std::vector<uint8_t>* out) {
  if (encoded.size() == n) {
    out->assign(encoded.begin(), encoded.end());
    return true;
  }
  if (encoded.empty() || encoded.size() > n) {
    return false;
  }
  const uint8_t tag = encoded[0];
  if (tag == 0x01U) {
    const size_t expected = 1U + ((static_cast<size_t>(n) + 7U) / 8U);
    if (encoded.size() != expected) {
      return false;
    }
    out->assign(n, 0U);
    for (uint32_t i = 0; i < n; ++i) {
      (*out)[i] = ((encoded[1U + (i >> 3U)] >> (i & 7U)) & 1U) != 0 ? 1U : 0U;
    }
    return true;
  }
  if (tag == 0x02U || tag == 0x03U) {
    size_t pos = 1;
    uint32_t cnt = 0;
    if (!ReadU32Le(encoded, &pos, &cnt)) {
      return false;
    }
    if (encoded.size() != 5U + (static_cast<size_t>(cnt) * sizeof(uint32_t))) {
      return false;
    }
    const uint8_t default_val = (tag == 0x02U) ? 0U : 1U;
    const uint8_t mark_val = (tag == 0x02U) ? 1U : 0U;
    out->assign(n, default_val);
    for (uint32_t k = 0; k < cnt; ++k) {
      uint32_t idx = 0;
      if (!ReadU32Le(encoded, &pos, &idx) || idx >= n) {
        return false;
      }
      (*out)[idx] = mark_val;
    }
    return true;
  }
  return false;
}

bool GatherCompactNulls(std::span<const uint8_t> encoded, uint32_t n,
                        std::span<const uint32_t> rows, uint8_t* out) {
  if (encoded.size() == n) {
    for (size_t j = 0; j < rows.size(); ++j) {
      if (rows[j] >= n) {
        return false;
      }
      out[j] = encoded[rows[j]];
    }
    return true;
  }
  if (encoded.empty() || encoded.size() > n) {
    return false;
  }
  const uint8_t tag = encoded[0];
  if (tag == 0x01U) {
    const size_t expected = 1U + ((static_cast<size_t>(n) + 7U) / 8U);
    if (encoded.size() != expected) {
      return false;
    }
    for (size_t j = 0; j < rows.size(); ++j) {
      const uint32_t r = rows[j];
      if (r >= n) {
        return false;
      }
      out[j] = ((encoded[1U + (r >> 3U)] >> (r & 7U)) & 1U) != 0 ? 1U : 0U;
    }
    return true;
  }
  if (tag == 0x02U || tag == 0x03U) {
    size_t pos = 1;
    uint32_t cnt = 0;
    if (!ReadU32Le(encoded, &pos, &cnt)) {
      return false;
    }
    if (encoded.size() != 5U + (static_cast<size_t>(cnt) * sizeof(uint32_t))) {
      return false;
    }
    const uint8_t default_val = (tag == 0x02U) ? 0U : 1U;
    const uint8_t mark_val = (tag == 0x02U) ? 1U : 0U;
    const uint8_t* const base = encoded.data() + pos;
    auto read_at = [base](uint32_t idx) noexcept -> uint32_t {
      uint32_t v = 0;
      std::memcpy(&v, base + (static_cast<size_t>(idx) * sizeof(uint32_t)),
                  sizeof(uint32_t));
      return v;
    };
    for (size_t j = 0; j < rows.size(); ++j) {
      const uint32_t r = rows[j];
      if (r >= n) {
        return false;
      }
      // TryEncodeCompactNulls writes marked positions in strictly ascending
      // order; binary search locates `r` in O(log cnt) without allocation.
      uint32_t lo = 0;
      uint32_t hi = cnt;
      bool found = false;
      while (lo < hi) {
        const uint32_t mid = lo + ((hi - lo) >> 1U);
        const uint32_t mv = read_at(mid);
        if (mv < r) {
          lo = mid + 1U;
        } else if (mv > r) {
          hi = mid;
        } else {
          found = true;
          break;
        }
      }
      out[j] = found ? mark_val : default_val;
    }
    return true;
  }
  return false;
}

// ============================================================================
// 2. TemporalText 47-bit Timestamp Packing
// ============================================================================

bool TryEncodeTemporalText(std::span<const char> bytes,
                           std::span<const uint32_t> ends, const uint8_t* nulls,
                           uint32_t n, std::vector<uint8_t>* out) {
  out->clear();
  if (n == 0 || ends.size() != n) {
    return false;
  }
  // Must strictly save bytes compared to raw kVarChar (bytes + 4*n ends).
  const size_t packed_size = static_cast<size_t>(n) * kTemporalTextPackedBytes;
  const size_t raw_size = bytes.size() + (static_cast<size_t>(n) * 4ULL);
  if (packed_size >= raw_size) {
    return false;
  }
  out->resize(packed_size);
  uint32_t prev_end = 0;
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t cur_end = ends[r];
    if (cur_end < prev_end || cur_end > bytes.size()) {
      out->clear();
      return false;
    }
    uint64_t val = 0;
    if (nulls == nullptr || nulls[r] == 0) {
      const std::string_view sv(bytes.data() + prev_end, cur_end - prev_end);
      if (!ParseTemporalAscii22(sv, &val)) {
        out->clear();
        return false;
      }
    }
    uint8_t* dst =
        out->data() + (static_cast<size_t>(r) * kTemporalTextPackedBytes);
    for (size_t b = 0; b < kTemporalTextPackedBytes; ++b) {
      dst[b] = static_cast<uint8_t>((val >> (b * 8U)) & 0xFFULL);
    }
    prev_end = cur_end;
  }
  return true;
}

namespace {

template <typename ByteT>
bool DecodeTemporalTextImpl(std::span<const uint8_t> encoded,
                            const uint8_t* nulls, uint32_t n,
                            std::vector<ByteT>* bytes_out,
                            std::vector<uint32_t>* ends_out,
                            bool lengths_only) {
  if (encoded.size() != static_cast<size_t>(n) * kTemporalTextPackedBytes) {
    return false;
  }
  ends_out->resize(n);
  if (lengths_only) {
    bytes_out->clear();
    uint32_t total = 0;
    for (uint32_t r = 0; r < n; ++r) {
      if (nulls != nullptr && nulls[r] != 0) {
        (*ends_out)[r] = total;
        continue;
      }
      const uint8_t* src =
          encoded.data() + (static_cast<size_t>(r) * kTemporalTextPackedBytes);
      uint64_t val = 0;
      for (size_t b = 0; b < kTemporalTextPackedBytes; ++b) {
        val |= static_cast<uint64_t>(src[b]) << (b * 8U);
      }
      if (val > 99999999999999ULL) {
        return false;
      }
      total += static_cast<uint32_t>(kTemporalTextAsciiBytes);
      (*ends_out)[r] = total;
    }
    return true;
  }
  bytes_out->resize(static_cast<size_t>(n) * kTemporalTextAsciiBytes);
  char* dst = reinterpret_cast<char*>(bytes_out->data());
  uint32_t* const ends_dst = ends_out->data();
  const uint8_t* const enc_ptr = encoded.data();
  uint32_t written = 0;
  uint64_t prev_val = UINT64_MAX;
  for (uint32_t r = 0; r < n; ++r) {
    if (nulls != nullptr && nulls[r] != 0) {
      ends_dst[r] = written;
      continue;
    }
    const uint8_t* const src =
        enc_ptr + (static_cast<size_t>(r) * kTemporalTextPackedBytes);
    uint64_t val = 0;
    if constexpr (std::endian::native == std::endian::little) {
      if (r + 1U < n) {
        std::memcpy(&val, src, sizeof(val));
        val &= 0x0000FFFFFFFFFFFFULL;
      } else {
        std::memcpy(&val, src, kTemporalTextPackedBytes);
      }
    } else {
      for (size_t b = 0; b < kTemporalTextPackedBytes; ++b) {
        val |= static_cast<uint64_t>(src[b]) << (b * 8U);
      }
    }
    if (val > 99999999999999ULL) {
      bytes_out->clear();
      return false;
    }
    if (val == prev_val && written >= kTemporalTextAsciiBytes) {
      std::memcpy(dst + written, dst + written - kTemporalTextAsciiBytes,
                  kTemporalTextAsciiBytes);
    } else {
      FormatTemporalAscii22(val, dst + written);
      prev_val = val;
    }
    written += static_cast<uint32_t>(kTemporalTextAsciiBytes);
    ends_dst[r] = written;
  }
  bytes_out->resize(written);
  return true;
}

}  // namespace

bool DecodeTemporalText(std::span<const uint8_t> encoded, const uint8_t* nulls,
                        uint32_t n, std::vector<char>* bytes_out,
                        std::vector<uint32_t>* ends_out) {
  return DecodeTemporalTextImpl(encoded, nulls, n, bytes_out, ends_out,
                                /*lengths_only=*/false);
}

bool DecodeTemporalTextBytes(std::span<const uint8_t> encoded,
                             const uint8_t* nulls, uint32_t n,
                             std::vector<uint8_t>* bytes_out,
                             std::vector<uint32_t>* ends_out,
                             bool lengths_only) {
  return DecodeTemporalTextImpl(encoded, nulls, n, bytes_out, ends_out,
                                lengths_only);
}

bool DecodeTemporalTextRow(std::span<const uint8_t> encoded, uint32_t row,
                           std::string* out) {
  const size_t off = static_cast<size_t>(row) * kTemporalTextPackedBytes;
  if (off + kTemporalTextPackedBytes > encoded.size()) {
    return false;
  }
  uint64_t val = 0;
  for (size_t b = 0; b < kTemporalTextPackedBytes; ++b) {
    val |= static_cast<uint64_t>(encoded[off + b]) << (b * 8U);
  }
  if (val > 99999999999999ULL) {
    return false;
  }
  out->resize(kTemporalTextAsciiBytes);
  FormatTemporalAscii22(val, out->data());
  return true;
}

// ============================================================================
// 3. Sorted Front-Coded Block Dictionary + Bit-Packed IDs
// ============================================================================

bool TryEncodeVarCharFrontDict(std::span<const char> bytes,
                               std::span<const uint32_t> ends,
                               const uint8_t* nulls, uint32_t n,
                               std::vector<uint8_t>* out) {
  out->clear();
  if (n < 16 || ends.size() != n) {
    return false;
  }
  const size_t raw_total = bytes.size() + (static_cast<size_t>(n) * 4ULL);
  std::unordered_map<std::string_view, uint32_t> unique_map;
  unique_map.reserve(std::min<size_t>(n, 4096));
  std::vector<std::string_view> dict;
  dict.reserve(std::min<size_t>(n, 4096));

  uint32_t prev_end = 0;
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t cur_end = ends[r];
    if (cur_end < prev_end || cur_end > bytes.size()) {
      return false;
    }
    if (nulls == nullptr || nulls[r] == 0) {
      const std::string_view sv(bytes.data() + prev_end, cur_end - prev_end);
      if (unique_map.emplace(sv, 0U).second) {
        dict.push_back(sv);
      }
    }
    prev_end = cur_end;
  }
  if (dict.empty()) {
    return false;
  }
  std::ranges::sort(dict);
  for (uint32_t i = 0; i < static_cast<uint32_t>(dict.size()); ++i) {
    unique_map[dict[i]] = i;
  }

  const unsigned bw =
      dict.size() <= 1 ? 0U
                       : static_cast<unsigned>(std::bit_width(dict.size() - 1));
  const size_t packed_id_bytes = (static_cast<size_t>(n) * bw + 7ULL) / 8ULL;

  // Header: [u32 dict_size][u8 bit_width][front-coded dict...][packed IDs...]
  out->reserve(5U + packed_id_bytes + (bytes.size() / 2));
  AppendU32Le(out, static_cast<uint32_t>(dict.size()));
  out->push_back(static_cast<uint8_t>(bw));

  std::string_view prev_str;
  for (const std::string_view cur : dict) {
    size_t lcp = 0;
    const size_t max_lcp = std::min(prev_str.size(), cur.size());
    while (lcp < max_lcp && prev_str[lcp] == cur[lcp]) {
      ++lcp;
    }
    const size_t suffix_len = cur.size() - lcp;
    AppendVarU32(out, static_cast<uint32_t>(lcp));
    AppendVarU32(out, static_cast<uint32_t>(suffix_len));
    if (suffix_len > 0) {
      const auto* p = reinterpret_cast<const uint8_t*>(cur.data() + lcp);
      out->insert(out->end(), p, p + suffix_len);
    }
    if (out->size() + packed_id_bytes >= raw_total) {
      out->clear();
      return false;
    }
    prev_str = cur;
  }

  if (bw > 0) {
    BitWriter writer(out);
    prev_end = 0;
    for (uint32_t r = 0; r < n; ++r) {
      const uint32_t cur_end = ends[r];
      uint32_t id = 0;
      if (nulls == nullptr || nulls[r] == 0) {
        const std::string_view sv(bytes.data() + prev_end, cur_end - prev_end);
        id = unique_map[sv];
      }
      writer.WriteBits(id, bw);
      prev_end = cur_end;
    }
    writer.Finish();
  }

  if (out->size() >= raw_total) {
    out->clear();
    return false;
  }
  return true;
}

namespace {

inline bool ReadVarU32Fast(std::span<const uint8_t> in, size_t* pos,
                           uint32_t* out) noexcept {
  if (*pos < in.size() && in[*pos] < 0x80U) {
    *out = in[(*pos)++];
    return true;
  }
  return ReadVarU32(in, pos, out);
}

bool DecodeFrontDictHeader(std::span<const uint8_t> encoded, uint32_t n,
                           uint32_t* dict_size_out, unsigned* bw_out,
                           size_t* pos_out, RawByteScratch* dict_bytes,
                           size_t* dict_bytes_len_out,
                           std::vector<uint32_t>* dict_ends) {
  size_t pos = 0;
  uint32_t dict_size = 0;
  if (!ReadU32Le(encoded, &pos, &dict_size) || pos >= encoded.size() ||
      dict_size == 0 || dict_size > n) {
    return false;
  }
  const unsigned bw = encoded[pos++];
  const unsigned expected_bw =
      dict_size <= 1 ? 0U
                     : static_cast<unsigned>(std::bit_width(dict_size - 1));
  if (bw != expected_bw || bw > 32U) {
    return false;
  }
  dict_ends->resize(dict_size);
  dict_bytes->Ensure(std::max<size_t>(encoded.size() * 2U, 4096U));
  size_t written = 0;
  uint32_t prev_begin = 0;
  uint32_t prev_len = 0;
  const uint8_t* const enc_ptr = encoded.data();
  const size_t enc_size = encoded.size();
  for (uint32_t i = 0; i < dict_size; ++i) {
    uint32_t lcp = 0;
    uint32_t suffix_len = 0;
    if (!ReadVarU32Fast(encoded, &pos, &lcp) ||
        !ReadVarU32Fast(encoded, &pos, &suffix_len)) {
      return false;
    }
    if (i == 0 && lcp != 0) {
      return false;
    }
    if (i > 0 && lcp > prev_len) {
      return false;
    }
    if (suffix_len > enc_size - pos) {
      return false;
    }
    const size_t cur_len = static_cast<size_t>(lcp) + suffix_len;
    if (written + cur_len > dict_bytes->capacity) {
      const size_t new_cap =
          std::max(written + cur_len, dict_bytes->capacity * 2U);
      // NOLINTNEXTLINE(modernize-avoid-c-arrays): runtime-sized uninit buf.
      auto next = std::make_unique_for_overwrite<uint8_t[]>(new_cap);
      if (written > 0) {
        std::memcpy(next.get(), dict_bytes->data.get(), written);
      }
      dict_bytes->data = std::move(next);
      dict_bytes->capacity = new_cap;
    }
    uint8_t* const dst = dict_bytes->data.get() + written;
    if (lcp > 0) {
      std::memcpy(dst, dict_bytes->data.get() + prev_begin, lcp);
    }
    if (suffix_len > 0) {
      std::memcpy(dst + lcp, enc_ptr + pos, suffix_len);
      pos += suffix_len;
    }
    prev_begin = static_cast<uint32_t>(written);
    prev_len = static_cast<uint32_t>(cur_len);
    written += cur_len;
    (*dict_ends)[i] = static_cast<uint32_t>(written);
  }
  const size_t expected_id_bytes = (static_cast<size_t>(n) * bw + 7ULL) / 8ULL;
  if (enc_size - pos != expected_id_bytes) {
    return false;
  }
  *dict_size_out = dict_size;
  *bw_out = bw;
  *pos_out = pos;
  *dict_bytes_len_out = written;
  return true;
}

template <typename ByteT>
bool DecodeVarCharFrontDictImpl(std::span<const uint8_t> encoded,
                                const uint8_t* nulls, uint32_t n,
                                std::vector<ByteT>* bytes_out,
                                std::vector<uint32_t>* ends_out,
                                bool lengths_only,
                                std::span<const uint32_t> selected_rows) {
  if (lengths_only) {
    size_t pos = 0;
    uint32_t dict_size = 0;
    if (!ReadU32Le(encoded, &pos, &dict_size) || pos >= encoded.size() ||
        dict_size == 0 || dict_size > n) {
      return false;
    }
    const unsigned bw = encoded[pos++];
    const unsigned expected_bw =
        dict_size <= 1 ? 0U
                       : static_cast<unsigned>(std::bit_width(dict_size - 1));
    if (bw != expected_bw || bw > 32U) {
      return false;
    }
    thread_local std::vector<uint32_t> dict_lens;
    dict_lens.resize(dict_size);
    uint32_t prev_len = 0;
    for (uint32_t i = 0; i < dict_size; ++i) {
      uint32_t lcp = 0;
      uint32_t suffix_len = 0;
      if (!ReadVarU32Fast(encoded, &pos, &lcp) ||
          !ReadVarU32Fast(encoded, &pos, &suffix_len)) {
        return false;
      }
      if (i == 0 && lcp != 0) {
        return false;
      }
      if (i > 0 && lcp > prev_len) {
        return false;
      }
      if (suffix_len > encoded.size() - pos) {
        return false;
      }
      pos += suffix_len;
      const uint32_t cur_len = lcp + suffix_len;
      dict_lens[i] = cur_len;
      prev_len = cur_len;
    }
    const size_t expected_id_bytes =
        (static_cast<size_t>(n) * bw + 7ULL) / 8ULL;
    if (encoded.size() - pos != expected_id_bytes) {
      return false;
    }
    bytes_out->clear();
    ends_out->resize(n);
    uint32_t* const ends_dst = ends_out->data();
    const uint32_t* const dlens = dict_lens.data();
    BitReader reader(encoded, pos);
    const uint64_t mask = bw > 0 ? ((uint64_t{1} << bw) - 1ULL) : 0ULL;
    uint32_t total = 0;
    for (uint32_t r = 0; r < n; ++r) {
      const uint32_t id = bw > 0 ? reader.ReadBits32Unchecked(bw, mask) : 0U;
      if (nulls != nullptr && nulls[r] != 0) {
        ends_dst[r] = total;
        continue;
      }
      if (id >= dict_size) {
        return false;
      }
      total += dlens[id];
      ends_dst[r] = total;
    }
    return true;
  }

  thread_local RawByteScratch dict_bytes;
  thread_local std::vector<uint32_t> dict_ends;
  thread_local std::vector<uint32_t> row_ids;
  uint32_t dict_size = 0;
  unsigned bw = 0;
  size_t pos = 0;
  size_t dict_bytes_len = 0;
  if (!DecodeFrontDictHeader(encoded, n, &dict_size, &bw, &pos, &dict_bytes,
                             &dict_bytes_len, &dict_ends)) {
    dict_bytes.TrimIfLarge();
    return false;
  }

  const uint32_t* const dends = dict_ends.data();
  const uint8_t* const dict_data = dict_bytes.data.get();
  const uint64_t mask = bw > 0 ? ((uint64_t{1} << bw) - 1ULL) : 0ULL;
  BitReader reader(encoded, pos);

  if (!selected_rows.empty()) {
    row_ids.resize(selected_rows.size());
    size_t sel_idx = 0;
    uint64_t total_bytes = 0;
    for (uint32_t r = 0; r < n; ++r) {
      const uint32_t id = bw > 0 ? reader.ReadBits32Unchecked(bw, mask) : 0U;
      if (sel_idx < selected_rows.size() && selected_rows[sel_idx] == r) {
        if (nulls != nullptr && nulls[r] != 0) {
          row_ids[sel_idx] = UINT32_MAX;
        } else {
          if (id >= dict_size) {
            dict_bytes.TrimIfLarge();
            return false;
          }
          row_ids[sel_idx] = id;
          const uint32_t d_begin = id == 0 ? 0U : dends[id - 1];
          total_bytes += dends[id] - d_begin;
        }
        ++sel_idx;
      } else if ((nulls == nullptr || nulls[r] == 0) && id >= dict_size) {
        dict_bytes.TrimIfLarge();
        return false;
      }
    }
    bytes_out->resize(static_cast<size_t>(total_bytes));
    ends_out->resize(n);
    ByteT* dst = bytes_out->data();
    uint32_t packed = 0;
    uint32_t next_row = 0;
    for (size_t si = 0; si < selected_rows.size(); ++si) {
      const uint32_t row = selected_rows[si];
      std::fill_n(ends_out->begin() + next_row, row - next_row, packed);
      const uint32_t uid = row_ids[si];
      if (uid != UINT32_MAX) {
        const uint32_t d_begin = uid == 0 ? 0U : dends[uid - 1];
        const uint32_t len = dends[uid] - d_begin;
        if (len > 0) {
          std::memcpy(dst + packed, dict_data + d_begin, len);
          packed += len;
        }
      }
      (*ends_out)[row] = packed;
      next_row = row + 1;
    }
    std::fill(ends_out->begin() + next_row, ends_out->end(), packed);
    dict_bytes.TrimIfLarge();
    return true;
  }

  row_ids.resize(n);
  ends_out->resize(n);
  uint32_t* const ends_dst = ends_out->data();
  uint32_t* const ids_dst = row_ids.data();
  uint64_t total_bytes = 0;
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t id = bw > 0 ? reader.ReadBits32Unchecked(bw, mask) : 0U;
    if (nulls != nullptr && nulls[r] != 0) {
      ids_dst[r] = UINT32_MAX;
      ends_dst[r] = static_cast<uint32_t>(total_bytes);
      continue;
    }
    if (id >= dict_size) {
      dict_bytes.TrimIfLarge();
      return false;
    }
    ids_dst[r] = id;
    const uint32_t d_begin = id == 0 ? 0U : dends[id - 1];
    total_bytes += dends[id] - d_begin;
    ends_dst[r] = static_cast<uint32_t>(total_bytes);
  }

  bytes_out->resize(static_cast<size_t>(total_bytes));
  ByteT* dst = bytes_out->data();
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t uid = ids_dst[r];
    if (uid == UINT32_MAX) {
      continue;
    }
    const uint32_t d_begin = uid == 0 ? 0U : dends[uid - 1];
    const uint32_t len = dends[uid] - d_begin;
    if (len > 0) {
      std::memcpy(dst, dict_data + d_begin, len);
      dst += len;
    }
  }
  dict_bytes.TrimIfLarge();
  return true;
}

}  // namespace

bool DecodeVarCharFrontDict(std::span<const uint8_t> encoded,
                            const uint8_t* nulls, uint32_t n,
                            std::vector<char>* bytes_out,
                            std::vector<uint32_t>* ends_out) {
  return DecodeVarCharFrontDictImpl(encoded, nulls, n, bytes_out, ends_out,
                                    /*lengths_only=*/false, {});
}

bool DecodeVarCharFrontDictBytes(std::span<const uint8_t> encoded,
                                 const uint8_t* nulls, uint32_t n,
                                 std::vector<uint8_t>* bytes_out,
                                 std::vector<uint32_t>* ends_out,
                                 bool lengths_only,
                                 std::span<const uint32_t> selected_rows) {
  return DecodeVarCharFrontDictImpl(encoded, nulls, n, bytes_out, ends_out,
                                    lengths_only, selected_rows);
}

bool MatchVarCharFrontDictContains(std::span<const uint8_t> encoded,
                                   const uint8_t* nulls, uint32_t n,
                                   std::string_view needle,
                                   std::vector<uint32_t>* matching_rows_out) {
  matching_rows_out->clear();
  if (needle.empty()) {
    matching_rows_out->reserve(n);
    for (uint32_t r = 0; r < n; ++r) {
      if (nulls == nullptr || nulls[r] == 0) {
        matching_rows_out->push_back(r);
      }
    }
    return true;
  }
  thread_local RawByteScratch dict_bytes;
  thread_local std::vector<uint32_t> dict_ends;
  uint32_t dict_size = 0;
  unsigned bw = 0;
  size_t pos = 0;
  size_t dict_bytes_len = 0;
  if (!DecodeFrontDictHeader(encoded, n, &dict_size, &bw, &pos, &dict_bytes,
                             &dict_bytes_len, &dict_ends)) {
    dict_bytes.TrimIfLarge();
    return false;
  }
  const uint8_t* const dict_data = dict_bytes.data.get();
  const uint8_t* hit =
      FastMemmem(dict_data, dict_bytes_len, needle.data(), needle.size());
  if (hit == nullptr) {
    dict_bytes.TrimIfLarge();
    return true;
  }

  thread_local std::vector<uint8_t> dict_match;
  dict_match.assign(dict_size, 0U);
  bool any_dict_match = false;
  const uint32_t* const dends = dict_ends.data();
  const size_t nlen = needle.size();
  while (hit != nullptr) {
    const auto off = static_cast<uint32_t>(hit - dict_data);
    const auto* const it = std::upper_bound(dends, dends + dict_size, off);
    if (it == dends + dict_size) {
      break;
    }
    const auto entry_idx = static_cast<uint32_t>(it - dends);
    const uint32_t entry_end = *it;
    if (off + nlen <= entry_end) {
      dict_match[entry_idx] = 1U;
      any_dict_match = true;
    }
    if (entry_end >= dict_bytes_len) {
      break;
    }
    hit = FastMemmem(dict_data + entry_end, dict_bytes_len - entry_end,
                     needle.data(), nlen);
  }
  if (!any_dict_match) {
    dict_bytes.TrimIfLarge();
    return true;
  }

  BitReader reader(encoded, pos);
  const uint64_t mask = bw > 0 ? ((uint64_t{1} << bw) - 1ULL) : 0ULL;
  const uint8_t* const dm = dict_match.data();
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t id = bw > 0 ? reader.ReadBits32Unchecked(bw, mask) : 0U;
    if (nulls != nullptr && nulls[r] != 0) {
      continue;
    }
    if (id >= dict_size) {
      matching_rows_out->clear();
      dict_bytes.TrimIfLarge();
      return false;
    }
    if (dm[id] != 0U) {
      matching_rows_out->push_back(r);
    }
  }
  dict_bytes.TrimIfLarge();
  return true;
}

// ============================================================================
// 4. Bit-Packed Strided Frame-of-Reference
// ============================================================================

bool TryEncodeBitPackedFor(std::span<const int64_t> ints, const uint8_t* nulls,
                           uint32_t n, int64_t min_int, int64_t max_int,
                           uint64_t byte_aligned_size,
                           std::vector<uint8_t>* out) {
  out->clear();
  if (n < 16 || ints.size() != n) {
    return false;
  }
  const auto base_u = static_cast<uint64_t>(min_int);
  const uint64_t span = static_cast<uint64_t>(max_int) - base_u;
  if (span == 0) {
    return false;  // Handled by zero-width kFor.
  }
  uint64_t stride = 0;
  for (uint32_t r = 0; r < n; ++r) {
    if (nulls != nullptr && nulls[r] != 0) {
      continue;
    }
    const uint64_t d = static_cast<uint64_t>(ints[r]) - base_u;
    if (d != 0) {
      stride = (stride == 0) ? d : std::gcd(stride, d);
      if (stride == 1ULL) {
        break;
      }
    }
  }
  if (stride == 0) {
    stride = 1ULL;
  }
  const uint64_t scaled_max = span / stride;
  const auto bw = static_cast<unsigned>(std::bit_width(scaled_max));
  if (bw == 0 || bw >= 64U) {
    return false;
  }
  // Header: [u8 bw][u64 le stride] = 9 bytes + packed bits.
  const size_t total_size = 9U + ((static_cast<size_t>(n) * bw + 7ULL) / 8ULL);
  if (total_size >= byte_aligned_size) {
    return false;
  }
  out->reserve(total_size);
  out->push_back(static_cast<uint8_t>(bw));
  AppendU64Le(out, stride);
  BitWriter writer(out);
  for (uint32_t r = 0; r < n; ++r) {
    uint64_t q = 0;
    if (nulls == nullptr || nulls[r] == 0) {
      q = (static_cast<uint64_t>(ints[r]) - base_u) / stride;
    }
    writer.WriteBits(q, bw);
  }
  writer.Finish();
  return out->size() == total_size;
}

namespace {

template <bool kUnitStride>
inline void UnpackBitPackedForFast56(const uint8_t* payload,
                                     size_t payload_size, uint32_t n,
                                     unsigned bw, uint64_t base_u,
                                     uint64_t stride, int64_t* out) noexcept {
  const uint64_t mask = (uint64_t{1} << bw) - 1ULL;
  uint64_t buf = 0;
  unsigned bits_in_buf = 0;
  size_t bp = 0;
  auto refill = [&]() noexcept {
    if constexpr (std::endian::native == std::endian::little) {
      if (bp + 8U <= payload_size) {
        uint64_t word = 0;
        std::memcpy(&word, payload + bp, sizeof(word));
        const unsigned empty_bytes = (64U - bits_in_buf) >> 3U;
        buf |= (word << bits_in_buf);
        bp += empty_bytes;
        bits_in_buf += empty_bytes << 3U;
        return;
      }
    }
    while (bits_in_buf <= 56U && bp < payload_size) {
      buf |= static_cast<uint64_t>(payload[bp++]) << bits_in_buf;
      bits_in_buf += 8U;
    }
  };
  for (uint32_t r = 0; r < n; ++r) {
    if (bits_in_buf < bw) {
      refill();
    }
    const uint64_t q = buf & mask;
    buf >>= bw;
    bits_in_buf -= bw;
    if constexpr (kUnitStride) {
      out[r] = static_cast<int64_t>(base_u + q);
    } else {
      out[r] = static_cast<int64_t>(base_u + (q * stride));
    }
  }
}

}  // namespace

bool DecodeBitPackedFor(std::span<const uint8_t> encoded, uint32_t n,
                        int64_t min_int, int64_t* out) {
  if (encoded.size() < 9U) {
    return false;
  }
  const unsigned bw = encoded[0];
  if (bw == 0 || bw >= 64U) {
    return false;
  }
  size_t pos = 1;
  uint64_t stride = 0;
  if (!ReadU64Le(encoded, &pos, &stride) || stride == 0) {
    return false;
  }
  const size_t expected_payload = (static_cast<size_t>(n) * bw + 7ULL) / 8ULL;
  if (encoded.size() - pos != expected_payload) {
    return false;
  }
  const auto base_u = static_cast<uint64_t>(min_int);
  if (bw <= 56U) {
    const uint8_t* const payload = encoded.data() + pos;
    if (stride == 1ULL) {
      UnpackBitPackedForFast56<true>(payload, expected_payload, n, bw, base_u,
                                     1ULL, out);
    } else {
      UnpackBitPackedForFast56<false>(payload, expected_payload, n, bw, base_u,
                                      stride, out);
    }
    return true;
  }
  BitReader reader(encoded, pos);
  for (uint32_t r = 0; r < n; ++r) {
    uint64_t q = 0;
    if (!reader.ReadBits(bw, &q)) {
      return false;
    }
    out[r] = static_cast<int64_t>(base_u + (q * stride));
  }
  return true;
}

bool GatherBitPackedFor(std::span<const uint8_t> encoded, uint32_t n,
                        int64_t min_int, std::span<const uint32_t> rows,
                        int64_t* out) {
  if (encoded.size() < 9U) {
    return false;
  }
  const unsigned bw = encoded[0];
  if (bw == 0 || bw >= 64U) {
    return false;
  }
  size_t pos = 1;
  uint64_t stride = 0;
  if (!ReadU64Le(encoded, &pos, &stride) || stride == 0) {
    return false;
  }
  const size_t expected_payload = (static_cast<size_t>(n) * bw + 7ULL) / 8ULL;
  if (encoded.size() - pos != expected_payload) {
    return false;
  }
  const auto base_u = static_cast<uint64_t>(min_int);
  const uint64_t value_mask = (uint64_t{1} << bw) - 1ULL;
  const uint8_t* const payload = encoded.data() + pos;
  const size_t payload_size = expected_payload;
  for (size_t j = 0; j < rows.size(); ++j) {
    const uint32_t r = rows[j];
    if (r >= n) {
      return false;
    }
    const size_t bit_pos = static_cast<size_t>(r) * bw;
    size_t byte_pos = bit_pos >> 3U;
    auto bit_off = static_cast<unsigned>(bit_pos & 7U);
    uint64_t q = 0;
    if (bit_off + bw <= 64U && byte_pos + 8U <= payload_size) {
      uint64_t word = 0;
      std::memcpy(&word, payload + byte_pos, sizeof(word));
      q = (word >> bit_off) & value_mask;
    } else {
      unsigned filled = 0;
      while (filled < bw) {
        if (byte_pos >= payload_size) {
          return false;
        }
        const unsigned avail = 8U - bit_off;
        const unsigned take = std::min(avail, bw - filled);
        const uint64_t take_mask = (uint64_t{1} << take) - 1ULL;
        const uint64_t bits =
            (static_cast<uint64_t>(payload[byte_pos++]) >> bit_off) & take_mask;
        q |= (bits << filled);
        filled += take;
        bit_off = 0;
      }
    }
    out[j] = static_cast<int64_t>(base_u + (q * stride));
  }
  return true;
}

// ============================================================================
// 5. Bit-Exact XOR-Chain Compressed IEEE-754 Doubles
// ============================================================================

bool TryEncodeXorDouble(std::span<const int64_t> bits, uint32_t n,
                        std::vector<uint8_t>* out) {
  out->clear();
  if (n < 8 || bits.size() != n) {
    return false;
  }
  const size_t raw_size = static_cast<size_t>(n) * sizeof(uint64_t);
  out->reserve(raw_size / 2);
  BitWriter writer(out);
  auto prev = static_cast<uint64_t>(bits[0]);
  writer.WriteBits(prev, 64U);
  unsigned prev_leading = 64U;
  unsigned prev_trailing = 0U;

  for (uint32_t r = 1; r < n; ++r) {
    const auto cur = static_cast<uint64_t>(bits[r]);
    const uint64_t x = prev ^ cur;
    if (x == 0ULL) {
      writer.WriteBits(0ULL, 1U);
    } else {
      const auto leading = static_cast<unsigned>(std::countl_zero(x));
      const auto trailing = static_cast<unsigned>(std::countr_zero(x));
      if (prev_leading <= leading && prev_trailing <= trailing &&
          prev_leading < 64U) {
        // Control bits `10`: reuse previous meaningful window.
        writer.WriteBits(0b01ULL, 2U);  // bit 0 = 1, bit 1 = 0
        const unsigned sig_bits = 64U - prev_leading - prev_trailing;
        writer.WriteBits(x >> prev_trailing, sig_bits);
      } else {
        // Control bits `11`: 6-bit leading + 6-bit (sig_bits - 1) + payload.
        writer.WriteBits(0b11ULL, 2U);
        const unsigned sig_bits = 64U - leading - trailing;
        writer.WriteBits(leading, 6U);
        writer.WriteBits(sig_bits - 1U, 6U);
        writer.WriteBits(x >> trailing, sig_bits);
        prev_leading = leading;
        prev_trailing = trailing;
      }
    }
    if (out->size() + 16U >= raw_size) {
      out->clear();
      return false;
    }
    prev = cur;
  }
  writer.Finish();
  if (out->size() >= raw_size) {
    out->clear();
    return false;
  }
  return true;
}

bool DecodeXorDouble(std::span<const uint8_t> encoded, uint32_t n,
                     int64_t* out) {
  if (n == 0) {
    return true;
  }
  if (encoded.size() < 8U) {
    return false;
  }
  const uint8_t* const data = encoded.data();
  const size_t size = encoded.size();
  uint64_t prev = 0;
  if constexpr (std::endian::native == std::endian::little) {
    std::memcpy(&prev, data, sizeof(prev));
  } else {
    for (size_t b = 0; b < 8U; ++b) {
      prev |= static_cast<uint64_t>(data[b]) << (b * 8U);
    }
  }
  out[0] = static_cast<int64_t>(prev);
  unsigned prev_leading = 64U;
  unsigned prev_trailing = 0U;

  // Register-local 64-bit bit-buffer eliminates 1-5 non-inlined BitReader
  // calls per double on TPC-H Q01/Q06/Q19/Q20.
  uint64_t buf = 0;
  unsigned bits_in_buf = 0;
  size_t bp = 8U;
  auto refill = [&]() noexcept {
    if constexpr (std::endian::native == std::endian::little) {
      if (bp + 8U <= size) {
        uint64_t word = 0;
        std::memcpy(&word, data + bp, sizeof(word));
        const unsigned empty_bytes = (64U - bits_in_buf) >> 3U;
        buf |= (word << bits_in_buf);
        bp += empty_bytes;
        bits_in_buf += empty_bytes << 3U;
        return;
      }
    }
    while (bits_in_buf <= 56U && bp < size) {
      buf |= static_cast<uint64_t>(data[bp++]) << bits_in_buf;
      bits_in_buf += 8U;
    }
  };
  auto read_sig = [&](unsigned sig_bits, uint64_t* sig_out) noexcept -> bool {
    if (sig_bits <= 56U) {
      if (bits_in_buf < sig_bits) {
        refill();
        if (bits_in_buf < sig_bits) {
          return false;
        }
      }
      const uint64_t mask = (uint64_t{1} << sig_bits) - 1ULL;
      *sig_out = buf & mask;
      buf >>= sig_bits;
      bits_in_buf -= sig_bits;
      return true;
    }
    if (bits_in_buf < 32U) {
      refill();
      if (bits_in_buf < 32U) {
        return false;
      }
    }
    const uint64_t lo = buf & 0xFFFFFFFFULL;
    buf >>= 32U;
    bits_in_buf -= 32U;
    const unsigned hi_bits = sig_bits - 32U;
    if (bits_in_buf < hi_bits) {
      refill();
      if (bits_in_buf < hi_bits) {
        return false;
      }
    }
    const uint64_t hi_mask =
        hi_bits == 32U ? 0xFFFFFFFFULL : ((uint64_t{1} << hi_bits) - 1ULL);
    const uint64_t hi = buf & hi_mask;
    buf >>= hi_bits;
    bits_in_buf -= hi_bits;
    *sig_out = lo | (hi << 32U);
    return true;
  };

  for (uint32_t r = 1; r < n; ++r) {
    if (bits_in_buf < 14U) {
      refill();
    }
    if (bits_in_buf == 0U) {
      return false;
    }
    const uint64_t bit0 = buf & 1ULL;
    buf >>= 1U;
    --bits_in_buf;
    if (bit0 == 0ULL) {
      out[r] = static_cast<int64_t>(prev);
      continue;
    }
    if (bits_in_buf == 0U) {
      return false;
    }
    const uint64_t bit1 = buf & 1ULL;
    buf >>= 1U;
    --bits_in_buf;
    uint64_t x = 0;
    if (bit1 == 0ULL) {
      if (prev_leading >= 64U || prev_leading + prev_trailing >= 64U) {
        return false;
      }
      const unsigned sig_bits = 64U - prev_leading - prev_trailing;
      uint64_t sig = 0;
      if (!read_sig(sig_bits, &sig)) {
        return false;
      }
      x = sig << prev_trailing;
    } else {
      if (bits_in_buf < 12U) {
        refill();
        if (bits_in_buf < 12U) {
          return false;
        }
      }
      const auto lead = static_cast<unsigned>(buf & 0x3FULL);
      const auto sig_bits = static_cast<unsigned>(((buf >> 6U) & 0x3FULL) + 1U);
      buf >>= 12U;
      bits_in_buf -= 12U;
      if (lead + sig_bits > 64U) {
        return false;
      }
      const unsigned trailing = 64U - lead - sig_bits;
      uint64_t sig = 0;
      if (!read_sig(sig_bits, &sig)) {
        return false;
      }
      x = sig << trailing;
      prev_leading = lead;
      prev_trailing = trailing;
    }
    prev ^= x;
    out[r] = static_cast<int64_t>(prev);
  }
  return true;
}

// ============================================================================
// 6. Phrase Table Compression (Compression/PhraseTables.lean)
// ============================================================================

bool TryEncodeVarCharPhraseTable(std::span<const char> bytes,
                                 std::span<const uint32_t> ends,
                                 const uint8_t* nulls, uint32_t n,
                                 std::vector<uint8_t>* out) {
  out->clear();
  if (n < 16 || ends.size() != n || bytes.size() < 256) {
    return false;
  }
  const size_t raw_total = bytes.size() + (static_cast<size_t>(n) * 4ULL);

  // Sample up to 512 non-null rows to build a 2..8-byte phrase histogram.
  std::unordered_map<std::string_view, uint32_t> freq;
  freq.reserve(4096);
  const uint32_t step = std::max<uint32_t>(1U, n / 512U);
  for (uint32_t r = 0; r < n; r += step) {
    if (nulls != nullptr && nulls[r] != 0) {
      continue;
    }
    const uint32_t begin = (r == 0) ? 0U : ends[r - 1];
    const uint32_t end = ends[r];
    if (end < begin || end > bytes.size()) {
      return false;
    }
    const uint32_t len = end - begin;
    const uint32_t cap = std::min<uint32_t>(len, 128U);
    for (uint32_t pos = 0; pos + 2U <= cap; ++pos) {
      for (uint32_t plen :
           {uint32_t{2}, uint32_t{3}, uint32_t{4}, uint32_t{6}, uint32_t{8}}) {
        if (pos + plen <= cap) {
          ++freq[std::string_view(bytes.data() + begin + pos, plen)];
        }
      }
    }
  }
  if (freq.empty()) {
    return false;
  }
  struct Cand {
    std::string_view sv;
    int64_t gain{0};
  };
  std::vector<Cand> cands;
  cands.reserve(freq.size());
  for (const auto& [sv, cnt] : freq) {
    if (cnt < 3U) {
      continue;
    }
    const int64_t gain =
        (static_cast<int64_t>(cnt) * static_cast<int64_t>(sv.size() - 1U) *
         static_cast<int64_t>(step)) -
        static_cast<int64_t>(sv.size() + 1U);
    if (gain > 16) {
      cands.push_back(Cand{.sv = sv, .gain = gain});
    }
  }
  if (cands.empty()) {
    return false;
  }
  const size_t keep = std::min<size_t>(cands.size(), 255U);
  std::nth_element(
      cands.begin(), cands.begin() + static_cast<std::ptrdiff_t>(keep),
      cands.end(),
      [](const Cand& a, const Cand& b) { return a.gain > b.gain; });
  cands.resize(keep);
  std::ranges::sort(cands, [](const Cand& a, const Cand& b) {
    return a.sv.size() > b.sv.size();
  });

  // Lookup table indexed by first byte -> list of (symbol_id, sv).
  std::array<std::vector<std::pair<uint8_t, std::string_view>>, 256> by_first;
  for (size_t i = 0; i < cands.size(); ++i) {
    const auto b0 = static_cast<uint8_t>(cands[i].sv[0]);
    by_first[b0].emplace_back(static_cast<uint8_t>(i), cands[i].sv);
  }

  // Header: [u8 num_phrases][for each: u8 len, bytes...]
  out->reserve((bytes.size() / 2) + 256);
  out->push_back(static_cast<uint8_t>(cands.size()));
  for (const Cand& c : cands) {
    out->push_back(static_cast<uint8_t>(c.sv.size()));
    const auto* p = reinterpret_cast<const uint8_t*>(c.sv.data());
    out->insert(out->end(), p, p + c.sv.size());
  }

  // Exact DP shortest parse (`PhraseTables.sound_OptimalParse`) per row.
  std::vector<uint32_t> dp_cost;
  std::vector<uint16_t> dp_choice;  // 0..254 = phrase id, 255 = literal byte
  std::vector<uint8_t> row_buf;
  uint32_t prev_end = 0;
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t cur_end = ends[r];
    if (cur_end < prev_end || cur_end > bytes.size()) {
      out->clear();
      return false;
    }
    if (nulls != nullptr && nulls[r] != 0) {
      AppendVarU32(out, 0U);
      prev_end = cur_end;
      continue;
    }
    const std::string_view sv(bytes.data() + prev_end, cur_end - prev_end);
    const size_t len = sv.size();
    dp_cost.assign(len + 1U, UINT32_MAX);
    dp_choice.assign(len + 1U, 255U);
    dp_cost[len] = 0U;
    for (size_t i = len; i-- > 0;) {
      // Option 1: literal escape (0xFF + byte) = 2 bytes.
      uint32_t best = dp_cost[i + 1U] + 2U;
      uint16_t choice = 255U;
      const auto b0 = static_cast<uint8_t>(sv[i]);
      for (const auto& [sym, psv] : by_first[b0]) {
        const size_t plen = psv.size();
        if (i + plen <= len && sv.substr(i, plen) == psv) {
          const uint32_t cost = dp_cost[i + plen] + 1U;
          if (cost < best) {
            best = cost;
            choice = sym;
          }
        }
      }
      dp_cost[i] = best;
      dp_choice[i] = choice;
    }
    row_buf.clear();
    row_buf.reserve(dp_cost[0]);
    for (size_t i = 0; i < len;) {
      const uint16_t ch = dp_choice[i];
      if (ch == 255U) {
        row_buf.push_back(0xFFU);
        row_buf.push_back(static_cast<uint8_t>(sv[i]));
        ++i;
      } else {
        row_buf.push_back(static_cast<uint8_t>(ch));
        i += cands[ch].sv.size();
      }
    }
    AppendVarU32(out, static_cast<uint32_t>(row_buf.size()));
    out->insert(out->end(), row_buf.begin(), row_buf.end());
    if (out->size() >= raw_total) {
      out->clear();
      return false;
    }
    prev_end = cur_end;
  }
  return out->size() < raw_total;
}

namespace {

template <typename ByteT>
bool DecodeVarCharPhraseTableImpl(std::span<const uint8_t> encoded,
                                  const uint8_t* nulls, uint32_t n,
                                  std::vector<ByteT>* bytes_out,
                                  std::vector<uint32_t>* ends_out,
                                  bool lengths_only) {
  if (encoded.empty()) {
    return false;
  }
  size_t pos = 0;
  const uint8_t num_phrases = encoded[pos++];
  std::array<std::string_view, 256> phrases{};
  for (uint8_t i = 0; i < num_phrases; ++i) {
    if (pos >= encoded.size()) {
      return false;
    }
    const uint8_t plen = encoded[pos++];
    if (plen == 0 || plen > encoded.size() - pos) {
      return false;
    }
    phrases[i] = std::string_view(
        reinterpret_cast<const char*>(encoded.data() + pos), plen);
    pos += plen;
  }
  bytes_out->clear();
  if (!lengths_only) {
    bytes_out->reserve(encoded.size() * 2U);
  }
  ends_out->resize(n);
  uint32_t total = 0;
  for (uint32_t r = 0; r < n; ++r) {
    uint32_t enc_len = 0;
    if (!ReadVarU32(encoded, &pos, &enc_len) ||
        enc_len > encoded.size() - pos) {
      return false;
    }
    const size_t row_end = pos + enc_len;
    if (nulls != nullptr && nulls[r] != 0) {
      pos = row_end;
      (*ends_out)[r] = total;
      continue;
    }
    while (pos < row_end) {
      const uint8_t tok = encoded[pos++];
      if (tok == 0xFFU) {
        if (pos >= row_end) {
          return false;
        }
        const uint8_t lit = encoded[pos++];
        if (!lengths_only) {
          bytes_out->push_back(static_cast<ByteT>(lit));
        }
        ++total;
      } else {
        if (tok >= num_phrases) {
          return false;
        }
        const std::string_view sv = phrases[tok];
        if (!lengths_only) {
          const size_t cur = bytes_out->size();
          bytes_out->resize(cur + sv.size());
          std::memcpy(bytes_out->data() + cur, sv.data(), sv.size());
        }
        total += static_cast<uint32_t>(sv.size());
      }
    }
    (*ends_out)[r] = total;
  }
  return pos == encoded.size();
}

}  // namespace

bool DecodeVarCharPhraseTable(std::span<const uint8_t> encoded,
                              const uint8_t* nulls, uint32_t n,
                              std::vector<char>* bytes_out,
                              std::vector<uint32_t>* ends_out) {
  return DecodeVarCharPhraseTableImpl(encoded, nulls, n, bytes_out, ends_out,
                                      /*lengths_only=*/false);
}

bool DecodeVarCharPhraseTableBytes(std::span<const uint8_t> encoded,
                                   const uint8_t* nulls, uint32_t n,
                                   std::vector<uint8_t>* bytes_out,
                                   std::vector<uint32_t>* ends_out,
                                   bool lengths_only) {
  return DecodeVarCharPhraseTableImpl(encoded, nulls, n, bytes_out, ends_out,
                                      lengths_only);
}

// ============================================================================
// 7. Zstandard Per-Block Frame Container (Compression/Container.lean)
// ============================================================================

bool HasZstdSupport() noexcept {
#ifdef TINYLAMB_HAS_ZSTD
  return true;
#else
  return false;
#endif
}

#ifdef TINYLAMB_HAS_ZSTD
namespace {

struct ThreadLocalZstdCCtx {
  ZSTD_CCtx* ctx{nullptr};
  ThreadLocalZstdCCtx() = default;
  ThreadLocalZstdCCtx(const ThreadLocalZstdCCtx&) = delete;
  ThreadLocalZstdCCtx& operator=(const ThreadLocalZstdCCtx&) = delete;
  ThreadLocalZstdCCtx(ThreadLocalZstdCCtx&&) = delete;
  ThreadLocalZstdCCtx& operator=(ThreadLocalZstdCCtx&&) = delete;
  ~ThreadLocalZstdCCtx() {
    if (ctx != nullptr) {
      ZSTD_freeCCtx(ctx);
    }
  }
  ZSTD_CCtx* Get() noexcept {
    if (ctx == nullptr) {
      ctx = ZSTD_createCCtx();
    }
    return ctx;
  }
};

struct ThreadLocalZstdDCtx {
  ZSTD_DCtx* ctx{nullptr};
  ThreadLocalZstdDCtx() = default;
  ThreadLocalZstdDCtx(const ThreadLocalZstdDCtx&) = delete;
  ThreadLocalZstdDCtx& operator=(const ThreadLocalZstdDCtx&) = delete;
  ThreadLocalZstdDCtx(ThreadLocalZstdDCtx&&) = delete;
  ThreadLocalZstdDCtx& operator=(ThreadLocalZstdDCtx&&) = delete;
  ~ThreadLocalZstdDCtx() {
    if (ctx != nullptr) {
      ZSTD_freeDCtx(ctx);
    }
  }
  ZSTD_DCtx* Get() noexcept {
    if (ctx == nullptr) {
      ctx = ZSTD_createDCtx();
    }
    return ctx;
  }
};

}  // namespace
#endif

bool TryCompressZstdFrame(std::span<const uint8_t> payload, uint8_t inner_tag,
                          uint8_t inner_subtag, int level,
                          std::vector<uint8_t>* out) {
  out->clear();
#ifdef TINYLAMB_HAS_ZSTD
  if (payload.size() < 32U ||
      payload.size() > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  const size_t bound = ZSTD_compressBound(payload.size());
  // 10-byte header: [u8 inner_tag][u8 inner_subtag][u32 orig_len][u32 crc32c]
  constexpr size_t kHdr = 10U;
  out->resize(kHdr + bound);
  thread_local ThreadLocalZstdCCtx tl_cctx;
  ZSTD_CCtx* cctx = tl_cctx.Get();
  const size_t csize =
      cctx != nullptr ? ZSTD_compressCCtx(cctx, out->data() + kHdr, bound,
                                          payload.data(), payload.size(), level)
                      : ZSTD_compress(out->data() + kHdr, bound, payload.data(),
                                      payload.size(), level);
  if (ZSTD_isError(csize) != 0 || kHdr + csize >= payload.size()) {
    out->clear();
    return false;
  }
  (*out)[0] = inner_tag;
  (*out)[1] = inner_subtag;
  const auto orig_len = static_cast<uint32_t>(payload.size());
  const uint32_t crc = Crc32C(payload.data(), payload.size());
  std::memcpy(out->data() + 2U, &orig_len, 4U);
  std::memcpy(out->data() + 6U, &crc, 4U);
  out->resize(kHdr + csize);
  return true;
#else
  (void)payload;
  (void)inner_tag;
  (void)inner_subtag;
  (void)level;
  return false;
#endif
}

bool DecompressZstdFrameRaw(std::span<const uint8_t> encoded,
                            uint8_t* inner_tag_out, uint8_t* inner_subtag_out,
                            std::span<const uint8_t>* payload_out) {
  *payload_out = {};
#ifdef TINYLAMB_HAS_ZSTD
  constexpr size_t kHdr = 10U;
  if (encoded.size() <= kHdr) {
    return false;
  }
  const uint8_t inner_tag = encoded[0];
  const uint8_t inner_subtag = encoded[1];
  uint32_t orig_len = 0;
  uint32_t expected_crc = 0;
  std::memcpy(&orig_len, encoded.data() + 2U, 4U);
  std::memcpy(&expected_crc, encoded.data() + 6U, 4U);
  if (orig_len == 0 || orig_len > (256U << 20U)) {
    return false;
  }
  const unsigned long long frame_size =
      ZSTD_getFrameContentSize(encoded.data() + kHdr, encoded.size() - kHdr);
  if (frame_size == ZSTD_CONTENTSIZE_ERROR ||
      frame_size == ZSTD_CONTENTSIZE_UNKNOWN || frame_size != orig_len) {
    return false;
  }
  thread_local RawByteScratch tl_buf;
  tl_buf.TrimIfLarge();
  tl_buf.Ensure(orig_len);
  thread_local ThreadLocalZstdDCtx tl_dctx;
  ZSTD_DCtx* dctx = tl_dctx.Get();
  const size_t dsize =
      dctx != nullptr
          ? ZSTD_decompressDCtx(dctx, tl_buf.data.get(), orig_len,
                                encoded.data() + kHdr, encoded.size() - kHdr)
          : ZSTD_decompress(tl_buf.data.get(), orig_len, encoded.data() + kHdr,
                            encoded.size() - kHdr);
  if (ZSTD_isError(dsize) != 0 || dsize != orig_len) {
    return false;
  }
  if (Crc32C(tl_buf.data.get(), orig_len) != expected_crc) {
    return false;
  }
  if (inner_tag_out != nullptr) {
    *inner_tag_out = inner_tag;
  }
  if (inner_subtag_out != nullptr) {
    *inner_subtag_out = inner_subtag;
  }
  *payload_out = std::span<const uint8_t>(tl_buf.data.get(), orig_len);
  return true;
#else
  (void)encoded;
  (void)inner_tag_out;
  (void)inner_subtag_out;
  return false;
#endif
}

bool DecompressZstdFrame(std::span<const uint8_t> encoded,
                         uint8_t* inner_tag_out, uint8_t* inner_subtag_out,
                         std::vector<uint8_t>* payload_out) {
  payload_out->clear();
  std::span<const uint8_t> raw;
  if (!DecompressZstdFrameRaw(encoded, inner_tag_out, inner_subtag_out, &raw)) {
    return false;
  }
  payload_out->assign(raw.begin(), raw.end());
  return true;
}

bool TryEncodeZstdVarChar(std::span<const char> bytes,
                          std::span<const uint32_t> ends, const uint8_t* nulls,
                          uint32_t n, size_t max_bytes, int level,
                          std::vector<uint8_t>* out) {
  out->clear();
#ifdef TINYLAMB_HAS_ZSTD
  if (n == 0 || ends.size() != n) {
    return false;
  }
  std::vector<uint8_t> packed;
  packed.reserve(bytes.size() + (static_cast<size_t>(n) * 2U));
  uint32_t prev_end = 0;
  for (uint32_t r = 0; r < n; ++r) {
    const uint32_t cur_end = ends[r];
    if (cur_end < prev_end || cur_end > bytes.size()) {
      return false;
    }
    const uint32_t len =
        (nulls != nullptr && nulls[r] != 0) ? 0U : (cur_end - prev_end);
    AppendVarU32(&packed, len);
    if (len > 0) {
      const auto* p = reinterpret_cast<const uint8_t*>(bytes.data() + prev_end);
      packed.insert(packed.end(), p, p + len);
    }
    prev_end = cur_end;
  }
  if (!TryCompressZstdFrame(packed, /*inner_tag=*/0U, /*inner_subtag=*/0U,
                            level, out)) {
    return false;
  }
  if (out->size() >= max_bytes) {
    out->clear();
    return false;
  }
  return true;
#else
  (void)bytes;
  (void)ends;
  (void)nulls;
  (void)n;
  (void)max_bytes;
  (void)level;
  return false;
#endif
}

namespace {

template <typename ByteT>
bool DecodeZstdVarCharUnpackedImpl(std::span<const uint8_t> packed,
                                   const uint8_t* nulls, uint32_t n,
                                   std::vector<ByteT>* bytes_out,
                                   std::vector<uint32_t>* ends_out,
                                   bool lengths_only,
                                   std::span<const uint32_t> selected_rows) {
  ends_out->resize(n);
  uint32_t* const ends_dst = ends_out->data();
  const uint8_t* const base = packed.data();
  const size_t packed_size = packed.size();
  size_t pos = 0;

  if (lengths_only) {
    bytes_out->clear();
    uint32_t total = 0;
    for (uint32_t r = 0; r < n; ++r) {
      uint32_t len = 0;
      if (pos < packed_size && base[pos] < 0x80U) {
        len = base[pos++];
      } else if (!ReadVarU32(packed, &pos, &len)) {
        return false;
      }
      if (len > packed_size - pos) {
        return false;
      }
      if (nulls == nullptr || nulls[r] == 0) {
        total += len;
      }
      pos += len;
      ends_dst[r] = total;
    }
    return pos == packed_size;
  }

  if (!selected_rows.empty()) {
    bytes_out->clear();
    size_t sel_idx = 0;
    uint32_t written = 0;
    for (uint32_t r = 0; r < n; ++r) {
      uint32_t len = 0;
      if (pos < packed_size && base[pos] < 0x80U) {
        len = base[pos++];
      } else if (!ReadVarU32(packed, &pos, &len)) {
        bytes_out->clear();
        return false;
      }
      if (len > packed_size - pos) {
        bytes_out->clear();
        return false;
      }
      if (sel_idx < selected_rows.size() && selected_rows[sel_idx] == r) {
        if ((nulls == nullptr || nulls[r] == 0) && len > 0) {
          const size_t cur = bytes_out->size();
          bytes_out->resize(cur + len);
          std::memcpy(bytes_out->data() + cur, base + pos, len);
          written += len;
        }
        ++sel_idx;
      }
      pos += len;
      ends_dst[r] = written;
    }
    return pos == packed_size;
  }

  // Decoded bytes are always <= packed.size() (each row has >= 1 varint byte).
  bytes_out->resize(packed_size);
  ByteT* dst = bytes_out->data();
  uint32_t written = 0;
  for (uint32_t r = 0; r < n; ++r) {
    uint32_t len = 0;
    if (pos < packed_size && base[pos] < 0x80U) {
      len = base[pos++];
    } else if (!ReadVarU32(packed, &pos, &len)) {
      bytes_out->clear();
      return false;
    }
    if (len > packed_size - pos) {
      bytes_out->clear();
      return false;
    }
    if (nulls == nullptr || nulls[r] == 0) {
      if (len > 0) {
        std::memcpy(dst + written, base + pos, len);
        written += len;
      }
    }
    pos += len;
    ends_dst[r] = written;
  }
  bytes_out->resize(written);
  return pos == packed_size;
}

}  // namespace

bool DecodeZstdVarChar(std::span<const uint8_t> encoded, const uint8_t* nulls,
                       uint32_t n, std::vector<char>* bytes_out,
                       std::vector<uint32_t>* ends_out) {
  uint8_t tag = 0;
  uint8_t subtag = 0;
  std::span<const uint8_t> packed;
  if (!DecompressZstdFrameRaw(encoded, &tag, &subtag, &packed)) {
    return false;
  }
  return DecodeZstdVarCharUnpackedImpl(packed, nulls, n, bytes_out, ends_out,
                                       /*lengths_only=*/false, {});
}

bool DecodeZstdVarCharUnpackedBytes(std::span<const uint8_t> packed,
                                    const uint8_t* nulls, uint32_t n,
                                    std::vector<uint8_t>* bytes_out,
                                    std::vector<uint32_t>* ends_out,
                                    bool lengths_only,
                                    std::span<const uint32_t> selected_rows) {
  return DecodeZstdVarCharUnpackedImpl(packed, nulls, n, bytes_out, ends_out,
                                       lengths_only, selected_rows);
}

bool MatchZstdVarCharUnpackedContains(
    std::span<const uint8_t> packed, const uint8_t* nulls, uint32_t n,
    std::string_view needle, std::vector<uint32_t>* matching_rows_out) {
  matching_rows_out->clear();
  if (needle.empty()) {
    matching_rows_out->reserve(n);
    for (uint32_t r = 0; r < n; ++r) {
      if (nulls == nullptr || nulls[r] == 0) {
        matching_rows_out->push_back(r);
      }
    }
    return true;
  }
  const uint8_t* const base = packed.data();
  const size_t packed_size = packed.size();
  const size_t nlen = needle.size();
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Exec_ConcatSearch.lean
  // (sound): search the packed byte stream once per hit and jump straight to
  // the next byte-stream occurrence, skipping FastMemmem on non-hitting rows
  // and rejecting cross-row boundary matches via `hit_off + nlen <= row_end`.
  const uint8_t* next_hit_ptr =
      FastMemmem(base, packed_size, needle.data(), nlen);
  if (next_hit_ptr == nullptr) {
    return true;
  }
  auto next_hit = static_cast<size_t>(next_hit_ptr - base);
  size_t pos = 0;
  for (uint32_t r = 0; r < n; ++r) {
    uint32_t len = 0;
    if (pos < packed_size && base[pos] < 0x80U) {
      len = base[pos++];
    } else if (!ReadVarU32(packed, &pos, &len)) {
      matching_rows_out->clear();
      return false;
    }
    if (len > packed_size - pos) {
      matching_rows_out->clear();
      return false;
    }
    const size_t row_end = pos + len;
    if (next_hit < row_end) {
      while (next_hit < pos) {
        // Previous hit started inside a length varint or spanned a prior row
        // boundary; advance to the next occurrence at or after `pos`.
        next_hit_ptr =
            FastMemmem(base + pos, packed_size - pos, needle.data(), nlen);
        if (next_hit_ptr == nullptr) {
          next_hit = packed_size;
          break;
        }
        next_hit = static_cast<size_t>(next_hit_ptr - base);
      }
      if (next_hit < row_end) {
        if (next_hit + nlen <= row_end) {
          if (nulls == nullptr || nulls[r] == 0) {
            matching_rows_out->push_back(r);
          }
          if (row_end < packed_size) {
            next_hit_ptr = FastMemmem(base + row_end, packed_size - row_end,
                                      needle.data(), nlen);
            next_hit = next_hit_ptr == nullptr
                           ? packed_size
                           : static_cast<size_t>(next_hit_ptr - base);
          } else {
            next_hit = packed_size;
          }
        } else if (next_hit + 1 < packed_size) {
          // Cross-row match: advance past `next_hit` so a match starting
          // later in the next row is not skipped.
          next_hit_ptr =
              FastMemmem(base + next_hit + 1, packed_size - (next_hit + 1),
                         needle.data(), nlen);
          next_hit = next_hit_ptr == nullptr
                         ? packed_size
                         : static_cast<size_t>(next_hit_ptr - base);
        } else {
          next_hit = packed_size;
        }
      }
    }
    pos = row_end;
  }
  return pos == packed_size;
}

// ============================================================================
// 8. ClickstreamLayout.Storage: TieOrientation, ClockWindow, HashExceptions
// ============================================================================

namespace {

size_t UVarIntBytes(uint64_t v) noexcept {
  size_t n = 1;
  while (v >= 0x80ULL) {
    v >>= 7U;
    ++n;
  }
  return n;
}

size_t SVarIntBytes(int64_t v) noexcept {
  // Zigzag encoding: (v << 1) ^ (v >> 63).
  const auto uv = static_cast<uint64_t>(v);
  const uint64_t zz = (uv << 1U) ^ (v < 0 ? ~uint64_t{0} : uint64_t{0});
  return UVarIntBytes(zz);
}

size_t FrontCodePrice(std::string_view prev, std::string_view cur) noexcept {
  const size_t lim = std::min(prev.size(), cur.size());
  size_t lcp = 0;
  while (lcp < lim && prev[lcp] == cur[lcp]) {
    ++lcp;
  }
  const size_t suffix = cur.size() - lcp;
  return UVarIntBytes(lcp) + UVarIntBytes(suffix) + suffix;
}

size_t ChargeGroupOrder(std::string_view prev,
                        const std::vector<std::string_view>& group,
                        bool rev) noexcept {
  size_t total = 0;
  std::string_view cur_prev = prev;
  if (!rev) {
    for (const std::string_view w : group) {
      total += FrontCodePrice(cur_prev, w);
      cur_prev = w;
    }
  } else {
    for (size_t i = group.size(); i-- > 0;) {
      total += FrontCodePrice(cur_prev, group[i]);
      cur_prev = group[i];
    }
  }
  return total;
}

}  // namespace

TieOrientationPlan ChooseOptimalTieOrientations(
    std::span<const std::vector<std::string_view>> groups) {
  TieOrientationPlan plan;
  plan.reversed.assign(groups.size(), 0);
  // Two-state DP: state 0 = previous group forward, state 1 = previous group
  // reversed. Initial state before group 0 has empty word "" at cost 0.
  std::array<std::string_view, 2> prev_word{"", ""};
  std::array<size_t, 2> prev_cost{0, 0};
  std::vector<std::array<uint8_t, 2>> parent(groups.size(), {0, 0});
  bool has_nonempty = false;

  for (size_t g = 0; g < groups.size(); ++g) {
    if (groups[g].empty()) {
      parent[g] = {0, 1};
      continue;
    }
    has_nonempty = true;
    std::array<size_t, 2> next_cost{0, 0};
    std::array<std::string_view, 2> next_word{groups[g].back(),
                                              groups[g].front()};
    for (size_t orient = 0; orient < 2; ++orient) {
      const bool rev = (orient == 1);
      const size_t c0 =
          prev_cost[0] + ChargeGroupOrder(prev_word[0], groups[g], rev);
      const size_t c1 =
          prev_cost[1] + ChargeGroupOrder(prev_word[1], groups[g], rev);
      if (c1 < c0) {
        next_cost[orient] = c1;
        parent[g][orient] = 1;
      } else {
        next_cost[orient] = c0;
        parent[g][orient] = 0;
      }
    }
    prev_cost = next_cost;
    prev_word = next_word;
  }

  if (!has_nonempty) {
    return plan;
  }
  size_t cur_state = (prev_cost[1] < prev_cost[0]) ? 1U : 0U;
  plan.min_bytes = prev_cost[cur_state];
  for (size_t g = groups.size(); g-- > 0;) {
    if (!groups[g].empty()) {
      plan.reversed[g] = static_cast<uint8_t>(cur_state);
      cur_state = parent[g][cur_state];
    }
  }
  return plan;
}

ClockWindowPlan ChooseOptimalClockWindow(std::span<const int64_t> deltas,
                                         unsigned bit_width,
                                         size_t exception_bytes) {
  const size_t packed_bytes =
      ((deltas.size() * static_cast<size_t>(bit_width)) + 7U) / 8U;
  const __int128 span =
      (bit_width == 0)
          ? __int128{0}
          : ((__int128{1} << std::min<unsigned>(bit_width, 62U)) - 1);
  constexpr auto kMinI64 =
      static_cast<__int128>(std::numeric_limits<int64_t>::min());
  constexpr auto kMaxI64 =
      static_cast<__int128>(std::numeric_limits<int64_t>::max());

  std::vector<int64_t> anchors;
  anchors.reserve(1U + (deltas.size() * 2U));
  anchors.push_back(0);
  for (int64_t d : deltas) {
    anchors.push_back(d);
    const __int128 lo_anchor = static_cast<__int128>(d) - span;
    if (lo_anchor >= kMinI64 && lo_anchor <= kMaxI64) {
      anchors.push_back(static_cast<int64_t>(lo_anchor));
    }
  }
  std::ranges::sort(anchors);
  const auto dup = std::ranges::unique(anchors);
  anchors.erase(dup.begin(), dup.end());

  ClockWindowPlan best{
      .base = 0,
      .total_bytes = std::numeric_limits<size_t>::max(),
      .exception_count = deltas.size(),
  };
  for (int64_t low : anchors) {
    const auto lo128 = static_cast<__int128>(low);
    const __int128 hi128 = lo128 + span;
    size_t misses = 0;
    for (int64_t d : deltas) {
      const auto d128 = static_cast<__int128>(d);
      if (d128 < lo128 || d128 > hi128) {
        ++misses;
      }
    }
    const size_t cost =
        1U + SVarIntBytes(low) + packed_bytes + (misses * exception_bytes);
    if (cost < best.total_bytes) {
      best.base = low;
      best.total_bytes = cost;
      best.exception_count = misses;
    }
  }
  return best;
}

uint64_t HashExceptionsTable::HashAt(uint32_t row,
                                     uint32_t code) const noexcept {
  const auto it = std::lower_bound(exceptions.begin(), exceptions.end(), row,
                                   [](const std::pair<uint32_t, uint64_t>& e,
                                      uint32_t r) { return e.first < r; });
  if (it != exceptions.end() && it->first == row) {
    return it->second;
  }
  if (code < mode_hash_by_code.size()) {
    return mode_hash_by_code[code];
  }
  return 0;
}

HashExceptionsTable EncodeHashExceptions(std::span<const uint32_t> codes,
                                         std::span<const uint64_t> hashes) {
  HashExceptionsTable tbl;
  const size_t n = std::min(codes.size(), hashes.size());
  if (n == 0) {
    return tbl;
  }
  uint32_t max_code = 0;
  for (size_t i = 0; i < n; ++i) {
    max_code = std::max(max_code, codes[i]);
  }
  std::vector<std::unordered_map<uint64_t, uint32_t>> freq(
      static_cast<size_t>(max_code) + 1U);
  for (size_t i = 0; i < n; ++i) {
    ++freq[codes[i]][hashes[i]];
  }
  tbl.mode_hash_by_code.assign(static_cast<size_t>(max_code) + 1U, 0);
  for (size_t c = 0; c < freq.size(); ++c) {
    uint64_t best_h = 0;
    uint32_t best_cnt = 0;
    for (const auto& [h, cnt] : freq[c]) {
      if (cnt > best_cnt) {
        best_cnt = cnt;
        best_h = h;
      }
    }
    tbl.mode_hash_by_code[c] = best_h;
  }
  for (size_t i = 0; i < n; ++i) {
    if (hashes[i] != tbl.mode_hash_by_code[codes[i]]) {
      tbl.exceptions.emplace_back(static_cast<uint32_t>(i), hashes[i]);
    }
  }
  return tbl;
}

}  // namespace tinylamb::tlcs_compression
