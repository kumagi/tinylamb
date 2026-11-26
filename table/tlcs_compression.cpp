/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/tlcs_compression.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
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

class BitReader {
 public:
  BitReader(std::span<const uint8_t> in, size_t byte_pos)
      : in_(in), byte_pos_(byte_pos) {}

  bool ReadBits(unsigned bit_count, uint64_t* out) {
    if (bit_count == 0) {
      *out = 0;
      return true;
    }
    if (bit_count > 64U) {
      return false;
    }
    uint64_t result = 0;
    unsigned filled = 0;
    while (filled < bit_count) {
      if (bits_in_buf_ == 0) {
        if (byte_pos_ >= in_.size()) {
          return false;
        }
        buf_ = in_[byte_pos_++];
        bits_in_buf_ = 8U;
      }
      const unsigned take = std::min(bits_in_buf_, bit_count - filled);
      const uint64_t mask = (uint64_t{1} << take) - 1ULL;
      result |= (buf_ & mask) << filled;
      buf_ >>= take;
      bits_in_buf_ -= take;
      filled += take;
    }
    *out = result;
    return true;
  }

 private:
  std::span<const uint8_t> in_;
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

inline void FormatTemporalAscii22(uint64_t packed, char* dst) {
  // Format `YYYYMMDDHHMMSS` into `YYYY-MM-DD HH:MM:SS+00`.
  static constexpr std::array<size_t, 14> kDigitIdx = {
      0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
  dst[4] = '-';
  dst[7] = '-';
  dst[10] = ' ';
  dst[13] = ':';
  dst[16] = ':';
  dst[19] = '+';
  dst[20] = '0';
  dst[21] = '0';
  for (int i = 13; i >= 0; --i) {
    dst[kDigitIdx[i]] = static_cast<char>('0' + (packed % 10ULL));
    packed /= 10ULL;
  }
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

bool DecodeTemporalText(std::span<const uint8_t> encoded, const uint8_t* nulls,
                        uint32_t n, std::vector<char>* bytes_out,
                        std::vector<uint32_t>* ends_out) {
  if (encoded.size() != static_cast<size_t>(n) * kTemporalTextPackedBytes) {
    return false;
  }
  bytes_out->clear();
  bytes_out->reserve(static_cast<size_t>(n) * kTemporalTextAsciiBytes);
  ends_out->resize(n);
  std::array<char, kTemporalTextAsciiBytes> buf{};
  for (uint32_t r = 0; r < n; ++r) {
    if (nulls != nullptr && nulls[r] != 0) {
      (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
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
    FormatTemporalAscii22(val, buf.data());
    bytes_out->insert(bytes_out->end(), buf.begin(), buf.end());
    (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
  }
  return true;
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

bool DecodeVarCharFrontDict(std::span<const uint8_t> encoded,
                            const uint8_t* nulls, uint32_t n,
                            std::vector<char>* bytes_out,
                            std::vector<uint32_t>* ends_out) {
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

  std::vector<std::string> dict(dict_size);
  for (uint32_t i = 0; i < dict_size; ++i) {
    uint32_t lcp = 0;
    uint32_t suffix_len = 0;
    if (!ReadVarU32(encoded, &pos, &lcp) ||
        !ReadVarU32(encoded, &pos, &suffix_len)) {
      return false;
    }
    if (i == 0 && lcp != 0) {
      return false;
    }
    if (i > 0 && lcp > dict[i - 1].size()) {
      return false;
    }
    if (suffix_len > encoded.size() - pos) {
      return false;
    }
    dict[i].reserve(static_cast<size_t>(lcp) + suffix_len);
    if (lcp > 0) {
      dict[i].append(dict[i - 1].data(), lcp);
    }
    if (suffix_len > 0) {
      dict[i].append(reinterpret_cast<const char*>(encoded.data() + pos),
                     suffix_len);
      pos += suffix_len;
    }
  }

  const size_t expected_id_bytes = (static_cast<size_t>(n) * bw + 7ULL) / 8ULL;
  if (encoded.size() - pos != expected_id_bytes) {
    return false;
  }

  bytes_out->clear();
  ends_out->resize(n);
  BitReader reader(encoded, pos);
  for (uint32_t r = 0; r < n; ++r) {
    uint64_t id = 0;
    if (bw > 0 && !reader.ReadBits(bw, &id)) {
      return false;
    }
    if (nulls != nullptr && nulls[r] != 0) {
      (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
      continue;
    }
    if (id >= dict_size) {
      return false;
    }
    const std::string& s = dict[static_cast<size_t>(id)];
    if (!s.empty()) {
      bytes_out->insert(bytes_out->end(), s.begin(), s.end());
    }
    (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
  }
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
  BitReader reader(encoded, 0);
  uint64_t prev = 0;
  if (!reader.ReadBits(64U, &prev)) {
    return false;
  }
  out[0] = static_cast<int64_t>(prev);
  unsigned prev_leading = 64U;
  unsigned prev_trailing = 0U;

  for (uint32_t r = 1; r < n; ++r) {
    uint64_t bit0 = 0;
    if (!reader.ReadBits(1U, &bit0)) {
      return false;
    }
    if (bit0 == 0ULL) {
      out[r] = static_cast<int64_t>(prev);
      continue;
    }
    uint64_t bit1 = 0;
    if (!reader.ReadBits(1U, &bit1)) {
      return false;
    }
    uint64_t x = 0;
    if (bit1 == 0ULL) {
      if (prev_leading >= 64U || prev_leading + prev_trailing >= 64U) {
        return false;
      }
      const unsigned sig_bits = 64U - prev_leading - prev_trailing;
      uint64_t sig = 0;
      if (!reader.ReadBits(sig_bits, &sig)) {
        return false;
      }
      x = sig << prev_trailing;
    } else {
      uint64_t lead = 0;
      uint64_t sig_minus_1 = 0;
      if (!reader.ReadBits(6U, &lead) || !reader.ReadBits(6U, &sig_minus_1)) {
        return false;
      }
      const unsigned sig_bits = static_cast<unsigned>(sig_minus_1) + 1U;
      if (lead + sig_bits > 64U) {
        return false;
      }
      const unsigned trailing = 64U - static_cast<unsigned>(lead) - sig_bits;
      uint64_t sig = 0;
      if (!reader.ReadBits(sig_bits, &sig)) {
        return false;
      }
      x = sig << trailing;
      prev_leading = static_cast<unsigned>(lead);
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

bool DecodeVarCharPhraseTable(std::span<const uint8_t> encoded,
                              const uint8_t* nulls, uint32_t n,
                              std::vector<char>* bytes_out,
                              std::vector<uint32_t>* ends_out) {
  if (encoded.empty()) {
    return false;
  }
  size_t pos = 0;
  const uint8_t num_phrases = encoded[pos++];
  std::vector<std::string_view> phrases(num_phrases);
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
  ends_out->resize(n);
  for (uint32_t r = 0; r < n; ++r) {
    uint32_t enc_len = 0;
    if (!ReadVarU32(encoded, &pos, &enc_len) ||
        enc_len > encoded.size() - pos) {
      return false;
    }
    const size_t row_end = pos + enc_len;
    if (nulls != nullptr && nulls[r] != 0) {
      pos = row_end;
      (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
      continue;
    }
    while (pos < row_end) {
      const uint8_t tok = encoded[pos++];
      if (tok == 0xFFU) {
        if (pos >= row_end) {
          return false;
        }
        bytes_out->push_back(static_cast<char>(encoded[pos++]));
      } else {
        if (tok >= num_phrases) {
          return false;
        }
        const std::string_view sv = phrases[tok];
        bytes_out->insert(bytes_out->end(), sv.begin(), sv.end());
      }
    }
    (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
  }
  return pos == encoded.size();
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
  const size_t csize = ZSTD_compress(out->data() + kHdr, bound, payload.data(),
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

bool DecompressZstdFrame(std::span<const uint8_t> encoded,
                         uint8_t* inner_tag_out, uint8_t* inner_subtag_out,
                         std::vector<uint8_t>* payload_out) {
  payload_out->clear();
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
  // Validate the Zstd frame header before allocating `orig_len` bytes so a
  // corrupt or fuzzed 15-byte header cannot trigger a 256 MiB OOM allocation.
  const unsigned long long frame_size =
      ZSTD_getFrameContentSize(encoded.data() + kHdr, encoded.size() - kHdr);
  if (frame_size == ZSTD_CONTENTSIZE_ERROR ||
      frame_size == ZSTD_CONTENTSIZE_UNKNOWN || frame_size != orig_len) {
    return false;
  }
  payload_out->resize(orig_len);
  const size_t dsize =
      ZSTD_decompress(payload_out->data(), orig_len, encoded.data() + kHdr,
                      encoded.size() - kHdr);
  if (ZSTD_isError(dsize) != 0 || dsize != orig_len) {
    payload_out->clear();
    return false;
  }
  if (Crc32C(payload_out->data(), orig_len) != expected_crc) {
    payload_out->clear();
    return false;
  }
  if (inner_tag_out != nullptr) {
    *inner_tag_out = inner_tag;
  }
  if (inner_subtag_out != nullptr) {
    *inner_subtag_out = inner_subtag;
  }
  return true;
#else
  (void)encoded;
  (void)inner_tag_out;
  (void)inner_subtag_out;
  return false;
#endif
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

bool DecodeZstdVarChar(std::span<const uint8_t> encoded, const uint8_t* nulls,
                       uint32_t n, std::vector<char>* bytes_out,
                       std::vector<uint32_t>* ends_out) {
  uint8_t tag = 0;
  uint8_t subtag = 0;
  std::vector<uint8_t> packed;
  if (!DecompressZstdFrame(encoded, &tag, &subtag, &packed)) {
    return false;
  }
  bytes_out->clear();
  ends_out->resize(n);
  size_t pos = 0;
  for (uint32_t r = 0; r < n; ++r) {
    uint32_t len = 0;
    if (!ReadVarU32(packed, &pos, &len) || len > packed.size() - pos) {
      return false;
    }
    if (nulls == nullptr || nulls[r] == 0) {
      if (len > 0) {
        bytes_out->insert(
            bytes_out->end(),
            reinterpret_cast<const char*>(packed.data() + pos),
            reinterpret_cast<const char*>(packed.data() + pos + len));
      }
    }
    pos += len;
    (*ends_out)[r] = static_cast<uint32_t>(bytes_out->size());
  }
  return pos == packed.size();
}

// ============================================================================
// 8. ClickBenchLayout.Storage: TieOrientation, ClockWindow, HashExceptions
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
