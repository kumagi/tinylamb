/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Pure in-process libFuzzer for all TLCS v3/v4 column compression codecs
// (table/tlcs_compression.hpp):
//   1. Compact nullmap: TryEncodeCompactNulls / DecodeCompactNulls /
//   GatherCompactNulls
//   2. TemporalText 47-bit timestamp packing: TryEncode / Decode / DecodeRow
//   3. Sorted front-coded VARCHAR block dictionary: TryEncode / Decode
//   4. Bit-packed strided Frame-of-Reference: TryEncode / Decode / Gather
//   5. Bit-exact XOR-chain IEEE-754 doubles: TryEncode / Decode
//   6. FSST-style VARCHAR phrase table: TryEncode / Decode
//   7. Zstd frame container + Zstd VARCHAR: TryCompress / Decompress + CRC
//   tamper
//
// Each codec is verified for:
//   - Lossless round-trip (including sparse random-access Gather parity)
//   - Corrupt / truncated / bit-flipped decode resilience (must return false
//   cleanly)

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "table/tlcs_compression.hpp"

namespace tinylamb::fuzz {
namespace {

using namespace tinylamb::tlcs_compression;

struct Cursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};

  [[nodiscard]] size_t Remaining() const noexcept {
    return pos < size ? size - pos : 0;
  }

  uint8_t NextU8() noexcept { return pos < size ? data[pos++] : 0U; }

  uint32_t NextU32() noexcept {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
      v = (v << 8U) | NextU8();
    }
    return v;
  }

  int64_t NextI64() noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
      v = (v << 8U) | NextU8();
    }
    return static_cast<int64_t>(v);
  }
};

[[noreturn]] void Fail(const char* codec, const char* msg) {
  (void)std::fprintf(stderr, "[TLCS-COMP-FUZZ FAIL][%s] %s\n", codec, msg);
  (void)std::fflush(stderr);
  std::abort();
}

void FuzzCompactNulls(Cursor& c) {
  const uint32_t n = 1U + (c.NextU8() % 128U);
  const uint8_t density_mode = c.NextU8() % 4U;
  std::vector<uint8_t> nulls(n, 0);
  for (uint32_t i = 0; i < n; ++i) {
    const uint8_t b = c.NextU8();
    if (density_mode == 0) {
      nulls[i] = (b % 16U == 0) ? 1U : 0U;  // sparse nulls
    } else if (density_mode == 1) {
      nulls[i] = (b % 16U == 0) ? 0U : 1U;  // sparse non-nulls
    } else if (density_mode == 2) {
      nulls[i] = b & 1U;  // dense 50/50
    } else {
      nulls[i] = (b & 3U) == 0 ? 1U : 0U;
    }
  }

  std::vector<uint8_t> encoded;
  const bool compressed = TryEncodeCompactNulls(nulls, &encoded);
  if (compressed && encoded.size() >= n) {
    Fail("CompactNulls",
         "TryEncodeCompactNulls returned true without shrinking");
  }
  const std::span<const uint8_t> payload =
      compressed ? std::span<const uint8_t>(encoded)
                 : std::span<const uint8_t>(nulls);

  std::vector<uint8_t> decoded;
  if (!DecodeCompactNulls(payload, n, &decoded) || decoded != nulls) {
    Fail("CompactNulls", "DecodeCompactNulls mismatch");
  }

  // Sparse sorted gather parity
  std::vector<uint32_t> rows;
  const uint32_t k = 1U + (c.NextU8() % n);
  for (uint32_t i = 0; i < k; ++i) {
    rows.push_back(c.NextU8() % n);
  }
  std::sort(rows.begin(), rows.end());
  std::vector<uint8_t> gathered(rows.size(), 0xFF);
  if (!GatherCompactNulls(payload, n, rows, gathered.data())) {
    Fail("CompactNulls", "GatherCompactNulls failed on valid payload");
  }
  for (size_t i = 0; i < rows.size(); ++i) {
    if (gathered[i] != nulls[rows[i]]) {
      Fail("CompactNulls", "GatherCompactNulls value mismatch");
    }
  }
}

void BuildVarCharBlock(Cursor& c, uint32_t n, bool temporal_mode,
                       std::vector<char>* bytes, std::vector<uint32_t>* ends,
                       std::vector<uint8_t>* nulls) {
  bytes->clear();
  ends->clear();
  nulls->assign(n, 0);
  // Small dictionary pool so front-coding and phrase tables get rich hits.
  std::array<std::string, 8> pool;
  for (size_t i = 0; i < pool.size(); ++i) {
    const size_t len = c.NextU8() % 24U;
    pool[i].resize(len);
    for (size_t j = 0; j < len; ++j) {
      pool[i][j] = static_cast<char>(32 + (c.NextU8() % 95U));
    }
  }

  for (uint32_t i = 0; i < n; ++i) {
    const uint8_t sel = c.NextU8();
    if ((sel & 7U) == 0) {
      (*nulls)[i] = 1;
      ends->push_back(static_cast<uint32_t>(bytes->size()));
      continue;
    }
    std::string s;
    if (temporal_mode && (sel & 1U) == 0) {
      char buf[32];
      const unsigned yr = 1970U + (c.NextU8() % 130U);
      const unsigned mo = 1U + (c.NextU8() % 12U);
      const unsigned dy = 1U + (c.NextU8() % 28U);
      const unsigned hh = c.NextU8() % 24U;
      const unsigned mm = c.NextU8() % 60U;
      const unsigned ss = c.NextU8() % 60U;
      (void)std::snprintf(buf, sizeof(buf), "%04u-%02u-%02u %02u:%02u:%02u+00",
                          yr, mo, dy, hh, mm, ss);
      s.assign(buf, kTemporalTextAsciiBytes);
    } else if ((sel & 3U) != 3U) {
      s = pool[sel % pool.size()];
      if ((sel & 4U) != 0) {
        s += pool[(sel >> 3U) % pool.size()];
      }
    } else {
      const size_t len = c.NextU8() % 40U;
      s.resize(len);
      for (size_t j = 0; j < len; ++j) {
        s[j] = static_cast<char>(c.NextU8());
      }
    }
    bytes->insert(bytes->end(), s.begin(), s.end());
    ends->push_back(static_cast<uint32_t>(bytes->size()));
  }
}

void VerifyVarCharEqual(const char* codec, std::span<const char> exp_bytes,
                        std::span<const uint32_t> exp_ends,
                        const std::vector<uint8_t>& nulls,
                        const std::vector<char>& got_bytes,
                        const std::vector<uint32_t>& got_ends) {
  if (got_ends.size() != exp_ends.size()) {
    Fail(codec, "ends size mismatch");
  }
  for (size_t i = 0; i < exp_ends.size(); ++i) {
    if (nulls[i] != 0) {
      continue;
    }
    const uint32_t eb = i == 0 ? 0U : exp_ends[i - 1];
    const uint32_t ee = exp_ends[i];
    const uint32_t gb = i == 0 ? 0U : got_ends[i - 1];
    const uint32_t ge = got_ends[i];
    if (ge < gb || ge > got_bytes.size()) {
      Fail(codec, "decoded ends out of range");
    }
    if ((ee - eb) != (ge - gb) ||
        std::memcmp(exp_bytes.data() + eb, got_bytes.data() + gb, ee - eb) !=
            0) {
      Fail(codec, "decoded string mismatch");
    }
  }
}

void FuzzVarCharCodecs(Cursor& c) {
  const uint32_t n = 1U + (c.NextU8() % 64U);
  const bool temporal_mode = (c.NextU8() & 1U) != 0;
  std::vector<char> bytes;
  std::vector<uint32_t> ends;
  std::vector<uint8_t> nulls;
  BuildVarCharBlock(c, n, temporal_mode, &bytes, &ends, &nulls);

  // 1. TemporalText
  {
    std::vector<uint8_t> encoded;
    if (TryEncodeTemporalText(bytes, ends, nulls.data(), n, &encoded)) {
      std::vector<char> dec_bytes;
      std::vector<uint32_t> dec_ends;
      if (!DecodeTemporalText(encoded, nulls.data(), n, &dec_bytes,
                              &dec_ends)) {
        Fail("TemporalText", "DecodeTemporalText failed");
      }
      VerifyVarCharEqual("TemporalText", bytes, ends, nulls, dec_bytes,
                         dec_ends);
      for (uint32_t r = 0; r < n; ++r) {
        if (nulls[r] != 0) {
          continue;
        }
        std::string row_str;
        if (!DecodeTemporalTextRow(encoded, r, &row_str)) {
          Fail("TemporalText", "DecodeTemporalTextRow failed");
        }
        const uint32_t eb = r == 0 ? 0U : ends[r - 1];
        const uint32_t ee = ends[r];
        if (row_str.size() != (ee - eb) ||
            std::memcmp(row_str.data(), bytes.data() + eb, ee - eb) != 0) {
          Fail("TemporalText", "DecodeTemporalTextRow mismatch");
        }
      }
    }
  }

  // 2. FrontDict
  {
    std::vector<uint8_t> encoded;
    if (TryEncodeVarCharFrontDict(bytes, ends, nulls.data(), n, &encoded)) {
      std::vector<char> dec_bytes;
      std::vector<uint32_t> dec_ends;
      if (!DecodeVarCharFrontDict(encoded, nulls.data(), n, &dec_bytes,
                                  &dec_ends)) {
        Fail("FrontDict", "DecodeVarCharFrontDict failed");
      }
      VerifyVarCharEqual("FrontDict", bytes, ends, nulls, dec_bytes, dec_ends);
    }
  }

  // 3. PhraseTable
  {
    std::vector<uint8_t> encoded;
    if (TryEncodeVarCharPhraseTable(bytes, ends, nulls.data(), n, &encoded)) {
      std::vector<char> dec_bytes;
      std::vector<uint32_t> dec_ends;
      if (!DecodeVarCharPhraseTable(encoded, nulls.data(), n, &dec_bytes,
                                    &dec_ends)) {
        Fail("PhraseTable", "DecodeVarCharPhraseTable failed");
      }
      VerifyVarCharEqual("PhraseTable", bytes, ends, nulls, dec_bytes,
                         dec_ends);
    }
  }

  // 4. ZstdVarChar + ZstdFrame
  if (HasZstdSupport()) {
    std::vector<uint8_t> encoded;
    const size_t max_bytes = bytes.size() + static_cast<size_t>(n) * 4U + 64U;
    if (TryEncodeZstdVarChar(bytes, ends, nulls.data(), n, max_bytes, 1,
                             &encoded)) {
      std::vector<char> dec_bytes;
      std::vector<uint32_t> dec_ends;
      if (!DecodeZstdVarChar(encoded, nulls.data(), n, &dec_bytes, &dec_ends)) {
        Fail("ZstdVarChar", "DecodeZstdVarChar failed");
      }
      VerifyVarCharEqual("ZstdVarChar", bytes, ends, nulls, dec_bytes,
                         dec_ends);
    }

    std::vector<uint8_t> raw_payload(bytes.begin(), bytes.end());
    std::vector<uint8_t> frame;
    const uint8_t tag = c.NextU8();
    const uint8_t subtag = c.NextU8();
    if (TryCompressZstdFrame(raw_payload, tag, subtag, 1, &frame)) {
      uint8_t got_tag = 0;
      uint8_t got_subtag = 0;
      std::vector<uint8_t> got_payload;
      if (!DecompressZstdFrame(frame, &got_tag, &got_subtag, &got_payload) ||
          got_tag != tag || got_subtag != subtag ||
          got_payload != raw_payload) {
        Fail("ZstdFrame", "DecompressZstdFrame mismatch");
      }
      // Flipping a byte in the CRC32C field (bytes 6..9) must always fail;
      // flipping a byte in the compressed stream must either fail or decode to
      // the exact original payload (never silently corrupt).
      if (frame.size() > 10) {
        std::vector<uint8_t> bad_crc = frame;
        bad_crc[6 + (c.NextU8() % 4U)] ^= 0x01U;
        std::vector<uint8_t> bad_out;
        if (DecompressZstdFrame(bad_crc, &got_tag, &got_subtag, &bad_out)) {
          Fail("ZstdFrame", "corrupted CRC32C field passed verification");
        }
        std::vector<uint8_t> tampered = frame;
        tampered[10 + (c.NextU8() % (tampered.size() - 10))] ^= 0x5AU;
        bad_out.clear();
        if (DecompressZstdFrame(tampered, &got_tag, &got_subtag, &bad_out) &&
            bad_out != raw_payload) {
          Fail("ZstdFrame", "tampered frame silently returned corrupted data");
        }
      }
    }
  }
}

void FuzzBitPackedForAndXorDouble(Cursor& c) {
  const uint32_t n = 1U + (c.NextU8() % 96U);
  std::vector<uint8_t> nulls(n, 0);
  std::vector<int64_t> ints(n, 0);

  const uint8_t stride = 1U + (c.NextU8() % 16U);
  const int64_t base = (c.NextU8() & 1U) != 0 ? c.NextI64() / 4 : -1000;
  int64_t min_v = std::numeric_limits<int64_t>::max();
  int64_t max_v = std::numeric_limits<int64_t>::min();
  bool any_non_null = false;

  for (uint32_t i = 0; i < n; ++i) {
    const uint8_t sel = c.NextU8();
    if ((sel & 7U) == 0) {
      nulls[i] = 1;
      continue;
    }
    const int64_t delta = static_cast<int64_t>(sel & 0x3FU) * stride;
    const int64_t v = static_cast<int64_t>(static_cast<uint64_t>(base) +
                                           static_cast<uint64_t>(delta));
    ints[i] = v;
    min_v = std::min(min_v, v);
    max_v = std::max(max_v, v);
    any_non_null = true;
  }

  if (any_non_null) {
    std::vector<uint8_t> encoded;
    if (TryEncodeBitPackedFor(ints, nulls.data(), n, min_v, max_v,
                              static_cast<uint64_t>(n) * 8U, &encoded)) {
      std::vector<int64_t> decoded(n, 0);
      if (!DecodeBitPackedFor(encoded, n, min_v, decoded.data())) {
        Fail("BitPackedFor", "DecodeBitPackedFor failed");
      }
      for (uint32_t i = 0; i < n; ++i) {
        if (nulls[i] == 0 && decoded[i] != ints[i]) {
          Fail("BitPackedFor", "DecodeBitPackedFor value mismatch");
        }
      }
      std::vector<uint32_t> rows;
      const uint32_t k = 1U + (c.NextU8() % n);
      for (uint32_t i = 0; i < k; ++i) {
        rows.push_back(c.NextU8() % n);
      }
      std::sort(rows.begin(), rows.end());
      std::vector<int64_t> gathered(rows.size(), 0);
      if (!GatherBitPackedFor(encoded, n, min_v, rows, gathered.data())) {
        Fail("BitPackedFor", "GatherBitPackedFor failed");
      }
      for (size_t i = 0; i < rows.size(); ++i) {
        if (nulls[rows[i]] == 0 && gathered[i] != ints[rows[i]]) {
          Fail("BitPackedFor", "GatherBitPackedFor mismatch");
        }
      }
    }
  }

  // Bit-exact XOR double codec (including -0.0, NaN payloads, +-Inf, repeats)
  std::vector<int64_t> dbl_bits(n, 0);
  int64_t prev = 0;
  for (uint32_t i = 0; i < n; ++i) {
    const uint8_t kind = c.NextU8() % 8U;
    double d = 0.0;
    int64_t bits = 0;
    switch (kind) {
      case 0:
        bits = prev;  // repeat
        break;
      case 1:
        d = -0.0;
        std::memcpy(&bits, &d, sizeof(bits));
        break;
      case 2:
        d = std::numeric_limits<double>::quiet_NaN();
        std::memcpy(&bits, &d, sizeof(bits));
        bits |= static_cast<int64_t>(c.NextU8() & 0x0FU);
        break;
      case 3:
        d = std::numeric_limits<double>::infinity();
        std::memcpy(&bits, &d, sizeof(bits));
        break;
      case 4:
        d = -std::numeric_limits<double>::infinity();
        std::memcpy(&bits, &d, sizeof(bits));
        break;
      case 5:
        bits = prev ^ static_cast<int64_t>(c.NextU8());
        break;
      case 6:
        bits = prev ^ (static_cast<int64_t>(c.NextU8()) << 48U);
        break;
      default:
        bits = c.NextI64();
        break;
    }
    dbl_bits[i] = bits;
    prev = bits;
  }

  std::vector<uint8_t> xor_encoded;
  if (TryEncodeXorDouble(dbl_bits, n, &xor_encoded)) {
    std::vector<int64_t> xor_decoded(n, 0);
    if (!DecodeXorDouble(xor_encoded, n, xor_decoded.data())) {
      Fail("XorDouble", "DecodeXorDouble failed");
    }
    if (xor_decoded != dbl_bits) {
      Fail("XorDouble", "DecodeXorDouble bit-exact mismatch");
    }
  }
}

void FuzzCorruptDecoders(const uint8_t* data, size_t size) {
  const std::span<const uint8_t> raw(data, size);
  const uint32_t n = size > 0 ? (1U + (data[0] % 32U)) : 1U;
  std::vector<uint8_t> nulls(n, 0);
  for (uint32_t i = 0; i < n && i < size; ++i) {
    nulls[i] = data[i] & 1U;
  }
  std::vector<uint8_t> out_u8;
  (void)DecodeCompactNulls(raw, n, &out_u8);
  std::array<uint32_t, 4> probe_rows = {0, 0, n > 1 ? n - 1 : 0, n - 1};
  std::array<uint8_t, 4> gathered_u8{};
  (void)GatherCompactNulls(raw, n, probe_rows, gathered_u8.data());

  std::vector<char> out_bytes;
  std::vector<uint32_t> out_ends;
  (void)DecodeTemporalText(raw, nulls.data(), n, &out_bytes, &out_ends);
  std::string one_row;
  (void)DecodeTemporalTextRow(raw, 0, &one_row);
  (void)DecodeVarCharFrontDict(raw, nulls.data(), n, &out_bytes, &out_ends);
  (void)DecodeVarCharPhraseTable(raw, nulls.data(), n, &out_bytes, &out_ends);

  std::vector<int64_t> out_i64(n, 0);
  (void)DecodeBitPackedFor(raw, n, -100, out_i64.data());
  std::array<int64_t, 4> gathered_i64{};
  (void)GatherBitPackedFor(raw, n, -100, probe_rows, gathered_i64.data());
  (void)DecodeXorDouble(raw, n, out_i64.data());

  if (HasZstdSupport()) {
    uint8_t tag = 0;
    uint8_t subtag = 0;
    std::vector<uint8_t> payload;
    (void)DecompressZstdFrame(raw, &tag, &subtag, &payload);
    (void)DecodeZstdVarChar(raw, nulls.data(), n, &out_bytes, &out_ends);
  }
}

}  // namespace
}  // namespace tinylamb::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2 || size > 4096) {
    return 0;
  }
  tinylamb::fuzz::Cursor c{data, size, 0};
  const uint8_t mode = c.NextU8() % 4U;
  switch (mode) {
    case 0:
      tinylamb::fuzz::FuzzCompactNulls(c);
      break;
    case 1:
      tinylamb::fuzz::FuzzVarCharCodecs(c);
      break;
    case 2:
      tinylamb::fuzz::FuzzBitPackedForAndXorDouble(c);
      break;
    default:
      tinylamb::fuzz::FuzzCorruptDecoders(data + 1, size - 1);
      break;
  }
  return 0;
}
