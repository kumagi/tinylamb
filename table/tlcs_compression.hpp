/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TABLE_TLCS_COMPRESSION_HPP
#define TINYLAMB_TABLE_TLCS_COMPRESSION_HPP

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb::tlcs_compression {

// ============================================================================
// 1. Compact Nullmap (Compression/Nullable.lean: sound_ValidityBits,
//    sound_MinorityRoundTrip, sound_SparseWins, sound_DenseRoundTrip)
// ============================================================================
// Encodes `n` byte-per-row null flags (0 = non-null, 1 = null) into a compact
// representation strictly smaller than `n` bytes if profitable:
//   tag 0x01: 1-bit validity bitmap (1 + ceil(n/8) bytes)
//   tag 0x02: sparse NULL positions (1 + 4 + 4*null_cnt bytes)
//   tag 0x03: sparse non-NULL positions (1 + 4 + 4*non_null_cnt bytes)
// Returns true and fills `out` iff the compact encoding is strictly smaller
// than `n` bytes. Callers fall back to raw `n` bytes when false (preserving
// 100% backward compatibility with TLCS v3 files where nulls_size == n).
[[nodiscard]] bool TryEncodeCompactNulls(std::span<const uint8_t> nulls,
                                         std::vector<uint8_t>* out);

// Decodes either a raw (`encoded.size() == n`) or compact (`encoded.size() <
// n`) nullmap into `out` (resized to `n`, each byte 0 or 1). Returns false on
// corrupt input.
[[nodiscard]] bool DecodeCompactNulls(std::span<const uint8_t> encoded,
                                      uint32_t n, std::vector<uint8_t>* out);

// O(|rows|) / O(|rows| log k) sparse random-access gather directly from a raw
// or compact nullmap without materializing all `n` null flags.
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/BitLayoutsA.lean
//     (RowMajor.SparseGather, RowMajor.PointSpan)
//   lean/TinylambOpt/ResearchProofs/Compression/Nullable.lean
//     (sound_ValidityBits, sound_MinorityRoundTrip)
[[nodiscard]] bool GatherCompactNulls(std::span<const uint8_t> encoded,
                                      uint32_t n,
                                      std::span<const uint32_t> rows,
                                      uint8_t* out);

// ============================================================================
// 2. TemporalText 47-bit Timestamp Packing (Compression/TemporalText.lean:
//    sound_LexicalInverse, sound_CodeDomain, sound_LexicalOrder,
//    sound_FixedWidthCharge, sound_PackedSeek)
// ============================================================================
// Fixed-width 6-byte (48-bit) little-endian packing of 22-byte UTC timestamps
// matching exact ASCII `YYYY-MM-DD HH:MM:SS+00` (14 decimal digits < 2^47).
// Eliminates the 4-byte offset array and shrinks 26 B/row -> 6 B/row (4.33x).
inline constexpr size_t kTemporalTextAsciiBytes = 22;
inline constexpr size_t kTemporalTextPackedBytes = 6;

[[nodiscard]] bool TryEncodeTemporalText(std::span<const char> bytes,
                                         std::span<const uint32_t> ends,
                                         const uint8_t* nulls, uint32_t n,
                                         std::vector<uint8_t>* out);

[[nodiscard]] bool DecodeTemporalText(std::span<const uint8_t> encoded,
                                      const uint8_t* nulls, uint32_t n,
                                      std::vector<char>* bytes_out,
                                      std::vector<uint32_t>* ends_out);

[[nodiscard]] bool DecodeTemporalTextBytes(std::span<const uint8_t> encoded,
                                           const uint8_t* nulls, uint32_t n,
                                           std::vector<uint8_t>* bytes_out,
                                           std::vector<uint32_t>* ends_out,
                                           bool lengths_only = false);

// O(1) single-row random access (`TemporalText.sound_PackedSeek`).
[[nodiscard]] bool DecodeTemporalTextRow(std::span<const uint8_t> encoded,
                                         uint32_t row, std::string* out);

// ============================================================================
// 3. Sorted Front-Coded Block Dictionary + Bit-Packed IDs
//    (Compression/Text.lean: FrontInverse_sound, FrontEntryCharge_sound,
//     FrontPageInverse_sound, FramedInverse_sound;
//     Compression/Wire.lean: sound_PackedInverse, BitPackBreakEven_sound;
//     Compression/Container.lean: sound_PerChunkChoice, sound_RawEnvelope)
// ============================================================================
// Sorts unique non-null strings in a block, front-codes consecutive entries
// with ULEB128 (shared_prefix_len, suffix_len, suffix_bytes), and bit-packs
// per-row dictionary IDs at `bit_width = bit_width(dict_size - 1)`.
// Returns true iff the encoded payload is strictly smaller than
// `bytes.size() + n * sizeof(uint32_t)`.
[[nodiscard]] bool TryEncodeVarCharFrontDict(std::span<const char> bytes,
                                             std::span<const uint32_t> ends,
                                             const uint8_t* nulls, uint32_t n,
                                             std::vector<uint8_t>* out);

[[nodiscard]] bool DecodeVarCharFrontDict(std::span<const uint8_t> encoded,
                                          const uint8_t* nulls, uint32_t n,
                                          std::vector<char>* bytes_out,
                                          std::vector<uint32_t>* ends_out);

[[nodiscard]] bool DecodeVarCharFrontDictBytes(
    std::span<const uint8_t> encoded, const uint8_t* nulls, uint32_t n,
    std::vector<uint8_t>* bytes_out, std::vector<uint32_t>* ends_out,
    bool lengths_only = false, std::span<const uint32_t> selected_rows = {});

// Evaluates `row contains needle` directly over a front-coded dictionary block
// without expanding row strings. Writes ascending matching row ordinals into
// `matching_rows_out`. Returns false only on corrupt input.
[[nodiscard]] bool MatchVarCharFrontDictContains(
    std::span<const uint8_t> encoded, const uint8_t* nulls, uint32_t n,
    std::string_view needle, std::vector<uint32_t>* matching_rows_out);

// ============================================================================
// 4. Bit-Packed Strided Frame-of-Reference
//    (Compression/IntegerFrames.lean: sound_ForEnvelope, sound_StrideFactor,
//     sound_SignedFor; Compression/Wire.lean: sound_PackedInverse,
//     BitPackBreakEven_sound)
// ============================================================================
// Computes GCD stride across non-null `(val - min_int)` deltas and bit-packs
// `(val - min_int) / stride` at exact `bw` bits/row (`1 <= bw <= 63`).
// Returns true iff strictly smaller than byte-aligned FOR (`n * byte_width`).
[[nodiscard]] bool TryEncodeBitPackedFor(std::span<const int64_t> ints,
                                         const uint8_t* nulls, uint32_t n,
                                         int64_t min_int, int64_t max_int,
                                         uint64_t byte_aligned_size,
                                         std::vector<uint8_t>* out);

[[nodiscard]] bool DecodeBitPackedFor(std::span<const uint8_t> encoded,
                                      uint32_t n, int64_t min_int,
                                      int64_t* out);

// O(|rows|) sparse random-access gather directly from bit-packed FOR payload
// without decompressing the entire block.
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/BitLayoutsA.lean
//     (RowMajor.Lossless, RowMajor.SparseGather, RowMajor.PointSpan)
[[nodiscard]] bool GatherBitPackedFor(std::span<const uint8_t> encoded,
                                      uint32_t n, int64_t min_int,
                                      std::span<const uint32_t> rows,
                                      int64_t* out);

// ============================================================================
// 5. Bit-Exact XOR-Chain Compressed IEEE-754 Doubles
//    (Compression/BitPatterns.lean: XorChain_sound, SignificantWindow_sound,
//     CommonHighBits_sound)
// ============================================================================
// Gorilla-style bit-exact XOR compression over raw uint64 IEEE-754 bits
// (preserves -0.0 and exact NaN payloads). Returns true iff strictly smaller
// than `n * 8` bytes.
[[nodiscard]] bool TryEncodeXorDouble(std::span<const int64_t> bits, uint32_t n,
                                      std::vector<uint8_t>* out);

[[nodiscard]] bool DecodeXorDouble(std::span<const uint8_t> encoded, uint32_t n,
                                   int64_t* out);

// ============================================================================
// 6. Phrase Table Compression with Exact DP Shortest Parse
//    (Compression/PhraseTables.lean: sound_OptimalParse, sound_OptimalInverse,
//     sound_TableExtension, sound_LiteralCeiling, sound_RowBoundary)
// ============================================================================
// FSST-style 1-byte symbol table (0..254 = frequent 2..8-byte phrases,
// 0xFF = 1-byte literal escape) with exact per-row DP shortest parse.
// Returns true iff strictly smaller than `bytes.size() + n * 4`.
[[nodiscard]] bool TryEncodeVarCharPhraseTable(std::span<const char> bytes,
                                               std::span<const uint32_t> ends,
                                               const uint8_t* nulls, uint32_t n,
                                               std::vector<uint8_t>* out);

[[nodiscard]] bool DecodeVarCharPhraseTable(std::span<const uint8_t> encoded,
                                            const uint8_t* nulls, uint32_t n,
                                            std::vector<char>* bytes_out,
                                            std::vector<uint32_t>* ends_out);

[[nodiscard]] bool DecodeVarCharPhraseTableBytes(
    std::span<const uint8_t> encoded, const uint8_t* nulls, uint32_t n,
    std::vector<uint8_t>* bytes_out, std::vector<uint32_t>* ends_out,
    bool lengths_only = false);

// ============================================================================
// 7. Zstandard Per-Block Frame Container
//    (Compression/Container.lean: sound_VerifiedContainer, sound_RawEnvelope,
//     sound_PerChunkChoice, sound_BeatTarget)
// ============================================================================
[[nodiscard]] bool HasZstdSupport() noexcept;

// Wraps an already-encoded block payload in a CRC32C-verified Zstd frame:
//   [u8 inner_tag][u8 inner_subtag][u32 le orig_len][u32 le crc32c][zstd...]
// Returns true iff Zstd is available and the frame is strictly smaller than
// `payload.size()`.
[[nodiscard]] bool TryCompressZstdFrame(std::span<const uint8_t> payload,
                                        uint8_t inner_tag, uint8_t inner_subtag,
                                        int level, std::vector<uint8_t>* out);

[[nodiscard]] bool DecompressZstdFrame(std::span<const uint8_t> encoded,
                                       uint8_t* inner_tag_out,
                                       uint8_t* inner_subtag_out,
                                       std::vector<uint8_t>* payload_out);

// Zero-memset variant backed by a thread-local uninitialized scratch buffer.
// The returned span in `payload_out` remains valid until the next call to
// `DecompressZstdFrameRaw` on the same thread.
[[nodiscard]] bool DecompressZstdFrameRaw(
    std::span<const uint8_t> encoded, uint8_t* inner_tag_out,
    uint8_t* inner_subtag_out, std::span<const uint8_t>* payload_out);

// Packs VARCHAR rows as `(ULEB128 len, bytes)` and compresses with Zstd.
// Returns true iff Zstd is available and strictly smaller than `max_bytes`.
[[nodiscard]] bool TryEncodeZstdVarChar(std::span<const char> bytes,
                                        std::span<const uint32_t> ends,
                                        const uint8_t* nulls, uint32_t n,
                                        size_t max_bytes, int level,
                                        std::vector<uint8_t>* out);

[[nodiscard]] bool DecodeZstdVarChar(std::span<const uint8_t> encoded,
                                     const uint8_t* nulls, uint32_t n,
                                     std::vector<char>* bytes_out,
                                     std::vector<uint32_t>* ends_out);

// Decodes an already-decompressed `(ULEB128 len, bytes)` VARCHAR payload
// directly into `uint8_t` bytes (or lengths only when `lengths_only` is true).
[[nodiscard]] bool DecodeZstdVarCharUnpackedBytes(
    std::span<const uint8_t> packed, const uint8_t* nulls, uint32_t n,
    std::vector<uint8_t>* bytes_out, std::vector<uint32_t>* ends_out,
    bool lengths_only = false, std::span<const uint32_t> selected_rows = {});

// Evaluates `row contains needle` directly over an unpacked `(len, bytes)`
// payload without copying row strings. Returns false only on corrupt input.
[[nodiscard]] bool MatchZstdVarCharUnpackedContains(
    std::span<const uint8_t> packed, const uint8_t* nulls, uint32_t n,
    std::string_view needle, std::vector<uint32_t>* matching_rows_out);

// ============================================================================
// 8. ClickstreamLayout.Storage: TieOrientation, ClockWindow, HashExceptions
// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/OptimalEqualKeyOrientation.lean
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/ClockResidualWindow.lean
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/ModalHashExceptions.lean
// ============================================================================
struct TieOrientationPlan {
  size_t min_bytes{0};
  // Per-group orientation: false = forward, true = reversed.
  std::vector<uint8_t> reversed;
};

// Two-state DP minimizing exact front-coded payload bytes (including varint
// LCP/suffix lengths) across all tie-group forward/reverse orientations.
[[nodiscard]] TieOrientationPlan ChooseOptimalTieOrientations(
    std::span<const std::vector<std::string_view>> groups);

struct ClockWindowPlan {
  int64_t base{0};
  size_t total_bytes{0};
  size_t exception_count{0};
};

// Finds the globally optimal residual window base for fixed `bit_width`
// (0 < bit_width < 63) by testing only `0` and `{d, d - span}` anchors,
// pricing exact zigzag-varint base + packed slots + exception bytes.
[[nodiscard]] ClockWindowPlan ChooseOptimalClockWindow(
    std::span<const int64_t> deltas, unsigned bit_width,
    size_t exception_bytes);

struct HashExceptionsTable {
  std::vector<uint64_t> mode_hash_by_code;
  // Sorted by row_id ascending: (row_id, actual_hash) for non-mode rows.
  std::vector<std::pair<uint32_t, uint64_t>> exceptions;

  [[nodiscard]] uint64_t HashAt(uint32_t row, uint32_t code) const noexcept;
};

// Builds per-code mode hash predictor and minimal exception list.
[[nodiscard]] HashExceptionsTable EncodeHashExceptions(
    std::span<const uint32_t> codes, std::span<const uint64_t> hashes);

}  // namespace tinylamb::tlcs_compression

#endif  // TINYLAMB_TABLE_TLCS_COMPRESSION_HPP
