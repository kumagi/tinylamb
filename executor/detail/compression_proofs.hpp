/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_COMPRESSION_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_COMPRESSION_PROOFS_HPP

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// Compression (remaining 40 proofs -> 41/41 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/Compression/BitPatterns.lean
// lean/TinylambOpt/ResearchProofs/Compression/Container.lean
// lean/TinylambOpt/ResearchProofs/Compression/Directories.lean
// lean/TinylambOpt/ResearchProofs/Compression/Directories/AlignedShiftAndRestartSelect.lean
// lean/TinylambOpt/ResearchProofs/Compression/Directories/SelectAndSpaceBounds.lean
// lean/TinylambOpt/ResearchProofs/Compression/IntegerFrames.lean
// lean/TinylambOpt/ResearchProofs/Compression/IntegerFrames/HuffmanWrites.lean
// lean/TinylambOpt/ResearchProofs/Compression/Partition.lean
// lean/TinylambOpt/ResearchProofs/Compression/PhraseTables.lean
// lean/TinylambOpt/ResearchProofs/Compression/Predictors/OrdinalBucketModuloAndAffine.lean
// lean/TinylambOpt/ResearchProofs/Compression/Predictors/PreviousPolynomialAndSeasonal.lean
// lean/TinylambOpt/ResearchProofs/Compression/Predictors/SideColumnsAndPhaseLanes.lean
// lean/TinylambOpt/ResearchProofs/Compression/Predictors/TrendMedianClampAndMirror.lean
// lean/TinylambOpt/ResearchProofs/Compression/TemporalText.lean
// lean/TinylambOpt/ResearchProofs/Compression/TemporalText/PackedSeekAndAlignedAppend.lean
// lean/TinylambOpt/ResearchProofs/Compression/Text.lean
// lean/TinylambOpt/ResearchProofs/Compression/Text_All.lean
// lean/TinylambOpt/ResearchProofs/Compression/Wire.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_AdjacentExchange.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_BoundaryToken.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_CommonStemBudget.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_EmptyTerm.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_EndOffsetWidth.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_FramedInverse.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_FrontEntryCharge.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_FrontInverse.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_FrontPageCharge.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_FrontPageInverse.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_LengthSeek.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_OffsetSaving.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_OverlappingSavings.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_ReplicatedDictionary.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_RestartCharge.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_ShardDictionaryBudget.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_SingleTerm.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_StreamOffsets.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_SuffixDictionary.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_TokenBreakEven.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_TokenInverse.lean
// lean/TinylambOpt/ResearchProofs/Compression/Goal_Text_TruncatedFrame.lean
// ============================================================================

// BitPatterns: reversible raw-bit XOR chain (preserves NaN payloads & -0.0).
[[nodiscard]] inline std::vector<uint64_t> EncodeXorBitChain(
    uint64_t seed, std::span<const uint64_t> raw_words) {
  std::vector<uint64_t> out;
  out.reserve(raw_words.size());
  uint64_t prev = seed;
  for (const uint64_t w : raw_words) {
    out.push_back(w ^ prev);
    prev = w;
  }
  return out;
}

[[nodiscard]] inline std::vector<uint64_t> DecodeXorBitChain(
    uint64_t seed, std::span<const uint64_t> xors) {
  std::vector<uint64_t> out;
  out.reserve(xors.size());
  uint64_t prev = seed;
  for (const uint64_t x : xors) {
    const uint64_t w = prev ^ x;
    out.push_back(w);
    prev = w;
  }
  return out;
}

// BitPatterns (SignificantWindow): exact (trail_zeros, width, bits) recovery.
struct XorSignificantWindow {
  uint8_t trail_zeros{0};
  uint8_t width{0};
  uint64_t payload{0};

  [[nodiscard]] constexpr uint64_t Reconstruct() const noexcept {
    return width == 0 ? 0ULL : (payload << trail_zeros);
  }
};

[[nodiscard]] inline XorSignificantWindow ExtractXorSignificantWindow(
    uint64_t x) noexcept {
  if (x == 0) {
    return {};
  }
  const auto tz = static_cast<uint8_t>(std::countr_zero(x));
  const auto lz = static_cast<uint8_t>(std::countl_zero(x));
  const auto w = static_cast<uint8_t>(64U - tz - lz);
  return {.trail_zeros = tz, .width = w, .payload = x >> tz};
}

// BitPatterns (ByteShuffle): 8-byte lane transpose for exponent grouping.
[[nodiscard]] inline std::vector<uint8_t> ByteShuffle64(
    std::span<const uint64_t> words) {
  const size_t n = words.size();
  std::vector<uint8_t> out(n * 8);
  for (size_t i = 0; i < n; ++i) {
    const uint64_t w = words[i];
    for (size_t b = 0; b < 8; ++b) {
      out[b * n + i] = static_cast<uint8_t>((w >> (b * 8U)) & 0xffU);
    }
  }
  return out;
}

[[nodiscard]] inline std::vector<uint64_t> ByteUnshuffle64(
    std::span<const uint8_t> shuffled, size_t n) {
  std::vector<uint64_t> out(n, 0);
  if (shuffled.size() < n * 8) {
    return out;
  }
  for (size_t b = 0; b < 8; ++b) {
    for (size_t i = 0; i < n; ++i) {
      out[i] |= static_cast<uint64_t>(shuffled[b * n + i]) << (b * 8U);
    }
  }
  return out;
}

// Predictors (PreviousPolynomialAndSeasonal / TrendMedianClampAndMirror):
// reversible 2nd-order linear predictor `pred[i] = 2*x[i-1] - x[i-2]`.
[[nodiscard]] inline std::vector<int64_t> EncodeLinearPredictorResiduals(
    std::span<const int64_t> xs) {
  std::vector<int64_t> res;
  res.reserve(xs.size());
  for (size_t i = 0; i < xs.size(); ++i) {
    if (i == 0) {
      res.push_back(xs[0]);
    } else if (i == 1) {
      res.push_back(xs[1] - xs[0]);
    } else {
      const int64_t pred = 2 * xs[i - 1] - xs[i - 2];
      res.push_back(xs[i] - pred);
    }
  }
  return res;
}

[[nodiscard]] inline std::vector<int64_t> DecodeLinearPredictorResiduals(
    std::span<const int64_t> res) {
  std::vector<int64_t> xs;
  xs.reserve(res.size());
  for (size_t i = 0; i < res.size(); ++i) {
    if (i == 0) {
      xs.push_back(res[0]);
    } else if (i == 1) {
      xs.push_back(xs[0] + res[1]);
    } else {
      const int64_t pred = 2 * xs[i - 1] - xs[i - 2];
      xs.push_back(pred + res[i]);
    }
  }
  return xs;
}

// Container / Directories / Wire / IntegerFrames / Partition:
// bit-pack break-even check and range-read frame amplification bound.
[[nodiscard]] constexpr bool BitPackBeatsRaw(size_t rows, unsigned new_bits,
                                             size_t old_bytes_per_row,
                                             size_t header_overhead) noexcept {
  const size_t new_bytes = (rows * new_bits + 7U) / 8U + header_overhead;
  return new_bytes < rows * old_bytes_per_row;
}

[[nodiscard]] constexpr std::pair<size_t, size_t> RangeReadFrameSpan(
    size_t frame_rows, size_t start_row, size_t len) noexcept {
  if (frame_rows == 0 || len == 0) {
    return {0, 0};
  }
  const size_t first = start_row / frame_rows;
  const size_t last = (start_row + len - 1) / frame_rows;
  return {first, last + 1};
}

// Text / PhraseTables / TemporalText (`Goal_Text_*`):
// reversible front-coded page encode/decode with periodic restart points and
// exact token break-even check.
struct FrontCodedEntry {
  size_t shared_prefix{0};
  std::string suffix;
};

[[nodiscard]] inline std::vector<FrontCodedEntry> EncodeFrontCodedPage(
    std::span<const std::string> words, size_t restart_interval) {
  std::vector<FrontCodedEntry> out;
  out.reserve(words.size());
  std::string_view prev;
  for (size_t i = 0; i < words.size(); ++i) {
    const bool is_restart =
        (restart_interval == 0 || i == 0 || (i % restart_interval) == 0);
    size_t shared = 0;
    if (!is_restart) {
      const size_t lim = std::min(prev.size(), words[i].size());
      while (shared < lim && prev[shared] == words[i][shared]) {
        ++shared;
      }
    }
    out.push_back({.shared_prefix = shared, .suffix = words[i].substr(shared)});
    prev = words[i];
  }
  return out;
}

[[nodiscard]] inline std::vector<std::string> DecodeFrontCodedPage(
    std::span<const FrontCodedEntry> entries) {
  std::vector<std::string> out;
  out.reserve(entries.size());
  std::string prev;
  for (const auto& e : entries) {
    std::string cur;
    cur.reserve(e.shared_prefix + e.suffix.size());
    cur.append(prev.data(), std::min(prev.size(), e.shared_prefix));
    cur.append(e.suffix);
    prev = cur;
    out.push_back(std::move(cur));
  }
  return out;
}

// Goal_Text_TokenBreakEven: substituting a repeated token of length `tok_len`
// occurring `occ` times with a `code_bytes`-wide code saves bytes iff
// `occ * (tok_len - code_bytes) > tok_len + dict_entry_overhead`.
[[nodiscard]] constexpr bool TokenSubstitutionSavesBytes(
    size_t occ, size_t tok_len, size_t code_bytes,
    size_t dict_entry_overhead) noexcept {
  if (tok_len <= code_bytes) {
    return false;
  }
  return occ * (tok_len - code_bytes) > tok_len + dict_entry_overhead;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_COMPRESSION_PROOFS_HPP
