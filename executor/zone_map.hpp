/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_ZONE_MAP_HPP
#define TINYLAMB_EXECUTOR_ZONE_MAP_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "type/value.hpp"

namespace tinylamb {

struct LSTablePageBlock;
class Schema;

// Lean proof: lean/TinylambOpt/BlockKleene.lean
//   (aeval_contains_ceval, block_kleene_sound, block_all_pass_sound)
// 4-valued abstract domain {T, F, U, E} for compound block-level pruning.
using KleeneMask = uint8_t;
inline constexpr KleeneMask kKleeneNone = 0;
inline constexpr KleeneMask kKleeneT = 1U << 0U;
inline constexpr KleeneMask kKleeneF = 1U << 1U;
inline constexpr KleeneMask kKleeneU = 1U << 2U;
inline constexpr KleeneMask kKleeneE = 1U << 3U;
inline constexpr KleeneMask kKleeneTF = kKleeneT | kKleeneF;
inline constexpr KleeneMask kKleeneAll =
    kKleeneT | kKleeneF | kKleeneU | kKleeneE;

[[nodiscard]] KleeneMask EvaluateBlockKleene(const Expression& expr,
                                             const Schema& schema,
                                             const LSTablePageBlock& blk);

[[nodiscard]] inline bool BlockKleeneCanSkip(KleeneMask mask) noexcept {
  return (mask & (kKleeneT | kKleeneE)) == 0;
}

[[nodiscard]] inline bool BlockKleeneAllPass(KleeneMask mask) noexcept {
  return mask == kKleeneT;
}

// Lean proof: lean/TinylambOpt/TrigramSkip.lean (trigram_skip_sound)
// 256-bit block trigram summary for skipping LIKE blocks whose mandatory
// 3-byte literal substrings are absent from every row in the block.
struct TrigramBlockSummary {
  std::array<uint64_t, 4> bits{};

  static constexpr uint32_t HashTrigram(uint8_t a, uint8_t b,
                                        uint8_t c) noexcept {
    uint32_t h = (static_cast<uint32_t>(a) << 16U) ^
                 (static_cast<uint32_t>(b) << 8U) ^ static_cast<uint32_t>(c);
    h ^= h >> 11U;
    h *= 2654435761U;
    return (h >> 24U) & 255U;
  }

  void AddString(std::string_view s) noexcept {
    if (s.size() < 3) {
      return;
    }
    for (size_t i = 0; i + 2 < s.size(); ++i) {
      const uint32_t bit = HashTrigram(static_cast<uint8_t>(s[i]),
                                       static_cast<uint8_t>(s[i + 1]),
                                       static_cast<uint8_t>(s[i + 2]));
      bits[bit >> 6U] |= (uint64_t{1} << (bit & 63U));
    }
  }

  [[nodiscard]] bool MayContainTrigram(uint8_t a, uint8_t b,
                                       uint8_t c) const noexcept {
    const uint32_t bit = HashTrigram(a, b, c);
    return (bits[bit >> 6U] & (uint64_t{1} << (bit & 63U))) != 0;
  }
};

// Returns true when the block cannot match `like_pattern` (binary collation,
// optional escape char) because at least one mandatory 3-byte literal window
// inside a wildcard-free segment is absent from `summary`.
[[nodiscard]] bool CanSkipLikeBlockByTrigrams(
    std::string_view like_pattern, const TrigramBlockSummary& summary,
    char escape_char = '\\') noexcept;

// Lean proof: lean/TinylambOpt/BloomBlockIntersection.lean
//   (bloom_block_intersection_sound, bloomSkip_sound, nullSafeSkip_sound)
// Per-block Bloom signature for inner equi-join / IN-list block pruning.
struct BlockBloomSignature {
  uint64_t seed{0};
  std::vector<uint64_t> words;
  bool has_null{false};

  static constexpr uint64_t HashKey(uint64_t seed_val, int64_t key) noexcept {
    uint64_t z = static_cast<uint64_t>(key) ^ seed_val;
    z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31U);
  }

  void Init(size_t num_words, uint64_t seed_val) {
    seed = seed_val;
    words.assign(num_words, 0ULL);
    has_null = false;
  }

  void AddInt64(int64_t key) noexcept {
    if (words.empty()) {
      return;
    }
    const uint64_t bit_width = static_cast<uint64_t>(words.size()) * 64ULL;
    const uint64_t h1 = HashKey(seed, key);
    const uint64_t h2 = HashKey(seed ^ 0x9e3779b97f4a7c15ULL, key);
    const uint64_t b1 = h1 % bit_width;
    const uint64_t b2 = h2 % bit_width;
    words[b1 >> 6U] |= (uint64_t{1} << (b1 & 63U));
    words[b2 >> 6U] |= (uint64_t{1} << (b2 & 63U));
  }

  void AddNull() noexcept { has_null = true; }
};

[[nodiscard]] bool CanSkipBlockByBloomIntersection(
    const BlockBloomSignature& build_sig, const BlockBloomSignature& probe_sig,
    bool null_safe_eq = false) noexcept;

// Lean proof: lean/TinylambOpt/SnapshotZoneMap.lean (snapshot_zone_map_sound)
// Returns true iff a block's zone map is sound for pruning under `snapshot_lsn`
// (i.e. all visible versions are covered and no unindexed uncommitted deltas
// exist on the block).
[[nodiscard]] inline bool SnapshotZoneMapValid(
    uint64_t block_commit_lsn, uint64_t snapshot_lsn,
    bool has_uncommitted_deltas) noexcept {
  return !has_uncommitted_deltas && block_commit_lsn <= snapshot_lsn;
}

class ZoneMap {
 public:
  void Add(const Value& value);
  // Typed variants for callers that hold unboxed storage.  They keep the
  // minimum/maximum Values correctly typed (kInt64/kDate/kDouble/kVarChar)
  // so MayMatch still compares against same-type constants.
  void AddInt(int64_t value);
  void AddDate(int64_t days);
  void AddDouble(double value);
  void AddString(std::string_view value);
  void AddNull();
  void AddNulls(size_t count);
  void AddIntRange(int64_t mn, int64_t mx, size_t count);
  void AddDateRange(int64_t mn, int64_t mx, size_t count);
  void AddDoubleRange(double mn, double mx, bool has_bounds, bool saw_nan,
                      size_t count);
  void AddStringRange(std::string_view mn, std::string_view mx, size_t count);
  // Records a non-NULL value that carries no scalar ordering (arrays): it
  // bumps value_count_ so MayMatch keeps the zone instead of pruning a batch
  // that holds only array values.
  void AddOpaque();
  void Merge(const ZoneMap& other);
  void Reset();

  [[nodiscard]] bool MayMatch(BinaryOperation operation,
                              const Value& constant) const;
  // False until any Add* observed a row.  Callers must treat an
  // uninitialized zone conservatively (it may match anything); only an
  // initialized, value-free zone proves "no non-NULL values here".
  [[nodiscard]] bool Initialized() const { return initialized_; }
  [[nodiscard]] const std::optional<Value>& Minimum() const { return minimum_; }
  [[nodiscard]] const std::optional<Value>& Maximum() const { return maximum_; }
  [[nodiscard]] size_t NullCount() const { return null_count_; }
  [[nodiscard]] size_t ValueCount() const { return value_count_; }

 private:
  std::optional<Value> minimum_;
  std::optional<Value> maximum_;
  size_t null_count_{0};
  size_t value_count_{0};
  bool initialized_{false};
  // Set when a DOUBLE NaN was added: NaN is excluded from the envelope but
  // must conservatively keep the zone eligible for every predicate.
  bool has_nan_{false};
  // A byte-order envelope is not a proof under a folding comparison.
  bool has_collated_values_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_ZONE_MAP_HPP
