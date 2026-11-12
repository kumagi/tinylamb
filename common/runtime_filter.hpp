/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Runtime ("dynamic") join filters over one INT64/DATE key domain.
//
// A hash join knows the COMPLETE set of join keys on its build side before it
// starts pulling rows from the probe side.  That key set is a *sound* pruning
// superset for the probe scan: a probe row whose join key is not in the build
// key domain cannot produce a single join match, so the scan may drop it
// before decoding, indexing, or feeding the probe hash lookup.  Three filter
// shapes trade memory for rejection power (docs/todo.md Phase 2 "Dynamic
// Filter Join / SIP / Bloom"):
//
//   kExactSet  information passing: the distinct build keys themselves.
//              No false positives, 8 bytes/key, chosen while the domain is
//              small.
//   kMinMax    shared information passing, range half: the [lo, hi]
//              enclosure of the build keys.  Two int64 of state regardless
//              of domain size; very effective when the build side is itself
//              range-restricted (`driver.k < 100`).
//   kBloom     shared information passing, membership half: a bitset with a
//              handful of probes.  False positives are allowed (a probe key
//              that survives still goes through the real join), false
//              negatives never are.
//
// Soundness contract (relied on by every consumer):
//   * Reject(key) == true  =>  no build row carries an equal, non-NULL key.
//   * The filters are built once and are immutable afterwards, so a
//     `RuntimeFilterGroup` may be shared by concurrent scan workers.
//   * NULL build keys and NULL probe keys are NOT part of the domain.  An
//     equi-join (`=`/`<=>` handled by the caller) never matches NULL, so a
//     consumer that sees a NULL key may reject it unconditionally; see
//     ScanRuntimeFilter.
//   * The domain is the raw int64 bit pattern of an INT64 or DATE column.
//     Both order and equality agree with `Value::value.int_value`, but a
//     build/probe pair whose declared types DIFFER (INT64 vs DATE) shares no
//     domain, so callers must gate on type equality before binding.
//
// `common/` is the lowest layer, which is what lets the storage-level scan
// iterator (table/full_scan_iterator.cpp) reject rows before decoding without
// depending on the relational or executor layers.

#ifndef TINYLAMB_COMMON_RUNTIME_FILTER_HPP
#define TINYLAMB_COMMON_RUNTIME_FILTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

enum class RuntimeFilterKind : uint8_t {
  kExactSet,
  kMinMax,
  kBloom,
  kRejectAll,
};

[[nodiscard]] const char* RuntimeFilterKindName(RuntimeFilterKind kind);

// Immutable-after-build sound membership filter over an INT64 key domain.
class RuntimeFilter {
 public:
  RuntimeFilter() = default;
  RuntimeFilter(const RuntimeFilter&) = delete;
  RuntimeFilter& operator=(const RuntimeFilter&) = delete;
  RuntimeFilter(RuntimeFilter&&) = delete;
  RuntimeFilter& operator=(RuntimeFilter&&) = delete;
  virtual ~RuntimeFilter() = default;

  [[nodiscard]] virtual RuntimeFilterKind Kind() const = 0;
  // True when `key` provably has no equal non-NULL counterpart on the build
  // side. False means "maybe" (a Bloom false positive) or "certainly matches".
  [[nodiscard]] virtual bool Reject(int64_t key) const = 0;
  // Distinct build keys the filter was built from (0 when unknown).
  [[nodiscard]] virtual size_t DistinctKeys() const = 0;
  // Resident footprint in bytes, for EXPLAIN / cost accounting.
  [[nodiscard]] virtual size_t MemoryBytes() const = 0;
};

// The distinct build keys themselves: a sorted array (Min/Max/EXPLAIN) plus
// an open-addressing membership table so Reject() answers with one or two
// probe loads instead of a ~log2(n)-level binary search, whose branch
// pattern is unpredictable on data-ordered probe streams.
class ExactSetRuntimeFilter final : public RuntimeFilter {
 public:
  explicit ExactSetRuntimeFilter(std::vector<int64_t> sorted_distinct_keys,
                                 bool already_sorted_unique = false,
                                 bool skip_hash_table = false);

  [[nodiscard]] RuntimeFilterKind Kind() const override {
    return RuntimeFilterKind::kExactSet;
  }
  [[nodiscard]] bool Reject(int64_t key) const override;
  [[nodiscard]] size_t DistinctKeys() const override { return keys_.size(); }
  [[nodiscard]] size_t MemoryBytes() const override {
    size_t table_slots = table_.size();
    if (table_slots == 0 && !keys_.empty()) {
      table_slots = 16;
      while (table_slots < keys_.size() * 2) {
        table_slots <<= 1U;
      }
    }
    return sizeof(*this) + (keys_.size() * sizeof(int64_t)) +
           (table_slots * sizeof(uint32_t));
  }
  [[nodiscard]] const std::vector<int64_t>& Keys() const { return keys_; }
  [[nodiscard]] int64_t Min() const { return keys_.front(); }
  [[nodiscard]] int64_t Max() const { return keys_.back(); }

 private:
  std::vector<int64_t> keys_;
  // Slot -> sorted-key index + 1 (0 = empty); sized >= 2x keys, power of two.
  std::vector<uint32_t> table_;
  uint64_t table_mask_{0};
};

// [lo, hi] enclosure of the build keys: rejects everything strictly outside.
class MinMaxRuntimeFilter final : public RuntimeFilter {
 public:
  MinMaxRuntimeFilter(int64_t lo, int64_t hi, size_t distinct_keys);

  [[nodiscard]] RuntimeFilterKind Kind() const override {
    return RuntimeFilterKind::kMinMax;
  }
  [[nodiscard]] bool Reject(int64_t key) const override {
    return key < lo_ || hi_ < key;
  }
  [[nodiscard]] size_t DistinctKeys() const override { return distinct_keys_; }
  [[nodiscard]] size_t MemoryBytes() const override { return sizeof(*this); }
  [[nodiscard]] int64_t Lo() const { return lo_; }
  [[nodiscard]] int64_t Hi() const { return hi_; }

 private:
  int64_t lo_;
  int64_t hi_;
  size_t distinct_keys_;
};

// Bloom filter over the build keys. kHashProbes independent bit positions are
// set per key; a key is rejected when any required probe bit is clear.
class BloomRuntimeFilter final : public RuntimeFilter {
 public:
  // `distinct_keys` sizes the bitset at ~10 bits/key (false-positive rate
  // well under 2%); the bit count is rounded up to a power of two.
  BloomRuntimeFilter(std::vector<uint64_t> bits, size_t bit_count,
                     size_t distinct_keys, size_t nominal_bit_count = 0);
  // Zero-copy constructor over an immutable memory-mapped word span whose
  // lifetime outlives this filter (e.g. a persistent `.tlcs` file).
  BloomRuntimeFilter(std::span<const uint64_t> borrowed_bits, size_t bit_count,
                     size_t distinct_keys, size_t nominal_bit_count = 0);

  [[nodiscard]] RuntimeFilterKind Kind() const override {
    return RuntimeFilterKind::kBloom;
  }
  [[nodiscard]] bool Reject(int64_t key) const override;
  [[nodiscard]] size_t DistinctKeys() const override { return distinct_keys_; }
  [[nodiscard]] size_t MemoryBytes() const override {
    const size_t words =
        nominal_bit_count_ != 0 ? (nominal_bit_count_ / 64) : bits_view_.size();
    return sizeof(*this) + (words * sizeof(uint64_t));
  }
  [[nodiscard]] size_t BitCount() const { return bit_count_; }
  [[nodiscard]] std::span<const uint64_t> Words() const noexcept {
    return bits_view_;
  }
  // A malformed bitset (non power-of-two size, or fewer words than the bit
  // count claims) makes every probe an out-of-bounds read. The repair is to
  // stop pruning entirely rather than to invent bits: a filter that rejects
  // build keys is worse than no filter at all.
  [[nodiscard]] bool disabled() const { return disabled_; }

  // Exposed for the builder and its tests.
  static constexpr unsigned kHashProbes = 2;
  static uint64_t Hash(int64_t key, unsigned probe);
  // Bits sized for `distinct_keys` at the ~10 bits/key target, minimum 512.
  static size_t BitsForKeys(size_t distinct_keys);

 private:
  std::vector<uint64_t> bits_;
  std::span<const uint64_t> bits_view_;
  size_t bit_count_;
  size_t distinct_keys_;
  size_t nominal_bit_count_{0};
  size_t mask_;
  bool disabled_{false};
};

// The build side contributed no usable key at all, so no probe row can match.
// (An inner equi-join against an all-NULL / empty build side is empty.)
class RejectAllRuntimeFilter final : public RuntimeFilter {
 public:
  [[nodiscard]] RuntimeFilterKind Kind() const override {
    return RuntimeFilterKind::kRejectAll;
  }
  [[nodiscard]] bool Reject(int64_t key) const override {
    (void)key;
    return true;
  }
  [[nodiscard]] size_t DistinctKeys() const override { return 0; }
  [[nodiscard]] size_t MemoryBytes() const override { return sizeof(*this); }
};

// Conjunctive (AND) combination: a key survives only when EVERY part accepts
// it.  Sharing one group across scan workers is safe because the parts are
// immutable once the group is built.
class RuntimeFilterGroup final {
 public:
  static constexpr uint64_t kMaxDenseSpan = 33'554'432ULL;

  RuntimeFilterGroup() = default;
  explicit RuntimeFilterGroup(
      std::vector<std::unique_ptr<RuntimeFilter>> parts,
      const std::vector<int64_t>* sorted_distinct_keys = nullptr);
  RuntimeFilterGroup(std::vector<std::unique_ptr<RuntimeFilter>> parts,
                     std::vector<uint64_t> prebuilt_dense_bits, int64_t lo,
                     int64_t hi);

  [[nodiscard]] bool empty() const { return parts_.empty(); }
  [[nodiscard]] bool Reject(int64_t key) const noexcept {
    if (reject_all_) [[unlikely]] {
      return true;
    }
    if (has_bounds_ && (key < lo_ || key > hi_)) {
      return true;
    }
    if (has_dense_bits_) {
      const uint64_t off =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(lo_);
      return (dense_bits_[off >> 6] & (uint64_t{1} << (off & 63))) == 0;
    }
    for (const std::unique_ptr<RuntimeFilter>& part : parts_) {
      if (part->Reject(key)) {
        return true;
      }
    }
    return false;
  }
  // Sound range rejection for ZoneMap block pruning: returns true when NO key
  // in [min_key, max_key] can match the filter.
  [[nodiscard]] bool RejectRange(int64_t min_key,
                                 int64_t max_key) const noexcept {
    if (reject_all_) [[unlikely]] {
      return true;
    }
    if (min_key > max_key) [[unlikely]] {
      return false;
    }
    if (has_bounds_ && (max_key < lo_ || min_key > hi_)) {
      return true;
    }
    if (has_dense_bits_) {
      const int64_t clamped_min = min_key > lo_ ? min_key : lo_;
      const int64_t clamped_max = max_key < hi_ ? max_key : hi_;
      if (clamped_min > clamped_max) {
        return true;
      }
      const uint64_t off_lo =
          static_cast<uint64_t>(clamped_min) - static_cast<uint64_t>(lo_);
      const uint64_t off_hi =
          static_cast<uint64_t>(clamped_max) - static_cast<uint64_t>(lo_);
      const auto word_lo = static_cast<size_t>(off_lo >> 6);
      const auto word_hi = static_cast<size_t>(off_hi >> 6);
      if (word_hi - word_lo <= 8) {
        const uint64_t low_mask = ~uint64_t{0} << (off_lo & 63);
        const uint64_t high_mask = (~uint64_t{0}) >> (63 - (off_hi & 63));
        if (word_lo == word_hi) {
          return (dense_bits_[word_lo] & low_mask & high_mask) == 0;
        }
        if ((dense_bits_[word_lo] & low_mask) != 0) {
          return false;
        }
        for (size_t w = word_lo + 1; w < word_hi; ++w) {
          if (dense_bits_[w] != 0) {
            return false;
          }
        }
        return (dense_bits_[word_hi] & high_mask) == 0;
      }
      return false;
    }
    if (exact_keys_ != nullptr) {
      if (exact_keys_->empty()) {
        return true;
      }
      if (max_key < exact_keys_->front() || min_key > exact_keys_->back()) {
        return true;
      }
      size_t lo = 0;
      size_t hi = exact_keys_->size();
      while (lo < hi) {
        const size_t mid = lo + ((hi - lo) >> 1);
        if ((*exact_keys_)[mid] < min_key) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return lo == exact_keys_->size() || (*exact_keys_)[lo] > max_key;
    }
    return false;
  }
  [[nodiscard]] bool has_dense_bits() const noexcept { return has_dense_bits_; }
  [[nodiscard]] const uint64_t* dense_bits_data() const noexcept {
    return dense_bits_.data();
  }
  [[nodiscard]] size_t dense_bits_word_count() const noexcept {
    return dense_bits_.size();
  }
  [[nodiscard]] bool has_bounds() const noexcept { return has_bounds_; }
  [[nodiscard]] int64_t Lo() const noexcept { return lo_; }
  [[nodiscard]] int64_t Hi() const noexcept { return hi_; }
  // A borrowed sorted enclosure of every key accepted by the conjunction.
  // Null means no exact part exists; callers must still apply Reject().
  [[nodiscard]] const std::vector<int64_t>* ExactKeys() const {
    return exact_keys_;
  }
  [[nodiscard]] size_t PartCount() const { return parts_.size(); }
  [[nodiscard]] bool Has(RuntimeFilterKind kind) const;
  // True when the group rejects unconditionally (the build side contributed no
  // comparable key at all). Such a filter has no comparison domain, so a
  // consumer may bind it to any INT64/DATE column regardless of the build
  // side's declared type.
  [[nodiscard]] bool RejectsAll() const { return reject_all_; }
  [[nodiscard]] size_t DistinctKeys() const;
  [[nodiscard]] size_t MemoryBytes() const;
  // Stable, order-independent summary for EXPLAIN / rule fingerprints.
  [[nodiscard]] std::string Describe() const;

 private:
  std::vector<std::unique_ptr<RuntimeFilter>> parts_;
  std::vector<uint64_t> dense_bits_;
  const std::vector<int64_t>* exact_keys_{nullptr};
  int64_t lo_{0};
  int64_t hi_{0};
  bool has_bounds_{false};
  bool has_dense_bits_{false};
  bool reject_all_{false};
};

// Ingests build-side key observations and picks the cheapest sound filter.
//
// Shape selection (Phase 2 "Dynamic Filter Join / SIP / Bloom"):
//   * nothing observed        -> nullptr, or RejectAll when allow_reject_all
//   * <= kMaxExactKeys keys   -> kExactSet (no false positives)
//   * otherwise               -> kMinMax + kBloom (SIP)
//   * more than kMaxTrackedKeys observations -> kMinMax only; the exact set
//     and the Bloom bits are dropped rather than truncated, because a filter
//     missing build keys would prune rows that DO match.
//
// MinMax is always worth including once a non-NULL key exists: it costs two
// comparisons and no allocation, and it is the only shape that prunes a probe
// scan when the build domain is a narrow range.
class RuntimeFilterBuilder {
 public:
  // Above this many distinct keys an exact set is replaced by the
  // range+membership pair; 4096 keys is 32 KiB, already more than the probe
  // scan saves in a typical plan.
  static constexpr size_t kMaxExactKeys = 4096;
  // Hard cap on retained keys (2M keys = 16 MiB). Beyond it only the running
  // [lo, hi] enclosure survives.
  static constexpr size_t kMaxTrackedKeys = 2000000;

  void Observe(int64_t key);
  void Reserve(size_t count) {
    keys_.reserve(std::min(count, kMaxTrackedKeys));
  }
  // A build row whose join key is NULL. NULLs never join, so they only matter
  // as "the build side is not all-NULL"; the filter itself stays NULL-free.
  void ObserveNull();
  [[nodiscard]] size_t DistinctKeys() const { return keys_.size(); }
  [[nodiscard]] size_t Observations() const { return observations_; }
  [[nodiscard]] bool saw_null() const { return saw_null_; }
  [[nodiscard]] bool empty() const { return observations_ == 0; }
  [[nodiscard]] bool overflowed() const { return overflowed_; }

  // Returns nullptr only when nothing was observed and `allow_reject_all` is
  // false; callers that want "no build key at all prunes everything" pass
  // true and get a RejectAll part.
  [[nodiscard]] std::shared_ptr<const RuntimeFilterGroup> Build(
      bool allow_reject_all = false,
      size_t max_exact_keys = kMaxExactKeys) const;

  // Convenience for callers that already hold a key collection (the
  // relational path's uncorrelated-IN membership filter).
  [[nodiscard]] static std::shared_ptr<const RuntimeFilterGroup> FromKeys(
      const std::vector<int64_t>& keys, size_t max_exact_keys = kMaxExactKeys);

 private:
  std::vector<int64_t> keys_;
  size_t observations_{0};
  bool saw_null_{false};
  bool overflowed_{false};
  bool has_key_{false};
  int64_t lo_{0};
  int64_t hi_{0};
};

// A filter bound to one scanned column.
//
// Rejection rule at the consumer:
//   * a NULL key is always rejected (an equi-join never matches NULL);
//   * a value whose decoded type differs from `type` is never rejected, so a
//     filter built over INT64 can never prune a DATE probe column (or an
//     unexpected type change) by comparing incomparable bit patterns.
struct ScanRuntimeFilter {
  std::shared_ptr<const RuntimeFilterGroup> group{};
  slot_t column{0};
  ValueType type{ValueType::kInt64};
  // Additional selective runtime filters on other columns of the same table
  // discovered by BuildInput when multiple selective join partners are already
  // loaded (e.g. lineitem joined with both filtered orders and filtered
  // supplier/part).
  std::vector<ScanRuntimeFilter> extra{};

  [[nodiscard]] bool enabled() const {
    return group != nullptr && !group->empty();
  }
  // Int64-domain rejection, for the pre-decode scan path where the caller
  // already knows the value is INT64/DATE (Row::TryPeekInteger).
  [[nodiscard]] bool Reject(int64_t key) const {
    return enabled() && group->Reject(key);
  }
  // Type-checked rejection for decoded rows; see RuntimeFilterBuilder.
  [[nodiscard]] bool Reject(int8_t value_type, bool is_null,
                            int64_t key) const {
    if (!enabled() || is_null) {
      return is_null && enabled();
    }
    if (value_type != static_cast<int8_t>(type)) {
      return false;
    }
    return group->Reject(key);
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_RUNTIME_FILTER_HPP
