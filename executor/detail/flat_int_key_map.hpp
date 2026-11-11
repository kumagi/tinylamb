/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP
#define TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

namespace tinylamb::relational_detail {

// Open-addressing int64 -> T table.  Grouped builds create millions of
// single-value groups (Q18/Q21 shapes), where a node-based unordered_map
// spends more time in the allocator than in the aggregate math.  Slots hold
// the mapped value inline, so a fresh group costs no allocation and an
// accumulate is one probe.  Iteration order is hash-slot order, NOT insertion
// order; callers that need per-group order must keep their own side arrays
// indexed by the mapped value.
//
// Two slot layouts, chosen by T:
//  - unsigned integral T (the group-index shape): a 16-byte {key, value}
//    slot whose occupancy is value == kEmptyValue (uint64 max).  A 2.9M-group
//    fold holds ~50 MB of slots either way, but the 16-byte slot puts 4 of
//    them per cache line instead of 2.67, which shrinks both the probe
//    traffic and the Grow() rehash pass by a third.  Callers must never store
//    kEmptyValue itself (group indices never do).
//  - any other T (including signed integers): the historical 24-byte
//    {key, value, used} slot.
//
// Insert contract: TryEmplace marks the slot occupied with a provisional
// value (zero) and the caller MUST overwrite the returned value immediately
// (every caller does: the mapped value is the new group's index).  Until the
// overwrite lands the slot reads as an occupied group 0, which is fine --
// only the same key re-probing in between could observe it, and no caller
// interleaves probes with the assignment.
template <typename T>
class FlatIntKeyMap {
 public:
  T* TryEmplace(int64_t key, bool* inserted = nullptr) {
    if ((size_ + 1) * 10 > Capacity() * 7) {
      Grow();
    }
    size_t slot = SlotOf(key);
    while (Occupied(slots_[slot])) {
      if (slots_[slot].key == key) {
        if (inserted != nullptr) {
          *inserted = false;
        }
        return &slots_[slot].value;
      }
      slot = (slot + 1) & mask_;
    }
    slots_[slot].key = key;
    MarkOccupied(slots_[slot]);
    ++size_;
    if (inserted != nullptr) {
      *inserted = true;
    }
    return &slots_[slot].value;
  }

  [[nodiscard]] size_t Size() const noexcept { return size_; }
  [[nodiscard]] bool Empty() const noexcept { return size_ == 0; }

  // Warms the slot for a key whose TryEmplace happens a few rows later (the
  // caller's filter work covers most of the line-fill latency).
  void Prefetch(int64_t key) const {
    if (slots_.empty()) {
      return;
    }
    __builtin_prefetch(&slots_[SlotOf(key)]);
  }

  // Read-only probe: never inserts, never grows.  nullptr when the key is
  // absent.
  [[nodiscard]] const T* Find(int64_t key) const {
    if (slots_.empty()) {
      return nullptr;
    }
    size_t slot = SlotOf(key);
    while (Occupied(slots_[slot])) {
      if (slots_[slot].key == key) {
        return &slots_[slot].value;
      }
      slot = (slot + 1) & mask_;
    }
    return nullptr;
  }

  template <typename Fn>
  void ForEach(Fn&& fn) {
    // slots_.size() == Capacity() whenever the map has grown at least once;
    // a never-grown map has no slots and visits nothing.
    for (size_t slot = 0; slot < slots_.size(); ++slot) {
      if (Occupied(slots_[slot])) {
        fn(slots_[slot].key, slots_[slot].value);
      }
    }
  }

  void Reserve(size_t expected) {
    if (expected == 0) {
      return;
    }
    const size_t min_cap = (expected * 10 + 6) / 7;
    size_t next = Capacity() < 16 ? 16 : Capacity();
    while (next < min_cap) {
      next *= 2;
    }
    if (next > Capacity()) {
      GrowTo(next);
    }
  }

 private:
  static constexpr bool kCompact =
      std::is_integral_v<T> && std::is_unsigned_v<T>;

  struct EmptyTag {};
  struct Slot {
    int64_t key{0};
    T value{};
    [[no_unique_address]] std::conditional_t<kCompact, EmptyTag, uint8_t>
        used{};
  };

  [[nodiscard]] static bool Occupied(const Slot& slot) {
    if constexpr (kCompact) {
      return slot.value != std::numeric_limits<T>::max();
    } else {
      return slot.used != 0;
    }
  }
  static void MarkOccupied(Slot& slot) {
    if constexpr (kCompact) {
      slot.value = T{};  // provisional occupied; caller overwrites
    } else {
      slot.used = 1;
    }
  }
  [[nodiscard]] static Slot EmptySlot() {
    Slot slot{};
    if constexpr (kCompact) {
      slot.value = std::numeric_limits<T>::max();
    }
    return slot;
  }

  [[nodiscard]] size_t Capacity() const { return mask_ + 1; }
  [[nodiscard]] size_t SlotOf(int64_t key) const {
    auto x = static_cast<uint64_t>(key);
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return static_cast<size_t>(x ^ (x >> 31)) & mask_;
  }
  void Grow() {
    // Quadruple instead of double: total rehash work across a build is
    // final_size * factor/(factor-1), so x4 pays ~1.33x the final table
    // instead of ~2x — callgrind put the rehash passes at a visible slice of
    // the multi-million-group folds' TryEmplace cost.
    const size_t next = Capacity() < 16 ? 16 : Capacity() * 4;
    GrowTo(next);
  }
  void GrowTo(size_t next) {
    std::vector<Slot> old_slots = std::move(slots_);
    mask_ = next - 1;
    slots_.clear();
    if constexpr (kCompact) {
      slots_.resize(next, EmptySlot());
    } else {
      slots_.resize(next);  // T may be non-copyable: value-init only
    }
    size_ = 0;
    for (Slot& old : old_slots) {
      if (!Occupied(old)) {
        continue;
      }
      size_t probe = SlotOf(old.key);
      while (Occupied(slots_[probe])) {
        probe = (probe + 1) & mask_;
      }
      slots_[probe].key = old.key;
      slots_[probe].value = std::move(old.value);
      if constexpr (!kCompact) {
        slots_[probe].used = 1;
      }
      ++size_;
    }
  }

  std::vector<Slot> slots_;
  size_t size_ = 0;
  size_t mask_ = 0;
};

// Immutable open-addressing set over int64 payloads (INT64 values and DATE
// day counts share the bit domain here).  Membership builds for
// `x [NOT] IN (uncorrelated subquery)` stream hundreds of thousands of rows
// into a distinct-member set (Q22 probes the 950K-row orders build, Q20 the
// 800K-row partsupp build), where unordered_set<Value>'s per-element node
// allocation and Value hashing dominate the build.  The set is built once
// (Reserve + Insert) and only probed afterwards: no tombstones, no growth,
// capacity fixed from the member count with FlatIntKeyMap's load rule.
// Duplicate keys re-probe to an occupied equal slot and are dropped.
class FlatIntSet {
 public:
  static constexpr uint64_t kMaxDenseSpan = 33'554'432ULL;

  [[nodiscard]] static size_t ReservationBytes(size_t members) {
    return sizeof(FlatIntSet) + (CapacityFor(members) * sizeof(Slot));
  }

  void Reserve(size_t members) {
    dense_overflow_.store(false, std::memory_order_relaxed);
    dense_mode_ = false;
    dense_bits_.clear();
    slots_.assign(CapacityFor(members), Slot{});
    mask_ = slots_.size() - 1;
    size_ = 0;
    has_bounds_ = false;
  }

  bool TryReserveDense(int64_t min_key, int64_t max_key) {
    if (min_key > max_key) {
      return false;
    }
    const uint64_t span =
        static_cast<uint64_t>(max_key) - static_cast<uint64_t>(min_key);
    if (span > kMaxDenseSpan) {
      return false;
    }
    slots_.clear();
    mask_ = 0;
    dense_bits_.assign(static_cast<size_t>((span >> 6) + 1), 0ULL);
    dense_overflow_.store(false, std::memory_order_relaxed);
    dense_mode_ = true;
    dense_min_ = min_key;
    dense_max_ = max_key;
    size_ = 0;
    has_bounds_ = false;
    return true;
  }

  [[nodiscard]] bool IsDenseMode() const noexcept { return dense_mode_; }

  // A failed dense build must be discarded before publishing a predicate.
  [[nodiscard]] bool DenseOverflowed() const noexcept {
    return dense_overflow_.load(std::memory_order_relaxed);
  }

  void InsertDenseAtomic(int64_t key) noexcept {
    if (!dense_mode_ || key < dense_min_ || key > dense_max_) {
      dense_overflow_.store(true, std::memory_order_relaxed);
      return;
    }
    const uint64_t off =
        static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min_);
    const uint64_t bit_mask = uint64_t{1} << (off & 63);
    uint64_t& word = dense_bits_[off >> 6];
    if ((std::atomic_ref<uint64_t>(word).load(std::memory_order_relaxed) &
         bit_mask) == 0) {
      std::atomic_ref<uint64_t>(word).fetch_or(bit_mask,
                                               std::memory_order_relaxed);
    }
  }

  void FinalizeDenseBounds() noexcept {
    size_ = 0;
    has_bounds_ = false;
    for (size_t w = 0; w < dense_bits_.size(); ++w) {
      const uint64_t word = dense_bits_[w];
      if (word == 0) {
        continue;
      }
      size_ += static_cast<size_t>(__builtin_popcountll(word));
      const int64_t lo_k =
          dense_min_ +
          static_cast<int64_t>((static_cast<uint64_t>(w) << 6) +
                               static_cast<uint64_t>(__builtin_ctzll(word)));
      const int64_t hi_k =
          dense_min_ + static_cast<int64_t>(
                           (static_cast<uint64_t>(w) << 6) +
                           static_cast<uint64_t>(63 - __builtin_clzll(word)));
      if (!has_bounds_) {
        has_bounds_ = true;
        min_key_ = lo_k;
        max_key_ = hi_k;
      } else {
        max_key_ = hi_k;
      }
    }
  }

  void Insert(int64_t key) {
    if (dense_mode_ && (key < dense_min_ || key > dense_max_)) {
      dense_overflow_.store(true, std::memory_order_relaxed);
      return;
    }
    if (!has_bounds_) {
      has_bounds_ = true;
      min_key_ = key;
      max_key_ = key;
    } else {
      min_key_ = std::min(key, min_key_);
      max_key_ = std::max(key, max_key_);
    }
    if (dense_mode_) {
      const uint64_t off =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min_);
      const uint64_t bit_mask = uint64_t{1} << (off & 63);
      uint64_t& word = dense_bits_[off >> 6];
      if ((word & bit_mask) == 0) {
        word |= bit_mask;
        ++size_;
      }
      return;
    }
    size_t slot = SlotOf(key);
    while (slots_[slot].used != 0) {
      if (slots_[slot].key == key) {
        return;
      }
      slot = (slot + 1) & mask_;
    }
    slots_[slot].key = key;
    slots_[slot].used = 1;
    ++size_;
  }

  [[nodiscard]] bool Contains(int64_t key) const noexcept {
    if (!has_bounds_ || key < min_key_ || key > max_key_) {
      return false;
    }
    if (dense_mode_) {
      const uint64_t off =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min_);
      return (dense_bits_[off >> 6] & (uint64_t{1} << (off & 63))) != 0;
    }
    size_t slot = SlotOf(key);
    while (slots_[slot].used != 0) {
      if (slots_[slot].key == key) {
        return true;
      }
      slot = (slot + 1) & mask_;
    }
    return false;
  }

  [[nodiscard]] bool ContainsAnyInRange(int64_t lo, int64_t hi) const noexcept {
    if (!has_bounds_ || lo > hi || hi < min_key_ || lo > max_key_) {
      return false;
    }
    if (dense_mode_) {
      const int64_t clamped_lo = lo > min_key_ ? lo : min_key_;
      const int64_t clamped_hi = hi < max_key_ ? hi : max_key_;
      if (clamped_lo > clamped_hi) {
        return false;
      }
      const uint64_t off_lo =
          static_cast<uint64_t>(clamped_lo) - static_cast<uint64_t>(dense_min_);
      const uint64_t off_hi =
          static_cast<uint64_t>(clamped_hi) - static_cast<uint64_t>(dense_min_);
      const auto word_lo = static_cast<size_t>(off_lo >> 6);
      const auto word_hi = static_cast<size_t>(off_hi >> 6);
      if (word_hi - word_lo <= 8) {
        const uint64_t low_mask = ~uint64_t{0} << (off_lo & 63);
        const uint64_t high_mask = (~uint64_t{0}) >> (63 - (off_hi & 63));
        if (word_lo == word_hi) {
          return (dense_bits_[word_lo] & low_mask & high_mask) != 0;
        }
        if ((dense_bits_[word_lo] & low_mask) != 0) {
          return true;
        }
        for (size_t w = word_lo + 1; w < word_hi; ++w) {
          if (dense_bits_[w] != 0) {
            return true;
          }
        }
        return (dense_bits_[word_hi] & high_mask) != 0;
      }
    }
    return true;
  }

  [[nodiscard]] bool Empty() const noexcept { return size_ == 0; }
  [[nodiscard]] size_t Size() const noexcept { return size_; }
  [[nodiscard]] bool HasBounds() const noexcept { return has_bounds_; }
  [[nodiscard]] int64_t MinKey() const noexcept { return min_key_; }
  [[nodiscard]] int64_t MaxKey() const noexcept { return max_key_; }

  // Visits every distinct member.  Build-once contract: the set is frozen
  // while this runs.
  template <typename Fn>
  void ForEach(Fn&& fn) const {
    if (dense_mode_) {
      for (size_t w = 0; w < dense_bits_.size(); ++w) {
        uint64_t word = dense_bits_[w];
        while (word != 0) {
          const auto bit = static_cast<unsigned>(__builtin_ctzll(word));
          const int64_t key =
              dense_min_ +
              static_cast<int64_t>((static_cast<uint64_t>(w) << 6) + bit);
          fn(key);
          word &= word - 1;
        }
      }
      return;
    }
    for (const Slot& slot : slots_) {
      if (slot.used != 0) {
        fn(slot.key);
      }
    }
  }

 private:
  struct Slot {
    int64_t key;
    uint8_t used;
  };

  static size_t CapacityFor(size_t members) {
    size_t capacity = 16;
    while ((members + 1) * 10 > capacity * 7) {
      capacity *= 4;
    }
    return capacity;
  }

  [[nodiscard]] size_t SlotOf(int64_t key) const {
    auto x = static_cast<uint64_t>(key);
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return static_cast<size_t>(x ^ (x >> 31)) & mask_;
  }

  std::vector<Slot> slots_;
  std::vector<uint64_t> dense_bits_;
  int64_t min_key_ = 0;
  int64_t max_key_ = 0;
  int64_t dense_min_ = 0;
  int64_t dense_max_ = 0;
  size_t size_ = 0;
  size_t mask_ = 0;
  std::atomic<bool> dense_overflow_{false};
  bool dense_mode_ = false;
  bool has_bounds_ = false;
};

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP
