/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP
#define TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP

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
  T* TryEmplace(int64_t key, bool* inserted) {
    if ((size_ + 1) * 10 > Capacity() * 7) {
      Grow();
    }
    size_t slot = SlotOf(key);
    while (Occupied(slots_[slot])) {
      if (slots_[slot].key == key) {
        *inserted = false;
        return &slots_[slot].value;
      }
      slot = (slot + 1) & mask_;
    }
    slots_[slot].key = key;
    MarkOccupied(slots_[slot]);
    ++size_;
    *inserted = true;
    return &slots_[slot].value;
  }

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

 private:
  static constexpr bool kCompact =
      std::is_integral_v<T> && std::is_unsigned_v<T>;
  // Compact-layout vacancy marker; callers never store this value (it would
  // need 2^64 - 1 groups).  Non-compact layouts keep the explicit used byte.
  static constexpr T kEmptyValue =
      kCompact ? std::numeric_limits<T>::max() : T{};

  struct EmptyTag {};
  struct Slot {
    int64_t key;
    T value;
    [[no_unique_address]] std::conditional_t<kCompact, EmptyTag, uint8_t> used;
  };

  [[nodiscard]] static bool Occupied(const Slot& slot) {
    if constexpr (kCompact) {
      return slot.value != kEmptyValue;
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
      slot.value = kEmptyValue;
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
  void Reserve(size_t members) {
    size_t capacity = 16;
    while ((members + 1) * 10 > capacity * 7) {
      capacity *= 4;
    }
    slots_.assign(capacity, Slot{});
    mask_ = capacity - 1;
  }

  void Insert(int64_t key) {
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

  [[nodiscard]] bool Contains(int64_t key) const {
    size_t slot = SlotOf(key);
    while (slots_[slot].used != 0) {
      if (slots_[slot].key == key) {
        return true;
      }
      slot = (slot + 1) & mask_;
    }
    return false;
  }

  [[nodiscard]] bool Empty() const { return size_ == 0; }
  [[nodiscard]] size_t Size() const { return size_; }

  // Visits every distinct member.  Build-once contract: the set is frozen
  // while this runs.
  template <typename Fn>
  void ForEach(Fn&& fn) const {
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

  [[nodiscard]] size_t SlotOf(int64_t key) const {
    auto x = static_cast<uint64_t>(key);
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return static_cast<size_t>(x ^ (x >> 31)) & mask_;
  }

  std::vector<Slot> slots_;
  size_t size_ = 0;
  size_t mask_ = 0;
};

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP
