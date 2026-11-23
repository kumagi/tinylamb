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
//    slot whose occupancy is value == std::numeric_limits<uint64_t>::max()
//    (uint64 max).  At the shared capacity a 2.9M-group fold holds ~64 MiB of
//    16-byte slots vs ~96 MiB of 24-byte slots (4 vs 2.67 per cache line),
//    which shrinks both the probe traffic and the Grow() rehash pass by a
//    third. Callers must never store std::numeric_limits<uint64_t>::max()
//    itself (group indices never do).
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

  // Distinct aggregation owns the reservation alongside this map, including
  // when an aggregate is moved to another worker during merge.
  template <typename Charge>
  [[nodiscard]] bool TryInsertCharged(int64_t key, Charge& charge) {
    if ((size_ + 1) * 10 > Capacity() * 7) {
      if (Find(key) != nullptr) {
        return true;
      }
      const size_t next = NextCapacity();
      if (next <= Capacity() || next > slots_.max_size() ||
          !charge.TryAdd(next * sizeof(Slot))) {
        return false;
      }
      const size_t old_bytes = slots_.capacity() * sizeof(Slot);
      GrowTo(next);
      charge.Release(old_bytes);
    }
    TryEmplace(key);
    return true;
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

  void Clear() {
    if constexpr (kCompact) {
      if (!slots_.empty()) {
        // EmptySlot() value-initializes the whole Slot (key included) before
        // setting the "empty" value, so this resets keys as well — no stale
        // key from a previous occupancy survives Clear().
        std::fill(slots_.begin(), slots_.end(), EmptySlot());
      }
    } else {
      for (Slot& slot : slots_) {
        slot.used = 0;
        slot.value = T{};
      }
    }
    size_ = 0;
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
  [[nodiscard]] size_t NextCapacity() const {
    // Quadruple instead of double: total rehash work across a build is
    // final_size * factor/(factor-1), so x4 pays ~1.33x the final table
    // instead of ~2x — callgrind put the rehash passes at a visible slice of
    // the multi-million-group folds' TryEmplace cost.
    return Capacity() < 16 ? 16 : Capacity() * 4;
  }
  void Grow() { GrowTo(NextCapacity()); }
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
  // Keep exact membership in at most 16 MiB instead of allocating a
  // multi-gigabyte hash table for a wide but compact integer domain.
  static constexpr uint64_t kMaxDenseSpan = 134'217'727ULL;

  [[nodiscard]] static size_t ReservationBytes(size_t members) {
    return sizeof(FlatIntSet) + (CapacityFor(members) * sizeof(Slot));
  }

  void Reserve(size_t members) {
    dense_overflow_.store(false, std::memory_order_relaxed);
    dense_mode_ = false;
    dense_bits_.clear();
    slots_.assign(CapacityFor(members), Slot{});
    mask_ = slots_.size() - 1;
    size_.store(0, std::memory_order_relaxed);
    ResetBounds();
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
    size_.store(0, std::memory_order_relaxed);
    ResetBounds();
    return true;
  }

  // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
  //   (dense_compact_membership_eq)
  // After all keys are inserted into hash mode, compact to a dense bitmap when
  // the observed [min_key, max_key] span fits within kMaxDenseSpan and the
  // bitmap is no larger than the hash table (or <= 2 MiB). This shrinks
  // dense integer sets (e.g. 600K order numbers: 16 MiB hash -> 75 KiB bitmap)
  // into L2 cache and makes ContainsAnyInRange O((hi - lo) / 64).
  bool TryCompactToDense() {
    if (dense_mode_ || !has_bounds_.load(std::memory_order_relaxed)) {
      return dense_mode_;
    }
    const int64_t min_k = min_key_.load(std::memory_order_relaxed);
    const int64_t max_k = max_key_.load(std::memory_order_relaxed);
    if (min_k > max_k) {
      return false;
    }
    const uint64_t span =
        static_cast<uint64_t>(max_k) - static_cast<uint64_t>(min_k);
    if (span > kMaxDenseSpan) {
      return false;
    }
    const auto words = static_cast<size_t>((span >> 6) + 1);
    const size_t dense_bytes = words * sizeof(uint64_t);
    const size_t hash_bytes = slots_.size() * sizeof(Slot);
    if (dense_bytes > std::max<size_t>(hash_bytes, size_t{2} << 20)) {
      return false;
    }
    std::vector<uint64_t> bits(words, 0ULL);
    for (const Slot& slot : slots_) {
      if (slot.used != 0) {
        const uint64_t off =
            static_cast<uint64_t>(slot.key) - static_cast<uint64_t>(min_k);
        bits[off >> 6] |= uint64_t{1} << (off & 63);
      }
    }
    slots_.clear();
    slots_.shrink_to_fit();
    mask_ = 0;
    dense_bits_ = std::move(bits);
    dense_min_ = min_k;
    dense_max_ = max_k;
    dense_mode_ = true;
    dense_overflow_.store(false, std::memory_order_relaxed);
    return true;
  }

  [[nodiscard]] bool IsDenseMode() const noexcept { return dense_mode_; }
  [[nodiscard]] int64_t DenseMin() const noexcept { return dense_min_; }
  [[nodiscard]] int64_t DenseMax() const noexcept { return dense_max_; }
  [[nodiscard]] size_t DenseWordCount() const noexcept {
    return dense_bits_.size();
  }

  // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
  //   (parallel_bitset_or_eq_union)
  void MergeDenseWordsAtomic(const uint64_t* local_words,
                             size_t count) noexcept {
    const size_t n = std::min(count, dense_bits_.size());
    for (size_t i = 0; i < n; ++i) {
      if (local_words[i] != 0) {
        std::atomic_ref<uint64_t>(dense_bits_[i])
            .fetch_or(local_words[i], std::memory_order_relaxed);
      }
    }
  }

  // A failed dense build must be discarded before publishing a predicate.
  [[nodiscard]] bool DenseOverflowed() const noexcept {
    return dense_overflow_.load(std::memory_order_relaxed);
  }

  // A worker-local accumulator amortizes atomic updates for nearby keys.
  // Writers may overlap arbitrarily; publication still waits for all writers
  // to be destroyed, then calls FinalizeDenseBounds().
  class DenseWriter {
   public:
    explicit DenseWriter(FlatIntSet& set) : set_(set) {}
    DenseWriter(const DenseWriter&) = delete;
    DenseWriter& operator=(const DenseWriter&) = delete;
    DenseWriter(DenseWriter&&) = delete;
    DenseWriter& operator=(DenseWriter&&) = delete;
    ~DenseWriter() { Flush(); }

    void Insert(int64_t key) noexcept {
      if (!set_.dense_mode_ || key < set_.dense_min_ || key > set_.dense_max_) {
        set_.dense_overflow_.store(true, std::memory_order_relaxed);
        return;
      }
      const uint64_t offset =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(set_.dense_min_);
      const auto word = static_cast<size_t>(offset >> 6);
      if (word != word_) {
        Flush();
        word_ = word;
      }
      bits_ |= uint64_t{1} << (offset & 63);
    }

   private:
    void Flush() noexcept {
      if (bits_ == 0) {
        return;
      }
      std::atomic_ref<uint64_t>(set_.dense_bits_[word_])
          .fetch_or(bits_, std::memory_order_relaxed);
      bits_ = 0;
    }
    FlatIntSet& set_;
    size_t word_{0};
    uint64_t bits_{0};
  };

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
    size_t count = 0;
    bool have_bounds = false;
    int64_t lo = 0;
    int64_t hi = 0;
    for (size_t w = 0; w < dense_bits_.size(); ++w) {
      const uint64_t word = dense_bits_[w];
      if (word == 0) {
        continue;
      }
      count += static_cast<size_t>(__builtin_popcountll(word));
      const int64_t lo_k =
          dense_min_ +
          static_cast<int64_t>((static_cast<uint64_t>(w) << 6) +
                               static_cast<uint64_t>(__builtin_ctzll(word)));
      const int64_t hi_k =
          dense_min_ + static_cast<int64_t>(
                           (static_cast<uint64_t>(w) << 6) +
                           static_cast<uint64_t>(63 - __builtin_clzll(word)));
      if (!have_bounds) {
        have_bounds = true;
        lo = lo_k;
        hi = hi_k;
      } else {
        hi = hi_k;
      }
    }
    size_.store(count, std::memory_order_relaxed);
    if (have_bounds) {
      min_key_.store(lo, std::memory_order_relaxed);
      max_key_.store(hi, std::memory_order_relaxed);
    }
    has_bounds_.store(have_bounds, std::memory_order_relaxed);
  }

  void Insert(int64_t key) {
    if (dense_mode_ && (key < dense_min_ || key > dense_max_)) {
      dense_overflow_.store(true, std::memory_order_relaxed);
      return;
    }
    // Writers may overlap (the dense build uses several workers).  Publish the
    // range with atomic RMWs so concurrent inserts converge on the true
    // [min, max] instead of losing one another's extreme key: Contains uses
    // this range as its first gate, so a stale bound hides real members.
    AtomicFetchMin(min_key_, key);
    AtomicFetchMax(max_key_, key);
    has_bounds_.store(true, std::memory_order_relaxed);
    if (dense_mode_) {
      const uint64_t off =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min_);
      const uint64_t bit_mask = uint64_t{1} << (off & 63);
      uint64_t& word = dense_bits_[off >> 6];
      if ((std::atomic_ref<uint64_t>(word).fetch_or(bit_mask,
                                                    std::memory_order_relaxed) &
           bit_mask) == 0) {
        size_.fetch_add(1, std::memory_order_relaxed);
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
    size_.fetch_add(1, std::memory_order_relaxed);
  }

  [[nodiscard]] bool Contains(int64_t key) const noexcept {
    if (!has_bounds_.load(std::memory_order_relaxed) ||
        key < min_key_.load(std::memory_order_relaxed) ||
        key > max_key_.load(std::memory_order_relaxed)) {
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
    const int64_t min_k = min_key_.load(std::memory_order_relaxed);
    const int64_t max_k = max_key_.load(std::memory_order_relaxed);
    if (!has_bounds_.load(std::memory_order_relaxed) || lo > hi || hi < min_k ||
        lo > max_k) {
      return false;
    }
    if (dense_mode_) {
      const int64_t clamped_lo = lo > min_k ? lo : min_k;
      const int64_t clamped_hi = hi < max_k ? hi : max_k;
      if (clamped_lo > clamped_hi) {
        return false;
      }
      const uint64_t off_lo =
          static_cast<uint64_t>(clamped_lo) - static_cast<uint64_t>(dense_min_);
      const uint64_t off_hi =
          static_cast<uint64_t>(clamped_hi) - static_cast<uint64_t>(dense_min_);
      const auto word_lo = static_cast<size_t>(off_lo >> 6);
      const auto word_hi = static_cast<size_t>(off_hi >> 6);
      // Dense sets keep no hash slots, so every width is answered from the
      // bitmap; a wide range over a large bitmap stays conservative (true).
      if (word_hi - word_lo > 4096) {
        return true;
      }
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
    // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
    //   (range_check_conservative_sound)
    // Hash mode: when the query range covers the whole member domain every key
    // is in range (O(1)). For narrow ranges (<= 64 distinct integers), probe
    // Contains() directly in O(hi - lo). For large hash tables with wide block
    // ranges, return true (sound conservative over-approximation) instead of
    // linearly scanning millions of hash slots on every page block.
    if (lo <= min_k && hi >= max_k) {
      return true;
    }
    const int64_t clamped_lo = lo > min_k ? lo : min_k;
    const int64_t clamped_hi = hi < max_k ? hi : max_k;
    if (clamped_lo > clamped_hi) {
      return false;
    }
    const uint64_t width =
        static_cast<uint64_t>(clamped_hi) - static_cast<uint64_t>(clamped_lo);
    if (width <= 64ULL) {
      for (uint64_t off = 0; off <= width; ++off) {
        const auto k =
            static_cast<int64_t>(static_cast<uint64_t>(clamped_lo) + off);
        if (Contains(k)) {
          return true;
        }
      }
      return false;
    }
    if (slots_.size() > 1024) {
      return true;
    }
    for (const Slot& slot : slots_) {  // NOLINT(readability-use-anyofallof)
      if (slot.used != 0 && slot.key >= clamped_lo && slot.key <= clamped_hi) {
        return true;
      }
    }
    return false;
  }

  [[nodiscard]] bool Empty() const noexcept {
    return size_.load(std::memory_order_relaxed) == 0;
  }
  [[nodiscard]] size_t Size() const noexcept {
    return size_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] bool HasBounds() const noexcept {
    return has_bounds_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t MinKey() const noexcept {
    return min_key_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t MaxKey() const noexcept {
    return max_key_.load(std::memory_order_relaxed);
  }

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

  // The bounds start at sentinels so every Insert can use an atomic RMW (C++20
  // has no atomic fetch_min/fetch_max).  ResetBounds is a single-threaded
  // lifecycle step; Insert and the readers below are the concurrent part.
  void ResetBounds() noexcept {
    min_key_.store(std::numeric_limits<int64_t>::max(),
                   std::memory_order_relaxed);
    max_key_.store(std::numeric_limits<int64_t>::min(),
                   std::memory_order_relaxed);
    has_bounds_.store(false, std::memory_order_relaxed);
  }
  static void AtomicFetchMin(std::atomic<int64_t>& target,
                             int64_t key) noexcept {
    int64_t cur = target.load(std::memory_order_relaxed);
    while (key < cur &&
           !target.compare_exchange_weak(cur, key, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
  }
  static void AtomicFetchMax(std::atomic<int64_t>& target,
                             int64_t key) noexcept {
    int64_t cur = target.load(std::memory_order_relaxed);
    while (key > cur &&
           !target.compare_exchange_weak(cur, key, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
  }

  std::vector<Slot> slots_;
  std::vector<uint64_t> dense_bits_;
  std::atomic<int64_t> min_key_{std::numeric_limits<int64_t>::max()};
  std::atomic<int64_t> max_key_{std::numeric_limits<int64_t>::min()};
  int64_t dense_min_ = 0;
  int64_t dense_max_ = 0;
  std::atomic<size_t> size_{0};
  size_t mask_ = 0;
  std::atomic<bool> dense_overflow_{false};
  bool dense_mode_ = false;
  std::atomic<bool> has_bounds_{false};
};

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_FLAT_INT_KEY_MAP_HPP
