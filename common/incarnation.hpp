#ifndef TINYLAMB_INCARNATION_HPP
#define TINYLAMB_INCARNATION_HPP

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"

namespace tinylamb {

// Per-catalog-entry state shared by every Table copy of the same table
// (Database::GetTable hands each caller its own Table value, all bound to one
// TableIncarnation). Besides drop/lease coordination it carries the *physical
// row-layout* bookkeeping the clustered-order proof in
// table/table_statistics.hpp depends on:
//
//   * LayoutGeneration() advances on every row mutation (insert, update,
//     delete) at both the entry and the exit of the operation. A proof
//     measured by ANALYZE therefore stops matching the moment a writer -- even
//     one that later aborts, or one that is still in flight while ANALYZE
//     scans -- touches the chain.
//   * RowsRewrittenInPlace() latches once an UPDATE or DELETE has rewritten or
//     vacated a slot in this process. Older MVCC versions are in-memory only,
//     so no pre-image survives a restart; while the flag is set a reader with
//     an older snapshot could still resolve a pre-image whose value does not
//     respect the measured order, and no physical-layout measurement can prove
//     otherwise.
//
// Generations are seeded from a process-global counter with a wide stride so
// two incarnations can never hold the same value, and a persisted statistics
// record written by a previous process can never be matched by a fresh
// incarnation (0 stays reserved for "unknown").
class TableIncarnation {
 public:
  enum class State : uint8_t { kLive, kDropPending, kRetired };

  static constexpr uint64_t kLayoutGenerationStride = 1ULL << 32;

  TableIncarnation() {
    std::weak_ptr<ColumnarCacheState> weak = columnar_cache_;
    reclaim_hook_ = ScopedReclaimHook(
        ReclaimStage::kColumnar,
        [weak = std::move(weak)](size_t target) {
          auto state = weak.lock();
          if (!state) {
            return size_t{0};
          }
          return ReclaimFromState(*state, target, /*try_lock_only=*/true);
        },
        columnar_cache_->recency);
  }

  ~TableIncarnation() { reclaim_hook_.Reset(); }
  TableIncarnation(const TableIncarnation&) = delete;
  TableIncarnation& operator=(const TableIncarnation&) = delete;
  TableIncarnation(TableIncarnation&&) = delete;
  TableIncarnation& operator=(TableIncarnation&&) = delete;

  bool Acquire(txn_id_t owner) {
    std::scoped_lock lock(mutex_);
    if (state_ != State::kLive) {
      return false;
    }
    ++owner_leases_[owner];
    ++active_leases_;
    return true;
  }

  void Release(txn_id_t owner) {
    std::scoped_lock lock(mutex_);
    if (active_leases_ == 0) {
      return;
    }
    auto found = owner_leases_.find(owner);
    if (found == owner_leases_.end()) {
      return;  // unknown owner: do not break the active/owner accounting
    }
    --active_leases_;
    if (--found->second == 0) {
      owner_leases_.erase(found);
    }
    if (active_leases_ == 0) {
      quiescent_.notify_all();
    }
  }

  // True while no drop has begun on this incarnation.  The compiled-plan
  // cache probes this (via Database::IsCurrentIncarnation) instead of
  // paying a catalog read + full Table deserialization per replay.
  [[nodiscard]] bool IsLive() const {
    std::scoped_lock lock(mutex_);
    return state_ == State::kLive;
  }

  bool BeginDrop() {
    std::scoped_lock lock(mutex_);
    if (state_ != State::kLive) {
      return false;
    }
    state_ = State::kDropPending;
    return true;
  }

  bool WaitForQuiescence(txn_id_t owner, bool non_blocking = false) {
    std::unique_lock lock(mutex_);
    if (owner_leases_.contains(owner)) {
      return false;
    }
    if (non_blocking) {
      return active_leases_ == 0;
    }
    // Do not block a DDL worker forever on a leaked/long-lived lease: the
    // callers already treat false as a retryable conflict. The bound is
    // configurable so a workload with legitimately long readers (or a test
    // exercising the timeout path) need not be pinned to an arbitrary 60s.
    using Clock = std::chrono::steady_clock;
    using Milliseconds = std::chrono::milliseconds;
    static const auto timeout = [] {
      constexpr auto kDefault =
          std::chrono::duration_cast<Clock::duration>(Milliseconds(60000));
      constexpr auto kMaxMillis = static_cast<unsigned long long>(
          std::chrono::duration_cast<Milliseconds>(Clock::duration::max())
              .count());
      const char* env = std::getenv("TINYLAMB_DDL_QUIESCENCE_TIMEOUT_MS");
      if (env == nullptr || *env == '\0') {
        return kDefault;
      }
      const auto is_space = [](char c) {
        return std::isspace(static_cast<unsigned char>(c)) != 0;
      };
      const char* begin = env;
      while (*begin != '\0' && is_space(*begin)) {
        ++begin;
      }
      const char* end = begin;
      while (*end != '\0') {
        ++end;
      }
      while (end > begin && is_space(end[-1])) {
        --end;
      }
      if (begin < end && *begin == '+') {
        ++begin;
      }
      if (begin == end) {
        return kDefault;
      }
      unsigned long long ms = 0;
      for (const char* p = begin; p != end; ++p) {
        if (*p < '0' || *p > '9') {
          return kDefault;
        }
        const auto digit = static_cast<unsigned>(*p - '0');
        if (ms > (kMaxMillis - digit) / 10) {
          return kDefault;
        }
        ms = (ms * 10) + digit;
      }
      return std::chrono::duration_cast<Clock::duration>(Milliseconds(ms));
    }();
    const auto now = Clock::now();
    const auto max = Clock::time_point::max();
    const auto deadline = now > max - timeout ? max : now + timeout;
    return quiescent_.wait_until(lock, deadline,
                                 [&] { return active_leases_ == 0; });
  }

  void CommitDrop() {
    bool retired = false;
    {
      std::scoped_lock lock(mutex_);
      if (state_ == State::kDropPending) {
        state_ = State::kRetired;
      }
      retired = state_ == State::kRetired;
      quiescent_.notify_all();
    }
    if (retired) {
      DropColumnarCaches();
    }
  }

  // Releases every columnar image this incarnation caches (full / projected
  // LSTable stores and the backing TLCS file).  Called once nobody can scan
  // the incarnation any more: DROP TABLE committed, or the owning Database is
  // closing.  Compiled plans keep Table copies -- and so this object -- alive
  // until the process-wide plan cache evicts them; before this, a closed
  // database's columnar bytes stayed charged to the columnar pool (and an
  // unlinked .tlcs mapping stayed open) for that whole time
  // (sql_ooc_fuzzer: "memory pool accounting drifted across the session").
  // Scans already holding a store keep their own reference.
  void DropColumnarCaches() {
    std::vector<std::shared_ptr<const void>> dropped;
    std::shared_ptr<const void> old_tlcs;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      if (columnar_cache_->lstable_store) {
        dropped.push_back(std::move(columnar_cache_->lstable_store));
      }
      if (columnar_cache_->projected_lstable_store) {
        dropped.push_back(std::move(columnar_cache_->projected_lstable_store));
      }
      if (columnar_cache_->block_layout) {
        dropped.push_back(std::move(columnar_cache_->block_layout));
      }
      columnar_cache_->block_layout_generation = 0;
      for (auto& view : columnar_cache_->views) {
        if (view.store) {
          dropped.push_back(std::move(view.store));
        }
      }
      columnar_cache_->views.clear();
      for (auto& [col, slot] : columnar_cache_->columns) {
        if (slot.entry) {
          dropped.push_back(std::move(slot.entry));
        }
      }
      columnar_cache_->columns.clear();
      columnar_cache_->rejected_columns.clear();
      columnar_cache_->rejected_columns_generation = 0;
      old_tlcs = std::move(backing_tlcs_store_);
    }
  }

  void AbortDrop() {
    std::scoped_lock lock(mutex_);
    if (state_ == State::kDropPending) {
      state_ = State::kLive;
    }
    quiescent_.notify_all();
  }

  // Physical row-layout generation; see the class comment for the contract
  // TableStatistics::LayoutGeneration() relies on.
  [[nodiscard]] uint64_t LayoutGeneration() const noexcept {
    return layout_generation_.load(std::memory_order_acquire);
  }
  // Called on entry to and on exit from every row mutation, so a writer that is
  // still running while an ANALYZE scans can never leave a proof behind that
  // the ANALYZE would accept.
  void BumpLayoutGeneration() noexcept {
    layout_generation_.fetch_add(1, std::memory_order_acq_rel);
  }
  // Latched by UPDATE/DELETE: those rewrite a slot in place, so a snapshot
  // older than the ANALYZE can resolve a pre-image the physical measurement
  // never saw.
  [[nodiscard]] bool RowsRewrittenInPlace() const noexcept {
    return rows_rewritten_in_place_.load(std::memory_order_acquire);
  }
  void LatchRowRewrite() noexcept {
    rows_rewritten_in_place_.store(true, std::memory_order_release);
  }

  [[nodiscard]] std::shared_ptr<const void> GetLSTableStoreRaw() const {
    std::scoped_lock lock(columnar_cache_->mu);
    if (columnar_cache_->lstable_store) {
      TouchColumnarCache();
    }
    return columnar_cache_->lstable_store;
  }
  void SetLSTableStoreRaw(std::shared_ptr<const void> store) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old = std::move(columnar_cache_->lstable_store);
      columnar_cache_->lstable_store = std::move(store);
      TouchColumnarCache();
    }
  }
  [[nodiscard]] std::shared_ptr<const void> GetProjectedLSTableStoreRaw()
      const {
    std::scoped_lock lock(columnar_cache_->mu);
    if (columnar_cache_->projected_lstable_store) {
      TouchColumnarCache();
      return columnar_cache_->projected_lstable_store;
    }
    const ColumnarCacheState::CachedViewSlot* best = nullptr;
    for (const auto& view : columnar_cache_->views) {
      if (view.store && (best == nullptr || view.last_used > best->last_used)) {
        best = &view;
      }
    }
    if (best != nullptr) {
      TouchColumnarCache();
      return best->store;
    }
    return nullptr;
  }
  void SetProjectedLSTableStoreRaw(std::shared_ptr<const void> store) {
    std::vector<std::shared_ptr<const void>> dropped;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      if (columnar_cache_->projected_lstable_store) {
        dropped.push_back(std::move(columnar_cache_->projected_lstable_store));
      }
      for (auto& view : columnar_cache_->views) {
        if (view.store) {
          dropped.push_back(std::move(view.store));
        }
      }
      columnar_cache_->views.clear();
      columnar_cache_->projected_lstable_store = std::move(store);
      TouchColumnarCache();
    }
  }
  [[nodiscard]] std::shared_ptr<const void> GetCachedColumnRaw(
      uint32_t col, uint64_t generation) const {
    std::scoped_lock lock(columnar_cache_->mu);
    auto it = columnar_cache_->columns.find(col);
    if (it == columnar_cache_->columns.end() ||
        it->second.generation != generation || !it->second.entry) {
      return nullptr;
    }
    TouchColumnarCache();
    it->second.last_used = columnar_cache_->touch_clock;
    return it->second.entry;
  }
  void PutCachedColumnRaw(
      uint32_t col, uint64_t generation, size_t bytes,
      std::shared_ptr<const void> entry,
      std::shared_ptr<const std::atomic<size_t>> dynamic_bytes = nullptr) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      TouchColumnarCache();
      const uint64_t tick = columnar_cache_->touch_clock;
      auto it = columnar_cache_->columns.find(col);
      if (!entry) {
        if (it != columnar_cache_->columns.end()) {
          old = std::move(it->second.entry);
          columnar_cache_->columns.erase(it);
        }
        return;
      }
      if (it == columnar_cache_->columns.end()) {
        columnar_cache_->columns.emplace(
            col, ColumnarCacheState::CachedColumnSlot{
                     .generation=generation, .last_used=tick, .bytes=bytes, .entry=std::move(entry),
                     .dynamic_bytes=std::move(dynamic_bytes)});
      } else {
        old = std::move(it->second.entry);
        it->second.generation = generation;
        it->second.last_used = tick;
        it->second.bytes = bytes;
        it->second.entry = std::move(entry);
        // A caller that does not supply a dynamic-byte counter must not clear
        // the one a previous put installed: ReclaimCandidate would then charge
        // only the static slot bytes and undercount live columnar memory.
        if (dynamic_bytes != nullptr) {
          it->second.dynamic_bytes = std::move(dynamic_bytes);
        }
      }
    }
  }
  [[nodiscard]] std::shared_ptr<const void> GetCachedViewRaw(
      uint64_t generation, const std::vector<uint32_t>& projection) const {
    std::scoped_lock lock(columnar_cache_->mu);
    for (auto& view : columnar_cache_->views) {
      if (view.generation == generation && view.projection == projection &&
          view.store) {
        TouchColumnarCache();
        const uint64_t tick = columnar_cache_->touch_clock;
        view.last_used = tick;
        if (projection.empty()) {
          for (auto& [col, slot] : columnar_cache_->columns) {
            if (slot.generation == generation) {
              slot.last_used = tick;
            }
          }
        } else {
          for (uint32_t col : projection) {
            auto it = columnar_cache_->columns.find(col);
            if (it != columnar_cache_->columns.end() &&
                it->second.generation == generation) {
              it->second.last_used = tick;
            }
          }
        }
        return view.store;
      }
    }
    return nullptr;
  }
  void PutCachedViewRaw(uint64_t generation, std::vector<uint32_t> projection,
                        std::shared_ptr<const void> store) {
    static constexpr size_t kMaxCachedViews = 16;
    std::vector<std::shared_ptr<const void>> dropped;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      if (columnar_cache_->projected_lstable_store) {
        dropped.push_back(std::move(columnar_cache_->projected_lstable_store));
      }
      for (size_t i = 0; i < columnar_cache_->views.size();) {
        auto& existing = columnar_cache_->views[i];
        if ((existing.generation == generation &&
             existing.projection == projection) ||
            existing.generation != generation || !existing.store) {
          if (existing.store) {
            dropped.push_back(std::move(existing.store));
          }
          existing = std::move(columnar_cache_->views.back());
          columnar_cache_->views.pop_back();
        } else {
          ++i;
        }
      }
      if (!store) {
        return;
      }
      TouchColumnarCache();
      const uint64_t tick = columnar_cache_->touch_clock;
      if (projection.empty()) {
        for (auto& [col, slot] : columnar_cache_->columns) {
          if (slot.generation == generation) {
            slot.last_used = tick;
          }
        }
      } else {
        for (uint32_t col : projection) {
          auto it = columnar_cache_->columns.find(col);
          if (it != columnar_cache_->columns.end() &&
              it->second.generation == generation) {
            it->second.last_used = tick;
          }
        }
      }
      if (columnar_cache_->views.size() >= kMaxCachedViews) {
        size_t lru_idx = 0;
        for (size_t i = 1; i < columnar_cache_->views.size(); ++i) {
          if (columnar_cache_->views[i].last_used <
              columnar_cache_->views[lru_idx].last_used) {
            lru_idx = i;
          }
        }
        if (columnar_cache_->views[lru_idx].store) {
          dropped.push_back(std::move(columnar_cache_->views[lru_idx].store));
        }
        columnar_cache_->views[lru_idx] =
            std::move(columnar_cache_->views.back());
        columnar_cache_->views.pop_back();
      }
      columnar_cache_->views.push_back(ColumnarCacheState::CachedViewSlot{
          .generation=generation, .last_used=tick, .projection=std::move(projection), .store=std::move(store)});
    }
  }
  [[nodiscard]] std::shared_ptr<const void> GetBlockLayoutRaw(
      uint64_t generation) const {
    std::scoped_lock lock(columnar_cache_->mu);
    if (columnar_cache_->block_layout_generation != generation ||
        !columnar_cache_->block_layout) {
      return nullptr;
    }
    return columnar_cache_->block_layout;
  }
  void PutBlockLayoutRaw(uint64_t generation,
                         std::shared_ptr<const void> layout) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old = std::move(columnar_cache_->block_layout);
      columnar_cache_->block_layout_generation = generation;
      columnar_cache_->block_layout = std::move(layout);
    }
  }
  [[nodiscard]] bool ColumnRejected(uint32_t col, uint64_t generation) const {
    std::scoped_lock lock(columnar_cache_->mu);
    return columnar_cache_->rejected_columns_generation == generation &&
           columnar_cache_->rejected_columns.contains(col);
  }
  void RejectColumn(uint32_t col, uint64_t generation) {
    std::scoped_lock lock(columnar_cache_->mu);
    if (columnar_cache_->rejected_columns_generation != generation) {
      columnar_cache_->rejected_columns.clear();
      columnar_cache_->rejected_columns_generation = generation;
    }
    columnar_cache_->rejected_columns.insert(col);
  }
  size_t EvictIdleColumnsForHeadroom(size_t target_bytes) const {
    return ReclaimFromState(*columnar_cache_, target_bytes,
                            /*try_lock_only=*/false);
  }
  [[nodiscard]] size_t CachedColumnCountForTest() const {
    std::scoped_lock lock(columnar_cache_->mu);
    return columnar_cache_->columns.size();
  }
  bool ProjectionRejected(uint64_t generation, uint64_t limit,
                          const std::vector<uint8_t>& mask) const {
    std::scoped_lock lock(columnar_cache_->mu);
    return rejected_projection_generation_ == generation &&
           rejected_projection_limit_ == limit && rejected_projection_ == mask;
  }
  void RejectProjection(uint64_t generation, uint64_t limit,
                        const std::vector<uint8_t>& mask) {
    std::scoped_lock lock(columnar_cache_->mu);
    rejected_projection_generation_ = generation;
    rejected_projection_limit_ = limit;
    rejected_projection_ = mask;
  }
  [[nodiscard]] uint64_t LSTableCapacityRejectedGeneration() const {
    return lstable_capacity_rejected_generation_.load(
        std::memory_order_acquire);
  }
  void RejectLSTableCapacity(uint64_t generation) {
    lstable_capacity_rejected_generation_.store(generation,
                                                std::memory_order_release);
  }
  [[nodiscard]] std::shared_ptr<const void> GetBackingTlcsStoreRaw() const {
    std::scoped_lock lock(columnar_cache_->mu);
    return backing_tlcs_store_;
  }
  [[nodiscard]] std::shared_ptr<const void> GetCleanPageDirectoryRaw() const {
    std::scoped_lock lock(columnar_cache_->mu);
    return clean_page_directory_;
  }
  void SetCleanPageDirectoryRaw(std::shared_ptr<const void> dir) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old = std::move(clean_page_directory_);
      clean_page_directory_ = std::move(dir);
    }
  }
  void SetBackingTLCSStoreRaw(std::shared_ptr<const void> store) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old = std::move(backing_tlcs_store_);
      backing_tlcs_store_ = std::move(store);
      rejected_projection_generation_ = 0;
      columnar_cache_->rejected_columns.clear();
      columnar_cache_->rejected_columns_generation = 0;
    }
    // Destroy the old TlcsFile outside the mutex so its budget return and
    // mmap unmap do not block a concurrent reclaim hook.
  }

 private:
  static uint64_t NextGlobalColumnarTouch() noexcept {
    static std::atomic<uint64_t> global_clock{1};
    return global_clock.fetch_add(1, std::memory_order_relaxed);
  }
  void TouchColumnarCache() const noexcept {
    // Callers hold columnar_cache_->mu; advancing from a global clock
    // maintains strict LRU order both within one table and across tables in
    // ProcessMemoryBudget::InvokeReclaimHooks.
    const uint64_t tick = NextGlobalColumnarTouch();
    columnar_cache_->touch_clock = tick;
    columnar_cache_->recency->store(tick, std::memory_order_relaxed);
  }

  struct ColumnarCacheState {
    struct CachedColumnSlot {
      uint64_t generation{0};
      uint64_t last_used{0};
      size_t bytes{0};
      std::shared_ptr<const void> entry;
      std::shared_ptr<const std::atomic<size_t>> dynamic_bytes;
    };
    struct CachedViewSlot {
      uint64_t generation{0};
      uint64_t last_used{0};
      std::vector<uint32_t> projection;
      std::shared_ptr<const void> store;
    };

    ReclaimRecency recency{std::make_shared<std::atomic<uint64_t>>(0)};
    uint64_t touch_clock{1};  // guarded by mu
    mutable std::mutex mu;
    std::shared_ptr<const void> lstable_store;
    std::shared_ptr<const void> projected_lstable_store;
    uint64_t block_layout_generation{0};
    std::shared_ptr<const void> block_layout;
    std::unordered_map<uint32_t, CachedColumnSlot> columns;
    std::vector<CachedViewSlot> views;
    uint64_t rejected_columns_generation{0};
    std::unordered_set<uint32_t> rejected_columns;
  };

  static size_t ReclaimFromState(ColumnarCacheState& state, size_t target_bytes,
                                 bool try_lock_only) {
    if (target_bytes == 0) {
      return 0;
    }
    const size_t before = ProcessMemoryBudget::Global().ColumnarUsed();
    {
      // Phase 1: Drop idle legacy stores and assembled views outside lock so
      // cached column entries referenced only by views drop to use_count()
      // == 1.
      std::vector<std::shared_ptr<const void>> dropped_views;
      std::unique_lock<std::mutex> lk =
          try_lock_only
              ? std::unique_lock<std::mutex>(state.mu, std::try_to_lock)
              : std::unique_lock<std::mutex>(state.mu);
      if (!lk.owns_lock()) {
        return 0;
      }
      if (state.lstable_store && state.lstable_store.use_count() == 1) {
        dropped_views.push_back(std::move(state.lstable_store));
      }
      if (state.projected_lstable_store &&
          state.projected_lstable_store.use_count() == 1) {
        dropped_views.push_back(std::move(state.projected_lstable_store));
      }
      for (size_t i = 0; i < state.views.size();) {
        if (!state.views[i].store || state.views[i].store.use_count() == 1) {
          if (state.views[i].store) {
            dropped_views.push_back(std::move(state.views[i].store));
          }
          state.views[i] = std::move(state.views.back());
          state.views.pop_back();
        } else {
          ++i;
        }
      }
    }
    const size_t after_phase1 = ProcessMemoryBudget::Global().ColumnarUsed();
    const size_t freed_phase1 =
        before > after_phase1 ? before - after_phase1 : 0;
    if (freed_phase1 >= target_bytes) {
      return freed_phase1;
    }

    // Phase 2: Reclaim idle cached columns in LRU order until target_bytes.
    size_t planned_total = freed_phase1;
    while (true) {
      const size_t cur_used = ProcessMemoryBudget::Global().ColumnarUsed();
      const size_t actual_freed = before > cur_used ? before - cur_used : 0;
      if (actual_freed >= target_bytes ||
          (actual_freed == 0 && planned_total >= target_bytes)) {
        break;
      }
      const size_t need = actual_freed > 0 ? (target_bytes - actual_freed)
                                           : (target_bytes - planned_total);
      std::vector<std::shared_ptr<const void>> dropped_cols;
      {
        std::unique_lock<std::mutex> lk =
            try_lock_only
                ? std::unique_lock<std::mutex>(state.mu, std::try_to_lock)
                : std::unique_lock<std::mutex>(state.mu);
        if (!lk.owns_lock()) {
          break;
        }
        struct Candidate {
          uint32_t col;
          uint64_t last_used;
          size_t bytes;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(state.columns.size());
        for (const auto& [col, slot] : state.columns) {
          if (!slot.entry || slot.entry.use_count() == 1) {
            const size_t entry_bytes =
                slot.bytes +
                (slot.dynamic_bytes
                     ? slot.dynamic_bytes->load(std::memory_order_relaxed)
                     : 0);
            candidates.push_back(Candidate{.col=col, .last_used=slot.last_used, .bytes=entry_bytes});
          }
        }
        if (candidates.empty()) {
          if (state.columns.empty() && state.views.empty() &&
              !state.lstable_store && !state.projected_lstable_store &&
              state.block_layout && state.block_layout.use_count() == 1) {
            dropped_cols.push_back(std::move(state.block_layout));
            state.block_layout_generation = 0;
          }
          break;
        }
        std::sort(candidates.begin(), candidates.end(),
                  [](const Candidate& a, const Candidate& b) {
                    return a.last_used < b.last_used;
                  });
        size_t batch_planned = 0;
        for (const Candidate& cand : candidates) {
          if (batch_planned >= need) {
            break;
          }
          auto it = state.columns.find(cand.col);
          if (it == state.columns.end()) {
            continue;
          }
          if (it->second.entry && it->second.entry.use_count() == 1) {
            dropped_cols.push_back(std::move(it->second.entry));
            batch_planned += cand.bytes;
            state.columns.erase(it);
          } else if (!it->second.entry) {
            state.columns.erase(it);
          }
        }
        if (state.columns.empty() && state.views.empty() &&
            !state.lstable_store && !state.projected_lstable_store &&
            state.block_layout && state.block_layout.use_count() == 1) {
          dropped_cols.push_back(std::move(state.block_layout));
          state.block_layout_generation = 0;
        }
        if (dropped_cols.empty()) {
          break;
        }
        planned_total += batch_planned;
      }
    }
    const size_t after = ProcessMemoryBudget::Global().ColumnarUsed();
    return before > after ? before - after : 0;
  }

  static uint64_t NextLayoutGenerationSeed() noexcept {
    static std::atomic<uint64_t> seed{1};
    return seed.fetch_add(kLayoutGenerationStride, std::memory_order_acq_rel);
  }

  mutable std::mutex mutex_;
  std::condition_variable quiescent_;
  State state_{State::kLive};
  size_t active_leases_{0};
  std::unordered_map<txn_id_t, size_t> owner_leases_;
  std::atomic<uint64_t> layout_generation_{NextLayoutGenerationSeed()};
  std::atomic<bool> rows_rewritten_in_place_{false};
  std::shared_ptr<ColumnarCacheState> columnar_cache_{
      std::make_shared<ColumnarCacheState>()};
  ScopedReclaimHook reclaim_hook_;
  uint64_t rejected_projection_generation_{0};
  uint64_t rejected_projection_limit_{0};
  std::vector<uint8_t> rejected_projection_;
  std::atomic<uint64_t> lstable_capacity_rejected_generation_{0};
  std::shared_ptr<const void> backing_tlcs_store_;
  std::shared_ptr<const void> clean_page_directory_;
};

class TableLease {
 public:
  TableLease() = default;
  TableLease(const TableLease&) = delete;
  TableLease& operator=(const TableLease&) = delete;
  TableLease(TableLease&& other) noexcept
      : incarnation_(std::move(other.incarnation_)), owner_(other.owner_) {
    other.owner_ = static_cast<txn_id_t>(-1);
  }
  TableLease& operator=(TableLease&& other) noexcept {
    if (this != &other) {
      Release();
      incarnation_ = std::move(other.incarnation_);
      owner_ = other.owner_;
      other.owner_ = static_cast<txn_id_t>(-1);
    }
    return *this;
  }
  ~TableLease() { Release(); }

  void Release() {
    if (incarnation_ != nullptr) {
      incarnation_->Release(owner_);
      incarnation_.reset();
      owner_ = static_cast<txn_id_t>(-1);
    }
  }

  [[nodiscard]] bool Valid() const { return incarnation_ != nullptr; }

 private:
  friend class Table;
  TableLease(std::shared_ptr<TableIncarnation> incarnation, txn_id_t owner)
      : incarnation_(std::move(incarnation)), owner_(owner) {}

  std::shared_ptr<TableIncarnation> incarnation_;
  txn_id_t owner_{static_cast<txn_id_t>(-1)};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INCARNATION_HPP
