#ifndef TINYLAMB_INCARNATION_HPP
#define TINYLAMB_INCARNATION_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
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
        [weak = std::move(weak)](size_t /*target*/) {
          auto state = weak.lock();
          if (!state) {
            return size_t{0};
          }
          std::shared_ptr<const void> old_full;
          std::shared_ptr<const void> old_proj;
          {
            std::unique_lock<std::mutex> lk(state->mu, std::try_to_lock);
            if (!lk.owns_lock()) {
              return size_t{0};
            }
            // A pinned store cannot release bytes. Keep its cache entry so
            // later readers do not rebuild an identical live representation.
            if (state->lstable_store.use_count() == 1) {
              old_full = std::move(state->lstable_store);
            }
            if (state->projected_lstable_store.use_count() == 1) {
              old_proj = std::move(state->projected_lstable_store);
            }
          }
          const size_t before = ProcessMemoryBudget::Global().ColumnarUsed();
          old_full.reset();
          old_proj.reset();
          const size_t after = ProcessMemoryBudget::Global().ColumnarUsed();
          return before > after ? before - after : size_t{0};
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
    ++active_leases_;
    ++owner_leases_[owner];
    return true;
  }

  void Release(txn_id_t owner) {
    std::scoped_lock lock(mutex_);
    if (active_leases_ == 0) {
      return;
    }
    if (active_leases_ == 1) {
      active_leases_ = 0;
      owner_leases_.clear();
      quiescent_.notify_all();
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

  bool WaitForQuiescence(txn_id_t owner) {
    std::unique_lock lock(mutex_);
    if (owner_leases_.contains(owner)) {
      return false;
    }
    // Do not block a DDL worker forever on a leaked/long-lived lease: the
    // callers already treat false as a retryable conflict.
    if (!quiescent_.wait_for(lock, std::chrono::seconds(60),
                             [&] { return active_leases_ == 0; })) {
      return false;
    }
    return true;
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
    std::shared_ptr<const void> old_full;
    std::shared_ptr<const void> old_proj;
    std::shared_ptr<const void> old_tlcs;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old_full = std::move(columnar_cache_->lstable_store);
      old_proj = std::move(columnar_cache_->projected_lstable_store);
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
    }
    return columnar_cache_->projected_lstable_store;
  }
  void SetProjectedLSTableStoreRaw(std::shared_ptr<const void> store) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old = std::move(columnar_cache_->projected_lstable_store);
      columnar_cache_->projected_lstable_store = std::move(store);
      TouchColumnarCache();
    }
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
  void SetBackingTLCSStoreRaw(std::shared_ptr<const void> store) {
    std::shared_ptr<const void> old;
    {
      std::scoped_lock lock(columnar_cache_->mu);
      old = std::move(backing_tlcs_store_);
      backing_tlcs_store_ = std::move(store);
      rejected_projection_generation_ = 0;
    }
    // Destroy the old TlcsFile outside the mutex so its budget return and
    // mmap unmap do not block a concurrent reclaim hook.
  }

 private:
  void TouchColumnarCache() const noexcept {
    // Callers hold columnar_cache_->mu, so a per-cache plain counter suffices;
    // a process-global atomic would serialise unrelated tables on one line.
    columnar_cache_->recency->store(++columnar_cache_->touch_clock,
                                    std::memory_order_relaxed);
  }

  struct ColumnarCacheState {
    ReclaimRecency recency{std::make_shared<std::atomic<uint64_t>>(0)};
    uint64_t touch_clock{1};  // guarded by mu
    mutable std::mutex mu;
    std::shared_ptr<const void> lstable_store;
    std::shared_ptr<const void> projected_lstable_store;
  };

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
