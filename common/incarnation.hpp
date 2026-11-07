#ifndef TINYLAMB_INCARNATION_HPP
#define TINYLAMB_INCARNATION_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/constants.hpp"

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
    --active_leases_;
    auto found = owner_leases_.find(owner);
    if (found != owner_leases_.end()) {
      if (--found->second == 0) {
        owner_leases_.erase(found);
      }
    }
    if (active_leases_ == 0) {
      quiescent_.notify_all();
    }
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
    quiescent_.wait(lock, [&] { return active_leases_ == 0; });
    return true;
  }

  void CommitDrop() {
    std::scoped_lock lock(mutex_);
    if (state_ == State::kDropPending) {
      state_ = State::kRetired;
    }
    quiescent_.notify_all();
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
    std::scoped_lock lock(lstable_mutex_);
    return lstable_store_;
  }
  void SetLSTableStoreRaw(std::shared_ptr<const void> store) {
    std::scoped_lock lock(lstable_mutex_);
    lstable_store_ = std::move(store);
  }

 private:
  static uint64_t NextLayoutGenerationSeed() noexcept {
    static std::atomic<uint64_t> seed{1};
    return seed.fetch_add(kLayoutGenerationStride, std::memory_order_acq_rel);
  }

  mutable std::mutex mutex_;
  mutable std::mutex lstable_mutex_;
  std::condition_variable quiescent_;
  State state_{State::kLive};
  size_t active_leases_{0};
  std::unordered_map<txn_id_t, size_t> owner_leases_;
  std::atomic<uint64_t> layout_generation_{NextLayoutGenerationSeed()};
  std::atomic<bool> rows_rewritten_in_place_{false};
  std::shared_ptr<const void> lstable_store_;
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
