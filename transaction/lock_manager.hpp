/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_LOCK_MANAGER_HPP
#define TINYLAMB_LOCK_MANAGER_HPP

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

class LockManager {
 public:
  static constexpr std::chrono::seconds kExclusiveWaitTimeout{5};
  // While any transaction waits for WAL durability (fsync) holding its write
  // locks, a stalled disk must not turn contenders' waits into spurious
  // timeouts; the effective exclusive-wait timeout becomes
  // max(kExclusiveWaitTimeout, kDurabilityWaitFloor) for that window.
  static constexpr std::chrono::seconds kDurabilityWaitFloor{60};

  LockManager() = default;
  // Every lock is owned by the txn_id that acquired it; release and upgrade
  // verify the caller's identity, so one transaction can never drop or
  // promote another's lock.  Asymmetric by design: GetSharedLock never waits
  // (callers retry on false), while GetExclusiveLock waits up to
  // kExclusiveWaitTimeout when wait=true.
  bool GetSharedLock(const RowPosition& row, txn_id_t owner);
  // Returns false when this owner does not hold a shared lock (double
  // release or foreign release).
  bool ReleaseSharedLock(const RowPosition& row, txn_id_t owner);
  bool GetExclusiveLock(const RowPosition& row, txn_id_t owner,
                        bool wait = true);
  bool GetExclusiveLock(const RowPosition& row, txn_id_t owner,
                        std::chrono::milliseconds timeout);
  // Effective timeout used by the wait=true overload:
  // max(kExclusiveWaitTimeout, kDurabilityWaitFloor) while at least one
  // durability wait is in flight.
  [[nodiscard]] std::chrono::milliseconds ExclusiveWaitTimeout() const {
    if (durability_waits_.load(std::memory_order_relaxed) > 0) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
          kDurabilityWaitFloor);
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        kExclusiveWaitTimeout);
  }
  // Marks the caller as waiting for log durability while holding row locks
  // (see ExclusiveWaitTimeout).  Prefer the RAII DurabilityWaitGuard.
  void BeginDurabilityWait() noexcept {
    durability_waits_.fetch_add(1, std::memory_order_relaxed);
  }
  void EndDurabilityWait() noexcept {
    durability_waits_.fetch_sub(1, std::memory_order_relaxed);
  }
  // Raises the exclusive-wait floor for its lifetime, even on exceptions.
  class [[nodiscard]] DurabilityWaitGuard {
   public:
    explicit DurabilityWaitGuard(LockManager& lm) : lm_(&lm) {
      lm_->BeginDurabilityWait();
    }
    ~DurabilityWaitGuard() { lm_->EndDurabilityWait(); }
    DurabilityWaitGuard(const DurabilityWaitGuard&) = delete;
    DurabilityWaitGuard& operator=(const DurabilityWaitGuard&) = delete;

   private:
    LockManager* lm_;
  };
  // Returns false when this owner does not hold the exclusive lock (double
  // release or foreign release).
  bool ReleaseExclusiveLock(const RowPosition& row, txn_id_t owner);
  // Promotes the caller's sole shared lock to exclusive.
  bool TryUpgradeLock(const RowPosition& row, txn_id_t owner);

  friend std::ostream& operator<<(std::ostream& o, const LockManager& lm) {
    std::scoped_lock lk(lm.latch_);
    o << "LockManager(shared=" << lm.shared_locks_.size()
      << ", exclusive=" << lm.exclusive_locks_.size() << ")";
    return o;
  }

 private:
  mutable std::mutex latch_;
  std::condition_variable available_;
  // Owners per shared-locked row (empty entries are removed immediately).
  std::unordered_map<RowPosition, std::unordered_set<txn_id_t>> shared_locks_;
  std::unordered_map<RowPosition, txn_id_t> exclusive_locks_;
  std::atomic<uint32_t> durability_waits_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOCK_MANAGER_HPP
