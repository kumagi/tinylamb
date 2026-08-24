/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_LOCK_MANAGER_HPP
#define TINYLAMB_LOCK_MANAGER_HPP

#include <array>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <mutex>
#include <functional>
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

  // Number of exclusive waits that gave up with no observable release
  // progress anywhere in the lock table: the only situation where a timeout
  // means "genuinely stuck" rather than "holder is merely slow".  Callers
  // (executors) can surface this as a retryable conflict instead of a lost
  // update.
  [[nodiscard]] uint64_t WaitTimeouts() const {
    return wait_timeouts_.load(std::memory_order_relaxed);
  }

  friend std::ostream& operator<<(std::ostream& o, const LockManager& lm);

 private:
  // Row locks are striped across independent shards so TPC-C style write
  // bursts stop serializing on one global mutex.  A lock operation takes
  // exactly one shard mutex (never nested), so shard ordering cannot
  // deadlock; release_epoch_ stays global because a waiter only needs to
  // know whether *the system* made progress while it waited, not which
  // shard released.
  static constexpr size_t kShards = 64;
  struct Shard {
    mutable std::mutex mu;
    std::condition_variable cv;
    // Owners per shared-locked row (empty entries are removed immediately).
    std::unordered_map<RowPosition, std::unordered_set<txn_id_t>> shared;
    std::unordered_map<RowPosition, txn_id_t> exclusive;
  };
  [[nodiscard]] static size_t ShardIndex(const RowPosition& row) {
    return std::hash<RowPosition>{}(row) % kShards;
  }
  std::array<Shard, kShards> shards_;
  std::atomic<uint32_t> durability_waits_{0};
  // Bumped on every release/upgrade anywhere in the table.  A waiter that
  // observed no bump across a whole timeout window is provably not waiting
  // on progress; one that did see releases is extended instead of failing
  // (bounded by kDurabilityWaitFloor) so a slow lock holder under active
  // system load cannot fake an abort.
  std::atomic<uint64_t> release_epoch_{0};
  std::atomic<uint64_t> wait_timeouts_{0};

  void BumpReleaseEpoch() noexcept {
    release_epoch_.fetch_add(1, std::memory_order_relaxed);
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOCK_MANAGER_HPP
