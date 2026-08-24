/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "transaction/lock_manager.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <mutex>
#include <ostream>

#include "common/constants.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

bool LockManager::GetSharedLock(const RowPosition& row, txn_id_t owner) {
  Shard& sh = shards_[ShardIndex(row)];
  std::unique_lock lk(sh.mu);
  // Any exclusive holder -- even the caller itself -- blocks shared
  // acquisition; an exclusive lock never coexists with shared entries.
  if (sh.exclusive.contains(row)) {
    return false;
  }
  sh.shared[row].insert(owner);
  return true;
}

bool LockManager::ReleaseSharedLock(const RowPosition& row, txn_id_t owner) {
  Shard& sh = shards_[ShardIndex(row)];
  std::unique_lock lk(sh.mu);
  auto it = sh.shared.find(row);
  if (it == sh.shared.end() || it->second.erase(owner) == 0) {
    // Double release, a never-acquired lock, or a foreign owner's release
    // must not decrement through the end iterator (undefined behavior in
    // NDEBUG builds).
    return false;
  }
  assert(!sh.exclusive.contains(row));
  if (it->second.empty()) {
    sh.shared.erase(it);
  }
  BumpReleaseEpoch();
  sh.cv.notify_all();
  return true;
}

bool LockManager::GetExclusiveLock(const RowPosition& row, txn_id_t owner,
                                   bool wait) {
  if (!wait) {
    return GetExclusiveLock(row, owner, std::chrono::milliseconds(0));
  }
  // Contended waits stretch with any in-flight durability stall so a slow
  // fsync under someone else's held write locks cannot fake a timeout here.
  return GetExclusiveLock(row, owner, ExclusiveWaitTimeout());
}

bool LockManager::GetExclusiveLock(const RowPosition& row, txn_id_t owner,
                                   std::chrono::milliseconds timeout) {
  Shard& sh = shards_[ShardIndex(row)];
  std::unique_lock lk(sh.mu);
  auto blocked = [&] {
    const auto shared = sh.shared.find(row);
    return (shared != sh.shared.end() && !shared->second.empty()) ||
           sh.exclusive.contains(row);
  };
  if (timeout.count() <= 0) {
    if (blocked()) {
      return false;
    }
  } else {
    // Wait up to |timeout| without any release progress; while other lock
    // activity keeps releasing rows the system is moving, so extend the
    // wait (bounded by kDurabilityWaitFloor) instead of reporting a
    // spurious conflict that executors would treat as a lost update.
    const auto total_cap = std::chrono::duration_cast<std::chrono::milliseconds>(
        kDurabilityWaitFloor);
    const auto start = std::chrono::steady_clock::now();
    auto patience = timeout;
    for (;;) {
      const uint64_t quiet_epoch =
          release_epoch_.load(std::memory_order_relaxed);
      if (sh.cv.wait_for(lk, patience, [&] { return !blocked(); })) {
        break;
      }
      if (!blocked()) {
        break;
      }
      const bool progressed =
          release_epoch_.load(std::memory_order_relaxed) != quiet_epoch;
      if (!progressed) {
        wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
      const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start);
      if (elapsed >= total_cap) {
        wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
      patience = std::min(total_cap - elapsed, timeout);
    }
  }
  sh.exclusive[row] = owner;
  return true;
}

bool LockManager::ReleaseExclusiveLock(const RowPosition& row,
                                       txn_id_t owner) {
  Shard& sh = shards_[ShardIndex(row)];
  std::unique_lock lk(sh.mu);
  const auto it = sh.exclusive.find(row);
  const bool released = it != sh.exclusive.end() && it->second == owner;
  // A foreign or double release is a documented false return, not a bug:
  // callers probe ownership through this path (see lock_manager_test).
  assert(!sh.shared.contains(row));
  if (!released) {
    return false;
  }
  sh.exclusive.erase(it);
  BumpReleaseEpoch();
  sh.cv.notify_all();
  return true;
}

bool LockManager::TryUpgradeLock(const RowPosition& row, txn_id_t owner) {
  Shard& sh = shards_[ShardIndex(row)];
  std::unique_lock lk(sh.mu);
  const auto exclusive = sh.exclusive.find(row);
  if (exclusive != sh.exclusive.end()) {
    // Already upgraded by this owner: idempotent success.
    return exclusive->second == owner;
  }
  const auto shared = sh.shared.find(row);
  if (shared == sh.shared.end() || shared->second.size() != 1 ||
      !shared->second.contains(owner)) {
    return false;
  }
  sh.shared.erase(shared);
  sh.exclusive[row] = owner;
  BumpReleaseEpoch();
  sh.cv.notify_all();
  return true;
}

std::ostream& operator<<(std::ostream& o, const LockManager& lm) {
  size_t shared = 0;
  size_t exclusive = 0;
  // Debug dump: take shard mutexes one at a time; counts may be momentarily
  // inconsistent across shards, which is fine for diagnostics.
  for (const auto& sh : lm.shards_) {
    std::scoped_lock lk(sh.mu);
    shared += sh.shared.size();
    exclusive += sh.exclusive.size();
  }
  o << "LockManager(shared=" << shared << ", exclusive=" << exclusive << ")";
  return o;
}

}  // namespace tinylamb
