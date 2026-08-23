/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "transaction/lock_manager.hpp"

#include <cassert>
#include <chrono>
#include <mutex>

#include "common/constants.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

bool LockManager::GetSharedLock(const RowPosition& row, txn_id_t owner) {
  std::unique_lock lk(latch_);
  // Any exclusive holder -- even the caller itself -- blocks shared
  // acquisition; an exclusive lock never coexists with shared entries.
  if (exclusive_locks_.contains(row)) {
    return false;
  }
  shared_locks_[row].insert(owner);
  return true;
}

bool LockManager::ReleaseSharedLock(const RowPosition& row, txn_id_t owner) {
  std::unique_lock lk(latch_);
  auto it = shared_locks_.find(row);
  if (it == shared_locks_.end() || it->second.erase(owner) == 0) {
    // Double release, a never-acquired lock, or a foreign owner's release
    // must not decrement through the end iterator (undefined behavior in
    // NDEBUG builds).
    return false;
  }
  assert(!exclusive_locks_.contains(row));
  if (it->second.empty()) {
    shared_locks_.erase(it);
  }
  available_.notify_all();
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
  std::unique_lock lk(latch_);
  auto blocked = [&] {
    const auto shared = shared_locks_.find(row);
    return (shared != shared_locks_.end() && !shared->second.empty()) ||
           exclusive_locks_.contains(row);
  };
  if (timeout.count() <= 0) {
    if (blocked()) { return false;
}
  } else {
    const bool granted =
        available_.wait_for(lk, timeout, [&] { return !blocked(); });
    if (!granted) { return false;
}
  }
  exclusive_locks_[row] = owner;
  return true;
}

bool LockManager::ReleaseExclusiveLock(const RowPosition& row,
                                       txn_id_t owner) {
  std::unique_lock lk(latch_);
  const auto it = exclusive_locks_.find(row);
  const bool released = it != exclusive_locks_.end() && it->second == owner;
  // A foreign or double release is a documented false return, not a bug:
  // callers probe ownership through this path (see lock_manager_test).
  assert(!shared_locks_.contains(row));
  if (!released) {
    return false;
  }
  exclusive_locks_.erase(it);
  available_.notify_all();
  return true;
}

bool LockManager::TryUpgradeLock(const RowPosition& row, txn_id_t owner) {
  std::unique_lock lk(latch_);
  const auto exclusive = exclusive_locks_.find(row);
  if (exclusive != exclusive_locks_.end()) {
    // Already upgraded by this owner: idempotent success.
    return exclusive->second == owner;
  }
  const auto shared = shared_locks_.find(row);
  if (shared == shared_locks_.end() || shared->second.size() != 1 ||
      !shared->second.contains(owner)) {
    return false;
  }
  shared_locks_.erase(shared);
  exclusive_locks_[row] = owner;
  available_.notify_all();
  return true;
}

}  // namespace tinylamb
