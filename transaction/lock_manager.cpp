/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "transaction/lock_manager.hpp"

#include <cassert>
#include <chrono>
#include <mutex>

#include "page/row_position.hpp"

namespace tinylamb {

bool LockManager::GetSharedLock(const RowPosition& row) {
  std::unique_lock lk(latch_);
  if (exclusive_locks_.find(row) != exclusive_locks_.end()) {
    return false;
  }
  auto it = shared_locks_.find(row);
  if (it == shared_locks_.end()) {
    shared_locks_[row] = 1;
  } else {
    it->second++;
  }
  return true;
}

bool LockManager::ReleaseSharedLock(const RowPosition& row) {
  std::unique_lock lk(latch_);
  assert(exclusive_locks_.find(row) == exclusive_locks_.end());
  assert(shared_locks_.find(row) != shared_locks_.end());
  auto it = shared_locks_.find(row);
  if (--it->second == 0) {
    shared_locks_.erase(it);
  }
  available_.notify_all();
  return true;
}

bool LockManager::GetExclusiveLock(const RowPosition& row, bool wait) {
  std::unique_lock lk(latch_);
  auto blocked = [&] {
    return shared_locks_.find(row) != shared_locks_.end() ||
           exclusive_locks_.find(row) != exclusive_locks_.end();
  };
  if (wait) {
    const bool granted = available_.wait_for(
        lk, std::chrono::seconds(5), [&] { return !blocked(); });
    if (!granted) return false;
  } else if (blocked()) {
    return false;
  }
  exclusive_locks_.emplace(row);
  return true;
}

bool LockManager::ReleaseExclusiveLock(const RowPosition& row) {
  std::unique_lock lk(latch_);
  assert(exclusive_locks_.find(row) != exclusive_locks_.end());
  assert(shared_locks_.find(row) == shared_locks_.end());
  exclusive_locks_.erase(row);
  available_.notify_all();
  return true;
}

bool LockManager::TryUpgradeLock(const RowPosition& row) {
  std::unique_lock lk(latch_);
  if (exclusive_locks_.find(row) != exclusive_locks_.end()) return false;
  const auto shared = shared_locks_.find(row);
  if (shared == shared_locks_.end() || shared->second != 1) return false;
  shared_locks_.erase(shared);
  exclusive_locks_.emplace(row);
  return true;
}

}  // namespace tinylamb
