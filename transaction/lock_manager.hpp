/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_LOCK_MANAGER_HPP
#define TINYLAMB_LOCK_MANAGER_HPP

#include <condition_variable>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <unordered_set>

#include "page/row_position.hpp"

namespace tinylamb {

class LockManager {
 public:
  LockManager() = default;
  bool GetSharedLock(const RowPosition& row);
  bool ReleaseSharedLock(const RowPosition& row);
  bool GetExclusiveLock(const RowPosition& row, bool wait = true);
  bool ReleaseExclusiveLock(const RowPosition& row);
  bool TryUpgradeLock(const RowPosition& row);

  friend std::ostream& operator<<(std::ostream& o, const LockManager& lm) {
    std::scoped_lock lk(lm.latch_);
    o << "LockManager(shared=" << lm.shared_locks_.size()
      << ", exclusive=" << lm.exclusive_locks_.size() << ")";
    return o;
  }

 private:
  mutable std::mutex latch_;
  std::condition_variable available_;
  std::unordered_map<RowPosition, size_t> shared_locks_;
  std::unordered_set<RowPosition> exclusive_locks_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOCK_MANAGER_HPP
