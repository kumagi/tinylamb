//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_LOCK_MANAGER_HPP
#define TINYLAMB_LOCK_MANAGER_HPP

#include <cassert>
#include <mutex>
#include <unordered_set>

#include "type/row_position.hpp"

namespace tinylamb {

class LockManager {
  bool GetSharedLock(const RowPosition& row) {
    std::scoped_lock lk(latch_);
    if (exclusive_locks_.find(row) != exclusive_locks_.end()) {
      return false;
    }
    auto it = shared_locks_.find(row);
    it->second++;
    return true;
  }
  bool ReleaseSharedLock(const RowPosition& row) {
    std::scoped_lock lk(latch_);
    assert(exclusive_locks_.find(row) == exclusive_locks_.end());
    assert(shared_locks_.find(row) != shared_locks_.end());
    auto it = shared_locks_.find(row);
    if (--it->second == 0) {
      shared_locks_.erase(it);
    }
  }
  bool GetExclusiveLock(const RowPosition& row) {
    std::scoped_lock lk(latch_);
    if (shared_locks_.find(row) != shared_locks_.end() ||
        exclusive_locks_.find(row) != exclusive_locks_.end()) {
      return false;
    }
    exclusive_locks_.emplace(row);
    return true;
  }
  bool ReleaseExclusiveLock(const RowPosition& row) {
    std::scoped_lock lk(latch_);
    assert(exclusive_locks_.find(row) != exclusive_locks_.end());
    assert(shared_locks_.find(row) == shared_locks_.end());
    exclusive_locks_.erase(row);
    return true;
  }

  std::mutex latch_;
  std::unordered_map<RowPosition, int> shared_locks_;
  std::unordered_set<RowPosition> exclusive_locks_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOCK_MANAGER_HPP
