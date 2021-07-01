#include "transaction/lock_manager.hpp"

#include <cassert>
#include <mutex>

#include "page/row_position.hpp"

namespace tinylamb {

bool LockManager::GetSharedLock(const RowPosition& row) {
  std::scoped_lock lk(latch_);
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
  std::scoped_lock lk(latch_);
  assert(exclusive_locks_.find(row) == exclusive_locks_.end());
  assert(shared_locks_.find(row) != shared_locks_.end());
  auto it = shared_locks_.find(row);
  if (--it->second == 0) {
    shared_locks_.erase(it);
  }
  return true;
}
bool LockManager::GetExclusiveLock(const RowPosition& row) {
  std::scoped_lock lk(latch_);
  if (shared_locks_.find(row) != shared_locks_.end() ||
      exclusive_locks_.find(row) != exclusive_locks_.end()) {
    return false;
  }
  exclusive_locks_.emplace(row);
  return true;
}
bool LockManager::ReleaseExclusiveLock(const RowPosition& row) {
  std::scoped_lock lk(latch_);
  assert(exclusive_locks_.find(row) != exclusive_locks_.end());
  assert(shared_locks_.find(row) == shared_locks_.end());
  exclusive_locks_.erase(row);
  return true;
}

}  // namespace tinylamb
