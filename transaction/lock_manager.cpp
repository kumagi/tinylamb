/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

bool LockManager::TryUpgradeLock(const RowPosition& row) {
  std::scoped_lock lk(latch_);
  if (exclusive_locks_.find(row) == exclusive_locks_.end() &&
      (shared_locks_.find(row) == shared_locks_.end() ||
       shared_locks_[row] <= 1)) {
    shared_locks_.clear();
    exclusive_locks_.emplace(row);
    return true;
  }
  return false;
}

}  // namespace tinylamb
