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

#ifndef TINYLAMB_LOCK_MANAGER_HPP
#define TINYLAMB_LOCK_MANAGER_HPP

#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "page/row_position.hpp"

namespace tinylamb {

class LockManager {
 public:
  LockManager() = default;
  bool GetSharedLock(const RowPosition& row);
  bool ReleaseSharedLock(const RowPosition& row);
  bool GetExclusiveLock(const RowPosition& row);
  bool ReleaseExclusiveLock(const RowPosition& row);
  bool TryUpgradeLock(const RowPosition& row);

 private:
  std::mutex latch_;
  std::unordered_map<RowPosition, size_t> shared_locks_;
  std::unordered_set<RowPosition> exclusive_locks_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOCK_MANAGER_HPP
