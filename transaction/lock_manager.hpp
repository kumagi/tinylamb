//
// Created by kumagi on 2021/05/29.
//

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
  std::unordered_map<RowPosition, txn_id_t> shared_locks_;
  std::unordered_set<RowPosition> exclusive_locks_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOCK_MANAGER_HPP
