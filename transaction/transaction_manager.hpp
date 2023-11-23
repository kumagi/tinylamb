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

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <shared_mutex>
#include <unordered_map>

#include "common/constants.hpp"

namespace tinylamb {

class IndexKey;
class LockManager;
class Logger;
class PageManager;
class Transaction;
class RecoveryManager;
enum class TransactionStatus : uint_fast8_t;
struct FosterPair;
struct LogRecord;
struct RowPosition;

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, PageManager* pm, Logger* l,
                     RecoveryManager* r)
      : lock_manager_(lm), page_manager_(pm), logger_(l), recovery_(r) {}

  Transaction Begin();

  Status PreCommit(Transaction& txn);

  void Abort(Transaction& txn);

  void CompensateInsertLog(txn_id_t txn_id, page_id_t pid, slot_t slot);
  void CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                           std::string_view key);
  void CompensateInsertBranchLog(txn_id_t txn_id, page_id_t pid,
                                 std::string_view key);
  void CompensateUpdateLog(txn_id_t txn_id, page_id_t pid, slot_t slot,
                           std::string_view redo);
  void CompensateUpdateLog(txn_id_t txn_id, page_id_t pid, std::string_view key,
                           std::string_view redo);
  void CompensateUpdateBranchLog(txn_id_t txn_id, page_id_t pid,
                                 std::string_view key, page_id_t redo);
  void CompensateDeleteLog(txn_id_t txn_id, page_id_t pid, slot_t slot,
                           std::string_view redo);
  void CompensateDeleteLog(txn_id_t txn_id, page_id_t pid, std::string_view key,
                           std::string_view redo);
  void CompensateDeleteBranchLog(txn_id_t txn_id, page_id_t pid,
                                 std::string_view key, page_id_t redo);
  void CompensateSetLowestValueLog(txn_id_t txn_id, page_id_t pid,
                                   page_id_t redo);
  void CompensateSetLowFenceLog(txn_id_t txn_id, page_id_t pid,
                                const IndexKey& redo);
  void CompensateSetHighFenceLog(txn_id_t txn_id, page_id_t pid,
                                 const IndexKey& redo);
  void CompensateSetFosterLog(txn_id_t txn_id, page_id_t pid,
                              const FosterPair& foster);
  bool GetExclusiveLock(const RowPosition& rp);
  bool GetSharedLock(const RowPosition& rp);

  bool TryUpgradeLock(const RowPosition& rp);

  lsn_t AddLog(const LogRecord& lr);
  lsn_t CommittedLSN() const;

  PageManager* GetPageManager() { return page_manager_; }

 private:
  friend class RecoveryManager;
  friend class CheckpointManager;

  std::unordered_map<txn_id_t, Transaction*> active_transactions_;
  std::atomic<txn_id_t> next_txn_id_ = 1;
  LockManager* const lock_manager_;
  PageManager* const page_manager_;
  Logger* const logger_;
  RecoveryManager* const recovery_;
  std::mutex transaction_table_lock;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
