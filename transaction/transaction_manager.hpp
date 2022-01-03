//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <shared_mutex>
#include <unordered_set>

namespace tinylamb {

class LockManager;
class Logger;
class LogRecord;
class PageManager;
class Transaction;
class Recovery;
class RowPosition;

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, PageManager* pm, Logger* l, Recovery* r)
      : lock_manager_(lm), page_manager_(pm), logger_(l), recovery_(r) {}

  Transaction Begin();

  bool PreCommit(Transaction& txn);

  void Abort(Transaction& txn);

  bool GetExclusiveLock(const RowPosition& rp);
  bool GetSharedLock(const RowPosition& rp);
  uint64_t AddLog(const LogRecord& lr);
  uint64_t CommittedLSN() const;

 private:
  friend class Recovery;

  std::atomic<uint64_t> next_txn_id_ = 1;
  LockManager* const lock_manager_;
  PageManager* const page_manager_;
  std::unordered_set<uint64_t> active_transactions_;
  Logger* const logger_;
  Recovery* const recovery_;
  std::shared_mutex global_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
