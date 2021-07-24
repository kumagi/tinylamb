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
class PageManager;
class Transaction;
class Recovery;

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, PageManager* pm, Logger* l)
      : lock_manager_(lm), page_manager_(pm), logger_(l) {}

  Transaction Begin();

  bool PreCommit(Transaction& txn);

  void Abort(Transaction& txn);

 private:
  friend class Recovery;
  std::atomic<uint64_t> next_txn_id_ = 1;
  LockManager* const lock_manager_;
  PageManager* const page_manager_;
  std::unordered_set<uint64_t> active_transactions_;
  Logger* const logger_;
  std::shared_mutex global_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
