//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include "recovery/logger.hpp"
#include "transaction.hpp"

namespace tinylamb {

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, PageManager* pm, Logger* l)
      : lock_manager_(lm), page_manager_(pm), logger_(l) {}

  // Thread safe.
  Transaction BeginTransaction() {
    return Transaction(next_txn_id_.fetch_add(1),
                       this, lock_manager_, page_manager_, logger_);
  }

 private:
  std::atomic<uint64_t> next_txn_id_ = 0;
  LockManager* const lock_manager_;
  PageManager* const page_manager_;
  Logger* const logger_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
