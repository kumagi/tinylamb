//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include "transaction.hpp"

namespace tinylamb {

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, PageManager* pm, Logger* l)
      : lock_manager_(lm), page_manager_(pm), logger_(l) {}
  static Transaction BeginTransaction() {
    return Transaction(next_txn_id_++, loc);
  }

 private:
  std::atomic<uint64_t> next_txn_id_ = 0;
  LockManager* lock_manager_;
  PageManager* page_manager_;
  Logger* logger_;
};


}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
