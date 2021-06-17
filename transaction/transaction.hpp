//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_HPP
#define TINYLAMB_TRANSACTION_HPP

#include "transaction/lock_manager.hpp"
#include "type/row_position.hpp"

namespace tinylamb {

class TransactionManager;
class Logger;
class PageManager;

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted
};

class Transaction {
  friend class TransactionManager;
  Transaction(uint64_t txn_id, TransactionManager* tm,
              LockManager* lm, PageManager* pm)
      : txn_id_(txn_id), transaction_manager_(tm),
        lock_manager_(lm), page_manager_(pm) {}
 public:
  void SetStatus(TransactionStatus status) {
    status_ = status;
  }
 private:
  const uint64_t txn_id_;

  uint64_t prev_lsn_ = 0;
  std::unordered_set<RowPosition> read_set_ = {};
  std::unordered_set<RowPosition> write_set_ = {};
  TransactionStatus status_ = TransactionStatus::kUnknown;

  // Not owned members.
  TransactionManager* transaction_manager_;
  LockManager* lock_manager_;
  PageManager* page_manager_;
  Logger* logger_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
