//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_TRANSACTION_HPP
#define TINYLAMB_TRANSACTION_HPP

#include "row_position.hpp"
#include "lock_manager.hpp"
#include "transaction_manager.hpp"

namespace tinylamb {

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted
};

class Transaction {
  friend class TransactionManager;
  Transaction(uint64_t txn_id, LockManager* lm) : txn_id_(txn_id), lock_manager_(lm) {}
 public:


 private:
  const uint64_t txn_id_;

  uint64_t prev_lsn_ = 0;
  std::unordered_set<RowPosition> read_set_;
  std::unordered_set<RowPosition> write_set_;
  TransactionStatus status_ = TransactionStatus::kUnknown;

  // not owned member
  LockManager* lock_manager_;
  PageManager* page_manager_;
  Logger* logger_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
