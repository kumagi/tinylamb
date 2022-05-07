//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_TRANSACTION_CONTEXT_HPP
#define TINYLAMB_TRANSACTION_CONTEXT_HPP

#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageManager;
class RelationStorage;

class TransactionContext {
 public:
  TransactionContext(Transaction&& txn, RelationStorage* catalog)
      : txn_(std::move(txn)), c_(catalog) {}
  TransactionContext(const TransactionContext&) = delete;
  TransactionContext& operator=(const TransactionContext&) = delete;
  TransactionContext(TransactionContext&&) = default;
  TransactionContext& operator=(TransactionContext&& o) noexcept {
    txn_ = std::move(o.txn_);
    c_ = o.c_;
    return *this;
  }
  Status PreCommit() { return txn_.PreCommit(); }
  void Abort() { txn_.Abort(); }

  Transaction txn_;
  RelationStorage* c_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_CONTEXT_HPP
