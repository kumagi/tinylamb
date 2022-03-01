//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_TRANSACTION_CONTEXT_HPP
#define TINYLAMB_TRANSACTION_CONTEXT_HPP

#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageManager;
class Catalog;

class TransactionContext {
 public:
  TransactionContext(TransactionManager* tm, PageManager* page_manager,
                     Catalog* catalog)
      : txn_(tm->Begin()), pm_(page_manager), c_(catalog) {}
  TransactionContext(const TransactionContext&) = delete;
  TransactionContext& operator=(const TransactionContext&) = delete;
  TransactionContext(TransactionContext&&) = default;
  TransactionContext& operator=(TransactionContext&& o) noexcept {
    txn_ = std::move(o.txn_);
    pm_ = o.pm_;
    c_ = o.c_;
    return *this;
  }
  Status PreCommit() { return txn_.PreCommit(); }
  void Abort() { txn_.Abort(); }

  Transaction txn_;
  PageManager* pm_;
  Catalog* c_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_CONTEXT_HPP
