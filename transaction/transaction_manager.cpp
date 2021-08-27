#include "transaction/transaction_manager.hpp"

#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

Transaction TransactionManager::Begin() {
  global_lock_.lock_shared();
  uint64_t new_txn_id = next_txn_id_.fetch_add(1);

  Transaction new_txn(new_txn_id, this, lock_manager_, page_manager_, logger_);
  LogRecord begin_log(0, new_txn_id, LogType::kBegin);
  logger_->AddLog(begin_log);
  new_txn.prev_lsn_ = begin_log.lsn;
  active_transactions_.insert(new_txn_id);
  return new_txn;
}

bool TransactionManager::PreCommit(Transaction& txn) {
  assert(!txn.IsFinished());
  LogRecord commit_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
  txn.SetStatus(TransactionStatus::kCommitted);
  logger_->AddLog(commit_log);
  txn.prev_lsn_ = commit_log.lsn;
  for (auto& row : txn.read_set_) {
    lock_manager_->ReleaseSharedLock(row);
  }
  for (auto& row : txn.write_set_) {
    lock_manager_->ReleaseExclusiveLock(row);
  }
  active_transactions_.erase(txn.txn_id_);
  global_lock_.unlock_shared();
  return true;
}

void TransactionManager::Abort(Transaction& txn) {
  // Iterate prev_lsn to beginning of the transaction with undoing.
  for (const auto& pr : txn.prev_record_) {
    RowPosition pos = pr.first;
    Page* page = txn.page_manager_->GetPage(pos.page_id);
    auto* target = reinterpret_cast<RowPage*>(page);
    switch (pr.second.entry_type) {
      case Transaction::WriteType::kInsert:
        target->DeleteImpl(pos);
        break;
      case Transaction::WriteType::kUpdate:
        break;
      case Transaction::WriteType::kDelete:
        break;
    }
  }
}

}  // namespace tinylamb