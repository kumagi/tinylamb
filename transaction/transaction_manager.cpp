#include "transaction/transaction_manager.hpp"

#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
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
  new_txn.prev_lsn_ = logger_->AddLog(begin_log);
  active_transactions_.insert(new_txn_id);
  assert(!new_txn.IsFinished());
  return new_txn;
}

bool TransactionManager::PreCommit(Transaction& txn) {
  assert(!txn.IsFinished());
  LogRecord commit_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
  txn.SetStatus(TransactionStatus::kCommitted);
  txn.prev_lsn_ = logger_->AddLog(commit_log);
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
  {
    const uint64_t latest_log = txn.wrote_logs_.back();
    while (CommittedLSN() < latest_log) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  for (const auto& log_lsn : txn.wrote_logs_) {
    LogRecord lr;
    recovery_->ReadLog(log_lsn, &lr);
    recovery_->LogUndo(log_lsn, lr, this);
  }
  PreCommit(txn);  // Writes an empty commit.
}

bool TransactionManager::GetExclusiveLock(const RowPosition& rp) {
  return lock_manager_->GetExclusiveLock(rp);
}
bool TransactionManager::GetSharedLock(const RowPosition& rp) {
  return lock_manager_->GetSharedLock(rp);
}

uint64_t TransactionManager::AddLog(const LogRecord& lr) {
  return logger_->AddLog(lr);
}

uint64_t TransactionManager::CommittedLSN() const {
  return logger_->CommittedLSN();
}

}  // namespace tinylamb