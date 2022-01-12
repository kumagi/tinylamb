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
  uint64_t new_txn_id = next_txn_id_.fetch_add(1);

  Transaction new_txn(new_txn_id, this);
  LogRecord begin_log(0, new_txn_id, LogType::kBegin);
  new_txn.lsns_.push_back(logger_->AddLog(begin_log));
  {
    std::scoped_lock lk(transaction_table_lock);
    active_transactions_.emplace(new_txn_id, &new_txn);
  }
  assert(!new_txn.IsFinished());
  return new_txn;
}

Transaction TransactionManager::Begin(uint64_t txn_id,
                                      TransactionStatus txn_status,
                                      uint64_t last_lsn) {
  uint64_t new_txn_id = txn_id;
  Transaction new_txn(new_txn_id, this);
  new_txn.lsns_.push_back(last_lsn);
  new_txn.status_ = txn_status;
  {
    std::scoped_lock lk(transaction_table_lock);
    active_transactions_.emplace(txn_id, &new_txn);
  }
  assert(!new_txn.IsFinished());
  return new_txn;
}

bool TransactionManager::PreCommit(Transaction& txn) {
  assert(!txn.IsFinished());
  txn.SetStatus(TransactionStatus::kCommitted);
  // CommitLog(txn.txn_id_);
  LogRecord commit_log(txn.lsns_.back(), txn.txn_id_, LogType::kCommit);
  txn.lsns_.push_back(logger_->AddLog(commit_log));
  for (auto& row : txn.read_set_) {
    lock_manager_->ReleaseSharedLock(row);
  }
  for (auto& row : txn.write_set_) {
    lock_manager_->ReleaseExclusiveLock(row);
  }
  {
    std::scoped_lock lk(transaction_table_lock);
    active_transactions_.erase(txn.txn_id_);
  }
  return true;
}

void TransactionManager::CommitLog(uint64_t txn_id) {
  logger_->AddLog(LogRecord(0, txn_id, LogType::kCommit));
}

void TransactionManager::Abort(Transaction& txn) {
  // Iterate prev_lsn to beginning of the transaction with undoing.
  {
    const uint64_t latest_log = txn.lsns_.back();
    while (CommittedLSN() < latest_log) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  std::reverse(txn.lsns_.begin(), txn.lsns_.end());
  for (const auto& log_lsn : txn.lsns_) {
    LogRecord lr;
    recovery_->ReadLog(log_lsn, &lr);
    LOG(TRACE) << "txn undo: " << lr;
    recovery_->LogUndoWithPage(log_lsn, lr, txn.transaction_manager_);
  }
  PreCommit(txn);  // Writes an empty commit.
}

void TransactionManager::CompensateInsertLog(uint64_t txn_id,
                                             const RowPosition& pos) {
  LogRecord lr = LogRecord::CompensatingInsertLogRecord(txn_id, pos);
  logger_->AddLog(lr);
}

void TransactionManager::CompensateUpdateLog(uint64_t txn_id,
                                             const RowPosition& pos,
                                             std::string_view redo) {
  LogRecord lr = LogRecord::CompensatingUpdateLogRecord(txn_id, pos, redo);
  logger_->AddLog(lr);
}

void TransactionManager::CompensateDeleteLog(uint64_t txn_id,
                                             const RowPosition& pos,
                                             std::string_view redo) {
  LogRecord lr = LogRecord::CompensatingDeleteLogRecord(txn_id, pos, redo);
  logger_->AddLog(lr);
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