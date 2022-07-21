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
  txn_id_t new_txn_id = next_txn_id_.fetch_add(1);
  Transaction new_txn(new_txn_id, this);
  new_txn.prev_lsn_ =
      logger_->AddLog(LogRecord(0, new_txn_id, LogType::kBegin).Serialize());
  {
    std::scoped_lock lk(transaction_table_lock);
    active_transactions_.emplace(new_txn_id, &new_txn);
  }
  assert(!new_txn.IsFinished());
  return new_txn;
}

Status TransactionManager::PreCommit(Transaction& txn) {
  assert(!txn.IsFinished());
  txn.SetStatus(TransactionStatus::kCommitted);
  LogRecord commit_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
  txn.prev_lsn_ = logger_->AddLog(commit_log.Serialize());
  for (const auto& row : txn.read_set_) {
    lock_manager_->ReleaseSharedLock(row);
  }
  for (const auto& row : txn.write_set_) {
    lock_manager_->ReleaseExclusiveLock(row);
  }
  {
    std::scoped_lock lk(transaction_table_lock);
    active_transactions_.erase(txn.txn_id_);
  }
  return Status::kSuccess;
}

void TransactionManager::Abort(Transaction& txn) {
  // Iterate prev_lsn to beginning of the transaction with undoing.
  {
    const uint64_t latest_log = txn.prev_lsn_;
    while (CommittedLSN() <= latest_log) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  lsn_t prev = txn.prev_lsn_;
  while (prev != 0) {
    LogRecord lr;
    recovery_->ReadLog(prev, &lr);
    recovery_->LogUndoWithPage(prev, lr, txn.transaction_manager_);
    prev = lr.prev_lsn;
  }
  PreCommit(txn);  // Writes an empty commit.
}

void TransactionManager::CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                                             slot_t slot) {
  logger_->AddLog(
      LogRecord::CompensatingInsertLogRecord(txn_id, pid, slot).Serialize());
}
void TransactionManager::CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                                             std::string_view key) {
  logger_->AddLog(
      LogRecord::CompensatingInsertLogRecord(txn_id, pid, key).Serialize());
}
void TransactionManager::CompensateInsertBranchLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   std::string_view key) {
  logger_->AddLog(LogRecord::CompensatingInsertBranchLogRecord(txn_id, pid, key)
                      .Serialize());
}

void TransactionManager::CompensateUpdateLog(txn_id_t txn_id, page_id_t pid,
                                             slot_t slot,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingUpdateLogRecord(txn_id, pid, slot, redo)
          .Serialize());
}
void TransactionManager::CompensateUpdateLog(txn_id_t txn_id, page_id_t pid,
                                             std::string_view key,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingUpdateLeafLogRecord(txn_id, pid, key, redo)
          .Serialize());
}
void TransactionManager::CompensateUpdateBranchLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   std::string_view key,
                                                   page_id_t redo) {
  logger_->AddLog(
      LogRecord::CompensatingUpdateBranchLogRecord(txn_id, pid, key, redo)
          .Serialize());
}

void TransactionManager::CompensateDeleteLog(txn_id_t txn_id, page_id_t pid,
                                             slot_t slot,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingDeleteLogRecord(txn_id, pid, slot, redo)
          .Serialize());
}

void TransactionManager::CompensateDeleteLog(txn_id_t txn_id, page_id_t pid,
                                             std::string_view key,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingDeleteLeafLogRecord(txn_id, pid, key, redo)
          .Serialize());
}

void TransactionManager::CompensateDeleteBranchLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   std::string_view key,
                                                   page_id_t redo) {
  logger_->AddLog(
      LogRecord::ComnensatingDeleteBranchLogRecord(txn_id, pid, key, redo)
          .Serialize());
}

void TransactionManager::CompensateSetLowestValueLog(txn_id_t txn_id,
                                                     page_id_t pid,
                                                     page_id_t redo) {
  logger_->AddLog(
      LogRecord::CompensateSetLowestValueLogRecord(txn_id, pid, redo)
          .Serialize());
}

bool TransactionManager::GetExclusiveLock(const RowPosition& rp) {
  return lock_manager_->GetExclusiveLock(rp);
}

bool TransactionManager::GetSharedLock(const RowPosition& rp) {
  return lock_manager_->GetSharedLock(rp);
}

bool TransactionManager::TryUpgradeLock(const RowPosition& rp) {
  return lock_manager_->TryUpgradeLock(rp);
}

uint64_t TransactionManager::AddLog(const LogRecord& lr) {
  return logger_->AddLog(lr.Serialize());
}

uint64_t TransactionManager::CommittedLSN() const {
  return logger_->CommittedLSN();
}

}  // namespace tinylamb