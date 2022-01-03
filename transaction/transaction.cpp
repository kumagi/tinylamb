#include "transaction/transaction.hpp"

#include <iostream>

#include "page/row_position.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

Transaction::Transaction(uint64_t txn_id, TransactionManager* tm,
                         LockManager* lm, PageManager* pm, Logger* l)
    : txn_id_(txn_id),
      status_(TransactionStatus::kRunning),
      transaction_manager_(tm) {}

Transaction Transaction::SpawnSystemTransaction() {
  return transaction_manager_->Begin();
}

bool Transaction::PreCommit() { return transaction_manager_->PreCommit(*this); }

void Transaction::Abort() { transaction_manager_->Abort(*this); }

void Transaction::SetStatus(TransactionStatus status) { status_ = status; }

bool Transaction::AddReadSet(const RowPosition& rp) {
  assert(!IsFinished());
  if (write_set_.find(rp) != write_set_.end() ||
      read_set_.find(rp) != read_set_.end()) {
    // Already having the lock.
    return true;
  }
  if (!transaction_manager_->GetSharedLock(rp)) {
    return false;
  }
  read_set_.insert(rp);
  return true;
}

bool Transaction::AddWriteSet(const RowPosition& rp) {
  assert(!IsFinished());
  if (write_set_.find(rp) != write_set_.end()) {
    // Already having the lock.
    return true;
  }
  if (!transaction_manager_->GetExclusiveLock(rp)) {
    return false;
  }
  write_set_.insert(rp);
  read_set_.erase(rp);
  return true;
}

uint64_t Transaction::InsertLog(const RowPosition& pos, std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::InsertingLogRecord(prev_lsn_, txn_id_, pos, redo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  wrote_logs_.push_back(prev_lsn_);
  return prev_lsn_;
}

uint64_t Transaction::UpdateLog(const RowPosition& pos, std::string_view undo,
                                std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::UpdatingLogRecord(prev_lsn_, txn_id_, pos, redo, undo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  wrote_logs_.push_back(prev_lsn_);
  return prev_lsn_;
}

uint64_t Transaction::DeleteLog(const RowPosition& pos, std::string_view undo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::DeletingLogRecord(prev_lsn_, txn_id_, pos, undo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  wrote_logs_.push_back(prev_lsn_);
  return prev_lsn_;
}

uint64_t Transaction::CompensateInsertLog(const RowPosition& pos,
                                          uint64_t undo_next_lsn) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::CompensatingInsertLogRecord(
      prev_lsn_, undo_next_lsn, txn_id_, pos);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

uint64_t Transaction::CompensateUpdateLog(const RowPosition& pos,
                                          uint64_t undo_next_lsn,
                                          std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::CompensatingUpdateLogRecord(
      prev_lsn_, undo_next_lsn, txn_id_, pos, redo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

uint64_t Transaction::CompensateDeleteLog(const RowPosition& pos,
                                          uint64_t undo_next_lsn,
                                          std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::CompensatingDeleteLogRecord(
      prev_lsn_, undo_next_lsn, txn_id_, pos, redo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

uint64_t Transaction::AllocatePageLog(uint64_t allocated_page_id,
                                      PageType new_page_type) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::AllocatePageLogRecord(
      prev_lsn_, txn_id_, allocated_page_id, new_page_type);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

uint64_t Transaction::DestroyPageLog(uint64_t destroyed_page_id) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::DestroyPageLogRecord(prev_lsn_, txn_id_, destroyed_page_id);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

// Using this function is discouraged to get performance of flush pipelining.
void Transaction::CommitWait() const {
  while (transaction_manager_->CommittedLSN() < prev_lsn_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

}  // namespace tinylamb
