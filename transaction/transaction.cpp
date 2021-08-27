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
      transaction_manager_(tm),
      lock_manager_(lm),
      page_manager_(pm),
      logger_(l) {}

Transaction Transaction::SpawnSystemTransaction() {
  return transaction_manager_->Begin();
}

bool Transaction::PreCommit() { return transaction_manager_->PreCommit(*this); }

void Transaction::Abort() {
  transaction_manager_->Abort(*this);
  status_ = TransactionStatus::kAborted;
}

void Transaction::SetStatus(TransactionStatus status) { status_ = status; }

bool Transaction::AddReadSet(const RowPosition& rs) {
  assert(!IsFinished());
  if (write_set_.find(rs) != write_set_.end()) {
    return false;
  }
  lock_manager_->GetSharedLock(rs);
  read_set_.insert(rs);
  return true;
}

bool Transaction::AddWriteSet(const RowPosition& rs) {
  assert(!IsFinished());
  lock_manager_->GetExclusiveLock(rs);
  write_set_.insert(rs);
  if (read_set_.find(rs) != read_set_.end()) {
    read_set_.erase(rs);
    lock_manager_->ReleaseSharedLock(rs);
  }
  return true;
}

uint64_t Transaction::InsertLog(const RowPosition& pos, std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::InsertingLogRecord(prev_lsn_, txn_id_, pos, redo);
  prev_record_.emplace(pos, WriteEntry{WriteType::kInsert, ""});
  logger_->AddLog(lr);
  prev_lsn_ = lr.lsn;
  return prev_lsn_;
}

uint64_t Transaction::UpdateLog(const RowPosition& pos, std::string_view undo,
                                std::string_view redo) {
  assert(!IsFinished());
  LOG(TRACE) << "insert log innovated";
  LogRecord lr =
      LogRecord::UpdatingLogRecord(prev_lsn_, txn_id_, pos, redo, undo);
  prev_record_.emplace(pos, WriteEntry{WriteType::kUpdate, std::string(undo)});
  logger_->AddLog(lr);
  prev_lsn_ = lr.lsn;
  return prev_lsn_;
}

uint64_t Transaction::DeleteLog(const RowPosition& pos, std::string_view undo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::DeletingLogRecord(prev_lsn_, txn_id_, pos, undo);
  prev_record_.emplace(pos, WriteEntry{WriteType::kDelete, std::string(undo)});
  logger_->AddLog(lr);
  prev_lsn_ = lr.lsn;
  return prev_lsn_;
}

uint64_t Transaction::AllocatePageLog(uint64_t allocated_page_id,
                                      PageType new_page_type) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::AllocatePageLogRecord(
      prev_lsn_, txn_id_, allocated_page_id, new_page_type);
  logger_->AddLog(lr);
  prev_lsn_ = lr.lsn;
  return prev_lsn_;
}

uint64_t Transaction::DestroyPageLog(uint64_t destroyed_page_id) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::DestroyPageLogRecord(prev_lsn_, txn_id_, destroyed_page_id);
  logger_->AddLog(lr);
  prev_lsn_ = lr.lsn;
  return prev_lsn_;
}

// Using this function is discouraged to get performance of flush pipelining.
void Transaction::CommitWait() const {
  while (logger_->CommittedLSN() < prev_lsn_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}
bool Transaction::IsFinished() const {
  return status_ == TransactionStatus::kCommitted ||
         status_ == TransactionStatus::kAborted;
}

}  // namespace tinylamb
