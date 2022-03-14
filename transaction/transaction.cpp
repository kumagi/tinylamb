#include "transaction/transaction.hpp"

#include <iostream>

#include "page/row_position.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const TransactionStatus& t) {
  switch (t) {
    case TransactionStatus::kUnknown:
      o << "Unknown";
      break;
    case TransactionStatus::kRunning:
      o << "Running";
      break;
    case TransactionStatus::kCommitted:
      o << "Committed";
      break;
    case TransactionStatus::kAborted:
      o << "Aborted";
      break;
  }
  return o;
}

Transaction::Transaction(txn_id_t txn_id, TransactionManager* tm)
    : txn_id_(txn_id),
      status_(TransactionStatus::kRunning),
      transaction_manager_(tm) {}

Status Transaction::PreCommit() {
  Status result = transaction_manager_->PreCommit(*this);
  status_ = TransactionStatus::kCommitted;
  return result;
}

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

lsn_t Transaction::InsertLog(page_id_t pid, slot_t slot,
                             std::string_view redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::InsertingLogRecord(prev_lsn_, txn_id_, pid, slot, redo));
  return prev_lsn_;
}
lsn_t Transaction::InsertLeafLog(page_id_t pid, std::string_view key,
                                 std::string_view redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::InsertingLeafLogRecord(prev_lsn_, txn_id_, pid, key, redo));
  return prev_lsn_;
}
lsn_t Transaction::InsertInternalLog(page_id_t pid, std::string_view key,
                                     page_id_t redo) {
  assert(!IsFinished());
  prev_lsn_ =
      transaction_manager_->AddLog(LogRecord::InsertingInternalLogRecord(
          prev_lsn_, txn_id_, pid, key, redo));
  return prev_lsn_;
}

lsn_t Transaction::UpdateLog(page_id_t pid, slot_t slot, std::string_view undo,
                             std::string_view redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::UpdatingLogRecord(prev_lsn_, txn_id_, pid, slot, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::UpdateLeafLog(page_id_t pid, std::string_view key,
                                 std::string_view undo, std::string_view redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(LogRecord::UpdatingLeafLogRecord(
      prev_lsn_, txn_id_, pid, key, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::UpdateInternalLog(page_id_t pid, std::string_view key,
                                     page_id_t undo, page_id_t redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(LogRecord::UpdatingInternalLogRecord(
      prev_lsn_, txn_id_, pid, key, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::DeleteLog(page_id_t pid, slot_t slot,
                             std::string_view undo) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::DeletingLogRecord(prev_lsn_, txn_id_, pid, slot, undo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

lsn_t Transaction::DeleteLeafLog(page_id_t pid, std::string_view key,
                                 std::string_view undo) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::DeletingLeafLogRecord(prev_lsn_, txn_id_, pid, key, undo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

lsn_t Transaction::DeleteInternalLog(page_id_t pid, std::string_view key,
                                     page_id_t undo) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::DeletingInternalLogRecord(prev_lsn_, txn_id_, pid, key, undo);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

lsn_t Transaction::SetLowestLog(page_id_t pid, page_id_t lowest_value) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::SetLowestLogRecord(prev_lsn_, txn_id_, pid, lowest_value);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

lsn_t Transaction::AllocatePageLog(page_id_t allocated_page_id,
                                   PageType new_page_type) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::AllocatePageLogRecord(
      prev_lsn_, txn_id_, allocated_page_id, new_page_type);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

lsn_t Transaction::DestroyPageLog(page_id_t destroyed_page_id) {
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

lsn_t Transaction::SetPrevNextLog(page_id_t target, page_id_t undo_prev,
                                  page_id_t undo_next, page_id_t redo_prev,
                                  page_id_t redo_next) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::SetPrevNextLogRecord(
      prev_lsn_, txn_id_, target, undo_prev, undo_next, redo_prev, redo_next);
  prev_lsn_ = transaction_manager_->AddLog(lr);
  return prev_lsn_;
}

}  // namespace tinylamb
