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

Transaction::Transaction(uint64_t txn_id, TransactionManager* tm)
    : txn_id_(txn_id),
      status_(TransactionStatus::kRunning),
      transaction_manager_(tm) {}

bool Transaction::PreCommit() {
  bool result = transaction_manager_->PreCommit(*this);
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

uint64_t Transaction::InsertLog(const RowPosition& pos, std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::InsertingLogRecord(lsns_.back(), txn_id_, pos, redo);
  lsns_.push_back(transaction_manager_->AddLog(lr));
  return lsns_.back();
}

uint64_t Transaction::UpdateLog(const RowPosition& pos, std::string_view undo,
                                std::string_view redo) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::UpdatingLogRecord(lsns_.back(), txn_id_, pos, redo, undo);
  lsns_.push_back(transaction_manager_->AddLog(lr));
  return lsns_.back();
}

uint64_t Transaction::DeleteLog(const RowPosition& pos, std::string_view undo) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::DeletingLogRecord(lsns_.back(), txn_id_, pos, undo);
  lsns_.push_back(transaction_manager_->AddLog(lr));
  return lsns_.back();
}

uint64_t Transaction::AllocatePageLog(uint64_t allocated_page_id,
                                      PageType new_page_type) {
  assert(!IsFinished());
  LogRecord lr = LogRecord::AllocatePageLogRecord(
      lsns_.back(), txn_id_, allocated_page_id, new_page_type);
  lsns_.push_back(transaction_manager_->AddLog(lr));
  return lsns_.back();
}

uint64_t Transaction::DestroyPageLog(uint64_t destroyed_page_id) {
  assert(!IsFinished());
  LogRecord lr =
      LogRecord::DestroyPageLogRecord(lsns_.back(), txn_id_, destroyed_page_id);
  lsns_.push_back(transaction_manager_->AddLog(lr));
  return lsns_.back();
}

// Using this function is discouraged to get performance of flush pipelining.
void Transaction::CommitWait() const {
  while (transaction_manager_->CommittedLSN() < lsns_.back()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

}  // namespace tinylamb
