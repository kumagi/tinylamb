#include "transaction/transaction_manager.hpp"

#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "page/page_ref.hpp"
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
  for (const auto& pr : txn.prev_record_) {
    RowPosition pos = pr.first;
    PageRef page = txn.page_manager_->GetPage(pos.page_id);
    RowPage& target = page.GetRowPage();
    switch (pr.second.entry_type) {
      case Transaction::WriteType::kInsert:
        txn.CompensateInsertLog(pos, pr.second.lsn);
        target.DeleteRow(pos.slot);
        break;
      case Transaction::WriteType::kUpdate: {
        LOG(ERROR) << "abort update";
        txn.CompensateUpdateLog(pos, pr.second.lsn, pr.second.payload);
        target.UpdateRow(pos.slot, pr.second.payload);
        break;
      }
      case Transaction::WriteType::kDelete:
        LOG(ERROR) << "abort delete";
        txn.CompensateDeleteLog(pos, pr.second.lsn, pr.second.payload);
        target.InsertRow(pr.second.payload);
        break;
    }
  }
  PreCommit(txn);  // Writes an empty commit.
}

}  // namespace tinylamb