//
// Created by kumagi on 2022/01/09.
//

#include "recovery/checkpoint_manager.hpp"

#include "page/page_pool.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

CheckpointManager::~CheckpointManager() {
  stop_ = true;
  checkpoint_worker_.join();
}

void CheckpointManager::WorkerThreadTask() {
  while (!start_ && !stop_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  while (!stop_) {
    for (size_t waited = 0; waited < interval_seconds_ * 1000 && !stop_;
         ++waited) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!stop_) {
      LOG(INFO) << "Start periodic checkpointing";
      WriteCheckpoint();
    }
  }
}

uint64_t CheckpointManager::WriteCheckpoint(
    const std::function<void()>& func_for_test) {
  LogRecord begin = LogRecord::BeginCheckpointLogRecord();

  // Write [Begin-Checkpoint] log.
  uint64_t begin_lsn = tm_->logger_->AddLog(begin);

  std::vector<std::pair<uint64_t, uint64_t>> dirty_page_table;
  {
    std::scoped_lock latch(pp_->pool_latch);
    dirty_page_table.reserve(pp_->pool_.size());
    for (const auto& it : pp_->pool_) {
      dirty_page_table.emplace_back(it.first, it.second->page->RecoveryLSN());
    }
  }
  std::vector<ActiveTransactionEntry> active_transaction_table;
  {
    std::scoped_lock lk(tm_->transaction_table_lock);
    active_transaction_table.reserve(tm_->active_transactions_.size());
    for (const auto& it : tm_->active_transactions_) {
      active_transaction_table.emplace_back(it.first, it.second->status_,
                                            it.second->PrevLSN());
    }
  }

  LogRecord end = LogRecord::EndCheckpointLogRecord(dirty_page_table,
                                                    active_transaction_table);

  func_for_test();

  // Write [End-Checkpoint] log.
  tm_->AddLog(end);

  std::ofstream master_log_file;
  master_log_file.open(master_record_path);
  master_log_file.write(reinterpret_cast<char*>(&begin_lsn), sizeof(begin_lsn));
  return begin_lsn;
}

}  // namespace tinylamb