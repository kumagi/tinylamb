/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

lsn_t CheckpointManager::WriteCheckpoint(
    const std::function<void()>& func_for_test) {
  LogRecord begin = LogRecord::BeginCheckpointLogRecord();

  // Write [BeginFullScan-Checkpoint] log.
  lsn_t begin_lsn = tm_->logger_->AddLog(begin.Serialize());

  std::vector<std::pair<page_id_t, lsn_t>> dirty_page_table;
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