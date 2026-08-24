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

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "page/page_pool.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

// Persists the master record so a crash can never leave a torn or empty
// file: write to a temp path, fsync it, then atomically rename over the real
// path. The directory fsync afterwards is best-effort -- without it a crash
// may revert the rename, but the previous (still valid) master record simply
// keeps recovery conservative.
void WriteMasterRecord(const std::filesystem::path& path, lsn_t lsn) {
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("Failed to open master record: " +
                               tmp.string());
    }
    std::array<char, sizeof(lsn_t)> encoded{};
    SerializeU64(encoded.data(), lsn);
    out.write(encoded.data(), encoded.size());
    out.flush();
    if (!out) {
      throw std::runtime_error("Failed to write master record: " +
                               tmp.string());
    }
  }
  std::filesystem::rename(tmp, path);
  const std::filesystem::path dir = path.has_parent_path()
                                        ? path.parent_path()
                                        : std::filesystem::path(".");
  const int dir_fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
  if (dir_fd >= 0) {
    ::fsync(dir_fd);
    ::close(dir_fd);
  }
}

}  // namespace

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
      try {
        WriteCheckpoint();
      } catch (const std::exception& e) {
        // The worker thread must never die from an IO failure; log and keep
        // retrying on the next tick.
        LOG(ERROR) << "Periodic checkpoint failed: " << e.what();
      }
    }
  }
}

lsn_t CheckpointManager::WriteCheckpoint(
    const std::function<void()>& func_for_test) {
  // The DPT snapshot takes each page's shared latch; callers must not hold
  // any page latch (PageRef) on this thread while checkpointing.
  LogRecord begin = LogRecord::BeginCheckpointLogRecord();

  // Write [BeginFullScan-Checkpoint] log.
  lsn_t begin_lsn = tm_->logger_->AddLog(begin.Serialize());

  std::vector<std::pair<page_id_t, lsn_t> > dirty_page_table;
  {
    // page_lsn/recovery_lsn are plain (non-atomic) fields mutated under the
    // per-page latch; read them under the shared page latch so a concurrent
    // SetRecLSN/SetPageLSN cannot tear the DPT entries published here.
    std::shared_lock latch(pp_->pool_latch);
    dirty_page_table.reserve(pp_->pool_.size());
    for (const auto& it : pp_->pool_) {
      PagePool::Entry& entry = *it.second;
      std::shared_lock page_latch(*entry.page_latch);
      dirty_page_table.emplace_back(it.first, entry.page->RecoveryLSN());
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
  // The master record must not overtake the WAL: wait until every record up
  // to (and including) EndCheckpoint is on stable storage, so a reader that
  // trusts begin_lsn always finds a complete checkpoint pair behind it.
  tm_->logger_->WaitForDurable(tm_->logger_->BufferedLSN());

  WriteMasterRecord(master_record_path, begin_lsn);
  return begin_lsn;
}
}  // namespace tinylamb
