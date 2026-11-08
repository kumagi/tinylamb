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

#include <algorithm>
#include <cstddef>
#include <exception>
#include <functional>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

CheckpointManager::CheckpointManager(std::string_view path,
                                     TransactionManager* tm, PageManager* pm,
                                     size_t interval)
    : master_record_path(path),
      tm_(tm),
      pm_(pm),
      interval_seconds_(interval),
      checkpoint_worker_([this] { WorkerThreadTask(); }) {
  if (pm_ != nullptr && interval_seconds_ != 0) {
    pm_->SetCompactionCallback([this] { RequestCompaction(); });
  }
}

CheckpointManager::~CheckpointManager() { Stop(); }

void CheckpointManager::Stop() {
  if (pm_ != nullptr) {
    pm_->SetCompactionCallback({});
  }
  {
    std::scoped_lock lk(worker_wait_mu_);
    stop_.store(true, std::memory_order_release);
  }
  worker_wait_cv_.notify_all();
  if (checkpoint_worker_.joinable()) {
    checkpoint_worker_.join();
  }
}

void CheckpointManager::RequestCompaction() {
  {
    std::scoped_lock lk(worker_wait_mu_);
    compaction_requested_ = true;
  }
  worker_wait_cv_.notify_one();
}

void CheckpointManager::WorkerThreadTask() {
  while (!stop_.load(std::memory_order_acquire)) {
    {
      std::unique_lock wait_lock(worker_wait_mu_);
      worker_wait_cv_.wait(wait_lock, [this] {
        return stop_.load(std::memory_order_acquire) ||
               (start_.load(std::memory_order_acquire) &&
                compaction_requested_);
      });
      if (stop_.load(std::memory_order_acquire) || interval_seconds_ == 0) {
        return;
      }
      compaction_requested_ = false;
    }
    try {
      const auto res = WriteCheckpoint();
      if (!res.HasValue()) {
        LOG(ERROR) << "Background compaction failed: " << res.GetStatus();
      }
    } catch (const std::exception& error) {
      LOG(ERROR) << "Background compaction threw: " << error.what();
    } catch (...) {
      LOG(ERROR) << "Background compaction threw an unknown exception";
    }
  }
}

StatusOr<lsn_t> CheckpointManager::WriteCheckpoint(
    const std::function<void()>& func_for_test) {
  std::scoped_lock checkpoint_guard(checkpoint_mu_);
  if (tm_->FailStopped() || tm_->logger_->Failed()) {
    return StatusError(StatusCode::kIOError,
                       "cannot checkpoint a failed transaction engine");
  }

  lsn_t oldest_active_first_lsn = std::numeric_limits<lsn_t>::max();
  lsn_t snap_lsn = 0;
  {
    std::scoped_lock lk(tm_->transaction_table_lock);
    snap_lsn = tm_->logger_->BufferedLSN();
    for (const auto& it : tm_->active_transactions_) {
      std::scoped_lock chain_guard(it.second->chain_mu_);
      if (it.second->HasLogRecord()) {
        oldest_active_first_lsn =
            std::min(oldest_active_first_lsn, it.second->FirstLSN());
      }
    }
  }

  const lsn_t replay_start_lsn = std::min(snap_lsn, oldest_active_first_lsn);

  func_for_test();

  const lsn_t durable_end = tm_->logger_->BufferedLSN();
  RETURN_IF_FAIL(tm_->logger_->WaitForDurable(durable_end));

  if (pm_ != nullptr) {
    RETURN_IF_FAIL(pm_->WriteFuzzyCheckpoint(replay_start_lsn));
  }

  tm_->logger_->AdviseOldBytesDurable(replay_start_lsn);
  tm_->logger_->RecycleUpTo(replay_start_lsn);
  return replay_start_lsn;
}

}  // namespace tinylamb
