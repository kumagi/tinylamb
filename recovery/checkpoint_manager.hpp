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

#ifndef TINYLAMB_CHECKPOINT_MANAGER_HPP
#define TINYLAMB_CHECKPOINT_MANAGER_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

inline constexpr uint32_t kCheckpointMasterMagic = 0x54494e4cU;
inline constexpr uint32_t kCheckpointMasterVersion = 1U;
inline constexpr size_t kCheckpointMasterRecordSize = 20U;

class TransactionManager;
class PageManager;

class CheckpointManager {
 public:
  CheckpointManager(const CheckpointManager&) = delete;
  CheckpointManager& operator=(const CheckpointManager&) = delete;
  CheckpointManager(CheckpointManager&&) = delete;
  CheckpointManager& operator=(CheckpointManager&&) = delete;

  CheckpointManager(std::string_view path, TransactionManager* tm,
                    PageManager* pm, size_t interval = 5)
      : master_record_path(path),
        tm_(tm),
        pm_(pm),
        interval_seconds_(interval),
        checkpoint_worker_([this] { WorkerThreadTask(); }) {
    RemoveStaleMasterTmp();
  }
  ~CheckpointManager();

  void Start() {
    if (!stop_.load(std::memory_order_acquire)) {
      start_.store(true, std::memory_order_release);
      worker_wait_cv_.notify_all();
    }
  }
  void Stop();

  void WorkerThreadTask();

  StatusOr<lsn_t> WriteCheckpoint(const std::function<void()>& func_for_test =
                                      []() {});
  void SetFlushBudgetMsForTest(size_t /*budget_ms*/) {}

  friend std::ostream& operator<<(std::ostream& o,
                                  const CheckpointManager& cm) {
    o << "CheckpointManager(start=" << cm.start_.load()
      << ", interval=" << cm.interval_seconds_
      << ", path=" << cm.master_record_path << ")";
    return o;
  }

 private:
  void RemoveStaleMasterTmp() const;

  std::atomic<bool> start_{false};
  std::atomic<bool> stop_{false};
  std::string master_record_path;
  TransactionManager* const tm_;
  PageManager* const pm_;
  size_t interval_seconds_;
  std::mutex checkpoint_mu_;
  std::mutex worker_wait_mu_;
  std::condition_variable worker_wait_cv_;
  std::thread checkpoint_worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CHECKPOINT_MANAGER_HPP
