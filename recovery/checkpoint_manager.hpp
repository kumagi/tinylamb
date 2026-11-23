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
#include <limits>
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
                    PageManager* pm, size_t interval = 5);
  ~CheckpointManager();

  // T3687: Start() is restart-safe.  Before the first Stop() it just arms
  // the worker predicate; after Stop() (which joins the worker thread out
  // of existence) it re-arms stop_, re-registers the PageManager
  // compaction callback and spawns a fresh worker, so a manager that was
  // stopped -- e.g. around EmulateCrash -- becomes fully functional again
  // instead of silently staying dead.  With interval 0 the worker exits
  // immediately by design and Start() only logs a diagnostic.
  void Start();
  void Stop();
  void RequestCompaction();

  void WorkerThreadTask();

  StatusOr<lsn_t> WriteCheckpoint(const std::function<void()>& func_for_test =
                                      []() {});
  void SetWalRetentionWarnBytesForTest(uint64_t bytes) noexcept {
    wal_retention_warn_bytes_override_.store(bytes, std::memory_order_release);
    last_wal_retention_warn_ms_.store(0, std::memory_order_release);
    wal_retention_warn_count_.store(0, std::memory_order_release);
  }
  // T4262: override for the WAL-lag checkpoint trigger used by the worker's
  // periodic (every interval_seconds_) watchdog pass.  Default is half the
  // retention-warning threshold.
  void SetWalLagCheckpointBytesForTest(uint64_t bytes) noexcept {
    wal_lag_checkpoint_bytes_override_.store(bytes, std::memory_order_release);
  }
  [[nodiscard]] uint64_t WalLagCheckpointBytes() const noexcept {
    const uint64_t override_bytes =
        wal_lag_checkpoint_bytes_override_.load(std::memory_order_acquire);
    if (override_bytes != 0) {
      return override_bytes;
    }
    const uint64_t warn_bytes = DefaultWalRetentionWarnBytes();
    return warn_bytes >= 2 ? warn_bytes / 2 : warn_bytes;
  }
  [[nodiscard]] uint64_t WalRetentionWarnCountForTest() const noexcept {
    return wal_retention_warn_count_.load(std::memory_order_acquire);
  }
  // Number of checkpoints the background worker has completed successfully.
  // Lets tests observe that the interval watchdog (T4262) and the
  // Stop()->Start() restart (T3687) actually put the worker back to work.
  [[nodiscard]] uint64_t WorkerCheckpointsForTest() const noexcept {
    return worker_checkpoints_.load(std::memory_order_acquire);
  }

  friend std::ostream& operator<<(std::ostream& o,
                                  const CheckpointManager& cm) {
    o << "CheckpointManager(start=" << cm.start_.load()
      << ", interval=" << cm.interval_seconds_
      << ", path=" << cm.database_path_ << ")";
    return o;
  }

 private:
  std::atomic<bool> start_{false};
  std::atomic<bool> stop_{false};
  bool compaction_requested_{false};
  std::string database_path_;
  TransactionManager* const tm_;
  PageManager* const pm_;
  size_t interval_seconds_;
  std::mutex checkpoint_mu_;
  std::mutex worker_wait_mu_;
  // Serializes Start()/Stop() as a whole. Callback registration must happen
  // outside worker_wait_mu_ (the compaction thread holds compaction_cb_mu_
  // while calling into RequestCompaction), so a separate lock is needed to
  // stop a concurrent Stop() from clearing a callback Start() just installed
  // -- or Start() re-installing one Stop() just cleared.
  std::mutex lifecycle_mu_;
  std::condition_variable worker_wait_cv_;
  // WAL retention warning support (`TINYLAMB_WAL_RETENTION_WARN_BYTES`).
  static uint64_t DefaultWalRetentionWarnBytes() noexcept;
  void MaybeWarnWalRetention(uint64_t retained_bytes, txn_id_t oldest_txn,
                             lsn_t oldest_first_lsn, lsn_t replay_anchor_lsn,
                             lsn_t lowest_retained_lsn, lsn_t durable_lsn);
  std::atomic<uint64_t> wal_retention_warn_bytes_override_{
      std::numeric_limits<uint64_t>::max()};
  std::atomic<int64_t> last_wal_retention_warn_ms_{0};
  std::atomic<uint64_t> wal_retention_warn_count_{0};
  // T4262: bytes of retained (durable, un-recycled) WAL that make the
  // worker's periodic watchdog pass run a checkpoint without a compaction
  // request; 0 selects the default (half the retention-warning threshold).
  std::atomic<uint64_t> wal_lag_checkpoint_bytes_override_{0};
  // Bumped by every RequestCompaction so the worker can tell "the request
  // I am about to clear is still the one I consumed" from a request that
  // arrived while the checkpoint was running.
  uint64_t request_generation_ = 0;
  std::atomic<uint64_t> worker_checkpoints_{0};
  // One-shot diagnostic for a logger-less manager: the worker cannot measure
  // WAL lag in that configuration, so warn once instead of staying silent.
  std::atomic<bool> no_logger_warned_{false};
  std::thread checkpoint_worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CHECKPOINT_MANAGER_HPP
