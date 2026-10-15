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
#include <thread>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

inline constexpr uint32_t kCheckpointMasterMagic = 0x54494e4cU;
inline constexpr uint32_t kCheckpointMasterVersion = 1U;
inline constexpr size_t kCheckpointMasterRecordSize = 20U;

class TransactionManager;
class PagePool;

// Time budget (ms) for the checkpoint's dirty-page flush pass; see
// CheckpointManager::FlushDirtyPages. Read from
// TINYLAMB_CHECKPOINT_FLUSH_MS (default 1000; 0 disables the flush phase).
size_t FlushBudgetMsFromEnv();

class CheckpointManager {
 public:
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  CheckpointManager(const CheckpointManager&) = delete;
  CheckpointManager& operator=(const CheckpointManager&) = delete;
  CheckpointManager(CheckpointManager&&) = delete;
  CheckpointManager& operator=(CheckpointManager&&) = delete;
  CheckpointManager(std::string_view path, TransactionManager* tm, PagePool* pp,
                    size_t interval = 5)
      : master_record_path(path),
        tm_(tm),
        pp_(pp),
        interval_seconds_(interval),
        flush_budget_ms_(FlushBudgetMsFromEnv()),
        checkpoint_worker_([this] { WorkerThreadTask(); }) {
    RemoveStaleMasterTmp();
  }
  // |interval| seconds between periodic checkpoints; 0 disables periodic
  // checkpointing entirely (only explicit WriteCheckpoint calls run). The
  // worker itself stays parked until Start(), so constructing the manager
  // never arms a background pass.
  ~CheckpointManager();

  void Start() {
    if (!stop_.load(std::memory_order_acquire)) {
      start_.store(true, std::memory_order_release);
      worker_wait_cv_.notify_all();
    }
  }
  void Stop();

  void WorkerThreadTask();
  struct ActiveTransactionEntry {
    ActiveTransactionEntry() = default;
    ActiveTransactionEntry(uint64_t id, TransactionStatus st, lsn_t lsn)
        : txn_id(id), status(st), last_lsn(lsn) {}
    friend std::ostream& operator<<(std::ostream& o,
                                    const ActiveTransactionEntry& a) {
      o << "ID: " << a.txn_id << ", status: " << a.status
        << ", lastLSN: " << a.last_lsn;
      return o;
    }

    friend Encoder& operator<<(Encoder& e, const ActiveTransactionEntry& t) {
      // TransactionStatus is serialized as a fixed-width uint8_t so the WAL
      // encoding does not depend on uint_fast8_t's platform-defined width.
      e << t.txn_id << static_cast<uint8_t>(t.status) << t.last_lsn;
      return e;
    }
    friend Decoder& operator>>(Decoder& d, ActiveTransactionEntry& t) {
      uint8_t raw_status = 0;
      d >> t.txn_id >> raw_status >> t.last_lsn;
      t.status = static_cast<TransactionStatus>(raw_status);
      return d;
    }
    static size_t Size() {
      return sizeof(txn_id_t) + sizeof(uint8_t) + sizeof(lsn_t);
    }
    bool operator==(const ActiveTransactionEntry& rhs) const = default;
    txn_id_t txn_id = 0;
    TransactionStatus status = TransactionStatus::kRunning;
    lsn_t last_lsn = 0;
  };

  // This function is intentionally public for test.
  StatusOr<lsn_t> WriteCheckpoint(const std::function<void()>& func_for_test =
                                      []() {});
  void SetFlushBudgetMsForTest(size_t budget_ms) {
    flush_budget_ms_ = budget_ms;
  }

  friend std::ostream& operator<<(std::ostream& o,
                                  const CheckpointManager& cm) {
    o << "CheckpointManager(start=" << cm.start_.load()
      << ", interval=" << cm.interval_seconds_
      << ", path=" << cm.master_record_path << ")";
    return o;
  }

 private:
  // The individual terms of the WAL-recycle floor. The warning below names
  // the smallest one so an operator can tell a long-running transaction from
  // a never-flushed hot page without re-deriving the floor by hand.
  struct RecycleFloorTerms {
    lsn_t begin_lsn{0};
    lsn_t min_dirty_page_reclsn{std::numeric_limits<lsn_t>::max()};
    page_id_t min_dirty_page{0};
    lsn_t oldest_active_first_lsn{std::numeric_limits<lsn_t>::max()};
    txn_id_t oldest_active_txn{0};
    lsn_t unsynced_fpw{std::numeric_limits<lsn_t>::max()};
  };

  // One-per-interval warning when the retained WAL (BufferedLSN minus the
  // lowest retained segment start) exceeds TINYLAMB_WAL_RETENTION_WARN_BYTES
  // (default 1 GiB): the recycle floor is pinned and the WAL grows without
  // bound until the pinning constraint clears.
  void MaybeWarnWalRetention(lsn_t recycle_floor,
                             const RecycleFloorTerms& terms);

  // Removes a master-record temp file left by a crash between WriteMaster
  // Record's write and its rename. Harmless (the rename overwrites it), but
  // it is garbage a long-lived deployment should not accumulate.
  void RemoveStaleMasterTmp() const;

  // One-per-interval warning when the WAL volume's free space drops below
  // TINYLAMB_WAL_FREE_WARN_BYTES (default 1 GiB). ENOSPC fail-stops the
  // engine by design; this surfaces the trend while there is still time to
  // react (quota-visible space is what statvfs reports, which matches what
  // a write() will actually see).
  void MaybeWarnWalVolumeSpace();

  // Writes back resident dirty pages (oldest RecoveryLSN first) within a
  // time budget so the pool's oldest dirty page -- and with it the
  // WAL-recycling floor -- advances on every checkpoint. A purely fuzzy
  // checkpoint never writes pages, so a continuously modified hot page kept
  // its setup-era recLSN forever and pinned the recycle floor: the WAL grew
  // without bound under sustained load (~8.5 MB/s observed on TPC-C). The
  // budget (ms; TINYLAMB_CHECKPOINT_FLUSH_MS, default 1000, 0 disables)
  // bounds the checkpoint's stall on very large dirty sets.
  void FlushDirtyPages(std::vector<std::pair<page_id_t, lsn_t>>* candidates);

  std::atomic<bool> start_ = false;
  std::atomic<bool> stop_ = false;
  std::string master_record_path;
  TransactionManager* const tm_;
  PagePool* const pp_;
  size_t interval_seconds_;
  size_t flush_budget_ms_;
  // Rate limiting for MaybeWarnWalVolumeSpace; steady_clock milliseconds.
  std::atomic<int64_t> last_volume_space_warn_ms_{0};
  std::mutex checkpoint_mu_;
  std::mutex worker_wait_mu_;
  std::condition_variable worker_wait_cv_;
  std::thread checkpoint_worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CHECKPOINT_MANAGER_HPP
