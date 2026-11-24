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

#include "common/platform/process.hpp"
#include "recovery/checkpoint_manager.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
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
    : database_path_(path), tm_(tm), pm_(pm), interval_seconds_(interval) {
  // Do NOT start the worker in the member-init list. SetCompactionCallback
  // copies a std::function and can throw bad_alloc; if the constructor body
  // throws after a joinable thread was started, ~thread calls terminate
  // instead of letting the caller map the failure to a Status.
  if (interval_seconds_ == 0) {
    return;
  }
  if (pm_ != nullptr) {
    pm_->SetCompactionCallback([this] { RequestCompaction(); });
  }
  checkpoint_worker_ = std::thread([this] { WorkerThreadTask(); });
}

CheckpointManager::~CheckpointManager() { Stop(); }

void CheckpointManager::Start() {
  // Serialize the whole lifecycle transition with Stop(): callback
  // registration and the worker (re)spawn must be observed as one step,
  // otherwise the loser of a Start/Stop race can leave a stale callback or
  // erase a freshly installed one.
  // lifecycle_mu_ serializes this transition. SetCompactionCallback briefly
  // takes and releases PageManager's compaction_cb_mu_; only then do we take
  // worker_wait_mu_ below. These are sequential acquisitions, not a nested
  // three-lock scope. The callback path itself runs under compaction_cb_mu_
  // and may take worker_wait_mu_, so register before taking worker_wait_mu_.
  std::scoped_lock lifecycle_guard(lifecycle_mu_);
  // T3687: Stop() joined the worker thread out of existence, so a plain
  // predicate flip could never resume processing.  Re-arm stop_, re-
  // register the compaction callback and spawn a fresh worker; the old
  // behavior was a silent no-op that left every later compaction request
  // unconsumed (WAL grew forever).
  if (interval_seconds_ == 0) {
    LOG(WARN) << "CheckpointManager::Start() ignored: checkpointing is "
                 "disabled (interval_seconds == 0)";
    return;
  }
  // Register the callback BEFORE taking worker_wait_mu_: the LSM compaction
  // thread fires the callback under compaction_cb_mu_, and RequestCompaction
  // grabs worker_wait_mu_ -- acquiring cb_mu_ under worker_wait_mu_ would
  // invert that order.
  if (stop_.load(std::memory_order_acquire) && pm_ != nullptr) {
    pm_->SetCompactionCallback([this] { RequestCompaction(); });
  }
  std::scoped_lock lk(worker_wait_mu_);
  // Re-check under the lock: a concurrent Start() may have restarted
  // already, in which case this call only (re-)armed start_.
  if (stop_.load(std::memory_order_acquire) && !checkpoint_worker_.joinable()) {
    stop_.store(false, std::memory_order_release);
    checkpoint_worker_ = std::thread([this] { WorkerThreadTask(); });
  }
  start_.store(true, std::memory_order_release);
  worker_wait_cv_.notify_all();
}

void CheckpointManager::Stop() {
  // Pairs with Start()'s lifecycle_guard: a concurrent Stop/Start pair can no
  // longer interleave their SetCompactionCallback calls. Same order as Start:
  // lifecycle_mu_ -> compaction_cb_mu_ (SetCompactionCallback below) ->
  // worker_wait_mu_. join() waits for a worker that never takes lifecycle_mu_.
  std::scoped_lock lifecycle_guard(lifecycle_mu_);
  if (pm_ != nullptr) {
    pm_->SetCompactionCallback({});
  }
  {
    std::scoped_lock lk(worker_wait_mu_);
    stop_.store(true, std::memory_order_release);
    start_.store(false, std::memory_order_release);
  }
  worker_wait_cv_.notify_all();
  if (checkpoint_worker_.joinable()) {
    checkpoint_worker_.join();
  }
}

void CheckpointManager::RequestCompaction() {
  {
    // This is the only mutex held here, and it is released before the
    // notification. The compaction callback holds compaction_cb_mu_ while
    // calling in, matching the lifecycle_mu_ -> cb_mu_ -> worker_wait_mu_
    // order used by Start()/Stop().
    std::scoped_lock lk(worker_wait_mu_);
    compaction_requested_ = true;
    ++request_generation_;
  }
  worker_wait_cv_.notify_one();
}

void CheckpointManager::WorkerThreadTask() {
  // T4262: interval_seconds_ is the worker's poll period.  The wait is
  // wait_for(interval), so the worker wakes on its own even when no
  // compaction callback ever fires.  A wake with nothing pending and WAL
  // retention below the checkpoint-lag threshold touches no disk, keeping
  // the documented "compaction-driven persistence, no idle periodic WAL
  // writes" design intact while giving the interval setting real meaning:
  // retained WAL above the lag threshold is checkpointed within one
  // interval instead of growing unboundedly (previously the interval was
  // only consulted as an enable/disable boolean at :interval_seconds_==0).
  while (!stop_.load(std::memory_order_acquire)) {
    bool requested = false;
    uint64_t consumed_generation = 0;
    {
      std::unique_lock wait_lock(worker_wait_mu_);
      worker_wait_cv_.wait_for(
          wait_lock, std::chrono::seconds(interval_seconds_), [this] {
            return stop_.load(std::memory_order_acquire) ||
                   (start_.load(std::memory_order_acquire) &&
                    compaction_requested_);
          });
      if (stop_.load(std::memory_order_acquire) || interval_seconds_ == 0) {
        return;
      }
      requested = compaction_requested_;
      consumed_generation = request_generation_;
    }
    if (!requested) {
      if (!start_.load(std::memory_order_acquire)) {
        continue;
      }
      // Periodic watchdog pass: only checkpoint when the retained (durable
      // but not yet recycled) WAL has grown past the lag threshold. An
      // embedded caller may run without a transaction manager / logger, in
      // which case there is no WAL lag to measure.
      if (tm_ == nullptr || tm_->logger_ == nullptr) {
        // Without a logger there is no WAL to checkpoint and no lag to
        // measure. Keep the worker alive (so a later logger can be used) but
        // make the otherwise silent no-op visible once to the operator.
        if (!no_logger_warned_.exchange(true, std::memory_order_relaxed)) {
          LOG(WARN) << "CheckpointManager: no transaction logger; background "
                       "checkpointing is idle (no WAL lag to measure)";
        }
        continue;
      }
      const lsn_t lowest_retained = tm_->logger_->LowestRetainedLSN();
      const lsn_t durable_lsn = tm_->logger_->DurableLSN();
      const uint64_t retained_bytes =
          durable_lsn > lowest_retained
              ? static_cast<uint64_t>(durable_lsn - lowest_retained)
              : 0;
      if (retained_bytes < WalLagCheckpointBytes()) {
        continue;
      }
    }
    bool checkpoint_ok = false;
    try {
      const auto res = WriteCheckpoint();
      if (!res.HasValue()) {
        LOG(ERROR) << "Background compaction failed: " << res.GetStatus();
      } else {
        checkpoint_ok = true;
      }
    } catch (const std::exception& error) {
      LOG(ERROR) << "Background compaction threw: " << error.what();
    } catch (...) {
      LOG(ERROR) << "Background compaction threw an unknown exception";
    }
    // Keep the request pending on failure so the next worker pass retries
    // instead of dropping the checkpoint until the next threshold/callback.
    if (checkpoint_ok) {
      worker_checkpoints_.fetch_add(1, std::memory_order_release);
      // No other mutex is held at this point (the wait_lock scope ended
      // above); this worker_wait_mu_ acquisition is disjoint from the one
      // used later in the failure branch, so no nested lock pair exists.
      std::scoped_lock lk(worker_wait_mu_);
      // Clear only the request this pass consumed; a RequestCompaction that
      // arrived while the checkpoint ran keeps the next pass armed.
      if (requested && request_generation_ == consumed_generation) {
        compaction_requested_ = false;
      }
    } else if (requested) {
      // T4262 companion: the predicate would re-satisfy immediately with the
      // request still pending, retrying in a hot loop until success.  Wait
      // out one interval as the retry backoff — but a NEWER request (a
      // bumped generation) still wakes the worker early instead of waiting
      // out the full interval.
      std::unique_lock wait_lock(worker_wait_mu_);
      worker_wait_cv_.wait_for(
          wait_lock, std::chrono::seconds(interval_seconds_),
          [this, consumed_generation] {
            return stop_.load(std::memory_order_acquire) ||
                   request_generation_ != consumed_generation;
          });
    }
  }
}

StatusOr<lsn_t> CheckpointManager::WriteCheckpoint(
    const std::function<void()>& func_for_test) {
  // checkpoint_mu_ is this class's outermost private lock: it serializes
  // checkpoints and is held before transaction_table_lock / chain_mu_ below
  // (docs/lock_order.md rule 8). No path acquires it while holding those.
  std::scoped_lock checkpoint_guard(checkpoint_mu_);
  if (tm_ == nullptr || tm_->logger_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot checkpoint without a transaction logger");
  }
  if (tm_->FailStopped() || tm_->logger_->Failed()) {
    return StatusError(StatusCode::kIOError,
                       "cannot checkpoint a failed transaction engine");
  }
  // Without a PageManager there is no snapshot to write. Recycling WAL in
  // that case would discard committed history with no durable image left;
  // fail before any Advise/Recycle work instead of skipping only the snapshot.
  if (pm_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot checkpoint without a page manager");
  }

  // Run the test hook BEFORE capturing snap_lsn: any WAL it appends is then
  // inside the durability target below, so a snapshot that reflects those
  // records can never outlive their COMMIT record (WAL-before-data).
  if (func_for_test) {
    try {
      func_for_test();
    } catch (const std::exception& error) {
      return StatusError(StatusCode::kRuntimeError,
                         std::string("checkpoint test hook failed: ") +
                             error.what());
    } catch (...) {
      return StatusError(StatusCode::kRuntimeError,
                         "checkpoint test hook failed with unknown exception");
    }
  }

  lsn_t oldest_active_first_lsn = std::numeric_limits<lsn_t>::max();
  txn_id_t oldest_active_txn_id = 0;
  lsn_t snap_lsn = 0;
  {
    // CheckpointManager's outermost private lock, acquired above the
    // transaction table lock and per-transaction chain mutexes below it
    // (docs/lock_order.md rule 8). Nothing takes checkpoint_mu_ while
    // holding a transaction/chain lock, so this order cannot invert.
    std::scoped_lock lk(tm_->transaction_table_lock);
    snap_lsn = tm_->logger_->BufferedLSN();
    for (const auto& it : tm_->active_transactions_) {
      std::scoped_lock chain_guard(it.second->chain_mu_);
      if (it.second->HasLogRecord()) {
        if (it.second->FirstLSN() < oldest_active_first_lsn) {
          oldest_active_first_lsn = it.second->FirstLSN();
          oldest_active_txn_id = it.first;
        }
      }
    }
  }

  const lsn_t replay_start_lsn = std::min(snap_lsn, oldest_active_first_lsn);

  // Wait for the LSN captured under the transaction-table lock, not a fresh
  // BufferedLSN(): the snapshot's replay anchor is derived from snap_lsn, so
  // unrelated WAL appended after the capture is not part of this checkpoint
  // and must not hold up the fsync wait. See docs/recovery_invariants.md
  // ("waits for the WAL's captured durable target").
  RETURN_IF_FAIL(tm_->logger_->WaitForDurable(snap_lsn));

  // pm_ was checked non-null above; no need to test it again here.
  RETURN_IF_FAIL(pm_->WriteFuzzyCheckpoint(replay_start_lsn));

  tm_->logger_->AdviseOldBytesDurable(replay_start_lsn);
  tm_->logger_->RecycleUpTo(replay_start_lsn);

  const lsn_t lowest_retained = tm_->logger_->LowestRetainedLSN();
  const lsn_t durable_lsn = tm_->logger_->DurableLSN();
  const uint64_t retained_bytes =
      durable_lsn > lowest_retained ? (durable_lsn - lowest_retained) : 0;
  MaybeWarnWalRetention(retained_bytes, oldest_active_txn_id,
                        oldest_active_first_lsn, replay_start_lsn,
                        lowest_retained, durable_lsn);
  return replay_start_lsn;
}

uint64_t CheckpointManager::DefaultWalRetentionWarnBytes() noexcept {
  static const uint64_t kDefaultBytes = []() -> uint64_t {
    const char* raw = ::tinylamb::platform::GetEnv("TINYLAMB_WAL_RETENTION_WARN_BYTES");
    if (raw == nullptr || *raw == '\0') {
      return uint64_t{64} << 20;
    }
    if (*raw == '-' || *raw == '+') {
      return uint64_t{64} << 20;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0') {
      return uint64_t{64} << 20;
    }
    return static_cast<uint64_t>(parsed);
  }();
  return kDefaultBytes;
}

void CheckpointManager::MaybeWarnWalRetention(
    uint64_t retained_bytes, txn_id_t oldest_txn, lsn_t oldest_first_lsn,
    lsn_t replay_anchor_lsn, lsn_t lowest_retained_lsn, lsn_t durable_lsn) {
  const uint64_t override_bytes =
      wal_retention_warn_bytes_override_.load(std::memory_order_acquire);
  const uint64_t warn_bytes =
      override_bytes != std::numeric_limits<uint64_t>::max()
          ? override_bytes
          : DefaultWalRetentionWarnBytes();
  if (warn_bytes == 0 || retained_bytes < warn_bytes) {
    return;
  }
  const int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t last = last_wal_retention_warn_ms_.load(std::memory_order_relaxed);
  constexpr int64_t kWarnIntervalMs = int64_t{60} * 1000;
  if (last != 0 && now_ms - last < kWarnIntervalMs) {
    return;
  }
  if (!last_wal_retention_warn_ms_.compare_exchange_strong(
          last, now_ms == 0 ? 1 : now_ms, std::memory_order_relaxed)) {
    return;
  }
  wal_retention_warn_count_.fetch_add(1, std::memory_order_relaxed);
  LOG(WARN) << "WAL retention is high: " << retained_bytes
            << " bytes retained (warn threshold=" << warn_bytes
            << "B, lowest_retained_lsn=" << lowest_retained_lsn
            << ", durable_lsn=" << durable_lsn
            << ", replay_anchor_lsn=" << replay_anchor_lsn
            << ", oldest_active_txn=" << oldest_txn
            // With no active transaction there is no "oldest first LSN";
            // printing the checkpoint's replay anchor under that label would
            // misdiagnose the retention cause.
            << ", oldest_active_first_lsn="
            << (oldest_txn != 0 ? std::to_string(oldest_first_lsn)
                                : std::string("none"))
            << "). Finish or abort long-running transactions to recycle old "
               "WAL segments.";
}

}  // namespace tinylamb
