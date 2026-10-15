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

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
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
Status WriteMasterRecord(const std::filesystem::path& path, lsn_t lsn) {
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      return StatusError(StatusCode::kIOError,
                         "Failed to open master record: " + tmp.string());
    }
    std::array<char, kCheckpointMasterRecordSize> encoded{};
    SerializeU32(encoded.data(), kCheckpointMasterMagic);
    SerializeU32(encoded.data() + sizeof(uint32_t), kCheckpointMasterVersion);
    SerializeU64(encoded.data() + (sizeof(uint32_t) * 2), lsn);
    const uint32_t crc =
        Crc32C(encoded.data(), kCheckpointMasterRecordSize - sizeof(uint32_t));
    SerializeU32(
        encoded.data() + kCheckpointMasterRecordSize - sizeof(uint32_t), crc);
    out.write(encoded.data(), encoded.size());
    out.flush();
    if (!out) {
      std::error_code ec;
      std::filesystem::remove(tmp, ec);
      return StatusError(StatusCode::kIOError,
                         "Failed to write master record: " + tmp.string());
    }
    // The comment contract above promises a file fsync before the rename:
    // without it a crash can leave an empty/torn master record behind even
    // though the directory entry was synced. std::ofstream has no fd
    // accessor, so flush through the OS handle for the same path.
    out.close();
#ifdef __APPLE__
    const int fd = ::open(tmp.c_str(), O_RDONLY | O_CLOEXEC);
    int rc = -1;
    int sync_errno = errno;
    if (fd >= 0) {
      rc = ::fcntl(fd, F_FULLFSYNC);
      sync_errno = errno;
      ::close(fd);
    }
#else
    const int fd = ::open(tmp.c_str(), O_RDONLY | O_CLOEXEC);
    int rc = -1;
    int sync_errno = errno;
    if (fd >= 0) {
      do {
        rc = ::fdatasync(fd);
      } while (rc < 0 && errno == EINTR);
      // ::close below can overwrite errno; keep the sync error for reporting.
      sync_errno = errno;
      ::close(fd);
    }
#endif
    if (rc != 0) {
      std::error_code ec;
      std::filesystem::remove(tmp, ec);
      return StatusError(StatusCode::kIOError,
                         "Failed to fsync master record: " + tmp.string() +
                             ": " + std::strerror(sync_errno));
    }
  }
  std::error_code rename_ec;
  std::filesystem::rename(tmp, path, rename_ec);
  if (rename_ec) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return StatusError(StatusCode::kIOError,
                       "Failed to rename master record to " + path.string() +
                           ": " + rename_ec.message());
  }
  const std::filesystem::path dir =
      path.has_parent_path() ? path.parent_path() : std::filesystem::path(".");
  const int dir_fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
  if (dir_fd < 0) {
    const int saved = errno;
    return StatusError(StatusCode::kIOError,
                       "Failed to open checkpoint directory: " + dir.string() +
                           ": " + std::strerror(saved));
  }
  int sync_rc = 0;
  int sync_errno = 0;
  do {
    sync_rc = ::fsync(dir_fd);
    sync_errno = errno;
  } while (sync_rc != 0 && sync_errno == EINTR);
  const int close_rc = ::close(dir_fd);
  if (sync_rc != 0) {
    return StatusError(StatusCode::kIOError,
                       "Failed to fsync checkpoint directory: " + dir.string() +
                           ": " + std::strerror(sync_errno));
  }
  if (close_rc != 0) {
    return StatusError(StatusCode::kIOError,
                       "Failed to close checkpoint directory: " + dir.string() +
                           ": " + std::strerror(errno));
  }
  return Status::kSuccess;
}

}  // namespace

size_t FlushBudgetMsFromEnv() {
  constexpr size_t kDefaultFlushBudgetMs = 1000;
  constexpr size_t kMaxFlushBudgetMs = 60'000;
  const char* env = std::getenv("TINYLAMB_CHECKPOINT_FLUSH_MS");
  if (env == nullptr || env[0] == '\0') {
    return kDefaultFlushBudgetMs;
  }
  const unsigned long long parsed = std::strtoull(env, nullptr, 10);
  if (parsed == 0) {
    return 0;  // 0 disables the flush phase entirely.
  }
  return static_cast<size_t>(
      std::min<unsigned long long>(parsed, kMaxFlushBudgetMs));
}

namespace {
// Rate-limited WAL-retention warning: see WriteCheckpoint tail. One warning
// per process per interval keeps a saturated log from spamming while the
// condition persists.
constexpr std::chrono::milliseconds kRetentionWarnInterval{10 * 60 * 1000};
std::atomic<int64_t> last_retention_warn_ms{0};

lsn_t RetentionWarnThresholdBytes() {
  constexpr lsn_t kDefaultWarnBytes = lsn_t{1} << 30;  // 1 GiB
  const char* env = std::getenv("TINYLAMB_WAL_RETENTION_WARN_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return kDefaultWarnBytes;
  }
  const unsigned long long parsed = std::strtoull(env, nullptr, 10);
  return parsed == 0 ? kDefaultWarnBytes : static_cast<lsn_t>(parsed);
}
}  // namespace

CheckpointManager::~CheckpointManager() { Stop(); }

void CheckpointManager::Stop() {
  stop_.store(true, std::memory_order_release);
  worker_wait_cv_.notify_all();
  if (checkpoint_worker_.joinable()) {
    checkpoint_worker_.join();
  }
}

void CheckpointManager::WorkerThreadTask() {
  // Periodic checkpointing starts only after Start(): a CheckpointManager
  // constructed for explicit WriteCheckpoint() calls (tests, tooling) must
  // never run a background pass against a storage stack the owner is
  // concurrently tearing down or re-creating -- the earlier unconditional
  // loop produced exactly that use-after-free. interval_seconds_ == 0 keeps
  // the worker parked forever (periodic checkpointing disabled).
  {
    std::unique_lock wait_lock(worker_wait_mu_);
    worker_wait_cv_.wait(wait_lock, [this] {
      return start_.load(std::memory_order_acquire) ||
             stop_.load(std::memory_order_acquire);
    });
  }
  while (!stop_ && interval_seconds_ != 0) {
    for (size_t waited = 0; waited < interval_seconds_ * 10 && !stop_;
         ++waited) {
      std::unique_lock wait_lock(worker_wait_mu_);
      worker_wait_cv_.wait_for(wait_lock, std::chrono::milliseconds(100),
                               [this] { return stop_.load(); });
    }
    if (!stop_) {
      LOG(INFO) << "Start periodic checkpointing";
      // Exception boundary: a checkpoint failure (Status or a throw out of
      // the DPT snapshot / record serialization under memory pressure) must
      // degrade to "this pass failed, retry next interval" -- the process is
      // a months-long storage engine, and letting an exception escape the
      // worker thread would std::terminate the whole database for a
      // condition the very next pass may clear. The startup (end-of-recovery)
      // checkpoint keeps failing visibly by design; only the periodic loop
      // absorbs.
      StatusOr<lsn_t> checkpoint_lsn(
          StatusError(StatusCode::kRuntimeError, "checkpoint not attempted"));
      try {
        checkpoint_lsn = WriteCheckpoint();
      } catch (const std::exception& error) {
        checkpoint_lsn = StatusError(
            StatusCode::kRuntimeError,
            std::string("periodic checkpoint threw: ") + error.what());
      } catch (...) {
        checkpoint_lsn = StatusError(StatusCode::kRuntimeError,
                                     "periodic checkpoint threw an unknown "
                                     "exception");
      }
      if (!checkpoint_lsn.HasValue()) {
        LOG(ERROR) << "Periodic checkpoint failed: "
                   << checkpoint_lsn.GetStatus();
      }
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
  // The DPT snapshot takes each page's shared latch; callers must not hold
  // any page latch (PageRef) on this thread while checkpointing.
  LogRecord begin = LogRecord::BeginCheckpointLogRecord();

  // Write [BeginFullScan-Checkpoint] log.
  ASSIGN_OR_RETURN(lsn_t, begin_lsn, tm_->logger_->AddLog(begin.Serialize()));
  RETURN_IF_FAIL(pp_->FlushPage(kMetaPageId));

  // Shared DPT-snapshot protocol: pin every resident entry under the shared
  // pool_latch first (pinning blocks DetachVictim, which needs pool_latch
  // exclusively and rechecks pins under the stripe mutex), release
  // pool_latch, then read each RecoveryLSN under the shared page latch so a
  // concurrent SetRecLSN/SetPageLSN cannot tear the entries published here.
  // Lock order: never take a page latch while holding pool_latch.  A
  // GetPage miss acquires pool_latch exclusively while its caller holds a
  // page latch (e.g. meta-page allocation across GetPage), so the old
  // pool_latch -> page_latch nesting here could deadlock.
  const auto snapshot_dpt =
      [&](std::vector<std::pair<page_id_t, lsn_t>>* dpt,
          std::vector<std::pair<page_id_t, lsn_t>>* dirty_out) {
        std::vector<std::pair<PagePool::Entry*, page_id_t>> snapshot;
        {
          std::shared_lock latch(pp_->pool_latch);
          snapshot.reserve(pp_->pool_.size());
          for (const auto& it : pp_->pool_) {
            PagePool::Entry& entry = *it.second;
            entry.pin_count.fetch_add(1, std::memory_order_relaxed);
            snapshot.emplace_back(&entry, it.first);
          }
        }
        dpt->reserve(snapshot.size());
        for (auto& [entry, pid] : snapshot) {
          {
            std::shared_lock page_latch(*entry->page_latch);
            const lsn_t rec_lsn = entry->page->RecoveryLSN();
            if (pid != kMetaPageId) {
              dpt->emplace_back(pid, rec_lsn);
            }
            if (dirty_out != nullptr &&
                (rec_lsn != std::numeric_limits<lsn_t>::max() ||
                 !entry->page->ChecksumMatches())) {
              dirty_out->emplace_back(pid, rec_lsn);
            }
          }
          PagePool::ReleasePin(*entry, pid);
        }
      };

  std::vector<std::pair<page_id_t, lsn_t>> dirty_page_table;
  std::vector<std::pair<page_id_t, lsn_t>> flush_candidates;
  snapshot_dpt(&dirty_page_table, &flush_candidates);
  std::vector<ActiveTransactionEntry> active_transaction_table;
  // A loser transaction's undo chain may walk below the checkpoint's begin
  // LSN, so the recycle floor must stop at the oldest active chain's first
  // record; zero (no records yet) adds no constraint.
  lsn_t oldest_active_first_lsn = std::numeric_limits<lsn_t>::max();
  txn_id_t oldest_active_txn = 0;
  {
    std::scoped_lock lk(tm_->transaction_table_lock);
    active_transaction_table.reserve(tm_->active_transactions_.size());
    for (const auto& it : tm_->active_transactions_) {
      // Per-transaction chain lock: the owning thread updates {record
      // appends, prev_lsn, status} under this same mutex (AppendLog,
      // PreCommit, Abort), so the snapshot can never tear a chain head away
      // from its status -- the stale running-entry combination that let a
      // durably committed transaction be restart-rolled-back as a loser.
      // Nesting order matches the rule in docs/lock_order.md: the table
      // lock is held first, the per-transaction chain mutexes below it; no
      // append path takes the table lock while holding its chain mutex.
      std::scoped_lock chain_guard(it.second->chain_mu_);
      active_transaction_table.emplace_back(it.first, it.second->status_,
                                            it.second->PrevLSN());
      if (it.second->HasLogRecord()) {
        const lsn_t first = it.second->FirstLSN();
        if (first < oldest_active_first_lsn) {
          oldest_active_first_lsn = first;
          oldest_active_txn = it.first;
        }
      }
    }
  }

  // Flush stale dirty pages within the time budget. This is what keeps a
  // long-running database's WAL bounded: without it a fuzzy checkpoint
  // never writes pages, a continuously modified hot page keeps its
  // setup-era RecoveryLSN, and the recycle floor below can never move past
  // the oldest dirty page no matter how many checkpoints succeed.
  FlushDirtyPages(&flush_candidates);
  // Re-snapshot the dirty-page table after the flush pass: flushed pages
  // are clean again (RecoveryLSN == max) and drop out of both the
  // EndCheckpoint record and the recycle floor computed below.
  dirty_page_table.clear();
  snapshot_dpt(&dirty_page_table, nullptr);

  LogRecord end = LogRecord::EndCheckpointLogRecord(dirty_page_table,
                                                    active_transaction_table);

  func_for_test();
  // Write [End-Checkpoint] log.
  RETURN_IF_FAIL(tm_->AddLog(end).GetStatus());
  // The master record must not overtake the WAL: wait until every record up
  // to (and including) EndCheckpoint is on stable storage, so a reader that
  // trusts begin_lsn always finds a complete checkpoint pair behind it.
  const lsn_t durable_end = tm_->logger_->BufferedLSN();
  RETURN_IF_FAIL(tm_->logger_->WaitForDurable(durable_end));

  // fdatasync the data file: every page image pwritten so far is durable,
  // which (a) bounds the window in which a torn data-file write can exist
  // and (b) retires the full-page WAL images that protected those writes,
  // letting the recycle floor below advance past them.
  const lsn_t pre_sync_lsn = tm_->logger_->BufferedLSN();
  if (!pp_->SyncDataFile(pre_sync_lsn)) {
    return StatusError(StatusCode::kIOError,
                       "checkpoint data-file sync failed");
  }

  RETURN_IF_FAIL(WriteMasterRecord(master_record_path, begin_lsn));
  tm_->logger_->AdviseOldBytesDurable(begin_lsn);

  // ---- WAL recycling ----
  // Everything below this floor can no longer be needed by a recovery that
  // anchors at THIS checkpoint (its master record is durable above):
  //   - begin_lsn: the redo/undo scan starts here,
  //   - the oldest DPT entry: per-page redo starts at its RecoveryLSN,
  //   - the oldest active transaction's first record: its undo chain may
  //     walk back that far,
  //   - the oldest not-yet-fsynced full-page image: its protected page image
  //     may still be torn on disk.
  RecycleFloorTerms terms;
  terms.begin_lsn = begin_lsn;
  lsn_t recycle_floor = begin_lsn;
  for (const auto& [pid, rec_lsn] : dirty_page_table) {
    if (rec_lsn < terms.min_dirty_page_reclsn) {
      terms.min_dirty_page_reclsn = rec_lsn;
      terms.min_dirty_page = pid;
    }
    recycle_floor = std::min(recycle_floor, rec_lsn);
  }
  terms.oldest_active_first_lsn = oldest_active_first_lsn;
  terms.oldest_active_txn = oldest_active_txn;
  recycle_floor = std::min(recycle_floor, oldest_active_first_lsn);
  terms.unsynced_fpw = pp_->UnsyncedSnapshotFloor();
  recycle_floor = std::min(recycle_floor, terms.unsynced_fpw);
  pp_->SetSnapshotRetentionFloor(recycle_floor);
  const lsn_t recycled_to = tm_->logger_->RecycleUpTo(recycle_floor);
  if (recycled_to != 0) {
    pp_->SetSnapshotRetentionFloor(recycled_to);
  }
  MaybeWarnWalRetention(recycle_floor, terms);
  MaybeWarnWalVolumeSpace();
  return begin_lsn;
}

void CheckpointManager::MaybeWarnWalRetention(lsn_t recycle_floor,
                                              const RecycleFloorTerms& terms) {
  const lsn_t retained_bytes =
      tm_->logger_->BufferedLSN() - tm_->logger_->LowestRetainedLSN();
  const lsn_t threshold = RetentionWarnThresholdBytes();
  if (retained_bytes <= threshold) {
    return;
  }
  const int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t last = last_retention_warn_ms.load(std::memory_order_relaxed);
  if (now_ms - last < kRetentionWarnInterval.count() ||
      !last_retention_warn_ms.compare_exchange_weak(
          last, now_ms, std::memory_order_relaxed)) {
    return;
  }
  // Name the term that actually pins the floor: a long-running transaction,
  // a page that never reaches the data file, and a DPT entry pinned by a
  // hot page each need a different remediation, and the previous warning
  // made the operator re-derive which one applied.
  std::string pin = "the checkpoint begin LSN";
  lsn_t pin_value = terms.begin_lsn;
  auto consider = [&](lsn_t value, std::string name) {
    if (value < pin_value) {
      pin_value = value;
      pin = std::move(name);
    }
  };
  consider(terms.min_dirty_page_reclsn,
           "oldest dirty-page recLSN (page " +
               std::to_string(terms.min_dirty_page) + ")");
  consider(terms.oldest_active_first_lsn,
           "oldest active transaction first LSN (txn " +
               std::to_string(terms.oldest_active_txn) + ")");
  consider(terms.unsynced_fpw, "oldest unsynced full-page image");
  LOG(WARN) << "Retained WAL is " << (retained_bytes >> 20)
            << " MiB with recycle floor " << recycle_floor
            << "; the floor is pinned by the " << pin << " (LSN " << pin_value
            << "). WAL grows without bound until that constraint clears.";
}

void CheckpointManager::RemoveStaleMasterTmp() const {
  const std::filesystem::path tmp = master_record_path + ".tmp";
  std::error_code ec;
  if (std::filesystem::exists(tmp, ec) && !ec) {
    // A crash between the temp write and the rename leaves this behind; the
    // next WriteMasterRecord would overwrite it anyway, so removal is pure
    // hygiene. Failure to remove is not fatal -- log and move on.
    std::filesystem::remove(tmp, ec);
    LOG(INFO) << "Removed stale master record temp " << tmp.string()
              << (ec ? (" (failed: " + ec.message() + ")") : "");
  }
}

void CheckpointManager::MaybeWarnWalVolumeSpace() {
  lsn_t threshold_bytes = 0;
  if (const char* env = std::getenv("TINYLAMB_WAL_FREE_WARN_BYTES");
      env != nullptr && env[0] != '\0') {
    const unsigned long long parsed = std::strtoull(env, nullptr, 10);
    // 0 keeps the default rather than disabling: an operator wanting to
    // silence the warning has the rate limit, and silence-by-zero would
    // hide the exact ENOSPC precursor this warning exists for.
    threshold_bytes =
        parsed == 0 ? (lsn_t{1} << 30) : static_cast<lsn_t>(parsed);
  } else {
    threshold_bytes = lsn_t{1} << 30;  // 1 GiB
  }
  const std::filesystem::path dir =
      std::filesystem::path(master_record_path).parent_path();
  std::error_code ec;
  const auto space = std::filesystem::space(dir, ec);
  if (ec) {
    return;  // Unobservable volume; nothing to warn about.
  }
  if (space.available > threshold_bytes) {
    last_volume_space_warn_ms_.store(0, std::memory_order_relaxed);
    return;
  }
  const int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t last = last_volume_space_warn_ms_.load(std::memory_order_relaxed);
  if (now_ms - last < kRetentionWarnInterval.count() ||
      !last_volume_space_warn_ms_.compare_exchange_weak(
          last, now_ms, std::memory_order_relaxed)) {
    return;
  }
  LOG(WARN) << "WAL volume " << (dir.empty() ? std::filesystem::path(".") : dir)
            << " is low on space: " << (space.available >> 20) << " MiB free ("
            << (space.capacity >> 20)
            << " MiB total). WAL appends fail with "
               "ENOSPC at 0, which fail-stops the engine by design; free "
               "space or lower the retention pressure before that.";
}

void CheckpointManager::FlushDirtyPages(
    std::vector<std::pair<page_id_t, lsn_t>>* candidates) {
  if (flush_budget_ms_ == 0 || candidates->empty()) {
    return;
  }
  // Oldest dirty page first: flushing in RecoveryLSN order advances the
  // recycle floor as far as the budget allows.
  std::sort(
      candidates->begin(), candidates->end(),
      [](const std::pair<page_id_t, lsn_t>& a,
         const std::pair<page_id_t, lsn_t>& b) { return a.second < b.second; });
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(flush_budget_ms_);
  size_t flushed = 0;
  for (const auto& [pid, rec_lsn] : *candidates) {
    (void)rec_lsn;
    if (std::chrono::steady_clock::now() >= deadline) {
      break;
    }
    // WriteBackStaged holds the page latch only for the content snapshot;
    // the durability gate and the pwrite run latch-free, so the flush pass
    // no longer blocks every transaction that needs one of the pages being
    // flushed (the latch-across-fsync wait was manufacturing write-intent
    // escape-hatch timeouts in bursts under WAL write pressure).  The staged
    // write re-validates cleanliness and re-dirty races internally.
    const Status written = pp_->WriteBackStaged(pid);
    if (written == Status::kSuccess) {
      ++flushed;
    } else {
      // Keep the page resident and dirty; the recycle floor stays
      // conservative (this page's recLSN) until a later pass flushes
      // it, so a failed write-back only retains WAL, never loses it.
      LOG(WARN) << "Checkpoint flush failed for page " << pid << ": "
                << written;
    }
  }
  if (flushed != 0) {
    LOG(INFO) << "Checkpoint flushed " << flushed << " dirty page(s)";
  }
}
}  // namespace tinylamb
